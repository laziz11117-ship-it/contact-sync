"""Paginated reader for the source API.

Pages are yielded lazily, one HTTP request at a time, each request wrapped
in the retry policy. The caller decides when to checkpoint, which is what
makes resume possible.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from collections.abc import Callable, Iterator, Mapping
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .config import SourceConfig
from .errors import PermanentError, TransientError
from .retry import raise_for_status, retry_call
from .transport import Transport


def get_path(record: Mapping[str, Any], dotted: str) -> Any:
    """Look up a dotted path such as `company.name`, returning None if absent."""
    current: Any = record
    for part in dotted.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current[part]
    return current


@dataclass(frozen=True)
class Page:
    """One page of raw records plus the cursor needed to fetch the next one."""

    records: tuple[Mapping[str, Any], ...]
    cursor: int
    next_cursor: int | None
    total: int | None


@dataclass
class SourceStats:
    """Counters the run report publishes."""

    http_requests: int = 0
    retries: int = 0
    pages_fetched: int = 0
    records_read: int = 0
    retry_detail: list[dict[str, Any]] = field(default_factory=list)


class PaginatedSource:
    """Reads an offset/limit paginated JSON endpoint."""

    def __init__(
        self,
        config: SourceConfig,
        transport: Transport,
        *,
        env: Mapping[str, str] | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rng: Callable[[], float] = random.random,
    ) -> None:
        """Store configuration and injected clock/randomness sources."""
        self._config = config
        self._transport = transport
        self._headers = {"Accept": "application/json", **config.auth_headers(env)}
        self._sleep = sleep
        self._rng = rng
        self.stats = SourceStats()

    def _url(self, cursor: int) -> str:
        """Build the page URL, preserving any query string already in base_url."""
        parts = urlsplit(self._config.base_url)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        query[self._config.limit_param] = str(self._config.page_size)
        query[self._config.offset_param] = str(cursor)
        return urlunsplit(parts._replace(query=urlencode(query)))

    def _fetch(self, cursor: int) -> Page:
        """Fetch exactly one page, retrying transient failures."""
        url = self._url(cursor)

        def attempt() -> Page:
            """One HTTP request and its decoding, as the retry layer sees it."""
            self.stats.http_requests += 1
            response = self._transport.get(
                url,
                headers=self._headers,
                timeout=self._config.timeout_seconds,
            )
            raise_for_status(
                response.status, url, response.headers, response.snippet()
            )
            payload = response.json()
            return self._to_page(payload, cursor)

        def note_retry(attempt_no: int, delay: float, error: TransientError) -> None:
            """Record a retry so the run report can show what was absorbed."""
            self.stats.retries += 1
            self.stats.retry_detail.append(
                {
                    "cursor": cursor,
                    "attempt": attempt_no,
                    "sleep_seconds": round(delay, 3),
                    "error": f"{type(error).__name__}: {error}",
                }
            )

        return retry_call(
            attempt,
            self._config.retry,
            sleep=self._sleep,
            rng=self._rng,
            on_retry=note_retry,
        )

    def _to_page(self, payload: Any, cursor: int) -> Page:
        """Extract records and the next cursor from a decoded response body."""
        records = get_path(payload, self._config.records_path) if isinstance(
            payload, Mapping
        ) else payload
        if not isinstance(records, list):
            raise PermanentError(
                f"expected a list at {self._config.records_path!r} in the response, "
                f"got {type(records).__name__}"
            )
        clean = tuple(r for r in records if isinstance(r, Mapping))
        total: int | None = None
        if self._config.total_path and isinstance(payload, Mapping):
            candidate = get_path(payload, self._config.total_path)
            if isinstance(candidate, int):
                total = candidate
        next_cursor: int | None = cursor + len(clean)
        if not clean:
            next_cursor = None
        elif total is not None and next_cursor is not None and next_cursor >= total:
            next_cursor = None
        return Page(records=clean, cursor=cursor, next_cursor=next_cursor, total=total)

    def pages(self, start_cursor: int = 0) -> Iterator[Page]:
        """Yield pages starting at `start_cursor` until the source is exhausted.

        Stops on an empty page, on reaching the reported total, or after
        `max_pages` pages when that limit is non-zero.
        """
        cursor: int | None = start_cursor
        fetched = 0
        while cursor is not None:
            if self._config.max_pages and fetched >= self._config.max_pages:
                return
            page = self._fetch(cursor)
            fetched += 1
            self.stats.pages_fetched += 1
            self.stats.records_read += len(page.records)
            if not page.records:
                return
            yield page
            cursor = page.next_cursor
