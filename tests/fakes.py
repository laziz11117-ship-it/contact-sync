"""In-memory doubles for the HTTP transport.

These implement the `Transport` protocol, which is the only place the real
package touches the network. Tests therefore exercise the real retry,
pagination, normalization, sink and checkpoint code, with only the socket
replaced.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs, urlsplit

from contact_sync.transport import Response


def json_response(
    status: int, payload: Any, headers: Mapping[str, str] | None = None
) -> Response:
    """Build a `Response` carrying a JSON body."""
    return Response(
        status=status,
        headers={k.lower(): v for k, v in (headers or {}).items()},
        body=json.dumps(payload).encode("utf-8"),
    )


@dataclass
class QueueTransport:
    """Returns (or raises) scripted items in order, ignoring the URL.

    Used where the point of the test is the retry layer, not pagination.
    """

    script: list[Response | BaseException]
    calls: int = 0

    def get(self, url: str, *, headers: Mapping[str, str], timeout: float) -> Response:
        """Pop the next scripted item; raise it if it is an exception."""
        if self.calls >= len(self.script):
            raise AssertionError(
                f"transport called {self.calls + 1} times, script has {len(self.script)}"
            )
        item = self.script[self.calls]
        self.calls += 1
        if isinstance(item, BaseException):
            raise item
        return item


@dataclass
class DatasetTransport:
    """Serves a dataset with offset/limit paging and injectable failures.

    Mirrors the bundled demo server's behaviour without opening a socket.
    """

    records: Sequence[Mapping[str, Any]]
    page_size_param: str = "limit"
    offset_param: str = "skip"
    records_key: str = "users"
    include_total: bool = True
    #: offset -> statuses served once each before that page succeeds
    one_shot_failures: dict[int, list[int]] = field(default_factory=dict)
    #: from this offset onwards, always fail with 503
    fail_from_offset: int | None = None
    requests: list[tuple[int, int]] = field(default_factory=list)
    seen_headers: list[Mapping[str, str]] = field(default_factory=list)

    def get(self, url: str, *, headers: Mapping[str, str], timeout: float) -> Response:
        """Return the requested page, or an injected failure."""
        query = parse_qs(urlsplit(url).query)
        limit = int(query[self.page_size_param][0])
        offset = int(query[self.offset_param][0])
        self.requests.append((offset, limit))
        self.seen_headers.append(dict(headers))

        if self.fail_from_offset is not None and offset >= self.fail_from_offset:
            return json_response(503, {"error": "upstream unavailable"})

        pending = self.one_shot_failures.get(offset)
        if pending:
            status = pending.pop(0)
            extra = {"Retry-After": "0"} if status == 429 else None
            return json_response(status, {"error": "try again"}, extra)

        window = list(self.records[offset : offset + limit])
        payload: dict[str, Any] = {
            self.records_key: window,
            "skip": offset,
            "limit": limit,
        }
        if self.include_total:
            payload["total"] = len(self.records)
        return json_response(200, payload)

    @property
    def offsets_requested(self) -> list[int]:
        """Offsets requested, in order, including retried ones."""
        return [offset for offset, _ in self.requests]
