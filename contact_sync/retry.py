"""Status classification and exponential backoff.

Two rules, both testable in isolation:

1. Only `TransientError` is retried. A permanent failure aborts immediately
   instead of burning four attempts and the operator's patience.
2. Delays grow geometrically, are capped, are jittered, and yield to a
   server-supplied `Retry-After`.

`sleep` and `rng` are injected so tests assert on the delay sequence
without spending wall-clock time.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass
from collections.abc import Callable, Mapping
from typing import TypeVar

from .errors import PermanentHttpError, TransientError, TransientHttpError

T = TypeVar("T")

#: Statuses worth retrying. Everything else in 4xx is the caller's fault.
TRANSIENT_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504, 509, 598, 599})


@dataclass(frozen=True)
class RetryPolicy:
    """Backoff parameters. `max_attempts` counts the first attempt."""

    max_attempts: int = 4
    initial_backoff_seconds: float = 0.5
    max_backoff_seconds: float = 8.0
    multiplier: float = 2.0
    jitter: float = 0.2

    def delay_for(self, attempt: int, rng: Callable[[], float]) -> float:
        """Return the delay in seconds to wait after a failed `attempt` (1-based)."""
        raw = self.initial_backoff_seconds * (self.multiplier ** (attempt - 1))
        capped = min(raw, self.max_backoff_seconds)
        if self.jitter <= 0:
            return capped
        # rng() in [0, 1) maps to a factor in [1 - jitter, 1 + jitter).
        factor = 1.0 + self.jitter * (rng() * 2.0 - 1.0)
        return max(0.0, capped * factor)


def parse_retry_after(value: str | None) -> float | None:
    """Parse a `Retry-After` header expressed in seconds.

    HTTP-date form is ignored on purpose; see README limitations.
    """
    if value is None:
        return None
    try:
        seconds = float(value.strip())
    except ValueError:
        return None
    return seconds if seconds >= 0 else None


def raise_for_status(
    status: int, url: str, headers: Mapping[str, str], body_snippet: str = ""
) -> None:
    """Raise the correctly classified error for a non-2xx status.

    Args:
        status: HTTP status code.
        url: Request URL, included in the error message.
        headers: Response headers, read for `Retry-After`.
        body_snippet: Short excerpt of the body for diagnostics.

    Raises:
        TransientHttpError: status is in `TRANSIENT_STATUSES`.
        PermanentHttpError: any other non-2xx status.
    """
    if 200 <= status < 300:
        return
    if status in TRANSIENT_STATUSES:
        retry_after = parse_retry_after(headers.get("retry-after"))
        raise TransientHttpError(status, url, body_snippet, retry_after)
    raise PermanentHttpError(status, url, body_snippet)


def retry_call(
    operation: Callable[[], T],
    policy: RetryPolicy,
    *,
    sleep: Callable[[float], None] = time.sleep,
    rng: Callable[[], float] = random.random,
    on_retry: Callable[[int, float, TransientError], None] | None = None,
) -> T:
    """Call `operation`, retrying transient failures with exponential backoff.

    Args:
        operation: Zero-argument callable to invoke.
        policy: Backoff parameters.
        sleep: Injected sleep, for tests.
        rng: Injected uniform [0, 1) source, for deterministic jitter in tests.
        on_retry: Called as `(attempt, delay, error)` before each sleep.

    Returns:
        Whatever `operation` returns on its first success.

    Raises:
        TransientError: the last attempt also failed transiently.
        PermanentError: raised immediately, without retrying.
    """
    last_error: TransientError | None = None
    for attempt in range(1, max(1, policy.max_attempts) + 1):
        try:
            return operation()
        except TransientError as exc:
            last_error = exc
            if attempt >= policy.max_attempts:
                break
            delay = policy.delay_for(attempt, rng)
            server_hint = getattr(exc, "retry_after_seconds", None)
            if server_hint is not None:
                delay = min(float(server_hint), policy.max_backoff_seconds)
            if on_retry is not None:
                on_retry(attempt, delay, exc)
            sleep(delay)
    assert last_error is not None  # loop only exits here after a failure
    raise last_error
