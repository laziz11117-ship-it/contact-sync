"""Error taxonomy.

The whole retry strategy hangs off one distinction: a `TransientError`
may succeed if we try again, a `PermanentError` will not. Every failure
path in this package raises one or the other, so the retry layer never
has to guess.
"""

from __future__ import annotations


class ContactSyncError(Exception):
    """Base class for every error this package raises deliberately."""


class TransientError(ContactSyncError):
    """Failure that is worth retrying (timeout, 503, connection reset)."""


class PermanentError(ContactSyncError):
    """Failure that retrying cannot fix (401, 404, schema mismatch)."""


class ConfigError(PermanentError):
    """Configuration is missing or invalid."""


class TransportError(TransientError):
    """The request never produced an HTTP response (DNS, TCP, timeout)."""


class HttpError(ContactSyncError):
    """An HTTP response arrived but carried a failure status."""

    def __init__(self, status: int, url: str, body_snippet: str = "") -> None:
        """Record the status, URL and a short body excerpt."""
        self.status = status
        self.url = url
        self.body_snippet = body_snippet
        detail = f": {body_snippet}" if body_snippet else ""
        super().__init__(f"HTTP {status} from {url}{detail}")


class TransientHttpError(HttpError, TransientError):
    """Retryable HTTP status (429, 500, 502, 503, 504, ...)."""

    def __init__(
        self,
        status: int,
        url: str,
        body_snippet: str = "",
        retry_after_seconds: float | None = None,
    ) -> None:
        """Record the failure plus any server-supplied retry hint."""
        super().__init__(status, url, body_snippet)
        self.retry_after_seconds = retry_after_seconds


class PermanentHttpError(HttpError, PermanentError):
    """Non-retryable HTTP status (400, 401, 403, 404, 422, ...)."""
