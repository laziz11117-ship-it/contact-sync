"""The single seam where this package touches the network.

Everything above this module depends on the `Transport` protocol, never on
`urllib`. That is what lets the test suite run with the socket module
disabled: tests inject an in-memory transport instead.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from collections.abc import Mapping
from typing import Any, Protocol

from .errors import TransportError

_SNIPPET_LIMIT = 200


@dataclass(frozen=True)
class Response:
    """An HTTP response, already read into memory."""

    status: int
    headers: Mapping[str, str]
    body: bytes

    def json(self) -> Any:
        """Parse the body as JSON.

        Raises:
            TransportError: the body is not valid JSON. This is classified as
                transient on purpose: in practice a non-JSON body on an
                otherwise fine response is a proxy or gateway interstitial,
                which goes away on retry. A source that genuinely serves
                malformed JSON will fail the run once retries are exhausted.
        """
        try:
            return json.loads(self.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TransportError(f"response body was not valid JSON: {exc}") from exc

    def snippet(self) -> str:
        """Return a short, log-safe excerpt of the body."""
        text = self.body.decode("utf-8", errors="replace").strip().replace("\n", " ")
        return text[:_SNIPPET_LIMIT]


class Transport(Protocol):
    """Minimal HTTP client surface used by this package."""

    def get(
        self, url: str, *, headers: Mapping[str, str], timeout: float
    ) -> Response:  # pragma: no cover - protocol declaration
        """Perform a GET request and return the response.

        Implementations must raise `TransportError` when no response was
        received, and must return non-2xx responses rather than raising,
        so that status classification happens in exactly one place.
        """
        ...


class UrllibTransport:
    """Standard-library HTTP transport. No third-party dependencies."""

    def get(self, url: str, *, headers: Mapping[str, str], timeout: float) -> Response:
        """Perform a GET request using `urllib.request`."""
        request = urllib.request.Request(url, headers=dict(headers), method="GET")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as raw:
                return Response(
                    status=raw.status,
                    headers={k.lower(): v for k, v in raw.headers.items()},
                    body=raw.read(),
                )
        except urllib.error.HTTPError as exc:
            # An HTTPError *is* a response; hand it back for classification.
            body = exc.read() if hasattr(exc, "read") else b""
            return Response(
                status=exc.code,
                headers={k.lower(): v for k, v in (exc.headers or {}).items()},
                body=body,
            )
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise TransportError(f"request to {url} failed: {exc}") from exc
