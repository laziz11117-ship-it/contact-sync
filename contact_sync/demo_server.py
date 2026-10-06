"""A local stand-in for a flaky third-party API.

It exists so that the pipeline can be run end to end, over real HTTP, with
no account, no API key and no network access. The dataset is synthetic and
deterministic, and it deliberately contains the mess a real source contains:
duplicate emails, blank emails, malformed emails.

The server also injects failures on purpose:

* one 503 on the second page, served once, then the page succeeds;
* one 429 with `Retry-After: 1` on the fourth page, served once;
* an optional permanent-looking 503 from a chosen offset onwards, used to
  demonstrate interruption and resume.
"""

from __future__ import annotations

import json
import random
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

RECORD_COUNT = 120
_FIRST = ("Ada", "Grace", "Alan", "Edsger", "Barbara", "Ken", "Dennis", "Radia")
_LAST = ("Lovelace", "Hopper", "Turing", "Dijkstra", "Liskov", "Thompson", "Ritchie", "Perlman")
_COMPANIES = ("Northwind", "Contoso", "Initech", "Globex", "Umbrella")


def build_dataset(count: int = RECORD_COUNT, seed: int = 7) -> list[dict[str, Any]]:
    """Build the synthetic source dataset.

    The shape mimics a typical CRM "list contacts" response. Defects are
    planted at fixed indices so counts in the README stay reproducible.
    """
    rnd = random.Random(seed)
    records: list[dict[str, Any]] = []
    for i in range(1, count + 1):
        first = _FIRST[i % len(_FIRST)]
        last = _LAST[(i * 3) % len(_LAST)]
        record: dict[str, Any] = {
            "id": i,
            "firstName": first,
            "lastName": last,
            "email": f"{first}.{last}{i}@example.invalid".lower(),
            "phone": f"+1 (555) {rnd.randint(100, 999)}-{rnd.randint(1000, 9999)}",
            "company": {"name": _COMPANIES[i % len(_COMPANIES)]},
            "updatedAt": f"2026-0{(i % 9) + 1}-15T10:00:00Z",
        }
        records.append(record)

    # Planted defects.
    for i in (9, 27, 61):  # blank email
        records[i]["email"] = ""
    for i in (14, 88):  # malformed email
        records[i]["email"] = "not-an-email"
    for source_i, duplicate_of in ((33, 2), (34, 2), (70, 40), (99, 40), (110, 5)):
        records[source_i]["email"] = records[duplicate_of]["email"]
    return records


@dataclass
class DemoState:
    """Mutable server state: the dataset and the remaining injected failures."""

    records: list[dict[str, Any]] = field(default_factory=build_dataset)
    one_shot_failures: dict[int, list[int]] = field(default_factory=dict)
    fail_from_offset: int | None = None
    request_log: list[tuple[int, int]] = field(default_factory=list)

    @classmethod
    def default(cls, page_size: int = 25, interrupt_at: int | None = None) -> "DemoState":
        """Build the state used by `contact-sync demo`."""
        return cls(
            one_shot_failures={page_size: [503], page_size * 3: [429]},
            fail_from_offset=interrupt_at,
        )


class _Handler(BaseHTTPRequestHandler):
    """Serves `GET /users?limit=&skip=`."""

    protocol_version = "HTTP/1.1"

    def __init__(self, *args: Any, state: DemoState, **kwargs: Any) -> None:
        """Attach the shared dataset and failure script to this handler."""
        self.state = state
        super().__init__(*args, **kwargs)

    def log_message(self, fmt: str, *args: Any) -> None:
        """Silence the default stderr access log."""

    def _send_json(
        self, status: int, payload: Any, extra: dict[str, str] | None = None
    ) -> None:
        """Write a JSON response with the given status and extra headers."""
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802 - name fixed by BaseHTTPRequestHandler
        """Return one page, or an injected failure."""
        parts = urlsplit(self.path)
        if parts.path != "/users":
            self._send_json(404, {"error": "not found"})
            return
        query = parse_qs(parts.query)
        limit = int(query.get("limit", ["25"])[0])
        skip = int(query.get("skip", ["0"])[0])
        self.state.request_log.append((skip, limit))

        if self.state.fail_from_offset is not None and skip >= self.state.fail_from_offset:
            self._send_json(503, {"error": "upstream unavailable"})
            return

        pending = self.state.one_shot_failures.get(skip)
        if pending:
            status = pending.pop(0)
            extra = {"Retry-After": "1"} if status == 429 else None
            self._send_json(status, {"error": "try again"}, extra)
            return

        window = self.state.records[skip : skip + limit]
        self._send_json(
            200, {"users": window, "total": len(self.state.records), "skip": skip, "limit": limit}
        )


class DemoServer:
    """Context manager that runs the demo API on a free localhost port."""

    def __init__(self, state: DemoState | None = None) -> None:
        """Bind to 127.0.0.1 on an ephemeral port."""
        self.state = state or DemoState.default()

        def factory(*args: Any, **kwargs: Any) -> _Handler:
            """Build a handler bound to this server's state."""
            return _Handler(*args, state=self.state, **kwargs)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), factory)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        """Return the `/users` endpoint URL, including the chosen port."""
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}/users"

    def __enter__(self) -> "DemoServer":
        """Start serving in a background thread."""
        self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        """Stop the server and join the thread."""
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)
