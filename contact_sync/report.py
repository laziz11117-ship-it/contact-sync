"""Machine-readable run report.

The report is the deliverable, not a side effect. It is written on every
outcome, including a failed run, so a scheduler or an operator can decide
what to do without parsing stdout.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

#: Report status values. `partial` means a resumable stop, not a silent loss.
STATUS_OK = "ok"
STATUS_PARTIAL = "partial"
STATUS_FAILED = "failed"

#: Process exit codes, keyed by status.
EXIT_CODES = {STATUS_OK: 0, STATUS_PARTIAL: 1, STATUS_FAILED: 2}

REPORT_SCHEMA_VERSION = 1


@dataclass
class RunReport:
    """Everything one run is willing to claim about itself."""

    run_id: str
    started_at: str
    finished_at: str = ""
    duration_ms: int = 0
    status: str = STATUS_FAILED
    resumed_from_cursor: int | None = None
    final_cursor: int = 0
    source: dict[str, Any] = field(default_factory=dict)
    normalized: dict[str, Any] = field(default_factory=dict)
    sink: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, str]] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serialisable view with a schema version."""
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            "run_id": self.run_id,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "cursor": {
                "resumed_from": self.resumed_from_cursor,
                "final": self.final_cursor,
            },
            "source": self.source,
            "normalized": self.normalized,
            "sink": self.sink,
            "errors": self.errors,
        }

    def to_json(self) -> str:
        """Serialise the report as indented JSON."""
        return json.dumps(self.as_dict(), indent=2, sort_keys=False) + "\n"

    def write(self, path: Path | str) -> Path:
        """Write the report to `path`, creating parent directories."""
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(self.to_json(), encoding="utf-8")
        return target

    def summary_line(self) -> str:
        """One-line human summary, for stdout."""
        sink = self.sink
        return (
            f"{self.status}: read {self.source.get('records_read', 0)}"
            f" | inserted {sink.get('inserted', 0)}"
            f" updated {sink.get('updated', 0)}"
            f" unchanged {sink.get('unchanged', 0)}"
            f" stale {sink.get('skipped_stale', 0)}"
            f" | skipped {self.normalized.get('skipped_total', 0)}"
            f" duplicates {self.normalized.get('duplicates_dropped', 0)}"
            f" | retries {self.source.get('retries', 0)}"
            f" | cursor {self.final_cursor}"
            f" | {self.duration_ms} ms"
        )
