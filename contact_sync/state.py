"""Resume checkpoint.

The checkpoint is written only after the sink has committed the page it
describes. If the process dies between the commit and the checkpoint write,
the next run re-reads one page and the sink reports it as `unchanged` —
at-least-once delivery made safe by an idempotent write.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class Checkpoint:
    """Where the next run should start, and how it got there."""

    cursor: int
    pages_committed: int
    updated_at: str
    run_id: str


class CheckpointStore:
    """Reads and atomically writes a single JSON checkpoint file."""

    def __init__(self, path: Path | str) -> None:
        """Remember the checkpoint path; the file need not exist yet."""
        self.path = Path(path)

    def load(self) -> Checkpoint | None:
        """Return the stored checkpoint, or None when there is none or it is unreadable."""
        if not self.path.is_file():
            return None
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            return Checkpoint(
                cursor=int(raw["cursor"]),
                pages_committed=int(raw.get("pages_committed", 0)),
                updated_at=str(raw.get("updated_at", "")),
                run_id=str(raw.get("run_id", "")),
            )
        except (json.JSONDecodeError, KeyError, TypeError, ValueError, OSError):
            # A corrupt checkpoint must not wedge the pipeline; start over.
            return None

    def save(self, checkpoint: Checkpoint) -> None:
        """Write the checkpoint atomically (temp file + rename)."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(
            json.dumps(asdict(checkpoint), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(tmp, self.path)

    def clear(self) -> None:
        """Delete the checkpoint, if present."""
        self.path.unlink(missing_ok=True)
