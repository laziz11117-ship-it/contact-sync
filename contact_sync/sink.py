"""Idempotent destination for normalized contacts.

Idempotency is structural, not hopeful. Three rules do the work:

* `dedupe_key` is the primary key, so a replay cannot create a second row;
* a row whose content hash already matches is reported as `unchanged` and
  is not rewritten, so re-running a completed sync writes nothing;
* a write carrying an older `source_updated_at` than the stored row is
  reported as `skipped_stale` and discarded, so out-of-order delivery (and
  a resumed run, whose in-process duplicate index starts empty) cannot
  resurrect an outdated version of a record.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterable
from typing import Protocol

from .normalize import Contact

_SCHEMA = """
CREATE TABLE IF NOT EXISTS contacts (
    dedupe_key    TEXT PRIMARY KEY,
    external_id   TEXT NOT NULL,
    email         TEXT NOT NULL,
    full_name     TEXT NOT NULL,
    phone         TEXT NOT NULL DEFAULT '',
    company       TEXT NOT NULL DEFAULT '',
    source_updated_at TEXT NOT NULL DEFAULT '',
    content_hash  TEXT NOT NULL,
    first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_write_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS contacts_external_id ON contacts (external_id);
"""


@dataclass
class UpsertResult:
    """How a batch of writes landed."""

    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    skipped_stale: int = 0

    def add(self, other: "UpsertResult") -> None:
        """Accumulate another batch's counters into this one."""
        self.inserted += other.inserted
        self.updated += other.updated
        self.unchanged += other.unchanged
        self.skipped_stale += other.skipped_stale

    def as_dict(self) -> dict[str, int]:
        """Return a JSON-serialisable view."""
        return {
            "inserted": self.inserted,
            "updated": self.updated,
            "unchanged": self.unchanged,
            "skipped_stale": self.skipped_stale,
        }


class Sink(Protocol):
    """Destination surface. One method, so alternatives are cheap to write."""

    def upsert(self, contacts: Iterable[Contact]) -> UpsertResult:
        """Write contacts idempotently and report what changed."""
        ...

    def close(self) -> None:
        """Release resources."""
        ...


class MemorySink:
    """In-memory sink used by tests and by `--dry-run`."""

    def __init__(self) -> None:
        """Start empty."""
        self.rows: dict[str, tuple[Contact, str]] = {}

    def upsert(self, contacts: Iterable[Contact]) -> UpsertResult:
        """Insert, update, skip-as-unchanged or skip-as-stale each contact."""
        result = UpsertResult()
        for contact in contacts:
            digest = contact.content_hash()
            existing = self.rows.get(contact.dedupe_key)
            if existing is None:
                self.rows[contact.dedupe_key] = (contact, digest)
                result.inserted += 1
            elif existing[1] == digest:
                result.unchanged += 1
            elif existing[0].updated_at > contact.updated_at:
                result.skipped_stale += 1
            else:
                self.rows[contact.dedupe_key] = (contact, digest)
                result.updated += 1
        return result

    def close(self) -> None:
        """No-op."""


class SqliteSink:
    """SQLite sink. One transaction per batch, so a crash cannot half-write."""

    def __init__(self, path: Path | str) -> None:
        """Open (and create if needed) the database at `path`."""
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def upsert(self, contacts: Iterable[Contact]) -> UpsertResult:
        """Write a batch in a single transaction and report what changed."""
        result = UpsertResult()
        batch = list(contacts)
        if not batch:
            return result
        with self._conn:  # commits on success, rolls back on exception
            for contact in batch:
                digest = contact.content_hash()
                row = self._conn.execute(
                    "SELECT content_hash, source_updated_at FROM contacts "
                    "WHERE dedupe_key = ?",
                    (contact.dedupe_key,),
                ).fetchone()
                if row is None:
                    self._conn.execute(
                        "INSERT INTO contacts (dedupe_key, external_id, email, "
                        "full_name, phone, company, source_updated_at, content_hash) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            contact.dedupe_key,
                            contact.external_id,
                            contact.email,
                            contact.full_name,
                            contact.phone,
                            contact.company,
                            contact.updated_at,
                            digest,
                        ),
                    )
                    result.inserted += 1
                elif row["content_hash"] == digest:
                    result.unchanged += 1
                elif row["source_updated_at"] > contact.updated_at:
                    # The stored row is newer. Do not let a replay or an
                    # out-of-order page overwrite it with older data.
                    result.skipped_stale += 1
                else:
                    self._conn.execute(
                        "UPDATE contacts SET external_id = ?, email = ?, "
                        "full_name = ?, phone = ?, company = ?, "
                        "source_updated_at = ?, content_hash = ?, "
                        "last_write_at = datetime('now') WHERE dedupe_key = ?",
                        (
                            contact.external_id,
                            contact.email,
                            contact.full_name,
                            contact.phone,
                            contact.company,
                            contact.updated_at,
                            digest,
                            contact.dedupe_key,
                        ),
                    )
                    result.updated += 1
        return result

    def count(self) -> int:
        """Return the number of rows currently stored."""
        return int(self._conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0])

    def close(self) -> None:
        """Close the database connection."""
        self._conn.close()
