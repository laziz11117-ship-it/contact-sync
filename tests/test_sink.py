"""Idempotency of the sink. Both implementations are held to one contract."""

from __future__ import annotations

import tempfile
from collections.abc import Callable
from pathlib import Path

from contact_sync.normalize import Contact
from contact_sync.sink import MemorySink, Sink, SqliteSink
from tests.support import NoNetworkTestCase


def contact(email: str = "a@x.example", phone: str = "+15550000000") -> Contact:
    """Build a canonical contact."""
    return Contact(
        dedupe_key=email,
        external_id="1",
        email=email,
        full_name="Ada Lovelace",
        phone=phone,
        company="Northwind",
        updated_at="2026-01-01T00:00:00Z",
    )


class SinkContractTests(NoNetworkTestCase):
    """Behaviour every sink must exhibit, checked against each implementation."""

    def setUp(self) -> None:
        """Create a scratch directory for sqlite files."""
        super().setUp()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp_path = Path(self._tmp.name)

    def each_sink(self) -> list[tuple[str, Callable[[], Sink]]]:
        """Return a label and factory for every sink implementation."""
        db = self.tmp_path / "contacts.sqlite3"
        return [
            ("memory", lambda: MemorySink()),
            ("sqlite", lambda: SqliteSink(db)),
        ]

    def test_first_write_inserts(self) -> None:
        """A new contact counts as an insert."""
        for label, factory in self.each_sink():
            with self.subTest(sink=label):
                sink = factory()
                try:
                    self.assertEqual(
                        sink.upsert([contact()]).as_dict(),
                        {"inserted": 1, "updated": 0, "unchanged": 0, "skipped_stale": 0},
                    )
                finally:
                    sink.close()

    def test_rewriting_identical_data_changes_nothing(self) -> None:
        """Replaying a batch is a no-op. This is what makes resume safe."""
        for label, factory in self.each_sink():
            with self.subTest(sink=label):
                sink = factory()
                try:
                    batch = [contact("a@x.example"), contact("b@x.example")]
                    first = sink.upsert(batch).as_dict()
                    second = sink.upsert(batch).as_dict()
                    third = sink.upsert(batch).as_dict()
                finally:
                    sink.close()
                self.assertEqual(first, {"inserted": 2, "updated": 0, "unchanged": 0, "skipped_stale": 0})
                self.assertEqual(second, {"inserted": 0, "updated": 0, "unchanged": 2, "skipped_stale": 0})
                self.assertEqual(third, second)

    def test_changed_data_updates_in_place(self) -> None:
        """Same key, different content: update rather than duplicate."""
        for label, factory in self.each_sink():
            with self.subTest(sink=label):
                sink = factory()
                try:
                    sink.upsert([contact(phone="+15550000000")])
                    result = sink.upsert([contact(phone="+15559999999")]).as_dict()
                finally:
                    sink.close()
                self.assertEqual(
                    result, {"inserted": 0, "updated": 1, "unchanged": 0, "skipped_stale": 0}
                )

    def test_empty_batch_is_accepted(self) -> None:
        """An empty page must not open a transaction or fail."""
        for label, factory in self.each_sink():
            with self.subTest(sink=label):
                sink = factory()
                try:
                    self.assertEqual(
                        sink.upsert([]).as_dict(),
                        {"inserted": 0, "updated": 0, "unchanged": 0, "skipped_stale": 0},
                    )
                finally:
                    sink.close()


class SqliteSinkTests(NoNetworkTestCase):
    """Durability specifics of the sqlite implementation."""

    def setUp(self) -> None:
        """Create a scratch directory."""
        super().setUp()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp_path = Path(self._tmp.name)

    def test_rows_survive_reopening(self) -> None:
        """Data is committed, not held in a connection, so a crash loses nothing."""
        db = self.tmp_path / "contacts.sqlite3"
        first = SqliteSink(db)
        first.upsert([contact("a@x.example"), contact("b@x.example")])
        first.close()

        second = SqliteSink(db)
        try:
            self.assertEqual(second.count(), 2)
            self.assertEqual(
                second.upsert([contact("a@x.example")]).as_dict(),
                {"inserted": 0, "updated": 0, "unchanged": 1, "skipped_stale": 0},
            )
        finally:
            second.close()

    def test_missing_parent_directory_is_created(self) -> None:
        """A fresh checkout with no data/ directory still works."""
        sink = SqliteSink(self.tmp_path / "nested" / "deeper" / "contacts.sqlite3")
        try:
            self.assertEqual(sink.count(), 0)
        finally:
            sink.close()


class StaleWriteTests(NoNetworkTestCase):
    """An older version of a record must never overwrite a newer one."""

    def setUp(self) -> None:
        """Create a scratch directory."""
        super().setUp()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp_path = Path(self._tmp.name)

    def older(self, email: str = "a@x.example") -> Contact:
        """A variant of the contact carrying an older source timestamp."""
        return Contact(
            dedupe_key=email,
            external_id="2",
            email=email,
            full_name="Ada L.",
            phone="+15551111111",
            company="Contoso",
            updated_at="2025-01-01T00:00:00Z",
        )

    def each_sink(self) -> list[tuple[str, Callable[[], Sink]]]:
        """Return a label and factory for every sink implementation."""
        db = self.tmp_path / "contacts.sqlite3"
        return [("memory", lambda: MemorySink()), ("sqlite", lambda: SqliteSink(db))]

    def test_stale_write_is_rejected_and_counted(self) -> None:
        """A write with an older source timestamp is skipped, not applied."""
        for label, factory in self.each_sink():
            with self.subTest(sink=label):
                sink = factory()
                try:
                    sink.upsert([contact()])
                    result = sink.upsert([self.older()]).as_dict()
                finally:
                    sink.close()
                self.assertEqual(
                    result,
                    {"inserted": 0, "updated": 0, "unchanged": 0, "skipped_stale": 1},
                )

    def test_equal_timestamps_still_allow_a_correction(self) -> None:
        """Only a strictly older timestamp is stale, so same-second edits apply."""
        for label, factory in self.each_sink():
            with self.subTest(sink=label):
                sink = factory()
                try:
                    sink.upsert([contact()])
                    result = sink.upsert([contact(phone="+15557777777")]).as_dict()
                finally:
                    sink.close()
                self.assertEqual(
                    result,
                    {"inserted": 0, "updated": 1, "unchanged": 0, "skipped_stale": 0},
                )
