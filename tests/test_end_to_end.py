"""The whole thing, over real HTTP, against the bundled demo API.

This is the only module that opens a socket, and it only ever talks to a
server started inside this process on 127.0.0.1. There is no third-party
API, no credential and no outbound traffic. It exists because the unit
tests replace the transport, and something should exercise the real one.

The three tests below are the same three runs the README shows, asserted
rather than narrated.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from contact_sync.cli import DEMO_PAGE_SIZE, _demo_config
from contact_sync.demo_server import DemoServer, DemoState
from contact_sync.pipeline import run_sync
from contact_sync.report import STATUS_OK, STATUS_PARTIAL, RunReport
from contact_sync.sink import SqliteSink
from contact_sync.state import CheckpointStore
from contact_sync.transport import UrllibTransport

#: 120 demo records minus 3 blank emails and 2 malformed ones, minus the 5
#: planted duplicates, gives the number of rows a complete sync must produce.
EXPECTED_ROWS = 110


class EndToEndTests(unittest.TestCase):
    """Real transport, real sqlite, real checkpoint file, local fake API."""

    def setUp(self) -> None:
        """Create a scratch data directory."""
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.data = Path(self._tmp.name)

    def sync(self, state: DemoState, *, resume: bool = True) -> RunReport:
        """Start the demo API, run one sync pass against it, and stop it."""
        with DemoServer(state) as server:
            config = _demo_config(self.data, server.base_url)
            sink = SqliteSink(config.sink.path)
            try:
                return run_sync(
                    config,
                    UrllibTransport(),
                    sink,
                    CheckpointStore(config.run.state_path),
                    resume=resume,
                    env={},
                    sleep=lambda _: None,  # keep the suite fast; backoff is unit-tested
                )
            finally:
                sink.close()

    def row_count(self) -> int:
        """Count rows in the demo database."""
        sink = SqliteSink(self.data / "contacts.sqlite3")
        try:
            return sink.count()
        finally:
            sink.close()

    def test_full_run_survives_injected_failures(self) -> None:
        """A 503 and a 429 mid-run are retried; the run still completes."""
        report = self.sync(DemoState.default(page_size=DEMO_PAGE_SIZE))

        self.assertEqual(report.status, STATUS_OK)
        self.assertEqual(report.source["records_read"], 120)
        self.assertEqual(report.source["retries"], 2)
        self.assertEqual(report.normalized["skipped_total"], 5)
        self.assertEqual(report.sink["inserted"], EXPECTED_ROWS)
        self.assertEqual(self.row_count(), EXPECTED_ROWS)

    def test_rerun_is_a_no_op(self) -> None:
        """Running the same sync again writes nothing at all."""
        self.sync(DemoState.default(page_size=DEMO_PAGE_SIZE))
        second = self.sync(DemoState(one_shot_failures={}))

        self.assertEqual(second.status, STATUS_OK)
        self.assertEqual(second.sink["inserted"], 0)
        self.assertEqual(second.sink["updated"], 0)
        self.assertEqual(second.sink["unchanged"], EXPECTED_ROWS)
        self.assertEqual(self.row_count(), EXPECTED_ROWS)

    def test_interrupt_then_resume_equals_one_clean_run(self) -> None:
        """A source that dies at record 50 costs no data and no duplicates."""
        interrupted = self.sync(
            DemoState.default(page_size=DEMO_PAGE_SIZE, interrupt_at=50)
        )
        self.assertEqual(interrupted.status, STATUS_PARTIAL)
        self.assertEqual(interrupted.final_cursor, 50)
        partial_rows = self.row_count()
        self.assertGreater(partial_rows, 0)
        self.assertLess(partial_rows, EXPECTED_ROWS)

        resumed = self.sync(DemoState(one_shot_failures={}))

        self.assertEqual(resumed.status, STATUS_OK)
        self.assertEqual(resumed.resumed_from_cursor, 50)
        self.assertEqual(resumed.source["records_read"], 70)
        self.assertEqual(self.row_count(), EXPECTED_ROWS)
        self.assertIsNone(CheckpointStore(self.data / "state.json").load())
