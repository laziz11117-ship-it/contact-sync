"""End-to-end pipeline behaviour: reporting, idempotency, interrupt, resume.

These tests drive the real pipeline, source, normalizer, sqlite sink and
checkpoint store. Only the HTTP transport is a double, and the socket module
is disabled, so nothing here can reach a network.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from contact_sync.config import (
    Config,
    FieldMapping,
    RunConfig,
    SinkConfig,
    SourceConfig,
)
from contact_sync.pipeline import run_sync
from contact_sync.report import (
    EXIT_CODES,
    STATUS_FAILED,
    STATUS_OK,
    STATUS_PARTIAL,
    RunReport,
)
from contact_sync.retry import RetryPolicy
from contact_sync.sink import SqliteSink
from contact_sync.state import Checkpoint, CheckpointStore
from tests.fakes import DatasetTransport, QueueTransport, json_response
from tests.support import NoNetworkTestCase

PAGE_SIZE = 10


def dataset() -> list[dict[str, Any]]:
    """30 records: 1 duplicate pair, 1 blank email, 1 malformed email.

    Expected accounting for a full run: 30 read, 27 valid, 1 duplicate
    dropped, 2 skipped.
    """
    records: list[dict[str, Any]] = [
        {
            "id": i,
            "firstName": "Ada",
            "lastName": f"N{i}",
            "email": f"u{i}@x.example",
            "phone": f"+1555000{i:04d}",
            "company": {"name": "Northwind"},
            "updatedAt": f"2026-01-{(i % 28) + 1:02d}T00:00:00Z",
        }
        for i in range(1, 31)
    ]
    records[4]["email"] = ""                           # skipped: missing_email
    records[15]["email"] = "nope"                      # skipped: invalid_email
    records[22]["email"] = records[1]["email"]         # duplicate of record 2
    records[22]["updatedAt"] = "2025-01-01T00:00:00Z"  # older, so it loses
    return records


class PipelineTestCase(NoNetworkTestCase):
    """Shared setup: a scratch data directory and a run helper."""

    def setUp(self) -> None:
        """Create a scratch data directory for this test."""
        super().setUp()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.data = Path(self._tmp.name)
        self.config = self.make_config()

    def make_config(self, **source_overrides: Any) -> Config:
        """Build a config whose artefacts all live in the scratch directory."""
        source_kwargs: dict[str, Any] = dict(
            base_url="http://fake.invalid/users",
            records_path="users",
            total_path="total",
            limit_param="limit",
            offset_param="skip",
            page_size=PAGE_SIZE,
            retry=RetryPolicy(max_attempts=3, initial_backoff_seconds=0.01, jitter=0.0),
        )
        source_kwargs.update(source_overrides)
        return Config(
            source=SourceConfig(**source_kwargs),
            mapping=FieldMapping(
                external_id="id",
                email="email",
                first_name="firstName",
                last_name="lastName",
                phone="phone",
                company="company.name",
                updated_at="updatedAt",
            ),
            sink=SinkConfig(kind="sqlite", path=self.data / "contacts.sqlite3"),
            run=RunConfig(
                state_path=self.data / "state.json",
                report_path=self.data / "last-run.json",
            ),
        )

    def execute(
        self,
        transport: Any,
        *,
        resume: bool = True,
        config: Config | None = None,
        env: dict[str, str] | None = None,
    ) -> RunReport:
        """Run one sync pass with sleeping, jitter and clocks pinned."""
        active = config or self.config
        sink = SqliteSink(active.sink.path)
        try:
            return run_sync(
                active,
                transport,
                sink,
                CheckpointStore(active.run.state_path),
                resume=resume,
                env=env or {},
                sleep=lambda _: None,
                rng=lambda: 0.5,
                monotonic=iter([0.0, 1.5]).__next__,
                now=lambda: "2026-10-06T00:00:00.000+00:00",
            )
        finally:
            sink.close()

    def row_count(self) -> int:
        """Count rows currently in the sink."""
        sink = SqliteSink(self.config.sink.path)
        try:
            return sink.count()
        finally:
            sink.close()

    def checkpoint(self) -> Checkpoint | None:
        """Read the stored checkpoint, if any."""
        return CheckpointStore(self.config.run.state_path).load()


class HappyPathTests(PipelineTestCase):
    """A clean run, and what it reports."""

    def test_report_accounts_for_every_record(self) -> None:
        """read == valid + duplicates dropped + skipped. No unexplained gaps."""
        report = self.execute(DatasetTransport(records=dataset()))

        self.assertEqual(report.status, STATUS_OK)
        self.assertEqual(report.source["records_read"], 30)
        self.assertEqual(report.normalized["valid"], 27)
        self.assertEqual(report.normalized["duplicates_dropped"], 1)
        self.assertEqual(
            report.normalized["skipped_by_reason"],
            {"invalid_email": 1, "missing_email": 1},
        )
        self.assertEqual(
            report.normalized["valid"]
            + report.normalized["duplicates_dropped"]
            + report.normalized["skipped_total"],
            report.source["records_read"],
        )
        self.assertEqual(
            report.sink,
            {
                "kind": "sqlite",
                "inserted": 27,
                "updated": 0,
                "unchanged": 0,
                "skipped_stale": 0,
            },
        )
        self.assertEqual(report.final_cursor, 30)
        self.assertEqual(report.duration_ms, 1500)
        self.assertEqual(report.errors, [])

    def test_successful_run_clears_the_checkpoint(self) -> None:
        """A completed run leaves no checkpoint, so the next run starts clean."""
        self.execute(DatasetTransport(records=dataset()))
        self.assertIsNone(self.checkpoint())

    def test_empty_source_is_a_clean_no_op(self) -> None:
        """Zero records is a successful run, not an error."""
        report = self.execute(DatasetTransport(records=[]))

        self.assertEqual(report.status, STATUS_OK)
        self.assertEqual(report.source["records_read"], 0)
        self.assertEqual(report.sink["inserted"], 0)
        self.assertEqual(report.final_cursor, 0)

    def test_retries_are_reported_not_buried(self) -> None:
        """Transient hiccups appear in the report and do not fail the run."""
        report = self.execute(
            DatasetTransport(records=dataset(), one_shot_failures={10: [503], 20: [429]})
        )

        self.assertEqual(report.status, STATUS_OK)
        self.assertEqual(report.source["retries"], 2)
        self.assertEqual([d["cursor"] for d in report.source["retry_detail"]], [10, 20])
        self.assertEqual(report.sink["inserted"], 27)

    def test_summary_line_mentions_the_decisive_numbers(self) -> None:
        """The one-line stdout summary is enough for a cron log."""
        report = self.execute(DatasetTransport(records=dataset()))
        line = report.summary_line()

        self.assertTrue(line.startswith("ok:"))
        for fragment in ("read 30", "inserted 27", "skipped 2", "duplicates 1", "cursor 30"):
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, line)


class IdempotencyTests(PipelineTestCase):
    """Running twice must not double anything."""

    def test_rerunning_a_completed_sync_writes_nothing(self) -> None:
        """The second run inserts zero rows and updates nothing."""
        self.execute(DatasetTransport(records=dataset()))
        second = self.execute(DatasetTransport(records=dataset()))

        self.assertEqual(second.status, STATUS_OK)
        self.assertEqual(
            second.sink,
            {
                "kind": "sqlite",
                "inserted": 0,
                "updated": 0,
                "unchanged": 27,
                "skipped_stale": 0,
            },
        )
        self.assertEqual(self.row_count(), 27)

    def test_changed_source_data_updates_rather_than_duplicating(self) -> None:
        """A modified record updates its row; the row count does not grow."""
        self.execute(DatasetTransport(records=dataset()))

        changed = dataset()
        changed[0]["phone"] = "+15550009999"
        report = self.execute(DatasetTransport(records=changed))

        self.assertEqual(
            report.sink,
            {
                "kind": "sqlite",
                "inserted": 0,
                "updated": 1,
                "unchanged": 26,
                "skipped_stale": 0,
            },
        )
        self.assertEqual(self.row_count(), 27)


class InterruptAndResumeTests(PipelineTestCase):
    """Losing the source mid-run must cost at most one page of work."""

    def test_interrupted_run_is_partial_and_checkpoints_committed_work(self) -> None:
        """Committed pages stay committed and the cursor persists."""
        report = self.execute(
            DatasetTransport(records=dataset(), fail_from_offset=20)
        )

        self.assertEqual(report.status, STATUS_PARTIAL)
        self.assertEqual(EXIT_CODES[report.status], 1)
        self.assertEqual(report.final_cursor, 20)
        # 20 records read; 1 blank email and 1 malformed email were skipped.
        self.assertEqual(report.sink["inserted"], 18)
        self.assertEqual(report.errors[0]["retryable"], "true")
        self.assertEqual(report.errors[0]["at_cursor"], "20")

        checkpoint = self.checkpoint()
        assert checkpoint is not None
        self.assertEqual(checkpoint.cursor, 20)
        self.assertEqual(checkpoint.pages_committed, 2)

    def test_resume_finishes_the_job_without_duplicating_anything(self) -> None:
        """Interrupted run plus resumed run equal exactly one clean run."""
        self.execute(DatasetTransport(records=dataset(), fail_from_offset=20))

        healthy = DatasetTransport(records=dataset())
        resumed = self.execute(healthy)

        self.assertEqual(resumed.status, STATUS_OK)
        self.assertEqual(resumed.resumed_from_cursor, 20)
        self.assertEqual(healthy.offsets_requested, [20])  # earlier pages not re-read
        # The last page holds 10 records, one of which is an outdated duplicate
        # of a record committed by the interrupted run. The deduper index does
        # not survive a restart, so the sink is what rejects it.
        self.assertEqual(resumed.sink["inserted"], 9)
        self.assertEqual(resumed.sink["skipped_stale"], 1)
        self.assertEqual(resumed.final_cursor, 30)
        self.assertEqual(self.row_count(), 27)  # same as one uninterrupted run
        self.assertIsNone(self.checkpoint())

    def test_replayed_page_costs_a_reread_not_duplicate_rows(self) -> None:
        """A checkpoint lost after a commit is survivable: at-least-once is safe."""
        self.execute(DatasetTransport(records=dataset(), fail_from_offset=20))

        # Simulate a crash between the sink commit and the checkpoint write by
        # rewinding the stored cursor by one page.
        store = CheckpointStore(self.config.run.state_path)
        store.save(
            Checkpoint(cursor=10, pages_committed=1, updated_at="", run_id="rewound")
        )

        replayed = self.execute(DatasetTransport(records=dataset()))

        self.assertEqual(replayed.status, STATUS_OK)
        self.assertEqual(replayed.sink["inserted"], 9)
        self.assertEqual(replayed.sink["skipped_stale"], 1)
        self.assertEqual(replayed.sink["unchanged"], 9)  # replayed page writes nothing
        self.assertEqual(self.row_count(), 27)

    def test_no_resume_ignores_the_checkpoint(self) -> None:
        """`--no-resume` re-reads from zero and still produces no duplicates."""
        self.execute(DatasetTransport(records=dataset(), fail_from_offset=20))

        transport = DatasetTransport(records=dataset())
        report = self.execute(transport, resume=False)

        self.assertIsNone(report.resumed_from_cursor)
        self.assertEqual(transport.offsets_requested, [0, 10, 20])
        # Re-reading from zero rebuilds the duplicate index, so the outdated
        # duplicate is dropped before the sink ever sees it.
        self.assertEqual(report.sink["inserted"], 9)
        self.assertEqual(report.sink["unchanged"], 18)
        self.assertEqual(report.sink["skipped_stale"], 0)

    def test_corrupt_checkpoint_does_not_wedge_the_pipeline(self) -> None:
        """An unreadable checkpoint means "start over", not "crash"."""
        self.config.run.state_path.write_text("{ not json", encoding="utf-8")
        report = self.execute(DatasetTransport(records=dataset()))

        self.assertEqual(report.status, STATUS_OK)
        self.assertIsNone(report.resumed_from_cursor)
        self.assertEqual(self.row_count(), 27)


class FailFastTests(PipelineTestCase):
    """Permanent failures must not be retried and must not look like success."""

    def test_permanent_failure_fails_fast_and_keeps_the_checkpoint(self) -> None:
        """A 401 is reported as failed, exit code 2, cursor left intact."""
        transport = QueueTransport(
            script=[
                json_response(200, {"users": dataset()[:10], "total": 30, "skip": 0}),
                json_response(401, {"error": "token expired"}),
            ]
        )
        report = self.execute(transport)

        self.assertEqual(report.status, STATUS_FAILED)
        self.assertEqual(EXIT_CODES[report.status], 2)
        self.assertEqual(report.errors[0]["retryable"], "false")
        self.assertEqual(transport.calls, 2)  # no retries spent on a permanent failure

        checkpoint = self.checkpoint()
        assert checkpoint is not None
        self.assertEqual(checkpoint.cursor, 10)


class ReportArtefactTests(PipelineTestCase):
    """The report is the deliverable, so it gets its own tests."""

    def test_report_is_written_and_machine_readable(self) -> None:
        """The report file parses as JSON and carries the documented keys."""
        report = self.execute(DatasetTransport(records=dataset()))
        path = report.write(self.config.run.report_path)

        payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema_version"], 1)
        self.assertEqual(payload["status"], STATUS_OK)
        self.assertEqual(
            set(payload),
            {
                "schema_version", "run_id", "status", "started_at", "finished_at",
                "duration_ms", "cursor", "source", "normalized", "sink", "errors",
            },
        )
        self.assertEqual(payload["cursor"], {"resumed_from": None, "final": 30})

    def test_report_is_written_even_when_the_run_fails(self) -> None:
        """A failed run still produces an artefact a scheduler can read."""
        report = self.execute(DatasetTransport(records=dataset(), fail_from_offset=0))
        path = report.write(self.config.run.report_path)

        payload = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(payload["status"], STATUS_PARTIAL)
        self.assertTrue(payload["errors"])

    def test_report_never_contains_the_source_token(self) -> None:
        """Secrets must not leak into an artefact people paste into tickets."""
        config = self.make_config(auth_env_var="DEMO_TOKEN")
        report = self.execute(
            DatasetTransport(records=dataset()),
            config=config,
            env={"DEMO_TOKEN": "super-secret-value"},
        )
        rendered = report.to_json()

        self.assertNotIn("super-secret-value", rendered)
        self.assertNotIn("DEMO_TOKEN", rendered)
