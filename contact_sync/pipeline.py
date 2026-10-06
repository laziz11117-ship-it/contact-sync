"""Orchestration: read a page, normalize it, commit it, checkpoint it.

The loop order is the whole safety argument:

    fetch page -> normalize -> sink.upsert (commits) -> checkpoint

A transient failure that outlives the retry policy stops the run at the last
committed page and reports `partial`; the next run resumes from there. A
permanent failure stops immediately and reports `failed`. Neither case
discards the checkpoint.
"""

from __future__ import annotations

import random
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from collections.abc import Callable, Mapping

from .config import Config
from .errors import ContactSyncError, PermanentError, TransientError
from .normalize import Deduper, normalize_batch
from .report import STATUS_FAILED, STATUS_OK, STATUS_PARTIAL, RunReport
from .sink import Sink, UpsertResult
from .source import PaginatedSource
from .state import Checkpoint, CheckpointStore
from .transport import Transport


def _utc_now() -> str:
    """Return the current time as an ISO-8601 UTC string."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def run_sync(
    config: Config,
    transport: Transport,
    sink: Sink,
    store: CheckpointStore,
    *,
    resume: bool = True,
    env: Mapping[str, str] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    rng: Callable[[], float] = random.random,
    monotonic: Callable[[], float] = time.monotonic,
    now: Callable[[], str] = _utc_now,
    run_id: str | None = None,
) -> RunReport:
    """Run one sync pass and return its report.

    Args:
        config: Validated configuration.
        transport: HTTP transport (injected, so tests stay offline).
        sink: Idempotent destination.
        store: Checkpoint store used for resume.
        resume: When False, ignore any existing checkpoint and start at 0.
        env: Environment mapping used to resolve the source token.
        sleep: Injected sleep for backoff.
        rng: Injected uniform [0, 1) source for jitter.
        monotonic: Injected monotonic clock for duration measurement.
        now: Injected wall-clock formatter for timestamps.
        run_id: Override the generated run identifier.

    Returns:
        A `RunReport`. The report is returned for every outcome, including
        failure; it is never raised instead of returned.
    """
    report = RunReport(
        run_id=run_id or uuid.uuid4().hex[:12],
        started_at=now(),
    )
    started = monotonic()

    checkpoint = store.load() if resume else None
    start_cursor = checkpoint.cursor if checkpoint else 0
    report.resumed_from_cursor = checkpoint.cursor if checkpoint else None
    cursor = start_cursor
    pages_committed = checkpoint.pages_committed if checkpoint else 0

    source = PaginatedSource(config.source, transport, env=env, sleep=sleep, rng=rng)
    deduper = Deduper()
    totals = UpsertResult()
    skip_counts: Counter[str] = Counter()
    valid = 0
    status = STATUS_OK

    try:
        for page in source.pages(start_cursor=start_cursor):
            batch = normalize_batch(page.records, config.mapping, deduper)
            skip_counts.update(batch.skip_counts())
            valid += len(batch.contacts)
            totals.add(sink.upsert(batch.contacts))
            cursor = page.cursor + len(page.records)
            pages_committed += 1
            store.save(
                Checkpoint(
                    cursor=cursor,
                    pages_committed=pages_committed,
                    updated_at=now(),
                    run_id=report.run_id,
                )
            )
    except TransientError as exc:
        # Retries were exhausted. Work already committed stays committed.
        status = STATUS_PARTIAL
        report.errors.append(
            {
                "type": type(exc).__name__,
                "message": str(exc),
                "at_cursor": str(cursor),
                "retryable": "true",
            }
        )
    except PermanentError as exc:
        status = STATUS_FAILED
        report.errors.append(
            {
                "type": type(exc).__name__,
                "message": str(exc),
                "at_cursor": str(cursor),
                "retryable": "false",
            }
        )
    except ContactSyncError as exc:  # pragma: no cover - defensive
        status = STATUS_FAILED
        report.errors.append(
            {"type": type(exc).__name__, "message": str(exc), "at_cursor": str(cursor)}
        )

    report.status = status
    report.final_cursor = cursor
    report.finished_at = now()
    report.duration_ms = int((monotonic() - started) * 1000)
    report.source = {
        "base_url": config.source.base_url,
        "page_size": config.source.page_size,
        "http_requests": source.stats.http_requests,
        "retries": source.stats.retries,
        "pages_fetched": source.stats.pages_fetched,
        "records_read": source.stats.records_read,
        "retry_detail": source.stats.retry_detail,
    }
    report.normalized = {
        "valid": valid,
        "duplicates_dropped": deduper.duplicates_dropped,
        "duplicates_superseded": deduper.superseded,
        "skipped_total": sum(skip_counts.values()),
        "skipped_by_reason": dict(sorted(skip_counts.items())),
    }
    report.sink = {"kind": config.sink.kind, **totals.as_dict()}

    if status == STATUS_OK:
        store.clear()
    return report
