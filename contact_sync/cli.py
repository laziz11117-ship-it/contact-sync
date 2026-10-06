"""Command line entry point.

Two subcommands:

* `run`   — sync using a JSON config file.
* `demo`  — sync against the bundled local flaky API, no credentials needed.

Exit code is derived from the run status: 0 ok, 1 partial (resumable),
2 failed (needs a human).
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from collections.abc import Sequence

from .config import Config, FieldMapping, RunConfig, SinkConfig, SourceConfig, load_config
from .demo_server import DemoServer, DemoState
from .errors import ContactSyncError
from .pipeline import run_sync
from .report import EXIT_CODES, RunReport
from .retry import RetryPolicy
from .sink import MemorySink, Sink, SqliteSink
from .state import CheckpointStore
from .transport import UrllibTransport

DEMO_PAGE_SIZE = 25


def _emit(report: RunReport, report_path: Path, as_json: bool) -> int:
    """Write the report, print it, and return the process exit code."""
    report.write(report_path)
    if as_json:
        sys.stdout.write(report.to_json())
    else:
        sys.stdout.write(report.summary_line() + "\n")
        sys.stdout.write(f"report: {report_path}\n")
        for error in report.errors:
            sys.stdout.write(f"error: {error['type']}: {error['message']}\n")
    return EXIT_CODES.get(report.status, 2)


def _build_sink(config: Config, dry_run: bool) -> Sink:
    """Return the configured sink, or an in-memory one for a dry run."""
    return MemorySink() if dry_run else SqliteSink(config.sink.path)


def _demo_config(data_dir: Path, base_url: str) -> Config:
    """Build the configuration used by the demo, pointed at the local server."""
    return Config(
        source=SourceConfig(
            base_url=base_url,
            records_path="users",
            total_path="total",
            limit_param="limit",
            offset_param="skip",
            page_size=DEMO_PAGE_SIZE,
            timeout_seconds=5.0,
            retry=RetryPolicy(
                max_attempts=4,
                initial_backoff_seconds=0.2,
                max_backoff_seconds=2.0,
                multiplier=2.0,
                jitter=0.2,
            ),
        ),
        mapping=FieldMapping(
            external_id="id",
            email="email",
            first_name="firstName",
            last_name="lastName",
            phone="phone",
            company="company.name",
            updated_at="updatedAt",
        ),
        sink=SinkConfig(kind="sqlite", path=data_dir / "contacts.sqlite3"),
        run=RunConfig(
            state_path=data_dir / "state.json",
            report_path=data_dir / "last-run.json",
        ),
    )


def cmd_run(args: argparse.Namespace) -> int:
    """Execute the `run` subcommand."""
    config = load_config(args.config)
    sink = _build_sink(config, args.dry_run)
    try:
        report = run_sync(
            config,
            UrllibTransport(),
            sink,
            CheckpointStore(config.run.state_path),
            resume=not args.no_resume,
        )
    finally:
        sink.close()
    return _emit(report, config.run.report_path, args.json)


def cmd_demo(args: argparse.Namespace) -> int:
    """Execute the `demo` subcommand against the bundled local API."""
    data_dir = Path(args.data_dir)
    if not args.resume and data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    state = DemoState.default(
        page_size=DEMO_PAGE_SIZE,
        interrupt_at=args.interrupt_at,
    )
    if args.resume:
        # A resumed run should not replay the one-shot failures of the first run.
        state.one_shot_failures = {}

    with DemoServer(state) as server:
        config = _demo_config(data_dir, server.base_url)
        sink = _build_sink(config, args.dry_run)
        try:
            report = run_sync(
                config,
                UrllibTransport(),
                sink,
                CheckpointStore(config.run.state_path),
                resume=args.resume,
            )
        finally:
            sink.close()
        return _emit(report, config.run.report_path, args.json)


def build_parser() -> argparse.ArgumentParser:
    """Construct the argument parser."""
    parser = argparse.ArgumentParser(
        prog="contact-sync",
        description="Resumable, idempotent one-way sync from a paginated REST API.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="sync using a JSON config file")
    run_parser.add_argument("--config", default="config.json", help="path to config.json")
    run_parser.add_argument(
        "--no-resume", action="store_true", help="ignore any existing checkpoint"
    )
    run_parser.add_argument("--json", action="store_true", help="print the full JSON report")
    run_parser.add_argument(
        "--dry-run", action="store_true", help="read and normalize, write nothing"
    )
    run_parser.set_defaults(func=cmd_run)

    demo_parser = sub.add_parser("demo", help="sync against the bundled local flaky API")
    demo_parser.add_argument(
        "--data-dir", default="data/demo", help="where the sqlite file, state and report go"
    )
    demo_parser.add_argument(
        "--interrupt-at",
        type=int,
        default=None,
        metavar="OFFSET",
        help="make the fake API fail permanently from this record offset",
    )
    demo_parser.add_argument(
        "--resume",
        action="store_true",
        help="keep the existing database and continue from the checkpoint",
    )
    demo_parser.add_argument("--json", action="store_true", help="print the full JSON report")
    demo_parser.add_argument(
        "--dry-run", action="store_true", help="read and normalize, write nothing"
    )
    demo_parser.set_defaults(func=cmd_demo)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Parse arguments, dispatch, and map errors to exit codes."""
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except ContactSyncError as exc:
        sys.stderr.write(f"{type(exc).__name__}: {exc}\n")
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
