# contact-sync

A small, complete one-way sync between a paginated REST API and a local
sink, written the way an integration has to be written if nobody is going to
babysit it: **retries only where retrying helps, idempotent writes, resume
from the last committed page, and a machine-readable report of every run.**

It is deliberately small. It is not a framework. It is a working example of
the parts that usually get skipped when a custom API integration is built
under time pressure — and the parts that cause the 2am pages afterwards.

---

## The problem it solves

You need records out of system A and into system B on a schedule. The naive
version works on the happy path and then:

- the source rate-limits you halfway through and the job dies;
- someone re-runs it and you get duplicates;
- it dies again and you have no idea which records made it;
- the source returns a 401 and the job spends twelve minutes retrying it;
- a week later someone asks "did it actually sync everything?" and the only
  answer is 4,000 lines of stdout.

This repository is the answer to those five questions, in ~1,700 lines of
annotated Python (docstrings included) and 78 tests.

---

## Run it

Python 3.10+. **No dependencies, nothing to install, no credentials.**

```
python -m unittest discover -s tests -t .
python -m contact_sync demo
```

The `demo` command starts a small fake "CRM API" on localhost inside the same
process, then syncs 120 records out of it. The fake API is deliberately
hostile: its data contains blank emails, malformed emails and duplicate
emails, and it injects a `503` on one page and a `429` with `Retry-After` on
another. Nothing leaves your machine.

### Actual output

```
$ python -V
Python 3.10.12

$ python -m unittest discover -s tests -t .
..............................................................................
----------------------------------------------------------------------
Ran 78 tests in 4.670s

OK

$ python -m contact_sync demo
ok: read 120 | inserted 110 updated 3 unchanged 0 stale 0 | skipped 5 duplicates 2 | retries 2 | cursor 120 | 1430 ms
report: data/demo/last-run.json
```

Read that summary line as: 120 records read, 5 rejected as unusable, 2
dropped as outdated duplicates, 113 written (110 new rows plus 3 rows
replaced by a newer duplicate of the same contact), 2 transient HTTP
failures absorbed by retries, finished in 1.4 seconds.

---

## Interruption, resume and idempotency

The same three runs the tests assert, run from a shell:

```
$ python -m contact_sync demo --interrupt-at 50
partial: read 50 | inserted 45 updated 2 unchanged 0 stale 0 | skipped 3 duplicates 0 | retries 4 | cursor 50 | 1701 ms
report: data/demo/last-run.json
error: TransientHttpError: HTTP 503 from http://127.0.0.1:39345/users?limit=25&skip=50: {"error": "upstream unavailable"}
exit code: 1

$ cat data/demo/state.json
{
  "cursor": 50,
  "pages_committed": 2,
  "run_id": "746690e79509",
  "updated_at": "2026-10-06T00:17:11.835+00:00"
}

$ python -m contact_sync demo --resume
ok: read 70 | inserted 65 updated 1 unchanged 0 stale 1 | skipped 2 duplicates 1 | retries 0 | cursor 120 | 107 ms
exit code: 0

$ python -m contact_sync demo --resume
ok: read 120 | inserted 0 updated 0 unchanged 110 stale 3 | skipped 5 duplicates 2 | retries 0 | cursor 120 | 69 ms
exit code: 0
```

Four things to notice:

1. The interrupted run **kept** the 45 records it had already committed, exited
   **1** (resumable), and left a cursor behind.
2. The resumed run read **70** records, not 120. It did not re-fetch pages the
   first run had already committed.
3. 45 + 65 = **110** rows — exactly what an uninterrupted run produces. No
   duplicates, no gaps.
4. The final run wrote **nothing**: 110 unchanged. Re-running a completed sync
   is free and safe, which is what makes it safe to put on a cron.

```
$ python -c "import sqlite3; print(sqlite3.connect('data/demo/contacts.sqlite3').execute('SELECT COUNT(*) FROM contacts').fetchone()[0])"
110
```

---

## The run report

Every run writes a JSON report, including a failed one. This is the artefact
a scheduler, a dashboard or a support ticket consumes — not stdout.

```json
{
  "schema_version": 1,
  "run_id": "44012adf1cb6",
  "status": "ok",
  "started_at": "2026-10-06T00:16:47.840+00:00",
  "finished_at": "2026-10-06T00:16:49.301+00:00",
  "duration_ms": 1461,
  "cursor": {
    "resumed_from": null,
    "final": 120
  },
  "source": {
    "base_url": "http://127.0.0.1:37389/users",
    "page_size": 25,
    "http_requests": 7,
    "retries": 2,
    "pages_fetched": 5,
    "records_read": 120,
    "retry_detail": [
      {
        "cursor": 25,
        "attempt": 1,
        "sleep_seconds": 0.193,
        "error": "TransientHttpError: HTTP 503 from http://127.0.0.1:37389/users?limit=25&skip=25: {\"error\": \"try again\"}"
      },
      {
        "cursor": 75,
        "attempt": 1,
        "sleep_seconds": 1.0,
        "error": "TransientHttpError: HTTP 429 from http://127.0.0.1:37389/users?limit=25&skip=75: {\"error\": \"try again\"}"
      }
    ]
  },
  "normalized": {
    "valid": 113,
    "duplicates_dropped": 2,
    "duplicates_superseded": 3,
    "skipped_total": 5,
    "skipped_by_reason": {
      "invalid_email": 2,
      "missing_email": 3
    }
  },
  "sink": {
    "kind": "sqlite",
    "inserted": 110,
    "updated": 3,
    "unchanged": 0,
    "skipped_stale": 0
  },
  "errors": []
}
```

The arithmetic reconciles, and a test enforces it:

```
records_read == valid + duplicates_dropped + skipped_total
```

So "why are there 110 rows when the source has 120 records?" is answered by
the report, by reason, without reading a log or the code.

A failed run reports the same shape:

```json
{
  "status": "partial",
  "cursor": { "resumed_from": null, "final": 50 },
  "errors": [
    {
      "type": "TransientHttpError",
      "message": "HTTP 503 from http://127.0.0.1:34953/users?limit=25&skip=50: {\"error\": \"upstream unavailable\"}",
      "at_cursor": "50",
      "retryable": "true"
    }
  ]
}
```

Exit codes: `0` ok, `1` partial (safe to re-run, it will resume), `2` failed
(a human needs to look — bad credentials, bad schema, bad config).

---

## How it behaves under failure

| Situation | Behaviour |
|---|---|
| `429`, `500`, `502`, `503`, `504`, `408`, `425` | Retried, geometric backoff with jitter, capped |
| `Retry-After` header present | Honoured, capped at `max_backoff_seconds` |
| `400`, `401`, `403`, `404`, `422`, … | **Not** retried. Run fails immediately, exit 2 |
| Connection reset / DNS / timeout | Retried as transient |
| Unexpected response schema | Permanent. Fails fast rather than looping |
| Retries exhausted mid-run | Status `partial`, exit 1, committed pages kept, cursor saved |
| Process killed mid-run | Next run resumes from the last committed page |
| Checkpoint lost after a commit | One page is re-read and written as `unchanged` — at-least-once delivery, made harmless by the idempotent sink |
| Corrupt checkpoint file | Ignored, run starts from zero, no duplicates |
| Record with blank / malformed email | Skipped and counted by reason, run continues |
| Same contact seen twice | Newest `updated_at` wins; the other is counted |
| An older version of a record arrives late | Rejected by the sink as `skipped_stale` |

The loop order is the whole safety argument:

```
fetch page  ->  normalize  ->  sink.upsert (commits)  ->  write checkpoint
```

The checkpoint is only written after the sink has committed the page it
describes. A crash can therefore cost one replayed page, never a lost one —
and a replayed page is a no-op because `dedupe_key` is the primary key and a
row whose content hash already matches is not rewritten.

---

## Tests

78 tests, stdlib `unittest`, run in about 5 seconds. They test behaviour, not
implementation:

| File | What it pins down |
|---|---|
| `test_retry.py` | Status classification table, geometric backoff sequence, jitter bounds, `Retry-After` handling and capping, permanent errors never retried, attempt counts |
| `test_normalize.py` | Canonicalisation, every skip reason, duplicate resolution in both directions, dedupe across page boundaries, hash sensitivity |
| `test_sink.py` | Insert / update / unchanged / stale, replay safety, durability across reopen — the same contract applied to both sink implementations |
| `test_source.py` | Offset walking, start cursor, sources without a total, `max_pages`, retry accounting, auth header from the environment |
| `test_pipeline.py` | Report arithmetic, checkpoint lifecycle, interrupt → resume → re-run, `--no-resume`, corrupt checkpoint, fail-fast, no token in the report |
| `test_end_to_end.py` | The three runs above, over real HTTP against the in-process demo API, with the real `urllib` transport |
| `test_config.py` | Config validation, unknown-key rejection, the shipped example config, repository hygiene |
| `test_isolation.py` | Proves the network guard is active and the demo fixture is deterministic |

**No test touches the network.** Every test except `test_end_to_end.py`
inherits `NoNetworkTestCase`, which replaces the socket constructors with
functions that raise; `test_isolation.py` asserts that the guard actually
works, so this is a checked property rather than a claim. The external API is
replaced at a single seam — the `Transport` protocol — so the real retry,
pagination, normalization, sink and checkpoint code all run in tests.
`test_end_to_end.py` is the one exception and it only talks to a server
started inside the same process on `127.0.0.1`.

`pytest` also runs the suite if you prefer it.

---

## Pointing it at a real API

```
cp config.example.json config.json
# edit config.json
export CONTACT_SYNC_SOURCE_TOKEN=...   # only if your source needs one
python -m contact_sync run --config config.json
```

`config.example.json` is the only configuration surface. Keys starting with
`_` are ignored, which is how that file carries its own comments. Unknown
keys are **rejected** at load time, so `page_sze` fails immediately instead
of silently using the default.

Credentials are never stored in the repository. `source.auth_env_var` *names*
an environment variable; the value is read at request time, is never logged,
and a test asserts it never reaches the run report.

Useful flags: `--dry-run` (read and normalize, write nothing), `--no-resume`
(ignore the checkpoint), `--json` (print the full report to stdout).

---

## Layout

```
contact_sync/
  transport.py     the only place this package touches a socket
  retry.py         transient/permanent classification, backoff
  source.py        offset/limit pagination
  normalize.py     mapping, validation, deduplication
  sink.py          idempotent destination (sqlite + in-memory)
  state.py         atomic resume checkpoint
  report.py        machine-readable run report
  pipeline.py      the loop that makes the guarantees hold
  config.py        typed, validated configuration
  cli.py           run / demo
  demo_server.py   local fake API: paginated, flaky, deterministic
tests/             78 tests
config.example.json
```

Fully type-annotated, docstrings throughout. Dependency injection is used
only where it buys testability: `sleep`, the jitter source, the clock, the
transport and the sink are all parameters, which is why the suite can assert
on an exact backoff sequence in milliseconds.

---

## Limitations

Stated plainly, because the point of this repository is that you can trust
what it says about itself.

- **Offset/limit pagination only.** Opaque cursor and `Link`-header pagination
  are not implemented. The source reader is one small class; adding a cursor
  strategy is a contained change, but it is not here.
- **One-way.** Source to sink. No conflict resolution, no write-back, no
  deletion propagation — a record removed at the source stays in the sink.
- **Full scan each run, not incremental.** There is no `updated_since` filter,
  so a complete run reads every record. Fine at this scale, wrong at a
  million records; the fix is a watermark in the checkpoint file.
- **SQLite sink only.** A real engagement would write to the customer's actual
  destination API. `Sink` is a two-method protocol and `MemorySink` shows the
  second implementation, but an HTTP sink would need its own idempotency key
  handling, and that is destination-specific.
- **Checkpointing is page-granular, not record-granular.** A crash mid-page
  replays that page. Safe, but it means delivery is at-least-once rather than
  exactly-once.
- **The duplicate index lives in memory for the duration of a run.** A resumed
  run starts with an empty index, so cross-run duplicate resolution falls to
  the sink's stale-write check. That is why the sink compares
  `source_updated_at` — and why resuming produces the same 110 rows. It does
  mean the two mechanisms have to agree about what "newer" means.
- **Dedupe key is the email address.** Correct for contact data, wrong for
  anything else. It is a single function, but it is not configurable.
- **`Retry-After` in HTTP-date form is ignored**, only the seconds form is
  parsed. Deliberate, tested, and recorded here rather than discovered later.
- **A non-JSON body on a `200` is treated as transient.** In practice that is a
  proxy interstitial and retrying is right; a source that genuinely serves
  malformed JSON will fail the run after retries are exhausted rather than
  failing fast.
- **Single-process, no concurrency.** Pages are fetched serially. Parallel page
  fetching would break the "checkpoint after commit" ordering that the resume
  guarantee depends on, so it was left out rather than bolted on.
- **The demo API is local and synthetic.** It mimics the shape of a typical
  "list contacts" endpoint and injects realistic failures, but it is not a
  stand-in for any specific vendor's quirks.

## License

MIT.
