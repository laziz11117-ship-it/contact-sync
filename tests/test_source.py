"""Pagination behaviour of the source reader."""

from __future__ import annotations

from typing import Any

from contact_sync.config import SourceConfig
from contact_sync.errors import PermanentError, PermanentHttpError, TransientError
from contact_sync.retry import RetryPolicy
from contact_sync.source import PaginatedSource, get_path
from tests.fakes import DatasetTransport, QueueTransport, json_response
from tests.support import NoNetworkTestCase

POLICY = RetryPolicy(max_attempts=3, initial_backoff_seconds=0.1, jitter=0.0)


def dataset(count: int) -> list[dict[str, Any]]:
    """Build `count` trivial records."""
    return [{"id": i, "email": f"u{i}@x.example"} for i in range(1, count + 1)]


def config(**overrides: Any) -> SourceConfig:
    """Build a source config pointed at a non-resolvable host."""
    base: dict[str, Any] = dict(
        base_url="http://fake.invalid/users",
        records_path="users",
        total_path="total",
        limit_param="limit",
        offset_param="skip",
        page_size=10,
        retry=POLICY,
    )
    base.update(overrides)
    return SourceConfig(**base)


def source(transport: Any, **overrides: Any) -> PaginatedSource:
    """Build a source with sleeping and jitter neutralised."""
    return PaginatedSource(
        config(**overrides), transport, env={}, sleep=lambda _: None, rng=lambda: 0.5
    )


class PaginationTests(NoNetworkTestCase):
    """Walking the pages."""

    def test_pages_walk_the_whole_dataset(self) -> None:
        """Offsets advance by page size and stop once the total is reached."""
        transport = DatasetTransport(records=dataset(25))
        reader = source(transport)

        pages = list(reader.pages())

        self.assertEqual(transport.offsets_requested, [0, 10, 20])
        self.assertEqual([len(p.records) for p in pages], [10, 10, 5])
        self.assertEqual(reader.stats.records_read, 25)
        self.assertEqual(reader.stats.pages_fetched, 3)
        self.assertEqual(reader.stats.http_requests, 3)

    def test_resume_starts_at_the_given_cursor(self) -> None:
        """A start cursor skips pages an earlier run already committed."""
        transport = DatasetTransport(records=dataset(25))
        reader = source(transport)

        pages = list(reader.pages(start_cursor=10))

        self.assertEqual(transport.offsets_requested, [10, 20])
        self.assertEqual([p.records[0]["id"] for p in pages], [11, 21])

    def test_source_without_a_total_stops_on_the_first_empty_page(self) -> None:
        """A source that reports no total needs one extra request to find the end."""
        transport = DatasetTransport(records=dataset(20), include_total=False)
        reader = source(transport, total_path=None)

        pages = list(reader.pages())

        self.assertEqual(transport.offsets_requested, [0, 10, 20])
        self.assertEqual([len(p.records) for p in pages], [10, 10])

    def test_max_pages_limits_the_run(self) -> None:
        """max_pages caps requests, for smoke tests against a large source."""
        transport = DatasetTransport(records=dataset(100))
        reader = source(transport, max_pages=2)

        self.assertEqual(len(list(reader.pages())), 2)
        self.assertEqual(transport.offsets_requested, [0, 10])

    def test_existing_query_string_is_preserved(self) -> None:
        """Paging parameters are merged into the URL, not substituted for it."""
        transport = DatasetTransport(records=dataset(5))
        reader = source(transport, base_url="http://fake.invalid/users?active=true")
        list(reader.pages())
        self.assertEqual(transport.requests, [(0, 10)])


class FailureHandlingTests(NoNetworkTestCase):
    """How the reader reacts to each class of failure."""

    def test_transient_page_failure_is_retried_and_counted(self) -> None:
        """A 503 on one page is retried, the run completes, the report says so."""
        transport = DatasetTransport(records=dataset(20), one_shot_failures={10: [503]})
        reader = source(transport)

        pages = list(reader.pages())

        self.assertEqual([len(p.records) for p in pages], [10, 10])
        self.assertEqual(transport.offsets_requested, [0, 10, 10])
        self.assertEqual(reader.stats.retries, 1)
        self.assertEqual(reader.stats.http_requests, 3)
        self.assertEqual(reader.stats.retry_detail[0]["cursor"], 10)

    def test_permanent_page_failure_aborts_without_retrying(self) -> None:
        """A 401 stops the read immediately instead of retrying three times."""
        transport = QueueTransport(script=[json_response(401, {"error": "bad token"})])
        reader = source(transport)

        with self.assertRaises(PermanentHttpError):
            list(reader.pages())
        self.assertEqual(transport.calls, 1)

    def test_exhausted_retries_surface_as_transient(self) -> None:
        """When a page never recovers, the error stays classified as retryable."""
        transport = DatasetTransport(records=dataset(20), fail_from_offset=0)
        reader = source(transport)

        with self.assertRaises(TransientError):
            list(reader.pages())
        self.assertEqual(reader.stats.http_requests, POLICY.max_attempts)

    def test_unexpected_payload_shape_is_a_permanent_error(self) -> None:
        """A schema mismatch is the caller's problem, not something to retry."""
        transport = QueueTransport(
            script=[json_response(200, {"users": {"not": "a list"}})]
        )
        reader = source(transport)

        with self.assertRaises(PermanentError) as caught:
            list(reader.pages())
        self.assertIn("expected a list", str(caught.exception))

    def test_non_json_body_is_retried_then_reported_as_transient(self) -> None:
        """A gateway interstitial on a 200 is retried, as documented."""
        from contact_sync.transport import Response

        transport = QueueTransport(
            script=[Response(200, {}, b"<html>502 Bad Gateway</html>")] * 3
        )
        reader = source(transport)

        with self.assertRaises(TransientError):
            list(reader.pages())
        self.assertEqual(transport.calls, 3)


class AuthTests(NoNetworkTestCase):
    """Credentials come from the environment, never from the repository."""

    def test_bearer_token_is_read_from_the_environment(self) -> None:
        """A configured env var becomes an Authorization header."""
        transport = DatasetTransport(records=dataset(5))
        reader = PaginatedSource(
            config(auth_env_var="DEMO_TOKEN"),
            transport,
            env={"DEMO_TOKEN": "s3cret"},
            sleep=lambda _: None,
            rng=lambda: 0.5,
        )
        list(reader.pages())
        self.assertEqual(transport.seen_headers[0]["Authorization"], "Bearer s3cret")

    def test_no_auth_header_when_the_variable_is_unset(self) -> None:
        """An unauthenticated source gets no empty Bearer header."""
        transport = DatasetTransport(records=dataset(5))
        reader = PaginatedSource(
            config(auth_env_var="DEMO_TOKEN"),
            transport,
            env={},
            sleep=lambda _: None,
            rng=lambda: 0.5,
        )
        list(reader.pages())
        self.assertNotIn("Authorization", transport.seen_headers[0])


class GetPathTests(NoNetworkTestCase):
    """Dotted lookups tolerate missing keys instead of raising."""

    def test_get_path(self) -> None:
        """Present, nested, and absent paths."""
        record = {"a": 1, "b": {"c": 2}}
        for path, expected in [("a", 1), ("b.c", 2), ("b.missing", None), ("nope.deep", None)]:
            with self.subTest(path=path):
                self.assertEqual(get_path(record, path), expected)
