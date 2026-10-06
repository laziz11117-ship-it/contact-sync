"""Behaviour of status classification and exponential backoff."""

from __future__ import annotations

from typing import Any

from contact_sync.errors import (
    PermanentHttpError,
    TransientError,
    TransientHttpError,
    TransportError,
)
from contact_sync.retry import (
    RetryPolicy,
    parse_retry_after,
    raise_for_status,
    retry_call,
)
from tests.support import NoNetworkTestCase

HALF = 0.5  # rng() == 0.5 cancels jitter, which makes delays deterministic


def policy(**overrides: Any) -> RetryPolicy:
    """Build a retry policy with jitter off unless a test asks for it."""
    base: dict[str, Any] = dict(
        max_attempts=4,
        initial_backoff_seconds=0.5,
        max_backoff_seconds=8.0,
        multiplier=2.0,
        jitter=0.0,
    )
    base.update(overrides)
    return RetryPolicy(**base)


class StatusClassificationTests(NoNetworkTestCase):
    """Every status falls on exactly one side of the transient/permanent split."""

    def test_transient_statuses(self) -> None:
        """Statuses that mean "the server is having a moment" are retryable."""
        for status in (408, 425, 429, 500, 502, 503, 504):
            with self.subTest(status=status):
                with self.assertRaises(TransientHttpError):
                    raise_for_status(status, "http://x/y", {}, "body")

    def test_permanent_statuses(self) -> None:
        """Statuses that mean "your request is wrong" are not retryable."""
        for status in (400, 401, 403, 404, 405, 409, 410, 422):
            with self.subTest(status=status):
                with self.assertRaises(PermanentHttpError):
                    raise_for_status(status, "http://x/y", {}, "body")

    def test_success_statuses_do_not_raise(self) -> None:
        """2xx passes through untouched."""
        for status in (200, 201, 204, 299):
            with self.subTest(status=status):
                raise_for_status(status, "http://x/y", {})

    def test_error_message_carries_status_and_url(self) -> None:
        """The message is what ends up in the run report, so it must be useful."""
        with self.assertRaises(PermanentHttpError) as caught:
            raise_for_status(404, "http://x/users?skip=10", {}, "no such collection")
        message = str(caught.exception)
        self.assertIn("404", message)
        self.assertIn("http://x/users?skip=10", message)
        self.assertIn("no such collection", message)


class BackoffTests(NoNetworkTestCase):
    """Delay computation."""

    def test_backoff_is_geometric_and_capped(self) -> None:
        """Delays double from the initial value and stop at the ceiling."""
        subject = policy(max_attempts=8, max_backoff_seconds=4.0)
        delays = [subject.delay_for(n, lambda: HALF) for n in range(1, 7)]
        self.assertEqual(delays, [0.5, 1.0, 2.0, 4.0, 4.0, 4.0])

    def test_jitter_stays_inside_the_configured_band(self) -> None:
        """Jitter perturbs a delay by at most the configured fraction."""
        subject = policy(jitter=0.2)
        self.assertAlmostEqual(subject.delay_for(1, lambda: 0.0), 0.4)
        self.assertAlmostEqual(subject.delay_for(1, lambda: HALF), 0.5)
        self.assertAlmostEqual(subject.delay_for(1, lambda: 0.999), 0.5999, places=3)

    def test_parse_retry_after(self) -> None:
        """Seconds form is parsed; HTTP-date form is ignored (documented limitation)."""
        cases: list[tuple[str | None, float | None]] = [
            ("5", 5.0),
            ("0", 0.0),
            (" 2.5 ", 2.5),
            (None, None),
            ("", None),
            ("Wed, 21 Oct 2026 07:28:00 GMT", None),
            ("-1", None),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(parse_retry_after(raw), expected)


class RetryCallTests(NoNetworkTestCase):
    """The retry loop itself."""

    def test_transient_failures_are_retried_until_success(self) -> None:
        """Two transient failures then success: three calls, two sleeps."""
        attempts: list[int] = []
        slept: list[float] = []

        def operation() -> str:
            attempts.append(1)
            if len(attempts) < 3:
                raise TransportError("connection reset")
            return "done"

        result = retry_call(operation, policy(), sleep=slept.append, rng=lambda: HALF)

        self.assertEqual(result, "done")
        self.assertEqual(len(attempts), 3)
        self.assertEqual(slept, [0.5, 1.0])

    def test_permanent_failure_is_not_retried(self) -> None:
        """A permanent error aborts on the first attempt and never sleeps."""
        attempts: list[int] = []
        slept: list[float] = []

        def operation() -> None:
            attempts.append(1)
            raise PermanentHttpError(404, "http://x/y")

        with self.assertRaises(PermanentHttpError):
            retry_call(operation, policy(), sleep=slept.append, rng=lambda: HALF)

        self.assertEqual(len(attempts), 1)
        self.assertEqual(slept, [])

    def test_exhausted_retries_raise_the_last_transient_error(self) -> None:
        """max_attempts bounds the call count, and the final error surfaces."""
        attempts: list[int] = []
        slept: list[float] = []

        def operation() -> None:
            attempts.append(1)
            raise TransientHttpError(503, "http://x/y", f"failure {len(attempts)}")

        with self.assertRaises(TransientError) as caught:
            retry_call(
                operation, policy(max_attempts=4), sleep=slept.append, rng=lambda: HALF
            )

        self.assertIn("failure 4", str(caught.exception))
        self.assertEqual(len(attempts), 4)
        self.assertEqual(slept, [0.5, 1.0, 2.0])

    def test_single_attempt_policy_never_sleeps(self) -> None:
        """max_attempts=1 disables retrying without disabling the pipeline."""
        slept: list[float] = []
        calls: list[int] = []

        def operation() -> None:
            calls.append(1)
            raise TransportError("boom")

        with self.assertRaises(TransientError):
            retry_call(
                operation, policy(max_attempts=1), sleep=slept.append, rng=lambda: HALF
            )
        self.assertEqual(len(calls), 1)
        self.assertEqual(slept, [])

    def test_retry_after_header_overrides_computed_backoff(self) -> None:
        """A server-supplied Retry-After wins over the geometric delay."""
        slept: list[float] = []
        attempts: list[int] = []

        def operation() -> str:
            attempts.append(1)
            if len(attempts) == 1:
                raise TransientHttpError(
                    429, "http://x/y", "slow down", retry_after_seconds=3.0
                )
            return "ok"

        self.assertEqual(
            retry_call(operation, policy(), sleep=slept.append, rng=lambda: HALF), "ok"
        )
        self.assertEqual(slept, [3.0])

    def test_retry_after_is_capped_by_max_backoff(self) -> None:
        """An absurd Retry-After cannot park the run for an hour."""
        slept: list[float] = []

        def operation() -> None:
            raise TransientHttpError(429, "http://x/y", retry_after_seconds=3600.0)

        with self.assertRaises(TransientError):
            retry_call(
                operation,
                policy(max_attempts=2, max_backoff_seconds=8.0),
                sleep=slept.append,
                rng=lambda: HALF,
            )
        self.assertEqual(slept, [8.0])

    def test_on_retry_receives_attempt_delay_and_error(self) -> None:
        """This callback is what populates the run report's retry detail."""
        seen: list[tuple[int, float, str]] = []
        attempts: list[int] = []

        def operation() -> str:
            attempts.append(1)
            if len(attempts) == 1:
                raise TransportError("timeout")
            return "ok"

        retry_call(
            operation,
            policy(),
            sleep=lambda _: None,
            rng=lambda: HALF,
            on_retry=lambda n, delay, error: seen.append((n, delay, str(error))),
        )
        self.assertEqual(seen, [(1, 0.5, "timeout")])
