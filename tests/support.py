"""Shared test scaffolding.

`NoNetworkTestCase` replaces the socket constructors with functions that
raise, for the duration of every test. If any test ever reaches the real
network it fails here, loudly, instead of becoming an intermittent failure
on someone else's machine.
"""

from __future__ import annotations

import socket
import unittest
from unittest import mock


class NetworkAccessDenied(RuntimeError):
    """Raised when a test attempts a real socket connection."""


def _deny(*args: object, **kwargs: object) -> None:
    """Stand-in for socket constructors."""
    raise NetworkAccessDenied("tests must not touch the network")


class NoNetworkTestCase(unittest.TestCase):
    """Base class that makes real socket use impossible inside a test."""

    def setUp(self) -> None:
        """Patch out the socket constructors for this test."""
        super().setUp()
        for target in ("socket", "create_connection"):
            patcher = mock.patch.object(socket, target, _deny)
            patcher.start()
            self.addCleanup(patcher.stop)
