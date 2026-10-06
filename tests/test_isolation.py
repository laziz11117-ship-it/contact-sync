"""Guards that keep the suite trustworthy."""

from __future__ import annotations

import socket

from contact_sync.demo_server import build_dataset
from tests.support import NetworkAccessDenied, NoNetworkTestCase


class NetworkGuardTests(NoNetworkTestCase):
    """Proves the guard is active, so "no network in tests" is not a claim."""

    def test_socket_construction_is_blocked(self) -> None:
        """Any attempt to open a socket inside a guarded test fails."""
        with self.assertRaises(NetworkAccessDenied):
            socket.socket()
        with self.assertRaises(NetworkAccessDenied):
            socket.create_connection(("example.com", 80))


class DemoDatasetTests(NoNetworkTestCase):
    """The demo fixture must be deterministic or the README numbers drift."""

    def test_dataset_is_deterministic_and_defective_on_purpose(self) -> None:
        """Same data every time, with a known number of planted defects."""
        first = build_dataset()
        self.assertEqual(first, build_dataset())
        self.assertEqual(len(first), 120)
        self.assertEqual(sum(1 for r in first if not r["email"]), 3)
        self.assertEqual(sum(1 for r in first if r["email"] == "not-an-email"), 2)
        # 5 records were given an email that already belongs to another record,
        # so 5 distinct addresses disappear from the set.
        well_formed = [
            r["email"] for r in first if r["email"] and r["email"] != "not-an-email"
        ]
        self.assertEqual(len(well_formed), 115)
        self.assertEqual(len(set(well_formed)), 110)
