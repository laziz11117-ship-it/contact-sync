"""Normalization, validation and deduplication behaviour."""

from __future__ import annotations

from typing import Any

from contact_sync.config import FieldMapping
from contact_sync.normalize import (
    REASON_INVALID_EMAIL,
    REASON_MISSING_EMAIL,
    REASON_MISSING_EXTERNAL_ID,
    REASON_MISSING_NAME,
    Contact,
    Deduper,
    Skipped,
    normalize_batch,
    normalize_phone,
    normalize_record,
)
from tests.support import NoNetworkTestCase

MAPPING = FieldMapping(
    external_id="id",
    email="email",
    first_name="firstName",
    last_name="lastName",
    phone="phone",
    company="company.name",
    updated_at="updatedAt",
)


def record(**overrides: Any) -> dict[str, Any]:
    """Build a valid raw source record, overriding selected fields."""
    base: dict[str, Any] = {
        "id": 1,
        "firstName": "Ada",
        "lastName": "Lovelace",
        "email": "  Ada.Lovelace@Example.Invalid ",
        "phone": "+1 (555) 010-2030",
        "company": {"name": "Northwind"},
        "updatedAt": "2026-01-01T00:00:00Z",
    }
    base.update(overrides)
    return base


class NormalizeRecordTests(NoNetworkTestCase):
    """One raw record in, one canonical contact or one explained skip out."""

    def test_valid_record_is_canonicalised(self) -> None:
        """Email lowercased and trimmed, names joined, nested company read."""
        contact = normalize_record(record(), MAPPING)
        assert isinstance(contact, Contact)
        self.assertEqual(contact.email, "ada.lovelace@example.invalid")
        self.assertEqual(contact.dedupe_key, "ada.lovelace@example.invalid")
        self.assertEqual(contact.full_name, "Ada Lovelace")
        self.assertEqual(contact.company, "Northwind")
        self.assertEqual(contact.phone, "+15550102030")
        self.assertEqual(contact.external_id, "1")

    def test_defective_records_are_skipped_with_a_stable_reason(self) -> None:
        """Each defect yields a machine-readable reason, not an exception."""
        cases: list[tuple[dict[str, Any], str]] = [
            ({"email": ""}, REASON_MISSING_EMAIL),
            ({"email": None}, REASON_MISSING_EMAIL),
            ({"email": "   "}, REASON_MISSING_EMAIL),
            ({"email": "not-an-email"}, REASON_INVALID_EMAIL),
            ({"email": "two@@at.example"}, REASON_INVALID_EMAIL),
            ({"email": "no-tld@example"}, REASON_INVALID_EMAIL),
            ({"email": "spaces in@example.com"}, REASON_INVALID_EMAIL),
            ({"id": None}, REASON_MISSING_EXTERNAL_ID),
            ({"id": "  "}, REASON_MISSING_EXTERNAL_ID),
            ({"firstName": "", "lastName": ""}, REASON_MISSING_NAME),
        ]
        for overrides, reason in cases:
            with self.subTest(overrides=overrides):
                outcome = normalize_record(record(**overrides), MAPPING)
                assert isinstance(outcome, Skipped)
                self.assertEqual(outcome.reason, reason)

    def test_missing_optional_fields_do_not_skip_the_record(self) -> None:
        """Absent phone and company are blanks, not failures."""
        contact = normalize_record(
            {"id": 7, "firstName": "Grace", "email": "g@x.example"}, MAPPING
        )
        assert isinstance(contact, Contact)
        self.assertEqual(contact.phone, "")
        self.assertEqual(contact.company, "")
        self.assertEqual(contact.full_name, "Grace")

    def test_full_name_mapping_is_supported(self) -> None:
        """Sources that expose a single name field need no first/last split."""
        mapping = FieldMapping(external_id="id", email="email", full_name="name")
        contact = normalize_record(
            {"id": 1, "name": "  Alan   Turing ", "email": "a@x.example"}, mapping
        )
        assert isinstance(contact, Contact)
        self.assertEqual(contact.full_name, "Alan Turing")

    def test_normalize_phone(self) -> None:
        """Phone numbers keep a leading + and digits only."""
        cases: list[tuple[Any, str]] = [
            ("+1 (555) 010-2030", "+15550102030"),
            ("555.010.2030", "5550102030"),
            ("ext. none", ""),
            (None, ""),
            ("", ""),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(normalize_phone(raw), expected)

    def test_content_hash_ignores_updated_at_but_tracks_payload(self) -> None:
        """The hash changes when data changes, not when only the timestamp does."""
        a = normalize_record(record(updatedAt="2026-01-01T00:00:00Z"), MAPPING)
        b = normalize_record(record(updatedAt="2026-09-09T00:00:00Z"), MAPPING)
        c = normalize_record(record(phone="+1 555 999 0000"), MAPPING)
        assert isinstance(a, Contact) and isinstance(b, Contact) and isinstance(c, Contact)
        self.assertEqual(a.content_hash(), b.content_hash())
        self.assertNotEqual(a.content_hash(), c.content_hash())


class DeduperTests(NoNetworkTestCase):
    """Duplicate resolution, and the arithmetic the run report relies on."""

    def test_newest_duplicate_wins_and_the_older_one_is_superseded(self) -> None:
        """A later duplicate with a newer updated_at replaces the earlier record."""
        deduper = Deduper()
        old = normalize_record(record(id=1, updatedAt="2026-01-01T00:00:00Z"), MAPPING)
        new = normalize_record(record(id=2, updatedAt="2026-05-01T00:00:00Z"), MAPPING)
        assert isinstance(old, Contact) and isinstance(new, Contact)

        self.assertIs(deduper.offer(old), old)
        self.assertIs(deduper.offer(new), new)
        self.assertEqual(deduper.superseded, 1)
        self.assertEqual(deduper.duplicates_dropped, 0)

    def test_older_duplicate_is_dropped(self) -> None:
        """A later duplicate with an older updated_at is discarded."""
        deduper = Deduper()
        new = normalize_record(record(id=1, updatedAt="2026-05-01T00:00:00Z"), MAPPING)
        old = normalize_record(record(id=2, updatedAt="2026-01-01T00:00:00Z"), MAPPING)
        assert isinstance(old, Contact) and isinstance(new, Contact)

        self.assertIs(deduper.offer(new), new)
        self.assertIsNone(deduper.offer(old))
        self.assertEqual(deduper.duplicates_dropped, 1)
        self.assertEqual(deduper.superseded, 0)

    def test_without_updated_at_the_first_occurrence_wins(self) -> None:
        """With no modification timestamp available, first seen wins."""
        mapping = FieldMapping(external_id="id", email="email", first_name="firstName")
        deduper = Deduper()
        first = normalize_record({"id": 1, "firstName": "A", "email": "x@y.example"}, mapping)
        second = normalize_record({"id": 2, "firstName": "B", "email": "x@y.example"}, mapping)
        assert isinstance(first, Contact) and isinstance(second, Contact)

        self.assertIs(deduper.offer(first), first)
        self.assertIsNone(deduper.offer(second))
        self.assertEqual(deduper.duplicates_dropped, 1)

    def test_dedupe_spans_page_boundaries(self) -> None:
        """The deduper is shared across batches, so cross-page duplicates collapse."""
        mapping = FieldMapping(external_id="id", email="email", first_name="firstName")
        deduper = Deduper()
        page_one = [{"id": 1, "firstName": "A", "email": "x@y.example"}]
        page_two = [{"id": 2, "firstName": "B", "email": "x@y.example"}]

        self.assertEqual(len(normalize_batch(page_one, mapping, deduper).contacts), 1)
        self.assertEqual(len(normalize_batch(page_two, mapping, deduper).contacts), 0)
        self.assertEqual(deduper.duplicates_dropped, 1)

    def test_batch_accounts_for_every_record(self) -> None:
        """valid + dropped + skipped equals the number of records read."""
        deduper = Deduper()
        raw = [
            record(id=1, email="a@x.example", updatedAt="2026-02-01T00:00:00Z"),
            record(id=2, email="a@x.example", updatedAt="2026-01-01T00:00:00Z"),
            record(id=3, email=""),
            record(id=4, email="bad"),
            record(id=5, email="b@x.example"),
        ]
        result = normalize_batch(raw, MAPPING, deduper)

        self.assertEqual(len(result.contacts), 2)
        self.assertEqual(deduper.duplicates_dropped, 1)
        self.assertEqual(
            result.skip_counts(), {REASON_MISSING_EMAIL: 1, REASON_INVALID_EMAIL: 1}
        )
        self.assertEqual(
            len(result.contacts) + deduper.duplicates_dropped + len(result.skipped),
            len(raw),
        )
