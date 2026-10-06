"""Mapping raw source records to canonical contacts, with deduplication.

Every record that does not make it through is accounted for by reason, so
the run report can answer "why is the sink count lower than the read count"
without anyone opening a log file.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass, field
from collections.abc import Mapping
from typing import Any

from .config import FieldMapping
from .source import get_path

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]{2,}$")

#: Reasons a record is skipped. Stable strings: reports are machine-read.
REASON_MISSING_EXTERNAL_ID = "missing_external_id"
REASON_MISSING_EMAIL = "missing_email"
REASON_INVALID_EMAIL = "invalid_email"
REASON_MISSING_NAME = "missing_name"


@dataclass(frozen=True)
class Contact:
    """A canonical contact, ready for the sink."""

    dedupe_key: str
    external_id: str
    email: str
    full_name: str
    phone: str
    company: str
    updated_at: str

    def content_hash(self) -> str:
        """Hash of the mutable payload, used to detect no-op writes."""
        payload = {
            "email": self.email,
            "full_name": self.full_name,
            "phone": self.phone,
            "company": self.company,
            "external_id": self.external_id,
        }
        blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Skipped:
    """A record that was dropped, with a stable machine-readable reason."""

    external_id: str
    reason: str


def normalize_phone(value: Any) -> str:
    """Reduce a phone number to `+` plus digits; return "" when unusable."""
    if value is None:
        return ""
    text = str(value).strip()
    digits = re.sub(r"\D", "", text)
    if not digits:
        return ""
    return ("+" if text.startswith("+") else "") + digits


def normalize_email(value: Any) -> str:
    """Lowercase and trim an email address."""
    return "" if value is None else str(value).strip().lower()


class Deduper:
    """Collapses records that share a dedupe key, across page boundaries.

    The winner is the record with the greatest `updated_at`; when the source
    exposes no `updated_at`, the first occurrence wins and later ones are
    counted as duplicates.
    """

    def __init__(self) -> None:
        """Start with an empty key index."""
        self._winners: dict[str, Contact] = {}
        #: Incoming records discarded because a better one was already seen.
        self.duplicates_dropped = 0
        #: Already-emitted records replaced by a newer incoming duplicate.
        self.superseded = 0

    def offer(self, contact: Contact) -> Contact | None:
        """Register `contact` and decide whether it should be written.

        Returns:
            The contact when it should be written, or None when an
            already-seen record wins. Exactly one of `duplicates_dropped`
            (incoming record discarded) or `superseded` (earlier record
            replaced) is incremented on a key collision, which keeps the
            report arithmetic exact:
            `records_read == valid + duplicates_dropped + skipped`.
        """
        existing = self._winners.get(contact.dedupe_key)
        if existing is None:
            self._winners[contact.dedupe_key] = contact
            return contact
        if contact.updated_at > existing.updated_at:
            self._winners[contact.dedupe_key] = contact
            self.superseded += 1
            return contact
        self.duplicates_dropped += 1
        return None


@dataclass
class NormalizeResult:
    """Outcome of normalizing one page."""

    contacts: list[Contact] = field(default_factory=list)
    skipped: list[Skipped] = field(default_factory=list)

    def skip_counts(self) -> dict[str, int]:
        """Count skipped records by reason."""
        return dict(Counter(s.reason for s in self.skipped))


def normalize_record(
    raw: Mapping[str, Any], mapping: FieldMapping
) -> Contact | Skipped:
    """Convert one raw record into a `Contact`, or explain why it was skipped."""
    external_id = get_path(raw, mapping.external_id)
    external_id = "" if external_id is None else str(external_id).strip()
    if not external_id:
        return Skipped(external_id="", reason=REASON_MISSING_EXTERNAL_ID)

    email = normalize_email(get_path(raw, mapping.email))
    if not email:
        return Skipped(external_id=external_id, reason=REASON_MISSING_EMAIL)
    if not _EMAIL_RE.match(email):
        return Skipped(external_id=external_id, reason=REASON_INVALID_EMAIL)

    if mapping.full_name:
        name = str(get_path(raw, mapping.full_name) or "").strip()
    else:
        first = str(get_path(raw, mapping.first_name) or "").strip() if mapping.first_name else ""
        last = str(get_path(raw, mapping.last_name) or "").strip() if mapping.last_name else ""
        name = " ".join(part for part in (first, last) if part)
    name = re.sub(r"\s+", " ", name)
    if not name:
        return Skipped(external_id=external_id, reason=REASON_MISSING_NAME)

    phone = normalize_phone(get_path(raw, mapping.phone)) if mapping.phone else ""
    company = str(get_path(raw, mapping.company) or "").strip() if mapping.company else ""
    updated_at = ""
    if mapping.updated_at:
        updated_at = str(get_path(raw, mapping.updated_at) or "").strip()

    return Contact(
        dedupe_key=email,
        external_id=external_id,
        email=email,
        full_name=name,
        phone=phone,
        company=company,
        updated_at=updated_at,
    )


def normalize_batch(
    records: tuple[Mapping[str, Any], ...] | list[Mapping[str, Any]],
    mapping: FieldMapping,
    deduper: Deduper,
) -> NormalizeResult:
    """Normalize a page of records and drop duplicates via `deduper`."""
    result = NormalizeResult()
    for raw in records:
        outcome = normalize_record(raw, mapping)
        if isinstance(outcome, Skipped):
            result.skipped.append(outcome)
            continue
        winner = deduper.offer(outcome)
        if winner is not None:
            result.contacts.append(winner)
    return result
