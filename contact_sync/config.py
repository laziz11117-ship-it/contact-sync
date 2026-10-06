"""Typed configuration loaded from a single JSON file.

JSON rather than YAML or TOML so that the package keeps zero runtime
dependencies on every supported interpreter. Any key whose name starts with
an underscore is ignored, which is how the example file carries comments.

Secrets are never stored here. `SourceConfig.auth_env_var` names an
environment variable; the value is read at request time and never written
to a report or log.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .errors import ConfigError
from .retry import RetryPolicy


@dataclass(frozen=True)
class SourceConfig:
    """Where records come from and how the endpoint pages."""

    base_url: str
    records_path: str = "data"
    total_path: str | None = None
    limit_param: str = "limit"
    offset_param: str = "offset"
    page_size: int = 25
    max_pages: int = 0
    timeout_seconds: float = 10.0
    auth_env_var: str | None = None
    retry: RetryPolicy = field(default_factory=RetryPolicy)

    def auth_headers(self, env: Mapping[str, str] | None = None) -> dict[str, str]:
        """Return auth headers, or an empty dict when no token is configured."""
        if not self.auth_env_var:
            return {}
        token = (env if env is not None else os.environ).get(self.auth_env_var, "")
        if not token:
            return {}
        return {"Authorization": f"Bearer {token}"}


@dataclass(frozen=True)
class FieldMapping:
    """Dotted paths from a raw source record to canonical contact fields."""

    external_id: str = "id"
    email: str = "email"
    full_name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    phone: str | None = None
    company: str | None = None
    updated_at: str | None = None

    def __post_init__(self) -> None:
        """Reject a mapping that cannot produce a name."""
        if not self.full_name and not (self.first_name or self.last_name):
            raise ConfigError(
                "mapping needs either `full_name` or `first_name`/`last_name`"
            )


@dataclass(frozen=True)
class SinkConfig:
    """Where records are written. Only `sqlite` ships in this sample."""

    kind: str = "sqlite"
    path: Path = Path("data/contacts.sqlite3")


@dataclass(frozen=True)
class RunConfig:
    """Paths for the resume checkpoint and the run report."""

    state_path: Path = Path("data/state.json")
    report_path: Path = Path("data/last-run.json")


@dataclass(frozen=True)
class Config:
    """The whole configuration file, validated."""

    source: SourceConfig
    mapping: FieldMapping
    sink: SinkConfig
    run: RunConfig


def _strip_comments(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Drop `_`-prefixed keys, which the config format reserves for comments."""
    return {k: v for k, v in raw.items() if not k.startswith("_")}


def _reject_unknown(section: str, raw: Mapping[str, Any], allowed: set[str]) -> None:
    """Fail loudly on typos instead of silently ignoring a key."""
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise ConfigError(f"{section} has unknown key(s): {', '.join(unknown)}")


def _retry_policy(raw: Mapping[str, Any]) -> RetryPolicy:
    """Build and sanity-check the retry policy section."""
    raw = _strip_comments(raw)
    allowed = {
        "max_attempts",
        "initial_backoff_seconds",
        "max_backoff_seconds",
        "multiplier",
        "jitter",
    }
    _reject_unknown("source.retry", raw, allowed)
    policy = RetryPolicy(**raw)
    if policy.max_attempts < 1:
        raise ConfigError("source.retry.max_attempts must be >= 1")
    if policy.initial_backoff_seconds < 0 or policy.max_backoff_seconds < 0:
        raise ConfigError("source.retry backoff values must be >= 0")
    return policy


def parse_config(raw: Mapping[str, Any]) -> Config:
    """Build a `Config` from an already-parsed JSON mapping.

    Raises:
        ConfigError: a required key is missing, or a key is unrecognised.
    """
    raw = _strip_comments(raw)
    _reject_unknown("config root", raw, {"source", "mapping", "sink", "run"})

    source_raw = _strip_comments(raw.get("source") or {})
    if "base_url" not in source_raw:
        raise ConfigError("[source] requires `base_url`")
    retry_raw = source_raw.pop("retry", {})
    _reject_unknown(
        "source",
        source_raw,
        {
            "base_url",
            "records_path",
            "total_path",
            "limit_param",
            "offset_param",
            "page_size",
            "max_pages",
            "timeout_seconds",
            "auth_env_var",
        },
    )
    source = SourceConfig(retry=_retry_policy(retry_raw), **source_raw)
    if source.page_size < 1:
        raise ConfigError("source.page_size must be >= 1")

    mapping_raw = _strip_comments(raw.get("mapping") or {})
    _reject_unknown(
        "mapping",
        mapping_raw,
        {
            "external_id",
            "email",
            "full_name",
            "first_name",
            "last_name",
            "phone",
            "company",
            "updated_at",
        },
    )
    mapping = FieldMapping(**mapping_raw)

    sink_raw = _strip_comments(raw.get("sink") or {})
    _reject_unknown("sink", sink_raw, {"kind", "path"})
    if "path" in sink_raw:
        sink_raw["path"] = Path(str(sink_raw["path"]))
    sink = SinkConfig(**sink_raw)
    if sink.kind != "sqlite":
        raise ConfigError(f"sink.kind {sink.kind!r} is not supported (only 'sqlite')")

    run_raw = _strip_comments(raw.get("run") or {})
    _reject_unknown("run", run_raw, {"state_path", "report_path"})
    for key in ("state_path", "report_path"):
        if key in run_raw:
            run_raw[key] = Path(str(run_raw[key]))
    run = RunConfig(**run_raw)

    return Config(source=source, mapping=mapping, sink=sink, run=run)


def load_config(path: str | Path) -> Config:
    """Read and validate a JSON configuration file.

    Raises:
        ConfigError: the file is missing, is not valid JSON, or is invalid.
    """
    file_path = Path(path)
    if not file_path.is_file():
        raise ConfigError(f"config file not found: {file_path}")
    try:
        raw = json.loads(file_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigError(f"{file_path} is not valid JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"{file_path} must contain a JSON object at the top level")
    return parse_config(raw)
