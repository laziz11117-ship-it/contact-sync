"""Configuration loading and validation."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

from contact_sync.config import SourceConfig, load_config, parse_config
from contact_sync.errors import ConfigError
from tests.support import NoNetworkTestCase

REPO_ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = REPO_ROOT / "config.example.json"

MINIMAL: dict[str, Any] = {
    "source": {"base_url": "http://fake.invalid/users"},
    "mapping": {"external_id": "id", "email": "email", "full_name": "name"},
}


class ParseConfigTests(NoNetworkTestCase):
    """Validation happens at load time, not halfway through a run."""

    def test_minimal_config_parses_with_defaults(self) -> None:
        """Only a source URL and a usable name mapping are mandatory."""
        config = parse_config(MINIMAL)
        self.assertEqual(config.source.page_size, 25)
        self.assertEqual(config.source.retry.max_attempts, 4)
        self.assertEqual(config.sink.kind, "sqlite")

    def test_comment_keys_are_ignored(self) -> None:
        """Underscore-prefixed keys are comments, at every level."""
        raw = {
            "_note": "top level comment",
            "source": {"_note": "comment", "base_url": "http://x/y",
                       "retry": {"_note": "comment", "max_attempts": 2}},
            "mapping": {"_note": "comment", **MINIMAL["mapping"]},
        }
        config = parse_config(raw)
        self.assertEqual(config.source.retry.max_attempts, 2)

    def test_missing_base_url_is_rejected(self) -> None:
        """A config without a source URL cannot be run."""
        with self.assertRaises(ConfigError) as caught:
            parse_config({"source": {}, "mapping": MINIMAL["mapping"]})
        self.assertIn("base_url", str(caught.exception))

    def test_invalid_config_is_rejected_loudly(self) -> None:
        """Typos and unsupported values fail fast with a readable message."""
        cases: list[tuple[dict[str, Any], str]] = [
            ({"source": {"base_url": "http://x/y", "page_sze": 10}}, "unknown key"),
            ({"sink": {"kind": "postgres"}}, "not supported"),
            ({"source": {"base_url": "http://x/y", "page_size": 0}}, "page_size"),
            (
                {"source": {"base_url": "http://x/y", "retry": {"max_attempts": 0}}},
                "max_attempts",
            ),
            ({"mapping": {"email": "email"}}, "full_name"),
            ({"unexpected": {}}, "unknown key"),
        ]
        for patch, expected in cases:
            with self.subTest(patch=patch):
                with self.assertRaises(ConfigError) as caught:
                    parse_config({**MINIMAL, **patch})
                self.assertIn(expected, str(caught.exception))


class AuthHeaderTests(NoNetworkTestCase):
    """Credentials are named in config and read from the environment."""

    def test_auth_headers(self) -> None:
        """No var configured, var unset, and var set."""
        self.assertEqual(SourceConfig(base_url="http://x/y").auth_headers({}), {})
        self.assertEqual(
            SourceConfig(base_url="http://x/y", auth_env_var="T").auth_headers({}), {}
        )
        self.assertEqual(
            SourceConfig(base_url="http://x/y", auth_env_var="T").auth_headers(
                {"T": "abc"}
            ),
            {"Authorization": "Bearer abc"},
        )


class LoadConfigTests(NoNetworkTestCase):
    """Reading the file from disk."""

    def test_shipped_example_config_is_valid(self) -> None:
        """config.example.json is not allowed to rot."""
        config = load_config(EXAMPLE)
        self.assertTrue(config.source.base_url.startswith("http://"))
        self.assertEqual(config.source.auth_env_var, "CONTACT_SYNC_SOURCE_TOKEN")
        self.assertEqual(config.sink.kind, "sqlite")

    def test_example_config_holds_no_credential(self) -> None:
        """The example names an environment variable; it carries no secret."""
        raw = json.loads(EXAMPLE.read_text(encoding="utf-8"))
        self.assertEqual(raw["source"]["auth_env_var"], "CONTACT_SYNC_SOURCE_TOKEN")
        flattened = json.dumps(raw).lower()
        for forbidden in ("bearer ", "password", "api_key", "secret_key"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, flattened)

    def test_missing_file_is_a_config_error(self) -> None:
        """A clear message beats a traceback."""
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ConfigError) as caught:
                load_config(Path(tmp) / "nope.json")
            self.assertIn("not found", str(caught.exception))

    def test_malformed_file_is_a_config_error(self) -> None:
        """Broken JSON names the file instead of raising a decoder error."""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text("{ not json", encoding="utf-8")
            with self.assertRaises(ConfigError) as caught:
                load_config(path)
            self.assertIn("not valid JSON", str(caught.exception))


class RepositoryHygieneTests(NoNetworkTestCase):
    """Checks that keep the promise "no secrets in this repository" honest."""

    def test_repository_contains_no_env_file(self) -> None:
        """Only .env.example is tracked; a real .env must never be committed."""
        self.assertFalse((REPO_ROOT / ".env").exists())
        self.assertTrue((REPO_ROOT / ".env.example").exists())

    def test_gitignore_covers_the_dangerous_paths(self) -> None:
        """Runtime artefacts and the operator's own config stay untracked."""
        ignored = (REPO_ROOT / ".gitignore").read_text(encoding="utf-8")
        for entry in ("data/", "config.json", ".env"):
            with self.subTest(entry=entry):
                self.assertIn(entry, ignored)
