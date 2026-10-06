"""Resumable, idempotent one-way sync from a paginated REST API to a sink."""

from .errors import (
    ConfigError,
    ContactSyncError,
    PermanentError,
    PermanentHttpError,
    TransientError,
    TransientHttpError,
    TransportError,
)

__all__ = [
    "ConfigError",
    "ContactSyncError",
    "PermanentError",
    "PermanentHttpError",
    "TransientError",
    "TransientHttpError",
    "TransportError",
    "__version__",
]

__version__ = "0.1.0"
