"""Small shared helpers used across the generators/ package - not tied to
any one entity domain, so they live here rather than being duplicated (or
awkwardly imported cross-module) per class file."""
from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    """Use this (not now_iso()) whenever you need to do arithmetic on the
    current time - e.g. `utc_now() + timedelta(minutes=10)` for a due date."""
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return utc_now().isoformat()
