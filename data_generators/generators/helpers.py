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


def resumed_utc_iso(dt) -> str | None:
    """Converts a psycopg2-returned datetime (naive - Postgres TIMESTAMP
    WITHOUT TIME ZONE columns always come back tz-naive) into a
    timezone-aware UTC ISO string, matching every in-memory-created
    timestamp's own format (now_iso() above). This project's convention is
    that every stored timestamp already represents UTC, just via a
    non-tz-aware column type - so a naive value is only ever labeled here,
    never reinterpreted. Without this, a resumed timestamp compared against
    a utc_now()-derived one (e.g. Dispatcher's oldest-wait check) raises
    `TypeError: can't subtract offset-naive and offset-aware datetimes` -
    confirmed happening in practice on every restart-with-resume."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()
