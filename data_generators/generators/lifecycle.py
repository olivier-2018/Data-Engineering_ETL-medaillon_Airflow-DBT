"""Shared status-transition guard + event-emission primitive every
lifecycle-driven entity (PurchaseOrder, Invoice) uses, so business-state
mutation and Kafka event emission can never drift apart - there's only one
code path for both. See redesign plan §3b.

Not used by Customer (two independent booleans, not a single status enum),
Product (no status machine, just a stock threshold), or Truck (an
availability flag, not a business-object status with a transition table).

Each subclass defines its own transition table as a class attribute (it's a
property of the entity *type*, not of any one instance) and passes it into
Lifecycle.__init__ explicitly, so the constructor is the one place that
wires "which statuses can this kind of object be in" per subclass - e.g.
PurchaseOrder's table has 8 real statuses, Invoice's has 3, and neither
should silently inherit or override the other's."""
from __future__ import annotations

import logging
from typing import Callable

from kafka_producer import send_event

from .helpers import now_iso

logger = logging.getLogger(__name__)


class Lifecycle:
    def __init__(self, allowed_transitions: dict) -> None:
        # {current_status_or_None: {allowed_next_statuses}}. `None` is the
        # pseudo-state "doesn't exist yet" - every entity's very first
        # status change (e.g. None -> "created") goes through set_status()
        # the same as any later transition, keeping one uniform code path.
        self.ALLOWED_TRANSITIONS = allowed_transitions
        self.status: str | None = None
        self.updated_at: str = now_iso()

    def _log_label(self) -> str:
        """Override in a subclass to add human-readable context (e.g.
        PurchaseOrder appends the customer's email) to set_status()'s debug
        log line - the base implementation is just the entity's own id."""
        return str(getattr(self, "id", "?"))

    def set_status(
        self,
        new_status: str,
        producer,
        topic: str,
        event_builder: Callable[["Lifecycle"], dict],
    ) -> None:
        allowed = self.ALLOWED_TRANSITIONS.get(self.status, set())
        if new_status not in allowed:
            raise ValueError(
                f"{type(self).__name__} {self._log_label()}: "
                f"invalid transition {self.status!r} -> {new_status!r}"
            )
        previous_status = self.status
        self.status = new_status
        self.updated_at = now_iso()
        event = event_builder(self)
        logger.debug(
            "%s %s: %r -> %r (topic=%s, event_id=%s)",
            type(self).__name__, self._log_label(), previous_status, new_status,
            topic, event.get("event_id"),
        )
        send_event(producer, topic, key=str(self.id), value=event)
