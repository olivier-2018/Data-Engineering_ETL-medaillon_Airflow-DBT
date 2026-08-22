"""Payment generator - a separate domain from order status (per the user's
explicit "acknowledge orders AND financial transactions" framing). Reacts to
an order being confirmed by authorizing, then capturing (or, at the
configured failure rate, failing) after a configurable delay."""
from __future__ import annotations

import random
import time
import uuid
from datetime import datetime, timezone

from kafka_producer import send_event

TOPIC = "iot.payment_events"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class PaymentState:
    def __init__(self):
        # order_id -> {"amount": float, "capture_at": epoch seconds, "will_fail": bool}
        self._pending: dict[str, dict] = {}

    def authorize(self, producer, cfg: dict, order_id: str, amount: float) -> None:
        payments_cfg = cfg["payments"]
        delay_cfg = payments_cfg["processing_delay_seconds"]
        delay = random.uniform(delay_cfg["min"], delay_cfg["max"])
        will_fail = random.random() < payments_cfg["failure_probability"]

        self._pending[order_id] = {
            "amount": amount,
            "capture_at": time.monotonic() + delay,
            "will_fail": will_fail,
        }

        event = {
            "event_id": str(uuid.uuid4()),
            "order_id": order_id,
            "payment_status": "authorized",
            "amount": amount,
            "event_at": _now(),
        }
        send_event(producer, TOPIC, key=order_id, value=event)

    def tick(self, producer) -> None:
        """Emit captured/failed events for any payment whose delay has elapsed."""
        now = time.monotonic()
        due = [oid for oid, p in self._pending.items() if now >= p["capture_at"]]
        for order_id in due:
            pending = self._pending.pop(order_id)
            status = "failed" if pending["will_fail"] else "captured"
            event = {
                "event_id": str(uuid.uuid4()),
                "order_id": order_id,
                "payment_status": status,
                "amount": pending["amount"],
                "event_at": _now(),
            }
            send_event(producer, TOPIC, key=order_id, value=event)
