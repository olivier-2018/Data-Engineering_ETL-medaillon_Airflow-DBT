"""Inventory generator - tracks current_stock per product in-memory and
emits a decrement event when an order moves to `picking`. The automatic
restock-at-20% mechanism is NOT here - that's Spark's restock_check.py
(§1c/§6a), reading silver and appending directly into iot.inventory_changes."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from kafka_producer import send_event

TOPIC = "iot.inventory_changes"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class InventoryState:
    def __init__(self, products: dict[str, dict]):
        self.current_stock: dict[str, int] = {
            pid: p["initial_stock"] for pid, p in products.items()
        }

    def decrement_for_sale(self, producer, product_id: str, quantity: int) -> None:
        current = self.current_stock.get(product_id, 0)
        new_stock = max(current - quantity, 0)
        self.current_stock[product_id] = new_stock

        event = {
            "event_id": str(uuid.uuid4()),
            "product_id": product_id,
            "quantity_delta": -quantity,
            "current_stock": new_stock,
            "change_reason": "sale",
            "changed_at": _now(),
        }
        send_event(producer, TOPIC, key=product_id, value=event)
