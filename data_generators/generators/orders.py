"""Order lifecycle generator - owns order_status transitions
(pending -> confirmed -> picking -> ready_for_dispatch -> shipped -> delivered,
or cancelled from pending/confirmed). `shipped`->`delivered` is driven by
trucks.py reporting a shipment delivery back via mark_delivered(); everything
before `ready_for_dispatch` advances here on a fixed per-tick pace (not a
separate config knob - the configurable rates that actually matter for load
are order_arrival_rate_per_minute, payment delay, and truck ping interval)."""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

from kafka_producer import send_event

TOPIC = "iot.sales_order_events"

# Fixed simulation pace for pending->confirmed->picking->ready_for_dispatch;
# not exposed in config.yaml since it doesn't affect downstream pipeline load
# the way arrival rate / ping interval do.
_STAGE_ADVANCE_PROBABILITY = 0.2


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class OrderRegistry:
    def __init__(self):
        self.orders: dict[str, dict] = {}

    def open_count(self) -> int:
        return len(self.orders)


def create_order(producer, cfg, registry: OrderRegistry, products: dict, customers: dict) -> None:
    orders_cfg = cfg["orders"]
    if registry.open_count() >= orders_cfg["target_concurrent_orders"]:
        return
    if not products or not customers:
        return

    product_id = random.choice(list(products.keys()))
    customer_id = random.choice(list(customers.keys()))
    product = products[product_id]
    customer = customers[customer_id]

    order_id = str(uuid.uuid4())
    created_at = _now()
    order = {
        "order_id": order_id,
        "customer_id": customer_id,
        "product_id": product_id,
        "quantity": random.randint(1, 5),
        "unit_price_snapshot": product["unit_price"],
        "status": "pending",
        "created_at": created_at,
        "destination_country": customer["country"],
    }
    registry.orders[order_id] = order
    _emit(producer, order)


def _emit(producer, order: dict) -> None:
    event = {
        "event_id": str(uuid.uuid4()),
        "order_id": order["order_id"],
        "customer_id": order["customer_id"],
        "product_id": order["product_id"],
        "quantity": order["quantity"],
        "unit_price_snapshot": order["unit_price_snapshot"],
        "order_status": order["status"],
        "created_at": order["created_at"],
        "event_at": _now(),
    }
    send_event(producer, TOPIC, key=order["order_id"], value=event)


def advance_orders(producer, cfg, registry: OrderRegistry, inventory_state, payment_state, truck_fleet) -> None:
    cancellation_probability = cfg["orders"]["cancellation_probability"]

    for order_id, order in list(registry.orders.items()):
        status = order["status"]

        if status == "pending":
            if random.random() < cancellation_probability:
                order["status"] = "cancelled"
                _emit(producer, order)
                del registry.orders[order_id]
            elif random.random() < _STAGE_ADVANCE_PROBABILITY:
                order["status"] = "confirmed"
                _emit(producer, order)
                payment_state.authorize(producer, cfg, order_id, order["quantity"] * order["unit_price_snapshot"])

        elif status == "confirmed":
            if random.random() < _STAGE_ADVANCE_PROBABILITY:
                order["status"] = "picking"
                _emit(producer, order)
                inventory_state.decrement_for_sale(producer, order["product_id"], order["quantity"])

        elif status == "picking":
            if random.random() < _STAGE_ADVANCE_PROBABILITY:
                order["status"] = "ready_for_dispatch"
                _emit(producer, order)

        elif status == "ready_for_dispatch":
            if truck_fleet.has_free_truck():
                shipment_id = truck_fleet.dispatch(producer, order_id, order["destination_country"])
                if shipment_id is not None:
                    order["status"] = "shipped"
                    _emit(producer, order)

        # "shipped": waits for trucks.py to report delivery via mark_delivered().
        # "delivered"/"cancelled" are terminal and already removed from the registry.


def mark_delivered(producer, registry: OrderRegistry, order_id: str) -> None:
    order = registry.orders.pop(order_id, None)
    if order is None:
        return
    order["status"] = "delivered"
    _emit(producer, order)
