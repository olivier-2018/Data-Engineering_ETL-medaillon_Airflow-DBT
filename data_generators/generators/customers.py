"""Customer reference-data generator - same shape as products.py (created +
occasional updated events), feeding the customer SCD2 story downstream."""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

from kafka_producer import send_event

TOPIC = "iot.customer_events"

_FIRST_NAMES = ["Anna", "Marco", "Lukas", "Sophie", "Julia", "Luca", "Emma", "Noah", "Mia", "Leon"]
_LAST_NAMES = ["Muller", "Rossi", "Meier", "Dubois", "Keller", "Bianchi", "Weber", "Fischer"]
_CITIES_BY_COUNTRY = {
    "CH": ["Biel", "Bern", "Zurich", "Basel", "Geneva"],
    "FR": ["Paris", "Lyon", "Marseille", "Strasbourg"],
    "DE": ["Berlin", "Munich", "Frankfurt", "Stuttgart"],
    "IT": ["Milan", "Turin", "Rome", "Bologna"],
}
_SEGMENTS = ["retail", "small_business", "vip"]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def seed_customers(producer, cfg: dict) -> dict[str, dict]:
    customers_cfg = cfg["customers"]
    countries = customers_cfg["countries"]
    customers: dict[str, dict] = {}

    for _ in range(customers_cfg["num_customers"]):
        customer_id = str(uuid.uuid4())
        country = random.choice(countries)
        customer = {
            "customer_id": customer_id,
            "name": f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}",
            "country": country,
            "city": random.choice(_CITIES_BY_COUNTRY[country]),
            "segment": random.choice(_SEGMENTS),
        }
        customers[customer_id] = customer

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "created",
            "event_at": _now(),
            **customer,
        }
        send_event(producer, TOPIC, key=customer_id, value=event)

    producer.flush()
    return customers


def maybe_update_customer(producer, cfg: dict, customers: dict[str, dict], elapsed_seconds: float) -> None:
    rate_per_hour = cfg["customers"]["attribute_update_rate_per_hour"]
    probability = (rate_per_hour / 3600.0) * elapsed_seconds
    if random.random() >= probability or not customers:
        return

    customer_id = random.choice(list(customers.keys()))
    customer = customers[customer_id]
    customer["city"] = random.choice(_CITIES_BY_COUNTRY[customer["country"]])
    customer["segment"] = random.choice(_SEGMENTS)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "updated",
        "event_at": _now(),
        **customer,
    }
    send_event(producer, TOPIC, key=customer_id, value=event)
