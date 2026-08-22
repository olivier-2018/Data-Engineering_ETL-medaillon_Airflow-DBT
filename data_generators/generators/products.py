"""Product reference-data generator. Emits `created` events at startup
(bootstrap = the catalog) and occasional `updated` events (price/category
changes) - this is what feeds the product SCD2 story downstream."""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

from kafka_producer import send_event

TOPIC = "iot.product_events"

_ADJECTIVES = ["Compact", "Pro", "Ultra", "Classic", "Deluxe", "Eco", "Smart", "Heavy-Duty"]
_NOUNS = ["Widget", "Gadget", "Kit", "Set", "Unit", "Module", "Device", "Pack"]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _random_name(category: str) -> str:
    return f"{random.choice(_ADJECTIVES)} {category.split(' ')[0]} {random.choice(_NOUNS)}"


def seed_products(producer, cfg: dict) -> dict[str, dict]:
    products_cfg = cfg["products"]
    categories = products_cfg["categories"]
    products: dict[str, dict] = {}

    for _ in range(products_cfg["num_products"]):
        product_id = str(uuid.uuid4())
        category = random.choice(categories)
        product = {
            "product_id": product_id,
            "name": _random_name(category),
            "category": category,
            "subcategory": None,
            "unit_price": round(random.uniform(5.0, 500.0), 2),
            "weight_kg": round(random.uniform(0.1, 25.0), 2),
            "initial_stock": random.randint(
                products_cfg["initial_stock_min"], products_cfg["initial_stock_max"]
            ),
        }
        products[product_id] = product

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": "created",
            "event_at": _now(),
            **product,
        }
        send_event(producer, TOPIC, key=product_id, value=event)

    producer.flush()
    return products


def maybe_update_product(producer, cfg: dict, products: dict[str, dict], elapsed_seconds: float) -> None:
    """Probabilistically emit a price/category update for a random product,
    calibrated so the expected rate matches attribute_update_rate_per_hour."""
    rate_per_hour = cfg["products"]["attribute_update_rate_per_hour"]
    probability = (rate_per_hour / 3600.0) * elapsed_seconds
    if random.random() >= probability or not products:
        return

    product_id = random.choice(list(products.keys()))
    product = products[product_id]
    product["unit_price"] = round(product["unit_price"] * random.uniform(0.85, 1.15), 2)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "updated",
        "event_at": _now(),
        **product,
    }
    send_event(producer, TOPIC, key=product_id, value=event)
