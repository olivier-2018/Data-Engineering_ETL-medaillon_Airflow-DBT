"""Product catalog - a fixed warehouse catalog seeded once at startup
(unlike customers, which register continuously - see config.yaml's own
header comment). Stock only ever depletes here; the automatic
20%-threshold refill is entirely a downstream Spark responsibility
(restock_check.py, reading silver.products_current.restock_required and
writing directly to Postgres bronze - bypassing Kafka and this generator
process entirely), so no refill/check_stock method lives on this class.

Categories AND their subcategories both come from `reference.product_categories`
(seeded in 02-seed-reference-data.sql, `subcategories` is a native Postgres
TEXT[] column - not a CSV string) - read once at generator startup via
db.py's bootstrap query and passed in as `categories` below. Nothing here is
hardcoded, so adding a category/subcategory is a SQL insert, not a code
change."""
from __future__ import annotations

import logging
import random
import uuid

from kafka_producer import send_event

from .helpers import now_iso

logger = logging.getLogger(__name__)

PRODUCT_TOPIC = "iot.product_events"
INVENTORY_TOPIC = "iot.inventory_changes"

_ADJECTIVES = [
    "Compact", "Pro", "Ultra", "Classic", "Deluxe", "Eco", "Smart", "Heavy-Duty",
    "Premium", "Portable", "Rugged", "Slim", "Advanced", "Essential", "Modular", "Turbo",
]
_NOUNS = ["Widget", "Gadget", "Kit", "Set", "Unit", "Module", "Device", "Pack"]
_BRANDS = [
    "Alpina", "Nordwerk", "Helvetia Goods", "Cresta", "Matterhorn Supply",
    "Glacierline", "Rigi Tools", "Sarnen & Co", "Bernina Home", "Furka Industries",
]
_MODEL_PREFIXES = ["M", "X", "Z", "Pro", "Elite", "Alpha", "Vertex", "Nova", "Prime", "Core"]


class Product:
    def __init__(
        self,
        product_id: str,
        name: str,
        brand: str | None,
        model: str | None,
        category: str,
        subcategory: str | None,
        unit_price: float,
        weight_kg: float | None,
        nominal_capacity: int,
        qty: int,
    ):
        self.id = product_id
        self.product_id = product_id
        self.name = name
        self.brand = brand
        self.model = model
        self.category = category
        self.subcategory = subcategory
        self.unit_price = unit_price
        self.weight_kg = weight_kg
        self.nominal_capacity = nominal_capacity
        self.qty = qty
        self.updated_at = now_iso()

    def _emit(self, producer, event_type: str) -> None:
        event = {
            "event_id": str(uuid.uuid4()),
            "product_id": self.product_id,
            "event_type": event_type,
            "name": self.name,
            "brand": self.brand,
            "model": self.model,
            "category": self.category,
            "subcategory": self.subcategory,
            "unit_price": self.unit_price,
            "weight_kg": self.weight_kg,
            "nominal_capacity": self.nominal_capacity,
            "event_at": now_iso(),
        }
        send_event(producer, PRODUCT_TOPIC, key=self.product_id, value=event)

    @classmethod
    def seed_catalog(cls, producer, cfg: dict, categories: list[dict]) -> dict[str, "Product"]:
        """`categories` comes from reference.product_categories (read at
        startup by db.py) - each entry is {"name": ..., "subcategories": [...]},
        never hardcoded here."""
        products_cfg = cfg["products"]
        products: dict[str, Product] = {}
        for _ in range(products_cfg["num_products"]):
            category = random.choice(categories)
            subcategory_choices = category.get("subcategories") or []
            subcategory = random.choice(subcategory_choices) if subcategory_choices else None
            nominal_capacity = random.randint(
                products_cfg["nominal_capacity_min"], products_cfg["nominal_capacity_max"]
            )
            product = cls(
                product_id=str(uuid.uuid4()),
                name=f"{random.choice(_ADJECTIVES)} {category['name'].split(' ')[0]} {random.choice(_NOUNS)}",
                brand=random.choice(_BRANDS),
                model=f"{random.choice(_MODEL_PREFIXES)}-{random.randint(100, 999)}",
                category=category["name"],
                subcategory=subcategory,
                unit_price=round(random.uniform(5.0, 500.0), 2),
                weight_kg=round(random.uniform(0.1, 25.0), 2),
                nominal_capacity=nominal_capacity,
                qty=nominal_capacity,  # full stock at seed time
            )
            products[product.product_id] = product
            product._emit(producer, "created")
            logger.debug(
                "Product created %r category=%s/%s (%s) nominal_capacity=%d",
                product.name, product.category, product.subcategory, product.product_id, nominal_capacity,
            )
        logger.info("Seeded %d products across %d categories", len(products), len(categories))
        return products

    def maybe_update(self, producer, cfg: dict, elapsed_seconds: float) -> None:
        rate_per_hour = cfg["products"]["attribute_update_rate_per_hour"]
        probability = (rate_per_hour / 3600.0) * elapsed_seconds
        if random.random() >= probability:
            return
        old_price = self.unit_price
        self.unit_price = round(self.unit_price * random.uniform(0.85, 1.15), 2)
        self.updated_at = now_iso()
        self._emit(producer, "updated")
        logger.debug(
            "Product %r category=%s/%s (%s) price updated: %.2f -> %.2f",
            self.name, self.category, self.subcategory, self.product_id, old_price, self.unit_price,
        )

    def decrement_for_sale(self, producer, quantity: int) -> None:
        new_qty = max(self.qty - quantity, 0)
        self.qty = new_qty
        self.updated_at = now_iso()
        event = {
            "event_id": str(uuid.uuid4()),
            "product_id": self.product_id,
            "quantity_delta": -quantity,
            "current_stock": new_qty,
            "change_reason": "sale",
            "changed_at": now_iso(),
        }
        send_event(producer, INVENTORY_TOPIC, key=self.product_id, value=event)
        logger.debug(
            "Product %r category=%s/%s (%s) sold qty=%d, stock now %d",
            self.name, self.category, self.subcategory, self.product_id, quantity, new_qty,
        )
