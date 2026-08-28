"""Purchase-order lifecycle - the central integration point tying customer,
invoice, inventory, and truck-dispatch together (the role orders.py played
pre-redesign, now via explicit Lifecycle-validated transitions instead of
implicit ordering - see redesign plan §3b). Line items live in
product_on_order_events, not on this header event - a header row never
carries product/quantity itself.

Status lifecycle: created -> invoiced -> paid -> on-hold -> loaded ->
in-transit -> delivered -> closed, or cancelled from created/invoiced/paid/
on-hold. Matches the transition-rank table silver independently re-validates
(redesign plan §5) - kept here too so the generator structurally cannot
itself emit an impossible transition."""
from __future__ import annotations

import logging
import random
import uuid
from datetime import timedelta

from kafka_producer import send_event

from .geo import road_distance_km
from .helpers import now_iso, resumed_utc_iso, utc_now
from .lifecycle import Lifecycle

logger = logging.getLogger(__name__)

TOPIC = "iot.purchase_order_events"
LINE_ITEM_TOPIC = "iot.product_on_order_events"

_TRANSITIONS = {
    None: {"created"},
    "created": {"invoiced", "cancelled"},
    "invoiced": {"paid", "cancelled"},
    "paid": {"on-hold", "cancelled"},
    "on-hold": {"loaded", "cancelled"},
    "loaded": {"in-transit"},
    "in-transit": {"delivered"},
    "delivered": {"closed"},
    "closed": set(),
    "cancelled": set(),
}


class PurchaseOrder(Lifecycle):
    def __init__(
        self,
        purchase_order_id: str,
        customer_id: str,
        zone: dict,
        warehouse: dict,
        delivery_address: str,
        contact_tel: str,
        invoice_address: str,
        line_items: list[tuple[str, int]],
        avg_speed_kmh: float,
        delivery_buffer_days: float,
        created_at: str | None = None,
        customer_email: str | None = None,
    ):
        super().__init__(_TRANSITIONS)
        self.id = purchase_order_id
        self.purchase_order_id = purchase_order_id
        self.customer_id = customer_id
        self.customer_email = customer_email  # for set_status()'s debug log only - not a business field
        self.zone = zone
        self.zone_id = zone["zone_id"]
        self.delivery_address = delivery_address
        self.contact_tel = contact_tel
        self.invoice_address = invoice_address
        self.vat_number = f"CHE-{random.randint(100, 999)}.{random.randint(100, 999)}.{random.randint(100, 999)}"
        distance_km = road_distance_km(warehouse["lat"], warehouse["lon"], zone["lat"], zone["lon"])
        travel_hours = distance_km / avg_speed_kmh
        self.target_delivery_date = (
            utc_now() + timedelta(hours=travel_hours) + timedelta(days=delivery_buffer_days)
        ).isoformat()
        self.truck_id: str | None = None
        self.created_at = created_at or now_iso()
        self.line_items = line_items  # list[(product_id, qty)]
        # In-memory-only reservation flag, never emitted to Kafka. Set the
        # instant the Dispatcher pulls this order into a truck's batch -
        # status itself doesn't flip away from "on-hold" until that order's
        # *staggered* assign_truck() callback fires (up to
        # (len(batch)-1)*load_seconds_per_order later), so without this
        # flag a second free truck's Dispatcher.tick() could see the same
        # still-"on-hold" order and double-assign it to two trucks at once.
        self.claimed = False

    def _log_label(self) -> str:
        if self.customer_email:
            return f"{self.id} ({self.customer_email})"
        return str(self.id)

    def _event(self) -> dict:
        return {
            "event_id": str(uuid.uuid4()),
            "purchase_order_id": self.purchase_order_id,
            "customer_id": self.customer_id,
            "status": self.status,
            "delivery_address": self.delivery_address,
            "contact_tel": self.contact_tel,
            "invoice_address": self.invoice_address,
            "vat_number": self.vat_number,
            "target_delivery_date": self.target_delivery_date,
            "truck_id": self.truck_id,
            "zone_id": self.zone_id,
            "event_at": now_iso(),
        }

    @classmethod
    def create(cls, producer, customer, products: dict, zone: dict, warehouse: dict, cfg: dict) -> "PurchaseOrder":
        max_items = cfg["orders"]["max_products_per_po"]
        max_qty = cfg["orders"]["max_products_qty_per_po"]
        num_items = random.randint(1, max_items)
        product_ids = random.sample(list(products.keys()), min(num_items, len(products)))
        line_items = [(pid, random.randint(1, max_qty)) for pid in product_ids]

        # customer.address already carries the customer's own city (see
        # Customer.create); for a different delivery zone (business/gift,
        # ~5% per home_delivery_probability), or even for a home-delivery
        # order, just re-appending zone['city'] would duplicate it whenever
        # it matches - use the street portion only, then append the actual
        # delivery zone's city once.
        street = customer.address.split(",", 1)[0]
        order = cls(
            purchase_order_id=str(uuid.uuid4()),
            customer_id=customer.customer_id,
            zone=zone,
            warehouse=warehouse,
            delivery_address=f"{street}, {zone['city']}",
            contact_tel=customer.tel,
            invoice_address=customer.address,
            line_items=line_items,
            avg_speed_kmh=cfg["trucks"]["avg_speed_kmh"],
            delivery_buffer_days=cfg["orders"]["delivery_buffer_days"],
            customer_email=customer.email,
        )
        order.set_status("created", producer, TOPIC, lambda o: o._event())

        for product_id, qty in line_items:
            event = {
                "event_id": str(uuid.uuid4()),
                "product_on_order_id": str(uuid.uuid4()),
                "purchase_order_id": order.purchase_order_id,
                "product_id": product_id,
                "qty_on_order": qty,
                "customer_comment": None,
                "event_at": now_iso(),
            }
            send_event(producer, LINE_ITEM_TOPIC, key=order.purchase_order_id, value=event)
        logger.debug(
            "PurchaseOrder %s created: customer=%s (%s) zone=%s line_items=%d target_delivery=%s",
            order.purchase_order_id, customer.email, customer.customer_id, zone["city"], len(line_items),
            order.target_delivery_date,
        )
        return order

    def create_invoice(self, producer) -> None:
        self.set_status("invoiced", producer, TOPIC, lambda o: o._event())

    def total_amount(self, products: dict) -> float:
        """Sum of unit_price * qty across every line item - the amount the
        linked Invoice should be created with. `if pid in products` guards
        against a product removed from the catalog between order-creation
        and invoicing (never happens today - products are never deleted -
        but line_items' product_ids only exist because they were sampled
        from products.keys() at order-creation time, so this is defensive,
        not load-bearing)."""
        return round(
            sum(products[pid].unit_price * qty for pid, qty in self.line_items if pid in products), 2
        )

    def mark_paid(self, producer, products: dict) -> None:
        self.set_status("paid", producer, TOPIC, lambda o: o._event())
        for product_id, qty in self.line_items:
            product = products.get(product_id)
            if product is not None:
                product.decrement_for_sale(producer, qty)
        # "on-hold" is the explicit waiting-for-truck-capacity state, always
        # traversed even if a truck happens to be free right away - the
        # Dispatcher decides when to pull an order out of it, not this
        # transition (redesign plan §3/§3b).
        self.set_status("on-hold", producer, TOPIC, lambda o: o._event())

    def assign_truck(self, producer, truck_id: str) -> None:
        self.truck_id = truck_id
        self.set_status("loaded", producer, TOPIC, lambda o: o._event())

    def depart(self, producer) -> None:
        self.set_status("in-transit", producer, TOPIC, lambda o: o._event())

    def deliver(self, producer) -> None:
        self.set_status("delivered", producer, TOPIC, lambda o: o._event())
        self.set_status("closed", producer, TOPIC, lambda o: o._event())

    def cancel(self, producer) -> None:
        if "cancelled" in self.ALLOWED_TRANSITIONS.get(self.status, set()):
            self.set_status("cancelled", producer, TOPIC, lambda o: o._event())

    @classmethod
    def from_resume(
        cls,
        row: dict,
        zones: list[dict],
        warehouse: dict,
        line_items: list[tuple[str, int]],
        customer_email: str | None = None,
    ) -> "PurchaseOrder":
        """Rehydrates a PurchaseOrder from a silver.purchase_orders_current
        row at generator startup (see db.py/redesign plan §3/§3c) - status/
        truck_id/target_delivery_date are taken as-is from what was already
        persisted, not recomputed. Falls back to the warehouse zone if the
        order's zone_id is no longer active (rare: only happens if a zone
        was deactivated mid-flight - the Dispatcher only loops over
        currently-active zones, so such an order can't be picked up for
        dispatch anyway, a disclosed limitation of the zone-toggle design)."""
        zone = next((z for z in zones if z["zone_id"] == row["zone_id"]), warehouse)
        order = cls(
            purchase_order_id=str(row["purchase_order_id"]),
            customer_id=str(row["customer_id"]),
            zone=zone,
            warehouse=warehouse,
            delivery_address=row["delivery_address"],
            contact_tel=row["contact_tel"],
            invoice_address=row["invoice_address"],
            line_items=line_items,
            avg_speed_kmh=1.0,  # unused - target_delivery_date is overwritten below with the persisted value
            delivery_buffer_days=0,
            created_at=resumed_utc_iso(row["created_at"]),
            customer_email=customer_email,
        )
        order.status = row["status"]
        order.truck_id = row["truck_id"]
        order.target_delivery_date = resumed_utc_iso(row["target_delivery_date"]) or order.target_delivery_date
        order.updated_at = resumed_utc_iso(row["updated_at"]) or order.updated_at
        return order
