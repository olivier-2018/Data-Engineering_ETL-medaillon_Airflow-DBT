"""Orchestrator: bootstraps state (fresh seed, or resume from silver via
db.py), then runs a 1-second tick loop driving every domain's business logic
per config.yaml's configured rates. Publishes synthetic events directly to
Kafka - no MQTT hop (per the scenario decision).

See TODO_improve_business_logic.md / the redesign plan
(i-want-to-implement-pure-boole.md) for the full business-process rationale:
registration -> purchase order -> invoice -> consolidated truck delivery ->
restock.

Set LOG_LEVEL=DEBUG (env var, default INFO) for detailed per-event tracing
from every generators/ module - each emits its own logger.debug() calls at
every state change/decision point."""
from __future__ import annotations

import logging
import os
import random
import signal
import time

import db
from kafka_producer import build_producer
from settings import load_config

from generators.customer import Customer
from generators.dispatch import Dispatcher
from generators.invoice import Invoice
from generators.product import Product
from generators.purchase_order import PurchaseOrder
from generators.scheduler import Scheduler
from generators.truck import Truck

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("generator")

TICK_SECONDS = 1.0

_shutdown = False


def _handle_shutdown(signum, frame):
    global _shutdown
    logger.info("Received signal %s, shutting down after this tick ...", signum)
    _shutdown = True


def _warehouse(zones: list[dict]) -> dict:
    return next(z for z in zones if z["is_warehouse"])


def _delivery_zone_for(customer: Customer, zones: list[dict], cfg: dict) -> dict:
    """95% of orders (home_delivery_probability) deliver to the customer's
    own home city; the rest go to a different zone (business/gift
    delivery). A customer's `city` is always sourced from this same active
    `zones` list at both creation (Customer.create) and drift
    (Customer.maybe_update) time, so the home-city lookup below is
    guaranteed to resolve under normal operation - it cannot land on a
    zone that isn't currently active.

    The one way this invariant could break is a *resumed* customer whose
    home city was deactivated (reference.delivery_zones.is_active flipped
    false) while the generator was down. There's no self-healing for that
    case here: the generator's DB role (generator_ro) is read-only by
    design (redesign plan §2a/decision #11) and cannot flip a zone back to
    active itself - that would require pipeline_rw (which does have
    UPDATE on reference.*) acting from a Spark job, a decision deliberately
    left for that layer, not the generator, if it's ever needed. This falls
    back to a random active zone instead and logs a warning, rather than
    silently mis-delivering or crashing.
    """
    if random.random() < cfg["orders"]["home_delivery_probability"]:
        home_zone = next((z for z in zones if z["city"] == customer.city), None)
        if home_zone is not None:
            return home_zone
        logger.warning(
            "Customer %s (%s)'s home city %r is not (or no longer) an active delivery zone - "
            "falling back to a random active zone for this order.",
            customer.email, customer.customer_id, customer.city,
        )
    return random.choice(zones)


def _log_no_eligible_customer(customers: dict, cfg: dict) -> None:
    """Breaks down *why* no customer could place a new order this tick -
    the same three reasons Customer.can_create_purchase_order() checks,
    tallied instead of collapsed into one boolean - and suggests concrete
    remedies. WARNING, not DEBUG: "the whole system is currently
    order-creation-starved" is operationally meaningful, but this still
    stays naturally rate-limited since it only fires when the per-tick
    order-arrival roll already succeeded (at most
    order_arrival_rate_per_minute times per minute, not every tick)."""
    max_open = cfg["orders"]["max_concurrent_orders_per_customer"]
    total = len(customers)
    unverified = sum(1 for c in customers.values() if not c.verified_account)
    disabled = sum(1 for c in customers.values() if c.disabled_account)
    at_cap = sum(
        1 for c in customers.values()
        if c.verified_account and not c.disabled_account and len(c.open_order_ids) >= max_open
    )
    logger.warning(
        "PurchaseOrder creation rejected -- no eligible customer (%d total: "
        "%d unverified, %d disabled, %d at the max_concurrent_orders_per_customer=%d cap). \n"
        "== Consider: 1) raising PO limit per customer, 2) increase customer creation rate, "
        "or 3) speed up delivery throughput. ==", total, unverified, disabled, at_cap, max_open,
    )


def _process_pending_invoices(producer, cfg: dict, scheduler: Scheduler, orders: dict, products: dict, invoices: dict) -> None:
    """For every order still at status=='created' (PO emitted, no invoice
    yet), checks whether the PO has actually landed in
    silver.purchase_orders_current before creating its invoice - avoids
    emitting a product_on_order_events/invoice_events row whose
    purchase_order_id doesn't exist in silver yet from
    product_on_orders_to_silver.py's/invoices_to_silver.py's own
    perspective, which would otherwise get permanently quarantined as an
    orphan (see TODO.md's "avoid orphans" items). No blocking: an order
    whose PO isn't confirmed yet just stays 'created' and is reconsidered
    next tick, since this runs every tick (also called once at bootstrap,
    covering the pre-existing "crash between PurchaseOrder.create() and
    create_invoice()" repair case - those orders were just read from
    silver.purchase_orders_current, so they're always already confirmed).

    `existing_invoice_pos` guards against a double-invoice at bootstrap
    specifically: silver.purchase_orders_current.status and
    silver.invoices_current are updated by independently-scheduled DAGs, so
    a resumed order's status can still read 'created' even though its
    invoice already exists (and was already loaded via Invoice.from_resume())
    - without this check, this function would create a second invoice for
    the same order, whose own due-timer would later attempt a duplicate
    mark_paid() and raise an invalid on-hold -> paid transition (confirmed
    happening in practice)."""
    pending = [o for o in orders.values() if o.status == "created"]
    if not pending:
        return
    existing_invoice_pos = {inv.purchase_order_id for inv in invoices.values()}
    pending = [o for o in pending if o.purchase_order_id not in existing_invoice_pos]
    if not pending:
        return
    confirmed_ids = db.purchase_order_ids_in_silver([o.purchase_order_id for o in pending])
    for order in pending:
        if order.purchase_order_id not in confirmed_ids:
            continue
        order.create_invoice(producer)
        invoice = Invoice.create(producer, cfg, scheduler, order, order.total_amount(products), products)
        invoices[invoice.invoice_id] = invoice


def _bootstrap(producer, cfg: dict, scheduler: Scheduler, state: dict):
    zones = state["zones"]
    warehouse = _warehouse(zones)
    categories = state["categories"]

    customers: dict[str, Customer] = {}
    for row in state["customers"]:
        customer = Customer(
            customer_id=str(row["customer_id"]),
            name=row["name"],
            email=row["email"],
            address=row["address"],
            tel=row["tel"],
            country=row["country"],
            city=row["city"],
            segment=row["segment"],
            verified_account=row["verified_account"],
            disabled_account=row["disabled_account"],
            created_at=row["created_at"].isoformat() if row["created_at"] else None,
        )
        customers[customer.customer_id] = customer
    if not customers:
        logger.info("No existing customers found - starting from an empty customer base.")

    products: dict[str, Product] = {}
    for row in state["products"]:
        product = Product(
            product_id=str(row["product_id"]),
            name=row["name"],
            brand=row["brand"],
            model=row["model"],
            category=row["category"],
            subcategory=row["subcategory"],
            unit_price=float(row["unit_price"]),
            weight_kg=float(row["weight_kg"]) if row["weight_kg"] is not None else None,
            nominal_capacity=row["nominal_capacity"],
            qty=row["qty"],
        )
        products[product.product_id] = product
    if not products:
        products = Product.seed_catalog(producer, cfg, categories)

    trucks: list[Truck] = []
    for row in state["trucks"]:
        trucks.append(
            Truck(
                truck_id=row["truck_id"],
                name=row["name"],
                brand=row["brand"],
                model=row["model"],
                size=row["size"],
                capacity=row["capacity"],
                weight_kg=float(row["weight_kg"]) if row["weight_kg"] is not None else None,
                warehouse=warehouse,
                avg_speed_kmh=cfg["trucks"]["avg_speed_kmh"],
            )
        )
    if not trucks:
        trucks = Truck.seed_fleet(producer, cfg, warehouse)

    line_items_by_order: dict[str, list] = {}
    for row in state["line_items"]:
        line_items_by_order.setdefault(str(row["purchase_order_id"]), []).append(
            (str(row["product_id"]), row["qty_on_order"])
        )

    orders: dict[str, PurchaseOrder] = {}
    for row in state["open_orders"]:
        resumed_customer = customers.get(str(row["customer_id"]))
        order = PurchaseOrder.from_resume(
            row,
            zones,
            warehouse,
            line_items_by_order.get(str(row["purchase_order_id"]), []),
            customer_email=resumed_customer.email if resumed_customer else None,
        )
        # A truck resumes as "free" with an empty manifest (redesign plan
        # §3c - only a genuine crash mid-run loses this, not a graceful
        # restart), so any order left "loaded"/"in-transit" is no longer
        # tracked by any truck object - requeue it rather than leaving it
        # permanently stuck. In-memory repair only, no Kafka event: the
        # bronze/silver history still accurately reflects what really
        # happened, this is just live-simulation self-healing.
        if order.status in ("loaded", "in-transit"):
            logger.warning(
                "Order %s resumed mid-truck-run (status=%s) but no truck object "
                "retained its manifest across the restart - requeuing to on-hold.",
                order.purchase_order_id, order.status,
            )
            order.status = "on-hold"
            order.truck_id = None
        elif order.status == "delivered":
            order.status = "closed"

        orders[order.purchase_order_id] = order
        customer = customers.get(order.customer_id)
        if customer is not None:
            customer.open_order_ids.add(order.purchase_order_id)

    invoices: dict[str, Invoice] = {}
    for row in state["open_invoices"]:
        purchase_order = orders.get(str(row["purchase_order_id"]))
        if purchase_order is None:
            continue
        invoice = Invoice.from_resume(row, purchase_order, scheduler, producer, cfg, products)
        invoices[invoice.invoice_id] = invoice

    # Repair: a crash between PurchaseOrder.create() and create_invoice()
    # (a sub-second window) would resume with an order stuck at "created"
    # and no invoice - close that gap via the same silver-gated helper the
    # tick loop uses every tick (these particular orders were just read
    # from silver.purchase_orders_current above, so they're always already
    # confirmed - this call always succeeds for them, matching the
    # original repair loop's immediate behavior).
    _process_pending_invoices(producer, cfg, scheduler, orders, products, invoices)

    dispatcher = Dispatcher(cfg, trucks, zones)
    return customers, products, orders, invoices, trucks, dispatcher, zones


def main() -> None:
    # Graceful-shutdown handling matters here specifically: main.py is the
    # one place that makes a plain `docker compose stop`/`restart` gap-free
    # (redesign plan §3c) by flushing every recently-decided event to Kafka
    # before the process actually exits, rather than relying solely on the
    # DB-resume bootstrap below to reconstruct state on the next start.
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    logger.info("Log level: %s", LOG_LEVEL)
    cfg = load_config()
    producer = build_producer()
    scheduler = Scheduler()  # shared timer-heap for due-dates/reminders/loading - see generators/scheduler.py

    # Bootstrap: either a genuinely fresh stack (silver is empty -> seed
    # products/trucks, start with zero customers/orders) or a resumed one
    # (silver has prior state -> rehydrate every class from it). Either way,
    # this is a one-shot startup read - the generator never queries Postgres
    # again after this point (redesign plan §3/§3c: in-memory is
    # authoritative from here on, DB access was purely for catching up).
    logger.info("Loading bootstrap state from silver (generator_ro) ...")
    state = db.load_bootstrap_state()
    customers, products, orders, invoices, trucks, dispatcher, zones = _bootstrap(
        producer, cfg, scheduler, state
    )
    warehouse = _warehouse(zones)
    logger.info(
        "Bootstrap complete: %d customers, %d products, %d trucks, %d open orders, "
        "%d open invoices, %d active zones",
        len(customers), len(products), len(trucks), len(orders), len(invoices), len(zones),
    )

    logger.info("Starting tick loop (1 tick = %.1fs) ...", TICK_SECONDS)
    tick_count = 0
    while not _shutdown:
        tick_start = time.monotonic()
        tick_count += 1

        # 1. Fire any due timers (invoice due-dates/reminders, truck loading
        #    steps, account verification) - see generators/scheduler.py.
        try:
            scheduler.tick()
        except Exception:
            logger.exception("A scheduled callback raised - continuing")

        # 2. New customer registrations (rate-limited, capped at
        #    max_customers) + slow background profile/disable/enable drift
        #    on existing customers.
        if len(customers) < cfg["customers"]["max_customers"]:
            creation_probability = cfg["customers"]["creation_rate_per_minute"] / 60.0
            if random.random() < creation_probability:
                existing_emails = {c.email for c in customers.values()}
                customer = Customer.create(producer, cfg, scheduler, zones, existing_emails)
                customers[customer.customer_id] = customer

        for customer in list(customers.values()):
            try:
                customer.maybe_update(producer, cfg, zones, TICK_SECONDS)
            except Exception:
                logger.exception("Customer.maybe_update raised for %s (%s)", customer.email, customer.customer_id)

        # 3. Product price drift (rare) - stock itself only ever moves via
        #    PurchaseOrder.mark_paid()'s decrement, never here.
        for product in products.values():
            try:
                product.maybe_update(producer, cfg, TICK_SECONDS)
            except Exception:
                logger.exception("Product.maybe_update raised for %r (%s)", product.name, product.product_id)

        # 4. New purchase orders, from an eligible (verified, not disabled,
        #    under their per-customer open-order cap) customer whose own
        #    'created' event has already landed in silver.customers_current -
        #    skip (not block) this tick's attempt otherwise, avoiding an
        #    orphan_customer rejection in purchase_orders_to_silver.py; a
        #    later tick will naturally retry (see TODO.md's "avoid orphans").
        #    The invoice is no longer created synchronously here - see step
        #    4b, which defers it until the PO itself is confirmed in silver.
        order_arrival_per_tick = cfg["orders"]["order_arrival_rate_per_minute"] / 60.0
        if random.random() < order_arrival_per_tick and products:
            eligible = [c for c in customers.values() if c.can_create_purchase_order(cfg)]
            if not eligible:
                _log_no_eligible_customer(customers, cfg)
            else:
                try:
                    customer = random.choice(eligible)
                    if not db.customer_exists_in_silver(customer.customer_id):
                        logger.debug(
                            "Skipping order creation for %s this tick - not yet in silver.customers_current",
                            customer.email,
                        )
                    else:
                        zone = _delivery_zone_for(customer, zones, cfg)
                        order = PurchaseOrder.create(producer, customer, products, zone, warehouse, cfg)
                        orders[order.purchase_order_id] = order
                        customer.open_order_ids.add(order.purchase_order_id)
                except Exception:
                    logger.exception("Order creation raised")

        # 4b. Create invoices for any order whose PO has landed in silver
        #     since it was created (runs every tick, not just at bootstrap -
        #     see _process_pending_invoices()'s own docstring).
        try:
            _process_pending_invoices(producer, cfg, scheduler, orders, products, invoices)
        except Exception:
            logger.exception("_process_pending_invoices raised")

        # 5. Truck consolidation (Dispatcher decides which on-hold orders to
        #    batch onto a free truck) + truck movement/delivery progress.
        #    Everything from here on downstream of "paid" is driven by
        #    these two ticks, not by scanning order status directly.
        try:
            dispatcher.tick(producer, scheduler, orders)
        except Exception:
            logger.exception("Dispatcher.tick raised")

        for truck in trucks:
            try:
                truck.tick(producer, cfg, dispatcher.on_free)
            except Exception:
                logger.exception("Truck.tick raised for %s", truck.truck_id)

        # 6. Garbage-collect terminal orders/invoices from local in-memory
        #    tracking dicts - Kafka/silver already has the full history, so
        #    this is just bookkeeping cleanup, not a data-loss concern.
        for order_id, order in list(orders.items()):
            if order.status in ("closed", "cancelled"):
                logger.debug("Order %s reached terminal status=%s, dropping from active tracking", order_id, order.status)
                customer = customers.get(order.customer_id)
                if customer is not None:
                    customer.open_order_ids.discard(order_id)
                del orders[order_id]
                stale_invoice_id = next(
                    (iid for iid, inv in invoices.items() if inv.purchase_order_id == order_id), None
                )
                invoices.pop(stale_invoice_id, None)

        if tick_count % 60 == 0:
            producer.flush()
            logger.info(
                "tick=%d customers=%d open_orders=%d open_invoices=%d free_trucks=%d/%d",
                tick_count, len(customers), len(orders), len(invoices),
                len(dispatcher.free_trucks), len(trucks),
            )

        elapsed = time.monotonic() - tick_start
        time.sleep(max(TICK_SECONDS - elapsed, 0))

    # confluent_kafka.Producer has no close() - flush() alone is sufficient,
    # it's not a persistent connection object requiring explicit teardown.
    producer.flush()
    logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()
