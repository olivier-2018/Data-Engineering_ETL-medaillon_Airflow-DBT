"""Read-only Postgres access (generator_ro role): startup bootstrap reads
(resumes in-memory state after a process restart - see the redesign plan
§3/§3c) plus a handful of live, one-shot per-tick orphan-avoidance checks
(the functions below load_bootstrap_state() - see their own docstrings and
TODO.md's "avoid orphans" items). The live checks are never a blocking
dependency: each is a single SELECT, and callers skip their action for the
current tick if it comes back negative rather than waiting."""
from __future__ import annotations

import logging
import os

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


def _connect():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["GENERATOR_DB_USER"],
        password=os.environ["GENERATOR_DB_PASSWORD"],
    )


def load_bootstrap_state() -> dict:
    """Returns raw DB rows (list[dict] each) for every table the generator
    needs to resume from - main.py turns these into live class instances."""
    logger.debug(
        "Connecting to Postgres as %s @ %s:%s/%s",
        os.environ["GENERATOR_DB_USER"], os.environ["POSTGRES_HOST"],
        os.environ.get("POSTGRES_PORT", "5432"), os.environ["POSTGRES_DB"],
    )
    with _connect() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT zone_id, city, canton, country, lat, lon, is_warehouse "
                "FROM reference.delivery_zones WHERE is_active ORDER BY zone_id"
            )
            zones = [dict(r) for r in cur.fetchall()]
            logger.debug("Loaded %d active delivery zones", len(zones))

            cur.execute(
                "SELECT name, subcategories FROM reference.product_categories ORDER BY category_id"
            )
            categories = [dict(r) for r in cur.fetchall()]
            logger.debug("Loaded %d product categories", len(categories))

            cur.execute(
                "SELECT customer_id, name, email, address, tel, country, city, segment, "
                "verified_account, disabled_account, created_at "
                "FROM silver.customers_current"
            )
            customers = [dict(r) for r in cur.fetchall()]
            logger.debug("Loaded %d existing customers from silver", len(customers))

            cur.execute(
                "SELECT p.product_id, p.name, p.brand, p.model, p.category, p.subcategory, "
                "p.unit_price, p.weight_kg, p.nominal_capacity, "
                "COALESCE(i.current_stock, p.nominal_capacity) AS qty "
                "FROM silver.products_current p "
                "LEFT JOIN silver.inventory_current i ON i.product_id = p.product_id"
            )
            products = [dict(r) for r in cur.fetchall()]
            logger.debug("Loaded %d existing products from silver", len(products))

            cur.execute(
                "SELECT truck_id, name, brand, model, size, capacity, weight_kg "
                "FROM silver.truck_fleet_current"
            )
            trucks = [dict(r) for r in cur.fetchall()]
            logger.debug("Loaded %d existing trucks from silver", len(trucks))

            cur.execute(
                "SELECT purchase_order_id, customer_id, status, delivery_address, contact_tel, "
                "invoice_address, vat_number, target_delivery_date, truck_id, zone_id, created_at, updated_at "
                "FROM silver.purchase_orders_current "
                "WHERE status NOT IN ('closed', 'cancelled')"
            )
            open_orders = [dict(r) for r in cur.fetchall()]
            open_order_ids = [o["purchase_order_id"] for o in open_orders]
            logger.debug("Loaded %d open (non-terminal) purchase orders from silver", len(open_orders))

            line_items: list[dict] = []
            if open_order_ids:
                cur.execute(
                    "SELECT purchase_order_id, product_id, qty_on_order "
                    "FROM silver.product_on_orders_current "
                    "WHERE purchase_order_id = ANY(%s::uuid[])",
                    ([str(oid) for oid in open_order_ids],),
                )
                line_items = [dict(r) for r in cur.fetchall()]
            logger.debug("Loaded %d line items for those open orders", len(line_items))

            cur.execute(
                "SELECT invoice_id, purchase_order_id, customer_id, amount, status, "
                "payment_reminder, due_payment_date, created_at "
                "FROM silver.invoices_current WHERE status = 'pending'"
            )
            open_invoices = [dict(r) for r in cur.fetchall()]
            logger.debug("Loaded %d pending invoices from silver", len(open_invoices))

    return {
        "zones": zones,
        "categories": categories,
        "customers": customers,
        "products": products,
        "trucks": trucks,
        "open_orders": open_orders,
        "line_items": line_items,
        "open_invoices": open_invoices,
    }


# ===========================================================================
# Live, per-tick orphan-avoidance checks (unlike load_bootstrap_state above,
# these run continuously, not just at startup) - see TODO.md's "avoid
# orphans" items. Each is a single one-shot SELECT, never a blocking/retry
# loop: if a check comes back negative, the caller just skips its action for
# this tick and tries again naturally on a later one (new customers/orders
# keep arriving at their own configured rate regardless), rather than
# waiting for silver to catch up. This deliberately does NOT reintroduce a
# per-tick dependency on silver for the generator's own business decisions -
# it only guards against emitting an event whose parent hasn't been *seen in
# silver* yet, which would otherwise get permanently quarantined as an
# orphan by the corresponding *_to_silver.py job's own orphan check.
# ===========================================================================

def customer_exists_in_silver(customer_id: str) -> bool:
    """Used before creating a purchase order for a customer, so the order's
    customer_id is never a silver-side orphan by the time
    purchase_orders_to_silver.py's own orphan check runs."""
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM silver.customers_current WHERE customer_id = %s::uuid",
                (customer_id,),
            )
            return cur.fetchone() is not None


def purchase_order_ids_in_silver(purchase_order_ids: list[str]) -> set[str]:
    """Used to defer a purchase order's invoice/line-item creation until its
    own 'created' event has actually landed in silver, avoiding an orphan
    rejection in invoices_to_silver.py/product_on_orders_to_silver.py."""
    if not purchase_order_ids:
        return set()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT purchase_order_id FROM silver.purchase_orders_current "
                "WHERE purchase_order_id = ANY(%s::uuid[])",
                ([str(pid) for pid in purchase_order_ids],),
            )
            return {str(r[0]) for r in cur.fetchall()}


def paid_purchase_order_ids_in_silver(purchase_order_ids: list[str]) -> set[str]:
    """Used before Dispatcher pulls an order onto a truck - returns which of
    the given purchase_order_ids show a post-payment status in
    silver.purchase_orders_current (anything past 'invoiced'), confirming
    the payment transition has actually landed there, not just in the
    generator's own in-memory state."""
    if not purchase_order_ids:
        return set()
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT purchase_order_id FROM silver.purchase_orders_current "
                "WHERE purchase_order_id = ANY(%s::uuid[]) AND status NOT IN ('created', 'invoiced')",
                ([str(pid) for pid in purchase_order_ids],),
            )
            return {str(r[0]) for r in cur.fetchall()}
