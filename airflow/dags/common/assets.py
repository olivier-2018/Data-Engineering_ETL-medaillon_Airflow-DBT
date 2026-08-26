"""Shared Airflow 3 Asset definitions, one per silver domain, imported by
every DAG that produces or depends on one - avoids duplicating/typo-ing the
same URI string across files.

NOTE: `airflow.sdk.Asset` is Airflow 3's Task SDK import path for what was
`airflow.datasets.Dataset` in Airflow 2. Verify this import against the
exact installed Airflow 3.0.3 API when first running the stack (flagged in
docs/DEVELOPMENT.md) - this could not be executed/verified in the planning
environment.
"""
from airflow.sdk import Asset

SILVER_CUSTOMERS = Asset("silver://customers_current")
SILVER_PRODUCTS = Asset("silver://products_current")
SILVER_PURCHASE_ORDERS = Asset("silver://purchase_orders_current")
SILVER_PRODUCT_ON_ORDERS = Asset("silver://product_on_orders_current")
SILVER_INVOICES = Asset("silver://invoices_current")
SILVER_INVENTORY = Asset("silver://inventory_current")
SILVER_TRUCK_FLEET = Asset("silver://truck_fleet_current")
SILVER_TRUCK_POSITIONS = Asset("silver://truck_current_position")

ALL_SILVER_ASSETS = [
    SILVER_CUSTOMERS,
    SILVER_PRODUCTS,
    SILVER_PURCHASE_ORDERS,
    SILVER_PRODUCT_ON_ORDERS,
    SILVER_INVOICES,
    SILVER_INVENTORY,
    SILVER_TRUCK_FLEET,
    SILVER_TRUCK_POSITIONS,
]

# gold_dbt_dag.py's schedule - deliberately NOT the same list as
# ALL_SILVER_ASSETS above. SILVER_TRUCK_FLEET only ever produces an Asset
# event once (the one-time fleet seed) and then never again in steady
# state, since truck_fleet_to_silver's ShortCircuitOperator legitimately
# skips every cycle once there's no new iot.truck_fleet_events data (a
# near-static fleet has no ongoing updates to process). Airflow 3's
# Asset-list scheduling requires EVERY listed asset to have a fresh event
# since the DAG's last run before it fires again - if SILVER_TRUCK_FLEET
# were included, gold_dbt_dag would fire exactly once, ever, then never
# again, since that one asset can never satisfy the AND condition a second
# time under normal operation. dim_truck still gets rebuilt on every gold
# run regardless (dbt run rebuilds every model, not just the domain that
# triggered it) - it just isn't part of what triggers that run.
GOLD_TRIGGER_ASSETS = [
    SILVER_CUSTOMERS,
    SILVER_PRODUCTS,
    SILVER_PURCHASE_ORDERS,
    SILVER_PRODUCT_ON_ORDERS,
    SILVER_INVOICES,
    SILVER_INVENTORY,
    SILVER_TRUCK_POSITIONS,
]

# Bronze-level assets, one per Kafka-sourced domain ingested by the
# bronze_ingest_*_dag.py DAGs (§7 of the redesign plan). Not consumed by any
# DAG's `schedule=` yet - the silver DAGs still gate on a plain timedelta +
# has_new_bronze_data() watermark check - but declaring these outlets now
# means a later switch to Asset-driven silver triggering is a schedule-line
# change, not a bronze-DAG rewrite.
BRONZE_CUSTOMER_EVENTS = Asset("bronze://customer_events")
BRONZE_PRODUCT_EVENTS = Asset("bronze://product_events")
BRONZE_PURCHASE_ORDER_EVENTS = Asset("bronze://purchase_order_events")
BRONZE_PRODUCT_ON_ORDER_EVENTS = Asset("bronze://product_on_order_events")
BRONZE_INVOICE_EVENTS = Asset("bronze://invoice_events")
BRONZE_TRUCK_FLEET_EVENTS = Asset("bronze://truck_fleet_events")
BRONZE_INVENTORY_CHANGES = Asset("bronze://inventory_changes")
