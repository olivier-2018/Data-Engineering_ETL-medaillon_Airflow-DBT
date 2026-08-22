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
SILVER_SALES_ORDERS = Asset("silver://sales_orders_current")
SILVER_PAYMENTS = Asset("silver://payments_current")
SILVER_INVENTORY = Asset("silver://inventory_current")
SILVER_TRUCK_POSITIONS = Asset("silver://truck_positions_current")

ALL_SILVER_ASSETS = [
    SILVER_CUSTOMERS,
    SILVER_PRODUCTS,
    SILVER_SALES_ORDERS,
    SILVER_PAYMENTS,
    SILVER_INVENTORY,
    SILVER_TRUCK_POSITIONS,
]
