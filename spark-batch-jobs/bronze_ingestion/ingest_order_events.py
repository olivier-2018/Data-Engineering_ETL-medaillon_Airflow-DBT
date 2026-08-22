from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from shared_ingestion_utils import run_ingestion

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price_snapshot", DoubleType(), False),
        StructField("order_status", StringType(), False),
        StructField("created_at", StringType(), False),
        StructField("event_at", StringType(), False),
    ]
)

COLUMNS = [
    "event_id", "order_id", "customer_id", "product_id", "quantity",
    "unit_price_snapshot", "order_status", "created_at", "event_at",
]

if __name__ == "__main__":
    run_ingestion(
        topic="iot.sales_order_events",
        table="iot.sales_order_events",
        schema=SCHEMA,
        columns=COLUMNS,
        conflict_cols="event_id, event_at",
    )
