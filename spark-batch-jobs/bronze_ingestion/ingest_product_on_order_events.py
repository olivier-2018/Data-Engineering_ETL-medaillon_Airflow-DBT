from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from shared_ingestion_utils import run_ingestion

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("product_on_order_id", StringType(), False),
        StructField("purchase_order_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("qty_on_order", IntegerType(), False),
        StructField("customer_comment", StringType(), True),
        StructField("event_at", StringType(), False),
    ]
)

COLUMNS = [
    "event_id", "product_on_order_id", "purchase_order_id", "product_id",
    "qty_on_order", "customer_comment", "event_at",
]

if __name__ == "__main__":
    run_ingestion(
        topic="iot.product_on_order_events",
        table="iot.product_on_order_events",
        schema=SCHEMA,
        columns=COLUMNS,
        conflict_cols="event_id, event_at",
    )
