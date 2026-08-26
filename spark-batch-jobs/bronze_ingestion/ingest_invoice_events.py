from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from shared_ingestion_utils import run_ingestion

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("invoice_id", StringType(), False),
        StructField("purchase_order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("status", StringType(), False),
        StructField("payment_reminder", IntegerType(), False),
        StructField("due_at", StringType(), True),
        StructField("event_at", StringType(), False),
    ]
)

COLUMNS = [
    "event_id", "invoice_id", "purchase_order_id", "customer_id",
    "amount", "status", "payment_reminder", "due_at", "event_at",
]

if __name__ == "__main__":
    run_ingestion(
        topic="iot.invoice_events",
        table="iot.invoice_events",
        schema=SCHEMA,
        columns=COLUMNS,
        conflict_cols="event_id, event_at",
    )
