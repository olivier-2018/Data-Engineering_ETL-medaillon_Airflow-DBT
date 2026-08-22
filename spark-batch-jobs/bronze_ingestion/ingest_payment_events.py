from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from shared_ingestion_utils import run_ingestion

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("payment_status", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("event_at", StringType(), False),
    ]
)

COLUMNS = ["event_id", "order_id", "payment_status", "amount", "event_at"]

if __name__ == "__main__":
    run_ingestion(
        topic="iot.payment_events",
        table="iot.payment_events",
        schema=SCHEMA,
        columns=COLUMNS,
        conflict_cols="event_id, event_at",
    )
