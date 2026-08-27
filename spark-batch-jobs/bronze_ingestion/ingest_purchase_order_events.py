from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from shared_ingestion_utils import run_ingestion

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("purchase_order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("status", StringType(), False),
        StructField("delivery_address", StringType(), True),
        StructField("contact_tel", StringType(), True),
        StructField("invoice_address", StringType(), True),
        StructField("vat_number", StringType(), True),
        StructField("target_delivery_date", StringType(), True),
        StructField("truck_id", StringType(), True),
        StructField("zone_id", IntegerType(), True),
        StructField("event_at", StringType(), False),
    ]
)

COLUMNS = [
    "event_id", "purchase_order_id", "customer_id", "status", "delivery_address", "contact_tel",
    "invoice_address", "vat_number", "target_delivery_date", "truck_id", "zone_id", "event_at",
]

if __name__ == "__main__":
    run_ingestion(
        topic="iot.purchase_order_events",
        table="iot.purchase_order_events",
        schema=SCHEMA,
        columns=COLUMNS,
        conflict_cols="event_id, event_at",
        # Flagship job for the repartition+foreachPartition distributed-write
        # demo (see docs/ARCHITECTURE.md) - the other 6 bronze domains stay
        # on run_ingestion()'s default single-threaded path, unchanged.
        distributed=True,
    )
