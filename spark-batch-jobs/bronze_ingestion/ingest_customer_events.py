from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from shared_ingestion_utils import run_ingestion

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("name", StringType(), False),
        StructField("email", StringType(), True),
        StructField("address", StringType(), True),
        StructField("tel", StringType(), True),
        StructField("country", StringType(), False),
        StructField("city", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("verified_account", BooleanType(), True),
        StructField("disabled_account", BooleanType(), True),
        StructField("event_at", StringType(), False),
    ]
)

COLUMNS = [
    "event_id", "customer_id", "event_type", "name", "email", "address", "tel",
    "country", "city", "segment", "verified_account", "disabled_account", "event_at",
]

if __name__ == "__main__":
    run_ingestion(
        topic="iot.customer_events",
        table="iot.customer_events",
        schema=SCHEMA,
        columns=COLUMNS,
        conflict_cols="event_id",
        distributed=True,
    )
