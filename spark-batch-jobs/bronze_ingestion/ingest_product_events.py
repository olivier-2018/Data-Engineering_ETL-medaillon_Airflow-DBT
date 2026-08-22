from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from shared_ingestion_utils import run_ingestion

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("subcategory", StringType(), True),
        StructField("unit_price", DoubleType(), False),
        StructField("weight_kg", DoubleType(), True),
        StructField("initial_stock", IntegerType(), False),
        StructField("event_at", StringType(), False),
    ]
)

COLUMNS = [
    "event_id", "product_id", "event_type", "name", "category", "subcategory",
    "unit_price", "weight_kg", "initial_stock", "event_at",
]

if __name__ == "__main__":
    run_ingestion(
        topic="iot.product_events",
        table="iot.product_events",
        schema=SCHEMA,
        columns=COLUMNS,
        conflict_cols="event_id",
    )
