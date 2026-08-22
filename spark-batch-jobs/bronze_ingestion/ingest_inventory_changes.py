from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from shared_ingestion_utils import run_ingestion

SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity_delta", IntegerType(), False),
        StructField("current_stock", IntegerType(), False),
        StructField("change_reason", StringType(), False),
        StructField("changed_at", StringType(), False),
    ]
)

COLUMNS = ["event_id", "product_id", "quantity_delta", "current_stock", "change_reason", "changed_at"]

if __name__ == "__main__":
    run_ingestion(
        topic="iot.inventory_changes",
        table="iot.inventory_changes",
        schema=SCHEMA,
        columns=COLUMNS,
        conflict_cols="event_id, changed_at",
    )
