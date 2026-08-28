"""The ONE persistent Structured Streaming job in this pipeline (§1a) - every
other domain is periodic batch. Reads iot.truck_position_events off Kafka
continuously and appends to bronze + upserts the silver "current position"
table via foreachBatch, since Spark's JDBC writer (not used here anyway -
psycopg2 throughout) has no upsert mode and can't express PostGIS calls.

Submitted with --deploy-mode cluster (see scripts and the
bronze_streaming_supervisor_dag) so spark-submit returns immediately once the
driver is accepted, rather than blocking the submitting process forever.
"""
from __future__ import annotations

import os

from pyspark.sql.functions import col, from_json

from shared_streaming_utils import (
    TRUCK_POSITION_SCHEMA,
    get_spark_session,
    read_partitions_per_topic,
    write_truck_positions_partition,
)

APP_NAME = "bronze-truck-position-ingest"
TOPIC = "iot.truck_position_events"
CHECKPOINT_DIR = "/tmp/spark-data/checkpoints/truck_position"


def process_batch(batch_df, epoch_id: int) -> None:
    # repartition(N).foreachPartition(...) instead of collect() + one
    # driver-side write - same distributed-write pattern as the batch DAGs
    # (see docs/SPARK_PROJECT.md), N matching the Kafka topic's own
    # partition count (config/kafka/topics_config.yml).
    batch_df.repartition(read_partitions_per_topic()).foreachPartition(write_truck_positions_partition)


def main() -> None:
    spark = get_spark_session(APP_NAME)
    spark.sparkContext.setLogLevel("WARN")

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = raw.select(
        from_json(col("value").cast("string"), TRUCK_POSITION_SCHEMA).alias("data")
    ).select("data.*")

    query = (
        parsed.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
