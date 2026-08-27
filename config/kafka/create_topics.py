"""One-shot Kafka topic provisioning, run by the kafka-init service at every
stack startup (not just a fresh one - safe to re-run, see below). Reads
topics_config.yml (this directory) and, per topic: creates it with the configured
partition count if missing, or raises its partition count via
create_partitions() if it already exists with fewer partitions than
configured. Never lowers partition count (Kafka itself doesn't support
that) and never touches any existing data in a topic - purely a metadata
operation, safe to run before every producer/consumer connects.
"""
from __future__ import annotations

import logging
import os
import sys
import time

import yaml
from confluent_kafka.admin import AdminClient, NewPartitions, NewTopic

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG_PATH = os.environ.get("KAFKA_TOPICS_CONFIG", "/app/topics_config.yml")


def _connect(bootstrap: str) -> tuple[AdminClient, dict]:
    # Mirrors data_generators/kafka_producer.py's build_producer() readiness
    # retry - Kafka may still be starting when this container comes up.
    admin = AdminClient({"bootstrap.servers": bootstrap})
    last_error: Exception | None = None
    for attempt in range(1, 21):
        try:
            return admin, admin.list_topics(timeout=10).topics
        except Exception as exc:  # noqa: BLE001 - confluent-kafka raises plain Exception subclasses here
            last_error = exc
            logger.info("Kafka not ready yet (attempt %d/20): %s", attempt, exc)
            time.sleep(3)
    raise RuntimeError("Could not reach Kafka after 20 attempts") from last_error


def main() -> None:
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)

    partitions = cfg["partitions_per_topic"]
    replication = cfg["replication_factor"]
    topics = cfg["topics"]

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    admin, existing = _connect(bootstrap)

    to_create, to_widen = [], {}
    for topic in topics:
        if topic not in existing:
            to_create.append(NewTopic(topic, num_partitions=partitions, replication_factor=replication))
        else:
            current = len(existing[topic].partitions)
            if current < partitions:
                to_widen[topic] = partitions
            elif current > partitions:
                logger.warning(
                    "%s already has %d partition(s) (> configured %d) - Kafka can't shrink "
                    "partition count, leaving as-is",
                    topic, current, partitions,
                )

    if to_create:
        for topic, fut in admin.create_topics(to_create).items():
            fut.result()
            logger.info("Created topic %s with %d partition(s)", topic, partitions)

    if to_widen:
        new_parts = [NewPartitions(t, n) for t, n in to_widen.items()]
        for topic, fut in admin.create_partitions(new_parts).items():
            fut.result()
            logger.info("Widened topic %s to %d partition(s)", topic, to_widen[topic])

    if not to_create and not to_widen:
        logger.info("All %d topic(s) already match desired partition count (%d)", len(topics), partitions)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Kafka topic provisioning failed")
        sys.exit(1)
