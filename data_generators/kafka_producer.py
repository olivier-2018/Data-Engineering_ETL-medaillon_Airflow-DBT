"""Shared Kafka producer helper - every generator module publishes through
this single client, JSON-serialized, keyed by natural id for partition
affinity.

Uses confluent-kafka (librdkafka), not kafka-python: kafka-python==2.0.2's
vendored `six.moves` shim is broken under Python 3.12
(`ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`), confirmed
by actually running it in this container - confluent-kafka is actively
maintained and ships prebuilt manylinux wheels for 3.12, no such issue.
"""
from __future__ import annotations

import json
import logging
import os
import time

from confluent_kafka import KafkaException, Producer

logger = logging.getLogger(__name__)


def _delivery_report(err, msg) -> None:
    if err is not None:
        logger.warning("Delivery failed for %s: %s", msg.topic(), err)


def build_producer(retries: int = 20, delay_seconds: float = 3.0) -> Producer:
    """Kafka may still be starting when this container comes up - retry
    until list_topics() succeeds rather than crash-looping immediately.
    confluent-kafka's Producer() constructor doesn't fail eagerly (unlike
    kafka-python's), so readiness has to be checked explicitly."""
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 50, "acks": "all"})

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            producer.list_topics(timeout=5)
            return producer
        except KafkaException as exc:
            last_error = exc
            logger.info("Kafka not ready yet (attempt %d/%d): %s", attempt, retries, exc)
            time.sleep(delay_seconds)
    raise RuntimeError(f"Could not connect to Kafka after {retries} attempts") from last_error


def send_event(producer: Producer, topic: str, key: str, value: dict) -> None:
    producer.produce(
        topic,
        key=key.encode("utf-8"),
        value=json.dumps(value, default=str).encode("utf-8"),
        callback=_delivery_report,
    )
    # Non-blocking - serves any queued delivery-report callbacks without
    # forcing a full flush() on every single event.
    producer.poll(0)
