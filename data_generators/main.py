"""Orchestrator: seeds reference data, then runs a 1-second tick loop driving
every domain generator per config.yaml's configured rates. Publishes
synthetic events directly to Kafka - no MQTT hop (per the scenario decision)."""
from __future__ import annotations

import logging
import random
import signal
import sys
import time

from kafka_producer import build_producer
from settings import load_config

from generators import customers as customers_gen
from generators import orders as orders_gen
from generators import payments as payments_gen
from generators import products as products_gen
from generators.inventory import InventoryState
from generators.trucks import TruckFleet

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("generator")

TICK_SECONDS = 1.0

_shutdown = False


def _handle_shutdown(signum, frame):
    global _shutdown
    logger.info("Received signal %s, shutting down after this tick ...", signum)
    _shutdown = True


def main() -> None:
    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    cfg = load_config()
    logger.info(
        "Config loaded: %d products, %d customers, %d trucks",
        cfg["products"]["num_products"],
        cfg["customers"]["num_customers"],
        cfg["trucks"]["num_trucks"],
    )

    producer = build_producer()

    logger.info("Seeding products ...")
    products = products_gen.seed_products(producer, cfg)
    logger.info("Seeding customers ...")
    customers = customers_gen.seed_customers(producer, cfg)

    order_registry = orders_gen.OrderRegistry()
    inventory_state = InventoryState(products)
    payment_state = payments_gen.PaymentState()
    truck_fleet = TruckFleet(cfg)

    order_arrival_per_tick = cfg["orders"]["order_arrival_rate_per_minute"] / 60.0

    logger.info("Starting tick loop (1 tick = %.1fs) ...", TICK_SECONDS)
    tick_count = 0
    while not _shutdown:
        tick_start = time.monotonic()
        tick_count += 1

        products_gen.maybe_update_product(producer, cfg, products, TICK_SECONDS)
        customers_gen.maybe_update_customer(producer, cfg, customers, TICK_SECONDS)

        if random.random() < order_arrival_per_tick:
            orders_gen.create_order(producer, cfg, order_registry, products, customers)

        orders_gen.advance_orders(producer, cfg, order_registry, inventory_state, payment_state, truck_fleet)
        payment_state.tick(producer)

        delivered_order_ids = truck_fleet.tick(producer)
        for order_id in delivered_order_ids:
            orders_gen.mark_delivered(producer, order_registry, order_id)

        if tick_count % 60 == 0:
            producer.flush()
            logger.info(
                "tick=%d open_orders=%d active_shipments=%d free_trucks=%d",
                tick_count,
                order_registry.open_count(),
                len(truck_fleet.active_shipments),
                len(truck_fleet.free_trucks),
            )

        elapsed = time.monotonic() - tick_start
        time.sleep(max(TICK_SECONDS - elapsed, 0))

    # confluent_kafka.Producer has no close() - flush() alone is sufficient,
    # it's not a persistent connection object requiring explicit teardown.
    producer.flush()
    logger.info("Shutdown complete.")


if __name__ == "__main__":
    main()
