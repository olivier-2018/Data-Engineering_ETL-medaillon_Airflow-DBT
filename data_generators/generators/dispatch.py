"""Truck-consolidation coordinator - the one genuinely novel piece with no
pre-redesign analog. Groups paid orders by delivery zone, waits for either
enough orders or a max-wait timeout, then hands a batch to a free truck.

Zone queues are derived from live order state each tick (filtered by
status=="on-hold" and zone_id, ordered by updated_at), not stored
separately - see redesign plan §3b.

Also filters on `not order.claimed`: status stays "on-hold" until an
order's *staggered* assign_truck() callback actually fires during loading
(see Truck.start_run), so without this in-memory reservation flag a second
free truck could see the same still-"on-hold" order in the same or a later
tick and double-assign it - confirmed happening in practice (multiple
trucks stuck forever re-attempting delivery on an order another truck had
already delivered and closed)."""
from __future__ import annotations

import logging
from datetime import datetime

import db
from .helpers import utc_now

logger = logging.getLogger(__name__)


class Dispatcher:
    def __init__(self, cfg: dict, trucks: list, zones: list[dict]):
        self.cfg = cfg
        self.zones = zones
        self.free_trucks: list = list(trucks)
        self._tick_count = 0

    def on_free(self, truck) -> None:
        self.free_trucks.append(truck)

    def tick(self, producer, scheduler, orders: dict) -> None:
        self._tick_count += 1
        if not self.free_trucks:
            return

        dispatch_cfg = self.cfg["dispatch"]
        min_batch_size = dispatch_cfg["min_batch_size"]
        max_wait_seconds = dispatch_cfg["max_wait_minutes"] * 60
        capacity = self.cfg["trucks"]["capacity_products"]
        now = utc_now()

        # Confirms each candidate's payment has actually landed in
        # silver.purchase_orders_current before it's eligible for dispatch -
        # not just the generator's own in-memory "on-hold" status - avoiding
        # a dispatch decision the pipeline can't yet see (see TODO.md's
        # "avoid orphans" items). One batched query per Dispatcher.tick()
        # call (not per zone) covering every zone's candidates at once. No
        # blocking: an order not yet confirmed just stays in its zone's
        # queue and is reconsidered next tick, same as any other order still
        # waiting on min_batch_size/max_wait_minutes.
        on_hold_candidates = [o for o in orders.values() if o.status == "on-hold" and not o.claimed]
        confirmed_paid_ids = (
            db.paid_purchase_order_ids_in_silver([o.purchase_order_id for o in on_hold_candidates])
            if on_hold_candidates else set()
        )

        for zone in self.zones:
            if not self.free_trucks:
                return

            queue = sorted(
                (
                    o for o in on_hold_candidates
                    if o.zone_id == zone["zone_id"] and o.purchase_order_id in confirmed_paid_ids
                ),
                key=lambda o: o.updated_at,
            )
            if not queue:
                continue

            oldest_wait = (now - _parse(queue[0].updated_at)).total_seconds()
            if len(queue) < min_batch_size and oldest_wait < max_wait_seconds:
                # This branch re-fires every tick for as long as a zone has
                # a non-empty, non-dispatchable queue - throttle it to every
                # 10th tick so DEBUG output stays readable.
                if self._tick_count % 10 == 0:
                    logger.debug(
                        "Zone %s: %d order(s) on-hold, oldest waiting %.0fs - not dispatching yet "
                        "(need %d orders or %.0fs wait)",
                        zone["city"], len(queue), oldest_wait, min_batch_size, max_wait_seconds,
                    )
                continue

            truck = self.free_trucks.pop(0)
            batch = []
            used_capacity = 0
            for order in queue:
                order_qty = sum(qty for _, qty in order.line_items)
                if batch and used_capacity + order_qty > capacity:
                    break
                batch.append(order)
                used_capacity += order_qty

            # Claim immediately, synchronously - before start_run() schedules
            # any staggered load callbacks - so no other truck's Dispatcher
            # pass (even later this same tick) can pull these orders again.
            for order in batch:
                order.claimed = True

            logger.debug(
                "Zone %s: dispatching truck %s with %d/%d queued orders (capacity used %d/%d)",
                zone["city"], truck.truck_id, len(batch), len(queue), used_capacity, capacity,
            )
            truck.start_run(producer, self.cfg, scheduler, batch)


def _parse(iso_str: str) -> datetime:
    return datetime.fromisoformat(iso_str)
