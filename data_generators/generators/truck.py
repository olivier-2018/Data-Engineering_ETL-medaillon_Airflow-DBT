"""Truck fleet - loads a batch of paid orders (assigned by Dispatcher, see
dispatch.py) onto one truck run, drives a multi-stop route (nearest-neighbor
over the involved delivery zones, not a full TSP/VRP solve - redesign plan
decision #2), and delivers each stop's orders independently as the truck
reaches it (per-stop delivery granularity - decision #7).

Not Lifecycle-based: a truck is closer to an availability flag
(free/loading/in_transit/returning) than a business-object status with a
transition table to validate - see redesign plan §3b."""
from __future__ import annotations

import logging
import random
import time
import uuid

from kafka_producer import send_event

from .geo import road_distance_km
from .helpers import now_iso

logger = logging.getLogger(__name__)

TOPIC = "iot.truck_position_events"
FLEET_TOPIC = "iot.truck_fleet_events"

_BRANDS = ["Volvo", "Scania", "MAN", "Mercedes-Benz", "Iveco"]
_MODELS = ["FH16", "R450", "TGX", "Actros", "S-Way"]


class Truck:
    def __init__(
        self,
        truck_id: str,
        name: str,
        brand: str | None,
        model: str | None,
        size: str | None,
        capacity: int,
        weight_kg: float | None,
        warehouse: dict,
        avg_speed_kmh: float,
    ):
        self.truck_id = truck_id
        self.name = name
        self.brand = brand
        self.model = model
        self.size = size
        self.capacity = capacity
        self.weight_kg = weight_kg
        self.warehouse = warehouse
        self.avg_speed_kmh = avg_speed_kmh
        self.status = "free"
        self.manifest: list = []          # PurchaseOrder objects currently aboard
        self.route: list[dict] = []       # [{"zone": zone_dict, "orders": [PurchaseOrder, ...]}, ...]
        self.route_index = 0
        self.lat = warehouse["lat"]
        self.lon = warehouse["lon"]
        self.current_zone_id: int | None = None
        self._leg_origin = (warehouse["lat"], warehouse["lon"])
        self._leg_destination = (warehouse["lat"], warehouse["lon"])
        self._leg_start = 0.0
        self._leg_seconds = 1.0
        self.next_ping_at = 0.0

    def _fleet_event(self, event_type: str) -> dict:
        return {
            "event_id": str(uuid.uuid4()),
            "truck_id": self.truck_id,
            "event_type": event_type,
            "name": self.name,
            "brand": self.brand,
            "model": self.model,
            "size": self.size,
            "capacity": self.capacity,
            "weight_kg": self.weight_kg,
            "event_at": now_iso(),
        }

    @classmethod
    def seed_fleet(cls, producer, cfg: dict, warehouse: dict) -> list["Truck"]:
        trucks_cfg = cfg["trucks"]
        trucks = []
        for i in range(1, trucks_cfg["fleet_size"] + 1):
            truck = cls(
                truck_id=f"TRUCK-{i:02d}",
                name=f"Truck {i:02d}",
                brand=random.choice(_BRANDS),
                model=random.choice(_MODELS),
                size="large",
                capacity=trucks_cfg["capacity_products"],
                weight_kg=round(random.uniform(7500, 18000), 1),
                warehouse=warehouse,
                avg_speed_kmh=trucks_cfg["avg_speed_kmh"],
            )
            send_event(producer, FLEET_TOPIC, key=truck.truck_id, value=truck._fleet_event("created"))
            trucks.append(truck)
        logger.info("Seeded %d trucks (fleet_size=%d)", len(trucks), trucks_cfg["fleet_size"])
        return trucks

    def _ping(self, producer) -> None:
        event = {
            "event_id": str(uuid.uuid4()),
            "truck_id": self.truck_id,
            "lat": self.lat,
            "lon": self.lon,
            "truck_status": self.status,
            "current_zone_id": self.current_zone_id,
            "event_at": now_iso(),
        }
        send_event(producer, TOPIC, key=self.truck_id, value=event)

    def start_run(self, producer, cfg: dict, scheduler, orders: list) -> None:
        """Groups `orders` into stops by zone, greedy nearest-neighbor
        sequenced from the warehouse, then loads them onto the truck
        sequentially (load_seconds_per_order each) before departing."""
        self.manifest = list(orders)
        self.route = self._build_route(orders)
        self.status = "loading"
        self.current_zone_id = None
        load_seconds = cfg["trucks"]["load_seconds_per_order"]
        for i, order in enumerate(orders):
            scheduler.schedule(i * load_seconds, lambda o=order: o.assign_truck(producer, self.truck_id))
        scheduler.schedule(len(orders) * load_seconds, lambda: self._depart(producer))
        logger.debug(
            "Truck %s starting run: %d orders, %d stops (%s), loading takes %ds",
            self.truck_id, len(orders), len(self.route),
            [stop["zone"]["city"] for stop in self.route], len(orders) * load_seconds,
        )

    def _build_route(self, orders: list) -> list[dict]:
        by_zone: dict[int, list] = {}
        zones_by_id: dict[int, dict] = {}
        for order in orders:
            by_zone.setdefault(order.zone_id, []).append(order)
            zones_by_id[order.zone_id] = order.zone

        remaining = dict(by_zone)
        route = []
        current_lat, current_lon = self.warehouse["lat"], self.warehouse["lon"]
        while remaining:
            nearest_zone_id = min(
                remaining,
                key=lambda zid: road_distance_km(
                    current_lat, current_lon, zones_by_id[zid]["lat"], zones_by_id[zid]["lon"]
                ),
            )
            zone = zones_by_id[nearest_zone_id]
            route.append({"zone": zone, "orders": remaining.pop(nearest_zone_id)})
            current_lat, current_lon = zone["lat"], zone["lon"]
        return route

    def _depart(self, producer) -> None:
        self.status = "in_transit"
        for order in self.manifest:
            order.depart(producer)
        self._start_leg(self.warehouse["lat"], self.warehouse["lon"], self.route[0]["zone"])
        logger.debug("Truck %s departed warehouse, first stop=%s", self.truck_id, self.route[0]["zone"]["city"])

    def _start_leg(self, origin_lat: float, origin_lon: float, destination: dict) -> None:
        self._leg_origin = (origin_lat, origin_lon)
        self._leg_destination = (destination["lat"], destination["lon"])
        distance_km = road_distance_km(origin_lat, origin_lon, destination["lat"], destination["lon"])
        self._leg_seconds = max((distance_km / self.avg_speed_kmh) * 3600.0, 5.0)
        self._leg_start = time.monotonic()

    def tick(self, producer, cfg: dict, on_free) -> None:
        if self.status not in ("in_transit", "returning"):
            return

        now = time.monotonic()
        if now >= self.next_ping_at:
            self._ping(producer)
            self.next_ping_at = now + cfg["trucks"]["position_ping_interval_seconds"]
            logger.debug(
                "Truck %s ping: status=%s lat=%.4f lon=%.4f zone=%s",
                self.truck_id, self.status, self.lat, self.lon, self.current_zone_id,
            )

        elapsed = now - self._leg_start
        fraction = min(elapsed / self._leg_seconds, 1.0)
        origin_lat, origin_lon = self._leg_origin
        dest_lat, dest_lon = self._leg_destination
        self.lat = origin_lat + (dest_lat - origin_lat) * fraction
        self.lon = origin_lon + (dest_lon - origin_lon) * fraction

        if fraction < 1.0:
            return

        if self.status == "in_transit":
            stop = self.route[self.route_index]
            self.current_zone_id = stop["zone"]["zone_id"]
            for order in stop["orders"]:
                # One order's deliver() must never prevent route_index from
                # advancing below, or the truck gets permanently stuck
                # re-attempting the same stop forever - confirmed happening
                # in practice from the double-dispatch bug this guards
                # against (see Dispatcher's `claimed` flag); this is a
                # second, independent layer of fault isolation.
                try:
                    order.deliver(producer)
                except ValueError:
                    logger.exception(
                        "Truck %s: order %s failed to deliver at %s - skipping it, "
                        "continuing the run",
                        self.truck_id, order.purchase_order_id, stop["zone"]["city"],
                    )
            logger.debug(
                "Truck %s delivered %d order(s) at %s (stop %d/%d)",
                self.truck_id, len(stop["orders"]), stop["zone"]["city"],
                self.route_index + 1, len(self.route),
            )
            self.route_index += 1
            if self.route_index < len(self.route):
                next_stop = self.route[self.route_index]["zone"]
                self._start_leg(stop["zone"]["lat"], stop["zone"]["lon"], next_stop)
            else:
                self.status = "returning"
                self.current_zone_id = None
                self._start_leg(stop["zone"]["lat"], stop["zone"]["lon"], self.warehouse)
                logger.debug("Truck %s heading back to warehouse", self.truck_id)
        elif self.status == "returning":
            self.status = "free"
            self.manifest = []
            self.route = []
            self.route_index = 0
            self.current_zone_id = None
            self._ping(producer)
            on_free(self)
            logger.debug("Truck %s back at warehouse, now free", self.truck_id)
