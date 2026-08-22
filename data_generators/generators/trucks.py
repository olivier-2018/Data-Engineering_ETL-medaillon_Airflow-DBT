"""Truck/shipment generator - the only domain feeding a persistent Spark
Structured Streaming job downstream, so it's the one place freshness
(position_ping_interval_seconds) really matters.

No route is modeled (per the user's explicit simplification): a destination
point is picked once at dispatch within the customer's country bounding box,
and position is advanced via straight-line interpolation over the simulated
time-to-destination - realistic enough for a live map, without waypoint/route
data. Concurrency is hard-bounded by `trucks.num_trucks` (§1b): an order
waits in `ready_for_dispatch` until a truck frees up.
"""
from __future__ import annotations

import math
import random
import time
import uuid
from datetime import datetime, timezone

from kafka_producer import send_event

TOPIC = "iot.truck_position_events"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


class TruckFleet:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        n = cfg["trucks"]["num_trucks"]
        self.free_trucks: list[str] = [f"TRUCK-{i:02d}" for i in range(1, n + 1)]
        # shipment_id -> state
        self.active_shipments: dict[str, dict] = {}
        self._truck_of_shipment: dict[str, str] = {}

    def has_free_truck(self) -> bool:
        return len(self.free_trucks) > 0

    def dispatch(self, producer, order_id: str, destination_country: str) -> str | None:
        """Assigns a free truck to a newly ready-for-dispatch order. Returns
        the shipment_id, or None if no truck is currently free (caller should
        leave the order in ready_for_dispatch and retry next tick)."""
        if not self.free_trucks:
            return None

        truck_id = self.free_trucks.pop(0)
        shipment_id = str(uuid.uuid4())

        origin_lat, origin_lon = self.cfg["warehouse"]["origin_lat"], self.cfg["warehouse"]["origin_lon"]
        zone = self.cfg["zones"][destination_country]
        dest_lat = random.uniform(zone["lat_min"], zone["lat_max"])
        dest_lon = random.uniform(zone["lon_min"], zone["lon_max"])

        distance_km = _haversine_km(origin_lat, origin_lon, dest_lat, dest_lon)
        avg_speed = self.cfg["trucks"]["avg_speed_kmh"]
        estimated_seconds = max((distance_km / avg_speed) * 3600.0, 60.0)

        now = time.monotonic()
        self.active_shipments[shipment_id] = {
            "truck_id": truck_id,
            "order_id": order_id,
            "destination_country": destination_country,
            "origin": (origin_lat, origin_lon),
            "destination": (dest_lat, dest_lon),
            "start_time": now,
            "estimated_seconds": estimated_seconds,
            "next_ping_at": now,  # ping immediately on first tick
            "status": "loading",
        }
        self._truck_of_shipment[shipment_id] = truck_id

        event = {
            "event_id": str(uuid.uuid4()),
            "shipment_id": shipment_id,
            "truck_id": truck_id,
            "order_id": order_id,
            "lat": origin_lat,
            "lon": origin_lon,
            "shipment_status": "loading",
            "destination_country": destination_country,
            "event_at": _now(),
        }
        send_event(producer, TOPIC, key=shipment_id, value=event)
        return shipment_id

    def tick(self, producer) -> list[str]:
        """Emits due position pings; returns order_ids whose shipment was
        just delivered this tick (caller marks the order delivered)."""
        ping_interval = self.cfg["trucks"]["position_ping_interval_seconds"]
        now = time.monotonic()
        delivered_order_ids: list[str] = []

        for shipment_id, s in list(self.active_shipments.items()):
            if now < s["next_ping_at"]:
                continue

            elapsed = now - s["start_time"]
            fraction = min(elapsed / s["estimated_seconds"], 1.0)
            origin_lat, origin_lon = s["origin"]
            dest_lat, dest_lon = s["destination"]
            lat = origin_lat + (dest_lat - origin_lat) * fraction
            lon = origin_lon + (dest_lon - origin_lon) * fraction
            status = "in_transit" if fraction < 1.0 else "delivered"

            event = {
                "event_id": str(uuid.uuid4()),
                "shipment_id": shipment_id,
                "truck_id": s["truck_id"],
                "order_id": s["order_id"],
                "lat": lat,
                "lon": lon,
                "shipment_status": status,
                "destination_country": s["destination_country"],
                "event_at": _now(),
            }
            send_event(producer, TOPIC, key=shipment_id, value=event)

            if status == "delivered":
                self.free_trucks.append(s["truck_id"])
                delivered_order_ids.append(s["order_id"])
                del self.active_shipments[shipment_id]
                del self._truck_of_shipment[shipment_id]
            else:
                s["next_ping_at"] = now + ping_interval
                s["status"] = status

        return delivered_order_ids
