"""Shared distance helpers - straight-line, not a real routing engine
(matches the project's existing simplification for truck travel-time
simulation, extended to the truck-consolidation route-building logic)."""
from __future__ import annotations

import math

# Real roads aren't great-circle lines - this approximates the extra
# distance/time from terrain and road shape rather than assuming an
# as-the-crow-flies journey. Applied to every travel-time calculation
# (delivery-date estimation, truck leg durations), not to raw distance
# reporting - use haversine_km directly if you need the unadjusted figure.
_ROAD_DISTANCE_PENALTY = 1.15


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def road_distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    return haversine_km(lat1, lon1, lat2, lon2) * _ROAD_DISTANCE_PENALTY
