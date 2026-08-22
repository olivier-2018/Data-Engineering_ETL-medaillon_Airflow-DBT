"""OpenWeatherMap client for weather_enrichment_dag.py (§8). Plain `requests`
calls rather than HttpHook - simpler and lower-risk to get right without a
live Airflow instance to test against during development.

NOTE: exact current OpenWeatherMap free-tier rate limits and endpoint
versions could not be verified live during planning (fetch attempts hit
JS-rendered marketing pages) - confirm against https://openweathermap.org/api
before relying on this in a long-running demo.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)

CURRENT_URL = "https://api.openweathermap.org/data/2.5/weather"
FORECAST_URL = "https://api.openweathermap.org/data/2.5/forecast"
REQUEST_TIMEOUT_SECONDS = 10


def fetch_observations(location: dict, api_key: str) -> list[dict]:
    """Returns one current-observation row plus a handful of forecast rows
    for a single {name, lat, lon} location."""
    observations: list[dict] = []
    params_common = {"lat": location["lat"], "lon": location["lon"], "appid": api_key, "units": "metric"}

    try:
        resp = requests.get(CURRENT_URL, params=params_common, timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        data = resp.json()
        observations.append(
            {
                "location_name": location["name"],
                "lat": location["lat"],
                "lon": location["lon"],
                "observed_at": datetime.fromtimestamp(data["dt"], tz=timezone.utc),
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "weather_condition": data["weather"][0]["main"] if data.get("weather") else None,
                "forecast_horizon_hours": None,
            }
        )
    except requests.RequestException:
        logger.exception("OpenWeatherMap current-weather call failed for %s", location["name"])

    try:
        resp = requests.get(FORECAST_URL, params=params_common, timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        data = resp.json()
        now = datetime.now(timezone.utc)
        for entry in data.get("list", [])[:4]:  # next ~12h in 3h steps
            forecast_time = datetime.fromtimestamp(entry["dt"], tz=timezone.utc)
            horizon_hours = round((forecast_time - now).total_seconds() / 3600)
            observations.append(
                {
                    "location_name": location["name"],
                    "lat": location["lat"],
                    "lon": location["lon"],
                    "observed_at": forecast_time,
                    "temperature": entry["main"]["temp"],
                    "humidity": entry["main"]["humidity"],
                    "pressure": entry["main"]["pressure"],
                    "weather_condition": entry["weather"][0]["main"] if entry.get("weather") else None,
                    "forecast_horizon_hours": horizon_hours,
                }
            )
    except requests.RequestException:
        logger.exception("OpenWeatherMap forecast call failed for %s", location["name"])

    return observations
