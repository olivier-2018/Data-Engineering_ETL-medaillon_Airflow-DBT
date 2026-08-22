"""Loads config.yaml - the single source of truth for every generator-tunable
parameter (scenario sizing, per-domain rates, geography). See
docs/DEVELOPMENT.md for the full field reference."""
from __future__ import annotations

import os
from pathlib import Path

import yaml

CONFIG_PATH = Path(os.environ.get("GENERATOR_CONFIG_PATH", "/app/config.yaml"))


def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


class Settings:
    """Thin typed wrapper around the raw config dict."""

    def __init__(self, raw: dict):
        self.raw = raw

    @property
    def origin(self) -> tuple[float, float]:
        w = self.raw["warehouse"]
        return w["origin_lat"], w["origin_lon"]

    @property
    def num_products(self) -> int:
        return self.raw["products"]["num_products"]

    @property
    def num_customers(self) -> int:
        return self.raw["customers"]["num_customers"]

    @property
    def num_trucks(self) -> int:
        return self.raw["trucks"]["num_trucks"]

    @property
    def countries(self) -> list[str]:
        return self.raw["customers"]["countries"]

    @property
    def zones(self) -> dict:
        return self.raw["zones"]

    @property
    def categories(self) -> list[str]:
        return self.raw["products"]["categories"]

    def zone_for(self, country: str) -> dict:
        return self.zones[country]


def get_settings() -> Settings:
    return Settings(load_config())
