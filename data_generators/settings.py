"""Loads config.yaml - the single source of truth for every generator
hyperparameter (rates, probabilities, durations, counts). Structural/
reference data (delivery zones, product categories, weather stations) lives
in Postgres instead - see config.yaml's own header comment and the redesign
plan §2a/§3a/decision #12."""
from __future__ import annotations

import os
from pathlib import Path

import yaml

CONFIG_PATH = Path(os.environ.get("GENERATOR_CONFIG_PATH", "/app/config.yaml"))


def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)
