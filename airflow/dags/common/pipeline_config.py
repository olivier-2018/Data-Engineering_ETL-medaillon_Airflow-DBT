"""Single source of truth for DAG schedule cadences, read from
config/airflow/airflow_config.yml (§5/§7 of the redesign plan) - every
bronze_ingest_*_dag.py / silver_*_dag.py file imports the timedeltas below
instead of hardcoding timedelta(minutes=N) per file.

Read at import time (i.e. DAG-parse time), so both airflow-scheduler AND
airflow-dag-processor need this file mounted - unlike
weather_enrichment_dag.py's config read (deliberately deferred into its
task callable so only airflow-scheduler needs that mount), a DAG's
`schedule=` is a parse-time constructor argument, not something that can be
deferred into task-execution code."""
from __future__ import annotations

import os
from datetime import timedelta

import yaml

CONFIG_PATH = os.environ.get("AIRFLOW_CONFIG_PATH", "/opt/config/airflow_config.yml")

with open(CONFIG_PATH) as f:
    _cfg = yaml.safe_load(f)

BRONZE_INGESTION_SCHEDULE = timedelta(minutes=_cfg["bronze_ingestion_schedule_minutes"])
SILVER_PROCESSING_SCHEDULE = timedelta(minutes=_cfg["silver_processing_schedule_minutes"])
TRUCK_POSITIONS_SCHEDULE = timedelta(minutes=_cfg["truck_positions_schedule_minutes"])
