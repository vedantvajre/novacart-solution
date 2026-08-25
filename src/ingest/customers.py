"""Ingest customers nested JSON → Bronze parquet."""
from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.transform.schema_check import check_schema
from src.utils.exceptions import IngestionError
from src.utils.logging_setup import log_event

EXPECTED_COLUMNS = [
    "customer_id", "first_name", "last_name", "email",
    "city", "country", "signup_date", "tier",
]


def ingest_customers(
    landing_dir: Path,
    bronze_dir: Path,
    logger: logging.Logger,
) -> Path:
    """Flatten nested JSON export and write to Bronze. Returns output path."""
    src = landing_dir / "customers.json"
    if not src.exists():
        raise IngestionError(f"customers file not found: {src}")

    raw = json.loads(src.read_text())

    # Flatten: each record has nested address: {city, country}
    rows = []
    for rec in raw:
        address = rec.pop("address", {})
        rec["city"] = address.get("city", "")
        rec["country"] = address.get("country", "")
        rows.append(rec)

    df = pd.DataFrame(rows)
    log_event(logger, "INFO", "customers_ingested", rows=len(df))

    check_schema(list(df.columns), EXPECTED_COLUMNS, "customers", logger)

    df["_source_file"] = src.name
    df["_ingested_at"] = datetime.now(UTC).isoformat()

    out_dir = bronze_dir / "customers"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)

    log_event(logger, "INFO", "customers_bronze_written", path=str(out_path), rows=len(df))
    return out_path
