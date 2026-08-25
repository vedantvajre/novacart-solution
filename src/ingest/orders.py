"""Ingest daily orders CSV → Bronze parquet."""
from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.transform.schema_check import check_schema
from src.utils.exceptions import IngestionError
from src.utils.logging_setup import log_event

EXPECTED_COLUMNS = [
    "order_id", "customer_id", "product_id",
    "order_date", "quantity", "unit_price", "status",
]


def ingest_orders(
    date_str: str,
    landing_dir: Path,
    bronze_dir: Path,
    logger: logging.Logger,
) -> Path:
    """Read orders_YYYY-MM-DD.csv and write to Bronze layer. Returns output path."""
    src = landing_dir / f"orders_{date_str}.csv"
    if not src.exists():
        raise IngestionError(f"orders file not found: {src}")

    df = pd.read_csv(src)
    log_event(logger, "INFO", "orders_ingested", date=date_str, rows=len(df))

    check_schema(list(df.columns), EXPECTED_COLUMNS, "orders", logger)

    # Add Bronze metadata
    df["_source_file"] = src.name
    df["_ingested_at"] = datetime.now(UTC).isoformat()
    df["_partition_date"] = date_str

    out_dir = bronze_dir / "orders" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)

    log_event(logger, "INFO", "orders_bronze_written", path=str(out_path), rows=len(df))
    return out_path
