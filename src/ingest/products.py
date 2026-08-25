"""Ingest products from SQLite with watermark-based incremental loading → Bronze parquet."""
from __future__ import annotations
import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.utils.exceptions import IngestionError
from src.utils.logging_setup import log_event
from src.utils.state import StateManager
from src.transform.schema_check import check_schema

EXPECTED_COLUMNS = [
    "product_id", "name", "category", "unit_cost", "supplier_id", "updated_at",
]
WATERMARK_KEY = "products_updated_at"


def ingest_products(
    db_path: Path,
    bronze_dir: Path,
    state: StateManager,
    logger: logging.Logger,
) -> tuple[Path, str | None]:
    """
    Incrementally load only rows newer than the stored watermark.

    Returns (out_path, pending_watermark). The caller is responsible for
    committing the pending watermark to state only after the full pipeline
    run succeeds (two-phase commit pattern).
    """
    if not db_path.exists():
        raise IngestionError(f"products DB not found: {db_path}")

    watermark = state.get_watermark(WATERMARK_KEY) or "1970-01-01T00:00:00"
    log_event(logger, "INFO", "products_watermark_read", watermark=watermark)

    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            "SELECT * FROM products WHERE updated_at > ? ORDER BY updated_at",
            conn,
            params=(watermark,),
        )
    finally:
        conn.close()

    log_event(logger, "INFO", "products_ingested", rows=len(df), since=watermark)

    if df.empty:
        log_event(logger, "INFO", "products_no_new_rows")
        out_dir = bronze_dir / "products"
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir / "data.parquet", None

    check_schema(list(df.columns), EXPECTED_COLUMNS, "products", logger)

    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    out_dir = bronze_dir / "products" / f"ingested_at={ts}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)

    new_watermark = str(df["updated_at"].max())

    return out_path, new_watermark
