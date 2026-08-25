"""
Ingest products from SQLite with watermark-based incremental loading into the
Bronze layer.

The expected column set is derived at runtime from
L{BronzeProductRow.model_fields}, making it the single source of truth for
products structure.  The redundant C{EXPECTED_COLUMNS} list and the
C{check_schema()} call have been removed; L{_assert_bronze_columns} replaces
them with a one-line column-presence check.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.utils.exceptions import IngestionError, SchemaError
from src.utils.logging_setup import log_event
from src.utils.schemas import BronzeProductRow
from src.utils.state import StateManager

WATERMARK_KEY = "products_updated_at"


def _assert_bronze_columns(df: pd.DataFrame, source_name: str) -> None:
    """
    Verify that C{df} contains every column declared in L{BronzeProductRow}.

    Extra columns (additive drift) are permitted and logged as a warning.
    Missing columns (subtractive drift) raise L{SchemaError} immediately,
    preventing corrupt data from reaching the Bronze parquet file.

    Columns are derived from C{BronzeProductRow.model_fields} so this check
    and the Silver Pydantic model share a single field definition.

    @param df:          DataFrame loaded from the SQLite C{products} table.
    @param source_name: Label used in log and error messages
                        (e.g. C{"products"}).
    @raises SchemaError: If one or more required columns are absent from C{df}.
    """
    expected = set(BronzeProductRow.model_fields)
    actual = set(df.columns)

    added = actual - expected
    missing = expected - actual

    if added:
        log_event(
            logging.getLogger(__name__),
            "WARNING",
            "schema_drift",
            source=source_name,
            unexpected_columns=sorted(added),
        )

    if missing:
        raise SchemaError(
            f"[schema_drift] {source_name}: required columns missing {sorted(missing)}"
        )


def ingest_products(
    db_path: Path,
    bronze_dir: Path,
    state: StateManager,
    logger: logging.Logger,
) -> Path:
    """
    Incrementally load only product rows newer than the stored watermark and
    write them to the Bronze layer as a Parquet file.

    The watermark (stored in C{StateManager} under key
    C{products_updated_at}) is advanced to C{max(updated_at)} of the newly
    ingested rows after a successful write.

    Steps:

      1. Read the current watermark from C{state} (defaults to epoch).
      2. Query SQLite for rows with C{updated_at > watermark}.
      3. Return early if no new rows are found.
      4. Assert column presence against L{BronzeProductRow.model_fields}.
      5. Attach the C{_ingested_at} metadata column.
      6. Write to C{bronze_dir/products/data.parquet}.
      7. Advance the watermark to C{max(updated_at)}.

    @param db_path:    Path to the SQLite database containing the
                       C{products} table.
    @param bronze_dir: Root directory for Bronze-layer Parquet output.
    @param state:      L{StateManager} instance for reading and advancing
                       the incremental watermark.
    @param logger:     Structured logger instance for this pipeline run.
    @raises IngestionError: If the SQLite database file does not exist.
    @raises SchemaError:    If required columns are missing from the query
                            result.
    @return: Path to the written (or pre-existing) Bronze Parquet file.
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
        return out_dir / "data.parquet"

    _assert_bronze_columns(df, "products")

    df["_ingested_at"] = datetime.now(UTC).isoformat()

    out_dir = bronze_dir / "products"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)

    new_watermark = str(df["updated_at"].max())
    state.set_watermark(WATERMARK_KEY, new_watermark)
    log_event(logger, "INFO", "products_watermark_advanced", new_watermark=new_watermark)

    return out_path
