"""
Ingest daily orders CSV into the Bronze layer.

The expected column set is derived at runtime from L{BronzeOrderRow.model_fields},
making it the single source of truth for orders structure.  The redundant
C{EXPECTED_COLUMNS} list and the C{check_schema()} call have been removed;
L{assert_bronze_columns} replaces them with a one-line column-presence check.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.utils.exceptions import IngestionError, SchemaError
from src.utils.logging_setup import log_event
from src.utils.schemas import BronzeOrderRow


def _assert_bronze_columns(df: pd.DataFrame, source_name: str) -> None:
    """
    Verify that C{df} contains every column declared in L{BronzeOrderRow}.

    Extra columns (additive drift) are permitted and logged as a warning.
    Missing columns (subtractive drift) raise L{SchemaError} immediately,
    preventing corrupt data from reaching the Bronze parquet file.

    Columns are derived from C{BronzeOrderRow.model_fields} so this check
    and the Silver Pydantic model share a single field definition.

    @param df:          DataFrame loaded from the raw CSV source file.
    @param source_name: Label used in log and error messages (e.g. C{"orders"}).
    @raises SchemaError: If one or more required columns are absent from C{df}.
    """
    expected = set(BronzeOrderRow.model_fields)
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


def ingest_orders(
    date_str: str,
    landing_dir: Path,
    bronze_dir: Path,
    logger: logging.Logger,
) -> Path:
    """
    Read C{orders_YYYY-MM-DD.csv} from C{landing_dir} and write it to the
    Bronze layer as a partitioned Parquet file.

    Steps:

      1. Locate and read the source CSV.
      2. Assert column presence against L{BronzeOrderRow.model_fields}.
      3. Attach Bronze metadata columns (C{_source_file}, C{_ingested_at},
         C{_partition_date}).
      4. Write to C{bronze_dir/orders/date=YYYY-MM-DD/data.parquet}.

    @param date_str:    ISO date string (C{YYYY-MM-DD}) identifying the run
                        partition.
    @param landing_dir: Directory containing the raw CSV landing files.
    @param bronze_dir:  Root directory for Bronze-layer Parquet output.
    @param logger:      Structured logger instance for this pipeline run.
    @raises IngestionError: If the source CSV file does not exist.
    @raises SchemaError:    If required columns are missing from the CSV.
    @return: Path to the written Bronze Parquet file.
    """
    src = landing_dir / f"orders_{date_str}.csv"
    if not src.exists():
        raise IngestionError(f"orders file not found: {src}")

    df = pd.read_csv(src)
    log_event(logger, "INFO", "orders_ingested", date=date_str, rows=len(df))

    _assert_bronze_columns(df, "orders")

    # Attach Bronze metadata
    df["_source_file"] = src.name
    df["_ingested_at"] = datetime.now(UTC).isoformat()
    df["_partition_date"] = date_str

    out_dir = bronze_dir / "orders" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)

    log_event(logger, "INFO", "orders_bronze_written", path=str(out_path), rows=len(df))
    return out_path
