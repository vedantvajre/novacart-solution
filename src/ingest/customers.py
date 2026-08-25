"""
Ingest customers nested JSON into the Bronze layer.

The expected column set is derived at runtime from
L{BronzeCustomerRow.model_fields}, making it the single source of truth
for customers structure.  The redundant C{EXPECTED_COLUMNS} list and the
C{check_schema()} call have been removed; L{_assert_bronze_columns} replaces
them with a one-line column-presence check.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.utils.exceptions import IngestionError, SchemaError
from src.utils.logging_setup import log_event
from src.utils.schemas import BronzeCustomerRow


def _assert_bronze_columns(df: pd.DataFrame, source_name: str) -> None:
    """
    Verify that C{df} contains every column declared in L{BronzeCustomerRow}.

    Extra columns (additive drift) are permitted and logged as a warning.
    Missing columns (subtractive drift) raise L{SchemaError} immediately,
    preventing corrupt data from reaching the Bronze parquet file.

    Columns are derived from C{BronzeCustomerRow.model_fields} so this check
    and the Silver Pydantic model share a single field definition.

    @param df:          DataFrame loaded from the flattened JSON source.
    @param source_name: Label used in log and error messages
                        (e.g. C{"customers"}).
    @raises SchemaError: If one or more required columns are absent from C{df}.
    """
    expected = set(BronzeCustomerRow.model_fields)
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


def ingest_customers(
    landing_dir: Path,
    bronze_dir: Path,
    logger: logging.Logger,
) -> Path:
    """
    Flatten C{customers.json} from C{landing_dir} and write it to the Bronze
    layer as a Parquet file.

    The source JSON contains a nested C{address} object per record.  This
    function extracts C{city} and C{country} from that sub-object before
    writing.

    Steps:

      1. Read and parse the source JSON file.
      2. Flatten the nested C{address} field into top-level C{city} and
         C{country} columns.
      3. Assert column presence against L{BronzeCustomerRow.model_fields}.
      4. Attach Bronze metadata columns (C{_source_file}, C{_ingested_at}).
      5. Write to C{bronze_dir/customers/data.parquet}.

    @param landing_dir: Directory containing the raw JSON landing files.
    @param bronze_dir:  Root directory for Bronze-layer Parquet output.
    @param logger:      Structured logger instance for this pipeline run.
    @raises IngestionError: If C{customers.json} does not exist.
    @raises SchemaError:    If required columns are missing after flattening.
    @return: Path to the written Bronze Parquet file.
    """
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

    _assert_bronze_columns(df, "customers")

    df["_source_file"] = src.name
    df["_ingested_at"] = datetime.now(UTC).isoformat()

    out_dir = bronze_dir / "customers"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)

    log_event(logger, "INFO", "customers_bronze_written", path=str(out_path), rows=len(df))
    return out_path
