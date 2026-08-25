"""Bronze → Silver: validate with Pydantic, dedupe, quarantine bad rows."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from pydantic import BaseModel, ValidationError

from src.utils.logging_setup import log_event
from src.utils.schemas import CustomerRow, OrderRow, ProductRow


def _validate_df(
    df: pd.DataFrame,
    model: type[BaseModel],
    primary_key: str,
    quarantine_path: Path,
    logger: logging.Logger,
    source_name: str,
) -> pd.DataFrame:
    """Validate each row with Pydantic. Good rows → Silver, bad rows → quarantine.

    Uses C{model.model_validate()} over a list comprehension rather than
    C{iterrows()}, tracking good row indices and collecting bad row dicts only
    on failure.  This avoids constructing a Python C{Series} object per row on
    the happy path (H-5).

    @param df:              Input DataFrame from the Bronze layer.
    @param model:           Pydantic model class used to validate each row.
    @param primary_key:     Column name used for deduplication.
    @param quarantine_path: Root directory for quarantine Parquet output.
    @param logger:          Structured logger instance.
    @param source_name:     Label used in log events and quarantine sub-dir.
    @return: Validated, deduplicated DataFrame ready for Silver output.
    """
    good_indices: list[int] = []
    bad: list[dict] = []

    records = df.to_dict(orient="records")
    for i, record in enumerate(records):
        try:
            model.model_validate(record)
            good_indices.append(i)
        except (ValidationError, Exception) as exc:  # noqa: BLE001
            bad_record = record.copy()
            bad_record["_quarantine_reason"] = str(exc)
            bad_record["_quarantined_at"] = datetime.now(UTC).isoformat()
            bad.append(bad_record)

    if bad:
        q_dir = quarantine_path / source_name
        q_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        pd.DataFrame(bad).to_parquet(q_dir / f"{ts}.parquet", index=False)
        log_event(logger, "WARNING", f"{source_name}_quarantined", count=len(bad))

    result = (
        df.iloc[good_indices].reset_index(drop=True)
        if good_indices
        else pd.DataFrame(columns=df.columns)
    )

    # Deduplicate on primary key — keep last occurrence
    if primary_key in result.columns and not result.empty:
        before = len(result)
        result = result.drop_duplicates(subset=[primary_key], keep="last")
        dupes = before - len(result)
        if dupes:
            log_event(logger, "INFO", f"{source_name}_deduped", dropped=dupes)

    return result.reset_index(drop=True)


def build_silver_orders(
    date_str: str,
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
) -> Path:
    src = bronze_dir / "orders" / f"date={date_str}" / "data.parquet"
    if not src.exists():
        log_event(logger, "WARNING", "silver_orders_no_bronze", date=date_str)
        return silver_dir / "orders" / f"date={date_str}" / "data.parquet"

    df = pd.read_parquet(src)
    df = _validate_df(df, OrderRow, "order_id", quarantine_dir, logger, "orders")

    out_dir = silver_dir / "orders" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_orders_written", rows=len(df), date=date_str)
    return out_path


def build_silver_customers(
    bronze_dir: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
) -> Path:
    src = bronze_dir / "customers" / "data.parquet"
    if not src.exists():
        log_event(logger, "WARNING", "silver_customers_no_bronze")
        return silver_dir / "customers" / "data.parquet"

    df = pd.read_parquet(src)
    df = _validate_df(df, CustomerRow, "customer_id", quarantine_dir, logger, "customers")

    out_dir = silver_dir / "customers"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_customers_written", rows=len(df))
    return out_path


def build_silver_products(
    bronze_src: Path,
    silver_dir: Path,
    quarantine_dir: Path,
    logger: logging.Logger,
) -> Path:
    """Build the Silver products table from a specific Bronze source path (H-2).

    Accepts C{bronze_src} as a direct path rather than a directory so that
    callers can pass the exact partitioned Bronze file returned by
    C{ingest_products} (H-1 two-phase commit pattern).

    @param bronze_src:    Exact path to the Bronze products Parquet file.
    @param silver_dir:    Root directory for Silver-layer Parquet output.
    @param quarantine_dir: Root directory for quarantine Parquet output.
    @param logger:         Structured logger instance.
    @return: Path to the written (or pre-existing) Silver Parquet file.
    """
    src = bronze_src
    if not src.exists():
        log_event(logger, "WARNING", "silver_products_no_bronze")
        return silver_dir / "products" / "data.parquet"

    df = pd.read_parquet(src)
    if df.empty:
        return silver_dir / "products" / "data.parquet"

    df = _validate_df(df, ProductRow, "product_id", quarantine_dir, logger, "products")

    # M-4: cast updated_at string from SQLite to datetime64 at the Silver boundary.
    if "updated_at" in df.columns:
        df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True)

    out_dir = silver_dir / "products"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "silver_products_written", rows=len(df))
    return out_path
