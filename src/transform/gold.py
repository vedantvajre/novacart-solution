"""
Silver → Gold transforms: SCD Type 1 & 2 dimensions plus an idempotent
fact_orders partition.

Each builder function asserts the shape of its output DataFrame via
L{assert_gold_schema} before writing to Parquet.  Silver has already
guaranteed row-level quality; the Gold assertion is a cheap, single-call
structural guard that catches missing derived columns or unexpected dtype
regressions introduced by upstream changes.

Gold schema contracts
=====================

  - C{dim_product}  → product attributes + C{_updated_at} (SCD1 snapshot)
  - C{dim_customer} → customer attributes + SCD2 columns (C{_eff_start},
                       C{_eff_end}, C{_current}, C{_row_hash})
  - C{fact_orders}  → order measures + C{total_amount} derived column
"""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from src.utils.logging_setup import log_event
from src.utils.schemas import (
    CustomerRow,
    OrderRow,
    ProductRow,
    assert_gold_schema,
    silver_schema,
)

_HIGH_DATE = "9999-12-31"

# ── Gold schema contracts ─────────────────────────────────────────────────────
#
# Business-attribute columns are derived from the Silver models via
# silver_schema() so that adding a field to CustomerRow (or any Silver model)
# propagates here automatically — no manual update required.
#
# Only Gold-specific derived / SCD2 columns are declared explicitly below.

#: dim_product: Silver attributes + the SCD1 pipeline timestamp.
_DIM_PRODUCT_SCHEMA: dict[str, type] = {
    **silver_schema(ProductRow),
    "_updated_at": str,
}

#: dim_customer: Silver attributes + SCD2 bookkeeping columns.
_DIM_CUSTOMER_SCHEMA: dict[str, type] = {
    **silver_schema(CustomerRow),
    "_eff_start": str,
    "_eff_end": str,
    "_current": bool,
    "_row_hash": str,
}

#: fact_orders: Silver attributes + the C{total_amount} derived metric.
#: C{order_date} is re-declared as C{datetime} because pd.to_datetime()
#: promotes the Silver C{date} column to C{datetime64[us]} at Gold.
_FACT_ORDERS_SCHEMA: dict[str, type] = {
    **silver_schema(OrderRow),
    "order_date": datetime,  # override: date -> datetime64 after pd.to_datetime()
    "total_amount": float,
}


def _row_hash(row: pd.Series, fields: list[str]) -> str:
    """
    Compute a deterministic SHA-256 hex digest over a sorted subset of row fields.

    Fields are sorted before hashing to ensure consistent results regardless
    of DataFrame column order.  SHA-256 replaces MD5 (H-4) to meet IBM
    security policy which prohibits MD5 for any hashing purpose.

    @param row:    A pandas Series representing one customer row.
    @param fields: Column names whose values contribute to the hash.
    @return: Lowercase hexadecimal SHA-256 digest string.
    """
    val = "|".join(str(row.get(f, "")) for f in sorted(fields))
    return hashlib.sha256(val.encode()).hexdigest()


# ── dim_product: SCD Type 1 (overwrite) ──────────────────────────────────────


def build_dim_product(
    silver_dir: Path,
    gold_dir: Path,
    logger: logging.Logger,
) -> Path:
    """
    Build the C{dim_product} Gold table using SCD Type 1 (full overwrite).

    Each pipeline run replaces the entire dimension with the latest Silver
    snapshot.  Internal Bronze/Silver metadata columns (prefixed with C{_})
    are stripped before writing.  A C{_updated_at} pipeline-run timestamp
    is added to the output.

    Validates the output shape against L{_DIM_PRODUCT_SCHEMA} before writing.

    @param silver_dir: Root directory for Silver-layer Parquet files.
    @param gold_dir:   Root directory for Gold-layer Parquet output.
    @param logger:     Structured logger instance for this pipeline run.
    @raises SchemaError: If the output DataFrame is missing a required column
                         or has a dtype mismatch.
    @return: Path to the written (or pre-existing) Gold Parquet file.
    """
    src = silver_dir / "products" / "data.parquet"
    out_dir = gold_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "dim_product.parquet"

    if not src.exists():
        log_event(logger, "INFO", "dim_product_skipped_no_data")
        return out_path

    df = pd.read_parquet(src)
    if df.empty:
        log_event(logger, "INFO", "dim_product_skipped_no_data")
        return out_path

    # SCD1 — keep the latest snapshot; strip internal pipeline columns
    df = df[[c for c in df.columns if not c.startswith("_")]].copy()
    df["_updated_at"] = datetime.now(UTC).isoformat()

    assert_gold_schema(df, _DIM_PRODUCT_SCHEMA, "dim_product")

    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "dim_product_written", rows=len(df))
    return out_path


# ── dim_customer: SCD Type 2 (track history) ─────────────────────────────────


def build_dim_customer(
    silver_dir: Path,
    gold_dir: Path,
    scd2_fields: list[str],
    logger: logging.Logger,
) -> Path:
    """
    Build the C{dim_customer} Gold table using SCD Type 2 (history tracking).

    On first load all rows are opened with C{_eff_start = today} and
    C{_eff_end = 9999-12-31}.  On subsequent loads:

      - Unchanged rows are left untouched.
      - Changed rows have their existing record expired (C{_eff_end = today},
        C{_current = False}) and a new record opened.
      - Brand-new customers are appended as open rows.

    Validates the output shape against L{_DIM_CUSTOMER_SCHEMA} before writing.

    @param silver_dir:  Root directory for Silver-layer Parquet files.
    @param gold_dir:    Root directory for Gold-layer Parquet output.
    @param scd2_fields: Column names whose changes trigger a new SCD2 version.
    @param logger:      Structured logger instance for this pipeline run.
    @raises SchemaError: If the output DataFrame is missing a required column
                         or has a dtype mismatch.
    @return: Path to the written (or pre-existing) Gold Parquet file.
    """
    src = silver_dir / "customers" / "data.parquet"
    out_dir = gold_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "dim_customer.parquet"

    if not src.exists():
        log_event(logger, "INFO", "dim_customer_skipped_no_data")
        return out_path

    incoming = pd.read_parquet(src)
    incoming = incoming[[c for c in incoming.columns if not c.startswith("_")]].copy()
    today = datetime.now(UTC).date().isoformat()

    if not out_path.exists():
        # First load — open all rows
        incoming["_eff_start"] = today
        incoming["_eff_end"] = _HIGH_DATE
        incoming["_current"] = True
        incoming["_row_hash"] = incoming.apply(lambda r: _row_hash(r, scd2_fields), axis=1)
        assert_gold_schema(incoming, _DIM_CUSTOMER_SCHEMA, "dim_customer")
        incoming.to_parquet(out_path, index=False)
        log_event(logger, "INFO", "dim_customer_initial_load", rows=len(incoming))
        return out_path

    existing = pd.read_parquet(out_path)
    current = existing[existing["_current"]][["customer_id", "_row_hash"]].copy()

    # Compute hashes for all incoming rows in one vectorised pass (H-6).
    incoming["_new_hash"] = incoming.apply(lambda r: _row_hash(r, scd2_fields), axis=1)

    # Single O(n log n) join replaces O(n×m) per-row filter loop (H-6).
    merged = incoming.merge(current, on="customer_id", how="left")

    brand_new = merged["_row_hash"].isna()
    changed = ~brand_new & (merged["_new_hash"] != merged["_row_hash"])

    # Expire changed rows in existing via vectorised mask — no per-row .at[] (H-6).
    changed_ids = merged.loc[changed, "customer_id"].values
    expire_mask = existing["customer_id"].isin(changed_ids) & existing["_current"]
    existing.loc[expire_mask, "_eff_end"] = today
    existing.loc[expire_mask, "_current"] = False

    # Build new/replacement rows for brand-new and changed customers.
    to_open = merged.loc[brand_new | changed].copy()
    to_open["_eff_start"] = today
    to_open["_eff_end"] = _HIGH_DATE
    to_open["_current"] = True
    to_open["_row_hash"] = to_open["_new_hash"]
    to_open = to_open.drop(columns=["_new_hash"])

    frames = [existing]
    if not to_open.empty:
        frames.append(to_open)

    result = pd.concat(frames, ignore_index=True)
    assert_gold_schema(result, _DIM_CUSTOMER_SCHEMA, "dim_customer")
    result.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "dim_customer_written", rows=len(result), new=len(to_open))
    return out_path


# ── fact_orders: idempotent partition-replace ─────────────────────────────────


def build_fact_orders(
    date_str: str,
    silver_dir: Path,
    gold_dir: Path,
    logger: logging.Logger,
) -> Path:
    """
    Build one C{fact_orders} date partition using an idempotent full-replace
    strategy.

    Internal pipeline columns are stripped, C{order_date} is cast to a pandas
    Timestamp, and the derived C{total_amount} column is computed as
    C{quantity * unit_price}.  Running this function twice for the same
    C{date_str} produces identical output.

    Validates the output shape against L{_FACT_ORDERS_SCHEMA} before writing.

    @param date_str:   ISO date string (C{YYYY-MM-DD}) identifying the
                       partition to build.
    @param silver_dir: Root directory for Silver-layer Parquet files.
    @param gold_dir:   Root directory for Gold-layer Parquet output.
    @param logger:     Structured logger instance for this pipeline run.
    @raises SchemaError: If the output DataFrame is missing a required column
                         or has a dtype mismatch.
    @return: Path to the written (or pre-existing) Gold Parquet file.
    """
    src = silver_dir / "orders" / f"date={date_str}" / "data.parquet"
    out_dir = gold_dir / "fact_orders" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "data.parquet"

    if not src.exists():
        log_event(logger, "INFO", "fact_orders_skipped_no_data", date=date_str)
        return out_path

    df = pd.read_parquet(src)
    df = df[[c for c in df.columns if not c.startswith("_")]].copy()

    # Derived metrics
    df["order_date"] = pd.to_datetime(df["order_date"])
    df["total_amount"] = df["quantity"] * df["unit_price"]

    assert_gold_schema(df, _FACT_ORDERS_SCHEMA, "fact_orders")

    # Idempotent: full replace of this date partition
    df.to_parquet(out_path, index=False)
    log_event(logger, "INFO", "fact_orders_written", date=date_str, rows=len(df))
    return out_path
