"""Schema drift detection: additive drift warns, subtractive drift raises."""
from __future__ import annotations

import logging

from src.utils.exceptions import SchemaError


def check_schema(
    df_columns: list[str],
    expected_columns: list[str],
    source_name: str,
    logger: logging.Logger,
) -> None:
    """
    Compare actual DataFrame columns against expected schema.

    - Extra columns (additive drift)  → WARNING, pipeline continues.
    - Missing columns (subtractive drift) → raises SchemaError.
    """
    actual = set(df_columns)
    expected = set(expected_columns)

    added = actual - expected
    missing = expected - actual

    if added:
        logger.warning(
            f"[schema_drift] {source_name}: unexpected columns {sorted(added)} — ignoring extras"
        )

    if missing:
        raise SchemaError(
            f"[schema_drift] {source_name}: required columns missing {sorted(missing)}"
        )
