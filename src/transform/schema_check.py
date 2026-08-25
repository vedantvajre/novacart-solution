"""Schema drift detection: additive drift warns, subtractive drift raises."""
from __future__ import annotations
import logging
from src.utils.exceptions import SchemaError
from src.utils.logging_setup import log_event


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
        log_event(logger, "warning", "schema_drift",
                  source=source_name, unexpected_columns=sorted(added))

    if missing:
        raise SchemaError(
            f"[schema_drift] {source_name}: required columns missing {sorted(missing)}"
        )
