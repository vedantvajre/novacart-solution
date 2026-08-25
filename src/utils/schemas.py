"""
Layer-scoped Pydantic schema contracts for medallion pipeline.

Architecture
============
Each entity has models at two layers plus a Gold shape assertion helper::

    Bronze  →  BronzeOrderRow, BronzeCustomerRow, BronzeProductRow
               All fields typed as C{str}.  Config: strict=False,
               coerce_numbers_to_str=True.  Used exclusively to derive
               the expected column set at ingest — never instantiated
               per-row.

    Silver  →  OrderRow, CustomerRow, ProductRow
               Inherit from their Bronze counterpart so field names are
               defined once.  Override field types with proper Python
               types and add business-rule validators.

    Gold    →  assert_gold_schema(df, expected)
               A single DataFrame-level shape assertion (column names +
               dtypes).  No row-level Pydantic models — Silver has
               already guaranteed row quality; Gold only adds derived
               columns whose shape can be checked in one call.
"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator

from src.utils.exceptions import SchemaError

# Bronze Models


class _BronzeBase(BaseModel):
    """
    Base configuration for all Bronze-layer models.

    Tolerates raw CSV/JSON strings arriving for every field:

      - C{strict=False}               — relaxed type coercion mode.
      - C{coerce_numbers_to_str=True} — numeric columns that arrive as
                                        integers or floats are silently
                                        cast to C{str} rather than
                                        raising a validation error.

    Bronze models are used *only* to derive the expected column set via
    C{model_fields}.  They are never instantiated per data row.
    """

    model_config = ConfigDict(strict=False, coerce_numbers_to_str=True)


class BronzeOrderRow(_BronzeBase):
    """
    Structural contract for a raw orders row at the Bronze layer.

    All fields are typed as C{str} to tolerate CSV source data.
    Field names are the single source of truth consumed by
    L{src.ingest.orders.ingest_orders} to validate column presence.

    @ivar order_id:    Unique order identifier.
    @ivar customer_id: Foreign key to the customer dimension.
    @ivar product_id:  Foreign key to the product dimension.
    @ivar order_date:  ISO-8601 date string as received from source.
    @ivar quantity:    Raw quantity string; typed and validated at Silver.
    @ivar unit_price:  Raw price string; typed and validated at Silver.
    @ivar status:      Raw status string; allowed-list enforced at Silver.
    """

    order_id: str
    customer_id: str
    product_id: str
    order_date: str
    quantity: str
    unit_price: str
    status: str


class BronzeCustomerRow(_BronzeBase):
    """
    Structural contract for a raw customers row at the Bronze layer.

    @ivar customer_id: Unique customer identifier.
    @ivar first_name:  Customer given name.
    @ivar last_name:   Customer family name.
    @ivar email:       Raw email string; RFC-5322 validation at Silver.
    @ivar city:        City extracted from nested address object.
    @ivar country:     Country extracted from nested address object.
    @ivar signup_date: ISO-8601 date string as received from source.
    @ivar tier:        Optional customer tier string; defaults to C{None}
                       at Bronze — default applied at Silver.
    """

    customer_id: str
    first_name: str
    last_name: str
    email: str
    city: str
    country: str
    signup_date: str
    tier: str | None = None


class BronzeProductRow(_BronzeBase):
    """
    Structural contract for a raw products row at the Bronze layer.

    @ivar product_id:  Unique product identifier.
    @ivar name:        Product display name.
    @ivar category:    Product category string.
    @ivar unit_cost:   Raw cost string; typed and validated at Silver.
    @ivar supplier_id: Supplier identifier.
    @ivar updated_at:  ISO-8601 timestamp string from the SQLite source.
    """

    product_id: str
    name: str
    category: str
    unit_cost: str
    supplier_id: str
    updated_at: str


# Silver Models


class OrderRow(BronzeOrderRow):
    """
    Silver-layer contract for a validated orders row.

    Inherits field names from L{BronzeOrderRow} and narrows types to
    proper Python primitives.  Business rules are enforced via
    C{field_validator} methods.

    @ivar order_date: Parsed as a C{date} object; coerced from ISO string.
    @ivar quantity:   Positive integer (> 0).
    @ivar unit_price: Non-negative float (>= 0.0).
    @ivar status:     One of: pending, shipped, delivered, cancelled,
                      returned.  Normalised to lowercase on ingestion.
    """

    model_config = ConfigDict(strict=False, coerce_numbers_to_str=False)

    order_date: date  # type: ignore[assignment]
    quantity: int  # type: ignore[assignment]
    unit_price: float  # type: ignore[assignment]

    @field_validator("quantity")
    @classmethod
    def qty_positive(cls, v: int) -> int:
        """
        Reject zero or negative quantities.

        @param v: Raw quantity value after type coercion.
        @raises ValueError: If C{v} is not greater than zero.
        @return: The validated quantity unchanged.
        """
        if v <= 0:
            raise ValueError(f"quantity must be > 0, got {v}")
        return v

    @field_validator("unit_price")
    @classmethod
    def price_non_negative(cls, v: float) -> float:
        """
        Reject negative unit prices.

        @param v: Raw unit_price value after type coercion.
        @raises ValueError: If C{v} is less than zero.
        @return: The validated price unchanged.
        """
        if v < 0:
            raise ValueError(f"unit_price must be >= 0, got {v}")
        return v

    @field_validator("status")
    @classmethod
    def valid_status(cls, v: str) -> str:
        """
        Enforce the order-status allowed list and normalise to lowercase.

        @param v: Raw status string.
        @raises ValueError: If C{v} is not in the allowed set.
        @return: Lowercase status string.
        """
        allowed = {"pending", "shipped", "delivered", "cancelled", "returned"}
        if v.lower() not in allowed:
            raise ValueError(f"status '{v}' not in {allowed}")
        return v.lower()


class CustomerRow(BronzeCustomerRow):
    """
    Silver-layer contract for a validated customers row.

    Inherits field names from L{BronzeCustomerRow} and narrows types.

    @ivar signup_date: Parsed as a C{date} object.
    @ivar email:       Validated for C{@} presence; normalised to
                       lowercase.  (Full RFC-5322 validation is tracked
                       under M-2.)
    @ivar tier:        Defaults to C{"standard"} when absent.
    """

    model_config = ConfigDict(strict=False, coerce_numbers_to_str=False)

    signup_date: date  # type: ignore[assignment]
    tier: str | None = "standard"

    @field_validator("email")
    @classmethod
    def email_has_at(cls, v: str) -> str:
        """
        Require an C{@} character and normalise the email to lowercase.

        @param v: Raw email string.
        @raises ValueError: If C{v} does not contain C{@}.
        @return: Lowercase email string.
        """
        if "@" not in v:
            raise ValueError(f"invalid email: {v}")
        return v.lower()


class ProductRow(BronzeProductRow):
    """
    Silver-layer contract for a validated products row.

    Inherits field names from L{BronzeProductRow} and narrows types.

    @ivar unit_cost:  Non-negative float (>= 0.0).
    @ivar updated_at: Kept as C{str} (ISO timestamp from SQLite).
                      Full C{datetime} typing is tracked under M-4.
    """

    model_config = ConfigDict(strict=False, coerce_numbers_to_str=False)

    unit_cost: float  # type: ignore[assignment]

    @field_validator("unit_cost")
    @classmethod
    def cost_non_negative(cls, v: float) -> float:
        """
        Reject negative unit costs.

        @param v: Raw unit_cost value after type coercion.
        @raises ValueError: If C{v} is less than zero.
        @return: The validated cost unchanged.
        """
        if v < 0:
            raise ValueError(f"unit_cost must be >= 0, got {v}")
        return v


# Gold Helper Methods


def silver_schema(model: type[BaseModel]) -> dict[str, type]:
    """
    Derive a Gold-compatible column-to-type mapping from a Silver Pydantic model.

    Iterates C{model.model_fields} and maps each field's outer annotation to
    the nearest Python primitive understood by L{assert_gold_schema}.  This
    ensures the business-attribute columns of every Gold table stay in sync
    with their Silver counterpart automatically — adding a field to
    C{CustomerRow} propagates to C{_DIM_CUSTOMER_SCHEMA} without any manual
    update.

    Type mapping rules:

      - C{date}, C{datetime}       → C{datetime} (pandas Timestamp / datetime64)
      - C{int}                     → C{int}
      - C{float}                   → C{float}
      - C{bool}                    → C{bool}
      - C{str}, C{Optional[str]},
        C{str | None}, anything else → C{str}

    @param model: A Silver-layer Pydantic model class (subclass of
                  L{BaseModel}).
    @return: Mapping of field name to Python type, ready to pass as the
             C{expected} argument of L{assert_gold_schema}.
    """
    import types as _types

    _type_map: dict[type, type] = {
        # date stays as str at Gold — pandas reads date columns from Parquet
        # as plain strings unless explicitly cast with pd.to_datetime().
        # Callers that DO cast (e.g. fact_orders order_date) override the
        # entry in their schema dict after the silver_schema() spread.
        datetime: datetime,
        int: int,
        float: float,
        bool: bool,
    }

    result: dict[str, type] = {}
    for name, field_info in model.model_fields.items():
        annotation = field_info.annotation

        # Unwrap Optional[X] / X | None → X
        origin = getattr(annotation, "__origin__", None)
        raw_args = getattr(annotation, "__args__", None)
        if (
            origin is _types.UnionType or origin is getattr(__builtins__, "Union", None)
        ) and raw_args:
            args = [a for a in raw_args if a is not type(None)]
            annotation = args[0] if args else str

        result[name] = _type_map.get(annotation, str)  # type: ignore[arg-type]
    return result


def assert_gold_schema(
    df: pd.DataFrame,
    expected: dict[str, type],
    table_name: str,
) -> None:
    """
    Assert that a Gold-layer DataFrame satisfies its shape contract.

    Checks two things in a single pass over C{expected}:

      1. Every expected column is present in C{df}.
      2. Each column's pandas dtype is compatible with the declared
         Python type (C{int}, C{float}, C{str}, C{bool}, C{object}).

    This replaces row-level Pydantic validation at Gold.  Silver has
    already guaranteed row quality; Gold only adds derived columns
    (e.g. C{total_amount}, C{_eff_start}) whose shape can be asserted
    cheaply against the whole DataFrame in one call.

    @param df:         The Gold-layer output DataFrame to validate.
    @param expected:   Mapping of column name to expected Python type.
                       Use C{object} for mixed-type or string columns
                       stored as C{dtype("O")}.
    @param table_name: Human-readable table name used in error messages.
    @raises SchemaError: If any expected column is missing or has an
                         incompatible dtype.
    """
    dtype_map: dict[type, tuple[str, ...]] = {
        int: ("int", "Int"),
        float: ("float", "Float"),
        str: ("object", "string", "str"),
        bool: ("bool",),
        object: ("object", "string", "str"),
        # datetime columns produced by pd.to_datetime()
        datetime: ("datetime64",),
    }

    missing = [col for col in expected if col not in df.columns]
    if missing:
        raise SchemaError(f"[gold_schema] {table_name}: required columns missing {sorted(missing)}")

    type_errors: list[str] = []
    for col, expected_type in expected.items():
        actual_dtype = str(df[col].dtype)
        if not any(actual_dtype.startswith(p) for p in dtype_map.get(expected_type, ("object",))):
            type_errors.append(
                f"  {col}: expected {expected_type.__name__}, got dtype '{actual_dtype}'"
            )

    if type_errors:
        raise SchemaError(
            f"[gold_schema] {table_name}: dtype mismatches:\n" + "\n".join(type_errors)
        )
