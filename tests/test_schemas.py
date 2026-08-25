"""
Unit tests for Pydantic schema contracts and Gold schema assertions.

Coverage
========

  - L{BronzeOrderRow}, L{BronzeCustomerRow}, L{BronzeProductRow}:
      field-set derivation and tolerance of raw string input.

  - L{OrderRow}, L{CustomerRow}, L{ProductRow} (Silver):
      type narrowing, business-rule validators, field inheritance from
      Bronze counterparts.

  - L{assert_gold_schema}:
      pass on compliant DataFrames; L{SchemaError} on missing columns
      and dtype mismatches.
"""

from __future__ import annotations

from datetime import date, datetime

import pandas as pd
import pytest
from pydantic import ValidationError

from src.utils.exceptions import SchemaError
from src.utils.schemas import (
    BronzeCustomerRow,
    BronzeOrderRow,
    BronzeProductRow,
    CustomerRow,
    OrderRow,
    ProductRow,
    assert_gold_schema,
)

# ── Bronze model tests ────────────────────────────────────────────────────────


class TestBronzeOrderRow:
    """
    Tests for L{BronzeOrderRow}.

    Verifies that the model exposes the correct field set and tolerates
    raw string values for every field.
    """

    def test_model_fields_match_expected_column_set(self):
        """
        L{BronzeOrderRow.model_fields} must declare exactly the columns
        expected at the orders Bronze layer.

        This is the source of truth consumed by
        L{src.ingest.orders._assert_bronze_columns}.
        """
        expected = {
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "quantity",
            "unit_price",
            "status",
        }
        assert set(BronzeOrderRow.model_fields) == expected

    def test_accepts_all_string_values(self):
        """
        Every field should accept a plain string without raising, because
        Bronze data arrives from CSV where all columns are strings.
        """
        row = BronzeOrderRow(
            order_id="ORD-1",
            customer_id="C1",
            product_id="P1",
            order_date="2025-01-01",
            quantity="2",
            unit_price="9.99",
            status="shipped",
        )
        assert row.order_id == "ORD-1"

    def test_numeric_quantity_coerced_to_str(self):
        """
        When a numeric value is passed for a C{str} field (e.g. from a
        DataFrame with mixed dtypes), C{coerce_numbers_to_str=True} must
        convert it silently.
        """
        row = BronzeOrderRow(
            order_id="ORD-2",
            customer_id="C1",
            product_id="P1",
            order_date="2025-01-01",
            quantity=3,  # int, not str
            unit_price=5.0,  # float, not str
            status="pending",
        )
        assert row.quantity == "3"
        assert row.unit_price == "5.0"


class TestBronzeCustomerRow:
    """Tests for L{BronzeCustomerRow}."""

    def test_model_fields_match_expected_column_set(self):
        """
        L{BronzeCustomerRow.model_fields} must declare exactly the columns
        expected at the customers Bronze layer.
        """
        expected = {
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "city",
            "country",
            "signup_date",
            "tier",
        }
        assert set(BronzeCustomerRow.model_fields) == expected

    def test_tier_defaults_to_none_at_bronze(self):
        """
        C{tier} is optional at Bronze with a default of C{None}.  The
        C{"standard"} default is applied at Silver, not here.
        """
        row = BronzeCustomerRow(
            customer_id="C1",
            first_name="Alice",
            last_name="Smith",
            email="alice@example.com",
            city="NYC",
            country="US",
            signup_date="2024-01-01",
        )
        assert row.tier is None


class TestBronzeProductRow:
    """Tests for L{BronzeProductRow}."""

    def test_model_fields_match_expected_column_set(self):
        """
        L{BronzeProductRow.model_fields} must declare exactly the columns
        expected at the products Bronze layer.
        """
        expected = {
            "product_id",
            "name",
            "category",
            "unit_cost",
            "supplier_id",
            "updated_at",
        }
        assert set(BronzeProductRow.model_fields) == expected

    def test_accepts_all_string_values(self):
        """Every field should accept a plain string without raising."""
        row = BronzeProductRow(
            product_id="P1",
            name="Widget",
            category="Electronics",
            unit_cost="10.0",
            supplier_id="SUP-A",
            updated_at="2025-01-01T00:00:00",
        )
        assert row.product_id == "P1"


# ── Silver model tests ────────────────────────────────────────────────────────


class TestOrderRow:
    """
    Tests for L{OrderRow} (Silver).

    Verifies type narrowing, business-rule validators, and that field names
    are inherited from L{BronzeOrderRow}.
    """

    def _valid(self, **overrides):
        base = {
            "order_id": "ORD-1",
            "customer_id": "C1",
            "product_id": "P1",
            "order_date": date(2025, 1, 1),
            "quantity": 2,
            "unit_price": 9.99,
            "status": "shipped",
        }
        base.update(overrides)
        return base

    def test_valid_order(self):
        """A fully valid row should parse without error."""
        row = OrderRow(**self._valid())
        assert row.order_id == "ORD-1"

    def test_inherits_bronze_field_names(self):
        """
        L{OrderRow} must inherit every field declared in L{BronzeOrderRow}
        so that field names have a single source of truth.
        """
        assert set(BronzeOrderRow.model_fields).issubset(set(OrderRow.model_fields))

    def test_status_normalised_to_lowercase(self):
        """C{status} must be lowercased regardless of input casing."""
        row = OrderRow(**self._valid(status="Shipped"))
        assert row.status == "shipped"

    def test_zero_quantity_rejected(self):
        """Quantity of zero must raise L{ValidationError}."""
        with pytest.raises(ValidationError):
            OrderRow(**self._valid(quantity=0))

    def test_negative_quantity_rejected(self):
        """Negative quantity must raise L{ValidationError}."""
        with pytest.raises(ValidationError):
            OrderRow(**self._valid(quantity=-1))

    def test_negative_price_rejected(self):
        """Negative unit_price must raise L{ValidationError}."""
        with pytest.raises(ValidationError):
            OrderRow(**self._valid(unit_price=-0.01))

    def test_zero_price_allowed(self):
        """A unit_price of zero must be accepted (free items)."""
        row = OrderRow(**self._valid(unit_price=0.0))
        assert row.unit_price == 0.0

    def test_invalid_status_rejected(self):
        """An unrecognised status value must raise L{ValidationError}."""
        with pytest.raises(ValidationError):
            OrderRow(**self._valid(status="refunded"))


class TestCustomerRow:
    """Tests for L{CustomerRow} (Silver)."""

    def _valid(self, **overrides):
        base = {
            "customer_id": "C1",
            "first_name": "Alice",
            "last_name": "Smith",
            "email": "alice@example.com",
            "city": "NYC",
            "country": "US",
            "signup_date": date(2024, 1, 1),
            "tier": "gold",
        }
        base.update(overrides)
        return base

    def test_valid_customer(self):
        """A fully valid row should parse without error."""
        row = CustomerRow(**self._valid())
        assert row.customer_id == "C1"

    def test_inherits_bronze_field_names(self):
        """
        L{CustomerRow} must inherit every field declared in
        L{BronzeCustomerRow}.
        """
        assert set(BronzeCustomerRow.model_fields).issubset(set(CustomerRow.model_fields))

    def test_email_domain_normalised_to_lowercase(self):
        """
        EmailStr must normalise the domain part to lowercase.

        Per RFC 5321 the local part (before C{@}) is technically
        case-sensitive, so C{EmailStr} preserves its case.  Only the
        domain is guaranteed to be lowercased.
        """
        row = CustomerRow(**self._valid(email="Alice@Example.COM"))
        assert row.email.split("@")[1] == "example.com"

    @pytest.mark.parametrize(
        "bad_email",
        [
            "not-an-email",  # no @ at all
            "@",  # bare @ — previously passed the weak check
            "a@",  # missing domain — previously passed
            "@b",  # missing local part — previously passed
            "alice @example.com",  # space in local part
            "alice@example",  # missing TLD
            "",  # empty string
        ],
    )
    def test_invalid_emails_rejected(self, bad_email: str):
        """
        EmailStr must reject malformed addresses that the old C{@}-presence
        check silently passed.

        @param bad_email: An invalid email string that must raise
                          L{ValidationError}.
        """
        with pytest.raises(ValidationError):
            CustomerRow(**self._valid(email=bad_email))

    def test_tier_defaults_to_standard(self):
        """Omitting C{tier} at Silver should default to C{"standard"}."""
        data = self._valid()
        data.pop("tier")
        row = CustomerRow(**data)
        assert row.tier == "standard"

    @pytest.mark.parametrize("tier", ["standard", "silver", "gold"])
    def test_valid_tiers_accepted(self, tier: str):
        """
        All three members of the allowed-list must be accepted.

        @param tier: A valid tier value from C{VALID_TIERS}.
        """
        row = CustomerRow(**self._valid(tier=tier))
        assert row.tier == tier

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("Gold", "gold"),
            ("SILVER", "silver"),
            ("Standard", "standard"),
        ],
    )
    def test_tier_case_normalised(self, raw: str, expected: str):
        """
        Mixed-case tier values must be normalised to lowercase before
        the allowed-list check, so C{"Gold"} and C{"SILVER"} are accepted.

        @param raw:      Raw tier string as it might arrive from a CRM export.
        @param expected: Expected normalised value stored in the Silver row.
        """
        row = CustomerRow(**self._valid(tier=raw))
        assert row.tier == expected

    @pytest.mark.parametrize("bad_tier", ["platinum", "VIP", "bronze", ""])
    def test_invalid_tiers_rejected(self, bad_tier: str):
        """
        Values outside the allowed list must raise L{ValidationError} and
        be quarantined rather than reaching Gold.

        @param bad_tier: An invalid tier string.
        """
        with pytest.raises(ValidationError):
            CustomerRow(**self._valid(tier=bad_tier))


class TestProductRow:
    """Tests for L{ProductRow} (Silver)."""

    def _valid(self, **overrides):
        base = {
            "product_id": "P1",
            "name": "Widget",
            "category": "Electronics",
            "unit_cost": 10.0,
            "supplier_id": "SUP-A",
            "updated_at": "2025-01-01T00:00:00",
        }
        base.update(overrides)
        return base

    def test_valid_product(self):
        """A fully valid row should parse without error."""
        row = ProductRow(**self._valid())
        assert row.product_id == "P1"

    def test_inherits_bronze_field_names(self):
        """
        L{ProductRow} must inherit every field declared in
        L{BronzeProductRow}.
        """
        assert set(BronzeProductRow.model_fields).issubset(set(ProductRow.model_fields))

    def test_negative_cost_rejected(self):
        """Negative unit_cost must raise L{ValidationError}."""
        with pytest.raises(ValidationError):
            ProductRow(**self._valid(unit_cost=-1.0))

    def test_zero_cost_allowed(self):
        """A unit_cost of zero must be accepted (promotional products)."""
        row = ProductRow(**self._valid(unit_cost=0.0))
        assert row.unit_cost == 0.0


# ── assert_gold_schema tests ──────────────────────────────────────────────────


class TestAssertGoldSchema:
    """
    Tests for L{assert_gold_schema}.

    Verifies that compliant DataFrames pass silently and that both missing
    columns and dtype mismatches raise L{SchemaError} with descriptive
    messages.
    """

    def _make_df(self) -> pd.DataFrame:
        """Return a minimal compliant DataFrame for C{fact_orders}."""
        return pd.DataFrame(
            {
                "order_id": pd.array(["ORD-1"], dtype=object),
                "customer_id": pd.array(["C1"], dtype=object),
                "product_id": pd.array(["P1"], dtype=object),
                "order_date": pd.to_datetime(["2025-01-01"]),
                "quantity": pd.array([2], dtype="int64"),
                "unit_price": pd.array([9.99], dtype="float64"),
                "status": pd.array(["shipped"], dtype=object),
                "total_amount": pd.array([19.98], dtype="float64"),
            }
        )

    def test_compliant_dataframe_passes(self):
        """
        A DataFrame with all required columns at correct dtypes must not
        raise any exception.
        """
        schema = {
            "order_id": str,
            "order_date": datetime,
            "quantity": int,
            "unit_price": float,
            "total_amount": float,
        }
        assert_gold_schema(self._make_df(), schema, "fact_orders")  # no exception

    def test_missing_column_raises_schema_error(self):
        """
        A DataFrame missing a required column must raise L{SchemaError}
        with the missing column name in the message.
        """
        df = self._make_df().drop(columns=["total_amount"])
        with pytest.raises(SchemaError, match="total_amount"):
            assert_gold_schema(df, {"total_amount": float}, "fact_orders")

    def test_dtype_mismatch_raises_schema_error(self):
        """
        A DataFrame where a required column has an incompatible dtype must
        raise L{SchemaError} describing the mismatch.
        """
        df = self._make_df().copy()
        # Cast total_amount (float64) to int — mismatches the float expectation
        df["total_amount"] = df["total_amount"].astype("int64")
        with pytest.raises(SchemaError, match="total_amount"):
            assert_gold_schema(df, {"total_amount": float}, "fact_orders")

    def test_multiple_missing_columns_all_reported(self):
        """
        When multiple columns are missing, all of them must appear in the
        L{SchemaError} message.
        """
        df = pd.DataFrame({"order_id": ["ORD-1"]})
        schema = {"order_id": str, "quantity": int, "total_amount": float}
        with pytest.raises(SchemaError) as exc_info:
            assert_gold_schema(df, schema, "fact_orders")
        msg = str(exc_info.value)
        assert "quantity" in msg
        assert "total_amount" in msg

    def test_table_name_appears_in_error_message(self):
        """
        The C{table_name} argument must be included in the L{SchemaError}
        message to aid debugging.
        """
        df = pd.DataFrame({"x": [1]})
        with pytest.raises(SchemaError, match="dim_customer"):
            assert_gold_schema(df, {"missing_col": str}, "dim_customer")
