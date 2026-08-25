"""
14 tests covering the 7 required acceptance scenarios.
Run with: pytest -v
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.pipeline import run_one_date
from src.utils.config import Config
from tests.conftest import make_products_db, write_customers_json, write_orders_csv

DATE = "2025-11-07"

GOOD_CUSTOMER = {
    "customer_id": "CUST-001",
    "first_name": "Alice",
    "last_name": "Smith",
    "email": "alice@example.com",
    "address": {"city": "NYC", "country": "US"},
    "signup_date": "2024-01-01",
    "tier": "gold",
}
GOOD_PRODUCT = ("PROD-001", "Widget", "Electronics", 10.0, "SUP-A", "2025-01-01T00:00:00")


# ── Scenario 1: Happy path ────────────────────────────────────────────────────


def test_happy_path_fact_rows(config: Config):
    write_orders_csv(
        config.landing_orders,
        DATE,
        [
            ["ORD-001", "CUST-001", "PROD-001", DATE, "2", "49.99", "shipped"],
            ["ORD-002", "CUST-001", "PROD-001", DATE, "1", "19.99", "delivered"],
            ["ORD-003", "CUST-001", "PROD-001", DATE, "3", "9.99", "pending"],
        ],
    )
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    result = run_one_date(DATE, config)
    assert result["status"] == "SUCCESS"

    fact = pd.read_parquet(config.gold / "fact_orders" / f"date={DATE}" / "data.parquet")
    assert len(fact) == 3


def test_happy_path_total_amount_calculated(config: Config):
    write_orders_csv(
        config.landing_orders,
        DATE,
        [
            ["ORD-001", "CUST-001", "PROD-001", DATE, "2", "50.00", "shipped"],
        ],
    )
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    run_one_date(DATE, config)
    fact = pd.read_parquet(config.gold / "fact_orders" / f"date={DATE}" / "data.parquet")
    assert fact.iloc[0]["total_amount"] == pytest.approx(100.0)


# ── Scenario 2: Duplicate handling ───────────────────────────────────────────


def test_duplicates_collapsed(config: Config):
    write_orders_csv(
        config.landing_orders,
        DATE,
        [
            ["ORD-001", "CUST-001", "PROD-001", DATE, "2", "49.99", "shipped"],
            ["ORD-001", "CUST-001", "PROD-001", DATE, "2", "49.99", "shipped"],  # duplicate
        ],
    )
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    run_one_date(DATE, config)
    fact = pd.read_parquet(config.gold / "fact_orders" / f"date={DATE}" / "data.parquet")
    assert len(fact) == 1


def test_duplicates_keep_last_value(config: Config):
    write_orders_csv(
        config.landing_orders,
        DATE,
        [
            ["ORD-001", "CUST-001", "PROD-001", DATE, "2", "49.99", "shipped"],
            ["ORD-001", "CUST-001", "PROD-001", DATE, "5", "49.99", "shipped"],  # updated qty
        ],
    )
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    run_one_date(DATE, config)
    silver = pd.read_parquet(config.silver / "orders" / f"date={DATE}" / "data.parquet")
    assert silver.iloc[0]["quantity"] == 5


# ── Scenario 3: Bad data → quarantine ────────────────────────────────────────


def test_bad_rows_quarantined(config: Config):
    write_orders_csv(
        config.landing_orders,
        DATE,
        [
            ["ORD-001", "CUST-001", "PROD-001", DATE, "2", "49.99", "shipped"],  # good
            ["ORD-002", "CUST-001", "PROD-001", DATE, "0", "49.99", "shipped"],  # bad qty
        ],
    )
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    run_one_date(DATE, config)
    q_files = list((config.quarantine / "orders").glob("*.parquet"))
    assert q_files, "quarantine directory should contain at least one file"
    q_df = pd.concat([pd.read_parquet(f) for f in q_files])
    assert len(q_df) == 1


def test_bad_rows_dont_reach_gold(config: Config):
    write_orders_csv(
        config.landing_orders,
        DATE,
        [
            ["ORD-001", "CUST-001", "PROD-001", DATE, "2", "49.99", "shipped"],
            ["ORD-002", "CUST-001", "PROD-001", DATE, "-1", "49.99", "shipped"],  # negative qty
        ],
    )
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    run_one_date(DATE, config)
    fact = pd.read_parquet(config.gold / "fact_orders" / f"date={DATE}" / "data.parquet")
    assert len(fact) == 1


# ── Scenario 4: Additive schema drift ────────────────────────────────────────


def test_additive_drift_succeeds(config: Config):
    """Extra column in source → pipeline continues, column ignored."""
    path = config.landing_orders / f"orders_{DATE}.csv"
    import csv

    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "order_id",
                "customer_id",
                "product_id",
                "order_date",
                "quantity",
                "unit_price",
                "status",
                "new_mystery_column",
            ]
        )
        w.writerow(["ORD-001", "CUST-001", "PROD-001", DATE, "2", "49.99", "shipped", "surprise"])
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    result = run_one_date(DATE, config)
    assert result["status"] == "SUCCESS"


def test_additive_drift_data_still_lands(config: Config):
    path = config.landing_orders / f"orders_{DATE}.csv"
    import csv

    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "order_id",
                "customer_id",
                "product_id",
                "order_date",
                "quantity",
                "unit_price",
                "status",
                "extra_col",
            ]
        )
        w.writerow(["ORD-001", "CUST-001", "PROD-001", DATE, "2", "49.99", "shipped", "x"])
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    run_one_date(DATE, config)
    fact = pd.read_parquet(config.gold / "fact_orders" / f"date={DATE}" / "data.parquet")
    assert len(fact) == 1


# ── Scenario 5: Subtractive schema drift ─────────────────────────────────────


def test_subtractive_drift_fails(config: Config):
    """Missing required column → pipeline stage fails."""
    path = config.landing_orders / f"orders_{DATE}.csv"
    import csv

    with path.open("w", newline="") as f:
        w = csv.writer(f)
        # unit_price is missing
        w.writerow(["order_id", "customer_id", "product_id", "order_date", "quantity", "status"])
        w.writerow(["ORD-001", "CUST-001", "PROD-001", DATE, "2", "shipped"])
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    result = run_one_date(DATE, config)
    assert result["status"] == "FAIL"


def test_subtractive_drift_error_message(config: Config):
    path = config.landing_orders / f"orders_{DATE}.csv"
    import csv

    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "order_date", "quantity", "status"])
        w.writerow(["ORD-001", "CUST-001", DATE, "2", "shipped"])
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    result = run_one_date(DATE, config)
    assert "missing" in result["error"].lower() or "schema" in result["error"].lower()


# ── Scenario 6: Idempotency ───────────────────────────────────────────────────


def test_idempotency_same_row_count(config: Config):
    write_orders_csv(
        config.landing_orders,
        DATE,
        [
            ["ORD-001", "CUST-001", "PROD-001", DATE, "2", "49.99", "shipped"],
            ["ORD-002", "CUST-001", "PROD-001", DATE, "1", "19.99", "delivered"],
        ],
    )
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    run_one_date(DATE, config)
    fact1 = pd.read_parquet(config.gold / "fact_orders" / f"date={DATE}" / "data.parquet")

    run_one_date(DATE, config)
    fact2 = pd.read_parquet(config.gold / "fact_orders" / f"date={DATE}" / "data.parquet")

    assert len(fact1) == len(fact2)


def test_idempotency_same_values(config: Config):
    write_orders_csv(
        config.landing_orders,
        DATE,
        [
            ["ORD-001", "CUST-001", "PROD-001", DATE, "2", "49.99", "shipped"],
        ],
    )
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    run_one_date(DATE, config)
    fact1 = pd.read_parquet(config.gold / "fact_orders" / f"date={DATE}" / "data.parquet")

    run_one_date(DATE, config)
    fact2 = pd.read_parquet(config.gold / "fact_orders" / f"date={DATE}" / "data.parquet")

    pd.testing.assert_frame_equal(
        fact1.sort_values("order_id").reset_index(drop=True),
        fact2.sort_values("order_id").reset_index(drop=True),
    )


# ── Scenario 7: Backfill ──────────────────────────────────────────────────────


def test_backfill_all_dates_present(config: Config):
    for d in ["2025-11-07", "2025-11-08", "2025-11-09"]:
        write_orders_csv(
            config.landing_orders,
            d,
            [
                [f"ORD-{d[-2:]}", "CUST-001", "PROD-001", d, "1", "49.99", "shipped"],
            ],
        )
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    from src.pipeline import main

    main(["--date", "2025-11-09", "--backfill", "2", "--config", "config/pipeline.yaml"])

    for d in ["2025-11-07", "2025-11-08", "2025-11-09"]:
        p = config.gold / "fact_orders" / f"date={d}" / "data.parquet"
        assert p.exists(), f"Missing Gold partition for {d}"


def test_backfill_equals_individual_runs(config: Config):
    for d in ["2025-11-07", "2025-11-08"]:
        write_orders_csv(
            config.landing_orders,
            d,
            [
                [f"ORD-{d[-2:]}", "CUST-001", "PROD-001", d, "2", "49.99", "shipped"],
            ],
        )
    write_customers_json(config.landing_customers, [GOOD_CUSTOMER])
    make_products_db(config.landing_products_db, [GOOD_PRODUCT])

    from src.pipeline import main

    main(["--date", "2025-11-08", "--backfill", "1", "--config", "config/pipeline.yaml"])

    for d in ["2025-11-07", "2025-11-08"]:
        p = config.gold / "fact_orders" / f"date={d}" / "data.parquet"
        df = pd.read_parquet(p)
        assert len(df) == 1
