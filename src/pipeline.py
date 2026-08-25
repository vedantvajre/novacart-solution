"""
NovaCart ETL — CLI Orchestrator
Usage:
    python -m src.pipeline --date 2025-11-07
    python -m src.pipeline --date 2025-11-10 --backfill 3
"""
from __future__ import annotations
import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.utils.config import Config
from src.utils.logging_setup import get_logger, log_event
from src.utils.state import StateManager
from src.ingest.orders import ingest_orders
from src.ingest.customers import ingest_customers
from src.ingest.products import ingest_products
from src.transform.silver import (
    build_silver_orders,
    build_silver_customers,
    build_silver_products,
)
from src.transform.gold import (
    build_dim_product,
    build_dim_customer,
    build_fact_orders,
)


def run_one_date(date_str: str, config: Config) -> dict:
    logger = get_logger("novacart", config.logs)
    state = StateManager(config.state)
    started_at = datetime.now(timezone.utc)
    stages: list[dict] = []

    def stage(name: str, fn):
        t0 = datetime.now(timezone.utc)
        try:
            fn()
            stages.append({"stage": name, "status": "OK",
                           "duration_sec": (datetime.now(timezone.utc) - t0).total_seconds()})
        except Exception as exc:
            stages.append({"stage": name, "status": "FAIL", "error": str(exc),
                           "duration_sec": (datetime.now(timezone.utc) - t0).total_seconds()})
            raise

    status, error_msg = "SUCCESS", None
    try:
        # ── Bronze ────────────────────────────────────────────────────────────
        stage("ingest_orders",    lambda: ingest_orders(
            date_str, config.landing_orders, config.bronze, logger))
        stage("ingest_customers", lambda: ingest_customers(
            config.landing_customers, config.bronze, logger))
        stage("ingest_products",  lambda: ingest_products(
            config.landing_products_db, config.bronze, state, logger))

        # ── Silver ────────────────────────────────────────────────────────────
        stage("silver_orders",    lambda: build_silver_orders(
            date_str, config.bronze, config.silver, config.quarantine, logger))
        stage("silver_customers", lambda: build_silver_customers(
            config.bronze, config.silver, config.quarantine, logger))
        stage("silver_products",  lambda: build_silver_products(
            config.bronze, config.silver, config.quarantine, logger))

        # ── Gold ──────────────────────────────────────────────────────────────
        stage("dim_product",   lambda: build_dim_product(
            config.silver, config.gold, logger))
        stage("dim_customer",  lambda: build_dim_customer(
            config.silver, config.gold,
            config.gold_cfg.get("scd2_track_fields", ["city", "country", "email"]),
            logger))
        stage("fact_orders",   lambda: build_fact_orders(
            date_str, config.silver, config.gold, logger))

    except Exception as exc:
        status = "FAIL"
        error_msg = str(exc)

    finished_at = datetime.now(timezone.utc)
    metadata = {
        "date": date_str,
        "status": status,
        "error": error_msg,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_sec": (finished_at - started_at).total_seconds(),
        "stages": stages,
    }
    state.record_run(metadata)
    log_event(logger, "INFO", "pipeline_end",
              **{k: v for k, v in metadata.items() if k != "stages"})
    return metadata


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="NovaCart ETL pipeline")
    parser.add_argument("--date",     required=True, help="Processing date YYYY-MM-DD")
    parser.add_argument("--backfill", type=int, default=0,
                        help="Also process N days before --date")
    parser.add_argument("--config",   default="config/pipeline.yaml")
    args = parser.parse_args(argv)

    config = Config.load(args.config)
    target = datetime.strptime(args.date, "%Y-%m-%d").date()
    dates  = [target - timedelta(days=i) for i in range(args.backfill, -1, -1)]

    failures = 0
    for d in dates:
        result = run_one_date(d.strftime("%Y-%m-%d"), config)
        if result["status"] != "SUCCESS":
            failures += 1
            print(f"[FAIL] {d}: {result['error']}", file=sys.stderr)
        else:
            print(f"[OK]   {d}: {result['duration_sec']:.2f}s")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
