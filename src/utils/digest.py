"""Daily digest: inspect quarantine files and produce a structured run report.

If quarantine rows exist, an ibm-watsonx-ai LLM agent is asked to explain
what went wrong in plain language before the summary is assembled.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

import pandas as pd

from src.utils.logging_setup import log_event


# ── quarantine inspector ──────────────────────────────────────────────────────

def _read_quarantine(quarantine_dir: Path, date_str: str) -> dict[str, pd.DataFrame]:
    """Return {source_name: DataFrame} for every quarantine file written today."""
    result: dict[str, pd.DataFrame] = {}
    if not quarantine_dir.exists():
        return result
    for source_dir in quarantine_dir.iterdir():
        if not source_dir.is_dir():
            continue
        # Files are named <YYYYMMDDTHHmmSS>.parquet — match today's date prefix
        day_prefix = date_str.replace("-", "")
        files = sorted(source_dir.glob(f"{day_prefix}*.parquet"))
        if not files:
            continue
        frames = [pd.read_parquet(f) for f in files]
        result[source_dir.name] = pd.concat(frames, ignore_index=True)
    return result


def _quarantine_summary_text(quarantined: dict[str, pd.DataFrame]) -> str:
    """Produce a concise text representation of quarantined rows for the LLM."""
    lines: list[str] = []
    for source, df in quarantined.items():
        lines.append(f"Source: {source}  ({len(df)} bad rows)")
        reason_counts = df["_quarantine_reason"].value_counts()
        for reason, count in reason_counts.items():
            lines.append(f"  - {count}x  {reason}")
        # Show up to 3 sample rows (drop internal columns for readability)
        sample = df[[c for c in df.columns if not c.startswith("_")]].head(3)
        lines.append(f"  Sample rows:\n{sample.to_string(index=False)}")
    return "\n".join(lines)


# ── watsonx LLM agent ─────────────────────────────────────────────────────────

def _watsonx_diagnose(summary_text: str, model_id: str, project_id: str) -> str:
    """Ask watsonx.ai to explain the quarantine errors in plain language."""
    try:
        from ibm_watsonx_ai import Credentials
        from ibm_watsonx_ai.foundation_models import ModelInference
    except ImportError:
        return "(ibm-watsonx-ai not installed — install it to enable LLM diagnosis)"

    api_key = os.environ.get("WATSONX_API_KEY")
    if not api_key:
        return "(WATSONX_API_KEY env var not set — skipping LLM diagnosis)"

    credentials = Credentials(
        url="https://us-south.ml.cloud.ibm.com",
        api_key=api_key,
    )

    prompt = (
        "You are a data-quality engineer reviewing a daily ETL pipeline digest.\n"
        "Below are rows that were rejected and sent to quarantine during today's run.\n"
        "For each source, explain in 2–3 plain-English sentences:\n"
        "  1. What the likely root cause is.\n"
        "  2. Whether it looks like a data-source issue or a pipeline bug.\n"
        "  3. A concrete recommended action.\n\n"
        f"{summary_text}\n\n"
        "Keep the response concise and actionable."
    )

    model = ModelInference(
        model_id=model_id,
        credentials=credentials,
        project_id=project_id,
        params={"max_new_tokens": 400, "temperature": 0.2},
    )
    response = model.generate_text(prompt=prompt)
    return response.strip()


# ── report builder ────────────────────────────────────────────────────────────

def build_digest(
    run_metadata: dict,
    quarantine_dir: Path,
    watsonx_model_id: str,
    watsonx_project_id: str,
    logger: logging.Logger,
) -> dict:
    """
    Assemble a digest dict for one pipeline run.

    Returns
    -------
    {
        "date":          str,
        "status":        "SUCCESS" | "FAIL",
        "duration_sec":  float,
        "stages":        [...],
        "quarantine":    {source: {"count": int, "reasons": {reason: count}}},
        "llm_diagnosis": str | None,
        "error":         str | None,
    }
    """
    date_str: str = run_metadata["date"]
    quarantined = _read_quarantine(quarantine_dir, date_str)

    quarantine_report: dict[str, dict] = {}
    for source, df in quarantined.items():
        quarantine_report[source] = {
            "count": len(df),
            "reasons": df["_quarantine_reason"].value_counts().to_dict(),
            "sample_rows": json.loads(
                df[[c for c in df.columns if not c.startswith("_")]]
                .head(5)
                .to_json(orient="records")
            ),
        }

    llm_diagnosis: str | None = None
    if quarantined:
        log_event(logger, "INFO", "digest_invoking_llm", date=date_str)
        summary_text = _quarantine_summary_text(quarantined)
        llm_diagnosis = _watsonx_diagnose(summary_text, watsonx_model_id, watsonx_project_id)
        log_event(logger, "INFO", "digest_llm_done", date=date_str)

    digest = {
        "date": date_str,
        "status": run_metadata["status"],
        "duration_sec": run_metadata["duration_sec"],
        "stages": run_metadata.get("stages", []),
        "error": run_metadata.get("error"),
        "quarantine": quarantine_report,
        "llm_diagnosis": llm_diagnosis,
    }

    log_event(logger, "INFO", "digest_built", date=date_str,
              quarantine_sources=list(quarantine_report.keys()))
    return digest
