"""Structured JSON logging setup."""
from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from pathlib import Path


def get_logger(name: str, log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    for h in logger.handlers[:]:
        h.close()
        logger.removeHandler(h)

    fh = logging.FileHandler(log_dir / "pipeline.jsonl")
    fh.setFormatter(_JsonFormatter())
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)

    return logger


def log_event(logger: logging.Logger, level: str, event: str, **kwargs):
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **kwargs,
    }
    getattr(logger, level.lower())(json.dumps(payload))


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return record.getMessage()
