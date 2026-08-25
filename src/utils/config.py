"""Config loader — reads pipeline.yaml and exposes typed paths."""

from __future__ import annotations

from pathlib import Path

import yaml


class Config:
    def __init__(self, raw: dict[str, object], root: Path):
        self._raw = raw
        self.root = root

    @classmethod
    def load(cls, path: str = "config/pipeline.yaml") -> Config:
        p = Path(path)
        with p.open() as f:
            raw = yaml.safe_load(f)
        return cls(raw, p.parent.parent)

    def _p(self, key: str) -> Path:
        paths = self._raw["paths"]
        assert isinstance(paths, dict)
        return self.root / str(paths[key])

    @property
    def landing_orders(self) -> Path:
        return self._p("landing_orders")

    @property
    def landing_customers(self) -> Path:
        return self._p("landing_customers")

    @property
    def landing_products_db(self) -> Path:
        return self._p("landing_products_db")

    @property
    def bronze(self) -> Path:
        return self._p("bronze")

    @property
    def silver(self) -> Path:
        return self._p("silver")

    @property
    def gold(self) -> Path:
        return self._p("gold")

    @property
    def quarantine(self) -> Path:
        return self._p("quarantine")

    @property
    def logs(self) -> Path:
        return self._p("logs")

    @property
    def state(self) -> Path:
        return self._p("state")

    @property
    def silver_cfg(self) -> dict[str, object]:
        value = self._raw.get("silver", {})
        assert isinstance(value, dict)
        return value

    @property
    def gold_cfg(self) -> dict[str, object]:
        value = self._raw.get("gold", {})
        assert isinstance(value, dict)
        return value
