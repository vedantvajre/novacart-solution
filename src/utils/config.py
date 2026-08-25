"""Config loader — reads pipeline.yaml and exposes typed paths and config sections."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class SilverConfig:
    """
    Typed representation of the C{silver:} section in C{pipeline.yaml}.

    Provides validated, attribute-accessible config for Silver-layer
    transforms.  Prevents silent key-typo failures that occur with raw
    C{dict.get()} calls.

    @ivar min_order_amount: Minimum acceptable order amount.  Rows below
                            this threshold are candidates for quarantine.
    @ivar max_order_amount: Maximum acceptable order amount.  Rows above
                            this threshold are candidates for quarantine.
    """

    min_order_amount: float = 0.0
    max_order_amount: float = 100_000.0

    @classmethod
    def from_dict(cls, raw: dict[str, object]) -> SilverConfig:
        """
        Construct a L{SilverConfig} from a raw YAML section dict.

        Unknown keys are silently ignored so that adding new YAML fields
        does not break existing code before the dataclass is updated.

        @param raw: The C{silver:} section parsed from C{pipeline.yaml}.
        @return:    A fully populated L{SilverConfig} instance.
        """
        return cls(
            min_order_amount=float(raw.get("min_order_amount", 0.0)),  # type: ignore[arg-type]
            max_order_amount=float(raw.get("max_order_amount", 100_000.0)),  # type: ignore[arg-type]
        )


@dataclass
class GoldConfig:
    """
    Typed representation of the C{gold:} section in C{pipeline.yaml}.

    Provides validated, attribute-accessible config for Gold-layer
    transforms.  Replaces raw C{dict.get("scd2_track_fields", [...])} calls
    with a typed attribute so key typos are caught at development time.

    @ivar scd2_track_fields: Column names whose changes trigger a new SCD2
                             version in C{dim_customer}.  Defaults to
                             C{["city", "country", "email"]}.
    """

    scd2_track_fields: list[str] = field(default_factory=lambda: ["city", "country", "email"])

    @classmethod
    def from_dict(cls, raw: dict[str, object]) -> GoldConfig:
        """
        Construct a L{GoldConfig} from a raw YAML section dict.

        @param raw: The C{gold:} section parsed from C{pipeline.yaml}.
        @return:    A fully populated L{GoldConfig} instance.
        """
        scd2: list[str] = raw.get(  # type: ignore[assignment]
            "scd2_track_fields", ["city", "country", "email"]
        )
        return cls(scd2_track_fields=list(scd2))


class Config:
    """
    Pipeline configuration loaded from C{pipeline.yaml}.

    Path properties resolve relative paths against the project root.
    Config section properties return typed dataclass instances rather than
    raw dicts, so key access is validated at development time.

    @ivar root: Absolute path to the project root directory.
    """

    def __init__(self, raw: dict[str, object], root: Path):
        self._raw = raw
        self.root = root

    @classmethod
    def load(cls, path: str = "config/pipeline.yaml") -> Config:
        """
        Load and parse C{pipeline.yaml} from the given path.

        @param path: Relative or absolute path to the YAML config file.
        @return:     A fully initialised L{Config} instance.
        """
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
    def silver_cfg(self) -> SilverConfig:
        """
        Return the typed Silver-layer configuration.

        @return: L{SilverConfig} parsed from the C{silver:} YAML section.
        """
        raw = self._raw.get("silver", {})
        assert isinstance(raw, dict)
        return SilverConfig.from_dict(raw)

    @property
    def gold_cfg(self) -> GoldConfig:
        """
        Return the typed Gold-layer configuration.

        @return: L{GoldConfig} parsed from the C{gold:} YAML section.
        """
        raw = self._raw.get("gold", {})
        assert isinstance(raw, dict)
        return GoldConfig.from_dict(raw)
