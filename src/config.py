from __future__ import annotations

from dataclasses import dataclass
from typing import List
import yaml


@dataclass(frozen=True)
class PathsConfig:
    """Input/output paths for the pipeline."""
    input_base: str
    output_base: str


@dataclass(frozen=True)
class ValidationConfig:
    """Validation thresholds and rules."""
    axis_min_g: float
    axis_max_g: float
    dedupe_keys: List[str]


@dataclass(frozen=True)
class ConsentConfig:
    """Consent enforcement settings."""
    enabled: bool


@dataclass(frozen=True)
class JoinConfig:
    """Join strategy settings."""
    mode: str  # "exact" for now


@dataclass(frozen=True)
class AppConfig:
    """Top-level application config."""
    name: str
    paths: PathsConfig
    validation: ValidationConfig
    consent: ConsentConfig
    join: JoinConfig


def load_config(path: str = "config.yaml") -> AppConfig:
    """
    Load pipeline configuration from a YAML file.

    Args:
        path: Path to YAML config file.

    Returns:
        AppConfig object containing validated configuration.
    """
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    return AppConfig(
        name=raw["app"]["name"],
        paths=PathsConfig(
            input_base=raw["paths"]["input_base"],
            output_base=raw["paths"]["output_base"],
        ),
        validation=ValidationConfig(
            axis_min_g=float(raw["validation"]["axis_min_g"]),
            axis_max_g=float(raw["validation"]["axis_max_g"]),
            dedupe_keys=list(raw["validation"]["dedupe_keys"]),
        ),
        consent=ConsentConfig(
            enabled=bool(raw["consent"]["enabled"]),
        ),
        join=JoinConfig(
            mode=str(raw["join"]["mode"]),
        ),
    )