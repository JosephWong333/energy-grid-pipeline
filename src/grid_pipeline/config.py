"""Load pipeline configuration from config/pipeline.yml + environment."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "pipeline.yml"


@dataclass(frozen=True)
class EIASettings:
    base_url: str
    page_size: int
    sleep_between_requests: float
    max_retries: int
    backoff_base_seconds: float
    request_timeout_seconds: float


@dataclass(frozen=True)
class PipelineConfig:
    db_path: str
    eia: EIASettings
    backfill_start: str
    incremental_lookback_hours: int
    balancing_authorities: list[str] = field(default_factory=list)
    region_data_path: str = "electricity/rto/region-data"
    region_data_types: list[str] = field(default_factory=lambda: ["D", "NG", "TI"])
    fuel_mix_path: str = "electricity/rto/fuel-type-data"


def load_config(path: Path | None = None) -> PipelineConfig:
    """Read pipeline.yml, applying environment overrides where they exist."""
    load_dotenv(REPO_ROOT / ".env")
    raw = yaml.safe_load((path or DEFAULT_CONFIG_PATH).read_text())

    eia = raw["eia"]
    return PipelineConfig(
        db_path=os.environ.get("GRID_DB_PATH", raw["database"]["path"]),
        eia=EIASettings(
            base_url=eia["base_url"],
            page_size=int(eia["page_size"]),
            sleep_between_requests=float(eia["sleep_between_requests"]),
            max_retries=int(eia["max_retries"]),
            backoff_base_seconds=float(eia["backoff_base_seconds"]),
            request_timeout_seconds=float(eia["request_timeout_seconds"]),
        ),
        backfill_start=str(raw["backfill"]["start"]),
        incremental_lookback_hours=int(raw["incremental"]["lookback_hours"]),
        balancing_authorities=list(raw["balancing_authorities"]),
        region_data_path=raw["routes"]["region_data"]["path"],
        region_data_types=list(raw["routes"]["region_data"]["types"]),
        fuel_mix_path=raw["routes"]["fuel_mix"]["path"],
    )


def get_api_key() -> str:
    """EIA API key from the environment. Free at https://www.eia.gov/opendata/register.php."""
    key = os.environ.get("EIA_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "EIA_API_KEY is not set. Register for a free key at "
            "https://www.eia.gov/opendata/register.php and put it in .env "
            "(see .env.example)."
        )
    return key
