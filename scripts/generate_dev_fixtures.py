"""Deterministic, API-shaped fixture pages for CI and local dev.

Coverage
--------
* ALL ten configured balancing authorities, each with a distinct grid
  archetype (solar-heavy CISO, wind-heavy ERCO/SWPP, hydro-dominant BPAT,
  nuclear/gas east-coast profiles, ...), so seed-coverage and per-BA
  freshness tests are exercised for real in CI.
* Local-time shapes use each BA's IANA zone straight from the dbt seed via
  zoneinfo — correct offsets year-round, no hardcoded summer offsets.
* Two windows: a rolling 14-day window ending near now (freshness tests),
  plus a fixed slice around the 2026-03-08 US spring-forward (23-hour local
  days, exercising DST-exact completeness).

Physics (same identities the dbt reconciliation tests assert):
  demand = net_generation - total_interchange; per-fuel sums equal net
  generation; storage/hybrids carry signed values (charge = negative).

Planted quirks, mirroring the live API:
  * ~30% of values serialized as strings, ~0.5% as nulls
  * CISO hour 100:  impossible negative demand
  * CISO hour 50:   fuel report entirely absent
  * CISO hour 130:  ONLY the solar rows (SUN+SNB) absent -> has_vre_absent
  * ERCO hour 60:   demand row absent
  * SWPP hour 80:   400 GWh demand AND missing NG row (unverifiable hour)
  * PJM  hour 90:   solar value = 600,000 MWh (extreme fuel outlier)
"""

from __future__ import annotations

import csv
import json
import math
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

REPO = Path(__file__).resolve().parents[1]
OUT_DIR = REPO / "data" / "sample"
SEED_CSV = REPO / "dbt" / "seeds" / "balancing_authorities.csv"
ROLLING_HOURS = 14 * 24
SEED = 930

GLITCH_NEG_DEMAND = ("CISO", 100)
GLITCH_NO_FUEL = ("CISO", 50)
GLITCH_NO_SOLAR = ("CISO", 130)
GLITCH_NO_DEMAND = ("ERCO", 60)
GLITCH_FUEL_OUTLIER = ("PJM", 90, "SUN", 600000.0)
GLITCH_UNVERIFIABLE = ("SWPP", 80)  # absurd demand AND no NG row: the
# identity cannot be evaluated, and "cannot validate" must gate publication

# (base_demand, morning_amp, evening_amp, interchange_mode, {fuel: weight})
# Weights are relative pre-normalization shares; the physics closer rescales
# non-signed fuels each hour so sums balance exactly. Signed fuels (BAT,
# SNB, UES) keep their sign: charging hours are negative by design.
PROFILES: dict[str, tuple[float, float, float, str, dict[str, float]]] = {
    "CISO": (22000, 3000, 8000, "import", {"NG": 6.5, "NUC": 2.2, "WAT": 3.2,
             "SUN": 12.0, "WND": 2.5, "GEO": 0.9, "OTH": 0.4, "BAT": 1.0, "SNB": 1.0}),
    "ERCO": (45000, 4000, 15000, "island", {"NG": 14.0, "COL": 6.5, "NUC": 5.1,
             "SUN": 9.0, "WND": 9.0, "OTH": 0.8}),
    "MISO": (75000, 6000, 16000, "mixed", {"NG": 28.0, "COL": 22.0, "NUC": 12.0,
             "WND": 12.0, "SUN": 4.0, "OTH": 1.5, "UES": 0.3}),
    "PJM":  (88000, 7000, 18000, "export", {"NG": 36.0, "COL": 16.0, "NUC": 32.0,
             "SUN": 5.0, "WND": 4.0, "WAT": 2.5, "OTH": 1.5}),
    "NYIS": (17000, 1500, 4500, "import", {"NG": 7.0, "NUC": 4.3, "WAT": 4.0,
             "WND": 1.5, "SUN": 1.2, "OTH": 0.4}),
    "ISNE": (13500, 1200, 3800, "import", {"NG": 6.5, "NUC": 3.3, "WAT": 1.2,
             "SUN": 1.5, "WND": 1.0, "OTH": 0.5, "SNB": 0.3}),
    "SWPP": (30000, 2500, 7000, "mixed", {"WND": 14.0, "NG": 8.0, "COL": 7.0,
             "NUC": 2.0, "SUN": 2.5, "OTH": 0.6}),
    "BPAT": (7500, 700, 1800, "export", {"WAT": 8.5, "NG": 1.2, "NUC": 1.1,
             "WND": 1.5, "OTH": 0.2}),
    "FPL":  (26000, 2000, 6000, "import", {"NG": 18.0, "NUC": 3.4, "SUN": 4.0,
             "OTH": 0.5, "SNB": 0.5}),
    "DUK":  (15000, 1400, 4200, "mixed", {"NG": 6.0, "NUC": 5.5, "COL": 2.5,
             "WAT": 1.0, "SUN": 1.4, "OTH": 0.4}),
    "SOCO": (30000, 2500, 7000, "mixed", {"NG": 14.0, "NUC": 8.0, "COL": 4.0,
             "SUN": 4.0, "WAT": 1.5, "OTH": 0.6, "OIL": 0.2, "WND": 1.0,
             "BAT": 0.4}),
}
SIGNED_FUELS = {"BAT", "SNB", "UES"}


def seed_timezones() -> dict[str, str]:
    with SEED_CSV.open() as fh:
        return {row["ba_code"]: row["timezone"] for row in csv.DictReader(fh)}


def daily(base: float, m_amp: float, e_amp: float, h: int) -> float:
    return (base
            + m_amp * math.exp(-((h - 8) ** 2) / 8.0)
            + e_amp * math.exp(-((h - 19) ** 2) / 10.0))


def solar_bell(h: int) -> float:
    return math.exp(-((h - 12.5) ** 2) / 7.0) if 6 <= h <= 19 else 0.0


def fuel_shape(fuel: str, h: int, rng: random.Random) -> float:
    if fuel == "SUN":
        return solar_bell(h) * (1 + 0.04 * rng.gauss(0, 1))
    if fuel == "SNB":
        return solar_bell(h) - 0.08 * math.exp(-((h - 6) ** 2) / 2.0)
    if fuel == "WND":
        return 0.6 + 0.4 * math.exp(-((h - 3) ** 2) / 35.0) + 0.07 * rng.gauss(0, 1)
    if fuel == "BAT":
        return (-0.8 * math.exp(-((h - 12.5) ** 2) / 6.0)
                + 1.0 * math.exp(-((h - 19.5) ** 2) / 4.0))
    if fuel == "UES":
        return (-0.6 * math.exp(-((h - 13) ** 2) / 6.0)
                + 0.8 * math.exp(-((h - 20) ** 2) / 4.0))
    if fuel == "NG":
        return 0.55 + 0.45 * math.exp(-((h - 19) ** 2) / 12.0)
    if fuel == "COL":
        return 0.8 + 0.2 * math.exp(-((h - 19) ** 2) / 25.0)
    if fuel == "WAT":
        return 0.75 + 0.25 * math.exp(-((h - 19) ** 2) / 20.0)
    if fuel == "GEO":
        return 1.0 + 0.02 * rng.gauss(0, 1)
    return 1.0  # NUC, OTH: flat


def interchange(mode: str, h: int, rng: random.Random, scale: float) -> float:
    if mode == "import":
        return -(0.25 + 0.12 * math.exp(-((h - 20) ** 2) / 18.0)) * scale \
            * (1 + 0.05 * rng.gauss(0, 1))
    if mode == "export":
        return (0.10 + 0.05 * math.exp(-((h - 12) ** 2) / 20.0)) * scale \
            * (1 + 0.05 * rng.gauss(0, 1))
    if mode == "island":
        return rng.gauss(150, 400)
    return rng.gauss(0, 0.03 * scale)  # mixed


def build_hour(ba: str, h_local: int, rng: random.Random):
    base, m_amp, e_amp, ti_mode, palette = PROFILES[ba]
    d = daily(base, m_amp, e_amp, h_local) * (1 + 0.01 * rng.gauss(0, 1))
    ti = interchange(ti_mode, h_local, rng, base * 0.3)
    target_ng = d + ti

    raw = {f: w * base / 10.0 * fuel_shape(f, h_local, rng)
           for f, w in palette.items()}
    signed = sum(v for f, v in raw.items() if f in SIGNED_FUELS)
    rest = {f: v for f, v in raw.items() if f not in SIGNED_FUELS}
    scale = (target_ng - signed) / sum(rest.values())
    fuels = {f: (v * scale if f not in SIGNED_FUELS else v)
             for f, v in raw.items()}
    return d, target_ng, ti, fuels


def quirkify(rng: random.Random, v: float, allow_nulls: bool = True):
    if allow_nulls and rng.random() < 0.002:
        return None
    if rng.random() < 0.30:
        return f"{v:.2f}"
    return round(v, 2)


def page(route_key: str, records: list[dict]) -> dict:
    return {"_route_key": route_key,
            "response": {"total": str(len(records)), "data": records}}


def emit(hours: list[datetime], tzmap: dict[str, str], rng: random.Random,
         apply_glitches: bool, allow_nulls: bool = True):
    region, fuel = [], []
    for ba in PROFILES:
        tz = ZoneInfo(tzmap[ba])
        for i, ts in enumerate(hours):
            h_local = ts.replace(tzinfo=UTC).astimezone(tz).hour
            d, ng, ti, fuels = build_hour(ba, h_local, rng)
            if apply_glitches and (ba, i) == GLITCH_NEG_DEMAND:
                d = -50.0
            if apply_glitches and (ba, i) == GLITCH_UNVERIFIABLE:
                d = 400000.0
            for metric, val in (("D", d), ("NG", ng), ("TI", ti)):
                if apply_glitches and metric == "D" and (ba, i) == GLITCH_NO_DEMAND:
                    continue
                if (apply_glitches and metric == "NG"
                        and (ba, i) == GLITCH_UNVERIFIABLE):
                    continue
                region.append({"period": ts.strftime("%Y-%m-%dT%H"),
                               "respondent": ba, "respondent-name": ba,
                               "type": metric, "type-name": metric,
                               "value": quirkify(rng, val, allow_nulls),
                               "value-units": "megawatthours"})
            for f, val in fuels.items():
                if apply_glitches:
                    if (ba, i) == GLITCH_NO_FUEL:
                        continue
                    if (ba, i) == GLITCH_NO_SOLAR and f in ("SUN", "SNB"):
                        continue
                    if (ba, i, f) == GLITCH_FUEL_OUTLIER[:3]:
                        val = GLITCH_FUEL_OUTLIER[3]
                fuel.append({"period": ts.strftime("%Y-%m-%dT%H"),
                             "respondent": ba, "respondent-name": ba,
                             "fueltype": f, "type-name": f,
                             "value": quirkify(rng, val, allow_nulls),
                             "value-units": "megawatthours"})
    return region, fuel


def main() -> None:
    rng = random.Random(SEED)
    tzmap = seed_timezones()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for stale in OUT_DIR.glob("*.json"):  # self-cleaning (see project history)
        stale.unlink()

    end = datetime.now(UTC).replace(tzinfo=None, minute=0, second=0,
                                    microsecond=0) - timedelta(hours=6)
    rolling = [end - timedelta(hours=ROLLING_HOURS - 1 - i)
               for i in range(ROLLING_HOURS)]
    region, fuel = emit(rolling, tzmap, rng, apply_glitches=True)

    # Fixed DST slices, deliberately glitch- and null-free so the
    # transition days themselves are COMPLETE and the 23h/25h expected-hour
    # math is pinned by data, not just by formula:
    #   2026-03-08 US spring-forward -> 23-hour local days
    #   2025-11-02 US fall-back      -> 25-hour local days
    for anchor in (datetime(2026, 3, 7, 0), datetime(2025, 11, 1, 0)):
        slice_hours = [anchor + timedelta(hours=i) for i in range(72)]
        r2, f2 = emit(slice_hours, tzmap, rng, apply_glitches=False,
                      allow_nulls=False)
        region += r2
        fuel += f2

    def write(route: str, records: list[dict], parts: int):
        step = math.ceil(len(records) / parts)
        for k in range(parts):
            chunk = records[k * step:(k + 1) * step]
            (OUT_DIR / f"{route}_page{k + 1}.json").write_text(
                json.dumps(page(route, chunk), indent=1))

    write("region_data", region, 2)
    write("fuel_mix", fuel, 4)
    print(f"wrote {len(region)} region + {len(fuel)} fuel records "
          f"({len(PROFILES)} BAs; rolling window ends {end}; DST slice "
          f"2026-03-07..09) to {OUT_DIR}")


if __name__ == "__main__":
    main()
