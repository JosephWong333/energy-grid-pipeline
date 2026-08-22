"""Portfolio charts from the marts, with fail-closed provenance.

The watermark logic is deliberately paranoid: charts are only rendered
clean when row-level lineage proves EVERY raw row came from the EIA API.
Fixtures, mixed warehouses, pre-lineage warehouses, and empty databases all
get watermarked. Losing metadata can only ADD a watermark, never remove one.

Duck-curve honesty: the hourly profile mart is built exclusively from
complete local days, and the chart labels exactly which days it averaged —
a month-to-date window is titled as a date range, never passed off as the
full month.
"""

from __future__ import annotations

import argparse
import calendar
import os
import sys
from datetime import date, datetime
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from grid_pipeline.provenance import data_provenance  # noqa: E402

WATERMARKS = {
    "fixtures": "SYNTHETIC FIXTURE DATA — NOT REAL",
    "mixed": "MIXED DATA PROVENANCE — NOT FOR PUBLICATION",
    "unknown": "UNVERIFIED DATA PROVENANCE — NOT FOR PUBLICATION",
    "empty": "NO DATA",
}
SOURCE_BASE = "Source: EIA-930 via EIA API v2"
NET_LOAD_NOTE = (
    " · Net load = demand − wind − solar (utility-scale as reported by the "
    "BA; generally excludes rooftop/behind-the-meter solar)"
)


def apply_watermark(fig, provenance: str) -> None:
    if provenance == "real":
        return
    fig.text(0.5, 0.5, WATERMARKS.get(provenance, WATERMARKS["unknown"]),
             fontsize=26, color="red", alpha=0.28, ha="center", va="center",
             rotation=22, zorder=100)


def footer(fig, note: str = "") -> None:
    stamp = datetime.now().strftime("%Y-%m-%d")
    fig.text(0.01, 0.01, f"{SOURCE_BASE}{note} · Generated {stamp}",
             fontsize=6.5, color="#666666", ha="left", va="bottom")


def pick_profile_window(con, ba: str):
    """Latest month in the profile mart, with an honest label.

    Returns (month_date, n_days, label). A full calendar month is labeled
    'June 2026 average'; a partial one is labeled with the exact complete-day
    range it averages.
    """
    row = con.execute(
        """
        select month, max(n_days)
        from main.mart_hourly_profile
        where ba_code = ?
        group by month order by month desc limit 1
        """,
        [ba],
    ).fetchone()
    if row is None:
        raise SystemExit(
            f"No complete days in mart_hourly_profile for {ba}; "
            "nothing to chart yet (has a backfill + build run?)")
    month, n_days = row
    month = month if isinstance(month, date) else month.date()
    days_in_month = calendar.monthrange(month.year, month.month)[1]
    if n_days >= days_in_month:
        label = f"{month:%B %Y} average ({n_days} days)"
    else:
        lo, hi = con.execute(
            """
            select min(local_date), max(local_date)
            from main.mart_grid_daily
            where ba_code = ? and is_complete_day
              and is_net_load_complete_day and hours_expected = 24
              and date_trunc('month', local_date) = ?
            """,
            [ba, month],
        ).fetchone()
        if lo.month == hi.month:
            label = f"{lo:%b} {lo.day}–{hi.day}, {hi:%Y} · average of {n_days} complete days"
        else:
            label = (f"{lo:%b} {lo.day} – {hi:%b} {hi.day}, {hi:%Y}"
                     f" · average of {n_days} complete days")
    return month, n_days, label


def duck_curve(con, ba: str, ba_name: str, out: Path, provenance: str) -> None:
    month, _, label = pick_profile_window(con, ba)
    df = con.execute(
        """
        select local_hour,
               avg_demand_mwh / 1000.0   as demand_gw,
               avg_net_load_mwh / 1000.0 as net_load_gw,
               (coalesce(avg_solar_mwh, 0) + coalesce(avg_wind_mwh, 0)) / 1000.0
                                         as wind_solar_gw
        from main.mart_hourly_profile
        where ba_code = ? and month = ?
        order by local_hour
        """,
        [ba, month],
    ).df()

    if df.net_load_gw.isna().all():
        raise SystemExit(
            f"Refusing to render a duck curve for {ba}: no valid net-load "
            "hours in the selected window (fuel data absent or incomplete). "
            "A demand-only chart is not a duck curve.")
    fig, ax = plt.subplots(figsize=(10, 5.6))
    ax.plot(df.local_hour, df.demand_gw, lw=2.2, color="#1f77b4", label="Demand")
    ax.plot(df.local_hour, df.net_load_gw, lw=2.2, color="#d62728",
            label="Net load (demand − wind − solar)")
    ax.fill_between(df.local_hour, df.net_load_gw, df.demand_gw,
                    color="#2ca02c", alpha=0.18)

    # Annotate the two operational features that make this "the duck".
    valid = df.dropna(subset=["net_load_gw"])
    midday = valid[(valid.local_hour >= 9) & (valid.local_hour <= 16)]
    if not midday.empty:
        m = midday.loc[midday.net_load_gw.idxmin()]
        ax.annotate(f"midday minimum\n{m.net_load_gw:.1f} GW at {int(m.local_hour):02d}:00",
                    xy=(m.local_hour, m.net_load_gw),
                    xytext=(m.local_hour + 1.5, m.net_load_gw + 2.6),
                    fontsize=8.5, color="#8b0000",
                    arrowprops=dict(arrowstyle="->", color="#8b0000", lw=1))
        evening = valid[valid.local_hour >= int(m.local_hour)]
        if not evening.empty:
            e = evening.loc[evening.net_load_gw.idxmax()]
            ramp = e.net_load_gw - m.net_load_gw
            hrs = int(e.local_hour - m.local_hour)
            ax.annotate(f"evening ramp:\n+{ramp:.1f} GW in {hrs}h",
                        xy=(e.local_hour, e.net_load_gw),
                        xytext=(e.local_hour - 1.8, e.net_load_gw + 1.6),
                        fontsize=8.5, color="#8b0000",
                        arrowprops=dict(arrowstyle="->", color="#8b0000", lw=1))

    handles, labels = ax.get_legend_handles_labels()
    handles.append(Patch(facecolor="#2ca02c", alpha=0.18))
    labels.append("Wind + solar")
    ax.legend(handles, labels, loc="lower left", fontsize=9, framealpha=0.9)

    ax.set_title(f"{ba_name} ({ba}) — hourly demand vs net load\n{label}",
                 fontsize=12)
    ax.set_xlabel("Hour of day (local time)")
    ax.set_ylabel("Average load (GW)")
    ax.set_xticks(range(0, 24, 3))
    ax.grid(alpha=0.25)
    apply_watermark(fig, provenance)
    footer(fig, NET_LOAD_NOTE)
    fig.tight_layout(rect=(0, 0.03, 1, 0.98))
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"wrote {out}")


def renewable_share_chart(con, ba: str, ba_name: str, out: Path,
                          provenance: str) -> None:
    df = con.execute(
        """
        select local_date, renewable_share, carbon_free_share
        from main.mart_grid_daily
        where ba_code = ? and is_complete_day and is_fuel_mix_complete_day
          and local_date > (
              select max(local_date) - interval 90 day
              from main.mart_grid_daily where ba_code = ?)
        order by local_date
        """,
        [ba, ba],
    ).df()

    if df.empty or df.carbon_free_share.isna().all():
        raise SystemExit(
            f"Refusing to render the renewable-share chart for {ba}: no "
            "fuel-mix-complete days with usable shares in the last 90 days. "
            "Publishing nothing beats publishing an empty axis.")
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(df.local_date, 100 * df.carbon_free_share, lw=1.8,
            color="#2ca02c", label="Carbon-free (incl. nuclear)")
    ax.plot(df.local_date, 100 * df.renewable_share, lw=1.8,
            color="#1f77b4", label="Renewable")
    locator = mdates.AutoDateLocator()
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
    ax.set_title(f"{ba_name} ({ba}) — daily generation shares\n"
                 f"last 90 days · complete days only · n={len(df)}", fontsize=12)
    ax.set_ylabel("Share of reported generation (%)")
    ax.set_ylim(0, 100)
    ax.grid(alpha=0.25)
    ax.legend(loc="upper left", fontsize=9)
    fig.subplots_adjust(top=0.86)
    apply_watermark(fig, provenance)
    footer(fig, " · volume-weighted daily shares, fuel-mix-complete days only")
    fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"wrote {out}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ba", default="CISO")
    parser.add_argument("--db", default=os.environ.get(
        "GRID_DB_PATH", str(REPO_ROOT / "data" / "energy_grid.duckdb")))
    parser.add_argument("--outdir", default=str(REPO_ROOT / "docs" / "img"))
    args = parser.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    try:
        con = duckdb.connect(args.db, read_only=True)
    except duckdb.Error as exc:
        raise SystemExit(
            f"Refusing to chart: cannot open {args.db} ({exc}). "
            "Point --db (or GRID_DB_PATH for the make targets) at a built "
            "warehouse.") from exc
    try:
        provenance = data_provenance(con)
        print(f"data provenance: {provenance}"
              + ("" if provenance == "real" else "  -> watermarking output"))
        ba_name = con.execute(
            "select ba_name from main_seeds.balancing_authorities where ba_code = ?",
            [args.ba]).fetchone()[0]
        suffix = f"_{args.ba.lower()}" if args.ba != "CISO" else ""
        targets = [outdir / f"duck_curve{suffix}.png",
                   outdir / f"renewable_share{suffix}.png"]
        tmps = [t.with_name(t.stem + ".tmp.png") for t in targets]
        try:
            # Render BOTH to temp files first: if either fails, no published
            # chart changes, and a stale-but-consistent pair beats a fresh
            # duck curve sitting next to a broken share chart.
            duck_curve(con, args.ba, ba_name, tmps[0], provenance)
            renewable_share_chart(con, args.ba, ba_name, tmps[1], provenance)
            for tmp, target in zip(tmps, targets, strict=True):
                os.replace(tmp, target)
        except duckdb.Error as exc:
            for tmp in tmps:
                tmp.unlink(missing_ok=True)
            raise SystemExit(
                f"Refusing to chart: required marts are missing or unreadable "
                f"in {args.db} ({exc}). Run the dbt build first; if it failed, "
                "the data was not publication-ready and no chart should exist."
            ) from exc
        except BaseException:
            for tmp in tmps:
                tmp.unlink(missing_ok=True)
            raise
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
