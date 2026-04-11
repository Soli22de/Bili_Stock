"""
Regime IC Analysis
==================
Compares factor_z_neu IC across regimes under two regime classifiers:
  - Old: pure HS300 20-day return (上涨>2%, 下跌<-2%, else 震荡)
  - New: ret20 + ADX(14) combined (上涨: ret20>2% AND ADX>20, etc.)

Also reports:
  - Date distribution by regime (old vs new)
  - IC per year by regime
  - Choppy regime breakdown (how many formerly-choppy dates move to bull/bear)

Run: python research/baseline_v6_1/code/run_regime_ic_analysis.py
"""

import os
import sys

import numpy as np
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import baostock as bs
from research.baseline_v4.code.run_baseline_v4_2_up_filter import _adx
from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5


def _load_hs300_old(start: str, end: str) -> pd.DataFrame:
    lg = bs.login()
    rs = bs.query_history_k_data_plus("sh.000300", "date,close", start, end, "d")
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    bs.logout()
    df = pd.DataFrame(rows, columns=["date", "close"])
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna().sort_values("date")
    df["ret20"] = df["close"] / df["close"].shift(20) - 1.0
    df["regime_old"] = "震荡"
    df.loc[df["ret20"] > 0.02, "regime_old"] = "上涨"
    df.loc[df["ret20"] < -0.02, "regime_old"] = "下跌"
    return df[["date", "ret20", "regime_old"]]


def _load_hs300_new(start: str, end: str) -> pd.DataFrame:
    lg = bs.login()
    rs = bs.query_history_k_data_plus("sh.000300", "date,open,high,low,close", start, end, "d")
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    bs.logout()
    df = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close"])
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["close"]).sort_values("date").reset_index(drop=True)
    df["ret20"] = df["close"] / df["close"].shift(20) - 1.0
    df["adx14"] = _adx(df["high"], df["low"], df["close"], n=14)
    df["regime_new"] = "震荡"
    trending = df["adx14"] > 20
    df.loc[trending & (df["ret20"] > 0.02), "regime_new"] = "上涨"
    df.loc[trending & (df["ret20"] < -0.02), "regime_new"] = "下跌"
    return df[["date", "adx14", "regime_new"]]


def _ic_by_regime(panel: pd.DataFrame, regime_col: str) -> pd.DataFrame:
    rows = []
    for regime in ["上涨", "震荡", "下跌"]:
        sub = panel[panel[regime_col] == regime].dropna(subset=["factor_z_neu", "fwd_ret_2w"])
        if sub.empty:
            rows.append({"regime": regime, "n_dates": 0, "IC": np.nan, "ICIR": np.nan, "win_rate": np.nan})
            continue
        ics = sub.groupby("date").apply(
            lambda g: g["factor_z_neu"].corr(g["fwd_ret_2w"]) if len(g) >= 5 else np.nan
        ).dropna()
        rows.append({
            "regime": regime,
            "n_dates": len(ics),
            "pct_dates": len(ics) / panel["date"].nunique() * 100,
            "IC": ics.mean(),
            "ICIR": ics.mean() / ics.std() if ics.std() > 0 else np.nan,
            "win_rate": (ics > 0).mean() * 100,
        })
    return pd.DataFrame(rows)


def main():
    print("Loading panel …", flush=True)
    panel = _prepare_panel_v5()
    panel = panel[
        (panel["date"] >= pd.Timestamp("2010-01-01"))
        & (panel["date"] <= pd.Timestamp("2025-12-31"))
    ].copy()
    print(f"Panel: {len(panel):,} rows, {panel['date'].nunique()} unique dates", flush=True)

    print("Loading HS300 (old regime) …", flush=True)
    hs_old = _load_hs300_old("2010-01-01", "2025-12-31")
    print("Loading HS300 (new ADX regime) …", flush=True)
    hs_new = _load_hs300_new("2010-01-01", "2025-12-31")

    # Merge both regimes into panel
    panel = panel.merge(hs_old[["date", "regime_old"]], on="date", how="left")
    panel = panel.merge(hs_new[["date", "regime_new"]], on="date", how="left")

    # ── Regime distribution ───────────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("REGIME DISTRIBUTION (trading days)")
    hs_both = hs_old.merge(hs_new, on="date", how="inner")
    print("\nOld (ret20 only):")
    print(hs_both["regime_old"].value_counts().to_string())
    print(f"  Choppy %: {(hs_both['regime_old']=='震荡').mean()*100:.1f}%")
    print("\nNew (ret20 + ADX):")
    print(hs_both["regime_new"].value_counts().to_string())
    print(f"  Choppy %: {(hs_both['regime_new']=='震荡').mean()*100:.1f}%")

    # Transition matrix: what happened to formerly-choppy dates?
    choppy_old = hs_both[hs_both["regime_old"] == "震荡"]
    print(f"\nFormerly-choppy dates ({len(choppy_old)}) reclassified as:")
    print(choppy_old["regime_new"].value_counts().to_string())

    # ── IC by regime ──────────────────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("FACTOR_Z_NEU IC BY REGIME")
    print("\nOld classifier:")
    ic_old = _ic_by_regime(panel, "regime_old")
    print(ic_old.to_string(index=False))
    print("\nNew (ADX) classifier:")
    ic_new = _ic_by_regime(panel, "regime_new")
    print(ic_new.to_string(index=False))

    # ── Year-by-year choppy IC ────────────────────────────────────────────────
    print("\n" + "─" * 60)
    print("CHOPPY REGIME IC BY YEAR (old vs new classifier)")
    panel["year"] = pd.to_datetime(panel["date"]).dt.year
    rows = []
    for yr in sorted(panel["year"].unique()):
        yp = panel[panel["year"] == yr]
        for col, label in [("regime_old", "old"), ("regime_new", "new")]:
            sub = yp[yp[col] == "震荡"].dropna(subset=["factor_z_neu", "fwd_ret_2w"])
            ics = sub.groupby("date").apply(
                lambda g: g["factor_z_neu"].corr(g["fwd_ret_2w"]) if len(g) >= 5 else np.nan
            ).dropna()
            rows.append({"year": yr, "classifier": label,
                         "n_choppy_dates": len(ics), "IC": ics.mean() if len(ics) else np.nan})
    yr_df = pd.DataFrame(rows)
    pivot = yr_df.pivot(index="year", columns="classifier", values=["n_choppy_dates", "IC"])
    print(pivot.to_string())

    # Save
    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    os.makedirs(out_dir, exist_ok=True)
    ic_old.to_csv(os.path.join(out_dir, "regime_ic_old_classifier.csv"), index=False)
    ic_new.to_csv(os.path.join(out_dir, "regime_ic_new_classifier.csv"), index=False)
    yr_df.to_csv(os.path.join(out_dir, "regime_ic_by_year.csv"), index=False)
    print(f"\nSaved to {out_dir}")


if __name__ == "__main__":
    main()
