"""
Signal IC Comparison: Count vs Conviction vs Quality-Weighted
=============================================================
Tests three Xueqiu signal variants head-to-head:

  1) net_buy_cube_count (current): COUNT(DISTINCT cube) where buy
  2) conviction_weighted: SUM(weight_delta) where buy, clipped at p99
  3) quality_weighted: SUM(weight_delta * log(1 + total_gain)) where buy
  4) net_flow: SUM(weight_delta) for ALL trades (buys positive, sells negative)

For each, compute:
  - Cross-sectional IC with fwd_ret_2w, by regime
  - ICIR
  - Hit rate (% of dates with positive IC)

Run: python research/baseline_v6_1/code/run_signal_ic_comparison.py
"""

import os
import sys
import sqlite3

import numpy as np
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def _zscore(s: pd.Series) -> pd.Series:
    std = s.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=s.index)
    return (s - s.mean()) / std


def _load_rebalancing() -> pd.DataFrame:
    db_path = os.path.join(ROOT, "data", "cubes.db")
    conn = sqlite3.connect(db_path)
    rb = pd.read_sql_query(
        "SELECT cube_symbol, stock_symbol, created_at, target_weight, prev_weight_adjusted "
        "FROM rebalancing_history WHERE status = 'success'",
        conn,
    )
    conn.close()
    rb["date"] = pd.to_datetime(rb["created_at"], errors="coerce").dt.normalize()
    rb["target_weight"] = pd.to_numeric(rb["target_weight"], errors="coerce")
    rb["prev_weight_adjusted"] = pd.to_numeric(rb["prev_weight_adjusted"], errors="coerce")
    rb = rb.dropna(subset=["date", "cube_symbol", "stock_symbol", "target_weight", "prev_weight_adjusted"])
    rb["weight_delta"] = rb["target_weight"] - rb["prev_weight_adjusted"]
    rb["stock_symbol"] = rb["stock_symbol"].astype(str).str.upper()
    rb["cube_symbol"] = rb["cube_symbol"].astype(str)
    return rb


def _load_cube_quality() -> pd.DataFrame:
    db_path = os.path.join(ROOT, "data", "cubes.db")
    conn = sqlite3.connect(db_path)
    cubes = pd.read_sql_query(
        "SELECT symbol, total_gain, followers_count FROM cubes",
        conn,
    )
    conn.close()
    cubes["total_gain"] = pd.to_numeric(cubes["total_gain"], errors="coerce").fillna(0.0)
    cubes["followers_count"] = pd.to_numeric(cubes["followers_count"], errors="coerce").fillna(0)
    cubes["symbol"] = cubes["symbol"].astype(str)
    # Quality weight: log(1 + total_gain%) — rewards higher-return cubes
    cubes["quality_w"] = np.log1p(cubes["total_gain"].clip(lower=0) / 100.0)
    return cubes


def _build_factors(rb: pd.DataFrame, cubes: pd.DataFrame) -> pd.DataFrame:
    """Build four factor variants from rebalancing data."""
    buy = rb[rb["weight_delta"] > 0].copy()
    # Clip extreme weight_delta at p99 to reduce noise
    clip_val = buy["weight_delta"].quantile(0.99)
    buy["wd_clipped"] = buy["weight_delta"].clip(upper=clip_val)

    # Join cube quality
    buy = buy.merge(cubes[["symbol", "quality_w"]], left_on="cube_symbol", right_on="symbol", how="left")
    buy["quality_w"] = buy["quality_w"].fillna(0.0)

    # Factor 1: net_buy_cube_count (current)
    f1 = buy.groupby(["date", "stock_symbol"])["cube_symbol"].nunique().reset_index()
    f1.columns = ["date", "stock_symbol", "f_count"]

    # Factor 2: conviction-weighted (sum of clipped weight_delta)
    f2 = buy.groupby(["date", "stock_symbol"])["wd_clipped"].sum().reset_index()
    f2.columns = ["date", "stock_symbol", "f_conviction"]

    # Factor 3: quality-weighted (sum of weight_delta * quality_w)
    buy["qw_delta"] = buy["wd_clipped"] * buy["quality_w"]
    f3 = buy.groupby(["date", "stock_symbol"])["qw_delta"].sum().reset_index()
    f3.columns = ["date", "stock_symbol", "f_quality"]

    # Factor 4: net_flow (buys - sells, using all trades)
    all_trades = rb.copy()
    clip_all = all_trades["weight_delta"].abs().quantile(0.99)
    all_trades["wd_clipped"] = all_trades["weight_delta"].clip(lower=-clip_all, upper=clip_all)
    f4 = all_trades.groupby(["date", "stock_symbol"])["wd_clipped"].sum().reset_index()
    f4.columns = ["date", "stock_symbol", "f_netflow"]

    # Factor 5: high-conviction only (weight_delta > 2.0 threshold)
    high_conv = buy[buy["weight_delta"] > 2.0].copy()
    f5 = high_conv.groupby(["date", "stock_symbol"])["cube_symbol"].nunique().reset_index()
    f5.columns = ["date", "stock_symbol", "f_highconv_count"]

    # Merge all
    out = f1.merge(f2, on=["date", "stock_symbol"], how="outer")
    out = out.merge(f3, on=["date", "stock_symbol"], how="outer")
    out = out.merge(f4, on=["date", "stock_symbol"], how="outer")
    out = out.merge(f5, on=["date", "stock_symbol"], how="outer")
    for c in ["f_count", "f_conviction", "f_quality", "f_netflow", "f_highconv_count"]:
        out[c] = out[c].fillna(0.0)
    return out


def _ic_analysis(panel: pd.DataFrame, factor_col: str, regime_col: str = "regime") -> dict:
    """Compute IC stats for a factor against fwd_ret_2w, overall and by regime."""
    sub = panel.dropna(subset=[factor_col, "fwd_ret_2w"]).copy()
    if sub.empty:
        return {}

    results = {}
    # Overall
    ics = sub.groupby("date").apply(
        lambda g: g[factor_col].corr(g["fwd_ret_2w"]) if len(g) >= 5 else np.nan
    ).dropna()
    if len(ics) > 0:
        results["overall"] = {
            "IC": float(ics.mean()),
            "ICIR": float(ics.mean() / ics.std()) if ics.std() > 0 else np.nan,
            "hit_rate": float((ics > 0).mean() * 100),
            "n_dates": int(len(ics)),
        }

    # By regime
    if regime_col in sub.columns:
        for regime in ["上涨", "震荡", "下跌"]:
            rsub = sub[sub[regime_col] == regime]
            if rsub.empty:
                continue
            rics = rsub.groupby("date").apply(
                lambda g: g[factor_col].corr(g["fwd_ret_2w"]) if len(g) >= 5 else np.nan
            ).dropna()
            if len(rics) > 0:
                results[regime] = {
                    "IC": float(rics.mean()),
                    "ICIR": float(rics.mean() / rics.std()) if rics.std() > 0 else np.nan,
                    "hit_rate": float((rics > 0).mean() * 100),
                    "n_dates": int(len(rics)),
                }
    return results


def main():
    print("Loading rebalancing data …", flush=True)
    rb = _load_rebalancing()
    print(f"  {len(rb):,} trades ({(rb['weight_delta']>0).sum():,} buys, {(rb['weight_delta']<0).sum():,} sells)")

    print("Loading cube quality …", flush=True)
    cubes = _load_cube_quality()
    print(f"  {len(cubes):,} cubes, median total_gain={cubes['total_gain'].median():.1f}%")

    print("Building factor variants …", flush=True)
    factors = _build_factors(rb, cubes)
    print(f"  {len(factors):,} date-stock rows")

    # Load panel with forward returns and regime
    print("Loading panel (fwd_ret + regime) …", flush=True)
    from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5
    panel = _prepare_panel_v5()
    panel = panel[
        (panel["date"] >= pd.Timestamp("2010-01-01"))
        & (panel["date"] <= pd.Timestamp("2025-12-31"))
    ].copy()
    panel["stock_symbol"] = panel["stock_symbol"].astype(str).str.upper()

    # Merge factors into panel
    panel = panel.merge(factors, on=["date", "stock_symbol"], how="left")
    for c in ["f_count", "f_conviction", "f_quality", "f_netflow", "f_highconv_count"]:
        panel[c] = panel[c].fillna(0.0)
        # Cross-sectional z-score per date
        panel[f"{c}_z"] = panel.groupby("date")[c].transform(_zscore)

    # Correlation between signals
    print("\n" + "=" * 70)
    print("SIGNAL CORRELATION (cross-sectional z-scores, mean across dates)")
    factor_z_cols = [f"{c}_z" for c in ["f_count", "f_conviction", "f_quality", "f_netflow", "f_highconv_count"]]
    corr = panel[factor_z_cols].corr()
    corr.columns = ["count", "conviction", "quality", "netflow", "highconv"]
    corr.index = ["count", "conviction", "quality", "netflow", "highconv"]
    print(corr.round(3).to_string())

    # IC analysis for each factor
    factor_labels = {
        "f_count_z": "1. Count (current)",
        "f_conviction_z": "2. Conviction (sum delta)",
        "f_quality_z": "3. Quality-weighted",
        "f_netflow_z": "4. Net flow (buy-sell)",
        "f_highconv_count_z": "5. High-conviction count",
        "factor_z": "0. Current pipeline (factor_z)",
    }

    print("\n" + "=" * 70)
    print("IC COMPARISON BY REGIME")
    print(f"{'Factor':<30s} {'Overall IC':>10s} {'ICIR':>8s} {'Hit%':>6s} {'上涨 IC':>10s} {'震荡 IC':>10s} {'下跌 IC':>10s}")
    print("-" * 90)

    rows = []
    for col, label in factor_labels.items():
        if col not in panel.columns:
            continue
        ic = _ic_analysis(panel, col)
        ov = ic.get("overall", {})
        row = {
            "factor": label,
            "overall_IC": ov.get("IC", np.nan),
            "ICIR": ov.get("ICIR", np.nan),
            "hit_rate": ov.get("hit_rate", np.nan),
            "上涨_IC": ic.get("上涨", {}).get("IC", np.nan),
            "震荡_IC": ic.get("震荡", {}).get("IC", np.nan),
            "下跌_IC": ic.get("下跌", {}).get("IC", np.nan),
        }
        rows.append(row)
        print(f"{label:<30s} {row['overall_IC']:>10.6f} {row['ICIR']:>8.4f} {row['hit_rate']:>5.1f}% {row['上涨_IC']:>10.6f} {row['震荡_IC']:>10.6f} {row['下跌_IC']:>10.6f}")

    # Save results
    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    os.makedirs(out_dir, exist_ok=True)
    pd.DataFrame(rows).to_csv(
        os.path.join(out_dir, "signal_ic_comparison.csv"),
        index=False, encoding="utf-8-sig",
    )
    print(f"\nSaved → {out_dir}/signal_ic_comparison.csv")

    # Conviction vs count scatter — how different are they?
    print("\n" + "=" * 70)
    print("SIGNAL DIVERGENCE ANALYSIS")
    active = panel[panel["f_count"] > 0].copy()
    if not active.empty:
        print(f"Days with any buy signal: {active['date'].nunique()}")
        print(f"Stock-days with buy: {len(active):,}")
        # Where conviction disagrees with count
        active["count_rank"] = active.groupby("date")["f_count_z"].rank(pct=True)
        active["conv_rank"] = active.groupby("date")["f_conviction_z"].rank(pct=True)
        divergent = active[(active["count_rank"] >= 0.7) != (active["conv_rank"] >= 0.7)]
        print(f"Divergent stock-days (top 30% by one, not by other): {len(divergent):,} ({len(divergent)/len(active)*100:.1f}%)")
        if not divergent.empty and "fwd_ret_2w" in divergent.columns:
            # When conviction says top but count doesn't
            conv_top = divergent[divergent["conv_rank"] >= 0.7]
            count_top = divergent[divergent["count_rank"] >= 0.7]
            print(f"  Conviction-top-only fwd_ret: {conv_top['fwd_ret_2w'].mean()*100:.3f}%")
            print(f"  Count-top-only fwd_ret: {count_top['fwd_ret_2w'].mean()*100:.3f}%")


if __name__ == "__main__":
    main()
