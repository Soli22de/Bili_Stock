"""
Live Validation: SRF Scoring vs battle_trades Ground Truth
===========================================================
Tests whether SmartResonanceFactor scores predict returns within the
live paper-trading universe (data/battle_trades_all.csv, Jan 2025 – Feb 2026).

Two questions:
  Q1. Selection accuracy — do high-SRF stocks land in Smart Money trades
      more than chance? (measures whether SRF agrees with live signals)
  Q2. Ranking quality — within the Smart Money basket for each date, do
      higher-SRF stocks produce better 1-week returns?

Output:
  research/baseline_v6_1/output/live_validation_srf_summary.csv
  research/baseline_v6_1/output/live_validation_srf_by_date.csv
"""

import os
import sys

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v6_1.code.run_baseline_v6_v61_suite import _enrich_from_stock_data, _srf_score
from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5


# ── helpers ──────────────────────────────────────────────────────────────────

def _load_battle_trades() -> pd.DataFrame:
    path = os.path.join(ROOT, "data", "battle_trades_all.csv")
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])
    df["symbol"] = df["symbol"].astype(str).str.upper()
    return df


def _round_trips(df: pd.DataFrame) -> pd.DataFrame:
    """Match each BUY to its corresponding SELL and compute return."""
    buys = df[df["type"] == "BUY"].copy()
    sells = df[df["type"] == "SELL"].copy()
    m = buys.merge(sells, on=["symbol", "Strategy"], suffixes=("_buy", "_sell"))
    m = m[m["date_sell"] > m["date_buy"]]
    m = m.sort_values("date_sell").groupby(["symbol", "Strategy", "date_buy"]).first().reset_index()
    m["ret"] = m["price_sell"] / m["price_buy"] - 1.0
    m["hold_days"] = (m["date_sell"] - m["date_buy"]).dt.days
    m["win"] = m["ret"] > 0
    return m


def _nearest_panel_date(panel_dates: list, target: pd.Timestamp) -> pd.Timestamp | None:
    """Return closest panel date ≤ target within 5 days, else None."""
    candidates = [d for d in panel_dates if d <= target]
    if not candidates:
        return None
    closest = max(candidates)
    if (target - closest).days > 5:
        return None
    return closest


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    os.makedirs(out_dir, exist_ok=True)

    print("Loading panel …")
    panel_raw = _prepare_panel_v5(start_date="2024-01-01", end_date="2026-03-31")
    panel, px_map = _enrich_from_stock_data(panel_raw)
    panel["symbol_upper"] = panel["stock_symbol"].astype(str).str.upper()
    panel_dates = sorted(panel["date"].unique().tolist())

    print("Loading battle_trades …")
    trades = _load_battle_trades()
    rt = _round_trips(trades)
    smart = rt[rt["Strategy"] == "Smart Money"].copy()
    smart_buys = smart[["date_buy", "symbol", "ret", "win"]].copy()

    # ── Q1 & Q2: for each battle date, score the full panel ──────────────────
    rows = []
    battle_dates = sorted(smart_buys["date_buy"].unique().tolist())
    print(f"Scoring {len(battle_dates)} battle dates …")

    for bd in battle_dates:
        pdate = _nearest_panel_date(panel_dates, pd.Timestamp(bd))
        if pdate is None:
            print(f"  {bd.date()} — no panel date found, skipping")
            continue
        day = panel[panel["date"] == pdate].copy()
        if len(day) < 10:
            continue
        # compute SRF scores for all stocks on this panel date
        day["srf"] = _srf_score(day).values
        day["srf_pct"] = day["srf"].rank(pct=True)

        # battle stocks on this date
        battle_syms = set(smart_buys[smart_buys["date_buy"] == bd]["symbol"].tolist())
        day["is_battle"] = day["symbol_upper"].isin(battle_syms)
        matched = day[day["is_battle"]].copy()

        n_panel = len(day)
        n_battle = len(battle_syms)
        n_matched = len(matched)

        # Q1: average SRF percentile of battle stocks vs 0.5 (random baseline)
        avg_srf_pct = float(matched["srf_pct"].mean()) if not matched.empty else np.nan

        # Q2: within matched stocks, Spearman rank correlation between SRF and ret
        battle_ret = smart_buys[smart_buys["date_buy"] == bd].set_index("symbol")["ret"]
        matched = matched.copy()
        matched["battle_ret"] = matched["symbol_upper"].map(battle_ret.to_dict())
        valid = matched.dropna(subset=["srf", "battle_ret"])
        if len(valid) >= 3:
            rho, pval = scipy_stats.spearmanr(valid["srf"], valid["battle_ret"])
        else:
            rho, pval = np.nan, np.nan

        # Q2b: top-half SRF vs bottom-half within battle stocks
        if len(valid) >= 4:
            mid = valid["srf"].median()
            top_ret = float(valid[valid["srf"] >= mid]["battle_ret"].mean())
            bot_ret = float(valid[valid["srf"] < mid]["battle_ret"].mean())
        else:
            top_ret = bot_ret = np.nan

        rows.append({
            "date": bd.date(),
            "panel_date": pdate.date(),
            "n_panel": n_panel,
            "n_battle": n_battle,
            "n_matched": n_matched,
            "avg_srf_pct": avg_srf_pct,
            "srf_spearman_rho": rho,
            "srf_spearman_pval": pval,
            "top_srf_ret": top_ret,
            "bot_srf_ret": bot_ret,
            "srf_split_edge": top_ret - bot_ret if pd.notna(top_ret) and pd.notna(bot_ret) else np.nan,
        })

    by_date = pd.DataFrame(rows)
    by_date_path = os.path.join(out_dir, "live_validation_srf_by_date.csv")
    by_date.to_csv(by_date_path, index=False, encoding="utf-8-sig")

    # ── aggregate summary ─────────────────────────────────────────────────────
    valid_q1 = by_date.dropna(subset=["avg_srf_pct"])
    valid_q2 = by_date.dropna(subset=["srf_spearman_rho"])
    valid_edge = by_date.dropna(subset=["srf_split_edge"])

    # Q1: t-test — is avg_srf_pct > 0.5?
    tstat, tpval = scipy_stats.ttest_1samp(valid_q1["avg_srf_pct"], 0.5) if len(valid_q1) >= 3 else (np.nan, np.nan)

    summary = {
        "n_dates": len(by_date),
        "n_dates_with_panel_match": len(valid_q1),
        # Q1: selection accuracy
        "avg_srf_pct_of_battle_stocks": float(valid_q1["avg_srf_pct"].mean()),
        "srf_pct_vs_0.5_tstat": float(tstat),
        "srf_pct_vs_0.5_pval": float(tpval),
        # Q2: ranking quality (Spearman)
        "avg_srf_spearman_rho": float(valid_q2["srf_spearman_rho"].mean()),
        "pct_dates_positive_rho": float((valid_q2["srf_spearman_rho"] > 0).mean()),
        # Q2b: top-half vs bottom-half split
        "avg_top_srf_ret": float(valid_edge["top_srf_ret"].mean()),
        "avg_bot_srf_ret": float(valid_edge["bot_srf_ret"].mean()),
        "avg_srf_split_edge": float(valid_edge["srf_split_edge"].mean()),
        "pct_dates_positive_edge": float((valid_edge["srf_split_edge"] > 0).mean()),
    }

    summary_df = pd.DataFrame([summary]).T.rename(columns={0: "value"})
    summary_path = os.path.join(out_dir, "live_validation_srf_summary.csv")
    summary_df.to_csv(summary_path, encoding="utf-8-sig")

    print("\n── Live Validation Summary ──────────────────────────────")
    print(f"  Dates tested               : {summary['n_dates_with_panel_match']}/{summary['n_dates']}")
    print(f"\n  Q1 — Selection accuracy")
    print(f"  Avg SRF pct of battle picks: {summary['avg_srf_pct_of_battle_stocks']:.3f}  (0.50 = random)")
    print(f"  t-stat vs 0.50             : {summary['srf_pct_vs_0.5_tstat']:.2f}  (p={summary['srf_pct_vs_0.5_pval']:.3f})")
    print(f"\n  Q2 — Ranking quality (Spearman rho, SRF vs next-week ret)")
    print(f"  Avg rho                    : {summary['avg_srf_spearman_rho']:.3f}")
    print(f"  % dates with positive rho  : {summary['pct_dates_positive_rho']:.1%}")
    print(f"\n  Q2b — Top-half SRF vs bottom-half SRF (within battle stocks)")
    print(f"  Top-SRF avg ret            : {summary['avg_top_srf_ret']:.4f}")
    print(f"  Bot-SRF avg ret            : {summary['avg_bot_srf_ret']:.4f}")
    print(f"  Edge (top - bot)           : {summary['avg_srf_split_edge']:.4f}")
    print(f"  % dates with positive edge : {summary['pct_dates_positive_edge']:.1%}")
    print(f"\n  Saved → {summary_path}")
    print(f"  Saved → {by_date_path}")


if __name__ == "__main__":
    main()
