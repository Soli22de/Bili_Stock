import glob
import os
import sys

import numpy as np
import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v4.code.run_baseline_v4_2_up_filter import (
    _apply_liq_dynamic,
    _apply_up_exposure,
    _assign_other_industry_by_proxy,
    _attach_base_fields,
    _industry_neutralize,
    _load_hs300,
    _select_top_with_industry_cap,
)
from research.data_prep.build_rebalance_momentum_panel import build_rebalance_momentum_panel


def _load_tradability_from_stock_data(root: str) -> pd.DataFrame:
    rows = []
    files = glob.glob(os.path.join(root, "data", "stock_data", "*.csv"))
    for fp in files:
        sym = os.path.splitext(os.path.basename(fp))[0].upper()
        try:
            df = pd.read_csv(fp, usecols=["日期", "涨跌幅", "成交量"])
        except Exception:
            continue
        df.rename(columns={"日期": "date", "涨跌幅": "pctChg", "成交量": "volume"}, inplace=True)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
        df["pctChg"] = pd.to_numeric(df["pctChg"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        df["stock_symbol"] = sym
        df = df.dropna(subset=["date"])
        rows.append(df[["date", "stock_symbol", "pctChg", "volume"]])
    if not rows:
        return pd.DataFrame(columns=["date", "stock_symbol", "is_suspended", "is_limit"])
    x = pd.concat(rows, ignore_index=True)
    code = x["stock_symbol"].str[-6:]
    is_20 = code.str.startswith(("30", "68"))
    limit_abs = np.where(is_20, 19.6, 9.6)
    x["is_suspended"] = x["volume"].fillna(0) <= 0
    x["is_limit"] = x["pctChg"].abs() >= limit_abs
    return x[["date", "stock_symbol", "is_suspended", "is_limit"]]


def _prepare_panel_v5(start_date: str = "2010-01-01", end_date: str = "2025-12-31", signal_mode: str = "count") -> pd.DataFrame:
    stock_data_dir = os.path.join(ROOT, "data", "stock_data")
    market_cache_dir = os.path.join(ROOT, "data", "market_cache")
    cache_dir = stock_data_dir if os.path.exists(stock_data_dir) and len(glob.glob(os.path.join(stock_data_dir, "*.csv"))) > 0 else market_cache_dir
    panel = build_rebalance_momentum_panel(
        db_path=os.path.join(ROOT, "data", "cubes.db"),
        cache_dir=cache_dir,
        out_csv=os.path.join(ROOT, "research", "baseline_v5", "output", f"factor_panel_rebalance_momentum_{start_date[:4]}_{end_date[:4]}.csv"),
        start_date=start_date,
        end_date=end_date,
        lag_days=14,
        smoothing_days=3,
        factor_mode="rate",
        signal_mode=signal_mode,
    )
    base = _attach_base_fields(
        panel,
        industry_map_csv=os.path.join(ROOT, "research", "baseline_v1", "data_delivery", "industry_mapping_v2.csv"),
        liquidity_csv=os.path.join(ROOT, "research", "baseline_v1", "data_delivery", "liquidity_daily_v1.csv"),
    )
    base = _assign_other_industry_by_proxy(base)
    base["factor_z_raw"] = base["factor_z"]
    base = _industry_neutralize(base, source_col="factor_z_raw", out_col="factor_z_neu")
    regime = _load_hs300(start_date, end_date)
    panel_liq = _apply_liq_dynamic(base, regime_df=regime, keep_other=0.6, keep_up=0.2)
    # Create liq_rank_pct so _run_one can apply its own liquidity filter
    if "amount" in panel_liq.columns:
        panel_liq["liq_rank_pct"] = panel_liq.groupby("date")["amount"].transform(
            lambda s: s.rank(pct=True, method="first") if s.notna().any() else 1.0
        )
    trad = _load_tradability_from_stock_data(ROOT)
    panel_v5 = panel_liq.merge(trad, on=["date", "stock_symbol"], how="left")
    panel_v5["is_suspended"] = panel_v5["is_suspended"].fillna(False)
    panel_v5["is_limit"] = panel_v5["is_limit"].fillna(False)
    panel_v5 = panel_v5[~panel_v5["is_suspended"] & ~panel_v5["is_limit"]].copy()
    return panel_v5


def _build_rebalance_rows(panel_v5: pd.DataFrame, trim_q: float = 0.05, hold_step: int = 10) -> pd.DataFrame:
    df = panel_v5.dropna(subset=["date", "stock_symbol", "factor_z_raw", "factor_z_neu", "fwd_ret_2w"]).copy()
    df["factor_use"] = np.where(df["regime"] == "上涨", -df["factor_z_raw"], df["factor_z_neu"])
    lo = df.groupby("date")["factor_use"].transform(lambda s: s.quantile(trim_q))
    hi = df.groupby("date")["factor_use"].transform(lambda s: s.quantile(1 - trim_q))
    df = df[(df["factor_use"] >= lo) & (df["factor_use"] <= hi)].copy()
    dates = sorted(df["date"].unique().tolist())
    keep = []
    i = 0
    while i < len(dates):
        keep.append(dates[i])
        i += hold_step
    df = df[df["date"].isin(set(keep))].copy()
    rows = []
    for d, day in df.groupby("date"):
        n = len(day)
        if n < 5:
            continue
        day = day.copy()
        day["rank"] = day["factor_use"].rank(pct=True, method="first")
        mid = day[(day["rank"] > 0.3) & (day["rank"] < 0.7)].copy()
        bot = day[day["rank"] <= 0.3].copy()
        regime = str(day["regime"].iloc[0])
        if regime == "上涨":
            n_pool = max(1, int(round(0.5 * n)))
            pool = _select_top_with_industry_cap(day, n_target=n_pool, cap_ratio=0.2)
            n_pick = max(1, int(round(0.3 * n)))
            top = pool.sort_values("ret20d_stock", ascending=True).head(n_pick).copy()
        else:
            top = day[day["rank"] >= 0.7].copy()
        rows.append(
            {
                "date": d,
                "regime": regime,
                "Bottom30": float(bot["fwd_ret_2w"].mean()) if not bot.empty else np.nan,
                "Middle40": float(mid["fwd_ret_2w"].mean()) if not mid.empty else np.nan,
                "Top30": float(top["fwd_ret_2w"].mean()) if not top.empty else np.nan,
                "top_symbols": "|".join(top["stock_symbol"].astype(str).tolist()),
            }
        )
    out = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
    out = _apply_up_exposure(out, up_scale=0.5)
    return out


def _apply_costs(group_ret: pd.DataFrame, impact_cost: float) -> pd.DataFrame:
    buy_rate = 0.0002 + 0.00002 + impact_cost
    sell_rate = 0.0002 + 0.00002 + 0.001 + impact_cost
    x = group_ret.copy().sort_values("date").reset_index(drop=True)
    costs = []
    prev = set()
    for i, r in x.iterrows():
        cur = set(str(r["top_symbols"]).split("|")) if pd.notna(r["top_symbols"]) and str(r["top_symbols"]) else set()
        if i == 0:
            one_way_turnover = 1.0
        else:
            overlap = len(prev & cur)
            base_n = max(len(cur), 1)
            one_way_turnover = 1.0 - overlap / base_n
        cost = one_way_turnover * (buy_rate + sell_rate)
        costs.append(cost)
        prev = cur
    x["trade_cost_rate"] = costs
    x["Top30_net"] = x["Top30"] - x["trade_cost_rate"]
    return x


def _metrics(x: pd.DataFrame) -> dict:
    d = x.copy().sort_values("date")
    spread = d["Top30_net"] - d["Bottom30"]
    curve = (1 + spread.fillna(0)).cumprod()
    peak = curve.cummax()
    dd = curve / peak - 1.0
    mdd = float(dd.min()) if not dd.empty else float("nan")
    calmar = float(spread.mean()) / abs(mdd) if (not pd.isna(mdd) and mdd != 0) else float("nan")
    out = {
        "calmar_ratio": calmar,
        "max_drawdown_ls_curve": mdd,
        "hit_ratio_top_gt_bottom": float((d["Top30_net"] > d["Bottom30"]).mean()),
        "mean_top_minus_bottom": float(spread.mean()),
        "avg_trade_cost_rate": float(d["trade_cost_rate"].mean()),
    }
    for rg in ["上涨", "震荡", "下跌"]:
        s = d[d["regime"] == rg]
        out[f"{rg}_top_bottom"] = float((s["Top30_net"] - s["Bottom30"]).mean()) if not s.empty else float("nan")
    return out


def _capital_validation(x: pd.DataFrame, initial_capital: float = 100000.0) -> tuple[pd.DataFrame, dict]:
    d = x.copy().sort_values("date").reset_index(drop=True)
    d["spread_ret"] = d["Top30_net"] - d["Bottom30"]
    d["equity"] = initial_capital * (1 + d["spread_ret"].fillna(0)).cumprod()
    d["peak"] = d["equity"].cummax()
    d["drawdown"] = d["equity"] / d["peak"] - 1.0
    summary = {
        "initial_capital": float(initial_capital),
        "ending_capital": float(d["equity"].iloc[-1]) if not d.empty else float(initial_capital),
        "pnl": float((d["equity"].iloc[-1] - initial_capital)) if not d.empty else 0.0,
        "total_return": float((d["equity"].iloc[-1] / initial_capital - 1.0)) if not d.empty else 0.0,
        "max_drawdown": float(d["drawdown"].min()) if not d.empty else float("nan"),
    }
    return d, summary


def main():
    out_dir = os.path.join(ROOT, "research", "baseline_v5", "output")
    os.makedirs(out_dir, exist_ok=True)
    panel_v5 = _prepare_panel_v5()
    grp = _build_rebalance_rows(panel_v5, trim_q=0.05, hold_step=10)
    no_impact = _apply_costs(grp, impact_cost=0.0)
    with_impact = _apply_costs(grp, impact_cost=0.0005)
    m0 = _metrics(no_impact)
    m1 = _metrics(with_impact)
    curve0, cap0 = _capital_validation(no_impact, initial_capital=100000.0)
    curve1, cap1 = _capital_validation(with_impact, initial_capital=100000.0)
    pd.DataFrame(
        {"metric": list(m0.keys()), "no_impact": list(m0.values()), "with_impact": [m1[k] for k in m0.keys()]}
    ).to_csv(os.path.join(out_dir, "core_metrics_baseline_v5_costs_2019_2025.csv"), index=False, encoding="utf-8-sig")
    curve0[["date", "regime", "spread_ret", "equity", "drawdown"]].to_csv(
        os.path.join(out_dir, "equity_curve_100k_no_impact_2019_2025.csv"), index=False, encoding="utf-8-sig"
    )
    curve1[["date", "regime", "spread_ret", "equity", "drawdown"]].to_csv(
        os.path.join(out_dir, "equity_curve_100k_with_impact_2019_2025.csv"), index=False, encoding="utf-8-sig"
    )
    pd.DataFrame(
        {
            "metric": list(cap0.keys()),
            "no_impact": list(cap0.values()),
            "with_impact": [cap1[k] for k in cap0.keys()],
        }
    ).to_csv(os.path.join(out_dir, "capital_validation_100k_summary.csv"), index=False, encoding="utf-8-sig")
    with open(os.path.join(out_dir, "baseline_v5_cost_backtest_report.md"), "w", encoding="utf-8") as f:
        f.write("# baseline_v5 交易成本回测（2019-2025）\n\n")
        f.write("- 成本参数：佣金0.02%双边、印花税0.1%卖出、过户费0.002%双边。\n")
        f.write("- 可选冲击成本：0.05%双边（with_impact）。\n")
        f.write("- 资金口径验证：初始资金 100,000。\n")
        f.write(f"- no_impact: calmar={m0['calmar_ratio']:.6f}, mdd={m0['max_drawdown_ls_curve']:.6f}, hit={m0['hit_ratio_top_gt_bottom']:.4f}, excess={m0['mean_top_minus_bottom']:.6f}\n")
        f.write(f"- with_impact: calmar={m1['calmar_ratio']:.6f}, mdd={m1['max_drawdown_ls_curve']:.6f}, hit={m1['hit_ratio_top_gt_bottom']:.4f}, excess={m1['mean_top_minus_bottom']:.6f}\n")
        f.write(f"- no_impact_capital: ending={cap0['ending_capital']:.2f}, pnl={cap0['pnl']:.2f}, return={cap0['total_return']:.4%}\n")
        f.write(f"- with_impact_capital: ending={cap1['ending_capital']:.2f}, pnl={cap1['pnl']:.2f}, return={cap1['total_return']:.4%}\n")
    print(f"no_impact_calmar={m0['calmar_ratio']:.6f}")
    print(f"no_impact_mdd={m0['max_drawdown_ls_curve']:.6f}")
    print(f"with_impact_calmar={m1['calmar_ratio']:.6f}")
    print(f"with_impact_mdd={m1['max_drawdown_ls_curve']:.6f}")
    print(f"with_impact_ending_capital={cap1['ending_capital']:.2f}")


if __name__ == "__main__":
    main()
