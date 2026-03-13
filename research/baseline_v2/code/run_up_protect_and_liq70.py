import os
import sys

import baostock as bs
import numpy as np
import pandas as pd


def _to_summary_dict(res):
    if res["summary"].empty:
        return {}
    return res["summary"].set_index("metric")["value"].to_dict()


def _calmar(summary_dict):
    excess = float(summary_dict.get("mean_top_minus_bottom", float("nan")))
    mdd = float(summary_dict.get("max_drawdown_ls_curve", float("nan")))
    if pd.isna(excess) or pd.isna(mdd) or mdd == 0:
        return float("nan")
    return excess / abs(mdd)


def _load_hs300(start_date: str, end_date: str) -> pd.DataFrame:
    lg = bs.login()
    if str(lg.error_code) != "0":
        raise RuntimeError(f"baostock login failed: {lg.error_msg}")
    rs = bs.query_history_k_data_plus("sh.000300", "date,close", start_date, end_date, "d")
    if str(rs.error_code) != "0":
        bs.logout()
        raise RuntimeError(f"query_history_k_data_plus failed: {rs.error_msg}")
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    bs.logout()
    idx = pd.DataFrame(rows, columns=["date", "close"])
    idx["date"] = pd.to_datetime(idx["date"], errors="coerce").dt.normalize()
    idx["close"] = pd.to_numeric(idx["close"], errors="coerce")
    idx = idx.dropna(subset=["date", "close"]).sort_values("date")
    idx["ret20"] = idx["close"] / idx["close"].shift(20) - 1.0
    idx["regime"] = "震荡"
    idx.loc[idx["ret20"] > 0.02, "regime"] = "上涨"
    idx.loc[idx["ret20"] < -0.02, "regime"] = "下跌"
    return idx[["date", "regime"]]


def _summary_from_group_ret(group_ret: pd.DataFrame) -> dict:
    g = group_ret.copy().sort_values("date")
    ls = g["Top30"] - g["Bottom30"]
    curve = (1 + ls.fillna(0)).cumprod()
    peak = curve.cummax()
    dd = curve / peak - 1.0
    return {
        "hit_ratio_top_gt_bottom": float((g["Top30"] > g["Bottom30"]).mean()),
        "mean_top_minus_bottom": float(ls.mean()),
        "max_drawdown_ls_curve": float(dd.min()) if not dd.empty else float("nan"),
        "obs_days": int(ls.dropna().shape[0]),
    }


def _regime_top_bottom(group_ret: pd.DataFrame, regime_df: pd.DataFrame) -> pd.DataFrame:
    g = group_ret.copy()
    g["date"] = pd.to_datetime(g["date"], errors="coerce").dt.normalize()
    x = g.merge(regime_df, on="date", how="left")
    x["regime"] = x["regime"].fillna("震荡")
    rows = []
    for rg in ["上涨", "震荡", "下跌"]:
        d = x[x["regime"] == rg].copy()
        rows.append(
            {
                "regime": rg,
                "obs_days": int(len(d)),
                "top_bottom": float((d["Top30"] - d["Bottom30"]).mean()) if len(d) else float("nan"),
            }
        )
    return pd.DataFrame(rows)


def _apply_liquidity_filter_dynamic(panel: pd.DataFrame, regime_df: pd.DataFrame, keep_other: float, keep_up: float) -> pd.DataFrame:
    df = panel.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    x = df.merge(regime_df, on="date", how="left")
    x["regime"] = x["regime"].fillna("震荡")
    rank = x.groupby("date")["amount"].transform(lambda s: s.rank(pct=True, method="first"))
    keep_ratio = np.where(x["regime"] == "上涨", keep_up, keep_other)
    keep = rank >= (1 - keep_ratio)
    out = x[keep.fillna(False)].copy()
    return out.drop(columns=["regime"])


def _apply_up_exposure_on_group(group_ret: pd.DataFrame, regime_df: pd.DataFrame, up_scale: float = 0.5) -> pd.DataFrame:
    g = group_ret.copy()
    g["date"] = pd.to_datetime(g["date"], errors="coerce").dt.normalize()
    g = g.merge(regime_df, on="date", how="left")
    up = g["regime"] == "上涨"
    for c in ["Bottom30", "Middle40", "Top30"]:
        g.loc[up, c] = g.loc[up, c] * up_scale
    return g.drop(columns=["regime"])


def _assign_bucket(s: pd.Series) -> pd.Series:
    r = s.rank(pct=True, method="first")
    out = pd.Series(index=s.index, dtype=object)
    out[r <= 0.3] = "Bottom30"
    out[r >= 0.7] = "Top30"
    out[(r > 0.3) & (r < 0.7)] = "Middle40"
    return out


def _analyze_transactions(panel: pd.DataFrame, hold_lock_days: int = 10, trim_q: float = 0.05) -> dict:
    df = panel.dropna(subset=["date", "stock_symbol", "factor_z"]).copy()
    lo = df.groupby("date")["factor_z"].transform(lambda s: s.quantile(trim_q))
    hi = df.groupby("date")["factor_z"].transform(lambda s: s.quantile(1 - trim_q))
    df = df[(df["factor_z"] >= lo) & (df["factor_z"] <= hi)].copy()
    dates = sorted(df["date"].unique().tolist())
    rebalance_dates = []
    i = 0
    while i < len(dates):
        rebalance_dates.append(dates[i])
        i += hold_lock_days
    df_reb = df[df["date"].isin(rebalance_dates)].copy()
    df_reb["bucket"] = df_reb.groupby("date")["factor_z"].transform(_assign_bucket)
    holdings = []
    for d in rebalance_dates:
        day_data = df_reb[df_reb["date"] == d]
        holdings.append(set(day_data[day_data["bucket"] == "Top30"]["stock_symbol"].tolist()))
    total_buys = len(holdings[0]) if holdings else 0
    total_sells = 0
    turnover_counts = []
    for j in range(1, len(holdings)):
        prev = holdings[j - 1]
        curr = holdings[j]
        buys = len(curr - prev)
        sells = len(prev - curr)
        total_buys += buys
        total_sells += sells
        turnover_counts.append(buys + sells)
    return {
        "total_transactions": int(total_buys + total_sells),
        "avg_turnover_per_rebalance": float(np.mean(turnover_counts)) if turnover_counts else 0.0,
    }


def main():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    if root not in sys.path:
        sys.path.insert(0, root)
    from research.backtest.group_backtest_three_bucket import run_three_bucket_backtest, save_three_bucket_results
    from research.data_prep.build_rebalance_momentum_panel import build_rebalance_momentum_panel
    from research.run_rebalance_momentum_grid_and_indopt import _attach_base_fields, _assign_other_industry_by_proxy, _industry_neutralize, apply_liquidity_filter

    out_dir = os.path.join(root, "research", "baseline_v2", "output")
    os.makedirs(out_dir, exist_ok=True)
    panel = build_rebalance_momentum_panel(
        db_path=os.path.join(root, "data", "cubes.db"),
        cache_dir=os.path.join(root, "data", "market_cache"),
        out_csv=os.path.join(out_dir, "temp_panel_up_protect.csv"),
        start_date="2022-01-01",
        end_date="2025-12-31",
        lag_days=14,
        smoothing_days=3,
        factor_mode="rate",
    )
    base = _attach_base_fields(
        panel,
        industry_map_csv=os.path.join(root, "research", "baseline_v1", "data_delivery", "industry_mapping_v2.csv"),
        liquidity_csv=os.path.join(root, "research", "baseline_v1", "data_delivery", "liquidity_daily_v1.csv"),
    )
    base = _assign_other_industry_by_proxy(base)
    base = _industry_neutralize(base)
    regime_df = _load_hs300("2022-01-01", "2025-12-31")

    panel_v2 = apply_liquidity_filter(base, quantile_keep=0.6)
    res_v2 = run_three_bucket_backtest(panel_v2, horizon_col="fwd_ret_2w", hold_lock_days=10, trim_q=0.05)
    save_three_bucket_results(res_v2, out_dir, "baseline_v2_liq60_2w_2022_2025")
    s_v2 = _to_summary_dict(res_v2)
    c_v2 = _calmar(s_v2)
    regime_v2 = _regime_top_bottom(res_v2["group_ret"], regime_df)

    panel_up = _apply_liquidity_filter_dynamic(base, regime_df=regime_df, keep_other=0.6, keep_up=0.2)
    res_up_raw = run_three_bucket_backtest(panel_up, horizon_col="fwd_ret_2w", hold_lock_days=10, trim_q=0.05)
    group_up_scaled = _apply_up_exposure_on_group(res_up_raw["group_ret"], regime_df, up_scale=0.5)
    s_up = _summary_from_group_ret(group_up_scaled)
    c_up = _calmar(s_up)
    regime_up = _regime_top_bottom(group_up_scaled, regime_df)
    group_up_scaled.to_csv(
        os.path.join(out_dir, "group_ret_baseline_v2_up_protect_2w_2022_2025.csv"),
        index=False,
        encoding="utf-8-sig",
    )

    panel_70 = apply_liquidity_filter(base, quantile_keep=0.7)
    res_70 = run_three_bucket_backtest(panel_70, horizon_col="fwd_ret_2w", hold_lock_days=10, trim_q=0.05)
    save_three_bucket_results(res_70, out_dir, "baseline_v2_1_liq70_2w_2022_2025")
    s_70 = _to_summary_dict(res_70)
    c_70 = _calmar(s_70)
    tx_v2 = _analyze_transactions(panel_v2, hold_lock_days=10, trim_q=0.05)
    tx_70 = _analyze_transactions(panel_70, hold_lock_days=10, trim_q=0.05)

    up_base = float(regime_v2.loc[regime_v2["regime"] == "上涨", "top_bottom"].iloc[0])
    up_new = float(regime_up.loc[regime_up["regime"] == "上涨", "top_bottom"].iloc[0])
    md = []
    md.append("# 上涨环境保护版与流动性70%探索（2022-2025）")
    md.append("")
    md.append("## 实验A：上涨环境保护版")
    md.append("")
    md.append("- 调整内容：上涨市单票权重20%→10%（收益缩放0.5）；上涨市流动性改为仅保留前20%最活跃股票；其余环境保持baseline_v2。")
    md.append(f"- 上涨市 top-bottom（baseline_v2）：{up_base:.6f}")
    md.append(f"- 上涨市 top-bottom（保护版）：{up_new:.6f}")
    md.append(f"- 是否从负值拉回正：{'是' if up_new > 0 else '否'}")
    md.append(f"- 整体 Calmar（baseline_v2）：{c_v2:.6f}")
    md.append(f"- 整体 Calmar（保护版）：{c_up:.6f}")
    md.append("")
    md.append("## 实验B：流动性阈值70%探索")
    md.append("")
    md.append(f"- 总交易次数 baseline_v2(liq60)：{tx_v2['total_transactions']}")
    md.append(f"- 总交易次数 liq70：{tx_70['total_transactions']}")
    md.append(f"- hit_ratio baseline_v2(liq60)：{float(s_v2.get('hit_ratio_top_gt_bottom', float('nan'))):.4f}")
    md.append(f"- hit_ratio liq70：{float(s_70.get('hit_ratio_top_gt_bottom', float('nan'))):.4f}")
    md.append(f"- max_drawdown baseline_v2(liq60)：{float(s_v2.get('max_drawdown_ls_curve', float('nan'))):.4f}")
    md.append(f"- max_drawdown liq70：{float(s_70.get('max_drawdown_ls_curve', float('nan'))):.4f}")
    md.append(f"- calmar baseline_v2(liq60)：{c_v2:.6f}")
    md.append(f"- calmar liq70：{c_70:.6f}")
    md.append("")
    md.append("## 结论")
    md.append("")
    md.append("- baseline_v2.1 建议条件：hit_ratio接近0.6、max_drawdown>-0.12、calmar变化可接受且可选标的更广。")
    with open(os.path.join(out_dir, "baseline_v2_up_protect_and_liq70_report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(md))

    print(f"up_top_bottom_base={up_base:.6f}")
    print(f"up_top_bottom_protect={up_new:.6f}")
    print(f"calmar_base={c_v2:.6f}")
    print(f"calmar_up_protect={c_up:.6f}")
    print(f"tx_base={tx_v2['total_transactions']}")
    print(f"tx_liq70={tx_70['total_transactions']}")
    print(f"report={os.path.join(out_dir, 'baseline_v2_up_protect_and_liq70_report.md')}")


if __name__ == "__main__":
    main()
