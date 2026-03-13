import os
import sys

import numpy as np
import pandas as pd


def _to_summary_dict(res):
    if res["summary"].empty:
        return {}
    return res["summary"].set_index("metric")["value"].to_dict()


def _zscore(s: pd.Series) -> pd.Series:
    std = s.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=s.index)
    return (s - s.mean()) / std


def apply_industry_neutral(panel: pd.DataFrame, industry_map_csv: str) -> pd.DataFrame:
    ind = pd.read_csv(industry_map_csv, usecols=["stock_symbol_standard", "industry_l1", "industry_l2"])
    ind["stock_symbol_standard"] = ind["stock_symbol_standard"].astype(str).str.upper()
    ind = ind.sort_values(["stock_symbol_standard"]).drop_duplicates("stock_symbol_standard", keep="first")
    df = panel.copy()
    df["stock_symbol"] = df["stock_symbol"].astype(str).str.upper()
    df = df.merge(ind, left_on="stock_symbol", right_on="stock_symbol_standard", how="left")
    df["industry_l2"] = df["industry_l2"].fillna("其他")
    df["factor_ind_neu"] = df["factor_z"] - df.groupby(["date", "industry_l2"])["factor_z"].transform("mean")
    df["factor_z"] = df.groupby("date")["factor_ind_neu"].transform(_zscore)
    return df.drop(columns=["stock_symbol_standard", "factor_ind_neu"])


def apply_liquidity_filter(panel: pd.DataFrame, liquidity_csv: str, quantile_keep: float = 0.5) -> pd.DataFrame:
    liq = pd.read_csv(liquidity_csv, usecols=["date", "stock_symbol", "amount", "turnover_rate"])
    liq["date"] = pd.to_datetime(liq["date"], errors="coerce").dt.normalize()
    liq["stock_symbol"] = liq["stock_symbol"].astype(str).str.upper()
    liq["amount"] = pd.to_numeric(liq["amount"], errors="coerce")
    liq["turnover_rate"] = pd.to_numeric(liq["turnover_rate"], errors="coerce")
    liq = liq.dropna(subset=["date", "stock_symbol"])
    liq["circ_mv_proxy"] = np.where(liq["turnover_rate"] > 0, liq["amount"] / (liq["turnover_rate"] / 100.0), np.nan)
    df = panel.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df["stock_symbol"] = df["stock_symbol"].astype(str).str.upper()
    df = df.merge(liq, on=["date", "stock_symbol"], how="left")
    df["year"] = df["date"].dt.year
    df["is_main_board"] = df["stock_symbol"].str.startswith(("SH60", "SZ00"))
    mask_recent = df["year"] >= 2022
    amount_rank = df[mask_recent].groupby("date")["amount"].transform(lambda s: s.rank(pct=True, method="first"))
    keep_recent = amount_rank >= (1 - quantile_keep)
    mask_old = ~mask_recent
    mv_rank = df[mask_old].groupby("date")["circ_mv_proxy"].transform(lambda s: s.rank(pct=True, method="first"))
    keep_old_mv = mv_rank >= (1 - quantile_keep)
    keep_old_board = df[mask_old]["is_main_board"] & df[mask_old]["circ_mv_proxy"].isna()
    keep = pd.Series(False, index=df.index)
    keep.loc[mask_recent] = keep_recent.fillna(False)
    keep.loc[mask_old] = (keep_old_mv.fillna(False) | keep_old_board.fillna(False))
    out = df[keep].copy()
    return out.drop(columns=["year", "is_main_board"])


def _calmar(summary_dict):
    excess = float(summary_dict.get("mean_top_minus_bottom", float("nan")))
    mdd = float(summary_dict.get("max_drawdown_ls_curve", float("nan")))
    if pd.isna(excess) or pd.isna(mdd) or mdd == 0:
        return float("nan")
    return excess / abs(mdd)


def save_compare_md(base_res, liq_res, out_md, start_date, end_date):
    b = _to_summary_dict(base_res)
    l = _to_summary_dict(liq_res)
    b_calmar = _calmar(b)
    l_calmar = _calmar(l)
    lines = []
    lines.append("# 行业中性与流动性过滤回测对比（2022-2025）")
    lines.append("")
    lines.append(f"- 样本区间：{start_date} ~ {end_date}")
    lines.append("- 固定设置：三项优化（信号平滑+2w持有+极值剔除）")
    lines.append("- 行业处理：使用 industry_mapping_v2 做行业中性（含其他行业）")
    lines.append("- 流动性过滤：2022-2025 按 amount 保留前50%")
    lines.append("")
    lines.append("## 指标对比")
    lines.append("")
    lines.append(f"- 行业中性_only hit_ratio: {b.get('hit_ratio_top_gt_bottom', float('nan')):.4f}")
    lines.append(f"- 行业中性_only max_drawdown: {b.get('max_drawdown_ls_curve', float('nan')):.4f}")
    lines.append(f"- 行业中性_only top-bottom: {b.get('mean_top_minus_bottom', float('nan')):.6f}")
    lines.append(f"- 行业中性_only calmar: {b_calmar:.6f}")
    lines.append(f"- 行业中性+流动性 hit_ratio: {l.get('hit_ratio_top_gt_bottom', float('nan')):.4f}")
    lines.append(f"- 行业中性+流动性 max_drawdown: {l.get('max_drawdown_ls_curve', float('nan')):.4f}")
    lines.append(f"- 行业中性+流动性 top-bottom: {l.get('mean_top_minus_bottom', float('nan')):.6f}")
    lines.append(f"- 行业中性+流动性 calmar: {l_calmar:.6f}")
    lines.append("")
    lines.append("## 结论")
    lines.append("")
    mdd_improve = float(l.get("max_drawdown_ls_curve", -1)) > float(b.get("max_drawdown_ls_curve", -1))
    lines.append(f"- max_drawdown 是否进一步降低：{'是' if mdd_improve else '否'}")
    lines.append("- 实盘可操作性：流动性过滤后小额成交标的已被剔除，可操作性提升。")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if root not in sys.path:
        sys.path.insert(0, root)
    from research.backtest.group_backtest_three_bucket import run_three_bucket_backtest, save_three_bucket_results
    from research.data_prep.build_rebalance_momentum_panel import build_rebalance_momentum_panel
    from research.plots.plot_three_bucket import plot_three_bucket_curves

    db_path = os.path.join(root, "data", "cubes.db")
    cache_dir = os.path.join(root, "data", "market_cache")
    out_dir = os.path.join(root, "research", "output")
    start_date = "2022-01-01"
    end_date = "2025-12-31"
    panel_csv = os.path.join(out_dir, "factor_panel_rebalance_momentum_2022_2025.csv")
    industry_csv = os.path.join(root, "research", "baseline_v1", "data_delivery", "industry_mapping_v2.csv")
    liquidity_csv = os.path.join(root, "research", "baseline_v1", "data_delivery", "liquidity_daily_v1.csv")
    os.makedirs(out_dir, exist_ok=True)
    panel = build_rebalance_momentum_panel(
        db_path=db_path,
        cache_dir=cache_dir,
        out_csv=panel_csv,
        start_date=start_date,
        end_date=end_date,
        lag_days=14,
        smoothing_days=3,
        factor_mode="rate",
    )
    panel_neu = apply_industry_neutral(panel, industry_map_csv=industry_csv)
    res_neu = run_three_bucket_backtest(panel_neu, horizon_col="fwd_ret_2w", hold_lock_days=10, trim_q=0.05)
    save_three_bucket_results(res_neu, out_dir, "rebalance_momentum_indneutral_2w_2022_2025")
    plot_three_bucket_curves(
        res_neu["group_ret"],
        res_neu["ls_curve"],
        os.path.join(out_dir, "group_curve_rebalance_momentum_indneutral_2w_2022_2025.png"),
        "Rebalance Momentum Industry Neutral - 2w (2022-2025)",
    )
    panel_neu_liq = apply_liquidity_filter(panel_neu, liquidity_csv=liquidity_csv, quantile_keep=0.5)
    res_neu_liq = run_three_bucket_backtest(panel_neu_liq, horizon_col="fwd_ret_2w", hold_lock_days=10, trim_q=0.05)
    save_three_bucket_results(res_neu_liq, out_dir, "rebalance_momentum_indneutral_liq50_2w_2022_2025")
    plot_three_bucket_curves(
        res_neu_liq["group_ret"],
        res_neu_liq["ls_curve"],
        os.path.join(out_dir, "group_curve_rebalance_momentum_indneutral_liq50_2w_2022_2025.png"),
        "Rebalance Momentum Industry Neutral + Liquidity50 - 2w (2022-2025)",
    )
    compare_md = os.path.join(out_dir, "factor_rebalance_momentum_indneutral_liq_compare_2022_2025.md")
    save_compare_md(res_neu, res_neu_liq, compare_md, start_date, end_date)
    print(f"panel_rows={len(panel)}")
    print(f"panel_neu_rows={len(panel_neu)}")
    print(f"panel_neu_liq_rows={len(panel_neu_liq)}")
    print(f"compare={compare_md}")


if __name__ == "__main__":
    main()
