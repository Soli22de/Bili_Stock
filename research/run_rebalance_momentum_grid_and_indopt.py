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


def _calmar(summary_dict):
    excess = float(summary_dict.get("mean_top_minus_bottom", float("nan")))
    mdd = float(summary_dict.get("max_drawdown_ls_curve", float("nan")))
    if pd.isna(excess) or pd.isna(mdd) or mdd == 0:
        return float("nan")
    return excess / abs(mdd)


def _load_liquidity(liquidity_csv: str) -> pd.DataFrame:
    liq = pd.read_csv(liquidity_csv, usecols=["date", "stock_symbol", "amount", "turnover_rate"])
    liq["date"] = pd.to_datetime(liq["date"], errors="coerce").dt.normalize()
    liq["stock_symbol"] = liq["stock_symbol"].astype(str).str.upper()
    liq["amount"] = pd.to_numeric(liq["amount"], errors="coerce")
    liq["turnover_rate"] = pd.to_numeric(liq["turnover_rate"], errors="coerce")
    liq = liq.dropna(subset=["date", "stock_symbol"])
    liq["circ_mv_proxy"] = np.where(liq["turnover_rate"] > 0, liq["amount"] / (liq["turnover_rate"] / 100.0), np.nan)
    return liq


def _load_industry(industry_map_csv: str) -> pd.DataFrame:
    ind = pd.read_csv(industry_map_csv, usecols=["stock_symbol_standard", "industry_l1", "industry_l2"])
    ind["stock_symbol_standard"] = ind["stock_symbol_standard"].astype(str).str.upper()
    ind = ind.sort_values(["stock_symbol_standard"]).drop_duplicates("stock_symbol_standard", keep="first")
    return ind


def _attach_base_fields(panel: pd.DataFrame, industry_map_csv: str, liquidity_csv: str) -> pd.DataFrame:
    ind = _load_industry(industry_map_csv)
    liq = _load_liquidity(liquidity_csv)
    df = panel.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df["stock_symbol"] = df["stock_symbol"].astype(str).str.upper()
    df = df.merge(ind, left_on="stock_symbol", right_on="stock_symbol_standard", how="left")
    df["industry_l2"] = df["industry_l2"].fillna("其他")
    df = df.merge(liq[["date", "stock_symbol", "amount", "turnover_rate", "circ_mv_proxy"]], on=["date", "stock_symbol"], how="left")
    ret = df.groupby("stock_symbol")["close"].pct_change()
    df["vol20"] = ret.groupby(df["stock_symbol"]).transform(lambda s: s.rolling(20, min_periods=10).std())
    return df


def _assign_other_industry_by_proxy(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    base = out[(out["industry_l2"] != "其他") & out["circ_mv_proxy"].notna() & out["vol20"].notna()].copy()
    if base.empty:
        out = out[out["industry_l2"] != "其他"].copy()
        return out
    base["log_mv"] = np.log1p(base["circ_mv_proxy"])
    cent = base.groupby("industry_l2", as_index=False).agg(log_mv=("log_mv", "median"), vol20=("vol20", "median"))
    cent = cent.replace([np.inf, -np.inf], np.nan).dropna(subset=["log_mv", "vol20"])
    if cent.empty:
        out = out[out["industry_l2"] != "其他"].copy()
        return out
    other = out["industry_l2"] == "其他"
    have_proxy = other & out["circ_mv_proxy"].notna() & out["vol20"].notna()
    if have_proxy.any():
        mv = np.log1p(out.loc[have_proxy, "circ_mv_proxy"].to_numpy())
        vol = out.loc[have_proxy, "vol20"].to_numpy()
        pts = np.column_stack([mv, vol])
        cts = cent[["log_mv", "vol20"]].to_numpy()
        dist = ((pts[:, None, :] - cts[None, :, :]) ** 2).sum(axis=2)
        idx = dist.argmin(axis=1)
        out.loc[have_proxy, "industry_l2"] = cent["industry_l2"].to_numpy()[idx]
    out = out[~other | have_proxy].copy()
    return out


def _industry_neutralize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["factor_ind_neu"] = out["factor_z"] - out.groupby(["date", "industry_l2"])["factor_z"].transform("mean")
    out["factor_z"] = out.groupby("date")["factor_ind_neu"].transform(_zscore)
    return out.drop(columns=["factor_ind_neu"])


def apply_liquidity_filter(panel: pd.DataFrame, quantile_keep: float = 0.5) -> pd.DataFrame:
    df = panel.copy()
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


def _pick_baseline_v2(grid_rows: list) -> dict:
    ok = [r for r in grid_rows if float(r["hit_ratio"]) >= 0.6 and pd.notna(r["calmar"])]
    if ok:
        return sorted(ok, key=lambda x: x["calmar"], reverse=True)[0]
    all_valid = [r for r in grid_rows if pd.notna(r["calmar"])]
    if not all_valid:
        return grid_rows[0]
    return sorted(all_valid, key=lambda x: x["calmar"], reverse=True)[0]


def save_markdown(grid_rows: list, best_row: dict, indopt_row: dict, out_md: str):
    lines = []
    lines.append("# 流动性阈值网格搜索与行业中性微优化（2022-2025）")
    lines.append("")
    lines.append("- 固定设置：三项优化 + 行业中性")
    lines.append("- 网格参数：流动性阈值 40% / 50% / 60%（保留每期成交额前N%）")
    lines.append("")
    lines.append("## 网格搜索结果")
    lines.append("")
    lines.append("| 阈值 | hit_ratio | max_drawdown | top-bottom | calmar |")
    lines.append("|---:|---:|---:|---:|---:|")
    for r in grid_rows:
        lines.append(
            f"| {int(r['threshold']*100)}% | {r['hit_ratio']:.4f} | {r['max_drawdown']:.4f} | {r['top_bottom']:.6f} | {r['calmar']:.6f} |"
        )
    lines.append("")
    lines.append("## baseline_v2 推荐")
    lines.append("")
    lines.append(
        f"- 推荐阈值：**{int(best_row['threshold']*100)}%**（在 hit_ratio>=0.6 条件下，Calmar 最高）"
    )
    lines.append(f"- 推荐版本指标：hit_ratio={best_row['hit_ratio']:.4f}, calmar={best_row['calmar']:.6f}, max_drawdown={best_row['max_drawdown']:.4f}")
    lines.append("")
    lines.append("## 行业中性微优化（并行）")
    lines.append("")
    lines.append(f"- 优化方式：对“其他行业”先按市值代理+20日波动率映射到已知行业；无法映射则剔除。")
    lines.append(f"- 使用阈值：{int(best_row['threshold']*100)}%")
    lines.append(
        f"- 优化后指标：hit_ratio={indopt_row['hit_ratio']:.4f}, calmar={indopt_row['calmar']:.6f}, max_drawdown={indopt_row['max_drawdown']:.4f}"
    )
    delta = indopt_row["calmar"] - best_row["calmar"]
    lines.append(f"- Calmar 变化：{delta:+.6f}")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if root not in sys.path:
        sys.path.insert(0, root)
    from research.backtest.group_backtest_three_bucket import run_three_bucket_backtest, save_three_bucket_results
    from research.data_prep.build_rebalance_momentum_panel import build_rebalance_momentum_panel

    db_path = os.path.join(root, "data", "cubes.db")
    cache_dir = os.path.join(root, "data", "market_cache")
    out_dir = os.path.join(root, "research", "output")
    industry_csv = os.path.join(root, "research", "baseline_v1", "data_delivery", "industry_mapping_v2.csv")
    liquidity_csv = os.path.join(root, "research", "baseline_v1", "data_delivery", "liquidity_daily_v1.csv")
    os.makedirs(out_dir, exist_ok=True)
    panel = build_rebalance_momentum_panel(
        db_path=db_path,
        cache_dir=cache_dir,
        out_csv=os.path.join(out_dir, "factor_panel_rebalance_momentum_2022_2025.csv"),
        start_date="2022-01-01",
        end_date="2025-12-31",
        lag_days=14,
        smoothing_days=3,
        factor_mode="rate",
    )
    base = _attach_base_fields(panel, industry_map_csv=industry_csv, liquidity_csv=liquidity_csv)
    panel_neu_plain = _industry_neutralize(base)
    grid_rows = []
    for q in [0.4, 0.5, 0.6]:
        p = apply_liquidity_filter(panel_neu_plain, quantile_keep=q)
        r = run_three_bucket_backtest(p, horizon_col="fwd_ret_2w", hold_lock_days=10, trim_q=0.05)
        s = _to_summary_dict(r)
        row = {
            "threshold": q,
            "hit_ratio": float(s.get("hit_ratio_top_gt_bottom", float("nan"))),
            "max_drawdown": float(s.get("max_drawdown_ls_curve", float("nan"))),
            "top_bottom": float(s.get("mean_top_minus_bottom", float("nan"))),
            "calmar": _calmar(s),
        }
        grid_rows.append(row)
        save_three_bucket_results(r, out_dir, f"rebalance_momentum_indneutral_liq{int(q*100)}_2w_2022_2025")
    best = _pick_baseline_v2(grid_rows)
    panel_neu_opt = _assign_other_industry_by_proxy(base)
    panel_neu_opt = _industry_neutralize(panel_neu_opt)
    panel_neu_opt_best = apply_liquidity_filter(panel_neu_opt, quantile_keep=float(best["threshold"]))
    r_opt = run_three_bucket_backtest(panel_neu_opt_best, horizon_col="fwd_ret_2w", hold_lock_days=10, trim_q=0.05)
    s_opt = _to_summary_dict(r_opt)
    indopt = {
        "threshold": float(best["threshold"]),
        "hit_ratio": float(s_opt.get("hit_ratio_top_gt_bottom", float("nan"))),
        "max_drawdown": float(s_opt.get("max_drawdown_ls_curve", float("nan"))),
        "top_bottom": float(s_opt.get("mean_top_minus_bottom", float("nan"))),
        "calmar": _calmar(s_opt),
    }
    save_three_bucket_results(
        r_opt, out_dir, f"rebalance_momentum_indneutral_opt_liq{int(best['threshold']*100)}_2w_2022_2025"
    )
    md = os.path.join(out_dir, "factor_rebalance_momentum_liq_grid_indopt_2022_2025.md")
    save_markdown(grid_rows, best, indopt, md)
    print(f"panel_rows={len(panel)}")
    print(f"grid={md}")
    print(f"best_threshold={best['threshold']}")
    print(f"best_calmar={best['calmar']}")
    print(f"indopt_calmar={indopt['calmar']}")


if __name__ == "__main__":
    main()
