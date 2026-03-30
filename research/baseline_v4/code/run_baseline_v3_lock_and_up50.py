import os
import sys

import baostock as bs
import numpy as np
import pandas as pd


def _zscore(s: pd.Series) -> pd.Series:
    std = s.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=s.index)
    return (s - s.mean()) / std


def _to_summary_dict(df: pd.DataFrame) -> dict:
    if df.empty:
        return {}
    return df.set_index("metric")["value"].to_dict()


def _calmar(summary_dict: dict) -> float:
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
        return out[out["industry_l2"] != "其他"].copy()
    base["log_mv"] = np.log1p(base["circ_mv_proxy"])
    cent = base.groupby("industry_l2", as_index=False).agg(log_mv=("log_mv", "median"), vol20=("vol20", "median"))
    cent = cent.replace([np.inf, -np.inf], np.nan).dropna(subset=["log_mv", "vol20"])
    if cent.empty:
        return out[out["industry_l2"] != "其他"].copy()
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
    return out[~other | have_proxy].copy()


def _industry_neutralize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["factor_ind_neu"] = out["factor_z"] - out.groupby(["date", "industry_l2"])["factor_z"].transform("mean")
    out["factor_z"] = out.groupby("date")["factor_ind_neu"].transform(_zscore)
    return out.drop(columns=["factor_ind_neu"])


def _apply_liq_dynamic(df: pd.DataFrame, regime_df: pd.DataFrame, keep_other: float, keep_up: float) -> pd.DataFrame:
    x = df.copy()
    x["date"] = pd.to_datetime(x["date"], errors="coerce").dt.normalize()
    x = x.merge(regime_df, on="date", how="left")
    x["regime"] = x["regime"].fillna("震荡")
    rank = x.groupby("date")["amount"].transform(lambda s: s.rank(pct=True, method="first"))
    keep_ratio = np.where(x["regime"] == "上涨", keep_up, keep_other)
    keep = rank >= (1 - keep_ratio)
    return x[keep.fillna(False)].drop(columns=["regime"]).copy()


def _summary_from_group_ret(group_ret: pd.DataFrame) -> pd.DataFrame:
    g = group_ret.copy().sort_values("date")
    ls = g["Top30"] - g["Bottom30"]
    curve = (1 + ls.fillna(0)).cumprod()
    peak = curve.cummax()
    dd = curve / peak - 1.0
    summary = pd.DataFrame(
        {
            "metric": [
                "mean_top",
                "mean_middle",
                "mean_bottom",
                "mean_top_minus_bottom",
                "hit_ratio_top_gt_bottom",
                "obs_days",
                "max_drawdown_ls_curve",
            ],
            "value": [
                float(g["Top30"].mean()),
                float(g["Middle40"].mean()),
                float(g["Bottom30"].mean()),
                float(ls.mean()),
                float((g["Top30"] > g["Bottom30"]).mean()),
                float(ls.dropna().shape[0]),
                float(dd.min()) if not dd.empty else float("nan"),
            ],
        }
    )
    return summary


def _apply_up_exposure(group_ret: pd.DataFrame, regime_df: pd.DataFrame, up_scale: float) -> pd.DataFrame:
    g = group_ret.copy()
    g["date"] = pd.to_datetime(g["date"], errors="coerce").dt.normalize()
    g = g.merge(regime_df, on="date", how="left")
    up = g["regime"] == "上涨"
    for c in ["Bottom30", "Middle40", "Top30"]:
        g.loc[up, c] = g.loc[up, c] * up_scale
    return g.drop(columns=["regime"]).copy()


def _regime_top_bottom(group_ret: pd.DataFrame, regime_df: pd.DataFrame) -> pd.DataFrame:
    g = group_ret.copy()
    g["date"] = pd.to_datetime(g["date"], errors="coerce").dt.normalize()
    x = g.merge(regime_df, on="date", how="left")
    x["regime"] = x["regime"].fillna("震荡")
    rows = []
    for rg in ["上涨", "震荡", "下跌"]:
        d = x[x["regime"] == rg]
        rows.append(
            {
                "regime": rg,
                "obs_days": int(len(d)),
                "top_bottom": float((d["Top30"] - d["Bottom30"]).mean()) if len(d) else float("nan"),
                "hit_ratio": float((d["Top30"] > d["Bottom30"]).mean()) if len(d) else float("nan"),
            }
        )
    return pd.DataFrame(rows)


def main():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    if root not in sys.path:
        sys.path.insert(0, root)
    from research.backtest.group_backtest_three_bucket import run_three_bucket_backtest
    from research.data_prep.build_rebalance_momentum_panel import build_rebalance_momentum_panel

    out_dir = os.path.join(root, "research", "baseline_v3", "output")
    os.makedirs(out_dir, exist_ok=True)
    panel = build_rebalance_momentum_panel(
        db_path=os.path.join(root, "data", "cubes.db"),
        cache_dir=os.path.join(root, "data", "market_cache"),
        out_csv=os.path.join(out_dir, "factor_panel_rebalance_momentum_2022_2025.csv"),
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

    panel_v3 = _apply_liq_dynamic(base, regime_df=regime_df, keep_other=0.6, keep_up=0.2)
    raw_v3 = run_three_bucket_backtest(panel_v3, horizon_col="fwd_ret_2w", hold_lock_days=10, trim_q=0.05)
    group_v3 = _apply_up_exposure(raw_v3["group_ret"], regime_df, up_scale=0.5)
    summary_v3 = _summary_from_group_ret(group_v3)
    regime_v3 = _regime_top_bottom(group_v3, regime_df)

    panel_up50 = _apply_liq_dynamic(base, regime_df=regime_df, keep_other=0.6, keep_up=0.2)
    raw_up50 = run_three_bucket_backtest(panel_up50, horizon_col="fwd_ret_2w", hold_lock_days=10, trim_q=0.05)
    group_up50 = _apply_up_exposure(raw_up50["group_ret"], regime_df, up_scale=0.5)
    summary_up50 = _summary_from_group_ret(group_up50)
    regime_up50 = _regime_top_bottom(group_up50, regime_df)

    group_v3.to_csv(os.path.join(out_dir, "group_ret_baseline_v3_2w_2022_2025.csv"), index=False, encoding="utf-8-sig")
    summary_v3.to_csv(os.path.join(out_dir, "summary_baseline_v3_2w_2022_2025.csv"), index=False, encoding="utf-8-sig")
    regime_v3.to_csv(os.path.join(out_dir, "regime_baseline_v3_2w_2022_2025.csv"), index=False, encoding="utf-8-sig")
    group_up50.to_csv(os.path.join(out_dir, "group_ret_baseline_v3_up50_2w_2022_2025.csv"), index=False, encoding="utf-8-sig")
    summary_up50.to_csv(os.path.join(out_dir, "summary_baseline_v3_up50_2w_2022_2025.csv"), index=False, encoding="utf-8-sig")
    regime_up50.to_csv(os.path.join(out_dir, "regime_baseline_v3_up50_2w_2022_2025.csv"), index=False, encoding="utf-8-sig")

    s3 = _to_summary_dict(summary_v3)
    su = _to_summary_dict(summary_up50)
    c3 = _calmar(s3)
    cu = _calmar(su)
    up_tb_v3 = float(regime_v3.loc[regime_v3["regime"] == "上涨", "top_bottom"].iloc[0])
    up_tb_up50 = float(regime_up50.loc[regime_up50["regime"] == "上涨", "top_bottom"].iloc[0])
    lines = []
    lines.append("# baseline_v3 与上涨市降仓50%对比（2022-2025）")
    lines.append("")
    lines.append("## baseline_v3")
    lines.append("")
    lines.append("- 配置：三项优化 + 行业中性 + 流动性阈值60% + 上涨市单票10% + 上涨市流动性前20%")
    lines.append(f"- hit_ratio: {float(s3.get('hit_ratio_top_gt_bottom', float('nan'))):.4f}")
    lines.append(f"- max_drawdown: {float(s3.get('max_drawdown_ls_curve', float('nan'))):.4f}")
    lines.append(f"- top-bottom: {float(s3.get('mean_top_minus_bottom', float('nan'))):.6f}")
    lines.append(f"- calmar: {c3:.6f}")
    lines.append(f"- 上涨市top-bottom: {up_tb_v3:.6f}")
    lines.append("")
    lines.append("## 上涨市降仓50%微优化")
    lines.append("")
    lines.append("- 配置：在 baseline_v3 基础上，仅上涨市整体仓位50%（其余不变）")
    lines.append(f"- hit_ratio: {float(su.get('hit_ratio_top_gt_bottom', float('nan'))):.4f}")
    lines.append(f"- max_drawdown: {float(su.get('max_drawdown_ls_curve', float('nan'))):.4f}")
    lines.append(f"- top-bottom: {float(su.get('mean_top_minus_bottom', float('nan'))):.6f}")
    lines.append(f"- calmar: {cu:.6f}")
    lines.append(f"- 上涨市top-bottom: {up_tb_up50:.6f}")
    lines.append("")
    lines.append("## 结论")
    lines.append("")
    lines.append(f"- 上涨市超额是否转正：{'是' if up_tb_up50 > 0 else '否'}")
    lines.append(f"- 整体Calmar是否进一步提升：{'是' if cu > c3 else '否'}")
    with open(os.path.join(out_dir, "baseline_v3_up50_experiment.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"calmar_v3={c3:.6f}")
    print(f"hit_v3={float(s3.get('hit_ratio_top_gt_bottom', float('nan'))):.4f}")
    print(f"mdd_v3={float(s3.get('max_drawdown_ls_curve', float('nan'))):.4f}")
    print(f"up_tb_v3={up_tb_v3:.6f}")
    print(f"calmar_up50={cu:.6f}")
    print(f"up_tb_up50={up_tb_up50:.6f}")


if __name__ == "__main__":
    main()
