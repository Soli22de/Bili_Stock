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
    ind = pd.read_csv(industry_map_csv, usecols=["stock_symbol_standard", "industry_l2"])
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


def _industry_neutralize(df: pd.DataFrame, source_col: str = "factor_z", out_col: str = "factor_z_neu") -> pd.DataFrame:
    out = df.copy()
    tmp = out[source_col] - out.groupby(["date", "industry_l2"])[source_col].transform("mean")
    out[out_col] = tmp.groupby(out["date"]).transform(_zscore)
    return out


def _apply_liq_dynamic(df: pd.DataFrame, regime_df: pd.DataFrame, keep_other: float = 0.6, keep_up: float = 0.2) -> pd.DataFrame:
    x = df.copy()
    x = x.merge(regime_df, on="date", how="left")
    x["regime"] = x["regime"].fillna("震荡")
    rank = x.groupby("date")["amount"].transform(lambda s: s.rank(pct=True, method="first"))
    keep_ratio = np.where(x["regime"] == "上涨", keep_up, keep_other)
    keep = rank >= (1 - keep_ratio)
    return x[keep.fillna(False)].copy()


def _select_top_with_industry_cap(day: pd.DataFrame, n_target: int, cap_ratio: float = 0.2) -> pd.DataFrame:
    if day.empty or n_target <= 0:
        return day.iloc[0:0].copy()
    cap_n = max(1, int(np.floor(n_target * cap_ratio)))
    cand = day.sort_values("factor_use", ascending=False).copy()
    picked_idx = []
    cnt = {}
    for idx, row in cand.iterrows():
        ind = row["industry_l2"]
        cur = cnt.get(ind, 0)
        if cur < cap_n:
            picked_idx.append(idx)
            cnt[ind] = cur + 1
        if len(picked_idx) >= n_target:
            break
    if len(picked_idx) < n_target:
        for idx, _ in cand.iterrows():
            if idx in picked_idx:
                continue
            picked_idx.append(idx)
            if len(picked_idx) >= n_target:
                break
    return day.loc[picked_idx].copy()


def _build_group_ret_baseline_v4(panel: pd.DataFrame, trim_q: float = 0.05, hold_step: int = 10) -> pd.DataFrame:
    df = panel.dropna(subset=["date", "stock_symbol", "factor_z_raw", "factor_z_neu", "fwd_ret_2w"]).copy()
    df["factor_use"] = np.where(df["regime"] == "上涨", -df["factor_z_raw"], df["factor_z_neu"])
    lo = df.groupby("date")["factor_use"].transform(lambda s: s.quantile(trim_q))
    hi = df.groupby("date")["factor_use"].transform(lambda s: s.quantile(1 - trim_q))
    df = df[(df["factor_use"] >= lo) & (df["factor_use"] <= hi)].copy()
    dates = sorted(df["date"].unique().tolist())
    keep_dates = []
    i = 0
    while i < len(dates):
        keep_dates.append(dates[i])
        i += hold_step
    df = df[df["date"].isin(set(keep_dates))].copy()
    rows = []
    for d, day in df.groupby("date"):
        n = len(day)
        if n < 5:
            continue
        r = day["factor_use"].rank(pct=True, method="first")
        day = day.assign(rank=r)
        top = day[day["rank"] >= 0.7].copy()
        mid = day[(day["rank"] > 0.3) & (day["rank"] < 0.7)].copy()
        bot = day[day["rank"] <= 0.3].copy()
        regime = str(day["regime"].iloc[0])
        if regime == "上涨":
            n_target = max(1, int(round(0.3 * n)))
            top = _select_top_with_industry_cap(day, n_target=n_target, cap_ratio=0.2)
        rows.append(
            {
                "date": d,
                "Bottom30": float(bot["fwd_ret_2w"].mean()) if not bot.empty else np.nan,
                "Middle40": float(mid["fwd_ret_2w"].mean()) if not mid.empty else np.nan,
                "Top30": float(top["fwd_ret_2w"].mean()) if not top.empty else np.nan,
                "regime": regime,
            }
        )
    out = pd.DataFrame(rows).sort_values("date")
    return out


def _apply_up_exposure(group_ret: pd.DataFrame, up_scale: float = 0.5) -> pd.DataFrame:
    g = group_ret.copy()
    up = g["regime"] == "上涨"
    for c in ["Bottom30", "Middle40", "Top30"]:
        g.loc[up, c] = g.loc[up, c] * up_scale
    return g


def _summary(group_ret: pd.DataFrame) -> pd.DataFrame:
    g = group_ret.sort_values("date").copy()
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
                "calmar_ratio",
            ],
            "value": [
                float(g["Top30"].mean()),
                float(g["Middle40"].mean()),
                float(g["Bottom30"].mean()),
                float(ls.mean()),
                float((g["Top30"] > g["Bottom30"]).mean()),
                float(ls.dropna().shape[0]),
                float(dd.min()) if not dd.empty else float("nan"),
                float(ls.mean()) / abs(float(dd.min())) if (not dd.empty and float(dd.min()) != 0) else float("nan"),
            ],
        }
    )
    return summary


def _regime_metrics(group_ret: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for rg in ["上涨", "震荡", "下跌"]:
        d = group_ret[group_ret["regime"] == rg].copy()
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
    from research.data_prep.build_rebalance_momentum_panel import build_rebalance_momentum_panel

    out_dir = os.path.join(root, "research", "baseline_v4", "output")
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
    base["factor_z_raw"] = base["factor_z"]
    base = _industry_neutralize(base, source_col="factor_z_raw", out_col="factor_z_neu")
    regime = _load_hs300("2022-01-01", "2025-12-31")
    panel_liq = _apply_liq_dynamic(base, regime_df=regime, keep_other=0.6, keep_up=0.2)
    group_ret = _build_group_ret_baseline_v4(panel_liq, trim_q=0.05, hold_step=10)
    group_ret = _apply_up_exposure(group_ret, up_scale=0.5)
    summary = _summary(group_ret)
    regime_m = _regime_metrics(group_ret)
    group_ret[["date", "Bottom30", "Middle40", "Top30"]].to_csv(
        os.path.join(out_dir, "group_ret_baseline_v4_2w_2022_2025.csv"), index=False, encoding="utf-8-sig"
    )
    summary.to_csv(os.path.join(out_dir, "summary_baseline_v4_2w_2022_2025.csv"), index=False, encoding="utf-8-sig")
    regime_m.to_csv(os.path.join(out_dir, "regime_baseline_v4_2w_2022_2025.csv"), index=False, encoding="utf-8-sig")
    lines = []
    s = summary.set_index("metric")["value"].to_dict()
    up_tb = float(regime_m.loc[regime_m["regime"] == "上涨", "top_bottom"].iloc[0])
    lines.append("# baseline_v4 回测摘要（2022-2025）")
    lines.append("")
    lines.append("- 配置：上涨市反向因子 + 宽松行业中性（行业上限20%）+ 流动性前20% + 单票10%；非上涨市保留baseline_v3。")
    lines.append(f"- hit_ratio: {float(s.get('hit_ratio_top_gt_bottom', float('nan'))):.4f}")
    lines.append(f"- max_drawdown: {float(s.get('max_drawdown_ls_curve', float('nan'))):.4f}")
    lines.append(f"- top-bottom: {float(s.get('mean_top_minus_bottom', float('nan'))):.6f}")
    lines.append(f"- calmar: {float(s.get('calmar_ratio', float('nan'))):.6f}")
    lines.append(f"- 上涨市 top-bottom: {up_tb:.6f}")
    with open(os.path.join(out_dir, "baseline_v4_backtest_report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(os.path.join(out_dir, "summary_baseline_v4_2w_2022_2025.csv"))
    print(os.path.join(out_dir, "regime_baseline_v4_2w_2022_2025.csv"))


if __name__ == "__main__":
    main()
