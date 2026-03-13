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


def _assign_bucket(s: pd.Series) -> pd.Series:
    r = s.rank(pct=True, method="first")
    out = pd.Series(index=s.index, dtype=object)
    out[r <= 0.3] = "Bottom30"
    out[r >= 0.7] = "Top30"
    out[(r > 0.3) & (r < 0.7)] = "Middle40"
    return out


def _build_group_ret(panel: pd.DataFrame, factor_col: str = "factor_z", horizon_col: str = "fwd_ret_2w", hold_step: int = 10, trim_q: float = 0.05) -> pd.DataFrame:
    df = panel.dropna(subset=["date", "stock_symbol", factor_col, horizon_col]).copy()
    lo = df.groupby("date")[factor_col].transform(lambda s: s.quantile(trim_q))
    hi = df.groupby("date")[factor_col].transform(lambda s: s.quantile(1 - trim_q))
    df = df[(df[factor_col] >= lo) & (df[factor_col] <= hi)].copy()
    dates = sorted(df["date"].unique().tolist())
    keep = []
    i = 0
    while i < len(dates):
        keep.append(dates[i])
        i += hold_step
    df = df[df["date"].isin(set(keep))].copy()
    df["bucket"] = df.groupby("date")[factor_col].transform(_assign_bucket)
    g = df.groupby(["date", "bucket"], as_index=False)[horizon_col].mean()
    p = g.pivot(index="date", columns="bucket", values=horizon_col).sort_index()
    for c in ["Bottom30", "Middle40", "Top30"]:
        if c not in p.columns:
            p[c] = np.nan
    return p[["Bottom30", "Middle40", "Top30"]].reset_index()


def _build_group_ret_dynamic_hold(panel: pd.DataFrame, regime_df: pd.DataFrame, factor_col: str = "factor_z", horizon_col: str = "fwd_ret_2w", up_step: int = 5, other_step: int = 10, trim_q: float = 0.05) -> pd.DataFrame:
    df = panel.dropna(subset=["date", "stock_symbol", factor_col, horizon_col]).copy()
    lo = df.groupby("date")[factor_col].transform(lambda s: s.quantile(trim_q))
    hi = df.groupby("date")[factor_col].transform(lambda s: s.quantile(1 - trim_q))
    df = df[(df[factor_col] >= lo) & (df[factor_col] <= hi)].copy()
    dates = sorted(df["date"].unique().tolist())
    rg = regime_df.set_index("date")["regime"].to_dict()
    keep = []
    i = 0
    while i < len(dates):
        d = dates[i]
        keep.append(d)
        step = up_step if rg.get(d, "震荡") == "上涨" else other_step
        i += step
    df = df[df["date"].isin(set(keep))].copy()
    df["bucket"] = df.groupby("date")[factor_col].transform(_assign_bucket)
    g = df.groupby(["date", "bucket"], as_index=False)[horizon_col].mean()
    p = g.pivot(index="date", columns="bucket", values=horizon_col).sort_index()
    for c in ["Bottom30", "Middle40", "Top30"]:
        if c not in p.columns:
            p[c] = np.nan
    return p[["Bottom30", "Middle40", "Top30"]].reset_index()


def _apply_up_protect(group_ret: pd.DataFrame, regime_df: pd.DataFrame, up_scale: float = 0.5) -> pd.DataFrame:
    g = group_ret.copy()
    g = g.merge(regime_df, on="date", how="left")
    up = g["regime"] == "上涨"
    for c in ["Bottom30", "Middle40", "Top30"]:
        g.loc[up, c] = g.loc[up, c] * up_scale
    return g.drop(columns=["regime"]).copy()


def _apply_exp1_reverse_on_up(group_ret: pd.DataFrame, regime_df: pd.DataFrame) -> pd.DataFrame:
    g = group_ret.copy()
    g = g.merge(regime_df, on="date", how="left")
    up = g["regime"] == "上涨"
    top = g.loc[up, "Top30"].copy()
    g.loc[up, "Top30"] = g.loc[up, "Bottom30"]
    g.loc[up, "Bottom30"] = top
    return g.drop(columns=["regime"]).copy()


def _summary(group_ret: pd.DataFrame) -> dict:
    g = group_ret.sort_values("date").copy()
    ls = g["Top30"] - g["Bottom30"]
    curve = (1 + ls.fillna(0)).cumprod()
    peak = curve.cummax()
    dd = curve / peak - 1.0
    mdd = float(dd.min()) if not dd.empty else float("nan")
    top_bottom = float(ls.mean())
    hit = float((g["Top30"] > g["Bottom30"]).mean())
    calmar = float("nan") if (pd.isna(mdd) or mdd == 0 or pd.isna(top_bottom)) else top_bottom / abs(mdd)
    return {"hit_ratio": hit, "top_bottom": top_bottom, "max_drawdown": mdd, "calmar": calmar}


def _up_metrics(group_ret: pd.DataFrame, regime_df: pd.DataFrame) -> dict:
    x = group_ret.merge(regime_df, on="date", how="left")
    d = x[x["regime"] == "上涨"].copy()
    if d.empty:
        return {"up_top_bottom": float("nan"), "up_hit_ratio": float("nan")}
    return {
        "up_top_bottom": float((d["Top30"] - d["Bottom30"]).mean()),
        "up_hit_ratio": float((d["Top30"] > d["Bottom30"]).mean()),
    }


def main():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    if root not in sys.path:
        sys.path.insert(0, root)
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
    base["factor_z_raw"] = base["factor_z"]
    base = _industry_neutralize(base, source_col="factor_z_raw", out_col="factor_z_neu")
    regime_df = _load_hs300("2022-01-01", "2025-12-31")
    panel_liq = _apply_liq_dynamic(base, regime_df, keep_other=0.6, keep_up=0.2)

    control_group = _build_group_ret(panel_liq.assign(factor_z=panel_liq["factor_z_neu"]), factor_col="factor_z", hold_step=10, trim_q=0.05)
    control_group = _apply_up_protect(control_group, regime_df, up_scale=0.5)

    exp1_group = _build_group_ret(panel_liq.assign(factor_z=panel_liq["factor_z_neu"]), factor_col="factor_z", hold_step=10, trim_q=0.05)
    exp1_group = _apply_exp1_reverse_on_up(exp1_group, regime_df)
    exp1_group = _apply_up_protect(exp1_group, regime_df, up_scale=0.5)

    exp2_group = _build_group_ret_dynamic_hold(panel_liq.assign(factor_z=panel_liq["factor_z_neu"]), regime_df=regime_df, factor_col="factor_z", up_step=5, other_step=10, trim_q=0.05)
    exp2_group = _apply_up_protect(exp2_group, regime_df, up_scale=0.5)

    mix = panel_liq.copy()
    if "regime" not in mix.columns:
        mix = mix.merge(regime_df, on="date", how="left")
    mix["regime"] = mix["regime"].fillna("震荡")
    mix["factor_z_mix"] = np.where(mix["regime"] == "上涨", mix["factor_z_raw"], mix["factor_z_neu"])
    exp3_group = _build_group_ret(mix.assign(factor_z=mix["factor_z_mix"]), factor_col="factor_z", hold_step=10, trim_q=0.05)
    exp3_group = _apply_up_protect(exp3_group, regime_df, up_scale=0.5)

    rows = []
    for name, g in [
        ("control_baseline_v3", control_group),
        ("exp1_reverse_factor_in_up", exp1_group),
        ("exp2_up_hold_1w", exp2_group),
        ("exp3_disable_indneutral_in_up", exp3_group),
    ]:
        s = _summary(g)
        u = _up_metrics(g, regime_df)
        rows.append(
            {
                "experiment": name,
                "hit_ratio": s["hit_ratio"],
                "max_drawdown": s["max_drawdown"],
                "top_bottom": s["top_bottom"],
                "calmar": s["calmar"],
                "up_top_bottom": u["up_top_bottom"],
                "up_hit_ratio": u["up_hit_ratio"],
            }
        )
    res = pd.DataFrame(rows)
    res.to_csv(os.path.join(out_dir, "up_regime_attribution_results.csv"), index=False, encoding="utf-8-sig")
    control_up = float(res.loc[res["experiment"] == "control_baseline_v3", "up_top_bottom"].iloc[0])
    lines = []
    lines.append("# 上涨市归因实验报告（baseline_v3）")
    lines.append("")
    lines.append("| 实验 | hit_ratio | max_drawdown | top-bottom | calmar | 上涨市top-bottom | 上涨市hit_ratio |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for _, r in res.iterrows():
        lines.append(
            f"| {r['experiment']} | {r['hit_ratio']:.4f} | {r['max_drawdown']:.4f} | {r['top_bottom']:.6f} | {r['calmar']:.6f} | {r['up_top_bottom']:.6f} | {r['up_hit_ratio']:.4f} |"
        )
    lines.append("")
    lines.append("## 归因结论")
    lines.append("")
    for _, r in res.iterrows():
        if r["experiment"] == "control_baseline_v3":
            continue
        improved = r["up_top_bottom"] - control_up
        lines.append(f"- {r['experiment']}：上涨市改善 {improved:+.6f}，是否转正：{'是' if r['up_top_bottom'] > 0 else '否'}")
    lines.append("")
    lines.append("## 针对性优化建议")
    lines.append("")
    e1 = res[res["experiment"] == "exp1_reverse_factor_in_up"].iloc[0]
    e2 = res[res["experiment"] == "exp2_up_hold_1w"].iloc[0]
    e3 = res[res["experiment"] == "exp3_disable_indneutral_in_up"].iloc[0]
    control_calmar = float(res.loc[res["experiment"] == "control_baseline_v3", "calmar"].iloc[0])
    e1_valid = float(e1["up_top_bottom"]) > 0 and float(e1["calmar"]) >= control_calmar
    e2_valid = float(e2["up_top_bottom"]) > 0 and float(e2["calmar"]) >= control_calmar
    e3_valid = float(e3["up_top_bottom"]) > 0 and float(e3["calmar"]) >= control_calmar
    if e1_valid:
        lines.append("- 情况1（实验1有效）：上涨市因子存在反向性，优先尝试反向或“温和正向”因子。")
    if e2_valid:
        lines.append("- 情况2（实验2有效）：上涨市缩短持有期有效，可进一步试1w持有或止盈规则。")
    if e3_valid:
        lines.append("- 情况3（实验3有效）：上涨市行业中性束缚主线，可改宽松中性或阶段性去中性。")
    if e1_valid and e2_valid and e3_valid:
        lines.append("- 情况4（三者均有效）：可组合为上涨市反向因子+1w持有+宽松中性。")
    with open(os.path.join(out_dir, "up_regime_attribution_report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(os.path.join(out_dir, "up_regime_attribution_results.csv"))
    print(os.path.join(out_dir, "up_regime_attribution_report.md"))


if __name__ == "__main__":
    main()
