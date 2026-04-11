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


def _adx(high: pd.Series, low: pd.Series, close: pd.Series, n: int = 14) -> pd.Series:
    """Average Directional Index (ADX). > 25 = trending, < 20 = choppy/ranging."""
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(span=n, adjust=False).mean()
    dm_up = (high - high.shift()).clip(lower=0)
    dm_dn = (low.shift() - low).clip(lower=0)
    dm_up = dm_up.where(dm_up > dm_dn, 0.0)
    dm_dn = dm_dn.where(dm_dn > dm_up, 0.0)
    di_up = 100 * dm_up.ewm(span=n, adjust=False).mean() / atr.replace(0, np.nan)
    di_dn = 100 * dm_dn.ewm(span=n, adjust=False).mean() / atr.replace(0, np.nan)
    dx = (100 * (di_up - di_dn).abs() / (di_up + di_dn).replace(0, np.nan))
    return dx.ewm(span=n, adjust=False).mean()


def _load_hs300(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Load HS300 daily OHLCV and compute:
      - ret20: 20-day return (existing regime signal)
      - adx14: ADX(14) — trending strength (Huatai top-10 A-share timing signal)

    Regime classification (combined ret20 + ADX):
      上涨:  ret20 > +2%  AND adx14 > 20  (confirmed uptrend)
      下跌:  ret20 < -2%  AND adx14 > 20  (confirmed downtrend)
      震荡:  |ret20| <= 2% OR adx14 <= 20  (sideways / weak directional)
    Using ADX removes 10-15 day lag from pure ret20 threshold.
    """
    lg = bs.login()
    if str(lg.error_code) != "0":
        raise RuntimeError(f"baostock login failed: {lg.error_msg}")
    rs = bs.query_history_k_data_plus("sh.000300", "date,open,high,low,close", start_date, end_date, "d")
    if str(rs.error_code) != "0":
        bs.logout()
        raise RuntimeError(f"query_history_k_data_plus failed: {rs.error_msg}")
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    bs.logout()
    idx = pd.DataFrame(rows, columns=["date", "open", "high", "low", "close"])
    idx["date"] = pd.to_datetime(idx["date"], errors="coerce").dt.normalize()
    for c in ["open", "high", "low", "close"]:
        idx[c] = pd.to_numeric(idx[c], errors="coerce")
    idx = idx.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    idx["ret20"] = idx["close"] / idx["close"].shift(20) - 1.0
    idx["adx14"] = _adx(idx["high"], idx["low"], idx["close"], n=14)
    # Combined regime: direction from ret20, conviction from ADX
    idx["regime"] = "震荡"
    trending = idx["adx14"] > 20
    idx.loc[trending & (idx["ret20"] > 0.02),  "regime"] = "上涨"
    idx.loc[trending & (idx["ret20"] < -0.02), "regime"] = "下跌"
    # Also store adx14 and hs300_ret20 for downstream use
    return idx[["date", "regime", "adx14"]]


def _load_liquidity(liquidity_csv: str) -> pd.DataFrame:
    if not os.path.exists(liquidity_csv):
        return pd.DataFrame(columns=["date", "stock_symbol", "amount", "turnover_rate"])
    liq = pd.read_csv(liquidity_csv, usecols=["date", "stock_symbol", "amount", "turnover_rate"])
    liq["date"] = pd.to_datetime(liq["date"], errors="coerce").dt.normalize()
    liq["stock_symbol"] = liq["stock_symbol"].astype(str).str.upper()
    liq["amount"] = pd.to_numeric(liq["amount"], errors="coerce")
    liq["turnover_rate"] = pd.to_numeric(liq["turnover_rate"], errors="coerce")
    liq = liq.dropna(subset=["date", "stock_symbol"])
    return liq


def _load_industry(industry_map_csv: str) -> pd.DataFrame:
    if not os.path.exists(industry_map_csv):
        return pd.DataFrame(columns=["stock_symbol_standard", "industry_l2"])
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
    df = df.merge(liq[["date", "stock_symbol", "amount"]], on=["date", "stock_symbol"], how="left")
    ret = df.groupby("stock_symbol")["close"].pct_change()
    df["vol20"] = ret.groupby(df["stock_symbol"]).transform(lambda s: s.rolling(20, min_periods=10).std())
    df["ret20d_stock"] = df.groupby("stock_symbol")["close"].transform(lambda s: s / s.shift(20) - 1.0)
    return df


def _assign_other_industry_by_proxy(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    base = out[(out["industry_l2"] != "其他") & out["vol20"].notna()].copy()
    if base.empty:
        return out
    base["mv_proxy"] = out["amount"]
    base["log_mv"] = np.log1p(base["mv_proxy"].fillna(base["mv_proxy"].median()))
    cent = base.groupby("industry_l2", as_index=False).agg(log_mv=("log_mv", "median"), vol20=("vol20", "median"))
    cent = cent.replace([np.inf, -np.inf], np.nan).dropna(subset=["log_mv", "vol20"])
    if cent.empty:
        return out
    other = out["industry_l2"] == "其他"
    have_proxy = other & out["amount"].notna() & out["vol20"].notna()
    if have_proxy.any():
        mv = np.log1p(out.loc[have_proxy, "amount"].to_numpy())
        vol = out.loc[have_proxy, "vol20"].to_numpy()
        pts = np.column_stack([mv, vol])
        cts = cent[["log_mv", "vol20"]].to_numpy()
        dist = ((pts[:, None, :] - cts[None, :, :]) ** 2).sum(axis=2)
        idx = dist.argmin(axis=1)
        out.loc[have_proxy, "industry_l2"] = cent["industry_l2"].to_numpy()[idx]
    return out


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
    has_amount = x.groupby("date")["amount"].transform(lambda s: s.notna().any())
    keep = np.where(has_amount, rank >= (1 - keep_ratio), True)
    keep = pd.Series(keep, index=x.index).fillna(True)
    return x[keep].copy()


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


def _build_group_ret_v42(panel: pd.DataFrame, trim_q: float = 0.05, hold_step: int = 10) -> pd.DataFrame:
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
        mid = day[(day["rank"] > 0.3) & (day["rank"] < 0.7)].copy()
        bot = day[day["rank"] <= 0.3].copy()
        regime = str(day["regime"].iloc[0])
        if regime == "上涨":
            n_pool = max(1, int(round(0.5 * n)))
            pool = _select_top_with_industry_cap(day, n_target=n_pool, cap_ratio=0.2)
            n_pick = max(1, int(round(0.3 * n)))
            pool = pool.sort_values("ret20d_stock", ascending=True)
            top = pool.head(n_pick).copy()
        else:
            top = day[day["rank"] >= 0.7].copy()
        rows.append(
            {
                "date": d,
                "Bottom30": float(bot["fwd_ret_2w"].mean()) if not bot.empty else np.nan,
                "Middle40": float(mid["fwd_ret_2w"].mean()) if not mid.empty else np.nan,
                "Top30": float(top["fwd_ret_2w"].mean()) if not top.empty else np.nan,
                "regime": regime,
            }
        )
    return pd.DataFrame(rows).sort_values("date")


def _apply_up_exposure(group_ret: pd.DataFrame, up_scale: float = 0.5) -> pd.DataFrame:
    g = group_ret.copy()
    up = g["regime"] == "上涨"
    for c in ["Bottom30", "Middle40", "Top30"]:
        g.loc[up, c] = g.loc[up, c] * up_scale
    return g


def _metrics(group_ret: pd.DataFrame) -> dict:
    g = group_ret.sort_values("date").copy()
    ls = g["Top30"] - g["Bottom30"]
    curve = (1 + ls.fillna(0)).cumprod()
    peak = curve.cummax()
    dd = curve / peak - 1.0
    mdd = float(dd.min()) if not dd.empty else float("nan")
    calmar = float(ls.mean()) / abs(mdd) if (not pd.isna(mdd) and mdd != 0) else float("nan")
    up = g[g["regime"] == "上涨"]
    up_tb = float((up["Top30"] - up["Bottom30"]).mean()) if not up.empty else float("nan")
    return {"up_top_bottom": up_tb, "calmar": calmar}


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
    group_ret = _build_group_ret_v42(panel_liq, trim_q=0.05, hold_step=10)
    group_ret = _apply_up_exposure(group_ret, up_scale=0.5)
    m = _metrics(group_ret)
    pd.DataFrame(
        {"metric": ["up_top_bottom", "calmar_ratio"], "value": [m["up_top_bottom"], m["calmar"]]}
    ).to_csv(os.path.join(out_dir, "core_metrics_baseline_v4_2_2022_2025.csv"), index=False, encoding="utf-8-sig")
    with open(os.path.join(out_dir, "baseline_v4_2_micro_report.md"), "w", encoding="utf-8") as f:
        f.write("# baseline_v4.2 微调结果（2022-2025）\n\n")
        f.write(f"- 上涨市 top-bottom: {m['up_top_bottom']:.6f}\n")
        f.write(f"- 整体 calmar: {m['calmar']:.6f}\n")
    print(f"up_top_bottom={m['up_top_bottom']:.6f}")
    print(f"calmar={m['calmar']:.6f}")


if __name__ == "__main__":
    main()
