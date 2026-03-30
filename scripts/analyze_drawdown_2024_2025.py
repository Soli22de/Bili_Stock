import os
import sys
import pandas as pd
import numpy as np


def _load_holdings(root: str) -> pd.DataFrame:
    if root not in sys.path:
        sys.path.insert(0, root)
    from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5
    from research.baseline_v6_1.code.run_baseline_v6_v61_suite import _enrich_from_stock_data

    path = os.path.join(root, "research", "baseline_v6_1", "output", "holdings_baseline_v6_1_2019_2025.csv")
    df = pd.read_csv(path)
    df["date"] = pd.to_datetime(df["date"])
    if "fwd_ret_2w" not in df.columns or "industry_l2" not in df.columns:
        panel = _prepare_panel_v5()
        panel, _ = _enrich_from_stock_data(panel)
        panel["date"] = pd.to_datetime(panel["date"], errors="coerce")
        panel = panel[(panel["date"] >= pd.Timestamp("2019-01-01")) & (panel["date"] <= pd.Timestamp("2025-12-31"))].copy()
        ext_cols = ["date", "stock_symbol", "industry_l2", "fwd_ret_2w", "net_buy_cube_count", "count_lag"]
        ext = panel[ext_cols].drop_duplicates(["date", "stock_symbol"])
        df["stock_symbol"] = df["stock_symbol"].astype(str)
        ext["stock_symbol"] = ext["stock_symbol"].astype(str)
        df = df.merge(ext, on=["date", "stock_symbol"], how="left")
    if "weight" not in df.columns:
        cnt = df.groupby("date")["stock_symbol"].transform("count").replace(0, np.nan)
        df["weight"] = 1.0 / cnt
    if "fwd_ret_2w_use" in df.columns:
        df["ret_use"] = pd.to_numeric(df["fwd_ret_2w_use"], errors="coerce")
    elif "fwd_ret_2w_sim" in df.columns:
        df["ret_use"] = pd.to_numeric(df["fwd_ret_2w_sim"], errors="coerce")
    else:
        df["ret_use"] = pd.to_numeric(df["fwd_ret_2w"], errors="coerce")
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
    df["industry_l2"] = df["industry_l2"].fillna("其他")
    df["net_buy_cube_count"] = pd.to_numeric(df.get("net_buy_cube_count"), errors="coerce")
    df["count_lag"] = pd.to_numeric(df.get("count_lag"), errors="coerce")
    return df


def _build_period_returns(df: pd.DataFrame) -> pd.DataFrame:
    x = df.dropna(subset=["date", "ret_use", "weight"]).copy()
    x["contrib"] = x["ret_use"] * x["weight"]
    pr = x.groupby("date", as_index=False)["contrib"].sum()
    pr.rename(columns={"contrib": "period_ret"}, inplace=True)
    pr = pr.sort_values("date").reset_index(drop=True)
    return pr


def _drawdown_intervals(pr: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    x = pr.copy()
    x = x[(x["date"] >= pd.to_datetime("2019-01-01")) & (x["date"] <= pd.to_datetime("2025-12-31"))]
    x["equity"] = (1 + x["period_ret"].fillna(0)).cumprod()
    x["peak"] = x["equity"].cummax()
    x["dd"] = x["equity"] / x["peak"] - 1.0
    intervals = []
    in_dd = False
    peak_date = None
    trough_date = None
    trough_dd = 0.0
    start_date = None
    current_peak = None
    current_peak_date = None
    for _, r in x.iterrows():
        d = r["date"]
        eq = r["equity"]
        dd = r["dd"]
        if current_peak is None or eq >= current_peak:
            current_peak = eq
            current_peak_date = d
        if dd < 0 and not in_dd:
            in_dd = True
            start_date = d
            peak_date = current_peak_date
            trough_date = d
            trough_dd = dd
        elif in_dd:
            if dd < trough_dd:
                trough_dd = dd
                trough_date = d
            if dd == 0:
                end_date = d
                intervals.append(
                    {
                        "peak_date": peak_date,
                        "start_date": start_date,
                        "trough_date": trough_date,
                        "end_date": end_date,
                        "drawdown": trough_dd,
                    }
                )
                in_dd = False
                peak_date = None
                trough_date = None
                trough_dd = 0.0
                start_date = None
    if in_dd:
        intervals.append(
            {
                "peak_date": peak_date,
                "start_date": start_date,
                "trough_date": trough_date,
                "end_date": x["date"].iloc[-1],
                "drawdown": trough_dd,
            }
        )
    out = pd.DataFrame(intervals)
    if out.empty:
        return out
    st = pd.to_datetime(start)
    ed = pd.to_datetime(end)
    mask = (out["start_date"] <= ed) & (out["end_date"] >= st)
    out = out[mask].reset_index(drop=True)
    out["start_date"] = out["start_date"].where(out["start_date"] >= st, st)
    out["end_date"] = out["end_date"].where(out["end_date"] <= ed, ed)
    out["interval_id"] = range(1, len(out) + 1)
    return out


def _industry_contrib(df: pd.DataFrame, intervals: pd.DataFrame) -> pd.DataFrame:
    rows = []
    x = df.copy()
    x = x.dropna(subset=["ret_use", "weight"])
    x["contrib"] = x["ret_use"] * x["weight"]
    for _, it in intervals.iterrows():
        s = it["start_date"]
        e = it["end_date"]
        seg = x[(x["date"] >= s) & (x["date"] <= e)].copy()
        if seg.empty:
            continue
        total = seg["contrib"].sum()
        by_ind = seg.groupby("industry_l2", as_index=False)["contrib"].sum()
        by_ind["interval_id"] = it["interval_id"]
        by_ind["start_date"] = s
        by_ind["end_date"] = e
        by_ind["total_contrib"] = total
        by_ind["share"] = by_ind["contrib"] / total if total != 0 else np.nan
        rows.append(by_ind)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out = out.sort_values(["interval_id", "contrib"])
    return out


def _rebalance_pnl(pr: pd.DataFrame) -> pd.DataFrame:
    x = pr.copy().sort_values("date").reset_index(drop=True)
    x["end_date"] = x["date"].shift(-1)
    x["end_date"] = x["end_date"].fillna(x["date"] + pd.Timedelta(days=14))
    return x


def _heat_signal(df: pd.DataFrame, intervals: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["heat_drop"] = (x["net_buy_cube_count"] - x["count_lag"]) / x["count_lag"].clip(lower=1.0)
    x["heat_flag"] = (x["heat_drop"] <= -0.3) & (x["net_buy_cube_count"] < x["count_lag"])
    rows = []
    for _, it in intervals.iterrows():
        s = it["start_date"] - pd.Timedelta(days=14)
        e = it["start_date"] - pd.Timedelta(days=1)
        seg = x[(x["date"] >= s) & (x["date"] <= e)].copy()
        if seg.empty:
            continue
        seg = seg.dropna(subset=["weight"])
        w = seg["weight"].fillna(0)
        flag_ratio = (seg["heat_flag"].astype(float) * w).sum() / w.sum() if w.sum() > 0 else np.nan
        avg_drop = (seg["heat_drop"] * w).sum() / w.sum() if w.sum() > 0 else np.nan
        rows.append(
            {
                "interval_id": it["interval_id"],
                "window_start": s,
                "window_end": e,
                "weighted_flag_ratio": flag_ratio,
                "weighted_heat_drop": avg_drop,
            }
        )
    return pd.DataFrame(rows)


def _market_sentiment(root: str, intervals: pd.DataFrame) -> pd.DataFrame:
    cache_dir = os.path.join(root, "data", "cache")
    hs300_path = os.path.join(cache_dir, "SH000300.csv")
    cn_path = os.path.join(cache_dir, "SZ159915_fresh.csv")
    if not os.path.exists(cn_path):
        cn_path = os.path.join(cache_dir, "SZ159915.csv")
    hs300 = pd.read_csv(hs300_path)
    cn = pd.read_csv(cn_path)
    if "date" not in hs300.columns and "日期" in hs300.columns:
        hs300 = hs300.rename(columns={"日期": "date"})
    if "close" not in hs300.columns and "收盘" in hs300.columns:
        hs300 = hs300.rename(columns={"收盘": "close"})
    if "date" not in cn.columns and "日期" in cn.columns:
        cn = cn.rename(columns={"日期": "date"})
    if "close" not in cn.columns and "收盘" in cn.columns:
        cn = cn.rename(columns={"收盘": "close"})
    if "volume" not in cn.columns and "成交量" in cn.columns:
        cn = cn.rename(columns={"成交量": "volume"})
    hs300["date"] = pd.to_datetime(hs300["date"])
    cn["date"] = pd.to_datetime(cn["date"])
    hs300 = hs300.sort_values("date").set_index("date")
    cn = cn.sort_values("date").set_index("date")
    hs300["ret20d"] = hs300["close"] / hs300["close"].shift(20) - 1.0
    cn["vol_pct"] = cn["volume"].rolling(250, min_periods=20).apply(
        lambda s: pd.Series(s).rank(pct=True).iloc[-1], raw=False
    )
    rows = []
    for _, it in intervals.iterrows():
        d = it["start_date"]
        dd = pd.to_datetime(d)
        hs = hs300.reindex(hs300.index.union([dd])).sort_index().loc[:dd].iloc[-1]
        cnr = cn.reindex(cn.index.union([dd])).sort_index().loc[:dd].iloc[-1]
        rows.append(
            {
                "interval_id": it["interval_id"],
                "date": dd,
                "hs300_ret20d": hs.get("ret20d"),
                "chinext_vol_pct": cnr.get("vol_pct"),
                "margin_balance_pct": np.nan,
                "vix_pct": np.nan,
            }
        )
    return pd.DataFrame(rows)


def _stop_loss_efficiency(df: pd.DataFrame, intervals: pd.DataFrame, root: str, threshold: float) -> pd.DataFrame:
    data_dir = os.path.join(root, "data", "stock_data")
    dates = sorted(df["date"].unique())
    next_map = {dates[i]: dates[i + 1] for i in range(len(dates) - 1)}
    x = df.copy()
    x = x[(x["date"] >= pd.to_datetime("2024-01-01")) & (x["date"] <= pd.to_datetime("2025-12-31"))]
    x = x.dropna(subset=["ret_use", "weight"])
    rows = []
    for _, r in x.iterrows():
        d = r["date"]
        sym = str(r["stock_symbol"])
        w = float(r["weight"])
        end_date = next_map.get(d, d + pd.Timedelta(days=14))
        fp = os.path.join(data_dir, f"{sym}.csv")
        if not os.path.exists(fp):
            continue
        p = pd.read_csv(fp)
        if "日期" in p.columns:
            p = p.rename(columns={"日期": "date", "收盘": "close"})
        if "date" not in p.columns or "close" not in p.columns:
            continue
        p["date"] = pd.to_datetime(p["date"])
        p["close"] = pd.to_numeric(p["close"], errors="coerce")
        p = p.dropna(subset=["date", "close"]).sort_values("date")
        seq = p[(p["date"] >= d) & (p["date"] <= end_date)]
        if seq.empty:
            continue
        entry = float(seq["close"].iloc[0])
        if entry <= 0:
            continue
        seq["ret"] = seq["close"] / entry - 1.0
        min_ret = float(seq["ret"].min())
        hit = min_ret <= -abs(threshold)
        stop_ret = np.nan
        stop_date = None
        if hit:
            stop_row = seq[seq["ret"] <= -abs(threshold)].iloc[0]
            stop_ret = float(stop_row["ret"])
            stop_date = stop_row["date"]
        exit_ret = float(seq["ret"].iloc[-1])
        avoid = exit_ret - stop_ret if hit else 0.0
        rows.append(
            {
                "date": d,
                "stock_symbol": sym,
                "weight": w,
                "min_ret": min_ret,
                "hit_stop": hit,
                "stop_date": stop_date,
                "stop_ret": stop_ret,
                "exit_ret": exit_ret,
                "avoidable_ret": avoid,
            }
        )
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["avoidable_contrib"] = out["avoidable_ret"] * out["weight"]
    return out


def main():
    root = os.getcwd()
    out_dir = os.path.join(root, "research", "baseline_v6_1", "report")
    os.makedirs(out_dir, exist_ok=True)

    hold = _load_holdings(root)
    pr = _build_period_returns(hold)
    pr_2024 = pr[(pr["date"] >= pd.to_datetime("2024-01-01")) & (pr["date"] <= pd.to_datetime("2025-12-31"))].copy()
    pr_2024.to_csv(os.path.join(out_dir, "rebalance_period_pnl_2024_2025.csv"), index=False)

    intervals = _drawdown_intervals(pr, "2024-01-01", "2025-12-31")
    intervals.to_csv(os.path.join(out_dir, "drawdown_intervals_2024_2025.csv"), index=False)

    ind = _industry_contrib(hold, intervals)
    ind.to_csv(os.path.join(out_dir, "industry_drawdown_contrib_2024_2025.csv"), index=False)

    heat = _heat_signal(hold, intervals)
    heat.to_csv(os.path.join(out_dir, "heat_signal_ahead_drawdown_2024_2025.csv"), index=False)

    sentiment = _market_sentiment(root, intervals)
    sentiment.to_csv(os.path.join(out_dir, "market_sentiment_drawdown_2024_2025.csv"), index=False)

    stop = _stop_loss_efficiency(hold, intervals, root, threshold=0.08)
    stop.to_csv(os.path.join(out_dir, "stop_loss_efficiency_2024_2025.csv"), index=False)


if __name__ == "__main__":
    main()
