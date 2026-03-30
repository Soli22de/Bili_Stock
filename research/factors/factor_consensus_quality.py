import numpy as np
import pandas as pd


def _zscore(s: pd.Series) -> pd.Series:
    std = s.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=s.index)
    return (s - s.mean()) / std


def _winsorize(s: pd.Series, lower=0.01, upper=0.99) -> pd.Series:
    if s.empty:
        return s
    lo = s.quantile(lower)
    hi = s.quantile(upper)
    return s.clip(lo, hi)


def _calc_rolling_consensus(daily: pd.DataFrame, window_days: int) -> pd.DataFrame:
    if daily.empty:
        return daily
    out = []
    for stock, g in daily.groupby("stock_symbol"):
        g = g.sort_values("date").set_index("date")
        full_idx = pd.date_range(g.index.min(), g.index.max(), freq="D")
        g = g.reindex(full_idx).fillna(0.0)
        g.index.name = "date"
        g["stock_symbol"] = stock
        g["good_count_7d"] = g["good_count"].rolling(window=window_days, min_periods=1).sum()
        g["good_buy_7d"] = g["good_buy_sum"].rolling(window=window_days, min_periods=1).sum()
        g["bad_count_7d"] = g["bad_count"].rolling(window=window_days, min_periods=1).sum()
        out.append(g.reset_index())
    return pd.concat(out, ignore_index=True)


def build_manager_quality(
    cube_perf: pd.DataFrame,
    rebalancing_df: pd.DataFrame,
    as_of_date: pd.Timestamp,
    min_closed_trades: int = 20,
) -> pd.DataFrame:
    req = {"symbol", "trade_count", "closed_trades", "win_rate", "simulated_return_pct", "followers_count", "created_at"}
    miss = req - set(cube_perf.columns)
    if miss:
        raise ValueError(f"cube_performance 缺少字段: {sorted(miss)}")
    df = cube_perf.copy()
    for col in ["trade_count", "closed_trades", "win_rate", "simulated_return_pct", "followers_count"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce").dt.normalize()
    rb = rebalancing_df.copy()
    rb["date"] = pd.to_datetime(rb["created_at"], errors="coerce").dt.normalize()
    rb = rb.dropna(subset=["cube_symbol", "date"]).copy()
    rb["cube_symbol"] = rb["cube_symbol"].astype(str)
    rebalance_counts = rb.groupby("cube_symbol", as_index=False)["date"].nunique().rename(columns={"date": "rebalance_days"})
    df["symbol"] = df["symbol"].astype(str)
    df = df.merge(rebalance_counts, left_on="symbol", right_on="cube_symbol", how="left")
    df["rebalance_days"] = pd.to_numeric(df["rebalance_days"], errors="coerce").fillna(0.0)
    df["age_years"] = (as_of_date - df["created_at"]).dt.days / 365.25
    df["age_years"] = df["age_years"].where(df["age_years"] > 0)
    df["annual_rebalance"] = df["rebalance_days"] / df["age_years"]
    df["annual_rebalance"] = df["annual_rebalance"].replace([np.inf, -np.inf], np.nan)
    df = df[df["closed_trades"] >= min_closed_trades].copy()
    df = df.dropna(subset=["symbol", "trade_count", "win_rate", "simulated_return_pct"])
    if df.empty:
        return pd.DataFrame(
            columns=[
                "cube_symbol",
                "quality_score",
                "win_rate",
                "annual_rebalance",
                "age_years",
                "followers_count",
                "is_top_quality",
                "is_bottom_quality",
            ]
        )
    ret = _winsorize(df["simulated_return_pct"])
    win = _winsorize(df["win_rate"])
    turn = _winsorize(df["trade_count"])
    score = 0.45 * _zscore(win) + 0.45 * _zscore(ret) - 0.10 * _zscore(turn)
    top10 = float(df["win_rate"].quantile(0.90))
    bottom20 = float(df["win_rate"].quantile(0.20))
    is_top_quality = (
        (df["win_rate"] >= top10)
        & (df["annual_rebalance"] < 200.0)
        & (df["age_years"] > 3.0)
        & (df["followers_count"] < 500.0)
    )
    is_bottom_quality = df["win_rate"] <= bottom20
    out = pd.DataFrame(
        {
            "cube_symbol": df["symbol"].astype(str),
            "quality_score": score.astype(float),
            "win_rate": df["win_rate"].astype(float),
            "annual_rebalance": df["annual_rebalance"].astype(float),
            "age_years": df["age_years"].astype(float),
            "followers_count": df["followers_count"].astype(float),
            "is_top_quality": is_top_quality.astype(bool),
            "is_bottom_quality": is_bottom_quality.astype(bool),
        }
    )
    out = out.groupby("cube_symbol", as_index=False).agg(
        quality_score=("quality_score", "mean"),
        win_rate=("win_rate", "mean"),
        annual_rebalance=("annual_rebalance", "mean"),
        age_years=("age_years", "mean"),
        followers_count=("followers_count", "mean"),
        is_top_quality=("is_top_quality", "max"),
        is_bottom_quality=("is_bottom_quality", "max"),
    )
    return out


def build_consensus_factor(
    rebalancing_df: pd.DataFrame,
    quality_df: pd.DataFrame,
    mode: str = "baseline",
    consensus_window_days: int = 7,
    min_quality_buyers: int = 3,
) -> pd.DataFrame:
    req = {"cube_symbol", "stock_symbol", "created_at", "target_weight", "prev_weight_adjusted"}
    miss = req - set(rebalancing_df.columns)
    if miss:
        raise ValueError(f"rebalancing_history 缺少字段: {sorted(miss)}")
    rb = rebalancing_df.copy()
    rb["date"] = pd.to_datetime(rb["created_at"], errors="coerce").dt.normalize()
    rb["target_weight"] = pd.to_numeric(rb["target_weight"], errors="coerce")
    rb["prev_weight_adjusted"] = pd.to_numeric(rb["prev_weight_adjusted"], errors="coerce")
    rb = rb.dropna(subset=["date", "cube_symbol", "stock_symbol", "target_weight", "prev_weight_adjusted"])
    rb["weight_delta"] = rb["target_weight"] - rb["prev_weight_adjusted"]
    rb["buy_delta"] = rb["weight_delta"].clip(lower=0)
    q = quality_df.copy()
    q["is_top_quality"] = q["is_top_quality"].fillna(False).astype(bool)
    q["is_bottom_quality"] = q["is_bottom_quality"].fillna(False).astype(bool)
    q["cube_symbol"] = q["cube_symbol"].astype(str)
    rb["cube_symbol"] = rb["cube_symbol"].astype(str)
    merged = rb.merge(q, on="cube_symbol", how="inner")
    if merged.empty:
        return pd.DataFrame(columns=["date", "stock_symbol", "factor_raw", "factor_z"])
    if mode == "optimized":
        m = merged.copy()
        m["good_count"] = np.where((m["is_top_quality"]) & (m["buy_delta"] > 0), 1.0, 0.0)
        m["bad_count"] = np.where((m["is_bottom_quality"]) & (m["buy_delta"] > 0), 1.0, 0.0)
        m["good_buy_sum"] = np.where((m["is_top_quality"]) & (m["buy_delta"] > 0), m["buy_delta"], 0.0)
        daily = m.groupby(["date", "stock_symbol"], as_index=False).agg(
            good_count=("good_count", "sum"),
            bad_count=("bad_count", "sum"),
            good_buy_sum=("good_buy_sum", "sum"),
        )
        roll = _calc_rolling_consensus(daily, consensus_window_days)
        roll["is_valid"] = (roll["good_count_7d"] >= float(min_quality_buyers)) & (roll["bad_count_7d"] == 0.0)
        roll["factor_raw"] = np.where(roll["is_valid"], roll["good_buy_7d"], np.nan)
        daily = roll[["date", "stock_symbol", "factor_raw"]].dropna(subset=["factor_raw"]).copy()
    else:
        merged["quality_buy"] = merged["quality_score"] * merged["buy_delta"]
        daily = merged.groupby(["date", "stock_symbol"], as_index=False)["quality_buy"].sum()
        daily.rename(columns={"quality_buy": "factor_raw"}, inplace=True)
    daily["factor_raw"] = _winsorize(daily["factor_raw"])
    daily["factor_z"] = daily.groupby("date")["factor_raw"].transform(_zscore)
    return daily.sort_values(["date", "stock_symbol"]).reset_index(drop=True)
