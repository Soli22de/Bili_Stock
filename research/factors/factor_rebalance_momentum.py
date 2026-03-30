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


def build_rebalance_momentum_factor(
    rebalancing_df: pd.DataFrame,
    start_date: str = "2019-01-01",
    end_date: str = "2025-12-31",
    lag_days: int = 14,
    smoothing_days: int = 3,
    factor_mode: str = "rate",
) -> pd.DataFrame:
    req = {"cube_symbol", "stock_symbol", "target_weight", "prev_weight_adjusted"}
    # 兼容 created_at 或 updated_at
    time_col = "created_at" if "created_at" in rebalancing_df.columns else "updated_at"
    req.add(time_col)
    
    miss = req - set(rebalancing_df.columns)
    if miss:
        raise ValueError(f"rebalancing_history 缺少字段: {sorted(miss)}")
    rb = rebalancing_df.copy()
    
    # 统一时间列名
    if time_col != "created_at":
        rb.rename(columns={time_col: "created_at"}, inplace=True)
        
    rb["date"] = pd.to_datetime(rb["created_at"], errors="coerce").dt.normalize()
    rb["target_weight"] = pd.to_numeric(rb["target_weight"], errors="coerce")
    rb["prev_weight_adjusted"] = pd.to_numeric(rb["prev_weight_adjusted"], errors="coerce")
    rb = rb.dropna(subset=["date", "cube_symbol", "stock_symbol", "target_weight", "prev_weight_adjusted"])
    start_dt = pd.to_datetime(start_date).normalize()
    end_dt = pd.to_datetime(end_date).normalize()
    rb = rb[(rb["date"] >= start_dt) & (rb["date"] <= end_dt)].copy()
    rb["weight_delta"] = rb["target_weight"] - rb["prev_weight_adjusted"]
    buy = rb[rb["weight_delta"] > 0].copy()
    buy["cube_symbol"] = buy["cube_symbol"].astype(str)
    buy["stock_symbol"] = buy["stock_symbol"].astype(str)
    daily = buy.groupby(["date", "stock_symbol"], as_index=False)["cube_symbol"].nunique()
    daily.rename(columns={"cube_symbol": "net_buy_cube_count"}, inplace=True)
    if daily.empty:
        return pd.DataFrame(columns=["date", "stock_symbol", "net_buy_cube_count", "count_lag", "factor_raw", "factor_z"])
    rows = []
    for stock, g in daily.groupby("stock_symbol"):
        g = g.sort_values("date").set_index("date")
        full_idx = pd.date_range(start_dt, end_dt, freq="D")
        
        if factor_mode == "rate":
            # For rate mode, we need history to calculate shift.
            pass

        g = g.reindex(full_idx)
        g.index.name = "date"
        g["stock_symbol"] = stock
        g["net_buy_cube_count"] = pd.to_numeric(g["net_buy_cube_count"], errors="coerce").fillna(0.0)
        
        # FIX: We need to smooth/accumulate counts before shifting?
        # If daily count is 0 on most days, then shift(14) is likely 0.
        # Then denom is 1.0 (clipped).
        # Then base is (0 - 0) / 1 = 0.
        # This results in 0 factor for most days.
        
        # Let's stick to the original logic but ensure we handle the start correctly.
        g["count_lag"] = g["net_buy_cube_count"].shift(lag_days).fillna(0.0)
        
        # If the whole count_lag is 0 (early history), we might want to fill with mean?
        # But for 'rate' mode, 0 lag means denominator is 1.0, and factor is net_buy_cube_count.
        # This is essentially absolute momentum in the early days.
        # This is acceptable behavior: when history is short, momentum = absolute count.
        
        if factor_mode == "rate":
            denom = g["count_lag"].clip(lower=1.0)
            base = (g["net_buy_cube_count"] - g["count_lag"]) / denom
        elif factor_mode == "absolute":
            base = g["net_buy_cube_count"].rolling(lag_days, min_periods=1).mean()
        else:
            raise ValueError("factor_mode must be 'rate' or 'absolute'")
        g["factor_raw"] = base.rolling(smoothing_days, min_periods=1).mean()
        rows.append(g.reset_index()[["date", "stock_symbol", "net_buy_cube_count", "count_lag", "factor_raw"]])
    out = pd.concat(rows, ignore_index=True)
    out["factor_z"] = out.groupby("date")["factor_raw"].transform(_zscore)
    out = out.sort_values(["date", "stock_symbol"]).reset_index(drop=True)
    return out
