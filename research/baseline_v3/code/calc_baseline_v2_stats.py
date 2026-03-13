
import os
import sys
import numpy as np
import pandas as pd

# Add root to path
root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if root not in sys.path:
    sys.path.insert(0, root)

from research.data_prep.build_rebalance_momentum_panel import build_rebalance_momentum_panel

# --- Helper Functions from run_rebalance_momentum_grid_and_indopt.py ---

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
    
    # Calculate vol20 for optimization
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
    
    # Drop remaining '其他'
    out = out[~other | have_proxy].copy()
    return out

def _zscore(s: pd.Series) -> pd.Series:
    std = s.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=s.index)
    return (s - s.mean()) / std

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
    # 2022-2025: amount filter
    amount_rank = df[mask_recent].groupby("date")["amount"].transform(lambda s: s.rank(pct=True, method="first"))
    keep_recent = amount_rank >= (1 - quantile_keep)
    
    mask_old = ~mask_recent
    # 2019-2021: circ_mv_proxy filter or main board fallback
    mv_rank = df[mask_old].groupby("date")["circ_mv_proxy"].transform(lambda s: s.rank(pct=True, method="first"))
    keep_old_mv = mv_rank >= (1 - quantile_keep)
    keep_old_board = df[mask_old]["is_main_board"] & df[mask_old]["circ_mv_proxy"].isna()
    
    keep = pd.Series(False, index=df.index)
    keep.loc[mask_recent] = keep_recent.fillna(False)
    keep.loc[mask_old] = (keep_old_mv.fillna(False) | keep_old_board.fillna(False))
    
    out = df[keep].copy()
    return out.drop(columns=["year", "is_main_board"])

# --- Holding Analysis Logic ---

def _assign_bucket(s: pd.Series) -> pd.Series:
    r = s.rank(pct=True, method="first")
    out = pd.Series(index=s.index, dtype=object)
    out[r <= 0.3] = "Bottom30"
    out[r >= 0.7] = "Top30" # This is our Long Basket
    out[(r > 0.3) & (r < 0.7)] = "Middle40"
    return out

def analyze_transactions(panel: pd.DataFrame, hold_lock_days: int = 10, trim_q: float = 0.05):
    """
    Simulates the rebalance process and tracks transactions for Top30 basket.
    """
    df = panel.dropna(subset=["date", "stock_symbol", "factor_z"]).copy()
    
    # 1. Apply outlier trim
    lo = df.groupby("date")["factor_z"].transform(lambda s: s.quantile(trim_q))
    hi = df.groupby("date")["factor_z"].transform(lambda s: s.quantile(1 - trim_q))
    df = df[(df["factor_z"] >= lo) & (df["factor_z"] <= hi)].copy()
    
    # 2. Get Rebalance Dates (Holding Lock)
    dates = sorted(df["date"].unique())
    rebalance_dates = []
    i = 0
    while i < len(dates):
        rebalance_dates.append(dates[i])
        i += hold_lock_days
    
    # 3. Filter to rebalance dates
    df_reb = df[df["date"].isin(rebalance_dates)].copy()
    
    # 4. Assign Buckets
    df_reb["bucket"] = df_reb.groupby("date")["factor_z"].transform(_assign_bucket)
    
    # 5. Track Holdings
    holdings = [] # List of sets
    dates_tracked = []
    
    for d in sorted(rebalance_dates):
        day_data = df_reb[df_reb["date"] == d]
        top_stocks = set(day_data[day_data["bucket"] == "Top30"]["stock_symbol"].tolist())
        holdings.append(top_stocks)
        dates_tracked.append(d)
        
    # 6. Calculate Metrics
    total_buys = 0
    total_sells = 0
    turnover_counts = [] # Changes per rebalance
    empty_periods = 0
    
    # Initial buy (from empty to first holding)
    if holdings:
        total_buys += len(holdings[0])
        # We usually don't count the initial setup as "turnover" for the average, 
        # but for "Total Transactions" user asked for "Buy + Sell total". 
        # Let's count it.
    
    for i in range(1, len(holdings)):
        prev = holdings[i-1]
        curr = holdings[i]
        
        buys = curr - prev
        sells = prev - curr
        
        n_buys = len(buys)
        n_sells = len(sells)
        
        total_buys += n_buys
        total_sells += n_sells
        turnover_counts.append(n_buys + n_sells)
        
        if len(curr) == 0:
            empty_periods += 1
            
    # Check if first period was empty
    if holdings and len(holdings[0]) == 0:
        empty_periods += 1
        
    avg_turnover = np.mean(turnover_counts) if turnover_counts else 0.0
    
    # Empty Days Calculation
    # We assume each rebalance period lasts `hold_lock_days` (trading days approx)
    # Or we can count exact calendar days if we want, but "days" in trading context usually means trading days.
    # The backtest locks for 10 periods (days in the filtered list? No, days in the sorted unique dates list).
    # Actually hold_lock_days=10 in `group_backtest` means we skip 10 timestamps in the sorted unique dates of the panel.
    # Since panel is daily, these are trading days.
    # So each period is ~10 trading days.
    total_trading_days = len(dates)
    # But wait, the last period might be shorter if data ends.
    # Let's count exactly.
    
    empty_days_count = 0
    
    # Map each rebalance date to its coverage
    # rebalance_dates[i] covers dates[idx] to dates[idx+hold_lock_days-1]
    
    # We need the index of each rebalance date in the full `dates` list
    date_to_idx = {d: i for i, d in enumerate(dates)}
    
    for i, reb_date in enumerate(rebalance_dates):
        start_idx = date_to_idx[reb_date]
        if i < len(rebalance_dates) - 1:
            end_idx = date_to_idx[rebalance_dates[i+1]]
        else:
            end_idx = len(dates) # Covers until end
            
        period_len = end_idx - start_idx
        
        # Check if holding was empty
        # holdings[i] corresponds to reb_date
        if len(holdings[i]) == 0:
            empty_days_count += period_len
            
    empty_ratio = empty_days_count / total_trading_days if total_trading_days > 0 else 0.0
    
    return {
        "total_transactions": total_buys + total_sells,
        "avg_turnover_per_rebalance": avg_turnover,
        "empty_days_ratio": empty_ratio,
        "total_buys": total_buys,
        "total_sells": total_sells,
        "n_rebalances": len(rebalance_dates),
        "total_trading_days": total_trading_days,
        "empty_days_count": empty_days_count
    }

def main():
    db_path = os.path.join(root, "data", "cubes.db")
    cache_dir = os.path.join(root, "data", "market_cache")
    out_dir = os.path.join(root, "research", "baseline_v2", "output")
    industry_csv = os.path.join(root, "research", "baseline_v1", "data_delivery", "industry_mapping_v2.csv")
    liquidity_csv = os.path.join(root, "research", "baseline_v1", "data_delivery", "liquidity_daily_v1.csv")
    
    print("Building panel...")
    tmp_csv = os.path.join(out_dir, "temp_panel_for_stats.csv")
    panel = build_rebalance_momentum_panel(
        db_path=db_path,
        cache_dir=cache_dir,
        out_csv=tmp_csv, 
        start_date="2022-01-01",
        end_date="2025-12-31",
        lag_days=14,
        smoothing_days=3,
        factor_mode="rate",
    )
    
    print("Attaching base fields...")
    base = _attach_base_fields(panel, industry_map_csv=industry_csv, liquidity_csv=liquidity_csv)
    
    print("Applying Industry Optimization (Baseline v2)...")
    panel_opt = _assign_other_industry_by_proxy(base)
    
    print("Neutralizing...")
    panel_neu = _industry_neutralize(panel_opt)
    
    print("Applying Liquidity Filter (60%)...")
    panel_final = apply_liquidity_filter(panel_neu, quantile_keep=0.6)
    
    print("Analyzing Transactions...")
    metrics = analyze_transactions(panel_final, hold_lock_days=10, trim_q=0.05)
    
    print("\n" + "="*40)
    print("BASELINE V2 TRADING METRICS (2022-2025)")
    print("="*40)
    print(f"Total Transactions (Buy+Sell): {metrics['total_transactions']}")
    print(f"Average Turnover per Rebalance: {metrics['avg_turnover_per_rebalance']:.2f} stocks")
    print(f"Empty Position Days Ratio: {metrics['empty_days_ratio']:.4%}")
    print("-" * 40)
    print(f"Details:")
    print(f"  Total Buys: {metrics['total_buys']}")
    print(f"  Total Sells: {metrics['total_sells']}")
    print(f"  Total Rebalances: {metrics['n_rebalances']}")
    print(f"  Total Trading Days: {metrics['total_trading_days']}")
    print(f"  Empty Days Count: {metrics['empty_days_count']}")
    print("="*40)
    
    # Save to file
    out_file = os.path.join(out_dir, "baseline_v2_trading_metrics.md")
    with open(out_file, "w", encoding="utf-8") as f:
        f.write("# Baseline v2 交易指标统计 (2022-2025)\n\n")
        f.write(f"- **总交易次数 (Total Transactions)**: {metrics['total_transactions']}\n")
        f.write(f"- **平均每次调仓换股数 (Avg Turnover)**: {metrics['avg_turnover_per_rebalance']:.2f}\n")
        f.write(f"- **空仓天数占比 (Empty Days Ratio)**: {metrics['empty_days_ratio']:.2%}\n")
        f.write("\n## 详细数据\n")
        f.write(f"- 样本区间: 2022-01-01 至 2025-12-31\n")
        f.write(f"- 总交易日: {metrics['total_trading_days']} 天\n")
        f.write(f"- 调仓次数: {metrics['n_rebalances']} 次\n")
        f.write(f"- 空仓天数: {metrics['empty_days_count']} 天\n")
        f.write(f"- 买入总数: {metrics['total_buys']}\n")
        f.write(f"- 卖出总数: {metrics['total_sells']}\n")

if __name__ == "__main__":
    main()
