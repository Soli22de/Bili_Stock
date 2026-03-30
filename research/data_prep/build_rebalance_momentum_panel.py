import os
import sqlite3
from typing import List, Optional

import pandas as pd

from research.factors.factor_rebalance_momentum import build_rebalance_momentum_factor


def load_rebalancing_from_db(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        rb = pd.read_sql_query(
            "SELECT cube_symbol, stock_symbol, stock_name, created_at, updated_at, target_weight, prev_weight_adjusted FROM rebalancing_history",
            conn,
        )
    finally:
        conn.close()
    rb["created_at"] = rb["created_at"].where(rb["created_at"].notna(), rb["updated_at"])
    return rb


def load_price_panel(
    cache_dir: str,
    symbols: List[str],
    start_date: Optional[pd.Timestamp] = None,
    end_date: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    rows = []
    
    # Debug: Check if cache dir exists and list files
    if not os.path.exists(cache_dir):
        print(f"Warning: Cache dir {cache_dir} not found.")
        return pd.DataFrame(columns=["date", "stock_symbol", "close"])
        
    for s in symbols:
        s_norm = str(s).strip().upper()
        s_digits = "".join(ch for ch in s_norm if ch.isdigit())
        s6 = s_digits.zfill(6) if s_digits else str(s).zfill(6)
        s5 = s_digits.zfill(5) if s_digits else ""
        candidates = [
            os.path.join(cache_dir, f"{s}.csv"),
            os.path.join(cache_dir, f"{s_norm}.csv"),
            os.path.join(cache_dir, f"{s6}.csv"),
            os.path.join(cache_dir, f"SH{s6}.csv"),
            os.path.join(cache_dir, f"SZ{s6}.csv"),
            os.path.join(cache_dir, f"BJ{s6}.csv"),
            os.path.join(cache_dir, f"{s6}.SH.csv"),
            os.path.join(cache_dir, f"{s6}.SZ.csv"),
            os.path.join(cache_dir, f"{s6}.BJ.csv"),
        ]
        if s5:
            candidates.extend(
                [
                    os.path.join(cache_dir, f"{s5}.HK.csv"),
                    os.path.join(cache_dir, f"HK{s5}.csv"),
                    os.path.join(cache_dir, f"{s5}.csv"),
                ]
            )
        f = next((x for x in candidates if os.path.exists(x)), "")
        if not os.path.exists(f):
            continue
            
        try:
            header = pd.read_csv(f, nrows=0).columns
            use_cols = []
            rename_map = {}
            
            if "日期" in header:
                use_cols.append("日期")
                rename_map["日期"] = "date"
            elif "date" in header:
                use_cols.append("date")
                
            if "收盘" in header:
                use_cols.append("收盘")
                rename_map["收盘"] = "close"
            elif "close" in header:
                use_cols.append("close")
                
            df = pd.read_csv(f, usecols=use_cols)
            df.rename(columns=rename_map, inplace=True)
            
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df = df.dropna(subset=["date", "close"])
            
            df = df.sort_values("date")
            
            if start_date is not None:
                df = df[df["date"] >= start_date]
            if end_date is not None:
                df = df[df["date"] <= end_date]
                
            df["stock_symbol"] = s_norm
            rows.append(df[["date", "stock_symbol", "close"]])
        except Exception:
            continue
            
    if not rows:
        return pd.DataFrame(columns=["date", "stock_symbol", "close"])
    out = pd.concat(rows, axis=0, ignore_index=True)
    out = out.sort_values(["stock_symbol", "date"])
    
    return out


def add_forward_returns(px: pd.DataFrame, horizon_days: int = 10) -> pd.DataFrame:
    out = px.copy()
    # Ensure sorted
    out = out.sort_values(["stock_symbol", "date"])
    
    # Shift(-10) means look 10 rows ahead.
    # If the data is daily trading days, this is exactly 2 weeks (10 trading days).
    # But if there are missing days, it might be > 2 weeks.
    # Assuming daily data is continuous trading days.
    out["fwd_ret_2w"] = out.groupby("stock_symbol")["close"].transform(lambda s: s.shift(-horizon_days) / s - 1.0)
    return out


def build_rebalance_momentum_panel(
    db_path: str,
    cache_dir: str,
    out_csv: str,
    start_date: str = "2010-01-01",
    end_date: str = "2025-12-31",
    lag_days: int = 14,
    smoothing_days: int = 3,
    factor_mode: str = "rate",
) -> pd.DataFrame:
    start_dt = pd.to_datetime(start_date).normalize()
    end_dt = pd.to_datetime(end_date).normalize()
    rb = load_rebalancing_from_db(db_path)
    factor = build_rebalance_momentum_factor(
        rb,
        start_date=start_date,
        end_date=end_date,
        lag_days=lag_days,
        smoothing_days=smoothing_days,
        factor_mode=factor_mode,
    )
    if factor.empty:
        print("Warning: Factor calculation returned empty result.")
        # Ensure 'date' and 'stock_symbol' exist even if empty, to prevent merge errors later
        return pd.DataFrame(columns=["date", "stock_symbol", "net_buy_cube_count", "count_lag", "factor_raw", "factor_z", "close", "fwd_ret_2w"])
        
    symbols = sorted(factor["stock_symbol"].astype(str).unique().tolist()) if not factor.empty else []
    
    # Debug info
    print(f"Generated factor rows: {len(factor)}")
    print(f"Date range in factor: {factor['date'].min()} to {factor['date'].max()}")
    
    px = load_price_panel(cache_dir, symbols, start_date=start_dt, end_date=end_dt + pd.Timedelta(days=40))
    px = add_forward_returns(px, horizon_days=10)
    panel = factor.merge(px, on=["date", "stock_symbol"], how="left")
    panel = panel[(panel["date"] >= start_dt) & (panel["date"] <= end_dt)].copy()
    panel = panel.sort_values(["date", "stock_symbol"]).reset_index(drop=True)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    panel.to_csv(out_csv, index=False, encoding="utf-8-sig")
    return panel


if __name__ == "__main__":
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    db_path = os.path.join(root, "data", "cubes.db")
    cache_dir = os.path.join(root, "data", "market_cache")
    out_csv = os.path.join(root, "research", "output", "factor_panel_rebalance_momentum.csv")
    panel = build_rebalance_momentum_panel(db_path, cache_dir, out_csv, factor_mode="rate")
    print(f"rows={len(panel)} output={out_csv}")
