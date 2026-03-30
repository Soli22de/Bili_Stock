import glob
import os
import sqlite3
from typing import Dict, List, Optional

import pandas as pd

from research.factors.factor_consensus_quality import build_consensus_factor, build_manager_quality


def load_rebalancing_from_db(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        rb = pd.read_sql_query("SELECT cube_symbol, stock_symbol, stock_name, created_at, target_weight, prev_weight_adjusted FROM rebalancing_history", conn)
    finally:
        conn.close()
    return rb


def load_cube_perf(csv_path: str) -> pd.DataFrame:
    return pd.read_csv(csv_path)


def load_cube_meta_from_db(db_path: str) -> pd.DataFrame:
    conn = sqlite3.connect(db_path)
    try:
        cubes = pd.read_sql_query("SELECT symbol, followers_count, created_at FROM cubes", conn)
    finally:
        conn.close()
    cubes["symbol"] = cubes["symbol"].astype(str)
    cubes["followers_count"] = pd.to_numeric(cubes["followers_count"], errors="coerce")
    cubes["created_at"] = pd.to_datetime(cubes["created_at"], errors="coerce").dt.normalize()
    return cubes


def load_price_panel(
    cache_dir: str,
    symbols: List[str],
    start_date: Optional[pd.Timestamp] = None,
    end_date: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    rows = []
    for s in symbols:
        f = os.path.join(cache_dir, f"{s}.csv")
        if not os.path.exists(f):
            continue
        try:
            df = pd.read_csv(f, usecols=["date", "close"])
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
            df["close"] = pd.to_numeric(df["close"], errors="coerce")
            df = df.dropna(subset=["date", "close"])
            if start_date is not None:
                df = df[df["date"] >= start_date]
            if end_date is not None:
                df = df[df["date"] <= end_date]
            df["stock_symbol"] = s
            rows.append(df[["date", "stock_symbol", "close"]])
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(columns=["date", "stock_symbol", "close"])
    out = pd.concat(rows, axis=0, ignore_index=True)
    out = out.sort_values(["stock_symbol", "date"])
    return out


def add_forward_returns(px: pd.DataFrame, horizons: Dict[str, int]) -> pd.DataFrame:
    out = px.copy()
    for label, n in horizons.items():
        out[f"fwd_ret_{label}"] = out.groupby("stock_symbol")["close"].transform(lambda s: s.shift(-n) / s - 1.0)
    return out


def build_factor_panel(
    db_path: str,
    cube_perf_path: str,
    cache_dir: str,
    out_csv: str,
    start_date: str = "2019-01-01",
    end_date: str = "2025-12-31",
    factor_mode: str = "baseline",
) -> pd.DataFrame:
    start_dt = pd.to_datetime(start_date).normalize()
    end_dt = pd.to_datetime(end_date).normalize()
    rb = load_rebalancing_from_db(db_path)
    rb["created_at"] = pd.to_datetime(rb["created_at"], errors="coerce")
    rb = rb.dropna(subset=["created_at"]).copy()
    rb = rb[(rb["created_at"] >= start_dt) & (rb["created_at"] <= end_dt)].copy()
    cube = load_cube_perf(cube_perf_path)
    meta = load_cube_meta_from_db(db_path)
    cube["symbol"] = cube["symbol"].astype(str)
    cube = cube.merge(meta, on="symbol", how="left", suffixes=("", "_meta"))
    if "followers_count_meta" in cube.columns:
        cube["followers_count"] = cube["followers_count"].fillna(cube["followers_count_meta"])
    if "created_at_meta" in cube.columns:
        cube["created_at"] = cube["created_at"].fillna(cube["created_at_meta"])
    quality = build_manager_quality(cube, rb, end_dt, min_closed_trades=20)
    factor = build_consensus_factor(rb, quality, mode=factor_mode, consensus_window_days=7, min_quality_buyers=3)
    symbols = sorted(factor["stock_symbol"].astype(str).unique().tolist()) if not factor.empty else []
    px_end_dt = end_dt + pd.Timedelta(days=40)
    px = load_price_panel(cache_dir, symbols, start_date=start_dt, end_date=px_end_dt)
    px = add_forward_returns(px, {"1w": 5, "2w": 10, "4w": 20})
    factor["date"] = pd.to_datetime(factor["date"], errors="coerce").dt.normalize()
    px["date"] = pd.to_datetime(px["date"], errors="coerce").dt.normalize()
    panel = factor.merge(px, on=["date", "stock_symbol"], how="left")
    panel = panel[(panel["date"] >= start_dt) & (panel["date"] <= end_dt)].copy()
    panel = panel.sort_values(["date", "stock_symbol"]).reset_index(drop=True)
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)
    panel.to_csv(out_csv, index=False, encoding="utf-8-sig")
    return panel


if __name__ == "__main__":
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    db_path = os.path.join(root, "data", "cubes.db")
    cube_perf = os.path.join(root, "data", "cube_performance_2025.csv")
    cache_dir = os.path.join(root, "data", "market_cache")
    out_csv = os.path.join(root, "research", "output", "factor_panel_consensus_quality_baseline.csv")
    panel = build_factor_panel(db_path, cube_perf, cache_dir, out_csv, factor_mode="baseline")
    print(f"rows={len(panel)} output={out_csv}")
