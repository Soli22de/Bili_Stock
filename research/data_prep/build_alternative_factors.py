"""
Build Alternative Factors from AkShare
=======================================
Factors independent of Xueqiu data, for alpha diversification:

1. money_flow_net: 主力净流入/总成交额 (institutional net inflow ratio)
2. margin_balance_chg: 融资余额变化率 (margin balance momentum)
3. lhb_buy_net: 龙虎榜净买入 (dragon tiger board net buy)

Run: python research/data_prep/build_alternative_factors.py
"""

import os
import sys
import time
import glob

import numpy as np
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
CACHE_DIR = os.path.join(ROOT, "data", "market_cache")


def build_money_flow_factor() -> pd.DataFrame:
    """
    个股主力资金净流入比率 — from akshare stock_individual_fund_flow.
    Batch-fetch for all stocks in stock_data dir.
    """
    import akshare as ak
    stock_dir = os.path.join(ROOT, "data", "stock_data")
    files = glob.glob(os.path.join(stock_dir, "S[HZ]*.csv"))
    # Get unique stock codes
    symbols = set()
    for fp in files:
        sym = os.path.basename(fp).replace(".csv", "").upper()
        if sym.startswith("SH6") or sym.startswith("SZ0") or sym.startswith("SZ3"):
            symbols.add(sym)

    # Sample: pick stocks that appear in cubes
    import sqlite3
    conn = sqlite3.connect(os.path.join(ROOT, "data", "cubes.db"))
    cube_stocks = pd.read_sql_query(
        "SELECT DISTINCT stock_symbol FROM rebalancing_history WHERE status='success'", conn)
    conn.close()
    cube_syms = set(str(s).upper() for s in cube_stocks["stock_symbol"].dropna()
                    if str(s).upper().startswith(("SH", "SZ")))
    # Prioritize cubes stocks, then sample others
    targets = sorted(cube_syms & symbols)
    print(f"  Money flow: {len(targets)} cubes stocks to fetch", flush=True)

    rows = []
    failed = 0
    for i, sym in enumerate(targets):
        code = sym[2:]  # SH600000 → 600000
        market = "sh" if sym.startswith("SH") else "sz"
        try:
            df = ak.stock_individual_fund_flow(stock=code, market=market)
            if df.empty:
                failed += 1
                continue
            # Columns: 日期, 收盘价, 涨跌幅, 主力净流入-净额, 主力净流入-净占比, ...
            out = pd.DataFrame()
            out["date"] = pd.to_datetime(df.iloc[:, 0], errors="coerce").dt.normalize()
            out["stock_symbol"] = sym
            # 主力净流入净占比 (%) — column index 4
            out["money_flow_pct"] = pd.to_numeric(df.iloc[:, 4], errors="coerce")
            # 超大单净流入净占比 (%) — column index 6
            out["big_order_pct"] = pd.to_numeric(df.iloc[:, 6], errors="coerce")
            out = out.dropna(subset=["date"])
            rows.append(out)
        except Exception:
            failed += 1

        if (i + 1) % 50 == 0:
            print(f"    {i+1}/{len(targets)}: {len(rows)} ok, {failed} fail", flush=True)
        # Rate limit
        if (i + 1) % 20 == 0:
            time.sleep(0.5)

    if not rows:
        return pd.DataFrame(columns=["date", "stock_symbol", "money_flow_pct", "big_order_pct"])
    out = pd.concat(rows, ignore_index=True)
    # Rolling 5-day average for smoothing
    out = out.sort_values(["stock_symbol", "date"])
    out["money_flow_5d"] = out.groupby("stock_symbol")["money_flow_pct"].transform(
        lambda s: s.rolling(5, min_periods=2).mean()
    )
    out["big_order_5d"] = out.groupby("stock_symbol")["big_order_pct"].transform(
        lambda s: s.rolling(5, min_periods=2).mean()
    )
    print(f"  Money flow done: {len(out):,} rows, {out['stock_symbol'].nunique()} stocks, failed={failed}", flush=True)
    return out


def main():
    os.makedirs(CACHE_DIR, exist_ok=True)
    t0 = time.time()

    print("Building money flow factor ...", flush=True)
    mf = build_money_flow_factor()
    mf_path = os.path.join(CACHE_DIR, "money_flow_daily.csv")
    mf.to_csv(mf_path, index=False, encoding="utf-8-sig")
    print(f"  Saved → {mf_path}")

    print(f"\nDone in {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
