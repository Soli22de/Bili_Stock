"""
Build Data Foundation — Fix all missing/stale data sources
==========================================================
Generates the critical data files that the pipeline depends on:

  1. liquidity_daily.csv — daily amount + turnover_rate from stock_data CSVs
  2. industry_mapping.csv — stock → industry L2 from BaoStock
  3. hs300_cache.csv — cached HS300 close + regime (reproducible backtests)
  4. Deduplicate cubes.db entries report

Run: python research/data_prep/build_data_foundation.py
"""

import os
import sys
import glob
import time

import numpy as np
import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
STOCK_DATA_DIR = os.path.join(ROOT, "data", "stock_data")
OUT_DIR = os.path.join(ROOT, "research", "baseline_v1", "data_delivery")


def build_liquidity_from_stock_data() -> pd.DataFrame:
    """
    Build liquidity panel from per-stock OHLCV CSVs.
    Columns: date, stock_symbol, amount, turnover_rate
    """
    print("Building liquidity data from stock_data/ ...", flush=True)
    files = glob.glob(os.path.join(STOCK_DATA_DIR, "S[HZ]*.csv"))
    print(f"  Found {len(files)} stock CSV files", flush=True)

    rows = []
    skipped = 0
    for i, fp in enumerate(files):
        sym = os.path.basename(fp).replace(".csv", "").upper()
        try:
            d = pd.read_csv(fp, encoding="utf-8-sig", header=0)
            ncols = len(d.columns)
            # Two formats:
            #   12-col: 日期,股票代码,开盘,收盘,最高,最低,成交量,成交额,振幅,涨跌幅,涨跌额,换手率
            #   7-col:  日期,开盘,收盘,最高,最低,成交量,成交额
            if ncols >= 12:
                date_col, amount_col, turnover_col = d.columns[0], d.columns[7], d.columns[11]
            elif ncols >= 7:
                date_col, amount_col, turnover_col = d.columns[0], d.columns[6], None
            else:
                skipped += 1
                continue
            out_d = pd.DataFrame()
            out_d["date"] = pd.to_datetime(d[date_col], errors="coerce").dt.normalize()
            out_d["amount"] = pd.to_numeric(d[amount_col], errors="coerce")
            out_d["turnover_rate"] = pd.to_numeric(d[turnover_col], errors="coerce") if turnover_col else np.nan
            out_d["stock_symbol"] = sym
            out_d = out_d.dropna(subset=["date"])
            rows.append(out_d[["date", "stock_symbol", "amount", "turnover_rate"]])
        except Exception:
            skipped += 1
            continue

        if (i + 1) % 500 == 0:
            print(f"  Processed {i+1}/{len(files)} ...", flush=True)

    if not rows:
        print("  WARNING: No liquidity data built!", flush=True)
        return pd.DataFrame(columns=["date", "stock_symbol", "amount", "turnover_rate"])

    out = pd.concat(rows, ignore_index=True)
    out = out.sort_values(["date", "stock_symbol"]).reset_index(drop=True)
    print(f"  Built: {len(out):,} rows, {out['stock_symbol'].nunique()} stocks, "
          f"{out['date'].min().date()} to {out['date'].max().date()}, skipped {skipped}", flush=True)
    return out


def build_industry_from_baostock() -> pd.DataFrame:
    """
    Fetch industry classification for all A-share stocks from BaoStock.
    Columns: stock_symbol_standard, industry_l2
    """
    print("Fetching industry mapping from BaoStock ...", flush=True)
    import baostock as bs
    lg = bs.login()
    if str(lg.error_code) != "0":
        print(f"  WARNING: BaoStock login failed: {lg.error_msg}", flush=True)
        return pd.DataFrame(columns=["stock_symbol_standard", "industry_l2"])

    # Get all stock codes from stock_data directory
    files = glob.glob(os.path.join(STOCK_DATA_DIR, "S[HZ]*.csv"))
    symbols = set()
    for fp in files:
        sym = os.path.basename(fp).replace(".csv", "").upper()
        # Convert SH600000 → sh.600000 for baostock
        if sym.startswith("SH"):
            symbols.add(f"sh.{sym[2:]}")
        elif sym.startswith("SZ"):
            symbols.add(f"sz.{sym[2:]}")

    print(f"  Querying {len(symbols)} stocks ...", flush=True)
    rows = []
    for i, code in enumerate(sorted(symbols)):
        rs = bs.query_stock_industry(code=code)
        while rs.error_code == "0" and rs.next():
            row = rs.get_row_data()
            # row: [updateDate, code, code_name, industry, industryClassification]
            if len(row) >= 4:
                bs_code = row[1]  # e.g., sh.600000
                industry = row[3]  # e.g., 银行
                # Convert back: sh.600000 → SH600000
                std = bs_code.replace(".", "").upper()
                rows.append({"stock_symbol_standard": std, "industry_l2": industry})
        if (i + 1) % 500 == 0:
            print(f"  Queried {i+1}/{len(symbols)} ...", flush=True)

    bs.logout()

    if not rows:
        print("  WARNING: No industry data fetched!", flush=True)
        return pd.DataFrame(columns=["stock_symbol_standard", "industry_l2"])

    out = pd.DataFrame(rows).drop_duplicates("stock_symbol_standard", keep="first")
    out = out.sort_values("stock_symbol_standard").reset_index(drop=True)
    n_industries = out["industry_l2"].nunique()
    print(f"  Built: {len(out)} stocks, {n_industries} industries", flush=True)
    print(f"  Top industries: {out['industry_l2'].value_counts().head(10).to_dict()}", flush=True)
    return out


def cache_hs300(start: str = "2005-01-01", end: str = "2026-12-31") -> pd.DataFrame:
    """
    Cache HS300 close prices + regime classification to CSV for reproducibility.
    """
    print("Caching HS300 data from BaoStock ...", flush=True)
    import baostock as bs
    lg = bs.login()
    if str(lg.error_code) != "0":
        print(f"  WARNING: BaoStock login failed: {lg.error_msg}", flush=True)
        return pd.DataFrame()

    rs = bs.query_history_k_data_plus("sh.000300", "date,close", start, end, "d")
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    bs.logout()

    df = pd.DataFrame(rows, columns=["date", "close"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    df["ret20"] = df["close"] / df["close"].shift(20) - 1.0
    df["hs300_ret20"] = df["ret20"]
    df["regime"] = "震荡"
    df.loc[df["ret20"] > 0.02, "regime"] = "上涨"
    df.loc[df["ret20"] < -0.02, "regime"] = "下跌"

    # Regime distribution
    dist = df["regime"].value_counts(normalize=True) * 100
    print(f"  Rows: {len(df)}, Date range: {df['date'].min().date()} to {df['date'].max().date()}", flush=True)
    for r in ["上涨", "震荡", "下跌"]:
        print(f"  {r}: {dist.get(r, 0):.1f}%", flush=True)

    return df[["date", "close", "ret20", "hs300_ret20", "regime"]]


def check_cubes_duplicates():
    """Report duplicates in cubes.db rebalancing_history."""
    import sqlite3
    print("Checking cubes.db duplicates ...", flush=True)
    conn = sqlite3.connect(os.path.join(ROOT, "data", "cubes.db"))
    dupes = pd.read_sql_query("""
        SELECT cube_symbol, stock_symbol, substr(created_at,1,10) as trade_date,
               COUNT(*) as cnt,
               GROUP_CONCAT(target_weight) as weights
        FROM rebalancing_history
        WHERE status='success'
        GROUP BY cube_symbol, stock_symbol, substr(created_at,1,10)
        HAVING cnt > 1
        ORDER BY cnt DESC
        LIMIT 20
    """, conn)
    conn.close()

    total_dupes = len(dupes)
    print(f"  Duplicate (cube+stock+date) groups: {total_dupes}", flush=True)
    if not dupes.empty:
        print(f"  Top duplicates:", flush=True)
        print(dupes.head(10).to_string(index=False), flush=True)
    return dupes


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    t0 = time.time()

    # 1. Liquidity
    liq = build_liquidity_from_stock_data()
    liq_path = os.path.join(OUT_DIR, "liquidity_daily_v1.csv")
    liq.to_csv(liq_path, index=False, encoding="utf-8-sig")
    print(f"  Saved → {liq_path}\n", flush=True)

    # 2. Industry mapping
    ind = build_industry_from_baostock()
    ind_path = os.path.join(OUT_DIR, "industry_mapping_v2.csv")
    ind.to_csv(ind_path, index=False, encoding="utf-8-sig")
    print(f"  Saved → {ind_path}\n", flush=True)

    # 3. HS300 cache
    hs = cache_hs300()
    hs_path = os.path.join(ROOT, "data", "market_cache", "hs300_daily_cache.csv")
    os.makedirs(os.path.dirname(hs_path), exist_ok=True)
    hs.to_csv(hs_path, index=False, encoding="utf-8-sig")
    print(f"  Saved → {hs_path}\n", flush=True)

    # 4. Cubes duplicates report
    check_cubes_duplicates()

    print(f"\nDone in {time.time()-t0:.0f}s", flush=True)
    print(f"\nData files created:")
    print(f"  {liq_path}")
    print(f"  {ind_path}")
    print(f"  {hs_path}")
    print(f"\nNext: re-run backtests with real data foundations.")


if __name__ == "__main__":
    main()
