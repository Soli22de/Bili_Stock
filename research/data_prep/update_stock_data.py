"""
Update Stock Data — Backfill and refresh per-stock OHLCV CSVs
==============================================================
1. Updates existing files that are stale (last date < target)
2. Downloads missing stocks that appear in cubes.db but have no CSV
3. Uses BaoStock daily k-line data

Run: python research/data_prep/update_stock_data.py
"""

import os
import sys
import glob
import time

import numpy as np
import pandas as pd
import baostock as bs

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
STOCK_DATA_DIR = os.path.join(ROOT, "data", "stock_data")
TARGET_END = "2026-04-11"  # Update to this date


def _bs_code(sym: str) -> str:
    """Convert SH600000 → sh.600000 for baostock."""
    sym = sym.upper()
    if sym.startswith("SH"):
        return f"sh.{sym[2:]}"
    elif sym.startswith("SZ"):
        return f"sz.{sym[2:]}"
    return sym


def _std_code(bs_code: str) -> str:
    """Convert sh.600000 → SH600000."""
    return bs_code.replace(".", "").upper()


def _download_stock(bs_code: str, start: str, end: str) -> pd.DataFrame:
    """Download daily OHLCV from baostock."""
    rs = bs.query_history_k_data_plus(
        bs_code,
        "date,open,close,high,low,volume,amount,pctChg,turn",
        start_date=start,
        end_date=end,
        frequency="d",
        adjustflag="2",  # 前复权
    )
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows, columns=["date", "open", "close", "high", "low", "volume", "amount", "pctChg", "turn"])
    for c in ["open", "close", "high", "low", "volume", "amount", "pctChg", "turn"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _get_stale_files(target_date: str) -> list:
    """Find CSV files whose latest date is before target."""
    target = pd.Timestamp(target_date)
    files = glob.glob(os.path.join(STOCK_DATA_DIR, "S[HZ]*.csv"))
    stale = []
    for fp in files:
        try:
            d = pd.read_csv(fp, encoding="utf-8-sig", usecols=[0], header=0)
            d.columns = ["date"]
            d["date"] = pd.to_datetime(d["date"], errors="coerce")
            latest = d["date"].max()
            if pd.notna(latest) and latest < target:
                sym = os.path.basename(fp).replace(".csv", "").upper()
                stale.append((sym, latest, fp))
        except Exception:
            pass
    return stale


def _get_missing_cubes_stocks() -> list:
    """Find stocks in cubes.db that have no stock_data CSV."""
    import sqlite3
    conn = sqlite3.connect(os.path.join(ROOT, "data", "cubes.db"))
    cube_stocks = pd.read_sql_query(
        "SELECT DISTINCT stock_symbol FROM rebalancing_history WHERE status='success'",
        conn,
    )
    conn.close()

    existing = set(
        os.path.basename(f).replace(".csv", "").upper()
        for f in glob.glob(os.path.join(STOCK_DATA_DIR, "S[HZ]*.csv"))
    )

    missing = []
    for sym in cube_stocks["stock_symbol"].dropna().unique():
        sym = str(sym).upper()
        if sym.startswith(("SH", "SZ")) and sym not in existing:
            missing.append(sym)
    return sorted(missing)


def main():
    os.makedirs(STOCK_DATA_DIR, exist_ok=True)

    lg = bs.login()
    if str(lg.error_code) != "0":
        print(f"BaoStock login failed: {lg.error_msg}")
        return

    # 1. Find stale files
    print("Scanning for stale files ...", flush=True)
    stale = _get_stale_files(TARGET_END)
    print(f"  {len(stale)} files need updating", flush=True)

    # 2. Find missing cubes stocks
    print("Scanning for missing cubes stocks ...", flush=True)
    missing = _get_missing_cubes_stocks()
    print(f"  {len(missing)} stocks in cubes.db have no CSV", flush=True)

    # 3. Update stale files (append new data)
    updated = 0
    failed = 0
    t0 = time.time()
    for i, (sym, latest, fp) in enumerate(stale):
        start = (latest + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        new_data = _download_stock(_bs_code(sym), start, TARGET_END)
        if new_data.empty:
            continue
        try:
            # Read existing, append, deduplicate, save
            old = pd.read_csv(fp, encoding="utf-8-sig", header=0)
            # Map new_data columns to match old format
            old_cols = list(old.columns)
            if len(old_cols) >= 7:
                # Align new data to old column structure
                new_mapped = pd.DataFrame()
                new_mapped[old_cols[0]] = new_data["date"]
                if len(old_cols) >= 12:
                    new_mapped[old_cols[1]] = sym[2:]  # stock code without prefix
                    new_mapped[old_cols[2]] = new_data["open"]
                    new_mapped[old_cols[3]] = new_data["close"]
                    new_mapped[old_cols[4]] = new_data["high"]
                    new_mapped[old_cols[5]] = new_data["low"]
                    new_mapped[old_cols[6]] = new_data["volume"]
                    new_mapped[old_cols[7]] = new_data["amount"]
                    new_mapped[old_cols[8]] = np.nan  # 振幅
                    new_mapped[old_cols[9]] = new_data["pctChg"]
                    new_mapped[old_cols[10]] = np.nan  # 涨跌额
                    new_mapped[old_cols[11]] = new_data["turn"]
                else:
                    new_mapped[old_cols[1]] = new_data["open"]
                    new_mapped[old_cols[2]] = new_data["close"]
                    new_mapped[old_cols[3]] = new_data["high"]
                    new_mapped[old_cols[4]] = new_data["low"]
                    new_mapped[old_cols[5]] = new_data["volume"]
                    new_mapped[old_cols[6]] = new_data["amount"]
                combined = pd.concat([old, new_mapped], ignore_index=True)
                combined = combined.drop_duplicates(subset=[old_cols[0]], keep="last")
                combined.to_csv(fp, index=False, encoding="utf-8-sig")
                updated += 1
        except Exception as e:
            failed += 1

        if (i + 1) % 100 == 0:
            print(f"  Updated {i+1}/{len(stale)} ({updated} ok, {failed} fail, {time.time()-t0:.0f}s)", flush=True)

    print(f"  Stale update done: {updated} updated, {failed} failed", flush=True)

    # 4. Download missing cubes stocks
    downloaded = 0
    t1 = time.time()
    for i, sym in enumerate(missing):
        new_data = _download_stock(_bs_code(sym), "2010-01-01", TARGET_END)
        if new_data.empty:
            continue
        fp = os.path.join(STOCK_DATA_DIR, f"{sym}.csv")
        # Save in 12-column format
        out = pd.DataFrame()
        out["日期"] = new_data["date"]
        out["股票代码"] = sym[2:]
        out["开盘"] = new_data["open"]
        out["收盘"] = new_data["close"]
        out["最高"] = new_data["high"]
        out["最低"] = new_data["low"]
        out["成交量"] = new_data["volume"]
        out["成交额"] = new_data["amount"]
        out["振幅"] = np.nan
        out["涨跌幅"] = new_data["pctChg"]
        out["涨跌额"] = np.nan
        out["换手率"] = new_data["turn"]
        out.to_csv(fp, index=False, encoding="utf-8-sig")
        downloaded += 1

        if (i + 1) % 100 == 0:
            print(f"  Downloaded {i+1}/{len(missing)} ({downloaded} ok, {time.time()-t1:.0f}s)", flush=True)

    print(f"  Missing download done: {downloaded}/{len(missing)} downloaded", flush=True)

    bs.logout()

    print(f"\nTotal time: {time.time()-t0:.0f}s")
    print(f"  Updated stale: {updated}")
    print(f"  Downloaded new: {downloaded}")
    print(f"\nNext: re-run build_data_foundation.py to regenerate liquidity data")


if __name__ == "__main__":
    main()
