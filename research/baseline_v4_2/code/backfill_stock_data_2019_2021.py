import glob
import os
import time
from typing import Tuple

import baostock as bs
import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
DATA_DIR = os.path.join(ROOT, "data", "stock_data")
START = os.environ.get("BACKFILL_START", "2010-01-01")
END = os.environ.get("BACKFILL_END", "2025-12-31")
START_TOL_DAYS = int(os.environ.get("BACKFILL_START_TOLERANCE_DAYS", "10"))


def _process(file_path: str) -> Tuple[str, str]:
    name = os.path.basename(file_path)
    symbol = os.path.splitext(name)[0]
    code = symbol[2:]
    try:
        old = pd.read_csv(file_path)
        if "日期" not in old.columns:
            return symbol, "skip_no_date_col"
        old_dates = pd.to_datetime(old["日期"], errors="coerce")
        if old_dates.notna().any():
            min_old = old_dates.min()
            cutoff = pd.Timestamp(START) + pd.Timedelta(days=START_TOL_DAYS)
            if min_old <= cutoff:
                return symbol, f"already_covered_{min_old.strftime('%Y-%m-%d')}"
        bs_code = f"{symbol[:2].lower()}.{code}"
        err = ""
        rs = None
        for _ in range(4):
            try:
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    "date,code,open,close,high,low,volume,amount,pctChg,turn",
                    START,
                    END,
                    "d",
                    "2",
                )
                if rs and rs.error_code == "0":
                    break
            except Exception as e:
                err = str(e)
                time.sleep(0.6)
        if rs is None or rs.error_code != "0":
            return symbol, f"error_{(err or 'baostock_query_failed')[:120]}"
        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())
        new = pd.DataFrame(rows, columns=["日期", "股票代码", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅", "换手率"])
        if new.empty:
            return symbol, "empty_fetch"
        new["股票代码"] = code
        for c in ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅", "换手率"]:
            new[c] = pd.to_numeric(new[c], errors="coerce")
        new["振幅"] = pd.NA
        new["涨跌额"] = pd.NA
        ordered_cols = ["日期", "股票代码", "开盘", "收盘", "最高", "最低", "成交量", "成交额", "振幅", "涨跌幅", "涨跌额", "换手率"]
        new = new[ordered_cols]
        all_df = pd.concat([new, old], axis=0, ignore_index=True)
        all_df["日期"] = pd.to_datetime(all_df["日期"], errors="coerce")
        all_df = all_df.dropna(subset=["日期"]).drop_duplicates(subset=["日期"], keep="first").sort_values("日期")
        all_df["日期"] = all_df["日期"].dt.strftime("%Y-%m-%d")
        all_df.to_csv(file_path, index=False, encoding="utf-8-sig")
        return symbol, f"backfilled_{len(new)}"
    except Exception as e:
        return symbol, f"error_{str(e)[:120]}"


def main():
    lg = bs.login()
    if str(lg.error_code) != "0":
        print("baostock login failed", lg.error_msg)
        return
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.csv")))
    start_idx = int(os.environ.get("START_INDEX", "0"))
    if start_idx > 0:
        files = files[start_idx:]
    max_files = int(os.environ.get("MAX_FILES", "0"))
    if max_files > 0:
        files = files[:max_files]
    print(f"range={START}~{END}")
    print(f"start_tolerance_days={START_TOL_DAYS}")
    print(f"start_index={start_idx}")
    print(f"files={len(files)}")
    stats = {"already_covered": 0, "skip_no_date_col": 0, "empty_fetch": 0, "backfilled": 0, "error": 0}
    detail_stats = {}
    samples = []
    for i, f in enumerate(files, 1):
        symbol, state = _process(f)
        if state.startswith("backfilled_"):
            stats["backfilled"] += 1
        elif state.startswith("already_covered"):
            stats["already_covered"] += 1
        elif state.startswith("error_"):
            stats["error"] += 1
        else:
            stats[state] = stats.get(state, 0) + 1
        detail_stats[state] = detail_stats.get(state, 0) + 1
        if len(samples) < 20 and (state.startswith("backfilled_") or state.startswith("error_")):
            samples.append((symbol, state))
        if i % 50 == 0:
            print(f"progress={i}/{len(files)}")
        time.sleep(0.08)
    print("stats", stats)
    print("detail_stats", detail_stats)
    print("samples", samples)
    bs.logout()


if __name__ == "__main__":
    main()
