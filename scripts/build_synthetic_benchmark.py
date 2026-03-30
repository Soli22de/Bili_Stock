import os
import pandas as pd


def _load_equity_from_csv(file_path: str, start_date: str, end_date: str) -> pd.Series:
    df = pd.read_csv(file_path)
    cols = set(df.columns)
    if "date" not in cols and "日期" in cols:
        df = df.rename(columns={"日期": "date"})
    if "close" not in cols and "收盘" in cols:
        df = df.rename(columns={"收盘": "close"})
    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["date", "close"]).sort_values("date")
    df = df[(df["date"] >= pd.to_datetime(start_date)) & (df["date"] <= pd.to_datetime(end_date))]
    s = df.set_index("date")["close"]
    s = s / s.iloc[0]
    s.name = "equity"
    return s


def build_benchmark_index():
    root = os.getcwd()
    cache_dir = os.path.join(root, "data", "cache")
    out_dir = os.path.join(root, "research", "baseline_v6_1", "output")
    os.makedirs(out_dir, exist_ok=True)

    start_date = "2019-01-01"
    end_date = "2025-12-31"

    csi300_file = os.path.join(cache_dir, "SH000300.csv")
    chinext_file = os.path.join(cache_dir, "SZ159915_fresh.csv")
    if not os.path.exists(chinext_file):
        chinext_file = os.path.join(cache_dir, "SZ159915.csv")

    if not os.path.exists(csi300_file):
        raise FileNotFoundError(f"Missing CSI300 source file: {csi300_file}")
    if not os.path.exists(chinext_file):
        raise FileNotFoundError(f"Missing ChiNext source file: {chinext_file}")

    csi300_equity = _load_equity_from_csv(csi300_file, start_date, end_date)
    chinext_equity = _load_equity_from_csv(chinext_file, start_date, end_date)

    full_idx = pd.date_range(start_date, end_date, freq="D")
    csi300_equity = csi300_equity.reindex(full_idx).ffill().bfill()
    chinext_equity = chinext_equity.reindex(full_idx).ffill().bfill()
    csi300_equity = csi300_equity / csi300_equity.iloc[0]
    chinext_equity = chinext_equity / chinext_equity.iloc[0]

    csi300_equity.name = "CSI300_Equity"
    chinext_equity.name = "ChiNext_Equity"
    csi300_equity.index.name = "date"
    chinext_equity.index.name = "date"

    csi300_out = os.path.join(out_dir, "csi300_benchmark_2019_2025.csv")
    chinext_out = os.path.join(out_dir, "chinext_benchmark_2019_2025.csv")
    csi300_equity.to_csv(csi300_out, header=True)
    chinext_equity.to_csv(chinext_out, header=True)

    print(f"Saved CSI300 benchmark: {csi300_out}")
    print(f"Saved ChiNext benchmark: {chinext_out}")
    print(f"CSI300 start: {csi300_equity.index.min()}, raw source: {csi300_file}")
    print(f"ChiNext start: {chinext_equity.index.min()}, raw source: {chinext_file}")


if __name__ == "__main__":
    build_benchmark_index()
