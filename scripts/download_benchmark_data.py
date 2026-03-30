
import akshare as ak
import pandas as pd
import os

def download_index_data():
    root = os.getcwd()
    out_dir = os.path.join(root, "data", "cache")
    os.makedirs(out_dir, exist_ok=True)
    
    # 1. CSI 300 (000300)
    print("Downloading CSI 300 (000300) data...")
    try:
        # stock_zh_index_daily_em (EastMoney)
        csi300 = ak.stock_zh_index_daily_em(symbol="sh000300")
        csi300["date"] = pd.to_datetime(csi300["date"])
        csi300 = csi300[csi300["date"] >= "2019-01-01"]
        csi300.set_index("date", inplace=True)
        # Rename cols to match backtrader/our standard
        # open, close, high, low, volume
        # Akshare returns: date, open, close, high, low, volume, amount
        csi300_out = csi300[["open", "close", "high", "low", "volume"]].copy()
        
        out_path = os.path.join(out_dir, "SH000300.csv")
        csi300_out.to_csv(out_path)
        print(f"Saved CSI 300 to {out_path}")
        
    except Exception as e:
        print(f"Error downloading CSI 300: {e}")
        
    # 2. ChiNext ETF (159915)
    # We already have SZ159915.csv in cache, but let's refresh it to ensure full history
    print("Downloading ChiNext ETF (159915) data...")
    try:
        # stock_zh_a_hist (EastMoney) for ETF? No, stock_zh_a_hist is for stocks.
        # For ETF: fund_etf_hist_em
        # NOTE: akshare start_date param needs string YYYYMMDD
        chinext = ak.fund_etf_hist_em(symbol="159915", period="daily", start_date="20190101", end_date="20251231")
        # Columns: 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, ...
        rename_map = {
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume"
        }
        chinext.rename(columns=rename_map, inplace=True)
        chinext["date"] = pd.to_datetime(chinext["date"])
        
        # Check start date
        min_date = chinext["date"].min()
        print(f"ChiNext ETF Min Date: {min_date}")
        
        chinext.set_index("date", inplace=True)
        
        chinext_out = chinext[["open", "close", "high", "low", "volume"]].copy()
        
        out_path = os.path.join(out_dir, "SZ159915_fresh.csv") # Use _fresh to differentiate
        chinext_out.to_csv(out_path)
        print(f"Saved ChiNext ETF to {out_path}")
        
    except Exception as e:
        print(f"Error downloading ChiNext ETF: {e}")
        
    # 3. CSI 300 ETF (510300) as Proxy if index fails
    # Sometimes index data is protected. Let's download 510300 ETF as a backup/alternative.
    print("Downloading CSI 300 ETF (510300) data...")
    try:
        csi300_etf = ak.fund_etf_hist_em(symbol="510300", period="daily", start_date="20190101", end_date="20251231")
        rename_map = {
            "日期": "date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume"
        }
        csi300_etf.rename(columns=rename_map, inplace=True)
        csi300_etf["date"] = pd.to_datetime(csi300_etf["date"])
        
        # Check start date
        min_date = csi300_etf["date"].min()
        print(f"CSI 300 ETF Min Date: {min_date}")
        
        csi300_etf.set_index("date", inplace=True)
        csi300_out = csi300_etf[["open", "close", "high", "low", "volume"]].copy()
        
        out_path = os.path.join(out_dir, "SH510300_fresh.csv")
        csi300_out.to_csv(out_path)
        print(f"Saved CSI 300 ETF to {out_path}")
        
    except Exception as e:
        print(f"Error downloading CSI 300 ETF: {e}")

if __name__ == "__main__":
    download_index_data()
