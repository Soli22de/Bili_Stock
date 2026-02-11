import pandas as pd
import sys
import os
import time
from datetime import datetime, timedelta

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from core.data_provider import DataProvider

def download_history_data(days_back=30):
    """
    批量下载历史分钟数据进行预热
    """
    print(f"=== Starting Data Pre-fetch (Last {days_back} days) ===")
    
    # 1. Load Signal List (Get target stocks)
    if not os.path.exists(config.SIGNALS_CSV):
        print(f"Error: {config.SIGNALS_CSV} not found.")
        return
        
    df_signals = pd.read_csv(config.SIGNALS_CSV)
    unique_codes = df_signals['stock_code'].astype(str).str.zfill(6).unique()
    
    print(f"Target Stocks: {len(unique_codes)}")
    
    provider = DataProvider()
    
    # Generate date range (Market days only ideally, but we iterate all for simplicity)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
    
    date_range = pd.date_range(start=start_date, end=end_date, freq='B') # Business days
    
    total_tasks = len(unique_codes) * len(date_range)
    print(f"Estimated Requests: {total_tasks} (This may take a while...)")
    
    processed = 0
    success = 0
    
    for code in unique_codes:
        print(f"\nProcessing {code}...")
        for date in date_range:
            date_str = date.strftime('%Y-%m-%d')
            
            # Check cache first (Implicit in get_minute_data)
            # We force fetch by calling get_minute_data. 
            # If cached, it returns fast. If not, it fetches and caches.
            
            # Note: BaoStock requires login (Provider handles it)
            # AkShare is free.
            
            try:
                # Check if cache exists directly to avoid spamming logs
                if provider.cache.has_data(code, date_str, period=5):
                    # print(f"  [Skip] {date_str} already cached.")
                    continue
                
                print(f"  [Fetch] Downloading {date_str}...")
                df = provider.get_minute_data(code, date_str)
                
                if df is not None and not df.empty:
                    success += 1
                
                # Rate limit politeness
                time.sleep(0.1) 
                
            except Exception as e:
                print(f"  [Error] {date_str}: {e}")
                
            processed += 1
            
    print("\n=== Data Pre-fetch Complete ===")
    print(f"Total Processed: {processed}")
    print(f"Successfully Cached: {success}")

if __name__ == "__main__":
    download_history_data(days_back=10) # Default to 10 days for test
