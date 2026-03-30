import akshare as ak
import pandas as pd
import os
import time
import concurrent.futures
import logging
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DATA_DIR = "data/stock_data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def normalize_symbol(stock_code):
    s = str(stock_code).strip().upper()
    if s.endswith(".HK") or s.startswith("HK"):
        hk = s.replace(".HK", "").replace("HK", "")
        hk = re.sub(r"[^0-9]", "", hk).zfill(5)
        return f"{hk}.HK", "HK", hk
    raw = s.replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
    raw = re.sub(r"[^0-9]", "", raw).zfill(6)
    if raw.startswith(("6", "5", "9")):
        return f"{raw}.SH", "A", raw
    if raw.startswith(("0", "2", "3")):
        return f"{raw}.SZ", "A", raw
    if raw.startswith(("4", "8")):
        return f"{raw}.BJ", "A", raw
    return f"{raw}.SZ", "A", raw

def fetch_stock(stock_code):
    std_symbol, market, source_symbol = normalize_symbol(stock_code)
    file_path = os.path.join(DATA_DIR, f"{std_symbol}.csv")
    if os.path.exists(file_path):
        return f"Skipped {std_symbol}"

    try:
        if market == "HK":
            df = ak.stock_hk_hist(symbol=source_symbol, period="daily", start_date="20100101", end_date="20251231", adjust="qfq")
        else:
            df = ak.stock_zh_a_hist(symbol=source_symbol, period="daily", start_date="20100101", end_date="20251231", adjust="qfq")
        if df.empty:
            return f"Empty {std_symbol}"
            
        df.to_csv(file_path, index=False)
        return f"Fetched {std_symbol}"
    except Exception as e:
        return f"Error {std_symbol}: {str(e)}"

def run():
    with open("data/long_history_stocks.txt", "r") as f:
        stocks = [line.strip() for line in f if line.strip()]
        
    logging.info(f"Fetching data for {len(stocks)} stocks...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_stock, stock): stock for stock in stocks}
        
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            result = future.result()
            if i % 10 == 0:
                logging.info(f"Progress: {i}/{len(stocks)} - {result}")

    logging.info("Data fetching complete.")

if __name__ == "__main__":
    run()
