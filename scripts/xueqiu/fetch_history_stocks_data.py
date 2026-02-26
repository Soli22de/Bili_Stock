import akshare as ak
import pandas as pd
import os
import time
import concurrent.futures
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DATA_DIR = "data/stock_data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)

def fetch_stock(stock_code):
    file_path = os.path.join(DATA_DIR, f"{stock_code}.csv")
    if os.path.exists(file_path):
        # Check if file is recent enough?
        # For now, just skip if exists to save time, unless user wants refresh
        return f"Skipped {stock_code}"

    try:
        # Convert SH600000 to 600000
        code = stock_code[2:]
        df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date="20220101", end_date="20260217", adjust="qfq")
        if df.empty:
            return f"Empty {stock_code}"
            
        df.to_csv(file_path, index=False)
        return f"Fetched {stock_code}"
    except Exception as e:
        return f"Error {stock_code}: {str(e)}"

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
