import akshare as ak
import pandas as pd
import os
import json
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

def get_needed_stocks():
    # 1. Get all stocks from history files
    needed_stocks = set()
    history_dir = "data/history"
    
    for filename in os.listdir(history_dir):
        if not filename.endswith(".json"): continue
        
        with open(os.path.join(history_dir, filename), "r", encoding="utf-8") as f:
            try:
                signals = json.load(f)
                for s in signals:
                    needed_stocks.add(s["stock_code"])
            except:
                pass
                
    # 2. Check which ones are missing
    missing_stocks = []
    for stock_code in needed_stocks:
        if not os.path.exists(os.path.join(DATA_DIR, f"{stock_code}.csv")):
            missing_stocks.append(stock_code)
            
    return missing_stocks

def fetch_stock(stock_code):
    file_path = os.path.join(DATA_DIR, f"{stock_code}.csv")
    
    try:
        # Convert SH600000 to 600000
        if stock_code.startswith("SH") or stock_code.startswith("SZ"):
             code = stock_code[2:]
        else:
             # Skip unknown formats or assume it's just numbers
             code = stock_code
             
        # Skip ETFs (usually 51xxxx or 15xxxx) if Akshare fails, but let's try.
        # Akshare stock_zh_a_hist is for stocks. For ETFs it might fail or return empty.
        
        df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date="20220101", end_date="20260217", adjust="qfq")
        if df.empty:
            # Try ETF interface if stock fails? Or just skip.
            return f"Empty {stock_code}"
            
        df.to_csv(file_path, index=False)
        return f"Fetched {stock_code}"
    except Exception as e:
        return f"Error {stock_code}"

def run():
    missing = get_needed_stocks()
    logging.info(f"Found {len(missing)} missing stocks.")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_stock, stock): stock for stock in missing}
        
        count = 0
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            count += 1
            if count % 20 == 0:
                logging.info(f"Progress: {count}/{len(missing)} - {result}")

    logging.info("Data fetching complete.")

if __name__ == "__main__":
    run()
