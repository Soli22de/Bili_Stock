
import requests
import logging
import time
import random
import os
import sys
import pandas as pd
from datetime import datetime

# Ensure core module is importable
sys.path.append(os.getcwd())
from core.storage import CubeStorage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/fetch_elite_history.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class EliteHistoryFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.storage = CubeStorage()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://xueqiu.com/",
            "Origin": "https://xueqiu.com",
            "Host": "xueqiu.com",
            "X-Requested-With": "XMLHttpRequest"
        }
        
        # Hardcoded Cookie for stability (from previous successful sessions)
        self.base_cookie = "xq_a_token=93155702220d9129525164893706440b84f3c4c9;" 
        self.init_cookie()

    def init_cookie(self):
        """Initialize cookie."""
        try:
            for cookie in self.base_cookie.split('; '):
                if '=' in cookie:
                    key, value = cookie.split('=', 1)
                    self.session.cookies.set(key, value, domain='.xueqiu.com')
            logging.info("Cookie initialized.")
        except Exception as e:
            logging.error(f"Cookie init failed: {e}")

    def fetch_history(self, symbol, count=20):
        """Fetch rebalancing history for a cube."""
        # 1. Visit the cube page first to set session state/cookies
        cube_url = f"https://xueqiu.com/P/{symbol}"
        try:
            self.session.headers.update({
                "Referer": "https://xueqiu.com/cube/center",
                "Host": "xueqiu.com"
            })
            self.session.get(cube_url, timeout=10)
            time.sleep(random.uniform(0.5, 1.0))
        except Exception as e:
            logging.warning(f"Failed to visit page {cube_url}: {e}")

        # 2. Now fetch history
        url = f"https://xueqiu.com/cubes/rebalancing/history.json"
        params = {
            "cube_symbol": symbol,
            "count": count,
            "page": 1,
            "_": int(time.time() * 1000) # Timestamp is often required
        }
        
        # Dynamic Referer is CRITICAL for Xueqiu API
        self.session.headers.update({
            "Referer": cube_url,
            "X-Requested-With": "XMLHttpRequest"
        })
        
        try:
            resp = self.session.get(url, params=params)
            if resp.status_code == 200:
                data = resp.json()
                if "list" in data:
                    return data["list"]
            elif resp.status_code == 400:
                logging.warning(f"400 Error for {symbol}. Session might be invalid or cube is private.")
            else:
                logging.warning(f"Error {resp.status_code} for {symbol}")
                
        except Exception as e:
            logging.error(f"Exception fetching {symbol}: {e}")
        
        return []

    def process_records(self, symbol, raw_records):
        """Transform raw API records to DB schema."""
        processed = []
        for item in raw_records:
            # Each rebalancing item can contain multiple stocks
            # But typically 'rebalancing_histories' contains the details
            if "rebalancing_histories" in item:
                for hist in item["rebalancing_histories"]:
                    processed.append({
                        "cube_symbol": symbol,
                        "stock_symbol": hist.get("stock_symbol"),
                        "stock_name": hist.get("stock_name"),
                        "prev_weight_adjusted": hist.get("prev_weight_adjusted"),
                        "target_weight": hist.get("target_weight"),
                        "price": hist.get("price"),
                        "net_value": item.get("net_value"), # Use net_value from parent
                        "created_at": item.get("created_at"), # Use timestamp from parent
                        "status": item.get("status")
                    })
        return processed

    def run(self, limit=None):
        """Main loop."""
        # Load candidates
        try:
            df = pd.read_csv("data/smart_money_candidates.csv")
            candidates = df['symbol'].tolist()
            if limit:
                candidates = candidates[:limit]
            
            logging.info(f"Loaded {len(candidates)} candidates. Processing...")
        except Exception as e:
            logging.error(f"Failed to load candidates: {e}")
            return

        total = len(candidates)
        for idx, symbol in enumerate(candidates):
            logging.info(f"[{idx+1}/{total}] Fetching history for {symbol}...")
            
            raw_list = self.fetch_history(symbol, count=50) # Get last 50 moves
            if raw_list:
                records = self.process_records(symbol, raw_list)
                if records:
                    self.storage.save_rebalancing_history(records)
                    logging.info(f"Saved {len(records)} records for {symbol}")
                else:
                    logging.info(f"No valid records for {symbol}")
            else:
                logging.info(f"No history found for {symbol}")
            
            # Anti-crawl delay
            time.sleep(random.uniform(2, 4))

if __name__ == "__main__":
    fetcher = EliteHistoryFetcher()
    # Test with top 5 first
    fetcher.run(limit=5)
