import requests
import json
import time
import random
import os
import pandas as pd
from datetime import datetime
import logging
import sys

# Ensure core module is importable
sys.path.append(os.getcwd())
from core.storage import CubeStorage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/fetch_cube_history.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class XueqiuHistoryFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://xueqiu.com/",
            "Origin": "https://xueqiu.com",
            "Host": "xueqiu.com",
            "X-Requested-With": "XMLHttpRequest"
        }
        self.data_dir = "data/history"
        os.makedirs(self.data_dir, exist_ok=True) # Ensure dir exists
        self.min_timestamp = datetime(2022, 1, 1).timestamp() * 1000
        self.storage = CubeStorage()
        
        self._init_session()

    def _init_session(self):
        self.session = requests.Session()
        self._init_cookie()

    def _init_cookie(self):
        """Initialize with hardcoded robust cookie from xueqiu_spy.py"""
        try:
            logging.info("Initializing Xueqiu Cookie (Using Hardcoded Token)...")
            
            # Use the robust cookie from xueqiu_spy.py which is known to work for history
            # This cookie seems to be valid until 2026-02-27 (based on creation date 2026-02-17 + 10 days typical?)
            # Actually, xq_a_token usually lasts longer.
            raw_cookie = "acw_tc=3ccdc17e17713034273231509ee15c6e0f10dfb94b43e659b594d9d295e9b5; cookiesu=341771303427330; device_id=c803c3ee03e54a3a75ffde5e3f9b928d; Hm_lvt_1db88642e346389874251b5a1eded6e3=1771303428; HMACCOUNT=10B820C8E54C37A9; smidV2=20260217124348f9f63420da6530e44bccf87b1221265d009b6bb43cc4d88d0; xq_a_token=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xqat=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjYyOTc4MjIyMzgsImlzcyI6InVjIiwiZXhwIjoxNzczODk1Mjg0LCJjdG0iOjE3NzEzMDM0Mzc5NDEsImNpZCI6ImQ5ZDBuNEFadXAifQ.QjcXIPhbZmzCzhl1h8WQDjPFWOwu1P70rITs1UO_JrulikDYlGAevgkLGj4bG1AeQK4P8OKoQHkVzZc_Y1C5mLYxIdtyGUwVmyWhOrvtBYpx-IdWDhfxelt9sUCeyzWPKMQGU6K9dX64b4PfJ2RU1AjkysRXdBaP_lwtIUygOFH_M0GatP31lfX-yVNS5HdhQx7GGZX2QHIOo5JYzV9Fk-kcUW_G17DOqqhA03ZFcfrtYiydjICQPD7pAiaXGWuV4h1dmkk--IMYIL2ihbGMzkiEiAMKvOedAw4yPvPJGu_yMauYC-KLV_E49UlWLOjR_F5X1z4Ey8xVEPst2XEXsw; xq_r_token=075e5a5288f1d196eed9ffa5cc99aca5e136bff8; xq_is_login=1; u=6297822238; is_overseas=0;"

            for cookie in raw_cookie.split('; '):
                if '=' in cookie:
                    key, value = cookie.split('=', 1)
                    self.session.cookies.set(key, value, domain=".xueqiu.com")
            
            self.session.headers.update(self.headers)
            
            # Log status
            cookies = self.session.cookies.get_dict()
            logging.info(f"Cookies obtained: {list(cookies.keys())}")
            if "xq_a_token" in cookies:
                logging.info(f"Cookie Success: xq_a_token found: {cookies['xq_a_token'][:10]}...")
            else:
                logging.warning(f"Cookie Warning: Missing xq_a_token.")
                
        except Exception as e:
            logging.error(f"Cookie Init Failed: {e}")

    def fetch_history(self, symbol):
        url = "https://xueqiu.com/cubes/rebalancing/history.json"
        all_signals = []
        page = 1
        
        logging.info(f"Fetching history for {symbol}...")
        
        # Set specific headers for this cube
        cube_url = f"https://xueqiu.com/P/{symbol}"
        
        # Reset headers to base state
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Host": "xueqiu.com",
            "Referer": cube_url,
            "Origin": "https://xueqiu.com",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
        })

        # Visit cube page first to simulate user behavior and get potential cookies
        try:
            self.session.get(cube_url, timeout=10)
            time.sleep(random.uniform(1.0, 2.0))
        except Exception as e:
            logging.warning(f"Failed to visit cube page {cube_url}: {e}")
        
        # Test NAV endpoint first to check if block is specific
        try:
            nav_url = f"https://xueqiu.com/cubes/nav_daily/all.json?cube_symbol={symbol}&since=1600000000000"
            nav_resp = self.session.get(nav_url, headers=self.session.headers, timeout=10)
            logging.info(f"NAV Fetch Status: {nav_resp.status_code}")
        except Exception as e:
            logging.warning(f"NAV Fetch Failed: {e}")

        while True:
            params = {
                "cube_symbol": symbol,
                "count": 20,
                "page": page,
                "_": int(time.time() * 1000)
            }
            
            try:
                resp = self.session.get(url, params=params)
                
                # Retry logic for 400 errors
                if resp.status_code == 400:
                    logging.warning(f"Got 400 for {symbol} page {page}. Sleeping for 30s before retry...")
                    time.sleep(30 + random.uniform(1, 5))
                    # Re-init cookie might help
                    self._init_session()
                    
                    try:
                        resp = self.session.get(url, params=params)
                    except Exception as e:
                        logging.error(f"Retry failed: {e}")
                        break

                if resp.status_code != 200:
                    logging.warning(f"Failed to fetch page {page} for {symbol}: {resp.status_code}")
                    break
                    
                data = resp.json()
                if "list" not in data or not data["list"]:
                    logging.info(f"No more data for {symbol}.")
                    break
                
                reached_limit = False
                for item in data["list"]:
                    updated_at = item.get("updated_at", 0)
                    
                    if updated_at < self.min_timestamp:
                        reached_limit = True
                        break
                        
                    # Parse signals
                    signals = self._parse_move(item)
                    all_signals.extend(signals)
                
                if reached_limit:
                    logging.info(f"Reached time limit (2022-01-01) for {symbol}.")
                    break
                    
                page += 1
                # Increased delay for stealth mode
                time.sleep(random.uniform(2.0, 4.0)) 
                
            except Exception as e:
                logging.error(f"Error fetching page {page} for {symbol}: {e}")
                break
                
        return all_signals

    def _parse_move(self, item):
        signals = []
        updated_at = item["updated_at"]
        time_str = datetime.fromtimestamp(updated_at/1000).strftime('%Y-%m-%d %H:%M:%S')
        
        for hist in item.get("rebalancing_histories", []):
            stock_code = hist.get("stock_symbol")
            stock_name = hist.get("stock_name")
            
            prev_w = hist.get("prev_weight_adjusted")
            if prev_w is None: prev_w = 0.0
            
            target_w = hist.get("target_weight")
            if target_w is None: target_w = 0.0
            
            price = hist.get("price")
            delta = target_w - prev_w
            
            if abs(delta) < 0.5: # Ignore small changes
                continue
                
            action = "BUY" if delta > 0 else "SELL"
            
            signals.append({
                "time": time_str,
                "timestamp": updated_at,
                "cube_symbol": item.get("cube_symbol", ""),
                "stock_code": stock_code,
                "stock_name": stock_name,
                "action": action,
                "delta": round(delta, 2),
                "price": price,
                "comment": item.get("comment", "") or ""
            })
        return signals

    def run(self, input_file="data/long_history_cubes.json"):
        # Load candidates
        if not os.path.exists(input_file):
            logging.error(f"Input file not found: {input_file}")
            return
            
        with open(input_file, "r", encoding="utf-8") as f:
            cubes = json.load(f)
            
        logging.info(f"Loaded {len(cubes)} cubes from {input_file}.")
        
        processed_count = 0
        for i, cube in enumerate(cubes):
            # Reset session every 5 requests to avoid tracking/blocking
            if i > 0 and i % 5 == 0:
                logging.info("Resetting session to avoid blocking...")
                self._init_session()
                # Increased cool-down for stealth mode
                time.sleep(random.uniform(10.0, 20.0))

            symbol = cube["symbol"]
            name = cube["name"]
            
            file_path = os.path.join(self.data_dir, f"{symbol}.json")
            
            # Check if exists in DB to skip (Robust Check)
            # Or just rely on file existence
            if os.path.exists(file_path):
                logging.info(f"History for {name} ({symbol}) already exists. Skipping.")
                continue
                
            signals = self.fetch_history(symbol)
            
            if signals:
                # Save to JSON
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(signals, f, ensure_ascii=False, indent=2)
                
                # Save to DB (Import)
                try:
                    self.storage.save_rebalancing_history(signals)
                    logging.info(f"Saved {len(signals)} signals for {name} ({symbol}) to DB.")
                except Exception as e:
                    logging.error(f"Failed to save to DB for {symbol}: {e}")
                    
                processed_count += 1
            else:
                logging.warning(f"No signals found for {name} ({symbol}).")
                
            time.sleep(random.uniform(3.0, 6.0))

if __name__ == "__main__":
    fetcher = XueqiuHistoryFetcher()
    input_file = sys.argv[1] if len(sys.argv) > 1 else "data/long_history_cubes.json"
    fetcher.run(input_file)
