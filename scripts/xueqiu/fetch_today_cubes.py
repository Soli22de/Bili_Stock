
import requests
import json
import time
import random
import os
import pandas as pd
from datetime import datetime
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

class XueqiuTodayFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://xueqiu.com/",
            "Origin": "https://xueqiu.com",
            "Host": "xueqiu.com",
            "X-Requested-With": "XMLHttpRequest"
        }
        self._init_session()

    def _init_session(self):
        self.session = requests.Session()
        self._init_cookie()

    def _init_cookie(self):
        """Initialize with hardcoded robust cookie"""
        try:
            # Same cookie as in fetch_cube_history.py
            raw_cookie = "acw_tc=3ccdc17e17713034273231509ee15c6e0f10dfb94b43e659b594d9d295e9b5; cookiesu=341771303427330; device_id=c803c3ee03e54a3a75ffde5e3f9b928d; Hm_lvt_1db88642e346389874251b5a1eded6e3=1771303428; HMACCOUNT=10B820C8E54C37A9; smidV2=20260217124348f9f63420da6530e44bccf87b1221265d009b6bb43cc4d88d0; xq_a_token=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xqat=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjYyOTc4MjIyMzgsImlzcyI6InVjIiwiZXhwIjoxNzczODk1Mjg0LCJjdG0iOjE3NzEzMDM0Mzc5NDEsImNpZCI6ImQ5ZDBuNEFadXAifQ.QjcXIPhbZmzCzhl1h8WQDjPFWOwu1P70rITs1UO_JrulikDYlGAevgkLGj4bG1AeQK4P8OKoQHkVzZc_Y1C5mLYxIdtyGUwVmyWhOrvtBYpx-IdWDhfxelt9sUCeyzWPKMQGU6K9dX64b4PfJ2RU1AjkysRXdBaP_lwtIUygOFH_M0GatP31lfX-yVNS5HdhQx7GGZX2QHIOo5JYzV9Fk-kcUW_G17DOqqhA03ZFcfrtYiydjICQPD7pAiaXGWuV4h1dmkk--IMYIL2ihbGMzkiEiAMKvOedAw4yPvPJGu_yMauYC-KLV_E49UlWLOjR_F5X1z4Ey8xVEPst2XEXsw; xq_r_token=075e5a5288f1d196eed9ffa5cc99aca5e136bff8; xq_is_login=1; u=6297822238; is_overseas=0;"

            for cookie in raw_cookie.split('; '):
                if '=' in cookie:
                    key, value = cookie.split('=', 1)
                    self.session.cookies.set(key, value, domain=".xueqiu.com")
            
            self.session.headers.update(self.headers)
            logging.info("Cookie initialized.")

        except Exception as e:
            logging.error(f"Cookie Init Failed: {e}")

    def fetch_today_moves(self, symbol, name):
        url = "https://xueqiu.com/cubes/rebalancing/history.json"
        
        # Set specific headers for this cube
        cube_url = f"https://xueqiu.com/P/{symbol}"
        self.session.headers.update({"Referer": cube_url})

        params = {
            "cube_symbol": symbol,
            "count": 20,
            "page": 1,
            "_": int(time.time() * 1000)
        }
        
        try:
            resp = self.session.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                return []
                
            data = resp.json()
            if "list" not in data or not data["list"]:
                return []
            
            todays_moves = []
            today_str = datetime.now().strftime('%Y-%m-%d')
            # For testing, we might want to check recent days if today is empty (e.g. weekend or early morning)
            # But user asked for "today" (2026-02-24)
            target_date = "2026-02-24" 
            
            for item in data["list"]:
                updated_at = item.get("updated_at", 0)
                time_str = datetime.fromtimestamp(updated_at/1000).strftime('%Y-%m-%d')
                
                if time_str == target_date:
                    signals = self._parse_move(item, name)
                    todays_moves.extend(signals)
            
            return todays_moves

        except Exception as e:
            logging.error(f"Error fetching {symbol}: {e}")
            return []

    def _parse_move(self, item, cube_name):
        signals = []
        updated_at = item["updated_at"]
        time_full = datetime.fromtimestamp(updated_at/1000).strftime('%H:%M:%S')
        
        for hist in item.get("rebalancing_histories", []):
            stock_code = hist.get("stock_symbol")
            stock_name = hist.get("stock_name")
            
            prev_w = hist.get("prev_weight_adjusted")
            if prev_w is None: prev_w = 0.0
            
            target_w = hist.get("target_weight")
            if target_w is None: target_w = 0.0
            
            delta = target_w - prev_w
            
            if abs(delta) < 0.5: 
                continue
                
            action = "BUY" if delta > 0 else "SELL"
            
            signals.append({
                "time": time_full,
                "cube": cube_name,
                "stock": stock_name,
                "code": stock_code,
                "action": action,
                "delta": round(delta, 2),
                "comment": item.get("comment", "") or ""
            })
        return signals

    def run(self):
        cubes_file = "data/massive_cube_list.json"
        if not os.path.exists(cubes_file):
            cubes_file = "data/long_history_cubes.json"
            
        with open(cubes_file, "r", encoding="utf-8") as f:
            cubes = json.load(f)
            
        logging.info(f"Scanning {len(cubes)} cubes for today's moves...")
        
        all_moves = []
        for i, cube in enumerate(cubes):
            symbol = cube["symbol"]
            name = cube["name"]
            
            moves = self.fetch_today_moves(symbol, name)
            if moves:
                all_moves.extend(moves)
                print(f"Found {len(moves)} moves in {name}")
                
            time.sleep(random.uniform(0.5, 1.0))
            
        # Analysis
        print("\n" + "="*50)
        print(f"TODAY'S REBALANCING REPORT (2026-02-24)")
        print("="*50)
        
        if not all_moves:
            print("No rebalancing activities found today.")
            return

        df = pd.DataFrame(all_moves)
        
        # Save to CSV
        csv_path = "data/xueqiu_today_signals.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"\nSaved {len(df)} signals to {csv_path}")
        
        # 1. Hot Stocks (Most Bought)
        buys = df[df['action'] == 'BUY']
        if not buys.empty:
            print("\n[TOP BUYS]")
            top_buys = buys['stock'].value_counts().head(5)
            for stock, count in top_buys.items():
                print(f"{stock}: {count} cubes bought")
                
        # 2. Hot Stocks (Most Sold)
        sells = df[df['action'] == 'SELL']
        if not sells.empty:
            print("\n[TOP SELLS]")
            top_sells = sells['stock'].value_counts().head(5)
            for stock, count in top_sells.items():
                print(f"{stock}: {count} cubes sold")
                
        # 3. Detailed Moves
        print("\n[DETAILED MOVES]")
        # Sort by time
        df = df.sort_values(by='time', ascending=False)
        print(df[['time', 'cube', 'action', 'stock', 'delta']].to_string(index=False))

if __name__ == "__main__":
    fetcher = XueqiuTodayFetcher()
    fetcher.run()
