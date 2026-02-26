
import sqlite3
import requests
import pandas as pd
import logging
import time
import random
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class HistoryFetcherRetry:
    def __init__(self, db_path="data/cubes.db"):
        self.db_path = db_path
        self.session = requests.Session()
        
        # Use the long, robust cookie from xueqiu_spy.py
        self.raw_cookie = "acw_tc=3ccdc17e17713034273231509ee15c6e0f10dfb94b43e659b594d9d295e9b5; cookiesu=341771303427330; device_id=c803c3ee03e54a3a75ffde5e3f9b928d; Hm_lvt_1db88642e346389874251b5a1eded6e3=1771303428; HMACCOUNT=10B820C8E54C37A9; smidV2=20260217124348f9f63420da6530e44bccf87b1221265d009b6bb43cc4d88d0; xq_a_token=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xqat=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjYyOTc4MjIyMzgsImlzcyI6InVjIiwiZXhwIjoxNzczODk1Mjg0LCJjdG0iOjE3NzEzMDM0Mzc5NDEsImNpZCI6ImQ5ZDBuNEFadXAifQ.QjcXIPhbZmzCzhl1h8WQDjPFWOwu1P70rITs1UO_JrulikDYlGAevgkLGj4bG1AeQK4P8OKoQHkVzZc_Y1C5mLYxIdtyGUwVmyWhOrvtBYpx-IdWDhfxelt9sUCeyzWPKMQGU6K9dX64b4PfJ2RU1AjkysRXdBaP_lwtIUygOFH_M0GatP31lfX-yVNS5HdhQx7GGZX2QHIOo5JYzV9Fk-kcUW_G17DOqqhA03ZFcfrtYiydjICQPD7pAiaXGWuV4h1dmkk--IMYIL2ihbGMzkiEiAMKvOedAw4yPvPJGu_yMauYC-KLV_E49UlWLOjR_F5X1z4Ey8xVEPst2XEXsw; xq_r_token=075e5a5288f1d196eed9ffa5cc99aca5e136bff8; xq_is_login=1; u=6297822238; is_overseas=0;"
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://xueqiu.com/",
            "Origin": "https://xueqiu.com",
            "Host": "xueqiu.com",
            "X-Requested-With": "XMLHttpRequest"
        }
        
        self._init_session()

    def _init_session(self):
        for cookie in self.raw_cookie.split('; '):
            if '=' in cookie:
                key, value = cookie.split('=', 1)
                self.session.cookies.set(key, value, domain=".xueqiu.com")
        self.session.headers.update(self.headers)
        logging.info("Session initialized with robust cookie.")

    def run(self):
        # 1. Get targets
        conn = sqlite3.connect(self.db_path)
        # Get top gainers (Top 300)
        try:
            # Load all cubes to do tiered selection
            df = pd.read_sql_query("SELECT symbol, total_gain, followers_count, monthly_gain FROM cubes", conn)
            
            # Logic: Top 50 Legends, Top 200 Hidden Gems, Top 50 Rising Stars
            legends = df[(df['total_gain'] > 50) & (df['followers_count'] > 1000)].sort_values('total_gain', ascending=False).head(50)
            hidden_gems = df[(df['total_gain'] > 30) & (df['followers_count'] < 500)].sort_values('total_gain', ascending=False).head(200)
            rising_stars = df[(df['monthly_gain'] > 10)].sort_values('monthly_gain', ascending=False).head(50)
            
            combined = pd.concat([legends, hidden_gems, rising_stars]).drop_duplicates(subset=['symbol'])
            targets = combined['symbol'].tolist()
            logging.info(f"Selection: {len(legends)} Legends, {len(hidden_gems)} Hidden Gems, {len(rising_stars)} Rising Stars. Total Unique: {len(targets)}")
            
        except Exception as e:
            logging.error(f"Failed to get targets: {e}")
            targets = []
        conn.close()
        
        logging.info(f"Targeting {len(targets)} cubes for history fetch...")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        total_saved = 0
        for i, symbol in enumerate(targets):
            logging.info(f"[{i+1}/{len(targets)}] Fetching {symbol}...")
            
            # 2. Fetch history (try)
            try:
                # Need to implement fetch_history logic properly
                # Visit page first
                try:
                    self.session.get(f"https://xueqiu.com/P/{symbol}", headers={"User-Agent": self.headers["User-Agent"]}, timeout=5)
                    time.sleep(random.uniform(0.5, 1.0))
                except:
                    pass
                
                # Call API
                url = "https://xueqiu.com/cubes/rebalancing/history.json"
                params = {
                    "cube_symbol": symbol,
                    "count": 20,
                    "page": 1,
                    "_": int(time.time() * 1000)
                }
                self.session.headers.update({"Referer": f"https://xueqiu.com/P/{symbol}"})
                
                resp = self.session.get(url, params=params, timeout=10)
                
                if resp.status_code == 200:
                    data = resp.json()
                    items = data.get("list", [])
                    
                    if items:
                        for item in items:
                            if "rebalancing_histories" not in item: continue
                            
                            created_at_ts = item.get("created_at", 0)
                            if created_at_ts > 1000000000000:
                                created_at = datetime.fromtimestamp(created_at_ts/1000)
                            else:
                                created_at = datetime.fromtimestamp(created_at_ts)
                            
                            for hist in item["rebalancing_histories"]:
                                cursor.execute('''
                                    INSERT OR IGNORE INTO rebalancing_history (
                                        cube_symbol, stock_symbol, stock_name,
                                        prev_weight_adjusted, target_weight,
                                        price, net_value, created_at, updated_at, status
                                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ''', (
                                    symbol,
                                    hist.get("stock_symbol"),
                                    hist.get("stock_name", ""),
                                    hist.get("prev_weight_adjusted", 0.0),
                                    hist.get("target_weight", 0.0),
                                    hist.get("price", 0.0),
                                    item.get("net_value", 0.0),
                                    created_at,
                                    datetime.now(),
                                    item.get("status", "success")
                                ))
                                total_saved += 1
                        conn.commit()
                        logging.info(f"Saved records for {symbol}.")
                    else:
                        logging.warning(f"No history items for {symbol}")
                elif resp.status_code == 403 or resp.status_code == 400:
                    logging.warning(f"Access Denied (403/400) for {symbol}. Cookie might be invalid.")
                    # Break loop if cookie is invalid to avoid ban?
                    # Maybe try a few more.
                else:
                    logging.warning(f"Error {resp.status_code} for {symbol}")
                    
            except Exception as e:
                logging.error(f"Error processing {symbol}: {e}")
            
            time.sleep(random.uniform(1, 2))
            
        conn.close()
        logging.info(f"Done. Total records saved: {total_saved}")

if __name__ == "__main__":
    fetcher = HistoryFetcherRetry()
    fetcher.run()
