import requests
import json
import time
import random
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/fetch_candidates.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class XueqiuCandidateFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://xueqiu.com/",
            "Origin": "https://xueqiu.com",
            "Host": "xueqiu.com",
            "X-Requested-With": "XMLHttpRequest"
        }
        
        # 2022-01-01 timestamp in milliseconds
        self.MAX_CREATED_AT = 1640995200000 
        
        self._init_cookie()

    def _init_cookie(self):
        # Using the working cookie from previous attempts
        raw_cookie = "acw_tc=3ccdc17e17713034273231509ee15c6e0f10dfb94b43e659b594d9d295e9b5; cookiesu=341771303427330; device_id=c803c3ee03e54a3a75ffde5e3f9b928d; Hm_lvt_1db88642e346389874251b5a1eded6e3=1771303428; HMACCOUNT=10B820C8E54C37A9; smidV2=20260217124348f9f63420da6530e44bccf87b1221265d009b6bb43cc4d88d0; xq_a_token=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xqat=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjYyOTc4MjIyMzgsImlzcyI6InVjIiwiZXhwIjoxNzczODk1Mjg0LCJjdG0iOjE3NzEzMDM0Mzc5NDEsImNpZCI6ImQ5ZDBuNEFadXAifQ.QjcXIPhbZmzCzhl1h8WQDjPFWOwu1P70rITs1UO_JrulikDYlGAevgkLGj4bG1AeQK4P8OKoQHkVzZc_Y1C5mLYxIdtyGUwVmyWhOrvtBYpx-IdWDhfxelt9sUCeyzWPKMQGU6K9dX64b4PfJ2RU1AjkysRXdBaP_lwtIUygOFH_M0GatP31lfX-yVNS5HdhQx7GGZX2QHIOo5JYzV9Fk-kcUW_G17DOqqhA03ZFcfrtYiydjICQPD7pAiaXGWuV4h1dmkk--IMYIL2ihbGMzkiEiAMKvOedAw4yPvPJGu_yMauYC-KLV_E49UlWLOjR_F5X1z4Ey8xVEPst2XEXsw; xq_r_token=075e5a5288f1d196eed9ffa5cc99aca5e136bff8; xq_is_login=1; u=6297822238; is_overseas=0; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1771303439; .thumbcache_f24b8bbe5a5934237bbc0eda20c1b6e7=FHDhoCv08W8qq7LCvhI4wVC5fmdISE0i9YLaxXcdid1A+jiQmDbfysCKpXxIiCAi+YjiH73oKrrpKKQWDDHKEQ%3D%3D; ssxmod_itna=1-YqGO7KY5AIejOjDhx_oxKupDp2bb4DXDUkqiQGgDYq7=GFKDCgYIRr=m4BK1fq_qDAgDXGW46Y7tDlrOrYDSxD=HDK4GThCPeDt_jEw04vW8tiH_w9YiAoM9WYeC2bU53yKUq9UgXU=qqjWzhQiYDCPDExGk57A=hDiiHx0rD0eDPxDYDG4Do6YDn6xDjxDd84SAmRoDbxi3E4GCo__L24DFkAopR3xD0oa_HGbveDDzXovqrSijDiW_RU2Phz0uWihID753Dlc4zTGIV/1BeGSuHvZLdroDXZtDvrSUGzrQ1f8EXK_wdgAiY_x0PmnmM0peeeGBDpihhee5AwMAqpjG4jD2j51mqx9rrDDA4hqt_YtiYQ_q4xlIHZbc_0HY37eFR4EuxF_KREVn2D9u4CDHiwVRxNlxi4biB2zib4zGbwGhK74eD; ssxmod_itna2=1-YqGO7KY5AIejOjDhx_oxKupDp2bb4DXDUkqiQGgDYq7=GFKDCgYIRr=m4BK1fq_qDAgDXGW46Y7YDiPbH_C=bD7pGeDGunvDBw0ne5IICLLLS4MS5auxyU7fwE9KjpRc0/mTCk_GT1gvOkPvdWQ0HdW5xSlHi587_QIFatncWzOnxmP5IeiKw7zPOOgcxna9sWQcxWiI=EU8kEE8E84uhPUf6WA8U5jdh53eOXxwFn=iXgE0FxpahW=jqNNG08zDhWl9Xka0sO26h=KkuVOZGITUmh21cwyUm4y9t0R6HgfT_BzAq6hIfVAkEuYcEz62NKwkWohuMz/oCYm18_FRhDz4KXgha4i/gid7Hxxhvqazg36h3p3x1M=Dnsb4GnhWLbpqGr5RmmxNNCH53OmLif8DGuFzurVl5L8k9RWTODW23UbvLbWUBwT2mW_5USb0EaWQO_iehGDdGj6OrM24dYe57jU66MA5ViYxh5tYjUPXeiDD"
        
        self.session.headers.update(self.headers)
        for cookie in raw_cookie.split('; '):
            if '=' in cookie:
                key, value = cookie.split('=', 1)
                self.session.cookies.set(key, value)

    def scan(self, categories=[12, 14], pages=100):
        url = "https://xueqiu.com/cubes/discover/rank/cube/list.json"
        
        # Load existing to avoid duplicates
        existing_symbols = set()
        if os.path.exists("data/long_history_cubes.json"):
            with open("data/long_history_cubes.json", "r", encoding="utf-8") as f:
                try:
                    old_data = json.load(f)
                    for item in old_data:
                        existing_symbols.add(item["symbol"])
                except:
                    pass
        
        all_candidates = []
        
        for category in categories:
            logging.info(f"Scanning category {category}...")
            
            for page in range(1, pages + 1):
                params = {
                    "market": "cn",
                    "sale_flag": 0,
                    "stock_positions": 0,
                    "sort": "best_benefit",
                    "category": category,
                    "page": page,
                    "count": 20
                }
                
                try:
                    resp = self.session.get(url, params=params)
                    if resp.status_code != 200:
                        logging.warning(f"Failed to fetch page {page}: {resp.status_code}")
                        break
                        
                    data = resp.json()
                    
                    if "list" not in data or not data["list"]:
                        logging.info("No more data.")
                        break
                        
                    page_candidates = 0
                    for item in data["list"]:
                        symbol = item["symbol"]
                        name = item["name"]
                        followers = item.get("follower_count", 0)
                        created_at = item.get("created_at", 0)
                        
                        # Filter criteria
                        if symbol in existing_symbols:
                            continue
                            
                        if followers < 50: # Lowered threshold
                            continue
                            
                        if created_at > self.MAX_CREATED_AT: # Must be created before 2022
                            continue
                            
                        logging.info(f"Found candidate: {name} ({symbol}) - Created: {datetime.fromtimestamp(created_at/1000).date()} - Followers: {followers}")
                        
                        candidate = {
                            "symbol": symbol,
                            "name": name,
                            "followers": followers,
                            "created_at": created_at,
                            "category": category
                        }
                        all_candidates.append(candidate)
                        existing_symbols.add(symbol)
                        page_candidates += 1
                    
                    logging.info(f"Category {category} Page {page}: Added {page_candidates} candidates.")
                    
                    time.sleep(random.uniform(0.5, 1.5))
                    
                except Exception as e:
                    logging.error(f"Error on page {page}: {e}")
                    break
        
        # Save results
        if all_candidates:
            # Append to existing
            if os.path.exists("data/long_history_cubes.json"):
                with open("data/long_history_cubes.json", "r", encoding="utf-8") as f:
                    try:
                        existing_data = json.load(f)
                        all_candidates.extend(existing_data)
                    except:
                        pass
            
            # Remove duplicates by symbol (just in case)
            unique_candidates = {c["symbol"]: c for c in all_candidates}.values()
            
            with open("data/long_history_cubes.json", "w", encoding="utf-8") as f:
                json.dump(list(unique_candidates), f, ensure_ascii=False, indent=2)
            
            logging.info(f"Saved {len(unique_candidates)} total candidates to data/long_history_cubes.json")
        else:
            logging.info("No new candidates found.")

if __name__ == "__main__":
    fetcher = XueqiuCandidateFetcher()
    fetcher.scan()
