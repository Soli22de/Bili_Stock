import akshare as ak
import pandas as pd
import requests
import json
import os
import time
from datetime import datetime

# Common Headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://xueqiu.com/",
    "Origin": "https://xueqiu.com",
    "Host": "xueqiu.com",
    "X-Requested-With": "XMLHttpRequest"
}

def get_session():
    session = requests.Session()
    session.headers.update(HEADERS)
    
    # Hardcoded cookie from fetch_cube_history.py (which is known to work or was working)
    raw_cookie = "acw_tc=3ccdc17e17713034273231509ee15c6e0f10dfb94b43e659b594d9d295e9b5; cookiesu=341771303427330; device_id=c803c3ee03e54a3a75ffde5e3f9b928d; Hm_lvt_1db88642e346389874251b5a1eded6e3=1771303428; HMACCOUNT=10B820C8E54C37A9; smidV2=20260217124348f9f63420da6530e44bccf87b1221265d009b6bb43cc4d88d0; xq_a_token=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xqat=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjYyOTc4MjIyMzgsImlzcyI6InVjIiwiZXhwIjoxNzczODk1Mjg0LCJjdG0iOjE3NzEzMDM0Mzc5NDEsImNpZCI6ImQ5ZDBuNEFadXAifQ.QjcXIPhbZmzCzhl1h8WQDjPFWOwu1P70rITs1UO_JrulikDYlGAevgkLGj4bG1AeQK4P8OKoQHkVzZc_Y1C5mLYxIdtyGUwVmyWhOrvtBYpx-IdWDhfxelt9sUCeyzWPKMQGU6K9dX64b4PfJ2RU1AjkysRXdBaP_lwtIUygOFH_M0GatP31lfX-yVNS5HdhQx7GGZX2QHIOo5JYzV9Fk-kcUW_G17DOqqhA03ZFcfrtYiydjICQPD7pAiaXGWuV4h1dmkk--IMYIL2ihbGMzkiEiAMKvOedAw4yPvPJGu_yMauYC-KLV_E49UlWLOjR_F5X1z4Ey8xVEPst2XEXsw; xq_r_token=075e5a5288f1d196eed9ffa5cc99aca5e136bff8; xq_is_login=1; u=6297822238; is_overseas=0; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1771303439; .thumbcache_f24b8bbe5a5934237bbc0eda20c1b6e7=FHDhoCv08W8qq7LCvhI4wVC5fmdISE0i9YLaxXcdid1A+jiQmDbfysCKpXxIiCAi+YjiH73oKrrpKKQWDDHKEQ%3D%3D; ssxmod_itna=1-YqGO7KY5AIejOjDhx_oxKupDp2bb4DXDUkqiQGgDYq7=GFKDCgYIRr=m4BK1fq_qDAgDXGW46Y7tDlrOrYDSxD=HDK4GThCPeDt_jEw04vW8tiH_w9YiAoM9WYeC2bU53yKUq9UgXU=qqjWzhQiYDCPDExGk57A=hDiiHx0rD0eDPxDYDG4Do6YDn6xDjxDd84SAmRoDbxi3E4GCo__L24DFkAopR3xD0oa_HGbveDDzXovqrSijDiW_RU2Phz0uWihID753Dlc4zTGIV/1BeGSuHvZLdroDXZtDvrSUGzrQ1f8EXK_wdgAiY_x0PmnmM0peeeGBDpihhee5AwMAqpjG4jD2j51mqx9rrDDA4hqt_YtiYQ_q4xlIHZbc_0HY37eFR4EuxF_KREVn2D9u4CDHiwVRxNlxi4biB2zib4zGbwGhK74eD; ssxmod_itna2=1-YqGO7KY5AIejOjDhx_oxKupDp2bb4DXDUkqiQGgDYq7=GFKDCgYIRr=m4BK1fq_qDAgDXGW46Y7YDiPbH_C=bD7pGeDGunvDBw0ne5IICLLLS4MS5auxyU7fwE9KjpRc0/mTCk_GT1gvOkPvdWQ0HdW5xSlHi587_QIFatncWzOnxmP5IeiKw7zPOOgcxna9sWQcxWiI=EU8kEE8E84uhPUf6WA8U5jdh53eOXxwFn=iXgE0FxpahW=jqNNG08zDhWl9Xka0sO26h=KkuVOZGITUmh21cwyUm4y9t0R6HgfT_BzAq6hIfVAkEuYcEz62"
    
    for cookie in raw_cookie.split('; '):
        if '=' in cookie:
            key, value = cookie.split('=', 1)
            session.cookies.set(key, value)
            
    return session

SESSION = get_session()

def fetch_cube_nav(symbol, name):
    print(f"Fetching NAV for {name} ({symbol})...")
    # Try xueqiu.com instead of cube.xueqiu.com
    url = f"https://xueqiu.com/cubes/nav_daily/all.json?cube_symbol={symbol}"
    
    try:
        resp = SESSION.get(url)
        data = resp.json()
        
        # Structure: [{"date": "2023-01-01", "value": 1.23, "percent": 0.01}, ...]
        # Actually usually: [{"date":"2016-06-27","net_value":1.0,"percent":0.0}, ...]
        
        # Handle case where response is [ {'list': [...]} ]
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict) and 'list' in data[0]:
             data = data[0]['list']
        elif isinstance(data, dict) and 'list' in data:
             data = data['list']
             
        if not isinstance(data, list):
             print(f"Unexpected data format for {symbol}: {str(data)[:100]}")
             return None
                
        if not data:
            print(f"Empty data for {symbol}")
            return None
            
        # Debug keys
        # print(f"Keys: {data[0].keys()}")
        
        df = pd.DataFrame(data)
        if df.empty: return None
        
        # Check date column
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        elif 'time' in df.columns:
             df['date'] = pd.to_datetime(df['time'])
        else:
            print(f"No date column found. Columns: {df.columns}")
            return None
            
        df.set_index('date', inplace=True)
        df = df[['percent']] # We focus on daily change for correlation
        df.rename(columns={'percent': symbol}, inplace=True)
        return df
        
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

def fetch_sector_indices():
    print("Fetching Sector Indices via AkShare...")
    
    # Mapping: Name -> AkShare Symbol (or ETF code)
    # Using ETFs is often better for "tradable" correlation
    
    sectors = {
        "Semiconductor": "512480", # 半导体ETF
        "Coal": "515220",          # 煤炭ETF
        "Gold": "518880",          # 黄金ETF
        "NewEnergy": "516160",     # 新能源ETF
        "Liquor": "512690",        # 酒ETF
        "Medical": "512170",       # 医疗ETF
        "Bank": "512800",          # 银行ETF
        "Broker": "512880",        # 证券ETF
        "CSI300": "510300"         # 沪深300ETF
    }
    
    dfs = []
    
    for name, code in sectors.items():
        try:
            # ak.fund_etf_hist_em(symbol="512480", period="daily", start_date="20220101", end_date="20260217", adjust="qfq")
            # This is slow if loop.
            print(f"  Fetching {name} ({code})...")
            df = ak.fund_etf_hist_em(symbol=code, period="daily", start_date="20230101", end_date="20260217", adjust="qfq")
            df['date'] = pd.to_datetime(df['日期'])
            df.set_index('date', inplace=True)
            
            # Calculate daily pct_change if not present (涨跌幅 is usually present)
            # '涨跌幅' is usually percentage (e.g. 1.23 for 1.23%)
            # Convert to decimal? No, Xueqiu usually gives decimal or percent. 
            # Xueqiu 'percent' is usually 0.01 for 1%? Or 0.01 is 0.01%?
            # Let's check Xueqiu format later. Usually it is ratio (0.01 = 1%).
            # AkShare '涨跌幅' is usually percent (1.23 = 1.23%).
            
            df = df[['涨跌幅']].rename(columns={'涨跌幅': name})
            df[name] = df[name] / 100.0 # Convert 1.23 to 0.0123
            
            dfs.append(df)
            time.sleep(0.5)
            
        except Exception as e:
            print(f"  Error fetching {name}: {e}")
            
    if dfs:
        return pd.concat(dfs, axis=1)
    return pd.DataFrame()

def run_nav_detective():
    # 1. Load Valuable Cubes
    with open("data/valuable_cubes.json", "r", encoding="utf-8") as f:
        cubes = json.load(f)
    
    # Focus on Top 5
    targets = cubes[:5]
    
    # 2. Fetch Cube NAVs
    cube_dfs = []
    for cube in targets:
        df = fetch_cube_nav(cube['symbol'], cube['name'])
        if df is not None:
            cube_dfs.append(df)
        time.sleep(1)
            
    if not cube_dfs:
        print("No Cube NAV data fetched.")
        return
        
    df_cubes = pd.concat(cube_dfs, axis=1)
    
    # 3. Fetch Indices
    df_indices = fetch_sector_indices()
    
    if df_indices.empty:
        print("No Index data fetched.")
        return
        
    # 4. Merge
    # Inner join on index
    df_all = pd.concat([df_cubes, df_indices], axis=1).dropna()
    
    print(f"\nData range: {df_all.index.min()} to {df_all.index.max()}")
    print(f"Total days: {len(df_all)}")
    
    # 5. Calculate Rolling Correlation (30 days)
    # We want to see if correlation changed recently.
    
    window = 30
    
    print(f"\n=== NAV Detective Report (Rolling {window} Days Correlation) ===")
    
    recent_date = df_all.index.max()
    print(f"Latest Date: {recent_date.date()}")
    
    for cube in targets:
        symbol = cube['symbol']
        if symbol not in df_all.columns: continue
        
        print(f"\nTarget: {cube['name']} ({symbol})")
        
        # Compute correlation with each sector
        corrs = {}
        for col in df_indices.columns:
            # Series.rolling.corr(Series)
            rolling_corr = df_all[symbol].rolling(window).corr(df_all[col])
            current_corr = rolling_corr.iloc[-1]
            corrs[col] = current_corr
            
        # Sort correlations
        sorted_corrs = sorted(corrs.items(), key=lambda x: x[1], reverse=True)
        
        print("  Current Sector Correlation:")
        for sector, corr in sorted_corrs[:3]:
            print(f"    {sector}: {corr:.2f}")
            
        # Check for Style Drift (compare with 3 months ago)
        # Look back 60 days (approx 3 months trading)
        if len(df_all) > 60:
            past_date = df_all.index[-60]
            print(f"  vs 3 Months Ago ({past_date.date()}):")
            
            for sector, curr_c in sorted_corrs[:3]:
                # Get past correlation
                past_c = df_all[symbol].rolling(window).corr(df_all[sector]).iloc[-60]
                diff = curr_c - past_c
                
                change_str = ""
                if diff > 0.3: change_str = "!!! (SHARP INCREASE)"
                elif diff < -0.3: change_str = "(Dropped)"
                
                print(f"    {sector}: {past_c:.2f} -> {curr_c:.2f} {change_str}")

if __name__ == "__main__":
    run_nav_detective()
