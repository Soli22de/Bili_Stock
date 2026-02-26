
import requests
import json
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

def test_fetch():
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://xueqiu.com/",
        "Origin": "https://xueqiu.com",
        "Host": "xueqiu.com",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    # Hardcoded cookie from xueqiu_spy.py
    # Note: I removed some potentially sensitive or truncated parts if any, but copying what was in xueqiu_spy.py
    raw_cookie = "acw_tc=3ccdc17e17713034273231509ee15c6e0f10dfb94b43e659b594d9d295e9b5; cookiesu=341771303427330; device_id=c803c3ee03e54a3a75ffde5e3f9b928d; Hm_lvt_1db88642e346389874251b5a1eded6e3=1771303428; HMACCOUNT=10B820C8E54C37A9; smidV2=20260217124348f9f63420da6530e44bccf87b1221265d009b6bb43cc4d88d0; xq_a_token=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xqat=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjYyOTc4MjIyMzgsImlzcyI6InVjIiwiZXhwIjoxNzczODk1Mjg0LCJjdG0iOjE3NzEzMDM0Mzc5NDEsImNpZCI6ImQ5ZDBuNEFadXAifQ.QjcXIPhbZmzCzhl1h8WQDjPFWOwu1P70rITs1UO_JrulikDYlGAevgkLGj4bG1AeQK4P8OKoQHkVzZc_Y1C5mLYxIdtyGUwVmyWhOrvtBYpx-IdWDhfxelt9sUCeyzWPKMQGU6K9dX64b4PfJ2RU1AjkysRXdBaP_lwtIUygOFH_M0GatP31lfX-yVNS5HdhQx7GGZX2QHIOo5JYzV9Fk-kcUW_G17DOqqhA03ZFcfrtYiydjICQPD7pAiaXGWuV4h1dmkk--IMYIL2ihbGMzkiEiAMKvOedAw4yPvPJGu_yMauYC-KLV_E49UlWLOjR_F5X1z4Ey8xVEPst2XEXsw; xq_r_token=075e5a5288f1d196eed9ffa5cc99aca5e136bff8; xq_is_login=1; u=6297822238; is_overseas=0;"
    
    for cookie in raw_cookie.split('; '):
        if '=' in cookie:
            key, value = cookie.split('=', 1)
            session.cookies.set(key, value, domain=".xueqiu.com")
            
    session.headers.update(headers)
    
    symbol = "ZH2479048"
    url = "https://xueqiu.com/cubes/rebalancing/history.json"
    
    # Set specific Referer as in xueqiu_spy.py
    session.headers.update({
        "Referer": f"https://xueqiu.com/P/{symbol}",
        "Host": "xueqiu.com"
    })
    
    params = {
        "cube_symbol": symbol,
        "count": 20,
        "page": 1,
        "_": int(time.time() * 1000)
    }
    
    try:
        logging.info(f"Fetching history for {symbol} with hardcoded cookie...")
        resp = session.get(url, params=params)
        logging.info(f"Status: {resp.status_code}")
        logging.info(f"Response: {resp.text[:500]}")
    except Exception as e:
        logging.error(f"Error: {e}")

if __name__ == "__main__":
    test_fetch()
