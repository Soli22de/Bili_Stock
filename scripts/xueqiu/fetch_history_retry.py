import sqlite3
import requests
import pandas as pd
import logging
import time
import random
import os
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def _load_xueqiu_cookie():
    cookie = os.getenv("XUEQIU_COOKIE", "").strip()
    if cookie:
        return cookie
    try:
        import config
        cookie = str(getattr(config, "XUEQIU_COOKIE", "")).strip()
    except Exception:
        cookie = ""
    return cookie


class HistoryFetcherRetry:
    def __init__(self, db_path="data/cubes.db"):
        self.db_path = db_path
        self.session = requests.Session()
        self.raw_cookie = _load_xueqiu_cookie()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://xueqiu.com/",
            "Origin": "https://xueqiu.com",
            "Host": "xueqiu.com",
            "X-Requested-With": "XMLHttpRequest"
        }
        self._init_session()

    def _init_session(self):
        if not self.raw_cookie:
            raise RuntimeError("Missing XUEQIU_COOKIE. Set env XUEQIU_COOKIE or config.XUEQIU_COOKIE.")
        for cookie in self.raw_cookie.split(';'):
            cookie = cookie.strip()
            if '=' in cookie:
                key, value = cookie.split('=', 1)
                self.session.cookies.set(key, value, domain=".xueqiu.com")
        self.session.headers.update(self.headers)
        if "xq_a_token" not in self.session.cookies:
            raise RuntimeError("XUEQIU_COOKIE missing xq_a_token.")
        logging.info("Session initialized.")

    def _select_targets(self):
        conn = sqlite3.connect(self.db_path)
        try:
            df = pd.read_sql_query("SELECT symbol, total_gain, followers_count, monthly_gain FROM cubes", conn)
            legends = df[(df['total_gain'] > 50) & (df['followers_count'] > 1000)].sort_values('total_gain', ascending=False).head(50)
            hidden_gems = df[(df['total_gain'] > 30) & (df['followers_count'] < 500)].sort_values('total_gain', ascending=False).head(200)
            rising_stars = df[(df['monthly_gain'] > 10)].sort_values('monthly_gain', ascending=False).head(50)
            combined = pd.concat([legends, hidden_gems, rising_stars]).drop_duplicates(subset=['symbol'])
            targets = combined['symbol'].dropna().astype(str).tolist()
            logging.info(f"Selection: {len(legends)} Legends, {len(hidden_gems)} Hidden Gems, {len(rising_stars)} Rising Stars. Total Unique: {len(targets)}")
            return targets
        finally:
            conn.close()

    def _fetch_symbol_pages(self, symbol, page_size=50, max_pages=80):
        items_all = []
        for page in range(1, max_pages + 1):
            self.session.headers.update({"Referer": f"https://xueqiu.com/P/{symbol}"})
            params = {
                "cube_symbol": symbol,
                "count": page_size,
                "page": page,
                "_": int(time.time() * 1000)
            }
            resp = self.session.get("https://xueqiu.com/cubes/rebalancing/history.json", params=params, timeout=12)
            if resp.status_code in (400, 401, 403):
                return [], page, f"HTTP_{resp.status_code}"
            if resp.status_code != 200:
                return items_all, page, f"HTTP_{resp.status_code}"
            data = resp.json()
            items = data.get("list", []) or []
            if not items:
                return items_all, page, "OK"
            items_all.extend(items)
            if len(items) < page_size:
                return items_all, page, "OK"
            time.sleep(random.uniform(0.2, 0.5))
        return items_all, max_pages, "MAX_PAGES_REACHED"

    def _to_dt(self, ts):
        try:
            ts = int(ts)
            if ts > 1000000000000:
                return datetime.fromtimestamp(ts / 1000)
            return datetime.fromtimestamp(ts)
        except Exception:
            return datetime.now()

    def _flatten_records(self, symbol, items):
        now = datetime.now()
        rows = []
        for item in items:
            created_at = self._to_dt(item.get("created_at", 0))
            status = item.get("status", "success")
            net_value = item.get("net_value", 0.0)
            histories = item.get("rebalancing_histories", []) or []
            for hist in histories:
                stock_symbol = str(hist.get("stock_symbol", "")).strip()
                if not stock_symbol:
                    continue
                rows.append((
                    symbol,
                    stock_symbol,
                    str(hist.get("stock_name", "")).strip(),
                    float(hist.get("prev_weight_adjusted", 0.0) or 0.0),
                    float(hist.get("target_weight", 0.0) or 0.0),
                    float(hist.get("price", 0.0) or 0.0),
                    float(net_value or 0.0),
                    created_at,
                    now,
                    status
                ))
        return rows

    def run(self):
        targets = self._select_targets()
        logging.info(f"Targeting {len(targets)} cubes for history fetch...")
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        quality_rows = []
        total_saved = 0
        for i, symbol in enumerate(targets):
            logging.info(f"[{i + 1}/{len(targets)}] Fetching {symbol}...")
            try:
                self.session.get(f"https://xueqiu.com/P/{symbol}", timeout=6)
            except Exception:
                pass
            try:
                items, pages, status = self._fetch_symbol_pages(symbol)
                rows = self._flatten_records(symbol, items)
                if rows:
                    before = conn.total_changes
                    cursor.executemany('''
                        INSERT OR IGNORE INTO rebalancing_history (
                            cube_symbol, stock_symbol, stock_name,
                            prev_weight_adjusted, target_weight,
                            price, net_value, created_at, updated_at, status
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', rows)
                    conn.commit()
                    inserted = conn.total_changes - before
                    total_saved += inserted
                else:
                    inserted = 0
                quality_rows.append({
                    "cube_symbol": symbol,
                    "status": status,
                    "pages": pages,
                    "history_items": len(items),
                    "rows_flattened": len(rows),
                    "rows_inserted": inserted
                })
                logging.info(f"{symbol} status={status} pages={pages} items={len(items)} inserted={inserted}")
            except Exception as e:
                conn.rollback()
                quality_rows.append({
                    "cube_symbol": symbol,
                    "status": f"ERROR:{e}",
                    "pages": 0,
                    "history_items": 0,
                    "rows_flattened": 0,
                    "rows_inserted": 0
                })
                logging.error(f"Error processing {symbol}: {e}")
            time.sleep(random.uniform(0.6, 1.2))
        conn.close()
        if quality_rows:
            os.makedirs("data", exist_ok=True)
            pd.DataFrame(quality_rows).to_csv("data/xueqiu_fetch_quality.csv", index=False, encoding="utf-8-sig")
            logging.info("Saved quality report: data/xueqiu_fetch_quality.csv")
        logging.info(f"Done. Total records inserted: {total_saved}")


if __name__ == "__main__":
    fetcher = HistoryFetcherRetry()
    fetcher.run()
