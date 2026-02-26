import asyncio
import aiohttp
import logging
import urllib.parse
import re
import json
import os
import sys
import time
import random

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config

# 复用 bili_collector.py 的配置
# 实际上我们可以直接从 config 或 collector 导入，但这里为了独立性暂时保留
BILI_SESSDATA = "c85e0129%2C1785554128%2Cae0eb%2A21CjCTX6LPMl_eX4I-7qwEitcJqwiEiNP6tBucoDCpupjIG-GR9i8mV6mLKRmxCtiCPgQSVkdHOEFvdVVFRk54X241b3I3M2x1SFE5Q2pfR0xoUnlKaElsX1RFTmdtbTFvcDdjcy1rclFYbGFLTElDd2l5OUJObHZxcTE1SnF5dmJoTFF6WXNMTzJnIIEC"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Cookie": f"SESSDATA={BILI_SESSDATA}",
    "Referer": "https://www.bilibili.com/"
}

# 扩大关键词范围，覆盖复盘、短线、打板等
KEYWORDS = [
    "A股实盘", "股票实盘", "超短实盘", "淘股吧实盘", "实盘日记", "游资实盘",
    "股票复盘", "A股复盘", "涨停复盘", "龙虎榜", "打板日记", "龙头股", 
    "短线交易", "股市早评", "股市收评", "交割单",
    "实盘挑战", "百万实盘", "小资金实盘", "实盘记录",
    "游资", "悟道", "淘股吧",
    "首板", "连板", "二板", "打板", "低吸", "翘板", "竞价",
    "可转债实盘", "ETF实盘"
]

UP_LIST_FILE = config.UP_LIST_FILE if hasattr(config, 'UP_LIST_FILE') else "data/up_list.json"
KEYWORD_POOL_FILE = os.path.join("data", "keyword_pool.json")

DEFAULT_VIDEO_KEYWORDS = [
    "实盘", "交割单", "持仓", "对账单", "复盘", "明日计划", "打板", "连板", "龙头",
    "起飞", "翻倍", "收益", "涨停", "大肉", "爆赚", "悟道", "挑战", "记录", "日志"
]

def _load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default

def _save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def _normalize_keyword(kw: str) -> str:
    kw = (kw or "").strip()
    kw = re.sub(r"\s+", "", kw)
    return kw

def load_keyword_pool() -> list[str]:
    pool = _load_json(KEYWORD_POOL_FILE, {})
    keywords = pool.get("keywords") if isinstance(pool, dict) else None
    if not isinstance(keywords, list) or not keywords:
        keywords = list(dict.fromkeys(KEYWORDS + DEFAULT_VIDEO_KEYWORDS))
    keywords = [_normalize_keyword(k) for k in keywords if _normalize_keyword(k)]
    return list(dict.fromkeys(keywords))

def save_keyword_pool(keywords: list[str]):
    keywords = [_normalize_keyword(k) for k in (keywords or []) if _normalize_keyword(k)]
    keywords = list(dict.fromkeys(keywords))
    _save_json(KEYWORD_POOL_FILE, {"keywords": keywords, "updated_at": int(time.time())})

async def search_users(keyword, page=1):
    print(f"Searching for: {keyword} (Page {page})...")
    encoded_keyword = urllib.parse.quote(keyword)
    # Bilibili Web Search API for Users
    url = f"https://api.bilibili.com/x/web-interface/search/type?search_type=bili_user&keyword={encoded_keyword}&page={page}"
    
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data['code'] == 0:
                        return data['data']['result']
                    else:
                        print(f"Error searching {keyword}: {data}")
                else:
                    print(f"HTTP Error {resp.status} for {keyword}")
        except Exception as e:
            print(f"Exception searching {keyword}: {e}")
    return []

async def search_videos(keyword, page=1):
    print(f"Searching videos for: {keyword} (Page {page})...")
    encoded_keyword = urllib.parse.quote(keyword)
    url = f"https://api.bilibili.com/x/web-interface/search/type?search_type=video&keyword={encoded_keyword}&page={page}"

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        try:
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("code") == 0:
                        return data.get("data", {}).get("result", []) or []
                    print(f"Error searching videos {keyword}: {data}")
                else:
                    print(f"HTTP Error {resp.status} for video search {keyword}")
        except Exception as e:
            print(f"Exception searching videos {keyword}: {e}")
    return []

async def main():
    # 1. 加载现有的 UP 主
    known_ups = {}
    if os.path.exists(UP_LIST_FILE):
        try:
            with open(UP_LIST_FILE, 'r', encoding='utf-8') as f:
                known_ups = json.load(f)
        except:
            pass
            
    print(f"Current known UPs: {len(known_ups)}")
    
    found_ups = known_ups.copy()
    new_count = 0
    keywords = load_keyword_pool()
    
    # 2. 搜索更多 UP 主
    for kw in keywords:
        # 每个关键词搜前 30 页 (Expanded from 10)
        for page in range(1, 31):
            results = await search_users(kw, page)
            if not results:
                break
                
            for user in results:
                uid = str(user['mid']) # 统一转字符串
                name = user['uname']
                sign = user.get('usign', '')
                
                # 过滤条件
                # 1. 名字或简介包含股票相关词
                relevant_terms = ["实盘", "股票", "A股", "交易", "复盘", "涨停", "短线", "龙虎榜", "财经", "投资", "挑战", "记录", "日志", "悟道"]
                is_relevant = any(t in name or t in sign for t in relevant_terms)
                
                # 2. 排除明显无关的（如游戏、生活区，虽然API没直接给分区，但可以通过关键词再次过滤）
                # 暂时只依赖关键词
                
                if is_relevant and uid not in found_ups:
                    found_ups[uid] = name
                    new_count += 1
                    print(f"  [+] Found: {name} ({uid})")
                    
            await asyncio.sleep(random.uniform(3.0, 6.0)) # Increase delay to avoid 412

        # 3. 视频搜索反推作者 mid
        for page in range(1, 31):
            results = await search_videos(kw, page)
            if not results:
                break

            for item in results:
                uid = None
                name = None
                if isinstance(item, dict):
                    mid = item.get("mid")
                    if mid is not None:
                        uid = str(mid)
                    name = item.get("author") or item.get("uname") or item.get("upic")
                if uid and uid not in found_ups:
                    found_ups[uid] = name or f"User_{uid}"
                    new_count += 1
                    print(f"  [+] Found via video: {found_ups[uid]} ({uid})")

            await asyncio.sleep(random.uniform(3.0, 6.0))

    print(f"\nTotal UPs: {len(found_ups)} (New: {new_count})")
    
    save_keyword_pool(keywords)

    # 4. 保存到文件
    with open(UP_LIST_FILE, 'w', encoding='utf-8') as f:
        json.dump(found_ups, f, ensure_ascii=False, indent=4)
    print(f"Saved to {UP_LIST_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
