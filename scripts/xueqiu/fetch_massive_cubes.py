import requests
import json
import logging
import time
import random
import os
import sys

# Ensure core module is importable
sys.path.append(os.getcwd())
from core.storage import CubeStorage

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/fetch_cubes.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class XueqiuMassiveHunter:
    def __init__(self, data_file="data/massive_cube_list.json"):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://xueqiu.com/cube/center",
            "Host": "xueqiu.com"
        }
        self.data_file = data_file
        self.storage = CubeStorage()
        
        # Hardcoded Cookie for stability
        self.base_cookie = "xq_a_token=93155702220d9129525164893706440b84f3c4c9;" 
        
        self.init_cookie()
        
    def init_cookie(self):
        logging.info("Initializing Xueqiu Cookie...")
        try:
            self.session.headers.update(self.headers)
            self.session.headers.update({"Cookie": self.base_cookie})
            
            # Verify
            resp = self.session.get("https://xueqiu.com/")
            if resp.status_code == 200:
                logging.info("Cookie Success: xq_a_token found")
            else:
                logging.warning(f"Cookie check returned {resp.status_code}")
                
        except Exception as e:
            logging.error(f"Cookie Init Failed: {e}")

    def load_existing(self):
        """Load existing symbols from DB"""
        return self.storage.get_existing_symbols()

    def save_results(self, new_items):
        """Save new results to DB"""
        if not new_items:
            return
            
        try:
            self.storage.upsert_cubes(new_items)
            logging.info(f"Saved {len(new_items)} new items to DB.")
        except Exception as e:
            logging.error(f"Error saving to DB: {e}")
            
    def scan_by_category(self, categories=[12, 14], pages=20):
        """Scan rank categories"""
        url = "https://xueqiu.com/cubes/discover/rank/cube/list.json"
        existing_symbols = self.load_existing()
        logging.info(f"Loaded {len(existing_symbols)} existing cubes from DB.")
        
        for category in categories:
            for sort in ["best_benefit", "list_overall", "daily_gain", "monthly_gain", "annualized_gain_rate"]:
                logging.info(f"Scanning Category {category} with Sort {sort}...")
                
                for page in range(1, pages + 1):
                    params = {
                        "category": category,
                        "count": 20,
                        "page": page,
                        "market": "cn",
                        "sort": sort
                    }
                    
                    try:
                        resp = self.session.get(url, params=params)
                        if resp.status_code != 200:
                            logging.warning(f"Failed page {page}: {resp.status_code}")
                            break
                            
                        data = resp.json()
                        if "list" not in data or not data["list"]:
                            break
                            
                        items = data["list"]
                        new_items = []
                        for item in items:
                            if item["symbol"] not in existing_symbols:
                                new_items.append(item)
                                existing_symbols.add(item["symbol"])
                                
                        if new_items:
                            self.save_results(new_items)
                            
                        logging.info(f"Category {category} Page {page}: Found {len(items)} items.")
                        time.sleep(random.uniform(1, 2))
                        
                    except Exception as e:
                        logging.error(f"Error scanning page {page}: {e}")
                        break

    def scan_by_search(self, keywords, pages=20):
        """Scan by searching keywords"""
        url = "https://xueqiu.com/query/v1/search/cube.json"
        existing_symbols = self.load_existing()
        new_candidates = []
        
        for kw in keywords:
            logging.info(f"Searching keyword: {kw}...")
            
            for page in range(1, pages + 1):
                params = {
                    "q": kw,
                    "page": page,
                    "count": 20,
                    "sort": "best_benefit" 
                }
            
                try:
                    resp = self.session.get(url, params=params)
                    if resp.status_code != 200:
                        logging.warning(f"Failed search {kw} page {page}: {resp.status_code}")
                        break
                        
                    data = resp.json()
                    if "list" not in data or not data["list"]:
                        break
                        
                    items = data["list"]
                    added_count = 0
                    for item in items:
                        symbol = item["symbol"]
                        if symbol in existing_symbols:
                            continue
                        new_candidates.append(item)
                        existing_symbols.add(symbol)
                        added_count += 1
                        
                    logging.info(f"Search {kw} Page {page}: Found {len(items)} items, Added {added_count} new.")
                    
                    if len(items) < 20: # End of results
                        break
                        
                    time.sleep(random.uniform(0.8, 1.5))
                    
                except Exception as e:
                    logging.error(f"Error searching {kw}: {e}")
                    break
            
            # Save incrementally after each keyword
            if new_candidates:
                self.save_results(new_candidates)
                new_candidates = []

if __name__ == "__main__":
    hunter = XueqiuMassiveHunter()
    
    # 1. Scan Categories (Proven ones)
    hunter.scan_by_category(categories=[12, 14], pages=20)
    
    # 2. Scan by Search (High Volume)
    keywords = [
        # 基础风格
        "量化", "趋势", "稳健", "成长", "价值", "小盘", "ETF", 
        "回撤", "收益", "实盘", "主理人", "复利", "长线", "短线",
        "大盘", "中小盘", "龙头", "白马", "黑马", "套利", "对冲",
        "FOF", "多因子", "AI", "智能", "机器", "算法",
        
        # 行业板块
        "医药", "科技", "消费", "新能源", "红利", "低波", "高股息",
        "银行", "券商", "保险", "房地产", "建材", "家电", "食品", "饮料", "酿酒", 
        "纺织", "服装", "轻工", "造纸", "钢铁", "煤炭", "有色", "化工", "石油", 
        "机械", "电力", "公用", "环保", "建筑", "交通", "运输", "仓储", "物流", 
        "批发", "零售", "旅游", "酒店", "餐饮", "传媒", "互联网", "软件", "硬件", 
        "电子", "通信", "计算机", "半导体", "芯片", "光伏", "风电", "储能", "锂电池",
        
        # 热门概念
        "华为", "苹果", "特斯拉", "宁德时代", "比亚迪", "茅台", "腾讯", "阿里", 
        "拼多多", "中概互联", "恒生科技", "纳指", "标普", "道指", "黄金", "原油", 
        "债券", "转债", "可转债", "REITs",
        
        # 策略/术语
        "网格", "定投", "轮动", "择时", "选股", "打新", "分红", "股息", "现金流", 
        "护城河", "核心资产", "拥挤度", "情绪", "估值", "盈利", "质量", "动量", "反转"
    ]
    hunter.scan_by_search(keywords=keywords, pages=50)
