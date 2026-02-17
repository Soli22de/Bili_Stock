import requests
import json
import time
import random
import os
import pandas as pd
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/discover_and_clean.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class XueqiuCubeDiscover:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://xueqiu.com/",
            "Origin": "https://xueqiu.com",
            "Host": "xueqiu.com",
            "X-Requested-With": "XMLHttpRequest"
        }
        self.output_file = "data/active_target_cubes.json"
        
        # 手动注入用户提供的完整 Cookie
        self._init_cookie()
        
        # 筛选标准配置
        self.criteria = {
            "min_total_gain": 0.0,
            "max_drawdown": -30.0,
            "min_followers": 0,
            "min_net_value": 0.0,
            "max_inactive_days": 365,
            "min_capital_suspected": 500000,
        }

    def _init_cookie(self):
        """手动注入 Cookie"""
        try:
            logging.info("正在注入雪球 Cookie...")
            raw_cookie = "acw_tc=3ccdc17e17713034273231509ee15c6e0f10dfb94b43e659b594d9d295e9b5; cookiesu=341771303427330; device_id=c803c3ee03e54a3a75ffde5e3f9b928d; Hm_lvt_1db88642e346389874251b5a1eded6e3=1771303428; HMACCOUNT=10B820C8E54C37A9; smidV2=20260217124348f9f63420da6530e44bccf87b1221265d009b6bb43cc4d88d0; xq_a_token=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xqat=33c6a88412590f9d707de51fbca5a323ef9a0ef3; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjYyOTc4MjIyMzgsImlzcyI6InVjIiwiZXhwIjoxNzczODk1Mjg4LCJjdG0iOjE3NzEzMDM0Mzc5NDEsImNpZCI6ImQ5ZDBuNEFadXAifQ.QjcXIPhbZmzCzhl1h8WQDjPFWOwu1P70rITs1UO_JrulikDYlGAevgkLGj4bG1AeQK4P8OKoQHkVzZc_Y1C5mLYxIdtyGUwVmyWhOrvtBYpx-IdWDhfxelt9sUCeyzWPKMQGU6K9dX64b4PfJ2RU1AjkysRXdBaP_lwtIUygOFH_M0GatP31lfX-yVNS5HdhQx7GGZX2QHIOo5JYzV9Fk-kcUW_G17DOqqhA03ZFcfrtYiydjICQPD7pAiaXGWuV4h1dmkk--IMYIL2ihbGMzkiEiAMKvOedAw4yPvPJGu_yMauYC-KLV_E49UlWLOjR_F5X1z4Ey8xVEPst2XEXsw; xq_r_token=075e5a5288f1d196eed9ffa5cc99aca5e136bff8; xq_is_login=1; u=6297822238; is_overseas=0; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1771303439; .thumbcache_f24b8bbe5a5934237bbc0eda20c1b6e7=FHDhoCv08W8qq7LCvhI4wVC5fmdISE0i9YLaxXcdid1A+jiQmDbfysCKpXxIiCAi+YjiH73oKrrpKKQWDDHKEQ%3D%3D; ssxmod_itna=1-YqGO7KY5AIejOjDhx_oxKupDp2bb4DXDUkqiQGgDYq7=GFKDCgYIRr=m4BK1fq_qDAgDXGW46Y7tDlrOrYDSxD=HDK4GThCPeDt_jEw04vW8tiH_w9YiAoM9WYeC2bU53yKUq9UgXU=qqjWzhQiYDCPDExGk57A=hDiiHx0rD0eDPxDYDG4Do6YDn6xDjxDd84SAmRoDbxi3E4GCo__L24DFkAopR3xD0oa_HGbveDDzXovqrSijDiW_RU2Phz0uWihID753Dlc4zTGIV/1BeGSuHvZLdroDXZtDvrSUGzzQ1f8EXK_wdgAiY_x0PmnmM0peeeGBDpihhee5AwMAqpjG4jD2j51mqx9rrDDA4hqt_YtiYQ_q4xlIHZbc_0HY37eFR4EuxF_KREVn2D9u4CDHiwVRxNlxi4biB2zib4zGbwGhK74eD; ssxmod_itna2=1-YqGO7KY5AIejOjDhx_oxKupDp2bb4DXDUkqiQGgDYq7=GFKDCgYIRr=m4BK1fq_qDAgDXGW46Y7YDiPbH_C=bD7pGeDGunvDBw0ne5IICLLLS4MS5auxyU7fwE9KjpRc0/mTCk_GT1gvOkPvdWQ0HdW5xSlHi587_QIFatncWzOnxmP5IeiKw7zPOOgcxna9sWQcxWiI=EU8kEE8E84uhPUf6WA8U5jdh53eOXxwFn=iXgE0FxpahW=jqNNG08zDhWl9Xka0sO26h=KkuVOZGITUmh21cwyUm4y9t0R6HgfT_BzAq6hIfVAkEuYcEz62NKwkWohuMz/oCYm18_FRhDz4KXgha4i/gid7Hxxhvqazg36h3p3x1M=Dnsb4GnhWLbpqGr5RmmxNNCH53OmLif8DGuFzurVl5L8k9RWTODW23UbvLbWUBwT2mW_5USb0EaWQO_iehGDdGj6OrM24dYe57jU66MA5ViYxh5tYjUPXeiDD"
            
            for cookie in raw_cookie.split('; '):
                if '=' in cookie:
                    key, value = cookie.split('=', 1)
                    self.session.cookies.set(key, value)
            
            self.session.headers.update(self.headers)
            if "xq_a_token" in self.session.cookies:
                logging.info(f"Cookie 注入成功: xq_a_token found")
            else:
                logging.warning(f"Cookie 可能不完整: {self.session.cookies.get_dict()}")
                
        except Exception as e:
            logging.error(f"Cookie 注入失败: {e}")

    def get_rank_list(self, category: int, count: int = 100, market: str = "cn", 
                     sort: str = "best_benefit") -> pd.DataFrame:
        """
        获取组合排行榜
        category: 10=最赚钱, 12=热门, 13=本月最佳, 14=年化最高
        """
        url = "https://xueqiu.com/cubes/discover/rank/cube/list.json"
        
        all_cubes = []
        page = 1
        max_pages = 20
        
        headers = self.headers.copy()
        headers["Referer"] = "https://xueqiu.com/cubes/discover/rank"
        headers["Accept"] = "application/json, text/plain, */*"
        active_params = None
        
        while page <= max_pages:
            base_params = {
                "sale_flag": 0,
                "stock_positions": 0,
                "sort": sort,
                "page": page,
                "count": count
            }
            param_variants = []
            if market:
                param_variants.append({**base_params, "market": str(market).upper(), "category": category})
                param_variants.append({**base_params, "market": str(market).upper(), "category": str(category)})
            param_variants.append({**base_params, "category": category})
            param_variants.append({**base_params, "category": str(category)})
            if market:
                param_variants.append({**base_params, "market": str(market).upper()})
            param_variants.append(base_params)
            
            response = None
            data = None
            if active_params:
                try:
                    params = {**active_params, "page": page}
                    logging.info(f"正在抓取第 {page} 页排行榜 (category={category})...")
                    response = self.session.get(url, headers=headers, params=params)
                    if response.status_code == 200:
                        data = response.json()
                except Exception as e:
                    logging.error(f"请求排行榜出错: {e}")
                    response = None
                    data = None
            else:
                for params in param_variants:
                    try:
                        logging.info(f"正在抓取第 {page} 页排行榜 (category={category})...")
                        response = self.session.get(url, headers=headers, params=params)
                        if response.status_code != 200:
                            continue
                        data = response.json()
                        if "list" in data and data["list"]:
                            active_params = {k: v for k, v in params.items() if k != "page"}
                            break
                    except Exception as e:
                        logging.error(f"请求排行榜出错: {e}")
                        response = None
                        data = None
            
            if not data or "list" not in data or not data["list"]:
                if response is not None:
                    logging.warning(f"API 请求失败: {response.status_code} {response.text[:100]}")
                else:
                    logging.warning("API 请求失败: 无响应")
                break
            
            cubes = data["list"]
            for item in cubes:
                cube_data = {
                    "name": item.get("name"),
                    "symbol": item.get("symbol"),
                    "owner": item.get("owner", {}).get("screen_name"),
                    "follower_count": item.get("follower_count", 0),
                    "total_gain": item.get("total_gain", 0),
                    "annualized_gain": item.get("annualized_gain", 0),
                    "monthly_gain": item.get("monthly_gain", 0),
                    "daily_gain": item.get("daily_gain", 0),
                    "net_value": item.get("net_value", 1.0),
                    "created_at": item.get("created_at"),
                    "last_rebalancing_time": item.get("last_rebalancing_time"),
                    "category": category
                }
                all_cubes.append(cube_data)
            
            page += 1
            time.sleep(random.uniform(2, 5)) 
                
        return pd.DataFrame(all_cubes)

    def search_cubes_by_keyword(self, keyword: str, count: int = 100) -> pd.DataFrame:
        """通过关键词搜索组合"""
        urls = [
            "https://xueqiu.com/cubes/discover/search/cube/list.json",
            "https://xueqiu.com/cubes/search.json",
            "https://xueqiu.com/cubes/discover/search.json",
            "https://xueqiu.com/search.json"
        ]
        params = {
            "q": keyword,
            "count": count,
            "page": 1,
            "type": "cube"
        }
        try:
            logging.info(f"正在搜索关键词: {keyword}")
            data = None
            response = None
            for url in urls:
                response = self.session.get(url, headers=self.headers, params=params)
                if response.status_code != 200:
                    continue
                data = response.json()
                if data:
                    break
            
            if not data:
                if response is not None:
                    logging.warning(f"搜索请求失败: {response.status_code}")
                else:
                    logging.warning("搜索请求失败: 无响应")
                return pd.DataFrame()
            
            items = []
            if "list" in data:
                items = data["list"]
            elif "data" in data and isinstance(data["data"], dict) and "list" in data["data"]:
                items = data["data"]["list"]
            
            cubes = []
            for item in items:
                cube_data = {
                    "name": item.get("name"),
                    "symbol": item.get("symbol"),
                    "owner": item.get("owner", {}).get("screen_name"),
                    "follower_count": item.get("follower_count", 0),
                    "total_gain": item.get("total_gain", 0),
                    "annualized_gain": item.get("annualized_gain", 0),
                    "monthly_gain": item.get("monthly_gain", 0),
                    "daily_gain": item.get("daily_gain", 0),
                    "net_value": item.get("net_value", 1.0),
                    "created_at": item.get("created_at"),
                    "last_rebalancing_time": item.get("last_rebalancing_time"),
                    "category": f"search_{keyword}"
                }
                cubes.append(cube_data)
            
            return pd.DataFrame(cubes)
            
        except Exception as e:
            logging.error(f"关键词搜索出错: {e}")
            return pd.DataFrame()

    def get_cube_details(self, symbol: str) -> Dict[str, Any]:
        """获取组合详情，包括最大回撤等数据"""
        url = f"https://xueqiu.com/cubes/nav_daily/all.json"
        params = {
            "cube_symbol": symbol,
            "since": 0,  # 从开始时间获取
            "_": int(time.time() * 1000)
        }
        
        try:
            response = self.session.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if "list" in data:
                    # 计算最大回撤
                    nav_data = data["list"]
                    if len(nav_data) > 1:
                        max_drawdown = self._calculate_max_drawdown(nav_data)
                        return {"max_drawdown": max_drawdown}
            
        except Exception as e:
            logging.error(f"获取组合 {symbol} 详情失败: {e}")
        
        return {}

    def _calculate_max_drawdown(self, nav_data: List[Dict]) -> float:
        """计算最大回撤"""
        try:
            # 提取净值和时间戳
            navs = [{"timestamp": item["timestamp"], "value": item["value"]} for item in nav_data]
            navs.sort(key=lambda x: x["timestamp"])
            
            peak = navs[0]["value"]
            max_drawdown = 0.0
            
            for nav in navs:
                if nav["value"] > peak:
                    peak = nav["value"]
                else:
                    drawdown = (peak - nav["value"]) / peak * 100
                    max_drawdown = max(max_drawdown, drawdown)
            
            return round(max_drawdown, 2)
        except:
            return 0.0

    def filter_cubes(self, df: pd.DataFrame) -> pd.DataFrame:
        """根据多重标准筛选优质组合"""
        if df.empty:
            return df
            
        logging.info(f"开始筛选，初始数量: {len(df)}")
        
        # 1. 基础业绩筛选
        df = df[df["total_gain"] >= self.criteria["min_total_gain"]]
        df = df[df["follower_count"] >= self.criteria["min_followers"]]
        df = df[df["net_value"] >= self.criteria["min_net_value"]]
        
        # 2. 活跃度筛选 (剔除僵尸组合)
        current_time = int(time.time() * 1000)
        df["days_since_last_rebalance"] = (current_time - df["last_rebalancing_time"]) / (1000 * 60 * 60 * 24)
        df.loc[df["last_rebalancing_time"].isna(), "days_since_last_rebalance"] = 0
        df = df[df["days_since_last_rebalance"] <= self.criteria["max_inactive_days"]]
        
        # 3. 疑似实盘标记
        df["estimated_capital"] = df["follower_count"] * 100
        df["has_real_capital"] = df["estimated_capital"] >= self.criteria["min_capital_suspected"]
        
        # 4. 关键词标记
        df["has_strategy_keyword"] = df["name"].str.contains(r"量化|实盘|策略|交易", na=False)
        
        logging.info(f"筛选后剩余数量: {len(df)}")
        return df.sort_values(by=["has_real_capital", "has_strategy_keyword", "total_gain"], ascending=[False, False, False])

    def discover_active_cubes(self) -> pd.DataFrame:
        """广撒网发现活跃组合"""
        all_cubes = []
        
        # 1. 多个排行榜抓取
        categories = [
            (10, "最赚钱"),
            (12, "热门"), 
            (13, "本月最佳"),
            (14, "年化最高")
        ]
        
        for category_id, category_name in categories:
            logging.info(f"正在抓取 {category_name} 榜单...")
            df_category = self.get_rank_list(category_id, count=100)
            if not df_category.empty:
                all_cubes.append(df_category)
                logging.info(f"{category_name} 榜单获取 {len(df_category)} 个组合")
            time.sleep(2)
        
        # 2. 关键词搜索
        keywords = ["量化", "短线", "龙头", "实盘", "交易"]
        for keyword in keywords:
            logging.info(f"正在搜索关键词: {keyword}")
            df_search = self.search_cubes_by_keyword(keyword, count=50)
            if not df_search.empty:
                all_cubes.append(df_search)
                logging.info(f"关键词 '{keyword}' 搜索到 {len(df_search)} 个组合")
            time.sleep(1)
        
        # 合并并去重
        if all_cubes:
            df_all = pd.concat(all_cubes).drop_duplicates(subset=["symbol"])
            logging.info(f"总共发现 {len(df_all)} 个唯一组合")
            return df_all
        else:
            logging.warning("未发现任何组合")
            return pd.DataFrame()

    def save_active_targets(self, df: pd.DataFrame):
        """保存动态维护的活跃目标组合"""
        if df.empty:
            logging.warning("没有符合条件的组合，跳过保存")
            return
            
        # 转换为字典列表
        targets = df.to_dict(orient="records")
        
        # 确保目录存在
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        
        # 读取现有文件（如果存在）并合并
        existing_targets = []
        if os.path.exists(self.output_file):
            try:
                with open(self.output_file, 'r', encoding='utf-8') as f:
                    existing_targets = json.load(f)
            except:
                pass
        
        # 合并并去重
        existing_symbols = {t["symbol"] for t in existing_targets}
        new_targets = [t for t in targets if t["symbol"] not in existing_symbols]
        
        # 合并所有目标
        all_targets = existing_targets + new_targets
        
        # 保存
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(all_targets, f, indent=4, ensure_ascii=False)
            
        logging.info(f"已保存 {len(all_targets)} 个活跃组合到 {self.output_file}")
        
        # 打印预览
        print("\n=== 活跃组合预览 ===")
        preview_df = pd.DataFrame(all_targets[:10])
        if not preview_df.empty:
            print(preview_df[["name", "symbol", "total_gain", "follower_count", "days_since_last_rebalance"]].to_string())

    def run(self):
        """运行完整的发现与清洗流程"""
        logging.info("启动组合发现与清洗模块...")
        
        # 1. 广撒网发现
        df_discovered = self.discover_active_cubes()
        if df_discovered.empty:
            logging.error("组合发现失败")
            return
        
        # 2. 强过滤清洗
        df_filtered = self.filter_cubes(df_discovered)
        
        # 3. 动态维护保存
        self.save_active_targets(df_filtered)
        
        logging.info("组合发现与清洗任务完成")

if __name__ == "__main__":
    discover = XueqiuCubeDiscover()
    discover.run()
