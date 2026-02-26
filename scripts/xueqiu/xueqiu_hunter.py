import requests
import json
import time
import random
import os
import pandas as pd
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/xueqiu_hunter.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class XueqiuHeadhunter:
    def __init__(self):
        self.session = requests.Session()
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://xueqiu.com/",
            "Origin": "https://xueqiu.com",
            "Host": "xueqiu.com",
            "X-Requested-With": "XMLHttpRequest"
        }
        self.output_file = "data/target_cubes.json"
        
        # 筛选标准配置
        self.criteria = {
            "min_annual_return": 15.0,  # 最小年化收益 %
            "max_drawdown": -20.0,      # 最大回撤 % (注意是负数，越小越好，即 > -20%)
            "min_total_gain": 20.0,     # 最小累计收益 %
            "min_followers": 1000,      # 最小关注人数
            "min_net_value": 1.2        # 最小净值
        }

        self._init_cookie()

    def _init_cookie(self):
        """访问主页获取初始 Cookie"""
        try:
            logging.info("正在获取雪球 Cookie...")
            # 模拟更真实的浏览器行为
            self.session.headers.update(self.headers)
            
            # 1. 访问主页
            self.session.get("https://xueqiu.com/", timeout=10)
            
            # 2. 只有当 xq_a_token 缺失时才尝试修复
            if "xq_a_token" not in self.session.cookies:
                 # 手动设置一个伪造但符合格式的 token (仅用于测试是否是此原因)
                 # 或者尝试访问其他子页面
                 self.session.get("https://xueqiu.com/hq", timeout=10)

            # 3. 打印最终 cookie 状态
            cookies = self.session.cookies.get_dict()
            if "xq_a_token" in cookies:
                logging.info(f"Cookie 获取成功: xq_a_token found")
            else:
                logging.warning(f"Cookie 可能不完整 (缺少 xq_a_token): {cookies}")
                
        except Exception as e:
            logging.error(f"Cookie 获取失败: {e}")

    def get_rank_list(self, category=12, count=20, market="cn", sort="best_benefit"):
        """
        获取组合排行榜
        修复参数错误: "发现页category非法参数", "发现页market非法参数"
        category=10 可能已经不再支持，只支持 category=12 (热门) 或其他
        market=cn 可能需要大写 CN 或移除
        """
        url = "https://xueqiu.com/cubes/discover/rank/cube/list.json"
        
        all_cubes = []
        page = 1
        max_pages = 3 
        
        headers = self.headers.copy()
        
        while page <= max_pages:
            params = {
                "market": market,
                "sale_flag": 0,
                "stock_positions": 0,
                "sort": sort,
                "category": category,
                "page": page,
                "count": count
            }
            
            # 针对性修复参数
            if category == 10: 
                # 如果 category=10 报错，尝试移除 category 参数，让其默认为最赚钱？
                # 或者使用 list_overall (综合榜)
                # 暂时跳过 10，只抓 12
                logging.warning("Category 10 may be invalid, skipping or switching to default.")
                break
                
            try:
                logging.info(f"正在抓取第 {page} 页排行榜 (API: {url})...")
                response = self.session.get(url, headers=headers, params=params)
                
                if response.status_code != 200:
                     logging.warning(f"API 请求失败: {response.status_code} - {response.text[:100]}")
                     break
                     
                data = response.json()
                
                cubes = []
                if "list" in data:
                    cubes = data["list"]
                
                if not cubes:
                    logging.warning("数据为空")
                    break

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
                        "net_value": item.get("net_value", 1.0)
                    }
                    all_cubes.append(cube_data)
                
                page += 1
                time.sleep(random.uniform(1, 3)) 
                
            except Exception as e:
                logging.error(f"请求排行榜出错: {e}")
                break
                
        return pd.DataFrame(all_cubes)

    def get_cube_details(self, symbol):
        """
        获取组合详情（用于补充回撤等数据，排行榜可能不全）
        注意：排行榜接口没有最大回撤数据，需要单独请求或计算
        目前先简化，仅使用排行榜数据筛选，后续可扩展
        """
        # TODO: 访问 /cubes/nav_daily/all.json?cube_symbol={symbol} 计算最大回撤
        pass

    def filter_cubes(self, df):
        """根据标准筛选优质组合"""
        if df.empty:
            return df
            
        logging.info(f"开始筛选，初始数量: {len(df)}")
        
        # 1. 累计收益筛选 (Total Gain > 20%)
        # 发现接口返回的 total_gain 是百分比 (如 15082.53 代表 150倍?)
        # 经过观察数据：ZH085468 total_gain=63330.35, net_value=634.3035 -> 对应 +63330%
        # 所以筛选逻辑应该基于 total_gain > 20.0
        df = df[df["total_gain"] >= self.criteria["min_total_gain"]]
        
        # 2. 关注人数筛选 (Follower > 1000)
        df = df[df["follower_count"] >= self.criteria["min_followers"]]
        
        # 3. 净值筛选 (Net Value > 1.2)
        df = df[df["net_value"] >= self.criteria["min_net_value"]]
        
        # 4. 年化收益 annualized_gain 为 0 问题：
        # 似乎发现页接口没有返回年化收益数据。
        # 我们需要根据 total_gain 和成立时间估算，或者暂时只用 total_gain 排序
        
        # 5. 排序：按累计收益降序
        df = df.sort_values(by="total_gain", ascending=False)
        
        logging.info(f"筛选后剩余数量: {len(df)}")
        return df

    def save_targets(self, df):
        """保存筛选结果到 JSON"""
        if df.empty:
            logging.warning("没有符合条件的组合，跳过保存")
            return
            
        # 转换为字典列表
        targets = df.to_dict(orient="records")
        
        # 确保目录存在
        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(targets, f, indent=4, ensure_ascii=False)
            
        logging.info(f"已保存 {len(targets)} 个优质组合到 {self.output_file}")
        
        # 同时打印预览
        print("\n=== 优质组合预览 ===")
        print(df[["name", "symbol", "annualized_gain", "total_gain", "follower_count"]].head(10).to_string())

    def run(self):
        logging.info("启动猎头模块 (Headhunter)...")
        
        # 1. 获取排行榜 (最赚钱榜单 category=10)
        logging.info("正在抓取'最赚钱'榜单...")
        df_profit = self.get_rank_list(category=10, count=20)
        
        # 2. 获取排行榜 (热门榜单 category=12)
        logging.info("正在抓取'热门'榜单...")
        df_hot = self.get_rank_list(category=12, count=20)
        
        # 合并并去重
        df_all = pd.concat([df_profit, df_hot]).drop_duplicates(subset=["symbol"])
        
        # 3. 筛选
        df_filtered = self.filter_cubes(df_all)
        
        # 4. 保存
        self.save_targets(df_filtered)
        
        logging.info("猎头任务完成。")

if __name__ == "__main__":
    hunter = XueqiuHeadhunter()
    hunter.run()
