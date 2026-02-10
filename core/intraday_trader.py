"""
盘中交易执行模块 - 竞价筛选与盘中择时

功能：
1. 盘前竞价筛选 (9:25) - 剔除不及预期股票
2. 盘中分时择时 - 寻找最佳买点
3. 实时风控监控 - 防止黑天鹅事件
"""

import pandas as pd
import time
import os
import requests
from datetime import datetime, time as dt_time
import logging
from typing import List, Dict, Tuple
import random

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import config

_proxy_disabled = False

def _disable_proxy():
    global _proxy_disabled
    if _proxy_disabled:
        return
    _proxy_disabled = True
    orig = requests.Session.request
    def patched(self, method, url, *args, **kwargs):
        kwargs["proxies"] = {"http": None, "https": None}
        return orig(self, method, url, *args, **kwargs)
    requests.Session.request = patched
    os.environ["http_proxy"] = ""
    os.environ["https_proxy"] = ""
    os.environ["HTTP_PROXY"] = ""
    os.environ["HTTPS_PROXY"] = ""


class PreMarketFilter:
    """盘前竞价筛选器"""
    
    def __init__(self):
        self.filtered_stocks = []
        self.rejected_stocks = []
        self.data_available = True
    
    def get_auction_data(self, stock_codes: List[str]) -> pd.DataFrame:
        """获取竞价数据"""
        if getattr(config, "DISABLE_PROXY", False):
            _disable_proxy()
        try:
            # 首先尝试使用 AKShare
            try:
                import akshare as ak
                stock_data = ak.stock_zh_a_spot_em()
            except Exception as e:
                logger.warning(f"AKShare 数据获取失败: {e}")
                stock_data = pd.DataFrame()
            
            # 筛选目标股票
            target_data = pd.DataFrame()
            if stock_data is not None and not stock_data.empty and '代码' in stock_data.columns:
                target_data = stock_data[stock_data['代码'].isin(stock_codes)]
            
            if not target_data.empty:
                return target_data
            self.data_available = False
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"获取竞价数据失败: {e}")
            
            # 生成模拟数据用于测试
            logger.warning("使用模拟数据进行测试")
            self.data_available = False
            return self.generate_mock_data(stock_codes)
    
    def calculate_open_pct(self, current_price: float, pre_close: float) -> float:
        """计算开盘涨幅"""
        if pre_close == 0:
            return 0.0
        return ((current_price - pre_close) / pre_close) * 100
    
    def filter_auction_candidates(self, stock_codes: List[str]) -> Tuple[List[Dict], List[Dict]]:
        """
        竞价筛选函数
        
        筛选规则：
        - 保留：开盘涨幅 0% ~ 4% (平开或小幅高开)
        - 剔除：高开 > 6% (防骗炮) 或 低开 < -2% (不及预期)
        - 量比 > 2.0 (主力抢筹信号)
        """
        
        # 获取竞价数据
        auction_data = self.get_auction_data(stock_codes)
        
        if auction_data.empty:
            logger.warning("竞价数据为空，无法进行筛选")
            return [], []
        
        self.filtered_stocks = []
        self.rejected_stocks = []
        
        for _, row in auction_data.iterrows():
            stock_code = row['代码']
            stock_name = row['名称']
            current_price = row['最新价']
            pre_close = row['昨收']
            volume_ratio = row.get('量比', 1.0)  # 量比
            
            # 计算开盘涨幅
            open_pct = self.calculate_open_pct(current_price, pre_close)
            
            stock_info = {
                'code': stock_code,
                'name': stock_name,
                'current_price': current_price,
                'pre_close': pre_close,
                'open_pct': open_pct,
                'volume_ratio': volume_ratio
            }
            
            # 筛选逻辑
            rejection_reason = None
            
            if open_pct > 6.0:
                rejection_reason = f"高开过多: {open_pct:.2f}%"
            elif open_pct < -2.0:
                rejection_reason = f"低开不及预期: {open_pct:.2f}%"
            elif volume_ratio < 2.0:
                rejection_reason = f"量比不足: {volume_ratio:.2f}"
            
            if rejection_reason:
                stock_info['rejection_reason'] = rejection_reason
                self.rejected_stocks.append(stock_info)
                logger.info(f"剔除 {stock_name}({stock_code}): {rejection_reason}")
            else:
                self.filtered_stocks.append(stock_info)
                logger.info(f"保留 {stock_name}({stock_code}): 开盘{open_pct:.2f}%, 量比{volume_ratio:.2f}")
        
        return self.filtered_stocks, self.rejected_stocks


def read_trading_plan(csv_path: str = "data/trading_signals.csv") -> List[str]:
    """读取交易计划中的股票代码"""
    try:
        df = pd.read_csv(csv_path)
        
        # 获取所有不重复的股票代码
        stock_codes = df['stock_code'].dropna().unique().tolist()
        
        logger.info(f"从交易计划中读取到 {len(stock_codes)} 只股票")
        return stock_codes
        
    except Exception as e:
        logger.error(f"读取交易计划失败: {e}")
        return []


def run_auction_filter():
    """运行竞价筛选"""
    logger.info("=== 开始竞价筛选 ===")
    
    # 读取交易计划
    stock_codes = read_trading_plan()
    
    if not stock_codes:
        logger.warning("没有找到可筛选的股票")
        return
    
    # 创建筛选器
    filter = PreMarketFilter()
    
    # 执行筛选
    filtered, rejected = filter.filter_auction_candidates(stock_codes)
    
    # 输出结果
    logger.info(f"=== 筛选结果 ===")
    logger.info(f"原始股票数量: {len(stock_codes)}")
    logger.info(f"通过筛选: {len(filtered)} 只")
    logger.info(f"被剔除: {len(rejected)} 只")
    
    # 输出通过筛选的股票
    if filtered:
        logger.info("\n=== 今日精选池 ===")
        for stock in filtered:
            logger.info(f"{stock['name']}({stock['code']}) - 开盘{stock['open_pct']:.2f}%, 量比{stock['volume_ratio']:.2f}")
    
    # 输出被剔除的股票及原因
    if rejected:
        logger.info("\n=== 被剔除股票及原因 ===")
        for stock in rejected:
            logger.info(f"{stock['name']}({stock['code']}) - {stock.get('rejection_reason', '未知原因')}")


if __name__ == "__main__":
    # 测试竞价筛选
    run_auction_filter()
