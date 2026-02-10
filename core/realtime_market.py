import asyncio
import aiohttp
import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import traceback

# 配置日志
logger = logging.getLogger(__name__)

class MarketDataSource:
    """市场数据源基类"""
    def __init__(self, name: str, weight: float = 1.0):
        self.name = name
        self.weight = weight  # 信任权重
        
    async def get_price(self, code: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

class SinaSource(MarketDataSource):
    """新浪财经数据源 (HTTP)"""
    def __init__(self):
        super().__init__("Sina", weight=1.2)
        self.base_url = "http://hq.sinajs.cn/list="
        
    def _format_code(self, code: str) -> str:
        if code.startswith('6'): return f"sh{code}"
        if code.startswith('0') or code.startswith('3'): return f"sz{code}"
        if code.startswith('8') or code.startswith('4'): return f"bj{code}"
        return f"sh{code}"

    async def get_price(self, code: str) -> Optional[Dict[str, Any]]:
        full_code = self._format_code(code)
        url = f"{self.base_url}{full_code}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=3) as response:
                    text = await response.text()
                    # var hq_str_sh600000="浦发银行,27.55,27.25,27.25,27.55,27.20,27.25,27.26,2200,27.25,..."
                    if "=\"" not in text: return None
                    content = text.split("=\"")[1].split("\"")[0]
                    if not content: return None
                    parts = content.split(",")
                    if len(parts) < 30: return None
                    
                    return {
                        "price": float(parts[3]),  # 当前价格
                        "pre_close": float(parts[2]), # 昨日收盘价
                        "open": float(parts[1]),
                        "high": float(parts[4]),
                        "low": float(parts[5]),
                        "time": f"{parts[30]} {parts[31]}",
                        "source": self.name
                    }
        except Exception as e:
            logger.warning(f"Sina source failed for {code}: {e}")
            return None

class TencentSource(MarketDataSource):
    """腾讯财经数据源 (HTTP)"""
    def __init__(self):
        super().__init__("Tencent", weight=1.0)
        self.base_url = "http://qt.gtimg.cn/q="

    def _format_code(self, code: str) -> str:
        if code.startswith('6'): return f"sh{code}"
        if code.startswith('0') or code.startswith('3'): return f"sz{code}"
        if code.startswith('8') or code.startswith('4'): return f"bj{code}"
        return f"sh{code}"

    async def get_price(self, code: str) -> Optional[Dict[str, Any]]:
        full_code = self._format_code(code)
        url = f"{self.base_url}{full_code}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=3) as response:
                    text = await response.text()
                    # v_sh600000="1~浦发银行~600000~27.25~27.55~27.25~2200~..."
                    # 腾讯接口: 3: current, 4: pre_close, 5: open
                    if "=\"" not in text: return None
                    content = text.split("=\"")[1].split("\"")[0]
                    parts = content.split("~")
                    if len(parts) < 30: return None
                    
                    return {
                        "price": float(parts[3]),
                        "pre_close": float(parts[4]),
                        "open": float(parts[5]),
                        "high": float(parts[33]),
                        "low": float(parts[34]),
                        "time": parts[30], # 20230101150000
                        "source": self.name
                    }
        except Exception as e:
            logger.warning(f"Tencent source failed for {code}: {e}")
            return None

class EastMoneySource(MarketDataSource):
    """东方财富数据源 (AkShare fallback)"""
    def __init__(self):
        super().__init__("EastMoney", weight=1.0)
        
    async def get_price(self, code: str) -> Optional[Dict[str, Any]]:
        # 这里为了异步效率，暂时使用 requests 同步调用（在线程池中运行）
        # 实际生产建议用 aiohttp 重写 akshare 逻辑，这里简化处理
        try:
            import akshare as ak
            loop = asyncio.get_event_loop()
            # stock_zh_a_spot_em 很慢，只取单个
            # 但 akshare 没有很好的单只股票实时接口，通常拉全量
            # 这里改用更轻量的逻辑：直接请求东财 API 接口（模拟）
            # 或者复用 akshare 的逻辑但只查一个？AkShare 的 stock_zh_a_hist_min_em 可以
            
            # 为了性能，这里我们只在 Sina/Tencent 都失败时才用 AkShare 全量接口
            # 或者直接返回 None，让外部逻辑处理
            return None 
        except Exception:
            return None

class RealTimePriceValidator:
    """多源价格验证器"""
    def __init__(self):
        self.sources = [SinaSource(), TencentSource()]
        # EastMoney 留作兜底或扩展
        self.price_cache = {} # code -> {price, time, update_time}

    async def get_verified_price(self, code: str) -> Dict[str, Any]:
        """
        获取经过验证的价格
        返回: {
            "price": float, 
            "is_valid": bool, 
            "sources_count": int, 
            "details": list,
            "quality_score": float,
            "reason": str
        }
        """
        tasks = [s.get_price(code) for s in self.sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        valid_data = []
        for res in results:
            if isinstance(res, dict) and res is not None:
                valid_data.append(res)
                
        if not valid_data:
            return {
                "price": None,
                "is_valid": False,
                "reason": "所有数据源均不可用",
                "quality_score": 0.0
            }
            
        # 提取价格列表
        prices = [d["price"] for d in valid_data]
        
        # 1. 价格一致性检查
        mean_price = np.mean(prices)
        if len(prices) >= 2:
            std_dev = np.std(prices)
            # 如果标准差超过均值的 1%，认为数据源冲突严重
            if std_dev > mean_price * 0.01:
                return {
                    "price": mean_price,
                    "is_valid": False,
                    "reason": f"数据源冲突: {prices}",
                    "quality_score": 40.0
                }
        
        # 2. 零价格检查
        if mean_price <= 0:
             return {
                "price": 0.0,
                "is_valid": False,
                "reason": "价格为零或负数",
                "quality_score": 0.0
            }

        # 3. 最终定价 (取中位数防极端值)
        final_price = float(np.median(prices))
        
        # 4. 价格异常检测 (涨跌停、价格跳跃)
        pre_close = None
        for d in valid_data:
            if d.get("pre_close") and d["pre_close"] > 0:
                pre_close = d["pre_close"]
                break
        
        # 4.1 涨跌停/幅度检查
        if pre_close:
            change_pct = (final_price - pre_close) / pre_close * 100
            # 科创板/创业板 20%，主板 10%，ST 5%
            # 这里给一个宽松的 22% 阈值防止数据错误
            if abs(change_pct) > 22:
                return {
                    "price": final_price,
                    "is_valid": False,
                    "reason": f"价格幅度异常: {change_pct:.2f}%",
                    "quality_score": 20.0
                }
        
        # 4.2 价格跳跃检测 (与上次查询对比)
        now = datetime.now()
        last_info = self.price_cache.get(code)
        if last_info:
            last_price = last_info["price"]
            last_time = last_info["update_time"]
            # 如果 1 分钟内波动超过 3%，认为异常 (除了开盘瞬间)
            if (now - last_time).total_seconds() < 60:
                jump_pct = abs(final_price - last_price) / last_price * 100
                if jump_pct > 3.0:
                    logger.warning(f"Price jump detected for {code}: {last_price} -> {final_price} ({jump_pct:.2f}%)")
                    # 这里可以标记为异常，或者只是警告
                    # 考虑到实盘可能有急速拉升，暂记为警告，降低分数
                    return {
                        "price": final_price,
                        "is_valid": True, # 仍然认为是有效价格，但分数降低
                        "reason": f"短时价格剧烈波动: {jump_pct:.2f}%",
                        "quality_score": 60.0
                    }

        # 更新缓存
        self.price_cache[code] = {
            "price": final_price,
            "update_time": now
        }
        
        return {
            "price": final_price,
            "is_valid": True,
            "sources_count": len(valid_data),
            "source_names": [d["source"] for d in valid_data],
            "quality_score": 100.0 if len(valid_data) >= 2 else 80.0
        }

# 单例模式
_validator = None

def get_market_validator():
    global _validator
    if _validator is None:
        _validator = RealTimePriceValidator()
    return _validator
