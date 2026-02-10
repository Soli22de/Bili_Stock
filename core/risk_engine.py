"""
风险控制引擎 - 量化交易系统核心风控模块
提供仓位管理、风险限额、流动性检测等功能
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
import asyncio
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)

class RiskEngine:
    def __init__(self, config: Dict = None):
        """
        初始化风险控制引擎
        
        Args:
            config: 风险控制配置
        """
        self.config = config or {
            # 仓位控制
            'max_position_size': 0.1,           # 单票最大仓位比例 (10%)
            'max_daily_loss': 0.03,            # 日最大亏损比例 (3%)
            'max_portfolio_risk': 0.15,        # 组合最大风险敞口 (15%)
            
            # 流动性控制
            'min_liquidity_score': 0.3,         # 最小流动性评分
            'max_trade_volume_ratio': 0.1,     # 最大交易量占比 (10%)
            
            # 波动率控制
            'max_volatility': 0.5,             # 最大允许波动率 (50%)
            'volatility_lookback': 20,         # 波动率计算回溯期
            
            # 行业/板块控制
            'max_sector_exposure': 0.25,       # 单一行业最大暴露 (25%)
            
            # 交易时间控制
            'trading_hours': {
                'morning_start': '09:15',
                'morning_end': '11:30', 
                'afternoon_start': '13:00',
                'afternoon_end': '15:00'
            }
        }
        
        # 风险状态跟踪
        self.daily_pnl = 0.0
        self.positions = {}  # {symbol: {'quantity': int, 'entry_price': float, 'current_value': float}}
        self.trade_history = []
        self.last_reset_date = datetime.now().date()
        
        logger.info("RiskEngine initialized with config: %s", self.config)
    
    def reset_daily_stats(self):
        """重置每日统计数据"""
        current_date = datetime.now().date()
        if current_date != self.last_reset_date:
            self.daily_pnl = 0.0
            self.last_reset_date = current_date
            logger.info("Daily stats reset for date: %s", current_date)
    
    async def pre_trade_check(self, symbol: str, signal_score: float, 
                             proposed_price: float, proposed_quantity: int) -> Tuple[bool, str]:
        """
        交易前风险检查
        
        Args:
            symbol: 股票代码
            signal_score: 信号评分
            proposed_price: 建议交易价格
            proposed_quantity: 建议交易数量
            
        Returns:
            (是否通过, 原因说明)
        """
        self.reset_daily_stats()
        
        # 1. 基础检查
        checks = [
            self._check_trading_hours(),
            self._check_daily_loss_limit(),
            self._check_position_size(symbol, proposed_quantity, proposed_price),
            await self._check_liquidity(symbol, proposed_quantity),
            await self._check_volatility(symbol),
            self._check_sector_exposure(symbol, proposed_quantity, proposed_price)
        ]
        
        for check_passed, reason in checks:
            if not check_passed:
                return False, reason
        
        return True, "所有风控检查通过"
    
    def _check_trading_hours(self) -> Tuple[bool, str]:
        """检查交易时间"""
        now = datetime.now()
        time_str = now.strftime("%H:%M")
        
        trading_hours = self.config['trading_hours']
        morning_start = datetime.strptime(trading_hours['morning_start'], "%H:%M").time()
        morning_end = datetime.strptime(trading_hours['morning_end'], "%H:%M").time()
        afternoon_start = datetime.strptime(trading_hours['afternoon_start'], "%H:%M").time()
        afternoon_end = datetime.strptime(trading_hours['afternoon_end'], "%H:%M").time()
        
        current_time = now.time()
        
        if ((morning_start <= current_time <= morning_end) or 
            (afternoon_start <= current_time <= afternoon_end)):
            return True, "交易时间正常"
        
        return False, f"非交易时间: {time_str}"
    
    def _check_daily_loss_limit(self) -> Tuple[bool, str]:
        """检查日亏损限额"""
        if self.daily_pnl < -self.config['max_daily_loss']:
            return False, f"日亏损已达限额: {self.daily_pnl:.2%}"
        return True, "日亏损限额正常"
    
    def _check_position_size(self, symbol: str, quantity: int, price: float) -> Tuple[bool, str]:
        """检查单票仓位限制"""
        position_value = quantity * price
        
        # 这里需要总资产数据，暂时使用固定值模拟
        total_assets = 1000000  # 假设总资产100万
        position_pct = position_value / total_assets
        
        if position_pct > self.config['max_position_size']:
            return False, f"单票仓位超限: {position_pct:.2%} > {self.config['max_position_size']:.2%}"
        
        return True, "仓位大小正常"
    
    async def _check_liquidity(self, symbol: str, quantity: int) -> Tuple[bool, str]:
        """检查流动性"""
        # 模拟流动性检查 - 实际应接入实时行情数据
        # 这里简单返回通过
        return True, "流动性充足"
    
    async def _check_volatility(self, symbol: str) -> Tuple[bool, str]:
        """检查波动率"""
        # 模拟波动率检查 - 实际应计算历史波动率
        return True, "波动率正常"
    
    def _check_sector_exposure(self, symbol: str, quantity: int, price: float) -> Tuple[bool, str]:
        """检查行业暴露"""
        # 简化版行业检查
        return True, "行业暴露正常"
    
    def calculate_position_size(self, signal_score: float, volatility: float, 
                              total_assets: float) -> float:
        """
        基于凯利公式和波动率调整计算仓位大小
        
        Args:
            signal_score: 信号强度 (1.0-2.0)
            volatility: 年化波动率
            total_assets: 总资产
            
        Returns:
            建议仓位比例
        """
        # 基础仓位 = 最大仓位 × 信号强度调整
        base_size = self.config['max_position_size'] * min(signal_score / 2.0, 1.0)
        
        # 波动率调整: 高波动品种降低仓位
        # 波动率超过30%时开始惩罚
        vol_penalty = max(0, (volatility - 0.3) / 0.7)  # 0-1的惩罚系数
        adj_size = base_size * (1.0 - vol_penalty * 0.5)  # 最多减少50%
        
        # 确保不超过最大限制
        final_size = min(adj_size, self.config['max_position_size'])
        
        logger.debug(f"Position sizing: signal={signal_score:.2f}, vol={volatility:.2f}, "
                    f"base={base_size:.3f}, final={final_size:.3f}")
        
        return round(final_size, 4)
    
    def calculate_stop_loss(self, entry_price: float, volatility: float, 
                           signal_score: float) -> Dict[str, float]:
        """
        计算止损止盈价格
        
        Args:
            entry_price: 进场价格
            volatility: 年化波动率
            signal_score: 信号强度
            
        Returns:
            {'stop_loss': 止损价, 'take_profit': 止盈价}
        """
        # 基于波动率的ATR止损 (2倍ATR)
        atr_stop = entry_price * volatility * 2 / np.sqrt(252)  # 日波动幅度
        
        # 基于信号强度的动态止损
        if signal_score > 1.5:
            # 强信号: 宽松止损
            stop_loss_pct = 0.08  # 8%
            take_profit_pct = 0.15  # 15%
        elif signal_score > 1.2:
            # 中等信号: 标准止损
            stop_loss_pct = 0.06  # 6%
            take_profit_pct = 0.12  # 12%
        else:
            # 弱信号: 严格止损
            stop_loss_pct = 0.04  # 4%
            take_profit_pct = 0.08  # 8%
        
        # 取两者中较宽松的止损
        final_stop_loss = min(entry_price * (1 - stop_loss_pct), 
                             entry_price - atr_stop)
        
        take_profit = entry_price * (1 + take_profit_pct)
        
        return {
            'stop_loss': round(final_stop_loss, 2),
            'take_profit': round(take_profit, 2)
        }
    
    def record_trade(self, symbol: str, action: str, quantity: int, 
                   price: float, reason: str = ""):
        """记录交易记录"""
        trade = {
            'timestamp': datetime.now(),
            'symbol': symbol,
            'action': action,
            'quantity': quantity,
            'price': price,
            'value': quantity * price,
            'reason': reason
        }
        self.trade_history.append(trade)
        
        # 更新持仓
        if action.upper() == 'BUY':
            self.positions[symbol] = {
                'quantity': quantity,
                'entry_price': price,
                'current_value': quantity * price
            }
        elif action.upper() == 'SELL':
            if symbol in self.positions:
                del self.positions[symbol]
    
    def update_pnl(self, symbol: str, current_price: float):
        """更新盈亏"""
        if symbol in self.positions:
            position = self.positions[symbol]
            current_value = position['quantity'] * current_price
            entry_value = position['quantity'] * position['entry_price']
            
            pnl = current_value - entry_value
            position['current_value'] = current_value
            position['unrealized_pnl'] = pnl
            
            # 只记录已实现盈亏到每日统计
            # 未实现盈亏仅用于风险监控
    
    def get_risk_report(self) -> Dict:
        """生成风险报告"""
        total_assets = 1000000  # 模拟总资产
        portfolio_value = sum(pos['current_value'] for pos in self.positions.values())
        
        return {
            'timestamp': datetime.now(),
            'daily_pnl': self.daily_pnl,
            'total_positions': len(self.positions),
            'portfolio_value': portfolio_value,
            'portfolio_pct': portfolio_value / total_assets,
            'max_drawdown': self._calculate_drawdown(),
            'risk_metrics': self._calculate_risk_metrics()
        }
    
    def _calculate_drawdown(self) -> float:
        """计算最大回撤"""
        # 简化实现
        return 0.0
    
    def _calculate_risk_metrics(self) -> Dict:
        """计算风险指标"""
        return {
            'var_95': 0.0,  # 95%置信度VaR
            'expected_shortfall': 0.0,
            'sharpe_ratio': 0.0
        }

# 简化版风险引擎（用于快速集成）
class SimpleRiskManager:
    """简化风险管理器，用于快速集成到现有系统"""
    
    def __init__(self, small_capital_mode=True):
        """
        初始化风险管理器
        
        Args:
            small_capital_mode: 是否为小资金模式 (全仓操作)
        """
        self.small_capital_mode = small_capital_mode
        
        if small_capital_mode:
            # 小资金超短线模式配置
            self.config = {
                'max_position_size': 1.0,      # 全仓操作 (100%)
                'max_daily_loss': 0.05,        # 日最大亏损5% (更宽松)
                'min_signal_score': 1.05,      # 最低信号强度
                'max_price_change': 0.08,      # 最大允许涨幅8% (超短线可适当追高)
                'stop_loss_pct': 0.03,          # 止损比例3%
                'take_profit_pct': 0.06         # 止盈比例6%
            }
        else:
            # 常规资金管理模式
            self.config = {
                'max_position_size': 0.1,
                'max_daily_loss': 0.03,
                'min_signal_score': 1.05,
                'max_price_change': 0.05,
                'stop_loss_pct': 0.04,
                'take_profit_pct': 0.08
            }
        
        logger.info(f"RiskManager initialized in {'小资金全仓' if small_capital_mode else '常规'}模式")
    
    def validate_signal(self, signal_score: float, current_price: float, 
                       entry_price: float) -> Tuple[bool, str]:
        """验证交易信号"""
        # 1. 信号强度检查
        if signal_score < self.config['min_signal_score']:
            return False, f"信号强度不足: {signal_score:.3f} < {self.config['min_signal_score']}"
        
        # 2. 价格合理性检查
        if entry_price <= 0 or current_price <= 0:
            return False, "价格数据异常"
        
        # 3. 涨幅限制检查
        price_change = (current_price - entry_price) / entry_price
        if price_change > self.config['max_price_change']:
            return False, f"涨幅过大: {price_change:.2%} > {self.config['max_price_change']:.0%}"
        
        return True, "信号验证通过"
    
    def calculate_simple_position(self, signal_score: float, total_assets: float) -> float:
        """简化仓位计算"""
        if self.small_capital_mode:
            # 小资金模式: 全仓操作，但根据信号强度调整是否入场
            # 信号越强，越倾向于全仓
            confidence = min((signal_score - 1.0) / 1.0, 1.0)  # 1-2分映射到0-1
            
            # 只有信心度超过阈值才全仓入场
            if confidence > 0.2:  # 信号强度1.2以上就全仓 (从0.3下调到0.2)
                return self.config['max_position_size']  # 100%
            else:
                return 0.0  # 不入场
        else:
            # 常规模式: 分级仓位
            base_size = 0.05
            score_multiplier = min((signal_score - 1.0) / 1.0, 1.0)
            position_size = base_size * (1 + score_multiplier)
            return min(position_size, self.config['max_position_size'])
    
    def get_stop_levels(self, entry_price: float, signal_score: float) -> Dict[str, float]:
        """获取止损止盈价位"""
        if self.small_capital_mode:
            # 小资金超短线:  tighter stops
            stop_loss = entry_price * (1 - self.config['stop_loss_pct'])
            take_profit = entry_price * (1 + self.config['take_profit_pct'])
            
            # 根据信号强度微调
            if signal_score > 1.5:
                # 强信号: 稍微放宽止损
                stop_loss = entry_price * (1 - self.config['stop_loss_pct'] * 0.8)
            
            return {
                'stop_loss': round(stop_loss, 2),
                'take_profit': round(take_profit, 2),
                'risk_reward_ratio': self.config['take_profit_pct'] / self.config['stop_loss_pct']
            }
        else:
            # 常规模式的止损逻辑
            stop_loss = entry_price * (1 - 0.04)
            take_profit = entry_price * (1 + 0.08)
            return {
                'stop_loss': round(stop_loss, 2),
                'take_profit': round(take_profit, 2)
            }

# 单例实例
risk_engine = RiskEngine()
simple_risk_manager = SimpleRiskManager()