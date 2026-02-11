from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Tuple, Optional, List
import pandas as pd
import numpy as np

@dataclass
class StrategyConfig:
    """策略配置参数类"""
    # 风险控制参数
    max_sentiment_score: int = 5          # 最大情绪得分（博主推荐数）
    min_amount: float = 30000000.0        # 最小成交额 (3000万)
    min_circ_mv: float = 2000000000.0     # 最小流通市值 (20亿)
    max_3d_pct: float = 20.0              # 3日累计最大涨幅 (%)
    max_index_drop: float = -3.0          # 最大指数跌幅 (%)

    # 竞价策略参数
    max_open_pct: float = 7.0             # 最大高开幅度 (%)
    min_open_pct: float = -3.0            # 最大低开幅度 (%)

    # 盘中策略参数
    vwap_threshold: float = 1.0           # 价格/VWAP 阈值 (1.0表示价格>VWAP)

    # 退出策略参数
    stop_loss_atr_multiplier: float = 2.0 # ATR止损倍数
    default_atr_volatility: float = 0.03  # 默认波动率 (当无法计算ATR时)
    take_profit_open_pct: float = 0.05    # 开盘止盈阈值 (%)
    take_profit_ratio: float = 0.5        # 开盘止盈比例
    max_holding_days: int = 3             # 最大持仓天数

class BaseStrategy(ABC):
    """策略抽象基类"""
    
    def __init__(self, config: StrategyConfig = None):
        self.config = config or StrategyConfig()

    @abstractmethod
    def check_risk(self, context: Dict) -> Tuple[bool, str]:
        """
        全天候风控检查
        Args:
            context: 包含 'price_data', 'signals', 'index_data', 'history_df' 等
        Returns:
            (is_safe, reason)
        """
        pass

    @abstractmethod
    def on_open_auction(self, context: Dict) -> Tuple[bool, str, str]:
        """
        集合竞价阶段处理
        Returns:
            (can_participate, status, reason)
        """
        pass

    @abstractmethod
    def on_intraday(self, context: Dict) -> Tuple[Optional[float], str, str]:
        """
        盘中择时处理
        Returns:
            (buy_price, status, reason)
        """
        pass

    @abstractmethod
    def on_exit(self, context: Dict) -> Tuple[str, float, float, str, str]:
        """
        卖出策略处理
        Returns:
            (sell_date, sell_price, pnl_pct, status, reason)
        """
        pass

class DragonStrategy(BaseStrategy):
    """
    小资金激进打板/龙头策略 (默认策略)
    移植自原 BacktestEngine 的硬编码逻辑
    """

    def check_risk(self, context: Dict) -> Tuple[bool, str]:
        # 1. 情绪过热反指
        try:
            day_signals = context.get('day_signals')
            code = context.get('code')
            if day_signals is not None:
                authors = day_signals[day_signals['stock_code'].astype(str).str.zfill(6) == str(code).zfill(6)]['author_name'].unique()
                if len(authors) > self.config.max_sentiment_score:
                    return False, f"情绪过热: {len(authors)}位博主同推"
        except Exception:
            pass

        # 2. 市场数据检查
        price_data = context.get('price_data')
        if price_data is None:
            return False, "SKIP (No Data)"

        # 3. 流动性过滤
        amount = price_data['amount']
        if amount < self.config.min_amount:
            return False, f"流动性不足:成交额{amount/10000:.0f}万<{self.config.min_amount/10000:.0f}万"

        turn = price_data['turn']
        if turn > 0:
            circ_mv = amount / (turn / 100)
            if circ_mv < self.config.min_circ_mv:
                return False, f"小市值:流通市值{circ_mv/100000000:.1f}亿<{self.config.min_circ_mv/100000000:.0f}亿"

        # 4. 短期暴涨过滤
        history_df = context.get('history_df')
        signal_date_str = context.get('signal_date_str')
        
        if history_df is not None and signal_date_str in history_df.index:
            try:
                idx = history_df.index.get_loc(signal_date_str)
                if idx >= 3:
                    close_t = history_df.iloc[idx]['close']
                    close_prev_3 = history_df.iloc[idx - 3]['close']
                    pct_3d = (close_t - close_prev_3) / close_prev_3 * 100
                    if pct_3d > self.config.max_3d_pct:
                        return False, f"高位接盘风险:3日涨幅{pct_3d:.1f}%>{self.config.max_3d_pct}%"
            except Exception:
                pass

        return True, "SAFE"

    def on_open_auction(self, context: Dict) -> Tuple[bool, str, str]:
        open_price = context.get('open_price')
        pre_close = context.get('pre_close')
        
        if open_price is None or pre_close is None:
            return False, "DATA_ERR", "竞价数据缺失"

        open_pct = (open_price - pre_close) / pre_close * 100

        if open_pct > self.config.max_open_pct:
            return False, "SKIP_HIGH_OPEN", f"高开幅度过大({open_pct:.2f}%)"
        
        if open_pct < self.config.min_open_pct:
            return False, "SKIP_LOW_OPEN", f"低开幅度过大({open_pct:.2f}%)"

        return True, "READY", f"竞价符合预期({open_pct:.2f}%)"

    def on_intraday(self, context: Dict) -> Tuple[Optional[float], str, str]:
        minute_df = context.get('minute_df')
        pre_close = context.get('pre_close')
        
        if minute_df is None or minute_df.empty:
            return None, "NO_DATA", "无法获取分钟数据"

        # 简单模拟盘中择时：价格 > VWAP
        for i in range(len(minute_df)):
            row = minute_df.iloc[i]
            price = row['close']
            vwap = row['vwap']
            
            # 必须未封涨停
            is_limit_up = price >= pre_close * 1.099 # 简单估算
            
            if price > vwap * self.config.vwap_threshold and not is_limit_up:
                buy_price = price
                buy_time = str(row['time'])
                return buy_price, "BUY_VWAP", f"盘中择时: {buy_time} Close({buy_price})>VWAP({vwap:.2f})"

        return None, "MISS", "未触发买入条件"

    def on_exit(self, context: Dict) -> Tuple[str, float, float, str, str]:
        history_df = context.get('history_df')
        buy_date_str = context.get('buy_date_str')
        buy_price = context.get('buy_price')
        
        if history_df is None:
            return None, buy_price, 0, "ERROR", "无历史数据"

        all_dates = history_df.index.tolist()
        try:
            start_pos = all_dates.index(buy_date_str)
        except ValueError:
            return None, buy_price, 0, "ERROR", "买入日期缺失数据"

        # 计算 ATR
        atr = self._calculate_atr(history_df, buy_date_str)
        if atr is None:
            atr = buy_price * self.config.default_atr_volatility

        stop_loss = buy_price - self.config.stop_loss_atr_multiplier * atr
        
        remaining_ratio = 1.0
        realized_pnl_amount = 0.0
        sell_logs = []

        # 遍历后续交易日
        for i in range(start_pos + 1, len(all_dates)):
            curr_date = all_dates[i]
            row = history_df.loc[curr_date]
            
            open_p = row['open']
            low_p = row['low']
            close_p = row['close']
            pre_close = history_df.iloc[i - 1]['close'] if i - 1 >= 0 else None
            
            days_held = i - start_pos

            # 一字跌停无法卖出
            if self._is_limit_down(open_p, low_p, close_p, pre_close):
                sell_logs.append((curr_date, close_p, 0, "一字跌停无法卖出"))
                continue

            # 移动止损
            if close_p > buy_price:
                new_stop = close_p - self.config.stop_loss_atr_multiplier * atr
                if new_stop > stop_loss:
                    stop_loss = new_stop

            # 1. 开盘止盈
            if days_held == 1:
                open_change = (open_p - buy_price) / buy_price
                if open_change > self.config.take_profit_open_pct:
                    sell_ratio = self.config.take_profit_ratio * remaining_ratio
                    realized_pnl_amount += sell_ratio * (open_p - buy_price)
                    remaining_ratio -= sell_ratio
                    sell_logs.append((curr_date, open_p, sell_ratio, f"开盘止盈(>{self.config.take_profit_open_pct:.0%})"))

            # 2. ATR 止损
            if low_p < stop_loss:
                exec_price = close_p # 简化：按收盘价止损，实际可能在 stop_loss 附近
                sell_ratio = remaining_ratio
                realized_pnl_amount += sell_ratio * (exec_price - buy_price)
                remaining_ratio = 0
                sell_logs.append((curr_date, exec_price, sell_ratio, f"ATR止损(Low{low_p:.2f}<Stop{stop_loss:.2f})"))
                break

            # 3. 时间止损
            curr_float_pnl = (close_p - buy_price) / buy_price
            if days_held >= self.config.max_holding_days and curr_float_pnl < 0:
                sell_ratio = remaining_ratio
                realized_pnl_amount += sell_ratio * (close_p - buy_price)
                remaining_ratio = 0
                sell_logs.append((curr_date, close_p, sell_ratio, f"时间止损({self.config.max_holding_days}日浮亏)"))
                break
            
            if remaining_ratio <= 0:
                break

        # 强制平仓
        if remaining_ratio > 0:
            last_date = all_dates[-1]
            last_close = history_df.iloc[-1]['close']
            realized_pnl_amount += remaining_ratio * (last_close - buy_price)
            sell_logs.append((last_date, last_close, remaining_ratio, "回测结束强制平仓"))

        total_profit_pct = (realized_pnl_amount / buy_price) * 100
        final_sell_date = sell_logs[-1][0] if sell_logs else buy_date_str
        final_sell_price = sell_logs[-1][1] if sell_logs else buy_price
        final_reason = " | ".join([log[3] for log in sell_logs]) if sell_logs else "Holding"

        return final_sell_date, final_sell_price, total_profit_pct, "SOLD", final_reason

    def _calculate_atr(self, df: pd.DataFrame, date_str: str, period: int = 14) -> Optional[float]:
        if date_str not in df.index: return None
        idx = df.index.get_loc(date_str)
        if idx < period: return None
        
        start_idx = idx - period - 1
        if start_idx < 0: start_idx = 0
        
        subset = df.iloc[start_idx : idx + 1].copy()
        subset['h-l'] = subset['high'] - subset['low']
        subset['h-cp'] = abs(subset['high'] - subset['close'].shift(1))
        subset['l-cp'] = abs(subset['low'] - subset['close'].shift(1))
        subset['tr'] = subset[['h-l', 'h-cp', 'l-cp']].max(axis=1)
        return subset['tr'].rolling(window=period).mean().iloc[-1]

    def _is_limit_down(self, open_p, low_p, close_p, pre_close):
        if pre_close is None or pre_close == 0: return False
        if open_p == low_p == close_p and close_p <= pre_close * 0.902:
            return True
        return (close_p - pre_close) / pre_close <= -0.098
