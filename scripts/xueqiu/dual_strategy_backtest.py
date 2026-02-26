
import backtrader as bt
import pandas as pd
import akshare as ak
import datetime
import os
import sys
import matplotlib.pyplot as plt
import numpy as np

# Ensure parent directory is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.xueqiu.full_backtest_engine import FullBacktestEngine, XueqiuAdvancedStrategy

# Configure plotting style
plt.style.use('seaborn-v0_8')
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

# ==========================================
# 策略B：全仓轮动 (Small Capital, Aggressive)
# ==========================================
class StrategyB_Aggressive(bt.Strategy):
    """
    策略B：全仓轮动 (All-in Rotation)
    - 资金规模：小资金 (e.g. 10万)
    - 仓位管理：单票 50% - 100% (集中持仓)
    - 止损：严格 (3-5%)
    - 止盈：趋势跟随 (15%+)
    - 换手率：高
    """
    params = (
        ('stop_loss', 0.05),      # 止损 5%
        ('take_profit', 0.15),    # 止盈 15% (初始目标)
        ('max_positions', 1),     # 最大持仓数: 1 (全仓)
        ('position_size', 0.98),  # 单笔仓位 98% (留点现金防滑点)
        ('cooldown_days', 0),     # 无冷却，敢于追涨
    )

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        # print(f'[Strategy B] {dt.isoformat()}, {txt}')

    def __init__(self):
        self.entry_prices = {}
        self.buy_dates = {}
        self.total_value = []      # Record daily value
        self.dates = []            # Record dates

    def next(self):
        # Record Value
        self.total_value.append(self.broker.getvalue())
        self.dates.append(self.datas[0].datetime.date(0))

        current_date = self.datas[0].datetime.date(0)
        
        # 1. 风险控制 (Stop Loss / Take Profit)
        for data in self.datas:
            stock_code = data._name
            position = self.getposition(data).size
            if position > 0:
                price = data.close[0]
                entry_price = self.entry_prices.get(stock_code, price)
                
                # 止损
                if price < entry_price * (1 - self.params.stop_loss):
                    self.close(data=data)
                    self.log(f'STOP LOSS: {stock_code}, Price: {price:.2f}, Entry: {entry_price:.2f}')
                    del self.entry_prices[stock_code]
                
                # 移动止盈 (简化版: 超过20%后回撤5%止盈)
                elif price > entry_price * (1 + self.params.take_profit):
                    self.close(data=data)
                    self.log(f'TAKE PROFIT: {stock_code}, Price: {price:.2f}, Entry: {entry_price:.2f}')
                    del self.entry_prices[stock_code]

        # 2. 信号处理
        if current_date in self.cerebro.signals_dict:
            todays_signals = self.cerebro.signals_dict[current_date]
            current_positions = sum(1 for d in self.datas if self.getposition(d).size > 0)
            
            for signal in todays_signals:
                stock_code = signal['stock_code']
                action = signal['action']
                try:
                    data = self.getdatabyname(stock_code)
                except KeyError:
                    continue
                if not data: continue
                
                position = self.getposition(data).size
                
                if action == 'BUY':
                    if current_positions >= self.params.max_positions:
                        continue
                        
                    if position == 0:
                        # 全仓买入
                        self.order_target_percent(data=data, target=self.params.position_size)
                        self.entry_prices[stock_code] = data.close[0]
                        self.log(f'BUY ALL-IN: {stock_code}, Price: {data.close[0]:.2f}')
                        current_positions += 1
                        
                elif action == 'SELL':
                    if position > 0:
                        self.close(data=data)
                        if stock_code in self.entry_prices: del self.entry_prices[stock_code]
                        self.log(f'SELL SIGNAL: {stock_code}, Price: {data.close[0]:.2f}')

# ==========================================
# 策略C：行业贝塔 (Large Capital, Sector)
# ==========================================
class StrategyC_Sector(bt.Strategy):
    """
    策略C：行业贝塔 (Sector Beta)
    - 资金规模：大资金 (e.g. 1000万)
    - 仓位管理：单票 10% (分散持仓)
    - 止损：宽 (10%)
    - 止盈：趋势跟随 (30%+)
    - 换手率：低
    """
    params = (
        ('stop_loss', 0.10),      # 止损 10%
        ('take_profit', 0.30),    # 止盈 30%
        ('max_positions', 10),    # 最大持仓数: 10 (分散)
        ('position_size', 0.09),  # 单笔仓位 9% (留现金)
        ('cooldown_days', 5),     # 有冷却，避免频繁交易
    )

    def log(self, txt, dt=None):
        dt = dt or self.datas[0].datetime.date(0)
        # print(f'[Strategy C] {dt.isoformat()}, {txt}')

    def __init__(self):
        self.entry_prices = {}
        self.buy_dates = {}
        self.total_value = []      # Record daily value
        self.dates = []            # Record dates

    def next(self):
        # Record Value
        self.total_value.append(self.broker.getvalue())
        self.dates.append(self.datas[0].datetime.date(0))

        current_date = self.datas[0].datetime.date(0)
        
        # 1. 风险控制
        for data in self.datas:
            stock_code = data._name
            position = self.getposition(data).size
            if position > 0:
                price = data.close[0]
                entry_price = self.entry_prices.get(stock_code, price)
                
                if price < entry_price * (1 - self.params.stop_loss):
                    self.close(data=data)
                    self.log(f'STOP LOSS: {stock_code}')
                    del self.entry_prices[stock_code]
                elif price > entry_price * (1 + self.params.take_profit):
                    self.close(data=data)
                    self.log(f'TAKE PROFIT: {stock_code}')
                    del self.entry_prices[stock_code]

        # 2. 信号处理
        if current_date in self.cerebro.signals_dict:
            todays_signals = self.cerebro.signals_dict[current_date]
            current_positions = sum(1 for d in self.datas if self.getposition(d).size > 0)
            
            for signal in todays_signals:
                stock_code = signal['stock_code']
                action = signal['action']
                try:
                    data = self.getdatabyname(stock_code)
                except KeyError:
                    continue
                if not data: continue
                
                position = self.getposition(data).size
                
                if action == 'BUY':
                    if current_positions >= self.params.max_positions:
                        continue
                        
                    if position == 0:
                        # 检查冷却期
                        last_buy = self.buy_dates.get(stock_code)
                        if last_buy:
                            days_since = (current_date - last_buy).days
                            if days_since < self.params.cooldown_days:
                                continue

                        self.order_target_percent(data=data, target=self.params.position_size)
                        self.entry_prices[stock_code] = data.close[0]
                        self.buy_dates[stock_code] = current_date
                        self.log(f'BUY SECTOR: {stock_code}')
                        current_positions += 1
                        
                elif action == 'SELL':
                    if position > 0:
                        self.close(data=data)
                        if stock_code in self.entry_prices: del self.entry_prices[stock_code]
                        self.log(f'SELL SIGNAL: {stock_code}')

class DualBacktestEngine(FullBacktestEngine):
    def run_strategy(self, strategy_cls, strategy_name, initial_capital):
        print(f"\n🚀 开始回测: {strategy_name}")
        print(f"   初始资金: {initial_capital:,.2f}")
        
        # 重置 Cerebro
        self.cerebro = bt.Cerebro()
        
        # 1. 加载信号
        signals_dict = self.load_signals()
        self.cerebro.signals_dict = signals_dict
        
        # 2. 加载数据
        stock_counts = {}
        for dt in signals_dict:
            for s in signals_dict[dt]:
                code = s['stock_code']
                stock_counts[code] = stock_counts.get(code, 0) + 1
        sorted_stocks = sorted(stock_counts.items(), key=lambda x: x[1], reverse=True)
        target_stocks = [x[0] for x in sorted_stocks[:50]]
        
        count = 0
        for stock_code in target_stocks:
            df = self.get_stock_data(stock_code)
            if df is not None:
                data = bt.feeds.PandasData(
                    dataname=df,
                    open='开盘', high='最高', low='最低', close='收盘', volume='成交量', openinterest=-1
                )
                self.cerebro.adddata(data, name=stock_code)
                count += 1
        print(f"   已加载 {count} 只股票数据")

        # 3. 配置策略
        self.cerebro.addstrategy(strategy_cls)
        self.cerebro.broker.setcash(initial_capital)
        self.cerebro.broker.setcommission(commission=0.0003)
        
        # 4. 分析器
        self.cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        self.cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
        
        # 5. 运行
        results = self.cerebro.run()
        strat = results[0]
        
        # 6. 输出结果
        end_value = self.cerebro.broker.getvalue()
        total_return = (end_value - initial_capital) / initial_capital * 100
        sharpe = strat.analyzers.sharpe.get_analysis().get('sharperatio', 0)
        max_dd = strat.analyzers.drawdown.get_analysis()['max']['drawdown']
        
        if sharpe is None: sharpe = 0.0
        
        print(f"🏁 回测结束: {strategy_name}")
        print(f"   最终资金: {end_value:,.2f}")
        print(f"   总收益率: {total_return:.2f}%")
        print(f"   夏普比率: {sharpe:.2f}")
        print(f"   最大回撤: {max_dd:.2f}%")
        
        return {
            'name': strategy_name,
            'return': total_return,
            'sharpe': sharpe,
            'max_dd': max_dd,
            'dates': strat.dates,
            'values': strat.total_value
        }

    def run_dual_test(self):
        # 运行策略 B
        res_b = self.run_strategy(StrategyB_Aggressive, "策略B (全仓轮动)", 100000.0)
        
        # 运行策略 C
        res_c = self.run_strategy(StrategyC_Sector, "策略C (行业贝塔)", 10000000.0)
        
        # 对比绘图
        plt.figure(figsize=(12, 6))
        
        # 策略B 净值
        df_b = pd.DataFrame({'value': res_b['values']}, index=pd.to_datetime(res_b['dates']))
        if not df_b.empty:
            df_b['nav'] = df_b['value'] / df_b['value'].iloc[0]
            plt.plot(df_b.index, df_b['nav'], label=f"策略B: 全仓轮动 (+{res_b['return']:.1f}%)", color='red')
            
        # 策略C 净值
        df_c = pd.DataFrame({'value': res_c['values']}, index=pd.to_datetime(res_c['dates']))
        if not df_c.empty:
            df_c['nav'] = df_c['value'] / df_c['value'].iloc[0]
            plt.plot(df_c.index, df_c['nav'], label=f"策略C: 行业贝塔 (+{res_c['return']:.1f}%)", color='blue')
            
        plt.title('双策略表现对比 (Strategy B vs C)')
        plt.xlabel('日期')
        plt.ylabel('净值 (归一化)')
        plt.legend()
        plt.grid(True)
        
        output_path = 'analysis/dual_strategy_comparison.png'
        plt.savefig(output_path)
        print(f"\n✅ 对比图表已保存至: {output_path}")

if __name__ == "__main__":
    engine = DualBacktestEngine()
    engine.run_dual_test()
