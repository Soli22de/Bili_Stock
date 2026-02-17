import backtrader as bt
import pandas as pd
import akshare as ak
import datetime
import os
import sys
import matplotlib.pyplot as plt

# 解决中文显示问题
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

class XueqiuAdvancedStrategy(bt.Strategy):
    params = (
        ('printlog', True),
        ('stop_loss', 0.10),      # 止损 10%
        ('take_profit', 0.20),    # 止盈 20%
        ('max_positions', 10),    # 最大持仓数
        ('position_size', 0.1),   # 单笔仓位 10%
        ('cooldown_days', 5),     # 买入冷却期
    )

    def log(self, txt, dt=None):
        if self.params.printlog:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.orders = {}           # 记录订单状态
        self.buy_dates = {}        # 记录买入日期用于冷却
        self.total_value = []      # 记录每日净值
        self.dates = []            # 记录日期序列
        
        # 记录每只股票的入场价格，用于止损止盈
        self.entry_prices = {}

    def next(self):
        # 记录当前净值
        self.total_value.append(self.broker.getvalue())
        self.dates.append(self.datas[0].datetime.date(0))
        
        current_date = self.datas[0].datetime.date(0)
        
        # 1. 遍历持仓，检查止损止盈
        for data in self.datas:
            stock_code = data._name
            position = self.getposition(data).size
            
            if position > 0:
                price = data.close[0]
                entry_price = self.entry_prices.get(stock_code)
                
                if entry_price:
                    # 止损检查
                    if price < entry_price * (1 - self.params.stop_loss):
                        self.close(data=data)
                        self.log(f'STOP LOSS: {stock_code}, Price: {price:.2f}, Entry: {entry_price:.2f}')
                        del self.entry_prices[stock_code]
                        
                    # 止盈检查
                    elif price > entry_price * (1 + self.params.take_profit):
                        self.close(data=data)
                        self.log(f'TAKE PROFIT: {stock_code}, Price: {price:.2f}, Entry: {entry_price:.2f}')
                        del self.entry_prices[stock_code]

        # 2. 处理当日信号
        if current_date in self.cerebro.signals_dict:
            todays_signals = self.cerebro.signals_dict[current_date]
            
            # 计算当前持仓数量
            current_positions = sum(1 for d in self.datas if self.getposition(d).size > 0)
            
            for signal in todays_signals:
                stock_code = signal['stock_code']
                action = signal['action']
                
                # 查找数据源
                try:
                    data = self.getdatabyname(stock_code)
                except KeyError:
                    # 如果数据未加载，跳过
                    continue
                
                if not data:
                    continue
                    
                position = self.getposition(data).size
                
                # 买入逻辑
                if action == 'BUY':
                    # 检查持仓限制
                    if current_positions >= self.params.max_positions:
                        continue
                        
                    # 检查是否已持仓
                    if position > 0:
                        continue
                        
                    # 检查冷却期
                    last_buy = self.buy_dates.get(stock_code)
                    if last_buy:
                        days_since = (current_date - last_buy).days
                        if days_since < self.params.cooldown_days:
                            continue
                    
                    # 执行买入
                    target_value = self.broker.getvalue() * self.params.position_size
                    self.order_target_value(data=data, target=target_value)
                    self.entry_prices[stock_code] = data.close[0]
                    self.buy_dates[stock_code] = current_date
                    self.log(f'BUY EXECUTED: {stock_code}, Price: {data.close[0]:.2f}')
                    current_positions += 1
                    
                # 卖出逻辑 (跟随组合调仓卖出)
                elif action == 'SELL':
                    if position > 0:
                        self.close(data=data)
                        if stock_code in self.entry_prices:
                            del self.entry_prices[stock_code]
                        self.log(f'SELL EXECUTED: {stock_code}, Price: {data.close[0]:.2f}')

class FullBacktestEngine:
    def __init__(self, signals_file="data/cube_rebalancing.csv"):
        self.signals_file = signals_file
        self.cerebro = bt.Cerebro()
        self.benchmark_data = None
        self.start_date = datetime.date(2022, 1, 1)  # 覆盖熊市
        self.end_date = datetime.date(2026, 2, 15)   # 覆盖牛市
        
        # 确保数据目录存在
        os.makedirs("data/cache", exist_ok=True)

    def load_signals(self):
        """加载并预处理信号"""
        if not os.path.exists(self.signals_file):
            print("信号文件不存在")
            return {}
            
        df = pd.read_csv(self.signals_file)
        
        # 过滤ST股
        df = df[~df['stock_name'].str.contains('ST', na=False, case=False)]
        
        # 转换为字典格式
        signals_dict = {}
        for _, row in df.iterrows():
            try:
                dt = datetime.datetime.strptime(row['time'], '%Y-%m-%d %H:%M:%S').date()
                if dt < self.start_date or dt > self.end_date:
                    continue
                    
                if dt not in signals_dict:
                    signals_dict[dt] = []
                
                signals_dict[dt].append({
                    'stock_code': row['stock_code'],
                    'action': row['action'],
                    'delta': row['delta']
                })
            except Exception as e:
                pass
                
        return signals_dict

    def get_stock_data(self, stock_code):
        """获取股票数据（带缓存）"""
        cache_file = f"data/cache/{stock_code}.csv"
        symbol = stock_code[2:]
        
        # 1. 尝试读取缓存
        if os.path.exists(cache_file):
            try:
                df = pd.read_csv(cache_file, index_col='date', parse_dates=['date'])
                return df
            except:
                pass
                
        # 2. 从Akshare获取
        try:
            print(f"下载数据: {stock_code}")
            start_str = self.start_date.strftime("%Y%m%d")
            end_str = self.end_date.strftime("%Y%m%d")
            
            df = ak.stock_zh_a_hist(symbol=symbol, start_date=start_str, end_date=end_str, adjust="qfq")
            if df.empty:
                return None
                
            df['date'] = pd.to_datetime(df['日期'])
            df.set_index('date', inplace=True)
            df.sort_index(inplace=True)
            
            # 保存缓存
            df.to_csv(cache_file)
            return df
        except Exception as e:
            print(f"下载失败 {stock_code}: {e}")
            return None

    def get_benchmark_data(self):
        """获取沪深300基准数据"""
        cache_file = "data/cache/sh000300.csv"
        
        if os.path.exists(cache_file):
            return pd.read_csv(cache_file, index_col='date', parse_dates=['date'])
            
        try:
            print("下载基准数据: 沪深300")
            start_str = self.start_date.strftime("%Y%m%d")
            end_str = self.end_date.strftime("%Y%m%d")
            
            df = ak.stock_zh_index_daily(symbol="sh000300")
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            
            # 筛选时间段
            df = df[(df.index.date >= self.start_date) & (df.index.date <= self.end_date)]
            
            df.to_csv(cache_file)
            return df
        except Exception as e:
            print(f"基准数据下载失败: {e}")
            return None

    def run(self):
        # 1. 加载信号
        print("1. 加载交易信号...")
        signals_dict = self.load_signals()
        self.cerebro.signals_dict = signals_dict
        
        # 2. 确定股票池
        all_stocks = set()
        for dt in signals_dict:
            for s in signals_dict[dt]:
                all_stocks.add(s['stock_code'])
        
        print(f"   涉及股票数量: {len(all_stocks)}")
        
        # 3. 加载数据 feeds
        print("2. 加载历史数据...")
        count = 0
        
        # 为了演示效率，我们取信号最多的前50只股票
        stock_counts = {}
        for dt in signals_dict:
            for s in signals_dict[dt]:
                code = s['stock_code']
                stock_counts[code] = stock_counts.get(code, 0) + 1
                
        sorted_stocks = sorted(stock_counts.items(), key=lambda x: x[1], reverse=True)
        target_stocks = [x[0] for x in sorted_stocks[:50]]
        
        for stock_code in target_stocks:
            df = self.get_stock_data(stock_code)
            if df is not None:
                data = bt.feeds.PandasData(
                    dataname=df,
                    open='开盘',
                    high='最高',
                    low='最低',
                    close='收盘',
                    volume='成交量',
                    openinterest=-1
                )
                self.cerebro.adddata(data, name=stock_code)
                count += 1
                
        print(f"   成功加载 {count} 只股票数据")
        
        # 4. 配置策略和资金
        self.cerebro.addstrategy(XueqiuAdvancedStrategy)
        self.cerebro.broker.setcash(1000000.0)  # 100万初始资金
        self.cerebro.broker.setcommission(commission=0.0003)
        
        # 添加分析器
        self.cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
        self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        self.cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='annual')
        self.cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
        
        # 5. 运行回测
        print("3. 开始回测...")
        results = self.cerebro.run()
        strat = results[0]
        
        # 6. 生成报告
        self.generate_report(strat)

    def generate_report(self, strat):
        print("\n" + "="*50)
        print("📊 全量化回测报告 (2022-2026)")
        print("="*50)
        
        # 1. 总体表现
        start_value = 1000000.0
        end_value = self.cerebro.broker.getvalue()
        total_return = (end_value - start_value) / start_value * 100
        
        print(f"初始资金: {start_value:,.2f}")
        print(f"最终资金: {end_value:,.2f}")
        print(f"总收益率: {total_return:.2f}%")
        
        # 2. 风险指标
        sharpe = strat.analyzers.sharpe.get_analysis().get('sharperatio', 0)
        max_drawdown = strat.analyzers.drawdown.get_analysis()['max']['drawdown']
        
        print(f"夏普比率: {sharpe:.2f}")
        print(f"最大回撤: {max_drawdown:.2f}%")
        
        # 3. 交易统计
        trades = strat.analyzers.trades.get_analysis()
        total_trades = trades.get('total', {}).get('total', 0)
        if total_trades > 0:
            win_rate = trades.get('won', {}).get('total', 0) / total_trades * 100
            print(f"交易次数: {total_trades}")
            print(f"胜率: {win_rate:.2f}%")
        
        # 4. 基准对比
        benchmark_df = self.get_benchmark_data()
        if benchmark_df is not None:
            # 计算基准收益率
            start_idx = benchmark_df.index.searchsorted(pd.Timestamp(self.start_date))
            if start_idx < len(benchmark_df):
                bench_start = benchmark_df.iloc[start_idx]['close']
                bench_end = benchmark_df.iloc[-1]['close']
                bench_return = (bench_end - bench_start) / bench_start * 100
                
                print("\n📈 业绩对比")
                print(f"策略收益: {total_return:.2f}%")
                print(f"沪深300: {bench_return:.2f}%")
                print(f"超额收益: {total_return - bench_return:.2f}%")
                
                # 绘制对比图
                self.plot_comparison(strat, benchmark_df)

    def plot_comparison(self, strat, benchmark_df):
        """绘制策略vs基准对比图"""
        try:
            # 提取策略净值曲线
            strategy_dates = strat.dates
            strategy_values = strat.total_value
            
            # 创建策略DataFrame
            df_strat = pd.DataFrame({'value': strategy_values}, index=pd.to_datetime(strategy_dates))
            # 归一化
            df_strat['nav'] = df_strat['value'] / df_strat['value'].iloc[0]
            
            # 处理基准数据
            benchmark_df = benchmark_df[benchmark_df.index.isin(df_strat.index)]
            benchmark_df['nav'] = benchmark_df['close'] / benchmark_df['close'].iloc[0]
            
            # 绘图
            plt.figure(figsize=(12, 6))
            plt.plot(df_strat.index, df_strat['nav'], label='雪球跟单策略', color='red', linewidth=2)
            plt.plot(benchmark_df.index, benchmark_df['nav'], label='沪深300基准', color='gray', linestyle='--')
            
            plt.title('全周期回测对比 (2022熊市 - 2025牛市)')
            plt.xlabel('日期')
            plt.ylabel('净值 (归一化)')
            plt.legend()
            plt.grid(True)
            
            # 保存图表
            plt.savefig('analysis/full_backtest_report.png')
            print("\n✅ 回测图表已保存至: analysis/full_backtest_report.png")
            
        except Exception as e:
            print(f"绘图失败: {e}")

if __name__ == "__main__":
    engine = FullBacktestEngine()
    engine.run()