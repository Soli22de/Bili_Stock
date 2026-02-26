import pandas as pd
import akshare as ak
import backtrader as bt
from datetime import datetime
import os
import sys

# 设置支持中文显示
import matplotlib.pyplot as plt
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

class XueqiuSignalStrategy(bt.Strategy):
    params = (
        ('printlog', True),
    )

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        if self.params.printlog:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # 记录已持仓股票
        self.orders = {}
        
    def next(self):
        # 获取当前日期
        current_date = self.datas[0].datetime.date(0)
        
        # 检查是否有当天的信号
        if current_date in self.cerebro.signals_dict:
            todays_signals = self.cerebro.signals_dict[current_date]
            
            for signal in todays_signals:
                stock_code = signal['stock_code']
                action = signal['action']
                delta = signal['delta']
                
                # 转换股票代码格式以匹配数据源
                # 假设数据源中的 data name 是 akshare 格式 (e.g. sh600519)
                # 而信号中的 stock_code 可能是 SH600519
                # 需要根据实际情况调整匹配逻辑
                
                # 简单起见，我们假设只回测特定几只股票，并且 data 已经加载
                # 查找对应的 data feed
                # 使用 try-except 避免 KeyError
                try:
                    data = self.getdatabyname(stock_code)
                except KeyError:
                    # 可能是因为我们只加载了部分数据，而信号涉及其他股票
                    continue
                
                if data:
                    position = self.getposition(data).size
                    
                    if action == 'BUY':
                        # 买入逻辑：按仓位比例买入？或者按固定金额？
                        # 这里简单模拟：每条买入信号买入 10% 资金
                        target_value = self.broker.getvalue() * 0.1
                        self.order_target_value(data=data, target=target_value)
                        self.log(f'BUY CREATE, {stock_code}, Price: {data.close[0]}')
                        
                    elif action == 'SELL':
                        # 卖出逻辑：清仓
                        if position > 0:
                            self.close(data=data)
                            self.log(f'SELL CREATE, {stock_code}, Price: {data.close[0]}')

def load_signals(file_path):
    """加载并过滤信号"""
    if not os.path.exists(file_path):
        print("信号文件不存在")
        return {}
        
    df = pd.read_csv(file_path)
    
    # 过滤 ST 股 (保留 ETF)
    # 简单规则：名字包含 ST 的剔除
    # 注意：ETF 名字通常包含 ETF 或 51/15 开头代码
    
    # 1. 剔除名字含 ST
    df = df[~df['stock_name'].str.contains('ST', na=False, case=False)]
    
    # 2. 剔除退市 (可选)
    
    print(f"过滤后剩余 {len(df)} 条信号")
    
    # 按日期组织信号
    signals_dict = {}
    for _, row in df.iterrows():
        try:
            dt = datetime.strptime(row['time'], '%Y-%m-%d %H:%M:%S').date()
            if dt not in signals_dict:
                signals_dict[dt] = []
            
            signals_dict[dt].append({
                'stock_code': row['stock_code'],
                'action': row['action'],
                'delta': row['delta']
            })
        except Exception as e:
            print(f"解析日期失败: {row['time']}")
            
    return signals_dict

def run_backtest():
    # 1. 加载信号
    signals_file = "data/cube_rebalancing.csv"
    signals_dict = load_signals(signals_file)
    
    if not signals_dict:
        print("没有有效信号，无法回测")
        return

    # 2. 初始化 Cerebro
    cerebro = bt.Cerebro()
    cerebro.addstrategy(XueqiuSignalStrategy)
    
    # 将信号注入 cerebro (作为自定义属性)
    cerebro.signals_dict = signals_dict
    
    # 3. 确定回测时间范围和股票池
    # 提取所有涉及的股票代码
    all_stocks = set()
    start_date = None
    end_date = None
    
    for dt, signals in signals_dict.items():
        if start_date is None or dt < start_date: start_date = dt
        if end_date is None or dt > end_date: end_date = dt
        for s in signals:
            all_stocks.add(s['stock_code'])
            
    print(f"回测区间: {start_date} 至 {end_date}")
    print(f"涉及股票: {len(all_stocks)} 只")
    
    # 4. 加载历史数据 (从 Akshare)
    # 实际应用中需要构建本地数据库
    count = 0
    # 选择前20只股票进行回测
    selected_stocks = list(all_stocks)[:20]
    
    for stock_code in selected_stocks: 
        print(f"正在加载数据: {stock_code}...")
        
        # 转换代码格式：SZ000711 -> sz000711 (Akshare通常需要纯数字或带前缀)
        # akshare.stock_zh_a_hist 需要 6 位代码
        symbol = stock_code[2:] # 去掉前缀
        
        try:
            # 获取日线数据
            stock_df = ak.stock_zh_a_hist(symbol=symbol, start_date="20240101", end_date="20260218", adjust="qfq")
            
            if stock_df.empty:
                print(f"无数据: {stock_code}")
                continue
                
            stock_df['date'] = pd.to_datetime(stock_df['日期'])
            stock_df.set_index('date', inplace=True)
            
            data = bt.feeds.PandasData(
                dataname=stock_df,
                open='开盘',
                high='最高',
                low='最低',
                close='收盘',
                volume='成交量',
                openinterest=-1
            )
            
            # 使用原始代码作为名称，方便策略中匹配
            cerebro.adddata(data, name=stock_code)
            count += 1
            
        except Exception as e:
            print(f"加载数据失败 {stock_code}: {e}")
            
    if count == 0:
        print("没有成功加载任何股票数据")
        return

    # 5. 设置资金
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.0003) # 万三佣金

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # 6. 运行
    cerebro.run()

    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    
    # 7. 绘图
    # cerebro.plot() # 在服务器环境可能无法显示

if __name__ == '__main__':
    run_backtest()
