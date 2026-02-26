import backtrader as bt
import pandas as pd
import akshare as ak
import datetime
import os
import sys
import json
import collections
import matplotlib.pyplot as plt

# 解决中文显示问题
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False

class HighWinRateStrategy(bt.Strategy):
    """Base Strategy with Win Rate Calculation"""
    params = (
        ('printlog', True),
        ('stop_loss', 0.10),      # 止损 10%
        ('take_profit', 0.20),    # 止盈 20%
        ('max_positions', 10),    # 最大持仓数
        ('position_size', 0.1),   # 单笔仓位 10%
        ('cooldown_days', 5),     # 买入冷却期
        ('trend_period', 60),     # 趋势过滤周期 (MA60), 0=禁用
    )

    def log(self, txt, dt=None):
        if self.params.printlog:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.orders = {}           # 记录订单状态
        self.buy_dates = {}        # 记录买入日期用于冷却
        self.entry_prices = {}     # 记录入场价格
        
        # Win Rate Metrics
        self.trades_closed = 0
        self.trades_won = 0
        self.total_profit = 0.0
        
        # Trend Indicators (Lazy initialization or pre-calc)
        # Note: In Backtrader, indicators must be created in __init__ for each data
        self.smas = {}
        if self.params.trend_period > 0:
            for data in self.datas:
                # Use a dictionary to map data object to its SMA
                # We use a try-except block in case data length < period initially, 
                # but Backtrader handles this by producing NaN.
                self.smas[data] = bt.indicators.SimpleMovingAverage(
                    data.close, period=self.params.trend_period, plot=False
                )

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log(f'OPERATION PROFIT, GROSS {trade.pnl:.2f}, NET {trade.pnlcomm:.2f}')
        
        self.trades_closed += 1
        if trade.pnl > 0:
            self.trades_won += 1
        self.total_profit += trade.pnl

    def stop(self):
        win_rate = (self.trades_won / self.trades_closed * 100) if self.trades_closed > 0 else 0
        print(f"\n=== Strategy Results ===")
        print(f"Total Trades: {self.trades_closed}")
        print(f"Win Rate: {win_rate:.1f}%")
        print(f"Total Profit: {self.total_profit:.2f}")
        print(f"Final Value: {self.broker.getvalue():.2f}")

    def next(self):
        current_date = self.datas[0].datetime.date(0)
        
        # 1. Check Positions for SL/TP
        for data in self.datas:
            stock_code = data._name
            position = self.getposition(data).size
            
            if position > 0:
                price = data.close[0]
                # Skip invalid price (padded data)
                if price < 0.01:
                    continue
                    
                entry_price = self.entry_prices.get(stock_code)
                
                if entry_price:
                    # Stop Loss
                    if price < entry_price * (1 - self.params.stop_loss):
                        self.close(data=data)
                        self.log(f'STOP LOSS: {stock_code}, Price: {price:.2f}, Entry: {entry_price:.2f}')
                        if stock_code in self.entry_prices: del self.entry_prices[stock_code]
                        
                    # Take Profit
                    elif price > entry_price * (1 + self.params.take_profit):
                        self.close(data=data)
                        self.log(f'TAKE PROFIT: {stock_code}, Price: {price:.2f}, Entry: {entry_price:.2f}')
                        if stock_code in self.entry_prices: del self.entry_prices[stock_code]

class StrategyD_Resonance(HighWinRateStrategy):
    """
    Strategy D: Resonance (Shadow Tracker)
    Buy when >= 2 Cubes buy within 5 days.
    """
    params = (
        ('resonance_window', 5), # Look back 5 days
        ('min_cubes', 2),        # Min 2 cubes
    )

    def next(self):
        super().next() # Run SL/TP checks
        
        current_date = self.datas[0].datetime.date(0)
        
        if current_date not in self.cerebro.signals_dict:
            return
        
        todays_signals = self.cerebro.signals_dict.get(current_date, [])
        
        current_positions = sum(1 for d in self.datas if self.getposition(d).size > 0)
        
        for signal in todays_signals:
            stock_code = signal['stock_code']
            action = signal['action'] # 'BUY' or 'SELL'
            
            # Find data feed
            try:
                data = self.getdatabyname(stock_code)
            except KeyError:
                continue
            
            if data.datetime.date(0) != current_date:
                continue
            
            position = self.getposition(data).size
            
            if action == 'BUY':
                if current_positions >= self.params.max_positions: continue
                if position > 0: continue
                
                # Check cooldown
                last_buy = self.buy_dates.get(stock_code)
                if last_buy and (current_date - last_buy).days < self.params.cooldown_days:
                    continue

                # Execute Buy
                target_value = self.broker.getvalue() * self.params.position_size
                self.order_target_value(data=data, target=target_value)
                self.entry_prices[stock_code] = data.close[0]
                self.buy_dates[stock_code] = current_date
                
                # Identify contributing cubes
                contributing_cubes = signal.get('cubes', [])
                
                self.log(f'RESONANCE BUY: {stock_code}, Price: {data.close[0]:.2f}, Cubes: {", ".join(contributing_cubes)}')
                current_positions += 1
                
            elif action == 'SELL':
                if position > 0:
                    self.close(data=data)
                    if stock_code in self.entry_prices: del self.entry_prices[stock_code]
                    self.log(f'RESONANCE SELL: {stock_code}, Price: {data.close[0]:.2f}')

class StrategyE_TopGuru(HighWinRateStrategy):
    """
    Strategy E: Top Guru Follow
    Follow trades of a single best performing cube.
    """
    params = (
        ('guru_symbol', 'ZH1745648'), # Default: 价值元年 (or 南极风暴 ZH583267)
    )

    def next(self):
        super().next()
        current_date = self.datas[0].datetime.date(0)
        
        if current_date not in self.cerebro.signals_dict:
            return
            
        todays_signals = self.cerebro.signals_dict.get(current_date, [])
        current_positions = sum(1 for d in self.datas if self.getposition(d).size > 0)
        
        for signal in todays_signals:
            stock_code = signal['stock_code']
            action = signal['action']
            cube_symbol = signal.get('cube_symbol')
            
            if cube_symbol != self.params.guru_symbol:
                continue
                
            try:
                data = self.getdatabyname(stock_code)
            except KeyError:
                continue

            if data.datetime.date(0) != current_date:
                continue
                
            if data.close[0] < 0.01: # Skip invalid/padded data
                continue
            
            # Trend Filter Check
            if self.params.trend_period > 0:
                sma = self.smas.get(data)
                if sma is not None:
                    # Check if SMA is valid (not NaN)
                    if sma[0] != sma[0]: # NaN check
                         continue
                    
                    if data.close[0] < sma[0]:
                        # self.log(f'TREND FILTER: Skip {stock_code}, Price {data.close[0]:.2f} < SMA {sma[0]:.2f}')
                        continue

            position = self.getposition(data).size
            
            if action == 'BUY':
                if current_positions >= self.params.max_positions: continue
                if position > 0: continue
                
                # Execute Buy
                target_value = self.broker.getvalue() * self.params.position_size
                self.order_target_value(data=data, target=target_value)
                self.entry_prices[stock_code] = data.close[0]
                self.buy_dates[stock_code] = current_date
                self.log(f'GURU BUY: {stock_code}, Price: {data.close[0]:.2f}')
                current_positions += 1
                
            elif action == 'SELL':
                if position > 0:
                    self.close(data=data)
                    if stock_code in self.entry_prices: del self.entry_prices[stock_code]
                    self.log(f'GURU SELL: {stock_code}, Price: {data.close[0]:.2f}')


class HighWinRateEngine:
    def __init__(self):
        self.cerebro = bt.Cerebro()
        self.start_date = datetime.date(2022, 1, 1) # Backtest 2022-2026 (Full Cycle)
        self.end_date = datetime.date(2026, 2, 15)
        self.signals = []
        
        # Clear cache to ensure full data range download
        # cache_dir = "data/cache"
        # if os.path.exists(cache_dir):
        #     import shutil
        #     shutil.rmtree(cache_dir)
        os.makedirs("data/cache", exist_ok=True)

    def load_signals(self):
        """Load signals from all JSON files in data/history"""
        print("Loading signals from data/history/...")
        signals_list = []
        data_dir = "data/history"
        
        if not os.path.exists(data_dir):
            print("No history data found.")
            return
            
        for f in os.listdir(data_dir):
            if f.endswith(".json"):
                symbol = f.replace(".json", "")
                path = os.path.join(data_dir, f)
                try:
                    with open(path, "r", encoding="utf-8") as file:
                        cube_signals = json.load(file)
                        for s in cube_signals:
                            s['cube_symbol'] = symbol
                            # Ensure date format
                            try:
                                dt = datetime.datetime.strptime(s['time'], '%Y-%m-%d %H:%M:%S').date()
                                s['date'] = dt
                                if self.start_date <= dt <= self.end_date:
                                    signals_list.append(s)
                            except:
                                pass
                except Exception as e:
                    print(f"Error loading {f}: {e}")
                    
        self.signals = sorted(signals_list, key=lambda x: x['date'])
        print(f"Loaded {len(self.signals)} signals.")

    def get_stock_data(self, stock_code):
        """Fetch stock data with cache"""
        cache_file = f"data/cache/{stock_code}.csv"
        symbol = stock_code[2:]
        
        df = None
        if os.path.exists(cache_file):
            try:
                df = pd.read_csv(cache_file, index_col='date', parse_dates=['date'])
            except:
                pass
                
        if df is None:
            try:
                # print(f"Downloading {stock_code}...")
                start_str = self.start_date.strftime("%Y%m%d")
                end_str = self.end_date.strftime("%Y%m%d")
                df = ak.stock_zh_a_hist(symbol=symbol, start_date=start_str, end_date=end_str, adjust="qfq")
                if df.empty: return None
                
                # Rename columns for Backtrader
                df.rename(columns={
                    '日期': 'date',
                    '开盘': 'open',
                    '收盘': 'close',
                    '最高': 'high',
                    '最低': 'low',
                    '成交量': 'volume',
                }, inplace=True)
                
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
                df.sort_index(inplace=True)
                
                # Ensure columns exist
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col not in df.columns:
                        df[col] = 0.0

                df.to_csv(cache_file)
            except Exception as e:
                # print(f"Error fetching {stock_code}: {e}")
                return None
        
        return df

    def get_stock_data_padded(self, stock_code):
        """
        Fetch stock data and pad it to the full business day range (2022-2026).
        Aligns data for Backtrader multi-feed synchronization.
        """
        df = self.get_stock_data(stock_code)
        if df is None:
            return None
            
        # Create Business Day range index for padding (Skip weekends)
        full_idx = pd.bdate_range(start=self.start_date, end=self.end_date)
        
        # Keep only OHLCV columns
        needed_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in needed_cols:
            if col not in df.columns:
                df[col] = 0.0
        df = df[needed_cols]
        
        # Reindex to full business day range
        df = df.reindex(full_idx)
        
        # Forward fill for suspensions/holidays (keep last price)
        df.ffill(inplace=True)
        
        # Fill remaining NaNs (pre-listing) with 0.0
        df.fillna(0.0, inplace=True)
        
        df.index.name = 'datetime'
        return df

    def prepare_resonance_signals(self, window=5, min_cubes=2):
        """
        Pre-calculate Resonance Signals.
        Output: Dictionary {date: [{'stock_code': '...', 'action': 'BUY/SELL'}]}
        """
        print("Preparing Resonance Signals...")
        signals_dict = collections.defaultdict(list)
        
        # Group buys by stock
        buys_by_stock = collections.defaultdict(list) # stock -> list of (date, cube)
        
        for s in self.signals:
            if s['action'] == 'BUY':
                buys_by_stock[s['stock_code']].append((s['date'], s['cube_symbol']))
                
        # Analyze resonance
        resonance_buys = set() # (date, stock)
        
        for stock, buys in buys_by_stock.items():
            # Sort by date
            buys.sort(key=lambda x: x[0])
            
            # Sliding window check
            for i in range(len(buys)):
                current_date = buys[i][0]
                cubes_in_window = set()
                cubes_in_window.add(buys[i][1])
                
                # Look back
                for j in range(i-1, -1, -1):
                    prev_date = buys[j][0]
                    if (current_date - prev_date).days > window:
                        break
                    cubes_in_window.add(buys[j][1])
                    
                if len(cubes_in_window) >= min_cubes:
                    # Trigger BUY on current_date
                    if (current_date, stock) not in resonance_buys:
                        signals_dict[current_date].append({
                            'stock_code': stock,
                            'action': 'BUY',
                            'reason': f'Resonance: {len(cubes_in_window)} cubes',
                            'cubes': list(cubes_in_window)
                        })
                        resonance_buys.add((current_date, stock))
                        
        # Also handle SELLs (Consensus Sell? Or just individual sell?)
        # For simplicity, if ANY cube in the resonance group sells, we might consider selling?
        # Or just use Stop Loss. Let's add explicit SELL signals from ANY valuable cube as a potential exit signal.
        # But that might be too noisy.
        # Let's rely on Strategy Stop Loss/Take Profit mostly.
        # But we can add "Consensus Sell" logic later.
        
        print(f"Generated {sum(len(v) for v in signals_dict.values())} Resonance BUY signals.")
        return signals_dict

    def prepare_guru_signals(self, guru_symbol):
        """Prepare signals for a single Guru"""
        print(f"Preparing Signals for Guru {guru_symbol}...")
        signals_dict = collections.defaultdict(list)
        
        for s in self.signals:
            if s['cube_symbol'] == guru_symbol:
                signals_dict[s['date']].append({
                    'stock_code': s['stock_code'],
                    'action': s['action'],
                    'cube_symbol': s['cube_symbol']
                })
        
        return signals_dict

    def run_strategy(self, strategy_class, signals_dict, name="Strategy", **kwargs):
        print(f"\n--- Running Backtest: {name} ---")
        
        # Reset Cerebro
        self.cerebro = bt.Cerebro()
        self.cerebro.broker.setcash(100000.0)
        self.cerebro.broker.setcommission(commission=0.001) # 0.1% comm
        
        # Add Data Feeds
        # Only add stocks that are in the signals to save time/memory
        involved_stocks = set()
        for dt, sigs in signals_dict.items():
            for s in sigs:
                involved_stocks.add(s['stock_code'])
        
        print(f"Loading data for {len(involved_stocks)} stocks...")
        count = 0
        for stock_code in involved_stocks:
            df = self.get_stock_data(stock_code)
            if df is not None and not df.empty:
                data = bt.feeds.PandasData(dataname=df, name=stock_code, fromdate=datetime.datetime(2023,1,1), plot=False)
                self.cerebro.adddata(data)
                count += 1
        print(f"Loaded {count} data feeds.")
        
        # Add Strategy
        self.cerebro.addstrategy(strategy_class, **kwargs)
        
        # Inject Signals
        self.cerebro.signals_dict = signals_dict
        
        # Run
        print("Starting Portfolio Value: %.2f" % self.cerebro.broker.getvalue())
        results = self.cerebro.run()
        print("Final Portfolio Value: %.2f" % self.cerebro.broker.getvalue())
        
        # Plot (optional)
        # self.cerebro.plot(style='candlestick')
        
        return results[0]

def main():
    engine = HighWinRateEngine()
    engine.load_signals()
    
    # 1. Run Strategy D: Resonance
    # Buy when >= 2 Cubes buy within 5 days
    resonance_signals = engine.prepare_resonance_signals(window=5, min_cubes=2)
    engine.run_strategy(StrategyD_Resonance, resonance_signals, name="Strategy D (Resonance)")
    
    # Save recent resonance signals
    recent_signals = []
    sorted_dates = sorted(resonance_signals.keys())
    for dt in sorted_dates:
        if dt >= datetime.date(2026, 1, 1):
            for sig in resonance_signals[dt]:
                recent_signals.append({
                    'date': dt,
                    'stock_code': sig['stock_code'],
                    'action': sig['action'],
                    'reason': sig.get('reason', '')
                })
    
    os.makedirs('analysis', exist_ok=True)
    if recent_signals:
        pd.DataFrame(recent_signals).to_csv('analysis/high_win_rate_opportunities.csv', index=False)
        print("\nSaved recent opportunities to analysis/high_win_rate_opportunities.csv")
    
    # 2. Run Strategy E: Top Guru (ZH583267 南极风暴)
    guru_symbol = "ZH583267" # 南极风暴
    guru_signals = engine.prepare_guru_signals(guru_symbol)
    engine.run_strategy(StrategyE_TopGuru, guru_signals, name=f"Strategy E (Top Guru: {guru_symbol})", guru_symbol=guru_symbol)


if __name__ == "__main__":
    main()
