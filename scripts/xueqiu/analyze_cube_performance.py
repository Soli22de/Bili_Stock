import backtrader as bt
import pandas as pd
import os
import json
import logging
from datetime import datetime, date

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class SingleCubeStrategy(bt.Strategy):
    params = (
        ('cube_symbol', ''),
    )

    def __init__(self):
        self.cube_symbol = self.params.cube_symbol
        
    def log(self, txt, dt=None):
        pass

    def next(self):
        current_date = self.datas[0].datetime.date(0)
        
        # Process signals for this specific cube
        if current_date in self.cerebro.signals_dict:
            todays_signals = self.cerebro.signals_dict[current_date]
            
            for signal in todays_signals:
                if signal['source'] != self.cube_symbol:
                    continue
                    
                stock_code = signal['stock_code']
                action = signal['action']
                data = self.getdatabyname(stock_code)
                if not data: continue
                
                position = self.getposition(data).size
                
                # Simple logic: Buy = 10% position, Sell = Close
                # This is a simplification to test "signal quality"
                if action == 'BUY':
                    if position == 0:
                        self.order_target_percent(data=data, target=0.1) 
                elif action == 'SELL':
                    if position > 0:
                        self.close(data=data)

def run_analysis():
    # 1. Load Cubes
    with open("data/long_history_cubes.json", "r", encoding="utf-8") as f:
        cubes = json.load(f)
    
    results = []
    
    # Pre-load all signals to avoid reading files multiple times
    all_signals_cache = {} # {cube_symbol: [signals]}
    
    logging.info("Loading all signals...")
    for cube in cubes:
        symbol = cube["symbol"]
        path = f"data/history/{symbol}.json"
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                all_signals_cache[symbol] = json.load(f)
                
    # 2. Run Backtest for each cube
    for i, cube in enumerate(cubes):
        symbol = cube["symbol"]
        name = cube["name"]
        
        if symbol not in all_signals_cache:
            continue
            
        signals = all_signals_cache[symbol]
        if not signals: continue
        
        # Prepare signals dict for Cerebro
        signals_dict = {}
        involved_stocks = set()
        
        has_recent_data = False
        
        for s in signals:
            dt_str = s["time"][:10]
            dt = datetime.strptime(dt_str, "%Y-%m-%d").date()
            
            if dt < date(2022, 1, 1): continue
            has_recent_data = True
            
            if dt not in signals_dict: signals_dict[dt] = []
            
            signals_dict[dt].append({
                "stock_code": s["stock_code"],
                "action": s["action"],
                "source": symbol
            })
            involved_stocks.add(s["stock_code"])
            
        if not has_recent_data:
            continue
            
        # Run Cerebro
        cerebro = bt.Cerebro()
        cerebro.addstrategy(SingleCubeStrategy, cube_symbol=symbol)
        cerebro.broker.setcash(1000000)
        cerebro.broker.setcommission(commission=0.0003)
        cerebro.signals_dict = signals_dict
        
        # Add Data
        data_added = 0
        for stock_code in involved_stocks:
            file_path = f"data/stock_data/{stock_code}.csv"
            if os.path.exists(file_path):
                try:
                    df = pd.read_csv(file_path)
                    df['date'] = pd.to_datetime(df['日期'])
                    df.set_index('date', inplace=True)
                    if df.empty: continue
                    
                    data = bt.feeds.PandasData(
                        dataname=df,
                        open='开盘', high='最高', low='最低', close='收盘', volume='成交量',
                        fromdate=datetime(2022, 1, 1),
                        todate=datetime(2026, 2, 17)
                    )
                    cerebro.adddata(data, name=stock_code)
                    data_added += 1
                except: pass
        
        if data_added == 0: continue
        
        # Analyzers
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', riskfreerate=0.0)
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')

        try:
            strat = cerebro.run()[0]
            
            end_value = cerebro.broker.getvalue()
            ret = (end_value - 1000000) / 1000000 * 100
            
            sharpe = strat.analyzers.sharpe.get_analysis().get('sharperatio', 0)
            if sharpe is None: sharpe = 0
            
            max_dd = strat.analyzers.drawdown.get_analysis().get('max', {}).get('drawdown', 0)
            
            trades = strat.analyzers.trades.get_analysis()
            total_trades = trades.get('total', {}).get('closed', 0)
            win_rate = (trades.get('won', {}).get('total', 0) / total_trades * 100) if total_trades > 0 else 0
            
            results.append({
                "symbol": symbol,
                "name": name,
                "return": ret,
                "sharpe": sharpe,
                "max_dd": max_dd,
                "trades": total_trades,
                "win_rate": win_rate
            })
            
            logging.info(f"[{i+1}/{len(cubes)}] {name}: {ret:.2f}% Ret, {sharpe:.2f} Sharpe")
            
        except Exception as e:
            logging.error(f"Error running {name}: {e}")

    # Save results
    df_res = pd.DataFrame(results)
    df_res.sort_values("sharpe", ascending=False, inplace=True)
    df_res.to_csv("data/cube_performance_ranking.csv", index=False)
    
    print("\nTop 10 Cubes (by Sharpe):")
    print(df_res.head(10).to_string())

if __name__ == "__main__":
    run_analysis()
