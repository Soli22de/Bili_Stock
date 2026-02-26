import backtrader as bt
import pandas as pd
import os
import sys
import json
import logging
from datetime import datetime, date

# Ensure parent directory is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.xueqiu.full_backtest_engine import FullBacktestEngine
from scripts.xueqiu.dual_strategy_backtest import StrategyB_Aggressive, StrategyC_Sector

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class LongHistoryBacktest(FullBacktestEngine):
    def __init__(self):
        super().__init__()
        self.signals_dict = {} # {date: [signal_list]}
        self.target_cubes = ["ZH2875926", "ZH583267", "ZH1745648", "ZH2888835", "ZH197295"]

    def load_signals(self):
        """Load signals from the 5 long-history cubes"""
        logging.info("Loading signals from long-history cubes...")
        
        all_signals = []
        
        for cube in self.target_cubes:
            path = f"data/history/{cube}.json"
            if not os.path.exists(path):
                logging.warning(f"Missing history for {cube}")
                continue
                
            with open(path, "r", encoding="utf-8") as f:
                signals = json.load(f)
                all_signals.extend(signals)
        
        # Process signals
        # Format: {"time": "2022-01-04 10:00:00", "stock_code": "...", "action": "BUY/SELL", ...}
        
        count = 0
        for s in all_signals:
            # Parse date
            dt_str = s["time"][:10] # YYYY-MM-DD
            dt = datetime.strptime(dt_str, "%Y-%m-%d").date()
            
            # Filter by date range (2022-2024 focus, but include up to now)
            if dt < date(2022, 1, 1):
                continue
                
            stock_code = s["stock_code"]
            action = s["action"]
            
            # Skip ETFs (usually 51xxxx or 15xxxx) - simplistic check, but akshare might have failed for them anyway
            # But let's keep them if we have data. The backtest loop checks if data exists.
            
            if dt not in self.signals_dict:
                self.signals_dict[dt] = []
            
            # Avoid duplicate signals for same stock on same day (take the last one or all? Rotation strategy usually picks one)
            # Let's keep all, the strategy decides.
            self.signals_dict[dt].append({
                "stock_code": stock_code,
                "action": action,
                "source": s["cube_symbol"]
            })
            count += 1
            
        logging.info(f"Loaded {count} signals from {len(self.target_cubes)} cubes.")

    def run_strategy(self, strategy_class, strategy_name, cash):
        logging.info(f"Running {strategy_name} with {cash} cash...")
        
        cerebro = bt.Cerebro()
        cerebro.addstrategy(strategy_class)
        cerebro.broker.setcash(cash)
        cerebro.broker.setcommission(commission=0.0003) # Low commission
        
        # Inject signals_dict into cerebro so strategy can access it
        cerebro.signals_dict = self.signals_dict
        
        # Add Data Feeds
        # We need to add ALL stocks that appear in signals
        # This might be heavy.
        
        # 1. Identify all stocks involved
        involved_stocks = set()
        for dt in self.signals_dict:
            for s in self.signals_dict[dt]:
                involved_stocks.add(s["stock_code"])
        
        logging.info(f"Adding data feeds for {len(involved_stocks)} stocks...")
        
        added_count = 0
        for stock_code in involved_stocks:
            # Check if file exists
            file_path = f"data/stock_data/{stock_code}.csv"
            if not os.path.exists(file_path):
                # Try with SH/SZ prefix adjustment if needed, but fetch script handled it
                # fetch script saved as SH600000.csv
                continue
                
            try:
                # Load data
                df = pd.read_csv(file_path)
                df['date'] = pd.to_datetime(df['日期'])
                df.set_index('date', inplace=True)
                
                data = bt.feeds.PandasData(
                    dataname=df,
                    open='开盘',
                    high='最高',
                    low='最低',
                    close='收盘',
                    volume='成交量',
                    fromdate=datetime(2022, 1, 1),
                    todate=datetime(2026, 2, 17)
                )
                cerebro.adddata(data, name=stock_code)
                added_count += 1
            except Exception as e:
                # logging.warning(f"Failed to load data for {stock_code}: {e}")
                pass
                
        logging.info(f"Added {added_count} data feeds.")
        
        # Run
        logging.info("Starting Backtest...")
        start_value = cerebro.broker.getvalue()
        results = cerebro.run()
        end_value = cerebro.broker.getvalue()
        
        # Analysis
        strat = results[0]
        total_return = (end_value - start_value) / start_value * 100
        
        # Calculate Max Drawdown & Sharpe manually or via analyzers if added
        # Here we use the recorded values in strategy
        values = pd.Series(strat.total_value)
        if not values.empty:
            # Drawdown
            rolling_max = values.cummax()
            drawdown = (values - rolling_max) / rolling_max
            max_drawdown = drawdown.min() * 100
            
            # Sharpe (Daily)
            returns = values.pct_change().dropna()
            if returns.std() != 0:
                sharpe = (returns.mean() / returns.std()) * (252 ** 0.5)
            else:
                sharpe = 0
        else:
            max_drawdown = 0
            sharpe = 0
            
        logging.info(f"{strategy_name} Result: {total_return:.2f}% Return, MaxDD: {max_drawdown:.2f}%, Sharpe: {sharpe:.2f}")
        
        return {
            "strategy": strategy_name,
            "start_value": start_value,
            "end_value": end_value,
            "return": total_return,
            "max_drawdown": max_drawdown,
            "sharpe": sharpe
        }

    def run(self):
        self.load_signals()
        
        results = []
        
        # Strategy B
        res_b = self.run_strategy(StrategyB_Aggressive, "Strategy B (Small Cap)", 100000)
        results.append(res_b)
        
        # Strategy C
        res_c = self.run_strategy(StrategyC_Sector, "Strategy C (Large Cap)", 10000000)
        results.append(res_c)
        
        # Save Report
        with open("data/long_history_backtest_report.txt", "w", encoding="utf-8") as f:
            f.write("Long History Backtest Report (2022-2024 Focus)\n")
            f.write("==============================================\n")
            f.write(f"Data Source: 5 Long-History Xueqiu Cubes ({', '.join(self.target_cubes)})\n")
            f.write(f"Time Range: 2022-01-01 to 2026-02-17\n\n")
            
            for res in results:
                f.write(f"Strategy: {res['strategy']}\n")
                f.write(f"Initial Capital: {res['start_value']:,.2f}\n")
                f.write(f"Final Capital:   {res['end_value']:,.2f}\n")
                f.write(f"Total Return:    {res['return']:.2f}%\n")
                f.write(f"Max Drawdown:    {res['max_drawdown']:.2f}%\n")
                f.write(f"Sharpe Ratio:    {res['sharpe']:.2f}\n")
                f.write("-" * 30 + "\n")
        
        logging.info("Report saved to data/long_history_backtest_report.txt")

if __name__ == "__main__":
    engine = LongHistoryBacktest()
    engine.run()
