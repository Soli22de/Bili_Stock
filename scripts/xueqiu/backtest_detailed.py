import backtrader as bt
import pandas as pd
import os
import sys
import json
import logging
import random
from datetime import datetime, date

# Ensure parent directory is in path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.xueqiu.full_backtest_engine import FullBacktestEngine
from scripts.xueqiu.dual_strategy_backtest import StrategyB_Aggressive, StrategyC_Sector

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class DetailedBacktest(FullBacktestEngine):
    def __init__(self):
        super().__init__()
        self.signals_dict = {} # {date: [signal_list]}
        self.target_cubes = []

    def load_candidates(self):
        """Load all candidate cubes from JSON"""
        path = "data/long_history_cubes.json"
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                cubes = json.load(f)
                self.target_cubes = [c["symbol"] for c in cubes]
        else:
            # Fallback
            self.target_cubes = ["ZH2875926", "ZH583267", "ZH1745648", "ZH2888835", "ZH197295"]
        
        logging.info(f"Targeting {len(self.target_cubes)} cubes.")

    def load_signals(self):
        self.load_candidates()
        logging.info("Loading signals from cubes...")
        
        all_signals = []
        
        for cube in self.target_cubes:
            path = f"data/history/{cube}.json"
            if not os.path.exists(path):
                # logging.warning(f"Missing history for {cube}")
                continue
                
            with open(path, "r", encoding="utf-8") as f:
                signals = json.load(f)
                all_signals.extend(signals)
        
        count = 0
        for s in all_signals:
            dt_str = s["time"][:10]
            dt = datetime.strptime(dt_str, "%Y-%m-%d").date()
            
            if dt < date(2022, 1, 1):
                continue
                
            stock_code = s["stock_code"]
            action = s["action"]
            
            if dt not in self.signals_dict:
                self.signals_dict[dt] = []
            
            self.signals_dict[dt].append({
                "stock_code": stock_code,
                "action": action,
                "source": s["cube_symbol"]
            })
            count += 1
            
        logging.info(f"Loaded {count} signals.")

    def run_strategy(self, strategy_class, strategy_name, cash, commission=0.0003, **kwargs):
        logging.info(f"Running {strategy_name} with {cash} cash and commission {commission}...")
        
        cerebro = bt.Cerebro()
        cerebro.addstrategy(strategy_class, **kwargs)
        cerebro.broker.setcash(cash)
        cerebro.broker.setcommission(commission=commission)
        
        # Inject signals
        cerebro.signals_dict = self.signals_dict
        
        # Add Data Feeds
        involved_stocks = set()
        for dt in self.signals_dict:
            for s in self.signals_dict[dt]:
                involved_stocks.add(s["stock_code"])
        
        # Also add some random stocks for benchmark if needed, but here we just use involved
        
        logging.info(f"Adding data feeds for {len(involved_stocks)} stocks...")
        
        added_count = 0
        for stock_code in involved_stocks:
            file_path = f"data/stock_data/{stock_code}.csv"
            if not os.path.exists(file_path):
                continue
                
            try:
                df = pd.read_csv(file_path)
                df['date'] = pd.to_datetime(df['日期'])
                df.set_index('date', inplace=True)
                
                if df.empty: continue

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
                pass
                
        logging.info(f"Added {added_count} data feeds.")
        
        # Analyzers
        cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='trades')
        cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
        cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe', timeframe=bt.TimeFrame.Days, compression=1, riskfreerate=0.0)
        
        # Run
        logging.info("Starting Backtest...")
        start_value = cerebro.broker.getvalue()
        results = cerebro.run()
        end_value = cerebro.broker.getvalue()
        
        # Analysis
        strat = results[0]
        total_return = (end_value - start_value) / start_value * 100
        
        # Get Analyzer Results
        trade_analysis = strat.analyzers.trades.get_analysis()
        dd_analysis = strat.analyzers.drawdown.get_analysis()
        sharpe_analysis = strat.analyzers.sharpe.get_analysis()
        
        # Safe extraction
        total_trades = trade_analysis.get('total', {}).get('closed', 0)
        won_trades = trade_analysis.get('won', {}).get('total', 0)
        win_rate = (won_trades / total_trades * 100) if total_trades > 0 else 0
        
        max_drawdown = dd_analysis.get('max', {}).get('drawdown', 0)
        sharpe = sharpe_analysis.get('sharperatio', 0)
        if sharpe is None: sharpe = 0
            
        logging.info(f"{strategy_name} Result: {total_return:.2f}% Return, MaxDD: {max_drawdown:.2f}%, Sharpe: {sharpe:.2f}")
        logging.info(f"Trades: {total_trades}, Win Rate: {win_rate:.1f}%")
        
        return {
            "strategy": strategy_name,
            "start_value": start_value,
            "end_value": end_value,
            "return": total_return,
            "max_drawdown": max_drawdown,
            "sharpe": sharpe,
            "trades": total_trades,
            "win_rate": win_rate
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
        
        # Strategy C with Slippage (Feasibility Test)
        # 0.0003 is normal. Let's try 0.003 (0.3%) to simulate impact + slippage.
        logging.info("Running Feasibility Test (High Slippage)...")
        res_c_slip = self.run_strategy(StrategyC_Sector, "Strategy C (Slippage Test)", 10000000, commission=0.003)
        results.append(res_c_slip)
        
        # Save Report
        with open("data/detailed_backtest_report.txt", "w", encoding="utf-8") as f:
            f.write("Detailed Backtest Report (2022-2024 Focus)\n")
            f.write("==========================================\n")
            f.write(f"Data Source: {len(self.target_cubes)} Xueqiu Cubes\n")
            f.write(f"Time Range: 2022-01-01 to 2026-02-17\n\n")
            
            for res in results:
                f.write(f"Strategy: {res['strategy']}\n")
                f.write(f"Initial Capital: {res['start_value']:,.2f}\n")
                f.write(f"Final Capital:   {res['end_value']:,.2f}\n")
                f.write(f"Total Return:    {res['return']:.2f}%\n")
                f.write(f"Max Drawdown:    {res['max_drawdown']:.2f}%\n")
                f.write(f"Sharpe Ratio:    {res['sharpe']:.2f}\n")
                f.write(f"Total Trades:    {res['trades']}\n")
                f.write(f"Win Rate:        {res['win_rate']:.1f}%\n")
                f.write("-" * 30 + "\n")
        
        logging.info("Report saved to data/detailed_backtest_report.txt")

if __name__ == "__main__":
    engine = DetailedBacktest()
    engine.run()
