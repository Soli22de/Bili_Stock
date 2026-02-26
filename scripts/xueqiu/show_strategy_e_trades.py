import backtrader as bt
import pandas as pd
import os
import sys
import datetime

# Add project root to sys.path
sys.path.append(os.getcwd())

from scripts.xueqiu.high_win_rate_backtest import HighWinRateEngine, StrategyE_TopGuru

class StrategyE_WithLogging(StrategyE_TopGuru):
    def __init__(self):
        super(StrategyE_WithLogging, self).__init__()
        self.trade_history = []
        self.last_exit_prices = {}

    def notify_order(self, order):
        if order.status in [order.Completed]:
            if order.issell():
                self.last_exit_prices[order.data._name] = order.executed.price
        super(StrategyE_WithLogging, self).notify_order(order)

    def notify_trade(self, trade):
        if not trade.isclosed:
            return
            
        duration = (trade.dtclose - trade.dtopen)
        stock_name = trade.data._name
        exit_price = self.last_exit_prices.get(stock_name, 0.0)
        
        trade_record = {
            'Stock': stock_name,
            'Entry Date': bt.num2date(trade.dtopen).date(),
            'Entry Price': round(trade.price, 2),
            'Exit Date': bt.num2date(trade.dtclose).date(),
            'Exit Price': round(exit_price, 2),
            'PnL': round(trade.pnl, 2),
            'Return %': 0.0,
            'Duration (Days)': duration,
            'Status': 'WIN' if trade.pnl > 0 else 'LOSS'
        }
        
        if trade_record['Entry Price'] > 0:
            trade_record['Return %'] = round(((exit_price - trade_record['Entry Price']) / trade_record['Entry Price']) * 100, 2)
            
        self.trade_history.append(trade_record)

def main():
    guru_symbol = 'ZH583267' # South Pole Storm
    print(f"Running Strategy E (Top Guru: {guru_symbol}) to extract trade history...")
    
    # Clear cache to ensure fresh data download for 2022-2026 range
    import shutil
    cache_dir = "data/cache"
    if os.path.exists(cache_dir):
        print(f"Clearing cache directory: {cache_dir}...")
        shutil.rmtree(cache_dir)
    os.makedirs(cache_dir, exist_ok=True)
    
    engine = HighWinRateEngine() # Uses 2022-2026 range now
    engine.load_signals()
    
    # Prepare signals dict for the guru
    guru_signals_dict = engine.prepare_guru_signals(guru_symbol)
    
    cerebro = bt.Cerebro()
    
    # Inject signals dict into cerebro (as expected by StrategyE_TopGuru)
    cerebro.signals_dict = guru_signals_dict
    
    # Add data feeds for stocks traded by this guru
    # Extract stocks from the dict
    guru_stocks = set()
    for dt, sigs in guru_signals_dict.items():
        for s in sigs:
            guru_stocks.add(s['stock_code'])
            
    print(f"Loading data for {len(guru_stocks)} stocks traded by {guru_symbol}...")
    
    for stock_code in guru_stocks:
        # Use the padded data method from HighWinRateEngine
        df = engine.get_stock_data_padded(stock_code)
        
        if df is not None:
            data = bt.feeds.PandasData(dataname=df, plot=False)
            cerebro.adddata(data, name=stock_code)
            
    cerebro.addstrategy(StrategyE_WithLogging, guru_symbol=guru_symbol)
    
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001)
    
    print("Executing Backtest...")
    strategies = cerebro.run()
    
    first_strategy = strategies[0]
    trades = first_strategy.trade_history
    
    df_trades = pd.DataFrame(trades)
    if not df_trades.empty:
        df_trades = df_trades.sort_values(by='Entry Date')
        output_file = 'analysis/strategy_e_trade_history.csv'
        os.makedirs('analysis', exist_ok=True)
        df_trades.to_csv(output_file, index=False)
        
        print(f"\n=== Strategy E Trade History ({guru_symbol}) ===")
        print(df_trades.to_string(index=False))
        
        print("\n=== Performance Metrics ===")
        total_trades = len(df_trades)
        wins = len(df_trades[df_trades['Status'] == 'WIN'])
        win_rate = (wins / total_trades) * 100 if total_trades > 0 else 0
        total_pnl = df_trades['PnL'].sum()
        
        print(f"Total Trades: {total_trades}")
        print(f"Win Rate: {win_rate:.1f}%")
        print(f"Total PnL: {total_pnl:.2f}")
        print(f"Saved full history to {output_file}")
    else:
        print("No trades generated.")

if __name__ == "__main__":
    main()
