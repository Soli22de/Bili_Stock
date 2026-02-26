
import sys
import os
import datetime
import pandas as pd
import backtrader as bt

# Ensure we can import from the scripts directory
sys.path.append(os.getcwd())

from scripts.xueqiu.high_win_rate_backtest import HighWinRateEngine, StrategyD_Resonance

class StrategyD_WithLogging(StrategyD_Resonance):
    def __init__(self):
        # Initialize parent
        super(StrategyD_WithLogging, self).__init__()
        self.trade_history = []
        self.last_exit_prices = {}

    def notify_order(self, order):
        # Capture exit price from Sell orders
        if order.status in [order.Completed]:
            if order.issell():
                self.last_exit_prices[order.data._name] = order.executed.price
        
        # Call parent notify_order (important for logging)
        super(StrategyD_WithLogging, self).notify_order(order)

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        # Calculate duration
        duration = (trade.dtclose - trade.dtopen)
        
        # Get the stock name/code
        stock_name = trade.data._name
        
        # Get exit price from our tracker
        exit_price = self.last_exit_prices.get(stock_name, 0.0)

        trade_record = {
            'Stock': stock_name,
            'Entry Date': bt.num2date(trade.dtopen).date(),
            'Entry Price': round(trade.price, 2),
            'Exit Date': bt.num2date(trade.dtclose).date(),
            'Exit Price': round(exit_price, 2),
            'PnL': round(trade.pnl, 2),
            'Return %': 0.0, # Calculated below
            'Duration (Days)': duration,
            'Status': 'WIN' if trade.pnl > 0 else 'LOSS'
        }
        
        # Calculate Return % accurately using Entry Price and Exit Price
        if trade_record['Entry Price'] > 0:
            trade_record['Return %'] = round(((exit_price - trade_record['Entry Price']) / trade_record['Entry Price']) * 100, 2)
            
        self.trade_history.append(trade_record)

def main():
    print("Running Strategy D (Resonance) to extract trade history...")
    engine = HighWinRateEngine()
    engine.load_signals()
    
    # Generate signals
    resonance_signals = engine.prepare_resonance_signals(window=5, min_cubes=2)
    
    # Setup Cerebro
    cerebro = bt.Cerebro()
    
    # Inject signals
    cerebro.signals_dict = resonance_signals
    
    # Collect all stocks involved
    all_stocks = set()
    for dt, sigs in resonance_signals.items():
        for s in sigs:
            all_stocks.add(s['stock_code'])
    
    print(f"Loading data for {len(all_stocks)} stocks...")
    for stock_code in all_stocks:
        df = engine.get_stock_data(stock_code)
        if df is not None:
            # Create Data Feed
            data = bt.feeds.PandasData(dataname=df, plot=False)
            cerebro.adddata(data, name=stock_code)
            
    # Add strategy
    cerebro.addstrategy(StrategyD_WithLogging)
    
    # Set initial cash
    cerebro.broker.setcash(100000.0)
    cerebro.broker.setcommission(commission=0.001) # 0.1% commission
    
    # Run
    print("Executing Backtest...")
    strategies = cerebro.run()
    first_strategy = strategies[0]
    
    # Extract trades
    trades = first_strategy.trade_history
    df_trades = pd.DataFrame(trades)
    
    if not df_trades.empty:
        # Sort by Entry Date
        df_trades = df_trades.sort_values(by='Entry Date')
        
        # Save to CSV
        output_file = 'analysis/strategy_d_trade_history.csv'
        os.makedirs('analysis', exist_ok=True)
        df_trades.to_csv(output_file, index=False)
        
        # Print summary
        print("\n=== Strategy D Trade History (All Trades) ===")
        # Print all trades for the user to see
        print(df_trades.to_string(index=False))
        
        print("\n=== Performance Metrics ===")
        total_trades = len(df_trades)
        wins = len(df_trades[df_trades['Status'] == 'WIN'])
        win_rate = (wins / total_trades) * 100
        total_pnl = df_trades['PnL'].sum()
        
        print(f"Total Trades: {total_trades}")
        print(f"Win Rate: {win_rate:.1f}%")
        print(f"Total PnL: {total_pnl:.2f}")
        print(f"Saved full history to {output_file}")
    else:
        print("No trades generated.")

if __name__ == "__main__":
    main()
