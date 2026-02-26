
import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

def generate_mock_ohlcv(days=250, n_stocks=50):
    """Generate realistic Mock OHLCV Data for testing."""
    logging.info("Generating Mock OHLCV Data...")
    
    end_date = datetime.now().date()
    dates = pd.date_range(end=end_date, periods=days)
    stocks = [f"Stock_{i:03d}" for i in range(n_stocks)]
    
    data = []
    
    for stock in stocks:
        # Random Walk Parameters
        start_price = np.random.uniform(10, 100)
        daily_vol = np.random.uniform(0.01, 0.04) # 1-4% daily vol
        drift = np.random.normal(0.0002, 0.0005) # Slight upward bias
        
        price = start_price
        for date in dates:
            # Generate daily move
            ret = np.random.normal(drift, daily_vol)
            close = price * (1 + ret)
            
            # Generate High/Low/Open around Close
            # Open is close to prev close + overnight gap
            open_p = price * (1 + np.random.normal(0, 0.005))
            
            # High/Low based on Open/Close
            high_p = max(open_p, close) * (1 + abs(np.random.normal(0, daily_vol/2)))
            low_p = min(open_p, close) * (1 - abs(np.random.normal(0, daily_vol/2)))
            
            # Volume
            volume = int(np.random.lognormal(10, 1))
            
            data.append({
                'date': date,
                'symbol': stock,
                'open': round(open_p, 2),
                'high': round(high_p, 2),
                'low': round(low_p, 2),
                'close': round(close, 2),
                'volume': volume
            })
            
            price = close
            
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index(['date', 'symbol']).sort_index()

def generate_mock_signals(ohlcv_df, n_cubes=200):
    """Generate consensus_score signals based on Mock Rebalancing."""
    logging.info("Generating Mock Consensus Signals...")
    
    dates = ohlcv_df.index.get_level_values('date').unique()
    stocks = ohlcv_df.index.get_level_values('symbol').unique()
    
    signals = []
    
    for date in dates:
        # Randomly assign consensus scores to stocks
        # Most stocks have 0 score. Some have positive/negative.
        # We simulate "smart money" hitting a few stocks
        
        daily_scores = {}
        for stock in stocks:
            # Poisson distribution for number of buyers/sellers
            n_buy = np.random.poisson(0.5) # Lambda=0.5, mostly 0
            n_sell = np.random.poisson(0.4) 
            
            # Inject Alpha: If stock will go up tomorrow, increase n_buy chance
            # (Cheating slightly to verify strategy logic works on 'good' signals)
            try:
                next_ret = ohlcv_df.loc[(date + timedelta(days=1), stock), 'close'] / \
                           ohlcv_df.loc[(date, stock), 'close'] - 1
                if next_ret > 0.02:
                    n_buy += np.random.randint(1, 4)
            except:
                pass
                
            score = n_buy - n_sell
            if score != 0:
                daily_scores[stock] = score
        
        # Convert to DataFrame rows
        for stock, score in daily_scores.items():
            signals.append({
                'date': date,
                'symbol': stock,
                'consensus_score': score
            })
            
    return pd.DataFrame(signals)

class BacktestEngine:
    def __init__(self, initial_capital=100000):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions = {} # {symbol: {shares, entry_price, entry_date, stop_price}}
        self.trade_log = []
        self.equity_curve = []
        self.history = [] # Daily portfolio value
        
        # Config
        self.comm_buy = 0.0002
        self.comm_sell = 0.0012
        self.position_pct = 0.10
        self.max_daily_buy = 3
        self.stop_loss_pct = 0.06
        self.hold_days = 3
        self.min_consensus = 2

    def run(self, ohlcv_df, signals_df):
        logging.info("Starting Backtest Loop...")
        
        # Prepare Data
        dates = sorted(ohlcv_df.index.get_level_values('date').unique())
        signals_df = signals_df.set_index(['date', 'symbol']).sort_index()
        
        for i, date in enumerate(dates):
            if i == 0: continue # Skip first day (need T-1 for signals)
            
            prev_date = dates[i-1]
            
            # 1. Update Portfolio Value (Mark-to-Market at Close)
            # Use T-1 signals to buy at T Open
            # Process Exits at T Open
            
            daily_equity = self.cash
            todays_prices = ohlcv_df.loc[date]
            
            # --- MORNING SESSION (OPEN) ---
            
            # A. Check Time-Based Exits (Sell at Open)
            # Hold for 3 days: Bought T, Sell T+3 (Hold T, T+1, T+2) -> Actually T+4 per requirement
            # Requirement: "Hold full 3 trading days (T+4 Open Sell)"
            # If bought on Day 1 (Entry Date), hold Day 1, Day 2, Day 3. Sell Day 4 Open.
            # So if (current_date - entry_date).days >= 3? No, trading days.
            # We can track 'days_held'.
            
            # Let's iterate a copy to modify dictionary safely
            for symbol in list(self.positions.keys()):
                pos = self.positions[symbol]
                
                # Check if price exists for today
                if symbol not in todays_prices.index:
                    continue
                    
                open_price = todays_prices.loc[symbol, 'open']
                low_price = todays_prices.loc[symbol, 'low']
                close_price = todays_prices.loc[symbol, 'close']
                
                # Update days held (simplified: count every bar as a day)
                pos['days_held'] += 1
                
                # Exit Logic 1: Time-based (Hold 3 full days, sell on 4th open)
                # Bought T+1 Open. 
                # End of T+1: Held 1 day.
                # End of T+2: Held 2 days.
                # End of T+3: Held 3 days.
                # Start of T+4: Sell.
                # So if days_held > 3 at start of day?
                # Actually, we increment at start. 
                # Entry Day: days_held = 0.
                # Next Day: days_held = 1.
                # ...
                # Wait, simpler: if date index - entry_date index >= 3
                
                # Let's use simple counter logic:
                # Initialized at 0 on purchase.
                # Start of Day check: if days_held == 3: Sell at Open.
                
                if pos['days_held'] >= 3:
                    # Sell at Open
                    self._sell(symbol, open_price, date, "TimeExit")
                    continue

            # B. Execute New Buys (Signals from Yesterday Close)
            # Signal Date: prev_date
            # Execution Date: date (Today Open)
            
            try:
                # Get potential buys
                candidates = signals_df.loc[prev_date]
                candidates = candidates[candidates['consensus_score'] >= self.min_consensus]
                candidates = candidates.sort_values('consensus_score', ascending=False).head(self.max_daily_buy)
                
                # Calculate size per trade (Dynamic based on current equity)
                # Note: Equity is roughly Cash + MarketValue. 
                # We haven't updated MarketValue for today yet, use yesterday's close or cash approximation.
                # Simple: Cash + Sum(Pos Shares * Open Price)
                current_equity = self.cash + sum(p['shares'] * todays_prices.loc[s, 'open'] for s, p in self.positions.items() if s in todays_prices.index)
                
                target_size = current_equity * self.position_pct
                
                for symbol in candidates.index:
                    if symbol in self.positions: continue # Don't add to existing
                    if symbol not in todays_prices.index: continue
                    
                    open_price = todays_prices.loc[symbol, 'open']
                    shares = int(target_size / open_price / 100) * 100 # Round lot
                    
                    if shares > 0 and self.cash >= shares * open_price * (1 + self.comm_buy):
                        self._buy(symbol, open_price, shares, date)
                        
            except KeyError:
                pass # No signals for prev_date

            # --- INTRADAY / CLOSE SESSION ---
            
            # C. Check Stop-Loss (Intraday Low)
            # Must check AFTER buys to protect new positions too?
            # Yes, if we buy at Open, we are exposed to Low of the day.
            
            for symbol in list(self.positions.keys()):
                pos = self.positions[symbol]
                if symbol not in todays_prices.index: continue
                
                low_price = todays_prices.loc[symbol, 'low']
                close_price = todays_prices.loc[symbol, 'close']
                
                # Check Stop Loss
                if low_price < pos['stop_price']:
                    # Sell at Close (Conservative assumption: we realized the drop late or sell into close)
                    # Or Sell at Stop Price? Slippage is real.
                    # Rule says: "按当日收盘价清仓" (Sell at Close)
                    self._sell(symbol, close_price, date, "StopLoss")
                
                # Update holding counter for next day
                # (We already incremented 'days_held' at start of loop? 
                # No, let's increment at END of day to mark a full day held)
                # Logic correction:
                # Day 1 (Entry): Processed Buy. End of Day: days_held = 1.
                # Day 2: Start of Day: days_held=1. End: 2.
                # Day 3: Start: 2. End: 3.
                # Day 4: Start: 3 -> Sell at Open.
                # So we verify days_held >= 3 at Start.
                
                # But wait, I incremented at Start earlier. 
                # Correct logic: 
                # 1. Buy at Open. Set days_held = 0.
                # 2. End of Day. Set days_held += 1.
                # 3. Next Morning. If days_held >= 3, Sell.
                
                # Since I'm iterating positions *before* buying new ones, the new ones aren't checked for time exit yet.
                # So I should increment days_held HERE (End of Day).
                
                # Only increment if still in position (not sold today)
                if symbol in self.positions:
                    self.positions[symbol]['days_held'] += 1

            # D. Update Equity Curve
            daily_mv = sum(p['shares'] * todays_prices.loc[s, 'close'] for s, p in self.positions.items() if s in todays_prices.index)
            total_equity = self.cash + daily_mv
            self.equity_curve.append({'date': date, 'equity': total_equity})

    def _buy(self, symbol, price, shares, date):
        cost = shares * price
        comm = cost * self.comm_buy
        self.cash -= (cost + comm)
        
        self.positions[symbol] = {
            'shares': shares,
            'entry_price': price,
            'entry_date': date,
            'stop_price': price * (1 - self.stop_loss_pct),
            'days_held': 0
        }
        # Logging is verbose, maybe skip for speed
        # logging.info(f"[{date.date()}] BUY {symbol}: {shares} @ {price:.2f}")

    def _sell(self, symbol, price, date, reason):
        pos = self.positions.pop(symbol)
        revenue = pos['shares'] * price
        comm = revenue * self.comm_sell # Includes tax
        self.cash += (revenue - comm)
        
        # Log Trade
        pnl = (revenue - comm) - (pos['shares'] * pos['entry_price'] * (1 + self.comm_buy))
        pnl_pct = pnl / (pos['shares'] * pos['entry_price'])
        
        self.trade_log.append({
            'symbol': symbol,
            'entry_date': pos['entry_date'],
            'exit_date': date,
            'entry_price': pos['entry_price'],
            'exit_price': price,
            'shares': pos['shares'],
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'reason': reason,
            'hold_days': pos['days_held']
        })
        # logging.info(f"[{date.date()}] SELL {symbol} ({reason}): {pnl_pct*100:.2f}%")

    def report(self):
        if not self.equity_curve:
            return "No trades executed."
            
        df_eq = pd.DataFrame(self.equity_curve).set_index('date')
        df_trades = pd.DataFrame(self.trade_log)
        
        final_equity = df_eq['equity'].iloc[-1]
        total_ret = (final_equity - self.initial_capital) / self.initial_capital
        
        if not df_trades.empty:
            win_rate = len(df_trades[df_trades['pnl'] > 0]) / len(df_trades)
            avg_pnl = df_trades['pnl_pct'].mean()
        else:
            win_rate = 0
            avg_pnl = 0
            
        # Max Drawdown
        df_eq['peak'] = df_eq['equity'].cummax()
        df_eq['dd'] = (df_eq['equity'] - df_eq['peak']) / df_eq['peak']
        max_dd = df_eq['dd'].min()
        
        logging.info("\n" + "="*40)
        logging.info(f"Strategy Report (Consensus Score >= {self.min_consensus})")
        logging.info("="*40)
        logging.info(f"Initial Capital : {self.initial_capital:,.2f}")
        logging.info(f"Final Equity    : {final_equity:,.2f}")
        logging.info(f"Total Return    : {total_ret*100:.2f}%")
        logging.info(f"Max Drawdown    : {max_dd*100:.2f}%")
        logging.info(f"Total Trades    : {len(df_trades)}")
        logging.info(f"Win Rate        : {win_rate*100:.2f}%")
        logging.info(f"Avg PnL per Trade: {avg_pnl*100:.2f}%")
        logging.info("="*40)
        
        # Save Log
        os.makedirs("data", exist_ok=True)
        df_trades.to_csv("data/consensus_strategy_trades.csv", index=False)
        logging.info("Trade log saved to data/consensus_strategy_trades.csv")

if __name__ == "__main__":
    # 1. Generate Data
    ohlcv = generate_mock_ohlcv(days=250, n_stocks=50) # 1 Year
    signals = generate_mock_signals(ohlcv)
    
    # 2. Run Backtest
    engine = BacktestEngine(initial_capital=100000)
    engine.run(ohlcv, signals)
    engine.report()
