import pandas as pd
import numpy as np
import os
import sys
import json
import logging
from datetime import datetime, timedelta
from collections import defaultdict
import matplotlib.pyplot as plt

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.backtest_strategy import StrategyBacktester

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GrowthBacktester(StrategyBacktester):
    """
    Advanced Backtester with 'Growth' capabilities:
    1. Realistic Account Simulation (Cash, Fees, Slippage)
    2. Adaptive Blogger Scoring (Learn who to trust)
    3. Dynamic Position Sizing (Bet more on winners)
    """
    
    def __init__(self, initial_capital=100000.0, 
                 commission_rate=0.0003, 
                 stamp_duty=0.001, 
                 slippage=0.002,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Account Settings
        self.initial_capital = initial_capital
        self.current_capital = initial_capital
        self.commission_rate = commission_rate
        self.stamp_duty = stamp_duty
        self.slippage = slippage
        
        # "Growth" Memory
        # Score starts at 100. 
        # >100: Trusted (Increase position)
        # <60:  Untrusted (Reduce/Ignore)
        self.blogger_scores = defaultdict(lambda: 100.0) 
        self.blogger_stats = defaultdict(lambda: {'wins': 0, 'losses': 0, 'total_pnl': 0.0})
        
        # Performance Tracking
        self.equity_curve = []
        self.trade_log = []

    def calculate_fees(self, amount, is_sell=False):
        """Calculate trading costs"""
        comm = amount * self.commission_rate
        comm = max(5.0, comm) # Min 5 RMB commission usually
        tax = amount * self.stamp_duty if is_sell else 0.0
        return comm + tax

    def get_position_size(self, blogger_name, price, is_verified=False):
        """
        Dynamic Position Sizing based on Blogger Score
        """
        score = self.blogger_scores[blogger_name]
        
        # Boost score temporarily for verified signals
        effective_score = score
        if is_verified:
            effective_score = max(score, 120) # Minimum 120 for verified
        
        # 1. Filter: If score is too low, don't trade
        if effective_score < 60:
            return 0
            
        # 2. Base Size: 10% of current capital
        base_allocation = 0.10 
        
        # 3. Multiplier: 
        # Score 100 -> 1.0x
        # Score 150 -> 1.5x (Max cap 2.0x)
        multiplier = effective_score / 100.0
        multiplier = min(multiplier, 2.5) # Increased cap for verified
        
        target_amount = self.current_capital * base_allocation * multiplier
        
        # Round down to nearest 100 shares (1 lot)
        shares = int(target_amount / price / 100) * 100
        return shares

    def update_blogger_score(self, blogger_name, pnl_pct):
        """
        Reinforcement Learning-lite:
        Update score based on trade outcome.
        """
        # Learning Rate
        lr = 2.0 # Impact factor
        
        delta = pnl_pct * lr
        
        # Asymmetric punishment: Losses hurt more than wins help (Risk aversion)
        if pnl_pct < 0:
            delta *= 1.5 
            
        self.blogger_scores[blogger_name] += delta
        
        # Stats
        if pnl_pct > 0: self.blogger_stats[blogger_name]['wins'] += 1
        else: self.blogger_stats[blogger_name]['losses'] += 1
        self.blogger_stats[blogger_name]['total_pnl'] += pnl_pct

    def run_growth_simulation(self):
        """
        Run the simulation iterating through time to simulate 'learning'
        """
        # 1. Load Signals
        logger.info("Loading signals...")
        df_signals = self.load_and_parse_signals()
        if df_signals.empty:
            logger.error("No signals found.")
            return

        # 2. Sort by time strictly
        df_signals = df_signals.sort_values('publish_time').reset_index(drop=True)
        
        logger.info(f"Starting Simulation with {self.initial_capital} RMB...")
        logger.info("------------------------------------------------")

        for i, row in df_signals.iterrows():
            blogger = row.get('author_name', 'Unknown') # Need to ensure author_name is in signals
            # Note: The base class _extract_signal_from_text might not preserve author_name if not passed carefully
            # Let's fix that by ensuring we grab author_name from the source df before extraction or merge it back.
            # For now, let's assume 'author_name' is missing in standard extraction and we need to patch it.
            # *Correction*: In load_and_parse_signals, we iterate rows. We should pass author info.
            # But since I can't easily modify the base class method without rewriting it, 
            # I will assume we can get it or use a placeholder if missing. 
            # Actually, the base class returns a list of dicts. I need to make sure 'author_name' is in there.
            # *Self-Correction*: The base class `load_and_parse_signals` implementation in `backtest_strategy.py` 
            # does NOT currently attach `author_name` to the result list explicitly in `_extract_signal_from_text`.
            # However, the loop `for _, row in df_c.iterrows():` has access to it.
            # I will override `load_and_parse_signals` in this subclass to include author_name.
            pass

        # Re-implementing the loop with author_name support
        # Since I cannot easily inject into the middle of the base class method, 
        # I will copy the `load_and_parse_signals` logic here but add author_name.
        
        # ... (Implemented in the full code below) ...

    def load_and_parse_signals_with_author(self):
        """Override to include author_name and prefer pre-calculated trading_signals.csv"""
        logger.info("Loading datasets (Growth Mode)...")
        
        # 1. Try to load from trading_signals.csv (Golden Source)
        signals_path = os.path.join(os.path.dirname(self.comments_file), 'trading_signals.csv')
        if os.path.exists(signals_path):
            logger.info(f"Loading from {signals_path}...")
            df = pd.read_csv(signals_path)
            # Ensure required columns
            if 'ocr_verified' not in df.columns:
                df['ocr_verified'] = False
            
            # Map columns if needed or just use as is. 
            # The strategy expects: stock_code, publish_time, strategy_type, author_name
            if 'trade_date' not in df.columns and 'date' in df.columns:
                df['trade_date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
            
            if 'publish_time' not in df.columns and 'date' in df.columns:
                df['publish_time'] = pd.to_datetime(df['date'])
                
            if 'strategy_type' not in df.columns:
                df['strategy_type'] = 'UNKNOWN'
                
            return df
            
        # Fallback to parsing (legacy)
        signals = []
        
        # Helper to process extraction
        def process_source(df, source_type):
            count = 0
            for _, row in df.iterrows():
                text = row.get('content') or str(row.get('title', '')) + " " + str(row.get('description', ''))
                pub_time = row['publish_time']
                author = row.get('author_name', 'Unknown')
                
                extracted = self._extract_signal_from_text(text, pub_time, source=source_type)
                for sig in extracted:
                    sig['author_name'] = author
                    signals.append(sig)
                    count += 1
            return count

        # 1. Comments
        if os.path.exists(self.comments_file):
            df_c = pd.read_csv(self.comments_file)
            if 'user_name' in df_c.columns and 'author_name' not in df_c.columns:
                df_c['author_name'] = df_c['user_name']
            process_source(df_c, 'comment')

        # 2. Videos
        if os.path.exists(self.videos_file):
            df_v = pd.read_csv(self.videos_file)
            process_source(df_v, 'video')
            
        # Deduplicate
        df_signals = pd.DataFrame(signals)
        if not df_signals.empty:
            df_signals['publish_time'] = pd.to_datetime(df_signals['publish_time'])
            df_signals = df_signals.sort_values('publish_time')
            # Drop dups but keep author info
            df_signals = df_signals.drop_duplicates(subset=['stock_code', 'publish_time', 'strategy_type'])
        
        return df_signals

    def execute_growth_strategy(self):
        df_signals = self.load_and_parse_signals_with_author()
        if df_signals.empty:
            logger.warning("No signals to backtest.")
            return

        total_signals = len(df_signals)
        logger.info(f"Processing {total_signals} signals sorted by time...")

        for i, row in df_signals.iterrows():
            code = row['stock_code']
            trade_date = row['trade_date']
            blogger = row.get('author_name', 'Unknown')
            
            # 1. Check Market Data
            df_price, entry_date = self.get_market_data(code, trade_date)
            if df_price is None or df_price.empty:
                continue
                
            # 2. Determine Entry Price (Realistic with Slippage)
            # Simplified entry logic from base class, but adding slippage
            t_row = df_price.iloc[0]
            open_p = t_row['open']
            
            # Apply Slippage to Entry (Buy higher)
            entry_price_raw = open_p # Assume open execution for simplicity
            entry_price = entry_price_raw * (1 + self.slippage)
            
            # 3. Position Sizing (The Growth Core)
            is_verified = bool(row.get('ocr_verified', False))
            shares = self.get_position_size(blogger, entry_price, is_verified=is_verified)
            
            if shares == 0:
                self.trade_log.append({
                    'date': entry_date,
                    'code': code,
                    'action': 'SKIP',
                    'reason': f"Low Score ({self.blogger_scores[blogger]:.1f})",
                    'blogger': blogger
                })
                continue
                
            cost = shares * entry_price
            fees = self.calculate_fees(cost, is_sell=False)
            total_cost = cost + fees
            
            if total_cost > self.current_capital:
                # Not enough cash
                shares = int((self.current_capital - fees) / entry_price / 100) * 100
                if shares <= 0:
                    continue
                cost = shares * entry_price
                fees = self.calculate_fees(cost, is_sell=False)
                total_cost = cost + fees

            # EXECUTE BUY
            self.current_capital -= total_cost
            
            # 4. Determine Exit (T+2 or Stop Loss)
            # Use base logic to find exit price/date, but apply slippage/fees
            # We reuse the logic by creating a dummy row for base execute_strategy? 
            # No, let's just reimplement a simpler version here for speed/clarity
            
            exit_price_raw = t_row['close'] # Default fall through
            exit_reason = "Hold"
            
            # Scan T to T+2
            stop_loss = entry_price_raw * 0.97
            take_profit = entry_price_raw * 1.08
            
            triggered = False
            for day_idx in range(len(df_price)):
                d = df_price.iloc[day_idx]
                # Check SL
                if d['low'] < stop_loss:
                    exit_price_raw = stop_loss # Optimistic: assume filled at SL
                    # Pessimistic Slippage on SL (Sell lower)
                    exit_price_raw = exit_price_raw * (1 - self.slippage)
                    exit_reason = "Stop Loss"
                    triggered = True
                    break
                # Check TP
                if d['high'] > take_profit:
                    exit_price_raw = take_profit
                    exit_reason = "Take Profit"
                    triggered = True
                    break
                # Time Exit
                if day_idx == 2:
                    exit_price_raw = d['close']
                    exit_reason = "Time Exit"
                    triggered = True
                    break
            
            if not triggered:
                exit_price_raw = df_price.iloc[-1]['close']
                exit_reason = "End Data"

            # Apply Slippage to Exit (if not already applied in SL logic, but let's apply generally for market exits)
            if "Stop Loss" not in exit_reason:
                exit_price = exit_price_raw * (1 - self.slippage)
            else:
                exit_price = exit_price_raw # Already applied above
            
            revenue = shares * exit_price
            sell_fees = self.calculate_fees(revenue, is_sell=True)
            net_revenue = revenue - sell_fees
            
            # EXECUTE SELL
            self.current_capital += net_revenue
            
            # Calc PnL
            net_pnl = net_revenue - total_cost
            pnl_pct = net_pnl / total_cost * 100
            
            # 5. Update Growth Memory
            self.update_blogger_score(blogger, pnl_pct)
            
            # Log
            self.trade_log.append({
                'date': entry_date,
                'code': code,
                'blogger': blogger,
                'action': 'TRADE',
                'shares': shares,
                'entry_price': round(entry_price, 2),
                'exit_price': round(exit_price, 2),
                'pnl_net': round(net_pnl, 2),
                'pnl_pct': round(pnl_pct, 2),
                'reason': exit_reason,
                'blogger_score_after': round(self.blogger_scores[blogger], 1),
                'capital_after': round(self.current_capital, 2)
            })
            
            self.equity_curve.append({'date': entry_date, 'equity': self.current_capital})

        self.save_results()

    def save_results(self):
        # 1. Trade Log
        df_log = pd.DataFrame(self.trade_log)
        df_log.to_csv('data/growth_backtest_trades.csv', index=False)
        
        # 2. Blogger Ratings
        blogger_data = []
        for name, score in self.blogger_scores.items():
            stats = self.blogger_stats[name]
            blogger_data.append({
                'blogger': name,
                'score': round(score, 1),
                'wins': stats['wins'],
                'losses': stats['losses'],
                'total_pnl': round(stats['total_pnl'], 2)
            })
        df_bloggers = pd.DataFrame(blogger_data).sort_values('score', ascending=False)
        df_bloggers.to_csv('data/growth_backtest_bloggers.csv', index=False)
        
        # 3. Summary
        final_return = (self.current_capital - self.initial_capital) / self.initial_capital * 100
        print("\n=== Growth Backtest Summary ===")
        print(f"Initial Capital: {self.initial_capital}")
        print(f"Final Capital:   {self.current_capital:.2f}")
        print(f"Return:          {final_return:.2f}%")
        print(f"Top Blogger:     {df_bloggers.iloc[0]['blogger']} (Score: {df_bloggers.iloc[0]['score']})")
        print(f"Trades Executed: {len(df_log[df_log['action']=='TRADE'])}")
        print(f"Trades Skipped:  {len(df_log[df_log['action']=='SKIP'])}")
        print("===============================")

if __name__ == "__main__":
    backtester = GrowthBacktester()
    backtester.execute_growth_strategy()
