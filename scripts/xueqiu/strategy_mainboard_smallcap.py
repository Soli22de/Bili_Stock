
import pandas as pd
import numpy as np
import logging
import os
import sqlite3
import baostock as bs
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

class MainboardSignalLoader:
    def __init__(self, db_path="data/cubes.db"):
        self.db_path = db_path

    def load_signals(self):
        """Load signals specifically for Mainboard Small Cap strategy."""
        if not os.path.exists(self.db_path):
            logging.error(f"Database not found: {self.db_path}")
            return pd.DataFrame()

        logging.info("Loading Data for Mainboard Strategy...")
        conn = sqlite3.connect(self.db_path)
        
        # 1. Load Cube Metadata to identify "Steady Hands"
        # Logic: High gain, long history, low turnover (implied by description or just long term)
        # For simplicity, we use the "Steady Hands" tier defined earlier or just big followers + decent gain
        cubes_df = pd.read_sql_query("SELECT symbol, total_gain, followers_count FROM cubes", conn)
        steady_hands = cubes_df[
            (cubes_df['total_gain'] > 20) & 
            (cubes_df['followers_count'] > 5000)
        ]['symbol'].tolist()
        logging.info(f"Identified {len(steady_hands)} 'Steady Hands' institutional-like cubes.")
        
        # 2. Load Rebalancing History
        try:
            df = pd.read_sql_query("SELECT * FROM rebalancing_history", conn)
        except Exception as e:
            logging.error(f"DB Error: {e}")
            conn.close()
            return pd.DataFrame()
        conn.close()
        
        if df.empty: return pd.DataFrame()

        # 3. Preprocess & Hard Filter (Mainboard Only)
        if 'date' not in df.columns and 'created_at' in df.columns:
            df['date'] = df['created_at']
        df['date'] = pd.to_datetime(df['date'], format='mixed', errors='coerce').dt.date
        df = df.dropna(subset=['date'])
        
        # --- HARD FILTER: NO 688/300 ---
        # Regex: Starts with SH60, SH60, SZ00...
        # Easier: Exclude SH688 and SZ300
        logging.info(f"Filtering Mainboard from {len(df)} records...")
        
        # Keep only 6 digits
        df = df[df['stock_symbol'].astype(str).str.match(r'^(SH|SZ)\d{6}$', na=False)]
        
        # Exclude 688 (Star) and 300 (ChiNext) and 8xx/4xx (Beijing)
        # SH688..., SZ300...
        mask_mainboard = ~df['stock_symbol'].astype(str).str.contains(r'^(SH688|SZ300|BJ)')
        df = df[mask_mainboard]
        
        logging.info(f"Records after Mainboard filter: {len(df)}")
        
        # 4. Factor Calculation (Upgraded V2)
        
        # Factor A: Conviction Buy (High Weight Bet)
        # Logic: Hidden Gems adding > 5% weight to a stock in a single day or short window
        # Load Hidden Gems first
        gems = cubes_df[
            (cubes_df['total_gain'] > 30) & 
            (cubes_df['followers_count'] < 500)
        ]['symbol'].tolist()
        
        gems_df = df[df['cube_symbol'].isin(gems)].copy()
        gems_df['weight_delta'] = gems_df['target_weight'] - gems_df['prev_weight_adjusted']
        
        # High Conviction: Single add > 5%
        high_conviction = gems_df[gems_df['weight_delta'] > 5].copy()
        high_conviction['signal_type'] = 'ConvictionBuy'
        high_conviction = high_conviction[['date', 'stock_symbol', 'signal_type']].rename(columns={'stock_symbol': 'symbol'})
        
        # Factor B: 10-Day Accumulation (Institutional Stealth Buy)
        # Logic: Steady Hands net buying > 2% over 10 days
        steady_df = df[df['cube_symbol'].isin(steady_hands)].copy()
        steady_df['weight_delta'] = steady_df['target_weight'] - steady_df['prev_weight_adjusted']
        
        inst_flow = steady_df.groupby(['date', 'stock_symbol'])['weight_delta'].sum().reset_index()
        pivot_flow = inst_flow.pivot(index='date', columns='stock_symbol', values='weight_delta').fillna(0)
        rolling_10d = pivot_flow.rolling(window=10, min_periods=1).sum()
        
        # Stack
        acc_signals = rolling_10d.stack().reset_index()
        acc_signals.columns = ['date', 'symbol', 'net_flow']
        acc_signals = acc_signals[acc_signals['net_flow'] > 2].copy() # >2% net accumulation
        acc_signals['signal_type'] = 'Accumulation'
        acc_signals = acc_signals[['date', 'symbol', 'signal_type']]
        
        # Factor C: Dumb Money Oversold (Contrarian)
        # Logic: "Others" (Retail) selling heavily, but Price > MA60 (Trend intact)
        # Identify "Others"
        all_elite = set(steady_hands + gems)
        others_df = df[~df['cube_symbol'].isin(all_elite)].copy()
        others_df['weight_delta'] = others_df['target_weight'] - others_df['prev_weight_adjusted']
        
        retail_sell = others_df[others_df['weight_delta'] < 0].groupby(['date', 'stock_symbol'])['weight_delta'].sum().reset_index()
        # Heavy selling: Net sell < -5% aggregate
        panic_sells = retail_sell[retail_sell['weight_delta'] < -5].copy()
        panic_sells['signal_type'] = 'Oversold'
        panic_sells = panic_sells.rename(columns={'stock_symbol': 'symbol'})[['date', 'symbol', 'signal_type']]
        
        # Combine All Signals
        signals = pd.concat([high_conviction, acc_signals, panic_sells])
        signals['date'] = pd.to_datetime(signals['date'])
        
        # Sort by date
        signals = signals.sort_values('date')
        
        logging.info(f"Generated {len(signals)} Upgraded Mainboard signals (Raw).")
        return signals

class MarketDataLoader:
    def __init__(self, cache_dir="data/market_cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.data_cache = {}

    def get_ohlcv(self, symbol, date):
        """Get single day OHLCV."""
        # Check memory cache
        if symbol in self.data_cache:
            df = self.data_cache[symbol]
            if date in df.index: return df.loc[date]
            return None
            
        # Check disk cache
        cache_file = os.path.join(self.cache_dir, f"{symbol}.csv")
        if os.path.exists(cache_file):
            try:
                df = pd.read_csv(cache_file, parse_dates=['date'], index_col='date')
                self.data_cache[symbol] = df
                if date in df.index: return df.loc[date]
            except: pass
            
        # Fetch (Batch for efficiency)
        bs.login()
        bs_symbol = symbol.lower().replace('sz', 'sz.').replace('sh', 'sh.')
        
        # Fetch 3 months around date to calc volatility
        s_date = (date - timedelta(days=60)).strftime("%Y-%m-%d")
        e_date = (date + timedelta(days=30)).strftime("%Y-%m-%d")
        
        rs = bs.query_history_k_data_plus(
            bs_symbol, "date,open,high,low,close,volume,pctChg",
            start_date=s_date, end_date=e_date, frequency="d", adjustflag="3"
        )
        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        bs.logout()
        
        if not data_list: return None
        
        df = pd.DataFrame(data_list, columns=rs.fields)
        cols = ['open', 'high', 'low', 'close', 'volume', 'pctChg']
        for c in cols: df[c] = pd.to_numeric(df[c], errors='coerce')
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        # Save to disk
        df.to_csv(cache_file)
        self.data_cache[symbol] = df
        
        if date in df.index: return df.loc[date]
        return None

    def get_volatility(self, symbol, date, window=20):
        """Check if stock is Low Volatility."""
        if symbol not in self.data_cache:
            self.get_ohlcv(symbol, date) # Trigger load
            
        if symbol in self.data_cache:
            df = self.data_cache[symbol]
            # Get data up to date
            hist = df.loc[:date].tail(window)
            if len(hist) < window: return 999 # Not enough data
            
            # Calc ATR or StdDev
            # Simple: StdDev of PctChg
            vol = hist['pctChg'].std()
            return vol
        return 999

    def get_ma(self, symbol, date, window):
        """Get Moving Average."""
        if symbol not in self.data_cache:
            self.get_ohlcv(symbol, date)
            
        if symbol in self.data_cache:
            df = self.data_cache[symbol]
            # Get data up to date
            hist = df.loc[:date].tail(window)
            if len(hist) < window: return None
            
            ma = hist['close'].mean()
            return ma
        return None

class SmallAccountEngine:
    def __init__(self, initial_capital=30000):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions = {}
        self.trade_log = []
        self.equity_curve = []
        self.market_loader = MarketDataLoader()
        
        # Config
        self.max_vol_threshold = 3.5 # StdDev of daily returns < 3.5%
        self.trailing_stop_pct = 0.05 # 5% trailing stop
        self.max_hold_days = 7
        self.comm_rate = 0.00025 # Low comms
        
        # New Config for Greedy Allocation
        self.base_allocation = 10000 # Target 10k per stock
        self.max_holdings = 4 # Soft Cap
        self.skipped_expensive_count = 0

    def run(self, signals_df):
        logging.info("Starting Small Account Backtest (Greedy Allocation)...")
        if signals_df.empty: return

        dates = sorted(signals_df['date'].unique())
        
        for date in dates:
            # 1. Update Portfolio & Check Exits
            self._process_exits(date)
            
            # 2. Process Entries (Greedy Logic)
            self._process_entries(date, signals_df)
            
            # 3. Record
            self._record_equity(date)
            
        logging.info(f"Skipped due to insufficient cash: {self.skipped_expensive_count} times.")

    def _process_exits(self, date):
        for symbol in list(self.positions.keys()):
            pos = self.positions[symbol]
            bar = self.market_loader.get_ohlcv(symbol, date)
            
            if bar is None: 
                pos['days_held'] += 1
                continue
            
            # Update High Watermark for Trailing Stop
            if bar['high'] > pos['highest_price']:
                pos['highest_price'] = bar['high']
            
            # Logic 1: Trailing Stop
            # If price falls 5% from highest since entry
            drawdown = (bar['close'] - pos['highest_price']) / pos['highest_price']
            
            # Logic 2: Time Exit (7 days)
            time_exit = pos['days_held'] >= self.max_hold_days
            
            # Logic 3: Hard Stop (-5% from cost)
            hard_stop = (bar['close'] - pos['cost_price']) / pos['cost_price'] < -0.05
            
            reason = ""
            if drawdown < -self.trailing_stop_pct: reason = "TrailingStop"
            elif time_exit: reason = "TimeExit"
            elif hard_stop: reason = "HardStop"
            
            if reason:
                self._sell(symbol, bar['open'], date, reason) # Sell next open usually, but here current close/open proxy
                # Simplified: Sell at Open of TODAY if condition met yesterday? 
                # Or Sell at Close of TODAY if condition met intraday?
                # Let's assume Sell at Close for simplicity in this engine version
                # Better: Sell at 'close'
                pass
            else:
                pos['days_held'] += 1

    def _process_entries(self, date, signals_df):
        # Look for signals generated on 'date' (to be executed tomorrow? or today?)
        # Standard: Signals generated T-1, Execute T.
        # Let's assume 'date' is execution day.
        # So we need signals from date - 1.
        
        target_signal_date = date - timedelta(days=1)
        # Find signals
        candidates = signals_df[signals_df['date'] == pd.Timestamp(target_signal_date)]
        
        # Deduplication Logic: Cooldown Check
        # We need to filter candidates that have been traded recently or are duplicate for today
        candidates = candidates.drop_duplicates(subset=['symbol']) # Keep one signal per stock per day
        
        # Sort candidates by Priority
        # Priority: ConvictionBuy > Accumulation > Oversold
        # Or simple map
        type_priority = {'ConvictionBuy': 3, 'Accumulation': 2, 'Oversold': 1}
        candidates['priority'] = candidates['signal_type'].map(type_priority)
        candidates = candidates.sort_values('priority', ascending=False)
        
        for _, row in candidates.iterrows():
            symbol = row['symbol']
            signal_type = row['signal_type']
            
            # 1. Soft Cap Check
            if len(self.positions) >= self.max_holdings:
                # logging.info(f"Skip {symbol}: Max holdings reached.")
                continue
                
            # 2. Cooldown Check (Re-entry allowed after cooldown)
            # If we hold it, skip.
            if symbol in self.positions: continue
            
            # 3. Price & Cash Check
            bar = self.market_loader.get_ohlcv(symbol, date)
            if bar is None: continue
            
            price = bar['open']
            
            # --- REMOVED ALL TECHNICAL FILTERS (Volatility, MA60) ---
            # Just follow the Smart Money Signal directly.
            
            # 4. Affordability-First Sizing (30k Capital)
            # Calculate 1 lot cost
            cost_1_lot = price * 100 * (1 + self.comm_rate)
            
            # Hard Check: Can we afford 1 lot?
            if self.cash < cost_1_lot:
                self.skipped_expensive_count += 1
                continue
                
            # Target allocation: 50% of available cash
            ideal_spend = self.cash / 2
            
            # Calculate Lots
            expected_lots = int(ideal_spend / cost_1_lot)
            
            # KEY OVERRIDE: If expected is 0 but we can afford 1 lot, buy 1 lot!
            if expected_lots == 0:
                expected_lots = 1
                
            shares = expected_lots * 100
            
            # Double check total cost against cash
            total_cost = shares * price * (1 + self.comm_rate)
            if self.cash >= total_cost:
                self._buy(symbol, price, shares, date)

    def _buy(self, symbol, price, shares, date):
        cost = price * shares
        self.cash -= cost * (1 + self.comm_rate)
        self.positions[symbol] = {
            'shares': shares,
            'cost_price': price,
            'highest_price': price,
            'days_held': 0
        }
        # logging.info(f"BUY {symbol} @ {price}")

    def _sell(self, symbol, price, date, reason):
        pos = self.positions.pop(symbol)
        rev = price * pos['shares']
        self.cash += rev * (1 - self.comm_rate - 0.001) # Tax
        
        pnl = (price - pos['cost_price']) / pos['cost_price']
        self.trade_log.append({
            'symbol': symbol,
            'pnl': pnl,
            'reason': reason,
            'date': date
        })
        # logging.info(f"SELL {symbol} ({reason}): {pnl*100:.1f}%")

    def _record_equity(self, date):
        mv = 0
        for s, p in self.positions.items():
            bar = self.market_loader.get_ohlcv(s, date)
            price = bar['close'] if bar is not None else p['cost_price']
            mv += p['shares'] * price
        
        self.equity_curve.append({'date': date, 'equity': self.cash + mv})

    def report(self):
        if not self.equity_curve: return
        df = pd.DataFrame(self.equity_curve)
        final = df.iloc[-1]['equity']
        ret = (final - self.initial_capital) / self.initial_capital
        
        logging.info("="*40)
        logging.info("SMALL ACCOUNT (MAINBOARD) REPORT")
        logging.info("="*40)
        logging.info(f"Initial: {self.initial_capital}")
        logging.info(f"Final:   {final:.2f}")
        logging.info(f"Return:  {ret*100:.2f}%")
        logging.info(f"Trades:  {len(self.trade_log)}")
        
        # Win Rate
        wins = [t for t in self.trade_log if t['pnl'] > 0]
        if self.trade_log:
            logging.info(f"Win Rate: {len(wins)/len(self.trade_log)*100:.1f}%")

if __name__ == "__main__":
    loader = MainboardSignalLoader()
    signals = loader.load_signals()
    
    if not signals.empty:
        engine = SmallAccountEngine(initial_capital=20000)
        engine.run(signals)
        engine.report()
