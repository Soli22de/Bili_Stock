
import pandas as pd
import numpy as np
import logging
import os
import sqlite3
import baostock as bs
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

class SmartSignalLoader:
    def __init__(self, db_path="data/cubes.db"):
        self.db_path = db_path

    def load_signals(self, min_score=2):
        """Load real consensus signals from DB."""
        if not os.path.exists(self.db_path):
            logging.error(f"Database not found: {self.db_path}")
            return pd.DataFrame()

        logging.info("Loading Real Rebalancing Data...")
        conn = sqlite3.connect(self.db_path)
        
        # Load history
        try:
            df = pd.read_sql_query("SELECT * FROM rebalancing_history", conn)
        except Exception as e:
            logging.error(f"Failed to load rebalancing history: {e}")
            conn.close()
            return pd.DataFrame()
        
        conn.close()
        
        if df.empty:
            logging.warning("No rebalancing history found.")
            return pd.DataFrame()

        # Preprocess
        # Ensure date format
        if 'date' not in df.columns and 'created_at' in df.columns:
            df['date'] = df['created_at']
            
        df['date'] = pd.to_datetime(df['date'], format='mixed', errors='coerce').dt.date
        df = df.dropna(subset=['date']) # Drop invalid dates
        df['weight_delta'] = df['target_weight'] - df['prev_weight_adjusted']
        
        # --- A-Share Filtering ---
        # Keep only SHxxxxxx or SZxxxxxx (6 digits)
        # Regex: ^(SH|SZ)\d{6}$
        logging.info(f"Filtering A-shares from {len(df)} records...")
        df = df[df['stock_symbol'].astype(str).str.match(r'^(SH|SZ)\d{6}$', na=False)]
        logging.info(f"Records after A-share filter: {len(df)}")
        
        # Calculate Action: Buy > 0, Sell < 0
        df['action'] = df['weight_delta'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        
        # Group by Date + Stock to get Daily Consensus Score
        logging.info("Calculating Daily Consensus Scores...")
        daily_scores = df.groupby(['date', 'stock_symbol'])['action'].sum().reset_index()
        daily_scores.rename(columns={'action': 'daily_score', 'stock_symbol': 'symbol'}, inplace=True)
        
        # --- Rolling Consensus Logic ---
        # Need to pivot to apply rolling sum
        logging.info("Calculating 3-Day Rolling Consensus...")
        pivot_scores = daily_scores.pivot(index='date', columns='symbol', values='daily_score').fillna(0)
        
        # Apply Rolling Sum (Window=3)
        rolling_scores = pivot_scores.rolling(window=3, min_periods=1).sum()
        
        # Stack back to long format
        signals = rolling_scores.stack().reset_index()
        signals.columns = ['date', 'symbol', 'consensus_score']
        
        # Filter: Rolling Score >= 2 AND Daily Score > 0 (Must have buy action today)
        # Join with daily_scores to check today's action
        signals = pd.merge(signals, daily_scores, on=['date', 'symbol'], how='left')
        signals['daily_score'] = signals['daily_score'].fillna(0)
        
        # Condition: Rolling >= 2 AND Daily > 0
        signals = signals[
            (signals['consensus_score'] >= min_score) & 
            (signals['daily_score'] > 0)
        ].copy()
        
        # Convert date to datetime for index alignment
        signals['date'] = pd.to_datetime(signals['date'])
        
        logging.info(f"Loaded {len(signals)} valid rolling signals (Score >= {min_score}).")
        return signals

class MarketDataLoader:
    def __init__(self, cache_dir="data/market_cache"):
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        self.bs_logged_in = False

    def purge_cache(self):
        """Purge all cache files to remove poisoned data."""
        logging.info(f"PURGING CACHE: Deleting all CSV files in {self.cache_dir}...")
        count = 0
        if os.path.exists(self.cache_dir):
            for f in os.listdir(self.cache_dir):
                if f.endswith(".csv"):
                    try:
                        os.remove(os.path.join(self.cache_dir, f))
                        count += 1
                    except Exception as e:
                        logging.error(f"Failed to delete {f}: {e}")
        logging.info(f"Purged {count} cache files.")

    def _login(self):
        if not self.bs_logged_in:
            bs.login()
            self.bs_logged_in = True

    def _logout(self):
        if self.bs_logged_in:
            bs.logout()
            self.bs_logged_in = False

    def get_ohlcv(self, symbol, start_date, end_date):
        """Get OHLCV from Cache or BaoStock."""
        # Clean symbol format (SH600000 -> sh.600000)
        # BaoStock requires lower case and dot separator.
        symbol = str(symbol).strip().upper()
        if symbol.startswith('SH'):
            bs_symbol = 'sh.' + symbol[2:]
        elif symbol.startswith('SZ'):
            bs_symbol = 'sz.' + symbol[2:]
        else:
            logging.warning(f"Invalid symbol format for BaoStock: {symbol}")
            return pd.DataFrame()
        
        cache_file = os.path.join(self.cache_dir, f"{symbol}.csv")
        df_cache = pd.DataFrame()
        
        # 1. Try Cache
        if os.path.exists(cache_file):
            try:
                df_cache = pd.read_csv(cache_file, parse_dates=['date'], index_col='date')
            except Exception as e:
                logging.warning(f"Cache corrupt for {symbol}: {e}")

        # Determine what to fetch
        fetch_start = start_date
        
        if not df_cache.empty:
            last_date = df_cache.index.max()
            if last_date >= end_date - timedelta(days=1):
                # Cache is fresh enough
                return df_cache
            
            # Need to update
            fetch_start = last_date + timedelta(days=1)
            
        # 2. Fetch from BaoStock
        self._login()
        
        # Convert dates to string YYYY-MM-DD
        s_str = fetch_start.strftime("%Y-%m-%d")
        e_str = end_date.strftime("%Y-%m-%d")
        
        if fetch_start > end_date:
            return df_cache

        rs = bs.query_history_k_data_plus(
            bs_symbol,
            "date,open,high,low,close,volume,pctChg,isST",
            start_date=s_str, end_date=e_str,
            frequency="d", adjustflag="3" 
        )
        
        if rs.error_code != '0':
            logging.warning(f"BaoStock Error for {symbol}: {rs.error_msg}")
            return df_cache if not df_cache.empty else pd.DataFrame()
            
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
            
        if not data_list:
            # If fetch failed or no data, return what we have
            # But maybe we should verify if the stock is delisted or suspended?
            # For now, just return cache.
            return df_cache if not df_cache.empty else pd.DataFrame()
            
        df_new = pd.DataFrame(data_list, columns=rs.fields)
        
        # Type conversion
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'pctChg']
        for col in numeric_cols:
            df_new[col] = pd.to_numeric(df_new[col], errors='coerce')
            
        df_new['date'] = pd.to_datetime(df_new['date'])
        df_new.set_index('date', inplace=True)
        
        # Merge
        if not df_cache.empty:
            # Combine and drop duplicates
            df = pd.concat([df_cache, df_new])
            df = df[~df.index.duplicated(keep='last')]
        else:
            df = df_new
            
        df.sort_index(inplace=True)
        
        # Save to Cache
        df.to_csv(cache_file)
        
        return df

class SmartBacktestEngine:
    def __init__(self, initial_capital=30000):
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.positions = {} 
        self.trade_log = []
        self.equity_curve = []
        
        self.market_loader = MarketDataLoader()
        
        # Config
        self.comm_buy = 0.0002
        self.comm_sell = 0.0012
        
        # 资金利用率优化 (Capital Efficiency Optimization)
        # 单笔 25% (约 7.5k)，最大持仓 4 只。
        self.position_pct = 0.25 
        self.max_daily_buy = 2 # 每日最多买入
        self.max_holdings = 4  # Implicit limit based on position_pct (1/0.25 = 4)
        
        # Momentum Strategy Config
        self.watchlist_window = 5 # 观察期 5 天
        self.hard_stop_pct = 0.07 # 7% 硬止损
        self.vol_multiplier = 1.5 # 1.5倍放量
        self.min_breakout_pct = 3.0 # 3% 涨幅突破
        
        # Signal Funnel Tracker
        self.funnel = {
            'Total_Signals_Observed': 0,
            'Added_to_Watchlist': 0,
            'Triggered_Breakout': 0,
            'Blocked_by_Max_Holdings': 0,
            'Blocked_by_No_Market_Data': 0,
            'Blocked_by_Limit_Up': 0,
            'Blocked_by_ST': 0, 
            'Blocked_by_Board': 0, # New Board Filter
            'Skipped_Too_Expensive': 0,
            'Successfully_Executed_Trades': 0
        }
        
        # Watchlist: {symbol: expiry_date}
        self.watchlist = {}

    def _get_trading_days(self, start_date, end_date):
        """Get list of trading days from BaoStock."""
        self.market_loader._login()
        s_str = start_date.strftime("%Y-%m-%d")
        e_str = end_date.strftime("%Y-%m-%d")
        
        rs = bs.query_trade_dates(start_date=s_str, end_date=e_str)
        if rs.error_code != '0':
            logging.error(f"BS Error: {rs.error_msg}")
            # Fallback to weekdays
            return pd.date_range(start=start_date, end=end_date, freq='B')
            
        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
            
        df = pd.DataFrame(data_list, columns=rs.fields)
        # Filter is_trading_day == '1'
        df = df[df['is_trading_day'] == '1']
        return pd.to_datetime(df['calendar_date']).tolist()

    def _get_stock_data(self, symbol, date):
        """Get OHLCV for specific date. Handles caching."""
        if symbol not in self.data_cache:
            s_date = date - timedelta(days=100) # Fetch enough for MA
            e_date = datetime.now()
            df = self.market_loader.get_ohlcv(symbol, s_date, e_date)
            self.data_cache[symbol] = df
            
        df = self.data_cache[symbol]
        if df.empty:
            return None
        
        # Check specific date
        if date in df.index:
            return df.loc[date]
        return None

    def _get_history(self, symbol, date, lookback=100):
        """Get historical data up to date (inclusive)"""
        if symbol not in self.data_cache:
            self._get_stock_data(symbol, date) # Prime cache
            
        df = self.data_cache.get(symbol)
        if df is None or df.empty:
            return None
            
        # Get data up to date
        mask = df.index <= date
        return df[mask].tail(lookback)

    def _print_funnel(self):
        logging.info("\n" + "="*40)
        logging.info("SMART MOMENTUM FUNNEL TRACKER")
        logging.info("="*40)
        logging.info(f"Total Signals Observed        : {self.funnel['Total_Signals_Observed']}")
        logging.info(f"Added to Watchlist            : {self.funnel['Added_to_Watchlist']}")
        logging.info("-" * 40)
        logging.info(f"🔥 Triggered Breakout         : {self.funnel['Triggered_Breakout']}")
        logging.info("-" * 40)
        logging.info(f"❌ Blocked by Max Holdings    : {self.funnel['Blocked_by_Max_Holdings']}")
        logging.info(f"❌ Blocked by No Market Data  : {self.funnel['Blocked_by_No_Market_Data']}")
        logging.info(f"❌ Blocked by Limit Up        : {self.funnel['Blocked_by_Limit_Up']}")
        logging.info(f"❌ Blocked by ST/Delisted     : {self.funnel['Blocked_by_ST']}")
        logging.info(f"❌ Blocked by Board (688/300) : {self.funnel['Blocked_by_Board']}")
        logging.info(f"❌ Skipped Too Expensive      : {self.funnel['Skipped_Too_Expensive']}")
        logging.info("-" * 40)
        logging.info(f"✅ Successfully Executed      : {self.funnel['Successfully_Executed_Trades']}")
        logging.info("="*40)

    def run(self, signals_df):
        logging.info("Starting Smart Momentum Backtest...")
        
        # Get Date Range
        if signals_df.empty:
            logging.warning("No signals to backtest.")
            return

        min_date = signals_df['date'].min()
        max_date = signals_df['date'].max() + timedelta(days=20) # Buffer for exit
        
        # Iterate by TRADING Day
        end_date = datetime.now() if max_date > datetime.now() else max_date
        
        try:
            trading_days = self._get_trading_days(min_date, end_date)
            logging.info(f"Loaded {len(trading_days)} trading days.")
        except Exception as e:
            logging.error(f"Failed to load trading days: {e}")
            return

        self.data_cache = {} 
        self.watchlist = {} # Reset Watchlist
        
        # We need previous trading day to find signals
        prev_trading_day = None
        
        for date in trading_days:
            self._process_day(date, signals_df, prev_trading_day)
            prev_trading_day = date
            
        self.market_loader._logout()
        self._print_funnel()

    def _process_day(self, date, signals_df, prev_trading_day):
        # 1. Update Watchlist (Remove Expired)
        # Use list() to avoid runtime error during modification
        expired = [s for s, exp in self.watchlist.items() if date > exp]
        for s in expired:
            del self.watchlist[s]
            
        # 2. Add New Signals to Watchlist (From Prev Trading Day)
        if prev_trading_day is not None:
            target_signal_date = pd.Timestamp(prev_trading_day)
            todays_signals = signals_df[signals_df['date'] == target_signal_date]
            
            self.funnel['Total_Signals_Observed'] += len(todays_signals)
            
            for _, row in todays_signals.iterrows():
                symbol = row['symbol']
                # Add to Watchlist with Expiry
                # Expiry = Current Date + 5 Trading Days? 
                # Simplification: Calendar days or just check age.
                # Let's set expiry to date + 10 calendar days (approx 5-7 trading days)
                self.watchlist[symbol] = date + timedelta(days=10)
                self.funnel['Added_to_Watchlist'] += 1

        # 3. Process Exits (Momentum Exit & Hard Stop)
        # Copy positions to allow modification
        for symbol in list(self.positions.keys()):
            pos = self.positions[symbol]
            bar = self._get_stock_data(symbol, date)

            if bar is None: continue
            
            # --- Hard Stop (-7%) ---
            if bar['low'] < pos['entry_price'] * (1 - self.hard_stop_pct):
                self._sell(symbol, bar['close'], date, "HardStop")
                continue
                
            # --- Trend Exit (Close < MA10) ---
            # Need history for MA10
            history = self._get_history(symbol, date, lookback=20)
            
            # Let's stick to simple: Sell at Close if condition met.
            # "Close < MA10" -> Sell at Close.
            if history is not None and len(history) >= 10:
                 ma10 = history['close'].rolling(10).mean().iloc[-1]
                 if bar['close'] < ma10:
                     self._sell(symbol, bar['close'], date, "TrendExit")
                     continue
            
            # Increment Hold Day
            self.positions[symbol]['days_held'] += 1

        # 4. Scan Watchlist for Breakouts (Entries)
        daily_buys = 0
        
        # --- CORRECT ENTRY LOGIC ---
        # Check Watchlist against YESTERDAY's data to buy TODAY.
        if prev_trading_day is not None:
             for symbol in list(self.watchlist.keys()):
                if daily_buys >= self.max_daily_buy: break
                if len(self.positions) >= self.max_holdings: 
                    self.funnel['Blocked_by_Max_Holdings'] += 1
                    break
                if symbol in self.positions: continue
                
                # Get Yesterday's Data
                history = self._get_history(symbol, prev_trading_day, lookback=20)
                if history is None or len(history) < 10: 
                    continue
                    
                yesterday_bar = history.iloc[-1]
                
                # --- Risk Filters (on Yesterday's data is fine) ---
                if str(yesterday_bar.get('isST', '0')) == '1':
                    self.funnel['Blocked_by_ST'] += 1; 
                    if symbol in self.watchlist: del self.watchlist[symbol]; 
                    continue
                if symbol.startswith('SH688') or symbol.startswith('SZ300'):
                    self.funnel['Blocked_by_Board'] += 1; 
                    if symbol in self.watchlist: del self.watchlist[symbol]; 
                    continue
                    
                # --- Triggers (on Yesterday) ---
                # 1. Trend: Close > MA10
                ma10 = history['close'].rolling(10).mean().iloc[-1]
                if yesterday_bar['close'] <= ma10: continue
                
                # 2. Price: Close > Open & PctChg > 3%
                if not (yesterday_bar['close'] > yesterday_bar['open'] and yesterday_bar['pctChg'] > self.min_breakout_pct):
                    continue
                    
                # 3. Volume: Vol > 1.5 * MA5
                vol_ma5 = history['volume'].rolling(5).mean().iloc[-1]
                if yesterday_bar['volume'] <= self.vol_multiplier * vol_ma5:
                    continue
                    
                # TRIGGERED!
                self.funnel['Triggered_Breakout'] += 1
                
                # EXECUTE BUY TODAY (Open)
                bar_today = self._get_stock_data(symbol, date)
                if bar_today is None:
                    self.funnel['Blocked_by_No_Market_Data'] += 1
                    continue
                    
                # Limit Up Check (Today)
                if bar_today['open'] == bar_today['high'] and bar_today['pctChg'] > 9.5:
                    self.funnel['Blocked_by_Limit_Up'] += 1
                    continue
                    
                # Calculate Size
                current_equity = self._calculate_equity(date)
                target_amt = current_equity * self.position_pct
                price = bar_today['open']
                if price <= 0: continue
                
                shares = int(target_amt / price / 100) * 100
                if shares < 100:
                    self.funnel['Skipped_Too_Expensive'] += 1
                    continue
                    
                cost = shares * price * (1 + self.comm_buy)
                if self.cash < cost:
                    self.funnel['Blocked_by_Max_Holdings'] += 1
                    continue
                    
                self._buy(symbol, price, shares, date)
                self.funnel['Successfully_Executed_Trades'] += 1
                daily_buys += 1
                
                # Remove from watchlist after buy?
                if symbol in self.watchlist: del self.watchlist[symbol]

        # 5. Record Equity
        total_eq = self._calculate_equity(date)
        self.equity_curve.append({'date': date, 'equity': total_eq})

    def _calculate_equity(self, date):
        mv = 0
        for symbol, pos in self.positions.items():
            try:
                bar = self._get_stock_data(symbol, date)
                price = bar['close'] if bar is not None else pos['entry_price'] # Fallback
            except:
                price = pos['entry_price']
            mv += pos['shares'] * price
        return self.cash + mv

    def _buy(self, symbol, price, shares, date):
        cost = shares * price
        comm = cost * self.comm_buy
        self.cash -= (cost + comm)
        self.positions[symbol] = {
            'shares': shares,
            'entry_price': price,
            'entry_date': date,
            'days_held': 0
        }
        logging.info(f"[{date.date()}] 🚀 BUY {symbol} @ {price:.2f} (Breakout)")

    def _sell(self, symbol, price, date, reason):
        pos = self.positions.pop(symbol)
        rev = pos['shares'] * price
        comm = rev * self.comm_sell
        self.cash += (rev - comm)
        
        pnl = (rev - comm) - (pos['shares'] * pos['entry_price'] * (1 + self.comm_buy))
        pnl_pct = pnl / (pos['shares'] * pos['entry_price'])
        
        self.trade_log.append({
            'symbol': symbol,
            'entry_date': pos['entry_date'],
            'exit_date': date,
            'entry_price': pos['entry_price'],
            'exit_price': price,
            'pnl': pnl,
            'pnl_pct': pnl_pct,
            'reason': reason
        })
        logging.info(f"[{date.date()}] 🔻 SELL {symbol} ({reason}): {pnl_pct*100:.2f}%")

    def report(self):
        if not self.equity_curve:
            logging.info("No equity history.")
            return

        df_eq = pd.DataFrame(self.equity_curve).set_index('date')
        df_trades = pd.DataFrame(self.trade_log)
        
        final_equity = df_eq['equity'].iloc[-1]
        ret = (final_equity - self.initial_capital) / self.initial_capital
        
        # Drawdown
        df_eq['peak'] = df_eq['equity'].cummax()
        df_eq['dd'] = (df_eq['equity'] - df_eq['peak']) / df_eq['peak']
        max_dd = df_eq['dd'].min()
        
        win_rate = 0
        if not df_trades.empty:
            win_rate = len(df_trades[df_trades['pnl'] > 0]) / len(df_trades)

        logging.info("\n" + "="*40)
        logging.info("SMART MOMENTUM BACKTEST REPORT")
        logging.info("="*40)
        logging.info(f"Initial Capital : {self.initial_capital:,.2f}")
        logging.info(f"Final Equity    : {final_equity:,.2f}")
        logging.info(f"Total Return    : {ret*100:.2f}%")
        logging.info(f"Max Drawdown    : {max_dd*100:.2f}%")
        logging.info(f"Total Trades    : {len(df_trades)}")
        logging.info(f"Win Rate        : {win_rate*100:.2f}%")
        logging.info("="*40)
        
        df_trades.to_csv("data/smart_momentum_trades.csv", index=False)
        logging.info("Trade log saved to data/smart_momentum_trades.csv")

if __name__ == "__main__":
    # 0. Purge Cache (Optional, but good for safety)
    # temp_loader = MarketDataLoader()
    # temp_loader.purge_cache()
    
    # 1. Load Signals
    loader = SmartSignalLoader()
    signals = loader.load_signals(min_score=2)
    
    if not signals.empty:
        # Filter signals to 2025-2026 for speed and relevance
        signals = signals[signals['date'] >= pd.Timestamp('2025-01-01')]
        
        # 2. Run Backtest
        engine = SmartBacktestEngine()
        engine.run(signals)
        engine.report()
    else:
        logging.warning("No signals found.")
