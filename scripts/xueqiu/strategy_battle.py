import pandas as pd
import numpy as np
import sqlite3
import baostock as bs
import akshare as ak
import logging
import os
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import time
import random

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

BACKTEST_CONFIG = {
    "initial_capital": 100000,
    "lot_size": 100,
    "buy_commission": 0.0003,
    "sell_commission": 0.0003,
    "sell_stamp_tax": 0.001,
    "min_commission": 5.0,
    "slippage_mainboard": 0.0008,
    "slippage_growth": 0.0012,
    "slippage_etf": 0.0003,
    "max_position_weight": 0.2,
    "max_board_weight": 0.4,
    "enable_t1_execution": True
}

class BattleData:
    def __init__(self, db_path="data/cubes.db"):
        self.db_path = db_path
        self.conn = None
        self.data_quality = {}

    def load_data(self, start_date="2025-01-01", end_date="2026-03-01"):
        logging.info(f"Loading data from {self.db_path}...")
        self.conn = sqlite3.connect(self.db_path)
        
        # 1. Load Metadata (Identify Smart Money vs Dumb Money)
        cubes = pd.read_sql("SELECT symbol, total_gain, followers_count FROM cubes", self.conn)
        
        # Smart Money Definition: 
        # Legends: Gain > 50%, Followers > 1000
        # Hidden Gems: Gain > 30%, Followers < 500
        # (Using relatively loose criteria to get enough breadth for 1000+ universe)
        legends = cubes[(cubes['total_gain'] > 50) & (cubes['followers_count'] > 1000)]
        gems = cubes[(cubes['total_gain'] > 30) & (cubes['followers_count'] < 500)]
        self.smart_money = set(legends['symbol']).union(set(gems['symbol']))
        
        logging.info(f"Identified {len(self.smart_money)} Smart Money cubes out of {len(cubes)} total.")
        
        # 2. Load Rebalancing History
        query = f"""
            SELECT * FROM rebalancing_history 
            WHERE created_at >= '{start_date}' AND created_at <= '{end_date}'
        """
        history = pd.read_sql(query, self.conn)
        
        # Preprocessing
        if 'date' not in history.columns:
            # history['date'] = pd.to_datetime(history['created_at'], format='mixed').dt.date
            # Convert to string format YYYY-MM-DD for consistency
            history['date'] = pd.to_datetime(history['created_at'], format='mixed').dt.strftime('%Y-%m-%d')
        else:
            history['date'] = pd.to_datetime(history['date'], format='mixed').dt.strftime('%Y-%m-%d')
            
        # Filter valid stocks (SH/SZ 6 digits)
        # Refined regex to include only A-shares (Mainboard/SME/ChiNext/STAR)
        # Excludes Bonds (11/12), Funds (15/51), etc.
        # SH: 60xxxx, 68xxxx
        # SZ: 00xxxx, 30xxxx
        stock_pattern = r'^(SH60|SH68|SZ00|SZ30)\d{4}$'
        valid_stocks = history['stock_symbol'].astype(str).str.match(stock_pattern, na=False)
        invalid_count = len(history) - valid_stocks.sum()
        if invalid_count > 0:
            logging.info(f"Filtered out {invalid_count} non-stock assets (Bonds/Funds/etc).")
        history = history[valid_stocks]
        st_filtered = 0
        if 'stock_name' in history.columns:
            st_mask = history['stock_name'].astype(str).str.contains(r'ST|\*ST', case=False, na=False)
            st_filtered = int(st_mask.sum())
            history = history[~st_mask]
        
        # Calculate Weight Delta
        history['weight_delta'] = history['target_weight'] - history['prev_weight_adjusted']
        
        self.history = history
        self.conn.close()
        self.data_quality = {
            "invalid_assets_filtered": int(invalid_count),
            "st_filtered": int(st_filtered),
            "remaining_rows": int(len(history))
        }
        
        # Group by date for faster access
        # Calculate aggregated net increase per stock per day
        # For Battle: We want "Net Increase" across ALL cubes for Dumb Money
        daily_stats = history.groupby(['date', 'stock_symbol'])['weight_delta'].sum().reset_index()
        
        # Sort each day by net increase descending
        data_by_date = {}
        for date in daily_stats['date'].unique():
            day_data = daily_stats[daily_stats['date'] == date]
            sorted_data = day_data.sort_values('weight_delta', ascending=False)
            
            # Convert to list of dicts: [{'symbol': 'SZ000001', 'net_increase': 1.5}, ...]
            records = []
            for _, row in sorted_data.iterrows():
                records.append({
                    'symbol': row['stock_symbol'],
                    'net_increase': row['weight_delta']
                })
            data_by_date[date] = records
            
        return data_by_date

class MarketEngine:
    def __init__(self):
        self.cache_dir = "data/market_cache"
        os.makedirs(self.cache_dir, exist_ok=True)
        self.bs_logged_in = False
        self.blacklist = set() # Symbols that failed all sources

    def _login(self):
        if not self.bs_logged_in:
            bs.login()
            self.bs_logged_in = True

    def _logout(self):
        if self.bs_logged_in:
            bs.logout()
            self.bs_logged_in = False

    def _fetch_baostock(self, symbol, start_date, end_date):
        # Fetch with Retry
        max_retries = 3
        data = []
        
        for attempt in range(max_retries):
            try:
                self._login()
                bs_symbol = symbol.lower().replace('sz', 'sz.').replace('sh', 'sh.')
                rs = bs.query_history_k_data_plus(
                    bs_symbol, "date,open,close,pctChg",
                    start_date=start_date, end_date=end_date,
                    frequency="d", adjustflag="3"
                )
                
                while rs.error_code == '0' and rs.next():
                    data.append(rs.get_row_data())
                
                if data:
                    break # Success
                
                if rs.error_code != '0':
                    logging.warning(f"BaoStock Error {rs.error_code} for {symbol}. Retrying ({attempt+1}/{max_retries})...")
                    self._logout() # Force relogin
            except Exception as e:
                 logging.warning(f"BaoStock Connection Error for {symbol}: {e}. Retrying ({attempt+1}/{max_retries})...")
                 self._logout()
                 time.sleep(1)
        
        if not data: return pd.DataFrame()
        
        df = pd.DataFrame(data, columns=['date', 'open', 'close', 'pctChg'])
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        
        # Clean empty strings
        df.replace('', np.nan, inplace=True)
        df.dropna(subset=['close', 'open'], inplace=True)
        
        df['close'] = df['close'].astype(float)
        df['open'] = df['open'].astype(float)
        if 'pctChg' in df.columns:
            df['pctChg'] = df['pctChg'].astype(float)
            
        return df

    def _fetch_benchmark_baostock(self, bs_symbol, start_date, end_date):
        data = []
        try:
            self._login()
            rs = bs.query_history_k_data_plus(
                bs_symbol, "date,close",
                start_date=start_date, end_date=end_date,
                frequency="d", adjustflag="3"
            )
            while rs.error_code == '0' and rs.next():
                data.append(rs.get_row_data())
        except Exception as e:
            logging.warning(f"Benchmark fetch error for {bs_symbol}: {e}")
        if not data:
            return pd.DataFrame()
        df = pd.DataFrame(data, columns=['date', 'close'])
        df['date'] = pd.to_datetime(df['date'])
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df.dropna(subset=['close'], inplace=True)
        df.set_index('date', inplace=True)
        return df

    def get_benchmark_history(self, benchmark_name, start_date, end_date):
        bm_map = {
            "HS300": "sh.000300",
            "CSI1000": "sh.000852"
        }
        bs_symbol = bm_map.get(benchmark_name)
        if not bs_symbol:
            return pd.DataFrame()
        df = self._fetch_benchmark_baostock(bs_symbol, start_date, end_date)
        if df.empty and os.getenv("ENABLE_AKSHARE_FALLBACK", "0") == "1":
            ak_map = {
                "HS300": "sh000300",
                "CSI1000": "sh000852"
            }
            ak_symbol = ak_map.get(benchmark_name)
            try:
                ak_df = ak.stock_zh_index_daily_em(symbol=ak_symbol)
                if not ak_df.empty:
                    ak_df['date'] = pd.to_datetime(ak_df['date'])
                    ak_df.set_index('date', inplace=True)
                    if 'close' in ak_df.columns:
                        df = ak_df[['close']].copy()
            except Exception:
                pass
        if df.empty:
            return pd.DataFrame()
        req_start = pd.to_datetime(start_date)
        req_end = pd.to_datetime(end_date)
        return df.loc[req_start:req_end]

    def get_price_history(self, symbol, start_date, end_date):
        if symbol in self.blacklist:
            return pd.DataFrame()

        # 1. Check cache first
        cache_file = os.path.join(self.cache_dir, f"{symbol}.csv")
        df_cached = pd.DataFrame()
        
        if os.path.exists(cache_file):
            try:
                df_cached = pd.read_csv(cache_file, parse_dates=['date'], index_col='date')
                # Check if requested range is covered
                if not df_cached.empty:
                    # Convert to datetime for comparison
                    req_start = pd.to_datetime(start_date)
                    req_end = pd.to_datetime(end_date)
                    
                    cache_start = df_cached.index.min()
                    cache_end = df_cached.index.max()
                    
                    if cache_start <= req_start and cache_end >= req_end:
                        # Fully covered
                        return df_cached.loc[req_start:req_end]
            except Exception as e:
                logging.warning(f"Cache read error for {symbol}: {e}")
        
        # 2. Fetch Full History: BaoStock First, AKShare Optional Fallback
        df = pd.DataFrame()
        bs_start = "2025-01-01"
        bs_end = datetime.now().strftime("%Y-%m-%d")
        df = self._fetch_baostock(symbol, bs_start, bs_end)

        if df.empty and os.getenv("ENABLE_AKSHARE_FALLBACK", "0") == "1":
            fetch_start = "20250101"
            fetch_end = datetime.now().strftime("%Y%m%d")
            try:
                code = symbol[2:]
                if code.isdigit() and len(code) == 6:
                    df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=fetch_start, end_date=fetch_end, adjust="hfq")
                    if df.empty:
                        logging.info(f"AKShare returned empty for {symbol}.")
                else:
                    logging.warning(f"Invalid code for AKShare: {code}")
            except Exception as e:
                logging.info(f"AKShare fallback failed for {symbol}: {e}")
            
        if df.empty:
            logging.warning(f"All data sources failed for {symbol}. Blacklisting.")
            self.blacklist.add(symbol)
            return pd.DataFrame()

        try:
            # Map columns if from AKShare (BaoStock already mapped)
            if '日期' in df.columns:
                df.rename(columns={'日期': 'date', '开盘': 'open', '收盘': 'close', '涨跌幅': 'pctChg'}, inplace=True)
                df = df[['date', 'open', 'close', 'pctChg']]
                df['date'] = pd.to_datetime(df['date'])
                df.set_index('date', inplace=True)
            
            # Save full history to cache
            df.to_csv(cache_file)
            
            # Return requested slice
            req_start = pd.to_datetime(start_date)
            req_end = pd.to_datetime(end_date)
            return df.loc[req_start:req_end]
            
        except Exception as e:
            logging.error(f"Data processing error for {symbol}: {e}")
            return pd.DataFrame()

    def get_momentum(self, symbol, date, window=20):
        # Need history prior to date
        start_dt = (pd.to_datetime(date) - timedelta(days=window*2)).strftime("%Y-%m-%d")
        df = self.get_price_history(symbol, start_dt, str(date))
        
        if df.empty: return 0
        
        # Get slice up to date
        df_slice = df.loc[:date]
        if len(df_slice) < window: return 0
        
        # Momentum: Return over window
        p_now = df_slice.iloc[-1]['close']
        p_prev = df_slice.iloc[-window]['close']
        return (p_now - p_prev) / p_prev

class StrategyEngine:
    def __init__(self, name="Strategy", initial_capital=100000, market_engine=None, config=None):
        self.name = name
        self.config = config if config else BACKTEST_CONFIG
        self.initial_capital = initial_capital
        self.cash = initial_capital
        self.holdings = {}
        self.history_val = []
        self.trade_log = []
        self.market = market_engine if market_engine else MarketEngine()
        self.last_prices = {}
        self.cost_stats = {'commission': 0.0, 'stamp_tax': 0.0, 'slippage': 0.0}
        self.constraint_stats = {'position_cap_hit': 0, 'board_cap_hit': 0}

    def _board(self, symbol):
        if symbol.startswith("SH68") or symbol.startswith("SZ30"):
            return "growth"
        if symbol.startswith("SH60") or symbol.startswith("SZ00"):
            return "mainboard"
        return "other"

    def _slippage_rate(self, symbol):
        board = self._board(symbol)
        if board == "growth":
            return self.config['slippage_growth']
        if board == "mainboard":
            return self.config['slippage_mainboard']
        return self.config['slippage_etf']

    def _get_price(self, symbol, date, field='open'):
        df = self.market.get_price_history(symbol, str(date), str(date))
        if df.empty:
            return self.last_prices.get(symbol, 0)
        if field in df.columns:
            price = float(df.iloc[-1][field])
        else:
            price = float(df.iloc[-1]['close'])
        if price > 0:
            self.last_prices[symbol] = price
        return price

    def _buy_cost(self, symbol, gross_amount):
        commission = max(gross_amount * self.config['buy_commission'], self.config['min_commission'])
        slippage = gross_amount * self._slippage_rate(symbol)
        total_cost = gross_amount + commission + slippage
        return total_cost, commission, slippage

    def _sell_revenue(self, symbol, gross_amount):
        commission = max(gross_amount * self.config['sell_commission'], self.config['min_commission'])
        stamp_tax = gross_amount * self.config['sell_stamp_tax']
        slippage = gross_amount * self._slippage_rate(symbol)
        net_revenue = gross_amount - commission - stamp_tax - slippage
        return net_revenue, commission, stamp_tax, slippage

    def _estimate_total_value(self, date):
        total = self.cash
        for symbol, shares in self.holdings.items():
            price = self._get_price(symbol, date, 'close')
            if price > 0:
                total += shares * price
        return total

    def _current_board_values(self, date):
        board_values = {}
        for symbol, shares in self.holdings.items():
            price = self._get_price(symbol, date, 'close')
            if price <= 0:
                continue
            board = self._board(symbol)
            board_values[board] = board_values.get(board, 0.0) + shares * price
        return board_values

    def rebalance(self, date, target_symbols, exec_field='open'):
        current_holdings = list(self.holdings.keys())
        logging.info(f"[{self.name}] Rebalance {date}. Cash before sell: {self.cash:.0f}")
        for symbol in current_holdings:
            if target_symbols and symbol in target_symbols:
                continue
            price = self._get_price(symbol, date, exec_field)
            if price <= 0:
                logging.warning(f"[{self.name}] Cannot sell {symbol} on {date}: Missing Price. Forced Hold.")
                continue
            shares = self.holdings.pop(symbol)
            gross_amount = shares * price
            revenue, commission, stamp_tax, slippage = self._sell_revenue(symbol, gross_amount)
            self.cash += revenue
            self.cost_stats['commission'] += commission
            self.cost_stats['stamp_tax'] += stamp_tax
            self.cost_stats['slippage'] += slippage
            self.trade_log.append({
                'date': date, 'type': 'SELL', 'symbol': symbol,
                'price': price, 'shares': shares, 'amount': revenue,
                'gross_amount': gross_amount, 'commission': commission, 'stamp_tax': stamp_tax, 'slippage': slippage
            })
        if not target_symbols:
            return
        valid_targets = []
        target_prices = {}
        for symbol in target_symbols:
            if symbol in self.holdings:
                continue
            price = self._get_price(symbol, date, exec_field)
            if price > 0:
                valid_targets.append(symbol)
                target_prices[symbol] = price
            else:
                logging.warning(f"[{self.name}] Cannot buy {symbol} on {date}: Missing Price. Skipping.")
        if not valid_targets:
            return
        total_equity = self._estimate_total_value(date)
        max_position_val = total_equity * self.config['max_position_weight']
        max_board_val = total_equity * self.config['max_board_weight']
        board_values = self._current_board_values(date)
        allocation = self.cash / len(valid_targets)
        for symbol in valid_targets:
            board = self._board(symbol)
            board_room = max(0.0, max_board_val - board_values.get(board, 0.0))
            if board_room <= 0:
                self.constraint_stats['board_cap_hit'] += 1
                continue
            alloc_cap = min(allocation, max_position_val, board_room)
            if alloc_cap < allocation:
                self.constraint_stats['position_cap_hit'] += 1
            price = target_prices[symbol]
            max_shares = int(alloc_cap / (price * (1 + self.config['buy_commission'] + self._slippage_rate(symbol))) / self.config['lot_size']) * self.config['lot_size']
            if max_shares <= 0:
                continue
            gross_amount = max_shares * price
            total_cost, commission, slippage = self._buy_cost(symbol, gross_amount)
            if self.cash >= total_cost:
                self.cash -= total_cost
                self.holdings[symbol] = max_shares
                self.last_prices[symbol] = price
                self.cost_stats['commission'] += commission
                self.cost_stats['slippage'] += slippage
                board_values[board] = board_values.get(board, 0.0) + gross_amount
                self.trade_log.append({
                    'date': date, 'type': 'BUY', 'symbol': symbol,
                    'price': price, 'shares': max_shares, 'amount': total_cost,
                    'gross_amount': gross_amount, 'commission': commission, 'stamp_tax': 0.0, 'slippage': slippage
                })

    def update_value(self, date):
        val = self.cash
        for symbol, shares in self.holdings.items():
            price = self._get_price(symbol, date, 'close')
            if price > 0:
                val += shares * price
            else:
                logging.warning(f"No price for {symbol} around {date}.")
        self.history_val.append({'date': date, 'value': val})

    def get_performance(self):
        if not self.history_val:
            return {}
        df = pd.DataFrame(self.history_val)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        df = df[~df.index.duplicated(keep='last')].sort_index()
        start_val = float(df.iloc[0]['value'])
        end_val = float(df.iloc[-1]['value'])
        total_ret = (end_val - start_val) / start_val if start_val > 0 else 0
        roll_max = df['value'].cummax()
        drawdown = (df['value'] - roll_max) / roll_max
        max_dd = float(drawdown.min()) if not drawdown.empty else 0
        df['daily_ret'] = df['value'].pct_change().fillna(0.0)
        mean_ret = float(df['daily_ret'].mean())
        std_ret = float(df['daily_ret'].std())
        sharpe = (mean_ret / std_ret * np.sqrt(252)) if std_ret > 0 else 0
        n = max(len(df), 2)
        annual_ret = (end_val / start_val) ** (252 / (n - 1)) - 1 if start_val > 0 else 0
        annual_vol = std_ret * np.sqrt(252) if std_ret > 0 else 0
        calmar = annual_ret / abs(max_dd) if max_dd < 0 else 0
        return {
            'Total Return': total_ret,
            'Max Drawdown': max_dd,
            'Sharpe Ratio': sharpe,
            'Final Value': end_val,
            'Annual Return': annual_ret,
            'Annual Volatility': annual_vol,
            'Calmar Ratio': calmar,
            'Daily Returns': df['daily_ret']
        }

    def get_cost_summary(self):
        return {
            'commission': float(self.cost_stats['commission']),
            'stamp_tax': float(self.cost_stats['stamp_tax']),
            'slippage': float(self.cost_stats['slippage']),
            'total_cost': float(self.cost_stats['commission'] + self.cost_stats['stamp_tax'] + self.cost_stats['slippage'])
        }

    def get_stock_performance(self):
        stock_pnl = {}
        for trade in self.trade_log:
            symbol = trade['symbol']
            cost = trade['amount']
            if symbol not in stock_pnl:
                stock_pnl[symbol] = {'net_cash': 0, 'buy_amt': 0, 'sell_amt': 0, 'unrealized': 0}
            if trade['type'] == 'BUY':
                stock_pnl[symbol]['buy_amt'] += cost
                stock_pnl[symbol]['net_cash'] -= cost
            elif trade['type'] == 'SELL':
                stock_pnl[symbol]['sell_amt'] += cost
                stock_pnl[symbol]['net_cash'] += cost
        for symbol, shares in self.holdings.items():
            price = self.last_prices.get(symbol, 0)
            if price > 0:
                value = shares * price
                if symbol not in stock_pnl:
                    stock_pnl[symbol] = {'net_cash': 0, 'buy_amt': 0, 'sell_amt': 0, 'unrealized': 0}
                stock_pnl[symbol]['unrealized'] = value
        data = []
        for symbol, metrics in stock_pnl.items():
            total_pnl = metrics['net_cash'] + metrics['unrealized']
            ret = 0
            if metrics['buy_amt'] > 0:
                ret = total_pnl / metrics['buy_amt']
            data.append({
                'Symbol': symbol,
                'Total PnL': total_pnl,
                'Return %': ret,
                'Buy Amount': metrics['buy_amt'],
                'Sell Amount': metrics['sell_amt'],
                'Unrealized Value': metrics['unrealized']
            })
        if not data:
            return pd.DataFrame(columns=['Symbol', 'Total PnL', 'Return %', 'Buy Amount', 'Sell Amount', 'Unrealized Value'])
        return pd.DataFrame(data).sort_values('Total PnL', ascending=False)

def _build_targets_for_signal(signal_date, data_by_date, market, momentum_window=20, momentum_threshold=0.05):
    candidates = data_by_date.get(signal_date, [])
    target_a = [x['symbol'] for x in candidates[:5]]
    smart_candidates = []
    top_pool = candidates[:50]
    for item in top_pool:
        symbol = item['symbol']
        try:
            start_lookback = (datetime.strptime(signal_date, "%Y-%m-%d") - timedelta(days=max(momentum_window * 2, 30))).strftime("%Y-%m-%d")
            hist = market.get_price_history(symbol, start_lookback, signal_date)
            if len(hist) >= momentum_window:
                mom = (hist.iloc[-1]['close'] / hist.iloc[-momentum_window]['close']) - 1
                if mom > momentum_threshold:
                    smart_candidates.append(symbol)
        except Exception:
            pass
    target_b = smart_candidates[:5]
    return target_a, target_b


def _calc_excess_metrics(strategy_series, benchmark_df):
    if strategy_series.empty or benchmark_df.empty:
        return {}
    s = strategy_series.copy()
    b = benchmark_df['close'].copy()
    s.index = pd.to_datetime(s.index)
    b.index = pd.to_datetime(b.index)
    s = s.sort_index()
    b = b.sort_index()
    s_ret = s.pct_change().fillna(0)
    b_ret = b.pct_change().fillna(0)
    aligned = pd.concat([s_ret.rename('s'), b_ret.rename('b')], axis=1, sort=False).dropna()
    if aligned.empty:
        return {}
    excess_daily = aligned['s'] - aligned['b']
    te = float(excess_daily.std() * np.sqrt(252)) if excess_daily.std() > 0 else 0
    ir = float(excess_daily.mean() / excess_daily.std() * np.sqrt(252)) if excess_daily.std() > 0 else 0
    s_total = float((s.iloc[-1] / s.iloc[0]) - 1) if len(s) > 1 else 0
    b_total = float((b.iloc[-1] / b.iloc[0]) - 1) if len(b) > 1 else 0
    return {
        'Strategy Return': s_total,
        'Benchmark Return': b_total,
        'Excess Return': s_total - b_total,
        'Tracking Error': te,
        'Information Ratio': ir
    }


def _classify_regime(benchmark_df):
    if benchmark_df.empty:
        return pd.Series(dtype=str)
    b = benchmark_df['close'].copy().sort_index()
    roll20 = b.pct_change(20)
    regime = pd.Series(index=b.index, dtype=object)
    regime[roll20 > 0.05] = '上涨'
    regime[roll20 < -0.05] = '下跌'
    regime[(roll20 <= 0.05) & (roll20 >= -0.05)] = '震荡'
    regime.fillna('震荡', inplace=True)
    return regime


def _regime_stats(strategy_series, regime_series):
    if strategy_series.empty or regime_series.empty:
        return pd.DataFrame()
    s = strategy_series.copy().sort_index()
    r = regime_series.copy().sort_index()
    s_ret = s.pct_change().fillna(0)
    merged = pd.concat([s_ret.rename('ret'), r.rename('regime')], axis=1, sort=False).dropna()
    rows = []
    for regime_name in ['上涨', '震荡', '下跌']:
        part = merged[merged['regime'] == regime_name]
        if part.empty:
            continue
        cum = float((1 + part['ret']).prod() - 1)
        vol = float(part['ret'].std() * np.sqrt(252)) if part['ret'].std() > 0 else 0
        win = float((part['ret'] > 0).mean())
        curve = (1 + part['ret']).cumprod()
        mdd = float((curve / curve.cummax() - 1).min()) if len(curve) > 1 else 0
        rows.append({
            '阶段': regime_name,
            '样本天数': int(len(part)),
            '阶段收益': cum,
            '阶段波动': vol,
            '阶段胜率': win,
            '阶段最大回撤': mdd
        })
    return pd.DataFrame(rows)


def _stability_test(data_by_date, valid_dates, market):
    thresholds = [0.03, 0.05, 0.08]
    windows = [10, 20]
    records = []
    sample_dates = valid_dates[:60]
    for w in windows:
        for th in thresholds:
            sel_cnt = 0
            ret_vals = []
            for i in range(len(sample_dates) - 1):
                s_date = sample_dates[i]
                x_date = sample_dates[i + 1]
                _, target_b = _build_targets_for_signal(s_date, data_by_date, market, momentum_window=w, momentum_threshold=th)
                if not target_b:
                    continue
                sel_cnt += len(target_b)
                for sym in target_b:
                    p0 = 0
                    p1 = 0
                    df0 = market.get_price_history(sym, s_date, s_date)
                    df1 = market.get_price_history(sym, x_date, x_date)
                    if not df0.empty:
                        p0 = float(df0.iloc[-1]['close'])
                    if not df1.empty:
                        p1 = float(df1.iloc[-1]['close'])
                    if p0 > 0 and p1 > 0:
                        ret_vals.append((p1 / p0) - 1)
            records.append({
                'window': w,
                'threshold': th,
                'signals': int(sel_cnt),
                'avg_next_ret': float(np.mean(ret_vals)) if ret_vals else 0.0
            })
    return pd.DataFrame(records).sort_values(['window', 'threshold'])


def _auto_commentary(res_a, res_b, ex_a, ex_b):
    score_a = 0
    score_b = 0
    if res_a['Total Return'] > res_b['Total Return']:
        score_a += 1
    else:
        score_b += 1
    if res_a['Max Drawdown'] > res_b['Max Drawdown']:
        score_a += 1
    else:
        score_b += 1
    if res_a['Sharpe Ratio'] > res_b['Sharpe Ratio']:
        score_a += 1
    else:
        score_b += 1
    if res_a['Calmar Ratio'] > res_b['Calmar Ratio']:
        score_a += 1
    else:
        score_b += 1
    if ex_a.get('Excess Return', 0) > ex_b.get('Excess Return', 0):
        score_a += 1
    else:
        score_b += 1
    if score_a == score_b:
        winner = "平局（进攻与防守分化）"
    elif score_a > score_b:
        winner = "A (Dumb Money)"
    else:
        winner = "B (Smart Money)"
    if winner == "平局（进攻与防守分化）":
        commentary = "两策略在收益与风控维度分化明显：一方更强调收益扩张，另一方在回撤和风险调整收益更稳。"
    elif winner == "A (Dumb Money)":
        commentary = "A在综合评分上领先，主要优势来自回撤控制与风险调整后收益。"
    else:
        commentary = "B在综合评分上领先，主要优势来自总收益与超额收益能力。"
    return winner, commentary, score_a, score_b


def run_battle():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    output_dir = f"battle_reports/report_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"Report will be saved to: {output_dir}")
    loader = BattleData()
    data_by_date = loader.load_data()
    market = MarketEngine()
    strat_A = StrategyEngine("A (Dumb Money)", BACKTEST_CONFIG['initial_capital'], market, BACKTEST_CONFIG)
    strat_B = StrategyEngine("B (Smart Money)", BACKTEST_CONFIG['initial_capital'], market, BACKTEST_CONFIG)
    logging.info("--- BATTLE START ---")
    dates = sorted(data_by_date.keys())
    valid_dates = [d for d in dates if isinstance(d, str) and d.startswith('20')]
    if len(valid_dates) < 2:
        logging.error("Not enough valid dates for T+1 execution.")
        return
    for i in range(len(valid_dates) - 1):
        signal_date = valid_dates[i]
        exec_date = valid_dates[i + 1]
        logging.info(f"Signal {signal_date} -> Execute {exec_date}")
        target_A, target_B = _build_targets_for_signal(signal_date, data_by_date, market, momentum_window=20, momentum_threshold=0.05)
        strat_A.rebalance(exec_date, target_A, exec_field='open')
        strat_B.rebalance(exec_date, target_B, exec_field='open')
        strat_A.update_value(exec_date)
        strat_B.update_value(exec_date)
    logging.info("--- BATTLE END ---")
    res_A = strat_A.get_performance()
    res_B = strat_B.get_performance()
    if not res_A or not res_B:
        logging.error("Simulation failed to produce results.")
        return
    df_A = pd.DataFrame(strat_A.history_val)
    df_B = pd.DataFrame(strat_B.history_val)
    if df_A.empty or df_B.empty:
        logging.error("No equity curve generated.")
        return
    df_A['date'] = pd.to_datetime(df_A['date'])
    df_B['date'] = pd.to_datetime(df_B['date'])
    df_A.set_index('date', inplace=True)
    df_B.set_index('date', inplace=True)
    start_date = df_A.index.min().strftime("%Y-%m-%d")
    end_date = df_A.index.max().strftime("%Y-%m-%d")
    bm_hs300 = market.get_benchmark_history("HS300", start_date, end_date)
    bm_csi1000 = market.get_benchmark_history("CSI1000", start_date, end_date)
    ex_a_hs300 = _calc_excess_metrics(df_A['value'], bm_hs300)
    ex_b_hs300 = _calc_excess_metrics(df_B['value'], bm_hs300)
    ex_a_csi1000 = _calc_excess_metrics(df_A['value'], bm_csi1000)
    ex_b_csi1000 = _calc_excess_metrics(df_B['value'], bm_csi1000)
    winner, commentary, score_a, score_b = _auto_commentary(res_A, res_B, ex_a_hs300, ex_b_hs300)
    regime = _classify_regime(bm_hs300) if not bm_hs300.empty else pd.Series(dtype=str)
    regime_a = _regime_stats(df_A['value'], regime)
    regime_b = _regime_stats(df_B['value'], regime)
    stability_df = _stability_test(data_by_date, valid_dates, market)
    cost_A = strat_A.get_cost_summary()
    cost_B = strat_B.get_cost_summary()
    plt.figure(figsize=(12, 6))
    plt.plot(df_A.index, df_A['value'], label=f"Dumb Money ({res_A['Total Return']*100:.1f}%)", color='red')
    plt.plot(df_B.index, df_B['value'], label=f"Smart Money ({res_B['Total Return']*100:.1f}%)", color='blue')
    if not bm_hs300.empty:
        bm_norm = bm_hs300['close'] / bm_hs300['close'].iloc[0] * BACKTEST_CONFIG['initial_capital']
        plt.plot(bm_norm.index, bm_norm, label='HS300 Benchmark', color='gray', linestyle='--')
    plt.title("Strategy Battle Professional Report")
    plt.xlabel("Date")
    plt.ylabel("Portfolio Value (CNY)")
    plt.legend()
    plt.grid(True)
    plot_path = os.path.join(output_dir, "battle_results.png")
    plt.savefig(plot_path)
    trades_A = pd.DataFrame(strat_A.trade_log)
    trades_B = pd.DataFrame(strat_B.trade_log)
    if not trades_A.empty:
        trades_A['Strategy'] = 'A (Dumb Money)'
    if not trades_B.empty:
        trades_B['Strategy'] = 'B (Smart Money)'
    all_trades = pd.concat([trades_A, trades_B]) if (not trades_A.empty or not trades_B.empty) else pd.DataFrame()
    if not all_trades.empty:
        all_trades.sort_values('date', inplace=True)
        all_trades.to_csv(os.path.join(output_dir, "battle_trades_all.csv"), index=False)
        trades_A.to_csv(os.path.join(output_dir, "battle_trades_A.csv"), index=False)
        trades_B.to_csv(os.path.join(output_dir, "battle_trades_B.csv"), index=False)
    pnl_A = strat_A.get_stock_performance()
    pnl_B = strat_B.get_stock_performance()
    fig, axes = plt.subplots(1, 2, figsize=(16, 8))
    for dfp, name, ax in [(pnl_A, "Dumb Money", axes[0]), (pnl_B, "Smart Money", axes[1])]:
        if dfp.empty:
            continue
        combined = pd.concat([dfp.head(5), dfp.tail(5)])
        colors = ['green' if x >= 0 else 'red' for x in combined['Total PnL']]
        ax.barh(combined['Symbol'], combined['Total PnL'], color=colors)
        ax.set_title(name)
        ax.set_xlabel("Total PnL (CNY)")
        ax.invert_yaxis()
    plt.tight_layout()
    pnl_plot_path = os.path.join(output_dir, "battle_stock_pnl.png")
    plt.savefig(pnl_plot_path)
    report_path = os.path.join(output_dir, "battle_report.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(f"# 📈 A-Share Strategy Battle Pro Report\n\n")
        f.write(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M')}  \n")
        f.write(f"**Execution Mode:** T+1 信号成交（Signal day -> Next trade day）\n\n")
        f.write("## 📊 综合绩效\n\n")
        f.write("| Metric | A (Dumb Money) | B (Smart Money) |\n")
        f.write("| :--- | :--- | :--- |\n")
        f.write(f"| Total Return | {res_A['Total Return']*100:.2f}% | {res_B['Total Return']*100:.2f}% |\n")
        f.write(f"| Annual Return | {res_A['Annual Return']*100:.2f}% | {res_B['Annual Return']*100:.2f}% |\n")
        f.write(f"| Annual Volatility | {res_A['Annual Volatility']*100:.2f}% | {res_B['Annual Volatility']*100:.2f}% |\n")
        f.write(f"| Max Drawdown | {res_A['Max Drawdown']*100:.2f}% | {res_B['Max Drawdown']*100:.2f}% |\n")
        f.write(f"| Sharpe Ratio | {res_A['Sharpe Ratio']:.2f} | {res_B['Sharpe Ratio']:.2f} |\n")
        f.write(f"| Calmar Ratio | {res_A['Calmar Ratio']:.2f} | {res_B['Calmar Ratio']:.2f} |\n")
        f.write(f"| Final Capital | ¥{res_A['Final Value']:.0f} | ¥{res_B['Final Value']:.0f} |\n\n")
        f.write("## 📈 基准与超额（HS300）\n\n")
        if ex_a_hs300 and ex_b_hs300:
            f.write("| Metric | A | B |\n")
            f.write("| :--- | :--- | :--- |\n")
            f.write(f"| Benchmark Return | {ex_a_hs300['Benchmark Return']*100:.2f}% | {ex_b_hs300['Benchmark Return']*100:.2f}% |\n")
            f.write(f"| Excess Return | {ex_a_hs300['Excess Return']*100:.2f}% | {ex_b_hs300['Excess Return']*100:.2f}% |\n")
            f.write(f"| Tracking Error | {ex_a_hs300['Tracking Error']*100:.2f}% | {ex_b_hs300['Tracking Error']*100:.2f}% |\n")
            f.write(f"| Information Ratio | {ex_a_hs300['Information Ratio']:.2f} | {ex_b_hs300['Information Ratio']:.2f} |\n\n")
        else:
            f.write("基准数据不足，无法计算超额指标。\n\n")
        f.write("## 📉 成本分解\n\n")
        f.write("| Cost Item | A | B |\n")
        f.write("| :--- | :--- | :--- |\n")
        f.write(f"| Commission | ¥{cost_A['commission']:.2f} | ¥{cost_B['commission']:.2f} |\n")
        f.write(f"| Stamp Tax | ¥{cost_A['stamp_tax']:.2f} | ¥{cost_B['stamp_tax']:.2f} |\n")
        f.write(f"| Slippage | ¥{cost_A['slippage']:.2f} | ¥{cost_B['slippage']:.2f} |\n")
        f.write(f"| Total Cost | ¥{cost_A['total_cost']:.2f} | ¥{cost_B['total_cost']:.2f} |\n\n")
        f.write("## 🧩 分市场阶段表现（基于HS300）\n\n")
        if not regime_a.empty:
            f.write("### A 策略\n")
            f.write(regime_a.to_markdown(index=False, floatfmt=".4f"))
            f.write("\n\n")
        if not regime_b.empty:
            f.write("### B 策略\n")
            f.write(regime_b.to_markdown(index=False, floatfmt=".4f"))
            f.write("\n\n")
        f.write("## 🔧 参数稳定性测试（样本前60信号日）\n\n")
        if not stability_df.empty:
            f.write(stability_df.to_markdown(index=False, floatfmt=".4f"))
            f.write("\n\n")
        f.write("## 🏆 自动结论\n\n")
        f.write(f"- Winner: **{winner}**  \n")
        f.write(f"- Score(A/B): **{score_a}/{score_b}**  \n")
        f.write(f"- Commentary: {commentary}\n\n")
        f.write("## 🌟 Star Performers\n\n")
        f.write("![Stock PnL](battle_stock_pnl.png)\n\n")
        if not pnl_A.empty:
            f.write("### A Top Winners\n")
            f.write(pnl_A.head(5)[['Symbol', 'Total PnL', 'Return %', 'Buy Amount']].to_markdown(index=False, floatfmt=".2f"))
            f.write("\n\n")
        if not pnl_B.empty:
            f.write("### B Top Winners\n")
            f.write(pnl_B.head(5)[['Symbol', 'Total PnL', 'Return %', 'Buy Amount']].to_markdown(index=False, floatfmt=".2f"))
            f.write("\n\n")
        f.write("## 🛡️ 风险约束与数据质量\n\n")
        f.write(f"- A Position Cap Hits: {strat_A.constraint_stats['position_cap_hit']}  \n")
        f.write(f"- A Board Cap Hits: {strat_A.constraint_stats['board_cap_hit']}  \n")
        f.write(f"- B Position Cap Hits: {strat_B.constraint_stats['position_cap_hit']}  \n")
        f.write(f"- B Board Cap Hits: {strat_B.constraint_stats['board_cap_hit']}  \n")
        f.write(f"- Invalid Assets Filtered: {loader.data_quality.get('invalid_assets_filtered', 0)}  \n")
        f.write(f"- ST Filtered: {loader.data_quality.get('st_filtered', 0)}  \n")
        f.write(f"- Blacklisted Symbols: {len(list(market.blacklist))}  \n\n")
        f.write("## 📌 ST / ETF / 基金口径说明\n\n")
        f.write("- 主策略默认过滤ST，避免可执行性与极端风险偏差。  \n")
        f.write("- 基金/ETF可能优于股票，但应作为独立策略组回测并单独排名。  \n")
        f.write("- 本报告股票主策略仅使用A股股票池。\n\n")
        f.write("## 📝 交易明细\n\n")
        f.write("- [All Trades](battle_trades_all.csv)\n")
        f.write("- [Dumb Money Trades](battle_trades_A.csv)\n")
        f.write("- [Smart Money Trades](battle_trades_B.csv)\n")
    logging.info(f"Report generated: {report_path}")

if __name__ == "__main__":
    run_battle()
