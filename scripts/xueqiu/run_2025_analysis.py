import json
import os
import sys
import pandas as pd
from datetime import datetime, timedelta
import logging
import time
import random

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from scripts.xueqiu.fetch_cube_history import XueqiuHistoryFetcher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

class YearToDateAnalyzer:
    def __init__(self):
        self.fetcher = XueqiuHistoryFetcher()
        self.data_dir = "data/history_2025"
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            
        self.start_date = datetime(2025, 1, 1)
        self.fetcher.min_timestamp = self.start_date.timestamp() * 1000
        
    def load_cubes(self):
        cubes_file = "data/massive_cube_list.json"
        if not os.path.exists(cubes_file):
            cubes_file = "data/long_history_cubes.json"
            
        if not os.path.exists(cubes_file):
            logging.error("No cube list found.")
            return []
            
        with open(cubes_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def fetch_all_history(self):
        cubes = self.load_cubes()
        logging.info(f"Fetching 2025 history for {len(cubes)} cubes...")
        
        all_signals = []
        
        for i, cube in enumerate(cubes):
            symbol = cube["symbol"]
            name = cube["name"]
            
            # Check if cache exists
            cache_file = os.path.join(self.data_dir, f"{symbol}_2025.csv")
            if os.path.exists(cache_file):
                df = pd.read_csv(cache_file)
                signals = df.to_dict('records')
                logging.info(f"Loaded {len(signals)} signals from cache for {name}")
            else:
                signals = self.fetcher.fetch_history(symbol)
                # Save cache
                if signals:
                    df = pd.DataFrame(signals)
                    df.to_csv(cache_file, index=False, encoding="utf-8-sig")
                time.sleep(random.uniform(1.0, 2.0))
                
            for s in signals:
                s['cube_name'] = name
                s['cube_symbol'] = symbol
                
            all_signals.extend(signals)
            
        return all_signals

    def run_backtest(self, signals):
        if not signals:
            logging.warning("No signals to backtest.")
            return

        df = pd.DataFrame(signals)
        
        # Ensure 'date' column exists (YYYY-MM-DD)
        if 'timestamp' in df.columns:
            df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.strftime('%Y-%m-%d')
        elif 'time' in df.columns:
             df['date'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%d')
        else:
            logging.error(f"Cannot find timestamp or time column. Columns: {df.columns}")
            return
             
        # Filter for 2025
        df = df[df['date'] >= '2025-01-01']
        df = df.sort_values(by='date')
        
        logging.info(f"Backtesting on {len(df)} signals from 2025-01-01 to now...")
        
        # Strategy D: Resonance
        # Logic: If >= 2 cubes buy the same stock within 5 days -> BUY
        # Exit: Hold for 10 days or if >= 2 cubes sell
        
        positions = {} # stock -> {entry_date, entry_price, quantity, cubes}
        trades = []
        
        # Mock price (since we don't have historical price data easily available here without fetching)
        # We will assume entry at signal price (if available) or just track signal accuracy
        # For this report, let's focus on SIGNAL QUALITY (Win Rate)
        # We can try to fetch current price to see if it's up or down?
        # Or just list the signals.
        
        # Let's try to simulate simplified PnL if we have price
        # fetch_cube_history.py _parse_move might not extract price.
        # Let's check fetch_cube_history.py again.
        
        # Actually, for the user report "What it looks like", a list of "Consensus Buys" and their dates is a good start.
        
        # Group by (stock, date) to find resonance
        # Rolling window of 5 days is tricky in pandas without daily resampling.
        
        # Let's iterate days
        dates = sorted(df['date'].unique())
        
        resonance_signals = []
        
        for i, date in enumerate(dates):
            # Get signals in window [date-4, date]
            # Actually, "within 5 days" means if Cube A buys on Day 1 and Cube B buys on Day 3, on Day 3 we have consensus.
            
            window_start = (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=5)).strftime('%Y-%m-%d')
            window_signals = df[(df['date'] >= window_start) & (df['date'] <= date) & (df['action'] == 'BUY')]
            
            # Count unique cubes per stock
            stock_counts = window_signals.groupby('stock_name')['cube_symbol'].nunique()
            
            # Stocks with >= 2 cubes
            candidates = stock_counts[stock_counts >= 2].index.tolist()
            
            for stock in candidates:
                # Check if we already triggered this resonance recently to avoid duplicates
                # Simple logic: only trigger if today contributes to the count (i.e., at least one buy today)
                today_buys = df[(df['date'] == date) & (df['stock_name'] == stock) & (df['action'] == 'BUY')]
                if not today_buys.empty:
                    # It's a new trigger or reinforcement
                    cubes = window_signals[window_signals['stock_name'] == stock]['cube_name'].unique().tolist()
                    resonance_signals.append({
                        "date": date,
                        "stock": stock,
                        "cubes": ", ".join(cubes),
                        "count": len(cubes)
                    })

        # Save resonance signals
        res_df = pd.DataFrame(resonance_signals)
        if not res_df.empty:
            res_df = res_df.drop_duplicates(subset=['date', 'stock'])
            res_df.to_csv("data/2025_resonance_signals.csv", index=False, encoding="utf-8-sig")
            
            print("\n=== 2025 YTD Resonance Strategy Report ===")
            print(f"Total Resonance Signals: {len(res_df)}")
            print("\nRecent Signals (Last 10):")
            print(res_df.tail(10).to_string(index=False))
            
            # Simple Stat
            print(f"\nMost Popular Stocks 2025:")
            print(res_df['stock'].value_counts().head(10))
            
        else:
            print("No resonance signals found in 2025.")

if __name__ == "__main__":
    analyzer = YearToDateAnalyzer()
    signals = analyzer.fetch_all_history()
    analyzer.run_backtest(signals)
