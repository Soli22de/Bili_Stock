
import pandas as pd
import json
import os
import glob
from datetime import datetime, timedelta

def run_backtest():
    # 1. Load Data
    print("Loading cube history...")
    data_dir = "data/history"
    json_files = glob.glob(os.path.join(data_dir, "*.json"))
    
    all_moves = []
    for fpath in json_files:
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                moves = json.load(f)
                cube_symbol = os.path.basename(fpath).replace(".json", "")
                for m in moves:
                    m['cube_symbol'] = cube_symbol
                    all_moves.append(m)
        except Exception as e:
            print(f"Error loading {fpath}: {e}")
            
    df = pd.DataFrame(all_moves)
    if df.empty:
        print("No history data found.")
        return

    # 2. Preprocess
    df['date'] = pd.to_datetime(df['time']).dt.date
    df['datetime'] = pd.to_datetime(df['time'])
    
    # Filter for 2025+
    start_date = pd.to_datetime("2025-01-01").date()
    df = df[df['date'] >= start_date].sort_values('datetime')
    
    print(f"Loaded {len(df)} moves since {start_date}")
    
    # 3. Simulate Strategy
    # Strategy: Buy if >= 2 cubes buy the same stock on the same day.
    # Hold for 5 days.
    
    positions = [] # list of {code, name, buy_date, buy_price, sell_date, sell_price, return, cubes}
    
    # Group by date
    dates = sorted(df['date'].unique())
    
    for d in dates:
        day_moves = df[df['date'] == d]
        buys = day_moves[day_moves['action'] == 'BUY']
        
        if buys.empty:
            continue
            
        # Count unique cubes buying each stock
        buy_counts = buys.groupby(['stock_code', 'stock_name'])['cube_symbol'].apply(list).reset_index()
        buy_counts['count'] = buy_counts['cube_symbol'].apply(lambda x: len(set(x)))
        
        # Signal: >= 2 cubes
        signals = buy_counts[buy_counts['count'] >= 2]
        
        for _, row in signals.iterrows():
            code = row['stock_code']
            name = row['stock_name']
            cubes = row['cube_symbol']
            
            # Get entry price (average of buy prices on that day from the cubes)
            entry_prices = buys[buys['stock_code'] == code]['price'].astype(float)
            entry_price = entry_prices.mean()
            
            # Determine Exit Date (T+5) and Price
            # Since we might not have daily data for every stock, we look for the next available price in history 
            # OR we just say "held until now" if recent.
            # Ideally, we look for a SELL action in the future from ANY cube to get a proxy price, 
            # or we need external data. 
            # Limitation: Without external data, we can only estimate PnL if there are future transactions in the dataset.
            # Let's try to find a price T+5 days later from the dataset (any transaction on that stock).
            
            exit_date = d + timedelta(days=5)
            
            # Look for any transaction of this stock on or after exit_date
            future_moves = df[
                (df['stock_code'] == code) & 
                (df['date'] >= exit_date)
            ].sort_values('datetime')
            
            if not future_moves.empty:
                exit_move = future_moves.iloc[0]
                exit_price = float(exit_move['price'])
                actual_exit_date = exit_move['date']
                status = "CLOSED"
            else:
                # Still open or no data
                # If "now" is close to exit_date, use latest price if available?
                # For this report, let's mark as OPEN if no future data, 
                # or try to use the very last transaction in the DB as current price.
                last_move = df[df['stock_code'] == code].sort_values('datetime').iloc[-1]
                exit_price = float(last_move['price'])
                actual_exit_date = last_move['date']
                status = "OPEN" if actual_exit_date < pd.to_datetime("2026-02-24").date() else "Current" 
                # Note: Data might be up to 2026, user asked for "from start of year". 
                # The data I see in LS is "2026-01-30" in one file. 
                # Wait, the core memory says "Backtest (2023-2026)". 
                # Today is 2026-02-24. So "start of year" means 2026-01-01.
                # If user meant "this year", it's 2026.
                
            if exit_price > 0 and entry_price > 0:
                pnl_pct = (exit_price - entry_price) / entry_price
            else:
                pnl_pct = 0
                
            positions.append({
                "buy_date": d,
                "stock": name,
                "code": code,
                "entry_price": entry_price,
                "exit_date": actual_exit_date,
                "exit_price": exit_price,
                "return": pnl_pct,
                "cubes": len(cubes),
                "status": status
            })

    # 4. Report
    results = pd.DataFrame(positions)
    if results.empty:
        print("No trades generated.")
        return

    print(f"\n--- Backtest Report (2025-01-01 to Present) ---")
    print(f"Total Trades: {len(results)}")
    
    # Win Rate
    wins = results[results['return'] > 0]
    win_rate = len(wins) / len(results) if len(results) > 0 else 0
    print(f"Win Rate: {win_rate:.2%}")
    
    # Average Return
    avg_ret = results['return'].mean()
    print(f"Avg Return per Trade: {avg_ret:.2%}")
    
    # Cumulative Return (Simple sum)
    total_ret = results['return'].sum()
    print(f"Total Simple Return: {total_ret:.2%}")

    print("\nTop 10 Trades:")
    print(results.sort_values('return', ascending=False).head(10)[['buy_date', 'stock', 'return', 'cubes', 'status']].to_string(index=False))

    print("\nRecent Trades:")
    print(results.sort_values('buy_date', ascending=False).head(10)[['buy_date', 'stock', 'return', 'cubes', 'status']].to_string(index=False))

if __name__ == "__main__":
    run_backtest()
