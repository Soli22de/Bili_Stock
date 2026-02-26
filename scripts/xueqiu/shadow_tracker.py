import json
import os
import pandas as pd
from datetime import datetime, timedelta

def load_history():
    data_dir = "data/history"
    all_signals = []
    
    if not os.path.exists(data_dir):
        print("No history data found.")
        return []
        
    for f in os.listdir(data_dir):
        if f.endswith(".json"):
            symbol = f.replace(".json", "")
            with open(os.path.join(data_dir, f), "r", encoding="utf-8") as file:
                signals = json.load(file)
                for s in signals:
                    s['cube_symbol'] = symbol
                    all_signals.append(s)
                    
    return all_signals

def run_shadow_tracker():
    print("=== Shadow Tracker: Marginal Change Analysis ===\n")
    
    signals = load_history()
    if not signals: return

    df = pd.DataFrame(signals)
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values('time')
    
    # Filter for recent data (e.g., last 30 days)
    # Since we are in 2026-02-17, let's look at 2026-01-17 onwards
    # But for demo, let's look at the very last 2 weeks of data available in the dataset
    last_date = df['time'].max()
    start_date = last_date - timedelta(days=14)
    
    print(f"Analysis Window: {start_date.date()} to {last_date.date()}")
    
    df_recent = df[df['time'] >= start_date].copy()
    
    if df_recent.empty:
        print("No recent signals found.")
        return

    # 1. Rapid Addition (Same Cube, Same Stock, Multiple Buys in 3 Days)
    print("\n[1. Rapid Accumulation Alert]")
    
    # Group by Cube, Stock
    grouped = df_recent[df_recent['action'] == 'BUY'].groupby(['cube_symbol', 'stock_name'])
    
    for (cube, stock), group in grouped:
        if len(group) < 2: continue
        
        # Check time difference
        group = group.sort_values('time')
        times = group['time'].tolist()
        
        # Check if any 2 buys are within 3 days
        rapid = False
        for i in range(len(times)-1):
            if (times[i+1] - times[i]).days <= 3:
                rapid = True
                break
        
        if rapid:
            total_delta = group['delta'].sum()
            print(f"  ★ {cube} rapidly accumulated {stock}: {len(group)} buys, +{total_delta:.1f}%")

    # 2. Sector/Stock Resonance (Multiple Cubes Buying Same Stock in 5 Days)
    print("\n[2. Multi-Cube Resonance Alert]")
    
    # Sliding window of 5 days is hard to check efficiently.
    # Simplified: Check stocks bought by >= 2 distinct cubes in the last 7 days.
    
    df_very_recent = df[df['time'] >= (last_date - timedelta(days=7))]
    buys = df_very_recent[df_very_recent['action'] == 'BUY']
    
    stock_buyers = buys.groupby('stock_name')['cube_symbol'].nunique()
    resonance_stocks = stock_buyers[stock_buyers >= 2]
    
    if resonance_stocks.empty:
        print("  No multi-cube resonance found in last 7 days.")
    else:
        for stock, count in resonance_stocks.items():
            buyers = buys[buys['stock_name'] == stock]['cube_symbol'].unique()
            print(f"  ★ {stock} bought by {count} cubes: {', '.join(buyers)}")
            
    # 3. Profit Taking (Selling Top Holdings)
    print("\n[3. Profit Taking / Exit Alert]")
    
    # Identify sells in recent window
    sells = df_recent[df_recent['action'] == 'SELL']
    
    # Logic: Consecutive sells or large sell (>5%)
    for _, row in sells.iterrows():
        if row['delta'] <= -5:
            print(f"  ⚠ {row['cube_symbol']} dumped {row['stock_name']} ({row['delta']}%) on {row['time'].date()}")
        # Check consecutive sells?
        # (Simplified for now)

    print("\n=== End of Report ===")

if __name__ == "__main__":
    run_shadow_tracker()
