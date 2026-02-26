
import json
import os
import pandas as pd
from datetime import datetime
import numpy as np

def analyze_2025_performance():
    history_dir = "data/history"
    if not os.path.exists(history_dir):
        print("No history data found.")
        return

    results = []
    
    for filename in os.listdir(history_dir):
        if not filename.endswith(".json"):
            continue
            
        symbol = filename.replace(".json", "")
        filepath = os.path.join(history_dir, filename)
        
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                
            if not data:
                continue
                
            df = pd.DataFrame(data)
            
            # Standardize date
            if 'timestamp' in df.columns:
                df['date'] = pd.to_datetime(df['timestamp'], unit='ms')
            elif 'time' in df.columns:
                df['date'] = pd.to_datetime(df['time'])
            else:
                continue
                
            # Filter 2025
            df_2025 = df[df['date'] >= '2025-01-01'].copy()
            if df_2025.empty:
                continue
                
            # Calculate metrics
            # 1. Trade count
            trades = df_2025[df_2025['action'].isin(['BUY', 'SELL'])]
            trade_count = len(trades)
            
            # 2. Win rate (Approximation: if price moves up after buy?)
            # Since we don't have exit price easily paired, let's look at "Success Rate" of calls.
            # Or just count profitable simulated trades if we had price data.
            # But history only has 'price' at time of action.
            # We can't know if they sold at profit without pairing.
            
            # Let's try to pair BUYs and SELLs FIFO
            # Simple simulation
            pnl = 0
            wins = 0
            losses = 0
            holdings = {} # stock -> list of [price, qty]
            
            # Sort by date asc
            df_2025 = df_2025.sort_values(by='date')
            
            for _, row in df_2025.iterrows():
                stock = row['stock_name']
                action = row['action']
                price = row.get('price', 0)
                if not price: continue
                
                # Delta is weight change. We don't know qty.
                # Assume 10000 capital per trade unit?
                # Or just use price diff percentage.
                
                if action == 'BUY':
                    if stock not in holdings:
                        holdings[stock] = []
                    holdings[stock].append(price)
                elif action == 'SELL':
                    if stock in holdings and holdings[stock]:
                        buy_price = holdings[stock].pop(0)
                        diff = (price - buy_price) / buy_price
                        if diff > 0:
                            wins += 1
                        else:
                            losses += 1
                        pnl += diff
            
            total_closed = wins + losses
            win_rate = (wins / total_closed) * 100 if total_closed > 0 else 0
            
            results.append({
                "symbol": symbol,
                "cube_name": df['cube_symbol'].iloc[0] if 'cube_symbol' in df.columns else symbol, # Name might be missing
                "trade_count": trade_count,
                "closed_trades": total_closed,
                "win_rate": round(win_rate, 2),
                "simulated_return_pct": round(pnl * 100, 2)
            })
            
        except Exception as e:
            # print(f"Error processing {filename}: {e}")
            pass
            
    # Convert to DF
    res_df = pd.DataFrame(results)
    if res_df.empty:
        print("No results.")
        return

    # Filter for meaningful stats
    res_df = res_df[res_df['closed_trades'] >= 5]
    
    # Sort by Win Rate
    top_win = res_df.sort_values(by='win_rate', ascending=False).head(20)
    
    # Sort by Return
    top_return = res_df.sort_values(by='simulated_return_pct', ascending=False).head(20)
    
    print("\n=== Top 20 Cubes by Win Rate (2025, min 5 trades) ===")
    print(top_win[['symbol', 'win_rate', 'closed_trades', 'simulated_return_pct']].to_string(index=False))
    
    print("\n=== Top 20 Cubes by Simulated Return (2025) ===")
    print(top_return[['symbol', 'simulated_return_pct', 'win_rate', 'closed_trades']].to_string(index=False))
    
    # Save
    res_df.to_csv("data/cube_performance_2025.csv", index=False, encoding="utf-8-sig")
    print("\nSaved full report to data/cube_performance_2025.csv")

if __name__ == "__main__":
    analyze_2025_performance()
