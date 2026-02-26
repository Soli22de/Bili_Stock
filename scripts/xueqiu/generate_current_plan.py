import json
import os
import pandas as pd
from datetime import datetime

def get_latest_holdings(symbol, name):
    path = f"data/history/{symbol}.json"
    if not os.path.exists(path):
        return {}
    
    with open(path, "r", encoding="utf-8") as f:
        signals = json.load(f)
        
    # Sort by time
    signals.sort(key=lambda x: x["time"])
    
    # Reconstruct portfolio
    # stock_code -> {name, weight, time}
    portfolio = {}
    
    for s in signals:
        code = s["stock_code"]
        name = s["stock_name"]
        weight = s.get("target_weight", 0)
        
        # Some signals might not have weight, use simple logic if missing
        # But our fetcher gets target_weight.
        # DEBUG: Check if target_weight exists in keys
        # The file content shows: time, timestamp, cube_symbol, stock_code, stock_name, action, delta, price, comment.
        # It DOES NOT seem to have target_weight in the JSON snippet shown.
        # This means fetch_cube_history.py might not have saved it, or it was None.
        # Let's use 'delta' to reconstruct weights if target_weight is missing.
        # But delta is change. We need cumulative sum.
        
        weight = s.get("target_weight")
        delta = s.get("delta", 0)
        
        if weight is not None:
            if weight > 0:
                portfolio[code] = {"name": name, "weight": weight, "last_update": s["time"]}
            elif weight == 0:
                if code in portfolio: del portfolio[code]
        else:
            # Reconstruct from delta
            current_w = portfolio.get(code, {}).get("weight", 0)
            new_w = current_w + delta
            if new_w < 0.1: # Threshold for 0
                if code in portfolio: del portfolio[code]
            else:
                portfolio[code] = {"name": name, "weight": new_w, "last_update": s["time"]}
                
    return portfolio

def run():
    # Load valuable cubes
    with open("data/valuable_cubes.json", "r", encoding="utf-8") as f:
        cubes = json.load(f)
        
    # Focus on Top 5 (Alpha tier)
    top_cubes = cubes[:5]
    
    print(f"Generating Investment Plan for {datetime.now().strftime('%Y-%m-%d')} based on Top 5 Alpha Cubes...\n")
    
    all_holdings = []
    
    for cube in top_cubes:
        symbol = cube["symbol"]
        cname = cube["name"]
        
        holdings = get_latest_holdings(symbol, cname)
        
        # Debug: Print raw holdings count
        # print(f"DEBUG: {cname} has {len(holdings)} holdings.")
        
        if not holdings:
            print(f"[{cname}] Empty or No Data (might be empty position).")
            # continue # Don't skip, just show empty
        
        print(f"--- {cname} (Return: {cube['return']:.1f}%) ---")
        if not holdings:
             print("  (Empty Position / Cash)")
        
        sorted_h = sorted(holdings.items(), key=lambda x: x[1]['weight'], reverse=True)
        
        for code, info in sorted_h:
            # Filter out tiny residual weights
            # if info['weight'] < 1: continue 
            
            print(f"  {info['name']} ({code}): {info['weight']:.1f}%")
            
            all_holdings.append({
                "cube": cname,
                "stock_name": info['name'],
                "stock_code": code,
                "weight": info['weight']
            })
        print("")

    # Consensus Analysis
    df = pd.DataFrame(all_holdings)
    if df.empty:
        print("No current holdings found.")
        return

    print("=== Consensus / High Conviction Holdings ===")
    # Group by stock
    summary = df.groupby(['stock_name', 'stock_code']).agg({
        'cube': lambda x: list(x),
        'weight': 'mean'
    }).reset_index()
    
    # Sort by number of cubes holding it, then by avg weight
    summary['count'] = summary['cube'].apply(len)
    summary = summary.sort_values(['count', 'weight'], ascending=[False, True]) # Actually higher weight is better? No, count is key.
    
    # Just print all
    for _, row in summary.iterrows():
        print(f"Stock: {row['stock_name']} ({row['stock_code']})")
        print(f"  Held by {row['count']} Gurus: {', '.join(row['cube'])}")
        print(f"  Avg Weight: {row['weight']:.1f}%")
        print("-" * 20)

if __name__ == "__main__":
    run()
