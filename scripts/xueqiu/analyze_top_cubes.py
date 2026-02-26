import pandas as pd
import json
import os
import numpy as np
from datetime import datetime

def analyze_cube(symbol, name):
    print(f"\nAnalyzing {name} ({symbol})...")
    
    # Load signals
    path = f"data/history/{symbol}.json"
    if not os.path.exists(path):
        print("No history found.")
        return

    with open(path, "r", encoding="utf-8") as f:
        signals = json.load(f)
    
    # Convert to DF
    df = pd.DataFrame(signals)
    df['time'] = pd.to_datetime(df['time'])
    df = df.sort_values('time')  # Ensure chronological order
    df = df[df['time'] >= '2022-01-01']
    
    if df.empty:
        print("No signals since 2022.")
        return

    # 1. Trading Frequency
    total_days = (df['time'].max() - df['time'].min()).days
    trades_count = len(df)
    avg_trades_per_month = trades_count / (total_days / 30) if total_days > 0 else 0
    
    print(f"Total Trades: {trades_count}")
    print(f"Avg Trades/Month: {avg_trades_per_month:.1f}")

    # 2. Holding Period (Approximate)
    # Track open positions: {stock: buy_date}
    holdings = {}
    holding_periods = []
    
    # 3. Buy Timing (Red/Green)
    buy_on_red = 0
    buy_on_green = 0
    total_buys = 0
    
    # 4. Top Stocks
    stock_counts = df['stock_name'].value_counts().head(5)
    
    for _, row in df.iterrows():
        stock = row['stock_code']
        action = row['action']
        date = row['time']
        
        if action == 'BUY':
            if stock not in holdings:
                holdings[stock] = date
            
            # Check if bought on red/green (need daily data)
            # We'll just look at the price change in the signal if available, 
            # or skip for now as signal doesn't have daily change.
            total_buys += 1
            
        elif action == 'SELL':
            if stock in holdings:
                open_date = holdings.pop(stock)
                days = (date - open_date).days
                holding_periods.append(days)

    avg_holding_days = np.mean(holding_periods) if holding_periods else 0
    median_holding_days = np.median(holding_periods) if holding_periods else 0
    
    print(f"Avg Holding Days: {avg_holding_days:.1f}")
    print(f"Median Holding Days: {median_holding_days:.1f}")
    
    print("Top Traded Stocks:")
    print(stock_counts.to_string())
    
    # 5. Sector/Theme Inference (Simple string match)
    themes = {
        "Tech": ["科技", "电子", "半导体", "信息", "软件", "通信", "计算机", "AI", "智能"],
        "NewEnergy": ["新能源", "电池", "光伏", "锂", "车", "电", "能"],
        "Consumption": ["酒", "药", "医", "食", "饮", "消费", "乳"],
        "Finance": ["银行", "证券", "保险", "金"],
        "Cyclical": ["煤", "油", "矿", "钢", "铜", "铝", "运"]
    }
    
    theme_counts = {k: 0 for k in themes}
    
    unique_stocks = df[['stock_code', 'stock_name']].drop_duplicates()
    
    for _, row in unique_stocks.iterrows():
        stock_n = row['stock_name']
        if not isinstance(stock_n, str): continue
        
        for theme, keywords in themes.items():
            for kw in keywords:
                if kw in stock_n:
                    theme_counts[theme] += 1
                    break
    
    print("Sector Preference (Stock Count):")
    print(pd.Series(theme_counts).sort_values(ascending=False).to_string())
    
    return {
        "symbol": symbol,
        "name": name,
        "trades_per_month": avg_trades_per_month,
        "avg_holding_days": avg_holding_days,
        "top_theme": max(theme_counts, key=theme_counts.get)
    }

def run():
    # Top 3 from previous analysis
    targets = [
        ("ZH583267", "南极风暴"), 
        ("ZH1745648", "价值元年"), 
        ("ZH2278787", "北斗七K")
    ]
    
    summary = []
    for t in targets:
        res = analyze_cube(t[0], t[1])
        if res: summary.append(res)
        
    print("\nSummary Comparison:")
    print(pd.DataFrame(summary).to_string())

if __name__ == "__main__":
    run()
