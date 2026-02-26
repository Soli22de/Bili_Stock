import pandas as pd
import json
import os
import datetime
import sys

# Add parent directory to path to import config if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import config

def filter_active_ups():
    # 1. Read dataset_videos.csv
    csv_path = "data/dataset_videos.csv"
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} not found.")
        return

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return

    # 2. Filter for today's videos
    today = datetime.datetime.now().strftime('%Y-%m-%d')
    print(f"Filtering active UPs for today: {today}")
    
    # Ensure publish_time is string
    df['publish_time'] = df['publish_time'].astype(str)
    
    today_df = df[df['publish_time'].str.startswith(today)].copy()
    
    if today_df.empty:
        print("No videos found for today.")
        return

    print(f"Found {len(today_df)} videos from today.")

    # 3. Filter by stock-related keywords
    stock_keywords = [
        "股票", "A股", "大盘", "行情", "板块", "涨停", "跌停", "复盘", "实盘", "交割单", 
        "买入", "卖出", "持仓", "代码", "龙头", "打板", "低吸", "接力", "主升", "趋势",
        "上证", "深证", "创业板", "ETF", "量能", "缩量", "放量"
    ]
    
    active_ups = {} # uid -> name

    for _, row in today_df.iterrows():
        content = str(row.get('title', '')) + " " + str(row.get('content', ''))
        author_name = str(row.get('author_name', ''))
        author_id = str(row.get('author_id', ''))
        
        # Check if content matches keywords
        if any(k in content for k in stock_keywords):
            active_ups[author_id] = author_name
            
    print(f"Identified {len(active_ups)} active stock-related UPs.")
    
    # 4. Save to monitored_ups.json
    output_path = "data/monitored_ups.json"
    
    # Load existing to merge? User said "filter these out and monitor them".
    # This implies these should be the *priority* or added to the list.
    # I'll merge them.
    
    current_monitored = {}
    if os.path.exists(output_path):
        try:
            with open(output_path, 'r', encoding='utf-8') as f:
                current_monitored = json.load(f)
        except:
            pass
            
    # Update/Add
    for uid, name in active_ups.items():
        current_monitored[uid] = name
        
    # Save
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(current_monitored, f, ensure_ascii=False, indent=4)
        
    print(f"Updated {output_path} with {len(active_ups)} active UPs.")
    print("Active UPs today:")
    for uid, name in active_ups.items():
        print(f"- {name} (UID: {uid})")

if __name__ == "__main__":
    filter_active_ups()
