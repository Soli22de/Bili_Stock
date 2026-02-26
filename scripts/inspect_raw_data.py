import pandas as pd
import sys

pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', 1000)

try:
    df = pd.read_csv('data/dataset_videos.csv')
    today = '2026-02-24'
    df_today = df[df['publish_time'].astype(str).str.contains(today)]
    
    print("--- RAW CONTENT FOR TODAY ---")
    for idx, row in df_today.iterrows():
        print(f"Author: {row.get('author_name', 'Unknown')}")
        print(f"Time: {row.get('publish_time', 'Unknown')}")
        print(f"Title: {row.get('title', '-')}")
        print(f"Content: {row.get('content', '-')}")
        print("-" * 50)

except Exception as e:
    print(f"Error: {e}")
