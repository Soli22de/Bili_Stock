import json
import pandas as pd
import os

try:
    with open('data/up_list.json', 'r', encoding='utf-8') as f:
        ups = json.load(f)
    print(f"Total UPs in pool: {len(ups)}")
except Exception as e:
    print(f"Error reading up_list.json: {e}")

try:
    if os.path.exists('data/discovery_videos.csv'):
        videos = pd.read_csv('data/discovery_videos.csv')
        print(f"Total Videos collected: {len(videos)}")
        print(f"Unique UPs scanned: {videos['author_id'].nunique() if not videos.empty else 0}")
    else:
        print("data/discovery_videos.csv does not exist.")
except Exception as e:
    print(f"Error reading discovery_videos.csv: {e}")

try:
    if os.path.exists('data/trader_bloggers_rank.csv'):
        rank = pd.read_csv('data/trader_bloggers_rank.csv')
        print(f"Ranked UPs: {len(rank)}")
    else:
        print("data/trader_bloggers_rank.csv does not exist.")
except Exception as e:
    print(f"Error reading trader_bloggers_rank.csv: {e}")
