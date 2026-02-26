import pandas as pd
import sys
import os

# Set display options
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 100)

today = '2026-02-24'
data_dir = 'data'

print(f"Checking data for: {today}")

# 1. Check Signals
signals_file = os.path.join(data_dir, 'trading_signals.csv')
if os.path.exists(signals_file):
    try:
        df_signals = pd.read_csv(signals_file)
        # Check date format in file first
        if 'date' in df_signals.columns:
            df_today_signals = df_signals[df_signals['date'].astype(str).str.contains(today)]
            print(f"\n--- TRADING SIGNALS ({len(df_today_signals)}) ---")
            if not df_today_signals.empty:
                # Use source_segment as content preview if title is missing
                cols = ['author_name', 'stock_name', 'stock_code', 'action', 'source_segment']
                existing_cols = [c for c in cols if c in df_today_signals.columns]
                print(df_today_signals[existing_cols].to_string(index=False))
            else:
                print("No signals found for today.")
    except Exception as e:
        print(f"Error reading signals: {e}")
else:
    print("Signals file not found.")

# 2. Check Raw Videos
videos_file = os.path.join(data_dir, 'dataset_videos.csv')
if os.path.exists(videos_file):
    try:
        df_videos = pd.read_csv(videos_file)
        if 'publish_time' in df_videos.columns:
            df_today_videos = df_videos[df_videos['publish_time'].astype(str).str.contains(today)]
            print(f"\n--- RAW VIDEOS ({len(df_today_videos)}) ---")
            if not df_today_videos.empty:
                print(df_today_videos[['author_name', 'title', 'publish_time']].to_string(index=False))
            else:
                print("No videos found for today.")
    except Exception as e:
        print(f"Error reading videos: {e}")
else:
    print("Videos file not found.")

# 3. Check Raw Comments
comments_file = os.path.join(data_dir, 'dataset_comments.csv')
if os.path.exists(comments_file):
    try:
        df_comments = pd.read_csv(comments_file)
        if 'publish_time' in df_comments.columns:
            df_today_comments = df_comments[df_comments['publish_time'].astype(str).str.contains(today)]
            print(f"\n--- RAW COMMENTS ({len(df_today_comments)}) ---")
            if not df_today_comments.empty:
                # Truncate content for display
                df_today_comments['content_short'] = df_comments['content'].str.slice(0, 50)
                print(df_today_comments[['user_name', 'content_short', 'publish_time']].to_string(index=False))
            else:
                print("No comments found for today.")
    except Exception as e:
        print(f"Error reading comments: {e}")
else:
    print("Comments file not found.")
