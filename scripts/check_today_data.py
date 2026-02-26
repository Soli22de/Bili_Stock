import pandas as pd
import os
import datetime

def check_today_videos():
    csv_path = "data/dataset_videos.csv"
    if not os.path.exists(csv_path):
        print("dataset_videos.csv not found.")
        return

    try:
        df = pd.read_csv(csv_path)
        print(f"Total rows: {len(df)}")
        print(f"Columns: {df.columns.tolist()}")
        
        # Convert publish_time to string just in case
        df['publish_time'] = df['publish_time'].astype(str)
        
        today = datetime.datetime.now().strftime('%Y-%m-%d')
        print(f"Checking for date: {today}")
        
        today_videos = df[df['publish_time'].str.startswith(today)]
        print(f"Videos found for today: {len(today_videos)}")
        
        if not today_videos.empty:
            print("\nSample videos from today:")
            print(today_videos[['author_name', 'title']].head(10))
            
            # Group by author
            active_authors = today_videos['author_name'].unique()
            print(f"\nActive authors today: {len(active_authors)}")
            print(list(active_authors))
            
    except Exception as e:
        print(f"Error reading CSV: {e}")

if __name__ == "__main__":
    check_today_videos()
