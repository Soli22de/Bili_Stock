import json
import pandas as pd
import os

def check_data():
    file_path = r"c:\jz_code\Bili_Stock\data\massive_cube_list.json"
    if not os.path.exists(file_path):
        print("File not found.")
        return

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        print(f"Total entries loaded: {len(data)}")
        
        if not data:
            print("Data is empty.")
            return

        df = pd.DataFrame(data)
        if "symbol" in df.columns:
            unique_symbols = df["symbol"].nunique()
            print(f"Unique symbols: {unique_symbols}")
            
            # Check for duplicates
            if len(data) > unique_symbols:
                print(f"Duplicates found: {len(data) - unique_symbols}")
                # Remove duplicates
                df_unique = df.drop_duplicates(subset=["symbol"])
                # Save back
                result = df_unique.to_dict(orient="records")
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(result, f, indent=4, ensure_ascii=False)
                print(f"Cleaned and saved {len(result)} unique entries.")
                df = df_unique # Update df for stats
            else:
                print("No duplicates found.")
            
            # Basic stats
            if "total_gain" in df.columns:
                 print(f"Mean Total Gain: {df['total_gain'].mean():.2f}%")
                 print(f"Max Total Gain: {df['total_gain'].max():.2f}%")
                 print(f"Min Total Gain: {df['total_gain'].min():.2f}%")
                 
            if "follower_count" in df.columns:
                 print(f"Mean Followers: {df['follower_count'].mean():.2f}")
                 print(f"Max Followers: {df['follower_count'].max()}")

            if "created_at" in df.columns:
                 # Convert to datetime
                 df['created_at_dt'] = pd.to_datetime(df['created_at'], unit='ms')
                 print(f"Oldest Cube: {df['created_at_dt'].min()}")
                 print(f"Newest Cube: {df['created_at_dt'].max()}")
                 
        else:
            print("No 'symbol' column found.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_data()
