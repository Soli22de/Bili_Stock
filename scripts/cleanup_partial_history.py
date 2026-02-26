
import json
import os
import glob
from datetime import datetime

history_dir = r"c:\jz_code\Bili_Stock\data\history"
files = glob.glob(os.path.join(history_dir, "ZH*.json"))

deleted_count = 0

print(f"Checking {len(files)} files in {history_dir}...")

for file_path in files:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                signals = json.load(f)
            except json.JSONDecodeError:
                print(f"Deleting corrupted file: {file_path}")
                f.close()
                os.remove(file_path)
                deleted_count += 1
                continue
            
        if not signals:
            print(f"Deleting empty file: {file_path}")
            os.remove(file_path)
            deleted_count += 1
            continue
            
        # Check date range
        timestamps = [s.get("timestamp", 0) for s in signals]
        if not timestamps:
            print(f"Deleting file with no timestamps: {file_path}")
            os.remove(file_path)
            deleted_count += 1
            continue
            
        min_ts = min(timestamps)
        min_date = datetime.fromtimestamp(min_ts/1000)
        
        # If data only goes back to 2025, it's likely partial (failed at page 2)
        # But we need to be careful. Some cubes might be new.
        # Let's check if the cube creation date is recent?
        # We don't have creation date here easily.
        # But if it has < 50 signals and starts in 2025, it's suspicious if it's an old cube.
        # For now, let's delete if it has < 100 signals AND starts after 2024-01-01, 
        # unless it's the debug one we just fetched (Supernova).
        
        # Actually, looking at the logs, the failed ones had "Saved 32 signals", "Saved 66 signals" etc.
        # And they failed at page 2.
        # If page 1 was successful (20 items), but page 2 failed, we have 20 items.
        # The logs showed "Saved 66 signals" for ZH1263458 (failed page 2). Wait, if page 2 failed, how did we get 66?
        # Ah, maybe page size is 20? 66 signals implies 3+ pages.
        # Wait, if page 2 failed, we should have at most 20 signals (from page 1).
        # Let's re-read the log.
        # "Failed to fetch page 2 for ZH1263458: 400" -> "Saved 66 signals".
        # This implies `fetch_history` loop didn't break immediately or page 2 failure was soft?
        # No, `break` is in the `except` or `if status != 200`.
        
        # In the log:
        # 2026-02-19 17:34:18,184 - INFO - Fetching history for ZH1263458...
        # 2026-02-19 17:34:19,080 - WARNING - Failed to fetch page 2 for ZH1263458: 400
        # 2026-02-19 17:34:19,083 - INFO - Saved 66 signals for 随意 (ZH1263458).
        
        # How can it save 66 signals if page 2 failed?
        # Maybe `all_signals` accumulates?
        # Ah, maybe page 1 returned 66 items? "count": 20 in params.
        # Xueqiu might ignore count or return more?
        # Or `_parse_move` returns multiple signals per item (one item = one rebalancing, multiple stocks).
        # Yes, one rebalancing can have multiple stock changes.
        
        # So if page 2 failed, we definitely missed data.
        # We should delete ANY file where we know we encountered an error.
        # But the script doesn't mark the file as incomplete.
        
        # Heuristic: If min_date > 2023-01-01, delete it to be safe and re-fetch.
        # Most "Massive" cubes are old.
        if min_date.year >= 2023:
             print(f"Deleting likely partial history (starts {min_date.date()}): {file_path}")
             os.remove(file_path)
             deleted_count += 1
            
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

print(f"Deleted {deleted_count} partial/empty files.")
