import os
import time
import pandas as pd
from datetime import datetime

def get_file_info(filepath):
    if not os.path.exists(filepath):
        return "Not Found", 0, "-"
    
    size = os.path.getsize(filepath)
    mtime = os.path.getmtime(filepath)
    mtime_str = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        if filepath.endswith('.csv'):
            # Try reading with pandas, fallback to line count if error
            try:
                df = pd.read_csv(filepath)
                count = len(df)
            except:
                with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                    count = sum(1 for _ in f) - 1 # approximate header
        else:
            count = "-"
    except:
        count = "?"
        
    return f"{size/1024:.1f} KB", count, mtime_str

def check_log_status(log_file):
    if not os.path.exists(log_file):
        return "No log file"
    
    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
        if not lines:
            return "Empty log"
        return lines[-1].strip()

def main():
    print("="*60)
    print(f"Bili_Stock System Status Monitor - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    files_to_check = [
        ("UP Pool (Videos)", "data/discovery_videos.csv"),
        ("UP Pool (Comments)", "data/discovery_comments.csv"),
        ("OCR Results", "data/video_ocr_results.csv"),
        ("Keyframe Images", "data/keyframes"), # Special handling for dir
        ("Ranked Traders", "data/trader_bloggers_rank.csv"),
        ("Strategy Log", "data/strategy_log.csv"),
    ]
    
    print(f"{'File':<25} | {'Size':<10} | {'Rows/Count':<10} | {'Last Updated':<20}")
    print("-" * 75)
    
    for label, path in files_to_check:
        if label == "Keyframe Images":
            if os.path.exists(path):
                # Count total files in subdirs
                count = 0
                last_mtime = 0
                for root, dirs, files in os.walk(path):
                    for file in files:
                        count += 1
                        fp = os.path.join(root, file)
                        mt = os.path.getmtime(fp)
                        if mt > last_mtime:
                            last_mtime = mt
                
                size_str = "Dir"
                count_str = str(count)
                mtime_str = datetime.fromtimestamp(last_mtime).strftime('%Y-%m-%d %H:%M:%S') if last_mtime > 0 else "-"
            else:
                size_str, count_str, mtime_str = "Not Found", 0, "-"
        else:
            size_str, count_str, mtime_str = get_file_info(path)
            
        print(f"{label:<25} | {size_str:<10} | {count_str:<10} | {mtime_str:<20}")

    print("-" * 75)
    print("\nRecent Activity Analysis:")
    
    # Check discovery log (using recent file update as proxy for now, or check specific log if we had one)
    # Since we run discovery in terminal, we might not have a dedicated log file unless we redirected it.
    # But we can check discovery_videos.csv mtime.
    disc_vid_path = "data/discovery_videos.csv"
    if os.path.exists(disc_vid_path):
        mtime = os.path.getmtime(disc_vid_path)
        elapsed = time.time() - mtime
        if elapsed < 300: # 5 mins
            print(f"[ACTIVE] UP Discovery: Updated {elapsed:.0f}s ago.")
        else:
            print(f"[WARNING] UP Discovery: No updates for {elapsed/60:.1f} mins. (May be paused/throttled)")
    
    # Check extraction status via log file and keyframes dir
    kf_log_path = "logs/extract_video_keyframes.log"
    kf_active = False
    
    if os.path.exists(kf_log_path):
        mtime = os.path.getmtime(kf_log_path)
        elapsed = time.time() - mtime
        if elapsed < 60:
            print(f"[ACTIVE] Keyframe Extraction: Log updated {elapsed:.0f}s ago (Processing/OCR).")
            kf_active = True
        else:
             print(f"[IDLE?] Keyframe Extraction: Log not updated for {elapsed:.0f}s.")
    
    if not kf_active:
        kf_path = "data/keyframes"
        if os.path.exists(kf_path):
            # Find latest file
            latest_time = 0
            for root, dirs, files in os.walk(kf_path):
                for file in files:
                    mt = os.path.getmtime(os.path.join(root, file))
                    if mt > latest_time:
                        latest_time = mt
            
            elapsed = time.time() - latest_time
            if elapsed < 60:
                print(f"[ACTIVE] Keyframe Extraction: New frames saved {elapsed:.0f}s ago.")
            else:
                print(f"[STATUS] Keyframe Extraction: No new frames for {elapsed:.0f}s. (Likely running OCR or idle)")

    print("\nNote: 'UP Pool' collection often pauses due to API limits (412 error). This is normal.")
    print("      'OCR Results' will only appear when specific trading keywords are found.")
    print("="*60)

if __name__ == "__main__":
    main()
