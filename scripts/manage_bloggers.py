import requests
import json
import os
import pandas as pd
import sys
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
NEW_UID = 550494308
MIN_SIGNALS = 5
DATA_DIR = "data"
UP_LIST_FILE = os.path.join(DATA_DIR, "up_list.json")
SIGNALS_FILE = os.path.join(DATA_DIR, "trading_signals.csv")
VIDEOS_FILE = os.path.join(DATA_DIR, "dataset_videos.csv")
COLLECTOR_FILE = "core/bili_collector.py"

# Bloggers to protect from cleanup regardless of signal count
PROTECTED_BLOGGERS = ["小匠财", "小匠才", "卢本圆复盘"]

def get_username(uid):
    url = f"https://api.bilibili.com/x/space/acc/info?mid={uid}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=5)
        data = response.json()
        if data['code'] == 0:
            return data['data']['name']
    except Exception as e:
        logger.error(f"Error fetching name for {uid}: {e}")
    return f"User_{uid}"

def add_blogger(uid, name):
    # 1. Update up_list.json
    if os.path.exists(UP_LIST_FILE):
        with open(UP_LIST_FILE, 'r', encoding='utf-8') as f:
            up_list = json.load(f)
    else:
        up_list = {}
    
    uid_str = str(uid)
    if uid_str not in up_list:
        up_list[uid_str] = name
        logger.info(f"Adding {name} ({uid}) to {UP_LIST_FILE}")
        with open(UP_LIST_FILE, 'w', encoding='utf-8') as f:
            json.dump(up_list, f, ensure_ascii=False, indent=4)
    else:
        logger.info(f"{name} already in {UP_LIST_FILE}")

    # 2. Update core/bili_collector.py (UID_MAP_INTERNAL)
    # This is a bit hacky, but ensures it's in the hardcoded list too if needed
    with open(COLLECTOR_FILE, 'r', encoding='utf-8') as f:
        content = f.read()
    
    if str(uid) not in content:
        # Find the end of UID_MAP_INTERNAL
        marker = "UID_MAP_INTERNAL = {"
        if marker in content:
            # Simple insertion at the start of the dict
            new_line = f"\n    {uid}: \"{name}\","
            content = content.replace(marker, marker + new_line)
            logger.info(f"Adding {name} to {COLLECTOR_FILE}")
            with open(COLLECTOR_FILE, 'w', encoding='utf-8') as f:
                f.write(content)

def clean_low_data_bloggers():
    if not os.path.exists(SIGNALS_FILE):
        logger.warning("Signals file not found.")
        return

    df = pd.read_csv(SIGNALS_FILE)
    counts = df['author_name'].value_counts()
    
    to_remove = counts[counts < MIN_SIGNALS].index.tolist()
    
    # Filter out protected bloggers
    to_remove = [name for name in to_remove if name not in PROTECTED_BLOGGERS]
    
    if not to_remove:
        logger.info("No bloggers to remove.")
        return

    logger.info(f"Removing bloggers with < {MIN_SIGNALS} signals: {to_remove}")
    
    # 1. Remove from signals
    df_clean = df[~df['author_name'].isin(to_remove)]
    df_clean.to_csv(SIGNALS_FILE, index=False)
    logger.info(f"Removed {len(df) - len(df_clean)} rows from signals.")

    # 2. Remove from videos
    if os.path.exists(VIDEOS_FILE):
        df_v = pd.read_csv(VIDEOS_FILE)
        df_v_clean = df_v[~df_v['author_name'].isin(to_remove)]
        df_v_clean.to_csv(VIDEOS_FILE, index=False)
        logger.info(f"Removed {len(df_v) - len(df_v_clean)} rows from videos.")
    
    # 3. Remove from up_list.json (Need to map Name -> UID)
    # This is hard because up_list is UID -> Name.
    # We'll load up_list, invert it, find UIDs for names, and remove.
    if os.path.exists(UP_LIST_FILE):
        with open(UP_LIST_FILE, 'r', encoding='utf-8') as f:
            up_list = json.load(f)
        
        new_up_list = {k: v for k, v in up_list.items() if v not in to_remove}
        
        if len(new_up_list) < len(up_list):
            logger.info(f"Removed {len(up_list) - len(new_up_list)} bloggers from {UP_LIST_FILE}")
            with open(UP_LIST_FILE, 'w', encoding='utf-8') as f:
                json.dump(new_up_list, f, ensure_ascii=False, indent=4)

    # 4. Remove from collector (Hardcoded)
    # This is risky with regex, maybe skip for now or print manual instruction
    logger.info("Note: Please manually remove these bloggers from core/bili_collector.py if they are hardcoded.")

def main():
    # 1. Add new blogger
    name = get_username(NEW_UID)
    logger.info(f"Fetched name for {NEW_UID}: {name}")
    add_blogger(NEW_UID, name)
    
    # 2. Clean low data
    clean_low_data_bloggers()

if __name__ == "__main__":
    main()
