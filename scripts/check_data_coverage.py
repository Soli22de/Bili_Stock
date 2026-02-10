import pandas as pd
from bili_collector import UID_MAP

def check_coverage():
    try:
        df = pd.read_csv('dataset_videos.csv')
    except FileNotFoundError:
        print("Dataset not found.")
        return

    collected_uids = set(df['author_id'].unique())
    target_uids = set(UID_MAP.keys())
    
    missing_uids = target_uids - collected_uids
    
    print(f"Total UPs targeted: {len(target_uids)}")
    print(f"Total UPs collected: {len(collected_uids)}")
    print(f"Missing coverage: {len(missing_uids)}")
    
    if missing_uids:
        print("Missing UPs:")
        for uid in missing_uids:
            print(f"- {UID_MAP.get(uid, uid)}")
    else:
        print("All UPs covered!")

if __name__ == "__main__":
    check_coverage()
