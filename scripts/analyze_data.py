import csv
from datetime import datetime
import sys
import os

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config

def analyze_csv(filename, time_col_idx):
    count = 0
    min_time = None
    max_time = None
    
    try:
        with open(filename, 'r', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                count += 1
                try:
                    ts_str = row[time_col_idx]
                    ts = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
                    if min_time is None or ts < min_time:
                        min_time = ts
                    if max_time is None or ts > max_time:
                        max_time = ts
                except:
                    pass
    except FileNotFoundError:
        print(f"File not found: {filename}")
        return 0, None, None
                
    return count, min_time, max_time

print("=== 数据集统计 ===")
v_count, v_min, v_max = analyze_csv(config.VIDEOS_CSV, 5)
print(f"视频/动态总数: {v_count}")
print(f"时间跨度: {v_min} 至 {v_max}")

c_count, c_min, c_max = analyze_csv(config.COMMENTS_CSV, 6)
print(f"评论总数: {c_count}")
print(f"时间跨度: {c_min} 至 {c_max}")
