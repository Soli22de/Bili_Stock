import os
import json

data_dir = 'data/history'
earliest = '9999-99-99'
latest = '0000-00-00'
count = 0

if not os.path.exists(data_dir):
    print("No history directory.")
    exit()

for f in os.listdir(data_dir):
    if f.endswith('.json'):
        try:
            with open(os.path.join(data_dir, f), 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                if not data: continue
                times = [d.get('time', '') for d in data if 'time' in d]
                if times:
                    e = min(times)
                    l = max(times)
                    if e < earliest: earliest = e
                    if l > latest: latest = l
                    count += 1
        except Exception as e:
            print(f"Error {f}: {e}")

print(f"Analyzed {count} files.")
print(f"Overall Range: {earliest} to {latest}")
