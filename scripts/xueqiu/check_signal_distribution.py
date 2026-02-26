import os
import json
import datetime
from collections import defaultdict

data_dir = 'data/history'
year_counts = defaultdict(int)
cube_years = defaultdict(set)

if not os.path.exists(data_dir):
    print("No history data.")
    exit()

for f in os.listdir(data_dir):
    if f.endswith('.json'):
        try:
            with open(os.path.join(data_dir, f), 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                if not data: continue
                for d in data:
                    t = d.get('time', '')
                    if t:
                        year = t[:4]
                        year_counts[year] += 1
                        cube_years[year].add(f)
        except:
            pass

for y in sorted(year_counts.keys()):
    print(f"{y}: {year_counts[y]} signals from {len(cube_years[y])} cubes")

if '2022' in cube_years:
    print("Cubes with 2022 data:", list(cube_years['2022']))
