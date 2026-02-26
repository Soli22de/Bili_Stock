import json
try:
    with open('data/history/ZH583267.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        dates = [d.get('time', '') for d in data]
        print(f"2022: {sum(1 for d in dates if '2022' in d)}")
        print(f"2023: {sum(1 for d in dates if '2023' in d)}")
        print(f"2024: {sum(1 for d in dates if '2024' in d)}")
        print(f"2025: {sum(1 for d in dates if '2025' in d)}")
        
        # Check earliest signal
        if dates:
            print(f"Earliest: {min(dates)}")
            print(f"Latest: {max(dates)}")
except Exception as e:
    print(f"Error: {e}")
