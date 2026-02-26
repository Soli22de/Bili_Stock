import os
import json
import pandas as pd
from datetime import datetime

def analyze_history():
    data_dir = "data/history"
    files = [f for f in os.listdir(data_dir) if f.endswith(".json")]
    
    summary = []
    
    for file in files:
        path = os.path.join(data_dir, file)
        with open(path, "r", encoding="utf-8") as f:
            try:
                signals = json.load(f)
            except:
                continue
                
        if not signals:
            continue
            
        dates = [s["time"][:10] for s in signals]
        dates.sort()
        
        start_date = dates[0]
        end_date = dates[-1]
        
        years = [d[:4] for d in dates]
        year_counts = {y: years.count(y) for y in set(years)}
        
        # Calculate coverage score
        # Ideally we want data in 2022, 2023, 2024
        has_2022 = year_counts.get("2022", 0) > 0
        has_2023 = year_counts.get("2023", 0) > 0
        has_2024 = year_counts.get("2024", 0) > 0
        
        cube_symbol = file.replace(".json", "")
        # Try to find name from signals if available (not stored in signals directly, but let's see)
        # Actually I can load long_history_cubes.json to get names
        
        summary.append({
            "symbol": cube_symbol,
            "total_signals": len(signals),
            "start_date": start_date,
            "end_date": end_date,
            "count_2022": year_counts.get("2022", 0),
            "count_2023": year_counts.get("2023", 0),
            "count_2024": year_counts.get("2024", 0),
            "count_2025": year_counts.get("2025", 0),
            "count_2026": year_counts.get("2026", 0),
            "coverage_quality": sum([has_2022, has_2023, has_2024])
        })
        
    df = pd.DataFrame(summary)
    if not df.empty:
        df = df.sort_values("coverage_quality", ascending=False)
        print(df.to_string())
        
        # Filter good candidates
        good_candidates = df[df["coverage_quality"] >= 2]
        print(f"\nGood candidates (covering at least 2 years of 2022-2024): {len(good_candidates)}")
        print(good_candidates[["symbol", "start_date", "count_2022", "count_2023", "count_2024"]].to_string())
        
        # Save analysis
        df.to_csv("data/history_analysis.csv", index=False)
    else:
        print("No history data found.")

if __name__ == "__main__":
    analyze_history()
