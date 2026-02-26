import pandas as pd
import json

try:
    df = pd.read_csv('data/cube_performance_ranking.csv')
    df_valuable = df[df['return'] > 0].copy()
    
    output_path = 'data/valuable_cubes.json'
    df_valuable.to_json(output_path, orient='records', force_ascii=False, indent=2)
    
    print(f"Saved {len(df_valuable)} valuable cubes to {output_path}")
    print(df_valuable[['symbol', 'name', 'return', 'sharpe']].to_string())
except Exception as e:
    print(f"Error: {e}")
