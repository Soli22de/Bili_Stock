
import sqlite3
import pandas as pd
import os

db_path = os.path.join(os.getcwd(), "data", "cubes.db")
if not os.path.exists(db_path):
    print("DB not found")
else:
    conn = sqlite3.connect(db_path)
    try:
        # Check table names
        res = conn.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()
        print("Tables:", res)
        
        # Check date range in rebalancing_history
        if ('rebalancing_history',) in res:
            min_date = conn.execute("SELECT MIN(updated_at) FROM rebalancing_history").fetchone()[0]
            max_date = conn.execute("SELECT MAX(updated_at) FROM rebalancing_history").fetchone()[0]
            print(f"Rebalancing History Range: {min_date} to {max_date}")
            
            # Check count
            count = conn.execute("SELECT COUNT(*) FROM rebalancing_history").fetchone()[0]
            print(f"Total rows: {count}")
            
            # Check a sample from 2019
            sample = conn.execute("SELECT * FROM rebalancing_history WHERE updated_at LIKE '2019%' LIMIT 1").fetchone()
            print("2019 Sample:", sample)
            
    except Exception as e:
        print("Error:", e)
    finally:
        conn.close()
