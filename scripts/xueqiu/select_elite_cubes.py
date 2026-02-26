
import sqlite3
import pandas as pd
import json
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(message)s')

def select_elite_cubes(limit=5000):
    db_path = "data/cubes.db"
    if not os.path.exists(db_path):
        logging.error(f"Database not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    
    try:
        # Select Candidates
        # Criteria: Followers > 40 (User Defined) AND Total Gain > 0
        # Order by Followers (Consensus)
        query = f"""
            SELECT symbol, name, followers_count, total_gain 
            FROM cubes 
            WHERE followers_count > 40 AND total_gain > 0
            ORDER BY followers_count DESC 
            LIMIT {limit}
        """
        
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            logging.warning("No candidates found.")
            return

        logging.info(f"Selected {len(df)} elite candidates (Followers > 40).")
        logging.info("Top 5 Candidates:")
        logging.info(df.head(5).to_string(index=False))
        
        # Convert to list of dicts
        candidates = df.to_dict('records')
        
        # Save to JSON
        output_file = "data/elite_5000_candidates.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(candidates, f, ensure_ascii=False, indent=2)
            
        logging.info(f"Saved candidate list to {output_file}")

    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    select_elite_cubes()
