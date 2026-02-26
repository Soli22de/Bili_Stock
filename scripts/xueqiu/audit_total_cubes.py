
import sqlite3
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(message)s')

def audit_total_cubes():
    db_path = "data/cubes.db"
    if not os.path.exists(db_path):
        logging.error(f"Database not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    try:
        cursor = conn.cursor()
        
        # 1. Total Cubes in Metadata
        cursor.execute("SELECT COUNT(*) FROM cubes")
        result = cursor.fetchone()
        total_cubes = result[0] if result else 0
        print(f"Total Cubes in Metadata (cubes table): {total_cubes}")
        
        # 2. Total Cubes with History
        cursor.execute("SELECT COUNT(DISTINCT cube_symbol) FROM rebalancing_history")
        result = cursor.fetchone()
        cubes_with_history = result[0] if result else 0
        print(f"Cubes with Rebalancing History: {cubes_with_history}")
        
        # 3. Gap Analysis
        print(f"Gap (Unmonitored Cubes): {total_cubes - cubes_with_history}")
        
        # 4. Sample Unmonitored Cubes (Top 10 by Followers)
        print("\n[Sample Unmonitored Cubes - Top 10 by Followers]")
        query = """
            SELECT symbol, name, followers_count, total_gain 
            FROM cubes 
            WHERE symbol NOT IN (SELECT DISTINCT cube_symbol FROM rebalancing_history)
            ORDER BY followers_count DESC 
            LIMIT 10
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        for row in rows:
            print(f"{row[0]} | {row[1]} | Followers: {row[2]} | Gain: {row[3]}%")

    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    audit_total_cubes()
