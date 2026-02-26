
import sqlite3
import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(message)s')

def audit_cube_counts():
    db_path = "data/cubes.db"
    if not os.path.exists(db_path):
        logging.error(f"Database not found: {db_path}")
        return

    conn = sqlite3.connect(db_path)
    
    try:
        # 1. Check Schema
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(rebalancing_history)")
        columns = [info[1] for info in cursor.fetchall()]
        # logging.info(f"Table columns: {columns}")
        
        # Identify Cube Identifier Column
        cube_col = None
        if 'cube_symbol' in columns:
            cube_col = 'cube_symbol'
        elif 'symbol' in columns and 'stock_symbol' in columns: # Ambiguous, but maybe 'symbol' is cube?
            # Usually 'cube_symbol' is explicit. 
            # Let's check if 'cube_id' exists
            pass
        
        if not cube_col:
            # Try to find any column that looks like a cube identifier
            # Common names: cube_symbol, cube_id, code
            for col in ['cube_symbol', 'cube_id', 'code']:
                if col in columns:
                    cube_col = col
                    break
        
        if not cube_col:
            logging.error(f"Could not identify cube column in {columns}")
            return

        # 2. Query Counts
        logging.info(f"Auditing history depth by Cube ({cube_col})...")
        
        date_col = 'created_at' if 'created_at' in columns else 'date'
        
        query = f"""
            SELECT 
                {cube_col} as cube,
                COUNT(*) as total_records,
                MIN({date_col}) as first_date,
                MAX({date_col}) as last_date
            FROM rebalancing_history
            GROUP BY {cube_col}
            ORDER BY total_records DESC
        """
        
        df = pd.read_sql_query(query, conn)
        
        if df.empty:
            logging.warning("No records found.")
            return

        # 3. Display Results
        logging.info("\n" + "="*60)
        logging.info("📊 CUBE HISTORY DEPTH AUDIT")
        logging.info("="*60)
        logging.info(f"Total Unique Cubes with History: {len(df)}")
        logging.info("-" * 60)
        logging.info(f"{'Cube Symbol':<15} | {'Records':<8} | {'First Date':<12} | {'Last Date':<12}")
        logging.info("-" * 60)
        
        # Show Top 20
        for _, row in df.head(20).iterrows():
            logging.info(f"{row['cube']:<15} | {row['total_records']:<8} | {str(row['first_date'])[:10]:<12} | {str(row['last_date'])[:10]:<12}")
            
        logging.info("-" * 60)
        
        # 4. Distribution Stats
        logging.info("\n📈 Data Quality Distribution:")
        bins = [0, 10, 50, 100, 500, 1000, 99999]
        labels = ['1-10', '11-50', '51-100', '101-500', '501-1000', '>1000']
        df['depth_tier'] = pd.cut(df['total_records'], bins=bins, labels=labels)
        dist = df['depth_tier'].value_counts().sort_index()
        
        for tier, count in dist.items():
            logging.info(f"  - {tier:<10} records: {count} cubes")

    except Exception as e:
        logging.error(f"Audit failed: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    audit_cube_counts()
