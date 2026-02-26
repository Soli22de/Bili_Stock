
import sqlite3
import pandas as pd
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(message)s')

def audit_data_density():
    db_path = "data/cubes.db"
    if not os.path.exists(db_path):
        logging.error(f"Database not found: {db_path}")
        return

    logging.info("Connecting to database...")
    conn = sqlite3.connect(db_path)
    
    try:
        # Load date column from rebalancing_history
        # Check if 'created_at' or 'date' exists
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(rebalancing_history)")
        columns = [info[1] for info in cursor.fetchall()]
        
        date_col = 'created_at' if 'created_at' in columns else 'date'
        
        # Fallback if neither found (should exist based on previous code)
        if 'created_at' not in columns and 'date' not in columns:
             # Just select * limit 1 to see cols
             df_head = pd.read_sql_query("SELECT * FROM rebalancing_history LIMIT 1", conn)
             logging.info(f"Columns: {df_head.columns.tolist()}")
             return

        query_col = 'created_at' if 'created_at' in columns else 'date'
        
        logging.info(f"Querying {query_col} from rebalancing_history...")
        df = pd.read_sql_query(f"SELECT {query_col} as date_val FROM rebalancing_history", conn)
        
    except Exception as e:
        logging.error(f"Query failed: {e}")
        return
    finally:
        conn.close()

    if df.empty:
        logging.warning("No records found.")
        return

    # Process dates
    # Handle unix timestamp if necessary, but usually it's string or int
    # Based on previous code: pd.to_datetime(df['date'], format='mixed', errors='coerce')
    df['date'] = pd.to_datetime(df['date_val'], format='mixed', errors='coerce')
    df = df.dropna(subset=['date'])
    
    if df.empty:
        logging.warning("No valid dates found.")
        return

    # Group by Month
    df['month'] = df['date'].dt.to_period('M')
    monthly_counts = df['month'].value_counts().sort_index()
    
    logging.info("\n" + "="*40)
    logging.info("📅 DATA DENSITY AUDIT (Monthly Records)")
    logging.info("="*40)
    logging.info(f"{'Month':<10} | {'Count':<10} | {'Distribution'}")
    logging.info("-" * 40)
    
    if not monthly_counts.empty:
        max_count = monthly_counts.max()
        scale = 50 / max_count if max_count > 0 else 1
        
        for period, count in monthly_counts.items():
            bar_len = int(count * scale)
            bar = "█" * bar_len
            logging.info(f"{str(period):<10} | {count:<10} | {bar}")
            
        logging.info("="*40)
        logging.info(f"Total Records: {len(df)}")
        logging.info(f"Date Range: {df['date'].min().date()} to {df['date'].max().date()}")
    else:
        logging.info("No monthly data found.")

if __name__ == "__main__":
    audit_data_density()
