
import sqlite3
import logging
import os
import sys
from datetime import datetime

# Add project root to sys.path
sys.path.append(os.getcwd())

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def escape_string(val):
    if val is None:
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    # Escape single quotes for SQL
    return "'" + str(val).replace("'", "''") + "'"

def export_sqlite_to_sql():
    sqlite_path = "data/cubes.db"
    output_path = "data/smart_money_dump.sql"
    
    if not os.path.exists(sqlite_path):
        logging.error(f"Source SQLite DB not found: {sqlite_path}")
        return

    logging.info(f"Exporting {sqlite_path} to {output_path}...")
    
    conn = sqlite3.connect(sqlite_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    with open(output_path, "w", encoding="utf-8") as f:
        # 1. Export Schema (DDL)
        # Note: Supabase/Postgres syntax might differ slightly from SQLite.
        # We will generate standard PostgreSQL compatible DDL.
        
        f.write("-- Smart Momentum Quant Database Dump\n")
        f.write(f"-- Generated at {datetime.now()}\n\n")
        
        # Table: cubes
        f.write("CREATE TABLE IF NOT EXISTS cubes (\n")
        f.write("    symbol VARCHAR(20) PRIMARY KEY,\n")
        f.write("    name VARCHAR(100),\n")
        f.write("    owner_id VARCHAR(50),\n")
        f.write("    owner_name VARCHAR(100),\n")
        f.write("    followers_count INTEGER,\n")
        f.write("    total_gain FLOAT,\n")
        f.write("    monthly_gain FLOAT,\n")
        f.write("    daily_gain FLOAT,\n")
        f.write("    annualized_gain_rate FLOAT,\n")
        f.write("    description TEXT,\n")
        f.write("    created_at TIMESTAMP,\n")
        f.write("    updated_at TIMESTAMP,\n")
        f.write("    raw_json TEXT\n")
        f.write(");\n\n")
        
        # Table: rebalancing_history
        f.write("CREATE TABLE IF NOT EXISTS rebalancing_history (\n")
        f.write("    id SERIAL PRIMARY KEY,\n")
        f.write("    cube_symbol VARCHAR(20),\n")
        f.write("    stock_symbol VARCHAR(20),\n")
        f.write("    stock_name VARCHAR(50),\n")
        f.write("    prev_weight_adjusted FLOAT,\n")
        f.write("    target_weight FLOAT,\n")
        f.write("    price FLOAT,\n")
        f.write("    net_value FLOAT,\n")
        f.write("    created_at TIMESTAMP,\n")
        f.write("    updated_at TIMESTAMP,\n")
        f.write("    status VARCHAR(20),\n")
        f.write("    UNIQUE(cube_symbol, stock_symbol, created_at)\n")
        f.write(");\n\n")
        
        # Indexes
        f.write("CREATE INDEX IF NOT EXISTS idx_cubes_total_gain ON cubes(total_gain);\n")
        f.write("CREATE INDEX IF NOT EXISTS idx_rebalancing_cube_symbol ON rebalancing_history(cube_symbol);\n")
        f.write("CREATE INDEX IF NOT EXISTS idx_rebalancing_stock_symbol ON rebalancing_history(stock_symbol);\n\n")
        
        # 2. Export Data (INSERT statements)
        
        # --- Cubes ---
        logging.info("Exporting Cubes Metadata...")
        cursor.execute("SELECT * FROM cubes")
        rows = cursor.fetchall()
        
        f.write("-- Data: cubes\n")
        for i, row in enumerate(rows):
            vals = [
                escape_string(row['symbol']),
                escape_string(row['name']),
                escape_string(row['owner_id']),
                escape_string(row['owner_name']),
                escape_string(row['followers_count']),
                escape_string(row['total_gain']),
                escape_string(row['monthly_gain']),
                escape_string(row['daily_gain']),
                escape_string(row['annualized_gain_rate']),
                escape_string(row['description']),
                escape_string(row['created_at']), # Already string in SQLite
                escape_string(row['updated_at']),
                escape_string(row['raw_json'])
            ]
            
            # Use ON CONFLICT DO NOTHING to be safe
            sql = f"INSERT INTO cubes VALUES ({', '.join(vals)}) ON CONFLICT (symbol) DO NOTHING;\n"
            f.write(sql)
            
            if (i + 1) % 1000 == 0:
                print(f"\rExported Cubes: {i+1}", end="")
        
        print(f"\rExported Cubes: {len(rows)} (Done)\n")
        
        # --- History ---
        logging.info("Exporting Rebalancing History...")
        cursor.execute("SELECT * FROM rebalancing_history")
        # Use fetchmany to save memory
        BATCH_SIZE = 5000
        total_exported = 0
        
        f.write("\n-- Data: rebalancing_history\n")
        
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows: break
            
            for row in rows:
                vals = [
                    # Skip 'id' to let Postgres auto-increment (SERIAL)
                    # But wait, insert values need to match columns.
                    # We should specify columns in INSERT.
                    # Or just pass DEFAULT for id?
                    # Let's specify columns to be safe.
                    
                    escape_string(row['cube_symbol']),
                    escape_string(row['stock_symbol']),
                    escape_string(row['stock_name']),
                    escape_string(row['prev_weight_adjusted']),
                    escape_string(row['target_weight']),
                    escape_string(row['price']),
                    escape_string(row['net_value']),
                    escape_string(row['created_at']),
                    escape_string(row['updated_at']),
                    escape_string(row['status'])
                ]
                
                sql = f"INSERT INTO rebalancing_history (cube_symbol, stock_symbol, stock_name, prev_weight_adjusted, target_weight, price, net_value, created_at, updated_at, status) VALUES ({', '.join(vals)}) ON CONFLICT (cube_symbol, stock_symbol, created_at) DO NOTHING;\n"
                f.write(sql)
                total_exported += 1
            
            print(f"\rExported History: {total_exported}", end="")
            
        print(f"\rExported History: {total_exported} (Done)\n")

    conn.close()
    logging.info(f"Export complete: {output_path}")
    logging.info(f"File size: {os.path.getsize(output_path) / 1024 / 1024:.2f} MB")

if __name__ == "__main__":
    export_sqlite_to_sql()
