import json
import logging
import os
import sys
import sqlite3

# Ensure core module is importable
sys.path.append(os.getcwd())
from core.storage import CubeStorage

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def migrate():
    json_path = "data/massive_cube_list.json"
    if not os.path.exists(json_path):
        logging.error(f"File not found: {json_path}")
        return

    logging.info(f"Loading data from {json_path}...")
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load JSON: {e}")
        return

    if not data:
        logging.info("No data to migrate.")
        return

    logging.info(f"Found {len(data)} records. Initializing database...")
    storage = CubeStorage()
    
    # Migrate in batches to avoid memory issues if list grows huge
    batch_size = 1000
    total = len(data)
    
    for i in range(0, total, batch_size):
        batch = data[i:i+batch_size]
        logging.info(f"Migrating batch {i//batch_size + 1}/{(total-1)//batch_size + 1} ({len(batch)} items)...")
        storage.upsert_cubes(batch)
        
    logging.info("Migration completed successfully!")
    # Verify count
    conn = sqlite3.connect("data/cubes.db")
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM cubes")
    count = cursor.fetchone()[0]
    conn.close()
    logging.info(f"Total cubes in DB: {count}")

if __name__ == "__main__":
    migrate()
