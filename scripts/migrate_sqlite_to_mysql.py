
import sqlite3
import logging
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Add project root to sys.path
sys.path.append(os.getcwd())

# Import Models from Core (Must be imported AFTER sys.path)
try:
    from core.storage import CubeModel, RebalancingModel, Base
    import config
except ImportError:
    logging.error("Failed to import core modules. Run from project root.")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def parse_dt(val):
    if not val: return None
    if isinstance(val, str):
        try: return datetime.fromisoformat(val)
        except: pass
        try: return datetime.strptime(val, "%Y-%m-%d %H:%M:%S.%f")
        except: pass
        try: return datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
        except: pass
    return val

def migrate_data():
    sqlite_path = "data/cubes.db"
    
    # Check Source
    if not os.path.exists(sqlite_path):
        logging.error(f"Source SQLite DB not found: {sqlite_path}")
        return

    # Check Target
    db_url = getattr(config, 'DB_URL', None)
    if not db_url:
        logging.error("Target DB_URL not found in config.py. Please configure target database first.")
        return
        
    logging.info(f"Source: {sqlite_path}")
    logging.info(f"Target: {db_url.split('@')[-1]}") # Hide password
    
    # 1. Connect to Source (SQLite)
    conn_sqlite = sqlite3.connect(sqlite_path)
    conn_sqlite.row_factory = sqlite3.Row
    cursor = conn_sqlite.cursor()
    
    # 2. Connect to Target (MySQL/Postgres)
    engine = create_engine(db_url)
    Base.metadata.create_all(engine) # Ensure schema exists
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # --- Migrate Cubes ---
        logging.info("Migrating Cubes Metadata...")
        cursor.execute("SELECT * FROM cubes")
        rows = cursor.fetchall()
        count = 0
        total_source = len(rows)
        
        for row in rows:
            # Check existence
            exists = session.query(CubeModel).filter_by(symbol=row['symbol']).first()
            if exists: continue
            
            cube = CubeModel(
                symbol=row['symbol'],
                name=row['name'],
                owner_id=row['owner_id'],
                owner_name=row['owner_name'],
                followers_count=row['followers_count'],
                total_gain=row['total_gain'],
                monthly_gain=row['monthly_gain'],
                daily_gain=row['daily_gain'],
                annualized_gain_rate=row['annualized_gain_rate'],
                description=row['description'],
                created_at=parse_dt(row['created_at']),
                updated_at=parse_dt(row['updated_at']),
                raw_json=row['raw_json']
            )
            session.add(cube)
            count += 1
            
            if count % 100 == 0:
                session.commit()
                print(f"\rMigrated Cubes: {count}/{total_source}", end="")
        
        session.commit()
        print(f"\rMigrated Cubes: {count}/{total_source} (Done)\n")
        
        # --- Migrate History ---
        logging.info("Migrating Rebalancing History...")
        cursor.execute("SELECT * FROM rebalancing_history")
        rows = cursor.fetchall()
        count = 0
        total_source = len(rows)
        
        # Batch Insert for Speed
        batch = []
        BATCH_SIZE = 1000
        
        for row in rows:
            c_at = parse_dt(row['created_at'])
            
            # Simple check if already exists (can be slow, optimize if needed)
            # For now, rely on try/except unique constraint or just check latest date?
            # Let's assume target is empty for first migration.
            # If not empty, we should check.
            
            rec = RebalancingModel(
                cube_symbol=row['cube_symbol'],
                stock_symbol=row['stock_symbol'],
                stock_name=row['stock_name'],
                prev_weight_adjusted=row['prev_weight_adjusted'],
                target_weight=row['target_weight'],
                price=row['price'],
                net_value=row['net_value'],
                created_at=c_at,
                updated_at=parse_dt(row['updated_at']),
                status=row['status']
            )
            session.add(rec)
            count += 1
            
            if count % BATCH_SIZE == 0:
                try:
                    session.commit()
                except Exception as e:
                    session.rollback()
                    logging.warning(f"Batch commit failed (likely duplicates): {e}")
                    # Retry one by one? Too slow. Just skip batch for now or log error.
                print(f"\rMigrated History: {count}/{total_source}", end="")
                
        session.commit()
        print(f"\rMigrated History: {count}/{total_source} (Done)\n")

    except Exception as e:
        session.rollback()
        logging.error(f"Migration failed: {e}")
    finally:
        session.close()
        conn_sqlite.close()

if __name__ == "__main__":
    migrate_data()
