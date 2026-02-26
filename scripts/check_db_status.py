import sqlite3
import os
from datetime import datetime

def check_db():
    db_path = r"c:\jz_code\Bili_Stock\data\cubes.db"
    if not os.path.exists(db_path):
        print(f"Database file not found at {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Count total cubes
        cursor.execute("SELECT COUNT(*) FROM cubes")
        total_count = cursor.fetchone()[0]
        
        # Get last update time (file modification time)
        file_mtime = datetime.fromtimestamp(os.path.getmtime(db_path))
        
        print(f"Database Path: {db_path}")
        print(f"Total Cubes: {total_count}")
        print(f"Last DB File Write: {file_mtime}")
        
        # Get count of cubes added today (if created_at is reliable)
        # Note: created_at in DB is timestamp of cube creation on platform, not insertion time.
        # We can check updated_at which we set on insertion/update.
        cursor.execute("SELECT COUNT(*) FROM cubes WHERE date(updated_at) = date('now')")
        updated_today = cursor.fetchone()[0]
        print(f"Cubes Updated/Added Today: {updated_today}")

        conn.close()
        
    except Exception as e:
        print(f"Error checking DB: {e}")

if __name__ == "__main__":
    check_db()
