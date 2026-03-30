import sys
import os
import subprocess
import logging
import sqlite3
import pandas as pd
from datetime import datetime

# Setup logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, f"daily_job_{datetime.now().strftime('%Y%m%d')}.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

def run_script(script_path, description):
    logging.info(f"--- Starting {description} ---")
    start_time = datetime.now()
    
    if not os.path.exists(script_path):
        logging.error(f"Script not found: {script_path}")
        return None

    try:
        # Use sys.executable to ensure we use the same python interpreter
        # Capture both stdout and stderr (logging often goes to stderr)
        result = subprocess.run(
            [sys.executable, script_path],
            capture_output=True,
            text=True,
            encoding='utf-8',
            check=False # Don't raise exception immediately, check returncode manually
        )
        
        if result.returncode == 0:
            logging.info(f"{description} completed successfully.")
        else:
            logging.error(f"{description} FAILED with return code {result.returncode}")

        # Log output
        full_output = result.stdout + "\n" + result.stderr
        logging.info("Output:\n" + full_output)
            
        duration = datetime.now() - start_time
        logging.info(f"{description} took {duration}")
        return full_output
        
    except Exception as e:
        logging.error(f"{description} FAILED with error: {e}")
        return None


def is_rebalancing_data_fresh(db_path, max_age_hours=36):
    if not os.path.exists(db_path):
        logging.error(f"DB not found: {db_path}")
        return False
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute("SELECT MAX(created_at) FROM rebalancing_history").fetchone()
        latest = row[0] if row else None
    finally:
        conn.close()
    if not latest:
        logging.error("No rebalancing_history found.")
        return False
    latest_dt = pd.to_datetime(latest, errors="coerce")
    if pd.isna(latest_dt):
        logging.error(f"Invalid latest created_at: {latest}")
        return False
    age_hours = (datetime.now() - latest_dt.to_pydatetime()).total_seconds() / 3600
    if age_hours > max_age_hours:
        logging.error(f"Stale rebalancing data: latest={latest_dt}, age_hours={age_hours:.1f}")
        return False
    logging.info(f"Freshness check passed: latest={latest_dt}, age_hours={age_hours:.1f}")
    return True

def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    xueqiu_scripts_dir = os.path.join(base_dir, "scripts", "xueqiu")
    
    logging.info("Starting Daily Job Pipeline...")
    
    # 1. Fetch Latest Data (Xueqiu)
    fetch_script = os.path.join(xueqiu_scripts_dir, "fetch_history_retry.py")
    if not run_script(fetch_script, "Data Fetching (fetch_history_retry.py)"):
        logging.error("Aborting pipeline due to data fetch failure.")
        return

    db_path = os.path.join(base_dir, "data", "cubes.db")
    if not is_rebalancing_data_fresh(db_path):
        logging.error("Aborting pipeline due to stale or invalid Xueqiu data.")
        return

    smart_script = os.path.join(xueqiu_scripts_dir, "strategy_smart_momentum.py")
    smart_output = run_script(smart_script, "Smart Momentum Backtest (strategy_smart_momentum.py)")

    # 2. Run Mainboard Strategy (Small Cap Alpha)
    # This runs the backtest/signal generation on the latest data
    mainboard_script = os.path.join(xueqiu_scripts_dir, "strategy_mainboard_smallcap.py")
    mb_output = run_script(mainboard_script, "Mainboard Strategy (strategy_mainboard_smallcap.py)")
    
    # 3. Run Rolling Strategy (Daily Trader Bot)
    # This runs the paper trader for the rolling strategy
    rolling_script = os.path.join(xueqiu_scripts_dir, "daily_trader_bot.py")
    rolling_output = run_script(rolling_script, "Rolling Strategy (daily_trader_bot.py)")
    
    # 4. Generate Console Summary
    print("\n" + "="*50)
    print(f"DAILY REPORT {datetime.now().strftime('%Y-%m-%d')}")
    print("="*50)

    if smart_output:
        if "SMART MOMENTUM BACKTEST REPORT" in smart_output:
            idx = smart_output.find("SMART MOMENTUM BACKTEST REPORT")
            print("\n[Smart Momentum Performance]")
            print(smart_output[idx:])
        else:
            print("\n[Smart Momentum Output (Last 1000 chars)]")
            print(smart_output[-1000:])
    
    if mb_output:
        # Try to find the start of the report in the output
        if "SMALL ACCOUNT (MAINBOARD) REPORT" in mb_output:
             idx = mb_output.find("SMALL ACCOUNT (MAINBOARD) REPORT")
             print("\n[Mainboard Strategy Performance]")
             print(mb_output[idx:])
        else:
             print("\n[Mainboard Strategy Output (Last 1000 chars)]")
             print(mb_output[-1000:])

    if rolling_output:
         print("\n[Rolling Strategy Output (Last 1000 chars)]")
         print(rolling_output[-1000:])

if __name__ == "__main__":
    main()
