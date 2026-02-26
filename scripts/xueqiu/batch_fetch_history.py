
import json
import os
import time
import random
import subprocess
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/batch_fetch_history.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def run_batch():
    input_file = r"c:\jz_code\Bili_Stock\data\massive_cube_list.json"
    history_dir = r"c:\jz_code\Bili_Stock\data\history"
    temp_file = r"c:\jz_code\Bili_Stock\data\temp_batch_cube.json"
    script_path = r"c:\jz_code\Bili_Stock\scripts\xueqiu\fetch_cube_history.py"
    
    with open(input_file, "r", encoding="utf-8") as f:
        cubes = json.load(f)
        
    logging.info(f"Loaded {len(cubes)} cubes from {input_file}.")
    
    for i, cube in enumerate(cubes):
        symbol = cube["symbol"]
        name = cube["name"]
        
        file_path = os.path.join(history_dir, f"{symbol}.json")
        if os.path.exists(file_path):
            # logging.info(f"[{i+1}/{len(cubes)}] History for {name} ({symbol}) already exists. Skipping.")
            continue
            
        logging.info(f"[{i+1}/{len(cubes)}] Processing {name} ({symbol})...")
        
        # Create temp file for this single cube
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump([cube], f, ensure_ascii=False, indent=2)
            
        # Run fetcher as subprocess
        try:
            cmd = ["python", script_path, temp_file]
            result = subprocess.run(cmd, capture_output=True, text=True, cwd=r"c:\jz_code\Bili_Stock")
            
            if result.returncode != 0:
                logging.error(f"Error running fetcher for {symbol}: {result.stderr}")
            else:
                # Check output for success/failure hints
                # The script logs to stdout/stderr.
                if "Saved" in result.stderr or "Saved" in result.stdout:
                    logging.info(f"Successfully fetched {symbol}.")
                else:
                    logging.warning(f"Fetcher finished but maybe no data for {symbol}. Output: {result.stderr[-200:]}")
                    
        except Exception as e:
            logging.error(f"Exception running subprocess: {e}")
            
        # Sleep to be safe
        sleep_time = random.uniform(5.0, 10.0)
        logging.info(f"Sleeping {sleep_time:.1f}s...")
        time.sleep(sleep_time)
        
    if os.path.exists(temp_file):
        os.remove(temp_file)

if __name__ == "__main__":
    run_batch()
