import json
import os
import sys
import psutil

def check_cube_count():
    file_path = "data/massive_cube_list.json"
    if not os.path.exists(file_path):
        print(f"File {file_path} not found.")
        return 0
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                print(f"Current cube count in {file_path}: {len(data)}")
                return len(data)
            else:
                print(f"File content is not a list. Type: {type(data)}")
                return 0
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0

def check_running_process():
    target_script = "fetch_massive_cubes.py"
    found = False
    print("\nChecking for running processes...")
    
    # Try using psutil if available
    try:
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = proc.info['cmdline']
                if cmdline and any(target_script in arg for arg in cmdline):
                    print(f"Found running process: PID={proc.info['pid']}, Command={' '.join(cmdline)}")
                    found = True
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
    except ImportError:
        print("psutil not installed, falling back to tasklist...")
        # Fallback to tasklist for Windows
        output = os.popen('tasklist /v /fi "imagename eq python.exe"').read()
        if target_script in output:
             print("Found running process in tasklist output.")
             found = True
        else:
             print(f"Process {target_script} not found in tasklist.")

    if not found:
        print(f"No running process found for {target_script}.")

if __name__ == "__main__":
    check_cube_count()
    check_running_process()
