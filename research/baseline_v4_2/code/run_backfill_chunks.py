import os
import subprocess
import sys


def main():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    script = os.path.join(root, "research", "baseline_v4_2", "code", "backfill_stock_data_2019_2021.py")
    chunks = [0, 150, 300, 450, 600, 750]
    for c in chunks:
        print(f"chunk={c}")
        env = os.environ.copy()
        env["START_INDEX"] = str(c)
        env["MAX_FILES"] = "150"
        p = subprocess.run([sys.executable, script], cwd=root, env=env, capture_output=True, text=True)
        print(p.stdout)
        if p.returncode != 0:
            print(p.stderr)
            raise SystemExit(p.returncode)


if __name__ == "__main__":
    main()
