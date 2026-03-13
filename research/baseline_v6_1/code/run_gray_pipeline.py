import argparse
import os
import subprocess
import sys
from datetime import datetime


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def _run(cmd: list[str], cwd: str) -> tuple[int, str]:
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    out = (p.stdout or "") + (p.stderr or "")
    return int(p.returncode), out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live-dir", default=os.path.join(ROOT, "research", "baseline_v6_1", "output", "live"))
    ap.add_argument("--baseline", default=os.path.join(ROOT, "research", "baseline_v6_1", "output", "base_E_foundation_group_ret_2010_2025.csv"))
    ap.add_argument("--cycle-bars", type=int, default=12)
    ap.add_argument("--weekly-bars", type=int, default=5)
    ap.add_argument("--bootstrap-sample-days", type=int, default=0)
    ap.add_argument("--auto-fill-from-baseline", type=int, default=1)
    ap.add_argument("--stale-days", type=int, default=30)
    ap.add_argument("--strict-live-freshness", type=int, default=1)
    args = ap.parse_args()

    os.makedirs(args.live_dir, exist_ok=True)
    log_path = os.path.join(args.live_dir, "pipeline_run_log.md")
    status_path = os.path.join(args.live_dir, "pipeline_status.csv")

    daily_cmd = [
        sys.executable,
        os.path.join(ROOT, "research", "baseline_v6_1", "code", "run_gray_daily.py"),
        "--live-dir",
        args.live_dir,
        "--baseline",
        args.baseline,
        "--cycle-bars",
        str(args.cycle_bars),
        "--auto-fill-from-baseline",
        str(args.auto_fill_from_baseline),
        "--stale-days",
        str(args.stale_days),
        "--strict-live-freshness",
        str(args.strict_live_freshness),
    ]
    if int(args.bootstrap_sample_days) > 0:
        daily_cmd.extend(["--bootstrap-sample-days", str(args.bootstrap_sample_days)])

    weekly_cmd = [
        sys.executable,
        os.path.join(ROOT, "research", "baseline_v6_1", "code", "run_gray_weekly.py"),
        "--live-dir",
        args.live_dir,
        "--baseline-ret",
        args.baseline,
        "--weekly-bars",
        str(args.weekly_bars),
    ]

    t0 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    d_code, d_out = _run(daily_cmd, ROOT)
    w_code, w_out = _run(weekly_cmd, ROOT) if d_code == 0 else (999, "skip weekly because daily failed")
    ok = d_code == 0 and w_code == 0

    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"\n## {t0}\n")
        f.write(f"- daily_exit={d_code}\n")
        f.write(f"- weekly_exit={w_code}\n")
        f.write("\n### daily_output\n")
        f.write("```\n")
        f.write(d_out[-6000:])
        f.write("\n```\n")
        f.write("\n### weekly_output\n")
        f.write("```\n")
        f.write(w_out[-6000:])
        f.write("\n```\n")

    import pandas as pd

    pd.DataFrame(
        [
            {
                "run_time": t0,
                "daily_exit": d_code,
                "weekly_exit": w_code,
                "pipeline_ok": bool(ok),
                "fallback_keep_last_decision": bool(d_code != 0),
            }
        ]
    ).to_csv(status_path, index=False, encoding="utf-8-sig")

    print(status_path)
    print(log_path)


if __name__ == "__main__":
    main()
