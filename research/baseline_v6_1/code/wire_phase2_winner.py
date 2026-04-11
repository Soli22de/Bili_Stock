"""
wire_phase2_winner.py — Auto-wire Phase 2 Grid Winner
======================================================
Run this after run_phase2_topk_srf_grid.py completes.

What it does:
  1. Reads phase2_topk_srf_grid_2010_2025.csv
  2. Picks the best valid config (highest calmar, not 0.0)
  3. Validates the three baseline files exist
  4. Patches prod_config.py with the new PHASE2_TAG and PROD params
  5. Prints a summary + next-step command

Usage:
    python research/baseline_v6_1/code/wire_phase2_winner.py
    python research/baseline_v6_1/code/wire_phase2_winner.py --dry-run
"""
import argparse
import os
import re
import sys

import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
OUT_DIR = os.path.join(ROOT, "research", "baseline_v6_1", "output")
PROD_CONFIG = os.path.join(ROOT, "research", "baseline_v6_1", "prod_config.py")
GRID_CSV = os.path.join(OUT_DIR, "phase2_topk_srf_grid_2010_2025.csv")


def _pick_winner(grid: pd.DataFrame) -> dict | None:
    """
    Select the best SRF config:
      - Must have use_srf=True (not the baseline row)
      - Must have calmar > 0 (not degenerate/zeroed)
      - Highest calmar
    Falls back to highest calmar among all SRF rows if none > 0.
    """
    srf = grid[grid["use_srf"] == True].copy()
    if srf.empty:
        return None
    positive = srf[srf["calmar"] > 0]
    pool = positive if not positive.empty else srf
    pool = pool.sort_values("calmar", ascending=False)
    return pool.iloc[0].to_dict()


def _baseline_files_exist(tag: str) -> tuple[bool, list[str]]:
    paths = [
        os.path.join(OUT_DIR, f"{tag}_group_ret_2010_2025.csv"),
        os.path.join(OUT_DIR, f"{tag}_holdings_2010_2025.csv"),
        os.path.join(OUT_DIR, f"{tag}_risk_log_2010_2025.csv"),
    ]
    missing = [p for p in paths if not os.path.exists(p)]
    return len(missing) == 0, missing


def _patch_prod_config(winner: dict, tag: str, dry_run: bool) -> str:
    """Patch PHASE2_TAG and PROD risk_cfg in prod_config.py."""
    with open(PROD_CONFIG, "r", encoding="utf-8") as f:
        src = f.read()

    top_k = winner["top_k"]
    top_k_val = "None" if str(top_k) in ("None", "nan") else str(int(float(top_k)))
    use_srf = str(winner.get("use_srf", True)).capitalize()  # True/False

    # Patch PHASE2_TAG line
    src = re.sub(
        r'PHASE2_TAG\s*:\s*str \| None\s*=\s*.*',
        f'PHASE2_TAG: str | None = "{tag}"',
        src,
    )
    # Patch use_srf inside PROD
    src = re.sub(r'(use_srf\s*=\s*)(?:True|False)', rf'\g<1>{use_srf}', src)
    # Patch top_k inside PROD
    src = re.sub(r'(top_k\s*=\s*)(?:None|\d+)', rf'\g<1>{top_k_val}', src)

    if not dry_run:
        with open(PROD_CONFIG, "w", encoding="utf-8") as f:
            f.write(src)
    return src


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    args = ap.parse_args()

    # ── 1. Load grid ──────────────────────────────────────────────────────────
    if not os.path.exists(GRID_CSV):
        print(f"ERROR: grid CSV not found: {GRID_CSV}")
        print("Run run_phase2_topk_srf_grid.py first.")
        sys.exit(1)

    grid = pd.read_csv(GRID_CSV)
    print(f"Loaded grid: {len(grid)} rows\n")
    print(grid[["label", "top_k", "use_srf", "calmar", "excess", "mdd"]].to_string(index=False))
    print()

    # ── 2. Pick winner ────────────────────────────────────────────────────────
    winner = _pick_winner(grid)
    if winner is None:
        print("ERROR: No valid SRF config found in grid.")
        sys.exit(1)

    label = str(winner["label"])
    tag = f"choppy_fix_B_hold12_cap10_srf_{label}"
    print(f"Winner: {label}  calmar={winner['calmar']:.4f}  tag={tag}")

    # ── 3. Validate files ─────────────────────────────────────────────────────
    ok, missing = _baseline_files_exist(tag)
    if not ok:
        print(f"\nERROR: Missing baseline files:")
        for m in missing:
            print(f"  {m}")
        print("\nRe-run run_phase2_topk_srf_grid.py — it saves all three files for the winner.")
        sys.exit(1)
    print("Baseline files: all present")

    # ── 4. Patch prod_config.py ───────────────────────────────────────────────
    patched = _patch_prod_config(winner, tag, dry_run=args.dry_run)
    if args.dry_run:
        print(f"\n[DRY RUN] Would write prod_config.py:\n{'─'*60}")
        print(patched)
    else:
        print(f"\nPatched {PROD_CONFIG}")
        print(f"  PHASE2_TAG = \"{tag}\"")
        print(f"  use_srf    = {winner.get('use_srf', True)}")
        print(f"  top_k      = {winner['top_k']}")

    # ── 5. Next-step command ──────────────────────────────────────────────────
    print(f"\nNext step — run gray pipeline with Phase 2 baseline:")
    print(f"  python research/baseline_v6_1/code/run_gray_daily.py \\")
    print(f"    --baseline research/baseline_v6_1/output/{tag}_group_ret_2010_2025.csv")
    print(f"\nOr full pipeline:")
    print(f"  python research/baseline_v6_1/code/run_gray_pipeline.py \\")
    print(f"    --baseline research/baseline_v6_1/output/{tag}_group_ret_2010_2025.csv")


if __name__ == "__main__":
    main()
