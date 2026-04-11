"""
Production Config — Single Source of Truth
==========================================
Import this wherever you need production params.

Usage:
    from research.baseline_v6_1.prod_config import PROD, baseline_paths
"""
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
OUT_DIR = os.path.join(ROOT, "research", "baseline_v6_1", "output")

# ── Active production branch ──────────────────────────────────────────────────
PROD = dict(
    hold_step=12,
    liq_other=0.60,
    cap_non_up=0.10,
    cap_up=0.20,
    with_takeprofit=True,
    risk_cfg=dict(
        non_up_vol_q=0.65,
        choppy_loss_scale=0.50,
        # Phase 2 SRF — set use_srf=True and top_k=N once grid winner is known
        use_srf=False,
        top_k=None,
    ),
)

# ── Baseline file tag (drives all three file names) ───────────────────────────
BASELINE_TAG = "choppy_fix_B_hold12_cap10"

# Phase 2 winner tag — update this after grid search completes, e.g.:
#   PHASE2_TAG = "choppy_fix_B_hold12_cap10_srf_top10"
PHASE2_TAG: str | None = None

ACTIVE_TAG = PHASE2_TAG or BASELINE_TAG


def baseline_paths(tag: str = ACTIVE_TAG, out_dir: str = OUT_DIR) -> dict:
    """Return the three file paths for a given baseline tag."""
    return {
        "group_ret": os.path.join(out_dir, f"{tag}_group_ret_2010_2025.csv"),
        "holdings": os.path.join(out_dir, f"{tag}_holdings_2010_2025.csv"),
        "risk_log": os.path.join(out_dir, f"{tag}_risk_log_2010_2025.csv"),
    }


ACTIVE_PATHS = baseline_paths()
