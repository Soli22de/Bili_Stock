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
        # Phase 2 winner: choppy_loss_scale=0.0 keeps full exposure on winning
        # choppy days but caps losers at 30% floor — calmar 0.073→0.480 (+557%)
        # True go-flat (go_flat_choppy=True) was tested and is worse (calmar=0.208)
        # because it discards winning choppy periods along with the losers.
        choppy_loss_scale=0.0,
        use_srf=False,
        top_k=None,
        go_flat_choppy=False,
    ),
)

# ── Baseline file tag (drives all three file names) ───────────────────────────
BASELINE_TAG = "choppy_fix_B_hold12_cap10"

# Phase 2 winner tag — B_goflat_choppy from v3 grid (calmar=0.4802, 2010-2025)
PHASE2_TAG: str | None = "choppy_fix_B_hold12_cap10_B_goflat_choppy"

ACTIVE_TAG = PHASE2_TAG or BASELINE_TAG


def baseline_paths(tag: str = ACTIVE_TAG, out_dir: str = OUT_DIR) -> dict:
    """Return the three file paths for a given baseline tag."""
    return {
        "group_ret": os.path.join(out_dir, f"{tag}_group_ret_2010_2025.csv"),
        "holdings": os.path.join(out_dir, f"{tag}_holdings_2010_2025.csv"),
        "risk_log": os.path.join(out_dir, f"{tag}_risk_log_2010_2025.csv"),
    }


ACTIVE_PATHS = baseline_paths()
