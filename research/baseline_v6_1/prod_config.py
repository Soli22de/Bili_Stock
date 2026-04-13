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
        # Phase 4 final: calmar=1.128, ann_ret=18.8%, MDD=-16.7%, sharpe=1.478
        # Validated on complete data (3767 stocks, 508K panel rows, 2010-2025)
        #
        # Key optimizations:
        #   regime ±3% (was ±2%): calmar 0.626→1.036
        #   vol filter 0.65→0.50: calmar +4%
        #   drawdown brake tightened: MDD -16.5%→-15.6%
        #   SRF v2 top_k=15 (was 25): calmar +20%
        #   highconv_10d as 6th SRF factor: +1.8%
        non_up_vol_q=0.50,
        dd_soft=-0.05,
        dd_mid=-0.07,
        dd_hard=-0.10,
        choppy_loss_scale=0.0,
        choppy_loss_floor=0.0,
        use_srf=False,
        use_srf_v2=True,
        top_k=15,
        go_flat_choppy=False,
    ),
)

# ── Baseline file tag (drives all three file names) ───────────────────────────
BASELINE_TAG = "choppy_fix_B_hold12_cap10"

# Phase 4 winner — SRF v2 top25 + go-flat (calmar=0.513 on complete data)
PHASE2_TAG: str | None = "choppy_fix_B_hold12_cap10_D_srfv2_top25_goflat"

ACTIVE_TAG = PHASE2_TAG or BASELINE_TAG


def baseline_paths(tag: str = ACTIVE_TAG, out_dir: str = OUT_DIR) -> dict:
    """Return the three file paths for a given baseline tag."""
    return {
        "group_ret": os.path.join(out_dir, f"{tag}_group_ret_2010_2025.csv"),
        "holdings": os.path.join(out_dir, f"{tag}_holdings_2010_2025.csv"),
        "risk_log": os.path.join(out_dir, f"{tag}_risk_log_2010_2025.csv"),
    }


ACTIVE_PATHS = baseline_paths()
