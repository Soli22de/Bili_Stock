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
        # LONG-ONLY A-share backtest with realistic costs (2010-2025):
        #   ann_ret=22.9%, MDD=-23.0%, calmar=0.99, sharpe=1.32
        #   Annual trading cost: 9.8% (83% turnover × 56bp round-trip)
        #   Win rate: 58.8% (excluding go-flat), 1/16 years negative
        #
        # Cost model: buy 13bp + sell 43bp = 56bp round-trip (asymmetric)
        # hold_step=12 is stable in 10-15 range; 16-20 is unstable (overfit risk)
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
        buy_cost=0.0013,   # 13bp: commission 3bp + transfer 0.2bp + slippage 10bp
        sell_cost=0.0043,   # 43bp: commission 3bp + stamp 10bp + transfer 0.2bp + slippage 10bp + impact 20bp
    ),
)

# ── Baseline file tag ─────────────────────────────────────────────────────────
BASELINE_TAG = "choppy_fix_B_hold12_cap10"
PHASE2_TAG: str | None = "choppy_fix_B_hold12_cap10_D_srfv2_top15_goflat"

ACTIVE_TAG = PHASE2_TAG or BASELINE_TAG


def baseline_paths(tag: str = ACTIVE_TAG, out_dir: str = OUT_DIR) -> dict:
    """Return the three file paths for a given baseline tag."""
    return {
        "group_ret": os.path.join(out_dir, f"{tag}_group_ret_2010_2025.csv"),
        "holdings": os.path.join(out_dir, f"{tag}_holdings_2010_2025.csv"),
        "risk_log": os.path.join(out_dir, f"{tag}_risk_log_2010_2025.csv"),
    }


ACTIVE_PATHS = baseline_paths()
