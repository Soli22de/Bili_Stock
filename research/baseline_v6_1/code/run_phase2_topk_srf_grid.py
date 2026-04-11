"""
Phase 2: Top-K SRF Grid Search
================================
Tests SmartResonanceFactor (SRF) scoring + Top-K selection on top of the
production choppy_fix_B branch (hold_step=12, cap_non_up=0.10, non_up_vol_q=0.65,
choppy_loss_scale=0.50).

Grid:
  use_srf : [True]          — always SRF on in this sweep
  top_k   : [5, 8, 10, 12, 15, 20, None]
             None = keep existing 30%-threshold behaviour as baseline

Output:
  research/baseline_v6_1/output/phase2_topk_srf_grid_YYYY_YYYY.csv
  research/baseline_v6_1/output/phase2_topk_srf_best_<top_k>_hold_ret_*.csv
"""

import itertools
import os
import sys

import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v6_1.code.run_baseline_v6_v61_suite import _enrich_from_stock_data, _run_one
from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5

# ── choppy_fix_B production params ───────────────────────────────────────────
PROD_CFG = dict(
    hold_step=12,
    liq_other=0.60,
    cap_non_up=0.10,
    cap_up=0.20,
    with_takeprofit=True,
    risk_cfg=dict(
        non_up_vol_q=0.65,
        choppy_loss_scale=0.50,
    ),
)

# ── Phase 2 grid ──────────────────────────────────────────────────────────────
TOP_K_VALUES = [5, 8, 10, 12, 15, 20, None]   # None = baseline (no SRF, rank>=0.7)


def _label(top_k):
    return f"top{top_k}" if top_k is not None else "baseline"


def main():
    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    os.makedirs(out_dir, exist_ok=True)

    print("Loading panel …")
    panel_raw = _prepare_panel_v5()
    panel, px_map = _enrich_from_stock_data(panel_raw)
    panel = panel[
        (panel["date"] >= pd.Timestamp("2010-01-01"))
        & (panel["date"] <= pd.Timestamp("2025-12-31"))
    ].copy()

    rows = []
    best_calmar = -999.0
    best_label = None
    best_ret = None
    best_hold = None
    best_risk = None

    for top_k in TOP_K_VALUES:
        use_srf = top_k is not None  # baseline row uses original logic
        label = _label(top_k)
        cfg = dict(PROD_CFG)
        cfg["risk_cfg"] = dict(PROD_CFG["risk_cfg"])
        cfg["risk_cfg"]["top_k"] = top_k
        cfg["risk_cfg"]["use_srf"] = use_srf

        print(f"  Running {label} (use_srf={use_srf}) …", end="", flush=True)
        m, ret, hold, _, _, risk_log = _run_one(
            panel,
            px_map,
            hold_step=cfg["hold_step"],
            liq_other=cfg["liq_other"],
            cap_non_up=cfg["cap_non_up"],
            cap_up=cfg["cap_up"],
            with_takeprofit=cfg["with_takeprofit"],
            risk_cfg=cfg["risk_cfg"],
        )
        calmar = m.get("calmar", float("nan"))
        print(f"  calmar={calmar:.4f}")
        rows.append({"label": label, "top_k": str(top_k), "use_srf": use_srf, **m})

        if pd.notna(calmar) and calmar > best_calmar:
            best_calmar = calmar
            best_label = label
            best_ret = ret
            best_hold = hold
            best_risk = risk_log

    grid = pd.DataFrame(rows).sort_values("calmar", ascending=False)
    grid_path = os.path.join(out_dir, "phase2_topk_srf_grid_2010_2025.csv")
    grid.to_csv(grid_path, index=False, encoding="utf-8-sig")
    print(f"\nGrid saved → {grid_path}")
    print(grid[["label", "top_k", "calmar", "excess", "mdd"]].to_string(index=False))

    if best_ret is not None:
        tag = f"srf_{best_label}"
        ret_path = os.path.join(out_dir, f"choppy_fix_B_hold12_cap10_{tag}_group_ret_2010_2025.csv")
        hold_path = os.path.join(out_dir, f"choppy_fix_B_hold12_cap10_{tag}_holdings_2010_2025.csv")
        risk_path = os.path.join(out_dir, f"choppy_fix_B_hold12_cap10_{tag}_risk_log_2010_2025.csv")
        best_ret.to_csv(ret_path, index=False, encoding="utf-8-sig")
        if best_hold is not None:
            best_hold.to_csv(hold_path, index=False, encoding="utf-8-sig")
        if best_risk is not None:
            best_risk.to_csv(risk_path, index=False, encoding="utf-8-sig")
        print(f"\nBest ({best_label}, calmar={best_calmar:.4f}):")
        print(f"  group_ret  → {ret_path}")
        print(f"  holdings   → {hold_path}")
        print(f"  risk_log   → {risk_path}")


if __name__ == "__main__":
    main()
