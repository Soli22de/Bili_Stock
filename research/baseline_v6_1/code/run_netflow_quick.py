"""
Quick Net Flow Backtest: Count baseline vs Net Flow (no go-flat)
================================================================
Stripped-down 2-experiment comparison to test the core hypothesis:
  A) Count signal + go-flat choppy (current production)
  C) Net flow + trade ALL days (choppy IC is positive, no need to go flat)

Run: python research/baseline_v6_1/code/run_netflow_quick.py
"""

import os
import sys
import time

import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v6_1.code.run_baseline_v6_v61_suite import _enrich_from_stock_data, _run_one
from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5

BASE_CFG = dict(
    hold_step=12,
    liq_other=0.60,
    cap_non_up=0.10,
    cap_up=0.20,
    with_takeprofit=True,
)

RISK_BASE = dict(
    non_up_vol_q=0.65,
    use_srf=False,
    use_srf_v2=False,
    top_k=None,
    go_flat_choppy=False,
)


def main():
    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    os.makedirs(out_dir, exist_ok=True)

    # ── Load count panel ─────────────────────────────────────────────────
    print("Loading panel (count signal) …", flush=True)
    t0 = time.time()
    panel_count = _prepare_panel_v5(signal_mode="count")
    panel_count = panel_count[
        (panel_count["date"] >= pd.Timestamp("2010-01-01"))
        & (panel_count["date"] <= pd.Timestamp("2025-12-31"))
    ].copy()
    panel_count, px_map_count = _enrich_from_stock_data(panel_count)
    print(f"  Count panel: {len(panel_count):,} rows ({time.time()-t0:.0f}s)", flush=True)

    # ── Load net_flow panel ──────────────────────────────────────────────
    print("Loading panel (net_flow signal) …", flush=True)
    t1 = time.time()
    panel_nf = _prepare_panel_v5(signal_mode="net_flow")
    panel_nf = panel_nf[
        (panel_nf["date"] >= pd.Timestamp("2010-01-01"))
        & (panel_nf["date"] <= pd.Timestamp("2025-12-31"))
    ].copy()
    panel_nf, px_map_nf = _enrich_from_stock_data(panel_nf)
    print(f"  Net flow panel: {len(panel_nf):,} rows ({time.time()-t1:.0f}s)", flush=True)

    experiments = [
        {
            "label": "A_count_goflat",
            "desc": "Count signal + go-flat choppy (current prod)",
            "panel": panel_count,
            "px_map": px_map_count,
            "risk": {**RISK_BASE, "choppy_loss_scale": 0.0},
        },
        {
            "label": "C_netflow_full",
            "desc": "Net flow + trade ALL days (no choppy restriction)",
            "panel": panel_nf,
            "px_map": px_map_nf,
            "risk": {**RISK_BASE, "choppy_loss_scale": 1.0},
        },
    ]

    rows = []
    best_calmar = -999.0
    best_label = None
    best_ret = None
    best_hold = None
    best_risk_log = None

    for exp in experiments:
        label = exp["label"]
        print(f"\n[{label}] {exp['desc']}  …", end="", flush=True)
        t2 = time.time()
        m, ret, hold, _, _, risk_log = _run_one(
            exp["panel"], exp["px_map"],
            hold_step=BASE_CFG["hold_step"],
            liq_other=BASE_CFG["liq_other"],
            cap_non_up=BASE_CFG["cap_non_up"],
            cap_up=BASE_CFG["cap_up"],
            with_takeprofit=BASE_CFG["with_takeprofit"],
            risk_cfg=exp["risk"],
        )
        calmar  = m.get("calmar", float("nan"))
        ann_ret = m.get("ann_ret", float("nan"))
        mdd     = m.get("mdd", float("nan"))
        sharpe  = m.get("sharpe", float("nan"))
        elapsed = time.time() - t2
        print(
            f"  calmar={calmar:.4f}  ann_ret={ann_ret*100:.2f}%  mdd={mdd*100:.1f}%"
            f"  sharpe={sharpe:.3f}  ({elapsed:.0f}s)",
            flush=True,
        )
        rows.append({"label": label, "desc": exp["desc"], **m})

        if pd.notna(calmar) and calmar > best_calmar:
            best_calmar = calmar
            best_label = label
            best_ret = ret
            best_hold = hold
            best_risk_log = risk_log

    # Save grid
    grid = pd.DataFrame(rows).sort_values("calmar", ascending=False)
    grid_path = os.path.join(out_dir, "netflow_quick_grid_2010_2025.csv")
    grid.to_csv(grid_path, index=False, encoding="utf-8-sig")

    cols = ["label", "calmar", "ann_ret", "mdd", "sharpe", "excess", "hit_ratio",
            "震荡_top_bottom", "上涨_top_bottom", "下跌_top_bottom"]
    cols = [c for c in cols if c in grid.columns]
    print(f"\n{'─'*70}")
    print(f"Grid saved → {grid_path}")
    print(grid[cols].to_string(index=False))

    # Save best files
    if best_ret is not None:
        tag = f"choppy_fix_B_hold12_cap10_{best_label}"
        paths = {
            "group_ret": os.path.join(out_dir, f"{tag}_group_ret_2010_2025.csv"),
            "holdings":  os.path.join(out_dir, f"{tag}_holdings_2010_2025.csv"),
            "risk_log":  os.path.join(out_dir, f"{tag}_risk_log_2010_2025.csv"),
        }
        best_ret.to_csv(paths["group_ret"], index=False, encoding="utf-8-sig")
        if best_hold is not None:
            best_hold.to_csv(paths["holdings"], index=False, encoding="utf-8-sig")
        if best_risk_log is not None:
            best_risk_log.to_csv(paths["risk_log"], index=False, encoding="utf-8-sig")
        print(f"\nBest: {best_label}  calmar={best_calmar:.4f}")
        for k, p in paths.items():
            print(f"  {k:10s} → {p}")


if __name__ == "__main__":
    main()
