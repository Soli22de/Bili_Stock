"""
Phase 2 v4: True Go-Flat Choppy (go_flat_choppy=True)
======================================================
v3 used choppy_loss_scale=0.0 which still had a 0.30 floor on losing choppy days.
v4 uses go_flat_choppy=True which zeros ALL choppy periods (no floor).

IC in 震荡 = -0.001 (trading noise). v3 B_goflat_choppy got calmar=0.4802.
True go-flat should do better by eliminating ALL choppy noise.

Experiments:
  A) Baseline (no changes) — reference
  B) True go-flat (go_flat_choppy=True) — zero ALL choppy periods
  C) v3 go-flat repro (choppy_loss_scale=0.0) — apples-to-apples comparison
  D) SRF v2 top25 + true go-flat — combined

All use choppy_fix_B production params:
  hold_step=12, liq_other=0.60, cap_non_up=0.10, cap_up=0.20, non_up_vol_q=0.65
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
    choppy_loss_scale=0.50,
    use_srf=False,
    use_srf_v2=False,
    top_k=None,
    go_flat_choppy=False,
)

EXPERIMENTS = [
    {
        "label": "A_baseline",
        "desc": "Original choppy_fix_B (reference)",
        "risk": {**RISK_BASE},
    },
    {
        "label": "B_goflat_true",
        "desc": "True go-flat: zero ALL 震荡 periods",
        "risk": {**RISK_BASE, "go_flat_choppy": True},
    },
    {
        "label": "C_goflat_v3_repro",
        "desc": "v3 go-flat repro (choppy_loss_scale=0.0, 0.30 floor on losing)",
        "risk": {**RISK_BASE, "choppy_loss_scale": 0.0},
    },
    {
        "label": "D_srfv2_top25_goflat_true",
        "desc": "SRF v2 top25 + true go-flat choppy",
        "risk": {**RISK_BASE, "use_srf_v2": True, "top_k": 25, "go_flat_choppy": True},
    },
]


def main():
    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    os.makedirs(out_dir, exist_ok=True)

    print("Loading panel …", flush=True)
    t0 = time.time()
    panel_raw = _prepare_panel_v5()
    panel, px_map = _enrich_from_stock_data(panel_raw)
    panel = panel[
        (panel["date"] >= pd.Timestamp("2010-01-01"))
        & (panel["date"] <= pd.Timestamp("2025-12-31"))
    ].copy()

    for col in ["vol_price_div5d", "ret_intra5d", "hv20_hv60_ratio"]:
        cov = panel[col].notna().mean() * 100 if col in panel.columns else 0.0
        print(f"  {col}: {cov:.1f}% coverage", flush=True)
    print(f"Panel ready in {time.time()-t0:.1f}s  ({len(panel):,} rows)", flush=True)

    rows = []
    best_calmar = -999.0
    best_label = None
    best_ret = None
    best_hold = None
    best_risk_log = None

    for exp in EXPERIMENTS:
        label = exp["label"]
        print(f"\n[{label}] {exp['desc']}  …", end="", flush=True)
        t1 = time.time()
        cfg = dict(BASE_CFG, risk_cfg=exp["risk"])
        m, ret, hold, _, _, risk_log = _run_one(
            panel, px_map,
            hold_step=cfg["hold_step"],
            liq_other=cfg["liq_other"],
            cap_non_up=cfg["cap_non_up"],
            cap_up=cfg["cap_up"],
            with_takeprofit=cfg["with_takeprofit"],
            risk_cfg=cfg["risk_cfg"],
        )
        calmar  = m.get("calmar", float("nan"))
        ann_ret = m.get("ann_ret", float("nan"))
        mdd     = m.get("mdd", float("nan"))
        sharpe  = m.get("sharpe", float("nan"))
        print(
            f"  calmar={calmar:.4f}  ann_ret={ann_ret*100:.2f}%  mdd={mdd*100:.1f}%"
            f"  sharpe={sharpe:.3f}  ({time.time()-t1:.0f}s)",
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
    grid_path = os.path.join(out_dir, "phase2_v4_grid_2010_2025.csv")
    grid.to_csv(grid_path, index=False, encoding="utf-8-sig")

    cols = ["label", "calmar", "ann_ret", "mdd", "sharpe", "excess", "hit_ratio",
            "震荡_top_bottom", "上涨_top_bottom", "下跌_top_bottom"]
    cols = [c for c in cols if c in grid.columns]
    print(f"\n{'─'*70}")
    print(f"Grid saved → {grid_path}")
    print(grid[cols].to_string(index=False))

    # Save best files
    if best_ret is not None and best_label != "A_baseline":
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
        print(f"\nNext step:")
        print(f"  python research/baseline_v6_1/code/run_gray_daily.py \\")
        print(f"    --baseline {paths['group_ret']}")
    else:
        print(f"\nBaseline still best (calmar={best_calmar:.4f}). No new files saved.")


if __name__ == "__main__":
    main()
