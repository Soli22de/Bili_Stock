"""
Phase 2 v2: SRF-v2 Re-ranker Grid Search
==========================================
Research-backed redesign after v1 grid showed all SRF top-K configs underperform baseline.

Root causes identified:
  1. v1 used raw momentum (ret20d) — hurts in choppy A-shares (momentum crash, T+1 asymmetry)
  2. v1 used raw volume (amount) — near-zero IC in sideways markets
  3. v1 replaced the Xueqiu gate entirely — destroyed the primary alpha source

SRF v2 design:
  - Re-ranks WITHIN the Xueqiu top-30% gate (rank >= 0.7) — gate is preserved
  - 60% factor_z_neu  — Xueqiu consensus (dominant, proven alpha)
  - 25% -ret20d_stock — reversal (contrarian, correct sign for choppy A-shares)
  - 15% vol_price_div5d — 量价背离, -corr(close,vol,5d), Guojin 2022 IC 4-6%

Grid:
  top_k: [None, 20, 25, 30]  — None = all top-30% re-ranked (pure ordering test)
  baseline row: use_srf_v2=False (original rank>=0.7, no re-ranking)

Output:
  research/baseline_v6_1/output/phase2_v2_srf_grid_2010_2025.csv
  Best config files saved with tag choppy_fix_B_hold12_cap10_srfv2_{label}
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

# Grid: None = all top-30% re-ranked (ordering-only test)
TOP_K_VALUES = [None, 20, 25, 30]


def _label(top_k: int | None) -> str:
    return f"top{top_k}" if top_k is not None else "all"


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
    print(f"Panel ready in {time.time()-t0:.1f}s  "
          f"({len(panel):,} rows, vol_price_div5d coverage: "
          f"{panel['vol_price_div5d'].notna().mean()*100:.1f}%)", flush=True)

    rows = []
    best_calmar = -999.0
    best_label = None
    best_ret = None
    best_hold = None
    best_risk = None

    # ── Baseline row (original logic, no SRF v2) ─────────────────────────────
    print("\n[baseline] use_srf_v2=False  …", end="", flush=True)
    t1 = time.time()
    cfg = dict(PROD_CFG)
    cfg["risk_cfg"] = dict(PROD_CFG["risk_cfg"])
    cfg["risk_cfg"]["use_srf_v2"] = False
    cfg["risk_cfg"]["top_k"] = None
    m, ret, hold, _, _, risk_log = _run_one(
        panel, px_map,
        hold_step=cfg["hold_step"], liq_other=cfg["liq_other"],
        cap_non_up=cfg["cap_non_up"], cap_up=cfg["cap_up"],
        with_takeprofit=cfg["with_takeprofit"], risk_cfg=cfg["risk_cfg"],
    )
    calmar = m.get("calmar", float("nan"))
    print(f"  calmar={calmar:.4f}  ({time.time()-t1:.0f}s)", flush=True)
    rows.append({"label": "baseline", "top_k": "None", "use_srf_v2": False, **m})
    if pd.notna(calmar) and calmar > best_calmar:
        best_calmar, best_label, best_ret, best_hold, best_risk = calmar, "baseline", ret, hold, risk_log

    # ── SRF v2 configs ────────────────────────────────────────────────────────
    for top_k in TOP_K_VALUES:
        label = _label(top_k)
        print(f"\n[srfv2_{label}] top_k={top_k}  …", end="", flush=True)
        t1 = time.time()
        cfg = dict(PROD_CFG)
        cfg["risk_cfg"] = dict(PROD_CFG["risk_cfg"])
        cfg["risk_cfg"]["use_srf_v2"] = True
        cfg["risk_cfg"]["top_k"] = top_k
        m, ret, hold, _, _, risk_log = _run_one(
            panel, px_map,
            hold_step=cfg["hold_step"], liq_other=cfg["liq_other"],
            cap_non_up=cfg["cap_non_up"], cap_up=cfg["cap_up"],
            with_takeprofit=cfg["with_takeprofit"], risk_cfg=cfg["risk_cfg"],
        )
        calmar = m.get("calmar", float("nan"))
        print(f"  calmar={calmar:.4f}  ({time.time()-t1:.0f}s)", flush=True)
        rows.append({"label": f"srfv2_{label}", "top_k": str(top_k), "use_srf_v2": True, **m})
        if pd.notna(calmar) and calmar > best_calmar:
            best_calmar, best_label, best_ret, best_hold, best_risk = calmar, f"srfv2_{label}", ret, hold, risk_log

    # ── Save grid CSV ─────────────────────────────────────────────────────────
    grid = pd.DataFrame(rows).sort_values("calmar", ascending=False)
    grid_path = os.path.join(out_dir, "phase2_v2_srf_grid_2010_2025.csv")
    grid.to_csv(grid_path, index=False, encoding="utf-8-sig")
    print(f"\n{'─'*60}")
    print(f"Grid saved → {grid_path}")
    display_cols = ["label", "top_k", "calmar", "ann_ret", "mdd", "sharpe", "excess", "hit_ratio"]
    display_cols = [c for c in display_cols if c in grid.columns]
    print(grid[display_cols].to_string(index=False))

    # ── Save best config files ────────────────────────────────────────────────
    if best_ret is not None and best_label != "baseline":
        tag = f"choppy_fix_B_hold12_cap10_{best_label}"
        paths = {
            "group_ret": os.path.join(out_dir, f"{tag}_group_ret_2010_2025.csv"),
            "holdings":  os.path.join(out_dir, f"{tag}_holdings_2010_2025.csv"),
            "risk_log":  os.path.join(out_dir, f"{tag}_risk_log_2010_2025.csv"),
        }
        best_ret.to_csv(paths["group_ret"], index=False, encoding="utf-8-sig")
        if best_hold is not None:
            best_hold.to_csv(paths["holdings"], index=False, encoding="utf-8-sig")
        if best_risk is not None:
            best_risk.to_csv(paths["risk_log"], index=False, encoding="utf-8-sig")
        print(f"\nBest: {best_label}  calmar={best_calmar:.4f}")
        for k, p in paths.items():
            print(f"  {k:10s} → {p}")
        print(f"\nNext step — run gray pipeline with Phase 2 v2 baseline:")
        print(f"  python research/baseline_v6_1/code/run_gray_daily.py \\")
        print(f"    --baseline {paths['group_ret']}")
    elif best_label == "baseline":
        print(f"\nBaseline still wins (calmar={best_calmar:.4f}). SRF v2 re-ranking did not improve.")
        print("Consider tuning factor weights or widening the gate threshold.")


if __name__ == "__main__":
    main()
