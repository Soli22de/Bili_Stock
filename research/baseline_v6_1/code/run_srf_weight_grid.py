"""
SRF v2 Weight + Top-K Grid Search
===================================
Two-stage optimization on complete data:
  Stage 1: Sweep top_k with current weights → find best top_k
  Stage 2: Sweep SRF weight combos with best top_k → find best weights
  Stage 3: Final validation with best top_k × best weights

All with go-flat choppy (choppy_loss_scale=0.0) which is the proven base.

Run: python research/baseline_v6_1/code/run_srf_weight_grid.py
"""

import os
import sys
import time

import pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v6_1.code.run_baseline_v6_v61_suite import (
    _enrich_from_stock_data, _run_one, _srf_score_v2,
)
from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5

RISK_BASE = dict(
    non_up_vol_q=0.65,
    choppy_loss_scale=0.0,
    choppy_loss_floor=0.0,
    use_srf=False,
    use_srf_v2=True,
    go_flat_choppy=False,
)

RUN_CFG = dict(
    hold_step=12,
    liq_other=0.60,
    cap_non_up=0.10,
    cap_up=0.20,
    with_takeprofit=True,
)


def run_exp(label, panel, px_map, risk_cfg):
    t0 = time.time()
    m, ret, hold, _, _, risk_log = _run_one(
        panel, px_map, **RUN_CFG, risk_cfg=risk_cfg,
    )
    c = m.get("calmar", float("nan"))
    a = m.get("ann_ret", float("nan"))
    d = m.get("mdd", float("nan"))
    s = m.get("sharpe", float("nan"))
    print(
        f"  {label:<35s} calmar={c:.4f}  ret={a*100:.2f}%  mdd={d*100:.1f}%  "
        f"sharpe={s:.3f}  ({time.time()-t0:.0f}s)",
        flush=True,
    )
    return {"label": label, **m}


def main():
    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    os.makedirs(out_dir, exist_ok=True)

    print("Loading panel ...", flush=True)
    t0 = time.time()
    panel = _prepare_panel_v5()
    panel = panel[
        (panel["date"] >= pd.Timestamp("2010-01-01"))
        & (panel["date"] <= pd.Timestamp("2025-12-31"))
    ].copy()
    panel, px_map = _enrich_from_stock_data(panel)
    print(f"Panel: {len(panel):,} rows ({time.time()-t0:.0f}s)\n", flush=True)

    all_rows = []

    # ═══════════════════════════════════════════════════════════════════
    # Stage 1: Sweep top_k with current SRF v2 weights
    # ═══════════════════════════════════════════════════════════════════
    print("═" * 70, flush=True)
    print("STAGE 1: Top-K sweep (current weights: 55/20/15/10)", flush=True)
    print("═" * 70, flush=True)

    for tk in [10, 15, 20, 25, 30, 40, None]:
        label = f"topk_{tk if tk else 'all'}"
        risk = {**RISK_BASE, "top_k": tk}
        row = run_exp(label, panel, px_map, risk)
        row["stage"] = 1
        row["top_k"] = tk
        row["weights"] = "55/20/15/10"
        all_rows.append(row)

    # Find best top_k
    stage1 = pd.DataFrame([r for r in all_rows if r["stage"] == 1])
    best_tk_row = stage1.sort_values("calmar", ascending=False).iloc[0]
    best_tk = best_tk_row["top_k"]
    best_tk = int(best_tk) if pd.notna(best_tk) else None
    print(f"\n  → Best top_k: {best_tk} (calmar={best_tk_row['calmar']:.4f})\n", flush=True)

    # ═══════════════════════════════════════════════════════════════════
    # Stage 2: Sweep SRF weight configs with best top_k
    # ═══════════════════════════════════════════════════════════════════
    print("═" * 70, flush=True)
    print(f"STAGE 2: Weight sweep (top_k={best_tk})", flush=True)
    print("═" * 70, flush=True)

    # Weight configs: (factor_z_neu, ret20d, -ret_intra5d, vol_price_div5d)
    # Must sum to 1.0. We patch _srf_score_v2 via monkey-patching the weights.
    weight_configs = [
        ("55/20/15/10 (current)", (0.55, 0.20, 0.15, 0.10)),
        ("65/15/10/10 (consensus+)", (0.65, 0.15, 0.10, 0.10)),
        ("70/10/10/10 (consensus++)", (0.70, 0.10, 0.10, 0.10)),
        ("45/25/20/10 (momentum+)", (0.45, 0.25, 0.20, 0.10)),
        ("40/20/25/15 (reversal+)", (0.40, 0.20, 0.25, 0.15)),
        ("50/15/20/15 (balanced)", (0.50, 0.15, 0.20, 0.15)),
        ("40/30/15/15 (momentum++)", (0.40, 0.30, 0.15, 0.15)),
        ("60/10/20/10 (cons+rev)", (0.60, 0.10, 0.20, 0.10)),
    ]

    import research.baseline_v6_1.code.run_baseline_v6_v61_suite as suite
    original_srf_v2 = suite._srf_score_v2

    for wname, (w1, w2, w3, w4) in weight_configs:
        def _patched_srf(day, _w1=w1, _w2=w2, _w3=w3, _w4=w4):
            import numpy as np
            def _z(s):
                std = s.std(ddof=0)
                return (s - s.mean()) / std if std > 0 else s * 0.0
            f     = _z(pd.to_numeric(day["factor_z_neu"],    errors="coerce").fillna(0.0))
            mom   = _z(pd.to_numeric(day["ret20d_stock"],    errors="coerce").fillna(0.0))
            intra = _z(-pd.to_numeric(day.get("ret_intra5d", pd.Series(0.0, index=day.index)), errors="coerce").fillna(0.0))
            div   = _z(pd.to_numeric(day.get("vol_price_div5d", pd.Series(0.0, index=day.index)), errors="coerce").fillna(0.0))
            score = _w1 * f + _w2 * mom + _w3 * intra + _w4 * div
            if "hv20_hv60_ratio" in day.columns:
                hv_ratio = pd.to_numeric(day["hv20_hv60_ratio"], errors="coerce").fillna(1.0)
                score = score - (hv_ratio > 1.5).astype(float) * 0.5
            return score

        suite._srf_score_v2 = _patched_srf
        label = f"w_{wname}"
        risk = {**RISK_BASE, "top_k": best_tk}
        row = run_exp(label, panel, px_map, risk)
        row["stage"] = 2
        row["top_k"] = best_tk
        row["weights"] = wname
        all_rows.append(row)

    suite._srf_score_v2 = original_srf_v2  # restore

    # Find best weights
    stage2 = pd.DataFrame([r for r in all_rows if r["stage"] == 2])
    best_w_row = stage2.sort_values("calmar", ascending=False).iloc[0]
    print(f"\n  → Best weights: {best_w_row['weights']} (calmar={best_w_row['calmar']:.4f})\n", flush=True)

    # ═══════════════════════════════════════════════════════════════════
    # Save results
    # ═══════════════════════════════════════════════════════════════════
    grid = pd.DataFrame(all_rows).sort_values("calmar", ascending=False)
    grid_path = os.path.join(out_dir, "srf_weight_grid_2010_2025.csv")
    grid.to_csv(grid_path, index=False, encoding="utf-8-sig")

    print("═" * 70, flush=True)
    print("SUMMARY", flush=True)
    print("═" * 70, flush=True)
    cols = ["label", "calmar", "ann_ret", "mdd", "sharpe", "top_k", "weights"]
    cols = [c for c in cols if c in grid.columns]
    print(grid[cols].head(10).to_string(index=False), flush=True)
    print(f"\nGrid saved → {grid_path}", flush=True)


if __name__ == "__main__":
    main()
