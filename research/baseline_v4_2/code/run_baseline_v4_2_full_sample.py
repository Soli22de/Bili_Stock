import os
import sys

import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v4.code.run_baseline_v4_2_up_filter import (
    _apply_liq_dynamic,
    _apply_up_exposure,
    _assign_other_industry_by_proxy,
    _attach_base_fields,
    _build_group_ret_v42,
    _industry_neutralize,
    _load_hs300,
    _metrics,
)
from research.data_prep.build_rebalance_momentum_panel import build_rebalance_momentum_panel


def main():
    out_dir = os.path.join(ROOT, "research", "baseline_v4_2", "output")
    os.makedirs(out_dir, exist_ok=True)
    panel = build_rebalance_momentum_panel(
        db_path=os.path.join(ROOT, "data", "cubes.db"),
        cache_dir=os.path.join(ROOT, "data", "market_cache"),
        out_csv=os.path.join(out_dir, "factor_panel_rebalance_momentum_2019_2025.csv"),
        start_date="2019-01-01",
        end_date="2025-12-31",
        lag_days=14,
        smoothing_days=3,
        factor_mode="rate",
    )
    base = _attach_base_fields(
        panel,
        industry_map_csv=os.path.join(ROOT, "research", "baseline_v1", "data_delivery", "industry_mapping_v2.csv"),
        liquidity_csv=os.path.join(ROOT, "research", "baseline_v1", "data_delivery", "liquidity_daily_v1.csv"),
    )
    base = _assign_other_industry_by_proxy(base)
    base["factor_z_raw"] = base["factor_z"]
    base = _industry_neutralize(base, source_col="factor_z_raw", out_col="factor_z_neu")
    regime = _load_hs300("2019-01-01", "2025-12-31")
    panel_liq = _apply_liq_dynamic(base, regime_df=regime, keep_other=0.6, keep_up=0.2)
    group_ret = _build_group_ret_v42(panel_liq, trim_q=0.05, hold_step=10)
    group_ret = _apply_up_exposure(group_ret, up_scale=0.5)
    m = _metrics(group_ret)
    group_ret[["date", "Bottom30", "Middle40", "Top30"]].to_csv(
        os.path.join(out_dir, "group_ret_baseline_v4_2_2w_2019_2025.csv"), index=False, encoding="utf-8-sig"
    )
    pd.DataFrame(
        {"metric": ["up_top_bottom", "calmar_ratio"], "value": [m["up_top_bottom"], m["calmar"]]}
    ).to_csv(os.path.join(out_dir, "core_metrics_baseline_v4_2_2019_2025.csv"), index=False, encoding="utf-8-sig")
    with open(os.path.join(out_dir, "baseline_v4_2_full_sample_report.md"), "w", encoding="utf-8") as f:
        f.write("# baseline_v4.2 全样本验证（2019-2025）\n\n")
        f.write(f"- 上涨市 top-bottom: {m['up_top_bottom']:.6f}\n")
        f.write(f"- 整体 calmar: {m['calmar']:.6f}\n")
    print(f"up_top_bottom={m['up_top_bottom']:.6f}")
    print(f"calmar={m['calmar']:.6f}")


if __name__ == "__main__":
    main()
