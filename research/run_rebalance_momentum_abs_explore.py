import os
import sys

import pandas as pd


def _to_summary_dict(res):
    if res["summary"].empty:
        return {}
    return res["summary"].set_index("metric")["value"].to_dict()


def _calmar(summary_dict):
    excess = float(summary_dict.get("mean_top_minus_bottom", float("nan")))
    mdd = float(summary_dict.get("max_drawdown_ls_curve", float("nan")))
    if pd.isna(excess) or pd.isna(mdd) or mdd == 0:
        return float("nan")
    return excess / abs(mdd)


def _load_baseline_summary(summary_md: str):
    if not os.path.exists(summary_md):
        return {}
    out = {}
    with open(summary_md, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if t.startswith("- "):
                t = t[2:]
                if ":" in t:
                    k, v = t.split(":", 1)
                    out[k.strip()] = v.strip()
    return out


def _to_float(text, default=float("nan")):
    try:
        return float(str(text))
    except Exception:
        return default


def save_explore_summary(result, out_md, start_date, end_date, baseline_summary_md):
    s = _to_summary_dict(result)
    calmar = _calmar(s)
    b = _load_baseline_summary(baseline_summary_md)
    b_hit = _to_float(b.get("hit_ratio_top_gt_bottom"))
    b_mdd = _to_float(b.get("max_drawdown_ls_curve"))
    b_excess = _to_float(b.get("mean_top_minus_bottom"))
    b_calmar = _to_float(b.get("calmar_ratio"))
    lines = []
    lines.append("# 调仓动量因子探索报告（热度绝对值）")
    lines.append("")
    lines.append(f"- 样本区间：{start_date} ~ {end_date}")
    lines.append("- 探索因子：过去14天净买入组合数绝对值的3日移动平均")
    lines.append("- 极端值处理：每期剔除因子最高5%和最低5%")
    lines.append("- 持有期约束：买入后强制持有2w（10交易日）")
    lines.append("- 分组：Top30 / Middle40 / Bottom30")
    lines.append("- 预测周期：2w")
    lines.append("")
    lines.append("## 探索结果")
    lines.append("")
    lines.append(f"- mean_top_minus_bottom: {s.get('mean_top_minus_bottom', float('nan')):.6f}")
    lines.append(f"- hit_ratio_top_gt_bottom: {s.get('hit_ratio_top_gt_bottom', float('nan')):.4f}")
    lines.append(f"- max_drawdown_ls_curve: {s.get('max_drawdown_ls_curve', float('nan')):.4f}")
    lines.append(f"- calmar_ratio: {calmar:.6f}")
    lines.append(f"- obs_days_2w: {s.get('obs_days', float('nan')):.0f}")
    lines.append("")
    lines.append("## 与基线对比")
    lines.append("")
    lines.append(f"- baseline_hit_ratio: {b_hit:.4f}")
    lines.append(f"- explore_hit_ratio: {s.get('hit_ratio_top_gt_bottom', float('nan')):.4f}")
    lines.append(f"- baseline_max_drawdown: {b_mdd:.4f}")
    lines.append(f"- explore_max_drawdown: {s.get('max_drawdown_ls_curve', float('nan')):.4f}")
    lines.append(f"- baseline_top_bottom: {b_excess:.6f}")
    lines.append(f"- explore_top_bottom: {s.get('mean_top_minus_bottom', float('nan')):.6f}")
    lines.append(f"- baseline_calmar: {b_calmar:.6f}")
    lines.append(f"- explore_calmar: {calmar:.6f}")
    lines.append("")
    lines.append("## 判定")
    lines.append("")
    hit_ok = float(s.get("hit_ratio_top_gt_bottom", float("nan"))) > b_hit
    mdd_ok = float(s.get("max_drawdown_ls_curve", float("nan"))) >= -0.1
    lines.append(f"- hit_ratio是否超过0.64（并高于基线）：{'是' if hit_ok else '否'}")
    lines.append(f"- max_drawdown是否在-0.1以内：{'是' if mdd_ok else '否'}")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if root not in sys.path:
        sys.path.insert(0, root)
    from research.backtest.group_backtest_three_bucket import run_three_bucket_backtest, save_three_bucket_results
    from research.data_prep.build_rebalance_momentum_panel import build_rebalance_momentum_panel
    from research.plots.plot_three_bucket import plot_three_bucket_curves
    db_path = os.path.join(root, "data", "cubes.db")
    cache_dir = os.path.join(root, "data", "market_cache")
    out_dir = os.path.join(root, "research", "output")
    start_date = "2019-01-01"
    end_date = "2025-12-31"
    os.makedirs(out_dir, exist_ok=True)
    panel_csv = os.path.join(out_dir, "factor_panel_rebalance_momentum_abs.csv")
    panel = build_rebalance_momentum_panel(
        db_path=db_path,
        cache_dir=cache_dir,
        out_csv=panel_csv,
        start_date=start_date,
        end_date=end_date,
        lag_days=14,
        smoothing_days=3,
        factor_mode="absolute",
    )
    result = run_three_bucket_backtest(panel, horizon_col="fwd_ret_2w", hold_lock_days=10, trim_q=0.05)
    save_three_bucket_results(result, out_dir, "rebalance_momentum_abs_2w")
    png = os.path.join(out_dir, "group_curve_rebalance_momentum_abs_2w.png")
    plot_three_bucket_curves(result["group_ret"], result["ls_curve"], png, "Rebalance Momentum Absolute Heat - 2w")
    summary_md = os.path.join(out_dir, "factor_rebalance_momentum_abs_summary.md")
    baseline_summary_md = os.path.join(out_dir, "factor_rebalance_momentum_summary.md")
    save_explore_summary(result, summary_md, start_date, end_date, baseline_summary_md)
    print(f"panel_rows={len(panel)}")
    print(f"summary={summary_md}")


if __name__ == "__main__":
    main()
