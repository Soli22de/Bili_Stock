import os
import sys


def _to_summary_dict(res):
    if res["summary"].empty:
        return {}
    return res["summary"].set_index("metric")["value"].to_dict()


def _threshold_judge(summary_dict):
    obs_days = float(summary_dict.get("obs_days", float("nan")))
    hit_ratio = float(summary_dict.get("hit_ratio_Q5_gt_Q1", float("nan")))
    mean_excess = float(summary_dict.get("mean_Q5_minus_Q1", float("nan")))
    checks = {
        "obs_days>=30": bool(obs_days >= 30.0),
        "hit_ratio>0.55": bool(hit_ratio > 0.55),
        "mean_excess>=0.03": bool(mean_excess >= 0.03),
    }
    return checks, all(checks.values())


def save_markdown_summary(all_results, out_md, start_date, end_date):
    lines = []
    lines.append("# 雪球中长期因子MVP对比报告")
    lines.append("")
    lines.append("因子：优质主理人共识净买入因子")
    lines.append("")
    lines.append(f"样本区间：{start_date} ~ {end_date}")
    lines.append("")
    for variant in ["baseline", "optimized"]:
        lines.append(f"## {variant}")
        lines.append("")
        for hname, res in all_results[variant].items():
            s = _to_summary_dict(res)
            lines.append(f"### {hname}")
            lines.append("")
            lines.append(f"- mean_Q1: {s.get('mean_Q1', float('nan')):.6f}")
            lines.append(f"- mean_Q5: {s.get('mean_Q5', float('nan')):.6f}")
            lines.append(f"- mean_Q5_minus_Q1: {s.get('mean_Q5_minus_Q1', float('nan')):.6f}")
            lines.append(f"- hit_ratio_Q5_gt_Q1: {s.get('hit_ratio_Q5_gt_Q1', float('nan')):.4f}")
            lines.append(f"- obs_days: {s.get('obs_days', float('nan')):.0f}")
            lines.append(f"- max_drawdown_ls_curve: {s.get('max_drawdown_ls_curve', float('nan')):.4f}")
            lines.append("")
    lines.append("## 2w门槛判定")
    lines.append("")
    for variant in ["baseline", "optimized"]:
        s2w = _to_summary_dict(all_results[variant]["2w"])
        checks, passed = _threshold_judge(s2w)
        lines.append(f"### {variant}")
        lines.append("")
        lines.append(f"- obs_days_2w: {s2w.get('obs_days', float('nan')):.0f}")
        lines.append(f"- hit_ratio_2w: {s2w.get('hit_ratio_Q5_gt_Q1', float('nan')):.4f}")
        lines.append(f"- mean_excess_2w: {s2w.get('mean_Q5_minus_Q1', float('nan')):.6f}")
        lines.append(f"- max_drawdown_2w_ls_curve: {s2w.get('max_drawdown_ls_curve', float('nan')):.4f}")
        lines.append(f"- threshold_obs_days>=30: {'通过' if checks['obs_days>=30'] else '未通过'}")
        lines.append(f"- threshold_hit_ratio>0.55: {'通过' if checks['hit_ratio>0.55'] else '未通过'}")
        lines.append(f"- threshold_mean_excess>=0.03: {'通过' if checks['mean_excess>=0.03'] else '未通过'}")
        lines.append(f"- threshold_overall: {'通过' if passed else '未通过'}")
        lines.append("")
    _, opt_passed = _threshold_judge(_to_summary_dict(all_results["optimized"]["2w"]))
    recommendation = "继续优化" if opt_passed else "更换方向"
    lines.append("## 决策建议")
    lines.append("")
    lines.append(f"- 建议：{recommendation}")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if root not in sys.path:
        sys.path.insert(0, root)
    from research.backtest.group_backtest import run_group_backtest, save_group_backtest_results
    from research.data_prep.build_factor_panel import build_factor_panel
    from research.plots.plot_factor_groups import plot_group_curves
    db_path = os.path.join(root, "data", "cubes.db")
    cube_perf = os.path.join(root, "data", "cube_performance_2025.csv")
    cache_dir = os.path.join(root, "data", "market_cache")
    out_dir = os.path.join(root, "research", "output")
    start_date = "2019-01-01"
    end_date = "2025-12-31"
    os.makedirs(out_dir, exist_ok=True)
    horizons = {"1w": "fwd_ret_1w", "2w": "fwd_ret_2w", "4w": "fwd_ret_4w"}
    all_results = {"baseline": {}, "optimized": {}}
    panel_sizes = {}
    for variant in ["baseline", "optimized"]:
        panel_csv = os.path.join(out_dir, f"factor_panel_consensus_quality_{variant}.csv")
        panel = build_factor_panel(
            db_path,
            cube_perf,
            cache_dir,
            panel_csv,
            start_date=start_date,
            end_date=end_date,
            factor_mode=variant,
        )
        panel_sizes[variant] = len(panel)
        for hname, hcol in horizons.items():
            res = run_group_backtest(panel, hcol)
            save_group_backtest_results(res, out_dir, f"{variant}_{hname}")
            png = os.path.join(out_dir, f"group_curve_{variant}_{hname}.png")
            plot_group_curves(res["group_ret"], res["ls_curve"], png, f"Consensus Quality Factor - {variant} - {hname}")
            all_results[variant][hname] = res
    summary_md = os.path.join(out_dir, "factor_mvp_summary.md")
    save_markdown_summary(all_results, summary_md, start_date=start_date, end_date=end_date)
    print(f"panel_rows_baseline={panel_sizes.get('baseline', 0)}")
    print(f"panel_rows_optimized={panel_sizes.get('optimized', 0)}")
    print(f"summary={summary_md}")


if __name__ == "__main__":
    main()
