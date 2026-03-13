import os
import sqlite3
import sys
from datetime import datetime

import pandas as pd


def _to_summary_dict(res):
    if res["summary"].empty:
        return {}
    return res["summary"].set_index("metric")["value"].to_dict()


def _judge(summary_dict):
    obs_days = float(summary_dict.get("obs_days", float("nan")))
    hit_ratio = float(summary_dict.get("hit_ratio_top_gt_bottom", float("nan")))
    excess = float(summary_dict.get("mean_top_minus_bottom", float("nan")))
    mdd = float(summary_dict.get("max_drawdown_ls_curve", float("nan")))
    checks = {
        "obs_days>=20": bool(obs_days >= 20.0),
        "hit_ratio>=0.65": bool(hit_ratio >= 0.65),
        "top_bottom_excess>0": bool(excess > 0.0),
        "max_drawdown<=0.3": bool(mdd >= -0.3),
    }
    return checks, all(checks.values())


def _write_data_gap_checklist(out_md: str, db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("select created_at from rebalancing_history where created_at is not null order by created_at asc limit 1")
    min_dt = cur.fetchone()[0]
    cur.execute("select created_at from rebalancing_history where created_at is not null order by created_at desc limit 1")
    max_dt = cur.fetchone()[0]
    cur.execute("select count(*) from rebalancing_history where date(created_at)>=date('2019-01-01') and date(created_at)<=date('2025-12-31')")
    rows_19_25 = int(cur.fetchone()[0])
    cur.execute("select count(distinct stock_symbol) from rebalancing_history where date(created_at)>=date('2019-01-01') and date(created_at)<=date('2025-12-31')")
    stocks_raw = int(cur.fetchone()[0])
    cur.execute("select count(distinct stock_symbol) from rebalancing_history where date(created_at)>=date('2019-01-01') and date(created_at)<=date('2025-12-31') and (stock_symbol glob 'SH60????' or stock_symbol glob 'SH68????' or stock_symbol glob 'SZ00????' or stock_symbol glob 'SZ30????')")
    stocks_ashare = int(cur.fetchone()[0])
    conn.close()
    lines = []
    lines.append("# 补数据清单（P0/P1）")
    lines.append("")
    lines.append("## 当前覆盖快照")
    lines.append("")
    lines.append(f"- 调仓记录最早时间：{min_dt}")
    lines.append(f"- 调仓记录最晚时间：{max_dt}")
    lines.append(f"- 2019-2025 调仓记录行数：{rows_19_25}")
    lines.append(f"- 2019-2025 股票池去重（原始symbol）：{stocks_raw}")
    lines.append(f"- 2019-2025 股票池去重（A股口径）：{stocks_ashare}")
    lines.append("")
    lines.append("## P0（立即补）")
    lines.append("")
    lines.append("- 组合绩效历史面板（按月/按季）")
    lines.append("  - 字段：cube_symbol, period_end, return, max_drawdown, turnover, win_rate")
    lines.append("- 调仓披露时点字段")
    lines.append("  - 字段：signal_publish_time, signal_visible_time")
    lines.append("- 组合调仓后披露延迟天数（P0补充）")
    lines.append("  - 字段：disclosure_delay_days")
    lines.append("- 价格覆盖质量报告")
    lines.append("  - 指标：coverage_by_year, missing_rate_by_symbol, join_success_rate")
    lines.append("")
    lines.append("## P1（增强稳健性）")
    lines.append("")
    lines.append("- 最高优先级：行业映射")
    lines.append("  - 字段：stock_symbol, industry_l1, industry_l2")
    lines.append("- 次高优先级：流动性字段")
    lines.append("  - 字段：amount, turnover_rate")
    lines.append("- 一般优先级：涨跌停/停牌标记")
    lines.append("  - 字段：limit_up_down_flag, suspended_flag")
    lines.append("")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def save_markdown_summary(result, out_md, start_date, end_date, factor_label):
    s = _to_summary_dict(result)
    checks, passed = _judge(s)
    excess = float(s.get("mean_top_minus_bottom", float("nan")))
    mdd = float(s.get("max_drawdown_ls_curve", float("nan")))
    calmar = excess / abs(mdd) if pd.notna(excess) and pd.notna(mdd) and mdd != 0 else float("nan")
    lines = []
    lines.append("# 雪球调仓动量因子MVP报告")
    lines.append("")
    lines.append(f"- 样本区间：{start_date} ~ {end_date}")
    lines.append(f"- 因子：{factor_label}")
    lines.append("- 极端值处理：每期剔除因子最高5%和最低5%")
    lines.append("- 持有期约束：买入后强制持有2w（10交易日）")
    lines.append("- 分组：Top30 / Middle40 / Bottom30")
    lines.append("- 预测周期：2w")
    lines.append("")
    lines.append("## 回测摘要")
    lines.append("")
    lines.append(f"- mean_top: {s.get('mean_top', float('nan')):.6f}")
    lines.append(f"- mean_middle: {s.get('mean_middle', float('nan')):.6f}")
    lines.append(f"- mean_bottom: {s.get('mean_bottom', float('nan')):.6f}")
    lines.append(f"- mean_top_minus_bottom: {s.get('mean_top_minus_bottom', float('nan')):.6f}")
    lines.append(f"- hit_ratio_top_gt_bottom: {s.get('hit_ratio_top_gt_bottom', float('nan')):.4f}")
    lines.append(f"- obs_days_2w: {s.get('obs_days', float('nan')):.0f}")
    lines.append(f"- max_drawdown_ls_curve: {s.get('max_drawdown_ls_curve', float('nan')):.4f}")
    lines.append(f"- calmar_ratio: {calmar:.6f}")
    lines.append("")
    lines.append("## 门槛判定")
    lines.append("")
    lines.append(f"- threshold_obs_days>=20: {'通过' if checks['obs_days>=20'] else '未通过'}")
    lines.append(f"- threshold_hit_ratio>=0.65: {'通过' if checks['hit_ratio>=0.65'] else '未通过'}")
    lines.append(f"- threshold_top_bottom_excess>0: {'通过' if checks['top_bottom_excess>0'] else '未通过'}")
    lines.append(f"- threshold_max_drawdown<=0.3: {'通过' if checks['max_drawdown<=0.3'] else '未通过'}")
    lines.append(f"- threshold_overall: {'通过' if passed else '未通过'}")
    lines.append("")
    lines.append("## 与优化前对比")
    lines.append("")
    lines.append("- max_drawdown 基线：-0.5388")
    lines.append(f"- max_drawdown 当前：{s.get('max_drawdown_ls_curve', float('nan')):.4f}")
    lines.append("- hit_ratio 基线：0.7078")
    lines.append(f"- hit_ratio 当前：{s.get('hit_ratio_top_gt_bottom', float('nan')):.4f}")
    lines.append("- top-bottom 基线：0.010932")
    lines.append(f"- top-bottom 当前：{s.get('mean_top_minus_bottom', float('nan')):.6f}")
    lines.append(f"- calmar 当前：{calmar:.6f}")
    lines.append("")
    lines.append("## 结论")
    lines.append("")
    lines.append(f"- 方向判断：{'可继续迭代' if passed else '先补数据再判断'}")
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
    panel_csv = os.path.join(out_dir, "factor_panel_rebalance_momentum.csv")
    panel = build_rebalance_momentum_panel(
        db_path=db_path,
        cache_dir=cache_dir,
        out_csv=panel_csv,
        start_date=start_date,
        end_date=end_date,
        lag_days=14,
        smoothing_days=3,
        factor_mode="rate",
    )
    result = run_three_bucket_backtest(panel, horizon_col="fwd_ret_2w", hold_lock_days=10, trim_q=0.05)
    save_three_bucket_results(result, out_dir, "rebalance_momentum_2w")
    png = os.path.join(out_dir, "group_curve_rebalance_momentum_2w.png")
    plot_three_bucket_curves(result["group_ret"], result["ls_curve"], png, "Rebalance Momentum Factor - 2w")
    summary_md = os.path.join(out_dir, "factor_rebalance_momentum_summary.md")
    save_markdown_summary(
        result,
        summary_md,
        start_date=start_date,
        end_date=end_date,
        factor_label="过去14天净买入组合数变化率的3日移动平均",
    )
    checklist_md = os.path.join(out_dir, "data_gap_checklist_rebalance_momentum.md")
    _write_data_gap_checklist(checklist_md, db_path)
    print(f"panel_rows={len(panel)}")
    print(f"summary={summary_md}")
    print(f"checklist={checklist_md}")


if __name__ == "__main__":
    main()
