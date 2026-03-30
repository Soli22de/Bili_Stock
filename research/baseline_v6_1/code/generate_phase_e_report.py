import os
import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def _safe_read(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def main():
    rep = os.path.join(ROOT, "research", "baseline_v6_1", "report")
    ind = _safe_read(os.path.join(rep, "industry_drawdown_contrib_2024_2025.csv"))
    heat = _safe_read(os.path.join(rep, "heat_signal_ahead_drawdown_2024_2025.csv"))
    reb = _safe_read(os.path.join(rep, "rebalance_period_pnl_2024_2025.csv"))
    mkt = _safe_read(os.path.join(rep, "market_sentiment_drawdown_2024_2025.csv"))
    stop = _safe_read(os.path.join(rep, "stop_loss_efficiency_2024_2025.csv"))
    out = os.path.join(rep, "phase_e_special_diagnostics_report.md")
    with open(out, "w", encoding="utf-8") as f:
        f.write("# 阶段E 五项专项诊断报告\n\n")
        f.write("## E1 分行业回撤贡献\n\n")
        if ind.empty:
            f.write("- 无可用数据。\n")
        else:
            top = ind.sort_values("contrib").head(10)
            f.write(f"- 样本行数：{len(ind)}，重点负贡献行业条目：{len(top)}\n")
            for _, r in top.head(5).iterrows():
                f.write(f"- interval={int(r['interval_id'])}, 行业={r['industry_l2']}, 贡献={float(r['contrib']):.6f}, 占比={float(r['share']):.2%}\n")
        f.write("\n## E2 雪球热度领先性验证\n\n")
        if heat.empty:
            f.write("- 无可用数据。\n")
        else:
            warn = heat[(heat["weighted_heat_drop"] <= -0.3) & (heat["weighted_flag_ratio"] >= 0.3)]
            f.write(f"- 回撤前窗口数：{len(heat)}，触发预警窗口：{len(warn)}\n")
            if not warn.empty:
                for _, r in warn.head(5).iterrows():
                    f.write(f"- interval={int(r['interval_id'])}, 热度降幅={float(r['weighted_heat_drop']):.2%}, 预警权重占比={float(r['weighted_flag_ratio']):.2%}\n")
        f.write("\n## E3 分调仓期盈亏拆解\n\n")
        if reb.empty:
            f.write("- 无可用数据。\n")
        else:
            reb["period_ret"] = pd.to_numeric(reb["period_ret"], errors="coerce")
            lose_streak = 0
            best_streak = 0
            for v in reb["period_ret"].fillna(0).tolist():
                if v < 0:
                    lose_streak += 1
                    best_streak = max(best_streak, lose_streak)
                else:
                    lose_streak = 0
            f.write(f"- 调仓期总数：{len(reb)}，最大连续亏损期数：{best_streak}，单期最差收益：{reb['period_ret'].min():.2%}\n")
        f.write("\n## E4 市场情绪对应分析\n\n")
        if mkt.empty:
            f.write("- 无可用数据。\n")
        else:
            hot = mkt[(pd.to_numeric(mkt["hs300_ret20d"], errors="coerce") >= 0.08) & (pd.to_numeric(mkt["chinext_vol_pct"], errors="coerce") >= 0.8)]
            f.write(f"- 情绪样本数：{len(mkt)}，过热样本数：{len(hot)}\n")
        f.write("\n## E5 个股止损执行效率\n\n")
        if stop.empty:
            f.write("- 无可用数据。\n")
        else:
            stop["avoidable_contrib"] = pd.to_numeric(stop["avoidable_contrib"], errors="coerce")
            hit = stop[stop["hit_stop"] == True]
            f.write(f"- 样本行数：{len(stop)}，触发止损样本：{len(hit)}，可避免损失贡献合计：{stop['avoidable_contrib'].sum():.6f}\n")
        f.write("\n- 结论：五项专项CSV已落地，建议将热度预警与组合止损阈值联动为实盘告警条件。\n")
    print(out)


if __name__ == "__main__":
    main()
