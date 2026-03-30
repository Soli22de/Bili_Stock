import os
import numpy as np
import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def _metrics(ret: pd.Series) -> dict:
    r = ret.dropna()
    if r.empty:
        return {"ann_ret": np.nan, "ann_vol": np.nan, "sharpe": np.nan, "sortino": np.nan, "mdd": np.nan, "calmar": np.nan}
    ann = 26.0
    avg = float(r.mean())
    vol = float(r.std(ddof=0))
    ann_ret = float((1 + avg) ** ann - 1.0)
    ann_vol = float(vol * np.sqrt(ann))
    neg = r[r < 0]
    downside = float(np.sqrt((neg.pow(2).mean()))) if not neg.empty else 0.0
    ann_down = float(downside * np.sqrt(ann))
    sharpe = ann_ret / ann_vol if ann_vol > 0 else np.nan
    sortino = ann_ret / ann_down if ann_down > 0 else np.nan
    curve = (1 + r).cumprod()
    dd = curve / curve.cummax() - 1.0
    mdd = float(dd.min()) if not dd.empty else np.nan
    calmar = ann_ret / abs(mdd) if pd.notna(mdd) and mdd != 0 else np.nan
    return {"ann_ret": ann_ret, "ann_vol": ann_vol, "sharpe": sharpe, "sortino": sortino, "mdd": mdd, "calmar": calmar}


def _load_returns() -> pd.DataFrame:
    p = os.path.join(ROOT, "research", "baseline_v6_1", "output", "strategy_comparison_returns_2019_2025.csv")
    x = pd.read_csv(p)
    x["date"] = pd.to_datetime(x["date"], errors="coerce").dt.normalize()
    return x.dropna(subset=["date"]).sort_values("date")


def _oos_eval(ret: pd.DataFrame) -> pd.DataFrame:
    out = []
    train_mask = (ret["date"] >= pd.Timestamp("2019-01-01")) & (ret["date"] <= pd.Timestamp("2022-12-31"))
    oos_mask = (ret["date"] >= pd.Timestamp("2023-01-01")) & (ret["date"] <= pd.Timestamp("2025-12-31"))
    for col in [c for c in ret.columns if c.endswith("_ret")]:
        mt = _metrics(ret.loc[train_mask, col])
        mo = _metrics(ret.loc[oos_mask, col])
        out.append({"strategy": col, **{f"train_{k}": v for k, v in mt.items()}, **{f"oos_{k}": v for k, v in mo.items()}})
    return pd.DataFrame(out)


def _rolling_eval(ret: pd.DataFrame) -> pd.DataFrame:
    cols = [c for c in ret.columns if c.endswith("_ret")]
    years = list(range(2010, 2026))
    rows = []
    for y in years:
        tr_s = pd.Timestamp(f"{y}-01-01")
        tr_e = pd.Timestamp(f"{y+2}-12-31")
        va_s = pd.Timestamp(f"{y+3}-01-01")
        va_e = pd.Timestamp(f"{y+3}-12-31")
        if va_s > ret["date"].max():
            continue
        tr = ret[(ret["date"] >= tr_s) & (ret["date"] <= tr_e)]
        va = ret[(ret["date"] >= va_s) & (ret["date"] <= va_e)]
        if tr.empty or va.empty:
            continue
        for c in cols:
            mtr = _metrics(tr[c])
            mva = _metrics(va[c])
            rows.append(
                {
                    "strategy": c,
                    "train_start": tr_s,
                    "train_end": tr_e,
                    "valid_start": va_s,
                    "valid_end": va_e,
                    "train_sortino": mtr["sortino"],
                    "valid_sortino": mva["sortino"],
                    "train_calmar": mtr["calmar"],
                    "valid_calmar": mva["calmar"],
                    "valid_mdd": mva["mdd"],
                }
            )
    return pd.DataFrame(rows)


def _cost_sensitivity() -> pd.DataFrame:
    p = os.path.join(ROOT, "research", "baseline_v6_1", "output", "group_ret_baseline_v6_1_2019_2025.csv")
    x = pd.read_csv(p).sort_values("date").reset_index(drop=True)
    x["date"] = pd.to_datetime(x["date"], errors="coerce").dt.normalize()
    turnovers = []
    prev = set()
    for i, r in x.iterrows():
        cur = set(str(r["top_symbols"]).split("|")) if pd.notna(r["top_symbols"]) and str(r["top_symbols"]) else set()
        if i == 0:
            t = 1.0
        else:
            overlap = len(prev & cur)
            t = 1.0 - overlap / max(len(cur), 1)
        turnovers.append(t)
        prev = cur
    x["turnover"] = turnovers
    rows = []
    for c in [0.0005, 0.0010, 0.0015]:
        spread = (x["Top30"] - x["turnover"] * c) - x["Bottom30"]
        m = _metrics(spread)
        rows.append({"one_way_cost": c, **m})
    return pd.DataFrame(rows)


def _param_stability() -> pd.DataFrame:
    p = os.path.join(ROOT, "research", "baseline_v6_1", "output", "sensitivity_grid_metrics.csv")
    if not os.path.exists(p):
        return pd.DataFrame()
    x = pd.read_csv(p)
    x = x.dropna(subset=["sortino", "calmar", "mdd"], how="any")
    if x.empty:
        return x
    q = x["sortino"].quantile(0.8)
    stable = x[x["sortino"] >= q].copy()
    out = pd.DataFrame(
        [
            {"metric": "grid_count", "value": float(len(x))},
            {"metric": "stable_count", "value": float(len(stable))},
            {"metric": "stable_ratio", "value": float(len(stable) / len(x))},
            {"metric": "sortino_std", "value": float(x["sortino"].std(ddof=0))},
            {"metric": "calmar_std", "value": float(x["calmar"].std(ddof=0))},
            {"metric": "mdd_std", "value": float(x["mdd"].std(ddof=0))},
        ]
    )
    return out


def _bias_audit() -> pd.DataFrame:
    files = [
        os.path.join(ROOT, "research", "baseline_v6_1", "code", "run_baseline_v6_v61_suite.py"),
        os.path.join(ROOT, "research", "baseline_v6_1", "code", "run_e3_focus_experiments.py"),
        os.path.join(ROOT, "research", "baseline_v6_1", "code", "run_e3_2_micro_tuning.py"),
        os.path.join(ROOT, "research", "baseline_v6_1", "code", "run_e3_2_light_tuning.py"),
        os.path.join(ROOT, "research", "baseline_v6_1", "code", "run_minimal_experiment_set.py"),
    ]
    shift_neg_count = 0
    for fp in files:
        with open(fp, "r", encoding="utf-8") as f:
            s = f.read()
        shift_neg_count += s.count("shift(-")
    cost_df = _cost_sensitivity()
    cost_pass = bool(cost_df["calmar"].min() > 0) if not cost_df.empty else False
    return pd.DataFrame(
        [
            {"check": "future_function_pattern", "value": shift_neg_count, "status": "review_required" if shift_neg_count > 0 else "pass"},
            {"check": "survivorship_bias_proxy", "value": np.nan, "status": "manual_review_required"},
            {"check": "cost_slippage_sensitivity", "value": float(cost_df["calmar"].min()) if not cost_df.empty else np.nan, "status": "pass" if cost_pass else "warning"},
        ]
    )


def _credibility(oos: pd.DataFrame, rolling: pd.DataFrame, bias: pd.DataFrame) -> pd.DataFrame:
    score = 100.0
    if not oos.empty:
        top = oos.sort_values("oos_sortino", ascending=False).iloc[0]
        if pd.notna(top["oos_mdd"]) and top["oos_mdd"] < -0.30:
            score -= 30
        if pd.notna(top["oos_calmar"]) and top["oos_calmar"] < 0:
            score -= 25
    if not rolling.empty:
        bad = rolling[(rolling["valid_calmar"] < 0) | (rolling["valid_mdd"] < -0.30)]
        score -= min(30, len(bad) * 2)
    if not bias.empty and (bias["status"] == "warning").any():
        score -= 10
    score = max(0.0, min(100.0, score))
    return pd.DataFrame([{"credibility_score": score}])


def main():
    rep = os.path.join(ROOT, "research", "baseline_v6_1", "report")
    os.makedirs(rep, exist_ok=True)
    ret = _load_returns()
    oos = _oos_eval(ret)
    rolling = _rolling_eval(ret)
    stability = _param_stability()
    cost = _cost_sensitivity()
    bias = _bias_audit()
    score = _credibility(oos, rolling, bias)
    oos.to_csv(os.path.join(rep, "phase_d_oos_metrics.csv"), index=False, encoding="utf-8-sig")
    rolling.to_csv(os.path.join(rep, "phase_d_rolling_validation.csv"), index=False, encoding="utf-8-sig")
    stability.to_csv(os.path.join(rep, "phase_d_parameter_stability.csv"), index=False, encoding="utf-8-sig")
    cost.to_csv(os.path.join(rep, "phase_d_cost_sensitivity.csv"), index=False, encoding="utf-8-sig")
    bias.to_csv(os.path.join(rep, "phase_d_bias_audit.csv"), index=False, encoding="utf-8-sig")
    score.to_csv(os.path.join(rep, "phase_d_credibility_scorecard.csv"), index=False, encoding="utf-8-sig")
    with open(os.path.join(rep, "phase_d_validation_report.md"), "w", encoding="utf-8") as f:
        f.write("# 阶段D 多周期与样本外验证报告\n\n")
        f.write("- 主切分：2019-2022训练，2023-2025样本外。\n")
        f.write(f"- 策略数：{len(oos)}，滚动窗口结果数：{len(rolling)}。\n")
        if not oos.empty:
            best = oos.sort_values("oos_sortino", ascending=False).iloc[0]
            f.write(f"- 最优样本外策略：{best['strategy']}，Sortino={best['oos_sortino']:.3f}，Calmar={best['oos_calmar']:.3f}，MDD={best['oos_mdd']:.2%}\n")
        if not stability.empty:
            sr = float(stability.loc[stability["metric"] == "stable_ratio", "value"].iloc[0])
            f.write(f"- 参数稳定区占比：{sr:.2%}\n")
        f.write(f"- 可信度评分：{float(score['credibility_score'].iloc[0]):.1f}/100\n")
    print(os.path.join(rep, "phase_d_validation_report.md"))


if __name__ == "__main__":
    main()
