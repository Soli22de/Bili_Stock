import os
import sys

import numpy as np
import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5
from research.baseline_v6_1.code.run_baseline_v6_v61_suite import _enrich_from_stock_data, _run_one


def _overheat_trigger_counts(ret: pd.DataFrame, start: str, end: str, hs_thr: float, ind_thr: float, turn_q: float) -> dict:
    x = ret.copy()
    x = x[(x["date"] >= pd.Timestamp(start)) & (x["date"] <= pd.Timestamp(end))].copy()
    turn_cut = float(x["market_turnover_proxy"].quantile(turn_q)) if ("market_turnover_proxy" in x.columns and not x["market_turnover_proxy"].dropna().empty) else np.nan
    hs_cnt = int((pd.to_numeric(x["hs300_ret20"], errors="coerce") > hs_thr).sum()) if "hs300_ret20" in x.columns else 0
    ind_cnt = int((pd.to_numeric(x["top3_ind_ret20"], errors="coerce") > ind_thr).sum()) if "top3_ind_ret20" in x.columns else 0
    turn_cnt = int((pd.to_numeric(x["market_turnover_proxy"], errors="coerce") > turn_cut).sum()) if pd.notna(turn_cut) else 0
    return {
        "hs300_ret20_trigger_count": hs_cnt,
        "top3_ind_ret20_trigger_count": ind_cnt,
        "turnover_q_trigger_count": turn_cnt,
        "turnover_quantile_cut": turn_cut,
        "bars": int(len(x)),
    }


def _remove_add_eval(panel: pd.DataFrame, hold_base: pd.DataFrame, hold_exp: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    base_map = hold_base.groupby("date")["stock_symbol"].apply(lambda s: set(s.astype(str))).to_dict() if not hold_base.empty else {}
    exp_map = hold_exp.groupby("date")["stock_symbol"].apply(lambda s: set(s.astype(str))).to_dict() if not hold_exp.empty else {}
    dts = sorted(set(base_map.keys()) & set(exp_map.keys()))
    dts = [d for d in dts if pd.Timestamp(start) <= pd.Timestamp(d) <= pd.Timestamp(end)]
    pr = panel[["date", "stock_symbol", "fwd_ret_2w"]].copy()
    rows = []
    for d in dts:
        b = base_map.get(d, set())
        e = exp_map.get(d, set())
        removed = sorted(list(b - e))
        added = sorted(list(e - b))
        for s in removed:
            rr = pr[(pr["date"] == d) & (pr["stock_symbol"] == s)]
            if rr.empty:
                continue
            rows.append({"date": d, "stock_symbol": s, "side": "removed", "fwd_ret_2w": float(rr["fwd_ret_2w"].iloc[0])})
        for s in added:
            rr = pr[(pr["date"] == d) & (pr["stock_symbol"] == s)]
            if rr.empty:
                continue
            rows.append({"date": d, "stock_symbol": s, "side": "added", "fwd_ret_2w": float(rr["fwd_ret_2w"].iloc[0])})
    det = pd.DataFrame(rows)
    if det.empty:
        return pd.DataFrame(
            [
                {"side": "removed", "count": 0, "mean_fwd_ret_2w": np.nan, "positive_ratio": np.nan},
                {"side": "added", "count": 0, "mean_fwd_ret_2w": np.nan, "positive_ratio": np.nan},
            ]
        )
    out = []
    for side in ["removed", "added"]:
        s = det[det["side"] == side]
        out.append(
            {
                "side": side,
                "count": int(len(s)),
                "mean_fwd_ret_2w": float(pd.to_numeric(s["fwd_ret_2w"], errors="coerce").mean()) if not s.empty else np.nan,
                "positive_ratio": float((pd.to_numeric(s["fwd_ret_2w"], errors="coerce") > 0).mean()) if not s.empty else np.nan,
            }
        )
    return pd.DataFrame(out)


def main():
    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    rep_dir = os.path.join(ROOT, "research", "baseline_v6_1", "report")
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    panel = _prepare_panel_v5(start_date="2010-01-01", end_date="2025-12-31")
    panel, px_map = _enrich_from_stock_data(panel)
    panel = panel[(panel["date"] >= pd.Timestamp("2010-01-01")) & (panel["date"] <= pd.Timestamp("2025-12-31"))].copy()

    exps = [
        {"experiment": "base_v6_1", "hold_step": 10, "liq_other": 0.60, "cap_non_up": 0.15, "cap_up": 0.25, "with_takeprofit": True, "risk_cfg": {}},
        {"experiment": "base_E_foundation", "hold_step": 12, "liq_other": 0.55, "cap_non_up": 0.10, "cap_up": 0.25, "with_takeprofit": True, "risk_cfg": {"non_up_vol_q": 0.65, "choppy_loss_scale": 0.50}},
        {
            "experiment": "exp1_1_E_overheat_loose",
            "hold_step": 12,
            "liq_other": 0.55,
            "cap_non_up": 0.10,
            "cap_up": 0.25,
            "with_takeprofit": True,
            "risk_cfg": {
                "non_up_vol_q": 0.65,
                "choppy_loss_scale": 0.50,
                "overheat_hs_trigger": 0.03,
                "overheat_ind_trigger": 0.05,
                "overheat_turn_q": 0.75,
                "overheat_hs_release": 0.01,
                "overheat_ind_release": 0.02,
                "overheat_cap_non_up": 0.12,
                "overheat_cap_up": 0.27,
            },
        },
        {
            "experiment": "exp2_1_E_xq_loose40",
            "hold_step": 12,
            "liq_other": 0.55,
            "cap_non_up": 0.10,
            "cap_up": 0.25,
            "with_takeprofit": True,
            "risk_cfg": {"non_up_vol_q": 0.65, "choppy_loss_scale": 0.50, "xq_enable": True, "xq_warn_drop": 0.40, "xq_recover_rise": 0.10, "xq_require_neg_ret": False},
        },
    ]

    rows = []
    ret_map = {}
    hold_map = {}
    for e in exps:
        m, ret, hold, attr, sf, risk_log = _run_one(
            panel,
            px_map,
            hold_step=e["hold_step"],
            liq_other=e["liq_other"],
            cap_non_up=e["cap_non_up"],
            cap_up=e["cap_up"],
            with_takeprofit=e["with_takeprofit"],
            risk_cfg=e.get("risk_cfg", {}),
        )
        rows.append({**e, **m})
        ret_map[e["experiment"]] = ret.copy()
        hold_map[e["experiment"]] = hold.copy()
        ret.to_csv(os.path.join(out_dir, f"{e['experiment']}_group_ret_2010_2025.csv"), index=False, encoding="utf-8-sig")
        hold.to_csv(os.path.join(out_dir, f"{e['experiment']}_holdings_2010_2025.csv"), index=False, encoding="utf-8-sig")
        if attr is not None and not attr.empty:
            attr.to_csv(os.path.join(out_dir, f"{e['experiment']}_attribution_2010_2025.csv"), index=False, encoding="utf-8-sig")
        if sf is not None and not sf.empty:
            sf.to_csv(os.path.join(out_dir, f"{e['experiment']}_sell_fly_2010_2025.csv"), index=False, encoding="utf-8-sig")
        risk_log.to_csv(os.path.join(out_dir, f"{e['experiment']}_risk_log_2010_2025.csv"), index=False, encoding="utf-8-sig")

    res = pd.DataFrame(rows)
    train_s, train_e = "2010-01-01", "2020-12-31"
    oos_s, oos_e = "2021-01-01", "2025-12-31"
    panel_train = panel[(panel["date"] >= pd.Timestamp(train_s)) & (panel["date"] <= pd.Timestamp(train_e))].copy()
    panel_oos = panel[(panel["date"] >= pd.Timestamp(oos_s)) & (panel["date"] <= pd.Timestamp(oos_e))].copy()
    oos_rows = []
    for e in exps:
        m_tr, _, _, _, _, _ = _run_one(panel_train, px_map, e["hold_step"], e["liq_other"], e["cap_non_up"], e["cap_up"], e["with_takeprofit"], risk_cfg=e.get("risk_cfg", {}))
        m_oos, _, _, _, _, _ = _run_one(panel_oos, px_map, e["hold_step"], e["liq_other"], e["cap_non_up"], e["cap_up"], e["with_takeprofit"], risk_cfg=e.get("risk_cfg", {}))
        oos_rows.append(
            {
                "experiment": e["experiment"],
                "train_sortino": m_tr.get("sortino", np.nan),
                "train_calmar": m_tr.get("calmar", np.nan),
                "train_mdd": m_tr.get("mdd", np.nan),
                "oos_sortino": m_oos.get("sortino", np.nan),
                "oos_calmar": m_oos.get("calmar", np.nan),
                "oos_mdd": m_oos.get("mdd", np.nan),
            }
        )
    oos_df = pd.DataFrame(oos_rows)
    oos_df.to_csv(os.path.join(out_dir, "enhanced_oos_eval_2010_2025.csv"), index=False, encoding="utf-8-sig")

    rank = res.merge(oos_df[["experiment", "oos_sortino"]], on="experiment", how="left")
    rank = rank.sort_values(["oos_sortino", "震荡_top_bottom", "calmar", "mdd"], ascending=[False, False, False, False]).reset_index(drop=True)
    rank.to_csv(os.path.join(out_dir, "enhanced_choppy_summary_2010_2025.csv"), index=False, encoding="utf-8-sig")

    sens = []
    for _, r in rank.iterrows():
        cfg = r.get("risk_cfg", {})
        sens.append(
            {
                "experiment": r["experiment"],
                "non_up_vol_q": cfg.get("non_up_vol_q", 1.0) if isinstance(cfg, dict) else np.nan,
                "choppy_loss_scale": cfg.get("choppy_loss_scale", 1.0) if isinstance(cfg, dict) else np.nan,
                "cap_non_up": r["cap_non_up"],
                "liq_other": r["liq_other"],
                "oos_sortino": r["oos_sortino"],
                "choppy_top_bottom": r["震荡_top_bottom"],
                "calmar": r["calmar"],
                "mdd": r["mdd"],
            }
        )
    pd.DataFrame(sens).to_csv(os.path.join(out_dir, "param_sensitivity_analysis.csv"), index=False, encoding="utf-8-sig")

    base_e_ret = ret_map["base_E_foundation"]
    d_over = pd.DataFrame(
        [
            {"rule": "hs300_ret20>5%", **_overheat_trigger_counts(base_e_ret, oos_s, oos_e, 0.05, 999.0, 1.0)},
            {"rule": "top3_ind_ret20>8%", **_overheat_trigger_counts(base_e_ret, oos_s, oos_e, 999.0, 0.08, 1.0)},
            {"rule": "market_turn>q90", **_overheat_trigger_counts(base_e_ret, oos_s, oos_e, 999.0, 999.0, 0.90)},
            {"rule": "hs300_ret20>3%", **_overheat_trigger_counts(base_e_ret, oos_s, oos_e, 0.03, 999.0, 1.0)},
            {"rule": "top3_ind_ret20>5%", **_overheat_trigger_counts(base_e_ret, oos_s, oos_e, 999.0, 0.05, 1.0)},
            {"rule": "market_turn>q75", **_overheat_trigger_counts(base_e_ret, oos_s, oos_e, 999.0, 999.0, 0.75)},
        ]
    )
    d_over.to_csv(os.path.join(out_dir, "diagnostic_overheat_trigger_2021_2025.csv"), index=False, encoding="utf-8-sig")

    ra = _remove_add_eval(panel_oos, hold_map["base_E_foundation"], hold_map["exp2_1_E_xq_loose40"], oos_s, oos_e)
    ra.to_csv(os.path.join(out_dir, "diagnostic_xq_remove_add_eval_2021_2025.csv"), index=False, encoding="utf-8-sig")

    best = rank.iloc[0]
    with open(os.path.join(rep_dir, "baseline_v6_1_震荡优化诊断与实验报告.md"), "w", encoding="utf-8") as f:
        f.write("# baseline_v6.1 震荡优化诊断与实验报告\n\n")
        f.write("- 评估口径：训练2010-2020，样本外2021-2025，单边成本0.1%。\n")
        f.write("- 排序优先级：oos_sortino > 震荡_top_bottom > calmar > mdd。\n\n")
        for _, r in rank.iterrows():
            f.write(
                f"- {r['experiment']}: oos_sortino={r['oos_sortino']:.6f}, 震荡_top_bottom={r['震荡_top_bottom']:.6f}, calmar={r['calmar']:.6f}, mdd={r['mdd']:.6f}\n"
            )
        f.write("\n")
        f.write(f"- 最终建议：{'升级增强版' if str(best['experiment']).startswith('exp') else '回退E基础版'}（{best['experiment']}）。\n")

    with open(os.path.join(rep_dir, "baseline_v6_1_实盘灰度运行指引.md"), "w", encoding="utf-8") as f:
        f.write("# baseline_v6.1 实盘灰度运行指引\n\n")
        f.write("- 初始仓位：50%。\n")
        f.write("- 升仓条件：连续2个调仓周期净值优于E基础版且oos_sortino>0。\n")
        f.write("- 回退条件：净值连续2期低于E超5% 或 单期回撤>8% 或 oos_sortino连续1期<0。\n")
        f.write("- 回退后：恢复E基础版+50%仓位并重启参数验证。\n")

    print(os.path.join(out_dir, "enhanced_choppy_summary_2010_2025.csv"))
    print(os.path.join(out_dir, "enhanced_oos_eval_2010_2025.csv"))
    print(os.path.join(out_dir, "diagnostic_overheat_trigger_2021_2025.csv"))
    print(os.path.join(out_dir, "diagnostic_xq_remove_add_eval_2021_2025.csv"))
    print(os.path.join(rep_dir, "baseline_v6_1_震荡优化诊断与实验报告.md"))


if __name__ == "__main__":
    main()
