import argparse
import ast
import os
import sys

import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5
from research.baseline_v6_1.code.run_baseline_v6_v61_suite import _enrich_from_stock_data, _run_one


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", default="2019-01-01")
    ap.add_argument("--end-date", default="2025-12-31")
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()
    tag = f"{args.start_date[:4]}_{args.end_date[:4]}"

    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    rep_dir = os.path.join(ROOT, "research", "baseline_v6_1", "report")
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)

    panel = _prepare_panel_v5(start_date=args.start_date, end_date=args.end_date)
    panel, px_map = _enrich_from_stock_data(panel)
    panel = panel[(panel["date"] >= pd.Timestamp(args.start_date)) & (panel["date"] <= pd.Timestamp(args.end_date))].copy()

    exps = [
        {
            "experiment": "base_v6_1",
            "hold_step": 10,
            "liq_other": 0.60,
            "cap_non_up": 0.15,
            "cap_up": 0.25,
            "with_takeprofit": True,
            "risk_cfg": {},
        },
        {
            "experiment": "base_E_foundation",
            "hold_step": 12,
            "liq_other": 0.55,
            "cap_non_up": 0.10,
            "cap_up": 0.25,
            "with_takeprofit": True,
            "risk_cfg": {"non_up_vol_q": 0.65, "choppy_loss_scale": 0.50},
        },
        {
            "experiment": "exp1_E_overheat_light",
            "hold_step": 12,
            "liq_other": 0.55,
            "cap_non_up": 0.10,
            "cap_up": 0.25,
            "with_takeprofit": True,
            "risk_cfg": {
                "non_up_vol_q": 0.65,
                "choppy_loss_scale": 0.50,
                "overheat_hs_trigger": 0.05,
                "overheat_ind_trigger": 0.08,
                "overheat_turn_q": 0.90,
                "overheat_hs_release": 0.02,
                "overheat_ind_release": 0.03,
                "overheat_cap_non_up": 0.12,
                "overheat_cap_up": 0.27,
            },
        },
        {
            "experiment": "exp2_E_xq_warn_scale55",
            "hold_step": 12,
            "liq_other": 0.55,
            "cap_non_up": 0.10,
            "cap_up": 0.25,
            "with_takeprofit": True,
            "risk_cfg": {"non_up_vol_q": 0.65, "choppy_loss_scale": 0.55, "xq_enable": True, "xq_warn_drop": 0.25, "xq_recover_rise": 0.10},
        },
    ]

    rows = []
    total = len(exps)
    for idx, e in enumerate(exps, 1):
        out_ret = os.path.join(out_dir, f"{e['experiment']}_group_ret_{tag}.csv")
        out_hold = os.path.join(out_dir, f"{e['experiment']}_holdings_{tag}.csv")
        out_attr = os.path.join(out_dir, f"{e['experiment']}_attribution_{tag}.csv")
        out_sf = os.path.join(out_dir, f"{e['experiment']}_sell_fly_{tag}.csv")
        print(f"[{idx}/{total}] start {e['experiment']}", flush=True)
        if args.resume and os.path.exists(out_ret):
            d = pd.read_csv(out_ret)
            spread = d["Top30_net"] - d["Bottom30"]
            ann_factor = 26.0
            avg = float(spread.mean()) if not spread.empty else float("nan")
            vol = float(spread.std(ddof=0)) if not spread.empty else float("nan")
            ann_ret = float((1.0 + avg) ** ann_factor - 1.0) if pd.notna(avg) else float("nan")
            ann_vol = float(vol * (ann_factor ** 0.5)) if pd.notna(vol) else float("nan")
            neg = spread[spread < 0]
            downside = float((neg.pow(2).mean()) ** 0.5) if not neg.empty else 0.0
            ann_downside = float(downside * (ann_factor ** 0.5))
            curve = (1 + spread.fillna(0)).cumprod()
            dd = curve / curve.cummax() - 1.0
            mdd = float(dd.min()) if not dd.empty else float("nan")
            calmar = ann_ret / abs(mdd) if pd.notna(ann_ret) and pd.notna(mdd) and mdd != 0 else float("nan")
            sortino = ann_ret / ann_downside if pd.notna(ann_downside) and ann_downside > 0 else float("nan")
            choppy = float((d[d["regime"] == "震荡"]["Top30_net"] - d[d["regime"] == "震荡"]["Bottom30"]).mean()) if "regime" in d.columns else float("nan")
            rows.append(
                {
                    **e,
                    "ann_ret": ann_ret,
                    "calmar": calmar,
                    "sortino": sortino,
                    "mdd": mdd,
                    "turnover": float(d["one_way_turnover"].mean()) if "one_way_turnover" in d.columns else float("nan"),
                    "震荡_top_bottom": choppy,
                }
            )
            print(f"[{idx}/{total}] resume-hit {e['experiment']}", flush=True)
            continue
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
        ret.to_csv(out_ret, index=False, encoding="utf-8-sig")
        if hold is not None and not hold.empty:
            hold.to_csv(out_hold, index=False, encoding="utf-8-sig")
        if attr is not None and not attr.empty:
            attr.to_csv(out_attr, index=False, encoding="utf-8-sig")
        if sf is not None and not sf.empty:
            sf.to_csv(out_sf, index=False, encoding="utf-8-sig")
        if risk_log is not None and not risk_log.empty:
            risk_log.to_csv(os.path.join(out_dir, f"{e['experiment']}_risk_log_{tag}.csv"), index=False, encoding="utf-8-sig")
        print(f"[{idx}/{total}] done {e['experiment']}", flush=True)

    res = pd.DataFrame(rows)
    train_s = "2010-01-01"
    train_e = "2020-12-31"
    oos_s = "2021-01-01"
    oos_e = "2025-12-31"
    panel_train = panel[(panel["date"] >= pd.Timestamp(train_s)) & (panel["date"] <= pd.Timestamp(train_e))].copy()
    panel_oos = panel[(panel["date"] >= pd.Timestamp(oos_s)) & (panel["date"] <= pd.Timestamp(oos_e))].copy()
    oos_rows = []
    for e in exps:
        m_tr, _, _, _, _, _ = _run_one(
            panel_train,
            px_map,
            hold_step=e["hold_step"],
            liq_other=e["liq_other"],
            cap_non_up=e["cap_non_up"],
            cap_up=e["cap_up"],
            with_takeprofit=e["with_takeprofit"],
            risk_cfg=e.get("risk_cfg", {}),
        )
        m_oos, _, _, _, _, _ = _run_one(
            panel_oos,
            px_map,
            hold_step=e["hold_step"],
            liq_other=e["liq_other"],
            cap_non_up=e["cap_non_up"],
            cap_up=e["cap_up"],
            with_takeprofit=e["with_takeprofit"],
            risk_cfg=e.get("risk_cfg", {}),
        )
        oos_rows.append(
            {
                "experiment": e["experiment"],
                "train_sortino": m_tr.get("sortino", float("nan")),
                "train_calmar": m_tr.get("calmar", float("nan")),
                "train_mdd": m_tr.get("mdd", float("nan")),
                "oos_sortino": m_oos.get("sortino", float("nan")),
                "oos_calmar": m_oos.get("calmar", float("nan")),
                "oos_mdd": m_oos.get("mdd", float("nan")),
            }
        )
    oos_df = pd.DataFrame(oos_rows)
    oos_df.to_csv(os.path.join(out_dir, f"oos_eval_{tag}.csv"), index=False, encoding="utf-8-sig")
    rank = res.merge(oos_df[["experiment", "oos_sortino"]], on="experiment", how="left")
    rank = rank.sort_values(["oos_sortino", "震荡_top_bottom", "calmar", "mdd"], ascending=[False, False, False, False]).reset_index(drop=True)
    rank.to_csv(os.path.join(out_dir, f"choppy_fix_miniset_summary_{tag}.csv"), index=False, encoding="utf-8-sig")
    sens_rows = []
    for _, r in rank.iterrows():
        cfg = r.get("risk_cfg", {})
        if isinstance(cfg, str):
            try:
                cfg = ast.literal_eval(cfg)
            except Exception:
                cfg = {}
        sens_rows.append(
            {
                "experiment": r["experiment"],
                "non_up_vol_q": cfg.get("non_up_vol_q", 1.0),
                "choppy_loss_scale": cfg.get("choppy_loss_scale", 1.0),
                "cap_non_up": r["cap_non_up"],
                "liq_other": r["liq_other"],
                "oos_sortino": rank.loc[rank["experiment"] == r["experiment"], "oos_sortino"].iloc[0],
                "choppy_top_bottom": r["震荡_top_bottom"],
                "calmar": r["calmar"],
                "mdd": r["mdd"],
            }
        )
    pd.DataFrame(sens_rows).to_csv(os.path.join(out_dir, "param_sensitivity_analysis.csv"), index=False, encoding="utf-8-sig")

    with open(os.path.join(rep_dir, f"choppy_fix_miniset_report_{tag}.md"), "w", encoding="utf-8") as f:
        f.write(f"# 震荡期修复最小实验集（{args.start_date}~{args.end_date}）\n\n")
        f.write("- 对比目标：优先提升样本外Sortino与震荡_top_bottom，再看Calmar与MDD。\n\n")
        for _, r in rank.iterrows():
            f.write(
                f"- {r['experiment']}: oos_sortino={r.get('oos_sortino', float('nan')):.6f}, 震荡_top_bottom={r['震荡_top_bottom']:.6f}, sortino={r['sortino']:.6f}, calmar={r['calmar']:.6f}, mdd={r['mdd']:.6f}, turnover={r['turnover']:.6f}\n"
            )
        best = rank.iloc[0]
        f.write("\n")
        f.write(f"- 推荐方案：{best['experiment']}\n")
        f.write("- 风险提示：\n")
        f.write("  - 当前Calmar与硬约束仍有差距，极端回撤收敛仍需强化。\n")
        f.write("  - 震荡防守参数主要改善日常亏损，不完全覆盖黑天鹅。\n")
        f.write("  - 参数需滚动检验，禁止一次性放大仓位。\n")
        f.write("  - non_up_vol_q上调通常提升弹性但稳定性下降；choppy_loss_scale上调通常收敛回撤但牺牲收益。\n")

    print(os.path.join(out_dir, f"choppy_fix_miniset_summary_{tag}.csv"))
    print(os.path.join(rep_dir, f"choppy_fix_miniset_report_{tag}.md"))


if __name__ == "__main__":
    main()
