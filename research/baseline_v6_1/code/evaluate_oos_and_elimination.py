import os
import numpy as np
import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def _load_data():
    eq_path = os.path.join(ROOT, "research", "baseline_v6_1", "output", "strategy_comparison_equity_curves_2019_2025.csv")
    bm_path = os.path.join(ROOT, "research", "baseline_v6_1", "output", "csi300_benchmark_2019_2025.csv")
    eq = pd.read_csv(eq_path)
    bm = pd.read_csv(bm_path)
    eq["date"] = pd.to_datetime(eq["date"])
    bm["date"] = pd.to_datetime(bm["date"])
    eq = eq.set_index("date").sort_index()
    bm = bm.set_index("date").sort_index()
    eq = eq.join(bm[["CSI300_Equity"]], how="left")
    eq["CSI300_Equity"] = eq["CSI300_Equity"].ffill().bfill()
    return eq


def _to_period_ret(equity: pd.Series, step_days: int = 10) -> pd.Series:
    s = equity.dropna().sort_index()
    dates = s.index.to_list()
    keep = []
    i = 0
    while i < len(dates):
        keep.append(dates[i])
        i += step_days
    x = s[s.index.isin(keep)].copy()
    r = x.pct_change().dropna()
    return r


def _metrics_from_ret(ret: pd.Series) -> dict:
    if ret.empty:
        return {"ann_ret": np.nan, "ann_vol": np.nan, "sharpe": np.nan, "sortino": np.nan, "mdd": np.nan, "calmar": np.nan}
    ann_factor = 26.0
    avg = float(ret.mean())
    vol = float(ret.std(ddof=0))
    ann_ret = float((1 + avg) ** ann_factor - 1.0)
    ann_vol = float(vol * np.sqrt(ann_factor))
    neg = ret[ret < 0]
    downside = float(np.sqrt((neg.pow(2).mean()))) if not neg.empty else 0.0
    ann_down = downside * np.sqrt(ann_factor)
    sharpe = ann_ret / ann_vol if ann_vol > 0 else np.nan
    sortino = ann_ret / ann_down if ann_down > 0 else np.nan
    curve = (1 + ret).cumprod()
    dd = curve / curve.cummax() - 1.0
    mdd = float(dd.min()) if not dd.empty else np.nan
    calmar = ann_ret / abs(mdd) if pd.notna(mdd) and mdd != 0 else np.nan
    return {"ann_ret": ann_ret, "ann_vol": ann_vol, "sharpe": sharpe, "sortino": sortino, "mdd": mdd, "calmar": calmar}


def _underperform_12m(strategy_ret: pd.Series, benchmark_ret: pd.Series) -> bool:
    a = strategy_ret.resample("ME").apply(lambda x: (1 + x).prod() - 1)
    b = benchmark_ret.resample("ME").apply(lambda x: (1 + x).prod() - 1)
    z = pd.concat([a.rename("s"), b.rename("b")], axis=1).dropna()
    if len(z) < 12:
        return False
    flag = (z["s"] < z["b"]).astype(int)
    roll = flag.rolling(12).sum()
    return bool((roll == 12).any())


def main():
    df = _load_data()
    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "report")
    os.makedirs(out_dir, exist_ok=True)

    train_mask = (df.index >= pd.Timestamp("2019-01-01")) & (df.index <= pd.Timestamp("2022-12-31"))
    oos_mask = (df.index >= pd.Timestamp("2023-01-01")) & (df.index <= pd.Timestamp("2025-12-31"))

    benchmark_train = _to_period_ret(df.loc[train_mask, "CSI300_Equity"])
    benchmark_oos = _to_period_ret(df.loc[oos_mask, "CSI300_Equity"])

    rows = []
    for col in [c for c in df.columns if c.endswith("_Equity") and c != "CSI300_Equity"]:
        r_train = _to_period_ret(df.loc[train_mask, col])
        r_oos = _to_period_ret(df.loc[oos_mask, col])
        m_train = _metrics_from_ret(r_train)
        m_oos = _metrics_from_ret(r_oos)
        elim_12m = _underperform_12m(r_oos, benchmark_oos)
        elim_calmar = pd.notna(m_oos["calmar"]) and m_oos["calmar"] < 0
        elim_mdd = pd.notna(m_oos["mdd"]) and m_oos["mdd"] < -0.30
        eliminate = bool(elim_12m or elim_calmar or elim_mdd)
        rows.append(
            {
                "strategy": col,
                "train_ann_ret": m_train["ann_ret"],
                "train_ann_vol": m_train["ann_vol"],
                "train_sharpe": m_train["sharpe"],
                "train_sortino": m_train["sortino"],
                "train_mdd": m_train["mdd"],
                "train_calmar": m_train["calmar"],
                "oos_ann_ret": m_oos["ann_ret"],
                "oos_ann_vol": m_oos["ann_vol"],
                "oos_sharpe": m_oos["sharpe"],
                "oos_sortino": m_oos["sortino"],
                "oos_mdd": m_oos["mdd"],
                "oos_calmar": m_oos["calmar"],
                "elim_underperform_12m": elim_12m,
                "elim_calmar_negative": bool(elim_calmar),
                "elim_mdd_over_30pct": bool(elim_mdd),
                "eliminate": eliminate,
            }
        )

    if rows:
        out = pd.DataFrame(rows).sort_values(["eliminate", "oos_calmar"], ascending=[True, False])
    else:
        out = pd.DataFrame(
            columns=[
                "strategy",
                "train_ann_ret",
                "train_ann_vol",
                "train_sharpe",
                "train_sortino",
                "train_mdd",
                "train_calmar",
                "oos_ann_ret",
                "oos_ann_vol",
                "oos_sharpe",
                "oos_sortino",
                "oos_mdd",
                "oos_calmar",
                "elim_underperform_12m",
                "elim_calmar_negative",
                "elim_mdd_over_30pct",
                "eliminate",
            ]
        )
    out_csv = os.path.join(out_dir, "oos_elimination_2019_2025.csv")
    out.to_csv(out_csv, index=False, encoding="utf-8-sig")

    keep = out[(~out["eliminate"]) & (out["strategy"].str.startswith("E3"))].copy()
    keep_csv = os.path.join(out_dir, "e3_keep_list_2019_2025.csv")
    keep.to_csv(keep_csv, index=False, encoding="utf-8-sig")

    md = os.path.join(out_dir, "oos_elimination_report.md")
    with open(md, "w", encoding="utf-8") as f:
        f.write("# 样本外与淘汰规则评估\n\n")
        f.write("- 训练期：2019-2022\n")
        f.write("- 样本外：2023-2025\n")
        f.write("- 淘汰规则：连续12个月跑输基准 或 卡玛<0 或 最大回撤>30%\n\n")
        if not keep.empty:
            f.write("## 保留策略（E3系列）\n\n")
            for _, r in keep.iterrows():
                f.write(f"- {r['strategy']}: OOS年化={r['oos_ann_ret']:.2%}, OOS卡玛={r['oos_calmar']:.3f}, OOS索提诺={r['oos_sortino']:.3f}, OOS回撤={r['oos_mdd']:.2%}\n")
        else:
            f.write("## 保留策略（E3系列）\n\n- 无满足条件策略\n")

    print(out_csv)
    print(keep_csv)
    print(md)


if __name__ == "__main__":
    main()
