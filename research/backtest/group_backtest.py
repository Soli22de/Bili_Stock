import os
from typing import Dict

import numpy as np
import pandas as pd


def _safe_qcut(s: pd.Series, q: int = 5) -> pd.Series:
    x = s.rank(method="first")
    if x.nunique() < q:
        return pd.Series(np.nan, index=s.index)
    return pd.qcut(x, q=q, labels=False) + 1


def run_group_backtest(panel: pd.DataFrame, horizon_col: str) -> Dict[str, pd.DataFrame]:
    need = {"date", "stock_symbol", "factor_z", horizon_col}
    miss = need - set(panel.columns)
    if miss:
        raise ValueError(f"panel 缺少字段: {sorted(miss)}")
    df = panel.dropna(subset=["date", "stock_symbol", "factor_z", horizon_col]).copy()
    if df.empty:
        empty_group = pd.DataFrame(columns=["date", "Q1", "Q2", "Q3", "Q4", "Q5"])
        empty_ls = pd.DataFrame(columns=["date", "long_short", "long_short_curve"])
        empty_summary = pd.DataFrame(
            {
                "metric": [
                    "mean_Q1",
                    "mean_Q5",
                    "mean_Q5_minus_Q1",
                    "hit_ratio_Q5_gt_Q1",
                    "obs_days",
                    "max_drawdown_ls_curve",
                ],
                "value": [np.nan, np.nan, np.nan, np.nan, 0.0, np.nan],
            }
        )
        return {"group_ret": empty_group, "ls_curve": empty_ls, "summary": empty_summary}
    df["group"] = df.groupby("date")["factor_z"].transform(lambda s: _safe_qcut(s, q=5))
    df = df.dropna(subset=["group"]).copy()
    df["group"] = df["group"].astype(int)
    group_ret = df.groupby(["date", "group"], as_index=False)[horizon_col].mean()
    pivot = group_ret.pivot(index="date", columns="group", values=horizon_col).sort_index()
    pivot = pivot.rename(columns={1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4", 5: "Q5"})
    for c in ["Q1", "Q2", "Q3", "Q4", "Q5"]:
        if c not in pivot.columns:
            pivot[c] = np.nan
    pivot = pivot[["Q1", "Q2", "Q3", "Q4", "Q5"]]
    ls = pd.DataFrame(index=pivot.index)
    ls["long_short"] = pivot["Q5"] - pivot["Q1"]
    ls["long_short_curve"] = (1 + ls["long_short"].fillna(0)).cumprod()
    rolling_peak = ls["long_short_curve"].cummax()
    drawdown = ls["long_short_curve"] / rolling_peak - 1.0
    max_drawdown = float(drawdown.min()) if not drawdown.empty else float("nan")
    obs_days = int(ls["long_short"].dropna().shape[0])
    summary = pd.DataFrame(
        {
            "metric": [
                "mean_Q1",
                "mean_Q5",
                "mean_Q5_minus_Q1",
                "hit_ratio_Q5_gt_Q1",
                "obs_days",
                "max_drawdown_ls_curve",
            ],
            "value": [
                float(pivot["Q1"].mean()),
                float(pivot["Q5"].mean()),
                float((pivot["Q5"] - pivot["Q1"]).mean()),
                float((pivot["Q5"] > pivot["Q1"]).mean()),
                float(obs_days),
                max_drawdown,
            ],
        }
    )
    return {"group_ret": pivot.reset_index(), "ls_curve": ls.reset_index(), "summary": summary}


def save_group_backtest_results(results: Dict[str, pd.DataFrame], out_dir: str, horizon_name: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    results["group_ret"].to_csv(os.path.join(out_dir, f"group_ret_{horizon_name}.csv"), index=False, encoding="utf-8-sig")
    results["ls_curve"].to_csv(os.path.join(out_dir, f"long_short_{horizon_name}.csv"), index=False, encoding="utf-8-sig")
    results["summary"].to_csv(os.path.join(out_dir, f"summary_{horizon_name}.csv"), index=False, encoding="utf-8-sig")
