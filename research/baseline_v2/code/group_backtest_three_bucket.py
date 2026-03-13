import os
from typing import Dict

import numpy as np
import pandas as pd


def _assign_bucket(s: pd.Series) -> pd.Series:
    r = s.rank(pct=True, method="first")
    out = pd.Series(index=s.index, dtype=object)
    out[r <= 0.3] = "Bottom30"
    out[r >= 0.7] = "Top30"
    out[(r > 0.3) & (r < 0.7)] = "Middle40"
    return out


def _apply_outlier_trim(df: pd.DataFrame, factor_col: str = "factor_z", trim_q: float = 0.05) -> pd.DataFrame:
    lo = df.groupby("date")[factor_col].transform(lambda s: s.quantile(trim_q))
    hi = df.groupby("date")[factor_col].transform(lambda s: s.quantile(1 - trim_q))
    return df[(df[factor_col] >= lo) & (df[factor_col] <= hi)].copy()


def _apply_holding_lock(df: pd.DataFrame, hold_lock_days: int = 10) -> pd.DataFrame:
    dates = sorted(df["date"].dropna().unique().tolist())
    if not dates:
        return df.iloc[0:0].copy()
    keep = []
    i = 0
    while i < len(dates):
        keep.append(dates[i])
        i += hold_lock_days
    keep = set(keep)
    return df[df["date"].isin(keep)].copy()


def run_three_bucket_backtest(
    panel: pd.DataFrame,
    horizon_col: str = "fwd_ret_2w",
    hold_lock_days: int = 10,
    trim_q: float = 0.05,
) -> Dict[str, pd.DataFrame]:
    need = {"date", "stock_symbol", "factor_z", horizon_col}
    miss = need - set(panel.columns)
    if miss:
        raise ValueError(f"panel 缺少字段: {sorted(miss)}")
    df = panel.dropna(subset=["date", "stock_symbol", "factor_z", horizon_col]).copy()
    if df.empty:
        empty_group = pd.DataFrame(columns=["date", "Bottom30", "Middle40", "Top30"])
        empty_ls = pd.DataFrame(columns=["date", "long_short", "long_short_curve"])
        empty_summary = pd.DataFrame(
            {
                "metric": ["mean_top", "mean_middle", "mean_bottom", "mean_top_minus_bottom", "hit_ratio_top_gt_bottom", "obs_days", "max_drawdown_ls_curve"],
                "value": [np.nan, np.nan, np.nan, np.nan, np.nan, 0.0, np.nan],
            }
        )
        return {"group_ret": empty_group, "ls_curve": empty_ls, "summary": empty_summary}
    df = _apply_outlier_trim(df, factor_col="factor_z", trim_q=trim_q)
    df = _apply_holding_lock(df, hold_lock_days=hold_lock_days)
    df["bucket"] = df.groupby("date")["factor_z"].transform(_assign_bucket)
    df = df.dropna(subset=["bucket"]).copy()
    group_ret = df.groupby(["date", "bucket"], as_index=False)[horizon_col].mean()
    pivot = group_ret.pivot(index="date", columns="bucket", values=horizon_col).sort_index()
    for c in ["Bottom30", "Middle40", "Top30"]:
        if c not in pivot.columns:
            pivot[c] = np.nan
    pivot = pivot[["Bottom30", "Middle40", "Top30"]]
    ls = pd.DataFrame(index=pivot.index)
    ls["long_short"] = pivot["Top30"] - pivot["Bottom30"]
    ls["long_short_curve"] = (1 + ls["long_short"].fillna(0)).cumprod()
    rolling_peak = ls["long_short_curve"].cummax()
    drawdown = ls["long_short_curve"] / rolling_peak - 1.0
    max_drawdown = float(drawdown.min()) if not drawdown.empty else float("nan")
    obs_days = int(ls["long_short"].dropna().shape[0])
    summary = pd.DataFrame(
        {
            "metric": ["mean_top", "mean_middle", "mean_bottom", "mean_top_minus_bottom", "hit_ratio_top_gt_bottom", "obs_days", "max_drawdown_ls_curve"],
            "value": [
                float(pivot["Top30"].mean()),
                float(pivot["Middle40"].mean()),
                float(pivot["Bottom30"].mean()),
                float((pivot["Top30"] - pivot["Bottom30"]).mean()),
                float((pivot["Top30"] > pivot["Bottom30"]).mean()),
                float(obs_days),
                max_drawdown,
            ],
        }
    )
    return {"group_ret": pivot.reset_index(), "ls_curve": ls.reset_index(), "summary": summary}


def save_three_bucket_results(results: Dict[str, pd.DataFrame], out_dir: str, prefix: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    results["group_ret"].to_csv(os.path.join(out_dir, f"group_ret_{prefix}.csv"), index=False, encoding="utf-8-sig")
    results["ls_curve"].to_csv(os.path.join(out_dir, f"long_short_{prefix}.csv"), index=False, encoding="utf-8-sig")
    results["summary"].to_csv(os.path.join(out_dir, f"summary_{prefix}.csv"), index=False, encoding="utf-8-sig")
