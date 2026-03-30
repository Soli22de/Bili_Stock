import os
import sys

import baostock as bs
import numpy as np
import pandas as pd


def _load_hs300(start_date: str, end_date: str) -> pd.DataFrame:
    lg = bs.login()
    if str(lg.error_code) != "0":
        raise RuntimeError(f"baostock login failed: {lg.error_msg}")
    rs = bs.query_history_k_data_plus("sh.000300", "date,close", start_date, end_date, "d")
    if str(rs.error_code) != "0":
        bs.logout()
        raise RuntimeError(f"query_history_k_data_plus failed: {rs.error_msg}")
    rows = []
    while rs.error_code == "0" and rs.next():
        rows.append(rs.get_row_data())
    bs.logout()
    idx = pd.DataFrame(rows, columns=["date", "close"])
    idx["date"] = pd.to_datetime(idx["date"], errors="coerce").dt.normalize()
    idx["close"] = pd.to_numeric(idx["close"], errors="coerce")
    idx = idx.dropna(subset=["date", "close"]).sort_values("date")
    idx["ret20"] = idx["close"] / idx["close"].shift(20) - 1.0
    idx["regime"] = "震荡"
    idx.loc[idx["ret20"] > 0.02, "regime"] = "上涨"
    idx.loc[idx["ret20"] < -0.02, "regime"] = "下跌"
    return idx[["date", "ret20", "regime"]]


def _max_drawdown_from_returns(s: pd.Series) -> float:
    curve = (1 + s.fillna(0)).cumprod()
    peak = curve.cummax()
    dd = curve / peak - 1.0
    return float(dd.min()) if not dd.empty else float("nan")


def run_regime_analysis(group_ret_csv: str, hs300_df: pd.DataFrame, out_csv: str, out_md: str) -> None:
    g = pd.read_csv(group_ret_csv)
    g["date"] = pd.to_datetime(g["date"], errors="coerce").dt.normalize()
    g = g.dropna(subset=["date"])
    g["long_short"] = g["Top30"] - g["Bottom30"]
    x = g.merge(hs300_df, on="date", how="left")
    x["regime"] = x["regime"].fillna("震荡")
    rows = []
    for rg in ["上涨", "震荡", "下跌"]:
        d = x[x["regime"] == rg].copy()
        if d.empty:
            rows.append({"regime": rg, "obs_days": 0, "hit_ratio": np.nan, "top_bottom": np.nan, "max_drawdown": np.nan})
            continue
        hit = float((d["Top30"] > d["Bottom30"]).mean())
        top_bottom = float((d["Top30"] - d["Bottom30"]).mean())
        mdd = _max_drawdown_from_returns(d["long_short"])
        rows.append({"regime": rg, "obs_days": int(d["long_short"].dropna().shape[0]), "hit_ratio": hit, "top_bottom": top_bottom, "max_drawdown": mdd})
    res = pd.DataFrame(rows)
    res.to_csv(out_csv, index=False, encoding="utf-8-sig")
    best = res.sort_values("top_bottom", ascending=False).iloc[0]["regime"] if not res["top_bottom"].isna().all() else "未知"
    worst = res.sort_values("top_bottom", ascending=True).iloc[0]["regime"] if not res["top_bottom"].isna().all() else "未知"
    lines = []
    lines.append("# baseline_v2 市场环境分段分析（2022-2025）")
    lines.append("")
    lines.append("- 环境划分：HS300 20日涨跌幅 >2% 上涨，<-2% 下跌，其余震荡")
    lines.append("- 策略配置：三项优化 + 行业中性（含其他行业处理） + 流动性阈值60%")
    lines.append("")
    lines.append("| 市场环境 | obs_days | hit_ratio | max_drawdown | top-bottom |")
    lines.append("|---|---:|---:|---:|---:|")
    for _, r in res.iterrows():
        lines.append(
            f"| {r['regime']} | {int(r['obs_days'])} | {r['hit_ratio']:.4f} | {r['max_drawdown']:.4f} | {r['top_bottom']:.6f} |"
        )
    lines.append("")
    lines.append("## 结论")
    lines.append("")
    lines.append(f"- 表现最好环境（按top-bottom）：{best}")
    lines.append(f"- 表现最弱环境（按top-bottom）：{worst}")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def main():
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if root not in sys.path:
        sys.path.insert(0, root)
    out_dir = os.path.join(root, "research", "output")
    hs300 = _load_hs300("2022-01-01", "2025-12-31")
    group_ret_csv = os.path.join(out_dir, "group_ret_rebalance_momentum_indneutral_liq60_2w_2022_2025.csv")
    out_csv = os.path.join(out_dir, "regime_metrics_baseline_v2_2022_2025.csv")
    out_md = os.path.join(out_dir, "baseline_v2_market_regime_analysis_2022_2025.md")
    run_regime_analysis(group_ret_csv, hs300, out_csv, out_md)
    print(out_csv)
    print(out_md)


if __name__ == "__main__":
    main()
