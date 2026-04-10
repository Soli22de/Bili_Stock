import argparse
import os

import numpy as np
import pandas as pd
import math


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def _load_live_compare(live_dir: str) -> pd.DataFrame:
    fp = os.path.join(live_dir, "daily_nav_compare.csv")
    if not os.path.exists(fp):
        raise RuntimeError("daily_nav_compare.csv not found, run daily first")
    x = pd.read_csv(fp)
    x["date"] = pd.to_datetime(x["date"], errors="coerce")
    x = x.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return x


def _week_slice(x: pd.DataFrame, days: int = 5) -> pd.DataFrame:
    return x.tail(max(int(days), 1)).copy()


def _mdd(eq: pd.Series) -> float:
    y = pd.to_numeric(eq, errors="coerce")
    dd = y / y.cummax() - 1.0
    return float(dd.min()) if not dd.empty else np.nan


def _load_trades(live_dir: str) -> pd.DataFrame:
    fp = os.path.join(live_dir, "live_trades.csv")
    if not os.path.exists(fp):
        return pd.DataFrame(columns=["date", "stock_symbol", "side", "order_qty", "fill_qty", "order_px", "fill_px", "commission", "tax", "fees"])
    x = pd.read_csv(fp)
    if x is None or x.empty:
        return pd.DataFrame(columns=["date", "stock_symbol", "side", "order_qty", "fill_qty", "order_px", "fill_px", "commission", "tax", "fees"])
    for c in ("date", "stock_symbol", "side", "order_qty", "fill_qty", "order_px", "fill_px", "commission", "tax", "fees"):
        if c not in x.columns:
            x[c] = np.nan
    x["date"] = pd.to_datetime(x["date"], errors="coerce")
    x = x.dropna(subset=["date"]).copy()
    x["order_qty"] = pd.to_numeric(x["order_qty"], errors="coerce")
    x["fill_qty"] = pd.to_numeric(x["fill_qty"], errors="coerce")
    x["order_px"] = pd.to_numeric(x["order_px"], errors="coerce")
    x["fill_px"] = pd.to_numeric(x["fill_px"], errors="coerce")
    x["commission"] = pd.to_numeric(x["commission"], errors="coerce").fillna(0.0)
    x["tax"] = pd.to_numeric(x["tax"], errors="coerce").fillna(0.0)
    x["fees"] = pd.to_numeric(x["fees"], errors="coerce").fillna(0.0)
    return x


def _kpi_weekly(tr: pd.DataFrame, start_dt: pd.Timestamp, end_dt: pd.Timestamp) -> dict:
    if tr is None or tr.empty:
        return {"trades": 0, "turnover": 0.0, "slip_bps": np.nan, "slip_bps_vwap": np.nan, "fill_rate": np.nan, "cost": 0.0}
    t = tr[(tr["date"] >= start_dt.normalize()) & (tr["date"] <= end_dt.normalize())].copy()
    if t.empty:
        return {"trades": 0, "turnover": 0.0, "slip_bps": np.nan, "slip_bps_vwap": np.nan, "fill_rate": np.nan, "cost": 0.0}
    t["notional"] = pd.to_numeric(t["fill_px"], errors="coerce") * pd.to_numeric(t["fill_qty"], errors="coerce")
    def _slip_row(row):
        op = float(row.get("order_px")) if pd.notna(row.get("order_px")) else np.nan
        fp = float(row.get("fill_px")) if pd.notna(row.get("fill_px")) else np.nan
        sd = str(row.get("side")).upper() if pd.notna(row.get("side")) else ""
        if not (pd.notna(op) and pd.notna(fp) and op > 0):
            return np.nan
        if sd == "BUY":
            return (fp - op) / op * 10000.0
        if sd == "SELL":
            return (op - fp) / op * 10000.0
        return (fp - op) / op * 10000.0
    t["slip_bps"] = t.apply(_slip_row, axis=1)
    def _fill_rate(row):
        oq = float(row.get("order_qty")) if pd.notna(row.get("order_qty")) else np.nan
        fq = float(row.get("fill_qty")) if pd.notna(row.get("fill_qty")) else np.nan
        if pd.notna(oq) and oq > 0 and pd.notna(fq):
            return float(fq) / float(oq)
        return np.nan
    t["fill_rate"] = t.apply(_fill_rate, axis=1)
    out = {}
    out["trades"] = int(len(t))
    out["turnover"] = float(pd.to_numeric(t["notional"], errors="coerce").sum(skipna=True))
    out["slip_bps"] = float(pd.to_numeric(t["slip_bps"], errors="coerce").mean(skipna=True)) if not t["slip_bps"].dropna().empty else np.nan
    if not t["slip_bps"].dropna().empty:
        w = pd.to_numeric(t["notional"], errors="coerce").fillna(0.0)
        x = pd.to_numeric(t["slip_bps"], errors="coerce")
        denom = float(w.sum())
        out["slip_bps_vwap"] = float((w * x).sum() / denom) if denom > 0 else np.nan
    else:
        out["slip_bps_vwap"] = np.nan
    out["fill_rate"] = float(pd.to_numeric(t["fill_rate"], errors="coerce").mean(skipna=True)) if not t["fill_rate"].dropna().empty else np.nan
    out["cost"] = float((t["commission"] + t["tax"] + t["fees"]).sum(skipna=True))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live-dir", default=os.path.join(ROOT, "research", "baseline_v6_1", "output", "live"))
    ap.add_argument("--baseline-ret", default=os.path.join(ROOT, "research", "baseline_v6_1", "output", "choppy_fix_B_hold12_cap10_group_ret_2010_2025.csv"))
    ap.add_argument("--weekly-bars", type=int, default=5)
    args = ap.parse_args()

    weekly_fp = os.path.join(args.live_dir, "weekly_report.md")
    x = _load_live_compare(args.live_dir)
    w = _week_slice(x, args.weekly_bars)
    base = pd.read_csv(args.baseline_ret)
    base["date"] = pd.to_datetime(base["date"], errors="coerce")
    base["regime"] = base["regime"].astype(str)
    m = w.merge(base[["date", "regime"]], on="date", how="left")
    choppy = m[m["regime"] == "震荡"]
    week_choppy_top_bottom = float(pd.to_numeric(choppy["spread"], errors="coerce").mean()) if not choppy.empty else np.nan
    week_mdd = _mdd(w["equity"])
    tr = _load_trades(args.live_dir)
    wk = _kpi_weekly(tr, pd.Timestamp(w["date"].iloc[0]), pd.Timestamp(w["date"].iloc[-1]))
    last_decision_fp = os.path.join(args.live_dir, "gray_deployment_decision.csv")
    action = "hold_50"
    reasons = ""
    if os.path.exists(last_decision_fp):
        d = pd.read_csv(last_decision_fp)
        if not d.empty:
            action = str(d["action"].iloc[0])
            reasons = "" if pd.isna(d["reasons"].iloc[0]) else str(d["reasons"].iloc[0])
    with open(weekly_fp, "w", encoding="utf-8") as f:
        f.write("# baseline_v6.1 周报\n\n")
        f.write(f"- 周期末日期：{pd.Timestamp(w['date'].iloc[-1]).strftime('%Y-%m-%d')}\n")
        f.write(f"- 周度震荡_top_bottom：{week_choppy_top_bottom:.6f}\n" if pd.notna(week_choppy_top_bottom) else "- 周度震荡_top_bottom：nan\n")
        f.write(f"- 周度MDD：{week_mdd:.2%}\n" if pd.notna(week_mdd) else "- 周度MDD：nan\n")
        f.write(f"- 周度成交笔数：{wk['trades']}\n")
        f.write(f"- 周度成交额：{wk['turnover']:.2f}\n")
        f.write(f"- 周度滑点均值(bps)：{wk['slip_bps']:.2f}\n" if not math.isnan(wk["slip_bps"]) else "- 周度滑点均值(bps)：nan\n")
        f.write(f"- 周度滑点加权均值(bps)：{wk['slip_bps_vwap']:.2f}\n" if not math.isnan(wk["slip_bps_vwap"]) else "- 周度滑点加权均值(bps)：nan\n")
        f.write(f"- 周度平均成交率：{wk['fill_rate']:.2%}\n" if not math.isnan(wk["fill_rate"]) else "- 周度平均成交率：nan\n")
        f.write(f"- 周度交易成本：{wk['cost']:.2f}\n")
        f.write(f"- 当前动作建议：{action}\n")
        f.write(f"- 原因：{reasons}\n")
        f.write("- 下周执行：按动作建议执行仓位，若出现单周MDD>8%则暂停并复核。\n")
    print(weekly_fp)


if __name__ == "__main__":
    main()
