import argparse
import os
from typing import List

import numpy as np
import pandas as pd
from datetime import datetime
import math


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))


def _must_columns(df: pd.DataFrame, cols: List[str], name: str):
    miss = [c for c in cols if c not in df.columns]
    if miss:
        raise RuntimeError(f"{name} missing columns: {miss}")


def _read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise RuntimeError(f"file not exists: {path}")
    x = pd.read_csv(path)
    if x.empty:
        raise RuntimeError(f"file empty: {path}")
    return x


def _load_live_equity(path: str) -> pd.DataFrame:
    x = _read_csv(path)
    _must_columns(x, ["date"], "live_equity")
    x["date"] = pd.to_datetime(x["date"], errors="coerce")
    x = x.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if "spread" in x.columns:
        x["spread"] = pd.to_numeric(x["spread"], errors="coerce")
        x["equity"] = (1 + x["spread"].fillna(0)).cumprod()
    elif "equity" in x.columns:
        x["equity"] = pd.to_numeric(x["equity"], errors="coerce")
        x["spread"] = x["equity"].pct_change().fillna(0.0)
    else:
        raise RuntimeError("live_equity must include spread or equity")
    return x[["date", "spread", "equity"]]


def _load_baseline(path: str) -> pd.DataFrame:
    x = _read_csv(path)
    _must_columns(x, ["date", "Top30_net", "Bottom30"], "baseline")
    x["date"] = pd.to_datetime(x["date"], errors="coerce")
    x = x.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    x["spread"] = pd.to_numeric(x["Top30_net"], errors="coerce") - pd.to_numeric(x["Bottom30"], errors="coerce")
    x["equity"] = (1 + x["spread"].fillna(0)).cumprod()
    return x[["date", "spread", "equity"]]


def _load_holdings(path: str) -> tuple[pd.DataFrame, List[str]]:
    warns = []
    x = _read_csv(path)
    _must_columns(x, ["date", "stock_symbol", "weight"], "live_holdings")
    x["date"] = pd.to_datetime(x["date"], errors="coerce")
    x["weight"] = pd.to_numeric(x["weight"], errors="coerce")
    x = x.dropna(subset=["date", "stock_symbol"]).copy()
    ww = x.groupby("date")["weight"].sum().reset_index(name="w")
    bad = ww[(ww["w"] < 0.95) | (ww["w"] > 1.05)]
    if not bad.empty:
        warns.append(f"holding weight out of range rows={len(bad)}")
    return x, warns


def _load_risk(path: str) -> pd.DataFrame:
    x = _read_csv(path)
    _must_columns(x, ["date", "trigger_type", "subject", "value"], "live_risk_log")
    x["date"] = pd.to_datetime(x["date"], errors="coerce")
    x = x.dropna(subset=["date"]).copy()
    return x


def _load_trades(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=["date", "stock_symbol", "side", "order_qty", "fill_qty", "order_px", "fill_px", "commission", "tax", "fees"])
    x = pd.read_csv(path)
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


def _kpi_from_trades(tr: pd.DataFrame, day: pd.Timestamp) -> dict:
    if tr is None or tr.empty:
        return {
            "trades_count": 0,
            "turnover_notional": 0.0,
            "avg_slippage_bps": np.nan,
            "vwap_slippage_bps": np.nan,
            "avg_fill_rate": np.nan,
            "total_cost": 0.0,
        }
    t = tr[tr["date"].dt.normalize() == day.normalize()].copy()
    if t.empty:
        return {
            "trades_count": 0,
            "turnover_notional": 0.0,
            "avg_slippage_bps": np.nan,
            "vwap_slippage_bps": np.nan,
            "avg_fill_rate": np.nan,
            "total_cost": 0.0,
        }
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
    k = {}
    k["trades_count"] = int(len(t))
    k["turnover_notional"] = float(pd.to_numeric(t["notional"], errors="coerce").sum(skipna=True))
    k["avg_slippage_bps"] = float(pd.to_numeric(t["slip_bps"], errors="coerce").mean(skipna=True)) if not t["slip_bps"].dropna().empty else np.nan
    if not t["slip_bps"].dropna().empty:
        w = pd.to_numeric(t["notional"], errors="coerce").fillna(0.0)
        x = pd.to_numeric(t["slip_bps"], errors="coerce")
        denom = float(w.sum())
        k["vwap_slippage_bps"] = float((w * x).sum() / denom) if denom > 0 else np.nan
    else:
        k["vwap_slippage_bps"] = np.nan
    k["avg_fill_rate"] = float(pd.to_numeric(t["fill_rate"], errors="coerce").mean(skipna=True)) if not t["fill_rate"].dropna().empty else np.nan
    k["total_cost"] = float((t["commission"] + t["tax"] + t["fees"]).sum(skipna=True))
    return k


def _cycle_tail(x: pd.DataFrame, n: int) -> pd.DataFrame:
    return x.tail(max(int(n), 1)).copy()


def _cycle_mdd(spread: pd.Series) -> float:
    eq = (1 + pd.to_numeric(spread, errors="coerce").fillna(0)).cumprod()
    dd = eq / eq.cummax() - 1.0
    return float(dd.min()) if not dd.empty else np.nan


def _sortino(spread: pd.Series) -> float:
    s = pd.to_numeric(spread, errors="coerce").dropna()
    if s.empty:
        return np.nan
    ann = 26.0
    ann_ret = float((1 + float(s.mean())) ** ann - 1.0)
    neg = s[s < 0]
    downside = float(np.sqrt((neg.pow(2).mean()))) if not neg.empty else 0.0
    ann_down = float(downside * np.sqrt(ann))
    return ann_ret / ann_down if ann_down > 0 else np.nan


def _decide(live: pd.DataFrame, base: pd.DataFrame, cycle_bars: int) -> pd.DataFrame:
    if base is None or base.empty:
        m = live.copy()
        m["base_equity"] = np.nan
    else:
        m = live.merge(base[["date", "equity"]].rename(columns={"equity": "base_equity"}), on="date", how="left")
    m["gap"] = m["equity"] / m["base_equity"] - 1.0
    now = _cycle_tail(m, cycle_bars)
    prev = _cycle_tail(m.iloc[:-cycle_bars] if len(m) > cycle_bars else m.iloc[0:0], cycle_bars)
    gap_now = float(now["gap"].iloc[-1]) if not now.empty else np.nan
    gap_prev = float(prev["gap"].iloc[-1]) if not prev.empty else np.nan
    cyc_mdd = _cycle_mdd(now["spread"])
    oos = _sortino(m["spread"])
    action = "hold_50"
    reasons = []
    if pd.notna(gap_now) and pd.notna(gap_prev) and gap_now < -0.05 and gap_prev < -0.05:
        action = "reduce_to_30"
        reasons.append("relative_nav_below_baseline_5pct_for_two_cycles")
    if pd.notna(cyc_mdd) and cyc_mdd < -0.08:
        action = "pause_revalidate"
        reasons.append("single_cycle_mdd_over_8pct")
    if pd.notna(gap_now) and pd.notna(gap_prev) and gap_now > 0 and gap_prev > 0 and pd.notna(oos) and oos > 0:
        action = "upgrade_to_70"
        reasons = ["outperform_baseline_two_cycles_and_positive_sortino"]
    if pd.notna(oos) and oos < 0:
        reasons.append("oos_sortino_below_zero")
    return pd.DataFrame(
        [
            {
                "latest_date": m["date"].iloc[-1].strftime("%Y-%m-%d"),
                "action": action,
                "gap_now": gap_now,
                "gap_prev": gap_prev,
                "cycle_mdd": cyc_mdd,
                "oos_sortino": oos,
                "reasons": "|".join(reasons),
            }
        ]
    )


def _bootstrap_live(live_dir: str, days: int):
    eq_src = os.path.join(ROOT, "research", "baseline_v6_1", "output", "base_E_foundation_group_ret_2010_2025.csv")
    h_src = os.path.join(ROOT, "research", "baseline_v6_1", "output", "base_E_foundation_holdings_2010_2025.csv")
    r_src = os.path.join(ROOT, "research", "baseline_v6_1", "output", "base_E_foundation_risk_log_2010_2025.csv")
    if not (os.path.exists(eq_src) and os.path.exists(h_src) and os.path.exists(r_src)):
        raise RuntimeError("bootstrap source files not found")
    eq = pd.read_csv(eq_src)
    eq["date"] = pd.to_datetime(eq["date"], errors="coerce")
    eq = eq.dropna(subset=["date"]).sort_values("date").tail(max(int(days), 3)).copy()
    eq["spread"] = pd.to_numeric(eq["Top30_net"], errors="coerce") - pd.to_numeric(eq["Bottom30"], errors="coerce")
    eq[["date", "spread"]].to_csv(os.path.join(live_dir, "live_equity.csv"), index=False, encoding="utf-8-sig")
    dt_set = set(eq["date"].dt.normalize().tolist())
    h = pd.read_csv(h_src)
    h["date"] = pd.to_datetime(h["date"], errors="coerce")
    h = h[h["date"].dt.normalize().isin(dt_set)].copy()
    if "weight" not in h.columns:
        h["weight"] = 1.0 / h.groupby("date")["stock_symbol"].transform("count")
    h[["date", "stock_symbol", "weight"]].to_csv(os.path.join(live_dir, "live_holdings.csv"), index=False, encoding="utf-8-sig")
    r = pd.read_csv(r_src)
    r["date"] = pd.to_datetime(r["date"], errors="coerce")
    r = r[r["date"].dt.normalize().isin(dt_set)].copy()
    need = ["date", "trigger_type", "subject", "value"]
    for c in need:
        if c not in r.columns:
            r[c] = np.nan
    r[need].to_csv(os.path.join(live_dir, "live_risk_log.csv"), index=False, encoding="utf-8-sig")


def _maybe_read_csv(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        x = pd.read_csv(path)
    except Exception:
        return pd.DataFrame()
    return x if x is not None else pd.DataFrame()


def _normalize_dates(x: pd.Series) -> pd.Series:
    return pd.to_datetime(x, errors="coerce").dt.normalize()


def _build_holdings_from_baseline_ret(base_ret: pd.DataFrame) -> pd.DataFrame:
    if base_ret is None or base_ret.empty:
        return pd.DataFrame(columns=["date", "stock_symbol", "weight"])
    if "top_symbols" not in base_ret.columns:
        return pd.DataFrame(columns=["date", "stock_symbol", "weight"])
    rows = []
    for _, r in base_ret.iterrows():
        d = pd.to_datetime(r.get("date"), errors="coerce")
        if pd.isna(d):
            continue
        syms_raw = r.get("top_symbols")
        syms = []
        if pd.notna(syms_raw):
            for s in str(syms_raw).split("|"):
                s = s.strip()
                if s:
                    syms.append(s)
        if not syms:
            continue
        w = 1.0 / float(len(syms))
        for s in syms:
            rows.append({"date": d.normalize(), "stock_symbol": s, "weight": w})
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=["date", "stock_symbol", "weight"])
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["weight"] = pd.to_numeric(out["weight"], errors="coerce")
    out = out.dropna(subset=["date", "stock_symbol"]).copy()
    out = out.sort_values(["date", "stock_symbol"]).reset_index(drop=True)
    return out[["date", "stock_symbol", "weight"]]


def _build_risk_from_baseline_ret(base_ret: pd.DataFrame) -> pd.DataFrame:
    if base_ret is None or base_ret.empty:
        return pd.DataFrame(columns=["date", "trigger_type", "subject", "value"])
    rows = []
    for _, r in base_ret.iterrows():
        d = pd.to_datetime(r.get("date"), errors="coerce")
        if pd.isna(d):
            continue
        trigger = r.get("risk_reason")
        trigger = "none" if pd.isna(trigger) else str(trigger)
        value = r.get("risk_scale")
        rows.append(
            {
                "date": d.normalize(),
                "trigger_type": trigger,
                "subject": "risk_scale",
                "value": value,
            }
        )
    out = pd.DataFrame(rows)
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out = out.dropna(subset=["date"]).copy()
    return out[["date", "trigger_type", "subject", "value"]]


def _auto_fill_live_inputs_from_baseline(live_dir: str, baseline_path: str, cycle_bars: int, stale_days: int) -> None:
    eq_fp = os.path.join(live_dir, "live_equity.csv")
    h_fp = os.path.join(live_dir, "live_holdings.csv")
    r_fp = os.path.join(live_dir, "live_risk_log.csv")

    base_raw = _maybe_read_csv(baseline_path)
    if base_raw.empty:
        return
    base_raw["date"] = pd.to_datetime(base_raw.get("date"), errors="coerce")
    base_raw = base_raw.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if base_raw.empty:
        return

    base_raw["spread"] = pd.to_numeric(base_raw.get("Top30_net"), errors="coerce") - pd.to_numeric(base_raw.get("Bottom30"), errors="coerce")
    base_ret = base_raw[["date", "spread"]].dropna(subset=["date"]).copy()
    base_last = pd.Timestamp(base_ret["date"].iloc[-1]).normalize()

    live_raw = _maybe_read_csv(eq_fp)
    live_ok = not live_raw.empty and "date" in live_raw.columns and (("spread" in live_raw.columns) or ("equity" in live_raw.columns))
    now_d = pd.Timestamp(datetime.now().date())

    should_fill = False
    if not live_ok:
        should_fill = True
        live_dates = pd.Series([], dtype="datetime64[ns]")
    else:
        live_dates = _normalize_dates(live_raw["date"]).dropna()
        last_live = pd.Timestamp(live_dates.max()).normalize() if not live_dates.empty else pd.NaT
        if pd.isna(last_live):
            should_fill = True
        else:
            age_days = int((now_d - last_live).days)
            should_fill = age_days >= int(stale_days) and base_last > last_live

    if should_fill:
        if not live_ok:
            keep_n = max(int(cycle_bars) * 6, 24)
            out_eq = base_ret.tail(keep_n).copy()
        else:
            last_live = pd.Timestamp(live_dates.max()).normalize() if not live_dates.empty else pd.NaT
            out_eq = live_raw.copy()
            out_eq["date"] = pd.to_datetime(out_eq["date"], errors="coerce")
            out_eq = out_eq.dropna(subset=["date"]).copy()
            out_eq["date_norm"] = out_eq["date"].dt.normalize()
            base_append = base_ret[base_ret["date"].dt.normalize() > last_live].copy() if pd.notna(last_live) else base_ret.copy()
            if not base_append.empty:
                base_append["date"] = pd.to_datetime(base_append["date"], errors="coerce")
                out_eq = pd.concat([out_eq, base_append], ignore_index=True)
            if "date_norm" in out_eq.columns:
                out_eq = out_eq.drop(columns=["date_norm"], errors="ignore")

        out_eq["date"] = pd.to_datetime(out_eq["date"], errors="coerce")
        out_eq = out_eq.dropna(subset=["date"]).copy()
        if "spread" in out_eq.columns:
            out_eq["spread"] = pd.to_numeric(out_eq["spread"], errors="coerce")
        else:
            out_eq["equity"] = pd.to_numeric(out_eq["equity"], errors="coerce")
            out_eq["spread"] = out_eq["equity"].pct_change().fillna(0.0)
            out_eq = out_eq.drop(columns=["equity"], errors="ignore")
        out_eq = out_eq.dropna(subset=["spread"]).copy()
        out_eq["date"] = out_eq["date"].dt.normalize()
        out_eq = out_eq.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
        out_eq[["date", "spread"]].to_csv(eq_fp, index=False, encoding="utf-8-sig")

    eq_post = _maybe_read_csv(eq_fp)
    if eq_post.empty or "date" not in eq_post.columns:
        return
    dt_set = set(_normalize_dates(eq_post["date"]).dropna().tolist())
    if not dt_set:
        return

    h_src = os.path.join(ROOT, "research", "baseline_v6_1", "output", "base_E_foundation_holdings_2010_2025.csv")
    r_src = os.path.join(ROOT, "research", "baseline_v6_1", "output", "base_E_foundation_risk_log_2010_2025.csv")

    h_live = pd.DataFrame()
    if os.path.exists(h_src):
        try:
            h = pd.read_csv(h_src)
            h["date"] = pd.to_datetime(h.get("date"), errors="coerce").dt.normalize()
            h = h.dropna(subset=["date"]).copy()
            h = h[h["date"].isin(dt_set)].copy()
            if not h.empty:
                if "weight" not in h.columns:
                    h["weight"] = 1.0 / h.groupby("date")["stock_symbol"].transform("count")
                h_live = h[["date", "stock_symbol", "weight"]].copy()
        except Exception:
            h_live = pd.DataFrame()
    if h_live.empty:
        base_for_dates = base_raw.copy()
        base_for_dates["date"] = pd.to_datetime(base_for_dates.get("date"), errors="coerce").dt.normalize()
        base_for_dates = base_for_dates[base_for_dates["date"].isin(dt_set)].copy()
        h_live = _build_holdings_from_baseline_ret(base_for_dates)
    if h_live.empty:
        h_live = pd.DataFrame([{"date": min(dt_set), "stock_symbol": "UNKNOWN", "weight": 1.0}])
    h_live.to_csv(h_fp, index=False, encoding="utf-8-sig")

    r_live = pd.DataFrame()
    if os.path.exists(r_src):
        try:
            r = pd.read_csv(r_src)
            r["date"] = pd.to_datetime(r.get("date"), errors="coerce").dt.normalize()
            r = r.dropna(subset=["date"]).copy()
            r = r[r["date"].isin(dt_set)].copy()
            need = ["date", "trigger_type", "subject", "value"]
            for c in need:
                if c not in r.columns:
                    r[c] = np.nan
            r_live = r[need].copy()
        except Exception:
            r_live = pd.DataFrame()
    if r_live.empty:
        base_for_dates = base_raw.copy()
        base_for_dates["date"] = pd.to_datetime(base_for_dates.get("date"), errors="coerce").dt.normalize()
        base_for_dates = base_for_dates[base_for_dates["date"].isin(dt_set)].copy()
        r_live = _build_risk_from_baseline_ret(base_for_dates)
    if r_live.empty:
        r_live = pd.DataFrame([{"date": min(dt_set), "trigger_type": "none", "subject": "risk_scale", "value": 1.0}])
    r_live.to_csv(r_fp, index=False, encoding="utf-8-sig")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live-dir", default=os.path.join(ROOT, "research", "baseline_v6_1", "output", "live"))
    ap.add_argument("--baseline", default=os.path.join(ROOT, "research", "baseline_v6_1", "output", "base_E_foundation_group_ret_2010_2025.csv"))
    ap.add_argument("--cycle-bars", type=int, default=12)
    ap.add_argument("--bootstrap-sample-days", type=int, default=0)
    ap.add_argument("--auto-fill-from-baseline", type=int, default=1)
    ap.add_argument("--stale-days", type=int, default=30)
    args = ap.parse_args()

    os.makedirs(args.live_dir, exist_ok=True)
    if int(args.bootstrap_sample_days) > 0:
        _bootstrap_live(args.live_dir, int(args.bootstrap_sample_days))
    if int(args.auto_fill_from_baseline) > 0:
        _auto_fill_live_inputs_from_baseline(args.live_dir, args.baseline, int(args.cycle_bars), int(args.stale_days))

    eq_fp = os.path.join(args.live_dir, "live_equity.csv")
    h_fp = os.path.join(args.live_dir, "live_holdings.csv")
    r_fp = os.path.join(args.live_dir, "live_risk_log.csv")
    trades_fp = os.path.join(args.live_dir, "live_trades.csv")
    decision_fp = os.path.join(args.live_dir, "gray_deployment_decision.csv")
    compare_fp = os.path.join(args.live_dir, "daily_nav_compare.csv")
    report_fp = os.path.join(args.live_dir, "daily_report.md")
    error_fp = os.path.join(args.live_dir, "daily_error_report.md")

    try:
        live = _load_live_equity(eq_fp)
        base = _load_baseline(args.baseline)
        hold, warns = _load_holdings(h_fp)
        risk = _load_risk(r_fp)
        trades = _load_trades(trades_fp)
        dec = _decide(live, base, args.cycle_bars)
        dec.to_csv(decision_fp, index=False, encoding="utf-8-sig")
        if base is None or base.empty:
            cmp = live.copy()
            cmp["baseline_equity"] = np.nan
        else:
            cmp = live.merge(base[["date", "equity"]].rename(columns={"equity": "baseline_equity"}), on="date", how="left")
        cmp["gap"] = cmp["equity"] / cmp["baseline_equity"] - 1.0
        cmp.to_csv(compare_fp, index=False, encoding="utf-8-sig")
        latest = cmp.iloc[-1]
        latest_d = pd.Timestamp(latest["date"])
        hold_n = int((hold["date"].dt.normalize() == latest_d.normalize()).sum())
        risk_n = int((risk["date"].dt.normalize() == latest_d.normalize()).sum())
        kpi = _kpi_from_trades(trades, latest_d)
        with open(report_fp, "w", encoding="utf-8") as f:
            f.write("# baseline_v6.1 日报\n\n")
            f.write(f"- 日期：{latest_d.strftime('%Y-%m-%d')}\n")
            f.write(f"- 决策动作：{dec['action'].iloc[0]}\n")
            f.write(f"- 触发原因：{dec['reasons'].iloc[0]}\n")
            f.write(f"- 实盘净值：{float(latest['equity']):.6f}\n")
            f.write(f"- 基线净值：{float(latest['baseline_equity']):.6f}\n")
            f.write(f"- 相对偏离：{float(latest['gap']):.2%}\n")
            f.write(f"- 当日持仓数：{hold_n}\n")
            f.write(f"- 当日风控触发数：{risk_n}\n")
            f.write(f"- 成交笔数：{kpi['trades_count']}\n")
            f.write(f"- 成交额：{kpi['turnover_notional']:.2f}\n")
            f.write(f"- 滑点均值(bps)：{kpi['avg_slippage_bps']:.2f}\n" if not math.isnan(kpi["avg_slippage_bps"]) else "- 滑点均值(bps)：nan\n")
            f.write(f"- 滑点加权均值(bps)：{kpi['vwap_slippage_bps']:.2f}\n" if not math.isnan(kpi["vwap_slippage_bps"]) else "- 滑点加权均值(bps)：nan\n")
            f.write(f"- 平均成交率：{kpi['avg_fill_rate']:.2%}\n" if not math.isnan(kpi["avg_fill_rate"]) else "- 平均成交率：nan\n")
            f.write(f"- 交易成本：{kpi['total_cost']:.2f}\n")
            if warns:
                f.write(f"- 警告：{'|'.join(warns)}\n")
        if os.path.exists(error_fp):
            os.remove(error_fp)
        print(decision_fp)
        print(compare_fp)
        print(report_fp)
    except Exception as e:
        with open(error_fp, "w", encoding="utf-8") as f:
            f.write("# baseline_v6.1 日频执行错误\n\n")
            f.write(f"- 错误：{str(e)}\n")
            if os.path.exists(decision_fp):
                f.write("- 动作：保留上次 gray_deployment_decision.csv\n")
            else:
                f.write("- 动作：无上次决策，需人工介入\n")
        print(error_fp)


if __name__ == "__main__":
    main()
