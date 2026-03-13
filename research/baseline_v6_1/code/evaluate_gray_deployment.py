import argparse
import os

import numpy as np
import pandas as pd


def _load_series(path: str) -> pd.DataFrame:
    x = pd.read_csv(path)
    x["date"] = pd.to_datetime(x["date"], errors="coerce")
    x = x.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    if {"Top30_net", "Bottom30"}.issubset(x.columns):
        x["spread"] = pd.to_numeric(x["Top30_net"], errors="coerce") - pd.to_numeric(x["Bottom30"], errors="coerce")
    elif "spread" in x.columns:
        x["spread"] = pd.to_numeric(x["spread"], errors="coerce")
    else:
        raise ValueError(f"unsupported input: {path}")
    x["equity"] = (1 + x["spread"].fillna(0)).cumprod()
    x["dd"] = x["equity"] / x["equity"].cummax() - 1.0
    return x[["date", "spread", "equity", "dd"]]


def _cycle_slice(x: pd.DataFrame, cycle: int) -> pd.DataFrame:
    if x.empty:
        return x
    n = max(int(cycle), 1)
    return x.tail(n).copy()


def _mdd(x: pd.DataFrame) -> float:
    return float(pd.to_numeric(x["dd"], errors="coerce").min()) if not x.empty else np.nan


def _sortino(x: pd.DataFrame) -> float:
    s = pd.to_numeric(x["spread"], errors="coerce").dropna()
    if s.empty:
        return np.nan
    ann_factor = 26.0
    avg = float(s.mean())
    ann_ret = float((1.0 + avg) ** ann_factor - 1.0)
    neg = s[s < 0]
    downside = float(np.sqrt((neg.pow(2).mean()))) if not neg.empty else 0.0
    ann_down = float(downside * np.sqrt(ann_factor))
    return ann_ret / ann_down if ann_down > 0 else np.nan


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", required=True)
    ap.add_argument("--baseline", required=True)
    ap.add_argument("--cycle-bars", type=int, default=12)
    ap.add_argument("--risk", default="")
    ap.add_argument("--output", default="")
    args = ap.parse_args()

    live = _load_series(args.live)
    base = _load_series(args.baseline)
    m = live.merge(base[["date", "equity"]].rename(columns={"equity": "base_equity"}), on="date", how="inner")
    if m.empty:
        raise RuntimeError("no overlapped dates between live and baseline")
    m["rel_gap"] = m["equity"] / m["base_equity"] - 1.0
    cyc = _cycle_slice(m, args.cycle_bars)
    prev = _cycle_slice(m.iloc[:-args.cycle_bars] if len(m) > args.cycle_bars else m.iloc[0:0], args.cycle_bars)
    gap_now = float(cyc["rel_gap"].iloc[-1]) if not cyc.empty else np.nan
    gap_prev = float(prev["rel_gap"].iloc[-1]) if not prev.empty else np.nan
    cycle_mdd = _mdd(cyc)
    oos_sortino = _sortino(m)
    risk_flags = {"pause": False, "reduce": False, "reasons": []}
    if args.risk:
        try:
            r = pd.read_csv(args.risk)
            r["date"] = pd.to_datetime(r["date"], errors="coerce").dt.normalize()
            recent_dates = set(pd.to_datetime(cyc["date"], errors="coerce").dropna().dt.normalize().tolist())
            rx = r[r["date"].isin(recent_dates)].copy()
            if not rx.empty:
                trig = rx["trigger_type"].astype(str).str.lower().fillna("")
                severe = {"portfolio_stop", "drawdown_brake", "risk_pause", "trading_halt"}
                moderate = {"overheat_brake", "stock_stop", "industry_stop", "concentration_limit"}
                if trig.isin(list(severe)).any():
                    risk_flags["pause"] = True
                    risk_flags["reasons"].append("risk_log_pause_triggered")
                if int(trig.isin(list(moderate)).sum()) >= 3:
                    risk_flags["reduce"] = True
                    risk_flags["reasons"].append("risk_log_reduce_triggered")
                rs = rx[rx["subject"].astype(str) == "risk_scale"]
                if not rs.empty:
                    vals = pd.to_numeric(rs["value"], errors="coerce").dropna()
                    if not vals.empty:
                        vmin = float(vals.min())
                        vavg = float(vals.mean())
                        if vmin < 0.4:
                            risk_flags["pause"] = True
                            risk_flags["reasons"].append("risk_scale_below_0_4")
                        elif vavg < 0.6:
                            risk_flags["reduce"] = True
                            risk_flags["reasons"].append("risk_scale_avg_below_0_6")
        except Exception:
            pass
    sig_reduce_gap = pd.notna(gap_now) and pd.notna(gap_prev) and gap_now < -0.05 and gap_prev < -0.05
    sig_pause_mdd = pd.notna(cycle_mdd) and cycle_mdd < -0.08
    sig_upgrade = pd.notna(gap_now) and pd.notna(gap_prev) and gap_now > 0 and gap_prev > 0 and pd.notna(oos_sortino) and oos_sortino > 0
    sig_oos_neg = pd.notna(oos_sortino) and oos_sortino < 0
    reasons = []
    if sig_pause_mdd:
        reasons.append("single_cycle_mdd_over_8pct")
    if sig_reduce_gap:
        reasons.append("relative_nav_below_baseline_5pct_for_two_cycles")
    if sig_upgrade:
        reasons.append("outperform_baseline_two_cycles_and_positive_sortino")
    if sig_oos_neg:
        reasons.append("oos_sortino_below_zero")
    if risk_flags.get("reasons"):
        reasons.extend(risk_flags.get("reasons", []))
    if risk_flags.get("pause") or sig_pause_mdd:
        action = "pause_revalidate"
    elif risk_flags.get("reduce") or sig_reduce_gap:
        action = "reduce_to_30"
    elif sig_upgrade:
        action = "upgrade_to_70"
    else:
        action = "hold_50"
    reasons = list(dict.fromkeys([str(x) for x in reasons if str(x).strip()]))

    out = pd.DataFrame(
        [
            {
                "latest_date": m["date"].iloc[-1].strftime("%Y-%m-%d"),
                "action": action,
                "gap_now": gap_now,
                "gap_prev": gap_prev,
                "cycle_mdd": cycle_mdd,
                "oos_sortino": oos_sortino,
                "reasons": "|".join(reasons),
            }
        ]
    )
    out_path = args.output or os.path.join(os.path.dirname(args.live), "gray_deployment_decision.csv")
    out.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(out_path)


if __name__ == "__main__":
    main()
