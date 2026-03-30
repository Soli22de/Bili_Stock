import os
import sys
import numpy as np
import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5
from research.baseline_v6_1.code.run_baseline_v6_v61_suite import _enrich_from_stock_data


def _load_series(path: str, date_col: str = "date", close_cols: tuple[str, ...] = ("close", "收盘")) -> pd.DataFrame:
    x = pd.read_csv(path)
    if date_col not in x.columns and "日期" in x.columns:
        x = x.rename(columns={"日期": date_col})
    close_col = None
    for c in close_cols:
        if c in x.columns:
            close_col = c
            break
    if close_col is None:
        raise ValueError(path)
    if close_col != "close":
        x = x.rename(columns={close_col: "close"})
    x["date"] = pd.to_datetime(x[date_col], errors="coerce").dt.normalize()
    x["close"] = pd.to_numeric(x["close"], errors="coerce")
    x = x.dropna(subset=["date", "close"]).sort_values("date")
    return x[["date", "close"]]


def _load_market_features() -> pd.DataFrame:
    cache_dir = os.path.join(ROOT, "data", "cache")
    hs = _load_series(os.path.join(cache_dir, "SH000300.csv"))
    cn_path = os.path.join(cache_dir, "SZ159915_fresh.csv")
    if not os.path.exists(cn_path):
        cn_path = os.path.join(cache_dir, "SZ159915.csv")
    cn = pd.read_csv(cn_path)
    if "date" not in cn.columns and "日期" in cn.columns:
        cn = cn.rename(columns={"日期": "date"})
    if "close" not in cn.columns and "收盘" in cn.columns:
        cn = cn.rename(columns={"收盘": "close"})
    if "volume" not in cn.columns and "成交量" in cn.columns:
        cn = cn.rename(columns={"成交量": "volume"})
    cn["date"] = pd.to_datetime(cn["date"], errors="coerce").dt.normalize()
    cn["close"] = pd.to_numeric(cn["close"], errors="coerce")
    cn["volume"] = pd.to_numeric(cn["volume"], errors="coerce")
    cn = cn.dropna(subset=["date"]).sort_values("date")
    hs["hs300_ret1d"] = hs["close"].pct_change()
    hs["hs300_ret20"] = hs["close"] / hs["close"].shift(20) - 1.0
    hs["hs300_ret10"] = hs["close"] / hs["close"].shift(10) - 1.0
    hs["hs300_ret20_pct"] = hs["hs300_ret20"].rolling(250, min_periods=50).rank(pct=True)
    hs["panic_proxy_pct"] = hs["hs300_ret1d"].abs().rolling(250, min_periods=50).rank(pct=True)
    cn["chinext_turnover_pct"] = cn["volume"].rolling(250, min_periods=50).rank(pct=True)
    mkt = hs.merge(cn[["date", "chinext_turnover_pct"]], on="date", how="left")
    mkt["chinext_turnover_pct"] = mkt["chinext_turnover_pct"].ffill().bfill()
    return mkt


def _load_baseline() -> tuple[pd.DataFrame, pd.DataFrame]:
    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    ret = pd.read_csv(os.path.join(out_dir, "group_ret_baseline_v6_1_2019_2025.csv"))
    hold = pd.read_csv(os.path.join(out_dir, "holdings_baseline_v6_1_2019_2025.csv"))
    ret["date"] = pd.to_datetime(ret["date"], errors="coerce").dt.normalize()
    hold["date"] = pd.to_datetime(hold["date"], errors="coerce").dt.normalize()
    panel = _prepare_panel_v5()
    panel, _ = _enrich_from_stock_data(panel)
    panel["date"] = pd.to_datetime(panel["date"], errors="coerce").dt.normalize()
    panel["stock_symbol"] = panel["stock_symbol"].astype(str)
    panel = panel[(panel["date"] >= pd.Timestamp("2019-01-01")) & (panel["date"] <= pd.Timestamp("2025-12-31"))].copy()
    hold["stock_symbol"] = hold["stock_symbol"].astype(str)
    ext = panel[["date", "stock_symbol", "industry_l2", "fwd_ret_2w", "net_buy_cube_count", "count_lag"]].drop_duplicates(["date", "stock_symbol"])
    hold = hold.merge(ext, on=["date", "stock_symbol"], how="left")
    hold["industry_l2"] = hold["industry_l2"].fillna("其他")
    hold["ret_use"] = pd.to_numeric(hold["fwd_ret_2w"], errors="coerce")
    cnt = hold.groupby("date")["stock_symbol"].transform("count").replace(0, np.nan)
    hold["weight"] = 1.0 / cnt
    return ret.sort_values("date"), hold


def _industry_stop_flags(hold: pd.DataFrame, threshold: float = -0.10) -> pd.DataFrame:
    x = hold.dropna(subset=["ret_use", "weight"]).copy()
    x["contrib"] = x["ret_use"] * x["weight"]
    g = x.groupby(["date", "industry_l2"], as_index=False).agg(industry_ret=("ret_use", "mean"), industry_weight=("weight", "sum"), industry_contrib=("contrib", "sum"))
    g["trigger_industry_stop"] = g["industry_ret"] <= threshold
    return g


def _stock_stop_flags(hold: pd.DataFrame, threshold: float = -0.08) -> pd.DataFrame:
    x = hold.dropna(subset=["ret_use"]).copy()
    x["trigger_stock_stop"] = x["ret_use"] <= threshold
    return x[["date", "stock_symbol", "industry_l2", "ret_use", "weight", "trigger_stock_stop"]]


def _run_risk_engine(ret: pd.DataFrame, hold: pd.DataFrame, mkt: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    x = ret.merge(mkt[["date", "hs300_ret20", "hs300_ret10", "hs300_ret20_pct", "chinext_turnover_pct", "panic_proxy_pct"]], on="date", how="left")
    x = x.sort_values("date").reset_index(drop=True)
    ind = _industry_stop_flags(hold, threshold=-0.10)
    stk = _stock_stop_flags(hold, threshold=-0.08)
    ind_sum = ind.groupby("date", as_index=False)["trigger_industry_stop"].sum().rename(columns={"trigger_industry_stop": "industry_stop_cnt"})
    stk_sum = stk.groupby("date", as_index=False)["trigger_stock_stop"].sum().rename(columns={"trigger_stock_stop": "stock_stop_cnt"})
    conc = hold.groupby(["date", "industry_l2"], as_index=False)["weight"].sum()
    conc = conc.sort_values(["date", "weight"], ascending=[True, False]).groupby("date", as_index=False).first().rename(columns={"weight": "top_industry_weight", "industry_l2": "top_industry"})
    x = x.merge(ind_sum, on="date", how="left").merge(stk_sum, on="date", how="left").merge(conc, on="date", how="left")
    x["industry_stop_cnt"] = x["industry_stop_cnt"].fillna(0).astype(int)
    x["stock_stop_cnt"] = x["stock_stop_cnt"].fillna(0).astype(int)
    x["top_industry_weight"] = x["top_industry_weight"].fillna(0.0)
    equity = 1.0
    peak = 1.0
    overheat_streak = 0
    trigger_rows = []
    ret_rows = []
    for _, r in x.iterrows():
        risk_scale = 1.0
        overheat = bool(
            pd.notna(r["hs300_ret20_pct"])
            and pd.notna(r["chinext_turnover_pct"])
            and pd.notna(r["panic_proxy_pct"])
            and (r["hs300_ret20_pct"] >= 0.90)
            and (r["chinext_turnover_pct"] >= 0.85)
            and (r["panic_proxy_pct"] <= 0.70)
        )
        if overheat:
            overheat_streak += 1
            risk_scale = 0.70 if overheat_streak == 1 else 0.55
        else:
            overheat_streak = 0
        dd = equity / peak - 1.0
        portfolio_stop = dd <= -0.10
        if portfolio_stop:
            risk_scale = min(risk_scale, 0.45)
        if r["industry_stop_cnt"] > 0:
            risk_scale = min(risk_scale, 0.80)
        if r["stock_stop_cnt"] >= 3:
            risk_scale = min(risk_scale, 0.75)
        concentration_hit = bool(r["top_industry_weight"] >= 0.35)
        if concentration_hit:
            risk_scale = min(risk_scale, 0.85)
        hedge_trigger = bool(pd.notna(r["panic_proxy_pct"]) and pd.notna(r["hs300_ret20"]) and r["panic_proxy_pct"] >= 0.80 and r["hs300_ret20"] < 0)
        hedge_ratio = 0.35 if hedge_trigger else 0.0
        base_top = float(r["Top30"])
        base_bottom = float(r["Bottom30"])
        hs_ret10 = float(r["hs300_ret10"]) if pd.notna(r["hs300_ret10"]) else 0.0
        top_risk = base_top * risk_scale - hedge_ratio * hs_ret10
        spread = top_risk - base_bottom
        equity = equity * (1 + spread)
        peak = max(peak, equity)
        ret_rows.append(
            {
                "date": r["date"],
                "regime": r["regime"],
                "Bottom30": base_bottom,
                "Top30_base": base_top,
                "Top30_risk": top_risk,
                "risk_scale": risk_scale,
                "hedge_ratio": hedge_ratio,
                "spread_ret": spread,
                "equity": equity,
                "drawdown": equity / peak - 1.0,
                "overheat_trigger": overheat,
                "portfolio_stop_trigger": portfolio_stop,
                "industry_stop_cnt": int(r["industry_stop_cnt"]),
                "stock_stop_cnt": int(r["stock_stop_cnt"]),
                "concentration_trigger": concentration_hit,
                "top_industry": r["top_industry"],
                "top_industry_weight": r["top_industry_weight"],
            }
        )
        if overheat or portfolio_stop or hedge_trigger or concentration_hit or int(r["industry_stop_cnt"]) > 0 or int(r["stock_stop_cnt"]) > 0:
            trigger_rows.append(
                {
                    "date": r["date"],
                    "overheat_trigger": overheat,
                    "portfolio_stop_trigger": portfolio_stop,
                    "hedge_trigger": hedge_trigger,
                    "industry_stop_cnt": int(r["industry_stop_cnt"]),
                    "stock_stop_cnt": int(r["stock_stop_cnt"]),
                    "concentration_trigger": concentration_hit,
                    "risk_scale": risk_scale,
                    "hedge_ratio": hedge_ratio,
                }
            )
    ret_cols = [
        "date",
        "regime",
        "Bottom30",
        "Top30_base",
        "Top30_risk",
        "risk_scale",
        "hedge_ratio",
        "spread_ret",
        "equity",
        "drawdown",
        "overheat_trigger",
        "portfolio_stop_trigger",
        "industry_stop_cnt",
        "stock_stop_cnt",
        "concentration_trigger",
        "top_industry",
        "top_industry_weight",
    ]
    trig_cols = [
        "date",
        "overheat_trigger",
        "portfolio_stop_trigger",
        "hedge_trigger",
        "industry_stop_cnt",
        "stock_stop_cnt",
        "concentration_trigger",
        "risk_scale",
        "hedge_ratio",
    ]
    return pd.DataFrame(ret_rows, columns=ret_cols), pd.DataFrame(trigger_rows, columns=trig_cols), pd.DataFrame(
        [
            {"name": "overheat_hs300_ret20_pct", "value": 0.90},
            {"name": "overheat_chinext_turnover_pct", "value": 0.85},
            {"name": "overheat_panic_proxy_pct_max", "value": 0.70},
            {"name": "industry_stop_threshold", "value": -0.10},
            {"name": "stock_stop_threshold", "value": -0.08},
            {"name": "portfolio_stop_threshold", "value": -0.10},
            {"name": "concentration_limit", "value": 0.35},
            {"name": "max_hedge_ratio", "value": 0.35},
        ]
    )


def _metrics(ret: pd.DataFrame) -> dict:
    if ret is None or ret.empty or "spread_ret" not in ret.columns:
        return {"ann_ret": np.nan, "ann_vol": np.nan, "sortino": np.nan, "mdd": np.nan, "calmar": np.nan}
    x = ret.sort_values("date").copy()
    spread = x["spread_ret"]
    ann = 26.0
    avg = float(spread.mean()) if not spread.empty else np.nan
    vol = float(spread.std(ddof=0)) if not spread.empty else np.nan
    ann_ret = float((1 + avg) ** ann - 1.0) if pd.notna(avg) else np.nan
    ann_vol = float(vol * np.sqrt(ann)) if pd.notna(vol) else np.nan
    neg = spread[spread < 0]
    downside = float(np.sqrt((neg.pow(2).mean()))) if not neg.empty else 0.0
    ann_down = float(downside * np.sqrt(ann))
    sortino = ann_ret / ann_down if ann_down > 0 else np.nan
    curve = (1 + spread.fillna(0)).cumprod()
    dd = curve / curve.cummax() - 1.0
    mdd = float(dd.min()) if not dd.empty else np.nan
    calmar = ann_ret / abs(mdd) if pd.notna(mdd) and mdd != 0 else np.nan
    return {"ann_ret": ann_ret, "ann_vol": ann_vol, "sortino": sortino, "mdd": mdd, "calmar": calmar}


def main():
    output_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    report_dir = os.path.join(ROOT, "research", "baseline_v6_1", "report")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(report_dir, exist_ok=True)
    ret, hold = _load_baseline()
    mkt = _load_market_features()
    risk_ret, trigger_log, cfg = _run_risk_engine(ret, hold, mkt)
    risk_ret.to_csv(os.path.join(output_dir, "group_ret_phase_b_risk_managed_2019_2025.csv"), index=False, encoding="utf-8-sig")
    trigger_log.to_csv(os.path.join(report_dir, "risk_trigger_log_2019_2025.csv"), index=False, encoding="utf-8-sig")
    cfg.to_csv(os.path.join(report_dir, "risk_control_config_2019_2025.csv"), index=False, encoding="utf-8-sig")
    m = _metrics(risk_ret)
    with open(os.path.join(report_dir, "phase_b_risk_control_report.md"), "w", encoding="utf-8") as f:
        f.write("# 阶段B 风控主模块报告\n\n")
        f.write("- 覆盖：市场过热刹车、组合止损、行业止损、个股止损、集中度约束、对冲触发。\n")
        f.write(f"- 年化={m['ann_ret']:.2%}, 波动={m['ann_vol']:.2%}, 索提诺={m['sortino']:.3f}, 卡玛={m['calmar']:.3f}, 回撤={m['mdd']:.2%}\n")
        f.write(f"- 风控触发次数={len(trigger_log)}，过热触发={int(trigger_log['overheat_trigger'].sum()) if not trigger_log.empty else 0}，组合止损触发={int(trigger_log['portfolio_stop_trigger'].sum()) if not trigger_log.empty else 0}\n")
    print(os.path.join(output_dir, "group_ret_phase_b_risk_managed_2019_2025.csv"))
    print(os.path.join(report_dir, "risk_trigger_log_2019_2025.csv"))
    print(os.path.join(report_dir, "phase_b_risk_control_report.md"))


if __name__ == "__main__":
    main()
