import os
import sys
from typing import Dict, List, Tuple

import baostock as bs
import numpy as np
import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v4.code.run_baseline_v4_2_up_filter import _apply_up_exposure
from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5
from research.baseline_v6_1.code.run_baseline_v6_v61_suite import _apply_costs, _enrich_from_stock_data


def _load_hs300_ret20(start_date: str, end_date: str) -> pd.DataFrame:
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
    x = pd.DataFrame(rows, columns=["date", "close"])
    x["date"] = pd.to_datetime(x["date"], errors="coerce").dt.normalize()
    x["close"] = pd.to_numeric(x["close"], errors="coerce")
    x = x.dropna(subset=["date", "close"]).sort_values("date")
    x["hs300_ret20"] = x["close"] / x["close"].shift(20) - 1.0
    return x[["date", "hs300_ret20"]]


def _normalize_with_bounds(w: Dict[str, float], w_min: float, w_max: float) -> Dict[str, float]:
    if not w:
        return {}
    s = pd.Series(w, dtype=float).fillna(0.0)
    s = s.clip(lower=w_min, upper=w_max)
    if s.sum() <= 0:
        s[:] = 1.0 / len(s)
        return s.to_dict()
    s = s / s.sum()
    for _ in range(5):
        s = s.clip(lower=w_min, upper=w_max)
        s = s / s.sum()
    return s.to_dict()


def _industry_scores(day: pd.DataFrame, hs_ret20: float) -> pd.Series:
    g = day.groupby("industry_l2")["ret20d_stock"].mean()
    return g - (hs_ret20 if pd.notna(hs_ret20) else 0.0)


def _industry_mkt_share(day: pd.DataFrame) -> pd.Series:
    amt = day.groupby("industry_l2")["amount"].sum().fillna(0.0)
    if amt.sum() <= 0:
        return pd.Series(1.0 / len(amt), index=amt.index)
    return amt / amt.sum()


def _build_budget(day: pd.DataFrame, hs_ret20: float, mode: str, risk_scale: float = 1.0) -> Dict[str, float]:
    score = _industry_scores(day, hs_ret20)
    share = _industry_mkt_share(day)
    inds = share.index.tolist()
    if mode == "scheme1":
        q30 = score.quantile(0.3)
        q70 = score.quantile(0.7)
        m = {}
        for ind in inds:
            v = score.get(ind, 0.0)
            if v >= q70:
                mul = 1.5
            elif v <= q30:
                mul = 0.5
            else:
                mul = 1.0
            m[ind] = float(share.get(ind, 0.0) * mul)
        return _normalize_with_bounds(m, 0.01, 0.25)
    if mode == "scheme2":
        m = {ind: float(share.get(ind, 0.0)) for ind in inds}
        return _normalize_with_bounds(m, 0.0, 0.25)
    if mode == "scheme3":
        top5 = score.sort_values(ascending=False).head(5).index.tolist()
        m = {ind: (1.0 if ind in top5 else 0.0) for ind in inds}
        return _normalize_with_bounds(m, 0.0, 0.25)
    if mode == "up_dynamic":
        if str(day["regime"].iloc[0]) != "上涨":
            m = {ind: float(share.get(ind, 0.0)) for ind in inds}
            return _normalize_with_bounds(m, 0.0, 0.20)
        top3 = score.sort_values(ascending=False).head(3).index.tolist()
        m = {}
        for ind in inds:
            if ind in top3:
                mul = 2.0 * risk_scale
            else:
                mul = 0.5
            m[ind] = float(share.get(ind, 0.0) * mul)
        return _normalize_with_bounds(m, 0.01, 0.30)
    raise ValueError(mode)


def _pick_top_with_budget(day: pd.DataFrame, budget: Dict[str, float], n_target: int = 30, only_inds: List[str] = None) -> pd.DataFrame:
    x = day.copy()
    if only_inds:
        x = x[x["industry_l2"].isin(only_inds)].copy()
    x = x.sort_values("factor_use", ascending=False)
    if x.empty:
        return x
    n_ind = {k: int(round(v * n_target)) for k, v in budget.items()}
    picked = []
    for ind, n in sorted(n_ind.items(), key=lambda t: t[1], reverse=True):
        if n <= 0:
            continue
        sub = x[x["industry_l2"] == ind].head(n)
        if not sub.empty:
            picked.append(sub)
    out = pd.concat(picked, ignore_index=True) if picked else x.iloc[0:0].copy()
    if len(out) < n_target:
        left = x[~x["stock_symbol"].isin(out["stock_symbol"])].head(n_target - len(out))
        out = pd.concat([out, left], ignore_index=True)
    return out.head(n_target).copy()


def _sim_path_ret(symbol: str, entry_date: pd.Timestamp, px_map: dict, max_days: int, stop_loss: float = None) -> float:
    px = px_map.get(symbol)
    if px is None or px.empty:
        return np.nan
    seq = px[px["date"] >= entry_date].head(max_days + 1).copy()
    if len(seq) < 2:
        return np.nan
    entry = float(seq["close_sd"].iloc[0])
    if entry <= 0:
        return np.nan
    for v in seq["close_sd"].astype(float).values[1:]:
        rr = v / entry - 1.0
        if stop_loss is not None and rr <= -abs(stop_loss):
            return rr
    return float(seq["close_sd"].iloc[-1]) / entry - 1.0


def _run_experiment(panel: pd.DataFrame, px_map: dict, hs: pd.DataFrame, mode: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = panel.dropna(subset=["date", "stock_symbol", "factor_z_raw", "factor_z_neu", "fwd_ret_2w", "ret20d_stock"]).copy()
    df = df.merge(hs, on="date", how="left")
    df["factor_use"] = np.where(df["regime"] == "上涨", -df["factor_z_raw"], df["factor_z_neu"])
    lo = df.groupby("date")["factor_use"].transform(lambda s: s.quantile(0.05))
    hi = df.groupby("date")["factor_use"].transform(lambda s: s.quantile(0.95))
    df = df[(df["factor_use"] >= lo) & (df["factor_use"] <= hi)].copy()
    dates = sorted(df["date"].unique().tolist())
    keep = []
    i = 0
    while i < len(dates):
        keep.append(dates[i])
        i += 10
    df = df[df["date"].isin(set(keep))].copy()
    equity = 1.0
    peak = 1.0
    risk_scale = 1.0
    rows = []
    holds = []
    for d, day in df.groupby("date"):
        if len(day) < 20:
            continue
        day = day.copy()
        day["rank"] = day["factor_use"].rank(pct=True, method="first")
        mid = day[(day["rank"] > 0.3) & (day["rank"] < 0.7)].copy()
        if mode == "up_dynamic":
            dd = equity / peak - 1.0
            if dd <= -0.08:
                risk_scale = 0.5
            elif dd <= -0.05:
                risk_scale = 0.75
            elif equity >= peak:
                risk_scale = 1.0
        b = _build_budget(day, float(day["hs300_ret20"].iloc[0]) if "hs300_ret20" in day else np.nan, mode=mode if mode in ["scheme1", "scheme2", "scheme3", "up_dynamic"] else "scheme1", risk_scale=risk_scale)
        if mode == "scheme3":
            top_inds = sorted(b, key=lambda k: b[k], reverse=True)[:5]
            top = _pick_top_with_budget(day, b, n_target=30, only_inds=top_inds)
        else:
            top = _pick_top_with_budget(day, b, n_target=30)
        bot = day.nsmallest(max(1, int(round(len(day) * 0.3))), "factor_use")
        if mode == "layer_switch":
            top = top.sort_values("factor_use", ascending=False).head(30).copy()
            core = top.head(10).copy()
            sat = top.iloc[10:30].copy()
            core_ret = []
            for s in core["stock_symbol"].astype(str):
                core_ret.append(_sim_path_ret(s, pd.Timestamp(d), px_map, max_days=20, stop_loss=0.08))
            sat_ret = []
            for s in sat["stock_symbol"].astype(str):
                sat_ret.append(_sim_path_ret(s, pd.Timestamp(d), px_map, max_days=10, stop_loss=0.08))
            core["fwd_ret_2w_use"] = pd.to_numeric(pd.Series(core_ret), errors="coerce").fillna(core["fwd_ret_2w"])
            sat["fwd_ret_2w_use"] = pd.to_numeric(pd.Series(sat_ret), errors="coerce").fillna(sat["fwd_ret_2w"])
            top = pd.concat([core, sat], ignore_index=True)
            top_ret = float(top["fwd_ret_2w_use"].mean())
        else:
            top_ret = float(top["fwd_ret_2w"].mean()) if not top.empty else np.nan
        row = {
            "date": d,
            "regime": str(day["regime"].iloc[0]),
            "Top30": top_ret,
            "Bottom30": float(bot["fwd_ret_2w"].mean()) if not bot.empty else np.nan,
            "Middle40": float(mid["fwd_ret_2w"].mean()) if not mid.empty else np.nan,
            "top_symbols": "|".join(top["stock_symbol"].astype(str).tolist()),
        }
        rows.append(row)
        if not top.empty:
            top2 = top.copy()
            top2["date"] = d
            holds.append(top2)
        spread = (row["Top30"] - row["Bottom30"]) if pd.notna(row["Top30"]) and pd.notna(row["Bottom30"]) else 0.0
        equity *= (1 + spread)
        peak = max(peak, equity)
    if rows:
        ret = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
        ret = _apply_up_exposure(ret, up_scale=0.5)
        ret = _apply_costs(ret, impact_cost=0.0005)
    else:
        ret = pd.DataFrame(columns=["date", "regime", "Top30", "Bottom30", "Middle40", "top_symbols", "one_way_turnover", "trade_cost_rate", "Top30_net"])
    hold = pd.concat(holds, ignore_index=True) if holds else pd.DataFrame()
    return ret, hold


def _calc_metrics(ret: pd.DataFrame, hold: pd.DataFrame, panel: pd.DataFrame) -> dict:
    x = ret.sort_values("date").copy()
    spread = x["Top30_net"] - x["Bottom30"]
    curve = (1 + spread.fillna(0)).cumprod()
    dd = curve / curve.cummax() - 1.0
    out = {
        "calmar": float(spread.mean()) / abs(float(dd.min())) if len(dd) and float(dd.min()) != 0 else np.nan,
        "mdd": float(dd.min()) if len(dd) else np.nan,
        "excess": float(spread.mean()) if len(spread) else np.nan,
        "hit_ratio": float((x["Top30_net"] > x["Bottom30"]).mean()) if len(x) else np.nan,
    }
    for rg in ["上涨", "震荡", "下跌"]:
        s = x[x["regime"] == rg]
        out[f"{rg}_top_bottom"] = float((s["Top30_net"] - s["Bottom30"]).mean()) if not s.empty else np.nan
    top_by_date = hold.groupby("date")["stock_symbol"].apply(lambda s: set(s.astype(str))).to_dict() if not hold.empty else {}
    dates = sorted(top_by_date.keys())
    sf_rows = []
    panel_r = panel[["date", "stock_symbol", "fwd_ret_2w"]].copy()
    for i in range(1, len(dates)):
        prev_d, d = dates[i - 1], dates[i]
        sold = top_by_date[prev_d] - top_by_date[d]
        for s in sold:
            r = panel_r[(panel_r["date"] == d) & (panel_r["stock_symbol"] == s)]
            if r.empty:
                continue
            rr = float(r["fwd_ret_2w"].iloc[0]) if pd.notna(r["fwd_ret_2w"].iloc[0]) else np.nan
            sf_rows.append(rr)
    sf = pd.Series(sf_rows)
    out["sell_fly_rate"] = float((sf > 0.10).mean()) if not sf.empty else np.nan
    h = hold[["date", "stock_symbol", "industry_l2", "fwd_ret_2w"]].copy() if not hold.empty else pd.DataFrame(columns=["date", "stock_symbol", "industry_l2", "fwd_ret_2w"])
    u = panel[["date", "stock_symbol", "industry_l2", "fwd_ret_2w"]].copy()
    rows = []
    for d, uh in u.groupby("date"):
        ph = h[h["date"] == d]
        uh = uh.dropna(subset=["fwd_ret_2w"])
        ph = ph.dropna(subset=["fwd_ret_2w"])
        if uh.empty or ph.empty:
            continue
        rb = float(uh["fwd_ret_2w"].mean())
        rp = float(ph["fwd_ret_2w"].mean())
        alloc = 0.0
        sel = 0.0
        inds = sorted(set(uh["industry_l2"].astype(str)) | set(ph["industry_l2"].astype(str)))
        for ind in inds:
            u_i = uh[uh["industry_l2"].astype(str) == ind]
            p_i = ph[ph["industry_l2"].astype(str) == ind]
            if u_i.empty:
                continue
            b_w = len(u_i) / max(len(uh), 1)
            p_w = len(p_i) / max(len(ph), 1)
            r_i = float(u_i["fwd_ret_2w"].mean())
            p_r = float(p_i["fwd_ret_2w"].mean()) if not p_i.empty else 0.0
            alloc += (p_w - b_w) * r_i
            sel += p_w * (p_r - r_i)
        rows.append({"allocation": alloc, "selection": sel, "total": rp - rb})
    ad = pd.DataFrame(rows)
    out["allocation"] = float(ad["allocation"].sum()) if not ad.empty else np.nan
    out["selection"] = float(ad["selection"].sum()) if not ad.empty else np.nan
    out["total_excess_attr"] = float(ad["total"].sum()) if not ad.empty else np.nan
    return out


def main():
    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    rep_dir = os.path.join(ROOT, "research", "baseline_v6_1", "report")
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)
    panel = _prepare_panel_v5()
    panel, px_map = _enrich_from_stock_data(panel)
    panel = panel[(panel["date"] >= pd.Timestamp("2019-01-01")) & (panel["date"] <= pd.Timestamp("2025-12-31"))].copy()
    hs = _load_hs300_ret20("2019-01-01", "2025-12-31")
    exps = {
        "E1_scheme1_industry_momentum": "scheme1",
        "E2_scheme2_mkt_neutral": "scheme2",
        "E3_scheme3_top5_industry": "scheme3",
        "E4_up_dynamic_exposure": "up_dynamic",
        "E5_layer_switch": "layer_switch",
    }
    rows = []
    for name, mode in exps.items():
        ret, hold = _run_experiment(panel, px_map, hs, mode=mode)
        m = _calc_metrics(ret, hold, panel)
        rows.append({"experiment": name, **m})
        ret.to_csv(os.path.join(out_dir, f"{name}_group_ret.csv"), index=False, encoding="utf-8-sig")
    res = pd.DataFrame(rows)
    res.to_csv(os.path.join(out_dir, "minimal_experiment_set_summary_2019_2025.csv"), index=False, encoding="utf-8-sig")
    with open(os.path.join(rep_dir, "minimal_experiment_set_report.md"), "w", encoding="utf-8") as f:
        f.write("# 最小实验集回测报告（2019-2025）\n\n")
        f.write("- 实验集：E1/E2/E3/E4/E5\n")
        f.write("- 指标：Calmar、MDD、超额、三市top-bottom、卖飞率、行业归因\n\n")
        for _, r in res.sort_values("calmar", ascending=False).iterrows():
            f.write(
                f"- {r['experiment']}: calmar={r['calmar']:.6f}, mdd={r['mdd']:.6f}, excess={r['excess']:.6f}, allocation={r['allocation']:.6f}, sell_fly={r['sell_fly_rate']:.2%}\n"
            )
        best = res.sort_values("calmar", ascending=False).iloc[0]
        f.write("\n")
        f.write(f"- 最优实验：{best['experiment']}\n")
    print(os.path.join(out_dir, "minimal_experiment_set_summary_2019_2025.csv"))
    print(os.path.join(rep_dir, "minimal_experiment_set_report.md"))


if __name__ == "__main__":
    main()
