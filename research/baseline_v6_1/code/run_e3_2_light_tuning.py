import os
import sys

import baostock as bs
import numpy as np
import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v4.code.run_baseline_v4_2_up_filter import _apply_up_exposure
from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5
from research.baseline_v6_1.code.run_baseline_v6_v61_suite import _apply_costs, _enrich_from_stock_data


def _load_hs(start_date: str, end_date: str) -> pd.DataFrame:
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
    x["hs_ret20"] = x["close"] / x["close"].shift(20) - 1.0
    return x[["date", "hs_ret20"]]


def _score(day: pd.DataFrame, hs_ret20: float) -> pd.Series:
    return day.groupby("industry_l2")["ret20d_stock"].mean() - (hs_ret20 if pd.notna(hs_ret20) else 0.0)


def _pick(cand: pd.DataFrame, n_target: int, cap_ratio: float, weak_cap: dict = None) -> pd.DataFrame:
    if cand.empty:
        return cand
    cap_n = max(1, int(np.floor(n_target * cap_ratio)))
    weak_cap = weak_cap or {}
    picked = []
    cnt = {}
    for _, r in cand.sort_values("factor_use", ascending=False).iterrows():
        ind = str(r["industry_l2"])
        lim = weak_cap.get(ind, cap_n)
        if cnt.get(ind, 0) >= lim:
            continue
        picked.append(r)
        cnt[ind] = cnt.get(ind, 0) + 1
        if len(picked) >= n_target:
            break
    return pd.DataFrame(picked) if picked else cand.head(0).copy()


def _sim(symbol: str, d: pd.Timestamp, px_map: dict, max_days: int, stop_loss: float) -> float:
    px = px_map.get(symbol)
    if px is None or px.empty:
        return np.nan
    seq = px[px["date"] >= d].head(max_days + 1)
    if len(seq) < 2:
        return np.nan
    entry = float(seq["close_sd"].iloc[0])
    if entry <= 0:
        return np.nan
    for v in seq["close_sd"].astype(float).iloc[1:]:
        rr = v / entry - 1.0
        if rr <= -abs(stop_loss):
            return rr
    return float(seq["close_sd"].iloc[-1]) / entry - 1.0


def _build(panel: pd.DataFrame, px_map: dict, hs: pd.DataFrame, mode: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    x = panel.copy().merge(hs, on="date", how="left")
    x["factor_use"] = np.where(x["regime"] == "上涨", -x["factor_z_raw"], x["factor_z_neu"])
    lo = x.groupby("date")["factor_use"].transform(lambda s: s.quantile(0.05))
    hi = x.groupby("date")["factor_use"].transform(lambda s: s.quantile(0.95))
    x = x[(x["factor_use"] >= lo) & (x["factor_use"] <= hi)].copy()
    dates = sorted(x["date"].unique().tolist())
    keep = []
    i = 0
    while i < len(dates):
        keep.append(dates[i])
        i += 10
    x = x[x["date"].isin(set(keep))].copy()
    rows = []
    holds = []
    for d, day in x.groupby("date"):
        if len(day) < 30:
            continue
        day = day.copy()
        day["rank"] = day["factor_use"].rank(pct=True, method="first")
        mid = day[(day["rank"] > 0.3) & (day["rank"] < 0.7)]
        bot = day[day["rank"] <= 0.3]
        sc = _score(day, float(day["hs_ret20"].iloc[0]))
        top3 = sc.sort_values(ascending=False).head(3).index.tolist()
        pool = day[day["industry_l2"].isin(top3)].copy()
        cap_ratio = 0.30
        stop_loss = 0.08
        use_layer = True
        core_n = 15
        sat_n = 5
        core_days = 20
        sat_days = 15
        switch_q = 0.30
        weak_cap = None
        exposure = 1.0
        if mode == "E3_2_4":
            use_layer = False
            core_n = 20
            sat_n = 0
            cap_ratio = 0.30
            stop_loss = 0.08
            core_days = 30
        if mode == "E3_2_5":
            cap_ratio = 0.25
            stop_loss = 0.06
        if mode == "E3_2_6":
            cap_ratio = 0.25
            stop_loss = 0.06
            core_n = 12
            sat_n = 8
            sat_days = 15
            switch_q = 0.20
        if mode in ["E3_2_1", "E3_2_3"]:
            cap_ratio = 0.20
            weak_inds = sc[sc < -0.05].index.tolist()
            weak_cap = {ind: max(1, int(round(20 * 0.05))) for ind in weak_inds}
            if pd.notna(day["hs_ret20"].iloc[0]) and float(day["hs_ret20"].iloc[0]) < -0.08:
                exposure = 0.70
        top = _pick(pool, n_target=20, cap_ratio=cap_ratio, weak_cap=weak_cap)
        if top.empty:
            continue
        if use_layer:
            top = top.sort_values("factor_use", ascending=False).reset_index(drop=True)
            core = top.head(core_n).copy()
            sat = top.iloc[core_n : core_n + sat_n].copy()
            q = float(core["ret20d_stock"].quantile(switch_q)) if not core.empty else 0.0
            core_bad = core[core["ret20d_stock"] <= q]
            if not core_bad.empty and not sat.empty:
                sat_good = sat.sort_values("ret20d_stock", ascending=False)
                k = min(len(core_bad), len(sat_good))
                cidx = core_bad.index[:k]
                sidx = sat_good.index[:k]
                core.loc[cidx, ["stock_symbol", "factor_use", "ret20d_stock", "fwd_ret_2w"]] = sat.loc[
                    sidx, ["stock_symbol", "factor_use", "ret20d_stock", "fwd_ret_2w"]
                ].to_numpy()
            core_ret = [_sim(s, pd.Timestamp(d), px_map, core_days, stop_loss) for s in core["stock_symbol"].astype(str)]
            core["ret_use"] = pd.to_numeric(pd.Series(core_ret), errors="coerce").fillna(core["fwd_ret_2w"])
            if sat_n > 0 and not sat.empty:
                sat_ret = [_sim(s, pd.Timestamp(d), px_map, sat_days, stop_loss) for s in sat["stock_symbol"].astype(str)]
                sat["ret_use"] = pd.to_numeric(pd.Series(sat_ret), errors="coerce").fillna(sat["fwd_ret_2w"])
                top = pd.concat([core, sat], ignore_index=True)
            else:
                top = core
            top_ret = float(top["ret_use"].mean())
        else:
            top = top.sort_values("factor_use", ascending=False).head(20).reset_index(drop=True).copy()
            rr = [_sim(s, pd.Timestamp(d), px_map, core_days, stop_loss) for s in top["stock_symbol"].astype(str)]
            top["ret_use"] = pd.to_numeric(pd.Series(rr), errors="coerce").fillna(top["fwd_ret_2w"])
            top_ret = float(top["ret_use"].mean())
        top_ret = top_ret * exposure if pd.notna(top_ret) else top_ret
        rows.append(
            {
                "date": d,
                "regime": str(day["regime"].iloc[0]),
                "Top30": top_ret,
                "Middle40": float(mid["fwd_ret_2w"].mean()) if not mid.empty else np.nan,
                "Bottom30": float(bot["fwd_ret_2w"].mean()) if not bot.empty else np.nan,
                "top_symbols": "|".join(top["stock_symbol"].astype(str).tolist()),
            }
        )
        t = top.copy()
        t["date"] = d
        holds.append(t)
    if rows:
        ret = pd.DataFrame(rows).sort_values("date").reset_index(drop=True)
        ret = _apply_up_exposure(ret, up_scale=0.5)
        ret = _apply_costs(ret, impact_cost=0.0005)
    else:
        ret = pd.DataFrame(columns=["date", "regime", "Top30", "Bottom30", "Middle40", "top_symbols", "one_way_turnover", "trade_cost_rate", "Top30_net"])
    hold = pd.concat(holds, ignore_index=True) if holds else pd.DataFrame()
    return ret, hold


def _metrics(ret: pd.DataFrame, hold: pd.DataFrame, panel: pd.DataFrame) -> dict:
    d = ret.sort_values("date").copy()
    spread = d["Top30_net"] - d["Bottom30"]
    curve = (1 + spread.fillna(0)).cumprod()
    dd = curve / curve.cummax() - 1.0
    mdd = float(dd.min()) if not dd.empty else np.nan
    out = {
        "calmar": float(spread.mean()) / abs(mdd) if pd.notna(mdd) and mdd != 0 else np.nan,
        "mdd": mdd,
        "excess": float(spread.mean()) if not spread.empty else np.nan,
        "hit_ratio": float((d["Top30_net"] > d["Bottom30"]).mean()) if not d.empty else np.nan,
    }
    for rg in ["上涨", "震荡", "下跌"]:
        s = d[d["regime"] == rg]
        out[f"{rg}_top_bottom"] = float((s["Top30_net"] - s["Bottom30"]).mean()) if not s.empty else np.nan
    top_by_date = hold.groupby("date")["stock_symbol"].apply(lambda s: set(s.astype(str))).to_dict() if not hold.empty else {}
    dates = sorted(top_by_date.keys())
    sold = []
    p = panel[["date", "stock_symbol", "fwd_ret_2w"]].copy()
    for i in range(1, len(dates)):
        prev_d, d0 = dates[i - 1], dates[i]
        for s in (top_by_date[prev_d] - top_by_date[d0]):
            r = p[(p["date"] == d0) & (p["stock_symbol"] == s)]
            if r.empty:
                continue
            rr = pd.to_numeric(r["fwd_ret_2w"], errors="coerce").iloc[0]
            if pd.notna(rr):
                sold.append(float(rr))
    sr = pd.Series(sold)
    out["sell_fly_rate"] = float((sr > 0.10).mean()) if not sr.empty else np.nan
    return out


def main():
    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    rep_dir = os.path.join(ROOT, "research", "baseline_v6_1", "report")
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(rep_dir, exist_ok=True)
    panel = _prepare_panel_v5()
    panel, px_map = _enrich_from_stock_data(panel)
    panel = panel.dropna(subset=["ret20d_stock", "fwd_ret_2w"]).copy()
    panel = panel[(panel["date"] >= pd.Timestamp("2019-01-01")) & (panel["date"] <= pd.Timestamp("2025-12-31"))].copy()
    hs = _load_hs("2019-01-01", "2025-12-31")
    exp_map = {
        "E3_2_4": "核心仓扩容",
        "E3_2_5": "轻量风控",
        "E3_2_6": "阈值适配",
    }
    rows = []
    for k, desc in exp_map.items():
        ret, hold = _build(panel, px_map, hs, mode=k)
        m = _metrics(ret, hold, panel)
        rows.append({"experiment": k, "desc": desc, **m})
        ret.to_csv(os.path.join(out_dir, f"{k}_group_ret.csv"), index=False, encoding="utf-8-sig")
    res = pd.DataFrame(rows)
    res.to_csv(os.path.join(out_dir, "e3_2_light_tuning_summary_2019_2025.csv"), index=False, encoding="utf-8-sig")
    with open(os.path.join(rep_dir, "e3_2_light_tuning_report.md"), "w", encoding="utf-8") as f:
        f.write("# E3-2 轻量微调报告（2019-2025）\n\n")
        for _, r in res.iterrows():
            f.write(
                f"- {r['experiment']}({r['desc']}): calmar={r['calmar']:.6f}, mdd={r['mdd']:.6f}, excess={r['excess']:.6f}, hit={r['hit_ratio']:.4f}, sell_fly={r['sell_fly_rate']:.2%}\n"
            )
    print(os.path.join(out_dir, "e3_2_light_tuning_summary_2019_2025.csv"))
    print(os.path.join(rep_dir, "e3_2_light_tuning_report.md"))


if __name__ == "__main__":
    main()
