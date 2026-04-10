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


def _load_hs300(start_date: str, end_date: str) -> pd.DataFrame:
    try:
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
        if not x.empty:
            return x
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("baostock hs300 failed, using cached proxy: %s", e)
    # Fallback: load from cached CSI300 proxy equity curve
    proxy_path = os.path.join(ROOT, "research", "baseline_v6_1", "output", "csi300_proxy_2019_2025.csv")
    if os.path.exists(proxy_path):
        p = pd.read_csv(proxy_path)
        col = [c for c in p.columns if c != "date"][0]
        p["date"] = pd.to_datetime(p["date"], errors="coerce").dt.normalize()
        p = p.dropna(subset=["date"]).sort_values("date")
        p["close"] = pd.to_numeric(p[col], errors="coerce")
        p = p.dropna(subset=["close"])
        # Equity curve → synthetic close: set base = 1000
        p["close"] = p["close"] * 1000.0
        return p[["date", "close"]]
    return pd.DataFrame(columns=["date", "close"])


def _build_hs_indicators(start_date: str, end_date: str) -> pd.DataFrame:
    hs = _load_hs300(start_date, end_date)
    if hs.empty:
        return pd.DataFrame(columns=["date", "hs_ret10", "hs_ret20"])
    hs = hs.copy()
    hs["hs_ret10"] = hs["close"] / hs["close"].shift(10) - 1.0
    hs["hs_ret20"] = hs["close"] / hs["close"].shift(20) - 1.0
    return hs[["date", "hs_ret10", "hs_ret20"]]


def _industry_score(day: pd.DataFrame, hs_ret: float) -> pd.Series:
    g = day.groupby("industry_l2")["ret20d_stock"].mean()
    return g - (hs_ret if pd.notna(hs_ret) else 0.0)


def _pick_with_industry_cap(cand: pd.DataFrame, n_target: int, cap_ratio: float) -> pd.DataFrame:
    if cand.empty:
        return cand
    cap_n = max(1, int(np.floor(n_target * cap_ratio)))
    picked = []
    cnt = {}
    for _, r in cand.sort_values("factor_use", ascending=False).iterrows():
        ind = str(r["industry_l2"])
        if cnt.get(ind, 0) >= cap_n:
            continue
        picked.append(r)
        cnt[ind] = cnt.get(ind, 0) + 1
        if len(picked) >= n_target:
            break
    if not picked:
        return cand.head(0).copy()
    return pd.DataFrame(picked)


def _sim_ret(symbol: str, d: pd.Timestamp, px_map: dict, max_days: int, stop_loss: float = 0.08) -> float:
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


def _build_rebalance(panel: pd.DataFrame, px_map: dict, hs: pd.DataFrame, mode: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    x = panel.copy()
    x = x.merge(hs, on="date", how="left")
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
        look = 20 if mode in ["E3_1", "E3_2"] else 10
        hs_ret = float(day["hs_ret20"].iloc[0]) if look == 20 else float(day["hs_ret10"].iloc[0])
        score = _industry_score(day, hs_ret)
        top3_inds = score.sort_values(ascending=False).head(3).index.tolist()
        pool = day[day["industry_l2"].isin(top3_inds)].copy()
        top = _pick_with_industry_cap(pool, n_target=20, cap_ratio=0.30)
        if mode in ["E3_2", "E3_3"] and not top.empty:
            top = top.sort_values("factor_use", ascending=False).reset_index(drop=True)
            core = top.head(10).copy()
            sat = top.iloc[10:20].copy()
            med = float(top["ret20d_stock"].median()) if pd.notna(top["ret20d_stock"]).any() else 0.0
            core_bad = core[core["ret20d_stock"] <= med]
            if not core_bad.empty and not sat.empty:
                sat_good = sat.sort_values("ret20d_stock", ascending=False)
                k = min(len(core_bad), len(sat_good))
                core_idx = core_bad.index[:k]
                sat_idx = sat_good.index[:k]
                core.loc[core_idx, ["stock_symbol", "factor_use", "ret20d_stock", "fwd_ret_2w"]] = sat.loc[
                    sat_idx, ["stock_symbol", "factor_use", "ret20d_stock", "fwd_ret_2w"]
                ].to_numpy()
            core_ret = []
            for s in core["stock_symbol"].astype(str):
                core_ret.append(_sim_ret(s, pd.Timestamp(d), px_map, max_days=20, stop_loss=0.08))
            sat_ret = []
            for s in sat["stock_symbol"].astype(str):
                sat_ret.append(_sim_ret(s, pd.Timestamp(d), px_map, max_days=10, stop_loss=0.08))
            core["ret_use"] = pd.to_numeric(pd.Series(core_ret), errors="coerce").fillna(core["fwd_ret_2w"])
            sat["ret_use"] = pd.to_numeric(pd.Series(sat_ret), errors="coerce").fillna(sat["fwd_ret_2w"])
            top = pd.concat([core, sat], ignore_index=True)
            top_ret = float(top["ret_use"].mean())
        else:
            top_ret = float(top["fwd_ret_2w"].mean()) if not top.empty else np.nan
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
        if not top.empty:
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
    d = ret.copy().sort_values("date")
    spread = d["Top30_net"] - d["Bottom30"]
    curve = (1 + spread.fillna(0)).cumprod()
    dd = curve / curve.cummax() - 1.0
    mdd = float(dd.min()) if not dd.empty else np.nan
    out = {
        "calmar": float(spread.mean()) / abs(mdd) if pd.notna(mdd) and mdd != 0 else np.nan,
        "mdd": mdd,
        "excess": float(spread.mean()),
    }
    for rg in ["上涨", "震荡", "下跌"]:
        s = d[d["regime"] == rg]
        out[f"{rg}_top_bottom"] = float((s["Top30_net"] - s["Bottom30"]).mean()) if not s.empty else np.nan
    top_by_date = hold.groupby("date")["stock_symbol"].apply(lambda s: set(s.astype(str))).to_dict() if not hold.empty else {}
    dates = sorted(top_by_date.keys())
    sold_ret = []
    p = panel[["date", "stock_symbol", "fwd_ret_2w"]].copy()
    for i in range(1, len(dates)):
        prev_d, d0 = dates[i - 1], dates[i]
        sold = top_by_date[prev_d] - top_by_date[d0]
        for s in sold:
            r = p[(p["date"] == d0) & (p["stock_symbol"] == s)]
            if r.empty:
                continue
            rr = pd.to_numeric(r["fwd_ret_2w"], errors="coerce").iloc[0]
            if pd.notna(rr):
                sold_ret.append(float(rr))
    sr = pd.Series(sold_ret)
    out["sell_fly_rate"] = float((sr > 0.10).mean()) if not sr.empty else np.nan
    h = hold[["date", "stock_symbol", "industry_l2", "fwd_ret_2w"]].copy() if not hold.empty else pd.DataFrame(columns=["date", "stock_symbol", "industry_l2", "fwd_ret_2w"])
    u = panel[["date", "stock_symbol", "industry_l2", "fwd_ret_2w"]].copy()
    ar = []
    for d0, uh in u.groupby("date"):
        ph = h[h["date"] == d0]
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
        ar.append((alloc, sel, rp - rb))
    if ar:
        arr = np.array(ar, dtype=float)
        out["allocation"] = float(arr[:, 0].sum())
        out["selection"] = float(arr[:, 1].sum())
        out["attr_total"] = float(arr[:, 2].sum())
    else:
        out["allocation"] = np.nan
        out["selection"] = np.nan
        out["attr_total"] = np.nan
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
    hs = _build_hs_indicators("2019-01-01", "2025-12-31")
    exp_map = {
        "E3_1": "强势Top3行业+Top20个股+30%行业上限",
        "E3_2": "E3_1+分层持有+强弱切换",
        "E3_3": "E3_2+行业强势周期10日",
    }
    rows = []
    for k, desc in exp_map.items():
        ret, hold = _build_rebalance(panel, px_map, hs, mode=k)
        m = _metrics(ret, hold, panel)
        rows.append({"experiment": k, "desc": desc, **m})
        ret.to_csv(os.path.join(out_dir, f"{k}_group_ret.csv"), index=False, encoding="utf-8-sig")
    res = pd.DataFrame(rows)
    res.to_csv(os.path.join(out_dir, "e3_focus_summary_2019_2025.csv"), index=False, encoding="utf-8-sig")
    with open(os.path.join(rep_dir, "e3_focus_report.md"), "w", encoding="utf-8") as f:
        f.write("# E3聚焦实验报告（2019-2025）\n\n")
        f.write("- 目标：验证E3赛道优先策略的可持续改进路径。\n\n")
        for _, r in res.iterrows():
            f.write(
                f"- {r['experiment']}({r['desc']}): calmar={r['calmar']:.6f}, mdd={r['mdd']:.6f}, excess={r['excess']:.6f}, sell_fly={r['sell_fly_rate']:.2%}, allocation={r['allocation']:.6f}\n"
            )
    print(os.path.join(out_dir, "e3_focus_summary_2019_2025.csv"))
    print(os.path.join(rep_dir, "e3_focus_report.md"))


if __name__ == "__main__":
    main()
