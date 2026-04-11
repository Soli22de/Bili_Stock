import itertools
import os
import sys
from collections import defaultdict

import numpy as np
import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v4.code.run_baseline_v4_2_up_filter import _apply_up_exposure, _select_top_with_industry_cap
from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5


def _enrich_from_stock_data(panel: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    x = panel.copy()
    symbols = set(x["stock_symbol"].astype(str).str.upper().unique().tolist())
    rows = []
    px_map = {}
    data_dir = os.path.join(ROOT, "data", "stock_data")
    for s in symbols:
        fp = os.path.join(data_dir, f"{s}.csv")
        if not os.path.exists(fp):
            continue
        try:
            d = pd.read_csv(fp, usecols=["日期", "收盘", "成交额"])
        except Exception:
            continue
        d.rename(columns={"日期": "date", "收盘": "close_sd", "成交额": "amount_sd"}, inplace=True)
        d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.normalize()
        d["close_sd"] = pd.to_numeric(d["close_sd"], errors="coerce")
        d["amount_sd"] = pd.to_numeric(d["amount_sd"], errors="coerce")
        d = d.dropna(subset=["date"]).sort_values("date")
        d["stock_symbol"] = s
        d["ret20d_stock_sd"] = d["close_sd"] / d["close_sd"].shift(20) - 1.0
        d["fwd_ret_2w_sd"] = d["close_sd"].shift(-10) / d["close_sd"] - 1.0
        # 量价背离 (volume-price divergence): positive when price falls while volume rises (accumulation)
        d["vol_price_div5d"] = -(d["close_sd"].rolling(5, min_periods=3).corr(d["amount_sd"]))
        # 日内反转 (intraday reversal): col index 2 = open price
        # Documented IC -6 to -8%, ICIR -3.6, win rate 85% in A-shares at 10-20d hold (民生金工/中信建投 2025)
        open_col = d.columns[2] if len(d.columns) > 2 else None
        if open_col is not None:
            open_sd = pd.to_numeric(d.iloc[:, 2], errors="coerce")
            ret_intra = (d["close_sd"] / open_sd.replace(0, np.nan) - 1.0)
            d["ret_intra5d"] = ret_intra.rolling(5, min_periods=3).sum()
        else:
            d["ret_intra5d"] = np.nan
        # HV ratio: 20-day vs 60-day historical vol. < 1.0 = volatility contracting (calm entry, 选股策略 gate_hv)
        daily_ret = d["close_sd"].pct_change()
        hv20 = daily_ret.rolling(20, min_periods=10).std()
        hv60 = daily_ret.rolling(60, min_periods=20).std()
        d["hv20_hv60_ratio"] = hv20 / hv60.replace(0, np.nan)
        rows.append(d[["date", "stock_symbol", "close_sd", "amount_sd", "ret20d_stock_sd", "fwd_ret_2w_sd",
                        "vol_price_div5d", "ret_intra5d", "hv20_hv60_ratio"]])
        px_map[s] = d[["date", "close_sd"]].dropna().reset_index(drop=True)
    if rows:
        p = pd.concat(rows, ignore_index=True)
        x["date"] = pd.to_datetime(x["date"], errors="coerce").dt.normalize()
        x["stock_symbol"] = x["stock_symbol"].astype(str).str.upper()
        x = x.merge(p, on=["date", "stock_symbol"], how="left")
        x["close"] = x["close"].fillna(x["close_sd"])
        x["amount"] = x["amount"].fillna(x["amount_sd"])
        x["ret20d_stock"] = x["ret20d_stock"].fillna(x["ret20d_stock_sd"])
        x["fwd_ret_2w"] = x["fwd_ret_2w"].fillna(x["fwd_ret_2w_sd"])
        # vol_price_div5d, ret_intra5d, hv20_hv60_ratio arrive directly from merge
        x = x.drop(columns=["close_sd", "amount_sd", "ret20d_stock_sd", "fwd_ret_2w_sd"], errors="ignore")
    return x, px_map


def _srf_score(day: pd.DataFrame) -> pd.Series:
    """
    SmartResonanceFactor: composite ranking score for Top-K selection.
    Adapted from 选股策略 4-factor model for Xueqiu smart-money data.

    Components (all z-scored cross-sectionally within the day):
      40%  factor_z_neu        — smart-money consensus signal (analogous to main_net_inflow)
      30%  ret20d_stock        — 20-day price momentum (technical strength)
      20%  amount              — trading volume proxy (volume confirmation)
      10%  net_buy_cube_count  — same-day buying pulse (analogous to DDX acceleration)
    """
    def _z(s: pd.Series) -> pd.Series:
        std = s.std()
        return (s - s.mean()) / (std if std > 1e-9 else 1.0)

    f = _z(pd.to_numeric(day["factor_z_neu"], errors="coerce").fillna(0.0))
    p = _z(pd.to_numeric(day["ret20d_stock"], errors="coerce").fillna(0.0)) if "ret20d_stock" in day.columns else pd.Series(0.0, index=day.index)
    v = _z(pd.to_numeric(day["amount"], errors="coerce").fillna(0.0)) if "amount" in day.columns else pd.Series(0.0, index=day.index)
    c = _z(pd.to_numeric(day["net_buy_cube_count"], errors="coerce").fillna(0.0)) if "net_buy_cube_count" in day.columns else pd.Series(0.0, index=day.index)
    return 0.40 * f + 0.30 * p + 0.20 * v + 0.10 * c


def _srf_score_v2(day: pd.DataFrame) -> pd.Series:
    """
    SmartResonanceFactor v2 — re-ranker within the Xueqiu top-30% pool.

    Research-backed weights (corrected 2026-04-11):
      55%  factor_z_neu     — Xueqiu smart-money consensus (dominant, proven alpha)
      20%  +ret20d_stock    — mild momentum (within-holdings corr is +0.016 to +0.067, not reversal)
      15%  -ret_intra5d     — 日内反转: inverted 5-day cumulative intraday return
                              IC -6 to -8%, ICIR -3.6, win rate 85% (民生金工/中信建投 2025)
      10%  vol_price_div5d  — 量价背离: -corr(close, vol, 5d), IC 4-6% (国金证券 2022)

    HV penalty: stocks with hv20/hv60 > 1.5 (vol expanding) penalised -0.5σ
    (from 选股策略 gate_hv: only enter when HV20 < HV60)
    """
    def _z(s: pd.Series) -> pd.Series:
        std = s.std()
        return (s - s.mean()) / (std if std > 1e-9 else 1.0)

    f     = _z(pd.to_numeric(day["factor_z_neu"],    errors="coerce").fillna(0.0))
    mom   = _z(pd.to_numeric(day["ret20d_stock"],    errors="coerce").fillna(0.0)) if "ret20d_stock"    in day.columns else pd.Series(0.0, index=day.index)
    intra = _z(-pd.to_numeric(day["ret_intra5d"],    errors="coerce").fillna(0.0)) if "ret_intra5d"    in day.columns else pd.Series(0.0, index=day.index)
    div   = _z(pd.to_numeric(day["vol_price_div5d"], errors="coerce").fillna(0.0)) if "vol_price_div5d" in day.columns else pd.Series(0.0, index=day.index)
    score = 0.55 * f + 0.20 * mom + 0.15 * intra + 0.10 * div
    # HV penalty: expanding-vol stocks get a half-sigma deduction
    if "hv20_hv60_ratio" in day.columns:
        hv_ratio = pd.to_numeric(day["hv20_hv60_ratio"], errors="coerce").fillna(1.0)
        score = score - (hv_ratio > 1.5).astype(float) * 0.5
    return score


def _pick_top(
    day: pd.DataFrame,
    regime: str,
    cap_non_up: float,
    cap_up: float,
    non_up_vol_q: float = 1.0,
    top_k: int | None = None,
    use_srf: bool = False,
    use_srf_v2: bool = False,
) -> pd.DataFrame:
    n = len(day)
    if regime == "上涨":
        n_pool = max(1, int(round(0.5 * n)))
        pool = _select_top_with_industry_cap(day, n_target=n_pool, cap_ratio=cap_up)
        n_pick = max(1, int(round(0.3 * n))) if top_k is None else min(top_k, len(pool))
        top = pool.sort_values("ret20d_stock", ascending=True).head(n_pick).copy()
        if not top.empty:
            top = _select_top_with_industry_cap(top, n_target=len(top), cap_ratio=cap_up)
        return top
    if use_srf:
        day = day.copy()
        day["srf_score"] = _srf_score(day).values
        n_pick = top_k if top_k is not None else max(1, int(round(0.3 * n)))
        n_pick = min(n_pick, n)
        if float(non_up_vol_q) < 0.999 and "ret20d_stock" in day.columns:
            vol_proxy = pd.to_numeric(day["ret20d_stock"], errors="coerce").abs()
            valid = vol_proxy.dropna()
            if len(valid) >= 5:
                cut = float(valid.quantile(max(min(float(non_up_vol_q), 0.99), 0.50)))
                # NaN vol → treated as infinite (unknown = risky → filtered out)
                day = day[vol_proxy.fillna(float("inf")) <= cut].copy()
                n_pick = min(n_pick, len(day))
        if day.empty:
            return day
        cap_n = max(1, int(np.floor(n_pick * cap_non_up)))
        cand = day.sort_values("srf_score", ascending=False)
        picked_idx: list = []
        cnt: dict = {}
        for idx, row in cand.iterrows():
            ind = str(row.get("industry_l2", "其他"))
            if cnt.get(ind, 0) < cap_n:
                picked_idx.append(idx)
                cnt[ind] = cnt.get(ind, 0) + 1
            if len(picked_idx) >= n_pick:
                break
        if len(picked_idx) < n_pick:
            for idx, _ in cand.iterrows():
                if idx not in picked_idx:
                    picked_idx.append(idx)
                if len(picked_idx) >= n_pick:
                    break
        return day.loc[picked_idx].copy()
    if use_srf_v2:
        # Step 1: Xueqiu gate — same rank>=0.7 filter as baseline
        raw = day[day["rank"] >= 0.7].copy()
        if raw.empty:
            return raw
        # Step 2: Vol filter — identical to baseline path
        if float(non_up_vol_q) < 0.999 and "ret20d_stock" in raw.columns:
            vol_proxy = pd.to_numeric(raw["ret20d_stock"], errors="coerce").abs()
            valid = vol_proxy.dropna()
            if len(valid) >= 5:
                cut = float(valid.quantile(max(min(float(non_up_vol_q), 0.99), 0.50)))
                raw = raw[vol_proxy.fillna(float("inf")) <= cut].copy()
        if raw.empty:
            return raw
        # Step 3: Re-rank within gate by SRF v2 (reversal + divergence, no momentum)
        raw = raw.copy()
        raw["srf_v2_score"] = _srf_score_v2(raw).values
        n_pool = len(raw)
        n_pick = min(int(top_k), n_pool) if top_k is not None else n_pool
        cap_n = max(1, int(np.floor(n_pick * cap_non_up)))
        cand = raw.sort_values("srf_v2_score", ascending=False)
        picked_idx: list = []
        cnt: dict = {}
        for idx, row in cand.iterrows():
            ind = str(row.get("industry_l2", "其他"))
            if cnt.get(ind, 0) < cap_n:
                picked_idx.append(idx)
                cnt[ind] = cnt.get(ind, 0) + 1
            if len(picked_idx) >= n_pick:
                break
        if len(picked_idx) < n_pick:
            for idx, _ in cand.iterrows():
                if idx not in picked_idx:
                    picked_idx.append(idx)
                if len(picked_idx) >= n_pick:
                    break
        return raw.loc[picked_idx].copy()
    raw = day[day["rank"] >= 0.7].copy()
    if not raw.empty and float(non_up_vol_q) < 0.999 and "ret20d_stock" in raw.columns:
        vol_proxy = pd.to_numeric(raw["ret20d_stock"], errors="coerce").abs()
        valid = vol_proxy.dropna()
        if len(valid) >= 5:
            cut = float(valid.quantile(max(min(float(non_up_vol_q), 0.99), 0.50)))
            raw = raw[vol_proxy.fillna(float("inf")) <= cut].copy()
    if raw.empty:
        return raw
    return _select_top_with_industry_cap(raw, n_target=len(raw), cap_ratio=cap_non_up)


def _simulate_takeprofit(symbol: str, entry_date: pd.Timestamp, px_map: dict, tp_gain: float = 0.2, tp_dd: float = 0.08) -> tuple[float, bool]:
    px = px_map.get(symbol)
    if px is None or px.empty:
        return np.nan, False
    seq = px[px["date"] >= entry_date].head(11).copy()
    if len(seq) < 2:
        return np.nan, False
    entry = float(seq["close_sd"].iloc[0])
    if entry <= 0:
        return np.nan, False
    vals = seq["close_sd"].astype(float).values
    peak = entry
    for v in vals[1:]:
        if v > peak:
            peak = v
        gain = v / entry - 1.0
        dd_from_peak = 1.0 - (v / peak if peak > 0 else 1.0)
        if gain >= tp_gain:
            return tp_gain, True
        if dd_from_peak >= tp_dd:
            return v / entry - 1.0, True
    return vals[-1] / entry - 1.0, False


def _cooldown_active(cooldown_map: dict, idx: int) -> set:
    return {k for k, v in cooldown_map.items() if idx <= int(v)}


def _xq_heat_change(day: pd.DataFrame) -> pd.Series:
    if {"xq_buy_ratio", "xq_sell_ratio", "xq_heat_prev"}.issubset(day.columns):
        cur = pd.to_numeric(day["xq_buy_ratio"], errors="coerce") - pd.to_numeric(day["xq_sell_ratio"], errors="coerce")
        prev = pd.to_numeric(day["xq_heat_prev"], errors="coerce")
        return (cur - prev) / prev.abs().replace(0, np.nan)
    if {"net_buy_cube_count", "count_lag"}.issubset(day.columns):
        cur = pd.to_numeric(day["net_buy_cube_count"], errors="coerce")
        prev = pd.to_numeric(day["count_lag"], errors="coerce")
        return (cur - prev) / prev.abs().replace(0, np.nan)
    if {"net_buy_cube_count", "net_buy_cube_count_lag"}.issubset(day.columns):
        cur = pd.to_numeric(day["net_buy_cube_count"], errors="coerce")
        prev = pd.to_numeric(day["net_buy_cube_count_lag"], errors="coerce")
        return (cur - prev) / prev.abs().replace(0, np.nan)
    return pd.Series(np.nan, index=day.index)


def _build_rebalance(
    panel: pd.DataFrame,
    px_map: dict,
    hold_step: int,
    cap_non_up: float,
    cap_up: float,
    with_takeprofit: bool,
    industry_stop: float = -0.10,
    stock_stop: float = -0.08,
    cooldown_ind_steps: int = 2,
    cooldown_stock_steps: int = 1,
    non_up_vol_q: float = 1.0,
    top_k: int | None = None,
    use_srf: bool = False,
    use_srf_v2: bool = False,
    overheat_hs_trigger: float = 0.05,
    overheat_ind_trigger: float = 0.08,
    overheat_turn_q: float = 0.90,
    overheat_hs_release: float = 0.02,
    overheat_ind_release: float = 0.03,
    overheat_cap_non_up: float = 0.12,
    overheat_cap_up: float = 0.27,
    xq_enable: bool = False,
    xq_warn_drop: float = 0.25,
    xq_recover_rise: float = 0.10,
    xq_require_neg_ret: bool = True,
):
    df = panel.dropna(subset=["date", "stock_symbol", "factor_z_raw", "factor_z_neu", "fwd_ret_2w"]).copy()
    df = df[(df["date"] >= pd.Timestamp("2010-01-01")) & (df["date"] <= pd.Timestamp("2025-12-31"))].copy()
    df["factor_use"] = np.where(df["regime"] == "上涨", -df["factor_z_raw"], df["factor_z_neu"])
    lo = df.groupby("date")["factor_use"].transform(lambda s: s.quantile(0.05))
    hi = df.groupby("date")["factor_use"].transform(lambda s: s.quantile(0.95))
    df = df[(df["factor_use"] >= lo) & (df["factor_use"] <= hi)].copy()
    dates = sorted(df["date"].unique().tolist())
    keep = []
    i = 0
    while i < len(dates):
        keep.append(dates[i])
        i += hold_step
    df = df[df["date"].isin(set(keep))].copy()
    reb_rows = []
    hold_rows = []
    tp_event = {}
    risk_rows = []
    ind_cooldown_until = {}
    stk_cooldown_until = {}
    overheat_on = False
    turn_hist: list[float] = []
    for i_date, d in enumerate(sorted(df["date"].unique().tolist())):
        day = df[df["date"] == d].copy()
        n = len(day)
        if n < 5:
            continue
        hs_now = float(day["hs300_ret20"].dropna().iloc[0]) if "hs300_ret20" in day.columns and not day["hs300_ret20"].dropna().empty else np.nan
        ind_now = (
            float(day.groupby(day["industry_l2"].fillna("其他").astype(str))["ret20d_stock"].mean().sort_values(ascending=False).head(3).mean())
            if "ret20d_stock" in day.columns
            else np.nan
        )
        turn_now = float(pd.to_numeric(day["amount"], errors="coerce").fillna(0).sum()) if "amount" in day.columns else np.nan
        q_turn = float(np.quantile(turn_hist, float(overheat_turn_q))) if len(turn_hist) >= 20 else np.nan
        trig = bool(
            (pd.notna(hs_now) and hs_now > float(overheat_hs_trigger))
            or (pd.notna(ind_now) and ind_now > float(overheat_ind_trigger))
            or (pd.notna(turn_now) and pd.notna(q_turn) and turn_now > q_turn)
        )
        release = bool((pd.notna(hs_now) and hs_now < float(overheat_hs_release)) and (pd.notna(ind_now) and ind_now < float(overheat_ind_release)))
        if (not overheat_on) and trig:
            overheat_on = True
            risk_rows.append(
                {
                    "date": d,
                    "trigger_type": "overheat_on",
                    "subject": "portfolio",
                    "value": float(hs_now if pd.notna(hs_now) else 0.0),
                    "new_risk_scale": np.nan,
                    "recover_flag": False,
                }
            )
        elif overheat_on and release:
            overheat_on = False
            risk_rows.append(
                {
                    "date": d,
                    "trigger_type": "overheat_off",
                    "subject": "portfolio",
                    "value": float(hs_now if pd.notna(hs_now) else 0.0),
                    "new_risk_scale": np.nan,
                    "recover_flag": True,
                }
            )
        if pd.notna(turn_now):
            turn_hist.append(float(turn_now))
        day = day.copy()
        active_ind = _cooldown_active(ind_cooldown_until, i_date)
        active_stk = _cooldown_active(stk_cooldown_until, i_date)
        if active_ind or active_stk:
            day = day[
                (~day["industry_l2"].fillna("其他").astype(str).isin(active_ind))
                & (~day["stock_symbol"].astype(str).isin(active_stk))
            ].copy()
            if len(day) < 5:
                day = df[df["date"] == d].copy()
        day["rank"] = day["factor_use"].rank(pct=True, method="first")
        mid = day[(day["rank"] > 0.3) & (day["rank"] < 0.7)].copy()
        bot = day[day["rank"] <= 0.3].copy()
        regime = str(day["regime"].iloc[0])
        cap_non_up_use = max(float(cap_non_up), float(overheat_cap_non_up)) if overheat_on else float(cap_non_up)
        cap_up_use = max(float(cap_up), float(overheat_cap_up)) if overheat_on else float(cap_up)
        top = _pick_top(day, regime, cap_non_up=cap_non_up_use, cap_up=cap_up_use, non_up_vol_q=non_up_vol_q, top_k=top_k, use_srf=use_srf, use_srf_v2=use_srf_v2)
        if xq_enable and not top.empty:
            top = top.copy()
            top["xq_heat_chg"] = _xq_heat_change(top)
            ret5 = pd.to_numeric(top["ret5d_stock"], errors="coerce") if "ret5d_stock" in top.columns else pd.to_numeric(top["ret20d_stock"], errors="coerce") / 4.0
            if bool(xq_require_neg_ret):
                drop_mask = (top["xq_heat_chg"] <= -abs(float(xq_warn_drop))) & (ret5 < 0)
            else:
                drop_mask = top["xq_heat_chg"] <= -abs(float(xq_warn_drop))
            n_drop = int(drop_mask.sum())
            if n_drop > 0:
                top_inds = top["industry_l2"].fillna("其他").astype(str).value_counts().head(3).index.tolist()
                pool = day[day["industry_l2"].fillna("其他").astype(str).isin(top_inds)].copy()
                pool = pool[~pool["stock_symbol"].astype(str).isin(set(top["stock_symbol"].astype(str)))].copy()
                if not pool.empty:
                    pool["xq_heat_chg"] = _xq_heat_change(pool)
                    add = pool[pool["xq_heat_chg"] >= abs(float(xq_recover_rise))].sort_values(["xq_heat_chg", "factor_use"], ascending=False).head(n_drop).copy()
                    keep = top[~drop_mask].copy()
                    top = pd.concat([keep, add], ignore_index=True).head(len(keep) + len(add))
        if with_takeprofit and regime == "上涨" and not top.empty:
            rts = []
            for s in top["stock_symbol"].astype(str).tolist():
                rr, hit = _simulate_takeprofit(s, pd.Timestamp(d), px_map)
                tp_event[(pd.Timestamp(d), s)] = bool(hit)
                rts.append(rr)
            top = top.copy()
            top["fwd_ret_2w_sim"] = pd.to_numeric(pd.Series(rts), errors="coerce")
            top["fwd_ret_2w_use"] = top["fwd_ret_2w_sim"].fillna(top["fwd_ret_2w"])
        else:
            top = top.copy()
            top["fwd_ret_2w_use"] = top["fwd_ret_2w"]
        top["date"] = d
        top["weight"] = 1.0 / max(len(top), 1)
        hold_rows.append(top)
        if not top.empty:
            ind_ret = top.groupby(top["industry_l2"].fillna("其他").astype(str))["fwd_ret_2w_use"].mean()
            for ind_name, r in ind_ret.items():
                if pd.notna(r) and float(r) <= float(industry_stop):
                    ind_cooldown_until[ind_name] = i_date + max(int(cooldown_ind_steps), 1)
                    risk_rows.append(
                        {
                            "date": d,
                            "trigger_type": "industry_stop",
                            "subject": str(ind_name),
                            "value": float(r),
                            "new_risk_scale": np.nan,
                            "recover_flag": False,
                        }
                    )
            stk_ret = top.set_index(top["stock_symbol"].astype(str))["fwd_ret_2w_use"]
            for sym, r in stk_ret.items():
                if pd.notna(r) and float(r) <= float(stock_stop):
                    stk_cooldown_until[str(sym)] = i_date + max(int(cooldown_stock_steps), 1)
                    risk_rows.append(
                        {
                            "date": d,
                            "trigger_type": "stock_stop",
                            "subject": str(sym),
                            "value": float(r),
                            "new_risk_scale": np.nan,
                            "recover_flag": False,
                        }
                    )
        reb_rows.append(
            {
                "date": d,
                "regime": regime,
                "hs300_ret20": hs_now,
                "top3_ind_ret20": ind_now,
                "market_turnover_proxy": turn_now,
                "overheat_on": bool(overheat_on),
                "Bottom30": float(bot["fwd_ret_2w"].mean()) if not bot.empty else np.nan,
                "Middle40": float(mid["fwd_ret_2w"].mean()) if not mid.empty else np.nan,
                "Top30": float(top["fwd_ret_2w_use"].mean()) if not top.empty else np.nan,
                "top_symbols": "|".join(top["stock_symbol"].astype(str).tolist()),
            }
        )
    if reb_rows:
        reb = pd.DataFrame(reb_rows).sort_values("date").reset_index(drop=True)
        reb = _apply_up_exposure(reb, up_scale=0.5)
    else:
        reb = pd.DataFrame(columns=["date", "regime", "hs300_ret20", "top3_ind_ret20", "market_turnover_proxy", "overheat_on", "Bottom30", "Middle40", "Top30", "top_symbols"])
    hold = pd.concat(hold_rows, ignore_index=True) if hold_rows else pd.DataFrame(columns=["date", "stock_symbol"])
    risk_log = pd.DataFrame(risk_rows) if risk_rows else pd.DataFrame(columns=["date", "trigger_type", "subject", "value", "new_risk_scale", "recover_flag"])
    return reb, hold, tp_event, risk_log


def _apply_costs(group_ret: pd.DataFrame, one_way_cost: float = 0.001, impact_cost: float | None = None) -> pd.DataFrame:
    if group_ret is None or group_ret.empty:
        return pd.DataFrame(columns=["date", "regime", "Bottom30", "Middle40", "Top30", "top_symbols", "one_way_turnover", "trade_cost_rate", "Top30_net"])
    if impact_cost is not None:
        one_way_cost = float(impact_cost)
    x = group_ret.copy().sort_values("date").reset_index(drop=True)
    costs = []
    turnovers = []
    prev = set()
    for i, r in x.iterrows():
        cur = set(str(r["top_symbols"]).split("|")) if pd.notna(r["top_symbols"]) and str(r["top_symbols"]) else set()
        if i == 0:
            one_way_turnover = 1.0
        else:
            overlap = len(prev & cur)
            base_n = max(len(cur), 1)
            one_way_turnover = 1.0 - overlap / base_n
        turnovers.append(one_way_turnover)
        costs.append(one_way_turnover * one_way_cost)
        prev = cur
    x["one_way_turnover"] = turnovers
    x["trade_cost_rate"] = costs
    x["Top30_net"] = x["Top30"] - x["trade_cost_rate"]
    return x


def _apply_risk_controls(
    ret: pd.DataFrame,
    market_hot_q_mid: float = 0.8,
    market_hot_q_high: float = 0.9,
    dd_soft: float = -0.08,
    dd_mid: float = -0.10,
    dd_hard: float = -0.12,
    choppy_loss_scale: float = 1.0,
):
    if ret is None or ret.empty:
        return ret, pd.DataFrame(columns=["date", "trigger_type", "subject", "value", "new_risk_scale", "recover_flag"])
    x = ret.sort_values("date").reset_index(drop=True).copy()
    spread = (x["Top30_net"] - x["Bottom30"]).fillna(0.0)
    curve = 1.0
    peak = 1.0
    scales = []
    reasons = []
    risk_rows = []
    for i, r in x.iterrows():
        hs = float(r["hs300_ret20"]) if ("hs300_ret20" in x.columns and pd.notna(r.get("hs300_ret20", np.nan))) else np.nan
        hs_hist = x.loc[:i, "hs300_ret20"].dropna() if "hs300_ret20" in x.columns else pd.Series(dtype=float)
        q_mid = float(hs_hist.quantile(market_hot_q_mid)) if len(hs_hist) >= 10 else np.nan
        q_high = float(hs_hist.quantile(market_hot_q_high)) if len(hs_hist) >= 10 else np.nan
        market_scale = 1.0
        reason = ""
        if pd.notna(hs) and str(r.get("regime", "")) == "上涨":
            if pd.notna(q_high) and hs >= q_high:
                market_scale = 0.5
                reason = "market_hot_high"
            elif pd.notna(q_mid) and hs >= q_mid:
                market_scale = 0.7
                reason = "market_hot_mid"
        dd = curve / peak - 1.0
        dd_scale = 1.0
        if dd <= dd_hard:
            dd_scale = 0.5
        elif dd <= dd_mid:
            dd_scale = 0.6
        elif dd <= dd_soft:
            dd_scale = 0.75
        risk_scale = float(min(market_scale, dd_scale))
        if str(r.get("regime", "")) == "震荡" and float(spread.iloc[i]) < 0:
            risk_scale = float(min(risk_scale, max(min(float(choppy_loss_scale), 1.0), 0.30)))
        scales.append(risk_scale)
        reasons.append(reason if reason else ("drawdown_brake" if dd_scale < 1.0 else "none"))
        if risk_scale < 1.0:
            risk_rows.append(
                {
                    "date": r["date"],
                    "trigger_type": reason if reason else "drawdown_brake",
                    "subject": "portfolio",
                    "value": float(dd if not reason else hs),
                    "new_risk_scale": risk_scale,
                    "recover_flag": bool(risk_scale >= 0.99),
                }
            )
        sp = float(spread.iloc[i]) * risk_scale
        curve = curve * (1.0 + sp)
        peak = max(peak, curve)
        x.loc[i, "Top30_net"] = float(r["Bottom30"]) + sp
    x["risk_scale"] = scales
    x["risk_reason"] = reasons
    risk_log = pd.DataFrame(risk_rows) if risk_rows else pd.DataFrame(columns=["date", "trigger_type", "subject", "value", "new_risk_scale", "recover_flag"])
    return x, risk_log


def _metrics(x: pd.DataFrame) -> dict:
    if x is None or x.empty:
        out = {
            "ann_ret": np.nan,
            "ann_vol": np.nan,
            "sharpe": np.nan,
            "sortino": np.nan,
            "calmar": np.nan,
            "mdd": np.nan,
            "hit_ratio": np.nan,
            "excess": np.nan,
            "turnover": np.nan,
            "downside_dev": np.nan,
            "mdd_duration_periods": np.nan,
            "cvar95": np.nan,
        }
        for rg in ["上涨", "震荡", "下跌"]:
            out[f"{rg}_top_bottom"] = np.nan
        return out
    d = x.sort_values("date").copy()
    spread = d["Top30_net"] - d["Bottom30"]
    curve = (1 + spread.fillna(0)).cumprod()
    peak = curve.cummax()
    dd = curve / peak - 1.0
    mdd = float(dd.min()) if not dd.empty else float("nan")
    ann_factor = 26.0
    avg = float(spread.mean()) if not spread.empty else np.nan
    vol = float(spread.std(ddof=0)) if not spread.empty else np.nan
    ann_ret = float((1.0 + avg) ** ann_factor - 1.0) if pd.notna(avg) else np.nan
    ann_vol = float(vol * np.sqrt(ann_factor)) if pd.notna(vol) else np.nan
    neg = spread[spread < 0]
    downside = float(np.sqrt((neg.pow(2).mean()))) if not neg.empty else 0.0
    ann_downside = float(downside * np.sqrt(ann_factor))
    cvar95 = float(neg[neg <= neg.quantile(0.05)].mean()) if not neg.empty else np.nan
    sharpe = ann_ret / ann_vol if (pd.notna(ann_vol) and ann_vol > 0) else np.nan
    sortino = ann_ret / ann_downside if (pd.notna(ann_downside) and ann_downside > 0) else np.nan
    # mdd==0 means risk controls zeroed the strategy (spread flat) → calmar = 0 (degenerate, not infinite)
    calmar = ann_ret / abs(mdd) if (pd.notna(ann_ret) and pd.notna(mdd) and mdd != 0) else (0.0 if (pd.notna(ann_ret) and pd.notna(mdd)) else float("nan"))
    turnover = float(d["one_way_turnover"].mean()) if "one_way_turnover" in d.columns and not d.empty else np.nan
    dd_flag = dd < 0
    dur = []
    cur = 0
    for flag in dd_flag.tolist():
        if flag:
            cur += 1
        elif cur > 0:
            dur.append(cur)
            cur = 0
    if cur > 0:
        dur.append(cur)
    mdd_duration = float(max(dur)) if dur else 0.0
    out = {
        "ann_ret": ann_ret,
        "ann_vol": ann_vol,
        "sharpe": sharpe,
        "sortino": sortino,
        "calmar": calmar,
        "mdd": mdd,
        "hit_ratio": float((d["Top30_net"] > d["Bottom30"]).mean()),
        "excess": float(spread.mean()),
        "turnover": turnover,
        "downside_dev": ann_downside,
        "mdd_duration_periods": mdd_duration,
        "cvar95": cvar95,
    }
    for rg in ["上涨", "震荡", "下跌"]:
        s = d[d["regime"] == rg]
        out[f"{rg}_top_bottom"] = float((s["Top30_net"] - s["Bottom30"]).mean()) if not s.empty else np.nan
    return out


def _attribution(hold: pd.DataFrame, panel: pd.DataFrame) -> pd.DataFrame:
    if hold is None or hold.empty:
        return pd.DataFrame(columns=["date", "allocation", "selection", "timing", "total_excess"])
    h = hold[["date", "stock_symbol", "industry_l2", "fwd_ret_2w_use"]].copy()
    u = panel[["date", "stock_symbol", "industry_l2", "fwd_ret_2w"]].copy()
    h["industry"] = h["industry_l2"].fillna("其他")
    u["industry"] = u["industry_l2"].fillna("其他")
    rows = []
    for d, uh in u.groupby("date"):
        ph = h[h["date"] == d]
        uh = uh.dropna(subset=["fwd_ret_2w"])
        ph = ph.dropna(subset=["fwd_ret_2w_use"])
        if uh.empty or ph.empty:
            continue
        rb = float(uh["fwd_ret_2w"].mean())
        rp = float(ph["fwd_ret_2w_use"].mean())
        alloc = 0.0
        sel = 0.0
        inds = sorted(set(uh["industry"].tolist()) | set(ph["industry"].tolist()))
        for ind in inds:
            u_i = uh[uh["industry"] == ind]
            p_i = ph[ph["industry"] == ind]
            if u_i.empty:
                continue
            b_w = len(u_i) / max(len(uh), 1)
            p_w = len(p_i) / max(len(ph), 1)
            r_i = float(u_i["fwd_ret_2w"].mean())
            p_r = float(p_i["fwd_ret_2w_use"].mean()) if not p_i.empty else 0.0
            alloc += (p_w - b_w) * r_i
            sel += p_w * (p_r - r_i)
        total = rp - rb
        timing = total - alloc - sel
        rows.append({"date": d, "allocation": alloc, "selection": sel, "timing": timing, "total_excess": total})
    if not rows:
        return pd.DataFrame(columns=["date", "allocation", "selection", "timing", "total_excess"])
    return pd.DataFrame(rows).sort_values("date")


def _sell_fly(hold: pd.DataFrame, panel: pd.DataFrame, tp_event: dict) -> pd.DataFrame:
    if hold is None or hold.empty:
        return pd.DataFrame(columns=["sell_date", "stock_symbol", "post_2w_ret", "is_sell_fly", "sell_reason", "industry_l2", "amount"])
    panel_ret = panel[["date", "stock_symbol", "fwd_ret_2w", "industry_l2", "amount"]].copy()
    top_by_date = hold.groupby("date")["stock_symbol"].apply(lambda s: set(s.astype(str))).to_dict()
    dates = sorted(top_by_date.keys())
    rows = []
    for i in range(1, len(dates)):
        d_prev = pd.Timestamp(dates[i - 1])
        d_now = pd.Timestamp(dates[i])
        sold = top_by_date[d_prev] - top_by_date[d_now]
        for s in sold:
            r = panel_ret[(panel_ret["date"] == d_now) & (panel_ret["stock_symbol"] == s)]
            if r.empty:
                continue
            rr = float(r["fwd_ret_2w"].iloc[0]) if pd.notna(r["fwd_ret_2w"].iloc[0]) else np.nan
            rows.append(
                {
                    "sell_date": d_now,
                    "stock_symbol": s,
                    "post_2w_ret": rr,
                    "is_sell_fly": bool(pd.notna(rr) and rr > 0.10),
                    "sell_reason": "止盈触发" if tp_event.get((d_prev, s), False) else "调仓换出",
                    "industry_l2": r["industry_l2"].iloc[0],
                    "amount": r["amount"].iloc[0],
                }
            )
    return pd.DataFrame(rows)


def _run_one(
    panel: pd.DataFrame,
    px_map: dict,
    hold_step: int,
    liq_other: float,
    cap_non_up: float,
    cap_up: float,
    with_takeprofit: bool,
    risk_cfg: dict | None = None,
):
    cfg = {
        "industry_stop": -0.10,
        "stock_stop": -0.08,
        "cooldown_ind_steps": 2,
        "cooldown_stock_steps": 1,
        "market_hot_q_mid": 0.8,
        "market_hot_q_high": 0.9,
        "dd_soft": -0.08,
        "dd_mid": -0.10,
        "dd_hard": -0.12,
        "non_up_vol_q": 1.0,
        "choppy_loss_scale": 1.0,
        "overheat_hs_trigger": 0.05,
        "overheat_ind_trigger": 0.08,
        "overheat_turn_q": 0.90,
        "overheat_hs_release": 0.02,
        "overheat_ind_release": 0.03,
        "overheat_cap_non_up": 0.12,
        "overheat_cap_up": 0.27,
        "xq_enable": False,
        "xq_warn_drop": 0.25,
        "xq_recover_rise": 0.10,
        "xq_require_neg_ret": True,
        "top_k": None,
        "use_srf": False,
        "use_srf_v2": False,
    }
    if risk_cfg:
        cfg.update(risk_cfg)
    p = panel.copy()
    p = p[p["liq_rank_pct"] <= liq_other].copy() if "liq_rank_pct" in p.columns else p
    reb, hold, tp_event, risk_log_sel = _build_rebalance(
        p,
        px_map,
        hold_step=hold_step,
        cap_non_up=cap_non_up,
        cap_up=cap_up,
        with_takeprofit=with_takeprofit,
        industry_stop=float(cfg["industry_stop"]),
        stock_stop=float(cfg["stock_stop"]),
        cooldown_ind_steps=int(cfg["cooldown_ind_steps"]),
        cooldown_stock_steps=int(cfg["cooldown_stock_steps"]),
        non_up_vol_q=float(cfg["non_up_vol_q"]),
        overheat_hs_trigger=float(cfg["overheat_hs_trigger"]),
        overheat_ind_trigger=float(cfg["overheat_ind_trigger"]),
        overheat_turn_q=float(cfg["overheat_turn_q"]),
        overheat_hs_release=float(cfg["overheat_hs_release"]),
        overheat_ind_release=float(cfg["overheat_ind_release"]),
        overheat_cap_non_up=float(cfg["overheat_cap_non_up"]),
        overheat_cap_up=float(cfg["overheat_cap_up"]),
        xq_enable=bool(cfg["xq_enable"]),
        xq_warn_drop=float(cfg["xq_warn_drop"]),
        xq_recover_rise=float(cfg["xq_recover_rise"]),
        xq_require_neg_ret=bool(cfg["xq_require_neg_ret"]),
        top_k=cfg["top_k"],
        use_srf=bool(cfg["use_srf"]),
        use_srf_v2=bool(cfg["use_srf_v2"]),
    )
    ret = _apply_costs(reb, one_way_cost=0.001)
    ret, risk_log_dyn = _apply_risk_controls(
        ret,
        market_hot_q_mid=float(cfg["market_hot_q_mid"]),
        market_hot_q_high=float(cfg["market_hot_q_high"]),
        dd_soft=float(cfg["dd_soft"]),
        dd_mid=float(cfg["dd_mid"]),
        dd_hard=float(cfg["dd_hard"]),
        choppy_loss_scale=float(cfg["choppy_loss_scale"]),
    )
    m = _metrics(ret)
    attr = _attribution(hold, p)
    attr_sum = attr[["allocation", "selection", "timing", "total_excess"]].sum() if not attr.empty else pd.Series([np.nan, np.nan, np.nan, np.nan], index=["allocation", "selection", "timing", "total_excess"])
    sf = _sell_fly(hold, p, tp_event)
    risk_log = pd.concat([risk_log_sel, risk_log_dyn], ignore_index=True) if (not risk_log_sel.empty or not risk_log_dyn.empty) else pd.DataFrame(columns=["date", "trigger_type", "subject", "value", "new_risk_scale", "recover_flag"])
    sell_fly_rate = float(sf["is_sell_fly"].mean()) if not sf.empty else np.nan
    m["sell_fly_rate"] = sell_fly_rate
    m["attr_allocation"] = float(attr_sum["allocation"])
    m["attr_selection"] = float(attr_sum["selection"])
    m["attr_timing"] = float(attr_sum["timing"])
    m["attr_total"] = float(attr_sum["total_excess"])
    m["risk_trigger_count"] = float(len(risk_log))
    m["risk_scale_avg"] = float(ret["risk_scale"].mean()) if "risk_scale" in ret.columns and not ret.empty else np.nan
    return m, ret, hold, attr, sf, risk_log


def main():
    out_v6 = os.path.join(ROOT, "research", "baseline_v6")
    out_v61 = os.path.join(ROOT, "research", "baseline_v6_1")
    for d in [
        os.path.join(out_v6, "code"),
        os.path.join(out_v6, "output"),
        os.path.join(out_v6, "report"),
        os.path.join(out_v61, "output"),
        os.path.join(out_v61, "report"),
    ]:
        os.makedirs(d, exist_ok=True)
    panel = _prepare_panel_v5()
    panel, px_map = _enrich_from_stock_data(panel)
    panel = panel[(panel["date"] >= pd.Timestamp("2010-01-01")) & (panel["date"] <= pd.Timestamp("2025-12-31"))].copy()
    m_v5, ret_v5, hold_v5, attr_v5, sf_v5, risk_v5 = _run_one(panel, px_map, hold_step=10, liq_other=0.60, cap_non_up=0.10, cap_up=0.20, with_takeprofit=False)
    m_v6, ret_v6, hold_v6, attr_v6, sf_v6, risk_v6 = _run_one(panel, px_map, hold_step=10, liq_other=0.60, cap_non_up=0.15, cap_up=0.25, with_takeprofit=False)
    m_v61, ret_v61, hold_v61, attr_v61, sf_v61, risk_v61 = _run_one(panel, px_map, hold_step=10, liq_other=0.60, cap_non_up=0.15, cap_up=0.25, with_takeprofit=True)
    pd.DataFrame(
        {
            "metric": list(m_v6.keys()),
            "baseline_v5": [m_v5[k] for k in m_v6.keys()],
            "baseline_v6": [m_v6[k] for k in m_v6.keys()],
        }
    ).to_csv(os.path.join(out_v6, "output", "core_metrics_baseline_v6_2019_2025.csv"), index=False, encoding="utf-8-sig")
    attr_cmp = pd.DataFrame(
        {
            "source": ["allocation", "selection", "timing", "total_excess"],
            "baseline_v5": [m_v5["attr_allocation"], m_v5["attr_selection"], m_v5["attr_timing"], m_v5["attr_total"]],
            "baseline_v6": [m_v6["attr_allocation"], m_v6["attr_selection"], m_v6["attr_timing"], m_v6["attr_total"]],
        }
    )
    attr_cmp.to_csv(os.path.join(out_v6, "output", "attribution_baseline_v6_2019_2025.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(
        {
            "metric": list(m_v61.keys()),
            "baseline_v6": [m_v6[k] for k in m_v61.keys()],
            "baseline_v6_1": [m_v61[k] for k in m_v61.keys()],
        }
    ).to_csv(os.path.join(out_v61, "output", "core_metrics_baseline_v6_1_2019_2025.csv"), index=False, encoding="utf-8-sig")
    sf_cmp = pd.DataFrame(
        {
            "metric": ["sell_count", "sell_fly_count", "sell_fly_rate", "tp_sell_count"],
            "baseline_v6": [len(sf_v6), int(sf_v6["is_sell_fly"].sum()) if not sf_v6.empty else 0, m_v6["sell_fly_rate"], int((sf_v6["sell_reason"] == "止盈触发").sum()) if not sf_v6.empty else 0],
            "baseline_v6_1": [len(sf_v61), int(sf_v61["is_sell_fly"].sum()) if not sf_v61.empty else 0, m_v61["sell_fly_rate"], int((sf_v61["sell_reason"] == "止盈触发").sum()) if not sf_v61.empty else 0],
        }
    )
    sf_cmp.to_csv(os.path.join(out_v61, "output", "sell_fly_compare_v6_vs_v6_1.csv"), index=False, encoding="utf-8-sig")
    rows = []
    for hs, liq, cnd, cup in itertools.product([7, 10, 12], [0.55, 0.60, 0.65], [0.14, 0.15, 0.16], [0.24, 0.25, 0.26]):
        m, _, _, _, _, _ = _run_one(panel, px_map, hold_step=hs, liq_other=liq, cap_non_up=cnd, cap_up=cup, with_takeprofit=True)
        rows.append({"hold_step": hs, "liq_other": liq, "cap_non_up": cnd, "cap_up": cup, **m})
    grid = pd.DataFrame(rows).sort_values(["hold_step", "liq_other", "cap_non_up", "cap_up"])
    grid.to_csv(os.path.join(out_v61, "output", "sensitivity_grid_metrics.csv"), index=False, encoding="utf-8-sig")
    pos_ratio = float((grid["excess"] > 0).mean()) if not grid.empty else np.nan
    with open(os.path.join(out_v6, "report", "baseline_v6_eval.md"), "w", encoding="utf-8") as f:
        f.write("# baseline_v6 评估报告\n\n")
        f.write("- 目标：行业上限松绑后，改善行业Beta并提升总超额。\n")
        f.write(f"- 成本口径：单边交易成本0.1%。\n")
        f.write(f"- v5 年化={m_v5['ann_ret']:.2%}, 波动={m_v5['ann_vol']:.2%}, 夏普={m_v5['sharpe']:.3f}, 索提诺={m_v5['sortino']:.3f}, 卡玛={m_v5['calmar']:.3f}, 回撤={m_v5['mdd']:.2%}, 换手={m_v5['turnover']:.2%}\n")
        f.write(f"- v6 年化={m_v6['ann_ret']:.2%}, 波动={m_v6['ann_vol']:.2%}, 夏普={m_v6['sharpe']:.3f}, 索提诺={m_v6['sortino']:.3f}, 卡玛={m_v6['calmar']:.3f}, 回撤={m_v6['mdd']:.2%}, 换手={m_v6['turnover']:.2%}\n")
        f.write(f"- 三市top-bottom(v6)：上涨={m_v6['上涨_top_bottom']:.6f}, 震荡={m_v6['震荡_top_bottom']:.6f}, 下跌={m_v6['下跌_top_bottom']:.6f}\n")
    with open(os.path.join(out_v61, "report", "baseline_v6_1_eval.md"), "w", encoding="utf-8") as f:
        f.write("# baseline_v6.1 评估报告\n\n")
        f.write("- 目标：上涨市止盈后降低卖飞率并保持收益风险。\n")
        f.write(f"- 成本口径：单边交易成本0.1%。\n")
        f.write(f"- v6 sell_fly_rate={m_v6['sell_fly_rate']:.2%}, 年化={m_v6['ann_ret']:.2%}, 波动={m_v6['ann_vol']:.2%}, 夏普={m_v6['sharpe']:.3f}, 索提诺={m_v6['sortino']:.3f}, 卡玛={m_v6['calmar']:.3f}, 回撤={m_v6['mdd']:.2%}, 换手={m_v6['turnover']:.2%}\n")
        f.write(f"- v6.1 sell_fly_rate={m_v61['sell_fly_rate']:.2%}, 年化={m_v61['ann_ret']:.2%}, 波动={m_v61['ann_vol']:.2%}, 夏普={m_v61['sharpe']:.3f}, 索提诺={m_v61['sortino']:.3f}, 卡玛={m_v61['calmar']:.3f}, 回撤={m_v61['mdd']:.2%}, 换手={m_v61['turnover']:.2%}\n")
        f.write(f"- v6.1 三市top-bottom：上涨={m_v61['上涨_top_bottom']:.6f}, 震荡={m_v61['震荡_top_bottom']:.6f}, 下跌={m_v61['下跌_top_bottom']:.6f}\n")
    with open(os.path.join(out_v61, "report", "sensitivity_report.md"), "w", encoding="utf-8") as f:
        f.write("# baseline_v6.1 参数敏感性报告\n\n")
        f.write(f"- 网格样本数：{len(grid)}\n")
        f.write(f"- Calmar区间：[{grid['calmar'].min():.6f}, {grid['calmar'].max():.6f}]\n")
        f.write(f"- 正超额占比：{pos_ratio:.2%}\n")
        best = grid.sort_values("calmar", ascending=False).iloc[0]
        f.write(f"- 最优参数：hold_step={int(best['hold_step'])}, liq={best['liq_other']:.2f}, cap_non_up={best['cap_non_up']:.2f}, cap_up={best['cap_up']:.2f}\n")
    ret_v6.to_csv(os.path.join(out_v6, "output", "group_ret_baseline_v6_2019_2025.csv"), index=False, encoding="utf-8-sig")
    ret_v61.to_csv(os.path.join(out_v61, "output", "group_ret_baseline_v6_1_2019_2025.csv"), index=False, encoding="utf-8-sig")
    hold_v6.to_csv(os.path.join(out_v6, "output", "holdings_baseline_v6_2019_2025.csv"), index=False, encoding="utf-8-sig")
    hold_v61.to_csv(os.path.join(out_v61, "output", "holdings_baseline_v6_1_2019_2025.csv"), index=False, encoding="utf-8-sig")
    sf_v61.to_csv(os.path.join(out_v61, "output", "sell_fly_list_baseline_v6_1.csv"), index=False, encoding="utf-8-sig")
    risk_v6.to_csv(os.path.join(out_v6, "output", "risk_trigger_log_baseline_v6.csv"), index=False, encoding="utf-8-sig")
    risk_v61.to_csv(os.path.join(out_v61, "output", "risk_trigger_log_baseline_v6_1.csv"), index=False, encoding="utf-8-sig")
    print("done")
    print(os.path.join(out_v6, "report", "baseline_v6_eval.md"))
    print(os.path.join(out_v61, "report", "baseline_v6_1_eval.md"))
    print(os.path.join(out_v61, "report", "sensitivity_report.md"))


if __name__ == "__main__":
    main()
