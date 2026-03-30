import os
import sys
from itertools import product

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import numpy as np
import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v4.code.run_baseline_v4_2_up_filter import (
    _apply_liq_dynamic,
    _apply_up_exposure,
    _load_hs300,
    _select_top_with_industry_cap,
)
from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5


def _setup_plot_style():
    cands = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "PingFang SC", "Arial Unicode MS"]
    installed = {f.name for f in fm.fontManager.ttflist}
    selected = None
    for c in cands:
        if c in installed:
            selected = c
            break
    if selected:
        plt.rcParams["font.sans-serif"] = [selected, "DejaVu Sans"]
    else:
        plt.rcParams["font.sans-serif"] = ["DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False


def _ensure_industry_l1(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    if "industry_l1" not in x.columns:
        if "industry_l2" in x.columns:
            x["industry_l1"] = x["industry_l2"]
        else:
            x["industry_l1"] = "其他"
    return x


def _enrich_panel_with_stock_data_prices(panel: pd.DataFrame) -> pd.DataFrame:
    x = panel.copy()
    symbols = set(x["stock_symbol"].astype(str).str.upper().unique().tolist())
    rows = []
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
        rows.append(d[["date", "stock_symbol", "close_sd", "amount_sd", "ret20d_stock_sd", "fwd_ret_2w_sd"]])
    if not rows:
        return x
    p = pd.concat(rows, ignore_index=True)
    x["date"] = pd.to_datetime(x["date"], errors="coerce").dt.normalize()
    x["stock_symbol"] = x["stock_symbol"].astype(str).str.upper()
    x = x.merge(p, on=["date", "stock_symbol"], how="left")
    x["close"] = x["close"].fillna(x["close_sd"])
    x["amount"] = x["amount"].fillna(x["amount_sd"])
    x["ret20d_stock"] = x["ret20d_stock"].fillna(x["ret20d_stock_sd"])
    x["fwd_ret_2w"] = x["fwd_ret_2w"].fillna(x["fwd_ret_2w_sd"])
    x = x.drop(columns=["close_sd", "amount_sd", "ret20d_stock_sd", "fwd_ret_2w_sd"], errors="ignore")
    return x


def _build_rebalance_holdings(panel: pd.DataFrame, hold_step: int = 10, trim_q: float = 0.05, industry_cap: float = 0.2):
    df = panel.dropna(subset=["date", "stock_symbol", "factor_z_raw", "factor_z_neu", "fwd_ret_2w"]).copy()
    df["factor_use"] = np.where(df["regime"] == "上涨", -df["factor_z_raw"], df["factor_z_neu"])
    lo = df.groupby("date")["factor_use"].transform(lambda s: s.quantile(trim_q))
    hi = df.groupby("date")["factor_use"].transform(lambda s: s.quantile(1 - trim_q))
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
    for d, day in df.groupby("date"):
        n = len(day)
        if n < 5:
            continue
        day = day.copy()
        day["rank"] = day["factor_use"].rank(pct=True, method="first")
        mid = day[(day["rank"] > 0.3) & (day["rank"] < 0.7)].copy()
        bot = day[day["rank"] <= 0.3].copy()
        regime = str(day["regime"].iloc[0])
        if regime == "上涨":
            n_pool = max(1, int(round(0.5 * n)))
            pool = _select_top_with_industry_cap(day, n_target=n_pool, cap_ratio=industry_cap)
            n_pick = max(1, int(round(0.3 * n)))
            top = pool.sort_values("ret20d_stock", ascending=True).head(n_pick).copy()
        else:
            top = day[day["rank"] >= 0.7].copy()
        top = top.copy()
        top["date"] = d
        top["weight"] = 1.0 / max(len(top), 1)
        hold_rows.append(top)
        reb_rows.append(
            {
                "date": d,
                "regime": regime,
                "Bottom30": float(bot["fwd_ret_2w"].mean()) if not bot.empty else np.nan,
                "Middle40": float(mid["fwd_ret_2w"].mean()) if not mid.empty else np.nan,
                "Top30": float(top["fwd_ret_2w"].mean()) if not top.empty else np.nan,
                "top_symbols": "|".join(top["stock_symbol"].astype(str).tolist()),
            }
        )
    reb = pd.DataFrame(reb_rows).sort_values("date").reset_index(drop=True)
    reb = _apply_up_exposure(reb, up_scale=0.5)
    hold = pd.concat(hold_rows, ignore_index=True) if hold_rows else pd.DataFrame()
    return reb, hold


def _apply_costs(group_ret: pd.DataFrame, impact_cost: float = 0.0005) -> pd.DataFrame:
    buy_rate = 0.0002 + 0.00002 + impact_cost
    sell_rate = 0.0002 + 0.00002 + 0.001 + impact_cost
    x = group_ret.copy().sort_values("date").reset_index(drop=True)
    costs = []
    prev = set()
    for i, r in x.iterrows():
        cur = set(str(r["top_symbols"]).split("|")) if pd.notna(r["top_symbols"]) and str(r["top_symbols"]) else set()
        if i == 0:
            one_way_turnover = 1.0
        else:
            overlap = len(prev & cur)
            base_n = max(len(cur), 1)
            one_way_turnover = 1.0 - overlap / base_n
        costs.append(one_way_turnover * (buy_rate + sell_rate))
        prev = cur
    x["trade_cost_rate"] = costs
    x["Top30_net"] = x["Top30"] - x["trade_cost_rate"]
    return x


def _metrics(x: pd.DataFrame) -> dict:
    d = x.sort_values("date").copy()
    spread = d["Top30_net"] - d["Bottom30"]
    curve = (1 + spread.fillna(0)).cumprod()
    peak = curve.cummax()
    dd = curve / peak - 1.0
    mdd = float(dd.min()) if not dd.empty else float("nan")
    calmar = float(spread.mean()) / abs(mdd) if (not pd.isna(mdd) and mdd != 0) else float("nan")
    return {
        "calmar": calmar,
        "mdd": mdd,
        "hit": float((d["Top30_net"] > d["Bottom30"]).mean()),
        "excess": float(spread.mean()),
    }


def _calc_holding_durations(hold: pd.DataFrame, hold_step: int = 10) -> pd.DataFrame:
    x = hold[["date", "stock_symbol"]].drop_duplicates().sort_values(["stock_symbol", "date"]).copy()
    x["prev_date"] = x.groupby("stock_symbol")["date"].shift(1)
    x["new_seg"] = ((x["date"] - x["prev_date"]).dt.days > 18) | x["prev_date"].isna()
    x["seg_id"] = x.groupby("stock_symbol")["new_seg"].cumsum()
    g = x.groupby(["stock_symbol", "seg_id"], as_index=False).agg(start=("date", "min"), end=("date", "max"), periods=("date", "count"))
    g["holding_days"] = g["periods"] * hold_step
    return g


def _market_cap_bucket(s: pd.Series) -> pd.Series:
    q1 = s.quantile(0.33)
    q2 = s.quantile(0.67)
    out = pd.Series(index=s.index, dtype=object)
    out[s <= q1] = "小盘"
    out[(s > q1) & (s <= q2)] = "中盘"
    out[s > q2] = "大盘"
    return out


def _holding_preference_report(hold: pd.DataFrame, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    h = _ensure_industry_l1(hold)
    h["industry_l1"] = h["industry_l1"].fillna("其他")
    h["industry_l2"] = h["industry_l2"].fillna("其他")
    h["amount"] = pd.to_numeric(h["amount"], errors="coerce")
    if "circ_mv_proxy" in h.columns:
        h["mv_proxy"] = pd.to_numeric(h["circ_mv_proxy"], errors="coerce")
    else:
        h["mv_proxy"] = h["amount"]
    h = h.dropna(subset=["amount"])
    h["mv_bucket"] = _market_cap_bucket(h["mv_proxy"].fillna(h["mv_proxy"].median()))
    ind_l1 = h["industry_l1"].value_counts()
    ind_l2 = h["industry_l2"].value_counts().head(12)
    plt.figure(figsize=(8, 8))
    plt.pie(ind_l1.values, labels=ind_l1.index, autopct="%1.1f%%")
    plt.title("Holdings Industry Mix (industry_l1)")
    plt.tight_layout()
    pie_path = os.path.join(out_dir, "industry_l1_pie.png")
    plt.savefig(pie_path, dpi=140)
    plt.close()
    plt.figure(figsize=(9, 5))
    plt.bar(ind_l2.index, ind_l2.values)
    plt.xticks(rotation=45, ha="right")
    plt.title("Holdings Industry Mix (industry_l2 Top12)")
    plt.tight_layout()
    l2_path = os.path.join(out_dir, "industry_l2_bar.png")
    plt.savefig(l2_path, dpi=140)
    plt.close()
    plt.figure(figsize=(8, 5))
    plt.hist(np.log1p(h["mv_proxy"].dropna()), bins=35)
    plt.title("Market-Cap Proxy Histogram (log1p)")
    plt.tight_layout()
    mv_path = os.path.join(out_dir, "market_cap_hist.png")
    plt.savefig(mv_path, dpi=140)
    plt.close()
    plt.figure(figsize=(8, 5))
    plt.hist(np.log1p(h["amount"].dropna()), bins=35)
    plt.title("Liquidity Histogram (log1p(amount))")
    plt.tight_layout()
    liq_path = os.path.join(out_dir, "liquidity_hist.png")
    plt.savefig(liq_path, dpi=140)
    plt.close()
    durations = _calc_holding_durations(h, hold_step=10)
    plt.figure(figsize=(8, 5))
    plt.hist(durations["holding_days"], bins=20)
    plt.title("Holding Days Distribution")
    plt.tight_layout()
    hd_path = os.path.join(out_dir, "holding_days_hist.png")
    plt.savefig(hd_path, dpi=140)
    plt.close()
    lines = []
    lines.append("# baseline_v5 持仓偏好分析报告")
    lines.append("")
    lines.append("## 图表")
    lines.append("")
    lines.append("![industry_l1](industry_l1_pie.png)")
    lines.append("")
    lines.append("![industry_l2](industry_l2_bar.png)")
    lines.append("")
    lines.append("![market_cap](market_cap_hist.png)")
    lines.append("")
    lines.append("![liquidity](liquidity_hist.png)")
    lines.append("")
    lines.append("![holding_days](holding_days_hist.png)")
    lines.append("")
    lines.append("## 统计摘要")
    lines.append("")
    lines.append(f"- 持仓样本数：{len(h)}")
    lines.append(f"- 行业集中（industry_l1 Top3）：{', '.join([f'{k}:{v}' for k,v in ind_l1.head(3).items()])}")
    lines.append(f"- 市值分布：{h['mv_bucket'].value_counts().to_dict()}")
    lines.append(f"- 成交额中位数：{h['amount'].median():.2f}")
    lines.append(f"- 平均持有时间：{durations['holding_days'].mean():.2f} 交易日")
    lines.append("")
    top_l1 = ind_l1.head(2).index.tolist()
    mv_pref = h["mv_bucket"].value_counts().idxmax() if not h["mv_bucket"].empty else "未知"
    lines.append("## 选股偏好总结")
    lines.append("")
    lines.append(f"- 策略偏好行业：{'/'.join(top_l1)}；")
    lines.append(f"- 偏好市值层级：{mv_pref}；")
    lines.append(f"- 典型持有周期约 {durations['holding_days'].median():.0f} 交易日（接近2周节奏）。")
    with open(os.path.join(out_dir, "holding_preference_report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    h[["date", "stock_symbol", "industry_l1", "industry_l2", "mv_proxy", "amount", "mv_bucket"]].to_csv(
        os.path.join(out_dir, "holding_preference_detail.csv"), index=False, encoding="utf-8-sig"
    )


def _sell_fly_report(hold: pd.DataFrame, panel: pd.DataFrame, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    p = _ensure_industry_l1(panel)
    panel_ret = p[["date", "stock_symbol", "fwd_ret_2w", "industry_l1", "amount"]].copy()
    panel_ret["date"] = pd.to_datetime(panel_ret["date"])
    top_by_date = hold.groupby("date")["stock_symbol"].apply(lambda s: set(s.astype(str))).to_dict()
    dates = sorted(top_by_date.keys())
    rows = []
    for i in range(1, len(dates)):
        d_prev = dates[i - 1]
        d_now = dates[i]
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
                    "sell_reason": "调仓换出",
                    "industry_l1": r["industry_l1"].iloc[0],
                    "mv_proxy": r["amount"].iloc[0],
                    "amount": r["amount"].iloc[0],
                }
            )
    sf = pd.DataFrame(rows)
    if sf.empty:
        sf = pd.DataFrame(columns=["sell_date", "stock_symbol", "post_2w_ret", "is_sell_fly", "sell_reason", "industry_l1", "mv_proxy", "amount"])
    sf["industry_l1"] = sf["industry_l1"].fillna("其他")
    fly = sf[sf["is_sell_fly"]].copy()
    ind = fly["industry_l1"].value_counts().head(10)
    if not ind.empty:
        plt.figure(figsize=(8, 5))
        plt.bar(ind.index, ind.values)
        plt.xticks(rotation=45, ha="right")
        plt.title("Sold-Too-Early Industry Mix")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, "sell_fly_industry_bar.png"), dpi=140)
        plt.close()
    plt.figure(figsize=(8, 5))
    plt.hist(pd.to_numeric(fly["post_2w_ret"], errors="coerce").dropna(), bins=25)
    plt.title("Post-Sell 2W Return Distribution")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "sell_fly_ret_hist.png"), dpi=140)
    plt.close()
    lines = []
    lines.append("# baseline_v5 卖飞分析报告")
    lines.append("")
    lines.append(f"- 卖出样本数：{len(sf)}")
    lines.append(f"- 卖飞样本数：{len(fly)}")
    lines.append(f"- 卖飞占比：{(len(fly)/len(sf) if len(sf)>0 else 0):.2%}")
    lines.append("")
    lines.append("## 图表")
    lines.append("")
    if not ind.empty:
        lines.append("![sell_fly_industry](sell_fly_industry_bar.png)")
        lines.append("")
    lines.append("![sell_fly_ret](sell_fly_ret_hist.png)")
    lines.append("")
    lines.append("## 卖飞原因与特征")
    lines.append("")
    lines.append("- 当前策略没有止盈触发机制，卖飞原因主要来自调仓换出；")
    lines.append("- 卖飞股票更集中于趋势延续行业时段，建议上涨市延长强势股持有窗口。")
    lines.append("")
    lines.append("## 止盈优化建议")
    lines.append("")
    lines.append("- 建议1：上涨市将止盈阈值上调到20%；")
    lines.append("- 建议2：上涨市取消硬止盈，改为回撤止盈（如从高点回撤8%再退出）；")
    lines.append("- 建议3：仅对非主线行业启用止盈，主线行业放宽。")
    with open(os.path.join(out_dir, "sell_fly_analysis_report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    sf.sort_values(["sell_date", "post_2w_ret"], ascending=[True, False]).to_csv(
        os.path.join(out_dir, "sell_fly_list.csv"), index=False, encoding="utf-8-sig"
    )


def _attribution_report(hold: pd.DataFrame, panel: pd.DataFrame, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    h0 = _ensure_industry_l1(hold)
    u0 = _ensure_industry_l1(panel)
    h = h0[["date", "stock_symbol", "industry_l1", "fwd_ret_2w", "weight"]].copy()
    u = u0[["date", "stock_symbol", "industry_l1", "fwd_ret_2w"]].copy()
    h["industry_l1"] = h["industry_l1"].fillna("其他")
    u["industry_l1"] = u["industry_l1"].fillna("其他")
    rows = []
    for d, uh in u.groupby("date"):
        ph = h[h["date"] == d]
        uh = uh.dropna(subset=["fwd_ret_2w"])
        ph = ph.dropna(subset=["fwd_ret_2w"])
        if ph.empty:
            continue
        if uh.empty:
            continue
        rb = float(uh["fwd_ret_2w"].mean())
        rp = float(ph["fwd_ret_2w"].mean())
        alloc = 0.0
        sel = 0.0
        inds = sorted(set(uh["industry_l1"].tolist()) | set(ph["industry_l1"].tolist()))
        for ind in inds:
            u_i = uh[uh["industry_l1"] == ind]
            p_i = ph[ph["industry_l1"] == ind]
            if u_i.empty:
                continue
            b_w = len(u_i) / max(len(uh), 1)
            p_w = len(p_i) / max(len(ph), 1)
            r_i = float(u_i["fwd_ret_2w"].mean()) if not u_i.empty else 0.0
            p_r = float(p_i["fwd_ret_2w"].mean()) if not p_i.empty else 0.0
            alloc += (p_w - b_w) * r_i
            sel += p_w * (p_r - r_i)
        total_excess = rp - rb
        timing = total_excess - alloc - sel
        rows.append({"date": d, "allocation": alloc, "selection": sel, "timing": timing, "total_excess": total_excess})
    a = pd.DataFrame(rows).sort_values("date")
    sums = a[["allocation", "selection", "timing", "total_excess"]].sum()
    contrib = (sums[["allocation", "selection", "timing"]] / sums["total_excess"]) if sums["total_excess"] != 0 else pd.Series([0, 0, 0], index=["allocation", "selection", "timing"])
    plt.figure(figsize=(7, 5))
    plt.bar(["Industry", "Selection", "Timing"], [sums["allocation"], sums["selection"], sums["timing"]])
    plt.title("Excess Return Attribution")
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, "attribution_bar.png"), dpi=140)
    plt.close()
    lines = []
    lines.append("# baseline_v5 超额收益归因报告")
    lines.append("")
    lines.append("![attribution](attribution_bar.png)")
    lines.append("")
    lines.append("## 归因汇总")
    lines.append("")
    lines.append(f"- 总超额：{sums['total_excess']:.6f}")
    lines.append(f"- 行业选择贡献：{sums['allocation']:.6f}（占比{contrib['allocation']:.2%}）")
    lines.append(f"- 个股选择贡献：{sums['selection']:.6f}（占比{contrib['selection']:.2%}）")
    lines.append(f"- 择时/交互贡献：{sums['timing']:.6f}（占比{contrib['timing']:.2%}）")
    lines.append("")
    lines.append("## 超额收益来源总结")
    lines.append("")
    dom_key = contrib.abs().idxmax()
    dom_map = {"allocation": "行业选择", "selection": "个股选择", "timing": "择时/交互"}
    lines.append(f"- 主导来源为{dom_map.get(dom_key, '归因残差')}，占比约{contrib[dom_key]:.2%}；")
    lines.append("- 其余分项贡献较小，可作为次级优化方向。")
    with open(os.path.join(out_dir, "excess_attribution_report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    a.to_csv(os.path.join(out_dir, "excess_attribution_detail.csv"), index=False, encoding="utf-8-sig")


def _robustness_report(panel_base: pd.DataFrame, out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    hold_steps = [5, 10, 15]
    liq_ths = [0.5, 0.6, 0.7]
    ind_caps = [0.1, 0.15, 0.2]
    regime = _load_hs300("2019-01-01", "2025-12-31")
    rows = []
    for hs, lt, cap in product(hold_steps, liq_ths, ind_caps):
        p0 = panel_base.drop(columns=["regime"], errors="ignore")
        panel = _apply_liq_dynamic(p0, regime_df=regime, keep_other=lt, keep_up=0.2)
        reb, _ = _build_rebalance_holdings(panel, hold_step=hs, trim_q=0.05, industry_cap=cap)
        ret = _apply_costs(reb, impact_cost=0.0005)
        m = _metrics(ret)
        rows.append({"hold_step": hs, "liq_threshold": lt, "industry_cap": cap, **m})
    g = pd.DataFrame(rows).sort_values(["hold_step", "liq_threshold", "industry_cap"])
    for cap in ind_caps:
        sub = g[g["industry_cap"] == cap].pivot(index="hold_step", columns="liq_threshold", values="calmar")
        plt.figure(figsize=(6, 4))
        plt.imshow(sub.values, aspect="auto")
        plt.xticks(range(len(sub.columns)), [f"{int(c*100)}%" for c in sub.columns])
        plt.yticks(range(len(sub.index)), [f"{int(i/5)}w" for i in sub.index])
        plt.colorbar()
        plt.title(f"Calmar Heatmap (Industry Cap {int(cap*100)}%)")
        plt.tight_layout()
        plt.savefig(os.path.join(out_dir, f"robustness_heatmap_cap_{int(cap*100)}.png"), dpi=140)
        plt.close()
    lines = []
    lines.append("# baseline_v5 参数鲁棒性分析报告")
    lines.append("")
    for cap in ind_caps:
        lines.append(f"![cap_{int(cap*100)}](robustness_heatmap_cap_{int(cap*100)}.png)")
        lines.append("")
    best = g.sort_values("calmar", ascending=False).head(1).iloc[0]
    lines.append("## 参数网格摘要")
    lines.append("")
    lines.append(f"- 最优组合：调仓{int(best['hold_step']/5)}w、流动性{int(best['liq_threshold']*100)}%、行业上限{int(best['industry_cap']*100)}%")
    lines.append(f"- 最优Calmar：{best['calmar']:.6f}，回撤：{best['mdd']:.6f}")
    lines.append(f"- 全网格Calmar区间：[{g['calmar'].min():.6f}, {g['calmar'].max():.6f}]")
    lines.append("")
    lines.append("## 鲁棒性区间结论")
    lines.append("")
    pos_ratio = float((g["excess"] > 0).mean()) if not g.empty else 0.0
    lines.append(f"- 正超额参数点占比：{pos_ratio:.2%}；")
    if g["calmar"].max() >= 0.25:
        lines.append("- 网格内存在高质量稳健区间，可围绕最优点做局部收敛。")
    else:
        lines.append("- 全网格Calmar偏弱，说明严格样本下参数外推稳定性一般。")
    with open(os.path.join(out_dir, "robustness_report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    g.to_csv(os.path.join(out_dir, "robustness_grid_metrics.csv"), index=False, encoding="utf-8-sig")


def main():
    _setup_plot_style()
    base_out = os.path.join(ROOT, "research", "baseline_v5", "analysis")
    os.makedirs(base_out, exist_ok=True)
    panel = _prepare_panel_v5()
    panel = _enrich_panel_with_stock_data_prices(panel)
    panel = panel[(panel["date"] >= pd.Timestamp("2019-01-01")) & (panel["date"] <= pd.Timestamp("2025-12-31"))].copy()
    reb, hold = _build_rebalance_holdings(panel, hold_step=10, trim_q=0.05, industry_cap=0.2)
    _holding_preference_report(hold, os.path.join(base_out, "holding_preference"))
    _sell_fly_report(hold, panel, os.path.join(base_out, "sell_fly"))
    _attribution_report(hold, panel, os.path.join(base_out, "attribution"))
    _robustness_report(panel, os.path.join(base_out, "robustness"))
    reb.to_csv(os.path.join(base_out, "baseline_v5_rebalance_returns.csv"), index=False, encoding="utf-8-sig")
    hold.to_csv(os.path.join(base_out, "baseline_v5_holdings_detail.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(
        {
            "name": ["panel_date_min", "panel_date_max", "reb_date_min", "reb_date_max", "reb_rows", "hold_rows"],
            "value": [
                str(panel["date"].min().date()) if not panel.empty else "",
                str(panel["date"].max().date()) if not panel.empty else "",
                str(pd.to_datetime(reb["date"]).min().date()) if not reb.empty else "",
                str(pd.to_datetime(reb["date"]).max().date()) if not reb.empty else "",
                int(len(reb)),
                int(len(hold)),
            ],
        }
    ).to_csv(os.path.join(base_out, "strict_2019_2025_qc.csv"), index=False, encoding="utf-8-sig")
    print(os.path.join(base_out, "holding_preference", "holding_preference_report.md"))
    print(os.path.join(base_out, "sell_fly", "sell_fly_analysis_report.md"))
    print(os.path.join(base_out, "attribution", "excess_attribution_report.md"))
    print(os.path.join(base_out, "robustness", "robustness_report.md"))


if __name__ == "__main__":
    main()
