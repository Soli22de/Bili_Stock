"""
Professional Visual Report for Expert Review
==============================================
Publication-quality charts with context, annotations, and storytelling.

Run: PYTHONIOENCODING=utf-8 python research/baseline_v6_1/code/generate_visual_report.py
"""

import os
import sys
import time

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.gridspec import GridSpec
from matplotlib.patches import FancyBboxPatch
from datetime import datetime

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

OUT_DIR = os.path.join(ROOT, "research", "baseline_v6_1", "report", "visual")
os.makedirs(OUT_DIR, exist_ok=True)

# ── Professional Style ────────────────────────────────────────────────────────
plt.rcParams.update({
    "font.sans-serif": ["Microsoft YaHei", "SimHei", "Arial"],
    "axes.unicode_minus": False,
    "figure.facecolor": "#FAFAFA",
    "axes.facecolor": "#FFFFFF",
    "axes.edgecolor": "#CCCCCC",
    "axes.grid": True,
    "grid.color": "#E8E8E8",
    "grid.linewidth": 0.6,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "font.size": 11,
    "axes.titlesize": 15,
    "axes.labelsize": 12,
    "xtick.labelsize": 10,
    "ytick.labelsize": 10,
})

C_BLUE = "#1A73E8"
C_RED = "#EA4335"
C_GREEN = "#34A853"
C_ORANGE = "#FBBC04"
C_GRAY = "#9AA0A6"
C_DARK = "#202124"
C_LIGHT_BLUE = "#D2E3FC"
C_LIGHT_RED = "#FCE8E6"
C_LIGHT_GREEN = "#E6F4EA"


def _add_watermark(fig, text="Bili_Stock Quant"):
    fig.text(0.99, 0.01, text, fontsize=8, color="#CCCCCC",
             ha="right", va="bottom", style="italic")


def _metric_box(ax, x, y, label, value, color=C_DARK, fontsize=20):
    ax.text(x, y, value, fontsize=fontsize, fontweight="bold", color=color,
            ha="center", va="center", transform=ax.transAxes)
    ax.text(x, y - 0.18, label, fontsize=10, color=C_GRAY,
            ha="center", va="center", transform=ax.transAxes)


def run_backtest():
    from research.baseline_v6_1.code.run_baseline_v6_v61_suite import _enrich_from_stock_data, _run_one
    from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5

    print("Loading panel ...", flush=True)
    panel = _prepare_panel_v5()
    panel = panel[
        (panel["date"] >= pd.Timestamp("2010-01-01"))
        & (panel["date"] <= pd.Timestamp("2025-12-31"))
    ].copy()
    panel, px_map = _enrich_from_stock_data(panel)
    panel["stock_symbol"] = panel["stock_symbol"].str.upper()

    nf_path = os.path.join(ROOT, "data", "market_cache", "northbound_daily.csv")
    if os.path.exists(nf_path):
        nf = pd.read_csv(nf_path, encoding="utf-8-sig")
        nf["date"] = pd.to_datetime(nf["date"]).dt.normalize()
        panel = panel.merge(nf[["date", "stock_symbol", "north_hold_chg_5d"]], on=["date", "stock_symbol"], how="left")
        panel["north_hold_chg_5d"] = panel["north_hold_chg_5d"].fillna(0)

    RISK = dict(
        non_up_vol_q=0.50, dd_soft=-0.05, dd_mid=-0.07, dd_hard=-0.10,
        choppy_loss_scale=0.0, choppy_loss_floor=0.0,
        use_srf=False, use_srf_v2=True, top_k=15, go_flat_choppy=False,
    )

    print("Running backtest ...", flush=True)
    m, ret, hold, _, _, risk_log = _run_one(
        panel, px_map, hold_step=12, liq_other=0.60,
        cap_non_up=0.10, cap_up=0.20, with_takeprofit=True, risk_cfg=RISK,
    )
    print(f"Calmar={m['calmar']:.4f}, AnnRet={m['ann_ret']*100:.2f}%, Sharpe={m['sharpe']:.3f}", flush=True)
    return panel, m, ret, hold, risk_log


def chart0_dashboard(m, ret):
    """0. KPI dashboard — single-page summary."""
    fig = plt.figure(figsize=(16, 9), facecolor="#FAFAFA")
    fig.suptitle("Bili_Stock 量化策略回测报告", fontsize=22, fontweight="bold", color=C_DARK, y=0.97)
    fig.text(0.5, 0.935, "回测区间: 2010-01 ~ 2025-12  |  调仓周期: 12 个交易日  |  宇宙: A 股",
             fontsize=11, color=C_GRAY, ha="center")

    # Top row: KPI boxes
    gs = GridSpec(3, 5, figure=fig, top=0.88, bottom=0.05, left=0.04, right=0.96, hspace=0.4, wspace=0.3)

    kpi_data = [
        ("年化收益", f"{m['ann_ret']*100:.1f}%", C_GREEN if m["ann_ret"] > 0 else C_RED),
        ("Calmar Ratio", f"{m['calmar']:.2f}", C_GREEN if m["calmar"] > 0.5 else C_ORANGE),
        ("Sharpe Ratio", f"{m['sharpe']:.2f}", C_GREEN if m["sharpe"] > 1.0 else C_ORANGE),
        ("最大回撤", f"{m['mdd']*100:.1f}%", C_RED),
        ("胜率", f"{m['hit_ratio']*100:.1f}%", C_GREEN if m["hit_ratio"] > 0.5 else C_ORANGE),
    ]
    for i, (label, value, color) in enumerate(kpi_data):
        ax = fig.add_subplot(gs[0, i])
        ax.set_xlim(0, 1); ax.set_ylim(0, 1)
        ax.axis("off")
        rect = FancyBboxPatch((0.05, 0.05), 0.9, 0.9, boxstyle="round,pad=0.05",
                               facecolor="white", edgecolor="#E0E0E0", linewidth=1.5)
        ax.add_patch(rect)
        _metric_box(ax, 0.5, 0.6, label, value, color=color, fontsize=22)

    # Middle: equity curve (compact)
    ax_eq = fig.add_subplot(gs[1, :3])
    ret_s = ret.sort_values("date").copy()
    spread = (ret_s["Top30_net"] - ret_s["Bottom30"]).fillna(0)
    equity = (1 + spread).cumprod()
    ax_eq.fill_between(ret_s["date"], 1, equity, alpha=0.15, color=C_BLUE)
    ax_eq.plot(ret_s["date"], equity, color=C_BLUE, linewidth=1.8)
    hs = pd.read_csv(os.path.join(ROOT, "data", "market_cache", "hs300_daily_cache.csv"), encoding="utf-8-sig")
    hs["date"] = pd.to_datetime(hs["date"]).dt.normalize()
    hs["close"] = pd.to_numeric(hs["close"], errors="coerce")
    hs = hs[(hs["date"] >= ret_s["date"].min()) & (hs["date"] <= ret_s["date"].max())]
    hs_eq = hs["close"] / hs["close"].iloc[0]
    ax_eq.plot(hs["date"], hs_eq, color=C_GRAY, linewidth=1, alpha=0.6)
    ax_eq.set_title("累计净值", fontsize=13, fontweight="bold", loc="left")
    ax_eq.text(0.98, 0.95, f"策略 {equity.iloc[-1]:.1f}x", fontsize=11, color=C_BLUE,
               ha="right", va="top", transform=ax_eq.transAxes, fontweight="bold")
    ax_eq.text(0.98, 0.82, f"沪深300 {hs_eq.iloc[-1]:.1f}x", fontsize=10, color=C_GRAY,
               ha="right", va="top", transform=ax_eq.transAxes)

    # Middle right: drawdown
    ax_dd = fig.add_subplot(gs[1, 3:])
    peak = equity.cummax()
    dd = (equity / peak - 1) * 100
    ax_dd.fill_between(ret_s["date"], dd, 0, color=C_RED, alpha=0.3)
    ax_dd.plot(ret_s["date"], dd, color=C_RED, linewidth=0.8)
    ax_dd.set_title("回撤", fontsize=13, fontweight="bold", loc="left")
    ax_dd.text(0.98, 0.05, f"最大 {dd.min():.1f}%", fontsize=11, color=C_RED,
               ha="right", va="bottom", transform=ax_dd.transAxes, fontweight="bold")

    # Bottom: annual returns
    ax_yr = fig.add_subplot(gs[2, :])
    ret_s["year"] = pd.to_datetime(ret_s["date"]).dt.year
    yearly = []
    for yr, g in ret_s.groupby("year"):
        sp = (g["Top30_net"] - g["Bottom30"]).fillna(0)
        yearly.append({"year": yr, "ret": (1 + sp).prod() - 1})
    ydf = pd.DataFrame(yearly)
    colors = [C_GREEN if r >= 0 else C_RED for r in ydf["ret"]]
    bars = ax_yr.bar(ydf["year"], ydf["ret"] * 100, color=colors, width=0.7, edgecolor="white", linewidth=0.5)
    for bar, val in zip(bars, ydf["ret"]):
        y = bar.get_height()
        ax_yr.text(bar.get_x() + bar.get_width() / 2, y + (0.8 if y >= 0 else -2.5),
                   f"{val*100:.0f}%", ha="center", fontsize=9, color=C_DARK)
    ax_yr.axhline(0, color=C_DARK, linewidth=0.8)
    ax_yr.set_title("年度收益", fontsize=13, fontweight="bold", loc="left")
    ax_yr.set_ylabel("%")
    wins = sum(1 for r in ydf["ret"] if r > 0)
    ax_yr.text(0.98, 0.95, f"{wins}/{len(ydf)} 年正收益", fontsize=10, color=C_GRAY,
               ha="right", va="top", transform=ax_yr.transAxes)

    _add_watermark(fig)
    fig.savefig(os.path.join(OUT_DIR, "0_dashboard.png"), dpi=180, bbox_inches="tight")
    plt.close(fig)
    print("  0. Dashboard done", flush=True)


def chart1_equity_deep(ret):
    """1. Equity curve with regime background shading."""
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 9), height_ratios=[3, 1], sharex=True)
    ret_s = ret.sort_values("date").copy()
    spread = (ret_s["Top30_net"] - ret_s["Bottom30"]).fillna(0)
    equity = (1 + spread).cumprod()

    # Regime background
    regime_colors = {"上涨": C_LIGHT_RED, "震荡": "#FFF3E0", "下跌": C_LIGHT_GREEN}
    if "regime" in ret_s.columns:
        for regime, color in regime_colors.items():
            mask = ret_s["regime"] == regime
            for start, end in _contiguous_ranges(ret_s["date"], mask):
                ax1.axvspan(start, end, alpha=0.25, color=color, linewidth=0)

    ax1.plot(ret_s["date"], equity, color=C_BLUE, linewidth=2, label="策略净值")
    hs = pd.read_csv(os.path.join(ROOT, "data", "market_cache", "hs300_daily_cache.csv"), encoding="utf-8-sig")
    hs["date"] = pd.to_datetime(hs["date"]).dt.normalize()
    hs["close"] = pd.to_numeric(hs["close"], errors="coerce")
    hs = hs[(hs["date"] >= ret_s["date"].min()) & (hs["date"] <= ret_s["date"].max())]
    hs_eq = hs["close"] / hs["close"].iloc[0]
    ax1.plot(hs["date"], hs_eq, color=C_GRAY, linewidth=1.2, label="沪深300", alpha=0.7)

    ax1.set_ylabel("累计净值", fontsize=12)
    ax1.set_title(f"策略净值曲线  |  年化 {ret_s['Top30_net'].sub(ret_s['Bottom30']).mean()*252/12*100:.1f}%  "
                  f"Sharpe {spread.mean()/spread.std()*np.sqrt(252/12):.2f}  "
                  f"终值 {equity.iloc[-1]:.2f}x",
                  fontsize=15, fontweight="bold", loc="left")
    ax1.legend(loc="upper left", fontsize=11)

    # Bottom: per-period return bars
    bar_colors = [C_GREEN if s >= 0 else C_RED for s in spread]
    ax2.bar(ret_s["date"], spread * 100, color=bar_colors, width=8, alpha=0.7)
    ax2.axhline(0, color=C_DARK, linewidth=0.5)
    ax2.set_ylabel("期收益 (%)", fontsize=11)
    ax2.set_xlabel("")

    # Legend for regime shading
    from matplotlib.patches import Patch
    handles = [Patch(facecolor=regime_colors[r], alpha=0.4, label=r) for r in ["上涨", "震荡", "下跌"]]
    ax1.legend(handles=handles + ax1.get_legend_handles_labels()[0], loc="upper left", fontsize=10)

    _add_watermark(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "1_equity_deep.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  1. Equity deep done", flush=True)


def _contiguous_ranges(dates, mask):
    """Yield (start, end) date ranges where mask is True."""
    ranges = []
    in_range = False
    start = None
    for d, m in zip(dates, mask):
        if m and not in_range:
            start = d; in_range = True
        elif not m and in_range:
            ranges.append((start, d)); in_range = False
    if in_range:
        ranges.append((start, dates.iloc[-1]))
    return ranges


def chart5_ic_heatmap(panel):
    """5. Factor IC heatmap — professional version."""
    factor_cols = {
        "factor_z_neu": "雪球共识信号",
        "vol_price_div5d": "量价背离 (5日)",
        "ret_intra5d": "日内反转 (5日)",
        "ret20d_stock": "20日价格动量",
        "hv20_hv60_ratio": "波动率比率",
        "highconv_10d": "高conviction买入",
        "north_hold_chg_5d": "北向持仓变化 (5日)",
    }

    ic_data = []
    for col, label in factor_cols.items():
        if col not in panel.columns:
            continue
        row = {"factor": label}
        mult = -1.0 if col == "ret_intra5d" else 1.0
        for regime in ["上涨", "震荡", "下跌", "全样本"]:
            sub = panel if regime == "全样本" else panel[panel["regime"] == regime]
            sub = sub.dropna(subset=[col, "fwd_ret_2w"])
            if len(sub) < 50:
                row[regime] = np.nan; continue
            vals = sub[col] * mult if mult != 1.0 else sub[col]
            ics = sub.assign(_f=vals).groupby("date").apply(
                lambda g: g["_f"].corr(g["fwd_ret_2w"]) if len(g) >= 5 else np.nan
            ).dropna()
            row[regime] = ics.mean() if len(ics) > 0 else np.nan
        ic_data.append(row)

    df = pd.DataFrame(ic_data).set_index("factor")

    fig, ax = plt.subplots(figsize=(12, 7))
    data = df.values
    im = ax.imshow(data, cmap="RdYlGn", aspect="auto", vmin=-0.02, vmax=0.03)

    ax.set_xticks(range(len(df.columns)))
    ax.set_xticklabels(df.columns, fontsize=13, fontweight="bold")
    ax.set_yticks(range(len(df.index)))
    ax.set_yticklabels(df.index, fontsize=12)

    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            v = data[i, j]
            if pd.notna(v):
                color = "white" if abs(v) > 0.015 else C_DARK
                weight = "bold" if abs(v) > 0.01 else "normal"
                ax.text(j, i, f"{v:.4f}", ha="center", va="center",
                        fontsize=12, color=color, fontweight=weight)

    cbar = plt.colorbar(im, ax=ax, shrink=0.8, pad=0.02)
    cbar.set_label("Information Coefficient (IC)", fontsize=11)

    ax.set_title("因子 IC 热力图  |  绿色 = 正向预测能力, 红色 = 反向",
                 fontsize=15, fontweight="bold", loc="left", pad=15)

    # Add annotation
    best_idx = np.unravel_index(np.nanargmax(data), data.shape)
    ax.annotate("", xy=(best_idx[1], best_idx[0]),
                xytext=(best_idx[1] + 0.5, best_idx[0] - 0.5),
                arrowprops=dict(arrowstyle="->", color=C_GREEN, lw=2))

    _add_watermark(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "5_ic_heatmap.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  5. IC heatmap done", flush=True)


def chart6_regime_analysis(ret):
    """6. Regime analysis — distribution + return + equity per regime."""
    ret_s = ret.sort_values("date").copy()
    spread = (ret_s["Top30_net"] - ret_s["Bottom30"]).fillna(0)

    fig = plt.figure(figsize=(16, 6))
    gs = GridSpec(1, 3, figure=fig, wspace=0.35)

    regime_colors = {"上涨": C_RED, "震荡": C_ORANGE, "下跌": C_GREEN}

    # Left: Regime pie
    ax1 = fig.add_subplot(gs[0, 0])
    counts = ret_s["regime"].value_counts()
    wedges, texts, autotexts = ax1.pie(
        counts, labels=None,
        colors=[regime_colors.get(r, C_GRAY) for r in counts.index],
        autopct="%1.0f%%", textprops={"fontsize": 13, "fontweight": "bold"},
        startangle=90, pctdistance=0.6,
    )
    ax1.legend([f"{r} ({c}期)" for r, c in zip(counts.index, counts)],
               loc="lower center", fontsize=10, ncol=1, bbox_to_anchor=(0.5, -0.15))
    ax1.set_title("调仓期市况分布", fontsize=14, fontweight="bold")

    # Middle: Per-regime return distribution (box plot style)
    ax2 = fig.add_subplot(gs[0, 1])
    regime_list = ["上涨", "震荡", "下跌"]
    bp_data = []
    means = []
    for regime in regime_list:
        mask = ret_s["regime"] == regime
        sp = spread[mask] * 100
        bp_data.append(sp.dropna().values)
        means.append(sp.mean())

    bp = ax2.boxplot(bp_data, labels=regime_list, patch_artist=True, widths=0.5,
                     medianprops=dict(color=C_DARK, linewidth=2))
    for patch, regime in zip(bp["boxes"], regime_list):
        patch.set_facecolor(regime_colors.get(regime, C_GRAY))
        patch.set_alpha(0.5)
    for i, mean in enumerate(means):
        ax2.scatter(i + 1, mean, color=C_DARK, s=80, zorder=5, marker="D")
        ax2.text(i + 1.25, mean, f"{mean:.2f}%", fontsize=10, va="center", fontweight="bold")
    ax2.axhline(0, color=C_DARK, linewidth=0.8, linestyle="--")
    ax2.set_title("各市况期收益分布", fontsize=14, fontweight="bold")
    ax2.set_ylabel("期收益 (%)")

    # Right: Cumulative equity per regime
    ax3 = fig.add_subplot(gs[0, 2])
    for regime in regime_list:
        mask = ret_s["regime"] == regime
        sp = spread.copy()
        sp[~mask] = 0
        eq = (1 + sp).cumprod()
        ax3.plot(ret_s["date"], eq, color=regime_colors.get(regime, C_GRAY),
                 linewidth=1.8, label=f"{regime} ({(1+spread[mask]).prod()-1:.0%})")
    ax3.set_title("各市况累计贡献", fontsize=14, fontweight="bold")
    ax3.set_ylabel("累计净值")
    ax3.legend(fontsize=10)

    fig.suptitle("市况分析  |  震荡期 go-flat 是核心 alpha 来源", fontsize=16, fontweight="bold", y=1.02)
    _add_watermark(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "6_regime_analysis.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  6. Regime analysis done", flush=True)


def chart7_holdings_table(hold, ret):
    """7. Holdings detail — professional table."""
    if hold.empty:
        print("  7. Holdings — SKIPPED", flush=True)
        return
    hold = hold.copy()
    hold["date"] = pd.to_datetime(hold["date"]).dt.normalize()
    dates = sorted(hold["date"].unique())
    recent = dates[-5:] if len(dates) >= 5 else dates

    all_rows = []
    for d in recent:
        day = hold[hold["date"] == d]
        regime = ret[ret["date"] == d]["regime"].iloc[0] if d in ret["date"].values else "?"
        for _, h in day.iterrows():
            all_rows.append({
                "调仓日": str(d.date()),
                "市况": regime,
                "股票代码": h.get("stock_symbol", ""),
                "行业": str(h.get("industry_l2", ""))[:10],
            })
    df = pd.DataFrame(all_rows)
    df.to_csv(os.path.join(OUT_DIR, "7_holdings_detail.csv"), index=False, encoding="utf-8-sig")

    # Summary
    summary = df.groupby("调仓日").agg(
        市况=("市况", "first"),
        持仓数=("股票代码", "count"),
        行业数=("行业", "nunique"),
        行业列表=("行业", lambda x: ", ".join(x.value_counts().head(3).index)),
    ).reset_index()

    fig, ax = plt.subplots(figsize=(14, 3.5))
    ax.axis("off")
    table = ax.table(cellText=summary.values, colLabels=summary.columns, loc="center", cellLoc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 1.8)
    for j in range(len(summary.columns)):
        table[0, j].set_facecolor(C_BLUE)
        table[0, j].set_text_props(color="white", fontweight="bold")
    for i in range(1, len(summary) + 1):
        for j in range(len(summary.columns)):
            table[i, j].set_facecolor("#F8F9FA" if i % 2 == 0 else "white")
    ax.set_title("最近 5 次调仓持仓概览", fontsize=15, fontweight="bold", pad=20)
    _add_watermark(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "7_holdings_summary.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  7. Holdings done", flush=True)


def chart8_industry(hold):
    """8. Industry concentration — treemap style."""
    if hold.empty or "industry_l2" not in hold.columns:
        print("  8. Industry — SKIPPED", flush=True)
        return
    hold = hold.copy()
    hold["date"] = pd.to_datetime(hold["date"]).dt.normalize()

    # Aggregate across all periods
    ind_all = hold["industry_l2"].value_counts().head(15)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7), width_ratios=[1, 1.2])

    # Left: overall industry distribution
    colors_palette = plt.cm.Set3(np.linspace(0, 1, len(ind_all)))
    labels = [f"{n[:8]}" for n in ind_all.index]
    bars = ax1.barh(labels[::-1], ind_all.values[::-1], color=colors_palette[::-1], edgecolor="white")
    for bar, val in zip(bars, ind_all.values[::-1]):
        ax1.text(bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                 str(val), va="center", fontsize=10, color=C_DARK)
    ax1.set_title("累计持仓行业分布 (Top 15)", fontsize=14, fontweight="bold")
    ax1.set_xlabel("持仓股票次数")

    # Right: industry concentration over time
    dates = sorted(hold["date"].unique())
    sample_dates = dates[::max(1, len(dates) // 20)]  # ~20 time points
    top_inds = ind_all.head(8).index.tolist()
    time_data = {}
    for ind in top_inds:
        time_data[ind] = []
    for d in sample_dates:
        day = hold[hold["date"] == d]
        total = len(day)
        for ind in top_inds:
            pct = (day["industry_l2"] == ind).sum() / max(total, 1) * 100
            time_data[ind].append(pct)

    bottom = np.zeros(len(sample_dates))
    for i, ind in enumerate(top_inds):
        vals = np.array(time_data[ind])
        ax2.bar(range(len(sample_dates)), vals, bottom=bottom,
                color=colors_palette[i], label=ind[:8], width=0.9)
        bottom += vals
    ax2.set_xticks(range(0, len(sample_dates), max(1, len(sample_dates) // 6)))
    ax2.set_xticklabels([str(sample_dates[i].date())[:7] for i in range(0, len(sample_dates), max(1, len(sample_dates) // 6))],
                         rotation=30, fontsize=9)
    ax2.set_title("行业集中度随时间变化 (Top 8)", fontsize=14, fontweight="bold")
    ax2.set_ylabel("占比 (%)")
    ax2.legend(fontsize=9, loc="upper right", ncol=2)

    _add_watermark(fig)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "8_industry.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  8. Industry done", flush=True)


def main():
    t0 = time.time()
    panel, m, ret, hold, risk_log = run_backtest()

    print(f"\nGenerating charts -> {OUT_DIR}", flush=True)
    chart0_dashboard(m, ret)
    chart1_equity_deep(ret)
    chart5_ic_heatmap(panel)
    chart6_regime_analysis(ret)
    chart7_holdings_table(hold, ret)
    chart8_industry(hold)

    # Save data
    ret.to_csv(os.path.join(OUT_DIR, "backtest_returns.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame([{"k": k, "v": f"{v:.6f}" if isinstance(v, float) else str(v)} for k, v in m.items()]).to_csv(
        os.path.join(OUT_DIR, "metrics_summary.csv"), index=False, encoding="utf-8-sig")

    print(f"\nDone in {time.time()-t0:.0f}s")
    print(f"Output: {OUT_DIR}")


if __name__ == "__main__":
    main()
