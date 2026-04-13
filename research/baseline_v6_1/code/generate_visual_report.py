"""
Generate Visual Report for Expert Review
=========================================
Produces 8 core visualizations from a full backtest run:

1. Equity curve vs HS300
2. Annual return bar chart
3. Drawdown chart
4. Rolling 12-period Sharpe
5. Factor IC heatmap by regime
6. Regime distribution + per-regime return
7. Holdings detail (recent 5 rebalances)
8. Industry concentration per period

Output: research/baseline_v6_1/report/visual/

Run: python research/baseline_v6_1/code/generate_visual_report.py
"""

import os
import sys
import time
import sqlite3

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

OUT_DIR = os.path.join(ROOT, "research", "baseline_v6_1", "report", "visual")
os.makedirs(OUT_DIR, exist_ok=True)

# Chinese font
plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "DejaVu Sans"]
plt.rcParams["axes.unicode_minus"] = False


def run_backtest():
    """Run production config backtest, return all data needed for charts."""
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

    # Merge northbound
    nf_path = os.path.join(ROOT, "data", "market_cache", "northbound_daily.csv")
    if os.path.exists(nf_path):
        nf = pd.read_csv(nf_path, encoding="utf-8-sig")
        nf["date"] = pd.to_datetime(nf["date"]).dt.normalize()
        panel = panel.merge(
            nf[["date", "stock_symbol", "north_hold_chg_5d"]],
            on=["date", "stock_symbol"], how="left",
        )
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


def chart1_equity_curve(ret):
    """1. Equity curve vs HS300."""
    fig, ax = plt.subplots(figsize=(14, 6))
    ret = ret.sort_values("date").copy()
    spread = (ret["Top30_net"] - ret["Bottom30"]).fillna(0)
    equity = (1 + spread).cumprod()

    hs = pd.read_csv(os.path.join(ROOT, "data", "market_cache", "hs300_daily_cache.csv"), encoding="utf-8-sig")
    hs["date"] = pd.to_datetime(hs["date"]).dt.normalize()
    hs["close"] = pd.to_numeric(hs["close"], errors="coerce")
    hs = hs[(hs["date"] >= ret["date"].min()) & (hs["date"] <= ret["date"].max())]
    hs_equity = hs["close"] / hs["close"].iloc[0]

    ax.plot(ret["date"], equity, label=f"策略净值 (Calmar={equity.iloc[-1]:.2f}x)", linewidth=2, color="#2196F3")
    ax.plot(hs["date"], hs_equity, label="沪深300", linewidth=1.2, color="#9E9E9E", alpha=0.7)
    ax.fill_between(ret["date"], 1, equity, alpha=0.1, color="#2196F3")
    ax.set_title("策略净值 vs 沪深300 (2010-2025)", fontsize=16)
    ax.set_ylabel("累计净值")
    ax.legend(fontsize=12)
    ax.grid(True, alpha=0.3)
    ax.set_xlim(ret["date"].min(), ret["date"].max())
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "1_equity_curve.png"), dpi=150)
    plt.close(fig)
    print("  1. Equity curve ✓", flush=True)


def chart2_annual_returns(ret):
    """2. Annual return bar chart."""
    ret = ret.sort_values("date").copy()
    ret["year"] = pd.to_datetime(ret["date"]).dt.year
    spread = (ret["Top30_net"] - ret["Bottom30"]).fillna(0)

    yearly = []
    for yr, g in ret.groupby("year"):
        sp = (g["Top30_net"] - g["Bottom30"]).fillna(0)
        cum = (1 + sp).prod() - 1
        yearly.append({"year": yr, "return": cum})
    ydf = pd.DataFrame(yearly)

    fig, ax = plt.subplots(figsize=(14, 6))
    colors = ["#4CAF50" if r >= 0 else "#F44336" for r in ydf["return"]]
    bars = ax.bar(ydf["year"], ydf["return"] * 100, color=colors, edgecolor="white", linewidth=0.5)
    for bar, val in zip(bars, ydf["return"]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f"{val*100:.1f}%", ha="center", va="bottom", fontsize=9)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_title("年度收益率 (2010-2025)", fontsize=16)
    ax.set_ylabel("收益率 (%)")
    ax.set_xlabel("年份")
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "2_annual_returns.png"), dpi=150)
    plt.close(fig)
    print("  2. Annual returns ✓", flush=True)


def chart3_drawdown(ret):
    """3. Drawdown chart."""
    ret = ret.sort_values("date").copy()
    spread = (ret["Top30_net"] - ret["Bottom30"]).fillna(0)
    equity = (1 + spread).cumprod()
    peak = equity.cummax()
    dd = (equity / peak - 1) * 100

    fig, ax = plt.subplots(figsize=(14, 4))
    ax.fill_between(ret["date"], dd, 0, color="#F44336", alpha=0.4)
    ax.plot(ret["date"], dd, color="#F44336", linewidth=0.8)
    ax.set_title(f"回撤曲线 (最大回撤: {dd.min():.1f}%)", fontsize=16)
    ax.set_ylabel("回撤 (%)")
    ax.grid(True, alpha=0.3)
    ax.set_xlim(ret["date"].min(), ret["date"].max())
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "3_drawdown.png"), dpi=150)
    plt.close(fig)
    print("  3. Drawdown ✓", flush=True)


def chart4_rolling_sharpe(ret):
    """4. Rolling 12-period Sharpe."""
    ret = ret.sort_values("date").copy()
    spread = (ret["Top30_net"] - ret["Bottom30"]).fillna(0)
    rolling_mean = spread.rolling(12, min_periods=6).mean()
    rolling_std = spread.rolling(12, min_periods=6).std()
    rolling_sharpe = (rolling_mean / rolling_std.replace(0, np.nan)) * np.sqrt(252 / 12)

    fig, ax = plt.subplots(figsize=(14, 5))
    ax.plot(ret["date"], rolling_sharpe, color="#FF9800", linewidth=1.5)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.axhline(1.0, color="#4CAF50", linewidth=0.8, linestyle="--", alpha=0.5, label="Sharpe=1.0")
    ax.fill_between(ret["date"], 0, rolling_sharpe,
                    where=rolling_sharpe > 0, color="#4CAF50", alpha=0.15)
    ax.fill_between(ret["date"], 0, rolling_sharpe,
                    where=rolling_sharpe < 0, color="#F44336", alpha=0.15)
    ax.set_title("滚动 Sharpe (12 期窗口, 年化)", fontsize=16)
    ax.set_ylabel("Sharpe Ratio")
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_xlim(ret["date"].min(), ret["date"].max())
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "4_rolling_sharpe.png"), dpi=150)
    plt.close(fig)
    print("  4. Rolling Sharpe ✓", flush=True)


def chart5_ic_heatmap(panel):
    """5. Factor IC heatmap by regime."""
    factor_cols = {
        "factor_z_neu": "雪球共识",
        "vol_price_div5d": "量价背离",
        "ret_intra5d": "日内反转(-)",
        "ret20d_stock": "20日动量",
        "hv20_hv60_ratio": "HV比率",
        "highconv_10d": "高conviction",
        "north_hold_chg_5d": "北向持仓变化",
    }

    ic_matrix = []
    for col, label in factor_cols.items():
        if col not in panel.columns:
            continue
        row = {"factor": label}
        # Invert for display if needed
        mult = -1.0 if col == "ret_intra5d" else 1.0
        for regime in ["上涨", "震荡", "下跌", "全样本"]:
            sub = panel if regime == "全样本" else panel[panel["regime"] == regime]
            sub = sub.dropna(subset=[col, "fwd_ret_2w"])
            if len(sub) < 50:
                row[regime] = np.nan
                continue
            vals = sub[col] * mult if mult != 1.0 else sub[col]
            ics = sub.assign(_f=vals).groupby("date").apply(
                lambda g: g["_f"].corr(g["fwd_ret_2w"]) if len(g) >= 5 else np.nan
            ).dropna()
            row[regime] = ics.mean() if len(ics) > 0 else np.nan
        ic_matrix.append(row)

    df = pd.DataFrame(ic_matrix).set_index("factor")
    fig, ax = plt.subplots(figsize=(10, 6))
    im = ax.imshow(df.values, cmap="RdYlGn", aspect="auto", vmin=-0.02, vmax=0.03)
    ax.set_xticks(range(len(df.columns)))
    ax.set_xticklabels(df.columns, fontsize=12)
    ax.set_yticks(range(len(df.index)))
    ax.set_yticklabels(df.index, fontsize=11)
    for i in range(len(df.index)):
        for j in range(len(df.columns)):
            v = df.values[i, j]
            if pd.notna(v):
                ax.text(j, i, f"{v:.4f}", ha="center", va="center", fontsize=10,
                        color="white" if abs(v) > 0.015 else "black")
    plt.colorbar(im, ax=ax, label="IC")
    ax.set_title("因子 IC 热力图 (按市况)", fontsize=16)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "5_ic_heatmap.png"), dpi=150)
    plt.close(fig)
    print("  5. IC heatmap ✓", flush=True)


def chart6_regime_returns(ret):
    """6. Regime distribution + per-regime return."""
    ret = ret.sort_values("date").copy()
    spread = (ret["Top30_net"] - ret["Bottom30"]).fillna(0)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Left: regime distribution
    regime_counts = ret["regime"].value_counts()
    colors_map = {"上涨": "#F44336", "震荡": "#FF9800", "下跌": "#4CAF50"}
    colors = [colors_map.get(r, "#9E9E9E") for r in regime_counts.index]
    ax1.pie(regime_counts, labels=[f"{r}\n({c}期)" for r, c in zip(regime_counts.index, regime_counts.values)],
            colors=colors, autopct="%1.1f%%", textprops={"fontsize": 12})
    ax1.set_title("市况分布 (调仓期数)", fontsize=14)

    # Right: per-regime mean return
    regime_ret = {}
    for regime in ["上涨", "震荡", "下跌"]:
        mask = ret["regime"] == regime
        if mask.any():
            regime_ret[regime] = spread[mask].mean() * 100
    rdf = pd.DataFrame({"regime": list(regime_ret.keys()), "mean_ret": list(regime_ret.values())})
    bar_colors = [colors_map.get(r, "#9E9E9E") for r in rdf["regime"]]
    bars = ax2.bar(rdf["regime"], rdf["mean_ret"], color=bar_colors, edgecolor="white")
    for bar, val in zip(bars, rdf["mean_ret"]):
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.02,
                 f"{val:.3f}%", ha="center", va="bottom", fontsize=11)
    ax2.axhline(0, color="black", linewidth=0.8)
    ax2.set_title("各市况平均期收益", fontsize=14)
    ax2.set_ylabel("平均收益 (%)")
    ax2.grid(True, alpha=0.3, axis="y")

    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "6_regime_returns.png"), dpi=150)
    plt.close(fig)
    print("  6. Regime returns ✓", flush=True)


def chart7_holdings_detail(hold, ret):
    """7. Holdings detail — recent 5 rebalances."""
    if hold.empty:
        print("  7. Holdings detail — SKIPPED (empty)", flush=True)
        return
    hold = hold.copy()
    hold["date"] = pd.to_datetime(hold["date"]).dt.normalize()
    dates = sorted(hold["date"].unique())
    recent = dates[-5:] if len(dates) >= 5 else dates

    rows = []
    for d in recent:
        day_hold = hold[hold["date"] == d].copy()
        regime = ret[ret["date"] == d]["regime"].iloc[0] if d in ret["date"].values else "?"
        for _, h in day_hold.iterrows():
            rows.append({
                "调仓日": str(d.date()),
                "市况": regime,
                "股票": h.get("stock_symbol", ""),
                "行业": h.get("industry_l2", ""),
                "权重": f"{h.get('weight', 0):.1%}" if pd.notna(h.get("weight")) else "",
            })

    df = pd.DataFrame(rows)
    # Save as CSV for the analyst
    df.to_csv(os.path.join(OUT_DIR, "7_holdings_detail.csv"), index=False, encoding="utf-8-sig")

    # Also make a summary table image
    summary = df.groupby("调仓日").agg(
        市况=("市况", "first"),
        持仓数=("股票", "count"),
        行业数=("行业", "nunique"),
    ).reset_index()

    fig, ax = plt.subplots(figsize=(10, 3))
    ax.axis("off")
    table = ax.table(cellText=summary.values, colLabels=summary.columns, loc="center", cellLoc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1.2, 1.5)
    ax.set_title("最近 5 次调仓概览", fontsize=14, pad=20)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "7_holdings_summary.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print("  7. Holdings detail ✓", flush=True)


def chart8_industry_concentration(hold):
    """8. Industry concentration per period."""
    if hold.empty or "industry_l2" not in hold.columns:
        print("  8. Industry concentration — SKIPPED", flush=True)
        return
    hold = hold.copy()
    hold["date"] = pd.to_datetime(hold["date"]).dt.normalize()
    dates = sorted(hold["date"].unique())
    recent = dates[-6:] if len(dates) >= 6 else dates

    fig, axes = plt.subplots(2, 3, figsize=(16, 10))
    axes = axes.flatten()

    for i, d in enumerate(recent):
        if i >= 6:
            break
        ax = axes[i]
        day = hold[hold["date"] == d]
        ind_counts = day["industry_l2"].value_counts().head(8)
        if ind_counts.empty:
            ax.set_title(str(d.date()))
            continue
        # Shorten industry names
        labels = [n[:8] if len(n) > 8 else n for n in ind_counts.index]
        ax.barh(labels, ind_counts.values, color="#2196F3", edgecolor="white")
        ax.set_title(f"{d.date()} ({len(day)}只)", fontsize=11)
        ax.invert_yaxis()

    for j in range(i + 1, 6):
        axes[j].axis("off")

    fig.suptitle("最近 6 次调仓行业分布 (Top 8)", fontsize=16)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "8_industry_concentration.png"), dpi=150)
    plt.close(fig)
    print("  8. Industry concentration ✓", flush=True)


def main():
    t0 = time.time()
    panel, m, ret, hold, risk_log = run_backtest()

    print(f"\nGenerating charts → {OUT_DIR}", flush=True)
    chart1_equity_curve(ret)
    chart2_annual_returns(ret)
    chart3_drawdown(ret)
    chart4_rolling_sharpe(ret)
    chart5_ic_heatmap(panel)
    chart6_regime_returns(ret)
    chart7_holdings_detail(hold, ret)
    chart8_industry_concentration(hold)

    # Save key metrics summary
    metrics_summary = pd.DataFrame([{
        "指标": k, "值": f"{v:.4f}" if isinstance(v, float) else str(v)
    } for k, v in m.items()])
    metrics_summary.to_csv(os.path.join(OUT_DIR, "metrics_summary.csv"), index=False, encoding="utf-8-sig")

    # Save return series
    ret.to_csv(os.path.join(OUT_DIR, "backtest_returns.csv"), index=False, encoding="utf-8-sig")

    print(f"\nDone in {time.time()-t0:.0f}s")
    print(f"All outputs → {OUT_DIR}")
    print(f"  8 charts (.png)")
    print(f"  1 holdings detail (.csv)")
    print(f"  1 metrics summary (.csv)")
    print(f"  1 return series (.csv)")


if __name__ == "__main__":
    main()
