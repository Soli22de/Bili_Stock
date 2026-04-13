"""Deep QC audit: go-flat overfitting + liquidity + cost sensitivity."""
import sys, os, time
import numpy as np, pandas as pd

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, ROOT)

from research.baseline_v6_1.code.run_baseline_v6_v61_suite import _enrich_from_stock_data, _run_one
from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5

panel = _prepare_panel_v5()
panel = panel[(panel["date"] >= pd.Timestamp("2010-01-01")) & (panel["date"] <= pd.Timestamp("2025-12-31"))].copy()
panel, px_map = _enrich_from_stock_data(panel)
RISK = dict(non_up_vol_q=0.50, dd_soft=-0.05, dd_mid=-0.07, dd_hard=-0.10,
            choppy_loss_scale=0.0, choppy_loss_floor=0.0,
            use_srf=False, use_srf_v2=True, top_k=15, go_flat_choppy=False)
m, ret, hold, _, _, risk_log = _run_one(
    panel, px_map, hold_step=12, liq_other=0.60, cap_non_up=0.10, cap_up=0.20,
    with_takeprofit=True, risk_cfg=RISK)

ret = ret.sort_values("date").copy()
top = ret["Top30_net"].fillna(0)
top_raw = ret["Top30"].fillna(0)

out_path = os.path.join(ROOT, "research", "baseline_v6_1", "report", "visual", "_qc_deep.txt")
with open(out_path, "w", encoding="utf-8") as f:

    # ═══ QC-1: Go-flat across all crash periods ═══
    f.write("=" * 70 + "\n")
    f.write("QC-1: Go-flat 在历史重大下跌期的表现\n")
    f.write("=" * 70 + "\n\n")

    crashes = [
        ("2015股灾(6-9月)", "2015-06-01", "2015-09-30"),
        ("2018全年熊市", "2018-01-01", "2018-12-31"),
        ("2020疫情(1-3月)", "2020-01-15", "2020-03-31"),
        ("2022年4月", "2022-03-15", "2022-05-15"),
        ("2022年10月", "2022-09-15", "2022-11-15"),
        ("2024年1-2月", "2024-01-01", "2024-02-29"),
    ]

    header = f"{'期间':<20s} {'期数':>4s} {'flat期':>5s} {'原始Top30':>10s} {'风控后':>10s} {'保护':>8s}"
    f.write(header + "\n")
    f.write("-" * 65 + "\n")

    for label, start, end in crashes:
        mask = (pd.to_datetime(ret["date"]) >= start) & (pd.to_datetime(ret["date"]) <= end)
        if not mask.any():
            f.write(f"{label:<20s} 无数据\n")
            continue
        raw = top_raw[mask]
        net = top[mask]
        n = len(raw)
        n_flat = (net == 0).sum()
        raw_cum = (1 + raw).prod() - 1
        net_cum = (1 + net).prod() - 1
        f.write(f"{label:<20s} {n:>4d} {n_flat:>5d} {raw_cum*100:>9.2f}% {net_cum*100:>9.2f}% {(net_cum-raw_cum)*100:>+7.2f}%\n")

    f.write("\n全样本 go-flat 命中统计:\n")
    down = top_raw < -0.01  # meaningful loss (>1%)
    down_caught = (top[down] == 0).sum()
    f.write(f"  原始亏损>1%的期数: {down.sum()}\n")
    f.write(f"  被go-flat避开: {down_caught} ({down_caught/max(down.sum(),1)*100:.1f}%)\n")
    up = top_raw > 0.01
    up_killed = (top[up] == 0).sum()
    f.write(f"  原始盈利>1%的期数: {up.sum()}\n")
    f.write(f"  被go-flat误杀: {up_killed} ({up_killed/max(up.sum(),1)*100:.1f}%)\n")

    # Check: is go-flat just coincidentally aligned with crashes?
    # If regime=震荡 was defined differently, would it still work?
    f.write("\ngo-flat 触发条件验证:\n")
    for i, r in ret.iterrows():
        pass  # just to have ret available
    regime_dist = ret["regime"].value_counts()
    f.write(f"  上涨期: {regime_dist.get('上涨', 0)}\n")
    f.write(f"  震荡期: {regime_dist.get('震荡', 0)}\n")
    f.write(f"  下跌期: {regime_dist.get('下跌', 0)}\n")
    choppy = ret[ret["regime"] == "震荡"]
    choppy_raw = top_raw[ret["regime"] == "震荡"]
    f.write(f"  震荡期原始Top30均值: {choppy_raw.mean()*100:.3f}%/期\n")
    f.write(f"  震荡期原始Top30正收益比: {(choppy_raw>0).mean()*100:.1f}%\n")

    # ═══ QC-2: Liquidity ═══
    f.write("\n" + "=" * 70 + "\n")
    f.write("QC-2: 持仓股票流动性和资金容量\n")
    f.write("=" * 70 + "\n\n")

    if not hold.empty:
        hold_c = hold.copy()
        hold_c["date"] = pd.to_datetime(hold_c["date"]).dt.normalize()
        hold_c["stock_symbol"] = hold_c["stock_symbol"].str.upper()

        # Get liquidity from file
        liq_path = os.path.join(ROOT, "research", "baseline_v1", "data_delivery", "liquidity_daily_v1.csv")
        if os.path.exists(liq_path):
            liq = pd.read_csv(liq_path, encoding="utf-8-sig", usecols=["date", "stock_symbol", "amount"])
            liq["date"] = pd.to_datetime(liq["date"]).dt.normalize()
            liq["stock_symbol"] = liq["stock_symbol"].str.upper()
            hold_c = hold_c.merge(liq, on=["date", "stock_symbol"], how="left")

        if "amount" not in hold_c.columns or hold_c["amount"].notna().sum() == 0:
            f.write("  无成交额数据，跳过流动性分析\n")
        else:
            f.write(f"持仓记录: {len(hold_c)}, 有成交额: {hold_c['amount'].notna().sum()} ({hold_c['amount'].notna().mean()*100:.1f}%)\n\n")
            avg_amt = hold_c.groupby("stock_symbol")["amount"].mean().dropna()
            f.write("持仓股票日均成交额分布:\n")
            for p in [10, 25, 50, 75, 90]:
                f.write(f"  p{p}: {avg_amt.quantile(p/100)/1e8:.2f} 亿\n")
            f.write(f"  均值: {avg_amt.mean()/1e8:.2f} 亿\n\n")
            low_pct = (avg_amt < 5e7).mean() * 100
            very_low_pct = (avg_amt < 1e7).mean() * 100
            f.write(f"日均成交额<5000万的股票占比: {low_pct:.1f}%\n")
            f.write(f"日均成交额<1000万的股票占比: {very_low_pct:.1f}%\n\n")
            f.write("资金容量估算 (买入不超过日成交额5%):\n")
            for fund in [100, 500, 1000, 5000]:
                per_stock = fund / 15
                need = per_stock * 1e4 / 0.05
                feasible = (avg_amt > need).mean() * 100
                f.write(f"  {fund:>5d}万: 每股{per_stock:.0f}万, 需日成交{need/1e8:.2f}亿, {feasible:.0f}%可执行\n")
            f.write("\n各年持仓股票成交额中位数:\n")
            hold_c["year"] = hold_c["date"].dt.year
            for yr, g in hold_c.groupby("year"):
                med = g["amount"].median()
                if pd.notna(med):
                    f.write(f"  {yr}: {med/1e8:.2f} 亿\n")

    # ═══ QC-3: Cost sensitivity ═══
    f.write("\n" + "=" * 70 + "\n")
    f.write("QC-3: 交易成本敏感性\n")
    f.write("=" * 70 + "\n\n")

    avg_to = ret["one_way_turnover"].mean() if "one_way_turnover" in ret.columns else 0.83
    f.write(f"平均单边换手率: {avg_to*100:.1f}%\n")
    f.write(f"回测单边成本: 10bp (0.1%)\n")
    f.write(f"实际A股成本: 佣金3bp + 印花税10bp + 过户费0.2bp + 滑点10-30bp = 23-43bp\n\n")

    f.write(f"{'单边成本':>10s} {'年化收益':>10s} {'MDD':>8s} {'Calmar':>8s} {'收益变化':>10s}\n")
    f.write("-" * 50 + "\n")

    base = ret["Top30"].fillna(0)
    to = ret.get("one_way_turnover", pd.Series(avg_to, index=ret.index)).fillna(avg_to)
    rs = ret.get("risk_scale", pd.Series(1.0, index=ret.index)).fillna(1.0)

    base_ann = None
    for cost_bp in [0, 10, 25, 40, 50, 80, 100]:
        cost = cost_bp / 10000.0
        net_ret = (base - to * cost) * rs
        eq = (1 + net_ret).cumprod()
        ann = float((1 + net_ret.mean()) ** 21 - 1)
        mdd = float((eq / eq.cummax() - 1).min())
        calmar = ann / abs(mdd) if mdd != 0 else np.nan
        if base_ann is None:
            base_ann = ann
        delta = ann - base_ann
        f.write(f"{cost_bp:>7d}bp {ann*100:>9.2f}% {mdd*100:>7.1f}% {calmar:>7.3f} {delta*100:>+9.1f}pp\n")

    f.write("\n结论:\n")
    f.write("  实际A股成本约40bp单边时, 看上面的40bp行\n")
    f.write("  如果年化从31%降到X%, Calmar从1.35降到Y\n")

print(f"QC written to {out_path}")
