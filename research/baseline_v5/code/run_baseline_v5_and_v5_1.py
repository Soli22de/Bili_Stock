import glob
import os
import sys

import numpy as np
import pandas as pd


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v4.code.run_baseline_v4_2_up_filter import (
    _apply_liq_dynamic,
    _apply_up_exposure,
    _assign_other_industry_by_proxy,
    _attach_base_fields,
    _build_group_ret_v42,
    _industry_neutralize,
    _load_hs300,
)
from research.data_prep.build_rebalance_momentum_panel import build_rebalance_momentum_panel


def _load_tradability_from_stock_data(root: str) -> pd.DataFrame:
    rows = []
    files = glob.glob(os.path.join(root, "data", "stock_data", "*.csv"))
    for fp in files:
        sym = os.path.splitext(os.path.basename(fp))[0].upper()
        try:
            df = pd.read_csv(fp, usecols=["日期", "涨跌幅", "成交量"])
        except Exception:
            continue
        df.rename(columns={"日期": "date", "涨跌幅": "pctChg", "成交量": "volume"}, inplace=True)
        df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
        df["pctChg"] = pd.to_numeric(df["pctChg"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
        df["stock_symbol"] = sym
        df = df.dropna(subset=["date"])
        rows.append(df[["date", "stock_symbol", "pctChg", "volume"]])
    if not rows:
        return pd.DataFrame(columns=["date", "stock_symbol", "is_suspended", "is_limit"])
    x = pd.concat(rows, ignore_index=True)
    code = x["stock_symbol"].str[-6:]
    is_20 = code.str.startswith(("30", "68"))
    limit_abs = np.where(is_20, 19.6, 9.6)
    x["is_suspended"] = x["volume"].fillna(0) <= 0
    x["is_limit"] = x["pctChg"].abs() >= limit_abs
    return x[["date", "stock_symbol", "is_suspended", "is_limit"]]


def _calc_metrics(group_ret: pd.DataFrame) -> dict:
    g = group_ret.sort_values("date").copy()
    ls = g["Top30"] - g["Bottom30"]
    curve = (1 + ls.fillna(0)).cumprod()
    peak = curve.cummax()
    dd = curve / peak - 1.0
    mdd = float(dd.min()) if not dd.empty else float("nan")
    calmar = float(ls.mean()) / abs(mdd) if (not pd.isna(mdd) and mdd != 0) else float("nan")
    out = {"calmar_ratio": calmar, "max_drawdown_ls_curve": mdd, "top_bottom": float(ls.mean())}
    for rg in ["上涨", "震荡", "下跌"]:
        d = g[g["regime"] == rg]
        out[f"{rg}_top_bottom"] = float((d["Top30"] - d["Bottom30"]).mean()) if not d.empty else float("nan")
    return out


def _load_hs300_panic(start_date: str, end_date: str) -> pd.DataFrame:
    hs = _load_hs300(start_date, end_date)
    hs = hs.rename(columns={"regime": "market_regime"})
    idx = hs[["date"]].copy()
    idx2 = _load_hs300(start_date, end_date)
    idx = idx.merge(idx2[["date"]], on="date", how="left")
    idx = idx.drop_duplicates("date").sort_values("date")
    hs300 = _load_hs300(start_date, end_date)
    hs300 = hs300.sort_values("date")
    hs_px = hs300.copy()
    hs_px["dummy"] = 0
    hs_raw = _load_hs300(start_date, end_date)
    hs_raw = hs_raw.rename(columns={"regime": "regime_tmp"})
    m = hs_raw.merge(hs_px[["date"]], on="date", how="left")
    m = m.sort_values("date")
    return m


def _load_hs300_vol(start_date: str, end_date: str) -> pd.DataFrame:
    import baostock as bs

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
    idx = pd.DataFrame(rows, columns=["date", "close"])
    idx["date"] = pd.to_datetime(idx["date"], errors="coerce").dt.normalize()
    idx["close"] = pd.to_numeric(idx["close"], errors="coerce")
    idx = idx.dropna(subset=["date", "close"]).sort_values("date")
    idx["ret1"] = idx["close"].pct_change()
    idx["panic_vol20"] = idx["ret1"].rolling(20, min_periods=20).std(ddof=0) * np.sqrt(252)
    idx["panic_scale"] = 1.0
    idx.loc[idx["panic_vol20"] > 0.20, "panic_scale"] = 0.7
    idx.loc[idx["panic_vol20"] < 0.10, "panic_scale"] = 0.8
    return idx[["date", "panic_vol20", "panic_scale"]]


def main():
    out_dir = os.path.join(ROOT, "research", "baseline_v4_2", "output")
    os.makedirs(out_dir, exist_ok=True)
    panel = build_rebalance_momentum_panel(
        db_path=os.path.join(ROOT, "data", "cubes.db"),
        cache_dir=os.path.join(ROOT, "data", "market_cache"),
        out_csv=os.path.join(out_dir, "factor_panel_rebalance_momentum_2019_2025.csv"),
        start_date="2019-01-01",
        end_date="2025-12-31",
        lag_days=14,
        smoothing_days=3,
        factor_mode="rate",
    )
    base = _attach_base_fields(
        panel,
        industry_map_csv=os.path.join(ROOT, "research", "baseline_v1", "data_delivery", "industry_mapping_v2.csv"),
        liquidity_csv=os.path.join(ROOT, "research", "baseline_v1", "data_delivery", "liquidity_daily_v1.csv"),
    )
    base = _assign_other_industry_by_proxy(base)
    base["factor_z_raw"] = base["factor_z"]
    base = _industry_neutralize(base, source_col="factor_z_raw", out_col="factor_z_neu")
    regime = _load_hs300("2019-01-01", "2025-12-31")
    panel_liq = _apply_liq_dynamic(base, regime_df=regime, keep_other=0.6, keep_up=0.2)
    trad = _load_tradability_from_stock_data(ROOT)
    panel_v5 = panel_liq.merge(trad, on=["date", "stock_symbol"], how="left")
    panel_v5["is_suspended"] = panel_v5["is_suspended"].fillna(False)
    panel_v5["is_limit"] = panel_v5["is_limit"].fillna(False)
    panel_v5 = panel_v5[~panel_v5["is_suspended"] & ~panel_v5["is_limit"]].copy()
    group_v5 = _build_group_ret_v42(panel_v5, trim_q=0.05, hold_step=10)
    group_v5 = _apply_up_exposure(group_v5, up_scale=0.5)
    m_v5 = _calc_metrics(group_v5)
    panic = _load_hs300_vol("2019-01-01", "2025-12-31")
    group_v51 = group_v5.merge(panic[["date", "panic_scale"]], on="date", how="left")
    group_v51["panic_scale"] = group_v51["panic_scale"].fillna(1.0)
    for c in ["Bottom30", "Middle40", "Top30"]:
        group_v51[c] = group_v51[c] * group_v51["panic_scale"]
    m_v51 = _calc_metrics(group_v51)
    pd.DataFrame(
        {"metric": list(m_v5.keys()), "baseline_v5": list(m_v5.values()), "baseline_v5_1": [m_v51[k] for k in m_v5.keys()]}
    ).to_csv(os.path.join(out_dir, "core_metrics_baseline_v5_vs_v5_1_2019_2025.csv"), index=False, encoding="utf-8-sig")
    group_v5[["date", "Bottom30", "Middle40", "Top30"]].to_csv(
        os.path.join(out_dir, "group_ret_baseline_v5_2w_2019_2025.csv"), index=False, encoding="utf-8-sig"
    )
    group_v51[["date", "Bottom30", "Middle40", "Top30"]].to_csv(
        os.path.join(out_dir, "group_ret_baseline_v5_1_2w_2019_2025.csv"), index=False, encoding="utf-8-sig"
    )
    with open(os.path.join(out_dir, "baseline_v5_and_v5_1_report.md"), "w", encoding="utf-8") as f:
        f.write("# baseline_v5 与 baseline_v5.1 全样本复核（2019-2025）\n\n")
        f.write("- baseline_v5：加入涨跌停/停牌过滤。\n")
        f.write("- baseline_v5.1：在 v5 上加入恐慌指数仓位管理。\n\n")
        f.write(f"- v5 calmar: {m_v5['calmar_ratio']:.6f}, mdd: {m_v5['max_drawdown_ls_curve']:.6f}, up_tb: {m_v5['上涨_top_bottom']:.6f}\n")
        f.write(f"- v5.1 calmar: {m_v51['calmar_ratio']:.6f}, mdd: {m_v51['max_drawdown_ls_curve']:.6f}, up_tb: {m_v51['上涨_top_bottom']:.6f}\n")
    print(f"v5_calmar={m_v5['calmar_ratio']:.6f}")
    print(f"v5_mdd={m_v5['max_drawdown_ls_curve']:.6f}")
    print(f"v5_1_calmar={m_v51['calmar_ratio']:.6f}")
    print(f"v5_1_mdd={m_v51['max_drawdown_ls_curve']:.6f}")


if __name__ == "__main__":
    main()
