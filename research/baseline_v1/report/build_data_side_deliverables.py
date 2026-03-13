import glob
import os
import sqlite3

import baostock as bs
import pandas as pd


def _extract_code(symbol: str) -> str:
    s = str(symbol or "").upper()
    digits = "".join([c for c in s if c.isdigit()])
    if not digits:
        return ""
    return digits[-6:].zfill(6)


def _normalize_symbol(code: str) -> str:
    c = str(code or "").zfill(6)
    if c.startswith(("60", "68", "90")):
        return f"SH{c}"
    return f"SZ{c}"


def _classify(name: str):
    n = str(name or "")
    rules = [
        (["银行"], ("金融", "银行")),
        (["证券", "股份", "信托", "保险"], ("金融", "非银金融")),
        (["医药", "生物", "制药", "医疗"], ("医药", "医药生物")),
        (["半导体", "电子", "芯片", "光电"], ("科技", "电子")),
        (["软件", "信息", "数码", "通信", "网络", "科技"], ("科技", "计算机通信")),
        (["汽车", "电池", "新能源"], ("制造", "汽车新能源")),
        (["煤", "石油", "天然气", "电力", "能源"], ("周期", "能源公用")),
        (["有色", "钢", "金属", "材料"], ("周期", "材料")),
        (["地产", "置业", "城建"], ("地产", "房地产")),
        (["食品", "饮料", "酒"], ("消费", "食品饮料")),
        (["机场", "港", "物流", "航运", "运输"], ("交运", "交通运输")),
        (["农业", "农", "牧", "种业"], ("农业", "农林牧渔")),
        (["建筑", "建材", "工程", "装饰"], ("建筑", "建筑建材")),
        (["家电", "电器", "厨卫"], ("消费", "家用电器")),
        (["纺织", "服饰", "家纺"], ("消费", "纺织服装")),
        (["百货", "商贸", "零售", "超市"], ("消费", "商贸零售")),
        (["军工", "航天", "船舶", "兵器"], ("制造", "国防军工")),
        (["化工", "化学"], ("周期", "基础化工")),
        (["环保", "环境"], ("公用", "环保")),
        (["传媒", "影视", "出版", "文化"], ("传媒", "传媒")),
        (["重工"], ("制造", "国防军工")),
        (["商管"], ("地产", "房地产")),
        (["汇丰"], ("金融", "银行")),
        (["高鸿"], ("科技", "计算机通信")),
        (["爱康"], ("制造", "汽车新能源")),
        (["丹邦"], ("科技", "电子")),
        (["神雾"], ("公用", "环保")),
    ]
    for keys, val in rules:
        if any(k in n for k in keys):
            return val
    return ("其他", "其他")


def _build_authoritative_industry_map():
    lg = bs.login()
    if str(lg.error_code) != "0":
        raise RuntimeError(f"baostock login failed: {lg.error_msg}")
    rs = bs.query_stock_industry()
    if str(rs.error_code) != "0":
        bs.logout()
        raise RuntimeError(f"query_stock_industry failed: {rs.error_msg}")
    rows = []
    while rs.error_code == "0" and rs.next():
        d = rs.get_row_data()
        if len(d) < 5:
            continue
        _, code, name, industry, _ = d
        stock_code = str(code).split(".")[-1].zfill(6)
        ind = str(industry)
        ind_l1 = ind[:3] if len(ind) >= 3 else ""
        ind_l2 = ind[3:] if len(ind) > 3 else ""
        rows.append([stock_code, name, ind_l1, ind_l2])
    bs.logout()
    if not rows:
        return pd.DataFrame(columns=["stock_code", "stock_name_auth", "industry_l1_auth", "industry_l2_auth"])
    auth = pd.DataFrame(rows, columns=["stock_code", "stock_name_auth", "industry_l1_auth", "industry_l2_auth"])
    auth = auth.dropna(subset=["stock_code"]).copy()
    auth = auth[auth["stock_code"].str.match(r"^\d{6}$", na=False)].copy()
    auth = auth.sort_values(["stock_code", "industry_l2_auth"]).drop_duplicates("stock_code", keep="first")
    return auth


def build_deliverables(root: str):
    out_dir = os.path.join(root, "research", "baseline_v1", "data_delivery")
    os.makedirs(out_dir, exist_ok=True)
    db_path = os.path.join(root, "data", "cubes.db")
    conn = sqlite3.connect(db_path)
    try:
        pool = pd.read_sql_query(
            """
            SELECT stock_symbol, MAX(stock_name) AS stock_name_rb
            FROM rebalancing_history
            WHERE date(created_at) BETWEEN '2019-01-01' AND '2025-12-31'
            GROUP BY stock_symbol
            """,
            conn,
        )
    finally:
        conn.close()
    pool["stock_symbol"] = pool["stock_symbol"].astype(str).str.upper()
    pool["stock_code"] = pool["stock_symbol"].map(_extract_code)
    pool["stock_symbol_norm"] = pool["stock_code"].map(_normalize_symbol)
    pool = pool[pool["stock_code"].str.match(r"^\d{6}$", na=False)].copy()
    pool = pool[
        pool["stock_code"].str.startswith(("00", "30", "60", "68"), na=False)
    ].copy()
    stock_list = pd.read_csv(os.path.join(root, "data", "stock_list.csv"))
    stock_list["stock_code"] = stock_list["code"].astype(str).str.zfill(6)
    stock_list.rename(columns={"name": "stock_name"}, inplace=True)
    auth_map = _build_authoritative_industry_map()
    valid_codes = set(stock_list["stock_code"].astype(str).tolist()) | set(auth_map["stock_code"].astype(str).tolist())
    pool = pool[pool["stock_code"].isin(valid_codes)].copy()
    merged = pool.merge(stock_list[["stock_code", "stock_name"]], on="stock_code", how="left")
    merged = merged.merge(auth_map, on="stock_code", how="left")
    merged["stock_name_final"] = merged["stock_name"].fillna(merged["stock_name_rb"]).fillna(merged["stock_name_auth"])
    cls = merged["stock_name_final"].map(_classify)
    merged["industry_l1_fallback"] = cls.map(lambda x: x[0])
    merged["industry_l2_fallback"] = cls.map(lambda x: x[1])
    merged["industry_l1_auth"] = merged["industry_l1_auth"].replace("", pd.NA)
    merged["industry_l2_auth"] = merged["industry_l2_auth"].replace("", pd.NA)
    merged["industry_l1"] = merged["industry_l1_auth"].fillna(merged["industry_l1_fallback"])
    merged["industry_l2"] = merged["industry_l2_auth"].fillna(merged["industry_l2_fallback"])
    merged["mapping_method"] = "name_keyword_fallback"
    merged.loc[merged["industry_l2_auth"].notna(), "mapping_method"] = "baostock_csrc_industry"
    merged["stock_name"] = merged["stock_name_final"]
    industry = merged[
        ["stock_symbol", "stock_symbol_norm", "stock_code", "stock_name", "industry_l1", "industry_l2", "mapping_method"]
    ].copy()
    industry.rename(columns={"stock_symbol_norm": "stock_symbol_standard"}, inplace=True)
    industry.to_csv(os.path.join(out_dir, "industry_mapping_v2.csv"), index=False, encoding="utf-8-sig")
    unresolved = industry[industry["industry_l2"] == "其他"].copy()
    unresolved.to_csv(os.path.join(out_dir, "industry_mapping_unresolved_v2.csv"), index=False, encoding="utf-8-sig")
    files = glob.glob(os.path.join(root, "data", "stock_data", "*.csv"))
    liq_rows = []
    for fp in files:
        sym = os.path.splitext(os.path.basename(fp))[0].upper()
        try:
            df = pd.read_csv(fp, usecols=["日期", "成交额", "换手率"])
        except Exception:
            continue
        df.rename(columns={"日期": "date", "成交额": "amount", "换手率": "turnover_rate"}, inplace=True)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        df["turnover_rate"] = pd.to_numeric(df["turnover_rate"], errors="coerce")
        df["stock_symbol"] = sym
        df = df.dropna(subset=["date"]).copy()
        df = df[(df["date"] >= "2010-01-01") & (df["date"] <= "2025-12-31")]
        liq_rows.append(df[["date", "stock_symbol", "amount", "turnover_rate"]])
    liquidity = pd.concat(liq_rows, ignore_index=True) if liq_rows else pd.DataFrame(columns=["date", "stock_symbol", "amount", "turnover_rate"])
    liquidity = liquidity[liquidity["stock_symbol"].isin(set(pool["stock_symbol"].tolist()))].copy()
    liquidity.sort_values(["stock_symbol", "date"], inplace=True)
    liquidity.to_csv(os.path.join(out_dir, "liquidity_daily_v1.csv"), index=False, encoding="utf-8-sig")
    industry_cov = pd.DataFrame(
        {
            "metric": [
                "pool_symbols",
                "authoritative_mapped_symbols",
                "authoritative_mapping_rate",
                "other_symbols",
                "other_ratio",
            ],
            "value": [
                float(pool["stock_symbol"].nunique()),
                float((industry["mapping_method"] == "baostock_csrc_industry").sum()),
                float((industry["mapping_method"] == "baostock_csrc_industry").mean()),
                float((industry["industry_l2"] == "其他").sum()),
                float((industry["industry_l2"] == "其他").mean()),
            ],
        }
    )
    liquidity["year"] = liquidity["date"].dt.year
    yearly = (
        liquidity.groupby("year", as_index=False)
        .agg(
            records=("stock_symbol", "count"),
            symbols=("stock_symbol", "nunique"),
            amount_na=("amount", lambda s: float(s.isna().mean())),
            turnover_na=("turnover_rate", lambda s: float(s.isna().mean())),
        )
        .sort_values("year")
    )
    yearly.to_csv(os.path.join(out_dir, "liquidity_coverage_by_year_v1.csv"), index=False, encoding="utf-8-sig")
    industry_cov.to_csv(os.path.join(out_dir, "industry_mapping_coverage_v2.csv"), index=False, encoding="utf-8-sig")
    liq_has = liquidity.groupby("stock_symbol").size().rename("n").reset_index()
    join_success_rate = float(pool["stock_symbol"].isin(set(liq_has["stock_symbol"].tolist())).mean())
    lines = []
    lines.append("# 数据侧首版交付质量报告")
    lines.append("")
    lines.append("## 行业映射")
    lines.append("")
    lines.append(f"- pool_symbols: {int(pool['stock_symbol'].nunique())}")
    lines.append(f"- authoritative_mapped_symbols: {int((industry['mapping_method'] == 'baostock_csrc_industry').sum())}")
    lines.append(f"- authoritative_mapping_rate: {(industry['mapping_method'] == 'baostock_csrc_industry').mean():.4f}")
    lines.append(f"- other_symbols: {int((industry['industry_l2'] == '其他').sum())}")
    lines.append(f"- other_ratio: {(industry['industry_l2'] == '其他').mean():.4f}")
    lines.append("- unresolved_list: industry_mapping_unresolved_v2.csv")
    lines.append("")
    lines.append("## 流动性字段")
    lines.append("")
    lines.append(f"- liquidity_records: {int(len(liquidity))}")
    lines.append(f"- liquidity_symbols: {int(liquidity['stock_symbol'].nunique())}")
    lines.append(f"- join_success_rate_to_pool: {join_success_rate:.4f}")
    if not yearly.empty:
        y0 = int(yearly["year"].min())
        y1 = int(yearly["year"].max())
        lines.append(f"- coverage_year_range: {y0}-{y1}")
        lines.append(f"- amount_na_mean: {yearly['amount_na'].mean():.4f}")
        lines.append(f"- turnover_na_mean: {yearly['turnover_na'].mean():.4f}")
    lines.append("")
    lines.append("## 说明")
    lines.append("")
    lines.append("- 行业映射优先使用 Baostock 证监会行业分类，剩余使用关键词兜底。")
    lines.append("- 流动性字段来源于 data/stock_data 日频历史。")
    with open(os.path.join(out_dir, "delivery_quality_report_v2.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
    build_deliverables(ROOT)
