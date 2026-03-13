# data_delivery 首版说明

## 文件清单

- `industry_mapping_v1.csv`：行业映射首版（关键词映射）
- `industry_mapping_v2.csv`：行业映射增强版（权威源+兜底）
- `industry_mapping_coverage_v1.csv`：行业映射覆盖率统计（v1）
- `industry_mapping_coverage_v2.csv`：行业映射覆盖率统计（v2）
- `industry_mapping_unresolved_v2.csv`：v2 未解析清单
- `liquidity_daily_v1.csv`：流动性日频首版（amount, turnover_rate）
- `liquidity_coverage_by_year_v1.csv`：流动性按年份覆盖统计
- `delivery_quality_report_v1.md`：交付质量摘要
- `delivery_quality_report_v2.md`：交付质量摘要（增强版）

## 关键口径

- 股票池来源：`rebalancing_history` 在 2019-01-01 ~ 2025-12-31 的A股样本
- 行业映射 v2：优先使用 Baostock 的证监会行业分类，再用名称关键词兜底
- 流动性来源：`data/stock_data/*.csv` 的“成交额/换手率”字段

## 当前限制

- 流动性覆盖区间当前为 2022-2025，2019-2021 待补齐
- 行业映射已接入权威行业源，仍建议定期刷新同步
