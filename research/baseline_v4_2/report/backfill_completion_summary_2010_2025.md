# 2010-2025 分批回补补全执行摘要

## 执行范围

- 回补区间：2010-01-01 ~ 2025-12-31
- 执行分段：0-299、300-599、600-856
- 批次大小：50
- 起始容忍窗口：10天

## 分段结果汇总

- 0-299：already_covered=149，backfilled=151，empty_fetch=0，error=0
- 300-599：already_covered=132，backfilled=168，empty_fetch=0，error=0
- 600-856：already_covered=6，backfilled=244，empty_fetch=7，error=0

## 全量汇总

- already_covered：287
- backfilled：563
- empty_fetch：7
- error：0
- timeout_batch_starts：0
- failed_batch_starts：0
- failed_single_indices：0

## 产物与校验

- 批次报告：
  - `research/baseline_v4_2/report/backfill_batch_summary_20260307_172723.csv`
  - `research/baseline_v4_2/report/backfill_batch_summary_20260307_173425.csv`
  - `research/baseline_v4_2/report/backfill_batch_summary_20260307_174313.csv`
- 流动性重建后：`liquidity_daily_v1.csv` 覆盖 2010-01-04 ~ 2025-12-31（rows=614295, symbols=203）
- 因子面板重建后：`factor_panel_rebalance_momentum.csv` 覆盖 2010-01-01 ~ 2025-12-31（rows=3366144）
- v6.1 指标文件已更新：`core_metrics_baseline_v6_1_2019_2025.csv`

## 说明

- `empty_fetch=7` 属于可解释现象，主要为上市前无历史可抓取。
- 已覆盖判定已改为“起始日期+容忍窗口”逻辑，避免对已满足覆盖的文件重复回补。
