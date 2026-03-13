# 补数据清单（P0/P1）

## 当前覆盖快照

- 调仓记录最早时间：2014-11-25 00:12:52.108000
- 调仓记录最晚时间：2026-03-02 23:00:31.172000
- 2019-2025 调仓记录行数：6029
- 2019-2025 股票池去重（原始symbol）：1003
- 2019-2025 股票池去重（A股口径）：581

## P0（立即补）

- 组合绩效历史面板（按月/按季）
  - 字段：cube_symbol, period_end, return, max_drawdown, turnover, win_rate
- 调仓披露时点字段
  - 字段：signal_publish_time, signal_visible_time
- 组合调仓后披露延迟天数（P0补充）
  - 字段：disclosure_delay_days
- 价格覆盖质量报告
  - 指标：coverage_by_year, missing_rate_by_symbol, join_success_rate

## P1（增强稳健性）

- 最高优先级：行业映射
  - 字段：stock_symbol, industry_l1, industry_l2
- 次高优先级：流动性字段
  - 字段：amount, turnover_rate
- 一般优先级：涨跌停/停牌标记
  - 字段：limit_up_down_flag, suspended_flag
