# baseline_v6.1 实盘输入规范

## 1. 目录约定

- 根目录：`research/baseline_v6_1/output/live/`
- 必备输入：
  - `live_equity.csv`
  - `live_holdings.csv`
  - `live_risk_log.csv`
- 自动输出：
  - `gray_deployment_decision.csv`
  - `daily_nav_compare.csv`
  - `daily_report.md`
  - `weekly_report.md`

## 2. 字段规范

## 2.1 live_equity.csv

- 必需字段：
  - `date`：交易日，格式 `YYYY-MM-DD`
  - 二选一：
    - `spread`：当期多空收益
    - `equity`：累计净值

## 2.2 live_holdings.csv

- 必需字段：
  - `date`
  - `stock_symbol`
  - `weight`

## 2.3 live_risk_log.csv

- 必需字段：
  - `date`
  - `trigger_type`
  - `subject`
  - `value`
- 可选字段：
  - `new_risk_scale`
  - `recover_flag`

## 3. 校验规则

- 日期字段必须可解析为交易日时间。
- 同一文件内 `date` 不能为空；`live_holdings` 还需校验 `stock_symbol` 非空。
- `live_equity` 中若无 `spread` 则必须提供 `equity`。
- 当日持仓权重之和允许区间：`[0.95, 1.05]`，超出则写入警告。
- 文件存在但为空时，执行器输出错误报告并保持上一决策。
