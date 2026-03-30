# baseline_v6.1 日常值守检查单

## 使用说明

- 每个交易日执行一次，按顺序完成三步检查。
- 任一步出现 FAIL，先处理故障再继续。
- 填写后建议归档到当天日志或日报附件。

## 基本信息

- 日期：____-__-__
- 值守人：____
- 环境：`research/baseline_v6_1/output/live`

## Step 1: Gray Daily Ops

- 执行技能：`gray-daily-ops`
- Pipeline 状态：PASS / WARN / FAIL
- 核心产物检查：
  - `gray_deployment_decision.csv`：已生成 / 未生成
  - `daily_report.md`：已生成 / 未生成
  - `weekly_report.md`：已生成 / 未生成
  - `live_input_quality_report.md`：已生成 / 未生成
- 备注：____

## Step 2: Decision Consistency

- 执行技能：`decision-consistency-check`
- 一致性状态：PASS / WARN / FAIL
- 对比字段：
  - action：一致 / 不一致
  - reasons：一致 / 不一致
  - gap_now：一致 / 不一致
  - gap_prev：一致 / 不一致
  - cycle_mdd：一致 / 不一致
  - oos_sortino：一致 / 不一致
- 漂移说明：____

## Step 3: Risk Log Policy

- 执行技能：`risk-log-policy-check`
- 风控映射状态：PASS / WARN / FAIL
- 关键检查：
  - severe 触发是否正确映射 pause：是 / 否
  - moderate 累计是否正确映射 reduce：是 / 否
  - risk_scale 阈值映射是否正确：是 / 否
  - reasons 可追溯性：PASS / WARN
- 备注：____

## 异常分流

- 若 Step 1 = FAIL：停止后续检查，先修执行错误。
- 若 Step 2 = WARN/FAIL：冻结策略变更，先修线上/离线决策漂移。
- 若 Step 3 = WARN/FAIL：先修风控映射与 reasons，再评估是否继续灰度放量。
- 若输入质量告警包含 `live_equity_stale`：保持 `pause_revalidate`，优先补齐 live 输入数据。

## 当日结论

- Pipeline：PASS / WARN / FAIL
- Consistency：PASS / WARN / FAIL
- RiskPolicy：PASS / WARN / FAIL
- FinalAction：pause_revalidate / reduce_to_30 / hold_50 / upgrade_to_70
- FinalReasons：____
- 是否允许下一交易日按原计划执行：是 / 否
