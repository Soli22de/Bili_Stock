# 更新日志

所有 notable changes 将会记录在此文件中。

## [Unreleased]

### 项目维护
- 移除 config.example.py 中泄露的 API 密钥
- 修复 monitor_and_notify.py 和 extract_signals.py 中引用已归档模块的断裂导入
- 移除 run_pipeline_today.py 对已归档 bili_collector.py 的子进程调用
- 清理根目录散落文件，移至 data/ 和 scripts/analysis/
- 删除空桩文件 (run_paper_trading.py, test)
- 清理 extract_signals.py 中遗留的 DEBUG print
- 重写 ARCHITECTURE.md，反映当前雪球智能资金跟踪系统

## [v6.1] - 2026-03 ~ 灰度部署阶段

### 研究基线 v6.1
- Phase A: 扩展绩效指标 (Sortino, CVaR95, MDD 持续天数)
- Phase B: 风控模块 (过热熔断、组合止损、行业/个股回撤限制、集中度限制)
- Phase C: E3 变体微调家族 (E3_1 ~ E3_3, E3_2_1 ~ E3_2_6)
- Phase D: 样本外验证、滚动窗口测试、参数稳健性审计
- Phase E: 5 类诊断 CSV + 汇总报告
- 进入灰度实盘部署

## [v5] - 2026-02

### 研究基线 v5
- 回测样本扩展至 2019-2025 (新增 2019-2021 数据)
- 增加实时约束: 每日选股排除涨停/跌停/停牌
- 所有市场状态 top-bottom 均为正 (上涨 0.009, 震荡 0.018, 下跌 0.063)
- Calmar 0.359 (较 v4.2 大幅提升)

## [v4.2] - 2026-02

### 研究基线 v4.2
- 牛市选股改为两步反转动量 (Top 50 反转 → Top 30 按 20 日收益率)
- 牛市 top-bottom 由负转正 (0.005702)
- Calmar 0.181

## [v4] - 2026-02

### 研究基线 v4
- 市场状态分类: 20 日沪深 300 收益率判断牛/震荡/熊
- 牛市反转动量 + 流动性过滤 80%, 非牛市正向动量 + 60% 过滤
- Calmar 0.173, 最大回撤 -7.39%

## [v1.0.0] - 2026-02-10

### 初始版本
- B 站数据采集基础架构 (BiliCollector, OCR, 关键帧提取)
- 基础回测引擎
- UP 主发现和评分系统
- 风险控制框架

---

## 提交规范

```
类型(范围): 简短描述
```

类型: `feat` / `fix` / `docs` / `style` / `refactor` / `test` / `chore`
