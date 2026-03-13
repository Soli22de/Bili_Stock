# baseline_v2 锁定说明

- 基线版本：三项优化 + 行业中性（含其他行业处理） + 流动性阈值60%
- 三项优化：信号平滑（14天变化率+3日均值）、2w持有约束、5%/95%极值剔除
- 行业中性：使用 industry_mapping_v2，其他行业按既有处理纳入
- 流动性过滤：2022-2025 每期按成交额 amount 保留前60%
- 样本区间：2022-01-01 ~ 2025-12-31

## 锁定指标

- hit_ratio_top_gt_bottom: 0.6000
- max_drawdown_ls_curve: -0.0956
- mean_top_minus_bottom: 0.009622
- calmar_ratio: 0.100635
- obs_days_2w: 25

## 入选理由

- 在约束条件 hit_ratio>=0.6 下，流动性阈值网格（40%/50%/60%）中 Calmar 最高。
- 相比更严阈值，60%在命中率和回撤之间取得更稳健平衡。

## 归档内容

- code/: baseline_v2 相关运行与核心模块代码快照
- output/: baseline_v2 回测输出、网格对比结果、市场环境分段结果
- report/: 锁定说明与后续执行计划
