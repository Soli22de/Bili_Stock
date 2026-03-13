# baseline_v3 锁定说明

- 基线版本：三项优化 + 行业中性 + 流动性阈值60% + 上涨环境保护开关
- 上涨环境保护开关：HS300 20日涨跌幅>2% 时，单票权重10%，流动性仅保留前20%最活跃股票
- 样本区间：2022-01-01 ~ 2025-12-31

## 锁定指标

- hit_ratio_top_gt_bottom: 0.6400
- max_drawdown_ls_curve: -0.0937
- mean_top_minus_bottom: 0.012570
- calmar_ratio: 0.134190
- 上涨市 top-bottom: -0.005296

## 入选理由

- 在 baseline_v2 基础上，整体 Calmar 从 0.100635 提升至 0.134190。
- 上涨市超额从 -0.013484 改善到 -0.005296，方向显著改善。
- 回撤控制仍稳健，max_drawdown 维持在可接受区间。

## 归档内容

- code/: baseline_v3 运行脚本与核心模块快照
- output/: baseline_v3 回测结果与上涨市微优化结果
- report/: 锁定说明与后续执行计划
