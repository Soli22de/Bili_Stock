# 探索对比摘要（热度绝对值 vs baseline_v1）

## 对比口径

- 基线因子：过去14天净买入组合数变化率的3日移动平均
- 探索因子：过去14天净买入组合数绝对值的3日移动平均
- 共同设置：2w强制持有、5%/95%极值剔除、三分组、2019-2025

## 指标对比

- baseline hit_ratio: 0.6400
- explore hit_ratio: 0.6400
- baseline max_drawdown: -0.0881
- explore max_drawdown: -0.0933
- baseline top-bottom: 0.011583
- explore top-bottom: 0.010726
- baseline calmar: 0.131458
- explore calmar: 0.115019

## 结论

- 探索版未超过基线 hit_ratio，也未改善回撤。
- 探索版 top-bottom 与 Calmar 均弱于基线。
- 维持 baseline_v1 作为当前基线，探索版仅保留为对照结果。
