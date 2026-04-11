# baseline_v6.1 震荡优化诊断与实验报告

- 评估口径：训练2010-2020，样本外2021-2025，单边成本0.1%。
- 排序优先级：oos_sortino > 震荡_top_bottom > calmar > mdd。

- base_E_foundation: oos_sortino=0.192489, 震荡_top_bottom=-0.001585, calmar=0.048743, mdd=-0.284348
- exp1_1_E_overheat_loose: oos_sortino=0.192489, 震荡_top_bottom=-0.001585, calmar=0.048743, mdd=-0.284348
- exp2_1_E_xq_loose40: oos_sortino=0.192489, 震荡_top_bottom=-0.001585, calmar=0.048743, mdd=-0.284348
- base_v6_1: oos_sortino=-0.268642, 震荡_top_bottom=-0.002298, calmar=0.195924, mdd=-0.192153

- 最终建议：回退E基础版（base_E_foundation）。
