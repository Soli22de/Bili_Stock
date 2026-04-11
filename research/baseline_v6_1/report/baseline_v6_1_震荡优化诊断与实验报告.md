# baseline_v6.1 震荡优化诊断与实验报告

## Phase 1: 初始诊断（已完成）

- 评估口径：训练2010-2020，样本外2021-2025，单边成本0.1%。
- 排序优先级：oos_sortino > 震荡_top_bottom > calmar > mdd。

- base_E_foundation: oos_sortino=0.192489, 震荡_top_bottom=-0.001585, calmar=0.048743, mdd=-0.284348
- exp1_1_E_overheat_loose: oos_sortino=0.192489, 震荡_top_bottom=-0.001585, calmar=0.048743, mdd=-0.284348
- exp2_1_E_xq_loose40: oos_sortino=0.192489, 震荡_top_bottom=-0.001585, calmar=0.048743, mdd=-0.284348
- base_v6_1: oos_sortino=-0.268642, 震荡_top_bottom=-0.002298, calmar=0.195924, mdd=-0.192153

- 最终建议：回退E基础版（base_E_foundation）。

## Phase 2: 震荡处理优化（已完成）

核心发现：震荡IC=-0.001（纯噪声），占30%交易日。

| 方案 | Calmar | 年化 | MDD | 结论 |
|------|--------|------|-----|------|
| choppy_loss_scale=0.0（非对称） | **0.480** | 6.91% | -14.4% | **生产方案** |
| SRF v2 + 非对称 | 0.319 | 5.79% | -18.2% | SRF增加噪声 |
| go_flat_choppy=True（全平） | 0.208 | 3.77% | -18.1% | 丢弃盈利震荡期 |
| baseline | 0.103 | 2.58% | -24.9% | 参考基准 |
| SRF v1 替换门控 | <0 | 负 | >-50% | 摧毁alpha |

关键洞察：非对称>全平。震荡IC=-0.001是均值，非常数——部分震荡期盈利，部分亏损。保留盈利日全仓位+亏损日缩至30%，捕捉正尾部。

详见：[phase2_research_report.md](phase2_research_report.md)
