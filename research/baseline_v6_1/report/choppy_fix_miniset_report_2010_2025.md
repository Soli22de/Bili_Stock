# 震荡期修复最小实验集（2010-01-01~2025-12-31）

- 对比目标：优先提升样本外Sortino与震荡_top_bottom，再看Calmar与MDD。

- base_E_foundation: oos_sortino=-0.215511, 震荡_top_bottom=-0.002473, sortino=0.000205, calmar=0.000042, mdd=-0.476548, turnover=0.722146
- exp1_E_overheat_light: oos_sortino=-0.215511, 震荡_top_bottom=-0.002473, sortino=0.000205, calmar=0.000042, mdd=-0.476548, turnover=0.722146
- exp2_E_xq_warn_scale55: oos_sortino=-0.233065, 震荡_top_bottom=-0.002772, sortino=-0.013116, calmar=-0.002677, mdd=-0.482323, turnover=0.722029
- base_v6_1: oos_sortino=-0.358485, 震荡_top_bottom=-0.003588, sortino=-0.201166, calmar=-0.039276, mdd=-0.494451, turnover=0.605846

- 推荐方案：base_E_foundation
- 风险提示：
  - 当前Calmar与硬约束仍有差距，极端回撤收敛仍需强化。
  - 震荡防守参数主要改善日常亏损，不完全覆盖黑天鹅。
  - 参数需滚动检验，禁止一次性放大仓位。
  - non_up_vol_q上调通常提升弹性但稳定性下降；choppy_loss_scale上调通常收敛回撤但牺牲收益。
