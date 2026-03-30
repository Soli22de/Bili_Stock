# 震荡期修复最小实验集（2019-01-01~2025-12-31）

- 对比目标：优先提升震荡_top_bottom，其次保证Calmar不显著恶化。

- choppy_fix_B_hold12_cap10: 震荡_top_bottom=0.001045, calmar=0.282405, mdd=-0.236972, turnover=0.528254, liq=0.60, cap_non_up=0.10, hold_step=12
- choppy_fix_C_hold12_liq55_cap10: 震荡_top_bottom=0.001045, calmar=0.282405, mdd=-0.236972, turnover=0.528254, liq=0.55, cap_non_up=0.10, hold_step=12
- choppy_fix_A_liq50_cap12: 震荡_top_bottom=-0.004230, calmar=-0.104253, mdd=-0.583241, turnover=0.488906, liq=0.50, cap_non_up=0.12, hold_step=10
- base_v6_1: 震荡_top_bottom=-0.004230, calmar=-0.104253, mdd=-0.583241, turnover=0.488906, liq=0.60, cap_non_up=0.15, hold_step=10

- 推荐方案：choppy_fix_B_hold12_cap10
