# baseline_v6.1 A-E 阶段执行汇总

- 阶段A：基线冻结+评估指标扩展（含Sortino/Downside/MDD持续期/CVaR95）。
- 阶段B：风控主模块执行（过热刹车/组合止损/行业与个股止损/集中度约束/对冲触发）。
- 阶段C：E3版本族微调（E3_1~E3_3，E3_2_1~E3_2_6）。
- 阶段D：样本外与滚动验证+参数稳健性+偏差审计+可信度评分。
- 阶段E：五项专项诊断CSV与汇总报告。

## 产物检查

- 阶段A: 已生成 - C:\jz_code\Bili_Stock\research\baseline_v6_1\report\phase_a_baseline_metrics.csv
- 阶段B: 已生成 - C:\jz_code\Bili_Stock\research\baseline_v6_1\report\phase_b_risk_control_report.md
- 阶段C1: 已生成 - C:\jz_code\Bili_Stock\research\baseline_v6_1\report\e3_focus_report.md
- 阶段C2: 已生成 - C:\jz_code\Bili_Stock\research\baseline_v6_1\report\e3_2_micro_tuning_report.md
- 阶段C3: 已生成 - C:\jz_code\Bili_Stock\research\baseline_v6_1\report\e3_2_light_tuning_report.md
- 阶段D1: 已生成 - C:\jz_code\Bili_Stock\research\baseline_v6_1\report\oos_elimination_report.md
- 阶段D2: 已生成 - C:\jz_code\Bili_Stock\research\baseline_v6_1\report\phase_d_validation_report.md
- 阶段E1: 已生成 - C:\jz_code\Bili_Stock\research\baseline_v6_1\report\phase_e_special_diagnostics_report.md

## 决策门槛与淘汰规则

- 样本外淘汰规则已执行：连续12个月跑输基准/Calmar<0/MDD>30%。
- 停机回退建议：若新版收益提升但回撤恶化，回退上一稳定版本。
