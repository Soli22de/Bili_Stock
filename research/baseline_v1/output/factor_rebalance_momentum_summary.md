# 雪球调仓动量因子MVP报告

- 样本区间：2019-01-01 ~ 2025-12-31
- 因子：过去14天净买入组合数变化率的3日移动平均
- 极端值处理：每期剔除因子最高5%和最低5%
- 持有期约束：买入后强制持有2w（10交易日）
- 分组：Top30 / Middle40 / Bottom30
- 预测周期：2w

## 回测摘要

- mean_top: 0.026289
- mean_middle: 0.022706
- mean_bottom: 0.014706
- mean_top_minus_bottom: 0.011583
- hit_ratio_top_gt_bottom: 0.6400
- obs_days_2w: 25
- max_drawdown_ls_curve: -0.0881
- calmar_ratio: 0.131458

## 门槛判定

- threshold_obs_days>=20: 通过
- threshold_hit_ratio>=0.65: 未通过
- threshold_top_bottom_excess>0: 通过
- threshold_max_drawdown<=0.3: 通过
- threshold_overall: 未通过

## 与优化前对比

- max_drawdown 基线：-0.5388
- max_drawdown 当前：-0.0881
- hit_ratio 基线：0.7078
- hit_ratio 当前：0.6400
- top-bottom 基线：0.010932
- top-bottom 当前：0.011583
- calmar 当前：0.131458

## 结论

- 方向判断：先补数据再判断