# baseline_v1 锁定说明

- 基线版本：信号平滑 + 持有期约束 + 极端值剔除
- 因子定义：过去14天净买入组合数变化率的3日移动平均
- 回测设置：2w强制持有、每期剔除最高5%与最低5%、三分组、2019-2025
- 运行入口：`python research/run_rebalance_momentum_mvp.py`

## 锁定指标

- hit_ratio_top_gt_bottom: 0.6400
- max_drawdown_ls_curve: -0.0881
- mean_top_minus_bottom: 0.011583
- calmar_ratio: 0.131458
- obs_days_2w: 25

## 归档内容

- code/: 因子、面板、回测、绘图、主入口代码快照
- output/: 面板CSV、分组收益CSV、多空CSV、曲线PNG、摘要与补数据清单
- report/: 锁定说明、数据侧外发文本、探索对比摘要
