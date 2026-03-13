# baseline_v5 参数鲁棒性分析报告

![cap_10](robustness_heatmap_cap_10.png)

![cap_15](robustness_heatmap_cap_15.png)

![cap_20](robustness_heatmap_cap_20.png)

## 参数网格摘要

- 最优组合：调仓2w、流动性60%、行业上限15%
- 最优Calmar：0.000368，回撤：-0.531113
- 全网格Calmar区间：[-0.008233, 0.000368]

## 鲁棒性区间结论

- 正超额参数点占比：11.11%；
- 全网格Calmar偏弱，说明严格样本下参数外推稳定性一般。