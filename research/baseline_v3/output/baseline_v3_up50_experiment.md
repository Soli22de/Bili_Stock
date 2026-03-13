# baseline_v3 与上涨市降仓50%对比（2022-2025）

## baseline_v3

- 配置：三项优化 + 行业中性 + 流动性阈值60% + 上涨市单票10% + 上涨市流动性前20%
- hit_ratio: 0.6400
- max_drawdown: -0.0937
- top-bottom: 0.012570
- calmar: 0.134190
- 上涨市top-bottom: -0.005296

## 上涨市降仓50%微优化

- 配置：在 baseline_v3 基础上，仅上涨市整体仓位50%（其余不变）
- hit_ratio: 0.6400
- max_drawdown: -0.0937
- top-bottom: 0.012570
- calmar: 0.134190
- 上涨市top-bottom: -0.005296

## 结论

- 上涨市超额是否转正：否
- 整体Calmar是否进一步提升：否