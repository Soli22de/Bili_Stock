# baseline_v5.1 评估结论（恐慌指数仓位管理）

- 恐慌指数定义：HS300 20日年化波动率
- 仓位规则：
  - vol20 > 20%：仓位 70%
  - 10% <= vol20 <= 20%：满仓
  - vol20 < 10%：仓位 80%

## 对比结果（2019-2025）

- baseline_v5：calmar 0.359286，max_drawdown -0.061488
- baseline_v5.1：calmar 0.299026，max_drawdown -0.061488

## 结论

- 虽然 baseline_v5.1 的 Calmar 仍高于 0.2，但相对 baseline_v5 明显下降。
- 本轮不固化 v5.1，维持 baseline_v5 为当前最优。
