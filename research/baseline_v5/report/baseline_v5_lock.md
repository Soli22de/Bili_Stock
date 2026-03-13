# baseline_v5 锁定说明

- 样本区间：2019-01-01 ~ 2025-12-31
- 基线来源：baseline_v4.2
- 本轮新增规则：调仓当日剔除涨跌停或停牌标的

## 配置

- 非上涨市：沿用 baseline_v4.2 逻辑
- 上涨市：沿用 baseline_v4.2 反向因子 + 二次过滤逻辑
- 实盘约束：当日 `is_limit=True` 或 `is_suspended=True` 的股票不参与当期选股

## 核心指标（全样本）

- calmar_ratio: 0.359286
- max_drawdown_ls_curve: -0.061488
- top_bottom: 0.022092
- 上涨_top_bottom: 0.009394
- 震荡_top_bottom: 0.017920
- 下跌_top_bottom: 0.063177

## 锁定结论

- Calmar 显著高于 0.18，最大回撤优于 -0.08，满足固化条件。
- 三类市场环境 top-bottom 均为正，具备进入小资金实盘前复核基础。
