# baseline_v5 交易成本回测（2019-2025）

- 成本参数：佣金0.02%双边、印花税0.1%卖出、过户费0.002%双边。
- 可选冲击成本：0.05%双边（with_impact）。
- 资金口径验证：初始资金 100,000。
- no_impact: calmar=0.212043, mdd=-0.070461, hit=0.6400, excess=0.014941
- with_impact: calmar=0.200878, mdd=-0.071607, hit=0.6400, excess=0.014384
- no_impact_capital: ending=143483.42, pnl=43483.42, return=43.4834%
- with_impact_capital: ending=141529.93, pnl=41529.93, return=41.5299%
