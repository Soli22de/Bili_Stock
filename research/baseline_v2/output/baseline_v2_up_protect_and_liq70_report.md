# 上涨环境保护版与流动性70%探索（2022-2025）

## 实验A：上涨环境保护版

- 调整内容：上涨市单票权重20%→10%（收益缩放0.5）；上涨市流动性改为仅保留前20%最活跃股票；其余环境保持baseline_v2。
- 上涨市 top-bottom（baseline_v2）：-0.013484
- 上涨市 top-bottom（保护版）：-0.005296
- 是否从负值拉回正：否
- 整体 Calmar（baseline_v2）：0.100635
- 整体 Calmar（保护版）：0.134190

## 实验B：流动性阈值70%探索

- 总交易次数 baseline_v2(liq60)：1844
- 总交易次数 liq70：2008
- hit_ratio baseline_v2(liq60)：0.6000
- hit_ratio liq70：0.6000
- max_drawdown baseline_v2(liq60)：-0.0956
- max_drawdown liq70：-0.1142
- calmar baseline_v2(liq60)：0.100635
- calmar liq70：0.065421

## 结论

- baseline_v2.1 建议条件：hit_ratio接近0.6、max_drawdown>-0.12、calmar变化可接受且可选标的更广。