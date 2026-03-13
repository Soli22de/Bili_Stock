# 发数据侧消息（可直接复制）

各位同学好，这周请按以下优先级补数，供我们下一步策略回测使用：

1) 最最紧急（本周就要）：行业映射  
- 字段：stock_symbol, industry_l1, industry_l2  
- 原因：拿到后我们会立刻做“行业中性”，优先继续压回撤。  
- 要求：2019-2025 覆盖，附缺失率与映射成功率。

2) 次紧急：流动性字段（成交额/换手率）  
- 字段：amount, turnover_rate  
- 原因：用于“流动性过滤”，提升实盘稳定性。  
- 要求：日频，附按年份覆盖率与缺失率。

3) 可以稍晚：涨跌停/停牌标记、P0组合绩效历史面板  
- 涨跌停/停牌字段：limit_up_down_flag, suspended_flag  
- 绩效历史字段：cube_symbol, period_end, return, max_drawdown, turnover, win_rate

请先回传三项的预计交付时间，尤其第1项请确认本周内可交付首版。谢谢。
