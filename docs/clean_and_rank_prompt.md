# 第三步：数据清洗与黑名单 (The Cleaner)

## Role: 量化策略风控官

## Context:
我们已经有了 `Time_Score` (时效分) 和 `OCR_Verify` (持仓验证)。现在要将它们结合，清洗我们的博主库。

## Task:
请编写一个脚本 `scripts/clean_and_rank_bloggers.py`：

### 1. 综合评分计算:
* `Total_Score = Time_Score * 0.4 + OCR_Verify_Bonus * 0.6`
* 如果 OCR 验证为 **Verified**，给予巨大的加分 (例如直接 +50分)。
* 如果 OCR 验证为 **Fake** (价格对不上)，直接**拉黑 (Blacklist)**。

### 2. 博主分层:
* **Tier 1 (王者)**: 总分 > 80 且有 OCR 实盘证据。 → 策略权重 1.5倍。
* **Tier 2 (观察)**: 总分 > 60。 → 策略权重 1.0倍。
* **Tier 3 (淘汰)**: 总分 < 60 或 疑似模拟盘。 → 剔除出监控列表。

### 3. 输出:
* 生成 `blogger_tier_list.json`，供后续的交易决策模块使用。
* 生成 `blacklist.json`，记录被拉黑的博主及原因。

## Output:
请给出实现这个清洗逻辑的代码。

## 技术要点:
- 整合前两步的评分结果
- 实现分层逻辑和权重分配
- 生成结构化 JSON 输出
- 与现有监控系统集成

## 预期输出:
- `scripts/clean_and_rank_bloggers.py` 完整脚本
- 分层逻辑的实现
- JSON 输出格式定义
- 集成到监控流程的示例

## 示例输出格式:
```json
// blogger_tier_list.json
{
  "tier1": [
    {
      "author_name": "九哥实盘日记",
      "total_score": 92,
      "time_score": 100,
      "ocr_bonus": 50,
      "weight_multiplier": 1.5,
      "verification_status": "verified"
    }
  ],
  "tier2": [...],
  "tier3": [...],
  "blacklist": [
    {
      "author_name": "疑似模拟盘",
      "reason": "OCR验证价格不匹配",
      "detected_date": "2026-02-06"
    }
  ]
}
```

## 调用示例:
```python
# 运行清洗脚本
python scripts/clean_and_rank_bloggers.py

# 输出结果
Generated blogger_tier_list.json with 15 bloggers ranked
Blacklisted 3 bloggers for fake verification
```

## 注意事项:
- 确保评分权重合理分配
- 黑名单机制要谨慎，避免误杀
- 定期重新评估博主分层
- 记录详细的审计日志