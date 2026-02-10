# 第一步：博主置信度评分框架 (The Scaffold)

## Role: Python 量化工程师

## Context:
根据《V2.0 架构蓝图》，我们要开始执行 **阶段一：数据基建**。
首先需要构建 `core/credibility_scorer.py` 模块，用于给博主和单条信号打分。

## Task:
请创建 `CreatorCredibilityScorer` 类，并实现以下基础评分逻辑：

### 1. 时效性评分 (Time_Score):
* 读取信号的 `publish_time`。
* **盘前 (00:00 - 09:25)**: 黄金时段，预判型，得 **100分**。
* **盘中 (09:25 - 11:30, 13:00 - 14:50)**: 实战时段，得 **85分**。
* **尾盘 (14:50 - 15:00)**: 偷袭/抢筹，得 **80分**。
* **盘后 (15:00 - 23:59)**: 复盘/马后炮，风险较高，得 **40分**。

### 2. 胜率校正 (WinRate_Score):
* 读取该博主在 `backtest_report.csv` 中的历史回测数据。
* 如果有历史记录，`Score = 基础分 * (历史胜率 / 0.5)` (胜率高加权，胜率低降权)。
* 如果是新博主（无记录），给一个默认的中性权重 (1.0)。

### 3. 整合接口:
* 在 `monitor_and_notify.py` 或信号提取流程中调用这个 Scorer。
* 在生成的 CSV 中新增一列 `credibility_score`。

## Output:
请提供 `core/credibility_scorer.py` 的完整代码，并演示如何调用它。

## 技术要点:
- 使用 pandas 处理时间计算
- 集成到现有信号处理流程中
- 保持向后兼容性
- 添加详细的日志记录

## 示例数据格式:
```python
# trading_signals.csv 格式
date,timestamp,author_name,stock_code,action,publish_time
2026-02-06,09:15:00,九哥实盘日记,002995,BUY,09:14:32
2026-02-06,15:30:00,松风论龙头,603629,NEUTRAL,15:28:45
```

## 预期输出:
- `core/credibility_scorer.py` 完整实现
- 集成到现有管道的示例代码
- 测试用例和验证方法