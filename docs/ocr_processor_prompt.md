# 第二步：OCR验证管道 (The Hard Core)

## Role: 计算机视觉工程师 & 数据清洗专家

## Context:
继续推进阶段一。现在我们需要实现最核心的 **"持仓验证"** 模块。很多博主会在视频中展示持仓界面或交割单，这是验证是否实盘的铁证。

## Task:
请创建 `core/ocr_processor.py`，并在 `scripts/research_ocr_demo.py` 的基础上进行工程化落地。

## Requirements:

### 1. 技术栈: 
使用 `PaddleOCR` (对中文表格支持最好)。

### 2. 核心功能:
* `extract_trade_info(image_path)`: 传入视频关键帧。
* **关键词定位**: 搜索图中是否存在 "成交"、"持仓"、"成本"、"盈亏" 等关键词。
* **正则提取**: 一旦定位到关键词，尝试提取附近的 **股票名称/代码** 和 **数字(价格)**。

### 3. BaoStock 对齐验证 (Cross-Check):
* 如果 OCR 提取到了 "买入价 15.50"，请调用 `BaoStock` 获取当天的 `High` 和 `Low`。
* 如果 `Low <= 15.50 <= High`，标记为 **Verified (真)**。
* 如果价格不在当日范围内，标记为 **Fake (假/P图)**。

### 4. 多帧投票机制 (防模糊):
* 对同一视频连续提取 5 帧关键帧
* 对每帧进行 OCR 识别
* 取出现次数最多的识别结果作为最终结果
* 设置置信度阈值 (如: 至少3帧识别结果一致)

## Output:
请写出带有 BaoStock 验证逻辑的 OCR 处理类代码。

## 技术要点:
- 集成 PaddleOCR 并处理中文表格
- 实现多帧投票机制提高识别准确率
- 与 BaoStock API 进行数据对齐验证
- 添加错误处理和重试机制

## 预期输出:
- `core/ocr_processor.py` 完整实现
- 多帧投票机制的实现
- BaoStock 验证逻辑
- 测试用例和验证方法

## 示例调用:
```python
from core.ocr_processor import OCRProcessor

processor = OCRProcessor()
result = processor.process_video_frame("video_frame_001.jpg")
# 返回: {'verified': True, 'stock_code': '002995', 'price': 25.60, 'confidence': 0.85}
```

## 注意事项:
- OCR 识别可能因视频模糊而错误，多帧投票是关键
- BaoStock 数据需要处理网络请求异常
- 考虑性能优化，避免频繁调用 OCR 接口