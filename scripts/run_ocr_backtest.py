import os
import sys
import pandas as pd
import logging
from tqdm import tqdm
from datetime import datetime

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import GEMINI_API_KEY
from core.llm_processor import LLMProcessor
from core.ocr_validation import verify_price_with_baostock

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # 1. 设置 API Key
    # 强制读取或使用硬编码 (Debugging)
    api_key = GEMINI_API_KEY
    if not api_key or "填入" in api_key:
        api_key = "AIzaSyBNRzGiAuQk9MQEcJbVfdQ1cncmtl58RFw"
    
    if not api_key:
        logger.error("请在 config.py 中设置 GEMINI_API_KEY")
        return
    
    os.environ["GEMINI_API_KEY"] = api_key
    
    # 2. 初始化 LLM 处理器
    try:
        processor = LLMProcessor()
        if not processor.model:
            logger.error("Gemini 模型初始化失败，请检查 API Key 和网络连接")
            return
    except Exception as e:
        logger.error(f"初始化失败: {e}")
        return

    # 3. 加载交易信号数据
    signals_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "trading_signals.csv")
    if not os.path.exists(signals_path):
        logger.error(f"找不到文件: {signals_path}")
        return
        
    df = pd.read_csv(signals_path)
    logger.info(f"加载了 {len(df)} 条交易信号")

    # 4. 准备结果存储
    results = []
    
    # 5. 遍历处理 (为了演示，我们处理前 20 条，或者根据 API 限流情况调整)
    # Gemini 免费版有限流，这里我们批量处理一部分
    process_limit = 20 
    logger.info(f"开始使用 Gemini 进行 LLM/OCR 回测 (处理前 {process_limit} 条)...")
    
    for index, row in tqdm(df.head(process_limit).iterrows(), total=process_limit):
        video_id = str(row.get('video_id', ''))
        text_content = str(row.get('keywords', '')) + " " + str(row.get('source_segment', ''))
        publish_time = str(row.get('date', datetime.now().strftime("%Y-%m-%d")))
        stock_code = str(row.get('stock_code', '')).zfill(6)
        
        # --- A. 文本/语义重提取 (Real LLM) ---
        # 使用 Gemini 重新分析文本，提取更准确的信号和逻辑
        llm_signals = processor.parse_trading_signal(text_content, publish_time)
        
        # 找到匹配当前股票的信号
        matched_signal = next((s for s in llm_signals if s.get('stock_code') == stock_code), None)
        
        llm_action = "UNKNOWN"
        llm_confidence = "LOW"
        llm_logic = ""
        
        if matched_signal:
            llm_action = matched_signal.get('action', 'UNKNOWN')
            llm_confidence = matched_signal.get('confidence', 'MEDIUM') # 默认中等，如果LLM返回则用LLM的
            llm_logic = matched_signal.get('logic', '')
            # 也可以提取价格区间...

        # --- B. 图片 OCR 分析 (Simulation / Placeholder) ---
        # 检查是否有图片文件 (data/images/{video_id}.jpg)
        image_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "images", f"{video_id}.jpg")
        ocr_result = {}
        
        if os.path.exists(image_path):
            # 如果有真实图片，调用 Vision 模型
            ocr_result = processor.analyze_image(image_path, context_text=text_content)
        else:
            # 如果没有图片，我们记录为 "No Image"
            ocr_result = {"error": "No image found"}

        # --- C. 综合验证 ---
        # 如果 LLM 提取的信号比 Regex 更强，或者 OCR 验证通过，我们更新信号强度
        
        new_strength = row.get('strength', 0.5)
        verification_note = []

        # 1. LLM 文本验证
        if llm_action == row.get('action'):
            new_strength += 0.1 # LLM 确认了 Regex 的判断
            verification_note.append("LLM_Confirmed")
        elif llm_action != "UNKNOWN" and llm_action != row.get('action'):
            new_strength -= 0.2 # LLM 反对
            verification_note.append(f"LLM_Disagree({llm_action})")
            
        # 2. OCR 验证 (如果有)
        if "error" not in ocr_result and ocr_result:
            ocr_verified = ocr_result.get("verification_status")
            if ocr_verified == "Verified":
                new_strength += 0.3 # 强力加分
                verification_note.append("OCR_Verified")
            elif ocr_verified == "Suspicious":
                new_strength = 0 # 直接归零
                verification_note.append("OCR_Suspicious")

        # 记录结果
        result_row = row.to_dict()
        result_row['llm_action'] = llm_action
        result_row['llm_logic'] = llm_logic
        result_row['ocr_status'] = ocr_result.get("verification_status", "No_Image")
        result_row['new_strength'] = round(new_strength, 4)
        result_row['verification_details'] = ";".join(verification_note)
        
        results.append(result_row)

    # 6. 保存结果
    output_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "ocr_backtest_results.csv")
    result_df = pd.DataFrame(results)
    result_df.to_csv(output_path, index=False)
    logger.info(f"回测完成，结果已保存至 {output_path}")
    
    # 简单的统计输出
    logger.info("=== 回测统计 ===")
    logger.info(f"处理数量: {len(result_df)}")
    if not result_df.empty:
        logger.info(f"LLM 确认一致: {len(result_df[result_df['verification_details'].astype(str).str.contains('LLM_Confirmed')])}")
        logger.info(f"LLM 意见不合: {len(result_df[result_df['verification_details'].astype(str).str.contains('LLM_Disagree')])}")

if __name__ == "__main__":
    main()
