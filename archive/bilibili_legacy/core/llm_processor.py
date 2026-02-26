import os
import json
import re
from typing import Dict, Any, Optional, List
import logging
try:
    import google.generativeai as genai
except ImportError:
    genai = None

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LLMProcessor:
    """
    负责调用 LLM (如 Gemini) 进行非结构化数据的结构化提取。
    主要用于从视频 OCR 文字、标题、简介中提取具体的交易指令。
    支持多模态（图片）分析。
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        self.model = None
        self.vision_model = None
        
        if self.api_key and genai:
            try:
                genai.configure(api_key=self.api_key)
                self.model = genai.GenerativeModel('gemini-2.0-flash')
                self.vision_model = genai.GenerativeModel('gemini-2.0-flash')
                logger.info("Gemini API configured successfully")
            except Exception as e:
                logger.error(f"Failed to configure Gemini API: {e}")
        else:
            logger.warning("Gemini API Key or library not available. Using fallback mode.")

    def parse_trading_signal(self, text: str, publish_time: str) -> List[Dict[str, Any]]:
        """
        利用 LLM 从文本中提取交易信号。
        """
        if not self.model:
            return self._fallback_regex_extraction(text)

        try:
            prompt = self._build_extraction_prompt(text, publish_time)
            response = self.model.generate_content(prompt)
            return self._parse_json_response(response.text)
        except Exception as e:
            logger.error(f"LLM extraction failed: {e}")
            return self._fallback_regex_extraction(text)

    def analyze_image(self, image_path: str, context_text: str = "") -> Dict[str, Any]:
        """
        使用 Gemini Vision 模型分析图片（OCR + 语义理解）
        """
        if not self.vision_model:
            return {"error": "Vision model not initialized"}
            
        if not os.path.exists(image_path):
            return {"error": "Image file not found"}

        try:
            import PIL.Image
            img = PIL.Image.open(image_path)
            
            prompt = """
            你是一个专业的量化交易员助手。请分析这张图片（通常是股票持仓截图、K线图或交易记录）。
            请提取以下信息 (JSON格式):
            1. stock_name: 股票名称
            2. stock_code: 股票代码
            3. price: 当前价格或成交价格 (float)
            4. profit_rate: 盈亏比例 (如 "+5.2%", 提取为 0.052)
            5. is_holding: 是否为持仓界面 (bool)
            6. is_transaction: 是否为成交记录 (bool)
            7. verification_status: 根据图片内容判断是否真实实盘 (Verified/Suspicious/Unknown)
            
            重要提示：
            - 很多博主会把操作记录藏在视频关键帧中，请仔细识别。
            - 即使是"事后"展示的交割单，只要清晰可见，也请提取，并标记 is_transaction=True。
            - 如果图片模糊或无相关信息，请返回空JSON。
            """
            if context_text:
                prompt += f"\n上下文参考: {context_text}"
                
            response = self.vision_model.generate_content([prompt, img])
            return self._parse_json_dict(response.text)
        except Exception as e:
            logger.error(f"Image analysis failed: {e}")
            return {"error": str(e)}

    def _parse_json_response(self, text: str) -> List[Dict[str, Any]]:
        try:
            # 尝试提取 JSON 块
            match = re.search(r'```json\s*(.*?)\s*```', text, re.DOTALL)
            if match:
                json_str = match.group(1)
            else:
                json_str = text
            
            # 清理可能的非 JSON 字符
            json_str = json_str.strip()
            if not json_str.startswith('['):
                # 尝试找到列表开始
                start = json_str.find('[')
                if start != -1:
                    json_str = json_str[start:]
            
            if not json_str.endswith(']'):
                 end = json_str.rfind(']')
                 if end != -1:
                     json_str = json_str[:end+1]

            return json.loads(json_str)
        except Exception as e:
            logger.warning(f"Failed to parse LLM JSON response: {e}")
            return []

    def _parse_json_dict(self, text: str) -> Dict[str, Any]:
        try:
            match = re.search(r'```json\s*(.*?)\s*```', text, re.DOTALL)
            if match:
                json_str = match.group(1)
            else:
                json_str = text
            return json.loads(json_str)
        except Exception:
            return {}


    def _build_extraction_prompt(self, text: str, publish_time: str) -> str:
        return f"""
        你是一个专业的量化交易员助手。请从以下文本中提取股票交易信号。
        
        文本内容: "{text}"
        发布时间: {publish_time}
        
        请提取以下字段 (JSON格式返回):
        - stock_code: 6位股票代码 (如 600000)
        - stock_name: 股票名称
        - action: 操作建议 (BUY / SELL / WATCH / HOLD)
        - entry_price_min: 建议买入价格下限 (可选, float)
        - entry_price_max: 建议买入价格上限 (可选, float)
        - target_price: 止盈/目标价格 (可选, float)
        - stop_loss_price: 止损价格 (可选, float)
        - logic: 推荐逻辑摘要
        - is_hindsight: 是否为事后/复盘内容 (bool)。如果内容是"昨天买了XXX"或"之前在XXX低吸了"，标记为 true。
        - actual_buy_price: 如果是事后/复盘内容，提取其实际买入价格 (可选, float)
        - actual_buy_time: 如果是事后/复盘内容，提取其实际买入时间 (可选, string)

        重要规则:
        1. 即使是"事后诸葛亮"的内容(如"昨天我低吸了XX")，也请提取出来，将 is_hindsight 设为 true，并记录 actual_buy_price。这类信息用于分析博主的历史准确率。
        2. 如果文本中包含具体的买卖价格区间，请务必提取。
        3. 如果没有明确股票代码或交易建议，返回空列表 []。
        """

    def _fallback_regex_extraction(self, text: str) -> List[Dict[str, Any]]:
        """
        简单的正则提取作为 Fallback，用于提取代码和基本方向。
        无法提取复杂的逻辑或价格区间。
        """
        signals = []
        # 匹配 6位数字代码
        codes = re.findall(r'\b[036]\d{5}\b', text)
        unique_codes = list(set(codes))
        
        for code in unique_codes:
            signal = {
                "stock_code": code,
                "action": "WATCH", # 默认
                "confidence": "LOW"
            }
            
            # 简单关键词匹配方向
            if any(w in text for w in ["买入", "看多", "机会", "低吸", "建仓"]):
                signal["action"] = "BUY"
            elif any(w in text for w in ["卖出", "止盈", "出货", "风险", "高抛"]):
                signal["action"] = "SELL"
                
            # 尝试提取价格 (非常简单的正则，仅作示例)
            # 例如: "10.5附近买入" -> 10.5
            price_match = re.search(r'(\d+\.\d+)[\u4e00-\u9fa5]*买', text)
            if price_match:
                try:
                    p = float(price_match.group(1))
                    signal["entry_price_max"] = p * 1.01
                    signal["entry_price_min"] = p * 0.99
                except:
                    pass
                    
            signals.append(signal)
            
        return signals

    def optimize_ocr_text(self, raw_ocr_text: str) -> str:
        """
        清洗 OCR 出来的乱码或无关字符
        """
        # 简单清洗逻辑
        lines = raw_ocr_text.split('\n')
        clean_lines = [line.strip() for line in lines if len(line.strip()) > 2]
        return " ".join(clean_lines)
