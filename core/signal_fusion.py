import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import logging
from typing import Dict, List, Any, Tuple
import re

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/signal_fusion.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class SignalFusionEngine:
    def __init__(self):
        # 权重配置
        self.weight_config = {
            "ocr_verified": 1.5,        # 实盘截图验证
            "high_winrate": 1.2,       # 历史胜率高
            "normal_opinion": 0.8,     # 普通口嗨
            "cube_signal": 1.0,        # 组合调仓信号
            "strong_buy_threshold": 2.5,  # 强力买入阈值
        }
        
        # 时间窗口配置 (小时)
        self.time_window = 24
        
        # 数据文件路径
        self.cube_signals_file = "data/cube_rebalancing.csv"
        self.blogger_opinions_file = "data/blogger_opinions.json"
        self.output_file = "data/fused_signals.csv"

    def load_cube_signals(self) -> pd.DataFrame:
        """加载组合调仓信号"""
        try:
            df = pd.read_csv(self.cube_signals_file)
            df['time'] = pd.to_datetime(df['time'])
            # 过滤掉ST股票但保留ETF
            df = df[~df['stock_name'].str.contains('ST', na=False, case=False)]
            df = df[df['stock_name'] != '']
            logging.info(f"加载 {len(df)} 条组合调仓信号")
            return df
        except FileNotFoundError:
            logging.warning("组合调仓信号文件不存在")
            return pd.DataFrame()

    def load_blogger_opinions(self) -> pd.DataFrame:
        """加载博主观点数据"""
        try:
            with open(self.blogger_opinions_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            opinions = []
            for item in data:
                # 提取观点信息
                opinion_data = {
                    "time": pd.to_datetime(item.get("time")),
                    "blogger_name": item.get("blogger_name"),
                    "stock_code": item.get("stock_code"),
                    "stock_name": item.get("stock_name"),
                    "sentiment": item.get("sentiment"),  # 看多/看空/观望
                    "content": item.get("content"),
                    "ocr_verified": item.get("ocr_verified", False),
                    "win_rate": item.get("win_rate", 0.5),
                    "weight": self._calculate_opinion_weight(item)
                }
                opinions.append(opinion_data)
            
            df = pd.DataFrame(opinions)
            logging.info(f"加载 {len(df)} 条博主观点")
            return df
            
        except FileNotFoundError:
            logging.warning("博主观点文件不存在，将使用模拟数据")
            return self._generate_sample_opinions()
        except Exception as e:
            logging.error(f"加载博主观点失败: {e}")
            return pd.DataFrame()

    def _calculate_opinion_weight(self, opinion: Dict) -> float:
        """计算观点权重"""
        weight = self.weight_config["normal_opinion"]
        
        if opinion.get("ocr_verified", False):
            weight *= self.weight_config["ocr_verified"]
        
        if opinion.get("win_rate", 0.5) > 0.7:  # 胜率超过70%
            weight *= self.weight_config["high_winrate"]
        
        return round(weight, 2)

    def _generate_sample_opinions(self) -> pd.DataFrame:
        """生成示例博主观点数据（用于测试）"""
        sample_opinions = [
            {
                "time": datetime.now() - timedelta(hours=2),
                "blogger_name": "量化老王",
                "stock_code": "SZ000858",
                "stock_name": "五粮液",
                "sentiment": "看多",
                "content": "五粮液估值合理，技术面突破，目标价200",
                "ocr_verified": True,
                "win_rate": 0.75,
                "weight": 1.5
            },
            {
                "time": datetime.now() - timedelta(hours=5), 
                "blogger_name": "短线小李",
                "stock_code": "SH600519",
                "stock_name": "贵州茅台",
                "sentiment": "看多",
                "content": "茅台消费复苏，资金流入明显",
                "ocr_verified": False,
                "win_rate": 0.65,
                "weight": 0.8
            }
        ]
        return pd.DataFrame(sample_opinions)

    def extract_opinions_from_text(self, text: str, blogger_info: Dict) -> List[Dict]:
        """
        使用LLM从文本中提取观点（伪代码）
        实际实现需要集成LLM API
        """
        # 这里应该是LLM API调用
        # 返回格式: [{"stock_code": "SZ000001", "sentiment": "看多", "confidence": 0.8}]
        
        # 简单实现：正则匹配股票名称和情绪词
        opinions = []
        
        # 股票模式匹配
        stock_patterns = {
            r"(茅台|600519)": "SH600519",
            r"(五粮液|000858)": "SZ000858", 
            r"(宁德时代|300750)": "SZ300750",
            r"(招商银行|600036)": "SH600036",
        }
        
        # 情绪词匹配
        bullish_words = ["看好", "推荐", "买入", "突破", "目标价", "低估"]
        bearish_words = ["看空", "卖出", "高估", "风险", "谨慎"]
        
        for pattern, stock_code in stock_patterns.items():
            if re.search(pattern, text):
                sentiment = "观望"
                confidence = 0.5
                
                # 判断情绪倾向
                bullish_count = sum(1 for word in bullish_words if word in text)
                bearish_count = sum(1 for word in bearish_words if word in text)
                
                if bullish_count > bearish_count:
                    sentiment = "看多"
                    confidence = 0.7
                elif bearish_count > bullish_count:
                    sentiment = "看空" 
                    confidence = 0.7
                
                opinions.append({
                    "stock_code": stock_code,
                    "sentiment": sentiment,
                    "confidence": confidence,
                    "source_text": text[:100] + "..."  # 截断
                })
        
        return opinions

    def find_signal_resonance(self, cube_signals: pd.DataFrame, 
                             blogger_opinions: pd.DataFrame) -> pd.DataFrame:
        """发现信号共振"""
        if cube_signals.empty or blogger_opinions.empty:
            return pd.DataFrame()
        
        resonant_signals = []
        
        # 按股票分组处理
        for stock_code, stock_cube_signals in cube_signals.groupby('stock_code'):
            stock_opinions = blogger_opinions[blogger_opinions['stock_code'] == stock_code]
            
            if stock_opinions.empty:
                continue
            
            # 对每个调仓信号，查找时间窗口内的观点
            for _, signal in stock_cube_signals.iterrows():
                signal_time = signal['time']
                signal_action = signal['action']
                
                # 查找时间窗口内的观点
                time_low = signal_time - timedelta(hours=self.time_window)
                time_high = signal_time + timedelta(hours=self.time_window)
                
                window_opinions = stock_opinions[
                    (stock_opinions['time'] >= time_low) & 
                    (stock_opinions['time'] <= time_high)
                ]
                
                if not window_opinions.empty:
                    # 计算共振强度
                    resonance_strength = self._calculate_resonance_strength(
                        signal, window_opinions
                    )
                    
                    if resonance_strength >= self.weight_config["strong_buy_threshold"]:
                        resonant_signals.append({
                            "time": signal_time,
                            "stock_code": stock_code,
                            "stock_name": signal['stock_name'],
                            "cube_action": signal_action,
                            "resonance_strength": resonance_strength,
                            "matching_opinions_count": len(window_opinions),
                            "blogger_names": ",".join(window_opinions['blogger_name'].tolist()),
                            "signal_type": "STRONG_BUY" if signal_action == "BUY" else "STRONG_SELL"
                        })
        
        return pd.DataFrame(resonant_signals)

    def _calculate_resonance_strength(self, signal: pd.Series, opinions: pd.DataFrame) -> float:
        """计算共振强度"""
        base_strength = self.weight_config["cube_signal"]
        
        # 观点权重总和
        opinion_strength = opinions['weight'].sum()
        
        # 情绪一致性检查
        signal_sentiment = "看多" if signal['action'] == "BUY" else "看空"
        consistent_opinions = opinions[opinions['sentiment'] == signal_sentiment]
        
        if len(consistent_opinions) > 0:
            # 共振强度 = 基础信号强度 + 一致观点权重和
            resonance = base_strength + consistent_opinions['weight'].sum()
        else:
            # 观点分歧，降低强度
            resonance = base_strength - opinions['weight'].sum() * 0.5
        
        return round(resonance, 2)

    def detect_divergence(self, cube_signals: pd.DataFrame, 
                        blogger_opinions: pd.DataFrame) -> pd.DataFrame:
        """检测信号背离"""
        divergence_signals = []
        
        for stock_code, stock_cube_signals in cube_signals.groupby('stock_code'):
            stock_opinions = blogger_opinions[blogger_opinions['stock_code'] == stock_code]
            
            if stock_opinions.empty:
                continue
            
            for _, signal in stock_cube_signals.iterrows():
                signal_time = signal['time']
                signal_action = signal['action']
                
                # 时间窗口
                time_low = signal_time - timedelta(hours=self.time_window)
                time_high = signal_time + timedelta(hours=self.time_window)
                
                window_opinions = stock_opinions[
                    (stock_opinions['time'] >= time_low) & 
                    (stock_opinions['time'] <= time_high)
                ]
                
                if not window_opinions.empty:
                    signal_sentiment = "看多" if signal_action == "BUY" else "看空"
                    opposite_opinions = window_opinions[window_opinions['sentiment'] != signal_sentiment]
                    
                    if len(opposite_opinions) >= 2:  # 至少2个相反观点
                        divergence_strength = opposite_opinions['weight'].sum()
                        
                        divergence_signals.append({
                            "time": signal_time,
                            "stock_code": stock_code,
                            "stock_name": signal['stock_name'],
                            "cube_action": signal_action,
                            "divergence_strength": divergence_strength,
                            "opposite_opinions_count": len(opposite_opinions),
                            "blogger_names": ",".join(opposite_opinions['blogger_name'].tolist()),
                            "signal_type": "DIVERGENCE"
                        })
        
        return pd.DataFrame(divergence_signals)

    def save_fused_signals(self, resonant_signals: pd.DataFrame, 
                          divergence_signals: pd.DataFrame):
        """保存融合信号"""
        all_signals = []
        
        # 添加共振信号
        for _, signal in resonant_signals.iterrows():
            all_signals.append({
                "timestamp": signal['time'],
                "stock_code": signal['stock_code'],
                "stock_name": signal['stock_name'],
                "signal_type": signal['signal_type'],
                "strength": signal['resonance_strength'],
                "matching_count": signal['matching_opinions_count'],
                "bloggers": signal['blogger_names'],
                "description": f"优质组合调仓 + {signal['matching_opinions_count']}位博主观点共振"
            })
        
        # 添加背离信号
        for _, signal in divergence_signals.iterrows():
            all_signals.append({
                "timestamp": signal['time'],
                "stock_code": signal['stock_code'],
                "stock_name": signal['stock_name'],
                "signal_type": "DIVERGENCE",
                "strength": signal['divergence_strength'],
                "opposite_count": signal['opposite_opinions_count'],
                "bloggers": signal['blogger_names'],
                "description": f"组合调仓与{signal['opposite_opinions_count']}位博主观点背离，建议观望"
            })
        
        # 保存到CSV
        if all_signals:
            df = pd.DataFrame(all_signals)
            df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
            logging.info(f"已保存 {len(all_signals)} 条融合信号")
        else:
            logging.warning("没有发现融合信号")

    def run_fusion(self):
        """运行信号融合流程"""
        logging.info("启动信号融合引擎...")
        
        # 1. 加载数据
        cube_signals = self.load_cube_signals()
        blogger_opinions = self.load_blogger_opinions()
        
        if cube_signals.empty:
            logging.warning("没有组合调仓信号，跳过融合")
            return
        
        # 2. 发现共振信号
        resonant_signals = self.find_signal_resonance(cube_signals, blogger_opinions)
        
        # 3. 检测背离信号
        divergence_signals = self.detect_divergence(cube_signals, blogger_opinions)
        
        # 4. 保存结果
        self.save_fused_signals(resonant_signals, divergence_signals)
        
        # 5. 打印统计信息
        logging.info(f"发现 {len(resonant_signals)} 条共振信号")
        logging.info(f"发现 {len(divergence_signals)} 条背离信号")
        
        if not resonant_signals.empty:
            print("\n=== 强力买入信号 ===")
            print(resonant_signals[["stock_name", "resonance_strength", "blogger_names"]].to_string())

if __name__ == "__main__":
    fusion_engine = SignalFusionEngine()
    fusion_engine.run_fusion()