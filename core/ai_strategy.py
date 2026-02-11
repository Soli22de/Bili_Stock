import numpy as np
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime
import logging
import random
import sys
import os

# Ensure core modules can be imported
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.strategies import StrategyConfig, DragonStrategy
from core.backtest_engine import BacktestEngine

class EvolutionaryTradingAI:
    """
    进化交易AI核心类
    基于遗传算法(Genetic Algorithm)和深度学习(Deep Learning)的自我进化交易系统
    """
    
    def __init__(self, config: Dict = None):
        self.config = config or {}
        self.logger = logging.getLogger("EvolutionaryTradingAI")
        self.strategies = []  # 策略池 (DragonStrategy objects)
        self.generation = 0   # 当前进化代数
        
    def evolve_strategies(self, population_size: int = 20, generations: int = 5):
        """
        执行策略进化过程
        """
        self.logger.info(f"Starting evolution process: Gen {self.generation} -> {self.generation + generations}")
        
        # 1. 初始化种群
        if not self.strategies:
            self.strategies = self._initialize_population(population_size)
            
        for gen in range(generations):
            self.generation += 1
            print(f"\n--- Evolution Generation {self.generation} ---")
            
            # 2. 评估当前种群
            scores = self._evaluate_population(self.strategies)
            
            # 3. 选择优胜者
            elite = self._select_elite(self.strategies, scores)
            
            # 记录本代最佳
            best_score = max(scores) if scores else 0
            print(f"Gen {self.generation} Best Score: {best_score:.4f}")
            
            # 4. 交叉变异产生下一代
            offspring = self._crossover_and_mutate(elite, population_size)
            
            self.strategies = elite + offspring
            
    def _initialize_population(self, size: int) -> List[DragonStrategy]:
        """初始化随机策略种群"""
        population = []
        for _ in range(size):
            # 随机生成策略参数
            config = StrategyConfig(
                max_sentiment_score=random.randint(3, 8),
                min_amount=random.choice([20000000.0, 30000000.0, 50000000.0]),
                max_3d_pct=random.uniform(15.0, 30.0),
                stop_loss_atr_multiplier=random.uniform(1.5, 3.5),
                max_holding_days=random.randint(2, 5),
                take_profit_open_pct=random.uniform(0.03, 0.08),
                vwap_threshold=random.uniform(0.98, 1.02)
            )
            population.append(DragonStrategy(config))
        return population

    def _evaluate_population(self, population: List[DragonStrategy]) -> List[float]:
        """回测评估种群中每个策略的表现"""
        scores = []
        for i, strategy in enumerate(population):
            # 创建临时引擎运行回测
            engine = BacktestEngine(strategy=strategy)
            
            # 捕获输出以避免刷屏，实际使用中可能需要优化
            try:
                # 运行回测 (限制天数以加快速度)
                # 注意：这里会产生 I/O 和 网络请求，大规模运行时需谨慎
                # 实际生产中应使用本地缓存数据
                engine.run_backtest(max_days=5) 
                
                # 从报告中读取结果计算得分
                # 简化的适应度函数: PnL * 0.7 + WinRate * 0.3
                report_path = "data/backtest_report.csv"
                if os.path.exists(report_path):
                    df = pd.read_csv(report_path)
                    executed = df[df['status'] == 'EXECUTED']
                    if not executed.empty:
                        avg_pnl = executed['pnl'].mean()
                        win_rate = (executed['pnl'] > 0).mean() * 100
                        score = avg_pnl * 0.7 + win_rate * 0.05 # 权重可调
                    else:
                        score = -10.0 # 无交易惩罚
                else:
                    score = -10.0
            except Exception as e:
                self.logger.error(f"Strategy {i} evaluation failed: {e}")
                score = -20.0
                
            scores.append(score)
            print(f"Strategy {i}: Score {score:.2f} (Cfg: SL={strategy.config.stop_loss_atr_multiplier:.1f}, Hold={strategy.config.max_holding_days})")
            
        return scores

    def _select_elite(self, population: List[DragonStrategy], scores: List[float], top_k: int = 5) -> List[DragonStrategy]:
        """选择表现最好的策略保留"""
        # 将策略和分数打包排序
        combined = list(zip(population, scores))
        combined.sort(key=lambda x: x[1], reverse=True)
        
        elite = [item[0] for item in combined[:top_k]]
        return elite

    def _crossover_and_mutate(self, elite: List[DragonStrategy], target_size: int) -> List[DragonStrategy]:
        """通过交叉和变异生成新策略"""
        offspring = []
        while len(elite) + len(offspring) < target_size:
            # 随机选择父母
            parent1 = random.choice(elite)
            parent2 = random.choice(elite)
            
            # 交叉 (Crossover): 混合参数
            child_config = StrategyConfig(
                max_sentiment_score=random.choice([parent1.config.max_sentiment_score, parent2.config.max_sentiment_score]),
                min_amount=random.choice([parent1.config.min_amount, parent2.config.min_amount]),
                stop_loss_atr_multiplier=(parent1.config.stop_loss_atr_multiplier + parent2.config.stop_loss_atr_multiplier) / 2,
                max_holding_days=random.choice([parent1.config.max_holding_days, parent2.config.max_holding_days]),
                # ... 其他参数混合
            )
            
            # 变异 (Mutation): 10% 概率随机调整
            if random.random() < 0.1:
                child_config.stop_loss_atr_multiplier *= random.uniform(0.9, 1.1)
            if random.random() < 0.1:
                child_config.max_holding_days = max(1, child_config.max_holding_days + random.choice([-1, 1]))
                
            offspring.append(DragonStrategy(child_config))
            
        return offspring

import requests
import json
import config

class DeepSeekAnalyzer:
    """
    基于DeepSeek大模型的市场分析器
    用于分析非结构化数据（新闻、评论、研报）
    """
    
    def __init__(self, api_key: str = None):
        self.api_key = api_key or config.DEEPSEEK_API_KEY
        self.base_url = getattr(config, 'DEEPSEEK_BASE_URL', "https://api.deepseek.com")
        self.model = getattr(config, 'DEEPSEEK_MODEL', "deepseek-chat")
        
        if not self.api_key:
            logging.warning("DeepSeek API Key is missing!")

    def _call_api(self, messages: List[Dict], temperature: float = 0.7) -> str:
        """调用 DeepSeek API 的通用方法"""
        if not self.api_key:
            return "Error: API Key Missing"
            
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": self.model,
            "messages": messages,
            "temperature": temperature
        }
        
        try:
            response = requests.post(f"{self.base_url}/chat/completions", headers=headers, json=data, timeout=30)
            response.raise_for_status()
            result = response.json()
            return result['choices'][0]['message']['content']
        except Exception as e:
            logging.error(f"DeepSeek API Call Failed: {e}")
            return f"Error: {str(e)}"
        
    def analyze_sentiment(self, text: str) -> float:
        """
        分析文本情绪得分 (-1.0 to 1.0)
        -1.0: 极度悲观
        0.0: 中性
        1.0: 极度乐观
        """
        prompt = f"""
        请分析以下金融文本的市场情绪。
        文本: "{text}"
        
        请只输出一个介于 -1.0 (极度悲观) 到 1.0 (极度乐观) 之间的浮点数。
        不要输出任何其他文字或解释。
        """
        
        messages = [
            {"role": "system", "content": "你是一个资深的金融情绪分析师，擅长从新闻和研报中量化市场情绪。"},
            {"role": "user", "content": prompt}
        ]
        
        try:
            response = self._call_api(messages, temperature=0.1)
            # 清理可能的非数字字符
            clean_resp = response.strip()
            score = float(clean_resp)
            return max(-1.0, min(1.0, score))
        except ValueError:
            logging.error(f"Failed to parse sentiment score from: {response}")
            return 0.0 # 默认中性
        
    def generate_strategy_insight(self, market_summary: str) -> str:
        """基于市场总结生成策略建议"""
        prompt = f"""
        基于以下市场数据摘要，请给出今日的交易策略建议。
        
        市场摘要:
        {market_summary}
        
        请给出以下方面的建议：
        1. 仓位控制 (0-100%)
        2. 风格偏好 (如：接力、低吸、趋势、空仓)
        3. 重点关注板块
        4. 风险提示
        
        请保持简练、专业，直接给出结论。
        """
        
        messages = [
            {"role": "system", "content": "你是一个激进的短线游资操盘手，擅长捕捉市场热点和情绪周期。"},
            {"role": "user", "content": prompt}
        ]
        
        return self._call_api(messages, temperature=0.7)
