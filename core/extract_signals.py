import pandas as pd
import json
import re
from datetime import datetime
import os
import importlib.util
from pathlib import Path

try:
    import config
except ImportError:
    _config_path = Path(__file__).resolve().parent.parent / "config.py"
    _spec = importlib.util.spec_from_file_location("config", _config_path)
    if _spec is None or _spec.loader is None:
        raise
    config = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(config)

import random
try:
    from core.bayesian_scorer import CreatorCredibilityScorer
    from core.intraday_validator import IntradaySignalValidator
except ImportError:
    from bayesian_scorer import CreatorCredibilityScorer
    from intraday_validator import IntradaySignalValidator

class SignalExtractor:
    def __init__(self, stock_map_path=config.STOCK_MAP_PATH):
        print(f"Loading stock map from {stock_map_path}...")
        try:
            with open(stock_map_path, 'r', encoding='utf-8') as f:
                self.stock_map = json.load(f)
            print(f"Loaded {len(self.stock_map)} stocks.")
            # Debug: print sample
            print(f"Sample stocks: {list(self.stock_map.keys())[:5]}")
            if "天奇股份" in self.stock_map:
                print("天奇股份 is in stock_map")
            else:
                print("天奇股份 is NOT in stock_map")
        except Exception as e:
            print(f"Error loading stock map: {e}")
            self.stock_map = {}
            
        self.code_map = {v: k for k, v in self.stock_map.items()}
        
        # 关键词定义 (扩展)
        self.buy_keywords = ['买入', '建仓', '上车', '低吸', '抄底', '加仓', '买点', '进场', '新入', '做新', '打板', '接力', '埋伏', '看好', '继续持有', '强势', '拉板', '涨停', '起飞', '梭哈', '满仓', '重点', '核心', '龙头', '主升', '突破', '放量', '金叉', '主升浪', '博弈', '轻仓', '试错', '首板', '连板', '吃板', '吃肉', '封板', '回封']
        self.sell_keywords = ['卖出', '清仓', '止盈', '止损', '下车', '跑路', '减仓', '出货', '离场', '落袋', '清了', '走了', '割肉', '止跌', '回落', '破位', '取关', '核按钮']
        self.negative_words = ['不买', '别买', '谨慎', '观望', '减仓', '止盈', '止损', '跑了', '清了', '不操作', '不追', '不接', '先看', '等待', '放弃']
        self.author_weights = {
            '九哥实盘日记': 0.9,
            '松风论龙头': 0.8,
            '小匠财': 0.7,
            '博主B': 0.6,
            '月影实盘日记': 0.5,
            '行者实盘': 0.9,
            '追涨日记': 0.8,
            'A股妍秘书实盘': 0.8,
            'A股小哥实盘记录': 0.8,
            'A股实盘账操作分享': 0.8,
            '坤哥超短实盘': 0.8,
            '超短实盘记录': 0.8,
            '知行超短实盘': 0.8,
            '18万实盘日记': 0.8,
            '纯阳的500万实盘日记': 0.8,
            '小小小实盘日记': 0.8,
            '小明的实盘日记': 0.8,
            '白鸽的实盘日记': 0.8,
            'Rayson的实盘日记': 0.8,
            '悍匪实盘日记': 0.8,
            '星仔实盘日记': 0.8,
            '涛哥的实盘日记': 0.8,
            '南一环路实盘日记': 0.8,
            '散户每日实盘记录': 0.8,
            '每日实盘实战记录': 0.8,
            '风风每日实盘记录': 0.8,
            '交易实盘日记': 0.8,
            'Nutss的实盘日记': 0.8,
            '阿Y的实盘日记': 0.8,
            '老杜的实盘日记': 0.8,
            '小卢的实盘日记': 0.8,
            '游资老王实盘': 0.8
        }
        self.tier_multipliers = self._load_tier_multipliers()
        self.validator = None
        if getattr(config, "ENABLE_REALTIME_VALIDATION", False):
            try:
                self.validator = IntradaySignalValidator(
                    min_score=getattr(config, "MIN_VALIDATION_SCORE", 0.3),
                    max_score=getattr(config, "MAX_VALIDATION_SCORE", 2.0),
                )
                print("实时BaoStock验证器初始化成功")
            except Exception as e:
                print(f"实时BaoStock验证器初始化失败: {e}")
                self.validator = None

    def _load_tier_multipliers(self):
        path = "data/blogger_tier_list.json"
        if not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            out = {}
            for tier_key in ("tier1", "tier2", "tier3"):
                for item in payload.get(tier_key, []) or []:
                    author = str(item.get("author_name", "")).strip()
                    if not author:
                        continue
                    mult = item.get("weight_multiplier", 1.0)
                    try:
                        out[author] = float(mult)
                    except Exception:
                        out[author] = 1.0
            return out
        except Exception:
            return {}

    def has_negative_words(self, segment):
        return any(w in segment for w in self.negative_words)

    def calculate_signal_strength(self, segment, author_name):
        strength = 0.6
        for k in self.buy_keywords:
            if k in segment:
                strength += 0.1
        for k in self.sell_keywords:
            if k in segment:
                strength += 0.05
        for k in self.negative_words:
            if k in segment:
                strength -= 0.2
        if '涨停' in segment or '拉板' in segment:
            strength += 0.1
        weight = self.author_weights.get(author_name, 0.5)
        strength *= (0.7 + 0.3 * weight)
        multiplier = self.tier_multipliers.get(author_name, 1.0)
        try:
            multiplier = float(multiplier)
        except Exception:
            multiplier = 1.0
        strength *= multiplier
        return max(0.1, min(1.0, strength))
        
    def extract_price(self, text):
        """
        尝试从文本中提取价格
        匹配格式: 12.34, 12块5
        排除百分比
        """
        # 1. 匹配数字+小数 (如 12.34)
        # 排除后面紧跟 % 的情况
        matches = list(re.finditer(r'(\d+\.\d+)(?!%)', text))
        if not matches:
             # 2. 匹配 "xx块xx"
             matches = list(re.finditer(r'(\d+)块(\d*)', text))
             if matches:
                 try:
                     m = matches[0]
                     p = float(m.group(1))
                     if m.group(2):
                         p += float(m.group(2)) / 10 if len(m.group(2)) == 1 else float(m.group(2)) / 100
                     return p
                 except:
                     pass
        
        if matches:
            # 返回第一个匹配到的价格
            # TODO: 优化逻辑，返回离关键词最近的价格
            try:
                return float(matches[0].group(1))
            except:
                return 0.0
        return 0.0

    def find_stocks(self, text):
        found_stocks = []
        # 1. 匹配名称
        for name, code in self.stock_map.items():
            if name in text:
                found_stocks.append({'name': name, 'code': code, 'pos': text.find(name)})
                
        # 2. 匹配代码
        for match in re.finditer(r'\d{6}', text):
            code = match.group()
            name = self.code_map.get(code, 'Unknown')
            if name != 'Unknown':
                found_stocks.append({'name': name, 'code': code, 'pos': match.start()})
            
        # 去重 (保留位置信息)
        # 如果同一个股票出现多次，保留多次？这里简单起见，按位置排序
        found_stocks.sort(key=lambda x: x['pos'])
        return found_stocks

    def determine_action_in_segment(self, segment):
        """
        判断短句中的交易动作
        """
        segment = segment.lower()
        
        if self.has_negative_words(segment):
            return 'NEUTRAL', []

        buy_keywords = [k for k in self.buy_keywords if k in segment]
        sell_keywords = [k for k in self.sell_keywords if k in segment]

        if buy_keywords and not sell_keywords:
            return 'BUY', buy_keywords
        if sell_keywords and not buy_keywords:
            return 'SELL', sell_keywords
        if buy_keywords and sell_keywords:
            first_buy = min([segment.find(k) for k in buy_keywords])
            first_sell = min([segment.find(k) for k in sell_keywords])
            if first_buy <= first_sell:
                return 'BUY', buy_keywords
            return 'SELL', sell_keywords
        return 'NEUTRAL', []

    def load_video_authors(self, video_csv_path):
        """加载视频作者映射 dynamic_id -> author_name"""
        author_map = {}
        try:
            df = pd.read_csv(video_csv_path)
            for _, row in df.iterrows():
                did = str(row.get('dynamic_id', ''))
                author = str(row.get('author_name', 'Unknown'))
                if did:
                    author_map[did] = author
        except FileNotFoundError:
            pass
        return author_map

    def process_videos(self, csv_path):
        try:
            df = pd.read_csv(csv_path)
        except FileNotFoundError:
            return pd.DataFrame()

        signals = []
        
        row_count = 0
        for _, row in df.iterrows():
            row_count += 1
            title = str(row.get('title', ''))
            desc = str(row.get('content', '')) # dataset_videos.csv uses 'content' not 'description'
            if desc == 'nan': desc = ''
            
            # 预处理：将标题和简介合并，并按标点分割
            full_text = f"{title}，{desc}" # 强制加逗号分隔
            
            # 分割成短句
            segments = re.split(r'[，,。？！\s]+', full_text)
            
            # 优先使用 publish_time (字符串), 其次 pubdate (时间戳)
            date_str = str(row.get('publish_time', ''))
            timestamp = 0
            
            # ... (timestamp handling logic kept same or improved)
            if not date_str or date_str == 'nan':
                 # Fallback if publish_time is missing
                 date_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            for seg in segments:
                if not seg.strip():
                    continue
                
                stocks = self.find_stocks(seg)
                
                if not stocks:
                    continue
                    
                action, keywords = self.determine_action_in_segment(seg)
                price = self.extract_price(seg)
                
                for stock in stocks:
                    author_name = row.get('author_name', 'Unknown')
                    strength = self.calculate_signal_strength(seg, author_name)
                    if action in ['BUY', 'SELL'] and strength < 0.5:
                        action = 'NEUTRAL'
                        keywords = []
                    signals.append({
                        'date': date_str,
                        'timestamp': row.get('pubdate', 0), # dataset_videos might not have pubdate, use 0
                        'video_id': row.get('dynamic_id', ''), # dataset_videos uses dynamic_id
                        'author_name': author_name,
                        'stock_name': stock['name'],
                        'stock_code': stock['code'],
                        'action': action,
                        'price': price,
                        'keywords': ','.join(keywords),
                        'source_segment': seg,
                        'source_type': 'video',
                        'strength': strength
                    })
                
        return pd.DataFrame(signals)

    def process_comments(self, csv_path, author_map=None):
        try:
            df = pd.read_csv(csv_path)
        except FileNotFoundError:
            return pd.DataFrame()

        signals = []
        
        for _, row in df.iterrows():
            content = str(row.get('content', ''))
            if content == 'nan': continue
            
            # 分割成短句
            segments = re.split(r'[，,。？！\s]+', content)
            
            date_str = str(row.get('publish_time', ''))
            if not date_str or date_str == 'nan':
                date_str = str(row.get('ctime', datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

            for seg in segments:
                if not seg.strip():
                    continue
                    
                stocks = self.find_stocks(seg)
                if not stocks:
                    continue
                    
                action, keywords = self.determine_action_in_segment(seg)
                price = self.extract_price(seg)
                
                for stock in stocks:
                    # 获取视频作者
                    did = str(row.get('dynamic_id', ''))
                    author = 'Unknown'
                    if author_map and did in author_map:
                        author = author_map[did]
                    strength = self.calculate_signal_strength(seg, author)
                    if action in ['BUY', 'SELL'] and strength < 0.5:
                        action = 'NEUTRAL'
                        keywords = []

                    signals.append({
                        'date': date_str,
                        'timestamp': 0, # Comments usually have ctime string
                        'video_id': did,
                        'author_name': author,
                        'stock_name': stock['name'],
                        'stock_code': stock['code'],
                        'action': action,
                        'price': price,
                        'keywords': ','.join(keywords),
                        'source_segment': seg,
                        'source_type': 'comment',
                        'strength': strength
                    })
        return pd.DataFrame(signals)

    def enhance_signals(self, df_signals: pd.DataFrame) -> pd.DataFrame:
        if df_signals is None or df_signals.empty:
            return df_signals
        if self.validator is None or not getattr(config, "ENABLE_REALTIME_VALIDATION", False):
            return df_signals
        try:
            validated = self.validator.validate_signals(df_signals.to_dict("records"))
            df2 = pd.DataFrame(validated)
            return df2
        except Exception as e:
            print(f"实时验证失败，已降级跳过: {e}")
            return df_signals

    def close(self):
        if self.validator is not None:
            try:
                self.validator.close()
            except Exception:
                pass


if __name__ == "__main__":
    extractor = SignalExtractor()
    scorer = CreatorCredibilityScorer()
    
    # 测试价格提取
    test_str = "我们在10.5元买入，目标12块3"
    print(f"测试提取: '{test_str}' -> 价格: {extractor.extract_price(test_str)}")

    # 加载作者映射
    author_map = extractor.load_video_authors(config.VIDEOS_CSV)
    
    print("提取视频信号...")
    df_video_signals = extractor.process_videos(config.VIDEOS_CSV)
    print("提取评论信号...")
    df_comment_signals = extractor.process_comments(config.COMMENTS_CSV, author_map)
    
    # 合并信号
    df_signals = pd.concat([df_video_signals, df_comment_signals], ignore_index=True)
    
    if not df_signals.empty:
        # 1. 基础清洗：去除空日期
        df_signals = df_signals[df_signals['date'] != 'nan']
        
        # 2. 按时间排序 (倒序，最新的在前面)
        df_signals = df_signals[df_signals['date'] != 'nan'] # Re-check to be safe
        df_signals = df_signals.sort_values(by='date', ascending=False)
        
        # 3. 智能去重：同一个UP主推荐的一只股票一天保留至多两次
        # 提取日期部分 (YYYY-MM-DD)
        df_signals['day'] = df_signals['date'].apply(lambda x: str(x).split(' ')[0])
        
        # 分组并保留前2条
        # group keys: day, stock_code, author_name
        # head(2) 会保留每个组的前2条 (因为已经按时间倒序排列，所以保留的是最新的2条)
        print(f"去重前信号数量: {len(df_signals)}")
        df_signals = df_signals.groupby(['day', 'stock_code', 'author_name']).head(2)
        print(f"去重后信号数量: {len(df_signals)}")
        
        # 移除临时列
        df_signals = df_signals.drop(columns=['day'], errors='ignore')
        
        print(f"共提取到 {len(df_signals)} 个信号 (视频: {len(df_video_signals)}, 评论: {len(df_comment_signals)}):")
        print(df_signals[['date', 'stock_name', 'author_name', 'action', 'price', 'source_segment']].head(10))
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(config.SIGNALS_CSV), exist_ok=True)
        df_signals = scorer.add_scores_to_signals_df(df_signals)
        df_signals = extractor.enhance_signals(df_signals)
        df_signals.to_csv(config.SIGNALS_CSV, index=False, encoding='utf-8-sig')
        print(f"信号已保存至 {config.SIGNALS_CSV}")
    else:
        print("未提取到任何信号。可能是映射表不全或没有明确的买卖关键词。")
