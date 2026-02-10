#!/usr/bin/env python3
"""
历史回测脚本 - B站舆情策略 (Gemini Task)
功能：从评论/视频中提取信号，基于策略文档进行模拟交易回测
特点：
1. 信号提取：集成 LLM/OCR 结构化提取 (Gemini) + 关键词正则兜底
2. 时间对齐：早盘/盘中/晚间策略分类
3. 真实行情：使用2026对应日期的历史行情数据进行回测
4. 完整风控：竞价过滤、止损止盈、T+2强制平仓
"""

import pandas as pd
import numpy as np
import datetime
import re
import os
import sys
import json
import time
from typing import Dict, List, Tuple, Optional

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from core.data_provider import DataProvider
from core.llm_processor import LLMProcessor  # New: Integrated LLM Processor

class StrategyBacktester:
    def __init__(self, 
                 comments_file='data/dataset_comments.csv',
                 videos_file='data/dataset_videos.csv',
                 stock_map_file='data/stock_map_final.json',
                 output_file='data/backtest_result_v2.csv'):
        
        self.comments_file = comments_file
        self.videos_file = videos_file
        self.stock_map_file = stock_map_file
        self.output_file = output_file
        
        # Load Stock Map
        self.stock_map = self._load_stock_map()
        self.stock_name_to_code = {v: k for k, v in self.stock_map.items()} 
        
        # Initialize DataProvider
        self.data_provider = DataProvider()
        
        # Initialize LLM Processor
        self.llm_processor = LLMProcessor()
        
        # Cache for market data
        self.data_cache = {}
        self.minute_cache = {}
        
    def _load_stock_map(self):
        try:
            with open(self.stock_map_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading stock map: {e}")
            return {}

    def _get_daily_df(self, code: str, year: int) -> Optional[pd.DataFrame]:
        cache_key = f"{str(code).zfill(6)}:{year}"
        if cache_key in self.data_cache:
            return self.data_cache.get(cache_key)

        start_dt = f"{year}-01-01"
        end_dt = f"{year}-12-31"
        try:
            df = self.data_provider.get_daily_data(code, start_dt, end_dt)
        except Exception as e:
            print(f"Error fetching daily data for {code}: {e}")
            df = None

        if df is None or df.empty:
            self.data_cache[cache_key] = None
            return None

        self.data_cache[cache_key] = df
        return df

    def _align_to_trading_day(self, df: pd.DataFrame, date_str: str) -> Optional[str]:
        if df is None or df.empty:
            return None
        try:
            idx = df.index
            if date_str in idx:
                return date_str
            pos = idx.searchsorted(date_str)
            if pos >= len(idx):
                return None
            return str(idx[pos])
        except Exception:
            return None

    def _slice_trading_days(self, df: pd.DataFrame, start_date_str: str, count: int = 3) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
        if df is None or df.empty:
            return None, None
        aligned = self._align_to_trading_day(df, start_date_str)
        if not aligned:
            return None, None
        try:
            start_pos = df.index.get_loc(aligned)
            return df.iloc[start_pos:start_pos + count].copy(), aligned
        except Exception:
            return None, None

    def _get_minute_df(self, code: str, date_str: str) -> Optional[pd.DataFrame]:
        cache_key = f"{str(code).zfill(6)}:{date_str}"
        if cache_key in self.minute_cache:
            return self.minute_cache.get(cache_key)
        try:
            df = self.data_provider.get_minute_data(code, date_str)
        except Exception as e:
            print(f"Error fetching minute data for {code} on {date_str}: {e}")
            df = None
        self.minute_cache[cache_key] = df
        return df

    def load_and_parse_signals(self):
        """Load data and extract signals"""
        print("Loading datasets...")
        signals = []
        
        # 1. Process Comments
        if os.path.exists(self.comments_file):
            df_c = pd.read_csv(self.comments_file)
            print(f"Loaded {len(df_c)} comments.")
            for _, row in df_c.iterrows():
                # 使用 LLM Processor 或 Regex 提取
                # 优先尝试 Regex 提取作为基准，若有 LLM 资源可替换
                extracted = self._extract_signal_from_text(row['content'], row['publish_time'], source='comment')
                if extracted:
                    signals.extend(extracted)
                    
        # 2. Process Videos
        if os.path.exists(self.videos_file):
            try:
                df_v = pd.read_csv(self.videos_file)
                print(f"Loaded {len(df_v)} videos.")
                text_cols = [c for c in df_v.columns if c in ['title', 'description', 'dynamic_text', 'content']]
                for _, row in df_v.iterrows():
                    full_text = " ".join([str(row[c]) for c in text_cols])
                    extracted = self._extract_signal_from_text(full_text, row['publish_time'], source='video')
                    if extracted:
                        signals.extend(extracted)
            except Exception as e:
                print(f"Error reading videos: {e}")

        # Deduplicate and Sort
        df_signals = pd.DataFrame(signals)
        if not df_signals.empty:
            df_signals['publish_time'] = pd.to_datetime(df_signals['publish_time'])
            df_signals = df_signals.sort_values('publish_time')
            df_signals['date_str'] = df_signals['publish_time'].dt.strftime('%Y-%m-%d')
            df_signals = df_signals.drop_duplicates(subset=['stock_code', 'date_str', 'strategy_type'])
            print(f"Total unique signals extracted: {len(df_signals)}")
        else:
            print("No signals extracted.")
            
        return df_signals

    def _extract_signal_from_text(self, text, publish_time, source='unknown'):
        """Extract stock and intent from text"""
        if not isinstance(text, str): return []
        
        # 1. 使用 LLM Processor 解析 (模拟调用)
        # 这里实际上会调用 regex fallback，除非配置了 API
        llm_signals = self.llm_processor.parse_trading_signal(text, publish_time)
        
        # 2. 如果 LLM/Regex 返回了信号，我们需要补全 strategy_type 和 trade_date
        results = []
        
        try:
            dt = pd.to_datetime(publish_time)
            t = dt.time()
            
            # Logic:
            # 00:00 - 09:25 -> Early Morning (T)
            # 09:30 - 14:50 -> Intraday (T)
            # 15:00 - 23:59 -> Evening (T+1)
            
            if t < datetime.time(9, 25):
                strategy = "EARLY_MORNING"
                trade_date = dt.date()
            elif t < datetime.time(14, 50):
                strategy = "INTRADAY"
                trade_date = dt.date()
            else:
                strategy = "EVENING"
                trade_date = dt.date() + datetime.timedelta(days=1)
            
            # 如果 LLM 解析出了代码
            if llm_signals:
                for sig in llm_signals:
                    if sig.get('action') == 'BUY':
                        # 查找股票名称
                        stock_name = "Unknown"
                        for name, code in self.stock_map.items():
                            if code == sig['stock_code']:
                                stock_name = name
                                break
                                
                        results.append({
                            'stock_code': sig['stock_code'],
                            'stock_name': stock_name,
                            'publish_time': publish_time,
                            'source_text': text[:50] + "...",
                            'strategy_type': strategy,
                            'trade_date': trade_date.strftime('%Y-%m-%d'),
                            'source_type': source,
                            # 关键：透传 LLM 提取的具体价格指令
                            'entry_price_min': sig.get('entry_price_min'),
                            'entry_price_max': sig.get('entry_price_max'),
                            'stop_loss_price': sig.get('stop_loss_price'),
                            'target_price': sig.get('target_price')
                        })
            
            # 如果没有信号，或者是旧的正则逻辑 (作为双重保险)
            if not results:
                # Original fallback logic
                keywords = ['买入', '新入', '关注', '低吸', '计划', '建仓', '上车']
                if not any(k in text for k in keywords):
                    return []
                
                found_stocks = []
                for name in sorted(self.stock_map.keys(), key=len, reverse=True):
                    if len(name) > 1 and name in text:
                        found_stocks.append((name, self.stock_map[name]))
                codes = re.findall(r'(?<!\d)\d{6}(?!\d)', text)
                for code in codes:
                    if code.startswith(('0', '3', '6', '4', '8')):
                        found_stocks.append(('Code:'+code, code))
                
                for name, code in set(found_stocks):
                    results.append({
                        'stock_code': code,
                        'stock_name': name,
                        'publish_time': publish_time,
                        'source_text': text[:50] + "...",
                        'strategy_type': strategy,
                        'trade_date': trade_date.strftime('%Y-%m-%d'),
                        'source_type': source,
                        'entry_price_min': None,
                        'entry_price_max': None,
                        'stop_loss_price': None,
                        'target_price': None
                    })
                    
            return results
            
        except Exception as e:
            print(f"Error parsing time {publish_time}: {e}")
            return []

    def get_market_data(self, code, date_str):
        """Fetch real historical data for a specific trade date (T, T+1, T+2)."""
        try:
            trade_dt = pd.to_datetime(date_str)
        except Exception:
            return None, None

        df_daily = self._get_daily_df(code, int(trade_dt.year))
        if df_daily is None or df_daily.empty:
            return None, None

        sliced, aligned = self._slice_trading_days(df_daily, trade_dt.strftime("%Y-%m-%d"), count=3)
        return sliced, aligned

    def execute_strategy(self, signals_df):
        """Execute trading logic"""
        print("Executing Strategy...")
        results = []
        
        total = len(signals_df)
        for i, row in signals_df.iterrows():
            if i % 10 == 0: print(f"Processing {i}/{total}...", end='\r')
            
            code = row['stock_code']
            trade_date = row['trade_date']
            strategy = row['strategy_type']
            
            # Specific Instructions from LLM
            instr_entry_min = row.get('entry_price_min')
            instr_entry_max = row.get('entry_price_max')
            instr_sl = row.get('stop_loss_price')
            instr_tp = row.get('target_price')
            
            # Get Market Data (T, T+1, T+2)
            df_price, entry_date = self.get_market_data(code, trade_date)
            if df_price is None or df_price.empty or not entry_date:
                results.append({**row, 'status': 'NO_DATA', 'pnl_pct': 0})
                continue
                
            t_row = df_price.iloc[0]
            
            # 1. Anti-Manipulation / Risk Filter
            if t_row['amount'] < 20000000:
                results.append({**row, 'status': 'SKIP_LIQUIDITY', 'entry_reason': 'Liquidity<20M', 'pnl_pct': 0})
                continue
                
            # 2. Entry Logic
            entry_price = 0.0
            entry_reason = ""
            entry_time = None
            
            # Call Auction Check
            if strategy in ['EARLY_MORNING', 'EVENING']:
                open_p = t_row['open']
                pre_c = t_row['pre_close']
                if pre_c == 0: pre_c = open_p 
                open_pct = (open_p - pre_c) / pre_c
                
                # Filter Extreme Opens
                if open_pct > 0.07:
                    results.append({**row, 'status': 'SKIP_HIGH_OPEN', 'entry_reason': f'HighOpen {open_pct:.1%}', 'pnl_pct': 0})
                    continue
                if open_pct < -0.03:
                    results.append({**row, 'status': 'SKIP_LOW_OPEN', 'entry_reason': f'LowOpen {open_pct:.1%}', 'pnl_pct': 0})
                    continue
                    
                entry_price = open_p
                entry_reason = "Call Auction Open"
                
            elif strategy == 'INTRADAY':
                # Try to use Minute Data
                minute_df = self._get_minute_df(code, entry_date)
                publish_dt = pd.to_datetime(row.get('publish_time'))
                
                if minute_df is not None and not minute_df.empty and 'time' in minute_df.columns:
                    dfm = minute_df.copy()
                    dfm['time'] = pd.to_datetime(dfm['time'])
                    start_time = publish_dt.floor('min')
                    dfm = dfm[dfm['time'] >= start_time]
                    picked = None
                    
                    # Enhanced Intraday Logic
                    for _, mr in dfm.iterrows():
                        current_p = float(mr['close'])
                        
                        # A. Specific Price Instruction (Highest Priority)
                        if instr_entry_max and instr_entry_min:
                            if instr_entry_min <= current_p <= instr_entry_max:
                                picked = mr
                                entry_reason = f"Instruction Match [{instr_entry_min}-{instr_entry_max}]"
                                break
                        elif instr_entry_max:
                            if current_p <= instr_entry_max:
                                picked = mr
                                entry_reason = f"Instruction Match [<{instr_entry_max}]"
                                break
                                
                        # B. Generic Logic (VWAP Breakout)
                        try:
                            if float(mr['close']) > float(mr['vwap']):
                                picked = mr
                                entry_reason = "VWAP Breakout"
                                break
                        except:
                            continue
                            
                    if picked is None:
                        # Fallback: if instructed price never hit
                        if instr_entry_max:
                            results.append({**row, 'status': 'MISS', 'entry_reason': 'Price Not Reached', 'pnl_pct': 0})
                            continue
                        # Fallback for generic
                        results.append({**row, 'status': 'MISS', 'entry_reason': 'No VWAP Break', 'pnl_pct': 0})
                        continue
                        
                    entry_price = float(picked['close'])
                    entry_time = picked['time']
                else:
                    # No minute data available
                    if t_row['volume'] > 0:
                        entry_price = float(t_row['amount']) / float(t_row['volume'])
                        entry_reason = "Intraday VWAP (Daily)"
                    else:
                        entry_price = float(t_row['close'])
                        entry_reason = "Intraday Close"
            
            # 3. Holding & Exit Logic (T to T+2)
            exit_price = 0.0
            exit_reason = ""
            exit_date = ""
            
            # Dynamic Stop Loss / Take Profit
            # If instruction exists, use it. Otherwise use optimized generic params.
            if instr_sl and instr_sl > 0:
                stop_loss_price = instr_sl
            else:
                stop_loss_price = entry_price * 0.97  # Tighter SL (3% vs 5%)
                
            if instr_tp and instr_tp > 0:
                take_profit_price = instr_tp
            else:
                take_profit_price = entry_price * 1.08 # 8% TP
            
            triggered = False
            start_day_idx = 0
            if strategy == 'INTRADAY':
                start_day_idx = 1 # Start checking exit from next day (unless we want intraday exit?)
                # Actually, for intraday buy, we should also check if it hits SL later that same day
                # But our daily data check loop starts from index 0 which is T. 
                # Let's check T day low/high for SL/TP first if it's Intraday
                
                day_data = df_price.iloc[0]
                # Conservative: check if Low < SL (assume we might have hit it)
                # But we don't know if Low happened after Entry. 
                # Ideally check minute data, but for simplicity:
                # If Close < SL, we definitely stopped out.
                if day_data['close'] < stop_loss_price:
                     exit_price = day_data['close']
                     exit_reason = "Intraday Stop Loss"
                     exit_date = day_data.name
                     triggered = True
            
            if not triggered:
                for day_idx in range(start_day_idx, len(df_price)):
                    day_data = df_price.iloc[day_idx]
                    curr_date = day_data.name 
    
                    # Check Low for SL
                    if day_data['low'] < stop_loss_price:
                        # Gap Down check
                        if day_data['open'] < stop_loss_price:
                            exit_price = day_data['open']
                            exit_reason = "Stop Loss (Gap Down)"
                        else:
                            exit_price = stop_loss_price
                            exit_reason = "Stop Loss"
                        exit_date = curr_date
                        triggered = True
                        break
                        
                    # Check High for TP
                    if day_data['high'] > take_profit_price:
                        # Gap Up check
                        if day_data['open'] > take_profit_price:
                            exit_price = day_data['open']
                            exit_reason = "Take Profit (Gap Up)"
                        else:
                            exit_price = take_profit_price
                            exit_reason = "Take Profit"
                        exit_date = curr_date
                        triggered = True
                        break
    
                    # Time Exit (End of T+2)
                    if day_idx == 2:
                        exit_price = day_data['close']
                        exit_reason = "Time Exit (T+2)"
                        exit_date = curr_date
                        triggered = True
                        break
            
            # If not triggered (e.g. ran out of data)
            if not triggered:
                last_row = df_price.iloc[-1]
                exit_price = last_row['close']
                exit_reason = "End of Data Exit"
                exit_date = last_row.name
            
            # Calculate PnL
            if entry_price > 0:
                pnl_pct = (exit_price - entry_price) / entry_price * 100
            else:
                pnl_pct = 0.0
                
            results.append({
                **row,
                'status': 'EXECUTED',
                'entry_date': entry_date,
                'entry_price': round(entry_price, 2),
                'exit_date': exit_date,
                'exit_price': round(exit_price, 2),
                'pnl_pct': round(pnl_pct, 2),
                'entry_reason': entry_reason,
                'exit_reason': exit_reason
            })
            
        print("\nExecution complete.")
        return pd.DataFrame(results)

    def run(self):
        # 1. Load
        df_signals = self.load_and_parse_signals()
        if df_signals.empty:
            print("No signals to backtest.")
            return
            
        # 2. Execute
        df_results = self.execute_strategy(df_signals)
        
        # 3. Report
        if not df_results.empty:
            cols = ['stock_code', 'stock_name', 'publish_time', 'strategy_type', 
                    'entry_date', 'entry_price', 'exit_date', 'exit_price', 
                    'pnl_pct', 'entry_reason', 'exit_reason', 'status', 'source_text']
            final_cols = [c for c in cols if c in df_results.columns]
            df_final = df_results[final_cols]
            
            df_final.to_csv(self.output_file, index=False)
            print(f"Report saved to {self.output_file}")
            
            # Summary
            executed = df_final[df_final['status'] == 'EXECUTED']
            if not executed.empty:
                print("\n=== Performance Summary ===")
                print(f"Total Trades: {len(executed)}")
                print(f"Win Rate: {(executed['pnl_pct'] > 0).mean():.2%}")
                print(f"Avg PnL: {executed['pnl_pct'].mean():.2f}%")
                print(f"Max Profit: {executed['pnl_pct'].max():.2f}%")
                print(f"Max Loss: {executed['pnl_pct'].min():.2f}%")
        else:
            print("No results generated.")

if __name__ == "__main__":
    backtester = StrategyBacktester()
    backtester.run()
