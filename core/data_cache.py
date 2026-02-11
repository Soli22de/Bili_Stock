import sqlite3
import pandas as pd
import os
import logging
from datetime import datetime
from typing import Optional

class DataCache:
    """
    本地数据缓存系统 (SQLite)
    用于存储分钟级K线数据，减少网络请求并加速回测
    """
    
    def __init__(self, db_path="data/stock_data.db"):
        self.db_path = db_path
        self.logger = logging.getLogger("DataCache")
        self._init_db()
        
    def _init_db(self):
        """初始化数据库表结构"""
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 分钟线表 (包含 1min, 5min 等)
        # code: 股票代码 (6位)
        # date: 日期 (YYYY-MM-DD)
        # time: 时间 (HH:MM:SS)
        # period: 周期 (1, 5, 15, 30, 60)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS minute_data (
                code TEXT,
                date TEXT,
                time TEXT,
                period INTEGER,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                amount REAL,
                vwap REAL,
                PRIMARY KEY (code, date, time, period)
            )
        ''')
        
        # 索引加速查询
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_code_date ON minute_data (code, date)')
        
        conn.commit()
        conn.close()
        
    def save_minute_data(self, df: pd.DataFrame, code: str, period: int = 5):
        """
        保存分钟数据到缓存
        Args:
            df: 包含 time, open, high, low, close, volume, (amount, vwap 可选) 的 DataFrame
            code: 股票代码
            period: 周期 (默认 5)
        """
        if df is None or df.empty:
            return
            
        conn = sqlite3.connect(self.db_path)
        
        try:
            # 准备数据
            data_to_insert = []
            for _, row in df.iterrows():
                # 解析日期和时间
                # 假设 time 格式为 'YYYY-MM-DD HH:MM:SS' 或 'YYYYMMDDHHMMSS'
                try:
                    ts = pd.to_datetime(row['time'])
                    date_str = ts.strftime('%Y-%m-%d')
                    time_str = ts.strftime('%H:%M:%S')
                except:
                    # 如果只有时间没有日期，通常不应发生，因为 df 应该是单日的或带日期的
                    continue
                
                # 计算 VWAP (如果不存在)
                vwap = row.get('vwap', 0.0)
                if vwap == 0.0 and row.get('volume', 0) > 0 and row.get('amount', 0) > 0:
                    vwap = row['amount'] / row['volume']
                elif vwap == 0.0:
                    vwap = row['close'] # 降级处理
                    
                data_to_insert.append((
                    str(code).zfill(6),
                    date_str,
                    time_str,
                    period,
                    row['open'],
                    row['high'],
                    row['low'],
                    row['close'],
                    row['volume'],
                    row.get('amount', 0.0),
                    vwap
                ))
            
            # 批量插入 (REPLACE INTO 处理重复)
            conn.executemany('''
                REPLACE INTO minute_data 
                (code, date, time, period, open, high, low, close, volume, amount, vwap)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', data_to_insert)
            
            conn.commit()
            # self.logger.info(f"Saved {len(data_to_insert)} rows for {code} to cache.")
            
        except Exception as e:
            self.logger.error(f"Failed to save cache for {code}: {e}")
        finally:
            conn.close()
            
    def load_minute_data(self, code: str, date_str: str, period: int = 5) -> Optional[pd.DataFrame]:
        """
        从缓存加载分钟数据
        """
        conn = sqlite3.connect(self.db_path)
        
        try:
            query = '''
                SELECT time, open, high, low, close, volume, amount, vwap
                FROM minute_data
                WHERE code = ? AND date = ? AND period = ?
                ORDER BY time ASC
            '''
            
            df = pd.read_sql_query(query, conn, params=(str(code).zfill(6), date_str, period))
            
            if df.empty:
                return None
                
            # 还原时间列格式 (合并 date 和 time 供 Engine 使用)
            # Engine 期望 'time' 列包含完整 datetime 或 string
            # 这里我们简单地返回 HH:MM:SS，因为 Engine 主要是盘中择时
            # 为了兼容性，我们重新构建完整的 datetime 字符串
            df['time'] = pd.to_datetime(date_str + ' ' + df['time'])
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to load cache for {code}: {e}")
            return None
        finally:
            conn.close()

    def has_data(self, code: str, date_str: str, period: int = 5) -> bool:
        """检查是否存在数据"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            'SELECT 1 FROM minute_data WHERE code = ? AND date = ? AND period = ? LIMIT 1', 
            (str(code).zfill(6), date_str, period)
        )
        exists = cursor.fetchone() is not None
        conn.close()
        return exists
