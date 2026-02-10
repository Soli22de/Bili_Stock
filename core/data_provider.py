import pandas as pd
import sys
import os
import datetime
import time

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import os
import requests

class DataProvider:
    def __init__(self):
        self.ts_pro = None
        self.bs_logged_in = False
        
        if getattr(config, "DISABLE_PROXY", False):
            try:
                orig = requests.Session.request
                def patched(self, method, url, *args, **kwargs):
                    kwargs["proxies"] = {"http": None, "https": None}
                    return orig(self, method, url, *args, **kwargs)
                requests.Session.request = patched
                os.environ["http_proxy"] = ""
                os.environ["https_proxy"] = ""
                os.environ["HTTP_PROXY"] = ""
                os.environ["HTTPS_PROXY"] = ""
            except Exception:
                pass
        
        # Initialize Tushare
        enable_tushare = getattr(config, "ENABLE_TUSHARE", False)
        if enable_tushare and hasattr(config, 'TUSHARE_TOKEN') and config.TUSHARE_TOKEN:
            try:
                import tushare as ts
                ts.set_token(config.TUSHARE_TOKEN)
                self.ts_pro = ts.pro_api()
                print("Tushare Pro initialized.")
            except ImportError:
                print("Tushare not installed. Please `pip install tushare`.")
            except Exception as e:
                print(f"Tushare init failed: {e}")
        else:
            print("Tushare disabled or token not found. Using BaoStock/AkShare fallback.")

        # Initialize BaoStock
        self._login_baostock()

    def _login_baostock(self):
        try:
            import baostock as bs
            lg = bs.login()
            if lg.error_code == '0':
                self.bs_logged_in = True
                print("BaoStock logged in successfully.")
            else:
                print(f"BaoStock login failed: {lg.error_msg}")
        except ImportError:
            print("BaoStock not installed. Please `pip install baostock`.")
        except Exception as e:
            print(f"BaoStock init failed: {e}")

    def __del__(self):
        if self.bs_logged_in:
            try:
                import baostock as bs
                bs.logout()
            except:
                pass

    def _normalize_code(self, code):
        """Standardize code for internal use (e.g. 600000)"""
        return str(code).zfill(6)

    def _to_ts_code(self, code):
        """Convert to Tushare format: 600000.SH"""
        c = self._normalize_code(code)
        if c.startswith('6'): return f"{c}.SH"
        if c.startswith('0') or c.startswith('3'): return f"{c}.SZ"
        if c.startswith('8') or c.startswith('4'): return f"{c}.BJ"
        return c

    def _to_bs_code(self, code):
        """Convert to BaoStock format: sh.600000"""
        c = self._normalize_code(code)
        if c.startswith('6'): return f"sh.{c}"
        if c.startswith('0') or c.startswith('3'): return f"sz.{c}"
        if c.startswith('8') or c.startswith('4'): return f"bj.{c}"
        return f"sh.{c}" # default

    def get_minute_data(self, code, date_str):
        """
        Get minute-level data (1min or 5min).
        Returns DataFrame with columns: ['time', 'open', 'high', 'low', 'close', 'volume', 'amount', 'vwap']
        """
        df = None
        
        # Strategy A: Tushare (1min)
        enable_tushare_minute = getattr(config, "ENABLE_TUSHARE_MINUTE", True)
        if self.ts_pro and enable_tushare_minute:
            try:
                ts_code = self._to_ts_code(code)
                # Tushare expects start_date/end_date as YYYY-MM-DD HH:MM:SS or YYYYMMDD
                # stk_mins limit: single fetch max 8000 rows. One day is 240 rows.
                start_dt = f"{date_str} 09:30:00"
                end_dt = f"{date_str} 15:00:00"
                
                df = self.ts_pro.stk_mins(ts_code=ts_code, start_date=start_dt, end_date=end_dt, freq='1min')
                if df is not None and not df.empty:
                    # Tushare cols: trade_time, open, close, high, low, vol, amount, ...
                    # Sort by time
                    df = df.sort_values('trade_time').reset_index(drop=True)
                    df = df.rename(columns={
                        'trade_time': 'time',
                        'vol': 'volume'
                    })
                    print(f"Tushare 1min data fetched for {code} on {date_str} ({len(df)} rows)")
            except Exception as e:
                print(f"Tushare minute data failed: {e}")

        # Strategy B: BaoStock (5min) - Fallback
        if (df is None or df.empty) and self.bs_logged_in:
            try:
                import baostock as bs
                bs_code = self._to_bs_code(code)
                # frequency="5"
                rs = bs.query_history_k_data_plus(bs_code,
                    "date,time,open,high,low,close,volume,amount",
                    start_date=date_str, end_date=date_str,
                    frequency="5", adjustflag="3")
                
                if rs.error_code == '0':
                    data_list = []
                    while rs.next():
                        data_list.append(rs.get_row_data())
                    
                    if data_list:
                        df = pd.DataFrame(data_list, columns=rs.fields)
                        # Convert types
                        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'amount']
                        for col in numeric_cols:
                            df[col] = pd.to_numeric(df[col])
                            
                        # Format time: BaoStock time is YYYYMMDDHHMMSSssss
                        # We need YYYY-MM-DD HH:MM:SS
                        # Actually standard bs time format for 5min is YYYYMMDDHHMMSS000
                        # Example: 20250101093500000 -> 2025-01-01 09:35:00
                        
                        def fmt_time(t):
                            t = str(t)
                            if len(t) >= 14:
                                return f"{t[0:4]}-{t[4:6]}-{t[6:8]} {t[8:10]}:{t[10:12]}:{t[12:14]}"
                            return t
                            
                        df['time'] = df['time'].apply(fmt_time)
                        print(f"BaoStock 5min data fetched for {code} on {date_str} ({len(df)} rows)")
            except Exception as e:
                print(f"BaoStock minute data failed: {e}")

        if df is None or df.empty:
            print(f"No minute data found for {code} on {date_str}. Returning empty.")
            return None

        # Calculate VWAP
        # VWAP = Cumulative(Price * Volume) / Cumulative(Volume)
        # Ensure necessary columns exist
        if 'amount' not in df.columns:
            df['amount'] = df['close'] * df['volume'] # Estimate if missing
            
        # Tushare/BaoStock 'amount' might be string or float, ensure float
        df['amount'] = pd.to_numeric(df['amount'])
        df['volume'] = pd.to_numeric(df['volume'])
        
        # Calculate intraday VWAP
        df['cum_amount'] = df['amount'].cumsum()
        df['cum_vol'] = df['volume'].cumsum()
        df['vwap'] = df['cum_amount'] / df['cum_vol']
        
        return df

    def get_daily_data(self, code, start_date, end_date):
        """
        Get daily data with risk indicators (turnover, amount).
        Prioritize BaoStock for reliable turnover/amount.
        """
        df = None
        
        # 1. BaoStock
        if self.bs_logged_in:
            try:
                import baostock as bs
                bs_code = self._to_bs_code(code)
                rs = bs.query_history_k_data_plus(bs_code,
                    "date,open,high,low,close,volume,amount,turn,pctChg",
                    start_date=start_date, end_date=end_date,
                    frequency="d", adjustflag="3")
                
                if rs.error_code == '0':
                    data_list = []
                    while rs.next():
                        data_list.append(rs.get_row_data())
                    
                    if data_list:
                        df = pd.DataFrame(data_list, columns=rs.fields)
                        for col in ['open', 'high', 'low', 'close', 'volume', 'amount', 'turn', 'pctChg']:
                            df[col] = pd.to_numeric(df[col])
                        df.set_index('date', inplace=True)
                        print(f"BaoStock daily data fetched for {code} ({len(df)} rows)")
            except Exception as e:
                print(f"BaoStock daily data failed: {e}")

        # 2. AkShare (Fallback)
        if df is None or df.empty:
            if self.ts_pro:
                try:
                    ts_code = self._to_ts_code(code)
                    s_dt = start_date.replace('-', '')
                    e_dt = end_date.replace('-', '')
                    df_ts = self.ts_pro.daily(ts_code=ts_code, start_date=s_dt, end_date=e_dt)
                    if df_ts is not None and not df_ts.empty:
                        df_ts = df_ts.copy()
                        # Convert trade_date to YYYY-MM-DD index
                        df_ts['date'] = pd.to_datetime(df_ts['trade_date'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
                        df_ts = df_ts.rename(columns={
                            'open': 'open',
                            'high': 'high',
                            'low': 'low',
                            'close': 'close',
                            'vol': 'volume',
                            'amount': 'amount',
                            'pct_chg': 'pctChg'
                        })
                        df_ts.set_index('date', inplace=True)
                        df = df_ts[['open','high','low','close','volume','amount','pctChg']].astype(float)
                        print(f"Tushare daily data fetched for {code} ({len(df)} rows)")
                except Exception as e:
                    print(f"Tushare daily data failed: {e}")
        if df is None or df.empty:
            try:
                import akshare as ak
                ak_code = self._normalize_code(code)
                # ak.stock_zh_a_daily often fails or changes, use spot_em or hist
                # stock_zh_a_hist usually reliable
                # start_date for ak is YYYYMMDD
                s_dt = start_date.replace('-', '')
                e_dt = end_date.replace('-', '')
                
                df_ak = ak.stock_zh_a_hist(symbol=ak_code, start_date=s_dt, end_date=e_dt, adjust="qfq")
                if df_ak is not None and not df_ak.empty:
                    # Rename columns
                    # 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
                    df_ak = df_ak.rename(columns={
                        '日期': 'date', '开盘': 'open', '收盘': 'close', 
                        '最高': 'high', '最低': 'low', '成交量': 'volume', 
                        '成交额': 'amount', '换手率': 'turn', '涨跌幅': 'pctChg'
                    })
                    df_ak['date'] = df_ak['date'].astype(str)
                    df_ak.set_index('date', inplace=True)
                    df = df_ak
                    print(f"AkShare daily data fetched for {code} ({len(df)} rows)")
            except Exception as e:
                print(f"AkShare daily data failed: {e}")
                
        if df is not None:
            # Post-processing
            if 'pre_close' not in df.columns:
                df['pre_close'] = df['close'].shift(1)
                # First row pre_close approximation
                df.iloc[0, df.columns.get_loc('pre_close')] = df.iloc[0]['open'] 
            
            # Ensure turn/amount are present
            if 'turn' not in df.columns: df['turn'] = 0.0
            if 'amount' not in df.columns: df['amount'] = df['volume'] * df['close']
            
        return df

    def get_index_data(self, date_str, index_code="000001.SH"):
        if self.ts_pro:
            try:
                df = self.ts_pro.index_daily(ts_code=index_code, start_date=date_str.replace('-', ''), end_date=date_str.replace('-', ''))
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    return {
                        'open': float(row.get('open')),
                        'high': float(row.get('high')),
                        'low': float(row.get('low')),
                        'close': float(row.get('close')),
                        'pctChg': float(row.get('pct_chg'))
                    }
            except Exception:
                pass

        if self.bs_logged_in:
            try:
                import baostock as bs
                rs = bs.query_history_k_data_plus("sh.000001",
                    "date,open,high,low,close,pctChg",
                    start_date=date_str, end_date=date_str,
                    frequency="d", adjustflag="3")
                if rs.error_code == '0':
                    data_list = []
                    while rs.next():
                        data_list.append(rs.get_row_data())
                    if data_list:
                        df = pd.DataFrame(data_list, columns=rs.fields)
                        for col in ['open', 'high', 'low', 'close', 'pctChg']:
                            df[col] = pd.to_numeric(df[col])
                        row = df.iloc[0]
                        return {
                            'open': float(row.get('open')),
                            'high': float(row.get('high')),
                            'low': float(row.get('low')),
                            'close': float(row.get('close')),
                            'pctChg': float(row.get('pctChg'))
                        }
            except Exception:
                pass
        return None
