import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
import sys
import os
import random

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from core.data_provider import DataProvider

class BacktestEngine:
    def __init__(self, signals_file=config.SIGNALS_CSV):
        self.signals_raw = pd.read_csv(signals_file)
        self.signals_raw['date'] = pd.to_datetime(self.signals_raw['date'])
        self.signals_raw['stock_code'] = self.signals_raw['stock_code'].astype(str).str.zfill(6)
        self.signals_raw = self.signals_raw.sort_values('date')
        self.signals = self.signals_raw.copy()
        self.signals['date_only'] = self.signals['date'].dt.strftime('%Y-%m-%d')
        self.signals = self.signals.drop_duplicates(subset=['date_only', 'stock_code'], keep='first')
        self.signals = self.signals.drop(columns=['date_only'])
        self.cache = {} # 简单的内存缓存
        self.data_provider = DataProvider()
        
    def add_prefix(self, code):
        """为代码添加 sh/sz 前缀 (Legacy helper, mostly handled in DataProvider)"""
        code = str(code).zfill(6)
        if code.startswith('6'):
            return f"sh{code}"
        elif code.startswith('0') or code.startswith('3'):
            return f"sz{code}"
        elif code.startswith('8') or code.startswith('4'):
            return f"bj{code}"
        return code

    def get_price_data(self, code, date_str):
        """
        获取特定日期的价格数据，包含前一日收盘价。
        使用 DataProvider 获取真实数据，严禁 Mock。
        """
        # 1. 检查缓存 (Cache uses daily dataframe)
        code_str = str(code).zfill(6)
        
        if code_str not in self.cache:
            print(f"Fetching daily data for {code_str}...")
            # Fetch range: 2024-2026 to cover recent history
            df = self.data_provider.get_daily_data(code_str, "2024-01-01", "2026-12-31")
            self.cache[code_str] = df
            
        df = self.cache.get(code_str)
        
        if df is not None and date_str in df.index:
            row = df.loc[date_str]
            pre_close = row.get('pre_close')
            if pd.isna(pre_close):
                pre_close = row['open'] 

            return {
                'open': float(row['open']),
                'close': float(row['close']),
                'high': float(row['high']),
                'low': float(row['low']),
                'pre_close': float(pre_close),
                'volume': float(row['volume']),
                'amount': float(row['amount']),
                'turn': float(row['turn']),
                'pctChg': float(row['pctChg']),
                'is_mock': False
            }
            
        # Strict Mode: No Mock
        print(f"Error: No real data for {code_str} on {date_str}. Skipping.")
        return None

    def check_risk_control(self, code, signal_date_str):
        """
        反操纵与合规风控 (Anti-Manipulation)
        """
        # 1. 情绪过热反指
        try:
            day_signals = self.signals_raw[self.signals_raw['date'].astype(str).str.startswith(signal_date_str)]
            authors = day_signals[day_signals['stock_code'].astype(str).str.zfill(6) == str(code).zfill(6)]['author_name'].unique()
            if len(authors) > 5:
                return False, f"情绪过热: {len(authors)}位博主同推"
        except Exception as e:
            # print(f"Error checking sentiment for {code}: {e}")
            pass
            
        # Get Price Data
        price_data = self.get_price_data(code, signal_date_str)
        
        if price_data is None:
            return False, "SKIP (No Data)"
            
        # 2. 流动性过滤
        amount = price_data['amount']
        if amount < 30000000: # 3000万
            return False, f"流动性不足:成交额{amount/10000:.0f}万<3000万"
            
        turn = price_data['turn']
        if turn > 0:
            circ_mv = amount / (turn / 100)
            if circ_mv < 2000000000: # 20亿
                return False, f"小市值:流通市值{circ_mv/100000000:.1f}亿<20亿"
                
        # 3. 短期暴涨过滤 (前3天累计涨幅)
        code_str = str(code).zfill(6)
        df = self.cache.get(code_str)
        
        if df is not None:
            try:
                if signal_date_str in df.index:
                    idx = df.index.get_loc(signal_date_str)
                    if idx >= 3:
                        close_t = df.iloc[idx]['close']
                        close_prev_3 = df.iloc[idx - 3]['close']
                        pct_3d = (close_t - close_prev_3) / close_prev_3 * 100
                        if pct_3d > 20.0:
                            return False, f"高位接盘风险:3日涨幅{pct_3d:.1f}%>20%"
            except Exception as e:
                pass
            
        return True, "SAFE"

    def check_call_auction_strategy(self, open_price, pre_close):
        """
        开盘竞价策略 (9:15-9:25)
        """
        open_pct = (open_price - pre_close) / pre_close * 100
        
        # 1. 集合竞价高开 > 7% -> 放弃 (防止骗线)
        if open_pct > 7.0:
            return False, "SKIP_HIGH_OPEN", f"高开幅度过大({open_pct:.2f}%)"
            
        # 2. 集合竞价低开 < -3% -> 放弃 (弱势)
        if open_pct < -3.0:
            return False, "SKIP_LOW_OPEN", f"低开幅度过大({open_pct:.2f}%)"
            
        return True, "READY", f"竞价符合预期({open_pct:.2f}%)"

    def check_intraday_strategy(self, code, date_str, pre_close):
        """
        盘中择时与均价策略 (VWAP & Smart Beta)
        """
        df = self.data_provider.get_minute_data(code, date_str)
        if df is None or df.empty:
            return None, "NO_DATA", "无法获取分钟数据"

        for i in range(len(df)):
            row = df.iloc[i]
            price = row['close']
            vwap = row['vwap']
            if price > vwap and not self._is_limit_up(price, pre_close):
                buy_price = price
                buy_time = str(row['time'])
                return buy_price, "BUY_VWAP", f"盘中择时: {buy_time} Close({buy_price})>VWAP({vwap:.2f})"

        return None, "MISS", "未触发买入条件"

    def _is_limit_up(self, price, pre_close):
        if pre_close is None or pre_close == 0:
            return False
        return price >= pre_close * 1.099

    def _is_limit_down(self, open_p, low_p, close_p, pre_close):
        if pre_close is None or pre_close == 0:
            return False
        if open_p == low_p == close_p and close_p <= pre_close * 0.902:
            return True
        return (close_p - pre_close) / pre_close <= -0.098

    def calculate_atr(self, code, date_str, period=14):
        code_str = str(code).zfill(6)
        df = self.cache.get(code_str)
        
        if df is None: return None
        if date_str not in df.index: return None
            
        try:
            idx = df.index.get_loc(date_str)
            if idx < period: return None
                
            start_idx = idx - period - 1
            if start_idx < 0: start_idx = 0
            
            subset = df.iloc[start_idx : idx + 1].copy()
            
            subset['h-l'] = subset['high'] - subset['low']
            subset['h-cp'] = abs(subset['high'] - subset['close'].shift(1))
            subset['l-cp'] = abs(subset['low'] - subset['close'].shift(1))
            subset['tr'] = subset[['h-l', 'h-cp', 'l-cp']].max(axis=1)
            
            atr = subset['tr'].rolling(window=period).mean().iloc[-1]
            return atr
            
        except Exception as e:
            # print(f"Error calculating ATR for {code}: {e}")
            return None

    def run_exit_strategy(self, code, buy_date_str, buy_price):
        """
        执行卖出与风控策略 (Exit & Risk)
        """
        code_str = str(code).zfill(6)
        df = self.cache.get(code_str)
        
        if df is None:
             return None, buy_price, 0, "ERROR", "无历史数据"
             
        # 找到买入日之后的交易日
        all_dates = df.index.tolist()
        try:
            start_pos = all_dates.index(buy_date_str)
        except ValueError:
            return None, buy_price, 0, "ERROR", "买入日期缺失数据"
            
        # 初始 ATR
        atr = self.calculate_atr(code, buy_date_str)
        if atr is None:
            atr = buy_price * 0.03 # 默认 3% 波动率
            
        initial_stop_loss = buy_price - 2 * atr
        stop_loss = initial_stop_loss
        
        remaining_ratio = 1.0
        realized_pnl_amount = 0.0
        
        sell_logs = [] 
        
        # 遍历买入日之后的每一天
        for i in range(start_pos + 1, len(all_dates)):
            curr_date = all_dates[i]
            row = df.loc[curr_date]
            
            open_p = row['open']
            high_p = row['high']
            low_p = row['low']
            close_p = row['close']
            pre_close = df.iloc[i - 1]['close'] if i - 1 >= 0 else None
            
            days_held = i - start_pos

            if self._is_limit_down(open_p, low_p, close_p, pre_close):
                sell_logs.append((curr_date, close_p, 0, "一字跌停无法卖出"))
                continue
            
            # 更新止损线 (Trailing Stop)
            if close_p > buy_price:
                new_stop = close_p - 2 * atr
                if new_stop > stop_loss:
                    stop_loss = new_stop
            
            # 1. 开盘止盈 (仅 T+1)
            if days_held == 1:
                open_change = (open_p - buy_price) / buy_price
                if open_change > 0.05:
                    # 卖出 50%
                    sell_ratio = 0.5 * remaining_ratio
                    realized_pnl_amount += sell_ratio * (open_p - buy_price)
                    remaining_ratio -= sell_ratio
                    sell_logs.append((curr_date, open_p, sell_ratio, "开盘止盈(>5%)"))
            
            # 2. ATR 止损检查 (Intraday Low)
            if low_p < stop_loss:
                exec_price = close_p
                sell_ratio = remaining_ratio
                realized_pnl_amount += sell_ratio * (exec_price - buy_price)
                remaining_ratio = 0
                sell_logs.append((curr_date, exec_price, sell_ratio, f"ATR止损(Low{low_p:.2f}<Stop{stop_loss:.2f})"))
                break
            
            # 3. 时间止损 (持有满 3 天 且 浮亏)
            curr_float_pnl = (close_p - buy_price) / buy_price
            if days_held >= 3 and curr_float_pnl < 0:
                sell_ratio = remaining_ratio
                realized_pnl_amount += sell_ratio * (close_p - buy_price)
                remaining_ratio = 0
                sell_logs.append((curr_date, close_p, sell_ratio, "时间止损(3日浮亏)"))
                break
                
            if remaining_ratio <= 0:
                break
        
        # 如果回测结束仍持有
        if remaining_ratio > 0:
            last_date = all_dates[-1]
            last_close = df.iloc[-1]['close']
            realized_pnl_amount += remaining_ratio * (last_close - buy_price)
            sell_logs.append((last_date, last_close, remaining_ratio, "回测结束强制平仓"))
            
        total_profit_pct = (realized_pnl_amount / buy_price) * 100
        final_sell_date = sell_logs[-1][0] if sell_logs else buy_date_str
        final_sell_price = sell_logs[-1][1] if sell_logs else buy_price
        final_reason = " | ".join([log[3] for log in sell_logs]) if sell_logs else "Holding"
        
        return final_sell_date, final_sell_price, total_profit_pct, "SOLD", final_reason

    def _calc_max_drawdown(self, code, buy_date_str, sell_date_str, buy_price):
        code_str = str(code).zfill(6)
        df = self.cache.get(code_str)
        if df is None:
            return 0.0
        if buy_date_str not in df.index or sell_date_str not in df.index:
            return 0.0
        try:
            start = df.index.get_loc(buy_date_str)
            end = df.index.get_loc(sell_date_str)
            if end < start:
                return 0.0
            subset = df.iloc[start:end + 1]
            min_low = subset['low'].min()
            if pd.isna(min_low):
                return 0.0
            dd = (min_low - buy_price) / buy_price * 100
            return min(0.0, dd)
        except Exception:
            return 0.0

    def _calc_holding_days(self, code, buy_date_str, sell_date_str):
        code_str = str(code).zfill(6)
        df = self.cache.get(code_str)
        if df is None:
            return 0
        if buy_date_str not in df.index or sell_date_str not in df.index:
            return 0
        try:
            start = df.index.get_loc(buy_date_str)
            end = df.index.get_loc(sell_date_str)
            if end < start:
                return 0
            return int(end - start)
        except Exception:
            return 0

    def check_market_risk(self, date_str):
        market = self.data_provider.get_index_data(date_str)
        if market is None:
            return True, "MARKET_DATA_MISSING"
        pct = market.get('pctChg')
        if pct is None:
            return True, "MARKET_DATA_MISSING"
        if pct <= -3.0:
            return False, f"市场系统性风险: 指数跌幅{pct:.2f}%"
        return True, "MARKET_SAFE"

    def run_backtest(self, max_days=10): 
        print("Starting Backtest (REAL DATA ONLY)...")
        results = []
        
        processed_count = 0
        
        for idx, row in self.signals.iterrows():
            if processed_count >= max_days: break
            
            stock_code = str(row['stock_code']).zfill(6)
            signal_date = row['date']
            signal_date_str = signal_date.strftime('%Y-%m-%d')
            
            print(f"\nProcessing Signal: {stock_code} on {signal_date_str}")
            
            # 1. 风控检查
            is_safe, risk_reason = self.check_risk_control(stock_code, signal_date_str)
            if not is_safe:
                results.append({
                    'code': stock_code,
                    'signal_date': signal_date_str,
                    'status': 'RISK_REJECT',
                    'reason': risk_reason,
                    'pnl': 0,
                        'buy_price': 0,
                        'max_drawdown': 0,
                        'holding_days': 0
                })
                continue
                
            # 2. 获取 T+1 日数据进行买入尝试
            price_data = self.get_price_data(stock_code, signal_date_str)
            if price_data is None:
                results.append({
                    'code': stock_code, 
                    'signal_date': signal_date_str,
                    'status': 'DATA_MISSING', 
                    'pnl': 0,
                    'buy_price': 0,
                    'max_drawdown': 0,
                    'holding_days': 0
                })
                continue
                
            df_daily = self.cache.get(stock_code)
            if df_daily is None: continue
            
            try:
                if signal_date_str not in df_daily.index:
                    results.append({
                        'code': stock_code,
                        'signal_date': signal_date_str,
                        'status': 'DATA_MISSING',
                        'pnl': 0,
                        'buy_price': 0,
                        'max_drawdown': 0,
                        'holding_days': 0
                    })
                    continue

                t_idx = df_daily.index.get_loc(signal_date_str)
                if t_idx + 1 >= len(df_daily):
                    results.append({
                        'code': stock_code, 
                        'signal_date': signal_date_str,
                        'status': 'NO_FUTURE_DATA', 
                        'pnl': 0,
                        'buy_price': 0,
                        'max_drawdown': 0,
                        'holding_days': 0
                    })
                    continue
                
                t_plus_1_date = df_daily.index[t_idx + 1]
                t_plus_1_row = df_daily.iloc[t_idx + 1]
                
                open_p = t_plus_1_row['open']
                pre_close = t_plus_1_row['pre_close']
                
                market_safe, market_reason = self.check_market_risk(t_plus_1_date)
                if not market_safe:
                    results.append({
                        'code': stock_code,
                        'signal_date': signal_date_str,
                        'status': 'MARKET_RISK',
                        'reason': market_reason,
                        'buy_date': t_plus_1_date,
                        'pnl': 0,
                        'buy_price': 0,
                        'max_drawdown': 0,
                        'holding_days': 0
                    })
                    continue

                # 3. 开盘竞价策略
                can_participate, auc_status, auc_reason = self.check_call_auction_strategy(open_p, pre_close)
                if not can_participate:
                    results.append({
                        'code': stock_code,
                        'signal_date': signal_date_str,
                        'status': auc_status,
                        'reason': auc_reason,
                        'buy_date': t_plus_1_date,
                        'pnl': 0,
                        'buy_price': 0,
                        'max_drawdown': 0,
                        'holding_days': 0
                    })
                    continue
                    
                # 4. 盘中择时
                buy_price, buy_status, buy_reason = self.check_intraday_strategy(stock_code, t_plus_1_date, pre_close)
                
                if buy_price:
                    # 5. 持仓与卖出 (Exit Strategy)
                    sell_date, sell_price, pnl_pct, sell_status, sell_reason = self.run_exit_strategy(stock_code, t_plus_1_date, buy_price)
                    max_drawdown = self._calc_max_drawdown(stock_code, t_plus_1_date, sell_date, buy_price)
                    holding_days = self._calc_holding_days(stock_code, t_plus_1_date, sell_date)
                    
                    results.append({
                        'code': stock_code,
                        'signal_date': signal_date_str,
                        'buy_date': t_plus_1_date,
                        'buy_price': buy_price,
                        'sell_date': sell_date,
                        'sell_price': sell_price,
                        'pnl': pnl_pct,
                        'max_drawdown': max_drawdown,
                        'holding_days': holding_days,
                        'status': 'EXECUTED',
                        'reason': f"{buy_reason} -> {sell_reason}"
                    })
                else:
                    results.append({
                        'code': stock_code,
                        'signal_date': signal_date_str,
                        'status': buy_status,
                        'reason': buy_reason,
                        'pnl': 0,
                        'buy_price': 0,
                        'max_drawdown': 0,
                        'holding_days': 0
                    })
                    
            except Exception as e:
                print(f"Error processing {stock_code}: {e}")
                
            processed_count += 1
            
        # 生成报告
        df_res = pd.DataFrame(results)
        if not df_res.empty:
            print("\nBacktest Results Summary:")
            print(df_res[['code', 'signal_date', 'status', 'pnl', 'reason']])
            df_res.to_csv(config.BACKTEST_REPORT, index=False)
            print(f"Report saved to {config.BACKTEST_REPORT}")
            
            # Stats
            if 'buy_price' in df_res.columns:
                executed = df_res[df_res['status'] == 'EXECUTED']
                if not executed.empty:
                    win_rate = (executed['pnl'] > 0).mean()
                    avg_pnl = executed['pnl'].mean()
                    avg_holding = executed['holding_days'].mean() if 'holding_days' in executed.columns else 0
                    max_dd = executed['max_drawdown'].min() if 'max_drawdown' in executed.columns else 0
                    print(f"Win Rate: {win_rate:.2%}, Avg PnL: {avg_pnl:.2f}%, Max DD: {max_dd:.2f}%, Avg Hold Days: {avg_holding:.2f}")
            self._append_summary_to_report(df_res)
        else:
            print("No results generated.")

    def _append_summary_to_report(self, df_res: pd.DataFrame):
        try:
            executed = df_res[df_res['status'] == 'EXECUTED']
            total = len(df_res)
            exec_count = len(executed)
            win_rate = (executed['pnl'] > 0).mean() if exec_count else 0
            avg_pnl = executed['pnl'].mean() if exec_count else 0
            max_dd = executed['max_drawdown'].min() if exec_count else 0
            avg_holding = executed['holding_days'].mean() if exec_count else 0
            summary = pd.DataFrame([{
                "code": "SUMMARY",
                "signal_date": datetime.now().strftime("%Y-%m-%d"),
                "status": f"TOTAL_{total}_EXEC_{exec_count}",
                "pnl": avg_pnl,
                "buy_price": "",
                "sell_price": "",
                "max_drawdown": max_dd,
                "holding_days": avg_holding,
                "reason": f"WIN_RATE_{win_rate:.4f}",
            }])
            df_out = pd.concat([df_res, summary], ignore_index=True)
            df_out.to_csv(config.BACKTEST_REPORT, index=False)
        except Exception:
            return

if __name__ == "__main__":
    engine = BacktestEngine()
    engine.run_backtest()
