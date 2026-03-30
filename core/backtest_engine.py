import pandas as pd
import akshare as ak
from datetime import datetime, timedelta
import random
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

try:
    from core.data_provider import DataProvider
    from core.strategies import BaseStrategy, DragonStrategy
except ImportError:
    from data_provider import DataProvider
    from strategies import BaseStrategy, DragonStrategy

class BacktestEngine:
    def __init__(self, signals_file=config.SIGNALS_CSV, strategy: BaseStrategy = None):
        """
        回测引擎
        Args:
            signals_file: 信号文件路径
            strategy: 策略实例，默认为 DragonStrategy
        """
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
        
        # 注入策略
        self.strategy = strategy or DragonStrategy()
        
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
            
            # 获取当天数据用于风控
            price_data = self.get_price_data(stock_code, signal_date_str)
            history_df = self.cache.get(stock_code)
            
            # 1. 风控检查
            context_risk = {
                'day_signals': self.signals_raw[self.signals_raw['date'].astype(str).str.startswith(signal_date_str)],
                'code': stock_code,
                'signal_date_str': signal_date_str,
                'price_data': price_data,
                'history_df': history_df
            }
            
            is_safe, risk_reason = self.strategy.check_risk(context_risk)
            
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
            # 确保 T+1 数据存在
            if history_df is None: continue
            
            try:
                if signal_date_str not in history_df.index:
                    results.append({
                        'code': stock_code, 'signal_date': signal_date_str, 'status': 'DATA_MISSING', 
                        'pnl': 0, 'buy_price': 0, 'max_drawdown': 0, 'holding_days': 0
                    })
                    continue

                t_idx = history_df.index.get_loc(signal_date_str)
                if t_idx + 1 >= len(history_df):
                    results.append({
                        'code': stock_code, 'signal_date': signal_date_str, 'status': 'NO_FUTURE_DATA', 
                        'pnl': 0, 'buy_price': 0, 'max_drawdown': 0, 'holding_days': 0
                    })
                    continue
                
                t_plus_1_date = history_df.index[t_idx + 1]
                t_plus_1_row = history_df.iloc[t_idx + 1]
                
                open_p = t_plus_1_row['open']
                pre_close = t_plus_1_row['pre_close']
                
                # 市场风控
                market_safe, market_reason = self.check_market_risk(t_plus_1_date)
                if not market_safe:
                    results.append({
                        'code': stock_code, 'signal_date': signal_date_str, 'status': 'MARKET_RISK',
                        'reason': market_reason, 'buy_date': t_plus_1_date,
                        'pnl': 0, 'buy_price': 0, 'max_drawdown': 0, 'holding_days': 0
                    })
                    continue

                # 3. 开盘竞价策略
                context_auction = {
                    'open_price': open_p,
                    'pre_close': pre_close
                }
                can_participate, auc_status, auc_reason = self.strategy.on_open_auction(context_auction)
                
                if not can_participate:
                    results.append({
                        'code': stock_code, 'signal_date': signal_date_str, 'status': auc_status,
                        'reason': auc_reason, 'buy_date': t_plus_1_date,
                        'pnl': 0, 'buy_price': 0, 'max_drawdown': 0, 'holding_days': 0
                    })
                    continue
                    
                # 4. 盘中择时
                minute_df = self.data_provider.get_minute_data(stock_code, t_plus_1_date)
                context_intraday = {
                    'minute_df': minute_df,
                    'pre_close': pre_close
                }
                
                buy_price, buy_status, buy_reason = self.strategy.on_intraday(context_intraday)
                
                if buy_price:
                    # 5. 持仓与卖出 (Exit Strategy)
                    context_exit = {
                        'history_df': history_df,
                        'buy_date_str': t_plus_1_date,
                        'buy_price': buy_price
                    }
                    sell_date, sell_price, pnl_pct, sell_status, sell_reason = self.strategy.on_exit(context_exit)
                    
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
                import traceback
                traceback.print_exc()
                
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
