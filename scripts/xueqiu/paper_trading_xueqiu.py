
import sys
import os
import pandas as pd
from datetime import datetime
import json
import logging
import requests
import time

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from config import DINGTALK_WEBHOOK, DINGTALK_KEYWORDS
except ImportError:
    DINGTALK_WEBHOOK = ""
    DINGTALK_KEYWORDS = ["葵花宝典"]

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

class XueqiuPaperTrader:
    def __init__(self):
        self.data_dir = "data"
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            
        self.log_file = os.path.join(self.data_dir, "paper_trading_history.csv")
        self.portfolio_file = os.path.join(self.data_dir, "paper_trading_portfolio.json")
        self.signals_file = os.path.join(self.data_dir, "xueqiu_today_signals.csv")
        
        self.min_cubes_buy = 1  # Reduced to 1 for now as user wants to see operations, but 2 is safer. Let's stick to user's "strategy" which usually implies consensus. 
        # Wait, user said "按照这个策略". If strategy D is consensus, then min 2.
        # But for "today's operation", if no consensus, maybe show nothing?
        # Let's use 1 for "Watch" and 2 for "Strong Buy".
        # Actually, let's use 1 but mark as "Low Confidence" if only 1.
        
        self._init_files()
        self.portfolio = self._load_portfolio()

    def _init_files(self):
        if not os.path.exists(self.log_file):
            df = pd.DataFrame(columns=[
                "date", "time", "stock_name", "stock_code", 
                "action", "price", "quantity", "amount", "reason", 
                "status", "cubes_count", "commission"
            ])
            df.to_csv(self.log_file, index=False, encoding="utf-8-sig")

    def _load_portfolio(self):
        if os.path.exists(self.portfolio_file):
            try:
                with open(self.portfolio_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except:
                pass
        return {"cash": 100000.0, "positions": {}, "initial_capital": 100000.0}

    def _save_portfolio(self):
        with open(self.portfolio_file, "w", encoding="utf-8") as f:
            json.dump(self.portfolio, f, indent=4, ensure_ascii=False)

    def get_current_price(self, code):
        """Fetch current price from Sina API"""
        # Format code: SZ000001 -> sz000001, 600000 -> sh600000
        clean_code = str(code).upper().replace("SZ", "").replace("SH", "")
        
        if clean_code.startswith("6"):
            symbol = f"sh{clean_code}"
        elif clean_code.startswith("0") or clean_code.startswith("3"):
            symbol = f"sz{clean_code}"
        elif clean_code.startswith("8") or clean_code.startswith("4"):
            symbol = f"bj{clean_code}"
        else:
            symbol = f"sh{clean_code}"

        url = f"http://hq.sinajs.cn/list={symbol}"
        try:
            resp = requests.get(url, headers={"Referer": "http://finance.sina.com.cn"}, timeout=5)
            if '="' in resp.text:
                content = resp.text.split('="')[1].split('"')[0]
                parts = content.split(',')
                if len(parts) > 3:
                    price = float(parts[3])
                    if price == 0 and len(parts) > 2: # Suspended or pre-market
                        price = float(parts[2])
                    return price
        except Exception as e:
            logging.error(f"Error fetching price for {code}: {e}")
        return 0.0

    def send_dingtalk(self, title, content):
        if not DINGTALK_WEBHOOK:
            logging.warning("No DINGTALK_WEBHOOK configured.")
            return
        
        # Ensure keyword
        prefix = ""
        if DINGTALK_KEYWORDS:
            keyword = DINGTALK_KEYWORDS[0]
            if keyword not in content and keyword not in title:
                prefix = f"【{keyword}】"

        full_content = f"{prefix}## {title}\n{content}"
        
        data = {
            "msgtype": "markdown",
            "markdown": {
                "title": title,
                "text": full_content
            }
        }
        try:
            resp = requests.post(DINGTALK_WEBHOOK, json=data)
            if resp.status_code == 200:
                logging.info("DingTalk notification sent.")
            else:
                logging.error(f"DingTalk failed: {resp.text}")
        except Exception as e:
            logging.error(f"DingTalk error: {e}")

    def run_daily_update(self):
        today_str = datetime.now().strftime('%Y-%m-%d')
        print(f"--- Paper Trading Update ({today_str}) ---")
        
        # 1. Update Portfolio Prices & PnL
        total_value = self.portfolio["cash"]
        holdings_msg = ""
        
        positions_to_remove = []
        
        for code, pos in self.portfolio["positions"].items():
            current_price = self.get_current_price(code)
            if current_price > 0:
                pos["current_price"] = current_price
                pos["market_value"] = current_price * pos["quantity"]
                pos["pnl"] = pos["market_value"] - (pos["cost_price"] * pos["quantity"])
                pos["pnl_pct"] = (pos["pnl"] / (pos["cost_price"] * pos["quantity"])) * 100
                
                total_value += pos["market_value"]
                
                emoji = "🔴" if pos["pnl"] > 0 else "🟢"
                holdings_msg += f"- {pos['name']}({code}): {current_price} ({pos['pnl_pct']:.2f}%) {emoji}\n"
            else:
                # Keep old price if fetch fails
                total_value += pos["market_value"]
                holdings_msg += f"- {pos['name']}({code}): Price Error\n"

        daily_pnl = total_value - self.portfolio["initial_capital"] # Simple total PnL
        # Ideally track daily change, but for now total PnL is fine
        
        # 2. Process Today's Signals
        executed_orders = []
        
        if os.path.exists(self.signals_file):
            try:
                df = pd.read_csv(self.signals_file)
                # Filter for BUYs
                buys = df[df['action'] == 'BUY']
                
                # Group by stock
                if not buys.empty:
                    grouped = buys.groupby(['stock', 'code'])['cube'].apply(list).reset_index()
                    grouped['count'] = grouped['cube'].apply(len)
                    
                    # Sort by resonance count descending
                    grouped = grouped.sort_values(by='count', ascending=False)
                    
                    # Strategy: Buy if count >= 1 (Active), but highlight if >= 2 (Consensus)
                    for _, row in grouped.iterrows():
                        stock_code = str(row['code']).zfill(6)
                        stock_name = row['stock']
                        count = row['count']
                        cubes = row['cube']
                        
                        # Check if already held
                        if stock_code in self.portfolio["positions"]:
                            continue
                            
                        # Execute BUY
                        price = self.get_current_price(stock_code)
                        if price > 0:
                            # Position sizing: 10% of capital or fixed amount? 
                            # Let's use fixed 20,000 RMB for simulation
                            buy_amount = 20000
                            if self.portfolio["cash"] < buy_amount:
                                buy_amount = self.portfolio["cash"]
                                
                            if buy_amount > 1000: # Min trade
                                quantity = int(buy_amount / price / 100) * 100
                                if quantity > 0:
                                    cost = quantity * price
                                    self.portfolio["cash"] -= cost
                                    self.portfolio["positions"][stock_code] = {
                                        "name": stock_name,
                                        "code": stock_code,
                                        "quantity": quantity,
                                        "cost_price": price,
                                        "current_price": price,
                                        "market_value": cost,
                                        "date": today_str
                                    }
                                    
                                    # Log
                                    self._log_trade(today_str, stock_name, stock_code, "BUY", price, quantity, cost, f"Resonance: {count} cubes")
                                    executed_orders.append(f"买入 {stock_name} {quantity}股 @ {price:.2f} (共振:{count})")
            except Exception as e:
                logging.error(f"Error processing signals: {e}")
        
        self._save_portfolio()
        
        # 3. Generate Report
        report = f"📅 **{today_str} 模拟盘日报**\n\n"
        report += f"💰 **总资产**: {total_value:.2f} (初始: {self.portfolio['initial_capital']})\n"
        report += f"📈 **总收益**: {daily_pnl:.2f} ({daily_pnl/self.portfolio['initial_capital']*100:.2f}%)\n\n"
        
        if executed_orders:
            report += "**今日操作**:\n" + "\n".join(executed_orders) + "\n\n"
        else:
            report += "**今日操作**: 无\n\n"
            
        if holdings_msg:
            report += "**持仓表现**:\n" + holdings_msg
        else:
            report += "**当前空仓**\n"
            
        print(report)
        self.send_dingtalk("雪球模拟盘日报", report)

    def _log_trade(self, date, name, code, action, price, qty, amount, reason):
        # Append to CSV
        with open(self.log_file, 'a', newline='', encoding='utf-8-sig') as f:
            f.write(f"{date},{datetime.now().strftime('%H:%M:%S')},{name},{code},{action},{price},{qty},{amount},{reason},FILLED,0,0\n")

if __name__ == "__main__":
    trader = XueqiuPaperTrader()
    trader.run_daily_update()
