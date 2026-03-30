
import json
import os
import sqlite3
import pandas as pd
import baostock as bs
import logging
import requests
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PaperAccountManager:
    def __init__(self, account_file="data/paper_account.json"):
        self.account_file = account_file
        self.load_account()

    def load_account(self):
        if os.path.exists(self.account_file):
            with open(self.account_file, 'r', encoding='utf-8') as f:
                self.data = json.load(f)
        else:
            self.data = {
                "cash": 100000.0,
                "positions": {}, # {symbol: {cost_price, shares, entry_date, days_held}}
                "history": []
            }
            self.save_account()

    def save_account(self):
        with open(self.account_file, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=4, ensure_ascii=False)

    def update_holding_days(self):
        """Increment holding days for all positions."""
        for symbol in self.data["positions"]:
            self.data["positions"][symbol]["days_held"] += 1
        self.save_account()

    def execute_buy(self, symbol, price, shares, date):
        cost = price * shares
        if self.data["cash"] < cost:
            logging.warning(f"Not enough cash to buy {symbol}")
            return False
        
        self.data["cash"] -= cost
        self.data["positions"][symbol] = {
            "cost_price": price,
            "shares": shares,
            "entry_date": date,
            "days_held": 0
        }
        self.save_account()
        return True

    def execute_sell(self, symbol, price, date, reason):
        if symbol not in self.data["positions"]:
            return None
        
        pos = self.data["positions"].pop(symbol)
        revenue = price * pos["shares"]
        self.data["cash"] += revenue
        
        pnl = revenue - (pos["cost_price"] * pos["shares"])
        if pos["cost_price"] > 0:
            pnl_pct = pnl / (pos["cost_price"] * pos["shares"])
        else:
            pnl_pct = 0
        
        record = {
            "symbol": symbol,
            "entry_date": pos["entry_date"],
            "exit_date": date,
            "entry_price": pos["cost_price"],
            "exit_price": price,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
            "reason": reason
        }
        self.data["history"].append(record)
        self.save_account()
        return record

class DailySignalEngine:
    def __init__(self, db_path="data/cubes.db"):
        self.db_path = db_path

    def get_todays_signals(self):
        """Calculate Rolling Consensus for TODAY."""
        conn = sqlite3.connect(self.db_path)
        # Load recent history (last 5 days to be safe for 3-day rolling)
        # Note: In production, we need to ensure DB is up to date.
        # Assuming fetch_history_retry.py ran before this.
        
        query = """
        SELECT * FROM rebalancing_history 
        WHERE created_at >= date('now', '-5 days')
        """
        try:
            df = pd.read_sql_query(query, conn)
        except Exception as e:
            logging.error(f"DB Error: {e}")
            conn.close()
            return pd.DataFrame()
        conn.close()
        
        if df.empty:
            logging.warning("No recent rebalancing data found in DB.")
            return pd.DataFrame()
            
        # Preprocess
        if 'date' not in df.columns and 'created_at' in df.columns:
            df['date'] = df['created_at']
        
        df['date'] = pd.to_datetime(df['date'], format='mixed', errors='coerce').dt.date
        df = df.dropna(subset=['date'])
        
        # Filter A-Shares
        df = df[df['stock_symbol'].astype(str).str.match(r'^(SH|SZ)\d{6}$', na=False)]
        
        # Calculate Action
        df['weight_delta'] = df['target_weight'] - df['prev_weight_adjusted']
        df['action'] = df['weight_delta'].apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
        
        # Group Daily
        daily_scores = df.groupby(['date', 'stock_symbol'])['action'].sum().reset_index()
        daily_scores.rename(columns={'action': 'daily_score', 'stock_symbol': 'symbol'}, inplace=True)
        
        # Rolling
        pivot = daily_scores.pivot(index='date', columns='symbol', values='daily_score').fillna(0)
        rolling = pivot.rolling(window=3, min_periods=1).sum()
        
        # Get Latest Day Signals
        if rolling.empty: return pd.DataFrame()
        
        last_date = rolling.index[-1]
        today_signals = rolling.loc[last_date].reset_index()
        today_signals.columns = ['symbol', 'consensus_score']
        
        # Filter: Rolling >= 2 AND Daily > 0 (Today must have action)
        # Need today's daily score
        if last_date in pivot.index:
            daily_now = pivot.loc[last_date].reset_index()
            daily_now.columns = ['symbol', 'daily_score']
            today_signals = pd.merge(today_signals, daily_now, on='symbol')
            
            final_signals = today_signals[
                (today_signals['consensus_score'] >= 2) & 
                (today_signals['daily_score'] > 0)
            ].copy()
            
            return final_signals.sort_values('consensus_score', ascending=False)
            
        return pd.DataFrame()

class DailyTraderBot:
    def __init__(self):
        self.account = PaperAccountManager()
        self.signal_engine = DailySignalEngine()
        self.today = datetime.now().strftime("%Y-%m-%d")

    def get_market_price(self, symbol):
        """Get latest closing price from BaoStock (Robust)."""
        bs.login()
        # Convert SH600000 -> sh.600000
        code = symbol.lower().replace('sh', 'sh.').replace('sz', 'sz.')
        
        # Try fetching last 5 days to ensure we get a price even if today is missing/holiday
        start_date = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")
        
        rs = bs.query_history_k_data_plus(
            code, "date,close", 
            start_date=start_date, end_date=end_date, 
            frequency="d", adjustflag="3"
        )
        bs.logout()
        
        data_list = []
        while rs.error_code == '0' and rs.next():
            data_list.append(rs.get_row_data())
            
        if data_list:
            # Return the last available close
            return float(data_list[-1][1])
            
        return None

    def run_daily_cycle(self):
        logging.info(f"--- 🤖 Running Daily Bot for {self.today} ---")
        
        plan = {
            "sells": [],
            "buys": [],
            "holdings_update": []
        }
        
        # 1. Update Holdings State (Mark-to-Market & Stop Loss Check)
        logging.info("Checking Holdings...")
        for symbol, pos in list(self.account.data["positions"].items()):
            # Check Time Exit
            if pos["days_held"] >= 3:
                plan["sells"].append({
                    "symbol": symbol,
                    "reason": "TimeExit (T+4)",
                    "shares": pos["shares"]
                })
                # Simulate Sell Execution (Assuming executed tomorrow open, but for state tracking we mark it pending)
                # In this simplified bot, we update JSON immediately assuming "Next Open" execution
                # self.account.execute_sell(symbol, pos["cost_price"], self.today, "TimeExit") 
                # Wait, real bot generates PLAN. Execution happens next day?
                # The user said: "自动把买卖记录更新到 JSON 中，模拟推进时间"
                # So we assume we execute these TOMORROW OPEN.
                # But to keep state clean, we remove them now or mark them?
                # Let's execute them in JSON now using "Last Close" as proxy for "Next Open" to keep it moving.
                continue

            # Check Hard Stop
            price = self.get_market_price(symbol)
            if price:
                pct_change = (price - pos["cost_price"]) / pos["cost_price"]
                if pct_change < -0.06:
                    plan["sells"].append({
                        "symbol": symbol,
                        "reason": f"StopLoss ({pct_change*100:.1f}%)",
                        "shares": pos["shares"]
                    })
                else:
                    plan["holdings_update"].append(f"{symbol}: {pct_change*100:.1f}% (Day {pos['days_held']})")
            else:
                logging.warning(f"Could not get price for {symbol}")

        # 2. Generate Buy Signals
        logging.info("Generating Buy Signals...")
        signals = self.signal_engine.get_todays_signals()
        
        if not signals.empty:
            # Filter existing holdings
            candidates = signals[~signals['symbol'].isin(self.account.data["positions"].keys())]
            top_buys = candidates.head(3)
            
            for _, row in top_buys.iterrows():
                plan["buys"].append({
                    "symbol": row['symbol'],
                    "score": row['consensus_score'],
                    "reason": "Rolling Consensus"
                })
        
        # 3. Execute Updates in JSON (Simulating Time Passing)
        # Execute Sells
        for item in plan["sells"]:
            # Use cost price as placeholder if no market price, or fetch real price
            price = self.get_market_price(item["symbol"]) or 0
            self.account.execute_sell(item["symbol"], price, self.today, item["reason"])
            
        # Execute Buys
        # Assume buying with 10% cash each
        for item in plan["buys"]:
            price = self.get_market_price(item["symbol"])
            if price:
                # 10% of CURRENT cash (after sells)
                target_amt = self.account.data["cash"] * 0.1
                shares = int(target_amt / price / 100) * 100
                if shares >= 100:
                    self.account.execute_buy(item["symbol"], price, shares, self.today)
                    item["shares"] = shares # Update plan for report
        
        # Increment Days Held for remaining positions
        self.account.update_holding_days()
        
        # 4. Notify
        self.send_notification(plan)

    def send_notification(self, plan):
        msg = f"## 🤖 Daily Trading Plan ({self.today})\n\n"
        
        msg += "### 🔴 Sell Orders (Tomorrow Open)\n"
        if plan["sells"]:
            for s in plan["sells"]:
                msg += f"- **{s['symbol']}**: {s['reason']}\n"
        else:
            msg += "- No sells.\n"
            
        msg += "\n### 🟢 Buy Orders (Tomorrow Open)\n"
        if plan["buys"]:
            for b in plan["buys"]:
                shares = b.get("shares", "N/A")
                msg += f"- **{b['symbol']}** (Score: {b['score']:.1f}): Buy ~{shares} shares\n"
        else:
            msg += "- No buys.\n"
            
        msg += "\n### 💼 Current Holdings\n"
        if plan["holdings_update"]:
            for h in plan["holdings_update"]:
                msg += f"- {h}\n"
        else:
            msg += "- Empty.\n"
            
        msg += f"\n**Cash**: {self.account.data['cash']:.2f}"
        
        print(msg)
        # Placeholder for DingTalk
        # requests.post(webhook_url, json={"msgtype": "markdown", "markdown": {"title": "Trading Plan", "text": msg}})

if __name__ == "__main__":
    bot = DailyTraderBot()
    bot.run_daily_cycle()
