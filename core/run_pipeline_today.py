import sys
import os
import subprocess
import asyncio
from datetime import datetime, timedelta
import pandas as pd

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from core.notifier import DingTalkNotifier
from core.monitor_and_notify import StockMonitor
from core.backtest_engine import BacktestEngine
from core.intraday_validator import IntradaySignalValidator
from core.intraday_trader import PreMarketFilter

def run_extraction():
    print("Step 1: Extracting signals...")
    # Run extract_signals.py as a separate process
    result = subprocess.run([sys.executable, os.path.join(os.path.dirname(__file__), 'extract_signals.py')], 
                          capture_output=True, text=True)
    if result.returncode != 0:
        print("Error extracting signals:")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        return False
    print(result.stdout)
    return True

async def send_plan():
    print("Step 2: Generating and sending plan...")
    today = datetime.now().strftime("%Y-%m-%d")
    daily = await _build_today_signals_from_watchlist(days=7)
    buy_rows = daily.get("buy", [])
    sell_rows = daily.get("sell", [])
    monitor = StockMonitor()

    def _fmt_num(v, digits=2, suffix=""):
        if v is None or pd.isna(v):
            return "--"
        try:
            return f"{float(v):.{digits}f}{suffix}"
        except Exception:
            return f"{v}{suffix}"

    parts = [f"### 今日策略 {today}\n\n"]
    if not buy_rows:
        parts.append("- 今日无买入信号\n")
    else:
        parts.append("#### 买入信号\n")
        seen = set()
        for row in buy_rows:
            code = str(row.get("stock_code"))
            if code in seen:
                continue
            seen.add(code)
            t = str(row.get("date") or "")
            entry_price = _fmt_num(row.get("entry_price"), 2)
            realtime_price = _fmt_num(row.get("realtime_price"), 2)
            pct = _fmt_num(row.get("price_change_pct"), 2, "%")
            score = _fmt_num(row.get("validation_score"), 3)
            name = str(row.get("stock_name"))
            parts.append(
                f"- {name}({code}) 时间 {t} 进场价 {entry_price} 实时价 {realtime_price} 涨跌幅 {pct} 分数 {score}\n"
            )
    if sell_rows:
        parts.append("\n#### 卖出信号\n")
        seen = set()
        for row in sell_rows:
            code = str(row.get("stock_code"))
            if code in seen:
                continue
            seen.add(code)
            t = str(row.get("date") or "")
            realtime_price = _fmt_num(row.get("realtime_price"), 2)
            pct = _fmt_num(row.get("price_change_pct"), 2, "%")
            score = _fmt_num(row.get("validation_score"), 3)
            name = str(row.get("stock_name"))
            parts.append(
                f"- {name}({code}) 时间 {t} 实时价 {realtime_price} 涨跌幅 {pct} 分数 {score}\n"
            )
    parts.append("> 本策略由系统自动生成，仅供参考。")
    plan_text = "".join(parts)

    print("--- Plan Preview ---")
    print(plan_text)
    print("--------------------")

    notifier = DingTalkNotifier()
    try:
        if notifier.webhook:
            title = f"今日策略 {today}"
            success = await notifier.send_markdown(title, plan_text)
            if success:
                print("Successfully sent to DingTalk.")
            else:
                print("Failed to send to DingTalk.")
        else:
            print("DingTalk webhook not configured. Skipping send.")
    finally:
        await notifier.close_session()

def _load_today_buy_signals():
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        df = pd.read_csv(config.SIGNALS_CSV, dtype={"stock_code": str})
    except Exception:
        return []
    if df is None or df.empty:
        return []
    try:
        df = df[df["date"].astype(str).str.startswith(today)]
    except Exception:
        return []
    if "action" not in df.columns:
        return []
    df = df[df["action"] == "BUY"]
    return [row.to_dict() for _, row in df.iterrows()]

def _load_week_watchlist(days=7, actions=None):
    today = datetime.now()
    try:
        df = pd.read_csv(config.SIGNALS_CSV, dtype={"stock_code": str})
    except Exception:
        return []
    if df is None or df.empty:
        return []
    if "date" not in df.columns:
        return []
    df = df.copy()
    df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
    cutoff = today - timedelta(days=int(days))
    df = df[df["date_dt"].notna() & (df["date_dt"] >= cutoff)]
    if "action" in df.columns and actions:
        df = df[df["action"].isin(list(actions))]
    if df.empty:
        return []
    df = df.sort_values(by=["date_dt"], ascending=[False])
    rows = []
    for code, group in df.groupby("stock_code", sort=False):
        row = group.iloc[0].to_dict()
        row["mentions"] = int(len(group))
        row["last_date"] = row.get("date")
        rows.append(row)
    return rows

async def _build_today_signals_from_watchlist(days=7):
    buy_rows = _load_week_watchlist(days=days, actions={"BUY"})
    sell_rows = _load_week_watchlist(days=days, actions={"SELL"})
    if not buy_rows and not sell_rows:
        return {"buy": [], "sell": []}
    monitor = StockMonitor()
    validator = IntradaySignalValidator(
        min_score=getattr(config, "MIN_VALIDATION_SCORE", 0.3),
        max_score=getattr(config, "MAX_VALIDATION_SCORE", 2.0),
    )
    today_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    auction_pool = {}
    require_auction = False
    if buy_rows:
        codes = [str(r.get("stock_code")).zfill(6) for r in buy_rows if r.get("stock_code") is not None]
        try:
            filter = PreMarketFilter()
            filtered, _ = filter.filter_auction_candidates(codes)
            auction_pool = {str(s.get("code")).zfill(6): s for s in filtered} if filtered else {}
            # 如果竞价结果为空且不是因为没有候选股，可能是数据接口挂了，不要强行 require_auction
            if not filtered and codes:
                # 简单判断：如果真的所有都被剔除是可能的，但如果连“被剔除”列表也为空（意味着根本没拿到数据）
                # filter_auction_candidates 返回 (filtered, rejected)
                # 如果两者都空，说明数据获取失败
                if not filter.rejected_stocks:
                    require_auction = False
                else:
                    require_auction = True
            else:
                require_auction = True
        except Exception:
            auction_pool = {}
            require_auction = False
    monitor.auction_pool = auction_pool
    monitor.auction_pool_date = datetime.now().strftime("%Y-%m-%d")
    results_buy = []
    
    # 增加连接状态检查
    print(f"Data Connection Check: Tushare={getattr(config, 'ENABLE_TUSHARE', False)}, Proxy=Disabled")
    
    for row in buy_rows:
        row = dict(row)
        row["action"] = "BUY"
        # 这里的 date 是处理时间，但 _estimate_entry_price 会用 row.get('last_date') 或原始 date
        # 我们需要保留原始信号日期用于计算 entry_price
        if "last_date" in row:
            row["date"] = row["last_date"]
        else:
            row["date"] = today_ts # 如果实在没有，只能用现在，但这会导致 entry==realtime
            
        try:
            validated = validator.validate_single_signal(row)
        except Exception:
            continue
            
        # 估算进场价 (基于历史时间)
        entry_price, entry_source = monitor._estimate_entry_price(validated)
        validated["entry_price"] = entry_price
        validated["entry_source"] = entry_source
        
        # 评估买入 (包含多源实时价格获取 + 进场条件校验)
        # 注意：这里会发起网络请求获取实时价
        ok, reason = await monitor._evaluate_buy_signal(validated, require_auction=require_auction)
        validated["decision_reason"] = reason
        
        # 打印调试信息，方便用户排查
        code = row.get("stock_code")
        dq = validated.get("data_quality_score", 0)
        rt = validated.get("realtime_price", 0)
        ep = validated.get("entry_price", 0)
        print(f"[{code}] Check: OK={ok}, Reason={reason}, Real={rt}, Entry={ep}, Quality={dq}")
        
        if ok:
            results_buy.append(validated)
    results_sell = []
    for row in sell_rows:
        row = dict(row)
        row["action"] = "SELL"
        row["date"] = today_ts
        try:
            validated = validator.validate_single_signal(row)
        except Exception:
            continue
        results_sell.append(validated)
    return {"buy": results_buy, "sell": results_sell}

async def send_today_buy_alerts():
    daily = await _build_today_signals_from_watchlist(days=7)
    rows = daily.get("buy", [])
    if not rows:
        print("No BUY signals today.")
        return
    monitor = StockMonitor()
    sent = 0
    for row in rows:
        ok, reason = await monitor._evaluate_buy_signal(row, require_auction=False)
        row["decision_reason"] = reason
        if ok:
            await monitor.send_alert(row)
            sent += 1
    try:
        await monitor.notifier.close_session()
    except Exception:
        pass
    print(f"Sent {sent} BUY alerts to DingTalk.")

async def send_close_summary_with_pnl():
    today = datetime.now().strftime("%Y-%m-%d")
    daily = await _build_today_signals_from_watchlist(days=7)
    rows = daily.get("buy", [])
    if not rows:
        return
    engine = BacktestEngine()
    notifier = DingTalkNotifier()
    monitor = StockMonitor()
    parts = [f"## 收盘汇总 {today} 实盘模拟\n\n"]
    seen = set()
    unique_rows = []
    for r in rows:
        code_u = str(r.get("stock_code"))
        if code_u in seen:
            continue
        seen.add(code_u)
        unique_rows.append(r)
    total = len(unique_rows)
    executed = 0
    for row in unique_rows:
        code = str(row.get("stock_code"))
        name = str(row.get("stock_name"))
        df_daily = engine.data_provider.get_daily_data(code, today, today)
        close_price = None
        if df_daily is not None and not df_daily.empty and today in df_daily.index:
            try:
                close_price = float(df_daily.loc[today]["close"])
            except Exception:
                pass
        ok, reason = await monitor._evaluate_buy_signal(row, require_auction=False)
        if not ok:
            continue
        entry_price, entry_source = monitor._estimate_entry_price(row)
        if entry_price is None or pd.isna(entry_price):
            continue
        if close_price is None:
            try:
                df_min = engine.data_provider.get_minute_data(code, today)
                if df_min is not None and not df_min.empty:
                    close_price = float(df_min.iloc[-1]["close"])
            except Exception:
                pass
        if close_price is None:
            continue
        pnl_pct = (close_price - float(entry_price)) / float(entry_price) * 100
        executed += 1
        parts.append(f"- {name}({code}) 进场 {float(entry_price):.2f} 收益 {pnl_pct:.2f}%\n")
    if executed == 0:
        parts.append("- 今日无有效成交\n")
    parts.append(f"> 成交 {executed}/{total}\n")
    parts.append("> 来自 BiliStock 监控系统")
    text = "".join(parts)
    await notifier.send_markdown(f"收盘汇总 {today}", text)
    await notifier.close_session()

def _should_today_only():
    return "--today-only" in sys.argv

if __name__ == "__main__":
    if _should_today_only():
        asyncio.run(send_plan())
        asyncio.run(send_today_buy_alerts())
        asyncio.run(send_close_summary_with_pnl())
    else:
        ok = run_extraction()
        if ok:
            asyncio.run(send_plan())
            asyncio.run(send_today_buy_alerts())
            asyncio.run(send_close_summary_with_pnl())
