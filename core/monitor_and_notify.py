import asyncio
import pandas as pd
import os
import json
import logging
from datetime import datetime, timedelta
import hashlib
import sys
import os

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入我们的模块
from core.bili_collector import BiliCollector
from core.extract_signals import SignalExtractor
from core.notifier import DingTalkNotifier
from core.bayesian_scorer import CreatorCredibilityScorer
from core.ocr_validation import merge_ocr_results
import config
from core.data_provider import DataProvider
from core.risk_engine import SimpleRiskManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/monitor.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# 状态文件，用于记录已经推送过的信号，防止重复推送
STATE_FILE = "data/monitor_state.json"

from core.realtime_market import get_market_validator

class StockMonitor:
    def __init__(self):
        self.collector = BiliCollector()
        self.extractor = SignalExtractor(config.STOCK_MAP_PATH)
        self.scorer = CreatorCredibilityScorer()
        self.notifier = DingTalkNotifier(config.DINGTALK_WEBHOOK, config.DINGTALK_SECRET)
        self.state = self.load_state()
        self.processed_hashes = self.state["processed_hashes"]
        self.daily_signals = self.state["daily_signals"]
        self.last_summary_date = self.state["last_summary_date"]
        self.morning_pool = self.state["morning_pool"]
        self.morning_pool_date = self.state["morning_pool_date"]
        self.auction_pool = self.state["auction_pool"]
        self.auction_pool_date = self.state["auction_pool_date"]
        self.last_morning_summary_date = self.state["last_morning_summary_date"]
        self.paper_positions = self.state["paper_positions"]
        self.paper_trades = self.state["paper_trades"]
        self.market_validator = get_market_validator()
        self.data_provider = DataProvider()
        self._paper_daily_cache = {}

    def load_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    payload = json.load(f)
                    if isinstance(payload, list):
                        return {
                            "processed_hashes": set(payload),
                            "daily_signals": {},
                            "last_summary_date": None,
                            "morning_pool": {},
                            "morning_pool_date": None,
                            "auction_pool": {},
                            "auction_pool_date": None,
                            "last_morning_summary_date": None,
                            "paper_positions": [],
                            "paper_trades": [],
                        }
                    if isinstance(payload, dict):
                        hashes = set(payload.get("processed_hashes", []) or [])
                        daily = payload.get("daily_signals", {}) or {}
                        last_summary = payload.get("last_summary_date")
                        morning_pool = payload.get("morning_pool", {}) or {}
                        morning_pool_date = payload.get("morning_pool_date")
                        auction_pool = payload.get("auction_pool", {}) or {}
                        auction_pool_date = payload.get("auction_pool_date")
                        last_morning_summary_date = payload.get("last_morning_summary_date")
                        paper_positions = payload.get("paper_positions", []) or []
                        paper_trades = payload.get("paper_trades", []) or []
                        return {
                            "processed_hashes": hashes,
                            "daily_signals": daily,
                            "last_summary_date": last_summary,
                            "morning_pool": morning_pool,
                            "morning_pool_date": morning_pool_date,
                            "auction_pool": auction_pool,
                            "auction_pool_date": auction_pool_date,
                            "last_morning_summary_date": last_morning_summary_date,
                            "paper_positions": paper_positions,
                            "paper_trades": paper_trades,
                        }
            except:
                return {
                    "processed_hashes": set(),
                    "daily_signals": {},
                    "last_summary_date": None,
                    "morning_pool": {},
                    "morning_pool_date": None,
                    "auction_pool": {},
                    "auction_pool_date": None,
                    "last_morning_summary_date": None,
                    "paper_positions": [],
                    "paper_trades": [],
                }
        return {
            "processed_hashes": set(),
            "daily_signals": {},
            "last_summary_date": None,
            "morning_pool": {},
            "morning_pool_date": None,
            "auction_pool": {},
            "auction_pool_date": None,
            "last_morning_summary_date": None,
            "paper_positions": [],
            "paper_trades": [],
        }

    def save_state(self):
        with open(STATE_FILE, 'w', encoding='utf-8') as f:
            json.dump(
                {
                    "processed_hashes": list(self.processed_hashes),
                    "daily_signals": self.daily_signals,
                    "last_summary_date": self.last_summary_date,
                    "morning_pool": self.morning_pool,
                    "morning_pool_date": self.morning_pool_date,
                    "auction_pool": self.auction_pool,
                    "auction_pool_date": self.auction_pool_date,
                    "last_morning_summary_date": self.last_morning_summary_date,
                    "paper_positions": self.paper_positions,
                    "paper_trades": self.paper_trades,
                },
                f,
                ensure_ascii=False,
            )

    def generate_hash(self, signal_row):
        """为每条信号生成唯一哈希值 (基于内容和时间)"""
        # 使用 date, stock_code, action, source_segment 组合
        unique_str = f"{signal_row['date']}_{signal_row['stock_code']}_{signal_row['action']}_{signal_row['source_segment']}"
        return hashlib.md5(unique_str.encode('utf-8')).hexdigest()

    async def run_once(self):
        await self._paper_update_positions()
        logging.info("Step 1: Running Collector...")
        await self.collector.run()

        logging.info("Step 2: Extracting Signals...")
        df_video_signals = self.extractor.process_videos(config.VIDEOS_CSV)
        df_comment_signals = self.extractor.process_comments(config.COMMENTS_CSV)
        
        df_signals = pd.concat([df_video_signals, df_comment_signals], ignore_index=True)
        
        if df_signals.empty:
            logging.info("No signals found.")
            return

        # 保存最新的信号文件
        df_signals = self.scorer.add_scores_to_signals_df(df_signals)
        df_signals = merge_ocr_results(df_signals)
        df_signals = self.extractor.enhance_signals(df_signals)
        df_signals.to_csv(config.SIGNALS_CSV, index=False, encoding='utf-8-sig')
        
        logging.info(f"Step 3: Checking for new signals (Total: {len(df_signals)})...")
        await self.maybe_prepare_morning_pool(df_signals)
        await self.maybe_send_morning_summary()
        await self.maybe_run_auction_filter()
        
        new_signals = []
        for _, row in df_signals.iterrows():
            sig_hash = self.generate_hash(row)
            if sig_hash not in self.processed_hashes:
                # 这是一个新信号！
                new_signals.append(row)
                self.processed_hashes.add(sig_hash)
        
        if new_signals:
            logging.info(f"Found {len(new_signals)} NEW signals. Sending notifications...")
            for row in new_signals:
                if await self._should_send_signal(row):
                    self._track_daily_signal(row)
                    await self._paper_maybe_open_position(row)
                    await self.send_alert(row)
            self.save_state()
        else:
            logging.info("No new signals to notify.")

        await self.maybe_send_daily_summary()

    def _today_key(self):
        return datetime.now().strftime("%Y-%m-%d")

    def _track_daily_signal(self, row):
        if str(row.get("action", "")).upper() != "BUY":
            return
        day = str(row.get("date", "")).split(" ")[0]
        if not day:
            day = self._today_key()
        bucket = self.daily_signals.get(day)
        if bucket is None:
            bucket = []
            self.daily_signals[day] = bucket
        entry_price, entry_source = self._estimate_entry_price(row)
        payload = {
            "stock_code": str(row.get("stock_code")),
            "stock_name": str(row.get("stock_name")),
            "date": str(row.get("date")),
            "keywords": str(row.get("keywords")) if pd.notna(row.get("keywords")) else "",
            "validation_score": row.get("validation_score"),
            "adjusted_strength": row.get("adjusted_strength"),
            "realtime_price": row.get("realtime_price"),
            "price_change_pct": row.get("price_change_pct"),
            "ti_ma20": row.get("ti_ma20"),
            "ti_ma60": row.get("ti_ma60"),
            "ti_rsi14": row.get("ti_rsi14"),
            "ti_macd_hist": row.get("ti_macd_hist"),
            "ti_trend_label": row.get("ti_trend_label"),
            "entry_price": entry_price,
            "entry_source": entry_source,
        }
        bucket.append(payload)

    def _paper_find_position(self, signal_hash: str):
        for p in self.paper_positions:
            if str(p.get("signal_hash")) == signal_hash:
                return p
        return None

    def _paper_has_closed_trade(self, signal_hash: str) -> bool:
        for t in self.paper_trades:
            if str(t.get("signal_hash")) == signal_hash:
                return True
        return False

    def _paper_get_daily_df(self, code: str, year: int):
        cache_key = f"{str(code).zfill(6)}:{year}"
        if cache_key in self._paper_daily_cache:
            return self._paper_daily_cache.get(cache_key)
        start_dt = f"{year}-01-01"
        end_dt = f"{year}-12-31"
        df = self.data_provider.get_daily_data(code, start_dt, end_dt)
        self._paper_daily_cache[cache_key] = df
        return df

    def _paper_calc_expiry_date(self, code: str, entry_date: str) -> str:
        try:
            year = int(str(entry_date)[0:4])
        except Exception:
            return entry_date
        df = self._paper_get_daily_df(code, year)
        if df is None or df.empty:
            return entry_date
        if entry_date not in df.index:
            try:
                pos = df.index.searchsorted(entry_date)
                if pos >= len(df.index):
                    return entry_date
                entry_date = str(df.index[pos])
            except Exception:
                return entry_date
        try:
            start_pos = df.index.get_loc(entry_date)
            expiry_pos = min(start_pos + 2, len(df.index) - 1)
            return str(df.index[expiry_pos])
        except Exception:
            return entry_date

    async def _paper_maybe_open_position(self, row):
        sig_hash = self.generate_hash(row)
        if self._paper_find_position(sig_hash) is not None:
            return
        if self._paper_has_closed_trade(sig_hash):
            return
        if str(row.get("action", "")).upper() != "BUY":
            return

        code = str(row.get("stock_code", "")).zfill(6)
        name = str(row.get("stock_name", ""))
        strategy_type = str(row.get("source_segment", "")) or str(row.get("strategy_type", ""))
        publish_time = str(row.get("date", "")) or str(row.get("publish_time", ""))
        entry_reason = str(row.get("reason", "")) if pd.notna(row.get("reason", "")) else ""
        entry_reason = entry_reason or "REALTIME_SIGNAL"

        verified = await self.market_validator.get_verified_price(code)
        if not verified or not verified.get("is_valid"):
            return
        entry_price = float(verified.get("price"))
        if entry_price <= 0:
            return

        score = row.get("adjusted_strength")
        try:
            score = float(score) if score is not None else 0.0
        except Exception:
            score = 0.0

        risk_manager = SimpleRiskManager(small_capital_mode=True)
        ok, _ = risk_manager.validate_signal(score, entry_price, entry_price)
        if not ok:
            return
        position_size = risk_manager.calculate_simple_position(score, 1.0)
        if position_size <= 0:
            return

        entry_date = datetime.now().strftime("%Y-%m-%d")
        expiry_date = self._paper_calc_expiry_date(code, entry_date)
        stop_levels = risk_manager.get_stop_levels(entry_price, score)

        pos = {
            "signal_hash": sig_hash,
            "stock_code": code,
            "stock_name": name,
            "strategy_type": strategy_type,
            "publish_time": publish_time,
            "entry_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "entry_date": entry_date,
            "entry_price": round(entry_price, 4),
            "entry_reason": entry_reason,
            "stop_loss": float(stop_levels.get("stop_loss")),
            "take_profit": float(stop_levels.get("take_profit")),
            "expiry_date": expiry_date,
            "position_size": float(position_size),
            "source_text": str(row.get("keywords", "")) if pd.notna(row.get("keywords")) else "",
        }
        self.paper_positions.append(pos)

    async def _paper_update_positions(self):
        if not self.paper_positions:
            return

        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        closed = []
        remaining = []

        for pos in self.paper_positions:
            code = str(pos.get("stock_code", "")).zfill(6)
            entry_price = float(pos.get("entry_price", 0))
            stop_loss = float(pos.get("stop_loss", 0))
            take_profit = float(pos.get("take_profit", 0))
            expiry_date = str(pos.get("expiry_date", ""))

            verified = await self.market_validator.get_verified_price(code)
            current_price = None
            if verified and verified.get("is_valid"):
                try:
                    current_price = float(verified.get("price"))
                except Exception:
                    current_price = None

            if current_price is not None:
                if stop_loss > 0 and current_price <= stop_loss:
                    closed.append((pos, current_price, "Stop Loss", today))
                    continue
                if take_profit > 0 and current_price >= take_profit:
                    closed.append((pos, current_price, "Take Profit", today))
                    continue

            if expiry_date and today >= expiry_date and now.time() >= datetime.strptime("14:55", "%H:%M").time():
                if current_price is None:
                    remaining.append(pos)
                    continue
                closed.append((pos, current_price, "Time Exit (T+2)", today))
                continue

            remaining.append(pos)

        self.paper_positions = remaining

        if not closed:
            return

        for pos, exit_price, exit_reason, exit_date in closed:
            pnl_pct = 0.0
            try:
                pnl_pct = (float(exit_price) - float(pos.get("entry_price", 0))) / float(pos.get("entry_price", 1)) * 100
            except Exception:
                pnl_pct = 0.0

            trade = {
                "signal_hash": str(pos.get("signal_hash")),
                "stock_code": str(pos.get("stock_code")),
                "stock_name": str(pos.get("stock_name")),
                "publish_time": str(pos.get("publish_time")),
                "strategy_type": str(pos.get("strategy_type")),
                "entry_date": str(pos.get("entry_date")),
                "entry_price": float(pos.get("entry_price")),
                "exit_date": str(exit_date),
                "exit_price": float(exit_price),
                "pnl_pct": round(float(pnl_pct), 4),
                "entry_reason": str(pos.get("entry_reason")),
                "exit_reason": str(exit_reason),
                "status": "EXECUTED",
                "source_text": str(pos.get("source_text", "")),
                "exported": False,
            }
            self.paper_trades.append(trade)

        self._paper_export_trades()
        self.save_state()

    def _paper_export_trades(self):
        export_rows = [t for t in self.paper_trades if not t.get("exported")]
        if not export_rows:
            return
        out_path = "data/backtest_result_v2.csv"
        df_new = pd.DataFrame(export_rows)
        if os.path.exists(out_path):
            try:
                df_old = pd.read_csv(out_path)
                df_out = pd.concat([df_old, df_new], ignore_index=True)
            except Exception:
                df_out = df_new
        else:
            df_out = df_new
        cols = ['stock_code', 'stock_name', 'publish_time', 'strategy_type',
                'entry_date', 'entry_price', 'exit_date', 'exit_price',
                'pnl_pct', 'entry_reason', 'exit_reason', 'status', 'source_text']
        for c in cols:
            if c not in df_out.columns:
                df_out[c] = ""
        df_out = df_out[cols]
        df_out.to_csv(out_path, index=False, encoding='utf-8-sig')
        for t in export_rows:
            t["exported"] = True

    async def check_connection_status(self):
        """检查数据连接状态并尝试重连"""
        status = {"baostock": False, "realtime_api": False}
        
        # 1. 检查 BaoStock
        if self.data_provider.bs_logged_in:
            status["baostock"] = True
        else:
            logging.warning("BaoStock disconnected. Attempting reconnect...")
            self.data_provider._login_baostock()
            status["baostock"] = self.data_provider.bs_logged_in
            
        # 2. 检查实时 API (简单 ping)
        try:
            # 使用上证指数测试
            res = await self.market_validator.get_verified_price("000001")
            status["realtime_api"] = res["is_valid"]
        except Exception as e:
            logging.error(f"Realtime API check failed: {e}")
            
        return status

    async def _evaluate_buy_signal(self, row, require_auction=None):
        today = self._today_key()
        
        # 1. 竞价筛选 (可选)
        if require_auction is None:
            require_auction = getattr(config, "REQUIRE_AUCTION_POOL", True)
        if require_auction:
            if self.auction_pool_date == today and self.auction_pool:
                code = str(row.get("stock_code"))
                if code not in self.auction_pool:
                    return False, "未通过竞价筛选"

        # 2. 分数筛选
        score = row.get("validation_score")
        min_score = getattr(config, "BUY_SIGNAL_MIN_SCORE", 1.05)
        if pd.notna(score) and float(score) < float(min_score):
            return False, "验证分数不足"

        # 3. 趋势筛选
        trend = row.get("ti_trend_label")
        if pd.notna(trend) and str(trend) == "bear":
            return False, "日线趋势偏空"
            
        # 4. 实时价格逻辑 (新增：多源验证 + 进场条件)
        code = str(row.get("stock_code"))
        # 已经在 async 环境中，直接 await
        market_res = await self.market_validator.get_verified_price(code)
        
        if not market_res["is_valid"]:
            logging.warning(f"[{code}] Realtime data invalid: {market_res.get('reason')}")
            return False, f"实时数据不可用({market_res.get('reason')})"
            
        current_price = market_res["price"]
        row["realtime_price"] = current_price
        row["data_quality_score"] = market_res.get("quality_score", 0)
        
        entry_price = row.get("entry_price")
        if entry_price is None or pd.isna(entry_price):
            return False, "无法获取进场价"
            
        entry_price = float(entry_price)
        if entry_price <= 0:
            return False, "进场价无效"

        # 核心逻辑：触发价必须低于实时价 (Trigger < Realtime)
        # 意味着价格已经确认上涨突破进场位
        if entry_price >= current_price:
            logging.info(f"[{code}] Price below entry: {current_price} < {entry_price}")
            return False, f"价格未突破进场位 (进:{entry_price:.2f} >= 现:{current_price:.2f})"
            
        # 避免追高过猛 (比如已经涨了 >3% 就别追了)
        pnl_pct = (current_price - entry_price) / entry_price * 100
        row["price_change_pct"] = pnl_pct
        if pnl_pct > 5.0:
            logging.info(f"[{code}] Price deviated too much: +{pnl_pct:.2f}%")
            return False, f"已偏离进场位过远 (+{pnl_pct:.2f}%)"
            
        # 5. 风险控制验证 (新增) - 小资金全仓模式
        risk_manager = SimpleRiskManager(small_capital_mode=True)
        risk_ok, risk_reason = risk_manager.validate_signal(
            score, current_price, entry_price
        )
        if not risk_ok:
            logging.info(f"[{code}] Risk check failed: {risk_reason}")
            return False, f"风控拒绝: {risk_reason}"
            
        # 6. 计算建议仓位 (新增) - 小资金全仓逻辑
        position_size = risk_manager.calculate_simple_position(score, 1000000)
        row["suggested_position"] = position_size
        
        # 7. 获取止损止盈价位 (新增)
        stop_levels = risk_manager.get_stop_levels(entry_price, score)
        row.update(stop_levels)
        
        logging.info(f"[{code}] Buy signal validated! Entry:{entry_price} Real:{current_price} (+{pnl_pct:.2f}%) Score:{score} Position:{position_size:.1%} Stop:{stop_levels['stop_loss']} Target:{stop_levels['take_profit']}")
        return True, "通过系统条件"

    async def _should_send_signal(self, row, require_auction=None):
        action = str(row.get("action", "")).upper()
        if action != "BUY":
            return True
        ok, _ = await self._evaluate_buy_signal(row, require_auction=require_auction)
        return ok

    async def send_alert(self, row):
        """发送钉钉警报"""
        action_emoji = "🤔"
        if row['action'] == 'BUY':
            action_emoji = "🔴 买入"
        elif row['action'] == 'SELL':
            action_emoji = "💚 卖出"
        else:
            action_emoji = "⚪ 提及"

        title = f"{action_emoji} {row['stock_name']} ({row['stock_code']})"
        parts = [f"### {title}\n\n"]
        if row.get("date"):
            parts.append(f"- 时间: {row['date']}\n")
        if 'entry_price' in row and pd.notna(row['entry_price']):
            parts.append(f"- 进场价: {row['entry_price']}\n")
        if 'realtime_price' in row and pd.notna(row['realtime_price']):
            parts.append(f"- 实时价: {row['realtime_price']}\n")
        if 'price_change_pct' in row and pd.notna(row['price_change_pct']):
            parts.append(f"- 涨跌幅: {row['price_change_pct']:.2f}%\n")
        if 'validation_score' in row and pd.notna(row['validation_score']):
            parts.append(f"- 分数: {row['validation_score']:.3f}\n")
        if 'adjusted_strength' in row and pd.notna(row['adjusted_strength']):
            parts.append(f"- 强度: {row['adjusted_strength']:.3f}\n")
        parts.append("> 来自 BiliStock 监控系统")
        text = "".join(parts)
               
        await self.notifier.send_markdown(title, text)
        # 避免瞬间发送太多被限流
        await asyncio.sleep(0.5)

    def _build_buy_reason(self, row):
        reasons = []
        score = row.get("validation_score")
        if pd.notna(score):
            if float(score) >= 1.1:
                reasons.append("验证分数较高")
            elif float(score) >= 1.0:
                reasons.append("验证分数稳定")
        min_score = getattr(config, "BUY_SIGNAL_MIN_SCORE", 1.05)
        if pd.notna(score) and float(score) >= float(min_score):
            reasons.append(f"满足阈值{float(min_score):.2f}")
        pct = row.get("price_change_pct")
        if pd.notna(pct) and float(pct) > 0:
            reasons.append("盘中价格走强")
        trend = row.get("ti_trend_label")
        if pd.notna(trend):
            if str(trend) == "bull":
                reasons.append("日线趋势偏多")
            elif str(trend) == "bear":
                reasons.append("日线趋势偏空")
        rsi = row.get("ti_rsi14")
        if pd.notna(rsi):
            if float(rsi) < getattr(config, "RSI_BUY_MAX", 35):
                reasons.append("RSI偏低有修复空间")
            elif float(rsi) > 70:
                reasons.append("RSI强势区但注意回撤")
        macd_hist = row.get("ti_macd_hist")
        if pd.notna(macd_hist):
            if float(macd_hist) > 0:
                reasons.append("MACD动能为正")
        keywords = row.get("keywords")
        if pd.notna(keywords) and str(keywords).strip():
            reasons.append(f"关键词: {keywords}")
        today = self._today_key()
        if self.auction_pool_date == today:
            code = str(row.get("stock_code"))
            if code in self.auction_pool:
                info = self.auction_pool.get(code, {})
                open_pct = info.get("open_pct")
                vol_ratio = info.get("volume_ratio")
                if open_pct is not None and vol_ratio is not None:
                    reasons.append(f"竞价开盘{open_pct:.2f}%, 量比{vol_ratio:.2f}")
                else:
                    reasons.append("通过竞价筛选")
        return "；".join(reasons)

    async def maybe_send_daily_summary(self):
        now = datetime.now()
        close_time = getattr(config, "CLOSE_SUMMARY_TIME", "15:05")
        try:
            close_h, close_m = [int(x) for x in close_time.split(":")]
        except Exception:
            close_h, close_m = 15, 5
        if now.hour < close_h or (now.hour == close_h and now.minute < close_m):
            return
        today = self._today_key()
        if self.last_summary_date == today:
            return

        day_signals = await self._collect_today_buy_signals_from_csv(today)
        title = f"收盘汇总 {today}"
        parts = [f"## {title}\n\n"]
        if not day_signals:
            parts.append("- 今日无买入信号\n")
        else:
            for item in day_signals:
                name = item.get("stock_name")
                code = item.get("stock_code")
                t = str(item.get("date")).split(" ")[-1] if item.get("date") else ""
                reason = self._build_buy_reason(item)
                vs = item.get("validation_score")
                vs_text = f"{float(vs):.3f}" if pd.notna(vs) else "N/A"
                parts.append(f"- {name}({code}) {t} 分数 {vs_text} 理由 {reason}\n")
        parts.append("> 来自 BiliStock 监控系统")
        text = "".join(parts)
        await self.notifier.send_markdown(title, text)
        self.last_summary_date = today
        self.save_state()

    def _estimate_entry_price(self, row):
        code = str(row.get("stock_code"))
        dt_raw = row.get("date")
        dt = pd.to_datetime(dt_raw, errors="coerce")
        day = str(dt.date()) if pd.notna(dt) else str(row.get("date", "")).split(" ")[0]
        if not day:
            day = self._today_key()

        # --- 缓存检查 ---
        # 缓存键：code + day + dt(信号时间)
        # 如果已经有 entry_price 且来源不是 "临时估算"，则直接返回
        # 注意：这里我们假设历史 entry_price 不会变。
        cache_key = f"{code}_{day}_{dt}"
        if not hasattr(self, "_entry_price_cache"):
            self._entry_price_cache = {}
        
        if cache_key in self._entry_price_cache:
            return self._entry_price_cache[cache_key]
        
        # 严格锁定进场价逻辑：进场价必须是历史数据，不能是“现在”
        # 如果是当日信号且时间很近，我们仍需获取那一刻的分钟线
        
        dp = self.data_provider
        # 优先尝试 BaoStock 5分钟线 (更稳定，且不耗费 Tushare 积分)
        # 如果是当天且未收盘，BaoStock 可能没有最新数据，需要 Tushare 分钟线
        # 但考虑到 Tushare 限流，我们可以先查 BaoStock
        
        price_found = None
        source_found = None

        df_min = dp.get_minute_data(code, day)
        if df_min is not None and not df_min.empty:
            try:
                df_min = df_min.copy()
                df_min["time"] = pd.to_datetime(df_min["time"], errors="coerce")
                df_min = df_min[df_min["time"].notna()]
                if pd.notna(dt):
                    # 寻找信号发生后的第一根K线
                    after_signal = df_min[df_min["time"] >= dt]
                    if not after_signal.empty:
                         price_found = float(after_signal.iloc[0]["close"])
                         source_found = "分钟线"
                
                # 如果找不到信号后的（可能信号太新），回退到最近的一根
                if price_found is None and not df_min.empty:
                     price_found = float(df_min.iloc[-1]["close"])
                     source_found = "分钟线(最新)"
            except Exception:
                pass
        
        if price_found is None:
            # 日线兜底
            df_daily = dp.get_daily_data(code, day, day)
            if df_daily is not None and not df_daily.empty and day in df_daily.index:
                try:
                    if pd.notna(dt):
                        open_time = pd.to_datetime(f"{day} 09:30:00", errors="coerce")
                        if pd.notna(open_time) and dt <= open_time:
                            price_found = float(df_daily.loc[day]["open"])
                            source_found = "日线开盘"
                    if price_found is None:
                        price_found = float(df_daily.loc[day]["close"])
                        source_found = "日线收盘"
                except Exception:
                    pass
        
        # 绝不使用 realtime_price 作为 entry_price 的兜底
        # 否则会导致 Trigger == Realtime
        
        if price_found is not None:
            # 写入缓存
            self._entry_price_cache[cache_key] = (price_found, source_found)
            return price_found, source_found

        return None, None

    async def maybe_prepare_morning_pool(self, df_signals: pd.DataFrame):
        today = self._today_key()
        if self.morning_pool_date == today:
            return
        if df_signals is None or df_signals.empty:
            return
        df = df_signals.copy()
        df = df[df["action"] == "BUY"] if "action" in df.columns else df
        if "date" in df.columns:
            df = df[df["date"].astype(str).str.startswith(today)]
        if df.empty:
            return
        pool = {}
        for _, row in df.iterrows():
            code = str(row.get("stock_code"))
            name = str(row.get("stock_name"))
            item = pool.get(code)
            if item is None:
                item = {"stock_code": code, "stock_name": name, "mentions": 0, "keywords": set()}
                pool[code] = item
            item["mentions"] += 1
            kw = row.get("keywords")
            if pd.notna(kw) and str(kw).strip():
                for k in str(kw).split(","):
                    if k.strip():
                        item["keywords"].add(k.strip())
        for item in pool.values():
            item["keywords"] = "，".join(sorted(item["keywords"])) if item["keywords"] else ""
        self.morning_pool = pool
        self.morning_pool_date = today
        self.save_state()

    async def maybe_send_morning_summary(self):
        now = datetime.now()
        morning_time = getattr(config, "MORNING_SUMMARY_TIME", "09:20")
        try:
            h, m = [int(x) for x in morning_time.split(":")]
        except Exception:
            h, m = 9, 20
        if now.hour < h or (now.hour == h and now.minute < m):
            return
        today = self._today_key()
        if self.last_morning_summary_date == today:
            return
        if self.morning_pool_date != today or not self.morning_pool:
            return
        title = f"盘前推荐池 {today}"
        parts = [f"## {title}\n\n"]
        items = list(self.morning_pool.values())
        items.sort(key=lambda x: x.get("mentions", 0), reverse=True)
        for item in items:
            parts.append(
                f"- {item['stock_name']}({item['stock_code']}) 关注{item['mentions']} 次"
                + (f" 关键词 {item['keywords']}\n" if item.get("keywords") else "\n")
            )
        parts.append("> 来自 BiliStock 监控系统")
        text = "".join(parts)
        await self.notifier.send_markdown(title, text)
        self.last_morning_summary_date = today
        self.save_state()

    async def maybe_run_auction_filter(self):
        now = datetime.now()
        auction_time = getattr(config, "AUCTION_FILTER_TIME", "09:25")
        try:
            h, m = [int(x) for x in auction_time.split(":")]
        except Exception:
            h, m = 9, 25
        if now.hour < h or (now.hour == h and now.minute < m):
            return
        today = self._today_key()
        if self.auction_pool_date == today:
            return
        if self.morning_pool_date != today or not self.morning_pool:
            return
        from core.intraday_trader import PreMarketFilter
        codes = list(self.morning_pool.keys())
        if not codes:
            return
        filter = PreMarketFilter()
        filtered, rejected = filter.filter_auction_candidates(codes)
        self.auction_pool = {str(s["code"]): s for s in filtered} if filtered else {}
        self.auction_pool_date = today
        title = f"竞价筛选结果 {today}"
        parts = [f"## {title}\n\n"]
        if filtered:
            parts.append("### 通过\n")
            for s in filtered:
                parts.append(f"- {s['name']}({s['code']}) 开盘{s['open_pct']:.2f}% 量比{s['volume_ratio']:.2f}\n")
        if rejected:
            parts.append("\n### 剔除\n")
            for s in rejected:
                parts.append(f"- {s['name']}({s['code']}) {s.get('rejection_reason','')}\n")
        if not filtered and not rejected:
            parts.append("- 未获取到有效竞价数据\n")
        parts.append("> 来自 BiliStock 监控系统")
        text = "".join(parts)
        await self.notifier.send_markdown(title, text)
        self.save_state()

    async def _collect_today_buy_signals_from_csv(self, today: str):
        out = []
        try:
            df = pd.read_csv(config.SIGNALS_CSV)
        except Exception:
            return out
        if df is None or df.empty:
            return out
        try:
            df = df[df["date"].astype(str).str.startswith(today)]
        except Exception:
            return out
        if "action" not in df.columns:
            return out
        df = df[df["action"] == "BUY"]
        # 类型规范化
        for c in ("validation_score", "adjusted_strength", "realtime_price", "price_change_pct",
                  "ti_ma20", "ti_ma60", "ti_rsi14", "ti_macd_hist"):
            if c in df.columns:
                try:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
                except Exception:
                    pass
        rows = []
        for _, row in df.iterrows():
            item = {
                "stock_code": str(row.get("stock_code")),
                "stock_name": str(row.get("stock_name")),
                "date": str(row.get("date")),
                "keywords": str(row.get("keywords")) if pd.notna(row.get("keywords")) else "",
                "validation_score": row.get("validation_score"),
                "adjusted_strength": row.get("adjusted_strength"),
                "realtime_price": row.get("realtime_price"),
                "price_change_pct": row.get("price_change_pct"),
                "ti_ma20": row.get("ti_ma20"),
                "ti_ma60": row.get("ti_ma60"),
                "ti_rsi14": row.get("ti_rsi14"),
                "ti_macd_hist": row.get("ti_macd_hist"),
                "ti_trend_label": row.get("ti_trend_label"),
                "action": "BUY",
            }
            # 只汇总符合系统买入条件的票
            if await self._should_send_signal(item):
                rows.append(item)
        return rows

    async def run_loop(self):
        logging.info(f"Starting Stock Monitor (Interval: {config.MONITOR_INTERVAL}s)...")
        if not config.DINGTALK_WEBHOOK:
             logging.warning("⚠️ 未配置钉钉 Webhook！手机无法接收消息。请在 config.py 中配置。")
        
        while True:
            try:
                await self.run_once()
            except Exception as e:
                logging.error(f"Error in monitor loop: {e}")
            
            logging.info(f"Waiting {config.MONITOR_INTERVAL}s for next check...")
            await asyncio.sleep(config.MONITOR_INTERVAL)

if __name__ == "__main__":
    monitor = StockMonitor()
    try:
        asyncio.run(monitor.run_loop())
    except KeyboardInterrupt:
        logging.info("Monitor stopped by user.")
