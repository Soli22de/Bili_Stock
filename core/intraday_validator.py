import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from core.realtime_baostock import RealTimeBaoStockFetcher
from core.data_provider import DataProvider

try:
    import config as _config
except Exception:
    _config = None


class IntradaySignalValidator:
    def __init__(
        self,
        min_score: float = 0.3,
        max_score: float = 2.0,
    ):
        self.fetcher = RealTimeBaoStockFetcher()
        self.min_score = float(min_score)
        self.max_score = float(max_score)
        self.enable_technical_indicators = bool(getattr(_config, "ENABLE_TECHNICAL_INDICATORS", True)) if _config else True
        self.indicator_lookback_days = int(getattr(_config, "INDICATOR_LOOKBACK_DAYS", 200)) if _config else 200
        self.rsi_buy_max = float(getattr(_config, "RSI_BUY_MAX", 35)) if _config else 35.0
        self.rsi_sell_min = float(getattr(_config, "RSI_SELL_MIN", 65)) if _config else 65.0
        self._snapshot_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._intraday_cache: Dict[Tuple[str, str, str], pd.DataFrame] = {}
        self._daily_cache: Dict[Tuple[str, str, int], pd.DataFrame] = {}

    def validate_signals(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        validated: List[Dict[str, Any]] = []
        for signal in signals:
            try:
                validated.append(self.validate_single_signal(signal))
            except Exception as e:
                logging.error(f"validate_signals failed: {e}")
                s = dict(signal)
                s["real_time_validated"] = False
                s["validation_error"] = str(e)
                validated.append(s)
        return validated

    def validate_single_signal(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        s = dict(signal)
        code = s.get("stock_code") or s.get("code")
        if code is None:
            s["real_time_validated"] = False
            s["validation_error"] = "missing_stock_code"
            return s

        code_str = str(code).zfill(6)
        today = datetime.now().strftime("%Y-%m-%d")
        snapshot = self._get_snapshot(code_str, today)
        bars = self._get_intraday_bars(code_str, frequency="5", date_str=today)
        daily = (
            self._get_daily_bars(code_str, end_date=today, lookback_days=self.indicator_lookback_days)
            if self.enable_technical_indicators
            else pd.DataFrame()
        )

        score, details = self._apply_rules(s, snapshot=snapshot, bars=bars, daily=daily)
        score = float(max(self.min_score, min(self.max_score, score)))

        strength = self._to_float(s.get("strength"))
        s["real_time_validated"] = True
        s["validation_score"] = score
        s["adjusted_strength"] = strength * score if strength is not None else None
        s["last_validation_time"] = datetime.now().isoformat(timespec="seconds")
        s["realtime_price"] = snapshot.get("close")
        s["price_change_pct"] = self._calc_price_change_pct(snapshot)
        s["validation_details"] = details
        ti = details.get("technical_indicators") if isinstance(details, dict) else None
        if isinstance(ti, dict) and ti.get("available"):
            s["ti_ma20"] = ti.get("ma20")
            s["ti_ma60"] = ti.get("ma60")
            s["ti_rsi14"] = ti.get("rsi14")
            s["ti_macd"] = ti.get("macd")
            s["ti_macd_signal"] = ti.get("macd_signal")
            s["ti_macd_hist"] = ti.get("macd_hist")
        else:
            s["ti_ma20"] = None
            s["ti_ma60"] = None
            s["ti_rsi14"] = None
            s["ti_macd"] = None
            s["ti_macd_signal"] = None
            s["ti_macd_hist"] = None
        ti_rule = details.get("technical_indicators_rule") if isinstance(details, dict) else None
        if isinstance(ti_rule, dict):
            s["ti_trend_label"] = ti_rule.get("trend")
        else:
            s["ti_trend_label"] = None
        return s

    def _apply_rules(
        self,
        signal: Dict[str, Any],
        snapshot: Dict[str, Any],
        bars: pd.DataFrame,
        daily: pd.DataFrame,
    ) -> Tuple[float, Dict[str, Any]]:
        total = 1.0
        details: Dict[str, Any] = {}

        price_rule = self._rule_price_direction(signal, snapshot)
        total *= price_rule["score"]
        details["price_direction"] = price_rule

        volume_rule = self._rule_volume_ratio(bars)
        total *= volume_rule["score"]
        details["volume"] = volume_rule

        decay_rule = self._rule_time_decay(signal)
        total *= decay_rule["score"]
        details["time_decay"] = decay_rule

        ti = self._compute_technical_indicators(daily)
        details["technical_indicators"] = ti
        if self.enable_technical_indicators:
            ti_rule = self._rule_technical_indicators(signal, ti)
            total *= ti_rule["score"]
            details["technical_indicators_rule"] = ti_rule
        return total, details

    def _get_snapshot(self, code: str, date_str: str) -> Dict[str, Any]:
        key = (code, date_str)
        if key in self._snapshot_cache:
            return self._snapshot_cache[key]
        snap = self.fetcher.get_latest_price_snapshot(code, date_str=date_str)
        if snap.get("close") is None:
            try:
                dp = DataProvider()
                df = dp.get_minute_data(code, date_str)
                if df is not None and not df.empty:
                    first = df.iloc[0]
                    last = df.iloc[-1]
                    snap = {
                        "stock_code": str(code).zfill(6),
                        "date": date_str,
                        "time": str(last.get("time")),
                        "open": float(first.get("open")) if pd.notna(first.get("open")) else None,
                        "close": float(last.get("close")) if pd.notna(last.get("close")) else None,
                        "high": float(df["high"].max()) if "high" in df.columns else None,
                        "low": float(df["low"].min()) if "low" in df.columns else None,
                        "volume": float(df["volume"].sum()) if "volume" in df.columns else None,
                        "amount": float(df["amount"].sum()) if "amount" in df.columns else None,
                    }
                else:
                    df_daily = dp.get_daily_data(code, date_str, date_str)
                    if df_daily is not None and not df_daily.empty and date_str in df_daily.index:
                        row = df_daily.loc[date_str]
                        snap = {
                            "stock_code": str(code).zfill(6),
                            "date": date_str,
                            "time": None,
                            "open": float(row.get("open")) if pd.notna(row.get("open")) else None,
                            "close": float(row.get("close")) if pd.notna(row.get("close")) else None,
                            "high": float(row.get("high")) if pd.notna(row.get("high")) else None,
                            "low": float(row.get("low")) if pd.notna(row.get("low")) else None,
                            "volume": float(row.get("volume")) if pd.notna(row.get("volume")) else None,
                            "amount": float(row.get("amount")) if pd.notna(row.get("amount")) else None,
                        }
            except Exception:
                pass
        self._snapshot_cache[key] = snap
        return snap

    def _get_intraday_bars(self, code: str, frequency: str, date_str: str) -> pd.DataFrame:
        key = (code, str(frequency), date_str)
        if key in self._intraday_cache:
            return self._intraday_cache[key]
        df = self.fetcher.get_intraday_bars(code=code, frequency=str(frequency), date_str=date_str)
        if df is None or df.empty:
            try:
                dp = DataProvider()
                df = dp.get_minute_data(code, date_str)
            except Exception:
                df = pd.DataFrame()
        self._intraday_cache[key] = df
        return df

    def _get_daily_bars(self, code: str, end_date: str, lookback_days: int) -> pd.DataFrame:
        key = (code, end_date, int(lookback_days))
        if key in self._daily_cache:
            return self._daily_cache[key]
        df = self.fetcher.get_daily_bars(code=code, lookback_days=int(lookback_days), end_date=end_date)
        if df is None or df.empty:
            try:
                dp = DataProvider()
                start_dt = (pd.to_datetime(end_date) - pd.Timedelta(days=int(lookback_days) * 2)).strftime("%Y-%m-%d")
                df = dp.get_daily_data(code, start_dt, end_date)
            except Exception:
                df = pd.DataFrame()
        self._daily_cache[key] = df
        return df

    def _compute_technical_indicators(self, daily: pd.DataFrame) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if daily is None or daily.empty or "close" not in daily.columns:
            out["available"] = False
            out["reason"] = "no_daily_data"
            return out

        close = pd.to_numeric(daily["close"], errors="coerce").dropna()
        if close.empty:
            out["available"] = False
            out["reason"] = "no_close"
            return out

        out["available"] = True
        out["close"] = self._last_or_none(close)
        out["prev_close"] = self._last_or_none(close.iloc[:-1]) if len(close) >= 2 else None
        out["ma5"] = self._last_or_none(close.rolling(5).mean())
        out["ma10"] = self._last_or_none(close.rolling(10).mean())
        out["ma20"] = self._last_or_none(close.rolling(20).mean())
        out["ma60"] = self._last_or_none(close.rolling(60).mean())

        rsi14 = self._calc_rsi(close, period=14)
        out["rsi14"] = self._last_or_none(rsi14)

        macd, macd_signal, macd_hist = self._calc_macd(close)
        out["macd"] = self._last_or_none(macd)
        out["macd_signal"] = self._last_or_none(macd_signal)
        out["macd_hist"] = self._last_or_none(macd_hist)
        return out

    def _rule_technical_indicators(self, signal: Dict[str, Any], ti: Dict[str, Any]) -> Dict[str, Any]:
        action = str(signal.get("action") or signal.get("signal_type") or signal.get("signal") or "").upper()
        if action not in {"BUY", "SELL"}:
            return {"score": 1.0, "reason": "neutral_action"}
        if not ti or not ti.get("available"):
            return {"score": 1.0, "reason": "no_indicators"}

        close = self._to_float(ti.get("close"))
        ma20 = self._to_float(ti.get("ma20"))
        ma60 = self._to_float(ti.get("ma60"))
        rsi14 = self._to_float(ti.get("rsi14"))
        macd_hist = self._to_float(ti.get("macd_hist"))

        score = 1.0
        components: Dict[str, Any] = {}

        if close is not None and ma20 is not None:
            if action == "BUY":
                comp = 1.1 if close > ma20 else 0.9
            else:
                comp = 1.1 if close < ma20 else 0.9
            score *= comp
            components["ma20_vs_close"] = {"score": comp, "close": close, "ma20": ma20}

        if ma20 is not None and ma60 is not None:
            if action == "BUY":
                comp = 1.05 if ma20 > ma60 else 0.95
            else:
                comp = 1.05 if ma20 < ma60 else 0.95
            score *= comp
            components["ma_trend"] = {"score": comp, "ma20": ma20, "ma60": ma60}

        if rsi14 is not None:
            if action == "BUY":
                if rsi14 < self.rsi_buy_max:
                    comp = 1.1
                elif rsi14 > 70:
                    comp = 0.85
                else:
                    comp = 1.0
            else:
                if rsi14 > self.rsi_sell_min:
                    comp = 1.1
                elif rsi14 < 30:
                    comp = 0.85
                else:
                    comp = 1.0
            score *= comp
            components["rsi14"] = {"score": comp, "rsi14": rsi14}

        if macd_hist is not None:
            if action == "BUY":
                comp = 1.05 if macd_hist > 0 else 0.95
            else:
                comp = 1.05 if macd_hist < 0 else 0.95
            score *= comp
            components["macd_hist"] = {"score": comp, "macd_hist": macd_hist}

        trend = "sideways"
        if close is not None and ma20 is not None and ma60 is not None:
            if close > ma20 > ma60:
                trend = "bull"
            elif close < ma20 < ma60:
                trend = "bear"

        return {"score": float(score), "trend": trend, "components": components}

    def _calc_rsi(self, close: pd.Series, period: int = 14) -> pd.Series:
        delta = close.diff()
        gain = delta.clip(lower=0.0)
        loss = (-delta).clip(lower=0.0)
        alpha = 1.0 / float(period)
        avg_gain = gain.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
        avg_loss = loss.ewm(alpha=alpha, adjust=False, min_periods=period).mean()
        rs = avg_gain / avg_loss.replace({0.0: pd.NA})
        rsi = 100.0 - (100.0 / (1.0 + rs))
        return rsi

    def _calc_macd(self, close: pd.Series) -> Tuple[pd.Series, pd.Series, pd.Series]:
        ema12 = close.ewm(span=12, adjust=False, min_periods=12).mean()
        ema26 = close.ewm(span=26, adjust=False, min_periods=26).mean()
        macd = ema12 - ema26
        signal = macd.ewm(span=9, adjust=False, min_periods=9).mean()
        hist = macd - signal
        return macd, signal, hist

    def _last_or_none(self, s: pd.Series) -> Optional[float]:
        if s is None or len(s) == 0:
            return None
        v = s.iloc[-1]
        if pd.isna(v):
            return None
        try:
            return float(v)
        except Exception:
            return None

    def _rule_price_direction(self, signal: Dict[str, Any], snapshot: Dict[str, Any]) -> Dict[str, Any]:
        open_price = self._to_float(snapshot.get("open"))
        last_price = self._to_float(snapshot.get("close"))
        if open_price is None or last_price is None or open_price == 0:
            return {"score": 1.0, "reason": "no_price"}

        actual = 1 if last_price > open_price else (-1 if last_price < open_price else 0)
        st = str(signal.get("action") or signal.get("signal_type") or signal.get("signal") or "").upper()
        expected = 1 if st == "BUY" else (-1 if st == "SELL" else 0)

        if expected == 0 or actual == 0:
            return {"score": 1.0, "reason": "neutral"}
        if expected == actual:
            return {"score": 1.2, "reason": "match"}
        return {"score": 0.8, "reason": "mismatch"}

    def _rule_volume_ratio(self, bars: pd.DataFrame) -> Dict[str, Any]:
        if bars is None or bars.empty or "volume" not in bars.columns:
            return {"score": 1.0, "reason": "no_volume"}

        vol = pd.to_numeric(bars["volume"], errors="coerce").dropna()
        if len(vol) < 10:
            return {"score": 1.0, "reason": "insufficient_volume"}

        recent = float(vol.tail(3).mean())
        base = float(vol.tail(min(len(vol), 30)).mean())
        if base <= 0:
            return {"score": 1.0, "reason": "zero_base"}

        ratio = recent / base
        if ratio >= 2.0:
            return {"score": 1.25, "reason": "high", "ratio": ratio}
        if ratio >= 1.5:
            return {"score": 1.1, "reason": "moderate", "ratio": ratio}
        if ratio <= 0.5:
            return {"score": 0.75, "reason": "low", "ratio": ratio}
        return {"score": 1.0, "reason": "normal", "ratio": ratio}

    def _rule_time_decay(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        ts = signal.get("timestamp") or signal.get("date") or signal.get("time")
        if ts is None:
            return {"score": 1.0, "reason": "missing_timestamp"}

        try:
            t0 = pd.to_datetime(ts)
        except Exception:
            return {"score": 1.0, "reason": "bad_timestamp"}

        hours = (datetime.now() - t0.to_pydatetime()).total_seconds() / 3600.0
        if hours <= 1.0:
            return {"score": 1.0, "hours": hours}

        decay = max(0.7, 1.0 - (hours - 1.0) * 0.05)
        return {"score": float(decay), "hours": hours}

    def _calc_price_change_pct(self, snapshot: Dict[str, Any]) -> Optional[float]:
        open_price = self._to_float(snapshot.get("open"))
        last_price = self._to_float(snapshot.get("close"))
        if open_price is None or last_price is None or open_price == 0:
            return None
        return (last_price - open_price) / open_price * 100.0

    def _to_float(self, x: Any) -> Optional[float]:
        if x is None:
            return None
        try:
            return float(x)
        except Exception:
            return None

    def close(self) -> None:
        if hasattr(self, "fetcher") and self.fetcher is not None:
            self.fetcher.logout()

    def __del__(self):
        self.close()
