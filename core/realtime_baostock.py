import logging
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd


class RealTimeBaoStockFetcher:
    def __init__(self):
        self._bs = None
        self._logged_in = False
        self._login()

    def _login(self) -> bool:
        try:
            import baostock as bs
            lg = bs.login()
            if lg.error_code == "0":
                self._bs = bs
                self._logged_in = True
                return True
            logging.error(f"BaoStock login failed: {lg.error_msg}")
            return False
        except Exception as e:
            logging.error(f"BaoStock login error: {e}")
            return False

    def logout(self) -> None:
        if self._logged_in and self._bs is not None:
            try:
                self._bs.logout()
            except Exception:
                pass
        self._logged_in = False
        self._bs = None

    def _to_bs_code(self, code: str) -> str:
        c = str(code).zfill(6)
        if c.startswith("6"):
            return f"sh.{c}"
        if c.startswith("0") or c.startswith("3"):
            return f"sz.{c}"
        if c.startswith("8") or c.startswith("4"):
            return f"bj.{c}"
        return f"sh.{c}"

    def get_daily_bars(
        self,
        code: str,
        lookback_days: int = 200,
        end_date: Optional[str] = None,
        adjustflag: str = "3",
    ) -> pd.DataFrame:
        if not self._logged_in or self._bs is None:
            return pd.DataFrame()

        end = end_date or datetime.now().strftime("%Y-%m-%d")
        try:
            end_dt = datetime.strptime(end, "%Y-%m-%d")
        except Exception:
            end_dt = datetime.now()
            end = end_dt.strftime("%Y-%m-%d")

        lookback_days = int(lookback_days) if lookback_days is not None else 200
        if lookback_days <= 0:
            lookback_days = 200

        start_dt = end_dt - timedelta(days=max(365, lookback_days * 3))
        start = start_dt.strftime("%Y-%m-%d")

        bs_code = self._to_bs_code(code)
        rs = self._bs.query_history_k_data_plus(
            bs_code,
            "date,open,high,low,close,volume,amount,turn",
            start_date=start,
            end_date=end,
            frequency="d",
            adjustflag=str(adjustflag),
        )
        if rs.error_code != "0":
            return pd.DataFrame()

        data_list = []
        while rs.next():
            data_list.append(rs.get_row_data())
        if not data_list:
            return pd.DataFrame()

        df = pd.DataFrame(data_list, columns=rs.fields)
        for col in ["open", "high", "low", "close", "volume", "amount", "turn"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "date" in df.columns:
            df = df.sort_values("date")

        if len(df) > lookback_days:
            df = df.tail(lookback_days).reset_index(drop=True)
        return df

    def get_intraday_bars(
        self,
        code: str,
        frequency: str = "5",
        date_str: Optional[str] = None,
        adjustflag: str = "3",
    ) -> pd.DataFrame:
        if not self._logged_in or self._bs is None:
            return pd.DataFrame()

        allowed = {"5", "15", "30", "60"}
        freq = str(frequency)
        if freq not in allowed:
            return pd.DataFrame()

        base_date = datetime.now() if date_str is None else datetime.strptime(date_str, "%Y-%m-%d")
        bs_code = self._to_bs_code(code)
        for i in range(0, 7):
            d = (base_date - timedelta(days=i)).strftime("%Y-%m-%d")
            rs = self._bs.query_history_k_data_plus(
                bs_code,
                "date,time,open,high,low,close,volume,amount",
                start_date=d,
                end_date=d,
                frequency=freq,
                adjustflag=str(adjustflag),
            )
            if rs.error_code != "0":
                continue
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
            if not data_list:
                continue
            df = pd.DataFrame(data_list, columns=rs.fields)
            for col in ["open", "high", "low", "close", "volume", "amount"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            if "time" in df.columns:
                df["time"] = df["time"].astype(str).apply(self._format_bs_time)
            return df
        return pd.DataFrame()

    def get_latest_price_snapshot(
        self,
        code: str,
        frequency_candidates: Optional[list[str]] = None,
        date_str: Optional[str] = None,
    ) -> dict:
        if frequency_candidates is None:
            frequency_candidates = ["5", "15"]

        bars = pd.DataFrame()
        for freq in frequency_candidates:
            bars = self.get_intraday_bars(code=code, frequency=freq, date_str=date_str)
            if bars is not None and not bars.empty:
                break

        if bars is None or bars.empty:
            return {
                "stock_code": str(code).zfill(6),
                "date": date_str or datetime.now().strftime("%Y-%m-%d"),
                "time": None,
                "open": None,
                "close": None,
                "high": None,
                "low": None,
                "volume": None,
                "amount": None,
            }

        last = bars.iloc[-1]
        first = bars.iloc[0]
        return {
            "stock_code": str(code).zfill(6),
            "date": str(last.get("date")),
            "time": str(last.get("time")),
            "open": float(first.get("open")) if pd.notna(first.get("open")) else None,
            "close": float(last.get("close")) if pd.notna(last.get("close")) else None,
            "high": float(last.get("high")) if pd.notna(last.get("high")) else None,
            "low": float(last.get("low")) if pd.notna(last.get("low")) else None,
            "volume": float(last.get("volume")) if pd.notna(last.get("volume")) else None,
            "amount": float(last.get("amount")) if pd.notna(last.get("amount")) else None,
        }

    def _format_bs_time(self, t: str) -> str:
        t = str(t)
        if len(t) >= 14 and t[0:4].isdigit():
            return f"{t[0:4]}-{t[4:6]}-{t[6:8]} {t[8:10]}:{t[10:12]}:{t[12:14]}"
        return t

    def __del__(self):
        self.logout()
