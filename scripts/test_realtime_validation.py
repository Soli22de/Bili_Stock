import os
import sys
from datetime import datetime, timedelta

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.intraday_validator import IntradaySignalValidator
import config


def _pick_recent_date(code: str, max_days: int = 10) -> str:
    from core.realtime_baostock import RealTimeBaoStockFetcher

    fetcher = RealTimeBaoStockFetcher()
    try:
        for i in range(max_days):
            d = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            bars = fetcher.get_intraday_bars(code, frequency="5", date_str=d)
            if bars is not None and not bars.empty:
                return d
        return datetime.now().strftime("%Y-%m-%d")
    finally:
        fetcher.logout()


def main():
    codes = ["600036", "000001"]
    date_str = _pick_recent_date(codes[0])

    now = datetime.now().isoformat(timespec="seconds")
    test_signals = [
        {
            "stock_code": codes[0],
            "signal_type": "BUY",
            "strength": 0.8,
            "timestamp": now,
            "date": f"{date_str} 10:00:00",
            "source_segment": "测试买入信号",
        },
        {
            "stock_code": codes[1],
            "signal_type": "SELL",
            "strength": 0.6,
            "timestamp": (datetime.now() - timedelta(hours=2)).isoformat(timespec="seconds"),
            "date": f"{date_str} 11:00:00",
            "source_segment": "测试卖出信号",
        },
    ]

    validator = IntradaySignalValidator()
    try:
        out = validator.validate_signals(test_signals)
    finally:
        validator.close()

    df = pd.DataFrame(out)
    cols = [
        "stock_code",
        "signal_type",
        "strength",
        "validation_score",
        "adjusted_strength",
        "realtime_price",
        "price_change_pct",
        "real_time_validated",
        "ti_ma20",
        "ti_ma60",
        "ti_rsi14",
        "ti_macd_hist",
        "ti_trend_label",
    ]
    cols = [c for c in cols if c in df.columns]
    print(df[cols].to_string(index=False))

    if getattr(config, "ENABLE_TECHNICAL_INDICATORS", True):
        required = ["ti_ma20", "ti_ma60", "ti_rsi14", "ti_macd_hist", "ti_trend_label"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise AssertionError(f"缺少技术指标字段: {missing}")


if __name__ == "__main__":
    main()
