# Data Dictionary

## 1. Market Data (Stock & Index)
Source: Akshare (Sina/Eastmoney interface)
Frequency: Daily
Adjust: Post-adjusted (qfq)

| Field | Type | Description | Cleaning Rule |
|-------|------|-------------|---------------|
| date | Date | Trade date (YYYY-MM-DD) | Filtered for trading days only |
| open | Float | Opening price | None |
| high | Float | Highest price | None |
| low | Float | Lowest price | None |
| close | Float | Closing price | None |
| volume | Float | Trading volume | Suspensions checked if vol=0 |
| amount | Float | Trading amount | None |
| pct_change | Float | Percentage change | Used for limit detection (>9.5%) |

## 2. Trading Signals (Cube Rebalancing)
Source: Xueqiu Cube Crawling
Frequency: Event-driven (Real-time/Daily)

| Field | Type | Description | Cleaning Rule |
|-------|------|-------------|---------------|
| stock_code | String | Stock identifier (e.g., SH600519) | Validated against stock list |
| stock_name | String | Chinese name | ST stocks filtered |
| action | Enum | BUY / SELL | Mapped to signal direction |
| price | Float | Transaction price | Checked against daily range |
| time | Datetime | Transaction timestamp | Filtered by market hours |
| delta | Float | Position change ratio | None |

## 3. Blogger Opinions (Sentiment)
Source: Bilibili/Xueqiu Comments
Frequency: Event-driven

| Field | Type | Description | Cleaning Rule |
|-------|------|-------------|---------------|
| blogger_name | String | Author name | High reputation whitelist |
| sentiment | Enum | Bullish/Bearish/Neutral | LLM extracted |
| ocr_verified | Bool | Has real-trade screenshot | OCR confidence > 0.8 |
| win_rate | Float | Historical accuracy | Rolling 10-trade window |
| weight | Float | Calculated influence weight | Range [0.5, 1.5] |

## 4. Macro Indicators (Regime)
Source: Akshare/Macro
Frequency: Daily/Monthly

| Field | Type | Description | Cleaning Rule |
|-------|------|-------------|---------------|
| sh000300 | Float | CSI 300 Index Close | Used for Regime calc |
| MA50 | Float | 50-day Moving Average | Computed |
| MA200 | Float | 200-day Moving Average | Computed |
| Regime | Enum | Bull/Bear/Sideways | MA Cross + Volatility |
