# Smart Momentum Quant

A quantitative stock selection system for the China A-share market that tracks "smart money" flows from Xueqiu portfolio rebalancing data to generate consensus-driven trading signals.

## Overview

Retail investors on Xueqiu (a major Chinese investing platform) manage public portfolios with transparent rebalancing histories. This project collects 55,000+ rebalancing records from the platform, filters them down to 1,400+ elite portfolios (by return rate and follower count), and builds a consensus signal: when multiple elite portfolios independently buy the same stock within a short window, that stock is flagged as a candidate.

The core insight is that these elite portfolios collectively exhibit stock-picking alpha -- but only under certain market conditions. In bull markets, they tend to take profits early, so the signal is actually reversed during uptrend regimes. This market-regime adaptation flips the factor's Top-Bottom spread from -0.46% to +0.57%.

## Key Features

**Data Asset** -- 55,000+ portfolio rebalancing histories scraped from Xueqiu, stored in SQLite. Elite pool of 1,400+ portfolios filtered by cumulative return > 0% and follower count > 40.

**Signal Engine** -- 3-day rolling consensus algorithm. A buy signal fires when 2 or more elite portfolios purchase the same stock within a 5-day window.

**Market Regime Adaptation** -- Bull/bear regime detection reverses the factor in uptrend conditions where elite portfolios systematically underperform due to premature profit-taking.

**Custom Backtest Engine** -- Handles real-world A-share constraints: trading suspensions, limit-up/limit-down restrictions, T+1 settlement, and trading day alignment.

**Rigorous Research Pipeline** -- 5-phase A-to-E development process. Hard elimination rules: 12 consecutive months underperforming, Calmar < 0, or max drawdown > 30%.

**Version Evolution** -- V3 through V6.1 with "hypothesis-backtest-lock" methodology. 81 parameter sensitivity experiments. Best configuration: Calmar 0.373, win rate 57.6% (2010-2025).

## Results

| Metric | Value | Period |
|--------|-------|--------|
| Win Rate | 57.6% | 2010-2025 |
| Calmar Ratio (best) | 0.373 | 2010-2025 |
| Factor Spread (regime-adapted) | +0.57% | Full sample |
| Parameter Experiments | 81 | Sensitivity grid |

## Tech Stack

Python, SQLite, BaoStock, Pandas, NumPy, custom backtest engine

## Project Structure

```
core/               # Backtest engine, data provider, risk engine, signal extraction
research/            # Versioned baselines v3-v6.1 (code/ output/ report/)
scripts/xueqiu/      # Crawlers, strategy scripts, daily automation
data/                # SQLite databases (not committed)
```
