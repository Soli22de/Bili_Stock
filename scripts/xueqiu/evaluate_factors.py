
import pandas as pd
import numpy as np
import logging
import sys
import os
from datetime import datetime, timedelta

# Ensure core module is importable
sys.path.append(os.getcwd())
from core.factor_miner import FactorMiner, SmartResonanceFactor, PanicReversalFactor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

def generate_mock_data(days=30, n_stocks=50, n_cubes=200):
    """Generate mock market and rebalancing data for testing."""
    logging.info("Generating Mock Data...")
    
    # 1. Dates
    end_date = datetime.now().date()
    dates = [end_date - timedelta(days=i) for i in range(days)]
    dates.reverse()
    dates = pd.to_datetime(dates)
    
    # 2. Symbols
    stocks = [f"Stock_{i:03d}" for i in range(n_stocks)]
    cubes = [f"Cube_{i:03d}" for i in range(n_cubes)]
    tiers = ['Hidden Gems', 'Legends', 'Rising Stars', 'Steady Hands', 'Others']
    
    # 3. Market Data (Random Walk)
    # Pivot format: Index=Date, Columns=Stock
    market_data = pd.DataFrame(index=dates, columns=stocks)
    for stock in stocks:
        start_price = np.random.uniform(10, 100)
        returns = np.random.normal(0.0005, 0.02, size=days) # Slight drift up
        price_path = start_price * (1 + returns).cumprod()
        market_data[stock] = price_path
        
    # 4. Rebalancing Data
    # Generate random rebalancing events
    records = []
    for date in dates:
        # 10% chance of a cube rebalancing on any day
        n_events = int(n_cubes * 0.1) 
        
        active_cubes = np.random.choice(cubes, n_events, replace=False)
        
        for cube in active_cubes:
            stock = np.random.choice(stocks)
            # Random Tier assignment (stable per cube usually, but random here is fine for test)
            tier = np.random.choice(tiers, p=[0.2, 0.1, 0.2, 0.3, 0.2])
            
            # Action
            prev_w = np.random.uniform(0, 10)
            target_w = np.random.uniform(0, 10)
            
            # Inject some logic for "PanicReversal" testing:
            # Force some "Steady Hands" to sell into downtrends
            stock_price = market_data.loc[date, stock]
            # Artificial trend check: look back 5 days
            try:
                past_price = market_data.loc[date - timedelta(days=5), stock]
                if stock_price < past_price * 0.9 and tier == 'Steady Hands':
                     # Sell hard in downtrend
                     prev_w = 20
                     target_w = 0
            except:
                pass
            
            records.append({
                'date': date,
                'cube_symbol': cube,
                'stock_symbol': stock,
                'cube_tier': tier,
                'prev_weight_adjusted': prev_w,
                'target_weight': target_w
            })
            
    rebalancing_df = pd.DataFrame(records)
    logging.info(f"Generated {len(rebalancing_df)} rebalancing records and {len(market_data)} days of market data.")
    return rebalancing_df, market_data

def evaluate_factors():
    # 1. Load Data
    rebalancing_df, market_data = generate_mock_data(days=60, n_stocks=100, n_cubes=500)
    
    # 2. Run Factor Miner
    miner = FactorMiner([
        SmartResonanceFactor(decay_window=3),
        PanicReversalFactor(trend_window=5) # Reduced window for mock
    ])
    
    factor_scores = miner.run(rebalancing_df, market_data)
    
    # 3. Evaluate Each Factor
    for factor_name, scores in factor_scores.items():
        if scores.empty:
            logging.warning(f"Skipping {factor_name} (Empty scores)")
            continue
            
        logging.info(f"\n--- Evaluating Factor: {factor_name} ---")
        
        # Calculate Forward Returns (T+1, T+5)
        # Shift market data backwards to get future returns aligned with current date
        # Ret_T+k = (Close_T+k - Close_T) / Close_T
        # Shift(-k) brings T+k to T
        
        fwd_ret_1 = (market_data.shift(-1) - market_data) / market_data
        fwd_ret_5 = (market_data.shift(-5) - market_data) / market_data
        
        # Align Data
        # Ensure we only check dates where we have both Scores and Returns
        valid_dates = scores.index.intersection(fwd_ret_1.index)
        scores = scores.loc[valid_dates]
        fwd_ret_1 = fwd_ret_1.loc[valid_dates]
        fwd_ret_5 = fwd_ret_5.loc[valid_dates]
        
        # Quantile Analysis (Top 20% vs Bottom 20%)
        # For each day, rank stocks
        long_ret_1 = []
        short_ret_1 = []
        long_ret_5 = []
        
        for date in valid_dates:
            day_scores = scores.loc[date]
            day_ret_1 = fwd_ret_1.loc[date]
            day_ret_5 = fwd_ret_5.loc[date]
            
            # Filter non-zero scores (Active signals)
            active_scores = day_scores[day_scores != 0]
            if len(active_scores) < 5:
                continue
                
            # Sort
            ranked = active_scores.sort_values()
            n = len(ranked)
            top_n = int(n * 0.2)
            if top_n == 0: top_n = 1
            
            top_stocks = ranked.tail(top_n).index
            bottom_stocks = ranked.head(top_n).index
            
            # Avg Return
            l_r1 = day_ret_1[top_stocks].mean()
            s_r1 = day_ret_1[bottom_stocks].mean()
            l_r5 = day_ret_5[top_stocks].mean()
            
            if not np.isnan(l_r1): long_ret_1.append(l_r1)
            if not np.isnan(s_r1): short_ret_1.append(s_r1)
            if not np.isnan(l_r5): long_ret_5.append(l_r5)
            
        # Results
        avg_long_1 = np.mean(long_ret_1) if long_ret_1 else 0
        avg_short_1 = np.mean(short_ret_1) if short_ret_1 else 0
        avg_long_5 = np.mean(long_ret_5) if long_ret_5 else 0
        
        logging.info(f"Days Evaluated: {len(long_ret_1)}")
        logging.info(f"Top 20% Avg Return (T+1): {avg_long_1*100:.4f}%")
        logging.info(f"Bottom 20% Avg Return (T+1): {avg_short_1*100:.4f}%")
        logging.info(f"Long-Short Spread (T+1): {(avg_long_1 - avg_short_1)*100:.4f}%")
        logging.info(f"Top 20% Avg Return (T+5): {avg_long_5*100:.4f}%")
        
        # Simple IC (Information Coefficient)
        # Correlation between Score and Future Return per day
        daily_ic = []
        for date in valid_dates:
            if date not in scores.index or date not in fwd_ret_1.index: continue
            s = scores.loc[date]
            r = fwd_ret_1.loc[date]
            # Filter 0s? Maybe not, 0 implies neutral. But for sparsity, maybe yes.
            # Let's keep 0s if they are valid stocks
            valid = ~np.isnan(s) & ~np.isnan(r)
            if valid.sum() > 5:
                # Handle constant values (std=0) which cause NaN correlation
                if s[valid].std() > 1e-6 and r[valid].std() > 1e-6:
                    ic = s[valid].corr(r[valid])
                    if not np.isnan(ic):
                        daily_ic.append(ic)
                
        avg_ic = np.mean(daily_ic) if daily_ic else 0
        ir = avg_ic / np.std(daily_ic) if daily_ic and np.std(daily_ic) > 1e-6 else 0
        
        logging.info(f"IC (Mean): {avg_ic:.4f}")
        logging.info(f"IR (Info Ratio): {ir:.4f}")

if __name__ == "__main__":
    evaluate_factors()
