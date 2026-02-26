
import sqlite3
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

def explore_data():
    logging.info("--- 🕵️ DATA EXPLORATION & FEATURE MINING ---")
    
    # 1. Load Data
    db_path = "data/cubes.db"
    conn = sqlite3.connect(db_path)
    
    # Load Cubes Metadata (for Tiers)
    logging.info("Loading Cube Metadata...")
    cubes_df = pd.read_sql_query("SELECT symbol, name, total_gain, followers_count, created_at FROM cubes", conn)
    
    # Helper to calculate tiers (Simplified logic from perform_deep_analysis.py)
    # Note: In a real scenario, we'd load the pre-calculated CSVs, but here we do it on fly for self-containment
    def assign_tier(row):
        # Timestamps might be large ints or strings, handle carefully
        # Simple heuristic here
        gain = row['total_gain'] if pd.notnull(row['total_gain']) else 0
        followers = row['followers_count'] if pd.notnull(row['followers_count']) else 0
        
        if gain > 50 and followers > 1000:
            return "Legends"
        elif gain > 30 and followers < 500:
            return "Hidden Gems"
        elif followers > 5000: # Popular but maybe not high gain
            return "Rising Stars" 
        else:
            return "Others"

    cubes_df['tier'] = cubes_df.apply(assign_tier, axis=1)
    tier_map = cubes_df.set_index('symbol')['tier'].to_dict()
    logging.info(f"Cube Tiers: {cubes_df['tier'].value_counts().to_dict()}")

    # Load Rebalancing History
    logging.info("Loading Rebalancing History...")
    try:
        # Limit for speed if needed, but let's try full load
        history_df = pd.read_sql_query("SELECT * FROM rebalancing_history", conn)
    except:
        logging.warning("Rebalancing history table empty or missing. Generating MOCK data for demonstration.")
        history_df = pd.DataFrame() # Trigger mock generation

    if history_df.empty:
        # Mock Data Generation (Same logic as before but inline)
        dates = pd.date_range(end=datetime.now(), periods=60)
        stocks = [f"Stock_{i:03d}" for i in range(50)]
        records = []
        for date in dates:
            for _ in range(100): # 100 trades per day
                records.append({
                    'date': date,
                    'cube_symbol': np.random.choice(cubes_df['symbol']),
                    'stock_symbol': np.random.choice(stocks),
                    'prev_weight_adjusted': np.random.uniform(0, 10),
                    'target_weight': np.random.uniform(0, 10),
                })
        history_df = pd.DataFrame(records)
    
    # Preprocess History
    history_df['date'] = pd.to_datetime(history_df['date'])
    history_df['cube_tier'] = history_df['cube_symbol'].map(tier_map).fillna("Others")
    history_df['weight_delta'] = history_df['target_weight'] - history_df['prev_weight_adjusted']
    history_df['action'] = history_df['weight_delta'].apply(lambda x: 'BUY' if x > 0 else 'SELL')
    
    # 2. Feature Engineering (The "Stew")
    logging.info("Cooking Features...")
    
    # Group by Date + Stock
    daily_groups = history_df.groupby(['date', 'stock_symbol'])
    
    features = pd.DataFrame()
    
    # Feature 1: Total Net Weight Change (Global Conviction)
    features['net_weight_change'] = daily_groups['weight_delta'].sum()
    
    # Feature 2: Buy/Sell Count Imbalance (Consensus Score)
    # Buy Count - Sell Count
    features['consensus_score'] = daily_groups['action'].apply(lambda x: (x == 'BUY').sum() - (x == 'SELL').sum())
    
    # Feature 3: Legends Net Weight Change (Smart Money Flow)
    legends_mask = history_df['cube_tier'] == 'Legends'
    features['legends_flow'] = history_df[legends_mask].groupby(['date', 'stock_symbol'])['weight_delta'].sum()
    
    # Feature 4: Hidden Gems Buying Intensity (Alpha Hunter Conviction)
    # Sum of weight delta ONLY for buys by Hidden Gems
    gems_mask = (history_df['cube_tier'] == 'Hidden Gems') & (history_df['weight_delta'] > 0)
    features['hidden_gems_buy_pressure'] = history_df[gems_mask].groupby(['date', 'stock_symbol'])['weight_delta'].sum()
    
    # Feature 5: Rising Star First Buy (New Trend?)
    # Is this the first time a Rising Star bought this stock in the last N days? 
    # Hard to vectorize fully without rolling, let's approximate with "Buy Count by Rising Stars"
    stars_mask = (history_df['cube_tier'] == 'Rising Stars') & (history_df['weight_delta'] > 0)
    features['rising_stars_buy_count'] = history_df[stars_mask].groupby(['date', 'stock_symbol']).size()
    
    # Feature 6: Divergence (Legends Buy vs Others Sell)
    # Logic: Legends Buy + Others Sell = High Quality Divergence
    others_mask = history_df['cube_tier'] == 'Others'
    others_sell = history_df[others_mask & (history_df['weight_delta'] < 0)].groupby(['date', 'stock_symbol'])['weight_delta'].sum()
    features['smart_divergence'] = features['legends_flow'].fillna(0) - others_sell.fillna(0) # Subtract negative sells -> Add
    
    # Fill NaNs
    features = features.fillna(0)
    
    # 3. Brute-Force Evaluation
    logging.info("Running Brute-Force Evaluation...")
    
    # Mock Market Returns (Since we might not have full daily history for all stocks)
    # In real life: Load 'stock_prices.csv'
    unique_dates = features.index.get_level_values('date').unique()
    unique_stocks = features.index.get_level_values('stock_symbol').unique()
    
    # Generate random returns for testing
    market_returns = pd.DataFrame(
        np.random.normal(0, 0.02, size=(len(unique_dates), len(unique_stocks))),
        index=unique_dates,
        columns=unique_stocks
    )
    
    # Calculate Forward Returns (T+1, T+3, T+5)
    # Stack to match features index (Date, Stock)
    fwd_ret_1 = market_returns.shift(-1).stack()
    fwd_ret_3 = market_returns.shift(-3).stack()
    fwd_ret_5 = market_returns.shift(-5).stack()
    
    # Align
    aligned_data = features.join(fwd_ret_1.rename('ret_1d'), how='inner')
    aligned_data = aligned_data.join(fwd_ret_3.rename('ret_3d'), how='inner')
    aligned_data = aligned_data.join(fwd_ret_5.rename('ret_5d'), how='inner')
    
    # Calculate IC (Information Coefficient) - Spearman Rank Correlation
    results = {}
    feature_cols = ['net_weight_change', 'consensus_score', 'legends_flow', 
                   'hidden_gems_buy_pressure', 'rising_stars_buy_count', 'smart_divergence']
    
    for col in feature_cols:
        ic_1 = aligned_data[col].corr(aligned_data['ret_1d'], method='spearman')
        ic_3 = aligned_data[col].corr(aligned_data['ret_3d'], method='spearman')
        ic_5 = aligned_data[col].corr(aligned_data['ret_5d'], method='spearman')
        results[col] = {'IC_1D': ic_1, 'IC_3D': ic_3, 'IC_5D': ic_5}
        
    # 4. Output
    results_df = pd.DataFrame(results).T
    logging.info("\n--- 🏆 FEATURE RANKING (Sorted by T+3 IC) ---")
    logging.info(results_df.sort_values(by='IC_3D', ascending=False))
    
    best_feature = results_df['IC_3D'].idxmax()
    logging.info(f"\n✨ The Winner is: {best_feature}")

    conn.close()

if __name__ == "__main__":
    explore_data()
