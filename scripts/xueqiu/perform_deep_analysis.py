
import sqlite3
import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

def perform_deep_analysis():
    db_path = r"c:\jz_code\Bili_Stock\data\cubes.db"
    if not os.path.exists(db_path):
        logging.error(f"Database not found: {db_path}")
        return

    logging.info("--- 🚀 STARTING DEEP ANALYSIS OF XUEQIU CUBES ---")
    conn = sqlite3.connect(db_path)
    
    # Load all cubes
    query = """
    SELECT 
        symbol, name, followers_count, total_gain, monthly_gain, 
        daily_gain, annualized_gain_rate, description, created_at, updated_at
    FROM cubes
    """
    try:
        df = pd.read_sql_query(query, conn)
        logging.info(f"Loaded {len(df)} cubes from database.")
        
        # --- 1. Data Cleaning & Feature Engineering ---
        
        # Convert numeric columns
        cols = ['total_gain', 'monthly_gain', 'daily_gain', 'annualized_gain_rate', 'followers_count']
        for col in cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        # Convert timestamps
        # In SQLite, timestamps are stored as strings (e.g., '2023-01-01 12:00:00') or None
        def parse_ts(ts):
            if pd.isna(ts) or ts == 0 or ts == '0':
                return None
            try:
                return pd.to_datetime(ts)
            except:
                return None

        df['created_dt'] = df['created_at'].apply(parse_ts)
        
        # Handle None in created_dt
        now = datetime.now()
        df['days_active'] = df['created_dt'].apply(lambda x: (now - x).days if pd.notnull(x) else 0)
        
        # Calculate "Stability Score" (Simple proxy: Total Gain / Monthly Volatility is hard without history)
        # Instead, let's use: Consistency = Total Gain / Days Active (Daily Alpha)
        df['daily_alpha'] = df['total_gain'] / df['days_active'].replace(0, 1)
        
        # Calculate "Popularity Score" (Followers log scale)
        df['popularity_score'] = np.log1p(df['followers_count'])

        # --- 2. Segmentation Strategy ---
        
        logging.info("\n--- 📊 SEGMENTATION ANALYSIS ---")
        
        # Segment A: The "Legends" (High Return, High Popularity, Long History)
        # Criteria: Total Gain > 50%, Followers > 1000, Active > 365 days
        legends = df[
            (df['total_gain'] > 50) & 
            (df['followers_count'] > 1000) & 
            (df['days_active'] > 365)
        ].sort_values(by='total_gain', ascending=False)
        logging.info(f"🏆 LEGENDS (Proven Winners): {len(legends)}")
        if not legends.empty:
            logging.info(legends[['symbol', 'name', 'total_gain', 'followers_count']].head(5).to_string(index=False))

        # Segment B: The "Hidden Gems" (High Return, Low Popularity)
        # Criteria: Total Gain > 30%, Followers < 500, Active > 180 days
        hidden_gems = df[
            (df['total_gain'] > 30) & 
            (df['followers_count'] < 500) & 
            (df['days_active'] > 180)
        ].sort_values(by='total_gain', ascending=False)
        logging.info(f"\n💎 HIDDEN GEMS (Undiscovered Alpha): {len(hidden_gems)}")
        if not hidden_gems.empty:
            logging.info(hidden_gems[['symbol', 'name', 'total_gain', 'followers_count']].head(5).to_string(index=False))

        # Segment C: The "Rising Stars" (High Monthly Gain, Recent)
        # Criteria: Monthly Gain > 10%, Active < 180 days
        rising_stars = df[
            (df['monthly_gain'] > 10) & 
            (df['days_active'] < 180) &
            (df['days_active'] > 30) # Filter brand new noise
        ].sort_values(by='monthly_gain', ascending=False)
        logging.info(f"\n🚀 RISING STARS (Momentum): {len(rising_stars)}")
        if not rising_stars.empty:
            logging.info(rising_stars[['symbol', 'name', 'monthly_gain', 'days_active']].head(5).to_string(index=False))

        # Segment D: The "Steady Hands" (Consistent Daily Alpha, Low Drawdown Proxy)
        # Criteria: Annualized > 10%, Total > 20%, Active > 2 years
        steady_hands = df[
            (df['annualized_gain_rate'] > 10) & 
            (df['total_gain'] > 20) & 
            (df['days_active'] > 730)
        ].sort_values(by='annualized_gain_rate', ascending=False)
        logging.info(f"\n🛡️ STEADY HANDS (Long Term Value): {len(steady_hands)}")
        if not steady_hands.empty:
            logging.info(steady_hands[['symbol', 'name', 'annualized_gain_rate', 'days_active']].head(5).to_string(index=False))

        # --- 3. Keyword Analysis (Sector/Theme) ---
        logging.info("\n--- 🏷️ THEMATIC ANALYSIS ---")
        
        keywords = {
            "ETF": ["ETF", "指数", "定投"],
            "Convertible Bond": ["可转债", "双低", "摊大饼"],
            "Tech/Growth": ["科技", "成长", "芯片", "AI", "半导体"],
            "Dividend/Value": ["红利", "高股息", "价值", "银行"],
            "Small Cap": ["小盘", "微盘", "国证2000"],
            "Quant": ["量化", "轮动", "网格"]
        }
        
        for category, kws in keywords.items():
            mask = df['name'].str.contains('|'.join(kws), case=False, na=False) | \
                   df['description'].str.contains('|'.join(kws), case=False, na=False)
            subset = df[mask]
            top_performer = subset.sort_values(by='total_gain', ascending=False).head(1)
            top_name = top_performer['name'].values[0] if not top_performer.empty else "N/A"
            top_gain = top_performer['total_gain'].values[0] if not top_performer.empty else 0
            
            logging.info(f"{category.ljust(18)}: {len(subset)} cubes. Top: {top_name} ({top_gain}%)")

        # --- 4. Export Actionable Lists ---
        logging.info("\n--- 💾 EXPORTING LISTS ---")
        os.makedirs("data/analysis_reports", exist_ok=True)
        
        # Save Top 100 of each category
        legends.head(100).to_csv("data/analysis_reports/pool_legends.csv", index=False)
        hidden_gems.head(100).to_csv("data/analysis_reports/pool_hidden_gems.csv", index=False)
        rising_stars.head(100).to_csv("data/analysis_reports/pool_rising_stars.csv", index=False)
        steady_hands.head(100).to_csv("data/analysis_reports/pool_steady_hands.csv", index=False)
        
        logging.info("Saved top 100 lists to 'data/analysis_reports/'")
        
        # --- 5. Summary Stats ---
        total_profitable = len(df[df['total_gain'] > 0])
        logging.info(f"\nOverall Health: {total_profitable}/{len(df)} ({total_profitable/len(df):.1%}) are profitable.")
        
    except Exception as e:
        logging.error(f"Error during deep analysis: {e}", exc_info=True)
    finally:
        conn.close()

if __name__ == "__main__":
    perform_deep_analysis()
