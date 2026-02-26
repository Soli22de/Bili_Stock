
import sqlite3
import pandas as pd
import os
import matplotlib.pyplot as plt
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

def analyze_cubes():
    db_path = r"c:\jz_code\Bili_Stock\data\cubes.db"
    if not os.path.exists(db_path):
        logging.error(f"Database not found: {db_path}")
        return

    logging.info("Connecting to database...")
    conn = sqlite3.connect(db_path)
    
    # Load data into DataFrame
    query = """
    SELECT 
        symbol, name, followers_count, total_gain, monthly_gain, 
        daily_gain, annualized_gain_rate, description, created_at
    FROM cubes
    """
    try:
        df = pd.read_sql_query(query, conn)
        logging.info(f"Loaded {len(df)} cubes from database.")
        
        # Data Cleaning
        # Convert columns to numeric, coercing errors
        cols_to_numeric = ['total_gain', 'monthly_gain', 'daily_gain', 'annualized_gain_rate', 'followers_count']
        for col in cols_to_numeric:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
        # Basic Stats
        logging.info("\n--- Basic Statistics ---")
        logging.info(f"Total Cubes: {len(df)}")
        logging.info(f"Profitable Cubes (>0% Total Gain): {len(df[df['total_gain'] > 0])} ({len(df[df['total_gain'] > 0])/len(df):.1%})")
        logging.info(f"High Return Cubes (>50% Total Gain): {len(df[df['total_gain'] > 50])} ({len(df[df['total_gain'] > 50])/len(df):.1%})")
        logging.info(f"Super Cubes (>100% Total Gain): {len(df[df['total_gain'] > 100])} ({len(df[df['total_gain'] > 100])/len(df):.1%})")
        logging.info(f"Popular Cubes (>1000 Followers): {len(df[df['followers_count'] > 1000])}")
        
        # Distribution
        logging.info("\n--- Performance Distribution ---")
        logging.info(df[['total_gain', 'monthly_gain', 'annualized_gain_rate', 'followers_count']].describe().to_string())
        
        # Top Performers
        logging.info("\n--- Top 10 by Total Gain ---")
        top_gain = df.sort_values(by='total_gain', ascending=False).head(10)
        logging.info(top_gain[['symbol', 'name', 'total_gain', 'followers_count']].to_string(index=False))
        
        # Top Popular
        logging.info("\n--- Top 10 by Followers ---")
        top_popular = df.sort_values(by='followers_count', ascending=False).head(10)
        logging.info(top_popular[['symbol', 'name', 'total_gain', 'followers_count']].to_string(index=False))
        
        # Strategy Candidates: "Smart Money"
        # Criteria: >20% total gain, >100 followers (to filter complete randoms), >0 monthly gain (active recently?)
        candidates = df[
            (df['total_gain'] > 20) & 
            (df['followers_count'] > 100)
        ].sort_values(by='total_gain', ascending=False)
        
        logging.info(f"\n--- Strategy Candidates (Gain > 20%, Followers > 100) ---")
        logging.info(f"Found {len(candidates)} potential 'Smart Money' candidates.")
        logging.info(candidates[['symbol', 'name', 'total_gain', 'followers_count']].head(10).to_string(index=False))
        
        # Save candidates for next step
        candidate_file = "data/smart_money_candidates.csv"
        candidates.to_csv(candidate_file, index=False)
        logging.info(f"\nSaved {len(candidates)} candidates to {candidate_file}")

    except Exception as e:
        logging.error(f"Error during analysis: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    analyze_cubes()
