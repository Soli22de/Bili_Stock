
import pandas as pd
import numpy as np
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Union

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

class BaseFactor(ABC):
    """
    Abstract Base Class for Xueqiu Behavioral Factors.
    """
    def __init__(self, name: str, params: Optional[Dict] = None):
        self.name = name
        self.params = params if params else {}

    @abstractmethod
    def compute(self, rebalancing_df: pd.DataFrame, market_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """
        Compute daily factor scores for stocks.
        
        Args:
            rebalancing_df (pd.DataFrame): Must contain columns: 
                ['date', 'stock_symbol', 'cube_symbol', 'prev_weight_adjusted', 'target_weight', 'cube_tier']
            market_data (pd.DataFrame, optional): Daily OHLCV data. 
                Index: Date, Columns: ['stock_symbol', 'close', 'volume', etc.]
        
        Returns:
            pd.DataFrame: Factor scores. Index: Date, Columns: Stock Symbols.
        """
        pass

    def _normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """Z-Score normalization per day (cross-sectional)."""
        return df.apply(lambda x: (x - x.mean()) / x.std(), axis=1)

class SmartResonanceFactor(BaseFactor):
    """
    Factor 1: Tiered Smart Money Resonance (阶层共振因子)
    
    Logic: 
    - Weights 'Hidden Gems' (High Alpha) heavily.
    - Weights 'Legends' (High Beta/Stability) moderately.
    - Captures the consensus of the most profitable traders.
    
    Formula:
        Score_i_t = Sum(Weight_c * Delta_Position_c_i_t) for all cubes c
        Where Weight_c depends on Cube Tier (Hidden Gem > Legend > Others)
    """
    def __init__(self, decay_window=5):
        super().__init__("SmartResonance", {"decay_window": decay_window})
        self.tier_weights = {
            "Hidden Gems": 3.0,  # Highest weight for alpha hunters
            "Legends": 1.5,      # Moderate weight for stability
            "Rising Stars": 1.2, # Momentum
            "Steady Hands": 1.0, # Baseline
            "Others": 0.1        # Noise
        }

    def compute(self, rebalancing_df: pd.DataFrame, market_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        df = rebalancing_df.copy()
        
        # 1. Calculate Position Delta
        df['delta'] = df['target_weight'] - df['prev_weight_adjusted']
        
        # 2. Apply Tier Weights
        # Map cube_tier to weight, default to 0.1
        df['tier_weight'] = df['cube_tier'].map(self.tier_weights).fillna(0.1)
        df['weighted_delta'] = df['delta'] * df['tier_weight']
        
        # 3. Pivot to Matrix: Index=Date, Columns=Stock, Values=Sum(Weighted Delta)
        # Note: We assume 'date' is already datetime
        daily_scores = df.pivot_table(
            index='date', 
            columns='stock_symbol', 
            values='weighted_delta', 
            aggfunc='sum'
        ).fillna(0)
        
        # 4. Apply Time Decay (Rolling Sum) to capture "Persistent Buying"
        # A single day buy is good, sustained buying over 3-5 days is better.
        window = self.params['decay_window']
        smooth_scores = daily_scores.rolling(window=window, min_periods=1).sum()
        
        return smooth_scores

class PanicReversalFactor(BaseFactor):
    """
    Factor 2: Institutional Capitulation (异常割肉反转因子)
    
    Logic:
    - When 'Steady Hands' or 'Legends' (who usually hold long-term) suddenly sell heavily 
      after a price drop, it often marks a local bottom (Capitulation).
    - Signal is POSITIVE (Buy) when they Sell in a downtrend.
    
    Formula:
        Score = -1 * (Sell_Volume_Legends) * (1 if Price_Trend < 0 else 0)
        (We invert the sell to make it a positive buy signal)
    """
    def __init__(self, trend_window=10):
        super().__init__("PanicReversal", {"trend_window": trend_window})

    def compute(self, rebalancing_df: pd.DataFrame, market_data: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        if market_data is None:
            logging.warning("PanicReversalFactor requires market_data for trend detection. Returning empty.")
            return pd.DataFrame()

        df = rebalancing_df.copy()
        
        # 1. Filter for "Heavy Selling" by "Steady Hands" or "Legends"
        # Significant Sell: delta < -5%
        df['delta'] = df['target_weight'] - df['prev_weight_adjusted']
        
        target_tiers = ['Steady Hands', 'Legends']
        mask = (df['cube_tier'].isin(target_tiers)) & (df['delta'] < -5)
        capitulation = df[mask].copy()
        
        # 2. Aggregate Selling Intensity
        # We take the absolute value of selling as the "Intensity"
        capitulation['sell_intensity'] = capitulation['delta'].abs()
        
        sell_matrix = capitulation.pivot_table(
            index='date', 
            columns='stock_symbol', 
            values='sell_intensity', 
            aggfunc='sum'
        ).fillna(0)
        
        # 3. Calculate Market Trend (Stock specific)
        # Need to align market data close prices to the sell matrix
        # Assuming market_data has a MultiIndex (Date, Stock) or we pivot it
        # Let's assume market_data is a DataFrame with MultiIndex: (date, symbol) -> close
        
        # For simplicity in this framework, let's assume market_data passed is a Pivot of Close Prices
        # Index=Date, Columns=Stock
        closes = market_data
        
        # Calculate Trend: (Close - MA(N)) / MA(N)
        window = self.params['trend_window']
        ma = closes.rolling(window=window).mean()
        trend = (closes - ma) / ma
        
        # 4. Signal Generation
        # Signal = Sell_Intensity * (1 if Trend < -0.05 else 0)
        # We want to buy when they sell into a deep downtrend (>5% below MA)
        
        # Align indexes
        common_dates = sell_matrix.index.intersection(trend.index)
        common_stocks = sell_matrix.columns.intersection(trend.columns)
        
        sells = sell_matrix.loc[common_dates, common_stocks]
        mkt_trend = trend.loc[common_dates, common_stocks]
        
        # The Factor: High Score = High Conviction Reversal
        # We only care when Trend is negative (Downtrend)
        reversal_signal = sells * (mkt_trend < -0.05).astype(int)
        
        return reversal_signal

class FactorMiner:
    """Factory and Runner for Factors"""
    def __init__(self, factors: List[BaseFactor]):
        self.factors = factors

    def run(self, rebalancing_df, market_data=None):
        results = {}
        for factor in self.factors:
            logging.info(f"Computing factor: {factor.name}...")
            try:
                scores = factor.compute(rebalancing_df, market_data)
                results[factor.name] = scores
            except Exception as e:
                logging.error(f"Failed to compute {factor.name}: {e}")
        return results
