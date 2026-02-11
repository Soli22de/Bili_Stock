import sys
import os
import pandas as pd

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.backtest_engine import BacktestEngine
from core.strategies import DragonStrategy

def main():
    print("Starting Backtest...")
    # Initialize strategy with default parameters
    strategy = DragonStrategy()
    
    # Initialize engine
    engine = BacktestEngine(strategy=strategy)
    
    # Run backtest
    # We don't need to specify dates here because the engine iterates over the signals file
    engine.run_backtest()
    
    print("Backtest finished.")

if __name__ == "__main__":
    main()
