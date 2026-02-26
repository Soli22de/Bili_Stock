import sys
import os
sys.path.append(os.getcwd())

from scripts.xueqiu.high_win_rate_backtest import HighWinRateEngine
import datetime
import pandas as pd

def main():
    engine = HighWinRateEngine()
    engine.load_signals()
    resonance_signals = engine.prepare_resonance_signals(window=5, min_cubes=2)
    
    print("Checking resonance signals > 2026-01-01")
    count = 0
    recent_signals = []
    
    for dt, sigs in resonance_signals.items():
        if dt >= datetime.date(2026, 1, 1):
            count += len(sigs)
            for s in sigs:
                print(f"{dt}: {s}")
                recent_signals.append({
                    'date': dt,
                    'stock_code': s['stock_code'],
                    'action': s['action'],
                    'reason': s.get('reason', '')
                })
                
    print(f"Total signals found: {count}")
    
    if recent_signals:
        os.makedirs('analysis', exist_ok=True)
        pd.DataFrame(recent_signals).to_csv('analysis/high_win_rate_opportunities.csv', index=False)
        print("Saved to analysis/high_win_rate_opportunities.csv")
    else:
        print("No recent signals found!")

if __name__ == "__main__":
    main()
