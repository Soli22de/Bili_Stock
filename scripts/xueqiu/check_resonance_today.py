
import sys
import os
import datetime
import json
import collections

# Add project root to sys.path
sys.path.append(os.getcwd())

from scripts.xueqiu.high_win_rate_backtest import HighWinRateEngine

def check_resonance(target_date=None):
    if target_date is None:
        target_date = datetime.date.today()
        
    engine = HighWinRateEngine()
    # Suppress prints from engine
    import contextlib
    with contextlib.redirect_stdout(open(os.devnull, 'w')):
        engine.load_signals()
        resonance_signals = engine.prepare_resonance_signals(window=5, min_cubes=2)
    
    # Get signals for the target date
    # Also check if there are any signals in the last 3 days that are still valid/fresh?
    # Strategy D acts on the day of resonance.
    
    todays_signals = resonance_signals.get(target_date, [])
    
    output = []
    for sig in todays_signals:
        output.append({
            'stock_code': sig['stock_code'],
            'action': sig['action'],
            'reason': sig.get('reason', 'Resonance')
        })
        
    return output

if __name__ == "__main__":
    # If date argument provided, use it
    if len(sys.argv) > 1:
        try:
            d_str = sys.argv[1]
            t_date = datetime.datetime.strptime(d_str, "%Y-%m-%d").date()
        except:
            t_date = datetime.date.today()
    else:
        t_date = datetime.date.today()
        
    signals = check_resonance(t_date)
    print(json.dumps(signals))
