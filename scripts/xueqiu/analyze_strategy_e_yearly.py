import pandas as pd
import os

def main():
    file_path = 'analysis/strategy_e_trade_history.csv'
    if not os.path.exists(file_path):
        print("No trade history found.")
        return
        
    df = pd.read_csv(file_path)
    df['Entry Date'] = pd.to_datetime(df['Entry Date'])
    df['Year'] = df['Entry Date'].dt.year
    
    print("\n=== Strategy E (Top Guru) Yearly Performance (2022-2026) ===")
    print(f"{'Year':<6} | {'Trades':<6} | {'Win Rate':<8} | {'Total PnL':<12} | {'Avg Return':<10}")
    print("-" * 55)
    
    for year in sorted(df['Year'].unique()):
        df_year = df[df['Year'] == year]
        trades = len(df_year)
        wins = len(df_year[df_year['PnL'] > 0])
        win_rate = (wins / trades * 100) if trades > 0 else 0
        total_pnl = df_year['PnL'].sum()
        avg_return = df_year['Return %'].mean()
        
        print(f"{year:<6} | {trades:<6} | {win_rate:6.1f}% | {total_pnl:12.2f} | {avg_return:9.2f}%")
        
    print("-" * 55)
    print(f"Total  | {len(df):<6} | {(len(df[df['PnL']>0])/len(df)*100):6.1f}% | {df['PnL'].sum():12.2f} | {df['Return %'].mean():9.2f}%")

if __name__ == "__main__":
    main()