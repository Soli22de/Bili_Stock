import pandas as pd
import os

def analyze_failures():
    file_path = 'analysis/strategy_e_trade_history.csv'
    if not os.path.exists(file_path):
        print("Trade history file not found.")
        return

    df = pd.read_csv(file_path)
    df['Entry Date'] = pd.to_datetime(df['Entry Date'])
    df['Exit Date'] = pd.to_datetime(df['Exit Date'])
    df['Year'] = df['Entry Date'].dt.year
    df['Duration'] = (df['Exit Date'] - df['Entry Date']).dt.days

    # Filter for 2024 (The worst year)
    df_2024 = df[df['Year'] == 2024]
    
    print("\n=== 2024 Failure Analysis ===")
    print(f"Total Trades: {len(df_2024)}")
    print(f"Win Rate: {(len(df_2024[df_2024['PnL']>0])/len(df_2024)*100):.1f}%")
    
    losses = df_2024[df_2024['PnL'] <= 0]
    print(f"\nLosses Summary (2024):")
    # Show worst 5
    print(losses[['Stock', 'Entry Date', 'Exit Date', 'Duration', 'Return %']].sort_values('Return %').head(5))
    
    print("\nLoss Distribution (Stats):")
    print(losses['Return %'].describe())
    
    # Check if losses are clustering around Stop Loss (-10%)
    stop_loss_hits = len(losses[losses['Return %'] <= -9.0])
    print(f"\nStop Loss Hits (<= -9%): {stop_loss_hits} / {len(losses)} ({stop_loss_hits/len(losses)*100:.1f}%)")
    
    # Monthly breakdown
    df_2024['Month'] = df_2024['Entry Date'].dt.month
    monthly = df_2024.groupby('Month')['PnL'].sum()
    print("\nMonthly PnL (2024):")
    print(monthly)

    # Global Stats
    print("\n=== Global Stats (2022-2026) ===")
    wins = df[df['PnL']>0]
    losses_all = df[df['PnL']<=0]
    
    print(f"Avg Duration (Wins): {wins['Duration'].mean():.1f} days")
    print(f"Avg Duration (Losses): {losses_all['Duration'].mean():.1f} days")
    
    print(f"Avg Return (Wins): {wins['Return %'].mean():.2f}%")
    print(f"Avg Return (Losses): {losses_all['Return %'].mean():.2f}%")

if __name__ == "__main__":
    analyze_failures()