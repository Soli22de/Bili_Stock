
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

def visualize_2026():
    file_path = "data/real_strategy_trades.csv"
    if not os.path.exists(file_path):
        logging.error("Trade log not found.")
        return

    df = pd.read_csv(file_path)
    df['entry_date'] = pd.to_datetime(df['entry_date'])
    df['exit_date'] = pd.to_datetime(df['exit_date'])
    
    # Filter for 2026
    df_2026 = df[df['exit_date'] >= '2026-01-01'].copy()
    
    if df_2026.empty:
        logging.info("No trades found in 2026.")
        return

    df_2026.sort_values('exit_date', inplace=True)
    
    # --- Analysis ---
    logging.info("==========================================")
    logging.info("📊 2026年近期实盘回测业绩分析 (Jan - Feb)")
    logging.info("==========================================")
    
    # Monthly Stats
    df_2026['month'] = df_2026['exit_date'].dt.to_period('M')
    monthly = df_2026.groupby('month').apply(
        lambda x: pd.Series({
            'profit': x['pnl'].sum(),
            'trades': len(x),
            'wins': len(x[x['pnl'] > 0]),
            'win_rate': len(x[x['pnl'] > 0]) / len(x)
        })
    )
    
    total_pnl_2026 = df_2026['pnl'].sum()
    
    for period, row in monthly.iterrows():
        logging.info(f"\n📅 {period} 月度表现:")
        logging.info(f"  - 净利润: {row['profit']:+.2f} 元")
        logging.info(f"  - 交易数: {int(row['trades'])} 笔")
        logging.info(f"  - 胜率:   {row['win_rate']*100:.1f}%")
        
        # Top Trades
        month_data = df_2026[df_2026['month'] == period]
        best_trade = month_data.loc[month_data['pnl'].idxmax()]
        logging.info(f"  - 🌟 最佳交易: {best_trade['symbol']} ({best_trade['reason']}) 盈利 {best_trade['pnl_pct']*100:.2f}%")
        
        worst_trade = month_data.loc[month_data['pnl'].idxmin()]
        logging.info(f"  - 💀 最差交易: {worst_trade['symbol']} ({worst_trade['reason']}) 亏损 {worst_trade['pnl_pct']*100:.2f}%")

    logging.info(f"\n📈 2026年累计净利润: {total_pnl_2026:+.2f} 元")

    # --- Visualization ---
    # Cumulative PnL Curve
    df_2026['cum_pnl'] = df_2026['pnl'].cumsum()
    
    plt.figure(figsize=(12, 6))
    plt.plot(df_2026['exit_date'], df_2026['cum_pnl'], marker='o', linestyle='-', color='b', label='Cumulative PnL')
    
    # Add annotations for big wins/losses
    for _, row in df_2026.iterrows():
        if row['pnl_pct'] > 0.10: # >10% win
            plt.annotate(f"{row['symbol']}\n+{row['pnl_pct']*100:.0f}%", 
                         (row['exit_date'], row['cum_pnl']), 
                         xytext=(0, 10), textcoords='offset points', ha='center', color='green', fontsize=8)
        elif row['pnl_pct'] < -0.08: # <-8% loss
             plt.annotate(f"{row['symbol']}\n{row['pnl_pct']*100:.0f}%", 
                         (row['exit_date'], row['cum_pnl']), 
                         xytext=(0, -15), textcoords='offset points', ha='center', color='red', fontsize=8)

    plt.title('Strategy Performance 2026 (Jan-Feb)', fontsize=14)
    plt.xlabel('Date')
    plt.ylabel('Cumulative Profit (CNY)')
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.legend()
    
    # Save
    output_path = "data/recent_performance_2026.png"
    plt.savefig(output_path)
    logging.info(f"\n📊 业绩走势图已保存至: {output_path}")
    
    # Detailed Log
    logging.info("\n📝 最近交易记录:")
    cols = ['symbol', 'entry_date', 'exit_date', 'pnl', 'pnl_pct', 'reason']
    logging.info(df_2026[cols].tail(10).to_string(index=False))

if __name__ == "__main__":
    visualize_2026()
