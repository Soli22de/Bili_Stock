import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

def visualize_growth():
    # Load Data
    trades_path = 'data/growth_backtest_trades.csv'
    bloggers_path = 'data/growth_backtest_bloggers.csv'
    
    if not os.path.exists(trades_path) or not os.path.exists(bloggers_path):
        print("Data files not found. Run run_growth_backtest.py first.")
        return

    df_trades = pd.read_csv(trades_path)
    df_bloggers = pd.read_csv(bloggers_path)
    
    # Setup Plot
    plt.style.use('seaborn-v0_8')
    fig = plt.figure(figsize=(15, 10))
    gs = fig.add_gridspec(2, 2)
    
    # 1. Equity Curve (Capital Growth)
    ax1 = fig.add_subplot(gs[0, :])
    
    # Reconstruct equity curve from trade log
    # Note: The log only records CLOSED trades. 
    # To make a smooth curve, we should ideally track daily, but let's use trade close times.
    df_trades['date'] = pd.to_datetime(df_trades['date'])
    df_trades = df_trades.sort_values('date')
    
    # We need to filter for executed trades to see capital changes
    df_exec = df_trades[df_trades['action'] == 'TRADE'].copy()
    
    if not df_exec.empty:
        ax1.plot(df_exec['date'], df_exec['capital_after'], label='Portfolio Equity', color='green', linewidth=2)
        ax1.set_title('Growth Strategy Equity Curve (Account Value)', fontsize=14)
        ax1.set_ylabel('Capital (RMB)')
        ax1.axhline(y=100000, color='r', linestyle='--', alpha=0.5, label='Initial Capital')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
    
    # 2. Top Bloggers (Bar Chart)
    ax2 = fig.add_subplot(gs[1, 0])
    top_bloggers = df_bloggers.head(10)
    sns.barplot(data=top_bloggers, x='score', y='blogger', ax=ax2, palette='viridis')
    ax2.set_title('Top 10 Blogger Reputation Scores', fontsize=12)
    ax2.set_xlabel('Reputation Score (Start=100)')
    
    # 3. Win Rate vs Score (Scatter)
    ax3 = fig.add_subplot(gs[1, 1])
    # Calculate Win Rate
    df_bloggers['total_trades'] = df_bloggers['wins'] + df_bloggers['losses']
    df_bloggers['win_rate'] = df_bloggers['wins'] / df_bloggers['total_trades']
    
    # Filter for bloggers with at least 1 trade
    active_bloggers = df_bloggers[df_bloggers['total_trades'] > 0]
    
    sns.scatterplot(data=active_bloggers, x='score', y='win_rate', size='total_trades', sizes=(20, 200), ax=ax3, alpha=0.7)
    ax3.set_title('Blogger Reliability: Score vs Win Rate', fontsize=12)
    ax3.set_xlabel('Reputation Score')
    ax3.set_ylabel('Win Rate (0-1)')
    ax3.axhline(y=0.5, color='gray', linestyle='--')
    ax3.axvline(x=100, color='gray', linestyle='--')
    
    # Annotate top guy
    if not top_bloggers.empty:
        best = top_bloggers.iloc[0]
        ax3.text(best['score'], best['wins']/(best['wins']+best['losses']), f"  {best['blogger']}", fontweight='bold')

    plt.tight_layout()
    output_path = 'data/growth_backtest_report.png'
    plt.savefig(output_path)
    print(f"Visualization saved to {output_path}")

if __name__ == "__main__":
    visualize_growth()
