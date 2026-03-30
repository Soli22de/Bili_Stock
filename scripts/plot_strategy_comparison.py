
import pandas as pd
import matplotlib.pyplot as plt
import os
import sys

def main():
    root = os.getcwd()
    ENABLE_LOG_SCALE = True
    
    # Path to the equity curves CSV
    csv_path = os.path.join(root, "research/baseline_v6_1/output/strategy_comparison_equity_curves_2019_2025.csv")
    
    if not os.path.exists(csv_path):
        print(f"Error: Data file not found at {csv_path}")
        return
        
    print(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path)
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    
    # Merge Benchmarks
    # 1. ChiNext
    chinext_path = os.path.join(root, "research/baseline_v6_1/output/chinext_benchmark_2019_2025.csv")
    if os.path.exists(chinext_path):
        print(f"Loading ChiNext from {chinext_path}...")
        bn = pd.read_csv(chinext_path)
        bn['date'] = pd.to_datetime(bn['date'])
        bn.set_index('date', inplace=True)
        df = df.join(bn, how='left')
        df['ChiNext_Equity'] = df['ChiNext_Equity'].ffill()
        # Normalize
        valid = df['ChiNext_Equity'].dropna()
        if not valid.empty:
            sv = valid.iloc[0]
            df['ChiNext_Equity'] = df['ChiNext_Equity'] / sv

    # 2. CSI 300
    csi300_path = os.path.join(root, "research/baseline_v6_1/output/csi300_benchmark_2019_2025.csv")
    if os.path.exists(csi300_path):
        print(f"Loading CSI 300 from {csi300_path}...")
        bn = pd.read_csv(csi300_path)
        bn['date'] = pd.to_datetime(bn['date'])
        bn.set_index('date', inplace=True)
        df = df.join(bn, how='left')
        df['CSI300_Equity'] = df['CSI300_Equity'].ffill()
        # Normalize
        valid = df['CSI300_Equity'].dropna()
        if not valid.empty:
            sv = valid.iloc[0]
            df['CSI300_Equity'] = df['CSI300_Equity'] / sv
        
    # Setup Plot
    # Enable Chinese font support
    plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS'] # Try common Chinese fonts
    plt.rcParams['axes.unicode_minus'] = False # Fix minus sign
    
    plt.figure(figsize=(12, 8))
    
    # Color map for distinct lines (主图仅保留E3有效策略+两大基准)
    colors = {
        'E3_IndSelect_Equity': 'blue',    # The Turnaround
        'E3_2_Layered_Equity': 'green',   # The Winner
        'E3_2_5_Tuned_Equity': 'orange',  # The Conservative Option
        'ChiNext_Equity': 'cyan',         # ChiNext
        'CSI300_Equity': 'black'          # CSI 300
    }
    
    styles = {
        'E3_IndSelect_Equity': '-',
        'E3_2_Layered_Equity': '-',
        'E3_2_5_Tuned_Equity': '-',
        'ChiNext_Equity': '-.',           # Dash-dot for Index
        'CSI300_Equity': '-'              # Solid for Main Index
    }
    
    # Chinese Labels
    labels = {
        'E3_IndSelect_Equity': 'E3 (Top5 行业)',
        'E3_2_Layered_Equity': 'E3-2 (Top3 行业+分层)',
        'E3_2_5_Tuned_Equity': 'E3-2-5 (稳健微调)',
        'ChiNext_Equity': '创业板 (ChiNext)',
        'CSI300_Equity': '沪深300 (CSI 300)'
    }
    
    # Line widths
    widths = {
        'CSI300_Equity': 3.0,
        'ChiNext_Equity': 3.0,
        'E3_2_Layered_Equity': 2.5
    }

    # Calculate metrics
    metrics = []
    for col in df.columns:
        if col not in colors:
            continue
            
        equity = df[col]
        # Total Return
        if equity.empty or equity.isna().all():
            continue
            
        total_ret = equity.iloc[-1] - 1.0
        
        # Max Drawdown
        peak = equity.cummax()
        dd = (equity - peak) / peak
        mdd = dd.min()
        
        # Calmar Ratio (Annualized Return / Abs Max Drawdown)
        # Approx 7 years
        ann_ret = (equity.iloc[-1] ** (1/7.0)) - 1.0
        calmar = ann_ret / abs(mdd) if mdd != 0 else 0
        
        # Chinese Metrics
        label_base = labels.get(col, col)
        
        metrics.append({
            "Strategy": label_base,
            "Total Ret": f"{total_ret:.1%}",
            "MDD": f"{mdd:.1%}",
            "Calmar": f"{calmar:.2f}"
        })
        
        # Add metrics to label
        labels[col] = f"{label_base} (总回报: {total_ret:.0%}, 卡玛比: {calmar:.1f})"

    # Plot each column
    plot_order = [c for c in df.columns if c not in ["CSI300_Equity", "ChiNext_Equity"]]
    if "ChiNext_Equity" in df.columns:
        plot_order.append("ChiNext_Equity")
    if "CSI300_Equity" in df.columns:
        plot_order.append("CSI300_Equity")

    for col in plot_order:
        if col not in colors:
            continue
            
        plt.plot(df.index, df[col], 
                 label=labels.get(col, col), 
                 color=colors.get(col, 'black'),
                 linestyle=styles.get(col, '-'),
                 linewidth=widths.get(col, 1.5),
                 zorder=10 if col in ["CSI300_Equity", "ChiNext_Equity"] else 3)
                 
    plt.title('策略全历史表现 vs 市场基准 (2019-2025)', fontsize=16)
    plt.xlabel('日期', fontsize=12)
    plt.ylabel('净值 (起始=1.0)', fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)
    if ENABLE_LOG_SCALE:
        plt.yscale("log")
    plt.legend(fontsize=10, loc='upper left')
    
    # Save
    out_path = os.path.join(root, "research/baseline_v6_1/output/strategy_comparison_2019_2025.png")
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    print(f"Plot saved to: {out_path}")

if __name__ == "__main__":
    main()
