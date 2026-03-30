
import pandas as pd
import os
import sys

def main():
    root = os.getcwd()
    
    # Define file paths
    files = {
        "v1_MVP": os.path.join(root, "research/baseline_v6_1/output/group_ret_v1_mvp_2019_2025.csv"),
        "v3_Up50": os.path.join(root, "research/baseline_v6_1/output/group_ret_v3_up50_2019_2025.csv"),
        "v4_Robust": os.path.join(root, "research/baseline_v6_1/output/group_ret_v4_robust_2019_2025.csv"),
        "v5_StrongStock": os.path.join(root, "research/baseline_v5/analysis/baseline_v5_rebalance_returns.csv"),
        "E3_IndSelect": os.path.join(root, "research/baseline_v6_1/output/E3_scheme3_top5_industry_group_ret.csv"),
        "E3_2_Layered": os.path.join(root, "research/baseline_v6_1/output/E3_2_group_ret.csv"),
        "E3_2_5_Tuned": os.path.join(root, "research/baseline_v6_1/output/E3_2_5_group_ret.csv")
    }
    
    dfs = []
    
    for name, path in files.items():
        if not os.path.exists(path):
            print(f"Warning: File not found: {path}")
            continue
            
        df = pd.read_csv(path)
        df['date'] = pd.to_datetime(df['date'])
        
        # Extract return
        if name in ["v1_MVP", "v4_Robust", "v3_Up50"]:
             # Standard cost assumption (0.25%)
             df["spread_ret"] = df["Top30"] - df["Bottom30"] - 0.0025
             ret_col = "spread_ret"
        elif name == "v5_StrongStock":
             # Special handling for Baseline v5 which lacks net return in analysis file
             # Assume 0.25% cost per period
             df["spread_ret"] = df["Top30"] - df["Bottom30"] - 0.0025
             ret_col = "spread_ret"
        elif "spread_ret" in df.columns:
            ret_col = "spread_ret"
        elif "Top30_net" in df.columns and "Bottom30" in df.columns:
            df["spread_ret"] = df["Top30_net"] - df["Bottom30"]
            ret_col = "spread_ret"
        else:
            print(f"Warning: Could not find return columns in {name}")
            continue
            
        # Keep only date and return
        sub = df[['date', ret_col]].copy()
        sub.rename(columns={ret_col: f"{name}_ret"}, inplace=True)
        sub.set_index('date', inplace=True)
        dfs.append(sub)
        
    if not dfs:
        print("No data loaded.")
        return

    # Merge all
    combined = pd.concat(dfs, axis=1, join='outer').sort_index()
    combined.fillna(0.0, inplace=True)
    
    # Calculate Equity Curves (starting at 1.0)
    equity = (1 + combined).cumprod()
    
    # Check 2025 data
    max_date = equity.index.max()
    min_date = equity.index.min()
    print(f"Data Range: {min_date.date()} to {max_date.date()}")
    
    has_2025 = max_date.year == 2025
    print(f"Includes 2025 data: {has_2025}")
    if has_2025:
        data_2025 = equity[equity.index.year == 2025]
        print(f"2025 Data Points: {len(data_2025)}")
        print(f"Last Date: {data_2025.index[-1]}")
    
    # Save output
    out_path = os.path.join(root, "research/baseline_v6_1/output/strategy_comparison_equity_curves_2019_2025.csv")
    equity.columns = [c.replace("_ret", "_Equity") for c in equity.columns]
    equity.to_csv(out_path)
    print(f"Saved equity curves to {out_path}")
    
    # Also save the returns for analysis
    out_ret_path = os.path.join(root, "research/baseline_v6_1/output/strategy_comparison_returns_2019_2025.csv")
    combined.to_csv(out_ret_path)
    print(f"Saved returns to {out_ret_path}")
    
    # Print final cumulative returns
    print("\nFinal Cumulative Returns (Total Return):")
    print(equity.iloc[-1] - 1.0)

if __name__ == "__main__":
    main()
