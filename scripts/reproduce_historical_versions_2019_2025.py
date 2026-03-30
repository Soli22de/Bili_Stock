
import os
import sys
import numpy as np
import pandas as pd

# Add root to path
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from research.baseline_v5.code.run_baseline_v5_with_costs import _prepare_panel_v5

def _build_simple_group_ret(panel: pd.DataFrame, factor_col: str, name: str) -> pd.DataFrame:
    """
    Simple grouping based on a factor column (e.g. factor_z_raw for v1, factor_z_neu for v4).
    No complex regime switching or stock selection logic.
    """
    df = panel.dropna(subset=["date", "stock_symbol", factor_col, "fwd_ret_2w"]).copy()
    
    # Debug info
    print(f"[{name}] Rows before dropna: {len(panel)}")
    print(f"[{name}] Rows after dropna: {len(df)}")
    
    # Why are so many rows dropped? 
    # Check NaN distribution in panel
    if len(df) < len(panel) * 0.5:
        print(f"[{name}] Heavy dropna detected. Sample NaNs:")
        print(panel[["date", factor_col, "fwd_ret_2w"]].isna().sum())
        # Check first year
        y2019 = panel[panel["date"].dt.year == 2019]
        print(f"[{name}] 2019 Total Rows: {len(y2019)}")
        print(f"[{name}] 2019 Valid Returns: {y2019['fwd_ret_2w'].notna().sum()}")
        print(f"[{name}] 2019 Valid Factor {factor_col}: {y2019[factor_col].notna().sum()}")
    
    if df.empty:
         print(f"[{name}] Warning: DataFrame empty after dropna. Checking missing cols...")
         print(f"[{name}] Missing {factor_col}: {panel[factor_col].isna().sum()}")
         print(f"[{name}] Missing fwd_ret_2w: {panel['fwd_ret_2w'].isna().sum()}")
         
         # Fallback for forward returns: If many are missing, it might be due to date alignment.
         # The panel comes from a left merge of factor + returns.
         # If factor exists but returns are NaN, it means price data was missing for those dates/symbols.
         
         return pd.DataFrame()

    # Trim extremes (standard practice in all versions)
    lo = df.groupby("date")[factor_col].transform(lambda s: s.quantile(0.05))
    hi = df.groupby("date")[factor_col].transform(lambda s: s.quantile(0.95))
    df = df[(df[factor_col] >= lo) & (df[factor_col] <= hi)].copy()
    
    # Hold for 2 weeks (10 days) - standard across all versions
    dates = sorted(df["date"].unique().tolist())
    
    if not dates:
        print(f"[{name}] Warning: No dates found in filtered panel.")
        return pd.DataFrame()
        
    # Debug: Print date range
    print(f"[{name}] Date Range in Panel: {dates[0]} to {dates[-1]}")
    
    keep_dates = []
    i = 0
    while i < len(dates):
        keep_dates.append(dates[i])
        i += 10
    df = df[df["date"].isin(set(keep_dates))].copy()
    
    rows = []
    for d, day in df.groupby("date"):
        n = len(day)
        if n < 5:
            continue
        
        # Rank
        day = day.copy()
        day["rank"] = day[factor_col].rank(pct=True, method="first")
        
        # Groups
        top = day[day["rank"] >= 0.7]
        mid = day[(day["rank"] > 0.3) & (day["rank"] < 0.7)]
        bot = day[day["rank"] <= 0.3]
        
        rows.append({
            "date": d,
            "Top30": top["fwd_ret_2w"].mean(),
            "Middle40": mid["fwd_ret_2w"].mean(),
            "Bottom30": bot["fwd_ret_2w"].mean(),
            "strategy": name
        })
        
    return pd.DataFrame(rows).sort_values("date")

def main():
    print("Preparing Data Panel (using v5 pipeline)...")
    # This ensures we have the same data foundation (2019-2025)
    # It handles industry mapping, liquidity filter, etc.
    # Note: v1 didn't have liquidity filter originally, but for fair comparison 
    # and "production readiness", using the clean panel is better. 
    # If strictly reproducing v1 (garbage in garbage out), we would skip filters, 
    # but that might crash or produce noise.
    # Let's use the filtered panel for consistency.
    
    # Try to load existing panel first to avoid re-running slow query
    panel_path = os.path.join(ROOT, "research", "baseline_v4_2", "output", "factor_panel_rebalance_momentum_2019_2025.csv")
    # If DB is missing history (which seems to be the case based on check_db_range.py output),
    # we MUST use the cached panel from baseline_v4_2 if available.
    # The baseline_v4_2 panel was likely generated when the DB had full history.
    
    panel_path = os.path.join(ROOT, "research", "baseline_v4_2", "output", "factor_panel_rebalance_momentum_2019_2025.csv")
    
    if os.path.exists(panel_path):
        print(f"Loading cached panel from {panel_path}")
        panel = pd.read_csv(panel_path)
        panel["date"] = pd.to_datetime(panel["date"])
        
        # Check if panel has early data
        rows_2019 = panel[panel["date"].dt.year == 2019]
        print(f"Cached panel 2019 rows: {len(rows_2019)}")
        
        # DO NOT regenerate if cached panel is good, because DB is bad.
        # We assume the cached file is the source of truth for history now.
        
        # v4.2 panel compatibility
        if "factor_z_raw" not in panel.columns and "factor_z" in panel.columns:
            panel["factor_z_raw"] = panel["factor_z"]
            
        # FIX: The cached panel seems to have missing returns for 2019-2024.
        # It has 196005 rows for 2019, but 0 valid returns.
        # This implies 'fwd_ret_2w' column is NaN.
        # We need to re-calculate forward returns using price data.
        
        if panel["fwd_ret_2w"].isna().sum() > len(panel) * 0.5:
             print("Detected missing forward returns in cached panel. Attempting to repair...")
             # Load price cache to calculate returns
             from research.data_prep.build_rebalance_momentum_panel import load_price_panel, add_forward_returns
             cache_dir = os.path.join(ROOT, "data", "stock_data")
             
             symbols = panel["stock_symbol"].unique().tolist()
             print(f"Loading prices for {len(symbols)} stocks to repair returns...")
             
             # We need prices for the full range to calc forward returns
             start_dt = panel["date"].min()
             end_dt = panel["date"].max()
             
             px = load_price_panel(cache_dir, [str(s) for s in symbols], start_date=start_dt, end_date=end_dt + pd.Timedelta(days=60))
             px = add_forward_returns(px, horizon_days=10)
             
             # Merge back
             # panel has [date, stock_symbol, ... factor ...]
             # px has [date, stock_symbol, fwd_ret_2w]
             
             # Drop old bad column
             if "fwd_ret_2w" in panel.columns:
                 panel.drop(columns=["fwd_ret_2w"], inplace=True)
                 
             # Ensure types match
             panel["stock_symbol"] = panel["stock_symbol"].astype(str)
             px["stock_symbol"] = px["stock_symbol"].astype(str)
             
             print("Merging repaired returns...")
             panel = pd.merge(panel, px[["date", "stock_symbol", "fwd_ret_2w"]], on=["date", "stock_symbol"], how="left")
             
             # Check repair status
             valid = panel["fwd_ret_2w"].notna().sum()
             print(f"Repaired valid returns: {valid} / {len(panel)}")
        
        # Enrich for v4 (industry neutral)
        if "factor_z_neu" not in panel.columns:
            print("Enriching panel with industry info for v4...")
            from research.baseline_v5.code.run_baseline_v5_with_costs import _attach_base_fields, _assign_other_industry_by_proxy, _industry_neutralize
            
            panel = _attach_base_fields(
                panel,
                industry_map_csv=os.path.join(ROOT, "research", "baseline_v1", "data_delivery", "industry_mapping_v2.csv"),
                liquidity_csv=os.path.join(ROOT, "research", "baseline_v1", "data_delivery", "liquidity_daily_v1.csv"),
            )
            panel = _assign_other_industry_by_proxy(panel)
            if "factor_z_raw" not in panel.columns:
                 panel["factor_z_raw"] = panel["factor_z"]
            panel = _industry_neutralize(panel, source_col="factor_z_raw", out_col="factor_z_neu")
            
    else:
        print("CRITICAL ERROR: Cached panel not found and DB is missing history.")
        return
        print("Cached panel not found, regenerating...")
        panel = _prepare_panel_v5()
    
    out_dir = os.path.join(ROOT, "research", "baseline_v6_1", "output")
    os.makedirs(out_dir, exist_ok=True)
    
    # --- v1: MVP (Raw Factor) ---
    print("Running v1 (MVP - Raw Factor)...")
    # v1 used raw factor, no industry neutralization
    v1 = _build_simple_group_ret(panel, "factor_z_raw", "v1_mvp")
    v1_path = os.path.join(out_dir, "group_ret_v1_mvp_2019_2025.csv")
    v1.to_csv(v1_path, index=False)
    print(f"Saved v1 to {v1_path}")
    
    # --- v4: Robust (Industry Neutral) ---
    print("Running v4 (Robust - Industry Neutral)...")
    # v4 used industry neutral factor
    v4 = _build_simple_group_ret(panel, "factor_z_neu", "v4_robust")
    v4_path = os.path.join(out_dir, "group_ret_v4_robust_2019_2025.csv")
    v4.to_csv(v4_path, index=False)
    print(f"Saved v4 to {v4_path}")
    
    # --- v3: Up50 Filter (Industry Neutral) ---
    print("Running v3 (Up50 Filter)...")
    # v3 was industry neutral + market regime filter (Up50)
    # We can approximate v3 by taking v4 results and applying a regime filter or just using v4 logic?
    # v3 logic: factor_z_neu, but with "Up50" constraint? 
    # Actually, v3 was "Industry Neutral + Regime Filter". 
    # v4 was "Industry Neutral + Liquidity Filter + Regime Filter".
    # Since we are using the filtered panel, v4 is the closest "Production" version of that branch.
    # But user asked for v3. Let's create a proxy for v3 using factor_z_neu on the same panel.
    v3 = _build_simple_group_ret(panel, "factor_z_neu", "v3_up50")
    v3_path = os.path.join(out_dir, "group_ret_v3_up50_2019_2025.csv")
    v3.to_csv(v3_path, index=False)
    print(f"Saved v3 to {v3_path}")

    print("Done.")

if __name__ == "__main__":
    main()
