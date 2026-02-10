import pandas as pd
import sys
import os

signals_path = "data/trading_signals.csv"
ocr_path = "data/ocr_results.csv"

if os.path.exists(signals_path) and os.path.exists(ocr_path):
    print("Loading files...")
    df_signals = pd.read_csv(signals_path)
    df_ocr = pd.read_csv(ocr_path)
    
    # Normalize keys
    df_signals['video_id_str'] = df_signals['video_id'].astype(str)
    df_ocr['video_id_str'] = df_ocr['video_id'].astype(str)
    
    # Create a map from video_id -> verified
    verified_videos = set(df_ocr[df_ocr['ocr_verified'] == True]['video_id_str'])
    print(f"Verified videos: {verified_videos}")
    
    # Update signals
    mask = df_signals['video_id_str'].isin(verified_videos)
    df_signals.loc[mask, 'ocr_verified'] = True
    df_signals.loc[mask, 'ocr_confidence'] = 0.95
    df_signals.loc[mask, 'verification_details'] = 'Simulated OCR Verified'
    
    # Drop temp col
    df_signals = df_signals.drop(columns=['video_id_str'])
    
    print(f"Updated {mask.sum()} rows.")
    
    df_signals.to_csv(signals_path, index=False)
    print("Saved merged signals.")
else:
    print("Files not found.")
