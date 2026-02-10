import json
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        v = float(x)
        if np.isnan(v):
            return default
        return v
    except Exception:
        return default


def _zscore(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").astype(float)
    mu = float(np.nanmean(s.values))
    sigma = float(np.nanstd(s.values))
    if sigma <= 1e-12:
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - mu) / sigma


def build_blogger_stats(df: pd.DataFrame) -> pd.DataFrame:
    df0 = df.copy()
    if "date" in df0.columns:
        df0["day"] = df0["date"].astype(str).str.split(" ").str[0]
    else:
        df0["day"] = ""

    if "ocr_price" in df0.columns:
        df0["ocr_price"] = pd.to_numeric(df0["ocr_price"], errors="coerce")
    else:
        df0["ocr_price"] = np.nan

    if "ocr_verified" in df0.columns:
        df0["ocr_verified"] = df0["ocr_verified"].fillna(False).astype(bool)
    else:
        df0["ocr_verified"] = False

    df0["ocr_fake"] = df0["ocr_price"].notna() & (~df0["ocr_verified"])

    if "credibility_score" in df0.columns:
        df0["credibility_score"] = pd.to_numeric(df0["credibility_score"], errors="coerce").fillna(0.0)
    else:
        df0["credibility_score"] = 0.0

    grouped = df0.groupby("author_name", dropna=False)

    stats = grouped.agg(
        total_signals=("author_name", "size"),
        unique_days=("day", pd.Series.nunique),
        avg_credibility=("credibility_score", "mean"),
        buy_signals=("action", lambda s: int((s == "BUY").sum()) if s is not None else 0),
        ocr_verified_count=("ocr_verified", "sum"),
        ocr_fake_count=("ocr_fake", "sum"),
    ).reset_index()

    stats["unique_days"] = stats["unique_days"].replace(0, 1)
    stats["signals_per_day"] = stats["total_signals"] / stats["unique_days"]
    stats["verified_rate"] = stats["ocr_verified_count"] / stats["total_signals"].replace(0, 1)

    return stats


def classify_bloggers(stats: pd.DataFrame) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    df = stats.copy()
    df["signals_per_day_z"] = _zscore(df["signals_per_day"])

    blacklist: List[Dict[str, Any]] = []
    tier1: List[Dict[str, Any]] = []
    tier2: List[Dict[str, Any]] = []
    tier3: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        author = str(row.get("author_name", "Unknown"))
        avg_cred = _safe_float(row.get("avg_credibility"), 0.0)
        verified_cnt = int(_safe_float(row.get("ocr_verified_count"), 0.0))
        fake_cnt = int(_safe_float(row.get("ocr_fake_count"), 0.0))
        spd = _safe_float(row.get("signals_per_day"), 0.0)
        spd_z = _safe_float(row.get("signals_per_day_z"), 0.0)

        if fake_cnt > 0:
            blacklist.append(
                {
                    "author_name": author,
                    "reason": "ocr_fake",
                    "signals_per_day": round(spd, 4),
                    "avg_credibility": round(avg_cred, 2),
                    "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            continue

        if spd > 10 or abs(spd_z) > 3:
            blacklist.append(
                {
                    "author_name": author,
                    "reason": "signal_frequency_outlier",
                    "signals_per_day": round(spd, 4),
                    "z_score": round(spd_z, 4),
                    "avg_credibility": round(avg_cred, 2),
                    "detected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            continue

        ocr_bonus = 100.0 if verified_cnt > 0 else 50.0
        total_score = 0.4 * avg_cred + 0.6 * ocr_bonus

        if total_score > 80 and verified_cnt > 0:
            tier1.append(
                {
                    "author_name": author,
                    "total_score": round(total_score, 2),
                    "avg_credibility": round(avg_cred, 2),
                    "ocr_verified": True,
                    "weight_multiplier": 1.5,
                }
            )
        elif total_score > 60:
            tier2.append(
                {
                    "author_name": author,
                    "total_score": round(total_score, 2),
                    "avg_credibility": round(avg_cred, 2),
                    "ocr_verified": verified_cnt > 0,
                    "weight_multiplier": 1.0,
                }
            )
        else:
            tier3.append(
                {
                    "author_name": author,
                    "total_score": round(total_score, 2),
                    "avg_credibility": round(avg_cred, 2),
                    "ocr_verified": verified_cnt > 0,
                    "weight_multiplier": 0.85,
                }
            )

    tier1.sort(key=lambda x: x["total_score"], reverse=True)
    tier2.sort(key=lambda x: x["total_score"], reverse=True)
    tier3.sort(key=lambda x: x["total_score"], reverse=True)

    return tier1, tier2, tier3, blacklist


def save_outputs(tier1: List[Dict[str, Any]], tier2: List[Dict[str, Any]], tier3: List[Dict[str, Any]], blacklist: List[Dict[str, Any]]) -> None:
    os.makedirs("data", exist_ok=True)
    tier_payload = {
        "tier1": tier1,
        "tier2": tier2,
        "tier3": tier3,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with open("data/blogger_tier_list.json", "w", encoding="utf-8") as f:
        json.dump(tier_payload, f, ensure_ascii=False, indent=2)
    with open("data/blacklist.json", "w", encoding="utf-8") as f:
        json.dump({"blacklist": blacklist, "generated_at": tier_payload["generated_at"]}, f, ensure_ascii=False, indent=2)


def main() -> int:
    import config

    if not os.path.exists(config.SIGNALS_CSV):
        print(f"signals file not found: {config.SIGNALS_CSV}")
        return 2

    df = pd.read_csv(config.SIGNALS_CSV)
    if df.empty:
        print("signals file is empty")
        return 0

    stats = build_blogger_stats(df)
    tier1, tier2, tier3, blacklist = classify_bloggers(stats)
    save_outputs(tier1, tier2, tier3, blacklist)

    print(f"tier1={len(tier1)} tier2={len(tier2)} tier3={len(tier3)} blacklist={len(blacklist)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
