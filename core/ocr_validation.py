import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List, Optional, Sequence, Tuple

import pandas as pd


logger = logging.getLogger(__name__)


DEFAULT_OCR_COLUMNS = {
    "ocr_verified": False,
    "ocr_confidence": 0.0,
    "ocr_price": float("nan"),
    "ocr_reason": "",
}


@dataclass(frozen=True)
class OCRMergeSpec:
    left_on: List[str]
    right_on: List[str]


def _ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for k, v in DEFAULT_OCR_COLUMNS.items():
        if k not in out.columns:
            out[k] = v
    return out


def load_ocr_results(path: str = "data/ocr_results.csv") -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        return df
    except Exception as e:
        logger.warning("Failed to read OCR results from %s: %s", path, e)
        return pd.DataFrame()


def _available_merge_specs(df_signals: pd.DataFrame, df_ocr: pd.DataFrame) -> List[OCRMergeSpec]:
    candidates: List[OCRMergeSpec] = [
        OCRMergeSpec(left_on=["video_id", "author_name", "stock_code"], right_on=["video_id", "author_name", "stock_code"]),
        OCRMergeSpec(left_on=["video_id", "stock_code"], right_on=["video_id", "stock_code"]),
        OCRMergeSpec(left_on=["video_id", "author_name"], right_on=["video_id", "author_name"]),
        OCRMergeSpec(left_on=["video_id"], right_on=["video_id"]),
    ]
    specs = []
    for spec in candidates:
        if all(c in df_signals.columns for c in spec.left_on) and all(c in df_ocr.columns for c in spec.right_on):
            specs.append(spec)
    return specs


def merge_ocr_results(
    df_signals: pd.DataFrame,
    df_ocr: Optional[pd.DataFrame] = None,
    ocr_path: str = "data/ocr_results.csv",
) -> pd.DataFrame:
    if df_signals is None or df_signals.empty:
        return df_signals

    if df_ocr is None:
        df_ocr = load_ocr_results(ocr_path)

    if df_ocr is None or df_ocr.empty:
        return _ensure_columns(df_signals)

    needed = set(DEFAULT_OCR_COLUMNS.keys())
    present = [c for c in df_ocr.columns if c in needed]
    if not present:
        return _ensure_columns(df_signals)

    df_ocr_norm = df_ocr.copy()
    for c in ("video_id", "author_name", "stock_code"):
        if c in df_ocr_norm.columns:
            df_ocr_norm[c] = df_ocr_norm[c].astype(str)

    df_signals_norm = df_signals.copy()
    for c in ("video_id", "author_name", "stock_code"):
        if c in df_signals_norm.columns:
            df_signals_norm[c] = df_signals_norm[c].astype(str)

    specs = _available_merge_specs(df_signals_norm, df_ocr_norm)
    if not specs:
        return _ensure_columns(df_signals_norm)

    spec = specs[0]
    df_ocr_small = df_ocr_norm[spec.right_on + present].drop_duplicates(subset=spec.right_on, keep="last")
    merged = df_signals_norm.merge(
        df_ocr_small,
        how="left",
        left_on=spec.left_on,
        right_on=spec.right_on,
        suffixes=("", "_ocr"),
    )

    for k, default in DEFAULT_OCR_COLUMNS.items():
        if k not in merged.columns:
            merged[k] = default
        else:
            if k == "ocr_verified":
                merged[k] = merged[k].fillna(False).astype(bool)
            elif k == "ocr_confidence":
                merged[k] = pd.to_numeric(merged[k], errors="coerce").fillna(0.0)
            elif k == "ocr_price":
                merged[k] = pd.to_numeric(merged[k], errors="coerce")
            elif k == "ocr_reason":
                merged[k] = merged[k].fillna("").astype(str)

    for c in spec.right_on:
        if c in merged.columns and c not in df_signals.columns:
            merged = merged.drop(columns=[c], errors="ignore")

    return merged


def verify_price_with_baostock(stock_code: str, trade_date: str, price: float) -> Tuple[bool, str]:
    try:
        import baostock as bs
    except Exception as e:
        return False, f"baostock_import_error:{e}"

    try:
        if not stock_code or not trade_date:
            return False, "missing_key"
        p = float(price)
        day = trade_date.split(" ")[0]
        if not day:
            return False, "bad_trade_date"

        if stock_code.startswith("6"):
            bs_code = f"sh.{stock_code}"
        else:
            bs_code = f"sz.{stock_code}"

        lg = bs.login()
        if getattr(lg, "error_code", "0") != "0":
            return False, f"baostock_login_error:{getattr(lg,'error_msg','')}"

        rs = bs.query_history_k_data_plus(
            bs_code,
            fields="date,high,low",
            start_date=day,
            end_date=day,
            frequency="d",
            adjustflag="3",
        )
        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())
        bs.logout()

        if not rows:
            return False, "no_market_data"

        high = float(rows[0][1])
        low = float(rows[0][2])
        if low <= p <= high:
            return True, "verified"
        return False, "price_out_of_range"
    except Exception as e:
        try:
            import baostock as bs
            bs.logout()
        except Exception:
            pass
        return False, f"verify_error:{e}"

