import json
import os
import re

import akshare as ak
import pandas as pd
import requests
from pypinyin import Style, pinyin


_orig_session_request = requests.Session.request


def _new_session_request(self, method, url, *args, **kwargs):
    kwargs["proxies"] = {"http": None, "https": None}
    return _orig_session_request(self, method, url, *args, **kwargs)


requests.Session.request = _new_session_request
os.environ["http_proxy"] = ""
os.environ["https_proxy"] = ""
os.environ["HTTP_PROXY"] = ""
os.environ["HTTPS_PROXY"] = ""

STOCK_MAP_PATH = "data/stock_map_final.json"
STOCK_LIST_CSV = "data/stock_list.csv"


def _normalize_a_code(code: str) -> str:
    c = str(code).strip().upper().replace(".SH", "").replace(".SZ", "").replace(".BJ", "")
    c = re.sub(r"[^0-9]", "", c).zfill(6)
    if c.startswith(("6", "5", "9")):
        return f"{c}.SH"
    if c.startswith(("0", "2", "3")):
        return f"{c}.SZ"
    if c.startswith(("4", "8")):
        return f"{c}.BJ"
    return f"{c}.SZ"


def _normalize_hk_code(code: str) -> str:
    c = str(code).strip().upper().replace(".HK", "").replace("HK", "")
    c = re.sub(r"[^0-9]", "", c).zfill(5)
    return f"{c}.HK"


def _infer_a_market(code: str) -> str:
    c = str(code)
    if c.startswith(("6", "5", "9")):
        return "SH"
    if c.startswith(("0", "2", "3")):
        return "SZ"
    if c.startswith(("4", "8")):
        return "BJ"
    return "SZ"


def get_a_share_list() -> pd.DataFrame:
    try:
        df = ak.stock_zh_a_spot_em()
        df = df[["代码", "名称"]].rename(columns={"代码": "code", "名称": "name"})
    except Exception:
        try:
            df = ak.stock_info_a_code_name()
            df = df.rename(columns={"code": "code", "name": "name"})
        except Exception:
            return pd.DataFrame(columns=["code", "name", "market", "std_code"])
    df["code"] = df["code"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df = df[df["code"].str.match(r"^\d{6}$", na=False)].copy()
    df["market"] = df["code"].map(_infer_a_market)
    df["std_code"] = df["code"].map(_normalize_a_code)
    return df[["code", "name", "market", "std_code"]].drop_duplicates()


def get_hk_share_list() -> pd.DataFrame:
    try:
        df = ak.stock_hk_spot_em()
        df = df[["代码", "名称"]].rename(columns={"代码": "code", "名称": "name"})
    except Exception:
        try:
            df = ak.stock_hk_spot()
            df = df[["代码", "中文名称"]].rename(columns={"代码": "code", "中文名称": "name"})
        except Exception:
            return pd.DataFrame(columns=["code", "name", "market", "std_code"])
    df["code"] = df["code"].astype(str).str.strip()
    df["name"] = df["name"].astype(str).str.strip()
    df = df[df["code"].str.match(r"^\d{4,5}$", na=False)].copy()
    df["std_code"] = df["code"].map(_normalize_hk_code)
    df["market"] = "HK"
    return df[["code", "name", "market", "std_code"]].drop_duplicates()


def generate_pinyin_abbr(name: str) -> str:
    try:
        pinyin_list = pinyin(name, style=Style.FIRST_LETTER)
        abbr = "".join([item[0].upper() for item in pinyin_list])
        return "".join(filter(str.isalnum, abbr))
    except Exception:
        return ""


def build_map():
    a_df = get_a_share_list()
    hk_df = get_hk_share_list()
    all_df = pd.concat([a_df, hk_df], ignore_index=True)
    stock_map = {}
    if all_df.empty:
        if os.path.exists(STOCK_MAP_PATH):
            with open(STOCK_MAP_PATH, "r", encoding="utf-8") as f:
                stock_map = json.load(f)
        else:
            return
    else:
        for _, row in all_df.iterrows():
            code = str(row["code"]).strip()
            std_code = str(row["std_code"]).strip().upper()
            market = str(row["market"]).strip().upper()
            name = str(row["name"]).strip()
            if name:
                stock_map[name] = std_code
            stock_map[std_code] = std_code
            if market == "HK":
                hk = std_code.replace(".HK", "")
                stock_map[hk] = std_code
                stock_map[f"HK{hk}"] = std_code
            else:
                raw = std_code.split(".")[0]
                stock_map[raw] = std_code
                stock_map[f"{market}{raw}"] = std_code
        current_items = list(stock_map.items())
        for name, code in current_items:
            if all(ord(c) < 128 for c in str(name)):
                continue
            abbr = generate_pinyin_abbr(str(name))
            if abbr and len(abbr) >= 3 and abbr not in stock_map:
                stock_map[abbr] = code
    os.makedirs(os.path.dirname(STOCK_MAP_PATH), exist_ok=True)
    with open(STOCK_MAP_PATH, "w", encoding="utf-8") as f:
        json.dump(stock_map, f, ensure_ascii=False, indent=2)
    if not all_df.empty:
        all_df = all_df.sort_values(["market", "code"]).reset_index(drop=True)
        all_df.to_csv(STOCK_LIST_CSV, index=False, encoding="utf-8-sig")
    print(f"Saved mapping entries: {len(stock_map)}")
    if not all_df.empty:
        print(all_df.groupby("market")["std_code"].nunique().to_string())


if __name__ == "__main__":
    build_map()
