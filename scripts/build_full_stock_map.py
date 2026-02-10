import akshare as ak
import json
import os
import sys
from pypinyin import pinyin, Style
import requests

# 强力禁用代理：Monkeypatch requests
# 某些环境（如 Windows）可能会强制从注册表读取代理，设置 ENV 为空可能不够
_orig_session_request = requests.Session.request

def _new_session_request(self, method, url, *args, **kwargs):
    # 强制不使用代理
    kwargs['proxies'] = {"http": None, "https": None}
    return _orig_session_request(self, method, url, *args, **kwargs)

requests.Session.request = _new_session_request

# 再次确保 ENV 为空
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''

# 配置
STOCK_MAP_PATH = "data/stock_map_final.json"
STOCK_LIST_CSV = "data/stock_list.csv"

def get_full_stock_list():
    """
    获取 A 股全市场股票列表 (代码 + 名称)
    """
    print("Fetching A-share stock list from AkShare (ak.stock_info_a_code_name())...")
    try:
        # 尝试使用备用接口，如果 szse 失败
        # ak.stock_info_a_code_name() 内部可能调用了交易所接口，容易超时或 SSL 错误
        # 尝试 ak.stock_zh_a_spot_em() (东方财富接口，通常更稳定)
        print("Trying ak.stock_zh_a_spot_em() (EastMoney source)...")
        df = ak.stock_zh_a_spot_em()
        # 只需要 code 和 name 列
        df = df[['代码', '名称']].rename(columns={'代码': 'code', '名称': 'name'})
        print(f"Fetched {len(df)} stocks from EastMoney.")
        return df
    except Exception as e:
        print(f"EastMoney source failed: {e}")
        try:
             # Fallback to ak.stock_info_a_code_name()
             print("Fallback to ak.stock_info_a_code_name()...")
             df = ak.stock_info_a_code_name()
             print(f"Fetched {len(df)} stocks.")
             return df
        except Exception as e2:
             print(f"All sources failed: {e2}")
             return None

def generate_pinyin_abbr(name):
    """
    生成股票名称的拼音首字母缩写
    例如: "中际旭创" -> "ZJXC"
    """
    try:
        # pypinyin returns a list of lists, e.g., [['zhong'], ['ji'], ['xu'], ['chuang']]
        pinyin_list = pinyin(name, style=Style.FIRST_LETTER)
        abbr = ''.join([item[0].upper() for item in pinyin_list])
        # remove non-alphanumeric characters just in case
        abbr = ''.join(filter(str.isalnum, abbr))
        return abbr
    except Exception as e:
        # Fallback or ignore
        return ""

def build_map():
    df = get_full_stock_list()
    stock_map = {}
    
    if df is None or df.empty:
        print("Network fetch failed. Trying to load existing stock map to append Pinyin...")
        if os.path.exists(STOCK_MAP_PATH):
            with open(STOCK_MAP_PATH, 'r', encoding='utf-8') as f:
                stock_map = json.load(f)
            print(f"Loaded {len(stock_map)} existing entries.")
        else:
            print("No existing stock map found. Cannot proceed.")
            return
    else:
        # Build from fresh data
        for index, row in df.iterrows():
            code = str(row['code'])
            name = str(row['name'])
            stock_map[name] = code

    # Add Pinyin abbreviations
    print("Generating Pinyin abbreviations...")
    # Create a copy of items to iterate safely
    current_items = list(stock_map.items())
    
    pinyin_count = 0
    for name, code in current_items:
        # Skip if key is already an abbreviation (simple heuristic: all ascii and short)
        if all(ord(c) < 128 for c in name):
            continue
            
        abbr = generate_pinyin_abbr(name)
        if abbr and len(abbr) >= 3 and abbr not in stock_map:
            stock_map[abbr] = code
            pinyin_count += 1
            
    print(f"Added {pinyin_count} Pinyin abbreviations.")

    # 保存
    # 确保目录存在
    os.makedirs(os.path.dirname(STOCK_MAP_PATH), exist_ok=True)
    
    with open(STOCK_MAP_PATH, 'w', encoding='utf-8') as f:
        json.dump(stock_map, f, ensure_ascii=False, indent=4)
    
    print(f"Saved {len(stock_map)} entries to {STOCK_MAP_PATH}")
    
    # Only save CSV if we fetched new data
    if df is not None and not df.empty:
        df.to_csv(STOCK_LIST_CSV, index=False, encoding='utf-8-sig')
        print(f"Saved stock list CSV to {STOCK_LIST_CSV}")

if __name__ == "__main__":
    # Check if pypinyin is installed
    try:
        import pypinyin
    except ImportError:
        print("pypinyin not found. Installing...")
        os.system("pip install pypinyin")
        import pypinyin
        
    build_map()
