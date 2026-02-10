import akshare as ak
import json
import os
import sys

# 添加项目根目录到 sys.path 以便导入 config (如果需要)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import config
    STOCK_MAP_PATH = config.STOCK_MAP_PATH
except ImportError:
    STOCK_MAP_PATH = "data/stock_map_final.json"

def update_stock_map():
    print("Fetching A-share stock list from AKShare...")
    try:
        # 获取 A 股列表
        df = ak.stock_zh_a_spot_em()
        # df columns: 序号, 代码, 名称, ...
        
        stock_map = {}
        for _, row in df.iterrows():
            code = str(row['代码'])
            name = str(row['名称'])
            stock_map[name] = code
            
        print(f"Fetched {len(stock_map)} stocks.")
        
        # 保存
        with open(STOCK_MAP_PATH, 'w', encoding='utf-8') as f:
            json.dump(stock_map, f, ensure_ascii=False, indent=4)
            
        print(f"Saved to {STOCK_MAP_PATH}")
        
    except Exception as e:
        print(f"Error fetching stock list: {e}")
        # 尝试备用接口
        try:
            print("Trying alternative source (Sina)...")
            df = ak.stock_zh_a_spot()
            stock_map = {}
            for _, row in df.iterrows():
                code = str(row['code'])
                name = str(row['name'])
                # sina code might have prefix like sh600000
                if code.startswith('sh') or code.startswith('sz'):
                    code = code[2:]
                stock_map[name] = code
            
            print(f"Fetched {len(stock_map)} stocks from Sina.")
            with open(STOCK_MAP_PATH, 'w', encoding='utf-8') as f:
                json.dump(stock_map, f, ensure_ascii=False, indent=4)
                
        except Exception as e2:
            print(f"Alternative source failed too: {e2}")

if __name__ == "__main__":
    update_stock_map()
