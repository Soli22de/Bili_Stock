import json
import os

def merge_maps():
    base_map = {}
    
    # 1. 加载基础映射 (如果存在)
    if os.path.exists('stock_map.json'):
        try:
            with open('stock_map.json', 'r', encoding='utf-8') as f:
                base_map = json.load(f)
            print(f"加载基础映射: {len(base_map)} 条")
        except Exception as e:
            print(f"加载基础映射失败: {e}")
            
    # 2. 加载补充映射
    if os.path.exists('supplement_stocks.json'):
        try:
            with open('supplement_stocks.json', 'r', encoding='utf-8') as f:
                supp_map = json.load(f)
            print(f"加载补充映射: {len(supp_map)} 条")
            
            # 合并 (补充映射覆盖基础映射，确保手动修正的准确性)
            base_map.update(supp_map)
            
        except Exception as e:
            print(f"加载补充映射失败: {e}")
            
    # 3. 保存
    with open('stock_map_final.json', 'w', encoding='utf-8') as f:
        json.dump(base_map, f, ensure_ascii=False, indent=2)
        
    print(f"最终映射表已保存至 stock_map_final.json，共 {len(base_map)} 条记录")

if __name__ == "__main__":
    merge_maps()