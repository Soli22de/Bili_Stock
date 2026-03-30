import json
import os

def merge_maps():
    base_map = {}
    base_path = os.path.join("data", "stock_map.json")
    supp_path = os.path.join("data", "supplement_stocks.json")
    final_path = os.path.join("data", "stock_map_final.json")
    if os.path.exists(base_path):
        try:
            with open(base_path, 'r', encoding='utf-8') as f:
                base_map = json.load(f)
            print(f"加载基础映射: {len(base_map)} 条")
        except Exception as e:
            print(f"加载基础映射失败: {e}")
    if os.path.exists(supp_path):
        try:
            with open(supp_path, 'r', encoding='utf-8') as f:
                supp_map = json.load(f)
            print(f"加载补充映射: {len(supp_map)} 条")
            base_map.update(supp_map)
        except Exception as e:
            print(f"加载补充映射失败: {e}")
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    with open(final_path, 'w', encoding='utf-8') as f:
        json.dump(base_map, f, ensure_ascii=False, indent=2)
    print(f"最终映射表已保存至 {final_path}，共 {len(base_map)} 条记录")

if __name__ == "__main__":
    merge_maps()
