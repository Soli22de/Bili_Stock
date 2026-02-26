
import json
import os

def merge_lists():
    files = ["data/massive_cube_list.json", "data/active_target_cubes.json"]
    all_cubes = {}
    
    for fpath in files:
        if os.path.exists(fpath):
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    print(f"Loaded {len(data)} from {fpath}")
                    for item in data:
                        symbol = item.get("symbol")
                        if symbol:
                            # Prefer the one with more info if conflict, but for now just overwrite
                            if symbol not in all_cubes:
                                all_cubes[symbol] = item
                            else:
                                # Merge fields?
                                all_cubes[symbol].update(item)
            except Exception as e:
                print(f"Error reading {fpath}: {e}")
                
    result = list(all_cubes.values())
    print(f"Total unique cubes: {len(result)}")
    
    with open("data/massive_cube_list.json", "w", encoding="utf-8") as f:
        json.dump(result, f, indent=4, ensure_ascii=False)
        
if __name__ == "__main__":
    merge_lists()
