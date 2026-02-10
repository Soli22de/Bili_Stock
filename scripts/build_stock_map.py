import akshare as ak
import pandas as pd
import json
import os

import requests
import time

def fetch_stock_list():
    print("尝试通过东方财富API直接获取全量A股数据 (循环分页)...")
    url = "http://82.push2.eastmoney.com/api/qt/clist/get"
    
    all_stocks = []
    page = 1
    
    while True:
        print(f"正在获取第 {page} 页...")
        params = {
            "pn": page,
            "pz": 100,  # 既然只能获取100条，那就按100条翻页
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048", 
            "fields": "f12,f14",
            "_": int(time.time() * 1000)
        }
        
        retry_count = 0
        success = False
        while retry_count < 3:
            try:
                resp = requests.get(url, params=params, timeout=10)
                data = resp.json()
                success = True
                break
            except Exception as e:
                print(f"第 {page} 页获取失败 (重试 {retry_count+1}/3): {e}")
                retry_count += 1
                time.sleep(2)
        
        if not success:
            print("重试多次失败，停止获取。")
            break

        if data and 'data' in data and 'diff' in data['data']:
            batch = data['data']['diff']
            if not batch:
                print("本页无数据，结束获取。")
                break
                
            all_stocks.extend(batch)
            
            # 如果获取的数量少于页大小，说明是最后一页
            if len(batch) < 100:
                print("到达最后一页。")
                break
                
            page += 1
            time.sleep(1.0) # 稍微礼貌一点，增加延时
        else:
            print("API返回数据格式异常或结束:", data)
            break
            
    if all_stocks:
        df = pd.DataFrame(all_stocks)
        df = df.rename(columns={'f12': '代码', 'f14': '名称'})
        print(f"总共获取到 {len(df)} 条股票数据")
        return df
    else:
        return None

def build_mapping(df):
    if df is None or df.empty:
        return
    
    stock_map = {}
    code_name_list = []
    
    for _, row in df.iterrows():
        code = str(row['代码'])
        name = str(row['名称'])
        
        # 基础映射
        stock_map[name] = code
        
        # 保存列表
        code_name_list.append({'code': code, 'name': name})
        
    # 保存为 JSON
    with open('stock_map.json', 'w', encoding='utf-8') as f:
        json.dump(stock_map, f, ensure_ascii=False, indent=2)
        
    # 保存为 CSV
    pd.DataFrame(code_name_list).to_csv('stock_list.csv', index=False, encoding='utf-8-sig')
    
    print(f"映射表已构建完成: stock_map.json (包含 {len(stock_map)} 个条目)")
    print(f"股票列表已保存: stock_list.csv")


if __name__ == "__main__":
    df = fetch_stock_list()
    build_mapping(df)
