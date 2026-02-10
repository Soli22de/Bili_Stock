#!/usr/bin/env python3
"""
测试BaoStock API接口
"""
import baostock as bs
import pandas as pd
from datetime import datetime

def test_baostock_connection():
    """测试BaoStock连接和基本数据获取"""
    print("=== 测试BaoStock连接 ===")
    
    # 登录
    lg = bs.login()
    if lg.error_code != "0":
        print(f"登录失败: {lg.error_msg}")
        return False
    print("登录成功!")
    
    # 测试不同频率的数据获取
    test_codes = ["600036", "000001"]  # 招商银行, 平安银行
    frequencies = ["1", "5", "15", "30", "60"]  # 1分钟, 5分钟, 15分钟, 30分钟, 60分钟
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    for code in test_codes:
        print(f"\n=== 测试股票 {code} ===")
        
        # 转换为BaoStock代码格式
        bs_code = f"sh.{code}" if code.startswith("6") else f"sz.{code}"
        print(f"BaoStock代码: {bs_code}")
        
        for freq in frequencies:
            print(f"\n尝试获取 {freq} 分钟数据...")
            try:
                rs = bs.query_history_k_data_plus(
                    bs_code,
                    "date,time,open,high,low,close,volume,amount",
                    start_date=today,
                    end_date=today,
                    frequency=freq,
                    adjustflag="3"
                )
                
                if rs.error_code != "0":
                    print(f"  {freq}分钟数据获取失败: {rs.error_msg}")
                else:
                    data_list = []
                    while rs.next():
                        data_list.append(rs.get_row_data())
                    
                    if data_list:
                        df = pd.DataFrame(data_list, columns=rs.fields)
                        print(f"  {freq}分钟数据获取成功: {len(df)} 条记录")
                        if not df.empty:
                            print(f"    最新数据时间: {df.iloc[-1]['time'] if 'time' in df.columns else 'N/A'}")
                    else:
                        print(f"  {freq}分钟数据: 无数据")
                        
            except Exception as e:
                print(f"  {freq}分钟数据异常: {e}")
    
    # 登出
    bs.logout()
    print("\n=== 测试完成 ===")
    return True

if __name__ == "__main__":
    test_baostock_connection()