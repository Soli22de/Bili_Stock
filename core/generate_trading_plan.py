import pandas as pd
from datetime import datetime
import sys
import os

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config

def get_plan_text(csv_path=config.SIGNALS_CSV, target_date=None):
    try:
        # 指定 stock_code 为字符串，防止丢失前导零
        df = pd.read_csv(csv_path, dtype={'stock_code': str})
    except FileNotFoundError:
        print("未找到信号文件。")
        return

    # 转换日期格式
    df['date_dt'] = pd.to_datetime(df['date'], errors='coerce')
    
    if target_date is None:
        # 默认取最新日期
        target_date_str = df['date_dt'].max().strftime('%Y-%m-%d')
    else:
        target_date_str = target_date

    lines = []
    lines.append(f"### 今日策略 {target_date_str}")
    
    # 筛选当日信号
    # 假设日期列格式为 "YYYY-MM-DD HH:MM:SS"
    daily_signals = df[df['date'].astype(str).str.startswith(target_date_str)]
    
    if daily_signals.empty:
        return f"日期 {target_date_str} 无信号。"

    if 'strength' in daily_signals.columns:
        daily_signals = daily_signals.sort_values(by=['strength', 'date'], ascending=[False, False])

    buy_signals = daily_signals[daily_signals['action'] == 'BUY']
    sell_signals = daily_signals[daily_signals['action'] == 'SELL']
    neutral_signals = daily_signals[daily_signals['action'] == 'NEUTRAL']

    if 'strength' in daily_signals.columns:
        buy_signals = buy_signals[buy_signals['strength'] >= 0.5]
        sell_signals = sell_signals[sell_signals['strength'] >= 0.5]
        neutral_signals = neutral_signals[neutral_signals['strength'] >= 0.35]

    lines.append(f"\n#### 买入信号 ({len(buy_signals)})")
    if not buy_signals.empty:
        for _, row in buy_signals.iterrows():
            price_info = f" @ {row['price']}" if row['price'] > 0 else ""
            score_info = f" | 评分 {row['strength']:.2f}" if 'strength' in row else ""
            lines.append(f"- {row['stock_name']}({row['stock_code']}){price_info}{score_info}")

    lines.append(f"\n#### 卖出信号 ({len(sell_signals)})")
    if not sell_signals.empty:
        for _, row in sell_signals.iterrows():
            price_info = f" @ {row['price']}" if row['price'] > 0 else ""
            score_info = f" | 评分 {row['strength']:.2f}" if 'strength' in row else ""
            lines.append(f"- {row['stock_name']}({row['stock_code']}){price_info}{score_info}")
            
    lines.append(f"\n#### 关注/中性 ({len(neutral_signals)})")
    for _, row in neutral_signals.head(12).iterrows():
        score_info = f" | 评分 {row['strength']:.2f}" if 'strength' in row else ""
        lines.append(f"- {row['stock_name']}({row['stock_code']}){score_info}")

    if len(neutral_signals) > 12:
        lines.append(f"... 等 {len(neutral_signals)-12} 条更多")

    lines.append("\n> 本策略由系统自动生成，仅供参考。")
    
    return "\n".join(lines)

def generate_plan(csv_path=config.SIGNALS_CSV, target_date=None):
    text = get_plan_text(csv_path, target_date)
    print(text)

if __name__ == "__main__":
    # 默认生成最新日期的计划
    generate_plan()
