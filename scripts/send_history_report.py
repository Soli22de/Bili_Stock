import asyncio
import pandas as pd
import sys
import os

# Add parent directory to sys.path to import config and notifier
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from core.notifier import DingTalkNotifier

async def send_report():
    print("正在生成历史数据报告...")
    
    try:
        df = pd.read_csv(config.SIGNALS_CSV)
    except FileNotFoundError:
        print(f"未找到 {config.SIGNALS_CSV}，请先运行信号提取。")
        return

    if df.empty:
        print("信号表为空。")
        return
        
    # 统计信息
    total = len(df)
    buy_signals = df[df['action'] == 'BUY']
    sell_signals = df[df['action'] == 'SELL']
    
    # 最近的5条买入信号
    recent_buys = buy_signals.head(5)
    
    # 构建报告消息
    title = "📊 历史信号回测报告"
    text = f"## 📊 历史信号数据报告\n\n" \
           f"- **总信号数**: {total}\n" \
           f"- **🔴 买入信号**: {len(buy_signals)}\n" \
           f"- **💚 卖出信号**: {len(sell_signals)}\n" \
           f"- **数据覆盖**: {df['date'].min()} ~ {df['date'].max()}\n\n" \
           f"### 最近 5 条买入信号:\n"

    for _, row in recent_buys.iterrows():
        # Handle source_segment safely
        source = str(row.get('source_segment', ''))
        text += f"- **{row['date']}**: {row['stock_name']} ({row['stock_code']}) - {source[:20]}...\n"
        
    text += "\n> 来自 BiliStock 历史数据回测"
    
    # 发送
    notifier = DingTalkNotifier(config.DINGTALK_WEBHOOK, config.DINGTALK_SECRET)
    print("正在发送报告到钉钉...")
    success = await notifier.send_markdown(title, text)
    
    if success:
        print("✅ 报告发送成功！")
    else:
        print("❌ 报告发送失败。")
        
    await notifier.close_session()

if __name__ == "__main__":
    asyncio.run(send_report())
