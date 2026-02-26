import asyncio
import aiohttp
import sqlite3
import json
import datetime
import re
import csv
import os

# ==================== 核心配置区 ====================
UID_MAP = {
    1810671711: "添材投资",
    1039576265: "博主B",
    21119247: "跳跳虎",
    3632311444703538: "小伙龙",
    511678642: "财迷有研",
    3691009626081367: "小匠财",
    3546948390881837: "小玉研财", 
    3632305287465674: "栖鸟听风",
    3546571476044623: "松风论龙头",
    3546905638341210: "行者实盘",
    3546890746465093: "追涨日记",
    3691104060240069: "成长笔记",
    1965519712: "超短奴财",
    691125305: "冰点擒牛",
    # 注意：此ID可能是动态ID而非用户UID，若无数据请核对
}
UID_LIST = list(UID_MAP.keys())

# True: 扫描今天历史动态 | False: 实时监控
HISTORY_MODE = True 

BILI_SESSDATA = "c85e0129%2C1785554128%2Cae0eb%2A21CjCTX6LPMl_eX4I-7qwEitcJqwiEiNP6tBucoDCpupjIG-GR9i8mV6mLKRmxCtiCPgQSVkdHOEFvdVVFRk54X241b3I3M2x1SFE5Q2pfR0xoUnlKaElsX1RFTmdtbTFvcDdjcy1rclFYbGFLTElDd2l5OUJObHZxcTE1SnF5dmJoTFF6WXNMTzJnIIEC"

# 【重要】请填入你的 Key，否则 CSV 里只有表头，没有内容！
GEMINI_API_KEY = "AIzaSyBNRzGiAuQk9MQEcJbVfdQ1cncmtl58RFw" 

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Cookie": f"SESSDATA={BILI_SESSDATA}",
    "Referer": "https://space.bilibili.com/"
}

# 自动获取脚本所在目录，确保 CSV 生成在正确位置
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE = os.path.join(BASE_DIR, "strategy_log.csv")
DB_FILE = os.path.join(BASE_DIR, "strategy_data.db")

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('CREATE TABLE IF NOT EXISTS history (id TEXT PRIMARY KEY, type TEXT, ts DATETIME)')
    conn.commit()
    
    # 初始化CSV
    if not os.path.exists(CSV_FILE):
        try:
            with open(CSV_FILE, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(['时间', '博主', '股票', '方向', '价格', '逻辑', '评分', '原文'])
            print(f">>> CSV 文件已创建: {CSV_FILE}")
        except Exception as e:
            print(f"创建CSV失败: {e}")
    return conn

def is_new(conn, unique_id):
    if HISTORY_MODE: return True 
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM history WHERE id = ?", (unique_id,))
    return cursor.fetchone() is None

def mark_done(conn, unique_id):
    if HISTORY_MODE: return 
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO history VALUES (?, ?, ?)", (unique_id, "dynamic", datetime.datetime.now()))
    conn.commit()

def save_to_csv(data):
    try:
        with open(CSV_FILE, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow([
                data['time'], data['blogger'], data['stock'], 
                data['action'], data['price'], data['logic'], 
                data['score'], data['raw_text'][:30]
            ])
            print(f">>> 策略已写入 CSV")
    except Exception as e:
        print(f"写入CSV失败: {e}")

async def get_image_bytes(session, img_url):
    """下载图片并转为Base64前的二进制数据"""
    try:
        async with session.get(img_url) as resp:
            if resp.status == 200:
                return await resp.read()
    except:
        return None
    return None

async def ai_analyze(session, text, img_url_list):
    """多模态分析"""
    if not GEMINI_API_KEY:
        return None

    prompt_text = f"""
    你是一个资深的A股短线交易员。
    请分析博主的【文字描述】以及【配图内容】(图中可能包含自选股列表或持仓截图)。
    
    提取其中的交易计划。
    注意：如果图中包含多个股票，只提取最核心的一个（通常是高亮、置顶或文字重点提到的）。
    
    博主文字：
    {text}
    
    请严格按 JSON 格式回复：
    {{
        "has_strategy": true/false,
        "stock_name": "股票名称",
        "action": "买入/卖出/观望/持仓",
        "price": "目标价位",
        "logic": "逻辑摘要(15字内)",
        "score": 0-100
    }}
    """
    
    parts = [{"text": prompt_text}]
    
    if img_url_list:
        import base64
        # 只处理第一张图
        img_data = await get_image_bytes(session, img_url_list[0])
        if img_data:
            b64_img = base64.b64encode(img_data).decode('utf-8')
            parts.append({
                "inline_data": {
                    "mime_type": "image/jpeg",
                    "data": b64_img
                }
            })

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-preview-09-2025:generateContent?key={GEMINI_API_KEY}"
    payload = {
        "contents": [{"parts": parts}], 
        "generationConfig": {"responseMimeType": "application/json"}
    }
    
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status == 200:
                data = await resp.json()
                raw_res = data['candidates'][0]['content']['parts'][0]['text']
                return json.loads(raw_res)
            else:
                return None
    except Exception as e:
        print(f"AI分析异常: {e}")
    return None

async def check_user(session, uid, name, conn):
    today = datetime.datetime.now().date()
    url = "https://api.vc.bilibili.com/dynamic_svr/v1/dynamic_svr/space_history"
    params = {"host_uid": uid, "offset_dynamic_id": 0, "need_top": 1}
    
    try:
        async with session.get(url, params=params) as resp:
            if resp.status != 200: return
            data = await resp.json()
            if data['code'] != 0: return
            cards = data.get('data', {}).get('cards', [])

            scan_count = 10 if HISTORY_MODE else 2
            for card in cards[:scan_count]:
                desc = card.get('desc', {})
                did = desc.get('dynamic_id_str')
                pub_ts = desc.get('timestamp')
                pub_time = datetime.datetime.fromtimestamp(pub_ts)
                
                if pub_time.date() != today and not HISTORY_MODE: continue

                if is_new(conn, did):
                    try:
                        card_data = json.loads(card.get('card', '{}'))
                    except: continue
                    
                    content = ""
                    img_urls = []

                    if 'item' in card_data:
                        # 提取文字
                        if 'description' in card_data['item']: 
                            content = card_data['item']['description']
                        elif 'content' in card_data['item']: 
                            content = card_data['item']['content']
                        
                        # 提取图片 (修复 NoneType 报错的核心点)
                        pictures = card_data['item'].get('pictures')
                        if pictures: # 确保 pictures 不为 None
                            img_urls = [p['img_src'] for p in pictures]
                    
                    elif 'origin' in card_data:
                        content = "【转发动态】"

                    if content or img_urls:
                        time_str = pub_time.strftime('%H:%M')
                        print(f"[{time_str}] 分析 {name}...", end="\r")
                        
                        res = await ai_analyze(session, content, img_urls)
                        
                        if res and res['has_strategy']:
                            print(f"[{time_str}] {name} 🟢 {res['stock_name']} | {res['action']} ({res['score']}分)")
                            print(f"   逻辑: {res['logic']}")
                            
                            save_data = {
                                "time": time_str,
                                "blogger": name,
                                "stock": res['stock_name'],
                                "action": res['action'],
                                "price": res['price'],
                                "logic": res['logic'],
                                "score": res['score'],
                                "raw_text": content
                            }
                            save_to_csv(save_data)
                        elif HISTORY_MODE:
                            tag = "🖼️含图" if img_urls else "📄文字"
                            ai_status = "❌Key未填" if not GEMINI_API_KEY else "⚪无信号"
                            print(f"[{time_str}] {name} {ai_status} | {tag}")
                    
                    mark_done(conn, did)

    except Exception as e:
        print(f"UID {uid} 错误: {e}")

async def main():
    conn = init_db()
    print(f">>> 监控启动 | 结果将存入: {CSV_FILE}")
    if not GEMINI_API_KEY:
        print(">>> ⚠️ 警告: 未检测到 API KEY！脚本将仅抓取，不会分析，也不会写入 CSV。")
        print(">>> 请去 https://aistudio.google.com/app/apikey 申请 Key 并填入代码第 22 行。")
    
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        while True:
            for uid in UID_LIST:
                name = UID_MAP.get(uid, "未知")
                await check_user(session, uid, name, conn)
                await asyncio.sleep(1)
            
            if HISTORY_MODE:
                print("\n>>> 复盘结束。如果 CSV 为空，请检查是否填入了 API KEY。")
                break
            
            await asyncio.sleep(300)

if __name__ == "__main__":
    asyncio.run(main())