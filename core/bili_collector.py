import asyncio
import aiohttp
import json
import datetime
import csv
import os
import logging
import re
import time
import random
import sys
import os

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/collector.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

import json

# 复用原有配置
UID_MAP_INTERNAL = {
    550494308: "卢本圆复盘",
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
    1227770900: "A股妍秘书实盘",
    3546948751591735: "A股小哥实盘记录",
    1705723922: "A股实盘账操作分享",
    1233307142: "坤哥超短实盘",
    3546728252836310: "超短实盘记录",
    3493116441004244: "知行超短实盘",
    285340365: "九哥实盘日记",
    3546563727067224: "月影实盘日记",
    3546899342690577: "18万实盘日记",
    592270874: "纯阳的500万实盘日记",
    3546941373811164: "小小小实盘日记",
    94655738: "小明的实盘日记",
    3546643213323115: "白鸽的实盘日记",
    3546914719009584: "Rayson的实盘日记",
    3546863043086795: "悍匪实盘日记",
    3546975024712279: "星仔实盘日记",
    1907033727: "涛哥的实盘日记",
    382677166: "南一环路实盘日记",
    1054375251: "散户每日实盘记录",
    1400477342: "每日实盘实战记录",
    1212432168: "风风每日实盘记录",
    1123968179: "交易实盘日记",
    3546823656475202: "Nutss的实盘日记",
    482301429: "阿Y的实盘日记",
    477190377: "老杜的实盘日记",
    3546972040464700: "小卢的实盘日记",
    1021819945: "游资老王实盘",
}

# 尝试加载外部列表并合并
UID_MAP = UID_MAP_INTERNAL.copy()
UP_LIST_FILE = "data/up_list.json"
if os.path.exists(UP_LIST_FILE):
    try:
        with open(UP_LIST_FILE, 'r', encoding='utf-8') as f:
            external_ups = json.load(f)
            for uid, name in external_ups.items():
                # JSON keys are always strings, convert to int if possible for consistency
                try:
                    # Filter: Keep bloggers with relevant keywords
                    keywords = ["实盘", "交割单", "复盘", "游资", "打板", "龙头", "交易", "记录"]
                    if not any(k in name for k in keywords):
                        continue
                        
                    uid_int = int(uid)
                    if uid_int not in UID_MAP:
                        UID_MAP[uid_int] = name
                except:
                    pass
        print(f"Loaded {len(external_ups)} external UPs. Total unique UPs: {len(UID_MAP)}")
    except Exception as e:
        print(f"Error loading up_list.json: {e}")

# Load monitored UPs (Active subset)
MONITORED_LIST_FILE = "data/monitored_ups.json"
if os.path.exists(MONITORED_LIST_FILE):
    try:
        with open(MONITORED_LIST_FILE, 'r', encoding='utf-8') as f:
            monitored_ups = json.load(f)
            count = 0
            for uid, name in monitored_ups.items():
                try:
                    uid_int = int(uid)
                    if uid_int not in UID_MAP:
                        UID_MAP[uid_int] = name
                        count += 1
                except:
                    pass
        print(f"Loaded {count} new monitored UPs from {MONITORED_LIST_FILE}. Total unique UPs: {len(UID_MAP)}")
    except Exception as e:
        print(f"Error loading monitored_ups.json: {e}")

# 全量运行
UID_LIST = list(UID_MAP.keys()) 

BILI_SESSDATA = "c85e0129%2C1785554128%2Cae0eb%2A21CjCTX6LPMl_eX4I-7qwEitcJqwiEiNP6tBucoDCpupjIG-GR9i8mV6mLKRmxCtiCPgQSVkdHOEFvdVVFRk54X241b3I3M2x1SFE5Q2pfR0xoUnlKaElsX1RFTmdtbTFvcDdjcy1rclFYbGFLTElDd2l5OUJObHZxcTE1SnF5dmJoTFF6WXNMTzJnIIEC"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Cookie": f"SESSDATA={BILI_SESSDATA}",
    "Referer": "https://www.bilibili.com/"
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
VIDEOS_CSV = os.path.join(PROJECT_DIR, config.VIDEOS_CSV)
COMMENTS_CSV = os.path.join(PROJECT_DIR, config.COMMENTS_CSV)

DISCOVERY_VIDEOS_CSV = os.path.join(PROJECT_DIR, "data", "discovery_videos.csv")
DISCOVERY_COMMENTS_CSV = os.path.join(PROJECT_DIR, "data", "discovery_comments.csv")

class BiliCollector:
    def __init__(
        self,
        history_mode: bool = False,
        mode: str = "monitor",
        days_back: int = 30,
        videos_csv: str | None = None,
        comments_csv: str | None = None,
    ):
        self.session = None
        self.history_mode = history_mode
        self.mode = mode
        self.days_back = days_back
        self.since_dt = None
        if self.mode == "discovery":
            self.since_dt = datetime.datetime.now() - datetime.timedelta(days=max(1, int(days_back)))
        self.lock = asyncio.Lock()
        self.existing_dynamic_ids = set()
        self.videos_csv = videos_csv or (DISCOVERY_VIDEOS_CSV if self.mode == "discovery" else VIDEOS_CSV)
        self.comments_csv = comments_csv or (DISCOVERY_COMMENTS_CSV if self.mode == "discovery" else COMMENTS_CSV)
        self.init_csv()
        self.load_existing_ids()

    def init_csv(self):
        # 初始化视频/动态数据表
        if not os.path.exists(self.videos_csv):
            with open(self.videos_csv, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(['dynamic_id', 'oid', 'type', 'author_id', 'author_name', 'publish_time', 'title', 'content', 'url'])
        
        # 初始化评论数据表
        if not os.path.exists(self.comments_csv):
            with open(self.comments_csv, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(['comment_id', 'oid', 'dynamic_id', 'user_id', 'user_name', 'content', 'publish_time', 'like_count', 'reply_count'])

    def load_existing_ids(self):
        try:
            with open(self.videos_csv, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if 'dynamic_id' in row:
                        self.existing_dynamic_ids.add(str(row['dynamic_id']))
            print(f"Loaded {len(self.existing_dynamic_ids)} existing videos from {self.videos_csv}.")
        except Exception as e:
            print(f"Error loading existing IDs: {e}")

    async def init_session(self):
        self.session = aiohttp.ClientSession(headers=HEADERS)

    async def close_session(self):
        if self.session:
            await self.session.close()

    def clean_text(self, text):
        if not text:
            return ""
        # 去除HTML标签
        text = re.sub(r'<[^>]+>', '', text)
        # 去除多余空白
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    async def save_video(self, data):
        did = str(data['dynamic_id'])
        if did in self.existing_dynamic_ids:
            return

        async with self.lock: # 简单的锁，防止并发写入混乱（虽然当前是单线程asyncio）
            if did in self.existing_dynamic_ids: # Double check inside lock
                return
            
            with open(self.videos_csv, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    data['dynamic_id'], data['oid'], data['type'], 
                    data['author_id'], data['author_name'], 
                    data['publish_time'], data['title'], 
                    self.clean_text(data['content']), data['url']
                ])
            self.existing_dynamic_ids.add(did)

    async def save_comments(self, comments_list):
        if not comments_list:
            return
        with open(self.comments_csv, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            for c in comments_list:
                writer.writerow([
                    c['rpid'], c['oid'], c['dynamic_id'],
                    c['mid'], c['uname'],
                    self.clean_text(c['content']),
                    c['ctime'], c['like'], c['rcount']
                ])

    async def fetch_url(self, url, params=None):
        # 增加随机等待，防止请求过快
        delay = random.uniform(2, 5)
        await asyncio.sleep(delay)

        retries = 3
        while retries > 0:
            try:
                async with self.session.get(url, params=params) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 412:
                        logging.warning(f"412 detected (Precondition Failed). Pausing for 60s...")
                        await asyncio.sleep(60)
                        retries -= 1
                    elif resp.status == 404:
                        logging.error(f"404 Not Found: {url}")
                        return None
                    else:
                        logging.error(f"Request failed: {url} status {resp.status}")
                        return None
            except Exception as e:
                logging.error(f"Exception fetching {url}: {e}")
                await asyncio.sleep(5)
                retries -= 1
        return None

    async def validate_uid(self, uid):
        """验证UID是否有效（检查主页是否可达）"""
        url = f"https://space.bilibili.com/{uid}"
        try:
            # 增加随机等待
            await asyncio.sleep(random.uniform(1, 3))
            async with self.session.get(url) as resp:
                if resp.status == 404:
                    return False
                return True
        except Exception as e:
            logging.warning(f"Error validating UID {uid}: {e}")
            return False

    async def get_comments(self, oid, type_, dynamic_id, author_id):
        """获取指定OID的评论，仅保留UP主的评论"""
        page = 1
        all_comments = []
        max_pages = 3  # UP主评论通常在第一页或被置顶，不需要翻太多页
        
        while page <= max_pages:
            url = "https://api.bilibili.com/x/v2/reply"
            params = {
                "type": type_, # 1: 视频, 11: 图片动态, 17: 文本动态
                "oid": oid,
                "sort": 1, # 按时间排序
                "pn": page,
                "ps": 20
            }
            
            data = await self.fetch_url(url, params)
            if not data or data['code'] != 0:
                break
                
            replies = data.get('data', {}).get('replies', [])
            if not replies:
                break
                
            for r in replies:
                # 仅保留UP主本人的评论
                if str(r['mid']) != str(author_id):
                    continue

                content = r['content']['message']
                ctime = datetime.datetime.fromtimestamp(r['ctime']).strftime('%Y-%m-%d %H:%M:%S')
                all_comments.append({
                    'rpid': r['rpid'],
                    'oid': oid,
                    'dynamic_id': dynamic_id,
                    'mid': r['mid'],
                    'uname': r['member']['uname'],
                    'content': content,
                    'ctime': ctime,
                    'like': r['like'],
                    'rcount': r['rcount']
                })
            
            page += 1
            
        if all_comments:
            logging.info(f"Fetched {len(all_comments)} UP comments for oid {oid}")
            await self.save_comments(all_comments)

    async def process_user(self, uid, name):
        logging.info(f"Start processing user: {name} ({uid})")
        offset = 0
        has_more = 1
        
        # 限制获取的动态数量，避免无限循环，设为 50 条或直到没有更多
        count = 0
        max_count = 30 if self.mode == "monitor" else 200

        while has_more and count < max_count:
            url = "https://api.vc.bilibili.com/dynamic_svr/v1/dynamic_svr/space_history"
            params = {"host_uid": uid, "offset_dynamic_id": offset, "need_top": 0}
            
            data = await self.fetch_url(url, params)
            if not data or data['code'] != 0:
                logging.error(f"Failed to fetch dynamics for {name}")
                break
                
            cards = data.get('data', {}).get('cards', [])
            has_more = data.get('data', {}).get('has_more', 0)
            offset = data.get('data', {}).get('next_offset', 0)
            
            if not cards:
                break

            for card in cards:
                count += 1
                desc = card.get('desc', {})
                dynamic_id = desc.get('dynamic_id_str')
                type_code = desc.get('type')
                timestamp = desc.get('timestamp')
                publish_time = datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
                
                # 优化：只抓取今天的数据 (根据用户需求：早盘数据)
                # 如果发现数据日期早于今天，直接停止该UP主的抓取
                pub_dt = datetime.datetime.fromtimestamp(timestamp)
                pub_date = pub_dt.date()
                today = datetime.date.today()
                
                if self.mode == "monitor":
                    if not self.history_mode and pub_date < today:
                        logging.info(f"Reached data older than today ({pub_date}) for {name}, stopping.")
                        has_more = 0
                        break

                    is_morning = pub_dt.hour < 11 or (pub_dt.hour == 11 and pub_dt.minute <= 30)
                    if not is_morning:
                        continue
                elif self.mode == "discovery":
                    if self.since_dt and pub_dt < self.since_dt:
                        logging.info(f"Reached data older than since_dt ({self.since_dt}) for {name}, stopping.")
                        has_more = 0
                        break
                
                try:
                    card_json = json.loads(card.get('card', '{}'))
                except:
                    continue

                # 解析动态类型
                oid = 0
                comment_type = 0 # 默认评论类型
                title = ""
                content = ""
                url_link = ""
                
                # 8: 视频, 2: 图片, 4: 纯文字, 64: 专栏
                if type_code == 8: # 视频
                    oid = desc.get('rid')
                    comment_type = 1
                    title = card_json.get('title', '')
                    content = card_json.get('desc', '')
                    url_link = f"https://www.bilibili.com/video/av{oid}"
                elif type_code == 2: # 图片
                    oid = desc.get('rid')
                    comment_type = 11
                    item = card_json.get('item', {})
                    content = item.get('description', '')
                    url_link = f"https://t.bilibili.com/{dynamic_id}"
                elif type_code == 4: # 纯文字
                    oid = dynamic_id # 纯文字动态的 oid 就是 dynamic_id
                    comment_type = 17
                    item = card_json.get('item', {})
                    content = item.get('content', '')
                    url_link = f"https://t.bilibili.com/{dynamic_id}"
                elif type_code == 64: # 专栏
                    oid = desc.get('rid')
                    comment_type = 12 # 专栏 type 通常是 12
                    title = card_json.get('title', '')
                    content = card_json.get('summary', '')
                    url_link = f"https://www.bilibili.com/read/cv{oid}"
                else:
                    # 其他类型跳过或记录
                    logging.warning(f"Skipping type {type_code} for {dynamic_id}")
                    continue

                # 保存视频/动态信息
                video_data = {
                    'dynamic_id': dynamic_id,
                    'oid': oid,
                    'type': type_code,
                    'author_id': uid,
                    'author_name': name,
                    'publish_time': publish_time,
                    'title': title,
                    'content': content,
                    'url': url_link
                }
                
                # 检查是否已存在，决定是否抓取评论
                is_new = str(dynamic_id) not in self.existing_dynamic_ids
                
                await self.save_video(video_data)
                
                # 获取评论
                if oid and is_new:
                    await self.get_comments(oid, comment_type, dynamic_id, uid)
                else:
                    logging.info(f"Skipping comments for existing video {dynamic_id}")
                
            # 分页间隔已经在 fetch_url 中有随机延迟了，这里保留极小延迟或去掉
            
    async def run(self, uid_map: dict[int, str] | None = None):
        await self.init_session()
        
        logging.info("Validating UIDs...")
        valid_uids = []
        active_uid_map = uid_map or UID_MAP
        uid_list = list(active_uid_map.keys())
        for uid in uid_list:
            name = active_uid_map.get(uid, "Unknown")
            if await self.validate_uid(uid):
                valid_uids.append(uid)
                logging.info(f"UID Valid: {uid} ({name})")
            else:
                logging.warning(f"UID Invalid or 404: {uid} ({name}) - Skipping")
        
        # 顺序执行，避免并发触发风控
        for uid in valid_uids:
            name = active_uid_map.get(uid, "Unknown")
            await self.process_user(uid, name)
            
        await self.close_session()
        logging.info("All tasks completed.")

if __name__ == "__main__":
    collector = BiliCollector()
    asyncio.run(collector.run())
