import sys
import os
import asyncio
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.bili_collector import BiliCollector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def fetch_new_blogger(uid, name):
    # Initialize with history_mode=True
    collector = BiliCollector(history_mode=True)
    await collector.init_session()
    
    logger.info(f"Fetching HISTORY data for {name} ({uid})...")
    
    try:
        await collector.process_user(uid, name)
    finally:
        await collector.close_session()

if __name__ == "__main__":
    uid = 3691009626081367
    name = "小匠财"
    asyncio.run(fetch_new_blogger(uid, name))
