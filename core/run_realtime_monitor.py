import asyncio
import logging
import time
from datetime import datetime
import sys
import os

# Add parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.monitor_and_notify import StockMonitor
import config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/realtime_monitor.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def is_trading_time():
    """检查是否为交易时间 (9:15-11:30, 13:00-15:05)"""
    now = datetime.now()
    t = now.time()
    
    # 周末不交易
    if now.weekday() >= 5: 
        return False
        
    morning_start = datetime.strptime("09:15", "%H:%M").time()
    morning_end = datetime.strptime("11:30", "%H:%M").time()
    afternoon_start = datetime.strptime("13:00", "%H:%M").time()
    afternoon_end = datetime.strptime("15:05", "%H:%M").time()
    
    if (morning_start <= t <= morning_end) or (afternoon_start <= t <= afternoon_end):
        return True
    return False

async def main_loop():
    logger.info("Initializing Real-time Monitor...")
    monitor = StockMonitor()
    
    # 初始连接检查
    logger.info("Checking connections...")
    status = await monitor.check_connection_status()
    logger.info(f"Connection status: {status}")
    
    if not status["realtime_api"]:
        logger.warning("Realtime API is not available at startup. Monitor may degrade.")
    
    logger.info("Entering monitoring loop. Press Ctrl+C to stop.")
    
    try:
        while True:
            # 1. 检查是否在交易时间
            if not is_trading_time():
                logger.info("Not trading time. Sleeping for 60s...")
                await asyncio.sleep(60)
                continue
            
            loop_start = datetime.now()
            logger.info(">>> Starting monitor cycle...")
            
            # 2. 执行一次完整的监控流程
            # 包括：收集视频 -> 提取信号 -> 验证价格 -> 发送警报
            try:
                await monitor.run_once()
            except Exception as e:
                logger.error(f"Error during monitor cycle: {e}", exc_info=True)
            
            # 3. 智能等待
            # 计算本次循环耗时
            elapsed = (datetime.now() - loop_start).total_seconds()
            
            # 默认间隔 300秒 (5分钟)
            # 信号是基于视频的，UP主发视频频率不高，5分钟一次足够
            # 过于频繁会触发 B站反爬
            interval = 300 
            
            sleep_time = max(10, interval - elapsed)
            logger.info(f"<<< Cycle finished. Sleeping for {sleep_time:.1f}s...")
            
            await asyncio.sleep(sleep_time)
            
    except KeyboardInterrupt:
        logger.info("Monitor stopped by user.")
    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
    finally:
        logger.info("Monitor shutdown.")

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        pass
