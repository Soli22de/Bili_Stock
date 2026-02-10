import pandas as pd
import requests
import os
import re
from bs4 import BeautifulSoup
import time
import random
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
DATA_DIR = "data"
IMAGES_DIR = os.path.join(DATA_DIR, "images")
VIDEOS_CSV = os.path.join(DATA_DIR, "dataset_videos.csv")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "https://www.bilibili.com/"
}

# Target Bloggers (High Yield First)
TARGET_BLOGGERS = ['超短奴财', '博主B', '松风论龙头', '添材投资', '行者实盘']

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def get_cover_url(video_url):
    try:
        # Check if it's a dynamic or video
        if "t.bilibili.com" in video_url:
            # Dynamic parsing is harder without API, skip for now or try simple regex
            return None
            
        time.sleep(random.uniform(0.5, 1.5))
        response = requests.get(video_url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            # Try meta tag
            meta_image = soup.find("meta", property="og:image")
            if meta_image:
                return meta_image["content"]
            # Try itemprop
            itemprop_image = soup.find("meta", itemprop="image")
            if itemprop_image:
                return itemprop_image["content"]
    except Exception as e:
        logger.error(f"Error fetching cover for {video_url}: {e}")
    return None

def download_image(url, save_path):
    try:
        if not url:
            return False
        if not url.startswith('http'):
            url = 'https:' + url
            
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                f.write(response.content)
            return True
    except Exception as e:
        logger.error(f"Error downloading image {url}: {e}")
    return False

def main():
    ensure_dir(IMAGES_DIR)
    
    if not os.path.exists(VIDEOS_CSV):
        logger.error(f"{VIDEOS_CSV} not found!")
        return

    df = pd.read_csv(VIDEOS_CSV)
    
    # Filter for target bloggers
    df_target = df[df['author_name'].isin(TARGET_BLOGGERS)].copy()
    logger.info(f"Found {len(df_target)} videos from target bloggers.")
    
    success_count = 0
    for index, row in df_target.iterrows():
        video_id = str(row.get('dynamic_id', ''))
        video_url = row.get('url', '')
        author = row.get('author_name', 'Unknown')
        
        if not video_id or not video_url:
            continue
            
        save_path = os.path.join(IMAGES_DIR, f"{video_id}.jpg")
        
        if os.path.exists(save_path):
            # logger.info(f"Image for {video_id} already exists. Skipping.")
            continue
            
        logger.info(f"Processing {author} - {video_id}...")
        
        cover_url = get_cover_url(video_url)
        if cover_url:
            if download_image(cover_url, save_path):
                success_count += 1
                logger.info(f"Downloaded cover for {video_id}")
            else:
                logger.warning(f"Failed to download image for {video_id}")
        else:
            logger.warning(f"Could not find cover URL for {video_id}")
            
    logger.info(f"Finished! Downloaded {success_count} new images.")

if __name__ == "__main__":
    main()
