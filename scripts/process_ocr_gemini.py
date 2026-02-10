import os
import pandas as pd
import sys
import time
import logging

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.llm_processor import LLMProcessor
from config import GEMINI_API_KEY

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = "data"
IMAGES_DIR = os.path.join(DATA_DIR, "images")
OCR_RESULTS_CSV = os.path.join(DATA_DIR, "ocr_results.csv")
VIDEOS_CSV = os.path.join(DATA_DIR, "dataset_videos.csv")

def load_video_info():
    if os.path.exists(VIDEOS_CSV):
        return pd.read_csv(VIDEOS_CSV)
    return pd.DataFrame()

def main():
    # Initialize LLM Processor
    # Ensure API Key is available
    api_key = os.environ.get("GEMINI_API_KEY") or GEMINI_API_KEY
    if not api_key:
        logger.error("GEMINI_API_KEY not found. Please set it in config.py or environment variables.")
        return

    processor = LLMProcessor(api_key=api_key)
    logger.info("LLM Processor initialized.")

    # Load existing results
    if os.path.exists(OCR_RESULTS_CSV):
        df_results = pd.read_csv(OCR_RESULTS_CSV)
        processed_ids = set(df_results['video_id'].astype(str))
    else:
        df_results = pd.DataFrame(columns=[
            'video_id', 'author_name', 'stock_name', 'stock_code', 
            'ocr_price', 'ocr_verified', 'ocr_confidence', 'ocr_reason', 'verification_details'
        ])
        processed_ids = set()

    # Load video info for context
    df_videos = load_video_info()
    video_map = {}
    if not df_videos.empty:
        for _, row in df_videos.iterrows():
            vid = str(row.get('dynamic_id', ''))
            video_map[vid] = {
                'title': row.get('title', ''),
                'author': row.get('author_name', 'Unknown')
            }

    # Process images
    if not os.path.exists(IMAGES_DIR):
        logger.warning(f"No images directory found at {IMAGES_DIR}")
        return

    image_files = [f for f in os.listdir(IMAGES_DIR) if f.endswith('.jpg')]
    logger.info(f"Found {len(image_files)} images.")

    new_results = []
    
    for i, img_file in enumerate(image_files):
        if i >= 10:
            logger.info("Limit of 10 images reached for this run.")
            break
            
        video_id = os.path.splitext(img_file)[0]
        
        if video_id in processed_ids:
            continue
            
        logger.info(f"Processing image for video {video_id}...")
        image_path = os.path.join(IMAGES_DIR, img_file)
        
        # Get context
        context = ""
        author_name = "Unknown"
        if video_id in video_map:
            info = video_map[video_id]
            context = f"Video Title: {info['title']}"
            author_name = info['author']
            
        # Call Gemini
        try:
            result = processor.analyze_image(image_path, context_text=context)
            
            # Parse result
            if "error" in result:
                logger.error(f"Error processing {video_id}: {result['error']}")
                continue
                
            # Map JSON to CSV columns
            # JSON keys: stock_name, stock_code, price, profit_rate, is_holding, is_transaction, verification_status
            
            is_verified = False
            if result.get('verification_status') == 'Verified':
                is_verified = True
            elif result.get('is_holding') or result.get('is_transaction'):
                # Weak verification if Gemini thinks it's a holding/transaction but didn't explicitly say Verified
                is_verified = True
            
            entry = {
                'video_id': video_id,
                'author_name': author_name,
                'stock_name': result.get('stock_name', ''),
                'stock_code': result.get('stock_code', ''),
                'ocr_price': result.get('price', ''),
                'ocr_verified': is_verified,
                'ocr_confidence': 0.8 if is_verified else 0.0, # Placeholder confidence
                'ocr_reason': f"Holding: {result.get('is_holding')}, Trans: {result.get('is_transaction')}",
                'verification_details': str(result)
            }
            
            new_results.append(entry)
            processed_ids.add(video_id)
            
            # Save incrementally
            if len(new_results) % 5 == 0:
                pd.DataFrame(new_results).to_csv(OCR_RESULTS_CSV, mode='a', header=not os.path.exists(OCR_RESULTS_CSV), index=False)
                new_results = [] # Clear buffer
                
            time.sleep(2) # Rate limit protection
            
        except Exception as e:
            logger.error(f"Exception processing {video_id}: {e}")

    # Save remaining
    if new_results:
        pd.DataFrame(new_results).to_csv(OCR_RESULTS_CSV, mode='a', header=not os.path.exists(OCR_RESULTS_CSV), index=False)
        
    logger.info("OCR Processing Complete.")

if __name__ == "__main__":
    main()
