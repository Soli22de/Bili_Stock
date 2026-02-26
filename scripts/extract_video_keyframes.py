import pandas as pd
import os
import sys
import cv2
import yt_dlp
import logging
import shutil
import glob
import json
import time
import re

# Add project root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Disable PaddlePaddle PIR to avoid runtime errors in v3.x
os.environ["FLAGS_enable_pir_api"] = "0"
os.environ["FLAGS_enable_pir_in_executor"] = "0"
# Disable MKLDNN (OneDNN) to avoid onednn_instruction error
os.environ["FLAGS_use_mkldnn"] = "0"
os.environ["FLAGS_enable_mkldnn"] = "0"

import config
from core.llm_processor import LLMProcessor

# Try to import paddleocr
try:
    from paddleocr import PaddleOCR
    HAS_PADDLE = True
except ImportError:
    HAS_PADDLE = False

# Configure logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(log_dir, "extract_video_keyframes.log"), mode='w', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Constants
DATA_DIR = "data"
KEYFRAMES_DIR = os.path.join(DATA_DIR, "keyframes")
TEMP_VIDEO_DIR = os.path.join(DATA_DIR, "temp_videos")
# Prefer discovery_videos.csv if it exists, otherwise fallback to dataset_videos.csv
if os.path.exists(os.path.join(DATA_DIR, "discovery_videos.csv")):
    VIDEOS_CSV = os.path.join(DATA_DIR, "discovery_videos.csv")
else:
    VIDEOS_CSV = os.path.join(DATA_DIR, "dataset_videos.csv")
OCR_RESULTS_CSV = os.path.join(DATA_DIR, "video_ocr_results.csv")

# Target bloggers will be loaded dynamically
TARGET_BLOGGERS = [] 

def load_target_bloggers():
    """Load high-quality bloggers from rank file"""
    rank_path = os.path.join(DATA_DIR, "trader_bloggers_rank.csv")
    if os.path.exists(rank_path):
        try:
            df = pd.read_csv(rank_path)
            if 'blogger_id' in df.columns:
                bloggers = df['blogger_id'].dropna().unique().tolist()
                # Convert to string to ensure matching
                bloggers = [str(int(b)) for b in bloggers]
                logger.info(f"Loaded {len(bloggers)} target bloggers (IDs) from {rank_path}")
                return bloggers
        except Exception as e:
            logger.warning(f"Failed to load bloggers from rank file: {e}")
    
    # Fallback default list if file missing or empty
    logger.info("Using default target bloggers list")
    return []


# Processing constraints
MAX_VIDEOS_PER_BLOGGER = 20  # Increased limit for better coverage
FRAME_INTERVAL_SEC = 2.0    # Extract 1 frame every X seconds

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

class LocalOCRProcessor:
    def __init__(self):
        self.ocr = None
        self.stock_map = {}
        try:
            stock_map_path = os.path.join(DATA_DIR, "stock_map_final.json")
            if os.path.exists(stock_map_path):
                with open(stock_map_path, "r", encoding="utf-8") as f:
                    self.stock_map = json.load(f)
                logger.info(f"Loaded {len(self.stock_map)} stock mappings")
        except Exception as e:
            logger.warning(f"Failed to load stock map: {e}")

        if HAS_PADDLE:
            self.ocr = PaddleOCR(
                use_angle_cls=True,
                lang='ch',
                det_db_thresh=0.1,  # Lower threshold for detection
                det_db_box_thresh=0.2,
                det_db_unclip_ratio=1.8, # Increase unclip ratio
                det_limit_side_len=1920
            )
            logger.info("PaddleOCR initialized successfully")
        else:
            logger.warning("PaddleOCR not installed. Local OCR capabilities unavailable.")

    def _extract_texts(self, result):
        lines = []
        if result is None:
            return lines
            
        # Debug log for result structure if it's not empty but we might miss it
        # logger.info(f"DEBUG OCR Result type: {type(result)}")
        
        try:
            if isinstance(result, dict):
                for key in ("text", "texts", "rec_text", "ocr_text"):
                    value = result.get(key)
                    if isinstance(value, list):
                        for item in value:
                            text = str(item).strip()
                            if text and text not in lines:
                                lines.append(text)
                    elif isinstance(value, str):
                        text = value.strip()
                        if text and text not in lines:
                            lines.append(text)
                return lines

            if isinstance(result, list):
                if not result:
                    return lines
                
                # Handle list of dicts
                if isinstance(result[0], dict):
                    for item in result:
                        if not isinstance(item, dict):
                            continue
                        text = item.get("text") or item.get("rec_text") or item.get("ocr_text")
                        if isinstance(text, str):
                            text = text.strip()
                            if text and text not in lines:
                                lines.append(text)
                    return lines

                # Handle list of lists (standard PaddleOCR format [[[[points], [text, score]], ...]])
                if isinstance(result[0], list):
                    # Check if it's the standard format: [ [ [[x,y],..], ("text", 0.9) ], ... ]
                    # Sometimes result[0] is the list of line results for the first image
                    
                    # Flatten logic: iterate over all items in the list
                    # If item is a list/tuple and has structure [[x,y]..], (text, score)
                    
                    # Case: result = [ [line1], [line2] ] where line1 = [box, (text, score)]
                    for item in result:
                        if isinstance(item, list) and len(item) > 0:
                            # It might be a list of line results, or a single line result
                            # Check if it looks like [box, (text, score)]
                            if len(item) == 2 and isinstance(item[1], (list, tuple)):
                                text_part = item[1]
                                if len(text_part) > 0 and isinstance(text_part[0], str):
                                    text = text_part[0].strip()
                                    if text and text not in lines:
                                        lines.append(text)
                                continue

                            # Check if it is a list of such items (multiple lines)
                            for subitem in item:
                                if isinstance(subitem, (list, tuple)) and len(subitem) == 2:
                                    text_part = subitem[1]
                                    if isinstance(text_part, (list, tuple)) and len(text_part) > 0:
                                        if isinstance(text_part[0], str):
                                            text = text_part[0].strip()
                                            if text and text not in lines:
                                                lines.append(text)
                    return lines

            return lines
        except Exception as e:
            logger.error(f"Error parsing OCR result: {e}. Result sample: {str(result)[:100]}")
            return lines

    def _build_variants(self, image):
        if image is None:
            return []
        variants = []
        h, w = image.shape[:2]
        crop_top = int(h * 0.12)
        crop_bottom = int(h * 0.92)
        crop_left = int(w * 0.03)
        crop_right = int(w * 0.97)
        if crop_bottom > crop_top and crop_right > crop_left:
            cropped = image[crop_top:crop_bottom, crop_left:crop_right]
            variants.append(cropped)
        scale = 1.5 if min(h, w) < 900 else 1.0
        if scale != 1.0:
            image = cv2.resize(image, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
        variants.append(image)
        gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
        variants.append(gray)
        blur = cv2.GaussianBlur(gray, (3, 3), 0)
        variants.append(blur)
        try:
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8)).apply(gray)
            variants.append(clahe)
        except Exception:
            pass
        _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        variants.append(thresh)
        adaptive = cv2.adaptiveThreshold(gray, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 31, 10)
        variants.append(adaptive)
        return variants

    def scan_image(self, image_path):
        if not self.ocr:
            return None
            
        try:
            image = cv2.imread(image_path)
            if image is None:
                result = self.ocr.ocr(image_path)
                if not result:
                    return []
                return self._extract_texts(result)

            variants = self._build_variants(image)
            texts = []
            for variant in variants:
                try:
                    if variant is not None and len(variant.shape) == 2:
                        variant = cv2.cvtColor(variant, cv2.COLOR_GRAY2BGR)
                    result = self.ocr.ocr(variant)
                    if not result:
                        continue
                    for text in self._extract_texts(result):
                        if text and text not in texts:
                            texts.append(text)
                except Exception as e:
                    logger.error(f"PaddleOCR failed for {image_path}: {e}")
                    continue
            if not texts:
                try:
                    result = self.ocr.ocr(image_path)
                    for text in self._extract_texts(result):
                        if text and text not in texts:
                            texts.append(text)
                except Exception as e:
                    logger.error(f"PaddleOCR failed for {image_path}: {e}")
            return texts
        except Exception as e:
            logger.error(f"PaddleOCR failed for {image_path}: {e}")
            return []

    def detect_keywords(self, text_lines):
        """
        Check for keywords indicating trading/holding
        """
        keywords = ["持仓", "成交", "买入", "卖出", "盈亏", "成本", "市值", "委托", "撤单", "证券", "资产", "交割", "银证", "流水", "日记", "实盘", "打板"]
        if not text_lines:
            return [], False

        joined = " ".join(text_lines)
        normalized = joined.lower().replace(" ", "")
        
        # Enhanced patterns for stock codes
        patterns = [
            r"(?:sz|sh)?\d{6}", # Standard
            r"\b[036]\d{5}\b",   # Standalone 6 digits
            r"(?:\d\s*){6}"      # Spaced digits
        ]
        codes = []
        for pattern in patterns:
            for match in re.findall(pattern, normalized):
                code = re.sub(r"\D", "", match)
                if len(code) == 6 and code not in codes:
                    # Filter common false positives (dates, times, counts)
                    if not (code.startswith('202') or code.startswith('19')): 
                         codes.append(code)
        
        # Check for stock names from map
        for name, code in self.stock_map.items():
            if name in joined or name in normalized:
                if code not in codes:
                    codes.append(code)
        
        found_keywords = [kw for kw in keywords if kw in joined or kw in normalized]
        has_stock_code = len(codes) > 0
        return found_keywords, has_stock_code

def analyze_frames(llm, local_ocr, video_id, author_name, frames_dir, max_frames=20):
    """
    Analyze extracted frames using Local OCR + LLMProcessor
    """
    results = []
    frames = sorted(glob.glob(os.path.join(frames_dir, "*.jpg")))
    
    if not frames:
        logger.warning(f"No frames found for {video_id}")
        return results
        
    logger.info(f"Scanning {len(frames)} frames for {video_id} ({author_name}) using Local OCR...")
    
    # Pre-scan with Local OCR to filter frames
    relevant_frames = []
    
    for frame_path in frames:
        if local_ocr and local_ocr.ocr:
            text_lines = local_ocr.scan_image(frame_path)
            keywords, has_stock_code = local_ocr.detect_keywords(text_lines)
            logger.info(f"Frame {os.path.basename(frame_path)}: keywords={keywords}, has_code={has_stock_code}")
            
            # Log first few chars of text for debugging
            if text_lines:
                preview = " ".join(text_lines)[:50]
                logger.info(f"Frame {os.path.basename(frame_path)} text: {preview}...")
            
            if keywords or has_stock_code:
                # If we found keywords or stock codes, this frame is interesting
                logger.info(f"Local OCR found keywords in {os.path.basename(frame_path)}: {keywords}, StockCode: {has_stock_code}")
                relevant_frames.append({
                    "path": frame_path,
                    "ocr_text": " ".join(text_lines)
                })
        else:
            # Fallback if no local OCR: Add to relevant frames (will be sampled later)
            relevant_frames.append({"path": frame_path, "ocr_text": ""})

    if not relevant_frames:
        logger.info(f"No relevant frames found by Local OCR for {video_id}")
        return results

    # Limit the number of frames sent to LLM
    frames_to_analyze = relevant_frames[:max_frames]
    logger.info(f"Selected {len(frames_to_analyze)} candidate frames for LLM analysis...")
    
    for item in frames_to_analyze:
        frame_path = item["path"]
        logger.info(f"Calling LLM for {os.path.basename(frame_path)}")
        context_text = f"Blogger: {author_name}. Local OCR Text: {item['ocr_text']}"
        
        try:
            # Use Gemini Vision for detailed extraction
            analysis = llm.analyze_image(frame_path, context_text=context_text)
            
            if "error" in analysis:
                logger.warning(f"Error analyzing {frame_path}: {analysis['error']}")
                continue
                
            # Filter for relevant content
            if analysis.get("is_holding") or analysis.get("is_transaction") or analysis.get("verification_status") == "Verified":
                logger.info(f"LLM Confirmed relevant info in {os.path.basename(frame_path)}: {analysis}")
                
                result = {
                    "video_id": video_id,
                    "author_name": author_name,
                    "frame_file": os.path.basename(frame_path),
                    "stock_name": analysis.get("stock_name"),
                    "stock_code": analysis.get("stock_code"),
                    "price": analysis.get("price"),
                    "profit_rate": analysis.get("profit_rate"),
                    "is_holding": analysis.get("is_holding"),
                    "is_transaction": analysis.get("is_transaction"),
                    "verification_status": analysis.get("verification_status"),
                    "analyzed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "local_ocr_text": item['ocr_text']
                }
                results.append(result)
                
                # Incremental save
                save_results([result])
                logger.info(f"Saved result for frame {os.path.basename(frame_path)}")
                
            # Rate limit
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Exception analyzing {frame_path}: {e}")
            
    return results

def save_results(new_results):
    if not new_results:
        return
        
    df_new = pd.DataFrame(new_results)
    
    if os.path.exists(OCR_RESULTS_CSV):
        df_old = pd.read_csv(OCR_RESULTS_CSV)
        df_combined = pd.concat([df_old, df_new], ignore_index=True)
        # Deduplicate based on video_id and frame_file
        df_combined = df_combined.drop_duplicates(subset=["video_id", "frame_file"], keep="last")
    else:
        df_combined = df_new
        
    df_combined.to_csv(OCR_RESULTS_CSV, index=False)
    logger.info(f"Saved {len(new_results)} new results to {OCR_RESULTS_CSV}")

def download_video(url, output_path):
    """Download video using yt-dlp"""
    if os.path.exists(output_path):
        return True
        
    try:
        ydl_opts = {
            # OpenCV often fails with AV1. We prefer AVC (h264) which is most compatible.
            # We don't need audio for OCR, so video-only is fine.
            # 1. Best video-only MP4 with AVC
            # 2. Best single-file MP4 with AVC
            # 3. Best video-only MP4 (any codec, hope for HEVC/AVC)
            # 4. Fallback
            'format': 'bestvideo[ext=mp4][vcodec^=avc]/best[ext=mp4][vcodec^=avc]/bestvideo[ext=mp4]/best[ext=mp4]/best',
            'outtmpl': output_path,
            'quiet': True,
            'no_warnings': True,
            # Use cookies if available to avoid some restrictions
            # 'cookies': 'cookies.txt', 
            'socket_timeout': 15,
            'retries': 3,
            # Add a random user agent to avoid bot detection
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([url])
        return True
    except Exception as e:
        logger.error(f"Download failed for {url}: {e}")
        return False

def extract_frames(video_path, output_dir, interval_sec=2.0):
    """
    Extract frames from video at given interval
    """
    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        logger.error(f"Could not open video {video_path}")
        return 0
    
    fps = cap.get(cv2.CAP_PROP_FPS)
    if fps <= 0:
        fps = 25 # Default fallback
        
    frame_interval = int(fps * interval_sec)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    
    logger.info(f"Video FPS: {fps}, Total Frames: {total_frames}, Interval: {frame_interval} frames")
    
    count = 0
    saved_count = 0
    
    while True:
        ret, frame = cap.read()
        if not ret:
            break
            
        if count % frame_interval == 0:
            frame_name = f"frame_{saved_count:04d}.jpg"
            save_path = os.path.join(output_dir, frame_name)
            cv2.imwrite(save_path, frame, [cv2.IMWRITE_JPEG_QUALITY, 95])
            saved_count += 1
            
        count += 1
        
    cap.release()
    logger.info(f"Extracted {saved_count} frames to {output_dir}")
    return saved_count

def main():
    ensure_dir(KEYFRAMES_DIR)
    ensure_dir(TEMP_VIDEO_DIR)
    
    if not os.path.exists(VIDEOS_CSV):
        logger.error("Videos CSV not found")
        return

    # Initialize LLM Processor
    llm = LLMProcessor(api_key=config.GEMINI_API_KEY)
    
    # Initialize Local OCR Processor
    local_ocr = LocalOCRProcessor()
    
    # Load target bloggers
    global TARGET_BLOGGERS
    TARGET_BLOGGERS = load_target_bloggers()
    
    # Read CSV with string types for IDs to preserve precision and formatting
    df = pd.read_csv(VIDEOS_CSV, dtype={'dynamic_id': str, 'author_id': str, 'oid': str})
    
    # Filter for target bloggers
    if 'author_id' in df.columns:
        # Ensure string type and handle potential float formatting
        df['author_id'] = df['author_id'].astype(str).str.replace(r'\.0$', '', regex=True)
        df = df[df['author_id'].isin(TARGET_BLOGGERS)]
    else:
        logger.warning("No author_id column in videos CSV")
        return
    
    # Sort by date descending (newest first)
    if 'publish_time' in df.columns:
        df['publish_time'] = pd.to_datetime(df['publish_time'], errors='coerce')
        df = df.sort_values('publish_time', ascending=False)
        
    logger.info(f"Found {len(df)} videos for target bloggers (IDs): {TARGET_BLOGGERS}")
    
    # Debug: Print first few video titles and authors
    if not df.empty:
        logger.info("Sample videos to process:")
        for i, row in df.head(5).iterrows():
            logger.info(f"- {row['author_name']} (ID: {row['author_id']}): {row['title']} ({row['url']})")

    processed_counts = {mid: 0 for mid in TARGET_BLOGGERS}
    all_ocr_results = []
    
    for index, row in df.iterrows():
        blogger_name = row['author_name']
        blogger_id = str(row['author_id'])
        video_url = row['url']
        video_id = str(row['dynamic_id']) # Using dynamic_id as unique key
        
        if processed_counts.get(blogger_id, 0) >= MAX_VIDEOS_PER_BLOGGER:
            continue
            
        # Skip if not a video link (simple check)
        # Note: Bilibili dynamic links might be like https://t.bilibili.com/xxx or https://www.bilibili.com/video/avxxx
        # We need to handle t.bilibili.com links which might not be videos directly
        if "video/av" not in video_url and "BV" not in video_url:
            # Maybe it's a dynamic without video?
            logger.info(f"Skipping non-video URL (likely text dynamic): {video_url}")
            continue
            
        video_frames_dir = os.path.join(KEYFRAMES_DIR, video_id)
        frames_extracted = False
        
        # Check if frames already exist
        if os.path.exists(video_frames_dir) and len(os.listdir(video_frames_dir)) > 0:
            logger.info(f"Frames already exist for {video_id}. Proceeding to analysis.")
            processed_counts[blogger_id] += 1
            frames_extracted = True
        else:
            ensure_dir(video_frames_dir)
            
            # Define temp video path
            temp_video_path = os.path.join(TEMP_VIDEO_DIR, f"{video_id}.mp4")
            
            # 1. Download
            download_success = False
            if os.path.exists(temp_video_path):
                 download_success = True
            else:
                 download_success = download_video(video_url, temp_video_path)
            
            if not download_success:
                # Cleanup empty dir
                try:
                    os.rmdir(video_frames_dir)
                except:
                    pass
                continue
            
            # 2. Extract Frames
            frame_count = extract_frames(temp_video_path, video_frames_dir, interval_sec=FRAME_INTERVAL_SEC)
            
            if frame_count > 0:
                processed_counts[blogger_id] += 1
                frames_extracted = True
                
            # 3. Cleanup Video (Optional: Keep it if you want to debug, but verify space)
            # For now, delete to save space
            if os.path.exists(temp_video_path):
                os.remove(temp_video_path)
        
        # 4. Analyze Frames (if frames exist)
        if frames_extracted:
            # Check if we already analyzed this video? (Optional optimization)
            video_results = analyze_frames(llm, local_ocr, video_id, blogger_name, video_frames_dir)
            if video_results:
                all_ocr_results.extend(video_results)
                # Save incrementally
                save_results(video_results)
            
    logger.info("Processing complete.")
    logger.info(f"Processed counts: {processed_counts}")
    
    if not all_ocr_results:
        logger.info("No relevant frames found so far. Checking if any frames were extracted at all.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Fatal error in main: {e}", exc_info=True)
