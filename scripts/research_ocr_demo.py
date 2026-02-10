import cv2
import os
import sys

# 尝试导入 OCR 库
try:
    import easyocr
    HAS_EASYOCR = True
except ImportError:
    HAS_EASYOCR = False
    print("Warning: easyocr module not found. Please install it using: pip install easyocr")

try:
    import pytesseract
    HAS_PYTESSERACT = True
except ImportError:
    HAS_PYTESSERACT = False
    print("Warning: pytesseract module not found. Please install it using: pip install pytesseract")

def extract_frames(video_path, interval=1):
    """
    每隔 interval 秒提取一帧
    """
    if not os.path.exists(video_path):
        print(f"Video not found: {video_path}")
        return []

    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS)
    frames = []
    
    count = 0
    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            break
            
        if count % int(fps * interval) == 0:
            frames.append(frame)
        
        count += 1
        
    cap.release()
    print(f"Extracted {len(frames)} frames from {video_path}")
    return frames

def ocr_frame(frame):
    """
    对单帧进行 OCR 识别
    """
    text = ""
    if HAS_EASYOCR:
        reader = easyocr.Reader(['ch_sim', 'en'])
        result = reader.readtext(frame)
        for bbox, t, conf in result:
            text += t + " "
    elif HAS_PYTESSERACT:
        # 需要安装 Tesseract-OCR 引擎
        text = pytesseract.image_to_string(frame, lang='chi_sim')
        
    return text

if __name__ == "__main__":
    print("OCR Research Demo")
    print("Usage: python scripts/research_ocr_demo.py <video_path>")
    # 示例代码，待完善
