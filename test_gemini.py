
import os
import sys
sys.path.append(os.getcwd())
import config
from core.llm_processor import LLMProcessor

def test():
    print("Initializing LLM...")
    llm = LLMProcessor(api_key=config.GEMINI_API_KEY)
    
    # Test text generation
    print("Testing text generation...")
    try:
        response = llm.model.generate_content("Hello, are you working?")
        print(f"Response: {response.text}")
    except Exception as e:
        print(f"Text generation failed: {e}")
        
    # Test vision if image exists
    image_path = "data/keyframes/1166125372307144745/frame_0000.jpg"
    if os.path.exists(image_path):
        print(f"Testing vision with {image_path}...")
        try:
            result = llm.analyze_image(image_path)
            print(f"Vision Result: {result}")
        except Exception as e:
            print(f"Vision failed: {e}")
    else:
        print("Image not found for vision test")

if __name__ == "__main__":
    test()
