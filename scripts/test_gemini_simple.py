import google.generativeai as genai
import os
import sys

api_key = os.environ.get("GEMINI_API_KEY")
print(f"Testing with key: {api_key[:5]}...")

try:
    genai.configure(api_key=api_key)
    model = genai.GenerativeModel('gemini-pro')
    response = model.generate_content("Hello, are you there?")
    print(f"Response: {response.text}")
except Exception as e:
    print(f"Error: {e}")
