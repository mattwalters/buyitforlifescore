import sys
import os
import asyncio
from google import genai
from google.genai import types

async def test_thinking(model_name):
    print(f"Testing model: {model_name}")
    client = genai.Client()
    prompt = "Hello"
    
    # Try thinking_level
    try:
        config = types.GenerateContentConfig(thinking_config=types.ThinkingConfig(thinking_level="low"))
        resp = await client.aio.models.generate_content(model=model_name, contents=prompt, config=config)
        print(f"  [{model_name}] thinking_level SUCCESS")
        return
    except Exception as e:
        print(f"  [{model_name}] thinking_level FAIL: {e}")

    # Try thinking_budget
    try:
        config = types.GenerateContentConfig(thinking_config=types.ThinkingConfig(thinking_budget=1024))
        resp = await client.aio.models.generate_content(model=model_name, contents=prompt, config=config)
        print(f"  [{model_name}] thinking_budget SUCCESS")
    except Exception as e:
        print(f"  [{model_name}] thinking_budget FAIL: {e}")

async def main():
    models = ["gemini-2.5-flash-lite", "gemini-3.1-flash-lite-preview", "gemini-2.5-flash", "gemini-3-flash-preview"]
    for m in models:
        await test_thinking(m)

asyncio.run(main())
