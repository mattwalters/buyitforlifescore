import asyncio
from google import genai
from google.genai import types

async def main():
    client = genai.Client()
    response = await client.aio.models.generate_content(
        model="gemini-2.5-pro",
        contents="Think step by step and tell me if 29 is prime.",
        config=types.GenerateContentConfig(
            thinking_config=types.ThinkingConfig(thinking_budget_tokens=1024)
        )
    )
    print(response.usage_metadata)
    
asyncio.run(main())
