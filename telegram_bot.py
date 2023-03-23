import telegram
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("token")
CHAT_ID = os.getenv("chat_id")

async def main(): #실행시킬 함수명 임의지정
    bot = telegram.Bot(token = TOKEN)
    await bot.send_message(CHAT_ID, "MESSAGE")

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main()) #봇 실행하는 코드