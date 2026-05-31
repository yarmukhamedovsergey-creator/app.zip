import asyncio
from telethon import TelegramClient

API_ID = 2040          # твой api_id
API_HASH = 'b18441a1ff607e10a989891a5462e627'

async def main():
    phone = input("Введите номер телефона в формате +7XXXXXXXXXX: ").strip()
    client = TelegramClient(f"sessions/{phone}", API_ID, API_HASH)
    await client.start(phone)
    print(f"✅ Сессия создана: sessions/{phone}.session")
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
