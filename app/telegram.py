import os
import aiohttp


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


async def send_telegram_alert(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[Telegram] Bot token or chat ID not set, skipping alert")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json={
                    "chat_id": TELEGRAM_CHAT_ID,
                    "text": f"Errore fetcher fibercop:\n{message}",
                },
            ) as response:
                response.raise_for_status()
                print("[Telegram] Alert sent successfully")
    except Exception as e:
        print(f"[Telegram] Failed to send alert: {str(e)}")
