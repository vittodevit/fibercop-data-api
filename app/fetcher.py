import os
import asyncio
import re
import csv
import io
import aiohttp
import zipfile
import logging
from datetime import datetime, date
from typing import List, Dict, Optional, Union
from io import BytesIO
from .cache import cache
from .telegram import send_telegram_alert


ARCHIVE_URL = os.getenv("ARCHIVE_URL", "https://example.com/archive.zip")
CACHE_DIR = os.getenv("CACHE_DIR", "/app/data")
logger = logging.getLogger(__name__)


def get_cache_filename(date: Union[datetime, date]) -> str:
    """Generate cache filename for a given date"""
    return f"fibercop_{date.strftime('%Y-%m-%d')}.csv"


def load_csv_from_disk(date: datetime) -> Optional[tuple[bytes, str]]:
    """Load cached CSV from disk if available for given date"""
    cache_file = os.path.join(CACHE_DIR, get_cache_filename(date))
    if not os.path.exists(cache_file):
        logger.info(f"No cached CSV found on disk for file: {get_cache_filename(date)}")
        return None
    try:
        with open(cache_file, "rb") as f:
            csv_bytes = f.read()
        logger.info(f"Loaded cached CSV from disk: {cache_file}")
        return csv_bytes, cache_file
    except Exception as e:
        logger.error(f"Failed to load cached CSV: {e}")
        return None


def save_csv_to_disk(date: Union[datetime, date], csv_bytes: bytes):
    """Save CSV to disk cache and cleanup old files"""
    cache_file = os.path.join(CACHE_DIR, get_cache_filename(date))
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(cache_file, "wb") as f:
            f.write(csv_bytes)
        logger.info(f"Saved CSV to disk cache: {cache_file}")
        cleanup_old_cache(date)
    except Exception as e:
        logger.error(f"Failed to save CSV to disk: {e}")


def cleanup_old_cache(current_date: Union[datetime, date]):
    """Remove old cached files when new one is available"""
    if isinstance(current_date, date) and not isinstance(current_date, datetime):
        current_date = datetime.combine(current_date, datetime.min.time())

    try:
        if not os.path.exists(CACHE_DIR):
            return
        for filename in os.listdir(CACHE_DIR):
            file_path = os.path.join(CACHE_DIR, filename)
            if os.path.isfile(file_path) and filename.startswith("fibercop_"):
                cache_date_str = filename.replace("fibercop_", "").replace(".csv", "")
                try:
                    cache_date = datetime.strptime(cache_date_str, "%Y-%m-%d")
                    if cache_date < current_date:
                        os.remove(file_path)
                        logger.info(f"Removed old cache file: {filename}")
                except ValueError:
                    continue
    except Exception as e:
        logger.error(f"Failed to cleanup old cache: {e}")


async def fetch_and_parse_data():
    cache.set_fetching()

    try:
        today = datetime.now().date()
        cached_data = load_csv_from_disk(datetime(today.year, today.month, today.day))

        if cached_data:
            csv_bytes, cache_path = cached_data
            csv_filename = os.path.basename(cache_path)
            csv_content = csv_bytes.decode("utf-8")
            csv_reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")
            parsed_data = list(csv_reader)

            date_match = re.search(r"(\d{8})", csv_filename)
            if date_match:
                date_str = date_match.group(1)
                year = int(date_str[:4])
                month = int(date_str[4:6])
                day = int(date_str[6:8])
                latest_date = datetime(year, month, day)
            else:
                latest_date = datetime(today.year, today.month, today.day)

            cache.update(latest_date, parsed_data, csv_bytes, csv_filename)
            logger.info(
                f"Loaded from disk cache (date: {latest_date.date()}, records: {len(parsed_data)})"
            )
            return

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Ch-Ua": '"Not(A:Brand";v="8", "Chromium";v="144"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"macOS"',
        }

        async with aiohttp.ClientSession() as session:
            async with session.get(
                ARCHIVE_URL, headers=headers, allow_redirects=True
            ) as response:
                response.raise_for_status()
                zip_content = await response.read()

        zip_file = zipfile.ZipFile(BytesIO(zip_content))
        csv_filename = None
        csv_bytes = None

        for name in zip_file.namelist():
            if name.endswith(".csv"):
                date_match = re.search(r"(\d{8})", name)
                if date_match:
                    csv_filename = name
                    csv_bytes = zip_file.read(name)
                    date_str = date_match.group(1)
                    year = int(date_str[:4])
                    month = int(date_str[4:6])
                    day = int(date_str[6:8])
                    latest_date = datetime(year, month, day)
                    break

        if not csv_filename or not csv_bytes:
            raise ValueError("No CSV file with 8-digit date found in archive")

        csv_content = csv_bytes.decode("utf-8")
        csv_reader = csv.DictReader(io.StringIO(csv_content), delimiter=";")
        parsed_data = list(csv_reader)

        cache.update(latest_date, parsed_data, csv_bytes, csv_filename)
        save_csv_to_disk(today, csv_bytes)
        logger.info(
            f"Successfully fetched and parsed data (date: {latest_date.date()}, records: {len(parsed_data)})"
        )

    except Exception as e:
        error_msg = f"Failed to fetch/parse data: {str(e)}"
        logger.error(error_msg)
        cache.set_error(error_msg)
        await send_telegram_alert(error_msg)
