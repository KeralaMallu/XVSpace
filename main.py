import os
import re
import math
import time
import json
import base64
import signal
import asyncio
import logging
import aiohttp
import hashlib
import shutil
import urllib.parse
import sys
import psutil
from pathlib import Path
from datetime import datetime, timedelta
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import web, ClientConnectionError, ClientTimeout
from dotenv import load_dotenv
from pyrogram import Client, filters, enums
from pyrogram.errors import FloodWait, UserNotParticipant, AuthBytesInvalid, PeerIdInvalid, LimitInvalid, Timeout, FileReferenceExpired
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from pyrogram.session import Session, Auth
from pyrogram.file_id import FileId, FileType
from pyrogram import raw
from pyrogram.raw.types import InputPhotoFileLocation, InputDocumentFileLocation
from collections import OrderedDict

# ============================================================================ #
# KeralaCaptain Bot - FIXED Production Streaming Engine V6.0
# Fixes: Variable chunk bug, cache headers, adds disk caching
# ============================================================================ #

load_dotenv()

logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s] - %(message)s')
LOGGER = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)

start_time = time.time()

# ============================================================================ #
# CONFIGURATION
# ============================================================================ #

class Config:
    API_ID = int(os.environ.get("API_ID", 0))
    API_HASH = os.environ.get("API_HASH", "")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
    
    ADMIN_IDS = list(int(admin_id) for admin_id in os.environ.get("ADMIN_IDS", "6644681404").split())
    PROTECTED_DOMAIN = os.environ.get("PROTECTED_DOMAIN", "https://www.keralacaptain.shop/").rstrip('/') + '/'
    
    MONGO_URI = os.environ.get("MONGO_URI", "")
    LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", 0))
    STREAM_URL = os.environ.get("STREAM_URL", "").rstrip('/')
    PORT = int(os.environ.get("PORT", 8080))
    
    PING_INTERVAL = int(os.environ.get("PING_INTERVAL", 1200))
    ON_HEROKU = 'DYNO' in os.environ
    
    # Disk cache settings
    CACHE_DIR = Path("./cache")
    MAX_CACHE_SIZE_GB = 25  # Use up to 25GB for caching
    CACHE_CLEANUP_INTERVAL = 1800  # Clean every 30 minutes

required_vars = [
    Config.API_ID, Config.API_HASH, Config.BOT_TOKEN,
    Config.MONGO_URI, Config.LOG_CHANNEL_ID, Config.STREAM_URL,
    Config.ADMIN_IDS
]
if not all(required_vars) or Config.ADMIN_IDS == [0]:
    LOGGER.critical("FATAL: Required variables missing. Cannot start.")
    exit(1)

CURRENT_PROTECTED_DOMAIN = Config.PROTECTED_DOMAIN

# Create cache directory
Config.CACHE_DIR.mkdir(exist_ok=True)

# ============================================================================ #
# FIXED CONSTANTS - NO VARIABLE CHUNK SIZES
# ============================================================================ #

# CRITICAL FIX: Use CONSTANT chunk size - no adaptive/variable sizing
FIXED_CHUNK_SIZE = 1024 * 1024  # Always 1MB - NEVER CHANGE THIS

# Telegram API limit is 512KB-1MB, we use 1MB as safe maximum
TELEGRAM_CHUNK_LIMIT = 1024 * 1024  # 1MB - Telegram's max limit

CACHE_TTL = 300
SESSION_CLEANUP_INTERVAL = 1200
MAX_MEMORY_CACHE = 200  # Reduced - we're using disk cache now

# ============================================================================ #
# SECURITY: BLOCKED AGENTS
# ============================================================================ #

BLOCKED_AGENTS = [
    # Download Managers
    "idm", "internet download manager", "adm", "advanced download manager",
    "fdm", "free download manager", "download master", "eagleget",
    "jdownloader", "flashget", "getright", "mass downloader",
    
    # Crawlers/Bots/Scrapers
    "wget", "curl", "python-requests", "go-http-client", "java",
    "bot", "crawler", "spider", "scraper", "bytespider", "headlesschrome",
    
    # Force Download Browsers
    "solo browser", "uc browser", "opera mini download", "download browser",
    
    # Video Downloaders
    "video download", "videoder", "tubemate", "snaptube", "vidmate"
]

def is_blocked_agent(user_agent):
    """Enhanced agent blocking"""
    if not user_agent:
        return False  # Allow missing user-agent (some mobile players)
    ua = user_agent.lower()
    for blocked in BLOCKED_AGENTS:
        if blocked in ua:
            return True
    return False

def is_download_request(request):
    """Detect forced downloads"""
    accept = request.headers.get('Accept', '').lower()
    
    # Allow common video player accept headers
    if 'video/' in accept or '*/*' in accept or 'text/html' in accept:
        return False
    
    # Block suspicious patterns
    connection = request.headers.get('Connection', '').lower()
    if connection == 'close' and 'video/' not in accept:
        return True
    
    return False

# ============================================================================ #
# HELPER FUNCTIONS
# ============================================================================ #

async def encode(string: str) -> str:
    string_bytes = string.encode("ascii")
    base64_bytes = base64.urlsafe_b64encode(string_bytes)
    return (base64_bytes.decode("ascii")).strip("=")

async def decode(base64_string: str) -> str:
    base64_string = base64_string.strip("=")
    base64_bytes = (base64_string + "=" * (-len(base64_string) % 4)).encode("ascii")
    string_bytes = base64.urlsafe_b64decode(base64_bytes)
    return string_bytes.decode("ascii")

def humanbytes(size):
    if not size: return "0 B"
    power = 1024
    n = 0
    power_labels = {0: ' ', 1: 'K', 2: 'M', 3: 'G', 4: 'T'}
    while size > power:
        size /= power
        n += 1
    return f"{round(size, 2)} {power_labels[n]}B"

def get_readable_time(seconds: int) -> str:
    result = ""
    (days, remainder) = divmod(seconds, 86400)
    days = int(days)
    if days != 0:
        result += f"{days}d "
    (hours, remainder) = divmod(remainder, 3600)
    hours = int(hours)
    if hours != 0:
        result += f"{hours}h "
    (minutes, seconds) = divmod(remainder, 60)
    minutes = int(minutes)
    if minutes != 0:
        result += f"{minutes}m "
    seconds = int(seconds)
    result += f"{seconds}s"
    return result

def get_cache_path(message_id: int, offset: int) -> Path:
    """Generate cache file path for a specific chunk"""
    return Config.CACHE_DIR / f"{message_id}_{offset}.chunk"

def get_cache_size() -> int:
    """Get total cache directory size in bytes"""
    total = 0
    for file in Config.CACHE_DIR.glob("*.chunk"):
        try:
            total += file.stat().st_size
        except:
            pass
    return total

async def cleanup_old_cache():
    """Remove oldest cache files if size exceeds limit"""
    max_size = Config.MAX_CACHE_SIZE_GB * 1024 * 1024 * 1024  # Convert to bytes
    current_size = get_cache_size()
    
    if current_size <= max_size:
        return
    
    LOGGER.info(f"Cache size {humanbytes(current_size)} exceeds limit. Cleaning...")
    
    # Get all cache files sorted by access time (oldest first)
    cache_files = sorted(
        Config.CACHE_DIR.glob("*.chunk"),
        key=lambda p: p.stat().st_atime
    )
    
    # Remove oldest files until we're under the limit
    for file in cache_files:
        try:
            size = file.stat().st_size
            file.unlink()
            current_size -= size
            LOGGER.debug(f"Removed cached chunk: {file.name}")
            
            if current_size <= max_size * 0.8:  # Target 80% of max
                break
        except Exception as e:
            LOGGER.warning(f"Failed to remove cache file {file}: {e}")
    
    LOGGER.info(f"Cache cleaned. New size: {humanbytes(get_cache_size())}")

async def periodic_cache_cleanup():
    """Background task to clean cache periodically"""
    while True:
        await asyncio.sleep(Config.CACHE_CLEANUP_INTERVAL)
        try:
            await cleanup_old_cache()
        except Exception as e:
            LOGGER.error(f"Cache cleanup error: {e}")

# ============================================================================ #
# DATABASE OPERATIONS
# ============================================================================ #

db_client = AsyncIOMotorClient(Config.MONGO_URI)
db = db_client['KeralaCaptainBotDB']

media_collection = db['media']
media_backup_collection = db['media_backup']
user_conversations_col = db['conversations']
settings_collection = db['settings']

async def check_duplicate(tmdb_id):
    return await media_collection.find_one({"tmdb_id": tmdb_id})

async def add_media_to_db(data):
    await media_collection.insert_one(data)
    await media_backup_collection.insert_one(data)

async def get_media_by_post_id(post_id: int):
    return await media_collection.find_one({"wp_post_id": post_id})

async def update_media_links_in_db(post_id: int, new_message_ids: dict, new_stream_link: str):
    update_query = {
        "$set": {"message_ids": new_message_ids, "stream_link": new_stream_link}
    }
    await media_collection.update_one({"wp_post_id": post_id}, update_query)
    await media_backup_collection.update_one({"wp_post_id": post_id}, update_query)

async def delete_media_from_db(post_id: int):
    result_main = await media_collection.delete_one({"wp_post_id": post_id})
    await media_backup_collection.delete_one({"wp_post_id": post_id})
    return result_main

async def get_stats():
    movies_count = await media_collection.count_documents({"type": "movie"})
    series_count = await media_collection.count_documents({"type": "series"})
    return movies_count, series_count

async def get_all_media_for_library(page: int = 0, limit: int = 10):
    cursor = media_collection.find().sort("added_at", -1).skip(page * limit).limit(limit)
    return await cursor.to_list(length=limit)

async def get_user_conversation(chat_id):
    return await user_conversations_col.find_one({"_id": chat_id})

async def update_user_conversation(chat_id, data):
    if data:
        await user_conversations_col.update_one({"_id": chat_id}, {"$set": data}, upsert=True)
    else:
        await user_conversations_col.delete_one({"_id": chat_id})

async def get_post_id_from_msg_id(msg_id: int):
    doc = await media_collection.find_one({"message_ids": {"$in": [msg_id]}})
    return doc['wp_post_id'] if doc else None

async def get_protected_domain() -> str:
    try:
        doc = await settings_collection.find_one({"_id": "bot_settings"})
        if doc and "protected_domain" in doc:
            return doc["protected_domain"]
    except Exception as e:
        LOGGER.error(f"Could not fetch domain from DB: {e}", exc_info=True)
    return Config.PROTECTED_DOMAIN

async def set_protected_domain(new_domain: str):
    global CURRENT_PROTECTED_DOMAIN
    if not (new_domain.startswith("https://") or new_domain.startswith("http://")):
        new_domain = "https://" + new_domain
    if not new_domain.endswith('/'):
        new_domain += '/'
        
    await settings_collection.update_one(
        {"_id": "bot_settings"},
        {"$set": {"protected_domain": new_domain}},
        upsert=True
    )
    CURRENT_PROTECTED_DOMAIN = new_domain
    LOGGER.info(f"Protected domain updated: {new_domain}")
    return new_domain

# ============================================================================ #
# LRU CACHE (for file properties only)
# ============================================================================ #

class LRUCache:
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity
        self.lock = asyncio.Lock()
    
    async def get(self, key):
        async with self.lock:
            if key not in self.cache:
                return None
            self.cache.move_to_end(key)
            return self.cache[key]
    
    async def put(self, key, value):
        async with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)

# ============================================================================ #
# FIXED BYTE STREAMER WITH DISK CACHE
# ============================================================================ #

multi_clients = {}
work_loads = {}
class_cache = {}
next_client_idx = 0
stream_errors = 0
last_error_reset = time.time()

class ByteStreamer:
    def __init__(self, client: Client):
        self.client: Client = client
        self.cached_file_ids = LRUCache(MAX_MEMORY_CACHE)
        self.session_cache = {}
        asyncio.create_task(self.clean_stale_sessions())

    async def clean_stale_sessions(self):
        """Clean stale sessions periodically"""
        while True:
            await asyncio.sleep(SESSION_CLEANUP_INTERVAL)
            current_time = time.time()
            stale = [
                dc_id for dc_id, (_, ts) in self.session_cache.items()
                if current_time - ts > CACHE_TTL * 3
            ]
            for dc_id in stale:
                try:
                    session, _ = self.session_cache[dc_id]
                    await session.stop()
                except:
                    pass
                del self.session_cache[dc_id]
            
            if stale:
                LOGGER.info(f"Cleaned {len(stale)} stale sessions")

    async def get_file_properties(self, message_id: int):
        cached = await self.cached_file_ids.get(message_id)
        if cached:
            return cached

        message = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
        if not message or message.empty or not (message.document or message.video):
            raise FileNotFoundError(f"Message {message_id} has no media")

        media = message.document or message.video
        file_id = FileId.decode(media.file_id)
        setattr(file_id, "file_size", media.file_size or 0)
        setattr(file_id, "mime_type", media.mime_type or "video/mp4")
        setattr(file_id, "file_name", media.file_name or "Unknown.mp4")

        await self.cached_file_ids.put(message_id, file_id)
        return file_id

    async def generate_media_session(self, file_id: FileId) -> Session:
        media_session = self.client.media_sessions.get(file_id.dc_id)
        dc_id = file_id.dc_id

        if dc_id in self.session_cache:
            session, ts = self.session_cache[dc_id]
            if time.time() - ts < CACHE_TTL:
                return session

        if media_session:
            try:
                await media_session.send(raw.functions.help.GetConfig(), timeout=10)
                self.session_cache[dc_id] = (media_session, time.time())
                return media_session
            except Exception as e:
                LOGGER.warning(f"Stale session DC {dc_id}, recreating")
                try:
                    await media_session.stop()
                except:
                    pass
                if dc_id in self.client.media_sessions:
                    del self.client.media_sessions[dc_id]
                media_session = None

        LOGGER.info(f"Creating media session for DC {dc_id}")
        if dc_id != await self.client.storage.dc_id():
            media_session = Session(
                self.client, dc_id,
                await Auth(self.client, dc_id, await self.client.storage.test_mode()).create(),
                await self.client.storage.test_mode(),
                is_media=True
            )
            await media_session.start()
            for i in range(3):
                try:
                    exported_auth = await self.client.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc_id))
                    await media_session.send(raw.functions.auth.ImportAuthorization(id=exported_auth.id, bytes=exported_auth.bytes))
                    break
                except AuthBytesInvalid as e:
                    LOGGER.warning(f"AuthBytesInvalid attempt {i+1}")
                    if i == 2:
                        raise
                    await asyncio.sleep(1)
        else:
            media_session = Session(
                self.client, dc_id,
                await self.client.storage.auth_key(),
                await self.client.storage.test_mode(),
                is_media=True
            )
            await media_session.start()

        self.client.media_sessions[dc_id] = media_session
        self.session_cache[dc_id] = (media_session, time.time())
        return media_session

    @staticmethod
    def get_location(file_id: FileId):
        if file_id.file_type == FileType.PHOTO:
            return InputPhotoFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size
            )
        else:
            return InputDocumentFileLocation(
                id=file_id.media_id,
                access_hash=file_id.access_hash,
                file_reference=file_id.file_reference,
                thumb_size=file_id.thumbnail_size
            )

    async def yield_file(self, file_id: FileId, offset: int, message_id: int):
        """
        CRITICAL FIX: 
        - Use FIXED 1MB chunks (no variable sizing)
        - Implement disk caching
        - Proper stopping conditions
        """
        media_session = await self.generate_media_session(file_id)
        location = self.get_location(file_id)

        current_offset = offset
        total_file_size = getattr(file_id, "file_size", 0)
        
        retry_count = 0
        max_retries = 3

        while True:
            # Calculate bytes remaining
            if total_file_size > 0:
                bytes_remaining = total_file_size - current_offset
                if bytes_remaining <= 0:
                    LOGGER.debug(f"[{message_id}] Reached end of file")
                    break
                
                # FIXED: Use constant chunk size, but don't exceed file size
                request_limit = min(FIXED_CHUNK_SIZE, bytes_remaining)
            else:
                request_limit = FIXED_CHUNK_SIZE

            # Check disk cache first
            cache_path = get_cache_path(message_id, current_offset)
            
            if cache_path.exists():
                # Serve from disk cache (ZERO bandwidth!)
                try:
                    with open(cache_path, 'rb') as f:
                        chunk_bytes = f.read()
                    
                    if chunk_bytes:
                        LOGGER.debug(f"[{message_id}] Serving from cache: offset {current_offset}")
                        yield chunk_bytes
                        current_offset += len(chunk_bytes)
                        
                        # Update access time for LRU cleanup
                        cache_path.touch()
                        continue
                except Exception as e:
                    LOGGER.warning(f"Cache read error: {e}, fetching from Telegram")
                    # Fall through to Telegram fetch

            # Fetch from Telegram
            try:
                chunk = await media_session.send(
                    raw.functions.upload.GetFile(
                        location=location,
                        offset=current_offset,
                        limit=request_limit  # Always 1MB or less
                    ),
                    timeout=30
                )
                
                if isinstance(chunk, raw.types.upload.File) and chunk.bytes:
                    chunk_bytes = chunk.bytes
                    chunk_len = len(chunk_bytes)
                    
                    # Save to disk cache asynchronously
                    try:
                        cache_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(cache_path, 'wb') as f:
                            f.write(chunk_bytes)
                        LOGGER.debug(f"[{message_id}] Cached chunk at offset {current_offset}")
                    except Exception as e:
                        LOGGER.warning(f"Cache write error: {e}")
                    
                    # Yield to client
                    yield chunk_bytes
                    current_offset += chunk_len
                    
                    # FIXED STOPPING CONDITION: Check if we got less than requested
                    # This means we've reached the end
                    if chunk_len < request_limit:
                        LOGGER.debug(f"[{message_id}] Received partial chunk, end of file")
                        break
                        
                else:
                    # No more data
                    break

            except FileReferenceExpired:
                retry_count += 1
                if retry_count > max_retries:
                    LOGGER.error(f"[{message_id}] Max retries exceeded")
                    raise
                
                LOGGER.warning(f"[{message_id}] FileReferenceExpired, refreshing...")
                
                original_msg = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
                if original_msg:
                    refreshed_msg = await forward_file_safely(original_msg)
                    if refreshed_msg:
                        new_file_id = await self.get_file_properties(refreshed_msg.id)
                        total_file_size = getattr(new_file_id, "file_size", total_file_size)
                        await self.cached_file_ids.put(message_id, new_file_id)
                        
                        post_id = await get_post_id_from_msg_id(message_id)
                        if post_id:
                            media_doc = await get_media_by_post_id(post_id)
                            if media_doc:
                                old_qualities = media_doc['message_ids']
                                quality_key = next((k for k, v in old_qualities.items() if v == message_id), None)
                                new_qualities = old_qualities.copy()
                                if quality_key:
                                    new_qualities[quality_key] = refreshed_msg.id
                                else:
                                    new_qualities = {k: refreshed_msg.id if v == message_id else v for k, v in old_qualities.items()}
                                await update_media_links_in_db(post_id, new_qualities, media_doc['stream_link'])
                        
                        # Clear cache for this file (reference is stale)
                        for old_cache in Config.CACHE_DIR.glob(f"{message_id}_*.chunk"):
                            try:
                                old_cache.unlink()
                            except:
                                pass
                        
                        location = self.get_location(new_file_id)
                        await asyncio.sleep(2)
                        continue
                raise

            except FloodWait as e:
                LOGGER.warning(f"FloodWait {e.value}s")
                await asyncio.sleep(e.value)
                continue
            
            except LimitInvalid:
                # This should never happen with FIXED_CHUNK_SIZE, but just in case
                LOGGER.error(f"[{message_id}] LimitInvalid error - chunk size issue")
                raise

# ============================================================================ #
# WEB SERVER ROUTES
# ============================================================================ #

routes = web.RouteTableDef()

@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.Response(text="KeralaCaptain Streaming Service V6.0", content_type='text/html')

@routes.get("/health")
async def health_handler(request):
    global stream_errors, last_error_reset
    if time.time() - last_error_reset > 60:
        stream_errors = 0
        last_error_reset = time.time()
    
    cache_size = get_cache_size()
    
    return web.json_response({
        "status": "ok",
        "active_clients": len(multi_clients),
        "stream_errors_last_min": stream_errors,
        "workloads": work_loads,
        "disk_cache_size": humanbytes(cache_size),
        "disk_cache_files": len(list(Config.CACHE_DIR.glob("*.chunk")))
    })

@routes.get("/favicon.ico")
async def favicon_handler(request):
    return web.Response(status=204)

@routes.get(r"/stream/{message_id:\d+}")
async def stream_handler(request: web.Request):
    client_index = None
    global stream_errors
    
    try:
        # ===== SECURITY LAYER 1: USER AGENT =====
        user_agent = request.headers.get('User-Agent', '')
        if is_blocked_agent(user_agent):
            LOGGER.warning(f"BLOCKED Agent: {user_agent[:100]}")
            return web.Response(status=403, text="Access denied")

        # ===== SECURITY LAYER 2: DOWNLOAD DETECTION =====
        if is_download_request(request):
            LOGGER.warning(f"BLOCKED Download: {user_agent[:100]}")
            return web.Response(status=403, text="Downloads not allowed")

        # ===== SECURITY LAYER 3: REFERER CHECK (Relaxed for mobile) =====
        referer = request.headers.get('Referer', '')
        allowed_domain = CURRENT_PROTECTED_DOMAIN.rstrip('/')
        
        # FIXED: Allow requests with no referer (mobile in-app browsers)
        # But if referer exists, it must match our domain
        if referer and allowed_domain not in referer:
            LOGGER.warning(f"BLOCKED - Invalid referer: {referer}")
            return web.Response(status=403, text="Access denied")

        message_id = int(request.match_info['message_id'])
        range_header = request.headers.get("Range", 0)

        # ===== LOAD BALANCING =====
        min_load = min(work_loads.values())
        candidates = [cid for cid, load in work_loads.items() if load == min_load]
        global next_client_idx
        client_index = candidates[next_client_idx % len(candidates)]
        next_client_idx += 1
        
        selected_client = multi_clients[client_index]
        work_loads[client_index] += 1

        if selected_client not in class_cache:
            class_cache[selected_client] = ByteStreamer(selected_client)
        tg_connect = class_cache[selected_client]

        # ===== GET FILE INFO =====
        file_id = await tg_connect.get_file_properties(message_id)
        file_size = file_id.file_size

        # ===== HANDLE RANGE REQUESTS =====
        from_bytes = 0
        if range_header:
            try:
                from_bytes_str = range_header.replace("bytes=", "").split("-")[0]
                from_bytes = int(from_bytes_str)
            except ValueError:
                from_bytes = 0

        if from_bytes >= file_size:
            return web.Response(status=416, reason="Range Not Satisfiable")

        # ===== FIXED HEADERS (Allow buffering) =====
        headers = {
            "Content-Type": file_id.mime_type,
            "Accept-Ranges": "bytes",
            "Content-Range": f"bytes {from_bytes}-{file_size - 1}/{file_size}",
            "Content-Length": str(file_size - from_bytes),
            "Content-Disposition": "inline",  # Force streaming
            "X-Content-Type-Options": "nosniff",
            "Access-Control-Allow-Origin": "*",  # Allow all origins for CORS
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, User-Agent",
            # CRITICAL FIX: Allow browser caching for smooth buffering
            "Cache-Control": "private, max-age=3600",
            "Pragma": "public"  # Enable caching
        }

        resp = web.StreamResponse(
            status=206 if range_header else 200,
            headers=headers
        )
        await resp.prepare(request)

        # ===== CALCULATE OFFSET (aligned to chunk size) =====
        offset = from_bytes - (from_bytes % FIXED_CHUNK_SIZE)
        first_part_cut = from_bytes - offset

        # ===== START STREAMING =====
        body_generator = tg_connect.yield_file(file_id, offset, message_id)
        is_first_chunk = True

        async for chunk in body_generator:
            try:
                if is_first_chunk and first_part_cut > 0:
                    await resp.write(chunk[first_part_cut:])
                    is_first_chunk = False
                else:
                    await resp.write(chunk)
                
                # Drain periodically to detect disconnects
                await resp.drain()
                
            except (ConnectionError, asyncio.CancelledError, ClientConnectionError, OSError) as e:
                LOGGER.info(f"[{message_id}] User disconnected: {type(e).__name__}")
                return resp
            except Exception as e:
                LOGGER.error(f"[{message_id}] Write error: {e}")
                return resp

        return resp

    except (FileReferenceExpired, AuthBytesInvalid) as e:
        stream_errors += 1
        LOGGER.error(f"[{message_id}] Stream expired: {type(e).__name__}")
        return web.Response(status=410, text="Stream expired. Refresh page.")

    except FileNotFoundError:
        LOGGER.error(f"[{message_id}] File not found")
        return web.Response(status=404, text="Media not found")

    except Exception as e:
        stream_errors += 1
        LOGGER.critical(f"[{message_id}] Unhandled error: {e}", exc_info=True)
        return web.Response(status=500, text="Internal error")

    finally:
        if client_index is not None:
            work_loads[client_index] = max(0, work_loads[client_index] - 1)

async def web_server():
    web_app = web.Application(client_max_size=30_000_000)
    web_app.add_routes(routes)
    return web_app

# ============================================================================ #
# BOT & CLIENT INITIALIZATION
# ============================================================================ #

main_bot = Client(
    "KeralaCaptainBot",
    api_id=Config.API_ID,
    api_hash=Config.API_HASH,
    bot_token=Config.BOT_TOKEN
)

class TokenParser:
    def parse_from_env(self):
        return {
            c + 2: t for c, (_, t) in enumerate(
                filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items()))
            )
        }

async def initialize_clients():
    multi_clients[0] = main_bot
    work_loads[0] = 0
    
    all_tokens = TokenParser().parse_from_env()
    if not all_tokens:
        LOGGER.info("Single-client mode")
        return

    async def start_client(client_id, token):
        try:
            client = await Client(
                name=str(client_id),
                api_id=Config.API_ID,
                api_hash=Config.API_HASH,
                bot_token=token,
                no_updates=True,
                in_memory=True
            ).start()
            work_loads[client_id] = 0
            return client_id, client
        except Exception as e:
            LOGGER.error(f"Failed to start client {client_id}: {e}")
            return None

    clients = await asyncio.gather(*[start_client(i, token) for i, token in all_tokens.items()])
    multi_clients.update({cid: client for cid, client in clients if client is not None})
    
    if len(multi_clients) > 1:
        LOGGER.info(f"Multi-client mode: {len(multi_clients)} clients")

async def forward_file_safely(message_to_forward: Message):
    """Refresh expired file reference"""
    try:
        media = message_to_forward.document or message_to_forward.video
        if not media:
            return None
            
        file_id = media.file_id
        return await main_bot.send_cached_media(
            chat_id=Config.LOG_CHANNEL_ID,
            file_id=file_id,
            caption=getattr(message_to_forward, 'caption', '')
        )
    except Exception as e:
        LOGGER.error(f"Failed to forward media: {e}", exc_info=True)
        return None

# ============================================================================ #
# BOT HANDLERS (ADMIN ONLY)
# ============================================================================ #

admin_only = filters.user(Config.ADMIN_IDS)

@main_bot.on_message(filters.command("start") & filters.private & admin_only)
async def start_command(client, message):
    await message.reply_text(
        "**👋 Admin Control Panel**\n\n"
        "Bot Version: 6.0 (FIXED)\n"
        "What would you like to do?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Statistics", callback_data="admin_stats")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings")],
            [InlineKeyboardButton("🗑️ Clear Cache", callback_data="admin_clear_cache")],
            [InlineKeyboardButton("🔄 Restart", callback_data="admin_restart")]
        ])
    )
    await update_user_conversation(message.chat.id, None)

@main_bot.on_callback_query(filters.regex("^admin_stats$") & admin_only)
async def stats_callback(client, cb: CallbackQuery):
    await cb.answer("Fetching...")
    
    uptime = get_readable_time(time.time() - start_time)
    
    try:
        cpu = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
    except:
        cpu = ram = disk = None

    cache_size = get_cache_size()
    cache_files = len(list(Config.CACHE_DIR.glob("*.chunk")))
    
    movies, series = await get_stats()

    if cpu:
        text = (
            f"**📊 Bot Statistics**\n\n"
            f"**⏱ Uptime:** `{uptime}`\n\n"
            f"**💾 System:**\n"
            f"  • CPU: `{cpu}%`\n"
            f"  • RAM: `{ram.percent}%` ({humanbytes(ram.used)}/{humanbytes(ram.total)})\n"
            f"  • Disk: `{disk.percent}%` ({humanbytes(disk.used)}/{humanbytes(disk.total)})\n\n"
            f"**💿 Disk Cache:**\n"
            f"  • Size: `{humanbytes(cache_size)}`\n"
            f"  • Files: `{cache_files}` chunks\n"
            f"  • Limit: `{Config.MAX_CACHE_SIZE_GB}GB`\n\n"
            f"**📡 Streaming:**\n"
            f"  • Clients: `{len(multi_clients)}`\n"
            f"  • Errors: `{stream_errors}`\n\n"
            f"**📚 Database:**\n"
            f"  • Movies: `{movies}`\n"
            f"  • Series: `{series}`"
        )
    else:
        text = f"**📊 Statistics**\n\nUptime: `{uptime}`\nCache: `{humanbytes(cache_size)}`"
           
    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="admin_stats")],
            [InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]
        ])
    )

@main_bot.on_callback_query(filters.regex("^admin_settings$") & admin_only)
async def settings_callback(client, cb: CallbackQuery):
    await cb.answer()
    current_domain = await get_protected_domain()
    
    text = (
        f"**⚙️ Settings**\n\n"
        f"**Protected Domain:**\n`{current_domain}`\n\n"
        f"**Security:**\n"
        f"✅ Agent blocking\n"
        f"✅ Download prevention\n"
        f"✅ Referer validation\n"
        f"✅ Fixed 1MB chunks\n"
        f"✅ Disk caching enabled"
    )
           
    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Change Domain", callback_data="admin_set_domain")],
            [InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]
        ])
    )

@main_bot.on_callback_query(filters.regex("^admin_clear_cache$") & admin_only)
async def clear_cache_callback(client, cb: CallbackQuery):
    await cb.answer("Clearing cache...")
    
    try:
        count = 0
        for file in Config.CACHE_DIR.glob("*.chunk"):
            file.unlink()
            count += 1
        
        await cb.message.edit_text(
            f"✅ **Cache Cleared**\n\n"
            f"Removed `{count}` cached chunks.\n"
            f"New cache size: `{humanbytes(get_cache_size())}`",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]
            ])
        )
    except Exception as e:
        await cb.message.edit_text(f"❌ Error: {e}")

@main_bot.on_callback_query(filters.regex("^admin_set_domain$") & admin_only)
async def set_domain_callback(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_domain"})
    await cb.message.edit_text(
        "**✏️ Set Protected Domain**\n\n"
        "Send new domain:\n"
        "Example: `keralacaptain.shop`",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="admin_cancel_conv")]
        ])
    )

@main_bot.on_callback_query(filters.regex("^admin_restart$") & admin_only)
async def restart_callback(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**⚠️ Restart?**\n\n"
        "All active streams will disconnect.",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes", callback_data="admin_restart_confirm"),
                InlineKeyboardButton("❌ No", callback_data="admin_main_menu")
            ]
        ])
    )

@main_bot.on_callback_query(filters.regex("^admin_restart_confirm$") & admin_only)
async def restart_confirm_callback(client, cb: CallbackQuery):
    await cb.answer("Restarting...")
    await cb.message.edit_text("**🔄 Restarting...**")
    
    try:
        if main_bot and main_bot.is_connected:
            await main_bot.stop()
        await asyncio.sleep(1)
    except:
        pass
    
    os.execl(sys.executable, sys.executable, *sys.argv)

@main_bot.on_callback_query(filters.regex("^(admin_main_menu|admin_cancel_conv)$") & admin_only)
async def main_menu_callback(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, None)
    await cb.message.edit_text(
        "**👋 Admin Control Panel**\n\n"
        "Bot Version: 6.0 (FIXED)\n"
        "What would you like to do?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Statistics", callback_data="admin_stats")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings")],
            [InlineKeyboardButton("🗑️ Clear Cache", callback_data="admin_clear_cache")],
            [InlineKeyboardButton("🔄 Restart", callback_data="admin_restart")]
        ])
    )

@main_bot.on_message(filters.private & filters.text & admin_only)
async def text_message_handler(client, message: Message):
    chat_id = message.chat.id
    conv = await get_user_conversation(chat_id)
    if not conv:
        return

    stage = conv.get("stage")
    
    if stage == "awaiting_domain":
        new_domain = message.text.strip()
        
        if not new_domain or " " in new_domain:
            return await message.reply_text("❌ Invalid format")
        
        new_domain = new_domain.replace("http://", "").replace("https://", "")
        
        if "." not in new_domain:
            return await message.reply_text("❌ Invalid domain")
        
        try:
            status = await message.reply_text("⏳ Saving...")
            saved = await set_protected_domain(new_domain)
            
            await status.edit_text(
                f"✅ **Updated**\n\nNew domain:\n`{saved}`",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Back", callback_data="admin_settings")]
                ])
            )
            await update_user_conversation(chat_id, None)
            
        except Exception as e:
            await status.edit_text(f"❌ Error: {e}")

# ============================================================================ #
# APPLICATION LIFECYCLE
# ============================================================================ #

async def ping_server():
    """Keep server alive"""
    while True:
        await asyncio.sleep(Config.PING_INTERVAL)
        try:
            async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
                async with session.get(Config.STREAM_URL) as resp:
                    LOGGER.info(f"Ping: {resp.status}")
        except Exception as e:
            LOGGER.warning(f"Ping failed: {e}")

if __name__ == "__main__":
    async def main_startup_shutdown_logic():
        """Startup and shutdown"""
        global CURRENT_PROTECTED_DOMAIN
        
        LOGGER.info("=" * 60)
        LOGGER.info("KeralaCaptain Bot V6.0 - FIXED VERSION")
        LOGGER.info("=" * 60)
        
        # Load domain
        CURRENT_PROTECTED_DOMAIN = await get_protected_domain()
        LOGGER.info(f"✅ Domain: {CURRENT_PROTECTED_DOMAIN}")
        
        # Create indexes
        try:
            await media_collection.create_index("tmdb_id", unique=True)
            await media_collection.create_index("wp_post_id", unique=True)
            LOGGER.info("✅ DB indexes ready")
        except Exception as e:
            LOGGER.warning(f"Index warning: {e}")
        
        # Start bot
        try:
            await main_bot.start()
            bot_info = await main_bot.get_me()
            LOGGER.info(f"✅ Bot: @{bot_info.username}")
        except FloodWait as e:
            LOGGER.error(f"FloodWait {e.value}s, waiting...")
            await asyncio.sleep(e.value + 5)
            await main_bot.start()
            bot_info = await main_bot.get_me()
            LOGGER.info(f"✅ Bot: @{bot_info.username}")
        except Exception as e:
            LOGGER.critical(f"❌ Bot start failed: {e}", exc_info=True)
            raise
            
        # Initialize clients
        await initialize_clients()
        LOGGER.info(f"✅ Clients: {len(multi_clients)}")
        
        # Start background tasks
        if Config.ON_HEROKU:
            asyncio.create_task(ping_server())
        
        asyncio.create_task(periodic_cache_cleanup())
        LOGGER.info("✅ Cache cleanup task started")
        
        # Start web server
        web_app = await web_server()
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
        await site.start()
        LOGGER.info(f"✅ Server: {Config.STREAM_URL}")
        
        # Notify admin
        try:
            await main_bot.send_message(
                Config.ADMIN_IDS[0],
                f"**✅ Bot Started (V6.0 FIXED)**\n\n"
                f"**URL:** `{Config.STREAM_URL}`\n"
                f"**Domain:** `{CURRENT_PROTECTED_DOMAIN}`\n"
                f"**Clients:** `{len(multi_clients)}`\n"
                f"**Cache:** `{humanbytes(get_cache_size())}`\n\n"
                f"**Fixes Applied:**\n"
                f"✅ Fixed chunk size (1MB)\n"
                f"✅ Fixed cache headers\n"
                f"✅ Disk caching enabled\n"
                f"✅ Mobile compatibility improved"
            )
        except:
            pass
        
        LOGGER.info("=" * 60)
        LOGGER.info("🚀 ALL SYSTEMS READY")
        LOGGER.info("=" * 60)
                
        await asyncio.Event().wait()

    loop = asyncio.get_event_loop()

    async def shutdown_handler(sig):
        LOGGER.info(f"⚠️ Shutdown signal: {sig.name}")
        
        if main_bot and main_bot.is_connected:
            try:
                await main_bot.stop()
                LOGGER.info("✅ Bot stopped")
            except:
                pass
        
        for client_id, client in list(multi_clients.items()):
            if client_id == 0:
                continue
            try:
                if client.is_connected:
                    await client.stop()
            except:
                pass
        
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        if tasks:
            [task.cancel() for task in tasks]
            await asyncio.gather(*tasks, return_exceptions=True)
        
        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown_handler(s))
        )

    try:
        loop.run_until_complete(main_startup_shutdown_logic())
        loop.run_forever()
    except KeyboardInterrupt:
        LOGGER.info("Keyboard interrupt")
    except Exception as e:
        LOGGER.critical(f"💥 Fatal: {e}", exc_info=True)
    finally:
        if loop.is_running():
            loop.stop()
        if not loop.is_closed():
            loop.close()
        LOGGER.info("👋 Shutdown complete")
