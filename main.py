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
import urllib.parse
import sys
import psutil
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
# KeralaCaptain Bot - Ultra-Optimized Streaming Engine V5.0
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

required_vars = [
    Config.API_ID, Config.API_HASH, Config.BOT_TOKEN,
    Config.MONGO_URI, Config.LOG_CHANNEL_ID, Config.STREAM_URL,
    Config.ADMIN_IDS
]
if not all(required_vars) or Config.ADMIN_IDS == [0]:
    LOGGER.critical("FATAL: Required variables missing. Cannot start.")
    exit(1)

CURRENT_PROTECTED_DOMAIN = Config.PROTECTED_DOMAIN

# ============================================================================ #
# BANDWIDTH OPTIMIZATION CONSTANTS
# ============================================================================ #

CHUNK_SIZE_INITIAL = 128 * 1024   # 128KB - Ultra small start (verify user is watching)
CHUNK_SIZE_SMALL = 256 * 1024     # 256KB
CHUNK_SIZE_MEDIUM = 512 * 1024    # 512KB
CHUNK_SIZE_LARGE = 1024 * 1024    # 1MB - Maximum
CHUNK_SCALE_THRESHOLD = 2 * 1024 * 1024  # Scale up after 2MB transferred

CACHE_TTL = 300
CACHE_CLEANUP_INTERVAL = 1200
MAX_CACHE_SIZE = 500  # Reduced for 512MB RAM

DRAIN_CHECK_INTERVAL = 50  # Check connection every 50 chunks

# ============================================================================ #
# SECURITY: BLOCKED AGENTS & BROWSERS
# ============================================================================ #

BLOCKED_AGENTS = [
    # Download Managers
    "idm", "internet download manager", "adm", "advanced download manager",
    "fdm", "free download manager", "download master", "eagleget",
    "jdownloader", "flashget", "getright", "mass downloader",
    
    # Crawlers/Bots/Scrapers
    "wget", "curl", "python-requests", "go-http-client", "java",
    "bot", "crawler", "spider", "scraper", "bytespider", "headlesschrome",
    
    # Force Download Browsers (Solo Browser, etc.)
    "solo browser", "uc browser", "opera mini download", "download browser",
    
    # Video Downloaders
    "video download", "videoder", "tubemate", "snaptube", "vidmate"
]

REQUIRED_HEADERS = ["range", "user-agent"]  # These headers MUST be present

def is_blocked_agent(user_agent):
    """Enhanced agent blocking with force-download detection"""
    if not user_agent:
        return True
    ua = user_agent.lower()
    for blocked in BLOCKED_AGENTS:
        if blocked in ua:
            return True
    return False

def is_download_request(request):
    """Detect if browser is forcing a download instead of streaming"""
    # Check for download-forcing headers
    accept = request.headers.get('Accept', '').lower()
    
    # Legitimate video players send video/* in Accept header
    if 'video/' not in accept and 'text/html' not in accept and '*/*' not in accept:
        return True
    
    # Check if Connection header suggests download tool
    connection = request.headers.get('Connection', '').lower()
    if connection == 'close':  # Download managers often use this
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
    LOGGER.info(f"Protected domain updated in DB: {new_domain}")
    return new_domain

# ============================================================================ #
# LRU CACHE FOR FILE PROPERTIES
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
    
    async def clear(self):
        async with self.lock:
            self.cache.clear()

# ============================================================================ #
# ULTRA-OPTIMIZED BYTE STREAMER
# ============================================================================ #

multi_clients = {}
work_loads = {}
class_cache = {}
processed_media_groups = {}
next_client_idx = 0
stream_errors = 0
last_error_reset = time.time()

class ByteStreamer:
    def __init__(self, client: Client):
        self.client: Client = client
        self.cached_file_ids = LRUCache(MAX_CACHE_SIZE)
        self.session_cache = {}
        asyncio.create_task(self.clean_stale_sessions())

    async def clean_stale_sessions(self):
        """Only clean stale sessions, keep active ones"""
        while True:
            await asyncio.sleep(CACHE_CLEANUP_INTERVAL)
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
            
            LOGGER.info(f"Cleaned {len(stale)} stale sessions. Active: {len(self.session_cache)}")

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
                LOGGER.warning(f"Stale session for DC {dc_id}: {e}. Recreating.")
                try:
                    await media_session.stop()
                except:
                    pass
                if dc_id in self.client.media_sessions:
                    del self.client.media_sessions[dc_id]
                media_session = None

        LOGGER.info(f"Creating new media session for DC {dc_id}")
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
                    LOGGER.warning(f"AuthBytesInvalid attempt {i+1}: {e}")
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
        """ULTRA-OPTIMIZED: Adaptive chunking + immediate disconnect detection"""
        media_session = await self.generate_media_session(file_id)
        location = self.get_location(file_id)

        current_offset = offset
        retry_count = 0
        max_retries = 3
        
        # Start with ultra-small chunks to verify user is watching
        current_chunk_size = CHUNK_SIZE_INITIAL
        bytes_transferred = 0
        chunk_count = 0

        while True:
            try:
                chunk = await media_session.send(
                    raw.functions.upload.GetFile(
                        location=location,
                        offset=current_offset,
                        limit=current_chunk_size
                    ),
                    timeout=30
                )
                
                if isinstance(chunk, raw.types.upload.File) and chunk.bytes:
                    yield chunk.bytes
                    
                    chunk_len = len(chunk.bytes)
                    bytes_transferred += chunk_len
                    chunk_count += 1
                    
                    if chunk_len < current_chunk_size:
                        break
                        
                    current_offset += chunk_len

                    # ADAPTIVE SCALING: Gradually increase chunk size
                    if bytes_transferred > CHUNK_SCALE_THRESHOLD:
                        if current_chunk_size < CHUNK_SIZE_LARGE:
                            current_chunk_size = min(CHUNK_SIZE_LARGE, current_chunk_size * 2)
                    elif bytes_transferred > 1024 * 1024:  # After 1MB
                        if current_chunk_size < CHUNK_SIZE_MEDIUM:
                            current_chunk_size = CHUNK_SIZE_MEDIUM

                else:
                    break

            except FileReferenceExpired:
                retry_count += 1
                if retry_count > max_retries:
                    raise Exception(f"Max retries exceeded for FileReferenceExpired")
                
                LOGGER.warning(f"FileReferenceExpired for msg {message_id}, retry {retry_count}/{max_retries}")
                
                original_msg = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
                if original_msg:
                    refreshed_msg = await forward_file_safely(original_msg)
                    if refreshed_msg:
                        new_file_id = await self.get_file_properties(refreshed_msg.id)
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
                                
                        location = self.get_location(new_file_id)
                        await asyncio.sleep(2)
                        continue
                raise

            except FloodWait as e:
                LOGGER.warning(f"FloodWait {e.value}s. Waiting...")
                await asyncio.sleep(e.value)
                continue

# ============================================================================ #
# WEB SERVER ROUTES
# ============================================================================ #

routes = web.RouteTableDef()

@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.Response(text="KeralaCaptain Streaming Service", content_type='text/html')

@routes.get("/health")
async def health_handler(request):
    global stream_errors, last_error_reset
    if time.time() - last_error_reset > 60:
        stream_errors = 0
        last_error_reset = time.time()
    
    active_sessions = len(multi_clients)
    cache_size = 0
    if multi_clients:
        sample_client = list(multi_clients.values())[0]
        if sample_client in class_cache:
            streamer = class_cache[sample_client]
            cache_size = len(streamer.cached_file_ids.cache) if hasattr(streamer.cached_file_ids, 'cache') else 0
    
    return web.json_response({
        "status": "ok",
        "active_clients": active_sessions,
        "cache_size": cache_size,
        "stream_errors_last_min": stream_errors,
        "workloads": work_loads,
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
            return web.Response(status=403, text="Access denied. Please use the website player.")

        # ===== SECURITY LAYER 2: DOWNLOAD DETECTION =====
        if is_download_request(request):
            LOGGER.warning(f"BLOCKED Download attempt from: {user_agent[:100]}")
            return web.Response(status=403, text="Downloads not allowed. Stream only.")

        # ===== SECURITY LAYER 3: REFERER CHECK =====
        referer = request.headers.get('Referer', '')
        allowed_domain = CURRENT_PROTECTED_DOMAIN.rstrip('/')
        
        # STRICT: Must have referer AND it must match our domain
        if not referer or allowed_domain not in referer:
            LOGGER.warning(f"BLOCKED - Invalid referer: {referer}")
            return web.Response(
                status=403,
                text="Access denied. This stream can only be accessed from the official website."
            )

        # ===== SECURITY LAYER 4: REQUIRED HEADERS =====
        for required_header in REQUIRED_HEADERS:
            if required_header not in request.headers:
                LOGGER.warning(f"BLOCKED - Missing header: {required_header}")
                return web.Response(status=400, text="Bad request")

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

        # ===== ANTI-DOWNLOAD HEADERS =====
        headers = {
            "Content-Type": file_id.mime_type,
            "Accept-Ranges": "bytes",
            "Content-Range": f"bytes {from_bytes}-{file_size - 1}/{file_size}",
            "Content-Length": str(file_size - from_bytes),
            "Content-Disposition": "inline",  # Force inline (no download)
            "X-Content-Type-Options": "nosniff",
            "Access-Control-Allow-Origin": allowed_domain,
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Access-Control-Allow-Headers": "Range, User-Agent",
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache"
        }

        resp = web.StreamResponse(
            status=206 if range_header else 200,
            headers=headers
        )
        await resp.prepare(request)

        # ===== CALCULATE OFFSET =====
        chunk_size = CHUNK_SIZE_INITIAL
        offset = from_bytes - (from_bytes % chunk_size)
        first_part_cut = from_bytes - offset

        # ===== START STREAMING WITH AGGRESSIVE DISCONNECT DETECTION =====
        body_generator = tg_connect.yield_file(file_id, offset, message_id)
        is_first_chunk = True
        chunk_count = 0

        async for chunk in body_generator:
            try:
                if is_first_chunk and first_part_cut > 0:
                    await resp.write(chunk[first_part_cut:])
                    is_first_chunk = False
                else:
                    await resp.write(chunk)
                
                # ===== CRITICAL: CHECK CONNECTION EVERY FEW CHUNKS =====
                chunk_count += 1
                if chunk_count % 5 == 0:  # Check every 5 chunks
                    try:
                        await asyncio.wait_for(resp.drain(), timeout=2.0)
                    except asyncio.TimeoutError:
                        LOGGER.info(f"Connection timeout detected for {message_id}. Stopping stream.")
                        return resp
                    except Exception:
                        LOGGER.info(f"Connection lost for {message_id}. Stopping stream.")
                        return resp
                
            except (ConnectionError, asyncio.CancelledError, ClientConnectionError, OSError) as e:
                LOGGER.info(f"User disconnected from {message_id}: {type(e).__name__}")
                return resp
            except Exception as e:
                LOGGER.error(f"Write error for {message_id}: {e}")
                return resp

        return resp

    except (FileReferenceExpired, AuthBytesInvalid) as e:
        stream_errors += 1
        LOGGER.error(f"Stream expired for {message_id}: {type(e).__name__}")
        return web.Response(status=410, text="Stream expired. Please refresh the page.")

    except FileNotFoundError as e:
        LOGGER.error(f"File not found: {message_id}")
        return web.Response(status=404, text="Media not found")

    except Exception as e:
        stream_errors += 1
        LOGGER.critical(f"Unhandled stream error for {message_id}: {e}", exc_info=True)
        return web.Response(status=500, text="Internal server error")

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
        LOGGER.info("No additional clients found. Running in single-client mode.")
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
            LOGGER.error(f"Failed to start Client {client_id}: {e}")
            return None

    clients = await asyncio.gather(*[start_client(i, token) for i, token in all_tokens.items()])
    multi_clients.update({cid: client for cid, client in clients if client is not None})
    
    if len(multi_clients) > 1:
        LOGGER.info(f"Multi-client mode enabled: {len(multi_clients)} clients active")

async def forward_file_safely(message_to_forward: Message):
    """Sends file to log channel using cached media"""
    try:
        media = message_to_forward.document or message_to_forward.video
        if not media:
            LOGGER.error("Message has no media to forward")
            return None
            
        file_id = media.file_id
            
        LOGGER.info(f"Forwarding cached media for message {message_to_forward.id}...")
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
        "**👋 Welcome, Admin!**\n\n"
        "This is your streaming bot's control panel.\n"
        "What would you like to do?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Statistics", callback_data="admin_stats")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings")],
            [InlineKeyboardButton("🔄 Restart Bot", callback_data="admin_restart")]
        ])
    )
    await update_user_conversation(message.chat.id, None)

@main_bot.on_callback_query(filters.regex("^admin_stats$") & admin_only)
async def stats_callback(client, cb: CallbackQuery):
    await cb.answer("Fetching stats...")
    
    uptime = get_readable_time(time.time() - start_time)
    
    try:
        cpu_usage = psutil.cpu_percent(interval=1)
        ram = psutil.virtual_memory()
        ram_usage = ram.percent
        ram_used = humanbytes(ram.used)
        ram_total = humanbytes(ram.total)
        disk = psutil.disk_usage('/')
        disk_usage = disk.percent
        disk_used = humanbytes(disk.used)
        disk_total = humanbytes(disk.total)
    except Exception as e:
        LOGGER.warning(f"Could not fetch system stats: {e}")
        cpu_usage = ram_usage = disk_usage = "N/A"
        ram_used = ram_total = disk_used = disk_total = "N/A"

    active_clients = len(multi_clients)
    workload_str = "\n".join([f"  - Client {cid}: {load} active streams" for cid, load in work_loads.items()])
    
    movies, series = await get_stats()

    text = (
        f"**📊 Bot Statistics**\n\n"
        f"**⏱ Uptime:** `{uptime}`\n\n"
        f"**💾 System Resources:**\n"
        f"  • CPU: `{cpu_usage}%`\n"
        f"  • RAM: `{ram_usage}%` ({ram_used} / {ram_total})\n"
        f"  • Disk: `{disk_usage}%` ({disk_used} / {disk_total})\n\n"
        f"**📡 Streaming:**\n"
        f"  • Active Clients: `{active_clients}`\n"
        f"  • Stream Errors (last min): `{stream_errors}`\n"
        f"  • Workloads:\n{workload_str}\n\n"
        f"**📚 Database:**\n"
        f"  • Movies: `{movies}`\n"
        f"  • Series: `{series}`\n"
        f"  • Total: `{movies + series}`"
    )
           
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
        f"**⚙️ Bot Settings**\n\n"
        f"**🔒 Protected Domain:**\n"
        f"Streams will ONLY work when accessed from this domain.\n"
        f"This prevents hotlinking and unauthorized access.\n\n"
        f"**Current Value:**\n`{current_domain}`\n\n"
        f"**Security Features Active:**\n"
        f"✅ User-Agent blocking (IDM/ADM blocked)\n"
        f"✅ Download prevention (force inline streaming)\n"
        f"✅ Referer validation\n"
        f"✅ Adaptive bandwidth optimization\n"
        f"✅ Connection monitoring"
    )
           
    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Change Domain", callback_data="admin_set_domain")],
            [InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]
        ])
    )

@main_bot.on_callback_query(filters.regex("^admin_set_domain$") & admin_only)
async def set_domain_callback(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_domain"})
    await cb.message.edit_text(
        "**✏️ Set New Protected Domain**\n\n"
        "Send the new domain URL that should be allowed to access streams.\n\n"
        "**Examples:**\n"
        "• `https://keralacaptain.shop`\n"
        "• `keralacaptain.shop`\n"
        "• `www.keralacaptain.shop`\n\n"
        "⚠️ **Important:** Only requests from this domain will be able to stream videos.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ Cancel", callback_data="admin_cancel_conv")]
        ])
    )

@main_bot.on_callback_query(filters.regex("^admin_restart$") & admin_only)
async def restart_callback(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**⚠️ Restart Confirmation**\n\n"
        "This will restart the entire bot process.\n"
        "All active streams will be disconnected.\n\n"
        "**Are you sure?**",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Restart", callback_data="admin_restart_confirm"),
                InlineKeyboardButton("❌ No, Cancel", callback_data="admin_main_menu")
            ]
        ])
    )

@main_bot.on_callback_query(filters.regex("^admin_restart_confirm$") & admin_only)
async def restart_confirm_callback(client, cb: CallbackQuery):
    await cb.answer("Restarting bot...")
    await cb.message.edit_text(
        "**🔄 Restarting Bot...**\n\n"
        "The bot will be back online in a few seconds.\n"
        "Please wait..."
    )
    
    try:
        LOGGER.info("RESTART initiated by admin")
        if main_bot and main_bot.is_connected:
            await main_bot.stop()
        
        await asyncio.sleep(1)
    except Exception as e:
        LOGGER.error(f"Error during restart cleanup: {e}")
    
    os.execl(sys.executable, sys.executable, *sys.argv)

@main_bot.on_callback_query(filters.regex("^(admin_main_menu|admin_cancel_conv)$") & admin_only)
async def main_menu_callback(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, None)
    await cb.message.edit_text(
        "**👋 Welcome, Admin!**\n\n"
        "This is your streaming bot's control panel.\n"
        "What would you like to do?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Statistics", callback_data="admin_stats")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings")],
            [InlineKeyboardButton("🔄 Restart Bot", callback_data="admin_restart")]
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
        
        # Validation
        if not new_domain or " " in new_domain:
            return await message.reply_text(
                "❌ **Invalid Format**\n\n"
                "Please send a valid domain without spaces.\n"
                "Example: `keralacaptain.shop`"
            )
        
        # Remove protocols if user included them
        new_domain = new_domain.replace("http://", "").replace("https://", "")
        
        if "." not in new_domain:
            return await message.reply_text(
                "❌ **Invalid Domain**\n\n"
                "Domain must contain at least one dot (.).\n"
                "Example: `keralacaptain.shop`"
            )
        
        try:
            status_msg = await message.reply_text("⏳ Saving new domain...")
            saved_domain = await set_protected_domain(new_domain)
            
            await status_msg.edit_text(
                f"✅ **Domain Updated Successfully!**\n\n"
                f"New Protected Domain:\n`{saved_domain}`\n\n"
                f"Streams will now ONLY work when accessed from this domain.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("⬅️ Back to Settings", callback_data="admin_settings")]
                ])
            )
            await update_user_conversation(chat_id, None)
            
        except Exception as e:
            LOGGER.error(f"Failed to save domain: {e}", exc_info=True)
            await status_msg.edit_text(
                f"❌ **Error Saving Domain**\n\n"
                f"Could not update domain: `{str(e)}`\n\n"
                f"Please try again.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Try Again", callback_data="admin_set_domain")],
                    [InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]
                ])
            )

# ============================================================================ #
# APPLICATION LIFECYCLE
# ============================================================================ #

async def ping_server():
    """Keeps server alive on platforms like Render/Heroku"""
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
        """Handles graceful startup and shutdown"""
        global CURRENT_PROTECTED_DOMAIN
        
        LOGGER.info("=" * 60)
        LOGGER.info("KeralaCaptain Streaming Bot v5.0 - Starting Up")
        LOGGER.info("=" * 60)
        
        # Fetch protected domain from database
        LOGGER.info("Loading protected domain from database...")
        CURRENT_PROTECTED_DOMAIN = await get_protected_domain()
        LOGGER.info(f"✅ Protected Domain: {CURRENT_PROTECTED_DOMAIN}")
        
        # Create database indexes
        LOGGER.info("Creating database indexes...")
        try:
            await media_collection.create_index("tmdb_id", unique=True)
            await media_collection.create_index("wp_post_id", unique=True)
            LOGGER.info("✅ Database indexes created")
        except Exception as e:
            LOGGER.warning(f"Index creation warning: {e}")
        
        # Start main bot
        LOGGER.info("Starting main bot...")
        try:
            await main_bot.start()
            bot_info = await main_bot.get_me()
            LOGGER.info(f"✅ Main Bot: @{bot_info.username}")
        except FloodWait as e:
            LOGGER.error(f"FloodWait on startup: {e.value}s. Waiting...")
            await asyncio.sleep(e.value + 5)
            await main_bot.start()
            bot_info = await main_bot.get_me()
            LOGGER.info(f"✅ Main Bot: @{bot_info.username} (after wait)")
        except Exception as e:
            LOGGER.critical(f"❌ Failed to start main bot: {e}", exc_info=True)
            raise
            
        # Initialize additional clients
        LOGGER.info("Initializing streaming clients...")
        await initialize_clients()
        LOGGER.info(f"✅ Total clients active: {len(multi_clients)}")
        
        # Start ping task if on Heroku/Render
        if Config.ON_HEROKU:
            LOGGER.info("Platform detected: Heroku/Render - Starting ping task")
            asyncio.create_task(ping_server())
        
        # Start web server
        LOGGER.info(f"Starting web server on port {Config.PORT}...")
        web_app = await web_server()
        runner = web.AppRunner(web_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
        await site.start()
        LOGGER.info(f"✅ Web server running: {Config.STREAM_URL}")
        
        # Send startup notification to admin
        try:
            startup_msg = (
                f"**✅ Bot Started Successfully!**\n\n"
                f"**🌐 Stream URL:** `{Config.STREAM_URL}`\n"
                f"**🔒 Protected Domain:** `{CURRENT_PROTECTED_DOMAIN}`\n"
                f"**📡 Active Clients:** `{len(multi_clients)}`\n"
                f"**⏱ Started At:** `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`\n\n"
                f"**Security Status:**\n"
                f"✅ User-Agent blocking enabled\n"
                f"✅ Download prevention active\n"
                f"✅ Referer validation enabled\n"
                f"✅ Adaptive bandwidth optimization active"
            )
            await main_bot.send_message(Config.ADMIN_IDS[0], startup_msg)
        except Exception as e:
            LOGGER.warning(f"Could not send startup notification: {e}")
        
        LOGGER.info("=" * 60)
        LOGGER.info("🚀 All systems operational - Bot is ready!")
        LOGGER.info("=" * 60)
                
        await asyncio.Event().wait()

    loop = asyncio.get_event_loop()

    async def shutdown_handler(sig):
        LOGGER.info(f"⚠️ Received signal {sig.name} - Shutting down gracefully...")
        
        if main_bot and main_bot.is_connected:
            LOGGER.info("Stopping main bot...")
            try:
                await main_bot.stop()
                LOGGER.info("✅ Main bot stopped")
            except Exception as e:
                LOGGER.error(f"Error stopping main bot: {e}")
        
        # Stop all multi clients
        for client_id, client in list(multi_clients.items()):
            if client_id == 0:
                continue  # Already stopped main bot
            try:
                if client.is_connected:
                    await client.stop()
                    LOGGER.info(f"✅ Client {client_id} stopped")
            except Exception as e:
                LOGGER.error(f"Error stopping client {client_id}: {e}")
        
        tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
        if tasks:
            LOGGER.info(f"Cancelling {len(tasks)} pending tasks...")
            [task.cancel() for task in tasks]
            await asyncio.gather(*tasks, return_exceptions=True)
        
        loop.stop()
        LOGGER.info("✅ Shutdown complete")

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown_handler(s))
        )

    try:
        loop.run_until_complete(main_startup_shutdown_logic())
        loop.run_forever()
    except KeyboardInterrupt:
        LOGGER.info("Keyboard interrupt received")
    except Exception as e:
        LOGGER.critical(f"💥 Critical error: {e}", exc_info=True)
    finally:
        LOGGER.info("Cleaning up...")
        if loop.is_running():
            loop.stop()
        if not loop.is_closed():
            loop.close()
        LOGGER.info("👋 Goodbye!")
