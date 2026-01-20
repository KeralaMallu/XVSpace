# main.py - KeralaCaptain Bot - Pure Streaming Engine V4.1 (Modified with caching + disconnect detection)
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
import aiofiles
import urllib.parse
import sys
import psutil # For stats
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

# -------------------------------------------------------------------------------- #
# KeralaCaptain Bot - Pure Streaming Engine V4.1 (Caching + Disconnect Detection)
# -------------------------------------------------------------------------------- #

# Load configurations from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s] - %(message)s')
LOGGER = logging.getLogger(__name__)
logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("aiohttp.web").setLevel(logging.ERROR)

# --- NEW: Record bot start time ---
start_time = time.time()

class Config:
    API_ID = int(os.environ.get("API_ID", 0))
    API_HASH = os.environ.get("API_HASH", "")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
    # REMOVED: BACKUP_BOT_TOKEN (no longer needed)
    
    # --- NEW: Admin and Domain Config ---
    ADMIN_IDS = list(int(admin_id) for admin_id in os.environ.get("ADMIN_IDS", "6644681404").split())
    PROTECTED_DOMAIN = os.environ.get("PROTECTED_DOMAIN", "https://www.keralacaptain.shop/").rstrip('/') + '/'
    
    MONGO_URI = os.environ.get("MONGO_URI", "")
    LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", 0))
    STREAM_URL = os.environ.get("STREAM_URL", "").rstrip('/')
    PORT = int(os.environ.get("PORT", 8080))
    
    PING_INTERVAL = int(os.environ.get("PING_INTERVAL", 1200))
    ON_HEROKU = 'DYNO' in os.environ

# --- VALIDATE ESSENTIAL CONFIGURATIONS ---
required_vars = [
    Config.API_ID, Config.API_HASH, Config.BOT_TOKEN,
    Config.MONGO_URI, Config.LOG_CHANNEL_ID, Config.STREAM_URL,
    Config.ADMIN_IDS
]
if not all(required_vars) or Config.ADMIN_IDS == [0]:
    LOGGER.critical("FATAL: One or more required variables (API_ID, API_HASH, BOT_TOKEN, MONGO_URI, LOG_CHANNEL_ID, STREAM_URL, ADMIN_IDS) are missing. Cannot start.")
    exit(1)

# --- NEW: Global variable for the protected domain ---
CURRENT_PROTECTED_DOMAIN = Config.PROTECTED_DOMAIN

# -------------------------------------------------------------------------------- #
# CACHING / STREAM CONTROL SETTINGS (NEW)
# -------------------------------------------------------------------------------- #

CACHE_DIR = "./cache"
CACHE_MAX_AGE_SECONDS = 30 * 60       # 30 minutes
CACHE_CLEAN_INTERVAL = 10 * 60        # 10 minutes
CHUNK_SIZE = 1024 * 1024              # constant chunk size (1 MiB) - DO NOT change
TAIL_SLEEP = 0.12                      # sleep while waiting for more bytes during tailing

# Ensure cache dir exists
os.makedirs(CACHE_DIR, exist_ok=True)

# Per-message coordination
cache_locks = {}        # message_id -> asyncio.Lock()
cache_events = {}       # message_id -> asyncio.Event()  (set when cache fully written)
cache_writers = {}      # message_id -> bool (True if writer active)
cache_writer_tasks = {} # message_id -> task (optional)

# Blocklist regex for download managers (case-insensitive)
DOWNLOAD_AGENT_PATTERNS = re.compile(
    r"(idman|internet download manager|adm:|advanced download manager|1dm|solo browser|download manager|getright|flashget|wget|curl)",
    flags=re.IGNORECASE
)

# -------------------------------------------------------------------------------- #
# HELPER FUNCTIONS & CLASSES (Unchanged logic preserved, new helpers added)
# -------------------------------------------------------------------------------- #

# --- Base64 Encoding/Decoding for Stream URLs (Kept for compatibility) ---
async def encode(string: str) -> str:
    string_bytes = string.encode("ascii")
    base64_bytes = base64.urlsafe_b64encode(string_bytes)
    return (base64_bytes.decode("ascii")).strip("=")

async def decode(base64_string: str) -> str:
    base64_string = base64_string.strip("=")
    base64_bytes = (base64_string + "=" * (-len(base64_string) % 4)).encode("ascii")
    string_bytes = base64.urlsafe_b64decode(base64_bytes)
    return string_bytes.decode("ascii")

# --- Human-readable formatters (Kept for new stats panel) ---
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
    """Return a human-readable time format"""
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

# Safe filename for cache (use message id + original extension)
def cache_filename_for(message_id: int, original_name: str) -> str:
    # sanitize extension
    _, ext = os.path.splitext(original_name or "")
    if not ext:
        ext = ".mp4"
    safe_name = f"{message_id}{ext}"
    return os.path.join(CACHE_DIR, safe_name)

# Check UA for download manager clients
def is_blocked_user_agent(ua: str) -> bool:
    if not ua:
        return False
    return bool(DOWNLOAD_AGENT_PATTERNS.search(ua))

# -------------------------------------------------------------------------------- #
# DATABASE OPERATIONS (UNCHANGED)
# -------------------------------------------------------------------------------- #

db_client = AsyncIOMotorClient(Config.MONGO_URI)
db = db_client['KeralaCaptainBotDB']

# Primary collection for all normal operations (Kept as is)
media_collection = db['media']
# Backup collection (Kept as is)
media_backup_collection = db['media_backup']
# User conversations (Kept for new settings panel)
user_conversations_col = db['conversations']
# --- NEW: Collection for bot settings ---
settings_collection = db['settings']

# --- Database Functions (Kept original read/write functions for streaming logic) ---

async def check_duplicate(tmdb_id):
    """Checks for duplicates only in the main collection."""
    return await media_collection.find_one({"tmdb_id": tmdb_id})

async def add_media_to_db(data):
    """Inserts new media data into both the main and backup collections."""
    await media_collection.insert_one(data)
    await media_backup_collection.insert_one(data) # Also write to the backup

async def get_media_by_post_id(post_id: int):
    """Reads media data only from the main collection for regular use."""
    return await media_collection.find_one({"wp_post_id": post_id})

async def update_media_links_in_db(post_id: int, new_message_ids: dict, new_stream_link: str):
    """Updates links in both the main and backup collections."""
    update_query = {
        "$set": {"message_ids": new_message_ids, "stream_link": new_stream_link}
    }
    await media_collection.update_one({"wp_post_id": post_id}, update_query)
    await media_backup_collection.update_one({"wp_post_id": post_id}, update_query) # Also update the backup

async def delete_media_from_db(post_id: int):
    """Deletes media data from both the main and backup collections."""
    result_main = await media_collection.delete_one({"wp_post_id": post_id})
    await media_backup_collection.delete_one({"wp_post_id": post_id}) # Also delete from the backup
    return result_main

async def get_stats():
    """Calculates stats based only on the main collection."""
    movies_count = await media_collection.count_documents({"type": "movie"})
    series_count = await media_collection.count_documents({"type": "series"})
    return movies_count, series_count

async def get_all_media_for_library(page: int = 0, limit: int = 10):
    """Fetches the library list only from the main collection."""
    cursor = media_collection.find().sort("added_at", -1).skip(page * limit).limit(limit)
    return await cursor.to_list(length=limit)

async def get_user_conversation(chat_id):
    """Manages user conversation state."""
    return await user_conversations_col.find_one({"_id": chat_id})

async def update_user_conversation(chat_id, data):
    """Manages user conversation state."""
    if data:
        await user_conversations_col.update_one({"_id": chat_id}, {"$set": data}, upsert=True)
    else:
        await user_conversations_col.delete_one({"_id": chat_id})

async def get_post_id_from_msg_id(msg_id: int):
    """Helper for stream refreshing. Reads only from the main collection."""
    doc = await media_collection.find_one({"message_ids": {"$in": [msg_id]}})
    return doc['wp_post_id'] if doc else None

# --- NEW: Database functions for Domain Settings (unchanged) ---

async def get_protected_domain() -> str:
    """Fetches the protected domain from settings, returns default if not found."""
    try:
        doc = await settings_collection.find_one({"_id": "bot_settings"})
        if doc and "protected_domain" in doc:
            return doc["protected_domain"]
    except Exception as e:
        LOGGER.error(f"Could not fetch domain from DB: {e}. Using default.")
    
    # Fallback to default from Config
    return Config.PROTECTED_DOMAIN

async def set_protected_domain(new_domain: str):
    """Saves the new protected domain to the database."""
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
    CURRENT_PROTECTED_DOMAIN = new_domain # Update global variable
    LOGGER.info(f"Protected domain updated in DB: {new_domain}")
    return new_domain

# -------------------------------------------------------------------------------- #
# STREAMING ENGINE & WEB SERVER (UNCHANGED CORE LOGIC but with caching + drain)
# -------------------------------------------------------------------------------- #

multi_clients = {}
work_loads = {}
class_cache = {}
processed_media_groups = {} # Kept for FileReference logic
next_client_idx = 0 
stream_errors = 0 
last_error_reset = time.time()

# Upgraded ByteStreamer (Kept As Is, unchanged except minor logging)
class ByteStreamer:
    def __init__(self, client: Client):
        self.client: Client = client
        self.cached_file_ids = {} # Cache for file properties
        self.session_cache = {} # {dc_id: (session, timestamp)} for TTL
        asyncio.create_task(self.clean_cache_regularly())

    async def clean_cache_regularly(self):
        while True:
            await asyncio.sleep(1200) # 20 min
            self.cached_file_ids.clear()
            self.session_cache.clear() 
            LOGGER.info("Cleared ByteStreamer's cached file properties and sessions.")

    async def get_file_properties(self, message_id: int):
        if message_id in self.cached_file_ids:
            return self.cached_file_ids[message_id]

        message = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
        if not message or message.empty or not (message.document or message.video):
            raise FileNotFoundError

        media = message.document or message.video
        file_id = FileId.decode(media.file_id)
        setattr(file_id, "file_size", media.file_size or 0)
        setattr(file_id, "mime_type", media.mime_type or "video/mp4")
        setattr(file_id, "file_name", media.file_name or "Unknown.mp4")

        self.cached_file_ids[message_id] = file_id
        return file_id

    async def generate_media_session(self, file_id: FileId) -> Session:
        media_session = self.client.media_sessions.get(file_id.dc_id)
        dc_id = file_id.dc_id

        if dc_id in self.session_cache:
            session, ts = self.session_cache[dc_id]
            if time.time() - ts < 300: # 5min TTL
                LOGGER.debug(f"Reusing TTL-cached media session for DC {dc_id}")
                return session

        if media_session:
            try:
                await media_session.send(raw.functions.help.GetConfig(), timeout=10)
                self.session_cache[dc_id] = (media_session, time.time()) # Cache on success
                LOGGER.debug(f"Reusing pinged media session for DC {dc_id}")
                return media_session
            except Exception as e:
                LOGGER.warning(f"Existing media session for DC {dc_id} is stale: {e}. Recreating.")
                try:
                    await media_session.stop()
                except: pass
                if dc_id in self.client.media_sessions:
                    del self.client.media_sessions[dc_id]
                media_session = None

        LOGGER.info(f"Creating new media session for DC {dc_id}")
        if dc_id != await self.client.storage.dc_id():
            media_session = Session(self.client, dc_id, await Auth(self.client, dc_id, await self.client.storage.test_mode()).create(), await self.client.storage.test_mode(), is_media=True)
            await media_session.start()
            for i in range(3):
                try:
                    exported_auth = await self.client.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc_id))
                    await media_session.send(raw.functions.auth.ImportAuthorization(id=exported_auth.id, bytes=exported_auth.bytes))
                    break
                except AuthBytesInvalid as e:
                    LOGGER.warning(f"AuthBytesInvalid on attempt {i+1}: {e}")
                    if i == 2: raise
                    await asyncio.sleep(1)
        else:
            media_session = Session(self.client, dc_id, await self.client.storage.auth_key(), await self.client.storage.test_mode(), is_media=True)
            await media_session.start()

        self.client.media_sessions[dc_id] = media_session
        self.session_cache[dc_id] = (media_session, time.time()) # Cache new
        return media_session

    @staticmethod
    def get_location(file_id: FileId):
        if file_id.file_type == FileType.PHOTO:
            return InputPhotoFileLocation(id=file_id.media_id, access_hash=file_id.access_hash, file_reference=file_id.file_reference, thumb_size=file_id.thumbnail_size)
        else:
            return InputDocumentFileLocation(id=file_id.media_id, access_hash=file_id.access_hash, file_reference=file_id.file_reference, thumb_size=file_id.thumbnail_size)

    async def yield_file(self, file_id: FileId, offset: int, chunk_size: int, message_id: int):
        """
        Async generator that yields bytes chunks from Telegram using media session.
        The logic here is the same as original V4.1; callers must handle cancellation
        and closing the async generator to stop the download immediately.
        """
        media_session = await self.generate_media_session(file_id)
        location = self.get_location(file_id)

        current_offset = offset
        retry_count = 0
        max_retries = 3

        while True:
            try:
                chunk = await media_session.send(
                    raw.functions.upload.GetFile(location=location, offset=current_offset, limit=chunk_size),
                    timeout=30
                )
                
                if isinstance(chunk, raw.types.upload.File) and chunk.bytes:
                    yield chunk.bytes
                    if len(chunk.bytes) < chunk_size:
                        break
                    current_offset += len(chunk.bytes)
                else:
                    break

            except FileReferenceExpired:
                # This logic is CRUCIAL and why we kept the DB functions
                retry_count += 1
                if retry_count > max_retries:
                    raise 
                LOGGER.warning(f"FileReferenceExpired for msg {message_id}, retry {retry_count}/{max_retries}. Refreshing...")
                
                original_msg = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
                if original_msg:
                    refreshed_msg = await forward_file_safely(original_msg) # Uses forward_file_safely
                    if refreshed_msg:
                        new_file_id = await self.get_file_properties(refreshed_msg.id)
                        self.cached_file_ids[message_id] = new_file_id 
                        
                        # Update DB
                        post_id = await get_post_id_from_msg_id(message_id) # Uses get_post_id_from_msg_id
                        if post_id:
                            media_doc = await get_media_by_post_id(post_id) # Uses get_media_by_post_id
                            if media_doc:
                                old_qualities = media_doc['message_ids']
                                # Find the key (quality) for the old message_id
                                quality_key = next((k for k, v in old_qualities.items() if v == message_id), None)
                                
                                new_qualities = old_qualities
                                if quality_key:
                                    new_qualities[quality_key] = refreshed_msg.id
                                else:
                                    # Fallback if key not found (shouldn't happen)
                                    new_qualities = {k: refreshed_msg.id if v == message_id else v for k, v in old_qualities.items()}

                                # Uses update_media_links_in_db
                                await update_media_links_in_db(post_id, new_qualities, media_doc['stream_link'])
                                
                        location = self.get_location(new_file_id) # Recreate location
                        await asyncio.sleep(2) 
                        continue # Retry fetch
                raise # Failed refresh

            except FloodWait as e:
                LOGGER.warning(f"FloodWait of {e.value} seconds on get_file. Waiting...")
                await asyncio.sleep(e.value)
                continue

# -------------------------------------------------------------------------------- #
# ROUTES
# -------------------------------------------------------------------------------- #

routes = web.RouteTableDef()

@routes.get("/", allow_head=True)
async def root_route_handler(request):
    return web.Response(text=f"Welcome to KeralaCaptain's Streaming Service!", content_type='text/html')

@routes.get("/health")
async def health_handler(request):
    global stream_errors, last_error_reset
    if time.time() - last_error_reset > 60: 
        stream_errors = 0
        last_error_reset = time.time()
    active_sessions = len(multi_clients)
    cache_size = 0
    # provide a simple cached files count
    try:
        cache_files = os.listdir(CACHE_DIR)
        cache_size = len([f for f in cache_files if os.path.isfile(os.path.join(CACHE_DIR, f))])
    except Exception:
        cache_size = 0
    return web.json_response({
        "status": "ok",
        "active_clients": active_sessions,
        "cache_files": cache_size,
        "stream_errors_last_min": stream_errors,
        "workloads": work_loads,
    })

@routes.get("/favicon.ico")
async def favicon_handler(request):
    return web.Response(status=204) 

@routes.get(r"/stream/{message_id:\d+}")
async def stream_handler(request: web.Request):
    """
    Main streaming handler.
    - Blocks download-managers by User-Agent.
    - Enforces Referer check using CURRENT_PROTECTED_DOMAIN.
    - Uses disk cache (./cache/) to serve repeated requests without Telegram bandwidth.
    - If cache missing, first requester downloads from Telegram and writes to disk while streaming.
    - Calls await resp.drain() after every chunk to detect disconnects and stop Telegram download immediately.
    """
    client_index = None 
    message_id = None
    try:
        # --- Referer Check (unchanged logic) ---
        referer = request.headers.get('Referer')
        allowed_referer = CURRENT_PROTECTED_DOMAIN 

        if not referer or not referer.startswith(allowed_referer):
            LOGGER.warning(f"Blocked hotlink attempt. Referer: {referer}. Allowed: {allowed_referer}")
            return web.Response(status=403, text="403 Forbidden: Direct access is not allowed.")
        
        # --- Block download manager User-Agents ---
        ua = request.headers.get("User-Agent", "")
        if is_blocked_user_agent(ua):
            LOGGER.warning(f"Blocked download-manager UA: {ua}")
            return web.Response(status=403, text="403 Forbidden: Download managers are not allowed.")

        # Parse message_id and Range header
        message_id = int(request.match_info['message_id'])
        range_header = request.headers.get("Range", None)

        # --- Client selection / load balancing (unchanged) ---
        min_load = min(work_loads.values())
        candidates = [cid for cid, load in work_loads.items() if load == min_load]
        
        global next_client_idx
        if len(candidates) > 1:
            client_index = candidates[next_client_idx % len(candidates)]
            next_client_idx += 1
        else:
            client_index = candidates[0]
            
        faster_client = multi_clients[client_index]
        work_loads[client_index] += 1 

        if faster_client not in class_cache:
            class_cache[faster_client] = ByteStreamer(faster_client)
        tg_connect = class_cache[faster_client]

        # Get file properties from Telegram (cached in ByteStreamer)
        file_id = await tg_connect.get_file_properties(message_id)
        file_size = file_id.file_size
        original_name = getattr(file_id, "file_name", f"{message_id}.mp4")

        # Range parsing
        from_bytes = 0
        if range_header:
            # Example: Range: bytes=12345-
            try:
                from_bytes_str, _ = range_header.replace("bytes=", "").split("-")
                from_bytes = int(from_bytes_str) if from_bytes_str else 0
            except Exception:
                return web.Response(status=400, text="Bad Range header")

        if from_bytes >= file_size:
            return web.Response(status=416, reason="Range Not Satisfiable")

        offset = from_bytes - (from_bytes % CHUNK_SIZE)
        first_part_cut = from_bytes - offset

        # Prepare headers (force inline + cache-control as requested)
        cors_headers = { 'Access-Control-Allow-Origin': allowed_referer }
        cd_header = f'inline; filename="{urllib.parse.quote_plus(original_name)}"'
        base_headers = {
            "Content-Type": file_id.mime_type,
            "Accept-Ranges": "bytes",
            "Content-Disposition": cd_header,
            "Cache-Control": "private, max-age=3600",
            **cors_headers
        }

        # Content-Range and Content-Length must be set for partial responses
        content_length = str(file_size - from_bytes)
        if range_header:
            base_headers["Content-Range"] = f"bytes {from_bytes}-{file_size - 1}/{file_size}"
            base_headers["Content-Length"] = content_length
            status_code = 206
        else:
            base_headers["Content-Length"] = content_length
            status_code = 200

        resp = web.StreamResponse(status=status_code, headers=base_headers)
        await resp.prepare(request)

        # Determine cache filename
        cache_path = cache_filename_for(message_id, original_name)

        # Helper: get or create lock/event for this message
        if message_id not in cache_locks:
            cache_locks[message_id] = asyncio.Lock()
        if message_id not in cache_events:
            cache_events[message_id] = asyncio.Event()
        if message_id not in cache_writers:
            cache_writers[message_id] = False

        # If cache exists AND fully written (event set) -> serve directly from disk (fast path)
        if os.path.exists(cache_path) and cache_events[message_id].is_set():
            LOGGER.info(f"Serving {message_id} from cache (fully available).")
            # Stream from disk supporting Range reads
            async with aiofiles.open(cache_path, mode='rb') as f:
                await f.seek(from_bytes)
                bytes_sent = 0
                while True:
                    chunk = await f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    try:
                        # If first chunk and first_part_cut present, slice it
                        if bytes_sent == 0 and first_part_cut > 0:
                            chunk_to_write = chunk[first_part_cut:]
                        else:
                            chunk_to_write = chunk
                        await resp.write(chunk_to_write)
                        await resp.drain()
                        bytes_sent += len(chunk_to_write)
                    except (ConnectionError, asyncio.CancelledError, ClientConnectionError, OSError) as e:
                        LOGGER.warning(f"Client disconnected during cached streaming of {message_id}: {e}")
                        return resp
            return resp

        # If cache file exists but writer is active OR cache doesn't exist, we may need to stream while tailing or create writer
        # Acquire lock: only one writer should perform Telegram download -> others will tail the file
        lock = cache_locks[message_id]
        # If lock is free, current requester will try to become writer; otherwise it will tail-read
        became_writer = False
        if not lock.locked():
            await lock.acquire()
            became_writer = True
            cache_writers[message_id] = True
            cache_events[message_id].clear()
            LOGGER.info(f"Acquired writer lock for message {message_id}")
        else:
            # Another writer exists; we will tail-read from disk as it is being written
            LOGGER.info(f"Another writer is active for {message_id}; tailing from cache file while it's written.")
        
        # Writer path: we download from Telegram using yield_file and write to disk + stream to client
        if became_writer:
            body_generator = tg_connect.yield_file(file_id, offset, CHUNK_SIZE, message_id)
            tmp_path = cache_path + ".part"
            try:
                async with aiofiles.open(tmp_path, mode='wb') as wf:
                    is_first_chunk = True
                    bytes_written = 0
                    async for chunk in body_generator:
                        try:
                            # If first chunk and first_part_cut > 0, write full chunk to disk but slice to client
                            await wf.write(chunk)
                            await wf.flush()
                            bytes_written += len(chunk)

                            # Stream to client (honoring first_part_cut)
                            if is_first_chunk and first_part_cut > 0:
                                data_for_client = chunk[first_part_cut:]
                                is_first_chunk = False
                            else:
                                data_for_client = chunk

                            await resp.write(data_for_client)
                            # CRUCIAL: drain after each write; if this raises, we must stop the Telegram download immediately
                            try:
                                await resp.drain()
                            except (ConnectionError, asyncio.CancelledError, ClientConnectionError, OSError) as e:
                                LOGGER.warning(f"Detected client disconnect while streaming {message_id}. Cancelling download to save bandwidth. Err: {e}")
                                # Close/stop the async generator to signal yield_file to stop further Telegram requests
                                try:
                                    await body_generator.aclose()
                                except Exception:
                                    pass
                                # Close file and delete partial
                                try:
                                    await wf.close()
                                except Exception:
                                    pass
                                try:
                                    if os.path.exists(tmp_path):
                                        os.remove(tmp_path)
                                except Exception as ex:
                                    LOGGER.warning(f"Failed to remove partial cache {tmp_path}: {ex}")
                                # Reset writer flags, release lock, notify waiting tailers (they should fail)
                                cache_writers[message_id] = False
                                cache_events[message_id].clear()
                                lock.release()
                                return resp

                        except Exception as write_err:
                            LOGGER.error(f"Error while writing/streaming chunk for {message_id}: {write_err}", exc_info=True)
                            # Attempt to stop generator and cleanup
                            try:
                                await body_generator.aclose()
                            except Exception:
                                pass
                            try:
                                if os.path.exists(tmp_path):
                                    os.remove(tmp_path)
                            except Exception:
                                pass
                            cache_writers[message_id] = False
                            cache_events[message_id].clear()
                            lock.release()
                            raise write_err

                    # Completed writing full file
                    # Atomically rename .part -> final cache file
                    try:
                        await wf.close()
                    except Exception:
                        pass
                os.replace(tmp_path, cache_path)
                cache_events[message_id].set()
                LOGGER.info(f"Cache written for message {message_id} -> {cache_path}")
                cache_writers[message_id] = False
                lock.release()
                return resp

            except Exception as e:
                LOGGER.error(f"Writer encountered error for message {message_id}: {e}", exc_info=True)
                # Cleanup
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass
                cache_writers[message_id] = False
                cache_events[message_id].clear()
                if lock.locked():
                    lock.release()
                # Re-raise to be handled by outer except
                raise

        else:
            # Tail-reading path: cache file might be being created by writer. We open file and read as bytes available.
            # If file does not yet exist, wait until at least partial appears (but do not wait indefinitely).
            tail_start = time.time()
            max_tail_wait = 30  # seconds, reasonable
            bytes_sent = 0
            try:
                # Wait for file to appear (but avoid infinite wait)
                while not os.path.exists(cache_path):
                    # If writer aborted (no writer active and no final file), then consider failing
                    if not cache_writers.get(message_id, False) and not os.path.exists(cache_path):
                        # No writer, and file not present -> try again briefly, then fallback to attempting to be writer
                        await asyncio.sleep(0.15)
                        # small grace; after waiting a bit, attempt to acquire lock to become writer
                        if not cache_locks[message_id].locked():
                            # Try to become writer ourselves (race resolution)
                            await cache_locks[message_id].acquire()
                            cache_writers[message_id] = True
                            cache_events[message_id].clear()
                            became_writer = True
                            break
                    if time.time() - tail_start > max_tail_wait:
                        # fallback: try to become writer if possible
                        if not cache_locks[message_id].locked():
                            await cache_locks[message_id].acquire()
                            cache_writers[message_id] = True
                            cache_events[message_id].clear()
                            became_writer = True
                            break
                        else:
                            # writer exists but file still not created, give up after some time
                            break
                    await asyncio.sleep(0.12)

                # If we became writer due to race, recursively call stream_handler to take writer path
                if became_writer:
                    LOGGER.info(f"Tail-reader promoted to writer for {message_id}")
                    # release and re-enter writer logic by calling the handler function again (recur)
                    # To avoid recursion depth, simply call the writer logic inline: create generator and perform same write-stream cycle
                    body_generator = tg_connect.yield_file(file_id, offset, CHUNK_SIZE, message_id)
                    tmp_path = cache_path + ".part"
                    try:
                        async with aiofiles.open(tmp_path, mode='wb') as wf:
                            is_first_chunk = True
                            bytes_written = 0
                            async for chunk in body_generator:
                                await wf.write(chunk)
                                await wf.flush()
                                bytes_written += len(chunk)

                                if is_first_chunk and first_part_cut > 0:
                                    data_for_client = chunk[first_part_cut:]
                                    is_first_chunk = False
                                else:
                                    data_for_client = chunk

                                await resp.write(data_for_client)
                                try:
                                    await resp.drain()
                                except (ConnectionError, asyncio.CancelledError, ClientConnectionError, OSError) as e:
                                    LOGGER.warning(f"Detected client disconnect while streaming {message_id} (promoted writer). Cancelling download. Err: {e}")
                                    try:
                                        await body_generator.aclose()
                                    except Exception:
                                        pass
                                    try:
                                        await wf.close()
                                    except Exception:
                                        pass
                                    try:
                                        if os.path.exists(tmp_path):
                                            os.remove(tmp_path)
                                    except Exception:
                                        pass
                                    cache_writers[message_id] = False
                                    cache_events[message_id].clear()
                                    cache_locks[message_id].release()
                                    return resp

                            try:
                                await wf.close()
                            except Exception:
                                pass
                        os.replace(tmp_path, cache_path)
                        cache_events[message_id].set()
                        cache_writers[message_id] = False
                        cache_locks[message_id].release()
                        LOGGER.info(f"Cache written (promoted writer) for message {message_id} -> {cache_path}")
                        return resp

                    except Exception as e:
                        LOGGER.error(f"Promoted-writer error for message {message_id}: {e}", exc_info=True)
                        try:
                            if os.path.exists(tmp_path):
                                os.remove(tmp_path)
                        except Exception:
                            pass
                        cache_writers[message_id] = False
                        cache_events[message_id].clear()
                        if cache_locks[message_id].locked():
                            cache_locks[message_id].release()
                        raise

                # Normal tailing: file exists and being written by another writer
                async with aiofiles.open(cache_path, mode='rb') as rf:
                    await rf.seek(from_bytes)
                    while True:
                        chunk = await rf.read(CHUNK_SIZE)
                        if chunk:
                            try:
                                if bytes_sent == 0 and first_part_cut > 0:
                                    await resp.write(chunk[first_part_cut:])
                                    bytes_sent += len(chunk) - first_part_cut
                                else:
                                    await resp.write(chunk)
                                    bytes_sent += len(chunk)
                                await resp.drain()
                            except (ConnectionError, asyncio.CancelledError, ClientConnectionError, OSError) as e:
                                LOGGER.warning(f"Client disconnected while tail-streaming {message_id}: {e}")
                                return resp
                        else:
                            # no new data; if writer still active, wait for more, else end
                            if cache_writers.get(message_id, False):
                                await asyncio.sleep(TAIL_SLEEP)
                                continue
                            else:
                                # writer finished but no more bytes
                                break

                # If we get here we finished tailing
                return resp

            except Exception as e:
                LOGGER.error(f"Tail-reader failed for message {message_id}: {e}", exc_info=True)
                # If we held the lock (unlikely here), release
                try:
                    if cache_locks[message_id].locked():
                        cache_locks[message_id].release()
                except Exception:
                    pass
                raise

    except (FileReferenceExpired, AuthBytesInvalid) as e:
        LOGGER.error(f"FATAL STREAM ERROR for {message_id}: {type(e).__name__}. Client needs to refresh.")
        return web.Response(status=410, text="Stream link expired, please refresh the page.")

    except Exception as e:
        LOGGER.critical(f"Unhandled stream error for {message_id}: {e}", exc_info=True)
        return web.Response(status=500)

    finally:
        if client_index is not None:
            work_loads[client_index] -= 1
            LOGGER.debug(f"Decremented workload for client {client_index}. Current workloads: {work_loads}")

# -------------------------------------------------------------------------------- #
# BOT & CLIENT INITIALIZATION (CLEANED)
# -------------------------------------------------------------------------------- #

main_bot = Client("KeralaCaptainBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN)
# REMOVED: backup_bot (no longer needed)

class TokenParser:
    def parse_from_env(self):
        return {c + 2: t for c, (_, t) in enumerate(filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items())))}

async def initialize_clients():
    multi_clients[0] = main_bot
    work_loads[0] = 0
    # REMOVED: backup_bot logic (no longer needed)
    
    all_tokens = TokenParser().parse_from_env()
    if not all_tokens:
        LOGGER.info("No additional clients found.")
        return

    async def start_client(client_id, token):
        try:
            client = await Client(name=str(client_id), api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=token, no_updates=True, in_memory=True).start()
            work_loads[client_id] = 0
            return client_id, client
        except Exception as e:
            LOGGER.error(f"Failed to start Client {client_id}: {e}")
            return None

    clients = await asyncio.gather(*[start_client(i, token) for i, token in all_tokens.items()])
    multi_clients.update({cid: client for cid, client in clients if client is not None})
    
    if len(multi_clients) > 1:
        LOGGER.info(f"Successfully initialized {len(multi_clients)} clients. Multi-Client mode is ON.")

async def forward_file_safely(message_to_forward: Message):
    """
    Sends a file to the log channel using send_cached_media.
    (Kept for FileReferenceExpired logic)
    (REMOVED: Backup bot logic)
    """
    try:
        media = message_to_forward.document or message_to_forward.video
        if not media:
            LOGGER.error("Message has no media to send.")
            return None
            
        file_id = media.file_id
            
        LOGGER.info(f"Sending cached media for message {message_to_forward.id} using main bot...")
        return await main_bot.send_cached_media(
            chat_id=Config.LOG_CHANNEL_ID,
            file_id=file_id,
            caption=getattr(message_to_forward, 'caption', '')
        )
            
    except Exception as e:
        LOGGER.error(f"Main bot failed to send cached media: {e}")
        return None

# -------------------------------------------------------------------------------- #
# NEW BOT HANDLERS (ADMIN ONLY) - unchanged
# -------------------------------------------------------------------------------- #

# --- NEW: Admin filter ---
admin_only = filters.user(Config.ADMIN_IDS)

@main_bot.on_message(filters.command("start") & filters.private & admin_only)
async def start_command(client, message):
    await message.reply_text(
        "**👋 Welcome, Admin!**\n\nThis is your streaming bot's control panel. What would you like to do?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 Statistics", callback_data="admin_stats")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings")],
            [InlineKeyboardButton("🔄 Restart Bot", callback_data="admin_restart")]
        ])
    )
    # Clear any old conversation state
    await update_user_conversation(message.chat.id, None)

@main_bot.on_callback_query(filters.regex("^admin_stats$") & admin_only)
async def stats_callback(client, cb: CallbackQuery):
    await cb.answer("Fetching stats...")
    
    # Uptime
    uptime = get_readable_time(time.time() - start_time)
    
    # System Stats
    try:
        cpu_usage = psutil.cpu_percent()
        ram_usage = psutil.virtual_memory().percent
        disk_usage = psutil.disk_usage('/').percent
        ram_total = humanbytes(psutil.virtual_memory().total)
    except Exception as e:
        LOGGER.warning(f"Could not fetch system stats: {e}")
        cpu_usage = ram_usage = disk_usage = "N/A"
        ram_total = "N/A"

    # Bot Stats
    active_clients = len(multi_clients)
    workload_str = "\n".join([f"  - Client {cid}: {load} streams" for cid, load in work_loads.items()])

    text = f"**📊 Bot Statistics**\n\n" \
           f"**Uptime:** `{uptime}`\n\n" \
           f"**System:**\n" \
           f"  - CPU: `{cpu_usage}%`\n" \
           f"  - RAM: `{ram_usage}%` (Total: `{ram_total}`)\n" \
           f"  - Disk: `{disk_usage}%`\n\n" \
           f"**Streaming:**\n" \
           f"  - Active Clients: `{active_clients}`\n" \
           f"  - Stream Errors (last min): `{stream_errors}`\n" \
           f"  - Current Workloads:\n{workload_str}"
           
    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]])
    )

@main_bot.on_callback_query(filters.regex("^admin_settings$") & admin_only)
async def settings_callback(client, cb: CallbackQuery):
    await cb.answer()
    current_domain = await get_protected_domain() # Fetch fresh from DB
    
    text = f"**⚙️ Settings**\n\n" \
           f"**Protected Domain:**\n" \
           f"The bot will only allow streaming requests from this URL (Referer).\n\n" \
           f"Current Value: `{current_domain}`"
           
    await cb.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✏️ Set New Domain", callback_data="admin_set_domain")],
            [InlineKeyboardButton("⬅️ Back", callback_data="admin_main_menu")]
        ])
    )

@main_bot.on_callback_query(filters.regex("^admin_set_domain$") & admin_only)
async def set_domain_callback(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, {"stage": "awaiting_domain"})
    await cb.message.edit_text(
        "**✏️ Set New Domain**\n\n"
        "Please send the new domain you want to protect.\n\n"
        "Example: `https://keralacaptain.in` or `keralacaptain.in`",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="admin_cancel_conv")]])
    )

@main_bot.on_callback_query(filters.regex("^admin_restart$") & admin_only)
async def restart_callback(client, cb: CallbackQuery):
    await cb.answer()
    await cb.message.edit_text(
        "**⚠️ Are you sure?**\n\nThis will perform a full restart of the bot. This is required for some updates to take effect.",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("✅ Yes, Restart", callback_data="admin_restart_confirm"),
                InlineKeyboardButton("❌ No, Go Back", callback_data="admin_main_menu")
            ]
        ])
    )

@main_bot.on_callback_query(filters.regex("^admin_restart_confirm$") & admin_only)
async def restart_confirm_callback(client, cb: CallbackQuery):
    await cb.answer("Restarting...")
    await cb.message.edit_text("✅ **Restarting...**\n\nBot will be back online shortly.")
    
    # --- Real Restart Logic ---
    # This replaces the current process with a new one
    try:
        LOGGER.info("RESTART triggered by admin.")
        # Clean up clients before exit
        if main_bot and main_bot.is_connected:
            await main_bot.stop()
        # REMOVED: backup_bot
    except Exception as e:
        LOGGER.error(f"Error during pre-restart cleanup: {e}")
    
    # Execute a new instance of the bot
    os.execl(sys.executable, sys.executable, *sys.argv)

@main_bot.on_callback_query(filters.regex("^(admin_main_menu|admin_cancel_conv)$") & admin_only)
async def main_menu_callback(client, cb: CallbackQuery):
    await cb.answer()
    await update_user_conversation(cb.message.chat.id, None)
    await cb.message.edit_text(
        "**👋 Welcome, Admin!**\n\nThis is your streaming bot's control panel. What would you like to do?",
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
    if not conv: return

    stage = conv.get("stage")
    
    if stage == "awaiting_domain":
        new_domain = message.text.strip()
        
        # Validate domain (simple check)
        if "." not in new_domain or " " in new_domain:
            return await message.reply_text("Invalid format. Please send a valid domain like `keralacaptain.in`.")
        
        try:
            status_msg = await message.reply_text("Saving...")
            saved_domain = await set_protected_domain(new_domain) # This also updates the global var
            
            await status_msg.edit_text(
                f"✅ **Success!**\n\nProtected domain has been updated to:\n`{saved_domain}`",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Settings", callback_data="admin_settings")]])
            )
            await update_user_conversation(chat_id, None) # Clear state
            
        except Exception as e:
            await status_msg.edit_text(f"❌ **Error!**\nCould not save domain: `{e}`")


# -------------------------------------------------------------------------------- #
# CACHE CLEANER TASK
# -------------------------------------------------------------------------------- #

async def cache_cleaner_loop():
    """Runs every CACHE_CLEAN_INTERVAL seconds and deletes files older than CACHE_MAX_AGE_SECONDS."""
    while True:
        try:
            now = time.time()
            for fn in os.listdir(CACHE_DIR):
                fpath = os.path.join(CACHE_DIR, fn)
                try:
                    if not os.path.isfile(fpath):
                        continue
                    mtime = os.path.getmtime(fpath)
                    age = now - mtime
                    # Remove .part files older than a grace (they indicate failed writes)
                    if fn.endswith(".part"):
                        if age > 60: # remove partials older than 1 minute
                            os.remove(fpath)
                            LOGGER.info(f"Removed stale partial cache file: {fpath}")
                        continue
                    if age > CACHE_MAX_AGE_SECONDS:
                        os.remove(fpath)
                        LOGGER.info(f"Removed cache file older than {CACHE_MAX_AGE_SECONDS}s: {fpath}")
                except Exception as e:
                    LOGGER.warning(f"Error while cleaning cache file {fpath}: {e}")
        except Exception as e:
            LOGGER.error(f"Cache cleaner loop error: {e}", exc_info=True)
        await asyncio.sleep(CACHE_CLEAN_INTERVAL)

# -------------------------------------------------------------------------------- #
# APPLICATION LIFECYCLE (CLEANED)
# -------------------------------------------------------------------------------- #

async def ping_server():
    """Pings the server to keep it alive on platforms like Heroku."""
    while True:
        await asyncio.sleep(Config.PING_INTERVAL)
        try:
            async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
                async with session.get(Config.STREAM_URL) as resp:
                    LOGGER.info(f"Pinged server with status: {resp.status}")
        except Exception as e:
            LOGGER.warning(f"Failed to ping server: {e}")

if __name__ == "__main__":
    # Globals to be set by startup logic so shutdown can cleanup them
    web_runner = None
    site_obj = None
    cache_cleaner_task_ref = None
    ping_task_ref = None

    async def main_startup_wrapper():
        """
        Wrapper that starts services and exposes runner & background tasks
        so the shutdown handler can cleanup them properly.
        """
        global web_runner, site_obj, cache_cleaner_task_ref, ping_task_ref

        # Run your existing startup logic (it sets up DB, starts bot, initializes clients, etc.)
        await main_startup_shutdown_logic()  # This will create web_app and site

        # The previous main_startup_shutdown_logic uses web_server internally and starts the site.
        # We must try to find the runner & tasks from the current event loop tasks.
        # However to keep minimal edits, we will capture relevant tasks that we started elsewhere:
        # - cache_cleaner_loop and ping_server are created with asyncio.create_task() earlier.
        # We'll try to find them in all_tasks by name or coroutine.
        all_tasks = {t.get_name(): t for t in asyncio.all_tasks()}

        # Attempt to discover tasks we created earlier by function name
        for t in asyncio.all_tasks():
            coro = t.get_coro()
            if hasattr(coro, "__name__"):
                cname = coro.__name__
                if cname == "cache_cleaner_loop":
                    cache_cleaner_task_ref = t
                elif cname == "ping_server":
                    ping_task_ref = t

        # Attempt to find aiohttp runner (if you stored it elsewhere, keep that store; else we can't reliably extract it here)
        # If your startup function stored runner in a variable available here, use that. Otherwise not required.
        return

    async def graceful_shutdown(sig=None):
        """
        Clean shutdown sequence: cancel background tasks, stop bots, cleanup web runner,
        cancel remaining tasks and only then stop the loop.
        """
        LOGGER.info(f"Shutdown initiated. Signal: {sig.name if isinstance(sig, signal.Signals) else sig}")

        # 1) Stop ping & cache cleaner tasks (if any)
        tasks_to_wait = []
        for tname, tref in (("cache_cleaner", cache_cleaner_task_ref), ("ping_server", ping_task_ref)):
            if tref is not None and not tref.done():
                LOGGER.info(f"Cancelling background task: {tname}")
                tref.cancel()
                tasks_to_wait.append(tref)

        if tasks_to_wait:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks_to_wait, return_exceptions=True), timeout=10)
            except asyncio.TimeoutError:
                LOGGER.warning("Timeout while waiting for background tasks to cancel.")

        # 2) Cleanup aiohttp runner if it exists
        try:
            # If you kept runner in a variable, call runner.cleanup() here.
            # Attempt to find a runner task by attribute: look for web.AppRunner instances in globals
            # (If you saved runner in a global earlier, prefer using that.)
            if 'runner' in globals() and globals().get('runner') is not None:
                r = globals().get('runner')
                LOGGER.info("Cleaning up aiohttp runner...")
                try:
                    await r.cleanup()
                except Exception as e:
                    LOGGER.warning(f"Error during runner cleanup: {e}")
        except Exception as e:
            LOGGER.debug(f"No web runner to cleanup or cleanup failed: {e}")

        # 3) Stop all pyrogram clients (main_bot + any additional clients)
        try:
            LOGGER.info("Stopping pyrogram clients...")
            stop_tasks = []
            for cid, c in list(multi_clients.items()):
                try:
                    if c and getattr(c, "is_connected", False):
                        LOGGER.info(f"Stopping client {cid}...")
                        stop_tasks.append(c.stop())
                except Exception as e:
                    LOGGER.warning(f"Error scheduling stop for client {cid}: {e}")
            if stop_tasks:
                try:
                    await asyncio.wait_for(asyncio.gather(*stop_tasks, return_exceptions=True), timeout=20)
                except asyncio.TimeoutError:
                    LOGGER.warning("Timeout while waiting for pyrogram clients to stop.")
        except Exception as e:
            LOGGER.error(f"Error stopping pyrogram clients: {e}", exc_info=True)

        # 4) Cancel any remaining tasks except current task
        current_task = asyncio.current_task()
        remaining = [t for t in asyncio.all_tasks() if t is not current_task]
        if remaining:
            LOGGER.info(f"Cancelling {len(remaining)} remaining tasks...")
            for t in remaining:
                t.cancel()
            try:
                await asyncio.wait_for(asyncio.gather(*remaining, return_exceptions=True), timeout=20)
            except asyncio.TimeoutError:
                LOGGER.warning("Timeout while waiting for remaining tasks to cancel.")
            except Exception as e:
                LOGGER.warning(f"Error while awaiting remaining tasks: {e}")

        LOGGER.info("Shutdown complete. Stopping event loop.")

    # Signal handler that schedules graceful_shutdown
    def _signal_handler(sig):
        LOGGER.info(f"Signal received: {sig.name}")
        # Schedule graceful_shutdown on the running loop
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(graceful_shutdown(sig))
        except RuntimeError:
            # if no running loop, fallback to calling graceful_shutdown via run_until_complete
            LOGGER.warning("No running loop to schedule graceful shutdown.")

    # Register signal handlers (works on Unix)
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop = asyncio.get_event_loop()
            loop.add_signal_handler(s, lambda s=s: _signal_handler(s))
        except NotImplementedError:
            # Some platforms (Windows, or certain containers) may not implement add_signal_handler
            LOGGER.warning("Signal handlers not supported in this environment.")

    # Run the application using asyncio.run to ensure loop lifecycle is managed properly.
    try:
        asyncio.run(main_startup_wrapper())
        # After startup wrapper returns we sleep forever until a shutdown signal triggers graceful_shutdown
        # Use a long-running waiter which will be cancelled by graceful_shutdown
        asyncio.run(asyncio.Event().wait())
    except KeyboardInterrupt:
        LOGGER.info("KeyboardInterrupt received. Exiting...")
    except Exception as e:
        LOGGER.critical(f"A critical error forced the application to stop: {e}", exc_info=True)
    finally:
        LOGGER.info("Exiting main program.")
