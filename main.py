# main.py - KeralaCaptain Bot - Pure Streaming Engine V4.1 (Complete with caching, disconnect detection, graceful shutdown)

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
import psutil  # For stats
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
# KeralaCaptain Bot - Pure Streaming Engine V4.1 (Final)
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
    ADMIN_IDS = list(int(admin_id) for admin_id in os.environ.get("ADMIN_IDS", "6644681404").split())
    PROTECTED_DOMAIN = os.environ.get("PROTECTED_DOMAIN", "https://www.keralacaptain.shop/").rstrip('/') + '/'
    MONGO_URI = os.environ.get("MONGO_URI", "")
    LOG_CHANNEL_ID = int(os.environ.get("LOG_CHANNEL_ID", 0))
    STREAM_URL = os.environ.get("STREAM_URL", "").rstrip('/')
    PORT = int(os.environ.get("PORT", 8080))
    PING_INTERVAL = int(os.environ.get("PING_INTERVAL", 1200))
    ON_HEROKU = 'DYNO' in os.environ

# Validate essential configurations
required_vars = [
    Config.API_ID, Config.API_HASH, Config.BOT_TOKEN,
    Config.MONGO_URI, Config.LOG_CHANNEL_ID, Config.STREAM_URL,
    Config.ADMIN_IDS
]
if not all(required_vars) or Config.ADMIN_IDS == [0]:
    LOGGER.critical("FATAL: One or more required variables (API_ID, API_HASH, BOT_TOKEN, MONGO_URI, LOG_CHANNEL_ID, STREAM_URL, ADMIN_IDS) are missing. Cannot start.")
    exit(1)

# Global dynamic domain value (can be updated via admin commands)
CURRENT_PROTECTED_DOMAIN = Config.PROTECTED_DOMAIN

# -------------------------------------------------------------------------------- #
# CACHING / STREAM CONTROL SETTINGS
# -------------------------------------------------------------------------------- #

CACHE_DIR = "./cache"
CACHE_MAX_AGE_SECONDS = 30 * 60       # 30 minutes
CACHE_CLEAN_INTERVAL = 10 * 60        # 10 minutes
CHUNK_SIZE = 1024 * 1024              # constant chunk size (1 MiB) - DO NOT change
TAIL_SLEEP = 0.12                     # sleep while waiting for more bytes during tailing

os.makedirs(CACHE_DIR, exist_ok=True)

# Per-message coordination structures
cache_locks = {}        # message_id -> asyncio.Lock()
cache_events = {}       # message_id -> asyncio.Event()  (set when cache fully written)
cache_writers = {}      # message_id -> bool (True if writer active)

# Blocklist regex for download managers (case-insensitive)
DOWNLOAD_AGENT_PATTERNS = re.compile(
    r"(idman|internet download manager|adm:|advanced download manager|1dm|solo browser|download manager|getright|flashget|wget|curl)",
    flags=re.IGNORECASE
)

# -------------------------------------------------------------------------------- #
# HELPER FUNCTIONS & UTILITIES
# -------------------------------------------------------------------------------- #

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

def cache_filename_for(message_id: int, original_name: str) -> str:
    _, ext = os.path.splitext(original_name or "")
    if not ext:
        ext = ".mp4"
    safe_name = f"{message_id}{ext}"
    return os.path.join(CACHE_DIR, safe_name)

def is_blocked_user_agent(ua: str) -> bool:
    if not ua:
        return False
    return bool(DOWNLOAD_AGENT_PATTERNS.search(ua))

# -------------------------------------------------------------------------------- #
# DATABASE OPERATIONS
# -------------------------------------------------------------------------------- #

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
    update_query = {"$set": {"message_ids": new_message_ids, "stream_link": new_stream_link}}
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
    global CURRENT_PROTECTED_DOMAIN
    try:
        doc = await settings_collection.find_one({"_id": "bot_settings"})
        if doc and "protected_domain" in doc:
            return doc["protected_domain"]
    except Exception as e:
        LOGGER.error(f"Could not fetch domain from DB: {e}. Using default.")
    return Config.PROTECTED_DOMAIN

async def set_protected_domain(new_domain: str):
    global CURRENT_PROTECTED_DOMAIN
    if not (new_domain.startswith("https://") or new_domain.startswith("http://")):
        new_domain = "https://" + new_domain
    if not new_domain.endswith('/'):
        new_domain += '/'
    await settings_collection.update_one({"_id": "bot_settings"}, {"$set": {"protected_domain": new_domain}}, upsert=True)
    CURRENT_PROTECTED_DOMAIN = new_domain
    LOGGER.info(f"Protected domain updated in DB: {new_domain}")
    return new_domain

# -------------------------------------------------------------------------------- #
# STREAMING ENGINE
# -------------------------------------------------------------------------------- #

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
        self.cached_file_ids = {}
        self.session_cache = {}
        asyncio.create_task(self.clean_cache_regularly())

    async def clean_cache_regularly(self):
        while True:
            await asyncio.sleep(1200)
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
            if time.time() - ts < 300:
                LOGGER.debug(f"Reusing TTL-cached media session for DC {dc_id}")
                return session

        if media_session:
            try:
                await media_session.send(raw.functions.help.GetConfig(), timeout=10)
                self.session_cache[dc_id] = (media_session, time.time())
                LOGGER.debug(f"Reusing pinged media session for DC {dc_id}")
                return media_session
            except Exception as e:
                LOGGER.warning(f"Existing media session for DC {dc_id} is stale: {e}. Recreating.")
                try:
                    await media_session.stop()
                except:
                    pass
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
        self.session_cache[dc_id] = (media_session, time.time())
        return media_session

    @staticmethod
    def get_location(file_id: FileId):
        if file_id.file_type == FileType.PHOTO:
            return InputPhotoFileLocation(id=file_id.media_id, access_hash=file_id.access_hash, file_reference=file_id.file_reference, thumb_size=file_id.thumbnail_size)
        else:
            return InputDocumentFileLocation(id=file_id.media_id, access_hash=file_id.access_hash, file_reference=file_id.file_reference, thumb_size=file_id.thumbnail_size)

    async def yield_file(self, file_id: FileId, offset: int, chunk_size: int, message_id: int):
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
                retry_count += 1
                if retry_count > max_retries:
                    raise
                LOGGER.warning(f"FileReferenceExpired for msg {message_id}, retry {retry_count}/{max_retries}. Refreshing...")

                original_msg = await self.client.get_messages(Config.LOG_CHANNEL_ID, message_id)
                if original_msg:
                    refreshed_msg = await forward_file_safely(original_msg)
                    if refreshed_msg:
                        new_file_id = await self.get_file_properties(refreshed_msg.id)
                        self.cached_file_ids[message_id] = new_file_id

                        post_id = await get_post_id_from_msg_id(message_id)
                        if post_id:
                            media_doc = await get_media_by_post_id(post_id)
                            if media_doc:
                                old_qualities = media_doc['message_ids']
                                quality_key = next((k for k, v in old_qualities.items() if v == message_id), None)
                                new_qualities = old_qualities
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
                LOGGER.warning(f"FloodWait of {e.value} seconds on get_file. Waiting...")
                await asyncio.sleep(e.value)
                continue

# -------------------------------------------------------------------------------- #
# ROUTES & STREAM HANDLER
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
    cache_files_count = 0
    try:
        cache_files = os.listdir(CACHE_DIR)
        cache_files_count = len([f for f in cache_files if os.path.isfile(os.path.join(CACHE_DIR, f))])
    except Exception:
        cache_files_count = 0
    return web.json_response({
        "status": "ok",
        "active_clients": active_sessions,
        "cache_files": cache_files_count,
        "stream_errors_last_min": stream_errors,
        "workloads": work_loads,
    })

@routes.get("/favicon.ico")
async def favicon_handler(request):
    return web.Response(status=204)

@routes.get(r"/stream/{message_id:\d+}")
async def stream_handler(request: web.Request):
    client_index = None
    message_id = None
    try:
        referer = request.headers.get('Referer')
        allowed_referer = CURRENT_PROTECTED_DOMAIN

        if not referer or not referer.startswith(allowed_referer):
            LOGGER.warning(f"Blocked hotlink attempt. Referer: {referer}. Allowed: {allowed_referer}")
            return web.Response(status=403, text="403 Forbidden: Direct access is not allowed.")

        ua = request.headers.get("User-Agent", "")
        if is_blocked_user_agent(ua):
            LOGGER.warning(f"Blocked download-manager UA: {ua}")
            return web.Response(status=403, text="403 Forbidden: Download managers are not allowed.")

        message_id = int(request.match_info['message_id'])
        range_header = request.headers.get("Range", None)

        # Choose client with least load
        min_load = min(work_loads.values()) if work_loads else 0
        candidates = [cid for cid, load in work_loads.items() if load == min_load] if work_loads else [0]

        global next_client_idx
        if len(candidates) > 1:
            client_index = candidates[next_client_idx % len(candidates)]
            next_client_idx += 1
        else:
            client_index = candidates[0]

        if client_index not in multi_clients:
            # Fallback to main client index 0
            client_index = 0
        faster_client = multi_clients[client_index]
        work_loads[client_index] = work_loads.get(client_index, 0) + 1

        if faster_client not in class_cache:
            class_cache[faster_client] = ByteStreamer(faster_client)
        tg_connect = class_cache[faster_client]

        file_id = await tg_connect.get_file_properties(message_id)
        file_size = file_id.file_size
        original_name = getattr(file_id, "file_name", f"{message_id}.mp4")

        from_bytes = 0
        if range_header:
            try:
                from_bytes_str, _ = range_header.replace("bytes=", "").split("-")
                from_bytes = int(from_bytes_str) if from_bytes_str else 0
            except Exception:
                return web.Response(status=400, text="Bad Range header")

        if from_bytes >= file_size:
            return web.Response(status=416, reason="Range Not Satisfiable")

        offset = from_bytes - (from_bytes % CHUNK_SIZE)
        first_part_cut = from_bytes - offset

        cors_headers = {'Access-Control-Allow-Origin': allowed_referer}
        cd_header = f'inline; filename="{urllib.parse.quote_plus(original_name)}"'
        base_headers = {
            "Content-Type": file_id.mime_type,
            "Accept-Ranges": "bytes",
            "Content-Disposition": cd_header,
            "Cache-Control": "private, max-age=3600",
            **cors_headers
        }

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

        cache_path = cache_filename_for(message_id, original_name)

        if message_id not in cache_locks:
            cache_locks[message_id] = asyncio.Lock()
        if message_id not in cache_events:
            cache_events[message_id] = asyncio.Event()
        if message_id not in cache_writers:
            cache_writers[message_id] = False

        # If cache exists and is marked complete
        if os.path.exists(cache_path) and cache_events[message_id].is_set():
            LOGGER.info(f"Serving {message_id} from cache.")
            async with aiofiles.open(cache_path, mode='rb') as f:
                await f.seek(from_bytes)
                bytes_sent = 0
                while True:
                    chunk = await f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    try:
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

        # Attempt to become writer if lock free
        lock = cache_locks[message_id]
        became_writer = False
        if not lock.locked():
            await lock.acquire()
            became_writer = True
            cache_writers[message_id] = True
            cache_events[message_id].clear()
            LOGGER.info(f"Acquired writer lock for message {message_id}")
        else:
            LOGGER.info(f"Another writer active for {message_id}; tailing from partial cache if available.")

        if became_writer:
            body_generator = tg_connect.yield_file(file_id, offset, CHUNK_SIZE, message_id)
            tmp_path = cache_path + ".part"
            try:
                async with aiofiles.open(tmp_path, mode='wb') as wf:
                    is_first_chunk = True
                    bytes_written = 0
                    async for chunk in body_generator:
                        try:
                            # write to disk
                            await wf.write(chunk)
                            await wf.flush()
                            bytes_written += len(chunk)

                            # prepare slice for client
                            if is_first_chunk and first_part_cut > 0:
                                data_for_client = chunk[first_part_cut:]
                                is_first_chunk = False
                            else:
                                data_for_client = chunk

                            await resp.write(data_for_client)
                            try:
                                await resp.drain()
                            except (ConnectionError, asyncio.CancelledError, ClientConnectionError, OSError) as e:
                                LOGGER.warning(f"Detected client disconnect while streaming {message_id}. Cancelling download. Err: {e}")
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
                                except Exception as ex:
                                    LOGGER.warning(f"Failed to remove partial cache {tmp_path}: {ex}")
                                cache_writers[message_id] = False
                                cache_events[message_id].clear()
                                lock.release()
                                return resp

                        except Exception as write_err:
                            LOGGER.error(f"Error while writing/streaming chunk for {message_id}: {write_err}", exc_info=True)
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

                try:
                    # finalize
                    os.replace(tmp_path, cache_path)
                except Exception as e:
                    LOGGER.warning(f"Failed to atomically replace partial cache: {e}")
                    # fallback: try rename using os.rename
                    try:
                        os.rename(tmp_path, cache_path)
                    except Exception as e2:
                        LOGGER.error(f"Failed to move partial cache to final: {e2}")

                cache_events[message_id].set()
                LOGGER.info(f"Cache written for message {message_id} -> {cache_path}")
                cache_writers[message_id] = False
                lock.release()
                return resp

            except Exception as e:
                LOGGER.error(f"Writer encountered error for message {message_id}: {e}", exc_info=True)
                try:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)
                except Exception:
                    pass
                cache_writers[message_id] = False
                cache_events[message_id].clear()
                if lock.locked():
                    lock.release()
                raise

        else:
            # Tail-reading path
            tail_start = time.time()
            max_tail_wait = 30
            bytes_sent = 0
            try:
                while not os.path.exists(cache_path):
                    if not cache_writers.get(message_id, False) and not os.path.exists(cache_path):
                        await asyncio.sleep(0.15)
                        if not cache_locks[message_id].locked():
                            await cache_locks[message_id].acquire()
                            cache_writers[message_id] = True
                            cache_events[message_id].clear()
                            became_writer = True
                            break
                    if time.time() - tail_start > max_tail_wait:
                        if not cache_locks[message_id].locked():
                            await cache_locks[message_id].acquire()
                            cache_writers[message_id] = True
                            cache_events[message_id].clear()
                            became_writer = True
                            break
                        else:
                            break
                    await asyncio.sleep(0.12)

                if became_writer:
                    LOGGER.info(f"Tail-reader promoted to writer for {message_id}")
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

                # Normal tailing
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
                            if cache_writers.get(message_id, False):
                                await asyncio.sleep(TAIL_SLEEP)
                                continue
                            else:
                                break

                return resp

            except Exception as e:
                LOGGER.error(f"Tail-reader failed for message {message_id}: {e}", exc_info=True)
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
        if client_index is not None and client_index in work_loads:
            work_loads[client_index] -= 1
            LOGGER.debug(f"Decremented workload for client {client_index}. Current workloads: {work_loads}")

async def web_server():
    web_app = web.Application(client_max_size=30_000_000)
    web_app.add_routes(routes)
    return web_app

# -------------------------------------------------------------------------------- #
# BOT & CLIENT INITIALIZATION
# -------------------------------------------------------------------------------- #

main_bot = Client("KeralaCaptainBot", api_id=Config.API_ID, api_hash=Config.API_HASH, bot_token=Config.BOT_TOKEN)

class TokenParser:
    def parse_from_env(self):
        return {c + 2: t for c, (_, t) in enumerate(filter(lambda n: n[0].startswith("MULTI_TOKEN"), sorted(os.environ.items())))}

async def initialize_clients():
    multi_clients[0] = main_bot
    work_loads[0] = 0

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
# ADMIN BOT HANDLERS
# -------------------------------------------------------------------------------- #

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
    await update_user_conversation(message.chat.id, None)

@main_bot.on_callback_query(filters.regex("^admin_stats$") & admin_only)
async def stats_callback(client, cb: CallbackQuery):
    await cb.answer("Fetching stats...")
    uptime = get_readable_time(time.time() - start_time)
    try:
        cpu_usage = psutil.cpu_percent()
        ram_usage = psutil.virtual_memory().percent
        disk_usage = psutil.disk_usage('/').percent
        ram_total = humanbytes(psutil.virtual_memory().total)
    except Exception as e:
        LOGGER.warning(f"Could not fetch system stats: {e}")
        cpu_usage = ram_usage = disk_usage = "N/A"
        ram_total = "N/A"

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
    current_domain = await get_protected_domain()
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
    try:
        LOGGER.info("RESTART triggered by admin.")
        if main_bot and main_bot.is_connected:
            await main_bot.stop()
    except Exception as e:
        LOGGER.error(f"Error during pre-restart cleanup: {e}")
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
        if "." not in new_domain or " " in new_domain:
            return await message.reply_text("Invalid format. Please send a valid domain like `keralacaptain.in`.")
        try:
            status_msg = await message.reply_text("Saving...")
            saved_domain = await set_protected_domain(new_domain)
            await status_msg.edit_text(
                f"✅ **Success!**\n\nProtected domain has been updated to:\n`{saved_domain}`",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Settings", callback_data="admin_settings")]])
            )
            await update_user_conversation(chat_id, None)
        except Exception as e:
            await status_msg.edit_text(f"❌ **Error!**\nCould not save domain: `{e}`")

# -------------------------------------------------------------------------------- #
# BACKGROUND TASKS
# -------------------------------------------------------------------------------- #

async def cache_cleaner_loop():
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
                    if fn.endswith(".part"):
                        if age > 60:
                            os.remove(fpath)
                            LOGGER.info(f"Removed stale partial cache file: {fpath}")
                        continue
                    if age > CACHE_MAX_AGE_SECONDS:
                        os.remove(fpath)
                        LOGGER.info(f"Removed cache file older than threshold: {fpath}")
                except Exception as e:
                    LOGGER.warning(f"Error while cleaning cache file {fpath}: {e}")
        except Exception as e:
            LOGGER.error(f"Cache cleaner loop error: {e}", exc_info=True)
        await asyncio.sleep(CACHE_CLEAN_INTERVAL)

async def ping_server():
    while True:
        await asyncio.sleep(Config.PING_INTERVAL)
        try:
            async with aiohttp.ClientSession(timeout=ClientTimeout(total=10)) as session:
                async with session.get(Config.STREAM_URL) as resp:
                    LOGGER.info(f"Pinged server with status: {resp.status}")
        except Exception as e:
            LOGGER.warning(f"Failed to ping server: {e}")

# -------------------------------------------------------------------------------- #
# APPLICATION LIFECYCLE (STARTUP & GRACEFUL SHUTDOWN)
# -------------------------------------------------------------------------------- #

if __name__ == "__main__":
    # Global references for cleanup
    runner: web.AppRunner | None = None
    site_obj = None
    cache_cleaner_task = None
    ping_task = None

    async def main_startup_shutdown_logic():
        global runner, site_obj, cache_cleaner_task, ping_task, CURRENT_PROTECTED_DOMAIN

        LOGGER.info("Application starting up...")

        # Load protected domain
        try:
            LOGGER.info("Fetching protected domain from database...")
            CURRENT_PROTECTED_DOMAIN = await get_protected_domain()
            LOGGER.info(f"Protected domain loaded: {CURRENT_PROTECTED_DOMAIN}")
        except Exception as e:
            LOGGER.warning(f"Could not load protected domain: {e}")

        # Ensure DB indexes
        try:
            await media_collection.create_index("tmdb_id", unique=True)
            await media_collection.create_index("wp_post_id", unique=True)
            LOGGER.info("DB indexes ensured.")
        except Exception as e:
            LOGGER.warning(f"Failed to ensure DB indexes: {e}")

        # Start main bot
        try:
            await main_bot.start()
            bot_info = await main_bot.get_me()
            LOGGER.info(f"Main Bot @{bot_info.username} started.")
        except FloodWait as e:
            LOGGER.warning(f"FloodWait on main bot startup: waiting {e.value}s...")
            await asyncio.sleep(e.value + 5)
            await main_bot.start()
            bot_info = await main_bot.get_me()
            LOGGER.info(f"Main Bot @{bot_info.username} started after wait.")
        except Exception as e:
            LOGGER.critical(f"Failed to start main bot: {e}", exc_info=True)
            raise

        # Initialize other clients
        try:
            await initialize_clients()
        except Exception as e:
            LOGGER.warning(f"Error initializing additional clients: {e}")

        # Start ping task if needed
        if Config.ON_HEROKU:
            try:
                ping_task = asyncio.create_task(ping_server(), name="ping_server")
                LOGGER.info("Started ping_server background task.")
            except Exception as e:
                LOGGER.warning(f"Could not start ping task: {e}")

        # Start cache cleaner
        try:
            cache_cleaner_task = asyncio.create_task(cache_cleaner_loop(), name="cache_cleaner_loop")
            LOGGER.info("Started cache_cleaner_loop background task.")
        except Exception as e:
            LOGGER.warning(f"Could not start cache cleaner: {e}")

        # Start web server
        try:
            web_app = await web_server()
            runner = web.AppRunner(web_app)
            await runner.setup()
            site_obj = web.TCPSite(runner, "0.0.0.0", Config.PORT)
            await site_obj.start()
            LOGGER.info(f"Web server started on port {Config.PORT}.")
        except Exception as e:
            LOGGER.critical(f"Failed to start web server: {e}", exc_info=True)
            try:
                if main_bot and main_bot.is_connected:
                    await main_bot.stop()
            except Exception:
                pass
            raise

        # Send startup notification (best-effort)
        try:
            await main_bot.send_message(Config.ADMIN_IDS[0], "**✅ Bot has restarted and all services are online!**")
        except Exception as e:
            LOGGER.debug(f"Could not send startup message: {e}")

        # Block and wait until shutdown handler triggers
        LOGGER.info("Startup complete. Entering idle state.")
        await asyncio.Event().wait()

    async def shutdown_handler(sig):
        global runner, site_obj, cache_cleaner_task, ping_task
        LOGGER.info(f"Shutdown initiated. Signal: {sig.name if hasattr(sig, 'name') else sig}")

        # 1) Cancel background tasks
        tasks_to_cancel = []
        if ping_task is not None and not ping_task.done():
            LOGGER.info("Cancelling ping task...")
            ping_task.cancel()
            tasks_to_cancel.append(ping_task)
        if cache_cleaner_task is not None and not cache_cleaner_task.done():
            LOGGER.info("Cancelling cache cleaner task...")
            cache_cleaner_task.cancel()
            tasks_to_cancel.append(cache_cleaner_task)

        if tasks_to_cancel:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks_to_cancel, return_exceptions=True), timeout=10)
            except asyncio.TimeoutError:
                LOGGER.warning("Timeout while waiting for background tasks to cancel.")
            except Exception as e:
                LOGGER.warning(f"Error while cancelling background tasks: {e}")

        # 2) Cleanup web runner
        if runner is not None:
            try:
                LOGGER.info("Cleaning up aiohttp runner...")
                await runner.cleanup()
            except Exception as e:
                LOGGER.warning(f"Error during runner.cleanup(): {e}")

        # 3) Stop pyrogram clients
        try:
            LOGGER.info("Stopping pyrogram clients...")
            stop_coros = []
            for cid, client in list(multi_clients.items()):
                try:
                    if client and getattr(client, "is_connected", False):
                        LOGGER.info(f"Stopping client {cid}...")
                        stop_coros.append(client.stop())
                except Exception as ex:
                    LOGGER.warning(f"Error scheduling stop for client {cid}: {ex}")
            if stop_coros:
                try:
                    await asyncio.wait_for(asyncio.gather(*stop_coros, return_exceptions=True), timeout=20)
                except asyncio.TimeoutError:
                    LOGGER.warning("Timeout while waiting for pyrogram clients to stop.")
        except Exception as e:
            LOGGER.error(f"Error while stopping pyrogram clients: {e}", exc_info=True)

        # 4) Cancel remaining tasks
        current = asyncio.current_task()
        remaining = [t for t in asyncio.all_tasks() if t is not current]
        if remaining:
            LOGGER.info(f"Cancelling {len(remaining)} remaining tasks...")
            for t in remaining:
                t.cancel()
            try:
                await asyncio.wait_for(asyncio.gather(*remaining, return_exceptions=True), timeout=20)
            except asyncio.TimeoutError:
                LOGGER.warning("Timeout while waiting for remaining tasks to cancel.")
            except Exception as e:
                LOGGER.debug(f"Issue while awaiting remaining tasks: {e}")

        LOGGER.info("Graceful shutdown complete. Stopping event loop.")
        loop = asyncio.get_event_loop()
        loop.stop()

    # Register signal handlers
    loop = asyncio.get_event_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(s, lambda sig=s: asyncio.create_task(shutdown_handler(sig)))
        except NotImplementedError:
            LOGGER.warning("Signal handlers not supported in this environment.")

    # Run startup then wait forever until shutdown_handler stops the loop
    try:
        LOGGER.info("Boot sequence: starting services...")
        loop.run_until_complete(main_startup_shutdown_logic())
        loop.run_forever()
    except KeyboardInterrupt:
        LOGGER.info("KeyboardInterrupt received - shutting down.")
    except Exception as e:
        LOGGER.critical(f"A critical error forced the application to stop: {e}", exc_info=True)
    finally:
        try:
            if not loop.is_closed():
                loop.stop()
        except Exception:
            pass
        LOGGER.info("Application has exited.")
