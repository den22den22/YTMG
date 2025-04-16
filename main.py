# -*- coding: utf-8 -*-
# --- START OF FILE main.py ---

# =============================================================================
#                            IMPORTS & SETUP
# =============================================================================

import asyncio
import csv
import datetime
import functools
import glob
import json
import logging
import os
import platform
import re
import shutil
import subprocess
import traceback
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple, Type, Union
from urllib.parse import urlparse

import psutil
import requests
import telethon
import yt_dlp
from PIL import Image, UnidentifiedImageError
from telethon import TelegramClient, events, functions, types
from ytmusicapi import YTMusic

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot_log.txt", mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Helper function for absolute paths ---
def get_script_dir():
    """Returns the absolute path to the directory containing this script."""
    try:
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        logger.warning("__file__ not defined, using current working directory for data/config files.")
        return os.getcwd()

SCRIPT_DIR = get_script_dir()

# =============================================================================
#                            CONFIGURATION LOADING
# =============================================================================

# --- Environment variables for Telegram API ---
TELEGRAM_API_ID = os.environ.get('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.environ.get('TELEGRAM_API_HASH')

if not all([TELEGRAM_API_ID, TELEGRAM_API_HASH]):
    logger.critical("CRITICAL ERROR: Telegram API ID/Hash environment variables not set.")
    exit(1)

# --- Telegram client initialization ---
try:
    session_path = os.path.join(SCRIPT_DIR, 'telegram_session')
    client = TelegramClient(session_path, int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
except ValueError:
    logger.critical("CRITICAL ERROR: TELEGRAM_API_ID must be an integer.")
    exit(1)
except Exception as e:
    logger.critical(f"CRITICAL ERROR: Failed to initialize TelegramClient: {e}")
    exit(1)

# --- yt-dlp Options ---
def load_ydl_opts(config_file: str = 'dlp.conf') -> Dict:
    """Loads yt-dlp options from a JSON file, merging with defaults."""
    default_opts = {
        'format': 'bestaudio[ext=m4a]/best[ext=m4a]',
        'outtmpl': '%(title)s [%(channel)s] [%(id)s].%(ext)s',
        'audioformat': 'm4a',
        'noplaylist': True,
        'extract_flat': 'discard_in_playlist',
        'ignoreerrors': True,
        'quiet': True,
        'add_metadata': True,
        'embed_metadata': True,
        'embed_thumbnail': True,
        'embed_chapters': True,
        'embed_info_json': True,
        'parse_metadata': [],
        'postprocessors': [
            {
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'm4a'
            }
        ]
    }
    absolute_config_path = os.path.join(SCRIPT_DIR, config_file)
    logger.info(f"Attempting to load yt-dlp options from: {absolute_config_path}")
    try:
        with open(absolute_config_path, 'r', encoding='utf-8') as f:
            opts = json.load(f)
            logger.info(f"Loaded yt-dlp options from {absolute_config_path}")
            merged_opts = default_opts.copy()
            merged_opts.update(opts)

            if 'outtmpl' in merged_opts and not os.path.isabs(merged_opts['outtmpl']):
                 outtmpl_path = merged_opts['outtmpl']
                 if not os.path.splitdrive(outtmpl_path)[0] and not os.path.isabs(outtmpl_path):
                     merged_opts['outtmpl'] = os.path.join(SCRIPT_DIR, outtmpl_path)
                     logger.info(f"Made yt-dlp outtmpl relative path absolute: {merged_opts['outtmpl']}")
                 else:
                      logger.debug(f"yt-dlp outtmpl already seems absolute or uses a drive: {outtmpl_path}")

            needs_ffmpeg = any(pp.get('key', '').startswith('FFmpeg') for pp in merged_opts.get('postprocessors', [])) or \
                           merged_opts.get('embed_metadata') or \
                           merged_opts.get('embed_thumbnail')
            ffmpeg_path = merged_opts.get('ffmpeg_location') or shutil.which('ffmpeg')
            if needs_ffmpeg and not ffmpeg_path:
                 logger.warning("FFmpeg is needed for audio extraction/embedding but not found in PATH and 'ffmpeg_location' is not set. These features might fail.")
            elif ffmpeg_path:
                 merged_opts['ffmpeg_location'] = ffmpeg_path
                 logger.debug(f"Using FFmpeg found at: {ffmpeg_path}")

            return merged_opts

    except FileNotFoundError:
        logger.warning(f"yt-dlp config file '{absolute_config_path}' not found. Using default options.")
        needs_ffmpeg_default = any(pp.get('key', '').startswith('FFmpeg') for pp in default_opts.get('postprocessors', [])) or \
                               default_opts.get('embed_metadata') or \
                               default_opts.get('embed_thumbnail')
        if needs_ffmpeg_default and shutil.which('ffmpeg') is None:
             logger.warning("FFmpeg is required for default audio extraction/embedding but not found in PATH. These features may fail.")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding yt-dlp config file '{absolute_config_path}': {e}. Using default options.")
    except Exception as e:
        logger.error(f"Error loading yt-dlp config '{absolute_config_path}': {e}. Using default options.")

    needs_ffmpeg_default = any(pp.get('key', '').startswith('FFmpeg') for pp in default_opts.get('postprocessors', [])) or \
                           default_opts.get('embed_metadata') or \
                           default_opts.get('embed_thumbnail')
    if needs_ffmpeg_default and shutil.which('ffmpeg') is None:
         logger.warning("FFmpeg is required for default audio extraction/embedding but not found in PATH. These features may fail.")

    return default_opts.copy()

YDL_OPTS = load_ydl_opts()

# --- Bot Configuration (UBOT.cfg) ---
DEFAULT_CONFIG = {
    "prefix": ",",
    "progress_messages": True,
    "whitelist_enabled": True,
    "auto_clear": True,
    "recent_downloads": True,
    "bot_credit": f"via [YTMG](https://github.com/den22den22/YTMG/)",
    "bot_enabled": True,
    "default_search_limit": 8,
    "artist_top_songs_limit": 5,
    "artist_albums_limit": 3,
    "recommendations_limit": 8,
    "history_limit": 10,
    "liked_songs_limit": 15,
}

def load_config(config_file: str = 'UBOT.cfg') -> Dict:
    """Loads bot configuration from a JSON file, merging with defaults."""
    absolute_config_path = os.path.join(SCRIPT_DIR, config_file)
    logger.info(f"Attempting to load bot config from: {absolute_config_path}")
    try:
        with open(absolute_config_path, 'r', encoding='utf-8') as f:
            loaded_config = json.load(f)
            config = DEFAULT_CONFIG.copy()
            config.update(loaded_config)
            added_keys = [key for key in DEFAULT_CONFIG if key not in loaded_config]
            if added_keys: logger.warning(f"Added missing default keys to config: {', '.join(added_keys)}")
            logger.info(f"Loaded configuration from {absolute_config_path}")
            return config
    except FileNotFoundError:
        logger.warning(f"Bot config file '{absolute_config_path}' not found. Using default configuration.")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding bot config file '{absolute_config_path}': {e}. Using default configuration.")
    except Exception as e:
        logger.error(f"Error loading bot config '{absolute_config_path}': {e}. Using default configuration.")
    return DEFAULT_CONFIG.copy()

def save_config(config_to_save: Dict, config_file: str = 'UBOT.cfg'):
    """Saves the current bot configuration to a JSON file."""
    absolute_config_path = os.path.join(SCRIPT_DIR, config_file)
    logger.info(f"Attempting to save bot config to: {absolute_config_path}")
    try:
        with open(absolute_config_path, 'w', encoding='utf-8') as f:
            json.dump(config_to_save, f, indent=4, ensure_ascii=False)
        logger.info(f"Configuration saved to {absolute_config_path}")
    except Exception as e:
        logger.error(f"Error saving configuration to {absolute_config_path}: {e}")

config = load_config()

# --- Constants derived from Config ---
BOT_CREDIT = config.get("bot_credit", "")
DEFAULT_SEARCH_LIMIT = config.get("default_search_limit", 8)
MAX_SEARCH_RESULTS_DISPLAY = 6

# --- YTMusic API Initialization ---
YT_MUSIC_AUTH_FILE = os.path.join(SCRIPT_DIR, 'headers_auth.json')
ytmusic: Optional[YTMusic] = None
ytmusic_authenticated = False
try:
    ytmusic = YTMusic(YT_MUSIC_AUTH_FILE)
    logger.debug(f"Проверка аутентификации YTMusic через {os.path.basename(YT_MUSIC_AUTH_FILE)}...")
    _ = ytmusic.get_history() # Auth check (no limit needed)
    logger.info(f"ytmusicapi успешно инициализирован (AUTHENTICATED) используя '{os.path.basename(YT_MUSIC_AUTH_FILE)}'.")
    ytmusic_authenticated = True
except Exception as e_auth:
    logger.warning(f"Не удалось инициализировать/проверить ytmusicapi с файлом '{os.path.basename(YT_MUSIC_AUTH_FILE)}': {e_auth}. Используется НЕаутентифицированный режим.")
    ytmusic = YTMusic()
    ytmusic_authenticated = False

# --- Helper Function for Auth Check ---
def require_ytmusic_auth(func):
    """Decorator for command handlers that require authenticated YTMusic."""
    @functools.wraps(func)
    async def wrapper(event: events.NewMessage.Event, args: List[str]):
        if not ytmusic:
            await event.reply("❌ Ошибка: Клиент YTMusic не инициализирован.")
            logger.error("Attempted authenticated command with uninitialized YTMusic.")
            return
        if not ytmusic_authenticated:
            auth_file_basename = os.path.basename(YT_MUSIC_AUTH_FILE)
            await event.reply(f"⚠️ Для этой команды требуется авторизация. Файл `{auth_file_basename}` не найден или недействителен.")
            logger.warning(f"Authenticated command '{func.__name__}' requires '{auth_file_basename}', which is missing or invalid.")
            return
        return await func(event, args)
    return wrapper

# =============================================================================
#                            DATA MANAGEMENT (Users, Last Tracks)
# =============================================================================

USERS_FILE = os.path.join(SCRIPT_DIR, 'users.csv')
LAST_TRACKS_FILE = os.path.join(SCRIPT_DIR, 'last.csv')
HELP_FILE = os.path.join(SCRIPT_DIR, 'help.txt')

def load_users() -> Dict[int, str]:
    """Loads whitelisted users from users.csv (format: Name;UserID)."""
    users: Dict[int, str] = {}
    if not os.path.exists(USERS_FILE):
        logger.warning(f"Whitelist file not found: {USERS_FILE}. Whitelist is empty.")
        return users
    try:
        with open(USERS_FILE, 'r', encoding='utf-8', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            for i, row in enumerate(reader):
                if len(row) == 2:
                    try:
                        user_id = int(row[1].strip())
                        user_name = row[0].strip()
                        if not user_name: user_name = f"User ID {user_id}"
                        users[user_id] = user_name
                    except ValueError:
                        logger.warning(f"Skipping invalid user ID '{row[1]}' in {USERS_FILE}, line {i+1}")
                elif row:
                    logger.warning(f"Skipping malformed row in {USERS_FILE}, line {i+1}: {row}")
        logger.info(f"Loaded {len(users)} users from {USERS_FILE}")
    except Exception as e:
        logger.error(f"Error loading users from {USERS_FILE}: {e}")
    return users

def save_users(users: Dict[int, str]):
    """Saves the current whitelist (UserID -> Name mapping) to users.csv."""
    try:
        with open(USERS_FILE, 'w', encoding='utf-8', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerows([(name, uid) for uid, name in users.items()])
        logger.info(f"Saved {len(users)} users to {USERS_FILE}")
    except Exception as e:
        logger.error(f"Error saving users to {USERS_FILE}: {e}")

ALLOWED_USERS = load_users()

def load_last_tracks() -> List[List[str]]:
    """Loads the history of recently downloaded tracks from last.csv."""
    tracks: List[List[str]] = []
    if not os.path.exists(LAST_TRACKS_FILE):
        logger.info(f"Last tracks file not found: {LAST_TRACKS_FILE}. History is empty.")
        return tracks
    try:
        with open(LAST_TRACKS_FILE, 'r', encoding='utf-8', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            header = next(reader, None)
            expected_header_parts = ['track', 'creator', 'browseid', 'tt:tt-dd-mm']
            if header and not all(part in ''.join(header).lower().replace(' ', '').replace('-', '') for part in expected_header_parts):
                 logger.warning(f"Unexpected header in {LAST_TRACKS_FILE}: {header}. Expected something like 'track;creator;browseId;tt:tt-dd-mm'.")
            tracks = [row for row in reader if len(row) >= 4]
            try:
                with open(LAST_TRACKS_FILE, 'r', encoding='utf-8', newline='') as f_count:
                    original_row_count = sum(1 for row in csv.reader(f_count, delimiter=';') if row) -1
                if len(tracks) < original_row_count:
                    logger.warning(f"Skipped {original_row_count - len(tracks)} malformed rows (less than 4 columns) in {LAST_TRACKS_FILE}.")
            except Exception: pass

        logger.info(f"Loaded {len(tracks)} valid last tracks entries from {LAST_TRACKS_FILE}")
    except StopIteration:
        logger.info(f"{LAST_TRACKS_FILE} is empty or contains only a header.")
    except Exception as e:
        logger.error(f"Error loading last tracks from {LAST_TRACKS_FILE}: {e}")
    return tracks

def save_last_tracks(tracks: List[List[str]]):
    """Saves the recent tracks history (keeping only the latest 5) to last.csv."""
    try:
        tracks_to_save = tracks[:5]
        with open(LAST_TRACKS_FILE, 'w', encoding='utf-8', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow(['track', 'creator', 'browseId', 'tt:tt-dd-mm'])
            writer.writerows(tracks_to_save)
        logger.info(f"Saved {len(tracks_to_save)} last tracks to {LAST_TRACKS_FILE}")
    except Exception as e:
        logger.error(f"Error saving last tracks to {LAST_TRACKS_FILE}: {e}")

# =============================================================================
#                            CORE UTILITIES
# =============================================================================

def retry(max_tries: int = 3, delay: float = 2.0, exceptions: Tuple[Type[Exception], ...] = (Exception,)):
    """Decorator to retry an async function upon encountering specific exceptions."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_tries):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_tries - 1:
                        logger.error(f"Function '{func.__name__}' failed after {max_tries} attempts. Last error: {e}")
                        raise
                    else:
                        wait_time = delay * (2 ** attempt)
                        logger.warning(f"Attempt {attempt + 1}/{max_tries} failed for '{func.__name__}': {e}. Retrying in {wait_time:.2f}s...")
                        await asyncio.sleep(wait_time)
            if last_exception: raise last_exception
        return wrapper
    return decorator

def extract_entity_id(link_or_id: str) -> Optional[str]:
    """
    Extracts YouTube Music video ID, playlist ID, album/artist browse ID from a URL or returns the input if it looks like an ID.
    """
    if not isinstance(link_or_id, str): return None

    if re.fullmatch(r'[A-Za-z0-9_-]{11}', link_or_id): return link_or_id
    if link_or_id.startswith(('PL', 'VL', 'OLAK5uy_')): return link_or_id
    if link_or_id.startswith(('MPRE', 'MPLA')): return link_or_id
    if link_or_id.startswith('UC'): return link_or_id

    id_patterns = [
        r"watch\?v=([A-Za-z0-9_-]{11})",
        r"youtu\.be/([A-Za-z0-9_-]{11})",
        r"playlist\?list=([A-Za-z0-9_-]+)",
        r"browse/([A-Za-z0-9_-]+)",
        r"channel/([A-Za-z0-9_-]+)",
    ]
    for pattern in id_patterns:
        match = re.search(pattern, link_or_id)
        if match:
            extracted_id = match.group(1)
            logger.debug(f"Extracted ID '{extracted_id}' using pattern '{pattern}' from link.")
            return extracted_id

    logger.warning(f"Could not extract a valid ID from input: {link_or_id}")
    return None

def format_artists(data: Optional[Union[List[Dict], Dict, str]]) -> str:
    """Formats artist names from various ytmusicapi structures."""
    names = []
    if isinstance(data, list):
        names = [a.get('name', '').strip() for a in data if isinstance(a, dict) and a.get('name')]
    elif isinstance(data, dict):
        name = data.get('name', '').strip()
        if name: names.append(name)
    elif isinstance(data, str):
        names.append(data.strip())
    cleaned_names = [re.sub(r'\s*-\s*Topic$', '', name).strip() for name in names if name]
    return ', '.join(filter(None, cleaned_names)) or 'Неизвестно'

# =============================================================================
#                       YOUTUBE MUSIC API INTERACTION
# =============================================================================

@retry(exceptions=(Exception,))
async def get_entity_info(entity_id: str, entity_type_hint: Optional[str] = None) -> Optional[Dict]:
    """
    Fetches metadata for a YouTube Music entity (track, album, playlist, artist).
    """
    if not ytmusic:
        logger.error("YTMusic API client not initialized.")
        return None

    logger.debug(f"Fetching entity info for ID: {entity_id}, Hint: {entity_type_hint}")
    try:
        inferred_type = None
        if isinstance(entity_id, str):
            if entity_id.startswith(('PL', 'VL')): inferred_type = "playlist"
            elif entity_id.startswith(('MPRE', 'MPLA')): inferred_type = "album"
            elif entity_id.startswith('UC'): inferred_type = "artist"
            elif re.fullmatch(r'[A-Za-z0-9_-]{11}', entity_id): inferred_type = "track"
        else:
            logger.warning(f"Invalid entity_id type provided: {type(entity_id)}.")
            return None

        current_hint = entity_type_hint or inferred_type
        logger.debug(f"Effective hint/inferred type for API call: {current_hint}")

        if (current_hint == "track" or inferred_type == "track") and re.fullmatch(r'[A-Za-z0-9_-]{11}', entity_id):
             try:
                 logger.debug(f"Attempting get_watch_playlist for potential track ID {entity_id}")
                 watch_info = await asyncio.to_thread(ytmusic.get_watch_playlist, videoId=entity_id, limit=1)
                 if watch_info and watch_info.get('tracks') and len(watch_info['tracks']) > 0:
                      track_data = watch_info['tracks'][0]
                      standardized_info = {
                          '_entity_type': 'track',
                          'videoId': track_data.get('videoId'),
                          'title': track_data.get('title'),
                          'artists': track_data.get('artists'),
                          'album': track_data.get('album'),
                          'duration': track_data.get('length'),
                          'lengthSeconds': track_data.get('lengthSeconds'),
                          'thumbnails': track_data.get('thumbnail'),
                          'year': track_data.get('year'),
                          'lyrics': watch_info.get('lyrics'),
                          'videoDetails': {
                                'videoId': track_data.get('videoId'),
                                'title': track_data.get('title'),
                                'lengthSeconds': track_data.get('lengthSeconds'),
                                'thumbnails': track_data.get('thumbnail'),
                                'author': format_artists(track_data.get('artists')),
                                'channelId': None,
                                'lyrics': watch_info.get('lyrics'),
                          }
                      }
                      logger.info(f"Successfully fetched track info for {entity_id} using get_watch_playlist")
                      return standardized_info
                 else:
                     logger.debug(f"get_watch_playlist for {entity_id} didn't return expected track data structure.")
             except Exception as e_watch:
                  logger.warning(f"get_watch_playlist failed for {entity_id}: {e_watch}. Falling back.")

        api_calls_by_type = {
            "playlist": lambda eid: asyncio.to_thread(ytmusic.get_playlist, playlistId=eid),
            "album": lambda eid: asyncio.to_thread(ytmusic.get_album, browseId=eid),
            "artist": lambda eid: asyncio.to_thread(ytmusic.get_artist, channelId=eid),
            "track": lambda eid: asyncio.to_thread(ytmusic.get_song, videoId=eid),
        }

        if current_hint and current_hint in api_calls_by_type:
            try:
                logger.debug(f"Trying API call for hinted/inferred type: {current_hint}")
                info = await api_calls_by_type[current_hint](entity_id)
                if info:
                    if current_hint == "track":
                        if info.get('videoDetails'):
                            processed_info = info['videoDetails']
                            if 'thumbnails' not in processed_info and 'thumbnail' in info:
                                processed_info['thumbnails'] = info['thumbnail'].get('thumbnails')
                            if 'artists' not in processed_info and 'artists' in info:
                                processed_info['artists'] = info['artists']
                            if 'lyrics' not in processed_info and 'lyrics' in info:
                                processed_info['lyrics'] = info['lyrics']
                            info = processed_info
                        else:
                             logger.warning(f"get_song for {entity_id} lacked 'videoDetails'. Structure may be inconsistent.")
                    info['_entity_type'] = current_hint
                    logger.info(f"Successfully fetched entity info using hint/inferred type '{current_hint}' for {entity_id}")
                    return info
                else:
                    logger.warning(f"API call for hint '{current_hint}' returned no data for {entity_id}.")
            except Exception as e_hint:
                 logger.warning(f"API call for hint/inferred type '{current_hint}' failed for {entity_id}: {e_hint}. Trying generic checks.")

        generic_check_order = [
             ("track", api_calls_by_type["track"]),
             ("playlist", api_calls_by_type["playlist"]),
             ("album", api_calls_by_type["album"]),
             ("artist", api_calls_by_type["artist"]),
        ]

        for type_name, api_func in generic_check_order:
            if current_hint and current_hint == type_name: continue
            if type_name == "track" and not re.fullmatch(r'[A-Za-z0-9_-]{11}', entity_id): continue
            if type_name == "album" and not entity_id.startswith(('MPRE', 'MPLA','OLAK5uy_')): continue
            if type_name == "artist" and not entity_id.startswith('UC'): continue
            if type_name == "playlist" and not entity_id.startswith(('PL', 'VL', 'OLAK5uy_')): continue

            try:
                logger.debug(f"Trying generic API call for type '{type_name}' for {entity_id}")
                result = await api_func(entity_id)
                if result:
                    final_info = result
                    if type_name == "track":
                        if result.get('videoDetails'):
                            processed_info = result['videoDetails']
                            if 'thumbnails' not in processed_info and 'thumbnail' in result:
                                processed_info['thumbnails'] = result['thumbnail'].get('thumbnails')
                            if 'artists' not in processed_info and 'artists' in result:
                                processed_info['artists'] = result['artists']
                            if 'lyrics' not in processed_info and 'lyrics' in result:
                                processed_info['lyrics'] = result['lyrics']
                            final_info = processed_info
                        else:
                             logger.warning(f"Generic check {type_name} for {entity_id} lacked 'videoDetails'. Structure may be inconsistent.")
                             final_info['_incomplete_structure'] = True

                    final_info['_entity_type'] = type_name
                    logger.info(f"Successfully fetched entity info as '{type_name}' for {entity_id} using generic check.")
                    return final_info
            except Exception:
                 pass # Ignore and try next type

        logger.error(f"Could not retrieve info for entity ID: {entity_id} using any method.")
        return None

    except Exception as e_outer:
        logger.error(f"Unexpected error in get_entity_info for {entity_id}: {e_outer}", exc_info=True)
        return None


@retry(exceptions=(Exception,))
async def search(query: str, search_type_flag: str, limit: int) -> List[Dict]:
    """
    Performs a search on YouTube Music using ytmusicapi.
    """
    if not ytmusic:
        logger.error("YTMusic API client not initialized. Cannot perform search.")
        return []

    filter_map = { "-t": "songs", "-a": "albums", "-p": "playlists", "-e": "artists" }
    filter_type = filter_map.get(search_type_flag)
    if not filter_type:
        logger.error(f"Invalid search type flag provided to search function: {search_type_flag}")
        raise ValueError(f"Invalid search type flag: {search_type_flag}")

    logger.info(f"Searching for '{query}' (Type: {filter_type}, Limit: {limit})")
    api_limit = min(max(limit + 3, 5), 20)
    try:
        results = await asyncio.to_thread(ytmusic.search, query, filter=filter_type, limit=api_limit)
    except Exception as e:
         logger.error(f"ytmusicapi search failed: {e}", exc_info=True)
         return []

    if not results:
        logger.info(f"No results found for query '{query}' (Type: {filter_type})")
        return []

    valid_results = [r for r in results if r and isinstance(r, dict)]
    if len(valid_results) < len(results):
         logger.warning(f"Removed {len(results) - len(valid_results)} invalid items from search results.")

    final_results = valid_results[:limit]
    logger.info(f"Found {len(final_results)} valid results for query '{query}' (fetched {len(results)} initially)")
    return final_results

# =============================================================================
#                       DOWNLOAD & PROCESSING FUNCTIONS
# =============================================================================

def extract_track_metadata(info: Dict) -> Tuple[str, str, int]:
    """
    Extracts Title, Performer, and Duration from yt-dlp's info dictionary.
    """
    title = info.get('track') or info.get('title') or 'Неизвестно'
    performer = 'Неизвестно'
    if info.get('artist'):
        performer = info['artist']
    elif info.get('artists') and isinstance(info['artists'], list):
         artist_names = [a['name'] for a in info['artists'] if isinstance(a, dict) and a.get('name')]
         if artist_names: performer = ', '.join(artist_names)
    elif info.get('creator'):
         performer = info['creator']
    elif info.get('uploader'):
         performer = re.sub(r'\s*-\s*Topic$', '', info['uploader']).strip()

    if performer in [None, "", "Неизвестно"] and info.get('channel'):
         performer = re.sub(r'\s*-\s*Topic$', '', info['channel']).strip()

    if performer in [None, "", "Неизвестно"]:
        performer = 'Неизвестно'

    performer = re.sub(r'\s*-\s*Topic$', '', performer).strip()

    duration = 0
    try:
        duration = int(info.get('duration') or 0)
    except (ValueError, TypeError):
         logger.warning(f"Could not parse duration '{info.get('duration')}' for track '{title}'. Defaulting to 0.")
         duration = 0

    logger.debug(f"Extracted metadata - Title: '{title}', Performer: '{performer}', Duration: {duration}s")
    return title, performer, duration

def download_track(track_link: str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Downloads a single track using yt-dlp with configured options.
    """
    logger.info(f"Attempting download and processing via yt-dlp: {track_link}")
    try:
        current_ydl_opts = YDL_OPTS.copy()

        if current_ydl_opts.get('noplaylist'):
             tmpl = current_ydl_opts.get('outtmpl', '')
             tmpl = re.sub(r'[\[\(]?%?\(playlist_index\)[0-9]*[ds]?[-_\. ]?[\]\)]?', '', tmpl).strip()
             current_ydl_opts['outtmpl'] = tmpl

        with yt_dlp.YoutubeDL(current_ydl_opts) as ydl:
            info = ydl.extract_info(track_link, download=True)

            if not info:
                logger.error(f"yt-dlp extract_info returned empty/None for {track_link}")
                return None, None

            final_filepath = None
            if info.get('requested_downloads') and isinstance(info['requested_downloads'], list):
                 final_download_info = next((d for d in reversed(info['requested_downloads']) if d.get('filepath') and os.path.exists(d['filepath'])), None)
                 if final_download_info:
                      final_filepath = final_download_info.get('filepath')
                      logger.debug(f"Found final path in 'requested_downloads': {final_filepath}")

            if not final_filepath:
                 final_filepath = info.get('filepath')
                 if final_filepath: logger.debug(f"Using top-level 'filepath' key: {final_filepath}")


            if final_filepath and os.path.exists(final_filepath) and os.path.isfile(final_filepath):
                 logger.info(f"Download and postprocessing successful. Final file: {final_filepath}")
                 info['filepath'] = final_filepath
                 return info, final_filepath
            else:
                logger.warning(f"Final 'filepath' key missing or file not found ('{final_filepath}'). Attempting to locate file.")
                try:
                    potential_path_after_pp = ydl.prepare_filename(info)
                    logger.debug(f"Path based on prepare_filename: {potential_path_after_pp}")

                    if os.path.exists(potential_path_after_pp) and os.path.isfile(potential_path_after_pp):
                         logger.warning(f"Located final file via updated prepare_filename: {potential_path_after_pp}")
                         info['filepath'] = potential_path_after_pp
                         return info, potential_path_after_pp
                    else:
                        original_path_before_pp = ydl.prepare_filename(info, outtmpl=current_ydl_opts.get('outtmpl_na', info.get('filename')))
                        base_potential, _ = os.path.splitext(original_path_before_pp)
                        potential_exts = ['.mp3', '.m4a', '.ogg', '.opus', '.aac', '.flac', '.wav']
                        try:
                            pref_codec = next((pp.get('preferredcodec') for pp in current_ydl_opts.get('postprocessors', []) if pp.get('key') == 'FFmpegExtractAudio' and pp.get('preferredcodec')), None)
                            if pref_codec and f'.{pref_codec}' not in potential_exts:
                                potential_exts.insert(0, f'.{pref_codec}')
                        except Exception: pass

                        for ext in potential_exts:
                            check_path = base_potential + ext
                            if os.path.exists(check_path) and os.path.isfile(check_path):
                                 logger.warning(f"Located final file via extension check: {check_path}")
                                 info['filepath'] = check_path
                                 return info, check_path

                        logger.error(f"Could not locate the final processed audio file for {track_link}.")
                        return info, None
                except Exception as e_locate:
                    logger.error(f"Error trying to locate final file for {track_link}: {e_locate}")
                    return info, None

    except yt_dlp.utils.DownloadError as e:
        logger.error(f"yt-dlp DownloadError for {track_link}: {e}")
        return None, None
    except Exception as e:
        logger.error(f"Unexpected download error for {track_link}: {e}", exc_info=True)
        return None, None

async def download_album_tracks(album_browse_id: str, progress_callback=None) -> List[Tuple[Dict, str]]:
    """
    Downloads all tracks from a given album browse ID using yt-dlp sequentially.
    """
    if not ytmusic:
        logger.error("YTMusic API client not initialized. Cannot download album.")
        if progress_callback: await progress_callback("album_error", error="YTMusic client not ready")
        return []

    logger.info(f"Attempting to download album/playlist sequentially: {album_browse_id}")
    downloaded_files: List[Tuple[Dict, str]] = []
    album_info, total_tracks, album_title = None, 0, album_browse_id

    try:
        logger.debug(f"Fetching album/playlist metadata for {album_browse_id}...")
        try:
            if album_browse_id.startswith(('MPRE', 'MPLA')):
                album_info = await asyncio.to_thread(ytmusic.get_album, browseId=album_browse_id)
                if album_info:
                    album_title = album_info.get('title', album_browse_id)
                    total_tracks = album_info.get('trackCount') or len(album_info.get('tracks', []))
                    logger.info(f"Fetched album metadata: '{album_title}', Expected tracks: {total_tracks or 'Unknown'}")
                else: logger.warning(f"Could not fetch album metadata for {album_browse_id} via ytmusicapi.")
            elif album_browse_id.startswith(('PL', 'VL', 'OLAK5uy_')):
                 album_info = await asyncio.to_thread(ytmusic.get_playlist, playlistId=album_browse_id, limit=None)
                 if album_info:
                     album_title = album_info.get('title', album_browse_id)
                     total_tracks = album_info.get('trackCount') or len(album_info.get('tracks', []))
                     logger.info(f"Fetched playlist metadata: '{album_title}', Expected tracks: {total_tracks or 'Unknown'}")
                 else: logger.warning(f"Could not fetch playlist metadata for {album_browse_id} via ytmusicapi.")
            else:
                 logger.info(f"ID {album_browse_id} type unknown. Attempting download via yt-dlp analysis.")
        except Exception as e_meta:
             logger.warning(f"Error fetching metadata for ID {album_browse_id} via ytmusicapi: {e_meta}. Proceeding.")


        tracks_to_download = []
        if album_info and 'tracks' in album_info:
             tracks_to_download = album_info.get("tracks", [])
             total_tracks = len(tracks_to_download)
             logger.info(f"Using track list from ytmusicapi metadata ({total_tracks} tracks).")
        else:
             logger.info(f"Metadata incomplete/missing. Using yt-dlp with --flat-playlist for {album_browse_id}...")
             try:
                 if album_browse_id.startswith(('MPRE', 'MPLA')):
                      analysis_url = f"https://music.youtube.com/browse/{album_browse_id}"
                 elif album_browse_id.startswith(('PL', 'VL', 'OLAK5uy_')):
                      analysis_url = f"https://music.youtube.com/playlist?list={album_browse_id}"
                 else:
                      analysis_url = f"https://music.youtube.com/browse/{album_browse_id}"

                 analysis_opts = {'extract_flat': True, 'skip_download': True, 'quiet': True, 'ignoreerrors': True, 'noplaylist': False}
                 with yt_dlp.YoutubeDL(analysis_opts) as ydl:
                     playlist_dict = ydl.extract_info(analysis_url, download=False)

                 if playlist_dict and playlist_dict.get('entries'):
                     tracks_to_download = [{'videoId': entry.get('id'), 'title': entry.get('title')}
                                           for entry in playlist_dict['entries'] if entry and entry.get('id')]
                     total_tracks = len(tracks_to_download)
                     if playlist_dict.get('title') and (album_title == album_browse_id or not album_title):
                          album_title = playlist_dict['title']
                     logger.info(f"Extracted {total_tracks} tracks using yt-dlp analysis for '{album_title}'.")
                 else:
                     logger.error(f"yt-dlp analysis failed to return track entries for {album_browse_id}.")
                     if progress_callback: await progress_callback("album_error", error="yt-dlp failed to get track list")
                     return []
             except Exception as e_analyze:
                 logger.error(f"Error during yt-dlp analysis phase for {album_browse_id}: {e_analyze}", exc_info=True)
                 if progress_callback: await progress_callback("album_error", error=f"yt-dlp analysis error: {e_analyze}")
                 return []

        if not tracks_to_download:
             logger.error(f"No tracks found to download for album/playlist {album_browse_id}.")
             if progress_callback: await progress_callback("album_error", error="No tracks found")
             return []

        if progress_callback:
            await progress_callback("analysis_complete", total_tracks=total_tracks, title=album_title)

        downloaded_count = 0
        loop = asyncio.get_running_loop()

        for i, track in enumerate(tracks_to_download):
            current_track_num = i + 1
            video_id = track.get('videoId')
            track_title_from_list = track.get('title') or f'Трек {current_track_num}'

            if not video_id:
                logger.warning(f"Skipping track {current_track_num}/{total_tracks} ('{track_title_from_list}') due to missing videoId.")
                if progress_callback:
                     await progress_callback("track_failed", current=downloaded_count + 1, total=total_tracks, title=f"{track_title_from_list} (No ID)")
                continue

            download_link = f"https://music.youtube.com/watch?v={video_id}"
            logger.info(f"Downloading track {current_track_num}/{total_tracks}: '{track_title_from_list}' ({video_id})...")

            if progress_callback:
                 perc = int(((downloaded_count + 1) / total_tracks) * 100) if total_tracks else 0
                 display_track_title = (track_title_from_list[:25] + '...') if len(track_title_from_list) > 28 else track_title_from_list
                 await progress_callback("track_downloading",
                                       current=current_track_num,
                                       total=total_tracks,
                                       percentage=perc,
                                       title=display_track_title)

            try:
                info, file_path = await loop.run_in_executor(None, functools.partial(download_track, download_link))

                if file_path and info:
                    actual_filename = os.path.basename(file_path)
                    final_track_title = info.get('title', track_title_from_list)
                    logger.info(f"Successfully downloaded and processed track {current_track_num}/{total_tracks}: {actual_filename}")
                    downloaded_files.append((info, file_path))
                    downloaded_count += 1
                    if progress_callback:
                         await progress_callback("track_downloaded", current=downloaded_count, total=total_tracks, title=final_track_title)
                else:
                    logger.error(f"Failed to download/process track {current_track_num}/{total_tracks}: '{track_title_from_list}' ({video_id})")
                    if progress_callback:
                         await progress_callback("track_failed", current=downloaded_count + 1,
                                               total=total_tracks, title=track_title_from_list)

            except Exception as e_track_dl:
                logger.error(f"Error during download process for track {current_track_num} ('{track_title_from_list}'): {e_track_dl}", exc_info=True)
                if progress_callback:
                     await progress_callback("track_failed", current=downloaded_count + 1, total=total_tracks, title=f"{track_title_from_list} (Error)")

            await asyncio.sleep(0.3)

    except Exception as e_album_outer:
        logger.error(f"Error during album processing loop for {album_browse_id}: {e_album_outer}", exc_info=True)
        if progress_callback:
            await progress_callback("album_error", error=str(e_album_outer))

    logger.info(f"Finished sequential album download for '{album_title}'. Successfully saved {len(downloaded_files)} out of {total_tracks} tracks attempted.")
    return downloaded_files

# =============================================================================
#                         LYRICS HANDLING
# =============================================================================

async def get_lyrics_for_track(video_id: Optional[str], lyrics_browse_id: Optional[str] = None) -> Optional[Dict[str, str]]:
    """
    Fetches lyrics for a track using its video ID or lyrics browse ID.
    """
    if not ytmusic:
        logger.error("YTMusic API client not initialized. Cannot fetch lyrics.")
        return None
    if not video_id and not lyrics_browse_id:
        logger.error("Cannot fetch lyrics without either video ID or lyrics browse ID.")
        return None

    final_lyrics_browse_id = lyrics_browse_id
    track_id_for_log = lyrics_browse_id or video_id

    try:
        if not final_lyrics_browse_id and video_id:
             logger.debug(f"Fetching watch playlist info to find lyrics browse ID for video: {video_id}")
             try:
                 watch_info = await asyncio.to_thread(ytmusic.get_watch_playlist, videoId=video_id, limit=1)
                 final_lyrics_browse_id = watch_info.get('lyrics')
                 if not final_lyrics_browse_id:
                      logger.info(f"No lyrics browse ID found in watch playlist info for {video_id}.")
                      return None
                 logger.debug(f"Found lyrics browse ID: {final_lyrics_browse_id} for video {video_id}")
             except Exception as e_watch:
                  logger.warning(f"Failed to get watch playlist info for lyrics browse ID lookup ({video_id}): {e_watch}")
                  return None

        if final_lyrics_browse_id:
             logger.info(f"Fetching lyrics using browse ID: {final_lyrics_browse_id} (original ID: {track_id_for_log})")
             try:
                 lyrics_data = await asyncio.to_thread(ytmusic.get_lyrics, browseId=final_lyrics_browse_id)
                 if lyrics_data and lyrics_data.get('lyrics'):
                     logger.info(f"Successfully fetched lyrics for {track_id_for_log}")
                     return lyrics_data
                 else:
                      logger.info(f"No lyrics content found for browse ID {final_lyrics_browse_id} (original ID: {track_id_for_log})")
                      return None
             except Exception as e_lyrics:
                  logger.error(f"Failed to fetch lyrics using browse ID {final_lyrics_browse_id} (original ID: {track_id_for_log}): {e_lyrics}")
                  return None
        else:
             logger.error(f"Could not determine lyrics browse ID for {track_id_for_log}.")
             return None

    except Exception as e_outer:
        logger.error(f"Unexpected error in get_lyrics_for_track for {track_id_for_log}: {e_outer}", exc_info=True)
        return None

# =============================================================================
#                           THUMBNAIL HANDLING
# =============================================================================

@retry(exceptions=(requests.exceptions.RequestException,), delay=1.0)
async def download_thumbnail(url: str, output_dir: str = SCRIPT_DIR) -> Optional[str]:
    """
    Downloads a thumbnail image from a URL.
    """
    if not url or not isinstance(url, str) or not url.startswith(('http://', 'https://')):
        logger.warning(f"Invalid or non-HTTP/S thumbnail URL provided: {url}")
        return None

    logger.debug(f"Attempting to download thumbnail: {url}")
    temp_file_path = None

    try:
        try:
            parsed_url = urlparse(url)
            base_name_from_url = os.path.basename(parsed_url.path) if parsed_url.path else "thumb"
        except Exception as parse_e:
            logger.warning(f"Could not parse URL path for thumbnail naming: {parse_e}. Using default 'thumb'.")
            base_name_from_url = "thumb"

        base_name, potential_ext = os.path.splitext(base_name_from_url)
        if potential_ext and len(potential_ext) <= 5 and potential_ext[1:].isalnum():
             ext = potential_ext.lower()
        else: ext = '.jpg'
        if not base_name or base_name == potential_ext: base_name = "thumb"

        safe_base_name = re.sub(r'[^\w.\-]', '_', base_name)
        max_len = 40
        safe_base_name = (safe_base_name[:max_len] + '...') if len(safe_base_name) > max_len else safe_base_name
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
        temp_filename = f"temp_thumb_{safe_base_name}_{timestamp}{ext}"
        temp_file_path = os.path.join(output_dir, temp_filename)

        response = requests.get(url, stream=True, timeout=25)
        response.raise_for_status()

        with open(temp_file_path, 'wb') as out_file:
            shutil.copyfileobj(response.raw, out_file)
        logger.debug(f"Thumbnail downloaded to temporary file: {temp_file_path}")

        try:
            with Image.open(temp_file_path) as img:
                img.verify()
            logger.debug(f"Thumbnail verified as valid image: {temp_file_path}")
            return temp_file_path
        except (FileNotFoundError, UnidentifiedImageError, SyntaxError, OSError, ValueError) as img_e:
             logger.error(f"Downloaded file is not a valid image ({url}): {img_e}. Deleting.")
             if os.path.exists(temp_file_path):
                 try: os.remove(temp_file_path)
                 except OSError as rm_e: logger.warning(f"Could not remove invalid temp thumb {temp_file_path}: {rm_e}")
             return None

    except requests.exceptions.Timeout:
        logger.error(f"Timeout occurred while downloading thumbnail: {url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error downloading thumbnail {url}: {e}")
        if temp_file_path and os.path.exists(temp_file_path):
            try: os.remove(temp_file_path)
            except OSError as rm_e: logger.warning(f"Could not remove partial temp thumb {temp_file_path}: {rm_e}")
        return None
    except Exception as e_outer:
        logger.error(f"Unexpected error downloading thumbnail {url}: {e_outer}", exc_info=True)
        if temp_file_path and os.path.exists(temp_file_path):
            try: os.remove(temp_file_path)
            except OSError as rm_e: logger.warning(f"Could not remove temp thumb {temp_file_path} after error: {rm_e}")
        return None

def crop_thumbnail(image_path: str) -> Optional[str]:
    """
    Crops an image to a square aspect ratio (center crop) and saves it as JPEG.
    """
    if not image_path or not os.path.exists(image_path):
        logger.error(f"Cannot crop thumbnail, file not found: {image_path}")
        return None

    logger.debug(f"Processing thumbnail (cropping to square): {image_path}")
    output_path = os.path.splitext(image_path)[0] + "_cropped.jpg"

    try:
        with Image.open(image_path) as img:
            img_rgb = img

            if img.mode != 'RGB':
                logger.debug(f"Image mode is '{img.mode}', converting to RGB.")
                try:
                    bg = Image.new("RGB", img.size, (255, 255, 255))
                    if img.mode in ('RGBA', 'LA') and len(img.split()) > 3:
                        bg.paste(img, mask=img.split()[-1])
                    else: bg.paste(img)
                    img_rgb = bg
                except Exception as conv_e:
                     logger.warning(f"Could not convert image {os.path.basename(image_path)} from {img.mode} to RGB using background paste: {conv_e}. Attempting basic conversion.")
                     try: img_rgb = img.convert('RGB')
                     except Exception as basic_conv_e:
                         logger.error(f"Failed basic RGB conversion for {os.path.basename(image_path)}: {basic_conv_e}. Cannot crop.")
                         return None

            width, height = img_rgb.size
            min_dim = min(width, height)
            left = (width - min_dim) / 2
            top = (height - min_dim) / 2
            right = (width + min_dim) / 2
            bottom = (height + min_dim) / 2
            crop_box = tuple(map(int, (left, top, right, bottom)))
            img_cropped = img_rgb.crop(crop_box)

            img_cropped.save(output_path, "JPEG", quality=90)
            logger.debug(f"Thumbnail cropped and saved successfully: {output_path}")
            return output_path

    except FileNotFoundError:
        logger.error(f"Cannot process thumbnail, file not found during Pillow ops: {image_path}")
        return None
    except UnidentifiedImageError:
        logger.error(f"Cannot process thumbnail, invalid image file format: {image_path}")
        return None
    except Exception as e:
        logger.error(f"Error processing (cropping) thumbnail {os.path.basename(image_path)}: {e}", exc_info=True)
        if os.path.exists(output_path):
            try: os.remove(output_path)
            except OSError as rm_e: logger.warning(f"Could not remove partial cropped thumb {output_path}: {rm_e}")
        return None

# =============================================================================
#                         FILE CLEANUP UTILITY
# =============================================================================

async def cleanup_files(*files: Optional[str]):
    """
    Safely removes specified files and files matching common temporary patterns.
    """
    temp_patterns = [
        os.path.join(SCRIPT_DIR, "temp_thumb_*"),
        os.path.join(SCRIPT_DIR, "*_cropped.jpg"),
        os.path.join(SCRIPT_DIR, "*.part"),
        os.path.join(SCRIPT_DIR, "*.ytdl"),
        os.path.join(SCRIPT_DIR, "*.webp"),
        os.path.join(SCRIPT_DIR, "lyrics_*.html"), # Cleanup generated lyrics files
    ]

    all_files_to_remove = set()
    for f in files:
        if f and isinstance(f, str):
            try:
                if os.path.exists(f):
                    real_path = os.path.realpath(f)
                    all_files_to_remove.add(real_path)
            except Exception as path_e:
                 logger.warning(f"Could not process path for file '{f}': {path_e}")


    for pattern in temp_patterns:
        try:
            abs_pattern = os.path.abspath(pattern)
            matched_files = glob.glob(abs_pattern)
            if matched_files:
                logger.debug(f"Globbed {len(matched_files)} files for cleanup pattern: {pattern}")
                all_files_to_remove.update(os.path.realpath(mf) for mf in matched_files)
        except Exception as e:
            logger.error(f"Error during glob matching for pattern '{pattern}': {e}")

    removed_count = 0
    if not all_files_to_remove:
        logger.debug("Cleanup called, but no files specified or matched for removal.")
        return

    logger.info(f"Attempting to clean up {len(all_files_to_remove)} potential files...")
    for file_path in all_files_to_remove:
        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
                logger.debug(f"Removed file: {file_path}")
                removed_count += 1
        except OSError as e:
            logger.error(f"Error removing file {file_path}: {e}")
        except Exception as e_remove:
            logger.error(f"Unexpected error removing file {file_path}: {e_remove}")

    if removed_count > 0:
        logger.info(f"Successfully cleaned up {removed_count} files.")


# =============================================================================
#                         TELEGRAM MESSAGE UTILITIES
# =============================================================================

previous_bot_messages: Dict[int, List[types.Message]] = {}

async def update_progress(progress_message: Optional[types.Message], statuses: Dict[str, str]):
    """
    Edits a progress message with the current status of different tasks.
    """
    if not progress_message or not isinstance(progress_message, types.Message):
        return

    text = "\n".join(f"{task}: {status}" for task, status in statuses.items())

    try:
        current_text = getattr(progress_message, 'text', None)
        if current_text != text:
            await progress_message.edit(text)
    except types.errors.MessageNotModifiedError:
        pass
    except types.errors.MessageIdInvalidError:
        logger.warning(f"Failed to update progress: Message {progress_message.id} seems invalid.")
    except types.errors.FloodWaitError as e:
         logger.warning(f"Flood wait ({e.seconds}s) while updating progress message {progress_message.id}.")
         await asyncio.sleep(e.seconds + 0.5)
    except Exception as e:
        logger.warning(f"Failed to update progress message {progress_message.id}: {type(e).__name__} - {e}")

async def clear_previous_responses(chat_id: int):
    """
    Deletes previously sent bot messages stored for a specific chat.
    """
    global previous_bot_messages
    if chat_id not in previous_bot_messages or not previous_bot_messages[chat_id]:
        return

    messages_to_delete = previous_bot_messages.pop(chat_id, [])
    if not messages_to_delete: return

    deleted_count = 0
    messages_to_retry = []

    logger.info(f"Attempting to clear {len(messages_to_delete)} previous bot messages in chat {chat_id}")

    for msg in messages_to_delete:
        if not msg or not isinstance(msg, types.Message): continue
        try:
            await msg.delete()
            deleted_count += 1
            await asyncio.sleep(0.2)
        except types.errors.FloodWaitError as e:
             wait_time = e.seconds
             logger.warning(f"Flood wait ({wait_time}s) during message clearing in chat {chat_id}. Pausing.")
             await asyncio.sleep(wait_time + 1.5)
             try:
                 failed_index = messages_to_delete.index(msg)
                 messages_to_retry.extend(messages_to_delete[failed_index:])
             except ValueError:
                 messages_to_retry.append(msg)
             break
        except (types.errors.MessageDeleteForbiddenError, types.errors.MessageIdInvalidError):
             logger.warning(f"Cannot delete message {getattr(msg, 'id', 'N/A')} (forbidden or invalid).")
        except Exception as e:
             msg_id = getattr(msg, 'id', 'N/A')
             logger.warning(f"Could not delete message {msg_id}: {e}. Retrying.")
             messages_to_retry.append(msg)

    if messages_to_retry:
        logger.info(f"Retrying deletion of {len(messages_to_retry)} messages in chat {chat_id}.")
        await asyncio.sleep(1)
        for msg in messages_to_retry:
             if not msg or not isinstance(msg, types.Message): continue
             msg_id = getattr(msg, 'id', 'N/A')
             try:
                 await msg.delete()
                 deleted_count += 1
                 await asyncio.sleep(0.3)
             except types.errors.FloodWaitError as e:
                  logger.error(f"Flood wait ({e.seconds}s) during retry for message {msg_id}. Aborting.")
                  await asyncio.sleep(e.seconds + 1)
                  break
             except Exception as e_retry:
                 logger.warning(f"Could not delete message {msg_id} on retry: {e_retry}")

    if deleted_count > 0:
        logger.info(f"Cleared {deleted_count} previous bot messages for chat {chat_id}.")

async def store_response_message(chat_id: int, message: Optional[types.Message]):
    """
    Stores a message object to be potentially cleared later by auto_clear.
    """
    if not message or not isinstance(message, types.Message) or not chat_id:
        return

    global previous_bot_messages
    if chat_id not in previous_bot_messages:
        previous_bot_messages[chat_id] = []

    if message not in previous_bot_messages[chat_id]:
        previous_bot_messages[chat_id].append(message)
        logger.debug(f"Stored message {message.id} for clearing in chat {chat_id}. (Total: {len(previous_bot_messages[chat_id])})")

async def send_long_message(event: events.NewMessage.Event, text: str, prefix: str = ""):
    """Sends a long message by splitting it into chunks."""
    MAX_LEN = 4096
    sent_msgs = []
    current_message = prefix
    lines = text.split('\n')

    for line in lines:
        if len(current_message) + len(line) + 1 > MAX_LEN:
            msg = await event.respond(current_message)
            sent_msgs.append(msg)
            await asyncio.sleep(0.3)
            current_message = prefix + line
        else:
            current_message += "\n" + line

    if current_message.strip() != prefix.strip():
        msg = await event.respond(current_message)
        sent_msgs.append(msg)

    for m in sent_msgs:
        await store_response_message(event.chat_id, m)


async def send_lyrics(event: events.NewMessage.Event, lyrics_text: str, lyrics_header: str, track_title: str, video_id: str):
    """
    Sends lyrics. If too long, sends as an HTML file.
    """
    MAX_LEN = 4096
    combined_length = len(lyrics_header) + len(lyrics_text) + 100 # Estimate

    if combined_length <= MAX_LEN:
        logger.info(f"Sending lyrics for '{track_title}' directly (Length: {combined_length})")
        await send_long_message(event, lyrics_text, prefix=lyrics_header)
    else:
        logger.info(f"Lyrics for '{track_title}' too long ({combined_length} > {MAX_LEN}). Sending as HTML file.")
        header_lines = lyrics_header.split('\n')
        html_title = track_title
        # Try to extract artist/source cleanly for HTML
        html_artist_line = header_lines[0].replace("📜 **Текст песни:** ", "").strip() if header_lines else f"{track_title} - Неизвестно"
        html_source_line = header_lines[1].replace("_", "").replace("(Источник: ", "").replace(")_", "").strip() if len(header_lines) > 1 and "Источник:" in header_lines[1] else ""

        html_content = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Текст: {html.escape(html_title)}</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; padding: 20px; background-color: #f8f9fa; color: #212529; margin: 0; }}
        .container {{ max-width: 800px; margin: 20px auto; background: #ffffff; padding: 30px; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.07); }}
        h1 {{ color: #343a40; border-bottom: 2px solid #dee2e6; padding-bottom: 15px; margin-top: 0; margin-bottom: 20px; font-size: 2em; font-weight: 600; }}
        .artist-info {{ font-size: 1.1em; color: #495057; margin-bottom: 10px; }}
        .source {{ font-size: 0.9em; color: #6c757d; margin-bottom: 30px; font-style: italic; }}
        pre {{ white-space: pre-wrap; word-wrap: break-word; background: #e9ecef; padding: 20px; border-radius: 5px; font-family: 'Menlo', 'Consolas', monospace; font-size: 1.05em; line-height: 1.7; border: 1px solid #ced4da; overflow-x: auto; }}
        ::-webkit-scrollbar {{ width: 8px; height: 8px; }} ::-webkit-scrollbar-track {{ background: #f1f1f1; border-radius: 10px; }} ::-webkit-scrollbar-thumb {{ background: #adb5bd; border-radius: 10px; }} ::-webkit-scrollbar-thumb:hover {{ background: #868e96; }}
    </style>
</head>
<body><div class="container"><h1>{html.escape(html_title)}</h1><p class="artist-info">{html.escape(html_artist_line.replace(f'{html_title} - ',''))}</p>{f'<p class="source">Источник: {html.escape(html_source_line)}</p>' if html_source_line else ''}<pre>{html.escape(lyrics_text)}</pre></div></body></html>"""

        import html # Import needed for escaping
        safe_title = re.sub(r'[^\w\-]+', '_', track_title)[:50]
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        temp_filename = f"lyrics_{safe_title}_{video_id}_{timestamp}.html"
        temp_filepath = os.path.join(SCRIPT_DIR, temp_filename)
        sent_file_msg = None
        try:
            with open(temp_filepath, 'w', encoding='utf-8') as f:
                f.write(html_content)
            logger.debug(f"Saved temporary HTML lyrics file: {temp_filepath}")

            caption = f"📜 Текст песни '{track_title}' (слишком длинный, в файле)"
            display_filename = f"{safe_title}_lyrics.html"
            sent_file_msg = await client.send_file(
                event.chat_id,
                file=temp_filepath,
                caption=caption,
                attributes=[types.DocumentAttributeFilename(file_name=display_filename)],
                force_document=True
            )
            await store_response_message(event.chat_id, sent_file_msg)
            logger.info(f"Sent lyrics for '{track_title}' as HTML file.")

        except Exception as e_html:
            logger.error(f"Failed to create/send HTML lyrics file for {video_id}: {e_html}", exc_info=True)
            fail_msg = await event.reply(f"❌ Не удалось отправить текст песни '{track_title}' в виде файла.")
            await store_response_message(event.chat_id, fail_msg)
        finally:
            if os.path.exists(temp_filepath):
                logger.debug(f"Cleaning up temporary HTML file: {temp_filepath}")
                await cleanup_files(temp_filepath)


# =============================================================================
#                         COMMAND HANDLERS
# =============================================================================

@client.on(events.NewMessage)
async def handle_message(event: events.NewMessage.Event):
    """Main handler for incoming messages."""

    if not config.get("bot_enabled", True): return
    if not event.message or not event.message.text or event.message.via_bot or not event.sender_id:
        return

    is_self = event.message.out
    sender_id = event.sender_id
    is_authorised = is_self or (not config.get("whitelist_enabled", True)) or (sender_id in ALLOWED_USERS)
    if not is_authorised:
        if not is_self: logger.warning(f"Ignoring unauthorized message from user: {sender_id} in chat {event.chat_id}")
        return

    message_text = event.message.text
    prefix = config.get("prefix", ",")
    if not message_text.startswith(prefix): return

    command_string = message_text[len(prefix):].strip()
    if not command_string: return

    parts = command_string.split(maxsplit=1)
    command = parts[0].lower()
    args_str = parts[1] if len(parts) > 1 else ""
    args = args_str.split()

    logger.info(f"Received command: '{command}', Args: {args}, User: {sender_id}, Chat: {event.chat_id}")

    if is_self:
        try: await event.message.delete(); logger.debug(f"Deleted self-command message {event.message.id}")
        except Exception as e_del: logger.warning(f"Failed to delete self-command message {event.message.id}: {e_del}")

    commands_to_clear_for = (
        "search", "see", "last", "list", "host", "download", "help", "dl",
        "rec", "alast", "likes", "text", "lyrics", "clear"
    )
    if config.get("auto_clear", True) and command in commands_to_clear_for:
         logger.debug(f"Auto-clearing previous responses for '{command}' in chat {event.chat_id}")
         await clear_previous_responses(event.chat_id)

    handlers = {
        "search": handle_search,
        "see": handle_see,
        "download": handle_download,
        "dl": handle_download,
        "add": handle_add,
        "delete": handle_delete,
        "del": handle_delete,
        "list": handle_list,
        "help": handle_help,
        "last": handle_last,
        "host": handle_host,
        "clear": handle_clear,
        "rec": handle_recommendations,
        "alast": handle_history,
        "likes": handle_liked_songs,
        "text": handle_lyrics,
        "lyrics": handle_lyrics,
    }

    handler_func = handlers.get(command)

    if handler_func:
        try:
            await handler_func(event, args)
        except Exception as e_handler:
            error_details = traceback.format_exc()
            logger.error(f"Error executing handler for command '{command}': {e_handler}\n{error_details}")
            try:
                error_msg_text = (f"❌ Произошла внутренняя ошибка при обработке команды `{command}`.\n"
                                  f"```\n{type(e_handler).__name__}: {e_handler}\n```"
                                  f"\n_Подробности записаны в лог._")
                error_msg = await event.reply(error_msg_text)
                await store_response_message(event.chat_id, error_msg)
            except Exception as notify_e:
                logger.error(f"Failed to notify user about handler error for command '{command}': {notify_e}")
    else:
        response_msg_text = f"⚠️ Неизвестная команда: `{command}`.\nИспользуйте `{prefix}help` для списка команд."
        response_msg = await event.reply(response_msg_text)
        await store_response_message(event.chat_id, response_msg)
        logger.warning(f"Unknown command '{command}' received from {sender_id}")

# -------------------------
# Command: clear
# -------------------------
async def handle_clear(event: events.NewMessage.Event, args: List[str]):
    """Clears previous bot responses in the chat."""
    if config.get("auto_clear", True):
        await event.respond("ℹ️ Предыдущие ответы очищаются автоматически.", delete_in=10)
        logger.info(f"Executed 'clear' command (auto-clear enabled) in chat {event.chat_id}.")
    else:
        logger.info(f"Executing manual clear via command in chat {event.chat_id}.")
        await clear_previous_responses(event.chat_id)
        await event.respond("✅ Предыдущие ответы бота очищены вручную.", delete_in=10)

# -------------------------
# Command: search (-t, -a, -p, -e)
# -------------------------
async def handle_search(event: events.NewMessage.Event, args: List[str]):
    """Handles the search command."""
    valid_flags = {"-t", "-a", "-p", "-e"}
    prefix = config.get("prefix", ",")

    if len(args) < 1:
        usage_text = (f"**Использование:** `{prefix}search -t|-a|-p|-e <запрос>`")
        await store_response_message(event.chat_id, await event.reply(usage_text))
        return

    search_type_flag = args[0]
    if search_type_flag not in valid_flags:
         await store_response_message(event.chat_id, await event.reply(f"⚠️ Неверный флаг: `{search_type_flag}`. Используйте `-t`, `-a`, `-p`, `-e`."))
         return

    query = " ".join(args[1:]).strip()
    if not query:
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не указан запрос после флага `{search_type_flag}`."))
        return

    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    sent_message = None

    try:
        if use_progress:
            statuses = {"Поиск": "⏳ Ожидание...", "Форматирование": "⏸️"}
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

        if use_progress:
            statuses["Поиск"] = f"🔄 Поиск '{query[:30]}...'..." if len(query)>33 else f"🔄 Поиск '{query}'..."
            await update_progress(progress_message, statuses)

        search_limit = min(max(1, config.get("default_search_limit", 8)), 15)
        results = await search(query, search_type_flag, search_limit)

        if use_progress:
            search_status = f"✅ Найдено: {len(results)}" if results else "ℹ️ Ничего не найдено"
            statuses["Поиск"] = search_status
            statuses["Форматирование"] = "🔄 Подготовка..." if results else "➖"
            await update_progress(progress_message, statuses)

        if not results:
            final_message = f"ℹ️ По запросу `{query}` ничего не найдено."
            if progress_message: await progress_message.edit(final_message); sent_message = progress_message
            else: sent_message = await event.reply(final_message)
        else:
            response_lines = []
            display_limit = min(len(results), MAX_SEARCH_RESULTS_DISPLAY)
            type_labels = {"-t": "Треки", "-a": "Альбомы", "-p": "Плейлисты", "-e": "Исполнители"}
            response_text = f"**🔎 Результаты поиска ({type_labels.get(search_type_flag, '?')}) для `{query}`:**\n"

            for i, item in enumerate(results[:display_limit]):
                if not item or not isinstance(item, dict):
                    logger.warning(f"Skipping invalid item in search results: {item}")
                    continue

                line = f"{i + 1}. "
                try:
                    if search_type_flag == "-t":
                        title = item.get('title', 'Unknown Title')
                        artists = format_artists(item.get('artists'))
                        vid = item.get('videoId')
                        link = f"https://music.youtube.com/watch?v={vid}" if vid else None
                        line += f"**{title}** - {artists}" + (f"\n   └ [Ссылка]({link})" if link else "")

                    elif search_type_flag == "-a":
                        title = item.get('title', 'Unknown Album')
                        artists = format_artists(item.get('artists'))
                        bid = item.get('browseId')
                        year = item.get('year', '')
                        link = f"https://music.youtube.com/browse/{bid}" if bid else None
                        line += f"**{title}** - {artists}" + (f" ({year})" if year else "") + (f"\n   └ [Ссылка]({link})" if link else "")

                    elif search_type_flag == "-e":
                        artist_name = item.get('artist', 'Unknown Artist')
                        bid = item.get('browseId')
                        link = f"https://music.youtube.com/channel/{bid}" if bid else None
                        if artist_name != 'Unknown Artist' and link:
                            line += f"**{artist_name}**\n   └ [Ссылка]({link})"
                        else: line = None

                    elif search_type_flag == "-p":
                        title = item.get('title', 'Unknown Playlist')
                        author = format_artists(item.get('author'))
                        pid_raw = item.get('browseId')
                        pid = pid_raw.replace('VL', '') if pid_raw and isinstance(pid_raw, str) else None
                        link = f"https://music.youtube.com/playlist?list={pid}" if pid else None
                        line += f"**{title}** (Автор: {author})" + (f"\n   └ [Ссылка]({link})" if link else "")

                    if line: response_lines.append(line)

                except Exception as fmt_e:
                     logger.error(f"Error formatting search result item {i+1}: {item} - {fmt_e}")
                     response_lines.append(f"{i + 1}. ⚠️ Ошибка форматирования.")

            response_text += "\n\n".join(response_lines)
            if len(results) > display_limit:
                response_text += f"\n\n... и еще {len(results) - display_limit}."

            if use_progress:
                statuses["Форматирование"] = "✅ Готово"
                await update_progress(progress_message, statuses)
                await progress_message.edit(response_text, link_preview=False)
                sent_message = progress_message
            else:
                sent_message = await event.reply(response_text, link_preview=False)

    except ValueError as e:
        error_text = f"⚠️ Ошибка поиска: {e}"
        logger.warning(error_text)
        if progress_message: await progress_message.edit(error_text); sent_message = progress_message
        else: sent_message = await event.reply(error_text)
    except Exception as e:
        logger.error(f"Неожиданная ошибка в команде search: {e}", exc_info=True)
        error_text = f"❌ Произошла неожиданная ошибка:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
            statuses["Поиск"] = str(statuses.get("Поиск", "⏸️")).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌")
            statuses["Форматирование"] = "❌"
            try: await update_progress(progress_message, statuses)
            except: pass
            try:
                await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}")
                sent_message = progress_message
            except: sent_message = await event.reply(error_text)
        else: sent_message = await event.reply(error_text)
    finally:
        if sent_message: await store_response_message(event.chat_id, sent_message)

# -------------------------
# Command: see (-t, -a, -p, -e) [-i] [-txt]
# -------------------------
async def handle_see(event: events.NewMessage.Event, args: List[str]):
    """Handles the 'see' command."""
    valid_flags = {"-t", "-a", "-p", "-e"}
    prefix = config.get("prefix", ",")

    if not args:
        usage = (f"**Использование:** `{prefix}see [-t|-a|-p|-e] [-i] [-txt] <ID или ссылка>`")
        await store_response_message(event.chat_id, await event.reply(usage))
        return

    entity_type_hint_flag = next((arg for arg in args if arg in valid_flags), None)
    include_cover = "-i" in args
    include_lyrics = "-txt" in args
    link_or_id_arg = next((arg for arg in reversed(args) if arg not in valid_flags and arg not in ["-i", "-txt"]), None)

    if not link_or_id_arg:
        await store_response_message(event.chat_id, await event.reply("⚠️ Не указана ссылка или ID."))
        return

    hint_map = {"-t": "track", "-a": "album", "-p": "playlist", "-e": "artist"}
    entity_type_hint = hint_map.get(entity_type_hint_flag) if entity_type_hint_flag else None

    entity_id = extract_entity_id(link_or_id_arg)
    if not entity_id:
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не удалось распознать ID из `{link_or_id_arg}`."))
        return

    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    temp_thumb_file, processed_thumb_file, final_sent_message = None, None, None
    files_to_clean_on_exit = []
    lyrics_message_stored = False # Flag to track if lyrics message/file was stored

    try:
        if use_progress:
            statuses = {"Получение данных": "⏳ Ожидание...", "Форматирование": "⏸️"}
            if include_cover: statuses["Обложка"] = "⏸️"
            if include_lyrics and (entity_type_hint == "track" or not entity_type_hint): statuses["Текст"] = "⏸️"
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

        if use_progress: statuses["Получение данных"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)

        entity_info = await get_entity_info(entity_id, entity_type_hint)

        if not entity_info:
            result_text = f"ℹ️ Не удалось найти информацию для: `{entity_id}`"
            final_sent_message = await (progress_message.edit(result_text) if progress_message else event.reply(result_text))
        else:
            actual_entity_type = entity_info.get('_entity_type', 'unknown')
            if include_lyrics and actual_entity_type != 'track' and "Текст" in statuses:
                 statuses["Текст"] = "➖ (Только для треков)"
            if use_progress: statuses["Получение данных"] = f"✅ ({actual_entity_type})"; statuses["Форматирование"] = "🔄 Подготовка..."; await update_progress(progress_message, statuses)

            response_text = ""
            thumbnail_url = None
            title, artists = "Неизвестно", "Неизвестно" # For lyrics header

            thumbnails_list = entity_info.get('thumbnails') or \
                              (entity_info.get('thumbnail') or {}).get('thumbnails') or \
                              (entity_info.get('videoDetails') or {}).get('thumbnails')
            if isinstance(thumbnails_list, list) and thumbnails_list:
                try:
                    highest_res_thumb = sorted(thumbnails_list, key=lambda x: x.get('width', 0) * x.get('height', 0), reverse=True)[0]
                    thumbnail_url = highest_res_thumb.get('url')
                except (IndexError, KeyError, TypeError):
                    if thumbnails_list: thumbnail_url = thumbnails_list[-1].get('url')
            if thumbnail_url: logger.debug(f"Found thumbnail URL: {thumbnail_url}")


            if actual_entity_type == 'track':
                details = entity_info.get('videoDetails') or entity_info
                title = details.get('title', 'Неизвестно')
                artists = format_artists(details.get('artists') or details.get('author'))
                album_info = details.get('album')
                album_name = album_info.get('name') if isinstance(album_info, dict) else None
                album_id = album_info.get('id') if isinstance(album_info, dict) else None
                duration_s = None
                try: duration_s = int(details.get('lengthSeconds', 0))
                except (ValueError, TypeError): pass
                duration_fmt = "N/A"
                if duration_s and duration_s > 0:
                    td = datetime.timedelta(seconds=duration_s)
                    mins, secs = divmod(td.seconds, 60)
                    hours, mins = divmod(mins, 60)
                    duration_fmt = f"{hours}:{mins:02}:{secs:02}" if hours > 0 else f"{mins}:{secs:02}"

                video_id = details.get('videoId', entity_id)
                link_url = f"https://music.youtube.com/watch?v={video_id}"
                lyrics_browse_id = details.get('lyrics')

                response_text = f"**Трек:** {title}\n**Исполнитель:** {artists}\n"
                if album_name:
                    album_link_url = f'https://music.youtube.com/browse/{album_id}' if album_id else None
                    album_link_md = f"[Ссылка]({album_link_url})" if album_link_url else ""
                    response_text += f"**Альбом:** {album_name} {album_link_md}\n"
                response_text += f"**Длительность:** {duration_fmt}\n"
                response_text += f"**ID:** `{video_id}`\n"
                if lyrics_browse_id: response_text += f"**Lyrics ID:** `{lyrics_browse_id}`\n"
                response_text += f"**Ссылка:** [Ссылка]({link_url})"

            elif actual_entity_type == 'album':
                title = entity_info.get('title', 'Неизвестный Альбом')
                artists = format_artists(entity_info.get('artists') or entity_info.get('author'))
                year = entity_info.get('year')
                count = entity_info.get('trackCount') or len(entity_info.get('tracks', []))
                bid_raw = entity_info.get('audioPlaylistId') or entity_info.get('browseId') or entity_id
                bid = bid_raw.replace('RDAMPL', '') if isinstance(bid_raw, str) else entity_id
                link_url = f"https://music.youtube.com/browse/{bid}"

                response_text = f"**Альбом:** {title}\n**Исполнитель:** {artists}\n"
                if year: response_text += f"**Год:** {year}\n"
                if count: response_text += f"**Треков:** {count}\n"
                response_text += f"**ID:** `{bid}`\n"
                response_text += f"**Ссылка:** [Ссылка]({link_url})\n"
                tracks = entity_info.get('tracks', [])
                if tracks:
                    response_text += f"\n**Треки (первые {min(len(tracks), 5)}):**\n"
                    for t in tracks[:5]:
                        t_title = t.get('title','?')
                        t_artists = format_artists(t.get('artists')) or artists
                        t_id = t.get('videoId')
                        t_link_url = f'https://music.youtube.com/watch?v={t_id}' if t_id else None
                        t_link_md = f"[Ссылка]({t_link_url})" if t_link_url else ""
                        response_text += f"• {t_title} ({t_artists}) {t_link_md}\n"
                response_text = response_text.strip()

            elif actual_entity_type == 'playlist':
                 title = entity_info.get('title', 'Неизвестный Плейлист')
                 author = format_artists(entity_info.get('author'))
                 count = entity_info.get('trackCount') or len(entity_info.get('tracks', []))
                 pid_raw = entity_info.get('id', entity_id)
                 pid = pid_raw.replace('VL', '') if isinstance(pid_raw, str) else entity_id
                 link_url = f"https://music.youtube.com/playlist?list={pid}"

                 response_text = f"**Плейлист:** {title}\n**Автор:** {author}\n"
                 if count: response_text += f"**Треков:** {count}\n"
                 response_text += f"**ID:** `{pid}`\n"
                 response_text += f"**Ссылка:** [Ссылка]({link_url})\n"
                 tracks = entity_info.get('tracks', [])
                 if tracks:
                     response_text += f"\n**Треки (первые {min(len(tracks), 5)}):**\n"
                     for t in tracks[:5]:
                         t_title = t.get('title','?')
                         t_artists = format_artists(t.get('artists'))
                         t_id = t.get('videoId')
                         t_link_url = f'https://music.youtube.com/watch?v={t_id}' if t_id else None
                         t_link_md = f"[Ссылка]({t_link_url})" if t_link_url else ""
                         response_text += f"• {t_title} ({t_artists}) {t_link_md}\n"
                 response_text = response_text.strip()

            elif actual_entity_type == 'artist':
                 name = entity_info.get('name', 'Неизвестный Исполнитель')
                 subs = entity_info.get('subscriberCountText')
                 songs_limit = config.get("artist_top_songs_limit", 5)
                 albums_limit = config.get("artist_albums_limit", 3)
                 songs_data = entity_info.get("songs", {})
                 songs = songs_data.get("results", []) if isinstance(songs_data, dict) else []
                 albums_data = entity_info.get("albums", {})
                 albums = albums_data.get("results", []) if isinstance(albums_data, dict) else []
                 cid = entity_info.get('channelId', entity_id)
                 link_url = f"https://music.youtube.com/channel/{cid}"

                 response_text = f"**Исполнитель:** {name}\n"
                 if subs: response_text += f"**Подписчики:** {subs}\n"
                 response_text += f"**ID:** `{cid}`\n"
                 response_text += f"**Ссылка:** [Ссылка]({link_url})\n"
                 if songs:
                     response_text += f"\n**Популярные треки (до {min(len(songs), songs_limit)}):**\n"
                     for s in songs[:songs_limit]:
                         s_title = s.get('title','?')
                         s_id = s.get('videoId')
                         s_link_url = f'https://music.youtube.com/watch?v={s_id}' if s_id else None
                         s_link_md = f"[Ссылка]({s_link_url})" if s_link_url else ""
                         response_text += f"• {s_title} {s_link_md}\n"
                 if albums:
                     if songs: response_text += "\n"
                     response_text += f"**Альбомы (до {min(len(albums), albums_limit)}):**\n"
                     for a in albums[:albums_limit]:
                         a_title = a.get('title','?')
                         a_id = a.get('browseId')
                         a_link_url = f'https://music.youtube.com/browse/{a_id}' if a_id else None
                         a_link_md = f"[Ссылка]({a_link_url})" if a_link_url else ""
                         a_year = a.get('year','')
                         response_text += f"• {a_title}" + (f" ({a_year})" if a_year else "") + f" {a_link_md}\n"
                 response_text = response_text.strip()
            else:
                response_text = f"⚠️ Тип '{actual_entity_type}' не поддерживается для `see`.\nID: `{entity_id}`"
                logger.warning(f"Unsupported entity type for 'see': {actual_entity_type}, ID: {entity_id}")

            if use_progress: statuses["Форматирование"] = "✅ Готово"; await update_progress(progress_message, statuses)

            if include_cover and thumbnail_url:
                if use_progress: statuses["Обложка"] = "🔄 Загрузка..."; await update_progress(progress_message, statuses)
                temp_thumb_file = await download_thumbnail(thumbnail_url)

                if temp_thumb_file:
                    files_to_clean_on_exit.append(temp_thumb_file)
                    if use_progress: statuses["Обложка"] = "🔄 Обработка..."; await update_progress(progress_message, statuses)

                    if actual_entity_type == 'artist':
                        processed_thumb_file = temp_thumb_file
                    else:
                        processed_thumb_file = crop_thumbnail(temp_thumb_file)
                        if processed_thumb_file and processed_thumb_file != temp_thumb_file:
                            files_to_clean_on_exit.append(processed_thumb_file)

                    status_icon = "✅" if processed_thumb_file and os.path.exists(processed_thumb_file) else "⚠️"
                    if use_progress: statuses["Обложка"] = f"{status_icon} Готово"; await update_progress(progress_message, statuses)

                    if processed_thumb_file and os.path.exists(processed_thumb_file):
                        try:
                            if use_progress: statuses["Обложка"] = "🔄 Отправка..."; await update_progress(progress_message, statuses)
                            final_sent_message = await client.send_file(
                                event.chat_id, file=processed_thumb_file, caption=response_text, link_preview=False
                            )
                            if progress_message: await progress_message.delete(); progress_message = None
                        except Exception as send_e:
                            logger.error(f"Failed to send file with cover {os.path.basename(processed_thumb_file)}: {send_e}", exc_info=True)
                            if use_progress: statuses["Обложка"] = "❌ Ошибка отправки"; await update_progress(progress_message, statuses)
                            fallback_text = f"{response_text}\n\n_(Ошибка при отправке обложки)_"
                            final_sent_message = await (progress_message.edit(fallback_text, link_preview=False) if progress_message else event.reply(fallback_text, link_preview=False))
                    else:
                        logger.warning(f"Thumbnail processing failed. Sending text only.")
                        if use_progress: statuses["Обложка"] = "❌ Ошибка обработки"; await update_progress(progress_message, statuses)
                        fallback_text = f"{response_text}\n\n_(Ошибка при обработке обложки)_"
                        final_sent_message = await (progress_message.edit(fallback_text, link_preview=False) if progress_message else event.reply(fallback_text, link_preview=False))
                else:
                     logger.warning(f"Thumbnail download failed. Sending text only.")
                     if use_progress: statuses["Обложка"] = "❌ Ошибка загрузки"; await update_progress(progress_message, statuses)
                     fallback_text = f"{response_text}\n\n_(Ошибка при загрузке обложки)_"
                     final_sent_message = await (progress_message.edit(fallback_text, link_preview=False) if progress_message else event.reply(fallback_text, link_preview=False))
            else:
                final_sent_message = await (progress_message.edit(response_text, link_preview=False) if progress_message else event.reply(response_text, link_preview=False))

            # --- Handle Lyrics Request (-txt) ---
            if include_lyrics and actual_entity_type == 'track':
                if use_progress: statuses["Текст"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)
                video_id_for_lyrics = entity_info.get('videoId') or entity_info.get('videoDetails', {}).get('videoId') or entity_id
                lyrics_browse_id_for_lyrics = entity_info.get('lyrics') or entity_info.get('videoDetails', {}).get('lyrics')
                lyrics_data = await get_lyrics_for_track(video_id_for_lyrics, lyrics_browse_id_for_lyrics)

                if lyrics_data and lyrics_data.get('lyrics'):
                    lyrics_text = lyrics_data['lyrics']
                    lyrics_source = lyrics_data.get('source')
                    # Use title/artists fetched earlier for this track
                    lyrics_header = f"📜 **Текст песни:** {title} - {artists}"
                    if lyrics_source: lyrics_header += f"\n_(Источник: {lyrics_source})_"
                    lyrics_header += "\n" + ("-"*15)

                    if use_progress: statuses["Текст"] = "✅ Отправка..."; await update_progress(progress_message, statuses)

                    # Use send_lyrics helper (handles long texts and file sending)
                    await send_lyrics(event, lyrics_text, lyrics_header, title, video_id_for_lyrics)
                    lyrics_message_stored = True # Flag that lyrics output was generated

                    if use_progress: statuses["Текст"] = "✅ Отправлено"; await update_progress(progress_message, statuses)

                else:
                    logger.info(f"Текст не найден для трека ID {video_id_for_lyrics}.")
                    if use_progress: statuses["Текст"] = "ℹ️ Не найден"; await update_progress(progress_message, statuses)
                    no_lyrics_msg = await event.respond("_Текст для этого трека не найден._", reply_to=final_sent_message.id if final_sent_message else None)
                    await store_response_message(event.chat_id, no_lyrics_msg)
                    await asyncio.sleep(5)
                    try: await no_lyrics_msg.delete()
                    except: pass

            if progress_message:
                 await asyncio.sleep(5)
                 try: await progress_message.delete()
                 except: pass
                 progress_message = None

    except Exception as e:
        logger.error(f"Unexpected error in handle_see for ID '{entity_id}': {e}", exc_info=True)
        error_text = f"❌ Ошибка при получении информации:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
             for task in statuses: statuses[task] = str(statuses[task]).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌").replace("⏸️", "❌")
             try: await update_progress(progress_message, statuses)
             except: pass
             try:
                 await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}")
                 final_sent_message = progress_message
             except: final_sent_message = await event.reply(error_text)
        else: final_sent_message = await event.reply(error_text)
    finally:
        # Store main info message only if it wasn't replaced by lyrics
        if final_sent_message and (final_sent_message != progress_message) and not lyrics_message_stored:
            await store_response_message(event.chat_id, final_sent_message)

        if files_to_clean_on_exit:
            logger.debug(f"Running cleanup for handle_see (Files: {len(files_to_clean_on_exit)})")
            await cleanup_files(*files_to_clean_on_exit)

# -------------------------
# Helper: Send Single Track
# -------------------------
async def send_single_track(event: events.NewMessage.Event, info: Dict, file_path: str):
    """
    Handles sending a single downloaded audio file via Telegram.
    """
    temp_telegram_thumb, processed_telegram_thumb = None, None
    files_to_clean = [file_path]
    title, performer, duration = "Неизвестно", "Неизвестно", 0
    sent_audio_msg = None

    try:
        if not info or not file_path or not os.path.exists(file_path):
             logger.error(f"send_single_track called with invalid info or missing file")
             await event.reply(f"❌ Ошибка: Не найден скачанный файл `{os.path.basename(file_path or 'N/A')}`.")
             await cleanup_files(*[f for f in files_to_clean if f != file_path])
             return None

        title, performer, duration = extract_track_metadata(info)

        thumb_url = None
        thumbnails = info.get('thumbnails') or (info.get('thumbnail') or {}).get('thumbnails')
        if isinstance(thumbnails, list) and thumbnails:
            try:
                thumb_url = sorted(thumbnails, key=lambda x: x.get('width', 0) * x.get('height', 0), reverse=True)[0].get('url')
            except (IndexError, KeyError, TypeError): thumb_url = thumbnails[-1].get('url')

        if thumb_url:
            logger.debug(f"Attempting download/process thumbnail for Telegram preview ('{title}')")
            temp_telegram_thumb = await download_thumbnail(thumb_url)
            if temp_telegram_thumb:
                files_to_clean.append(temp_telegram_thumb)
                processed_telegram_thumb = crop_thumbnail(temp_telegram_thumb)
                if processed_telegram_thumb and processed_telegram_thumb != temp_telegram_thumb:
                    files_to_clean.append(processed_telegram_thumb)

        logger.info(f"Отправка аудио: {os.path.basename(file_path)} (Title: '{title}', Performer: '{performer}')")
        final_telegram_thumb = processed_telegram_thumb if (processed_telegram_thumb and os.path.exists(processed_telegram_thumb)) else None

        sent_audio_msg = await client.send_file(
            event.chat_id,
            file=file_path,
            caption=BOT_CREDIT,
            attributes=[types.DocumentAttributeAudio(
                duration=duration, title=title, performer=performer
            )],
            thumb=final_telegram_thumb,
        )
        logger.info(f"Аудио успешно отправлено: {os.path.basename(file_path)} (Msg ID: {sent_audio_msg.id})")

        if config.get("recent_downloads", True):
             try:
                last_tracks = load_last_tracks()
                timestamp = datetime.datetime.now().strftime("%H:%M-%d-%m")
                artist_browse_id = None
                artists_list = info.get('artists')
                if isinstance(artists_list, list) and artists_list:
                     main_artist = next((a for a in artists_list if isinstance(a, dict) and a.get('id')), None)
                     if main_artist: artist_browse_id = main_artist['id']
                browse_id_to_save = artist_browse_id or info.get('channel_id') or info.get('uploader_id') or 'N/A'
                new_entry = [title, performer, browse_id_to_save, timestamp]
                last_tracks.insert(0, new_entry)
                save_last_tracks(last_tracks)
             except Exception as e_last:
                 logger.error(f"Не удалось обновить список последних треков ({title}): {e_last}", exc_info=True)

        return sent_audio_msg

    except types.errors.MediaCaptionTooLongError:
         logger.error(f"Ошибка отправки {os.path.basename(file_path)}: подпись слишком длинная.")
         await store_response_message(event.chat_id, await event.reply(f"⚠️ Не удалось отправить `{title}`: подпись длинная."))
    except types.errors.WebpageMediaEmptyError:
          logger.error(f"Ошибка отправки {os.path.basename(file_path)}: WebpageMediaEmptyError. Попытка без превью...")
          try:
              sent_audio_msg = await client.send_file(
                  event.chat_id, file_path, caption=BOT_CREDIT,
                  attributes=[types.DocumentAttributeAudio(duration=duration, title=title, performer=performer)],
                  thumb=None
              )
              logger.info(f"Повторная отправка без превью успешна: {os.path.basename(file_path)}")
              if config.get("recent_downloads", True):
                    try:
                         last_tracks = load_last_tracks(); timestamp = datetime.datetime.now().strftime("%H:%M-%d-%m")
                         browse_id_to_save = info.get('channel_id') or 'N/A'
                         artists_list_retry = info.get('artists')
                         if isinstance(artists_list_retry, list) and artists_list_retry:
                             main_artist_retry = next((a for a in artists_list_retry if isinstance(a, dict) and a.get('id')), None)
                             if main_artist_retry: browse_id_to_save = main_artist_retry['id']
                         last_tracks.insert(0, [title, performer, browse_id_to_save, timestamp]); save_last_tracks(last_tracks)
                    except Exception as e_last_retry: logger.error(f"Не удалось обновить 'last' после повторной отправки: {e_last_retry}")
              return sent_audio_msg
          except Exception as retry_e:
              logger.error(f"Повторная отправка {os.path.basename(file_path)} не удалась: {retry_e}", exc_info=True)
              await store_response_message(event.chat_id, await event.reply(f"❌ Не удалось отправить `{title}`: {retry_e}"))
    except Exception as e:
        logger.error(f"Неожиданная ошибка при отправке трека {os.path.basename(file_path or 'N/A')}: {e}", exc_info=True)
        try: await store_response_message(event.chat_id, await event.reply(f"❌ Не удалось отправить трек `{title}`: {e}"))
        except Exception as notify_e: logger.error(f"Не удалось уведомить об ошибке отправки: {notify_e}")

    finally:
        extensions_to_keep = ['.opus']
        keep_this_audio_file = False
        if file_path and os.path.exists(file_path):
            try:
                _, file_extension = os.path.splitext(file_path)
                if file_extension.lower() in extensions_to_keep:
                    keep_this_audio_file = True
                    logger.info(f"Файл '{os.path.basename(file_path)}' будет сохранен.")
            except Exception as e_ext: logger.warning(f"Не удалось проверить расширение {file_path}: {e_ext}")

        final_cleanup_list = []
        for f in files_to_clean:
            if f == file_path:
                if not keep_this_audio_file: final_cleanup_list.append(f)
            elif f: final_cleanup_list.append(f)

        if final_cleanup_list:
            logger.debug(f"Запуск очистки для send_single_track (Файлов: {len(final_cleanup_list)})")
            await cleanup_files(*final_cleanup_list)
        else:
             logger.debug(f"Очистка send_single_track: Нет файлов для удаления.")

    return None

# -------------------------
# Command: download (-t, -a) [-txt] / dl
# -------------------------
async def handle_download(event: events.NewMessage.Event, args: List[str]):
    """Handles the download command."""
    valid_flags = {"-t", "-a"}
    prefix = config.get("prefix", ",")

    if not args:
        usage = (f"**Использование:** `{prefix}dl -t|-a [-txt] <ссылка>`")
        await store_response_message(event.chat_id, await event.reply(usage))
        return

    download_type_flag = next((arg for arg in args if arg in valid_flags), None)
    if not download_type_flag:
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не указан тип (`-t` или `-a`)."))
        return

    include_lyrics = "-txt" in args and download_type_flag == "-t"
    link = next((arg for arg in reversed(args) if arg not in valid_flags and arg != "-txt"), None)

    if not link or not isinstance(link, str) or not link.startswith("http"):
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не найдена http(s) ссылка."))
        return

    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    final_sent_message: Optional[types.Message] = None

    try:
        if download_type_flag == "-t":
            if use_progress:
                statuses = {"Скачивание/Обработка": "⏳ Ожидание...", "Отправка Аудио": "⏸️"}
                if include_lyrics: statuses["Отправка Текста"] = "⏸️"
                progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

            if use_progress: statuses["Скачивание/Обработка"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)

            loop = asyncio.get_running_loop()
            info, file_path = await loop.run_in_executor(None, functools.partial(download_track, link))

            if not file_path or not info:
                fail_reason = "yt-dlp не смог скачать/обработать"
                if info and not file_path: fail_reason = "yt-dlp скачал, но файл не найден"
                elif not info: fail_reason = "yt-dlp не вернул информацию"
                logger.error(f"Download failed for {link}. Reason: {fail_reason}")
                raise Exception(f"Не удалось скачать/обработать. {fail_reason}")

            file_basename = os.path.basename(file_path)
            track_title = info.get('title', file_basename)
            logger.info(f"Track download successful: {file_basename}")
            if use_progress:
                 display_name = (track_title[:30] + '...') if len(track_title) > 33 else track_title
                 statuses["Скачивание/Обработка"] = f"✅ ({display_name})"
                 statuses["Отправка Аудио"] = "🔄 Подготовка..."
                 await update_progress(progress_message, statuses)

            sent_audio_message = await send_single_track(event, info, file_path)

            if sent_audio_message:
                if use_progress: statuses["Отправка Аудио"] = "✅ Готово"; await update_progress(progress_message, statuses)

                if include_lyrics:
                    if use_progress: statuses["Отправка Текста"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)
                    video_id = info.get('id') or info.get('videoId')
                    lyrics_browse_id = info.get('lyrics')
                    lyrics_data = await get_lyrics_for_track(video_id, lyrics_browse_id)

                    if lyrics_data and lyrics_data.get('lyrics'):
                         if use_progress: statuses["Отправка Текста"] = "✅ Отправка..."; await update_progress(progress_message, statuses)
                         lyrics_text = lyrics_data['lyrics']
                         lyrics_source = lyrics_data.get('source')
                         dl_track_title = info.get('title', 'Неизвестный трек')
                         artists_data_for_lyrics = info.get('artists')
                         artists = format_artists(artists_data_for_lyrics)
                         if not artists_data_for_lyrics:
                             logger.warning(f"Artist info missing in download info dict for {video_id}. Lyrics header fallback.")
                         lyrics_header = f"📜 **Текст песни:** {dl_track_title} - {artists or 'Неизвестный исполнитель'}"
                         if lyrics_source: lyrics_header += f"\n_(Источник: {lyrics_source})_"
                         lyrics_header += "\n" + ("-"*15)

                         # Use helper for sending lyrics (handles file/message split)
                         await send_lyrics(event, lyrics_text, lyrics_header, dl_track_title, video_id)

                         if use_progress: statuses["Отправка Текста"] = "✅ Отправлено"; await update_progress(progress_message, statuses)
                    else:
                         logger.info(f"Текст не найден для '{track_title}' ({video_id}) при скачивании.")
                         if use_progress: statuses["Отправка Текста"] = "ℹ️ Не найден"; await update_progress(progress_message, statuses)
                         no_lyrics_msg = await event.respond("_Текст для этого трека не найден._", reply_to=sent_audio_message.id)
                         await store_response_message(event.chat_id, no_lyrics_msg)
                         await asyncio.sleep(5); await no_lyrics_msg.delete()
            else:
                 if use_progress: statuses["Отправка Аудио"] = "❌ Ошибка"; await update_progress(progress_message, statuses)

            if progress_message:
                 await asyncio.sleep(1); await progress_message.delete(); progress_message = None

        elif download_type_flag == "-a":
            album_or_playlist_id = extract_entity_id(link)
            if not album_or_playlist_id:
                 await store_response_message(event.chat_id, await event.reply(f"⚠️ Не удалось извлечь ID из ссылки: `{link}`"))
                 return

            album_title = album_or_playlist_id
            total_tracks = 0
            downloaded_count = 0
            sent_count = 0
            progress_callback = None

            statuses = {}
            if use_progress:
                async def album_progress_updater(status_key, **kwargs):
                    nonlocal total_tracks, downloaded_count, sent_count, album_title
                    if not use_progress or not progress_message: return
                    current_statuses = statuses
                    try:
                        if status_key == "analysis_complete":
                            total_tracks = kwargs.get('total_tracks', 0)
                            temp_title = kwargs.get('title', album_or_playlist_id)
                            album_title = (temp_title[:40] + '...') if len(temp_title) > 43 else temp_title
                            current_statuses["Альбом"] = f"'{album_title}' ({total_tracks} тр.)"
                            current_statuses["Прогресс"] = f"▶️ Начинаем... (0/{total_tracks})"
                        elif status_key == "track_downloading":
                            curr_dl = kwargs.get('current', downloaded_count + 1)
                            perc = kwargs.get('percentage', 0)
                            title = kwargs.get('title', '?')
                            current_statuses["Прогресс"] = f"📥 {curr_dl}/{total_tracks} ({perc}%) - '{title}'"
                        elif status_key == "track_downloaded":
                            curr_ok = kwargs.get('current', downloaded_count)
                            perc = int((curr_ok / total_tracks) * 100) if total_tracks else 0
                            title = kwargs.get('title', '?')
                            current_statuses["Прогресс"] = f"✅ Загружен {curr_ok}/{total_tracks} ({perc}%) - '{title}'"
                        elif status_key == "track_sending":
                            curr_send_idx = kwargs.get('current_index', sent_count)
                            total_dl = kwargs.get('total_downloaded', downloaded_count)
                            title = kwargs.get('title', '?')
                            current_statuses["Прогресс"] = f"📤 Отправка {curr_send_idx+1}/{total_dl} - '{title}'"
                        elif status_key == "track_sent":
                            curr_sent_ok = kwargs.get('current_sent', sent_count)
                            total_dl = kwargs.get('total_downloaded', downloaded_count)
                            title = kwargs.get('title', '?')
                            current_statuses["Прогресс"] = f"✔️ Отправлен {curr_sent_ok}/{total_dl} - '{title}'"
                        elif status_key == "track_failed":
                            curr_fail_idx = kwargs.get('current', downloaded_count + 1)
                            title = kwargs.get('title', '?')
                            reason = kwargs.get('reason', 'Ошибка')
                            current_statuses["Прогресс"] = f"⚠️ {reason} {curr_fail_idx}/{total_tracks} - '{title}'"
                        elif status_key == "album_error":
                            err_msg = kwargs.get('error', 'Ошибка')
                            current_statuses["Альбом"] = f"❌ Ошибка: {err_msg[:40]}"
                            current_statuses["Прогресс"] = "⏹️ Остановлено"
                        await update_progress(progress_message, current_statuses)
                    except Exception as e_prog:
                        logger.error(f"Ошибка при обновлении прогресса: {e_prog}")
                progress_callback = album_progress_updater

            if use_progress:
                statuses = {"Альбом": f"🔄 Анализ ID '{album_or_playlist_id}'...", "Прогресс": "⏸️"}
                progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

            logger.info(f"Starting sequential download/send for: {album_or_playlist_id}")
            downloaded_tuples = await download_album_tracks(album_or_playlist_id, progress_callback)
            downloaded_count = len(downloaded_tuples)

            if downloaded_count == 0:
                if progress_callback: await progress_callback("album_error", error="Треки не скачаны")
                raise Exception(f"Не удалось скачать ни одного трека для `{album_title}`.")

            logger.info(f"Starting sequential sending of {downloaded_count} tracks for '{album_title}'...")

            for i, (info, file_path) in enumerate(downloaded_tuples):
                track_title_send = (info.get('title', os.path.basename(file_path)) if info else os.path.basename(file_path))
                short_title = (track_title_send[:25] + '...') if len(track_title_send) > 28 else track_title_send

                if not file_path or not os.path.exists(file_path):
                     logger.error(f"File path missing for track {i+1}/{downloaded_count}: {file_path}. Skipping send.")
                     if progress_callback:
                          await progress_callback("track_failed", current=i+1, total=total_tracks, title=short_title, reason="Файл не найден")
                     continue

                if progress_callback:
                    await progress_callback("track_sending", current_index=i, total_downloaded=downloaded_count, title=short_title)

                sent_msg_track = await send_single_track(event, info, file_path)

                if sent_msg_track:
                    sent_count += 1
                    if progress_callback:
                         await progress_callback("track_sent", current_sent=sent_count, total_downloaded=downloaded_count, title=short_title)
                else:
                    logger.warning(f"Failed to send track {i+1}/{downloaded_count}: {short_title}")
                    if progress_callback:
                          await progress_callback("track_failed", current=i+1, total=total_tracks, title=short_title, reason="Ошибка отправки")

                await asyncio.sleep(0.5)

            if use_progress and progress_message:
                final_icon = "✅" if sent_count == downloaded_count else "⚠️"
                statuses["Прогресс"] = f"{final_icon} Завершено: Отправлено {sent_count}/{downloaded_count} треков."
                try:
                    await update_progress(progress_message, statuses)
                    await asyncio.sleep(5)
                    await progress_message.delete(); progress_message = None
                except Exception as e_final_prog:
                     logger.warning(f"Could not update/delete final progress message: {e_final_prog}")

    except Exception as e:
        logger.error(f"Ошибка при выполнении download ({download_type_flag}, {link}): {e}", exc_info=True)
        error_prefix = "⚠️" if isinstance(e, (ValueError, FileNotFoundError)) else "❌"
        error_text = f"{error_prefix} Ошибка при скачивании/отправке:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
            for task in statuses: statuses[task] = str(statuses[task]).replace("🔄", "⏹️").replace("✅", "⏹️").replace("⏳", "⏹️").replace("▶️", "⏹️").replace("📥", "⏹️").replace("📤", "⏹️").replace("✔️", "⏹️")
            statuses["Прогресс"] = "❌ Ошибка!"
            try: await update_progress(progress_message, statuses)
            except: pass
            try:
                await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}")
                final_sent_message = progress_message
            except Exception as edit_e:
                logger.error(f"Не удалось изменить прогресс для ошибки: {edit_e}")
                final_sent_message = await event.reply(error_text)
        else: final_sent_message = await event.reply(error_text)
    finally:
        if final_sent_message and (final_sent_message != progress_message or progress_message is not None):
            await store_response_message(event.chat_id, final_sent_message)


# =============================================================================
#              AUTHENTICATED COMMAND HANDLERS (rec, alast, likes)
# =============================================================================

@require_ytmusic_auth
async def handle_recommendations(event: events.NewMessage.Event, args: List[str]):
    """Fetches personalized music recommendations."""
    limit = config.get("recommendations_limit", 8)
    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    sent_message = None

    try:
        if use_progress:
            statuses = {"Получение рекомендаций": "⏳ Ожидание...", "Форматирование": "⏸️"}
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

        if use_progress: statuses["Получение рекомендаций"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)

        try:
            history = await asyncio.to_thread(ytmusic.get_history)
            start_vid = history[0]['videoId'] if history and history[0].get('videoId') else None
            if start_vid:
                 logger.info(f"Using last played track ({start_vid}) as seed for recommendations.")
                 recommendations_raw = await asyncio.to_thread(ytmusic.get_watch_playlist, videoId=start_vid, radio=True, limit=limit + 5)
            else:
                 logger.info("No history found, using generic home feed suggestions.")
                 home_feed = await asyncio.to_thread(ytmusic.get_home, limit=limit + 5)
                 recommendations_raw = {'tracks': []}
                 for section in home_feed:
                     if 'contents' in section:
                         for item in section['contents']:
                             if item.get('videoId'): recommendations_raw['tracks'].append(item)
                             if len(recommendations_raw['tracks']) >= limit + 5: break
                     if len(recommendations_raw['tracks']) >= limit + 5: break

            recommendations = recommendations_raw.get('tracks', []) if recommendations_raw else []
            seen_ids = {start_vid} if start_vid else set()
            filtered_recs = []
            for track in recommendations:
                 vid = track.get('videoId')
                 if vid and vid not in seen_ids:
                     filtered_recs.append(track)
                     seen_ids.add(vid)
                 if len(filtered_recs) >= limit: break
            results = filtered_recs

        except Exception as api_e:
             logger.error(f"Failed to get recommendations API: {api_e}", exc_info=True)
             raise Exception(f"Ошибка API при получении рекомендаций: {api_e}")


        if use_progress:
            rec_status = f"✅ Найдено: {len(results)}" if results else "ℹ️ Не найдено"
            statuses["Получение рекомендаций"] = rec_status
            statuses["Форматирование"] = "🔄 Подготовка..." if results else "➖"
            await update_progress(progress_message, statuses)

        if not results:
            final_message = f"ℹ️ Не удалось найти персональные рекомендации."
            if progress_message: await progress_message.edit(final_message); sent_message = progress_message
            else: sent_message = await event.reply(final_message)
        else:
            response_lines = []
            response_text = f"🎧 **Рекомендации для вас:**\n"

            for i, item in enumerate(results):
                line = f"{i + 1}. "
                try:
                    title = item.get('title', 'Unknown Title')
                    artists = format_artists(item.get('artists'))
                    vid = item.get('videoId')
                    link_url = f"https://music.youtube.com/watch?v={vid}" if vid else None
                    album_name = (item.get('album') or {}).get('name')
                    album_part = f" (Альбом: {album_name})" if album_name else ""
                    line += f"**{title}** - {artists}{album_part}" + (f"\n   └ [Ссылка]({link_url})" if link_url else "")
                    response_lines.append(line)
                except Exception as fmt_e:
                     logger.error(f"Error formatting recommendation item {i+1}: {item} - {fmt_e}")
                     response_lines.append(f"{i + 1}. ⚠️ Ошибка форматирования.")

            response_text += "\n\n".join(response_lines)

            if use_progress:
                statuses["Форматирование"] = "✅ Готово"
                await update_progress(progress_message, statuses)
                await progress_message.edit(response_text, link_preview=False)
                sent_message = progress_message
            else:
                sent_message = await event.reply(response_text, link_preview=False)

    except Exception as e:
        logger.error(f"Ошибка в команде recommendations: {e}", exc_info=True)
        error_text = f"❌ Ошибка при получении рекомендаций:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
            statuses["Получение рекомендаций"] = "❌"
            statuses["Форматирование"] = "❌"
            try: await update_progress(progress_message, statuses)
            except: pass
            try: await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}"); sent_message = progress_message
            except: sent_message = await event.reply(error_text)
        else: sent_message = await event.reply(error_text)
    finally:
        if sent_message: await store_response_message(event.chat_id, sent_message)

@require_ytmusic_auth
async def handle_history(event: events.NewMessage.Event, args: List[str]):
    """Fetches user's listening history."""
    limit = config.get("history_limit", 10)
    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    sent_message = None

    try:
        if use_progress:
            statuses = {"Получение истории": "⏳ Ожидание...", "Форматирование": "⏸️"}
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

        if use_progress: statuses["Получение истории"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)

        try:
            results = await asyncio.to_thread(ytmusic.get_history)
        except Exception as api_e:
             logger.error(f"Failed to get history API: {api_e}", exc_info=True)
             raise Exception(f"Ошибка API при получении истории: {api_e}")

        if use_progress:
            hist_status = f"✅ Найдено: {len(results)}" if results else "ℹ️ История пуста"
            statuses["Получение истории"] = hist_status
            statuses["Форматирование"] = "🔄 Подготовка..." if results else "➖"
            await update_progress(progress_message, statuses)

        if not results:
            final_message = f"ℹ️ Ваша история прослушиваний пуста."
            if progress_message: await progress_message.edit(final_message); sent_message = progress_message
            else: sent_message = await event.reply(final_message)
        else:
            response_lines = []
            display_limit = min(len(results), limit)
            response_text = f"📜 **Недавняя история (последние {display_limit}):**\n"

            for i, item in enumerate(results[:display_limit]):
                line = f"{i + 1}. "
                try:
                    title = item.get('title', 'Unknown Title')
                    artists = format_artists(item.get('artists'))
                    vid = item.get('videoId')
                    link_url = f"https://music.youtube.com/watch?v={vid}" if vid else None
                    album_name = (item.get('album') or {}).get('name')
                    album_part = f" (Альбом: {album_name})" if album_name else ""
                    line += f"**{title}** - {artists}{album_part}" + (f"\n   └ [Ссылка]({link_url})" if link_url else "")
                    response_lines.append(line)
                except Exception as fmt_e:
                     logger.error(f"Error formatting history item {i+1}: {item} - {fmt_e}")
                     response_lines.append(f"{i + 1}. ⚠️ Ошибка форматирования.")

            response_text += "\n\n".join(response_lines)

            if use_progress:
                statuses["Форматирование"] = "✅ Готово"
                await update_progress(progress_message, statuses)
                await progress_message.edit(response_text, link_preview=False)
                sent_message = progress_message
            else:
                sent_message = await event.reply(response_text, link_preview=False)

    except Exception as e:
        logger.error(f"Ошибка в команде history: {e}", exc_info=True)
        error_text = f"❌ Ошибка при получении истории:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
            statuses["Получение истории"] = "❌"
            statuses["Форматирование"] = "❌"
            try: await update_progress(progress_message, statuses)
            except: pass
            try: await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}"); sent_message = progress_message
            except: sent_message = await event.reply(error_text)
        else: sent_message = await event.reply(error_text)
    finally:
        if sent_message: await store_response_message(event.chat_id, sent_message)

@require_ytmusic_auth
async def handle_liked_songs(event: events.NewMessage.Event, args: List[str]):
    """Fetches user's liked songs playlist."""
    limit = config.get("liked_songs_limit", 15)
    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    sent_message = None

    try:
        if use_progress:
            statuses = {"Получение лайков": "⏳ Ожидание...", "Форматирование": "⏸️"}
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

        if use_progress: statuses["Получение лайков"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)

        try:
            results_raw = await asyncio.to_thread(ytmusic.get_liked_songs, limit=limit)
            results = results_raw.get('tracks', []) if results_raw else []
        except Exception as api_e:
             logger.error(f"Failed to get liked songs API: {api_e}", exc_info=True)
             raise Exception(f"Ошибка API при получении лайков: {api_e}")

        if use_progress:
            like_status = f"✅ Найдено: {len(results)}" if results else "ℹ️ Лайков не найдено"
            statuses["Получение лайков"] = like_status
            statuses["Форматирование"] = "🔄 Подготовка..." if results else "➖"
            await update_progress(progress_message, statuses)

        if not results:
            final_message = f"ℹ️ Плейлист 'Мне понравилось' пуст."
            if progress_message: await progress_message.edit(final_message); sent_message = progress_message
            else: sent_message = await event.reply(final_message)
        else:
            response_lines = []
            display_limit = min(len(results), limit)
            response_text = f"👍 **Треки 'Мне понравилось' (последние {display_limit}):**\n"

            for i, item in enumerate(results[:display_limit]):
                line = f"{i + 1}. "
                try:
                    title = item.get('title', 'Unknown Title')
                    artists = format_artists(item.get('artists'))
                    vid = item.get('videoId')
                    link_url = f"https://music.youtube.com/watch?v={vid}" if vid else None
                    album_name = (item.get('album') or {}).get('name')
                    album_part = f" (Альбом: {album_name})" if album_name else ""
                    line += f"**{title}** - {artists}{album_part}" + (f"\n   └ [Ссылка]({link_url})" if link_url else "")
                    response_lines.append(line)
                except Exception as fmt_e:
                     logger.error(f"Error formatting liked song item {i+1}: {item} - {fmt_e}")
                     response_lines.append(f"{i + 1}. ⚠️ Ошибка форматирования.")

            response_text += "\n\n".join(response_lines)

            if use_progress:
                statuses["Форматирование"] = "✅ Готово"
                await update_progress(progress_message, statuses)
                await progress_message.edit(response_text, link_preview=False)
                sent_message = progress_message
            else:
                sent_message = await event.reply(response_text, link_preview=False)

    except Exception as e:
        logger.error(f"Ошибка в команде liked_songs: {e}", exc_info=True)
        error_text = f"❌ Ошибка при получении лайков:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
            statuses["Получение лайков"] = "❌"
            statuses["Форматирование"] = "❌"
            try: await update_progress(progress_message, statuses)
            except: pass
            try: await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}"); sent_message = progress_message
            except: sent_message = await event.reply(error_text)
        else: sent_message = await event.reply(error_text)
    finally:
        if sent_message: await store_response_message(event.chat_id, sent_message)

# -------------------------
# Command: text / lyrics
# -------------------------
async def handle_lyrics(event: events.NewMessage.Event, args: List[str]):
    """Fetches and displays lyrics for a track."""
    prefix = config.get("prefix", ",")

    if not args:
        usage = f"**Использование:** `{prefix}text <ID трека или ссылка>`"
        await store_response_message(event.chat_id, await event.reply(usage))
        return

    link_or_id_arg = args[0]
    video_id = extract_entity_id(link_or_id_arg)
    if not video_id or not re.fullmatch(r'[A-Za-z0-9_-]{11}', video_id):
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не удалось распознать Video ID из `{link_or_id_arg}`."))
        return

    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    sent_message = None
    lyrics_message_stored = False

    try:
        if use_progress:
            statuses = {"Поиск трека": "⏳ Ожидание...", "Получение текста": "⏸️", "Отправка": "⏸️"}
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

        track_title, track_artists = "Неизвестный трек", "Неизвестный исполнитель"
        lyrics_browse_id_from_info = None
        if use_progress: statuses["Поиск трека"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)
        try:
            track_info = await get_entity_info(video_id, entity_type_hint="track")
            if track_info:
                 details = track_info.get('videoDetails') or track_info
                 track_title = details.get('title', track_title)
                 fetched_artists_data = details.get('artists') or details.get('author')
                 track_artists = format_artists(fetched_artists_data) or track_artists
                 if not fetched_artists_data:
                     logger.warning(f"Artist info missing in track_info for {video_id}. Header fallback.")
                 lyrics_browse_id_from_info = details.get('lyrics')
                 if use_progress: statuses["Поиск трека"] = f"✅ {track_title}"; await update_progress(progress_message, statuses)
            else:
                 logger.warning(f"Track info not found for {video_id}. Lyrics header will use defaults.")
                 if use_progress: statuses["Поиск трека"] = "⚠️ Не найден"; await update_progress(progress_message, statuses)
        except Exception as e_info:
             logger.warning(f"Failed to get track info for lyrics header ({video_id}): {e_info}")
             if use_progress: statuses["Поиск трека"] = "⚠️ Ошибка"; await update_progress(progress_message, statuses)

        if use_progress: statuses["Получение текста"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)
        lyrics_data = await get_lyrics_for_track(video_id, lyrics_browse_id_from_info)

        if lyrics_data and lyrics_data.get('lyrics'):
            lyrics_text = lyrics_data['lyrics']
            lyrics_source = lyrics_data.get('source')
            logger.info(f"Lyrics received for '{track_title}' ({video_id})")
            if use_progress: statuses["Получение текста"] = "✅ Получен"; statuses["Отправка"] = "🔄 Подготовка..."; await update_progress(progress_message, statuses)

            lyrics_header = f"📜 **Текст песни:** {track_title} - {track_artists}"
            if lyrics_source: lyrics_header += f"\n_(Источник: {lyrics_source})_"
            lyrics_header += "\n" + ("-"*15)

            if progress_message:
                try: await progress_message.delete(); progress_message = None
                except Exception: pass

            await send_lyrics(event, lyrics_text, lyrics_header, track_title, video_id)
            lyrics_message_stored = True

        else:
            logger.info(f"Lyrics not found for '{track_title}' ({video_id}).")
            if use_progress: statuses["Получение текста"] = "ℹ️ Не найден"; statuses["Отправка"] = "➖"; await update_progress(progress_message, statuses)
            final_message = f"ℹ️ Не удалось найти текст для трека `{track_title}` (`{video_id}`)."
            if progress_message: await progress_message.edit(final_message); sent_message = progress_message
            else: sent_message = await event.reply(final_message)

    except Exception as e:
        logger.error(f"Ошибка в команде lyrics/text для {video_id}: {e}", exc_info=True)
        error_text = f"❌ Ошибка при получении текста:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
            statuses["Поиск трека"] = str(statuses.get("Поиск трека", "⏸️")).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌")
            statuses["Получение текста"] = "❌"
            statuses["Отправка"] = "❌"
            try: await update_progress(progress_message, statuses)
            except: pass
            try: await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}"); sent_message = progress_message
            except: sent_message = await event.reply(error_text)
        else: sent_message = await event.reply(error_text)
    finally:
        if sent_message and sent_message == progress_message and not lyrics_message_stored:
             await store_response_message(event.chat_id, sent_message)


# =============================================================================
#                 ADMIN & UTILITY COMMAND HANDLERS
# =============================================================================

@retry(max_tries=2, delay=1, exceptions=(telethon.errors.FloodWaitError,))
async def handle_add(event: events.NewMessage.Event, args: List[str]):
    """Adds a user to the whitelist. Owner only."""
    global ALLOWED_USERS
    prefix = config.get("prefix", ",")

    try:
        me = await client.get_me()
        if not me or event.sender_id != me.id:
            logger.warning(f"Unauthorized attempt to use '{prefix}add' by {event.sender_id}")
            try: await event.respond("🚫 Ошибка: Только владелец может добавлять.", delete_in=10)
            except Exception: pass
            return
    except telethon.errors.FloodWaitError as e:
         logger.warning(f"Flood wait ({e.seconds}s) verifying owner for '{prefix}add'. Retrying...")
         raise
    except Exception as e_me:
        logger.error(f"Could not verify bot owner for '{prefix}add': {e_me}")
        await store_response_message(event.chat_id, await event.reply("❌ Ошибка: Не удалось проверить владельца."))
        return

    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    target_user_info = "Не определен"
    final_sent_message = None

    if use_progress:
        statuses = {"Поиск Пользователя": "⏳ Ожидание...", "Сохранение": "⏸️"}
        progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

    try:
        user_id_to_add = None
        user_entity = None

        if event.is_reply:
            reply_message = await event.get_reply_message()
            if reply_message and reply_message.sender_id:
                user_id_to_add = reply_message.sender_id
                target_user_info = f"reply to user ID: {user_id_to_add}"
                try: user_entity = await client.get_entity(user_id_to_add)
                except Exception as e_get: logger.warning(f"Could not get entity for replied user {user_id_to_add}: {e_get}")
            else:
                raise ValueError("Не удалось получить ID из ответа.")
        elif args:
            user_arg = args[0]
            target_user_info = f"arg '{user_arg}'"
            if use_progress: statuses["Поиск Пользователя"] = f"🔄 Поиск '{user_arg}'..."; await update_progress(progress_message, statuses)
            try:
                user_entity = await client.get_entity(user_arg)
                if isinstance(user_entity, types.User): user_id_to_add = user_entity.id
                else: raise ValueError(f"`{user_arg}` не пользователь.")
            except ValueError as e_lookup:
                 if "Cannot find any entity" in str(e_lookup) or "Could not find the input entity" in str(e_lookup):
                     raise ValueError(f"Не удалось найти `{user_arg}`.")
                 else: raise ValueError(f"{e_lookup}")
            except telethon.errors.FloodWaitError as e:
                 logger.warning(f"Flood wait ({e.seconds}s) looking up user {user_arg}. Aborting.")
                 raise ValueError(f"Слишком много запросов. Попробуйте через {e.seconds} сек.")
            except Exception as e_lookup_other:
                 raise ValueError(f"Ошибка при поиске `{user_arg}`: {e_lookup_other}")
        else:
            raise ValueError(f"Укажите пользователя (ID, @username, тел) или ответьте на сообщение.")

        if user_id_to_add is None: raise ValueError("Не удалось определить ID.")

        user_name = f"ID: {user_id_to_add}"
        if user_entity:
            first = getattr(user_entity, 'first_name', '') or ''
            last = getattr(user_entity, 'last_name', '') or ''
            username = getattr(user_entity, 'username', None)
            if username: user_name = f"@{username}"
            elif first or last: user_name = f"{first} {last}".strip()
        elif user_id_to_add in ALLOWED_USERS: user_name = ALLOWED_USERS[user_id_to_add]

        if use_progress: statuses["Поиск Пользователя"] = f"✅ Найден: {user_name} (`{user_id_to_add}`)"; await update_progress(progress_message, statuses)

        if user_id_to_add in ALLOWED_USERS:
            result_text = f"ℹ️ {user_name} (`{user_id_to_add}`) уже в белом списке."
            logger.info(result_text)
            final_sent_message = await (progress_message.edit(result_text) if progress_message else event.reply(result_text))
            await asyncio.sleep(7)
            if progress_message: await progress_message.delete(); progress_message=None
            return

        if use_progress: statuses["Сохранение"] = "🔄 Добавление..."; await update_progress(progress_message, statuses)
        ALLOWED_USERS[user_id_to_add] = user_name
        save_users(ALLOWED_USERS)
        if use_progress: statuses["Сохранение"] = "✅ Добавлено!"; await update_progress(progress_message, statuses)

        result_text = f"✅ {user_name} (`{user_id_to_add}`) добавлен в белый список."
        logger.info(result_text)

        if progress_message:
             await progress_message.edit(result_text)
             await asyncio.sleep(7); await progress_message.delete(); progress_message = None
        else:
             try: await event.reply(result_text, delete_in=10)
             except: await event.reply(result_text)

    except Exception as e:
        logger.error(f"Ошибка при добавлении ({target_user_info}): {e}", exc_info=False)
        error_prefix = "⚠️" if isinstance(e, ValueError) else "❌"
        error_text = f"{error_prefix} Ошибка: {e}"
        if progress_message:
            statuses["Поиск Пользователя"] = str(statuses.get("Поиск Пользователя", "⏸️")).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌")
            statuses["Сохранение"] = "❌"
            try: await update_progress(progress_message, statuses)
            except: pass
            try:
                await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}")
                final_sent_message = progress_message
            except: final_sent_message = await event.reply(error_text)
        else: final_sent_message = await event.reply(error_text)
    finally:
         if final_sent_message and final_sent_message == progress_message:
              await store_response_message(event.chat_id, final_sent_message)

@retry(max_tries=2, delay=1, exceptions=(telethon.errors.FloodWaitError,))
async def handle_delete(event: events.NewMessage.Event, args: List[str]):
    """Removes a user from the whitelist. Owner only."""
    global ALLOWED_USERS
    prefix = config.get("prefix", ",")

    try:
        me = await client.get_me()
        if not me or event.sender_id != me.id:
            logger.warning(f"Unauthorized attempt to use '{prefix}delete' by {event.sender_id}")
            try: await event.respond("🚫 Ошибка: Только владелец может удалять.", delete_in=10)
            except Exception: pass
            return
    except telethon.errors.FloodWaitError as e:
         logger.warning(f"Flood wait ({e.seconds}s) verifying owner for '{prefix}delete'. Retrying...")
         raise
    except Exception as e_me:
        logger.error(f"Could not verify bot owner for '{prefix}delete': {e_me}")
        await store_response_message(event.chat_id, await event.reply("❌ Ошибка: Не удалось проверить владельца."))
        return

    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    target_user_info = "Не определен"
    final_sent_message = None

    if use_progress:
        statuses = {"Поиск Пользователя": "⏳ Ожидание...", "Сохранение": "⏸️"}
        progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

    try:
        user_id_to_delete = None
        name_display = None

        if event.is_reply:
            reply_message = await event.get_reply_message()
            if reply_message and reply_message.sender_id:
                user_id_to_delete = reply_message.sender_id
                target_user_info = f"reply to user ID: {user_id_to_delete}"
                name_display = ALLOWED_USERS.get(user_id_to_delete, f"ID: {user_id_to_delete}")
            else:
                raise ValueError("Не удалось получить ID из ответа.")
        elif args:
            user_arg = args[0]
            target_user_info = f"arg '{user_arg}'"
            if use_progress: statuses["Поиск Пользователя"] = f"🔄 Поиск '{user_arg}'..."; await update_progress(progress_message, statuses)

            resolved_entity = None
            search_error_message = None
            potential_id = None

            if user_arg.isdigit():
                try:
                    potential_id = int(user_arg)
                    if potential_id in ALLOWED_USERS:
                        user_id_to_delete = potential_id
                        name_display = ALLOWED_USERS[potential_id]
                        logger.debug(f"Found user by numeric ID in whitelist: {name_display} ({user_id_to_delete})")
                    else:
                         try: resolved_entity = await client.get_entity(potential_id)
                         except Exception: pass
                         if resolved_entity: name_display = f"{resolved_entity.first_name or ''} {resolved_entity.last_name or ''}".strip() or f"@{resolved_entity.username or potential_id}"
                         else: name_display = f"ID {potential_id}"
                         search_error_message = f"{name_display} (`{potential_id}`) не в белом списке."
                except ValueError: search_error_message = "Неверный ID."

            if user_id_to_delete is None and not user_arg.isdigit():
                 try:
                     resolved_entity = await client.get_entity(user_arg)
                     if isinstance(resolved_entity, types.User):
                         potential_id = resolved_entity.id
                         name_display = f"{resolved_entity.first_name or ''} {resolved_entity.last_name or ''}".strip() or f"@{resolved_entity.username or potential_id}"
                         if potential_id in ALLOWED_USERS:
                             user_id_to_delete = potential_id
                             name_display = ALLOWED_USERS[user_id_to_delete]
                             logger.debug(f"Found user by Telethon ({user_arg}) in whitelist: {name_display} ({user_id_to_delete})")
                             search_error_message = None
                         else:
                             search_error_message = f"{name_display} (`{potential_id}`) найден, но не в белом списке."
                     else:
                          search_error_message = f"`{user_arg}` найден, но это не пользователь."
                 except ValueError as e:
                      if "Cannot find any entity" in str(e) or "Could not find the input entity" in str(e):
                           search_error_message = f"`{user_arg}` не найден."
                      else: search_error_message = f"Ошибка поиска: {e}"
                 except telethon.errors.FloodWaitError as e:
                     logger.warning(f"Flood wait ({e.seconds}s) looking up user {user_arg}. Aborting.")
                     raise ValueError(f"Слишком много запросов. Попробуйте через {e.seconds} сек.")
                 except Exception as e_entity:
                     search_error_message = f"Ошибка при поиске `{user_arg}`: {e_entity}"

            if user_id_to_delete is None:
                logger.debug(f"Searching by name substring '{user_arg}' in whitelist.")
                user_arg_lower = user_arg.lower()
                potential_matches = [(uid, name) for uid, name in ALLOWED_USERS.items() if user_arg_lower in name.lower()]

                if len(potential_matches) == 1:
                    user_id_to_delete, name_display = potential_matches[0]
                    logger.info(f"Found user by unique name match: {name_display} ({user_id_to_delete})")
                    search_error_message = None
                elif len(potential_matches) > 1:
                     match_details = [f"'{name}' (`{uid}`)" for uid, name in potential_matches]
                     search_error_message = f"Найдено несколько: {', '.join(match_details[:3])}{'...' if len(match_details)>3 else ''}. Укажите точный ID/@username."

            if user_id_to_delete is None:
                 if search_error_message: raise ValueError(search_error_message)
                 else: raise ValueError(f"Не удалось найти `{user_arg}` для удаления.")
        else:
            raise ValueError(f"Укажите пользователя (ID, @username, имя) или ответьте на сообщение.")

        if name_display is None: name_display = f"ID: {user_id_to_delete}"

        if use_progress: statuses["Поиск Пользователя"] = f"✅ Цель: {name_display} (`{user_id_to_delete}`)"; await update_progress(progress_message, statuses)

        if user_id_to_delete not in ALLOWED_USERS:
            result_text = f"ℹ️ {name_display} (`{user_id_to_delete}`) и так нет в белом списке."
            logger.info(result_text)
            final_sent_message = await (progress_message.edit(result_text) if progress_message else event.reply(result_text))
            await asyncio.sleep(7)
            if progress_message: await progress_message.delete(); progress_message=None
            return

        if use_progress: statuses["Сохранение"] = "🔄 Удаление..."; await update_progress(progress_message, statuses)
        removed_name = ALLOWED_USERS.pop(user_id_to_delete)
        save_users(ALLOWED_USERS)
        if use_progress: statuses["Сохранение"] = "✅ Удалено!"; await update_progress(progress_message, statuses)

        result_text = f"✅ {removed_name} (`{user_id_to_delete}`) удален из белого списка."
        logger.info(result_text)

        if progress_message:
             await progress_message.edit(result_text)
             await asyncio.sleep(7); await progress_message.delete(); progress_message = None
        else:
            try: await event.reply(result_text, delete_in=10)
            except: await event.reply(result_text)

    except Exception as e:
        logger.error(f"Ошибка при удалении ({target_user_info}): {e}", exc_info=False)
        error_prefix = "⚠️" if isinstance(e, ValueError) else "❌"
        error_text = f"{error_prefix} Ошибка: {e}"
        if progress_message:
            statuses["Поиск Пользователя"] = str(statuses.get("Поиск Пользователя", "⏸️")).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌")
            statuses["Сохранение"] = "❌"
            try: await update_progress(progress_message, statuses)
            except: pass
            try:
                await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}")
                final_sent_message = progress_message
            except: final_sent_message = await event.reply(error_text)
        else: final_sent_message = await event.reply(error_text)
    finally:
         if final_sent_message and final_sent_message == progress_message:
              await store_response_message(event.chat_id, final_sent_message)

# -------------------------
# Command: list (Whitelist)
# -------------------------
async def handle_list(event: events.NewMessage.Event, args=None):
    """Lists all users currently in the whitelist."""
    if not ALLOWED_USERS:
        await store_response_message(event.chat_id, await event.reply("ℹ️ Белый список пуст."))
        return

    lines = []
    sorted_users = sorted(ALLOWED_USERS.items(), key=lambda item: str(item[1]).lower())
    for uid, name in sorted_users:
        cleaned_name = name.strip() if name else f"User ID {uid}"
        lines.append(f"• {cleaned_name} - `{uid}`")

    text_header = f"👥 **Пользователи в белом списке ({len(lines)}):**\n\n"
    await send_long_message(event, "\n".join(lines), prefix=text_header)

# -------------------------
# Command: help
# -------------------------
async def handle_help(event: events.NewMessage.Event, args=None):
    """Displays the help message from help.txt."""
    help_path = os.path.join(SCRIPT_DIR, 'help.txt')
    try:
        if not os.path.exists(help_path):
             logger.error(f"Файл справки не найден: {help_path}")
             error_msg = await event.reply(f"❌ Ошибка: Файл справки (`{os.path.basename(help_path)}`) не найден.")
             await store_response_message(event.chat_id, error_msg)
             try:
                 prefix = config.get("prefix", ",")
                 available_commands = sorted(list(set(handlers.keys())))
                 basic_help = f"**Доступные команды:**\n" + \
                              f"`{prefix}" + f"`\n`{prefix}".join(available_commands) + "`"
                 basic_msg = await event.reply(basic_help)
                 await store_response_message(event.chat_id, basic_msg)
             except Exception as basic_e:
                 logger.error(f"Не удалось сгенерировать базовую справку: {basic_e}")
             return

        with open(help_path, "r", encoding="utf-8") as f: help_text = f.read().strip()
        current_prefix = config.get("prefix", ",")
        auth_indicator = "🔑" if ytmusic_authenticated else "⚠️"
        formatted_help = help_text.replace("{prefix}", current_prefix)
        formatted_help = formatted_help.replace("{auth_status_indicator}", auth_indicator)

        response_msg = await event.reply(formatted_help, link_preview=False)
        await store_response_message(event.chat_id, response_msg)
    except Exception as e:
        logger.error(f"Ошибка чтения/форматирования справки: {e}", exc_info=True)
        error_msg = await event.reply("❌ Ошибка при отображении справки.")
        await store_response_message(event.chat_id, error_msg)

# -------------------------
# Command: last
# -------------------------
async def handle_last(event: events.NewMessage.Event, args=None):
    """Displays the list of recently downloaded tracks."""
    if not config.get("recent_downloads", True):
        await store_response_message(event.chat_id, await event.reply("ℹ️ Отслеживание недавних скачиваний отключено."))
        return

    tracks = load_last_tracks()
    if not tracks:
        await store_response_message(event.chat_id, await event.reply("ℹ️ Список недавних треков пуст."))
        return

    lines = ["**⏳ Недавно скачанные треки:**"]
    for i, entry in enumerate(tracks):
        if len(entry) >= 4:
            track_title, creator, browse_id, timestamp = entry[:4]
            display_title = track_title if track_title and track_title != 'Неизвестно' else 'N/A'
            display_creator = creator if creator and creator.lower() not in ['неизвестно', 'unknown artist', 'n/a', ''] else ''

            name_part = f"**{display_title}**"
            if display_creator: name_part += f" - {display_creator}"

            link_part = ""
            if browse_id and browse_id != 'N/A' and isinstance(browse_id, str):
                ytm_link = None
                if browse_id.startswith("UC"): ytm_link = f"https://music.youtube.com/channel/{browse_id}"
                elif browse_id.startswith(("MPRE", "MPLA", "OLAK5uy_")): ytm_link = f"https://music.youtube.com/browse/{browse_id}"
                elif re.fullmatch(r'[A-Za-z0-9_-]{11}', browse_id): ytm_link = f"https://music.youtube.com/watch?v={browse_id}"
                elif browse_id.startswith("PL") or browse_id.startswith("VL"): ytm_link = f"https://music.youtube.com/playlist?list={browse_id}"

                if ytm_link: link_part = f"[Ссылка]({ytm_link})"
                else: link_part = f"`{browse_id}`" # Fallback to showing ID

            ts_part = f"`({timestamp})`" if timestamp else ""
            lines.append(f"{i + 1}. {name_part} {link_part} {ts_part}".strip())
        else:
            logger.warning(f"Skipping malformed entry in last tracks: {entry}")

    if len(lines) == 1:
        await store_response_message(event.chat_id, await event.reply("ℹ️ Не найдено валидных записей."))
    else:
        response_msg = await event.reply("\n".join(lines), link_preview=False)
        await store_response_message(event.chat_id, response_msg)


# -------------------------
# Command: host
# -------------------------
async def handle_host(event: events.NewMessage.Event, args: List[str]):
    """Displays system information."""
    response_msg = await event.reply("`🔄 Собираю информацию...`")
    await store_response_message(event.chat_id, response_msg)

    try:
        system_info = platform.system()
        os_name = system_info
        kernel = platform.release()
        architecture = platform.machine()
        hostname = platform.node()

        try:
            if system_info == 'Linux':
                 os_release = platform.freedesktop_os_release()
                 os_name = os_release.get('PRETTY_NAME', system_info)
            elif system_info == 'Windows': os_name = f"{platform.system()} {platform.release()} ({platform.version()})"
            elif system_info == 'Darwin': os_name = f"macOS {platform.mac_ver()[0]}"
        except Exception as e_os: logger.warning(f"Could not get detailed OS name: {e_os}")

        ram_info, cpu_info, disk_info, uptime_str = "N/A", "N/A", "N/A", "N/A"

        try:
             mem = psutil.virtual_memory()
             ram_info = f"{mem.used / (1024 ** 3):.2f}/{mem.total / (1024 ** 3):.2f} GB ({mem.percent}%)"
        except Exception as e_ram: logger.warning(f"Could not get RAM info: {e_ram}")

        try:
            cpu_count_logical = psutil.cpu_count(logical=True)
            cpu_usage = psutil.cpu_percent(interval=0.5)
            cpu_info = f"{cpu_count_logical} Cores @ {cpu_usage:.1f}%"
        except Exception as e_cpu: logger.warning(f"Could not get CPU info: {e_cpu}")

        try:
            disk = psutil.disk_usage('/')
            disk_info = f"{disk.used / (1024 ** 3):.2f}/{disk.total / (1024 ** 3):.2f} GB ({disk.percent}%)"
        except Exception as e_disk: logger.warning(f"Could not get disk usage ('/'): {e_disk}")

        try:
            boot_time = psutil.boot_time()
            uptime_seconds = datetime.datetime.now().timestamp() - boot_time
            if uptime_seconds > 0:
                td = datetime.timedelta(seconds=int(uptime_seconds))
                days = td.days
                hours, rem = divmod(td.seconds, 3600)
                minutes, seconds = divmod(rem, 60)
                if days > 0: uptime_str = f"{days}d {hours:02}:{minutes:02}:{seconds:02}"
                else: uptime_str = f"{hours:02}:{minutes:02}:{seconds:02}"
            else: uptime_str = "< 1s"
        except Exception as e_uptime: logger.warning(f"Could not get uptime: {e_uptime}")

        ping_result = "N/A"
        ping_target = "1.1.1.1"
        try:
            ping_cmd_path = shutil.which('ping')
            if ping_cmd_path:
                p_args = [ping_cmd_path, '-n', '1', '-w', '2000', ping_target] if system_info == 'Windows' else [ping_cmd_path, '-c', '1', '-W', '2', ping_target]
                proc = await asyncio.create_subprocess_exec(*p_args, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
                rc = await asyncio.wait_for(proc.wait(), timeout=4.0)
                ping_result = f"✅ OK ({ping_target})" if rc == 0 else f"❌ ERR ({ping_target}, rc={rc})"
            else: ping_result = "⚠️ 'ping' n/a"
        except asyncio.TimeoutError: ping_result = f"⌛ Timeout ({ping_target})"
        except Exception as e_ping: logger.warning(f"Ping test failed: {e_ping}"); ping_result = f"❓ Error ({ping_target})"

        auth_file_base = os.path.basename(YT_MUSIC_AUTH_FILE)
        ytm_auth_status = f"✅ Активна (`{auth_file_base}`)" if ytmusic_authenticated else f"⚠ Не активна (нет `{auth_file_base}`)"

        text = (
            f"🖥️ **System Info**\n"
            f" ├ **Host:** `{hostname}`\n"
            f" ├ **OS:** `{os_name}`\n"
            f" ├ **Kernel:** `{kernel}`\n"
            f" ├ **Arch:** `{architecture}`\n"
            f" └ **Uptime:** `{uptime_str}`\n\n"
            f"⚙️ **Hardware**\n"
            f" ├ **CPU:** `{cpu_info}`\n"
            f" ├ **RAM:** `{ram_info}`\n"
            f" └ **Disk (/):** `{disk_info}`\n\n"
            f"🌐 **Network**\n"
            f" └ **Ping:** `{ping_result}`\n\n"
            f"🎵 **YouTube Music**\n"
            f" └ **Авторизация:** {ytm_auth_status}"
        )
        await response_msg.edit(text)

    except Exception as e_host:
        logger.error(f"Ошибка при сборе информации о хосте: {e_host}", exc_info=True)
        await response_msg.edit(f"❌ Не удалось получить инфо:\n`{e_host}`")


# =============================================================================
#                         MAIN EXECUTION & LIFECYCLE
# =============================================================================

async def main():
    """Main asynchronous function to start the bot."""
    logger.info("--- Запуск бота YTMG ---")
    try:
        versions = [f"Python: {platform.python_version()}"]
        try: versions.append(f"Telethon: {telethon.__version__}")
        except: versions.append("Telethon: ?")
        try: versions.append(f"yt-dlp: {yt_dlp.version.__version__}")
        except: versions.append("yt-dlp: ?")
        try: from importlib import metadata; versions.append(f"ytmusicapi: {metadata.version('ytmusicapi')}")
        except: versions.append("ytmusicapi: ?")
        try: versions.append(f"Pillow: {Image.__version__}")
        except: versions.append("Pillow: ?")
        try: versions.append(f"psutil: {psutil.__version__}")
        except: versions.append("psutil: ?")
        logger.info("Версии библиотек: " + " | ".join(versions))

        logger.info("Подключение к Telegram...")
        await client.start()
        me = await client.get_me()
        if me:
            name = f"@{me.username}" if me.username else f"{me.first_name or ''} {me.last_name or ''}".strip() or f"ID: {me.id}"
            logger.info(f"Бот запущен как: {name} (ID: {me.id})")
        else:
            logger.error("Не удалось получить информацию о себе (me).")
            return

        logger.info(f"Конфигурация: Префикс='{config.get('prefix')}', "
                    f"Whitelist={'Вкл' if config.get('whitelist_enabled') else 'Выкл'}, "
                    f"AutoClear={'Вкл' if config.get('auto_clear') else 'Выкл'}, "
                    f"YTMusic Auth={'Активна' if ytmusic_authenticated else 'Неактивна'}")
        pp_info = "N/A"
        if YDL_OPTS.get('postprocessors'):
            first_pp = YDL_OPTS['postprocessors'][0]
            pp_info = first_pp.get('key','?')
            if first_pp.get('key') == 'FFmpegExtractAudio' and first_pp.get('preferredcodec'):
                pp_info += f" ({first_pp.get('preferredcodec')})"
        logger.info(f"yt-dlp: Format='{YDL_OPTS.get('format', 'N/A')}', PP='{pp_info}', EmbedMeta={YDL_OPTS.get('embed_metadata')}, EmbedThumb={YDL_OPTS.get('embed_thumbnail')}")
        logger.info("--- Бот готов к приему команд ---")

        await client.run_until_disconnected()

    except (telethon.errors.AuthKeyError, telethon.errors.AuthKeyUnregisteredError) as e_authkey:
         session_file = os.path.join(SCRIPT_DIR, 'telegram_session.session')
         logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА ({type(e_authkey).__name__}): Невалидная сессия. Удалите '{session_file}' и перезапустите.")
    except Exception as e_main:
        logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА в main: {e_main}", exc_info=True)
    finally:
        logger.info("--- Завершение работы бота ---")
        if client and client.is_connected():
            logger.info("Отключение от Telegram...")
            try:
                await client.disconnect()
                logger.info("Клиент Telegram отключен.")
            except Exception as e_disc:
                 logger.error(f"Ошибка при отключении: {e_disc}")
        logging.shutdown()
        logger.info("--- Бот остановлен ---")

# --- Entry Point ---
if __name__ == '__main__':
    try:
        if not os.path.isdir(SCRIPT_DIR):
             logger.critical(f"CRITICAL: Script directory '{SCRIPT_DIR}' not found. Exiting.")
             exit(1)
        # Need to import html for send_lyrics
        import html
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Получен сигнал завершения (KeyboardInterrupt).")
    except Exception as e_top:
        logger.critical(f"Необработанное исключение: {e_top}", exc_info=True)
    finally:
        print("Процесс завершен.") # Use print as logging might be shut down

# --- END OF FILE main.py ---
