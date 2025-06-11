# -*- coding: utf-8 -*-
# --- START OF FILE main.py ---

# =============================================================================
#                            GPLv3 DISCLAIMER
# =============================================================================
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# Repository: https://github.com/den22den22/YTMG/
print("="*70)
print("YTMG (YouTube Music Grabber)")
print("Copyright (C) 2025 den22den22")
print("This program comes with ABSOLUTELY NO WARRANTY.")
print("This is free software, and you are welcome to redistribute it")
print("under certain conditions; see the GPLv3 license for details:")
print("https://www.gnu.org/licenses/gpl-3.0.html")
print("Repository: https://github.com/den22den22/YTMG/")
print("="*70)
print("\nStarting up...\n")


# =============================================================================
#                            IMPORTS & SETUP
# =============================================================================

import git
import asyncio
import csv
import datetime
import functools
import glob
import html # Import for send_lyrics html escaping
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
from telethon import errors as telethon_errors
from ytmusicapi import YTMusic
import dotenv # Added for pydotenv

# --- Load .env file ---
dotenv.load_dotenv()

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
    logger.critical("CRITICAL ERROR: Telegram API ID/Hash environment variables not set. Ensure they are in your .env file or environment.")
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
    # Add cookies file path
    COOKIES_FILE_PATH = os.path.join(SCRIPT_DIR, 'cookies.txt')
    if os.path.exists(COOKIES_FILE_PATH):
         logger.info(f"Found cookies file: {COOKIES_FILE_PATH}. Adding to yt-dlp options.")
         default_opts['cookiefile'] = COOKIES_FILE_PATH
    else:
         logger.info(f"Cookies file not found: {COOKIES_FILE_PATH}. yt-dlp will run without explicit cookies.")

    absolute_config_path = os.path.join(SCRIPT_DIR, config_file)
    logger.info(f"Attempting to load yt-dlp options from: {absolute_config_path}")
    opts = {}
    try:
        with open(absolute_config_path, 'r', encoding='utf-8') as f:
            opts = json.load(f)
            logger.info(f"Loaded yt-dlp options from {absolute_config_path}")
    except FileNotFoundError:
        logger.warning(f"yt-dlp config file '{absolute_config_path}' not found. Using default options.")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding yt-dlp config file '{absolute_config_path}': {e}. Using default options.")
    except Exception as e:
        logger.error(f"Error loading yt-dlp config '{absolute_config_path}': {e}. Using default options.")

    merged_opts = default_opts.copy()
    merged_opts.update(opts)

    if 'outtmpl' in merged_opts and not os.path.isabs(merged_opts['outtmpl']):
         outtmpl_path = merged_opts['outtmpl']
         if not os.path.splitdrive(outtmpl_path)[0] and not os.path.isabs(outtmpl_path):
             merged_opts['outtmpl'] = os.path.join(SCRIPT_DIR, merged_opts['outtmpl'])
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

YDL_OPTS = load_ydl_opts()

# --- Bot Configuration (UBOT.cfg) ---
DEFAULT_CONFIG = {
    "prefix": ",",
    "progress_messages": True,
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
    absolute_config_path = os.path.join(SCRIPT_DIR, config_file)
    logger.info(f"Attempting to load bot config from: {absolute_config_path}")
    config = DEFAULT_CONFIG.copy()
    try:
        with open(absolute_config_path, 'r', encoding='utf-8') as f:
            loaded_config = json.load(f)
            config.update(loaded_config)
            added_keys = [key for key in DEFAULT_CONFIG if key not in loaded_config]
            if added_keys: logger.warning(f"Added missing default keys to config: {', '.join(added_keys)}")
            if "whitelist_enabled" in config:
                 del config["whitelist_enabled"]
                 logger.warning("Removed 'whitelist_enabled' from active config as it's deprecated.")

            logger.info(f"Loaded configuration from {absolute_config_path}")
    except FileNotFoundError:
        logger.warning(f"Bot config file '{absolute_config_path}' not found. Using default configuration.")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding bot config file '{absolute_config_path}': {e}. Using default configuration.")
    except Exception as e:
        logger.error(f"Error loading bot config '{absolute_config_path}': {e}. Using default options.")

    if "whitelist_enabled" in config: del config["whitelist_enabled"]

    return config

def save_config(config_to_save: Dict, config_file: str = 'UBOT.cfg'):
    absolute_config_path = os.path.join(SCRIPT_DIR, config_file)
    logger.info(f"Attempting to save bot config to: {absolute_config_path}")
    config_copy = config_to_save.copy()
    if "whitelist_enabled" in config_copy: del config_copy["whitelist_enabled"]

    try:
        with open(absolute_config_path, 'w', encoding='utf-8') as f:
            json.dump(config_copy, f, indent=4, ensure_ascii=False)
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

# --- Bot Owner ID (Set in main() after client.start) ---
BOT_OWNER_ID: Optional[int] = None


async def initialize_ytmusic_client():
    """Initializes or re-initializes the YTMusic API client."""
    global ytmusic, ytmusic_authenticated
    auth_file_base = os.path.basename(YT_MUSIC_AUTH_FILE)
    current_loop = asyncio.get_running_loop()

    try:
        if os.path.exists(YT_MUSIC_AUTH_FILE):
            logger.info(f"Found YTMusic auth file: '{auth_file_base}'. Attempting to initialize with it.")
            temp_ytmusic = await current_loop.run_in_executor(None, YTMusic, YT_MUSIC_AUTH_FILE)
            logger.debug("Checking YTMusic authentication status by fetching history...")
            try:
                await current_loop.run_in_executor(None, temp_ytmusic.get_history)
                ytmusic = temp_ytmusic
                ytmusic_authenticated = True
                logger.info("YTMusic authentication successful with file.")
            except Exception as e_auth_check:
                logger.warning(f"YTMusic authentication with '{auth_file_base}' failed or cookies may be expired: {type(e_auth_check).__name__} - {e_auth_check}. Falling back to unauthenticated mode.")
                # Fallback to unauthenticated if auth check fails
                ytmusic = await current_loop.run_in_executor(None, YTMusic)
                ytmusic_authenticated = False
                logger.info("YTMusic API initialized in unauthenticated mode after auth file check failed.")
        else:
            logger.warning(f"YTMusic auth file '{auth_file_base}' not found. Initializing in unauthenticated mode.")
            ytmusic = await current_loop.run_in_executor(None, YTMusic)
            ytmusic_authenticated = False
            logger.info("YTMusic API initialized in unauthenticated mode.")

    except Exception as e_ytm_init:
        logger.error(f"Critical error during YTMusic API initialization: {e_ytm_init}", exc_info=True)
        # Attempt a final fallback to unauthenticated if primary init (even with file) fails badly
        try:
            logger.warning("Attempting final fallback to unauthenticated YTMusic initialization due to earlier critical error.")
            ytmusic = await current_loop.run_in_executor(None, YTMusic)
            ytmusic_authenticated = False
            logger.info("YTMusic API initialized in unauthenticated mode as a final fallback.")
        except Exception as e_final_fallback:
            logger.critical(f"Final fallback to unauthenticated YTMusic API also failed: {e_final_fallback}", exc_info=True)
            ytmusic = None
            ytmusic_authenticated = False


# --- Helper Function for Auth Check Decorator ---
def require_ytmusic_auth(func):
    """Decorator for command handlers that require authenticated YTMusic."""
    @functools.wraps(func)
    async def wrapper(event: events.NewMessage.Event, args: List[str]):
        if not ytmusic: # Check if ytmusic object exists at all
            # Attempt to re-initialize if it's None (e.g., failed critical init)
            logger.warning("YTMusic client is None. Attempting re-initialization before auth-required command.")
            await initialize_ytmusic_client() # Try to re-init
            if not ytmusic: # If still None after re-init attempt
                await event.reply("❌ Ошибка: Клиент YTMusic не смог инициализироваться.")
                logger.error("Attempted authenticated command, but YTMusic client is still None after re-init attempt.")
                return

        if not ytmusic_authenticated:
            auth_file_basename = os.path.basename(YT_MUSIC_AUTH_FILE)
            await event.reply(f"⚠️ Для этой команды требуется авторизация. Файл `{auth_file_basename}` не найден или недействителен.")
            logger.warning(f"Authenticated command '{func.__name__}' requires '{auth_file_basename}', which is missing or invalid.")
            return
        return await func(event, args)
    return wrapper


# =============================================================================
#                            DATA MANAGEMENT (Last Tracks)
# =============================================================================

LAST_TRACKS_FILE = os.path.join(SCRIPT_DIR, 'last.csv')
HELP_FILE = os.path.join(SCRIPT_DIR, 'help.txt')

# New header: Track Title,Artists,Video ID,Track URL,Duration Seconds,Timestamp
# Old header: track,creator,browseid,tt:tt-dd-mm
EXPECTED_LAST_TRACKS_COLUMNS = 6

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
            # A loose check for the new header structure
            expected_header_parts = ['title', 'artist', 'video', 'url', 'duration', 'timestamp']
            if header:
                header_str_lower = ''.join(header).lower().replace(' ', '').replace('-', '').replace('(', '').replace(')', '')
                if not all(part in header_str_lower for part in expected_header_parts):
                    logger.warning(f"Unexpected header in {LAST_TRACKS_FILE}: {header}. Expected something like 'Track Title;Artists;Video ID;Track URL;Duration Seconds;Timestamp'.")
            else: # No header means it's an old file or empty
                logger.warning(f"{LAST_TRACKS_FILE} is empty or has no header. Assuming old format or empty.")


            tracks = [row for row in reader if len(row) >= EXPECTED_LAST_TRACKS_COLUMNS]
            try:
                with open(LAST_TRACKS_FILE, 'r', encoding='utf-8', newline='') as f_count:
                    original_row_count = sum(1 for row in csv.reader(f_count, delimiter=';') if row) - (1 if header else 0)
                if len(tracks) < original_row_count:
                    logger.warning(f"Skipped {original_row_count - len(tracks)} malformed rows (less than {EXPECTED_LAST_TRACKS_COLUMNS} columns) in {LAST_TRACKS_FILE}.")
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
        tracks_to_save = tracks[:5] # Keep only top 5
        with open(LAST_TRACKS_FILE, 'w', encoding='utf-8', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            writer.writerow(['Track Title', 'Artists', 'Video ID', 'Track URL', 'Duration Seconds', 'Timestamp'])
            writer.writerows(tracks_to_save)
        logger.info(f"Saved {len(tracks_to_save)} last tracks to {LAST_TRACKS_FILE}")
    except Exception as e:
        logger.error(f"Error saving last tracks to {LAST_TRACKS_FILE}: {e}")

# =============================================================================
#                            CORE UTILITIES (with enhanced retry)
# =============================================================================

def retry(max_tries: int = 3, delay: float = 2.0, exceptions: Optional[Tuple[Type[Exception], ...]] = None, empty_result_check: Optional[str] = None):
    """Decorator to retry an async function upon encountering specific exceptions or empty results."""
    actual_exceptions = list(exceptions) if exceptions else []
    if requests.exceptions.RequestException not in actual_exceptions:
        actual_exceptions.append(requests.exceptions.RequestException)
    # Add common Telethon network errors
    if telethon_errors.rpcerrorlist.TimeoutError not in actual_exceptions: # Changed from TimeoutError to rpcerrorlist.TimeoutError
        actual_exceptions.append(telethon_errors.rpcerrorlist.TimeoutError)
    if telethon_errors.ApiIdInvalidError not in actual_exceptions: # To catch more specific network/connection issues
         actual_exceptions.append(telethon_errors.ApiIdInvalidError) # Placeholder for broader connection errors, check Telethon docs for better ones

    # Add generic ytmusicapi exception if it exists, or rely on requests.exceptions.HTTPError for 401, etc.
    # For now, relying on requests exceptions caught by the decorator.

    if Exception not in actual_exceptions: # Catch-all for other unexpected issues during the attempt
        actual_exceptions.append(Exception)

    actual_exceptions_tuple = tuple(actual_exceptions)

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            attempt = 0
            global ytmusic, ytmusic_authenticated # Allow modification for auth re-check

            while attempt < max_tries:
                try:
                    result = await func(*args, **kwargs)

                    is_empty_result = False
                    if empty_result_check == "None" and result is None: is_empty_result = True
                    elif empty_result_check == "[]" and result == []: is_empty_result = True
                    elif empty_result_check == "{}" and result == {}: is_empty_result = True

                    if is_empty_result:
                        if attempt == max_tries - 1:
                            logger.warning(f"'{func.__name__}' returned empty result ('{empty_result_check}') after {max_tries} attempts. Returning empty result.")
                            return result
                        else:
                            wait_time = delay * (2 ** attempt)
                            logger.warning(f"Attempt {attempt + 1}/{max_tries} of '{func.__name__}' returned empty result. Retrying in {wait_time:.2f}s...")
                            await asyncio.sleep(wait_time)
                            attempt += 1
                            continue
                    else:
                        return result

                except actual_exceptions_tuple as e:
                    last_exception = e
                    logger.warning(f"Attempt {attempt + 1}/{max_tries} for '{func.__name__}' failed: {type(e).__name__} - {e}")

                    # Specific handling for potential auth loss with YTMusic
                    # This is a basic check. A more robust solution would involve specific exception types from ytmusicapi if they exist for auth.
                    is_http_auth_error = isinstance(e, requests.exceptions.HTTPError) and e.response is not None and e.response.status_code in [401, 403]
                    # Add other conditions if ytmusicapi throws specific auth exceptions
                    # is_ytm_auth_error = isinstance(e, some_ytmusicapi_auth_exception_type)

                    if is_http_auth_error and ytmusic_authenticated:
                        logger.warning(f"Possible YTMusic authentication loss during '{func.__name__}' (HTTP {e.response.status_code}). Attempting to re-initialize YTMusic client.")
                        ytmusic_authenticated = False # Mark as de-authenticated
                        await initialize_ytmusic_client() # Attempt re-initialization
                        if not ytmusic_authenticated:
                            logger.error(f"Re-authentication failed after error in '{func.__name__}'. Further authenticated calls might fail.")
                            # If re-auth fails, we should probably not retry this specific call again with auth,
                            # or let it fail if the next attempt still uses the (now unauthenticated) client.
                            # For simplicity, we let the retry loop continue. If auth is truly lost, subsequent retries will also fail.

                    if attempt == max_tries - 1:
                        logger.error(f"'{func.__name__}' failed after {max_tries} attempts. Last error: {e}", exc_info=True if not is_http_auth_error else False)
                        raise # Re-raise the last exception
                    else:
                        wait_time = delay * (2 ** attempt)
                        logger.warning(f"Retrying '{func.__name__}' in {wait_time:.2f}s...")
                        await asyncio.sleep(wait_time)
                        attempt += 1
            # This part should ideally not be reached if max_tries > 0, as the loop either returns or raises.
            # However, if loop finishes due to max_tries and last_exception is set:
            if last_exception:
                raise last_exception
            return None # Should not happen if an exception was always raised on final failure.
        return wrapper
    return decorator


def extract_entity_id(link_or_id: str) -> Optional[str]:
    """
    Extracts YouTube Music video ID, playlist ID, album/artist browse ID from a URL or returns the input if it looks like an ID.
    Handles standard YouTube video IDs as well.
    """
    if not isinstance(link_or_id, str): return None
    link_or_id = link_or_id.strip()

    # Direct ID patterns
    if re.fullmatch(r'[A-Za-z0-9_-]{11}', link_or_id): # Standard YouTube video ID
        return link_or_id
    # YTMusic specific IDs (often longer or prefixed)
    if link_or_id.startswith(('PL', 'VL', 'OLAK5uy_')): return link_or_id # Playlist IDs (VL can be for auto-generated "album" playlists)
    if link_or_id.startswith(('MPRE', 'MPLA', 'RDAM')): return link_or_id # Album/release IDs
    if link_or_id.startswith('UC'): return link_or_id # Channel/Artist IDs

    # URL patterns
    id_patterns = [
        r"(?:youtube\.com/watch\?v=|youtu\.be/)([A-Za-z0-9_-]{11})", # Standard YouTube video
        r"(?:music\.youtube\.com/watch\?v=)([A-Za-z0-9_-]{11})",    # YTMusic video
        r"(?:music\.youtube\.com/playlist\?list=|youtube\.com/playlist\?list=)([A-Za-z0-9_-]+)", # YTMusic/YouTube Playlist
        r"(?:music\.youtube\.com/browse/|youtube\.com/channel/)([A-Za-z0-9_-]+)", # YTMusic Album/Artist browse, YouTube Channel
    ]
    for pattern in id_patterns:
        match = re.search(pattern, link_or_id)
        if match:
            extracted_id = match.group(1)
            logger.debug(f"Extracted ID '{extracted_id}' using pattern '{pattern}' from link: {link_or_id}")
            return extracted_id

    logger.warning(f"Could not extract a valid ID from input: {link_or_id}")
    return None

def format_artists(data: Optional[Union[List[Dict], Dict, str]]) -> str:
    """Formats artist names from various ytmusicapi structures."""
    names = []
    if isinstance(data, list):
        names = [a.get('name', '').strip() for a in data if isinstance(a, dict) and a.get('name')]
    elif isinstance(data, dict):
        name = data.get('name', data.get('artist', '')).strip()
        if name: names.append(name)
    elif isinstance(data, str):
        names.append(data.strip())
    cleaned_names = [re.sub(r'\s*-\s*Topic$', '', name).strip() for name in names if name]
    return ', '.join(filter(None, cleaned_names)) or 'Неизвестно'

# =============================================================================
#                       YOUTUBE MUSIC API INTERACTION (with wrappers)
# =============================================================================

@retry(max_tries=3, delay=2.0, empty_result_check='[]')
async def _api_search(query: str, filter_type: Optional[str], limit: int) -> List[Dict]:
     if not ytmusic: raise RuntimeError("YTMusic API client not initialized")
     logger.debug(f"Calling ytmusic.search(query='{query[:50]}...', filter='{filter_type}', limit={limit})")
     return await asyncio.to_thread(ytmusic.search, query, filter=filter_type, limit=limit, ignore_spelling=True) # Added ignore_spelling

@retry(max_tries=3, delay=2.0, empty_result_check='None')
async def _api_get_watch_playlist(video_id: str, **kwargs) -> Optional[Dict]:
     if not ytmusic: raise RuntimeError("YTMusic API client not initialized")
     logger.debug(f"Calling ytmusic.get_watch_playlist(videoId='{video_id}', radio={kwargs.get('radio', False)}, limit={kwargs.get('limit', 1)})")
     return await asyncio.to_thread(ytmusic.get_watch_playlist, videoId=video_id, **kwargs)

@retry(max_tries=3, delay=2.0, empty_result_check='None')
async def _api_get_song(video_id: str) -> Optional[Dict]:
     if not ytmusic: raise RuntimeError("YTMusic API client not initialized")
     logger.debug(f"Calling ytmusic.get_song(videoId='{video_id}')")
     return await asyncio.to_thread(ytmusic.get_song, videoId=video_id)

@retry(max_tries=3, delay=2.0, empty_result_check='None')
async def _api_get_album(browse_id: str) -> Optional[Dict]:
     if not ytmusic: raise RuntimeError("YTMusic API client not initialized")
     logger.debug(f"Calling ytmusic.get_album(browseId='{browse_id}')")
     return await asyncio.to_thread(ytmusic.get_album, browseId=browse_id)

@retry(max_tries=3, delay=2.0, empty_result_check='None')
async def _api_get_playlist(playlist_id: str, limit: Optional[int] = None) -> Optional[Dict]:
     if not ytmusic: raise RuntimeError("YTMusic API client not initialized")
     logger.debug(f"Calling ytmusic.get_playlist(playlistId='{playlist_id}', limit={limit})")
     return await asyncio.to_thread(ytmusic.get_playlist, playlistId=playlist_id, limit=limit)

@retry(max_tries=3, delay=2.0, empty_result_check='None')
async def _api_get_artist(channel_id: str) -> Optional[Dict]:
     if not ytmusic: raise RuntimeError("YTMusic API client not initialized")
     logger.debug(f"Calling ytmusic.get_artist(channelId='{channel_id}')")
     return await asyncio.to_thread(ytmusic.get_artist, channelId=channel_id)

@retry(max_tries=3, delay=2.0, empty_result_check='None')
async def get_entity_info(entity_id: str, entity_type_hint: Optional[str] = None) -> Optional[Dict]:
    """
    Fetches metadata for a YouTube Music entity (track, album, playlist, artist) using wrappers.
    """
    if not ytmusic:
        logger.error("YTMusic API client not initialized. Cannot fetch entity info.")
        # Try to re-initialize if ytmusic is None (should not happen if main() ran correctly)
        await initialize_ytmusic_client()
        if not ytmusic:
            logger.error("Re-initialization of YTMusic client failed. Entity info fetch aborted.")
            return None


    logger.debug(f"Fetching entity info for ID: {entity_id}, Hint: {entity_type_hint}")
    try:
        inferred_type = None
        if isinstance(entity_id, str):
            if re.fullmatch(r'[A-Za-z0-9_-]{11}', entity_id): inferred_type = "track"
            elif entity_id.startswith(('PL', 'VL')): inferred_type = "playlist" # VL prefix used for some auto-generated "album" like playlists
            elif entity_id.startswith('OLAK5uy_'): inferred_type = "album" # Often used for albums by YTMusic
            elif entity_id.startswith(('MPRE', 'MPLA', 'RDAM')): inferred_type = "album"
            elif entity_id.startswith('UC'): inferred_type = "artist"
        else:
            logger.warning(f"Invalid entity_id type provided: {type(entity_id)}.")
            return None

        current_hint = entity_type_hint or inferred_type
        logger.debug(f"Effective hint/inferred type for API call: {current_hint}")

        api_calls_by_type = {
            "playlist": _api_get_playlist,
            "album": _api_get_album,
            "artist": _api_get_artist,
            "track": _api_get_song,
        }

        call_func = None
        if current_hint and current_hint in api_calls_by_type:
             call_func = api_calls_by_type[current_hint]
             logger.debug(f"Trying API call for hinted/inferred type: {current_hint}")
             try:
                 # Pass browseId for album/artist/playlist, videoId for track
                 api_arg = entity_id
                 if current_hint == "playlist":
                     api_arg = entity_id # YTMusic takes full playlist ID
                 elif current_hint == "album":
                     api_arg = entity_id # YTMusic takes full album browse ID
                 elif current_hint == "artist":
                     api_arg = entity_id # YTMusic takes full artist channel ID

                 info = await call_func(api_arg)

                 if info:
                     if current_hint == "track":
                         # Ensure 'get_song' result is standardized if 'videoDetails' is missing but other fields are present
                         if 'videoDetails' not in info and 'title' in info and 'videoId' in info: # Basic check
                             logger.debug(f"get_song for {entity_id} might be missing 'videoDetails', attempting to build structure.")
                             temp_info = {'videoDetails': info.copy()} # Copy all to videoDetails
                             # Ensure common fields are at top level as well if possible, or rely on videoDetails
                             if 'thumbnails' not in temp_info and 'thumbnail' in info:
                                 temp_info['thumbnails'] = (info.get('thumbnail') or {}).get('thumbnails')
                             if 'lyrics' not in temp_info and 'lyrics' in info: # from get_song directly
                                 temp_info['lyrics'] = info['lyrics']
                             info = temp_info # Replace info with the structured one

                         if info.get('videoDetails'):
                             processed_info = info['videoDetails']
                             # Ensure top-level fields (like thumbnails, artists, lyrics from get_song) are merged if not in videoDetails
                             if 'thumbnails' not in processed_info and 'thumbnail' in info:
                                 processed_info['thumbnails'] = (info.get('thumbnail') or {}).get('thumbnails')
                             if 'artists' not in processed_info and 'artists' in info: # artists from root of get_song
                                 processed_info['artists'] = info['artists']
                             if 'lyrics' not in processed_info and 'lyrics' in info: # lyrics from root of get_song
                                 processed_info['lyricsBrowseId'] = info['lyrics'] # Store browseId
                                 # We might need to fetch lyrics content separately if only ID is here.
                                 # For get_entity_info, we mostly care about metadata, not full content.
                             info = processed_info # This is the main dictionary for track details
                         else:
                              logger.warning(f"API call for {current_hint} '{entity_id}' lacked 'videoDetails'. Structure may be inconsistent.")
                              info['_incomplete_structure'] = True


                     info['_entity_type'] = current_hint
                     logger.info(f"Successfully fetched entity info using hint/inferred type '{current_hint}' for {entity_id}")
                     return info
                 else:
                     logger.warning(f"API call for hint '{current_hint}' returned no data for {entity_id}.")
             except Exception as e_hint:
                  logger.warning(f"API call for hint/inferred type '{current_hint}' failed for {entity_id}: {e_hint}. Trying generic checks.")


        # Order of generic checks: track, playlist, album, artist (common to specific)
        generic_check_order_funcs = [
             ("track", _api_get_song),
             ("playlist", _api_get_playlist),
             ("album", _api_get_album),
             ("artist", _api_get_artist),
        ]

        for type_name, api_func in generic_check_order_funcs:
            # Skip if this was already tried via hint
            if current_hint and current_hint == type_name: continue

            # Basic sanity checks for ID format against type
            if type_name == "track" and not re.fullmatch(r'[A-Za-z0-9_-]{11}', entity_id): continue
            if type_name == "album" and not entity_id.startswith(('MPRE', 'MPLA', 'RDAM', 'OLAK5uy_')): continue # OLAK also for albums
            if type_name == "artist" and not entity_id.startswith('UC'): continue
            if type_name == "playlist" and not entity_id.startswith(('PL', 'VL')): continue # VL also for playlists

            try:
                logger.debug(f"Trying generic API call for type '{type_name}' for {entity_id}")
                api_arg_generic = entity_id # Default to passing the ID directly
                # Adjust arg if needed, similar to hinted calls
                result = await api_func(api_arg_generic)

                if result:
                    final_info = result
                    if type_name == "track":
                        if 'videoDetails' not in result and 'title' in result and 'videoId' in result:
                             temp_res = {'videoDetails': result.copy()}
                             if 'thumbnails' not in temp_res and 'thumbnail' in result: temp_res['thumbnails'] = (result.get('thumbnail') or {}).get('thumbnails')
                             if 'lyrics' not in temp_res and 'lyrics' in result: temp_res['lyricsBrowseId'] = result['lyrics']
                             final_info = temp_res

                        if final_info.get('videoDetails'):
                            processed_info_generic = final_info['videoDetails']
                            if 'thumbnails' not in processed_info_generic and 'thumbnail' in final_info: processed_info_generic['thumbnails'] = (final_info.get('thumbnail') or {}).get('thumbnails')
                            if 'artists' not in processed_info_generic and 'artists' in final_info: processed_info_generic['artists'] = final_info['artists']
                            if 'lyrics' not in processed_info_generic and 'lyrics' in final_info: processed_info_generic['lyricsBrowseId'] = final_info['lyrics']
                            final_info = processed_info_generic
                        else:
                             logger.warning(f"Generic check {type_name} for {entity_id} lacked 'videoDetails'. Structure may be inconsistent.")
                             final_info['_incomplete_structure'] = True


                    final_info['_entity_type'] = type_name
                    logger.info(f"Successfully fetched entity info as '{type_name}' for {entity_id} using generic check.")
                    return final_info
            except Exception as e_generic_check:
                 logger.debug(f"Generic check for type '{type_name}' for {entity_id} failed: {e_generic_check}")
                 pass # Silently try next


        # Final fallback for track-like IDs using get_watch_playlist if get_song failed
        if (inferred_type == "track" or re.fullmatch(r'[A-Za-z0-9_-]{11}', entity_id)) and (not entity_type_hint or entity_type_hint == "track"):
             logger.debug(f"Final fallback: Trying get_watch_playlist for potential track ID {entity_id}")
             try:
                 watch_info = await _api_get_watch_playlist(entity_id, limit=1) # Get info for the video itself
                 if watch_info and watch_info.get('tracks') and len(watch_info['tracks']) > 0:
                      # The first track in get_watch_playlist(videoId=X) is usually X itself.
                      track_data = watch_info['tracks'][0]
                      # Standardize to look like get_song's videoDetails structure
                      standardized_info = {
                          '_entity_type': 'track',
                          'videoId': track_data.get('videoId'),
                          'title': track_data.get('title'),
                          'artists': track_data.get('artists'), # list of dicts
                          'album': track_data.get('album'), # dict or None
                          'duration': track_data.get('length'), # "M:SS" string
                          'lengthSeconds': track_data.get('lengthSeconds'), # integer
                          'thumbnails': track_data.get('thumbnail'), # list of dicts
                          'year': track_data.get('year'), # string or None
                          'lyricsBrowseId': watch_info.get('lyrics'), # Lyrics browse ID for the *main* video
                          # Reconstruct a basic videoDetails-like structure
                          # This is a bit redundant but helps standardize
                          'author': format_artists(track_data.get('artists')), # For compatibility
                          'channelId': (track_data.get('artists')[0].get('id') if track_data.get('artists') and track_data.get('artists')[0] else None),
                          'viewCount': track_data.get('views'),
                          # Ensure videoDetails compatibility
                          'videoDetails': { # Add this for consistency with how get_song structures it
                                'videoId': track_data.get('videoId'),
                                'title': track_data.get('title'),
                                'lengthSeconds': track_data.get('lengthSeconds'),
                                'thumbnails': track_data.get('thumbnail'),
                                'author': format_artists(track_data.get('artists')),
                                'channelId': (track_data.get('artists')[0].get('id') if track_data.get('artists') and track_data.get('artists')[0] else None),
                                'lyricsBrowseId': watch_info.get('lyrics'),
                                'viewCount': track_data.get('views')
                          }
                      }
                      if track_data.get('videoId') == entity_id: # Ensure it's the correct track
                         logger.info(f"Successfully fetched track info (fallback) for {entity_id} using get_watch_playlist")
                         return standardized_info
                      else:
                         logger.warning(f"get_watch_playlist for {entity_id} returned a different track {track_data.get('videoId')}. Discarding.")
                 else:
                      logger.debug(f"Final fallback get_watch_playlist for {entity_id} didn't return expected track data structure.")
             except Exception as e_final_watch:
                  logger.warning(f"Final fallback get_watch_playlist failed for {entity_id}: {e_final_watch}")


        logger.error(f"Could not retrieve info for entity ID: {entity_id} using any method.")
        return None

    except Exception as e_outer:
        logger.error(f"Unexpected error in get_entity_info processing for {entity_id}: {e_outer}", exc_info=True)
        # Do not re-raise here, let the caller handle None return.
        return None


@retry(max_tries=3, delay=2.0, empty_result_check='None')
async def _api_get_watch_playlist_for_lyrics(video_id: str) -> Optional[Dict]:
    """Wrapper for get_watch_playlist specifically for finding lyrics browse ID."""
    if not ytmusic: raise RuntimeError("YTMusic API client not initialized")
    logger.debug(f"Calling ytmusic.get_watch_playlist(videoId='{video_id}', limit=1) for lyrics lookup")
    return await asyncio.to_thread(ytmusic.get_watch_playlist, videoId=video_id, limit=1)

@retry(max_tries=3, delay=2.0, empty_result_check='None')
async def _api_get_lyrics_content(browse_id: str) -> Optional[Dict[str, str]]:
    """Wrapper for get_lyrics to fetch the lyrics content."""
    if not ytmusic: raise RuntimeError("YTMusic API client not initialized")
    logger.debug(f"Calling ytmusic.get_lyrics(browseId='{browse_id}')")
    return await asyncio.to_thread(ytmusic.get_lyrics, browseId=browse_id)


async def get_lyrics_for_track(video_id: Optional[str], lyrics_browse_id: Optional[str] = None) -> Optional[Dict[str, str]]:
    """
    Fetches lyrics for a track using its video ID or lyrics browse ID, using wrapped API calls.
    The lyrics_browse_id can come from get_song or get_entity_info.
    """
    if not ytmusic:
        logger.error("YTMusic API client not initialized. Cannot fetch lyrics.")
        await initialize_ytmusic_client()
        if not ytmusic:
             logger.error("Re-initialization of YTMusic client failed. Lyrics fetch aborted.")
             return None

    if not video_id and not lyrics_browse_id:
        logger.error("Cannot fetch lyrics without either video ID or lyrics browse ID.")
        return None

    final_lyrics_browse_id = lyrics_browse_id
    track_id_for_log = video_id or lyrics_browse_id # Use video_id for logging if available, else browse_id

    try:
        # If we don't have a lyrics_browse_id, try to get it from get_watch_playlist
        if not final_lyrics_browse_id and video_id:
             logger.debug(f"No explicit lyrics browse ID. Attempting to find via watch playlist for video: {video_id}")
             try:
                 watch_info = await _api_get_watch_playlist_for_lyrics(video_id)
                 # 'lyrics' key in get_watch_playlist result is the browseId for lyrics
                 final_lyrics_browse_id = watch_info.get('lyrics') if watch_info else None

                 if not final_lyrics_browse_id:
                      logger.info(f"No lyrics browse ID found in watch playlist info for {video_id}.")
                      # It's possible the track has no lyrics, so this is not necessarily an error yet.
                 else:
                     logger.debug(f"Found lyrics browse ID via watch_playlist: {final_lyrics_browse_id} for video {video_id}")
             except Exception as e_watch_lookup:
                  logger.warning(f"Failed to get watch playlist info for lyrics browse ID lookup ({video_id}) after retries: {e_watch_lookup}")
                  # Proceed without it, get_lyrics might fail or return None.

        # If we have a lyrics_browse_id (either passed in or found), fetch the lyrics
        if final_lyrics_browse_id:
             logger.info(f"Fetching lyrics content using browse ID: {final_lyrics_browse_id} (for track: {track_id_for_log})")
             try:
                 lyrics_data = await _api_get_lyrics_content(final_lyrics_browse_id)
                 if lyrics_data and (lyrics_data.get('lyrics') or lyrics_data.get('description')): # Sometimes lyrics are in description
                     logger.info(f"Successfully fetched lyrics content for {track_id_for_log}")
                     # Prefer 'lyrics', fallback to 'description' if 'lyrics' is empty but 'description' has content
                     if not lyrics_data.get('lyrics') and lyrics_data.get('description'):
                         lyrics_data['lyrics'] = lyrics_data['description']
                         logger.info(f"Used 'description' field as lyrics for {track_id_for_log}")
                     return lyrics_data
                 else:
                      logger.info(f"API call for lyrics content succeeded but returned no lyrics for browse ID {final_lyrics_browse_id} (track: {track_id_for_log})")
                      return None # No lyrics found
             except Exception as e_lyrics_fetch:
                  logger.error(f"Failed to fetch lyrics content using browse ID {final_lyrics_browse_id} (track: {track_id_for_log}) after retries: {e_lyrics_fetch}")
                  return None # Error fetching lyrics
        else:
             # This means no lyrics_browse_id was provided AND it couldn't be found via get_watch_playlist.
             logger.info(f"Could not determine/find lyrics browse ID for {track_id_for_log}. No lyrics available through this method.")
             return None

    except Exception as e_outer:
        logger.error(f"Unexpected error in get_lyrics_for_track processing for {track_id_for_log}: {e_outer}", exc_info=True)
        return None


@retry(max_tries=3, delay=2.0, empty_result_check='[]')
async def _api_get_history():
    if not ytmusic: raise RuntimeError("YTMusic API client not initialized")
    if not ytmusic_authenticated: raise RuntimeError("YTMusic client not authenticated for get_history")
    logger.debug("Calling ytmusic.get_history()")
    return await asyncio.to_thread(ytmusic.get_history)

@retry(max_tries=3, delay=2.0, empty_result_check='None') # Liked songs can return a dict with 'tracks' or None
async def _api_get_liked_songs(limit):
    if not ytmusic: raise RuntimeError("YTMusic API client not initialized")
    if not ytmusic_authenticated: raise RuntimeError("YTMusic client not authenticated for get_liked_songs")
    logger.debug(f"Calling ytmusic.get_liked_songs(limit={limit})")
    return await asyncio.to_thread(ytmusic.get_liked_songs, limit=limit)

@retry(max_tries=3, delay=2.0, empty_result_check='[]')
async def _api_get_home(limit):
    if not ytmusic: raise RuntimeError("YTMusic API client not initialized")
    # get_home does not strictly require auth but works better with it
    logger.debug(f"Calling ytmusic.get_home(limit={limit})")
    return await asyncio.to_thread(ytmusic.get_home, limit=limit)


# =============================================================================
#                       DOWNLOAD & PROCESSING FUNCTIONS
# =============================================================================

def extract_track_metadata(info: Dict) -> Tuple[str, str, int]:
    """
    Extracts Title, Performer, and Duration from yt-dlp's info dictionary.
    """
    title = info.get('track') or info.get('title') or 'Неизвестно'
    performer = 'Неизвестно'
    if info.get('artist'): # Usually from --add-metadata
        performer = info['artist']
    elif info.get('artists') and isinstance(info['artists'], list): # From ytmusicapi structures if merged
         artist_names = [a['name'] for a in info['artists'] if isinstance(a, dict) and a.get('name')]
         if artist_names: performer = ', '.join(artist_names)
    elif info.get('creator'): # Fallback from yt-dlp
         performer = info['creator']
    elif info.get('uploader'): # Fallback from yt-dlp
         performer = re.sub(r'\s*-\s*Topic$', '', info['uploader']).strip()

    # If performer is still default and 'channel' exists (often for - Topic channels)
    if performer in [None, "", "Неизвестно"] and info.get('channel'):
         performer = re.sub(r'\s*-\s*Topic$', '', info['channel']).strip()

    if performer in [None, "", "Неизвестно"]: # Final default
        performer = 'Неизвестно'

    # Clean " - Topic" suffix again, just in case
    performer = re.sub(r'\s*-\s*Topic$', '', performer).strip()


    duration = 0
    try:
        # yt-dlp info['duration'] is usually float in seconds
        duration_val = info.get('duration') or info.get('lengthSeconds') # lengthSeconds from YTMusic API
        if duration_val:
            duration = int(float(duration_val))
    except (ValueError, TypeError):
         logger.warning(f"Could not parse duration '{info.get('duration')}' for track '{title}'. Defaulting to 0.")
         duration = 0

    logger.debug(f"Extracted metadata - Title: '{title}', Performer: '{performer}', Duration: {duration}s")
    return title, performer, duration


def download_track(track_link: str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Downloads a single track using yt-dlp with configured options.
    This is a synchronous function designed to be run in an executor.
    """
    logger.info(f"Attempting download and processing via yt-dlp: {track_link}")
    try:
        current_ydl_opts = YDL_OPTS.copy()

        # Ensure noplaylist is True for single track downloads to prevent numbered prefixes
        # if the link accidentally points to a playlist with one video.
        current_ydl_opts['noplaylist'] = True
        tmpl = current_ydl_opts.get('outtmpl', '%(title)s.%(ext)s')
        # Remove playlist index from template for single track downloads
        tmpl = re.sub(r'[\[\(]?%?\(playlist_index\)[0-9]*[ds]?[-_\. ]?[\]\)]?', '', tmpl).strip()
        current_ydl_opts['outtmpl'] = tmpl if tmpl else '%(title)s.%(ext)s'


        with yt_dlp.YoutubeDL(current_ydl_opts) as ydl:
            # Download=True will trigger postprocessors
            info = ydl.extract_info(track_link, download=True)

            if not info:
                logger.error(f"yt-dlp extract_info returned empty/None for {track_link}")
                return None, None

            # Determine the final file path after post-processing
            final_filepath = None
            # 'requested_downloads' might contain info about the file *after* postprocessing
            if info.get('requested_downloads') and isinstance(info['requested_downloads'], list):
                 # The last entry in requested_downloads for an audio format is usually the final one
                 final_download_info = next((d for d in reversed(info['requested_downloads'])
                                             if d.get('filepath') and os.path.exists(d['filepath']) and d.get('ext') in ['m4a', 'mp3', 'opus', 'ogg', 'flac', 'aac', 'wav']), None) # Added common audio exts
                 if final_download_info:
                      final_filepath = final_download_info.get('filepath')
                      logger.debug(f"Found final path in 'requested_downloads': {final_filepath}")

            # Fallback to 'filepath' from the main info dict if not in requested_downloads
            # This 'filepath' might be before postprocessing, so further checks are needed.
            if not final_filepath and info.get('filepath'):
                 final_filepath = info.get('filepath') # This might be the path *before* postprocessing like audio conversion
                 logger.debug(f"Using top-level 'filepath' key: {final_filepath}. Verifying existence and format.")


            # If the path from info dict exists and is a file, use it.
            # This could be the final path if no significant postprocessing changed the name/ext.
            if final_filepath and os.path.exists(final_filepath) and os.path.isfile(final_filepath):
                 logger.info(f"Download and postprocessing successful. Final file (verified from info): {final_filepath}")
                 info['filepath'] = final_filepath # Ensure this is set for return
                 return info, final_filepath
            else:
                # If the filepath from info is not the final one (e.g., after FFmpegExtractAudio)
                # we need to deduce the correct path.
                logger.warning(f"File at '{final_filepath}' (from info dict) not found or not a file. Attempting to locate final processed file.")
                # ydl.prepare_filename(info) *after* download should give the path considering postprocessor changes (like .m4a)
                try:
                    # This should reflect the filename after postprocessing if 'outtmpl' and 'postprocessors' are set correctly
                    potential_path_after_pp = ydl.prepare_filename(info)
                    logger.debug(f"Path based on prepare_filename after download: {potential_path_after_pp}")

                    if os.path.exists(potential_path_after_pp) and os.path.isfile(potential_path_after_pp):
                         logger.info(f"Located final file via prepare_filename: {potential_path_after_pp}")
                         info['filepath'] = potential_path_after_pp # Update info with the correct path
                         return info, potential_path_after_pp
                    else:
                        # If prepare_filename doesn't yield the correct one (e.g., if ext changed by PP but not reflected)
                        # Try to guess based on preferred codec.
                        base_potential, _ = os.path.splitext(potential_path_after_pp)
                        preferred_codec = None
                        for pp_cfg in current_ydl_opts.get('postprocessors', []):
                            if pp_cfg.get('key') == 'FFmpegExtractAudio':
                                preferred_codec = pp_cfg.get('preferredcodec')
                                break
                        if preferred_codec:
                            check_path_with_codec = base_potential + "." + preferred_codec
                            if os.path.exists(check_path_with_codec) and os.path.isfile(check_path_with_codec):
                                logger.info(f"Located final file via preferred codec check: {check_path_with_codec}")
                                info['filepath'] = check_path_with_codec
                                return info, check_path_with_codec

                        logger.error(f"Could not locate the final processed audio file for {track_link} even after prepare_filename and codec check. Path from prepare_filename: {potential_path_after_pp}")
                        return info, None # Return info but no valid path

                except Exception as e_locate:
                    logger.error(f"Error trying to locate final file for {track_link}: {e_locate}", exc_info=True)
                    return info, None # Return info (which might be partial) but no path

    except yt_dlp.utils.DownloadError as e:
        # Specific yt-dlp download errors (network, unavailable, etc.)
        logger.error(f"yt-dlp DownloadError for {track_link}: {e}")
        # Try to get partial info if available in the exception
        partial_info = getattr(e, 'exc_info', [None, None, None])[1] # Get the original exception if wrapped
        if isinstance(partial_info, dict): return partial_info, None
        return None, None
    except Exception as e:
        # Other unexpected errors during download process
        logger.error(f"Unexpected download error for {track_link}: {e}", exc_info=True)
        return None, None


async def download_album_tracks(album_browse_id: str, progress_callback=None) -> List[Tuple[Dict, str]]:
    """
    Downloads all tracks from a given album browse ID using yt-dlp sequentially.
    Uses wrapped API calls for metadata and runs synchronous download_track in executor.
    """
    if not ytmusic:
        logger.error("YTMusic API client not initialized. Cannot download album.")
        await initialize_ytmusic_client()
        if not ytmusic:
            logger.error("Re-initialization of YTMusic client failed. Album download aborted.")
            if progress_callback: await progress_callback("album_error", error="YTMusic client not ready")
            return []


    logger.info(f"Attempting to download album/playlist sequentially: {album_browse_id}")
    downloaded_files: List[Tuple[Dict, str]] = []
    album_info, total_tracks, album_title = None, 0, album_browse_id

    try:
        logger.debug(f"Fetching album/playlist metadata for {album_browse_id}...")
        tracks_to_download = [] # List of track dicts from API
        entity_type_for_api = None # 'album' or 'playlist'

        # Determine if it's an album or playlist based on ID prefix
        if album_browse_id.startswith(('MPRE', 'MPLA', 'RDAM', 'OLAK5uy_')):
            entity_type_for_api = "album"
        elif album_browse_id.startswith(('PL', 'VL')):
            entity_type_for_api = "playlist"
        else:
            logger.info(f"ID {album_browse_id} type is ambiguous based on prefix. Will rely on yt-dlp analysis if API metadata fails.")
            # We can still try to fetch as album first, then playlist, or let yt-dlp handle it if both fail.
            # For now, let yt-dlp handle it if no clear prefix match.

        api_fetch_successful = False
        if entity_type_for_api == "album":
            try:
                album_info = await _api_get_album(album_browse_id)
                if album_info:
                    album_title = album_info.get('title', album_browse_id)
                    # Tracks can be in 'tracks' list or 'tracks'.'results' list
                    album_tracks_section = album_info.get('tracks')
                    if isinstance(album_tracks_section, list):
                         tracks_to_download = album_tracks_section
                    elif isinstance(album_tracks_section, dict) and 'results' in album_tracks_section and isinstance(album_tracks_section['results'], list):
                         tracks_to_download = album_tracks_section['results']
                    else:
                         logger.warning(f"Could not find 'tracks' list in album info structure for {album_browse_id}.")

                    total_tracks = album_info.get('trackCount') or len(tracks_to_download)
                    logger.info(f"Fetched album metadata: '{album_title}', Expected tracks: {total_tracks or len(tracks_to_download)}")
                    api_fetch_successful = True
                else: logger.warning(f"Could not fetch album metadata for {album_browse_id} via wrapped api.")
            except Exception as e_meta_album:
                 logger.warning(f"Error fetching album metadata for {album_browse_id}: {e_meta_album}. Will try yt-dlp analysis.", exc_info=True)

        elif entity_type_for_api == "playlist":
            try:
                 album_info = await _api_get_playlist(album_browse_id, limit=None) # Fetch all tracks
                 if album_info:
                     album_title = album_info.get('title', album_browse_id)
                     tracks_to_download = album_info.get('tracks', []) # 'tracks' is usually a list here
                     total_tracks = album_info.get('trackCount') or len(tracks_to_download)
                     logger.info(f"Fetched playlist metadata: '{album_title}', Expected tracks: {total_tracks or len(tracks_to_download)}")
                     api_fetch_successful = True
                 else: logger.warning(f"Could not fetch playlist metadata for {album_browse_id} via wrapped api.")
            except Exception as e_meta_playlist:
                 logger.warning(f"Error fetching playlist metadata for {album_browse_id}: {e_meta_playlist}. Will try yt-dlp analysis.", exc_info=True)


        if not api_fetch_successful or not tracks_to_download:
             logger.info(f"No tracks obtained from ytmusicapi for '{album_browse_id}' (Type: {entity_type_for_api or 'Unknown'}). Using yt-dlp to analyze and get track list...")
             try:
                 # Construct full URL for yt-dlp if it's just an ID
                 analysis_url = album_browse_id
                 if not album_browse_id.startswith("http"):
                     if entity_type_for_api == "album" or album_browse_id.startswith(('MPRE', 'MPLA', 'RDAM', 'OLAK5uy_')):
                         analysis_url = f"https://music.youtube.com/browse/{album_browse_id}"
                     elif entity_type_for_api == "playlist" or album_browse_id.startswith(('PL', 'VL')):
                         analysis_url = f"https://music.youtube.com/playlist?list={album_browse_id}"
                     # If it's a video ID, yt-dlp will treat it as a single item, which is fine, download_track will handle it.
                     # This function is more for albums/playlists.
                     else: # If truly unknown, pass the ID as is, yt-dlp might figure it out if it's a valid URL part
                          analysis_url = f"https://music.youtube.com/browse/{album_browse_id}" # Default guess for browse IDs
                          logger.warning(f"ID '{album_browse_id}' type still unknown, trying browse URL for yt-dlp analysis.")


                 # yt-dlp options for extracting playlist/album info without downloading
                 analysis_opts = {
                     'extract_flat': 'in_playlist', # Get info for each item in playlist/album
                     'skip_download': True,
                     'quiet': True,
                     'ignoreerrors': True, # Skip problematic tracks
                     'noplaylist': False, # We *want* playlist/album items
                     'cookiefile': YDL_OPTS.get('cookiefile') # Use cookies if available
                 }
                 loop = asyncio.get_running_loop()
                 # Run synchronous yt-dlp call in an executor
                 playlist_dict = await loop.run_in_executor(None, functools.partial(yt_dlp.YoutubeDL(analysis_opts).extract_info, analysis_url, download=False))

                 if playlist_dict and playlist_dict.get('entries'):
                     # Convert yt-dlp entries to the structure expected by the download loop
                     # (mainly needs 'videoId', 'title', 'artists')
                     tracks_to_download = []
                     for entry in playlist_dict['entries']:
                         if entry and entry.get('id'): # 'id' is videoId in yt-dlp flat extract
                             track_item = {
                                 'videoId': entry.get('id'),
                                 'title': entry.get('title', 'Unknown Title'),
                                 # yt-dlp might have 'channel' or 'uploader' for artist-like info
                                 'artists': [{'name': entry.get('channel') or entry.get('uploader') or 'Unknown Artist'}]
                                            if entry.get('channel') or entry.get('uploader') else [{'name': 'Unknown Artist'}]
                             }
                             tracks_to_download.append(track_item)

                     total_tracks = len(tracks_to_download)
                     # Update album_title if yt-dlp found a title for the playlist/album
                     if playlist_dict.get('title') and (album_title == album_browse_id or not album_title or entity_type_for_api is None):
                          album_title = playlist_dict['title']
                     logger.info(f"Extracted {total_tracks} tracks using yt-dlp analysis for '{album_title or analysis_url}'.")
                 else:
                     logger.error(f"yt-dlp analysis failed to return track entries for {analysis_url}.")
                     if progress_callback: await progress_callback("album_error", error="yt-dlp failed to get track list")
                     return []
             except Exception as e_analyze:
                 logger.error(f"Error during yt-dlp analysis phase for {album_browse_id} (URL: {analysis_url}): {e_analyze}", exc_info=True)
                 if progress_callback: await progress_callback("album_error", error=f"yt-dlp analysis error: {str(e_analyze)[:50]}")
                 return []

        if not tracks_to_download:
             logger.error(f"No tracks found to download for album/playlist {album_browse_id} after all attempts.")
             if progress_callback: await progress_callback("album_error", error="No tracks found to download")
             return []

        # Ensure total_tracks is accurate if API didn't provide it but yt-dlp did
        if total_tracks == 0 and tracks_to_download:
             total_tracks = len(tracks_to_download)

        if progress_callback:
            await progress_callback("analysis_complete", total_tracks=total_tracks, title=album_title)

        downloaded_count = 0
        loop = asyncio.get_running_loop() # Get current loop for run_in_executor

        for i, track_api_info in enumerate(tracks_to_download):
            current_track_num = i + 1
            video_id = track_api_info.get('videoId')
            # Use title/artists from API info if available, otherwise use defaults
            track_title_from_list = track_api_info.get('title') or f'Трек {current_track_num}'
            # 'artists' in API info is usually a list of dicts. format_artists handles it.
            track_artists_from_list = format_artists(track_api_info.get('artists')) # format_artists returns "Неизвестно" if None/empty

            if not video_id:
                logger.warning(f"Skipping track {current_track_num}/{total_tracks} ('{track_title_from_list}') due to missing videoId.")
                if progress_callback:
                     await progress_callback("track_failed", current=current_track_num, total=total_tracks, title=f"{track_title_from_list} (No ID)")
                continue

            download_link = f"https://music.youtube.com/watch?v={video_id}"

            if progress_callback:
                 perc = int(((current_track_num) / total_tracks) * 100) if total_tracks else 0
                 display_track_title = (track_title_from_list[:25] + '...') if len(track_title_from_list) > 28 else track_title_from_list
                 await progress_callback("track_downloading",
                                       current=current_track_num,
                                       total=total_tracks,
                                       percentage=perc,
                                       title=display_track_title)

            try:
                # download_track is synchronous, run in executor
                # functools.partial helps pass arguments to the function run in executor
                info_dict_from_dl, file_path_from_dl = await loop.run_in_executor(None, functools.partial(download_track, download_link))

                if file_path_from_dl and info_dict_from_dl:
                    actual_filename = os.path.basename(file_path_from_dl)
                    # Use title from yt-dlp's more detailed info if available
                    final_track_title = info_dict_from_dl.get('title', track_title_from_list)
                    logger.info(f"Successfully downloaded and processed track {current_track_num}/{total_tracks}: {actual_filename}")
                    downloaded_files.append((info_dict_from_dl, file_path_from_dl)) # Store detailed info from download
                    downloaded_count += 1
                    if progress_callback:
                         # Pass the title from the detailed info_dict_from_dl
                         await progress_callback("track_downloaded", current=downloaded_count, total=total_tracks, title=final_track_title)
                else:
                    logger.error(f"Failed to download/process track {current_track_num}/{total_tracks}: '{track_title_from_list}' ({video_id})")
                    if progress_callback:
                         await progress_callback("track_failed", current=current_track_num,
                                               total=total_tracks, title=track_title_from_list, reason="Ошибка загрузки")

            except Exception as e_track_dl:
                logger.error(f"Error during download process for track {current_track_num} ('{track_title_from_list}'): {e_track_dl}", exc_info=True)
                if progress_callback:
                     await progress_callback("track_failed", current=current_track_num, total=total_tracks, title=f"{track_title_from_list} (Error)")

            await asyncio.sleep(0.3) # Small delay between downloads

    except Exception as e_album_outer:
        logger.error(f"Error during album processing loop for {album_browse_id}: {e_album_outer}", exc_info=True)
        if progress_callback:
            await progress_callback("album_error", error=f"Outer error: {str(e_album_outer)[:50]}")

    logger.info(f"Finished sequential album download for '{album_title or album_browse_id}'. Successfully saved {len(downloaded_files)} out of {total_tracks or 'Unknown'} tracks attempted.")
    return downloaded_files


# =============================================================================
#                         LYRICS HANDLING (Redundant, keeping one copy)
# =============================================================================
# This function is already defined above. Removing this redundant definition.
# async def get_lyrics_for_track(...):


# =============================================================================
#                           THUMBNAIL HANDLING
# =============================================================================

@retry(max_tries=3, delay=1.0, exceptions=(requests.exceptions.RequestException, Exception))
async def download_thumbnail(url: str, output_dir: str = SCRIPT_DIR) -> Optional[str]:
    """
    Downloads a thumbnail image from a URL.
    This is an async function designed to run directly with await.
    """
    if not url or not isinstance(url, str) or not url.startswith(('http://', 'https://')):
        logger.warning(f"Invalid or non-HTTP/S thumbnail URL provided: {url}")
        return None

    logger.debug(f"Attempting to download thumbnail: {url}")
    temp_file_path = None
    response = None # Initialize response to None

    try:
        try:
            parsed_url = urlparse(url)
            base_name_from_url = os.path.basename(parsed_url.path) if parsed_url.path else "thumb"
        except Exception as parse_e:
            logger.warning(f"Could not parse URL path for thumbnail naming: {parse_e}. Using default 'thumb'.")
            base_name_from_url = "thumb"

        base_name, potential_ext = os.path.splitext(base_name_from_url)
        # Ensure extension is simple (e.g., .jpg, .png, .webp)
        if potential_ext and 1 < len(potential_ext) <= 5 and potential_ext[1:].isalnum(): # [1:] to skip dot
             ext = potential_ext.lower()
        else: ext = '.jpg' # Default extension

        if not base_name or base_name == potential_ext: base_name = "thumb" # Handle cases like ".jpg" as basename
        # Sanitize base_name for filesystem
        safe_base_name = re.sub(r'[^\w.\-]', '_', base_name)
        max_len = 40 # Limit length of base name part
        safe_base_name = (safe_base_name[:max_len] + '...') if len(safe_base_name) > max_len + 3 else safe_base_name

        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
        temp_filename = f"temp_thumb_{safe_base_name}_{timestamp}{ext}"
        temp_file_path = os.path.join(output_dir, temp_filename)

        loop = asyncio.get_running_loop()
        # Run requests.get in an executor as it's a blocking I/O call
        response = await loop.run_in_executor(None, lambda: requests.get(url, stream=True, timeout=25))
        response.raise_for_status() # Check for HTTP errors

        # Save the content to file (also blocking I/O)
        await loop.run_in_executor(None, functools.partial(save_response_to_file, response, temp_file_path))

        logger.debug(f"Thumbnail downloaded to temporary file: {temp_file_path}")

        # Verify image integrity (Pillow operations are blocking)
        try:
            await loop.run_in_executor(None, functools.partial(verify_image_file, temp_file_path))
            logger.debug(f"Thumbnail verified as valid image: {temp_file_path}")
            return temp_file_path
        except (FileNotFoundError, UnidentifiedImageError, SyntaxError, OSError, ValueError) as img_e:
             logger.error(f"Downloaded file is not a valid image ({url}): {img_e}. Deleting.")
             if os.path.exists(temp_file_path):
                 try: asyncio.create_task(cleanup_files(temp_file_path)) # Schedule cleanup
                 except Exception as rm_e: logger.warning(f"Could not remove invalid temp thumb {temp_file_path}: {rm_e}")
             return None # Return None if image is invalid

    except requests.exceptions.Timeout:
        logger.error(f"Timeout occurred while downloading thumbnail: {url}")
        raise # Re-raise to be caught by @retry or caller
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error downloading thumbnail {url}: {e}")
        if temp_file_path and os.path.exists(temp_file_path): # Cleanup partially downloaded file
            try: asyncio.create_task(cleanup_files(temp_file_path))
            except Exception as rm_e: logger.warning(f"Could not remove partial temp thumb {temp_file_path}: {rm_e}")
        raise # Re-raise
    except Exception as e_outer:
        logger.error(f"Unexpected error downloading thumbnail {url}: {e_outer}", exc_info=True)
        if temp_file_path and os.path.exists(temp_file_path): # Cleanup on other errors
            try: asyncio.create_task(cleanup_files(temp_file_path))
            except Exception as rm_e: logger.warning(f"Could not remove temp thumb {temp_file_path} after error: {rm_e}")
        raise # Re-raise
    finally:
        if response: # Ensure response is closed
            try: response.close()
            except Exception as close_e: logger.warning(f"Error closing response for {url}: {close_e}")


def save_response_to_file(response: requests.Response, filepath: str):
    """Synchronously saves a requests response stream to a file."""
    with open(filepath, 'wb') as out_file:
        shutil.copyfileobj(response.raw, out_file)


def verify_image_file(filepath: str):
    """Synchronously verifies if a file is a valid image."""
    with Image.open(filepath) as img:
        img.verify() # verify() is a basic check, might raise on corrupt images


@retry(max_tries=2, delay=1.0, exceptions=(UnidentifiedImageError, OSError, ValueError, Exception))
async def crop_thumbnail(image_path: str) -> Optional[str]:
    """
    Crops an image to a square aspect ratio (center crop) and saves it as JPEG.
    This is an async function designed to run directly with await.
    """
    if not image_path or not os.path.exists(image_path):
        logger.error(f"Cannot crop thumbnail, file not found: {image_path}")
        return None

    logger.debug(f"Processing thumbnail (cropping to square): {image_path}")
    # Ensure output path is unique enough to avoid clashes if original name is short
    base, ext = os.path.splitext(image_path)
    output_path = f"{base}_cropped_{datetime.datetime.now().strftime('%f')}.jpg" # Add microsecs for uniqueness
    img_rgb = None # Initialize to avoid UnboundLocalError

    loop = asyncio.get_running_loop()

    try:
        # Image.open is blocking
        img = await loop.run_in_executor(None, Image.open, image_path)

        # All Pillow operations are blocking, run in executor
        try:
            img_rgb = img
            if img.mode != 'RGB':
                logger.debug(f"Image mode is '{img.mode}', converting to RGB for cropping.")
                try:
                    # Create a white background for transparency handling
                    bg = await loop.run_in_executor(None, Image.new, "RGB", img.size, (255, 255, 255))
                    if img.mode in ('RGBA', 'LA') and len(img.split()) > 3: # Check if alpha channel exists
                        bands = await loop.run_in_executor(None, img.split)
                        alpha_band = bands[-1]
                        # Paste using alpha band as mask
                        await loop.run_in_executor(None, functools.partial(bg.paste, img, mask=alpha_band))
                    else: # No alpha or not RGBA/LA, simple paste
                        await loop.run_in_executor(None, functools.partial(bg.paste, img))
                    img_rgb = bg
                except Exception as conv_e:
                     logger.warning(f"Could not convert image {os.path.basename(image_path)} from {img.mode} to RGB using background paste: {conv_e}. Attempting basic conversion.")
                     try: img_rgb = await loop.run_in_executor(None, functools.partial(img.convert, 'RGB'))
                     except Exception as basic_conv_e:
                         logger.error(f"Failed basic RGB conversion for {os.path.basename(image_path)}: {basic_conv_e}. Cannot crop.")
                         return None # Cannot proceed if RGB conversion fails

            width, height = img_rgb.size
            min_dim = min(width, height)
            left = (width - min_dim) / 2
            top = (height - min_dim) / 2
            right = (width + min_dim) / 2
            bottom = (height + min_dim) / 2
            crop_box = tuple(map(int, (left, top, right, bottom))) # Ensure integer coordinates
            img_cropped = await loop.run_in_executor(None, functools.partial(img_rgb.crop, crop_box))

            # Save the cropped image
            await loop.run_in_executor(None, functools.partial(img_cropped.save, output_path, "JPEG", quality=90))

            logger.debug(f"Thumbnail cropped and saved successfully: {output_path}")
            return output_path

        except (UnidentifiedImageError, SyntaxError, OSError, ValueError) as e: # Pillow specific errors
            logger.error(f"Cannot process thumbnail, invalid image file format or processing error during Pillow ops: {image_path} - {e}")
            raise # Re-raise for @retry or caller
        except Exception as e: # Other unexpected errors during Pillow ops
            logger.error(f"Unexpected error during Pillow operations on thumbnail {os.path.basename(image_path)}: {e}", exc_info=True)
            if os.path.exists(output_path): # Cleanup partially created file
                try: asyncio.create_task(cleanup_files(output_path))
                except Exception as rm_e: logger.warning(f"Could not remove partial cropped thumb {output_path}: {rm_e}")
            raise # Re-raise

    except FileNotFoundError: # If Image.open fails because file disappeared
        logger.error(f"Cannot process thumbnail, file not found during Pillow ops: {image_path}")
        raise # Re-raise
    except Exception as e: # Catch-all for other errors in this block
        logger.error(f"Error processing (cropping) thumbnail {os.path.basename(image_path)} (outer block): {e}", exc_info=True)
        if os.path.exists(output_path): # Cleanup on outer error
            try: asyncio.create_task(cleanup_files(output_path))
            except Exception as rm_e: logger.warning(f"Could not remove partial cropped thumb {output_path}: {rm_e}")
        raise # Re-raise


# =============================================================================
#                         FILE CLEANUP UTILITY
# =============================================================================

async def cleanup_files(*files: Optional[str]):
    """
    Safely removes specified files and files matching common temporary patterns.
    Ensures files are within SCRIPT_DIR.
    """
    temp_patterns = [
        os.path.join(SCRIPT_DIR, "temp_thumb_*"),    # Downloaded original thumbnails
        os.path.join(SCRIPT_DIR, "*_cropped_*.jpg"), # Cropped thumbnails (format from crop_thumbnail)
        os.path.join(SCRIPT_DIR, "*.part"),          # yt-dlp partial files
        os.path.join(SCRIPT_DIR, "*.ytdl"),          # yt-dlp temporary files
        os.path.join(SCRIPT_DIR, "*.webp"),          # Common temp image format from web
        os.path.join(SCRIPT_DIR, "lyrics_*.html"),   # Lyrics HTML files
        os.path.join(SCRIPT_DIR, "N_A.jpg"),         # Placeholder thumbnails sometimes created
        os.path.join(SCRIPT_DIR, "N_A.png"),
    ]

    all_files_to_remove = set()
    # Add explicitly passed files first, ensuring they are in SCRIPT_DIR
    for f_path in files:
        if f_path and isinstance(f_path, str):
            try:
                # Resolve to absolute path to prevent relative path issues (e.g., "temp_thumb_123.jpg")
                abs_f_path = os.path.abspath(f_path)
                # Ensure the file is within the SCRIPT_DIR for safety
                if abs_f_path.startswith(os.path.abspath(SCRIPT_DIR)):
                     all_files_to_remove.add(abs_f_path)
                else:
                     logger.warning(f"Skipping cleanup of file outside script directory: {f_path} (resolved: {abs_f_path})")
            except Exception as path_e:
                 logger.warning(f"Could not process path for file '{f_path}' during cleanup prep: {path_e}")


    # Add files matching glob patterns
    loop = asyncio.get_running_loop()
    script_abs_path = os.path.abspath(SCRIPT_DIR) # Cache for comparison

    for pattern in temp_patterns:
        try:
            # Ensure pattern is absolute for glob and starts within SCRIPT_DIR
            abs_pattern = os.path.abspath(pattern)
            if not abs_pattern.startswith(script_abs_path):
                logger.warning(f"Skipping glob pattern outside script directory: {pattern}")
                continue

            matched_files = await loop.run_in_executor(None, glob.glob, abs_pattern)
            if matched_files:
                logger.debug(f"Globbed {len(matched_files)} files for cleanup pattern: {pattern}")
                for mf in matched_files:
                    abs_mf = os.path.abspath(mf)
                    if abs_mf.startswith(script_abs_path): # Double check resolved path
                        all_files_to_remove.add(abs_mf)
                    # else: (should not happen if abs_pattern was checked)
        except Exception as e:
            logger.error(f"Error during glob matching for pattern '{pattern}': {e}")

    removed_count = 0
    if not all_files_to_remove:
        logger.debug("Cleanup called, but no files specified or matched for removal.")
        return

    logger.info(f"Attempting to clean up {len(all_files_to_remove)} potential files...")
    files_list = list(all_files_to_remove) # Convert set to list for iteration

    # Perform deletions (os.remove is blocking)
    for file_path_to_remove in files_list:
        try:
            if await loop.run_in_executor(None, os.path.isfile, file_path_to_remove): # Check if it's a file
                 await loop.run_in_executor(None, os.remove, file_path_to_remove)
                 logger.debug(f"Removed file: {file_path_to_remove}")
                 removed_count += 1
                 await asyncio.sleep(0.01) # Tiny sleep to yield if many files
        except FileNotFoundError:
             logger.debug(f"File not found for removal (already deleted?): {file_path_to_remove}")
        except OSError as e: # Catch permission errors etc.
            logger.error(f"Error removing file {file_path_to_remove}: {e}")
        except Exception as e_remove: # Catch other unexpected errors
            logger.error(f"Unexpected error removing file {file_path_to_remove}: {e_remove}")

    if removed_count > 0:
        logger.info(f"Successfully cleaned up {removed_count} files.")
    elif all_files_to_remove: # If there were files to remove but none were
        logger.info(f"Cleanup finished. No files were actually removed (checked {len(all_files_to_remove)}).")


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
        if current_text != text: # Only edit if text has changed
            await progress_message.edit(text)
    except telethon_errors.MessageNotModifiedError:
        pass # No change, ignore
    except telethon_errors.MessageIdInvalidError:
        logger.warning(f"Failed to update progress: Message {progress_message.id} seems invalid or was deleted.")
    except telethon_errors.FloodWaitError as e:
         logger.warning(f"Flood wait ({e.seconds}s) while updating progress message {progress_message.id}. Pausing.")
         await asyncio.sleep(e.seconds + 1.0) # Add a small buffer
    except Exception as e:
        logger.warning(f"Failed to update progress message {progress_message.id}: {type(e).__name__} - {e}")

async def clear_previous_responses(chat_id: int):
    """
    Deletes previously sent bot messages stored for a specific chat.
    """
    global previous_bot_messages
    if chat_id not in previous_bot_messages or not previous_bot_messages[chat_id]:
        return

    messages_to_delete = previous_bot_messages.pop(chat_id, []) # Get and clear list for this chat
    if not messages_to_delete: return

    # Filter out None or invalid message objects (though unlikely if stored correctly)
    valid_messages_to_delete = [msg for msg in messages_to_delete if msg and isinstance(msg, types.Message)]

    if not valid_messages_to_delete:
        logger.debug(f"No valid messages to delete found for chat {chat_id}.")
        return

    deleted_count = 0
    failed_to_delete_ids = [] # Store IDs that failed

    logger.info(f"Attempting to clear {len(valid_messages_to_delete)} previous bot messages in chat {chat_id}")

    # Telegram API allows deleting up to 100 messages at once
    chunk_size = 100
    for i in range(0, len(valid_messages_to_delete), chunk_size):
        chunk = valid_messages_to_delete[i : i + chunk_size]
        message_ids = [msg.id for msg in chunk]
        if not message_ids: continue

        try:
            await client.delete_messages(chat_id, message_ids)
            deleted_count += len(message_ids)
            logger.debug(f"Deleted {len(message_ids)} messages in chat {chat_id} (Chunk {i//chunk_size + 1}).")
        except telethon_errors.FloodWaitError as e:
             wait_time = e.seconds
             logger.warning(f"Flood wait ({wait_time}s) during message clearing chunk in chat {chat_id}. Pausing and will retry this chunk later if needed (currently, these are lost).")
             failed_to_delete_ids.extend(message_ids) # Mark these as failed for now
             await asyncio.sleep(wait_time + 1.5)
        except (telethon_errors.MessageDeleteForbiddenError, telethon_errors.MessageIdInvalidError) as e:
             # Some messages might have been deleted by user, or bot lacks permission
             logger.warning(f"Cannot delete some messages in chunk for chat {chat_id} ({len(message_ids)} IDs): {type(e).__name__} - {e}. These will be skipped.")
             # No need to add to failed_to_delete_ids if they are invalid/forbidden, as they can't be deleted by us.
        except Exception as e_chunk:
             logger.error(f"Unexpected error deleting message chunk in chat {chat_id}: {e_chunk}", exc_info=True)
             failed_to_delete_ids.extend(message_ids) # Mark as failed on unexpected error

    if deleted_count > 0:
        logger.info(f"Cleared {deleted_count} previous bot messages for chat {chat_id}.")
    if failed_to_delete_ids:
        logger.warning(f"Failed to delete {len(failed_to_delete_ids)} messages (IDs: {failed_to_delete_ids}) in chat {chat_id} after attempts. They are no longer tracked for auto-clear.")


async def store_response_message(chat_id: int, message: Optional[types.Message]):
    """
    Stores a message object to be potentially cleared later by auto_clear.
    """
    if not message or not isinstance(message, types.Message) or not chat_id:
        return

    if not config.get("auto_clear", True): # If auto_clear is off, don't store.
        return

    global previous_bot_messages
    if chat_id not in previous_bot_messages:
        previous_bot_messages[chat_id] = []

    # Avoid duplicate storage
    if message not in previous_bot_messages[chat_id]:
        previous_bot_messages[chat_id].append(message)
        logger.debug(f"Stored message {message.id} for clearing in chat {chat_id}. (Total tracked for chat: {len(previous_bot_messages[chat_id])})")


async def send_long_message(event: events.NewMessage.Event, text: str, prefix: str = ""):
    """Sends a long message by splitting it into chunks, respecting Telegram's limits."""
    MAX_LEN = 4096 # Telegram's max message length
    sent_msgs = []
    current_message = prefix.strip() # Start with prefix
    lines = text.split('\n')

    for line in lines:
        # Check if adding the new line (plus a newline character) exceeds MAX_LEN
        # Add 1 for the potential newline character if current_message is not empty
        space_needed = len(line) + (1 if current_message else 0)

        if len(current_message) + space_needed > MAX_LEN:
            # Current message + new line is too long. Send current message.
            if len(current_message) > 0: # Ensure there's something to send
                try:
                    msg = await event.respond(current_message)
                    sent_msgs.append(msg)
                    await asyncio.sleep(0.3) # Small delay between sending parts
                except Exception as e:
                    logger.error(f"Failed to send part of long message: {e}")
            # Start new message with prefix (if any) and current line
            current_message = (prefix.strip() + "\n" + line) if prefix.strip() else line
        else:
            # Append line to current message
            if current_message: # If message already has content, add newline first
                 current_message += "\n" + line
            else: # If message is empty (e.g., first line after prefix), just add line
                 current_message += line


    # Send any remaining part of the message
    # Ensure it's not just the prefix or empty
    if current_message.strip() and (not prefix.strip() or current_message.strip() != prefix.strip()):
         try:
            msg = await event.respond(current_message)
            sent_msgs.append(msg)
         except Exception as e:
             logger.error(f"Failed to send final part of long message: {e}")

    # Store all sent messages for auto-clear
    for m in sent_msgs:
        await store_response_message(event.chat_id, m)


async def send_lyrics(event: events.NewMessage.Event, lyrics_text: str, lyrics_header: str, track_title: str, video_id: str):
    """
    Sends lyrics. If too long, sends as an HTML file.
    """
    MAX_LEN = 4096 # Telegram's max message length for text
    # Estimate length: header + "------------" + lyrics text + some buffer for markdown
    estimated_length = len(lyrics_header) + 15 + len(lyrics_text) + 200 # Rough estimate

    if estimated_length <= MAX_LEN:
        logger.info(f"Sending lyrics for '{track_title}' directly (Estimated Length: {estimated_length})")
        # Construct full message and send via send_long_message to handle potential internal splits if estimate was off
        full_lyrics_message = lyrics_header + "\n" + lyrics_text
        await send_long_message(event, full_lyrics_message) # No prefix needed here, header is part of text
    else:
        logger.info(f"Lyrics for '{track_title}' too long (estimated {estimated_length} > {MAX_LEN}). Sending as HTML file.")

        # --- Prepare HTML content ---
        # Extract title and artist from header for HTML more reliably
        html_display_title = track_title # Default to passed track_title
        html_display_artist = "Неизвестный исполнитель" # Default

        header_lines = lyrics_header.split('\n')
        if header_lines:
            # Example header: "📜 **Текст песни:** Song Title - Artist Name"
            title_artist_match = re.search(r"\*\*Текст песни:\*\*\s*(.+?)\s*-\s*(.+)", header_lines[0])
            if title_artist_match:
                html_display_title = title_artist_match.group(1).strip()
                html_display_artist = title_artist_match.group(2).strip()
            else: # Fallback if regex fails, try to use what was passed
                logger.debug(f"Could not parse title/artist from lyrics_header for HTML: {header_lines[0]}")


        html_source_line_text = ""
        source_line_from_header = next((line for line in header_lines if "Источник:" in line), None)
        if source_line_from_header:
             source_match_html = re.search(r"\(Источник:\s*(.*?)\)_", source_line_from_header)
             if source_match_html:
                  html_source_line_text = source_match_html.group(1).strip()


        # Use the html module for escaping, already imported
        escaped_html_title = html.escape(html_display_title)
        escaped_html_artist = html.escape(html_display_artist)
        escaped_html_source = html.escape(html_source_line_text)
        escaped_lyrics_text = html.escape(lyrics_text)


        html_content = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{escaped_html_title} - текст песни</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; padding: 20px; background-color: #f8f9fa; color: #212529; margin: 0; }}
        .container {{ max-width: 800px; margin: 20px auto; background: #ffffff; padding: 30px; border-radius: 8px; box-shadow: 0 4px 15px rgba(0,0,0,0.07); }}
        h1 {{ color: #343a40; border-bottom: 2px solid #dee2e6; padding-bottom: 15px; margin-top: 0; margin-bottom: 10px; font-size: 2em; font-weight: 600; }}
        .artist-info {{ font-size: 1.2em; color: #495057; margin-bottom: 20px; font-weight: 500; }}
        .source {{ font-size: 0.9em; color: #6c757d; margin-bottom: 30px; font-style: italic; }}
        pre {{ white-space: pre-wrap; word-wrap: break-word; background: #e9ecef; padding: 20px; border-radius: 5px; font-family: 'Menlo', 'Consolas', 'Courier New', monospace; font-size: 1.05em; line-height: 1.7; border: 1px solid #ced4da; overflow-x: auto; }}
        ::-webkit-scrollbar {{ width: 8px; height: 8px; }} ::-webkit-scrollbar-track {{ background: #f1f1f1; border-radius: 10px; }} ::-webkit-scrollbar-thumb {{ background: #adb5bd; border-radius: 10px; }} ::-webkit-scrollbar-thumb:hover {{ background: #868e96; }}
    </style>
</head>
<body><div class="container"><h1>{escaped_html_title}</h1>
{f'<p class="artist-info">{escaped_html_artist}</p>' if escaped_html_artist and escaped_html_artist != "Неизвестный исполнитель" else ''}
{f'<p class="source">Источник: {escaped_html_source}</p>' if escaped_html_source else ''}
<pre>{escaped_lyrics_text}</pre>
</div></body></html>"""

        # Create a safe filename
        safe_title_for_file = re.sub(r'[^\w\-]+', '_', track_title)[:50] # Sanitize and shorten
        timestamp_file = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        # video_id can be None if called from download -s before ID is known for lyrics header.
        safe_video_id_part = f"_{video_id}" if video_id and video_id != 'N/A' else ""
        temp_filename = f"lyrics_{safe_title_for_file}{safe_video_id_part}_{timestamp_file}.html"
        temp_filepath = os.path.join(SCRIPT_DIR, temp_filename)
        sent_file_msg = None

        try:
            loop = asyncio.get_running_loop()
            # Write file (blocking I/O)
            await loop.run_in_executor(None, functools.partial(write_text_file, temp_filepath, html_content))
            logger.debug(f"Saved temporary HTML lyrics file: {temp_filepath}")

            caption_for_file = f"📜 Текст песни '{track_title}' (слишком длинный, отправлен в виде файла)"
            display_filename_tg = f"{safe_title_for_file}_lyrics.html" # Filename shown in Telegram

            sent_file_msg = await client.send_file(
                event.chat_id,
                file=temp_filepath,
                caption=caption_for_file,
                attributes=[types.DocumentAttributeFilename(file_name=display_filename_tg)],
                force_document=True, # Send as a document
                reply_to=event.message.id # Reply to original user command
            )
            await store_response_message(event.chat_id, sent_file_msg) # Store this file message for auto-clear
            logger.info(f"Sent lyrics for '{track_title}' as HTML file: {display_filename_tg}")

        except Exception as e_html:
            logger.error(f"Failed to create/send HTML lyrics file for {video_id or 'unknown track'}: {e_html}", exc_info=True)
            fail_msg = await event.reply(f"❌ Не удалось отправить текст песни '{track_title}' в виде файла.")
            await store_response_message(event.chat_id, fail_msg)
        finally:
            # Schedule cleanup of the temporary HTML file
            if os.path.exists(temp_filepath): # Check existence before scheduling
                logger.debug(f"Scheduling cleanup for temporary HTML file: {temp_filepath}")
                asyncio.create_task(cleanup_files(temp_filepath))


def write_text_file(filepath: str, content: str):
    """Synchronously writes text content to a file."""
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)


# =============================================================================
#                         COMMAND HANDLERS
# =============================================================================

@client.on(events.NewMessage)
async def handle_message(event: events.NewMessage.Event):
    """Main handler for incoming messages."""

    if not config.get("bot_enabled", True): return
    if not event.message or not event.message.text or event.message.via_bot or not event.sender_id:
        return

    # --- Authorization Check (Owner only) ---
    global BOT_OWNER_ID
    if not BOT_OWNER_ID: # Should be set in main()
        logger.error("BOT_OWNER_ID is not set. Cannot authorize commands.")
        return

    is_self = event.message.out # If message is outgoing (i.e., from the bot's own account)
    sender_id = event.sender_id
    is_owner = sender_id == BOT_OWNER_ID

    is_authorised = is_self or is_owner # Only owner or self can use commands

    if not is_authorised:
        message_text_check = event.message.text.strip()
        prefix = config.get("prefix", ",")
        if message_text_check.startswith(prefix): # Log if an unauthorized user tries a command
             logger.warning(f"Ignoring unauthorized command attempt from user: {sender_id} in chat {event.chat_id}: '{message_text_check[:50]}...'")
        return

    # --- Command Handling ---
    message_text = event.message.text
    prefix = config.get("prefix", ",")
    if not message_text.startswith(prefix): return # Not a command

    command_string = message_text[len(prefix):].strip()
    if not command_string: return # Empty command after prefix

    parts = command_string.split(maxsplit=1)
    command = parts[0].lower()
    args_str = parts[1] if len(parts) > 1 else ""
    # Smart split args, considering quotes for multi-word arguments (though not heavily used yet)
    # For now, simple split is fine as most commands take IDs/links or simple flags.
    args = args_str.split() # Basic split by space

    logger.info(f"Received command: '{command}', Args: {args}, User: {sender_id}, Chat: {event.chat_id} (Owner: {is_owner}, Self: {is_self})")

    # Delete the command message if it's from self or owner
    if is_self or is_owner: # is_self for userbot mode, is_owner if bot is run by someone else but owner sends command
        try:
            await event.message.delete()
            logger.debug(f"Deleted user/owner command message {event.message.id}")
        except Exception as e_del:
            logger.warning(f"Failed to delete user/owner command message {event.message.id}: {e_del}")

    commands_to_clear_for = (
        "search", "see", "last", "host", "download", "help", "dl",
        "rec", "alast", "likes", "text", "lyrics", "clear"
        # "ping" or other simple commands might not need auto-clear
    )
    if config.get("auto_clear", True) and command in commands_to_clear_for:
         logger.debug(f"Auto-clearing previous responses for '{command}' in chat {event.chat_id}")
         await clear_previous_responses(event.chat_id)

    # Command handlers dictionary (defined globally or imported if modularized)
    # handlers = { ... } # Defined later or in main
    handler_func = handlers.get(command)

    if handler_func:
        try:
            await handler_func(event, args)
        except Exception as e_handler:
            error_details = traceback.format_exc()
            logger.error(f"Error executing handler for command '{command}': {e_handler}\n{error_details}")
            try:
                error_msg_text = (f"❌ Произошла внутренняя ошибка при обработке команды `{command}`.\n"
                                  f"```\n{type(e_handler).__name__}: {str(e_handler)[:200]}\n```" # Limit length of error message
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
        # If auto-clear is on, this command is somewhat redundant but can confirm behavior.
        confirm_msg = await event.respond("ℹ️ Предыдущие ответы этого бота обычно очищаются автоматически перед новым ответом на команду.", delete_in=15) # Increased time
        logger.info(f"Executed 'clear' command (auto-clear enabled) in chat {event.chat_id}.")
        # No need to store confirm_msg if it auto-deletes quickly.
        # If user wants manual clear even with auto-clear on, they can disable auto_clear.
    else:
        # If auto-clear is off, then this command is useful.
        logger.info(f"Executing manual clear via command in chat {event.chat_id} (auto-clear is OFF).")
        await clear_previous_responses(event.chat_id) # Perform the actual clearing
        confirm_msg = await event.respond("✅ Предыдущие ответы бота (которые были отслежены) очищены вручную.", delete_in=10)
        # Don't store this confirmation if it auto-deletes.
    # No explicit deletion of confirm_msg needed due to delete_in.


# -------------------------
# Command: search (-t, -a, -p, -e, -v)
# -------------------------
async def handle_search(event: events.NewMessage.Event, args: List[str]):
    """Handles the search command."""
    valid_type_flags = {"-t", "-a", "-p", "-e"} # -t: tracks, -a: albums, -p: playlists, -e: artists/endpoints
    prefix = config.get("prefix", ",")

    search_type_flag = None # e.g., "-t"
    is_video_search = False # for -v flag
    query_parts = []

    for arg in args:
        if arg in valid_type_flags:
            if search_type_flag is None: # Take the first type flag encountered
                search_type_flag = arg
            else:
                logger.warning(f"Multiple type flags provided in search (e.g., -t -a), using first one: {search_type_flag}")
        elif arg == "-v":
            is_video_search = True
        else:
            query_parts.append(arg) # Collect parts of the search query

    query = " ".join(query_parts).strip()

    if not query:
        usage_text = (f"**Использование:** `{prefix}search [-t|-a|-p|-e] [-v] <запрос>`\n"
                      f"Типы поиска: `-t` (треки, по умолчанию), `-a` (альбомы), `-p` (плейлисты), `-e` (исполнители).\n"
                      f"Флаг `-v` (видео): Искать видеоклипы. Может сочетаться с `-t` (видеоклипы песен) или использоваться отдельно (общий поиск видео).")
        await store_response_message(event.chat_id, await event.reply(usage_text))
        return

    # Determine ytmusicapi filter type
    filter_type_api = None # For ytmusic.search()
    search_category_display = "треков" # For messages

    if search_type_flag == "-t":
        filter_type_api = "videos" if is_video_search else "songs"
        search_category_display = "видеоклипов" if is_video_search else "треков"
    elif search_type_flag == "-a":
        filter_type_api = "albums"
        search_category_display = "альбомов"
        if is_video_search: logger.warning("-v flag ignored with -a (albums search).")
    elif search_type_flag == "-p":
        filter_type_api = "playlists"
        search_category_display = "плейлистов"
        if is_video_search: logger.warning("-v flag ignored with -p (playlists search).")
    elif search_type_flag == "-e":
        filter_type_api = "artists" # ytmusicapi uses 'artists' for this filter
        search_category_display = "исполнителей"
        if is_video_search: logger.warning("-v flag ignored with -e (artists search).")
    else: # No specific type flag, default behavior
        filter_type_api = "videos" if is_video_search else "songs"
        search_category_display = "видеоклипов" if is_video_search else "треков"


    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    sent_message = None # To store the final message for auto-clear

    try:
        if use_progress:
            statuses = {"Поиск": "⏳ Ожидание...", "Форматирование": "⏸️"}
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))
            await store_response_message(event.chat_id, progress_message)

        if use_progress:
            query_display = (query[:30] + '...') if len(query) > 33 else query
            statuses["Поиск"] = f"🔄 Поиск {search_category_display} '{query_display}'..."
            await update_progress(progress_message, statuses)

        search_limit = min(max(1, config.get("default_search_limit", 8)), 20) # YTMusic API limit usually 20
        results = await _api_search(query, filter_type=filter_type_api, limit=search_limit)

        if use_progress:
            search_status_msg = f"✅ Найдено: {len(results)}" if results else "ℹ️ Ничего не найдено"
            statuses["Поиск"] = search_status_msg
            statuses["Форматирование"] = "🔄 Подготовка..." if results else "➖"
            await update_progress(progress_message, statuses)

        if not results:
            final_message_text = f"ℹ️ По запросу `{query}` ({search_category_display}) ничего не найдено."
            if progress_message: await progress_message.edit(final_message_text); sent_message = progress_message
            else: sent_message = await event.reply(final_message_text)
        else:
            response_lines = []
            display_limit = min(len(results), MAX_SEARCH_RESULTS_DISPLAY) # Max items to show in TG message
            type_labels_header = {"songs": "Треки", "albums": "Альбомы", "playlists": "Плейлисты", "artists": "Исполнители", "videos": "Видео"}
            header_label = type_labels_header.get(filter_type_api, search_category_display.capitalize())
            response_text_final = f"**🔎 Результаты поиска ({header_label}) для `{query}`:**\n"

            for i, item in enumerate(results[:display_limit]):
                if not item or not isinstance(item, dict):
                    logger.warning(f"Skipping invalid item in search results: {item}")
                    continue

                line_parts = [f"{i + 1}. "]
                try:
                    title = item.get('title', 'Неизвестно')
                    item_id = item.get('videoId') or item.get('browseId') # videoId for songs/videos, browseId for others
                    link_prefix = ""

                    if filter_type_api in ["songs", "videos"]:
                        artists_formatted = format_artists(item.get('artists'))
                        duration_str = item.get('duration') # "M:SS" or "H:MM:SS"
                        views = item.get('views') # For videos
                        link_prefix = "https://music.youtube.com/watch?v=" if filter_type_api == "songs" else "https://www.youtube.com/watch?v="

                        line_parts.append(f"**{title}** - {artists_formatted}")
                        if duration_str: line_parts.append(f"({duration_str})")
                        if views and filter_type_api == "videos": line_parts.append(f"[{views}]")

                    elif filter_type_api == "albums":
                        artists_formatted = format_artists(item.get('artists'))
                        year = item.get('year')
                        link_prefix = "https://music.youtube.com/browse/"
                        line_parts.append(f"**{title}** - {artists_formatted}")
                        if year: line_parts.append(f"({year})")

                    elif filter_type_api == "artists":
                        # 'artist' key for name, 'browseId' for ID
                        artist_name = item.get('artist', title) # Fallback to title if 'artist' key missing
                        link_prefix = "https://music.youtube.com/channel/" # Artist pages are channels
                        line_parts.append(f"**{artist_name}**")
                        # Artist results might not have much other info directly in search item

                    elif filter_type_api == "playlists":
                        author_formatted = format_artists(item.get('author')) # 'author' for playlists
                        item_count = item.get('itemCount') # Number of tracks in playlist
                        # Playlist browseId might start with 'VL', remove it for link
                        if item_id and item_id.startswith("VL"): item_id_for_link = item_id[2:]
                        else: item_id_for_link = item_id
                        link_prefix = "https://music.youtube.com/playlist?list="
                        item_id = item_id_for_link # Use modified ID for link

                        line_parts.append(f"**{title}** (Автор: {author_formatted})")
                        if item_count: line_parts.append(f"[{item_count} треков]")


                    # Construct the line
                    full_line = " ".join(part for part in line_parts if part) # Join non-empty parts
                    if item_id and link_prefix:
                        full_link = f"{link_prefix}{item_id}"
                        full_line += f"\n   └ [Ссылка]({full_link})"
                    response_lines.append(full_line)

                except Exception as fmt_e:
                     logger.error(f"Error formatting search result item {i+1} (Type: {filter_type_api}): {item} - {fmt_e}", exc_info=True)
                     response_lines.append(f"{i + 1}. ⚠️ Ошибка форматирования данных.")

            response_text_final += "\n\n".join(response_lines)
            if len(results) > display_limit:
                response_text_final += f"\n\n... и еще {len(results) - display_limit}."

            if use_progress:
                statuses["Форматирование"] = "✅ Готово"
                await update_progress(progress_message, statuses)
                await progress_message.edit(response_text_final, link_preview=False)
                sent_message = progress_message
            else:
                sent_message = await event.reply(response_text_final, link_preview=False)

    except ValueError as e: # e.g., from invalid limit parsing in config
        error_text = f"⚠️ Ошибка конфигурации поиска: {e}"
        logger.warning(error_text)
        if use_progress and progress_message:
            statuses["Поиск"] = str(statuses.get("Поиск", "⏸️")).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌")
            statuses["Форматирование"] = "❌"
            try: await update_progress(progress_message, statuses)
            except Exception: pass
            try: await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}"); sent_message = progress_message
            except Exception: sent_message = await event.reply(error_text)
        else: sent_message = await event.reply(error_text)
    except Exception as e:
        logger.error(f"Неожиданная ошибка в команде search: {e}", exc_info=True)
        error_text = f"❌ Произошла неожиданная ошибка при поиске:\n`{type(e).__name__}: {str(e)[:100]}`"
        if use_progress and progress_message:
            for task_key in statuses: statuses[task_key] = str(statuses[task_key]).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌").replace("⏸️", "❌")
            try: await update_progress(progress_message, statuses)
            except Exception: pass
            try:
                await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}")
                sent_message = progress_message
            except Exception:
                sent_message = await event.reply(error_text)
        else:
            sent_message = await event.reply(error_text)
    finally:
        if sent_message: # Ensure the final message (success or error) is stored
            await store_response_message(event.chat_id, sent_message)

# -------------------------
# Command: see (-t, -a, -p, -e) [-i] [-txt]
# -------------------------
async def handle_see(event: events.NewMessage.Event, args: List[str]):
    """Handles the 'see' command."""
    valid_flags = {"-t", "-a", "-p", "-e"}
    prefix = config.get("prefix", ",")

    if not args:
        usage = (f"**Использование:** `{prefix}see [-t|-a|-p|-e] [-i] [-txt] <ID или ссылка>`\n"
                 f"Флаги: `-i` (включить обложку), `-txt` (включить текст песни).\n"
                 f"Указывать тип (флаг вида `-t`) необязательно, бот попробует определить автоматически.")
        await store_response_message(event.chat_id, await event.reply(usage))
        return

    entity_type_hint_flag = None
    include_cover = False
    include_lyrics = False
    link_or_id_arg = None
    remaining_args = list(args) # Work with a copy

    # Parse flags first
    if "-i" in remaining_args:
        include_cover = True
        remaining_args.remove("-i")
    if "-txt" in remaining_args:
        include_lyrics = True
        remaining_args.remove("-txt")

    # Parse entity type hint flag
    for arg_idx, arg_val in enumerate(remaining_args):
        if arg_val in valid_flags:
            entity_type_hint_flag = arg_val
            remaining_args.pop(arg_idx) # Remove the flag from args
            break # Take the first one

    # The first remaining argument should be the link or ID
    if remaining_args:
        link_or_id_arg = remaining_args[0]
        if len(remaining_args) > 1:
             logger.warning(f"Ignoring extra arguments in see command after link/ID: {remaining_args[1:]}")
    else: # No link/ID provided after parsing flags
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не указана ссылка или ID для команды `see`."))
        return


    hint_map = {"-t": "track", "-a": "album", "-p": "playlist", "-e": "artist"}
    entity_type_hint = hint_map.get(entity_type_hint_flag) if entity_type_hint_flag else None

    entity_id = extract_entity_id(link_or_id_arg)
    if not entity_id:
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не удалось распознать ID из `{link_or_id_arg}`."))
        return

    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    temp_thumb_file, processed_thumb_file = None, None # For thumbnail processing
    final_info_message_object = None # Will hold the message object for the main info (text or with picture)
    files_to_clean_on_exit = []
    lyrics_message_handled_storage = False # True if send_lyrics sends a message and stores it

    try:
        if use_progress:
            statuses = {"Получение данных": "⏳ Ожидание...", "Форматирование": "⏸️"}
            if include_cover: statuses["Обложка"] = "⏸️"
            if include_lyrics: statuses["Текст"] = "⏸️"
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))
            await store_response_message(event.chat_id, progress_message) # Store initial progress message

        if use_progress: statuses["Получение данных"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)

        entity_info = await get_entity_info(entity_id, entity_type_hint)

        if not entity_info:
            result_text = f"ℹ️ Не удалось найти информацию для ID: `{entity_id}` (Подсказка: {entity_type_hint or 'авто'})"
            if use_progress and progress_message:
                await progress_message.edit(result_text)
                final_info_message_object = progress_message # Progress message became the final message
            else:
                final_info_message_object = await event.reply(result_text)
            # Store if it wasn't the progress message already stored
            if final_info_message_object and (final_info_message_object != progress_message or not use_progress):
                await store_response_message(event.chat_id, final_info_message_object)
            return
        else: # Entity info found
            actual_entity_type = entity_info.get('_entity_type', 'unknown')
            if include_lyrics and actual_entity_type not in ['track', 'artist']:
                 if "Текст" in statuses: statuses["Текст"] = "➖ (Для треков/артистов)"
            if not include_lyrics and "Текст" in statuses:
                 del statuses["Текст"]

            if use_progress:
                 statuses["Получение данных"] = f"✅ ({actual_entity_type})"
                 statuses["Форматирование"] = "🔄 Подготовка..." if actual_entity_type != 'unknown' else "➖"
                 await update_progress(progress_message, statuses)

            response_text_parts = []
            thumbnail_url = None
            title_display, artists_display = "Неизвестно", "Неизвестно"
            video_id_for_lyrics_later = None
            lyrics_browse_id_from_main_entity = None

            thumbnails_data = entity_info.get('thumbnails')
            if not thumbnails_data and isinstance(entity_info.get('thumbnail'), list):
                thumbnails_data = entity_info.get('thumbnail')
            elif not thumbnails_data and isinstance(entity_info.get('thumbnail'), dict) and isinstance(entity_info['thumbnail'].get('thumbnails'), list):
                thumbnails_data = entity_info['thumbnail']['thumbnails']
            if isinstance(thumbnails_data, list) and thumbnails_data:
                try:
                    highest_res_thumb = sorted(thumbnails_data, key=lambda x: x.get('width', 0) * x.get('height', 0), reverse=True)[0]
                    thumbnail_url = highest_res_thumb.get('url')
                except (IndexError, KeyError, TypeError, AttributeError):
                    thumbnail_url = thumbnails_data[-1].get('url') if thumbnails_data else None
            if thumbnail_url: logger.debug(f"Selected thumbnail URL for {actual_entity_type} '{entity_id}': {thumbnail_url}")

            if actual_entity_type == 'track':
                details_to_use = entity_info
                title_display = details_to_use.get('title', 'Неизвестный трек')
                artists_data = details_to_use.get('artists') or details_to_use.get('author')
                artists_display = format_artists(artists_data)
                video_id_for_lyrics_later = details_to_use.get('videoId', entity_id)
                lyrics_browse_id_from_main_entity = details_to_use.get('lyricsBrowseId') or details_to_use.get('lyrics')
                response_text_parts.append(f"**Трек:** {title_display}")
                response_text_parts.append(f"**Исполнитель:** {artists_display}")
                album_data = details_to_use.get('album')
                if isinstance(album_data, dict) and album_data.get('name'):
                    album_link = f"https://music.youtube.com/browse/{album_data.get('id')}" if album_data.get('id') else None
                    response_text_parts.append(f"**Альбом:** {album_data['name']}" + (f" [Ссылка]({album_link})" if album_link else ""))
                elif isinstance(album_data, str): response_text_parts.append(f"**Альбом:** {album_data}")
                duration_s = None
                try: duration_s = int(details_to_use.get('lengthSeconds', 0))
                except (ValueError, TypeError): pass
                if duration_s is not None and duration_s > 0:
                    td = datetime.timedelta(seconds=duration_s); mins, secs = divmod(td.seconds, 60); hours, mins_rem = divmod(mins, 60)
                    duration_fmt = f"{hours:01}:{mins_rem:02}:{secs:02}" if hours > 0 else f"{mins:01}:{secs:02}"
                    response_text_parts.append(f"**Длительность:** {duration_fmt}")
                response_text_parts.append(f"**ID:** `{video_id_for_lyrics_later}`")
                if lyrics_browse_id_from_main_entity: response_text_parts.append(f"**Lyrics ID:** `{lyrics_browse_id_from_main_entity}`")
                response_text_parts.append(f"**Ссылка:** [YouTube Music](https://music.youtube.com/watch?v={video_id_for_lyrics_later})")

            elif actual_entity_type == 'album':
                title_display = entity_info.get('title', 'Неизвестный альбом')
                artists_display = format_artists(entity_info.get('artists'))
                response_text_parts.append(f"**Альбом:** {title_display}")
                response_text_parts.append(f"**Исполнитель:** {artists_display}")
                if entity_info.get('year'): response_text_parts.append(f"**Год:** {entity_info.get('year')}")
                track_count = entity_info.get('trackCount') or len(entity_info.get('tracks', []))
                if track_count: response_text_parts.append(f"**Треков:** {track_count}")
                album_id_for_link = entity_info.get('audioPlaylistId') or entity_id
                response_text_parts.append(f"**ID:** `{album_id_for_link}`")
                response_text_parts.append(f"**Ссылка:** [YouTube Music](https://music.youtube.com/browse/{album_id_for_link})")
                album_tracks = entity_info.get('tracks', [])
                if isinstance(album_tracks, dict) and 'results' in album_tracks: album_tracks = album_tracks['results']
                if album_tracks and isinstance(album_tracks, list):
                    response_text_parts.append(f"\n**Треки (первые {min(len(album_tracks), 5)}):**")
                    for t_info in album_tracks[:5]:
                        t_title = t_info.get('title', '?'); t_artists = format_artists(t_info.get('artists')) or artists_display; t_id = t_info.get('videoId')
                        t_link = f"[Ссылка](https://music.youtube.com/watch?v={t_id})" if t_id else ""
                        response_text_parts.append(f"• {t_title} ({t_artists}) {t_link}")

            elif actual_entity_type == 'playlist':
                title_display = entity_info.get('title', 'Неизвестный плейлист')
                author_data = entity_info.get('author'); artists_display = format_artists(author_data) if isinstance(author_data, (dict, list)) else (author_data or "Неизвестно")
                response_text_parts.append(f"**Плейлист:** {title_display}")
                response_text_parts.append(f"**Автор:** {artists_display}")
                track_count_pl = entity_info.get('trackCount') or len(entity_info.get('tracks', []))
                if track_count_pl: response_text_parts.append(f"**Треков:** {track_count_pl}")
                playlist_id_for_link = entity_id;  _ = playlist_id_for_link[2:] if playlist_id_for_link.startswith("VL") else playlist_id_for_link # Python 3.8+ walrus; use temp var for older
                playlist_id_for_link_display = playlist_id_for_link[2:] if playlist_id_for_link.startswith("VL") else playlist_id_for_link
                response_text_parts.append(f"**ID:** `{entity_id}` (Ссылка использует: `{playlist_id_for_link_display}`)")
                response_text_parts.append(f"**Ссылка:** [YouTube Music](https://music.youtube.com/playlist?list={playlist_id_for_link_display})")
                pl_tracks = entity_info.get('tracks', [])
                if pl_tracks:
                    response_text_parts.append(f"\n**Треки (первые {min(len(pl_tracks), 5)}):**")
                    for t_info in pl_tracks[:5]:
                        t_title = t_info.get('title', '?'); t_artists = format_artists(t_info.get('artists')) or artists_display; t_id = t_info.get('videoId')
                        t_link = f"[Ссылка](https://music.youtube.com/watch?v={t_id})" if t_id else ""
                        response_text_parts.append(f"• {t_title} ({t_artists}) {t_link}")

            elif actual_entity_type == 'artist':
                title_display = entity_info.get('name', 'Неизвестный исполнитель'); artists_display = title_display
                response_text_parts.append(f"**Исполнитель:** {title_display}")
                if entity_info.get('subscriberCountText'): response_text_parts.append(f"**Подписчики:** {entity_info['subscriberCountText']}")
                artist_id_for_link = entity_info.get('channelId', entity_id)
                response_text_parts.append(f"**ID:** `{artist_id_for_link}`")
                response_text_parts.append(f"**Ссылка:** [YouTube Music](https://music.youtube.com/channel/{artist_id_for_link})")
                artist_songs_data = entity_info.get("songs", {}); artist_songs_list = []
                if isinstance(artist_songs_data.get("results"), list): artist_songs_list = artist_songs_data["results"]
                if artist_songs_list:
                    special_track_info_for_artist = artist_songs_list[0]
                    stfia_title = special_track_info_for_artist.get('title', 'Топ трек'); stfia_artists = format_artists(special_track_info_for_artist.get('artists')) or title_display
                    stfia_id = special_track_info_for_artist.get('videoId'); stfia_link = f"[Ссылка](https://music.youtube.com/watch?v={stfia_id})" if stfia_id else ""
                    response_text_parts.append(f"\n**🎧 Пример популярного трека:**\n• {stfia_title} - {stfia_artists} {stfia_link}")
                    if include_lyrics and stfia_id:
                        video_id_for_lyrics_later = stfia_id
                        lyrics_browse_id_from_main_entity = special_track_info_for_artist.get('lyricsBrowseId') or special_track_info_for_artist.get('lyrics')
                songs_limit = config.get("artist_top_songs_limit", 5); albums_limit = config.get("artist_albums_limit", 3)
                if artist_songs_list and songs_limit > 0 :
                    response_text_parts.append(f"\n**Популярные треки (до {min(len(artist_songs_list), songs_limit)}):**")
                    for s_info in artist_songs_list[:songs_limit]:
                        s_title = s_info.get('title','?'); s_id = s_info.get('videoId'); s_link = f"[Ссылка](https://music.youtube.com/watch?v={s_id})" if s_id else ""
                        response_text_parts.append(f"• {s_title} {s_link}")
                artist_albums_data = entity_info.get("albums", {}); artist_albums_list = []
                if isinstance(artist_albums_data.get("albums"), list): artist_albums_list = artist_albums_data["albums"]
                elif isinstance(artist_albums_data.get("results"), list): artist_albums_list = artist_albums_data["results"]
                if artist_albums_list and albums_limit > 0:
                    response_text_parts.append(f"\n**Альбомы/Синглы (до {min(len(artist_albums_list), albums_limit)}):**")
                    for a_info in artist_albums_list[:albums_limit]:
                        a_title = a_info.get('title','?'); a_id = a_info.get('browseId'); a_link = f"[Ссылка](https://music.youtube.com/browse/{a_id})" if a_id else ""
                        a_year = a_info.get('year',''); a_type_str = a_info.get('type', '').replace('single', 'Сингл').replace('album', 'Альбом')
                        type_part = f" ({a_type_str})" if a_type_str else ""; response_text_parts.append(f"• {a_title}{type_part}" + (f" ({a_year})" if a_year else "") + f" {a_link}")
            else:
                response_text_parts.append(f"⚠️ Тип сущности '{actual_entity_type}' не полностью поддерживается для детального просмотра.")
                response_text_parts.append(f"ID: `{entity_id}`"); response_text_parts.append(f"Данные: ```json\n{json.dumps(entity_info, indent=2, ensure_ascii=False)[:1000]}\n...```")
                logger.warning(f"Unsupported entity type for 'see': {actual_entity_type}, ID: {entity_id}")
                if use_progress and progress_message : statuses["Форматирование"] = "⚠️ Неподдерживаемый тип"; await update_progress(progress_message, statuses)

            final_response_text = "\n".join(response_text_parts)
            if use_progress and progress_message: statuses["Форматирование"] = "✅ Готово"; await update_progress(progress_message, statuses)

            if include_cover and thumbnail_url:
                if use_progress and progress_message: statuses["Обложка"] = "🔄 Загрузка..."; await update_progress(progress_message, statuses)
                temp_thumb_file = await download_thumbnail(thumbnail_url)
                if temp_thumb_file:
                    files_to_clean_on_exit.append(temp_thumb_file)
                    if use_progress and progress_message: statuses["Обложка"] = "🔄 Обработка..."; await update_progress(progress_message, statuses)
                    processed_thumb_file = temp_thumb_file if actual_entity_type == 'artist' else await crop_thumbnail(temp_thumb_file)
                    if actual_entity_type == 'artist': logger.debug(f"Using original thumbnail for artist: {temp_thumb_file}")
                    if processed_thumb_file and processed_thumb_file != temp_thumb_file: files_to_clean_on_exit.append(processed_thumb_file)
                    elif not processed_thumb_file and actual_entity_type != 'artist': logger.warning(f"Cropping failed for {temp_thumb_file}, using original."); processed_thumb_file = temp_thumb_file
                    if use_progress and progress_message:
                        thumb_status_icon = "✅" if processed_thumb_file and os.path.exists(processed_thumb_file) else "⚠️"
                        statuses["Обложка"] = f"{thumb_status_icon} Готово к отправке"; await update_progress(progress_message, statuses)
                    if processed_thumb_file and os.path.exists(processed_thumb_file):
                        try:
                            final_info_message_object = await client.send_file(event.chat_id, file=processed_thumb_file, caption=final_response_text, link_preview=False, reply_to=event.message.id)
                            if progress_message:
                                try: await progress_message.delete(); progress_message = None
                                except Exception: pass
                        except Exception as send_e:
                            logger.error(f"Failed to send file with cover {os.path.basename(processed_thumb_file)}: {send_e}", exc_info=True)
                            if use_progress and progress_message and "Обложка" in statuses: statuses["Обложка"] = "❌ Ошибка отправки"; await update_progress(progress_message, statuses)
                            final_response_text_fallback = f"{final_response_text}\n\n_(Ошибка при отправке обложки)_"
                            final_info_message_object = await (progress_message.edit(final_response_text_fallback, link_preview=False) if progress_message else event.reply(final_response_text_fallback, link_preview=False))
                    else:
                        logger.warning(f"Thumbnail processing failed or file not found for {entity_id}. Sending text only.")
                        if use_progress and progress_message and "Обложка" in statuses: statuses["Обложка"] = "❌ Ошибка обработки"; await update_progress(progress_message, statuses)
                        final_response_text_fallback = f"{final_response_text}\n\n_(Ошибка при обработке обложки)_"
                        final_info_message_object = await (progress_message.edit(final_response_text_fallback, link_preview=False) if progress_message else event.reply(final_response_text_fallback, link_preview=False))
                else:
                     logger.warning(f"Thumbnail download failed for {entity_id}. Sending text only.")
                     if use_progress and progress_message and "Обложка" in statuses: statuses["Обложка"] = "❌ Ошибка загрузки"; await update_progress(progress_message, statuses)
                     final_response_text_fallback = f"{final_response_text}\n\n_(Ошибка при загрузке обложки)_"
                     final_info_message_object = await (progress_message.edit(final_response_text_fallback, link_preview=False) if progress_message else event.reply(final_response_text_fallback, link_preview=False))
            else:
                 final_info_message_object = await (progress_message.edit(final_response_text, link_preview=False) if progress_message else event.reply(final_response_text, link_preview=False))
            if final_info_message_object: await store_response_message(event.chat_id, final_info_message_object)

            if include_lyrics and video_id_for_lyrics_later:
                if use_progress and progress_message: statuses["Текст"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)
                lyrics_data = await get_lyrics_for_track(video_id_for_lyrics_later, lyrics_browse_id_from_main_entity)
                if lyrics_data and lyrics_data.get('lyrics'):
                    if use_progress and progress_message: statuses["Текст"] = "✅ Отправка..."; await update_progress(progress_message, statuses)
                    lyrics_text_content = lyrics_data['lyrics']; lyrics_source_content = lyrics_data.get('source')
                    lyrics_header_text = f"📜 **Текст песни:** {title_display} - {artists_display}" + (f"\n_(Источник: {lyrics_source_content})_" if lyrics_source_content else "")
                    if progress_message:
                        try: await progress_message.delete(); progress_message = None
                        except Exception: pass
                    await send_lyrics(event, lyrics_text_content, lyrics_header_text, title_display, video_id_for_lyrics_later)
                    lyrics_message_handled_storage = True
                else:
                    logger.info(f"Текст не найден для '{title_display}' ({video_id_for_lyrics_later}).")
                    no_lyrics_text_reply = f"_Текст для '{title_display}' не найден._"
                    if use_progress and progress_message: statuses["Текст"] = "ℹ️ Не найден"; await update_progress(progress_message, statuses)
                    reply_to_msg_id_lyrics = final_info_message_object.id if final_info_message_object else event.message.id
                    no_lyrics_msg_obj_sent = await event.respond(no_lyrics_text_reply, reply_to=reply_to_msg_id_lyrics)
                    await store_response_message(event.chat_id, no_lyrics_msg_obj_sent)
            elif include_lyrics and not video_id_for_lyrics_later:
                if use_progress and progress_message and "Текст" in statuses: statuses["Текст"] = "⚠️ Не удалось определить трек"; await update_progress(progress_message, statuses)

    except Exception as e:
        logger.error(f"Unexpected error in handle_see for ID '{entity_id}': {e}", exc_info=True)
        error_prefix = "⚠️" if isinstance(e, (ValueError, FileNotFoundError, TypeError)) else "❌"
        error_text = f"{error_prefix} Ошибка при получении информации '{entity_id}':\n`{type(e).__name__}: {str(e)[:150]}`"
        current_progress_text = getattr(progress_message, 'text', '') if use_progress and progress_message else ""
        if use_progress and progress_message:
             for task_key_err in statuses: statuses[task_key_err] = str(statuses[task_key_err]).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌").replace("⏸️", "❌")
             try: await update_progress(progress_message, statuses)
             except Exception: pass
             try:
                 await progress_message.edit(f"{current_progress_text}\n\n{error_text}")
                 final_info_message_object = progress_message
             except Exception as edit_e:
                 logger.error(f"Failed to edit progress message with error for {entity_id}: {edit_e}")
                 final_info_message_object = await event.reply(error_text)
        else: final_info_message_object = await event.reply(error_text)
        if final_info_message_object and (final_info_message_object != progress_message or not use_progress):
            await store_response_message(event.chat_id, final_info_message_object)

    finally:
        if files_to_clean_on_exit:
            logger.debug(f"Scheduling cleanup for handle_see (Files: {len(files_to_clean_on_exit)})")
            asyncio.create_task(cleanup_files(*files_to_clean_on_exit))
        if use_progress and progress_message and \
           progress_message != final_info_message_object and \
           not lyrics_message_handled_storage:
            await asyncio.sleep(2)
            try:
                await progress_message.delete()
                logger.debug(f"Progress message {getattr(progress_message, 'id', 'N/A')} for 'see' cleaned up in finally block.")
            except Exception: pass


# -------------------------
# Helper: Send Single Track
# -------------------------
async def send_single_track(event: events.NewMessage.Event, info: Dict, file_path: str):
    """
    Handles sending a single downloaded audio file via Telegram.
    Updates last.csv.
    """
    temp_telegram_thumb, processed_telegram_thumb = None, None
    files_to_clean_after_send = [file_path] # Initially, only the audio file itself
    title, performer, duration_sec = "Неизвестно", "Неизвестно", 0
    sent_audio_msg = None
    video_id_for_last = "N/A" # For last.csv

    try:
        if not info or not file_path or not os.path.exists(file_path):
             logger.error(f"send_single_track called with invalid info or missing file: Path='{file_path}'")
             error_msg = await event.reply(f"❌ Ошибка: Не найден скачанный файл `{os.path.basename(file_path or 'N/A')}`.")
             await store_response_message(event.chat_id, error_msg)
             # Don't clean file_path here as it might be an error in logic, not the file itself being temporary.
             # Cleanup for non-existent files is moot. If it exists but info is bad, it's a logic error.
             return None # Cannot proceed

        title, performer, duration_sec = extract_track_metadata(info)
        video_id_for_last = info.get('id') or info.get('videoId') or 'N/A' # From yt-dlp 'id' or API 'videoId'

        # --- Thumbnail for Telegram Audio ---
        thumb_url = None
        # yt-dlp info['thumbnails'] is a list of dicts with 'url', 'width', 'height'
        thumbnails_list_from_info = info.get('thumbnails')
        # Fallback for structures where 'thumbnail' contains the list (like from ytmusicapi direct objects)
        if not thumbnails_list_from_info and isinstance(info.get('thumbnail'), list):
            thumbnails_list_from_info = info.get('thumbnail')
        elif not thumbnails_list_from_info and isinstance(info.get('thumbnail'), dict): # e.g. from get_song where thumbnail is a dict itself
            thumbnails_list_from_info = info['thumbnail'].get('thumbnails')


        if isinstance(thumbnails_list_from_info, list) and thumbnails_list_from_info:
            try: # Sort by area to get largest, prefer webp or jpg
                def sort_key_thumb(t):
                    pref = 0
                    if 'url' in t and t['url'].endswith('.webp'): pref = 2
                    elif 'url' in t and t['url'].endswith('.jpg'): pref = 1
                    return (t.get('width', 0) * t.get('height', 0), pref)

                best_thumb_info = sorted(thumbnails_list_from_info, key=sort_key_thumb, reverse=True)[0]
                thumb_url = best_thumb_info.get('url')
            except (IndexError, KeyError, TypeError, AttributeError):
                if thumbnails_list_from_info: # Fallback to just the last one if sorting or access fails
                     thumb_url = thumbnails_list_from_info[-1].get('url')
        if thumb_url: logger.debug(f"Selected thumbnail URL for Telegram audio preview ('{title}'): {thumb_url}")


        if thumb_url:
            logger.debug(f"Attempting download/process thumbnail for Telegram audio preview ('{title}')")
            temp_telegram_thumb = await download_thumbnail(thumb_url) # Downloads to SCRIPT_DIR
            if temp_telegram_thumb:
                files_to_clean_after_send.append(temp_telegram_thumb) # Add downloaded thumb for cleanup
                processed_telegram_thumb = await crop_thumbnail(temp_telegram_thumb) # Crops and saves with _cropped suffix
                if processed_telegram_thumb and processed_telegram_thumb != temp_telegram_thumb:
                    files_to_clean_after_send.append(processed_telegram_thumb) # Add cropped thumb for cleanup
                elif not processed_telegram_thumb: # Crop failed
                     logger.warning(f"crop_thumbnail returned None for {temp_telegram_thumb}. Will send without specific Telegram thumbnail, relying on file's embedded or none.")
                     processed_telegram_thumb = None # Ensure it's None
            else:
                 logger.warning(f"Failed to download thumbnail for track '{title}'. Sending without specific Telegram thumbnail.")
        else:
             logger.info(f"No suitable thumbnail URL found in metadata for track '{title}'. Sending without specific Telegram thumbnail.")


        logger.info(f"Отправка аудио: {os.path.basename(file_path)} (Title: '{title}', Performer: '{performer}', Duration: {duration_sec}s)")
        # Use processed_telegram_thumb if it exists and is a valid file
        final_thumb_for_telegram = processed_telegram_thumb if (processed_telegram_thumb and os.path.exists(processed_telegram_thumb)) else None

        sent_audio_msg = await client.send_file(
            event.chat_id,
            file=file_path,
            caption=BOT_CREDIT, # Bot credit from config
            attributes=[types.DocumentAttributeAudio(
                duration=duration_sec, title=title, performer=performer
            )],
            thumb=final_thumb_for_telegram, # Path to cropped thumbnail or None
            reply_to=event.message.id,
            # allow_cache=False # Consider if files are unique and shouldn't be cached by TG server for reuse by file_id
        )
        logger.info(f"Аудио успешно отправлено: {os.path.basename(file_path)} (Msg ID: {sent_audio_msg.id})")
        await store_response_message(event.chat_id, sent_audio_msg) # Store the sent audio message

        # --- Update last.csv ---
        if config.get("recent_downloads", True):
             try:
                last_tracks_list = load_last_tracks()
                timestamp_str = datetime.datetime.now().strftime("%H:%M-%d-%m") # H:M-D-M format
                track_url_for_last = f"https://music.youtube.com/watch?v={video_id_for_last}" if video_id_for_last != 'N/A' else 'N/A'

                # New entry: Track Title, Artists, Video ID, Track URL, Duration Seconds, Timestamp
                new_entry_last = [
                    title,
                    performer,
                    video_id_for_last,
                    track_url_for_last,
                    str(duration_sec),
                    timestamp_str
                ]
                last_tracks_list.insert(0, new_entry_last) # Add to beginning
                save_last_tracks(last_tracks_list) # Saves top 5
             except Exception as e_last_csv:
                 logger.error(f"Не удалось обновить список последних треков ({title}): {e_last_csv}", exc_info=True)

        return sent_audio_msg # Return the Telegram message object

    except telethon_errors.MediaCaptionTooLongError:
         logger.error(f"Ошибка отправки {os.path.basename(file_path)}: подпись (BOT_CREDIT) слишком длинная.")
         error_msg_caption = await event.reply(f"⚠️ Не удалось отправить `{title}`: подпись слишком длинная. Проверьте BOT_CREDIT в конфиге.")
         await store_response_message(event.chat_id, error_msg_caption)
         return None
    except telethon_errors.WebpageMediaEmptyError: # Sometimes happens with problematic thumbs
          logger.error(f"Ошибка отправки {os.path.basename(file_path)}: WebpageMediaEmptyError. Попытка без явного превью...")
          try:
              sent_audio_msg_no_thumb = await client.send_file(
                  event.chat_id, file_path, caption=BOT_CREDIT,
                  attributes=[types.DocumentAttributeAudio(duration=duration_sec, title=title, performer=performer)],
                  thumb=None, # Explicitly no thumbnail
                  reply_to=event.message.id
              )
              logger.info(f"Повторная отправка без явного превью успешна: {os.path.basename(file_path)}")
              await store_response_message(event.chat_id, sent_audio_msg_no_thumb) # Store retry message
              # Update last.csv for this successful send too (if not already done, but it should be if this path is reached)
              # The original call to update last.csv would have happened if we get here.
              return sent_audio_msg_no_thumb
          except Exception as retry_e:
              logger.error(f"Повторная отправка {os.path.basename(file_path)} без превью не удалась: {retry_e}", exc_info=True)
              error_msg_retry = await event.reply(f"❌ Не удалось отправить `{title}` даже без превью: {str(retry_e)[:100]}")
              await store_response_message(event.chat_id, error_msg_retry)
              return None
    except Exception as e_send_track:
        logger.error(f"Неожиданная ошибка при отправке трека {os.path.basename(file_path or 'N/A')}: {e_send_track}", exc_info=True)
        try:
             error_msg_send = await event.reply(f"❌ Не удалось отправить трек `{title}`: {str(e_send_track)[:100]}")
             await store_response_message(event.chat_id, error_msg_send)
        except Exception as notify_e: logger.error(f"Не удалось уведомить об ошибке отправки трека: {notify_e}")
        return None

    finally:
        # Cleanup temporary files (original downloaded thumb, cropped thumb)
        # The audio file (file_path) itself is kept if it's opus, or deleted if it's m4a (handled by yt-dlp usually if 'keepvideo': False or audio extraction)
        # However, our script currently downloads and then sends, so we might want to clean up the audio file too,
        # unless it's explicitly set to be kept (e.g., in a specific "downloads" folder via outtmpl).
        # For now, this cleanup focuses on thumbs. Audio file cleanup depends on YDL_OPTS.
        extensions_to_keep_audio = ['.opus'] # Example: keep opus files
        keep_this_audio_file = False
        final_audio_file_to_clean = file_path # Default to cleaning the audio file

        if file_path and os.path.exists(file_path):
            try:
                _, file_extension = os.path.splitext(file_path)
                if file_extension.lower() in extensions_to_keep_audio:
                    keep_this_audio_file = True
                    logger.info(f"Аудиофайл '{os.path.basename(file_path)}' будет сохранен (расширение в списке keep).")
                    final_audio_file_to_clean = None # Don't add to cleanup list
            except Exception as e_ext_check: logger.warning(f"Не удалось проверить расширение для сохранения аудиофайла {file_path}: {e_ext_check}")

        # Add only the audio file that needs cleaning to the list
        if final_audio_file_to_clean:
            # files_to_clean_after_send already contains thumbs. Add audio if needed.
            if final_audio_file_to_clean not in files_to_clean_after_send: # Avoid duplicates
                 files_to_clean_after_send.append(final_audio_file_to_clean)
        else: # If audio file is to be kept, remove it from the list if it was added initially
            if file_path in files_to_clean_after_send:
                files_to_clean_after_send.remove(file_path)


        if files_to_clean_after_send:
            # Filter out None values just in case
            valid_files_to_clean = [f for f in files_to_clean_after_send if f and isinstance(f, str)]
            if valid_files_to_clean:
                logger.debug(f"Scheduling file cleanup for send_single_track (Files: {len(valid_files_to_clean)}: {valid_files_to_clean})")
                asyncio.create_task(cleanup_files(*valid_files_to_clean))
        else:
             logger.debug(f"send_single_track: No temporary files to clean up.")

# -------------------------
# Command: download (-t, -a, -s) [-txt] / dl
# -------------------------
async def handle_download(event: 'events.NewMessage.Event', args: List[str]):
    """Handles the download command. Supports -t (track), -a (album/playlist), -s (search then download track)."""
    valid_flags = {"-t", "-a", "-s"} # -s for search and download
    prefix = config.get("prefix", ",")

    if not args:
        usage = (f"**Использование:** `{prefix}dl <флаг> <аргумент> [-txt]`\n"
                 f"Флаги и аргументы:\n"
                 f"  `-t <ссылка на трек>` - скачать трек.\n"
                 f"  `-a <ссылка на альбом/плейлист>` - скачать альбом/плейлист.\n"
                 f"  `-s <поисковый запрос>` - найти и скачать первый трек.\n"
                 f"Опциональный флаг: `-txt` (для `-t` или `-s`, включить текст песни).")
        await store_response_message(event.chat_id, await event.reply(usage))
        return

    download_type_flag = None # -t, -a, or -s
    include_lyrics = False
    target_arg = None # Link or search query
    remaining_args = list(args) # Work with a copy

    # Parse -txt first as it can appear anywhere
    if "-txt" in remaining_args:
        include_lyrics = True
        remaining_args.remove("-txt")
        logger.info("Lyrics inclusion requested for download.")

    # Parse main download type flag (-t, -a, -s)
    if remaining_args:
        potential_flag = remaining_args[0].lower()
        if potential_flag in valid_flags:
            download_type_flag = potential_flag
            remaining_args.pop(0) # Remove the flag
        else: # No valid flag found at the start
            await store_response_message(event.chat_id, await event.reply(f"⚠️ Не указан корректный флаг операции (`-t`, `-a` или `-s`).\n{usage}"))
            return
    else: # No arguments left after -txt or no args at all
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не указана ссылка или поисковый запрос для флага `{download_type_flag}`."))
        return

    # The rest of remaining_args is the target (link or search query)
    if remaining_args:
        target_arg = " ".join(remaining_args) # Join if query has spaces
    else:
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не указана ссылка или поисковый запрос для флага `{download_type_flag}`."))
        return

    # Validate specific conditions
    if download_type_flag in ["-t", "-a"] and (not target_arg or not target_arg.startswith("http")):
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Для флага `{download_type_flag}` ожидается http(s) ссылка."))
        return
    if download_type_flag == "-s" and not target_arg:
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Для флага `-s` не указан поисковый запрос."))
        return

    if include_lyrics and download_type_flag == "-a":
         logger.warning("-txt flag is ignored for album/playlist downloads (-a).")
         include_lyrics = False # Lyrics only for single tracks (-t or -s)

    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    # final_sent_message is not consistently used here as sending happens in helpers or per track

    loop = asyncio.get_running_loop() # Get loop once

    try:
        if download_type_flag == "-s": # Search and download
            search_query = target_arg
            if use_progress:
                statuses = {"Поиск трека": f"⏳ '{search_query[:30]}...'", "Скачивание/Обработка": "⏸️", "Отправка Аудио": "⏸️"}
                if include_lyrics: statuses["Отправка Текста"] = "⏸️"
                progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))
                await store_response_message(event.chat_id, progress_message)

            logger.info(f"Search and download requested for query: '{search_query}'")
            # Search for songs first, then videos if no songs found
            search_results_s = await _api_search(search_query, filter_type="songs", limit=1)
            found_item = None
            if search_results_s and search_results_s[0].get('videoId'):
                found_item = search_results_s[0]
                logger.info(f"Found song match for '{search_query}': {found_item.get('title')}")
                if use_progress: statuses["Поиск трека"] = f"✅ Трек: {found_item.get('title', 'Без названия')[:30]}..."
            else:
                if use_progress: statuses["Поиск трека"] = f"ℹ️ Песня не найдена, ищем видео '{search_query[:20]}...'"; await update_progress(progress_message, statuses)
                search_results_v = await _api_search(search_query, filter_type="videos", limit=1)
                if search_results_v and search_results_v[0].get('videoId'):
                    found_item = search_results_v[0]
                    logger.info(f"Found video match for '{search_query}': {found_item.get('title')}")
                    if use_progress: statuses["Поиск трека"] = f"✅ Видео: {found_item.get('title', 'Без названия')[:30]}..."
                else:
                    logger.warning(f"No track or video found for search query: '{search_query}'")
                    if use_progress: statuses["Поиск трека"] = f"❌ Не найдено: '{search_query[:30]}...'"
                    await update_progress(progress_message, statuses)
                    error_msg_search = await event.reply(f"❌ Не удалось найти трек или видео по запросу: `{search_query}`")
                    await store_response_message(event.chat_id, error_msg_search)
                    return # Exit if nothing found

            await update_progress(progress_message, statuses) # Update after search result

            video_id_to_dl = found_item.get('videoId')
            track_title_from_search = found_item.get('title', 'Неизвестный трек')
            download_link_from_search = f"https://music.youtube.com/watch?v={video_id_to_dl}"

            # Now, proceed like -t download
            if use_progress: statuses["Скачивание/Обработка"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)
            info_s, file_path_s = await loop.run_in_executor(None, functools.partial(download_track, download_link_from_search))

            if not file_path_s or not info_s:
                fail_reason_s = "yt-dlp не смог скачать/обработать"
                if info_s and not file_path_s: fail_reason_s = "yt-dlp скачал, но файл не найден"
                elif not info_s: fail_reason_s = "yt-dlp не вернул информацию"
                logger.error(f"Download failed for searched track {track_title_from_search} ({download_link_from_search}). Reason: {fail_reason_s}")
                if use_progress:
                    statuses["Скачивание/Обработка"] = f"❌ Ошибка ({fail_reason_s[:20]}...)"
                    statuses["Отправка Аудио"] = "❌"; statuses["Отправка Текста"] = "❌" # Ensure status is set
                    await update_progress(progress_message, statuses)
                error_msg_dl_s = await event.reply(f"❌ Не удалось скачать или обработать найденный трек '{track_title_from_search}':\n`{download_link_from_search}`\n_{fail_reason_s}_")
                await store_response_message(event.chat_id, error_msg_dl_s)
            else: # Download successful
                file_basename_s = os.path.basename(file_path_s)
                actual_title_s = info_s.get('title', file_basename_s)
                logger.info(f"Track from search download successful: {file_basename_s}")
                if use_progress:
                    display_title_s = (actual_title_s[:30] + '...') if len(actual_title_s) > 33 else actual_title_s
                    statuses["Скачивание/Обработка"] = f"✅ ({display_title_s})"
                    statuses["Отправка Аудио"] = "🔄 Подготовка..."; await update_progress(progress_message, statuses)

                sent_audio_msg_s = await send_single_track(event, info_s, file_path_s)
                if sent_audio_msg_s:
                    if use_progress: statuses["Отправка Аудио"] = "✅ Готово"; await update_progress(progress_message, statuses)
                    if include_lyrics: # Handle lyrics for -s
                        if use_progress: statuses["Отправка Текста"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)
                        lyrics_browse_id_s = info_s.get('lyricsBrowseId') or info_s.get('lyrics')
                        lyrics_data_s = await get_lyrics_for_track(video_id_to_dl, lyrics_browse_id_s)
                        if lyrics_data_s and lyrics_data_s.get('lyrics'):
                            if use_progress: statuses["Отправка Текста"] = "✅ Отправка..."; await update_progress(progress_message, statuses)
                            artists_s = format_artists(info_s.get('artists') or info_s.get('artist') or info_s.get('uploader') or info_s.get('creator'))
                            lyrics_header_s = f"📜 **Текст песни:** {actual_title_s} - {artists_s}"
                            if lyrics_data_s.get('source'): lyrics_header_s += f"\n_(Источник: {lyrics_data_s['source']})_"
                            await send_lyrics(event, lyrics_data_s['lyrics'], lyrics_header_s, actual_title_s, video_id_to_dl)
                            if use_progress: statuses["Отправка Текста"] = "✅ Отправлено"
                            # If lyrics sent (especially as HTML file), progress_message might have been deleted by send_lyrics
                            # We check below and handle deletion if it wasn't.
                        else:
                            if use_progress: statuses["Отправка Текста"] = "ℹ️ Не найден"
                            no_lyrics_msg_s = await event.respond(f"_Текст для '{actual_title_s}' не найден._", reply_to=sent_audio_msg_s.id)
                            await store_response_message(event.chat_id, no_lyrics_msg_s)
                            await asyncio.sleep(7)
                            try:
                                await no_lyrics_msg_s.delete()
                            except Exception: # Catch any error during deletion
                                pass
                        await update_progress(progress_message, statuses) # Final update for lyrics status
            # Explicitly delete progress_message after all single-track operations (audio + optional lyrics)
            if progress_message: # Check if the progress message object is still valid
                await asyncio.sleep(5) # Give user a moment to see final status
                try:
                    await progress_message.delete()
                    progress_message = None # Mark as None after successful deletion
                except Exception as e_del_prog:
                    logger.debug(f"Failed to delete progress message {getattr(progress_message, 'id', 'N/A')} for -s command: {e_del_prog}")
                    pass # Ignore if deletion fails (e.g., already deleted by user/Telegram)


        elif download_type_flag == "-t": # Download single track by link
            track_link = target_arg
            if use_progress:
                statuses = {"Скачивание/Обработка": "⏳ Ожидание...", "Отправка Аудио": "⏸️"}
                if include_lyrics: statuses["Отправка Текста"] = "⏸️"
                progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))
                await store_response_message(event.chat_id, progress_message)

            if use_progress: statuses["Скачивание/Обработка"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)
            info_t, file_path_t = await loop.run_in_executor(None, functools.partial(download_track, track_link))

            if not file_path_t or not info_t:
                fail_reason_t = "yt-dlp не смог скачать/обработать"
                if info_t and not file_path_t: fail_reason_t = "yt-dlp скачал, но файл не найден"
                elif not info_t: fail_reason_t = "yt-dlp не вернул информацию"
                logger.error(f"Download failed for {track_link}. Reason: {fail_reason_t}")
                if use_progress:
                    statuses["Скачивание/Обработка"] = f"❌ Ошибка ({fail_reason_t[:20]}...)"
                    statuses["Отправка Аудио"] = "❌"
                    if include_lyrics: statuses["Отправка Текста"] = "❌"
                    await update_progress(progress_message, statuses)
                error_msg_dl_t = await event.reply(f"❌ Не удалось скачать или обработать трек:\n`{track_link}`\n_{fail_reason_t}_")
                await store_response_message(event.chat_id, error_msg_dl_t)
            else: # Download successful
                 file_basename_t = os.path.basename(file_path_t)
                 track_title_t = info_t.get('title', file_basename_t)
                 logger.info(f"Track download successful: {file_basename_t}")
                 if use_progress:
                      display_title_t = (track_title_t[:30] + '...') if len(track_title_t) > 33 else track_title_t
                      statuses["Скачивание/Обработка"] = f"✅ ({display_title_t})"
                      statuses["Отправка Аудио"] = "🔄 Подготовка..."; await update_progress(progress_message, statuses)

                 sent_audio_msg_t = await send_single_track(event, info_t, file_path_t)
                 if sent_audio_msg_t:
                     if use_progress: statuses["Отправка Аудио"] = "✅ Готово"; await update_progress(progress_message, statuses)
                     if include_lyrics:
                         if use_progress: statuses["Отправка Текста"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)
                         video_id_t = info_t.get('id') or info_t.get('videoId')
                         lyrics_browse_id_t = info_t.get('lyricsBrowseId') or info_t.get('lyrics')

                         if video_id_t:
                             lyrics_data_t = await get_lyrics_for_track(video_id_t, lyrics_browse_id_t)
                             if lyrics_data_t and lyrics_data_t.get('lyrics'):
                                  if use_progress: statuses["Отправка Текста"] = "✅ Отправка..."; await update_progress(progress_message, statuses)
                                  artists_t = format_artists(info_t.get('artists') or info_t.get('artist') or info_t.get('uploader') or info_t.get('creator'))
                                  lyrics_header_t = f"📜 **Текст песни:** {track_title_t} - {artists_t}"
                                  if lyrics_data_t.get('source'): lyrics_header_t += f"\n_(Источник: {lyrics_data_t['source']})_"
                                  await send_lyrics(event, lyrics_data_t['lyrics'], lyrics_header_t, track_title_t, video_id_t)
                                  if use_progress: statuses["Отправка Текста"] = "✅ Отправлено"
                                  # Progress message might have been deleted by send_lyrics
                             else: # Lyrics not found
                                  logger.info(f"Текст не найден для '{track_title_t}' ({video_id_t}) при скачивании.")
                                  if use_progress: statuses["Отправка Текста"] = "ℹ️ Не найден"
                                  no_lyrics_msg_t = await event.respond(f"_Текст для '{track_title_t}' не найден._", reply_to=sent_audio_msg_t.id)
                                  await store_response_message(event.chat_id, no_lyrics_msg_t)
                                  await asyncio.sleep(7)
                                  try:
                                      await no_lyrics_msg_t.delete()
                                  except Exception: # Catch any error during deletion
                                      pass
                             await update_progress(progress_message, statuses) # Final update for lyrics status
                         else: # No video ID from info_t
                              logger.warning(f"Cannot fetch lyrics for downloaded track '{track_title_t}': No video ID available in yt-dlp info.")
                              if use_progress: statuses["Отправка Текста"] = "⚠️ Нет Video ID"; await update_progress(progress_message, statuses)
            # Explicitly delete progress_message after all single-track operations (audio + optional lyrics)
            if progress_message: # Check if the progress message object is still valid
                await asyncio.sleep(5) # Give user a moment to see final status
                try:
                    await progress_message.delete()
                    progress_message = None # Mark as None after successful deletion
                except Exception as e_del_prog:
                    logger.debug(f"Failed to delete progress message {getattr(progress_message, 'id', 'N/A')} for -t command: {e_del_prog}")
                    pass # Ignore if deletion fails (e.g., already deleted by user/Telegram)


        elif download_type_flag == "-a": # Download album/playlist
            album_playlist_link = target_arg
            album_or_playlist_id = extract_entity_id(album_playlist_link)
            if not album_or_playlist_id:
                 error_msg_id = await event.reply(f"⚠️ Не удалось извлечь ID из ссылки для альбома/плейлиста: `{album_playlist_link}`")
                 await store_response_message(event.chat_id, error_msg_id)
                 return

            album_title_display = album_or_playlist_id # Placeholder
            total_tracks_album, downloaded_count_album, sent_count_album = 0, 0, 0
            progress_callback_album = None

            if use_progress:
                # Define async callback for album progress updates
                async def album_progress_updater_local(status_key, **kwargs_album):
                    nonlocal total_tracks_album, downloaded_count_album, sent_count_album, album_title_display
                    if not use_progress or not progress_message: return
                    current_statuses_album = statuses

                    try:
                        if status_key == "analysis_complete":
                            total_tracks_album = kwargs_album.get('total_tracks', 0)
                            temp_title_album = kwargs_album.get('title', album_or_playlist_id)
                            album_title_display = (temp_title_album[:40] + '...') if len(temp_title_album) > 43 else temp_title_album
                            current_statuses_album["Альбом/Плейлист"] = f"'{album_title_display}' ({total_tracks_album} тр.)"
                            current_statuses_album["Прогресс Скачивания"] = f"▶️ Начинаем... (0/{total_tracks_album})"
                        elif status_key == "track_downloading":
                            curr_num_dl = kwargs_album.get('current', 1)
                            perc_dl = kwargs_album.get('percentage', 0)
                            title_dl = kwargs_album.get('title', '?')
                            current_statuses_album["Прогресс Скачивания"] = f"📥 {curr_num_dl}/{total_tracks_album} ({perc_dl}%) - '{title_dl}'"
                        elif status_key == "track_downloaded":
                            curr_ok_dl = kwargs_album.get('current', downloaded_count_album)
                            perc_ok_dl = int((curr_ok_dl / total_tracks_album) * 100) if total_tracks_album else 0
                            current_statuses_album["Прогресс Скачивания"] = f"✅ Скачано {curr_ok_dl}/{total_tracks_album} ({perc_ok_dl}%)"
                            if curr_ok_dl > 0: current_statuses_album["Отправка Треков"] = f"📤 Подготовка {curr_ok_dl} треков..."
                        elif status_key == "track_sending":
                            curr_send_idx = kwargs_album.get('current_index', sent_count_album)
                            total_to_send = kwargs_album.get('total_downloaded', downloaded_count_album)
                            title_send = kwargs_album.get('title', '?')
                            current_statuses_album["Отправка Треков"] = f"📤 Отправка {curr_send_idx+1}/{total_to_send} - '{title_send}'"
                        elif status_key == "track_sent":
                             curr_sent_ok = kwargs_album.get('current_sent', sent_count_album)
                             total_dl_for_send = kwargs_album.get('total_downloaded', downloaded_count_album)
                             current_statuses_album["Отправка Треков"] = f"✔️ Отправлен {curr_sent_ok}/{total_dl_for_send}"
                        elif status_key == "track_failed":
                            curr_num_fail = kwargs_album.get('current', downloaded_count_album + 1)
                            title_fail = kwargs_album.get('title', '?')
                            reason_fail = kwargs_album.get('reason', 'Ошибка')
                            if "Прогресс Скачивания" in current_statuses_album and "📥" in current_statuses_album["Прогресс Скачивания"]:
                                 current_statuses_album["Прогресс Скачивания"] = f"⚠️ '{title_fail}' - {reason_fail} (трек {curr_num_fail})"
                            elif "Отправка Треков" in current_statuses_album and "📤" in current_statuses_album["Отправка Треков"]:
                                 current_statuses_album["Отправка Треков"] = f"❌ Не отправлен '{title_fail}' ({reason_fail})"
                            else:
                                 current_statuses_album["Прогресс Скачивания"] = f"❌ Ошибка '{title_fail}' ({reason_fail})"
                        await update_progress(progress_message, current_statuses_album)
                    except Exception as e_prog_album:
                        logger.error(f"Ошибка при обновлении прогресса альбома: {e_prog_album}", exc_info=True)

                progress_callback_album = album_progress_updater_local
                statuses = {"Альбом/Плейлист": f"🔄 Анализ ID '{album_or_playlist_id[:30]}...'...", "Прогресс Скачивания": "⏸️", "Отправка Треков": "⏸️"}
                progress_message = await event.reply("\n".join(f"{task}: {value}" for task, value in statuses.items()))
                await store_response_message(event.chat_id, progress_message)

            logger.info(f"Starting sequential download for album/playlist: {album_or_playlist_id} (Link: {album_playlist_link})")
            downloaded_tuples_album = await download_album_tracks(album_or_playlist_id, progress_callback_album)
            downloaded_count_album = len(downloaded_tuples_album)

            if use_progress and progress_message:
                 dl_status_icon = "✅" if downloaded_count_album > 0 else ("ℹ️" if total_tracks_album > 0 else "❌")
                 statuses["Прогресс Скачивания"] = f"{dl_status_icon} Скачано {downloaded_count_album}/{total_tracks_album or '?'}"
                 if downloaded_count_album == 0: statuses["Отправка Треков"] = "➖ (Нет треков для отправки)"
                 else: statuses["Отправка Треков"] = f"📤 Ожидание отправки {downloaded_count_album} треков..."
                 await update_progress(progress_message, statuses)
                 await asyncio.sleep(1)

            if downloaded_count_album == 0:
                if progress_callback_album: await progress_callback_album("album_error", error="Треки не скачаны или ошибка анализа")
                error_msg_no_dl = await event.reply(f"❌ Не удалось скачать ни одного трека для `{album_title_display or album_or_playlist_id}`.")
                await store_response_message(event.chat_id, error_msg_no_dl)
                return

            logger.info(f"Starting sequential sending of {downloaded_count_album} tracks for '{album_title_display or album_or_playlist_id}'...")
            for i_send, (info_album_track, file_path_album_track) in enumerate(downloaded_tuples_album):
                track_title_to_send = (info_album_track.get('title', os.path.basename(file_path_album_track)) if info_album_track else os.path.basename(file_path_album_track))
                short_title_send = (track_title_to_send[:25] + '...') if len(track_title_to_send) > 28 else track_title_to_send

                if not file_path_album_track or not os.path.exists(file_path_album_track):
                     logger.error(f"Файл для трека {i_send+1}/{downloaded_count_album} ('{short_title_send}') не найден. Пропуск отправки.")
                     if progress_callback_album: await progress_callback_album("track_failed", current=i_send+1, total=downloaded_count_album, title=short_title_send, reason="Файл не найден")
                     continue

                if progress_callback_album:
                    await progress_callback_album("track_sending", current_index=i_send, total_downloaded=downloaded_count_album, title=short_title_send)

                sent_msg_album_track = await send_single_track(event, info_album_track, file_path_album_track)
                if sent_msg_album_track:
                    sent_count_album += 1
                    if progress_callback_album:
                         await progress_callback_album("track_sent", current_sent=sent_count_album, total_downloaded=downloaded_count_album, title=short_title_send)
                await asyncio.sleep(0.7)

            if use_progress and progress_message:
                final_album_icon = "✅" if sent_count_album == downloaded_count_album and downloaded_count_album > 0 else ("⚠️" if sent_count_album > 0 else "❌")
                statuses["Альбом/Плейлист"] = f"{final_album_icon} '{album_title_display}'"
                statuses["Прогресс Скачивания"] = f"🏁 Скачано {downloaded_count_album}/{total_tracks_album or '?'}"
                statuses["Отправка Треков"] = f"🏁 Отправлено {sent_count_album}/{downloaded_count_album}"
                await update_progress(progress_message, statuses)
                # FIX: Separate sleep and try-except for final album progress message deletion
                await asyncio.sleep(5)
                try:
                    await progress_message.delete()
                    progress_message = None # Mark as deleted
                except Exception as e_del_prog: # Catch any error during deletion
                    logger.debug(f"Failed to delete progress message {getattr(progress_message, 'id', 'N/A')} for album command: {e_del_prog}")
                    pass


    except Exception as e_dl_main:
        logger.error(f"Ошибка при выполнении команды download (Флаг: {download_type_flag}, Арг: '{target_arg}'): {e_dl_main}", exc_info=True)
        error_prefix_dl = "⚠️" if isinstance(e_dl_main, (ValueError, FileNotFoundError, TypeError)) else "❌"
        error_text_dl = f"{error_prefix_dl} Ошибка при скачивании/отправке:\n`{type(e_dl_main).__name__}: {str(e_dl_main)[:150]}`"
        final_error_message = None
        if use_progress and progress_message:
            for task_key_err_dl in statuses: statuses[task_key_err_dl] = str(statuses[task_key_err_dl]).replace("🔄", "⏹️").replace("✅", "⏹️").replace("⏳", "⏹️").replace("▶️", "⏹️").replace("📥", "⏹️").replace("📤", "⏹️").replace("✔️", "⏹️").replace("⏸️", "⏹️")
            statuses["Состояние"] = "❌ Глобальная ошибка!"
            try: await update_progress(progress_message, statuses)
            except Exception: pass
            try:
                await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text_dl}")
                final_error_message = progress_message
            except Exception as edit_e_dl:
                logger.error(f"Не удалось изменить прогресс для ошибки: {edit_e_dl}")
                final_error_message = await event.reply(error_text_dl)
        else:
            final_error_message = await event.reply(error_text_dl)

        if final_error_message and (final_error_message != progress_message or not use_progress):
            await store_response_message(event.chat_id, final_error_message)
    finally:
        pass # Cleanup is handled by send_single_track for each file


# =============================================================================
#              AUTHENTICATED COMMAND HANDLERS (rec, alast, likes)
# =============================================================================

@require_ytmusic_auth
async def handle_recommendations(event: events.NewMessage.Event, args: List[str]):
    """Fetches personalized music recommendations."""
    limit = config.get("recommendations_limit", 8)
    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    final_sent_message = None # To store the message that will be kept for auto-clear

    try:
        if use_progress:
            statuses = {"Получение рекомендаций": "⏳ Ожидание...", "Форматирование": "⏸️"}
            progress_message = await event.reply("\n".join(f"{task}: {value}" for task, value in statuses.items()))
            await store_response_message(event.chat_id, progress_message) # Store progress message

        if use_progress: statuses["Получение рекомендаций"] = "🔄 Запрос истории для основы..."; await update_progress(progress_message, statuses)

        recommendations_list = []
        seed_video_id = None
        history_fetch_success_for_seed = False
        recommendation_source_info = "из общей ленты" # Default source description

        try:
            history_items = await _api_get_history() # Wrapped API call
            if history_items and isinstance(history_items, list) and \
               history_items[0] and isinstance(history_items[0], dict) and history_items[0].get('videoId'):
                 seed_video_id = history_items[0]['videoId']
                 history_fetch_success_for_seed = True
                 recommendation_source_info = f"на основе трека '{history_items[0].get('title', seed_video_id)[:20]}...'"
                 logger.info(f"Using last listened track ({seed_video_id}) as seed for recommendations.")
            else:
                 logger.info("Listening history is empty or first item is invalid. Will use generic home feed recommendations.")
        except Exception as e_hist_rec:
             logger.warning(f"Error getting history for recommendation seed: {e_hist_rec}. Using generic home feed.", exc_info=True)
             history_items = [] # Ensure it's an empty list on error

        if use_progress:
             status_after_history = "✅ История для основы получена. Запрос рекомендаций..." if history_fetch_success_for_seed \
                               else "ℹ️ История пуста/ошибка. Запрос из общей ленты..."
             statuses["Получение рекомендаций"] = status_after_history
             await update_progress(progress_message, statuses)


        if seed_video_id: # Try recommendations based on seed track
             try:
                 # radio=True gets related tracks, limit needs to be sufficient
                 raw_recs_watch_playlist = await _api_get_watch_playlist(videoId=seed_video_id, radio=True, limit=limit + 5) # Fetch a bit more
                 recommendations_list = raw_recs_watch_playlist.get('tracks', []) if raw_recs_watch_playlist else []

                 # Optional: Add the seed track itself if not present and was from history
                 if history_fetch_success_for_seed and history_items:
                     current_rec_vids = {track.get('videoId') for track in recommendations_list if isinstance(track, dict)}
                     if seed_video_id not in current_rec_vids and history_items[0].get('videoId') == seed_video_id :
                          recommendations_list.insert(0, history_items[0]) # Add seed to start
                          logger.debug(f"Added seed track '{seed_video_id}' to recommendations list.")

             except Exception as e_watch_recs:
                  logger.warning(f"Error getting watch playlist for recommendations (seed: {seed_video_id}): {e_watch_recs}. Falling back to home feed.", exc_info=True)
                  seed_video_id = None # Nullify to trigger home feed fallback
                  recommendations_list = [] # Clear any partial list
                  recommendation_source_info = "из общей ленты (ошибка по истории)"
                  if use_progress:
                       statuses["Получение рекомендаций"] = "⚠️ Ошибка по истории. Запрос из общей ленты..."
                       await update_progress(progress_message, statuses)


        if not recommendations_list: # If no seed, or seed-based recs failed/empty
             if use_progress and seed_video_id is not None: # Means seed attempt was made but failed
                  statuses["Получение рекомендаций"] = "🔄 Запрос из общей ленты (Fallback)..."
                  await update_progress(progress_message, statuses)
             elif use_progress: # Directly went to home feed
                  statuses["Получение рекомендаций"] = "🔄 Запрос из общей ленты..."
                  await update_progress(progress_message, statuses)

             logger.info("Using generic home feed suggestions for recommendations.")
             try:
                 home_feed_sections = await _api_get_home(limit=limit + 5) # Fetch a bit more
                 # Extract tracks from various sections of home feed
                 # We need items with videoId, title, and artists/author
                 for section in home_feed_sections:
                     if isinstance(section, dict) and 'contents' in section and isinstance(section['contents'], list):
                         for item in section['contents']:
                             if isinstance(item, dict) and item.get('videoId') and item.get('title') and \
                                (isinstance(item.get('artists'), list) or isinstance(item.get('author'), dict)): # Check for artist info
                                 recommendations_list.append(item)
                 recommendation_source_info = "из общей ленты" # Update source if we used home feed
             except Exception as e_home_recs:
                  logger.error(f"Error getting home feed for recommendations: {e_home_recs}. Cannot provide recommendations.", exc_info=True)
                  raise Exception(f"Не удалось получить рекомендации из общей ленты: {e_home_recs}")


        # Filter duplicates and limit final list
        seen_track_ids = set()
        final_filtered_recs = []
        for track_item in recommendations_list:
             if track_item and isinstance(track_item, dict) and track_item.get('videoId'):
                 vid_rec = track_item['videoId']
                 # Ensure it's a valid 11-char ID (common for songs/videos)
                 if re.fullmatch(r'[A-Za-z0-9_-]{11}', vid_rec):
                      if vid_rec not in seen_track_ids:
                         final_filtered_recs.append(track_item)
                         seen_track_ids.add(vid_rec)
             if len(final_filtered_recs) >= limit: break # Stop once desired limit is reached

        results_to_display = final_filtered_recs

        if use_progress:
            rec_status_msg = f"✅ Найдено: {len(results_to_display)}" if results_to_display else "ℹ️ Не найдено"
            statuses["Получение рекомендаций"] = rec_status_msg
            statuses["Форматирование"] = "🔄 Подготовка..." if results_to_display else "➖"
            await update_progress(progress_message, statuses)

        if not results_to_display:
            final_message_text_no_recs = f"ℹ️ Не удалось найти персональные рекомендации ({recommendation_source_info})."
            if use_progress and progress_message:
                 await progress_message.edit(final_message_text_no_recs)
                 final_sent_message = progress_message # Progress message became the final message
            else:
                 final_sent_message = await event.reply(final_message_text_no_recs)
        else:
            response_lines_recs = []
            header_text_recs = f"🎧 **Рекомендации для вас ({recommendation_source_info}):**\n"
            response_text_final_recs = header_text_recs

            for i_rec, item_rec in enumerate(results_to_display):
                line_rec_parts = [f"{i_rec + 1}. "]
                if not item_rec or not isinstance(item_rec, dict):
                    logger.warning(f"Skipping invalid recommendation item {i_rec+1}: {item_rec}")
                    response_lines_recs.append(f"{i_rec + 1}. ⚠️ Неверный формат данных")
                    continue
                try:
                    title_rec = item_rec.get('title', 'Unknown Title')
                    artists_rec = format_artists(item_rec.get('artists') or item_rec.get('author'))
                    vid_rec_link = item_rec.get('videoId')
                    link_url_rec = f"https://music.youtube.com/watch?v={vid_rec_link}" if vid_rec_link else None
                    album_info_rec = item_rec.get('album') # dict with 'name', 'id'
                    album_name_rec = album_info_rec.get('name') if isinstance(album_info_rec, dict) else None
                    album_part_rec = f" (Альбом: {album_name_rec})" if album_name_rec else ""

                    line_rec_parts.append(f"**{title_rec}** - {artists_rec}{album_part_rec}")
                    full_line_rec = " ".join(part for part in line_rec_parts if part)
                    if link_url_rec: full_line_rec += f"\n   └ [Ссылка]({link_url_rec})"
                    response_lines_recs.append(full_line_rec)

                except Exception as fmt_e_rec:
                     logger.error(f"Error formatting recommendation item {i_rec+1}: {item_rec} - {fmt_e_rec}", exc_info=True)
                     response_lines_recs.append(f"{i_rec + 1}. ⚠️ Ошибка форматирования.")

            response_text_final_recs += "\n\n".join(response_lines_recs)

            if use_progress:
                statuses["Форматирование"] = "✅ Готово"
                await update_progress(progress_message, statuses)
                await progress_message.edit(response_text_final_recs, link_preview=False)
                final_sent_message = progress_message # Progress message became the final message
            else:
                final_sent_message = await event.reply(response_text_final_recs, link_preview=False)

    except Exception as e_recs_main:
        logger.error(f"Ошибка в команде recommendations: {e_recs_main}", exc_info=True)
        error_prefix_recs = "⚠️" if isinstance(e_recs_main, (ValueError, TypeError)) else "❌"
        error_text_recs = f"{error_prefix_recs} Ошибка при получении рекомендаций:\n`{type(e_recs_main).__name__}: {str(e_recs_main)[:100]}`"
        if use_progress and progress_message:
            for task_key_recs in statuses:
                 statuses[task_key_recs] = str(statuses[task_key_recs]).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌").replace("⏸️", "❌")
            try: await update_progress(progress_message, statuses)
            except Exception: pass
            try:
                await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text_recs}")
                final_sent_message = progress_message
            except Exception: final_sent_message = await event.reply(error_text_recs)
        else: final_sent_message = await event.reply(error_text_recs)
    finally:
        # Ensure the final message (success or error, which might be the progress message itself) is stored.
        # If progress_message was used and became final_sent_message, it's already stored.
        # If a new message was sent as final_sent_message, store it.
        if final_sent_message and (final_sent_message != progress_message or not use_progress):
            await store_response_message(event.chat_id, final_sent_message)
        # If progress_message exists and is different from final_sent_message, it means it should be deleted.
        elif use_progress and progress_message and progress_message != final_sent_message:
             await asyncio.sleep(2) # Give a moment
             try: await progress_message.delete()
             except Exception: pass


@require_ytmusic_auth
async def handle_history(event: events.NewMessage.Event, args: List[str]):
    """Fetches user's listening history."""
    limit = config.get("history_limit", 10)
    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    final_sent_message = None # To store the final message for auto-clear

    try:
        if use_progress:
            statuses = {"Получение истории": "⏳ Ожидание...", "Форматирование": "⏸️"}
            progress_message = await event.reply("\n".join(f"{task}: {value}" for task, value in statuses.items()))
            await store_response_message(event.chat_id, progress_message) # Store initial progress msg

        if use_progress: statuses["Получение истории"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)

        try:
            results_history = await _api_get_history() # Wrapped call
        except Exception as api_e_hist:
             logger.error(f"Failed to get history via API wrappers: {api_e_hist}", exc_info=True)
             raise Exception(f"Ошибка API при получении истории: {api_e_hist}") # Re-raise to be caught by main try-except

        if use_progress:
            hist_status_msg = f"✅ Найдено: {len(results_history)}" if results_history else "ℹ️ История пуста"
            statuses["Получение истории"] = hist_status_msg
            statuses["Форматирование"] = "🔄 Подготовка..." if results_history else "➖"
            await update_progress(progress_message, statuses)

        if not results_history:
            final_message_text_hist = f"ℹ️ Ваша история прослушиваний пуста."
            if progress_message: await progress_message.edit(final_message_text_hist); final_sent_message = progress_message
            else: final_sent_message = await event.reply(final_message_text_hist)
        else:
            response_lines_hist = []
            display_limit_hist = min(len(results_history), limit)
            response_text_final_hist = f"📜 **Недавняя история (последние {display_limit_hist}):**\n"

            for i_hist, item_hist in enumerate(results_history[:display_limit_hist]):
                line_hist_parts = [f"{i_hist + 1}. "]
                if not item_hist or not isinstance(item_hist, dict):
                    logger.warning(f"Skipping invalid history item {i_hist+1}: {item_hist}")
                    response_lines_hist.append(f"{i_hist + 1}. ⚠️ Неверный формат данных")
                    continue
                try:
                    title_hist = item_hist.get('title', 'Unknown Title')
                    artists_hist = format_artists(item_hist.get('artists'))
                    vid_hist = item_hist.get('videoId')
                    link_url_hist = f"https://music.youtube.com/watch?v={vid_hist}" if vid_hist else None
                    album_data_hist = item_hist.get('album') # dict with 'name', 'id'
                    album_name_hist = album_data_hist.get('name') if isinstance(album_data_hist, dict) else None
                    album_part_hist = f" (Альбом: {album_name_hist})" if album_name_hist else ""

                    line_hist_parts.append(f"**{title_hist}** - {artists_hist}{album_part_hist}")
                    full_line_hist = " ".join(part for part in line_hist_parts if part)
                    if link_url_hist: full_line_hist += f"\n   └ [Ссылка]({link_url_hist})"
                    response_lines_hist.append(full_line_hist)

                except Exception as fmt_e_hist:
                     logger.error(f"Error formatting history item {i_hist+1}: {item_hist} - {fmt_e_hist}", exc_info=True)
                     response_lines_hist.append(f"{i_hist + 1}. ⚠️ Ошибка форматирования.")

            response_text_final_hist += "\n\n".join(response_lines_hist)

            if use_progress:
                statuses["Форматирование"] = "✅ Готово"
                await update_progress(progress_message, statuses)
                await progress_message.edit(response_text_final_hist, link_preview=False)
                final_sent_message = progress_message # Progress message became the final one
            else:
                final_sent_message = await event.reply(response_text_final_hist, link_preview=False)

    except Exception as e_hist_main:
        logger.error(f"Ошибка в команде history: {e_hist_main}", exc_info=True)
        error_text_hist = f"❌ Ошибка при получении истории:\n`{type(e_hist_main).__name__}: {str(e_hist_main)[:100]}`"
        if use_progress and progress_message:
            for task_key_hist in statuses: statuses[task_key_hist] = str(statuses[task_key_hist]).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌").replace("⏸️", "❌")
            try: await update_progress(progress_message, statuses)
            except Exception: pass
            try: await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text_hist}"); final_sent_message = progress_message
            except Exception: final_sent_message = await event.reply(error_text_hist)
        else: final_sent_message = await event.reply(error_text_hist)
    finally:
        if final_sent_message and (final_sent_message != progress_message or not use_progress):
            await store_response_message(event.chat_id, final_sent_message)
        elif use_progress and progress_message and progress_message != final_sent_message:
            # FIX: Separate sleep and try-except on different lines
            await asyncio.sleep(2)
            try:
                await progress_message.delete()
            except Exception: # Catch any error during deletion
                pass

@require_ytmusic_auth
async def handle_liked_songs(event: events.NewMessage.Event, args: List[str]):
    """Fetches user's liked songs playlist."""
    limit = config.get("liked_songs_limit", 15)
    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    final_sent_message = None # To store the final message for auto-clear

    try:
        if use_progress:
            statuses = {"Получение лайков": "⏳ Ожидание...", "Форматирование": "⏸️"}
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))
            await store_response_message(event.chat_id, progress_message)

        if use_progress: statuses["Получение лайков"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)

        try:
            # _api_get_liked_songs returns the raw dict from ytmusicapi, which has a 'tracks' key
            liked_songs_data = await _api_get_liked_songs(limit=limit + 5) # Fetch a bit more for safety
            results_liked = liked_songs_data.get('tracks', []) if liked_songs_data and isinstance(liked_songs_data, dict) else []
        except Exception as api_e_liked:
             logger.error(f"Failed to get liked songs via API wrappers: {api_e_liked}", exc_info=True)
             raise Exception(f"Ошибка API при получении лайков: {api_e_liked}")

        if use_progress:
            like_status_msg = f"✅ Найдено: {len(results_liked)}" if results_liked else "ℹ️ Лайков не найдено"
            statuses["Получение лайков"] = like_status_msg
            statuses["Форматирование"] = "🔄 Подготовка..." if results_liked else "➖"
            await update_progress(progress_message, statuses)

        if not results_liked:
            final_message_text_liked = f"ℹ️ Плейлист 'Мне понравилось' пуст или не удалось загрузить."
            if progress_message: await progress_message.edit(final_message_text_liked); final_sent_message = progress_message
            else: final_sent_message = await event.reply(final_message_text_liked)
        else:
            response_lines_liked = []
            display_limit_liked = min(len(results_liked), limit) # Apply display limit
            response_text_final_liked = f"👍 **Треки 'Мне понравилось' (последние {display_limit_liked}):**\n"

            for i_liked, item_liked in enumerate(results_liked[:display_limit_liked]):
                line_liked_parts = [f"{i_liked + 1}. "]
                if not item_liked or not isinstance(item_liked, dict):
                    logger.warning(f"Skipping invalid liked song item {i_liked+1}: {item_liked}")
                    response_lines_liked.append(f"{i_liked + 1}. ⚠️ Неверный формат данных")
                    continue
                try:
                    title_liked = item_liked.get('title', 'Unknown Title')
                    artists_liked = format_artists(item_liked.get('artists'))
                    vid_liked = item_liked.get('videoId')
                    link_url_liked = f"https://music.youtube.com/watch?v={vid_liked}" if vid_liked else None
                    album_data_liked = item_liked.get('album') # dict with 'name', 'id'
                    album_name_liked = album_data_liked.get('name') if isinstance(album_data_liked, dict) else None
                    album_part_liked = f" (Альбом: {album_name_liked})" if album_name_liked else ""

                    line_liked_parts.append(f"**{title_liked}** - {artists_liked}{album_part_liked}")
                    full_line_liked = " ".join(part for part in line_liked_parts if part)
                    if link_url_liked: full_line_liked += f"\n   └ [Ссылка]({link_url_liked})"
                    response_lines_liked.append(full_line_liked)

                except Exception as fmt_e_liked:
                     logger.error(f"Error formatting liked song item {i_liked+1}: {item_liked} - {fmt_e_liked}", exc_info=True)
                     response_lines_liked.append(f"{i_liked + 1}. ⚠️ Ошибка форматирования.")

            response_text_final_liked += "\n\n".join(response_lines_liked)

            if use_progress:
                statuses["Форматирование"] = "✅ Готово"
                await update_progress(progress_message, statuses)
                await progress_message.edit(response_text_final_liked, link_preview=False)
                final_sent_message = progress_message # Progress message became the final one
            else:
                final_sent_message = await event.reply(response_text_final_liked, link_preview=False)

    except Exception as e_liked_main:
        logger.error(f"Ошибка в команде liked_songs: {e_liked_main}", exc_info=True)
        error_text_liked = f"❌ Ошибка при получении лайков:\n`{type(e_liked_main).__name__}: {str(e_liked_main)[:100]}`"
        if use_progress and progress_message:
            for task_key_liked in statuses: statuses[task_key_liked] = str(statuses[task_key_liked]).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌").replace("⏸️", "❌")
            try: await update_progress(progress_message, statuses)
            except Exception: pass
            try: await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text_liked}"); final_sent_message = progress_message
            except Exception: final_sent_message = await event.reply(error_text_liked)
        else: final_sent_message = await event.reply(error_text_liked)
    finally:
        if final_sent_message and (final_sent_message != progress_message or not use_progress):
            await store_response_message(event.chat_id, final_sent_message)
        elif use_progress and progress_message and progress_message != final_sent_message:
            # FIX: Separate sleep and try-except on different lines
            await asyncio.sleep(2)
            try:
                await progress_message.delete()
            except Exception: # Catch any error during deletion
                pass

# -------------------------
# Command: text / lyrics
# -------------------------
async def handle_lyrics(event: events.NewMessage.Event, args: List[str]):
    """Fetches and displays lyrics for a track ID or link."""
    prefix = config.get("prefix", ",")

    if not args:
        usage_lyrics = f"**Использование:** `{prefix}text <ID трека или ссылка на трек>`"
        await store_response_message(event.chat_id, await event.reply(usage_lyrics))
        return

    link_or_id_lyrics_arg = args[0] # Take the first argument as link/ID
    # Extract video ID, expecting an 11-character ID for tracks
    video_id_lyrics = extract_entity_id(link_or_id_lyrics_arg)

    if not video_id_lyrics or not re.fullmatch(r'[A-Za-z0-9_-]{11}', video_id_lyrics):
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не удалось распознать ID видео трека из `{link_or_id_lyrics_arg}`. Убедитесь, что это ID или ссылка на трек."))
        return

    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    lyrics_message_sent_and_stored = False # Track if send_lyrics handled storage

    try:
        if use_progress:
            statuses = {"Поиск информации о треке": "⏳ Ожидание...", "Получение текста": "⏸️"} # "Отправка" handled by send_lyrics
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))
            await store_response_message(event.chat_id, progress_message) # Store progress message

        # Fetch track info to get title and artist for the lyrics header
        track_title_for_header, track_artists_for_header = f"Трек ({video_id_lyrics})", "Неизвестный исполнитель"
        lyrics_browse_id_from_track_info = None

        if use_progress: statuses["Поиск информации о треке"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)
        try:
            # Use get_entity_info to get structured track details
            track_info_lyrics = await get_entity_info(video_id_lyrics, entity_type_hint="track")
            if track_info_lyrics and track_info_lyrics.get('_entity_type') == 'track':
                 # entity_info for track should be the videoDetails-like structure
                 track_title_for_header = track_info_lyrics.get('title', track_title_for_header)
                 artists_data_header = track_info_lyrics.get('artists') or track_info_lyrics.get('author')
                 track_artists_for_header = format_artists(artists_data_header) or track_artists_for_header
                 lyrics_browse_id_from_track_info = track_info_lyrics.get('lyricsBrowseId') or track_info_lyrics.get('lyrics') # from get_song

                 if use_progress: statuses["Поиск информации о треке"] = f"✅ {track_title_for_header[:30]}..."
            else: # Failed to get info or not a track
                 logger.warning(f"Информация о треке не найдена или неверный тип для {video_id_lyrics} (Тип: {track_info_lyrics.get('_entity_type') if track_info_lyrics else 'None'}). Заголовок текста будет стандартным.")
                 if use_progress: statuses["Поиск информации о треке"] = "⚠️ Инфо не найдено"
            await update_progress(progress_message, statuses)
        except Exception as e_info_lyrics:
             logger.warning(f"Ошибка получения информации о треке для заголовка текста ({video_id_lyrics}): {e_info_lyrics}", exc_info=True)
             if use_progress: statuses["Поиск информации о треке"] = "⚠️ Ошибка инфо"; await update_progress(progress_message, statuses)


        if use_progress: statuses["Получение текста"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)
        # Pass video_id and potentially lyrics_browse_id obtained from track_info
        lyrics_data_content = await get_lyrics_for_track(video_id_lyrics, lyrics_browse_id_from_track_info)

        if lyrics_data_content and lyrics_data_content.get('lyrics'):
            lyrics_actual_text = lyrics_data_content['lyrics']
            lyrics_source_details = lyrics_data_content.get('source')
            logger.info(f"Текст получен для '{track_title_for_header}' ({video_id_lyrics})")
            if use_progress: statuses["Получение текста"] = "✅ Получен. Отправка..."; await update_progress(progress_message, statuses)

            header_for_lyrics_msg = f"📜 **Текст песни:** {track_title_for_header} - {track_artists_for_header}"
            if lyrics_source_details: header_for_lyrics_msg += f"\n_(Источник: {lyrics_source_details})_"
            # Separator handled by send_lyrics logic or HTML structure

            # Delete progress message *before* sending lyrics, as send_lyrics will send new messages
            if progress_message:
                try: await progress_message.delete(); progress_message = None # Mark as deleted
                except Exception: pass # Ignore if already deleted

            await send_lyrics(event, lyrics_actual_text, header_for_lyrics_msg, track_title_for_header, video_id_lyrics)
            lyrics_message_sent_and_stored = True # send_lyrics handles storage of its messages

            # No further status update on original progress_message as it's gone.
        else: # Lyrics not found
            logger.info(f"Текст не найден для '{track_title_for_header}' ({video_id_lyrics}).")
            if use_progress and progress_message:
                statuses["Получение текста"] = "ℹ️ Не найден"; await update_progress(progress_message, statuses)
            # Edit progress message or send new one if progress_message is gone
            final_no_lyrics_text = f"ℹ️ Не удалось найти текст для трека `{track_title_for_header}` (`{video_id_lyrics}`)."
            no_lyrics_msg_obj = await (progress_message.edit(final_no_lyrics_text) if progress_message else event.reply(final_no_lyrics_text))
            # Store this "not found" message if it wasn't the original progress message that was already stored.
            if no_lyrics_msg_obj != progress_message:
                await store_response_message(event.chat_id, no_lyrics_msg_obj)


    except Exception as e_lyrics_main:
        logger.error(f"Ошибка в команде lyrics/text для {video_id_lyrics}: {e_lyrics_main}", exc_info=True)
        error_text_lyrics = f"❌ Ошибка при получении текста для `{video_id_lyrics}`:\n`{type(e_lyrics_main).__name__}: {str(e_lyrics_main)[:100]}`"
        final_error_msg_obj = None
        if use_progress and progress_message:
            for task_key_lyrics in statuses: statuses[task_key_lyrics] = str(statuses[task_key_lyrics]).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌").replace("⏸️", "❌")
            try: await update_progress(progress_message, statuses)
            except Exception: pass
            try:
                await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text_lyrics}")
                final_error_msg_obj = progress_message # Progress message became the error message
            except Exception: final_error_msg_obj = await event.reply(error_text_lyrics)
        else: final_error_msg_obj = await event.reply(error_text_lyrics)

        if final_error_msg_obj and (final_error_msg_obj != progress_message or not use_progress):
            await store_response_message(event.chat_id, final_error_msg_obj)

    finally:
         # Clean up progress message if it still exists and lyrics were not sent (meaning it wasn't deleted before send_lyrics)
         # and it wasn't converted into the final error/info message.
         if progress_message and not lyrics_message_sent_and_stored: # If lyrics were sent, progress_message was handled
              # Check if progress_message text is one of the final outcomes (error or "not found")
              current_text = getattr(progress_message, 'text', '')
              is_final_outcome = "Ошибка при получении текста" in current_text or "Не удалось найти текст для трека" in current_text
              if not is_final_outcome:
                   await asyncio.sleep(3) # Delay before deleting intermediate progress
                   try: await progress_message.delete()
                   except Exception: pass


# Removed handle_add, handle_delete, handle_list

# -------------------------
# Command: help
# -------------------------
async def handle_help(event: events.NewMessage.Event, args=None):
    """Displays the help message from help.txt."""
    help_path = os.path.join(SCRIPT_DIR, 'help.txt')
    try:
        if not os.path.exists(help_path):
             logger.error(f"Файл справки не найден: {help_path}")
             error_msg_help = await event.reply(f"❌ Ошибка: Файл справки (`{os.path.basename(help_path)}`) не найден.")
             await store_response_message(event.chat_id, error_msg_help)
             # Fallback to basic command list
             try:
                 prefix = config.get("prefix", ",")
                 # Get command names from the handlers dict keys
                 available_commands_help = sorted([cmd_name for cmd_name in handlers.keys()])
                 basic_help_text = f"**Доступные команды (базовый список):**\n" + \
                                   "\n".join(f"`{prefix}{cmd_name_help}`" for cmd_name_help in available_commands_help)
                 basic_msg_help = await event.reply(basic_help_text, link_preview=False)
                 await store_response_message(event.chat_id, basic_msg_help)
             except Exception as basic_e_help:
                 logger.error(f"Не удалось сгенерировать базовую справку: {basic_e_help}", exc_info=True)
             return

        with open(help_path, "r", encoding="utf-8") as f_help: help_text_content = f_help.read().strip()

        current_prefix_help = config.get("prefix", ",")
        # YTMusic auth status for help text
        auth_status_indicator_help = "✅ Авторизация YTMusic: Активна" if ytmusic_authenticated else "⚠️ Авторизация YTMusic: Неактивна (некоторые команды могут не работать или работать с ограничениями)"

        formatted_help_text = help_text_content.replace("{prefix}", current_prefix_help)
        formatted_help_text = formatted_help_text.replace("{auth_status_indicator}", auth_status_indicator_help)

        # send_long_message handles storing its own messages
        await send_long_message(event, formatted_help_text, prefix="") # No prefix needed for help message itself

    except Exception as e_help:
        logger.error(f"Ошибка чтения/форматирования справки: {e_help}", exc_info=True)
        error_msg_help_final = await event.reply("❌ Ошибка при отображении справки.")
        await store_response_message(event.chat_id, error_msg_help_final)


# -------------------------
# Command: last
# -------------------------
async def handle_last(event: events.NewMessage.Event, args=None):
    """Displays the list of recently downloaded tracks from last.csv."""
    if not config.get("recent_downloads", True):
        no_tracking_msg = await event.reply("ℹ️ Отслеживание недавних скачиваний отключено в конфигурации.")
        await store_response_message(event.chat_id, no_tracking_msg)
        return

    tracks_history = load_last_tracks() # Expects 6 columns per new format
    if not tracks_history:
        empty_hist_msg = await event.reply("ℹ️ Список недавних скачанных треков пуст.")
        await store_response_message(event.chat_id, empty_hist_msg)
        return

    response_lines_last = ["**⏳ Недавно скачанные треки:**"]
    # New format: Track Title, Artists, Video ID, Track URL, Duration Seconds, Timestamp
    for i_last, entry_last in enumerate(tracks_history): # Iterate up to 5 (handled by save_last_tracks)
        if len(entry_last) >= EXPECTED_LAST_TRACKS_COLUMNS: # Check for new 6-column format
            track_title_csv, artists_csv, video_id_csv, track_url_csv, duration_s_csv, timestamp_csv = entry_last[:EXPECTED_LAST_TRACKS_COLUMNS]

            # Clean up display values
            display_title_csv = track_title_csv.strip() if track_title_csv and track_title_csv.strip() != 'Неизвестно' else 'N/A'
            display_artists_csv = artists_csv.strip() if artists_csv and artists_csv.strip().lower() not in ['неизвестно', 'unknown artist', 'n/a', ''] else ''

            name_part_csv = f"**{display_title_csv}**"
            if display_artists_csv: name_part_csv += f" - {display_artists_csv}"

            # Link part using Track URL
            link_part_csv = ""
            if track_url_csv and track_url_csv.strip() != 'N/A' and track_url_csv.startswith("http"):
                link_part_csv = f"[Ссылка на трек]({track_url_csv.strip()})"
            elif video_id_csv and video_id_csv.strip() != 'N/A': # Fallback to constructing link from video_id if URL is bad
                fallback_url = f"https://music.youtube.com/watch?v={video_id_csv.strip()}"
                link_part_csv = f"[Ссылка на трек]({fallback_url})"
            elif video_id_csv and video_id_csv.strip() != 'N/A': # Just show ID if no valid URL
                 link_part_csv = f"(ID: `{video_id_csv.strip()}`)"


            duration_display_csv = ""
            if duration_s_csv and duration_s_csv.strip().isdigit() and int(duration_s_csv) > 0:
                try:
                    dur_s = int(duration_s_csv)
                    mins_dur, secs_dur = divmod(dur_s, 60)
                    duration_display_csv = f"({mins_dur}:{secs_dur:02})"
                except ValueError: pass # Ignore if duration is not a valid int

            ts_part_csv = f"`({timestamp_csv.strip()})`" if timestamp_csv and timestamp_csv.strip() else ""

            # Combine parts for the line
            line_entry_csv = f"{i_last + 1}. {name_part_csv} {duration_display_csv}".strip()
            if link_part_csv: line_entry_csv += f" {link_part_csv}"
            if ts_part_csv: line_entry_csv += f" {ts_part_csv}"
            response_lines_last.append(line_entry_csv.strip())
        else:
            logger.warning(f"Skipping malformed entry in last tracks display (expected {EXPECTED_LAST_TRACKS_COLUMNS} columns): {entry_last}")

    if len(response_lines_last) == 1: # Only header means no valid tracks
        no_valid_hist_msg = await event.reply("ℹ️ Не найдено валидных записей в истории скачиваний (возможно, старый формат файла last.csv).")
        await store_response_message(event.chat_id, no_valid_hist_msg)
    else:
        response_msg_last = await event.reply("\n".join(response_lines_last), link_preview=False)
        await store_response_message(event.chat_id, response_msg_last)


# Helper function to get FFmpeg version (sync, run in executor)
def get_ffmpeg_version(ffmpeg_path_param: Optional[str]) -> str:
    """Synchronously gets FFmpeg version string."""
    if not ffmpeg_path_param:
        return "Не найден (путь не указан)"
    try:
        startupinfo_ffmpeg = None
        if platform.system() == 'Windows':
            startupinfo_ffmpeg = subprocess.STARTUPINFO()
            startupinfo_ffmpeg.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo_ffmpeg.wShowWindow = subprocess.SW_HIDE

        result_ffmpeg = subprocess.run(
            [ffmpeg_path_param, '-version'],
            capture_output=True, text=True, encoding='utf-8', errors='ignore', # Added encoding & errors
            timeout=5, startupinfo=startupinfo_ffmpeg
        )
        if result_ffmpeg.returncode == 0:
            first_line_ffmpeg = result_ffmpeg.stdout.strip().split('\n')[0]
            match_ffmpeg = re.search(r'ffmpeg version ([^\s]+)', first_line_ffmpeg)
            if match_ffmpeg:
                return match_ffmpeg.group(1)
            return "OK (версия не распознана из вывода)"
        else:
            return f"Ошибка выполнения FFmpeg (код={result_ffmpeg.returncode})"
    except FileNotFoundError:
        return "Не найден (файл не существует по указанному пути)"
    except subprocess.TimeoutExpired:
        return "Ошибка FFmpeg (таймаут выполнения 5с)"
    except Exception as e_ffmpeg_ver:
        logger.warning(f"Error getting FFmpeg version from {ffmpeg_path_param}: {e_ffmpeg_ver}")
        return f"Ошибка получения версии FFmpeg ({type(e_ffmpeg_ver).__name__})"

# Helper function to get Git repository info (sync, run in executor)
def get_git_repo_info(repo_path: str) -> Dict[str, str]:
    """Synchronously gets Git repository information."""
    info = {
        "status": "N/A",
        "branch": "N/A",
        "last_commit_hash": "N/A",
        "last_commit_date": "N/A",
        "last_commit_msg": "N/A",
        "remote_url": "N/A",
        "local_ahead": "N/A",
        "local_behind": "N/A",
        "is_dirty": "N/A",
        "error": ""
    }
    try:
        if not os.path.isdir(os.path.join(repo_path, '.git')):
            info["status"] = "Не является Git репозиторием"
            info["error"] = f"Папка .git не найдена в {repo_path}"
            logger.warning(f"Attempted to get git info from non-repo path: {repo_path}")
            return info

        repo = git.Repo(repo_path)
        info["is_dirty"] = str(repo.is_dirty())

        if repo.head.is_detached:
            info["branch"] = "DETACHED HEAD"
            info["last_commit_hash"] = repo.head.commit.hexsha[:7] # Short hash
        else:
            info["branch"] = repo.active_branch.name
            info["last_commit_hash"] = repo.active_branch.commit.hexsha[:7]

        last_commit = repo.head.commit
        info["last_commit_date"] = datetime.datetime.fromtimestamp(last_commit.committed_date).strftime('%Y-%m-%d %H:%M:%S %Z')
        info["last_commit_msg"] = last_commit.summary # First line of commit message

        if repo.remotes:
            origin = repo.remotes.origin if 'origin' in [r.name for r in repo.remotes] else repo.remotes[0]
            info["remote_url"] = next(origin.urls, "N/A")

            # Fetch latest changes from remote (non-blocking in terms of changing local files)
            try:
                logger.debug(f"Fetching remote for repo at {repo_path} to check status...")
                origin.fetch(prune=True, progress=None) # progress=None to avoid verbose output
                logger.debug(f"Fetch complete for {repo_path}.")
            except git.exc.GitCommandError as fetch_err:
                info["error"] += f" Ошибка при fetch: {str(fetch_err)[:100]}..."
                logger.warning(f"Git fetch failed for {repo_path}: {fetch_err}")
                # Continue, but behind/ahead status might be inaccurate

            # Compare local branch with its remote tracking branch
            if not repo.head.is_detached:
                tracking_branch = repo.active_branch.tracking_branch()
                if tracking_branch:
                    # Number of commits local branch is ahead/behind its remote tracking branch
                    # Format: (ahead, behind)
                    commits_ahead = sum(1 for _ in repo.iter_commits(f'{tracking_branch.name}..{repo.active_branch.name}'))
                    commits_behind = sum(1 for _ in repo.iter_commits(f'{repo.active_branch.name}..{tracking_branch.name}'))

                    info["local_ahead"] = str(commits_ahead)
                    info["local_behind"] = str(commits_behind)

                    if commits_behind > 0:
                        info["status"] = f"⚠️ Требуется обновление ({commits_behind} коммитов позади)"
                    elif commits_ahead > 0 and commits_behind == 0 :
                        info["status"] = f"✅ Актуально (но {commits_ahead} локальных коммитов впереди)"
                    elif commits_ahead == 0 and commits_behind == 0:
                         info["status"] = "✅ Актуально"
                    else: # Should not happen if ahead/behind are numbers
                         info["status"] = "Состояние не определено"
                else:
                    info["status"] = "Локальная ветка не отслеживает удаленную"
            else: # Detached head
                 info["status"] = "Отсоединенный HEAD"
        else:
            info["status"] = "Удаленный репозиторий не настроен"

    except git.exc.InvalidGitRepositoryError:
        info["status"] = "Не является Git репозиторием"
        info["error"] = f"InvalidGitRepositoryError для {repo_path}"
        logger.warning(f"InvalidGitRepositoryError for path: {repo_path}")
    except git.exc.NoSuchPathError:
        info["status"] = "Путь не найден"
        info["error"] = f"NoSuchPathError для {repo_path}"
        logger.warning(f"NoSuchPathError for git repo: {repo_path}")
    except Exception as e_git:
        info["status"] = "Ошибка Git"
        info["error"] = str(e_git)[:150]
        logger.error(f"Unexpected error getting Git info for {repo_path}: {e_git}", exc_info=True)
    return info


# -------------------------
# Command: host
# -------------------------
async def handle_host(event: events.NewMessage.Event, args: List[str]):
    """Displays system information with progress updates, including Git repo status."""
    statuses_host = {
        "Состояние": "⏳ Ожидание...",
        "Система": "⏸️",
        "Ресурсы (ЦПУ/ОЗУ/Диск)": "⏸️",
        "Сеть": "⏸️",
        "ПО (Версии)": "⏸️",
        "YTM": "⏸️",
        "Репозиторий YTMG": "⏸️" # Changed icon and added to statuses
    }
    progress_message_host = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses_host.items()))
    await store_response_message(event.chat_id, progress_message_host) # Store initial progress

    loop_host = asyncio.get_running_loop() # Get current event loop

    try:
        # --- System Info ---
        statuses_host["Система"] = "🔄 Сбор инфо..."
        await update_progress(progress_message_host, statuses_host)
        system_info_val = platform.system()
        os_name_val = system_info_val # Default
        kernel_val = platform.release()
        architecture_val = platform.machine()
        hostname_val = platform.node()

        try: # More detailed OS name
            if system_info_val == 'Linux':
                 try: os_name_val = (await loop_host.run_in_executor(None, platform.freedesktop_os_release)).get('PRETTY_NAME', system_info_val)
                 except AttributeError: # Fallback if freedesktop_os_release not available
                      if os.path.exists('/etc/os-release'):
                          with open('/etc/os-release', 'r') as f_os:
                               lines_os = f_os.readlines()
                               os_name_line_val = next((line for line in lines_os if line.startswith('PRETTY_NAME=')), None)
                               if os_name_line_val: os_name_val = os_name_line_val.split('=', 1)[1].strip().strip('"\'')
                      elif os.path.exists('/etc/issue'):
                          with open('/etc/issue', 'r') as f_issue_os: os_name_val = f_issue_os.readline().strip().replace('\\n', '').replace('\\l', '').strip()
                      if os_name_val == system_info_val: os_name_val = f"{system_info_val} ({platform.platform()})" # Generic
            elif system_info_val == 'Windows': os_name_val = f"{platform.system()} {platform.release()} ({platform.version()})"
            elif system_info_val == 'Darwin': os_name_val = f"macOS {(await loop_host.run_in_executor(None, platform.mac_ver))[0]}"
        except Exception as e_os_detail: logger.warning(f"Could not get detailed OS name: {e_os_detail}")
        statuses_host["Система"] = f"✅ {os_name_val} ({architecture_val})"
        await update_progress(progress_message_host, statuses_host)


        # --- Resources ---
        statuses_host["Ресурсы (ЦПУ/ОЗУ/Диск)"] = "🔄 Сбор данных..."
        await update_progress(progress_message_host, statuses_host)
        ram_info_val, cpu_info_val, disk_info_val = "N/A", "N/A", "N/A"
        disk_check_path_val = SCRIPT_DIR # Default path for disk check

        try: # RAM
             mem_val = await loop_host.run_in_executor(None, psutil.virtual_memory)
             ram_info_val = f"{mem_val.used / (1024 ** 3):.2f} ГБ / {mem_val.total / (1024 ** 3):.2f} ГБ ({mem_val.percent}%)"
        except Exception as e_ram_host: logger.warning(f"Could not get RAM info: {e_ram_host}")
        try: # CPU
            cpu_count_logical_val = await loop_host.run_in_executor(None, psutil.cpu_count, True)
            cpu_usage_val = await loop_host.run_in_executor(None, functools.partial(psutil.cpu_percent, interval=0.5))
            cpu_info_val = f"{cpu_count_logical_val} ядер, загрузка {cpu_usage_val:.1f}%"
        except Exception as e_cpu_host: logger.warning(f"Could not get CPU info: {e_cpu_host}")
        try: # Disk
            disk_check_path_val = os.path.expanduser('~') # User's home directory
            if not await loop_host.run_in_executor(None, os.path.exists, disk_check_path_val):
                 disk_check_path_val = SCRIPT_DIR # Fallback to script dir
            disk_val = await loop_host.run_in_executor(None, functools.partial(psutil.disk_usage, disk_check_path_val))
            disk_info_val = f"{disk_val.used / (1024 ** 3):.2f} ГБ / {disk_val.total / (1024 ** 3):.2f} ГБ ({disk_val.percent}%)"
        except Exception as e_disk_host:
             logger.error(f"Could not get disk usage for {disk_check_path_val}: {e_disk_host}", exc_info=True)
             disk_info_val = f"Ошибка ({type(e_disk_host).__name__})"
        statuses_host["Ресурсы (ЦПУ/ОЗУ/Диск)"] = "✅ Данные получены" # Single update after all resource checks
        await update_progress(progress_message_host, statuses_host)


        # --- Uptime ---
        uptime_str_val = "N/A"
        try:
            boot_time_val = await loop_host.run_in_executor(None, psutil.boot_time)
            uptime_seconds_val = datetime.datetime.now().timestamp() - boot_time_val
            if uptime_seconds_val > 0:
                td_uptime = datetime.timedelta(seconds=int(uptime_seconds_val))
                days_up, rem_s_up = td_uptime.days, td_uptime.seconds
                hours_up, rem_min_s_up = divmod(rem_s_up, 3600)
                minutes_up, _ = divmod(rem_min_s_up, 60) # Seconds not usually shown for long uptimes
                parts_up = []
                if days_up > 0: parts_up.append(f"{days_up} дн.")
                if hours_up > 0 or days_up > 0: parts_up.append(f"{hours_up:02} ч.") # Show hours if days > 0
                if minutes_up > 0 or hours_up > 0 or days_up > 0: parts_up.append(f"{minutes_up:02} мин.")
                if not parts_up: parts_up.append(f"{int(uptime_seconds_val)} сек.") # Show seconds if very short uptime
                uptime_str_val = " ".join(parts_up).strip()
            else: uptime_str_val = "< 1 сек."
        except Exception as e_uptime_host: logger.warning(f"Could not get uptime: {e_uptime_host}")


        # --- Network Ping ---
        statuses_host["Сеть"] = "🔄 Пинг до 8.8.8.8..."
        await update_progress(progress_message_host, statuses_host)
        ping_result_val = "N/A"
        ping_target_val = "8.8.8.8"
        try:
            ping_cmd_path_val = await loop_host.run_in_executor(None, shutil.which, 'ping')
            if ping_cmd_path_val:
                startupinfo_ping = None
                if platform.system() == 'Windows':
                     startupinfo_ping = subprocess.STARTUPINFO(); startupinfo_ping.dwFlags |= subprocess.STARTF_USESHOWWINDOW; startupinfo_ping.wShowWindow = subprocess.SW_HIDE
                ping_args_val = [ping_cmd_path_val, '-n', '1', '-w', '2000', ping_target_val] if system_info_val == 'Windows' else [ping_cmd_path_val, '-c', '1', '-W', '2', ping_target_val]

                proc_ping = await asyncio.create_subprocess_exec(*ping_args_val, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, startupinfo=startupinfo_ping)
                stdout_ping, stderr_ping = await asyncio.wait_for(proc_ping.communicate(), timeout=4.0)
                if proc_ping.returncode == 0:
                    stdout_str_ping = stdout_ping.decode('utf-8', errors='ignore')
                    match_ping_time = re.search(r'time[=<]([^ ]+?) ?ms', stdout_str_ping, re.IGNORECASE) # More generic time match
                    if not match_ping_time and system_info_val == 'Windows':
                         match_ping_time = re.search(r'Average = (\d+)ms', stdout_str_ping, re.IGNORECASE)
                    ping_result_val = f"✅ {match_ping_time.group(1)} мс ({ping_target_val})" if match_ping_time else f"✅ OK ({ping_target_val}, RTT ?)"
                else:
                    stderr_str_ping = stderr_ping.decode('utf-8', errors='ignore').strip()
                    ping_result_val = f"❌ Ошибка ({ping_target_val}, код={proc_ping.returncode}{f': {stderr_str_ping[:30]}...' if stderr_str_ping else ''})"
            else: ping_result_val = "⚠️ 'ping' не найден"
        except asyncio.TimeoutError:
             try: proc_ping.terminate(); await proc_ping.wait() # type: ignore
             except Exception: pass
             ping_result_val = f"⌛ Таймаут 4с ({ping_target_val})"
        except FileNotFoundError: ping_result_val = f"⚠️ 'ping' не найден (FNF)"
        except Exception as e_ping_host:
             logger.warning(f"Ping test failed: {e_ping_host}"); ping_result_val = f"❓ Ошибка ({ping_target_val})"
        statuses_host["Сеть"] = ping_result_val
        await update_progress(progress_message_host, statuses_host)

        # --- Software Versions ---
        statuses_host["ПО (Версии)"] = "🔄 Сбор версий..."
        await update_progress(progress_message_host, statuses_host)
        python_v_val = platform.python_version()
        telethon_v_val, yt_dlp_v_val, ytmusicapi_v_val, pillow_v_val, psutil_v_val, requests_v_val, gitpython_v_val = ("Неизвестно",) * 7
        ffmpeg_v_str_val, ffmpeg_loc_str_val = "Неизвестно", "Неизвестно"

        try: telethon_v_val = telethon.__version__
        except Exception: pass
        try: yt_dlp_v_val = yt_dlp.version.__version__
        except Exception: pass
        try: from importlib import metadata as imeta; ytmusicapi_v_val = imeta.version('ytmusicapi')
        except Exception: pass
        try: pillow_v_val = Image.__version__
        except Exception: pass
        try: psutil_v_val = psutil.__version__
        except Exception: pass
        try: requests_v_val = requests.__version__
        except Exception: pass
        try: gitpython_v_val = git.__version__
        except Exception: pass


        ffmpeg_path_to_check = YDL_OPTS.get('ffmpeg_location') or await loop_host.run_in_executor(None, shutil.which, 'ffmpeg')
        if ffmpeg_path_to_check:
             ffmpeg_loc_str_val = ffmpeg_path_to_check
             ffmpeg_v_str_val = await loop_host.run_in_executor(None, get_ffmpeg_version, ffmpeg_path_to_check)
        else: ffmpeg_v_str_val = "Не найден (PATH/конфиг)"
        statuses_host["ПО (Версии)"] = "✅ Версии получены"
        await update_progress(progress_message_host, statuses_host)

        # --- YTM Auth Status ---
        statuses_host["YTM"] = "🔄 Проверка авторизации..."
        await update_progress(progress_message_host, statuses_host)
        auth_file_base_val = os.path.basename(YT_MUSIC_AUTH_FILE)
        ytm_auth_status_formatted_val = f"✅ Активна (`{auth_file_base_val}`)" if ytmusic_authenticated else f"⚠️ Не активна (нет `{auth_file_base_val}`)"
        statuses_host["YTM"] = ytm_auth_status_formatted_val
        await update_progress(progress_message_host, statuses_host)

        # --- Git Repository Info ---
        statuses_host["Репозиторий YTMG"] = "🔄 Проверка состояния..."
        await update_progress(progress_message_host, statuses_host)
        git_info = await loop_host.run_in_executor(None, get_git_repo_info, SCRIPT_DIR)
        git_status_display = git_info.get("status", "Ошибка получения статуса")
        if git_info.get("error"):
            git_status_display += f" (Детали: {git_info['error']})"
        statuses_host["Репозиторий YTMG"] = git_status_display
        await update_progress(progress_message_host, statuses_host)


        statuses_host["Состояние"] = "✅ Готово"
        await update_progress(progress_message_host, statuses_host)


        # Constructing the final message
        git_repo_details_lines = [
            f" ├ **URL:** `{git_info.get('remote_url', 'N/A')}`",
            f" ├ **Ветка:** `{git_info.get('branch', 'N/A')}`",
            f" ├ **Статус:** {git_status_display}", # Already formatted with ahead/behind
            f" ├ **Локально впереди:** `{git_info.get('local_ahead', 'N/A')}` коммитов",
            f" ├ **Локально позади:** `{git_info.get('local_behind', 'N/A')}` коммитов",
            f" ├ **Последний коммит:** `{git_info.get('last_commit_hash', 'N/A')}` от `{git_info.get('last_commit_date', 'N/A')}`",
            f" │  └ _Сообщение:_ `{git_info.get('last_commit_msg', 'N/A')}`",
            f" └ **Локальные изменения (is_dirty):** `{git_info.get('is_dirty', 'N/A')}`"
        ]
        git_repo_info_block = "\n".join(git_repo_details_lines)


        final_text_host = (
            f"🖥️ **Информация о системе**\n"
            f" ├ **Имя хоста:** `{hostname_val}`\n"
            f" ├ **ОС:** `{os_name_val}`\n"
            f" ├ **Ядро:** `{kernel_val}`\n"
            f" └ **Время работы системы:** `{uptime_str_val}`\n\n"

            f"⚙️ **Аппаратное обеспечение**\n"
            f" ├ **ЦПУ:** `{cpu_info_val}`\n"
            f" ├ **ОЗУ:** `{ram_info_val}`\n"
            f" └ **Диск ({disk_check_path_val or '/'}):** `{disk_info_val}`\n\n"

            f"🌐 **Сеть**\n"
            f" └ **Пинг:** `{ping_result_val}`\n\n"

            f"📦 **Версии ПО**\n"
            f" ├ **Python:** `{python_v_val}`\n"
            f" ├ **Telethon:** `{telethon_v_val}`\n"
            f" ├ **yt-dlp:** `{yt_dlp_v_val}`\n"
            f" ├ **ytmusicapi:** `{ytmusicapi_v_val}`\n"
            f" ├ **Pillow:** `{pillow_v_val}`\n"
            f" ├ **psutil:** `{psutil_v_val}`\n"
            f" ├ **Requests:** `{requests_v_val}`\n"
            f" ├ **GitPython:** `{gitpython_v_val}`\n"
            f" └ **FFmpeg:** `{ffmpeg_v_str_val}` (Путь: `{ffmpeg_loc_str_val}`)\n\n"

            f"🎵 **YouTube Music**\n"
            f" └ **Авторизация:** {ytm_auth_status_formatted_val}\n\n"

            f"💾 **Репозиторий YTMG ([den22den22/YTMG](https://github.com/den22den22/YTMG/))**\n" # Changed icon and added link
            f"{git_repo_info_block}"
        )
        await progress_message_host.edit(final_text_host, link_preview=False)

    except Exception as e_host_main:
        logger.error(f"Ошибка при сборе информации о хосте: {e_host_main}", exc_info=True)
        statuses_host["Состояние"] = "❌ Ошибка"
        for task_key_err_host in statuses_host:
             if statuses_host[task_key_err_host].startswith(("⏳", "🔄", "⏸️")):
                  statuses_host[task_key_err_host] = "❌ Ошибка"
        try: await update_progress(progress_message_host, statuses_host)
        except Exception: pass

        error_text_host = f"❌ Не удалось полностью получить инфо о хосте:\n`{type(e_host_main).__name__}: {str(e_host_main)[:100]}`"
        try:
             current_text_host = getattr(progress_message_host, 'text', '')
             await progress_message_host.edit(f"{current_text_host}\n\n{error_text_host}")
        except Exception as edit_e_host:
             logger.error(f"Не удалось изменить прогресс-сообщение для ошибки хоста: {edit_e_host}")
             new_error_msg_host = await event.reply(error_text_host)
             await store_response_message(event.chat_id, new_error_msg_host)

# ... (остальной ваш код после handle_host) ...


# =============================================================================
#                         MAIN EXECUTION & LIFECYCLE
# =============================================================================
# Define handlers dictionary globally after all handler functions are defined
handlers = {
    "search": handle_search,
    "see": handle_see,
    "download": handle_download,
    "dl": handle_download,
    "help": handle_help,
    "last": handle_last,
    "host": handle_host,
    "clear": handle_clear,
    "rec": handle_recommendations,
    "alast": handle_history, # 'alast' is an alias for history
    "history": handle_history, # Explicit history command
    "likes": handle_liked_songs,
    "text": handle_lyrics,
    "lyrics": handle_lyrics,
}

async def main():
    """Main asynchronous function to start the bot."""
    # Global variables for ytmusic client are already defined at module level
    # ytmusic, ytmusic_authenticated

    logger.info("--- Запуск бота YTMG ---")
    try:
        versions_startup = [f"Python: {platform.python_version()}"]
        try: versions_startup.append(f"Telethon: {telethon.__version__}")
        except Exception: versions_startup.append("Telethon: ?")
        try: versions_startup.append(f"yt-dlp: {yt_dlp.version.__version__}")
        except Exception: versions_startup.append("yt-dlp: ?")
        try: from importlib import metadata as imeta_startup; versions_startup.append(f"ytmusicapi: {imeta_startup.version('ytmusicapi')}")
        except Exception: versions_startup.append("ytmusicapi: ?")
        try: versions_startup.append(f"Pillow: {Image.__version__}")
        except Exception: versions_startup.append("Pillow: ?")
        try: versions_startup.append(f"psutil: {psutil.__version__}")
        except Exception: versions_startup.append("psutil: ?")
        try: versions_startup.append(f"Requests: {requests.__version__}")
        except Exception: versions_startup.append("Requests: ?")
        try: versions_startup.append(f"python-dotenv: {dotenv.__version__ if hasattr(dotenv, '__version__') else 'да'}") # Check for version
        except Exception: versions_startup.append("python-dotenv: ?")


        logger.info("Версии библиотек: " + " | ".join(versions_startup))

        logger.info("Подключение к Telegram...")
        await client.start()
        me_info = await client.get_me()
        if me_info:
            global BOT_OWNER_ID
            BOT_OWNER_ID = me_info.id
            name_owner = f"@{me_info.username}" if me_info.username else f"{me_info.first_name or ''} {me_info.last_name or ''}".strip() or f"ID: {me_info.id}"
            logger.info(f"Бот запущен как: {name_owner} (ID: {me_info.id}). Владелец определен как: {me_info.id}.")
        else:
            logger.critical("Не удалось получить информацию о себе (me). Не могу определить ID владельца. Завершение работы.")
            await client.disconnect()
            return

        # --- YTMusic API Initialization ---
        # initialize_ytmusic_client is async and handles setting ytmusic and ytmusic_authenticated
        await initialize_ytmusic_client()
        # Log status after initialization attempt
        if ytmusic:
            auth_status_log = "Активна" if ytmusic_authenticated else "Неактивна (или ошибка инициализации)"
            logger.info(f"Клиент YTMusic API инициализирован. Статус аутентификации: {auth_status_log}")
        else:
            logger.critical("КРИТИЧЕСКАЯ ОШИБКА: Клиент YTMusic API не смог инициализироваться (ytmusic is None). Функциональность YTM будет недоступна.")
            # Bot can continue running but YTM features will fail.


        logger.info(f"Конфигурация бота: Префикс='{config.get('prefix')}', "
                    f"AutoClear={'Вкл' if config.get('auto_clear') else 'Выкл'}, "
                    f"YTMusic Auth={'Активна' if ytmusic_authenticated else ('Неактивна' if ytmusic else 'ОШИБКА ИНИЦИАЛИЗАЦИИ')}")

        pp_info_main = "N/A"
        if YDL_OPTS.get('postprocessors'):
            try:
                 first_pp_main = YDL_OPTS['postprocessors'][0]
                 pp_info_main = first_pp_main.get('key','?')
                 if first_pp_main.get('key') == 'FFmpegExtractAudio' and first_pp_main.get('preferredcodec'):
                     pp_info_main += f" ({first_pp_main.get('preferredcodec')})"
            except Exception: pass # Ignore if structure is unexpected

        ydl_format_main = YDL_OPTS.get('format', 'N/A')
        ydl_outtmpl_main = YDL_OPTS.get('outtmpl', 'N/A')
        # Obscure full cookie path in logs for privacy, just show basename or N/A
        ydl_cookies_path_main = YDL_OPTS.get('cookiefile')
        ydl_cookies_display_main = os.path.basename(ydl_cookies_path_main) if ydl_cookies_path_main and isinstance(ydl_cookies_path_main, str) else 'N/A'

        logger.info(f"yt-dlp: Format='{ydl_format_main}', OutTmpl='{os.path.basename(ydl_outtmpl_main) if ydl_outtmpl_main else 'N/A'}', PP='{pp_info_main}', EmbedMeta={YDL_OPTS.get('embed_metadata')}, EmbedThumb={YDL_OPTS.get('embed_thumbnail')}, Cookies='{ydl_cookies_display_main}'")
        logger.info("--- Бот готов к приему команд ---")

        await client.run_until_disconnected()

    except (telethon_errors.AuthKeyError, telethon_errors.AuthKeyUnregisteredError, telethon_errors.rpcerrorlist.AuthKeyDuplicatedError) as e_authkey_main:
         session_file_main = os.path.join(SCRIPT_DIR, 'telegram_session.session') # Assuming default name
         logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА АВТОРИЗАЦИИ TELEGRAM ({type(e_authkey_main).__name__}): Невалидная сессия или ключ. "
                         f"Попробуйте удалить файл сессии '{session_file_main}' и перезапустить бота для новой авторизации.")
    except Exception as e_main_loop: # Catch-all for main loop errors
        logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА в главном цикле (main): {e_main_loop}", exc_info=True)
    finally:
        logger.info("--- Завершение работы бота YTMG ---")
        if client and client.is_connected():
            logger.info("Отключение от Telegram...")
            try:
                await client.disconnect()
                logger.info("Клиент Telegram успешно отключен.")
            except Exception as e_disc_main:
                 logger.error(f"Ошибка при отключении клиента Telegram: {e_disc_main}")
        logging.shutdown() # Ensure all log handlers are closed properly
        print("--- Бот YTMG остановлен ---")


# --- Entry Point ---
if __name__ == '__main__':
    try:
        if not os.path.isdir(SCRIPT_DIR): # Should not happen if get_script_dir works
             print(f"CRITICAL: Script directory '{SCRIPT_DIR}' not found or inaccessible. Exiting.")
             exit(1)

        # Ensure html module is available (already imported)
        # import html # Not needed here, already imported globally

        # Run the main async function
        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nПолучен сигнал прерывания (Ctrl+C). Остановка бота...")
    except Exception as e_top_level: # Catch any other exception at the very top level
        print(f"\nНеперехваченное исключение верхнего уровня: {e_top_level}")
        traceback.print_exc() # Print full traceback for diagnostics
    finally:
        print("Процесс завершен.")

# --- END OF FILE main.py ---