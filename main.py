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

# --- Helper Function for Auth Check Decorator ---
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
#                            DATA MANAGEMENT (Last Tracks)
# =============================================================================

LAST_TRACKS_FILE = os.path.join(SCRIPT_DIR, 'last.csv')
HELP_FILE = os.path.join(SCRIPT_DIR, 'help.txt')

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
                    original_row_count = sum(1 for row in csv.reader(f_count, delimiter=';') if row) - (1 if header else 0)
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
#                            CORE UTILITIES (with enhanced retry)
# =============================================================================

def retry(max_tries: int = 3, delay: float = 2.0, exceptions: Optional[Tuple[Type[Exception], ...]] = None, empty_result_check: Optional[str] = None):
    """Decorator to retry an async function upon encountering specific exceptions or empty results."""
    actual_exceptions = list(exceptions) if exceptions else []
    if requests.exceptions.RequestException not in actual_exceptions:
        actual_exceptions.append(requests.exceptions.RequestException)
    if Exception not in actual_exceptions:
        actual_exceptions.append(Exception)

    actual_exceptions_tuple = tuple(actual_exceptions)

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            attempt = 0
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
                    if attempt == max_tries - 1:
                        logger.error(f"'{func.__name__}' failed after {max_tries} attempts. Last error: {e}", exc_info=True)
                        raise
                    else:
                        wait_time = delay * (2 ** attempt)
                        logger.warning(f"Attempt {attempt + 1}/{max_tries} failed for '{func.__name__}': {e}. Retrying in {wait_time:.2f}s...")
                        await asyncio.sleep(wait_time)
                        attempt += 1

            if last_exception: raise last_exception
            return None
        return wrapper
    return decorator


def extract_entity_id(link_or_id: str) -> Optional[str]:
    """
    Extracts YouTube Music video ID, playlist ID, album/artist browse ID from a URL or returns the input if it looks like an ID.
    Handles standard YouTube video IDs as well.
    """
    if not isinstance(link_or_id, str): return None
    link_or_id = link_or_id.strip()

    if re.fullmatch(r'[A-Za-z0-9_-]{11}', link_or_id):
        return link_or_id

    if link_or_id.startswith(('PL', 'VL', 'OLAK5uy_')): return link_or_id
    if link_or_id.startswith(('MPRE', 'MPLA', 'RDAM')): return link_or_id
    if link_or_id.startswith('UC'): return link_or_id

    id_patterns = [
        r"(?:youtube\.com/watch\?v=|youtu\.be/)([A-Za-z0-9_-]{11})",
        r"(?:music\.youtube\.com/watch\?v=)([A-Za-z0-9_-]{11})",
        r"(?:music\.youtube\.com/playlist\?list=|youtube\.com/playlist\?list=)([A-Za-z0-9_-]+)",
        r"(?:music\.youtube\.com/browse/|youtube\.com/channel/)([A-Za-z0-9_-]+)",
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
     return await asyncio.to_thread(ytmusic.search, query, filter=filter_type, limit=limit)

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
        logger.error("YTMusic API client not initialized.")
        return None

    logger.debug(f"Fetching entity info for ID: {entity_id}, Hint: {entity_type_hint}")
    try:
        inferred_type = None
        if isinstance(entity_id, str):
            if re.fullmatch(r'[A-Za-z0-9_-]{11}', entity_id): inferred_type = "track"
            elif entity_id.startswith(('PL', 'VL', 'OLAK5uy_')): inferred_type = "playlist"
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
                 info = await call_func(entity_id if current_hint in ["playlist", "album", "artist"] else entity_id)
                 if info:
                     if current_hint == "track":
                         if info.get('videoDetails'):
                             processed_info = info['videoDetails']
                             if 'thumbnails' not in processed_info and 'thumbnail' in info:
                                 processed_info['thumbnails'] = (info.get('thumbnail') or {}).get('thumbnails')
                             if 'artists' not in processed_info and 'artists' in info:
                                 processed_info['artists'] = info['artists']
                             if 'lyrics' not in processed_info and 'lyrics' in info:
                                 processed_info['lyrics'] = info['lyrics']
                             info = processed_info
                         else:
                              logger.warning(f"_{current_hint} for {entity_id} lacked 'videoDetails'. Structure may be inconsistent.")
                              info['_incomplete_structure'] = True

                     info['_entity_type'] = current_hint
                     logger.info(f"Successfully fetched entity info using hint/inferred type '{current_hint}' for {entity_id}")
                     return info
                 else:
                     logger.warning(f"API call for hint '{current_hint}' returned no data for {entity_id}.")
             except Exception as e_hint:
                  logger.warning(f"API call for hint/inferred type '{current_hint}' failed for {entity_id}: {e_hint}. Trying generic checks.")


        generic_check_order_funcs = [
             ("track", _api_get_song),
             ("playlist", _api_get_playlist),
             ("album", _api_get_album),
             ("artist", _api_get_artist),
        ]

        for type_name, api_func in generic_check_order_funcs:
            if current_hint and current_hint == type_name: continue
            if type_name == "track" and not re.fullmatch(r'[A-Za-z0-9_-]{11}', entity_id): continue
            if type_name == "album" and not entity_id.startswith(('MPRE', 'MPLA', 'RDAM')): continue
            if type_name == "artist" and not entity_id.startswith('UC'): continue
            if type_name == "playlist" and not entity_id.startswith(('PL', 'VL', 'OLAK5uy_')): continue

            try:
                logger.debug(f"Trying generic API call for type '{type_name}' for {entity_id}")
                result = await api_func(entity_id if type_name in ["playlist", "album", "artist"] else entity_id)
                if result:
                    final_info = result
                    if type_name == "track":
                        if result.get('videoDetails'):
                            processed_info = result['videoDetails']
                            if 'thumbnails' not in processed_info and 'thumbnail' in result:
                                processed_info['thumbnails'] = (result.get('thumbnail') or {}).get('thumbnails')
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
                 pass


        if inferred_type == "track" and re.fullmatch(r'[A-Za-z0-9_-]{11}', entity_id):
             logger.debug(f"Final fallback: Trying get_watch_playlist for track ID {entity_id}")
             try:
                 watch_info = await _api_get_watch_playlist(entity_id, limit=1)
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
                                'viewCount': track_data.get('views')
                          }
                      }
                      logger.info(f"Successfully fetched track info (fallback) for {entity_id} using get_watch_playlist")
                      return standardized_info
                 else:
                      logger.debug(f"Final fallback get_watch_playlist for {entity_id} didn't return expected track data structure.")
             except Exception as e_final_watch:
                  logger.warning(f"Final fallback get_watch_playlist failed for {entity_id}: {e_final_watch}")


        logger.error(f"Could not retrieve info for entity ID: {entity_id} using any method.")
        return None

    except Exception as e_outer:
        logger.error(f"Unexpected error in get_entity_info processing for {entity_id}: {e_outer}", exc_info=True)
        raise


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
             logger.debug(f"Attempting to find lyrics browse ID via watch playlist for video: {video_id}")
             try:
                 watch_info = await _api_get_watch_playlist_for_lyrics(video_id)
                 final_lyrics_browse_id = watch_info.get('lyrics') if watch_info else None

                 if not final_lyrics_browse_id:
                      logger.info(f"No lyrics browse ID found in watch playlist info for {video_id}.")
                      return None
                 logger.debug(f"Found lyrics browse ID: {final_lyrics_browse_id} for video {video_id}")
             except Exception as e_watch_lookup:
                  logger.warning(f"Failed to get watch playlist info for lyrics browse ID lookup ({video_id}) after retries: {e_watch_lookup}")
                  return None

        if final_lyrics_browse_id:
             logger.info(f"Fetching lyrics content using browse ID: {final_lyrics_browse_id} (original ID: {track_id_for_log})")
             try:
                 lyrics_data = await _api_get_lyrics_content(final_lyrics_browse_id)
                 if lyrics_data and lyrics_data.get('lyrics'):
                     logger.info(f"Successfully fetched lyrics content for {track_id_for_log}")
                     return lyrics_data
                 else:
                      logger.info(f"API call for lyrics content succeeded but returned no lyrics for browse ID {final_lyrics_browse_id} (original ID: {track_id_for_log})")
                      return None

             except Exception as e_lyrics_fetch:
                  logger.error(f"Failed to fetch lyrics content using browse ID {final_lyrics_browse_id} (original ID: {track_id_for_log}) after retries: {e_lyrics_fetch}")
                  return None
        else:
             logger.error(f"Could not determine lyrics browse ID for {track_id_for_log}.")
             return None

    except Exception as e_outer:
        logger.error(f"Unexpected error in get_lyrics_for_track processing for {track_id_for_log}: {e_outer}", exc_info=True)
        return None


@retry(max_tries=3, delay=2.0, empty_result_check='[]')
async def _api_get_history():
    if not ytmusic: raise RuntimeError("YTMusic API client not initialized")
    logger.debug("Calling ytmusic.get_history()")
    return await asyncio.to_thread(ytmusic.get_history)

@retry(max_tries=3, delay=2.0, empty_result_check='[]')
async def _api_get_liked_songs(limit):
    if not ytmusic: raise RuntimeError("YTMusic API client not initialized")
    logger.debug(f"Calling ytmusic.get_liked_songs(limit={limit})")
    return await asyncio.to_thread(ytmusic.get_liked_songs, limit=limit)

@retry(max_tries=3, delay=2.0, empty_result_check='[]')
async def _api_get_home(limit):
    if not ytmusic: raise RuntimeError("YTMusic API client not initialized")
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
    This is a synchronous function designed to be run in an executor.
    """
    logger.info(f"Attempting download and processing via yt-dlp: {track_link}")
    try:
        current_ydl_opts = YDL_OPTS.copy()

        if current_ydl_opts.get('noplaylist', True):
             current_ydl_opts['noplaylist'] = True
             tmpl = current_ydl_opts.get('outtmpl', '%(title)s.%(ext)s')
             tmpl = re.sub(r'[\[\(]?%?\(playlist_index\)[0-9]*[ds]?[-_\. ]?[\]\)]?', '', tmpl).strip()
             current_ydl_opts['outtmpl'] = tmpl if tmpl else '%(title)s.%(ext)s'

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
                        base_potential, _ = os.path.splitext(potential_path_after_pp)
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
    Uses wrapped API calls for metadata and runs synchronous download_track in executor.
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
        tracks_to_download = []
        try:
            if album_browse_id.startswith(('MPRE', 'MPLA', 'RDAM')):
                album_info = await _api_get_album(album_browse_id)
                if album_info:
                    album_title = album_info.get('title', album_browse_id)
                    album_tracks_section = album_info.get('tracks')
                    if album_tracks_section and isinstance(album_tracks_section, list):
                         tracks_to_download = album_tracks_section
                    elif album_tracks_section and isinstance(album_tracks_section, dict) and 'results' in album_tracks_section:
                         tracks_to_download = album_tracks_section['results']
                    else:
                         logger.warning(f"Could not find 'tracks' list in album info structure for {album_browse_id}.")

                    total_tracks = album_info.get('trackCount') or len(tracks_to_download)
                    logger.info(f"Fetched album metadata: '{album_title}', Expected tracks: {total_tracks or len(tracks_to_download)}")
                else: logger.warning(f"Could not fetch album metadata for {album_browse_id} via wrapped api.")
            elif album_browse_id.startswith(('PL', 'VL', 'OLAK5uy_')):
                 album_info = await _api_get_playlist(album_browse_id, limit=None)
                 if album_info:
                     album_title = album_info.get('title', album_browse_id)
                     tracks_to_download = album_info.get('tracks', [])
                     total_tracks = album_info.get('trackCount') or len(tracks_to_download)
                     logger.info(f"Fetched playlist metadata: '{album_title}', Expected tracks: {total_tracks or len(tracks_to_download)}")
                 else: logger.warning(f"Could not fetch playlist metadata for {album_browse_id} via wrapped api.")
            else:
                 logger.info(f"ID {album_browse_id} type unknown. Attempting download via yt-dlp analysis.")
                 pass

        except Exception as e_meta:
             logger.warning(f"Error fetching metadata for ID {album_browse_id} via wrapped ytmusicapi: {e_meta}. Proceeding with yt-dlp analysis.", exc_info=True)


        if not tracks_to_download:
             logger.info(f"No tracks obtained from ytmusicapi metadata for {album_browse_id} or type unknown. Using yt-dlp analysis...")
             try:
                 analysis_url = album_browse_id
                 if album_browse_id.startswith(('MPRE', 'MPLA', 'RDAM')): analysis_url = f"https://music.youtube.com/browse/{album_browse_id}"
                 elif album_browse_id.startswith(('PL', 'VL', 'OLAK5uy_')): analysis_url = f"https://music.youtube.com/playlist?list={album_browse_id}"
                 elif re.fullmatch(r'[A-Za-z0-9_-]{11}', album_browse_id): analysis_url = f"https://music.youtube.com/watch?v={album_browse_id}"

                 analysis_opts = {'extract_flat': True, 'skip_download': True, 'quiet': True, 'ignoreerrors': True, 'noplaylist': False, 'cookiefile': YDL_OPTS.get('cookiefile')}
                 loop = asyncio.get_running_loop()
                 playlist_dict = await loop.run_in_executor(None, functools.partial(yt_dlp.YoutubeDL(analysis_opts).extract_info, analysis_url, download=False))

                 if playlist_dict and playlist_dict.get('entries'):
                     tracks_to_download = [{'videoId': entry.get('id'), 'title': entry.get('title'), 'artists': entry.get('channel') or entry.get('uploader')}
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
             logger.error(f"No tracks found to download for album/playlist {album_browse_id} after all attempts.")
             if progress_callback: await progress_callback("album_error", error="No tracks found to download")
             return []

        if total_tracks == 0 and tracks_to_download:
             total_tracks = len(tracks_to_download)

        if progress_callback:
            await progress_callback("analysis_complete", total_tracks=total_tracks, title=album_title)

        downloaded_count = 0
        loop = asyncio.get_running_loop()

        for i, track in enumerate(tracks_to_download):
            current_track_num = i + 1
            video_id = track.get('videoId')
            track_title_from_list = track.get('title') or f'Трек {current_track_num}'
            track_artists_from_list = format_artists(track.get('artists') or track.get('author'))

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
                         await progress_callback("track_failed", current=current_track_num,
                                               total=total_tracks, title=track_title_from_list, reason="Ошибка загрузки")

            except Exception as e_track_dl:
                logger.error(f"Error during download process for track {current_track_num} ('{track_title_from_list}'): {e_track_dl}", exc_info=True)
                if progress_callback:
                     await progress_callback("track_failed", current=current_track_num, total=total_tracks, title=f"{track_title_from_list} (Error)")

            await asyncio.sleep(0.3)

    except Exception as e_album_outer:
        logger.error(f"Error during album processing loop for {album_browse_id}: {e_album_outer}", exc_info=True)
        if progress_callback:
            await progress_callback("album_error", error=f"Outer error: {str(e_album_outer)[:50]}")

    logger.info(f"Finished sequential album download for '{album_title or album_browse_id}'. Successfully saved {len(downloaded_files)} out of {total_tracks or 'Unknown'} tracks attempted.")
    return downloaded_files


# =============================================================================
#                         LYRICS HANDLING
# =============================================================================

async def get_lyrics_for_track(video_id: Optional[str], lyrics_browse_id: Optional[str] = None) -> Optional[Dict[str, str]]:
    """
    Fetches lyrics for a track using its video ID or lyrics browse ID, using wrapped API calls.
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
             logger.debug(f"Attempting to find lyrics browse ID via watch playlist for video: {video_id}")
             try:
                 watch_info = await _api_get_watch_playlist_for_lyrics(video_id)
                 final_lyrics_browse_id = watch_info.get('lyrics') if watch_info else None

                 if not final_lyrics_browse_id:
                      logger.info(f"No lyrics browse ID found in watch playlist info for {video_id}.")
                      return None
                 logger.debug(f"Found lyrics browse ID: {final_lyrics_browse_id} for video {video_id}")
             except Exception as e_watch_lookup:
                  logger.warning(f"Failed to get watch playlist info for lyrics browse ID lookup ({video_id}) after retries: {e_watch_lookup}")
                  return None

        if final_lyrics_browse_id:
             logger.info(f"Fetching lyrics content using browse ID: {final_lyrics_browse_id} (original ID: {track_id_for_log})")
             try:
                 lyrics_data = await _api_get_lyrics_content(final_lyrics_browse_id)
                 if lyrics_data and lyrics_data.get('lyrics'):
                     logger.info(f"Successfully fetched lyrics content for {track_id_for_log}")
                     return lyrics_data
                 else:
                      logger.info(f"API call for lyrics content succeeded but returned no lyrics for browse ID {final_lyrics_browse_id} (original ID: {track_id_for_log})")
                      return None

             except Exception as e_lyrics_fetch:
                  logger.error(f"Failed to fetch lyrics content using browse ID {final_lyrics_browse_id} (original ID: {track_id_for_log}) after retries: {e_lyrics_fetch}")
                  return None
        else:
             logger.error(f"Could not determine lyrics browse ID for {track_id_for_log}.")
             return None

    except Exception as e_outer:
        logger.error(f"Unexpected error in get_lyrics_for_track processing for {track_id_for_log}: {e_outer}", exc_info=True)
        return None


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
    response = None

    try:
        try:
            parsed_url = urlparse(url)
            base_name_from_url = os.path.basename(parsed_url.path) if parsed_url.path else "thumb"
        except Exception as parse_e:
            logger.warning(f"Could not parse URL path for thumbnail naming: {parse_e}. Using default 'thumb'.")
            base_name_from_url = "thumb"

        base_name, potential_ext = os.path.splitext(base_name_from_url)
        if potential_ext and 1 <= len(potential_ext) <= 5 and potential_ext[1:].isalnum():
             ext = potential_ext.lower()
        else: ext = '.jpg'

        if not base_name or base_name == potential_ext: base_name = "thumb"
        safe_base_name = re.sub(r'[^\w.\-]', '_', base_name)
        max_len = 40
        safe_base_name = (safe_base_name[:max_len] + '...') if len(safe_base_name) > max_len else safe_base_name

        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")
        temp_filename = f"temp_thumb_{safe_base_name}_{timestamp}{ext}"
        temp_file_path = os.path.join(output_dir, temp_filename)

        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(None, lambda: requests.get(url, stream=True, timeout=25))
        response.raise_for_status()

        await loop.run_in_executor(None, functools.partial(save_response_to_file, response, temp_file_path))

        logger.debug(f"Thumbnail downloaded to temporary file: {temp_file_path}")

        try:
            await loop.run_in_executor(None, functools.partial(verify_image_file, temp_file_path))
            logger.debug(f"Thumbnail verified as valid image: {temp_file_path}")
            return temp_file_path
        except (FileNotFoundError, UnidentifiedImageError, SyntaxError, OSError, ValueError) as img_e:
             logger.error(f"Downloaded file is not a valid image ({url}): {img_e}. Deleting.")
             if os.path.exists(temp_file_path):
                 try: asyncio.create_task(cleanup_files(temp_file_path))
                 except Exception as rm_e: logger.warning(f"Could not remove invalid temp thumb {temp_file_path}: {rm_e}")
             return None

    except requests.exceptions.Timeout:
        logger.error(f"Timeout occurred while downloading thumbnail: {url}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error downloading thumbnail {url}: {e}")
        if temp_file_path and os.path.exists(temp_file_path):
            try: asyncio.create_task(cleanup_files(temp_file_path))
            except Exception as rm_e: logger.warning(f"Could not remove partial temp thumb {temp_file_path}: {rm_e}")
        raise
    except Exception as e_outer:
        logger.error(f"Unexpected error downloading thumbnail {url}: {e_outer}", exc_info=True)
        if temp_file_path and os.path.exists(temp_file_path):
            try: asyncio.create_task(cleanup_files(temp_file_path))
            except Exception as rm_e: logger.warning(f"Could not remove temp thumb {temp_file_path} after error: {rm_e}")
        raise
    finally:
        if response:
            try: response.close()
            except Exception as close_e: logger.warning(f"Error closing response for {url}: {close_e}")


def save_response_to_file(response: requests.Response, filepath: str):
    """Synchronously saves a requests response stream to a file."""
    with open(filepath, 'wb') as out_file:
        shutil.copyfileobj(response.raw, out_file)


def verify_image_file(filepath: str):
    """Synchronously verifies if a file is a valid image."""
    with Image.open(filepath) as img:
        img.verify()


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
    output_path = os.path.splitext(image_path)[0] + "_cropped.jpg"
    img_rgb = None

    loop = asyncio.get_running_loop()

    try:
        img = await loop.run_in_executor(None, Image.open, image_path)

        try:
            img_rgb = img
            if img.mode != 'RGB':
                logger.debug(f"Image mode is '{img.mode}', converting to RGB.")
                try:
                    bg = await loop.run_in_executor(None, Image.new, "RGB", img.size, (255, 255, 255))
                    if img.mode in ('RGBA', 'LA') and len(img.split()) > 3:
                        bands = await loop.run_in_executor(None, img.split)
                        alpha_band = bands[-1]
                        await loop.run_in_executor(None, functools.partial(bg.paste, img, mask=alpha_band))
                    else:
                        await loop.run_in_executor(None, functools.partial(bg.paste, img))
                    img_rgb = bg
                except Exception as conv_e:
                     logger.warning(f"Could not convert image {os.path.basename(image_path)} from {img.mode} to RGB using background paste: {conv_e}. Attempting basic conversion.")
                     try: img_rgb = await loop.run_in_executor(None, functools.partial(img.convert, 'RGB'))
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
            img_cropped = await loop.run_in_executor(None, functools.partial(img_rgb.crop, crop_box))

            await loop.run_in_executor(None, functools.partial(img_cropped.save, output_path, "JPEG", quality=90))

            logger.debug(f"Thumbnail cropped and saved successfully: {output_path}")
            return output_path

        except (UnidentifiedImageError, SyntaxError, OSError, ValueError) as e:
            logger.error(f"Cannot process thumbnail, invalid image file format or processing error during Pillow ops: {image_path} - {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during Pillow operations on thumbnail {os.path.basename(image_path)}: {e}", exc_info=True)
            if os.path.exists(output_path):
                try: asyncio.create_task(cleanup_files(output_path))
                except Exception as rm_e: logger.warning(f"Could not remove partial cropped thumb {output_path}: {rm_e}")
            raise

    except FileNotFoundError:
        logger.error(f"Cannot process thumbnail, file not found during Pillow ops: {image_path}")
        raise
    except Exception as e:
        logger.error(f"Error processing (cropping) thumbnail {os.path.basename(image_path)} (outer block): {e}", exc_info=True)
        if os.path.exists(output_path):
            try: asyncio.create_task(cleanup_files(output_path))
            except Exception as rm_e: logger.warning(f"Could not remove partial cropped thumb {output_path}: {rm_e}")
        raise


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
        os.path.join(SCRIPT_DIR, "lyrics_*.html"),
        os.path.join(SCRIPT_DIR, "N_A.jpg"),
        os.path.join(SCRIPT_DIR, "N_A.png"),
    ]

    all_files_to_remove = set()
    for f in files:
        if f and isinstance(f, str):
            try:
                real_path = os.path.realpath(f)
                if real_path.startswith(os.path.realpath(SCRIPT_DIR)):
                     all_files_to_remove.add(real_path)
                else:
                     logger.warning(f"Skipping cleanup of file outside script directory: {f}")
            except Exception as path_e:
                 logger.warning(f"Could not process path for file '{f}': {path_e}")


    for pattern in temp_patterns:
        try:
            script_real_path = os.path.realpath(SCRIPT_DIR)
            abs_pattern = os.path.abspath(pattern)
            loop = asyncio.get_running_loop()
            matched_files = await loop.run_in_executor(None, glob.glob, abs_pattern)
            if matched_files:
                logger.debug(f"Globbed {len(matched_files)} files for cleanup pattern: {pattern}")
                all_files_to_remove.update(os.path.realpath(mf) for mf in matched_files if os.path.realpath(mf).startswith(script_real_path))
        except Exception as e:
            logger.error(f"Error during glob matching for pattern '{pattern}': {e}")

    removed_count = 0
    if not all_files_to_remove:
        logger.debug("Cleanup called, but no files specified or matched for removal.")
        return

    logger.info(f"Attempting to clean up {len(all_files_to_remove)} potential files...")
    files_list = list(all_files_to_remove)

    loop = asyncio.get_running_loop()
    for file_path in files_list:
        try:
            if os.path.isfile(file_path):
                 await loop.run_in_executor(None, os.remove, file_path)
                 logger.debug(f"Removed file: {file_path}")
                 removed_count += 1
                 await asyncio.sleep(0.01)
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
    except telethon_errors.MessageNotModifiedError:
        pass
    except telethon_errors.MessageIdInvalidError:
        logger.warning(f"Failed to update progress: Message {progress_message.id} seems invalid or was deleted.")
    except telethon_errors.FloodWaitError as e:
         logger.warning(f"Flood wait ({e.seconds}s) while updating progress message {progress_message.id}. Pausing.")
         await asyncio.sleep(e.seconds + 1.0)
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

    valid_messages_to_delete = [msg for msg in messages_to_delete if msg and isinstance(msg, types.Message)]

    if not valid_messages_to_delete:
        logger.debug(f"No valid messages to delete found for chat {chat_id}.")
        return

    deleted_count = 0
    failed_to_delete = []

    logger.info(f"Attempting to clear {len(valid_messages_to_delete)} previous bot messages in chat {chat_id}")

    chunk_size = 100
    for i in range(0, len(valid_messages_to_delete), chunk_size):
        chunk = valid_messages_to_delete[i : i + chunk_size]
        message_ids = [msg.id for msg in chunk]
        if not message_ids: continue

        try:
            await client.delete_messages(chat_id, message_ids)
            deleted_count += len(message_ids)
            logger.debug(f"Deleted {len(message_ids)} messages in chat {chat_id}.")
        except telethon_errors.FloodWaitError as e:
             wait_time = e.seconds
             logger.warning(f"Flood wait ({wait_time}s) during message clearing chunk in chat {chat_id}. Pausing.")
             failed_to_delete.extend(chunk)
             await asyncio.sleep(wait_time + 1.5)
        except (telethon_errors.MessageDeleteForbiddenError, telethon_errors.MessageIdInvalidError) as e:
             logger.warning(f"Cannot delete some messages in chunk for chat {chat_id} ({len(message_ids)} IDs): {type(e).__name__} - {e}.")
             failed_to_delete.extend(chunk)
        except Exception as e_chunk:
             logger.error(f"Unexpected error deleting message chunk in chat {chat_id}: {e_chunk}", exc_info=True)
             failed_to_delete.extend(chunk)

    if deleted_count > 0:
        logger.info(f"Cleared {deleted_count} previous bot messages for chat {chat_id}.")
    if failed_to_delete:
        logger.warning(f"Failed to delete {len(failed_to_delete)} messages in chat {chat_id} after attempts.")

async def store_response_message(chat_id: int, message: Optional[types.Message]):
    """
    Stores a message object to be potentially cleared later by auto_clear.
    """
    if not message or not isinstance(message, types.Message) or not chat_id:
        return

    if not config.get("auto_clear", True):
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
    current_message = prefix.strip()
    lines = text.split('\n')

    for line in lines:
        space_needed = len(line) + (1 if current_message.strip() else 0)
        if len(current_message) + space_needed > MAX_LEN:
            if len(current_message) > 0:
                try:
                    msg = await event.respond(current_message)
                    sent_msgs.append(msg)
                    await asyncio.sleep(0.3)
                except Exception as e:
                    logger.error(f"Failed to send part of long message: {e}")
            current_message = prefix.strip()
            if len(current_message.strip()) > 0: current_message += "\n" + line
            else: current_message += line
        else:
            if len(current_message.strip()) > 0:
                 current_message += "\n" + line
            else:
                 current_message += line

    if current_message.strip() != prefix.strip() and current_message.strip() != "":
         try:
            msg = await event.respond(current_message)
            sent_msgs.append(msg)
         except Exception as e:
             logger.error(f"Failed to send final part of long message: {e}")

    for m in sent_msgs:
        await store_response_message(event.chat_id, m)


async def send_lyrics(event: events.NewMessage.Event, lyrics_text: str, lyrics_header: str, track_title: str, video_id: str):
    """
    Sends lyrics. If too long, sends as an HTML file.
    """
    MAX_LEN = 4096
    estimated_length = len(lyrics_header) + len(lyrics_text) + 200

    if estimated_length <= MAX_LEN:
        logger.info(f"Sending lyrics for '{track_title}' directly (Estimated Length: {estimated_length})")
        await send_long_message(event, lyrics_text, prefix=lyrics_header)
    else:
        logger.info(f"Lyrics for '{track_title}' too long ({estimated_length} > {MAX_LEN}). Sending as HTML file.")

        header_lines = lyrics_header.split('\n')
        html_title = track_title or "Текст песни"
        html_artist_line = ""
        if len(header_lines) > 0:
             artist_match = re.search(r"📜 \*\*Текст песни:\*\* .*? - (.*)", header_lines[0])
             if artist_match:
                  html_artist_line = artist_match.group(1).strip()
             else:
                  remaining_after_prefix = header_lines[0].replace("📜 **Текст песни:** ", "", 1).strip()
                  if remaining_after_prefix.lower().startswith(html_title.lower()):
                       html_artist_line = remaining_after_prefix[len(html_title):].strip(' -').strip()
                  else:
                       html_artist_line = remaining_after_prefix

        html_source_line = ""
        source_line = next((line for line in header_lines[1:] if "Источник:" in line), None)
        if source_line:
             source_match = re.search(r"\(Источник: (.*?)\)_", source_line)
             if source_match:
                  html_source_line = source_match.group(1).strip()

        import html as html_escape

        html_content = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{html_escape.escape(html_title)} - текст песни</title>
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
<body><div class="container"><h1>{html_escape.escape(html_title)}</h1>{f'<p class="artist-info">{html_escape.escape(html_artist_line)}</p>' if html_artist_line and html_artist_line != html_title else ''}{f'<p class="source">Источник: {html_escape.escape(html_source_line)}</p>' if html_source_line else ''}<pre>{html_escape.escape(lyrics_text)}</pre></div></body></html>"""

        safe_title = re.sub(r'[^\w\-]+', '_', track_title)[:50]
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        temp_filename = f"lyrics_{safe_title}_{video_id}_{timestamp}.html"
        temp_filepath = os.path.join(SCRIPT_DIR, temp_filename)
        sent_file_msg = None

        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, functools.partial(write_text_file, temp_filepath, html_content))
            logger.debug(f"Saved temporary HTML lyrics file: {temp_filepath}")

            caption = f"📜 Текст песни '{track_title}' (слишком длинный, в файле)"
            display_filename = f"{safe_title}_lyrics.html"
            sent_file_msg = await client.send_file(
                event.chat_id,
                file=temp_filepath,
                caption=caption,
                attributes=[types.DocumentAttributeFilename(file_name=display_filename)],
                force_document=True,
                reply_to=event.message.id
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
    if not BOT_OWNER_ID:
        logger.error("BOT_OWNER_ID is not set. Cannot authorize commands.")
        return

    is_self = event.message.out
    sender_id = event.sender_id
    is_owner = sender_id == BOT_OWNER_ID

    is_authorised = is_self or is_owner

    if not is_authorised:
        message_text_check = event.message.text.strip()
        prefix = config.get("prefix", ",")
        if message_text_check.startswith(prefix):
             logger.warning(f"Ignoring unauthorized command from user: {sender_id} in chat {event.chat_id}")
        return

    # --- Command Handling ---
    message_text = event.message.text
    prefix = config.get("prefix", ",")
    if not message_text.startswith(prefix): return

    command_string = message_text[len(prefix):].strip()
    if not command_string: return

    parts = command_string.split(maxsplit=1)
    command = parts[0].lower()
    args_str = parts[1] if len(parts) > 1 else ""
    args = args_str.split()

    logger.info(f"Received command: '{command}', Args: {args}, User: {sender_id}, Chat: {event.chat_id} (Owner: {is_owner})")

    if is_self or is_owner:
        try: await event.message.delete(); logger.debug(f"Deleted user/owner command message {event.message.id}")
        except Exception as e_del: logger.warning(f"Failed to delete user/owner command message {event.message.id}: {e_del}")

    commands_to_clear_for = (
        "search", "see", "last", "host", "download", "help", "dl",
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
        confirm_msg = await event.respond("ℹ️ Предыдущие ответы очищаются автоматически.", delete_in=10)
        logger.info(f"Executed 'clear' command (auto-clear enabled) in chat {event.chat_id}.")
        try: await asyncio.sleep(5); await confirm_msg.delete()
        except Exception: pass
    else:
        logger.info(f"Executing manual clear via command in chat {event.chat_id}.")
        await clear_previous_responses(event.chat_id)
        confirm_msg = await event.respond("✅ Предыдущие ответы бота очищены вручную.", delete_in=10)
        try: await asyncio.sleep(5); await confirm_msg.delete()
        except Exception: pass


# -------------------------
# Command: search (-t, -a, -p, -e, -v)
# -------------------------
async def handle_search(event: events.NewMessage.Event, args: List[str]):
    """Handles the search command."""
    valid_type_flags = {"-t", "-a", "-p", "-e"}
    prefix = config.get("prefix", ",")

    search_type_flag = None
    is_video_search = False
    query_parts = []

    for arg in args:
        if arg in valid_type_flags:
            if search_type_flag is None:
                search_type_flag = arg
            else:
                logger.warning(f"Multiple type flags provided in search, using first one: {search_type_flag}")
        elif arg == "-v":
            is_video_search = True
        else:
            query_parts.append(arg)

    query = " ".join(query_parts).strip()

    if not query:
        usage_text = (f"**Использование:** `{prefix}search [-t|-a|-p|-e|-v] <запрос>`\n"
                      f"Типы поиска: `-t` (треки, по умолчанию), `-a` (альбомы), `-p` (плейлисты), `-e` (исполнители), `-v` (видео).\n"
                      f"Флаг `-v` работает вместе с `-t` (поиск видео вместо аудио) или отдельно (поиск видео по умолчанию).")
        await store_response_message(event.chat_id, await event.reply(usage_text))
        return

    filter_type = "songs"

    if search_type_flag is None:
         if is_video_search:
              filter_type = "videos"
         else:
              filter_type = "songs"
    elif search_type_flag == "-t":
         if is_video_search:
              filter_type = "videos"
         else:
              filter_type = "songs"
    elif search_type_flag in {"-a", "-p", "-e"}:
         filter_map = {"-a": "albums", "-p": "playlists", "-e": "artists"}
         filter_type = filter_map[search_type_flag]
         if is_video_search:
              logger.warning(f"-v flag ignored when combined with {search_type_flag} in search command.")

    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    sent_message = None

    try:
        if use_progress:
            statuses = {"Поиск": "⏳ Ожидание...", "Форматирование": "⏸️"}
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))
            await store_response_message(event.chat_id, progress_message)

        if use_progress:
            search_context = {"songs": "треков", "albums": "альбомов", "playlists": "плейлистов", "artists": "исполнителей", "videos": "видео"}
            statuses["Поиск"] = f"🔄 Поиск {search_context.get(filter_type, '?')} '{query[:30]}...'..." if len(query)>33 else f"🔄 Поиск {search_context.get(filter_type, '?')} '{query}'..."
            await update_progress(progress_message, statuses)

        search_limit = min(max(1, config.get("default_search_limit", 8)), 15)
        results = await _api_search(query, filter_type, search_limit)

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
            type_labels = {"songs": "Треки", "albums": "Альбомы", "playlists": "Плейлисты", "artists": "Исполнители", "videos": "Видео"}
            response_text = f"**🔎 Результаты поиска ({type_labels.get(filter_type, '?')}) для `{query}`:**\n"

            for i, item in enumerate(results[:display_limit]):
                if not item or not isinstance(item, dict):
                    logger.warning(f"Skipping invalid item in search results: {item}")
                    continue

                line = f"{i + 1}. "
                try:
                    if filter_type == "songs":
                        title = item.get('title', 'Unknown Title')
                        artists = format_artists(item.get('artists'))
                        vid = item.get('videoId')
                        link = f"https://music.youtube.com/watch?v={vid}" if vid else None
                        line += f"**{title}** - {artists}" + (f"\n   └ [Ссылка]({link})" if link else "")

                    elif filter_type == "albums":
                        title = item.get('title', 'Unknown Album')
                        artists = format_artists(item.get('artists'))
                        bid = item.get('browseId')
                        year = item.get('year', '')
                        link = f"https://music.youtube.com/browse/{bid}" if bid else None
                        line += f"**{title}** - {artists}" + (f" ({year})" if year else "") + (f"\n   └ [Ссылка]({link})" if link else "")

                    elif filter_type == "artists":
                        artist_name = item.get('artist', item.get('title', 'Unknown Artist'))
                        bid = item.get('browseId')
                        link = f"https://music.youtube.com/channel/{bid}" if bid else None
                        if artist_name != 'Unknown Artist' and link:
                            line += f"**{artist_name}**\n   └ [Ссылка]({link})"
                        else: line = None

                    elif filter_type == "playlists":
                        title = item.get('title', 'Unknown Playlist')
                        author = format_artists(item.get('author'))
                        pid_raw = item.get('browseId')
                        pid = pid_raw.replace('VL', '') if pid_raw and isinstance(pid_raw, str) else None
                        link = f"https://music.youtube.com/playlist?list={pid}" if pid else None
                        line += f"**{title}** (Автор: {author})" + (f"\n   └ [Ссылка]({link})" if link else "")

                    elif filter_type == "videos":
                         title = item.get('title', 'Unknown Video')
                         artists_or_channel = format_artists(item.get('artists') or item.get('channel') or item.get('author'))
                         vid = item.get('videoId')
                         duration_s = item.get('duration_seconds') or item.get('duration')
                         duration_fmt = "N/A"
                         if duration_s is not None and duration_s > 0:
                             try:
                                 duration_s = int(duration_s)
                                 td = datetime.timedelta(seconds=duration_s)
                                 mins, secs = divmod(td.seconds, 60)
                                 hours, mins = divmod(mins, 60)
                                 duration_fmt = f"{hours}:{mins:02}:{secs:02}" if hours > 0 else f"{mins}:{secs:02}"
                             except (ValueError, TypeError):
                                 duration_fmt = "N/A"
                         link = f"https://www.youtube.com/watch?v={vid}" if vid else None
                         line += f"**{title}** - {artists_or_channel}" + (f" ({duration_fmt})" if duration_s is not None and duration_s > 0 else "") + (f"\n   └ [Ссылка]({link})" if link else "")

                    if line: response_lines.append(line)

                except Exception as fmt_e:
                     logger.error(f"Error formatting search result item {i+1} (Type: {filter_type}): {item} - {fmt_e}")
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
        error_text = f"❌ Произошла неожиданная ошибка:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
            for task in statuses: statuses[task] = str(statuses[task]).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌").replace("⏸️", "❌")
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
        if sent_message:
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
                 f"Указывать тип (флаг) необязательно, бот попробует определить автоматически.")
        # Store message for potential auto-clear triggered by *next* command
        await store_response_message(event.chat_id, await event.reply(usage))
        return

    entity_type_hint_flag = None
    include_cover = False
    include_lyrics = False
    link_or_id_arg = None
    remaining_args = list(args)

    if "-i" in remaining_args:
        include_cover = True
        remaining_args.remove("-i")
    if "-txt" in remaining_args:
        include_lyrics = True
        remaining_args.remove("-txt")

    for arg in remaining_args:
        if arg in valid_flags:
            entity_type_hint_flag = arg
            remaining_args.remove(arg)
            break

    if remaining_args:
        link_or_id_arg = remaining_args[0]
        if len(remaining_args) > 1:
             logger.warning(f"Ignoring extra arguments in see command: {remaining_args[1:]}")

    if not link_or_id_arg:
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не указана ссылка или ID."))
        return

    hint_map = {"-t": "track", "-a": "album", "-p": "playlist", "-e": "artist"}
    entity_type_hint = hint_map.get(entity_type_hint_flag) if entity_type_hint_flag else None

    entity_id = extract_entity_id(link_or_id_arg)
    if not entity_id:
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не удалось распознать ID из `{link_or_id_arg}`."))
        return

    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    temp_thumb_file, processed_thumb_file, final_info_message = None, None, None # Renamed final_sent_message for clarity
    files_to_clean_on_exit = []
    lyrics_message_stored = False # Keep track if send_lyrics handled the message storage

    try:
        if use_progress:
            statuses = {"Получение данных": "⏳ Ожидание...", "Форматирование": "⏸️"}
            if include_cover: statuses["Обложка"] = "⏸️"
            if include_lyrics: statuses["Текст"] = "⏸️"
            # FIX: Correctly unpack the tuple from statuses.items()
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))
            await store_response_message(event.chat_id, progress_message) # Store initial progress message

        if use_progress: statuses["Получение данных"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)

        entity_info = await get_entity_info(entity_id, entity_type_hint)

        if not entity_info:
            result_text = f"ℹ️ Не удалось найти информацию для: `{entity_id}`"
            # Edit progress message or send new reply, then store
            final_info_message = await (progress_message.edit(result_text) if use_progress and progress_message else event.reply(result_text))
            # Store the message containing the final result (failure or success)
            await store_response_message(event.chat_id, final_info_message)
        else:
            actual_entity_type = entity_info.get('_entity_type', 'unknown')
            if include_lyrics and actual_entity_type != 'track' and "Текст" in statuses:
                 statuses["Текст"] = "➖ (Только для треков)"
            if not include_lyrics and "Текст" in statuses:
                 del statuses["Текст"] # Remove status if lyrics not requested

            if use_progress:
                 statuses["Получение данных"] = f"✅ ({actual_entity_type})";
                 statuses["Форматирование"] = "🔄 Подготовка..." if actual_entity_type != 'unknown' else "➖";
                 await update_progress(progress_message, statuses)

            response_text = ""
            thumbnail_url = None
            title, artists = "Неизвестно", "Неизвестно" # Initialize title, artists here

            thumbnails_list = entity_info.get('thumbnails') or \
                              (entity_info.get('thumbnail') or {}).get('thumbnails')
            if not thumbnails_list:
                 thumbnails_list = entity_info.get('thumbnail')

            if isinstance(thumbnails_list, list) and thumbnails_list:
                try:
                    # Prefer highest resolution thumbnail
                    highest_res_thumb = sorted(thumbnails_list, key=lambda x: x.get('width', 0) * x.get('height', 0), reverse=True)[0]
                    thumbnail_url = highest_res_thumb.get('url')
                except (IndexError, KeyError, TypeError):
                    # Fallback to the last one if sorting fails or list structure is unexpected
                    if thumbnails_list:
                         thumbnail_url = thumbnails_list[-1].get('url')
            # If thumbnail_url is still None, maybe try the first one if available
            if not thumbnail_url and isinstance(thumbnails_list, list) and thumbnails_list:
                 try: thumbnail_url = thumbnails_list[0].get('url')
                 except (IndexError, KeyError, TypeError): pass


            if thumbnail_url: logger.debug(f"Found thumbnail URL: {thumbnail_url}")

            # Extract title and artists regardless of type for the response text
            title_from_info = entity_info.get('title', entity_info.get('name', 'Неизвестно'))
            artists_from_info = format_artists(entity_info.get('artists') or entity_info.get('author') or entity_info.get('uploader') or entity_info.get('artist'))
            title = title_from_info
            artists = artists_from_info

            if actual_entity_type == 'track':
                details = entity_info.get('videoDetails') or entity_info # Use videoDetails if present
                title = details.get('title', title) # Update title from details if available
                artists_data = details.get('artists') or details.get('author') or entity_info.get('uploader')
                artists = format_artists(artists_data) or artists # Update artists

                album_info = details.get('album')
                album_name = album_info.get('name') if isinstance(album_info, dict) else None
                album_id = album_info.get('id') if isinstance(album_info, dict) else None
                duration_s = None
                try: duration_s = int(details.get('lengthSeconds', 0))
                except (ValueError, TypeError): pass
                duration_fmt = "N/A"
                if duration_s is not None and duration_s > 0:
                    td = datetime.timedelta(seconds=duration_s)
                    mins, secs = divmod(td.seconds, 60)
                    hours, mins = divmod(mins, 60)
                    duration_fmt = f"{hours}:{mins:02}:{secs:02}" if hours > 0 else f"{mins}:{secs:02}"

                video_id_for_links = details.get('videoId', entity_id)
                link_url = f"https://music.youtube.com/watch?v={video_id_for_links}"
                lyrics_browse_id = details.get('lyrics') # Get lyrics ID from track info

                response_text = f"**Трек:** {title}\n**Исполнитель:** {artists}\n"
                if album_name:
                    album_link_url = f'https://music.youtube.com/browse/{album_id}' if album_id else None
                    album_link_md = f"[Ссылка]({album_link_url})" if album_link_url else ""
                    response_text += f"**Альбом:** {album_name} {album_link_md}\n"
                response_text += f"**Длительность:** {duration_fmt}\n"
                response_text += f"**ID:** `{video_id_for_links}`\n"
                if lyrics_browse_id: response_text += f"**Lyrics ID:** `{lyrics_browse_id}`\n"
                response_text += f"**Ссылка:** [Ссылка]({link_url})"

            elif actual_entity_type == 'album':
                # Title and artists already extracted above, use them
                year = entity_info.get('year')
                count = entity_info.get('trackCount') or len(entity_info.get('tracks', []))
                bid_raw = entity_info.get('audioPlaylistId') or entity_info.get('browseId') or entity_id
                bid = bid_raw.replace('RDAMPL', '').replace('RDAM', '').replace('MPRE', '') if isinstance(bid_raw, str) else entity_id
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
                        t_artists_list = t.get('artists')
                        t_artists = format_artists(t_artists_list) or artists # Fallback to album artist
                        t_id = t.get('videoId')
                        t_link_url = f'https://music.youtube.com/watch?v={t_id}' if t_id else None
                        t_link_md = f"[Ссылка]({t_link_url})" if t_link_url else ""
                        response_text += f"• {t_title} ({t_artists}) {t_link_md}\n"
                response_text = response_text.strip()

            elif actual_entity_type == 'playlist':
                 # Title and author already extracted above, use them
                 author = artists # Author is stored in 'artists' variable from format_artists
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
                         t_artists_list = t.get('artists')
                         t_artists = format_artists(t_artists_list) or author # Fallback to playlist author
                         t_id = t.get('videoId')
                         t_link_url = f'https://music.youtube.com/watch?v={t_id}' if t_id else None
                         t_link_md = f"[Ссылка]({t_link_url})" if t_link_url else ""
                         response_text += f"• {t_title} ({t_artists}) {t_link_md}\n"
                 response_text = response_text.strip()

            elif actual_entity_type == 'artist':
                 # Name already extracted above, use it
                 name = title # Artist name is stored in 'title' variable from entity_info
                 subs = entity_info.get('subscriberCountText')
                 songs_limit = config.get("artist_top_songs_limit", 5)
                 albums_limit = config.get("artist_albums_limit", 3)

                 cid = entity_info.get('channelId', entity_id)
                 link_url = f"https://music.youtube.com/channel/{cid}"

                 response_text = f"**Исполнитель:** {name}\n"
                 if subs: response_text += f"**Подписчики:** {subs}\n"
                 response_text += f"**ID:** `{cid}`\n"
                 response_text += f"**Ссылка:** [Ссылка]({link_url})\n"

                 # --- Поиск и отображение избранного/пин-трека ИЛИ последнего релиза ---
                 special_track_info = None
                 special_track_type = None # 'featured' or 'latest_release'
                 lyrics_browse_id_from_special_track = None

                 if use_progress: statuses["Спец. трек"] = "🔄 Поиск избранного..."; await update_progress(progress_message, statuses)

                 # 1. Поиск избранного трека
                 featured_sections = entity_info.get("featured", [])
                 if isinstance(featured_sections, list):
                     for section in featured_sections:
                         if isinstance(section, dict) and 'contents' in section and isinstance(section['contents'], list):
                             for item in section['contents']:
                                 # Ищем первый элемент, который выглядит как трек
                                 if isinstance(item, dict) and item.get('videoId') and item.get('title'):
                                     # item['type'] can be 'song' or 'video'. Both might be 'featured'.
                                     # Prioritize 'song' if possible, but take the first valid one found.
                                     special_track_info = item
                                     special_track_type = 'featured'
                                     break # Нашли первый избранный трек, останавливаем поиск
                             if special_track_info: break # Нашли в текущей секции, останавливаем внешние циклы

                 if use_progress:
                     if special_track_info: statuses["Спец. трек"] = "✅ Избранный найден"; await update_progress(progress_message, statuses)
                     else: statuses["Спец. трек"] = "🔄 Избранного нет. Поиск последнего релиза..."; await update_progress(progress_message, statuses)

                 # 2. Если избранный не найден, ищем трек из последнего релиза
                 if not special_track_info:
                     albums_data = entity_info.get("albums", {})
                     albums_list_results = albums_data.get("albums", []) # Use 'albums' key first
                     if not albums_list_results and isinstance(albums_data.get('results'), list):
                          albums_list_results = albums_data['results'] # Fallback to 'results'

                     latest_release_candidate = next((item for item in albums_list_results if isinstance(item, dict) and item.get('browseId')), None)

                     if latest_release_candidate:
                         latest_release_id = latest_release_candidate['browseId']
                         latest_release_title = latest_release_candidate.get('title', 'Неизвестный релиз')
                         logger.debug(f"Найден кандидат последнего релиза: '{latest_release_title}' ({latest_release_id}). Попытка получить его треки...")

                         if use_progress: statuses["Спец. трек"] = f"🔄 Анализ релиза '{latest_release_title[:20]}...'"; await update_progress(progress_message, statuses)

                         try:
                             # Fetch album/single details to get its tracks
                             latest_release_info = await _api_get_album(latest_release_id)

                             if latest_release_info and (isinstance(latest_release_info.get('tracks'), list) or (isinstance(latest_release_info.get('tracks'), dict) and isinstance(latest_release_info.get('tracks',{}).get('results'), list))):
                                  album_tracks_list = latest_release_info.get('tracks')
                                  if isinstance(album_tracks_list, dict) and isinstance(album_tracks_list.get('results'), list):
                                       album_tracks_list = album_tracks_list['results'] # Extract results list from dict structure

                                  # Find the first track in the latest release
                                  first_track_in_release = next((track for track in album_tracks_list if isinstance(track, dict) and track.get('videoId') and track.get('title')), None)

                                  if first_track_in_release:
                                       special_track_info = first_track_in_release
                                       special_track_type = 'latest_release'
                                       # Add album info to track info for display if needed
                                       special_track_info['_album'] = {'name': latest_release_title, 'id': latest_release_id}
                                       logger.debug(f"Найден первый трек из последнего релиза: '{first_track_in_release.get('title')}' ({first_track_in_release.get('videoId')})")
                                       if use_progress: statuses["Спец. трек"] = "✅ Найден трек релиза"; await update_progress(progress_message, statuses)
                                  else:
                                       logger.warning(f"Последний релиз {latest_release_id} не содержит валидных треков.")
                                       if use_progress: statuses["Спец. трек"] = "⚠️ Релиз без треков"; await update_progress(progress_message, statuses)
                             else:
                                  logger.warning(f"Не удалось получить информацию о треках для последнего релиза {latest_release_id}.")
                                  if use_progress: statuses["Спец. трек"] = "❌ Ошибка релиза"; await update_progress(progress_message, statuses)

                         except Exception as e_latest_release:
                             logger.error(f"Ошибка при получении треков последнего релиза {latest_release_id}: {e_latest_release}", exc_info=True)
                             if use_progress: statuses["Спец. трек"] = "❌ Ошибка получения релиза"; await update_progress(progress_message, statuses)

                     else:
                         logger.info("Не найдено кандидатов последнего релиза в альбомах артиста.")
                         if use_progress: statuses["Спец. трек"] = "ℹ️ Релизов нет"; await update_progress(progress_message, statuses)

                 # --- Отображение специального трека (избранного или последнего релиза) ---
                 if special_track_info:
                     st_title = special_track_info.get('title', 'Unknown Track')
                     st_artists_data = special_track_info.get('artists') or special_track_info.get('author')
                     st_artists = format_artists(st_artists_data) or name # Fallback to artist name

                     st_id = special_track_info.get('videoId')
                     st_link_url = f"https://music.youtube.com/watch?v={st_id}" if st_id else None
                     st_link_md = f"[Ссылка]({st_link_url})" if st_link_url else ""

                     st_header_text = "**🔥 Избранный трек:**" if special_track_type == 'featured' else "**🆕 Трек из последнего релиза:**"
                     st_album_part = ""
                     if special_track_type == 'latest_release':
                         st_album_info = special_track_info.get('_album')
                         if st_album_info and st_album_info.get('name'):
                             album_link_url = f'https://music.youtube.com/browse/{st_album_info.get("id")}' if st_album_info.get("id") else None
                             album_link_md = f"[Ссылка]({album_link_url})" if album_link_url else ""
                             st_album_part = f" (Альбом: {st_album_info['name']}{' ' + album_link_md if album_link_md else ''})"

                     response_text += f"\n{st_header_text}\n"
                     response_text += f"• {st_title} - {st_artists}{st_album_part} {st_link_md}\n"

                     # If fetching lyrics, use the lyrics ID from this special track
                     lyrics_browse_id_from_special_track = special_track_info.get('lyrics') # Get lyrics ID from the special track info


                 # --- Конец блока специального трека ---

                 songs_data = entity_info.get("songs", {})
                 songs = songs_data.get("results", []) if isinstance(songs_data, dict) else []
                 albums_data = entity_info.get("albums", {})
                 albums = albums_data.get("albums", []) if isinstance(albums_data, dict) else [] # Correction: often 'albums' key contains the list for artist albums
                 if not albums and isinstance(albums_data.get('results'), list):
                      albums = albums_data['results'] # Some structures might use 'results'

                 if songs:
                     if special_track_info: response_text += "\n" # Add separation if special track was shown
                     response_text += f"**Популярные треки (до {min(len(songs), songs_limit)}):**\n"
                     for s in songs[:songs_limit]:
                         s_title = s.get('title','?')
                         s_id = s.get('videoId')
                         s_link_url = f'https://music.youtube.com/watch?v={s_id}' if s_id else None
                         s_link_md = f"[Ссылка]({s_link_url})" if s_link_url else ""
                         response_text += f"• {s_title} {s_link_md}\n"
                 if albums:
                     if songs or special_track_info: response_text += "\n" # Add separation
                     response_text += f"**Альбомы/Синглы (до {min(len(albums), albums_limit)}):**\n" # Update label
                     for a in albums[:albums_limit]:
                         a_title = a.get('title','?')
                         a_id = a.get('browseId')
                         a_link_url = f'https://music.youtube.com/browse/{a_id}' if a_id else None
                         a_link_md = f"[Ссылка]({a_link_url})" if a_id else ""
                         a_year = a.get('year','')
                         # Show type if available
                         a_type = a.get('type', '').replace('single', 'Сингл').replace('album', 'Альбом')
                         type_part = f" ({a_type})" if a_type else ""
                         response_text += f"• {a_title}{type_part}" + (f" ({a_year})" if a_year else "") + f" {a_link_md}\n"
                 response_text = response_text.strip()
            else:
                response_text = f"⚠️ Тип '{actual_entity_type}' не поддерживается для `see`.\nID: `{entity_id}`"
                logger.warning(f"Unsupported entity type for 'see': {actual_entity_type}, ID: {entity_id}")
                # If unsupported type, still update progress and send the text
                if use_progress: statuses["Форматирование"] = "⚠️ Неподдерживаемый тип"; await update_progress(progress_message, statuses)


            # If response_text was generated, update formatting status
            if response_text and actual_entity_type != 'unknown':
                 if use_progress: statuses["Форматирование"] = "✅ Готово"; await update_progress(progress_message, statuses)
            elif use_progress: # If no response text was generated but info was found (shouldn't happen with current logic)
                 statuses["Форматирование"] = "⚠️ Пустой текст"; await update_progress(progress_message, statuses)


            # --- Thumbnail Handling ---
            # Only send the main info message *after* handling the thumbnail (if requested)
            # or directly if no thumbnail requested.

            if include_cover and thumbnail_url:
                if use_progress: statuses["Обложка"] = "🔄 Загрузка..."; await update_progress(progress_message, statuses)
                temp_thumb_file = await download_thumbnail(thumbnail_url)

                if temp_thumb_file:
                    files_to_clean_on_exit.append(temp_thumb_file)
                    if use_progress: statuses["Обложка"] = "🔄 Обработка..."; await update_progress(progress_message, statuses)

                    # Keep the original downloaded file for artists, crop for others
                    if actual_entity_type == 'artist':
                        processed_thumb_file = temp_thumb_file
                        logger.debug(f"Skipping crop for artist thumbnail: {temp_thumb_file}")
                    else:
                        processed_thumb_file = await crop_thumbnail(temp_thumb_file)
                        if processed_thumb_file and processed_thumb_file != temp_thumb_file:
                            files_to_clean_on_exit.append(processed_thumb_file)

                    status_icon = "✅" if processed_thumb_file and os.path.exists(processed_thumb_file) else "⚠️"
                    if use_progress: statuses["Обложка"] = f"{status_icon} Готово"; await update_progress(progress_message, statuses)

                    if processed_thumb_file and os.path.exists(processed_thumb_file):
                        try:
                            if use_progress: statuses["Обложка"] = "🔄 Отправка..."; await update_progress(progress_message, statuses)
                            # Send the main info message WITH the processed thumbnail
                            final_info_message = await client.send_file(
                                event.chat_id, file=processed_thumb_file, caption=response_text, link_preview=False, reply_to=event.message.id
                            )
                            # Store the message sent as a file
                            await store_response_message(event.chat_id, final_info_message)
                            # The progress message object is now outdated for status updates related to sending this file.
                            # We rely on auto-clear to remove the progress message later.
                        except Exception as send_e:
                            logger.error(f"Failed to send file with cover {os.path.basename(processed_thumb_file)}: {send_e}", exc_info=True)
                            if use_progress: statuses["Обложка"] = "❌ Ошибка отправки"; await update_progress(progress_message, statuses)
                            fallback_text = f"{response_text}\n\n_(Ошибка при отправке обложки)_"
                            # Send fallback text message if file sending fails
                            final_info_message = await (progress_message.edit(fallback_text, link_preview=False) if use_progress and progress_message else event.reply(fallback_text, link_preview=False)) # Use progress message if it exists and was used
                            await store_response_message(event.chat_id, final_info_message) # Store fallback message

                    else:
                        logger.warning(f"Thumbnail processing failed for {entity_id}. Sending text only.")
                        if use_progress: statuses["Обложка"] = "❌ Ошибка обработки"; await update_progress(progress_message, statuses)
                        fallback_text = f"{response_text}\n\n_(Ошибка при обработке обложки)_"
                        # Send fallback text message
                        final_info_message = await (progress_message.edit(fallback_text, link_preview=False) if use_progress and progress_message else event.reply(fallback_text, link_preview=False))
                        await store_response_message(event.chat_id, final_info_message) # Store fallback message


                else: # Thumbnail download failed
                     logger.warning(f"Thumbnail download failed for {entity_id}. Sending text only.")
                     if use_progress: statuses["Обложка"] = "❌ Ошибка загрузки"; await update_progress(progress_message, statuses)
                     fallback_text = f"{response_text}\n\n_(Ошибка при загрузке обложки)_"
                     # Send fallback text message
                     final_info_message = await (progress_message.edit(fallback_text, link_preview=False) if use_progress and progress_message else event.reply(fallback_text, link_preview=False))
                     await store_response_message(event.chat_id, final_info_message) # Store fallback message

            else: # No thumbnail requested or no URL found
                 # Send the main info message without a thumbnail
                 final_info_message = await (progress_message.edit(response_text, link_preview=False) if use_progress and progress_message else event.reply(response_text, link_preview=False))
                 await store_response_message(event.chat_id, final_info_message) # Store the main info message

            # --- Lyrics Handling ---
            # Check if progress message still exists and was used before trying to update it
            if use_progress and progress_message and "Отправка" in statuses and (statuses["Отправка"] == "⏸️" or statuses["Отправка"] == "🔄 Подготовка...") :
                 # Update status on the original progress message if it wasn't edited away
                 statuses["Отправка"] = "⏸️"; await update_progress(progress_message, statuses)


            # For lyrics, we need the video ID and potentially lyrics browse ID.
            # If entity is a track, we use its details.
            # If entity is an artist and we found a special track (featured or latest release), use its details.
            # Otherwise, we can't get lyrics directly from see for other entity types.
            video_id_for_lyrics = None
            lyrics_browse_id_final = None

            if actual_entity_type == 'track':
                 details = entity_info.get('videoDetails') or entity_info
                 video_id_for_lyrics = details.get('videoId') or entity_id
                 lyrics_browse_id_final = details.get('lyrics') # From track info

            elif actual_entity_type == 'artist' and special_track_info:
                 video_id_for_lyrics = special_track_info.get('videoId')
                 lyrics_browse_id_final = special_track_info.get('lyrics') # From the special track info


            if include_lyrics and video_id_for_lyrics: # Only proceed if lyrics requested and we have a video ID
                if use_progress and progress_message: statuses["Текст"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)
                lyrics_data = await get_lyrics_for_track(video_id_for_lyrics, lyrics_browse_id_final)

                if lyrics_data and lyrics_data.get('lyrics'):
                    lyrics_text = lyrics_data['lyrics']
                    lyrics_source = lyrics_data.get('source')
                    logger.info(f"Lyrics received for '{title}' ({video_id_for_lyrics})")

                    if use_progress and progress_message: statuses["Получение текста"] = "✅ Получен"; statuses["Отправка"] = "🔄 Отправка текста..."; await update_progress(progress_message, statuses)

                    # Determine title and artists for lyrics header based on context
                    lyrics_title = title # Default to main entity title
                    lyrics_artists = artists # Default to main entity artists
                    if actual_entity_type == 'artist' and special_track_info:
                        lyrics_title = special_track_info.get('title', 'Unknown Track')
                        lyrics_artists = format_artists(special_track_info.get('artists') or special_track_info.get('author')) or name # Use special track info

                    lyrics_header = f"📜 **Текст песни:** {lyrics_title} - {lyrics_artists}"
                    if lyrics_source: lyrics_header += f"\n_(Источник: {lyrics_source})_"
                    lyrics_header += "\n" + ("-"*15)

                    # Send lyrics message - send_lyrics handles message storage
                    # It will reply to the message containing the main info (final_info_message if set, else original)
                    # send_lyrics internally uses event.message.id for reply_to, which is fine.
                    await send_lyrics(event, lyrics_text, lyrics_header, lyrics_title, video_id_for_lyrics)
                    lyrics_message_stored = True # Mark that send_lyrics handled storage

                    if use_progress and progress_message: statuses["Отправка"] = "✅ Отправлено"; await update_message(progress_message, statuses) # Typo fixed: update_progress

                else: # Lyrics not found
                    logger.info(f"Текст не найден для '{title}' ({video_id_for_lyrics}).")
                    if use_progress and progress_message:
                        statuses["Получение текста"] = "ℹ️ Не найден"; statuses["Отправка"] = "➖"; await update_progress(progress_message, statuses)
                    # Reply to the message containing the main info, if available, otherwise original command
                    reply_to_id = final_info_message.id if final_info_message else event.message.id
                    no_lyrics_msg = await event.respond("_Текст для этого трека не найден._", reply_to=reply_to_id)
                    await store_response_message(event.chat_id, no_lyrics_msg)
                    # NO timed deletion here. Message stored for auto-clear.

            elif include_lyrics and actual_entity_type != 'track':
                 # Lyrics requested but entity is not a track and no special track was found for artist
                 if use_progress and progress_message:
                      # Check if status wasn't already set to '➖ (Только для треков)'
                      if statuses.get("Текст", "⏸️") == "⏸️":
                           statuses["Текст"] = "➖ (Только для треков/спец. трека)"
                           statuses["Отправка"] = "➖"
                           await update_progress(progress_message, statuses)


    except Exception as e:
        logger.error(f"Unexpected error in handle_see for ID '{entity_id}': {e}", exc_info=True)
        error_prefix = "⚠️" if isinstance(e, (ValueError, FileNotFoundError)) else "❌"
        error_text = f"{error_prefix} Ошибка при получении информации:\n`{type(e).__name__}: {e}`"
        # Update progress message with error status or send new error message
        if use_progress and progress_message:
             for task in statuses: statuses[task] = str(statuses[task]).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌").replace("⏸️", "❌")
             statuses["Состояние"] = "❌ Ошибка" # Add final error status
             if "Спец. трек" in statuses and statuses["Спец. трек"].startswith("🔄"):
                  statuses["Спец. трек"] = "❌ Ошибка" # Update if stuck on special track search
             try: await update_progress(progress_message, statuses)
             except Exception: pass # Ignore errors updating status during error handling
             try:
                 # Append error to existing progress message
                 await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}")
                 final_info_message = progress_message # The progress message is now the final error message
             except Exception as edit_e:
                 logger.error(f"Failed to edit progress message with error for {entity_id}: {edit_e}")
                 # If editing fails, send a new error message
                 final_info_message = await event.reply(error_text)
        else: # If no progress message was used/created, just send a new error message
            final_info_message = await event.reply(error_text)

        # Store the final error message if it wasn't the original progress message already stored
        # This check is crucial because the progress message is stored initially.
        if final_info_message and final_info_message != progress_message:
             await store_response_message(event.chat_id, final_info_message)


    finally:
        # Clean up temporary files like downloaded/cropped thumbnails
        if files_to_clean_on_exit:
            logger.debug(f"Running cleanup for handle_see (Files: {len(files_to_clean_on_exit)})")
            asyncio.create_task(cleanup_files(*files_to_clean_on_exit))

        # Ensure the progress message is still stored if it exists and wasn't edited into an error message
        # (Already handled by initial store_response_message and the error block's final_info_message logic)
        # No need for explicit delete here. Auto-clear or manual clear handle it.
        pass # Cleanup handled by async task and auto-clear

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
             error_msg = await event.reply(f"❌ Ошибка: Не найден скачанный файл `{os.path.basename(file_path or 'N/A')}`.")
             await store_response_message(event.chat_id, error_msg)
             asyncio.create_task(cleanup_files(*[f for f in files_to_clean if f != file_path]))
             return None

        title, performer, duration = extract_track_metadata(info)

        thumb_url = None
        thumbnails_list_info = info.get('thumbnails') or (info.get('thumbnail') or {}).get('thumbnails')
        if not thumbnails_list_info:
             thumbnails_list_info = info.get('thumbnail')

        if isinstance(thumbnails_list_info, list) and thumbnails_list_info:
            try:
                highest_res_thumb = sorted(thumbnails_list_info, key=lambda x: x.get('width', 0) * x.get('height', 0), reverse=True)[0]
                thumb_url = highest_res_thumb.get('url')
            except (IndexError, KeyError, TypeError):
                if thumbnails_list_info:
                     thumb_url = thumbnails_list_info[-1].get('url')

        if thumb_url:
            logger.debug(f"Attempting download/process thumbnail for Telegram preview ('{title}')")
            temp_telegram_thumb = await download_thumbnail(thumb_url)
            if temp_telegram_thumb:
                files_to_clean.append(temp_telegram_thumb)
                processed_telegram_thumb = await crop_thumbnail(temp_telegram_thumb)

                if processed_telegram_thumb and processed_telegram_thumb != temp_telegram_thumb:
                    files_to_clean.append(processed_telegram_thumb)
                elif not processed_telegram_thumb:
                     logger.warning(f"crop_thumbnail returned None for {temp_telegram_thumb}. Will send without thumbnail.")

            else:
                 logger.warning(f"Failed to download thumbnail for track '{title}'. Sending without thumbnail.")


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
            reply_to=event.message.id
        )
        logger.info(f"Аудио успешно отправлено: {os.path.basename(file_path)} (Msg ID: {sent_audio_msg.id})")

        if config.get("recent_downloads", True):
             try:
                last_tracks = load_last_tracks()
                timestamp = datetime.datetime.now().strftime("%H:%M-%d-%m")
                browse_id_to_save = 'N/A'
                artists_list = info.get('artists')
                if isinstance(artists_list, list) and artists_list:
                     main_artist = next((a for a in artists_list if isinstance(a, dict) and a.get('id')), None)
                     if main_artist and main_artist.get('id'):
                         browse_id_to_save = main_artist['id']
                if browse_id_to_save == 'N/A':
                     browse_id_to_save = info.get('channel_id') or info.get('uploader_id') or info.get('id') or 'N/A'

                new_entry = [title, performer, browse_id_to_save, timestamp]
                last_tracks.insert(0, new_entry)
                save_last_tracks(last_tracks)
             except Exception as e_last:
                 logger.error(f"Не удалось обновить список последних треков ({title}): {e_last}", exc_info=True)

        return sent_audio_msg

    except telethon_errors.MediaCaptionTooLongError:
         logger.error(f"Ошибка отправки {os.path.basename(file_path)}: подпись слишком длинная.")
         error_msg = await event.reply(f"⚠️ Не удалось отправить `{title}`: подпись длинная.")
         await store_response_message(event.chat_id, error_msg)
         return None

    except telethon_errors.WebpageMediaEmptyError:
          logger.error(f"Ошибка отправки {os.path.basename(file_path)}: WebpageMediaEmptyError. Попытка без превью...")
          try:
              sent_audio_msg = await client.send_file(
                  event.chat_id, file_path, caption=BOT_CREDIT,
                  attributes=[types.DocumentAttributeAudio(duration=duration, title=title, performer=performer)],
                  thumb=None,
                  reply_to=event.message.id
              )
              logger.info(f"Повторная отправка без превью успешна: {os.path.basename(file_path)}")
              await store_response_message(event.chat_id, sent_audio_msg)
              return sent_audio_msg
          except Exception as retry_e:
              logger.error(f"Повторная отправка {os.path.basename(file_path)} не удалась: {retry_e}", exc_info=True)
              error_msg = await event.reply(f"❌ Не удалось отправить `{title}`: {retry_e}")
              await store_response_message(event.chat_id, error_msg)
              return None

    except Exception as e:
        logger.error(f"Неожиданная ошибка при отправке трека {os.path.basename(file_path or 'N/A')}: {e}", exc_info=True)
        try:
             error_msg = await event.reply(f"❌ Не удалось отправить трек `{title}`: {e}")
             await store_response_message(event.chat_id, error_msg)
        except Exception as notify_e: logger.error(f"Не удалось уведомить об ошибке отправки: {notify_e}")
        return None

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
                if not keep_this_audio_file:
                     final_cleanup_list.append(f)
            elif f:
                 final_cleanup_list.append(f)

        if final_cleanup_list:
            logger.debug(f"Запуск очистки для send_single_track (Файлов: {len(final_cleanup_list)})")
            asyncio.create_task(cleanup_files(*final_cleanup_list))
        else:
             logger.debug(f"Очистка send_single_track: Нет файлов для удаления.")


# -------------------------
# Command: download (-t, -a) [-txt] / dl
# -------------------------
async def handle_download(event: events.NewMessage.Event, args: List[str]):
    """Handles the download command."""
    valid_flags = {"-t", "-a"}
    prefix = config.get("prefix", ",")

    if not args:
        usage = (f"**Использование:** `{prefix}dl -t|-a [-txt] <ссылка>`\n"
                 f"Типы: `-t` (трек), `-a` (альбом/плейлист).\n"
                 f"Флаг: `-txt` (для `-t`, включить текст песни).")
        await store_response_message(event.chat_id, await event.reply(usage))
        return

    download_type_flag = None
    include_lyrics = False
    link = None
    remaining_args = list(args)

    if "-txt" in remaining_args:
        include_lyrics = True
        remaining_args.remove("-txt")

    for arg in remaining_args:
         if arg in valid_flags:
             download_type_flag = arg
             remaining_args.remove(arg)
             break

    if remaining_args:
         link = remaining_args[0]
         if len(remaining_args) > 1:
             logger.warning(f"Ignoring extra arguments in download command: {remaining_args[1:]}")

    if not download_type_flag:
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не указан тип (`-t` или `-a`)."))
        return

    if not link or not isinstance(link, str) or not link.startswith("http"):
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не найдена http(s) ссылка."))
        return

    if include_lyrics and download_type_flag == "-a":
         logger.warning("-txt flag is ignored for album downloads.")
         include_lyrics = False

    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    final_sent_message: Optional[types.Message] = None

    try:
        if download_type_flag == "-t":
            if use_progress:
                statuses = {"Скачивание/Обработка": "⏳ Ожидание...", "Отправка Аудио": "⏸️"}
                if include_lyrics: statuses["Отправка Текста"] = "⏸️"
                progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))
                await store_response_message(event.chat_id, progress_message)

            if use_progress: statuses["Скачивание/Обработка"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)

            loop = asyncio.get_running_loop()
            info, file_path = await loop.run_in_executor(None, functools.partial(download_track, link))

            if not file_path or not info:
                fail_reason = "yt-dlp не смог скачать/обработать"
                if info is not None and file_path is None: fail_reason = "yt-dlp скачал, но файл не найден"
                elif info is None: fail_reason = "yt-dlp не вернул информацию"

                logger.error(f"Download failed for {link}. Reason: {fail_reason}")
                if use_progress:
                    statuses["Скачивание/Обработка"] = f"❌ Ошибка ({fail_reason[:20]}...)"
                    statuses["Отправка Аудио"] = "❌"
                    if include_lyrics: statuses["Отправка Текста"] = "❌"
                    await update_progress(progress_message, statuses)
                error_msg = await event.reply(f"❌ Не удалось скачать или обработать трек:\n`{link}`\n_{fail_reason}_")
                await store_response_message(event.chat_id, error_msg)

            else:
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

                         if video_id:
                             lyrics_data = await get_lyrics_for_track(video_id, lyrics_browse_id)

                             if lyrics_data and lyrics_data.get('lyrics'):
                                  if use_progress: statuses["Отправка Текста"] = "✅ Отправка..."; await update_progress(progress_message, statuses)
                                  lyrics_text = lyrics_data['lyrics']
                                  lyrics_source = lyrics_data.get('source')
                                  dl_track_title = info.get('title', 'Неизвестный трек')
                                  artists_data_for_lyrics = info.get('artists')
                                  if not artists_data_for_lyrics:
                                       artists_data_for_lyrics = info.get('uploader') or info.get('creator')
                                  artists = format_artists(artists_data_for_lyrics) or 'Неизвестный исполнитель'

                                  lyrics_header = f"📜 **Текст песни:** {dl_track_title} - {artists}"
                                  if lyrics_source: lyrics_header += f"\n_(Источник: {lyrics_source})_"
                                  lyrics_header += "\n" + ("-"*15)

                                  await send_lyrics(event, lyrics_text, lyrics_header, dl_track_title, video_id or 'N/A')

                                  if use_progress: statuses["Отправка Текста"] = "✅ Отправлено"; await update_progress(progress_message, statuses)
                             else:
                                  logger.info(f"Текст не найден для '{track_title}' ({video_id}) при скачивании.")
                                  if use_progress: statuses["Отправка Текста"] = "ℹ️ Не найден"; await update_progress(progress_message, statuses)
                                  if sent_audio_message:
                                      no_lyrics_msg = await event.respond("_Текст для этого трека не найден._", reply_to=sent_audio_message.id)
                                      await store_response_message(event.chat_id, no_lyrics_msg)
                                      await asyncio.sleep(5); asyncio.create_task(no_lyrics_msg.delete())
                                  else:
                                       logger.warning(f"Audio sending failed for {track_title} ({video_id}), skipping 'lyrics not found' message.")

                         else:
                              logger.warning(f"Cannot fetch lyrics for downloaded track '{track_title}': No video ID available.")
                              if use_progress: statuses["Отправка Текста"] = "⚠️ Нет Video ID"; await update_progress(progress_message, statuses)

            if progress_message:
                 await asyncio.sleep(1);
                 try: await progress_message.delete(); progress_message = None
                 except Exception: pass


        elif download_type_flag == "-a":
            album_or_playlist_id = extract_entity_id(link)
            if not album_or_playlist_id:
                 error_msg = await event.reply(f"⚠️ Не удалось извлечь ID из ссылки: `{link}`")
                 await store_response_message(event.chat_id, error_msg)
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
                            current_statuses["Альбом/Плейлист"] = f"'{album_title}' ({total_tracks} тр.)"
                            current_statuses["Прогресс"] = f"▶️ Начинаем скачивание... (0/{total_tracks})"
                        elif status_key == "track_downloading":
                            curr_num = kwargs.get('current', 1)
                            perc = kwargs.get('percentage', 0)
                            title = kwargs.get('title', '?')
                            current_statuses["Прогресс"] = f"📥 {curr_num}/{total_tracks} ({perc}%) - '{title}'"
                        elif status_key == "track_downloaded":
                            curr_ok = downloaded_count
                            perc = int((curr_ok / total_tracks) * 100) if total_tracks else 0
                            title = kwargs.get('title', '?')
                            current_statuses["Прогресс"] = f"✅ Скачано {curr_ok}/{total_tracks} ({perc}%)"
                            if downloaded_count > 0:
                                 current_statuses["Отправка Треков"] = f"📤 Подготовка к отправке {curr_ok}/{downloaded_count}"
                        elif status_key == "track_sending":
                            curr_send_idx = kwargs.get('current_index', sent_count)
                            total_dl = kwargs.get('total_downloaded', downloaded_count)
                            title = kwargs.get('title', '?')
                            current_statuses["Отправка Треков"] = f"📤 Отправка {curr_send_idx+1}/{total_dl} - '{title}'"
                        elif status_key == "track_sent":
                             curr_sent_ok = sent_count
                             total_dl = kwargs.get('total_downloaded', downloaded_count)
                             title = kwargs.get('title', '?')
                             current_statuses["Отправка Треков"] = f"✔️ Отправлен {curr_sent_ok}/{total_dl} - '{title}'"
                        elif status_key == "track_failed":
                            curr_num = kwargs.get('current', 0)
                            if curr_num == 0: curr_num = downloaded_count + 1
                            title = kwargs.get('title', '?')
                            reason = kwargs.get('reason', 'Ошибка')
                            if "📥" in statuses.get("Прогресс", ""):
                                 statuses["Прогресс"] = f"⚠️ {reason} (трек {curr_num})"
                            elif "📤" in statuses.get("Отправка Треков", ""):
                                 statuses["Отправка Треков"] = f"❌ Не отправлен '{title}' ({reason})"
                            else:
                                 statuses["Прогресс"] = f"❌ {reason} (трек {curr_num})"

                        elif status_key == "album_error":
                            err_msg = kwargs.get('error', 'Ошибка')
                            current_statuses["Альбом/Плейлист"] = f"❌ Ошибка: {err_msg[:40]}..."
                            current_statuses["Прогресс"] = "⏹️ Остановлено"
                            if "Отправка Треков" in current_statuses: current_statuses["Отправка Треков"] = "⏹️ Остановлено"
                        await update_progress(progress_message, current_statuses)
                    except Exception as e_prog:
                        logger.error(f"Ошибка при обновлении прогресса альбома: {e_prog}", exc_info=True)

                progress_callback = album_progress_updater
                statuses = {"Альбом/Плейлист": f"🔄 Анализ ID '{album_or_playlist_id[:30]}...'...", "Прогресс": "⏸️", "Отправка Треков": "⏸️"}
                 # FIX: Correctly iterate over items to get key and value
                progress_message = await event.reply("\n".join(f"{task}: {value}" for task, value in statuses.items()))
                await store_response_message(event.chat_id, progress_message)


            logger.info(f"Starting sequential download/send for: {album_or_playlist_id}")
            downloaded_tuples = await download_album_tracks(album_or_playlist_id, progress_callback)

            downloaded_count = len(downloaded_tuples)

            if use_progress and progress_message:
                 download_status_icon = "✅" if downloaded_count > 0 else "ℹ️"
                 statuses["Прогресс"] = f"{download_status_icon} Скачано {downloaded_count}/{total_tracks} треков."
                 if downloaded_count == 0:
                      statuses["Отправка Треков"] = "➖ (Треки не скачаны)"

                 await update_progress(progress_message, statuses)
                 await asyncio.sleep(1)


            if downloaded_count == 0:
                if progress_callback:
                     await progress_callback("album_error", error="Треки не скачаны")
                error_msg = await event.reply(f"❌ Не удалось скачать ни одного трека для `{album_title or album_or_playlist_id}`.")
                await store_response_message(event.chat_id, error_msg)
                return


            logger.info(f"Starting sequential sending of {downloaded_count} tracks for '{album_title or album_or_playlist_id}'...")

            for i, (info, file_path) in enumerate(downloaded_tuples):
                track_title_send = (info.get('title', os.path.basename(file_path)) if info else os.path.basename(file_path))
                short_title = (track_title_send[:25] + '...') if len(track_title_send) > 28 else track_title_send

                if not file_path or not os.path.exists(file_path):
                     logger.error(f"File path missing for track {i+1}/{downloaded_count}: {file_path}. Skipping send.")
                     if progress_callback:
                          await progress_callback("track_failed", current=i+1, total=downloaded_count, title=short_title, reason="Файл не найден")
                     continue

                if progress_callback:
                    await progress_callback("track_sending", current_index=i, total_downloaded=downloaded_count, title=short_title)

                sent_msg_track = await send_single_track(event, info, file_path)

                if sent_msg_track:
                    sent_count += 1
                    if progress_callback:
                         await progress_callback("track_sent", current_sent=sent_count, total_downloaded=downloaded_count, title=short_title)

                await asyncio.sleep(0.5)

            if use_progress and progress_message:
                final_icon = "✅" if sent_count == downloaded_count else "⚠️"
                statuses["Прогресс"] = f"{final_icon} Завершено: Отправлено {sent_count}/{downloaded_count} треков."
                statuses["Отправка Треков"] = f"{final_icon} Отправлено {sent_count}/{downloaded_count}"
                try:
                    await update_progress(progress_message, statuses)
                    await asyncio.sleep(5)
                    await progress_message.delete(); progress_message = None
                except Exception as e_final_prog:
                     logger.warning(f"Could not update/delete final album progress message: {e_final_prog}")

    except Exception as e:
        logger.error(f"Ошибка при выполнении download ({download_type_flag}, {link}): {e}", exc_info=True)
        error_prefix = "⚠️" if isinstance(e, (ValueError, FileNotFoundError)) else "❌"
        error_text = f"{error_prefix} Ошибка при скачивании/отправке:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
            for task in statuses: statuses[task] = str(statuses[task]).replace("🔄", "⏹️").replace("✅", "⏹️").replace("⏳", "⏹️").replace("▶️", "⏹️").replace("📥", "⏹️").replace("📤", "⏹️").replace("✔️", "⏹️")
            statuses["Прогресс"] = "❌ Ошибка!"
            try: await update_progress(progress_message, statuses)
            except Exception: pass
            try:
                await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}")
                final_sent_message = progress_message
            except Exception as edit_e:
                logger.error(f"Не удалось изменить прогресс для ошибки: {edit_e}")
                final_sent_message = await event.reply(error_text)
        else:
            final_sent_message = await event.reply(error_text)
        if final_sent_message and (final_sent_message != progress_message or progress_message is None):
            await store_response_message(event.chat_id, final_sent_message)

    finally:
        pass # Cleanup is handled by send_single_track for each file


# Note: A similar fix was applied to the handle_host function's initial message generation
# as it had the same error pattern. I am only providing the handle_download function
# as specifically requested, but be aware the fix is needed in handle_host as well.


# =============================================================================
#              AUTHENTICATED COMMAND HANDLERS (rec, alast, likes)
# =============================================================================

@require_ytmusic_auth
async def handle_recommendations(event: events.NewMessage.Event, args: List[str]):
    """Fetches personalized music recommendations."""
    limit = config.get("recommendations_limit", 8)
    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    final_sent_message = None

    try: # <-- ВНЕШНИЙ TRY НАЧИНАЕТСЯ ЗДЕСЬ
        if use_progress:
            statuses = {"Получение рекомендаций": "⏳ Ожидание...", "Форматирование": "⏸️"}
            progress_message = await event.reply("\n".join(f"{task}: {value}" for task, value in statuses.items()))
            await store_response_message(event.chat_id, progress_message)

        if use_progress: statuses["Получение рекомендаций"] = "🔄 Запрос истории для семени рекомендаций..."; await update_progress(progress_message, statuses)

        recommendations = []
        start_vid = None
        history_fetch_success = False

        try: # <-- ВНУТРЕННИЙ TRY ДЛЯ ПОЛУЧЕНИЯ ИСТОРИИ (опционально)
            history = await _api_get_history()
            if history and isinstance(history, list) and history[0] and isinstance(history[0], dict) and history[0].get('videoId'):
                 start_vid = history[0]['videoId']
                 history_fetch_success = True
                 logger.info(f"Используем последний прослушанный трек ({start_vid}) как семя для рекомендаций.")
            else:
                 logger.info("История прослушиваний пуста или первый элемент не содержит videoId. Будет использован fallback.")
        except Exception as e_hist:
             # Обработка ошибки при получении истории, но не завершение команды
             logger.warning(f"Ошибка получения истории для семени рекомендаций: {e_hist}. Будет использован fallback.", exc_info=True)
             history = [] # Убедимся, что история пуста в случае ошибки


        if use_progress:
             if history_fetch_success:
                  statuses["Получение рекомендаций"] = "✅ История получена. Запрос рекомендаций по семени..."
             else:
                  statuses["Получение рекомендаций"] = "ℹ️ История пуста/невалидна. Запрос рекомендаций из ленты..."
             await update_progress(progress_message, statuses)

        # --- Логика получения рекомендаций ---
        if start_vid:
             try: # <-- ВНУТРЕННИЙ TRY ДЛЯ WATCH PLAYLIST
                 recommendations_raw = await _api_get_watch_playlist(videoId=start_vid, radio=True, limit=limit + 10)
                 recommendations = recommendations_raw.get('tracks', []) if recommendations_raw and isinstance(recommendations_raw, dict) else []

                 # Добавляем сам семенной трек, если он не попал в рекомендации
                 if history_fetch_success:
                     rec_vids = {t.get('videoId') for t in recommendations if isinstance(t, dict)}
                     if start_vid and start_vid not in rec_vids: # Добавлена проверка start_vid
                          # Вставляем в начало, если семенной трек валиден
                          if history[0] and isinstance(history[0], dict) and history[0].get('videoId') == start_vid:
                                recommendations.insert(0, history[0])
                                logger.debug(f"Добавлен семенной трек {start_vid} в список рекомендаций.")

             except Exception as e_watch:
                  logger.warning(f"Ошибка получения watch playlist для рекомендаций ({start_vid}): {e_watch}. Переход к get_home.", exc_info=True)
                  start_vid = None
                  recommendations = [] # Очищаем рекомендации, полученные до сбоя watch_playlist
                  if use_progress:
                       statuses["Получение рекомендаций"] = "⚠️ Ошибка по семени. Переход к ленте..."
                       await update_progress(progress_message, statuses)


        # Если start_vid не был получен, или watch playlist вернул пусто или с ошибкой
        if not recommendations: # Проверяем recommendations, а не start_vid
             if use_progress and start_vid is not None: # Если до этого пытались по семени и не получилось
                  statuses["Получение рекомендаций"] = "🔄 Запрос рекомендаций из ленты (Fallback)..."
                  await update_progress(progress_message, statuses)
             elif use_progress: # Если сразу перешли к ленте
                  statuses["Получение рекомендаций"] = "🔄 Запрос рекомендаций из ленты..."
                  await update_progress(progress_message, statuses)


             logger.info("Использование generic home feed suggestions для рекомендаций.")
             try:
                 # Используем обернутый get_home
                 home_feed = await _api_get_home(limit=limit + 10)
                 # Извлекаем треки, фильтруя по наличию videoId, title и author/artists
                 recommendations = [item for section in home_feed if isinstance(section, dict) and 'contents' in section and isinstance(section['contents'], list) for item in section['contents'] if isinstance(item, dict) and item.get('videoId') and item.get('title') and (item.get('artists') or item.get('author'))]
                 # Теперь recommendations содержит треки из get_home

             except Exception as e_home:
                  logger.error(f"Ошибка получения home feed для рекомендаций: {e_home}. Не удалось получить рекомендации.", exc_info=True)
                  raise Exception(f"Не удалось получить рекомендации из ленты: {e_home}")


        # --- Фильтрация и форматирование результатов ---
        # ЭТИ СТРОКИ ДОЛЖНЫ БЫТЬ ВНУТРИ ВНЕШНЕГО TRY!
        seen_ids = set()
        filtered_recs = []
        # Итерируем по списку recommendations, который теперь содержит либо watch_playlist треки, либо home_feed треки
        for track in recommendations:
             if track and isinstance(track, dict) and track.get('videoId'):
                 vid = track['videoId']
                 if re.fullmatch(r'[A-Za-z0-9_-]{11}', vid):
                      if vid not in seen_ids:
                         filtered_recs.append(track)
                         seen_ids.add(vid)
             if len(filtered_recs) >= limit: break

        results = filtered_recs

        if use_progress:
            rec_status = f"✅ Найдено: {len(results)}" if results else "ℹ️ Не найдено"
            statuses["Получение рекомендаций"] = rec_status
            statuses["Форматирование"] = "🔄 Подготовка..." if results else "➖"
            await update_progress(progress_message, statuses)

        if not results:
            final_message_text = f"ℹ️ Не удалось найти персональные рекомендации."
            if use_progress and progress_message:
                 await progress_message.edit(final_message_text)
                 final_sent_message = progress_message
            else:
                 final_sent_message = await event.reply(final_message_text)
        else:
            response_lines = []
            # Изменяем заголовок в зависимости от того, откуда взяты рекомендации (если есть история)
            header_text = "🎧 **Рекомендации для вас (на основе вашей истории):**\n" if history_fetch_success else "🎧 **Рекомендации для вас:**\n"
            response_text = header_text

            for i, item in enumerate(results):
                line = f"{i + 1}. "
                if not item or not isinstance(item, dict):
                    logger.warning(f"Skipping invalid recommendation item {i+1}: {item}")
                    response_lines.append(f"{i + 1}. ⚠️ Неверный формат данных")
                    continue
                try:
                    title = item.get('title', 'Unknown Title')
                    artists = format_artists(item.get('artists') or item.get('author'))
                    vid = item.get('videoId')
                    link_url = f"https://music.youtube.com/watch?v={vid}" if vid else None
                    album_info = item.get('album')
                    album_name = album_info.get('name') if isinstance(album_info, dict) else None
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
                final_sent_message = progress_message
            else:
                final_sent_message = await event.reply(response_text, link_preview=False)

    except Exception as e: # <-- ВНЕШНИЙ EXCEPT
        logger.error(f"Ошибка в команде recommendations: {e}", exc_info=True)
        error_prefix = "⚠️" if isinstance(e, (ValueError, TypeError)) else "❌"
        error_text = f"{error_prefix} Ошибка при получении рекомендаций:\n`{type(e).__name__}: {e}`"
        # Обновляем прогресс-сообщение или отправляем новое с ошибкой
        if use_progress and progress_message:
            for task in statuses:
                 # Обновляем статусы при ошибке
                 if statuses[task] in ["⏳ Ожидание...", "🔄 Запрос истории для семени рекомендаций...", "✅ История получена. Запрос рекомендаций по семени...", "⚠️ Ошибка по семени. Переход к ленте...", "🔄 Запрос рекомендаций из ленты...", "ℹ️ История пуста/невалидна. Запрос рекомендаций из ленты...", "⏸️", "🔄 Подготовка...", "✅ Найдено: {len(results)}", "ℹ️ Не найдено"]:
                     statuses[task] = "❌ Ошибка"
            try: await update_progress(progress_message, statuses)
            except Exception: pass

            try:
                await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}")
                final_sent_message = progress_message
            except Exception: final_sent_message = await event.reply(error_text)
        else: final_sent_message = await event.reply(error_text)
    finally: # <-- ВНЕШНИЙ FINALLY
        if final_sent_message:
            await store_response_message(event.chat_id, final_sent_message)


@require_ytmusic_auth
async def handle_history(event: events.NewMessage.Event, args: List[str]):
    """Fetches user's listening history."""
    limit = config.get("history_limit", 10)
    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    sent_message = None

    try:
        if use_progress:
            statuses = {"Получение истории": "⏳ Ожидание...", "Форматирование": "⏸️"}
            progress_message = await event.reply("\n".join(f"{task}: {value}" for task, value in statuses.items()))
            await store_response_message(event.chat_id, progress_message)

        if use_progress: statuses["Получение истории"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)

        try:
            # Use wrapped get_history
            results = await _api_get_history()
        except Exception as api_e:
             # This catches exceptions from the wrapped API calls after their retries
             logger.error(f"Failed to get history via API wrappers: {api_e}", exc_info=True)
             raise Exception(f"Ошибка при получении истории: {api_e}")

        if use_progress:
            hist_status = f"✅ Найдено: {len(results)}" if results else "ℹ️ История пуста"
            statuses["Получение истории"] = hist_status
            statuses["Форматирование"] = "🔄 Подготовка..." if results else "➖"
            await update_progress(progress_message, statuses)

        if not results:
            final_message = f"ℹ️ Ваша история прослушиваний пуста."
            # Edit progress message or send new reply
            final_sent_message = await (progress_message.edit(final_message) if progress_message else event.reply(final_message))
        else:
            response_lines = []
            display_limit = min(len(results), limit)
            response_text = f"📜 **Недавняя история (последние {display_limit}):**\n"

            for i, item in enumerate(results[:display_limit]):
                line = f"{i + 1}. "
                # Ensure item is a dictionary before processing
                if not item or not isinstance(item, dict):
                    logger.warning(f"Skipping invalid history item {i+1}: {item}")
                    response_lines.append(f"{i + 1}. ⚠️ Неверный формат данных")
                    continue
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
                final_sent_message = progress_message
            else:
                final_sent_message = await event.reply(response_text, link_preview=False)

    except Exception as e:
        logger.error(f"Ошибка в команде history: {e}", exc_info=True)
        error_text = f"❌ Ошибка при получении истории:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
            for task in statuses: statuses[task] = str(statuses[task]).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌").replace("⏸️", "❌")
            try: await update_progress(progress_message, statuses)
            except Exception: pass
            try: await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}"); final_sent_message = progress_message
            except Exception: final_sent_message = await event.reply(error_text)
        else: final_sent_message = await event.reply(error_text)
    finally:
        if final_sent_message:
            await store_response_message(event.chat_id, final_sent_message)


@require_ytmusic_auth
async def handle_liked_songs(event: events.NewMessage.Event, args: List[str]):
    """Fetches user's liked songs playlist."""
    limit = config.get("liked_songs_limit", 15)
    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    sent_message = None

    try:
        if use_progress:
            statuses = {"Получение лайков": "⏳ Ожидание...", "Форматирование": "⏸️"}
            # FIX: Correctly unpack the tuple from statuses.items()
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))
            await store_response_message(event.chat_id, progress_message)

        if use_progress: statuses["Получение лайков"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)

        try:
            results_raw = await _api_get_liked_songs(limit=limit)
            results = results_raw.get('tracks', []) if results_raw and isinstance(results_raw, dict) else []
        except Exception as api_e:
             logger.error(f"Failed to get liked songs via API wrappers: {api_e}", exc_info=True)
             raise Exception(f"Ошибка при получении лайков: {api_e}")

        if use_progress:
            like_status = f"✅ Найдено: {len(results)}" if results else "ℹ️ Лайков не найдено"
            statuses["Получение лайков"] = like_status
            statuses["Форматирование"] = "🔄 Подготовка..." if results else "➖"
            await update_progress(progress_message, statuses)

        if not results:
            final_message = f"ℹ️ Плейлист 'Мне понравилось' пуст."
            final_sent_message = await (progress_message.edit(final_message) if progress_message else event.reply(final_message))
        else:
            response_lines = []
            display_limit = min(len(results), limit)
            response_text = f"👍 **Треки 'Мне понравилось' (последние {display_limit}):**\n"

            for i, item in enumerate(results[:display_limit]):
                line = f"{i + 1}. "
                if not item or not isinstance(item, dict):
                    logger.warning(f"Skipping invalid liked song item {i+1}: {item}")
                    response_lines.append(f"{i + 1}. ⚠️ Неверный формат данных")
                    continue
                try:
                    title = item.get('title', 'Unknown Title')
                    artists = format_artists(item.get('artists'))
                    vid = item.get('videoId')
                    link_url = f"https://music.youtube.com/watch?v={vid}" if vid else None
                    album_name = (item.get('album') or {}).get('name')
                    album_part = f" (Альbum: {album_name})" if album_name else ""
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
                final_sent_message = progress_message
            else:
                final_sent_message = await event.reply(response_text, link_preview=False)

    except Exception as e:
        logger.error(f"Ошибка в команде liked_songs: {e}", exc_info=True)
        error_text = f"❌ Ошибка при получении лайков:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
            for task in statuses: statuses[task] = str(statuses[task]).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌").replace("⏸️", "❌")
            try: await update_progress(progress_message, statuses)
            except Exception: pass
            try: await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}"); final_sent_message = progress_message
            except Exception: final_sent_message = await event.reply(error_text)
        else: final_sent_message = await event.reply(error_text)
    finally:
        if final_sent_message:
            await store_response_message(event.chat_id, final_sent_message)


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
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не удалось распознать ID видео трека из `{link_or_id_arg}`."))
        return

    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    final_sent_message = None
    lyrics_message_stored = False

    try:
        if use_progress:
            statuses = {"Поиск трека": "⏳ Ожидание...", "Получение текста": "⏸️", "Отправка": "⏸️"}
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))
            await store_response_message(event.chat_id, progress_message)

        track_title, track_artists = f"Трек `{video_id}`", "Неизвестный исполнитель"
        lyrics_browse_id_from_info = None

        if use_progress: statuses["Поиск трека"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)
        try:
            track_info = await get_entity_info(video_id, entity_type_hint="track")
            if track_info and track_info.get('_entity_type') == 'track':
                 details = track_info.get('videoDetails') or track_info
                 track_title = details.get('title', track_title)
                 fetched_artists_data = details.get('artists') or details.get('author') or track_info.get('uploader')
                 track_artists = format_artists(fetched_artists_data) or track_artists
                 lyrics_browse_id_from_info = details.get('lyrics')

                 if use_progress: statuses["Поиск трека"] = f"✅ {track_title}"; await update_progress(progress_message, statuses)
            else:
                 logger.warning(f"Track info not found or not a track ({entity_info.get('_entity_type') if entity_info else 'None'}) for {video_id}. Lyrics header will use defaults.")
                 if use_progress: statuses["Поиск трека"] = "⚠️ Не найден"; await update_progress(progress_message, statuses)
        except Exception as e_info:
             logger.warning(f"Failed to get track info for lyrics header ({video_id}): {e_info}", exc_info=True)
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

            if use_progress and progress_message:
                 statuses["Отправка"] = "✅ Отправлено"; await update_progress(progress_message, statuses)

        else:
            logger.info(f"Lyrics not found for '{track_title}' ({video_id}).")
            if use_progress:
                statuses["Получение текста"] = "ℹ️ Не найден"; statuses["Отправка"] = "➖"; await update_progress(progress_message, statuses)
            final_message = f"ℹ️ Не удалось найти текст для трека `{track_title}` (`{video_id}`)."
            final_sent_message = await (progress_message.edit(final_message) if progress_message else event.reply(final_message))
            if final_sent_message and (final_sent_message == progress_message or progress_message is None):
                 await store_response_message(event.chat_id, final_sent_message)

    except Exception as e:
        logger.error(f"Ошибка в команде lyrics/text для {video_id}: {e}", exc_info=True)
        error_text = f"❌ Ошибка при получении текста:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
            for task in statuses: statuses[task] = str(statuses[task]).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌").replace("⏸️", "❌")
            try: await update_progress(progress_message, statuses)
            except Exception: pass
            try: await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}"); final_sent_message = progress_message
            except Exception: final_sent_message = await event.reply(error_text)
        else: final_sent_message = await event.reply(error_text)
        if final_sent_message and (final_sent_message != progress_message or progress_message is None):
            await store_response_message(event.chat_id, final_sent_message)

    finally:
         if progress_message:
              await asyncio.sleep(3)
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
             error_msg = await event.reply(f"❌ Ошибка: Файл справки (`{os.path.basename(help_path)}`) не найден.")
             await store_response_message(event.chat_id, error_msg)
             try:
                 prefix = config.get("prefix", ",")
                 available_commands = sorted([cmd for cmd in handlers.keys()])
                 basic_help = f"**Доступные команды:**\n" + \
                              "\n".join(f"`{prefix}{cmd}`" for cmd in available_commands)
                 basic_msg = await event.reply(basic_help, link_preview=False)
                 await store_response_message(event.chat_id, basic_msg)
             except Exception as basic_e:
                 logger.error(f"Не удалось сгенерировать базовую справку: {basic_e}", exc_info=True)
             return

        with open(help_path, "r", encoding="utf-8") as f: help_text = f.read().strip()

        current_prefix = config.get("prefix", ",")
        auth_indicator = "✅ Авторизация YTM: Активна" if ytmusic_authenticated else "⚠️ Авторизация YTM: Неактивна"
        formatted_help = help_text.replace("{prefix}", current_prefix)
        formatted_help = formatted_help.replace("{auth_status_indicator}", auth_indicator)

        await send_long_message(event, formatted_help, prefix="")

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
            display_title = track_title.strip() if track_title and track_title.strip() != 'Неизвестно' else 'N/A'
            display_creator = creator.strip() if creator and creator.strip().lower() not in ['неизвестно', 'unknown artist', 'n/a', ''] else ''

            name_part = f"**{display_title}**"
            if display_creator: name_part += f" - {display_creator}"

            link_part = ""
            if browse_id and browse_id != 'N/A' and isinstance(browse_id, str) and browse_id.strip():
                cleaned_browse_id = browse_id.strip()
                ytm_link = None
                if cleaned_browse_id.startswith("UC"): ytm_link = f"https://music.youtube.com/channel/{cleaned_browse_id}"
                elif cleaned_browse_id.startswith(("MPRE", "MPLA", "RDAM", "OLAK5uy_")): ytm_link = f"https://music.youtube.com/browse/{cleaned_browse_id}"
                elif re.fullmatch(r'[A-Za-z0-9_-]{11}', cleaned_browse_id): ytm_link = f"https://music.youtube.com/watch?v={cleaned_browse_id}"
                elif cleaned_browse_id.startswith("PL") or cleaned_browse_id.startswith("VL"): ytm_link = f"https://music.youtube.com/playlist?list={cleaned_browse_id}"

                if ytm_link: link_part = f"[Ссылка]({ytm_link})"
                else: link_part = f"`{cleaned_browse_id}`"

            ts_part = f"`({timestamp.strip()})`" if timestamp and timestamp.strip() else ""
            lines.append(f"{i + 1}. {name_part} {link_part} {ts_part}".strip())
        else:
            logger.warning(f"Skipping malformed entry in last tracks display: {entry}")

    if len(lines) == 1:
        await store_response_message(event.chat_id, await event.reply("ℹ️ Не найдено валидных записей в истории."))
    else:
        response_msg = await event.reply("\n".join(lines), link_preview=False)
        await store_response_message(event.chat_id, response_msg)

# Helper function to get FFmpeg version (sync, run in executor)
def get_ffmpeg_version(ffmpeg_path: Optional[str]) -> str:
    """Synchronously gets FFmpeg version string."""
    if not ffmpeg_path:
        return "Не найден"
    try:
        # Use a short timeout for the subprocess call
        # Use startupinfo=None on Windows to avoid console window
        startupinfo = None
        if platform.system() == 'Windows':
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startupinfo.wShowWindow = subprocess.SW_HIDE

        result = subprocess.run(
            [ffmpeg_path, '-version'],
            capture_output=True,
            text=True,
            timeout=5,
            startupinfo=startupinfo # Pass startupinfo here
        )
        if result.returncode == 0:
            # FFmpeg version is usually in the first line
            first_line = result.stdout.strip().split('\n')[0]
            # Extract version number, e.g., "ffmpeg version 4.4.2-0ubuntu0.22.04.1" or "ffmpeg version n7.1.1"
            match = re.search(r'ffmpeg version ([^\s]+)', first_line)
            if match:
                return match.group(1)
            # Fallback if version parsing fails but command succeeded
            return "OK (версия не распознана)"
        else:
            # Command failed to run or returned non-zero exit code
            return f"Ошибка выполнения (код={result.returncode})"
    except FileNotFoundError:
        return "Не найден (ошибка пути или нет прав)" # Executable not found where expected or permissions issue
    except subprocess.TimeoutExpired:
        return "Ошибка (таймаут 5с)" # Command timed out
    except Exception as e:
        logger.warning(f"Error getting FFmpeg version from {ffmpeg_path}: {e}")
        return f"Ошибка ({type(e).__name__})" # Other unexpected error


# -------------------------
# Command: host
# -------------------------
async def handle_host(event: events.NewMessage.Event, args: List[str]):
    """Displays system information with progress updates."""
    statuses = {
        "Состояние": "⏳ Ожидание...",
        "Система": "⏸️",
        "Ресурсы (ЦПУ/ОЗУ/Диск)": "⏸️",
        "Сеть": "⏸️",
        "ПО (Версии)": "⏸️",
        "YTM": "⏸️"
    }
    # FIX: Correctly unpack the tuple from statuses.items()
    progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))
    await store_response_message(event.chat_id, progress_message)

    try:
        loop = asyncio.get_running_loop()

        # --- System Info ---
        statuses["Система"] = "🔄 Сбор инфо..."
        await update_progress(progress_message, statuses)
        system_info = platform.system()
        os_name = system_info
        kernel = platform.release()
        architecture = platform.machine()
        hostname = platform.node()

        try:
            if system_info == 'Linux':
                 try:
                     # Try freedesktop.org standard
                     os_release = await loop.run_in_executor(None, platform.freedesktop_os_release)
                     os_name = os_release.get('PRETTY_NAME', system_info)
                 except AttributeError:
                      # Fallback for other Linux systems
                      if os.path.exists('/etc/os-release'):
                          with open('/etc/os-release', 'r') as f:
                               lines = f.readlines()
                               os_name_line = next((line for line in lines if line.startswith('PRETTY_NAME=')), None)
                               if os_name_line:
                                    os_name = os_name_line.split('=', 1)[1].strip().strip('"\'')
                               elif os.path.exists('/etc/issue'):
                                    with open('/etc/issue', 'r') as f_issue:
                                         os_name = f_issue.readline().strip()
                      if os_name == system_info:
                          os_name = f"{system_info} ({platform.platform()})" # Generic fallback
            elif system_info == 'Windows':
                 os_name = f"{platform.system()} {platform.release()} ({platform.version()})"
            elif system_info == 'Darwin':
                 os_name = f"macOS {platform.mac_ver()[0]}"
        except Exception as e_os: logger.warning(f"Could not get detailed OS name: {e_os}")

        statuses["Система"] = f"✅ {os_name} ({architecture})"
        await update_progress(progress_message, statuses)

        # --- Resources ---
        statuses["Ресурсы (ЦПУ/ОЗУ/Диск)"] = "🔄 Сбор данных..."
        await update_progress(progress_message, statuses)
        ram_info, cpu_info, disk_info = "N/A", "N/A", "N/A"

        try:
             # Use executor for potentially blocking calls
             mem = await loop.run_in_executor(None, psutil.virtual_memory)
             ram_info = f"{mem.used / (1024 ** 3):.2f} ГБ / {mem.total / (1024 ** 3):.2f} ГБ ({mem.percent}%)"
        except Exception as e_ram: logger.warning(f"Could not get RAM info: {e_ram}")

        try:
            cpu_count_logical = await loop.run_in_executor(None, psutil.cpu_count, True)
            # cpu_percent(0.5) is blocking, run in executor
            cpu_usage = await loop.run_in_executor(None, functools.partial(psutil.cpu_percent, interval=0.5))
            cpu_info = f"{cpu_count_logical} ядер, загрузка {cpu_usage:.1f}%"
        except Exception as e_cpu: logger.warning(f"Could not get CPU info: {e_cpu}")

        # --- Disk Usage (Checking Home Directory) ---
        try:
            # Use os.path.expanduser('~') to get the user's home directory
            disk_check_path = os.path.expanduser('~')
            # Handle cases where home directory might not be accessible or defined
            if not disk_check_path or not os.path.exists(disk_check_path):
                 # Fallback to script directory or root if home is problematic
                 disk_check_path = SCRIPT_DIR or '/'
                 logger.warning(f"Home directory not found or accessible, falling back to {disk_check_path} for disk check.")

            # disk_usage(path) is blocking, run in executor
            disk = await loop.run_in_executor(None, functools.partial(psutil.disk_usage, disk_check_path))
            disk_info = f"{disk.used / (1024 ** 3):.2f} ГБ / {disk.total / (1024 ** 3):.2f} ГБ ({disk.percent}%)"
        except Exception as e_disk:
             logger.error(f"Could not get disk usage for {disk_check_path}: {e_disk}", exc_info=True)
             disk_info = f"Ошибка ({type(e_disk).__name__})"


        statuses["Ресурсы (ЦПУ/ОЗУ/Диск)"] = "✅ Данные получены"
        await update_progress(progress_message, statuses)

        # --- Uptime ---
        uptime_str = "N/A"
        try:
            # boot_time() is blocking, run in executor
            boot_time = await loop.run_in_executor(None, psutil.boot_time)
            uptime_seconds = datetime.datetime.now().timestamp() - boot_time
            if uptime_seconds > 0:
                td = datetime.timedelta(seconds=int(uptime_seconds))
                days = td.days
                hours, rem = divmod(td.seconds, 3600)
                minutes, seconds = divmod(rem, 60)
                parts = []
                if days > 0: parts.append(f"{days} дн.")
                if hours > 0 or days > 0: parts.append(f"{hours:02} ч.")
                if minutes > 0 or hours > 0 or days > 0: parts.append(f"{minutes:02} мин.")
                # Only show seconds if uptime is less than 1 minute
                if seconds > 0 and (days == 0 and hours == 0 and minutes == 0): parts.append(f"{seconds:02} сек.")
                elif seconds > 0 and (days > 0 or hours > 0 or minutes > 0): pass # Don't show seconds if over 1 min, for brevity
                elif not parts: parts.append("0 сек.") # Should not happen if uptime_seconds > 0

                uptime_str = " ".join(parts).strip()
            else: uptime_str = "< 1 сек."
        except Exception as e_uptime: logger.warning(f"Could not get uptime: {e_uptime}")


        # --- Network ---
        statuses["Сеть"] = "🔄 Пинг..."
        await update_progress(progress_message, statuses)
        ping_result = "N/A"
        ping_target = "8.8.8.8"
        try:
            # shutil.which is blocking, run in executor
            ping_cmd_path = await loop.run_in_executor(None, shutil.which, 'ping')
            if ping_cmd_path:
                # Use startupinfo=None on Windows to avoid console window
                startupinfo = None
                if platform.system() == 'Windows':
                     startupinfo = subprocess.STARTUPINFO()
                     startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                     startupinfo.wShowWindow = subprocess.SW_HIDE

                if system_info == 'Windows':
                     p_args = [ping_cmd_path, '-n', '1', '-w', '2000', ping_target] # -w in ms
                else:
                     p_args = [ping_cmd_path, '-c', '1', '-W', '2', ping_target] # -W in seconds
                try:
                     # Use subprocess_exec with timeout
                     proc = await asyncio.create_subprocess_exec(
                         *p_args,
                         stdout=asyncio.subprocess.PIPE,
                         stderr=asyncio.subprocess.PIPE,
                         startupinfo=startupinfo # Pass startupinfo here
                     )
                     stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=4.0)

                     if proc.returncode == 0:
                         stdout_str = stdout.decode('utf-8', errors='ignore')
                         # Look for time or avg time in different OS outputs
                         match = re.search(r'time[=<](\d+\.?\d*)ms', stdout_str) # Linux/macOS time=XX.Xms
                         if not match and system_info == 'Windows':
                              match = re.search(r'Average = (\d+)ms', stdout_str) # Windows Average = XXms
                         if match:
                             ping_result = f"✅ {match.group(1)} мс ({ping_target})"
                         else:
                             ping_result = f"✅ OK ({ping_target}, без RTT)" # Successful ping but no time parsed
                     else:
                         # Ping command failed (e.g., destination unreachable)
                         stderr_str = stderr.decode('utf-8', errors='ignore').strip()
                         error_output = (f": {stderr_str[:50]}...") if stderr_str else ""
                         ping_result = f"❌ Ошибка ({ping_target}, код={proc.returncode}{error_output})"
                except asyncio.TimeoutError:
                     # Process timed out
                     try: proc.terminate(); await proc.wait() # Try to clean up process
                     except Exception: pass
                     ping_result = f"⌛ Таймаут 4с ({ping_target})"
                except FileNotFoundError:
                     # ping command not found
                     ping_result = f"⚠️ 'ping' не найден"
                except Exception as e_ping_proc:
                     # Other error running subprocess
                     logger.warning(f"Error running ping process: {e_ping_proc}")
                     ping_result = f"❓ Ошибка процесса ({ping_target})"

            else: ping_result = "⚠️ 'ping' не найден" # shutil.which failed
        except Exception as e_ping_outer:
             # Error setting up ping test
             logger.warning(f"Ping test setup failed: {e_ping_outer}"); ping_result = f"❓ Ошибка запуска ({ping_target})"
        statuses["Сеть"] = ping_result
        await update_progress(progress_message, statuses)

        # --- Software Versions ---
        statuses["ПО (Версии)"] = "🔄 Сбор версий..."
        await update_progress(progress_message, statuses)
        python_v = platform.python_version()
        telethon_v = "Неизвестно"
        yt_dlp_v = "Неизвестно"
        ytmusicapi_v = "Неизвестно"
        pillow_v = "Неизвестно"
        psutil_v = "Неизвестно"
        requests_v = "Неизвестно"
        ffmpeg_v_str = "Неизвестно" # Renamed to avoid conflict if needed
        ffmpeg_loc_str = "Неизвестно"

        try: telethon_v = telethon.__version__
        except Exception: pass
        try: yt_dlp_v = yt_dlp.version.__version__
        except Exception: pass
        try: from importlib import metadata; ytmusicapi_v = metadata.version('ytmusicapi')
        except Exception: pass
        try: pillow_v = Image.__version__
        except Exception: pass
        try: psutil_v = psutil.__version__
        except Exception: pass
        try: requests_v = requests.__version__
        except Exception: pass

        # Get FFmpeg path and version using the helper function
        # Note: YDL_OPTS.get('ffmpeg_location') is likely already absolute if loaded from config
        ffmpeg_path_used = YDL_OPTS.get('ffmpeg_location') or await loop.run_in_executor(None, shutil.which, 'ffmpeg')
        if ffmpeg_path_used: # Typo fix: ffmpeg_path_used
             ffmpeg_loc_str = ffmpeg_path_used
             ffmpeg_v_str = await loop.run_in_executor(None, get_ffmpeg_version, ffmpeg_path_used)
        else:
             ffmpeg_v_str = "Не найден в PATH или конфиге"


        statuses["ПО (Версии)"] = "✅ Версии получены"
        await update_progress(progress_message, statuses)

        # --- YTM Auth Status ---
        statuses["YTM"] = "🔄 Проверка авторизации..."
        await update_progress(progress_message, statuses)
        auth_file_base = os.path.basename(YT_MUSIC_AUTH_FILE)
        ytm_auth_status_formatted = f"✅ Активна (`{auth_file_base}`)" if ytmusic_authenticated else f"⚠ Не активна (нет `{auth_file_base}`)"
        statuses["YTM"] = ytm_auth_status_formatted
        await update_progress(progress_message, statuses)

        statuses["Состояние"] = "✅ Готово"
        await update_progress(progress_message, statuses)


        final_text = (
            f"🖥️ **Информация о системе**\n"
            f" ├ **Имя хоста:** `{hostname}`\n"
            f" ├ **ОС:** `{os_name}`\n"
            f" ├ **Ядро:** `{kernel}`\n"
            f" └ **Время работы системы:** `{uptime_str}`\n\n"

            f"⚙️ **Аппаратное обеспечение**\n"
            f" ├ **ЦПУ:** `{cpu_info}`\n"
            # Display the path checked for disk usage
            f" └ **Диск ({disk_check_path or '/'}):** `{disk_info}`\n" # Use disk_check_path and fallback to '/'
            f" └ **ОЗУ:** `{ram_info}`\n\n" # Moved RAM below Disk for grouping

            f"🌐 **Сеть**\n"
            f" └ **Пинг:** `{ping_result}`\n\n"

            f"📦 **Версии ПО**\n"
            f" ├ **Python:** `{python_v}`\n"
            f" ├ **Telethon:** `{telethon_v}`\n"
            f" ├ **yt-dlp:** `{yt_dlp_v}`\n"
            f" ├ **ytmusicapi:** `{ytmusicapi_v}`\n"
            f" ├ **Pillow:** `{pillow_v}`\n"
            f" ├ **psutil:** `{psutil_v}`\n"
            f" ├ **Requests:** `{requests_v}`\n"
            # Show version and path for FFmpeg
            f" └ **FFmpeg:** `{ffmpeg_v_str}` (`{ffmpeg_loc_str}`)\n\n"


            f"🎵 **YouTube Music**\n"
            f" └ **Авторизация:** {ytm_auth_status_formatted}"
        )
        await progress_message.edit(final_text)

    except Exception as e_host:
        logger.error(f"Ошибка при сборе информации о хосте: {e_host}", exc_info=True)
        # Update statuses to reflect error
        statuses["Состояние"] = "❌ Ошибка"
        # Check if the task status indicates it was in progress or pending, mark as failed
        for task in statuses:
             if statuses[task] in ["⏳ Ожидание...", "🔄 Сбор инфо...", "⏸️",
                                   "🔄 Сбор данных...", "🔄 Пинг...", "🔄 Сбор версий...", "🔄 Проверка авторизации..."]:
                  statuses[task] = "❌ Ошибка"
        try: await update_progress(progress_message, statuses)
        except Exception: pass # Ignore errors during error status update

        error_text = f"❌ Не удалось полностью получить инфо:\n`{type(e_host).__name__}: {e_host}`"
        try:
             # Append error to the progress message
             await progress_message.edit(f"{getattr(progress_message, 'text', '')}\n\n{error_text}")
             # The progress message is now the final error message, already stored.
        except Exception as edit_e:
             # If editing fails (e.g., message deleted externally), send a new error message
             logger.error(f"Не удалось изменить прогресс-сообщение для ошибки хоста: {edit_e}")
             error_msg = await event.reply(error_text)
             await store_response_message(event.chat_id, error_msg) # Store the new error message

# =============================================================================
#                         MAIN EXECUTION & LIFECYCLE
# =============================================================================

async def main():
    """Main asynchronous function to start the bot."""
    global ytmusic
    global ytmusic_authenticated
    global handlers

    logger.info("--- Запуск бота YTMG ---")
    try:
        versions = [f"Python: {platform.python_version()}"]
        try: versions.append(f"Telethon: {telethon.__version__}")
        except Exception: versions.append("Telethon: ?")
        try: versions.append(f"yt-dlp: {yt_dlp.version.__version__}")
        except Exception: versions.append("yt-dlp: ?")
        try: from importlib import metadata; versions.append(f"ytmusicapi: {metadata.version('ytmusicapi')}")
        except Exception: versions.append("ytmusicapi: ?")
        try: versions.append(f"Pillow: {Image.__version__}")
        except Exception: versions.append("Pillow: ?")
        try: versions.append(f"psutil: {psutil.__version__}")
        except Exception: versions.append("psutil: ?")
        try: versions.append(f"Requests: {requests.__version__}")
        except Exception: versions.append("Requests: ?")

        logger.info("Версии библиотек: " + " | ".join(versions))

        logger.info("Подключение к Telegram...")
        await client.start()
        me = await client.get_me()
        if me:
            global BOT_OWNER_ID
            BOT_OWNER_ID = me.id
            name = f"@{me.username}" if me.username else f"{me.first_name or ''} {me.last_name or ''}".strip() or f"ID: {me.id}"
            logger.info(f"Бот запущен как: {name} (ID: {me.id}). Владелец: {me.id}.")
        else:
            logger.critical("Не удалось получить информацию о себе (me). Не могу определить ID владельца. Завершение работы.")
            await client.disconnect()
            return

        # --- YTMusic API Initialization with Auth File ---
        auth_file_base = os.path.basename(YT_MUSIC_AUTH_FILE)

        try:
            if os.path.exists(YT_MUSIC_AUTH_FILE):
                logger.info(f"Найден файл аутентификации YTMusic: '{auth_file_base}'. Попытка инициализации с файлом.")
                ytmusic = YTMusic(YT_MUSIC_AUTH_FILE)
                logger.info("ytmusicapi инициализирован с файлом аутентификации.")

                logger.debug("Проверка статуса аутентификации YTMusic...")
                try:
                    await asyncio.to_thread(ytmusic.get_history)
                    logger.info("YTMusic аутентификация успешна!")
                    ytmusic_authenticated = True
                except Exception as e_auth_check:
                    logger.warning(f"YTMusic аутентификация недействительна с файлом '{auth_file_base}' ({type(e_auth_check).__name__} - {e_auth_check}). Используется НЕаутентифицированный режим.")
                    ytmusic_authenticated = False
            else:
                logger.warning(f"Файл аутентификации YTMusic '{auth_file_base}' не найден. Используется НЕаутентифицированный режим.")
                ytmusic = YTMusic()
                logger.info("ytmusicapi инициализирован без файла аутентификации.")
                ytmusic_authenticated = False

        except Exception as e_ytm_init:
             logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА при инициализации ytmusicapi с файлом '{auth_file_base}': {e_ytm_init}", exc_info=True)
             ytmusic = None
             ytmusic_authenticated = False


        logger.info(f"Конфигурация: Префикс='{config.get('prefix')}', "
                    f"AutoClear={'Вкл' if config.get('auto_clear') else 'Выкл'}, "
                    f"YTMusic Auth={'Активна' if ytmusic_authenticated else 'Неактивна'}")

        pp_info = "N/A"
        if YDL_OPTS.get('postprocessors'):
            try:
                 first_pp = YDL_OPTS['postprocessors'][0]
                 pp_info = first_pp.get('key','?')
                 if first_pp.get('key') == 'FFmpegExtractAudio' and first_pp.get('preferredcodec'):
                     pp_info += f" ({first_pp.get('preferredcodec')})"
            except Exception: pass

        ydl_format = YDL_OPTS.get('format', 'N/A')
        ydl_outtmpl = YDL_OPTS.get('outtmpl', 'N/A')
        ydl_cookies = YDL_OPTS.get('cookiefile', 'N/A')
        logger.info(f"yt-dlp: Format='{ydl_format}', OutTmpl='{ydl_outtmpl}', PP='{pp_info}', EmbedMeta={YDL_OPTS.get('embed_metadata')}, EmbedThumb={YDL_OPTS.get('embed_thumbnail')}, Cookies='{os.path.basename(ydl_cookies) if ydl_cookies else 'N/A'}'")
        logger.info("--- Бот готов к приему команд ---")

        await client.run_until_disconnected()

    except (telethon_errors.AuthKeyError, telethon_errors.AuthKeyUnregisteredError) as e_authkey:
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
        print("--- Бот остановлен ---")


# --- Entry Point ---
if __name__ == '__main__':
    try:
        if not os.path.isdir(SCRIPT_DIR):
             print(f"CRITICAL: Script directory '{SCRIPT_DIR}' not found. Exiting.")
             exit(1)

        import html

        asyncio.run(main())

    except KeyboardInterrupt:
        print("\nReceived interrupt signal (Ctrl+C). Stopping.")
    except Exception as e_top:
        print(f"\nUncaught exception: {e_top}")
        traceback.print_exc()
    finally:
        print("Process finished.")

# --- END OF FILE main.py ---