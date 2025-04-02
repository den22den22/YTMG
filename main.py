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
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple, Type, Union
from urllib.parse import urlparse  # Added for robust filename extraction

import psutil
import requests
import telethon
import yt_dlp  # Preferred way to import
from PIL import Image, UnidentifiedImageError
from telethon import TelegramClient, events, functions, types
from ytmusicapi import YTMusic

# --- Logging Setup ---
# Configures logging to output to both a file and the console.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        # Log file stored in the script's directory
        logging.FileHandler("bot_log.txt", mode='w', encoding='utf-8'),
        # Console output
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- Helper function for absolute paths ---
# Ensures that paths to data/config files are relative to the script's location,
# even if the script is run from a different directory.
def get_script_dir():
    """Returns the absolute path to the directory containing this script."""
    try:
        # Standard way to get script directory
        return os.path.dirname(os.path.abspath(__file__))
    except NameError:
        # Fallback for environments where __file__ might not be defined (e.g., some interactive sessions)
        logger.warning("__file__ not defined, using current working directory for data/config files.")
        return os.getcwd()

SCRIPT_DIR = get_script_dir()

# =============================================================================
#                            CONFIGURATION LOADING
# =============================================================================

# --- Environment variables for Telegram API ---
# Critical credentials required to connect to Telegram.
TELEGRAM_API_ID = os.environ.get('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.environ.get('TELEGRAM_API_HASH')

# Exit if Telegram credentials are not provided via environment variables.
if not all([TELEGRAM_API_ID, TELEGRAM_API_HASH]):
    logger.critical("CRITICAL ERROR: Telegram API ID/Hash environment variables not set.")
    exit(1)

# --- Telegram client initialization ---
# Creates the Telethon client instance, using a session file for login persistence.
try:
    session_path = os.path.join(SCRIPT_DIR, 'telegram_session')
    # Ensure API ID is an integer
    client = TelegramClient(session_path, int(TELEGRAM_API_ID), TELEGRAM_API_HASH)
except ValueError:
    logger.critical("CRITICAL ERROR: TELEGRAM_API_ID must be an integer.")
    exit(1)
except Exception as e:
    logger.critical(f"CRITICAL ERROR: Failed to initialize TelegramClient: {e}")
    exit(1)

# --- yt-dlp Options ---
# Defines default options for yt-dlp and loads overrides from a config file.
# NOTE: Requires FFmpeg to be installed and in PATH for audio extraction and metadata/thumbnail embedding.
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

            # Start with defaults, update with loaded config values
            merged_opts = default_opts.copy()
            merged_opts.update(opts) # Loaded options override defaults

            # Ensure output template path is absolute
            if 'outtmpl' in merged_opts and not os.path.isabs(merged_opts['outtmpl']):
                 merged_opts['outtmpl'] = os.path.join(SCRIPT_DIR, merged_opts['outtmpl'])
                 logger.info(f"Made yt-dlp outtmpl absolute: {merged_opts['outtmpl']}")

            # --- FFmpeg Check ---
            # Check if FFmpeg is needed based on postprocessors or embedding flags
            needs_ffmpeg = any(pp.get('key', '').startswith('FFmpeg') for pp in merged_opts.get('postprocessors', [])) or \
                           merged_opts.get('embed_metadata') or \
                           merged_opts.get('embed_thumbnail')

            if needs_ffmpeg and not merged_opts.get('ffmpeg_location') and shutil.which('ffmpeg') is None:
                 logger.warning("FFmpeg is needed for audio extraction/embedding but not found in PATH and 'ffmpeg_location' is not set. These features might fail.")

            # Log final effective options for debugging if needed
            # logger.debug(f"Final yt-dlp options: {merged_opts}")
            return merged_opts

    except FileNotFoundError:
        logger.warning(f"yt-dlp config file '{absolute_config_path}' not found. Using default options.")
        # Check FFmpeg dependency for default options
        needs_ffmpeg_default = any(pp.get('key', '').startswith('FFmpeg') for pp in default_opts.get('postprocessors', [])) or \
                               default_opts.get('embed_metadata') or \
                               default_opts.get('embed_thumbnail')
        if needs_ffmpeg_default and shutil.which('ffmpeg') is None:
             logger.warning("FFmpeg is required for default audio extraction/embedding but not found in PATH. These features may fail.")
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding yt-dlp config file '{absolute_config_path}': {e}. Using default options.")
    except Exception as e:
        logger.error(f"Error loading yt-dlp config '{absolute_config_path}': {e}. Using default options.")

    # --- Return Defaults if Loading Failed ---
    # Ensure FFmpeg check is performed if FileNotFoundError wasn't the initial error
    if 'needs_ffmpeg_default' not in locals(): # Define if FileNotFoundError was not hit
        needs_ffmpeg_default = any(pp.get('key', '').startswith('FFmpeg') for pp in default_opts.get('postprocessors', [])) or \
                               default_opts.get('embed_metadata') or \
                               default_opts.get('embed_thumbnail')
    if needs_ffmpeg_default and shutil.which('ffmpeg') is None:
             logger.warning("FFmpeg is required for default audio extraction/embedding but not found in PATH. These features may fail.")

    return default_opts.copy()

# Re-initialize YDL_OPTS with the corrected function
YDL_OPTS = load_ydl_opts()

# --- Bot Configuration (UBOT.cfg) ---
# Loads bot behavior settings from a JSON file.
DEFAULT_CONFIG = {
    "prefix": ",",
    "progress_messages": True,      # Show step-by-step progress for commands
    "whitelist_enabled": True,      # Restrict usage to users in users.csv
    "auto_clear": True,             # Automatically delete previous bot responses on new commands
    "recent_downloads": True,       # Enable the ",last" command
    "bot_credit": f"via [YTMG](https://github.com/den22den22/YTMG/)",       # Caption added to sent media |  please do not change, thereby you will help the spread of the userbot
    "bot_enabled": True,            # Global enable/disable switch for the bot
    "default_search_limit": 8,      # How many results ytmusicapi should fetch
    "artist_top_songs_limit": 5,    # Max songs shown in ",see -e"
    "artist_albums_limit": 3,       # Max albums shown in ",see -e"
}

def load_config(config_file: str = 'UBOT.cfg') -> Dict:
    """Loads bot configuration from a JSON file, merging with defaults."""
    absolute_config_path = os.path.join(SCRIPT_DIR, config_file)
    logger.info(f"Attempting to load bot config from: {absolute_config_path}")
    try:
        with open(absolute_config_path, 'r', encoding='utf-8') as f:
            loaded_config = json.load(f)
            # Start with defaults, update with loaded values
            config = DEFAULT_CONFIG.copy()
            config.update(loaded_config)
            # Inform about any default keys that were missing and added
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
    # Return defaults if loading failed
    return DEFAULT_CONFIG.copy()

def save_config(config_to_save: Dict, config_file: str = 'UBOT.cfg'):
    """Saves the current bot configuration to a JSON file."""
    absolute_config_path = os.path.join(SCRIPT_DIR, config_file)
    logger.info(f"Attempting to save bot config to: {absolute_config_path}")
    try:
        with open(absolute_config_path, 'w', encoding='utf-8') as f:
            # Save with indentation for readability
            json.dump(config_to_save, f, indent=4, ensure_ascii=False)
        logger.info(f"Configuration saved to {absolute_config_path}")
    except Exception as e:
        logger.error(f"Error saving configuration to {absolute_config_path}: {e}")

# Load the configuration into a global variable
config = load_config()

# --- Constants derived from Config ---
BOT_CREDIT = config.get("bot_credit", "")
DEFAULT_SEARCH_LIMIT = config.get("default_search_limit", 8) # Renamed for clarity
MAX_SEARCH_RESULTS_DISPLAY = 6 # Max results to DISPLAY in the search results message

# --- YTMusic API Initialization ---
# Initializes the YouTube Music API client. Tries authenticated login if
# 'headers_auth.json' exists, otherwise uses an unauthenticated client.
try:
    auth_file = os.path.join(SCRIPT_DIR, 'headers_auth.json')
    if os.path.exists(auth_file):
        ytmusic = YTMusic(auth_file)
        logger.info("ytmusicapi initialized successfully (AUTHENTICATED).")
    else:
        ytmusic = YTMusic()
        logger.info("ytmusicapi initialized successfully (unauthenticated). Authentication file 'headers_auth.json' not found.")
except Exception as e:
    # Non-critical, log warning but continue (some features might be limited)
    logger.warning(f"Failed to initialize ytmusicapi: {e}. Some features might be limited (e.g., accessing private playlists/library).")
    ytmusic = None # Explicitly set to None if init fails

# =============================================================================
#                            DATA MANAGEMENT (Users, Last Tracks)
# =============================================================================

# --- File Paths for Data ---
USERS_FILE = os.path.join(SCRIPT_DIR, 'users.csv')         # Whitelisted user IDs and names
LAST_TRACKS_FILE = os.path.join(SCRIPT_DIR, 'last.csv')    # Recently downloaded tracks history
HELP_FILE = os.path.join(SCRIPT_DIR, 'help.txt')           # Help message content

# --- User and Track Management Functions ---

def load_users() -> Dict[int, str]:
    """Loads whitelisted users from users.csv (format: Name;UserID)."""
    users: Dict[int, str] = {}
    if not os.path.exists(USERS_FILE):
        logger.warning(f"Whitelist file not found: {USERS_FILE}. Whitelist is empty. Bot will allow all users unless 'whitelist_enabled' is explicitly true.")
        return users
    try:
        with open(USERS_FILE, 'r', encoding='utf-8', newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=';')
            for i, row in enumerate(reader):
                if len(row) == 2:
                    try:
                        user_id = int(row[1].strip())
                        user_name = row[0].strip()
                        if not user_name: user_name = f"User ID {user_id}" # Fallback name
                        users[user_id] = user_name
                    except ValueError:
                        logger.warning(f"Skipping invalid user ID '{row[1]}' in {USERS_FILE}, line {i+1}")
                elif row: # Log only if the row is not completely empty
                    logger.warning(f"Skipping malformed row (expected 2 columns separated by ';') in {USERS_FILE}, line {i+1}: {row}")
        logger.info(f"Loaded {len(users)} users from {USERS_FILE}")
    except Exception as e:
        logger.error(f"Error loading users from {USERS_FILE}: {e}")
    return users

def save_users(users: Dict[int, str]):
    """Saves the current whitelist (UserID -> Name mapping) to users.csv."""
    try:
        with open(USERS_FILE, 'w', encoding='utf-8', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            # Write rows as (Name, UserID)
            writer.writerows([(name, uid) for uid, name in users.items()])
        logger.info(f"Saved {len(users)} users to {USERS_FILE}")
    except Exception as e:
        logger.error(f"Error saving users to {USERS_FILE}: {e}")

# Load the initial whitelist
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
            header = next(reader, None) # Read header row
            # Basic header validation (optional but good practice)
            expected_header_parts = ['track', 'creator', 'browseid', 'tt:tt-dd-mm']
            if header and not all(part in ''.join(header).lower().replace(' ', '').replace('-', '') for part in expected_header_parts):
                 logger.warning(f"Unexpected header in {LAST_TRACKS_FILE}: {header}. Expected something like 'track;creator;browseId;tt:tt-dd-mm'.")
            # Read remaining rows, ensuring they have at least 4 columns
            tracks = [row for row in reader if len(row) >= 4]
            # Log if any rows were skipped due to incorrect length (re-read count for accuracy)
            try:
                with open(LAST_TRACKS_FILE, 'r', encoding='utf-8', newline='') as f_count:
                    original_row_count = sum(1 for row in csv.reader(f_count, delimiter=';') if row) -1 # Count non-empty rows minus header
                if len(tracks) < original_row_count:
                    logger.warning(f"Skipped {original_row_count - len(tracks)} malformed rows (less than 4 columns) in {LAST_TRACKS_FILE}.")
            except Exception: pass # Ignore errors during re-count

        logger.info(f"Loaded {len(tracks)} valid last tracks entries from {LAST_TRACKS_FILE}")
    except StopIteration: # Handles empty file after header check
        logger.info(f"{LAST_TRACKS_FILE} is empty or contains only a header.")
    except Exception as e:
        logger.error(f"Error loading last tracks from {LAST_TRACKS_FILE}: {e}")
    return tracks

def save_last_tracks(tracks: List[List[str]]):
    """Saves the recent tracks history (keeping only the latest 5) to last.csv."""
    try:
        # Keep only the last 5 tracks (or fewer if list is shorter)
        tracks_to_save = tracks[:5]
        with open(LAST_TRACKS_FILE, 'w', encoding='utf-8', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=';')
            # Write a standard header
            writer.writerow(['track', 'creator', 'browseId', 'tt:tt-dd-mm'])
            writer.writerows(tracks_to_save)
        logger.info(f"Saved {len(tracks_to_save)} last tracks to {LAST_TRACKS_FILE}")
    except Exception as e:
        logger.error(f"Error saving last tracks to {LAST_TRACKS_FILE}: {e}")

# =============================================================================
#                            CORE UTILITIES
# =============================================================================

# --- Retry Decorator ---
# Automatically retries a function if it fails with specified exceptions.
def retry(max_tries: int = 3, delay: float = 2.0, exceptions: Tuple[Type[Exception], ...] = (Exception,)):
    """Decorator to retry an async function upon encountering specific exceptions."""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_tries):
                try:
                    # Attempt to run the decorated function
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    # Check if this was the last attempt
                    if attempt == max_tries - 1:
                        # Log final failure and re-raise the exception
                        logger.error(f"Function '{func.__name__}' failed after {max_tries} attempts. Last error: {e}")
                        raise
                    else:
                        # Log warning and wait before retrying
                        wait_time = delay * (2 ** attempt) # Exponential backoff
                        logger.warning(f"Attempt {attempt + 1}/{max_tries} failed for '{func.__name__}': {e}. Retrying in {wait_time:.2f}s...")
                        await asyncio.sleep(wait_time)
            # This part should not be reachable if max_tries >= 1
            if last_exception: raise last_exception # Re-raise if loop finishes unexpectedly
        return wrapper
    return decorator

# =============================================================================
#                       YOUTUBE MUSIC API INTERACTION
# =============================================================================

@retry(exceptions=(Exception,)) # Retry on any exception for API calls
async def get_entity_info(entity_id: str, entity_type_hint: Optional[str] = None) -> Optional[Dict]:
    """
    Fetches metadata for a YouTube Music entity (track, album, playlist, artist)
    using its ID or browseId. Tries to infer type if hint is not provided.
    Uses get_watch_playlist for tracks for potentially richer metadata.

    Args:
        entity_id: The videoId (track), browseId (album/artist), or playlistId.
        entity_type_hint: Optional hint ('track', 'album', 'playlist', 'artist').

    Returns:
        A dictionary containing the entity's metadata, or None if not found/error.
        Includes an '_entity_type' key indicating the detected type.
    """
    if not ytmusic:
        logger.error("YTMusic API client not initialized. Cannot fetch entity info.")
        return None

    logger.debug(f"Fetching entity info for ID: {entity_id}, Hint: {entity_type_hint}")
    try:
        # --- Infer Type from ID structure ---
        inferred_type = None
        if isinstance(entity_id, str):
            if entity_id.startswith(('PL', 'VL')): inferred_type = "playlist"
            elif entity_id.startswith(('MPRE', 'MPLA')): inferred_type = "album"
            elif entity_id.startswith('UC'): inferred_type = "artist"
            # Basic check for video ID format (11 chars, specific characters)
            elif re.fullmatch(r'[A-Za-z0-9_-]{11}', entity_id): inferred_type = "track"
        else:
            logger.warning(f"Invalid entity_id type provided: {type(entity_id)}. Must be a string.")
            return None # ID must be a string

        current_hint = entity_type_hint or inferred_type
        logger.debug(f"Effective hint/inferred type for API call: {current_hint}")

        # --- Try get_watch_playlist first for Tracks (often better metadata) ---
        # This method is primarily for tracks, identified by an 11-char videoId.
        if (current_hint == "track" or inferred_type == "track") and re.fullmatch(r'[A-Za-z0-9_-]{11}', entity_id):
             try:
                 logger.debug(f"Attempting get_watch_playlist for potential track ID {entity_id}")
                 # limit=1 fetches info primarily for the main track
                 watch_info = ytmusic.get_watch_playlist(videoId=entity_id, limit=1)
                 if watch_info and watch_info.get('tracks') and len(watch_info['tracks']) > 0:
                      track_data = watch_info['tracks'][0]
                      # --- Standardize track_data structure ---
                      # Make it resemble results from get_song or search for consistency downstream
                      standardized_info = {
                          '_entity_type': 'track',
                          'videoId': track_data.get('videoId'),
                          'title': track_data.get('title'),
                          'artists': track_data.get('artists'), # List of dicts [{'name': '...', 'id': '...'}]
                          'album': track_data.get('album'), # Dict {'name': '...', 'id': '...'}
                          'duration': track_data.get('length'), # Often string "M:SS"
                          'lengthSeconds': track_data.get('lengthSeconds'), # Integer seconds
                          'thumbnails': track_data.get('thumbnail'), # List of dicts [{'url': '...', 'width': ..., 'height': ...}]
                          'year': track_data.get('year'),
                          # Reconstruct a 'videoDetails'-like structure for compatibility if needed elsewhere
                          'videoDetails': {
                                'videoId': track_data.get('videoId'),
                                'title': track_data.get('title'),
                                'lengthSeconds': track_data.get('lengthSeconds'),
                                'thumbnails': track_data.get('thumbnail'),
                                # Create 'author' string similar to get_song if needed
                                'author': ', '.join([a['name'] for a in track_data.get('artists', []) if a.get('name')]) if track_data.get('artists') else None,
                                'channelId': None, # Not directly available here, maybe infer from artist?
                          }
                      }
                      logger.info(f"Successfully fetched track info for {entity_id} using get_watch_playlist")
                      return standardized_info
                 else:
                     logger.debug(f"get_watch_playlist for {entity_id} didn't return expected track data structure.")
             except Exception as e_watch:
                  # Log as warning and continue to other methods
                  logger.warning(f"get_watch_playlist failed for {entity_id}: {e_watch}. Falling back to other methods.")

        # --- Fallback / Standard Method based on Hint/Inferred Type ---
        # These use the browseId for albums/artists and playlistId for playlists.
        api_calls_by_type = {
            "playlist": lambda eid: ytmusic.get_playlist(playlistId=eid),
            "album": lambda eid: ytmusic.get_album(browseId=eid),
            "artist": lambda eid: ytmusic.get_artist(channelId=eid),
            "track": lambda eid: ytmusic.get_song(videoId=eid), # Fallback for tracks if watch_playlist failed
        }

        if current_hint and current_hint in api_calls_by_type:
            try:
                logger.debug(f"Trying API call for hinted/inferred type: {current_hint}")
                info = api_calls_by_type[current_hint](entity_id)
                if info:
                    # Special handling for get_song response structure
                    if current_hint == "track":
                        if info.get('videoDetails'):
                            processed_info = info['videoDetails'] # Use the nested details dict
                            # Ensure 'thumbnails' is present, copying from outer level if needed
                            if 'thumbnails' not in processed_info and 'thumbnail' in info:
                                processed_info['thumbnails'] = info['thumbnail'].get('thumbnails')
                            info = processed_info # Replace original info with processed version
                        else:
                             logger.warning(f"get_song for {entity_id} lacked 'videoDetails'. Result structure may be inconsistent.")
                             # Use the raw result, but it might lack structure
                    info['_entity_type'] = current_hint # Add our type label
                    logger.info(f"Successfully fetched entity info using hint/inferred type '{current_hint}' for {entity_id}")
                    return info
                else:
                    logger.warning(f"API call for hint '{current_hint}' returned no data for {entity_id}.")
            except Exception as e_hint:
                 logger.warning(f"API call for hint/inferred type '{current_hint}' failed for {entity_id}: {e_hint}. Trying generic checks.")

        # --- Generic Type Check (if hint failed or wasn't provided/inferred) ---
        # Try each API endpoint sequentially until one succeeds.
        # Order: Track (most common?), Playlist, Album, Artist
        # Use the same lambda functions as above for consistency
        generic_check_order = [
             ("track", api_calls_by_type["track"]),
             ("playlist", api_calls_by_type["playlist"]),
             ("album", api_calls_by_type["album"]),
             ("artist", api_calls_by_type["artist"]),
        ]

        for type_name, api_func in generic_check_order:
            # Skip if we already tried this type via hint and it failed
            if current_hint and current_hint == type_name: continue
            # Skip track check if it's not a valid video ID format
            if type_name == "track" and not re.fullmatch(r'[A-Za-z0-9_-]{11}', entity_id): continue
            # Skip album/artist/playlist if ID structure doesn't remotely match (basic heuristic)
            if type_name == "album" and not entity_id.startswith(('MPRE', 'MPLA','OLAK5uy_')): continue
            if type_name == "artist" and not entity_id.startswith('UC'): continue
            if type_name == "playlist" and not entity_id.startswith(('PL', 'VL', 'OLAK5uy_')): continue


            try:
                logger.debug(f"Trying generic API call for type '{type_name}' for {entity_id}")
                result = api_func(entity_id)
                if result:
                    final_info = result
                    # Handle get_song's nested structure again
                    if type_name == "track":
                        if result.get('videoDetails'):
                            processed_info = result['videoDetails']
                            if 'thumbnails' not in processed_info and 'thumbnail' in result:
                                processed_info['thumbnails'] = result['thumbnail'].get('thumbnails')
                            final_info = processed_info
                        else:
                             logger.warning(f"Generic check {type_name} for {entity_id} lacked 'videoDetails'. Structure may be inconsistent.")
                             # Use raw result but mark it
                             final_info['_incomplete_structure'] = True

                    final_info['_entity_type'] = type_name
                    logger.info(f"Successfully fetched entity info as '{type_name}' for {entity_id} using generic check.")
                    return final_info
            except Exception as e_generic:
                 # Expected exceptions if the ID doesn't match the type, log minimally unless debugging
                 # logger.debug(f"Generic check for type '{type_name}' failed for {entity_id}: {type(e_generic).__name__}")
                 pass # Ignore and try next type

        # If all attempts failed
        logger.error(f"Could not retrieve info for entity ID: {entity_id} using any method.")
        return None

    except Exception as e_outer:
        # Catch unexpected errors in the logic itself
        logger.error(f"Unexpected error in get_entity_info for {entity_id}: {e_outer}", exc_info=True)
        return None


@retry(exceptions=(Exception,))
async def search(query: str, search_type_flag: str, limit: int) -> List[Dict]:
    """
    Performs a search on YouTube Music using ytmusicapi.

    Args:
        query: The search term.
        search_type_flag: The type flag ('-t', '-a', '-p', '-e').
        limit: The maximum number of results to fetch from the API.

    Returns:
        A list of search result dictionaries, or an empty list if no results or error.
    """
    if not ytmusic:
        logger.error("YTMusic API client not initialized. Cannot perform search.")
        return []

    # Map flags to API filter types
    filter_map = { "-t": "songs", "-a": "albums", "-p": "playlists", "-e": "artists" }
    filter_type = filter_map.get(search_type_flag)
    if not filter_type:
        # This should ideally be caught by the command handler, but double-check
        logger.error(f"Invalid search type flag provided to search function: {search_type_flag}")
        raise ValueError(f"Invalid search type flag: {search_type_flag}")

    logger.info(f"Searching for '{query}' (Type: {filter_type}, Limit: {limit})")
    # Request slightly more results from API than strictly needed,
    # in case some results are invalid or filtered out later. Clamped for safety.
    api_limit = min(max(limit + 3, 5), 20) # Fetch a few extra, max 20
    try:
        results = ytmusic.search(query, filter=filter_type, limit=api_limit)
    except Exception as e:
         logger.error(f"ytmusicapi search failed: {e}", exc_info=True)
         return [] # Return empty list on API error

    if not results:
        logger.info(f"No results found for query '{query}' (Type: {filter_type})")
        return []

    # Filter out potential None or empty results just in case
    valid_results = [r for r in results if r and isinstance(r, dict)]
    if len(valid_results) < len(results):
         logger.warning(f"Removed {len(results) - len(valid_results)} invalid items from search results.")

    # Return only the number of results originally requested by the user/config
    final_results = valid_results[:limit]
    logger.info(f"Found {len(final_results)} valid results for query '{query}' (fetched {len(results)} initially)")
    return final_results

# =============================================================================
#                       DOWNLOAD & PROCESSING FUNCTIONS
# =============================================================================

def extract_track_metadata(info: Dict) -> Tuple[str, str, int]:
    """
    Extracts Title, Performer, and Duration from yt-dlp's info dictionary.
    Prioritizes common metadata fields and cleans artist names.

    Args:
        info: The dictionary returned by yt_dlp.extract_info.

    Returns:
        A tuple: (title, performer, duration_seconds). Defaults to 'Неизвестно' or 0.
    """
    # --- Title Extraction ---
    title = info.get('track') or info.get('title') or 'Неизвестно'

    # --- Performer Extraction Logic ---
    performer = 'Неизвестно' # Default
    # Priority 1: 'artist' field (often pre-processed by yt-dlp)
    if info.get('artist'):
        performer = info['artist']
    # Priority 2: 'artists' list (common from ytmusicapi results merged by ytdlp)
    elif info.get('artists') and isinstance(info['artists'], list):
         # Extract names from the list of artist dictionaries
         artist_names = [a['name'] for a in info['artists'] if isinstance(a, dict) and a.get('name')]
         if artist_names: performer = ', '.join(artist_names)
    # Priority 3: 'creator' field (another yt-dlp field)
    elif info.get('creator'):
         performer = info['creator']
    # Priority 4: 'uploader' field (often channel name, clean '- Topic')
    elif info.get('uploader'):
         # Remove common " - Topic" suffix added by YouTube
         performer = re.sub(r'\s*-\s*Topic$', '', info['uploader']).strip()

    # Fallback 1: 'channel' field (if uploader was missing/unhelpful)
    if performer in [None, "", "Неизвестно"] and info.get('channel'):
         performer = re.sub(r'\s*-\s*Topic$', '', info['channel']).strip()

    # Final Fallback
    if performer in [None, "", "Неизвестно"]:
        performer = 'Неизвестно'

    # Clean performer name (remove potential extra spaces)
    performer = performer.strip()

    # --- Duration Extraction ---
    duration = 0
    try:
        # Prefer precise duration if available
        duration = int(info.get('duration') or 0)
    except (ValueError, TypeError):
         logger.warning(f"Could not parse duration '{info.get('duration')}' for track '{title}'. Defaulting to 0.")
         duration = 0

    # Log extracted data for debugging
    logger.debug(f"Extracted metadata - Title: '{title}', Performer: '{performer}', Duration: {duration}s")
    return title, performer, duration

def download_track(track_link: str) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Downloads a single track using yt-dlp with configured options.
    This includes audio extraction, metadata embedding, and thumbnail embedding via FFmpeg postprocessors.

    Args:
        track_link: The URL of the track to download.

    Returns:
        A tuple: (info_dict, file_path).
        info_dict: Metadata dictionary from yt-dlp.
        file_path: Absolute path to the downloaded/processed audio file.
        Returns (None, None) on failure.
    """
    logger.info(f"Attempting download and processing via yt-dlp: {track_link}")
    try:
        # Copy options to avoid modifying the global dict if adjustments are needed per-call
        current_ydl_opts = YDL_OPTS.copy()

        # Remove playlist index from template if downloading single track and noplaylist=True
        # This prevents potential errors if the template includes it but it's not available.
        if current_ydl_opts.get('noplaylist'):
             tmpl = current_ydl_opts.get('outtmpl', '') # Get template safely
             # Replace variations of playlist index placeholder
             tmpl = tmpl.replace('%(playlist_index)s-', '').replace('-%(playlist_index)s', '').replace('%(playlist_index)s', '')
             tmpl = tmpl.replace('[%(playlist_index)02d] ', '').replace('[%(playlist_index)s] ', '') # Handle format used in album dl
             current_ydl_opts['outtmpl'] = tmpl

        # Use yt-dlp context manager for cleanup
        with yt_dlp.YoutubeDL(current_ydl_opts) as ydl:
            # Start download and extraction
            # download=True tells yt-dlp to actually download based on the options
            # The postprocessors (FFmpegExtractAudio, etc.) run after download.
            info = ydl.extract_info(track_link, download=True)

            # Check if extraction itself failed
            if not info:
                logger.error(f"yt-dlp extract_info returned empty/None for {track_link}")
                return None, None

            # --- Determine Final File Path ---
            # The 'EmbedMetadata' and 'FFmpegExtractAudio' postprocessors usually update
            # the 'filepath' key in the info dict to the final processed file path.
            final_filepath = info.get('filepath')

            if final_filepath and os.path.exists(final_filepath) and os.path.isfile(final_filepath):
                 logger.info(f"Download and postprocessing successful. Final file: {final_filepath}")
                 return info, final_filepath
            else:
                # If 'filepath' is not set or doesn't exist, try to deduce it
                logger.warning(f"Final 'filepath' key missing or file not found ('{final_filepath}'). Attempting to locate file based on template.")
                try:
                    # Ask yt-dlp to generate the filename based on the *original* info, before PPs might change extension
                    # Note: This might not reflect the final extension after audio conversion.
                    potential_path_before_pp = ydl.prepare_filename(info)
                    logger.debug(f"Path based on prepare_filename (might be pre-conversion): {potential_path_before_pp}")
                    base_potential, _ = os.path.splitext(potential_path_before_pp)

                    # Check common audio extensions based on the base name derived from prepare_filename
                    potential_exts = ['.mp3', '.m4a', '.ogg', '.opus', '.aac', '.flac', '.wav']
                    # Add the extension from the preferred codec if specified
                    try:
                        pref_codec = next((pp.get('preferredcodec') for pp in current_ydl_opts.get('postprocessors', []) if pp.get('key') == 'FFmpegExtractAudio' and pp.get('preferredcodec')), None)
                        if pref_codec and f'.{pref_codec}' not in potential_exts:
                            potential_exts.insert(0, f'.{pref_codec}') # Check preferred first
                    except Exception: pass

                    for ext in potential_exts:
                        check_path = base_potential + ext
                        if os.path.exists(check_path) and os.path.isfile(check_path):
                             logger.warning(f"Located final file via extension check: {check_path}")
                             info['filepath'] = check_path # Update info dict with found path
                             return info, check_path

                    logger.error(f"Could not locate the final processed audio file for {track_link} even after checking extensions.")
                    return info, None # Return info for debugging, but no path found
                except Exception as e_locate:
                    logger.error(f"Error trying to locate final file for {track_link}: {e_locate}")
                    return info, None

    # --- Error Handling ---
    except yt_dlp.utils.DownloadError as e:
        # Specific yt-dlp download errors (network issues, unavailable video, etc.)
        logger.error(f"yt-dlp DownloadError for {track_link}: {e}")
        return None, None
    except Exception as e:
        # Catch any other unexpected errors during the process
        logger.error(f"Unexpected download error for {track_link}: {e}", exc_info=True)
        return None, None

async def download_album_tracks(album_browse_id: str, progress_callback=None) -> List[Tuple[Dict, str]]:
    """
    Downloads all tracks from a given album browse ID using yt-dlp,
    processing and sending each track sequentially.

    Args:
        album_browse_id: The MPRE... ID of the album.
        progress_callback: An async function to call with progress updates.

    Returns:
        A list of tuples: [(info_dict, file_path), ...]. Includes only successfully downloaded tracks.
    """
    if not ytmusic:
        logger.error("YTMusic API client not initialized. Cannot download album.")
        if progress_callback: await progress_callback("album_error", error="YTMusic client not ready")
        return []

    logger.info(f"Attempting to download album sequentially: {album_browse_id}")
    downloaded_files: List[Tuple[Dict, str]] = []
    album_info, total_tracks, album_title = None, 0, album_browse_id

    try:
        # --- Get Album Info ---
        logger.debug(f"Fetching album metadata for {album_browse_id}...")
        # Try fetching metadata, but proceed even if it fails (e.g., for playlist IDs ytmusicapi might not handle)
        try:
             if album_browse_id.startswith('MPRE'): # Only query ytmusicapi for actual album IDs
                 album_info = ytmusic.get_album(browseId=album_browse_id) if ytmusic else None
                 if album_info:
                     album_title = album_info.get('title', album_browse_id)
                     total_tracks = album_info.get('trackCount') or len(album_info.get('tracks', []))
                     logger.info(f"Fetched album metadata: '{album_title}', Expected tracks: {total_tracks or 'Unknown'}")
                 else: logger.warning(f"Could not fetch album metadata for MPRE ID {album_browse_id} via ytmusicapi.")
             else: # For OLAK or other IDs, skip ytmusicapi metadata fetch initially
                 logger.info(f"ID {album_browse_id} not MPRE. Will attempt download; metadata will come from yt-dlp if possible.")
        except Exception as e_meta:
             logger.warning(f"Error fetching album metadata for ID {album_browse_id} via ytmusicapi: {e_meta}. Proceeding.")


        # --- Get Track List (Crucial Step) ---
        # Need the list of video IDs to download sequentially.
        # If ytmusicapi fetch succeeded, use its track list.
        # If not, we might need to use yt-dlp with --flat-playlist to get IDs first.
        tracks_to_download = []
        if album_info and 'tracks' in album_info:
             tracks_to_download = album_info.get("tracks", [])
             total_tracks = len(tracks_to_download) # Update total_tracks based on this list
             logger.info(f"Using track list from ytmusicapi metadata ({total_tracks} tracks).")
        else:
             # Fallback: Use yt-dlp to extract just the track info (IDs, titles)
             logger.info(f"Metadata incomplete/missing. Using yt-dlp with --flat-playlist to get track list for {album_browse_id}...")
             try:
                 # Construct URL for yt-dlp analysis
                 if album_browse_id.startswith('MPRE'):
                      analysis_url = f"https://music.youtube.com/browse/{album_browse_id}"
                 else: # Assume playlist for OLAK or others
                      analysis_url = f"https://music.youtube.com/playlist?list={album_browse_id}"

                 # Options for analysis only
                 analysis_opts = {
                     'extract_flat': True, # Get only entry info, don't delve deeper
                     'skip_download': True, # Don't download anything
                     'quiet': True,
                     'ignoreerrors': True,
                     'noplaylist': False, # Important for getting playlist entries
                 }
                 with yt_dlp.YoutubeDL(analysis_opts) as ydl:
                     playlist_dict = ydl.extract_info(analysis_url, download=False)

                 if playlist_dict and playlist_dict.get('entries'):
                     # Reconstruct a tracks_to_download list similar to ytmusicapi's
                     tracks_to_download = [
                         {'videoId': entry.get('id'), 'title': entry.get('title')}
                         for entry in playlist_dict['entries'] if entry and entry.get('id')
                     ]
                     total_tracks = len(tracks_to_download)
                     # Update album title if found during analysis
                     if playlist_dict.get('title') and (album_title == album_browse_id or not album_title):
                          album_title = playlist_dict['title']
                     logger.info(f"Extracted {total_tracks} tracks using yt-dlp analysis for '{album_title}'.")
                 else:
                     logger.error(f"yt-dlp analysis failed to return track entries for {album_browse_id}.")
                     if progress_callback: await progress_callback("album_error", error="yt-dlp failed to get track list")
                     return [] # Cannot proceed without track list
             except Exception as e_analyze:
                 logger.error(f"Error during yt-dlp analysis phase for {album_browse_id}: {e_analyze}", exc_info=True)
                 if progress_callback: await progress_callback("album_error", error=f"yt-dlp analysis error: {e_analyze}")
                 return []

        if not tracks_to_download:
             logger.error(f"No tracks found to download for album/playlist {album_browse_id}.")
             if progress_callback: await progress_callback("album_error", error="No tracks found")
             return []

        # --- Notify progress: Analysis complete ---
        if progress_callback:
            await progress_callback("analysis_complete", total_tracks=total_tracks, title=album_title)

        # --- Download Tracks Sequentially ---
        downloaded_count = 0
        loop = asyncio.get_running_loop()

        for i, track in enumerate(tracks_to_download):
            current_track_num = i + 1
            video_id = track.get('videoId')
            # Use title from analysis/metadata, fallback to generic name
            track_title_from_list = track.get('title') or f'Track {current_track_num}'

            if not video_id:
                logger.warning(f"Skipping track {current_track_num}/{total_tracks} ('{track_title_from_list}') due to missing videoId.")
                if progress_callback: # Notify skip/fail
                     # Increment attempts count conceptually
                     await progress_callback("track_failed", current=downloaded_count + 1, total=total_tracks, title=f"{track_title_from_list} (No ID)")
                continue # Skip to next track

            download_link = f"https://music.youtube.com/watch?v={video_id}"
            logger.info(f"Downloading track {current_track_num}/{total_tracks}: '{track_title_from_list}' ({video_id})...")

            # Notify progress: Starting download for this track
            if progress_callback:
                 perc = int(((downloaded_count + 1) / total_tracks) * 100) if total_tracks else 0
                 display_track_title = (track_title_from_list[:25] + '...') if len(track_title_from_list) > 28 else track_title_from_list
                 await progress_callback("track_downloading",
                                       current=current_track_num, # Show which track number is starting
                                       total=total_tracks,
                                       percentage=perc,
                                       title=display_track_title)

            try:
                # Run the blocking download_track in an executor thread
                # download_track uses the global YDL_OPTS which includes postprocessing
                info, file_path = await loop.run_in_executor(None, functools.partial(download_track, download_link))

                # --- Handle Success ---
                if file_path and info:
                    actual_filename = os.path.basename(file_path)
                    # Use title from the *downloaded* info dict if available, it might be more accurate
                    final_track_title = info.get('title', track_title_from_list)
                    logger.info(f"Successfully downloaded and processed track {current_track_num}/{total_tracks}: {actual_filename}")
                    downloaded_files.append((info, file_path))
                    downloaded_count += 1
                    # Notify progress: Track finished
                    if progress_callback:
                         await progress_callback("track_downloaded", current=downloaded_count, total=total_tracks, title=final_track_title)
                # --- Handle Failure ---
                else:
                    logger.error(f"Failed to download/process track {current_track_num}/{total_tracks}: '{track_title_from_list}' ({video_id})")
                    if progress_callback:
                         # Increment attempts count conceptually when reporting failure
                         await progress_callback("track_failed", current=downloaded_count + 1,
                                               total=total_tracks, title=track_title_from_list)

            except Exception as e_track_dl:
                # Catch errors during the await/executor call itself for this specific track
                logger.error(f"Error during download process for track {current_track_num} ('{track_title_from_list}'): {e_track_dl}", exc_info=True)
                if progress_callback:
                     # Increment attempts count conceptually
                     await progress_callback("track_failed", current=downloaded_count + 1, total=total_tracks, title=f"{track_title_from_list} (Error)")

            # Optional: Add a small delay between track downloads if needed
            await asyncio.sleep(0.3) # Short delay

    except Exception as e_album_outer:
        logger.error(f"Error during album processing loop for {album_browse_id}: {e_album_outer}", exc_info=True)
        if progress_callback:
            await progress_callback("album_error", error=str(e_album_outer))

    logger.info(f"Finished sequential album download for '{album_title}'. Successfully saved {len(downloaded_files)} out of {total_tracks} tracks attempted.")
    return downloaded_files

# =============================================================================
#                           THUMBNAIL HANDLING
# =============================================================================

@retry(exceptions=(requests.exceptions.RequestException,), delay=1.0) # Retry only on network errors for download
async def download_thumbnail(url: str, output_dir: str = SCRIPT_DIR) -> Optional[str]:
    """
    Downloads a thumbnail image from a URL.

    Args:
        url: The URL of the thumbnail image.
        output_dir: Directory to save the temporary thumbnail.

    Returns:
        The absolute path to the downloaded temporary image file, or None on failure.
    """
    # --- Input Validation ---
    if not url or not isinstance(url, str) or not url.startswith(('http://', 'https://')):
        logger.warning(f"Invalid or non-HTTP/S thumbnail URL provided: {url}")
        return None

    logger.debug(f"Attempting to download thumbnail: {url}")
    temp_file_path = None # Initialize path variable

    try:
        # --- Generate a unique temporary filename ---
        # Use parts of the URL and timestamp to create a somewhat meaningful and unique name.
        try:
            parsed_url = urlparse(url)
            base_name_from_url = os.path.basename(parsed_url.path) if parsed_url.path else "thumb"
        except Exception as parse_e:
            logger.warning(f"Could not parse URL path for thumbnail naming: {parse_e}. Using default 'thumb'.")
            base_name_from_url = "thumb"

        # Extract base name and attempt to get original extension
        base_name, potential_ext = os.path.splitext(base_name_from_url)
        # Basic validation for a plausible extension (e.g., ".jpg", ".png", ".webp")
        if potential_ext and len(potential_ext) <= 5 and potential_ext[1:].isalnum():
             ext = potential_ext.lower() # Use original extension
        else:
             ext = '.jpg' # Default to jpg if no valid extension found

        # Clean base name (remove extension part if it was the whole name)
        if not base_name or base_name == potential_ext: base_name = "thumb"

        # Sanitize base name for filesystem compatibility and limit length
        safe_base_name = re.sub(r'[^\w.\-]', '_', base_name) # Replace invalid chars
        max_len = 40 # Slightly shorter max length
        safe_base_name = (safe_base_name[:max_len] + '...') if len(safe_base_name) > max_len else safe_base_name
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%f") # High precision timestamp
        temp_filename = f"temp_thumb_{safe_base_name}_{timestamp}{ext}"
        temp_file_path = os.path.join(output_dir, temp_filename)

        # --- Perform Download ---
        # Use requests with streaming for potentially large files and timeout
        response = requests.get(url, stream=True, timeout=25) # Increased timeout slightly
        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

        # Save the downloaded content to the temporary file
        with open(temp_file_path, 'wb') as out_file:
            shutil.copyfileobj(response.raw, out_file)
        logger.debug(f"Thumbnail downloaded to temporary file: {temp_file_path}")

        # --- Verify Image Integrity ---
        # Try opening the downloaded file with Pillow to check if it's a valid image
        try:
            with Image.open(temp_file_path) as img:
                img.verify() # Basic integrity check (structure, not full decoding)
            logger.debug(f"Thumbnail verified as valid image: {temp_file_path}")
            return temp_file_path # Return the path if download and verification succeeded
        except (FileNotFoundError, UnidentifiedImageError, SyntaxError, OSError, ValueError) as img_e:
             # Log error and cleanup the invalid file
             logger.error(f"Downloaded file is not a valid image ({url}): {img_e}. Deleting.")
             if os.path.exists(temp_file_path):
                 try: os.remove(temp_file_path)
                 except OSError as rm_e: logger.warning(f"Could not remove invalid temp thumb {temp_file_path}: {rm_e}")
             return None # Indicate failure

    # --- Error Handling for Download ---
    except requests.exceptions.Timeout:
        logger.error(f"Timeout occurred while downloading thumbnail: {url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Network error downloading thumbnail {url}: {e}")
        # Cleanup partially downloaded file if it exists
        if temp_file_path and os.path.exists(temp_file_path):
            try: os.remove(temp_file_path)
            except OSError as rm_e: logger.warning(f"Could not remove partial temp thumb {temp_file_path}: {rm_e}")
        return None
    except Exception as e_outer:
        logger.error(f"Unexpected error downloading thumbnail {url}: {e_outer}", exc_info=True)
        # Cleanup file if it exists
        if temp_file_path and os.path.exists(temp_file_path):
            try: os.remove(temp_file_path)
            except OSError as rm_e: logger.warning(f"Could not remove temp thumb {temp_file_path} after error: {rm_e}")
        return None

def crop_thumbnail(image_path: str) -> Optional[str]:
    """
    Crops an image to a square aspect ratio (center crop) and saves it as JPEG.
    Handles transparency by converting to RGB with a white background.

    Args:
        image_path: Path to the input image file.

    Returns:
        Path to the cropped JPEG image, or None on failure.
        The output filename will be based on the input name with "_cropped.jpg" suffix.
    """
    if not image_path or not os.path.exists(image_path):
        logger.error(f"Cannot crop thumbnail, file not found: {image_path}")
        return None

    logger.debug(f"Processing thumbnail (cropping to square): {image_path}")
    # Define the output path for the cropped image
    output_path = os.path.splitext(image_path)[0] + "_cropped.jpg"

    try:
        with Image.open(image_path) as img:
            img_rgb = img # Start with the original image

            # --- Convert to RGB if necessary (handling transparency) ---
            if img.mode != 'RGB':
                logger.debug(f"Image mode is '{img.mode}', converting to RGB for cropping/JPEG saving.")
                try:
                    # Create a white background image of the same size
                    bg = Image.new("RGB", img.size, (255, 255, 255))
                    # Paste the original image onto the white background
                    # Use the alpha channel as a mask if available (RGBA, LA)
                    if img.mode in ('RGBA', 'LA') and len(img.split()) > 3:
                        bg.paste(img, mask=img.split()[-1])
                    else:
                        # For modes without explicit alpha (like P, L), just paste directly
                        bg.paste(img)
                    img_rgb = bg # Use the image pasted on the background
                    logger.debug(f"Successfully converted image {os.path.basename(image_path)} from {img.mode} to RGB with background.")
                except Exception as conv_e:
                     logger.warning(f"Could not convert image {os.path.basename(image_path)} from {img.mode} to RGB using background paste: {conv_e}. Attempting basic conversion.")
                     # Fallback to basic RGB conversion (might lose transparency info differently)
                     try:
                         img_rgb = img.convert('RGB')
                     except Exception as basic_conv_e:
                         logger.error(f"Failed basic RGB conversion for {os.path.basename(image_path)}: {basic_conv_e}. Cannot crop.")
                         return None

            # --- Perform Center Crop to Square ---
            width, height = img_rgb.size
            min_dim = min(width, height) # Find the smaller dimension
            # Calculate coordinates for the square crop centered in the image
            left = (width - min_dim) / 2
            top = (height - min_dim) / 2
            right = (width + min_dim) / 2
            bottom = (height + min_dim) / 2
            # Ensure coordinates are integers for crop
            crop_box = tuple(map(int, (left, top, right, bottom)))
            # Crop the image
            img_cropped = img_rgb.crop(crop_box)

            # --- Save as JPEG ---
            # Save the cropped image in JPEG format with specified quality
            img_cropped.save(output_path, "JPEG", quality=90)
            logger.debug(f"Thumbnail cropped and saved successfully: {output_path}")
            return output_path

    # --- Error Handling for Image Processing ---
    except FileNotFoundError: # Should be caught earlier, but safe check
        logger.error(f"Cannot process thumbnail, file not found during Pillow operations: {image_path}")
        return None
    except UnidentifiedImageError:
        logger.error(f"Cannot process thumbnail, invalid or unsupported image file format: {image_path}")
        # Clean up potential output file if save started but failed? Unlikely here.
        return None
    except Exception as e:
        logger.error(f"Error processing (cropping) thumbnail {os.path.basename(image_path)}: {e}", exc_info=True)
        # Clean up partial output file if it exists
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
    Designed to clean up audio files and temporary thumbnails after processing/sending.

    Args:
        *files: Variable number of file paths (strings) to remove explicitly.
                None values or non-strings in args are ignored.
    """
    # --- Define Patterns for Automatic Cleanup ---
    # These patterns target temporary files created by the bot or yt-dlp.
    temp_patterns = [
        os.path.join(SCRIPT_DIR, "temp_thumb_*"),   # Matches thumbnails downloaded by download_thumbnail
        os.path.join(SCRIPT_DIR, "*_cropped.jpg"),  # Matches thumbnails processed by crop_thumbnail
        # os.path.join(SCRIPT_DIR, "*_converted.jpg"), # Matches thumbnails processed by old ensure_jpeg (if needed)
        os.path.join(SCRIPT_DIR, "*.part"),         # yt-dlp partial download files
        os.path.join(SCRIPT_DIR, "*.ytdl"),         # yt-dlp temporary download files
        os.path.join(SCRIPT_DIR, "*.webp"),         # Often used for thumbnails, clean up just in case
    ]

    # --- Collect All Files to Remove ---
    # Start with explicitly passed files, ensuring they are absolute paths and valid strings
    all_files_to_remove = set()
    for f in files:
        if f and isinstance(f, str):
            try:
                # Check if file exists before trying to get absolute path, might avoid errors
                if os.path.exists(f): # Check existence relative to where script thinks it is
                    abs_path = os.path.abspath(f)
                    all_files_to_remove.add(abs_path)
                # else: logger.debug(f"Cleanup: Explicit file '{f}' does not exist, skipping.") # Optional: Log skips
            except Exception as abs_e:
                 logger.warning(f"Could not get absolute path for file '{f}': {abs_e}")

    # Add files matching the defined patterns using glob
    for pattern in temp_patterns:
        try:
            # Ensure the pattern itself uses an absolute path for reliable globbing
            abs_pattern = os.path.abspath(pattern)
            matched_files = glob.glob(abs_pattern)
            if matched_files:
                logger.debug(f"Globbed {len(matched_files)} files for cleanup pattern: {pattern}")
                # Add all matched absolute paths to the set
                all_files_to_remove.update(os.path.abspath(mf) for mf in matched_files)
        except Exception as e:
            logger.error(f"Error during glob matching for pattern '{pattern}': {e}")

    # --- Perform Deletion ---
    removed_count = 0
    if not all_files_to_remove:
        logger.debug("Cleanup called, but no files specified or matched for removal.")
        return

    logger.info(f"Attempting to clean up {len(all_files_to_remove)} potential files...")
    for file_path in all_files_to_remove:
        try:
            # Check if it exists and is a file (not a directory) before removing
            if os.path.isfile(file_path): # os.path.isfile implies os.path.exists
                os.remove(file_path)
                logger.debug(f"Removed file: {file_path}")
                removed_count += 1
            # Optional: Log files that were targeted but didn't exist or weren't files
            # elif os.path.exists(file_path): logger.debug(f"Skipping cleanup for non-file path: {file_path}")
            # else: logger.debug(f"Skipping cleanup for non-existent path: {file_path}")
        except OSError as e:
            # Handle OS-level errors during deletion (e.g., permissions, file in use)
            logger.error(f"Error removing file {file_path}: {e}")
        except Exception as e_remove:
            # Catch any other unexpected errors during removal
            logger.error(f"Unexpected error removing file {file_path}: {e_remove}")

    if removed_count > 0:
        logger.info(f"Successfully cleaned up {removed_count} files.")
    # else: logger.info(f"Cleanup finished. No existing files were removed (out of {len(all_files_to_remove)} candidates).")


# =============================================================================
#                         TELEGRAM MESSAGE UTILITIES
# =============================================================================

# Global dictionary to store message IDs of previous bot responses per chat.
# Used by auto_clear functionality. Key: chat_id (int), Value: List[telethon.types.Message]
previous_bot_messages: Dict[int, List[types.Message]] = {}

async def update_progress(progress_message: Optional[types.Message], statuses: Dict[str, str]):
    """
    Edits a progress message with the current status of different tasks.

    Args:
        progress_message: The Telethon Message object to edit.
        statuses: A dictionary where keys are task names and values are status strings (e.g., with emojis).
    """
    if not progress_message or not isinstance(progress_message, types.Message):
        # logger.debug("update_progress called with invalid message object.")
        return # Ignore if message object is invalid

    # Format the text from the status dictionary
    text = "\n".join(f"{task}: {status}" for task, status in statuses.items())

    try:
        # Only edit if the text content has actually changed to avoid unnecessary API calls
        # and potential MessageNotModifiedError. Use getattr for safe access to text attribute.
        current_text = getattr(progress_message, 'text', None)
        if current_text != text:
            await progress_message.edit(text)
            # logger.debug(f"Updated progress message {progress_message.id}") # Optional debug log
    except types.errors.MessageNotModifiedError:
        # This is expected if the text hasn't changed, ignore silently.
        pass
    except types.errors.MessageIdInvalidError:
        # The message might have been deleted by the user or Telegram.
        logger.warning(f"Failed to update progress: Message {progress_message.id} seems to be deleted or invalid.")
        # Should we remove it from previous_bot_messages here? Might be complex.
    except types.errors.FloodWaitError as e:
         logger.warning(f"Flood wait ({e.seconds}s) while updating progress message {progress_message.id}. Skipping update.")
         await asyncio.sleep(e.seconds + 0.5) # Respect flood wait + buffer
    except Exception as e:
        # Catch other potential errors during edit (e.g., network issues, message too long)
        logger.warning(f"Failed to update progress message {progress_message.id}: {type(e).__name__} - {e}")

async def clear_previous_responses(chat_id: int):
    """
    Deletes previously sent bot messages stored for a specific chat.
    Handles flood waits and potential errors during deletion.
    """
    global previous_bot_messages
    if chat_id not in previous_bot_messages or not previous_bot_messages[chat_id]:
        # logger.debug(f"No previous messages to clear for chat {chat_id}.")
        return # Nothing to clear

    # Get the list of messages to delete for this chat
    # Use pop to remove the entry from the global dict immediately,
    # preventing race conditions if another command runs quickly.
    messages_to_delete = previous_bot_messages.pop(chat_id, [])
    if not messages_to_delete: return # Should not happen after check, but safe

    deleted_count = 0
    messages_to_retry = [] # Store messages that fail initially

    logger.info(f"Attempting to clear {len(messages_to_delete)} previous bot messages in chat {chat_id}")

    # --- Initial Deletion Attempt ---
    for msg in messages_to_delete:
        if not msg or not isinstance(msg, types.Message): continue # Skip invalid entries
        try:
            await msg.delete()
            deleted_count += 1
            # Add a small delay to potentially mitigate rate limits, but keep it short
            await asyncio.sleep(0.2) # Slightly increased delay
        except types.errors.FloodWaitError as e:
             # Telegram is rate-limiting, wait the specified time + a buffer
             wait_time = e.seconds
             logger.warning(f"Flood wait ({wait_time}s) encountered during message clearing in chat {chat_id}. Pausing and retrying remaining.")
             await asyncio.sleep(wait_time + 1.5) # Wait + buffer
             # Add the *remaining* messages (including the one that failed) to the retry list
             try:
                 failed_index = messages_to_delete.index(msg)
                 messages_to_retry.extend(messages_to_delete[failed_index:])
             except ValueError: # Should not happen if msg is from the list
                 logger.error("Could not find message in list during flood wait handling.")
                 messages_to_retry.append(msg) # Retry the current one at least
             break # Stop processing the initial list and move to retry phase
        except (types.errors.MessageDeleteForbiddenError, types.errors.MessageIdInvalidError):
             # Cannot delete (permissions, message already gone), log and skip
             # Use getattr for safe access to id attribute
             logger.warning(f"Cannot delete message {getattr(msg, 'id', 'N/A')} (forbidden or invalid). Skipping.")
        except Exception as e:
             # Other errors during deletion (network, etc.) - add to retry list
             msg_id = getattr(msg, 'id', 'N/A')
             logger.warning(f"Could not delete message {msg_id}: {e}. Scheduling for retry.")
             messages_to_retry.append(msg)

    # --- Retry Phase (if any messages failed) ---
    if messages_to_retry:
        logger.info(f"Retrying deletion of {len(messages_to_retry)} messages in chat {chat_id} after initial attempt/flood wait.")
        await asyncio.sleep(1) # Extra buffer before retrying
        for msg in messages_to_retry:
             if not msg or not isinstance(msg, types.Message): continue
             msg_id = getattr(msg, 'id', 'N/A')
             try:
                 await msg.delete()
                 deleted_count += 1
                 await asyncio.sleep(0.3) # Slightly longer delay during retry
             except types.errors.FloodWaitError as e:
                  logger.error(f"Flood wait ({e.seconds}s) encountered *during retry* for message {msg_id}. Aborting further retries for this batch.")
                  await asyncio.sleep(e.seconds + 1) # Wait but stop retrying this batch
                  break # Stop retrying
             except Exception as e_retry:
                 # Log final failure for messages that couldn't be deleted even on retry
                 logger.warning(f"Could not delete message {msg_id} on retry: {e_retry}")

    # Log final result
    if deleted_count > 0:
        logger.info(f"Cleared {deleted_count} previous bot messages for chat {chat_id}.")
    # else: logger.info(f"Finished clear operation for chat {chat_id}, no messages were successfully deleted (or none needed deletion).")


async def store_response_message(chat_id: int, message: Optional[types.Message]):
    """
    Stores a message object to be potentially cleared later by auto_clear.

    Args:
        chat_id: The ID of the chat where the message was sent.
        message: The Telethon Message object to store.
    """
    # Validate input
    if not message or not isinstance(message, types.Message) or not chat_id:
        # logger.debug("store_response_message called with invalid message or chat_id.")
        return

    global previous_bot_messages
    # Initialize list for the chat if it doesn't exist
    if chat_id not in previous_bot_messages:
        previous_bot_messages[chat_id] = []

    # Add the message object to the list for this chat
    # Avoid adding duplicates just in case (e.g., if called multiple times for same msg)
    if message not in previous_bot_messages[chat_id]:
        previous_bot_messages[chat_id].append(message)
        logger.debug(f"Stored message {message.id} for potential auto-clearing in chat {chat_id}. (Total stored: {len(previous_bot_messages[chat_id])})")
    # else: logger.debug(f"Message {message.id} already stored for chat {chat_id}.") # Optional


# =============================================================================
#                         COMMAND HANDLERS
# =============================================================================

# Decorator to register the main message handler
@client.on(events.NewMessage)
async def handle_message(event: events.NewMessage.Event):
    """Main handler for incoming messages. Checks authorization, parses commands, and dispatches."""

    # --- Basic Filters ---
    # Ignore if bot is globally disabled
    if not config.get("bot_enabled", True): return
    # Ignore empty messages, non-text messages, or messages sent via other bots
    # Also ignore messages without a sender (e.g., some channel posts)
    if not event.message or not event.message.text or event.message.via_bot or not event.sender_id:
        # Log if needed: logger.debug(f"Ignoring message {event.id}: No text, via bot, or no sender.")
        return

    # Determine if the message is from the userbot account itself
    is_self = event.message.out # True if the message is outgoing (sent by the user account running the bot)
    sender_id = event.sender_id # Already checked it exists above

    # --- Authorization Check ---
    # Allow self, or if whitelist disabled, or if sender is in the ALLOWED_USERS dict
    is_authorised = is_self or (not config.get("whitelist_enabled", True)) or (sender_id in ALLOWED_USERS)

    if not is_authorised:
        # Log unauthorized attempts unless it's just the userbot itself sending non-commands in a group
        if not is_self:
            logger.warning(f"Ignoring unauthorized message from user: {sender_id} in chat {event.chat_id}")
        return # Ignore unauthorized users

    # --- Command Parsing ---
    message_text = event.message.text
    prefix = config.get("prefix", ",")

    # Ignore messages that don't start with the configured prefix
    if not message_text.startswith(prefix): return

    # Extract command and arguments
    command_string = message_text[len(prefix):].strip()
    if not command_string: return # Ignore if only prefix is sent

    parts = command_string.split(maxsplit=1) # Split only once: [command, rest_of_args]
    command = parts[0].lower() # Command is case-insensitive
    args_str = parts[1] if len(parts) > 1 else "" # The rest is the arguments string

    # Further split args_str respecting potential quotes later if needed, for now simple split
    args = args_str.split() # Simple space separation for now

    logger.info(f"Received command: '{command}', Args: {args}, User: {sender_id}, Chat: {event.chat_id}")

    # --- Self-Command Deletion ---
    # If the command was issued by the userbot account itself, delete the command message
    if is_self:
        try:
            await event.message.delete()
            logger.debug(f"Deleted self-command message {event.message.id}")
        except Exception as e_del:
            logger.warning(f"Failed to delete self-command message {event.message.id}: {e_del}")

    # --- Auto-Clear Previous Responses ---
    # If auto_clear is enabled, clear previous messages for commands that generate new output
    commands_to_clear_for = (
        "search", "see", "last", "list", "host", "download", "help", "dl",
        "clear" # Clear should also clear previous messages before showing its confirmation
    )
    if config.get("auto_clear", True) and command in commands_to_clear_for:
         logger.debug(f"Auto-clearing previous responses for '{command}' in chat {event.chat_id}")
         await clear_previous_responses(event.chat_id)

    # --- Command Dispatching ---
    # Map command strings (and aliases) to their handler functions
    handlers = {
        "search": handle_search,        # Search for music
        "see": handle_see,              # Get details of an item
        "download": handle_download,    # Download track or album
        "dl": handle_download,          # Alias for download
        "add": handle_add,              # Add user to whitelist (owner only)
        "delete": handle_delete,        # Remove user from whitelist (owner only)
        "del": handle_delete,           # Alias for delete
        "list": handle_list,            # List whitelisted users
        "help": handle_help,            # Show help message
        "last": handle_last,            # Show recently downloaded tracks
        "host": handle_host,            # Show system information
        "clear": handle_clear,          # Manually clear previous responses / confirm auto-clear
        # Add other commands/aliases here
    }

    handler_func = handlers.get(command)

    if handler_func:
        try:
            # Execute the appropriate handler function, passing event and args list
            await handler_func(event, args)
        except Exception as e_handler:
            # Catch unexpected errors within the handler function itself
            logger.error(f"Error executing handler for command '{command}': {e_handler}", exc_info=True)
            try:
                # Try to notify the user about the internal error
                # Use code block for exception type/message
                error_msg_text = f"❌ Произошла внутренняя ошибка при обработке команды `{command}`:\n`{type(e_handler).__name__}: {e_handler}`"
                error_msg = await event.reply(error_msg_text)
                await store_response_message(event.chat_id, error_msg) # Store error message for potential clearing
            except Exception as notify_e:
                logger.error(f"Failed to notify user about handler error for command '{command}': {notify_e}")
    else:
        # --- Unknown Command ---
        # Only respond "Unknown command" if auto-clear isn't active for this command
        # (which it isn't, as unknown commands aren't in commands_to_clear_for)
        response_msg_text = f"⚠️ Неизвестная команда: `{command}`.\nИспользуйте `{prefix}help` для списка команд."
        response_msg = await event.reply(response_msg_text)
        await store_response_message(event.chat_id, response_msg)
        logger.warning(f"Unknown command '{command}' received from {sender_id}")

# -------------------------
# Command: clear
# -------------------------
async def handle_clear(event: events.NewMessage.Event, args: List[str]):
    """Clears previous bot responses in the chat."""
    # Note: If auto_clear is ON, the main handler already called clear_previous_responses.
    # This command then just serves as confirmation or manual trigger if auto_clear is OFF.
    sent_msg = None
    if config.get("auto_clear", True):
        # Send a confirmation message that deletes itself.
        sent_msg = await event.respond("ℹ️ Предыдущие ответы очищаются автоматически перед большинством команд.", delete_in=10)
        logger.info(f"Executed 'clear' command (auto-clear enabled) in chat {event.chat_id}. Responses likely cleared already by main handler.")
    else:
        # If auto-clear is off, perform the clear manually now.
        logger.info(f"Executing manual clear via command in chat {event.chat_id} (auto-clear disabled).")
        # Call the clear function explicitly (main handler didn't do it)
        await clear_previous_responses(event.chat_id)
        sent_msg = await event.respond("✅ Предыдущие ответы бота очищены вручную.", delete_in=10)
    # Do not store the self-deleting confirmation messages using store_response_message

# -------------------------
# Command: search (-t, -a, -p, -e)
# -------------------------
async def handle_search(event: events.NewMessage.Event, args: List[str]):
    """Handles the search command to find tracks, albums, playlists, or artists."""
    valid_flags = {"-t", "-a", "-p", "-e"}
    prefix = config.get("prefix", ",") # Get current prefix for usage message

    # --- Argument Validation ---
    if len(args) < 2 or args[0] not in valid_flags:
        usage_text = (f"**Использование:** `{prefix}search -t|-a|-p|-e <поисковый запрос>`\n\n"
                      f"  `-t`: Поиск треков\n"
                      f"  `-a`: Поиск альбомов\n"
                      f"  `-p`: Поиск плейлистов\n"
                      f"  `-e`: Поиск исполнителей")
        response_msg = await event.reply(usage_text)
        await store_response_message(event.chat_id, response_msg)
        return

    search_type_flag = args[0]
    query = " ".join(args[1:]).strip()
    if not query:
        # Use consistent warning style
        response_msg = await event.reply(f"⚠️ Не указан поисковый запрос после флага `{search_type_flag}`.")
        await store_response_message(event.chat_id, response_msg)
        return

    # --- Progress Setup ---
    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    sent_message = None # To track the final message for storing

    try:
        if use_progress:
            statuses = {"Поиск": "⏳ Ожидание...", "Форматирование": "⏸️"}
            # Initial progress message
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))
            # Don't store the progress message itself yet, only the final result/error

        # Update status: Searching
        if use_progress:
            statuses["Поиск"] = f"🔄 Поиск '{query[:30]}...'..." if len(query)>33 else f"🔄 Поиск '{query}'..."
            await update_progress(progress_message, statuses)

        # --- Perform Search ---
        # Use configured limit, clamped between 1 and 15 for safety/performance
        search_limit = min(max(1, config.get("default_search_limit", 8)), 15)
        results = await search(query, search_type_flag, search_limit)

        # Update status: Search complete
        if use_progress:
            search_status = f"✅ Найдено: {len(results)}" if results else "ℹ️ Ничего не найдено"
            statuses["Поиск"] = search_status
            statuses["Форматирование"] = "🔄 Подготовка..." if results else "➖" # Only format if results exist
            await update_progress(progress_message, statuses)

        # --- Handle No Results ---
        if not results:
            final_message = f"ℹ️ По запросу `{query}` ничего не найдено."
            if progress_message:
                await progress_message.edit(final_message)
                sent_message = progress_message # Progress msg becomes the final message
            else:
                sent_message = await event.reply(final_message)
            # No need to return here, finally block will handle storing if needed
        else:
            # --- Format Results ---
            response_lines = []
            # Limit the number of results displayed in the message
            display_limit = min(len(results), MAX_SEARCH_RESULTS_DISPLAY)

            type_labels = {"-t": "Треки", "-a": "Альбомы", "-p": "Плейлисты", "-e": "Исполнители"}
            response_text = f"**🔎 Результаты поиска ({type_labels.get(search_type_flag, '?')}) для `{query}`:**\n" # Use code for query

            for i, item in enumerate(results[:display_limit]):
                if not item or not isinstance(item, dict):
                    logger.warning(f"Skipping invalid item in search results: {item}")
                    continue # Skip None or invalid items

                line = f"{i + 1}. " # Numbered list item
                try:
                    # Format based on the search type flag
                    if search_type_flag == "-t": # Tracks
                        title = item.get('title', 'Unknown Title')
                        artists_list = item.get('artists', [])
                        artists = ', '.join([a.get('name', '').strip() for a in artists_list if isinstance(a, dict) and a.get('name')]) or 'Unknown Artist'
                        vid = item.get('videoId')
                        link = f"https://music.youtube.com/watch?v={vid}" if vid else '`Нет ID`'
                        line += f"**{title}** - {artists}\n   └ [🔗 Ссылка]({link})"

                    elif search_type_flag == "-a": # Albums
                        title = item.get('title', 'Unknown Album')
                        artists_list = item.get('artists', [])
                        artists = ', '.join([a.get('name', '').strip() for a in artists_list if isinstance(a, dict) and a.get('name')]) or 'Unknown Artist'
                        bid = item.get('browseId')
                        year = item.get('year', '')
                        link = f"https://music.youtube.com/browse/{bid}" if bid else '`Нет ID`'
                        line += f"**{title}** - {artists}" + (f" ({year})" if year else "") + f"\n   └ [🔗 Ссылка]({link})"

                    elif search_type_flag == "-e": # Artists
                        artist_name = item.get('artist', 'Unknown Artist')
                        bid = item.get('browseId')
                        link = f"https://music.youtube.com/channel/{bid}" if bid else '`Нет ID`'
                        if artist_name != 'Unknown Artist' and bid:
                            line += f"**{artist_name}**\n   └ [🔗 Ссылка]({link})"
                        else: line = None # Skip if essential info missing

                    elif search_type_flag == "-p": # Playlists
                        title = item.get('title', 'Unknown Playlist')
                        author_data = item.get('author')
                        author = 'Unknown Author'
                        if isinstance(author_data, list) and author_data:
                            author = author_data[0].get('name', '?') if isinstance(author_data[0], dict) else '?'
                        elif isinstance(author_data, dict): author = author_data.get('name', '?')
                        pid = item.get('browseId')
                        link_pid = pid.replace('VL', '') if pid and isinstance(pid, str) else None
                        link = f"https://music.youtube.com/playlist?list={link_pid}" if link_pid else '`Нет ID`'
                        line += f"**{title}** (Автор: {author})\n   └ [🔗 Ссылка]({link})" # Slightly changed format

                    if line: response_lines.append(line) # Add the formatted line

                except Exception as fmt_e:
                     logger.error(f"Error formatting search result item {i+1}: {item} - {fmt_e}")
                     response_lines.append(f"{i + 1}. ⚠️ Ошибка форматирования результата.")

            # Join the formatted lines with double newlines for better readability
            response_text += "\n\n".join(response_lines)
            # Indicate if more results were found but not displayed
            if len(results) > display_limit:
                response_text += f"\n\n... и еще {len(results) - display_limit}."

            # Update status: Formatting complete
            if use_progress:
                statuses["Форматирование"] = "✅ Готово"
                await update_progress(progress_message, statuses)
                # Edit the progress message to show the final results
                await progress_message.edit(response_text, link_preview=False)
                sent_message = progress_message
            else:
                # Send results as a new message if progress wasn't used
                sent_message = await event.reply(response_text, link_preview=False)

    # --- Error Handling ---
    except ValueError as e: # Handle known errors like invalid flag
        error_text = f"⚠️ Ошибка поиска: {e}"
        logger.warning(error_text)
        if progress_message:
            await progress_message.edit(error_text)
            sent_message = progress_message
        else:
            sent_message = await event.reply(error_text)
    except Exception as e: # Handle unexpected errors
        logger.error(f"Неожиданная ошибка в команде search: {e}", exc_info=True)
        error_text = f"❌ Произошла неожиданная ошибка во время поиска:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
            # Mark steps as failed
            statuses["Поиск"] = str(statuses.get("Поиск", "⏸️")).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌")
            statuses["Форматирование"] = "❌"
            try: await update_progress(progress_message, statuses)
            except: pass # Ignore if update fails during error reporting
            try:
                # Append error to the progress message if possible
                current_text = getattr(progress_message, 'text', '') # Get current text safely
                await progress_message.edit(f"{current_text}\n\n{error_text}")
                sent_message = progress_message
            except: # If editing fails (e.g., message deleted), send new
                 sent_message = await event.reply(error_text)
        else:
            sent_message = await event.reply(error_text)
    finally:
        # Store the final message (results or error) for potential clearing
        if sent_message:
            await store_response_message(event.chat_id, sent_message)


# -------------------------
# Command: see (-t, -a, -p, -e) [-i]
# -------------------------
async def handle_see(event: events.NewMessage.Event, args: List[str]):
    """Handles the 'see' command to display detailed info about an entity, optionally with cover art."""
    valid_flags = {"-t", "-a", "-p", "-e"}
    prefix = config.get("prefix", ",")

    # --- Argument Parsing ---
    if len(args) < 1:
        usage = (f"**Использование:** `{prefix}see [-t|-a|-p|-e] [-i] <ID или ссылка>`\n\n"
                 f"  `-t`: Трек\n  `-a`: Альбом\n  `-p`: Плейлист\n  `-e`: Исполнитель\n"
                 f"  `-i`: Включить обложку (если доступна)")
        await store_response_message(event.chat_id, await event.reply(usage))
        return

    entity_type_hint_flag = next((arg for arg in args if arg in valid_flags), None)
    include_cover = "-i" in args
    link_or_id_arg = next((arg for arg in reversed(args) if arg not in valid_flags and arg != "-i"), None)

    if not link_or_id_arg:
        # Use consistent warning style
        await store_response_message(event.chat_id, await event.reply("⚠️ Не указана ссылка или ID для просмотра."))
        return

    hint_map = {"-t": "track", "-a": "album", "-p": "playlist", "-e": "artist"}
    entity_type_hint = hint_map.get(entity_type_hint_flag) if entity_type_hint_flag else None

    # --- Extract Entity ID from Link or Argument ---
    entity_id = link_or_id_arg # Assume it's an ID initially
    id_patterns = [
        r"watch\?v=([A-Za-z0-9_-]{11})",       # Standard watch URL (Video ID)
        r"youtu\.be/([A-Za-z0-9_-]{11})",      # Short URL (Video ID)
        r"playlist\?list=([A-Za-z0-9_-]+)",    # Playlist URL (Playlist ID, PL or VL or OLAK5uy_)
        r"browse/([A-Za-z0-9_-]+)",          # Browse URL (Album MPRE/MPLA or Artist UC)
        r"channel/([A-Za-z0-9_-]+)",         # Channel URL (Artist UC)
    ]
    id_found_via_regex = False
    for pattern in id_patterns:
        match = re.search(pattern, link_or_id_arg)
        if match:
            entity_id = match.group(1) # Use the captured group as the ID
            id_found_via_regex = True
            logger.debug(f"Extracted ID '{entity_id}' using pattern '{pattern}' from '{link_or_id_arg}'")
            break # Stop after first successful match

    # Basic validation of the final ID (extracted or provided directly)
    if not isinstance(entity_id, str) or not re.fullmatch(r"^[A-Za-z0-9_-]+$", entity_id):
        # Use consistent warning style
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не удалось распознать или неверный формат ID: `{entity_id}`."))
        return
    # --- End ID Extraction ---

    # --- Progress Setup ---
    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    temp_thumb_file, processed_thumb_file, final_sent_message = None, None, None
    files_to_clean_on_exit = []

    try:
        if use_progress:
            statuses = {"Получение данных": "⏳ Ожидание...", "Форматирование": "⏸️"}
            if include_cover: statuses["Обложка"] = "⏸️" # Add cover status only if requested
            progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

        if use_progress: statuses["Получение данных"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)

        # --- Get Entity Information ---
        entity_info = await get_entity_info(entity_id, entity_type_hint)

        if not entity_info:
            result_text = f"ℹ️ Не удалось найти информацию для ID/ссылки: `{entity_id}`"
            final_sent_message = await (progress_message.edit(result_text) if progress_message else event.reply(result_text))
            # Go to finally block for storing
        else:
            actual_entity_type = entity_info.get('_entity_type', 'unknown') # Get detected type
            if use_progress: statuses["Получение данных"] = f"✅ ({actual_entity_type})"; statuses["Форматирование"] = "🔄 Подготовка..."; await update_progress(progress_message, statuses)

            # --- Format Response Based on Entity Type ---
            response_text = ""
            thumbnail_url = None

            # --- Extract Thumbnail URL ---
            thumbnails_list = None
            possible_thumbnail_locations = [
                entity_info.get('thumbnails'),
                entity_info.get('thumbnail', {}).get('thumbnails'),
                entity_info.get('videoDetails', {}).get('thumbnails'),
            ]
            for potential_list in possible_thumbnail_locations:
                if isinstance(potential_list, list) and potential_list:
                    thumbnails_list = potential_list
                    break

            if thumbnails_list:
                try:
                    highest_res_thumb = sorted(thumbnails_list, key=lambda x: x.get('width', 0) * x.get('height', 0), reverse=True)[0]
                    thumbnail_url = highest_res_thumb.get('url')
                except (IndexError, KeyError, TypeError) as e_thumb:
                    logger.warning(f"Could not get highest res thumbnail URL from list: {e_thumb}. Falling back.")
                    if thumbnails_list: thumbnail_url = thumbnails_list[-1].get('url')
            if thumbnail_url: logger.debug(f"Found thumbnail URL: {thumbnail_url}")

            # --- Helper for Formatting Artist Names ---
            def format_artists(data: Optional[Union[List[Dict], Dict, str]]) -> str:
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

            # --- Format Entity Details ---
            if actual_entity_type == 'track':
                details = entity_info.get('videoDetails') or entity_info
                title = details.get('title', 'Неизвестно')
                artists = format_artists(details.get('artists') or details.get('author'))
                album_info = details.get('album')
                album_name = album_info.get('name') if isinstance(album_info, dict) else None
                duration_s = None
                try: duration_s = int(details.get('lengthSeconds', 0))
                except (ValueError, TypeError): pass
                duration_fmt = None
                if duration_s and duration_s > 0:
                    td = datetime.timedelta(seconds=duration_s)
                    if td.total_seconds() >= 3600: duration_fmt = str(td).split('.')[0] # H:MM:SS
                    else: duration_fmt = f"{int(td.total_seconds() // 60)}:{int(td.total_seconds() % 60):02d}" # M:SS
                video_id = details.get('videoId', entity_id)
                link = f"https://music.youtube.com/watch?v={video_id}"

                response_text = f"**Трек:** {title}\n**Исполнитель:** {artists}\n"
                if album_name: response_text += f"**Альбом:** {album_name}\n"
                if duration_fmt: response_text += f"**Длительность:** {duration_fmt}\n"
                response_text += f"**Ссылка:** `{link}`"

            elif actual_entity_type == 'album':
                title = entity_info.get('title', 'Неизвестный Альбом')
                artists = format_artists(entity_info.get('artists') or entity_info.get('author'))
                year = entity_info.get('year')
                count = entity_info.get('trackCount') or len(entity_info.get('tracks', []))
                bid_raw = entity_info.get('audioPlaylistId') or entity_info.get('browseId') or entity_id
                bid = bid_raw.replace('RDAMPL', '') if isinstance(bid_raw, str) else entity_id
                link = f"https://music.youtube.com/browse/{bid}"

                response_text = f"**Альбом:** {title}\n**Исполнитель:** {artists}\n"
                if year: response_text += f"**Год:** {year}\n"
                if count: response_text += f"**Треков:** {count}\n"
                response_text += f"**Ссылка:** `{link}`\n"
                tracks = entity_info.get('tracks', [])
                if tracks:
                    response_text += f"\n**Треки (первые {min(len(tracks), 5)}):**\n"
                    for t in tracks[:5]:
                        t_title = t.get('title','?')
                        t_artists = format_artists(t.get('artists'))
                        if t_artists == 'Неизвестно': t_artists = artists
                        t_id = t.get('videoId')
                        t_link = f"[▶️]({f'https://music.youtube.com/watch?v={t_id}'})" if t_id else ""
                        response_text += f"• {t_title} ({t_artists}) {t_link}\n"
                response_text = response_text.strip()

            elif actual_entity_type == 'playlist':
                 title = entity_info.get('title', 'Неизвестный Плейлист')
                 author = format_artists(entity_info.get('author'))
                 count = entity_info.get('trackCount') or len(entity_info.get('tracks', []))
                 pid = entity_info.get('id', entity_id).replace('VL', '') if isinstance(entity_info.get('id'), str) else entity_id
                 link = f"https://music.youtube.com/playlist?list={pid}"

                 response_text = f"**Плейлист:** {title}\n**Автор:** {author}\n"
                 if count: response_text += f"**Треков:** {count}\n"
                 response_text += f"**Ссылка:** `{link}`\n"
                 tracks = entity_info.get('tracks', [])
                 if tracks:
                     response_text += f"\n**Треки (первые {min(len(tracks), 5)}):**\n"
                     for t in tracks[:5]:
                         t_title = t.get('title','?')
                         t_artists = format_artists(t.get('artists'))
                         t_id = t.get('videoId')
                         t_link = f"[▶️]({f'https://music.youtube.com/watch?v={t_id}'})" if t_id else ""
                         response_text += f"• {t_title} ({t_artists}) {t_link}\n"
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
                 link = f"https://music.youtube.com/channel/{cid}"

                 response_text = f"**Исполнитель:** {name}\n"
                 if subs: response_text += f"**Подписчики:** {subs}\n"
                 response_text += f"**Ссылка:** `{link}`\n"
                 if songs:
                     response_text += f"\n**Популярные треки (до {min(len(songs), songs_limit)}):**\n"
                     for s in songs[:songs_limit]:
                         s_title = s.get('title','?')
                         s_id = s.get('videoId')
                         s_link = f"[▶️]({f'https://music.youtube.com/watch?v={s_id}'})" if s_id else ""
                         response_text += f"• {s_title} {s_link}\n"
                 if albums:
                     if songs: response_text += "\n"
                     response_text += f"**Альбомы (до {min(len(albums), albums_limit)}):**\n"
                     for a in albums[:albums_limit]:
                         a_title = a.get('title','?')
                         a_id = a.get('browseId')
                         a_link = f"[💿]({f'https://music.youtube.com/browse/{a_id}'})" if a_id else ""
                         a_year = a.get('year','')
                         response_text += f"• {a_title}" + (f" ({a_year})" if a_year else "") + f" {a_link}\n"
                 response_text = response_text.strip()
            else:
                response_text = f"⚠️ Тип сущности '{actual_entity_type}' не поддерживается для отображения командой `see`.\nID: `{entity_id}`"
                logger.warning(f"Unsupported entity type for 'see' command display: {actual_entity_type}, ID: {entity_id}")

            if use_progress: statuses["Форматирование"] = "✅ Готово"; await update_progress(progress_message, statuses)

            # --- Send Response (Text or with Cover) ---
            if include_cover and thumbnail_url:
                if use_progress: statuses["Обложка"] = "🔄 Загрузка..."; await update_progress(progress_message, statuses)
                temp_thumb_file = await download_thumbnail(thumbnail_url)

                if temp_thumb_file:
                    files_to_clean_on_exit.append(temp_thumb_file)
                    if use_progress: statuses["Обложка"] = "🔄 Обработка..."; await update_progress(progress_message, statuses)

                    # --- Thumbnail Processing ---
                    if actual_entity_type == 'artist':
                        logger.debug(f"Artist entity detected. Using original downloaded thumbnail (no crop/conversion).")
                        processed_thumb_file = temp_thumb_file # Use the raw downloaded file path
                    else:
                        logger.debug(f"Non-artist entity type ({actual_entity_type}). Cropping thumbnail.")
                        processed_thumb_file = crop_thumbnail(temp_thumb_file)
                        if processed_thumb_file and processed_thumb_file != temp_thumb_file:
                            files_to_clean_on_exit.append(processed_thumb_file)

                    status_icon = "✅" if processed_thumb_file and os.path.exists(processed_thumb_file) else "⚠️"
                    if use_progress: statuses["Обложка"] = f"{status_icon} Готово"; await update_progress(progress_message, statuses)

                    # --- Send with Cover ---
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
                    else: # Thumbnail processing failed
                        logger.warning(f"Thumbnail processing failed for {os.path.basename(temp_thumb_file)}. Sending text only.")
                        if use_progress: statuses["Обложка"] = "❌ Ошибка обработки"; await update_progress(progress_message, statuses)
                        fallback_text = f"{response_text}\n\n_(Ошибка при обработке обложки)_"
                        final_sent_message = await (progress_message.edit(fallback_text, link_preview=False) if progress_message else event.reply(fallback_text, link_preview=False))
                else: # Thumbnail download failed
                     logger.warning(f"Thumbnail download failed for {thumbnail_url}. Sending text only.")
                     if use_progress: statuses["Обложка"] = "❌ Ошибка загрузки"; await update_progress(progress_message, statuses)
                     fallback_text = f"{response_text}\n\n_(Ошибка при загрузке обложки)_"
                     final_sent_message = await (progress_message.edit(fallback_text, link_preview=False) if progress_message else event.reply(fallback_text, link_preview=False))
            else: # No cover requested or no thumbnail URL found
                final_sent_message = await (progress_message.edit(response_text, link_preview=False) if progress_message else event.reply(response_text, link_preview=False))

    # --- General Error Handling ---
    except Exception as e:
        logger.error(f"Unexpected error in handle_see for ID '{entity_id}': {e}", exc_info=True)
        error_text = f"❌ Произошла ошибка при получении информации:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
             # Mark all steps as failed
             for task in statuses: statuses[task] = str(statuses[task]).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌").replace("⏸️", "❌")
             try: await update_progress(progress_message, statuses)
             except: pass # Ignore errors updating progress during error handling
             try:
                 current_text = getattr(progress_message, 'text', '')
                 await progress_message.edit(f"{current_text}\n\n{error_text}")
                 final_sent_message = progress_message # Keep the progress message showing the error
             except: # If editing fails, send new
                 final_sent_message = await event.reply(error_text)
        else: # No progress message, just send error
             final_sent_message = await event.reply(error_text)
    finally:
        # Store the final message (result or error) for potential clearing
        if final_sent_message and (final_sent_message != progress_message or progress_message is not None):
            await store_response_message(event.chat_id, final_sent_message)

        # --- Cleanup temporary files created *during this command* ---
        if files_to_clean_on_exit:
            logger.debug(f"Running cleanup for handle_see (Files: {len(files_to_clean_on_exit)})")
            await cleanup_files(*files_to_clean_on_exit)


# -------------------------
# Helper: Send Single Track
# -------------------------
async def send_single_track(event: events.NewMessage.Event, info: Dict, file_path: str):
    """
    Handles sending a single downloaded audio file via Telegram.
    Includes fetching/processing thumbnail for Telegram preview, setting audio attributes,
    adding to recent downloads, and cleaning up associated files.
    Relies on yt-dlp having embedded metadata/thumbnail into the file itself via postprocessors.

    Args:
        event: The Telethon NewMessage event.
        info: The metadata dictionary from yt-dlp for the track.
        file_path: The path to the downloaded and processed audio file.
    """
    temp_telegram_thumb, processed_telegram_thumb = None, None
    # Files to clean: start with the audio file itself, add Telegram preview thumbs later
    # ВАЖНО: file_path добавляется здесь, но он может быть исключен из удаления ниже, если это opus
    files_to_clean = [file_path]
    title, performer, duration = "Неизвестно", "Неизвестно", 0 # Defaults for error cases

    try:
        # --- Input Validation ---
        if not info or not file_path or not os.path.exists(file_path):
             logger.error(f"send_single_track called with invalid info or missing file: Info={info is not None}, Path={file_path}")
             # Inform user about internal error, don't store this message
             await event.reply(f"❌ Ошибка: Не найден скачанный файл `{os.path.basename(file_path or 'N/A')}` для отправки. Сообщите администратору.")
             # Попытаемся удалить временные файлы, если они были созданы до этой ошибки
             await cleanup_files(*[f for f in files_to_clean if f != file_path]) # Удаляем все, КРОМЕ основного file_path
             return # Cannot proceed

        # --- Extract Metadata for Telegram Attributes ---
        # Use helper for consistency. yt-dlp should have embedded this, but we need it for Telegram attributes.
        title, performer, duration = extract_track_metadata(info)

        # --- Telegram Thumbnail Handling (for preview in chat) ---
        # This is separate from the thumbnail embedded *in* the audio file by yt-dlp.
        # We create a square JPEG preview specifically for Telegram.
        thumb_url = None
        thumbnails = info.get('thumbnails') or info.get('thumbnail', {}).get('thumbnails') # Look in common places
        if isinstance(thumbnails, list) and thumbnails:
            try: # Get highest resolution URL
                thumb_url = sorted(thumbnails, key=lambda x: x.get('width', 0) * x.get('height', 0), reverse=True)[0].get('url')
            except (IndexError, KeyError, TypeError): # Fallback to last item
                thumb_url = thumbnails[-1].get('url') if thumbnails else None

        if thumb_url:
            logger.debug(f"Attempting to download/process thumbnail for Telegram preview ('{title}')")
            temp_telegram_thumb = await download_thumbnail(thumb_url) # Download original
            if temp_telegram_thumb:
                files_to_clean.append(temp_telegram_thumb) # Add original download to cleanup
                # Crop to square JPEG for Telegram preview
                processed_telegram_thumb = crop_thumbnail(temp_telegram_thumb)
                if processed_telegram_thumb and processed_telegram_thumb != temp_telegram_thumb:
                    files_to_clean.append(processed_telegram_thumb) # Add cropped version to cleanup
                # else: Logged in crop_thumbnail if failed
            # else: Logged in download_thumbnail if failed
        # else: logger.debug(f"No thumbnail URL found in info dict for Telegram preview ('{title}')")

        # --- Send Audio File ---
        logger.info(f"Отправка аудио файла: {os.path.basename(file_path)} (Title: '{title}', Performer: '{performer}', Duration: {duration}s)")
        # Use the processed (cropped) thumb if available and exists, otherwise no thumb for Telegram preview
        final_telegram_thumb = processed_telegram_thumb if (processed_telegram_thumb and os.path.exists(processed_telegram_thumb)) else None

        # Use send_file with audio attributes and optional Telegram thumbnail
        sent_audio_msg = await client.send_file(
            event.chat_id,
            file=file_path,
            caption=BOT_CREDIT, # Use configured credit
            # Specify audio attributes for music player integration in Telegram
            attributes=[types.DocumentAttributeAudio(
                duration=duration,
                title=title,
                performer=performer
            )],
            thumb=final_telegram_thumb, # Use the square cropped JPEG for Telegram preview
        )
        logger.info(f"Аудио успешно отправлено: {os.path.basename(file_path)} (Message ID: {sent_audio_msg.id})")

        # --- Add to Recent Downloads History (if enabled) ---
        if config.get("recent_downloads", True):
             try:
                last_tracks = load_last_tracks() # Load current list
                timestamp = datetime.datetime.now().strftime("%H:%M-%d-%m") # HH:MM-DD-MM format
                artist_browse_id = None
                artists_list = info.get('artists')
                if isinstance(artists_list, list) and artists_list:
                     main_artist = next((a for a in artists_list if isinstance(a, dict) and a.get('id')), None)
                     if main_artist: artist_browse_id = main_artist['id']
                browse_id_to_save = artist_browse_id or info.get('channel_id') or 'N/A' # Fallback logic
                new_entry = [title, performer, browse_id_to_save, timestamp]
                last_tracks.insert(0, new_entry) # Add to beginning
                save_last_tracks(last_tracks) # Save (handles slicing)
             except Exception as e_last:
                 logger.error(f"Не удалось обновить список последних треков ({title}): {e_last}", exc_info=True)

    # --- Specific Error Handling for Sending ---
    except types.errors.MediaCaptionTooLongError:
         logger.error(f"Ошибка отправки {os.path.basename(file_path)}: подпись (caption) слишком длинная.")
         await store_response_message(event.chat_id, await event.reply(f"⚠️ Не удалось отправить `{title}`: подпись слишком длинная."))
    except types.errors.WebpageMediaEmptyError:
          logger.error(f"Ошибка отправки {os.path.basename(file_path)}: WebpageMediaEmptyError. Попытка повторной отправки без Telegram-превью...")
          try:
              # Retry sending the file without the explicit Telegram thumbnail
              await client.send_file(
                  event.chat_id, file_path, caption=BOT_CREDIT,
                  attributes=[types.DocumentAttributeAudio(duration=duration, title=title, performer=performer)],
                  thumb=None # Explicitly no Telegram thumb on retry
              )
              logger.info(f"Повторная отправка без Telegram-превью успешна: {os.path.basename(file_path)}")
              # Still try to add to recent downloads if retry succeeds
              if config.get("recent_downloads", True):
                    try: # Simplified repeat
                         last_tracks = load_last_tracks(); timestamp = datetime.datetime.now().strftime("%H:%M-%d-%m")
                         browse_id_to_save = info.get('channel_id') or 'N/A' # Simplified browse_id logic for retry
                         artists_list_retry = info.get('artists') # Check artists again
                         if isinstance(artists_list_retry, list) and artists_list_retry:
                             main_artist_retry = next((a for a in artists_list_retry if isinstance(a, dict) and a.get('id')), None)
                             if main_artist_retry: browse_id_to_save = main_artist_retry['id']
                         last_tracks.insert(0, [title, performer, browse_id_to_save, timestamp]); save_last_tracks(last_tracks[:5])
                    except Exception as e_last_retry: logger.error(f"Не удалось обновить 'last' после повторной отправки: {e_last_retry}")
          except Exception as retry_e:
              logger.error(f"Повторная отправка {os.path.basename(file_path)} (без превью) также не удалась: {retry_e}", exc_info=True)
              await store_response_message(event.chat_id, await event.reply(f"❌ Не удалось отправить `{title}` даже без превью: {retry_e}"))

    # --- General Error Handling ---
    except Exception as e:
        logger.error(f"Неожиданная ошибка при отправке трека {os.path.basename(file_path or 'N/A')}: {e}", exc_info=True)
        try:
            await store_response_message(event.chat_id, await event.reply(f"❌ Не удалось отправить трек `{title}`: {e}"))
        except Exception as notify_e:
            logger.error(f"Не удалось уведомить пользователя об ошибке отправки трека: {notify_e}")

    finally:
        # --- Cleanup ---
        # Определяем, какие расширения мы хотим СОХРАНИТЬ
        extensions_to_keep = ['.opus'] # Можно добавить другие, например: ['.opus', '.flac']
        keep_this_audio_file = False

        if file_path and os.path.exists(file_path): # Проверяем, что путь валиден и файл существует
            try:
                _, file_extension = os.path.splitext(file_path)
                if file_extension.lower() in extensions_to_keep:
                    keep_this_audio_file = True
                    logger.info(f"Файл '{os.path.basename(file_path)}' будет сохранен (расширение {file_extension}).")
            except Exception as e_ext:
                logger.warning(f"Не удалось проверить расширение файла {file_path} для сохранения: {e_ext}")

        # Формируем финальный список файлов для удаления
        # Включаем все временные файлы (превью), но аудиофайл - только если его не нужно сохранять
        final_cleanup_list = []
        for f in files_to_clean: # files_to_clean содержит file_path и временные превью
            if f == file_path:
                if not keep_this_audio_file:
                    # Добавляем аудиофайл на удаление, ТОЛЬКО если его НЕ нужно сохранять
                    final_cleanup_list.append(f)
            elif f: # Добавляем другие файлы из списка (временные превью), если они не None
                final_cleanup_list.append(f)

        # Выполняем очистку, только если есть что удалять
        if final_cleanup_list:
            logger.debug(f"Запуск очистки для send_single_track (Файлов: {len(final_cleanup_list)})")
            await cleanup_files(*final_cleanup_list)
        else:
            if keep_this_audio_file:
                 logger.debug(f"Очистка для send_single_track пропущена, т.к. остался только сохраненный файл: {os.path.basename(file_path)}")
            else:
                 logger.debug("Очистка для send_single_track: Нет файлов для удаления.")

# -------------------------
# Command: download (-t, -a) / dl
# -------------------------
async def handle_download(event: events.NewMessage.Event, args: List[str]):
    """Handles the download command for single tracks or entire albums."""
    valid_flags = {"-t", "-a"}
    prefix = config.get("prefix", ",")

    # --- Argument Validation ---
    if len(args) < 2 or args[0] not in valid_flags:
        usage = (f"**Использование:** `{prefix}download -t|-a <ссылка>`\n\n"
                 f"  `-t`: Скачать трек по ссылке\n"
                 f"  `-a`: Скачать альбом по ссылке (на страницу `/browse/MPRE...` или `/playlist/...`)")
        await store_response_message(event.chat_id, await event.reply(usage))
        return
    download_type_flag = args[0]
    link = args[-1]
    if not isinstance(link, str) or not link.startswith("http"):
        await store_response_message(event.chat_id, await event.reply(f"⚠️ Не найдена http(s) ссылка для скачивания в аргументах: `{link}`."))
        return

    # --- Progress Setup ---
    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    final_sent_message: Optional[types.Message] = None # To store potential final error message

    try:
        # ===========================
        # --- DOWNLOAD SINGLE TRACK (-t) ---
        # ===========================
        if download_type_flag == "-t":
            if use_progress:
                statuses = {"Скачивание и обработка": "⏳ Ожидание...", "Отправка": "⏸️"}
                progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

            if use_progress: statuses["Скачивание и обработка"] = "🔄 Запрос..."; await update_progress(progress_message, statuses)

            loop = asyncio.get_running_loop()
            # download_track handles download + postprocessing (incl. metadata/thumb embed)
            info, file_path = await loop.run_in_executor(None, functools.partial(download_track, link))

            if not file_path or not info:
                fail_reason = "yt-dlp не смог скачать/обработать трек"
                if info and not file_path: fail_reason = "yt-dlp скачал, но не удалось найти финальный аудио файл"
                elif not info: fail_reason = "yt-dlp не вернул информацию о треке"
                logger.error(f"Download failed for link {link}. Reason: {fail_reason}")
                raise Exception(f"Не удалось скачать/обработать трек. {fail_reason}")

            file_basename = os.path.basename(file_path)
            logger.info(f"Track download and processing successful: {file_basename}")
            if use_progress:
                 display_name = (file_basename[:30] + '...') if len(file_basename) > 33 else file_basename
                 statuses["Скачивание и обработка"] = f"✅ ({display_name})"
                 statuses["Отправка"] = "🔄 Подготовка..."
                 await update_progress(progress_message, statuses)

            # send_single_track handles sending, Telegram preview thumb, recent list, and cleanup
            await send_single_track(event, info, file_path)

            if use_progress: statuses["Отправка"] = "✅ Готово"; await update_progress(progress_message, statuses)
            if progress_message:
                 await asyncio.sleep(1); await progress_message.delete(); progress_message = None


        # ===========================
        # --- DOWNLOAD ALBUM (-a) - SEQUENTIAL SEND ---
        # ===========================
        elif download_type_flag == "-a":
            # --- Extract Album/Playlist ID ---
            album_browse_id = None
            try:
                # Prioritize MPRE browse IDs if found
                album_bid_match = re.search(r"browse/(MPRE[A-Za-z0-9_-]+)", link)
                playlist_bid_match = re.search(r"playlist\?list=([A-Za-z0-9_-]+)", link)

                if album_bid_match:
                    album_browse_id = album_bid_match.group(1)
                    logger.debug(f"Using album browse ID from link: {album_browse_id}")
                elif playlist_bid_match:
                     album_browse_id = playlist_bid_match.group(1) # Accept any playlist ID
                     logger.debug(f"Using playlist ID from link: {album_browse_id}")
                else:
                    # Try to extract *any* browse ID as a last resort
                    fallback_browse_match = re.search(r"browse/([A-Za-z0-9_-]+)", link)
                    if fallback_browse_match:
                         album_browse_id = fallback_browse_match.group(1)
                         logger.warning(f"Using potentially non-album browse ID as fallback: {album_browse_id}")
                    else:
                         raise ValueError("Не удалось найти ID альбома (MPRE...) или плейлиста (list=...) в ссылке.")
            except ValueError as e_parse:
                 await store_response_message(event.chat_id, await event.reply(f"⚠️ {e_parse} Ссылка: `{link}`"))
                 return
            if not album_browse_id: # Should be caught above
                 await store_response_message(event.chat_id, await event.reply(f"⚠️ Не удалось извлечь ID из ссылки: `{link}`"))
                 return

            # --- Album Download Variables ---
            album_title = album_browse_id # Default title
            total_tracks = 0 # Will be updated by callback
            downloaded_count = 0 # Tracks successfully downloaded
            sent_count = 0 # Tracks successfully sent
            progress_callback = None # Define placeholder for callback

            # --- Progress Callback Setup ---
            statuses = {} # Define statuses dict here
            if use_progress: # Only define the callback if progress is enabled
                async def album_progress_updater(status_key, **kwargs):
                    # Use nonlocal to modify variables in handle_download scope
                    nonlocal total_tracks, downloaded_count, sent_count, album_title
                    if not use_progress or not progress_message: return

                    current_statuses = statuses
                    try:
                        if status_key == "analysis_complete":
                            total_tracks = kwargs.get('total_tracks', 0)
                            temp_title = kwargs.get('title', album_browse_id)
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
                            curr_send = kwargs.get('current', sent_count + 1)
                            title = kwargs.get('title', '?')
                            current_statuses["Прогресс"] = f"📤 Отправляем {curr_send}/{downloaded_count} - '{title}'"
                        elif status_key == "track_sent":
                            curr_sent = kwargs.get('current', sent_count)
                            title = kwargs.get('title', '?')
                            current_statuses["Прогресс"] = f"✔️ Отправлен {curr_sent}/{downloaded_count} - '{title}'"
                        elif status_key == "track_failed":
                            curr_fail = kwargs.get('current', downloaded_count + 1)
                            title = kwargs.get('title', '?')
                            current_statuses["Прогресс"] = f"⚠️ Ошибка {curr_fail}/{total_tracks} - '{title}'"
                        elif status_key == "album_error":
                            err_msg = kwargs.get('error', 'Неизвестная ошибка')
                            current_statuses["Альбом"] = f"❌ Ошибка: {err_msg[:40]}"
                            current_statuses["Прогресс"] = "⏹️ Остановлено"

                        await update_progress(progress_message, current_statuses)
                    except Exception as e_prog:
                        logger.error(f"Ошибка при обновлении прогресса скачивания альбома: {e_prog}")
                # Assign the actual function if progress is enabled
                progress_callback = album_progress_updater
            # --- End Callback Setup ---

            # --- Initial Setup ---
            if use_progress:
                statuses = {"Альбом": f"🔄 Анализ ID '{album_browse_id}'...", "Прогресс": "⏸️"}
                progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

            logger.info(f"Starting sequential download/send for album/playlist ID: {album_browse_id}")

            # --- Download All Tracks First ---
            # Pass the potentially defined progress_callback
            downloaded_tuples = await download_album_tracks(album_browse_id, progress_callback)
            downloaded_count = len(downloaded_tuples) # Update based on actual results

            if downloaded_count == 0:
                # Error already logged inside download_album_tracks if it failed
                raise Exception(f"Не удалось скачать ни одного трека для `{album_title}`.")

            # --- Send Tracks Sequentially ---
            logger.info(f"Starting sequential sending of {downloaded_count} downloaded tracks for '{album_title}'...")

            # Optional: Send album cover message (if needed) - not implemented here for simplicity

            for i, (info, file_path) in enumerate(downloaded_tuples):
                current_send_num = i + 1
                # Get title from downloaded info for progress/logging
                track_title_send = info.get('title', os.path.basename(file_path)) if info else os.path.basename(file_path)

                if not file_path or not os.path.exists(file_path):
                     logger.error(f"File path missing or invalid for track {current_send_num}/{downloaded_count}: {file_path}. Skipping send.")
                     # Update progress to show skip/error?
                     if progress_callback:
                          await progress_callback("track_failed", current=current_send_num, title=f"{track_title_send} (File Missing)")
                     continue # Skip this file

                # Update progress: Sending this track
                if progress_callback: # Check if callback exists
                    await progress_callback("track_sending", current=current_send_num, title=track_title_send)

                # --- Use send_single_track ---
                # This handles the actual sending, Telegram thumbnail, recent list update, AND cleanup for this track's files (unless it's .opus).
                await send_single_track(event, info, file_path)
                # Increment sent count *after* send_single_track finishes (assuming it didn't raise a critical error)
                sent_count += 1

                # Update progress: Sent this track
                if progress_callback:
                     await progress_callback("track_sent", current=sent_count, title=track_title_send)

                await asyncio.sleep(0.5) # Small delay between sending tracks

            # --- Final Album Progress Update ---
            if use_progress and progress_message: # Check if progress message still exists
                final_icon = "✅" if sent_count == downloaded_count else "⚠️"
                statuses["Прогресс"] = f"{final_icon} Завершено: Отправлено {sent_count}/{downloaded_count} треков."
                try:
                    await update_progress(progress_message, statuses)
                    await asyncio.sleep(5) # Keep final status visible
                    await progress_message.delete(); progress_message = None
                except Exception as e_final_prog:
                     logger.warning(f"Could not update/delete final progress message: {e_final_prog}")


    # --- General Error Handling ---
    except Exception as e:
        logger.error(f"Ошибка при выполнении команды download (Type: {download_type_flag}, Link: {link}): {e}", exc_info=True)
        error_prefix = "⚠️" if isinstance(e, (ValueError, FileNotFoundError)) else "❌"
        error_text = f"{error_prefix} Ошибка при скачивании/отправке:\n`{type(e).__name__}: {e}`"
        if use_progress and progress_message:
            # Update statuses to reflect error
            for task in statuses: statuses[task] = str(statuses[task]).replace("🔄", "⏹️").replace("✅", "⏹️").replace("⏳", "⏹️").replace("▶️", "⏹️").replace("📥", "⏹️").replace("📤", "⏹️").replace("✔️", "⏹️")
            statuses["Прогресс"] = "❌ Ошибка!" # Clear final status line
            try: await update_progress(progress_message, statuses)
            except: pass
            try: # Append specific error
                current_text = getattr(progress_message, 'text', '')
                await progress_message.edit(f"{current_text}\n\n{error_text}")
                final_sent_message = progress_message
            except Exception as edit_e:
                logger.error(f"Не удалось изменить прогресс-сообщение для ошибки: {edit_e}")
                final_sent_message = await event.reply(error_text)
        else: final_sent_message = await event.reply(error_text)
    finally:
        # Store final error message if needed
        if final_sent_message and (final_sent_message != progress_message or progress_message is not None):
            await store_response_message(event.chat_id, final_sent_message)
        # Cleanup for albums (-a) is handled per-track within send_single_track's finally block


# =============================================================================
#                 ADMIN & UTILITY COMMAND HANDLERS
# =============================================================================

# -------------------------
# Command: add (Whitelist)
# -------------------------
async def handle_add(event: events.NewMessage.Event, args: List[str]):
    """Adds a user to the whitelist (users.csv). Owner only."""
    global ALLOWED_USERS
    prefix = config.get("prefix", ",")

    # --- Authorization: Owner Check ---
    try:
        me = await client.get_me()
        if not me or event.sender_id != me.id:
            logger.warning(f"Unauthorized attempt to use '{prefix}add' by user {event.sender_id}")
            try: await event.respond("🚫 Ошибка: Только владелец бота может добавлять пользователей.", delete_in=10)
            except: await event.reply("🚫 Ошибка: Только владелец бота может добавлять пользователей.")
            return
    except Exception as e_me:
        logger.error(f"Could not verify bot owner for '{prefix}add' command: {e_me}")
        await event.reply("❌ Ошибка: Не удалось проверить владельца бота. Добавление отменено.")
        return

    # --- Progress Setup ---
    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    target_user_info = "Не определен"
    final_sent_message = None

    if use_progress:
        statuses = {"Поиск Пользователя": "⏳ Ожидание...", "Сохранение": "⏸️"}
        progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

    try:
        user_id_to_add = None
        user_entity = None

        # --- Determine Target User (Reply or Argument) ---
        if event.is_reply:
            reply_message = await event.get_reply_message()
            if reply_message and reply_message.sender_id:
                user_id_to_add = reply_message.sender_id
                target_user_info = f"ответ на сообщение пользователя ID: {user_id_to_add}"
                try: user_entity = await client.get_entity(user_id_to_add)
                except Exception as e_get: logger.warning(f"Could not get entity for replied user {user_id_to_add}: {e_get}")
            else:
                raise ValueError("Не удалось получить ID пользователя из ответного сообщения.")
        elif args:
            user_arg = args[0]
            target_user_info = f"аргумент '{user_arg}'"
            if use_progress: statuses["Поиск Пользователя"] = f"🔄 Поиск '{user_arg}'..."; await update_progress(progress_message, statuses)
            try:
                user_entity = await client.get_entity(user_arg)
                if isinstance(user_entity, types.User):
                    user_id_to_add = user_entity.id
                else:
                    raise ValueError(f"`{user_arg}` не является пользователем (тип: {type(user_entity).__name__}).")
            except ValueError as e_lookup:
                 # Improve user-facing error for not found
                 if "Cannot find any entity corresponding to" in str(e_lookup):
                     raise ValueError(f"Не удалось найти пользователя по `{user_arg}`.")
                 else: # Propagate other ValueErrors (like wrong type)
                     raise ValueError(f"{e_lookup}")
            except Exception as e_lookup_other:
                 raise ValueError(f"Ошибка при поиске пользователя `{user_arg}`: {e_lookup_other}")
        else:
            # Consistent warning style
            raise ValueError(f"Необходимо указать пользователя (ID, @username, телефон) или ответить на его сообщение.")

        if user_id_to_add is None: raise ValueError("Не удалось определить ID пользователя для добавления.")

        # --- Get User Display Name ---
        user_name = f"ID: {user_id_to_add}" # Default
        if user_entity:
            first = getattr(user_entity, 'first_name', '') or ''
            last = getattr(user_entity, 'last_name', '') or ''
            username = getattr(user_entity, 'username', None)
            if username: user_name = f"@{username}"
            elif first or last: user_name = f"{first} {last}".strip()
        elif user_id_to_add in ALLOWED_USERS: user_name = ALLOWED_USERS[user_id_to_add]

        if use_progress: statuses["Поиск Пользователя"] = f"✅ Найден: {user_name} (`{user_id_to_add}`)"; await update_progress(progress_message, statuses)

        # --- Check if Already Whitelisted ---
        if user_id_to_add in ALLOWED_USERS:
            result_text = f"ℹ️ Пользователь {user_name} (`{user_id_to_add}`) уже находится в белом списке."
            logger.info(result_text)
            final_sent_message = await (progress_message.edit(result_text) if progress_message else event.reply(result_text))
            await asyncio.sleep(7) # Keep info visible
            if progress_message: await progress_message.delete(); progress_message=None
            return # Exit

        # --- Add User and Save ---
        if use_progress: statuses["Сохранение"] = "🔄 Добавление..."; await update_progress(progress_message, statuses)
        ALLOWED_USERS[user_id_to_add] = user_name
        save_users(ALLOWED_USERS)
        if use_progress: statuses["Сохранение"] = "✅ Добавлено!"; await update_progress(progress_message, statuses)

        result_text = f"✅ Пользователь {user_name} (`{user_id_to_add}`) успешно добавлен в белый список."
        logger.info(result_text)

        if progress_message:
             await progress_message.edit(result_text)
             await asyncio.sleep(7); await progress_message.delete(); progress_message = None
        else:
            await event.reply(result_text, delete_in=10)

    except Exception as e:
        logger.error(f"Ошибка при добавлении пользователя ({target_user_info}): {e}", exc_info=False)
        # Consistent error style, check if it's a ValueError (likely user input issue)
        error_prefix = "⚠️" if isinstance(e, ValueError) else "❌"
        error_text = f"{error_prefix} Ошибка при добавлении: {e}"
        if progress_message:
            statuses["Поиск Пользователя"] = str(statuses.get("Поиск Пользователя", "⏸️")).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌")
            statuses["Сохранение"] = "❌"
            try: await update_progress(progress_message, statuses)
            except: pass
            try:
                current_text = getattr(progress_message, 'text', '')
                await progress_message.edit(f"{current_text}\n\n{error_text}")
                final_sent_message = progress_message
            except: final_sent_message = await event.reply(error_text)
        else: final_sent_message = await event.reply(error_text)
    finally:
         # Store only final error messages shown in progress window
         if final_sent_message and final_sent_message == progress_message:
              await store_response_message(event.chat_id, final_sent_message)


# -------------------------
# Command: delete (Whitelist) / del
# -------------------------
async def handle_delete(event: events.NewMessage.Event, args: List[str]):
    """Removes a user from the whitelist (users.csv). Owner only."""
    global ALLOWED_USERS
    prefix = config.get("prefix", ",")

    # --- Authorization: Owner Check ---
    try:
        me = await client.get_me()
        if not me or event.sender_id != me.id:
            logger.warning(f"Unauthorized attempt to use '{prefix}delete' by user {event.sender_id}")
            try: await event.respond("🚫 Ошибка: Только владелец бота может удалять пользователей.", delete_in=10)
            except: await event.reply("🚫 Ошибка: Только владелец бота может удалять пользователей.")
            return
    except Exception as e_me:
        logger.error(f"Could not verify bot owner for '{prefix}delete' command: {e_me}")
        await event.reply("❌ Ошибка: Не удалось проверить владельца бота. Удаление отменено.")
        return

    # --- Progress Setup ---
    progress_message, statuses, use_progress = None, {}, config.get("progress_messages", True)
    target_user_info = "Не определен"
    final_sent_message = None

    if use_progress:
        statuses = {"Поиск Пользователя": "⏳ Ожидание...", "Сохранение": "⏸️"}
        progress_message = await event.reply("\n".join(f"{task}: {status}" for task, status in statuses.items()))

    try:
        user_id_to_delete = None
        name_display = None

        # --- Determine Target User (Reply or Argument) ---
        if event.is_reply:
            reply_message = await event.get_reply_message()
            if reply_message and reply_message.sender_id:
                user_id_to_delete = reply_message.sender_id
                target_user_info = f"ответ на сообщение пользователя ID: {user_id_to_delete}"
                name_display = ALLOWED_USERS.get(user_id_to_delete, f"ID: {user_id_to_delete}")
            else:
                raise ValueError("Не удалось получить ID пользователя из ответного сообщения.")
        elif args:
            user_arg = args[0]
            target_user_info = f"аргумент '{user_arg}'"
            if use_progress: statuses["Поиск Пользователя"] = f"🔄 Поиск '{user_arg}'..."; await update_progress(progress_message, statuses)

            resolved_entity = None
            search_error_message = None

            # 1. Try as numeric User ID
            if user_arg.isdigit():
                try:
                    potential_id = int(user_arg)
                    if potential_id in ALLOWED_USERS:
                        user_id_to_delete = potential_id
                        name_display = ALLOWED_USERS[potential_id]
                        logger.debug(f"Found user to delete by numeric ID in whitelist: {name_display} ({user_id_to_delete})")
                    else:
                         search_error_message = f"ID `{potential_id}` не найден в текущем белом списке."
                except ValueError: search_error_message = "Неверный формат числового ID."

            # 2. Try resolving via Telethon (username, phone)
            if user_id_to_delete is None and not user_arg.isdigit():
                 try:
                     resolved_entity = await client.get_entity(user_arg)
                     if isinstance(resolved_entity, types.User):
                         if resolved_entity.id in ALLOWED_USERS:
                             user_id_to_delete = resolved_entity.id
                             name_display = ALLOWED_USERS[user_id_to_delete]
                             logger.debug(f"Found user by Telethon ({user_arg}) in whitelist: {name_display} ({user_id_to_delete})")
                             search_error_message = None # Clear previous error
                         else:
                             first = getattr(resolved_entity, 'first_name', '') or ''
                             last = getattr(resolved_entity, 'last_name', '') or ''
                             username = getattr(resolved_entity, 'username', None)
                             resolved_name = f"@{username}" if username else f"{first} {last}".strip() or f"ID: {resolved_entity.id}"
                             search_error_message = f"Пользователь {resolved_name} (`{resolved_entity.id}`) найден, но его нет в белом списке для удаления."
                     else:
                          search_error_message = f"`{user_arg}` найден, но это не пользователь (тип: {type(resolved_entity).__name__})."
                 except ValueError:
                     search_error_message = f"Пользователь `{user_arg}` не найден по @username или номеру телефона."
                 except Exception as e_entity:
                     search_error_message = f"Ошибка при поиске `{user_arg}` через Telethon: {e_entity}"

            # 3. Try searching by name substring in whitelist
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
                     search_error_message = f"Найдено несколько пользователей с именем, содержащим '{user_arg}': {', '.join(match_details)}. Укажите точный ID или @username."
                # else: search_error_message retains previous value

            # --- Evaluate Search Results ---
            if user_id_to_delete is None:
                 if search_error_message: raise ValueError(search_error_message)
                 else: raise ValueError(f"Не удалось найти пользователя `{user_arg}` для удаления.")
        else:
            raise ValueError(f"Необходимо указать пользователя (ID, @username, имя) или ответить на его сообщение.")

        if name_display is None: name_display = f"ID: {user_id_to_delete}" # Fallback

        if use_progress: statuses["Поиск Пользователя"] = f"✅ Цель: {name_display} (`{user_id_to_delete}`)"; await update_progress(progress_message, statuses)

        # --- Check if User is Actually in Whitelist ---
        if user_id_to_delete not in ALLOWED_USERS:
            result_text = f"ℹ️ Пользователя {name_display} (`{user_id_to_delete}`) и так нет в белом списке."
            logger.info(result_text)
            final_sent_message = await (progress_message.edit(result_text) if progress_message else event.reply(result_text))
            await asyncio.sleep(7)
            if progress_message: await progress_message.delete(); progress_message=None
            return

        # --- Perform Deletion ---
        if use_progress: statuses["Сохранение"] = "🔄 Удаление..."; await update_progress(progress_message, statuses)
        del ALLOWED_USERS[user_id_to_delete]
        save_users(ALLOWED_USERS)
        if use_progress: statuses["Сохранение"] = "✅ Удалено!"; await update_progress(progress_message, statuses)

        result_text = f"✅ Пользователь {name_display} (`{user_id_to_delete}`) удален из белого списка."
        logger.info(result_text)

        if progress_message:
             await progress_message.edit(result_text)
             await asyncio.sleep(7); await progress_message.delete(); progress_message = None
        else:
            await event.reply(result_text, delete_in=10)

    except Exception as e:
        logger.error(f"Ошибка при удалении пользователя ({target_user_info}): {e}", exc_info=False)
        error_prefix = "⚠️" if isinstance(e, ValueError) else "❌"
        error_text = f"{error_prefix} Ошибка при удалении: {e}"
        if progress_message:
            statuses["Поиск Пользователя"] = str(statuses.get("Поиск Пользователя", "⏸️")).replace("🔄", "❌").replace("✅", "❌").replace("⏳", "❌")
            statuses["Сохранение"] = "❌"
            try: await update_progress(progress_message, statuses)
            except: pass
            try:
                current_text = getattr(progress_message, 'text', '')
                await progress_message.edit(f"{current_text}\n\n{error_text}")
                final_sent_message = progress_message
            except: final_sent_message = await event.reply(error_text)
        else: final_sent_message = await event.reply(error_text)
    finally:
         # Store only final error messages shown in progress window
         if final_sent_message and final_sent_message == progress_message:
              await store_response_message(event.chat_id, final_sent_message)

# -------------------------
# Command: list (Whitelist)
# -------------------------
async def handle_list(event: events.NewMessage.Event, args=None):
    """Lists all users currently in the whitelist."""
    # Optional: Add owner check if needed
    # try:
    #     me = await client.get_me();
    #     if not me or event.sender_id != me.id: return
    # except Exception: return

    if not ALLOWED_USERS:
        await store_response_message(event.chat_id, await event.reply("ℹ️ Белый список пользователей пуст."))
        return

    lines = []
    # Sort by name for better readability? Optional.
    # sorted_users = sorted(ALLOWED_USERS.items(), key=lambda item: item[1].lower())
    # for uid, name in sorted_users:
    for uid, name in ALLOWED_USERS.items():
        cleaned_name = name.strip() if name else f"User ID {uid}"
        lines.append(f"• {cleaned_name} - `{uid}`")

    text_header = f"👥 **Пользователи в белом списке ({len(lines)}):**\n\n"
    full_text = text_header + "\n".join(lines)

    MAX_MSG_LEN = 4096
    sent_messages = []

    if len(full_text) <= MAX_MSG_LEN:
        msg = await event.reply(full_text)
        sent_messages.append(msg)
    else:
        current_chunk = text_header
        logger.info(f"Whitelist is long ({len(full_text)} chars), splitting.")
        for line in lines:
            if len(current_chunk) + len(line) + 1 > MAX_MSG_LEN:
                msg = await event.respond(current_chunk)
                sent_messages.append(msg)
                await asyncio.sleep(0.5)
                current_chunk = line + "\n" # Start new chunk with the line
            else:
                current_chunk += line + "\n"
        if current_chunk.strip() and current_chunk != text_header: # Send last part
            msg = await event.respond(current_chunk)
            sent_messages.append(msg)

    for msg in sent_messages:
        await store_response_message(event.chat_id, msg)

# -------------------------
# Command: help
# -------------------------
async def handle_help(event: events.NewMessage.Event, args=None):
    """Displays the help message from help.txt."""
    help_path = os.path.join(SCRIPT_DIR, 'help.txt')
    try:
        with open(help_path, "r", encoding="utf-8") as f:
             help_text = f.read().strip()
        current_prefix = config.get("prefix", ",")
        formatted_help = help_text.replace("{prefix}", current_prefix)
        response_msg = await event.reply(formatted_help, link_preview=False)
        await store_response_message(event.chat_id, response_msg)
    except FileNotFoundError:
        logger.error(f"Файл справки не найден по пути: {help_path}")
        error_msg = await event.reply(f"❌ Ошибка: Файл справки (`{os.path.basename(help_path)}`) не найден.")
        await store_response_message(event.chat_id, error_msg)
    except Exception as e:
        logger.error(f"Ошибка чтения или форматирования файла справки: {e}", exc_info=True)
        error_msg = await event.reply("❌ Произошла ошибка при отображении справки.")
        await store_response_message(event.chat_id, error_msg)

# -------------------------
# Command: last
# -------------------------
async def handle_last(event: events.NewMessage.Event, args=None):
    """Displays the list of recently downloaded tracks (if enabled)."""
    if not config.get("recent_downloads", True):
        await store_response_message(event.chat_id, await event.reply("ℹ️ Функция отслеживания недавних скачиваний отключена в конфигурации (`recent_downloads`)."))
        return

    tracks = load_last_tracks()
    if not tracks:
        await store_response_message(event.chat_id, await event.reply("ℹ️ Список недавно скачанных треков пуст."))
        return

    lines = ["**⏳ Недавно скачанные треки:**"]
    for i, entry in enumerate(tracks): # Already limited to 5 by save_last_tracks
        if len(entry) >= 4:
            track_title, creator, browse_id, timestamp = entry[:4]
            name_part = f"**{track_title or 'N/A'}**"
            if creator and creator.lower() not in ['неизвестно', 'unknown artist', 'n/a', '']:
                 name_part += f" - {creator}"
            link_part = ""
            if browse_id and browse_id != 'N/A' and isinstance(browse_id, str):
                if browse_id.startswith("UC"): # Artist
                    link_part = f"[👤]({f'https://music.youtube.com/channel/{browse_id}'})"
                elif browse_id.startswith(("MPRE","MPLA")): # Album
                     link_part = f"[💿]({f'https://music.youtube.com/browse/{browse_id}'})"
            ts_part = f"`({timestamp})`" if timestamp else ""
            lines.append(f"{i + 1}. {name_part} {link_part} {ts_part}".strip())
        else:
            logger.warning(f"Skipping malformed entry in last tracks file: {entry}")

    if len(lines) == 1: # Only header
        await store_response_message(event.chat_id, await event.reply("ℹ️ Не найдено валидных записей о недавних треках."))
    else:
        response_msg = await event.reply("\n".join(lines), link_preview=False)
        await store_response_message(event.chat_id, response_msg)


# -------------------------
# Command: host
# -------------------------
async def handle_host(event: events.NewMessage.Event, args: List[str]):
    """Displays system information about the host running the bot."""
    response_msg = await event.reply("`🔄 Собираю информацию о системе...`")
    await store_response_message(event.chat_id, response_msg)

    try:
        # --- Gather System Information ---
        system_info = platform.system()
        os_name = system_info
        kernel = platform.release()
        architecture = platform.machine()
        hostname = platform.node()

        try: # Detailed OS name
            if system_info == 'Linux':
                 os_release = platform.freedesktop_os_release()
                 os_name = os_release.get('PRETTY_NAME', system_info)
            elif system_info == 'Windows':
                 os_name = f"{platform.system()} {platform.release()} ({platform.version()})"
            elif system_info == 'Darwin': # macOS
                 os_name = f"macOS {platform.mac_ver()[0]}" # Simpler macOS version
        except Exception as e_os: logger.warning(f"Could not get detailed OS name: {e_os}")

        ram_info, cpu_info, disk_info, uptime_str = "Недоступно", "Недоступно", "Недоступно", "Недоступно"

        try: # RAM
             mem = psutil.virtual_memory()
             ram_info = f"{mem.used / (1024 ** 3):.2f}/{mem.total / (1024 ** 3):.2f} GB ({mem.percent}%)"
        except Exception as e_ram: logger.warning(f"Could not get RAM info: {e_ram}")

        try: # CPU
            cpu_count_logical = psutil.cpu_count(logical=True)
            # cpu_count_physical = psutil.cpu_count(logical=False) # Often less relevant for load
            cpu_usage = psutil.cpu_percent(interval=0.5)
            cpu_info = f"{cpu_count_logical} Cores @ {cpu_usage:.1f}%"
        except Exception as e_cpu: logger.warning(f"Could not get CPU info: {e_cpu}")

        try: # Disk Usage ('/')
            disk = psutil.disk_usage('/')
            disk_info = f"{disk.used / (1024 ** 3):.2f}/{disk.total / (1024 ** 3):.2f} GB ({disk.percent}%)"
        except Exception as e_disk: logger.warning(f"Could not get disk usage ('/'): {e_disk}")

        try: # Uptime
            boot_time = psutil.boot_time()
            uptime_seconds = datetime.datetime.now().timestamp() - boot_time
            if uptime_seconds > 0: uptime_str = str(datetime.timedelta(seconds=int(uptime_seconds))).split('.')[0]
            else: uptime_str = "< 1 сек"
        except Exception as e_uptime: logger.warning(f"Could not get uptime: {e_uptime}")

        ping_result = "Не проводился" # Ping Test
        ping_target = "1.1.1.1" # Cloudflare DNS (alternative to Google)
        try:
            ping_cmd = shutil.which('ping')
            if ping_cmd:
                p_args = [ping_cmd, '-n', '1', '-w', '2000', ping_target] if system_info == 'Windows' else [ping_cmd, '-c', '1', '-W', '2', ping_target]
                # Use asyncio subprocess for non-blocking ping
                proc = await asyncio.create_subprocess_exec(*p_args, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
                rc = await asyncio.wait_for(proc.wait(), timeout=4.0)
                ping_result = f"✅ OK ({ping_target})" if rc == 0 else f"❌ ERR ({ping_target}, rc={rc})"
            else: ping_result = "⚠️ 'ping' n/a"
        except asyncio.TimeoutError: ping_result = f"⌛ Timeout ({ping_target})"
        except Exception as e_ping: logger.warning(f"Ping test failed: {e_ping}"); ping_result = f"❓ Error ({ping_target})"


        # --- Construct Final Message ---
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
            f" └ **Ping:** `{ping_result}`"
        )
        await response_msg.edit(text)

    except Exception as e_host:
        logger.error(f"Ошибка при сборе информации о хосте: {e_host}", exc_info=True)
        await response_msg.edit(f"❌ Не удалось получить информацию о системе:\n`{e_host}`")


# =============================================================================
#                         MAIN EXECUTION & LIFECYCLE
# =============================================================================

async def main():
    """Main asynchronous function to start the bot and handle its lifecycle."""
    logger.info("--- Запуск бота YTMG ---")
    try:
        # --- Log Library Versions ---
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

        # --- Connect and Start Client ---
        logger.info("Подключение к Telegram...")
        await client.start()
        me = await client.get_me()
        if me:
            name = f"@{me.username}" if me.username else f"{me.first_name or ''} {me.last_name or ''}".strip() or f"ID: {me.id}"
            logger.info(f"Бот успешно запущен как: {name} (ID: {me.id})")
        else:
            logger.error("Не удалось получить информацию о себе (me). Проверьте сессию Telegram или API данные.")
            return

        # --- Log Initial Configuration ---
        logger.info(f"Конфигурация: Префикс='{config.get('prefix')}', "
                    f"Whitelist={'Включен' if config.get('whitelist_enabled') else 'Выключен'}, "
                    f"AutoClear={'Включен' if config.get('auto_clear') else 'Выключен'}")
        # Log first postprocessor key/codec for quick check
        pp_info = "N/A"
        if YDL_OPTS.get('postprocessors'):
            first_pp = YDL_OPTS['postprocessors'][0]
            pp_info = first_pp.get('key','?')
            if first_pp.get('key') == 'FFmpegExtractAudio' and first_pp.get('preferredcodec'):
                pp_info += f" ({first_pp.get('preferredcodec')})"
        logger.info(f"Настройки yt-dlp: Format='{YDL_OPTS.get('format', 'N/A')}', First PP='{pp_info}', EmbedMeta={YDL_OPTS.get('embed_metadata')}, EmbedThumb={YDL_OPTS.get('embed_thumbnail')}")
        logger.info("--- Бот готов к приему команд ---")

        # --- Run Until Disconnected ---
        await client.run_until_disconnected()

    except Exception as e_main:
        logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА во время выполнения основного цикла: {e_main}", exc_info=True)
    finally:
        # --- Graceful Shutdown ---
        logger.info("--- Завершение работы бота ---")
        if client and client.is_connected():
            logger.info("Отключение от Telegram...")
            await client.disconnect()
            logger.info("Клиент Telegram отключен.")
        logger.info("--- Бот остановлен ---")

# --- Entry Point ---
if __name__ == '__main__':
    try:
        # Use asyncio.run() to manage the main asynchronous event loop
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Получен сигнал завершения (KeyboardInterrupt). Завершение работы...")
    except Exception as e_top:
        logger.critical(f"Необработанное исключение на верхнем уровне: {e_top}", exc_info=True)
    finally:
        logging.info("Процесс скрипта завершен.")

# --- END OF FILE main.py ---