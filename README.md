## YTMG v0.5.0 Release Notes

This major release significantly expands YTMG's capabilities by introducing new commands for direct search-and-download and comprehensive system monitoring. It also includes numerous fixes and improvements to existing features, enhancing stability, usability, and data management.

### ✨ New Features

*   **Direct Search and Download (`dl -s`):**
    *   You can now search for a track by query and directly download the first found result using `{prefix}dl -s <query>`.
    *   This command supports the `-txt` flag for optional lyrics inclusion.
*   **Comprehensive Host Information (`host`):**
    *   The `{prefix}host` command has been completely revamped to provide a detailed overview of the environment where the bot is running:
        *   **System Info:** OS, kernel, architecture, system uptime.
        *   **Resource Usage:** CPU load, RAM usage, and disk space for the user's home directory.
        *   **Network Status:** A ping test to `8.8.8.8`.
        *   **Software Versions:** Displays versions of key Python libraries (Telethon, yt-dlp, ytmusicapi, Pillow, psutil, requests, `python-dotenv`, GitPython) and the FFmpeg executable being used.
        *   **Git Repository Status:** Shows the current Git branch, last commit hash/date/message, remote URL, whether there are local changes (dirty status), and crucially, indicates if your local repository is ahead of or behind the remote (an indicator for available updates).
*   **Enhanced Artist Information (`see -e`):**
    *   When using `{prefix}see -e <artist_ID_or_link>`, the bot will now attempt to find and display information about the artist's **featured (pinned) track**.
    *   If no featured track is found, it automatically falls back to showing the **first track from the artist's latest album or single release**, providing a relevant example of their work.
    *   The `-txt` flag can be used to fetch and display lyrics for this special track.
*   **Robust YTMusic API Initialization:**
    *   Improved internal handling of `ytmusicapi` client initialization, including automatic re-initialization attempts or fallback to unauthenticated mode upon authentication issues, enhancing reliability for YTMusic-dependent commands.
    *   The `@retry` decorator now includes basic HTTP 401/403 error handling for YTMusic API calls to trigger client re-initialization if authentication might be lost.
    *   Expanded the types of exceptions caught by the `retry` decorator to include more specific Telethon network errors (`rpcerrorlist.TimeoutError`, `ApiIdInvalidError`).
*   **`.env` File for Environment Variables:**
    *   The bot now uses the `python-dotenv` library to load `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` from a `.env` file, simplifying credential management.
*   **Lyrics Fallback to Description Field:**
    *   The `get_lyrics_for_track` logic now intelligently checks the 'description' field for lyrics if the primary 'lyrics' field is empty, potentially finding more available song texts.

### 🚀 Improvements & Fixes

*   **Fixed: Progress Message Display:** Resolved `NameError` issues in progress message formatting across various commands (`likes`, `host`, `see`), ensuring consistent status updates without crashes.
*   **Fixed: Auto-Clear Consistency:** Enhanced message storage and deletion logic to ensure all bot responses are correctly tracked and cleared with `auto_clear` enabled or by using `{prefix}clear`. This also fixes a `TypeError` in `handle_clear` and improves overall cleanup reliability.
*   **Fixed: `yt-dlp` Post-Processor File Path Detection:** Significantly improved the `download_track` function to accurately identify the final output file path after `yt-dlp`'s post-processing (e.g., audio format conversions to M4A), preventing "file not found" errors after a successful download.
*   **Fixed: `last.csv` Format Update:** The `last.csv` file now stores more detailed information (6 columns instead of 4), including the full track URL and its duration, providing a richer download history. Backward compatibility for loading older `last.csv` formats is maintained.
*   **Improved: `retry` Decorator:** Added more specific `telethon` network errors and a basic HTTP 401/403 re-authentication attempt for `ytmusicapi` to the retry mechanism, enhancing resilience to transient API issues.
*   **Improved: Thumbnail Handling:** Enhanced uniqueness of temporary thumbnail filenames and improved robustness of image cropping (`crop_thumbnail`) and general file cleanup (`cleanup_files`).
*   **Improved: `yt-dlp` Search:** Added `ignore_spelling=True` option to `_api_search` for potentially better search results.
*   **Improved: Logging:** Added more detailed and context-aware logging throughout the codebase for better debugging and monitoring.
*   **Improved: Album/Playlist Download:** Enhanced `download_album_tracks` to handle more `ytmusicapi` data structures and utilize `yt-dlp`'s internal analysis for track lists if API metadata retrieval is problematic.
*   **Improved: HTML Output for `send_lyrics`:** The HTML file generated for long lyrics now features a more polished design and better extraction of title/artist information.
*   **Improved: Single Track Sending (`send_single_track`):** Added specific error handling for `telethon_errors.WebpageMediaEmptyError` and `telethon_errors.MediaCaptionTooLongError` during file uploads, leading to more graceful error recovery. Additionally, `.opus` audio files are now preserved by default (not deleted during cleanup).

### 🗑️ Removed Features

*   **Whitelist Functionality:** The `{prefix}add`, `{prefix}del`, and `{prefix}list` commands, along with the `whitelist_enabled` configuration parameter, have been removed. The bot now operates solely based on `BOT_OWNER_ID` for command authorization.

### 📄 Documentation

*   **Updated `help.txt`:** The inline help message has been fully updated to reflect all new commands, flags, and removed features.
*   **Comprehensive README:** The `README.md` will be fully updated to cover all v0.5.0 changes, including detailed feature descriptions, streamlined setup steps, and accurate command usage.

### How to Update

1.  Stop the bot if it's running.
2.  Navigate to the bot's directory in your terminal.
3.  Pull the latest changes:
    ```bash
    git pull origin main
    ```
4.  Update dependencies (new `GitPython` and `python-dotenv` are required):
    ```bash
    pip install -r requirements.txt
    ```
5.  **Important:**
    *   Create a `.env` file in your bot's root directory if you haven't already, and move your `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` into it:
        ```env
        TELEGRAM_API_ID=YOUR_API_ID
        TELEGRAM_API_HASH=YOUR_API_HASH
        ```
    *   Review your `UBOT.cfg` file. The `whitelist_enabled` parameter should be removed. No other mandatory changes, but review `dlp.conf` for updated options.
    *   The `users.csv` file is no longer used and can be safely deleted.
    *   This release includes a new `help.txt` file; ensure you have the latest version.
6.  Restart the bot.

---

Thank you for using YTMG! Please report any issues or suggest features on the [GitHub Issues page](https://github.com/den22den22/YTMG/issues).