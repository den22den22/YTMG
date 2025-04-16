# YTMG (YouTube Music Grabber)

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0.html) [![Version](https://img.shields.io/badge/Version-v0.3.0-green)](https://github.com/den22den22/YTMG/releases/tag/v0.3.0)

**YTMG** is a Telegram userbot that utilizes `ytmusicapi` to conveniently search, view information, and download music and albums from YouTube Music directly into your Telegram chat.

**Important Note:** This code was largely written with the help of AI. Some parts might be suboptimal or illogical. Stable operation is not guaranteed. Use at your own risk.

---

## ⚠️ Disclaimer

*   This program is provided "AS IS", without any warranty.
*   The author is not responsible for any damage caused by using the program.
*   **The user bears full responsibility for respecting the copyrights of the downloaded content and the Terms of Service (TOS) of YouTube/YouTube Music and Telegram.**
*   Using the bot to infringe copyrights or violate the YouTube/Telegram Terms of Service is **strictly prohibited**. The download functionality is provided for personal evaluation and backup of legally acquired or freely distributable content.

---

## Key Features

*   🎵 **Search:** Find tracks, albums, playlists, and artists on YouTube Music.
*   ℹ️ **View Info:** Get detailed information about tracks, albums, playlists, and artists, including cover art, track lists, and popular releases.
*   ⬇️ **Download:**
    *   Download individual tracks with correct metadata (title, artist, album, year) and embedded cover art (requires `ffmpeg`).
    *   Download entire albums (tracks are sent sequentially).
    *   Get track lyrics (sent as a message or HTML file if too long).
*   👥 **Whitelist:** Option to restrict bot usage to trusted Telegram users only.
*   📜 **History:** View a list of recently downloaded tracks (`last` command) and your YouTube Music listening history (`alast` command, requires authentication).
*   👍 **Likes & Recommendations:** Fetch your liked songs (`likes` command) and personalized recommendations (`rec` command) (requires authentication).
*   ⚙️ **System Info:** Display information about the system running the bot (`host` command).
*   🗑️ **Auto-Clear:** Automatically delete previous bot responses to keep the chat clean (configurable).
*   🔧 **Configuration:**
    *   Customizable command prefix.
    *   Customizable caption (credit) for sent files with Markdown link support.
    *   Flexible download parameter tuning via the `yt-dlp` configuration file (`dlp.conf`).

---

## Requirements

*   **Python:** 3.8 or higher (3.10+ recommended).
*   **Git:** To clone the repository.
*   **pip:** To install Python dependencies.
*   **FFmpeg:** **Required** for downloading audio, embedding metadata, and cover art. It must be installed on your system and available in the `PATH` environment variable, or the path to it must be specified in `dlp.conf`.

---

## Installing FFmpeg

`FFmpeg` is a critical dependency. Install it using your system's package manager:

*   **Debian/Ubuntu:**
    ```bash
    sudo apt update && sudo apt install ffmpeg
    ```
*   **Arch Linux/Manjaro:**
    ```bash
    sudo pacman -Syu ffmpeg
    ```
*   **Fedora:**
    ```bash
    sudo dnf install ffmpeg
    ```
*   **macOS (using Homebrew):**
    ```bash
    brew install ffmpeg
    ```
*   **Windows:**
    1.  Download a build from the official [ffmpeg.org](https://ffmpeg.org/download.html) site (e.g., from gyan.dev or BtbN).
    2.  Extract the archive.
    3.  Add the path to the `bin` folder inside the extracted archive to your system's `PATH` variable, or specify the full path to `ffmpeg.exe` in the `ffmpeg_location` parameter of `dlp.conf`.

---

## Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/den22den22/YTMG.git
    cd YTMG
    ```

2.  **Install Python dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Telegram API:**
    *   Obtain your `API_ID` and `API_HASH` from [my.telegram.org/apps](https://my.telegram.org/apps).
    *   **IMPORTANT:** Do not hardcode them into the script! The bot expects them as **environment variables**. Set the `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` variables in your system before running the bot.
        *   *Example for Linux/macOS (temporary session):*
            ```bash
            export TELEGRAM_API_ID=1234567
            export TELEGRAM_API_HASH='abcdef1234567890abcdef1234567890'
            python main.py
            ```
        *   *Recommended:* Use a `.env` file and a library like `python-dotenv` (if you modify the code) or configure environment variables at the system/service level.

4.  **YTMusic Authentication (Optional but Recommended):**
    *   To access private playlists, liked songs, recommendations, history, and other content requiring a YouTube Music account login, you need to create a `headers_auth.json` file.
    *   Follow the **official `ytmusicapi` instructions**: [Setup using a browser](https://ytmusicapi.readthedocs.io/en/latest/setup/browser.html).
    *   Place the generated `headers_auth.json` file in the same directory as `main.py`.
    *   If this file is missing, the bot will operate in unauthenticated mode (functionality will be limited to public content).

5.  **Bot Configuration (`UBOT.cfg`):**
    *   Copy the example configuration file:
        ```bash
        cp UBOT.cfg.example UBOT.cfg
        ```
    *   Edit `UBOT.cfg` according to your preferences:
        *   `prefix`: Command prefix (e.g., `,`).
        *   `whitelist_enabled`: `true` to enable the whitelist, `false` to allow everyone to use the bot.
        *   `bot_credit`: Caption text for sent files. Supports Markdown for links (e.g., `"via [YTMG](https://github.com/den22den22/YTMG/)"`). Ensure you set `parse_mode='md'` in the sending code if using links (currently handled by the bot).
        *   `auto_clear`: `true` to automatically clear old bot messages.
        *   Other parameters: See the file and comments in `main.py` (`DEFAULT_CONFIG` section).

6.  **yt-dlp Configuration (`dlp.conf`):**
    *   Copy the example configuration file:
        ```bash
        cp dlp.conf.example dlp.conf
        ```
    *   Edit `dlp.conf` if necessary. Key parameters:
        *   `format`: Preferred audio/video format (see `yt-dlp` documentation). Default is `bestaudio/best`.
        *   `postprocessors`: Post-processing settings (conversion, embedding metadata/thumbnails).
            *   **ATTENTION:** Do not remove sections with keys `FFmpegExtractAudio`, `EmbedMetadata`, `EmbedThumbnail` if you want the bot to convert audio and embed metadata/thumbnails.
            *   `preferredcodec`, `preferredquality`: Codec and quality settings for `FFmpegExtractAudio`.
        *   `outtmpl`: Template for the output file path. Defaults to saving in the bot's directory.
        *   `ffmpeg_location`: Uncomment and provide the full path to `ffmpeg` if it's not in your `PATH`.

7.  **Whitelist (`users.csv`):**
    *   If `whitelist_enabled` is set to `true`, create a `users.csv` file in the bot's directory.
    *   Add users in the format `Name;UserID` (one user per line). You can find a user's ID using bots like `@userinfobot`.
    *   The name is used for display purposes in the `,list` command.

8.  **.gitignore:**
    *   A `.gitignore` file is included in the repository to prevent accidental committing of sensitive files (like `telegram_session.session`, `headers_auth.json`), logs, and temporary download files.

---

## Running the Bot

1.  Ensure you are in the directory containing `main.py`.
2.  Make sure the `TELEGRAM_API_ID` and `TELEGRAM_API_HASH` environment variables are set.
3.  Run the script:
    ```bash
    python main.py
    ```
4.  On the first run, Telethon will prompt you to log in to your Telegram account (enter phone number and confirmation code). A session file (`telegram_session.session`) will be created to avoid logging in every time. **Never share this file!**
5.  For running in the background, using `screen` or `tmux` is recommended:
    ```bash
    # Example using screen
    screen -S ytmgbot # Create a screen session
    # Set environment variables if needed
    export TELEGRAM_API_ID=...
    export TELEGRAM_API_HASH=...
    python main.py
    # Detach from session: Ctrl+A, then D
    # Re-attach: screen -r ytmgbot
    ```

---

## Usage

Use the commands in any Telegram chat (including Saved Messages) where your user account is active. Core commands:

*   `,search -t <query>`: Search for tracks.
*   `,search -a <query>`: Search for albums.
*   `,search -p <query>`: Search for playlists.
*   `,search -e <query>`: Search for artists.
*   `,see [-i] [-txt] <link or ID>`: Show info about a track/album/playlist/artist (`-i` for cover, `-txt` for lyrics).
*   `,dl -t [-txt] <link>`: Download a track (`-txt` to also send lyrics).
*   `,dl -a <link>`: Download an album/playlist.
*   `,last`: Show recently downloaded tracks.
*   `,alast`: Show your YTMusic listening history (auth required).
*   `,likes`: Show your liked songs (auth required).
*   `,rec`: Get music recommendations (auth required).
*   `,text <link or ID>`: Get lyrics for a track.
*   `,host`: Show system information.
*   `,help`: Show the help message.
*   `,list` / `,add <user>` / `,del <user>`: Manage the whitelist (owner only).
*   `,clear`: Manually clear previous bot responses.

The full list of commands and their descriptions are available via the `,help` command (the prefix can be changed in `UBOT.cfg`).

---

## License

This project is licensed under the **GNU General Public License v3.0**. See the [LICENSE](LICENSE) file for the full text.

---

## Acknowledgements

*   Developers of [ytmusicapi](https://github.com/sigma67/ytmusicapi)
*   Developers of [yt-dlp](https://github.com/yt-dlp/yt-dlp)
*   Developers of [Telethon](https://github.com/LonamiWebs/Telethon)
*   Developers of [Pillow](https://python-pillow.org/), [psutil](https://github.com/giampaolo/psutil), [requests](https://requests.readthedocs.io/)
