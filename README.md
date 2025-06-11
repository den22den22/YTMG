# YTMG (YouTube Music Grabber)

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0.html) [![Version](https://img.shields.io/badge/Version-v0.5.1-green)](https://github.com/den22den22/YTMG/releases/tag/v0.5.1)

**YTMG** is a Telegram userbot that utilizes `ytmusicapi` and `yt-dlp` to conveniently search, view information about, and download music and albums from YouTube Music directly into your Telegram chat.

**Important Note:** This code was significantly developed with the help of AI. Some parts might be suboptimal or illogical. Stable operation is not guaranteed. Use at your own risk.

---

## ⚠️ Disclaimer

*   This program is provided "AS IS", without any warranty.
*   The author is not responsible for any damage caused by using the program.
*   **The user bears full responsibility for respecting the copyrights of the downloaded content and the Terms of Service (TOS) of YouTube/YouTube Music and Telegram.**
*   Using the bot to infringe copyrights or violate the YouTube/Telegram Terms of Service is **strictly prohibited**. The download functionality is provided for personal evaluation and backup of legally acquired or freely distributable content.

---

## Key Features

*   🎵 **Search (`search`):** Find tracks, albums, playlists, artists, and videos on YouTube Music.
*   ℹ️ **View Info (`see`):** Get detailed information about a specific entity (track, album, playlist, artist) by its ID or URL.
    *   For tracks: title, artist, album, duration, ID, link.
    *   For albums/playlists: title, author, track count, ID, link, list of first few tracks.
    *   For artists: name, subscriber count, ID, link, **featured track OR track from latest release**, popular tracks, albums/singles.
    *   Option to include cover art (`-i`) and lyrics (`-txt`, for tracks and artist's special track).
*   ⬇️ **Download (`dl`, `download`):** Download and send audio files.
    *   Download individual tracks (`-t`) with automatic metadata tagging (title, artist, album, year) and **cover art embedding** (requires `ffmpeg`).
    *   Download entire albums or playlists (`-a`) (tracks are sent sequentially).
    *   **New:** Download the first found track by search query (`-s <query>`).
    *   Option to include lyrics when downloading an individual track (`-txt`).
*   📜 **History (`last`, `alast`, `history`):**
    *   View a list of recently downloaded tracks (`last` command - *configurable*).
    *   View your YouTube Music listening history (`alast` or `history` command - **requires authentication**).
*   👍 **Liked Songs & Recommendations (`likes`, `rec`):**
    *   Fetch tracks from your "Liked Songs" playlist (`likes` command - **requires authentication**).
    *   Get personalized music recommendations (`rec` command - **requires authentication**).
*   📝 **Lyrics (`text`, `lyrics`):** Get and display song lyrics for a track by its ID or link.
*   ⚙️ **System Info (`host`):** Display detailed information about the system running the bot:
    *   OS, CPU, RAM, disk usage for home directory, uptime, ping.
    *   **New:** Versions of key Python libraries (Telethon, yt-dlp, ytmusicapi, Pillow, psutil, requests, `python-dotenv`, GitPython).
    *   **New:** Detected FFmpeg version and path to the executable being used.
    *   **New:** Detailed Git repository status for YTMG: current branch, last commit hash/date/message, remote URL, local changes (dirty status), and indicates how far ahead/behind the local repository is compared to the remote (an indicator for available updates).
*   🗑️ **Auto-Clear (`auto_clear`):** Automatically delete previous bot responses in a chat when a new command from the auto-clear list is issued (configurable in `UBOT.cfg`).
*   🔧 **Configuration:**
    *   Customizable command prefix (`prefix`).
    *   Customizable caption (credit) for sent files (`bot_credit`) - supports Markdown links.
    *   Flexible download parameter tuning via the `yt-dlp` configuration file (`dlp.conf`).

---

## Requirements

*   **Python:** 3.8 or higher (3.10+ recommended).
*   **Git:** To clone the repository.
*   **pip:** To install Python dependencies.
*   **FFmpeg:** **Required** for downloading audio, embedding metadata, and cover art. It must be installed on your system and available in the `PATH` environment variable, or the full path to the executable must be specified in `dlp.conf`.

---

## Installing FFmpeg

`FFmpeg` is a critical dependency for most download features. Install it using your system's package manager:

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
    *   **IMPORTANT:** Do not hardcode them into the script! The bot expects them as **environment variables**. Create a `.env` file in the bot's root directory (next to `main.py`) and add the following lines to it:
        ```env
        TELEGRAM_API_ID=YOUR_API_ID
        TELEGRAM_API_HASH=YOUR_API_HASH
        ```
        The `python-dotenv` library (installed via `requirements.txt`) will automatically load these variables.

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
    *   Edit `UBOT.cfg` according to your preferences. See the comments in the `UBOT.cfg.example` file for details on each parameter.
    *   *Note:* The `whitelist_enabled` parameter and its associated commands have been removed.

6.  **yt-dlp Configuration (`dlp.conf`):**
    *   Copy the example configuration file:
        ```bash
        cp dlp.conf.example dlp.conf
        ```
    *   Edit `dlp.conf` if necessary. This file allows fine-tuning of how `yt-dlp` downloads and processes files. See the comments in the `dlp.conf.example` file and the official `yt-dlp` documentation for details.

7.  **.gitignore:**
    *   A `.gitignore` file is included in the repository to prevent accidental committing of sensitive files (like `telegram_session.session`, `headers_auth.json`), logs, and temporary download files. Ensure this file is present and contains appropriate entries.

---

## Running the Bot

1.  Ensure you are in the directory containing `main.py`.
2.  Make sure the `.env` file with your Telegram API keys is present.
3.  Run the script:
    ```bash
    python main.py
    ```
4.  On the first run, Telethon will prompt you to log in to your Telegram account (enter phone number and confirmation code). A session file (`telegram_session.session`) will be created to avoid logging in every time. **Never share this file!**
5.  For running in the background, using `screen` or `tmux` is highly recommended:
    ```bash
    # Example using screen
    screen -S ytmgbot # Create a screen session
    python main.py
    # Detach from session: Ctrl+A, then D
    # Re-attach: screen -r ytmgbot
    ```

---

## Usage

Use the commands in any Telegram chat (including Saved Messages) where your user account is active. The default command prefix is `,` (configurable).

*   `,help`: Show the help message with command descriptions.
*   `,search [-t|-a|-p|-e|-v] <query>`: Search YouTube Music. (`-t` tracks, `-a` albums, `-p` playlists, `-e` artists, `-v` videos). Default is tracks.
*   `,see [-t|-a|-p|-e] [-i] [-txt] <ID or link>`: Show info about an entity. (`-t,-a,-p,-e` optional type hint, `-i` include cover, `-txt` include lyrics - for track/artist special track).
*   `,dl` / `,download` `<flag> <argument> [-txt]`: Download audio. (`-t <link>` track, `-a <link>` album/playlist, `-s <query>` find and download track by query, `-txt` include lyrics for `-t` and `-s`).
*   `,last`: Show recently downloaded tracks (if enabled).
*   `,alast` / `,history`: Show YTMusic history (**auth required**).
*   `,likes`: Show YTMusic liked songs (**auth required**).
*   `,rec`: Get YTMusic recommendations (**auth required**).
*   `,text` / `,lyrics` `<ID or link>`: Get lyrics for a track.
*   `,host`: Show system information and Git repository status.
*   `,clear`: Manually clear previous bot responses.

The full list of commands and their brief descriptions are available via the `,help` command.

---

## License

This project is licensed under the **GNU General Public License v3.0**. See the [LICENSE](LICENSE) file for the full text.

---

## Acknowledgements

*   Developers of [ytmusicapi](https://github.com/sigma67/ytmusicapi)
*   Developers of [yt-dlp](https://github.com/yt-dlp/yt-dlp)
*   Developers of [Telethon](https://github.com/LonamiWebs/Telethon)
*   Developers of [Pillow](https://python-pillow.org/), [psutil](https://github.com/giampaolo/psutil), [requests](https://requests.readthedocs.io/), [GitPython](https://github.com/gitpython-developers/GitPython), [python-dotenv](https://github.com/theskumar/python-dotenv)
