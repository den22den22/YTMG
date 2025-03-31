# YTMG (YouTube Music Grabber)

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0.html)  [![Version](https://img.shields.io/badge/Version-v0.1.7--alpha-orange)](https://github.com/<USER>/<REPO>/releases/tag/v0.1.7-alpha)

**YTMG** — это Телеграм юзербот, использующий `ytmusicapi` для удобного поиска, просмотра информации и скачивания музыки и альбомов с YouTube Music прямо в ваш чат Telegram.

**Важное примечание:** Этот код в значительной степени написан с помощью ИИ. Некоторые части могут быть неоптимальными или нелогичными. Гарантии стабильной работы не предоставляется. Используйте на свой страх и риск.

---

## ⚠️ Дисклеймер

*   Эта программа предоставляется "КАК ЕСТЬ", без каких-либо гарантий.
*   Автор не несет ответственности за любой ущерб, вызванный использованием программы.
*   **Пользователь несет полную ответственность за соблюдение авторских прав на скачиваемый контент и Условий использования (TOS) сервисов YouTube/YouTube Music и Telegram.**
*   Использование бота для нарушения авторских прав или Условий обслуживания YouTube/Telegram **строго запрещено**. Функционал скачивания предоставляется для личного ознакомления и резервного копирования легально приобретенного или свободно распространяемого контента.

---

## Основные возможности

*   🎵 **Поиск:** Поиск треков, альбомов, плейлистов и исполнителей на YouTube Music.
*   ℹ️ **Просмотр информации:** Получение детальной информации о треках, альбомах, плейлистах и исполнителях, включая обложки, списки треков и популярные релизы.
*   ⬇️ **Скачивание:**
    *   Скачивание отдельных треков с корректными метаданными (название, исполнитель, альбом, год) и встроенной обложкой (требуется `ffmpeg`).
    *   Скачивание целых альбомов (треки отправляются группой).
*   👥 **Белый список:** Возможность ограничить использование бота только доверенными пользователями Telegram (опционально).
*   📜 **История:** Просмотр списка последних скачанных треков.
*   ⚙️ **Системная информация:** Отображение информации о системе, на которой запущен бот.
*   🗑️ **Авто-очистка:** Автоматическое удаление предыдущих ответов бота для поддержания чистоты в чате (настраиваемо).
*   🔧 **Настройка:**
    *   Настраиваемый префикс команд.
    *   Настраиваемая подпись (кредит) к отправляемым файлам с поддержкой Markdown-ссылок.
    *   Гибкая настройка параметров скачивания через конфигурационный файл `yt-dlp`.

---

## Требования

*   **Python:** 3.8 или выше (рекомендуется 3.10+).
*   **Git:** Для клонирования репозитория.
*   **pip:** Для установки зависимостей Python.
*   **FFmpeg:** **Обязателен** для скачивания аудио, встраивания метаданных и обложек. Он должен быть установлен в вашей системе и доступен в переменной `PATH`, либо путь к нему должен быть указан в `dlp.conf`.

---

## Установка FFmpeg

`FFmpeg` — это критически важная зависимость. Установите его с помощью менеджера пакетов вашей системы:

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
*   **macOS (используя Homebrew):**
    ```bash
    brew install ffmpeg
    ```
*   **Windows:**
    1.  Скачайте сборку с официального сайта [ffmpeg.org](https://ffmpeg.org/download.html) (например, от gyan.dev или BtbN).
    2.  Распакуйте архив.
    3.  Добавьте путь к папке `bin` внутри распакованного архива в системную переменную `PATH` или укажите полный путь к `ffmpeg.exe` в параметре `ffmpeg_location` файла `dlp.conf`.

---

## Настройка

1.  **Клонирование репозитория:**
    ```bash
    git clone https://github.com/den22den22/ytmg.git
    cd ytmg
    ```

2.  **Установка зависимостей Python:**
    ```bash
    pip install -r requirements.txt
    ```

3.  **Настройка Telegram API:**
    *   Получите ваши `API_ID` и `API_HASH` на [my.telegram.org](https://my.telegram.org/apps).
    *   **ВАЖНО:** Не указывайте их напрямую в коде! Бот ожидает их как **переменные окружения**. Установите переменные `TELEGRAM_API_ID` и `TELEGRAM_API_HASH` в вашей системе перед запуском бота.
        *   *Пример для Linux/macOS (временная установка):*
            ```bash
            export TELEGRAM_API_ID=1234567
            export TELEGRAM_API_HASH='abcdef1234567890abcdef1234567890'
            python main.py
            ```
        *   *Рекомендуется:* Использовать `.env` файл и библиотеку типа `python-dotenv` (если хотите модифицировать код) или настроить переменные окружения на уровне системы/сервиса.

4.  **Аутентификация YTMusic (Опционально):**
    *   Для доступа к приватным плейлистам, лайкам и другому контенту, требующему входа в аккаунт YouTube Music, необходимо создать файл `headers_auth.json`.
    *   Следуйте **официальной инструкции `ytmusicapi`**: [Настройка с помощью браузера](https://ytmusicapi.readthedocs.io/en/latest/setup/browser.html).
    *   Поместите сгенерированный файл `headers_auth.json` в ту же директорию, где находится `main.py`.
    *   Если этот файл отсутствует, бот будет работать в неаутентифицированном режиме (функционал будет ограничен публичным контентом).

5.  **Конфигурация бота (`UBOT.cfg`):**
    *   Скопируйте файл `UBOT.cfg.example` в `UBOT.cfg`:
        ```bash
        cp UBOT.cfg.example UBOT.cfg
        ```
    *   Отредактируйте `UBOT.cfg` по вашему усмотрению:
        *   `prefix`: Префикс для команд (например, `,`).
        *   `whitelist_enabled`: `true` для включения белого списка, `false` для разрешения использования бота всем.
        *   `bot_credit`: Текст подписи к файлам. Поддерживает Markdown для ссылок (например, `"via [YTMG](https://github.com/den22den22/ytmg)"`). Не забудьте также установить `parse_mode='md'` в коде отправки, если используете ссылки.
        *   `auto_clear`: `true` для автоматической очистки старых сообщений бота.
        *   Другие параметры см. в файле и комментариях в `main.py` (секция `DEFAULT_CONFIG`).

6.  **Конфигурация yt-dlp (`dlp.conf`):**
    *   Скопируйте файл `dlp.conf.example` в `dlp.conf`:
        ```bash
        cp dlp.conf.example dlp.conf
        ```
    *   Отредактируйте `dlp.conf` при необходимости. Основные параметры:
        *   `format`: Предпочитаемый формат аудио/видео (см. документацию `yt-dlp`). По умолчанию `bestaudio/best`.
        *   `postprocessors`: Настройки пост-обработки (конвертация, встраивание метаданных/обложки).
            *   **ВНИМАНИЕ:** Не удаляйте секции с ключами `FFmpegExtractAudio`, `EmbedMetadata`, `EmbedThumbnail`, если хотите, чтобы бот конвертировал аудио и встраивал метаданные/обложки.
            *   `preferredcodec`, `preferredquality`: Настройка кодека и качества для `FFmpegExtractAudio`.
        *   `outtmpl`: Шаблон пути для сохранения файлов. По умолчанию сохраняются в директорию бота.
        *   `ffmpeg_location`: Раскомментируйте и укажите полный путь к `ffmpeg`, если он не находится в `PATH`.

7.  **Белый список (`users.csv`):**
    *   Если `whitelist_enabled` установлено в `true`, создайте файл `users.csv` в директории бота.
    *   Добавьте пользователей в формате `Имя;UserID` (каждая строка - новый пользователь). `UserID` можно узнать у ботов типа `@userinfobot`.
    *   Имя используется для удобства в команде `,list`.

---

## Запуск бота

1.  Убедитесь, что вы находитесь в директории с `main.py`.
2.  Убедитесь, что установлены переменные окружения `TELEGRAM_API_ID` и `TELEGRAM_API_HASH`.
3.  Запустите скрипт:
    ```bash
    python main.py
    ```
4.  При первом запуске Telethon попросит вас войти в ваш аккаунт Telegram (ввести номер телефона и код подтверждения). Будет создан файл сессии (`telegram_session`), чтобы не входить каждый раз. **Никогда не делитесь этим файлом!**
5.  Для работы в фоновом режиме рекомендуется использовать `screen` или `tmux`:
    ```bash
    # Пример с screen
    screen -S ytmgbot # Создать сессию
    # Установить переменные окружения (если нужно)
    export TELEGRAM_API_ID=...
    export TELEGRAM_API_HASH=...
    python main.py
    # Отключиться от сессии: Ctrl+A, затем D
    # Подключиться обратно: screen -r ytmgbot
    ```

---

## Использование

Используйте команды в любом чате Telegram (включая "Избранное"), где активен ваш пользовательский аккаунт. Основные команды:

*   `,search -t <запрос>`: Поиск треков.
*   `,search -a <запрос>`: Поиск альбомов.
*   `,search -p <запрос>`: Поиск плейлистов.
*   `,search -e <запрос>`: Поиск исполнителей.
*   `,see [-i] <ссылка или ID>`: Показать информацию о треке/альбоме/плейлисте/исполнителе (`-i` для показа обложки).
*   `,dl -t <ссылка>`: Скачать трек.
*   `,dl -a <ссылка>`: Скачать альбом.
*   `,last`: Показать недавно скачанные треки.
*   `,host`: Показать информацию о системе.
*   `,help`: Показать это сообщение справки.
*   `,list` / `,add` / `,del`: Управление белым списком (только владелец).

Полный список команд и их описание доступны по команде `,help` (префикс может быть изменен в `UBOT.cfg`).

---

## Лицензия

Этот проект лицензирован под **GNU General Public License v3.0**. Полный текст лицензии см. в файле [LICENSE](LICENSE).

---

## Благодарности

*   Разработчикам [ytmusicapi](https://github.com/sigma67/ytmusicapi)
*   Разработчикам [yt-dlp](https://github.com/yt-dlp/yt-dlp)
*   Разработчикам [Telethon](https://github.com/LonamiWebs/Telethon)
