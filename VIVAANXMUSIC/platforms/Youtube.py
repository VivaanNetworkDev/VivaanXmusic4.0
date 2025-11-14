import asyncio
import glob
import json
import os
import random
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Union
import string
import requests
import yt_dlp
from pyrogram.enums import MessageEntityType
from pyrogram.types import Message
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from youtubesearchpython.__future__ import VideosSearch, CustomSearch
import base64
from VIVAANXMUSIC import LOGGER
from VIVAANXMUSIC.utils.database import is_on_off
from VIVAANXMUSIC.utils.formatters import time_to_seconds

logger = LOGGER(__name__)

# ============================================================================
# COOKIES CONFIGURATION - Directly integrated from Pastebin
# ============================================================================
COOKIES_URL = "https://pastebin.com/raw/RR0ucLw3"  # Your cookies URL
COOKIES_CACHE_PATH = os.path.join(os.getcwd(), "cookies", "youtube_cookies.txt")

async def download_and_cache_cookies():
    """
    Download cookies from Pastebin and cache them locally for fast access.
    Runs automatically on startup.
    """
    try:
        os.makedirs(os.path.dirname(COOKIES_CACHE_PATH), exist_ok=True)
        
        # Check if cookies already cached and fresh (within 24 hours)
        if os.path.exists(COOKIES_CACHE_PATH):
            file_age = os.time.time() - os.path.getmtime(COOKIES_CACHE_PATH)
            if file_age < 86400:  # 24 hours
                logger.info(f"✅ [Cookies] Using cached cookies from: {COOKIES_CACHE_PATH}")
                return COOKIES_CACHE_PATH
        
        logger.info(f"[Cookies] Downloading fresh cookies from: {COOKIES_URL}")
        
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retries)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        
        response = session.get(COOKIES_URL, timeout=30)
        response.raise_for_status()
        
        with open(COOKIES_CACHE_PATH, 'w') as f:
            f.write(response.text)
        
        logger.info(f"✅ [Cookies] Downloaded and cached successfully to: {COOKIES_CACHE_PATH}")
        return COOKIES_CACHE_PATH
        
    except Exception as e:
        logger.error(f"❌ [Cookies] Failed to download: {str(e)}")
        # Fallback: return path anyway, maybe old cache exists
        return COOKIES_CACHE_PATH

def get_cookies_file():
    """
    Get cookies file path - uses cached cookies automatically.
    Fast response: no need to download every time.
    """
    try:
        # First, check local cookies folder for any .txt files
        folder_path = os.path.join(os.getcwd(), "cookies")
        if os.path.exists(folder_path):
            txt_files = glob.glob(os.path.join(folder_path, '*.txt'))
            if txt_files:
                # Prefer the auto-cached cookies
                if COOKIES_CACHE_PATH in txt_files:
                    logger.debug(f"[Cookies] Using cached cookies: {COOKIES_CACHE_PATH}")
                    return COOKIES_CACHE_PATH
                # Otherwise use any available cookies
                selected_file = random.choice(txt_files)
                logger.debug(f"[Cookies] Using cookies file: {selected_file}")
                return selected_file
        
        # If no local cookies, return cache path (will be used by yt-dlp)
        logger.debug(f"[Cookies] Using default cache path: {COOKIES_CACHE_PATH}")
        return COOKIES_CACHE_PATH
        
    except Exception as e:
        logger.debug(f"[Cookies] Error getting cookies file: {e}")
        return COOKIES_CACHE_PATH

# ============================================================================
# Initialize cookies on startup
# ============================================================================
async def initialize_cookies():
    """Initialize cookies on bot startup"""
    logger.info("[Cookies] Initializing cookies download...")
    cookies_path = await download_and_cache_cookies()
    logger.info(f"[Cookies] Initialization complete. Path: {cookies_path}")

async def check_file_size(link):
    async def get_format_info(link):
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp",
            "--cookies", get_cookies_file(),
            "-J",
            link,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:
            print(f'Error:\n{stderr.decode()}')
            return None
        return json.loads(stdout.decode())

    def parse_size(formats):
        total_size = 0
        for format in formats:
            if 'filesize' in format:
                total_size += format['filesize']
        return total_size

    info = await get_format_info(link)
    if info is None:
        return None
    
    formats = info.get('formats', [])
    if not formats:
        print("No formats found.")
        return None
    
    total_size = parse_size(formats)
    return total_size

async def shell_cmd(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, errorz = await proc.communicate()
    if errorz:
        if "unavailable videos are hidden" in (errorz.decode("utf-8")).lower():
            return out.decode("utf-8")
        else:
            return errorz.decode("utf-8")
    return out.decode("utf-8")


class YouTubeAPI:
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="
        self.regex = r"(?:youtube\.com|youtu\.be)"
        self.status = "https://www.youtube.com/oembed?url="
        self.listbase = "https://youtube.com/playlist?list="
        self.reg = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        self.dl_stats = {
            "total_requests": 0,
            "socialdown_downloads": 0,
            "ytdlp_downloads": 0,
            "existing_files": 0
        }


    async def exists(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if re.search(self.regex, link):
            return True
        else:
            return False

    async def url(self, message_1: Message) -> Union[str, None]:
        messages = [message_1]
        if message_1.reply_to_message:
            messages.append(message_1.reply_to_message)
        text = ""
        offset = None
        length = None
        for message in messages:
            if offset:
                break
            if message.entities:
                for entity in message.entities:
                    if entity.type == MessageEntityType.URL:
                        text = message.text or message.caption
                        offset, length = entity.offset, entity.length
                        break
            elif message.caption_entities:
                for entity in message.caption_entities:
                    if entity.type == MessageEntityType.TEXT_LINK:
                        return entity.url
        if offset in (None,):
            return None
        return text[offset : offset + length]

    async def details(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]

        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            title = result["title"]
            duration_min = result["duration"]
            thumbnail = result["thumbnails"][0]["url"].split("?")[0]
            vidid = result["id"]
            if str(duration_min) == "None":
                duration_sec = 0
            else:
                duration_sec = int(time_to_seconds(duration_min))
        return title, duration_min, duration_sec, thumbnail, vidid

    async def title(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]
            
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            title = result["title"]
        return title

    async def duration(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]

        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            duration = result["duration"]
        return duration

    async def thumbnail(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]

        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            thumbnail = result["thumbnails"][0]["url"].split("?")[0]
        return thumbnail

    async def video(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]

        proc = await asyncio.create_subprocess_exec(
            "yt-dlp",
            "--cookies", get_cookies_file(),
            "-g",
            "-f",
            "best[height<=?720][width<=?1280]",
            f"{link}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if stdout:
            return 1, stdout.decode().split("\n")[0]
        else:
            return 0, stderr.decode()

    async def playlist(self, link, limit, user_id, videoid: Union[bool, str] = None):
        if videoid:
            link = self.listbase + link
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]
        playlist = await shell_cmd(
            f"yt-dlp -i --get-id --flat-playlist --cookies {get_cookies_file()} --playlist-end {limit} --skip-download {link}"
        )
        try:
            result = playlist.split("\n")
            for key in result:
                if key == "":
                    result.remove(key)
        except:
            result = []
        return result

    async def track(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]

        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            title = result["title"]
            duration_min = result["duration"]
            vidid = result["id"]
            yturl = result["link"]
            thumbnail = result["thumbnails"][0]["url"].split("?")[0]
        track_details = {
            "title": title,
            "link": yturl,
            "vidid": vidid,
            "duration_min": duration_min,
            "thumb": thumbnail,
        }
        return track_details, vidid

    async def formats(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]
        ytdl_opts = {"quiet": True, "cookiefile" : get_cookies_file()}
        ydl = yt_dlp.YoutubeDL(ytdl_opts)
        with ydl:
            formats_available = []
            r = ydl.extract_info(link, download=False)
            for format in r["formats"]:
                try:
                    str(format["format"])
                except:
                    continue
                if not "dash" in str(format["format"]).lower():
                    try:
                        format["format"]
                        format["filesize"]
                        format["format_id"]
                        format["ext"]
                        format["format_note"]
                    except:
                        continue
                    formats_available.append(
                        {
                            "format": format["format"],
                            "filesize": format["filesize"],
                            "format_id": format["format_id"],
                            "ext": format["ext"],
                            "format_note": format["format_note"],
                            "yturl": link,
                        }
                    )
        return formats_available, link

    async def slider(self, link: str, query_type: int, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]

        try:
            results = []
            search = VideosSearch(link, limit=10)
            search_results = (await search.next()).get("result", [])

            for result in search_results:
                duration_str = result.get("duration", "0:00")
                try:
                    parts = duration_str.split(":")
                    duration_secs = 0
                    if len(parts) == 3:
                        duration_secs = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                    elif len(parts) == 2:
                        duration_secs = int(parts[0]) * 60 + int(parts[1])

                    if duration_secs <= 3600:
                        results.append(result)
                except (ValueError, IndexError):
                    continue

            if not results or query_type >= len(results):
                raise ValueError("No suitable videos found within duration limit")

            selected = results[query_type]
            return (
                selected["title"],
                selected["duration"],
                selected["thumbnails"][0]["url"].split("?")[0],
                selected["id"]
            )

        except Exception as e:
            LOGGER(__name__).error(f"Error in slider: {str(e)}")
            raise ValueError("Failed to fetch video details")

    # ============================================================================
    # OPTIMIZED: Direct Cookies Integration + SocialDown API
    # Fastest possible implementation - Cookies auto-downloaded and cached
    # ============================================================================
    async def download(
        self,
        link: str,
        mystic,
        video: Union[bool, str] = None,
        videoid: Union[bool, str] = None,
        songaudio: Union[bool, str] = None,
        songvideo: Union[bool, str] = None,
        format_id: Union[bool, str] = None,
        title: Union[bool, str] = None,
    ) -> str:
        """
        Ultra-fast download: SocialDown (PRIMARY) + yt-dlp with auto-cached cookies (FALLBACK)
        Cookies are downloaded once and cached for fast subsequent access.
        """
        if videoid:
            vid_id = link
            link = self.base + link
        loop = asyncio.get_running_loop()

        def create_session():
            session = requests.Session()
            retries = Retry(total=3, backoff_factor=0.1)
            session.mount('http://', HTTPAdapter(max_retries=retries))
            session.mount('https://', HTTPAdapter(max_retries=retries))
            return session

        # ============================================================================
        # METHOD 1: SocialDown API (FASTEST - 2-5 seconds)
        # ============================================================================
        async def download_with_socialdown(vid_id, format_type='audio'):
            """SocialDown API - Completely FREE and FAST"""
            try:
                try:
                    from config import SOCIALDOWN_BASE_URL, SOCIALDOWN_TIMEOUT
                except ImportError:
                    SOCIALDOWN_BASE_URL = "https://socialdown.itz-ashlynn.workers.dev"
                    SOCIALDOWN_TIMEOUT = 30
                
                logger.info(f"⚡ [SocialDown] Trying FAST API download for {vid_id}")
                
                yt_link = f"https://www.youtube.com/watch?v={vid_id}"
                api_url = f"{SOCIALDOWN_BASE_URL}/yt"
                
                params = {
                    'url': yt_link,
                    'format': 'mp3'
                }
                
                session = create_session()
                response = session.get(
                    api_url,
                    params=params,
                    headers={'Accept': 'application/json'},
                    timeout=SOCIALDOWN_TIMEOUT
                )
                session.close()
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get('success') and data.get('data'):
                        if isinstance(data['data'], list) and len(data['data']) > 0:
                            download_url = data['data'][0].get('downloadUrl')
                            
                            if download_url:
                                logger.info(f"✅ [SocialDown] FASTEST METHOD WORKED! {vid_id} (2-5 sec)")
                                return (True, download_url, True)
                
                logger.warning(f"[SocialDown] API failed")
                return (False, None, False)
                
            except Exception as e:
                logger.warning(f"[SocialDown] Exception: {str(e)}")
                return (False, None, False)

        # ============================================================================
        # METHOD 2: yt-dlp with Auto-Cached Cookies (FALLBACK - 10-30 seconds)
        # ============================================================================
        async def download_with_ytdlp_fast(url, filepath, max_retries=3):
            """yt-dlp with cached cookies - No delays!"""
            default_headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.youtube.com/",
            }

            def run_download():
                ydl_opts = {
                    "quiet": True,
                    "no_warnings": True,
                    "outtmpl": filepath,
                    "force_overwrites": True,
                    "nopart": True,
                    "retries": max_retries,
                    "http_headers": default_headers,
                    "concurrent_fragment_downloads": 8,
                    "cookiefile": get_cookies_file(),  # CACHED cookies - FAST!
                }
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([url])

            try:
                if os.path.exists(filepath):
                    os.remove(filepath)
                await loop.run_in_executor(None, run_download)
                if os.path.exists(filepath):
                    return filepath
            except Exception as e:
                logger.error(f"yt-dlp download failed: {str(e)}")
            if os.path.exists(filepath):
                os.remove(filepath)
            return None

        async def download_from_url(url, filepath):
            """Stream download from URL - Very fast"""
            try:
                session = create_session()
                response = session.get(url, stream=True, timeout=60)
                response.raise_for_status()
                
                with open(filepath, 'wb') as file:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            file.write(chunk)
                
                return filepath
                
            except Exception as e:
                logger.error(f"Direct download failed: {str(e)}")
                if os.path.exists(filepath):
                    os.remove(filepath)
                return None
            finally:
                session.close()

        async def audio_dl(vid_id):
            """Download audio: SocialDown (FASTEST) → yt-dlp (FAST)"""
            
            # TRY SOCIALDOWN FIRST (2-5 seconds) - FASTEST!
            try:
                success, download_url, is_direct = await download_with_socialdown(vid_id, 'mp3')
                if success and is_direct:
                    logger.info(f"⚡ [SocialDown] Got FAST stream URL")
                    filepath = os.path.join("downloads", f"{vid_id}.mp3")
                    os.makedirs("downloads", exist_ok=True)
                    
                    result = await download_from_url(download_url, filepath)
                    if result:
                        logger.info(f"✅ [SocialDown] Audio FASTEST download complete: {filepath}")
                        return result
            except Exception as e:
                logger.debug(f"[SocialDown] fallback: {e}")
            
            # FALLBACK TO YT-DLP WITH CACHED COOKIES (10-30 seconds) - STILL FAST!
            try:
                logger.info(f"⚡ [yt-dlp] Using CACHED cookies for FAST download")
                filepath = os.path.join("downloads", f"{vid_id}.mp3")
                os.makedirs("downloads", exist_ok=True)
                
                if os.path.exists(filepath):
                    return filepath
                
                yt_link = f"https://www.youtube.com/watch?v={vid_id}"
                result = await download_with_ytdlp_fast(yt_link, filepath)
                if result:
                    logger.info(f"✅ [yt-dlp] Audio FAST download complete with CACHED cookies: {filepath}")
                    return result
                    
            except Exception as e:
                logger.error(f"[yt-dlp] Error: {str(e)}")
            
            return None
        
        
        async def video_dl(vid_id):
            """Download video: SocialDown (FASTEST) → yt-dlp (FAST)"""
            
            # TRY SOCIALDOWN FIRST (2-5 seconds) - FASTEST!
            try:
                success, download_url, is_direct = await download_with_socialdown(vid_id, 'mp4')
                if success and is_direct:
                    logger.info(f"⚡ [SocialDown] Got FAST stream URL")
                    filepath = os.path.join("downloads", f"{vid_id}.mp4")
                    os.makedirs("downloads", exist_ok=True)
                    
                    result = await download_from_url(download_url, filepath)
                    if result:
                        logger.info(f"✅ [SocialDown] Video FASTEST download complete: {filepath}")
                        return result
            except Exception as e:
                logger.debug(f"[SocialDown] fallback: {e}")
            
            # FALLBACK TO YT-DLP WITH CACHED COOKIES (10-30 seconds) - STILL FAST!
            try:
                logger.info(f"⚡ [yt-dlp] Using CACHED cookies for FAST download")
                filepath = os.path.join("downloads", f"{vid_id}.mp4")
                os.makedirs("downloads", exist_ok=True)
                
                if os.path.exists(filepath):
                    return filepath
                
                yt_link = f"https://www.youtube.com/watch?v={vid_id}"
                result = await download_with_ytdlp_fast(yt_link, filepath)
                if result:
                    logger.info(f"✅ [yt-dlp] Video FAST download complete with CACHED cookies: {filepath}")
                    return result
                    
            except Exception as e:
                logger.error(f"[yt-dlp] Error: {str(e)}")
            
            return None
        
        def song_video_dl():
            """Download with specific format"""
            formats = f"{format_id}+140"
            fpath = f"downloads/{title}"
            ydl_optssx = {
                "format": formats,
                "outtmpl": fpath,
                "geo_bypass": True,
                "nocheckcertificate": True,
                "quiet": True,
                "no_warnings": True,
                "cookiefile": get_cookies_file(),  # Use cached cookies!
                "prefer_ffmpeg": True,
                "merge_output_format": "mp4",
            }
            x = yt_dlp.YoutubeDL(ydl_optssx)
            x.download([link])

        def song_audio_dl():
            """Download with specific format"""
            fpath = f"downloads/{title}.%(ext)s"
            ydl_optssx = {
                "format": format_id,
                "outtmpl": fpath,
                "geo_bypass": True,
                "nocheckcertificate": True,
                "quiet": True,
                "no_warnings": True,
                "cookiefile": get_cookies_file(),  # Use cached cookies!
                "prefer_ffmpeg": True,
                "postprocessors": [
                    {
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                        "preferredquality": "192",
                    }
                ],
            }
            x = yt_dlp.YoutubeDL(ydl_optssx)
            x.download([link])

        if songvideo:
            await loop.run_in_executor(None, song_video_dl)
            fpath = f"downloads/{title}.mp4"
            return fpath
        elif songaudio:
            await loop.run_in_executor(None, song_audio_dl)
            fpath = f"downloads/{title}.mp3"
            return fpath
        elif video:
            direct = True
            downloaded_file = await video_dl(vid_id)
        else:
            direct = True
            downloaded_file = await audio_dl(vid_id)
        
        return downloaded_file, direct
