import asyncio
import glob
import json
import os
import random
import re
import time
from typing import Union
import requests
import yt_dlp
import aiohttp
from pyrogram.enums import MessageEntityType
from pyrogram.types import Message
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from youtubesearchpython.__future__ import VideosSearch
from VIVAANXMUSIC import LOGGER
from VIVAANXMUSIC.utils.database import is_on_off
from VIVAANXMUSIC.utils.formatters import time_to_seconds

logger = LOGGER(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================
SOCIALDOWN_API = "https://socialdown.itz-ashlynn.workers.dev/yt"
COOKIE_URL = "https://pastebin.com/raw/RR0ucLw3"

CACHE_TIME = 60 * 60  # 1 hour
DOWNLOADS_FOLDER = "downloads"
COOKIE_FILE_PATH = "downloads/youtube_cookies.txt"

MAX_RETRIES = 2
RETRY_DELAY = 0.5
CONNECTION_TIMEOUT = 15
DOWNLOAD_TIMEOUT = 300

# ============================================================================
# COOKIE MANAGEMENT
# ============================================================================
def download_cookies():
    """Download cookies from Pastebin"""
    try:
        logger.info(f"📥 Downloading cookies...")
        os.makedirs(os.path.dirname(COOKIE_FILE_PATH), exist_ok=True)
        
        response = requests.get(COOKIE_URL, timeout=10)
        
        if response.status_code == 200 and len(response.text) > 100:
            with open(COOKIE_FILE_PATH, 'w') as f:
                f.write(response.text)
            logger.info(f"✅ Cookies ready")
            return COOKIE_FILE_PATH
        
        return None
    except Exception as e:
        logger.error(f"Cookie error: {e}")
        return None

def get_cookie_file():
    """Get cookie file (auto-download if needed)"""
    try:
        if os.path.exists(COOKIE_FILE_PATH):
            file_age = time.time() - os.path.getmtime(COOKIE_FILE_PATH)
            if file_age < 3600:
                return COOKIE_FILE_PATH
        
        return download_cookies()
    except:
        return None

async def shell_cmd(cmd):
    """Execute shell command"""
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    if err and "unavailable" not in err.decode("utf-8").lower():
        return err.decode("utf-8")
    return out.decode("utf-8")

# ============================================================================
# YOUTUBE API - 4-TIER FALLBACK SYSTEM
# ============================================================================
class YouTubeAPI:
    """YouTube API - Production Ready"""
    
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="
        self.regex = r"(?:youtube\.com|youtu\.be)"
        self.status = "https://www.youtube.com/oembed?url="
        self.listbase = "https://youtube.com/playlist?list="
        self.reg = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

    async def _get_video_details(self, link: str, limit: int = 20):
        """GET VIDEO DETAILS - NULL SAFE"""
        try:
            if not link:
                return None
            
            link = str(link).strip()
            if not link:
                return None
            
            try:
                results = VideosSearch(link, limit=limit)
                search_results = (await results.next()).get("result", [])
            except:
                return None

            if not search_results:
                return None
            
            result = search_results[0]
            if not isinstance(result, dict):
                return None
            
            # ✅ SAFE EXTRACTION
            title = result.get("title") or "Unknown"
            if isinstance(title, str):
                title = title.strip() or "Unknown"
            else:
                title = str(title).strip() or "Unknown"
            
            duration = result.get("duration") or "0:00"
            if isinstance(duration, str):
                duration = duration.strip() or "0:00"
            else:
                duration = str(duration).strip() or "0:00"
            
            # Thumbnail
            thumbnails = result.get("thumbnails") or []
            thumbnail_url = ""
            
            if isinstance(thumbnails, list) and len(thumbnails) > 0:
                thumb = thumbnails[0]
                if isinstance(thumb, dict):
                    url = thumb.get("url")
                    if url and isinstance(url, str):
                        url = url.strip().split("?")[0]
                        if url:
                            thumbnail_url = url
            
            if not thumbnail_url:
                thumbnail_url = "https://via.placeholder.com/320x180"
            
            # Video ID
            video_id = result.get("id")
            if not video_id:
                return None
            
            video_id = str(video_id).strip()
            if not video_id:
                return None
            
            # Link
            link_url = result.get("link")
            if not link_url:
                link_url = f"https://www.youtube.com/watch?v={video_id}"
            else:
                link_url = str(link_url).strip()
                if not link_url:
                    link_url = f"https://www.youtube.com/watch?v={video_id}"
            
            return {
                "title": title,
                "duration": duration,
                "thumbnails": [{"url": thumbnail_url}],
                "id": video_id,
                "link": link_url
            }

        except Exception as e:
            logger.error(f"Details error: {e}")
            return None

    async def _fetch_api(self, url, fmt="mp3"):
        """TIER 3: Fetch from API"""
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"⚡ [API] Trying SocialDown...")
                params = {"url": url, "format": fmt}
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        SOCIALDOWN_API,
                        params=params,
                        timeout=aiohttp.ClientTimeout(total=CONNECTION_TIMEOUT)
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            
                            if data.get("success") and data.get("data"):
                                stream_url = data["data"][0].get("downloadUrl")
                                if stream_url:
                                    logger.info(f"✅ [API] Got URL!")
                                    return stream_url
                
                await asyncio.sleep(RETRY_DELAY)
            except Exception as e:
                logger.debug(f"[API] Error: {e}")
                await asyncio.sleep(RETRY_DELAY)
        
        return None

    async def _download_file(self, url, filepath):
        """Download file from URL"""
        for attempt in range(MAX_RETRIES):
            try:
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        url,
                        timeout=aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT)
                    ) as resp:
                        if resp.status == 200:
                            with open(filepath, 'wb') as f:
                                async for chunk in resp.content.iter_chunked(1024*1024):
                                    f.write(chunk)
                            
                            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                                logger.info(f"✅ [DOWNLOAD] Complete!")
                                return True
                
                await asyncio.sleep(RETRY_DELAY)
            except Exception as e:
                logger.debug(f"[DOWNLOAD] Error: {e}")
                await asyncio.sleep(RETRY_DELAY)
        
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except:
                pass
        return False

    async def exists(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        return bool(re.search(self.regex, link))

    async def url(self, message_1: Message) -> Union[str, None]:
        messages = [message_1]
        if message_1.reply_to_message:
            messages.append(message_1.reply_to_message)
        
        for message in messages:
            if message.entities:
                for entity in message.entities:
                    if entity.type == MessageEntityType.URL:
                        text = message.text or message.caption
                        return text[entity.offset : entity.offset + entity.length]
            elif message.caption_entities:
                for entity in message.caption_entities:
                    if entity.type == MessageEntityType.TEXT_LINK:
                        return entity.url
        return None

    async def details(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]

        result = await self._get_video_details(link)
        if not result:
            raise ValueError("No video found")

        title = result.get("title", "Unknown")
        duration_min = result.get("duration", "0:00")
        
        thumbnails = result.get("thumbnails", [])
        thumbnail = thumbnails[0].get("url", "") if thumbnails else ""
        
        vidid = result.get("id", "")

        try:
            if duration_min and str(duration_min) != "None":
                duration_sec = int(time_to_seconds(duration_min))
            else:
                duration_sec = 0
        except:
            duration_sec = 0

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
            
        result = await self._get_video_details(link)
        return result.get("title", "Unknown") if result else "Unknown"

    async def duration(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]

        result = await self._get_video_details(link)
        return result.get("duration", "0:00") if result else "0:00"

    async def thumbnail(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]

        result = await self._get_video_details(link)
        if not result:
            return ""
        
        thumbnails = result.get("thumbnails", [])
        return thumbnails[0].get("url", "") if thumbnails else ""

    async def video(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]

        video_url = await self._fetch_api(link, "mp4")
        if video_url:
            return 1, video_url
        
        logger.info("Trying yt-dlp...")
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp", "--cookies", get_cookie_file(), "-g", "-f", "best[height<=?720][width<=?1280]",
            f"{link}", stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
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
            f"yt-dlp -i --get-id --flat-playlist --cookies {get_cookie_file()} --playlist-end {limit} --skip-download {link}"
        )
        try:
            return [x for x in playlist.split("\n") if x.strip()]
        except:
            return []

    async def track(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]

        result = await self._get_video_details(link)
        if not result:
            raise ValueError("No video found")

        return {
            "title": result.get("title", "Unknown"),
            "link": result.get("link", ""),
            "vidid": result.get("id", ""),
            "duration_min": result.get("duration", "0:00"),
            "thumb": result.get("thumbnails", [{}])[0].get("url", ""),
        }, result.get("id", "")

    async def formats(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]
        elif "&si=" in link:
            link = link.split("&si=")[0]
        
        ytdl_opts = {"quiet": True, "cookiefile": get_cookie_file()}
        ydl = yt_dlp.YoutubeDL(ytdl_opts)
        with ydl:
            formats_available = []
            r = ydl.extract_info(link, download=False)
            for fmt in r.get("formats", []):
                try:
                    if "dash" not in str(fmt.get("format", "")).lower():
                        formats_available.append({
                            "format": fmt["format"],
                            "filesize": fmt.get("filesize", 0),
                            "format_id": fmt["format_id"],
                            "ext": fmt["ext"],
                            "format_note": fmt.get("format_note", ""),
                            "yturl": link,
                        })
                except:
                    pass
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
                try:
                    duration_str = result.get("duration", "0:00")
                    if not duration_str:
                        duration_str = "0:00"
                    
                    parts = str(duration_str).split(":")
                    duration_secs = 0
                    
                    try:
                        if len(parts) == 3:
                            duration_secs = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                        elif len(parts) == 2:
                            duration_secs = int(parts[0]) * 60 + int(parts[1])
                    except:
                        duration_secs = 0

                    if duration_secs <= 3600:
                        results.append(result)
                except:
                    pass

            if not results or query_type >= len(results):
                raise ValueError("No videos found")

            selected = results[query_type]
            
            return (
                selected.get("title", "Unknown"),
                selected.get("duration", "0:00"),
                selected.get("thumbnails", [{}])[0].get("url", ""),
                selected.get("id", "")
            )

        except Exception as e:
            logger.error(f"slider() error: {e}")
            raise ValueError("Failed to get videos")

    async def download(self, link: str, mystic, video: Union[bool, str] = None, videoid: Union[bool, str] = None,
                       songaudio: Union[bool, str] = None, songvideo: Union[bool, str] = None,
                       format_id: Union[bool, str] = None, title: Union[bool, str] = None) -> str:
        """
        4-TIER PERFECT FALLBACK:
        TIER 1: Local VPS Storage (⚡⚡⚡)
        TIER 2: SocialDown API (⚡⚡)
        TIER 3: yt-dlp (✅)
        """
        if videoid:
            vid_id = link
            link = self.base + link
        
        loop = asyncio.get_running_loop()

        async def audio_dl(vid_id):
            try:
                youtube_url = f"https://www.youtube.com/watch?v={vid_id}"
                filepath = os.path.join(DOWNLOADS_FOLDER, f"{vid_id}.mp3")
                
                logger.info(f"🎵 AUDIO: {vid_id}")
                
                # ====================================================
                # TIER 1: Local VPS Storage (INSTANT ⚡⚡⚡)
                # ====================================================
                if os.path.exists(filepath):
                    if time.time() - os.path.getmtime(filepath) < CACHE_TIME:
                        size = os.path.getsize(filepath)
                        if size > 0:
                            logger.info(f"⚡⚡⚡ TIER 1 LOCAL: INSTANT ({size} bytes)")
                            return filepath, False
                
                # ====================================================
                # TIER 2: SocialDown API (FAST ⚡⚡)
                # ====================================================
                logger.info(f"⚡⚡ TIER 2 API: Fetching...")
                audio_url = await self._fetch_api(youtube_url, "mp3")
                
                if audio_url:
                    if await self._download_file(audio_url, filepath):
                        logger.info(f"⚡⚡ TIER 2 API: Success")
                        return filepath, False
                
                # ====================================================
                # TIER 3: yt-dlp + Cookies (GUARANTEED ✅)
                # ====================================================
                logger.info(f"✅ TIER 3 yt-dlp: Downloading...")
                
                cookies = get_cookie_file()
                if not cookies:
                    logger.error(f"No cookies!")
                    return None, False
                
                for attempt in range(MAX_RETRIES):
                    try:
                        proc = await asyncio.create_subprocess_exec(
                            "yt-dlp", "--cookies", cookies, "-x", "-f", "bestaudio",
                            "-o", filepath, youtube_url,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                        )
                        stdout, stderr = await proc.communicate()
                        
                        if proc.returncode == 0 and os.path.exists(filepath):
                            size = os.path.getsize(filepath)
                            if size > 0:
                                logger.info(f"✅ TIER 3 yt-dlp: Success ({size} bytes)")
                                return filepath, False
                        
                        await asyncio.sleep(RETRY_DELAY)
                        
                    except Exception as e:
                        logger.warning(f"yt-dlp attempt {attempt+1} failed: {e}")
                        await asyncio.sleep(RETRY_DELAY)
                
                logger.error(f"❌ ALL TIERS FAILED")
                return None, False
                    
            except Exception as e:
                logger.error(f"FATAL audio_dl: {e}")
                return None, False

        async def video_dl(vid_id):
            try:
                youtube_url = f"https://www.youtube.com/watch?v={vid_id}"
                filepath = os.path.join(DOWNLOADS_FOLDER, f"{vid_id}.mp4")
                
                logger.info(f"🎥 VIDEO: {vid_id}")
                
                # TIER 1: Local
                if os.path.exists(filepath):
                    if time.time() - os.path.getmtime(filepath) < CACHE_TIME:
                        if os.path.getsize(filepath) > 0:
                            logger.info(f"⚡⚡⚡ TIER 1 LOCAL: INSTANT")
                            return filepath, False
                
                # TIER 2: API
                logger.info(f"⚡⚡ TIER 2 API: Fetching...")
                video_url = await self._fetch_api(youtube_url, "mp4")
                
                if video_url:
                    if await self._download_file(video_url, filepath):
                        logger.info(f"⚡⚡ TIER 2 API: Success")
                        return filepath, False
                
                # TIER 3: yt-dlp
                logger.info(f"✅ TIER 3 yt-dlp: Downloading...")
                
                cookies = get_cookie_file()
                if not cookies:
                    return None, False
                
                for attempt in range(MAX_RETRIES):
                    try:
                        proc = await asyncio.create_subprocess_exec(
                            "yt-dlp", "--cookies", cookies, "-f", "best[height<=?720]",
                            "-o", filepath, youtube_url,
                            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                        )
                        stdout, stderr = await proc.communicate()
                        
                        if proc.returncode == 0 and os.path.exists(filepath):
                            if os.path.getsize(filepath) > 0:
                                logger.info(f"✅ TIER 3 yt-dlp: Success")
                                return filepath, False
                        
                        await asyncio.sleep(RETRY_DELAY)
                    except:
                        await asyncio.sleep(RETRY_DELAY)
                
                return None, False
                    
            except Exception as e:
                logger.error(f"FATAL video_dl: {e}")
                return None, False
        
        def song_video_dl():
            formats = f"{format_id}+140"
            fpath = f"{DOWNLOADS_FOLDER}/{title}"
            ydl_opts = {
                "format": formats, "outtmpl": fpath, "geo_bypass": True, "nocheckcertificate": True,
                "quiet": True, "no_warnings": True, "cookiefile": get_cookie_file(),
                "prefer_ffmpeg": True, "merge_output_format": "mp4",
            }
            yt_dlp.YoutubeDL(ydl_opts).download([link])

        def song_audio_dl():
            fpath = f"{DOWNLOADS_FOLDER}/{title}.%(ext)s"
            ydl_opts = {
                "format": format_id, "outtmpl": fpath, "geo_bypass": True, "nocheckcertificate": True,
                "quiet": True, "no_warnings": True, "cookiefile": get_cookie_file(),
                "prefer_ffmpeg": True,
                "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}],
            }
            yt_dlp.YoutubeDL(ydl_opts).download([link])

        if songvideo:
            await loop.run_in_executor(None, song_video_dl)
            return f"{DOWNLOADS_FOLDER}/{title}.mp4", False
            
        elif songaudio:
            await loop.run_in_executor(None, song_audio_dl)
            return f"{DOWNLOADS_FOLDER}/{title}.mp3", False
            
        elif video:
            return await video_dl(vid_id)
            
        else:
            return await audio_dl(vid_id)

async def schedule_cleanup_task():
    """Background cleanup"""
    pass

async def init_youtube_api():
    """Initialize"""
    logger.info("[YouTube] Initialized - 4-TIER FALLBACK READY")
    await download_cookies()
