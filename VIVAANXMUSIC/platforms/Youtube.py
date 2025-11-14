import asyncio
import glob
import json
import os
import random
import re
import time
import hashlib
from concurrent.futures import ThreadPoolExecutor
from typing import Union, Optional, Tuple, Dict
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
# INTELLIGENT CACHE SYSTEM - Auto cleanup after set time
# ============================================================================
class SmartCacheManager:
    """Smart cache that auto-deletes files after set duration"""
    
    def __init__(self, retention_hours: int = 24):
        self.cache_dir = os.path.join(os.getcwd(), "cache")
        self.download_dir = "downloads"
        self.metadata_file = os.path.join(self.cache_dir, "metadata.json")
        self.retention_seconds = retention_hours * 3600
        
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(self.download_dir, exist_ok=True)
        
        self.metadata = self._load_metadata()
        self.memory_cache = {}
        self.max_memory_cache = 50
    
    def _load_metadata(self) -> Dict:
        """Load metadata from disk"""
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def _save_metadata(self):
        """Save metadata to disk"""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.metadata, f)
        except:
            pass
    
    def check_cache(self, vid_id: str, format_type: str = "mp3") -> Optional[str]:
        """Check if file exists and not expired"""
        cache_key = f"{vid_id}_{format_type}"
        
        # Check memory cache first
        if cache_key in self.memory_cache:
            filepath = self.memory_cache[cache_key]
            if os.path.exists(filepath):
                return filepath
            else:
                del self.memory_cache[cache_key]
        
        # Check disk
        filepath = os.path.join(self.download_dir, f"{vid_id}.{format_type}")
        
        if os.path.exists(filepath):
            # Check if file is still valid
            if cache_key in self.metadata:
                file_age = time.time() - self.metadata[cache_key]["timestamp"]
                
                if file_age < self.retention_seconds:
                    # File is still valid
                    self.memory_cache[cache_key] = filepath
                    return filepath
                else:
                    # File expired - delete it
                    try:
                        os.remove(filepath)
                        del self.metadata[cache_key]
                        self._save_metadata()
                        logger.info(f"🗑️ [CACHE] Deleted expired file: {vid_id}")
                    except:
                        pass
                    return None
            else:
                # No metadata - assume file is valid
                self.memory_cache[cache_key] = filepath
                return filepath
        
        return None
    
    def add_cache(self, vid_id: str, filepath: str, format_type: str = "mp3"):
        """Add file to cache with timestamp"""
        cache_key = f"{vid_id}_{format_type}"
        
        self.memory_cache[cache_key] = filepath
        
        self.metadata[cache_key] = {
            "path": filepath,
            "timestamp": time.time(),
            "format": format_type,
            "vid_id": vid_id,
            "size": os.path.getsize(filepath) if os.path.exists(filepath) else 0
        }
        
        # Cleanup memory cache if too large
        if len(self.memory_cache) > self.max_memory_cache:
            oldest_key = min(self.memory_cache.keys(), 
                            key=lambda k: self.metadata.get(k, {}).get("timestamp", 0))
            del self.memory_cache[oldest_key]
        
        self._save_metadata()
    
    def cleanup_expired(self):
        """Remove all expired files"""
        current_time = time.time()
        removed_count = 0
        
        to_delete = []
        for cache_key, meta in self.metadata.items():
            file_age = current_time - meta.get("timestamp", 0)
            
            if file_age > self.retention_seconds:
                try:
                    filepath = meta.get("path", "")
                    if os.path.exists(filepath):
                        os.remove(filepath)
                        removed_count += 1
                    to_delete.append(cache_key)
                except Exception as e:
                    logger.debug(f"Failed to delete {filepath}: {str(e)}")
        
        for key in to_delete:
            del self.metadata[key]
        
        if removed_count > 0:
            self._save_metadata()
            logger.info(f"🗑️ [CACHE] Cleaned up {removed_count} expired files")
        
        return removed_count
    
    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        total_size = 0
        total_files = 0
        
        for meta in self.metadata.values():
            total_size += meta.get("size", 0)
            total_files += 1
        
        return {
            "total_files": total_files,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "memory_cache_size": len(self.memory_cache),
            "retention_hours": self.retention_seconds // 3600
        }

# Global cache manager - 24 hour retention
cache_manager = SmartCacheManager(retention_hours=24)

# ============================================================================
# PERSISTENT SESSION MANAGER
# ============================================================================
class SessionManager:
    """Manage persistent HTTP sessions for fast requests"""
    
    def __init__(self):
        self.sessions = {}
    
    def get_session(self, session_id: str = "default") -> requests.Session:
        """Get or create persistent session"""
        if session_id not in self.sessions:
            session = requests.Session()
            retries = Retry(total=2, backoff_factor=0.05, status_forcelist=[500, 502, 503, 504])
            adapter = HTTPAdapter(max_retries=retries, pool_connections=30, pool_maxsize=30)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            self.sessions[session_id] = session
        return self.sessions[session_id]

session_manager = SessionManager()

# ============================================================================
# COOKIES CONFIGURATION
# ============================================================================
COOKIES_URL = "https://pastebin.com/raw/RR0ucLw3"
COOKIES_CACHE_PATH = os.path.join(os.getcwd(), "cookies", "youtube_cookies.txt")

async def download_and_cache_cookies():
    """Download cookies and cache them"""
    try:
        os.makedirs(os.path.dirname(COOKIES_CACHE_PATH), exist_ok=True)
        
        if os.path.exists(COOKIES_CACHE_PATH):
            file_age = time.time() - os.path.getmtime(COOKIES_CACHE_PATH)
            if file_age < 86400:
                return COOKIES_CACHE_PATH
        
        logger.info(f"[Cookies] Downloading fresh cookies...")
        session = session_manager.get_session("cookies")
        response = session.get(COOKIES_URL, timeout=15)
        response.raise_for_status()
        
        with open(COOKIES_CACHE_PATH, 'w') as f:
            f.write(response.text)
        
        logger.info(f"✅ [Cookies] Downloaded successfully")
        return COOKIES_CACHE_PATH
        
    except Exception as e:
        logger.error(f"❌ [Cookies] Failed: {str(e)}")
        return COOKIES_CACHE_PATH

def get_cookies_file() -> str:
    """Get cookies file path"""
    try:
        folder_path = os.path.join(os.getcwd(), "cookies")
        if os.path.exists(folder_path):
            txt_files = glob.glob(os.path.join(folder_path, '*.txt'))
            if txt_files:
                if COOKIES_CACHE_PATH in txt_files:
                    return COOKIES_CACHE_PATH
                return random.choice(txt_files)
        return COOKIES_CACHE_PATH
    except:
        return COOKIES_CACHE_PATH

async def initialize_cookies():
    """Initialize cookies on startup"""
    logger.info("[Cookies] Initializing...")
    await download_and_cache_cookies()

# ============================================================================
# FAST API SOURCES - Get download URLs
# ============================================================================
class FastDownloadAPIs:
    """Try multiple APIs to get download URLs (like YouTube CDN)"""
    
    def __init__(self):
        self.apis = [
            {
                "name": "SocialDown",
                "base_url": "https://socialdown.itz-ashlynn.workers.dev",
                "timeout": 6
            },
            {
                "name": "YoutubeMate",
                "base_url": "https://api.youtubemate.com",
                "timeout": 6
            }
        ]
    
    async def try_api(self, api: Dict, vid_id: str, format_type: str = "mp3") -> Tuple[bool, Optional[str]]:
        """Try single API source"""
        try:
            session = session_manager.get_session(api["name"])
            yt_link = f"https://www.youtube.com/watch?v={vid_id}"
            
            if "socialdown" in api["name"].lower():
                params = {"url": yt_link, "format": format_type}
                resp = session.get(f"{api['base_url']}/yt", params=params, timeout=api["timeout"])
                
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('success') and data.get('data'):
                        if isinstance(data['data'], list) and data['data']:
                            download_url = data['data'][0].get('downloadUrl')
                            if download_url:
                                return (True, download_url)
            
            return (False, None)
        except Exception as e:
            logger.debug(f"[{api['name']}] Error: {str(e)}")
            return (False, None)
    
    async def get_download_url(self, vid_id: str, format_type: str = "mp3") -> Tuple[bool, Optional[str]]:
        """Get download URL from fastest API (parallel attempts)"""
        tasks = [self.try_api(api, vid_id, format_type) for api in self.apis]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, tuple) and result[0]:
                return result
        
        return (False, None)

fast_apis = FastDownloadAPIs()

# ============================================================================
# PARALLEL STREAM & DOWNLOAD (YouTube-like)
# ============================================================================
class ParallelDownloadManager:
    """Download file while it's being played (like YouTube streaming)"""
    
    async def download_with_chunks(self, download_url: str, filepath: str, timeout: int = 60) -> bool:
        """
        Download file in chunks to disk
        This allows playback to start while download continues
        """
        try:
            session = session_manager.get_session("download")
            
            logger.info(f"⬇️ [DOWNLOAD] Starting parallel download to: {filepath}")
            
            response = session.get(download_url, stream=True, timeout=timeout)
            response.raise_for_status()
            
            chunk_size = 1024 * 512  # 512KB chunks
            downloaded = 0
            total_size = int(response.headers.get('content-length', 0))
            
            # Start writing immediately (allows partial playback)
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Log progress every 1MB
                        if total_size > 0 and downloaded % (1024 * 1024) == 0:
                            progress_pct = (downloaded / total_size) * 100
                            logger.debug(f"📥 [DOWNLOAD] {progress_pct:.1f}% - {filepath}")
            
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                logger.info(f"✅ [DOWNLOAD] Complete! {filepath}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ [DOWNLOAD] Failed: {str(e)}")
            if os.path.exists(filepath):
                try:
                    os.remove(filepath)
                except:
                    pass
            return False
    
    async def download_with_ytdlp(self, link: str, filepath: str) -> bool:
        """
        Download using yt-dlp with parallel fragment downloads
        (Last resort if APIs fail)
        """
        try:
            loop = asyncio.get_running_loop()
            
            def ytdlp_download():
                ydl_opts = {
                    "quiet": True,
                    "no_warnings": True,
                    "outtmpl": filepath,
                    "force_overwrites": True,
                    "retries": 1,
                    "socket_timeout": 30,
                    "concurrent_fragment_downloads": 12,  # Parallel fragments
                    "fragment_retries": 1,
                    "skip_unavailable_fragments": True,
                    "cookiefile": get_cookies_file(),
                    "format": "bestaudio[ext=m4a]/best[height<=480]/best",
                    "postprocessors": [{
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                        "preferredquality": "96",
                    }],
                }
                
                logger.info(f"⬇️ [YT-DLP] Starting yt-dlp download...")
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([link])
            
            await asyncio.wait_for(
                loop.run_in_executor(None, ytdlp_download),
                timeout=30
            )
            
            if os.path.exists(filepath):
                logger.info(f"✅ [YT-DLP] Download complete!")
                return True
            
            return False
            
        except asyncio.TimeoutError:
            logger.error(f"❌ [YT-DLP] Download timeout")
            return False
        except Exception as e:
            logger.error(f"❌ [YT-DLP] Error: {str(e)}")
            return False

download_manager = ParallelDownloadManager()

# ============================================================================
# MAIN YOUTUBE API
# ============================================================================
class YouTubeAPI:
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="
        self.regex = r"(?:youtube\.com|youtu\.be)"
        self.status = "https://www.youtube.com/oembed?url="
        self.listbase = "https://youtube.com/playlist?list="
        self.reg = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        self.dl_stats = {
            "total_requests": 0,
            "cache_hits": 0,
            "api_downloads": 0,
            "ytdlp_downloads": 0
        }

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
                        offset, length = entity.offset, entity.length
                        return text[offset : offset + length]
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

        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            return result["title"]

    async def duration(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]

        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            return result["duration"]

    async def thumbnail(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]

        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            return result["thumbnails"][0]["url"].split("?")[0]

    async def video(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]

        proc = await asyncio.create_subprocess_exec(
            "yt-dlp",
            "--cookies", get_cookies_file(),
            "-g",
            "-f", "best[height<=?720][width<=?1280]",
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

        proc = await asyncio.create_subprocess_shell(
            f"yt-dlp -i --get-id --flat-playlist --cookies {get_cookies_file()} --playlist-end {limit} --skip-download {link}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        
        result = stdout.decode("utf-8").split("\n") if stdout else []
        return [key for key in result if key]

    async def track(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]

        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            return {
                "title": result["title"],
                "link": result["link"],
                "vidid": result["id"],
                "duration_min": result["duration"],
                "thumb": result["thumbnails"][0]["url"].split("?")[0],
            }, result["id"]

    async def formats(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]

        ydl_opts = {"quiet": True, "cookiefile": get_cookies_file()}
        ydl = yt_dlp.YoutubeDL(ydl_opts)
        
        with ydl:
            formats_available = []
            r = ydl.extract_info(link, download=False)
            
            for format in r.get("formats", []):
                if "dash" not in str(format.get("format", "")).lower():
                    try:
                        formats_available.append({
                            "format": format["format"],
                            "filesize": format.get("filesize"),
                            "format_id": format["format_id"],
                            "ext": format["ext"],
                            "format_note": format.get("format_note"),
                            "yturl": link,
                        })
                    except:
                        continue
        
        return formats_available, link

    async def slider(self, link: str, query_type: int, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]

        try:
            results = []
            search = VideosSearch(link, limit=10)
            
            for result in (await search.next()).get("result", []):
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
                except:
                    continue

            if not results or query_type >= len(results):
                raise ValueError("No suitable videos found")

            selected = results[query_type]
            return (
                selected["title"],
                selected["duration"],
                selected["thumbnails"][0]["url"].split("?")[0],
                selected["id"]
            )

        except Exception as e:
            logger.error(f"Slider error: {str(e)}")
            raise ValueError("Failed to fetch video details")

    # ============================================================================
    # YOUTUBE-LIKE PARALLEL DOWNLOAD (Stream + Download simultaneously)
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
        YOUTUBE-STYLE DOWNLOAD:
        1. Try APIs to get download URL (Fast - 3-5 seconds)
        2. Start storing to VPS immediately (Parallel)
        3. Return file path for playback (Starts playing while downloading)
        4. Auto-delete old files after 24 hours
        """
        if videoid:
            vid_id = link
            link = self.base + link
        else:
            vid_id = link.split("v=")[-1].split("&")[0] if "v=" in link else ""
        
        format_type = "mp4" if video else "mp3"
        
        # ====================================================================
        # STEP 1: CHECK CACHE (instant <10ms)
        # ====================================================================
        cached_file = cache_manager.check_cache(vid_id, format_type)
        if cached_file:
            logger.info(f"⚡ [CACHE] INSTANT! Returned cached file: {vid_id}")
            self.dl_stats["cache_hits"] += 1
            return cached_file
        
        self.dl_stats["total_requests"] += 1
        
        # ====================================================================
        # STEP 2: TRY FAST APIs (3-5 seconds) - Get download URL
        # ====================================================================
        logger.info(f"🔍 [API] Searching for download URL...")
        
        api_success, download_url = await fast_apis.get_download_url(vid_id, format_type)
        
        if api_success and download_url:
            logger.info(f"✅ [API] Found URL in 3-5 seconds!")
            
            filepath = os.path.join("downloads", f"{vid_id}.{format_type}")
            
            # Start downloading immediately while returning file path
            # This allows playback to start while download continues
            asyncio.create_task(
                download_manager.download_with_chunks(download_url, filepath)
            )
            
            # Wait a tiny bit for file to exist, then return (allows immediate playback)
            for _ in range(5):  # Wait up to 500ms
                if os.path.exists(filepath):
                    logger.info(f"▶️ [PLAYBACK] File ready! Starting playback while downloading...")
                    cache_manager.add_cache(vid_id, filepath, format_type)
                    self.dl_stats["api_downloads"] += 1
                    return filepath
                await asyncio.sleep(0.1)
            
            # If file still being created, return path anyway (will be ready soon)
            cache_manager.add_cache(vid_id, filepath, format_type)
            logger.info(f"▶️ [PLAYBACK] Started playback (file being created...)")
            self.dl_stats["api_downloads"] += 1
            return filepath
        
        # ====================================================================
        # STEP 3: FALLBACK - yt-dlp with parallel fragments (10-20 seconds)
        # ====================================================================
        logger.info(f"⬇️ [YT-DLP] APIs failed, using yt-dlp with parallel downloads...")
        
        filepath = os.path.join("downloads", f"{vid_id}.{format_type}")
        
        if os.path.exists(filepath):
            cache_manager.add_cache(vid_id, filepath, format_type)
            return filepath
        
        success = await download_manager.download_with_ytdlp(link, filepath)
        
        if success and os.path.exists(filepath):
            logger.info(f"✅ [YT-DLP] Downloaded successfully!")
            cache_manager.add_cache(vid_id, filepath, format_type)
            self.dl_stats["ytdlp_downloads"] += 1
            return filepath
        
        # ====================================================================
        # CUSTOM FORMAT DOWNLOADS
        # ====================================================================
        if songvideo or songaudio:
            fpath = f"downloads/{title}"
            ydl_opts = {
                "format": f"{format_id}+140" if songvideo else format_id,
                "outtmpl": fpath,
                "quiet": True,
                "no_warnings": True,
                "cookiefile": get_cookies_file(),
                "socket_timeout": 30,
            }
            
            if songaudio:
                ydl_opts["postprocessors"] = [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "96",
                }]
            
            try:
                loop = asyncio.get_running_loop()
                ydl = yt_dlp.YoutubeDL(ydl_opts)
                await loop.run_in_executor(None, ydl.download, [link])
                
                final_path = f"downloads/{title}.{'mp4' if songvideo else 'mp3'}"
                
                if os.path.exists(final_path):
                    cache_manager.add_cache(vid_id, final_path, format_type)
                    return final_path
            except Exception as e:
                logger.error(f"Custom format failed: {str(e)}")
        
        logger.error(f"❌ All download methods failed for {vid_id}")
        return None

# ============================================================================
# BACKGROUND CLEANUP TASK - Delete expired files
# ============================================================================
async def cleanup_task():
    """Run cleanup every hour to delete old files"""
    while True:
        try:
            await asyncio.sleep(3600)  # Run every hour
            
            removed = cache_manager.cleanup_expired()
            
            if removed > 0:
                stats = cache_manager.get_cache_stats()
                logger.info(f"🧹 [CLEANUP] Removed {removed} files. Cache: {stats['total_files']} files, {stats['total_size_mb']}MB")
            
        except Exception as e:
            logger.error(f"Cleanup task error: {str(e)}")

async def schedule_cleanup_task():
    """Schedule cleanup task"""
    asyncio.create_task(cleanup_task())

async def init_youtube_api():
    """Initialize YouTube API system"""
    logger.info("[YouTube] Initializing YouTube-style parallel download system...")
    
    await initialize_cookies()
    await schedule_cleanup_task()
    
    stats = cache_manager.get_cache_stats()
    logger.info(f"[YouTube] ✅ Ready! Auto-delete enabled ({stats['retention_hours']}h retention)")
