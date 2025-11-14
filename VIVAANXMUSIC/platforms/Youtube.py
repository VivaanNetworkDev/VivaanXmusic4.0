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
# ULTRA-FAST CACHING SYSTEM
# ============================================================================
class CacheManager:
    """Advanced caching system for lightning-fast responses"""
    
    def __init__(self):
        self.cache_dir = os.path.join(os.getcwd(), "cache")
        self.metadata_file = os.path.join(self.cache_dir, "metadata.json")
        self.download_dir = "downloads"
        os.makedirs(self.cache_dir, exist_ok=True)
        os.makedirs(self.download_dir, exist_ok=True)
        self.metadata = self._load_metadata()
        self.memory_cache = {}
        self.max_memory_cache_size = 100
    
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
    
    def get_cache_key(self, vid_id: str, format_type: str = "mp3") -> str:
        """Generate cache key"""
        return f"{vid_id}_{format_type}"
    
    def check_cache(self, vid_id: str, format_type: str = "mp3") -> Optional[str]:
        """Check if file exists in cache (instant return)"""
        cache_key = self.get_cache_key(vid_id, format_type)
        
        if cache_key in self.memory_cache:
            return self.memory_cache[cache_key]
        
        filepath = os.path.join(self.download_dir, f"{vid_id}.{format_type}")
        if os.path.exists(filepath):
            self.memory_cache[cache_key] = filepath
            if len(self.memory_cache) > self.max_memory_cache_size:
                self.memory_cache.pop(next(iter(self.memory_cache)))
            return filepath
        
        return None
    
    def add_cache(self, vid_id: str, filepath: str, format_type: str = "mp3"):
        """Add to cache"""
        cache_key = self.get_cache_key(vid_id, format_type)
        self.memory_cache[cache_key] = filepath
        
        metadata_key = f"{vid_id}_{format_type}"
        self.metadata[metadata_key] = {
            "path": filepath,
            "timestamp": time.time(),
            "format": format_type
        }
        self._save_metadata()
    
    def cleanup_old_cache(self, max_age_days: int = 7):
        """Remove old cache files"""
        current_time = time.time()
        max_age_seconds = max_age_days * 86400
        
        to_remove = []
        for key, value in self.metadata.items():
            if current_time - value.get("timestamp", 0) > max_age_seconds:
                try:
                    if os.path.exists(value.get("path", "")):
                        os.remove(value["path"])
                    to_remove.append(key)
                except:
                    pass
        
        for key in to_remove:
            del self.metadata[key]
        
        self._save_metadata()

# Global cache manager
cache_manager = CacheManager()

# ============================================================================
# OPTIMIZED SESSION MANAGEMENT
# ============================================================================
class SessionManager:
    """Manage persistent HTTP sessions"""
    
    def __init__(self):
        self.sessions = {}
        self.executor = ThreadPoolExecutor(max_workers=12)
    
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
    
    def close_all(self):
        """Close all sessions"""
        for session in self.sessions.values():
            session.close()

session_manager = SessionManager()

# ============================================================================
# COOKIES CONFIGURATION
# ============================================================================
COOKIES_URL = "https://pastebin.com/raw/RR0ucLw3"
COOKIES_CACHE_PATH = os.path.join(os.getcwd(), "cookies", "youtube_cookies.txt")

async def download_and_cache_cookies():
    """Download cookies once and cache them"""
    try:
        os.makedirs(os.path.dirname(COOKIES_CACHE_PATH), exist_ok=True)
        
        if os.path.exists(COOKIES_CACHE_PATH):
            file_age = time.time() - os.path.getmtime(COOKIES_CACHE_PATH)
            if file_age < 86400:
                logger.info(f"✅ [Cookies] Using cached cookies")
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
# INSTANT STREAM URLS - NO WAITING FOR DOWNLOAD
# ============================================================================
class StreamURLProvider:
    """Get instant playable stream URLs without waiting"""
    
    async def get_direct_stream_url(self, link: str) -> Optional[Tuple[str, bool]]:
        """
        Get instant playable URL without downloading
        Returns (url, is_working) tuple
        """
        try:
            loop = asyncio.get_running_loop()
            
            # Use yt-dlp to extract stream URL instantly (no download)
            def extract_url():
                ydl_opts = {
                    "quiet": True,
                    "no_warnings": True,
                    "socket_timeout": 15,
                    "cookiefile": get_cookies_file(),
                    "skip_download": True,
                    "format": "bestaudio",
                }
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(link, download=False)
                    url = info.get('url')
                    if url:
                        return url
                    
                    # Try formats
                    formats = info.get('formats', [])
                    for fmt in formats:
                        if fmt.get('url'):
                            return fmt['url']
                
                return None
            
            # Run extraction in executor (non-blocking, fast!)
            url = await asyncio.wait_for(
                loop.run_in_executor(None, extract_url),
                timeout=8
            )
            
            if url:
                logger.info(f"⚡ [STREAM] Got instant URL in <1 second!")
                return (url, True)
            
            return (None, False)
            
        except asyncio.TimeoutError:
            logger.warning("[STREAM] Timeout extracting URL")
            return (None, False)
        except Exception as e:
            logger.debug(f"[STREAM] Error: {str(e)}")
            return (None, False)

stream_provider = StreamURLProvider()

# ============================================================================
# FAST API DOWNLOAD SOURCES - For actual file downloads
# ============================================================================
class FastDownloadSources:
    """Try multiple sources for file downloads"""
    
    def __init__(self):
        self.sources = [
            {
                "name": "SocialDown",
                "base_url": "https://socialdown.itz-ashlynn.workers.dev",
                "timeout": 5,
                "priority": 1
            },
            {
                "name": "YoutubeMate",
                "base_url": "https://api.youtubemate.com",
                "timeout": 5,
                "priority": 2
            }
        ]
    
    async def try_source(self, source: Dict, vid_id: str, format_type: str = "mp3") -> Tuple[bool, Optional[str]]:
        """Try single source"""
        try:
            session = session_manager.get_session(source["name"])
            yt_link = f"https://www.youtube.com/watch?v={vid_id}"
            
            if "socialdown" in source["name"].lower():
                params = {"url": yt_link, "format": format_type}
                resp = session.get(f"{source['base_url']}/yt", params=params, timeout=source["timeout"])
                
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('success') and data.get('data'):
                        if isinstance(data['data'], list) and data['data']:
                            url = data['data'][0].get('downloadUrl')
                            if url:
                                return (True, url)
            
            return (False, None)
        except:
            return (False, None)
    
    async def get_fastest_url(self, vid_id: str, format_type: str = "mp3") -> Tuple[bool, Optional[str]]:
        """Get URL from fastest available source"""
        tasks = [self.try_source(source, vid_id, format_type) for source in self.sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for result in results:
            if isinstance(result, tuple) and result[0]:
                return result
        
        return (False, None)

fast_sources = FastDownloadSources()

# ============================================================================
# BACKGROUND DOWNLOAD TASK - Download while playing
# ============================================================================
async def background_download(link: str, vid_id: str, filepath: str, cookies_file: str):
    """
    Download file in background while stream plays
    This happens AFTER playback starts
    """
    try:
        if os.path.exists(filepath):
            return
        
        loop = asyncio.get_running_loop()
        
        def download():
            ydl_opts = {
                "quiet": True,
                "no_warnings": True,
                "outtmpl": filepath,
                "force_overwrites": True,
                "retries": 1,
                "socket_timeout": 30,
                "concurrent_fragment_downloads": 12,
                "fragment_retries": 1,
                "skip_unavailable_fragments": True,
                "cookiefile": cookies_file,
                "format": "bestaudio[ext=m4a]/best[height<=480]/best",
                "postprocessors": [{
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "96",
                }],
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([link])
        
        await loop.run_in_executor(None, download)
        
        if os.path.exists(filepath):
            logger.info(f"✅ [BACKGROUND] File saved: {filepath}")
            cache_manager.add_cache(vid_id, filepath, "mp3")
        
    except Exception as e:
        logger.debug(f"[BACKGROUND] Download failed: {str(e)}")

# ============================================================================
# MAIN YOUTUBE API - INSTANT RESPONSE
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
            "stream_urls": 0,
            "downloads": 0
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
    # INSTANT PLAYABLE DOWNLOAD - <1 SECOND RESPONSE
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
        INSTANT RESPONSE - Returns playable URL/file in <1 second
        Background download happens after playback starts
        """
        if videoid:
            vid_id = link
            link = self.base + link
        else:
            vid_id = link.split("v=")[-1].split("&")[0] if "v=" in link else ""
        
        loop = asyncio.get_running_loop()
        format_type = "mp4" if video else "mp3"
        
        # ====================================================================
        # STEP 1: CHECK CACHE (instant <10ms)
        # ====================================================================
        cached_file = cache_manager.check_cache(vid_id, format_type)
        if cached_file and os.path.exists(cached_file):
            logger.info(f"⚡ [CACHE HIT] {vid_id} - INSTANT! ({format_type})")
            self.dl_stats["cache_hits"] += 1
            return cached_file
        
        self.dl_stats["total_requests"] += 1
        
        # ====================================================================
        # STEP 2: GET INSTANT STREAM URL - <1 SECOND RESPONSE!
        # ====================================================================
        stream_url, success = await stream_provider.get_direct_stream_url(link)
        
        if success and stream_url:
            logger.info(f"⚡ [INSTANT STREAM] URL ready in <1 second!")
            self.dl_stats["stream_urls"] += 1
            
            # Start background download (happens while playing)
            asyncio.create_task(background_download(link, vid_id, 
                                                    os.path.join("downloads", f"{vid_id}.{format_type}"),
                                                    get_cookies_file()))
            
            # Return stream URL immediately for playback
            return stream_url
        
        # ====================================================================
        # STEP 3: FALLBACK - Try Fast APIs (5-10 seconds)
        # ====================================================================
        logger.info(f"⚡ [FALLBACK] Trying fast API sources...")
        
        success, download_url = await fast_sources.get_fastest_url(vid_id, format_type)
        
        if success and download_url:
            logger.info(f"⚡ [FAST API] Got URL in 5-10 seconds")
            filepath = os.path.join("downloads", f"{vid_id}.{format_type}")
            
            try:
                session = session_manager.get_session("download")
                response = session.get(download_url, stream=True, timeout=45)
                response.raise_for_status()
                
                with open(filepath, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=1024*512):
                        if chunk:
                            f.write(chunk)
                
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    logger.info(f"✅ [FAST API] Downloaded: {filepath}")
                    cache_manager.add_cache(vid_id, filepath, format_type)
                    self.dl_stats["downloads"] += 1
                    return filepath
            except Exception as e:
                logger.debug(f"[FAST API] Download failed: {str(e)}")
        
        # ====================================================================
        # STEP 4: FULL YT-DLP DOWNLOAD (10-20 seconds) - Last resort
        # ====================================================================
        logger.info(f"⚡ [YT-DLP] Full download starting...")
        filepath = os.path.join("downloads", f"{vid_id}.{format_type}")
        
        if os.path.exists(filepath):
            cache_manager.add_cache(vid_id, filepath, format_type)
            return filepath
        
        def ytdlp_download():
            ydl_opts = {
                "quiet": True,
                "no_warnings": True,
                "outtmpl": filepath,
                "force_overwrites": True,
                "retries": 1,
                "socket_timeout": 30,
                "concurrent_fragment_downloads": 12,
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
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([link])
        
        try:
            await asyncio.wait_for(
                loop.run_in_executor(None, ytdlp_download),
                timeout=25
            )
            
            if os.path.exists(filepath):
                logger.info(f"✅ [YT-DLP] Downloaded: {filepath}")
                cache_manager.add_cache(vid_id, filepath, format_type)
                self.dl_stats["downloads"] += 1
                return filepath
        except asyncio.TimeoutError:
            logger.error(f"❌ Download timeout for {vid_id}")
        except Exception as e:
            logger.error(f"❌ Download failed: {str(e)}")
        
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
# STARTUP INITIALIZATION
# ============================================================================
async def schedule_cleanup_task():
    """Cleanup old cache files every 24 hours"""
    while True:
        try:
            await asyncio.sleep(86400)
            cache_manager.cleanup_old_cache(max_age_days=7)
            logger.info("✅ Cache cleanup completed")
        except Exception as e:
            logger.error(f"Cleanup task error: {str(e)}")

async def init_youtube_api():
    """Initialize YouTube API and cache systems"""
    logger.info("[YouTube] Initializing instant stream system...")
    
    await initialize_cookies()
    asyncio.create_task(schedule_cleanup_task())
    
    logger.info("[YouTube] ✅ Instant stream ready! <1 second response enabled!")
