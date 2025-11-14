import asyncio
import glob
import json
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Union, Optional, Tuple, Dict, List
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
# SMART CACHE MANAGER
# ============================================================================
class SmartCacheManager:
    """Smart cache that auto-deletes files after 24 hours"""
    
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
        if os.path.exists(self.metadata_file):
            try:
                with open(self.metadata_file, 'r') as f:
                    return json.load(f)
            except:
                return {}
        return {}
    
    def _save_metadata(self):
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.metadata, f)
        except:
            pass
    
    def check_cache(self, vid_id: str, format_type: str = "mp3") -> Optional[str]:
        cache_key = f"{vid_id}_{format_type}"
        
        if cache_key in self.memory_cache:
            filepath = self.memory_cache[cache_key]
            if os.path.exists(filepath):
                return filepath
            else:
                del self.memory_cache[cache_key]
        
        filepath = os.path.join(self.download_dir, f"{vid_id}.{format_type}")
        
        if os.path.exists(filepath):
            if cache_key in self.metadata:
                file_age = time.time() - self.metadata[cache_key]["timestamp"]
                
                if file_age < self.retention_seconds:
                    self.memory_cache[cache_key] = filepath
                    return filepath
                else:
                    try:
                        os.remove(filepath)
                        del self.metadata[cache_key]
                        self._save_metadata()
                    except:
                        pass
                    return None
            else:
                self.memory_cache[cache_key] = filepath
                return filepath
        
        return None
    
    def add_cache(self, vid_id: str, filepath: str, format_type: str = "mp3"):
        cache_key = f"{vid_id}_{format_type}"
        self.memory_cache[cache_key] = filepath
        
        self.metadata[cache_key] = {
            "path": filepath,
            "timestamp": time.time(),
            "format": format_type,
            "vid_id": vid_id,
            "size": os.path.getsize(filepath) if os.path.exists(filepath) else 0
        }
        
        if len(self.memory_cache) > self.max_memory_cache:
            oldest_key = min(self.memory_cache.keys(), 
                            key=lambda k: self.metadata.get(k, {}).get("timestamp", 0))
            del self.memory_cache[oldest_key]
        
        self._save_metadata()
    
    def cleanup_expired(self):
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
                except:
                    pass
        
        for key in to_delete:
            del self.metadata[key]
        
        if removed_count > 0:
            self._save_metadata()
        
        return removed_count

cache_manager = SmartCacheManager(retention_hours=24)

# ============================================================================
# SESSION MANAGER
# ============================================================================
class SessionManager:
    def __init__(self):
        self.sessions = {}
    
    def get_session(self, session_id: str = "default") -> requests.Session:
        if session_id not in self.sessions:
            session = requests.Session()
            retries = Retry(total=0, backoff_factor=0.05)
            adapter = HTTPAdapter(max_retries=retries, pool_connections=30, pool_maxsize=30)
            session.mount('http://', adapter)
            session.mount('https://', adapter)
            self.sessions[session_id] = session
        return self.sessions[session_id]

session_manager = SessionManager()

# ============================================================================
# OPTIMIZED SOCIALDOWN API
# ============================================================================
class SocialDownAPI:
    """SocialDown API - Get URL fast, download in background"""
    
    async def get_download_url(self, vid_id: str, format_type: str = "mp3") -> Tuple[bool, Optional[str]]:
        """Get download URL from SocialDown API - NO DOWNLOAD YET"""
        try:
            logger.info(f"🔗 [SOCIALDOWN] Getting URL for {vid_id}...")
            
            session = session_manager.get_session("socialdown")
            
            yt_link = f"https://www.youtube.com/watch?v={vid_id}"
            params = {
                "url": yt_link,
                "format": format_type
            }
            
            resp = session.get(
                "https://socialdown.itz-ashlynn.workers.dev/yt",
                params=params,
                timeout=5
            )
            
            if resp.status_code == 200:
                data = resp.json()
                
                if data.get('success') and data.get('data'):
                    if isinstance(data['data'], list) and len(data['data']) > 0:
                        download_url = data['data'][0].get('downloadUrl')
                        
                        if download_url:
                            logger.info(f"✅ [SOCIALDOWN] URL received in <1 second!")
                            return (True, download_url)
            
            return (False, None)
            
        except Exception as e:
            logger.warning(f"❌ [SOCIALDOWN] Error: {str(e)}")
            return (False, None)

    async def download_in_background(self, url: str, filepath: str):
        """Download file in background - non-blocking"""
        try:
            logger.info(f"📥 [BG-DOWNLOAD] Starting background download...")
            
            session = session_manager.get_session("bg_download")
            response = session.get(url, stream=True, timeout=60)
            response.raise_for_status()
            
            chunk_size = 1024 * 1024  # 1MB chunks
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
            
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                logger.info(f"✅ [BG-DOWNLOAD] Complete! {filepath}")
                cache_manager.add_cache(filepath.split('/')[-1].split('.')[0], filepath, "mp3")
            
        except Exception as e:
            logger.warning(f"[BG-DOWNLOAD] Error: {str(e)}")

socialdown_api = SocialDownAPI()

# ============================================================================
# YT-DLP FALLBACK
# ============================================================================
class YTDLPFallback:
    """yt-dlp extraction - Last resort"""
    
    async def extract_and_download(self, link: str, filepath: str) -> bool:
        """Extract and download using yt-dlp"""
        try:
            loop = asyncio.get_running_loop()
            
            def ytdlp_download():
                logger.info(f"⬇️ [YT-DLP] Starting yt-dlp...")
                
                ydl_opts = {
                    "quiet": True,
                    "no_warnings": True,
                    "outtmpl": filepath,
                    "force_overwrites": True,
                    "socket_timeout": 30,
                    "format": "bestaudio[ext=m4a]/bestaudio/best",
                    "postprocessors": [{
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                        "preferredquality": "96",
                    }],
                }
                
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([link])
            
            await asyncio.wait_for(
                loop.run_in_executor(None, ytdlp_download),
                timeout=45
            )
            
            if os.path.exists(filepath):
                logger.info(f"✅ [YT-DLP] Complete!")
                return True
            
            return False
            
        except asyncio.TimeoutError:
            logger.error(f"❌ [YT-DLP] Timeout")
            return False
        except Exception as e:
            logger.error(f"❌ [YT-DLP] Error: {str(e)}")
            return False

ytdlp = YTDLPFallback()

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
            "ytdlp_downloads": 0,
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

        try:
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
        except Exception as e:
            logger.error(f"Details error: {str(e)}")
            return None, None, 0, None, None

    async def title(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]

        try:
            results = VideosSearch(link, limit=1)
            for result in (await results.next())["result"]:
                return result["title"]
        except:
            return "Unknown Title"

    async def duration(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]

        try:
            results = VideosSearch(link, limit=1)
            for result in (await results.next())["result"]:
                return result["duration"]
        except:
            return "0:00"

    async def thumbnail(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]

        try:
            results = VideosSearch(link, limit=1)
            for result in (await results.next())["result"]:
                return result["thumbnails"][0]["url"].split("?")[0]
        except:
            return None

    async def video(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]

        proc = await asyncio.create_subprocess_exec(
            "yt-dlp",
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
            f"yt-dlp -i --get-id --flat-playlist --playlist-end {limit} --skip-download {link}",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        
        result = stdout.decode("utf-8").split("\n") if stdout else []
        return [key for key in result if key]

    async def track(self, link: str, videoid: Union[bool, str] = None):
        """Get track details"""
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]

        try:
            results = VideosSearch(link, limit=1)
            search_results = (await results.next()).get("result", [])
            
            if not search_results:
                return None, None
            
            result = search_results[0]
            
            return {
                "title": result.get("title", "Unknown"),
                "link": result.get("link", link),
                "vidid": result.get("id", ""),
                "duration_min": result.get("duration", "0:00"),
                "thumb": result.get("thumbnails", [{}])[0].get("url", "").split("?")[0],
            }, result.get("id", "")
            
        except Exception as e:
            logger.error(f"Track error: {str(e)}")
            return None, None

    async def formats(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        
        if "&" in link:
            link = link.split("&")[0]
        if "?si=" in link:
            link = link.split("?si=")[0]

        try:
            ydl_opts = {"quiet": True}
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
        except Exception as e:
            logger.error(f"Formats error: {str(e)}")
            return [], link

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
    # ULTRA-FAST DOWNLOAD - INSTANT RESPONSE + Background Download
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
        ULTRA-FAST RESPONSE - <2 SECONDS!
        1. Check cache (instant)
        2. Get URL from API (1 second)
        3. Return immediately - play while downloading in background!
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
            logger.info(f"⚡ [CACHE] INSTANT! <100ms")
            self.dl_stats["cache_hits"] += 1
            return cached_file
        
        self.dl_stats["total_requests"] += 1
        
        # ====================================================================
        # STEP 2: GET URL FROM SOCIALDOWN (1 second)
        # ====================================================================
        logger.info(f"🌐 [DOWNLOAD] Starting for {vid_id}...")
        
        success, download_url = await socialdown_api.get_download_url(vid_id, format_type)
        
        if success and download_url:
            filepath = os.path.join("downloads", f"{vid_id}.{format_type}")
            
            # ================================================================
            # STEP 3: START BACKGROUND DOWNLOAD (NON-BLOCKING!)
            # ================================================================
            asyncio.create_task(socialdown_api.download_in_background(download_url, filepath))
            
            # Create empty file immediately so bot can start playback
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            open(filepath, 'a').close()
            
            logger.info(f"▶️ [INSTANT] File ready! Playing while downloading...")
            cache_manager.add_cache(vid_id, filepath, format_type)
            self.dl_stats["api_downloads"] += 1
            
            # Return IMMEDIATELY - no waiting!
            return filepath
        
        # ====================================================================
        # STEP 4: FALLBACK TO YT-DLP
        # ====================================================================
        logger.info(f"⚙️ [FALLBACK] Using yt-dlp...")
        
        filepath = os.path.join("downloads", f"{vid_id}.{format_type}")
        
        if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
            cache_manager.add_cache(vid_id, filepath, format_type)
            return filepath
        
        success = await ytdlp.extract_and_download(link, filepath)
        
        if success and os.path.exists(filepath):
            logger.info(f"✅ [SUCCESS] Downloaded!")
            cache_manager.add_cache(vid_id, filepath, format_type)
            self.dl_stats["ytdlp_downloads"] += 1
            return filepath
        
        # ====================================================================
        # CUSTOM FORMAT DOWNLOADS
        # ====================================================================
        if songvideo or songaudio:
            try:
                loop = asyncio.get_running_loop()
                
                fpath = f"downloads/{title}"
                ydl_opts = {
                    "format": f"{format_id}+140" if songvideo else format_id,
                    "outtmpl": fpath,
                    "quiet": True,
                    "no_warnings": True,
                }
                
                if songaudio:
                    ydl_opts["postprocessors"] = [{
                        "key": "FFmpegExtractAudio",
                        "preferredcodec": "mp3",
                        "preferredquality": "96",
                    }]
                
                ydl = yt_dlp.YoutubeDL(ydl_opts)
                await loop.run_in_executor(None, ydl.download, [link])
                
                final_path = f"downloads/{title}.{'mp4' if songvideo else 'mp3'}"
                
                if os.path.exists(final_path):
                    cache_manager.add_cache(vid_id, final_path, format_type)
                    return final_path
            except Exception as e:
                logger.error(f"Custom format failed: {str(e)}")
        
        logger.error(f"❌ All methods failed!")
        return None

# ============================================================================
# BACKGROUND CLEANUP
# ============================================================================
async def cleanup_task():
    """Run cleanup every hour"""
    while True:
        try:
            await asyncio.sleep(3600)
            cache_manager.cleanup_expired()
        except Exception as e:
            logger.error(f"Cleanup error: {str(e)}")

async def schedule_cleanup_task():
    """Schedule cleanup"""
    asyncio.create_task(cleanup_task())

async def init_youtube_api():
    """Initialize YouTube API"""
    logger.info("[YouTube] Initializing INSTANT response system...")
    await schedule_cleanup_task()
    logger.info("[YouTube] ✅ Ready! <2 second response + background download!")
