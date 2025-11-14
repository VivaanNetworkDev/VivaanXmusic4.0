# Fixed Seek Function - Handles playback continuity and assistant staying in VC
# This version prevents the bot from leaving the VC and maintains proper stream state

from pyrogram import filters
from pyrogram.types import Message
import logging
import asyncio

from VIVAANXMUSIC import YouTube, app
from VIVAANXMUSIC.core.call import JARVIS
from VIVAANXMUSIC.misc import db
from VIVAANXMUSIC.utils import AdminRightsCheck, seconds_to_min
from VIVAANXMUSIC.utils.inline import close_markup
from config import BANNED_USERS

logger = logging.getLogger(__name__)


@app.on_message(
    filters.command(["seek", "cseek", "seekback", "cseekback"])
    & filters.group
    & ~BANNED_USERS
)
@AdminRightsCheck
async def seek_comm(cli, message: Message, _, chat_id):
    """
    Fixed seek command with proper stream handling
    Prevents bot from leaving VC and maintains playback state
    """
    try:
        # Validate command arguments
        if len(message.command) == 1:
            return await message.reply_text(_["admin_20"])
        
        query = message.text.split(None, 1)[1].strip()
        
        # Validate numeric input
        if not query.isnumeric():
            return await message.reply_text(_["admin_21"])
        
        # Check if music is currently playing
        playing = db.get(chat_id)
        if not playing:
            return await message.reply_text(_["queue_2"])
        
        # Extract current track data
        current_track = playing[0]
        duration_seconds = int(current_track.get("seconds", 0))
        
        # Validate duration
        if duration_seconds == 0:
            return await message.reply_text(_["admin_22"])
        
        # Extract file path and playback information
        file_path = current_track.get("file", "")
        if not file_path:
            return await message.reply_text(_["admin_22"])
        
        duration_played = int(current_track.get("played", 0))
        duration_to_skip = int(query)
        duration = current_track.get("dur", "")
        stream_type = current_track.get("streamtype", "audio")
        
        # Determine seek direction
        is_backward = message.command[0][-2] == "c"
        
        # Calculate target seek position
        if is_backward:
            # Backward seek: current - skip amount
            new_position = duration_played - duration_to_skip
            
            # Safety check: prevent seeking too far back (10 sec minimum)
            if new_position <= 10:
                return await message.reply_text(
                    text=_["admin_23"].format(
                        seconds_to_min(duration_played), 
                        duration
                    ),
                    reply_markup=close_markup(_),
                )
            to_seek = new_position + 1
        else:
            # Forward seek: current + skip amount
            new_position = duration_played + duration_to_skip
            remaining_duration = duration_seconds - new_position
            
            # Safety check: prevent seeking too far forward (10 sec minimum remaining)
            if remaining_duration <= 10:
                return await message.reply_text(
                    text=_["admin_23"].format(
                        seconds_to_min(duration_played), 
                        duration
                    ),
                    reply_markup=close_markup(_),
                )
            to_seek = new_position + 1
        
        # Send processing message
        mystic = await message.reply_text(_["admin_24"])
        
        # Handle video files (re-fetch if needed)
        if "vid_" in file_path:
            try:
                n, file_path = await YouTube.video(
                    current_track.get("vidid", ""), 
                    True
                )
                if n == 0:
                    return await mystic.edit_text(
                        _["admin_22"], 
                        reply_markup=close_markup(_)
                    )
            except Exception as e:
                logger.error(f"Failed to fetch video: {e}")
                return await mystic.edit_text(
                    _["admin_26"], 
                    reply_markup=close_markup(_)
                )
        
        # Check for speed-adjusted path (if available)
        speed_path = current_track.get("speed_path")
        if speed_path:
            file_path = speed_path
        
        # Handle index-based files
        if "index_" in file_path:
            file_path = current_track.get("vidid", file_path)
        
        # YouTube videos: limit seeking to 15 minutes (900 seconds)
        youtube_seek_limit = 900  # 15 minutes in seconds
        if stream_type == "youtube" and to_seek > youtube_seek_limit:
            return await mystic.edit_text(
                text=_["admin_28"].format(15),
                reply_markup=close_markup(_),
            )
        
        # CRITICAL FIX: Stop current stream BEFORE seeking, then restart
        # This prevents the stream from being orphaned or the bot leaving VC
        try:
            # Give a small delay to ensure clean state transition
            await asyncio.sleep(0.5)
            
            # Attempt to stop the current stream gracefully
            try:
                await JARVIS.stop_stream(chat_id)
            except Exception as stop_error:
                logger.warning(f"Error stopping stream before seek: {stop_error}")
            
            # Give time for the stream to fully stop
            await asyncio.sleep(0.5)
            
            # Now perform the seek operation with a fresh stream
            await JARVIS.seek_stream(
                chat_id,
                file_path,
                seconds_to_min(to_seek),
                duration,
                stream_type,
            )
            
        except Exception as e:
            logger.error(f"Seek operation failed: {e}")
            
            # Attempt to restart the stream if seek fails
            try:
                await JARVIS.seek_stream(
                    chat_id,
                    file_path,
                    seconds_to_min(duration_played),  # Reset to original position
                    duration,
                    stream_type,
                )
            except Exception as restart_error:
                logger.error(f"Failed to restart stream after seek failure: {restart_error}")
            
            return await mystic.edit_text(
                _["admin_26"], 
                reply_markup=close_markup(_)
            )
        
        # Update database with new playback position
        try:
            if is_backward:
                db[chat_id][0]["played"] -= duration_to_skip
            else:
                db[chat_id][0]["played"] += duration_to_skip
        except Exception as e:
            logger.warning(f"Failed to update playback position: {e}")
        
        # Send success message
        await mystic.edit_text(
            text=_["admin_25"].format(
                seconds_to_min(to_seek), 
                message.from_user.mention
            ),
            reply_markup=close_markup(_),
        )
        
    except Exception as e:
        logger.error(f"Seek command error: {e}")
        try:
            await message.reply_text(
                _["admin_26"],
                reply_markup=close_markup(_),
            )
        except:
            pass


# ALTERNATIVE: Stream-safe seek without stopping (if your JARVIS supports it)
# Use this if your seek_stream() is already async-safe
@app.on_message(
    filters.command(["sseek"])  # "smooth seek"
    & filters.group
    & ~BANNED_USERS
)
@AdminRightsCheck
async def smooth_seek_comm(cli, message: Message, _, chat_id):
    """
    Smooth seek variant - attempt seek without stopping stream
    Use if JARVIS.seek_stream() handles concurrent operations
    """
    try:
        if len(message.command) == 1:
            return await message.reply_text(_["admin_20"])
        
        query = message.text.split(None, 1)[1].strip()
        if not query.isnumeric():
            return await message.reply_text(_["admin_21"])
        
        playing = db.get(chat_id)
        if not playing:
            return await message.reply_text(_["queue_2"])
        
        current_track = playing[0]
        duration_seconds = int(current_track.get("seconds", 0))
        
        if duration_seconds == 0:
            return await message.reply_text(_["admin_22"])
        
        file_path = current_track.get("file", "")
        duration_played = int(current_track.get("played", 0))
        duration_to_skip = int(query)
        
        # Calculate seek position (forward only for smooth seek)
        new_position = duration_played + duration_to_skip
        remaining = duration_seconds - new_position
        
        if remaining <= 10:
            return await message.reply_text(
                text=_["admin_23"].format(
                    seconds_to_min(duration_played), 
                    current_track.get("dur", "")
                ),
                reply_markup=close_markup(_),
            )
        
        to_seek = new_position + 1
        
        # Check cache paths
        speed_path = current_track.get("speed_path")
        if speed_path:
            file_path = speed_path
        
        if "index_" in file_path:
            file_path = current_track.get("vidid", file_path)
        
        mystic = await message.reply_text(_["admin_24"])
        
        try:
            # Direct seek attempt (no stop)
            await JARVIS.seek_stream(
                chat_id,
                file_path,
                seconds_to_min(to_seek),
                current_track.get("dur", ""),
                current_track.get("streamtype", "audio"),
            )
            
            # Update DB
            db[chat_id][0]["played"] += duration_to_skip
            
            await mystic.edit_text(
                text=_["admin_25"].format(
                    seconds_to_min(to_seek), 
                    message.from_user.mention
                ),
                reply_markup=close_markup(_),
            )
        except Exception as e:
            logger.error(f"Smooth seek failed: {e}")
            await mystic.edit_text(
                _["admin_26"],
                reply_markup=close_markup(_)
            )
    
    except Exception as e:
        logger.error(f"Smooth seek command error: {e}")
