"""
Clean Forward Bot
Copies videos/documents from source channels to target without forward tags.
Auto-restart on crash via internal watchdog loop.
Multi-user support with MongoDB backend.
"""

import asyncio
import logging
import os
import re
import sys
import signal
import traceback
from typing import Optional, List, Dict, Any
from datetime import datetime

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, ChatAdminRequired, ChannelInvalid, PeerIdInvalid, UserNotParticipant
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
API_ID = int(os.environ.get("API_ID", ""))
API_HASH = os.environ.get("API_HASH", "")
MONGODB_URI = os.environ.get("MONGODB_URI", "")
DATABASE_NAME = os.environ.get("DATABASE_NAME", "clean_forward_bot")

MAX_RETRIES = 0  # 0 = restart forever
RESTART_DELAY = 5  # seconds between restart attempts

# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── MONGODB CONNECTION ────────────────────────────────────────────────────────

class MongoDBManager:
    """Manages MongoDB connections and data operations for multi-user support."""

    def __init__(self, uri: str, database_name: str):
        self.uri = uri
        self.database_name = database_name
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
        self.users_collection: Optional[AsyncIOMotorCollection] = None
        self.stats_collection: Optional[AsyncIOMotorCollection] = None

    async def connect(self):
        """Connect to MongoDB."""
        try:
            self.client = AsyncIOMotorClient(self.uri)
            self.db = self.client[self.database_name]
            self.users_collection = self.db["users"]
            self.stats_collection = self.db["stats"]
            
            # Create indexes
            await self.users_collection.create_index("user_id", unique=True)
            await self.stats_collection.create_index("user_id", unique=True)
            
            # Test connection
            await self.client.admin.command('ping')
            log.info("✅ Connected to MongoDB")
        except Exception as e:
            log.error(f"❌ MongoDB connection failed: {e}")
            raise

    async def disconnect(self):
        """Disconnect from MongoDB."""
        if self.client:
            self.client.close()
            log.info("MongoDB disconnected")

    async def get_user_config(self, user_id: int) -> Dict[str, Any]:
        """Get user configuration."""
        doc = await self.users_collection.find_one({"user_id": user_id})
        if doc:
            return doc
        # Create default config
        default_config = {
            "user_id": user_id,
            "sources": [],
            "target": None,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        await self.users_collection.insert_one(default_config)
        return default_config

    async def update_user_config(self, user_id: int, update_data: Dict[str, Any]):
        """Update user configuration."""
        update_data["updated_at"] = datetime.utcnow()
        await self.users_collection.update_one(
            {"user_id": user_id},
            {"$set": update_data}
        )

    async def add_source(self, user_id: int, channel_id: int) -> bool:
        """Add a source channel to user's list."""
        config = await self.get_user_config(user_id)
        if channel_id in config["sources"]:
            return False
        config["sources"].append(channel_id)
        await self.update_user_config(user_id, {"sources": config["sources"]})
        return True

    async def remove_source(self, user_id: int, channel_id: int) -> bool:
        """Remove a source channel from user's list."""
        config = await self.get_user_config(user_id)
        if channel_id not in config["sources"]:
            return False
        config["sources"].remove(channel_id)
        await self.update_user_config(user_id, {"sources": config["sources"]})
        return True

    async def set_target(self, user_id: int, channel_id: int):
        """Set target channel for user."""
        await self.update_user_config(user_id, {"target": channel_id})

    async def clear_target(self, user_id: int):
        """Clear target channel for user."""
        await self.update_user_config(user_id, {"target": None})

    async def get_sources(self, user_id: int) -> List[int]:
        """Get user's source channels."""
        config = await self.get_user_config(user_id)
        return config.get("sources", [])

    async def get_target(self, user_id: int) -> Optional[int]:
        """Get user's target channel."""
        config = await self.get_user_config(user_id)
        return config.get("target")

    async def record_copy(self, user_id: int, from_chat: int, msg_id: int, to_chat: int, success: bool):
        """Record a copy operation for statistics."""
        await self.stats_collection.update_one(
            {"user_id": user_id},
            {
                "$inc": {
                    "total_copies": 1,
                    "successful_copies": 1 if success else 0,
                    "failed_copies": 0 if success else 1
                },
                "$set": {"last_copy_at": datetime.utcnow()}
            },
            upsert=True
        )

# Global MongoDB manager
db_manager = MongoDBManager(MONGODB_URI, DATABASE_NAME)

# ─── PERMISSION CACHE ─────────────────────────────────────────────────────────

class PermissionCache:
    """Cache bot admin status in channels to avoid repeated checks."""
    
    def __init__(self):
        self.admin_in: Dict[int, bool] = {}  # {chat_id: is_admin}
    
    def is_admin(self, chat_id: int) -> Optional[bool]:
        """Get cached admin status, or None if not cached."""
        return self.admin_in.get(chat_id)
    
    def set_admin(self, chat_id: int, is_admin: bool):
        """Cache admin status for a chat."""
        self.admin_in[chat_id] = is_admin
        status = "✅ admin" if is_admin else "❌ not admin"
        log.info(f"Permission cache: {chat_id} → {status}")
    
    def clear(self):
        """Clear all cached permissions (on restart)."""
        self.admin_in.clear()
        log.info("Permission cache cleared")

perm_cache = PermissionCache()

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def parse_channel_id(text: str) -> Optional[int]:
    """Parse a channel ID string like -100xxxxxxxxxx"""
    text = text.strip()
    try:
        cid = int(text)
        if str(cid).startswith("-100"):
            return cid
    except ValueError:
        pass
    return None

async def verify_admin_in_chat(client: Client, chat_id: int) -> bool:
    """
    Verify bot is actually an admin in the given chat.
    Returns True if admin, False otherwise.
    Caches the result.
    """
    # Check cache first
    cached = perm_cache.is_admin(chat_id)
    if cached is not None:
        return cached
    
    try:
        me = await client.get_me()
        member = await client.get_chat_member(chat_id, me.id)
        is_admin = member.is_admin or member.is_owner
        perm_cache.set_admin(chat_id, is_admin)
        return is_admin
    except UserNotParticipant:
        log.warning(f"Bot is not a member of {chat_id}")
        perm_cache.set_admin(chat_id, False)
        return False
    except (ChatAdminRequired, ChannelInvalid, PeerIdInvalid) as e:
        log.error(f"Cannot verify admin status in {chat_id}: {e}")
        perm_cache.set_admin(chat_id, False)
        return False
    except Exception as e:
        log.error(f"Unexpected error verifying admin in {chat_id}: {e}")
        return False

async def safe_copy(client: Client, from_chat: int, msg_id: int, to_chat: int) -> bool:
    """Copy a single message without forward tag, with retry on flood wait."""
    
    # Verify admin status before attempting copy
    if not await verify_admin_in_chat(client, to_chat):
        log.error(f"Bot is not an admin in target chat {to_chat}")
        return False
    
    for attempt in range(3):
        try:
            await client.copy_message(
                chat_id=to_chat,
                from_chat_id=from_chat,
                message_id=msg_id
            )
            return True
        except FloodWait as e:
            log.warning(f"FloodWait {e.value}s — sleeping…")
            await asyncio.sleep(e.value + 1)
        except (ChatAdminRequired, ChannelInvalid, PeerIdInvalid) as e:
            log.error(f"Permission/channel error: {e}")
            perm_cache.set_admin(to_chat, False)
            return False
        except Exception as e:
            log.error(f"Attempt {attempt + 1} failed: {e}")
            await asyncio.sleep(2)
    return False

# ─── BOT FACTORY ──────────────────────────────────────────────────────────────

def build_app() -> Client:
    """Create a fresh Client instance (needed on each restart)."""
    return Client(
        "clean_forward_bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN
    )

# ─── COMMANDS ─────────────────────────────────────────────────────────────────

def register_handlers(app: Client):
    """Register all message handlers on the given Client."""

    @app.on_message(filters.command("start") & filters.private)
    async def cmd_start(client: Client, msg: Message):
        await msg.reply(
            "👋 **Welcome to the Clean Forward Bot**\n\n"
            "This bot copies videos and documents from source channels to your target channel "
            "— without any forwarding headers.\n\n"
            "**Commands:**\n"
            "`/addsource -100xxxxxxxxxx` — Add a source channel\n"
            "`/removesource -100xxxxxxxxxx` — Remove a source channel\n"
            "`/settarget -100xxxxxxxxxx` — Set the target channel\n"
            "`/cleartarget` — Clear the target channel\n"
            "`/list` — View current settings\n"
            "`/myforwards` — Alias for /list\n"
            "`/check https://t.me/c/xxxx/yyy` — Copy one specific message\n"
            "`/stats` — View your statistics\n\n"
            "⚙️ Make sure the bot is an **admin** in all channels."
        )

    @app.on_message(filters.command("addsource") & filters.private)
    async def cmd_addsource(client: Client, msg: Message):
        user_id = msg.from_user.id
        parts = msg.text.split()
        if len(parts) < 2:
            return await msg.reply("Usage: `/addsource -100xxxxxxxxxx`")
        cid = parse_channel_id(parts[1])
        if cid is None:
            return await msg.reply("❌ Invalid channel ID. Must start with `-100`.")
        
        added = await db_manager.add_source(user_id, cid)
        if not added:
            return await msg.reply("⚠️ Already in sources list.")
        
        await msg.reply(f"✅ Added source: `{cid}`")
        log.info(f"User {user_id}: Source added {cid}")

    @app.on_message(filters.command(["removesource", "remove"]) & filters.private)
    async def cmd_removesource(client: Client, msg: Message):
        user_id = msg.from_user.id
        parts = msg.text.split()
        if len(parts) < 2:
            return await msg.reply("Usage: `/removesource -100xxxxxxxxxx`")
        cid = parse_channel_id(parts[1])
        if cid is None:
            return await msg.reply("❌ Invalid channel ID.")
        
        removed = await db_manager.remove_source(user_id, cid)
        if not removed:
            return await msg.reply("⚠️ Not in sources list.")
        
        await msg.reply(f"✅ Removed source: `{cid}`")
        log.info(f"User {user_id}: Source removed {cid}")

    @app.on_message(filters.command("settarget") & filters.private)
    async def cmd_settarget(client: Client, msg: Message):
        user_id = msg.from_user.id
        parts = msg.text.split()
        if len(parts) < 2:
            return await msg.reply("Usage: `/settarget -100xxxxxxxxxx`")
        cid = parse_channel_id(parts[1])
        if cid is None:
            return await msg.reply("❌ Invalid channel ID. Must start with `-100`.")
        
        # Verify bot is admin in target before setting
        is_admin = await verify_admin_in_chat(client, cid)
        if not is_admin:
            return await msg.reply(
                f"❌ Bot is not an admin in `{cid}`.\n\n"
                "**Fix:** Add the bot as an admin to the target channel, then try again."
            )
        
        await db_manager.set_target(user_id, cid)
        await msg.reply(f"✅ Target set to: `{cid}`")
        log.info(f"User {user_id}: Target set to {cid}")

    @app.on_message(filters.command("cleartarget") & filters.private)
    async def cmd_cleartarget(client: Client, msg: Message):
        user_id = msg.from_user.id
        await db_manager.clear_target(user_id)
        await msg.reply("✅ Target channel cleared.")
        log.info(f"User {user_id}: Target cleared")

    @app.on_message(filters.command(["list", "myforwards"]) & filters.private)
    async def cmd_list(client: Client, msg: Message):
        user_id = msg.from_user.id
        sources = await db_manager.get_sources(user_id)
        target = await db_manager.get_target(user_id)
        
        src_txt = "\n".join(f"  • `{s}`" for s in sources) if sources else "  _None_"
        tgt_txt = f"`{target}`" if target else "_Not set_"
        
        await msg.reply(
            "📋 **Current Settings**\n\n"
            f"**Sources:**\n{src_txt}\n\n"
            f"**Target:** {tgt_txt}"
        )

    @app.on_message(filters.command("check") & filters.private)
    async def cmd_check(client: Client, msg: Message):
        """Copy a specific message by its t.me/c/ link."""
        user_id = msg.from_user.id
        parts = msg.text.split()
        if len(parts) < 2:
            return await msg.reply("Usage: `/check https://t.me/c/xxxx/yyy`")

        url = parts[1]
        m = re.match(r"https://t\.me/c/(\d+)/(\d+)", url)
        if not m:
            return await msg.reply("❌ Unsupported link format. Use `https://t.me/c/xxxx/yyy`")

        chat_id = int("-100" + m.group(1))
        msg_id = int(m.group(2))
        target = await db_manager.get_target(user_id)

        if not target:
            return await msg.reply("❌ No target set. Use `/settarget` first.")

        status = await msg.reply("⏳ Copying message…")
        ok = await safe_copy(client, chat_id, msg_id, target)
        await db_manager.record_copy(user_id, chat_id, msg_id, target, ok)
        
        if ok:
            await status.edit("✅ Message copied successfully!")
            log.info(f"User {user_id}: Message {msg_id} copied from {chat_id} to {target}")
        else:
            await status.edit("❌ Failed to copy. Check bot permissions and logs.")
            log.error(f"User {user_id}: Failed to copy message {msg_id} from {chat_id}")

    @app.on_message(filters.command("stats") & filters.private)
    async def cmd_stats(client: Client, msg: Message):
        """Show user statistics."""
        user_id = msg.from_user.id
        stats = await db_manager.stats_collection.find_one({"user_id": user_id})
        
        if not stats:
            return await msg.reply("📊 No statistics yet. Start copying messages!")
        
        total = stats.get("total_copies", 0)
        success = stats.get("successful_copies", 0)
        failed = stats.get("failed_copies", 0)
        
        success_rate = (success / total * 100) if total > 0 else 0
        
        await msg.reply(
            "📊 **Your Statistics**\n\n"
            f"Total Copies: `{total}`\n"
            f"Successful: `{success}`\n"
            f"Failed: `{failed}`\n"
            f"Success Rate: `{success_rate:.1f}%`"
        )

    # ─── AUTO-FORWARD HANDLER (PER USER) ─────────────────────────────────────

    @app.on_message(filters.channel)
    async def handle_channel_post(client: Client, msg: Message):
        """Triggered for every new channel post the bot can see."""
        chat_id = msg.chat.id
        
        # Find all users who have this channel as a source
        all_users = await db_manager.users_collection.find(
            {"sources": {"$in": [chat_id]}}
        ).to_list(None)
        
        if not all_users:
            return
        
        if not (msg.video or msg.document):
            return

        # Process for each user who follows this source
        for user_config in all_users:
            user_id = user_config["user_id"]
            target = user_config.get("target")
            
            if not target:
                log.warning(f"User {user_id}: Received message from source {chat_id} but no target is set.")
                continue

            log.info(f"User {user_id}: Copying msg {msg.id} from {chat_id} → {target}")
            ok = await safe_copy(client, chat_id, msg.id, target)
            await db_manager.record_copy(user_id, chat_id, msg.id, target, ok)
            
            if not ok:
                log.error(f"User {user_id}: Failed to copy msg {msg.id} from {chat_id}")

# ─── AUTO-RESTART WATCHDOG ────────────────────────────────────────────────────

_stop_requested = False

def _handle_sigterm(signum, frame):
    """On SIGTERM / Ctrl-C, stop the restart loop gracefully."""
    global _stop_requested
    log.info("Stop signal received — shutting down permanently.")
    _stop_requested = True
    sys.exit(0)

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

async def run_once() -> None:
    """Start the bot and run until it crashes or stops."""
    # Clear permission cache on each restart
    perm_cache.clear()
    
    await db_manager.connect()
    app = build_app()
    register_handlers(app)
    await app.start()
    log.info("✅ Bot is running.")
    try:
        await asyncio.Event().wait()  # block until an exception kills the task
    finally:
        await db_manager.disconnect()

def run_with_autorestart() -> None:
    """Keep restarting the bot forever (unless SIGTERM/SIGINT received)."""
    attempt = 0
    while not _stop_requested:
        attempt += 1
        log.info(f"▶️  Starting bot (attempt #{attempt})…")
        try:
            asyncio.run(run_once())
        except (KeyboardInterrupt, SystemExit):
            log.info("Clean exit — not restarting.")
            break
        except Exception:
            log.error("💥 Bot crashed!\n" + traceback.format_exc())

        if _stop_requested:
            break

        if MAX_RETRIES and attempt >= MAX_RETRIES:
            log.error(f"Reached max restart limit ({MAX_RETRIES}). Exiting.")
            break

        log.info(f"🔄 Restarting in {RESTART_DELAY} seconds…")
        try:
            import time
            time.sleep(RESTART_DELAY)
        except (KeyboardInterrupt, SystemExit):
            break

    log.info("🛑 Bot stopped.")

# ─── ENTRY POINT ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not BOT_TOKEN or not API_ID or not API_HASH:
        log.error("❌ Missing required environment variables: BOT_TOKEN, API_ID, API_HASH")
        sys.exit(1)
    
    log.info("🤖 Clean Forward Bot starting with auto-restart watchdog (MongoDB backend)…")
    run_with_autorestart()
