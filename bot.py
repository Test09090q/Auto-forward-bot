"""
Clean Forward Bot
Copies videos/documents from source channels to target without forward tags.
Auto-restart on crash via internal watchdog loop.
Multi-user support with MongoDB backend.

Fix: PeerIdInvalid after restart — peers are now resolved/pre-warmed on startup.
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
from pyrogram.errors import FloodWait, ChatAdminRequired, ChannelInvalid, PeerIdInvalid
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BOT_TOKEN   = os.environ.get("BOT_TOKEN", "")
API_ID      = int(os.environ.get("API_ID", "0"))
API_HASH    = os.environ.get("API_HASH", "")
MONGODB_URI = os.environ.get("MONGODB_URI", "")
DATABASE_NAME = os.environ.get("DATABASE_NAME", "clean_forward_bot")

MAX_RETRIES   = 0   # 0 = restart forever
RESTART_DELAY = 5   # seconds between restart attempts

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

# ─── MONGODB ──────────────────────────────────────────────────────────────────

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
        try:
            self.client = AsyncIOMotorClient(self.uri)
            self.db = self.client[self.database_name]
            self.users_collection = self.db["users"]
            self.stats_collection = self.db["stats"]
            await self.users_collection.create_index("user_id", unique=True)
            await self.stats_collection.create_index("user_id", unique=True)
            await self.client.admin.command("ping")
            log.info("✅ Connected to MongoDB")
        except Exception as e:
            log.error(f"❌ MongoDB connection failed: {e}")
            raise

    async def disconnect(self):
        if self.client:
            self.client.close()
            log.info("MongoDB disconnected")

    async def get_user_config(self, user_id: int) -> Dict[str, Any]:
        doc = await self.users_collection.find_one({"user_id": user_id})
        if doc:
            return doc
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
        update_data["updated_at"] = datetime.utcnow()
        await self.users_collection.update_one(
            {"user_id": user_id},
            {"$set": update_data}
        )

    async def add_source(self, user_id: int, channel_id: int) -> bool:
        config = await self.get_user_config(user_id)
        if channel_id in config["sources"]:
            return False
        config["sources"].append(channel_id)
        await self.update_user_config(user_id, {"sources": config["sources"]})
        return True

    async def remove_source(self, user_id: int, channel_id: int) -> bool:
        config = await self.get_user_config(user_id)
        if channel_id not in config["sources"]:
            return False
        config["sources"].remove(channel_id)
        await self.update_user_config(user_id, {"sources": config["sources"]})
        return True

    async def set_target(self, user_id: int, channel_id: int):
        await self.update_user_config(user_id, {"target": channel_id})

    async def clear_target(self, user_id: int):
        await self.update_user_config(user_id, {"target": None})

    async def get_sources(self, user_id: int) -> List[int]:
        config = await self.get_user_config(user_id)
        return config.get("sources", [])

    async def get_target(self, user_id: int) -> Optional[int]:
        config = await self.get_user_config(user_id)
        return config.get("target")

    async def record_copy(self, user_id: int, from_chat: int, msg_id: int, to_chat: int, success: bool):
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

    async def get_all_unique_peers(self) -> List[int]:
        """Return every unique source + target channel ID stored in DB."""
        all_users = await self.users_collection.find({}).to_list(None)
        seen: set = set()
        for user in all_users:
            for cid in user.get("sources", []):
                seen.add(cid)
            if t := user.get("target"):
                seen.add(t)
        return list(seen)


# Global MongoDB manager
db_manager = MongoDBManager(MONGODB_URI, DATABASE_NAME)

# ─── PEER RESOLUTION ──────────────────────────────────────────────────────────

from pyrogram.raw import functions as raw_functions
from pyrogram.raw import types as raw_types

async def resolve_peer_safe(client: Client, chat_id: int) -> bool:
    """
    Force Pyrogram to cache a channel peer so copy_message never gets
    PeerIdInvalid — even after a fresh restart where the session file
    has no stored peers.

    Strategy (in order):
      1. client.get_messages() — fetches 1 message, forces peer resolution
         for channels the bot is already a member/admin of.
      2. Direct raw API call InputChannel — last-resort populate of cache.

    Returns True when the peer is now cached, False when genuinely inaccessible.
    """
    # ── Method 1: get_messages (works for channels bot is member/admin of) ──
    try:
        await client.get_messages(chat_id, 1)
        log.debug(f"Peer {chat_id} resolved via get_messages.")
        return True
    except FloodWait as e:
        log.warning(f"FloodWait {e.value}s resolving {chat_id} — waiting…")
        await asyncio.sleep(e.value + 1)
    except (ChannelInvalid, PeerIdInvalid):
        pass   # fall through to method 2
    except Exception:
        pass   # fall through to method 2

    # ── Method 2: raw ResolveUsername / InputPeerChannel via invoke ─────────
    # Strip -100 prefix to get the bare channel id
    try:
        bare_id = int(str(chat_id).replace("-100", ""))
        # Build InputPeerChannel with access_hash=0 — Telegram will reject
        # it but Pyrogram will first try to resolve from its peer cache;
        # we use GetFullChannel so Telegram tells us the real access hash.
        from pyrogram.raw.functions.channels import GetFullChannel
        from pyrogram.raw.types import InputChannel
        await client.invoke(GetFullChannel(channel=InputChannel(
            channel_id=bare_id,
            access_hash=0
        )))
        log.debug(f"Peer {chat_id} resolved via raw GetFullChannel.")
        return True
    except PeerIdInvalid:
        # access_hash=0 rejected — try fetching dialogs to populate cache
        pass
    except Exception:
        pass

    # ── Method 3: walk dialogs until we find the channel ────────────────────
    try:
        async for dialog in client.get_dialogs():
            if dialog.chat and dialog.chat.id == chat_id:
                log.debug(f"Peer {chat_id} resolved via get_dialogs walk.")
                return True
        log.error(f"Peer {chat_id} not found in dialogs — bot may not be a member.")
        return False
    except FloodWait as e:
        log.warning(f"FloodWait {e.value}s during dialog walk for {chat_id}")
        await asyncio.sleep(e.value + 1)
        return False
    except Exception as e:
        log.error(f"All resolution methods failed for peer {chat_id}: {e}")
        return False


async def prewarm_peers(client: Client):
    """
    Called once right after app.start().
    Resolves every channel stored in MongoDB so Pyrogram caches them —
    prevents PeerIdInvalid on the first forwarded message after a restart.
    """
    log.info("🔥 Pre-warming peer cache…")
    peers = await db_manager.get_all_unique_peers()
    if not peers:
        log.info("   No peers to warm up.")
        return

    ok_count = 0
    for cid in peers:
        ok = await resolve_peer_safe(client, cid)
        log.info(f"   Peer {cid}: {'✅' if ok else '❌'}")
        if ok:
            ok_count += 1

    log.info(f"🔥 Pre-warm complete — {ok_count}/{len(peers)} peers resolved.")

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


async def safe_copy(client: Client, from_chat: int, msg_id: int, to_chat: int) -> bool:
    """
    Copy a single message without the forward tag.
    Resolves both peers first so PeerIdInvalid never surfaces after a restart.
    """
    # ── Resolve source peer ──────────────────────────────────────────────────
    if not await resolve_peer_safe(client, from_chat):
        log.error(f"Cannot resolve source peer {from_chat} — skipping copy.")
        return False

    # ── Resolve target peer ──────────────────────────────────────────────────
    if not await resolve_peer_safe(client, to_chat):
        log.error(f"Cannot resolve target peer {to_chat} — skipping copy.")
        return False

    # ── Attempt copy (up to 3 times) ─────────────────────────────────────────
    for attempt in range(1, 4):
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
            log.error(f"Permission/channel error on attempt {attempt}: {e}")
            return False
        except Exception as e:
            log.error(f"Attempt {attempt} failed: {e}")
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

# ─── COMMAND HANDLERS ─────────────────────────────────────────────────────────

def register_handlers(app: Client):
    """Register all message handlers on the given Client."""

    # ── /start ────────────────────────────────────────────────────────────────
    @app.on_message(filters.command("start") & filters.private)
    async def cmd_start(client: Client, msg: Message):
        await msg.reply(
            "👋 **Welome to the Clean Forward Bot**\n\n"
            "This bot copies videos and documents from source channels to your "
            "target channel — without any forwarding headers.\n\n"
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

    # ── /addsource ────────────────────────────────────────────────────────────
    @app.on_message(filters.command("addsource") & filters.private)
    async def cmd_addsource(client: Client, msg: Message):
        user_id = msg.from_user.id
        parts = msg.text.split()
        if len(parts) < 2:
            return await msg.reply("Usage: `/addsource -100xxxxxxxxxx`")
        cid = parse_channel_id(parts[1])
        if cid is None:
            return await msg.reply("❌ Invalid channel ID. Must start with `-100`.")

        # Try to resolve the peer immediately so we know the bot can see it
        status_msg = await msg.reply("⏳ Verifying channel access…")
        if not await resolve_peer_safe(client, cid):
            return await status_msg.edit(
                "❌ Cannot access that channel.\n"
                "Make sure the bot is an **admin** there, then try again."
            )

        added = await db_manager.add_source(user_id, cid)
        if not added:
            return await status_msg.edit("⚠️ Already in sources list.")

        await status_msg.edit(f"✅ Added source: `{cid}`")
        log.info(f"User {user_id}: Source added {cid}")

    # ── /removesource ─────────────────────────────────────────────────────────
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

    # ── /settarget ────────────────────────────────────────────────────────────
    @app.on_message(filters.command("settarget") & filters.private)
    async def cmd_settarget(client: Client, msg: Message):
        user_id = msg.from_user.id
        parts = msg.text.split()
        if len(parts) < 2:
            return await msg.reply("Usage: `/settarget -100xxxxxxxxxx`")
        cid = parse_channel_id(parts[1])
        if cid is None:
            return await msg.reply("❌ Invalid channel ID. Must start with `-100`.")

        # Verify access before saving
        status_msg = await msg.reply("⏳ Verifying channel access…")
        if not await resolve_peer_safe(client, cid):
            return await status_msg.edit(
                "❌ Cannot access that channel.\n"
                "Make sure the bot is an **admin** there, then try again."
            )

        await db_manager.set_target(user_id, cid)
        await status_msg.edit(f"✅ Target set to: `{cid}`")
        log.info(f"User {user_id}: Target set to {cid}")

    # ── /cleartarget ──────────────────────────────────────────────────────────
    @app.on_message(filters.command("cleartarget") & filters.private)
    async def cmd_cleartarget(client: Client, msg: Message):
        user_id = msg.from_user.id
        await db_manager.clear_target(user_id)
        await msg.reply("✅ Target channel cleared.")
        log.info(f"User {user_id}: Target cleared")

    # ── /list  /myforwards ────────────────────────────────────────────────────
    @app.on_message(filters.command(["list", "myforwards"]) & filters.private)
    async def cmd_list(client: Client, msg: Message):
        user_id = msg.from_user.id
        sources = await db_manager.get_sources(user_id)
        target  = await db_manager.get_target(user_id)

        src_txt = "\n".join(f"  • `{s}`" for s in sources) if sources else "  _None_"
        tgt_txt = f"`{target}`" if target else "_Not set_"

        await msg.reply(
            "📋 **Current Settings**\n\n"
            f"**Sources:**\n{src_txt}\n\n"
            f"**Target:** {tgt_txt}"
        )

    # ── /check ────────────────────────────────────────────────────────────────
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
            return await msg.reply(
                "❌ Unsupported link format. Use `https://t.me/c/xxxx/yyy`"
            )

        chat_id = int("-100" + m.group(1))
        msg_id  = int(m.group(2))
        target  = await db_manager.get_target(user_id)

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

    # ── /stats ────────────────────────────────────────────────────────────────
    @app.on_message(filters.command("stats") & filters.private)
    async def cmd_stats(client: Client, msg: Message):
        user_id = msg.from_user.id
        stats = await db_manager.stats_collection.find_one({"user_id": user_id})

        if not stats:
            return await msg.reply("📊 No statistics yet. Start copying messages!")

        total   = stats.get("total_copies", 0)
        success = stats.get("successful_copies", 0)
        failed  = stats.get("failed_copies", 0)
        rate    = (success / total * 100) if total > 0 else 0

        await msg.reply(
            "📊 **Your Statistics**\n\n"
            f"Total Copies: `{total}`\n"
            f"Successful: `{success}`\n"
            f"Failed: `{failed}`\n"
            f"Success Rate: `{rate:.1f}%`"
        )

    # ── Auto-forward: new channel posts ──────────────────────────────────────

    @app.on_message(filters.channel)
    async def handle_channel_post(client: Client, msg: Message):
        """Triggered for every new channel post the bot can see."""
        chat_id = msg.chat.id

        # Only process video or document messages
        if not (msg.video or msg.document):
            return

        # Find all users who have this channel as a source
        all_users = await db_manager.users_collection.find(
            {"sources": {"$in": [chat_id]}}
        ).to_list(None)

        if not all_users:
            return

        for user_config in all_users:
            user_id = user_config["user_id"]
            target  = user_config.get("target")

            if not target:
                log.warning(
                    f"User {user_id}: message from source {chat_id} "
                    "received but no target is set."
                )
                continue

            log.info(f"User {user_id}: Copying msg {msg.id} from {chat_id} → {target}")
            ok = await safe_copy(client, chat_id, msg.id, target)
            await db_manager.record_copy(user_id, chat_id, msg.id, target, ok)

            if not ok:
                log.error(
                    f"User {user_id}: Failed to copy msg {msg.id} from {chat_id}"
                )

# ─── AUTO-RESTART WATCHDOG ────────────────────────────────────────────────────

_stop_requested = False

def _handle_sigterm(signum, frame):
    global _stop_requested
    log.info("Stop signal received — shutting down permanently.")
    _stop_requested = True
    sys.exit(0)

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT,  _handle_sigterm)


async def run_once() -> None:
    """Start the bot, pre-warm peers, then run until crash or stop."""
    await db_manager.connect()
    app = build_app()
    register_handlers(app)
    await app.start()

    # ── Key fix: warm up every stored peer right after start ─────────────────
    await prewarm_peers(app)

    log.info("✅ Bot is running.")
    try:
        await asyncio.Event().wait()   # block forever
    finally:
        try:
            await app.stop()
        except Exception:
            pass
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
        log.error(
            "❌ Missing required environment variables: BOT_TOKEN, API_ID, API_HASH"
        )
        sys.exit(1)

    log.info("🤖 Clean Forward Bot starting with auto-restart watchdog (MongoDB backend)…")
    run_with_autorestart()
