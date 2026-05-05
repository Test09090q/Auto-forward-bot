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
from motor.motor_asyncio import AsyncIOMotorClient

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
API_ID = int(os.environ.get("API_ID", 0))
API_HASH = os.environ.get("API_HASH", "")
MONGODB_URI = os.environ.get("MONGODB_URI", "")
DATABASE_NAME = os.environ.get("DATABASE_NAME", "clean_forward_bot")

MAX_RETRIES = 0
RESTART_DELAY = 5

# ─── LOGGING ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ─── MONGODB ──────────────────────────────────────────────────────────────────

class MongoDBManager:
    def __init__(self, uri: str, db_name: str):
        self.uri = uri
        self.db_name = db_name
        self.client: Optional[AsyncIOMotorClient] = None
        self.db = None
        self.users = None
        self.stats = None

    async def connect(self):
        try:
            self.client = AsyncIOMotorClient(self.uri)
            self.db = self.client[self.db_name]
            self.users = self.db["users"]
            self.stats = self.db["stats"]

            await self.users.create_index("user_id", unique=True)
            await self.stats.create_index("user_id", unique=True)

            await self.client.admin.command('ping')
            log.info("✅ MongoDB Connected")
        except Exception as e:
            log.error(f"❌ MongoDB Error: {e}")
            raise

    async def disconnect(self):
        if self.client:
            self.client.close()

    async def get_user_config(self, user_id: int) -> Dict:
        doc = await self.users.find_one({"user_id": user_id})
        if not doc:
            doc = {
                "user_id": user_id,
                "sources": [],
                "target": None,
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow()
            }
            await self.users.insert_one(doc)
        return doc

    async def add_source(self, user_id: int, channel_id: int) -> bool:
        result = await self.users.update_one(
            {"user_id": user_id},
            {"$addToSet": {"sources": channel_id}, "$set": {"updated_at": datetime.utcnow()}}
        )
        return result.modified_count > 0

    async def remove_source(self, user_id: int, channel_id: int) -> bool:
        result = await self.users.update_one(
            {"user_id": user_id},
            {"$pull": {"sources": channel_id}, "$set": {"updated_at": datetime.utcnow()}}
        )
        return result.modified_count > 0

    async def set_target(self, user_id: int, channel_id: int):
        await self.users.update_one(
            {"user_id": user_id},
            {"$set": {"target": channel_id, "updated_at": datetime.utcnow()}}
        )

    async def clear_target(self, user_id: int):
        await self.users.update_one({"user_id": user_id}, {"$set": {"target": None, "updated_at": datetime.utcnow()}})

    async def get_sources(self, user_id: int) -> List[int]:
        config = await self.get_user_config(user_id)
        return config.get("sources", [])

    async def get_target(self, user_id: int) -> Optional[int]:
        config = await self.get_user_config(user_id)
        return config.get("target")

    async def record_copy(self, user_id: int, from_chat: int, msg_id: int, to_chat: int, success: bool):
        await self.stats.update_one(
            {"user_id": user_id},
            {
                "$inc": {"total_copies": 1, "successful_copies": 1 if success else 0, "failed_copies": 0 if success else 1},
                "$set": {"last_copy_at": datetime.utcnow()}
            },
            upsert=True
        )


db_manager = MongoDBManager(MONGODB_URI, DATABASE_NAME)

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def parse_channel_id(text: str) -> Optional[int]:
    text = text.strip()
    # Support -1001234567890, 1001234567890, or 1234567890
    match = re.search(r'(?:-100)?(\d{8,})', text)
    if match:
        return int("-100" + match.group(1))
    return None


async def resolve_peer(client: Client, chat_id: int, retries: int = 3) -> bool:
    """Force resolve peer to fix PeerIdInvalid after restart"""
    for attempt in range(retries):
        try:
            await client.get_chat(chat_id)
            return True
        except (PeerIdInvalid, ChannelInvalid, UserNotParticipant):
            await asyncio.sleep(1.5)
        except Exception as e:
            log.warning(f"Resolve peer {chat_id} failed: {e}")
            break
    return False


async def resolve_all_peers(client: Client):
    """Resolve all channels on startup"""
    log.info("🔄 Resolving all peers (sources + targets)...")
    users = await db_manager.users.find({}).to_list(None)
    peers = set()

    for u in users:
        if u.get("target"):
            peers.add(u["target"])
        for s in u.get("sources", []):
            peers.add(s)

    for pid in peers:
        if await resolve_peer(client, pid):
            log.info(f"✅ Peer resolved: {pid}")
        else:
            log.warning(f"⚠️ Could not resolve peer: {pid}")
        await asyncio.sleep(0.8)


async def safe_copy(client: Client, from_chat: int, msg_id: int, to_chat: int) -> bool:
    """Safe copy with peer recovery"""
    for attempt in range(5):
        try:
            await client.copy_message(to_chat, from_chat, msg_id)
            return True
        except PeerIdInvalid:
            log.warning(f"PeerIdInvalid → Resolving peers {from_chat} | {to_chat}")
            await resolve_peer(client, from_chat)
            await resolve_peer(client, to_chat)
            await asyncio.sleep(2)
        except FloodWait as e:
            log.warning(f"FloodWait {e.value}s")
            await asyncio.sleep(e.value + 1)
        except (ChatAdminRequired, ChannelInvalid, UserNotParticipant) as e:
            log.error(f"Permission error: {e}")
            return False
        except Exception as e:
            log.error(f"Copy error (attempt {attempt+1}): {e}")
            await asyncio.sleep(2)
    return False

# ─── BOT BUILDER ──────────────────────────────────────────────────────────────

def build_app() -> Client:
    return Client(
        "clean_forward_bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN
    )

# ─── COMMAND HANDLERS ─────────────────────────────────────────────────────────

def register_handlers(app: Client):

    @app.on_message(filters.command("start") & filters.private)
    async def cmd_start(_, msg: Message):
        await msg.reply(
            "👋 **Clean Forward Bot**\n\n"
            "Copies videos/documents **without forward tag**.\n\n"
            "Use commands below:"
        )

    @app.on_message(filters.command("addsource") & filters.private)
    async def cmd_addsource(client: Client, msg: Message):
        user_id = msg.from_user.id
        if len(msg.command) < 2:
            return await msg.reply("Usage: `/addsource -100xxxxxxxxxx`")

        cid = parse_channel_id(msg.command[1])
        if not cid:
            return await msg.reply("❌ Invalid Channel ID.")

        # Validate access
        if not await resolve_peer(client, cid):
            return await msg.reply("❌ Cannot access this channel. Make sure bot is **admin**.")

        if await db_manager.add_source(user_id, cid):
            await msg.reply(f"✅ Source added: `{cid}`")
        else:
            await msg.reply("⚠️ Already in sources.")

    @app.on_message(filters.command(["removesource", "remove"]) & filters.private)
    async def cmd_removesource(_, msg: Message):
        user_id = msg.from_user.id
        if len(msg.command) < 2:
            return await msg.reply("Usage: `/removesource -100xxxxxxxxxx`")

        cid = parse_channel_id(msg.command[1])
        if not cid:
            return await msg.reply("❌ Invalid Channel ID.")

        if await db_manager.remove_source(user_id, cid):
            await msg.reply(f"✅ Removed source: `{cid}`")
        else:
            await msg.reply("⚠️ Not in sources list.")

    @app.on_message(filters.command("settarget") & filters.private)
    async def cmd_settarget(client: Client, msg: Message):
        user_id = msg.from_user.id
        if len(msg.command) < 2:
            return await msg.reply("Usage: `/settarget -100xxxxxxxxxx`")

        cid = parse_channel_id(msg.command[1])
        if not cid:
            return await msg.reply("❌ Invalid Channel ID.")

        if not await resolve_peer(client, cid):
            return await msg.reply("❌ Cannot access that channel. Make sure the bot is an **admin** there.")

        await db_manager.set_target(user_id, cid)
        await msg.reply(f"✅ Target set to: `{cid}`")

    @app.on_message(filters.command("cleartarget") & filters.private)
    async def cmd_cleartarget(_, msg: Message):
        await db_manager.clear_target(msg.from_user.id)
        await msg.reply("✅ Target cleared.")

    @app.on_message(filters.command(["list", "myforwards"]) & filters.private)
    async def cmd_list(_, msg: Message):
        user_id = msg.from_user.id
        sources = await db_manager.get_sources(user_id)
        target = await db_manager.get_target(user_id)

        src_txt = "\n".join(f"• `{s}`" for s in sources) if sources else "_None_"
        await msg.reply(
            f"**Your Settings**\n\n"
            f"**Sources:**\n{src_txt}\n\n"
            f"**Target:** `{target}`" if target else "_Not set_"
        )

    @app.on_message(filters.command("check") & filters.private)
    async def cmd_check(client: Client, msg: Message):
        # ... (you can keep your original check logic or I can improve it later)
        pass  # Add if needed

    # Auto Forward Handler
    @app.on_message(filters.channel)
    async def handle_channel_post(client: Client, msg: Message):
        if not (msg.video or msg.document):
            return

        chat_id = msg.chat.id
        users = await db_manager.users.find({"sources": {"$in": [chat_id]}}).to_list(None)

        for user in users:
            target = user.get("target")
            if not target:
                continue

            log.info(f"Forwarding for user {user['user_id']}: {chat_id} → {target}")
            success = await safe_copy(client, chat_id, msg.id, target)
            await db_manager.record_copy(user['user_id'], chat_id, msg.id, target, success)


# ─── WATCHDOG ─────────────────────────────────────────────────────────────────

_stop_requested = False

def _handle_sigterm(*_):
    global _stop_requested
    _stop_requested = True
    sys.exit(0)

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


async def run_once():
    await db_manager.connect()
    app = build_app()
    register_handlers(app)

    await app.start()
    log.info("✅ Bot Started Successfully")

    await resolve_all_peers(app)          # ← Critical fix for PeerIdInvalid

    try:
        await asyncio.Event().wait()
    finally:
        await db_manager.disconnect()


def run_with_autorestart():
    attempt = 0
    while not _stop_requested:
        attempt += 1
        log.info(f"Starting bot (attempt #{attempt})...")
        try:
            asyncio.run(run_once())
        except (KeyboardInterrupt, SystemExit):
            break
        except Exception:
            log.error("Bot crashed:\n" + traceback.format_exc())

        if _stop_requested or (MAX_RETRIES and attempt >= MAX_RETRIES):
            break

        asyncio.run(asyncio.sleep(RESTART_DELAY))  # Use async sleep in main thread

    log.info("Bot stopped.")


# ─── START ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not all([BOT_TOKEN, API_ID, API_HASH, MONGODB_URI]):
        log.error("❌ Missing environment variables!")
        sys.exit(1)

    log.info("🚀 Clean Forward Bot with Auto-Restart + Peer Fix Started")
    run_with_autorestart()
