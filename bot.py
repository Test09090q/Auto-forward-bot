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

# ─── MONGODB MANAGER ─────────────────────────────────────────────────────────

class MongoDBManager:
    def __init__(self, uri: str, db_name: str):
        self.uri = uri
        self.db_name = db_name
        self.client = None
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
            log.info("✅ MongoDB Connected Successfully")
        except Exception as e:
            log.error(f"❌ MongoDB Connection Failed: {e}")
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
        return result.modified_count > 0 or result.upserted_id is not None

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
        await self.users.update_one(
            {"user_id": user_id},
            {"$set": {"target": None, "updated_at": datetime.utcnow()}}
        )

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
                "$inc": {
                    "total_copies": 1,
                    "successful_copies": 1 if success else 0,
                    "failed_copies": 0 if success else 1
                },
                "$set": {"last_copy_at": datetime.utcnow()}
            },
            upsert=True
        )


db_manager = MongoDBManager(MONGODB_URI, DATABASE_NAME)

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def parse_channel_id(text: str) -> Optional[int]:
    text = text.strip()
    match = re.search(r'(?:-100)?(\d{8,})', text)
    if match:
        return int("-100" + match.group(1))
    return None


async def resolve_peer(client: Client, chat_id: int) -> bool:
    """Force resolve peer to fix PeerIdInvalid after restart"""
    for _ in range(3):
        try:
            await client.get_chat(chat_id)
            return True
        except (PeerIdInvalid, ChannelInvalid, UserNotParticipant):
            await asyncio.sleep(1.5)
        except Exception:
            break
    return False


async def resolve_all_peers(client: Client):
    log.info("🔄 Resolving all peers to prevent PeerIdInvalid...")
    users = await db_manager.users.find({}).to_list(None)
    peers = set()
    for u in users:
        if u.get("target"):
            peers.add(u["target"])
        for s in u.get("sources", []):
            peers.add(s)

    for pid in peers:
        if await resolve_peer(client, pid):
            log.info(f"✅ Resolved: {pid}")
        await asyncio.sleep(0.7)


async def safe_copy(client: Client, from_chat: int, msg_id: int, to_chat: int) -> bool:
    for attempt in range(5):
        try:
            await client.copy_message(to_chat, from_chat, msg_id)
            return True
        except PeerIdInvalid:
            log.warning(f"PeerIdInvalid detected. Resolving {from_chat} and {to_chat}")
            await resolve_peer(client, from_chat)
            await resolve_peer(client, to_chat)
            await asyncio.sleep(2)
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
        except (ChatAdminRequired, ChannelInvalid, UserNotParticipant):
            return False
        except Exception as e:
            log.error(f"Copy failed attempt {attempt+1}: {e}")
            await asyncio.sleep(2)
    return False

# ─── BOT SETUP ────────────────────────────────────────────────────────────────

def build_app() -> Client:
    return Client(
        "clean_forward_bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN
    )


def register_handlers(app: Client):

    @app.on_message(filters.command("start") & filters.private)
    async def cmd_start(_, msg: Message):
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
        if len(msg.command) < 2:
            return await msg.reply("Usage: `/addsource -100xxxxxxxxxx`")

        cid = parse_channel_id(msg.command[1])
        if not cid:
            return await msg.reply("❌ Invalid Channel ID. Example: `-1001234567890`")

        if not await resolve_peer(client, cid):
            return await msg.reply("❌ Cannot access this channel. Make sure the bot is **admin** there.")

        if await db_manager.add_source(user_id, cid):
            await msg.reply(f"✅ Source added: `{cid}`")
        else:
            await msg.reply("⚠️ This channel is already in your sources.")

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
            await msg.reply("⚠️ Channel not found in your sources.")

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
        await msg.reply(f"✅ Target channel set to: `{cid}`")

    @app.on_message(filters.command("cleartarget") & filters.private)
    async def cmd_cleartarget(_, msg: Message):
        await db_manager.clear_target(msg.from_user.id)
        await msg.reply("✅ Target channel has been cleared.")

    @app.on_message(filters.command(["list", "myforwards"]) & filters.private)
    async def cmd_list(_, msg: Message):
        user_id = msg.from_user.id
        sources = await db_manager.get_sources(user_id)
        target = await db_manager.get_target(user_id)

        src_txt = "\n".join(f"• `{s}`" for s in sources) if sources else "None"
        tgt_txt = f"`{target}`" if target else "Not set"

        await msg.reply(
            "📋 **Current Settings**\n\n"
            f"**Sources:**\n{src_txt}\n\n"
            f"**Target:** {tgt_txt}"
        )

    @app.on_message(filters.command("check") & filters.private)
    async def cmd_check(client: Client, msg: Message):
        user_id = msg.from_user.id
        if len(msg.command) < 2:
            return await msg.reply("Usage: `/check https://t.me/c/xxxx/yyy`")

        url = msg.command[1]
        m = re.match(r"https?://t\.me/c/(\d+)/(\d+)", url)
        if not m:
            return await msg.reply("❌ Invalid link. Use format: `https://t.me/c/xxxx/yyy`")

        chat_id = int("-100" + m.group(1))
        msg_id = int(m.group(2))
        target = await db_manager.get_target(user_id)

        if not target:
            return await msg.reply("❌ No target channel set. Use `/settarget` first.")

        status = await msg.reply("⏳ Copying message...")
        success = await safe_copy(client, chat_id, msg_id, target)
        await db_manager.record_copy(user_id, chat_id, msg_id, target, success)

        if success:
            await status.edit("✅ Message copied successfully!")
        else:
            await status.edit("❌ Failed to copy message. Check permissions.")

    @app.on_message(filters.command("stats") & filters.private)
    async def cmd_stats(_, msg: Message):
        user_id = msg.from_user.id
        stats = await db_manager.stats.find_one({"user_id": user_id}) or {}
        
        total = stats.get("total_copies", 0)
        success = stats.get("successful_copies", 0)
        failed = stats.get("failed_copies", 0)
        rate = (success / total * 100) if total > 0 else 0

        await msg.reply(
            "📊 **Your Statistics**\n\n"
            f"Total Copies: `{total}`\n"
            f"Successful: `{success}`\n"
            f"Failed: `{failed}`\n"
            f"Success Rate: `{rate:.1f}%`"
        )

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
            success = await safe_copy(client, chat_id, msg.id, target)
            await db_manager.record_copy(user["user_id"], chat_id, msg.id, target, success)


# ─── AUTO RESTART WATCHDOG ───────────────────────────────────────────────────

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
    log.info("✅ Bot is running...")

    await resolve_all_peers(app)   # Important fix for Peer ID issues

    try:
        await asyncio.Event().wait()
    finally:
        await db_manager.disconnect()


def run_with_autorestart():
    attempt = 0
    while not _stop_requested:
        attempt += 1
        log.info(f"▶️ Starting bot (attempt #{attempt})...")
        try:
            asyncio.run(run_once())
        except (KeyboardInterrupt, SystemExit):
            break
        except Exception:
            log.error("💥 Bot crashed:\n" + traceback.format_exc())

        if _stop_requested or (MAX_RETRIES and attempt >= MAX_RETRIES):
            break

        log.info(f"🔄 Restarting in {RESTART_DELAY} seconds...")
        asyncio.run(asyncio.sleep(RESTART_DELAY))

    log.info("🛑 Bot stopped permanently.")


# ─── ENTRY POINT ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if not all([BOT_TOKEN, API_ID, API_HASH, MONGODB_URI]):
        log.error("❌ Missing environment variables (BOT_TOKEN, API_ID, API_HASH, MONGODB_URI)")
        sys.exit(1)

    log.info("🚀 Clean Forward Bot Starting with Peer Fix...")
    run_with_autorestart()
