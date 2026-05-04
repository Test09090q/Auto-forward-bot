"""
Clean Forward Bot
Copies videos/documents from source channels to target without forward tags.
Auto-restart on crash via internal watchdog loop.
"""

import asyncio
import logging
import json
import os
import re
import sys
import signal
import traceback
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, ChatAdminRequired, ChannelInvalid, PeerIdInvalid

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BOT_TOKEN = ""
API_ID    = int(os.environ.get("API_ID", ""))
API_HASH  = os.environ.get("API_HASH", "")
DATA_FILE = "data.json"

MAX_RETRIES    = 0          # 0 = restart forever
RESTART_DELAY  = 5          # seconds between restart attempts

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

# ─── DATA STORE ───────────────────────────────────────────────────────────────

def load_data() -> dict:
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r") as f:
            return json.load(f)
    return {"sources": [], "target": None}

def save_data(data: dict):
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)

data = load_data()

# ─── HELPERS ──────────────────────────────────────────────────────────────────

def parse_channel_id(text: str) -> int | None:
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
    """Copy a single message without forward tag, with retry on flood wait."""
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
            return False
        except Exception as e:
            log.error(f"Attempt {attempt+1} failed: {e}")
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
            "`/check https://t.me/c/xxxx/yyy` — Copy one specific message\n\n"
            "⚙️ Make sure the bot is an **admin** in all channels."
        )

    @app.on_message(filters.command("addsource") & filters.private)
    async def cmd_addsource(client: Client, msg: Message):
        parts = msg.text.split()
        if len(parts) < 2:
            return await msg.reply("Usage: `/addsource -100xxxxxxxxxx`")
        cid = parse_channel_id(parts[1])
        if cid is None:
            return await msg.reply("❌ Invalid channel ID. Must start with `-100`.")
        if cid in data["sources"]:
            return await msg.reply("⚠️ Already in sources list.")
        data["sources"].append(cid)
        save_data(data)
        await msg.reply(f"✅ Added source: `{cid}`")
        log.info(f"Source added: {cid}")

    @app.on_message(filters.command(["removesource", "remove"]) & filters.private)
    async def cmd_removesource(client: Client, msg: Message):
        parts = msg.text.split()
        if len(parts) < 2:
            return await msg.reply("Usage: `/removesource -100xxxxxxxxxx`")
        cid = parse_channel_id(parts[1])
        if cid is None:
            return await msg.reply("❌ Invalid channel ID.")
        if cid not in data["sources"]:
            return await msg.reply("⚠️ Not in sources list.")
        data["sources"].remove(cid)
        save_data(data)
        await msg.reply(f"✅ Removed source: `{cid}`")
        log.info(f"Source removed: {cid}")

    @app.on_message(filters.command("settarget") & filters.private)
    async def cmd_settarget(client: Client, msg: Message):
        parts = msg.text.split()
        if len(parts) < 2:
            return await msg.reply("Usage: `/settarget -100xxxxxxxxxx`")
        cid = parse_channel_id(parts[1])
        if cid is None:
            return await msg.reply("❌ Invalid channel ID. Must start with `-100`.")
        data["target"] = cid
        save_data(data)
        await msg.reply(f"✅ Target set to: `{cid}`")
        log.info(f"Target set: {cid}")

    @app.on_message(filters.command("cleartarget") & filters.private)
    async def cmd_cleartarget(client: Client, msg: Message):
        data["target"] = None
        save_data(data)
        await msg.reply("✅ Target channel cleared.")

    @app.on_message(filters.command(["list", "myforwards"]) & filters.private)
    async def cmd_list(client: Client, msg: Message):
        sources = data["sources"]
        target  = data["target"]
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
        parts = msg.text.split()
        if len(parts) < 2:
            return await msg.reply("Usage: `/check https://t.me/c/xxxx/yyy`")

        url = parts[1]
        m = re.match(r"https://t\.me/c/(\d+)/(\d+)", url)
        if not m:
            return await msg.reply("❌ Unsupported link format. Use `https://t.me/c/xxxx/yyy`")

        chat_id = int("-100" + m.group(1))
        msg_id  = int(m.group(2))
        target  = data.get("target")

        if not target:
            return await msg.reply("❌ No target set. Use `/settarget` first.")

        status = await msg.reply("⏳ Copying message…")
        ok = await safe_copy(client, chat_id, msg_id, target)
        if ok:
            await status.edit("✅ Message copied successfully!")
        else:
            await status.edit("❌ Failed to copy. Check bot permissions and logs.")

    # ─── AUTO-FORWARD HANDLER ─────────────────────────────────────────────────

    @app.on_message(filters.channel)
    async def handle_channel_post(client: Client, msg: Message):
        """Triggered for every new channel post the bot can see."""
        chat_id = msg.chat.id
        if chat_id not in data["sources"]:
            return
        target = data.get("target")
        if not target:
            log.warning(f"Received message from source {chat_id} but no target is set.")
            return

        if not (msg.video or msg.document):
            return

        log.info(f"Copying msg {msg.id} from {chat_id} → {target}")
        ok = await safe_copy(client, chat_id, msg.id, target)
        if not ok:
            log.error(f"Failed to copy msg {msg.id} from {chat_id}")

# ─── AUTO-RESTART WATCHDOG ────────────────────────────────────────────────────

_stop_requested = False

def _handle_sigterm(signum, frame):
    """On SIGTERM / Ctrl-C, stop the restart loop gracefully."""
    global _stop_requested
    log.info("Stop signal received — shutting down permanently.")
    _stop_requested = True
    sys.exit(0)

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT,  _handle_sigterm)

async def run_once() -> None:
    """Start the bot and run until it crashes or stops."""
    app = build_app()
    register_handlers(app)
    await app.start()
    log.info("✅ Bot is running.")
    await asyncio.Event().wait()   # block until an exception kills the task

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
    log.info("🤖 Clean Forward Bot starting with auto-restart watchdog…")
    run_with_autorestart()
