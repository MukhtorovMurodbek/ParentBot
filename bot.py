"""ParentBot -- the private one, for the owner only.

The other four bots in the family each serve the public and each answer to
their own owner-only commands. ParentBot serves exactly one person and
answers for all of them:

  * **Watches.** Every bot stamps a heartbeat into the shared database every
    30 seconds (family_link.py). ParentBot checks those stamps once a minute
    and messages the owner the moment one goes stale -- and again when it
    comes back. It never repeats itself while a bot stays down.
  * **Reports.** Anything a bot considers worth waking someone for -- an
    unhandled exception, a donation, a restart -- lands in family.events and
    ParentBot forwards it as a DM within about twenty seconds.
  * **Reaches in.** /run <bot> <command> puts a job on the family command
    queue; the target bot runs it in its own process, with its own code and
    its own Telegram identity, and the answer comes back here. That is how
    /dbdump, /whois, /message and the rest work against a bot deployed on a
    machine ParentBot cannot otherwise reach.
  * **Reads across.** One shared Postgres database with a schema per bot
    means "how many people used ConvertBot this week" is a single query, no
    matter whether ConvertBot itself is even running.

Nothing here is reachable by anyone but the ids in PBOT_ADMIN_ID. A stranger
who finds this bot gets one flat sentence and nothing else -- and the owner
gets told they turned up.

Env vars: PBOT_TOKEN, PBOT_USERNAME (no @), PBOT_ADMIN_ID (required -- with
it empty the bot refuses to start rather than run wide open), DATABASE_URL
(the shared family database), DB_SCHEMA (default "parent_bot").
"""
import asyncio
import html
import logging
import os
from datetime import datetime, time, timedelta, timezone
from io import BytesIO

try:  # optional convenience: load the .env sitting next to this file
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    TypeHandler,
    filters,
)

import db
import family_link
from db import init_db
from monitoring import (
    attach_maintenance,
    build_status_text,
    error_handler,
    flush_on_shutdown,
    setup_logging,
    track_activity,
    tune_runtime,
)

setup_logging(__file__)
logger = logging.getLogger(__name__)

START_TIME = datetime.now(timezone.utc)

BOT_TOKEN = os.environ.get("PBOT_TOKEN")
BOT_USERNAME = os.environ.get("PBOT_USERNAME")  # no @
BOT_NAME = "parentbot"
DISPLAY_NAME = "ParentBot"

ADMIN_IDS = {int(x) for x in os.environ.get("PBOT_ADMIN_ID", "").split(",") if x.strip()}

# A bot is "down" once its heartbeat is this stale. family_link beats every
# 30s by default, so this is four missed beats -- long enough not to page
# anyone over one slow database round-trip, short enough to catch a crash
# loop before it has been down for an hour.
DOWN_AFTER_SECONDS = int(os.environ.get("PBOT_DOWN_AFTER_SECONDS", "120"))

# How long a queued command waits for its target before ParentBot gives up
# and says so.
COMMAND_TIMEOUT_SECONDS = int(os.environ.get("PBOT_COMMAND_TIMEOUT_SECONDS", "90"))

# How long the startup roll-call waits before reporting who is up. The point
# of the delay is to send one message instead of five: the other four write
# their first heartbeat inside family_link.attach(), synchronously, before
# they start polling -- so a few seconds is all it takes for a family started
# together to be fully visible. start_all.ps1 already gives them the same head
# start, for the same reason. Set to 0 to report immediately.
STARTUP_ROLLCALL_SECONDS = int(os.environ.get("PBOT_ROLLCALL_SECONDS", "5"))

# A bot whose heartbeat is younger than this at roll-call time came up with
# this batch rather than having been running already. Affects the wording of
# one line, nothing else.
ROLLCALL_FRESH_SECONDS = int(os.environ.get("PBOT_ROLLCALL_FRESH_SECONDS", "120"))

# Which event levels are worth an unprompted DM. Everything is still
# recorded either way -- /events shows the rest.
ALERT_LEVELS = {"warning", "error", "critical"}

# "HH:MM" in UTC for a once-a-day summary, or empty for none (the default --
# down/up transitions and crashes already arrive on their own, and a daily
# "all fine" message is the kind of thing you stop reading).
DIGEST_AT_UTC = os.environ.get("PBOT_DIGEST_UTC", "").strip()

TELEGRAM_MAX_CHARS = 3900  # a little under the real 4096, leaving room for markup

# Telegram's long-poll window -- see the note in main().
POLL_TIMEOUT = int(os.environ.get("POLL_TIMEOUT", "30"))


# ---------------------------------------------------------------------------
# Who is in the family
# ---------------------------------------------------------------------------
# id : display name : Postgres schema. The ids match each bot's own BOT_NAME
# constant (and the SIBLING_BOTS env var they all share), which is what the
# command queue and the heartbeat table key on. Override with FAMILY_REGISTRY
# if you add a sixth bot without redeploying this one.

DEFAULT_REGISTRY = (
    "stickerbot:StickerBot:sticker_bot,"
    "convertbot:ConvertBot:convert_bot,"
    "downloaderbot:DownloaderBot:downloader_bot,"
    "anonbot:AnonBot:anon_bot"
)


def _parse_registry() -> list[dict]:
    raw = os.environ.get("FAMILY_REGISTRY", DEFAULT_REGISTRY)
    bots = []
    for entry in raw.split(","):
        parts = [p.strip() for p in entry.strip().split(":")]
        if len(parts) == 3 and all(parts):
            bots.append({"id": parts[0], "name": parts[1], "schema": parts[2]})
    return bots


CHILDREN = _parse_registry()
ALL_BOTS = CHILDREN + [{"id": BOT_NAME, "name": DISPLAY_NAME, "schema": db.DB_SCHEMA}]


def resolve_bot(token: str) -> dict | None:
    """Accepts "sticker", "stickerbot", "StickerBot", "sticker_bot" -- any
    unambiguous prefix of the id, the display name, or the schema."""
    token = token.strip().lower().lstrip("@")
    if not token:
        return None
    for bot in ALL_BOTS:
        if token in (bot["id"], bot["name"].lower(), bot["schema"]):
            return bot
    matches = [
        b for b in ALL_BOTS
        if b["id"].startswith(token) or b["name"].lower().startswith(token) or b["schema"].startswith(token)
    ]
    return matches[0] if len(matches) == 1 else None


def bot_list_hint() -> str:
    return ", ".join(b["id"] for b in ALL_BOTS)


# ---------------------------------------------------------------------------
# Access control
# ---------------------------------------------------------------------------
# Tracked so the owner hears about a stranger once rather than on every
# message they send. In memory only: a restart re-arms it, which is fine --
# the point is "someone found the private bot", not an audit log.
_reported_strangers: set[int] = set()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


async def guard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """True if this update may proceed. Non-admins get one flat sentence,
    and the owner gets told about them the first time."""
    user = update.effective_user
    if user and is_admin(user.id):
        return True
    if user and user.id not in _reported_strangers:
        _reported_strangers.add(user.id)
        handle = f" (@{user.username})" if user.username else ""
        await notify_owner(
            context,
            f"👀 A stranger found ParentBot: <code>{user.id}</code>{html.escape(handle)} "
            f"— {html.escape(user.full_name or '?')}. They were turned away.",
        )
        await asyncio.to_thread(
            db.log_event, BOT_NAME, "warning", "stranger",
            f"Unknown user {user.id}{handle} messaged ParentBot.",
        )
    if update.message:
        await update.message.reply_text("This is a private bot.")
    return False


# ---------------------------------------------------------------------------
# Sending
# ---------------------------------------------------------------------------

async def notify_owner(context: ContextTypes.DEFAULT_TYPE, text: str) -> None:
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(chat_id=admin_id, text=text, parse_mode=ParseMode.HTML)
        except Exception:
            logger.exception("Couldn't deliver an alert to %s", admin_id)


async def send_long(context: ContextTypes.DEFAULT_TYPE, chat_id: int, text: str, filename: str = "output.txt") -> None:
    """Anything past Telegram's message ceiling goes as a file instead of
    being silently cut in half -- a truncated log tail is worse than useless
    when you are trying to work out what broke."""
    if len(text) <= TELEGRAM_MAX_CHARS:
        await context.bot.send_message(chat_id=chat_id, text=text)
        return
    await context.bot.send_message(chat_id=chat_id, text=f"{text[:TELEGRAM_MAX_CHARS]}\n\n[…full text attached]")
    await context.bot.send_document(
        chat_id=chat_id, document=BytesIO(text.encode("utf-8")), filename=filename
    )


def format_delta(seconds: float) -> str:
    seconds = int(seconds)
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m"
    return f"{secs}s"


# ---------------------------------------------------------------------------
# /status -- the board
# ---------------------------------------------------------------------------

def _build_board() -> str:
    """Runs entirely in a worker thread (it is all blocking SQL), so the
    caller wraps it in asyncio.to_thread. One query for the whole board --
    see db.status_snapshot."""
    now = datetime.now(timezone.utc)
    snapshot = db.status_snapshot([b["schema"] for b in ALL_BOTS], now - timedelta(hours=1))
    beats = {row["bot_id"]: row for row in snapshot["beats"]}
    active_by_schema = snapshot["active"]

    up_count = 0
    lines = []
    for bot in ALL_BOTS:
        beat = beats.get(bot["id"])
        me = " (me)" if bot["id"] == BOT_NAME else ""
        if beat is None:
            lines.append(f"❔ <b>{bot['name']}</b>{me} — never seen. Has it ever been started?")
            continue

        is_up = beat["seconds_ago"] <= DOWN_AFTER_SECONDS
        up_count += is_up
        if not is_up:
            lines.append(
                f"❌ <b>{bot['name']}</b>{me} — DOWN, last heartbeat "
                f"{format_delta(beat['seconds_ago'])} ago (was up "
                f"{format_delta((beat['last_seen'] - beat['started_at']).total_seconds())})"
            )
            continue

        active = active_by_schema.get(bot["schema"])
        uptime = format_delta((now - beat["started_at"]).total_seconds())
        errors = f" · ⚠️ {beat['error_count']}" if beat["error_count"] else ""
        users = "" if active is None else f" · {active} user(s)/h"
        lines.append(
            f"✅ <b>{bot['name']}</b>{me} — up {uptime} · {html.escape(beat['host'] or '?')} "
            f"· v{beat['version']}{errors}{users}"
        )

    info = snapshot["database"]
    db_line = f"🗄 Database: {info['name']} · Postgres {info['version']} · {info['size']}"
    header = f"👪 <b>Bot family</b> — {up_count}/{len(ALL_BOTS)} up"
    alerts = "on" if snapshot["alerts"] == "on" else "OFF"
    stamp = now.strftime("%H:%M:%S UTC")
    return "\n".join([header, "", *lines, "", db_line,
                      f"🔔 Alerts: {alerts} · 🕐 {stamp}"])


def _board_keyboard() -> InlineKeyboardMarkup:
    """A refresh button, because /status is the command anyone watching a
    deploy types over and over -- and a mute toggle right where you notice
    you want it."""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("🔄 Refresh", callback_data="board:refresh"),
        InlineKeyboardButton("🔔 Alerts", callback_data="board:alerts"),
        InlineKeyboardButton("📡 Ping all", callback_data="board:ping"),
    ]])


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update, context):
        return
    try:
        board = await asyncio.to_thread(_build_board)
    except Exception as exc:
        await update.message.reply_text(
            f"⚠️ Couldn't read the family database: {exc}\n\n"
            "Every bot may well be fine -- this is ParentBot's own connection failing."
        )
        return
    await update.message.reply_text(board, parse_mode=ParseMode.HTML, reply_markup=_board_keyboard())


async def board_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """The three buttons under the /status board."""
    if not await guard(update, context):
        return
    query = update.callback_query
    action = query.data.split(":", 1)[1]

    if action == "alerts":
        current = await asyncio.to_thread(db.get_setting, "alerts", "on")
        new = "off" if current == "on" else "on"
        await asyncio.to_thread(db.set_setting, "alerts", new)
        await query.answer(f"Alerts {new}.")
    elif action == "ping":
        for child in CHILDREN:
            await asyncio.to_thread(
                db.queue_command, child["id"], "ping", "",
                update.effective_user.id, update.effective_chat.id,
            )
        await query.answer(f"Pinged all {len(CHILDREN)}.")
        return
    else:
        await query.answer()

    try:
        board = await asyncio.to_thread(_build_board)
    except Exception as exc:
        # A second query.answer() on the same callback is rejected by
        # Telegram, and this one has already been answered above -- so say it
        # in the chat instead.
        await query.message.reply_text(
            f"⚠️ Couldn't read the family database: {exc}\n\n"
            "Every bot may well be fine -- this is ParentBot's own connection failing."
        )
        return
    try:
        await query.message.edit_text(board, parse_mode=ParseMode.HTML, reply_markup=_board_keyboard())
    except Exception:
        # "message is not modified" when nothing changed within the same
        # second -- the timestamp in the footer makes this rare, and it is
        # never worth surfacing.
        pass


async def me_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ParentBot's own /status, in the same shape every other bot uses."""
    if not await guard(update, context):
        return
    now = datetime.now(timezone.utc)
    hour = await asyncio.to_thread(db.count_active_users_since, now - timedelta(hours=1))
    since_start = await asyncio.to_thread(db.count_active_users_since, START_TIME)
    await update.message.reply_text(build_status_text(START_TIME, hour, since_start))


# ---------------------------------------------------------------------------
# /run -- reaching into another bot
# ---------------------------------------------------------------------------

async def _dispatch(update: Update, context: ContextTypes.DEFAULT_TYPE,
                    bot: dict, command: str, args: list[str]) -> None:
    if bot["id"] == BOT_NAME:
        await update.message.reply_text(
            "That one is me. Use /status, /me, /events or /backup directly."
        )
        return
    if command not in family_link.COMMANDS:
        known = ", ".join(sorted(family_link.COMMANDS))
        await update.message.reply_text(f"{bot['name']} has no '{command}'. Try: {known}")
        return

    command_id = await asyncio.to_thread(
        db.queue_command, bot["id"], command, " ".join(args),
        update.effective_user.id, update.effective_chat.id,
    )
    # "queued as #N", not "(#N)": the bare number read as a *result* -- an
    # acknowledgement of "errors" that says (#5) and is answered seconds later
    # with "No errors since this instance started" looks like the bot
    # contradicting itself. N is the row id of the queued command, which is
    # only ever useful for matching this line to the timeout notice that
    # quotes the same id.
    await update.message.reply_text(f"→ {bot['name']} · {command} — queued as #{command_id}")


async def run_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/run <bot> <command> [args] -- the general form. The shortcuts below
    (/dbdump, /whois, /say, ...) are just this with the command baked in."""
    if not await guard(update, context):
        return
    if len(context.args) < 2:
        lines = ["Usage: /run &lt;bot&gt; &lt;command&gt; [args]", "", "<b>Bots:</b> " + bot_list_hint(), "", "<b>Commands:</b>"]
        lines += [f"  <code>{name}</code> — {desc}" for name, desc in family_link.COMMAND_HELP.items()]
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
        return

    bot = resolve_bot(context.args[0])
    if not bot:
        await update.message.reply_text(f"No such bot: {context.args[0]}. Known: {bot_list_hint()}")
        return
    await _dispatch(update, context, bot, context.args[1].lower(), context.args[2:])


def _shortcut(command: str, min_args: int, usage: str):
    """Builds /whois, /say, /logs and friends -- each is /run with the
    command fixed and the first argument still naming the target bot."""
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await guard(update, context):
            return
        if len(context.args) < min_args:
            await update.message.reply_text(usage)
            return
        bot = resolve_bot(context.args[0])
        if not bot:
            await update.message.reply_text(f"No such bot: {context.args[0]}. Known: {bot_list_hint()}")
            return
        await _dispatch(update, context, bot, command, list(context.args[1:]))
    return handler


async def broadcast_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/ping with no bot named asks every one of them at once -- the fastest
    way to tell "the database is down" from "one bot is down"."""
    if not await guard(update, context):
        return
    if context.args:
        bot = resolve_bot(context.args[0])
        if not bot:
            await update.message.reply_text(f"No such bot: {context.args[0]}. Known: {bot_list_hint()}")
            return
        await _dispatch(update, context, bot, "ping", [])
        return
    for child in CHILDREN:
        await asyncio.to_thread(
            db.queue_command, child["id"], "ping", "",
            update.effective_user.id, update.effective_chat.id,
        )
    await update.message.reply_text(f"Pinged all {len(CHILDREN)}. Silence past "
                                    f"{COMMAND_TIMEOUT_SECONDS}s means down.")


# ---------------------------------------------------------------------------
# Reading the shared database directly
# ---------------------------------------------------------------------------

async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """No command queue involved -- this is read straight out of the shared
    database, so it answers for bots that are currently down too."""
    if not await guard(update, context):
        return
    hours = int(context.args[0]) if context.args and context.args[0].isdigit() else 24
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    by_schema = await asyncio.to_thread(
        db.active_users_by_schema, [b["schema"] for b in ALL_BOTS], since, True
    )
    lines = [f"👥 Active users, last {hours}h (all-time known in brackets)", ""]
    for bot in ALL_BOTS:
        counts = by_schema.get(bot["schema"])
        if counts is None:
            lines.append(f"  {bot['name']}: — (no tables yet)")
        else:
            lines.append(f"  {bot['name']}: {counts[0]}  [{counts[1]}]")
    await update.message.reply_text("\n".join(lines))


async def donations_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update, context):
        return

    by_schema = await asyncio.to_thread(
        db.donations_by_schema, [b["schema"] for b in ALL_BOTS]
    )
    lines = ["💝 Paid donations, per bot", ""]
    any_paid = False
    for bot in ALL_BOTS:
        per_currency = by_schema.get(bot["schema"])
        if not per_currency:
            continue
        name = bot["name"]
        for currency, count, total in per_currency:
            any_paid = True
            unit = "⭐" if currency == "XTR" else f" {currency} (minor units)"
            lines.append(f"  {name}: {total}{unit} over {count} payment(s)")
    if not any_paid:
        lines.append("  Nothing paid yet, anywhere.")
    await update.message.reply_text("\n".join(lines))


async def events_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update, context):
        return
    limit = 15
    bot_id = None
    for arg in context.args:
        if arg.isdigit():
            limit = min(int(arg), 60)
        else:
            match = resolve_bot(arg)
            if match:
                bot_id = match["id"]
    rows = await asyncio.to_thread(db.recent_events, limit, bot_id)
    if not rows:
        await update.message.reply_text("Nothing recorded yet.")
        return
    icons = {"info": "·", "warning": "⚠️", "error": "🔥", "critical": "🚨"}
    lines = [f"🗒 Last {len(rows)} event(s)" + (f" from {bot_id}" if bot_id else ""), ""]
    for row in rows:
        stamp = row["occurred_at"].strftime("%m-%d %H:%M")
        lines.append(f"{icons.get(row['level'], '·')} {stamp} [{row['bot_id']}] {row['message']}")
    await send_long(context, update.effective_chat.id, "\n".join(lines), "events.txt")


async def sql_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/sql SELECT ... -- read-only, against the whole shared database, so
    joins across two bots' schemas work. The connection itself is set READ
    ONLY, so Postgres rejects a write even if the keyword check below is
    fooled."""
    if not await guard(update, context):
        return
    query = update.message.text.partition(" ")[2].strip()
    if not query:
        await update.message.reply_text(
            'Usage: /sql SELECT ...\n\ne.g. /sql SELECT count(*) FROM sticker_bot.packs'
        )
        return
    if query.lower().split()[0] not in ("select", "with", "table", "explain", "show"):
        await update.message.reply_text("Read-only: start with SELECT, WITH, TABLE, EXPLAIN or SHOW.")
        return
    try:
        cols, rows = await asyncio.to_thread(db.run_readonly_query, query)
    except Exception as exc:
        await update.message.reply_text(f"⚠️ {type(exc).__name__}: {exc}")
        return
    if not rows:
        await update.message.reply_text("No rows.")
        return
    body = [" | ".join(cols), "-" * 40]
    body += [" | ".join("NULL" if v is None else str(v) for v in row) for row in rows]
    body.append(f"\n({len(rows)} row(s), capped at 50)")
    await send_long(context, update.effective_chat.id, "\n".join(body), "query.txt")


# ---------------------------------------------------------------------------
# Backups
# ---------------------------------------------------------------------------

async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """The whole shared database -- every bot's schema, one folder each --
    as a single zip of CSVs. Built through the ordinary psycopg connection,
    so it needs no pg_dump anywhere and works identically against the
    laptop's Postgres and Railway's. For a restorable, full-fidelity copy
    (indexes, sequences, types) use db_backup.ps1 at the repo root."""
    if not await guard(update, context):
        return
    note = await update.message.reply_text("Exporting the whole family database…")
    schemas = [b["schema"] for b in ALL_BOTS] + ["family"]
    try:
        data = await asyncio.to_thread(db.dump_family_csv_zip, schemas)
    except Exception as exc:
        await note.edit_text(f"⚠️ Export failed: {exc}")
        return
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")
    await update.message.reply_document(
        document=BytesIO(data),
        filename=f"botfamily_{stamp}.zip",
        caption=f"{len(data)/1024:.0f} KB · {len(schemas)} schema(s)",
    )
    await note.delete()


# ---------------------------------------------------------------------------
# Alert toggles
# ---------------------------------------------------------------------------

async def alerts_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update, context):
        return
    if context.args and context.args[0].lower() in ("on", "off"):
        value = context.args[0].lower()
        await asyncio.to_thread(db.set_setting, "alerts", value)
        await update.message.reply_text(
            f"🔔 Alerts {value}."
            + ("" if value == "on" else "\nDown/up and crash alerts are muted. /status still works.")
        )
        return
    current = await asyncio.to_thread(db.get_setting, "alerts", "on")
    await update.message.reply_text(f"🔔 Alerts are {current}. Use /alerts on or /alerts off.")


# ---------------------------------------------------------------------------
# The watching itself
# ---------------------------------------------------------------------------

async def _alerts_on() -> bool:
    try:
        return await asyncio.to_thread(db.get_setting, "alerts", "on") == "on"
    except Exception:
        return True  # if the setting can't be read, err towards telling the owner


async def startup_rollcall(context: ContextTypes.DEFAULT_TYPE) -> None:
    """One message, once, a few seconds after ParentBot starts: who is up.

    This answers "I just started the family, did it work?" -- a question that
    previously had no answer except typing /status yourself. The watchdog only
    speaks on *changes*, and deliberately says nothing on its first pass
    (announcing four bots as up every time ParentBot restarts would be noise);
    a roll-call is the one moment where the current state, changed or not, is
    exactly what you want to see.

    Deliberately not five messages. Each bot could announce itself -- they
    already write a startup event -- but starting the family would then mean
    five notifications arriving in a random order over ten seconds. Waiting
    once and reporting the whole family together is the same information read
    in one glance.

    Runs on heartbeats, not on process management: ParentBot never starts,
    stops or supervises the other four, and does not need to in order to say
    what happened. That is what makes this work identically whether the four
    are on this laptop, on Railway, or split across both.
    """
    if not await _alerts_on():
        return
    try:
        beats = {row["bot_id"]: row for row in await asyncio.to_thread(db.all_heartbeats)}
    except Exception as exc:
        logger.warning("Startup roll-call couldn't reach the database: %s", exc)
        await notify_owner(
            context,
            "\U0001f44b <b>ParentBot is up</b>, but it cannot reach the family "
            "database, so it has no idea who else is.\n"
            f"<code>{html.escape(str(exc))}</code>",
        )
        return

    now = datetime.now(timezone.utc)
    lines, up_count = [], 0
    for bot in CHILDREN:
        beat = beats.get(bot["id"])
        if beat is None:
            lines.append(f"\u2754 <b>{bot['name']}</b> -- never seen. Has it ever been started?")
            continue
        if beat["seconds_ago"] > DOWN_AFTER_SECONDS:
            lines.append(
                f"\u274c <b>{bot['name']}</b> -- not up. Last heartbeat "
                f"{format_delta(beat['seconds_ago'])} ago."
            )
            continue

        up_count += 1
        uptime = (now - beat["started_at"]).total_seconds()
        host = html.escape(beat["host"] or "?")
        if uptime <= ROLLCALL_FRESH_SECONDS:
            lines.append(
                f"\U0001f7e2 <b>{bot['name']}</b> -- just started on {host} "
                f"\u00b7 v{beat['version']}"
            )
        else:
            lines.append(
                f"\u2705 <b>{bot['name']}</b> -- already up ({format_delta(uptime)}) "
                f"on {host} \u00b7 v{beat['version']}"
            )

    header = (
        f"\U0001f44b <b>ParentBot is up</b> on {html.escape(family_link.HOSTNAME)} -- "
        f"{up_count}/{len(CHILDREN)} of the family with it"
    )
    await notify_owner(context, "\n".join([header, "", *lines]))

    # Seed the up/down table with what was just reported. Without this the
    # watchdog's first pass treats every bot as newly discovered and can
    # repeat the bad news twenty seconds later; from here on it only speaks
    # when something actually changes, which is its job.
    for bot in CHILDREN:
        beat = beats.get(bot["id"])
        is_up = beat is not None and beat["seconds_ago"] <= DOWN_AFTER_SECONDS
        try:
            await asyncio.to_thread(db.set_known_state, bot["id"], is_up)
        except Exception:
            logger.debug("Couldn't seed bot_state for %s", bot["id"], exc_info=True)


# Consecutive failures of ParentBot's own database connection. The owner is
# told once, not once a minute, and told again when it recovers -- this is
# the one failure that would otherwise be completely silent, since every
# other alert path in this file runs through that same database.
_db_failures = 0
_db_alerted = False


async def watchdog(context: ContextTypes.DEFAULT_TYPE) -> None:
    global _db_failures, _db_alerted
    try:
        await asyncio.to_thread(db.expire_stale_commands, COMMAND_TIMEOUT_SECONDS)
        beats = {row["bot_id"]: row for row in await asyncio.to_thread(db.all_heartbeats)}
        known = await asyncio.to_thread(db.get_known_state)
    except Exception as exc:
        _db_failures += 1
        logger.warning("Watchdog couldn't reach the database (%s in a row): %s", _db_failures, exc)
        if _db_failures >= 3 and not _db_alerted:
            _db_alerted = True
            await notify_owner(
                context,
                "🚨 <b>ParentBot cannot reach the family database.</b>\n"
                f"<code>{html.escape(str(exc))}</code>\n\n"
                "Until it comes back I cannot see any bot's state, so treat "
                "silence from me as unknown, not as healthy.",
            )
        return

    if _db_alerted:
        _db_alerted = False
        await notify_owner(context, "✅ Family database is reachable again.")
    _db_failures = 0

    if not await _alerts_on():
        return

    for bot in CHILDREN:
        beat = beats.get(bot["id"])
        is_up = beat is not None and beat["seconds_ago"] <= DOWN_AFTER_SECONDS
        was_up = known.get(bot["id"])

        if was_up is None:
            # First sighting -- record it silently. Announcing "StickerBot is
            # up" the first time ParentBot ever runs is noise, not news.
            await asyncio.to_thread(db.set_known_state, bot["id"], is_up)
            if not is_up and beat is not None:
                await notify_owner(context, f"❌ <b>{bot['name']}</b> is down (first check since I started).")
            continue

        if is_up == was_up:
            continue

        await asyncio.to_thread(db.set_known_state, bot["id"], is_up)
        if is_up:
            uptime = format_delta((datetime.now(timezone.utc) - beat["started_at"]).total_seconds())
            await notify_owner(
                context,
                f"✅ <b>{bot['name']}</b> is back up (started {uptime} ago, "
                f"on {html.escape(beat['host'] or '?')})."
            )
        else:
            last = format_delta(beat["seconds_ago"]) if beat else "?"
            await notify_owner(
                context,
                f"❌ <b>{bot['name']}</b> has gone down — no heartbeat for {last}.\n"
                f"Its last host was {html.escape((beat or {}).get('host') or '?')}."
            )


async def event_pump(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Forwards what the bots themselves flagged. Info-level events are
    recorded but not pushed, except payments -- a donation arriving is the
    one piece of good news worth interrupting someone for."""
    if not await _alerts_on():
        # Deliberately before claiming anything: take_unnotified_events marks
        # rows as sent, so claiming while muted would silently eat them.
        return
    try:
        events = await asyncio.to_thread(db.take_unnotified_events, 20)
    except Exception:
        return

    icons = {"warning": "⚠️", "error": "🔥", "critical": "🚨", "info": "ℹ️"}
    for event in events:
        if event["level"] not in ALERT_LEVELS and event["kind"] != "payment":
            continue
        icon = icons.get(event["level"], "•")
        text = (
            f"{icon} <b>{html.escape(event['bot_id'])}</b> — {html.escape(event['kind'])}\n"
            f"{html.escape(event['message'])}"
        )
        if event["details"]:
            tail = event["details"].strip().splitlines()[-6:]
            text += "\n\n<pre>" + html.escape("\n".join(tail)) + "</pre>"
        await notify_owner(context, text[:4000])


async def result_pump(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Delivers whatever the child bots wrote back, and gives up on commands
    nobody claimed."""
    try:
        results = await asyncio.to_thread(db.take_finished_commands, 5)
    except Exception:
        return

    for result in results:
        chat_id = result["reply_chat_id"]
        if not chat_id:
            continue
        # Display name, not the raw queue key: the acknowledgement this is
        # answering said "StickerBot", and following it with "stickerbot"
        # reads as a different thing having replied.
        target = resolve_bot(result["target_bot"])
        head = f"{target['name'] if target else result['target_bot']} · {result['command']}"
        if result["status"] == "timeout":
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"⏳ {head} (queued as #{result['id']}) — no answer in {COMMAND_TIMEOUT_SECONDS}s. That bot looks down.",
            )
            continue

        mark = "✅" if result["ok"] else "⚠️"
        body = result["output"] or "(no output)"
        if result["file_bytes"]:
            await context.bot.send_document(
                chat_id=chat_id,
                document=BytesIO(bytes(result["file_bytes"])),
                filename=result["file_name"] or f"{result['target_bot']}.bin",
                caption=f"{mark} {head} — {body}"[:1000],
            )
        else:
            await send_long(context, chat_id, f"{mark} {head}\n\n{body}", f"{result['target_bot']}.txt")


async def daily_digest(context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        board = await asyncio.to_thread(_build_board)
    except Exception as exc:
        await notify_owner(context, f"🚨 Daily check failed: {html.escape(str(exc))}")
        return
    await notify_owner(context, "🌅 <b>Daily check</b>\n\n" + board)


# ---------------------------------------------------------------------------
# /start, /help
# ---------------------------------------------------------------------------

HELP = """👪 <b>ParentBot</b> — the family's manager.

<b>Watching</b>
/status — every bot: up/down, uptime, host, errors, users (with Refresh /
    Alerts / Ping buttons under it)
/me — ParentBot's own status
/events [bot] [n] — recent crashes, startups, payments
/alerts on|off — mute or unmute unprompted alerts

<b>Reaching into a bot</b>
/run &lt;bot&gt; &lt;command&gt; [args] — the general form; run it bare for the list
/ping [bot] — no bot named pings all of them
/errors &lt;bot&gt; — that bot's errors since it started
/logs &lt;bot&gt; [n] — tail its errors.log ("bot" at the end for bot.log)
/whois &lt;bot&gt; &lt;user_id&gt; — look someone up through that bot
/say &lt;bot&gt; &lt;user_id&gt; &lt;text&gt; — DM someone <i>as</i> that bot
/dbdump &lt;bot&gt; — that bot's own tables as CSVs
/restart &lt;bot&gt; — restart its process

<b>Across the whole family</b>
/users [hours] — active users per bot, default 24h
/donations — paid donations per bot
/sql &lt;SELECT …&gt; — read-only query on the shared database
/backup — the entire database as one zip of CSVs

Bots: <code>{bots}</code>
Any unambiguous prefix works — <code>/logs stick</code> is fine."""


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update, context):
        return
    await update.message.reply_text(HELP.format(bots=bot_list_hint()), parse_mode=ParseMode.HTML)


async def unknown_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update, context):
        return
    await update.message.reply_text("Not a command I have. /help lists them.")


async def plain_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update, context):
        return
    await update.message.reply_text("I only take commands here. /help lists them.")


async def crashtest_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Same escape hatch every other bot has: proves the crash path, the
    errors.log, and the family alert all really fire."""
    if not await guard(update, context):
        return
    raise RuntimeError("Manual /crashtest trigger -- ParentBot's error tracking works.")


BOT_COMMANDS = [
    BotCommand("status", "every bot: up/down, uptime, errors"),
    BotCommand("me", "ParentBot's own status"),
    BotCommand("ping", "ping one bot, or all of them"),
    BotCommand("run", "run a command inside another bot"),
    BotCommand("errors", "a bot's errors since it started"),
    BotCommand("logs", "tail a bot's log"),
    BotCommand("whois", "look a user up through a bot"),
    BotCommand("say", "DM someone as one of the bots"),
    BotCommand("dbdump", "one bot's tables as CSVs"),
    BotCommand("restart", "restart a bot's process"),
    BotCommand("users", "active users per bot"),
    BotCommand("donations", "paid donations per bot"),
    BotCommand("events", "recent crashes / startups / payments"),
    BotCommand("sql", "read-only query on the shared database"),
    BotCommand("backup", "whole database as one zip"),
    BotCommand("alerts", "mute or unmute alerts"),
    BotCommand("help", "what all of this does"),
]


async def _post_init(application):
    await tune_runtime(application)
    await application.bot.set_my_commands(BOT_COMMANDS)


def main():
    if not BOT_TOKEN or not BOT_USERNAME:
        raise SystemExit("Set PBOT_TOKEN and PBOT_USERNAME environment variables first.")
    if not ADMIN_IDS:
        # Every other bot treats an empty admin list as "disable the admin
        # commands". Here that would leave the entire bot open, so it is a
        # hard stop instead.
        raise SystemExit(
            "PBOT_ADMIN_ID is empty. ParentBot is owner-only by definition and "
            "refuses to start without knowing who the owner is -- put your numeric "
            "Telegram id there (get it from @userinfobot)."
        )

    init_db()

    app = (
        ApplicationBuilder().token(BOT_TOKEN)
        .post_init(_post_init).post_stop(flush_on_shutdown).build()
    )
    app.add_error_handler(error_handler)
    app.add_handler(TypeHandler(Update, track_activity), group=-1)

    app.add_handler(CommandHandler(["start", "help"], start_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("me", me_command))
    app.add_handler(CommandHandler("run", run_command))
    app.add_handler(CommandHandler("ping", broadcast_ping))
    app.add_handler(CommandHandler("users", users_command))
    app.add_handler(CommandHandler("donations", donations_command))
    app.add_handler(CommandHandler("events", events_command))
    app.add_handler(CommandHandler("sql", sql_command))
    app.add_handler(CommandHandler("backup", backup_command))
    app.add_handler(CommandHandler("alerts", alerts_command))
    app.add_handler(CommandHandler("crashtest", crashtest_command))

    app.add_handler(CommandHandler("errors", _shortcut("errors", 1, "Usage: /errors <bot>")))
    app.add_handler(CommandHandler("logs", _shortcut("logs", 1, "Usage: /logs <bot> [lines] [bot|all]")))
    app.add_handler(CommandHandler("dbdump", _shortcut("dbdump", 1, "Usage: /dbdump <bot> — or /backup for everything")))
    app.add_handler(CommandHandler("restart", _shortcut("restart", 1, "Usage: /restart <bot>")))
    app.add_handler(CommandHandler("whois", _shortcut("whois", 2, "Usage: /whois <bot> <user_id>")))
    app.add_handler(CommandHandler("say", _shortcut("message", 3, "Usage: /say <bot> <user_id> <text>")))

    app.add_handler(CallbackQueryHandler(board_button, pattern=r"^board:"))
    app.add_handler(MessageHandler(filters.COMMAND, unknown_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, plain_text))

    if app.job_queue is None:
        # Every other bot degrades quietly without one (family_link just stays
        # off). ParentBot cannot: the watchdog, the alert pump and the command
        # results are all jobs, and without them it would sit there looking
        # healthy while watching nothing at all.
        raise SystemExit(
            "No JobQueue available, which means no watchdog, no alerts and no /run "
            'results -- ParentBot would be a shell. Install it with:\n'
            '    pip install "python-telegram-bot[job-queue]"'
        )

    # Before the watchdog's first pass, so the roll-call is what seeds
    # family.bot_state and the watchdog has nothing left to announce.
    app.job_queue.run_once(startup_rollcall, when=STARTUP_ROLLCALL_SECONDS)
    app.job_queue.run_repeating(watchdog, interval=60, first=20)
    app.job_queue.run_repeating(event_pump, interval=20, first=10)
    app.job_queue.run_repeating(result_pump, interval=3, first=5)
    if DIGEST_AT_UTC:
        hour, _, minute = DIGEST_AT_UTC.partition(":")
        app.job_queue.run_daily(
            daily_digest, time(int(hour), int(minute or 0), tzinfo=timezone.utc)
        )
        logger.info("Daily digest scheduled for %s UTC.", DIGEST_AT_UTC)

    # ParentBot rides the same bus it runs: it heartbeats like everyone else,
    # so a second ParentBot (or a plain SQL query) can see whether it is alive.
    family_link.attach(app, BOT_NAME, DISPLAY_NAME, START_TIME)
    attach_maintenance(app)

    logger.info("ParentBot starting (polling). Watching: %s", bot_list_hint())
    # A 30-second long poll is the same latency as the default 10 -- Telegram
    # answers the moment an update exists -- for a third of the HTTP requests.
    # ParentBot has one user, so nearly every request it makes all day is an
    # empty poll.
    app.run_polling(
        timeout=POLL_TIMEOUT,
        allowed_updates=[Update.MESSAGE, Update.CALLBACK_QUERY],
    )


if __name__ == "__main__":
    main()
