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
import json
import logging
import os
import re
import time as time_module
from collections import OrderedDict
from itertools import takewhile
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
import lifecycle
from live_message import LiveMessage, edit_in_place
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

# How ParentBot collects command results and child-bot events: the same
# adaptive poll the child bots use for the command queue (family_link._bus_tick).
# One fast tick decides each time whether to sweep -- every FAST interval while
# the bus is active (family_link.bus_is_active(), which ParentBot sets the
# moment it queues a command), every SLOW interval once it has gone quiet.
RESULT_POLL_SECONDS = int(os.environ.get("PBOT_RESULT_POLL_SECONDS", "20"))
EVENT_POLL_SECONDS = int(os.environ.get("PBOT_EVENT_POLL_SECONDS", "30"))
RESULT_POLL_FAST_SECONDS = int(os.environ.get("PBOT_RESULT_POLL_FAST_SECONDS", "1"))
EVENT_POLL_FAST_SECONDS = int(os.environ.get("PBOT_EVENT_POLL_FAST_SECONDS", "2"))
# How often the pumps tick to make that decision.
PUMP_TICK_SECONDS = int(os.environ.get("PBOT_PUMP_TICK_SECONDS", "1"))

# A redeploy makes a bot's heartbeat stale exactly the way a crash does, and
# for the first minute of it there is no way to tell them apart from the
# outside. So the inside says: a bot that is shut down on purpose leaves a
# note in family.settings on its way out (lifecycle.mark_expected_restart),
# and a stale heartbeat with a fresh note next to it is reported as a
# redeploy rather than as a failure. If it is still missing when the note has
# gone stale, the ordinary "it is down" alert fires after all -- a deploy
# that never came back is exactly the thing worth being told about.
REDEPLOY_GRACE_SECONDS = int(os.environ.get("PBOT_REDEPLOY_GRACE_SECONDS", "300"))

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


# The one place a name that is not the family name is allowed to exist.
#
# Every layer of this project derives from one canonical id -- see the naming
# table in ARCHITECTURE.md -- with exactly one exception: the Telegram
# @username. Two of those were chosen before the family names settled and
# cannot simply be brought into line, because a username is not a label but
# an address:
#
#   AnonBot is @mumu_chat_bot, and every inbox link anybody has ever posted
#   is https://t.me/mumu_chat_bot?start=q_<token>. Changing it does not
#   rename the bot, it breaks every one of those links, permanently, with no
#   redirect and no way to find the people holding them. That username is
#   frozen for as long as the bot has users.
#
#   ParentBot is @mumu_manager_bot, which is only an inconsistency -- it is
#   private, it has one user, and nobody has a saved link to it. That one is
#   safe to change in @BotFather whenever it is convenient, and DEPLOY.md
#   says so.
#
# So rather than pretend the mismatch is not there, it is written down here
# and every form resolves. `/logs chat`, `/logs anon`, `/logs anonbot` and
# `/logs @mumu_chat_bot` all reach the same bot.
USERNAME_ALIASES = {
    "stickerbot": ["mumu_sticker_bot"],
    "convertbot": ["mumu_convert_bot"],
    "downloaderbot": ["mumu_downloader_bot"],
    "anonbot": ["mumu_chat_bot", "chat", "chatbot"],
    "parentbot": ["mumu_manager_bot", "manager", "managerbot"],
}


def _names_of(bot: dict) -> list[str]:
    return [bot["id"], bot["name"].lower(), bot["schema"], *USERNAME_ALIASES.get(bot["id"], [])]


def resolve_bot(token: str) -> dict | None:
    """Accepts "sticker", "stickerbot", "StickerBot", "sticker_bot",
    "@mumu_sticker_bot" -- any unambiguous prefix of the id, the display
    name, the schema, or the Telegram username."""
    token = token.strip().lower().lstrip("@")
    if not token:
        return None
    for bot in ALL_BOTS:
        if token in _names_of(bot):
            return bot
    matches = [b for b in ALL_BOTS if any(n.startswith(token) for n in _names_of(b))]
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
    # live_message rather than edit_text: it swallows "message is not
    # modified" (which the timestamp in the footer makes rare anyway), and it
    # moves the board down to the bottom of the chat if anything has been
    # said since the buttons were tapped.
    await edit_in_place(query.message, context.bot, board,
                        parse_mode=ParseMode.HTML, reply_markup=_board_keyboard())


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

# ---------------------------------------------------------------------------
# Asking an older bot for something it has never heard of
# ---------------------------------------------------------------------------
# The bots deploy independently and are meant to: publish.ps1 takes -Skip so
# a busy bot can be held back, and a bot on an older version answers an
# unknown family command with "Unknown command" rather than failing. That is
# the right behaviour and it stays.
#
# What it is not is a good explanation. ParentBot is always the first thing
# to ship (it is private, so it costs nobody anything), which means it is
# routinely a version or two ahead of the bots it is asking. Typing
# /providers and being told "Unknown command 'providers'" says the bot is
# broken; the truth is that DownloaderBot has not been published since the
# command was written.
#
# ParentBot already knows every bot's version -- it is in the heartbeat and
# on the /status board. So it can say which it is, before the round trip.
# Anything not listed here has been in the family since before the versions
# were tracked, and is never blocked.
COMMAND_SINCE = {
    "probe": "1.2.1",
    "providers": "1.2.3",
    "stars": "1.2.3",
    "crashtest": "1.2.3",
}


def _version_key(version: str | None):
    """A sortable form of a family version, or None if it cannot be read.

    Tolerant on purpose. It has to cope with `1.2.2R` (a retouch release
    sorts after the plain patch), with `test` (what testbot/run.ps1 sets),
    and with whatever a future release invents -- and the cost of getting it
    wrong is refusing a command that would have worked. So anything it
    cannot parse reads as None, and None never blocks.
    """
    if not version:
        return None
    parts = []
    for chunk in str(version).strip().split("."):
        digits = "".join(takewhile(str.isdigit, chunk))
        if not digits:
            return None
        # The suffix orders after the bare number: 1.2.2 < 1.2.2R.
        parts.append((int(digits), chunk[len(digits):]))
    return tuple(parts)


# A flag can be as new as a command, and is more dangerous: an unknown
# command is refused, while an unknown flag is just part of the message. A
# 1.2.x bot handed `broadcast --active hello` would send every user it has
# ever seen a message beginning "--active".
FLAG_SINCE = {("broadcast", "--active"): "1.3.0"}


def _too_old_for(bot_version: str | None, command: str, args: list[str] | None = None) -> str | None:
    """The sentence to show instead of queueing, or None to go ahead."""
    needed = COMMAND_SINCE.get(command)
    for (cmd, flag), since in FLAG_SINCE.items():
        if cmd == command and args and flag in args:
            needed = since
            command = f"{command} {flag}"
            break
    if not needed:
        return None
    have, want = _version_key(bot_version), _version_key(needed)
    if have is None or want is None or have >= want:
        return None
    return (f"That bot is on <b>{html.escape(str(bot_version))}</b> and "
            f"<code>{html.escape(command)}</code> arrived in <b>{needed}</b>, so it has "
            f"never heard of it — nothing is broken, it just has not been published "
            f"since.\n\nShip it with <code>.\\publish.ps1 -Only {{dir}}</code>, then try again.")


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

    # Asked before the round trip rather than after it, so a version gap
    # costs one lookup instead of ninety seconds and a confusing answer.
    beat = await asyncio.to_thread(db.heartbeat_of, bot["id"])
    stale = _too_old_for((beat or {}).get("version"), command, args)
    if stale:
        await update.message.reply_text(
            f"⏳ <b>{html.escape(bot['name'])}</b> · {html.escape(command)}\n\n"
            + stale.replace("{dir}", bot["schema"]),
            parse_mode=ParseMode.HTML,
        )
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
        # DownloaderBot registers one command of its own (see its bot.py), so
        # it is not in this process's COMMAND_HELP. Listed by hand rather than
        # left undiscoverable, since /run will happily dispatch it.
        lines.append("  <code>probe</code> — DownloaderBot only: try every "
                     "download route from inside its container")
        await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)
        return

    bot = resolve_bot(context.args[0])
    if not bot:
        await update.message.reply_text(f"No such bot: {context.args[0]}. Known: {bot_list_hint()}")
        return
    await _dispatch(update, context, bot, context.args[1].lower(), context.args[2:])


def _shortcut(command: str, min_args: int, usage: str, default_bot: str | None = None):
    """Builds /whois, /say, /logs and friends -- each is /run with the
    command fixed and the first argument still naming the target bot.

    `default_bot` is for the commands only one bot has. /providers and
    /stars are not ambiguous, so making the owner type which bot owns them
    is asking them to remember something this file already knows. Naming the
    bot still works, and still wins, for when that stops being true.
    """
    async def handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
        if not await guard(update, context):
            return
        args = list(context.args)
        bot = resolve_bot(args[0]) if args else None
        if bot is not None:
            args = args[1:]
        elif default_bot:
            bot = resolve_bot(default_bot)
        if bot is None:
            if len(args) < min_args or not args:
                await update.message.reply_text(usage or f"Which bot? Known: {bot_list_hint()}")
            else:
                await update.message.reply_text(
                    f"No such bot: {args[0]}. Known: {bot_list_hint()}")
            return
        if len(args) < max(0, min_args - 1):
            await update.message.reply_text(usage)
            return
        await _dispatch(update, context, bot, command, args)
    return handler


# ---------------------------------------------------------------------------
# /ping -- the whole round trip, leg by leg
# ---------------------------------------------------------------------------
# "Is it up?" was the old question and a word was a fine answer. The useful
# question is "where does the time go?", and answering that means measuring
# five separate hops on two machines whose clocks do not agree:
#
#   the phone -> Telegram -> here     network, plus however long the update
#                                     sat in Telegram's queue
#   here -> Supabase                  the INSERT that queues the command
#   Supabase -> the target bot        how fast it hears about it
#   the target bot                    its own work, including its own hop to
#                                     Supabase
#   Supabase -> here                  the answer coming back
#
# Two machines, one honest clock. Every cross-machine figure below is a
# difference between two Postgres timestamps -- created_at, claimed_at,
# finished_at, clock_timestamp() at pickup -- so none of them contains the
# difference between this host's idea of the time and Railway's. The
# per-machine figures (each end's own round trip to Supabase, and its clock
# skew against it) are measured locally at each end and reported separately,
# which is what makes "the database is slow from there" distinguishable from
# "that bot is slow".
#
# The one figure that is *not* exact is the first: Telegram stamps a message
# with whole seconds, so the phone-to-here leg is only good to about a
# second, and it is labelled that way rather than quietly presented as
# precise.

_PING_TRACES: "OrderedDict[int, dict]" = OrderedDict()
MAX_PING_TRACES = 64


def _remember_trace(command_id: int, trace: dict) -> None:
    _PING_TRACES[command_id] = trace
    while len(_PING_TRACES) > MAX_PING_TRACES:
        _PING_TRACES.popitem(last=False)


def _ms(delta) -> str:
    """A duration, in whichever unit makes it readable. Takes a timedelta
    (the Postgres-clock differences) or a plain number of milliseconds (the
    locally measured legs)."""
    if delta is None:
        return "     ?"
    ms = delta.total_seconds() * 1000 if hasattr(delta, "total_seconds") else float(delta)
    if ms >= 1000:
        return f"{ms / 1000:6.2f} s"
    return f"{ms:6.0f} ms"


def _row(label: str, value: str) -> str:
    return f"{label:<30}{value}"


def _describe_end(name: str, where: str, probe: dict) -> str:
    if not probe or "error" in probe:
        return _row(name, (probe or {}).get("error", "not reported"))
    skew = probe.get("skew_ms", 0)
    bits = [f"Supabase {_ms(probe.get('db_ms', 0)).strip()} per query",
            f"clock {skew:+.0f} ms"]
    return f"{name}\n  {where}\n  " + " · ".join(bits)


def _render_ping(trace: dict, result: dict) -> str:
    """The report. Everything in the first block is one Postgres clock or a
    stopwatch that started and stopped in this process; nothing in it is a
    subtraction across two machines' clocks."""
    name = trace["bot"]["name"]
    created, claimed = result.get("created_at"), result.get("claimed_at")
    finished, taken = result.get("finished_at"), result.get("taken_at")

    if result["status"] == "timeout" or claimed is None:
        return (f"🏓 <b>{html.escape(name)}</b> — no answer in {COMMAND_TIMEOUT_SECONDS}s.\n"
                f"It never picked the ping up, which means its process is not running.")

    there = {}
    try:
        there = json.loads(result.get("output") or "{}")
    except ValueError:
        pass

    lines = [
        _row("you → Telegram → ParentBot", _ms(trace["to_parent"]) + "   ±1 s"),
        _row("ParentBot → Telegram (ack)", _ms(trace["ack_ms"])),
        _row("ParentBot → Supabase (queue)", _ms(trace["queue_ms"])),
        _row(f"queued → {name} claimed it", _ms(claimed - created)),
        _row(f"{name} answering", _ms(finished - claimed)),
        _row("answer → ParentBot collected it", _ms(taken - finished)),
        "─" * 40,
        _row("bus round trip", _ms(taken - created)),
    ]

    # Named for what it is. This is the admin bus -- ParentBot handing a
    # command to another bot through a Postgres table and waiting for the
    # answer to come back. It is not what a user waits for: their message goes
    # to the bot they are talking to directly and never touches this path. The
    # two were easy to confuse while this number was tens of seconds.
    head = (f"🏓 <b>{html.escape(name)}</b> — {_ms(taken - created).strip()} "
            f"round trip on the admin bus")
    ends = "\n\n".join([
        _describe_end("ParentBot", trace["here"].get("where", "?"), trace["here"]),
        _describe_end(name, there.get("where", "?"), there),
    ])
    up = there.get("up")
    tail = f"\nUp {up}." if up else ""
    return (f"{head}\n\n<pre>{html.escape(chr(10).join(lines))}</pre>\n"
            f"<b>Each end</b>\n{html.escape(ends)}{html.escape(tail)}")


async def _ping_one(update: Update, context: ContextTypes.DEFAULT_TYPE, bot: dict) -> None:
    started = time_module.perf_counter()
    now = datetime.now(timezone.utc)
    live = await LiveMessage.reply_to(update.message, f"🏓 {bot['name']} — timing the round trip…")
    ack_ms = (time_module.perf_counter() - started) * 1000

    # This end's own distance from the database, measured the same way the
    # far end measures its own -- the two are only comparable because both
    # are one round trip from the same probe.
    try:
        here = await asyncio.to_thread(family_link.ping_probe)
    except Exception as exc:
        here = {"error": f"{type(exc).__name__}: {exc}"}

    queue_started = time_module.perf_counter()
    command_id = await asyncio.to_thread(
        db.queue_command, bot["id"], "ping", "trace",
        update.effective_user.id, update.effective_chat.id,
    )
    queue_ms = (time_module.perf_counter() - queue_started) * 1000

    _remember_trace(command_id, {
        "bot": bot, "live": live, "here": here,
        "to_parent": now - update.message.date,
        "ack_ms": ack_ms, "queue_ms": queue_ms,
    })


async def broadcast_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/ping <bot> reports the round trip leg by leg. /ping with nothing
    named asks all of them at once and reports one line each -- still the
    fastest way to tell "the database is down" from "one bot is down"."""
    if not await guard(update, context):
        return
    if context.args:
        bot = resolve_bot(context.args[0])
        if not bot:
            await update.message.reply_text(f"No such bot: {context.args[0]}. Known: {bot_list_hint()}")
            return
        await _ping_one(update, context, bot)
        return

    # Everything _render_ping needs is measured here too, per bot, even
    # though the summary shows one number each. That is what lets the
    # buttons under the result open a full leg-by-leg breakdown instantly,
    # from the ping that was actually run, rather than quietly running a
    # second one and showing different numbers than the row above it.
    started = time_module.perf_counter()
    now = datetime.now(timezone.utc)
    live = await LiveMessage.reply_to(update.message, f"🏓 Pinging all {len(CHILDREN)}…")
    ack_ms = (time_module.perf_counter() - started) * 1000
    try:
        here = await asyncio.to_thread(family_link.ping_probe)
    except Exception as exc:
        here = {"error": f"{type(exc).__name__}: {exc}"}

    group = {"live": live, "rows": {}, "expected": len(CHILDREN), "details": {}}
    for child in CHILDREN:
        queue_started = time_module.perf_counter()
        command_id = await asyncio.to_thread(
            db.queue_command, child["id"], "ping", "trace",
            update.effective_user.id, update.effective_chat.id,
        )
        _remember_trace(command_id, {
            "bot": child, "group": group, "here": here,
            "to_parent": now - update.message.date, "ack_ms": ack_ms,
            "queue_ms": (time_module.perf_counter() - queue_started) * 1000,
        })


# ---------------------------------------------------------------------------
# The buttons under a family ping
# ---------------------------------------------------------------------------
# /ping with no bot named used to end at four numbers, and getting the
# breakdown for the one that looked wrong meant typing /ping <bot> and
# waiting for a second round trip. Four commands to see four bots.
#
# The results of the ping that just ran are already in memory, so the
# breakdown costs nothing to show: one button per bot, plus one that prints
# all four. Kept per live message rather than globally, so two pings in the
# same chat do not overwrite each other's buttons.

_PING_GROUPS: "OrderedDict[tuple[int, int], dict]" = OrderedDict()
MAX_PING_GROUPS = 16


def _remember_group(group: dict) -> None:
    live = group["live"]
    _PING_GROUPS[(live.chat_id, live.message_id)] = group
    while len(_PING_GROUPS) > MAX_PING_GROUPS:
        _PING_GROUPS.popitem(last=False)


def _ping_keyboard(group: dict) -> InlineKeyboardMarkup:
    live = group["live"]
    key = f"{live.chat_id}:{live.message_id}"
    buttons = [
        InlineKeyboardButton(child["name"], callback_data=f"pingdet:{key}:{child['id']}")
        for child in CHILDREN if child["id"] in group["details"]
    ]
    rows = [buttons[i:i + 2] for i in range(0, len(buttons), 2)]
    rows.append([InlineKeyboardButton("📋 All details", callback_data=f"pingdet:{key}:*")])
    return InlineKeyboardMarkup(rows)


async def ping_detail_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """A tap on one of those buttons. Renders from what the ping already
    collected, so the detail agrees with the summary above it by
    construction -- a second round trip would give different numbers and
    invite the question of which of the two is real."""
    if not await guard(update, context):
        return
    query = update.callback_query
    _, chat_id, message_id, which = query.data.split(":", 3)
    group = _PING_GROUPS.get((int(chat_id), int(message_id)))
    if group is None:
        # Only after a redeploy, or sixteen pings ago. Say so rather than
        # showing an empty report.
        await query.answer("That ping is no longer in memory — run /ping again.",
                           show_alert=True)
        return

    wanted = [c for c in CHILDREN
              if (which == "*" or which == c["id"]) and c["id"] in group["details"]]
    if not wanted:
        await query.answer("Nothing was recorded for that one.", show_alert=True)
        return
    await query.answer()

    body = "\n\n".join(_render_ping(*group["details"][c["id"]]) for c in wanted)
    if len(body) <= TELEGRAM_MAX_CHARS:
        await context.bot.send_message(chat_id=query.message.chat_id, text=body,
                                       parse_mode=ParseMode.HTML)
        return
    # Four full reports can outgrow one message. The file is plain text, so
    # the markup is stripped rather than shown as tags.
    await send_long(context, query.message.chat_id,
                    re.sub(r"<[^>]+>", "", body), filename="ping.txt")


async def _deliver_ping(context: ContextTypes.DEFAULT_TYPE, trace: dict, result: dict) -> None:
    """A ping's answer replaces the message that announced it, rather than
    arriving underneath -- and moves to the bottom of the chat by itself if
    anything was said in the meantime (see live_message.py)."""
    group = trace.get("group")
    if group is None:
        await trace["live"].set(context.bot, _render_ping(trace, result), parse_mode=ParseMode.HTML)
        return

    name = trace["bot"]["name"]
    if result["status"] == "timeout" or not result.get("claimed_at"):
        group["rows"][name] = "no answer — down"
    else:
        group["rows"][name] = _ms(result["taken_at"] - result["created_at"]).strip()
    # Kept whole, so the buttons below can print the breakdown without
    # asking the bot anything a second time.
    group["details"][trace["bot"]["id"]] = (trace, result)
    lines = [f"{n:<14}{v}" for n, v in sorted(group["rows"].items())]
    missing = group["expected"] - len(group["rows"])
    text = f"🏓 <b>Family ping</b>\n<pre>{html.escape(chr(10).join(lines))}</pre>"
    keyboard = None
    if missing > 0:
        text += f"\nWaiting on {missing} more (down after {COMMAND_TIMEOUT_SECONDS}s)."
    else:
        text += ("\nAdmin round trip through Postgres — not what a user waits for; "
                 "their message never takes this path. Tap a bot for its breakdown.")
        _remember_group(group)
        keyboard = _ping_keyboard(group)
    await group["live"].set(context.bot, text, parse_mode=ParseMode.HTML,
                            reply_markup=keyboard)


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
        await edit_in_place(note, context.bot, f"⚠️ Export failed: {exc}")
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


async def _is_redeploying(bot_id: str) -> bool:
    """True while a bot's own goodbye note is still fresh."""
    key = lifecycle.RESTART_NOTE_PREFIX + bot_id
    try:
        raw = await asyncio.to_thread(db.get_setting, key)
        if not raw:
            return False
        stamped = datetime.fromisoformat(raw)
    except Exception:
        return False
    age = (datetime.now(timezone.utc) - stamped).total_seconds()
    return 0 <= age <= REDEPLOY_GRACE_SECONDS


async def _clear_redeploy_note(bot_id: str) -> None:
    try:
        await asyncio.to_thread(db.set_setting, lifecycle.RESTART_NOTE_PREFIX + bot_id, "")
    except Exception:
        logger.debug("Could not clear the redeploy note for %s", bot_id, exc_info=True)


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

        if is_up:
            await asyncio.to_thread(db.set_known_state, bot["id"], True)
            await _clear_redeploy_note(bot["id"])
            uptime = format_delta((datetime.now(timezone.utc) - beat["started_at"]).total_seconds())
            await notify_owner(
                context,
                f"✅ <b>{bot['name']}</b> is back up (started {uptime} ago, "
                f"on {html.escape(beat['host'] or '?')})."
            )
        elif await _is_redeploying(bot["id"]):
            # Deliberate, and recent. Leave known_state alone so that the
            # "it is back" message is not announced either -- a redeploy the
            # owner started should be silent at both ends. If it does not
            # come back, the note ages out and the next pass alerts.
            logger.info("%s is stale but was shut down on purpose -- redeploying.", bot["id"])
            continue
        else:
            await asyncio.to_thread(db.set_known_state, bot["id"], False)
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

    if events:
        # A crashing bot tends to report several things at once; stay on the
        # fast cadence long enough to catch the rest.
        family_link.mark_bus_active()

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


# How long ago each pump actually swept. The interval is chosen from whether
# the bus is active (family_link.bus_is_active(), set when ParentBot queues a
# command): fast while an answer could be coming back, slow once things have
# gone quiet -- the same adaptive poll the child bots run for the command
# queue, and the whole delivery mechanism now that there is no push.
_last_pumped = {"result": 0.0, "event": 0.0}


async def _pump_if_due(context, key: str, pump, fast: int, idle: int) -> None:
    due = fast if family_link.bus_is_active() else idle
    now = time_module.monotonic()
    if now - _last_pumped[key] < due:
        return
    _last_pumped[key] = now
    await pump(context)


async def _result_backstop(context: ContextTypes.DEFAULT_TYPE) -> None:
    await _pump_if_due(context, "result", result_pump,
                       RESULT_POLL_FAST_SECONDS, RESULT_POLL_SECONDS)


async def _event_backstop(context: ContextTypes.DEFAULT_TYPE) -> None:
    await _pump_if_due(context, "event", event_pump,
                       EVENT_POLL_FAST_SECONDS, EVENT_POLL_SECONDS)


async def result_pump(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Delivers whatever the child bots wrote back, and gives up on commands
    nobody claimed."""
    try:
        results = await asyncio.to_thread(db.take_finished_commands, 5)
    except Exception:
        return

    if results:
        # More may still be trickling in (a /ping to everything answers over
        # a few seconds); keep sweeping at the fast cadence for a bit.
        family_link.mark_bus_active()

    for result in results:
        # A /ping is answered by rewriting the message that announced it,
        # with the whole round trip broken down -- not by a second message
        # underneath. Checked before reply_chat_id because the trace already
        # holds the message it is going to become.
        trace = _PING_TRACES.pop(result["id"], None)
        if trace is not None:
            await _deliver_ping(context, trace, result)
            continue

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
        # The pre-flight check in _dispatch catches the version gap for every
        # command in COMMAND_SINCE. This catches the rest: a command added to
        # the bus without being listed there, or a bot whose version string
        # could not be read. "Unknown command" on its own reads as a fault;
        # with the two version numbers beside it, it reads as a deploy.
        if not result["ok"] and body.startswith("Unknown command"):
            beat = await asyncio.to_thread(db.heartbeat_of, result["target_bot"])
            theirs = (beat or {}).get("version")
            if theirs and theirs != family_link.VERSION:
                body += (f"\n\nThat bot is on {theirs}; ParentBot is on "
                         f"{family_link.VERSION}. Most likely it has not been "
                         f"published since the command was written.")
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
/crashtest &lt;bot&gt; — make it raise on purpose, to check the alert arrives
    (no bot named crashes ParentBot itself)
/providers [downloader] — which download route is working, which is resting
/probe &lt;downloader&gt; [platform] — actively try every route, now
/stars [convert] — the Stars ledger: paid, free and refunded

Every owner-only command any bot has is reachable from here. If one is
missing, that is a bug — see family_link.COMMANDS.

<b>Shipping an update</b>
/pause [bot|all] [minutes] — stop them taking work an update would lose;
    whoever asks is told to come back, and written down
/warn [bot|all] — tell whoever is mid-something that it is about to reset
/finishupdates [bot|all] — reopen, and tell everyone who was turned away
    (yours to say, so one update can be as many deploys as it needs)
/broadcast &lt;bot|all&gt; &lt;text&gt; — one message to everyone, <i>as</i> that bot

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
    """Proves the crash path, the errors.log and the family alert all really
    fire -- for ParentBot itself, or for any child bot by name.

    Naming a bot is the more useful direction and the one that was missing.
    ParentBot crashing tells you ParentBot's own reporting works, which you
    can see happening in front of you. What you actually want to know, when
    a bot has gone quiet, is whether *that* bot would have told you -- and
    that is the one test you could not run from here.
    """
    if not await guard(update, context):
        return
    if context.args:
        bot = resolve_bot(context.args[0])
        if not bot:
            await update.message.reply_text(
                f"No such bot: {context.args[0]}. Known: {bot_list_hint()}")
            return
        await _dispatch(update, context, bot, "crashtest", [])
        return
    raise RuntimeError("Manual /crashtest trigger -- ParentBot's error tracking works.")


# ---------------------------------------------------------------------------
# Running an update without ambushing anybody
# ---------------------------------------------------------------------------
# Four commands, in the order they are meant to be used:
#
#   /pause           the bots stop starting work a restart would throw away,
#                    and tell whoever asks how long they expect to be. Anyone
#                    turned away is written down.
#   /warn            everyone already mid-something hears that it is about to
#                    be reset -- before the deploy, not as the door closes.
#   ...deploy, as many times as it takes...
#   /finishupdates   the bots reopen, and everyone who was turned away is
#                    told they can try again.
#
# /broadcast is the odd one out and belongs here anyway: it is the same
# "speak as the bot they actually talk to" mechanism, for anything the owner
# wants to say that these four sentences do not cover.
#
# Nothing here is on a timer. An update is usually several deploys, and a
# pause that expired on its own would let the bots reopen between two of
# them -- which is the exact window this is meant to close.

def _targets(token: str | None) -> list[dict] | None:
    """The bots a command applies to. No name, or "all", means every child --
    ParentBot is never a target: it is the one doing the asking, and it has
    no users to announce anything to."""
    if not token or token.lower() == "all":
        return list(CHILDREN)
    bot = resolve_bot(token)
    if not bot or bot["id"] == BOT_NAME:
        return None
    return [bot]


async def _fan_out(update: Update, context: ContextTypes.DEFAULT_TYPE,
                   targets: list[dict], command: str, args: str, headline: str) -> None:
    """Queue one family command to several bots and say so once.

    Deliberately not a progress report: each bot answers in its own time and
    its answer arrives on its own, the way every other /run result does.
    """
    for bot in targets:
        await asyncio.to_thread(
            db.queue_command, bot["id"], command, args,
            update.effective_user.id, update.effective_chat.id,
        )
    names = ", ".join(b["name"] for b in targets)
    await update.message.reply_text(f"{headline}\n→ {names}")


async def pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/pause [bot|all] [minutes] — stop taking new long work, and say why."""
    if not await guard(update, context):
        return
    args = list(context.args)
    minutes = None
    if args and args[-1].isdigit():
        minutes = args.pop()
    targets = _targets(args[0] if args else None)
    if targets is None:
        await update.message.reply_text(
            f"Usage: /pause [bot|all] [minutes]. Known: {bot_list_hint()}"
        )
        return
    promised = minutes or str(lifecycle.DEFAULT_MAINTENANCE_MINUTES)
    await _fan_out(
        update, context, targets, "pause", minutes or "",
        f"⏸ Paused — telling users to come back in about {promised} minute(s).\n"
        f"They stay paused until /finishupdates, however many deploys that takes.",
    )


async def warn_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/warn [bot|all] — tell whoever is mid-something that it will be reset."""
    if not await guard(update, context):
        return
    targets = _targets(context.args[0] if context.args else None)
    if targets is None:
        await update.message.reply_text(f"Usage: /warn [bot|all]. Known: {bot_list_hint()}")
        return
    await _fan_out(
        update, context, targets, "warnbusy", "",
        "📣 Warning everyone with work in flight that it is about to be reset.",
    )


async def finish_updates_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/finishupdates [bot|all] — reopen, and go back to everyone turned away.

    Explicitly the owner's call and never automatic: one update is usually
    several deploys, and only the person doing them knows which one was the
    last.
    """
    if not await guard(update, context):
        return
    targets = _targets(context.args[0] if context.args else None)
    if targets is None:
        await update.message.reply_text(
            f"Usage: /finishupdates [bot|all]. Known: {bot_list_hint()}"
        )
        return
    await _fan_out(
        update, context, targets, "resume", "",
        "✅ Reopening — everyone who was turned away is being told they can try again.",
    )


# ---- /broadcast, which asks before it speaks ------------------------------
# Every other command here affects people who are already mid-something with
# a bot. This one reaches everyone the bot has ever met, cannot be recalled,
# and is one fat-fingered bot name away from going to the wrong audience. So
# it is the one command in ParentBot that asks twice.

BROADCAST_PENDING = "broadcast_pending"
BROADCAST_PREVIEW = 3000


async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/broadcast <bot|all> <text> — one message to everyone, as that bot."""
    if not await guard(update, context):
        return
    if len(context.args) < 2:
        await update.message.reply_text(
            f"Usage: /broadcast &lt;bot|all&gt; &lt;text&gt;\nKnown: {bot_list_hint()}",
            parse_mode=ParseMode.HTML,
        )
        return
    targets = _targets(context.args[0])
    if targets is None:
        await update.message.reply_text(
            f"No such bot: {context.args[0]}. Known: {bot_list_hint()}"
        )
        return
    text = " ".join(context.args[1:]).strip()
    if not text:
        await update.message.reply_text("Nothing to say — give it some text.")
        return

    context.user_data[BROADCAST_PENDING] = {
        "bots": [b["id"] for b in targets],
        "text": text,
    }
    names = ", ".join(b["name"] for b in targets)
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("Send it", callback_data="upd:send"),
        InlineKeyboardButton("Cancel", callback_data="upd:cancel"),
    ]])
    await update.message.reply_text(
        f"📢 <b>Send as {html.escape(names)}, to everyone they know?</b>\n\n"
        f"<pre>{html.escape(text[:BROADCAST_PREVIEW])}</pre>\n"
        f"This cannot be taken back.",
        parse_mode=ParseMode.HTML, reply_markup=kb,
    )


async def broadcast_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await guard(update, context):
        return
    pending = context.user_data.pop(BROADCAST_PENDING, None)
    if query.data == "upd:cancel" or not pending:
        await query.answer()
        await edit_in_place(
            query.message, context.bot,
            "Cancelled — nothing was sent." if pending else
            "That broadcast is no longer waiting. Send /broadcast again.",
        )
        return

    await query.answer()
    sent_to = []
    for bot_id in pending["bots"]:
        await asyncio.to_thread(
            db.queue_command, bot_id, "broadcast", pending["text"],
            update.effective_user.id, update.effective_chat.id,
        )
        sent_to.append(bot_id)
    await edit_in_place(
        query.message, context.bot,
        f"📢 Queued to {', '.join(sent_to)}. Each one reports back when it has "
        f"finished going through its list.",
    )


BOT_COMMANDS = [
    BotCommand("status", "every bot: up/down, uptime, errors"),
    BotCommand("me", "ParentBot's own status"),
    BotCommand("ping", "ping one bot, or all of them"),
    BotCommand("run", "run a command inside another bot"),
    BotCommand("errors", "a bot's errors since it started"),
    BotCommand("logs", "tail a bot's log"),
    BotCommand("whois", "look a user up through a bot"),
    BotCommand("say", "DM someone as one of the bots"),
    BotCommand("broadcast", "message everyone, as one of the bots"),
    BotCommand("pause", "stop the bots taking work an update would lose"),
    BotCommand("warn", "tell whoever is mid-something that it will reset"),
    BotCommand("finishupdates", "reopen, and tell everyone who was waiting"),
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
    await lifecycle.on_start(BOT_NAME)
    await application.bot.set_my_commands(BOT_COMMANDS)


async def _post_stop(application):
    await lifecycle.on_stop(application)
    await flush_on_shutdown(application)


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

    builder = (
        ApplicationBuilder().token(BOT_TOKEN)
        .post_init(_post_init).post_stop(_post_stop)
    )
    state = lifecycle.persistence()
    if state is not None:
        builder = builder.persistence(state)
    app = builder.build()
    lifecycle.install(app, BOT_NAME)
    app.add_error_handler(error_handler)
    # Its own group -- see the note in the child bots' main(): a TypeHandler
    # on Update matches everything, so anything sharing a group with it never
    # runs. ParentBot has nothing else up here today; the numbering is what
    # keeps that true when it does.
    app.add_handler(TypeHandler(Update, track_activity), group=-3)

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
    # The bot-specific ones. Each defaults to the only bot that has it, so
    # the command centre does not make you remember which bot owns what --
    # /providers and /stars are unambiguous, and typing the bot name is
    # still allowed for when that stops being true.
    app.add_handler(CommandHandler("providers", _shortcut("providers", 0, "", default_bot="downloaderbot")))
    app.add_handler(CommandHandler("probe", _shortcut("probe", 0, "", default_bot="downloaderbot")))
    app.add_handler(CommandHandler("stars", _shortcut("stars", 0, "", default_bot="convertbot")))

    app.add_handler(CommandHandler("broadcast", broadcast_command))
    app.add_handler(CommandHandler("pause", pause_command))
    app.add_handler(CommandHandler("warn", warn_command))
    app.add_handler(CommandHandler("finishupdates", finish_updates_command))

    app.add_handler(CallbackQueryHandler(board_button, pattern=r"^board:"))
    app.add_handler(CallbackQueryHandler(ping_detail_button, pattern=r"^pingdet:"))
    app.add_handler(CallbackQueryHandler(broadcast_button, pattern=r"^upd:"))
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
    # ParentBot collects command results and child-bot events on the same
    # adaptive poll the child bots run for the command queue: a fast tick that
    # decides each time whether to sweep -- every *_POLL_FAST_SECONDS while the
    # bus is active (set the moment ParentBot queues a command), every
    # *_POLL_SECONDS once it has gone quiet. There is no push behind it, so
    # there is nothing to be "down": a pump that is late is late by one idle
    # interval, not by the life of the process.
    app.job_queue.run_repeating(_event_backstop, interval=PUMP_TICK_SECONDS, first=10)
    app.job_queue.run_repeating(_result_backstop, interval=PUMP_TICK_SECONDS, first=5)

    # One sweep of each at startup -- however late the job queue actually
    # starts (see family_link.RUN_LATE) -- to clear anything a previous run
    # left queued before the first tick falls due.
    app.job_queue.run_once(result_pump, when=0, job_kwargs=family_link.RUN_LATE)
    app.job_queue.run_once(event_pump, when=0, job_kwargs=family_link.RUN_LATE)
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
    app.run_polling(**lifecycle.polling_kwargs(
        timeout=POLL_TIMEOUT,
        allowed_updates=[Update.MESSAGE, Update.CALLBACK_QUERY],
    ))


if __name__ == "__main__":
    main()
