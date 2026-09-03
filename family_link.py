"""The family bus: how one bot talks to ParentBot, and how ParentBot talks
back.

Every bot in the family (the four public ones and ParentBot itself) keeps
this file, byte-identical, exactly like shared_features.py -- the bots stay
independent processes with independent repos, so nothing is imported across
folders. What they *do* share is one Postgres database, and this module is
the only thing that touches the parts of it that aren't a single bot's own.

Layout of that shared database:

    family.*        this file's tables -- heartbeats, events, command queue
    sticker_bot.*   StickerBot's own tables (its db.py, unchanged)
    convert_bot.*   ConvertBot's own tables
    downloader_bot.*
    anon_bot.*
    parent_bot.*    ParentBot's own tables

One Postgres schema per bot means the four bots' identically-named tables
(user_settings, star_transactions, activity_events, ...) never collide, and
no bot's SQL had to change -- each connects with its own search_path (see
db.py's DB_SCHEMA). ParentBot is the only process that reads across schemas.

Three things flow over this bus:

  1. **Heartbeats** -- every HEARTBEAT_SECONDS each bot stamps
     family.heartbeats with "still alive, started at X, N errors so far".
     ParentBot decides a bot is down when that stamp goes stale, which
     works whether the bot crashed, was OOM-killed, lost its network, or
     was never started at all. No open ports, no HTTP between services.

  2. **Events** -- anything ParentBot should tell the owner about lands in
     family.events (an unhandled exception, a startup, a donation).
     ParentBot polls for undelivered ones and forwards them as a DM.

  3. **Commands** -- ParentBot inserts a row in family.commands aimed at
     one bot; that bot polls for it (fast while the bus is busy, every
     FAMILY_BUS_POLL_IDLE_SECONDS otherwise), runs it, and writes the answer
     back into the same row. This is how ParentBot
     runs another bot's owner-only commands (/status, /dbdump, /whois,
     /messageas, ...) without either process needing to reach the other
     over the network. A command aimed at a bot that is down simply stays
     pending until ParentBot times it out and says so.

Every bot also tidies up after itself here, on a slow timer: its own
finished rows in family.commands (which carry file payloads -- a delivered
/dbdump zip is megabytes of BYTEA nobody will read again), its own delivered
events, and its own activity log. Each bot only ever deletes rows keyed to
itself, so the five processes need no coordination to do it.

Everything here is best-effort by design: if the shared database is
unreachable, the family bus goes quiet but the bot itself keeps serving its
users normally. A monitoring layer must never be able to take down the
thing it monitors.
"""
from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import socket
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import db
import lifecycle

logger = logging.getLogger(__name__)

FAMILY_SCHEMA = "family"

# Bumped with the family's version (see CHANGELOG.md) -- reported in
# heartbeats so /status can show which bots are running stale code after a
# partial deploy.
VERSION = os.environ.get("FAMILY_VERSION", "1.2.2")

HEARTBEAT_SECONDS = int(os.environ.get("FAMILY_HEARTBEAT_SECONDS", "30"))

# ---------------------------------------------------------------------------
# How a bot finds out there is a command waiting for it
# ---------------------------------------------------------------------------
# It polls family.commands for its own pending rows. There is no push.
#
# v1.0.1 added a LISTEN/NOTIFY layer on top of the poll: a dedicated
# psycopg.AsyncConnection per channel, an asyncio task holding it open, its
# own reconnect/backoff. It was meant to turn "picked up within the poll
# interval" into "picked up in milliseconds". In practice it earned its
# reputation as the most fragile code in the family: the job that started it
# was silently misfire-dropped and never ran on any bot for two months
# (v1.1.2), and psycopg's async connection cannot run on Windows' default
# event loop at all, so it never worked on the laptop the bots are developed
# on. Both times the poll underneath carried the whole bus and nobody
# noticed. For a bus with one human operator typing /ping now and then, a
# held connection and 200 lines of reconnect logic bought a few seconds on a
# manual command. v1.1.4 deleted it.
#
# What is left is one poll, made adaptive: fast right after the bus was last
# busy, slow when it has been quiet. mark_bus_active() opens a short fast
# window; anything that expects bus traffic (a command just queued, claimed
# or finished) calls it. The result: a warm bus answers in about a second,
# and a cold one within FAMILY_BUS_POLL_IDLE_SECONDS. It behaves identically
# on a Windows laptop and on Railway, and it has no failure mode short of the
# database itself being unreachable -- which /status shows anyway.
BUS_POLL_ACTIVE_SECONDS = float(os.environ.get("FAMILY_BUS_POLL_ACTIVE_SECONDS", "1"))
BUS_POLL_IDLE_SECONDS = float(os.environ.get("FAMILY_BUS_POLL_IDLE_SECONDS", "10"))
# How long the fast cadence lasts after the last time the bus did something.
BUS_ACTIVE_WINDOW_SECONDS = float(os.environ.get("FAMILY_BUS_ACTIVE_WINDOW_SECONDS", "20"))

# Pass to every job_queue.run_once(..., when=0) in the family.
#
# APScheduler drops a job whose run time has already passed by more than
# misfire_grace_time, which defaults to one second, and python-telegram-bot
# does not override it. A bot's main() stamps "now" before run_polling()
# starts the scheduler -- and between those two moments sits post_init: an
# advisory lock, the waitlist table, the maintenance flag and set_my_commands,
# each a round trip to a database on another continent. Several seconds, every
# time. So a run_once(when=0) job would be scheduled, silently discarded as a
# misfire, and never run (this is exactly how the old LISTEN listener stayed
# dead for two months -- see v1.1.2). `None` means "run it however late it
# is", which for a job that only ever means "do this as soon as the loop is
# up" is the only correct setting.
RUN_LATE = {"misfire_grace_time": None}

# Set by attach(); everything below no-ops until then.
_bot_id: str | None = None
_display_name: str | None = None
_start_time: datetime | None = None
_enabled = False


# The host name never changes while the process runs, and gethostname() is a
# syscall -- worth doing once rather than on every heartbeat.
HOSTNAME = socket.gethostname()

# How long finished command rows and delivered events are kept before this bot
# tidies up after itself. The command queue carries BYTEA payloads (a /dbdump
# zip on its way to ParentBot), so letting it grow forever means paying to
# store megabytes of files that were already delivered.
COMMAND_RETENTION_HOURS = int(os.environ.get("FAMILY_COMMAND_RETENTION_HOURS", "24"))
EVENT_RETENTION_DAYS = int(os.environ.get("FAMILY_EVENT_RETENTION_DAYS", "30"))
HOUSEKEEPING_SECONDS = int(os.environ.get("FAMILY_HOUSEKEEPING_SECONDS", "21600"))  # 6h


# ---------------------------------------------------------------------------
# Connection helper -- the family schema is shared, so it is always addressed
# by its fully-qualified name and never relies on this bot's search_path.
# ---------------------------------------------------------------------------
# This borrows db.py's connection pool rather than opening its own connection
# per heartbeat/poll. At a 3-second command poll that was 1,200 connect-
# authenticate-fork-disconnect cycles an hour, per bot, to almost always find
# an empty queue.

def _connect():
    return db.pooled()


def init_family_schema() -> None:
    """Idempotent; every bot calls it at startup, whoever gets there first
    wins. Kept here rather than in ParentBot alone so a bot started on its
    own (no parent running yet) still has somewhere to write."""
    with _connect() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {FAMILY_SCHEMA}")
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {FAMILY_SCHEMA}.heartbeats (
                bot_id TEXT PRIMARY KEY,
                display_name TEXT,
                host TEXT,
                version TEXT,
                pid INTEGER,
                db_schema TEXT,
                started_at TIMESTAMPTZ NOT NULL,
                last_seen TIMESTAMPTZ NOT NULL,
                error_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {FAMILY_SCHEMA}.events (
                id BIGSERIAL PRIMARY KEY,
                bot_id TEXT NOT NULL,
                level TEXT NOT NULL,
                kind TEXT NOT NULL,
                message TEXT NOT NULL,
                details TEXT,
                occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                notified BOOLEAN NOT NULL DEFAULT FALSE
            )
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_family_events_pending "
            f"ON {FAMILY_SCHEMA}.events (notified, id)"
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {FAMILY_SCHEMA}.commands (
                id BIGSERIAL PRIMARY KEY,
                target_bot TEXT NOT NULL,
                command TEXT NOT NULL,
                args TEXT NOT NULL DEFAULT '',
                requested_by BIGINT,
                reply_chat_id BIGINT,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                claimed_at TIMESTAMPTZ,
                finished_at TIMESTAMPTZ,
                ok BOOLEAN,
                output TEXT,
                file_name TEXT,
                file_bytes BYTEA,
                delivered BOOLEAN NOT NULL DEFAULT FALSE
            )
            """
        )
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_family_commands_queue "
            f"ON {FAMILY_SCHEMA}.commands (target_bot, status, id)"
        )
        # ParentBot's memory of who was up last time it looked, so it can
        # alert on the *transition* (down -> up, up -> down) instead of
        # repeating "still down" every minute.
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {FAMILY_SCHEMA}.bot_state (
                bot_id TEXT PRIMARY KEY,
                is_up BOOLEAN NOT NULL,
                changed_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {FAMILY_SCHEMA}.settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        conn.commit()


# ---------------------------------------------------------------------------
# The adaptive poll
# ---------------------------------------------------------------------------
# One timer, ticking every BUS_POLL_ACTIVE_SECONDS, that decides on each tick
# whether this is also a database poll: always while the bus is "active",
# otherwise once per BUS_POLL_IDLE_SECONDS. mark_bus_active() opens a fresh
# active window and is called by anything that has reason to expect bus
# traffic in the next few seconds -- a command just queued, claimed, or
# finished. So a bus that is doing something polls at ~1s and one that has
# been quiet for a while polls at ~10s, with no held connection and no state
# machine to get wrong.

_bus_active_until = 0.0   # monotonic; poll fast while time.monotonic() < this


def mark_bus_active() -> None:
    """Poll at the fast cadence for the next BUS_ACTIVE_WINDOW_SECONDS. Cheap
    and idempotent -- call it whenever bus traffic is likely (a command was
    just written, picked up, or answered)."""
    global _bus_active_until
    _bus_active_until = time.monotonic() + BUS_ACTIVE_WINDOW_SECONDS


def bus_is_active() -> bool:
    return time.monotonic() < _bus_active_until


def db_round_trip_ms() -> float:
    """How long one trivial query to the shared database takes, from this
    process. The honest measure of "how far away is Postgres from here",
    which is most of what a slow bot turns out to be."""
    started = time.perf_counter()
    with _connect() as conn:
        conn.execute("SELECT 1").fetchone()
    return (time.perf_counter() - started) * 1000


# ---------------------------------------------------------------------------
# Outbound: heartbeats and events
# ---------------------------------------------------------------------------

def _monitoring():
    """The module holding this bot's error counter / status text. Called
    `shared_features` in the four public bots and `monitoring` in ParentBot
    (which has no donations or sibling cross-promotion to share) -- looked
    up lazily so this file can stay byte-identical in all five."""
    for name in ("shared_features", "monitoring"):
        try:
            return importlib.import_module(name)
        except ImportError:
            continue
        except Exception:
            return None
    return None


def write_heartbeat() -> None:
    sf = _monitoring()
    errors = getattr(sf, "_error_count", 0) if sf else 0
    with _connect() as conn:
        conn.execute(
            f"""
            INSERT INTO {FAMILY_SCHEMA}.heartbeats
                (bot_id, display_name, host, version, pid, db_schema, started_at, last_seen, error_count)
            VALUES (%s, %s, %s, %s, %s, %s, %s, now(), %s)
            ON CONFLICT (bot_id) DO UPDATE SET
                display_name = excluded.display_name,
                host = excluded.host,
                version = excluded.version,
                pid = excluded.pid,
                db_schema = excluded.db_schema,
                started_at = excluded.started_at,
                last_seen = excluded.last_seen,
                error_count = excluded.error_count
            """,
            (_bot_id, _display_name, HOSTNAME, VERSION, os.getpid(),
             getattr(db, "DB_SCHEMA", "public"), _start_time, errors),
        )
        conn.commit()


def report_event(level: str, kind: str, message: str, details: str | None = None) -> None:
    """Blocking -- call it through asyncio.to_thread from async code, or
    just let report_event_soon() below do that for you.

    level is one of info / warning / error / critical; ParentBot decides
    which levels are worth a DM at 3am (see its ALERT_LEVELS)."""
    if not _enabled:
        return
    with _connect() as conn:
        conn.execute(
            f"INSERT INTO {FAMILY_SCHEMA}.events (bot_id, level, kind, message, details) "
            f"VALUES (%s, %s, %s, %s, %s)",
            (_bot_id, level, kind, message[:4000], (details or "")[:8000] or None),
        )
        conn.commit()
    # ParentBot picks this up on its event pump's next pass -- fast, because a
    # bot reporting an event is usually in the middle of something the bus
    # cares about, and the pump runs its active cadence whenever ParentBot has
    # recently been busy.


def report_event_soon(level: str, kind: str, message: str, details: str | None = None) -> None:
    """Fire-and-forget version, safe to call from a running event loop or
    from plain sync code. Swallows everything -- a monitoring write must
    never propagate into the bot's own error path (which is often exactly
    where it is being called from)."""
    def _run():
        try:
            report_event(level, kind, message, details)
        except Exception:
            logger.debug("Could not report a family event", exc_info=True)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        _run()
        return
    loop.run_in_executor(None, _run)


# ---------------------------------------------------------------------------
# Inbound: the command queue
# ---------------------------------------------------------------------------
# Each entry returns (text, file_name, file_bytes). Only ParentBot ever puts
# rows in the queue, and only the owner can drive ParentBot, so these are the
# same trust level as each bot's own owner-only commands.

def _where_am_i() -> str:
    sf = _monitoring()
    detect = getattr(sf, "detect_host_environment", None) if sf else None
    if detect is not None:
        try:
            return detect()
        except Exception:
            pass
    return HOSTNAME


def ping_probe() -> dict:
    """Blocking -- call through asyncio.to_thread.

    Two numbers, and the second is the one that makes the whole report
    trustworthy. `db_ms` is how far this process is from Postgres. `skew_ms`
    is how far this machine's clock is from Postgres's: without it, any
    "sent at X, arrived at Y" figure computed across two hosts is that skew
    plus the real latency, with no way to tell which is which. Postgres's
    clock is the one both ends can see, so it is the referee.
    """
    local_before = datetime.now(timezone.utc)
    started = time.perf_counter()
    # Through pooled_read, so this is ONE round trip and reports the distance
    # to the database rather than the distance times however many statements
    # the pool wraps around it. Measured through pooled() it was the query
    # plus an implicit BEGIN plus a COMMIT plus a liveness check -- four trips
    # to the database reported as though they were one, which is how
    # "Supabase 976 ms" ended up on screen for a link that was nearer 250.
    with db.pooled_read() as conn:
        server_now = conn.execute("SELECT clock_timestamp()").fetchone()[0]
    elapsed_ms = (time.perf_counter() - started) * 1000
    local_after = datetime.now(timezone.utc)
    # The query result was produced somewhere inside the round trip; the
    # midpoint of it is the least wrong instant to compare against.
    midpoint = local_before + (local_after - local_before) / 2
    return {
        "db_ms": round(elapsed_ms, 2),
        "skew_ms": round((server_now - midpoint).total_seconds() * 1000, 1),
        "host": HOSTNAME,
        "where": _where_am_i(),
        "version": VERSION,
        "pid": os.getpid(),
    }


async def _cmd_ping(context, args):
    """Plain `ping` answers a sentence. `ping trace` answers the numbers
    ParentBot needs to draw the full round trip -- see its /ping."""
    up = datetime.now(timezone.utc) - _start_time
    if not args or args[0] != "trace":
        return f"pong -- up {_format_delta(up)}", None, None
    try:
        probe = await asyncio.to_thread(ping_probe)
    except Exception as exc:
        probe = {"error": f"{type(exc).__name__}: {exc}"}
    probe["up"] = _format_delta(up)
    return json.dumps(probe, separators=(",", ":")), None, None


async def _cmd_status(context, args):
    sf = _monitoring()
    now = datetime.now(timezone.utc)
    hour = await _active_users_since(now - timedelta(hours=1))
    since_start = await _active_users_since(_start_time)
    if sf and hasattr(sf, "build_status_text"):
        return sf.build_status_text(_start_time, hour, since_start), None, None
    return (
        f"Started: {_start_time:%Y-%m-%d %H:%M:%S UTC}\n"
        f"Active users (last hour): {hour}\n"
        f"Active users (since start): {since_start}"
    ), None, None


async def _cmd_errors(context, args):
    sf = _monitoring()
    if sf and hasattr(sf, "error_summary"):
        return sf.error_summary(), None, None
    return "No error tracking in this bot.", None, None


async def _cmd_users(context, args):
    hours = int(args[0]) if args and args[0].isdigit() else 24
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    return f"{await _active_users_since(since)} active user(s) in the last {hours}h.", None, None


async def _cmd_dbdump(context, args):
    dump = getattr(db, "dump_database_csv_zip", None)
    if dump is None:
        return "This bot has no database export.", None, None
    data = await asyncio.to_thread(dump)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")
    return f"{len(data)/1024:.0f} KB", f"{_bot_id}_db_{stamp}.zip", data


async def _cmd_whois(context, args):
    if not args or not args[0].lstrip("-").isdigit():
        return "Usage: whois <user_id>", None, None
    user_id = int(args[0])
    lines = [f"{user_id}"]
    try:
        chat = await context.bot.get_chat(user_id)
        name = " ".join(p for p in (chat.first_name, chat.last_name) if p)
        if name:
            lines.append(f"Name: {name}")
        if chat.username:
            lines.append(f"Username: @{chat.username}")
        if chat.bio:
            lines.append(f"Bio: {chat.bio}")
    except Exception as exc:
        lines.append(f"Couldn't fetch their profile from this bot: {exc}")
        lines.append("(They may have never messaged this bot, or blocked it.)")
    return "\n".join(lines), None, None


async def _cmd_message(context, args):
    """Sends as THIS bot -- that is the whole point of routing it here
    rather than having ParentBot send it: the user only ever sees the bot
    they actually talked to."""
    if len(args) < 2 or not args[0].lstrip("-").isdigit():
        return "Usage: message <user_id> <text>", None, None
    await context.bot.send_message(chat_id=int(args[0]), text=" ".join(args[1:]))
    return "Sent.", None, None


def _tail_lines(path: Path, wanted: int) -> list[str]:
    """Reads the last `wanted` lines by seeking backwards from the end of the
    file. bot.log rotates at 2 MB, and pulling all of it into memory to throw
    away everything but the last forty lines is exactly the kind of spike this
    bot cannot afford on a small container."""
    block = 8192
    with path.open("rb") as f:
        f.seek(0, os.SEEK_END)
        end = f.tell()
        data = b""
        while end > 0 and data.count(b"\n") <= wanted:
            step = min(block, end)
            end -= step
            f.seek(end)
            data = f.read(step) + data
    return data.decode("utf-8", errors="replace").splitlines()[-wanted:]


async def _cmd_logs(context, args):
    lines_wanted = min(int(args[0]), 500) if args and args[0].isdigit() else 40
    which = "errors.log"
    if args and args[-1] in ("bot", "all"):
        which = "bot.log"
    path = Path(__file__).resolve().parent / "logs" / which
    if not path.exists():
        return (
            f"No {which} on this host. File logging is off in the cloud by "
            "default (LOG_TO_FILES=1 turns it back on) -- use the platform's "
            "own log viewer there.", None, None,
        )
    tail = await asyncio.to_thread(_tail_lines, path, lines_wanted)
    if not tail:
        return f"{which} is empty.", None, None
    return f"--- {which}, last {len(tail)} line(s) ---\n" + "\n".join(tail), None, None


async def _cmd_restart(context, args):
    """Exits with a non-zero code so a supervisor restarts the process --
    Railway's restart policy, Docker's restart: unless-stopped, and so on.
    Run against a bot started by hand on a laptop it just stops it."""
    async def _bye():
        await asyncio.sleep(2)
        logger.warning("Restarting: asked to by ParentBot.")
        os._exit(1)

    asyncio.create_task(_bye())
    return "Restarting now (a supervisor brings it back; a hand-started process just stops).", None, None


# ---------------------------------------------------------------------------
# Talking to everybody, and announcing an update before it lands
# ---------------------------------------------------------------------------
# Four commands that all share one shape: ParentBot decides, the bot the user
# actually talks to does the speaking. That is the whole reason these live
# here rather than in ParentBot -- a person who has only ever met StickerBot
# should hear about StickerBot's update from StickerBot, in their own
# language, and not from a private bot they have never seen.

def _i18n():
    """This bot's translations, or None. ParentBot has no i18n.py -- it has
    exactly one reader -- so everything below degrades to plain English
    rather than requiring one."""
    try:
        return importlib.import_module("i18n")
    except ImportError:
        return None
    except Exception:
        return None


# Fallbacks for ParentBot, and for any key a translation file has not caught
# up with yet. Never the normal path in the four public bots.
_PLAIN = {
    "update_soon_try_later": "\U0001f527 I'm about to be updated, so I can't start anything new right now. Please try again in about {minutes} minutes.",
    "update_soon_try_later_soon": "\U0001f527 I'm about to be updated, so I can't start anything new right now. Please try again shortly.",
    "update_will_reset": "\U0001f527 Heads up: I'm about to be updated, and what you have going right now will be reset. You'll be able to start it again in a moment.",
    "update_done_try_now": "✅ The update is done. You can go ahead and try again now.",
}


def phrase(key: str, lang: str | None = None, **kwargs) -> str:
    """One of the four sentences above, translated if this bot can."""
    i18n = _i18n()
    if i18n is not None:
        try:
            text = i18n.t(lang or "en", key, **kwargs)
            # i18n.t returns the key itself when it has no such string, which
            # is the signal to fall back rather than send someone a key.
            if text and text != key:
                return text
        except Exception:
            pass
    return _PLAIN.get(key, key).format(**kwargs)


def _language_of(user_id: int) -> str | None:
    fn = getattr(db, "get_user_language", None)
    if fn is None:
        return None
    try:
        return fn(user_id)
    except Exception:
        return None


def _everyone() -> list[int]:
    fn = getattr(db, "list_all_users", None)
    if fn is None:
        return []
    try:
        return fn()
    except Exception:
        logger.exception("Could not read this bot's user list")
        return []


# Telegram's documented ceiling for bulk sends is about 30 messages a second,
# and a bot that trips it gets a 429 with a retry_after rather than a queue.
# A broadcast is never urgent to the second, so it is paced under the limit
# and simply keeps going past anyone who cannot be reached.
BROADCAST_PER_SECOND = float(os.environ.get("FAMILY_BROADCAST_PER_SECOND", "20"))


async def _send_to_each(context, targets, text_for) -> tuple[int, int]:
    """(delivered, skipped). `targets` is an iterable of (user_id, chat_id);
    `text_for(user_id)` returns that person's text, or None to skip them.

    Nothing here raises. Someone who blocked the bot, deleted their account
    or never really existed is a skip, not a failure -- the alternative is a
    broadcast that stops halfway through the alphabet.
    """
    delivered = skipped = 0
    delay = 1.0 / BROADCAST_PER_SECOND if BROADCAST_PER_SECOND > 0 else 0
    for user_id, chat_id in targets:
        text = text_for(user_id)
        if not text:
            skipped += 1
            continue
        try:
            await context.bot.send_message(chat_id=chat_id, text=text)
            delivered += 1
        except Exception as exc:
            skipped += 1
            logger.debug("Broadcast skipped %s: %s", chat_id, exc)
        if delay:
            await asyncio.sleep(delay)
    return delivered, skipped


async def _cmd_broadcast(context, args):
    """Sends one message to everyone this bot knows, as this bot.

    Verbatim, and deliberately not translated: the owner wrote these words
    and this file has no way to write them again in Uzbek. The four sentences
    the *bots* say about updates are the translated ones.
    """
    text = " ".join(args).strip()
    if not text:
        return "Usage: broadcast <text>", None, None
    users = await asyncio.to_thread(_everyone)
    if not users:
        return "Nobody to broadcast to (this bot has no user list).", None, None
    delivered, skipped = await _send_to_each(
        context, ((uid, uid) for uid in users), lambda _uid: text,
    )
    return (
        f"Broadcast to {delivered} of {len(users)} user(s)"
        + (f"; {skipped} unreachable." if skipped else "."),
        None, None,
    )


async def _cmd_pause(context, args):
    """Stop starting work a redeploy would throw away, and say so.

    Anyone turned away is written down, so `resume` can tell them it is over.
    """
    minutes = int(args[0]) if args and args[0].isdigit() else lifecycle.DEFAULT_MAINTENANCE_MINUTES
    until = await asyncio.to_thread(lifecycle.begin_maintenance, _bot_id, minutes)
    return (
        f"Paused. New long work is declined until you say otherwise; "
        f"users are being told to come back in about {minutes} minute(s) "
        f"(around {until.strftime('%H:%M')} UTC).",
        None, None,
    )


async def _cmd_warnbusy(context, args):
    """Tell everyone mid-something that it is about to be lost.

    Only the people actually inside a slow stretch of work: with state now
    persisted, an open conversation survives a redeploy and warning about it
    would be a false alarm. What does not survive is an encode or a download
    already running, which is exactly what busy() tracks.
    """
    waiting = lifecycle.in_flight()
    if not waiting:
        return "Nobody is mid-anything right now -- nothing to warn about.", None, None
    seen = set()
    targets = []
    for chat_id, _ in waiting:
        if chat_id not in seen:
            seen.add(chat_id)
            targets.append((chat_id, chat_id))
    delivered, skipped = await _send_to_each(
        context, targets,
        lambda uid: phrase("update_will_reset", _language_of(uid)),
    )
    return (
        f"Warned {delivered} user(s) with work in flight"
        + (f"; {skipped} unreachable." if skipped else "."),
        None, None,
    )


async def _cmd_resume(context, args):
    """The other half of pause: reopen, and go back to everyone who was
    turned away while it was closed."""
    await asyncio.to_thread(lifecycle.end_maintenance, _bot_id)
    held = await asyncio.to_thread(lifecycle.take_held, _bot_id)
    if not held:
        return "Open again. Nobody had been turned away.", None, None
    delivered, skipped = await _send_to_each(
        context, held,
        lambda uid: phrase("update_done_try_now", _language_of(uid)),
    )
    return (
        f"Open again. Told {delivered} of {len(held)} user(s) who had been "
        f"turned away" + (f"; {skipped} unreachable." if skipped else "."),
        None, None,
    )


COMMANDS = {
    "ping": _cmd_ping,
    "status": _cmd_status,
    "errors": _cmd_errors,
    "users": _cmd_users,
    "dbdump": _cmd_dbdump,
    "whois": _cmd_whois,
    "message": _cmd_message,
    "logs": _cmd_logs,
    "restart": _cmd_restart,
    "broadcast": _cmd_broadcast,
    "pause": _cmd_pause,
    "warnbusy": _cmd_warnbusy,
    "resume": _cmd_resume,
}

COMMAND_HELP = {
    "ping": "is it alive, and for how long",
    "status": "uptime, host, crash count, active users",
    "errors": "errors since that bot last started",
    "users": "active users -- users [hours], default 24",
    "dbdump": "that bot's own tables as a zip of CSVs",
    "whois": "whois <user_id> -- look a user up through that bot",
    "message": "message <user_id> <text> -- DM someone as that bot",
    "logs": "logs [n] [bot] -- tail errors.log, or bot.log with 'bot'",
    "restart": "restart that bot's process",
    "broadcast": "broadcast <text> -- one message to everyone, as that bot",
    "pause": "pause [minutes] -- decline new long work and say why",
    "warnbusy": "tell whoever is mid-something that it is about to be reset",
    "resume": "reopen, and tell everyone who was turned away",
}


async def _active_users_since(since) -> int:
    fn = getattr(db, "count_active_users_since", None)
    if fn is None:
        return 0
    try:
        return await asyncio.to_thread(fn, since)
    except Exception:
        return 0


def _format_delta(delta: timedelta) -> str:
    days, rem = divmod(int(delta.total_seconds()), 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h {minutes}m"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def _claim_next_command() -> tuple[int, str, str] | None:
    """FOR UPDATE SKIP LOCKED so two copies of the same bot (a laptop one
    and a deployed one both pointed at the same database) can't run the
    same command twice."""
    with _connect() as conn:
        cur = conn.execute(
            f"""
            UPDATE {FAMILY_SCHEMA}.commands SET status = 'running', claimed_at = now()
            WHERE id = (
                SELECT id FROM {FAMILY_SCHEMA}.commands
                WHERE target_bot = %s AND status = 'pending'
                ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED
            )
            RETURNING id, command, args
            """,
            (_bot_id,),
        )
        row = cur.fetchone()
        conn.commit()
        return row


def _finish_command(command_id: int, ok: bool, output: str, file_name, file_bytes) -> None:
    with _connect() as conn:
        conn.execute(
            f"""
            UPDATE {FAMILY_SCHEMA}.commands
            SET status = %s, ok = %s, output = %s, file_name = %s, file_bytes = %s, finished_at = now()
            WHERE id = %s
            """,
            ("done" if ok else "failed", ok, output[:60000], file_name, file_bytes, command_id),
        )
        conn.commit()
    # ParentBot's result pump collects this on its next pass. That pass is at
    # the fast cadence: ParentBot marked the bus active when it queued the
    # command, so the whole time an answer could be coming back it is looking
    # about once a second.


async def _run_one_command(context, row) -> None:
    command_id, command, raw_args = row
    handler = COMMANDS.get(command)
    logger.info("ParentBot asked for: %s %s", command, raw_args)
    try:
        if handler is None:
            ok, output, name, data = False, f"Unknown command '{command}'.", None, None
        else:
            output, name, data = await handler(context, raw_args.split())
            ok = True
    except Exception as exc:
        logger.exception("Family command %r failed", command)
        ok, output, name, data = False, f"{type(exc).__name__}: {exc}", None, None

    try:
        await asyncio.to_thread(_finish_command, command_id, ok, output, name, data)
    except Exception:
        logger.exception("Could not write the result of family command %s back", command_id)


# One pass does not mean one command: several can be queued between two ticks
# (a /ping to everything, a bot that was down catching up), so drain rather
# than leave the rest for "next time". The ceiling is there so that a queue
# somebody filled by accident cannot monopolise the event loop -- what is left
# waits for the next tick, which is immediate while the bus is active.
MAX_COMMANDS_PER_PASS = int(os.environ.get("FAMILY_MAX_COMMANDS_PER_PASS", "10"))


async def _poll_commands(context) -> None:
    ran = 0
    for _ in range(MAX_COMMANDS_PER_PASS):
        try:
            row = await asyncio.to_thread(_claim_next_command)
        except Exception:
            logger.debug("Family command poll failed (database unreachable?)", exc_info=True)
            break
        if not row:
            break
        await _run_one_command(context, row)
        ran += 1
    if ran:
        # Commands cluster -- one arriving means another probably is too.
        # Keep looking at the fast cadence for a while.
        mark_bus_active()


# _bus_tick fires every BUS_POLL_ACTIVE_SECONDS and decides whether this tick
# is also a database poll: always while the bus is active, otherwise once per
# BUS_POLL_IDLE_SECONDS. This is the whole delivery mechanism -- there is no
# push behind it any more -- so it is deliberately simple and has nothing that
# can be "down".
_last_bus_poll_at = 0.0


async def _bus_tick(context) -> None:
    global _last_bus_poll_at
    due = BUS_POLL_ACTIVE_SECONDS if bus_is_active() else BUS_POLL_IDLE_SECONDS
    now = time.monotonic()
    if now - _last_bus_poll_at < due:
        return
    _last_bus_poll_at = now
    await _poll_commands(context)


async def _send_heartbeat(context) -> None:
    try:
        await asyncio.to_thread(write_heartbeat)
    except Exception:
        logger.debug("Heartbeat failed (database unreachable?)", exc_info=True)


# ---------------------------------------------------------------------------
# Housekeeping -- every bot tidies up after itself
# ---------------------------------------------------------------------------
# Three tables grow forever if nobody deletes from them: family.commands (with
# a BYTEA column, so a handful of /dbdump zips is megabytes), family.events,
# and each bot's own activity_events. On a metered database that is a bill
# that only goes up, for rows nothing will ever read again.
#
# Each bot prunes only its *own* rows, so the five processes never collide and
# no coordination is needed -- target_bot / bot_id already partition the shared
# tables perfectly, and activity_events lives in the bot's own schema.

def _prune_family_rows() -> tuple[int, int]:
    with _connect() as conn:
        cur = conn.execute(
            f"DELETE FROM {FAMILY_SCHEMA}.commands "
            f"WHERE target_bot = %s AND delivered = TRUE "
            f"AND finished_at < now() - make_interval(hours => %s)",
            (_bot_id, COMMAND_RETENTION_HOURS),
        )
        commands = cur.rowcount
        cur = conn.execute(
            f"DELETE FROM {FAMILY_SCHEMA}.events "
            f"WHERE bot_id = %s AND notified = TRUE "
            f"AND occurred_at < now() - make_interval(days => %s)",
            (_bot_id, EVENT_RETENTION_DAYS),
        )
        events = cur.rowcount
        conn.commit()
    return commands, events


def _prune() -> str:
    commands, events = _prune_family_rows()
    parts = [f"{commands} command(s)", f"{events} event(s)"]
    own = getattr(db, "prune_old_data", None)
    if own is not None:
        parts.append(f"{own()} activity row(s)")
    return ", ".join(parts)


async def _housekeeping(context) -> None:
    try:
        removed = await asyncio.to_thread(_prune)
    except Exception:
        logger.debug("Housekeeping pass failed (database unreachable?)", exc_info=True)
        return
    logger.info("Housekeeping: pruned %s.", removed)


# ---------------------------------------------------------------------------
# Wiring
# ---------------------------------------------------------------------------

def attach(app, bot_id: str, display_name: str, start_time: datetime) -> None:
    """One line in each bot's main(), just before run_polling().

    Never raises: a bot whose shared database is unreachable logs a warning
    and carries on serving its users with no family bus at all.
    """
    global _bot_id, _display_name, _start_time, _enabled

    if os.environ.get("FAMILY_BUS", "on").lower() in ("off", "0", "false", "no"):
        logger.info("Family bus disabled (FAMILY_BUS=off) -- running standalone.")
        return

    _bot_id, _display_name, _start_time = bot_id, display_name, start_time

    try:
        init_family_schema()
        write_heartbeat()
    except Exception as exc:
        logger.warning(
            "Family bus unavailable (%s) -- this bot runs fine without it, but "
            "ParentBot will report it as down until the shared database is reachable.", exc,
        )
        return

    _enabled = True

    sf = _monitoring()
    if sf and hasattr(sf, "set_event_hook"):
        sf.set_event_hook(report_event_soon)

    report_event_soon("info", "startup", f"{display_name} started on {HOSTNAME}.")

    if app.job_queue is None:
        logger.warning("No job queue -- install python-telegram-bot[job-queue]. Family bus is off.")
        _enabled = False
        return

    app.job_queue.run_repeating(_send_heartbeat, interval=HEARTBEAT_SECONDS, first=HEARTBEAT_SECONDS)

    # The command bus. One repeating tick at the fast cadence; _bus_tick
    # decides on each one whether to actually poll (always while the bus is
    # active, otherwise every BUS_POLL_IDLE_SECONDS).
    app.job_queue.run_repeating(
        _bus_tick, interval=BUS_POLL_ACTIVE_SECONDS, first=BUS_POLL_ACTIVE_SECONDS
    )
    # A bot that has just started is usually about to be pinged by ParentBot's
    # roll-call, so open with the fast cadence rather than making that first
    # command wait a full idle interval.
    mark_bus_active()

    # First pass a minute in rather than at startup, so a restart loop cannot
    # turn into a delete storm.
    app.job_queue.run_repeating(_housekeeping, interval=HOUSEKEEPING_SECONDS, first=60)
    logger.info(
        "Family bus on: heartbeat every %ss, commands polled (%ss busy / %ss idle), "
        "tidy-up every %ss.",
        HEARTBEAT_SECONDS, BUS_POLL_ACTIVE_SECONDS, BUS_POLL_IDLE_SECONDS, HOUSEKEEPING_SECONDS,
    )
