"""ParentBot's own logging / crash tracking / activity tracking.

This is the monitoring half of the other four bots' shared_features.py,
copied here without the halves ParentBot has no use for (donations, the
sibling-bot cross-promotion, i18n -- ParentBot has exactly one user and
speaks one language). family_link.py looks for a module named either
`shared_features` or `monitoring` and uses whichever it finds, which is why
this file keeps the same function names.

Keeping ParentBot on the same vocabulary matters: a bot that watches the
others should be watched by exactly the same machinery, so "ParentBot has
been quietly crashing" is as visible as it would be for any of them.
"""
import asyncio
import logging
import os
import socket
import traceback
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler

from telegram.error import BadRequest, NetworkError, RetryAfter

import db

_LOG_FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"

_recent_errors: deque[tuple[str, str]] = deque(maxlen=10)
_error_count = 0

# Set by family_link.attach(); see shared_features.py in any of the other
# bots for the same hook.
_event_hook = None


def set_event_hook(fn) -> None:
    global _event_hook
    _event_hook = fn


def emit_event(level: str, kind: str, message: str, details: str | None = None) -> None:
    if _event_hook is None:
        return
    try:
        _event_hook(level, kind, message, details)
    except Exception:
        logging.getLogger(__name__).debug("Family event hook failed", exc_info=True)


def _log_to_files() -> bool:
    """Files on a laptop, stdout only in the cloud -- see the identical
    reasoning in the other four bots' shared_features.py."""
    override = os.environ.get("LOG_TO_FILES")
    if override is not None:
        return override.strip().lower() in ("1", "true", "yes", "on")
    return not (os.environ.get("RAILWAY_ENVIRONMENT_NAME") or os.environ.get("RAILWAY_ENVIRONMENT"))


def setup_logging(bot_file: str) -> None:
    fmt = logging.Formatter(_LOG_FORMAT)

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    root.addHandler(console)

    for noisy in ("httpx", "httpcore", "telegram.ext.Updater", "apscheduler"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    if not _log_to_files():
        return

    log_dir = os.path.join(os.path.dirname(os.path.abspath(bot_file)), "logs")
    os.makedirs(log_dir, exist_ok=True)

    info_file = RotatingFileHandler(
        os.path.join(log_dir, "bot.log"), maxBytes=2_000_000, backupCount=3, encoding="utf-8"
    )
    info_file.setFormatter(fmt)
    root.addHandler(info_file)

    error_file = RotatingFileHandler(
        os.path.join(log_dir, "errors.log"), maxBytes=1_000_000, backupCount=3, encoding="utf-8"
    )
    error_file.setLevel(logging.WARNING)
    error_file.setFormatter(fmt)
    root.addHandler(error_file)


# ---------------------------------------------------------------------------
# Transient network conditions vs. actual faults
# ---------------------------------------------------------------------------
# A long poll losing its connection, a read timing out, Telegram asking us to
# slow down: PTB retries all of these itself and the bot keeps working, so
# waking the owner with a traceback for each one is noise that trains you to
# ignore the channel that also carries real crashes.
#
# The catch is PTB's class hierarchy: BadRequest *subclasses* NetworkError,
# and a BadRequest is a genuine fault -- a malformed API call, our bug -- so a
# plain `isinstance(exc, NetworkError)` would swallow exactly the errors most
# worth hearing about. It has to be excluded explicitly.

TRANSIENT_NETWORK_ERRORS = (NetworkError, RetryAfter)

# How many blips in a row before saying something. Reset by any update that
# arrives, since one arriving proves the connection is working again. Roughly
# a few minutes of a dead link at a 30-second poll.
NETWORK_ALERT_AFTER = int(os.environ.get("NETWORK_ALERT_AFTER", "20"))

_network_blips = 0        # consecutive, since the last update actually arrived
_network_blips_total = 0  # since this process started
_network_alerted = False


def is_transient_network_error(exc: BaseException) -> bool:
    return isinstance(exc, TRANSIENT_NETWORK_ERRORS) and not isinstance(exc, BadRequest)


def note_network_blip(exc: BaseException) -> None:
    """Counted and logged, never reported as a crash -- until there have been
    enough in a row to mean the connection is gone rather than flaky, which is
    worth exactly one message."""
    global _network_blips, _network_blips_total, _network_alerted
    _network_blips += 1
    _network_blips_total += 1
    logging.getLogger(__name__).warning(
        "Transient network error (%s): %s -- retried by PTB, %s in a row",
        type(exc).__name__, exc, _network_blips,
    )
    if _network_blips >= NETWORK_ALERT_AFTER and not _network_alerted:
        _network_alerted = True
        emit_event(
            "warning", "network",
            f"{_network_blips} network errors in a row -- this bot may not be "
            f"reaching Telegram. Latest: {type(exc).__name__}: {exc}",
        )


def note_network_ok() -> None:
    """An update arrived, so the connection works. Called from track_activity,
    which runs before every other handler."""
    global _network_blips, _network_alerted
    if _network_blips and _network_alerted:
        emit_event("info", "network", "Telegram is reachable again.")
    _network_blips = 0
    _network_alerted = False


def record_error(exc: BaseException) -> None:
    global _error_count
    _error_count += 1
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    _recent_errors.append((stamp, repr(exc)))
    emit_event(
        "error", "crash", f"ParentBot itself hit an unhandled {type(exc).__name__}: {exc}",
        "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
    )


def error_summary() -> str:
    blips = (
        f"\n\U0001f310 {_network_blips_total} transient network error(s) -- retried, not crashes."
        if _network_blips_total else ""
    )
    if _error_count == 0:
        return "✅ No errors since this instance started." + blips
    lines = [f"⚠️ {_error_count} error(s) since start:"]
    lines.extend(f"  {stamp} — {msg}" for stamp, msg in _recent_errors)
    if _error_count > len(_recent_errors):
        lines.append(f"  (+{_error_count - len(_recent_errors)} earlier, see logs/errors.log)")
    return "\n".join(lines) + blips


async def error_handler(update, context) -> None:
    if is_transient_network_error(context.error):
        note_network_blip(context.error)
        return
    logging.getLogger(__name__).error("Unhandled exception while processing an update", exc_info=context.error)
    record_error(context.error)


# ---------------------------------------------------------------------------
# Active-user tracking, buffered -- see shared_features.py for the reasoning
# ---------------------------------------------------------------------------
# ParentBot has one user, so the saving here is small in absolute terms. It is
# kept identical anyway: the whole point of this file is that ParentBot is
# measured by exactly the same machinery as the bots it watches, and a
# divergence here is a divergence in what /status means.
ACTIVITY_FLUSH_SECONDS = int(os.environ.get("ACTIVITY_FLUSH_SECONDS", "60"))

_activity_buffer: set[int] = set()


def _flush_activity_now() -> int:
    global _activity_buffer
    if not _activity_buffer:
        return 0
    batch, _activity_buffer = _activity_buffer, set()
    try:
        db.record_activity_batch(batch)
    except Exception:
        _activity_buffer |= batch
        raise
    return len(batch)


async def _flush_activity_job(context) -> None:
    try:
        await asyncio.to_thread(_flush_activity_now)
    except Exception:
        logging.getLogger(__name__).debug("Activity flush failed; will retry", exc_info=True)


async def track_activity(update, context) -> None:
    note_network_ok()
    user = update.effective_user
    if not user:
        return
    _activity_buffer.add(user.id)


WORKER_THREADS = int(os.environ.get("WORKER_THREADS", "4"))


async def tune_runtime(application) -> None:
    """Call from post_init -- see shared_features.py for why the default
    executor is worth capping on a shared cloud host."""
    asyncio.get_running_loop().set_default_executor(
        ThreadPoolExecutor(max_workers=WORKER_THREADS, thread_name_prefix="worker")
    )


def attach_maintenance(app) -> None:
    """One line in ParentBot's main(). ParentBot refuses to start without a
    job queue at all (see bot.py), so there is no degraded path here."""
    app.job_queue.run_repeating(
        _flush_activity_job, interval=ACTIVITY_FLUSH_SECONDS, first=ACTIVITY_FLUSH_SECONDS
    )


async def flush_on_shutdown(application) -> None:
    """Register as Application.post_stop."""
    try:
        await asyncio.to_thread(_flush_activity_now)
    except Exception:
        logging.getLogger(__name__).debug("Final activity flush failed", exc_info=True)
    try:
        await asyncio.to_thread(db.close_pool)
    except Exception:
        logging.getLogger(__name__).debug("Closing the connection pool failed", exc_info=True)


def detect_host_environment() -> str:
    railway_env = os.environ.get("RAILWAY_ENVIRONMENT_NAME") or os.environ.get("RAILWAY_ENVIRONMENT")
    if railway_env:
        project = os.environ.get("RAILWAY_PROJECT_NAME", "?")
        service = os.environ.get("RAILWAY_SERVICE_NAME", "?")
        return f"☁️ Cloud (Railway -- project \"{project}\", service \"{service}\", env \"{railway_env}\")"
    return f"💻 Local ({socket.gethostname()})"


def build_status_text(start_time: datetime, users_last_hour: int, users_since_start: int) -> str:
    now = datetime.now(timezone.utc)
    uptime = now - start_time
    days, rem = divmod(int(uptime.total_seconds()), 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)
    uptime_str = f"{days}d {hours}h {minutes}m" if days else f"{hours}h {minutes}m"
    return "\n".join([
        "📊 Status",
        f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')} ({uptime_str} ago)",
        f"Hosted: {detect_host_environment()}",
        f"Active users (last hour): {users_last_hour}",
        f"Active users (since this start): {users_since_start}",
        "",
        error_summary(),
    ])
