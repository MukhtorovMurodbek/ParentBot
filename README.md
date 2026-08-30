# ParentBot

The private one. It watches the other four bots in the family, tells you when
something is wrong without being asked, and lets you run any of their
owner-only commands from one chat.

Unlike its four siblings this bot has no public side at all. `PBOT_ADMIN_ID`
is required, not optional — with it empty ParentBot refuses to start rather
than run open to whoever finds it. Anyone who isn't on that list gets one
sentence, and you get told they turned up.

## What it does on its own

- **Watches uptime.** Every bot stamps a heartbeat into the shared database
  every 30 seconds. ParentBot checks once a minute and messages you when one
  goes stale — and again when it comes back. It alerts on the change, so a
  bot that stays down doesn't repeat itself at you.
- **Forwards crashes.** Every unhandled exception in any bot already gets
  counted for that bot's `/status`; now it also arrives here, with the tail
  of the traceback.
- **Tells you about donations**, since that's the one interruption worth
  having.
- **Says when it has gone blind.** If ParentBot's own database connection
  fails, it says so over Telegram — otherwise the one failure that stops all
  the alerts would be the one failure you'd never hear about.

## Commands

Watching:
- `/status` — every bot: up/down, uptime, host, version, errors, active users
- `/me` — ParentBot's own status, in the shape every other bot uses
- `/events [bot] [n]` — recent crashes, startups, payments
- `/alerts on|off` — mute or unmute the unprompted messages

A few seconds after ParentBot itself starts it sends one **startup roll-call**:
which of the four are up, which just came up with it, and which are missing.
One message, not five — the delay (`PBOT_ROLLCALL_SECONDS`, default 5) is
there so the whole family has registered before it reports. `/alerts off`
silences it like everything else.

Reaching into a bot:
- `/run <bot> <command> [args]` — the general form; run it bare for the list
- `/ping [bot]` — no bot named pings all of them at once
- `/errors <bot>` — that bot's errors since it last started
- `/logs <bot> [n]` — tail its `errors.log` (add `bot` for `bot.log`)
- `/whois <bot> <user_id>` — look someone up through that bot
- `/say <bot> <user_id> <text>` — DM someone **as** that bot
- `/dbdump <bot>` — that bot's own tables as a zip of CSVs
- `/restart <bot>` — restart its process

Across the whole family:
- `/users [hours]` — active users per bot, default 24h
- `/donations` — paid donations per bot
- `/sql <SELECT …>` — read-only query against the shared database
- `/backup` — the entire database as one zip of CSVs
- `/start`, `/help` — the same thing: this list, printed in the chat

Any unambiguous prefix names a bot: `/logs stick`, `/run conv status`,
`/dbdump anon` all work.

## How it reaches the other bots

Not over the network — through the database. `/run` puts a row on
`family.commands`; the target bot's poller claims it, runs the handler in its
own process as its own Telegram identity, and writes the answer back. So:

- it works whether ParentBot is on your laptop and the bot is on Railway, or
  the other way round, with neither reachable from the other;
- a bot that's down never claims the command, and ParentBot tells you that
  after 90 seconds instead of hanging;
- `/say` arrives from the bot the user was already talking to, because that
  bot is what actually sends it.

ParentBot reads the other bots' tables directly (that's what one shared
database is for) but never writes to them. Anything that changes state goes
through the command queue so the owning bot does it with its own code.

See [../ARCHITECTURE.md](../ARCHITECTURE.md) for the full picture and
`family_link.py` for the bus itself.

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env      # then fill in PBOT_TOKEN, PBOT_USERNAME, PBOT_ADMIN_ID
python bot.py
```

It needs the shared family Postgres. Locally that's `docker compose up -d` in
the repo root; deployed it's the project's Postgres service. Get your numeric
Telegram id from [@userinfobot](https://t.me/userinfobot).

`/crashtest` deliberately raises, so you can confirm the whole chain —
`errors.log`, the error counter, and the alert arriving in your chat — really
works without waiting for a real bug.

## Also here

`family_db.py` is the portable database exporter/importer — it moves rows
over the same psycopg connection the bots use, so it needs no `pg_dump` and
can't hit a version mismatch:

```bash
python family_db.py backup  --from cloud
python family_db.py restore --into local --file backups/family_….zip
python family_db.py copy    --from cloud --into local
python family_db.py tables  --at cloud
```

`..\db_backup.ps1` wraps it (and `pg_dump`, when that's available and new
enough) into `save` / `load` / `pull` / `push`.
