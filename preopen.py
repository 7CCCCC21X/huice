"""Fixed, post-only quotes on newly listed 15-minute markets. Disabled by default; switched and sized on the control page
(PREOPEN_15M and the PREOPEN_* settings are Config's: LiveBot.preopen_apply starts, stops or restarts this mode as they change).

The model trader does not choose these orders: their identity, clock and fixed
size are checked independently. Records live in a separate, synchronous SQLite
ledger; unknown submissions are looked up by their saved hash, never re-signed.

Review fixes (the second revision): a definite refusal (post-only crossing, a
4xx) ends the order instead of leaving it "unknown" with its money reserved for
good; cancels and reads of a pair that has not reached its end are tried again
with a growing wait, not every half second; a pair whose orders are confirmed
gone with nothing filled gives its reservation back at once, and the queue takes
the soonest window first, only PREOPEN_AHEAD_MINUTES ahead; the size, the price
and the cards are settings; the listing's fields can be probed read-only before
the mode is switched on; the rate-limit cooldown is shared with the bot's own
API client; a crash of this mode no longer stops the bot.

1.126.0: every listed window is quoted — PREOPEN_AHEAD_MINUTES defaults to 1440
(a day) and any day counts — and the discovery stays cheap at that width: each
card's windows are asked in start order, the first unlisted one every poll (the
one Predict lists next), up to MISS_STOP unlisted in a row once a minute. Open
orders on the account that nobody here placed no longer hold the mode (their
money counts in the balance check). The rate limiter's cooldown is a hold, and a
pair whose first request it refused leaves no record: a later poll places it.

1.127.0: the quotes survive a restart and a settings change — an active pair
that is what the settings say (size, price, signed expiry, card) is taken over,
its queue position kept; one that is not is withdrawn and the window quoted
again under a new salt (attempt). A window withdrawn before its start by a stop,
a pause or an external cancel is quoted again too, at most MAX_ATTEMPTS times
(never after Predict's own refusal). The process stopping leaves the orders on
the book: the signed expiry is the backstop, the next process takes them over.
1.127.1: the request wrapper takes a cancel's answer (no "data" key) as the
success it is; only an explicit success=false is a refusal.

Audit PRE-LATE-FILL: an order off the book is ended by a read of its record made
LATE_FILL_MS after it left, not by the one right after its removal, which may
not show the last seconds' matches yet — a pair withdrawn with nothing filled
gives its reservation back a minute on, not at once (Controller.ended).
"""
from __future__ import annotations

import asyncio
import collections
import contextlib
import dataclasses
import datetime as dt
import hashlib
import json
import logging
import math
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable, Iterator

import main as core

LOG = logging.getLogger(__name__)
TERMINAL = {"removed", "not_sent", "rejected", "not_found", "final"}  # states an order cannot leave: nothing is left to cancel
NEVER_PLACED = {"not_sent", "rejected", "not_found"}                  # ...of which these never tied money up
RETRY_MS, RETRY_MAX_MS = 15_000, 120_000  # a closing pair's cancels and reads are tried again this long after a try, doubling to the cap
GIVE_UP_MS = 15 * 60_000  # after a window's start + this, an order Predict cannot show by its hash is taken as never placed (it expired before the start)
LATE_FILL_MS = 60_000     # an order off the book is ended only by a read of its record made this long after it left: an earlier one may not show its last matches yet (Controller.ended)
SPRINT_MS = 2_000         # the last stretch (past the cancel time, before the expiry): an order still open is asked to be cancelled this often, no backoff
REFRESH_MS = 30_000       # an active window's market is read again this often (matching on; a status change withdraws the pair)
REFRESH_FAR_MS = 900_000  # ...a window starting more than an hour away: this often (a day's windows must not cost a read a second; 1.134.1: 15 min, was 5)
ACCOUNT_PAGE_S = 1.5      # the account snapshot's pace: this long per page of open orders (1,140 orders = 23 pages every 35 s, not every 5), the poll at least
REFRESH_BATCH = 8         # refresh_active: at most this many market reads an iteration, three at a time (a burst of due windows is spread)
HOLD_RETRY_MS = 5_000     # a held candidate (hold(): a stale snapshot, the cooldown, the pause; or the budget) waits this long on the side, then is placed again — never read again meanwhile
ERROR_RETRY_MS = 30_000   # ...and one whose placement raised (a read failed, the market's tokens could not be told) this long
HALT_MS = 180_000         # a stock card's exchange quote whose own time stands still this long in session: trading halted (a VI, a 临停)
CARD_CHECK_MS = 5_000     # the stock cards' limit / halt check (card_hold) this often
CARD_HOLD_NEAR_MS = 60 * 60_000  # ...and a hold keeps back / withdraws only the windows starting within this (a day's windows stay: the hold is about the next ones)
LUNCH = {"sh": (dt.time(11, 30), dt.time(13, 0)), "hk": (dt.time(12, 0), dt.time(13, 0))}  # the lunch breaks (Beijing time) the halt rule does not count (core.quiet_ms)
FILL_SETTLE_MS = 45 * 60_000  # a pair with a fill is cleared by the result: "stuck" only this long past the start
SKIP_RETRY_MS = 60_000    # a listed window that is no candidate (a missing field, a fee, a status) is asked again this often, not every poll
MONITOR_MS = 5_000        # an active pair's orders are looked at this often (monitor_pair), the first look 10 s after the placement
SEND_MIN_LEFT_MS = 60_000  # an order is not sent with less than this left to its expiry
DUPLICATE_WORDS = ("already exist", "already placed", "duplicate", "exists")  # Predict has the order: not a refusal
REFUSED_STATUSES = {"REJECTED", "INVALIDATED", "FAILED"}  # an order's record ending so with nothing filled: Predict's deferred refusal (not quoted again)
FILLED_STATUSES = {"FILLED", "COMPLETED", "DONE", "MATCHED"}  # a record that says so without an amount filled: the whole order, by its own word
ERRORS_KEPT = 20          # the last problems kept with their time (/preopen check)
STUCK_MS = 15 * 60_000    # a withdrawn pair not cleared this long past its cancel time is announced (check_stuck)
LISTING_FIELDS = ("id", "conditionId", "status", "tradingStatus", "question", "outcomes")  # the details' fields the discovery reads
LISTING_FULL_FIELDS = (*LISTING_FIELDS, "feeRateBps", "isNegRisk", "isYieldBearing")  # ...and what the order itself needs: a category answer carrying them all is placed on without the details read (1.133.0)
MAX_ATTEMPTS = 3       # a window's quotes that ended by its own failure, at most (a pair withdrawn before the start is quoted again under a new salt)
BULK_MIN_IDS = 40        # more orders than this to withdraw at once (a pause, a settings change, the mode off) go in batches that wait out a rate limit (1.141.0)
BULK_WORKERS = 4         # ...holding at most this many of the cancel client's requests (live.PredictApi.PARALLEL: one left for a window at its cancel time)
STOP_CANCEL_S = 600      # a controller that crashed withdraws for this long at most on its way out (was 12 s for any stop: a book of thousands mostly stayed)
DEAD_WITHDRAW_S = 30     # a crashed controller's withdrawal from an operator's command: this long at most, the command waiting (1.141.1)
BULK_REFUSED_MAX = 2     # a pair whose batch withdrawal stopped on a refusal of the request this often goes to the per-pair path (an id of its own Predict will not take)
FINALS_WAIT_MS = 30 * 60_000  # winding down: once nothing can rest, the withdrawn orders' final fills are read this long at most (the rest at the next start)
MAY_REST = {"open", "placing", "unknown", "prepared"}  # order states that may be on the book: sent, or being sent, for all we know
FREE_REASONS = ("设置不同", "预挂模式停止", "到时撤单或真实交易暂停", "距涨停", "距跌停", "临停")  # withdrawals that were not the window's doing (counts_against)
MISS_STOP = 4          # discover: a card's coming windows are asked in start order; this many unlisted in a row end the card's full scan
FULL_SCAN_MS = 60_000  # a full scan (up to MISS_STOP unlisted per card) this often; between them each card's first unlisted window alone is asked every poll
ARCHIVE_AFTER_MS = 2 * core.DAY_MS  # a finished quote that filled nothing leaves the working records this long after its window's end (archivable, Ledger.archive_old)
ARCHIVE_EVERY_MS = 3_600_000        # ...looked for at a start and then this often (Controller.archive; LiveBot.preopen_tidy while the mode is off)
ARCHIVE_CHUNK = 2_000               # ...this many a transaction: the first pass over an old ledger moves tens of thousands, and the file must not double for it


def final_order(row: dict) -> bool:
    status = str(row.get("status") or "").upper()
    amount, filled = D(str(row.get("amount") or "0")), D(str(row.get("amountFilled") or "0"))
    return amount > 0 and (filled >= amount or status in {
        "FILLED", "MATCHED", "CANCELLED", "CANCELED", "EXPIRED", "INVALIDATED", "REJECTED", "FAILED", "CLOSED", "COMPLETED", "DONE", "SETTLED", "RESOLVED"})


def order_verified(order: dict) -> bool:
    """Whether the order's money is accounted for: never placed, or its final record read."""
    return order["state"] in NEVER_PLACED or order["state"] == "final"


Settings = core.PreopenSettings  # the settings live in Config (PREOPEN_*, the control page's values included): Settings.from_config(config)


def archivable(pair: dict, before_ms: int) -> bool:
    """Whether a record is history and nothing else (Ledger.archive_old): accounted and closed, its window over before
    ``before_ms``, every order ended in the ordinary way — its final record read, never sent, or refused outright, with no
    note of a doubt (an order given up on — never found by its hash, final only as known — stays among the known ones:
    Controller.known) — and nothing in it that anything but the review page's counts reads: no fill (no position, no
    P&L: fills_by_outcome, daily_pnl, the EV bets, the tally's fills), no official result (the tally's cross-check)."""
    return (bool(pair.get("accounted")) and pair.get("phase") == "closed" and int(pair.get("start_ms") or 0) > 0
            and settle_from(pair) < before_ms and not float(pair.get("pnl") or 0) and not pair.get("result")
            and all(o.get("state") in {"final", "not_sent", "rejected"} and float(o.get("filled") or 0) <= 0 and not o.get("note")
                    for o in (pair.get("orders") or {}).values()))


class Ledger:
    """No background writer or structural re-key: an acknowledged record is on disk.

    Audit PRE-LEDGER-GROWTH: ``rows`` (the table ``pairs``) are the working records — every one is loaded at a start and
    walked by the loops many times a second — and nothing ever left them: one record a window, a level and an attempt, for
    good (twenty cards at four levels: thousands a day). At 50,000 the start froze the bot for two seconds, the loops took
    a quarter of its event loop, a listing's burst of pairs took seconds to send, and the memory and the file only grew.
    The finished quotes that filled nothing now move to the table ``archive`` two days after their window (archive_old),
    whole, in the same file; only the review page reads them — once: after that it keeps their counts and reads the rows
    moved in since (ArchiveShare)."""
    rewrites = 0  # passes of this process (any ledger's) that replaced a record the archive already held: its rows did not only grow, and the counts kept of them are read afresh (ArchiveShare)

    def __init__(self, path: str):
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("CREATE TABLE IF NOT EXISTS pairs (slug TEXT PRIMARY KEY, body TEXT NOT NULL)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, body TEXT NOT NULL)")
        self.conn.commit()
        self.rows = {slug: json.loads(body) for slug, body in self.conn.execute("SELECT slug,body FROM pairs")}
        self.by_hash: dict[str, str] = {}  # order hash -> its record's key (holder); "" for a hash two records hold
        for slug, row in self.rows.items():
            self.index(slug, row)
        self.archived_count = self.archive_count()  # (the status says how many)

    def index(self, slug: str, row: dict) -> None:
        """A record's order hashes noted in by_hash (holder). A hash two records hold at once — a cloned record; never this
        code's own doing, the salt differs by window, level and attempt — is marked "": holder then walks the records,
        the first holder its answer as it always was. An entry naming a record that no longer holds the hash is replaced."""
        for order in (row.get("orders") or {}).values():
            digest = order.get("hash")
            if not digest:
                continue
            kept = self.by_hash.get(digest)
            other = self.rows.get(kept) if kept and kept != slug else None
            if other is not None and any(o.get("hash") == digest for o in (other.get("orders") or {}).values()):
                self.by_hash[digest] = ""
            elif kept != "":
                self.by_hash[digest] = slug

    def unindex(self, slug: str, row: dict) -> None:
        """A record leaving ``rows`` (deleted, renamed, archived): its entries in by_hash go with it."""
        for order in (row.get("orders") or {}).values():
            if order.get("hash") and self.by_hash.get(order["hash"]) == slug:
                del self.by_hash[order["hash"]]

    def holder(self, digest: str) -> dict | None:
        """The record one of whose orders has this hash (Controller.before_send). By the index: the walk it replaces went
        through every record from the oldest to the one being sent, the newest, four times a pair (Audit
        PRE-LEDGER-GROWTH). Whenever the index does not answer for certain — no entry, an entry whose record does not hold
        the hash, a hash two records hold — the walk itself, so the answer is the one it always gave."""
        row = self.rows.get(self.by_hash.get(digest) or "")
        if row is not None and any(o.get("hash") == digest for o in row["orders"].values()):
            return row
        return next((p for p in self.rows.values() if any(o.get("hash") == digest for o in p["orders"].values())), None)

    def save(self, row: dict) -> None:
        body = json.dumps(row, ensure_ascii=False)
        with self.conn:
            self.conn.execute("INSERT INTO pairs VALUES (?,?) ON CONFLICT(slug) DO UPDATE SET body=excluded.body", (row["slug"], body))
        self.rows[row["slug"]] = row
        self.index(row["slug"], row)

    def save_many(self, rows: list[dict]) -> None:
        """Several records in one transaction (1.141.0: a batch withdrawal touches thousands — one commit, not one each)."""
        if not rows:
            return
        with self.conn:
            self.conn.executemany("INSERT INTO pairs VALUES (?,?) ON CONFLICT(slug) DO UPDATE SET body=excluded.body",
                                  [(row["slug"], json.dumps(row, ensure_ascii=False)) for row in rows])
        for row in rows:
            self.rows[row["slug"]] = row
            self.index(row["slug"], row)

    def delete(self, slug: str) -> None:
        """A pair none of whose orders was ever sent (the rate limiter refused the first request): no record, so a later
        poll places the window afresh — the same signed orders, their salt being deterministic."""
        with self.conn:
            self.conn.execute("DELETE FROM pairs WHERE slug=?", (slug,))
        row = self.rows.pop(slug, None)
        if row is not None:
            self.unindex(slug, row)

    def rename(self, slug: str, new_slug: str) -> None:
        """A record stepping aside (its window quoted again under a new attempt): kept, under a key that is not the window's."""
        row = self.rows.pop(slug)
        row["slug"] = new_slug
        with self.conn:
            self.conn.execute("DELETE FROM pairs WHERE slug=?", (slug,))
            self.conn.execute("INSERT INTO pairs VALUES (?,?) ON CONFLICT(slug) DO UPDATE SET body=excluded.body", (new_slug, json.dumps(row, ensure_ascii=False)))
        self.unindex(slug, row)
        self.rows[new_slug] = row
        self.index(new_slug, row)

    def archive_old(self, now_ms: int) -> int:
        """The records that are history only (archivable: accounted, closed, every order ended in the ordinary way, nothing
        filled, no result, the window over ARCHIVE_AFTER_MS ago) moved from ``pairs`` to ``archive`` and out of ``rows``;
        how many. Whole and in the same file — nothing is deleted (putting one back is one INSERT … SELECT) — a chunk a
        transaction: in the archive and out of ``pairs`` together, or neither.

        Why nothing else misses them: such a window started days ago, so it is never quoted again (candidate_problem) and
        its attempts and failures (the salt, MAX_ATTEMPTS) are asked for by nothing; its orders are final or were never
        placed, so the account can show none of them open (known, account); with no fill it has no position, no P&L and
        no bet (fills_by_outcome, daily_pnl, cards_today — today's windows — the EV, the tally). A card's latest window
        stays whatever its age: the listing horizon a controller reads off the records at a start is as it was.

        What does change: the status counts and measures the working records (实测: the last two days' and the windows not
        over); and two readers of ``rows`` in live.py find an archived record no more. /live cancel <order id> with the id
        of an archived record's order (LiveBot.live_cancel_command) used to withdraw that window's pair and pause this
        mode: the id is nobody's now ("没有订单号为 … 的真实挂单") and nothing is paused. And a market whose only records
        are archived is none of this bot's markets any more (LiveBot.live_known_markets: live_orphan_sweep withdraws an
        open order nobody recorded when it is on one of them, and only reports it otherwise). Neither is missed: an
        archived record's window ended two days ago and more — its market is over — and its own orders' signatures
        expired before the window began, so nothing of it can rest."""
        latest: dict[str, int] = {}
        for p in self.rows.values():
            key = str(p.get("key") or "")
            latest[key] = max(latest.get(key, 0), int(p.get("start_ms") or 0))
        before = now_ms - ARCHIVE_AFTER_MS
        gone = [slug for slug, p in self.rows.items() if archivable(p, before) and int(p.get("start_ms") or 0) < latest[str(p.get("key") or "")]]
        if gone:  # the table is made by the first move, not at a start: opening a ledger writes nothing it did not write before
            with self.conn:
                self.conn.execute("CREATE TABLE IF NOT EXISTS archive (slug TEXT PRIMARY KEY, body TEXT NOT NULL)")
        for at in range(0, len(gone), ARCHIVE_CHUNK):
            part = gone[at:at + ARCHIVE_CHUNK]
            held = self.archive_count()
            with self.conn:
                self.conn.executemany("INSERT INTO archive VALUES (?,?) ON CONFLICT(slug) DO UPDATE SET body=excluded.body",
                                      ((slug, json.dumps(self.rows[slug], ensure_ascii=False)) for slug in part))
                self.conn.executemany("DELETE FROM pairs WHERE slug=?", ((slug,) for slug in part))
            for slug in part:  # (on disk first: a chunk that could not be written leaves its records where they were)
                self.unindex(slug, self.rows.pop(slug))
            self.archived_count = self.archive_count()
            if self.archived_count - held != len(part):  # a record the archive held already — saved again after its move by a task that still had it — was replaced there, in place
                Ledger.rewrites += 1
        return len(gone)

    def has_archive(self) -> bool:
        return self.conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='archive'").fetchone() is not None

    def archive_count(self) -> int:
        """How many records the archive holds (none in a file nothing was moved in yet)."""
        return int(self.conn.execute("SELECT COUNT(*) FROM archive").fetchone()[0]) if self.has_archive() else 0

    def archived(self) -> Iterator[dict]:
        """The archived records, one at a time and never kept: the review page counts them so (LiveBot.preopen_review) when
        it cannot go by their kept counts (ArchiveShare)."""
        if self.has_archive():
            for (body,) in self.conn.execute("SELECT body FROM archive"):
                yield json.loads(body)

    def close(self) -> None:
        self.conn.close()

    def pause(self, value: bool | None = None) -> bool:
        if value is not None:
            with self.conn:
                self.conn.execute("INSERT INTO settings VALUES ('paused',?) ON CONFLICT(key) DO UPDATE SET body=excluded.body", (json.dumps(value),))
        row = self.conn.execute("SELECT body FROM settings WHERE key='paused'").fetchone()
        return bool(json.loads(row[0])) if row else False


class NotSent(core.RemoteError):
    """A local gate refused the request before its bytes were dispatched."""


def safe_api(base: str, key: str, wallet: Any, before_send: Any = None, shared: Any = None) -> Any:
    """A PredictApi for this mode: the bot's own request path, every answer checked for success, ``before_send`` judged
    right before an order's bytes go out, and the rate-limit cooldown shared with ``shared`` (the bot's own client): a 429
    on either pauses both."""
    import live

    class Api(live.PredictApi):
        ORDERS_PAGES_MAX = 600  # 30,000 open orders a read at 50 a page (1.136.1: the 60 of 1.131.2 — 3,000 — was passed at 11-07 01:20 by twenty cards, four levels, a day ahead; the snapshot never completed again and every placement was held)

        @property
        def blocked_until(self) -> float:
            own = self.__dict__.get("_blocked_until", 0.0)
            return max(own, getattr(shared, "blocked_until", 0.0)) if shared is not None else own

        @blocked_until.setter
        def blocked_until(self, value: float) -> None:
            self.__dict__["_blocked_until"] = value
            if shared is not None and value > getattr(shared, "blocked_until", 0.0):
                shared.blocked_until = value

        async def send(self, method: str, path: str, payload: Any, timeout: int) -> bytes:
            if path == "/orders" and method == "POST" and before_send is not None:
                why = before_send(payload)
                if why:
                    raise NotSent(why)
            # the bot's own request path (its headers, its pooled connections: a plain request without them is refused with a 403
            # by Predict's edge); the pool resends a POST only when the request could not be sent at all on a connection the
            # server had closed — nothing of it taken — never after an answer went missing
            return await super().send(method, path, payload, timeout)

        async def call(self, method: str, path: str, *args: Any, **kwargs: Any) -> Any:
            answer = await super().call(method, path, *args, **kwargs)
            # an explicit success=false is a refusal, whatever the status; a dict without the key passes, as the bot's own
            # client takes it (1.127.1: POST /orders/remove answers {"success": true, "removed": [...], "noop": [...]} with
            # no "data" key — requiring one read every cancel as a failure, and the withdrawn pairs kept their money)
            if not isinstance(answer, dict) or answer.get("success", True) is not True:
                raise core.RemoteError(f"Predict 应答不是已确认成功的数据（{method} {path.split('?')[0]}），保留订单状态与额度")
            return answer

    return Api(base, key, wallet)


def candidate_problem(market: dict, now_ms: int, config: Any, settings: Settings) -> str:
    """Why a listed market is not one of this mode's ("" when it is): only exact, listed slugs of the configured cards,
    starting within PREOPEN_AHEAD_MINUTES (any day) and not within PREOPEN's entry seconds, registered, visible, open or
    awaiting matching, with no maker fee, with an id, a condition and a question that agrees with the slug's time."""
    slug = str(market.get("categorySlug") or market.get("slug") or "")
    for rolling in core.preopen_cards():  # 1.142.0: the hourly and daily cards too, each reading its own slugs
        start = rolling.start_of(slug)
        if start is None:
            continue
        if rolling.key not in settings.markets:
            return "不在 PREOPEN_MARKETS 里"
        if not rolling.on_grid(start):
            return "链接里的时间不在场次边界上"
        if start - now_ms < settings.entry_seconds * 1000:
            return "已开始或不足 3 分钟"
        if start - now_ms > settings.ahead_minutes * 60_000:
            return f"开始还在 {settings.ahead_minutes} 分钟之外（之后再扫）"
        holidays = config.holidays.get(rolling.exchange, frozenset()) if rolling.exchange else frozenset()
        if not rolling.exists(start, holidays):
            return "不在交易时段内"
        if str(market.get("status") or "").upper() != "REGISTERED" or market.get("isVisible") is False:
            return f"状态 {market.get('status') or '?'}" + ("、不可见" if market.get("isVisible") is False else "")
        if str(market.get("tradingStatus") or "").upper() not in {"OPEN", "MATCHING_NOT_ENABLED"}:
            return f"交易状态 {market.get('tradingStatus') or '缺失'}"
        if str(market.get("makerFeeBps") or "0") not in {"0", "0.0"}:
            return f"挂单费率 {market.get('makerFeeBps')} bps"
        if not market.get("id") or not market.get("conditionId"):
            return "缺少 id 或 conditionId"
        if rolling.title_problem(str(market.get("question") or market.get("title") or ""), start):
            return "问题里的时间段和链接对不上"
        return ""
    return "不是涨跌卡（15 分钟、5 分钟、小时、日）的链接"


def candidate(market: dict, now_ms: int, config: Any, settings: Settings) -> dict | None:
    """The pair to place for a listed market (candidate_problem says why not), or None."""
    if candidate_problem(market, now_ms, config, settings):
        return None
    slug = str(market.get("categorySlug") or market.get("slug") or "")
    rolling, start = next((r, s) for r in core.preopen_cards() if (s := r.start_of(slug)) is not None)
    spec = rolling.window_spec(start)
    return {"slug": slug, "key": rolling.key, "market_id": str(market["id"]), "start_ms": start, "end_ms": rolling.end_of(start),  # 1.142.0: settled after the end
            "cancel_ms": start - settings.cancel_seconds * 1000, "expires": start // 1000 - settings.expiry_seconds,
            "day": dt.datetime.fromtimestamp(start / 1000, core.BEIJING).date().isoformat(), "name": spec.name}


def session_opening(rolling: Any, start_ms: int, holidays: frozenset, quiet_ms: int) -> bool:
    """Whether a stock card's window starts within ``quiet_ms`` of one of its exchange's session segments' open (the day's
    open, the afternoon's): the opening auction's indicative price and the break's news are public before the index moves,
    so that window's direction is no coin toss (1.135.1: not quoted, PREOPEN_OPEN_QUIET_MINUTES)."""
    if not rolling.exchange or quiet_ms <= 0:
        return False
    tz = dt.timezone(dt.timedelta(hours=core.STOCK_MARKETS[rolling.exchange].utc_offset))
    day = dt.datetime.fromtimestamp(start_ms / 1000, tz).date()
    return any(seg_open <= start_ms < seg_open + quiet_ms for seg_open, _ in core.session_segments(rolling.exchange, day, holidays))


WEEKEND_END_HOUR = 8  # the weekend runs to Monday this o'clock Beijing (1.139.2: Sunday evening in the US to the Asian open is crypto's thinnest stretch)


def on_weekend(ms: int) -> bool:
    """Whether the moment falls in the weekend — Saturday 00:00 to Monday WEEKEND_END_HOUR, Beijing time (PREOPEN_WEEKEND=off: no
    window starting then is quoted, 1.136.0; 1.139.2: to Monday 08:00, was Monday 00:00)."""
    at = dt.datetime.fromtimestamp(ms / 1000, core.BEIJING)
    return at.weekday() >= 5 or (at.weekday() == 0 and at.hour < WEEKEND_END_HOUR)


def window_starts(now_ms: int, config: Any, settings: Settings) -> list[tuple[Any, int]]:
    """The windows this mode looks for now: each configured card's quarter hours from PREOPEN_ENTRY_SECONDS ahead to
    PREOPEN_AHEAD_MINUTES ahead, whatever the day, on the card's exchange session, a stock card's first windows after each
    session open left out (session_opening), the weekend's when PREOPEN_WEEKEND is off — (card, start_ms), soonest first."""
    out = []
    quiet_ms = int(getattr(settings, "open_quiet_minutes", 0) or 0) * 60_000
    weekend = getattr(settings, "weekend", True)
    for rolling in core.preopen_cards():
        if rolling.key not in settings.markets:
            continue
        holidays = config.holidays.get(rolling.exchange, frozenset()) if rolling.exchange else frozenset()
        start = rolling.first_start(now_ms + settings.entry_seconds * 1000)
        while start <= now_ms + settings.ahead_minutes * 60_000:
            if rolling.exists(start, holidays) and not session_opening(rolling, start, holidays, quiet_ms) and (weekend or not on_weekend(start)):
                out.append((rolling, start))
            start = rolling.next_start(start)  # a quarter hour, five minutes, an hour, a day (noon ET to noon ET: 23 or 25 hours over a switch)
    return sorted(out, key=lambda rs: (rs[1], rs[0].key))


def window_slug(rolling: Any, start_ms: int) -> str:
    return rolling.slug_of(start_ms)


def settle_from(pair: dict) -> int:
    """When a pair's window is over and its result may be read: its end (1.142.0: an hour or a day after the start for the
    hourly and daily cards); a record from before kept no end — a quarter hour after its start, as then."""
    return int(pair.get("end_ms") or 0) or int(pair["start_ms"]) + 15 * 60_000


def hms(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, core.BEIJING).strftime("%H:%M:%S")


def when(ms: int, now_ms: int) -> str:
    """HH:MM (Beijing), with the date when it is not today's."""
    at, today = dt.datetime.fromtimestamp(ms / 1000, core.BEIJING), dt.datetime.fromtimestamp(now_ms / 1000, core.BEIJING).date()
    return at.strftime("%H:%M") if at.date() == today else at.strftime("%m-%d %H:%M")


def category_node(answer: Any) -> dict | None:
    """The market object inside a category answer, as the API sent it (the first with an id, a condition and outcomes);
    None when the answer carries none."""
    def walk(node: Any, depth: int = 0) -> dict | None:
        if depth > 8:
            return None
        if isinstance(node, dict):
            if node.get("id") is not None and "conditionId" in node and isinstance(node.get("outcomes"), list):
                return node
            for value in node.values():
                hit = walk(value, depth + 1)
                if hit is not None:
                    return hit
        elif isinstance(node, list):
            for value in node:
                hit = walk(value, depth + 1)
                if hit is not None:
                    return hit
        return None
    return walk(answer)


def listing_complete(node: dict) -> bool:
    """Whether a category's market object carries everything an order needs (LISTING_FULL_FIELDS, two outcomes with their
    tokens): placed on as it is, the details not read (1.133.0: one round trip less at a listing)."""
    outcomes = node.get("outcomes")
    return (all(k in node for k in LISTING_FULL_FIELDS) and isinstance(outcomes, list) and len(outcomes) >= 2
            and all(isinstance(o, dict) and o.get("onChainId") and o.get("indexSet") is not None for o in outcomes))


async def category_market(api: Any, slug: str) -> dict | None:
    """A window's market, by its category (GET /categories/<slug>: the read the bot's feed has always used for these
    markets), and then its details (GET /markets/<id>) unless the category's own market object carries every field the
    order needs (listing_complete); the slug put on it, ``_from`` saying which; None when the category is not listed (404).
    1.125.1: the market listing (/markets?marketVariant=…) answers 403 where the bot runs, as GraphQL does."""
    try:
        answer = await api.call("GET", f"/categories/{core.urllib.parse.quote(slug)}")
    except core.RemoteError as error:
        if "HTTP 404" in str(error):
            return None
        raise
    found = core.predict_markets(answer)
    if not found:
        return None
    node = category_node(answer)
    if node is not None and str(node.get("id")) == found[0]["id"] and listing_complete(node):
        return {**node, "categorySlug": slug, "id": str(node["id"]), "_from": "category"}
    market = await api.market(found[0]["id"])
    return {**market, "categorySlug": slug, "id": market.get("id") or found[0]["id"], "_from": "details"}


def fills_by_outcome(rows: list[dict], now_ms: int, days: int = 3) -> dict[tuple[str, int], float]:
    """The shares the mode's orders filled, by (market id, index set), for the windows of the last ``days`` days: the model
    trader's positions reconciliation counts them as recorded, not as fills of its own orders to adopt — whatever the
    mode's state (1.131.2: read off the ledger file while it is off)."""
    since = now_ms - days * core.DAY_MS
    out: dict[tuple[str, int], float] = {}
    for pair in rows:
        if int(pair.get("start_ms") or 0) < since or not pair.get("market_id"):
            continue
        for order in (pair.get("orders") or {}).values():
            if float(order.get("filled") or 0) > 0 and order.get("index_set"):
                key = (str(pair["market_id"]), int(order["index_set"]))
                out[key] = out.get(key, 0.0) + float(order["filled"])
    return out


def unverified_outcomes(rows: list[dict], now_ms: int, days: int = 3) -> set[tuple[str, int]]:
    """The outcomes (market id, index set) where an order of this mode was sent and its fills are not known for certain
    (not order_verified: placing, open, unknown, removed with the final record unread), the windows of the last ``days``
    days: a position there may be that order's fill, the ledger a look behind the account (1.131.5: the model trader's
    positions check booked such a fill onto its own order on the same side)."""
    since = now_ms - days * core.DAY_MS
    out: set[tuple[str, int]] = set()
    for pair in rows:
        if int(pair.get("start_ms") or 0) < since or not pair.get("market_id"):
            continue
        for order in (pair.get("orders") or {}).values():
            if order.get("index_set") and order.get("state") != "prepared" and not order_verified(order):
                out.add((str(pair["market_id"]), int(order["index_set"])))
    return out


def cards_today(rows: list[dict], now_ms: int) -> list[dict]:
    """The review page's 今日按市场 rows of this mode (1.131.5): today's windows (Beijing, by the window's start) per card,
    in the model trader's rows' shape (core.summary_of with settled_extras): the expectation at the order is a coin toss
    bought at the pair's price — 0.5 − price a filled share — the mean fair price 0.5, the hit rate the settled records'
    wins (a tie half)."""
    today = dt.datetime.fromtimestamp(now_ms / 1000, core.BEIJING).date().isoformat()
    by: dict[str, list[dict]] = {}
    for pair in rows:
        if any(o.get("state") not in NEVER_PLACED for o in (pair.get("orders") or {}).values()):
            q = quote_of(pair)
            if q["date"] == today:
                by.setdefault(q["card"], []).append(q)
    out = []
    for card, qs in by.items():
        r = review_summary(card, qs)
        settled = [q for q in qs if q["settled"]]
        out.append({"name": f"{card} · 开盘前预挂", **summary_shape(r), "preopen": True,
                    "expected": round(sum((0.5 - q["price"]) * q["filled"] for q in settled), 4), "avg_fair": 0.5 if settled else None,
                    "hit": (r["wins"] + 0.5 * r["ties"]) / r["settled"] if r["settled"] else None})
    return sorted(out, key=lambda x: (-x["records"], x["name"]))


def removable_ids(pair: dict) -> list[str]:
    """The pair's orders a removal can still withdraw (1.141.0): sent — an id — and not ended."""
    return [str(o["id"]) for o in (pair.get("orders") or {}).values() if o.get("id") and o.get("state") not in TERMINAL]


def resting_orders(pair: dict, now_ms: int) -> int:
    """The pair's orders that may be on the book now (1.141.0): in a state that may rest, the signed expiry still ahead; none
    for an accounted pair."""
    if pair.get("accounted"):
        return 0
    end = int(pair.get("expires") or 0) * 1000 or int(pair.get("start_ms") or 0)
    if now_ms >= end:
        return 0
    return sum(1 for o in (pair.get("orders") or {}).values() if o.get("state") in MAY_REST)


def awaiting_finals(pair: dict, now_ms: int, days: int = 3) -> int:
    """The pair's withdrawn orders whose final fills are still unread ("removed"), its window within unverified_outcomes'
    ``days``; none for an accounted pair.

    1.150.0 (Audit XR-PRELATE-CRASHED): PRE-LATE-FILL ends an order off the book only by a read made LATE_FILL_MS after it
    left, and leaves that read to the cancellation loop (remove_pairs skips launch_reconcile while finals_wait is ahead).
    A crashed controller — still attached, alive False — has no loop: an operator's withdrawal on it (/preopen pause, /live
    cancel all, the emergency stop: withdraw_all) leaves its orders "removed", and with the mode then off nothing ever read
    them — the wind-down start (LiveBot.preopen_leftovers) counted only orders that may rest (resting_orders, MAY_REST),
    which "removed" is not. Their pairs stayed unaccounted and their outcomes in unverified_outcomes for 3 days: the model
    trader's positions check held those outcomes all that time (live_late_fills skipped a genuine late fill of its own
    there past its 2-day window — exposure, P&L and the day's loss stop missed it; live_unexplained kept an unknown order
    reserved), where 1.149.0 read the records at the withdrawal. Counted here as well, they start a wind-down controller
    that only reads (nothing new goes out, nothing is left to cancel): its loop reads each record once LATE_FILL_MS has
    passed, the pair is accounted, it stops itself (wound_up) and, nothing left, is not started again. Should Predict's
    reads keep failing it stops after FINALS_WAIT_MS and the apply its stop calls starts it again — reads only, until a read
    succeeds, the give-up (a read past start + GIVE_UP_MS ends the order) or the window leaves this span."""
    if pair.get("accounted") or int(pair.get("start_ms") or 0) < now_ms - days * core.DAY_MS:
        return 0
    return sum(1 for o in (pair.get("orders") or {}).values() if o.get("state") == "removed")


def counts_against(pair: dict) -> bool:
    """Whether a withdrawn record counts against its window's MAX_ATTEMPTS: one that ended by its own failure (a side not
    confirmed, a side ended unfilled — an external cancel, a deferred refusal — the market's status) does; one withdrawn
    for what was not the window's doing (a settings change, the mode stopped, an operator's stop — the mode or LIVE
    paused, /live cancel all, the emergency stop, at any book size: Audit PRE-PAUSE-STRIKES — and the stock guard) does not
    (1.131.6: a day of settings changes used up the three quotes of every window listed the day before). Marked on the
    record (free); an older record by its reason."""
    if pair.get("free"):
        return False
    reason = str(pair.get("reason") or "")
    return not any(word in reason for word in FREE_REASONS)


def window_strikes(rows: Any) -> dict[str, int]:
    """Record key (a window's level) -> its quotes that ended by its own failure (counts_against), the superseded ones included."""
    out: dict[str, int] = {}
    for p in rows:
        if p.get("accounted") and p.get("phase") != "active" and counts_against(p):
            out[record_key(p)] = out.get(record_key(p), 0) + 1
    return out


def order_brief(row: dict) -> str:
    """An open order of the account in a few words (/preopen check: the ones neither strategy recorded): its id, side,
    price and shares as the record states them, the market's title when it carries one."""
    import live
    order = row.get("order") if isinstance(row.get("order"), dict) else {}
    market = row.get("market") if isinstance(row.get("market"), dict) else {}
    try:
        maker, taker = D(str(order.get("makerAmount") or 0)), D(str(order.get("takerAmount") or 0))
        buy = int(order.get("side") or 0) == 0
        shares = live.from_wei(int(str(row.get("amount") or (taker if buy else maker) or 0)))
        price = float((maker / taker) if buy and taker else (taker / maker) if maker else 0)
    except (ValueError, ArithmeticError, TypeError):
        shares, price, buy = 0.0, 0.0, True
    title = str(market.get("title") or market.get("question") or market.get("slug") or "")
    return f"#{row.get('id')} {'买' if buy else '卖'} {core.cents(price) if price else '?'}×{shares:g}" + (f" {title[:28]}" if title else "") + f"（{row.get('status') or '?'}）"


def record_key(pair: dict) -> str:
    """A record's key without its attempt (a superseded record's key carries the attempt after '@'): the window's slug,
    with '#<level>' for a deeper price level (1.129.0)."""
    return str(pair.get("slug") or "").split("@")[0]


def window_of(pair: dict) -> str:
    """The window's slug behind a record's key."""
    return record_key(pair).split("#")[0]


def levels_text(settings: Settings) -> str:
    """The levels as the status and the probe say them: 49.0¢×10 份、48.0¢×20 份 …, and when the deeper ones wait for the start."""
    text = "、".join(f"{core.cents(float(p))}×{float(s):g} 份" for p, s in settings.levels)
    if len(settings.levels) > 1 and settings.deep_ahead_minutes < settings.ahead_minutes:
        text += f"（第 2 档起只挂 {settings.deep_ahead_minutes} 分钟内开始的场次）"
    return text


def pair_quote(pair: dict) -> tuple[float, float]:
    """A pair's price and shares a side: its own fields (1.128.0), else read off a signed order (older records)."""
    import live
    if pair.get("price") and pair.get("shares"):
        return float(pair["price"]), float(pair["shares"])
    for order in (pair.get("orders") or {}).values():
        data = (order.get("body") or {}).get("data") or {}
        signed = data.get("order") or {}
        if signed.get("takerAmount") and data.get("pricePerShare"):
            return float(D(str(data["pricePerShare"])) / live.WEI), float(D(str(signed["takerAmount"])) / live.WEI)
    return 0.0, 0.0


def read_rows(path: str) -> list[dict]:
    """The ledger's pairs read while the mode is off (no controller holds the file): every working record, as saved (the
    old finished quotes that filled nothing are in the archive: read_archive)."""
    if path == ":memory:" or not Path(path).exists():
        return []
    conn = sqlite3.connect(path)
    try:
        return [json.loads(body) for (body,) in conn.execute("SELECT body FROM pairs")]
    finally:
        conn.close()


def read_archive(path: str) -> Iterator[dict]:
    """The ledger file's archived records (Ledger.archive_old) while the mode is off, one at a time and never kept: the
    review page counts them so (LiveBot.preopen_review) when it cannot go by their kept counts (ArchiveShare). None in a
    file from before the archive."""
    if path == ":memory:" or not Path(path).exists():
        return
    conn = sqlite3.connect(path)
    try:
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='archive'").fetchone():
            for (body,) in conn.execute("SELECT body FROM archive"):
                yield json.loads(body)
    finally:
        conn.close()


def archive_file(path: str, now_ms: int) -> int:
    """Ledger.archive_old on the ledger file while no controller holds it (the mode off: LiveBot.preopen_tidy) — the
    positions check and the review page read that file's working records whole, twice a minute and six times a build;
    how many records moved. Nothing without a file (none is made)."""
    if path == ":memory:" or not Path(path).exists():
        return 0
    ledger = Ledger(path)
    try:
        return ledger.archive_old(now_ms)
    finally:
        ledger.close()


def level_name(price: float, shares: float) -> str:
    """A price level as the review's tables name it: 49¢×10."""
    return f"{price * 100:g}¢×{shares:g}"


def quote_of(pair: dict) -> dict:
    """One record as the review counts it: a window, an attempt, two sides at one price; settled = accounted with a fill
    (the official result priced it), pending = a fill the result has not priced yet, withdrawn = accounted with none."""
    price, shares = pair_quote(pair)
    orders = pair.get("orders") or {}
    filled = {side: float((orders.get(side) or {}).get("filled") or 0) for side in ("up", "down")}
    total = filled["up"] + filled["down"]
    accounted = bool(pair.get("accounted"))
    result = pair.get("result") if pair.get("result") in {"up", "down", "split"} else ""
    if not result and accounted and total > 0 and (filled["up"] > 0) != (filled["down"] > 0):
        # a record settled before 1.128.0 (no result kept): one side filled, so its payout — pnl / filled + price — says
        side = "up" if filled["up"] > 0 else "down"
        payout = float(pair.get("pnl") or 0) / filled[side] + price
        if abs(payout - 0.5) < 0.01:
            result = "split"
        elif abs(payout - 1) < 0.01 or abs(payout) < 0.01:
            result = side if abs(payout - 1) < 0.01 else ("down" if side == "up" else "up")
    payout = {"up": 1.0, "down": 0.0, "split": 0.5}.get(result)
    start = int(pair.get("start_ms") or 0)
    at = dt.datetime.fromtimestamp(start / 1000, core.BEIJING)
    return {"window": window_of(pair), "key": str(pair.get("key") or ""), "card": str(pair.get("name") or pair.get("key") or "").rsplit(" ", 1)[0],
            "name": str(pair.get("name") or window_of(pair)), "day": at.strftime("%m-%d"), "date": at.date().isoformat(), "hour": at.hour, "start_ms": start,
            "level_name": level_name(price, shares),
            "active": pair.get("phase") == "active", "accounted": accounted, "filled": total, "filled_up": filled["up"], "filled_down": filled["down"],
            "sent": {side: side in orders and orders[side].get("state") not in NEVER_PLACED for side in ("up", "down")},
            "price": price, "shares": shares, "volume": total * price, "settled": accounted and total > 0, "pending": total > 0 and not accounted,
            "pnl": float(pair.get("pnl") or 0) if accounted and total > 0 else None, "result": result,
            "side_pnl": {side: ((payout if side == "up" else 1 - payout) - price) * filled[side] if payout is not None else None for side in ("up", "down")},
            "attempt": int(pair.get("attempt") or 0),
            "lead_min": (start - int(pair["created_ms"])) / 60_000 if pair.get("created_ms") else None,
            "cancel_s": (int(pair["cancel_done_ms"]) - int(pair["cancel_sent_ms"])) / 1000 if pair.get("cancel_sent_ms") and pair.get("cancel_done_ms") else None,
            "status": ("挂着" if pair.get("phase") == "active" else "已结算" if accounted and total > 0 else "已撤" if accounted
                       else "待结算" if total > 0 else "撤单中")}


def review_summary(name: str, qs: list[dict]) -> dict:
    settled = [q for q in qs if q["settled"]]
    return {"name": name, "records": len(qs), "windows": len({q["window"] for q in qs}), "resting": sum(1 for q in qs if q["active"]),
            "filled_windows": sum(1 for q in qs if q["filled"] > 0), "filled": round(sum(q["filled"] for q in qs), 4),
            "volume": round(sum(q["volume"] for q in qs), 4), "pending": sum(1 for q in qs if q["pending"]),
            "settled": len(settled), "wins": sum(1 for q in settled if q["pnl"] > 1e-9), "losses": sum(1 for q in settled if q["pnl"] < -1e-9),
            "ties": sum(1 for q in settled if abs(q["pnl"]) <= 1e-9), "pnl": round(sum(q["pnl"] for q in settled), 4),
            "pending_cost": round(sum(q["volume"] for q in qs if q["pending"]), 4),
            "withdrawn": sum(1 for q in qs if q["accounted"] and q["filled"] <= 0)}


def summary_shape(r: dict) -> dict:
    """A review_summary row in the daily summary's shape (core.summary_of): the pending fills are its open positions, the
    resting pairs its resting orders; nothing is marked."""
    return {"records": r["records"], "markets": r["windows"], "filled": r["filled"], "volume": r["volume"], "settled": r["settled"],
            "wins": r["wins"], "losses": r["losses"], "ties": r["ties"], "pnl": r["pnl"], "open": r["pending"], "open_cost": r["pending_cost"],
            "resting": r["resting"], "marked": 0, "unrealized": 0.0}


def review_side(name: str, side: str, qs: list[dict]) -> dict:
    """One side over the records: the orders sent, those that filled, and what the filled ones made once the result is known."""
    sent = [q for q in qs if q["sent"][side]]
    filled = [q for q in sent if q[f"filled_{side}"] > 0]
    pnls = [q["side_pnl"][side] for q in filled if q["side_pnl"][side] is not None]
    return {"name": name, "records": len(sent), "filled_windows": len(filled), "filled": round(sum(q[f"filled_{side}"] for q in sent), 4),
            "volume": round(sum(q[f"filled_{side}"] * q["price"] for q in sent), 4), "settled": len(pnls),
            "wins": sum(1 for x in pnls if x > 1e-9), "losses": sum(1 for x in pnls if x < -1e-9), "ties": sum(1 for x in pnls if abs(x) <= 1e-9),
            "pnl": round(sum(pnls), 4)}


def ev_bets(rows: list[dict]) -> list[dict]:
    """The mode's settled fills as bets for the review page's 模型 EV 对实际盈亏 (core.sim_ev_timeline, 1.140.0): each accounted
    pair (one window) is one bet at the pair's price over its filled sides — EV = (0.5 − price) × the shares filled, P&L the
    filled sides' own (side_pnl) summed, the variance that of the unhedged part: (filled_up − filled_down)² / 4.
    1.150.x: the window's two sides settle on the same coin flip X, so the pair pays filled_up · X + filled_down · (1 − X) − cost
    and only the difference of the fills is at risk; a window filled alike on both sides is a near-certain payoff. Counted as two
    independent flips (filled² / 4 each, as before) it added variance it does not have — σ grew, z was pulled to 0 and the 激进
    verdict (and the hourly "模型把优势看高了" notice) could be hidden behind the hedged windows; it also counted the window twice
    toward EV_MIN_BETS. A one-sided window keeps core.ev_bet's filled² / 4. A pair without a sent order, an unsettled one or a
    side whose result is not kept is no bet (a window none of whose filled sides is kept is skipped).
    1.150.0: one bet per WINDOW, not per record. With PREOPEN_LEVELS a window is several records (record_key <window>#<level>)
    and a re-quote leaves superseded ones ('@<attempt>'), all on the same market and settled by the same coin flip X, so the
    window pays Σup · X + Σdown · (1 − X) − cost: one sample of variance (Σup − Σdown)² / 4. Netted per record only, a window
    filled Up on one level and Down on another still got the phantom variance (and the double count toward EV_MIN_BETS) the
    note above removed, and levels filled on the same side were taken for independent flips (2 × 100 instead of 400): σ too
    small and the window counted twice. Grouped by window_of the effect on σ goes both ways — hedged levels lose the variance
    they do not have (激进 sooner), same-side levels gain their true correlation and count once (激进 later) — both the true
    statistics under the model's own p = 0.5. The bet's price is the share-weighted average (display only)."""
    windows: dict[str, dict] = {}
    for pair in rows:
        if not any(o.get("state") not in NEVER_PLACED for o in (pair.get("orders") or {}).values()):
            continue
        q = quote_of(pair)
        if not q["settled"]:
            continue
        kept = {side: (q[f"filled_{side}"], q["side_pnl"][side]) for side in ("up", "down")
                if q[f"filled_{side}"] > 0 and q["side_pnl"][side] is not None}
        if not kept:
            continue
        w = windows.setdefault(q["window"], {"q": q, "up": 0.0, "down": 0.0, "ev": 0.0, "pnl": 0.0, "cost": 0.0})
        for side, (f, x) in kept.items():
            w[side] += f
            w["ev"] += (0.5 - q["price"]) * f
            w["pnl"] += x
            w["cost"] += q["price"] * f
    out = []
    for w in windows.values():
        filled = w["up"] + w["down"]
        bet = core.ev_bet(w["q"]["start_ms"], "预挂", w["q"]["card"], w["cost"] / filled, filled, 0.5, w["ev"], w["pnl"])
        bet["var"] = (w["up"] - w["down"]) ** 2 * 0.25  # the unhedged part; a one-sided window keeps core.ev_bet's filled² / 4
        out.append(bet)
    return out


def review(rows: Iterable[dict], now_ms: int, old: ArchiveShare | None = None) -> dict:
    """The review page's 开盘前预挂 section (journal.json analysis.preopen): what this mode's quotes made — the totals, by the
    window's day, card, start hour (Beijing) and side, and the windows that filled, newest first. A record is a pair
    (one window, one attempt) at least one order of which was sent; a window's two sides are two bets at the pair's price.
    ``rows`` are walked once (the archived records come one at a time: LiveBot.preopen_review). ``old``: the archived
    records counted in by their kept counts instead (ArchiveShare — one read for these ``rows`` that fits ``now_ms``):
    every figure, and the order of the lines, as if the archived records themselves followed ``rows``."""
    qs = [quote_of(p) for p in rows if any(o.get("state") not in NEVER_PLACED for o in (p.get("orders") or {}).values())]
    days: dict[str, list[dict]] = {}
    dates: dict[str, list[dict]] = {}
    cards: dict[str, list[dict]] = {}
    levels: dict[tuple[float, float], list[dict]] = {}
    hours: dict[tuple[str, int], list[dict]] = {}
    names = {q["card"] for q in qs} | set(old.lines["card"] if old is not None else ())
    many = len(names) > 1
    for q in qs:
        days.setdefault(q["day"], []).append(q)
        dates.setdefault(q["date"], []).append(q)
        cards.setdefault(q["card"], []).append(q)
        levels.setdefault((q["price"], q["shares"]), []).append(q)
        hours.setdefault((q["card"] if many else "", q["hour"]), []).append(q)
    if old is not None:  # the lines only archived records are on: no record of ``rows``, the archive's counts alone
        for kind, lines in (("day", days), ("date", dates), ("card", cards), ("level", levels)):
            for key in old.lines[kind]:
                lines.setdefault(key, [])
        for card, hour in old.lines["hour"]:
            hours.setdefault((card if many else "", hour), [])
    only = "" if many else next(iter(names), "")  # the one card there is, whose hours stand without its name

    def summary(name: str, group: list[dict], kind: str, key: Any) -> dict:
        r = review_summary(name, group)
        return old.counted(r, kind, key, group) if old is not None else r

    def side_line(name: str, side: str) -> dict:
        r = review_side(name, side, qs)
        return old.sided(r, side) if old is not None else r

    def latest(day: str) -> int:  # the day's latest window (an archived one too)
        starts = [q["start_ms"] for q in days[day]]
        if old is not None and day in old.lines["day"]:
            starts.append(old.lines["day"][day].latest)
        return max(starts)

    def size(card: str) -> int:  # the card's records (the archived ones too)
        return len(cards[card]) + (old.lines["card"][card].records if old is not None and card in old.lines["card"] else 0)
    recent = sorted([q for q in qs if q["filled"] > 0], key=lambda q: q["start_ms"], reverse=True)[:60]
    shown = ("start_ms", "name", "key", "filled_up", "filled_down", "price", "volume", "result", "pnl", "status", "lead_min", "cancel_s", "attempt")
    total = summary("全部", qs, "all", "")
    today = dt.datetime.fromtimestamp(now_ms / 1000, core.BEIJING).date().isoformat()  # the daily summary takes today's and earlier windows: tomorrow's resting pairs are no day yet
    return {"total": total,
            "daily": {"days": {date: summary_shape(summary(date, dates[date], "date", date)) for date in dates if date <= today},
                      "total": summary_shape(summary("全部", [q for q in qs if q["date"] <= today], "all", ""))},  # the daily summary's share (core.merge_preopen_summary)
            "resting_usd": round(sum(q["price"] * q["shares"] * sum(q["sent"].values()) for q in qs if q["active"]), 4),
            "days": [summary(day, days[day], "day", day) for day in sorted(days, key=latest, reverse=True)],
            "cards": [summary(card, cards[card], "card", card) for card in sorted(cards, key=lambda c: (-size(c), c))],
            "levels": [summary(levels[k][0]["level_name"] if levels[k] else level_name(*k), levels[k], "level", k) for k in sorted(levels, reverse=True)],  # the highest bid first
            "hours": [summary(f"{card} {hour:02d} 时开始".strip(), hours[(card, hour)], "hour", (card if many else only, hour)) for card, hour in sorted(hours)],
            "sides": [side_line(name, side) for side, name in (("up", "涨"), ("down", "跌"))],
            # 1.139.2: the weekend's windows against the weekdays' — the weekend as PREOPEN_WEEKEND draws it (on_weekend)
            "weekly": [summary(f"周末（周六 00:00 到周一 {WEEKEND_END_HOUR:02d}:00）", [q for q in qs if on_weekend(q["start_ms"])], "week", True),
                       summary("工作日", [q for q in qs if not on_weekend(q["start_ms"])], "week", False)],
            "recent": [{k: q[k] for k in shown} for q in recent]}


REVIEW_KINDS = ("all", "date", "day", "card", "level", "hour", "week")  # the review's tables, as ArchiveShare keeps the archive's lines of each


def review_lines(q: dict) -> tuple:
    """The lines of the review's tables a quote is counted on, as (kind, key) — one of each of REVIEW_KINDS: the total, its
    window's date (the daily summary) and day, its card, its level, its card's start hour, the weekend or the week."""
    return (("all", ""), ("date", q["date"]), ("day", q["day"]), ("card", q["card"]), ("level", (q["price"], q["shares"])),
            ("hour", (q["card"], q["hour"])), ("week", on_weekend(q["start_ms"])))


class ArchiveLine:
    """What the archive adds to one line of a review table: so many records, of so many windows, the latest starting then."""
    __slots__ = ("records", "windows", "latest")

    def __init__(self) -> None:
        self.records = self.windows = 0
        self.latest: float = -math.inf


class ArchiveShare:
    """The archive's share of the review's figures (review's ``old``), kept from one call to the next by
    LiveBot.preopen_review. Audit PRE-LEDGER-GROWTH, the follow-up: the page counts the archived records as it always
    did, and for that every call read the whole archive off the file and parsed it, record by record, on the bot's one
    event loop — twice a build of journal.json, a build a minute while the page is open: at 42,000 archived two seconds a
    call where the same history, all in memory, had taken one, and the one cost left that grew with the history.

    An archived record is a finished quote that filled nothing (archivable). All it adds to any table is a record, a
    withdrawn one, its window, and an order sent on a side — so the archive is read once, those counts are kept by table
    and line (``lines``, ``sent``), and from then on only the rows moved in since are read (``top``: a pass appends, the
    rowids grow). Windows are the one thing counts do not add up: a window with an archived record and a working one
    (its other level filled) is one window. ``known`` says, for the windows of the working records alone, on which lines
    their archived records are counted; a window it does not hold is looked up in the archive by its keys (learn). So
    what is kept grows with the working records and the days of history, not with the archive.

    The archive is read whole again (read) when its rows did not only grow: a record of it replaced in place by a pass
    (Ledger.rewrites); rows gone or put in by another hand (the last row counted is no longer where it was, or the rows
    do not add up to the count); or other records than before in both tables — those are counted as the working records
    they are and passed by here (``skip``). What another hand changes leaving every key where it was — a body rewritten
    in place — is not seen before the next start. And the counts are not used at all (fits) when the archive holds a
    record that is no plain withdrawn quote, or one of a window after today's: the page reads the archive record by
    record then, as it did. After a start the first call reads the archive whole, once."""
    def __init__(self, path: str = "") -> None:
        self.path = path
        self.top = self.count = 0  # the archive's rows counted here: those up to this rowid, this many
        self.last: tuple | None = None  # ...the last of them, as (its key,)
        self.skip: frozenset[str] = frozenset()  # the keys that were working records' too when it was read: counted there, not here
        self.rewrites = Ledger.rewrites
        self.plain = True  # every record counted is a plain withdrawn quote (quote)
        self.lines: dict[str, dict[Any, ArchiveLine]] = {kind: {} for kind in REVIEW_KINDS}
        self.sent = {"up": 0, "down": 0}  # the records counted with an order sent on the side
        self.known: dict[str, tuple] = {}  # window -> the lines its archived records are counted on: the working records' windows (every window of the archive while it is read whole)

    def quote(self, slug: str, pair: dict) -> dict | None:
        """An archived record as the review counts it (quote_of); None for one not counted here: no order of it ever placed
        (review passes those by), a working record too (``skip``), or no plain withdrawn quote under its own key — which
        leaves these counts no use (``plain``)."""
        if not any(o.get("state") not in NEVER_PLACED for o in (pair.get("orders") or {}).values()):
            return None
        if pair.get("slug") != slug:  # (the working records are told from the archived ones by their keys)
            self.plain = False
            return None
        if slug in self.skip:
            return None
        q = quote_of(pair)
        if (q["active"] or not q["accounted"] or q["filled_up"] != 0 or q["filled_down"] != 0
                or not (0 <= q["price"] < math.inf and math.isfinite(q["shares"]))):  # (a price that is a number: nothing filled at it is no volume)
            self.plain = False
            return None
        return q

    def add(self, slug: str, pair: dict) -> None:
        """One more archived record counted: a record on each of its lines, and a window on those where no archived record of
        its window was counted before (``known`` holds the window, or the archive has no earlier record of it)."""
        q = self.quote(slug, pair)
        if q is None:
            return
        seen = self.known.get(q["window"], ())
        for kind, key in review_lines(q):
            line = self.lines[kind].get(key)
            if line is None:
                line = self.lines[kind][key] = ArchiveLine()
            line.records += 1
            line.latest = max(line.latest, q["start_ms"])
            if line not in seen:
                line.windows += 1
                seen += (line,)
        self.known[q["window"]] = seen
        for side in ("up", "down"):
            self.sent[side] += q["sent"][side]

    def learn(self, conn: sqlite3.Connection, window: str) -> None:
        """``known`` for a window it does not hold: the lines the archive's records of that window are counted on, read off
        the archive by the keys a window's records have — <window>, <window>#<level>, either with @<attempt>
        (record_key, window_of) — among the rows counted (those up to ``top``)."""
        seen: tuple = ()
        for slug, body in conn.execute("SELECT slug, body FROM archive WHERE rowid <= ? AND (slug = ? OR (slug >= ? AND slug < ?) OR (slug >= ? AND slug < ?))",
                                       (self.top, window, window + "#", window + "$", window + "@", window + "A")):
            q = self.quote(slug, json.loads(body))
            if q is not None and q["window"] == window:
                seen += tuple(line for kind, key in review_lines(q) if (line := self.lines[kind][key]) not in seen)
        self.known[window] = seen

    @classmethod
    def read(cls, conn: sqlite3.Connection, path: str, rows: list[dict], kept: ArchiveShare | None = None) -> ArchiveShare:
        """The archive's share as the archive is now, for the working records ``rows``: ``kept`` — the last call's — with the
        rows moved in since counted in (none read when there are none), or the archive read whole (the first call, a
        ledger in memory, another file, and whenever its rows did not only grow: the class's own words). ``known`` then
        holds the windows of ``rows`` and no other. ``kept`` is changed in place: after an error it is no use."""
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='archive'").fetchone() is None:
            return cls(path)  # (a file nothing was moved in yet)
        count = conn.execute("SELECT COUNT(*) FROM archive").fetchone()[0]
        mine = [slug for slug in {p.get("slug") for p in rows} if isinstance(slug, str)]
        skip = frozenset(slug for at in range(0, len(mine), 500) for (slug,) in conn.execute(
            f"SELECT slug FROM archive WHERE slug IN ({','.join('?' * len(mine[at:at + 500]))})", mine[at:at + 500]))
        share = kept
        # still the rows it counted, and nothing but rows after them: the last of them is where it was, and they and the later ones are all there is
        fresh = (share is None or not share.count or path == ":memory:" or share.path != path or share.rewrites != Ledger.rewrites or share.skip != skip
                 or conn.execute("SELECT slug FROM archive WHERE rowid = ?", (share.top,)).fetchone() != share.last
                 or share.count + conn.execute("SELECT COUNT(*) FROM archive WHERE rowid > ?", (share.top,)).fetchone()[0] != count)
        if fresh:
            share = cls(path)
            share.skip = skip
        reached, added = share.top, 0
        for rowid, slug, body in conn.execute("SELECT rowid, slug, body FROM archive WHERE rowid > ? ORDER BY rowid", (share.top,)):
            pair = json.loads(body)
            if not fresh and window_of(pair) not in share.known:  # (a window quoted and archived since the last call: an earlier record of it would be among the rows counted)
                share.learn(conn, window_of(pair))
            share.add(slug, pair)
            reached, added, share.last = rowid, added + 1, (slug,)
        share.top, share.count = reached, share.count + added
        windows = {window_of(p) for p in rows}
        for window in windows - share.known.keys():
            if fresh:
                share.known[window] = ()  # (every window of the archive is held just now: this one has no record there)
            else:
                share.learn(conn, window)
        share.known = {window: share.known[window] for window in windows}
        return share

    def fits(self, now_ms: int) -> bool:
        """Whether these counts can stand for the archive's records in review(…, now_ms): each a plain withdrawn quote
        (``plain``), none of a window after today's — the daily summary's total takes today's and earlier (a clock set
        back)."""
        today = dt.datetime.fromtimestamp(now_ms / 1000, core.BEIJING).date().isoformat()
        return self.plain and all(date <= today for date in self.lines["date"])

    def counted(self, r: dict, kind: str, key: Any, qs: list[dict]) -> dict:
        """A review_summary line of the records ``qs`` with the archive's records of the same line counted in: that many more
        records, withdrawn every one, and their windows but those ``qs`` has a record of too. (The sums of a line are
        floats once a record was summed: 0.0, not 0, on a line of archived records alone.)"""
        line = self.lines[kind].get(key)
        if line is not None:
            r["records"] += line.records
            r["withdrawn"] += line.records
            r["windows"] += line.windows - sum(1 for window in {q["window"] for q in qs} if line in self.known[window])
            r["filled"] += 0.0
            r["volume"] += 0.0
        return r

    def sided(self, r: dict, side: str) -> dict:
        """A review_side line with the archive's orders sent on that side counted in (none of them filled)."""
        if self.sent[side]:
            r["records"] += self.sent[side]
            r["filled"] += 0.0
            r["volume"] += 0.0
        return r


def archive_share(source: Ledger | str, rows: list[dict], kept: ArchiveShare | None = None) -> ArchiveShare:
    """ArchiveShare.read on a ledger — a controller's, or while the mode is off the file's path (none there: an empty
    share) — whose working records are ``rows``; ``kept`` is what the last call returned (LiveBot.preopen_review)."""
    if not isinstance(source, str):
        return ArchiveShare.read(source.conn, source.path, rows, kept)
    if source == ":memory:" or not Path(source).exists():
        return ArchiveShare(source)
    conn = sqlite3.connect(source)
    try:
        return ArchiveShare.read(conn, source, rows, kept)
    finally:
        conn.close()


LISTING_CYCLE_MS = 15 * 60_000  # Predict lists one more window of a card every quarter hour (a day ahead): the next listing is expected a cycle after the last seen


def cycle_ms(key: str) -> int:
    """How often Predict lists one more window of a card: its window length (a 5-minute card's every five minutes, 1.137.0)."""
    card = next((r for r in core.preopen_cards() if r.key == key), None)
    return int(card.cycle_ms) if card is not None else LISTING_CYCLE_MS
LISTING_EARLY_MS, LISTING_LATE_MS = 30_000, 120_000  # ...and the discovery polls fast from this long before that moment until this long after it (or the listing)
FAST_POLL_S = 0.5               # the fast poll's cadence (the queue position: the bid goes in within a second of the listing; 1.133.0: was 1 s)
LISTING_PROBE_S = 2.0           # ...and each card's next listing is probed this often the rest of the time (was the poll setting, 5 s); the older windows keep the poll's pace
PLACE_WORKERS = 4               # pairs placed side by side (three cards list at once, four levels each: twelve pairs — two workers took seconds)
MARKET_FRESH_MS = 15_000        # a window's details read by the discovery this recently are placed on, not read again
DISCOVER_READS = 8              # listed windows with a level still to quote read in one discovery pass, the soonest first (the rest the next pass); each card's next listing is read besides (1.132.2)
ALLOWANCE_MS = 60_000           # the exchange allowance is read again this often, not before every pair
PROBE_HOURS = 2.0      # /preopen probe: the quarters of the next this many hours, each card's at most PROBE_PER_CARD — five cards over a day
PROBE_PER_CARD = 8     # were hundreds of reads, and the commands are answered one after another (1.130.6)
PROBE_BUDGET_S = 15.0  # ...and this long at most: what is left is said, not read


async def probe(api: Any, config: Any, settings: Settings, now_ms: int, hours: float = PROBE_HOURS) -> list[str]:
    """The next hours' windows, read as the discovery reads them: listed or not, the details' trading status, the fields the
    discovery needs that are missing, whether it would be placed and why not — run before switching on, and to see whether
    a card is listed under the slug the bot expects. Bounded (PROBE_PER_CARD a card, two reads at a time, PROBE_BUDGET_S)."""
    hm = lambda ms: when(ms, now_ms)
    lines = [f"开盘前预挂探测（只读；设置：{'、'.join(sorted(settings.markets)) or '没选卡'}，{levels_text(settings)}，"
             f"提前 {settings.ahead_minutes} 分钟内，不足 {settings.entry_seconds} 秒不挂；只看 {hours:g} 小时内、每张卡最近 {PROBE_PER_CARD} 场）"]
    per_card: dict[str, int] = {}
    picked = []
    for rolling, start in window_starts(now_ms, config, dataclasses.replace(settings, ahead_minutes=max(3, int(hours * 60)))):
        if per_card.get(rolling.key, 0) < PROBE_PER_CARD:
            per_card[rolling.key] = per_card.get(rolling.key, 0) + 1
            picked.append((rolling, start))
    if not picked:
        lines.append(f"{hours:g} 小时内没有要看的场次（不在交易时段）")
    gate = asyncio.Semaphore(2)
    deadline = time.monotonic() + PROBE_BUDGET_S
    failures: dict[str, int] = {}

    async def one(rolling: Any, start: int) -> str:
        slug = window_slug(rolling, start)
        async with gate:
            if time.monotonic() > deadline:
                return f"{slug}（{hm(start)}）：没问（探测限 {PROBE_BUDGET_S:g} 秒）"
            if failures.get(rolling.key, 0) >= 2:
                return f"{slug}（{hm(start)}）：没问（{rolling.key} 连续读取失败）"
            try:
                market = await category_market(api, slug)
            except Exception as error:
                failures[rolling.key] = failures.get(rolling.key, 0) + 1
                return f"{slug}（{hm(start)}）：读取失败：{core.clean_error(error) or type(error).__name__}"
        if market is None:
            return f"{slug}（{hm(start)}）：还没发布"
        why = candidate_problem(market, now_ms, config, settings)
        missing = [f for f in LISTING_FIELDS if f not in market]
        return (f"{slug}（{hm(start)}）：{'会挂' if not why else '不挂：' + why}；状态 {market.get('status') or '缺失'} / 交易状态 "
                f"{market.get('tradingStatus') or '缺失'}；缺字段：{'、'.join(missing) if missing else '无'}"
                + ("；分类应答自带下单字段" if market.get("_from") == "category" else "；下单字段要另读详情"))
    lines += list(await asyncio.gather(*(one(r, s) for r, s in picked)))
    return lines


class Controller:
    """Discovery and cancellation run independently; a placement backlog cannot delay cancels."""
    def __init__(self, owner: Any):
        import live
        self.owner = owner
        self.settings = Settings.from_config(owner.config)
        self.ledger = Ledger(self.settings.db)
        self.errors: collections.deque = collections.deque(maxlen=ERRORS_KEPT)  # (ms, text): the last problems (the error setter)
        self._error = ""
        self.error = ""
        self.alive = True
        self.limited_at: collections.deque = collections.deque(maxlen=50)  # when a cooldown began (a 429 on the shared client)
        self.in_cooldown = False
        self.stuck_checked_ms = 0
        self.stop = asyncio.Event()  # this mode's own stop (the page switching it off, a settings change): its run() withdraws the orders on the way out
        self.manual_pause = self.ledger.pause()
        self.archived_ms = 0  # when the old finished records were last moved to the archive (archive: now, then every ARCHIVE_EVERY_MS)
        self.archive(self.now())
        self.account_at = 0
        self.balance = 0.0
        self.open_reserved = 0.0
        self.listed_ids: set[str] = set()
        self.unmanaged = 0  # open orders on the account that neither strategy placed: shown; their money is in the balance check (no hold since 1.126.0)
        self.unmanaged_detail: list[str] = []  # ...the first few described (/preopen check)
        self.placing: set[str] = set()
        self.cancelling: set[str] = set()
        self.queue: asyncio.PriorityQueue = asyncio.PriorityQueue()  # (start_ms, slug, candidate): the soonest window first
        self.queued: set[str] = set()
        self.skipped: dict[str, str] = {}  # slug -> why a listed window was not placed (the status line)
        self.skipped_at: dict[str, int] = {}  # slug -> when it was last judged so (asked again SKIP_RETRY_MS on)
        self.scan_note = ""  # what the last discovery pass did (the status line): windows read, levels found, what waits, what stopped it
        self.scan_state: dict = {"reads": 0, "asked": 0, "found": 0, "deferred": 0, "pending": 0, "stopped": "", "probed": 0}  # ...its counts
        self.rescan_ms = 0  # when the known-listed windows were last re-read (reread_known: at the poll's pace, between listings)
        self.listing_deltas: collections.deque = collections.deque(maxlen=20)  # ms: each new listing's time against the expected moment (the last listing a cycle on)
        self.category_full: bool | None = None  # whether the category answers carried everything an order needs (no details read) — None until a listing was read
        self.awaiting: dict[str, int] = {}  # card -> when its next window (beyond the known listing, not listed yet) entered the range: the sprint runs from then (1.133.1)
        self.account_pages = 1  # pages the last open-order snapshot took: its pace and staleness follow (ACCOUNT_PAGE_S)
        self.account_rows: list[dict] = []  # the last complete snapshot's rows: the owner's whole-account view when its own read is cut (LiveBot.live_full_list, 1.136.2)
        self.account_read_s = 0.0  # how long the last complete snapshot read took: the staleness allows for it (1.136.1)
        self.account_failed = ""  # why the last snapshot read failed, until one succeeds (the hold names it: 1.136.1)
        self.probe_turn = 0  # which card of a group whose next windows start together is the pass's sentinel (the rest read once it is seen listed)
        self.rate_samples: collections.deque = collections.deque(maxlen=130)  # (ms, requests so far): the request rate of the last minute (the status)
        self.parked: dict[str, tuple[int, dict]] = {}  # slug -> (retry_ms, candidate): held by hold() or the budget, placed again HOLD_RETRY_MS on, still queued
        self.full_scan_ms = 0  # discover: when the last full scan ran (FULL_SCAN_MS)
        self.keep_on_stop = False  # the owner's stop for a settings change: the orders stay on the book, the next controller takes them over
        self.resumed = self.requoted = 0  # pairs taken over at this start; windows quoted again (superseded records) since
        self.card_holds: dict[str, str] = {}  # card key -> why its windows are not quoted now (card_hold: near the day's limit, halted)
        self.card_checked_ms = 0
        self.listed_until_ms = max((int(p.get("start_ms") or 0) for p in self.ledger.rows.values()), default=0)  # the latest window seen listed
        self.listed_by_card: dict[str, int] = {}  # card key -> the latest window of it seen listed (the status, per card: a card never found stands out)
        self.listed_at: dict[str, int] = {}  # card key -> when its latest window was first seen listed: the next listing is expected LISTING_CYCLE_MS on (poll_wait)
        self.allowance_cache: dict[str, tuple[int, int]] = {}  # exchange key -> (read at, allowance in wei)
        for p in self.ledger.rows.values():
            self.listed_by_card[str(p.get("key") or "")] = max(self.listed_by_card.get(str(p.get("key") or ""), 0), int(p.get("start_ms") or 0))
        self.account_lock = asyncio.Lock()
        shared = getattr(owner.live, "api", None)  # the bot's own client: one rate-limit cooldown for all three
        self.api = safe_api(live.API_BASES[owner.config.live_chain], owner.config.predict_api_key, owner.live.wallet, self.before_send, shared)
        # A separate request gate leaves cancellation capacity available even when
        # placement requests are queued or their HTTP responses are slow.
        self.cancel_api = safe_api(live.API_BASES[owner.config.live_chain], owner.config.predict_api_key, owner.live.wallet, None, shared)
        # 1.141.1: the batch withdrawals have a client of their own: a window's cancel at its cancel time and the final reads keep
        # the cancel client's slots (four batches of hundreds held them for seconds)
        self.bulk_api = safe_api(live.API_BASES[owner.config.live_chain], owner.config.predict_api_key, owner.live.wallet, None, shared)
        self.cancel_made = self.cancel_api  # the batches go through bulk_api while the cancel client is this one (the tests put their own in)
        self.api.ORDERS_PAGE = self.cancel_api.ORDERS_PAGE = int(getattr(owner.config, "live_orders_page", 50) or 50)  # LIVE_ORDERS_PAGE: fewer pages a snapshot (1.136.1)
        self.settling: dict[str, asyncio.Task] = {}
        self.settle_gate = asyncio.Semaphore(2)
        self.reconcile_gate = asyncio.Semaphore(2)  # the withdrawn pairs' final reads (launch_reconcile) have their own two: thousands of them never starve the monitors and settlements (1.141.0)
        self.bulk_slugs: set[str] = set()  # pairs a batch withdrawal holds: the loop leaves them to it (1.141.0)
        self.bulk_task: asyncio.Task | None = None  # the closing pairs' withdrawal in batches (a settings change: many at once), beside the loop
        self.bulk_progress: dict = {}  # the batch withdrawal under way (live.remove_in_batches), for the status
        self.bulk_last: dict = {}  # ...and the last one's figures
        self.winding = ""  # why the mode winds down (switched off with orders out): every order withdrawn, the final fills read, then it stops itself
        self.winding_since = 0
        self.rest_clear_ms = 0  # winding down: since when nothing of this mode can rest on the book (the final reads have FINALS_WAIT_MS)
        self._remove_limit: Any = None  # the measured batch when the owner keeps none (the tests' owner)

    def now(self) -> int:
        return self.owner.market.now_ms()

    @property
    def error(self) -> str:
        return self._error

    @error.setter
    def error(self, text: str) -> None:
        """The latest problem (the status line); each new one kept with its time (/preopen check) and announced once in a while."""
        text = str(text or "")
        fresh = text and text != self._error
        self._error = text
        if fresh:
            self.errors.append((self.now(), text))
            self.notify(f"⚠️ 开盘前预挂：{text}", key="preopen:error:" + text[:40])

    def notify(self, text: str, key: str = "") -> None:
        """A line to the administrator's chats (LiveBot.live_notify: a keyed notice repeats at most every LIVE_NOTE_SECONDS)."""
        send = getattr(self.owner, "live_notify", None)
        if send is not None:
            with contextlib.suppress(Exception):
                send(text, key=key)

    def archive(self, now_ms: int) -> None:
        """The old finished quotes that filled nothing leave the working records (Ledger.archive_old, Audit
        PRE-LEDGER-GROWTH) — at a start, before anything is read off them, and every ARCHIVE_EVERY_MS from the cancellation
        loop. Never raises: a ledger that cannot take the move (a full disk) keeps every record where it was, the problem
        is said and the next pass tries again — the quotes and the withdrawals do not wait on this."""
        self.archived_ms = now_ms
        try:
            moved = self.ledger.archive_old(now_ms)
        except Exception as error:
            why = core.clean_error(error) or type(error).__name__
            LOG.warning("开盘前预挂旧记录归档失败：%s", why)
            self.error = f"旧记录归档失败：{why}（记录原样保留，不影响挂单和撤单，{ARCHIVE_EVERY_MS // 60_000} 分钟后再试）"
            return
        if moved:
            LOG.info("开盘前预挂：%d 条早已结束、已撤且没成交的记录移入归档（复盘页照常统计），工作记录 %d 条", moved, len(self.ledger.rows))

    def note_cooldown(self) -> None:
        """A cooldown beginning (a 429 on any client sharing it) is counted and announced."""
        why = self.cooldown()
        if why and not self.in_cooldown:
            self.limited_at.append(self.now())
            self.notify(f"⚠️ 开盘前预挂：{why}（24 小时内第 {sum(1 for t in self.limited_at if self.now() - t <= core.DAY_MS)} 次）", key="preopen:429")
        self.in_cooldown = bool(why)

    def stuck_pairs(self, now_ms: int) -> list[dict]:
        """The withdrawn pairs not cleared STUCK_MS past their cancel time: a cancel or a read that keeps failing, an order in doubt."""
        def late(p: dict) -> bool:  # a pair with a fill waits for the result (FILL_SETTLE_MS past the start); one without should clear in minutes
            filled = any(float(o.get("filled") or 0) > 0 for o in p["orders"].values())
            return now_ms > (settle_from(p) + FILL_SETTLE_MS - 15 * 60_000 if filled else int(p.get("cancel_ms") or 0) + STUCK_MS)
        return [p for p in self.ledger.rows.values() if not p.get("accounted") and p["phase"] != "active" and late(p)]

    def check_stuck(self, now_ms: int) -> None:
        """The stuck pairs announced (once in a while each; at most three a pass, the count of the rest)."""
        stuck = self.stuck_pairs(now_ms)
        for p in stuck[:3]:
            states = "/".join(f"{s} {o['state']}" for s, o in p["orders"].items())
            self.notify(f"⚠️ 预挂 {p.get('name') or p['slug']} 撤单 {(now_ms - int(p.get('cancel_ms') or 0)) // 60_000} 分钟后仍未结清：{states}；"
                        f"{p.get('reason') or ''}；最近问题：{self.error or '无'}" + (f"（另有 {len(stuck) - 3} 场同样未结清，/preopen check 查看）" if len(stuck) > 3 else ""),
                        key="preopen:stuck:" + p["slug"])

    def check(self) -> str:
        """/preopen check: what is wrong now, in one message — the cooldown and the 429s, the account snapshot, why nothing
        new is placed, the stock cards held, the candidates waiting, the pairs not cleared long after their cancel time, the
        orders in doubt, the last problems."""
        now = self.now()
        hms = lambda ms: dt.datetime.fromtimestamp(ms / 1000, core.BEIJING).strftime("%H:%M:%S")
        hold = (self.hold() or self.budget_problem()) if self.alive else ""
        cooldown = self.cooldown()
        limited = [t for t in self.limited_at if now - t <= core.DAY_MS]
        stuck = self.stuck_pairs(now)
        unknown = [(p, s) for p in self.ledger.rows.values() if not p.get("accounted") for s, o in p["orders"].items() if o["state"] == "unknown"]
        recent = [(t, e) for t, e in self.errors if now - t <= 600_000]
        problems = bool(cooldown or hold or self.card_holds or stuck or unknown or recent or not self.alive)
        lines = [f"开盘前预挂体检 {hms(now)}：{'有问题' if problems else '正常'}" + ("" if self.alive else f"；本模式已停止：{self.error}")]
        lines.append(f"限流：{cooldown or '没有冷却'}；24 小时内触发 {len(limited)} 次" + (f"，最近 {hms(limited[-1])}" if limited else ""))
        lines.append(f"账户快照：{'还没读到' if not self.account_at else str((now - self.account_at) // 1000) + ' 秒前'}；余额 {self.balance:.2f}U，"
                     f"账户挂单占用 {self.open_reserved:.2f}U，本模式预留 {self.risk():.2f}U（{f'限额 {self.settings.max_open_usd:g}U' if self.settings.max_open_usd else '不设上限'}）"
                     + (f"；另有 {self.unmanaged} 笔两个策略都没记录的挂单：" + "；".join(self.unmanaged_detail) + ("…" if self.unmanaged > len(self.unmanaged_detail) else "")
                        if self.unmanaged else ""))
        lines.append(f"新挂：{hold or '正常'}；队列 {self.queue.qsize()} 场，等候 {len(self.parked)} 场"
                     + ("：" + "；".join(f"{c.get('name') or slug} 等到 {hms(at)}" for slug, (at, c) in list(self.parked.items())[:5]) if self.parked else ""))
        if self.card_holds:
            lines.append("股票卡暂停：" + "；".join(self.card_holds.values()))
        lines.append(f"撤单后超过 {STUCK_MS // 60_000} 分钟未结清：{len(stuck)} 场"
                     + ("：" + "；".join(f"{p.get('name') or p['slug']}（{'/'.join(o['state'] for o in p['orders'].values())}，{p.get('reason') or '?'}）" for p in stuck[:5]) if stuck else ""))
        lines.append(f"状态未知的订单：{len(unknown)} 张" + ("：" + "；".join(f"{p.get('name') or p['slug']} {s}" for p, s in unknown[:5]) if unknown else ""))
        lines.append(self.bulk_line())
        lines.append("最近 10 分钟的问题：" + ("；".join(f"{hms(t)} {e}" for t, e in recent[-5:]) if recent else "无"))
        return "\n".join(lines)

    def stopped(self) -> bool:
        return self.owner.stopping.is_set() or self.stop.is_set()

    def withdrawing(self) -> str:
        """An operator's stop, which withdraws the resting pairs: the mode switched off (winding down, 1.141.0) or paused, real
        trading off, paused or killed, the day's loss cap reached. Not the process or the mode stopping (on_stop decides
        what stays on the book), and not the account not verified yet at a start (1.128.3: that withdrew every pair just
        taken over, and re-quoted them)."""
        c = self.owner.config
        if self.winding:
            return self.winding
        if self.manual_pause:
            return "本模式已手动暂停（/preopen resume 恢复）"
        if c.live_mode != "on" or not c.live_allowed:
            return "真实交易未开启"
        if reason := self.owner.live_paused() or self.owner.live_killed():
            return reason
        cap = c.live_max_daily_loss_usd
        if cap and self.daily_pnl() + self.owner.live_daily_pnl(self.owner.sim_trades(), self.now()) <= -cap:
            return "已达到今日真实亏损上限"
        return ""

    def blocked(self) -> str:
        """Why nothing new goes out: stopping, an operator's stop (withdrawing), or the account not verified yet."""
        if self.stopped():
            return "进程或预挂模式停止中"
        if why := self.withdrawing():
            return why
        return "账户未就绪" if not self.owner.live.ready else ""

    def hold(self) -> str:
        """Why no new pair goes out right now, for the status line: the pause (blocked), the account snapshot not read yet
        or stale, or the rate limiter's cooldown. Open orders on the account that nobody here placed (a manual order, an
        older version's) are not a hold since 1.126.0: they are shown, and their money is in the balance check."""
        if why := self.blocked():
            return why
        if not self.account_at:
            return f"账户快照上次读取失败（{self.account_failed}），正在重读" if self.account_failed else "账户快照还没有读到（启动后第一次扫描中）"
        if self.now() - self.account_at > self.account_stale_ms():
            return (f"账户快照过期（{(self.now() - self.account_at) // 1000} 秒前读到，每 {self.account_interval_s():.0f} 秒读一次"
                    + (f"，上次读了 {self.account_read_s:.0f} 秒" if self.account_read_s >= 1 else "") + "）")
        return self.cooldown()

    def account_interval_s(self) -> float:
        """How often the account snapshot is read: the poll, or ACCOUNT_PAGE_S a page when the open orders fill more pages
        than that allows (1.134.1: 23 pages every five seconds was most of the request volume)."""
        return max(float(self.settings.poll), self.account_pages * ACCOUNT_PAGE_S)

    def account_stale_ms(self) -> int:
        """How old a snapshot may be before nothing new is placed on it: twice its pace, the last read's own duration (the
        snapshot is dated from the read's start: sixty pages take a minute) and ten seconds, 30 s at least."""
        return max(30_000, int((2 * self.account_interval_s() + self.account_read_s) * 1000) + 10_000)

    def account_wait(self, spent_s: float, failed: bool) -> float:
        """Seconds from the end of a snapshot read to the start of the next: the pace less the read's own duration (1.136.1:
        the pace used to be waited after the read, so a minute's read made the cycle twice the pace and the snapshot stale at
        the end of each), a second at least; after a failed read, the poll or ten seconds."""
        if failed:
            return max(float(self.settings.poll), 10.0)
        return max(1.0, self.account_interval_s() - spent_s)

    def request_rate(self) -> float | None:
        """Requests a second over the last minute (both API wrappers; the cancellation loop samples the counters): what the
        rate limit is up against. None before a minute of samples."""
        if len(self.rate_samples) < 2:
            return None
        (t0, n0), (t1, n1) = self.rate_samples[0], self.rate_samples[-1]
        return (n1 - n0) / ((t1 - t0) / 1000) if t1 > t0 else None

    def cooldown(self) -> str:
        """The shared rate-limit cooldown (a 429 on any client): no request goes out, so no new pair is started."""
        left = getattr(self.api, "blocked_until", 0.0) - time.monotonic()
        return f"Predict 接口限流冷却中（{int(left) + 1} 秒后重试）" if left > 0 else ""

    def budget_problem(self, pair_usd: float | None = None, side_usd: float | None = None) -> str:
        """Why the next pair (the first level's unless given) does not fit: this mode's own cap (PREOPEN_MAX_OPEN_USD —
        1.132.1: its own, not the lesser of it and LIVE_MAX_OPEN_USD less the model trader's exposure; Predict locks no
        USDT for a resting order, the reservation is this mode's accounting), the USDT balance, the per-order cap."""
        pair_usd = self.settings.pair_usd if pair_usd is None else pair_usd
        side_usd = self.settings.side_usd if side_usd is None else side_usd
        limit = self.settings.max_open_usd  # 0: none (1.132.3) — the balance (one pair's worth) and the per-order cap still hold
        if limit and self.risk() + pair_usd > limit + 1e-9:
            return f"预挂资金上限不足：已预留 {self.risk():.2f}U，再挂一场双边要 {pair_usd:g}U，限额 {limit:g}U（PREOPEN_MAX_OPEN_USD）"
        if self.balance < pair_usd:  # 1.130.5: the account's open orders are each their own allowance (the money is taken at a match): one pair's worth is enough
            return f"可用USDT {self.balance:.2f}U 不够一场双边（{pair_usd:g}U）"
        if side_usd > self.owner.config.live_max_order_usd:
            return f"LIVE_MAX_ORDER_USD 小于每边 {side_usd:g}U"
        return ""

    def card_hold(self, rolling: Any) -> str:
        """Why a stock card's windows are not quoted now (""): its exchange price within PREOPEN_LIMIT_GAP_PCT of the day's
        limit up or down (the previous close ± the exchange's limit), or trading halted (the quote's own time standing
        still HALT_MS in session) — the window's direction is then no coin toss, and the resting bids would be hit by those
        who know. Judged on the bot's own exchange quote (the card's lead quote); without a fresh one nothing is judged."""
        limit, gap = float(getattr(rolling, "limit_pct", 0) or 0), float(self.settings.limit_gap_pct)
        if not gap or not getattr(rolling, "exchange", ""):
            return ""  # the guard off, or no exchange (the round-the-clock cards): nothing to judge
        card = (getattr(self.owner, "updowns", None) or {}).get(rolling.key)
        q = getattr(card, "lead", None)
        now = self.now()
        if q is None or not getattr(q, "last", None) or now - int(getattr(q, "fetched_ms", 0) or 0) > 60_000:
            return ""  # no exchange quote, or the feed itself is quiet: nothing to judge by
        last, prev = float(q.last), float(getattr(q, "prev_close", None) or 0)
        if limit and prev > 0:  # an exchange with a daily price limit (1.134.0: HKEX has none — the halt rule alone)
            up, down = prev * (1 + limit), prev * (1 - limit)
            if last >= up * (1 - gap / 100):
                return f"{rolling.coin} 现价 {last:,.2f} 距涨停 {up:,.2f} 不足 {gap:g}%"
            if last <= down * (1 + gap / 100):
                return f"{rolling.coin} 现价 {last:,.2f} 距跌停 {down:,.2f} 不足 {gap:g}%"
        quoted = int(getattr(q, "quoted_ms", 0) or 0)
        if quoted and rolling.exchange and self.in_continuous_trading(rolling, now):
            quiet = core.quiet_ms(quoted, now, LUNCH.get(rolling.exchange))
            if quiet > HALT_MS:
                return f"{rolling.coin} 交易时段里成交时间 {quiet // 60_000} 分钟没动（临停或 VI）"
        return ""

    def in_continuous_trading(self, rolling: Any, now_ms: int) -> bool:
        """Whether the stock trades continuously now — inside a session segment, past its first five minutes (the first
        print after an open or the lunch break lags), before the closing auction (nothing prints there: KRX from 15:20,
        SSE from 14:57) — the only time a still quote means a halt (1.131.2: the KRX closing auction read as one every day)."""
        tz = dt.timezone(dt.timedelta(hours=core.STOCK_MARKETS[rolling.exchange].utc_offset))
        local = dt.datetime.fromtimestamp(now_ms / 1000, tz)
        holidays = self.owner.config.holidays.get(rolling.exchange, frozenset())
        for seg_open, seg_close in core.session_segments(rolling.exchange, local.date(), holidays):
            if seg_open <= now_ms < seg_close:
                return now_ms - seg_open >= 5 * 60_000 and local.time() < core.closing_auction_start(rolling.exchange, local.date())
        return False

    async def check_cards(self, now_ms: int) -> None:
        """The stock cards' holds (card_hold), every CARD_CHECK_MS: a card held has its resting pairs withdrawn (quoted again
        once the hold lifts: supersedable), and the discovery and the placement pass its windows by meanwhile."""
        if now_ms - self.card_checked_ms < CARD_CHECK_MS:
            return
        self.card_checked_ms = now_ms
        for rolling in core.preopen_cards():
            # Audit PRE-ZHIPU-HALT: a card is passed over only when it has no exchange (the round-the-clock cards: nothing to
            # judge). 1.130.0 passed over the cards without a daily price limit, which then meant the same thing; 1.134.0 added
            # 智谱 (HKEX 2513: no daily limit, the halt rule alone — card_hold judges it so, PREOPEN.md promises it) but left
            # this filter as it was, so 智谱 was never held: its resting 49¢ bids stayed up, and new ones went out, through a
            # halt — the informed-flow case the guard exists for. card_hold itself skips the limit part when limit_pct is 0.
            if rolling.key not in self.settings.markets or not getattr(rolling, "exchange", ""):
                continue
            why = self.card_hold(rolling)
            if why:
                self.card_holds[rolling.key] = why
                active = [p for p in self.ledger.rows.values() if p["phase"] == "active" and p.get("key") == rolling.key
                          and p["start_ms"] - now_ms <= CARD_HOLD_NEAR_MS]  # the next hour's windows: a day's quotes are not thrown away over a halt
                if active:
                    await self.cancel_many(active, why, free=True)
            else:
                self.card_holds.pop(rolling.key, None)

    def before_send(self, body: dict) -> str:
        if why := self.blocked() or self.cooldown():
            return why
        digest = str(body.get("data", {}).get("order", {}).get("hash") or "").lower()
        pair = self.ledger.holder(digest)  # (by the hash index: this walked every record, four times a pair — Audit PRE-LEDGER-GROWTH)
        if pair is None or pair["phase"] != "active" or self.now() >= pair["cancel_ms"]:
            return "市场已进入撤单阶段，不再发送"
        if (why := self.card_holds.get(str(pair.get("key") or ""))) and pair["start_ms"] - self.now() <= CARD_HOLD_NEAR_MS:
            return why
        if pair["expires"] * 1000 - self.now() < SEND_MIN_LEFT_MS:
            return "距订单到期不足一分钟，不再发送"
        if self.now() - self.account_at > self.account_stale_ms():
            return "账户快照过期，不再发送"
        if self.settings.max_open_usd and self.risk() > self.settings.max_open_usd + 1e-9:  # (this mode's own cap, 1.132.1; the pair is reserved by now)
            return "最新资金限额不允许发送"
        return ""

    def known(self) -> tuple[set[str], set[str]]:
        ids, hashes = set(), set()
        for pair in self.ledger.rows.values():
            for order in pair["orders"].values():
                if order.get("id"):
                    ids.add(str(order["id"]))
                hashes.update(str(order[k]).lower() for k in ("hash", "api_hash") if order.get(k))
        return ids, hashes

    def unmanaged_rows(self, rows: list[dict]) -> list[dict]:
        import live
        ids, hashes = self.known()
        return [r for r in rows if str(r.get("id")) not in ids and live.order_hash_of(r) not in hashes]

    def risk(self) -> float:
        """The money this mode may have tied up: every order that may exist (sent, or sent for all we know) at its full
        size, until its pair is accounted — by the final reads after a cancel (nothing filled) or by the official
        result. A refusal or a never-sent order ties up nothing."""
        total = 0.0
        for p in self.ledger.rows.values():
            if p.get("accounted"):
                continue
            price, shares = pair_quote(p)  # the pair's own size and price: a pair from older settings keeps its own figure
            side_usd = price * shares if price and shares else self.settings.side_usd
            total += sum(side_usd for o in p["orders"].values() if o["state"] not in NEVER_PLACED)
        return total

    def filled_by_outcome(self, days: int = 3) -> dict[tuple[str, int], float]:
        """The shares this mode's orders filled, by (market id, index set), for the windows of the last ``days`` days: the
        model trader's positions reconciliation counts them as recorded, not as fills of its own orders to adopt."""
        return fills_by_outcome(list(self.ledger.rows.values()), self.now(), days)

    def daily_pnl(self) -> float:
        day = dt.datetime.fromtimestamp(self.now() / 1000, core.BEIJING).date().isoformat()
        return sum(float(p.get("pnl") or 0) for p in self.ledger.rows.values() if p.get("accounted_day") == day)

    async def account(self) -> None:
        """The account snapshot: the open orders (every page) and the USDT balance, read outside the placement lock (1.133.0:
        a second or two of page reads every poll used to hold a placement at a listing), applied under it. The snapshot is
        dated when its read began: an order placed meanwhile is not judged by it."""
        import live
        fetched = self.now()
        try:
            rows = await self.api.orders("OPEN")
            page = int(getattr(self.api, "ORDERS_PAGE", 50) or 50)
            if not self.api.orders_complete:
                raise core.RemoteError(f"账户挂单分页未读取完整（读了 {len(rows)} 条、{-(-len(rows) // page)} 页仍没到底，上限 {getattr(self.api, 'ORDERS_PAGES_MAX', '?')} 页），暂停新增订单")
            self.account_pages = max(1, -(-len(rows) // page))
            other = self.unmanaged_rows(rows)
            known_core = {str(t.get("live", {}).get("order_id")) for t in self.owner.sim_trades().values() if isinstance(t.get("live"), dict)}
            strangers = [r for r in other if str(r.get("id")) not in known_core]
            unmanaged = len(strangers)
            detail = [order_brief(r) for r in strangers[:5]]  # /preopen check names them (1.132.1)
            reserved = 0.0
            for row in rows:
                order = row.get("order") or {}
                if int(order.get("side") or 0) == 0:
                    amount, filled = D(str(row.get("amount") or "0")), D(str(row.get("amountFilled") or "0"))
                    if amount <= 0 or not 0 <= filled <= amount:
                        raise core.RemoteError("账户订单缺少有效数量，暂停新增订单")
                    reserved += float(D(str(order["makerAmount"])) / live.WEI * (1 - filled / amount))
            balance = float(await self.owner.live.chain.usdt_balance()) / live.WEI
        except Exception as error:
            self.account_at = 0
            self.account_failed = core.clean_error(error) or type(error).__name__
            raise
        self.account_read_s, self.account_failed = (self.now() - fetched) / 1000, ""
        async with self.account_lock:
            self.unmanaged, self.open_reserved, self.balance, self.unmanaged_detail = unmanaged, reserved, balance, detail
            self.listed_ids = {str(r.get("id")) for r in rows}
            self.account_rows = rows
            by_hash = {live.order_hash_of(r): r for r in rows if live.order_hash_of(r)}
            for pair in self.ledger.rows.values():
                changed = False
                for order in pair["orders"].values():
                    row = by_hash.get(order.get("api_hash") or order["hash"])
                    if row is not None:
                        before = order["filled"]
                        self.apply_fill(order, row, pair)
                        changed |= order["filled"] != before
                if changed:
                    self.ledger.save(pair)
            self.account_at = fetched

    def resumable(self, pair: dict) -> bool:
        """An active pair found at the start (a restart, a settings change) that keeps resting, its queue position with it:
        every order open with an id, the card still quoted, the size, price and signed expiry as the settings say (the
        cancel time is derived again), the start still ahead. Anything else is withdrawn."""
        import live
        if pair.get("key") not in self.settings.markets or self.now() >= pair["start_ms"]:
            return False
        rolling = next((r for r in core.preopen_cards() if r.key == pair["key"]), None)
        if rolling is None or session_opening(rolling, int(pair["start_ms"]), self.owner.config.holidays.get(rolling.exchange, frozenset()),
                                              int(self.settings.open_quiet_minutes) * 60_000):
            return False  # 1.135.1: a stock card's first window after a session open, quoted before the rule (or with it off): withdrawn
        if not self.settings.weekend and on_weekend(int(pair["start_ms"])):
            return False  # 1.136.0: the weekend switched off since it was quoted: withdrawn
        if int(pair.get("expires") or 0) != pair["start_ms"] // 1000 - self.settings.expiry_seconds:
            return False
        price, shares = pair_quote(pair)
        index = int(pair.get("level") or 0)
        level = self.settings.levels[index] if index < len(self.settings.levels) else None
        if level is None or float(level[0]) != price or float(level[1]) != shares:
            return False  # not what the settings say at this level (a price or size changed, a level dropped): withdrawn, quoted again as they say
        amounts = live.limit_amounts(True, live.to_wei(level[0]), live.to_wei(level[1]))
        for order in pair["orders"].values():
            signed = (order.get("body") or {}).get("data", {}).get("order", {})
            if (order["state"] != "open" or not order.get("id")
                    or str(signed.get("makerAmount")) != str(amounts["maker"]) or str(signed.get("takerAmount")) != str(amounts["taker"])):
                return False
        return True

    def adopt(self) -> None:
        """The records found at the start: an active pair as the settings say is taken over (resumable); any other pair not
        at its end is withdrawn and reconciled (interrupted attempts included — a recovered record never gets a new salt)."""
        for pair in list(self.ledger.rows.values()):
            if not pair.get("price") or not pair.get("shares"):  # a record from before 1.128.0: its size and price, read off its orders, kept on it
                price, shares = pair_quote(pair)
                if price and shares:
                    pair.update(price=str(price), shares=str(int(shares) if shares == int(shares) else shares))
                    self.ledger.save(pair)
            if pair["phase"] == "active" and self.resumable(pair):
                pair["cancel_ms"] = pair["start_ms"] - self.settings.cancel_seconds * 1000
                self.ledger.save(pair)
                self.resumed += 1
                if pair.get("key"):  # the listing horizon as the records know it: the status's 发布到, and the discovery's "beyond the listing", from the start
                    self.listed_by_card[pair["key"]] = max(self.listed_by_card.get(pair["key"], 0), int(pair["start_ms"]))
            elif pair["phase"] != "closed" and not pair.get("accounted"):
                pair.update(phase="closing", retry_ms=0, **({"reason": "设置不同，撤掉后重新报价", "free": True} if pair["phase"] == "active" else {}))
                self.ledger.save(pair)

    def supersedable(self, pair: dict, strikes: int | None = None) -> bool:
        """A pair withdrawn before its window — a stop, a pause, a settings change, an external cancel — with the start
        still ahead: its record steps aside so the window is quoted again under a new salt. Not one Predict refused (a
        post-only order that would cross is refused again), not one with a fill (a position: that size is taken), and
        not with MAX_ATTEMPTS of the window's quotes ended by its own failure (``strikes``; window_strikes when not given:
        1.131.6, a withdrawal that was not its doing is free). Accounted means every order verified and nothing filled."""
        if pair["phase"] == "active" or not pair.get("accounted"):
            return False
        if pair["start_ms"] - self.now() < self.settings.entry_seconds * 1000:
            return False
        if any(o["state"] == "rejected" and not o.get("limited") or o.get("refused") for o in pair["orders"].values()):
            return False  # Predict's refusal, outright or deferred (monitor_pair): the same order would be refused again
        if strikes is None:
            strikes = window_strikes(self.ledger.rows.values()).get(record_key(pair), 0)
        return strikes < MAX_ATTEMPTS

    async def on_stop(self) -> None:
        """The way out: the orders are withdrawn when the mode is switched off or crashed; they stay on the book when the
        process is stopping or the owner restarts the mode for a settings change (keep_on_stop) — the signed expiry is the
        backstop, and the next controller takes them over (adopt)."""
        resting = sum(1 for p in self.ledger.rows.values() if p["phase"] == "active")
        if self.owner.stopping.is_set() or self.keep_on_stop:
            LOG.info("开盘前预挂：%d 场挂单留在簿上（签名到期兜底，重启后接管）", resting)
            return
        if not resting and not any(removable_ids(p) for p in self.ledger.rows.values() if p["phase"] != "closed"):
            return  # nothing left to withdraw (a wind-down that finished)
        until = time.monotonic() + STOP_CANCEL_S  # the whole way out, the fallback below included: STOP_CANCEL_S at most
        try:  # 1.141.0: in batches, a rate limit waited out (was cut after 12 s: a book of thousands mostly stayed)
            await self.cancel_all("预挂模式停止，撤销预挂单", free=True, deadline_s=STOP_CANCEL_S)
        except Exception as error:
            # Audit PRE-CRASH-WITHDRAW: this was contextlib.suppress — and the withdrawal fails exactly when the mode crashed on
            # a ledger it cannot write (a full disk, an I/O error, a read-only volume): cancel_many saves every pair before
            # it sends a removal, and each answer is saved inside the batch withdrawal, so the first write raised again, the
            # suppress swallowed it, and not one removal went out — every resting order stayed up, unmonitored and its
            # fills unbooked, until its signed expiry, while the crash notice said they were withdrawn. A removal needs no
            # record (save-first is for sending an order: place_pair), so what may still rest is withdrawn here with
            # nothing written; the next start reconciles the records against Predict, as after any stop.
            why = core.clean_error(error) or type(error).__name__
            LOG.error("开盘前预挂停止时撤单出错：%s；改为不写记录直接撤", why)
            self.error = f"停止时撤单出错：{why}；改为不写记录直接撤（下次启动时按 Predict 核对）"
            await self.remove_unrecorded(until)

    async def remove_unrecorded(self, until: float) -> None:
        """on_stop's fallback (Audit PRE-CRASH-WITHDRAW): every order of this mode that may still rest (an id, not ended,
        the signed expiry ahead) withdrawn in batches, the soonest window first, until ``until`` (time.monotonic), with
        nothing written to the ledger — a removal is marked in memory only, so the crash notice and the status count what
        is left (resting_orders). Never raises: a failure is logged, and what is left is counted the same way."""
        import live
        try:
            now = self.now()
            pairs = sorted((p for p in self.ledger.rows.values() if resting_orders(p, now)),
                           key=lambda p: (int(p.get("cancel_ms") or p.get("start_ms") or 0), str(p.get("slug") or "")))
            by_id = {str(o["id"]): o for p in pairs for o in p["orders"].values() if o.get("id") and o.get("state") not in TERMINAL}  # (removable_ids)
            if not by_id:
                return

            def answered(removed: list[str], noop: list[str]) -> None:
                for oid in removed:
                    if oid in by_id:
                        self.off_book(by_id[oid])
            # patient whatever the count: the last try — no loop, no reconciliation follows — so a rate limit is waited out
            # and a failed batch tried again, within ``until`` (impatient, a few ids would not be sent into a cooldown at all)
            result = await live.remove_in_batches(
                self.bulk_client(), list(by_id), self.remove_limit(), workers=BULK_WORKERS, patient=True,
                deadline=until, stop=self.owner.stopping.is_set, on_answer=answered, now_ms=self.now)
            LOG.warning("开盘前预挂不写记录直接撤：%d 张里撤掉 %d 张，%d 张已不在簿上，%d 张没撤成%s", len(by_id), len(result["removed"]),
                        len(result["noop"]) + len(result["unanswered"]), len(result["failed"]) + len(result["left"]),
                        f"（{result['errors'][-1]}）" if result["errors"] else "")
        except Exception as error:
            LOG.error("开盘前预挂不写记录直接撤单失败：%s", core.clean_error(error) or type(error).__name__)

    async def discover(self, probe_only: bool = False) -> None:
        """One discovery pass: first each card's next listing (the windows beyond the latest known listed, concurrently across
        cards, each candidate queued the moment it is found — the queue position), then, unless ``probe_only`` or a listing
        is expected any second (sprinting), the known-listed windows with a level still to quote, DISCOVER_READS a pass.
        Not listed, a card's later windows are asked only in a full scan (every FULL_SCAN_MS, up to MISS_STOP unlisted in a
        row); between full scans each card's first unlisted window alone is asked."""
        full = self.now() - self.full_scan_ms >= FULL_SCAN_MS
        if full:
            self.full_scan_ms = self.now()
        if why := self.cooldown():
            self.scan_note = f"{hms(self.now())} 没扫：{why}"
            return
        st = self.scan_state
        st.update(reads=0, asked=0, found=0, deferred=0, pending=0, stopped="", probed=0)
        known_before = dict(self.listed_by_card)  # the latest windows known listed as this pass starts: a window beyond them is a listing seen now
        attempts: dict[str, int] = {}  # record (a window's level) -> the attempt after its last record, the superseded ones included: an attempt that left no record is not reused (its salt would be the first attempt's)
        for p in self.ledger.rows.values():
            attempts[record_key(p)] = max(attempts.get(record_key(p), 0), int(p.get("attempt") or 0) + 1)
        strikes = window_strikes(self.ledger.rows.values())  # 1.131.6: the cap counts failures, not the salt's attempts
        sprint = self.sprinting()  # judged as the pass begins: a pass that started for a listing stays with the listings (the next tick re-reads)
        try:
            await self.probe_listings(known_before, full, attempts, strikes, st)
            if not probe_only and not st["stopped"] and not sprint:
                self.rescan_ms = self.now()
                await self.reread_known(known_before, attempts, strikes, st)
        finally:
            self.scan_note = (f"{hms(self.now())} 探 {st['probed']} 卡、读 {st['asked']} 场，找到 {st['found']} 档"
                              + (f"，还有 {st['deferred']} 场下次再读" if st["deferred"] else "") + (f"，{st['pending']} 档在等撤单核对" if st["pending"] else "")
                              + (f"；停在 {st['stopped']}" if st["stopped"] else ""))

    def sprinting(self) -> bool:
        """Whether a card's next listing is expected any second (poll_wait: the fast poll): the pass probes the listings alone."""
        return self.poll_wait() == FAST_POLL_S

    def wanted_levels(self, slug: str, start: int, strikes: dict, st: dict) -> list[tuple[str, int, D, D]]:
        """The levels of a window to quote now: (record key, level, price, shares) — not queued, not quoted, not past the
        failures cap, not waiting on a withdrawal's reconciliation (counted as pending), the deeper ones only near the start."""
        wanted = []
        for level, (price, shares) in enumerate(self.settings.levels):
            if level and start - self.now() > self.settings.deep_ahead_minutes * 60_000:
                break  # the deeper levels wait for the window to come near (PREOPEN_DEEP_AHEAD_MINUTES); the first keeps its queue position
            key = slug if level == 0 else f"{slug}#{level}"  # sorts after the first level's key, before the next's
            if key in self.queued or strikes.get(key, 0) >= MAX_ATTEMPTS:
                continue
            if key in self.ledger.rows and not self.supersedable(self.ledger.rows[key], strikes.get(key, 0)):
                if self.ledger.rows[key]["phase"] != "active" and not self.ledger.rows[key].get("accounted"):
                    st["pending"] += 1  # withdrawn, its final reads not in yet: quoted again once they are
                continue
            wanted.append((key, level, price, shares))
        return wanted

    async def probe_listings(self, known_before: dict, full: bool, attempts: dict, strikes: dict, st: dict) -> None:
        """Each card's windows beyond its latest known listing, in start order, the cards side by side: the next listing is
        found within a probe of its appearance and queued at once (1.133.0: it used to wait behind the older windows' reads,
        and for the end of the pass)."""
        per_card: dict[str, list[tuple[Any, int]]] = {}
        for rolling, start in window_starts(self.now(), self.owner.config, self.settings):
            if start > known_before.get(rolling.key, 0) and not (self.card_holds.get(rolling.key) and start - self.now() <= CARD_HOLD_NEAR_MS):
                per_card.setdefault(rolling.key, []).append((rolling, start))
        st["probed"] = len(per_card)
        for key in list(self.awaiting):
            if key not in per_card:
                self.awaiting.pop(key)  # nothing beyond the known listing in the range: nothing to wait for
        # the cards whose next windows start together are listed together (11-06: BNB, BTC and ETH at 19:31:44 alike): one of them, the
        # pass's sentinel, is probed; the rest are read the moment it is seen listed — a fifth of the probes, no later a find (1.134.1)
        groups: dict[tuple[int, int], list[str]] = {}
        for key, items in per_card.items():
            groups.setdefault((items[0][0].cycle_ms, items[0][1]), []).append(key)  # 1.142.0: a quarter hour's listing says nothing of an hour's
        self.probe_turn += 1
        leaders: dict[str, list[str]] = {}  # sentinel -> the cards that follow it (a full scan, once a minute, reads every card itself)
        for keys in groups.values():
            keys.sort()
            leader = keys[self.probe_turn % len(keys)]
            if full:
                leaders.update({k: [] for k in keys})
            else:
                leaders[leader] = [k for k in keys if k != leader]

        async def one_card(items: list[tuple[Any, int]]) -> None:
            misses, first = 0, True
            for rolling, start in items:
                if misses >= (MISS_STOP if full else 1) or st["stopped"]:
                    return
                slug = window_slug(rolling, start)
                if self.now() - self.skipped_at.get(slug, 0) < SKIP_RETRY_MS:
                    continue  # judged no candidate a moment ago: not read again every poll
                wanted = self.wanted_levels(slug, start, strikes, st)
                if not wanted:
                    continue
                st["asked"] += 1
                try:
                    market = await category_market(self.api, slug)
                except Exception as error:
                    st["stopped"] = f"{slug.rsplit('-', 1)[-1]}：{core.clean_error(error) or type(error).__name__}"
                    self.error = f"扫描 {st['stopped']}"
                    return  # this card's pass ends here (a 429, the cooldown, a timeout): what was found is queued, the rest read next time
                if market is None:
                    if first:  # the card's next window is in the range and not listed yet: Predict lists it about a minute on — the sprint (poll_wait)
                        self.awaiting[rolling.key] = start - self.settings.ahead_minutes * 60_000
                    first = False
                    misses += 1
                    continue
                first = False
                misses = 0
                self.awaiting.pop(rolling.key, None)
                self.note_listing(rolling, start, known_before)
                self.enqueue(market, slug, wanted, attempts, st)

        async def lead(leader: str, followers: list[str]) -> None:
            await one_card(per_card[leader])
            if followers and self.listed_by_card.get(leader, 0) >= per_card[leader][0][1]:  # the sentinel's window listed: the others now
                await asyncio.gather(*(one_card(per_card[k]) for k in followers))
        await asyncio.gather(*(lead(leader, followers) for leader, followers in leaders.items()))

    async def reread_known(self, known_before: dict, attempts: dict, strikes: dict, st: dict) -> None:
        """The known-listed windows with a level still to quote (a withdrawn pair's, a level the cap held back, a deeper
        level now near its start), the soonest first, DISCOVER_READS a pass: the rest the next pass."""
        for rolling, start in window_starts(self.now(), self.owner.config, self.settings):
            if start > known_before.get(rolling.key, 0) or (self.card_holds.get(rolling.key) and start - self.now() <= CARD_HOLD_NEAR_MS):
                continue
            slug = window_slug(rolling, start)
            if self.now() - self.skipped_at.get(slug, 0) < SKIP_RETRY_MS:
                continue
            wanted = self.wanted_levels(slug, start, strikes, st)
            if not wanted:
                continue
            if st["reads"] >= DISCOVER_READS:
                st["deferred"] += 1  # the next pass's (1.132.2: every such window was read again every poll)
                continue
            st["reads"] += 1
            st["asked"] += 1
            try:
                market = await category_market(self.api, slug)
            except Exception as error:
                st["stopped"] = f"{slug.rsplit('-', 1)[-1]}：{core.clean_error(error) or type(error).__name__}"
                self.error = f"扫描 {st['stopped']}"
                return
            if market is None:  # a window known listed answers 404: not asked again for a minute
                self.skipped[slug], self.skipped_at[slug] = "已发布的场次读不到（404）", self.now()
                continue
            self.enqueue(market, slug, wanted, attempts, st)

    def note_listing(self, rolling: Any, start: int, known_before: dict) -> None:
        """A window read listed: the card's listing horizon, and — a new listing beyond the known — the rhythm's anchor
        (poll_wait polls fast a cycle on) and how far it came from the expected moment (the status: listing_deltas)."""
        self.listed_until_ms = max(self.listed_until_ms, start)
        if known_before.get(rolling.key) and start > known_before[rolling.key]:
            if self.listed_at.get(rolling.key):
                self.listing_deltas.append(self.now() - (self.listed_at[rolling.key] + rolling.cycle_ms))
            self.listed_at[rolling.key] = self.now()
        self.listed_by_card[rolling.key] = max(self.listed_by_card.get(rolling.key, 0), start)

    def enqueue(self, market: dict, slug: str, wanted: list, attempts: dict, st: dict) -> None:
        """A listed window judged (candidate) and its wanted levels queued at once, the details just read carried along
        (placed on, not read again: MARKET_FRESH_MS); a window that is no candidate is noted (skipped)."""
        self.category_full = market.get("_from") == "category"
        c = candidate(market, self.now(), self.owner.config, self.settings)
        if c is None:
            self.skipped[slug] = candidate_problem(market, self.now(), self.owner.config, self.settings)
            self.skipped_at[slug] = self.now()
            return
        for key, level, price, shares in wanted:
            if key in self.ledger.rows:  # the withdrawn pair's record steps aside: the level is quoted again (place_pair: a new salt)
                self.ledger.rename(key, f"{key}@{int(self.ledger.rows[key].get('attempt') or 0)}")  # the record and the attempt: one key each
                self.requoted += 1
            self.queued.add(key)
            st["found"] += 1
            self.queue.put_nowait((c["start_ms"], key, {**c, "slug": key, "window": slug, "level": level, "price": str(price), "shares": str(shares),
                                                       "attempt": attempts.get(key, 0), "market": market, "read_ms": self.now()}))

    async def refresh_active(self) -> None:
        """An active window's market, read again every REFRESH_MS (REFRESH_FAR_MS while its start is more than an hour
        away): when matching comes on (note_open), and a market no longer registered or open for orders has its pair
        withdrawn."""
        every = lambda p: REFRESH_MS if p["start_ms"] - self.now() <= 3_600_000 else REFRESH_FAR_MS
        due = sorted((p for p in self.ledger.rows.values() if p["phase"] == "active" and self.now() - int(p.get("meta_ms") or 0) >= every(p)),
                     key=lambda p: int(p.get("meta_ms") or 0))  # the longest unread first; a burst of due windows is spread over iterations
        batch: dict[str, list[dict]] = {}  # market id -> its due records: one read a window, whatever its levels (1.136.1: four levels were four reads)
        for pair in due:
            key = str(pair["market_id"])
            if key in batch or len(batch) < REFRESH_BATCH:
                batch.setdefault(key, []).append(pair)
        gate = asyncio.Semaphore(3)

        async def one(pairs: list[dict]) -> None:
            async with gate:
                for pair in pairs:
                    pair["meta_ms"] = self.now()
                market = {**(await self.api.market(pairs[0]["market_id"])), "categorySlug": pairs[0].get("window") or window_of(pairs[0])}
                self.note_open(market)
                status, trading = str(market.get("status") or "").upper(), str(market.get("tradingStatus") or "").upper()
                if status != "REGISTERED" or trading not in {"OPEN", "MATCHING_NOT_ENABLED"}:
                    await self.cancel_many(pairs, f"市场状态 {status or '?'} / {trading or '?'}")
                else:
                    for pair in pairs:
                        self.ledger.save(pair)
        for error in await asyncio.gather(*(one(ps) for ps in batch.values()), return_exceptions=True):
            if isinstance(error, BaseException):
                raise error

    async def place_pair(self, c: dict) -> str:
        """The pair for a queued candidate: "" when done with it (placed, refused, or not this window's any more), "held"
        when a hold (a stale snapshot, the cooldown, the pause) or the budget keeps it back — nothing read then: the worker
        parks it and tries again HOLD_RETRY_MS on."""
        import live
        if c["slug"] in self.ledger.rows:
            return ""
        if self.hold() or (self.card_holds.get(str(c.get("key") or "")) and int(c.get("start_ms") or 0) - self.now() <= CARD_HOLD_NEAR_MS):
            return "held"
        price, shares = D(str(c.get("price") or self.settings.price)), D(str(c.get("shares") or self.settings.shares))  # the level's (PREOPEN_LEVELS)
        side_usd, pair_usd = float(price * shares), 2 * float(price * shares)
        if why := self.budget_problem(pair_usd, side_usd):
            self.error = why
            return "held"
        # Fresh details are authoritative, not a stale listing page or model card — the discovery's own read, when seconds old (MARKET_FRESH_MS)
        fresh = c.get("market") if c.get("market") and self.now() - int(c.get("read_ms") or 0) <= MARKET_FRESH_MS else None
        market = {**(fresh or await self.api.market(c["market_id"])), "categorySlug": c.get("window", c["slug"])}  # the window the category named: the details' own slug may differ
        current = candidate(market, self.now(), self.owner.config, self.settings)
        if current is None or current["slug"] != c.get("window", c["slug"]):
            return ""
        info = live.parse_market(market, self.owner.config.predict_fee_bps)
        outcomes = [live.outcome_for_side("updown", side, info.outcomes) for side in ("up", "down")]
        if any(outcome is None or assumed for outcome, assumed in outcomes):
            raise core.RemoteError("开盘前市场的涨跌代币无法确认")
        if outcomes[0][0]["token"] == outcomes[1][0]["token"]:
            raise core.RemoteError("涨跌结果指向相同代币")
        exchange = live.exchange_key(info.neg_risk, info.yield_bearing)
        cached = self.allowance_cache.get(exchange)
        if cached is None or self.now() - cached[0] > ALLOWANCE_MS:
            cached = (self.now(), int(await self.owner.live.chain.allowance(exchange)))
            self.allowance_cache[exchange] = cached
        if not cached[1] >= live.to_wei(pair_usd):
            raise core.RemoteError("开盘前双边挂单授权额度不足")
        await self.api.ensure_auth()
        async with self.account_lock:
            # Only one coroutine reserves a pair at a time; reservations are saved
            # before the lock is released, including orders not visible on REST yet.
            if c["slug"] in self.ledger.rows:
                return ""
            if self.hold():
                return "held"
            if why := self.budget_problem(pair_usd, side_usd):
                self.error = why
                return "held"
            amounts = live.limit_amounts(True, live.to_wei(price), live.to_wei(shares))
            orders = {}
            for side, (outcome, _) in zip(("up", "down"), outcomes):
                identity = (f"preopen-v2|{self.owner.config.live_chain}|{self.owner.live.wallet.maker.lower()}|{current['market_id']}|"
                            f"{outcome['token']}|{current['expires']}|{price}|{shares}"
                            + (f"|L{c['level']}" if c.get("level") else "")      # a deeper level: its own salt, whatever price the first level moves to
                            + (f"|{c['attempt']}" if c.get("attempt") else ""))  # a window quoted again: a new salt, the first attempt's unchanged
                salt = int.from_bytes(hashlib.sha256(identity.encode()).digest()[:8], "big") % live.MAX_SALT
                signed = live.build_order(self.owner.live.wallet, outcome["token"], amounts["maker"], amounts["taker"], info.fee_bps, current["expires"], salt=salt)
                digest, signature = self.owner.live.wallet.sign_order(signed, info.neg_risk, info.yield_bearing)
                orders[side] = {"hash": digest.lower(), "id": "", "state": "prepared", "filled": 0.0, "index_set": int(outcome.get("index_set") or 0),
                                "body": {"data": {"order": {**signed, "signature": signature, "hash": digest},
                                                   "strategy": "LIMIT", "pricePerShare": str(amounts["price_per_share"]),
                                                   "isPostOnly": True, "selfTradePrevention": "CANCEL_TAKER"}}}
            pair = {**current, "slug": c["slug"], "window": current["slug"], "level": int(c.get("level") or 0),  # the record's key: the window's slug, #<level> for a deeper level
                    "attempt": int(c.get("attempt") or 0), "price": str(price), "shares": str(shares),
                    "phase": "active", "orders": orders, "created_ms": self.now(), "tries": 0, "retry_ms": 0,
                    "open_seen_ms": self.now() if str(market.get("tradingStatus") or "").upper() == "OPEN" else 0,
                    "seen_ms": int(c.get("read_ms") or 0)}  # when the discovery read it listed (the 发现→挂上 of the status)
            self.ledger.save(pair)  # both hashes durable before either POST
        sides: list[str] = []
        for side in ("up", "down"):
            if why := self.before_send(pair["orders"][side]["body"]):
                pair["orders"][side].update(state="not_sent", error=why)
                pair["phase"] = "closing"
                sides = []  # a pair refused in part sends nothing: the side that passed stays prepared (never sent)
                break
            sides.append(side)
        for side in sides:
            pair["orders"][side]["state"] = "placing"
        self.ledger.save(pair)

        async def send(side: str) -> None:
            order = pair["orders"][side]
            try:
                result = await self.api.create_order(order["body"])
                order.update(id=result["order_id"], state="open", api_hash=str(result.get("hash") or "").lower())
            except NotSent as error:
                order.update(state="not_sent", error=core.clean_error(error))
                pair["phase"] = "closing"
            except Exception as error:
                text = core.clean_error(error) or type(error).__name__
                status = live.http_status(error)
                definite = isinstance(error, live.OrderRejected) or 400 <= status < 500
                if status == 429 or "限流冷却中" in text:
                    # the rate limiter refused the request before it was processed (429), or the shared cooldown kept it in: not placed
                    order.update(state="rejected" if status == 429 else "not_sent", error=text, limited=True)
                elif definite and not any(w in text.lower() for w in DUPLICATE_WORDS):
                    # Predict refused it (a post-only order that would cross, an invalid field): nothing was placed, nothing
                    # to reserve or reconcile (the first revision kept such an order "unknown", and its money, for good)
                    order.update(state="rejected", error=text)
                else:
                    # A timeout, malformed answer or duplicate response does not prove
                    # no order exists. Keep its hash and full reservation; do not retry.
                    order.update(state="unknown", error=text)
                pair["phase"] = "closing"
            self.ledger.save(pair)
        if sides:
            await asyncio.gather(*(send(side) for side in sides))  # 1.131.0: side by side — the second side no longer waits a round trip
            if any(o["state"] == "open" for o in pair["orders"].values()):
                pair["placed_ms"] = self.now()
                self.ledger.save(pair)
        if all(o["state"] in {"prepared", "not_sent"} or o.get("limited") for o in pair["orders"].values()):
            # nothing of the pair was ever placed (a gate or the rate limiter refused the first request): no record, so a
            # later poll places the window afresh — hold() keeps it back while the reason lasts
            self.error = next((o["error"] for o in pair["orders"].values() if o.get("error")), "")
            self.ledger.delete(pair["slug"])
            LOG.warning("开盘前预挂 %s 没有发出：%s", pair["slug"], self.error)
            return ""
        if pair["phase"] != "active" or self.now() >= pair["cancel_ms"] or self.withdrawing():
            if pair["phase"] == "active":
                # Audit PRE-PAUSE-STRIKES: still active = both sides confirmed (an unconfirmed side made it "closing" in send,
                # and that still counts): withdrawn here only for its cancel time or an operator's stop that came during the
                # two POSTs, which the cancellation loop withdraws free ("到时撤单或真实交易暂停") — not the window's failure
                pair["free"] = True
            await self.cancel_pair(pair, "单边未确认或已到撤单时间")
        LOG.info("开盘前预挂 %s：%s", pair["slug"], {s: o["state"] for s, o in pair["orders"].items()})
        return ""

    def apply_fill(self, order: dict, row: dict, pair: dict) -> None:
        # Fixed shares are the actual signed shares — the pair's own (a pair from older settings keeps its size), not an estimated count.
        amount, filled = D(str(row.get("amount") or "0")), D(str(row.get("amountFilled") or row.get("filled") or "0"))  # the fill field as live.order_fill reads it
        if filled <= 0 and str(row.get("status") or "").upper() in FILLED_STATUSES:
            filled = amount  # a record that says filled without the amount: the whole order, by its own word (not a zero-fill end: the other side stays)
        if amount <= 0 or filled < 0 or filled > amount:
            raise core.RemoteError("成交回报数量无效，保留风险额度")
        shares = D(str(pair_quote(pair)[1] or self.settings.shares))
        order["filled"] = max(float(order.get("filled") or 0), float(shares * filled / amount))

    def gave_up(self, pair: dict) -> bool:
        """Past the window's start by GIVE_UP_MS: an order Predict cannot show cannot fill any more (it expired before
        the start), and what it shows is final."""
        return self.now() >= pair["start_ms"] + GIVE_UP_MS

    def off_book(self, order: dict) -> None:
        """Predict's word that the order left the book (a removal it confirmed): "removed" — withdrawn, its final fills
        unread — and when (gone_ms), for the read that may end it (final_read_ms)."""
        order.update(state="removed", gone_ms=self.now())

    def final_read_ms(self, order: dict, pair: dict) -> int:
        """When a read of the record of an order off the book is its last word (ended): LATE_FILL_MS after the order was
        known off the book (gone_ms: the removal's confirmation, or the first read that showed it ended). 0 — any time —
        while that is not known (the first read says), and past the give-up (its signature expired a quarter hour ago)."""
        gone = int(order.get("gone_ms") or 0)
        return gone + LATE_FILL_MS if gone and not self.gave_up(pair) else 0

    def ended(self, order: dict, pair: dict) -> None:
        """An order whose record, just read and its fills applied, says it ended: "final" when that record is its last word
        — the whole order filled (nothing more can show), or read LATE_FILL_MS after the order was known off the book
        (final_read_ms) — else "removed": off the book, its final fills still to read (cancel_pair, at the try bookkeep
        books; monitor_pair for a pair that rests on).

        Audit PRE-LATE-FILL: any such read used to end the order, and a pair whose two records, read right after the
        removal, said CANCELLED with nothing filled was accounted there and then (pnl 0) — and nothing reads an accounted
        pair's orders again (due and settle_pair pass it by; the account snapshot shows open orders alone). That read can
        lag the matching: README 1.119.0, the model trader's 300-share order "撤掉、0 成交" by the cancel's read of the same
        GET /orders/{hash}, with 299.99 shares on the account. Shares matched in the seconds before a withdrawal so stayed
        out of this ledger for good: no P&L, nothing in the day's loss stop, the outcome neither in fills_by_outcome nor in
        unverified_outcomes — the model trader's positions check then took them for a fill of an order of its own on that
        side (the 1.131.5 mix-up, with no undo: the pre-open fills read there never cover them) — and a window withdrawn
        early through no fault of its own (supersedable) was quoted again on top of the fill. Held as "removed" instead,
        the order is all a doubt needs: not verified (the outcome stays in unverified_outcomes, the pair reserved and not
        accounted, its window not quoted again), nothing left to cancel, read again. The price is a minute: a pair
        withdrawn with nothing filled is released, and its window quoted again, LATE_FILL_MS after its orders left the
        book, not at once. A withdrawal costs no request more: the read right after a confirmed removal, which could not
        end the order, is not made (cancel_pair), and a read that comes that late anyway (a big withdrawal's, a start's,
        the result's) ends it as before; only an order that ended outside this mode's cancel is read once more. Not
        covered: a record that shows a fill later still, or never (the model trader's backstop for those is the account's
        positions; this mode has none)."""
        if order["state"] == "final":
            return  # (settle_pair reads a final order's record once more for its fills: final it stays)
        order.setdefault("gone_ms", self.now())
        whole = float(order.get("filled") or 0) >= float(pair_quote(pair)[1] or self.settings.shares) - 1e-9
        order["state"] = "final" if whole or self.now() >= self.final_read_ms(order, pair) else "removed"

    def finals_wait(self, pair: dict) -> int:
        """When a pair with nothing left to cancel has its withdrawn orders' final fills read: once each record is its last
        word (final_read_ms) — the one thing left to do for it. 0, nothing to wait for, while an order is still to cancel
        or to find, or when no order waits for such a read."""
        orders = pair["orders"].values()
        if any(o["state"] not in TERMINAL for o in orders):
            return 0
        return max((self.final_read_ms(o, pair) for o in orders if o["state"] == "removed"), default=0)

    def note_open(self, market: dict) -> None:
        """The first time the listing shows one of this mode's windows OPEN (matching on): kept on the pair — how long
        before the start matching begins is what the cancel time should be set against (/preopen shows the median)."""
        slug = str(market.get("categorySlug") or market.get("slug") or "")
        if str(market.get("tradingStatus") or "").upper() != "OPEN":
            return
        for pair in self.ledger.rows.values():  # every level's record of the window
            if pair["phase"] == "active" and window_of(pair) == slug and not pair.get("open_seen_ms"):
                pair["open_seen_ms"] = self.now()
                self.ledger.save(pair)

    def in_sprint(self, pair: dict) -> bool:
        """The last stretch: past the cancel time, before the signed expiry, with an order that may still be on the book —
        the cancel is asked for every SPRINT_MS, whatever the count (the backoff is for after the expiry, when nothing can
        fill any more and only the records are being read)."""
        now = self.now()
        return (now >= pair["cancel_ms"] and now < pair["expires"] * 1000
                and any(o["state"] in {"open", "placing", "unknown"} for o in pair["orders"].values()))

    def bookkeep(self, pair: dict, save: bool = True) -> None:
        """A closing or unverified pair's next try: SPRINT_MS on in the last stretch (in_sprint), else RETRY_MS after this one,
        doubling to RETRY_MAX_MS — and, with nothing left but its withdrawn orders' final fills, not before a read of their
        records can end them (finals_wait); closed once nothing is left to cancel; accounted (its money released) once
        every order is verified and nothing filled."""
        if self.in_sprint(pair):
            pair["retry_ms"] = self.now() + SPRINT_MS
        else:
            tries = int(pair.get("tries") or 0) + 1
            pair["tries"], pair["retry_ms"] = tries, self.now() + min(RETRY_MS * 2 ** (tries - 1), RETRY_MAX_MS)
        if pair["phase"] != "closed" and all(o["state"] in TERMINAL for o in pair["orders"].values()):
            pair["phase"] = "closed"
        pair["retry_ms"] = max(pair["retry_ms"], self.finals_wait(pair))  # (Audit PRE-LATE-FILL: the read that ends a withdrawn order is made LATE_FILL_MS after it left the book)
        if (not pair.get("accounted") and all(order_verified(o) for o in pair["orders"].values())
                and all(float(o.get("filled") or 0) <= 0 for o in pair["orders"].values())):
            pair.update(accounted=True, accounted_day=dt.datetime.fromtimestamp(self.now() / 1000, core.BEIJING).date().isoformat(), pnl=0.0)
        if save:
            self.ledger.save(pair)

    async def read_final(self, order: dict, pair: dict) -> None:
        """An order's own record, for its final fills: final → "final" once that record is its last word (ended: "removed",
        to be read again, till then); none, past the give-up — never placed (no id) or final as known (an id Predict no
        longer answers to)."""
        row = await self.cancel_api.order(order.get("id", ""), order.get("api_hash") or order["hash"])
        if row:
            if not order.get("id") and row.get("id") is not None:
                order["id"] = str(row["id"])
            self.apply_fill(order, row, pair)
            if final_order(row):
                self.ended(order, pair)
            elif self.gave_up(pair):  # a record Predict still shows open past the give-up: its signature expired long ago, nothing more can fill
                order.update(state="final", note="开始后 15 分钟记录仍不是终态，签名早已过期：按已知成交记账")
        elif self.gave_up(pair):
            if order.get("id"):
                order.update(state="final", note="撤单后按编号查不到记录，按已知成交记账")
            else:
                order.update(state="not_found", note="按哈希查不到，签名已过期：视为从未下单")

    async def cancel_pair(self, pair: dict, reason: str) -> None:
        if pair["slug"] in self.cancelling:
            return
        self.cancelling.add(pair["slug"])
        try:
            pair.update(phase="closing" if pair["phase"] != "closed" else "closed", reason=reason)
            self.ledger.save(pair)
            for order in pair["orders"].values():
                if order["state"] == "prepared":
                    order["state"] = "not_sent"
                    self.ledger.save(pair)
                    continue
                if order["state"] in NEVER_PLACED or order["state"] == "final":
                    continue
                if order["state"] == "removed":  # removed: its final fills, read once more (nothing is left to cancel)
                    if self.now() >= self.final_read_ms(order, pair):  # ...once that read can end it (ended), not before
                        await self.read_final(order, pair)
                        self.ledger.save(pair)
                    continue
                # Unknown submissions are reconciled by their original hash.
                if not order.get("id"):
                    row = await self.cancel_api.order("", order["hash"])
                    if row:
                        order["id"] = str(row["id"])
                        self.apply_fill(order, row, pair)
                    elif self.gave_up(pair):
                        order.update(state="not_found", note="按哈希查不到，签名已过期：视为从未下单")
                        self.ledger.save(pair)
                        continue
                    else:
                        continue
                pair.setdefault("cancel_sent_ms", self.now())
                result = await self.cancel_api.remove_orders([order["id"]])
                if order["id"] in result["removed"]:
                    self.off_book(order)
                    pair.setdefault("cancel_done_ms", self.now())
                    self.ledger.save(pair)
                    if self.now() < self.final_read_ms(order, pair):
                        continue  # the fills before the removal are read LATE_FILL_MS on, at the try bookkeep books: a read now could not end the order (ended)
                await self.read_final(order, pair)  # not removed on Predict's word (it ended some other way, or rests on), or long past its window: what its record says now
                self.ledger.save(pair)
            self.bookkeep(pair)
        finally:
            self.cancelling.discard(pair["slug"])

    async def settle_pair(self, pair: dict) -> None:
        """Official resolution + fresh final fills; never settle on a local candle."""
        import live
        if pair.get("accounted") or self.now() < settle_from(pair):
            return
        if self.now() - int(pair.get("settle_checked") or 0) < 60_000:
            return
        pair["settle_checked"] = self.now()
        market = await self.cancel_api.market(pair["market_id"])
        resolution = core.predict_resolution(market)
        if not resolution:
            return
        if resolution.get("split"):
            up = 0.5
        elif str(resolution.get("name") or "").lower() in live.UP_WORDS:
            up = 1.0
        elif str(resolution.get("name") or "").lower() in live.DOWN_WORDS:
            up = 0.0
        else:
            return
        for order in pair["orders"].values():
            if order["state"] in NEVER_PLACED:
                continue
            await self.read_final(order, pair)
            if order["state"] != "final" and order["state"] not in NEVER_PLACED:
                self.ledger.save(pair)
                return  # a record that is not final yet keeps the full reservation (past the give-up it is final as known)
        price = pair_quote(pair)[0]  # the pair's own price (the settings may have changed since it was placed)
        pair.update(accounted=True, accounted_day=dt.datetime.fromtimestamp(self.now() / 1000, core.BEIJING).date().isoformat(),
                    result="split" if up == 0.5 else "up" if up == 1.0 else "down",
                    pnl=sum(((up if side == "up" else 1 - up) - price) * float(order.get("filled") or 0) for side, order in pair["orders"].items()))
        self.ledger.save(pair)
        if any(float(order.get("filled") or 0) > 0 and (up == 0.5 or (side == "up") == (up == 1.0)) for side, order in pair["orders"].items()):
            soon = getattr(self.owner, "live_redeem_soon", None)
            if callable(soon) and getattr(self.owner.config, "live_auto_redeem", False):
                soon()  # a fill on the winning side: redeemed within seconds (1.131.3), the USDT back for the next quotes

    async def monitor_pair(self, pair: dict) -> None:
        """An order ID is not proof that a deferred post-only order was accepted."""
        if (pair["phase"] != "active" or not self.account_at or self.now() - int(pair.get("created_ms") or 0) < 10_000
                or self.now() - int(pair.get("checked_ms") or 0) < 5_000):
            return  # (no snapshot yet: the first one covers every resting order at once — no burst of reads at a start)
        pair["checked_ms"] = self.now()
        rejected = read = False
        for order in pair["orders"].values():
            # an open order — or one this look found ended a while ago (with a fill: the pair rests on), now that a read of its record can end it (ended)
            if order["state"] != "open" and not (order["state"] == "removed" and self.now() >= self.final_read_ms(order, pair)):
                continue
            if order.get("id") in self.listed_ids and self.now() - self.account_at <= self.account_stale_ms():
                continue  # one paginated account snapshot covers every healthy resting order
            row = await self.api.order(order["id"], order.get("api_hash") or order["hash"])
            if row is None:
                continue  # replication lag is not a zero-fill rejection
            read = True
            self.apply_fill(order, row, pair)
            if final_order(row):
                self.ended(order, pair)  # its last word only LATE_FILL_MS after it was seen ended: "removed" till then, and read again (Audit PRE-LATE-FILL)
                if order["filled"] <= 0:
                    rejected = True
                    if str(row.get("status") or "").upper() in REFUSED_STATUSES:
                        order["refused"] = True  # Predict's deferred refusal (a post-only order that would cross): the window is not quoted again
        if read:
            # Audit PRE-MONITOR-SAVES: saved only when a record was read (a fill or a final state may have changed). checked_ms
            # is pacing only (the same dict sits in ledger.rows, so the next look is paced from memory; after a restart a
            # stale value just makes the first look due at once). A look that every fresh account snapshot covered read
            # nothing and has nothing to make durable — before, it still cost one fsync'd commit (synchronous=FULL) on the
            # event loop per resting pair every MONITOR_MS, i.e. hundreds a second with a full book of pairs.
            self.ledger.save(pair)
        if rejected:
            await self.cancel_many([pair], "一边未成交即结束，撤掉另一边")

    async def cancel_all(self, reason: str, free: bool = False, deadline_s: float | None = None) -> dict:
        return await self.cancel_many([p for p in list(self.ledger.rows.values()) if p["phase"] != "closed"], reason, free, deadline_s)

    async def withdraw_all(self, reason: str) -> str:
        """An operator's withdrawal of this mode's orders (the pause, /live cancel, the emergency stop), the pause already set
        by the caller (1.141.0): a few withdrawn here and now; a book of more than BULK_MIN_IDS left to the cancellation
        loop, which withdraws it in batches — the command answers at once instead of minutes later. The reply's line."""
        n = sum(len(removable_ids(p)) for p in self.ledger.rows.values() if p["phase"] != "closed")
        if n <= BULK_MIN_IDS or not self.alive:
            # Audit PRE-PAUSE-STRIKES: an operator's stop is not the window's doing, so the resting pairs withdrawn here and
            # now are free (counts_against), as the cancellation loop's withdrawal of a bigger book always was
            # ("到时撤单或真实交易暂停", free=True). They used to keep the operator's reason without the mark: a pause, /live
            # cancel all or the emergency stop of BULK_MIN_IDS ids or fewer (PREOPEN_AHEAD_MINUTES=60 with BTC alone: about
            # eight), or of any book once the controller had crashed, counted one failure against every withdrawn window's
            # level — three pauses and the window was never quoted again ("失败满 3 次"), against PREOPEN.md (停止或暂停预挂
            # 撤掉的不算次数); whether a pause counted depended on the book's size. Only the active pairs: one already closing
            # on its own failure (a side not confirmed or ended unfilled, the market's status) keeps its count —
            # cancel_all(free=True) would erase it, and the cap with it. cancel_all below saves the marks (cancel_many saves
            # every pair it withdraws, with no await before).
            for pair in self.ledger.rows.values():
                if pair["phase"] == "active":
                    pair["free"] = True
        if n <= BULK_MIN_IDS:
            # 1.150.0 (Audit XR-STOP-OK-PREOPEN): the few withdrawn here and now say what did not go. This answered "" whatever
            # the removals did, and the emergency stop's verdict (LiveBot.live_control, Audit LIVE-STOP-OK) reads "没撤成" in this
            # line alone: a stop whose pre-open removals all failed (a server error, the sign-in refused, a cooldown on the key)
            # on a book of BULK_MIN_IDS ids or fewer — PREOPEN_AHEAD_MINUTES=60 with BTC alone, or the 30 a crashed controller's
            # first try left — came back ok=True, a green toast, with the bids still up; on a crashed controller nothing tries
            # again (no loop) before their signed expiry. Counted from the removals' own answer, as the bulk status and /live
            # cancel account count it: failed (the request failed) and left (never sent) are misses; Predict's "noop" (nothing
            # to remove: the order is gone) and an id its answer left out are not — the ledger still says "open" for those
            # until the reconcile reads them, so a recount of removable_ids here would turn an ordinary stop red. The same
            # requests go out as before; only the reply changes (the stop, /preopen pause and /live cancel all print it).
            result = await self.cancel_all(reason)
            missed = len(result["failed"]) + len(result["left"])
            if not missed:
                return ""
            why = result["errors"][-1] if result["errors"] else "没发出"
            if not self.alive:
                return f"预挂模式已停止：还有 {missed} 张没撤成（{why}；各自到期前仍挂着；重新打开预挂会接着撤，或再发一次）"
            return f"预挂单还有 {missed} 张没撤成（{why}）：本模式的撤单循环稍后接着撤"
        if not self.alive:  # 1.141.1: no loop to leave them to — here, for DEAD_WITHDRAW_S at most (the command waits)
            await self.cancel_all(reason, deadline_s=DEAD_WITHDRAW_S)
            left = sum(len(removable_ids(p)) for p in self.ledger.rows.values() if p["phase"] != "closed")
            return (f"预挂模式已停止：{DEAD_WITHDRAW_S} 秒内撤了 {n - left} 张，还有 {left} 张没撤成（各自到期前仍挂着；重新打开预挂会接着撤，或再发一次）"
                    if left else f"预挂模式已停止：{n} 张预挂单已撤")
        return f"正在分批撤 {n} 张预挂单（{self.remove_limit().text()}），/preopen 看进度"

    async def cancel_many(self, pairs: list[dict], reason: str, free: bool = False, deadline_s: float | None = None) -> dict:
        """Bulk removals first; slow hash/fill reads never queue known IDs behind them. ``free``: a withdrawal that was not
        the window's doing — it does not count against its MAX_ATTEMPTS (counts_against). 1.141.0: the removals in batches
        at Predict's measured size (remove_pairs), a rate limit waited out when there are many. Returns remove_pairs' result
        (1.150.0: withdraw_all's reply says what it could not withdraw)."""
        for pair in pairs:
            pair.update(phase="closing", reason=reason, **({"free": True} if free else {}))
            for order in pair["orders"].values():
                if order["state"] == "prepared":
                    order["state"] = "not_sent"
        self.ledger.save_many(pairs)
        return await self.remove_pairs(pairs, deadline_s=deadline_s)

    def bulk_client(self) -> Any:
        """The client the batch withdrawals use: their own (bulk_api), unless the cancel client was put in from outside (the tests)."""
        return self.bulk_api if self.cancel_api is self.cancel_made else self.cancel_api

    def remove_limit(self) -> Any:
        """Predict's measured removal batch (live.RemoveLimit): the owner's, kept in its store across restarts; else this controller's own."""
        get = getattr(self.owner, "live_remove_limit", None)
        if callable(get):
            return get()
        if self._remove_limit is None:
            import live
            self._remove_limit = live.RemoveLimit()
        return self._remove_limit

    async def remove_pairs(self, pairs: list[dict], patient: bool | None = None, deadline_s: float | None = None) -> dict:
        """The pairs' resting orders withdrawn in batches (live.remove_in_batches, 1.141.0), the soonest window first; then each
        pair booked (bookkeep) and its final fills read (launch_reconcile). ``patient`` — when there are more than
        BULK_MIN_IDS, unless told — waits out a rate limit and leaves a pair whose order could not be sent to the loop,
        which takes it on in batches again once its retry is due; the few (a window at its cancel time) get one try each,
        their retry the per-window path's, as before."""
        import live
        pairs = sorted(pairs, key=lambda p: (int(p.get("cancel_ms") or p.get("start_ms") or 0), str(p.get("slug") or "")))
        by_id: dict[str, tuple[dict, dict]] = {}
        for pair in pairs:
            for order in pair["orders"].values():
                if order.get("id") and order["state"] not in TERMINAL:
                    by_id[str(order["id"])] = pair, order
        patient = len(by_id) > BULK_MIN_IDS if patient is None else patient
        for pair, _ in by_id.values():
            pair.setdefault("cancel_sent_ms", self.now())  # the round trip and the lead on the start: /preopen shows them (the cancel time's evidence)

        def answered(removed: list[str], noop: list[str]) -> None:
            touched: dict[str, dict] = {}
            for oid in removed:
                if oid in by_id:
                    pair, order = by_id[oid]
                    self.off_book(order)
                    pair.setdefault("cancel_done_ms", self.now())
                    touched[pair["slug"]] = pair
            self.ledger.save_many(list(touched.values()))
        slugs = {p["slug"] for p in pairs}
        self.bulk_slugs |= slugs
        limit = self.remove_limit()
        result: dict = {"removed": [], "noop": [], "unanswered": [], "failed": [], "left": [], "errors": [], "requests": 0, "limited": 0, "seconds": 0.0}
        progress: dict | None = {} if patient and by_id else None
        if progress is not None:
            self.bulk_progress = progress  # the status shows the latest one under way
        try:
            if by_id:
                result = await live.remove_in_batches(
                    self.bulk_client() if patient else self.cancel_api, list(by_id), limit, workers=BULK_WORKERS, patient=patient,
                    deadline=time.monotonic() + deadline_s if deadline_s else None, stop=self.owner.stopping.is_set,
                    on_answer=answered, progress=progress, now_ms=self.now)
        finally:
            self.bulk_slugs -= slugs
            save = getattr(self.owner, "live_remove_saved", None)
            if callable(save):
                with contextlib.suppress(Exception):
                    save(limit)
            if progress is not None and self.bulk_progress is progress:
                self.bulk_progress = {}
        if progress is not None:  # done (a withdrawal cut short by the controller's stop leaves no figures)
            self.bulk_last = {"at": self.now(), "orders": len(by_id), "removed": len(result["removed"]),
                              "quiet": len(result["noop"]) + len(result["unanswered"]), "missed": len(result["failed"]) + len(result["left"]),
                              "requests": result["requests"], "limited": result["limited"], "seconds": result["seconds"]}
        if result["errors"]:
            self.error = result["errors"][-1]
        unsent = set(result["left"])  # never sent: the loop takes them on in batches again; a failed one goes to the per-pair path
        for pair in pairs:
            if result.get("halted") == "refused" and any(str(o.get("id") or "") in unsent for o in pair["orders"].values()):
                pair["bulk_refused"] = int(pair.get("bulk_refused") or 0) + 1  # BULK_REFUSED_MAX of these: one order at a time from then on
            self.bookkeep(pair, save=False)  # closed once nothing is left to cancel; the next try RETRY_MS on, doubling (not every half second)
        self.ledger.save_many(pairs)
        for pair in pairs:
            if pair.get("accounted") or pair["slug"] in self.cancelling:
                continue
            if patient and any(str(o.get("id") or "") in unsent for o in pair["orders"].values()):
                continue  # not sent: the loop takes it on in batches again once its retry is due
            if self.now() < self.finals_wait(pair):
                continue  # every order off the book this moment: their final fills are read LATE_FILL_MS on, at the try bookkeep booked (ended) — nothing to do now
            self.launch_reconcile(pair)  # the removed orders' final fills, the unknown ones' hashes: the reservation ends on what they show
        return result

    def wind_down(self, reason: str) -> None:
        """The mode switched off with orders out (1.141.0): nothing new goes out, every resting order is withdrawn in
        batches until none is left — a rate limit waited out, not cut after 12 seconds as before — their final fills are
        read, then the controller stops itself (the owner clears it: LiveBot.preopen_apply)."""
        if not self.winding:
            self.winding, self.winding_since, self.rest_clear_ms = reason, self.now(), 0
            LOG.info("开盘前预挂：%s，撤单收尾中", reason)

    def wound_up(self, now_ms: int) -> bool:
        """Winding down: done once no order of this mode can be on the book (withdrawn, ended, or past its signed expiry) and
        every withdrawn one's final fills are read — or FINALS_WAIT_MS after nothing could rest (the rest read at the next
        start)."""
        if self.bulk_task is not None and not self.bulk_task.done():
            return False
        rows = [p for p in self.ledger.rows.values() if not p.get("accounted")]
        if any(p["phase"] == "active" or resting_orders(p, now_ms) for p in rows):
            self.rest_clear_ms = 0
            return False
        self.rest_clear_ms = self.rest_clear_ms or now_ms
        unread = any(not order_verified(o) for p in rows for o in p["orders"].values())
        reading = any(key.startswith("cancel:") and not task.done() for key, task in self.settling.items())
        return not (unread or reading) or now_ms - self.rest_clear_ms >= FINALS_WAIT_MS

    def bulk_line(self) -> str:
        """The batch withdrawals, for the status: the one under way (how far), else the last one; the measured batch."""
        pr, last, now = self.bulk_progress, self.bulk_last, self.now()
        parts = []
        if pr.get("total"):
            took = time.monotonic() - float(pr.get("started") or time.monotonic())
            parts.append(f"正在分批撤：已撤 {pr.get('removed', 0)} / {pr['total']} 张" + (f"（{pr['noop']} 张已不在簿上）" if pr.get("noop") else "")
                         + f"，{pr.get('requests', 0)} 个请求、同时 {pr.get('workers', 0)} 个，{took:.0f} 秒"
                         + (f"，限流 {pr['limited']} 次（等冷却结束接着撤）" if pr.get("limited") else ""))
        elif last:
            parts.append(f"上次分批撤 {last['orders']} 张（{when(last['at'], now)}）：撤掉 {last['removed']} 张" + (f"、{last['quiet']} 张已不在簿上" if last.get("quiet") else "")
                         + f"，{last['seconds']:.0f} 秒、{last['requests']} 个请求" + (f"、限流 {last['limited']} 次" if last.get("limited") else "")
                         + (f"；{last['missed']} 张没撤成，稍后接着撤" if last.get("missed") else ""))
        if self.winding or not self.alive:
            # Audit PRE-CRASH-WITHDRAW: a crashed controller too — its status's "N 条挂着" counts the active records, which its
            # withdrawal marked closing in memory whether or not a removal went out ("0 条挂着" with every order still up)
            parts.append(f"还可能挂着 {sum(resting_orders(p, now) for p in self.ledger.rows.values())} 张")
        return "批量撤单：" + self.remove_limit().text() + ("；" + "；".join(parts) if parts else "")

    def launch_reconcile(self, pair: dict) -> None:
        key = "cancel:" + pair["slug"]
        if key in self.settling and not self.settling[key].done():
            return
        async def reconcile() -> None:
            async with self.reconcile_gate:
                try:
                    await self.cancel_pair(pair, pair.get("reason") or "撤单状态核对")
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self.error = core.clean_error(error)
        self.settling[key] = asyncio.create_task(reconcile(), name=key)

    def due(self, now_ms: int) -> list[dict]:
        """The pairs the cancellation loop acts on now: active ones at their cancel time (or the mode blocked), and
        closing or unverified ones whose next try has come (bookkeep: RETRY_MS doubling to RETRY_MAX_MS)."""
        blocked = bool(self.withdrawing())  # an operator's stop withdraws the active pairs; not the stop itself (on_stop decides), not an account not verified yet
        out = []
        for p in self.ledger.rows.values():
            if p.get("accounted"):
                continue
            if p["phase"] == "active" and (now_ms >= p["cancel_ms"] or blocked):
                out.append(p)
            elif p["phase"] != "active" and now_ms >= int(p.get("retry_ms") or 0):
                out.append(p)
        return sorted(out, key=lambda p: p["cancel_ms"])

    async def placement_worker(self) -> None:
        while not self.stopped():
            _, _, c = await self.queue.get()
            self.placing.add(c["slug"])
            wait = 0
            try:
                if await self.place_pair(c) == "held":
                    wait = HOLD_RETRY_MS
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.error = core.clean_error(error) or type(error).__name__
                LOG.warning("开盘前挂单失败 %s：%s", c["slug"], self.error)
                wait = ERROR_RETRY_MS
            finally:
                self.placing.discard(c["slug"])
                if wait and not self.stopped():  # still queued: placed again later (unpark), the window not read again meanwhile
                    self.parked[c["slug"]] = (self.now() + wait, c)
                else:
                    self.queued.discard(c["slug"])
                self.queue.task_done()

    def unpark(self) -> None:
        """The held candidates whose wait is over go back on the queue (the soonest first); one whose window is too close
        to its start is dropped, as the discovery would not take it either."""
        for slug, (at, c) in list(self.parked.items()):
            if c["start_ms"] - self.now() < self.settings.entry_seconds * 1000:
                del self.parked[slug]
                self.queued.discard(slug)
            elif self.now() >= at:
                del self.parked[slug]
                self.queue.put_nowait((c["start_ms"], c["slug"], c))

    async def account_loop(self) -> None:
        """The account snapshot (the open orders, the fills they show, the balance) every poll, on its own: the discovery
        and the refreshes, however long they take, never let it go stale (a stale snapshot holds every placement)."""
        while not self.stopped():
            started, failed = time.monotonic(), False
            try:
                if not self.blocked():
                    await self.account()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                failed = True
                self.error = core.clean_error(error) or type(error).__name__
                LOG.warning("开盘前账户快照：%s", self.error)
            await asyncio.sleep(self.account_wait(time.monotonic() - started, failed))

    def poll_wait(self) -> float:
        """Seconds to the next discovery: FAST_POLL_S while a card's next listing is expected — a cycle after its last one
        seen, LISTING_EARLY_MS before to LISTING_LATE_MS after (or until it is seen) — else the poll setting."""
        now = self.now()
        for key, at in self.listed_at.items():
            due = at + cycle_ms(key)
            if due - LISTING_EARLY_MS <= now <= due + LISTING_LATE_MS:
                return FAST_POLL_S
        for entered in self.awaiting.values():  # 1.133.1: a card's next window in the range and not listed yet — Predict lists it about a minute after it enters (11-06: 85 s), whatever the last listing's time
            if 0 <= now - entered <= LISTING_LATE_MS:
                return FAST_POLL_S
        return min(LISTING_PROBE_S, float(self.settings.poll))  # the listings probed every LISTING_PROBE_S; the older windows re-read at the poll's pace (discovery_loop)

    async def discovery_loop(self) -> None:
        """The discovery on its own (1.131.0: the refreshes have their own loop — a batch of reads never delays a listing's find)."""
        while not self.stopped():
            try:
                if not self.blocked():  # the listings every tick; the known windows' re-read once a poll (and never while a listing is expected: discover)
                    await self.discover(probe_only=self.now() - self.rescan_ms < self.settings.poll * 1000)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.error = core.clean_error(error) or type(error).__name__
                LOG.warning("开盘前市场扫描：%s", self.error)
            await asyncio.sleep(self.poll_wait())

    async def refresh_loop(self) -> None:
        while not self.stopped():
            try:
                if not self.blocked():
                    await self.refresh_active()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.error = core.clean_error(error) or type(error).__name__
                LOG.warning("开盘前市场重读：%s", self.error)
            await asyncio.sleep(self.settings.poll)

    async def cancellation_loop(self) -> None:
        while not self.stopped():
            self.unpark()
            now = self.now()
            def count(client: Any) -> int:  # a fake's calls are a list
                made = getattr(client, "calls", 0)
                return len(made) if isinstance(made, list) else int(made or 0)
            calls = sum(count(client) for client in {id(x): x for x in (self.api, self.cancel_api, self.bulk_client())}.values())
            if not self.rate_samples or now - self.rate_samples[-1][0] >= 500:
                self.rate_samples.append((now, calls))
            await self.check_cards(now)
            self.note_cooldown()
            if now - self.stuck_checked_ms >= 60_000:
                self.stuck_checked_ms = now
                self.check_stuck(now)
            if now - self.archived_ms >= ARCHIVE_EVERY_MS:
                self.archive(now)  # (never raises; only accounted, closed records go: none this loop or a task of it still works on)
            due = self.due(now)
            if due:
                active = [p for p in due if p["phase"] == "active"]
                if active:
                    await self.cancel_many(active, "到时撤单或真实交易暂停", free=True)
                # 1.141.1: a window in its last stretch (past its cancel time, before its signed expiry) with an order still out is
                # removed from here, one try every SPRINT_MS — never queued behind the final reads a big withdrawal left
                sprint = [p for p in due if p["phase"] != "active" and p["slug"] not in self.bulk_slugs and self.in_sprint(p) and removable_ids(p)]
                if sprint:
                    await self.remove_pairs(sprint, patient=False)
                done = {p["slug"] for p in sprint}
                stale = [p for p in due if p["phase"] != "active" and p["slug"] not in done and p["slug"] not in self.cancelling and p["slug"] not in self.bulk_slugs]
                heavy = [p for p in stale if removable_ids(p) and int(p.get("bulk_refused") or 0) < BULK_REFUSED_MAX]
                if sum(len(removable_ids(p)) for p in heavy) > BULK_MIN_IDS:
                    # 1.141.0: many closing pairs with orders out (a settings change withdrew them, a batch left them): in batches,
                    # beside this loop — the windows at their cancel time go on being withdrawn on time meanwhile
                    if self.bulk_task is None or self.bulk_task.done():
                        self.bulk_task = asyncio.create_task(self.remove_pairs(heavy, patient=True), name="preopen-bulk")
                    held = {p["slug"] for p in heavy}  # the running batch's turn ends first: these wait for the next
                    stale = [p for p in stale if p["slug"] not in held]
                for pair in stale:
                    if self.now() < self.finals_wait(pair):
                        continue  # withdrawn this very pass, every order off the book: nothing to do before their final reads, which its bookkeep booked (ended)
                    self.bookkeep(pair)  # the next try is booked now: a read that hangs does not make the loop ask again meanwhile
                    self.launch_reconcile(pair)
            if self.winding and self.wound_up(now):
                unread = sum(1 for p in self.ledger.rows.values() if not p.get("accounted") and any(not order_verified(o) for o in p["orders"].values()))
                self.notify(f"✅ 开盘前预挂已关闭：本模式的挂单已全部撤完" + (f"（{unread} 场的成交还没核对完，下次开启时接着核对）" if unread else ""), key="preopen:wound")
                LOG.info("开盘前预挂收尾完成：%s", self.winding)
                self.stop.set()
                return
            for pair in list(self.ledger.rows.values()):
                if pair["phase"] == "active" and now - int(pair.get("checked_ms") or 0) >= MONITOR_MS and now - int(pair.get("created_ms") or 0) >= 10_000:
                    key = "monitor:" + pair["slug"]  # (a look due: not a task for every active pair twice a second)
                    if key not in self.settling or self.settling[key].done():
                        async def monitor(p: dict = pair) -> None:
                            async with self.settle_gate:
                                try:
                                    await self.monitor_pair(p)
                                except asyncio.CancelledError:
                                    raise
                                except Exception as error:
                                    self.error = core.clean_error(error)
                        self.settling[key] = asyncio.create_task(monitor(), name=key)
                if pair.get("accounted") or self.now() < settle_from(pair):
                    continue
                key = "settle:" + pair["slug"]
                if key in self.settling and not self.settling[key].done():
                    continue
                async def settle(p: dict = pair) -> None:
                    async with self.settle_gate:
                        try:
                            await self.settle_pair(p)
                        except asyncio.CancelledError:
                            raise
                        except Exception as error:
                            self.error = core.clean_error(error)
                self.settling[key] = asyncio.create_task(settle(), name=key)
            self.settling = {k: t for k, t in self.settling.items() if not t.done()}
            await asyncio.sleep(0.5)

    async def run(self) -> None:
        jobs: list[asyncio.Task] = []
        stops: list[asyncio.Task] = []
        try:
            try:
                # Audit PRE-CRASH-WITHDRAW: adopt() inside the guard whose finally runs on_stop. It ran before it, so a failure
                # there (a record it must change — a window that started during a restart, a pair to withdraw — on a ledger
                # it cannot write) ended the mode with the orders it had just taken over left resting, unmonitored, while
                # the crash notice said they were withdrawn; now it withdraws them like any crash.
                self.adopt()  # the last run's pairs: taken over as they are, or withdrawn and reconciled
                jobs = [asyncio.create_task(self.account_loop(), name="preopen-account"),
                        asyncio.create_task(self.discovery_loop(), name="preopen-discovery"),
                        asyncio.create_task(self.refresh_loop(), name="preopen-refresh"),
                        asyncio.create_task(self.cancellation_loop(), name="preopen-cancel"),
                        *(asyncio.create_task(self.placement_worker(), name=f"preopen-place-{i}") for i in range(PLACE_WORKERS))]
                stops = [asyncio.create_task(self.owner.stopping.wait()), asyncio.create_task(self.stop.wait())]
                await asyncio.wait([*stops, *jobs], return_when=asyncio.FIRST_COMPLETED)
                for job in jobs:
                    if job.done() and not job.cancelled() and job.exception():
                        raise job.exception()
            finally:
                bulk = [self.bulk_task] if self.bulk_task is not None else []
                for job in [*stops, *jobs, *bulk]:
                    job.cancel()
                await asyncio.gather(*stops, *jobs, *bulk, return_exceptions=True)
                await self.on_stop()
                for task in self.settling.values():
                    task.cancel()
                await asyncio.gather(*self.settling.values(), return_exceptions=True)
        finally:
            self.alive = False  # the model trader opens orders again (LiveBot.preopen_active)

    def timing_line(self) -> str:
        """What the last windows measured: the cancel's round trip and how long before the start it was confirmed, and how
        long before the start the listing first showed them OPEN — the evidence PREOPEN_CANCEL_SECONDS is set against."""
        rows = sorted(self.ledger.rows.values(), key=lambda p: p["cancel_ms"])
        sent = [p for p in rows if p.get("cancel_sent_ms") and p.get("cancel_done_ms")
                and p["cancel_sent_ms"] >= p["cancel_ms"] - 1000][-20:]  # the scheduled cancels only: a withdrawal at a restart or a settings change says nothing about the last stretch
        parts = []
        if sent:
            trips = sorted((p["cancel_done_ms"] - p["cancel_sent_ms"]) / 1000 for p in sent)
            leads = sorted((p["start_ms"] - p["cancel_done_ms"]) / 1000 for p in sent)
            parts.append(f"撤单确认用时中位 {trips[len(trips) // 2]:.1f} 秒（最长 {trips[-1]:.1f}），确认时距开场中位 {leads[len(leads) // 2]:.0f} 秒"
                         f"（最短 {leads[0]:.0f}；最近 {len(sent)} 场）")
        else:
            parts.append("撤单确认用时：还没有撤过")
        opened = [p for p in rows if p.get("open_seen_ms")][-20:]
        if opened:
            leads = sorted((p["start_ms"] - p["open_seen_ms"]) / 1000 for p in opened)
            parts.append(f"撮合开启（列表显示 OPEN）在开场前中位 {leads[len(leads) // 2]:.0f} 秒（最早 {leads[-1]:.0f}、最晚 {leads[0]:.0f}；{len(opened)} 场）")
        else:
            unseen = sum(1 for p in rows if not p.get("open_seen_ms") and self.now() >= p["cancel_ms"])
            parts.append(f"撮合开启：{unseen} 场到撤单时还没见到 OPEN" if unseen else "撮合开启：还没有观察")
        placed = [p for p in rows if p.get("created_ms")][-20:]
        if placed:  # how long before the start a window was found listed and placed: the evidence PREOPEN_AHEAD_MINUTES is set against
            leads = sorted((p["start_ms"] - p["created_ms"]) / 60_000 for p in placed)
            parts.append(f"挂上时距开场中位 {leads[len(leads) // 2]:.0f} 分钟（最长 {leads[-1]:.0f}；{len(placed)} 场）"
                         + ("，贴着提前范围：Predict 可能发布得更早，可调大 PREOPEN_AHEAD_MINUTES"
                            if self.settings.ahead_minutes < 1440 and leads[-1] >= self.settings.ahead_minutes - 15 else ""))
        quick = [p for p in rows if p.get("seen_ms") and p.get("placed_ms") and p.get("created_ms")][-20:]
        if quick:  # from the discovery's read of the listing to both orders answered: the bot's own share of the queue position (1.133.0)
            spans = sorted((int(p["placed_ms"]) - int(p["seen_ms"])) / 1000 for p in quick)
            parts.append(f"发现→挂上中位 {spans[len(spans) // 2]:.1f} 秒（最长 {spans[-1]:.1f}；{len(quick)} 场）")
        if self.listing_deltas:  # each new listing against the moment expected (the last one a cycle on): how well the fast poll is aimed
            deltas = sorted(d / 1000 for d in self.listing_deltas)
            parts.append(f"发布比预计中位 {deltas[len(deltas) // 2]:+.0f} 秒（最早 {deltas[0]:+.0f}、最晚 {deltas[-1]:+.0f}；{len(deltas)} 次）")
        return "；".join(parts)

    def unquotable(self) -> tuple[list[dict], list[dict]]:
        """The coming windows' levels (start beyond the new-order cutoff) this mode will not quote again, the soonest first:
        (their failures at MAX_ATTEMPTS, refused by Predict) — each level's latest record (1.131.6: /preopen says so; a
        window that went missing used to say nothing)."""
        strikes = window_strikes(self.ledger.rows.values())
        latest: dict[str, dict] = {}
        for p in self.ledger.rows.values():
            k = record_key(p)
            if k not in latest or int(p.get("attempt") or 0) > int(latest[k].get("attempt") or 0):
                latest[k] = p
        cutoff = self.now() + self.settings.entry_seconds * 1000
        spent, refused = [], []
        for k, p in latest.items():
            if p["phase"] == "active" or not p.get("accounted") or int(p.get("start_ms") or 0) <= cutoff:
                continue
            if any(o["state"] == "rejected" and not o.get("limited") or o.get("refused") for o in p["orders"].values()):
                refused.append(p)
            elif strikes.get(k, 0) >= MAX_ATTEMPTS:
                spent.append(p)
        soonest = lambda ps: sorted(ps, key=lambda p: int(p.get("start_ms") or 0))
        return soonest(spent), soonest(refused)

    def unplaced(self) -> tuple[int, int, float]:
        """The listed windows' levels not quoted now — (windows, level-pairs, the reservation they would take): what the cap
        keeps back, so the status can say what PREOPEN_MAX_OPEN_USD would need to be for all of them (1.132.1). A card's
        windows up to the latest one seen listed count as listed; a level with its failures at the cap does not count."""
        now = self.now()
        active = {record_key(p) for p in self.ledger.rows.values() if p["phase"] == "active"}
        strikes = window_strikes(self.ledger.rows.values())
        windows: set[str] = set()
        pairs, usd = 0, 0.0
        for rolling, start in window_starts(now, self.owner.config, self.settings):
            if start > self.listed_by_card.get(rolling.key, 0):
                continue
            slug = window_slug(rolling, start)
            for level, (price, shares) in enumerate(self.settings.levels):
                if level and start - now > self.settings.deep_ahead_minutes * 60_000:
                    break
                key = slug if level == 0 else f"{slug}#{level}"
                if key in active or strikes.get(key, 0) >= MAX_ATTEMPTS:
                    continue
                windows.add(slug)
                pairs += 1
                usd += 2 * float(price) * float(shares)
        return len(windows), pairs, usd

    def status(self) -> str:
        rows = list(self.ledger.rows.values())
        spent, refused = self.unquotable()
        quoted_to: dict[str, int] = {}  # card -> the latest window with a quote resting (the first level's, the deeper ones wait for the start)
        for p in rows:
            if p["phase"] == "active" and p.get("key"):
                quoted_to[p["key"]] = max(quoted_to.get(p["key"], 0), int(p.get("start_ms") or 0))
        windows_left, pairs_left, usd_left = self.unplaced()
        named = lambda ps: "、".join(str(p.get("name") or window_of(p)) + (f" 第 {int(p['level']) + 1} 档" if p.get("level") else "") for p in ps[:3]) + ("…" if len(ps) > 3 else "")
        why = (self.hold() or self.budget_problem()) if self.alive else ""
        state = ("已停止" if not self.alive else f"收尾中（{self.winding}）：不再新挂，挂着的单分批撤完、成交核对完就自动停止" if self.winding
                 else f"暂停，{why}" if self.blocked() else "运行")
        bulk = self.bulk_line()
        busy = bool(self.winding or self.bulk_progress.get("total"))  # under way: right under the state line, else at the end
        until = lambda ms: f"{when(ms, self.now())}（开场前 {(ms - self.now()) / 60_000:.0f} 分钟）" if ms > self.now() else "还没见到已发布的场次"
        reach = lambda key: (f"{key} 发布到 {until(self.listed_by_card[key])}" if self.listed_by_card.get(key, 0) > self.now() else f"{key} 还没见到已发布的场次") \
            + (f"，挂到 {hms(quoted_to[key])}" if quoted_to.get(key) else "")
        listed = ("没选卡：不挂新场次（控制台“预挂哪几张卡”一键全关；再点亮一张就恢复）" if not self.settings.markets
                  else "已发布到 " + until(self.listed_until_ms) + (f"，挂到 {hms(max(quoted_to.values()))}" if quoted_to else "") if len(self.settings.markets) == 1
                  else "各卡：" + "；".join(reach(key) for key in sorted(self.settings.markets)))
        return (f"开盘前自动预挂：{state}\n" + (bulk + "\n" if busy else "") +
                f"{' / '.join(sorted(self.settings.markets)) or '没选卡'}：两边各 {levels_text(self.settings)}；"
                f"扫描 {self.settings.poll:g} 秒，"
                + ("挂 24 小时内已发布的全部场次" if self.settings.ahead_minutes >= 1440 else f"只挂 {self.settings.ahead_minutes} 分钟内开始的场次") + "\n"
                f"撤单：开场前 {self.settings.cancel_seconds} 秒发出，之后到开场前 {self.settings.expiry_seconds} 秒到期为止每 {SPRINT_MS // 1000} 秒催一次；"
                f"不足 {self.settings.entry_seconds} 秒不再新挂"
                + (f"；股票卡开盘、午后头 {self.settings.open_quiet_minutes} 分钟内开始的场次不挂" if self.settings.open_quiet_minutes else "")
                + (f"；周末不挂（北京时间周六 00:00 到周一 {WEEKEND_END_HOUR:02d}:00 开始的场次）" if not self.settings.weekend else "") + "\n"
                f"已记录 {len({window_of(p) for p in rows})} 场、{len(rows)} 条（{sum(1 for p in rows if p['phase'] == 'active')} 条挂着，{sum(1 for p in rows if p.get('accounted'))} 条已结清）"
                + (f"，另有 {self.ledger.archived_count} 条已归档（结束超过 {ARCHIVE_AFTER_MS // core.DAY_MS} 天、已撤且没成交的；复盘页照常统计）" if self.ledger.archived_count else "") + "，"
                f"队列 {self.queue.qsize() + len(self.parked)} 场" + (f"（{len(self.parked)} 场在等，见暂不新挂）" if self.parked else "") + f"；{listed}"
                + (f"；本次启动接管 {self.resumed} 场" if self.resumed else "") + (f"；重新报价 {self.requoted} 场" if self.requoted else "") + "\n"
                f"预留 {self.risk():.2f}U{f' / 限额 {self.settings.max_open_usd:g}U' if self.settings.max_open_usd else '，不设上限'}（本模式自己的账：Predict 不锁定挂单资金，和原策略的持仓+挂单上限各算各的；撤掉的场次在撤单 {LATE_FILL_MS // 1000} 秒后读到一份没成交才释放）"
                + (f"\n已发布未挂：{windows_left} 场 {pairs_left} 档，约 {usd_left:.0f}U" + (f"；预挂资金上限调到 {self.risk() + usd_left:.0f}U 以上可全部挂上" if self.settings.max_open_usd else "，按开场先后补挂中") if pairs_left else "")
                + (f"\n请求约 {self.request_rate():.1f} 次/秒（最近一分钟）；" if self.request_rate() is not None else "\n")
                + f"账户快照每 {self.account_interval_s():.0f} 秒（{self.account_pages} 页挂单" + (f"，上次读了 {self.account_read_s:.0f} 秒" if self.account_read_s >= 1 else "") + "）"
                + f"\n扫描：{self.scan_note or '还没扫过'}（发布探测每 {LISTING_PROBE_S:g} 秒，预计发布前后每 {FAST_POLL_S:g} 秒；"
                + ("分类应答自带下单字段，发现即下单" if self.category_full else "分类应答不带下单字段，每场多读一次详情" if self.category_full is False else "还没读过发布")
                + (f"；冲刺中：{'、'.join(sorted(k for k, t in self.awaiting.items() if 0 <= self.now() - t <= LISTING_LATE_MS))} 的下一场已进范围、等发布"
                   if any(0 <= self.now() - t <= LISTING_LATE_MS for t in self.awaiting.values()) else "") + "）"
                + f"\n最近问题：{self.error or '无'}"
                + (f"\n暂不新挂：{why}" if why and state == "运行" else "")
                + (f"\n周末不挂：现在是周末，不挂新场次，周一 {WEEKEND_END_HOUR:02d}:00（北京时间）起恢复" if not self.settings.weekend and on_weekend(self.now()) else "")
                + (f"\n账户另有 {self.unmanaged} 笔本程序没记录的挂单（手工挂的或旧版本留下的）：不计入预挂限额，也不挡预挂"
                   + ("；其中挂在本机器人市场上的，原策略 2 分钟后自动撤掉（LIVE_ORPHAN_CANCEL）" if getattr(self.owner.config, "live_orphan_cancel", True) else "") if self.unmanaged else "")
                + ("\n股票卡暂停：" + "；".join(self.card_holds.values()) if self.card_holds else "")
                + ("\n发布节奏（每张卡一场一次：15 分钟卡每刻钟、5 分钟卡每 5 分钟、小时卡每小时、日卡每天；预计前后每秒扫一次）：" + "；".join(f"{key} 上次 {hms(at)}，下次预计 {hms(at + cycle_ms(key))}" for key, at in sorted(self.listed_at.items()))
                   if self.listed_at else "")
                + (f"\n没挂的已发布场次：" + "；".join(f"{k.rsplit('-', 1)[-1]} {v}" for k, v in list(self.skipped.items())[-3:]) if self.skipped else "")
                + (f"\n不再报价：{len(spent)} 档失败满 {MAX_ATTEMPTS} 次（单边没确认、一边被撤或过期），如 {named(spent)}" if spent else "")
                + (f"\n不再报价：{len(refused)} 档被 Predict 拒绝（post-only 会吃单等），如 {named(refused)}" if refused else "") + "\n"
                f"实测：{self.timing_line()}" + ("" if busy else "\n" + bulk))
