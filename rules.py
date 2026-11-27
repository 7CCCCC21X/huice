"""规则吃单 (RULE_TAKER, 1.153.0): a few fixed windows of the gold and oil 15-minute cards whose outcome is not the coin flip the
book prices — gold's index stands still while CME is shut (an end equal to the start settles Up), the first window after the
daily reopen and gold's 05:15 New York window lean Down — are bought on the side the rule names whenever, before the window
starts, the book offers that side at or under the rule's price. A taker: a LIMIT order at the rule's price for what the book
shows there (never more than RULE_SHARES a window), what does not fill at once withdrawn a moment later. 1.154.15: with enough
on offer the order is for all the window may still buy — it takes what stands at the rule's price when it arrives, which a
ladder still being posted makes more than the book in hand showed — and a window's orders may be on their way together.

盘中吃单 (1.154.14, RULE_GOLD_SHUT_LIVE_CENTS / RULE_GOLD_OPEN_LIVE_CENTS; off by default): gold's shut quarter hours and the reopen's
first window are bought in the window's course too, at a price of their own — while the bot's own card of the market shows the
index ahead of the window's start on the rule's side (Rule.lead; live_guard): in a window's course the book's price already says
who is ahead, and a price alone would buy the side that is losing. From LIVE_START_MS after the start to LIVE_STOP_MS before the
end, each second; in the first minute no dearer than before the window; the window's shares and the day's cap are one.

The windows are set in New York time from CME's schedule (main.cme_shuts), so they follow daylight saving: gold's shut
quarter hours are 06:00–06:45 Beijing in winter and 05:00–05:45 in summer. Each rule's price is a setting (RULE_*_CENTS,
0 = the rule off), RULE_TAKER switches the whole: off, paper (只记录：what it would have bought, at the book's prices,
settled on Predict's result — no order), on (real orders, with LIVE=on and every stop the model trader has: the pause,
the day's loss stop, the operator's switches, the API's cooldown, the balance).

The per-second hours (RULE_FAST_HOURS, 1.154.12; Beijing time, 01:00–07:00 by default): in them every listed window is looked
at each second on the book the push connection holds, one window's book read over REST each second in rotation beside the
pushes, the sign-in / allowance / matching status made ready from a minute before, and the orders of several windows sent side
by side. RULE_API_KEY gives the taker a key of its own (its own budget and 429 cooldown).

Records: one a window, in the bot's store under ``rule:<window slug>`` — its orders (each one's hash saved before the
request goes out: a crash in between is reconciled by it, never sent again), the shares filled, their cost (price and taker
fee), Predict's result and the P&L. The positions check counts their fills as recorded (LiveBot.side_filled), the day's
loss stop counts their P&L (LiveBot.side_pnl), the review page lists them (复盘分析 · 规则吃单), /rules shows the state."""
from __future__ import annotations

import asyncio
import collections
import contextlib
import dataclasses
import datetime as dt
import hashlib
import inspect
import json
import logging
import os
import statistics
import sys
import time
from decimal import Decimal as D
from typing import Any, Callable

import main as core

LOG = logging.getLogger("rules")

PREFIX = "rule:"              # the records' keys in the store: rule:<window slug>
TICK_S = 1.0                  # the loop's pace
PLAN_EVERY_MS = 60_000        # the rules' windows of the next day worked out again this often (and at once after a settings change)
HORIZON_MS = 24 * 3_600_000 + 10 * 60_000  # windows followed this far ahead: Predict lists a window a day before its start
LISTED_AHEAD_MS = 24 * 3_600_000  # ...about 85 s after it enters that day (11-06): its category is looked for from then
PROBE_EARLY_MS = 120_000      # the listing is looked for from this long before it is expected
PROBE_FAST_S, PROBE_SLOW_S = 2.0, 30.0  # ...every PROBE_FAST_S until PROBE_LATE_MS past the moment, then every PROBE_SLOW_S
PROBE_LATE_MS = 5 * 60_000
NEAR_MS = 15 * 60_000         # the last stretch before the start: the status and the book read every FAST_S (matching opens then)
FAST_S, SLOW_S, CLOSED_SLOW_S = 2.0, 30.0, 60.0  # the book of an open market (fast near the start), a market not yet open for matching
OFFER_STATUS_S = 10.0         # 1.153.1: an offer at the rule's price on a market not yet open for matching: its status read this often (it buys the moment it opens)
# 1.154.22: ...and, from the moment such an offer shows, every STATUS_RUSH_S for STATUS_RUSH_MS beside the looks (rush) — ahead of the
# key's other reads, a request on the order's way. 11-18 13:15:07, the 18:15 window: 446 shares at ≤58¢ showed on a market listed
# seconds before, its matching not seen open yet; the status had just been read, so it was not read again for FAST_S, and the order
# left 2.7 s after the book that showed the offer had arrived (203 of 300 bought: nobody else was there before 3.4 s). The read is
# some 260 ms from this region: at this pace the reads follow one another, eighty of them at the most; RUSH_READS_MAX a window
STATUS_RUSH_S, STATUS_RUSH_MS, RUSH_READS_MAX = 0.25, 20_000, 240
# 1.154.74 (the user, of 11-27 07:00 黄金开盘第一档 — Thanksgiving's reopen, an offer of 301 shares at ≤73¢ in hand from 05:53, matching
# closed, the offer first taken from 20 s before the start and nothing bought: "两处都改"): an offer at the rule's price still waiting
# for matching in the start's last TAIL_RUSH_MS — the status asked every rush_pace() from then to the stop (STOP_MS before the start),
# TAIL_READS_MAX reads a window at the most, not every FAST_S; the rest of the bot keeps out of the way (quiet_ms) only in its last
# TAIL_QUIET_MS (a tail of two minutes held the model trader's steps that long, on a window that may never open before its start)
TAIL_RUSH_MS, TAIL_READS_MAX, TAIL_QUIET_MS = 120_000, 480, 30_000
# 1.154.23 — the rule taker first (the user: "每天凌晨 00:59 分开始规则吃单的优先级就要调到最高"). From WARM_MS before the per-second
# hours (00:59 for 01:00–07:00) to their end, and while a window is at the makers' moment or bought in its course outside them
# (first_now): a pushed book is looked at in the very turn of the loop that brought it, its order signed and its request handed to
# a thread before anything else gets a turn (pushed_now — it used to wait for the loop to come round to the taker: three or four
# turns, each behind whatever else was ready); its requests have threads of their own (core.first_pool); and around a makers'
# moment — QUIET_BEFORE_MS before it to QUIET_AFTER_MS after, while the window can still be bought — the rest of the bot keeps
# out of the loop's way (quiet_ms: the pre-open mode sends no new pair and applies no account snapshot, the model trader's step
# and the minute's checks wait). 11-18: offers showed 2.4–7.2 s after the quarter hour, the very seconds the pre-open mode signs
# and sends the pairs of the windows listed at that quarter hour; one race was lost by some 25 ms.
QUIET_BEFORE_MS, QUIET_AFTER_MS = 1_000, 15_000
GC_BEFORE_MS = 5_000  # ...and from this long before the moment to the end of those seconds no automatic garbage collection (gc_guard)
# 1.154.24 — the same in a window's course (the user: "这五个开盘后，盘中的优先级也是最高的" — gold's four shut quarter hours and the
# reopen's first, bought on while the index stands on the rule's side of the start). What a look in the course waits for is the
# index, not the book: Binance pushes it each second and the looks came each second too, of their own — half a second, on
# average, between the index that put the side ahead and the look that saw it. Now the index's arrival is the look (priced). And
# the rest of the bot keeps out of the way in the course's first seconds as it does at the makers' moment — the course begins
# LIVE_START_MS after a quarter hour, where the pre-open mode signs the windows just listed, and at the reopen the index has
# gapped by then — and for as long as an offer in hand waits for matching to open (rush), however late after the moment it showed
COURSE_QUIET_MS = 12_000  # the course's first seconds: from QUIET_BEFORE_MS before it begins to this long after
RUSH_QUIET_MS = 3_000     # an offer in hand, matching not open yet: the others asked to wait this long at a time, and asked again
INDEX_FRESH_MS = 1_500    # an index older than this as it comes (a socket catching up after a stall: frames of seconds ago, one after
#                           another) brings no look of its own — the next second's look judges on the newest, as before
EAGER = sys.version_info >= (3, 12)  # asyncio.Task(eager_start=True): a coroutine run at once, up to its first real wait


def start_now(coro: Any, name: str) -> "asyncio.Task":
    """A task whose coroutine runs now, in this turn of the loop, up to its first real wait (Python 3.12's eager start) — on an
    older Python a task like any other, begun at the loop's next turn."""
    loop = asyncio.get_running_loop()
    if EAGER:
        return asyncio.Task(coro, loop=loop, name=name, eager_start=True)
    return loop.create_task(coro, name=name)
STOP_MS = 15_000              # nothing placed this close to the start: the rule is about the window before it begins
EXPIRY_BEFORE_S = 5           # an order's signed expiry: this long before the start, always — a cancel that cannot get through leaves
#                               nothing matchable once the window runs (1.153.3: an order sent in the last 70 s was signed for 65 s,
#                               valid up to a minute into the window)
SEND_MIN_LEFT_MS = 5_000      # ...and none is sent with less than this left to it
# 1.154.52: Predict takes no order whose signed expiry is nearer than two minutes — HTTP 400 "expiry is too soon (minimum 2 minutes
# from now)". 11-24, the 18:15 window: an offer showed 34 s before the start, the order (signed to expire 5 s before it) was refused,
# the offer stood to the start. So was every order of a window's last two minutes — before its start, and in its course before its
# end — each a request, a refusal on the record (MAX_REJECTS) and half a minute's pause, while /rules said "到开场前 15 秒". An
# order's life is not lengthened for it (EXPIRY_BEFORE_S stands: nothing matchable once the window runs, or once it is over); none
# is decided where Predict would refuse it — nor in the five seconds before that, where one sent at once still went through (the spare):
PREDICT_MIN_LIFE_MS = 120_000  # what Predict wants left to an order's expiry...
LIFE_SPARE_MS = 5_000          # ...none is decided with less than this over it (its way to Predict; this clock and Predict's)...
LIFE_SEND_SPARE_MS = 1_000     # ...and none sent with less than this over it (before_send: an order whose way took long)
ORDER_BEFORE_MS = EXPIRY_BEFORE_S * 1000 + PREDICT_MIN_LIFE_MS + LIFE_SPARE_MS  # so: no order nearer than this (130 s) to the start — in a window's course, to its end
LIFE_WORD = "距订单到期不足 2 分钟"  # before_send's word for it
# 1.154.55 — the start's last two minutes after all (the user, 11-26: "最后 2 分钟要抢"; the 18:15 window of 11-24 again: an offer 34 s
# before the start that nobody took). An order there cannot be signed to expire before the start — Predict wants two minutes —
# so it is signed as short as Predict takes it, TAIL_LIFE_S from now: up to 110 s into the window (RULE_TAIL, on by default;
# only before the start — in a window's course its last ORDER_BEFORE_MS stay as they were: an order outliving the window would
# stand where the result is known). What the expiry before the start gave for nothing is now kept by hand: such an order asks
# for no more than the book in hand offers at the rule's price (so that it fills whole and nothing rests), its rest is
# withdrawn FILL_READ_MS on like any order's — still before the start: none is decided within STOP_MS of it — and asked again
# every TAIL_AGAIN_MS until Predict's word (not NOOP_AGAIN_MS doubling), an answer gone missing looked for as often; one not
# known off the book when its window starts is told at once (tail_alarm). What is left: a rest whose withdrawal fails for as
# long as the signature lasts can be taken at the rule's price in the window's first two minutes — the window's size at most.
TAIL_LIFE_S = (PREDICT_MIN_LIFE_MS + LIFE_SPARE_MS) // 1000  # 125
TAIL_AGAIN_MS = 1_000
FILL_READ_MS = 1_500          # an order is read this long after its answer; what has not filled then is withdrawn
LATE_FILL_MS = 60_000         # an order off the book is final only by a read this long after it left: an earlier one may not show its
#                               last matches yet (the pre-open mode's LATE_FILL_MS; 1.153.3 — the first read after the cancel ended it,
#                               a later fill was never booked and the window's cap could be passed)
REST_MIN_S = 0.1              # the loop never goes round without a wait (1.153.3)
REST_SOON_S = 0.005           # ...this short for a book that changed while the tick was running (1.154.15)
FOLLOW_EVERY_MS = 1_000       # a withdrawn order read again this often until Predict's word is final...
UNKNOWN_EVERY_MS = 10_000     # ...an order whose placement's answer went missing, looked for by its hash this often
GIVE_UP_MS = 15 * 60_000      # ...and given up as never placed this long after its signed expiry
# 1.154.33 (an outside review, R01 / R07 / CR-08): a withdrawal Predict answered "nothing to remove" (noop) while the order's read
# still shows it open is asked again this long on; one asked for with no read to go by (the read failed or found nothing, the
# taker's key cooling down) after such an answer, this long on
NOOP_AGAIN_MS, BLIND_AGAIN_MS = 5_000, 30_000
# ...doubled for each such answer in a row, up to this (the review of the fix: a record that never caught up had the order read
# every second and asked every five, inside the taker's tick, until its signed expiry — hours for one placed at the makers'
# moment). Never taken for its word: an order Predict says it cannot remove while its record shows it open keeps its window spoken for
NOOP_MAX_MS = 60_000
REJECT_PAUSE_MS = 30_000      # a window whose order Predict refused is looked at again this long on...
MAX_REJECTS = 5               # ...and left alone after this many refusals
SETTLE_AFTER_MS = 120_000     # Predict's result asked for this long after the window's end, then every SETTLE_EVERY_MS
SETTLE_EVERY_MS = 60_000
KEEP_DAYS = 90                # finished records kept this long (the review page's history)
OWN_LEVELS = 16               # the pre-open mode's price levels a window (8 at most): its records' keys looked up for its own bids (own_asks)
# 1.154.12 — the per-second hours (RULE_FAST_HOURS, Beijing time; 01:00–07:00 by default): 凌晨 1 点后每秒检查，符合规则就下单
WARM_MS = 60_000              # they are prepared for this long before they begin (the sign-in, the allowance, the windows' markets and matching status)...
WARM_EVERY_MS = 30_000        # ...and the preparation renewed this often through them
FAST_LOOK_S = 1.0             # in them every listed window is looked at this often
ALIVE_S = 25.0                # the push connection vouches for the books it holds while a frame (its heartbeat: every 15 s) is this recent
FAST_STALE_S = 20.0           # ...with the push down, a book read over REST is looked at up to this old
VERIFY_EVERY_S, VERIFY_AGAIN_S = 1.0, 3.0  # in them one REST read of a window's book at most this often, the longest unread first, none read again sooner
VERIFY_EVERY_OWN_S, VERIFY_AGAIN_OWN_S = 0.34, 1.0  # ...on a key of the rule taker's own (RULE_API_KEY): three a second
# 1.154.14 — 盘中吃单 (RULE_*_LIVE_CENTS): a rule's side bought in the window's course too, while the index is ahead of the start on its side (Rule.lead)
LIVE_START_MS = 3_000         # not in the window's first moments: the start line (the index's minute candle ending at the start) is read then
LIVE_FULL_MS = 60_000         # ...and in its first minute no dearer than before the window: the index has had no time to tell (of the 75 shut
#                               windows that ended Down, 51 were under their start within the first minute; a lead a minute in won 84%, later 98%)
LIVE_STOP_MS = 15_000         # nothing placed this close to the window's end; an order there is signed to expire EXPIRY_BEFORE_S before the end
# 1.154.15 — a window bought at once. 11-18, the 06:45 window: a ladder of offers from 50¢ up, some 300 shares within the rule's 60¢; the
# order was sized on the book in hand and cut to LIVE_MAX_ORDER_USD (166 shares), the next one waited for that one's word — 46
# shares four seconds later, the rest gone to faster takers. Now: with enough on offer the order asks for all the window may still
# buy (it takes what stands at the rule's price when it arrives — more than the book in hand showed, when a ladder is still being
# posted; what it does not take stays a bid at the rule's price until its read, FILL_READ_MS on, and is withdrawn then, as before:
# an offer posted meanwhile is bought by it); and a window's orders may be on their way together — the offers a book shows are
# first those of the orders already out, what shows beyond them is ordered at once, not after their word
SWEEP_SHARES = 20             # this much on offer at the rule's price (and a quarter of what the window may still buy: SWEEP_PART)...
SWEEP_PART = 4                # ...and the order is for all of it; less on offer is bought as the book shows it (a stray small offer:
#                               an order for the whole window would hold the window's shares for LATE_FILL_MS after it is withdrawn)
MAX_OPEN_ORDERS = 3           # a window's orders on their way together, at most
PNL_FRESH_MS = 3_000          # the day's P&L the loss stop goes by may be this old before an order: it is summed after a tick's looks
JOURNAL_SUFFIX = ".rule-orders"  # the taker's journal beside the database: a line an order, written before its request (RuleTaker.journal)
LAG_EVERY_S = 0.1             # the loop's own delay sampled this often (status: what an order waits for before the taker even looks)
# 1.154.13 — the makers' moment (RULE_LEAD_MINUTES before a window's start: someone posts five hours ahead, and the fastest taker wins)
SPRINT_MS = 15 * 60_000       # a window sprints — looked at each second, prepared — from WARM_MS before that moment to this long after it
HOT_BEFORE_MS, HOT_AFTER_MS = 10_000, 120_000  # ...and around the moment itself its book is the one read over REST, every second
VERIFY_HOT_S = 1.0
ALLOWANCE_MS = 60_000         # the exchange allowance read again this often
ALLOWANCE_UNLIMITED, ALLOWANCE_LONG_MS = 10 ** 40, 3_600_000  # ...one this large (an unlimited approval: 2^256 − 1) only this often
STOP_WORD = f"开场前 {STOP_MS // 1000} 秒内不再发单"  # (1.154.33, R18: before_send's word for an order whose way ran into STOP_MS)
RECHECKED = ("规则吃单的设置刚改过", "盘中：", STOP_WORD, LIFE_WORD)  # before_send's words for an order held back on what its look had judged (send: no pause for these)
HELD_BACK_MAX = 3             # ...up to this many in a row a window: then it pauses like a refusal (a record of them, one a look, helps nobody)
CONFLICT_GRACE_MS = 90_000    # an order Predict answered 409 for and then cannot show by its hash is given up as never placed after this long...
CONFLICT_MISSES = 6           # ...and this many reads that found nothing (an answer gone missing waits for its expiry, as before: GIVE_UP_MS)
LAST_WITHDRAW_S = 4.0         # the taker's last act as it stops: its orders still on the book asked to be withdrawn, within this (1.154.26)
DUST = 1e-6
FINAL_STATES = {"final", "rejected", "not_sent", "not_found"}
NEVER_PLACED = {"rejected", "not_sent", "not_found"}
MODES = {"off": "关", "paper": "只记录（不下单）", "on": "真实吃单"}


# --- the windows ------------------------------------------------------------------------------------------------------------
def et_parts(ms: int) -> tuple[int, int, int]:
    """(weekday — Monday 0, hour, minute) of an instant in New York, daylight saving followed (main.us_eastern_offset)."""
    at = dt.datetime.fromtimestamp(ms / 1000 + core.us_eastern_offset(ms) * 3600, dt.timezone.utc)
    return at.weekday(), at.hour, at.minute


def daily_break(start_ms: int) -> bool:
    """A quarter hour of CME's daily one-hour break: 17:00–17:45 New York, Monday to Thursday (北京冬令时周二至周五 06:00–06:45，
    夏令时 05:00–05:45). The weekend's same hour is not one: the index moves there more (2026: Up 66–70% against 90%)."""
    weekday, hour, minute = et_parts(start_ms)
    return weekday <= 3 and hour == 17 and minute % 15 == 0


def daily_reopen(start_ms: int) -> bool:
    """The first quarter hour after CME reopens at 18:00 New York, Monday to Thursday: after its daily one-hour break — and,
    1.154.71 (the user, of 11-27 07:00 北京 not bought: "节假日后的开盘也买"), after a holiday's shut too: Thanksgiving's from
    noon, a holiday Monday's. Not the weekend's (Sunday's 18:00, 北京周一早上: no lean, 2026: Down 41%). A Monday-to-Thursday
    18:00 that reopens nothing (CME shut on through it: a holiday's whole next day) is none."""
    weekday, hour, minute = et_parts(start_ms)
    if weekday > 3 or (hour, minute) != (18, 0):
        return False
    return any(s[1] == start_ms for s in core.market_shuts("cme", start_ms - 60_000, start_ms))


def ny_slot(hour: int, minute: int) -> Callable[[int], bool]:
    """The quarter hour from hour:minute New York, Monday to Friday, CME open through it (a holiday's shut and the weekend's
    left out)."""
    def matches(start_ms: int) -> bool:
        weekday, h, mi = et_parts(start_ms)
        return weekday <= 4 and (h, mi) == (hour, minute) and not core.market_shuts("cme", start_ms, start_ms + 15 * 60_000)
    matches.__name__ = f"ny_{hour:02d}{minute:02d}"
    return matches


# 05:15 New York (北京冬令时 18:15，夏令时 17:15): before the London morning fix (10:30 London) gold has leaned down (2025-12 to
# 2026-11: Down 65%, both halves of the year); the slot scan (1.153.2) found two more of gold's slots and one of oil's leaning
# the same way in both halves — 05:00 Up 62%, 16:45 (the last before the daily shut) Up 60%, oil's 00:45 Down 62% — off by default
ny_0515 = ny_slot(5, 15)


@dataclasses.dataclass(frozen=True)
class Rule:
    key: str                       # its price setting (RuleSettings, RULE_SETTING_KEYS) and the records' "rule"
    market: str                    # "gold" / "oil": the switch and the size it goes by (RULE_GOLD / RULE_OIL, RULE_*_SHARES; 1.153.1)
    card: str                      # the card whose windows it buys (WINDOW_UPDOWN_MARKETS)
    side: str                      # "up" / "down"
    name: str                      # 黄金休市 4 档
    when: str                      # its windows, in New York and Beijing time
    matches: Callable[[int], bool]
    lead: D = D(0)                 # 盘中 (1.154.14, with RULE_*_LIVE_CENTS set): bought in the window's course too while the index is ahead of the
    #                                window's start on the rule's side by this much (relative); 0 — only before the window. XAUUSDT's index,
    #                                1-minute candles. The shut quarter hours, 0.005%: the index stood still through CME's break until 2026-05
    #                                (every window a tie) and has moved in every one since — 2026-06..11, 380 windows, Up 84%: Up ahead so from
    #                                minute 2 ended Up 98% (October 80–92%), merely not under the start 93%. The reopen's first, 0.02%: 192
    #                                windows, Down 58% — Down ahead so ended Down 89–91% at minutes 1–4 (the first half-year 70–85%), 94–99%
    #                                from minute 5; Up ahead: Down 38% falling to 20%

    @property
    def word(self) -> str:
        return "涨" if self.side == "up" else "跌"


RULES: tuple[Rule, ...] = (
    Rule("gold_shut", "gold", "XAU-15M", "up", "黄金休市 4 档", "纽约 17:00–17:45，周一至周四（北京冬令时 06:00–06:45、夏令时 05:00–05:45）", daily_break, D("0.00005")),
    Rule("gold_open", "gold", "XAU-15M", "down", "黄金开盘第一档", "纽约 18:00，周一至周四（北京冬令时 07:00、夏令时 06:00；周一早上那场不做，节假日停盘后的那一场也做）", daily_reopen, D("0.0002")),
    Rule("gold_0515", "gold", "XAU-15M", "down", "黄金纽约 05:15", "纽约 05:15，周一至周五（北京冬令时 18:15、夏令时 17:15）", ny_0515),
    Rule("gold_0500", "gold", "XAU-15M", "up", "黄金纽约 05:00", "纽约 05:00，周一至周五（北京冬令时 18:00、夏令时 17:00）", ny_slot(5, 0)),
    Rule("gold_1645", "gold", "XAU-15M", "up", "黄金休市前一档", "纽约 16:45，周一至周五（北京冬令时 05:45、夏令时 04:45）", ny_slot(16, 45)),
    Rule("oil_shut", "oil", "CL-15M", "down", "原油休市 4 档", "纽约 17:00–17:45，周一至周四（北京冬令时 06:00–06:45、夏令时 05:00–05:45）", daily_break),
    Rule("oil_0045", "oil", "CL-15M", "down", "原油纽约 00:45", "纽约 00:45，周一至周五（北京冬令时 13:45、夏令时 12:45）", ny_slot(0, 45)),
    Rule("oil_open", "oil", "CL-15M", "down", "原油开盘第一档", "纽约 18:00，周一至周四（北京冬令时 07:00、夏令时 06:00；周一早上那场不做，节假日停盘后的那一场也做）", daily_reopen),
)
RULE_OF = {r.key: r for r in RULES}
MARKET_NAMES = {"gold": "黄金", "oil": "原油"}


def live_rule(settings: Any, rule: Rule) -> bool:
    """Whether a rule trades now by the settings: its market switched on and a price set."""
    return settings.on(rule.market) and settings.price(rule.key) > 0


def card_of(rule: Rule) -> Any:
    return next(c for c in core.WINDOW_UPDOWN_MARKETS if c.key == rule.card)


def windows(rule: Rule, from_ms: int, to_ms: int) -> list[int]:
    """The starts of the rule's windows in [from_ms, to_ms)."""
    card = card_of(rule)
    out, start = [], card.first_start(from_ms)
    while start < to_ms:
        if rule.matches(start):
            out.append(start)
        start = card.next_start(start)
    return out


def money(x: float) -> str:
    return ("+$" if x >= 0 else "−$") + f"{abs(x):,.2f}"


def bj(ms: int, day: bool = True) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, core.BEIJING).strftime("%m-%d %H:%M" if day else "%H:%M")


# --- the book -----------------------------------------------------------------------------------------------------------------
def offered(bids: tuple, asks: tuple, side: str, limit: D) -> list[tuple[D, D]]:
    """What a buy of ``side`` at ``limit`` or under takes, the cheapest first: (price, shares). Predict's book is quoted for 涨
    (Up): its asks are the Up shares for sale; a bid for Up at p is Down for sale at 1 − p (the two buys mint a pair)."""
    if side == "up":
        return [(p, q) for p, q in sorted(asks) if p <= limit]
    return [(1 - p, q) for p, q in sorted(bids, reverse=True) if 1 - p <= limit]


# 1.154.28 — the user: "对手是预挂的" (11-19 01:00: 598 shares at ≤60¢ gone in one piece 0.35 s after they showed, nothing of them to
# an order that left 2 ms after their book came). Predict takes an order in and shows it, and matches it a moment later — the
# pre-open mode meets this as the deferred refusal of a post-only order that would cross (preopen.REFUSED_STATUSES) — so a buy
# resting at the makers' prices before they come is matched with them before any order sent once they show, however soon. The
# watch keeps the bids of the rule's side (bidding): the first sight of one within RIVAL_BELOW of the rule's price, the book
# before the offers showed and the one that showed them, and the first after they were gone with none of this taker's orders
# resting — who rested there, since when, and what of it was left (what it took). Recorded only: nothing is bought on it
RIVAL_BELOW = D("0.10")  # a bid of the rule's side this far under its price, or dearer, is one that takes the makers' offers
RIVAL_LEVELS = 5         # ...the dearest bids of the rule's side kept at each of the race's moments
BIDS_SETTLE_S = 0.5      # ...and the book after the race: one taken this long after none of this taker's orders was seen resting
#                          (a frame on its way as the last of them left still shows it)


def bidding(bids: tuple, asks: tuple, side: str, n: int = RIVAL_LEVELS) -> list[list[float]]:
    """The book's ``n`` dearest bids for ``side``, the dearest first, as [price, shares]: Up's bids, or — for Down — Up's offers,
    a bid for Down at p being Up for sale at 1 − p (offered's other half)."""
    if side == "up":
        return [[float(p), float(q)] for p, q in sorted(bids, reverse=True)[:n]]
    return [[float(1 - p), float(q)] for p, q in sorted(asks)[:n]]


def minus(bids: list[list[float]], own: dict) -> list[list[float]]:
    """``bids`` (bidding's) less the pre-open mode's own resting bids among them (RuleTaker.own_bids: price -> shares)."""
    if not own:
        return bids
    out = []
    for price, shares in bids:
        left = round(shares - float(own.get(D(str(price)), 0)), 4)
        if left > DUST:
            out.append([price, left])
    return out


def without(levels: list[tuple[D, D]], shares: D) -> list[tuple[D, D]]:
    """``levels`` (the cheapest first) less ``shares`` taken from the cheapest."""
    out, left = [], shares
    for price, size in levels:
        took = min(size, left)
        left -= took
        if size > took:
            out.append((price, size - took))
    return out


def walk(levels: list[tuple[D, D]], shares: D) -> D:
    """The average price of ``shares`` bought from ``levels`` (cheapest first)."""
    left, cost = shares, D(0)
    for price, size in levels:
        took = min(size, left)
        cost += price * took
        left -= took
        if left <= 0:
            break
    return cost / (shares - left) if shares > left else D(0)


# --- the records ----------------------------------------------------------------------------------------------------------------
def rid(slug: str, mode: str) -> str:
    """A record's id (its store key after PREFIX): the window's slug for a real one, ``paper:<slug>`` for a paper one — one record
    a window and a mode (1.153.3: a switch between paper and on in a window's life put real orders on the paper record and paper
    fills on the real one)."""
    return slug if mode == "on" else f"{mode}:{slug}"


def records(store: Any) -> dict[str, dict]:
    return {k.removeprefix(PREFIX): v for k, v in store.items(PREFIX) if isinstance(v, dict)}


def sorted_out(recs: dict[str, dict]) -> tuple[dict[str, dict], list[str]]:
    """The records each under its own id (rid), a record mixing paper and real orders split in two (1.153.3); the old ids no
    longer used."""
    out: dict[str, dict] = {}
    for rec in recs.values():
        orders = rec.get("orders") or []
        for mode, mine in (("on", [o for o in orders if not o.get("paper")]), ("paper", [o for o in orders if o.get("paper")])):
            if not mine and rec.get("mode") != mode:
                continue
            key = rid(str(rec.get("slug") or ""), mode)
            if key in out:
                out[key]["orders"].extend(mine)
            else:
                out[key] = {**rec, "mode": mode, "orders": list(mine)}
                if rec.get("mode") != mode:
                    out[key].pop("seen", None)
                    out[key].update(pnl=None, result="")  # settled again on its own orders
    return out, [k for k in recs if k not in out]


def records_differ(a: dict, b: dict) -> bool:
    return a != b


def fee_share(avg: float, bps: int) -> float:
    """The part of a taker's shares Predict keeps as its fee: the fee (rate × min(p, 1 − p) a share) is taken in shares, not
    USDT (README 1.119.1 / 1.150.0: 97.74 bought, 96.30 held)."""
    return bps / 10_000 * min(avg, 1 - avg) / avg if avg > 0 else 0.0


def order_avg(o: dict) -> float:
    return float(o.get("avg") or o.get("limit") or 0)


def order_bps(o: dict) -> int:
    return int(o.get("fee_bps") if o.get("fee_bps") is not None else 200)


def filled(rec: dict) -> float:
    return sum(float(o.get("filled") or 0) for o in rec.get("orders") or [])


def cost(rec: dict) -> float:
    """What the fills cost in USDT: each order's shares at its average price (its limit while Predict states none). The fee is paid
    in shares (net), not here (1.153.3: it was added to the price as well)."""
    return sum(float(o.get("filled") or 0) * order_avg(o) for o in rec.get("orders") or [])


def net(rec: dict) -> float:
    """The shares the account holds of the fills: those bought less the fee Predict takes in shares."""
    return sum(float(o.get("filled") or 0) * (1 - fee_share(order_avg(o), order_bps(o))) for o in rec.get("orders") or [])


def avg_price(rec: dict) -> float:
    """The fills' average price, the fee apart (its limit for an order whose record states no price)."""
    shares = filled(rec)
    return sum(float(o.get("filled") or 0) * float(o.get("avg") or o.get("limit") or 0) for o in rec.get("orders") or []) / shares if shares > DUST else 0.0


def in_flight(rec: dict) -> bool:
    return any(o.get("state") not in FINAL_STATES for o in rec.get("orders") or [])


def held(rec: dict) -> float:
    """The window's shares bought or being bought: the fills, and the whole of an order whose word is not final."""
    return sum(float(o.get("shares") or 0) if o.get("state") not in FINAL_STATES else float(o.get("filled") or 0) for o in rec.get("orders") or [])


# A window's watch (RuleTaker.watch): the lowest offer of the rule's side at any price, the most on offer at the rule's price at once,
# when that first showed, when the window was listed and matching first seen open; 1.154.13: Predict's own stamps of the offer's
# coming and going; 1.154.16: when the amount on offer first went down — someone began to take it (the fastest taker's speed: the
# offers of a ladder are gone only when its last step is)
WATCH = {"best": None, "offer_max": 0.0, "offer_ms": 0, "listed_ms": 0, "open_ms": 0, "offer_ts": 0, "gone_ms": 0, "gone_ts": 0,
         "cut_ms": 0, "cut_ts": 0,
         # 1.154.28: the bids of the rule's side (bidding) before the offers showed, in the book that showed them, and in the first
         # after they were gone with none of this taker's orders resting; the window's first look; the first sight of a bid within
         # RIVAL_BELOW of the rule's price (when, Predict's stamp of that book, its price and shares; whether at the first look)
         "bids_before": None, "bids_at": None, "bids_after": None, "looked_ms": 0,
         "rival_ms": 0, "rival_ts": 0, "rival_px": None, "rival_sz": None, "rival_first": False}
OPEN_STATES = {"placing", "open", "unknown"}  # an order on its way or on the book: it may still take what the book offers
REST_STATES = {"open", "cancelling", "unknown"}  # ...one whose rest may stand on the book, to be withdrawn by its id (withdraw_rest)
PENDING_STATES = OPEN_STATES | {"cancelling"}   # ...one that may still fill or whose fills are not final yet (pending_usd)


def open_orders(rec: dict) -> list[dict]:
    """The window's orders that may still take from the book: on their way, or on it and not withdrawn."""
    return [o for o in rec.get("orders") or [] if o.get("state") in OPEN_STATES and not o.get("removed")]


def claims(rec: dict) -> float:
    """The shares those orders may still take (1.154.15): asked for and not known filled. The offers a book shows are theirs first —
    another order is sized on what is offered beyond them. (An order withdrawn takes nothing more; held counts it whole until its
    final word.)"""
    return sum(max(float(o.get("shares") or 0) - float(o.get("filled") or 0), 0.0) for o in open_orders(rec))


def day_of(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, core.BEIJING).date().isoformat()


def filled_by_outcome(recs: dict[str, dict], now_ms: int, days: int = 30) -> dict[tuple[str, int], float]:
    """The real orders' fills by (market id, index set), the windows of the last ``days`` days: shares the positions check
    counts as recorded (LiveBot.side_filled), never adopts onto an order of the model trader's."""
    out: dict[tuple[str, int], float] = {}
    for rec in recs.values():
        if rec.get("mode") != "on" or int(rec.get("start_ms") or 0) < now_ms - days * core.DAY_MS:
            continue
        shares = net(rec)  # what the account holds: the fee is taken in shares (1.153.3)
        if shares > DUST:
            k = (str(rec.get("market_id") or ""), int(rec.get("index_set") or 0))
            out[k] = out.get(k, 0.0) + shares
    return out


def unverified(recs: dict[str, dict]) -> set[tuple[str, int]]:
    """The outcomes where a real order's fills are not final yet: a position there may be its own (LiveBot.side_unverified)."""
    return {(str(r.get("market_id") or ""), int(r.get("index_set") or 0)) for r in recs.values() if r.get("mode") == "on" and in_flight(r)}


def spent_on(recs: dict[str, dict], day: str, mode: str) -> float:
    """What the orders placed on a Beijing day cost in USDT, the mode's own records (paper's and the real ones apart): the fills at
    their price, and the whole of an order whose word is not final at its limit (RULE_MAX_DAY_USD)."""
    total = 0.0
    for rec in recs.values():
        if rec.get("mode") != mode:
            continue
        for o in rec.get("orders") or []:
            if day_of(int(o.get("at") or 0)) != day:
                continue
            if o.get("state") in FINAL_STATES:
                total += float(o.get("filled") or 0) * order_avg(o)
            else:
                total += float(o.get("shares") or 0) * float(o.get("limit") or 0)
    return total


def known_orders(recs: dict[str, dict], now_ms: int) -> tuple[set[str], set[str]]:
    """The real orders' ids and hashes of the last two days: the bot's checks of the account's open orders take them as its own
    (1.153.3: a rule order caught on the list was announced as one nobody here placed)."""
    ids, hashes, since = set(), set(), now_ms - 2 * core.DAY_MS
    for rec in recs.values():
        if rec.get("mode") != "on" or int(rec.get("start_ms") or 0) < since:
            continue
        for o in rec.get("orders") or []:
            if o.get("id"):
                ids.add(str(o["id"]))
            for key in ("hash", "api_hash"):  # (1.154.33, R07: Predict's own hash too, where it gave one of its own — RuleTaker.send)
                if o.get(key):
                    hashes.add(str(o[key]).lower())
    return ids, hashes


def day_pnl(recs: dict[str, dict], day: str) -> float:
    """The real records' P&L settled on a Beijing day: counted with the model trader's against LIVE_MAX_DAILY_LOSS_USD."""
    return sum(float(r.get("pnl") or 0) for r in recs.values() if r.get("mode") == "on" and r.get("settled_day") == day)


def watch_stats(recs: dict[str, dict], rule: str, mode: str, since_ms: int) -> dict:
    """What the watches of a rule's windows saw since ``since_ms`` (RuleTaker.watch, 1.153.2): the windows watched, those that
    offered the rule's side at its price, the shares offered at once on average there, the median lowest offer of the side, the
    median lead of matching's opening over the start (seconds)."""
    # (1.154.16: of the windows whose watch is over — one still open is on its record already, save_watch, with what it has seen so
    # far: counted here it would be a window that offered among those that have not yet been counted as watched; races shows it)
    seen = [r["seen"] for r in recs.values() if r.get("rule") == rule and r.get("mode") == mode and isinstance(r.get("seen"), dict)
            and int(r["seen"].get("start_ms") or 0) >= since_ms and not r["seen"].get("open")]
    offered = [x for x in seen if float(x.get("offer_max") or 0) > 0]
    mid = lambda xs: statistics.median(xs) if xs else None
    # 1.154.13 — the race: how long before the start the first offer at the price showed (the makers' lead), and, of the offers
    # that were gone with nothing of them bought here, how long they stood — by Predict's own stamps of the two books where both
    # were pushed, else by the looks' times: the speed of whoever took them
    lost = [x for x in offered if x.get("gone_ms") and float(x.get("bought") or 0) <= DUST]
    won = [x for x in offered if x.get("gone_ts") and x.get("offer_ts") and float(x.get("bought") or 0) > DUST]  # (by Predict's stamps alone: this taker's own speed as the book saw it)
    stood = lambda x: (int(x["gone_ts"]) - int(x["offer_ts"])) if x.get("gone_ts") and x.get("offer_ts") else int(x["gone_ms"]) - int(x["offer_ms"])
    return {"watched": len(seen), "offered": len(offered),
            "offer_avg": round(sum(float(x["offer_max"]) for x in offered) / len(offered), 2) if offered else None,
            "best_mid": mid([float(x["best"]) for x in seen if x.get("best") is not None]),
            "open_lead": mid([(int(x["start_ms"]) - int(x["open_ms"])) / 1000 for x in seen if x.get("open_ms")]),
            "offer_lead": mid([(int(x["start_ms"]) - int(x["offer_ms"])) / 60_000 for x in offered if x.get("offer_ms")]),
            "lost": len(lost), "life": mid([max(0, stood(x)) for x in lost]),
            "won": len(won), "won_life": mid([max(0, stood(x)) for x in won])}


RACES_SHOWN = 8  # /rules lists this many of the latest windows' races


RACE_SAME_MS = 5_000  # an order sized on a book this long after the window's offers first showed answers a later showing of them (order_clock)


def order_clock(o: dict, shown: int) -> tuple[int | None, int | None]:
    """An order against the clock of its window's race (races): (how long after the offer showed it left, None) — or, when the
    book it was sized on came RACE_SAME_MS or more after the window's offers FIRST showed, (how long after that book — Predict's
    stamp on it — it left, how long after the first showing that was). 1.154.52: a window's offers may show, be taken and show
    again hours later (11-24 07:00: listed a day ahead, an order 19 hours on) — counted from the first showing that order "left
    68404348 ms after the offer", in /rules and in the month's median of this taker's speed."""
    sent = int(o.get("sent_wall") or 0)
    since = sent - shown if sent and shown else None
    react = sent - int(o["book_ts"]) if sent and o.get("book_ts") else None
    if since is not None and react is not None and since - react >= RACE_SAME_MS:
        return react, since
    return since, None


def span_text(ms: int) -> str:
    """19.0 小时 / 12.5 分钟 / 8.3 秒."""
    return f"{ms / 3_600_000:.1f} 小时" if ms >= 3_600_000 else f"{ms / 60_000:.1f} 分钟" if ms >= 60_000 else f"{ms / 1000:.1f} 秒"


def races(recs: dict[str, dict], mode: str, since_ms: int) -> list[dict]:
    """The windows' races since ``since_ms``, the latest start first (1.154.16: /rules and the review page, a line a window): each
    window whose watch saw an offer at the rule's price or that an order went out for before its start — when the offer showed
    (Predict's stamp of that book, else the look's time) and how long before the start, the most on offer, how long until it was
    first taken from and until none was left (by Predict's stamps of the books where both carried one, else by the looks'
    times: the speed of whoever took it), what this taker asked for and bought, and each order against the clock — how long
    after the offer showed it left (``after``), the push's lag, the taker's own time, the request's round trip."""
    rows = []
    for r in recs.values():
        if r.get("mode") != mode or int(r.get("start_ms") or 0) < since_ms:
            continue
        seen = r["seen"] if isinstance(r.get("seen"), dict) else {}
        orders = [o for o in r.get("orders") or [] if not o.get("live")]  # those placed before the window: its course is not a race
        if not seen.get("offer_ms") and not orders:
            continue
        shown = int(seen.get("offer_ts") or 0)

        def gap(kind: str) -> int | None:
            if not seen.get(kind + "_ms"):
                return None
            if seen.get(kind + "_ts") and shown:
                return max(0, int(seen[kind + "_ts"]) - shown)
            return max(0, int(seen[kind + "_ms"]) - int(seen["offer_ms"]))
        rule = RULE_OF.get(r.get("rule"))
        rows.append({
            "slug": r.get("slug"), "rule": r.get("rule"), "name": rule.name if rule else str(r.get("rule")), "window": r.get("name"),
            "start_ms": int(r.get("start_ms") or 0), "cents": int(seen.get("cents") or r.get("cents") or 0),
            "offer_at": shown or int(seen.get("offer_ms") or 0), "stamped": bool(shown),
            "lead_min": round((int(r.get("start_ms") or 0) - int(seen["offer_ms"])) / 60_000, 1) if seen.get("offer_ms") else None,
            "peak": float(seen.get("offer_max") or 0), "cut_ms": gap("cut"), "gone_ms": gap("gone"), "open": bool(seen.get("open")),
            # 1.154.74: when this taker first saw matching open (its status read), and when it first saw the offer — on its own clock
            "open_seen": int(seen.get("open_ms") or 0), "offer_seen": int(seen.get("offer_ms") or 0),
            # 1.154.28: the bids of the rule's side (clock_bids), and the first sight of one within RIVAL_BELOW of the rule's price
            "word": rule.word if rule else "", "floor_c": max(0, int(seen.get("cents") or r.get("cents") or 0) - int(RIVAL_BELOW * 100)),
            "bids_before": seen.get("bids_before"), "bids_at": seen.get("bids_at"), "bids_after": seen.get("bids_after"),
            "rival": ({"px": float(seen["rival_px"]), "sz": float(seen.get("rival_sz") or 0), "at": int(seen.get("rival_ts") or seen["rival_ms"]),
                       "first": bool(seen.get("rival_first")), "looked": int(seen.get("looked_ms") or 0)}
                      if seen.get("rival_ms") and seen.get("rival_px") is not None else None),
            "asked": round(sum(float(o.get("shares") or 0) for o in orders if o.get("state") not in NEVER_PLACED), 4),
            "bought": round(sum(float(o.get("filled") or 0) for o in orders), 4),
            "orders": [{"shares": float(o.get("shares") or 0), "filled": float(o.get("filled") or 0), "paper": bool(o.get("paper")),
                        "seen": o.get("seen"), "fresh": o.get("fresh"), "state": o.get("state"),
                        "error": str(o.get("error") or "") if o.get("state") in NEVER_PLACED else "",  # (1.154.18: never placed — said so, not "bought nothing")
                        "after": order_clock(o, shown)[0], "later": order_clock(o, shown)[1],  # (1.154.52: of a later showing — order_clock)
                        "lag": int(o["seen_wall"]) - int(o["book_ts"]) if o.get("book_ts") and o.get("seen_wall") else None,
                        "own": int(o["sent_wall"]) - int(o["seen_wall"]) if o.get("sent_wall") and o.get("seen_wall") else None,
                        # 1.154.22: the taker's own time in its parts — the book's arrival to the look that placed the order, that
                        # look's beginning to the order signed (its reads: matching's status, the sign-in, the allowance), the order
                        # signed to its request leaving; and the wait for matching to open, when the offer was there first
                        # (a book the look read itself arrived after the look began: no wait for the look, the rest counted from the book)
                        "wake": int(o["look_wall"]) - int(o["seen_wall"]) if o.get("look_wall") and o.get("seen_wall") and int(o["look_wall"]) >= int(o["seen_wall"]) else None,
                        "prep": int(o["signed_wall"]) - max(int(o["look_wall"]), int(o.get("seen_wall") or 0)) if o.get("signed_wall") and o.get("look_wall") else None,
                        "out": int(o["sent_wall"]) - int(o["signed_wall"]) if o.get("sent_wall") and o.get("signed_wall") else None,
                        "open_wait": o.get("open_wait"), "open_reads": o.get("open_reads"),
                        "trip": int(o["answered_wall"]) - int(o["sent_wall"]) if o.get("answered_wall") and o.get("sent_wall") else None}
                       for o in orders]})
    return sorted(rows, key=lambda x: -x["start_ms"])


def race_text(row: dict) -> str:
    """A window's race in a line (/rules)."""
    sec = lambda ms: f"{ms / 1000:.2f} 秒"
    clock = lambda ms: dt.datetime.fromtimestamp(ms / 1000, core.BEIJING).strftime("%H:%M:%S") + f".{ms % 1000:03d}"  # (1.154.17: to the ms — how regular the makers are)
    text = f"{bj(row['start_ms'])} {row['name']}："
    if row["offer_at"]:
        text += (f"≤{row['cents']}¢ 的卖单 {clock(row['offer_at'])} 出现" + (f"（开场前 {row['lead_min']:g} 分钟）" if row["lead_min"] is not None else "")
                 + f"，最多 {row['peak']:g} 份；"
                 # (1.154.18: a watch of before 1.154.16 knows when the offers were gone, not when they were first taken from — it read
                 # "还没有被吃、0.42 秒后被吃光")
                 + ("、".join(([f"{sec(row['cut_ms'])}后开始被吃"] if row["cut_ms"] is not None else [])
                             + ([f"{sec(row['gone_ms'])}后被吃光"] if row["gone_ms"] is not None else ["还没吃光" if row["open"] else "到开场前没吃光"] if row["cut_ms"] is not None else []))
                    or ("还没有被吃" if row["open"] else "到开场前没人吃")))
    else:
        text += "卖单出现的时刻没有记到（重启前的，或没看到规则价以内的卖单）"
    text += matching_text(row, clock)
    text += bids_text(row)
    orders = row["orders"]
    if not orders:
        return text + "；没下单" + ("（这一场还在看）" if row["open"] else "")
    text += "；" + ("只记录 " if all(o["paper"] for o in orders) else "") + (
        f"买到 {row['bought']:g}/{row['asked']:g} 份" if row["bought"] > DUST else f"下了 {row['asked']:g} 份，没买到" if row["asked"] > DUST else "单没下成")
    for i, o in enumerate(orders, 1):
        if o["paper"]:
            continue
        if o.get("state") in NEVER_PLACED:
            text += f"｜第 {i} 张 要 {o['shares']:g}：没下成" + (f"（{o['error']}）" if o.get("error") else "")
            continue
        parts = (([f"出现后 {o['after']} ms 发出" + (f"（这是卖单第一次出现 {span_text(o['later'])}后又有的卖单，从它出现算）" if o.get("later") else "")]
                  if o["after"] is not None else []) + ([f"推送 {o['lag']} ms"] if o["lag"] is not None else []))
        if o["own"] is not None:  # (an order that waited for something else than its book — the one before it — is told apart)
            split = [f"{name} {o[key]} ms" for name, key in (("到达→开始处理", "wake"), ("处理→签好", "prep"), ("签好→发出", "out")) if o.get(key) is not None]
            if o.get("open_wait") is not None:
                split.append(f"卖单先到、等撮合开启 {o['open_wait']} ms（读了 {o.get('open_reads') or 0} 次状态）")
            parts.append(f"到达→发出 {o['own']} ms" + (f"［{'、'.join(split)}］" if split else "") + ("（不是盘口一到就下的）" if o["fresh"] is False else ""))
        if o["trip"] is not None:
            parts.append(f"往返 {o['trip']} ms")
        text += (f"｜第 {i} 张 要 {o['shares']:g} 成交 {o['filled']:g}" + (f"（当时盘口上 {o['seen']:g} 份）" if o["seen"] else "")
                 + ("：" + "、".join(parts) if parts else ""))
    return text


def matching_text(row: dict, clock: Callable[[int], str]) -> str:
    """1.154.74 (the user, of 11-27 07:00: "两处都改"): a window whose offer waited for matching — when this taker first saw it open
    (its own status read: Predict may have opened it a little before), or that it had not by the stop (STOP_MS before the start,
    the last moment an order goes out before it). "" where matching was open before the offer showed (most windows: hours
    before), and while the window is still watched."""
    opened, shown, start = int(row.get("open_seen") or 0), int(row.get("offer_seen") or 0), int(row.get("start_ms") or 0)
    if not shown or row.get("open"):
        return ""
    if opened > shown:
        when = f"开场前 {span_text(start - opened)}" if opened < start else f"开场后 {span_text(opened - start)}"
        return f"；卖单先到，撮合 {clock(opened)} 才看到开启（{when}，卖单出现后 {span_text(opened - shown)}）"
    if not opened:
        return f"；卖单一直在等撮合：到开场前 {STOP_MS // 1000} 秒（停止下单）还没看到撮合开启"
    return ""


def bids_text(row: dict) -> str:
    """The bids of the rule's side in a window's race (1.154.28, clock_bids): who rested within RIVAL_BELOW of the rule's price before
    the offers showed (a buy resting there is matched with them before any order sent once they show), what of it was left once they
    were gone — what it took — and since when the first of them was there. "" for a race watched before 1.154.28."""
    word, floor = row.get("word") or "", int(row.get("floor_c") or 0)
    near = lambda xs: [(p, q) for p, q in xs or [] if p * 100 >= floor - 1e-6]
    shown = lambda xs: "、".join(f"{p * 100:g}¢×{q:g}" for p, q in xs)
    parts = []
    if row.get("bids_at") is not None:
        before = near(row["bids_before"] if row.get("bids_before") is not None else row["bids_at"])
        parts.append(f"卖单出现前{word}这边 ≥{floor}¢ 的买单 {shown(before)}" if before else f"卖单出现前{word}这边没有 ≥{floor}¢ 的买单")
        if row.get("bids_after") is not None and before:
            after = dict(near(row["bids_after"]))
            took = round(sum(max(0.0, q - after.get(p, 0.0)) for p, q in before), 4)
            peak = float(row.get("peak") or 0)
            parts.append(f"卖单没了以后剩 {shown(near(row['bids_after'])) or '0 份'}" + (
                f"（少了 {took:g} 份，卖单最多 {peak:g} 份：多半是被这些预挂的买单接走的）" if took > DUST and took >= 0.5 * peak else
                f"（少了 {took:g} 份）" if took > DUST else "（一份没少：卖单不是被它们接走的，是撤掉或被拒了）"))
    rival = row.get("rival")
    if rival:
        when = dt.datetime.fromtimestamp(rival["at"] / 1000, core.BEIJING).strftime("%m-%d %H:%M:%S")
        parts.append(f"{rival['px'] * 100:g}¢×{rival['sz']:g} 的{word}买单 {when} 第一次看到"
                     + (f"（{bj(rival['looked'])} 开始看这一场时就已经在了）" if rival["first"] and rival.get("looked") else ""))
    return "".join("；" + part for part in parts)


def race_summary(rows: list[dict]) -> str:
    """The races in a line (1.154.17): how regular the makers' moment is — the offers' showing, by Predict's own stamps, as seconds
    after the quarter hour: its median and its range (regular to a tenth of a second, an order can be timed to it instead of
    waiting for the push) — how soon the first taker was on them, and how soon after their showing this taker's orders left."""
    mid = lambda xs: statistics.median(xs)
    marks = [(r["offer_at"] % 900_000) / 1000 for r in rows if r.get("stamped")]
    cuts = [r["cut_ms"] / 1000 for r in rows if r.get("cut_ms") is not None and r.get("stamped")]
    ours = [o["after"] for r in rows for o in r["orders"] if o.get("after") is not None and o.get("fresh") is not False]
    parts = []
    if marks:
        parts.append(f"卖单出现在整刻钟后 中位 {mid(marks):.2f} 秒（{min(marks):.2f}～{max(marks):.2f}，{len(marks)} 场）")
    if cuts:
        parts.append(f"最快的人在出现后 中位 {mid(cuts):.2f} 秒下手（最快 {min(cuts):.2f}）")
    if ours:
        parts.append(f"我们的单在出现后 中位 {mid(ours):.0f} ms 发出（最快 {min(ours):.0f}），再加半个往返才到")
    # 1.154.28: the races where a buy rested within RIVAL_BELOW of the rule's price before the offers showed (clock_bids)
    known = [r for r in rows if r.get("bids_at") is not None]
    if known:
        ahead = sum(1 for r in known if any(p * 100 >= int(r.get("floor_c") or 0) - 1e-6
                                            for p, _ in (r["bids_before"] if r.get("bids_before") is not None else r["bids_at"]) or []))
        parts.append(f"卖单出现前已有人预挂买单（规则价以下 {int(RIVAL_BELOW * 100)}¢ 以内或更高）的 {ahead}/{len(known)} 场")
    return "；".join(parts)


def course_part(rec: dict) -> tuple[float, float, float | None]:
    """A record's fills bought in the window's course (盘中, 1.154.14): (shares, what they cost, their P&L — None until the window is
    settled), reckoned as the record's own (net, cost: the fee taken in shares)."""
    orders = [o for o in rec.get("orders") or [] if o.get("live")]
    shares = sum(float(o.get("filled") or 0) for o in orders)
    spent = sum(float(o.get("filled") or 0) * order_avg(o) for o in orders)
    up = {"up": 1.0, "down": 0.0, "split": 0.5}.get(str(rec.get("result") or ""))
    if rec.get("pnl") is None or up is None:
        return shares, spent, None
    kept = sum(float(o.get("filled") or 0) * (1 - fee_share(order_avg(o), order_bps(o))) for o in orders)
    return shares, spent, kept * (up if rec.get("side") == "up" else 1 - up) - spent


def course_stats(recs: dict[str, dict], rule: str, mode: str, since_ms: int = 0) -> dict:
    """What a rule bought in its windows' course since ``since_ms``: the windows, the shares, their cost, those settled and how."""
    rows = [course_part(r) for r in recs.values() if r.get("rule") == rule and r.get("mode") == mode and int(r.get("start_ms") or 0) >= since_ms]
    rows = [x for x in rows if x[0] > DUST]
    done = [x[2] for x in rows if x[2] is not None]
    return {"windows": len(rows), "shares": round(sum(x[0] for x in rows), 4), "cost": round(sum(x[1] for x in rows), 4), "settled": len(done),
            "wins": sum(1 for x in done if x > DUST), "losses": sum(1 for x in done if x < -DUST), "pnl": round(sum(done), 4)}


def live_price_of(settings: Any, rule: Rule) -> int:
    """The rule's price in its window's course by the settings (RULE_*_LIVE_CENTS), 0 = none: no such price, its market off, a rule
    with no way to judge a window's course (Rule.lead)."""
    if not rule.lead > 0 or not settings.on(rule.market):
        return 0
    return int(getattr(settings, "live_price", lambda key: 0)(rule.key))


def lead_note(rule: Rule) -> str:
    """The course's condition in a few words: 指数比起点高 0.005% 以上才买."""
    return f"指数比起点{'高' if rule.side == 'up' else '低'} {float(rule.lead) * 100:g}% 以上才买"


def speed_stats(recs: dict[str, dict], since_ms: int) -> dict | None:
    """The real orders against the clock since ``since_ms`` (1.154.13), None for none: how many went out and how many filled
    anything; in ms, the medians — and the fastest (``best``, 1.154.15) — of the push's lag (Predict's stamp on the book the order
    was sized on to its arrival), the taker's own time (that arrival to the request leaving; of it ``wake``, the arrival to the
    look that placed the order beginning) and the request's round trip. The taker's own time is counted over the orders whose
    look answered a book that had just come (``fresh``, 1.154.15: an order that waited for something else — the one before it
    to end, the index — says nothing of speed; 11-18: 2,098 ms the median of a first order and the bite four seconds after it);
    over all of them while none carries the mark (the orders of before)."""
    rows = [o for r in recs.values() if r.get("mode") == "on" and int(r.get("start_ms") or 0) >= since_ms
            for o in r.get("orders") or [] if o.get("sent_wall")]
    if not rows:
        return None
    marked = any("fresh" in o for o in rows)
    quick = [o for o in rows if o.get("fresh")] if marked else rows
    mid = lambda xs: statistics.median(xs) if xs else None
    lag = [int(o["seen_wall"]) - int(o["book_ts"]) for o in rows if o.get("book_ts") and o.get("seen_wall")]
    own = [int(o["sent_wall"]) - int(o["seen_wall"]) for o in quick if o.get("seen_wall")]
    wake = [int(o["look_wall"]) - int(o["seen_wall"]) for o in quick if o.get("seen_wall") and o.get("look_wall")]
    trip = [int(o["answered_wall"]) - int(o["sent_wall"]) for o in rows if o.get("answered_wall")]
    return {"orders": len(rows), "filled": sum(1 for o in rows if float(o.get("filled") or 0) > DUST),
            "lag": mid(lag), "own": mid(own), "trip": mid(trip), "wake": mid(wake), "timed": len(own),
            "best": {"lag": min(lag, default=None), "own": min(own, default=None), "trip": min(trip, default=None)}}


def course_speed(recs: dict[str, dict], since_ms: int) -> dict | None:
    """The real orders of the windows' course that the index brought, against the clock since ``since_ms`` (1.154.24), None for
    none: how many, and in ms the median and the least of the index's arrival to the request leaving (RuleTaker.priced: the
    index's arrival is the look; it used to wait for the taker's own next second)."""
    took = [int(o["sent_wall"]) - int(o["index_wall"]) for r in recs.values() if r.get("mode") == "on" and int(r.get("start_ms") or 0) >= since_ms
            for o in r.get("orders") or [] if o.get("live") and o.get("index_wall") and o.get("sent_wall")]
    return {"orders": len(took), "own": statistics.median(took), "best": min(took)} if took else None


def review(recs: dict[str, dict], settings: Any, now_ms: int) -> dict | None:
    """The review page's 规则吃单: each rule's totals (windows bought, shares, cost, settled P&L, wins and losses, open), the
    paper and the real apart, and the last records; None when nothing is recorded and the mode is off."""
    if not recs and settings.mode == "off":
        return None
    rows = []
    for rule in RULES:
        for mode in ("on", "paper"):
            mine = [r for r in recs.values() if r.get("rule") == rule.key and r.get("mode") == mode and filled(r) > DUST]
            stats = watch_stats(recs, rule.key, mode, now_ms - 30 * core.DAY_MS)
            if not mine and not stats["watched"] and (mode == "paper" or not (live_rule(settings, rule) or live_price_of(settings, rule))):
                continue
            done = [r for r in mine if r.get("pnl") is not None]
            rows.append({"rule": rule.key, "name": rule.name, "side": rule.side, "cents": settings.price(rule.key) if live_rule(settings, rule) else 0, "when": rule.when,
                         "live_cents": live_price_of(settings, rule), "course": course_stats(recs, rule.key, mode),  # 1.154.14: 盘中
                         "mode": mode, "windows": len(mine), "shares": round(sum(filled(r) for r in mine), 4),
                         "cost": round(sum(cost(r) for r in mine), 4), "settled": len(done),
                         "wins": sum(1 for r in done if float(r["pnl"]) > DUST), "losses": sum(1 for r in done if float(r["pnl"]) < -DUST),
                         "pnl": round(sum(float(r["pnl"]) for r in done), 4), "open": len(mine) - len(done), "watch": stats})
    last = sorted((r for r in recs.values() if filled(r) > DUST), key=lambda r: -int(r.get("start_ms") or 0))[:60]
    return {"mode": settings.mode, "markets": {m: {"on": settings.on(m), "shares": settings.shares_of(m), "name": n} for m, n in MARKET_NAMES.items()},
            "max_day_usd": settings.max_day_usd, "rules": rows, "fast": core.day_spans_text(getattr(settings, "fast", ())),
            "lead_minutes": int(getattr(settings, "lead_minutes", 0) or 0), "speed": speed_stats(recs, now_ms - 30 * core.DAY_MS),
            "races": races(recs, settings.mode, now_ms - 30 * core.DAY_MS)[:60] if settings.mode != "off" else [],  # 1.154.16: each window's race
            "recent": [{"slug": r.get("slug"), "rule": r.get("rule"), "name": (RULE_OF.get(r.get("rule")) or RULES[0]).name,
                        "window": r.get("name"), "start_ms": r.get("start_ms"), "side": r.get("side"), "mode": r.get("mode"),
                        "shares": round(filled(r), 4), "avg": round(avg_price(r), 4), "cost": round(cost(r), 4),
                        "live": round(course_part(r)[0], 4),  # of them, bought in the window's course
                        "result": r.get("result") or "", "pnl": r.get("pnl")} for r in last]}


# --- the pushed books ------------------------------------------------------------------------------------------------------------
class RuleStream:
    """Predict's WebSocket for the rule taker's own windows (1.153.1): each listed window's book pushed the moment it changes
    (predictOrderbook/<market id>; main.PredictStream's protocol: subscribe / unsubscribe by request, the heartbeat echoed), so an
    offer at a rule's price is bought at once — not at the next read. A connection of its own beside the model trader's (whose
    subscriptions follow the cards on its page). While it is down, or a book has not come lately, the taker reads it over REST."""
    FRESH_S = 20.0               # a pushed book stands for this long (the server sends a book again every second or so)
    SILENT_S = 45.0              # no frame for this long (three heartbeats): the connection is made anew
    BACKOFF = (2.0, 120.0)       # the wait before a new connection, doubled per failure
    SYNC_S = 30.0                # the subscriptions brought in step at least this often (at once when the wants change)
    RESUBSCRIBE_S = 2.0          # a subscription the server refused asked for again this soon (resubscribe, 1.154.33)

    def __init__(self, taker: "RuleTaker", connect: Any = None):
        self.taker = taker
        self.connect = connect or core.ws_connect
        self.wanted: set[str] = set()        # market ids to have pushed
        self.subscribed: set[str] = set()
        self.books: dict[str, tuple] = {}    # market id -> (bids, asks, monotonic time of the push)
        self.versions: dict[str, int] = {}
        self.changed: set[str] = set()       # markets whose book changed since the taker last looked
        self.arrivals = 0                    # ...and how many changes have come in all (the loop goes round at once for one that came mid-tick: rest)
        self.pushed = asyncio.Event()        # ...set as one comes: the taker's loop wakes at once
        self.on_change: Any = None           # ...and told right then (1.154.23: RuleTaker.pushed_now — looked at in this very turn when the taker goes first; True: all taken)
        self.wake = asyncio.Event()
        self.pending: dict[int, tuple[str, str]] = {}
        self.refused: dict[str, float] = {}  # market id -> monotonic time of the server's last refusal of its subscription (resubscribe)
        self.request_id = 0
        self.ws: Any = None
        self.connects = 0
        self.frames = 0
        self.last_frame = 0.0                # monotonic time of the last frame of any kind (the heartbeat included): the connection lives
        self.stamps: dict[str, tuple[int, int]] = {}  # market id -> (the server's stamp on the book in hand — 0 for a REST read —, its arrival, wall ms)
        self.lags: collections.deque = collections.deque(maxlen=300)  # ms from the server's stamp on a pushed change to its arrival (1.154.13)
        self.error = ""
        self.backoff = self.BACKOFF[0]

    def want(self, ids: set[str]) -> None:
        if ids != self.wanted:
            self.wanted = set(ids)
            self.wake.set()

    def book(self, mid: str) -> tuple | None:
        """(bids, asks) as last pushed while the connection lives, None when there is none fresh (the REST read stands in)."""
        got = self.books.get(mid)
        if got is None or self.ws is None or time.monotonic() - got[2] > self.FRESH_S:
            return None
        return got[0], got[1]

    def alive(self) -> bool:
        """The connection lives: a frame — a book, or the heartbeat every 15 s — within ALIVE_S. While it does, the book it holds of a
        subscribed market is the market's (a quiet one's is not sent again: 11-17, 75 topics and 1.2 re-sends a second in all)."""
        return self.ws is not None and time.monotonic() - self.last_frame <= ALIVE_S

    def vouches(self, mid: str) -> bool:
        """Whether the book held of this market stands for it however old (alive): the connection lives and the market is one of its
        subscriptions. 1.154.33 (an outside review, CR-04): of any market it held, before — one whose subscription the server had
        refused is pushed nothing, and what it held of it was a REST read's (merge), looked at as current for as long as the
        connection lived (the REST reads beside the pushes go to one window alone around its makers' moment: two minutes)."""
        return self.alive() and mid in self.subscribed

    def held(self, mid: str) -> tuple | None:
        """(bids, asks, when) as last pushed — or read over REST beside the pushes (merge) — however long ago; None for none."""
        return self.books.get(mid)

    def nudge(self, *mids: str) -> None:
        """The taker looks at these markets now though their books have not changed (1.154.22: matching just seen open with an
        offer in hand; 1.154.24: the index a window's course is judged by just moved): marked as a change that has just come —
        the loop goes round at once for one, mid-tick too (rest_s: REST_SOON_S)."""
        self.changed.update(mids)
        self.arrivals += 1
        self.told()

    def told(self) -> None:
        """A change just noted: the taker told of it on the spot (on_change, 1.154.23: it may take it in this very turn), and
        its loop woken unless that took everything there was to take."""
        if not (self.on_change is not None and self.on_change()):
            self.pushed.set()

    def merge(self, mid: str, bids: tuple, asks: tuple, asked: float) -> bool:
        """A REST read of a window's book beside the pushes (RuleTaker.verify, the per-second hours): dropped when a push came after
        the read was asked for (the push is the newer); else it is the book in hand, and the taker is woken when it differs from
        the one before. True when it differed from a book held — a change no push brought."""
        old = self.books.get(mid)
        if old is not None and old[2] > asked:
            return False
        self.books[mid] = (bids, asks, time.monotonic())
        if old is None or (bids, asks) != (old[0], old[1]):
            self.stamps[mid] = (0, int(time.time() * 1000))  # a REST read carries no stamp of the book's own
            self.changed.add(mid)
            self.arrivals += 1
            self.told()
            return old is not None
        return False

    def stopped(self) -> bool:
        return self.taker.stop.is_set() or self.taker.owner.stopping.is_set()

    async def pause(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.taker.stop.wait(), seconds)

    def headers(self) -> dict[str, str]:
        key = getattr(self.taker, "key", "") or getattr(self.taker.owner.config, "predict_api_key", "")  # the taker's own key when it has one (1.154.12)
        return {"x-api-key": key} if key else {}

    async def run(self) -> None:
        while not self.stopped():
            if not self.wanted:
                self.wake.clear()
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self.wake.wait(), 5.0)
                continue
            try:
                ws = await self.connect(core.PREDICT_WS_URL, self.headers())
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.error = core.clean_error(error) or type(error).__name__
                await self.pause(self.backoff)
                self.backoff = min(self.backoff * 2, self.BACKOFF[1])
                continue
            since = self.last_frame = time.monotonic()
            self.ws, self.connects = ws, self.connects + 1
            self.subscribed.clear(); self.pending.clear(); self.versions.clear(); self.books.clear(); self.changed.clear(); self.stamps.clear()
            why = ""
            try:
                why = await self.session(ws)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                why = core.clean_error(error) or type(error).__name__
            finally:
                self.ws = None
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(ws.close(), 1.0)
            if self.stopped():
                return
            self.error = why or "连接结束"
            self.backoff = self.BACKOFF[0] if time.monotonic() - since >= 60 else min(self.backoff * 2, self.BACKOFF[1])
            await self.pause(self.backoff)

    async def session(self, ws: Any) -> str:
        tasks = {asyncio.create_task(self.read(ws)), asyncio.create_task(self.sync_loop(ws)), asyncio.create_task(self.taker.stop.wait())}
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            task = next(iter(done))
            if self.stopped():
                return ""
            error = task.exception()
            return (core.clean_error(error) or type(error).__name__) if error else (task.result() or "连接结束")
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def read(self, ws: Any) -> str:
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=self.SILENT_S)
            except asyncio.TimeoutError:
                return f"{self.SILENT_S:.0f} 秒没有收到任何帧"
            self.frames += 1
            self.last_frame = time.monotonic()
            await self.handle(ws, raw)

    async def sync_loop(self, ws: Any) -> str:
        while True:
            self.wake.clear()
            # 1.154.76 (an outside audit, P1-12): the marks set before each request goes out, not after — the frames are read beside
            # this, and an answer read while the send awaited found no mark: the first book pushed was dropped (take), and a refusal
            # discarded nothing, the market then marked subscribed for the connection's life (never asked again, vouches() true on
            # a REST book). Let go before subscribing anew. (A connection's end clears the marks: nothing is left over)
            for mid in sorted(self.subscribed - self.wanted):
                self.subscribed.discard(mid)
                self.books.pop(mid, None)
                self.versions.pop(mid, None)
                self.stamps.pop(mid, None)
                self.changed.discard(mid)
                await self.request(ws, "unsubscribe", mid)
            for mid in sorted(self.wanted - self.subscribed):
                self.subscribed.add(mid)
                await self.request(ws, "subscribe", mid)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self.wake.wait(), self.SYNC_S)

    def resubscribe(self, mid: str) -> None:
        """A subscription the server refused, asked for again RESUBSCRIBE_S on (sync_loop woken) — the first refusal of the market in
        SYNC_S; refused again within it, at the sync's own pace (no request a round trip, however long it keeps refusing)."""
        now = time.monotonic()
        last, self.refused[mid] = self.refused.get(mid, -1e9), now
        if now - last >= self.SYNC_S:
            with contextlib.suppress(RuntimeError):  # (no loop running: nothing to wake)
                asyncio.get_running_loop().call_later(self.RESUBSCRIBE_S, self.wake.set)

    async def request(self, ws: Any, method: str, mid: str) -> None:
        self.request_id += 1
        self.pending[self.request_id] = (method, mid)
        await ws.send(json.dumps({"method": method, "requestId": self.request_id, "params": [core.PREDICT_WS_TOPIC + mid]}))

    async def handle(self, ws: Any, raw: Any) -> None:
        try:
            msg = json.loads(raw)
        except (TypeError, ValueError):
            return
        if not isinstance(msg, dict):
            return
        if msg.get("type") == "R":
            with contextlib.suppress(TypeError, ValueError):
                method, mid = self.pending.pop(int(msg.get("requestId")), ("", ""))
                if mid and (msg.get("success") is False or (msg.get("success") is None and msg.get("error"))):
                    if method == "subscribe":
                        self.subscribed.discard(mid)  # asked for again at the next sync
                        # 1.154.33 (an outside review, CR-04): nothing of it is pushed on this connection — what is held of it (a
                        # REST read's) is dropped, and it is asked for again in a moment rather than at the next sync (SYNC_S)
                        self.books.pop(mid, None)
                        self.versions.pop(mid, None)
                        self.stamps.pop(mid, None)
                        self.resubscribe(mid)
                    self.error = f"{method} {mid}：{msg.get('error') or '拒绝'}"
        elif msg.get("type") == "M":
            topic = str(msg.get("topic") or "")
            if topic == core.PREDICT_WS_HEARTBEAT:
                await ws.send(json.dumps({"method": "heartbeat", "data": msg.get("data")}))
            elif topic.startswith(core.PREDICT_WS_TOPIC):
                self.take(topic[len(core.PREDICT_WS_TOPIC):], msg.get("data"))

    def take(self, mid: str, data: Any) -> None:
        """A pushed book: kept (an older version dropped, a side left out keeps its own), and the taker woken when it changed."""
        if mid not in self.subscribed or not isinstance(data, dict):
            return
        version = data.get("version")
        if isinstance(version, (int, float)) and not isinstance(version, bool) and version == version and abs(version) != float("inf"):
            # (1.154.76, P1-12: not a NaN or an infinity — JSON lets them through, int() raised on them)
            if version < self.versions.get(mid, -1):
                return
            self.versions[mid] = int(version)
        old = self.books.get(mid)
        bids = core.predict_levels(data.get("bids"), True) if isinstance(data.get("bids"), list) else (old[0] if old else ())
        asks = core.predict_levels(data.get("asks"), False) if isinstance(data.get("asks"), list) else (old[1] if old else ())
        self.books[mid] = (bids, asks, time.monotonic())
        if old is None or (bids, asks) != (old[0], old[1]):
            stamp, wall = data.get("updateTimestampMs"), int(time.time() * 1000)
            stamp = int(stamp) if isinstance(stamp, (int, float)) and not isinstance(stamp, bool) and 0 < stamp <= wall + 60_000 else 0
            self.stamps[mid] = (stamp, wall)  # when Predict says this book came to be, and when it reached us (the race's clock, 1.154.13)
            if stamp and old is not None:
                self.lags.append(wall - stamp)
            self.changed.add(mid)
            self.arrivals += 1
            self.told()


# --- the taker --------------------------------------------------------------------------------------------------------------------
class RuleTaker:
    """Runs beside the model trader and the pre-open mode (LiveBot.rules_start): one loop that works out the rules' windows of
    the next day, finds each one's market as Predict lists it, watches its matching status and book before the start, buys
    what the rule allows, follows its orders to Predict's final word and settles each window on Predict's result. With
    RULE_TAKER off it only finishes what its records still hold (orders in flight, results to read)."""

    def __init__(self, owner: Any):
        import live
        import preopen
        self.owner = owner
        self.settings = owner.config.rules
        # 1.154.12: RULE_API_KEY — a key of the rule taker's own: its reads, its orders and its push connection on their own budget
        # (PredictPacer, one a key) and their own cooldown; without one, the bot's key and one cooldown with the bot's own client (a
        # 429 on either pauses both — 11-17: ninety of them in 2.3 hours, the taker blind for twenty seconds each)
        own = str(getattr(owner.config, "rule_api_key", "") or "")
        self.key = own or owner.config.predict_api_key
        self.own_key = bool(own) and own != owner.config.predict_api_key
        shared = None if self.own_key else getattr(owner.live, "api", None)
        self.api = preopen.safe_api(live.API_BASES[owner.config.live_chain], self.key, owner.live.wallet, self.before_send, shared)
        self.api.pool = core.first_pool()  # 1.154.23: its requests on threads of their own — never in the shared pool's queue
        self.fast = False                  # in the per-second hours now (RULE_FAST_HOURS; pace)
        self.warm_ms = 0                   # when their preparation last began (warm)
        self.warmed = False                # ...whether it was made at all since the start (pace: once, as soon as a market is known)
        self.warming: asyncio.Task | None = None
        self.verifying: asyncio.Task | None = None  # the REST read of a window's book under way beside the pushes (verify)
        self.verify_at = 0.0               # monotonic: when the last one began
        self.verifies = self.verify_diffs = self.fast_looks = 0  # REST reads beside the pushes, those that differed, the per-second looks (status)
        self.status_reads = 0              # ...and the reads of matching's status at the makers' moment (verify_status, 1.154.22)
        self.rush_tasks: set = set()       # the windows' rushes under way (rush): they share the key's pace, and end with the taker
        self.tick_lock = asyncio.Lock()    # one tick at a time: the loop's own, or one begun by a pushed book (pushed_now, 1.154.23)
        self.ticking = 0                   # ...the ticks running or waiting for their turn
        self.running = False               # the loop runs (run): only then is a pushed book looked at on the spot
        self.first_ticks = 0               # ticks begun by a pushed book in the turn that brought it (status)
        self.first_tasks: set = set()      # ...those still waiting for something (an order's answer): ended with the taker
        self.index_looks = 0               # looks at a window in its course that the index's arrival brought (priced, 1.154.24)
        self.index_late = 0                # ...and the indexes that came too old for one (INDEX_FRESH_MS: a clock apart from Binance's, a stalled socket)
        self.first_tick = False            # the tick under way was begun by a pushed book or an index (tick_first): what can wait is left to the loop's own
        self.course_cards: frozenset = frozenset()  # ...the cards whose index such a window hangs on (plan): every other price the bot tells of is none of the taker's
        self.quiet_held = (0.0, 0)         # (monotonic, ms) of quiet_ms as last worked out (asked by every placement of the pre-open mode)
        self.gc_off = False                # the garbage collector held off by this taker now (gc_guard)
        self.verify_error = ""
        self.verify_ms: collections.deque = collections.deque(maxlen=120)  # the last REST reads' round trips, ms
        self.collect: list | None = None   # the orders' requests of the tick under way: sent side by side, awaited at its end (send)
        self.ticks, self.in_tick = 0, False  # the loop's ticks so far, one under way...
        self.gated: tuple[int, str] = (-1, "")  # ...and block(0.0) as the tick under way found it (gate)
        self.pnl_at: tuple[int, float] = (0, 0.0)  # the day's P&L of the loss stop and when it was summed (day_pnl_now: after a tick's looks)
        self.pnl_of: tuple = ()            # ...and what that sum is of (pnl_mark): summed afresh when it is no longer
        self.looked = 0                    # the stream's count of changes as the last tick's looks began (rest)
        self.journal_fd: int | None = None  # the orders' journal, open for appending (journal); None: none (a store in memory)
        self.journal_cut = False           # ...its last line was cut short and may not be ended: the next one begins on a new line
        self.loop_lags: collections.deque = collections.deque(maxlen=int(60 / LAG_EVERY_S))  # the loop's delay over the last minute, ms (lag_loop)
        self.track: dict[str, dict] = {}   # window slug -> what is known of it (its rule, start, market, when last looked at, the last word)
        self.planned_ms = 0
        self.sending: dict[str, str] = {}  # hash -> slug while its request is out (before_send's lookup)
        self.decided: dict[str, Any] = {}  # ...and the settings its look decided it under (before_send: not sent once they changed, 1.154.26)
        self.allowance: dict[str, tuple[int, int]] = {}
        self.errors: collections.deque = collections.deque(maxlen=20)
        self.error = ""
        self.stop = asyncio.Event()
        self.alive = True
        self.reads = 0
        self.recs: dict[str, dict] = {}  # the records by id (rid), kept in memory: only this taker writes them
        recs, gone = sorted_out(records(owner.store))
        for key, rec in recs.items():
            if records_differ(rec, (owner.store.get(PREFIX + key) or {})):
                owner.store.put(PREFIX + key, rec)
        if gone:
            owner.store.delete_keys([PREFIX + k for k in gone])
        self.recs = recs
        self.recover()  # 1.154.15: the orders' journal — one the store never got is looked for by its hash
        self.stream = RuleStream(self)  # the listed windows' books pushed as they change (1.153.1)

    # -- plumbing --
    def now(self) -> int:
        return int(self.owner.market.now_ms())  # whole ms: the slugs are built from it

    @property
    def store(self) -> Any:
        return self.owner.store

    def record(self, slug: str, mode: str | None = None) -> dict | None:
        """The window's record of the mode (the mode in force by default)."""
        return self.recs.get(rid(slug, mode or self.settings.mode))

    def save(self, rec: dict) -> None:
        key = rid(rec["slug"], rec["mode"])
        self.recs[key] = rec
        self.store.put(PREFIX + key, rec)

    def known(self) -> tuple[set[str], set[str]]:
        return known_orders(self.recs, self.now())

    def note(self, text: str) -> None:
        self.error = text
        if text:
            self.errors.append((self.now(), text))
            LOG.warning("规则吃单：%s", text)

    def notify(self, text: str, key: str = "") -> None:
        send = getattr(self.owner, "live_notify", None)
        if callable(send):
            with contextlib.suppress(Exception):
                send(text, key=key)

    def cooling(self) -> float:
        return getattr(self.api, "blocked_until", 0.0) - time.monotonic()

    def configure(self, settings: Any) -> None:
        """The control page's values: the next tick plans with them (a rule switched off stops at once: its windows are dropped)."""
        self.settings = settings
        self.planned_ms = 0

    # -- the gates --
    def block(self, usd: float, net: bool = False) -> str:
        """Why no real order goes out now ("" to send): the mode, real trading off or paused, the day's loss stop, the account,
        the API's cooldown, the balance. ``net`` (1.154.33, an outside review, CR-03): the balance less what this taker's orders
        still out may take (pending_usd) — for an order being decided (place); not at its bytes (before_send: orders of one turn
        go out side by side, and each would count the others there)."""
        owner, c = self.owner, self.owner.config
        if self.settings.mode != "on":
            return f"规则吃单是“{MODES.get(self.settings.mode, self.settings.mode)}”，不下单"
        if c.live_mode != "on":
            return f"LIVE={c.live_mode}，规则吃单不下单"
        if not getattr(c, "live_allowed", True):
            return "部署变量 LIVE_TRADING_ALLOWED=off，不下单"
        if paused := owner.live_paused():
            return f"真实交易已暂停（{paused}）"
        if killed := owner.live_killed():
            return f"今日停止开新仓：{killed}"
        cap = c.live_max_daily_loss_usd
        if cap:
            pnl = self.day_pnl_now()
            floor = owner.live_loss_floor(cap) if hasattr(owner, "live_loss_floor") else -cap
            if pnl <= floor:
                return f"今日已结算亏损 {money(pnl)} 已达 LIVE_MAX_DAILY_LOSS_USD ${cap:g}"
        if not owner.live.ready:
            return f"真实交易未就绪：{getattr(owner.live, 'not_ready_why', '')}"
        if (left := self.cooling()) > 0:
            return f"Predict 接口限流冷却中（{int(left) + 1} 秒后重试）"
        usd_left = owner.live.usd() if callable(getattr(owner.live, "usd", None)) else None
        if usd_left is not None and usd_left < usd:
            return f"USDT 余额 ${usd_left:,.2f} 不足本单 ${usd:,.2f}"
        if usd_left is not None and net and usd > 0 and (pending := self.pending_usd()) > DUST and usd_left - pending < usd:
            return f"USDT 余额 ${usd_left:,.2f}，在途的规则单还要 ${pending:,.2f}，剩下的不足本单 ${usd:,.2f}"
        return ""

    def pending_usd(self) -> float:
        """USDT this taker's real orders may still take (1.154.33, an outside review, CR-03): each one on its way, on the book, of
        unknown answer or withdrawn unconfirmed — its unfilled shares at its limit; those of the turn under way too (place records
        an order before its request leaves). The balance (LiveTrader.usd: the chain's, read every minute) knows nothing of them —
        Predict holds no money for an order — so the windows of one turn were each sized on all of it (two orders of $29 on $30)."""
        now = self.now()
        # (the review of the fix: not an order past its signed expiry — it fills nothing more — nor one whose answer went missing and
        # whose hash finds nothing past CONFLICT_GRACE_MS, what a 409's is given: held at its full price until GIVE_UP_MS past its
        # expiry, it cut every other window's size for hours. Its own window stays spoken for whatever: held)
        # 1.154.76 (an outside audit, P1-7): ...and one Predict removed whose last read is not in yet (cancelling, final by a read
        # LATE_FILL_MS on): a fill matched just before the removal shows in a later read only, its USDT gone meanwhile — it was let
        # go at the removal, and an order of the same turn sized on that money
        out = sum(max(float(o.get("shares") or 0) - float(o.get("filled") or 0), 0.0) * float(o.get("limit") or 0)
                  for rec in self.recs.values() if rec.get("mode") == "on"
                  for o in rec.get("orders") or () if o.get("state") in PENDING_STATES and not o.get("paper")
                  and int(o.get("expires") or 0) * 1000 > now
                  and not (o.get("state") == "unknown" and not o.get("id") and now - int(o.get("at") or now) > CONFLICT_GRACE_MS))
        # 1.154.72 (an outside audit, P1-3): ...and what they spent since the balance was read — the chain's balance, read every
        # minute, still holds a fill's USDT until its next read, and the filled part no longer counted above: $30 read, $29
        # bought in one window, and the next window's order sized on the $30 again
        balance = getattr(self.owner.live, "balance", None)
        read_ms = float(balance[2]) * 1000 if isinstance(balance, tuple) and len(balance) > 2 and isinstance(balance[2], (int, float)) else 0.0
        if read_ms > 0:
            out += sum(float(o.get("filled") or 0) * float(o.get("avg") or o.get("limit") or 0)
                       for rec in self.recs.values() if rec.get("mode") == "on"
                       for o in rec.get("orders") or () if not o.get("paper") and int(o.get("fill_wall") or 0) > read_ms)
        return out

    def before_send(self, body: dict) -> str:
        """Judged right before an order's bytes go out (preopen.safe_api): the gates again, and the window's start not passed."""
        digest = str(body.get("data", {}).get("order", {}).get("hash") or "").lower()
        slug = self.sending.get(digest)
        if slug is None:
            return "不是规则吃单这一刻要发的订单"
        # 1.154.26 (audit 4): between an order's look and its bytes there may be waits — the sign-in, the allowance's read, a token
        # of the key's pace, a slot (11-18: 2.5 s once) — and the gates below knew nothing of what the look itself went by: the
        # rule's own settings (its switch, its price, the window's size, the day's cap) and, in a window's course, the index.
        # Judged again here: the settings the look decided under still the ones in force, the index still on the rule's side,
        # the bot not stopping. Refused, nothing went out (NotSent); the next look decides afresh
        if self.stop.is_set() or getattr(getattr(self.owner, "stopping", None), "is_set", bool)():
            return "机器人正在停止，不再发单"
        decided = self.decided.get(digest)
        if decided is not None and decided != self.settings:
            return "规则吃单的设置刚改过，这张单不发了（下一轮按新设置重新判断）"
        rec = self.record(slug, "on") or {}  # (a real order is on the real record, whatever the mode is by now)
        order = next((o for o in rec.get("orders") or [] if o.get("hash") == digest), None)
        if order is None:
            return "订单记录不见了"
        if int(order.get("expires") or 0) * 1000 - self.now() < PREDICT_MIN_LIFE_MS + LIFE_SEND_SPARE_MS:
            # 1.154.52: Predict would refuse it ("expiry is too soon"): decided with ORDER_BEFORE_MS in hand (look), its way — the
            # sign-in, the allowance's read, a token of the key's pace — took the spare. Held back as the look's own judgment
            # (RECHECKED: no refusal, no pause); the two checks below stand behind this one
            return (LIFE_WORD + ("（Predict 不收），不再发送（这张单签的是它允许的最短有效期，路上耽搁掉了余量）" if order.get("tail") else
                                 f"（Predict 不收；开场前的单签到开场前 {EXPIRY_BEFORE_S} 秒到期，盘中的单签到这一场结束前 {EXPIRY_BEFORE_S} 秒），不再发送"
                                 "（这张单在那之前决定，发出时已到这段时间）"))
        if int(order.get("expires") or 0) * 1000 - self.now() < SEND_MIN_LEFT_MS:
            return "距订单到期不足 5 秒（开场前 5 秒到期；盘中的单在这一场结束前 5 秒到期），不再发送"
        # 1.154.33 (an outside review, R18): nothing is placed within STOP_MS of the start (in a window's course, LIVE_STOP_MS of its
        # end) — held at the look alone (phase): an order decided 16 s before the start whose way took 4 (the sign-in, the allowance's
        # read, a token of the key's pace) still left at 12. Held back here as the look's own judgment (RECHECKED: not a refusal)
        live_order = bool(order.get("live"))
        edge = int(rec.get("end_ms") or 0) - LIVE_STOP_MS if live_order else int(rec.get("start_ms") or 0) - STOP_MS
        if edge > 0 and self.now() >= edge:
            return ((f"盘中：这一场结束前 {LIVE_STOP_MS // 1000} 秒内不再发单" if live_order else STOP_WORD)
                    + "（这张单在那之前决定，发出时已到这段时间）")
        if why := self.block(float(order.get("shares") or 0) * float(order.get("limit") or 0)):
            return why
        if order.get("live") and (rule := RULE_OF.get(rec.get("rule"))) is not None:
            try:
                why = self.live_guard(slug, rule, self.now())
            except Exception as error:  # (said as a refusal: raised from here it would read as a request whose answer went missing)
                why = f"发单前复核指数出错（{type(error).__name__}）"
            if why:
                return f"盘中：{why}"
        return ""

    def today_pnl(self) -> float:
        return day_pnl(self.recs, day_of(self.now()))

    def day_pnl_now(self) -> float:
        """The day's settled P&L the loss stop goes by — the model trader's, the pre-open mode's, the rules' own. A sum over the
        model trader's and the pre-open mode's records (12,800 and 39,000 on 11-17): 1.154.13 summed it once a tick; 1.154.15
        after the tick's looks (turn), so an order's own checks — the tick's gate, the order's, the one right before its bytes go
        out — find it summed: up to PNL_FRESH_MS old, summed here only when older."""
        now = self.now()
        if self.pnl_at[0] and 0 <= now - self.pnl_at[0] <= PNL_FRESH_MS and self.pnl_of[:1] == (day_of(now),):
            return self.pnl_at[1]  # (of today: yesterday's sum is not the new day's figure, however young — 1.154.26)
        return self.sum_pnl(now)

    def pnl_mark(self, now: int) -> tuple:
        """What the summed figure is of (1.154.26, the audit of 11-18, 15): the Beijing day, and the model trader's count of
        settlements (LiveBot.sim_settle bumps it). The sum was made again a second after the last one, whatever had been
        settled in between — and a window this taker settled itself (settle runs after the tick's sum) was not in the figure
        the next tick's orders were judged by. Summed again as soon as either moved, and right after a settlement of its own
        — at a tick's end both, never on an order's way (turn, settle: the sum walks the day's records of all three)."""
        return day_of(now), getattr(self.owner, "__dict__", {}).get("live_pnl_epoch", 0)

    def sum_pnl(self, now: int) -> float:
        owner = self.owner
        pnl = owner.live_daily_pnl(owner.sim_trades(), now) + float(getattr(owner, "preopen_pnl", lambda: 0.0)()) + self.today_pnl()
        self.pnl_at, self.pnl_of = (now, pnl), self.pnl_mark(now)
        return pnl

    # -- the loop --
    async def run(self) -> None:
        streaming = asyncio.create_task(self.stream.run(), name="rules-stream")
        lagging = asyncio.create_task(self.lag_loop(), name="rules-lag")
        self.stream.on_change = self.pushed_now
        self.running = True
        try:
            while not self.stop.is_set() and not self.owner.stopping.is_set():
                try:
                    await self.tick()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self.note(core.clean_error(error) or type(error).__name__)
                await self.rest()
            self.running = False  # (no tick begun by a pushed book from here on)
            # 1.154.23: a tick a pushed book began may still wait for its order's answer — it ends first, as the loop's own
            # tick would have (the bot allows the taker ten seconds: LiveBot.rules_stop); cancelled only with the taker itself
            async with self.tick_lock:
                pass
            # 1.154.26: an order answered in the loop's last moments would rest unread until the next start (a deploy: a minute
            # or two; its signed expiry at the worst) — its rest is asked to be withdrawn before the process goes. A few
            # seconds at the most, and never in the way of the stop
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.withdraw_all("机器人停止"), LAST_WITHDRAW_S)
        finally:
            self.alive = self.running = False
            if self.gc_off:  # (never left off)
                self.gc_off = False
                core.GC_WATCH.release()
            side = [t for t in (streaming, lagging, self.warming, self.verifying, *self.rush_tasks, *self.first_tasks) if t is not None]
            for task in side:
                task.cancel()
            try:
                await asyncio.gather(*side, return_exceptions=True)
            finally:
                self.close_journal()

    def close_journal(self) -> None:
        """1.154.33 (an outside review, CR-11): the orders' journal closed as the loop ends (its last withdrawal done, nothing more is
        sent: a send after it would wait for the store, as one with no journal does) — it was left open to the process's end."""
        fd, self.journal_fd = self.journal_fd, None
        if fd is not None:
            with contextlib.suppress(OSError):
                os.close(fd)

    async def lag_loop(self) -> None:
        """How late the loop wakes (1.154.15, /rules): a sleep of LAG_EVERY_S, and by how much it overran — the time a pushed book
        waits, with everything else the bot does on the same loop, before the taker even looks at it."""
        while True:
            began = time.monotonic()
            await asyncio.sleep(LAG_EVERY_S)
            self.loop_lags.append(max(0.0, (time.monotonic() - began - LAG_EVERY_S) * 1000))

    def rest_s(self) -> float:
        """How long the loop rests at the most: a tick; REST_MIN_S with a change in hand that the tick's looks left untaken;
        REST_SOON_S with one that came after they began."""
        return TICK_S if not self.stream.changed else REST_SOON_S if self.stream.arrivals != self.looked else REST_MIN_S

    async def rest(self) -> None:
        """Until the next tick: TICK_S, or less — a pushed book that changed wakes the loop at once (an offer bought within
        the moment it shows). 1.154.15: so does one that came while the tick was running (its looks over, an order's answer
        awaited): REST_SOON_S, where it used to be REST_MIN_S — the wait that keeps the loop from going round on a change no
        look takes (1.153.3), which a change that has just come is not."""
        self.stream.pushed.clear()
        waits = {asyncio.ensure_future(self.stop.wait()), asyncio.ensure_future(self.stream.pushed.wait())}
        try:  # 1.153.3: always a wait, however short — a change no look took used to send the loop round without one, the bot frozen
            await asyncio.wait(waits, timeout=self.rest_s(), return_when=asyncio.FIRST_COMPLETED)
        finally:
            for w in waits:
                w.cancel()
            await asyncio.gather(*waits, return_exceptions=True)

    async def tick(self) -> None:
        self.ticking += 1
        try:
            async with self.tick_lock:  # (1.154.23: the loop's tick and one a pushed book began never run side by side)
                self.ticks += 1
                self.in_tick = True
                try:
                    await self.turn()
                finally:
                    self.in_tick = False
        finally:
            self.ticking -= 1

    def first_now(self, now: int | None = None) -> bool:
        """Whether the rule taker goes first now (1.154.23): real orders, and the per-second hours run or begin within WARM_MS
        (00:59 for hours from 01:00), or a window is at the makers' moment or bought in its course (self.fast, as pace last
        found it)."""
        if self.settings.mode != "on":
            return False
        if self.fast:
            return True
        wait = self.settings.fast_in(self.now() if now is None else now)
        return wait is not None and wait <= WARM_MS

    def pushed_now(self) -> bool:
        """A followed window's book just changed (RuleStream.told: a push, a REST read that differed, matching seen open with
        an offer in hand). While the taker goes first (first_now) and no tick runs or waits for its turn, the tick begins here
        — in the turn of the loop that brought the book: the window is looked at, its order signed and the request handed to
        its thread (place: start_now) before any other task gets a turn. Otherwise the loop's own tick takes it, as before:
        woken by the stream's event, some seven turns of the loop on, each behind whatever else was ready. Not once the taker
        or the bot is stopping (as the loop begins no tick then: an order begun into a shutdown is one nobody follows). True
        when the tick ended here with nothing left to take: the loop need not be woken for this change."""
        if not (EAGER and self.running) or self.ticking or self.stop.is_set() or self.owner.stopping.is_set() or not self.first_now():
            return False
        self.first_ticks += 1
        task = start_now(self.tick_first(), "rules-first")
        if not task.done():
            self.first_tasks.add(task)
            task.add_done_callback(self.first_tasks.discard)
            return False
        return not self.stream.changed

    def priced(self, keys: Any) -> None:
        """The index of these cards just moved (the bot's priced(): Binance's index pushed each second, or a read that moved
        it), 1.154.24. A window in its course is bought while the index stands on the rule's side of its start (live_guard),
        and the looks that ask came once a second, of their own: half a second, on average, between the index that put the
        side ahead and the look that saw it — the offers a slower taker finds gone. Now the card's window in its course is
        looked at as the index comes — in the very turn of the loop that brought it while the taker goes first (nudge:
        pushed_now), at the loop's next turn otherwise — and its order leaves then, if the book in hand has an offer at the
        price. Not a window bought in full, nor twice for one index (the bot tells of a move more than once: the push, then
        the fair values it changed); the look is marked as the index's (w["indexed"]): an order of it carries the index's
        arrival, not that of a book that came long before (speed_stats, course_speed)."""
        if not self.running or self.course_cards.isdisjoint(keys):  # (told of every price that moves in the bot: most are no card of a rule's)
            return
        now, cards, hit = self.now(), getattr(self.owner, "updowns", None) or {}, []
        for slug, w in self.track.items():
            rule = RULE_OF[w["rule"]]
            # (not a window paused after a refusal: its look would be put off — due — and a tick begun for nothing each second)
            if w["market"] is None or rule.card not in keys or self.phase(w, now) != "live" or now < w["look_ms"] or self.finished(slug, rule, True):
                continue
            card = cards.get(rule.card)
            # told again for the same index (the push, then the fair values it changed): not looked at twice — but looked at for
            # the same index once the card has moved on to this window, or got its start (what the first look of a course waits for)
            seen = (getattr(card, "price", None), getattr(getattr(card, "spec", None), "slug", ""), self.card_line(card))
            if card is not None and seen == w.get("index_seen"):
                continue
            w["index_seen"] = seen
            if card is not None and now - int(getattr(card, "index_ms", now) or 0) > INDEX_FRESH_MS:
                self.index_late += 1  # an old index (a socket catching up frame by frame): left to the look of the next second
                continue
            hit.append(w)
        # (nor while no order could go out — the pause, the loss stop, the API's cooldown: the look would stop at the gate)
        if not hit or (self.settings.mode == "on" and self.block(0.0)) or self.cooling() > 0:
            return
        if not self.fast:
            self.pace(now)  # (a course that has just begun: the taker goes first from this index on, not from its next tick)
        wall = int(time.time() * 1000)
        for w in hit:
            w["indexed"] = wall
        self.index_looks += 1
        self.stream.nudge(*(str(w["market"]["id"]) for w in hit))

    @staticmethod
    def card_line(card: Any) -> Any:
        """The start a card judges its window by, as it stands (its candle, Predict's own price when it agrees); None for none."""
        try:
            return card.line_used() if card is not None else None
        except Exception:
            return None

    def gc_guard(self, now: int) -> None:
        """No automatic garbage collection from GC_BEFORE_MS before a makers' moment to the end of the seconds around it
        (quiet_ms: a window this taker can still buy), 1.154.23 — and so around the beginning of a window's course and while
        an offer waits for matching to open (1.154.24: quiet_ms's other seconds). A collection of the oldest generation walks every object the
        bot keeps — 220–370 ms measured with the records of 11-18, the whole process stopped — and comes when enough new
        long-lived objects have piled up (an account snapshot of the pre-open mode's is enough): in one of twenty of those
        quarter minutes, by the snapshot's pace. Looked at each tick; let go as the seconds end and when the taker stops."""
        want = self.running and (self.quiet_ms(now) > 0 or self.quiet_ms(now + GC_BEFORE_MS) > 0)  # (the taker's own loop running: run lets go of it as it ends)
        if want and not self.gc_off:
            self.gc_off = True
            core.GC_WATCH.hold()
        elif not want and self.gc_off:
            self.gc_off = False
            core.GC_WATCH.release()

    async def tick_first(self) -> None:
        self.first_tick = True  # (begun with no tick running or waiting: the lock is its own at once — the mark is this tick's alone)
        try:
            await self.tick()
        except asyncio.CancelledError:
            raise
        except Exception as error:  # (as run notes a tick's failure)
            self.note(core.clean_error(error) or type(error).__name__)
        finally:
            self.first_tick = False

    def quiet_ms(self, now: int | None = None) -> int:
        """The ms left of the seconds in which the rest of the bot keeps out of the loop's way (1.154.23): around the makers'
        moment — QUIET_BEFORE_MS before it to QUIET_AFTER_MS after — of a window this taker can still buy before its start (real
        orders, a price set, not bought in full or done with, and nothing that keeps every order back: block). 1.154.24: and
        the first seconds of a window's course, for a rule that buys there — QUIET_BEFORE_MS before the course begins
        (LIVE_START_MS after the start) to COURSE_QUIET_MS after; and while an offer in hand waits for matching to open (rush),
        RUSH_QUIET_MS at a time. 0 outside them. Asked by the pre-open mode before each pair and by the model trader before
        each step (LiveBot.rules_quiet): worked out at most every 50 ms."""
        mono = time.monotonic()
        if now is None and mono - self.quiet_held[0] < 0.05:
            return self.quiet_held[1]
        at_ms = self.now() if now is None else now
        left = 0
        if self.settings.mode == "on":
            for slug, w in self.track.items():
                rule = RULE_OF[w["rule"]]
                at, begins = self.moment(w), w["start"] + LIVE_START_MS
                if at and at - QUIET_BEFORE_MS <= at_ms < at + QUIET_AFTER_MS and at_ms < w["start"] - STOP_MS and self.settings.price(rule.key) > 0:
                    until, course = at + QUIET_AFTER_MS, False        # the makers' moment
                elif (begins - QUIET_BEFORE_MS <= at_ms < begins + COURSE_QUIET_MS and self.live_cents(rule) > 0
                      # (those seconds are of the course's first minute, bought no dearer than before the window: with no price
                      # there nothing is — nor without the bot's card of the market, which says who is ahead)
                      and self.settings.price(rule.key) > 0 and (getattr(self.owner, "updowns", None) or {}).get(rule.card) is not None):
                    until, course = begins + COURSE_QUIET_MS, True    # its course begins
                elif self.rushing(w) and (not w.get("rush_tail") or w["start"] - STOP_MS - at_ms <= TAIL_QUIET_MS):
                    until, course = at_ms + RUSH_QUIET_MS, False      # an offer in hand, matching not open yet (a tail: its last seconds)
                else:
                    continue
                if not self.finished(slug, rule, course):
                    left = max(left, until - at_ms)
            if left and self.block(0.0):
                left = 0  # (no order could go out now: nobody waits for it)
            elif left and self.settings.max_day_usd and self.settings.max_day_usd - spent_on(self.recs, day_of(at_ms), "on") < 1.0:
                left = 0  # (...nor with the day's cap spent, 1.154.24)
        if now is None:
            self.quiet_held = (mono, left)
        return left

    async def turn(self) -> None:
        """One tick's work: the plan, the pace, every window's look, the orders' sends, a REST read beside the pushes, the follow-ups."""
        now = self.now()
        # (1.154.24: the minute's plan — every rule's windows of the day ahead worked out, a millisecond — is the loop's own
        # tick's, a second on at the most: not on the way of an order a pushed book or an index brought; at once all the same
        # when there is none yet, or the settings changed)
        if now - self.planned_ms >= PLAN_EVERY_MS and not (self.first_tick and self.planned_ms):
            self.plan(now)
            self.planned_ms = now
        self.pace(now)
        self.gc_guard(now)
        watched = {str(w["market"]["id"]) for w in self.track.values() if w["market"] is not None}
        self.stream.changed &= watched  # a change of a window no longer watched is nobody's to take (1.153.3)
        if self.settings.mode == "off" or self.cooling() > 0:
            self.stream.changed.clear()  # nothing looked at now: the next look reads the book as it stands then
            for w in self.track.values():
                w.pop("indexed", None)   # (...nor is it the index's, when it comes)
        if self.settings.mode != "off":
            self.collect = []
            try:
                # (1.154.15: a window whose pushed book just changed first — its order is not behind the other windows' looks)
                changed, self.looked = self.stream.changed, self.stream.arrivals
                for slug, w in sorted(list(self.track.items()), key=lambda kv: (
                        kv[1]["market"] is None or str(kv[1]["market"]["id"]) not in changed, kv[1]["start"])):
                    if self.stop.is_set() or self.cooling() > 0:
                        break
                    try:  # one window's failed read never keeps the others, nor the orders' follow-up, waiting
                        await self.look(slug, w)
                    except asyncio.CancelledError:
                        raise
                    except Exception as error:
                        w["word"] = f"读取失败：{core.clean_error(error) or type(error).__name__}"
                        self.note(f"{slug}：{w['word']}")
            finally:
                sends, self.collect = self.collect, None
            if sends:  # 1.154.12: the windows' orders go out side by side (offers showing on several at once: none waits for another's answer)
                for failed in await asyncio.gather(*sends, return_exceptions=True):
                    if isinstance(failed, asyncio.CancelledError):
                        raise failed
                    if isinstance(failed, BaseException):  # (send answers Predict's failures itself: this is its own — the hash stays, found by it)
                        self.note(f"下单过程出错：{core.clean_error(failed) or type(failed).__name__}")
            if self.fast:
                self.verify_tick()
            if self.settings.mode == "on" and self.owner.config.live_max_daily_loss_usd and (
                    self.now() - self.pnl_at[0] >= 1_000 or self.pnl_of != self.pnl_mark(self.now())):
                self.sum_pnl(self.now())  # for the next ticks' orders: summed here, not on an order's way (day_pnl_now)
        self.stream.want({str(w["market"]["id"]) for w in self.track.values() if w["market"] is not None})
        await self.follow()
        await self.settle()

    def pace(self, now: int) -> None:
        """The per-second hours (RULE_FAST_HOURS, 1.154.12): whether they run now, and their preparation — begun WARM_MS before
        them and renewed every WARM_EVERY_MS through them, beside the loop (warm). A window at the makers' moment (1.154.13) and
        one bought in its course (1.154.14) are looked at each second, and prepared, whatever the hours."""
        on = self.settings.mode != "off"
        wait = self.settings.fast_in(now) if on else None
        sprint = on and any(self.sprinting(w, now) for w in self.track.values())  # 1.154.13: a window at the makers' moment
        course = on and any(self.phase(w, now) == "live" for w in self.track.values())  # 1.154.14: a window bought in its course
        fast = wait == 0 or sprint or course
        if fast != self.fast:
            self.fast = fast
            LOG.info("规则吃单：每秒检查%s", "开始" if fast else "结束")
        # ...and prepared from a minute before its start (the sign-in, the allowance: its first order with no request before it)
        near = on and any(now >= w["start"] - WARM_MS and self.live_cents(RULE_OF[w["rule"]]) > 0 for w in self.track.values())
        # 1.154.25: ...and through a window's last NEAR_MS before its start, where an offer is bought as it shows too — and once
        # after a start, as soon as a window's market is known. 11-18 18:06, nine minutes before the 18:15 window and outside
        # every hour above: 11 shares offered, the order signed 914 ms into its look — the sign-in and the allowance's read came
        # first (the process half an hour old: nothing in hand), and the offer went to someone else
        soon = on and any(w["market"] is not None and 0 <= w["start"] - now <= NEAR_MS and self.settings.price(RULE_OF[w["rule"]].key) > 0 for w in self.track.values())
        first = on and not self.warmed and any(w["market"] is not None for w in self.track.values())
        if (((wait is not None and wait <= WARM_MS) or sprint or near or soon or first)
                and now - self.warm_ms >= WARM_EVERY_MS and (self.warming is None or self.warming.done())):
            self.warm_ms, self.warmed = now, True
            self.warming = asyncio.create_task(self.warm(), name="rules-warm")

    def busy_in(self, now: int) -> int | None:
        """How long until the taker needs its key at speed, ms (1.154.21): 0 — it does now; None — not for any window it follows
        (or it is switched off). At speed: the per-second hours (RULE_FAST_HOURS); a window at the makers' moment, from WARM_MS
        before it to SPRINT_MS after; a window's last NEAR_MS before its start (its book and status read every FAST_S) and, for
        a rule that buys there, its course. The pre-open mode asks this before it borrows the key (preopen.Controller.lender):
        the key is the taker's alone from LEND_BEFORE_MS before any of these."""
        if self.settings.mode == "off":
            return None
        soonest = self.settings.fast_in(now)  # 0 inside the hours, None when there are none
        if soonest == 0 or self.fast:
            return 0
        for w in self.track.values():
            spans = [(w["start"] - NEAR_MS, self.end_of(w) - LIVE_STOP_MS if self.live_cents(RULE_OF[w["rule"]]) > 0 else w["start"] - STOP_MS)]
            if at := self.moment(w):
                spans.append((at - WARM_MS, at + SPRINT_MS))
            for begin, end in spans:
                if now >= end:
                    continue
                if now >= begin:
                    return 0
                soonest = begin - now if soonest is None else min(soonest, begin - now)
        return soonest

    def moment(self, w: dict) -> int:
        """When the makers post on this window (1.154.13): RULE_LEAD_MINUTES before its start; 0 with no such moment set."""
        lead = int(getattr(self.settings, "lead_minutes", 0) or 0) * 60_000
        return w["start"] - lead if lead > 0 else 0

    def rushing(self, w: dict) -> bool:
        """The window's offer waits for matching to open and its status is being asked every STATUS_RUSH_S (rush)."""
        task = w.get("rush_task")
        return task is not None and not task.done()

    def rush_pace(self) -> float:
        """Seconds between one window's reads of the status in its rush: STATUS_RUSH_S, and never faster than the key's own
        pace (a tenth slower than PREDICT_RATE_PER_MIN: 0.3 s at 220 a minute), shared by the windows rushing at once (gold
        and oil have their quarter hours in common: two windows, 0.6 s each) — these reads go ahead of the others and may
        borrow as an order does, and what they borrowed the order could not: at this pace the key's budget is not run down
        under it (two windows each at 0.3 s had it at its floor in 7.5 s, the next order waiting a second for its token)."""
        per_min = float(core.PREDICT_RATE.get("per_min") or 0)
        pace = max(STATUS_RUSH_S, 66.0 / per_min) if per_min > 0 and core.PREDICT_PACING else STATUS_RUSH_S
        return pace * max(1, sum(1 for t in self.rush_tasks if not t.done()))

    def rush(self, slug: str, w: dict, tail: bool = False) -> None:
        """1.154.22: an offer at the rule's price in hand on a market whose matching has not been seen open — its status asked now and
        every rush_pace() seconds for STATUS_RUSH_MS, beside the looks (their own read of it keeps its pace: FAST_S, OFFER_STATUS_S) and
        ahead of the key's other reads; the moment it says OPEN the window is looked at, and bought. Real orders only; over when
        the offer is gone, and RUSH_READS_MAX reads a window at the most. 1.154.74: ``tail`` — the start's last TAIL_RUSH_MS, the offer
        still waiting: the same reads to the stop (STOP_MS before the start), TAIL_READS_MAX at the most; the wait for matching is
        still counted from the offer's showing (rush_wall)."""
        if self.settings.mode != "on" or self.rushing(w):
            return
        if int(w.get("tail_reads" if tail else "rush_reads") or 0) >= (TAIL_READS_MAX if tail else RUSH_READS_MAX):
            return
        if not tail or not w.get("rush_wall"):
            w["rush_wall"] = int(time.time() * 1000)
        w["rush_tail"] = tail
        task = w["rush_task"] = asyncio.ensure_future(self.rush_run(slug, w, tail))
        self.rush_tasks.add(task)  # (ended with the taker: run)
        task.add_done_callback(self.rush_tasks.discard)

    async def rush_run(self, slug: str, w: dict, tail: bool = False) -> None:
        began, stop_at = time.monotonic(), w["start"] - STOP_MS
        prior = "priority" in inspect.signature(self.api.market).parameters  # (a stand-in's market() may not take it)
        while (not w["open_ms"] and w.get("offer", True) and self.track.get(slug) is w and w["market"] is not None and not self.stop.is_set()
               and self.settings.mode == "on"
               and ((self.now() < stop_at and int(w.get("tail_reads") or 0) < TAIL_READS_MAX) if tail
                    else (time.monotonic() - began < STATUS_RUSH_MS / 1000 and int(w.get("rush_reads") or 0) < RUSH_READS_MAX))):
            asked = time.monotonic()
            if self.cooling() <= 0:
                mid = str(w["market"]["id"])
                self.reads += 1
                self.status_reads += 1
                w["rush_reads"] = int(w.get("rush_reads") or 0) + 1  # (the order's "读了 N 次状态": the tail's reads too)
                if tail:
                    w["tail_reads"] = int(w.get("tail_reads") or 0) + 1
                try:
                    market = await (self.api.market(mid, priority=True) if prior else self.api.market(mid))
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    w["rush_error"] = core.clean_error(error) or type(error).__name__
                else:
                    if self.track.get(slug) is not w or w["market"] is None:
                        return
                    w["market"] = {**market, "categorySlug": slug}
                    if str(market.get("tradingStatus") or "").upper() == "OPEN":
                        if not w["open_ms"]:
                            w["open_ms"], w["open_wall"] = self.now(), int(time.time() * 1000)
                            self.watch(w)["open_ms"] = self.watch(w)["open_ms"] or w["open_ms"]
                        self.stream.nudge(mid)  # looked at now: the offer in hand is bought
                        return
            await asyncio.sleep(max(0.0, self.rush_pace() - (time.monotonic() - asked)))

    def sprinting(self, w: dict, now: int) -> bool:
        """The window is at the makers' moment: from WARM_MS before it (the preparation) to SPRINT_MS after."""
        at = self.moment(w)
        return bool(at) and -WARM_MS <= now - at < SPRINT_MS

    def hot(self, w: dict, now: int) -> bool:
        """...and right at it: the REST reads beside the pushes are this window's, one a second."""
        at = self.moment(w)
        return bool(at) and -HOT_BEFORE_MS <= now - at < HOT_AFTER_MS

    async def warm(self) -> None:
        """Ready before an offer shows: every window not yet found looked for at the next tick; a market not yet seen open for matching
        read once more (an offer found on it would wait for that read); and, for real orders, the sign-in and the exchange
        allowances in hand — an order then goes out on the first look, no request before it."""
        import live
        try:
            for w in self.track.values():
                if w["market"] is None and w["probe_ms"] <= self.now():  # (not one put off for a title that does not match: look)
                    w["probe_ms"] = 0
            if self.cooling() > 0:
                return
            for slug, w in sorted(list(self.track.items()), key=lambda kv: kv[1]["start"]):
                if w["market"] is None or w["open_ms"] or self.stop.is_set() or slug not in self.track:
                    continue
                now = self.now()
                self.reads += 1
                w["market"], w["status_ms"] = {**await self.api.market(str(w["market"]["id"])), "categorySlug": slug}, now
                if str(w["market"].get("tradingStatus") or "").upper() == "OPEN":
                    w["open_ms"], w["open_wall"] = now, int(time.time() * 1000)
                    self.watch(w)["open_ms"] = self.watch(w)["open_ms"] or now
            if self.settings.mode != "on" or self.block(0.0):
                return
            await self.api.ensure_auth()
            exchanges = set()
            for w in list(self.track.values()):
                if w["market"] is not None:
                    with contextlib.suppress(Exception):
                        info = live.parse_market(w["market"], self.owner.config.predict_fee_bps)
                        exchanges.add(live.exchange_key(info.neg_risk, info.yield_bearing))
            for exchange in sorted(exchanges):
                self.allowance[exchange] = (self.now(), int(await self.owner.live.chain.allowance(exchange)))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.note(f"每秒检查的准备没做完：{core.clean_error(error) or type(error).__name__}")

    def gate(self) -> str:
        """block(0.0) — why no real order could go out now — worked out once a tick: every window's look asked it afresh, and it
        sums the day's P&L over the model trader's and the pre-open mode's records (tens of thousands) for the loss stop; in the
        per-second hours that was several times a second (1.154.12). An order's own checks (place, before_send) stay fresh.
        Outside a tick (a look called by itself) it is worked out there and then."""
        if not self.in_tick:
            return self.block(0.0)
        if self.gated[0] != self.ticks:
            self.gated = (self.ticks, self.block(0.0))
        return self.gated[1]

    def finished(self, slug: str, rule: Rule, course: bool = False) -> str:
        """Why nothing more is bought in this window, "" while something may be: bought in full, an order still out, recorded on
        paper (once before the window, once in its course: ``course``), refused too often."""
        rec = self.record(slug)
        if rec is None:
            return ""
        size = self.settings.shares_of(rule.market)
        if held(rec) >= size - DUST:
            # (1.154.15: an order on its way no longer holds the window by itself — only the shares it asks for; one withdrawn is
            # counted whole until its final word, LATE_FILL_MS on: a fill matched just before may show late)
            return f"这一场已买满 {size:g} 份" if not in_flight(rec) else f"这一场的 {size:g} 份已全部下单，等结果（撤回的部分 {LATE_FILL_MS // 1000} 秒后确认没有晚到的成交才算空出来）"
        if self.settings.mode == "paper" and any(o.get("paper") and bool(o.get("live")) == course for o in rec["orders"]):
            return "只记录：这一场" + ("盘中" if course else "") + "已记过（只记一次：没真买，盘口上的单还在，再记就重复了）"
        # (1.154.26: an order answered 409 and then not to be found by its hash was not placed either — follow_order: counted too)
        if sum(1 for o in rec["orders"] if o.get("state") == "rejected" or (o.get("state") == "not_found" and o.get("conflict"))) >= MAX_REJECTS:
            return f"Predict 拒绝了 {MAX_REJECTS} 次，这一场不再下单"
        return ""

    def verify_tick(self) -> None:
        """The per-second hours' REST read beside the pushes: one window's book at a time, the longest unread first, at most one
        every VERIFY_EVERY_S and none again sooner than VERIFY_AGAIN_S (three a second on a key of the taker's own) — a change
        the push did not bring is seen within seconds, and with the push down the looks go on on these reads. Bounded: seven
        windows read every second would be the key's whole budget. Not while no order could go out, nor for a window done with."""
        if self.verifying is not None and not self.verifying.done():
            return
        mono = time.monotonic()
        every, again = (VERIFY_EVERY_OWN_S, VERIFY_AGAIN_OWN_S) if self.own_key else (VERIFY_EVERY_S, VERIFY_AGAIN_S)
        if mono - self.verify_at < every or self.cooling() > 0 or (self.settings.mode == "on" and self.gate()):
            return
        now = self.now()
        # (1.154.14: a window in its course while it may be bought there — not with the index on the wrong side of its start)
        open_to = [(slug, w) for slug, w in self.track.items()
                   if w["market"] is not None and (phase := self.phase(w, now)) and now >= w["look_ms"]
                   and (self.live_cents(RULE_OF[w["rule"]]) if phase == "live" else self.settings.price(w["rule"])) > 0
                   and not self.finished(slug, RULE_OF[w["rule"]], phase == "live")
                   and not (phase == "live" and self.live_guard(slug, RULE_OF[w["rule"]], now))]
        hot = [(slug, w) for slug, w in open_to if self.hot(w, now)]  # 1.154.13: at the makers' moment the reads are that window's alone
        # (1.154.15: on a key of the taker's own the window at the makers' moment is read as often as the key's pace allows — three a second)
        often = (every if self.own_key else min(again, VERIFY_HOT_S)) if hot else again
        due = [(w.get("rest_at", 0.0), slug) for slug, w in (hot or open_to) if mono - w.get("rest_at", 0.0) >= often]
        if not due:
            return
        slug = min(due)[1]
        w = self.track[slug]
        w["rest_at"] = self.verify_at = mono
        if hot and not w["open_ms"] and self.stream.vouches(str(w["market"]["id"])) and self.stream.held(str(w["market"]["id"])) is not None:
            # 1.154.22: at the makers' moment a market not yet seen open for matching is asked for its status, not its book — while
            # its book comes pushed (the connection alive, a book of it in hand; 1.154.33: and subscribed to it — vouches): an offer
            # found on it would wait for this very read. With the push down the read stays the book's, as before: the looks go on on
            # these reads alone then
            self.verifying = asyncio.create_task(self.verify_status(slug, w), name="rules-verify-status")
        else:
            self.verifying = asyncio.create_task(self.verify(str(w["market"]["id"])), name="rules-verify")

    async def verify_status(self, slug: str, w: dict) -> None:
        try:
            self.reads += 1
            self.status_reads += 1
            now = self.now()
            market = await self.api.market(str(w["market"]["id"]))
            if slug not in self.track or self.track[slug] is not w or w["market"] is None:
                return
            w["market"], w["status_ms"] = {**market, "categorySlug": slug}, max(now, int(w.get("status_ms") or 0))
            if str(market.get("tradingStatus") or "").upper() == "OPEN" and not w["open_ms"]:
                w["open_ms"], w["open_wall"] = self.now(), int(time.time() * 1000)
                self.watch(w)["open_ms"] = self.watch(w)["open_ms"] or w["open_ms"]
                if w.get("offer"):  # an offer already in hand: looked at now, bought now
                    self.stream.nudge(str(w["market"]["id"]))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.note(f"撮合状态没读到：{core.clean_error(error) or type(error).__name__}")

    async def verify(self, mid: str) -> None:
        asked = time.monotonic()
        try:
            self.reads += 1
            self.verifies += 1
            bids, asks = await self.book(mid)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.verify_error = f"{bj(self.now(), False)} {core.clean_error(error) or type(error).__name__}"
            return
        self.verify_error = ""
        self.verify_ms.append(int((time.monotonic() - asked) * 1000))  # a REST read's round trip, the wait for the key's pace included (status)
        if self.stream.merge(mid, bids, asks, asked):
            self.verify_diffs += 1

    def plan(self, now: int) -> None:
        """The windows of the enabled rules from now to a day on (HORIZON_MS); a window of a rule switched off is dropped, and so
        is one this close to its start (STOP_MS) — or, for a rule that buys in its windows' course too (1.154.14), to its end
        (over). The records past KEEP_DAYS and finished are removed."""
        want: dict[str, tuple[str, int]] = {}
        self.course_cards = frozenset(r.card for r in RULES if self.live_cents(r) > 0) if self.settings.mode != "off" else frozenset()  # (priced, 1.154.24)
        if self.settings.mode != "off":
            for rule in RULES:
                course = self.live_cents(rule) > 0  # 1.154.14: bought in the window's course too — followed to LIVE_STOP_MS before its end
                if live_rule(self.settings, rule) or course:  # its market switched on (RULE_GOLD / RULE_OIL) and a price set
                    back = card_of(rule).window_ms - 1 if course else -STOP_MS
                    for start in windows(rule, now - back, now + HORIZON_MS):
                        if not self.over({"rule": rule.key, "start": start}, now):
                            want[card_of(rule).slug_of(start)] = (rule.key, start)
        for slug in list(self.track):
            if slug not in want:
                w = self.track.pop(slug)
                if w["start"] - now <= STOP_MS and not w.get("kept"):  # its end reached between two looks: its watch kept all the same
                    self.keep_watch(slug, w)
        for slug, (key, start) in want.items():
            fresh = slug not in self.track
            w = self.track.setdefault(slug, {"rule": key, "start": start, "market": None, "listed_ms": 0, "open_ms": 0, "probe_ms": 0,
                                             "look_ms": 0, "status_ms": 0, "offer": False, "word": ""})
            kept = (self.record(slug) or {}).get("seen") if fresh else None
            if isinstance(kept, dict) and kept.get("open"):  # 1.154.16: the watch as it stood before a restart (save_watch)
                w["seen"] = {k: kept.get(k, v) for k, v in WATCH.items()}
        old = [key for key, r in self.recs.items()
               if int(r.get("start_ms") or 0) < now - KEEP_DAYS * core.DAY_MS and not in_flight(r) and (r.get("pnl") is not None or filled(r) <= DUST)]
        if old:
            self.store.delete_keys([PREFIX + key for key in old])
            for key in old:
                self.recs.pop(key, None)

    def live_cents(self, rule: Rule) -> int:
        """The rule's highest price in its window's course (RULE_*_LIVE_CENTS, 1.154.14), cents; 0 = it buys before the window alone
        (no such price set, its market switched off, or a rule that has no way to judge the window's course)."""
        return live_price_of(self.settings, rule)

    def end_of(self, w: dict) -> int:
        return card_of(RULE_OF[w["rule"]]).end_of(w["start"])

    def phase(self, w: dict, now: int) -> str:
        """Where the window stands: "before" — ahead of its start by more than STOP_MS (the rule's own price); "live" — in its
        course, from LIVE_START_MS after the start to LIVE_STOP_MS before the end, for a rule that buys there (1.154.14);
        "" — neither: the last seconds before the start and the first after it, or over (over)."""
        if w["start"] - now > STOP_MS:
            return "before"
        if self.live_cents(RULE_OF[w["rule"]]) > 0 and now - w["start"] >= LIVE_START_MS and self.end_of(w) - now > LIVE_STOP_MS:
            return "live"
        return ""

    def over(self, w: dict, now: int) -> bool:
        """Nothing more is bought in this window: its start STOP_MS away or passed, and no course to buy in (or that over too)."""
        if w["start"] - now > STOP_MS:
            return False
        return not (self.live_cents(RULE_OF[w["rule"]]) > 0 and self.end_of(w) - now > LIVE_STOP_MS)

    def live_guard(self, slug: str, rule: Rule, now: int) -> str:
        """Why the rule's side is not bought in the window's course right now, "" while it may be (1.154.14): by the bot's own card
        of the market — the window's start and the index this second. In the course of a window the book's price already tells who
        is ahead: a price alone would buy the side that is losing. The rule's side must be ahead of the start by Rule.lead. The
        start is the close of the index's minute candle ending there (what the backtest measured against) and, when Predict
        shows its own start price and it agrees with that candle (IndexUpDownMarket.line_used), that one too: ahead of both —
        of whichever is the less kind to the side. Not on a card still on another window, without a start or an index, nor on
        an index too old by the card's own rule (price_problem)."""
        card = (getattr(self.owner, "updowns", None) or {}).get(rule.card)
        if card is None:
            return f"机器人没有 {rule.card} 这张卡，判断不了涨跌谁领先"
        if getattr(getattr(card, "spec", None), "slug", "") != slug:
            return "卡片还没换到这一场，等起点价"
        lines = [x for x in {card.close_of("start"), card.line_used()} if x is not None and x > 0]
        price = card.price
        if not lines:
            return "还没读到这一场的起点价"
        if price is None or not price > 0:
            return "还没有实时指数"
        if why := card.price_problem(now):
            return why
        up = rule.side == "up"
        line = max(lines) if up else min(lines)  # the less kind to the rule's side
        if ((price - line) if up else (line - price)) / line >= rule.lead:
            return ""
        return (f"{rule.word}还没领先（指数 {core.fmt(price)}，起点 {core.fmt(line)}，要{'高' if up else '低'}出 {float(rule.lead) * 100:g}% 以上），不买")

    def due(self, w: dict, now: int) -> bool:
        """Whether the window is to be looked at this tick: at once when its pushed book changed (1.153.1); else by its cadence —
        the listing looked for around its moment, every FAST_S in the last NEAR_MS, every OFFER_STATUS_S while an offer at the
        price waits for matching to open, the rest every SLOW_S (CLOSED_SLOW_S before matching opens); in the per-second hours
        (RULE_FAST_HOURS, 1.154.12) every listed window every FAST_LOOK_S."""
        start, near = w["start"], w["start"] - now <= NEAR_MS
        if w["market"] is None:
            expected = start - LISTED_AHEAD_MS
            if now < expected - PROBE_EARLY_MS:
                return False
            every = PROBE_FAST_S if now <= expected + PROBE_LATE_MS or near else PROBE_SLOW_S
            return now - w["probe_ms"] >= every * 1000
        mid = str(w["market"]["id"])
        if now < w["look_ms"]:  # paused (a refusal): a change meanwhile is not one to act on (1.153.3: it kept the loop going round)
            self.stream.changed.discard(mid)
            w.pop("indexed", None)  # (nor an index that came meanwhile: the look after the pause is not its own, 1.154.24)
            return False
        if mid in self.stream.changed:
            return True
        every = (FAST_LOOK_S if self.fast  # the per-second hours (1.154.12)
                 else FAST_S if near else OFFER_STATUS_S if (w["offer"] and not w["open_ms"]) else SLOW_S if w["open_ms"] else CLOSED_SLOW_S)
        return now - w["look_ms"] >= every * 1000

    async def look(self, slug: str, w: dict) -> None:
        """One look at a window before its start: its market found as Predict lists it; its book (pushed, or read); the offers at
        the rule's price (the pre-open mode's own bids taken out); matching's status read when there is something to buy and
        it has not opened; then the take — up to the market's RULE_*_SHARES a window and RULE_MAX_DAY_USD a day. 1.154.14: and,
        for a rule with a price for it (RULE_*_LIVE_CENTS), in the window's course too (phase) — at that price, while the index
        stands on the rule's side of the window's start (live_guard); nothing in the seconds around the start nor in the last
        LIVE_STOP_MS; the window's size is one, before it and in its course together."""
        now = self.now()
        rule = RULE_OF[w["rule"]]
        phase = self.phase(w, now)
        course = phase == "live"
        if phase != "before" and not w.get("kept"):
            # the watch of the window before its start is kept as that phase ends (1.154.14: the window may be followed on, in its
            # course — where the offers say who is ahead, not what the rule's price should be)
            w["kept"] = True
            self.keep_watch(slug, w)
        if not phase:
            if self.over(w, now):
                self.track.pop(slug, None)
            else:
                w["word"] = "开场前后这几秒不下单（等这一场的起点价）"
            return
        if not self.due(w, now):
            return
        w["look_wall"], w["fresh"] = int(time.time() * 1000), False
        # 1.154.24: the index brought this look (priced) — its arrival kept for the order (place), when it is of this moment
        indexed = int(w.pop("indexed", 0) or 0)
        w["index_wall"] = indexed if course and 0 <= w["look_wall"] - indexed < 2_000 else 0
        if w["market"] is not None:
            # this look answers a book that just came (speed_stats) — not when an index marked its market as changed (however
            # long ago: a look that came seconds after it — a tick was running — is no answer to either)
            w["fresh"] = str(w["market"]["id"]) in self.stream.changed and not indexed
            self.stream.changed.discard(str(w["market"]["id"]))  # taken now, whatever this look decides (1.153.3)
            if not course and self.observe(w, rule, now, slug):
                self.save_watch(slug, w, rule)
        settings = self.settings  # the settings this look decides under: checked again after its reads (below)
        rec = self.record(slug)
        size = self.settings.shares_of(rule.market)
        cents = self.live_cents(rule) if course else self.settings.price(rule.key)
        early = course and now - w["start"] < LIVE_FULL_MS
        if early:
            cents = min(cents, self.settings.price(rule.key))  # the window's first minute: no dearer than before it
        if done := self.finished(slug, rule, course):
            w["word"] = done
            return
        if self.settings.mode == "on" and (why := self.gate()):
            w["word"] = why  # nothing read while no order could go out
            return
        if w["market"] is None:
            w["probe_ms"] = now
            import preopen
            self.reads += 1
            market = await preopen.category_market(self.api, slug)
            if market is None:
                w["word"] = "还没发布"
                return
            question = str(market.get("question") or market.get("title") or "")
            if problem := card_of(rule).title_problem(question, w["start"]):
                w["word"] = f"市场标题和场次对不上：{problem}"
                w["probe_ms"] = now + 10 * 60_000  # looked at again in ten minutes, not every two seconds
                return
            w["market"], w["listed_ms"], w["status_ms"] = market, now, now  # the listing's own read carries the status
            if str(market.get("tradingStatus") or "").upper() == "OPEN":
                w["open_ms"] = now
            self.watch(w)["listed_ms"] = self.watch(w)["listed_ms"] or now  # (as first seen: the watch may be one restored after a restart)
        w["look_ms"] = now
        if early and self.settings.mode == "paper":
            # 只记录 records the course once: at the course's own price, not in the first minute at the price before the window
            w["word"] = "盘中：只记录模式从开场 1 分钟后按盘中的价格记一次（第 1 分钟的价格和盘前一样）"
            return
        if cents <= 0:  # (its market found all the same: subscribed and at hand when the window's course begins)
            w["word"] = ("盘中：开场后第 1 分钟最高按盘前的价格买（盘前价格是 0，不买），1 分钟后按盘中的价格" if early
                         else "盘前不买这条（盘前价格是 0），开场 1 分钟后按盘中的价格买")
            return
        if course and (why := self.live_guard(slug, rule, now)):
            w["word"] = f"盘中：{why}"  # no book read while the index is on the wrong side of the start
            return
        mid = str(w["market"]["id"])
        pushed = self.stream.book(mid)
        asked = time.monotonic()
        if self.fast:
            # 1.154.12, the per-second hours: the book in hand — the push connection's while it lives (it vouches for a quiet market's
            # book, which is not sent again), else a REST read's (verify) while young. Read here only with none in hand and no read
            # of this window in the last FAST_STALE_S (the hours' first look, a connection just made): every window read every
            # second would be the key's whole budget — otherwise the look waits for verify's next read
            self.fast_looks += 1
            if pushed is None:
                got = self.stream.held(mid)
                # (1.154.33, an outside review, CR-04: the live connection vouches for its subscriptions' books alone — vouches)
                if got is not None and (self.stream.vouches(mid) or asked - got[2] <= FAST_STALE_S):
                    pushed = got[:2]
                elif asked - w.get("rest_at", -1e9) < FAST_STALE_S:
                    w["word"] = "每秒检查：等盘口（推送还没送到，REST 轮流读取中）"
                    return
                else:
                    w["rest_at"] = asked
        if pushed is None:
            self.reads += 1
        bids, asks = pushed if pushed is not None else await self.book(mid)
        if pushed is None and self.fast:
            got = self.stream.held(mid)
            if got is not None and got[2] > asked:
                # 1.154.26 (the audit of 11-18, 16): a book was pushed while this read was out. merge drops the read then (the push
                # is the newer) — but the look went on with the read's levels all the same, and marked the push as taken: an
                # offer the push brought was seen a look later, an order sized on offers the push had already taken away
                bids, asks = got[0], got[1]
                pushed = (bids, asks)  # (its stamps are the push's: book_at below)
            else:
                self.stream.merge(mid, bids, asks, asked)  # in hand for the next looks
            self.stream.changed.discard(mid)           # ...and taken by this one
        # 1.154.13, the race's clock: Predict's own stamp on this book (a pushed one's) and when it reached the bot — kept on the watch
        # (when an offer first showed, when it was gone) and on an order sized on it (how long the bot took)
        w["book_at"] = self.stream.stamps.get(mid, (0, 0)) if pushed is not None else (0, int(time.time() * 1000))
        limit = D(cents) / 100  # the rule's price before the window; in the window's course its own (RULE_*_LIVE_CENTS), the lower of the two in the first minute
        levels = offered(bids, asks, rule.side, limit) if limit > 0 else []
        if levels:  # (1.154.12: the pre-open mode's ledger — tens of thousands of records — is walked only when something is offered at all)
            own = self.own_asks(slug, rule.side)
            if own and min(own) <= limit:
                # 1.153.3: the pre-open mode's own bids there are offers of this side in the book; a buy at the rule's price would take
                # them first (the best price first) — the account trading with itself, the fee paid for nothing. Priced a cent under
                # the cheapest of them instead, sized from the others' offers below it
                limit = min(own) - D("0.01")
                levels = offered(bids, asks, rule.side, limit) if limit > 0 else []
        if course and self.settings.mode == "paper" and rec and levels:
            # 只记录: what the paper order before the window "bought" is still on the book (nothing was bought) — not there to buy again
            levels = without(levels, D(str(sum(float(o.get("filled") or 0) for o in rec["orders"] if o.get("paper") and not o.get("live")))))
        w["offer"] = bool(levels)
        if not levels:
            w.pop("rushed", None)  # (the offer gone: its next showing is asked about afresh)
            w.pop("tail_rushed", None)
        best = offered(bids, asks, rule.side, D("0.99"))[:1]
        if not course and self.note_offers(w, levels, best, now, slug, rule, (bids, asks), rec):
            self.save_watch(slug, w, rule)
        if not levels:
            w["word"] = ("盘中：" if course else "") + f"盘口没有 ≤{int(limit * 100)}¢ 的{rule.word}" + (f"（最低 {float(best[0][0]) * 100:g}¢）" if best else "（没人卖）")
            return
        tail = not course and w["start"] - now < ORDER_BEFORE_MS  # 1.154.55: the start's last two minutes
        if (self.end_of(w) if course else w["start"]) - now < ORDER_BEFORE_MS and not (tail and self.settings.tail):
            # 1.154.52: Predict takes no order with under two minutes to its expiry, and this one would be signed to expire
            # EXPIRY_BEFORE_S before the start (in the course: the end). Not sent to be refused; the offer is still watched (above)
            # (1.154.55: before the start it is signed for after it instead — place — unless RULE_TAIL is off)
            w["word"] = (("盘中：" if course else "") + f"盘口有 ≤{int(limit * 100)}¢ 的{rule.word} {sum(q for _, q in levels):g} 份，但离"
                         + ("这一场结束" if course else "开场") + f"不足 {ORDER_BEFORE_MS // 1000} 秒，不下单：订单签到" + ("结束" if course else "开场")
                         + f"前 {EXPIRY_BEFORE_S} 秒到期，Predict 不收有效期不足 2 分钟的单" + ("" if course else "（最后 2 分钟也吃：RULE_TAIL=on）"))
            return
        if not w["open_ms"]:
            near = w["start"] - now <= NEAR_MS
            if not w.get("rushed"):
                w["rushed"] = True  # 1.154.22: this showing of the offer: the status asked beside the looks, every STATUS_RUSH_S (rush)
                self.rush(slug, w)
            elif not w.get("tail_rushed") and STOP_MS < w["start"] - now <= TAIL_RUSH_MS and not self.rushing(w):
                w["tail_rushed"] = True  # 1.154.74: the start's last two minutes, the offer still waiting: asked to the stop (rush)
                self.rush(slug, w, tail=True)
            if now - w["status_ms"] >= (FAST_S if near or self.fast else OFFER_STATUS_S) * 1000:
                w["status_ms"] = now
                self.reads += 1
                w["market"] = {**await self.api.market(mid), "categorySlug": slug}
                if str(w["market"].get("tradingStatus") or "").upper() == "OPEN" and not w["open_ms"]:
                    w["open_ms"], w["open_wall"] = now, int(time.time() * 1000)
                    self.watch(w)["open_ms"] = self.watch(w)["open_ms"] or now
            if not w["open_ms"]:
                w["word"] = (f"盘口已有 ≤{int(limit * 100)}¢ 的{rule.word} {sum(q for _, q in levels):g} 份，等撮合开启就买"
                             f"（现在 {w['market'].get('tradingStatus') or '?'}" + (f"，每 {self.rush_pace():g} 秒读一次状态" if self.rushing(w) else "") + "）")
                return
        room = D(str(size)) - D(str(held(rec) if rec else 0.0))
        there = sum(q for _, q in levels)
        take = min(there, room)
        if self.settings.mode == "on":
            # 1.154.15: the window bought at once. The offers in hand are first those of the window's orders already on their way;
            # with enough beyond them the order is for all the window may still buy — it takes what stands at the rule's price when
            # it arrives, and a ladder still being posted has more by then than the book in hand showed. Not cut to
            # LIVE_MAX_ORDER_USD any more (that made two orders of one window, the second after the first's word): the window's
            # own size (RULE_*_SHARES) and the day's cap bound it, and the USDT at hand
            out = open_orders(rec) if rec else []
            if len(out) >= MAX_OPEN_ORDERS:
                w["word"] = f"这一场已有 {len(out)} 张单在途，等它们的结果"
                return
            fresh = there - D(str(claims(rec) if rec else 0.0))
            if fresh < 1:
                w["word"] = f"盘口 ≤{int(limit * 100)}¢ 的{rule.word} {there:g} 份，在途的单正在吃"
                return
            # (1.154.18: around the makers' moment an offer of SWEEP_SHARES is a ladder's first step, whatever part of the window it
            # is — 11-18 01:45, 46 shares in the first book of a ladder of 506: the order for all the window may buy then, not after
            # the next push)
            # (1.154.55: never in the start's last two minutes — an order there outlives the start, so it asks for what stands and no more)
            take = room if not tail and fresh >= SWEEP_SHARES and (fresh >= room / SWEEP_PART or self.sprinting(w, now)) else min(fresh, room)
            usd_left = self.owner.live.usd() if callable(getattr(self.owner.live, "usd", None)) else None
            if usd_left is not None:  # no more than the USDT at hand buys at the order's price
                # 1.154.33 (an outside review, CR-03): ...less what this taker's orders still out may take — those of this very turn
                # too (several windows' orders go out side by side, each sized on the whole balance before)
                pending = self.pending_usd()
                afford = (D(str(max(usd_left - pending, 0.0))) / limit).to_integral_value(rounding="ROUND_FLOOR")
                if afford < 1 or float(afford * limit) < 1.0:
                    w["word"] = (f"USDT 余额 ${usd_left:,.2f}，在途的规则单还要 ${pending:,.2f}，剩下的不够一张最小订单" if pending > DUST
                                 else f"USDT 余额 ${usd_left:,.2f} 不够一张最小订单")
                    return
                take = min(take, afford)
        if self.settings.max_day_usd:
            left = self.settings.max_day_usd - spent_on(self.recs, day_of(now), self.settings.mode)
            take = min(take, D(str(max(left, 0.0))) / limit)
            if left < 1.0:
                w["word"] = f"今天已花 ${self.settings.max_day_usd - left:,.2f}，到了每天上限 ${self.settings.max_day_usd:g}（RULE_MAX_DAY_USD）"
                return
        take = take.to_integral_value(rounding="ROUND_FLOOR")
        if take < 1 or float(take * limit) < 1.0:  # Predict's minimum order ($1, live.MIN_ORDER_USD)
            w["word"] = f"盘口 ≤{int(limit * 100)}¢ 的{rule.word}只有 {there:g} 份，不够一张最小订单（或额度、余额剩得不够）"
            return
        if self.settings != settings:
            # 1.154.26 (audit 5): the settings were saved anew while this look waited for a read (the listing, the book, the
            # status). The record in hand is the old mode's — on → paper: paper() added a paper fill to the real record, saved
            # as real (shares nobody bought in the day's spend, the positions check and the window's P&L against the loss stop);
            # paper → on: place() put a real order on the paper record, which nothing follows (never read, its rest never
            # withdrawn) — and the size, the price and the day's cap worked out above are the old ones. Nothing done on them
            w["word"] = "设置刚改过：这一轮不下单，下一轮按新设置重新看"
            w["look_ms"] = 0  # (at the next tick, not a cadence on: the book's change this look took is not marked any more)
            return
        if self.settings.mode == "paper":
            self.paper(slug, w, rule, levels, take, limit, rec, course)
        else:
            await self.place(slug, w, rule, levels, take, limit, rec, course)

    def observe(self, w: dict, rule: Rule, now: int, slug: str = "") -> bool:
        """The race's clock kept whatever this look may do (1.154.13): once an offer at the rule's price was seen, the book the
        push connection holds says when it was first taken from and when it was gone — by Predict's own stamp on that book. A
        look held back (the window's shares all ordered, for a minute after a miss; a pause) saw the offer's going only much
        later, and by then the stamp in hand was a later change's. True when something was noted (clock_offers)."""
        seen = w.get("seen")
        # (1.154.28: on past the offers' going, until the first book after it with none of this taker's orders resting — clock_bids)
        if not seen or not seen.get("offer_ms") or (seen.get("gone_ms") and (seen.get("bids_at") is None or seen.get("bids_after") is not None)):
            return False
        mid = str(w["market"]["id"])
        got, stamp = self.stream.held(mid), self.stream.stamps.get(mid)
        limit = D(self.settings.price(rule.key)) / 100
        if got is None or stamp is None or limit <= 0:
            return False
        noted = self.clock_offers(w, float(sum(q for _, q in offered(got[0], got[1], rule.side, limit))), now, int(stamp[0]))
        return self.clock_bids(w, slug, rule.side, bidding(got[0], got[1], rule.side), limit - RIVAL_BELOW, self.record(slug), now,
                               int(stamp[0]), got[2]) or noted

    def own_asks(self, slug: str, side: str) -> dict[D, D]:
        """The pre-open mode's resting bids on this window's other side, as offers of ``side`` (price -> shares): its Up bid at
        49¢ is a Down offer at 51¢ in the book. The shut and the reopen's first are never quoted (1.150.1 / 1.151.1); the slots
        05:15, 05:00, 16:45 and oil's 00:45 can be."""
        ctl = getattr(self.owner, "preopen", None)
        rows = getattr(getattr(ctl, "ledger", None), "rows", None)
        if not rows:
            return {}
        other, out = ("down" if side == "up" else "up"), {}
        # 1.154.13: by the ledger's own keys — the window's slug, "#<level>" for a deeper level — not a walk of every record (39,000 of
        # them on 11-17: 17 ms and more, between an offer showing and the order). A record renamed "@<attempt>" is a withdrawn pair
        # whose orders are all final (preopen.supersedable): it holds nothing that could rest
        for key in (slug, *(f"{slug}#{level}" for level in range(1, OWN_LEVELS))):
            pair = rows.get(key)
            if pair is None:
                continue
            order = (pair.get("orders") or {}).get(other) or {}
            if order.get("state") not in {"prepared", "placing", "open", "unknown"}:
                continue
            with contextlib.suppress(Exception):
                price, left = D(str(pair["price"])), D(str(pair["shares"])) - D(str(order.get("filled") or 0))
                if left > 0:
                    out[1 - price] = out.get(1 - price, D(0)) + left
        return out

    @staticmethod
    def watch(w: dict) -> dict:
        """What the window's watch saw (1.153.2), kept on its record at the end: the lowest offer of the rule's side at any price
        (best), the most shares offered at the rule's price at once (offer_max) and when first (offer_ms), when it was listed and
        when matching was first seen open — what /rules and the review page sum up to show whether a price or a size is off."""
        return w.setdefault("seen", dict(WATCH))

    def note_offers(self, w: dict, levels: list, best: list, now: int, slug: str = "", rule: Rule | None = None,
                    book: tuple | None = None, rec: dict | None = None) -> bool:
        """This look's book onto the window's watch; True when the race's clock noted something (the watch is saved then). With
        the rule and the book's two sides, its bids of the rule's side too (clock_bids, 1.154.28)."""
        seen = self.watch(w)
        if best:
            seen["best"] = min(float(best[0][0]), seen["best"] if seen["best"] is not None else 1.0)
        stamp = int((w.get("book_at") or (0, 0))[0])
        noted = self.clock_offers(w, float(sum(q for _, q in levels)), now, stamp)
        if rule is not None and book is not None:
            floor = D(self.settings.price(rule.key)) / 100 - RIVAL_BELOW
            noted = self.clock_bids(w, slug, rule.side, bidding(book[0], book[1], rule.side), floor, rec, now, stamp) or noted
        if w["open_ms"] and not seen["open_ms"]:
            seen["open_ms"] = w["open_ms"]
        return noted

    def clock_offers(self, w: dict, total: float, now: int, stamp: int) -> bool:
        """The race's clock on the window's watch: when the first offer at the rule's price showed, when the amount on offer first
        went down (1.154.16: someone began to take it) and when none was left (1.154.13: bought — by this taker or a faster one —
        or withdrawn), each with Predict's own stamp of that book: how fast whoever took them was. True when something was
        noted."""
        seen, noted = self.watch(w), False
        if total > seen["offer_max"]:
            seen["offer_max"], noted = total, True
        if total > 0 and not seen["offer_ms"]:
            seen["offer_ms"], seen["offer_ts"], noted = now, stamp, True
        elif seen["offer_ms"] and not seen.get("cut_ms") and total < float(w.get("there") or 0.0) - DUST:
            seen["cut_ms"], seen["cut_ts"], noted = now, stamp, True
        if total <= 0 and seen["offer_ms"] and not seen.get("gone_ms"):
            seen["gone_ms"], seen["gone_ts"], noted = now, stamp, True
        w["there"] = total
        return noted

    def clock_bids(self, w: dict, slug: str, side: str, bids: list, floor: D, rec: dict | None, now: int, stamp: int,
                   at: float | None = None) -> bool:
        """The bids of the rule's side on the window's watch (1.154.28, bidding): up to the book that showed the offers, the first
        sight of one at ``floor`` or dearer — the pre-open mode's own left out — with when, Predict's stamp, its price and shares and
        whether it was there at the window's first look; the book before the offers showed and the one that showed them (no order of
        this taker's is there yet: it is placed on that very book); once they were gone, the first book the push connection holds
        (``at``: when it was taken, monotonic) from BIDS_SETTLE_S after none of this taker's orders was seen resting — what the
        resting buys had left. True when something was noted (the watch is saved then)."""
        seen, noted, mine = self.watch(w), False, None
        if not seen.get("looked_ms"):
            seen["looked_ms"] = now

        def own() -> dict:  # (the pre-open mode's own bids of this side, looked up once a call at most, and only when needed)
            nonlocal mine
            mine = self.own_bids(slug, side) if mine is None else mine
            return mine
        showing = seen.get("offer_ms") == now and seen.get("bids_at") is None  # (the offers' showing noted by clock_offers this very look)
        if (not seen.get("offer_ms") or showing) and not seen.get("rival_ms") and bids and D(str(bids[0][0])) >= floor:
            first = next((level for level in minus(bids, own()) if D(str(level[0])) >= floor), None)
            if first is not None:
                seen.update(rival_ms=now, rival_ts=stamp, rival_px=first[0], rival_sz=first[1], rival_first=seen["looked_ms"] == now)
                noted = True
        if showing:
            before = w.get("bids_last")
            seen["bids_before"] = minus(before, own()) if before is not None else None
            seen["bids_at"], noted = minus(bids, own()), True
        elif seen.get("gone_ms") and seen.get("bids_at") is not None and seen.get("bids_after") is None:
            # (an order is off the book on Predict's word that it removed it, or ended: one whose withdrawal is not confirmed may rest)
            if any(o.get("state") in OPEN_STATES | {"cancelling"} and not o.get("removed") and not o.get("paper") for o in (rec or {}).get("orders") or []):
                w.pop("clear_mono", None)
            elif "clear_mono" not in w:
                w["clear_mono"] = time.monotonic()
            elif at is not None and at >= w["clear_mono"] + BIDS_SETTLE_S:
                seen["bids_after"], noted = minus(bids, own()), True
        w["bids_last"] = bids
        return noted

    def own_bids(self, slug: str, side: str) -> dict[D, D]:
        """The pre-open mode's resting bids on this window's ``side`` (price -> shares): bids of the rule's side in the book like anybody's,
        but not a buy resting ahead of this taker (clock_bids). The windows the rules take are never quoted (1.150.1 / 1.151.1 / 1.154.19);
        looked for all the same, as own_asks does for the other side."""
        ctl = getattr(self.owner, "preopen", None)
        rows = getattr(getattr(ctl, "ledger", None), "rows", None)
        if not rows:
            return {}
        out: dict[D, D] = {}
        for key in (slug, *(f"{slug}#{level}" for level in range(1, OWN_LEVELS))):
            pair = rows.get(key)
            if pair is None:
                continue
            order = (pair.get("orders") or {}).get(side) or {}
            if order.get("state") not in {"prepared", "placing", "open", "unknown"}:
                continue
            with contextlib.suppress(Exception):
                price, left = D(str(pair["price"])), D(str(pair["shares"])) - D(str(order.get("filled") or 0))
                if left > 0:
                    out[price] = out.get(price, D(0)) + left
        return out

    def save_watch(self, slug: str, w: dict, rule: Rule) -> None:
        """The watch onto the window's record as it goes (1.154.16) — an offer shown, taken, gone: /rules shows the race while the
        window is still hours away, and a restart does not lose it (11-18: the offers of 01:00–01:45 were in memory alone, three
        deploys later the watches of those windows knew of none). ``open``: not the window's last word yet (keep_watch)."""
        seen = w.get("seen")
        if not seen or w["market"] is None or self.settings.mode == "off" or self.settings.price(rule.key) <= 0:
            return
        rec = self.record(slug) or self.new_record(slug, w, rule, 0)
        rec["seen"] = {**seen, "start_ms": w["start"], "cents": self.settings.price(rule.key), "open": True}
        self.save(rec)

    def keep_watch(self, slug: str, w: dict) -> None:
        """The window's watch onto its record as it ends (one made for it when nothing was bought, so the windows that offered
        nothing — or nothing at the price — are counted too)."""
        seen = w.get("seen")
        rule = RULE_OF[w["rule"]]
        if not seen or not seen.get("listed_ms") or w["market"] is None or self.settings.mode == "off" or self.settings.price(rule.key) <= 0:
            return
        rec = self.record(slug) or self.new_record(slug, w, rule, 0)
        rec["seen"] = {**seen, "start_ms": w["start"], "cents": self.settings.price(rule.key), "bought": round(filled(rec), 4)}
        self.save(rec)

    async def book(self, market_id: str) -> tuple[tuple, tuple]:
        answer = await self.api.call("GET", f"/markets/{core.urllib.parse.quote(market_id)}/orderbook")
        data = answer.get("data", answer) if isinstance(answer, dict) else {}
        if not isinstance(data, dict) or ("bids" not in data and "asks" not in data):
            raise core.RemoteError("订单簿格式异常")
        return core.predict_levels(data.get("bids"), True), core.predict_levels(data.get("asks"), False)

    def new_record(self, slug: str, w: dict, rule: Rule, index_set: int, mode: str | None = None) -> dict:
        card, start = card_of(rule), w["start"]
        return {"slug": slug, "rule": rule.key, "card": rule.card, "side": rule.side, "mode": mode or self.settings.mode,
                "name": card.window_spec(start).name if hasattr(card.window_spec(start), "name") else slug,
                "start_ms": start, "end_ms": card.end_of(start), "market_id": str(w["market"]["id"]), "index_set": index_set,
                "cents": self.settings.price(rule.key), "orders": [], "created_ms": self.now(), "result": "", "pnl": None}

    def paper(self, slug: str, w: dict, rule: Rule, levels: list, take: D, limit: D, rec: dict | None, course: bool = False) -> None:
        """只记录: what the order would have bought, at the book's prices — no order. ``course``: in the window's course (1.154.14)."""
        import live
        info = live.parse_market(w["market"], self.owner.config.predict_fee_bps)
        outcome, _ = live.outcome_for_side("updown", rule.side, info.outcomes)
        if rec is not None and rec.get("mode") != "paper":
            rec = self.record(slug, "paper")  # 1.154.26 (audit 5): a paper fill goes onto the paper record alone, whatever was handed in
        rec = rec or self.new_record(slug, w, rule, int((outcome or {}).get("index_set") or 0), mode="paper")
        rec["index_set"] = int(rec.get("index_set") or 0) or int((outcome or {}).get("index_set") or 0)
        avg = walk(levels, take)
        rec["orders"].append({"paper": True, "state": "final", "shares": float(take), "filled": float(take), "limit": float(limit),
                              "avg": float(avg), "fee_bps": info.fee_bps, "at": self.now(), **({"live": True} if course else {}),
                              "book": [[float(p), float(q)] for p, q in levels[:5]]})
        self.save(rec)
        w["word"] = ("盘中 " if course else "") + f"只记录：{rule.word} {float(avg) * 100:.1f}¢ × {take:g} 份"
        self.notify(f"🧪 规则吃单{'·盘中' if course else ''}（只记录，没下单）：{rec['name']} 买{rule.word} 均价 {float(avg) * 100:.1f}¢ × {take:g} 份"
                    f"（{rule.name}{'盘中' if course else ''} ≤{int(limit * 100)}¢）")

    async def place(self, slug: str, w: dict, rule: Rule, levels: list, take: D, limit: D, rec: dict | None, course: bool = False) -> None:
        import live
        import preopen
        usd = float(take * limit)
        if why := self.block(usd, net=True):  # (1.154.33, CR-03: net of the orders still out, this turn's included)
            w["word"] = why
            return
        settings = self.settings  # (the look's: nothing waited for between its own check and here)
        info = live.parse_market(w["market"], self.owner.config.predict_fee_bps)
        outcome, assumed = live.outcome_for_side("updown", rule.side, info.outcomes)
        if outcome is None or assumed:
            w["word"] = f"认不出{rule.word}的代币：{assumed or '缺字段'}"
            return
        exchange = live.exchange_key(info.neg_risk, info.yield_bearing)
        cached = self.allowance.get(exchange)
        # (1.154.15: an allowance without a limit is not spent by an order — not read again for ALLOWANCE_LONG_MS: outside the
        # hours that keep it fresh, warm, the read was a round trip to the chain between an offer and its order)
        if cached is None or self.now() - cached[0] > (ALLOWANCE_LONG_MS if cached[1] >= ALLOWANCE_UNLIMITED else ALLOWANCE_MS):
            cached = (self.now(), int(await self.owner.live.chain.allowance(exchange)))
            self.allowance[exchange] = cached
        if cached[1] < live.to_wei(usd):
            w["word"] = "交易所授权额度不足"
            return
        await self.api.ensure_auth()
        if self.settings != settings:  # 1.154.26 (audit 5): saved anew during the two waits above — as in look
            w["word"] = "设置刚改过：这张单不下了，下一轮按新设置重新看"
            return
        now = self.now()
        # (the real record alone — never the one handed in when that is the paper one: an order there would be followed by nothing)
        rec = (self.record(slug, "on") or (rec if rec is not None and rec.get("mode") == "on" else None)
               or self.new_record(slug, w, rule, int(outcome.get("index_set") or 0), mode="on"))
        # (a record its watch made — save_watch, keep_watch — knows no outcome yet: without it the positions check would not take the
        # fills for this taker's, 1.154.16: filled_by_outcome goes by the record's index set)
        rec["index_set"] = int(rec.get("index_set") or 0) or int(outcome.get("index_set") or 0)
        take = min(take, D(str(self.settings.shares_of(rule.market))) - D(str(held(rec)))).to_integral_value(rounding="ROUND_FLOOR")
        if take < 1 or float(take * limit) < 1.0:  # (the window's shares as they stand now: this order was sized before the reads above)
            return
        # before the start, always: nothing of it matchable once the window runs — or, bought in the window's course (1.154.14), before its end
        expires = (self.end_of(w) if course else w["start"]) // 1000 - EXPIRY_BEFORE_S
        # 1.154.55: ...but in the start's last two minutes, where Predict would take no such order: as short as it takes one (TAIL_LIFE_S)
        tail = not course and self.settings.tail and expires * 1000 - now < PREDICT_MIN_LIFE_MS + LIFE_SPARE_MS
        if tail:
            expires = now // 1000 + TAIL_LIFE_S
            # (...and for no more than stands at the price: its look may have been a moment before these two minutes, where an
            # order is for all the window may buy — 1.154.15)
            take = min(take, sum((q for _, q in levels), D(0))).to_integral_value(rounding="ROUND_FLOOR")
            if take < 1 or float(take * limit) < 1.0:
                return
        amounts = live.limit_amounts(True, live.to_wei(limit), live.to_wei(take))
        wallet = self.owner.live.wallet
        identity = f"rule-v1|{self.owner.config.live_chain}|{wallet.maker.lower()}|{info.market_id}|{outcome['token']}|{w['start']}|{len(rec['orders'])}"
        salt = int.from_bytes(hashlib.sha256(identity.encode()).digest()[:8], "big") % live.MAX_SALT
        signed = live.build_order(wallet, outcome["token"], amounts["maker"], amounts["taker"], info.fee_bps, expires, salt=salt)
        digest, signature = wallet.sign_order(signed, info.neg_risk, info.yield_bearing)
        book_ts, seen_wall = w.get("book_at") or (0, 0)
        order = {"hash": digest.lower(), "id": "", "state": "placing", "shares": float(take), "limit": float(limit), "filled": 0.0,
                 "avg": None, "fee_bps": info.fee_bps, "at": now, "expires": expires,
                 "book": [[float(p), float(q)] for p, q in levels[:5]],
                 "book_ts": int(book_ts), "seen_wall": int(seen_wall),  # 1.154.13: Predict's stamp on the book it is sized on, and its arrival
                 # 1.154.15: whether its look answered a book that had just come (else the order waited for something else — a hold
                 # ended, the index — and its time from that book says nothing of speed), when that look began, the order signed
                 "fresh": bool(w.get("fresh")), "look_wall": int(w.get("look_wall") or 0), "signed_wall": int(time.time() * 1000),
                 # 1.154.22: matching seen open only after the offer was in hand — from when the offer was first seen to that sight, and
                 # how many reads of the status it took (none of it when matching was known open before the offer showed)
                 **({"open_wait": int(w["open_wall"]) - int(w["rush_wall"]), "open_reads": int(w.get("rush_reads") or 0)}
                    if w.get("open_wall") and w.get("rush_wall") and not w.get("open_told") else {}),
                 "seen": float(sum(q for _, q in levels)),  # what the book in hand offered at the price (the order may ask for more)
                 # 1.154.14: placed in the window's course. 1.154.24: ...by a look the index's arrival brought — when it arrived
                 **({"live": True, **({"index_wall": int(w["index_wall"])} if w.get("index_wall") else {})} if course else {}),
                 **({"tail": True} if tail else {})}  # 1.154.55: signed to outlive the start (its last two minutes): follow, tail_alarm
        w["open_told"] = True  # (the wait for matching to open is the first order's)
        rec["orders"].append(order)
        self.save(rec)
        self.decided[order["hash"]] = settings
        body = {"data": {"order": {**signed, "signature": signature, "hash": digest}, "strategy": "LIMIT",
                         "pricePerShare": str(amounts["price_per_share"])}}
        # 1.154.12: the request itself — the hash onto the disk, the POST, its answer — goes beside the other windows' (send). The
        # looks still go one by one and the record above is made within this one, so the next window's look counts this order (the
        # day's cap) while it is on its way; the tick waits for every send before it follows the orders ("placing" = in flight)
        # 1.154.22: ...and it starts now — its line in the journal, the request handed to its thread — not when the tick next
        # waits for something: the looks of the windows after this one (and their reads) stood between an order signed and its
        # request (signed_wall to sent_wall). 1.154.23: within this very call (start_now: Python 3.12's eager start), not a turn
        # of the loop on; on an older Python the tick lets go of the loop once for it, as 1.154.22 did
        task = start_now(self.send(slug, w, rule, rec, order, body, take, limit), "rules-send")
        if self.collect is not None:
            self.collect.append(task)
            if not EAGER:
                await asyncio.sleep(0)
        else:
            await task

    async def send(self, slug: str, w: dict, rule: Rule, rec: dict, order: dict, body: dict, take: D, limit: D) -> None:
        """One prepared order out: its hash on the disk first (a crash in between is found by it), then the request; Predict's answer
        onto the record. Run beside the other windows' sends of the same tick (place)."""
        import live
        import preopen
        # the hash where a restart finds it, before the request: a crash in between is found by it. 1.154.15: a line in the taker's own
        # journal (journal) — the store's flush waited for every record the whole bot had queued, in a thread it first had to wait
        # for; the flush only where there is no such file (a store in memory)
        # (1.154.23: a store with no writer of its own — one in memory — has nothing queued to wait for: no thread asked, so the
        # request is made in this very turn there too)
        if not self.journal(rec, order) and not (getattr(self.store, "writer", True) is None or await asyncio.to_thread(self.store.flush)):
            order.update(state="not_sent", error="数据库写入未完成，没有发单")
            self.decided.pop(order["hash"], None)
            self.save(rec)
            return
        self.sending[order["hash"]] = slug
        order["sent_wall"] = int(time.time() * 1000)  # 1.154.13: the request leaves (speed_stats)
        try:
            result = await self.api.create_order(body)
            order.update(id=str(result.get("order_id") or ""), state="open", next_ms=self.now() + FILL_READ_MS)
            # 1.154.33 (an outside review, R07): Predict's own hash, where its answer gives one other than the one signed here, is the
            # one its reads answer to — kept, as the model trader and the pre-open mode keep it (api_hash); read by the local one
            # alone, such an order was found by no read (and its rest, withdrawn after a read, never withdrawn)
            platform = str(result.get("hash") or "").lower()
            if platform and platform != order["hash"]:
                order["api_hash"] = platform
        except preopen.NotSent as error:
            order.update(state="not_sent", error=core.clean_error(error))
            order.pop("sent_wall", None)  # (it never left: not one of the orders timed — speed_stats)
        except Exception as error:
            text = core.clean_error(error) or type(error).__name__
            status = live.http_status(error)
            definite = isinstance(error, live.OrderRejected) or live.refused_status(status)  # (1.154.76: not a 408, P2-1)
            if isinstance(error, RuntimeError) and "shutdown" in str(error):
                # 1.154.23: the threads are gone (the process is ending: "cannot schedule new futures after shutdown") — the
                # request was never handed to one, let alone sent. Not "unknown": that holds the window as ordered until its start
                order.update(state="not_sent", error="进程正在停止，请求没有发出")
            elif status == 429 or "限流冷却中" in text:
                order.update(state="not_sent", error=text)  # the rate limiter, not the order: not one of MAX_REJECTS (1.153.3)
            elif definite and status != 409 and not any(word in text.lower() for word in preopen.DUPLICATE_WORDS):
                order.update(state="rejected", error=text)
            else:  # a timeout, a lost answer: it may exist — looked for by its hash, never sent again
                # 1.154.26 (audit 3): a bare 409 Conflict too — Predict, or a gateway before it, holding that hash, the answer the
                # model trader has taken so since 1.150.0 (a taker taken and filled at once, then "failed": its shares outside
                # every count). Here it was a refusal: the window's shares freed, the next look sending a second order beside
                # one that may exist. Looked for by its hash at once (FILL_READ_MS), not in ten seconds
                # (``conflict``: Predict did answer — an order it then cannot show by its hash for CONFLICT_GRACE_MS was a
                # refusal after all, follow_order; one whose answer went missing waits for its expiry, as before)
                order.update(state="unknown", error=text, next_ms=self.now() + (FILL_READ_MS if status == 409 else self.again(order, self.now(), UNKNOWN_EVERY_MS)),
                             **({"conflict": True} if status == 409 else {}))
        finally:
            self.sending.pop(order["hash"], None)
            self.decided.pop(order["hash"], None)
            order["answered_wall"] = int(time.time() * 1000)
        asked = bool(order.pop("cancel_asked", False))
        if order["state"] == "open" and order.get("id") and (self.halted() or asked):
            # 1.154.26: real trading was stopped while the request was out (the emergency stop, a pause, the mode): its rest is
            # asked to be withdrawn now, by the id this answer brought — before its first read (which may lag or fail), not
            # FILL_READ_MS on. 1.154.72 (an outside audit, P1-5): ...and so is one an operator's /live cancel all found on its
            # way (withdraw_all: cancel_asked), the taker going on — it was left to its own course, withdrawn 1.5 s later
            await self.withdraw_one(order)
        self.save(rec)
        w["word"] = ("盘中 " if order.get("live") else "") + {
            "open": f"已下单：{rule.word} ≤{int(limit * 100)}¢ × {take:g} 份", "unknown": f"下单结果未知（{order.get('error')}），按哈希核对"}.get(
            order["state"], f"没下成：{order.get('error') or order['state']}")
        if order["state"] in NEVER_PLACED:
            self.note(f"{rec['name']} 没下成：{order.get('error') or order['state']}")
            # (1.154.26: no half minute's pause for an order its own last check held back because what it was decided on had
            # moved — the settings saved anew, the index back across the start in a window's course: nothing was refused, and
            # the next look judges the same things first — it sends nothing while they stand, and buys the moment they do not)
            held_back = order["state"] == "not_sent" and str(order.get("error") or "").startswith(RECHECKED)
            w["held_back"] = int(w.get("held_back") or 0) + 1 if held_back else 0
            if not held_back or w["held_back"] >= HELD_BACK_MAX:  # (HELD_BACK_MAX of them in a row: paused all the same)
                w["held_back"] = 0
                w["look_ms"] = self.now() + REJECT_PAUSE_MS  # not again for half a minute (a refusal repeated every two seconds helps nobody)
            else:
                w["look_ms"] = 0  # ...looked at again at the next tick: under the settings as they are now, on the index as it stands then
        else:
            w.pop("held_back", None)

    def journal_path(self) -> str:
        path = str(getattr(self.store, "path", "") or "")
        return "" if not path or path == ":memory:" else path + JOURNAL_SUFFIX

    def journal(self, rec: dict, order: dict) -> bool:
        """A real order's record in a line of the taker's journal file, before its request leaves (1.154.15): what a restart needs
        to look for it by its hash should the process end before the store has it (recover). A write to an open file — no wait
        for the store's writer, no thread. False without such a file (the caller flushes the store, as before)."""
        if self.journal_fd is None:
            return False
        # (after a line that was cut short this one begins on a new line: the cut one's own end may not have been written either)
        line = (b"\n" if self.journal_cut else b"") + (json.dumps(
            {"key": rid(rec["slug"], rec["mode"]), "rec": {k: v for k, v in rec.items() if k != "orders"}, "order": order},
            ensure_ascii=False) + "\n").encode("utf-8")
        try:
            wrote = os.write(self.journal_fd, line)
        except OSError as error:
            self.note(f"订单日志写入失败（{type(error).__name__}），改为等数据库写完再发单")
            return False
        if wrote != len(line):
            # 1.154.26 (audit 11): a line cut short (a full disk, a quota — os.write says how much it wrote and raises nothing)
            # was taken for written: the order went out with nothing a restart could find it by (recover skips a cut line), the
            # store's own write maybe failing for the same reason. The cut line ended, so the next one stands on its own (and,
            # should that newline not get in either, the next line begins with one: journal_cut), and the order waits for
            # the store instead (send)
            self.journal_cut = True
            with contextlib.suppress(OSError):
                if os.write(self.journal_fd, b"\n") == 1:
                    self.journal_cut = False
            self.note(f"订单日志只写进 {wrote}/{len(line)} 字节（磁盘满了？），改为等数据库写完再发单")
            return False
        self.journal_cut = False
        return True

    def recover(self) -> None:
        """At the start: the journal's orders the records do not hold — a request that left as the process ended, its record not
        yet written by the store — onto their records as orders whose answer is unknown (looked for by their hash, never sent
        again); then, the store written, the journal begun anew."""
        path = self.journal_path()
        if not path:
            return
        try:
            if os.path.exists(path):
                with open(path, encoding="utf-8", errors="replace") as lines:
                    for line in lines:
                        try:
                            entry = json.loads(line)
                            key, order = entry.get("key"), entry.get("order") or {}
                            kept = self.recs.get(key)
                            rec = kept or {**(entry.get("rec") or {}), "orders": []}
                            old = kept is None and int(rec.get("start_ms") or 0) < self.now() - KEEP_DAYS * core.DAY_MS
                            if not key or not order.get("hash") or not rec.get("slug") or any(o.get("hash") == order["hash"] for o in rec["orders"]):
                                continue
                        except (ValueError, TypeError, AttributeError, KeyError):
                            # a line cut short: its request never left (the line is written whole before it) — or, 1.154.26, one
                            # that is no entry at all (another shape): passed over, never the taker's start (and the bot's) ended by it
                            continue
                        if old:
                            # 1.154.26 (the audit of 11-18, 25): the journal is begun anew only at a start, so one process running
                            # past KEEP_DAYS holds the lines of orders whose finished records plan has since removed — taken here
                            # for orders the store never got: back as "unknown", read by their hashes, their fills announced
                            # again and their window settled once more with today as its day (the loss stop's figure). A
                            # record that old and gone was finished, not lost
                            continue
                        rec["orders"].append({**order, "state": "unknown", "error": "重启前发出的单，记录里还没有：按哈希核对", "next_ms": 0})
                        with contextlib.suppress(TypeError, ValueError, AttributeError):
                            # 1.154.76 (an outside audit, P1-6): the store's copy may be the record its watch made, which knows no outcome
                            # until its order (place sets it, then journals): the journal's copy knows it. Left at 0, this order's fills
                            # were nobody's to the positions check (filled_by_outcome, unverified: by market and index set)
                            for k in ("index_set", "market_id"):
                                if not rec.get(k) and (entry.get("rec") or {}).get(k):
                                    rec[k] = int(entry["rec"][k]) if k == "index_set" else str(entry["rec"][k])
                        self.recs[key] = rec
                        self.store.put(PREFIX + key, rec)
                        LOG.warning("规则吃单：订单日志里有一张记录里没有的单（%s），按哈希核对", order["hash"][:12])
            written = self.store.flush()
            self.journal_fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | (os.O_TRUNC if written else 0), 0o600)
        except OSError as error:
            LOG.warning("规则吃单：订单日志打不开（%s），发单前改为等数据库写完", type(error).__name__)

    # -- the orders --
    async def follow(self) -> None:
        """Each real order to Predict's final word: read FILL_READ_MS after its answer, its rest withdrawn, read again until
        final; an order whose answer went missing looked for by its hash until GIVE_UP_MS past its signed expiry. While the
        taker's key cools down its reads wait (they are the key's) — 1.154.33 (an outside review, CR-08): and its withdrawals go
        on the bot's own client meanwhile (withdraw_cooling), as the stop's do; they waited too, the rest standing on the book
        the whole cooldown (a 429: twenty seconds and more) at the rule's price — in a window's course at its own."""
        import live
        now = self.now()
        for slug, rec in list(self.recs.items()):
            if rec.get("mode") != "on" or not in_flight(rec):
                continue
            for order in rec["orders"]:
                if order.get("paper"):
                    continue
                due = order.get("state") not in FINAL_STATES and now >= int(order.get("next_ms") or 0)
                # (1.154.55: an order of the start's last two minutes still out as its window starts is told in a pass that does not
                # read it too — its read not due yet, the key cooling down — on what is known of it; read, it is told after the read)
                if (not due or self.cooling() > 0) and self.tail_alarm(rec, order, now):
                    self.save(rec)
                if not due:
                    continue
                if self.cooling() > 0:
                    if await self.withdraw_cooling(order, now):
                        self.save(rec)
                    continue
                try:
                    await self.follow_order(rec, order, now)
                except Exception as error:
                    order["error"] = core.clean_error(error) or type(error).__name__
                    order["next_ms"] = now + self.again(order, now, UNKNOWN_EVERY_MS)
                self.tail_alarm(rec, order, now)  # (...and when this read found it off the book)
                self.save(rec)

    async def follow_order(self, rec: dict, order: dict, now: int) -> None:
        """One read of a real order: its fills taken; filled in full, final; off the book (withdrawn, cancelled, expired) it is
        final only by a read LATE_FILL_MS after it left (a fill matched just before may show late), still on it its rest is
        withdrawn. Past its signed expiry by GIVE_UP_MS it is final whatever its state, on the fills known (1.153.3)."""
        import live
        state = order["state"]
        if state == "placing":  # a request that left with the process gone before its answer (a restart): looked for by its hash
            order.update(state="unknown", next_ms=now)
            state = "unknown"
        late = now >= int(order.get("expires") or 0) * 1000 + GIVE_UP_MS
        self.reads += 1
        try:  # (1.154.33, an outside review, R07: by Predict's own hash where its answer gave one — send)
            row = await self.api.order(order.get("id") or "", order.get("api_hash") or order["hash"])
        except asyncio.CancelledError:
            raise
        except Exception:
            if not late:
                await self.withdraw_unread(order, now)  # (R07: its rest withdrawn all the same — the read's failure then, as ever)
            raise
        if row is None:
            if late:
                if state == "unknown" and not order.get("id"):
                    order.update(state="not_found", error="按哈希一直查不到：视为没下成")
                else:
                    order.update(state="final", error=f"到期后 {GIVE_UP_MS // 60_000} 分钟仍查不到这张单：按已知成交 {float(order.get('filled') or 0):g} 份记")
            elif state == "unknown" and not order.get("id") and order.get("conflict"):
                # 1.154.26: Predict answered 409 for it — it processed the request, so the order is there or it was refused, and
                # its record by the hash tells which. Not to be found CONFLICT_MISSES times over CONFLICT_GRACE_MS (a record
                # may lag its order by moments, not by a minute and a half): refused. Held as unknown until its expiry and a
                # quarter hour more, a 409 that was a refusal kept the whole window — bought in neither phase — and read its
                # hash every ten seconds all that time (five hours of it for an order of the makers' moment)
                order["misses"] = int(order.get("misses") or 0) + 1
                if order["misses"] >= CONFLICT_MISSES and now - int(order.get("at") or now) >= CONFLICT_GRACE_MS:
                    order.update(state="not_found", error=f"Predict 答复 409，之后按哈希查了 {order['misses']} 次都没有这张单：视为被拒、没下成")
                    self.note(f"{rec.get('name') or rec.get('slug')} 那张答复 409 的单按哈希查不到，视为没下成（这一场可以再买）")
                else:
                    order["next_ms"] = now + self.again(order, now, UNKNOWN_EVERY_MS)
            else:
                await self.withdraw_unread(order, now)  # (R07: an order with an id Predict's read cannot show — its rest withdrawn by the id)
                order["next_ms"] = now + (self.again(order, now, UNKNOWN_EVERY_MS) if order["state"] == "unknown" else FOLLOW_EVERY_MS)
            return
        order["id"] = str(row.get("id") or order.get("id") or "")
        self.take_fills(rec, order, row)
        if float(order.get("filled") or 0) >= float(order["shares"]) - DUST:
            order["state"] = "final"
            return
        if live.order_final(row, float(order["shares"])) or order.get("removed"):  # off the book
            order["left_ms"] = int(order.get("left_ms") or now)
            if now - order["left_ms"] >= LATE_FILL_MS or late:
                order["state"] = "final"
            else:
                order.update(state="closing", next_ms=order["left_ms"] + LATE_FILL_MS)
            return
        if late:
            order.update(state="final", error="到期后仍显示挂着：按已知成交记")
            return
        if order.get("id"):  # still open: the rest withdrawn (a taker never waits on the book)
            if self.rest_due(order, now, NOOP_AGAIN_MS):
                await self.withdraw_rest(order, now)
            else:  # (1.154.33, R01: Predict said "nothing to remove" a moment ago and still shows it open — read again when it is asked again)
                order.update(state="cancelling", next_ms=max(now + FOLLOW_EVERY_MS, int(order.get("noop_ms") or 0) + self.noop_wait(order, now)))
        else:
            order["next_ms"] = now + self.again(order, now, UNKNOWN_EVERY_MS)

    def clients(self) -> list:
        """The taker's client, then the bot's own (owner.live.api) when it is another: a request the first fails (its key cooling
        down, an error) goes on the second — the orders are the same account's on either (withdraw_all's removals, 1.154.26)."""
        main = getattr(getattr(self.owner, "live", None), "api", None)
        return [self.api] + ([main] if main is not None and main is not self.api else [])

    def spare_client(self) -> Any:
        """The bot's own client when it is another than the taker's, can withdraw orders and is not in a cooldown of its own: the
        way to Predict while the taker's key cools down (withdraw_cooling)."""
        main = self.clients()[1:]
        if not main or not callable(getattr(main[0], "remove_orders", None)):
            return None
        return None if float(getattr(main[0], "blocked_until", 0.0) or 0.0) > time.monotonic() else main[0]

    async def remove_ids(self, ids: list[str], clients: list | None = None) -> dict:
        """POST /orders/remove for ``ids`` on the first of ``clients`` (clients() by default) that answers: its answer; the last
        failure raised when none does. 1.154.33 (an outside review, CR-08): withdraw_one and the follow-up asked the taker's
        client alone (withdraw_all already went on to the bot's own) — in its key's cooldown an order answered after the stop
        stood until the cooldown was over."""
        failure: BaseException | None = None
        for api in clients or self.clients():
            if not callable(getattr(api, "remove_orders", None)):
                continue
            try:
                return await api.remove_orders(ids)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                failure = error
        raise failure or core.RemoteError("没有可用的撤单客户端")

    async def look_up(self, order: dict) -> tuple[dict | None, str]:
        """An order whose placement's answer went missing, looked for by its hash on the first client that answers (clients()):
        (its record — None when Predict knows nothing of it —, "") or (None, why every client failed). 1.154.33 (an outside
        review, CR-08): on the taker's client alone, before — in its key's cooldown the stop found nothing and said 查不到."""
        why = ""
        for api in self.clients():
            if not callable(getattr(api, "order", None)):
                continue
            try:
                return await api.order("", order.get("api_hash") or order["hash"]), ""
            except asyncio.CancelledError:
                raise
            except Exception as error:
                why = core.clean_error(error) or type(error).__name__
        return None, why or "没有可用的查询客户端"

    @staticmethod
    def tail_out(order: dict, now: int) -> bool:
        """An order of the start's last two minutes (1.154.55) whose signature still lasts: it may stand on the book once its
        window runs, so nothing of it waits — its rest asked to be withdrawn, an answer gone missing looked for, every
        TAIL_AGAIN_MS (again). Past its signed expiry it is an order like any other (it can fill no more)."""
        return bool(order.get("tail")) and now < int(order.get("expires") or 0) * 1000

    @classmethod
    def again(cls, order: dict, now: int, wait_ms: int) -> int:
        """``wait_ms`` — or, for an order that may stand past its window's start (tail_out), TAIL_AGAIN_MS when that is sooner."""
        return min(wait_ms, TAIL_AGAIN_MS) if cls.tail_out(order, now) else wait_ms

    @classmethod
    def noop_wait(cls, order: dict, now: int | None = None) -> int:
        """How long after Predict's last "nothing to remove" the order is asked again: NOOP_AGAIN_MS, doubled for each such answer
        in a row, NOOP_MAX_MS at the most (withdraw_rest counts them: noops) — TAIL_AGAIN_MS for an order that may stand past
        its window's start (``now`` given: tail_out)."""
        wait = min(NOOP_AGAIN_MS * 2 ** max(int(order.get("noops") or 0) - 1, 0), NOOP_MAX_MS)
        return wait if now is None else cls.again(order, now, wait)

    @classmethod
    def rest_due(cls, order: dict, now: int, again_ms: int) -> bool:
        """Whether an order's rest is to be asked to be withdrawn now: an id to withdraw it by, its removal not confirmed by
        Predict, and no "nothing to remove" from Predict within ``again_ms`` (withdraw_rest) — or within noop_wait, when longer
        (both TAIL_AGAIN_MS at the most for an order that may stand past its window's start, 1.154.55)."""
        return (bool(order.get("id")) and not order.get("removed")
                and now - int(order.get("noop_ms") or 0) >= max(cls.again(order, now, again_ms), cls.noop_wait(order, now)))

    def tail_alarm(self, rec: dict, order: dict, now: int) -> bool:
        """An order of the start's last two minutes not known off the book when its window starts (1.154.55): told at once —
        what of it may still be taken, at what price, until when — and told again when it is off the book or its signature
        has run out. The withdrawals go on every TAIL_AGAIN_MS meanwhile (follow). True when something was told (the order's
        record changed: to be saved)."""
        if not order.get("tail") or order.get("paper"):
            return False
        rest = max(float(order.get("shares") or 0) - float(order.get("filled") or 0), 0.0)
        out = order.get("state") in REST_STATES and not order.get("removed") and rest > DUST
        name = rec.get("name") or rec.get("slug")
        if out and not order.get("tail_told") and int(rec.get("start_ms") or 0) <= now < int(order.get("expires") or 0) * 1000:
            order["tail_told"] = now
            until = dt.datetime.fromtimestamp(int(order["expires"]), core.BEIJING).strftime("%H:%M:%S")
            self.notify(f"⚠️ 规则吃单：{name} 已经开场，开场前最后 2 分钟下的一张单还没确认撤掉——可能还挂着 {rest:g} 份、买价 ≤{round(float(order.get('limit') or 0) * 100)}¢，"
                        f"签名 {until} 到期。机器人每秒在重试撤单；到期前有人按这个价卖过来就会成交。也可以到 Predict 上手动撤。",
                        key=f"rule-tail:{order.get('hash')}")
            return True
        if order.get("tail_told") and not order.get("tail_clear") and (not out or now >= int(order.get("expires") or 0) * 1000):
            order["tail_clear"] = now
            self.notify(f"✅ 规则吃单：{name} 那张单" + ("已经不在簿上" if not out else "的签名已到期，不会再成交")
                        + f"（共成交 {float(order.get('filled') or 0):g}/{float(order.get('shares') or 0):g} 份）", key=f"rule-tail-clear:{order.get('hash')}")
            return True
        return False

    async def withdraw_rest(self, order: dict, now: int, clients: list | None = None) -> None:
        """An order's rest asked to be withdrawn by its id (remove_ids): off the book on Predict's word that it removed it — final
        by a read LATE_FILL_MS on (follow_order). 1.154.33 (an outside review, R01): "nothing to remove" (noop) is no such word.
        It was taken for one: an order Predict answered so while it still stood (one a moment old, as withdraw_one has it) was
        ended LATE_FILL_MS on though its reads showed it open, never asked again — the window's shares freed beside it (a second
        order for them) and its later fills booked nowhere. Now its next read decides: ended or filled, it goes the way of any
        order off the book; still open, it is asked again (NOOP_AGAIN_MS on)."""
        oid = str(order["id"])
        result = await self.remove_ids([oid], clients)
        if oid in {str(i) for i in (result.get("removed") or [])}:
            order.update(removed=now, left_ms=now)
        elif oid in {str(i) for i in (result.get("noop") or [])}:
            order.update(noop_ms=now, noops=int(order.get("noops") or 0) + 1)  # (noop_wait: the next ask waits longer each time)
        order.update(state="cancelling", next_ms=now + FOLLOW_EVERY_MS)

    async def withdraw_unread(self, order: dict, now: int) -> None:
        """1.154.33 (an outside review, R07): an order with an id whose read failed or found nothing (Predict's record lagging, a
        hash its reads do not answer to) — its rest asked to be withdrawn by that id all the same: a taker never waits on the
        book, and the withdrawal came only after a read that showed the order open, so reads that kept failing left the rest
        standing (to its signed expiry at the worst). At most every BLIND_AGAIN_MS after a "nothing to remove"; a failure of its
        own is noted on the order, never raised (the read's is)."""
        if order.get("state") not in REST_STATES or not self.rest_due(order, now, BLIND_AGAIN_MS):
            return
        try:
            await self.withdraw_rest(order, now)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            order["error"] = core.clean_error(error) or type(error).__name__

    async def withdraw_cooling(self, order: dict, now: int) -> bool:
        """An order's rest withdrawn on the bot's own client while the taker's key cools down (follow; 1.154.33, CR-08): one that
        may stand on the book (REST_STATES, an id, its removal not confirmed), at most every BLIND_AGAIN_MS after a "nothing to
        remove" (no read goes with it: the reads are the key's). True when it was asked."""
        spare = self.spare_client()
        if spare is None or order.get("state") not in REST_STATES or not self.rest_due(order, now, BLIND_AGAIN_MS):
            return False
        try:
            await self.withdraw_rest(order, now, [spare])
        except asyncio.CancelledError:
            raise
        except Exception as error:
            order.update(error=core.clean_error(error) or type(error).__name__, next_ms=now + FOLLOW_EVERY_MS)
        return True

    def halted(self) -> bool:
        """No real order may be opened now by the operator's word or the bot's own stop: the mode not 真实吃单, LIVE not on, a
        pause (/live pause, the emergency stop), the day's stop. (Not the passing reasons block also knows — a cooldown, the
        balance: those say nothing against an order already out.)"""
        owner, c = self.owner, self.owner.config
        return bool(self.settings.mode != "on" or c.live_mode != "on" or not getattr(c, "live_allowed", True)
                    or owner.live_paused() or owner.live_killed())

    async def withdraw_one(self, order: dict) -> None:
        """An order answered while real trading is stopped (send: halted): its rest asked to be withdrawn at once, by its id.
        Off the book only on Predict's word that it removed it — "nothing to remove" for an order a moment old proves
        nothing; else it is read at the next tick, and asked again (follow_order). 1.154.33 (an outside review, CR-08): on the
        bot's own client when the taker's fails (remove_ids) — in its key's cooldown the order stood until the cooldown ended."""
        now = self.now()
        try:
            result = await self.remove_ids([str(order["id"])])
        except asyncio.CancelledError:
            raise
        except Exception as error:
            order.update(error=core.clean_error(error) or type(error).__name__, next_ms=0)
            return
        if str(order["id"]) in {str(i) for i in (result.get("removed") or [])}:
            order.update(removed=now, left_ms=now, state="cancelling", next_ms=now + FOLLOW_EVERY_MS)
        else:
            order["next_ms"] = 0

    async def withdraw_all(self, why: str) -> str:
        """The emergency stop's and /live cancel all's pass over this taker's real orders (1.154.26, audit 1). They were in nobody's
        list but follow's: the stop paused real trading, withdrew the model trader's and the pre-open mode's orders and answered
        没有真实挂单可撤 — a rule order on the book stood until its own next read (FILL_READ_MS after its answer, then every
        second; ten while a read failed or the key cooled; at the worst until its signed expiry). Now: an order whose answer
        went missing is looked for by its hash here and now; every one with an order id still on the book is asked to be
        withdrawn, on the taker's key or, that failing, on the bot's own; one whose request is still out has no id to
        withdraw by — withdrawn the moment its answer brings one (send: halted). The line for the reply — "没撤成" in it for
        what could not be withdrawn, "结果未知" for an order that may exist and could not be found (neither is a stop that
        worked: LiveBot.live_control) — and "" when the taker has no real order out. 1.154.33 (an outside review): Predict's
        "nothing to remove" is no confirmation (R01: said apart, read at the next tick and asked again while the order shows
        open — not a miss either: most often an order that filled or ended a moment before); the look-ups go on the bot's own
        client too, and one that failed on both is said as such, not as 查不到 (CR-08)."""
        out = [(rec, o) for rec in self.recs.values() if rec.get("mode") == "on"
               for o in rec.get("orders") or [] if not o.get("paper") and o.get("state") not in FINAL_STATES]
        if not out:
            return ""
        for _, o in out:
            if not o.get("id") and o.get("state") == "placing":
                # 1.154.72 (an outside audit, P1-5): "应答一到就撤" — the answer's id withdraws it at once (send). 1.154.76 (a second
                # audit, P1-1): marked here, before any wait of this pass — marked after them, an answer that came during the
                # look-ups or the removals below found no mark: its order rested, in neither list, and the reply did not name it
                o["cancel_asked"] = True
        lost = [(rec, o) for rec, o in out if not o.get("id") and o.get("state") == "unknown" and o.get("hash")]
        unread: dict[int, str] = {}  # id(order) -> why its look-up failed on every client (CR-08)
        if lost:
            self.reads += len(lost)
            found = await asyncio.gather(*(self.look_up(o) for _, o in lost), return_exceptions=True)
            for (rec, o), got in zip(lost, found):
                if isinstance(got, asyncio.CancelledError):
                    raise got
                if isinstance(got, BaseException):
                    unread[id(o)] = core.clean_error(got) or type(got).__name__
                    continue
                row, why_not = got
                if why_not:
                    unread[id(o)] = why_not
                elif isinstance(row, dict) and row.get("id") not in (None, "") and not o.get("id"):
                    o["id"] = str(row["id"])  # found: withdrawn below (its fills are follow's to read, as ever)
        resting = [(rec, o) for rec, o in out if o.get("id") and not o.get("removed") and o.get("state") in REST_STATES]
        done: set[str] = set()
        quiet: set[str] = set()  # Predict's "nothing to remove" (R01)
        error = ""
        if resting:
            try:  # (the taker's client, then the bot's own: remove_ids)
                result = await self.remove_ids([str(o["id"]) for _, o in resting])
            except asyncio.CancelledError:
                raise
            except Exception as failed:
                error = core.clean_error(failed) or type(failed).__name__
            else:
                done = {str(i) for i in (result.get("removed") or [])}
                quiet = {str(i) for i in (result.get("noop") or [])} - done
        now, off, gone = self.now(), 0, 0
        for rec, o in resting:
            if o.get("state") in FINAL_STATES or o.get("removed") or o.get("state") == "closing":
                off += 1  # ended, or taken off by the taker's own follow-up, while this pass waited for its answer: left as it is
                continue
            if str(o["id"]) in done:  # (as follow_order has it: off the book, final by a read LATE_FILL_MS on — its fills are still to be read)
                o.update(removed=now, left_ms=int(o.get("left_ms") or now), state="cancelling", next_ms=now + FOLLOW_EVERY_MS)
                off += 1
            else:
                o["next_ms"] = 0  # read, and asked again, at the taker's next tick (a "nothing to remove" too: R01)
                if str(o["id"]) in quiet:
                    gone += 1
            self.save(rec)
        unknown = [(rec, o) for rec, o in out if not o.get("id") and o.get("state") == "unknown"]
        flying = [(rec, o) for rec, o in out if not o.get("id") and o.get("state") == "placing"]
        for rec, o in unknown:
            o["next_ms"] = 0
            self.save(rec)
        parts = []
        if resting:
            missed = len(resting) - off - gone
            parts.append(f"{len(resting)} 张挂着的单已请求撤回，Predict 确认 {off} 张"
                         + (f"，{gone} 张 Predict 称已不在簿上，下一轮读回确认" if gone else "")
                         + (f"，{missed} 张没撤成（{error or 'Predict 的应答里没有它'}），规则吃单每秒再撤" if missed else ""))
        if unknown:
            failed_ones = [unread[id(o)] for _, o in unknown if id(o) in unread]
            if len(failed_ones) < len(unknown):
                parts.append(f"{len(unknown) - len(failed_ones)} 张下单结果未知，按哈希现在查不到（可能没下成，也可能还挂着）：规则吃单接着核对，一查到就撤")
            if failed_ones:
                parts.append(f"{len(failed_ones)} 张下单结果未知，按哈希查询失败（{failed_ones[-1]}）：规则吃单接着核对，一查到就撤")
        if flying:
            parts.append(f"{len(flying)} 张还在发送途中，应答一到就撤")
        if not parts:
            return ""
        LOG.warning("规则吃单：%s——%s", why, "；".join(parts))
        return "规则吃单：" + "；".join(parts) + "。"

    def take_fills(self, rec: dict, order: dict, row: dict) -> None:
        """An order record's fills onto ours: the shares, the average price it states (its limit while none is stated)."""
        import live
        shares, _ = live.order_fill(row, float(order["shares"]))
        price = live.order_price(row)
        before = float(order.get("filled") or 0)
        if price is not None and shares >= before - DUST:
            # 1.154.72 (an outside audit, P2): the average price goes with the shares it was stated for — a record lagging behind
            # one read before (10 shares at 45¢ after 40 at 54¢) kept the 40 shares and took the 45¢: $21.60 spent booked as $18.00
            order.update(avg=price, avg_stated=True)
        if shares > before + DUST:
            order["filled"] = shares
            order["fill_wall"] = int(time.time() * 1000)  # 1.154.72: when the taker learned of it (pending_usd: spent after the balance was read)
            avg = float(order.get("avg") or order["limit"])
            rule = RULE_OF.get(rec.get("rule"))
            self.notify(f"🎯 规则吃单{'·盘中' if order.get('live') else ''}成交：{rec['name']} 买{rule.word if rule else rec.get('side')} {avg * 100:.1f}¢ × {shares - before:g} 份"
                        f"（累计 {shares:g}/{order['shares']:g}；{rule.name if rule else ''}{'盘中' if order.get('live') else ''} ≤{round(float(order.get('limit') or 0) * 100)}¢）")

    # -- the results --
    async def settle(self) -> None:
        """Each window with fills, on Predict's result: SETTLE_AFTER_MS past its end, then every SETTLE_EVERY_MS until it is in.
        A window whose orders all ended without a fill needs none."""
        now = self.now()
        for slug, rec in list(self.recs.items()):
            if rec.get("pnl") is not None or in_flight(rec) or filled(rec) <= DUST:
                continue
            if now < int(rec.get("end_ms") or 0) + SETTLE_AFTER_MS or now - int(rec.get("settle_checked") or 0) < SETTLE_EVERY_MS:
                continue
            if self.cooling() > 0:
                return
            rec["settle_checked"] = now
            try:
                self.reads += 1
                market = await self.api.market(str(rec["market_id"]))
                resolution = core.predict_resolution(market)
            except Exception as error:
                rec["settle_error"] = core.clean_error(error) or type(error).__name__
                self.save(rec)
                continue
            if not resolution:
                self.save(rec)
                continue
            if not core.PredictFeed.market_settled(market if isinstance(market, dict) else {}):
                # 1.154.77 (an outside audit's "to verify" 1): a result named before Predict calls the market settled may still be a
                # proposal — booked then, a result that flips later stood in the P&L and the day's loss stop. Settled on both words,
                # as the redeem does (RDM-OPEN-SHAPES); asked again every SETTLE_EVERY_MS meanwhile
                rec["settle_error"] = f"Predict 已写结果（{resolution.get('name') or '?'}），但市场状态还不是已结算：等结算"
                self.save(rec)
                continue
            import live
            name = str(resolution.get("name") or "").lower()
            up = 0.5 if resolution.get("split") else 1.0 if name in live.UP_WORDS else 0.0 if name in live.DOWN_WORDS else None
            if up is None:
                rec["settle_error"] = f"结果名称认不出：{resolution.get('name')}"
                self.save(rec)
                continue
            pay = up if rec["side"] == "up" else 1 - up
            pnl = net(rec) * pay - cost(rec)  # the shares held (the fee taken in shares) paid out, less the USDT spent (1.153.3)
            rec.update(result="split" if up == 0.5 else "up" if up == 1.0 else "down", pnl=round(pnl, 6), settled_ms=now, settled_day=day_of(now))
            self.save(rec)
            # (1.154.26: the day's P&L of the loss stop with this result in it, for the next tick's orders — summed here, at the
            # tick's end; with no loss stop set there is nothing to keep)
            if rec.get("mode") == "on" and self.owner.config.live_max_daily_loss_usd:
                self.sum_pnl(now)
                # 1.154.76 (an outside audit of 1.154.74, P1-2): the day's stop judged on it now — the model trader's step ran it, and with
                # SIM=off no step runs: the orders were held back (block), the stop itself (its notice, the resting orders withdrawn,
                # the record that keeps it for the day) never came, and a win later took the figure back over the line
                check = getattr(self.owner, "live_check_kill", None)
                if pnl < -DUST and callable(check):
                    with contextlib.suppress(Exception):
                        check(self.owner.sim_trades(), now)
            word = "赢" if pnl > DUST else "输" if pnl < -DUST else "平"
            rule = RULE_OF.get(rec.get("rule"))
            self.notify(f"{'✅' if pnl > DUST else '❌' if pnl < -DUST else '➖'} 规则吃单结算{'（只记录）' if rec.get('mode') == 'paper' else ''}："
                        f"{rec['name']} 买{rule.word if rule else rec['side']} {filled(rec):g} 份，结果{'涨' if up == 1.0 else '跌' if up == 0.0 else '平'}，"
                        f"{word} {money(pnl)}")
            if rec.get("mode") == "on" and pay > 0 and callable(soon := getattr(self.owner, "live_redeem_soon", None)):
                with contextlib.suppress(Exception):
                    soon(rec["market_id"])

    # -- what it says --
    def fast_line(self, now: int) -> str:
        """The per-second hours in a line (/rules): the hours, whether they run, what the looks go on, how the REST reads beside the
        pushes fared; which key the taker's requests use."""
        s = self.settings
        key = "｜请求用规则吃单自己的 Key（RULE_API_KEY）" if self.own_key else ""
        lending = getattr(self.owner, "key_lending", None)  # 1.154.21: the key lent while this taker is idle (preopen.KeyLending)
        if self.own_key and lending is not None and lending.key():
            why = lending.why()
            key += "，空闲时借给盘口和预挂（RULE_KEY_LEND）：" + ("现在借出中，要用之前 5 分钟收回" if not why else f"现在不借——{why}")
        lead = int(getattr(s, "lead_minutes", 0) or 0)
        ahead = sorted((self.moment(w), w["start"]) for w in self.track.values() if self.moment(w) and now - self.moment(w) < SPRINT_MS)
        moment = (f"｜对手提前 {lead} 分钟挂单的时刻前后也每秒检查（RULE_LEAD_MINUTES）"
                  + (f"，下一个 {bj(ahead[0][0], False)}（{bj(ahead[0][1], False)} 的场）" if ahead else "")) if lead else ""
        if not s.fast:
            text, wait = "每秒检查时段：关（RULE_FAST_HOURS=off）", None
        else:
            text, wait = f"每秒检查时段（北京时间）：{core.day_spans_text(s.fast)}", s.fast_in(now)
        if self.fast:
            listed = sum(1 for w in self.track.values() if w["market"] is not None)
            lag, trip = (statistics.median(self.stream.lags) if self.stream.lags else None), (statistics.median(self.verify_ms) if self.verify_ms else None)
            text += (f"｜进行中{'（有场次在盘中）' if any(self.phase(w, now) == 'live' for w in self.track.values()) else ''}：每秒看 {listed} 场（已看 {self.fast_looks} 次）；"
                     + ("推送正常" if self.stream.alive() else "推送断开，靠 REST 轮流读")
                     + (f"，推送延迟中位 {lag:.0f} ms" if lag is not None else "")
                     + f"；REST 核对 {self.verifies} 次" + (f"，{self.verify_diffs} 次发现推送没送到的变化" if self.verify_diffs else "")
                     + (f"，一次往返中位 {trip:.0f} ms" if trip is not None else "")
                     + (f"；等撮合开启时读状态 {self.status_reads} 次" if self.status_reads else "")
                     + (f"；核对读取失败：{self.verify_error}" if self.verify_error else "")
                     + (f"；机器人主循环延迟近 1 分钟中位 {statistics.median(self.loop_lags):.0f} ms、最长 {max(self.loop_lags):.0f} ms" if self.loop_lags else ""))
        elif wait is not None:
            text += f"｜下一次 {bj(now + wait, False)} 开始" + ("，正在准备（登录、授权额度、撮合状态、推送订阅）" if wait <= WARM_MS else "，提前 1 分钟准备")
        else:
            text += "：推送一有变化就看，另外每 30 秒读一次、开场前 15 分钟每 2 秒"
        return text + moment + key

    def first_line(self, now: int) -> str:
        """The rule taker first, in a line (/rules, 1.154.23): when it goes first, what that means, whether it does now. "" unless
        real orders are on."""
        s = self.settings
        if s.mode != "on":
            return ""
        clock = lambda minutes: f"{minutes % 1440 // 60:02d}:{minutes % 60:02d}"
        hours = "、".join(f"{clock(a - WARM_MS // 60_000)}–{clock(b)}" for a, b in s.fast) if s.fast else ""
        lead = int(getattr(s, "lead_minutes", 0) or 0)
        in_course = any(self.live_cents(r) > 0 for r in RULES)
        when = "、".join(x for x in (f"每天 {hours}（每秒检查时段和它前 {WARM_MS // 60_000} 分钟）" if hours else "", "对手挂单时刻前后和盘中吃单时" if lead or not hours else "盘中吃单时") if x)
        what = ("盘口一推到就在同一回合里看、签单、把请求交出去（不等主循环轮到）" if EAGER else f"当场处理需要 Python 3.12（现在 {sys.version_info[0]}.{sys.version_info[1]}）：盘口到了仍等主循环轮到"
                ) + ("；盘中的场次，指数一到也当场看（不等下一秒的检查）" if in_course else "") + "；请求走规则吃单自己的线程" + ("；这把 Key 不外借" if self.own_key else "")
        # (1.154.24: the seconds in which the rest of the bot keeps out of the way — quiet_ms)
        ways = (([f"对手挂单时刻前 {QUIET_BEFORE_MS // 1000} 秒到后 {QUIET_AFTER_MS // 1000} 秒"] if lead else [])
                + ([f"盘中开头（开场后 {(LIVE_START_MS - QUIET_BEFORE_MS) // 1000}～{(LIVE_START_MS + COURSE_QUIET_MS) // 1000} 秒）"] if in_course else [])
                + ["卖单已到而撮合还没开启时"])
        what += f"；{'、'.join(ways)}（都要这一场还能买），预挂不发新单、不处理账户快照，原策略的整步和每分钟的核对顺延（快车道照常）"
        if self.first_now(now):
            quiet = self.quiet_ms(now)
            state = "现在：优先中" + (f"，其他活儿正在让路（还有 {quiet / 1000:.0f} 秒）" if quiet else "") + (f"；本次启动当场处理 {self.first_ticks} 次" if self.first_ticks else "")
        else:
            wait = s.fast_in(now)
            state = "现在不在优先时段" + (f"，下一次 {bj(now + max(0, wait - WARM_MS), False)} 开始" if wait else "")
        if self.index_looks or self.index_late:
            state += f"；盘中由指数带来的检查 {self.index_looks} 次" + (
                f"（另有 {self.index_late} 次指数到达时已超过 {INDEX_FRESH_MS / 1000:g} 秒，留给每秒的检查——常见于行情连接卡顿后补发；一直很多就是本机时钟和币安差得远）" if self.index_late else "")
        ahead = self.first_plan(now)
        return f"优先：{when}，规则吃单排最前——{what}" + (f"｜{ahead}" if ahead else "") + f"｜{state}"

    def first_plan(self, now: int) -> str:
        """What the taker goes first for in the day ahead, in a few words (/rules, 1.154.24): the makers' moments still to
        come — of the windows it buys before their start — and the courses still to run, for the rules that buy there
        (quarter hours on end told as one stretch)."""
        s, moments, spans = self.settings, set(), []
        for w in sorted(self.track.values(), key=lambda w: w["start"]):
            rule = RULE_OF[w["rule"]]
            at, end = self.moment(w), self.end_of(w)
            if at and now < at + QUIET_AFTER_MS and s.price(rule.key) > 0:
                moments.add(at)
            if self.live_cents(rule) > 0 and now < end - LIVE_STOP_MS:
                if spans and spans[-1][1] >= w["start"]:
                    spans[-1][1] = max(spans[-1][1], end)
                else:
                    spans.append([w["start"], end])
        def told(items: list) -> str:  # (the day named where it changes: a time alone reads as today's)
            out, day = [], day_of(now)
            for at, text in items:
                out.append((bj(at)[:6] if day_of(at) != day else "") + text)
                day = day_of(at)
            return "、".join(out)
        parts = []
        if moments:
            shown = sorted(moments)[:8]
            parts.append("盘前抢单时刻 " + told([(at, bj(at, False)) for at in shown]) + (f" 等 {len(moments)} 个" if len(moments) > len(shown) else ""))
        if spans:
            # (a stretch that reaches the end of what is planned may go on beyond it: told by its beginning)
            whole = (self.planned_ms or now) + HORIZON_MS  # (a window starting before this is in the plan)
            parts.append("盘中 " + told([(a, f"{bj(a, False)}–{bj(b, False)}" if b < whole else f"{bj(a, False)} 起") for a, b in spans[:4]])
                         + (f" 等 {len(spans)} 段" if len(spans) > 4 else ""))
        return "接下来一天：" + "；".join(parts) if parts else ""

    def net_line(self) -> str:
        """How far Predict is, in a line (/rules, 1.154.17) — 11-18 02:00: an order that left a tenth of a second after its book came
        found the offers gone; what was left of the half second was the way there and back. The region the bot runs in (Railway's
        variable), the handshake of its new connections to Predict's host (TCP + TLS: two or three round trips to whatever
        answers there), the site an answer came through (cf-ray), and this taker's own requests' round trips — the request alone,
        not its wait for the key's pace. "" before any."""
        host = core.urllib.parse.urlsplit(str(getattr(self.api, "base", "") or "")).hostname or ""
        net = core.HTTP_NET.get(host) or {}
        trips = list(getattr(self.api, "trips", ()) or ())
        reads, writes = [ms for method, ms in trips if method == "GET"], [ms for method, ms in trips if method != "GET"]
        if not reads and not writes and not net.get("connect"):
            return ""
        spread = lambda xs: f"中位 {statistics.median(xs):.0f} ms（最快 {min(xs):.0f}）"
        connect = list(net.get("connect") or ())
        parts = [f"机器人在 {os.environ.get('RAILWAY_REPLICA_REGION') or '未知区域'}"]
        if connect:
            parts.append(f"到 {host} 新建连接 {spread(connect)}，是 2～3 个来回")
        if net.get("edge"):
            parts.append(f"入口 {net['edge']}")
        if reads:
            parts.append(f"读一次 {spread(reads)}")
        if writes:
            parts.append(f"下单 / 撤单一次 {spread(writes)}")
        hint = ""
        if reads and statistics.median(reads) >= 150:
            near = connect and statistics.median(connect) < 60
            hint = ("｜入口很近、请求却慢：Predict 的服务器离这个区域远（或它处理慢）" if near else "｜一次请求这么久，多半是离 Predict 的服务器远") \
                   + "——换一个部署区域试：在那个区域新建一个服务、启动命令填 python -u rules.py latency，量出来再比"
        return "网络（不含排队）：" + "｜".join(parts) + hint

    def speed_line(self, now: int) -> str:
        """The real orders against the clock, in a line (/rules, 1.154.13; the fastest of each, 1.154.15); "" before any."""
        sp = speed_stats(self.recs, now - 30 * core.DAY_MS)
        if not sp:
            return ""
        best = sp.get("best") or {}
        ms = lambda key: "—" if sp.get(key) is None else f"{sp[key]:.0f} ms" + (f"（最快 {best[key]:.0f}）" if best.get(key) is not None else "")
        cs = course_speed(self.recs, now - 30 * core.DAY_MS)  # 1.154.24: the course's orders the index brought
        return (f"下单速度（近 30 天真实订单 {sp['orders']} 张，{sp['filled']} 张有成交）：盘口变化→推送到达 中位 {ms('lag')}、"
                f"到达→发出 {ms('own')}" + (f"［其中到达→开始处理 {sp['wake']:.0f} ms］" if sp.get("wake") is not None else "")
                + f"、发出→应答 {ms('trip')}"
                + (f"｜盘中由指数带来的 {cs['orders']} 张：指数到达→发出 中位 {cs['own']:.0f} ms（最快 {cs['best']:.0f}）" if cs else ""))

    def status(self) -> str:
        now, s = self.now(), self.settings
        recs = self.recs
        in_course = any(self.live_cents(r) > 0 for r in RULES)
        lines = [f"规则吃单：{MODES.get(s.mode, s.mode)}（RULE_TAKER={s.mode}）｜{'开场前吃' if in_course else '只在开场前吃'}，盘口一有就买，"
                 + (f"到开场前 {STOP_MS // 1000} 秒（卖单在等撮合开启时，开场前 {TAIL_RUSH_MS // 60_000} 分钟起每 {STATUS_RUSH_S:g} 秒左右查一次撮合；"
                    f"最后 {ORDER_BEFORE_MS // 1000} 秒里的单签成约 2 分钟后到期——Predict 不收更短的——所以只按盘口现有的份数下，"
                    f"没成交的部分照常 {FILL_READ_MS / 1000:g} 秒后撤、没确认撤掉就每秒再撤，开场时还没撤掉会马上通知；RULE_TAIL=off 关掉）" if s.tail else
                    f"到开场前 {ORDER_BEFORE_MS // 1000} 秒（订单签到开场前 {EXPIRY_BEFORE_S} 秒到期，Predict 不收有效期不足 2 分钟的单；最后 2 分钟也吃：RULE_TAIL=on）")
                 + (f"；设了盘中价格的规则开场后接着买（开场 {LIVE_START_MS // 1000} 秒后到结束前 {ORDER_BEFORE_MS // 1000} 秒，"
                    f"第 1 分钟最高按盘前的价格）" if in_course else "")
                 + (f"｜每天最多花 ${s.max_day_usd:g}（今天已花 ${spent_on(recs, day_of(now), s.mode):,.2f}）" if s.max_day_usd else "")
                 + f"｜推送 {'已连接' if self.stream.ws is not None else '未连接' + ('（' + self.stream.error + '）' if self.stream.error else '')}"]
        if s.mode == "on" and (why := self.block(0.0)):
            lines.append(f"⏸ 现在不下单：{why}")
        if s.mode != "off":
            lines.append(self.fast_line(now))
            if first := self.first_line(now):
                lines.append(first)
            if speed := self.speed_line(now):
                lines.append(speed)
            if net := self.net_line():
                lines.append(net)
        for market, name in MARKET_NAMES.items():
            lines.append(f"{name}：{'开' if s.on(market) else '关'}" + (f"｜每场最多 {s.shares_of(market):g} 份" if s.on(market) else "（这个市场的规则都不做）"))
            if not s.on(market):
                continue
            for rule in (r for r in RULES if r.market == market):
                cents, during = s.price(rule.key), self.live_cents(rule)
                if cents <= 0 and during <= 0:
                    lines.append(f"· {rule.name}：不做（价格设为 0）")
                    continue
                nxt = min((w for w in self.track.values() if w["rule"] == rule.key), key=lambda w: w["start"], default=None)
                st = watch_stats(recs, rule.key, s.mode, now - 30 * core.DAY_MS) if s.mode != "off" and cents > 0 else {"watched": 0}
                cs = course_stats(recs, rule.key, s.mode, now - 30 * core.DAY_MS) if s.mode != "off" else {"windows": 0}
                lines.append(f"· {rule.name} 买{rule.word} " + (f"≤{cents}¢" if cents > 0 else "盘前不买")
                             + (f"｜盘中 ≤{during}¢（{lead_note(rule)}）" if during > 0 else "") + f"：{rule.when}"
                             + (f"｜{'这一场' if nxt['start'] <= now else '下一场'} {bj(nxt['start'])}{('：' + nxt['word']) if nxt['word'] else ''}" if nxt else "")
                             + (f"｜近 30 天看了 {st['watched']} 场，{st['offered']} 场有 ≤{cents}¢ 的卖单"
                                + (f"（平均 {st['offer_avg']:g} 份）" if st["offer_avg"] else "")
                                + (f"，最低卖价中位 {st['best_mid'] * 100:.0f}¢" if st["best_mid"] is not None else "")
                                + (f"，撮合开启在开场前中位 {st['open_lead']:.0f} 秒" if st["open_lead"] is not None else "")
                                + (f"，卖单首次出现在开场前中位 {st['offer_lead']:.0f} 分钟" if st.get("offer_lead") is not None else "")
                                + (f"，{st['lost']} 场的卖单没买到就没了（存活中位 {st['life'] / 1000:.1f} 秒）" if st.get("lost") else "")
                                + (f"，买到的 {st['won']} 场从卖单出现到被吃掉中位 {st['won_life'] / 1000:.1f} 秒" if st.get("won") else "") if st["watched"] else "")
                             + (f"｜盘中近 30 天买到 {cs['windows']} 场 {cs['shares']:g} 份（均价 {cs['cost'] / cs['shares'] * 100:.1f}¢）"
                                + (f"，已结算 {cs['settled']} 场：赢 {cs['wins']} 输 {cs['losses']}，{money(cs['pnl'])}" if cs["settled"] else "") if cs["windows"] else ""))
        if s.mode != "off" and (rows := races(recs, s.mode, now - 30 * core.DAY_MS)[:RACES_SHOWN]):
            # 1.154.16: each window's race, where the medians above are a month's — and there from the moment an offer shows, hours
            # before the window whose end the medians wait for
            lines.append(f"抢单记录（最近 {len(rows)} 场，北京时间；“开始被吃 / 被吃光”是卖单出现之后多久，按 Predict 盘口自己的时间戳——抢走它的人有多快）：")
            if summary := race_summary(races(recs, s.mode, now - 30 * core.DAY_MS)):
                lines.append(f"  近 30 天：{summary}")
            lines.extend("  " + race_text(row) for row in rows)
        today = day_of(now)
        # (1.154.26: the real windows and the paper ones apart — on a day the mode was switched the one line added shares bought
        # to shares only noted, and counted a window of both twice. By the windows' start, as before)
        for mode, word in (("on", "今天"), ("paper", "今天（只记录，没真买）")):
            mine = [r for r in recs.values() if r.get("mode") == mode and day_of(int(r.get("start_ms") or 0)) == today and filled(r) > DUST]
            if mine:
                done = [r for r in mine if r.get("pnl") is not None]
                lines.append(f"{word}：{len(mine)} 场买到 {sum(filled(r) for r in mine):g} 份，成本 ${sum(cost(r) for r in mine):,.2f}"
                             + (f"；已结算 {len(done)} 场 {money(sum(float(r['pnl']) for r in done))}" if done else ""))
        last = sorted((r for r in recs.values() if filled(r) > DUST), key=lambda r: -int(r.get("start_ms") or 0))[:5]
        for r in last:
            lines.append(f"  {bj(int(r['start_ms']))} {(RULE_OF.get(r['rule']) or RULES[0]).name} 买{'涨' if r['side'] == 'up' else '跌'} "
                         f"{avg_price(r) * 100:.1f}¢×{filled(r):g}" + (f"（其中盘中 {course_part(r)[0]:g} 份）" if course_part(r)[0] > DUST else "")
                         + ("（只记录）" if r.get("mode") == "paper" else "")
                         + (f" → {money(float(r['pnl']))}" if r.get("pnl") is not None else "（待结算）"))
        if self.error:
            lines.append(f"最近问题：{self.error}")
        return "\n".join(lines)


# --- how far Predict is from wherever this runs (1.154.17) ------------------------------------------------------------------------
async def latency_probe(key: str, out: Callable[[str], None] = print, rounds: int = 10) -> dict:
    """``python -u rules.py latency`` — read-only, no order: the way to Predict from the machine this runs on, to be compared
    between deploy regions (11-18: an order that left a tenth of a second after its book came lost the race; the other four
    tenths were the network). On the bot's own request path: a first request (a new connection: its handshake), then
    ``rounds`` reads of an order book on that connection; the push connection's handshake, the library's ping there and back,
    and a subscription's first book. Run it as a service of its own in the region to try (start command
    ``python -u rules.py latency``, PREDICT_API_KEY or RULE_API_KEY set) and compare its "读盘口" with the 网络 line of the
    running bot's /rules. What it measured, for the tests."""
    host = core.urllib.parse.urlsplit(core.PREDICT_REST).hostname or ""
    headers = {"x-api-key": key} if key else {}
    spread = lambda xs: f"中位 {statistics.median(xs):.0f} ms（最快 {min(xs):.0f}，最慢 {max(xs):.0f}）" if xs else "没量到"
    found: dict = {"region": os.environ.get("RAILWAY_REPLICA_REGION") or "", "reads": [], "pings": [], "first": None, "ws_open": None, "ws_book": None}
    out(f"Predict 网络延迟探测（只读，不下单）｜区域 {found['region'] or '未知'}｜版本 {core.VERSION}" + ("" if key else "｜没有设置 API Key：可能被拒绝"))
    card = next((c for c in core.WINDOW_UPDOWN_MARKETS if c.stem == "btc" and c.minutes == 15), core.WINDOW_UPDOWN_MARKETS[0])
    now = int(time.time() * 1000)
    slug, market_id = card.slug_of(now - now % card.window_ms), ""  # the BTC quarter hour running now: a market that is there
    try:
        began = time.perf_counter()
        answer = json.loads(await core.predict_blocking(core._http_get, f"{core.PREDICT_REST}/categories/{slug}", None, 10, headers))
        found["first"] = (time.perf_counter() - began) * 1000
        markets = (answer.get("data", answer) if isinstance(answer, dict) else {}).get("markets") or []
        market_id = str(markets[0].get("id") or "") if markets else ""
        out(f"第一次请求（含新建连接）{found['first']:.0f} ms｜市场 {slug} → {market_id or '没找到'}")
    except Exception as error:
        out(f"读市场失败：{core.clean_error(error) or type(error).__name__}")
    path = f"/markets/{market_id}/orderbook" if market_id else f"/categories/{slug}"
    refused: list[float] = []  # requests Predict answered with a refusal: there and back all the same
    why = ""
    for _ in range(rounds):
        began = time.perf_counter()
        try:
            await core.predict_blocking(core._http_get, core.PREDICT_REST + path, None, 10, headers)
            found["reads"].append((time.perf_counter() - began) * 1000)
        except Exception as error:
            why = core.clean_error(error) or type(error).__name__
            if why.startswith("HTTP "):
                refused.append((time.perf_counter() - began) * 1000)
        await asyncio.sleep(0.4)  # (well inside a key's pace)
    found["refused"] = refused
    net = core.HTTP_NET.get(host) or {}
    connect = list(net.get("connect") or ())
    out((f"读盘口 {len(found['reads'])} 次（同一条连接）：{spread(found['reads'])}" if found["reads"] or not refused else
         f"读盘口被拒绝（{why}：没带 Key 或 Key 不对），{len(refused)} 次被拒的应答来回 {spread(refused)}——也是一次往返，可以拿来比")
        + (f"｜新建连接（TCP+TLS，2～3 个来回）{spread(connect)}" if connect else "") + (f"｜入口 {net['edge']}" if net.get("edge") else "")
        + (f"｜另有读取失败：{why}" if why and found["reads"] else ""))
    try:
        began = time.perf_counter()
        ws = await core.ws_connect(core.PREDICT_WS_URL, headers)
        found["ws_open"] = (time.perf_counter() - began) * 1000
        try:
            for _ in range(5):
                began = time.perf_counter()
                await asyncio.wait_for(await ws.ping(), 5)
                found["pings"].append((time.perf_counter() - began) * 1000)
                await asyncio.sleep(0.2)
            if market_id:
                began = time.perf_counter()
                await ws.send(json.dumps({"method": "subscribe", "requestId": 1, "params": [f"predictOrderbook/{market_id}"]}))
                while time.perf_counter() - began < 5:
                    frame = json.loads(await asyncio.wait_for(ws.recv(), 5))
                    if isinstance(frame, dict) and frame.get("type") == "M" and str(frame.get("topic") or "").startswith("predictOrderbook/"):
                        found["ws_book"] = (time.perf_counter() - began) * 1000
                        break
        finally:
            await ws.close()
        out(f"推送连接：握手 {found['ws_open']:.0f} ms｜ping 来回 {spread(found['pings'])}"
            + (f"｜订阅到第一份盘口 {found['ws_book']:.0f} ms" if found["ws_book"] is not None else ""))
    except Exception as error:
        out(f"推送连接失败：{core.clean_error(error) or type(error).__name__}")
    out("怎么看：把这里的“读盘口”中位数和线上机器人 /rules 里“网络”一行的“读一次”比——哪个区域小，哪个区域离 Predict 近；"
        "下单的往返和它差不多。测完可以把这个服务删掉。")
    return found


if __name__ == "__main__":
    import sys
    if sys.argv[1:2] == ["latency"]:
        asyncio.run(latency_probe(os.environ.get("RULE_API_KEY", "").strip() or os.environ.get("PREDICT_API_KEY", "").strip()))
        while os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_SERVICE_ID"):
            time.sleep(3600)  # (a service on Railway is restarted when its process ends: once measured, it waits to be deleted)
    else:
        print("用法：python -u rules.py latency   —— 量一下这台机器到 Predict 的网络延迟（只读，不下单；需要 PREDICT_API_KEY 或 RULE_API_KEY）")
