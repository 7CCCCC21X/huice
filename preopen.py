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
"""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import hashlib
import json
import logging
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import main as core

LOG = logging.getLogger(__name__)
TERMINAL = {"removed", "not_sent", "rejected", "not_found", "final"}  # states an order cannot leave: nothing is left to cancel
NEVER_PLACED = {"not_sent", "rejected", "not_found"}                  # ...of which these never tied money up
RETRY_MS, RETRY_MAX_MS = 15_000, 120_000  # a closing pair's cancels and reads are tried again this long after a try, doubling to the cap
GIVE_UP_MS = 15 * 60_000  # after a window's start + this, an order Predict cannot show by its hash is taken as never placed (it expired before the start)
SPRINT_MS = 2_000         # the last stretch (past the cancel time, before the expiry): an order still open is asked to be cancelled this often, no backoff
SEND_MIN_LEFT_MS = 60_000  # an order is not sent with less than this left to its expiry
DUPLICATE_WORDS = ("already exist", "already placed", "duplicate", "exists")  # Predict has the order: not a refusal
LISTING_FIELDS = ("id", "conditionId", "categorySlug", "status", "tradingStatus", "isVisible", "makerFeeBps", "question", "outcomes")


def final_order(row: dict) -> bool:
    status = str(row.get("status") or "").upper()
    amount, filled = D(str(row.get("amount") or "0")), D(str(row.get("amountFilled") or "0"))
    return amount > 0 and (filled >= amount or status in {
        "FILLED", "CANCELLED", "CANCELED", "EXPIRED", "INVALIDATED", "REJECTED", "FAILED", "CLOSED", "COMPLETED", "DONE", "SETTLED", "RESOLVED"})


def order_verified(order: dict) -> bool:
    """Whether the order's money is accounted for: never placed, or its final record read."""
    return order["state"] in NEVER_PLACED or order["state"] == "final"


Settings = core.PreopenSettings  # the settings live in Config (PREOPEN_*, the control page's values included): Settings.from_config(config)


class Ledger:
    """No background writer or structural re-key: an acknowledged record is on disk."""
    def __init__(self, path: str):
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("CREATE TABLE IF NOT EXISTS pairs (slug TEXT PRIMARY KEY, body TEXT NOT NULL)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, body TEXT NOT NULL)")
        self.conn.commit()
        self.rows = {slug: json.loads(body) for slug, body in self.conn.execute("SELECT slug,body FROM pairs")}

    def save(self, row: dict) -> None:
        body = json.dumps(row, ensure_ascii=False)
        with self.conn:
            self.conn.execute("INSERT INTO pairs VALUES (?,?) ON CONFLICT(slug) DO UPDATE SET body=excluded.body", (row["slug"], body))
        self.rows[row["slug"]] = row

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
    """A PredictApi for this mode: no pooled transport (a sendall failure cannot prove zero delivery, so no automatic
    POST retry), every answer checked for success, ``before_send`` judged right before an order's bytes go out, and the
    rate-limit cooldown shared with ``shared`` (the bot's own client): a 429 on either pauses both."""
    import live

    class Api(live.PredictApi):
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
            data = None if payload is None else json.dumps(payload).encode()
            headers = self.headers()
            if data is not None:
                headers["Content-Type"] = "application/json"
            url = self.base + path
            try:
                return await core.predict_blocking(core._http_urllib, url, data, headers, timeout, core.urllib.parse.urlsplit(url).hostname or "")
            except core.RemoteError as error:
                if error.retry_after or live.http_status(error) == 429:
                    self.blocked_until = max(self.blocked_until, time.monotonic() + max(error.retry_after, self.BACKOFF_SECONDS))
                raise

        async def call(self, *args: Any, **kwargs: Any) -> Any:
            answer = await super().call(*args, **kwargs)
            if not isinstance(answer, dict) or answer.get("success") is not True or "data" not in answer:
                raise core.RemoteError("Predict 应答不是已确认成功的数据，保留订单状态与额度")
            return answer

    return Api(base, key, wallet)


def candidate_problem(market: dict, now_ms: int, config: Any, settings: Settings) -> str:
    """Why a listed market is not one of this mode's ("" when it is): only exact, listed slugs of the configured cards,
    starting today, within PREOPEN_AHEAD_MINUTES and not within PREOPEN's entry seconds, registered, visible, open or
    awaiting matching, with no maker fee, with an id, a condition and a question that agrees with the slug's time."""
    slug = str(market.get("categorySlug") or market.get("slug") or "")
    for rolling in core.WINDOW_UPDOWN_MARKETS:
        prefix = f"{rolling.stem}-updown-15m-"
        suffix = slug.removeprefix(prefix)
        if not slug.startswith(prefix) or not suffix.isdigit():
            continue
        if rolling.key not in settings.markets:
            return "不在 PREOPEN_MARKETS 里"
        start = int(suffix) * 1000
        if start % rolling.window_ms:
            return "链接里的时间不是整刻钟"
        if start - now_ms < settings.entry_seconds * 1000:
            return "已开始或不足 3 分钟"
        if start - now_ms > settings.ahead_minutes * 60_000:
            return f"开始还在 {settings.ahead_minutes} 分钟之外（之后再扫）"
        today = dt.datetime.fromtimestamp(now_ms / 1000, core.BEIJING).date()
        if dt.datetime.fromtimestamp(start / 1000, core.BEIJING).date() != today:
            return "不是北京时间当天的场次"
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
        spec = rolling.window_spec(start)
        checker = SimpleNamespace(predict=SimpleNamespace(info={slug: {"question": str(market.get("question") or market.get("title") or "")}}))
        if core.Bot.window_title_problem(checker, spec):
            return "问题里的时间段和链接对不上"
        return ""
    return "不是三张 15 分钟卡的链接"


def candidate(market: dict, now_ms: int, config: Any, settings: Settings) -> dict | None:
    """The pair to place for a listed market (candidate_problem says why not), or None."""
    if candidate_problem(market, now_ms, config, settings):
        return None
    slug = str(market.get("categorySlug") or market.get("slug") or "")
    rolling = next(r for r in core.WINDOW_UPDOWN_MARKETS if slug.startswith(f"{r.stem}-updown-15m-"))
    start = int(slug.removeprefix(f"{rolling.stem}-updown-15m-")) * 1000
    spec = rolling.window_spec(start)
    return {"slug": slug, "key": rolling.key, "market_id": str(market["id"]), "start_ms": start,
            "cancel_ms": start - settings.cancel_seconds * 1000, "expires": start // 1000 - settings.expiry_seconds,
            "day": dt.datetime.fromtimestamp(now_ms / 1000, core.BEIJING).date().isoformat(), "name": spec.name}


async def list_page(api: Any, variant: str, after: str = "", first: int = 100) -> dict:
    query: dict = {"first": first, "marketVariant": variant}
    if after:
        query["after"] = after
    answer = await api.call("GET", "/markets?" + core.urllib.parse.urlencode(query))
    if not isinstance(answer["data"], list):
        raise core.RemoteError("市场列表格式异常")
    return answer


async def probe(api: Any, config: Any, settings: Settings, now_ms: int) -> list[str]:
    """One read-only page of each listing, read as the discovery reads it: the fields the rows carry, the ones the
    discovery needs that are missing, and how many rows would be placed now — run before switching the mode on."""
    lines = [f"开盘前预挂探测（只读；设置：{'、'.join(sorted(settings.markets))}，{core.cents(float(settings.price))}×{settings.shares} 份，"
             f"提前 {settings.ahead_minutes} 分钟内）"]
    for variant in ("UP_DOWN", "CRYPTO_UP_DOWN"):
        try:
            answer = await list_page(api, variant, first=20)
        except Exception as error:
            lines.append(f"{variant}：读取失败：{core.clean_error(error) or type(error).__name__}")
            continue
        rows = [r for r in answer["data"] if isinstance(r, dict)]
        cursor = answer.get("cursor")
        if not rows:
            lines.append(f"{variant}：0 行（游标 {cursor!r}）")
            continue
        fields = sorted({k for r in rows for k in r})
        missing = [f for f in LISTING_FIELDS if not any(f in r for r in rows)]
        problems: dict[str, int] = {}
        ok = 0
        for r in rows:
            why = candidate_problem(r, now_ms, config, settings)
            if why:
                problems[why] = problems.get(why, 0) + 1
            else:
                ok += 1
        lines.append(f"{variant}：{len(rows)} 行，游标 {type(cursor).__name__}；字段：{'、'.join(fields)}")
        lines.append(f"  发现逻辑要用的字段缺失：{'、'.join(missing) if missing else '无'}；现在会挂的：{ok} 场"
                     + (f"；不挂的原因：" + "；".join(f"{why}×{n}" for why, n in sorted(problems.items(), key=lambda kv: -kv[1])) if problems else ""))
        slugs = [str(r.get("categorySlug") or r.get("slug") or "?") for r in rows[:4]]
        lines.append("  前几行：" + "、".join(slugs))
    return lines


class Controller:
    """Discovery and cancellation run independently; a placement backlog cannot delay cancels."""
    def __init__(self, owner: Any):
        import live
        self.owner = owner
        self.settings = Settings.from_config(owner.config)
        self.ledger = Ledger(self.settings.db)
        self.error = ""
        self.alive = True
        self.stop = asyncio.Event()  # this mode's own stop (the page switching it off, a settings change): its run() withdraws the orders on the way out
        self.manual_pause = self.ledger.pause()
        self.account_at = 0
        self.balance = 0.0
        self.open_reserved = 0.0
        self.listed_ids: set[str] = set()
        self.unmanaged = False
        self.placing: set[str] = set()
        self.cancelling: set[str] = set()
        self.queue: asyncio.PriorityQueue = asyncio.PriorityQueue()  # (start_ms, slug, candidate): the soonest window first
        self.queued: set[str] = set()
        self.account_lock = asyncio.Lock()
        shared = getattr(owner.live, "api", None)  # the bot's own client: one rate-limit cooldown for all three
        self.api = safe_api(live.API_BASES[owner.config.live_chain], owner.config.predict_api_key, owner.live.wallet, self.before_send, shared)
        # A separate request gate leaves cancellation capacity available even when
        # placement requests are queued or their HTTP responses are slow.
        self.cancel_api = safe_api(live.API_BASES[owner.config.live_chain], owner.config.predict_api_key, owner.live.wallet, None, shared)
        self.settling: dict[str, asyncio.Task] = {}
        self.settle_gate = asyncio.Semaphore(2)

    def now(self) -> int:
        return self.owner.market.now_ms()

    def stopped(self) -> bool:
        return self.owner.stopping.is_set() or self.stop.is_set()

    def blocked(self) -> str:
        c = self.owner.config
        if self.stopped():
            return "进程或预挂模式停止中"
        if c.live_mode != "on" or not c.live_allowed or self.manual_pause:
            return "真实交易或开盘前策略未开启"
        reason = self.owner.live_paused() or self.owner.live_killed() or ("账户未就绪" if not self.owner.live.ready else "")
        if reason:
            return reason
        cap = c.live_max_daily_loss_usd
        if cap and self.daily_pnl() + self.owner.live_daily_pnl(self.owner.sim_trades(), self.now()) <= -cap:
            return "已达到今日真实亏损上限"
        return ""

    def before_send(self, body: dict) -> str:
        if why := self.blocked():
            return why
        digest = str(body.get("data", {}).get("order", {}).get("hash") or "").lower()
        pair = next((p for p in self.ledger.rows.values() if any(o.get("hash") == digest for o in p["orders"].values())), None)
        if pair is None or pair["phase"] != "active" or self.now() >= pair["cancel_ms"]:
            return "市场已进入撤单阶段，不再发送"
        if pair["expires"] * 1000 - self.now() < SEND_MIN_LEFT_MS:
            return "距订单到期不足一分钟，不再发送"
        if self.now() - self.account_at > 30_000:
            return "账户快照过期，不再发送"
        if self.risk() + self.other_risk() > min(self.settings.max_open_usd, self.owner.config.live_max_open_usd) + 1e-9:
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

    def other_risk(self) -> float:
        return self.owner.live_exposure(self.owner.sim_trades())

    def risk(self) -> float:
        """The money this mode may have tied up: every order that may exist (sent, or sent for all we know) at its full
        size, until its pair is accounted — by the final reads after a cancel (nothing filled) or by the official
        result. A refusal or a never-sent order ties up nothing."""
        return sum(self.settings.side_usd for p in self.ledger.rows.values() if not p.get("accounted")
                   for o in p["orders"].values() if o["state"] not in NEVER_PLACED)

    def filled_by_outcome(self, days: int = 3) -> dict[tuple[str, int], float]:
        """The shares this mode's orders filled, by (market id, index set), for the windows of the last ``days`` days: the
        model trader's positions reconciliation counts them as recorded, not as fills of its own orders to adopt."""
        since = self.now() - days * core.DAY_MS
        out: dict[tuple[str, int], float] = {}
        for pair in self.ledger.rows.values():
            if int(pair.get("start_ms") or 0) < since:
                continue
            for order in pair["orders"].values():
                if float(order.get("filled") or 0) > 0 and order.get("index_set"):
                    key = (str(pair["market_id"]), int(order["index_set"]))
                    out[key] = out.get(key, 0.0) + float(order["filled"])
        return out

    def daily_pnl(self) -> float:
        day = dt.datetime.fromtimestamp(self.now() / 1000, core.BEIJING).date().isoformat()
        return sum(float(p.get("pnl") or 0) for p in self.ledger.rows.values() if p.get("accounted_day") == day)

    async def account(self) -> None:
        import live
        async with self.account_lock:
            try:
                rows = await self.api.orders("OPEN")
                if not self.api.orders_complete:
                    raise core.RemoteError("账户挂单分页未读取完整，暂停新增订单")
                other = self.unmanaged_rows(rows)
                known_core = {str(t.get("live", {}).get("order_id")) for t in self.owner.sim_trades().values() if isinstance(t.get("live"), dict)}
                unmanaged = any(str(r.get("id")) not in known_core for r in other)
                reserved = 0.0
                for row in rows:
                    order = row.get("order") or {}
                    if int(order.get("side") or 0) == 0:
                        amount, filled = D(str(row.get("amount") or "0")), D(str(row.get("amountFilled") or "0"))
                        if amount <= 0 or not 0 <= filled <= amount:
                            raise core.RemoteError("账户订单缺少有效数量，暂停新增订单")
                        reserved += float(D(str(order["makerAmount"])) / live.WEI * (1 - filled / amount))
                balance = float(await self.owner.live.chain.usdt_balance()) / live.WEI
            except Exception:
                self.account_at = 0
                raise
            self.unmanaged, self.open_reserved, self.balance = unmanaged, reserved, balance
            self.listed_ids = {str(r.get("id")) for r in rows}
            by_hash = {live.order_hash_of(r): r for r in rows if live.order_hash_of(r)}
            for pair in self.ledger.rows.values():
                changed = False
                for order in pair["orders"].values():
                    row = by_hash.get(order.get("api_hash") or order["hash"])
                    if row is not None:
                        before = order["filled"]
                        self.apply_fill(order, row)
                        changed |= order["filled"] != before
                if changed:
                    self.ledger.save(pair)
            self.account_at = self.now()

    async def discover(self) -> None:
        """Scan every page; never interpret a truncated/error response as an empty catalog. The windows found go into
        the queue soonest first, and only those within PREOPEN_AHEAD_MINUTES: the day's later windows wait their turn."""
        found: list[dict] = []
        for variant in ("UP_DOWN", "CRYPTO_UP_DOWN"):
            after, seen = "", set()
            for _ in range(100):
                answer = await list_page(self.api, variant, after)
                for market in answer["data"]:
                    if not isinstance(market, dict):
                        continue
                    self.note_open(market)  # when a window of ours starts matching, as the listing shows it (the cancel time's evidence)
                    c = candidate(market, self.now(), self.owner.config, self.settings)
                    if c and c["slug"] not in self.ledger.rows and c["slug"] not in self.queued:
                        self.queued.add(c["slug"])
                        found.append(c)
                cursor = str(answer.get("cursor") or "")
                if not cursor:
                    break
                if cursor in seen or not answer["data"]:
                    raise core.RemoteError("市场列表分页没有前进")
                seen.add(cursor)
                after = cursor
            else:
                raise core.RemoteError("市场列表超过100页，下一轮重新扫描")
        for c in sorted(found, key=lambda c: (c["start_ms"], c["slug"])):
            await self.queue.put((c["start_ms"], c["slug"], c))

    async def place_pair(self, c: dict) -> None:
        import live
        if c["slug"] in self.ledger.rows or self.blocked():
            return
        # Fresh details are authoritative, not a stale listing page or model card.
        market = await self.api.market(c["market_id"])
        current = candidate(market, self.now(), self.owner.config, self.settings)
        if current is None or current["slug"] != c["slug"]:
            return
        info = live.parse_market(market, self.owner.config.predict_fee_bps)
        outcomes = [live.outcome_for_side("updown", side, info.outcomes) for side in ("up", "down")]
        if any(outcome is None or assumed for outcome, assumed in outcomes):
            raise core.RemoteError("开盘前市场的涨跌代币无法确认")
        if outcomes[0][0]["token"] == outcomes[1][0]["token"]:
            raise core.RemoteError("涨跌结果指向相同代币")
        if not await self.owner.live.chain.allowance(live.exchange_key(info.neg_risk, info.yield_bearing)) >= live.to_wei(self.settings.pair_usd):
            raise core.RemoteError("开盘前双边挂单授权额度不足")
        await self.api.ensure_auth()
        async with self.account_lock:
            # Only one coroutine reserves a pair at a time; reservations are saved
            # before the lock is released, including orders not visible on REST yet.
            if c["slug"] in self.ledger.rows or self.blocked() or self.now() - self.account_at > 30_000 or self.unmanaged:
                return
            limit = min(self.settings.max_open_usd, self.owner.config.live_max_open_usd)
            if self.risk() + self.other_risk() + self.settings.pair_usd > limit + 1e-9:
                self.error = f"预挂资金上限不足：需要再预留 {self.settings.pair_usd:g}U，限额 {limit:g}U"
                return
            if self.balance < max(self.open_reserved, self.risk()) + self.settings.pair_usd:
                self.error = "可用USDT不足以预留下一场双边订单"
                return
            if self.settings.side_usd > self.owner.config.live_max_order_usd:
                self.error = f"LIVE_MAX_ORDER_USD 小于每边 {self.settings.side_usd:g}U"
                return
            cap = self.owner.config.live_max_daily_loss_usd
            if cap and self.daily_pnl() + self.owner.live_daily_pnl(self.owner.sim_trades(), self.now()) <= -cap:
                return
            amounts = live.limit_amounts(True, live.to_wei(self.settings.price), live.to_wei(self.settings.shares))
            orders = {}
            for side, (outcome, _) in zip(("up", "down"), outcomes):
                identity = (f"preopen-v2|{self.owner.config.live_chain}|{self.owner.live.wallet.maker.lower()}|{current['market_id']}|"
                            f"{outcome['token']}|{current['expires']}|{self.settings.price}|{self.settings.shares}")
                salt = int.from_bytes(hashlib.sha256(identity.encode()).digest()[:8], "big") % live.MAX_SALT
                signed = live.build_order(self.owner.live.wallet, outcome["token"], amounts["maker"], amounts["taker"], info.fee_bps, current["expires"], salt=salt)
                digest, signature = self.owner.live.wallet.sign_order(signed, info.neg_risk, info.yield_bearing)
                orders[side] = {"hash": digest.lower(), "id": "", "state": "prepared", "filled": 0.0, "index_set": int(outcome.get("index_set") or 0),
                                "body": {"data": {"order": {**signed, "signature": signature, "hash": digest},
                                                   "strategy": "LIMIT", "pricePerShare": str(amounts["price_per_share"]),
                                                   "isPostOnly": True, "selfTradePrevention": "CANCEL_TAKER"}}}
            pair = {**current, "phase": "active", "orders": orders, "created_ms": self.now(), "tries": 0, "retry_ms": 0,
                    "open_seen_ms": self.now() if str(market.get("tradingStatus") or "").upper() == "OPEN" else 0}
            self.ledger.save(pair)  # both hashes durable before either POST
        for side in ("up", "down"):
            order = pair["orders"][side]
            if self.before_send(order["body"]):
                order["state"] = "not_sent"
                pair["phase"] = "closing"
                self.ledger.save(pair)
                break
            order["state"] = "placing"
            self.ledger.save(pair)
            try:
                result = await self.api.create_order(order["body"])
                order.update(id=result["order_id"], state="open", api_hash=str(result.get("hash") or "").lower())
            except NotSent as error:
                order.update(state="not_sent", error=core.clean_error(error))
                pair["phase"] = "closing"
            except Exception as error:
                text = core.clean_error(error) or type(error).__name__
                definite = isinstance(error, live.OrderRejected) or 400 <= live.http_status(error) < 500
                if definite and not any(w in text.lower() for w in DUPLICATE_WORDS):
                    # Predict refused it (a post-only order that would cross, an invalid field): nothing was placed, nothing
                    # to reserve or reconcile (the first revision kept such an order "unknown", and its money, for good)
                    order.update(state="rejected", error=text)
                else:
                    # A timeout, malformed answer or duplicate response does not prove
                    # no order exists. Keep its hash and full reservation; do not retry.
                    order.update(state="unknown", error=text)
                pair["phase"] = "closing"
            self.ledger.save(pair)
            if pair["phase"] != "active":
                break
        if pair["phase"] != "active" or self.now() >= pair["cancel_ms"] or self.blocked():
            await self.cancel_pair(pair, "单边未确认或已到撤单时间")
        LOG.info("开盘前预挂 %s：%s", pair["slug"], {s: o["state"] for s, o in pair["orders"].items()})

    def apply_fill(self, order: dict, row: dict) -> None:
        # Fixed shares are the actual signed shares, not an estimated count.
        amount, filled = D(str(row.get("amount") or "0")), D(str(row.get("amountFilled") or "0"))
        if amount <= 0 or filled < 0 or filled > amount:
            raise core.RemoteError("成交回报数量无效，保留风险额度")
        order["filled"] = max(float(order.get("filled") or 0), float(self.settings.shares * filled / amount))

    def gave_up(self, pair: dict) -> bool:
        """Past the window's start by GIVE_UP_MS: an order Predict cannot show cannot fill any more (it expired before
        the start), and what it shows is final."""
        return self.now() >= pair["start_ms"] + GIVE_UP_MS

    def note_open(self, market: dict) -> None:
        """The first time the listing shows one of this mode's windows OPEN (matching on): kept on the pair — how long
        before the start matching begins is what the cancel time should be set against (/preopen shows the median)."""
        slug = str(market.get("categorySlug") or market.get("slug") or "")
        pair = self.ledger.rows.get(slug)
        if pair is not None and not pair.get("open_seen_ms") and str(market.get("tradingStatus") or "").upper() == "OPEN":
            pair["open_seen_ms"] = self.now()
            self.ledger.save(pair)

    def in_sprint(self, pair: dict) -> bool:
        """The last stretch: past the cancel time, before the signed expiry, with an order that may still be on the book —
        the cancel is asked for every SPRINT_MS, whatever the count (the backoff is for after the expiry, when nothing can
        fill any more and only the records are being read)."""
        now = self.now()
        return (now >= pair["cancel_ms"] and now < pair["expires"] * 1000
                and any(o["state"] in {"open", "placing", "unknown"} for o in pair["orders"].values()))

    def bookkeep(self, pair: dict) -> None:
        """A closing or unverified pair's next try: SPRINT_MS on in the last stretch (in_sprint), else RETRY_MS after this one,
        doubling to RETRY_MAX_MS; closed once nothing is left to cancel; accounted (its money released) once every order is
        verified and nothing filled."""
        if self.in_sprint(pair):
            pair["retry_ms"] = self.now() + SPRINT_MS
        else:
            tries = int(pair.get("tries") or 0) + 1
            pair["tries"], pair["retry_ms"] = tries, self.now() + min(RETRY_MS * 2 ** (tries - 1), RETRY_MAX_MS)
        if pair["phase"] != "closed" and all(o["state"] in TERMINAL for o in pair["orders"].values()):
            pair["phase"] = "closed"
        if (not pair.get("accounted") and all(order_verified(o) for o in pair["orders"].values())
                and all(float(o.get("filled") or 0) <= 0 for o in pair["orders"].values())):
            pair.update(accounted=True, accounted_day=dt.datetime.fromtimestamp(self.now() / 1000, core.BEIJING).date().isoformat(), pnl=0.0)
        self.ledger.save(pair)

    async def read_final(self, order: dict, pair: dict) -> None:
        """An order's own record, for its final fills: final → "final"; none, past the give-up — never placed (no id) or
        final as known (an id Predict no longer answers to)."""
        row = await self.cancel_api.order(order.get("id", ""), order.get("api_hash") or order["hash"])
        if row:
            if not order.get("id") and row.get("id") is not None:
                order["id"] = str(row["id"])
            self.apply_fill(order, row)
            if final_order(row):
                order["state"] = "final"
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
                    await self.read_final(order, pair)
                    self.ledger.save(pair)
                    continue
                # Unknown submissions are reconciled by their original hash.
                if not order.get("id"):
                    row = await self.cancel_api.order("", order["hash"])
                    if row:
                        order["id"] = str(row["id"])
                        self.apply_fill(order, row)
                    elif self.gave_up(pair):
                        order.update(state="not_found", note="按哈希查不到，签名已过期：视为从未下单")
                        self.ledger.save(pair)
                        continue
                    else:
                        continue
                pair.setdefault("cancel_sent_ms", self.now())
                result = await self.cancel_api.remove_orders([order["id"]])
                if order["id"] in result["removed"]:
                    order["state"] = "removed"
                    pair.setdefault("cancel_done_ms", self.now())
                    self.ledger.save(pair)
                await self.read_final(order, pair)  # the fills before the removal (bulk removals went first: no cancel waits behind this)
                self.ledger.save(pair)
            self.bookkeep(pair)
        finally:
            self.cancelling.discard(pair["slug"])

    async def settle_pair(self, pair: dict) -> None:
        """Official resolution + fresh final fills; never settle on a local candle."""
        import live
        if pair.get("accounted") or self.now() < pair["start_ms"] + 15 * 60_000:
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
        pair.update(accounted=True, accounted_day=dt.datetime.fromtimestamp(self.now() / 1000, core.BEIJING).date().isoformat(),
                    pnl=sum(((up if side == "up" else 1 - up) - float(self.settings.price)) * float(order.get("filled") or 0)
                            for side, order in pair["orders"].items()))
        self.ledger.save(pair)

    async def monitor_pair(self, pair: dict) -> None:
        """An order ID is not proof that a deferred post-only order was accepted."""
        if (pair["phase"] != "active" or self.now() - int(pair.get("created_ms") or 0) < 10_000
                or self.now() - int(pair.get("checked_ms") or 0) < 5_000):
            return
        pair["checked_ms"] = self.now()
        rejected = False
        for order in pair["orders"].values():
            if order["state"] != "open":
                continue
            if order.get("id") in self.listed_ids and self.now() - self.account_at <= 30_000:
                continue  # one paginated account snapshot covers every healthy resting order
            row = await self.api.order(order["id"], order.get("api_hash") or order["hash"])
            if row is None:
                continue  # replication lag is not a zero-fill rejection
            self.apply_fill(order, row)
            if final_order(row):
                order["state"] = "final"
                rejected |= order["filled"] <= 0
        self.ledger.save(pair)
        if rejected:
            await self.cancel_many([pair], "一边未成交即结束，撤掉另一边")

    async def cancel_all(self, reason: str) -> None:
        await self.cancel_many([p for p in list(self.ledger.rows.values()) if p["phase"] != "closed"], reason)

    async def cancel_many(self, pairs: list[dict], reason: str) -> None:
        """Bulk removals first; slow hash/fill reads never queue known IDs behind them."""
        by_id: dict[str, tuple[dict, dict]] = {}
        for pair in pairs:
            pair.update(phase="closing", reason=reason)
            for order in pair["orders"].values():
                if order["state"] == "prepared":
                    order["state"] = "not_sent"
                elif order["state"] not in TERMINAL and order.get("id"):
                    by_id[str(order["id"])] = pair, order
            self.ledger.save(pair)
        ids = list(by_id)
        for pair, _ in by_id.values():
            pair.setdefault("cancel_sent_ms", self.now())  # the round trip and the lead on the start: /preopen shows them (the cancel time's evidence)
        async def batch(chunk: list[str]) -> None:
            result = await self.cancel_api.remove_orders(chunk)
            for oid in result["removed"]:
                if oid in by_id:
                    pair, order = by_id[oid]
                    order["state"] = "removed"
                    pair.setdefault("cancel_done_ms", self.now())
                    self.ledger.save(pair)
        results = await asyncio.gather(*(batch(ids[n:n + 20]) for n in range(0, len(ids), 20)), return_exceptions=True)
        for error in results:
            if isinstance(error, BaseException):
                self.error = core.clean_error(error)
        for pair in pairs:
            self.bookkeep(pair)  # closed once nothing is left to cancel; the next try RETRY_MS on, doubling (not every half second)
            if not pair.get("accounted") and pair["slug"] not in self.cancelling:
                self.launch_reconcile(pair)  # the removed orders' final fills, the unknown ones' hashes: the reservation ends on what they show

    def launch_reconcile(self, pair: dict) -> None:
        key = "cancel:" + pair["slug"]
        if key in self.settling and not self.settling[key].done():
            return
        async def reconcile() -> None:
            async with self.settle_gate:
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
        blocked = bool(self.blocked())
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
            try:
                await self.place_pair(c)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.error = core.clean_error(error) or type(error).__name__
                LOG.warning("开盘前挂单失败 %s：%s", c["slug"], self.error)
            finally:
                self.placing.discard(c["slug"])
                self.queued.discard(c["slug"])
                self.queue.task_done()

    async def discovery_loop(self) -> None:
        while not self.stopped():
            try:
                if not self.blocked():
                    await self.account()
                    await self.discover()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.error = core.clean_error(error) or type(error).__name__
                LOG.warning("开盘前市场扫描：%s", self.error)
            await asyncio.sleep(self.settings.poll)

    async def cancellation_loop(self) -> None:
        while not self.stopped():
            now = self.now()
            due = self.due(now)
            if due:
                active = [p for p in due if p["phase"] == "active"]
                if active:
                    await self.cancel_many(active, "到时撤单或真实交易暂停")
                for pair in due:
                    if pair["phase"] != "active" and pair["slug"] not in self.cancelling:
                        self.bookkeep(pair)  # the next try is booked now: a read that hangs does not make the loop ask again meanwhile
                        self.launch_reconcile(pair)
            for pair in list(self.ledger.rows.values()):
                if pair["phase"] == "active":
                    key = "monitor:" + pair["slug"]
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
                if pair.get("accounted") or self.now() < pair["start_ms"] + 15 * 60_000:
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
        # Recovered prepared records never acquire a new salt on restart. Anything
        # already saved is closed/reconciled, including attempts interrupted mid-POST.
        try:
            for pair in self.ledger.rows.values():
                if pair["phase"] != "closed" and not pair.get("accounted"):
                    pair.update(phase="closing", retry_ms=0)
                    self.ledger.save(pair)
            jobs = [asyncio.create_task(self.discovery_loop(), name="preopen-discovery"),
                    asyncio.create_task(self.cancellation_loop(), name="preopen-cancel"),
                    *(asyncio.create_task(self.placement_worker(), name=f"preopen-place-{i}") for i in range(2))]
            stops = [asyncio.create_task(self.owner.stopping.wait()), asyncio.create_task(self.stop.wait())]
            try:
                await asyncio.wait([*stops, *jobs], return_when=asyncio.FIRST_COMPLETED)
                for job in jobs:
                    if job.done() and not job.cancelled() and job.exception():
                        raise job.exception()
            finally:
                for job in [*stops, *jobs]:
                    job.cancel()
                await asyncio.gather(*stops, *jobs, return_exceptions=True)
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(self.cancel_all("进程停止，撤销预挂单"), timeout=12)
                for task in self.settling.values():
                    task.cancel()
                await asyncio.gather(*self.settling.values(), return_exceptions=True)
        finally:
            self.alive = False  # the model trader opens orders again (LiveBot.preopen_active)

    def timing_line(self) -> str:
        """What the last windows measured: the cancel's round trip and how long before the start it was confirmed, and how
        long before the start the listing first showed them OPEN — the evidence PREOPEN_CANCEL_SECONDS is set against."""
        rows = sorted(self.ledger.rows.values(), key=lambda p: p["cancel_ms"])
        sent = [p for p in rows if p.get("cancel_sent_ms") and p.get("cancel_done_ms")][-20:]
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
        return "；".join(parts)

    def status(self) -> str:
        rows = list(self.ledger.rows.values())
        state = "已停止" if not self.alive else "暂停" if self.manual_pause or self.blocked() else "运行"
        return (f"开盘前自动预挂：{state}\n"
                f"{' / '.join(sorted(self.settings.markets))}：两边 {core.cents(float(self.settings.price))}，各 {self.settings.shares} 份；"
                f"扫描 {self.settings.poll:g} 秒，只挂 {self.settings.ahead_minutes} 分钟内开始的场次\n"
                f"撤单：开场前 {self.settings.cancel_seconds} 秒发出，之后到开场前 {self.settings.expiry_seconds} 秒到期为止每 {SPRINT_MS // 1000} 秒催一次；"
                f"不足 {self.settings.entry_seconds} 秒不再新挂\n"
                f"已记录 {len(rows)} 场（{sum(1 for p in rows if p['phase'] == 'active')} 场挂着，{sum(1 for p in rows if p.get('accounted'))} 场已结清），队列 {self.queue.qsize()} 场\n"
                f"预留 {self.risk():.2f}U（撤掉且一份没成交的场次立即释放）；最近问题：{self.error or '无'}\n"
                f"实测：{self.timing_line()}")
