"""Fixed, post-only quotes on newly listed 15-minute markets. Disabled by default.

The model trader does not choose these orders: their identity, clock and fixed
size are checked independently. Records live in a separate, synchronous SQLite
ledger; unknown submissions are looked up by their saved hash, never re-signed.
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
PRICE, SHARES = D("0.49"), D("50")
PAIR_USD = float(PRICE * SHARES * 2)
TERMINAL = {"removed", "not_sent", "final"}


def final_order(row: dict) -> bool:
    status = str(row.get("status") or "").upper()
    amount, filled = D(str(row.get("amount") or "0")), D(str(row.get("amountFilled") or "0"))
    return amount > 0 and (filled >= amount or status in {
        "FILLED", "CANCELLED", "CANCELED", "EXPIRED", "INVALIDATED", "REJECTED", "FAILED", "CLOSED", "COMPLETED", "DONE", "SETTLED", "RESOLVED"})


@dataclass(frozen=True)
class Settings:
    enabled: bool = False
    poll: float = 5.0
    cancel_seconds: int = 60
    expiry_seconds: int = 30
    entry_seconds: int = 180
    max_open_usd: float = 7000.0
    db: str = ""

    @classmethod
    def from_config(cls, config: Any) -> "Settings":
        env = config.env
        enabled = str(env.get("PREOPEN_15M", "off")).lower() == "on"
        if not enabled:
            return cls()
        poll = float(env.get("PREOPEN_POLL_SECONDS", "5"))
        cap = float(env.get("PREOPEN_MAX_OPEN_USD", "7000"))
        if not 1 <= poll <= 60 or not 49 <= cap <= 1_000_000:
            raise ValueError("PREOPEN_POLL_SECONDS 必须为 1～60，PREOPEN_MAX_OPEN_USD 必须为 49～1000000")
        path = str(env.get("PREOPEN_DB") or (":memory:" if config.db_path == ":memory:" else str(Path(config.db_path).with_suffix(".preopen.sqlite3"))))
        return cls(True, poll=poll, max_open_usd=cap, db=path)


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


def safe_api(base: str, key: str, wallet: Any, before_send: Any = None) -> Any:
    # Imported at runtime: live imports this module only after defining its API.
    import live

    class Api(live.PredictApi):
        async def send(self, method: str, path: str, payload: Any, timeout: int) -> bytes:
            if path == "/orders" and method == "POST" and before_send is not None:
                why = before_send(payload)
                if why:
                    raise NotSent(why)
            # A sendall failure cannot prove zero delivery. In particular, do not
            # use the pooled transport's automatic POST retry for this strategy.
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


def candidate(market: dict, now_ms: int, config: Any, settings: Settings) -> dict | None:
    """Only exact, listed slugs of the three configured assets, starting today."""
    slug = str(market.get("categorySlug") or market.get("slug") or "")
    for rolling in core.WINDOW_UPDOWN_MARKETS:
        prefix = f"{rolling.stem}-updown-15m-"
        suffix = slug.removeprefix(prefix)
        if not slug.startswith(prefix) or not suffix.isdigit():
            continue
        start = int(suffix) * 1000
        if start % rolling.window_ms or start - now_ms < settings.entry_seconds * 1000:
            return None
        today = dt.datetime.fromtimestamp(now_ms / 1000, core.BEIJING).date()
        if dt.datetime.fromtimestamp(start / 1000, core.BEIJING).date() != today:
            return None
        holidays = config.holidays.get(rolling.exchange, frozenset()) if rolling.exchange else frozenset()
        if not rolling.exists(start, holidays):
            return None
        if str(market.get("status") or "").upper() != "REGISTERED" or market.get("isVisible") is False:
            return None
        if str(market.get("tradingStatus") or "").upper() not in {"OPEN", "MATCHING_NOT_ENABLED"}:
            return None
        if str(market.get("makerFeeBps") or "0") not in {"0", "0.0"}:
            return None
        if not market.get("id") or not market.get("conditionId"):
            return None
        spec = rolling.window_spec(start)
        checker = SimpleNamespace(predict=SimpleNamespace(info={slug: {"question": str(market.get("question") or market.get("title") or "")}}))
        if core.Bot.window_title_problem(checker, spec):
            return None
        return {"slug": slug, "key": rolling.key, "market_id": str(market["id"]), "start_ms": start,
                "cancel_ms": start - settings.cancel_seconds * 1000, "expires": start // 1000 - settings.expiry_seconds,
                "day": today.isoformat(), "name": spec.name}
    return None


class Controller:
    """Discovery and cancellation run independently; a placement backlog cannot delay cancels."""
    def __init__(self, owner: Any):
        import live
        self.owner = owner
        self.settings = Settings.from_config(owner.config)
        self.ledger = Ledger(self.settings.db)
        self.error = ""
        self.manual_pause = self.ledger.pause()
        self.account_at = 0
        self.balance = 0.0
        self.open_reserved = 0.0
        self.listed_ids: set[str] = set()
        self.unmanaged = False
        self.placing: set[str] = set()
        self.cancelling: set[str] = set()
        self.queue: asyncio.Queue = asyncio.Queue()
        self.queued: set[str] = set()
        self.account_lock = asyncio.Lock()
        self.api = safe_api(live.API_BASES[owner.config.live_chain], owner.config.predict_api_key, owner.live.wallet, self.before_send)
        # A separate request gate leaves cancellation capacity available even when
        # placement requests are queued or their HTTP responses are slow.
        self.cancel_api = safe_api(live.API_BASES[owner.config.live_chain], owner.config.predict_api_key, owner.live.wallet)
        self.settling: dict[str, asyncio.Task] = {}
        self.settle_gate = asyncio.Semaphore(2)
        core.CALENDAR.configure(owner.config.hk_half_days, owner.config.kr_late_days,
                                owner.config.holidays.get("sg", frozenset()),
                                core.parse_dates(owner.config.env.get("SG_HALF_DAYS", core.DEFAULT_SPECIAL_DAYS["SG_HALF"]), "SG_HALF_DAYS"))

    def now(self) -> int:
        return self.owner.market.now_ms()

    def blocked(self) -> str:
        c = self.owner.config
        if self.owner.stopping.is_set():
            return "进程停止中"
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
        if pair["expires"] * 1000 - self.now() < 120_000:
            return "距订单到期不足两分钟，不再发送"
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
        # A removal acknowledgement alone does not tell us its final fills.
        # Preserve the whole reservation until official resolution + final reads.
        return sum(float(PRICE * SHARES) for p in self.ledger.rows.values() if not p.get("accounted")
                   for o in p["orders"].values() if o["state"] != "not_sent")

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
        """Scan every page; never interpret a truncated/error response as an empty catalog."""
        for variant in ("UP_DOWN", "CRYPTO_UP_DOWN"):
            after, seen = "", set()
            for _ in range(100):
                query = {"first": 100, "marketVariant": variant}
                if after:
                    query["after"] = after
                answer = await self.api.call("GET", "/markets?" + core.urllib.parse.urlencode(query))
                if not isinstance(answer["data"], list):
                    raise core.RemoteError("市场列表格式异常")
                for market in answer["data"]:
                    c = candidate(market, self.now(), self.owner.config, self.settings) if isinstance(market, dict) else None
                    if c and c["slug"] not in self.ledger.rows and c["slug"] not in self.queued:
                        self.queued.add(c["slug"])
                        await self.queue.put(c)
                cursor = str(answer.get("cursor") or "")
                if not cursor:
                    break
                if cursor in seen or not answer["data"]:
                    raise core.RemoteError("市场列表分页没有前进")
                seen.add(cursor)
                after = cursor
            else:
                raise core.RemoteError("市场列表超过100页，下一轮重新扫描")

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
        if not await self.owner.live.chain.allowance(live.exchange_key(info.neg_risk, info.yield_bearing)) >= live.to_wei(PAIR_USD):
            raise core.RemoteError("开盘前双边挂单授权额度不足")
        await self.api.ensure_auth()
        async with self.account_lock:
            # Only one coroutine reserves a pair at a time; reservations are saved
            # before the lock is released, including orders not visible on REST yet.
            if c["slug"] in self.ledger.rows or self.blocked() or self.now() - self.account_at > 30_000 or self.unmanaged:
                return
            limit = min(self.settings.max_open_usd, self.owner.config.live_max_open_usd)
            if self.risk() + self.other_risk() + PAIR_USD > limit + 1e-9:
                self.error = f"全天预挂资金上限不足：需要再预留 {PAIR_USD:g}U，限额 {limit:g}U"
                return
            if self.balance < max(self.open_reserved, self.risk()) + PAIR_USD:
                self.error = "可用USDT不足以预留下一场双边订单"
                return
            if float(PRICE * SHARES) > self.owner.config.live_max_order_usd:
                self.error = "LIVE_MAX_ORDER_USD 小于每边24.5U"
                return
            cap = self.owner.config.live_max_daily_loss_usd
            if cap and self.daily_pnl() + self.owner.live_daily_pnl(self.owner.sim_trades(), self.now()) <= -cap:
                return
            amounts = live.limit_amounts(True, live.to_wei(PRICE), live.to_wei(SHARES))
            orders = {}
            for side, (outcome, _) in zip(("up", "down"), outcomes):
                identity = f"preopen49-v1|{self.owner.config.live_chain}|{self.owner.live.wallet.maker.lower()}|{current['market_id']}|{outcome['token']}|{current['expires']}"
                salt = int.from_bytes(hashlib.sha256(identity.encode()).digest()[:8], "big") % live.MAX_SALT
                signed = live.build_order(self.owner.live.wallet, outcome["token"], amounts["maker"], amounts["taker"], info.fee_bps, current["expires"], salt=salt)
                digest, signature = self.owner.live.wallet.sign_order(signed, info.neg_risk, info.yield_bearing)
                orders[side] = {"hash": digest.lower(), "id": "", "state": "prepared", "filled": 0.0,
                                "body": {"data": {"order": {**signed, "signature": signature, "hash": digest},
                                                   "strategy": "LIMIT", "pricePerShare": str(amounts["price_per_share"]),
                                                   "isPostOnly": True, "selfTradePrevention": "CANCEL_TAKER"}}}
            pair = {**current, "phase": "active", "orders": orders, "created_ms": self.now()}
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
                # A timeout, malformed answer or duplicate response does not prove
                # no order exists. Keep its hash and full reservation; do not retry.
                order.update(state="unknown", error=core.clean_error(error))
                pair["phase"] = "closing"
            self.ledger.save(pair)
            if pair["phase"] != "active":
                break
        if pair["phase"] != "active" or self.now() >= pair["cancel_ms"] or self.blocked():
            await self.cancel_pair(pair, "单边未确认或已到撤单时间")
        LOG.info("开盘前预挂 %s：%s", pair["slug"], {s: o["state"] for s, o in pair["orders"].items()})

    @staticmethod
    def apply_fill(order: dict, row: dict) -> None:
        # Fixed shares are the actual signed shares, not an estimated count.
        amount, filled = D(str(row.get("amount") or "0")), D(str(row.get("amountFilled") or "0"))
        if amount <= 0 or filled < 0 or filled > amount:
            raise core.RemoteError("成交回报数量无效，保留风险额度")
        order["filled"] = max(float(order.get("filled") or 0), float(SHARES * filled / amount))

    async def cancel_pair(self, pair: dict, reason: str) -> None:
        if pair["slug"] in self.cancelling:
            return
        self.cancelling.add(pair["slug"])
        try:
            pair.update(phase="closing", reason=reason)
            self.ledger.save(pair)
            for order in pair["orders"].values():
                if order["state"] == "prepared":
                    order["state"] = "not_sent"
                    self.ledger.save(pair)
                    continue
                if order["state"] in TERMINAL:
                    continue
                # Unknown submissions are reconciled by their original hash.
                if not order.get("id"):
                    row = await self.cancel_api.order("", order["hash"])
                    if row:
                        order["id"] = str(row["id"])
                        self.apply_fill(order, row)
                    else:
                        continue
                result = await self.cancel_api.remove_orders([order["id"]])
                if order["id"] in result["removed"]:
                    order["state"] = "removed"
                    self.ledger.save(pair)
                    continue  # final-fill reads must not delay the next cancellation
                row = await self.cancel_api.order(order["id"], order.get("api_hash") or order["hash"])
                if row:
                    self.apply_fill(order, row)
                    if final_order(row):
                        order["state"] = "final"
                self.ledger.save(pair)
            if all(o["state"] in TERMINAL for o in pair["orders"].values()):
                pair["phase"] = "closed"
                self.ledger.save(pair)
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
            if order["state"] == "not_sent":
                continue
            row = await self.cancel_api.order(order.get("id", ""), order.get("api_hash") or order["hash"])
            if row is None or not final_order(row):
                return  # unresolved submissions retain the full reservation
            self.apply_fill(order, row)
            order["state"] = "final"
        pair.update(accounted=True, accounted_day=dt.datetime.fromtimestamp(self.now() / 1000, core.BEIJING).date().isoformat(),
                    pnl=sum(((up if side == "up" else 1 - up) - float(PRICE)) * order["filled"] for side, order in pair["orders"].items()))
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
        async def batch(chunk: list[str]) -> None:
            result = await self.cancel_api.remove_orders(chunk)
            for oid in result["removed"]:
                if oid in by_id:
                    pair, order = by_id[oid]
                    order["state"] = "removed"
                    self.ledger.save(pair)
        results = await asyncio.gather(*(batch(ids[n:n + 20]) for n in range(0, len(ids), 20)), return_exceptions=True)
        for error in results:
            if isinstance(error, BaseException):
                self.error = core.clean_error(error)
        for pair in pairs:
            if all(o["state"] in TERMINAL for o in pair["orders"].values()):
                pair["phase"] = "closed"
                self.ledger.save(pair)
            elif pair["slug"] not in self.cancelling:
                self.launch_reconcile(pair)

    def launch_reconcile(self, pair: dict) -> None:
        key = "cancel:" + pair["slug"]
        if key in self.settling and not self.settling[key].done():
            return
        async def reconcile() -> None:
            async with self.settle_gate:
                try:
                    await self.cancel_pair(pair, "撤单状态核对")
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self.error = core.clean_error(error)
        self.settling[key] = asyncio.create_task(reconcile(), name=key)

    async def placement_worker(self) -> None:
        while not self.owner.stopping.is_set():
            c = await self.queue.get()
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
        while not self.owner.stopping.is_set():
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
        while not self.owner.stopping.is_set():
            due = [p for p in list(self.ledger.rows.values()) if p["phase"] != "closed"
                   and (p["phase"] == "closing" or self.now() >= p["cancel_ms"] or self.blocked())]
            if due:
                await self.cancel_many(sorted(due, key=lambda p: p["cancel_ms"]), "到时撤单或真实交易暂停")
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
        for pair in self.ledger.rows.values():
            if pair["phase"] != "closed":
                pair["phase"] = "closing"
                self.ledger.save(pair)
        jobs = [asyncio.create_task(self.discovery_loop(), name="preopen-discovery"),
                asyncio.create_task(self.cancellation_loop(), name="preopen-cancel"),
                *(asyncio.create_task(self.placement_worker(), name=f"preopen-place-{i}") for i in range(2))]
        stop = asyncio.create_task(self.owner.stopping.wait())
        try:
            await asyncio.wait([stop, *jobs], return_when=asyncio.FIRST_COMPLETED)
            for job in jobs:
                if job.done() and not job.cancelled() and job.exception():
                    raise job.exception()
        finally:
            for job in [stop, *jobs]:
                job.cancel()
            await asyncio.gather(stop, *jobs, return_exceptions=True)
            with contextlib.suppress(Exception):
                await asyncio.wait_for(self.cancel_all("进程停止，撤销预挂单"), timeout=12)
            for task in self.settling.values():
                task.cancel()
            await asyncio.gather(*self.settling.values(), return_exceptions=True)

    def status(self) -> str:
        rows = list(self.ledger.rows.values())
        return (f"开盘前自动预挂：{'暂停' if self.manual_pause or self.blocked() else '运行'}\n"
                f"海力士 / BTC / 宇树15分钟：两边49¢，各50份；扫描{self.settings.poll:g}秒\n"
                f"每场提前60秒撤单、提前30秒到期；已记录{len(rows)}场，队列{self.queue.qsize()}场\n"
                f"保守预留 {self.risk():.2f}U；最近问题：{self.error or '无'}")
