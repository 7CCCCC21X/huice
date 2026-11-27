"""New listing -> durable fixed pair -> independent early cancellation. Entirely offline."""
import asyncio
import copy
import dataclasses
import datetime as dt
import hashlib
import json
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import offline  # noqa: F401
import main as m
import live as L
import preopen as p

NOW = int(dt.datetime(2026, 11, 6, 9, 0, tzinfo=m.BEIJING).timestamp() * 1000)


def window_slug_of(start_ms, stem="btc"):
    return f"/categories/{stem}-updown-15m-{start_ms // 1000}"


def market(stem="btc", at=NOW + 3600_000, mid=123, status="MATCHING_NOT_ENABLED", minutes=15):
    return {"id": mid, "categorySlug": f"{stem}-updown-{minutes}m-{at//1000}", "conditionId": "0xcondition",
            "status": "REGISTERED", "tradingStatus": status, "isVisible": True, "feeRateBps": 200,
            "outcomes": [{"name": "Down", "indexSet": 1, "onChainId": str(mid * 2)},
                         {"name": "Up", "indexSet": 2, "onChainId": str(mid * 2 + 1)}]}


class Chain:
    async def allowance(self, key): return 10 ** 30
    async def usdt_balance(self): return L.to_wei(10000)


class Api:
    def __init__(self, ctl, markets):
        self.ctl, self.markets = ctl, {str(x["id"]): x for x in markets}
        self.sent, self.removed, self.rows = [], [], {}
        self.orders_complete = True
        self.calls = []
        self.unlisted = set()  # category slugs that answer 404 although the market dict exists (listed later)
        self.fail_side = 0    # the n-th send: a timeout (result unknown)
        self.reject_side = 0  # the n-th send: Predict's refusal (definite)
        self.stuck = set()    # order ids a removal does not remove (answered "noop") and whose record stays OPEN
        self.before = None
        self.auth = None
        self.blocked_until = 0.0

    async def market(self, mid): return copy.deepcopy(self.markets[mid])
    async def ensure_auth(self):
        if self.auth: await self.auth()

    async def create_order(self, body):
        if self.before: await self.before()
        if why := self.ctl.before_send(body): raise p.NotSent(why)
        self.sent.append(copy.deepcopy(body))
        if len(self.sent) == self.fail_side: raise m.RemoteError("timeout: delivery unknown")
        if len(self.sent) == self.reject_side: raise L.OrderRejected("下单被拒绝：HTTP 400: post-only order would cross")
        oid = str(len(self.sent))
        signed = body["data"]["order"]
        self.rows[signed["hash"]] = {"id": oid, "order": signed, "amount": signed["takerAmount"], "amountFilled": "0", "status": "OPEN"}
        return {"order_id": oid, "hash": signed["hash"]}

    async def remove_orders(self, ids):
        self.removed.extend(ids)
        gone = [i for i in ids if i not in self.stuck]
        for row in self.rows.values():
            if row["id"] in gone: row["status"] = "CANCELLED"
        return {"removed": gone, "noop": [i for i in ids if i in self.stuck]}

    async def order(self, oid, digest=""):
        return copy.deepcopy(self.rows.get(digest))

    async def orders(self, status="OPEN"):
        return [copy.deepcopy(r) for r in self.rows.values() if r["status"] == "OPEN"]

    async def call(self, method, path, **kwargs):
        """GET /categories/<slug>: the category of a listed market (its id in the answer), 404 for the rest."""
        self.calls.append(path)
        slug = path.removeprefix("/categories/")
        for mkt in self.markets.values():
            if mkt.get("categorySlug") == slug and slug not in self.unlisted:
                return {"success": True, "data": {"slug": slug, "markets": [{"id": mkt["id"], "conditionId": mkt["conditionId"], "title": slug, "question": mkt.get("question", "")}]}}
        raise m.RemoteError(f"HTTP 404: no category {slug}")


def controller(markets=None, **env):
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on", "STATE_DB": ":memory:",
                             "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64, "LIVE_MAX_OPEN_USD": "7000", "PREOPEN_OPEN_QUIET_MINUTES": "0",
                             "PREOPEN_MARKETS": "btc,skhynix,unitree", "PREOPEN_SHARES": "50", "PREOPEN_AHEAD_MINUTES": "60", **env})  # the tests' figures: three cards, 50 a side, an hour ahead
    clock = types.SimpleNamespace(ms=NOW)
    wallet = types.SimpleNamespace(maker="0x" + "1" * 40,
        sign_order=lambda order, *flags: ("0x" + hashlib.sha256(json.dumps(order, sort_keys=True).encode()).hexdigest(), "0xsignature"))
    owner = types.SimpleNamespace(config=cfg, market=types.SimpleNamespace(now_ms=lambda: clock.ms), stopping=asyncio.Event(),
                                  live=types.SimpleNamespace(wallet=wallet, chain=Chain(), ready=True, api=types.SimpleNamespace(blocked_until=0.0)),
                                  live_paused=lambda: "", live_killed=lambda: "", live_exposure=lambda trades: 0.,
                                  sim_trades=lambda: {}, live_daily_pnl=lambda trades, now: 0.)
    ctl = p.Controller(owner)
    api = Api(ctl, markets or [market()])
    ctl.api = ctl.cancel_api = api
    ctl.account_at, ctl.balance = NOW, 10000.
    return ctl, api, clock


class PreopenTests(unittest.IsolatedAsyncioTestCase):
    async def finish(self, ctl):
        for task in ctl.settling.values(): task.cancel()
        await asyncio.gather(*ctl.settling.values(), return_exceptions=True)
        ctl.ledger.close()

    async def until(self, cond, timeout: float = 3.0) -> None:
        """A worker task's effect waited for, polling (a slow builder is no reason for a fixed short sleep to fall short)."""
        for _ in range(int(timeout / 0.01)):
            if cond():
                return
            await asyncio.sleep(0.01)
        self.fail(f"waited {timeout:g}s")

    async def settle_tasks(self, ctl):
        """The reconcile / verify tasks the loop would run: awaited here."""
        await asyncio.gather(*list(ctl.settling.values()), return_exceptions=True)

    async def read_finals(self, ctl, clock, *pairs):
        """The withdrawn pairs' next try as the cancellation loop makes it, the clock moved to it. Audit PRE-LATE-FILL: an
        order off the book is ended by a read of its record made LATE_FILL_MS after it left, not by one right after the
        removal — so a pair withdrawn with nothing filled is accounted at that try, a minute on, not at once."""
        clock.ms = max(clock.ms, *(int(pair["retry_ms"]) for pair in pairs))
        for pair in pairs:
            self.assertIn(pair, ctl.due(clock.ms))
            ctl.bookkeep(pair); ctl.launch_reconcile(pair)
        await self.settle_tasks(ctl)

    async def test_identity_clock_and_all_three(self):
        ctl, api, clock = controller()
        for stem in ("btc", "unitree", "skhynix"):
            self.assertIsNotNone(p.candidate(market(stem), NOW, ctl.owner.config, ctl.settings))
        for row in (market("sol"), market(at=NOW + 120000), market(at=NOW - 900000), market(at=NOW + 86400000),
                    market(status="CANCEL_ONLY"), market(status="CLOSED")):
            self.assertIsNone(p.candidate(row, NOW, ctl.owner.config, ctl.settings))
        row = market(); row["question"] = "10:00AM-10:15AM ET"
        self.assertIsNone(p.candidate(row, NOW, ctl.owner.config, ctl.settings))
        self.assertEqual(p.candidate_problem(market(at=NOW), NOW, ctl.owner.config, ctl.settings), "已开始或不足 3 分钟")
        self.assertEqual(p.candidate_problem(market(at=NOW + 120000), NOW, ctl.owner.config, ctl.settings), "链接里的时间不在场次边界上")
        self.assertEqual(p.candidate_problem(market(at=NOW + 2 * 3600_000), NOW, ctl.owner.config, ctl.settings), "开始还在 60 分钟之外（之后再扫）")
        self.assertEqual(p.candidate_problem(market("sol"), NOW, ctl.owner.config, ctl.settings), "不是涨跌卡（15 分钟、5 分钟、小时、日）的链接")
        self.assertEqual(p.candidate_problem(market("eth"), NOW, ctl.owner.config, ctl.settings), "不在 PREOPEN_MARKETS 里")  # 1.129.1: a card, not quoted by these settings
        for stem in ("eth", "bnb"):
            self.assertIsNotNone(p.candidate(market(stem), NOW, ctl.owner.config, dataclasses.replace(ctl.settings, markets=frozenset({"ETH-15M", "BNB-15M"}))))
        self.assertEqual(p.candidate_problem(market(status="CLOSED"), NOW, ctl.owner.config, ctl.settings), "交易状态 CLOSED")
        await self.finish(ctl)

    async def test_pair_exact_amounts_postonly_ttl_and_dedup(self):
        ctl, api, _ = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await asyncio.gather(ctl.place_pair(c), ctl.place_pair(c))
        self.assertEqual(len(api.sent), 2)
        tokens = []
        for body in api.sent:
            data = body["data"]; order = data["order"]
            self.assertTrue(data["isPostOnly"])
            self.assertEqual(data["selfTradePrevention"], "CANCEL_TAKER")
            self.assertEqual(data["pricePerShare"], str(L.to_wei(".49")))
            self.assertEqual(order["makerAmount"], str(L.to_wei("24.5")))
            self.assertEqual(order["takerAmount"], str(L.to_wei(50)))
            self.assertEqual(int(order["expiration"]), c["start_ms"] // 1000 - ctl.settings.expiry_seconds)  # 10 s before the start (the backstop)
            tokens.append(order["tokenId"])
        self.assertEqual(tokens, ["247", "246"])  # reversed metadata maps by name
        self.assertEqual(ctl.risk(), 49.)
        await ctl.place_pair(c)
        self.assertEqual(len(api.sent), 2)
        await self.finish(ctl)

    async def test_settings_size_price_cards_and_limits(self):
        ctl, api, _ = controller(PREOPEN_SHARES="10", PREOPEN_PRICE_CENTS="45", PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="90")
        self.assertEqual((ctl.settings.shares, ctl.settings.price, ctl.settings.markets, ctl.settings.ahead_minutes), (10, m.D("0.45"), frozenset({"BTC-15M"}), 90))
        self.assertEqual(p.candidate_problem(market("skhynix"), NOW, ctl.owner.config, ctl.settings), "不在 PREOPEN_MARKETS 里")
        await ctl.place_pair(p.candidate(market(), NOW, ctl.owner.config, ctl.settings))
        order = api.sent[0]["data"]["order"]
        self.assertEqual((order["makerAmount"], order["takerAmount"], api.sent[0]["data"]["pricePerShare"]), (str(L.to_wei("4.5")), str(L.to_wei(10)), str(L.to_wei(".45"))))
        self.assertEqual(ctl.risk(), 9.)
        await self.finish(ctl)
        for bad in ({"PREOPEN_SHARES": "1"}, {"PREOPEN_PRICE_CENTS": "51"}, {"PREOPEN_MARKETS": "sol"}, {"PREOPEN_AHEAD_MINUTES": "2"}):
            with self.assertRaises(ValueError):
                controller(**bad)
        cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_SHARES": "20"})
        self.assertFalse(p.Settings.from_config(cfg).enabled)
        self.assertEqual((p.Settings.from_config(cfg).shares, p.Settings.from_config(cfg, force=True).shares), (20, 20))  # read while off too: switching on from the page uses them
        self.assertEqual(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_SHARES": "abc"}).preopen.shares, 10, "off: a value that does not parse leaves the defaults")
        with self.assertRaises(ValueError):
            m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on", "STATE_DB": ":memory:", "PREOPEN_SHARES": "abc"})
        self.assertTrue({"PREOPEN_15M", "PREOPEN_SHARES", "PREOPEN_MARKETS", "PREOPEN_CANCEL_SECONDS"} <= m.CONTROL_KEY_SET and p.Settings is m.PreopenSettings)
        # the defaults are the trial: BTC alone, 10 shares a side (4.9U); "all" or a list widens it
        on = lambda **e: p.Settings.from_config(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on", "STATE_DB": ":memory:", **e}))
        self.assertEqual((on().shares, on().markets, on().side_usd, on().pair_usd, on().ahead_minutes), (10, frozenset({"BTC-15M"}), 4.9, 9.8, 1440))
        self.assertEqual(on(PREOPEN_MARKETS="all").markets, frozenset({"BTC-15M", "ETH-15M", "BNB-15M", "XAU-15M", "CL-15M", "SKHYNIX-15M", "UNITREE-15M", "ZHIPU-15M", *m.DEFAULT_SIM_SKIP,
                                                                       "BTC-1H", "ETH-1H", "BNB-1H", "BTC-1D", "ETH-1D", "BNB-1D"}))  # 1.142.0: the hourly and daily cards
        self.assertEqual(set(m.DEFAULT_SIM_SKIP), {"MU-15M", "SNDK-15M", "AVGO-15M", "SPCX-15M", "NVDA-15M", "GOOGL-15M", "META-15M", "TSM-15M", "AMZN-15M", "QQQ-15M", "SPY-15M", "MSFT-15M",
                                                   "BTC-5M", "ETH-5M", "BNB-5M"}, "1.135.0: the twelve US cards, the pre-open mode's alone by default; 1.137.0: the 5-minute cards too")
        self.assertEqual(on(PREOPEN_MARKETS="nvda,spy").markets, frozenset({"NVDA-15M", "SPY-15M"}))
        self.assertEqual(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline"}).sim_skip, frozenset(m.DEFAULT_SIM_SKIP))
        self.assertEqual(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "SIM_SKIP": ""}).sim_skip, frozenset(), "set empty on the page: the model trader takes them too")
        self.assertEqual(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "SIM_SKIP": "hsi"}).sim_skip, frozenset({"HSI"}))
        self.assertEqual(on(PREOPEN_MARKETS="xau, zhipu").markets, frozenset({"XAU-15M", "ZHIPU-15M"}))  # 1.134.0
        zcfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on", "PREOPEN_MARKETS": "zhipu", "SYMBOLS": "BTCUSDT"})
        self.assertEqual((zcfg.tickers["ZHIPUUSDT"].market, zcfg.tickers["ZHIPUUSDT"].code), ("hk", "02513"), "the quoted stock card's exchange ticker, SYMBOLS or not")
        self.assertNotIn("ZHIPUUSDT", m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "SYMBOLS": "BTCUSDT"}).tickers)
        self.assertEqual(on(PREOPEN_MARKETS="btc,eth,bnb").markets, frozenset({"BTC-15M", "ETH-15M", "BNB-15M"}))
        self.assertEqual(on(PREOPEN_MARKETS="btc, skhynix").markets, frozenset({"BTC-15M", "SKHYNIX-15M"}))
        self.assertEqual(on(PREOPEN_MARKETS="").markets, frozenset({"BTC-15M"}))

    async def test_failure_cancels_other_side_and_never_resubmits_unknown(self):
        ctl, api, clock = controller()
        api.fail_side = 2
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        self.assertEqual(api.removed, ["1"])
        pair = ctl.ledger.rows[c["slug"]]
        self.assertEqual((pair["orders"]["down"]["state"], pair["orders"]["up"]["state"]), ("unknown", "removed"))  # Audit PRE-LATE-FILL: withdrawn, its record read LATE_FILL_MS on (this pinned "final" at once)
        await self.read_finals(ctl, clock, pair)
        self.assertEqual((clock.ms - NOW, pair["orders"]["down"]["state"], pair["orders"]["up"]["state"]), (p.RETRY_MS, "unknown", "removed"), "a try before the minute is up reads nothing of it")
        clock.ms = NOW + p.LATE_FILL_MS
        await ctl.cancel_pair(pair, pair["reason"])
        self.assertEqual((pair["orders"]["down"]["state"], pair["orders"]["up"]["state"]), ("unknown", "final"))
        self.assertEqual(ctl.risk(), 49.)  # the unknown side may exist; the removed side's record read: nothing filled, still reserved until the pair is accounted
        await ctl.place_pair(c)
        self.assertEqual(len(api.sent), 2)
        await self.finish(ctl)

    async def test_refusal_is_final_and_releases(self):
        """A definite refusal (post-only would cross) ends the order: nothing reserved for good, the other side withdrawn,
        the pair accounted once that side's record is read (the first revision kept it "unknown" with its money, and asked
        every half second). Audit PRE-LATE-FILL: that read is made LATE_FILL_MS after the removal — this pinned the pair
        accounted at once, on the read right after it."""
        ctl, api, clock = controller()
        api.reject_side = 2
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        self.assertEqual((pair["orders"]["down"]["state"], pair["orders"]["up"]["state"], pair["phase"]), ("rejected", "removed", "closed"))
        self.assertIn("would cross", pair["orders"]["down"]["error"])
        self.assertEqual((pair.get("accounted"), ctl.risk(), pair["retry_ms"]), (None, 24.5, NOW + p.LATE_FILL_MS))  # the withdrawn side reserved until its record is read; the refused one ties up nothing
        self.assertEqual(ctl.due(NOW + p.LATE_FILL_MS - 1), [])
        await self.read_finals(ctl, clock, pair)
        self.assertEqual((pair["orders"]["up"]["state"], pair["accounted"], pair["pnl"]), ("final", True, 0.))
        self.assertEqual(ctl.risk(), 0.)
        self.assertEqual(ctl.due(NOW + 10 ** 9), [])  # nothing left to try
        ctl.stop.set()  # the stop itself is not a withdrawal: on_stop decides (1.128.2)
        self.assertEqual(ctl.due(NOW), [])
        ctl.stop.clear()
        await self.finish(ctl)

    async def test_a_markets_reread_is_one_write_of_the_ledger(self):
        """1.154.27: an active window's market is read again every REFRESH_MS / REFRESH_FAR_MS and each of its levels' records
        saved — one transaction a record, each waiting for the disk on the event loop (synchronous=FULL), seven levels seven
        waits back to back: 11-18 23:34, six stalls of more than a tenth of a second in ten minutes, one of 1,054 ms. One
        transaction a market now (and the first sight of matching open likewise), found among the records not accounted."""
        ctl, api, clock = controller()
        pairs = await self.book_of(ctl, api, 7)  # seven levels of one window: one market
        for pair in pairs:
            pair["meta_ms"] = 0
            pair.pop("open_seen_ms", None)
        writes, plain_save, plain_many = [], ctl.ledger.save, ctl.ledger.save_many
        ctl.ledger.save = lambda row: writes.append(1) or plain_save(row)
        ctl.ledger.save_many = lambda rows: writes.append(len(rows)) or plain_many(rows)
        reads = []
        plain_market = api.market

        async def market_read(mid):
            reads.append(mid)
            return await plain_market(mid)
        api.market = market_read
        await ctl.refresh_active()
        # 1.154.42: ...and none at all for the read itself — when a market was last read (meta_ms) is pacing, kept on the records in
        # memory as checked_ms is; it was a commit a market, eight every five seconds: more than half of all this mode wrote
        self.assertEqual((len(reads), writes), (1, []), "one read of the market, nothing written for it")
        self.assertTrue(all(pair["meta_ms"] == clock.ms for pair in pairs))
        stored = {slug: json.loads(body) for slug, body in ctl.ledger.conn.execute("SELECT slug, body FROM pairs")}
        self.assertEqual({stored[pair["slug"]].get("meta_ms", 0) for pair in pairs}, {0}, "the stamp is memory's; the file has the records as last saved")
        await ctl.refresh_active()
        self.assertEqual(len(reads), 1, "read a moment ago: not due again — paced from the records in memory")
        # matching seen open: noted on every level's record — one write
        api.markets["123"]["tradingStatus"] = "OPEN"
        del writes[:]
        clock.ms += p.REFRESH_MS
        await ctl.refresh_active()
        self.assertEqual((writes, {pair.get("open_seen_ms") for pair in pairs}), ([7], {clock.ms}))
        stored = {slug: json.loads(body) for slug, body in ctl.ledger.conn.execute("SELECT slug, body FROM pairs")}
        self.assertEqual({(stored[pair["slug"]]["open_seen_ms"], stored[pair["slug"]]["meta_ms"]) for pair in pairs}, {(clock.ms, clock.ms)}, "(what that write carries: the records as they stand)")
        del writes[:]
        clock.ms += p.REFRESH_MS
        await ctl.refresh_active()
        self.assertEqual(writes, [0], "seen open already: nothing to note (an empty write is none)")
        # a market no longer open for orders has its pairs withdrawn, and saved, as before
        api.markets["123"]["status"] = "RESOLVED"
        del writes[:]
        clock.ms += p.REFRESH_MS
        await ctl.refresh_active()
        self.assertTrue(writes and all(pair["phase"] != "active" for pair in pairs), writes)
        stored = {slug: json.loads(body) for slug, body in ctl.ledger.conn.execute("SELECT slug, body FROM pairs")}
        self.assertTrue(all(stored[pair["slug"]]["phase"] != "active" for pair in pairs))
        # ...and the cancellation loop's pass over the closing pairs whose try has come: their next tries booked in one write
        ctl2, api2, clock2 = controller()
        closing = await self.book_of(ctl2, api2, 7, phase="closing")
        writes2, save2, many2 = [], ctl2.ledger.save, ctl2.ledger.save_many
        ctl2.ledger.save = lambda row: writes2.append(1) or save2(row)
        ctl2.ledger.save_many = lambda rows: writes2.append(len(rows)) or many2(rows)
        self.assertEqual([pair["slug"] for pair in ctl2.due(clock2.ms)], [pair["slug"] for pair in closing])
        ctl2.book_due(closing)
        self.assertEqual((writes2, len(ctl2.settling)), ([7], 7), "one write for the seven bookings; a reconcile started for each")
        self.assertTrue(all(pair["retry_ms"] == clock2.ms + p.RETRY_MS and pair["tries"] == 1 for pair in closing))
        stored = {slug: json.loads(body) for slug, body in ctl2.ledger.conn.execute("SELECT slug, body FROM pairs")}
        self.assertEqual({stored[pair["slug"]]["retry_ms"] for pair in closing}, {clock2.ms + p.RETRY_MS})
        self.assertEqual(ctl2.due(clock2.ms), [], "booked: not due again before the next try")
        ctl2.ledger.save, ctl2.ledger.save_many = save2, many2
        await self.finish(ctl2)
        ctl.ledger.save, ctl.ledger.save_many = plain_save, plain_many
        await self.finish(ctl)

    async def test_a_bare_409_is_a_side_that_may_exist(self):
        """1.154.26 (the audit of 11-18, 3): "HTTP 409: Conflict" on a side's POST with none of the duplicate's wording was a
        refusal here ("rejected": nothing reserved, never looked for) while the model trader has taken it since 1.150.0 for
        Predict holding that hash — an order taken and answered so rested unknown to the mode until its signed expiry. Now
        "unknown", as an answer gone missing is: its hash and its money kept, the other side withdrawn, never sent again; a
        400 is a refusal as it was (test_refusal_is_final_and_releases)."""
        ctl, api, clock = controller()
        plain = api.create_order

        async def conflict_on_the_second(body):
            got = await plain(body)
            if len(api.sent) == 2:
                raise m.RemoteError("HTTP 409: Conflict")  # (Predict took it, and this is what came back)
            return got
        api.create_order = conflict_on_the_second
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        # looked for by its hash in the same pass — there it is, resting: withdrawn with the other side (as "rejected" it was
        # never looked for: one side of a pair left on the book until its signed expiry, in nobody's records)
        self.assertEqual((pair["orders"]["down"]["state"], pair["orders"]["up"]["state"], pair["orders"]["down"]["id"]), ("removed", "removed", "2"))
        self.assertEqual((sorted(api.removed), {row["status"] for row in api.rows.values()}, ctl.risk()), (["1", "2"], {"CANCELLED"}, 49.))
        await ctl.place_pair(c)
        self.assertEqual(len(api.sent), 2, "never sent again")
        await self.finish(ctl)
        # ...and one Predict does not have (the 409 was a refusal after all): unknown — its hash and its money kept, the other
        # side withdrawn — until the hash is given up after the window's start, as with an answer gone missing
        ctl, api, clock = controller()
        plain = api.create_order

        async def refused_on_the_second(body):
            if len(api.sent) == 1:
                api.sent.append(copy.deepcopy(body))
                raise m.RemoteError("HTTP 409: Conflict")
            return await plain(body)
        api.create_order = refused_on_the_second
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        self.assertEqual((pair["orders"]["down"]["state"], pair["orders"]["up"]["state"], api.removed, ctl.risk()), ("unknown", "removed", ["1"], 49.))
        clock.ms = c["start_ms"] + p.GIVE_UP_MS
        await ctl.cancel_pair(pair, "again")
        self.assertEqual((pair["orders"]["down"]["state"], pair["phase"]), ("not_found", "closed"))
        await self.finish(ctl)

    async def test_pause_during_send_queue_blocks_http(self):
        ctl, api, _ = controller()
        async def pause(): ctl.owner.live_paused = lambda: "paused"
        api.before = pause
        await ctl.place_pair(p.candidate(market(), NOW, ctl.owner.config, ctl.settings))
        self.assertEqual(api.sent, [])
        self.assertEqual(ctl.risk(), 0.)
        await self.finish(ctl)

    async def test_cancel_timing_settings_and_validation(self):
        ctl, api, clock = controller()
        self.assertEqual((ctl.settings.cancel_seconds, ctl.settings.expiry_seconds, ctl.settings.entry_seconds), (20, 10, 180))
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        self.assertEqual((c["cancel_ms"], c["expires"]), (c["start_ms"] - 20_000, c["start_ms"] // 1000 - 10))
        await self.finish(ctl)
        ctl, api, clock = controller(PREOPEN_CANCEL_SECONDS="12", PREOPEN_EXPIRY_SECONDS="6", PREOPEN_ENTRY_SECONDS="120")
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        self.assertEqual((c["cancel_ms"], c["expires"]), (c["start_ms"] - 12_000, c["start_ms"] // 1000 - 6))
        self.assertEqual(p.candidate_problem(market(at=NOW + 15 * 60_000), NOW + 15 * 60_000 - 119_000, ctl.owner.config, ctl.settings), "已开始或不足 120 秒")  # 1.152.0: the cut-off in force (was "3 分钟" whatever it was)
        self.assertIsNotNone(p.candidate(market(at=NOW + 15 * 60_000), NOW + 15 * 60_000 - 121_000, ctl.owner.config, ctl.settings))
        await self.finish(ctl)
        for bad in ({"PREOPEN_EXPIRY_SECONDS": "4"}, {"PREOPEN_CANCEL_SECONDS": "14"}, {"PREOPEN_ENTRY_SECONDS": "79"}, {"PREOPEN_CANCEL_SECONDS": "901", "PREOPEN_ENTRY_SECONDS": "3600"}):
            with self.assertRaises(ValueError):
                controller(**bad)

    async def test_last_stretch_sprints_and_the_status_measures_it(self):
        """Past the cancel time an order still open is asked to be cancelled every two seconds (no backoff) until the
        signed expiry; the round trip, the lead on the start and the moment the listing showed OPEN are measured."""
        ctl, api, clock = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        self.assertEqual(pair["open_seen_ms"], 0)
        api.markets["123"]["tradingStatus"] = "OPEN"
        clock.ms = c["start_ms"] - 300_000
        await ctl.refresh_active()  # the market shows OPEN: the moment kept
        self.assertEqual(pair["open_seen_ms"], c["start_ms"] - 300_000)
        api.stuck.add("2")
        clock.ms = c["cancel_ms"]
        self.assertEqual([x["slug"] for x in ctl.due(clock.ms)], [c["slug"]])
        await ctl.cancel_many([pair], "deadline")
        await self.settle_tasks(ctl)
        self.assertEqual((pair["orders"]["up"]["state"], pair["orders"]["down"]["state"]), ("removed", "open"))  # (the removed one's record is read LATE_FILL_MS on: Audit PRE-LATE-FILL)
        self.assertEqual((pair["cancel_sent_ms"], pair["cancel_done_ms"]), (clock.ms, clock.ms))
        self.assertTrue(ctl.in_sprint(pair))
        self.assertEqual((pair["retry_ms"], pair["tries"]), (clock.ms + p.SPRINT_MS, 0))  # two seconds, the count untouched
        clock.ms += p.SPRINT_MS
        asked = api.removed.count("2")
        self.assertEqual([x["slug"] for x in ctl.due(clock.ms)], [c["slug"]])
        ctl.bookkeep(pair); ctl.launch_reconcile(pair)
        await self.settle_tasks(ctl)
        self.assertGreater(api.removed.count("2"), asked)
        clock.ms = c["expires"] * 1000  # expired: nothing can fill; the reads back off from here
        self.assertFalse(ctl.in_sprint(pair))
        ctl.bookkeep(pair)
        self.assertEqual((pair["tries"], pair["retry_ms"]), (1, clock.ms + p.RETRY_MS))
        text = ctl.status()
        self.assertIn("撤单：开场前 20 秒发出，之后到开场前 10 秒到期为止每 2 秒催一次；不足 180 秒不再新挂", text)
        self.assertIn("撤单确认用时中位 0.0 秒（最长 0.0），确认时距开场中位 20 秒（最短 20；最近 1 场）", text)
        self.assertIn("撮合开启（列表显示 OPEN）在开场前中位 300 秒（最早 300、最晚 300；1 场）", text)
        await self.finish(ctl)

    async def test_expiry_rechecked_after_auth_delay(self):
        ctl, api, clock = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        async def slow_auth(): clock.ms = c["start_ms"] - 65_000  # under a minute to the expiry (start − 10 s): not sent
        api.auth = slow_auth
        await ctl.place_pair(c)
        self.assertEqual(api.sent, [])
        await self.finish(ctl)

    async def test_bulk_cancels_keep_the_reservation_until_the_records_are_read(self):
        ctl, api, clock = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        clock.ms = c["start_ms"] - 60_000
        await ctl.cancel_many([pair], "deadline")
        self.assertEqual(set(api.removed), {"1", "2"})
        self.assertEqual(pair["phase"], "closed")
        self.assertEqual(ctl.risk(), 49.)  # removed, fills not read yet
        await self.settle_tasks(ctl)
        # Audit PRE-LATE-FILL: ...nor read right after the removal (this pinned the pair accounted there, on records that may not
        # show the last seconds' matches yet): the loop's try comes LATE_FILL_MS on, and no read is made before it
        self.assertEqual((pair.get("accounted"), ctl.risk(), pair["retry_ms"], ctl.due(pair["retry_ms"] - 1)), (None, 49., clock.ms + p.LATE_FILL_MS, []))
        self.assertEqual({o["state"] for o in pair["orders"].values()}, {"removed"})
        await self.read_finals(ctl, clock, pair)  # the reconcile the loop runs then: both records final, nothing filled — released a minute on, not at the result
        self.assertTrue(pair["accounted"])
        self.assertEqual((ctl.risk(), pair["pnl"]), (0., 0.))
        await self.finish(ctl)

    async def test_stuck_cancel_backs_off(self):
        """A removal Predict does not confirm is asked again after RETRY_MS, doubling to RETRY_MAX_MS — not every half second."""
        ctl, api, clock = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        api.stuck.add("2")
        clock.ms = c["start_ms"] - 60_000
        ctl.manual_pause = True  # blocked: withdrawn now, before the cancel time (outside the last stretch: the backoff applies)
        self.assertEqual([x["slug"] for x in ctl.due(clock.ms)], [c["slug"]])
        await ctl.cancel_many([pair], "deadline")
        await self.settle_tasks(ctl)
        self.assertEqual((pair["orders"]["up"]["state"], pair["orders"]["down"]["state"], pair["phase"]), ("removed", "open", "closing"))  # (up's record is read LATE_FILL_MS on: Audit PRE-LATE-FILL)
        self.assertEqual(ctl.risk(), 49.)
        asked = api.removed.count("2")
        self.assertEqual(ctl.due(clock.ms), [])
        self.assertEqual(ctl.due(clock.ms + p.RETRY_MS - 1), [])
        self.assertEqual([x["slug"] for x in ctl.due(pair["retry_ms"])], [c["slug"]])
        tries = pair["tries"]
        for _ in range(4):
            clock.ms = pair["retry_ms"]
            ctl.bookkeep(pair); ctl.launch_reconcile(pair)
            await self.settle_tasks(ctl)
        self.assertGreater(api.removed.count("2"), asked)
        self.assertEqual(pair["retry_ms"] - clock.ms, p.RETRY_MAX_MS)  # capped
        self.assertGreater(pair["tries"], tries)
        self.assertEqual(pair["orders"]["up"]["state"], "final", "read by the tries that came a minute after it left the book")
        api.stuck.clear()
        clock.ms = pair["retry_ms"]
        ctl.bookkeep(pair); ctl.launch_reconcile(pair)
        await self.settle_tasks(ctl)
        # Audit PRE-LATE-FILL: removed now — its record ends it at the next try, not on a read right after the removal (this
        # pinned the pair accounted here)
        self.assertEqual((pair["orders"]["down"]["state"], pair["phase"], pair.get("accounted"), ctl.risk()), ("removed", "closed", None, 49.))
        await self.read_finals(ctl, clock, pair)
        self.assertEqual((pair["orders"]["down"]["state"], pair["phase"], pair["accounted"], ctl.risk()), ("final", "closed", True, 0.))
        await self.finish(ctl)

    async def test_unknown_submission_gives_up_after_the_window(self):
        """An order Predict never showed by its hash cannot fill after the start (it expired 30 s before): past the give-up it
        is taken as never placed and its money released."""
        ctl, api, clock = controller()
        api.fail_side = 2
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        self.assertEqual(pair["orders"]["down"]["state"], "unknown")
        clock.ms = c["start_ms"] + 5 * 60_000
        await ctl.cancel_pair(pair, "again")
        self.assertEqual((pair["orders"]["down"]["state"], ctl.risk()), ("unknown", 49.))  # before the give-up: still reserved
        clock.ms = c["start_ms"] + p.GIVE_UP_MS
        await ctl.cancel_pair(pair, "again")
        self.assertEqual((pair["orders"]["down"]["state"], pair["phase"], pair["accounted"], ctl.risk()), ("not_found", "closed", True, 0.))
        await self.finish(ctl)

    async def test_a_window_listed_later_is_found_on_the_next_poll_and_queued_once(self):
        ctl, api, clock = controller()
        api.unlisted.add(market()["categorySlug"])
        await ctl.discover()
        self.assertEqual(ctl.queue.qsize(), 0)  # 404: not listed yet
        api.unlisted.clear()
        clock.ms += 60_000  # 09:15–09:45 are not listed: a later window listed alone is found by the full scan, once a minute
        await ctl.discover()
        self.assertEqual(ctl.queue.qsize(), 1)
        await ctl.discover()
        self.assertEqual(ctl.queue.qsize(), 1)  # queued once
        self.assertTrue(all(c.startswith("/categories/") for c in api.calls))
        await self.finish(ctl)

    async def test_a_new_listing_goes_ahead_of_the_backlog_and_the_first_snapshot_comes_first(self):
        """1.154.8 (11-17 14:36): the placement queue went by start alone, so behind a backlog of 557 listed windows the new listings
        — the latest starts — waited 57 minutes (发现→挂上中位 3434 秒); 一有新的就挂、排在最前. And after a restart every placement waits
        for the first account snapshot (427 windows parked on "账户快照还没有读到"): the status re-reads and the known windows'
        re-reads leave its pages the budget until it comes (FIRST_SNAPSHOT_GRACE_MS at most), and the hold says how far it has come."""
        ctl, api, clock = controller()
        backlog = {"start_ms": NOW + 3_600_000, "slug": "old-window", "fresh": False}
        newest = {"start_ms": NOW + 20 * 3_600_000, "slug": "new-window", "fresh": True}
        later_new = {"start_ms": NOW + 21 * 3_600_000, "slug": "newer-window", "fresh": True}
        for c in (backlog, later_new, newest):
            ctl.queue.put_nowait((p.queue_rank(c), c["slug"], c))
        self.assertEqual([ctl.queue.get_nowait()[2]["slug"] for _ in range(3)], ["new-window", "newer-window", "old-window"],
                         "the new listings first (the soonest of them first), then the backlog")
        ctl.parked["new-window"] = (clock.ms - 1, newest)  # held, then back on the queue: still ahead of the backlog
        ctl.queue.put_nowait((p.queue_rank(backlog), backlog["slug"], backlog))
        ctl.unpark()
        self.assertEqual(ctl.queue.get_nowait()[2]["slug"], "new-window")
        ctl.queue.get_nowait()
        # the listing probe queues what it finds as fresh; the known windows' re-read as the backlog
        found = []
        real = ctl.enqueue

        def noting(market, slug, wanted, attempts, st, fresh=False):
            found.append((slug, fresh))
            return real(market, slug, wanted, attempts, st, fresh)
        ctl.enqueue = noting
        await ctl.discover()
        self.assertTrue(found and all(fresh for _, fresh in found), found)
        while not ctl.queue.empty():
            self.assertTrue(ctl.queue.get_nowait()[2]["fresh"])
        ctl.enqueue = real
        await self.finish(ctl)
        # after a start: no snapshot yet — the status re-reads and the known windows' re-reads wait; the hold says how far it has come
        ctl, api, clock = controller()
        ctl.account_at = 0
        self.assertTrue(ctl.first_snapshot_pending())
        refreshed = []

        async def refresh():
            refreshed.append(1)
        ctl.refresh_active = refresh
        ctl.stopped = lambda: bool(refreshed) or len(slept) > 0
        slept = []
        real_sleep = asyncio.sleep

        async def nap(s):
            slept.append(s)
            await real_sleep(0)
        p.asyncio.sleep, saved = nap, p.asyncio.sleep
        try:
            await ctl.refresh_loop()
        finally:
            p.asyncio.sleep = saved
            del ctl.stopped  # the loop's one turn done: the method again
        self.assertEqual(refreshed, [], "no status re-read before the first snapshot")
        asked = []

        async def reread(*args):
            asked.append(1)
        ctl.reread_known, ctl.rescan_ms, ctl.queued = reread, 0, set()
        await ctl.discover()
        self.assertEqual(asked, [], "nor the known windows' re-read")
        ctl.account_started_ms, api.paging = clock.ms - 40_000, {"/orders": 23}
        self.assertEqual(ctl.hold(), "账户快照还没有读到（启动后第一次读取中：已 40 秒、读了 23 页挂单）")
        await ctl.account()  # the first snapshot: all goes on as before
        self.assertFalse(ctl.first_snapshot_pending())
        await ctl.discover()
        self.assertEqual(asked, [1])
        fresh_ctl, _, fresh_clock = controller()
        fresh_ctl.account_at = 0
        fresh_clock.ms += p.FIRST_SNAPSHOT_GRACE_MS
        self.assertFalse(fresh_ctl.first_snapshot_pending(), "a snapshot failing for good stops the re-reads no longer than the grace")
        await self.finish(fresh_ctl)
        await self.finish(ctl)

    async def test_discovery_takes_the_soonest_first_and_only_within_the_horizon(self):
        rows = [market(at=NOW + 45 * 60_000, mid=1), market(at=NOW + 15 * 60_000, mid=2), market(at=NOW + 3 * 3600_000, mid=3), market("skhynix", at=NOW + 30 * 60_000, mid=4)]
        ctl, api, _ = controller(rows)
        starts = p.window_starts(NOW, ctl.owner.config, ctl.settings)
        self.assertEqual([(r.key, (s - NOW) // 60_000) for r, s in starts][:4], [("BTC-15M", 15), ("SKHYNIX-15M", 15), ("BTC-15M", 30), ("SKHYNIX-15M", 30)])
        got = [(r.key, (s - NOW) // 60_000) for r, s in starts]
        self.assertTrue(all(s - NOW >= 180_000 and s - NOW <= 3_600_000 for _, s in starts) and ("UNITREE-15M", 45) in got)
        self.assertNotIn(("UNITREE-15M", 30), got, "1.151.1: the SSE open's first window (09:30) is never quoted, the suite's 0 or not")
        await ctl.discover()
        self.assertEqual(ctl.queue.qsize(), 3)  # the 3-hour one is not even asked for
        order = [ctl.queue.get_nowait()[2]["market_id"] for _ in range(3)]
        self.assertEqual(order, ["2", "4", "1"])
        self.assertNotIn("/categories/" + market(at=NOW + 3 * 3600_000)["categorySlug"], api.calls)
        self.assertIn("各卡：BTC-15M 发布到 09:45（开场前 45 分钟）；SKHYNIX-15M 发布到 09:30（开场前 30 分钟）；UNITREE-15M 还没见到已发布的场次", ctl.status())  # 1.130.4: per card
        await self.finish(ctl)

    async def test_discovery_is_cheap_however_wide_the_horizon(self):
        """1.126.0: every listed window is quoted (PREOPEN_AHEAD_MINUTES 1440 by default, any day), and the discovery asks
        each card's windows in start order — a full scan stops after MISS_STOP unlisted in a row, and between full scans
        (once a minute) only the first unlisted window is asked every poll: the one Predict lists next."""
        rows = [market(at=NOW + n * 15 * 60_000, mid=n) for n in (1, 2, 3)] + [market(at=NOW + 16 * 3600_000, mid=9)]
        ctl, api, clock = controller(rows, PREOPEN_AHEAD_MINUTES="1440", PREOPEN_MARKETS="btc")
        starts = p.window_starts(NOW, ctl.owner.config, ctl.settings)
        self.assertEqual(len(starts), 96)  # the next 24 hours of BTC quarters, tomorrow's included
        self.assertEqual(p.candidate_problem(market(at=NOW + 16 * 3600_000, mid=9), NOW, ctl.owner.config, ctl.settings), "")  # 01:00 tomorrow
        self.assertEqual(p.candidate(market(at=NOW + 16 * 3600_000, mid=9), NOW, ctl.owner.config, ctl.settings)["day"], "2026-11-07")
        await ctl.discover()  # the first scan is a full one
        self.assertEqual(ctl.queue.qsize(), 3)
        self.assertEqual(len(api.calls), 3 + p.MISS_STOP)  # the three listed, then four unlisted in a row: the rest of the day waits
        self.assertEqual(ctl.listed_until_ms, NOW + 45 * 60_000)
        self.assertIn("已发布到 09:45（开场前 45 分钟）", ctl.status())
        api.calls.clear()
        clock.ms += 5_000
        await ctl.discover()  # a quick scan: the queued windows are skipped, the first unlisted one alone is asked
        self.assertEqual(api.calls, ["/categories/" + market(at=NOW + 60 * 60_000)["categorySlug"]])
        api.calls.clear()
        clock.ms += 60_000
        await ctl.discover()  # the full scan again, a minute on
        self.assertEqual(len(api.calls), p.MISS_STOP)
        # 1.131.0: a new listing seen anchors the rhythm: the next is expected a cycle on, polled every second around that moment
        self.assertEqual((ctl.listed_at, ctl.poll_wait()), ({}, p.LISTING_PROBE_S))  # 1.133.0: the listings probed every 2 s
        self.assertEqual(ctl.awaiting, {"BTC-15M": NOW + 60 * 60_000 - 1440 * 60_000}, "1.133.1: the 10:00 window entered the day's range yesterday, unlisted: no sprint for it")
        api.markets["4"] = market(at=NOW + 60 * 60_000, mid=4)
        await ctl.discover()
        seen = clock.ms
        queued = [ctl.queue.get_nowait()[2] for _ in range(ctl.queue.qsize())]
        self.assertEqual((ctl.listed_at, next(q for q in queued if q["market_id"] == "4")["read_ms"]), ({"BTC-15M": seen}, seen))
        self.assertEqual(ctl.poll_wait(), p.LISTING_PROBE_S)
        clock.ms = seen + p.LISTING_CYCLE_MS - 10_000
        self.assertEqual(ctl.poll_wait(), p.FAST_POLL_S)
        clock.ms = seen + p.LISTING_CYCLE_MS + p.LISTING_LATE_MS + 1000
        self.assertEqual(ctl.poll_wait(), p.LISTING_PROBE_S)
        self.assertIn("发布节奏", ctl.status())
        await self.finish(ctl)

    async def test_rate_limited_first_request_leaves_no_record_so_the_window_is_placed_later(self):
        """The cooldown is a hold (nothing is started); a 429 on the first order leaves no record, so the next poll places
        the window afresh; a cooldown refusal on the second order, the first placed, closes the pair as any one-sided
        failure (the first withdrawn), and the record stays: no duplicate."""
        ctl, api, _ = controller([market(), market(at=NOW + 30 * 60_000, mid=7)])
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        api.blocked_until = time.monotonic() + 30
        self.assertTrue(ctl.hold().startswith("Predict 接口限流冷却中（"), ctl.hold())
        await ctl.place_pair(c)
        self.assertEqual((api.sent, c["slug"] in ctl.ledger.rows), ([], False))
        api.blocked_until = 0.0
        original = api.create_order
        async def limited(body): raise m.RemoteError("HTTP 429: Too Many Requests")
        api.create_order = limited
        await ctl.place_pair(c)
        self.assertEqual((api.sent, c["slug"] in ctl.ledger.rows), ([], False))  # nothing exists: no record
        self.assertIn("HTTP 429", ctl.error)
        api.create_order = original
        await ctl.place_pair(c)
        self.assertEqual(len(api.sent), 2)
        first = []
        async def second_limited(body):
            if first: raise m.RemoteError("Predict 接口限流冷却中（20 秒后重试）")
            first.append(1); return await original(body)
        api.create_order = second_limited
        c2 = p.candidate(market(at=NOW + 30 * 60_000, mid=7), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c2)
        pair = ctl.ledger.rows[c2["slug"]]
        self.assertEqual((pair["orders"]["up"]["state"], pair["orders"]["down"]["state"], pair["phase"]), ("removed", "not_sent", "closed"))  # (withdrawn; its record is read LATE_FILL_MS on: Audit PRE-LATE-FILL)
        self.assertIn("限流冷却中", pair["orders"]["down"]["error"])
        self.assertIn("3", api.removed)  # the first side withdrawn
        api.create_order = original
        await ctl.place_pair(c2)
        self.assertEqual(len(api.sent), 3)  # recorded: not placed again
        await self.finish(ctl)

    async def test_a_held_candidate_waits_on_the_side_without_another_read(self):
        """1.128.1: a candidate the hold keeps back (a stale snapshot, the budget, the pause) is parked by the worker, still
        queued — the discovery does not read it again — and goes back on the queue HOLD_RETRY_MS on; one whose window got
        too close is dropped. The cancel measurements count the scheduled cancels only."""
        ctl, api, clock = controller()
        await ctl.discover()
        _, _, c = ctl.queue.get_nowait()
        ctl.account_at = NOW - 40_000
        self.assertEqual((await ctl.place_pair(c), api.sent), ("held", []))
        ctl.account_at = NOW
        ctl.owner.config = dataclasses.replace(ctl.owner.config, live_max_open_usd=48)
        ctl.owner.live_exposure = lambda trades: 60.
        self.assertEqual(ctl.budget_problem(), "", "1.132.1: the model trader's cap and exposure are not this mode's")
        ctl.settings = dataclasses.replace(ctl.settings, max_open_usd=48)
        self.assertEqual((await ctl.place_pair(c), api.sent), ("held", []))
        self.assertEqual(ctl.error, "预挂资金上限不足：已预留 0.00U，再挂一场双边要 49U，限额 48U（PREOPEN_MAX_OPEN_USD）")
        ctl.settings = dataclasses.replace(ctl.settings, max_open_usd=7000)
        ctl.owner.live_exposure = lambda trades: 0.
        ctl.owner.config = dataclasses.replace(ctl.owner.config, live_max_open_usd=7000)
        ctl.account_at = NOW - 40_000
        ctl.queue.put_nowait((c["start_ms"], c["slug"], c))
        worker = asyncio.create_task(ctl.placement_worker())
        await self.until(lambda: c["slug"] in ctl.parked)
        self.assertEqual((set(ctl.parked), ctl.queued, ctl.queue.qsize()), ({c["slug"]}, {c["slug"]}, 0))
        api.calls.clear()
        await ctl.discover()
        self.assertNotIn("/categories/" + c["slug"], api.calls, "parked: not asked for again")
        self.assertIn("队列 1 场（1 场在等，见暂不新挂）", ctl.status())
        ctl.unpark()
        self.assertEqual(ctl.queue.qsize(), 0, "its wait is not over")
        clock.ms += p.HOLD_RETRY_MS
        ctl.account_at = clock.ms
        ctl.unpark()
        await self.until(lambda: len(api.sent) >= 2 and not ctl.parked and not ctl.queued)
        self.assertEqual((len(api.sent), ctl.parked, ctl.queued, c["slug"] in ctl.ledger.rows), (2, {}, set(), True))
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        broken = {**c, "slug": "btc-updown-15m-broken", "market_id": "404", "market": None}  # its read raises: parked longer, not read again every poll
        ctl.queue.put_nowait((broken["start_ms"], broken["slug"], broken)); ctl.queued.add(broken["slug"])
        worker = asyncio.create_task(ctl.placement_worker())
        await self.until(lambda: broken["slug"] in ctl.parked)
        worker.cancel(); await asyncio.gather(worker, return_exceptions=True)
        self.assertEqual((ctl.parked[broken["slug"]][0], broken["slug"] in ctl.queued), (clock.ms + p.ERROR_RETRY_MS, True))
        del ctl.parked[broken["slug"]]; ctl.queued.discard(broken["slug"])
        late = {**c, "slug": "btc-updown-15m-late", "start_ms": clock.ms + 100_000}
        ctl.parked[late["slug"]] = (clock.ms, late); ctl.queued.add(late["slug"])
        ctl.unpark()
        self.assertEqual((ctl.parked, ctl.queued, ctl.queue.qsize()), ({}, set(), 0), "too close to its start: dropped")
        pair = ctl.ledger.rows[c["slug"]]
        pair.update(cancel_sent_ms=NOW, cancel_done_ms=NOW + 11_000)  # a withdrawal long before the cancel time: not a measurement
        self.assertIn("撤单确认用时：还没有撤过", ctl.timing_line())
        pair.update(cancel_sent_ms=pair["cancel_ms"], cancel_done_ms=pair["cancel_ms"] + 400)
        self.assertIn("撤单确认用时中位 0.4 秒", ctl.timing_line())
        await self.finish(ctl)

    async def test_snapshot_pace_counts_from_the_reads_start_and_a_failed_read_is_named(self):
        """1.136.1: the pre-open API reads up to 600 pages of open orders (60 — 3,000 orders — was passed by twenty cards with
        four levels a day ahead: the snapshot never completed again and every placement was held) at LIVE_ORDERS_PAGE a
        page; the wait after a read is the pace less the read's duration; the staleness allows for the read's duration; a
        failed read is named by the hold (it used to say 启动后第一次扫描中), with the page count on an incomplete list."""
        ctl, api, clock = controller()
        fresh = p.Controller(ctl.owner)
        self.assertEqual((fresh.api.ORDERS_PAGES_MAX, fresh.api.ORDERS_PAGE, fresh.cancel_api.ORDERS_PAGE), (600, 50, 50))
        fresh.ledger.close()
        wide, _, _ = controller(LIVE_ORDERS_PAGE="100")
        fresh = p.Controller(wide.owner)
        self.assertEqual((fresh.api.ORDERS_PAGE, fresh.cancel_api.ORDERS_PAGE), (100, 100))
        fresh.ledger.close()
        await self.finish(wide)
        ctl.account_pages = 60
        self.assertEqual((ctl.account_interval_s(), ctl.account_stale_ms()), (90.0, 190_000))
        ctl.account_read_s = 55.0
        self.assertEqual(ctl.account_stale_ms(), 245_000, "the read's own duration on top")
        self.assertEqual((ctl.account_wait(40.0, False), ctl.account_wait(100.0, False), ctl.account_wait(0.0, True)), (50.0, 1.0, 10.0))
        self.assertIn("账户快照每 90 秒（60 页挂单，上次读了 55 秒）", ctl.status())
        api.orders_complete = False
        with self.assertRaises(m.RemoteError) as caught:
            await ctl.account()
        self.assertIn("账户挂单分页未读取完整（读了 0 条、0 页仍没到底，上限 ? 页）", str(caught.exception))
        self.assertEqual((ctl.account_at, ctl.hold()), (0, "账户快照上次读取失败（账户挂单分页未读取完整（读了 0 条、0 页仍没到底，上限 ? 页），暂停新增订单），正在重读"))
        api.orders_complete = True
        api.rows["other"] = {"id": "manual", "status": "OPEN", "amount": str(L.to_wei(2)), "amountFilled": "0", "order": {"hash": "0xother", "side": 0, "makerAmount": str(L.to_wei(1))}}
        await ctl.account()
        self.assertEqual((ctl.account_at, ctl.account_failed, ctl.hold(), ctl.account_read_s), (NOW, "", "", 0.0))
        self.assertEqual([r["id"] for r in ctl.account_rows], ["manual"], "the whole account, for the owner (live_full_list)")
        self.assertIn("原策略 2 分钟后自动撤掉（LIVE_ORPHAN_CANCEL）", ctl.status())
        # 1.154.7: the snapshot's pages are the requests it made, and /preopen says what Predict really put on a page — a server cap
        # below LIVE_ORDERS_PAGE (asked for 100, given 50) would otherwise go unseen, the pages reckoned as rows / 100 (half the
        # requests: the snapshot read twice as often as ACCOUNT_PAGE_S means)
        api.ORDERS_PAGE, api.paged_stats = 100, {"/orders": (4, 50)}
        await ctl.account()
        self.assertEqual((ctl.account_pages, ctl.account_page_rows), (4, 50))
        self.assertIn("（4 页挂单，每页 50 张（LIVE_ORDERS_PAGE=100 没生效：Predict 每页最多给 50 张）", ctl.status())
        api.paged_stats = {"/orders": (2, 100)}
        await ctl.account()
        self.assertIn("（2 页挂单，每页 100 张）", ctl.status())
        await self.finish(ctl)

    async def test_a_windows_levels_are_refreshed_with_one_read(self):
        """1.136.1: the due records of one window (its price levels) share a read of the market; a status change withdraws
        them all. Reads of distinct windows are still REFRESH_BATCH an iteration."""
        ctl, api, clock = controller(PREOPEN_MARKETS="btc", PREOPEN_LEVELS="48x20,47x40")
        reads = []
        orig = api.market
        api.market = lambda mid: (reads.append(mid), orig(mid))[1]
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        for n in range(3):
            ctl.ledger.save({**c, "slug": c["slug"] + (f"#{n}" if n else ""), "level": n, "phase": "active", "created_ms": NOW,
                             "orders": {"up": {"state": "open", "id": f"u{n}", "hash": f"h{n}", "filled": 0.0}, "down": {"state": "open", "id": f"d{n}", "hash": f"g{n}", "filled": 0.0}}})
        await ctl.refresh_active()
        self.assertEqual((reads, all(r.get("meta_ms") == NOW for r in ctl.ledger.rows.values())), (["123"], True))
        clock.ms += p.REFRESH_MS
        api.markets["123"]["tradingStatus"] = "CANCEL_ONLY"
        await ctl.refresh_active()
        self.assertEqual((reads, sorted(r["phase"] for r in ctl.ledger.rows.values()), set(api.removed)),
                         (["123", "123"], ["closed"] * 3, {"u0", "d0", "u1", "d1", "u2", "d2"}))
        await self.finish(ctl)

    async def test_active_windows_are_refreshed_in_small_batches(self):
        rows = [market(at=NOW + n * 15 * 60_000, mid=n) for n in range(1, 11)]
        ctl, api, clock = controller(rows, PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="240")
        for n in range(1, 11):
            c = p.candidate(rows[n - 1], NOW, ctl.owner.config, ctl.settings)
            ctl.ledger.save({**c, "phase": "active", "orders": {"up": {"state": "open", "id": f"u{n}", "hash": f"h{n}", "filled": 0.0},
                                                                "down": {"state": "open", "id": f"d{n}", "hash": f"g{n}", "filled": 0.0}}, "created_ms": NOW})
        await ctl.refresh_active()
        read = lambda: sorted(int(p["market_id"]) for p in ctl.ledger.rows.values() if p.get("meta_ms"))
        self.assertEqual(len(read()), p.REFRESH_BATCH)
        await ctl.refresh_active()
        self.assertEqual(read(), list(range(1, 11)), "the rest the next iteration")
        clock.ms += p.REFRESH_MS
        api.markets["3"]["tradingStatus"] = "CANCEL_ONLY"
        await ctl.refresh_active()
        self.assertEqual(ctl.ledger.rows[rows[2]["categorySlug"]]["phase"], "closed")
        await self.finish(ctl)

    async def test_an_active_window_is_reread_and_withdrawn_when_its_market_closes(self):
        ctl, api, clock = controller([market(), market(at=NOW + 3 * 3600_000, mid=8)], PREOPEN_AHEAD_MINUTES="240")
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        api.markets["123"]["tradingStatus"] = "OPEN"
        await ctl.refresh_active()
        self.assertEqual((pair["open_seen_ms"], pair["phase"]), (NOW, "active"))
        api.markets["123"]["tradingStatus"] = "CANCEL_ONLY"
        await ctl.refresh_active()
        self.assertEqual(pair["phase"], "active", "read again only after REFRESH_MS")
        clock.ms += p.REFRESH_MS
        await ctl.refresh_active()
        self.assertEqual((pair["phase"], pair["reason"]), ("closed", "市场状态 REGISTERED / CANCEL_ONLY"))
        self.assertEqual(set(api.removed), {"1", "2"})
        far = p.candidate(market(at=NOW + 3 * 3600_000, mid=8), clock.ms, ctl.owner.config, ctl.settings)
        await ctl.place_pair(far)
        await ctl.refresh_active()
        api.markets["8"]["tradingStatus"] = "CANCEL_ONLY"
        clock.ms += p.REFRESH_MS
        await ctl.refresh_active()
        self.assertEqual(ctl.ledger.rows[far["slug"]]["phase"], "active", "more than an hour away: read again only after REFRESH_FAR_MS")
        clock.ms += p.REFRESH_FAR_MS
        await ctl.refresh_active()
        self.assertEqual(ctl.ledger.rows[far["slug"]]["phase"], "closed")
        await self.finish(ctl)

    async def test_own_budget_only_the_model_traders_cap_and_exposure_are_not_this_modes(self):
        """1.132.1: PREOPEN_MAX_OPEN_USD bounds this mode alone — LIVE_MAX_OPEN_USD and the model trader's exposure no longer hold
        it back (nor does its reservation squeeze the model trader: live_fit_shares)."""
        ctl, api, _ = controller(LIVE_MAX_OPEN_USD="48")
        ctl.owner.live_exposure = lambda trades: 60.
        await ctl.place_pair(p.candidate(market(), NOW, ctl.owner.config, ctl.settings))
        self.assertEqual(len(api.sent), 2)
        ctl.settings = dataclasses.replace(ctl.settings, max_open_usd=50)
        c2 = p.candidate(market("btc", at=NOW + 30 * 60_000, mid=9), NOW, ctl.owner.config, ctl.settings)
        self.assertEqual((await ctl.place_pair(c2), len(api.sent)), ("held", 2))
        self.assertIn("已预留 49.00U，再挂一场双边要 49U，限额 50U", ctl.error)
        await self.finish(ctl)

    async def test_discovery_reads_a_few_windows_a_pass_and_a_failed_read_strands_nothing(self):
        """1.132.2: a pass reads DISCOVER_READS listed windows with a level still to quote (the soonest first), each card's
        next listing besides; the rest wait for the next pass. A read that fails mid-pass ends the pass with the error kept,
        what was found is queued, and nothing stays marked queued without being on the queue (every listed window used to be
        read every poll, and a 429 mid-pass left the ones found before it unquoted for good)."""
        rows = [market(at=NOW + n * 15 * 60_000, mid=n) for n in range(1, 13)]
        ctl, api, clock = controller(rows, PREOPEN_AHEAD_MINUTES="240", PREOPEN_MARKETS="btc")
        ctl.listed_by_card["BTC-15M"] = NOW + 12 * 15 * 60_000  # the twelve known listed (seen before), none quoted yet: the budget's
        reads = lambda: sum(1 for c in api.calls if c.startswith("/categories/"))
        await ctl.discover()  # a full scan: the budget's eight, then the four unlisted beyond the listing (MISS_STOP, read whatever the budget)
        self.assertEqual((ctl.queue.qsize(), reads()), (p.DISCOVER_READS, p.DISCOVER_READS + p.MISS_STOP))
        self.assertIn(f"读 {p.DISCOVER_READS + p.MISS_STOP} 场，找到 {p.DISCOVER_READS} 档，还有 {12 - p.DISCOVER_READS} 场下次再读", ctl.scan_note)
        self.assertIn("\n扫描：", ctl.status())
        api.calls.clear()
        await ctl.discover()  # a quick scan: the four left, then the first beyond the listing (read whatever the budget; unlisted: one miss ends the card)
        self.assertEqual((ctl.queue.qsize(), reads()), (12, 12 - p.DISCOVER_READS + 1))
        self.assertEqual(sorted(ctl.queued), sorted(c["categorySlug"] for c in rows))
        while ctl.queue.qsize():
            ctl.queue.get_nowait()
        ctl.queued.clear()
        real = api.call
        async def flaky(method, path, **kw):
            if path.endswith(f"-{(NOW + 3 * 15 * 60_000) // 1000}"):
                raise m.RemoteError("HTTP 429: rate limited", 20)
            return await real(method, path, **kw)
        api.call = flaky
        ctl.full_scan_ms = 0
        await ctl.discover()
        queued = [ctl.queue.get_nowait()[1] for _ in range(ctl.queue.qsize())]
        self.assertEqual((len(queued), sorted(ctl.queued)), (2, sorted(queued)), "the two found before the failed read: queued, and nothing else marked queued")
        self.assertTrue(ctl.error.startswith("扫描 ") and "HTTP 429" in ctl.error and "停在" in ctl.scan_note, (ctl.error, ctl.scan_note))
        api.call = real
        ctl.api.blocked_until = time.monotonic() + 10  # the shared cooldown: no pass at all, said so
        await ctl.discover()
        self.assertIn("没扫：", ctl.scan_note)
        ctl.api.blocked_until = 0.0
        await self.finish(ctl)

    async def test_listings_are_probed_first_and_queued_at_once_the_details_read_only_when_needed(self):
        """1.133.0: a pass probes each card's next listing before re-reading the older windows (they used to come first, in
        start order) and queues the candidate the moment it is found; a probe-only pass reads nothing else; a category
        answer carrying every field an order needs is placed on without the details read; the pair records when it was
        found and when it was up, the status the span; the account snapshot reads outside the placement lock."""
        rows = [market(at=NOW + n * 15 * 60_000, mid=n) for n in range(1, 14)]
        ctl, api, clock = controller(rows, PREOPEN_AHEAD_MINUTES="240", PREOPEN_MARKETS="btc")
        ctl.listed_by_card["BTC-15M"] = NOW + 12 * 15 * 60_000  # twelve known listed, none quoted; the thirteenth is the next listing
        ctl.listed_at["BTC-15M"] = NOW - p.LISTING_CYCLE_MS - 3_000  # expected 3 s ago: the fast poll is on — the listing alone is probed
        self.assertTrue(ctl.sprinting())
        await ctl.discover()
        cats = [c for c in api.calls if c.startswith("/categories/")]
        self.assertEqual((cats[0], len(cats), ctl.queue.qsize()), (window_slug_of(NOW + 13 * 15 * 60_000), 4, 1),
                         "the next listing first (then, a full scan, the three unlisted beyond it); none of the older windows while sprinting")
        self.assertEqual(ctl.listed_by_card["BTC-15M"], NOW + 13 * 15 * 60_000)
        self.assertEqual(list(ctl.listing_deltas), [3_000], "seen 3 s after the expected moment")
        self.assertIn("探 1 卡、读 4 场，找到 1 档", ctl.scan_note)
        ctl.listed_at.clear()  # not sprinting: the older windows follow the listing probe in the same pass
        api.calls.clear()
        await ctl.discover()
        cats = [c for c in api.calls if c.startswith("/categories/")]
        self.assertEqual(cats[0], window_slug_of(NOW + 14 * 15 * 60_000), "the window beyond the listing is asked first (unlisted: one miss ends the probe)")
        self.assertEqual((ctl.queue.qsize(), len(cats)), (1 + p.DISCOVER_READS, 1 + p.DISCOVER_READS))
        api.calls.clear()
        await ctl.discover(probe_only=True)
        self.assertEqual([c for c in api.calls if c.startswith("/categories/")], [window_slug_of(NOW + 14 * 15 * 60_000)], "a probe-only pass: the listing alone")
        self.assertEqual(ctl.awaiting, {"BTC-15M": NOW + 14 * 15 * 60_000 - 240 * 60_000}, "the next window entered the range half an hour ago, not listed: no sprint any more")
        self.assertEqual(ctl.poll_wait(), p.LISTING_PROBE_S)
        clock.ms = ctl.awaiting["BTC-15M"] + 60_000
        self.assertEqual(ctl.poll_wait(), p.FAST_POLL_S, "a minute after it entered: sprinting")
        self.assertIn("冲刺中：BTC-15M 的下一场已进范围", ctl.status())
        clock.ms = NOW
        # the category answer carrying every field: placed on as it is, the details not read
        real = api.call
        async def full(method, path, **kw):
            answer = await real(method, path, **kw)
            mkt = next(mk for mk in api.markets.values() if mk["categorySlug"] == path.removeprefix("/categories/"))
            answer["data"]["markets"][0] = {**mkt, "title": mkt["categorySlug"], "question": "BTC up or down?", "isNegRisk": False, "isYieldBearing": False}
            return answer
        api.call = full
        got = await p.category_market(api, rows[0]["categorySlug"])
        self.assertEqual((got["_from"], got["id"], got["categorySlug"]), ("category", "1", rows[0]["categorySlug"]))
        self.assertFalse(p.listing_complete({**rows[0], "isNegRisk": False}), "a field short: the details are read")
        api.call = real
        self.assertEqual((await p.category_market(api, rows[0]["categorySlug"]))["_from"], "details")
        # the pair keeps when it was found and when it was up; the status measures the span
        while ctl.queue.qsize():
            c = ctl.queue.get_nowait()[2]
        ctl.queued.clear()
        c = p.candidate(rows[0], NOW, ctl.owner.config, ctl.settings)
        c.update(market=rows[0], read_ms=NOW - 1_200)
        clock.ms = NOW
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        self.assertEqual((pair["seen_ms"], pair["placed_ms"]), (NOW - 1_200, NOW))
        self.assertIn("发现→挂上中位 1.2 秒（最长 1.2；1 场）", ctl.status())
        self.assertIn("发布比预计中位 +3 秒", ctl.status())
        # 1.154.63: a listing found while this start's first account snapshot was still being read waited for it — said apart,
        # not in the median (11-26 16:25:54: 27 s, the settings just saved; the listings before and after it 2.5 s and less)
        ctl.started_ms, ctl.first_snapshot_ms = NOW - 60_000, NOW - 10_000
        pair["seen_ms"] = NOW - 37_200  # found 23 s after the start, placed 37 s on — once the snapshot was read
        self.assertNotIn("发现→挂上中位", ctl.timing_line())
        self.assertIn("另有 1 场是本次启动后等第一次账户快照读完才挂上的（发现→挂上 37～37 秒；启动后 50 秒读完——改设置、重新部署都会重新启动）", ctl.timing_line())
        pair["seen_ms"] = NOW - 70_000  # found before this start (the record an earlier run's): counted as ever
        self.assertIn("发现→挂上中位 70.0 秒（最长 70.0；1 场）", ctl.timing_line())
        self.assertNotIn("另有", ctl.timing_line())
        pair["seen_ms"], ctl.first_snapshot_ms = NOW - 1_200, 0
        # the account snapshot: dated when its read began, applied under the lock, the lock free during the read
        started = clock.ms
        async def slow_orders(status="OPEN"):
            clock.ms += 1_500
            self.assertFalse(ctl.account_lock.locked(), "the page reads hold no lock")
            return []
        api.orders = slow_orders
        await ctl.account()
        self.assertEqual(ctl.account_at, started)
        await self.finish(ctl)

    async def test_an_hk_card_has_no_daily_limit_but_the_halt_guard(self):
        """1.134.0: 智谱 (HKEX 2513) — no daily price limit, so of the stock guard only the halt rule: a quote standing still in a
        continuous-trading segment; the 12:00–13:00 break not counted, the closing auction (from 16:00) not judged. 黄金 and
        原油 have no exchange: nothing is judged."""
        zh = next(r for r in m.WINDOW_UPDOWN_MARKETS if r.key == "ZHIPU-15M")
        self.assertEqual((zh.stem, zh.symbol, zh.exchange, zh.limit_pct, zh.venue), ("zhipu", "ZHIPUUSDT", "hk", 0.0, "futures"))
        self.assertEqual([(r.key, r.stem, r.exchange, r.venue) for r in m.WINDOW_UPDOWN_MARKETS if r.key in {"XAU-15M", "CL-15M"}],
                         [("XAU-15M", "xau", "", "futures"), ("CL-15M", "cl", "", "futures")])
        ctl, api, clock = controller()
        quote = lambda last, prev="100", quoted=None, fetched=None: types.SimpleNamespace(last=m.D(last), prev_close=m.D(prev),
                                                                                       quoted_ms=clock.ms if quoted is None else quoted, fetched_ms=clock.ms if fetched is None else fetched)
        clock.ms = NOW + 3600_000  # 10:00 Beijing: the morning session
        ctl.owner.updowns = {"ZHIPU-15M": types.SimpleNamespace(lead=quote("119")), "XAU-15M": types.SimpleNamespace(lead=quote("4000", "3000", quoted=clock.ms - 3600_000))}
        self.assertEqual(ctl.card_hold(zh), "", "up 19% on the day: HKEX has no limit")
        self.assertEqual(ctl.card_hold(next(r for r in m.WINDOW_UPDOWN_MARKETS if r.key == "XAU-15M")), "", "no exchange: nothing judged")
        ctl.owner.updowns["ZHIPU-15M"].lead = quote("119", quoted=clock.ms - 4 * 60_000)
        self.assertIn("智谱 交易时段里成交时间 4 分钟没动", ctl.card_hold(zh))
        clock.ms = NOW + 3 * 3600_000 + 2 * 60_000  # 12:02: the break — nothing prints, no halt
        ctl.owner.updowns["ZHIPU-15M"].lead = quote("119", quoted=NOW + 3 * 3600_000 - 60_000)
        self.assertEqual(ctl.card_hold(zh), "")
        clock.ms = NOW + 4 * 3600_000 + 12 * 60_000  # 13:12, the last print 11:59: 13 minutes of trading without one
        ctl.owner.updowns["ZHIPU-15M"].lead = quote("119", quoted=NOW + 3 * 3600_000 - 60_000)
        self.assertIn("成交时间 13 分钟没动", ctl.card_hold(zh))
        clock.ms = NOW + 7 * 3600_000 + 5 * 60_000  # 16:05: the closing auction
        ctl.owner.updowns["ZHIPU-15M"].lead = quote("119", quoted=clock.ms - 4 * 60_000)
        self.assertEqual(ctl.card_hold(zh), "")
        await self.finish(ctl)

    async def test_one_sentinel_probes_for_the_cards_listed_together_and_the_snapshot_paces_itself(self):
        """1.134.1: of the cards whose next windows start together one is probed a pass (the turn rotating), the rest read the
        moment it is seen listed; the account snapshot's pace and staleness follow its page count; the status shows the request
        rate."""
        ctl, api, clock = controller([market(at=NOW + 55 * 60_000, mid=99)], PREOPEN_MARKETS="btc,skhynix")  # nothing listed on a quarter hour
        ctl.full_scan_ms = clock.ms  # quick scans: a card's first unlisted window alone
        await ctl.discover()
        self.assertEqual(len(api.calls), 1, "the two cards' 09:15 windows: one sentinel probed")
        first = api.calls[0]
        api.calls.clear()
        await ctl.discover()
        self.assertEqual(len(api.calls), 1)
        self.assertNotEqual(api.calls[0], first, "the turn rotates")
        api.markets.update({"5": market(at=NOW + 15 * 60_000, mid=5), "6": market("skhynix", at=NOW + 15 * 60_000, mid=6)})
        api.calls.clear()
        await ctl.discover()
        self.assertEqual((ctl.queue.qsize(), sum(1 for c in api.calls if c.endswith(f"-{(NOW + 15 * 60_000) // 1000}"))), (2, 2), "the sentinel listed: the other read in the same pass")
        # the snapshot's pace: 23 pages of orders every 35 s, stale after 79 s; a page every poll (5 s), stale after 30 s
        ctl.account_pages = 23
        self.assertEqual((ctl.account_interval_s(), ctl.account_stale_ms()), (34.5, 79_000))
        ctl.account_at = clock.ms - 44_000
        self.assertEqual(ctl.hold(), "", "44 s old: fresh enough for a 23-page snapshot")
        self.assertIn("账户快照每 34 秒（23 页挂单）", ctl.status())
        ctl.account_pages = 1
        self.assertEqual((ctl.account_interval_s(), ctl.account_stale_ms()), (5.0, 30_000))
        self.assertIn("账户快照过期（44 秒前读到，每 5 秒读一次）", ctl.hold())
        ctl.account_at = clock.ms
        await ctl.account()
        self.assertEqual(ctl.account_pages, 1)
        self.assertIsNone(ctl.request_rate())
        ctl.rate_samples.extend([(clock.ms - 60_000, 100), (clock.ms, 190)])
        self.assertAlmostEqual(ctl.request_rate(), 1.5)
        self.assertIn("请求约 1.5 次/秒（最近一分钟）", ctl.status())
        await self.finish(ctl)

    async def test_a_stock_cards_first_window_after_a_session_open_is_not_quoted(self):
        """1.135.1: a stock card's windows starting within PREOPEN_OPEN_QUIET_MINUTES (15) of a session segment's open — the
        day's open, the afternoon's — are left out: the auction's indicative price and the break's news are public before the
        index moves. The round-the-clock cards are untouched; 0 quotes them all (the suite's default)."""
        ctl, api, clock = controller(PREOPEN_OPEN_QUIET_MINUTES="15", PREOPEN_MARKETS="skhynix,unitree,btc", PREOPEN_AHEAD_MINUTES="300")
        self.assertEqual(ctl.settings.open_quiet_minutes, 15)
        starts = {(r.key, (s - NOW) // 60_000) for r, s in p.window_starts(NOW, ctl.owner.config, ctl.settings)}
        self.assertNotIn(("UNITREE-15M", 30), starts, "09:30: the SSE open")
        self.assertIn(("UNITREE-15M", 45), starts)
        self.assertNotIn(("UNITREE-15M", 240), starts, "13:00: after the break")
        self.assertIn(("UNITREE-15M", 255), starts)
        self.assertIn(("SKHYNIX-15M", 15), starts, "10:15 KST: mid-session")
        self.assertIn(("BTC-15M", 30), starts, "no exchange: round the clock")
        unitree = next(r for r in m.WINDOW_UPDOWN_MARKETS if r.key == "UNITREE-15M")
        sk = next(r for r in m.WINDOW_UPDOWN_MARKETS if r.key == "SKHYNIX-15M")
        self.assertTrue(p.session_opening(unitree, NOW + 30 * 60_000, frozenset(), 15 * 60_000))
        self.assertTrue(p.session_opening(sk, NOW - 60 * 60_000, frozenset(), 15 * 60_000), "08:00 Beijing = 09:00 KST, the KRX open")
        self.assertFalse(p.session_opening(sk, NOW - 45 * 60_000, frozenset(), 15 * 60_000))
        self.assertFalse(p.session_opening(unitree, NOW + 30 * 60_000, frozenset(), 0), "0: off")
        self.assertIn("股票卡开盘、午后头 15 分钟内开始的场次不挂", ctl.status())
        self.assertEqual(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline"}).preopen.open_quiet_minutes, 15, "the default")
        await self.finish(ctl)
        with tempfile.TemporaryDirectory() as folder:  # quoted before the rule (the suite's 0): the next start withdraws it, a free withdrawal
            db = {"STATE_DB": str(Path(folder) / "state.sqlite3")}
            ctl, api, clock = controller(PREOPEN_MARKETS="unitree", **db)
            c = p.candidate(market("unitree", at=NOW + 30 * 60_000), NOW, ctl.owner.config, ctl.settings)
            await ctl.place_pair(c)
            self.assertEqual(ctl.ledger.rows[c["slug"]]["phase"], "active")
            ctl.keep_on_stop = True
            await ctl.on_stop()
            ctl.ledger.close()
            again, _, _ = controller(PREOPEN_MARKETS="unitree", PREOPEN_OPEN_QUIET_MINUTES="15", **db)
            again.api = again.cancel_api = api; api.ctl = again
            again.adopt()
            pair = again.ledger.rows[c["slug"]]
            self.assertEqual((pair["phase"], pair["reason"], pair.get("free"), again.resumed), ("closing", "设置不同，撤掉后重新报价", True, 0))
            await again.cancel_pair(pair, pair["reason"])
            await again.discover()
            self.assertEqual(again.queue.qsize(), 0, "not quoted again: the window is left out")
            await self.finish(again)

    async def test_gold_and_oil_are_not_quoted_while_cme_is_shut(self):
        """1.150.1: gold and oil follow CME Globex — shut 17:00–18:00 New York time every day and from Friday 17:00 to Sunday
        18:00, their index standing still — and Predict lists their windows through the shut: 11-16 the mode's Down bids on
        every 17:00–18:00 ET window, and the Up ones on 18:00–18:15, were filled hours ahead, the other side left. A window
        touching the shut is not quoted (one resting from before is withdrawn at the next start, free), nor one starting
        within PREOPEN_OPEN_QUIET_MINUTES of the 18:00 reopen; the crypto cards and the model trader's windows are untouched."""
        et = lambda h, mi, day=16, month=11, offset=-5: m.et_ms(2026, month, day, h, mi, offset)
        now = et(8, 30)  # Monday 11-16 21:30 Beijing (EST): the screenshot's moment
        ctl, api, clock = controller(PREOPEN_MARKETS="xau,cl,btc", PREOPEN_AHEAD_MINUTES="1440", PREOPEN_OPEN_QUIET_MINUTES="15")
        starts = p.window_starts(now, ctl.owner.config, ctl.settings)
        by = lambda key, got=starts: [s for r, s in got if r.key == key]
        shut = {et(17, 0), et(17, 15), et(17, 30), et(17, 45)}  # 06:00–07:00 Beijing on 11-17
        for key in ("XAU-15M", "CL-15M"):
            self.assertFalse(set(by(key)) & shut, key)
            self.assertNotIn(et(18, 0), by(key), "the reopen's first window: quiet 15")
            self.assertTrue({et(16, 30), et(18, 15)} <= set(by(key)), "the windows before the shut and after the reopen's quiet: quoted")
            self.assertEqual(len(by(key)), len(by("BTC-15M")) - 5 - (3 if key == "XAU-15M" else 1),
                             "(the leaning windows too: gold's 05:00, 05:15 and 16:45 New York, 1.154.19 / 1.154.33; oil's 00:45, 1.154.33)")
        self.assertTrue(et(16, 45) in by("CL-15M") and et(16, 45) not in by("XAU-15M"), "the last window before the shut, ending at 17:00: oil's quoted, gold's leans")
        self.assertTrue(shut | {et(18, 0)} <= set(by("BTC-15M")), "round the clock: untouched")
        xau = next(r for r in m.WINDOW_UPDOWN_MARKETS if r.key == "XAU-15M")
        why = "黄金休市（纽约时间每天 17:00–18:00、周五 17:00 到周日 18:00，CME 节假日从 12:00 起）"
        self.assertEqual(p.candidate_problem(market("xau", at=et(17, 15), mid=41), now, ctl.owner.config, ctl.settings), why)
        self.assertIsNone(p.candidate(market("xau", at=et(17, 15), mid=41), now, ctl.owner.config, ctl.settings))
        self.assertEqual(p.candidate_problem(market("xau", at=et(16, 30), mid=42), now, ctl.owner.config, ctl.settings), "")
        self.assertEqual(p.candidate_problem(market("btc", at=et(17, 15), mid=43), now, ctl.owner.config, ctl.settings), "")
        self.assertTrue(xau.exists(et(17, 15)) and xau.current(et(17, 20)).start_ms == et(17, 15) - 60_000, "the model trader's card: as before")
        # the weekend: Friday 17:00 to Sunday 18:00, then the reopen's first window left out too
        friday = p.window_starts(et(16, 0, 20), ctl.owner.config, ctl.settings)
        self.assertEqual(by("XAU-15M", friday), [et(16, 15, 20), et(16, 30, 20)], "(16:45, the last before the shut: gold's leans — 1.154.33)")
        self.assertEqual(len(by("BTC-15M", friday)), 96)
        self.assertEqual(min(by("XAU-15M", p.window_starts(et(12, 0, 22), ctl.owner.config, ctl.settings))), et(18, 15, 22), "Sunday's reopen")
        # New York's clock, DST followed: 05:00 Beijing in summer; the shut that spans the switch (11-01) keeps both ends right
        self.assertTrue(p.market_shut(xau, et(17, 0, 15, 7, -4)))
        self.assertFalse(p.market_shut(xau, et(16, 45, 15, 7, -4)))
        self.assertEqual(dt.datetime.fromtimestamp(et(17, 0, 15, 7, -4) / 1000, m.BEIJING).strftime("%m-%d %H:%M"), "07-16 05:00")
        self.assertEqual(m.cme_shuts(et(12, 0, 30, 10, -4), et(12, 0, 2)), [(et(17, 0, 30, 10, -4), et(18, 0, 1))])
        self.assertEqual(m.cme_shuts(et(16, 45), et(17, 0)) + m.cme_shuts(et(18, 0), et(18, 15)), [], "the boundaries: a window may end at the shut, start at the reopen")
        self.assertIn("黄金、原油休市（纽约时间每天 17:00–18:00、周五 17:00 到周日 18:00，CME 节假日从 12:00 起）期间的场次不挂、重开后头 15 分钟内开始的也不挂（北京时间 11-07 06:00–11-09 07:00）", ctl.status())
        await self.finish(ctl)
        quiet0, _, _ = controller(PREOPEN_MARKETS="xau")  # the suite's quiet 0 — 1.151.1: the reopen's first window is not quoted all the same (fixed)
        got = set(by("XAU-15M", p.window_starts(now, quiet0.owner.config, dataclasses.replace(quiet0.settings, ahead_minutes=1440))))
        self.assertEqual((et(18, 0) in got, et(18, 15) in got, bool(got & shut)), (False, True, False))
        self.assertIn("黄金休市（纽约时间每天 17:00–18:00、周五 17:00 到周日 18:00，CME 节假日从 12:00 起）期间的场次不挂、重开后头 15 分钟内开始的也不挂（北京时间", quiet0.status())
        longer = set(by("XAU-15M", p.window_starts(now, quiet0.owner.config, dataclasses.replace(quiet0.settings, ahead_minutes=1440, open_quiet_minutes=30))))
        self.assertEqual((et(18, 0) in longer, et(18, 15) in longer, et(18, 30) in longer), (False, False, True), "the setting lengthens it")
        await self.finish(quiet0)
        btc, _, _ = controller(PREOPEN_MARKETS="btc")
        self.assertNotIn("休市", btc.status())
        await self.finish(btc)
        with tempfile.TemporaryDirectory() as folder:  # quoted by an older version: the next start withdraws it, a free withdrawal, not quoted again
            db = {"STATE_DB": str(Path(folder) / "state.sqlite3")}
            at = et(17, 15, 6)  # Friday 11-06 17:15 ET: 21 hours after the suite's NOW
            ctl, api, clock = controller([market("xau", at=at, mid=51)], PREOPEN_MARKETS="xau", PREOPEN_AHEAD_MINUTES="1440", **db)
            with patch.object(p, "market_shut", lambda rolling, start: ""):
                c = p.candidate(market("xau", at=at, mid=51), NOW, ctl.owner.config, ctl.settings)
                await ctl.place_pair(c)
            self.assertEqual(ctl.ledger.rows[c["slug"]]["phase"], "active")
            ctl.keep_on_stop = True
            await ctl.on_stop()
            ctl.ledger.close()
            again, _, _ = controller([market("xau", at=at, mid=51)], PREOPEN_MARKETS="xau", PREOPEN_AHEAD_MINUTES="1440", **db)
            again.api = again.cancel_api = api; api.ctl = again
            again.adopt()
            pair = again.ledger.rows[c["slug"]]
            self.assertEqual((pair["phase"], pair.get("free"), again.resumed), ("closing", True, 0))
            await again.cancel_pair(pair, pair["reason"])
            await again.discover()
            self.assertEqual(again.queue.qsize(), 0, "not quoted again: the window is left out")
            await self.finish(again)

    async def test_no_new_pair_in_the_rule_takers_seconds(self):
        """1.154.23 — the rule taker first. The windows listed at a quarter hour are signed and sent in the very seconds the
        rule taker's offers show (2.4–7.2 s after it on 11-18): two signatures and three saves a pair, each on the event loop
        that taker's order waits for. While the bot says those seconds run (rules_quiet) no new pair goes out — held, parked
        for that long and placed right after; the account snapshot's digest (a tenth of a second of the loop) waits them out;
        the cancels at their time go on."""
        ctl, api, clock = controller()
        quiet = [0.0]
        ctl.owner.rules_quiet = lambda: quiet[0]
        ctl.account_at = clock.ms  # (a snapshot in hand: nothing else holds)
        self.assertEqual((ctl.yielding(), ctl.hold()), (0.0, ""))
        quiet[0] = 12.4
        self.assertEqual((ctl.yielding(), ctl.hold()), (12.4, "规则吃单在抢单时刻，先让路（约 12 秒后继续）"))
        c = p.candidate(market(), clock.ms, ctl.owner.config, ctl.settings)
        self.assertEqual((await ctl.place_pair(c), api.sent, len(ctl.ledger.rows)), ("held", [], 0), "nothing signed, saved or sent")
        # the worker parks it for those seconds — not HOLD_RETRY_MS — and it is placed as they end
        quiet[0] = 0.3
        ctl.queue.put_nowait((p.queue_rank(c), c["slug"], c))
        ctl.queued.add(c["slug"])
        worker = asyncio.create_task(ctl.placement_worker())
        for _ in range(20):
            await asyncio.sleep(0)
            if c["slug"] in ctl.parked:
                break
        self.assertEqual(ctl.parked[c["slug"]][0] - clock.ms, 400, "parked for the rule taker's seconds (and a tenth)")
        quiet[0] = 16.0  # ...for all of them, however many: not tried again every HOLD_RETRY_MS meanwhile
        del ctl.parked[c["slug"]]
        ctl.queue.put_nowait((p.queue_rank(c), c["slug"], c))
        for _ in range(20):
            await asyncio.sleep(0)
            if c["slug"] in ctl.parked:
                break
        self.assertEqual(ctl.parked[c["slug"]][0] - clock.ms, 16_100)
        ctl.parked[c["slug"]] = (clock.ms + 400, c)
        quiet[0] = 0.0
        clock.ms += 400
        ctl.unpark()
        for _ in range(200):
            await asyncio.sleep(0)
            if c["slug"] in ctl.ledger.rows and not ctl.placing:
                break
        self.assertEqual((len(api.sent), list(ctl.ledger.rows)), (2, [c["slug"]]), "placed right after")
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        # what holds the loop for a while waits those seconds out, GIVE_WAY_MAX_S at the most
        quiet[0] = 0.05
        asyncio.get_running_loop().call_later(0.02, lambda: quiet.__setitem__(0, 0.0))
        began = time.monotonic()
        await ctl.give_way()
        self.assertTrue(0.02 <= time.monotonic() - began < 10.0)
        quiet[0] = 99.0
        with patch.object(p, "GIVE_WAY_MAX_S", 0.05):
            began = time.monotonic()
            await ctl.give_way()
            self.assertTrue(0.04 <= time.monotonic() - began < 10.0, "never for good")
        # the account snapshot: read as ever, its digest after those seconds — dated when its read began all the same
        quiet[0] = 0.06
        asyncio.get_running_loop().call_later(0.03, lambda: quiet.__setitem__(0, 0.0))
        ctl.account_at, began = 0, time.monotonic()
        await ctl.account()
        self.assertTrue(0.03 <= time.monotonic() - began < 10.0 and ctl.account_at == clock.ms and ctl.hold() == "", (time.monotonic() - began, ctl.account_at))
        # a bot without a rule taker (or one that cannot answer) keeps nobody waiting
        del ctl.owner.rules_quiet
        self.assertEqual((ctl.yielding(), ctl.hold()), (0.0, ""))
        ctl.owner.rules_quiet = lambda: 1 / 0
        self.assertEqual(ctl.yielding(), 0.0)
        await self.finish(ctl)

    async def test_the_positions_check_is_told_a_fill_before_its_record_is_saved(self):
        """1.154.22 — the model trader's positions check asks this mode, once a minute, what its orders filled (30 days) and
        which outcomes hold a fill of its own not read for certain yet; both walked every record's orders (78,000 on 11-18) and
        now go by the ledger's own two lists (Ledger.filled / unsure). The answers are the walks' at every moment — also between
        a fill being written onto an order and its record being saved: the reads that follow (the other side's, the final
        records') are waited for in between, and the check may run then. Told the mode's fills without that one, with the
        order already read final, it would take those shares for nobody's and could book them onto an order of the model
        trader's own (the double count of 1.131.5)."""
        with tempfile.TemporaryDirectory() as folder:
            ctl, api, clock = controller(STATE_DB=str(Path(folder) / "state.sqlite3"))
            pairs = await self.book_of(ctl, api, 4)
            rows = lambda: list(ctl.ledger.rows.values())

            def same(note):
                self.assertEqual(ctl.filled_by_outcome(30), p.fills_by_outcome(rows(), ctl.now(), 30), note)
                self.assertEqual(list(ctl.filled_by_outcome(30).items()), list(p.fills_by_outcome(rows(), ctl.now(), 30).items()), note)
                self.assertEqual(ctl.unverified_outcomes(), p.unverified_outcomes(rows(), ctl.now()), note)
            same("four resting pairs")
            self.assertEqual((ctl.filled_by_outcome(30), len(ctl.unverified_outcomes()) > 0, len(ctl.ledger.unsure)), ({}, True, 4))
            # the cancellation loop's read of one side: filled whole, read final — and the other side's read still to come (monitor_pair)
            pair, order = pairs[1], pairs[1]["orders"]["up"]
            ctl.apply_fill(order, {"id": order["id"], "status": "FILLED", "amount": "30000000000000000000", "amountFilled": "30000000000000000000"}, pair)
            order["state"] = "final"
            key = (str(pair["market_id"]), int(order["index_set"]))
            self.assertGreater(order["filled"], 0)
            self.assertEqual(ctl.filled_by_outcome(30), {key: order["filled"]}, "counted among the mode's fills before the record is saved")
            same("a fill applied, its record not saved yet")
            ctl.ledger.save(pair)
            same("saved")
            # a second pair's fill, an earlier record: summed in the records' own order; every order final: no outcome unread
            other, o2 = pairs[0], pairs[0]["orders"]["down"]
            ctl.apply_fill(o2, {"id": o2["id"], "status": "OPEN", "amount": "30000000000000000000", "amountFilled": "10000000000000000000"}, other)
            same("a part filled, not saved")
            for x in pairs:
                for o in x["orders"].values():
                    o["state"] = "final"
            ctl.ledger.save_many(pairs)
            same("all final")
            self.assertEqual((ctl.unverified_outcomes(), ctl.ledger.unsure, len(ctl.ledger.filled)), (set(), {}, 2))
            # a pair that is not the ledger's own record (a copy): its fill is no fill of the ledger's — the walk does not see it either
            ghost = copy.deepcopy(pairs[2])
            ctl.apply_fill(ghost["orders"]["up"], {"id": "x", "status": "FILLED", "amount": "5", "amountFilled": "5"}, ghost)
            same("a copy's fill")
            ctl.ledger.delete(pairs[1]["slug"])
            same("a filled record deleted")
            await self.finish(ctl)

    async def test_the_loops_walk_the_working_records_and_the_ledgers_own_counts(self):
        """1.154.20 — 11-17: 39,116 records, 26,956 of them accounted and only history, and every walk of the loops went through
        all of them: the day's P&L a dozen times a second (withdrawing: every placement's gates, the cancellation loop), the
        due list twice a second, the attempts and the failures at every discovery, the orders' ids and hashes at every account
        snapshot — a quarter of the one event loop the model trader's orders share. The ledger keeps what those walks need as
        its records are saved; each answer is the walk's own."""
        def walked(ctl):  # what the walks of every record gave (1.154.19)
            rows = list(ctl.ledger.rows.values())
            day = dt.datetime.fromtimestamp(ctl.now() / 1000, m.BEIJING).date().isoformat()
            ids, hashes, attempts = set(), set(), {}
            for pair in rows:
                attempts[p.record_key(pair)] = max(attempts.get(p.record_key(pair), 0), int(pair.get("attempt") or 0) + 1)
                for order in pair["orders"].values():
                    if order.get("id"):
                        ids.add(str(order["id"]))
                    hashes.update(str(order[k]).lower() for k in ("hash", "api_hash") if order.get(k))
            return {"pending": [x["slug"] for x in rows if not x.get("accounted")],
                    "pnl": sum(float(x.get("pnl") or 0) for x in rows if x.get("accounted_day") == day),
                    "known": (ids, hashes), "strikes": p.window_strikes(rows), "attempts": attempts,
                    "windows": len({p.window_of(x) for x in rows})}

        def same(ctl, note=""):
            want = walked(ctl)
            self.assertEqual([x["slug"] for x in ctl.ledger.pending()], want["pending"], note)
            self.assertEqual(ctl.daily_pnl(), want["pnl"], note)
            self.assertEqual(ctl.known(), want["known"], note)
            self.assertEqual(ctl.ledger.strikes(), want["strikes"], note)
            self.assertEqual({k: p.Attempts(ctl.ledger).get(k, 0) for k in want["attempts"]}, want["attempts"], note)
            self.assertEqual((p.Attempts(ctl.ledger).get("没有这一场", 0), len(ctl.ledger.windows)), (0, want["windows"]), note)
            return want

        with tempfile.TemporaryDirectory() as folder:
            db = {"STATE_DB": str(Path(folder) / "state.sqlite3")}
            ctl, api, clock = controller(LIVE_MAX_DAILY_LOSS_USD="100", **db)
            pairs = await self.book_of(ctl, api, 12)
            same(ctl, "twelve resting pairs")
            day = dt.datetime.fromtimestamp(clock.ms / 1000, m.BEIJING).date().isoformat()
            # settled with a result: a P&L of the day (three of them, so the sum's order matters); withdrawn with nothing filled: 0;
            # one that failed by its own doing counts against its window; one accounted yesterday counts on no figure of today's
            for k, pnl in ((1, -12.3), (4, 7.1), (9, -0.7)):
                pairs[k].update(phase="closed", accounted=True, accounted_day=day, pnl=pnl, result="up")
            pairs[2].update(phase="closed", accounted=True, accounted_day=day, pnl=0.0, reason="到时撤单或真实交易暂停", free=True)
            pairs[3].update(phase="closed", accounted=True, accounted_day=day, pnl=0.0, reason="一边未成交即结束，撤掉另一边")
            pairs[5].update(phase="closed", accounted=True, accounted_day="2026-11-05", pnl=-40.0)
            ctl.ledger.save_many(pairs)
            want = same(ctl, "six accounted")
            self.assertEqual((len(want["pending"]), want["pnl"]), (6, sum([-12.3, 7.1, -0.7])))
            self.assertEqual(want["strikes"], {pairs[k]["slug"]: 1 for k in (1, 3, 4, 5, 9)}, "every accounted one but the free withdrawal")
            self.assertEqual(set(ctl.ledger.struck), {pairs[k]["slug"] for k in (1, 3, 4, 5, 9)})
            self.assertEqual(set(ctl.ledger.scored), {day, "2026-11-05"})
            self.assertEqual(ctl.risk(), sum(49.0 for _ in want["pending"]))
            # a pair accounted a moment ago and not saved yet: still among the working records, passed by its own flag
            pairs[7].update(accounted=True, accounted_day=day, pnl=0.0)
            self.assertIn(pairs[7], ctl.ledger.pending())
            clock.ms = pairs[0]["cancel_ms"] + 1
            self.assertNotIn(pairs[7], ctl.due(clock.ms))
            self.assertEqual(sorted(x["slug"] for x in ctl.due(clock.ms)), sorted(x["slug"] for x in pairs if not x.get("accounted")))
            ctl.ledger.save(pairs[7])
            same(ctl, "saved accounted: out of the working records")
            # the day's loss cap, asked by every gate: over the day's records with a P&L
            self.assertEqual(ctl.withdrawing(), "")
            pairs[8].update(phase="closed", accounted=True, accounted_day=day, pnl=-95.0, result="down")
            ctl.ledger.save(pairs[8])
            self.assertEqual((ctl.daily_pnl(), ctl.withdrawing()), (walked(ctl)["pnl"], "已达到今日真实亏损上限"))
            pairs[8]["pnl"] = -1.0  # a result corrected: saved, the figure follows
            ctl.ledger.save(pairs[8])
            self.assertEqual((ctl.daily_pnl(), ctl.withdrawing()), (walked(ctl)["pnl"], ""))
            # a record stepping aside (quoted again), one deleted, an order's id learned: the indexes follow
            ctl.ledger.rename(pairs[3]["slug"], pairs[3]["slug"] + "@0")
            again = copy.deepcopy(pairs[0])
            again.update(slug=p.record_key(pairs[3]), attempt=1)
            for side, order in again["orders"].items():
                order.update(id=f"again{side}", hash=f"0xagain{side}", api_hash=f"0xAPI{side}".lower())
            ctl.ledger.save(again)
            want = same(ctl, "a superseded record and its window's next attempt")
            self.assertEqual((want["attempts"][p.record_key(pairs[3])], want["strikes"][p.record_key(pairs[3])]), (2, 1))
            self.assertTrue({"againup", "againdown"} <= ctl.known()[0] and {"0xagainup", "0xapiup"} <= ctl.known()[1])
            ctl.ledger.delete(pairs[6]["slug"])
            again["orders"]["up"]["id"] = "renumbered"
            ctl.ledger.save(again)
            want = same(ctl, "deleted, renumbered")
            self.assertTrue("againup" not in want["known"][0] and "renumbered" in want["known"][0] and pairs[6]["orders"]["up"]["hash"] not in want["known"][1])
            self.assertEqual(ctl.unmanaged_rows([{"id": "renumbered", "order": {"hash": "0xelse"}}, {"id": "x", "order": {"hash": "0xAGAINdown"}},
                                                 {"id": "manual", "order": {"hash": "0xmanual"}}]), [{"id": "manual", "order": {"hash": "0xmanual"}}])
            # the status counts as the walk did
            text = ctl.status()
            rows = list(ctl.ledger.rows.values())
            self.assertIn(f"已记录 {want['windows']} 场、{len(rows)} 条（{sum(1 for x in rows if x['phase'] == 'active')} 条挂着，"
                          f"{sum(1 for x in rows if x.get('accounted'))} 条已结清）", text)
            self.assertIn(f"预留 {ctl.risk():.2f}U", text)
            # records put in or taken out by hand (a test's, never this code): the next walk makes the indexes again
            ghost = copy.deepcopy(pairs[0])
            ghost.update(slug="ghost", accounted=True, accounted_day=day, pnl=-3.0, phase="closed")
            ctl.ledger.rows["ghost"] = ghost
            self.assertFalse(ctl.ledger.in_step())
            same(ctl, "a record put in by hand")
            del ctl.ledger.rows["ghost"]
            same(ctl, "and taken out")
            # a record back among the working ones out of its place: walked in the records' own order again
            pairs[2]["accounted"] = False
            ctl.ledger.save(pairs[2])
            same(ctl, "back among the working records")
            # the archive: the old accounted records leave every index with the records
            for x in (pairs[2], pairs[5]):
                x.update(accounted=True, phase="closed", start_ms=NOW - 4 * m.DAY_MS, end_ms=NOW - 4 * m.DAY_MS + 900_000, pnl=0.0)
                x.pop("result", None)
                for order in x["orders"].values():
                    order.update(state="final", verified=True, filled=0.0)
            ctl.ledger.save_many([pairs[2], pairs[5]])
            self.assertEqual(ctl.ledger.archive_old(clock.ms), 2)
            self.assertTrue(pairs[2]["slug"] not in ctl.ledger.rows and pairs[2]["slug"] not in ctl.ledger.seq and pairs[5]["slug"] not in ctl.ledger.struck)
            same(ctl, "after the archive")
            await self.finish(ctl)
            # a restart: the same answers from the file
            ctl2, _, _ = controller(LIVE_MAX_DAILY_LOSS_USD="100", **db)
            ctl2.owner.market.now_ms = lambda: clock.ms
            same(ctl2, "loaded at a start")
            await self.finish(ctl2)

    async def test_a_resting_pair_the_snapshot_covers_gets_no_task_and_a_restart_writes_nothing(self):
        """1.154.20 — every five seconds the cancellation loop made a task for each resting pair (12,143 on 11-17) only for
        monitor_pair to find every order covered by the account snapshot: the event loop held 0.2 s each time, and at every
        pass until the first snapshot after a start. The look is noted where the task would have been made; a task only for
        a pair an order of which is to be read. And taking the pairs over at a start writes only the ones it changes."""
        with tempfile.TemporaryDirectory() as folder:
            db = {"STATE_DB": str(Path(folder) / "state.sqlite3")}
            ctl, api, clock = controller(**db)
            pairs = await self.book_of(ctl, api, 40)
            for pair in pairs:
                pair["created_ms"] = clock.ms - 60_000
            ctl.ledger.save_many(pairs)
            ctl.listed_ids = {o["id"] for pair in pairs for o in pair["orders"].values()}
            ctl.account_at = clock.ms
            reads = []
            real_order = api.order

            async def order(oid, digest=""):
                reads.append(oid)
                return await real_order(oid, digest)
            api.order = order
            made = []
            real_task = asyncio.create_task

            def counting(coro, **kw):
                made.append(kw.get("name") or "")
                return real_task(coro, **kw)

            async def one_pass():
                made.clear()
                with patch.object(asyncio, "create_task", counting):
                    task = real_task(ctl.cancellation_loop())
                    await asyncio.sleep(0.05)
                    ctl.stop.set()
                    await asyncio.gather(task, return_exceptions=True)
                    await self.settle_tasks(ctl)
                ctl.stop.clear()
                return [name for name in made if name.startswith("monitor:")]

            self.assertEqual(await one_pass(), [], "every order covered by a fresh snapshot: no task")
            self.assertTrue(all(pair["checked_ms"] == clock.ms for pair in pairs) and reads == [], "the look is noted, nothing read")
            clock.ms += 1_000
            self.assertEqual(await one_pass(), [], "looked at a second ago")
            # an order the snapshot does not show: that pair's task, and its read
            clock.ms += p.MONITOR_MS
            ctl.account_at = clock.ms
            ctl.listed_ids.discard(pairs[7]["orders"]["up"]["id"])
            self.assertEqual(await one_pass(), ["monitor:" + pairs[7]["slug"]])
            self.assertEqual(reads, [pairs[7]["orders"]["up"]["id"]])
            self.assertTrue(ctl.monitor_reads(pairs[7], clock.ms, True) and not ctl.monitor_reads(pairs[8], clock.ms, True))
            self.assertTrue(ctl.monitor_reads(pairs[8], clock.ms, False), "a stale snapshot covers nothing")
            # no snapshot yet (a start): nothing to look with — no task, and the look not noted
            clock.ms += p.MONITOR_MS
            ctl.account_at, before = 0, [pair["checked_ms"] for pair in pairs]
            self.assertEqual(await one_pass(), [])
            self.assertEqual([pair["checked_ms"] for pair in pairs], before)
            await self.finish(ctl)
            # a restart with the same settings takes the pairs over without a write; a changed cancel time writes each
            ctl2, _, _ = controller(**db)
            saves = []
            real_save = ctl2.ledger.save
            ctl2.ledger.save = lambda row: (saves.append(row["slug"]), real_save(row))[1]
            ctl2.adopt()
            self.assertEqual((ctl2.resumed, saves), (40, []))
            await self.finish(ctl2)
            ctl3, _, _ = controller(PREOPEN_CANCEL_SECONDS="30", **db)
            saves3, many3 = [], []
            real_save3, real_many3 = ctl3.ledger.save, ctl3.ledger.save_many
            ctl3.ledger.save = lambda row: (saves3.append(row["slug"]), real_save3(row))[1]
            ctl3.ledger.save_many = lambda rows: (many3.append([row["slug"] for row in rows]), real_many3(rows))[1]
            ctl3.adopt()
            # 1.154.42: ...in one transaction (a commit each before: 12,000 in one stretch of the event loop at production's size)
            self.assertEqual((ctl3.resumed, saves3, [len(rows) for rows in many3]), (40, [], [40]))
            self.assertEqual(many3[0], [pair["slug"] for pair in pairs], "in the records' order")
            self.assertTrue(all(x["cancel_ms"] == x["start_ms"] - 30_000 for x in ctl3.ledger.rows.values()))
            stored = {slug: json.loads(body) for slug, body in ctl3.ledger.conn.execute("SELECT slug, body FROM pairs")}
            self.assertEqual(stored, ctl3.ledger.rows, "the file has every record as the start left it")
            await self.finish(ctl3)

    def count_writes(self, ctl):
        """Every write of the controller's ledger from now, in order: its kind (and how many records, for the ones that take several)."""
        writes = []
        for name in ("save", "save_many", "delete", "rename", "rename_many"):
            def counted(*args, name=name, plain=getattr(ctl.ledger, name)):
                writes.append((name, len(args[0])) if name.endswith("_many") else name)
                return plain(*args)
            setattr(ctl.ledger, name, counted)
        return writes

    def on_file(self, ledger):
        return {slug: json.loads(body) for slug, body in ledger.conn.execute("SELECT slug, body FROM pairs")}

    async def test_a_pair_is_written_once_before_its_orders_go_out(self):
        """1.154.42 — a pair placed was five commits of the ledger, each a wait for the disk on the event loop (synchronous=FULL):
        as prepared, again as placing a few lines on (both in front of the two requests), one with each answer, one more for
        the moment it was placed. Three now: once before the orders — among the records at once (Ledger.hold: the gates find
        it by its hashes), written as the gates left it — and one with each answer, the last carrying the moment. What the
        file holds when an order's bytes leave, and when all is done, is what it held before."""
        ctl, api, clock = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        writes, seen = self.count_writes(ctl), []

        async def at_send():
            seen.append((list(writes), self.on_file(ctl.ledger)))
            clock.ms += 5  # (each answer takes a moment)
        api.before = at_send
        self.assertEqual(await ctl.place_pair(c), "")
        pair = ctl.ledger.rows[c["slug"]]
        self.assertEqual(writes, ["save", "save", "save"], "one write before the orders, one with each answer")
        before, file = seen[0]
        self.assertEqual((before, list(file)), (["save"], [c["slug"]]), "the first order leaves with the pair on file — written once")
        self.assertEqual({side: (o["state"], o["hash"], bool(o["body"]["data"]["order"]["signature"])) for side, o in file[c["slug"]]["orders"].items()},
                         {side: ("placing", o["hash"], True) for side, o in pair["orders"].items()}, "both hashes durable before either POST, the orders marked as going out")
        self.assertEqual((file[c["slug"]]["phase"], "placed_ms" in file[c["slug"]]), ("active", False))
        self.assertEqual(({o["state"] for o in pair["orders"].values()}, pair["placed_ms"]), ({"open"}, clock.ms), "the moment it was placed: its last answer's")
        self.assertEqual(self.on_file(ctl.ledger), {c["slug"]: pair}, "on file as in memory, the moment included")
        self.assertEqual(len(api.sent), 2)
        await self.finish(ctl)
        # a side that stays unconfirmed: no moment of placement is claimed before the other's answer says the pair has an order out
        for case in ("unknown", "rejected"):
            ctl, api, clock = controller()
            c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
            writes = self.count_writes(ctl)
            if case == "unknown":
                api.fail_side = 2
            else:
                api.reject_side = 2
            await ctl.place_pair(c)
            pair = ctl.ledger.rows[c["slug"]]
            self.assertEqual(writes[:3], ["save", "save", "save"], case)
            self.assertEqual(pair.get("placed_ms"), clock.ms, "one side is open: placed")
            self.assertEqual(self.on_file(ctl.ledger)[c["slug"]], pair)
            await self.finish(ctl)
        # a gate that refuses the pair before any request: saved once as the gate left it (one side not sent, the other never
        # tried), then gone — nothing of it was ever sent; it was written twice before that
        ctl, api, clock = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        writes, last = self.count_writes(ctl), []
        gone = ctl.ledger.delete
        ctl.ledger.delete = lambda slug: (last.append(self.on_file(ctl.ledger)), gone(slug))[1]
        plain_gate, ctl.before_send = ctl.before_send, lambda body: "测试：闸门不放行"
        self.assertEqual(await ctl.place_pair(c), "")
        self.assertEqual((writes, api.sent, c["slug"] in ctl.ledger.rows, ctl.error), (["save", "delete"], [], False, "测试：闸门不放行"))
        self.assertEqual(({side: o["state"] for side, o in last[0][c["slug"]]["orders"].items()}, last[0][c["slug"]]["phase"]), ({"up": "not_sent", "down": "prepared"}, "closing"))
        self.assertEqual(self.on_file(ctl.ledger), {})
        # ...and a gate that fails: the pair is on file as it was held (prepared: never sent), as its first write left it before
        ctl.before_send = plain_gate
        del writes[:]

        def broken(body):
            raise RuntimeError("the gate itself failed")
        ctl.before_send = broken
        with self.assertRaises(RuntimeError):
            await ctl.place_pair(c)
        self.assertEqual((writes, api.sent), (["save"], []))
        self.assertEqual({o["state"] for o in self.on_file(ctl.ledger)[c["slug"]]["orders"].values()}, {"prepared"})
        self.assertIs(ctl.ledger.holder(next(iter(ctl.ledger.rows[c["slug"]]["orders"].values()))["hash"]), ctl.ledger.rows[c["slug"]])
        await self.finish(ctl)

    async def test_a_start_with_other_settings_saves_its_pairs_a_thousand_a_transaction(self):
        """1.154.42 — a start with another cancel time, or another price or size at a level, saved every resting pair by itself:
        12,000 commits in one stretch of the event loop at production's size, each a wait for the disk — 3.6 s on a fast disk,
        half a minute and more at 2 ms a commit, the whole bot standing still (a setting changed on the control page is such
        a start). A transaction a thousand records now; the records, and the file, as before."""
        with tempfile.TemporaryDirectory() as folder:
            db = {"STATE_DB": str(Path(folder) / "state.sqlite3")}
            ctl, api, clock = controller(**db)
            pairs = await self.book_of(ctl, api, 2300)
            old = ctl.ledger.rows[pairs[7]["slug"]]  # a record from before 1.128.0: its price and size are read off its orders at a start
            del old["price"], old["shares"]
            ctl.ledger.save(old)
            ctl.ledger.close()
            # another cancel time: every pair is taken over with it
            ctl2, _, _ = controller(PREOPEN_CANCEL_SECONDS="30", **db)
            writes = self.count_writes(ctl2)
            ctl2.adopt()
            self.assertEqual((ctl2.resumed, writes), (2300, [("save_many", 1000), ("save_many", 1000), ("save_many", 300)]))
            self.assertTrue(all(x["cancel_ms"] == x["start_ms"] - 30_000 and x["phase"] == "active" for x in ctl2.ledger.rows.values()))
            self.assertEqual((ctl2.ledger.rows[pairs[7]["slug"]]["price"], ctl2.ledger.rows[pairs[7]["slug"]]["shares"]), ("0.49", "50"), "(written once, with both changes)")
            self.assertEqual(self.on_file(ctl2.ledger), ctl2.ledger.rows, "the file has every record as the start left it")
            self.assertEqual(list(ctl2.ledger.rows), [pair["slug"] for pair in pairs], "the records in their order, as before")
            del writes[:]
            ctl2.adopt()
            self.assertEqual(writes, [], "the settings as they are: nothing to write (1.154.20)")
            ctl2.ledger.close()
            # another size: none is what the settings say — every pair withdrawn to be quoted again, saved the same way
            ctl3, _, _ = controller(PREOPEN_CANCEL_SECONDS="30", PREOPEN_SHARES="20", **db)
            writes = self.count_writes(ctl3)
            ctl3.adopt()
            self.assertEqual((ctl3.resumed, writes), (0, [("save_many", 1000), ("save_many", 1000), ("save_many", 300)]))
            self.assertEqual({(x["phase"], x["reason"], x["free"], x["retry_ms"]) for x in ctl3.ledger.rows.values()}, {("closing", "设置不同，撤掉后重新报价", True, 0)})
            self.assertEqual(self.on_file(ctl3.ledger), ctl3.ledger.rows)
            self.assertEqual(len(ctl3.ledger.pending()), 2300)
            ctl3.ledger.close()
            # a record that cannot be judged stops the start, as before — and what was changed before it is on file all the same
            ctl4, _, _ = controller(PREOPEN_CANCEL_SECONDS="45", **db)
            for x in ctl4.ledger.rows.values():
                x.update(phase="active")
                x.pop("reason", None)
            ctl4.ledger.save_many(list(ctl4.ledger.rows.values()))
            del ctl4.ledger.rows[pairs[1500]["slug"]]["start_ms"]
            writes = self.count_writes(ctl4)
            with self.assertRaises(KeyError):
                ctl4.adopt()
            self.assertEqual(writes, [("save_many", 1000), ("save_many", 500)])
            file = self.on_file(ctl4.ledger)
            self.assertTrue(all(file[pair["slug"]]["cancel_ms"] == pair["start_ms"] - 45_000 for pair in pairs[:1500]))
            self.assertTrue(all(file[pair["slug"]]["cancel_ms"] == pair["start_ms"] - 30_000 for pair in pairs[1501:]))
            ctl4.ledger.close()

    async def test_a_snapshots_fills_are_one_write(self):
        """1.154.42: the fills an account snapshot shows were saved a commit a pair, under the account's lock, on the event loop."""
        ctl, api, clock = controller()
        pairs = await self.book_of(ctl, api, 30)
        for row in api.rows.values():  # (the account's list names each order's money)
            row["order"].update(makerAmount=str(L.to_wei(24.5)), side=0)
        for k in (3, 4, 11, 29):  # four pairs' Up orders partly filled since
            api.rows[pairs[k]["orders"]["up"]["hash"]]["amountFilled"] = "20"
        writes = self.count_writes(ctl)
        await ctl.account()
        self.assertEqual(writes, [("save_many", 4)])
        self.assertEqual([pair["orders"]["up"]["filled"] > 0 for pair in pairs].count(True), 4)
        self.assertEqual(self.on_file(ctl.ledger), ctl.ledger.rows, "each fill on file with its pair")
        del writes[:]
        await ctl.account()
        self.assertEqual(writes, [("save_many", 0)], "nothing new: an empty write is none")
        # a row that cannot be read stops the snapshot, as before — the fills applied before it are saved
        api.rows[pairs[5]["orders"]["up"]["hash"]]["amountFilled"] = "30"
        api.rows[pairs[20]["orders"]["up"]["hash"]]["amountFilled"] = "30"
        plain = ctl.apply_fill

        def apply(order, row, pair):
            if pair is pairs[12]:
                raise m.RemoteError("这一行读不懂")
            return plain(order, row, pair)
        ctl.apply_fill = apply
        del writes[:]
        with self.assertRaises(m.RemoteError):
            await ctl.account()
        self.assertEqual(writes, [("save_many", 1)])
        file = self.on_file(ctl.ledger)
        self.assertTrue(file[pairs[5]["slug"]]["orders"]["up"]["filled"] > pairs[4]["orders"]["up"]["filled"] > 0 and file[pairs[20]["slug"]]["orders"]["up"]["filled"] == 0)
        await self.finish(ctl)

    async def test_records_step_aside_together(self):
        """1.154.42: a window quoted again renames each of its levels' records (the record and the attempt: one key each) — a
        commit a level, seven back to back. One transaction now (Ledger.rename_many), the records where one rename after
        another leaves them; and a record held (Ledger.hold) is among the records, in every index, with nothing written."""
        def filled(path=":memory:"):
            ledger = p.Ledger(path)
            for n in range(12):
                ledger.save({"slug": f"w{n // 4}#{n % 4}", "key": "BTC-15M", "window": f"w{n // 4}", "level": n % 4, "attempt": n % 3, "phase": "closed", "accounted": bool(n % 2),
                             "start_ms": NOW + n, "market_id": str(700 + n // 4), "name": f"w{n // 4}", "pnl": 0.0,
                             "orders": {side: {"hash": f"0x{n}{side}", "id": f"{n}{side[0]}", "state": "final", "filled": 0.0, "index_set": 1 + (side == "up"),
                                               "body": {"data": {"order": {"tokenId": f"{n}{side}"}}}} for side in ("up", "down")}})
            return ledger
        indexes = lambda ledger: {name: copy.deepcopy(getattr(ledger, name)) for name in ("by_hash", "tries", "tries_key", "ids", "hashes", "windows", "markets", "tokens", "market_name",
                                                                                               "token_name", "scored_day")} | {
            "working": list(ledger.working), "struck": list(ledger.struck), "unsure": list(ledger.unsure), "filled": list(ledger.filled),
            "order": [slug for slug, _ in sorted(ledger.seq.items(), key=lambda kv: kv[1])]}
        one, many = filled(), filled()
        moves = [(f"w1#{k}", f"w1#{k}@{one.rows[f'w1#{k}']['attempt']}") for k in (2, 0, 3)]
        for slug, new in moves:
            one.rename(slug, new)
        before = many.conn.total_changes
        many.rename_many(moves)
        self.assertEqual(list(many.rows), list(one.rows))
        self.assertEqual(many.rows, one.rows)
        self.assertEqual(self.on_file(many), self.on_file(one))
        self.assertEqual([slug for slug, in many.conn.execute("SELECT slug FROM pairs ORDER BY rowid")], [slug for slug, in one.conn.execute("SELECT slug FROM pairs ORDER BY rowid")])
        self.assertEqual(indexes(many), indexes(one))
        self.assertTrue(many.in_step() and all(many.rows[new]["slug"] == new for _, new in moves) and many.conn.total_changes - before == 6)
        many.rename_many([])
        # a write that fails leaves the records where they were (on disk first)
        with self.assertRaises(KeyError):
            many.rename_many([("w0#0", "w0#0@0"), ("no-such-record", "x")])
        self.assertTrue("w0#0" in many.rows and "w0#0@0" not in many.rows and "w0#0" in self.on_file(many) and many.in_step())
        # held: among the records and in the indexes, not on file until its save
        row = {"slug": "held#0", "key": "BTC-15M", "window": "held", "level": 0, "attempt": 0, "phase": "active", "start_ms": NOW + 99, "market_id": "799", "name": "held",
               "orders": {"up": {"hash": "0xheldup", "id": "", "state": "prepared", "filled": 0.0, "index_set": 2, "body": {"data": {"order": {"tokenId": "hu"}}}}}}
        many.hold(row)
        self.assertIs(many.holder("0xheldup"), row)
        self.assertTrue(many.rows["held#0"] is row and row in many.pending() and many.in_step() and "held#0" not in self.on_file(many) and "799" in many.market_names()[0])
        twin = filled()
        twin.rename_many(moves)
        twin.save(copy.deepcopy(row))
        many.save(row)
        self.assertEqual((self.on_file(many), indexes(many)), (self.on_file(twin), indexes(twin)), "held and then saved: as saved at once")
        for ledger in (one, many, twin):
            ledger.close()
        # ...and through the controller: a withdrawn window's seven levels step aside in one write when it is quoted again
        ctl, api, clock = controller(PREOPEN_LEVELS="48x50,47x50,46x50,45x50,44x50,43x50")
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        wanted = [(c["slug"] + (f"#{n}" if n else ""), n, price, shares) for n, (price, shares) in enumerate(ctl.settings.levels_for("BTC-15M"))]
        self.assertEqual(len(wanted), 7)
        for key, level, price, shares in wanted:
            ctl.ledger.save({**c, "slug": key, "window": c["slug"], "level": level, "attempt": 2, "price": str(price), "shares": str(shares), "phase": "closed", "accounted": True,
                             "free": True, "orders": {}})
        writes = self.count_writes(ctl)
        st = {"found": 0}
        ctl.enqueue({**market(), "_from": "category"}, c["slug"], wanted, {key: 3 for key, *_ in wanted}, st)
        self.assertEqual((writes, ctl.requoted, st["found"], ctl.queue.qsize()), ([("rename_many", 7)], 7, 7, 7))
        self.assertEqual(sorted(ctl.ledger.rows), sorted(f"{key}@2" for key, *_ in wanted))
        self.assertEqual(self.on_file(ctl.ledger), ctl.ledger.rows)
        del writes[:]
        ctl.enqueue({**market(), "_from": "category"}, c["slug"], wanted, {key: 3 for key, *_ in wanted}, st)
        self.assertEqual(writes, [("rename_many", 0)], "nothing in the way: nothing moved")
        await self.finish(ctl)

    async def test_the_fresh_listings_client_is_kept_signed_in(self):
        """1.154.33 (an outside review, CR-10) — a fresh listing's orders go out on a client of their own (first_api, 1.154.21), and
        place_pair signed in the mode's main client before them: the fresh one signed in inside its first order (two requests ahead
        of the one that is about the queue position). Now the client the orders go out on is the one made sure of, and the fresh
        one is kept signed in beside the loop — once a minute at the most, a failure noted and tried again then."""
        ctl, api, clock = controller()
        ctl.first_keep_warm()
        self.assertIsNone(ctl.first_warm, "the tests' own api in place: theirs is the client, nothing to warm")
        signed = []

        class Client:
            async def ensure_auth(self):
                signed.append(1)
        ctl.api, ctl.first_api = ctl.api_made, Client()
        self.assertIs(ctl.first(), ctl.first_api)
        ctl.first_keep_warm()
        await ctl.first_warm
        ctl.first_keep_warm()
        self.assertEqual(len(signed), 1, "once a minute at the most")
        ctl.first_warmed -= p.FIRST_WARM_S
        ctl.first_keep_warm()
        await ctl.first_warm
        self.assertEqual(len(signed), 2)

        class Broken:
            async def ensure_auth(self):
                raise m.RemoteError("HTTP 503")
        ctl.first_api, ctl.first_warmed = Broken(), ctl.first_warmed - p.FIRST_WARM_S
        ctl.first_keep_warm()
        await ctl.first_warm
        self.assertIn("新盘专用客户端登录失败：HTTP 503", ctl.errors[-1][1])
        ctl.api = api
        await self.finish(ctl)

    async def test_golds_0515_new_york_window_is_not_quoted(self):
        """1.154.19 — a window whose outcome is known to lean is not one to bid on both sides of. Gold's 05:15 New York quarter hour
        (the rule taker's 黄金纽约 05:15: Down 65% over a year): 11-18 the mode's Up bids there were sold into down the whole
        ladder — 190 shares at 42.4¢ on average, hours before the window — the Down bids left alone. Not quoted, not looked for;
        one resting from before is withdrawn at the next start (free). In New York time, as the rule is; oil and the crypto
        cards, and gold's other windows, as before. 1.154.33 (an outside review, CR-05): and the slot scan's other leaning windows
        — gold's 05:00 and 16:45, oil's 00:45."""
        et = lambda h, mi, day=6, month=11, offset=-5: m.et_ms(2026, month, day, h, mi, offset)
        ctl, api, clock = controller(PREOPEN_MARKETS="xau,cl,btc", PREOPEN_AHEAD_MINUTES="1440")
        starts = p.window_starts(NOW, ctl.owner.config, ctl.settings)  # Thursday 20:00 New York: Friday's 05:15 is nine hours on
        by = lambda key: [s for r, s in starts if r.key == key]
        self.assertNotIn(et(5, 15), by("XAU-15M"))
        # 1.154.33 (CR-05): the slot scan's other leaning windows too — gold's 05:00 and 16:45, oil's 00:45 (New York)
        self.assertTrue(et(5, 0) not in by("XAU-15M") and et(16, 45) not in by("XAU-15M") and et(0, 45) not in by("CL-15M"), sorted(by("XAU-15M")))
        self.assertTrue({et(4, 45), et(5, 30), et(16, 30)} <= set(by("XAU-15M")), "the quarter hours either side: quoted")
        self.assertTrue(et(5, 15) in by("CL-15M") and et(5, 15) in by("BTC-15M") and et(0, 45) in by("BTC-15M"), "oil's other windows and BTC: as before")
        why = "黄金纽约 05:15 这一场不挂：结果有偏向（回测跌占多数），两边都挂只会被吃掉输的那边"
        self.assertEqual(p.candidate_problem(market("xau", at=et(5, 15), mid=41), NOW, ctl.owner.config, ctl.settings), why)
        self.assertIsNone(p.candidate(market("xau", at=et(5, 15), mid=41), NOW, ctl.owner.config, ctl.settings))
        self.assertEqual(p.candidate_problem(market("xau", at=et(5, 30), mid=42), NOW, ctl.owner.config, ctl.settings), "")
        self.assertEqual(p.candidate_problem(market("cl", at=et(5, 15), mid=43), NOW, ctl.owner.config, ctl.settings), "")
        xau, cl = (next(r for r in m.WINDOW_UPDOWN_MARKETS if r.key == key) for key in ("XAU-15M", "CL-15M"))
        self.assertEqual((bool(p.leaning_window(xau, et(5, 15))), p.leaning_window(cl, et(5, 15)), p.leaning_window(xau, et(4, 45))), (True, "", ""))
        self.assertEqual((p.leaning_window(xau, et(5, 0)), p.leaning_window(xau, et(16, 45)), p.leaning_window(cl, et(0, 45))),
                         ("黄金纽约 05:00 这一场不挂：结果有偏向（回测涨占多数），两边都挂只会被吃掉输的那边",
                          "黄金休市前一档 这一场不挂：结果有偏向（回测涨占多数），两边都挂只会被吃掉输的那边",
                          "原油纽约 00:45 这一场不挂：结果有偏向（回测跌占多数），两边都挂只会被吃掉输的那边"))
        # New York's clock: 17:15 Beijing in summer (18:15 Beijing is an ordinary window then); Monday to Friday
        summer = m.et_ms(2026, 7, 15, 5, 15, -4)
        self.assertEqual(dt.datetime.fromtimestamp(summer / 1000, m.BEIJING).strftime("%H:%M"), "17:15")
        self.assertEqual((bool(p.leaning_window(xau, summer)), p.leaning_window(xau, summer + 3_600_000)), (True, ""))
        self.assertEqual(p.leaning_window(xau, et(5, 15, 7)), "", "Saturday: none (and CME is shut)")
        self.assertIn("；黄金纽约 05:15 那一场不挂（结果偏跌）；黄金纽约 05:00 那一场不挂（结果偏涨）；黄金休市前一档 那一场不挂（结果偏涨）"
                      "；原油纽约 00:45 那一场不挂（结果偏跌）", ctl.status())
        await self.finish(ctl)
        btc, _, _ = controller(PREOPEN_MARKETS="btc")
        self.assertNotIn("05:15", btc.status())
        await self.finish(btc)
        with tempfile.TemporaryDirectory() as folder:  # quoted by an older version: the next start withdraws it, a free withdrawal, not quoted again
            db = {"STATE_DB": str(Path(folder) / "state.sqlite3")}
            at = et(5, 15)
            ctl, api, clock = controller([market("xau", at=at, mid=51)], PREOPEN_MARKETS="xau", PREOPEN_AHEAD_MINUTES="1440", **db)
            with patch.object(p, "leaning_window", lambda rolling, start: ""):
                c = p.candidate(market("xau", at=at, mid=51), NOW, ctl.owner.config, ctl.settings)
                await ctl.place_pair(c)
            self.assertEqual(ctl.ledger.rows[c["slug"]]["phase"], "active")
            ctl.keep_on_stop = True
            await ctl.on_stop()
            ctl.ledger.close()
            again, _, _ = controller([market("xau", at=at, mid=51)], PREOPEN_MARKETS="xau", PREOPEN_AHEAD_MINUTES="1440", **db)
            again.api = again.cancel_api = api; api.ctl = again
            again.adopt()
            pair = again.ledger.rows[c["slug"]]
            self.assertEqual((pair["phase"], pair.get("free"), again.resumed), ("closing", True, 0))
            await again.cancel_pair(pair, pair["reason"])
            await again.discover()
            self.assertEqual(again.queue.qsize(), 0, "not quoted again: the window is left out")
            await self.finish(again)

    async def test_cme_holidays_and_us_hours_are_not_quoted(self):
        """1.151.0: gold's and oil's CME holiday schedules — an early halt (CME_EARLY_DAYS: shut from 12:00 New York time) and a day
        without a session (CME_CLOSED_DAYS: the session it would have had, from 18:00 the evening before) — merge with the daily
        break and the weekend into one shut, whose end is the reopen the quiet minutes follow; both lists can be overridden. The US
        stock cards (hours "us") quote only inside the regular session (us_session: US holidays, half days), not the open's first."""
        et = lambda mo, d, h, mi=0, y=2026: m.et_wall_ms(dt.date(y, mo, d), h, mi)
        cards = {r.key: r for r in m.WINDOW_UPDOWN_MARKETS}
        xau, nvda = cards["XAU-15M"], cards["NVDA-15M"]
        self.assertEqual((xau.hours, cards["CL-15M"].hours, nvda.hours, cards["BTC-15M"].hours, cards["SKHYNIX-15M"].hours), ("cme", "cme", "us", "", ""))
        # Thanksgiving (11-26) and the day after (11-27), both early: from noon, the Thursday's to its 18:00, the Friday's to Sunday's
        self.assertEqual(m.cme_shuts(et(11, 26, 0), et(11, 30, 0)), [(et(11, 26, 12), et(11, 26, 18)), (et(11, 27, 12), et(11, 29, 18))])
        # Christmas Eve (early) and Day (closed), New Year's Eve (early) and Day (closed), Good Friday 2027 (closed): one shut each
        self.assertIn((et(12, 24, 12), et(12, 27, 18)), m.cme_shuts(et(12, 24, 0), et(12, 28, 0)))
        self.assertIn((et(12, 31, 12), et(1, 3, 18, 0, 2027)), m.cme_shuts(et(12, 31, 0), et(1, 4, 0, 0, 2027)))
        self.assertIn((et(3, 25, 17, 0, 2027), et(3, 28, 18, 0, 2027)), m.cme_shuts(et(3, 25, 0, 0, 2027), et(3, 29, 0, 0, 2027)))
        quiet = 15 * 60_000
        for at, shut, opening in ((et(11, 26, 11, 45), False, False), (et(11, 26, 12), True, False), (et(11, 26, 17, 45), True, False),
                                  (et(11, 26, 18), False, True), (et(11, 27, 11, 45), False, False), (et(11, 28, 9), True, False),
                                  (et(11, 29, 18), False, True), (et(11, 29, 18, 15), False, False)):
            self.assertEqual((bool(p.market_shut(xau, at)), p.session_opening(xau, at, frozenset(), quiet)), (shut, opening), m.et_clock(at))
        # the discovery: a day ahead of Thanksgiving morning (08:00 ET, 21:00 Beijing) leaves out noon to 18:00 and the reopen's first
        ctl, api, clock = controller(PREOPEN_MARKETS="xau", PREOPEN_AHEAD_MINUTES="1440", PREOPEN_OPEN_QUIET_MINUTES="15")
        got = {s for _, s in p.window_starts(et(11, 26, 8), ctl.owner.config, ctl.settings)}
        self.assertTrue({et(11, 26, 11, 45), et(11, 26, 18, 15)} <= got and not got & {et(11, 26, 12), et(11, 26, 15), et(11, 26, 18)})
        self.assertIn("CME 节假日从 12:00 起", ctl.status())
        await self.finish(ctl)
        try:  # overridable (the calendar is the process's: the defaults put back after)
            m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "CME_EARLY_DAYS": "", "CME_CLOSED_DAYS": ""})
            self.assertEqual(m.cme_shuts(et(11, 26, 0), et(11, 27, 0)), [(et(11, 26, 17), et(11, 26, 18))], "the daily break alone")
            m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "CME_EARLY_DAYS": "2026-11-18"})
            self.assertIn((et(11, 18, 12), et(11, 18, 18)), m.cme_shuts(et(11, 18, 0), et(11, 19, 0)))
        finally:
            m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline"})
        self.assertEqual(m.cme_shuts(et(11, 26, 0), et(11, 27, 0))[0], (et(11, 26, 12), et(11, 26, 18)), "the defaults back")
        # the US stock cards: the regular session only, the open's first window left out; a half day ends at 13:00, a holiday has none
        for at, shut, opening in ((et(11, 25, 9, 15), True, False), (et(11, 25, 9, 30), False, True), (et(11, 25, 9, 45), False, False),
                                  (et(11, 25, 15, 45), False, False), (et(11, 25, 16), True, False), (et(11, 26, 12), True, False),
                                  (et(11, 27, 12, 45), False, False), (et(11, 27, 13), True, False), (et(11, 30, 9, 30), False, True)):
            self.assertEqual((bool(p.market_shut(nvda, at)), p.session_opening(nvda, at, frozenset(), quiet)), (shut, opening), m.et_clock(at))
        self.assertEqual(p.market_shut(nvda, et(11, 26, 12)), "英伟达休市（美股常规交易时段以外：纽约时间 09:30–16:00 以外、周末、美国假日，半日市 13:00 后）")
        us, _, _ = controller(PREOPEN_MARKETS="nvda,msft", PREOPEN_OPEN_QUIET_MINUTES="15")
        self.assertIn("英伟达、微软只挂美股常规交易时段内的场次（纽约时间 09:30–16:00，美国假日、半日市按日历），开盘后头 15 分钟的不挂", us.status())
        await self.finish(us)

    async def test_fills_are_timed_and_the_breakdown_says_where_the_money_went(self):
        """1.151.0: when a fill is first seen (fill_ms, kept) and so how long before the start (quote_of's fill_lead_min); 按市场
        over any days (cards_between — cards_today is today's) with 偏离, the settled P&L against the coin toss's expectation in σ
        (core.luck_of), and where the money went (card_diag): the one-sided windows' filled side against half, the start hours
        that carry the loss, the lead, in words only past the noise; the archived records counted in by their kept counts."""
        ctl, api, clock = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        api.rows[pair["orders"]["down"]["hash"]]["amountFilled"] = str(L.to_wei(20))
        clock.ms = NOW + 60_000
        await ctl.account()
        self.assertEqual(pair["orders"]["down"]["fill_ms"], NOW + 60_000)
        self.assertNotIn("fill_ms", pair["orders"]["up"])
        api.rows[pair["orders"]["down"]["hash"]]["amountFilled"] = str(L.to_wei(50))
        clock.ms = NOW + 120_000
        await ctl.account()
        self.assertEqual((pair["orders"]["down"]["filled"], pair["orders"]["down"]["fill_ms"]), (50.0, NOW + 60_000), "the first fill seen is kept")
        self.assertAlmostEqual(p.quote_of(pair)["fill_lead_min"], 59.0)
        await self.finish(ctl)

        BJ = m.BEIJING
        at = lambda d, h, mi=0: int(dt.datetime(2026, 11, d, h, mi, tzinfo=BJ).timestamp() * 1000)

        def rec(card, key, stem, start, up, down, result, lead, level=0, price=0.47, shares=30):
            payout = {"up": 1.0, "down": 0.0}.get(result)
            pnl = (payout - price) * up + ((1 - payout) - price) * down if payout is not None else 0.0
            orders = {side: {"state": "final", "filled": float(f), "index_set": 2 if side == "up" else 1,
                             **({"fill_ms": start - int(lead * 60_000)} if f and lead is not None else {})} for side, f in (("up", up), ("down", down))}
            return {"slug": f"{stem}-updown-15m-{start // 1000}" + (f"#{level}" if level else ""), "key": key, "name": f"{card} x", "start_ms": start,
                    "phase": "closed", "accounted": True, "result": result, "pnl": pnl if up or down else 0.0, "price": str(price), "shares": str(shares),
                    "orders": orders, "level": level, "attempt": 0}
        rows = []
        for d in (4, 5, 6):  # gold: the four 06:00 windows (CME's shut) each day, Down filled eight hours ahead at two levels, Up won
            for mi in (0, 15, 30, 45):
                rows += [rec("黄金 15分涨跌", "XAU-15M", "xau", at(d, 6, mi), 0, 30, "up", 480), rec("黄金 15分涨跌", "XAU-15M", "xau", at(d, 6, mi), 0, 80, "up", 480, 1, 0.43, 80)]
            for h, res, side in ((10, "up", "up"), (14, "down", "up"), (20, "down", "down"), (22, "up", "down"), (1, "up", "up")):
                rows.append(rec("黄金 15分涨跌", "XAU-15M", "xau", at(d, h), 30 if side == "up" else 0, 30 if side == "down" else 0, res, 5))
            rows.append(rec("黄金 15分涨跌", "XAU-15M", "xau", at(d, 9), 30, 30, "down", 30))  # both sides: hedged
            for h in range(0, 24, 2):  # BTC: a coin toss — the filled side wins half the time, at any hour, any lead
                side, res = ("up", "down")[(h // 2 + d) % 2], ("up", "down")[(h // 4 + d) % 2]
                rows.append(rec("BTC 15分涨跌", "BTC-15M", "btc", at(d, h), 30 if side == "up" else 0, 30 if side == "down" else 0, res, (h % 3) * 200 + 1))
        out = p.cards_between(rows, "2026-11-04", "2026-11-06")
        gold, btc = (next(r for r in out if r["card"] == card) for card in ("黄金 15分涨跌", "BTC 15分涨跌"))
        self.assertEqual((gold["name"], gold["preopen"], gold["markets"], gold["settled"]), ("黄金 15分涨跌 · 开盘前预挂", True, 30, 42))
        self.assertLess(gold["luck"]["z"], -2)
        self.assertAlmostEqual(gold["luck"]["pnl"], gold["pnl"], places=6)
        self.assertAlmostEqual(gold["luck"]["ev"], gold["expected"], places=6)
        d = gold["diag"]
        self.assertEqual((d["windows"], d["one"]["one"], d["one"]["won"], d["both"]["windows"]), (30, 27, 9, 3))  # 06:00: 0 of 12; the other hours 9 of 15
        self.assertEqual([h["name"] for h in d["hours"]], ["01:00", "06:00", "09:00", "10:00", "14:00", "20:00", "22:00"])
        six = next(h for h in d["hours"] if h["name"] == "06:00")
        self.assertEqual((six["windows"], six["one"], six["won"]), (12, 12, 0))
        self.assertEqual([x["name"] for x in d["lead"]], ["开场前 1–10 分钟", "开场前 10–60 分钟", "开场前 6 小时以上"])
        notes = "\n".join(d["notes"])
        for want in ("不像运气", "整张卡偏离 −1.7σ 还在运气范围内", "06:00（北京时间）开始的场次只成交一边的 12 场里，成交那边赢 0 场（偏离 −3.5σ）", "逆向选择",
                     "亏损集中在 06:00", "越早被吃越亏"):
            self.assertIn(want, notes)
        self.assertEqual([x["name"] for x in d["levels"]], ["47¢", "43¢"])
        self.assertGreater(btc["luck"]["z"], -2)
        bnotes = "\n".join(btc["diag"]["notes"])
        self.assertIn("运气范围内", bnotes)
        for absent in ("逆向选择", "亏损集中在", "越早被吃越亏"):
            self.assertNotIn(absent, bnotes)
        self.assertEqual(p.cards_between(rows, "2026-11-05", "2026-11-05")[0]["markets"] * 3, gold["markets"])
        self.assertEqual(p.cards_today(rows, NOW), p.cards_between(rows, "2026-11-06", "2026-11-06"), "today's: the same rows")
        self.assertEqual(p.card_diag([])["notes"], ["这段时间还没有结算的成交"])
        # the archived records (finished quotes that filled nothing, moved out two days on) counted in by their kept counts
        with tempfile.TemporaryDirectory() as folder:
            led = p.Ledger(str(Path(folder) / "x.preopen.sqlite3"))
            for row in rows:
                led.save(row)
            quiet = [rec("BTC 15分涨跌", "BTC-15M", "btc", at(4, h, 15), 0, 0, "", None) for h in range(5)]
            for row in quiet:
                row.update(result="", pnl=0.0)
                led.save(row)
            self.assertEqual(led.archive_old(at(9, 0)), 5)
            working = list(led.rows.values())
            share = p.archive_share(led, working)
            self.assertTrue(share.fits(at(9, 0)))
            ranged = {r["card"]: r for r in p.cards_between(working, "2026-11-04", "2026-11-06", share)}
            self.assertEqual((ranged["BTC 15分涨跌"]["records"], ranged["BTC 15分涨跌"]["markets"]), (btc["records"] + 5, btc["markets"] + 5))
            self.assertEqual(ranged["BTC 15分涨跌"]["pnl"], btc["pnl"])
            self.assertEqual(p.cards_between(working, "2026-11-05", "2026-11-06", share)[0]["records"] + 0, next(
                r for r in p.cards_between(rows, "2026-11-05", "2026-11-06") if r["card"] == "黄金 15分涨跌")["records"])
            led.close()

    async def test_the_first_window_after_an_open_is_never_quoted_nor_traded(self):
        """1.151.1: the first window after any open or reopen is not quoted whatever PREOPEN_OPEN_QUIET_MINUTES says — gold's and
        oil's 18:00 ET (Polymarket lists no window there), a US stock's 09:30, a stock card's open and its afternoon's — the
        setting lengthens it, 0 no longer takes it away (1.135.1–1.151.0: 0 quoted them); and the model trader gives no
        suggestion on a window of a card with hours that touches its shut or is the reopen's first (window_hours_hold)."""
        self.assertEqual([p.open_quiet_ms(types.SimpleNamespace(open_quiet_minutes=x)) // 60_000 for x in (0, 5, 15, 30, None)], [15, 15, 15, 30, 15])
        ctl, api, clock = controller(PREOPEN_MARKETS="unitree,zhipu,skhynix", PREOPEN_AHEAD_MINUTES="1440")  # the suite's 0
        self.assertEqual(ctl.settings.open_quiet_minutes, 0)
        bj = lambda h, mi=0: int(dt.datetime(2026, 11, 6, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)  # Friday, NOW 09:00 Beijing
        got = {(r.key, s) for r, s in p.window_starts(NOW, ctl.owner.config, ctl.settings)}
        for key, at in (("UNITREE-15M", bj(9, 30)), ("UNITREE-15M", bj(13)), ("ZHIPU-15M", bj(9, 30)), ("ZHIPU-15M", bj(13))):
            self.assertNotIn((key, at), got, (key, at))
        for key, at in (("UNITREE-15M", bj(9, 45)), ("UNITREE-15M", bj(13, 15)), ("ZHIPU-15M", bj(9, 45)), ("SKHYNIX-15M", bj(9, 15))):
            self.assertIn((key, at), got, (key, at))
        self.assertIn("股票卡开盘、午后头 15 分钟内开始的场次不挂", ctl.status(), "the minutes in force, not the setting's 0")
        await self.finish(ctl)
        et = lambda d, h, mi=0: m.et_wall_ms(dt.date(2026, 11, d), h, mi)
        cards = {r.key: r for r in m.WINDOW_UPDOWN_MARKETS}
        hold = lambda key, start: m.window_hours_hold(cards[key].window_spec(start))
        self.assertEqual(hold("XAU-15M", et(16, 17, 15)), "黄金休市（纽约时间每天 17:00–18:00、周五 17:00 到周日 18:00，CME 节假日从 12:00 起）：指数不动，暂不给建议")
        self.assertEqual(hold("CL-15M", et(16, 18)), "原油重新开盘后的第一场：休市期间的消息在指数动之前就已公开，暂不给建议")
        self.assertTrue(hold("XAU-15M", et(26, 13)).startswith("黄金休市"), "Thanksgiving's early halt")
        self.assertTrue(hold("XAU-15M", et(29, 18)).startswith("黄金重新开盘后的第一场"), "the Sunday reopen after the long weekend")
        self.assertTrue(hold("NVDA-15M", et(16, 9, 30)).startswith("英伟达重新开盘后的第一场") and hold("NVDA-15M", et(16, 16)).startswith("英伟达休市"))
        for key, start in (("XAU-15M", et(16, 16, 45)), ("XAU-15M", et(16, 18, 15)), ("NVDA-15M", et(16, 9, 45)), ("BTC-15M", et(16, 18)), ("UNITREE-15M", NOW + 30 * 60_000)):
            self.assertEqual(hold(key, start), "", (key, start))

    async def test_btc_five_minute_has_its_own_timing(self):
        """1.152.0: BTC 5-minute's own cancel time, signed expiry, entry cut-off and horizon (PREOPEN_BTC5_*, each 0 = the general
        one): its windows are looked for, judged, signed and withdrawn by them, every other card by the general ones; a change
        of them withdraws BTC 5-minute's resting pairs at the takeover (signed otherwise) and leaves the others; the combination
        is checked as the general one is; the control page carries the four."""
        env = {"PREOPEN_MARKETS": "btc5,btc,eth5", "PREOPEN_AHEAD_MINUTES": "120", "PREOPEN_BTC5_CANCEL_SECONDS": "10", "PREOPEN_BTC5_EXPIRY_SECONDS": "5",
               "PREOPEN_BTC5_ENTRY_SECONDS": "70", "PREOPEN_BTC5_AHEAD_MINUTES": "30"}
        ctl, api, clock = controller(**env)
        s = ctl.settings
        b5 = s.timing("BTC-5M")
        self.assertEqual((b5.cancel_seconds, b5.expiry_seconds, b5.entry_seconds, b5.ahead_minutes), (10, 5, 70, 30))
        self.assertIs(s.timing("BTC-15M"), s)
        self.assertIs(s.timing("ETH-5M"), s, "ETH 5-minute keeps the general timing")
        starts = p.window_starts(NOW, ctl.owner.config, s)
        mins = lambda key: [(st - NOW) // 60_000 for r, st in starts if r.key == key]
        self.assertEqual(mins("BTC-5M"), [5, 10, 15, 20, 25, 30])
        self.assertEqual((mins("ETH-5M")[0], mins("ETH-5M")[-1], mins("BTC-15M")[-1]), (5, 120, 120))
        c5 = p.candidate(market(at=NOW + 10 * 60_000, mid=7, minutes=5), NOW, ctl.owner.config, s)
        c15 = p.candidate(market(at=NOW + 15 * 60_000, mid=8), NOW, ctl.owner.config, s)
        self.assertEqual((c5["start_ms"] - c5["cancel_ms"], c5["start_ms"] // 1000 - c5["expires"]), (10_000, 5))
        self.assertEqual((c15["start_ms"] - c15["cancel_ms"], c15["start_ms"] // 1000 - c15["expires"]), (20_000, 10))
        self.assertEqual(p.candidate_problem(market(at=NOW + 35 * 60_000, mid=9, minutes=5), NOW, ctl.owner.config, s), "开始还在 30 分钟之外（之后再扫）")
        self.assertEqual(p.candidate_problem(market(at=NOW + 5 * 60_000, mid=10, minutes=5), NOW + 4 * 60_000, ctl.owner.config, s), "已开始或不足 70 秒")
        self.assertEqual(p.candidate_problem(market(at=NOW + 5 * 60_000, mid=10, minutes=5), NOW + 3 * 60_000, ctl.owner.config, s), "", "120 s ahead: BTC 5-minute takes it")
        self.assertEqual(p.candidate_problem(market(at=NOW + 15 * 60_000, mid=11), NOW + 13 * 60_000, ctl.owner.config, s), "已开始或不足 3 分钟", "BTC 15-minute: the general 180 s")
        self.assertIn("BTC 5 分钟自己的时间：开场前 10 秒撤单、5 秒签名到期，不足 70 秒不再新挂，只挂 30 分钟内开始的场次", ctl.status())
        await self.finish(ctl)
        plain, _, _ = controller(PREOPEN_MARKETS="btc5")
        self.assertIs(plain.settings.timing("BTC-5M"), plain.settings)
        self.assertNotIn("BTC 5 分钟自己的时间", plain.status(), "nothing of its own: no line")
        await self.finish(plain)
        # a change of BTC 5-minute's timing. 1.154.54: its expiry moved nearer the start (10 → 5 s) — the pair signed for the old one
        # rests on (it leaves the book earlier than the settings now say), withdrawn at the new cancel time, here its own signed
        # expiry; its expiry moved away from the start (10 → 20 s) — withdrawn at the takeover, as before (signed to stay longer
        # than the settings say). BTC 15-minute's stays either way
        with tempfile.TemporaryDirectory() as folder:
            db = {"STATE_DB": str(Path(folder) / "state.sqlite3")}
            rows = [market(at=NOW + 10 * 60_000, mid=7, minutes=5), market(at=NOW + 15 * 60_000, mid=8)]
            ctl, api, clock = controller(rows, PREOPEN_MARKETS="btc5,btc", PREOPEN_AHEAD_MINUTES="120", **db)
            for mk in rows:
                await ctl.place_pair(p.candidate(mk, NOW, ctl.owner.config, ctl.settings))
            ctl.keep_on_stop = True
            await ctl.on_stop()
            ctl.ledger.close()
            again, _, _ = controller(rows, PREOPEN_MARKETS="btc5,btc", PREOPEN_AHEAD_MINUTES="120", PREOPEN_BTC5_EXPIRY_SECONDS="5", PREOPEN_BTC5_CANCEL_SECONDS="10", **db)
            again.api = again.cancel_api = api; api.ctl = again
            again.adopt()
            by_key = {pair["key"]: pair for pair in again.ledger.rows.values()}
            self.assertEqual((by_key["BTC-5M"]["phase"], by_key["BTC-5M"].get("free"), by_key["BTC-15M"]["phase"], again.resumed), ("active", None, "active", 2))
            self.assertEqual((by_key["BTC-5M"]["start_ms"] - by_key["BTC-5M"]["cancel_ms"], by_key["BTC-5M"]["start_ms"] // 1000 - by_key["BTC-5M"]["expires"]), (10_000, 10))
            self.assertEqual((by_key["BTC-15M"]["start_ms"] - by_key["BTC-15M"]["cancel_ms"], api.removed), (20_000, []))
            again.keep_on_stop = True
            await again.on_stop()
            await self.finish(again)
            third, _, _ = controller(rows, PREOPEN_MARKETS="btc5,btc", PREOPEN_AHEAD_MINUTES="120", PREOPEN_BTC5_EXPIRY_SECONDS="20", PREOPEN_BTC5_CANCEL_SECONDS="30", **db)
            third.api = third.cancel_api = api; api.ctl = third
            third.adopt()
            by_key = {pair["key"]: pair for pair in third.ledger.rows.values()}
            self.assertEqual((by_key["BTC-5M"]["phase"], by_key["BTC-5M"].get("free"), by_key["BTC-15M"]["phase"], third.resumed), ("closing", True, "active", 1))
            self.assertEqual(by_key["BTC-15M"]["start_ms"] - by_key["BTC-15M"]["cancel_ms"], 20_000)
            await self.finish(third)
        # the combination is checked as the general one is (0 = the general one, counted in)
        base = {"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64}
        for bad in ({"PREOPEN_BTC5_CANCEL_SECONDS": "12"}, {"PREOPEN_BTC5_EXPIRY_SECONDS": "3"}, {"PREOPEN_BTC5_AHEAD_MINUTES": "2000"},
                    {"PREOPEN_BTC5_CANCEL_SECONDS": "10", "PREOPEN_BTC5_EXPIRY_SECONDS": "5", "PREOPEN_BTC5_ENTRY_SECONDS": "60"}, {"PREOPEN_BTC5_ENTRY_SECONDS": "-1"}):
            with self.assertRaises(ValueError, msg=str(bad)):
                m.Config.from_env({**base, **bad})
        self.assertEqual(m.Config.from_env({**base, "PREOPEN_BTC5_EXPIRY_SECONDS": "5", "PREOPEN_BTC5_CANCEL_SECONDS": "10"}).preopen.btc5_entry_seconds, 0)
        keys = {"PREOPEN_BTC5_CANCEL_SECONDS", "PREOPEN_BTC5_EXPIRY_SECONDS", "PREOPEN_BTC5_ENTRY_SECONDS", "PREOPEN_BTC5_AHEAD_MINUTES"}
        self.assertTrue(keys <= m.CONTROL_KEY_SET)
        # (1.154.56: on the page BTC 5-minute's own settings are a group of their own)
        self.assertEqual({k for k in keys if m.CONTROL_FORMS[k]["group"] == "预挂5" and m.CONTROL_FORMS[k]["zero"] == "跟所有卡一样"}, keys)
        # 1.152.2: 挂满 — BTC 5-minute quotes every listed window, the one Predict lists just now included (the front of the 49¢ queue),
        # while the other cards keep their 120 minutes; the control page names the preset
        form = m.CONTROL_FORMS["PREOPEN_BTC5_AHEAD_MINUTES"]
        self.assertEqual((form["presets"][-1], form["captions"]["1440"]), ("1440", "挂满：一发布就挂"))
        self.assertEqual(m.CONTROL_FORMS["PREOPEN_AHEAD_MINUTES"]["captions"], {"1440": "挂满"})
        self.assertEqual(m.Config.from_env({**base, "PREOPEN_BTC5_AHEAD_MINUTES": "1440"}).preopen.btc5_ahead_minutes, 1440)
        full, _, _ = controller(PREOPEN_MARKETS="btc5,btc", PREOPEN_AHEAD_MINUTES="120", PREOPEN_BTC5_AHEAD_MINUTES="1440")
        starts = p.window_starts(NOW, full.owner.config, full.settings)
        five = [(st - NOW) // 60_000 for r, st in starts if r.key == "BTC-5M"]
        self.assertEqual((five[0], five[-1], len(five)), (5, 1440, 288))
        self.assertEqual(max((st - NOW) // 60_000 for r, st in starts if r.key == "BTC-15M"), 120)
        self.assertEqual(p.candidate_problem(market(at=NOW + 1440 * 60_000, mid=12, minutes=5), NOW, full.owner.config, full.settings), "",
                         "listed a day ahead: placed at once")
        self.assertEqual(p.candidate_problem(market(at=NOW + 135 * 60_000, mid=13), NOW, full.owner.config, full.settings), "开始还在 120 分钟之外（之后再扫）")
        self.assertIn("BTC 5 分钟自己的时间：开场前 20 秒撤单、10 秒签名到期，不足 180 秒不再新挂，挂 24 小时内已发布的全部场次", full.status())
        await self.finish(full)

    async def test_btc_five_minute_has_its_own_ladder(self):
        """1.154.37 (the user: "单独 BTC 5 分钟的预挂，可以设置挂多少份额的吗"): BTC 5-minute's own price ladder, its first level
        included (PREOPEN_BTC5_LEVELS; empty = the general one): its windows are wanted, queued, placed and counted at it, every other
        card at the general one; a change of it withdraws BTC 5-minute's resting pairs at the takeover and leaves the others'; it is
        checked as the general ladder is; the status says it; the control page carries it."""
        rows = [market(at=NOW + 10 * 60_000, mid=7, minutes=5), market(at=NOW + 15 * 60_000, mid=8)]
        ctl, api, clock = controller(rows, PREOPEN_MARKETS="btc5,btc", PREOPEN_AHEAD_MINUTES="120", PREOPEN_PRICE_CENTS="49", PREOPEN_SHARES="10",
                                     PREOPEN_LEVELS="48x20", PREOPEN_BTC5_LEVELS="47x5,43x40")
        s = ctl.settings
        D = m.D
        self.assertEqual((s.levels_for("BTC-5M"), s.levels_for("BTC-15M"), s.levels_for("ETH-5M")),
                         (((D("0.47"), D("5")), (D("0.43"), D("40"))), ((D("0.49"), D("10")), (D("0.48"), D("20"))), s.levels))
        await ctl.discover()
        queued = sorted((c["slug"].split("-")[2], c["level"], c["price"], c["shares"]) for c in (ctl.queue.get_nowait()[2] for _ in range(ctl.queue.qsize())))
        self.assertEqual(queued, [("15m", 0, "0.49", "10"), ("15m", 1, "0.48", "20"), ("5m", 0, "0.47", "5"), ("5m", 1, "0.43", "40")])
        bare = p.candidate(rows[0], NOW, ctl.owner.config, s)  # a candidate with no level's figures: its card's first level
        self.assertEqual(await ctl.place_pair(bare), "")
        placed = ctl.ledger.rows[bare["slug"]]
        self.assertEqual((placed["price"], placed["shares"]), ("0.47", "5"))
        self.assertIn("BTC 5 分钟自己的档位：两边各 47.0¢×5 份、43.0¢×40 份", ctl.status())
        await self.finish(ctl)
        plain, _, _ = controller(PREOPEN_MARKETS="btc5,btc")
        self.assertEqual((plain.settings.btc5_levels, plain.settings.levels_for("BTC-5M")), ((), plain.settings.levels))
        self.assertNotIn("BTC 5 分钟自己的档位", plain.status(), "none of its own: no line")
        await self.finish(plain)
        # a change of BTC 5-minute's ladder: its resting pair is withdrawn at the takeover, BTC 15-minute's stays
        with tempfile.TemporaryDirectory() as folder:
            db = {"STATE_DB": str(Path(folder) / "state.sqlite3")}
            ctl, api, clock = controller(rows, PREOPEN_MARKETS="btc5,btc", PREOPEN_AHEAD_MINUTES="120", **db)
            for mk in rows:
                await ctl.place_pair(p.candidate(mk, NOW, ctl.owner.config, ctl.settings))
            ctl.keep_on_stop = True
            await ctl.on_stop()
            ctl.ledger.close()
            again, _, _ = controller(rows, PREOPEN_MARKETS="btc5,btc", PREOPEN_AHEAD_MINUTES="120", PREOPEN_BTC5_LEVELS="49x20", **db)
            again.api = again.cancel_api = api; api.ctl = again
            again.adopt()
            by_key = {pair["key"]: pair for pair in again.ledger.rows.values()}
            self.assertEqual((by_key["BTC-5M"]["phase"], by_key["BTC-15M"]["phase"], again.resumed), ("closing", "active", 1))
            await self.finish(again)
            third, _, _ = controller(rows, PREOPEN_MARKETS="btc5,btc", PREOPEN_AHEAD_MINUTES="120", PREOPEN_BTC5_LEVELS="49x50", **db)  # 49¢ × 50: the general first level
            third.api = third.cancel_api = api; api.ctl = third
            third.adopt()
            self.assertEqual(third.resumed, 1, "BTC 15-minute's again; BTC 5-minute's was withdrawn above")
            await self.finish(third)
        # checked as the general ladder is; empty = the general one; the control page carries it
        base = {"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64}
        for bad in ("47", "47x10,47x5", "40x10,43x10", "51x10", "47x501", "1x50", ",".join(f"{49 - i}x10" for i in range(9))):
            with self.assertRaises(ValueError, msg=bad):
                m.Config.from_env({**base, "PREOPEN_BTC5_LEVELS": bad})
        self.assertEqual(m.Config.from_env({**base, "PREOPEN_BTC5_LEVELS": " 47×10，43x40 "}).preopen.btc5_levels, ((D("0.47"), D("10")), (D("0.43"), D("40"))))
        self.assertEqual(m.Config.from_env({**base, "PREOPEN_BTC5_LEVELS": ""}).preopen.btc5_levels, ())
        self.assertIn("PREOPEN_BTC5_LEVELS", m.CONTROL_KEY_SET)
        form = m.CONTROL_FORMS["PREOPEN_BTC5_LEVELS"]
        self.assertEqual((form["group"], form["kind"], form["empty"], form["presets"][0]), ("高级", "text", "不用", ""))  # (1.154.56: the older way, among the advanced ones)
        bot = m.Bot(m.Config.from_env({**base, "PREOPEN_BTC5_LEVELS": "47x10,43x40"}), m.Store(":memory:"), types.SimpleNamespace(now_ms=lambda: NOW, config=None), None)
        self.assertEqual(bot.control_value("PREOPEN_BTC5_LEVELS"), "47x10,43x40")

    async def test_btc_five_minute_first_level_by_itself(self):
        """1.154.56 (the user: "BTC 5 分钟也可以设置第一个档预挂的"; "优化下单独设置 BTC 5 分钟的功能"): BTC 5-minute's first level set
        by itself — its price, its shares, each 0 = every card's — and its deeper levels by themselves (empty = every card's, none
        = none), where 1.154.37 wanted the whole ladder typed out. Its windows are quoted at what comes of them, every other
        card's as before; a change of the first level withdraws BTC 5-minute's first-level pairs alone; the page has the three in
        a group of BTC 5-minute's own, with its timing; the whole ladder of 1.154.37 still stands under them."""
        D = m.D
        base = {"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64,
                "PREOPEN_PRICE_CENTS": "47", "PREOPEN_SHARES": "30", "PREOPEN_LEVELS": "43x160,40x160"}
        ladder = lambda **env: m.Config.from_env({**base, **env}).preopen
        general = ((D("0.47"), D("30")), (D("0.43"), D("160")), (D("0.40"), D("160")))
        self.assertEqual((ladder().btc5_levels, ladder().levels_for("BTC-5M")), ((), general))
        # the shares alone, the price alone, both: the deeper levels as every card's
        self.assertEqual(ladder(PREOPEN_BTC5_SHARES="60").levels_for("BTC-5M"), ((D("0.47"), D("60")), *general[1:]))
        self.assertEqual(ladder(PREOPEN_BTC5_PRICE_CENTS="45").levels_for("BTC-5M"), ((D("0.45"), D("30")), *general[1:]))
        both = ladder(PREOPEN_BTC5_PRICE_CENTS="48", PREOPEN_BTC5_SHARES="10")
        self.assertEqual((both.levels_for("BTC-5M"), both.levels_for("BTC-15M"), both.levels_for("ETH-5M")), (((D("0.48"), D("10")), *general[1:]), general, general))
        # its deeper levels alone; none of them; with its first level
        self.assertEqual(ladder(PREOPEN_BTC5_DEEP_LEVELS="44x20").levels_for("BTC-5M"), (general[0], (D("0.44"), D("20"))))
        for none in ("none", "无", " None "):
            self.assertEqual(ladder(PREOPEN_BTC5_DEEP_LEVELS=none).levels_for("BTC-5M"), (general[0],), none)
        self.assertEqual(ladder(PREOPEN_BTC5_PRICE_CENTS="40", PREOPEN_BTC5_SHARES="50", PREOPEN_BTC5_DEEP_LEVELS="35x100，30×100").levels_for("BTC-5M"),
                         ((D("0.40"), D("50")), (D("0.35"), D("100")), (D("0.30"), D("100"))))
        # what comes to every card's ladder is nothing of its own (no line in the status, nothing to withdraw)
        self.assertEqual(ladder(PREOPEN_BTC5_PRICE_CENTS="47", PREOPEN_BTC5_SHARES="30").btc5_levels, ())
        # the whole ladder of 1.154.37 still stands — and the three, where set, over it
        whole = ladder(PREOPEN_BTC5_LEVELS="46x10,42x40")
        self.assertEqual((whole.levels_for("BTC-5M"), whole.btc5_whole), (((D("0.46"), D("10")), (D("0.42"), D("40"))),) * 2)
        self.assertEqual(ladder(PREOPEN_BTC5_LEVELS="46x10,42x40", PREOPEN_BTC5_SHARES="25").levels_for("BTC-5M"), ((D("0.46"), D("25")), (D("0.42"), D("40"))))
        self.assertEqual(ladder(PREOPEN_BTC5_LEVELS="46x10,42x40", PREOPEN_BTC5_DEEP_LEVELS="none").levels_for("BTC-5M"), ((D("0.46"), D("10")),))
        # checked together, and said in the settings' own words: a first level not above the level after it, a side under Predict's
        # minimum, a price or a size out of range, too many levels
        for bad in ({"PREOPEN_BTC5_PRICE_CENTS": "43"}, {"PREOPEN_BTC5_PRICE_CENTS": "40"}, {"PREOPEN_BTC5_PRICE_CENTS": "51"}, {"PREOPEN_BTC5_PRICE_CENTS": "-1"},
                    {"PREOPEN_BTC5_SHARES": "501"}, {"PREOPEN_BTC5_SHARES": "2"}, {"PREOPEN_BTC5_DEEP_LEVELS": "47x10"}, {"PREOPEN_BTC5_DEEP_LEVELS": "43"},
                    {"PREOPEN_BTC5_DEEP_LEVELS": ",".join(f"{46 - i}x10" for i in range(8))}):
            with self.assertRaises(ValueError, msg=str(bad)) as caught:
                m.Config.from_env({**base, **bad})
            if "PREOPEN_BTC5_DEEP_LEVELS" not in bad or bad["PREOPEN_BTC5_DEEP_LEVELS"] != "43":
                self.assertIn("BTC 5 分钟的价格档位合起来是 ", str(caught.exception), bad)
        self.assertIn("合起来是 43x30,43x160,40x160", str(self.assert_bad(base, PREOPEN_BTC5_PRICE_CENTS="43")))
        self.assertIn("把“更深的价格档位”也单独填上", str(self.assert_bad(base, PREOPEN_BTC5_PRICE_CENTS="40")))
        self.assertEqual(ladder(PREOPEN_BTC5_PRICE_CENTS="40", PREOPEN_BTC5_DEEP_LEVELS="35x100").levels_for("BTC-5M"), ((D("0.40"), D("30")), (D("0.35"), D("100"))))
        # the control page: a group of BTC 5-minute's own, the first level at its head; what is in force goes back to it as typed
        keys = [k for k, _, _ in m.CONTROL_KEYS if m.CONTROL_FORMS.get(k, {}).get("group") == "预挂5"]
        self.assertEqual(keys, ["PREOPEN_BTC5_PRICE_CENTS", "PREOPEN_BTC5_SHARES", "PREOPEN_BTC5_DEEP_LEVELS", "PREOPEN_BTC5_CANCEL_SECONDS",
                                "PREOPEN_BTC5_EXPIRY_SECONDS", "PREOPEN_BTC5_ENTRY_SECONDS", "PREOPEN_BTC5_AHEAD_MINUTES"])
        self.assertIn('["预挂","开盘前预挂（15 分钟市场）"],["预挂5","开盘前预挂 · BTC 5 分钟单独设置（留 0 / 留空 = 跟上面所有卡一样）"],'
                      '["预挂E5","开盘前预挂 · ETH 5 分钟单独设置（留 0 / 留空 = 跟上面所有卡一样）"],'
                      '["预挂E15","开盘前预挂 · ETH 15 分钟单独设置（留 0 / 留空 = 跟上面所有卡一样）"],["规则",', m.CONTROL_PAGE)  # (1.154.75: ETH's after it)
        self.assertEqual((m.CONTROL_FORMS["PREOPEN_BTC5_PRICE_CENTS"]["zero"], m.CONTROL_FORMS["PREOPEN_BTC5_SHARES"]["zero"], m.CONTROL_FORMS["PREOPEN_BTC5_DEEP_LEVELS"]["empty"],
                          m.CONTROL_FORMS["PREOPEN_BTC5_DEEP_LEVELS"]["captions"]), ("跟所有卡一样",) * 3 + ({"none": "只挂第一档"},))
        clock = types.SimpleNamespace(now_ms=lambda: NOW, config=None)
        bot = m.Bot(m.Config.from_env({**base, "PREOPEN_BTC5_PRICE_CENTS": "48", "PREOPEN_BTC5_SHARES": "10", "PREOPEN_BTC5_DEEP_LEVELS": "44x20,41x20"}), m.Store(":memory:"), clock, None)
        self.assertEqual([bot.control_value(k) for k in keys[:3]] + [bot.control_value("PREOPEN_BTC5_LEVELS")], ["48", "10", "44x20,41x20", ""])
        bot = m.Bot(m.Config.from_env({**base, "PREOPEN_BTC5_DEEP_LEVELS": "none"}), m.Store(":memory:"), clock, None)
        self.assertEqual([bot.control_value(k) for k in keys[:3]], ["0", "0", "none"])
        bot = m.Bot(m.Config.from_env(base), m.Store(":memory:"), clock, None)
        self.assertEqual([bot.control_value(k) for k in keys[:3]], ["0", "0", ""])
        # ...and saved from it: what does not make a ladder is refused in the page's own words, nothing saved; what does is in force
        answer = bot.control_set({"PREOPEN_BTC5_PRICE_CENTS": "40"})
        self.assertEqual((answer["ok"], bot.config.preopen.btc5_levels), (False, ()))
        self.assertTrue(answer["message"].startswith("BTC 5 分钟的价格档位合起来是 40x30,43x160,40x160（") and "把“更深的价格档位”也单独填上" in answer["message"], answer)
        answer = bot.control_set({"PREOPEN_BTC5_PRICE_CENTS": "45", "PREOPEN_BTC5_SHARES": "60"})
        self.assertEqual((answer["ok"], bot.config.preopen.levels_for("BTC-5M")), (True, ((D("0.45"), D("60")), *general[1:])))
        self.assertEqual((bot.control_set({"PREOPEN_BTC5_DEEP_LEVELS": "none"})["ok"], bot.config.preopen.levels_for("BTC-5M")), (True, ((D("0.45"), D("60")),)))
        bot.control_set({"PREOPEN_BTC5_PRICE_CENTS": "0", "PREOPEN_BTC5_SHARES": "0", "PREOPEN_BTC5_DEEP_LEVELS": ""})
        self.assertEqual((bot.config.preopen.btc5_levels, bot.config.preopen.levels_for("BTC-5M")), ((), general))
        # quoted so: BTC 5-minute's windows at its own first level, the deeper ones and every other card's as every card's
        rows = [market(at=NOW + 10 * 60_000, mid=7, minutes=5), market(at=NOW + 15 * 60_000, mid=8)]
        env = {"PREOPEN_MARKETS": "btc5,btc", "PREOPEN_AHEAD_MINUTES": "120", "PREOPEN_PRICE_CENTS": "47", "PREOPEN_SHARES": "30", "PREOPEN_LEVELS": "43x160"}
        ctl, api, clock = controller(rows, **env, PREOPEN_BTC5_SHARES="60")
        await ctl.discover()
        queued = sorted((c["slug"].split("-")[2], c["level"], c["price"], c["shares"]) for c in (ctl.queue.get_nowait()[2] for _ in range(ctl.queue.qsize())))
        self.assertEqual(queued, [("15m", 0, "0.47", "30"), ("15m", 1, "0.43", "160"), ("5m", 0, "0.47", "60"), ("5m", 1, "0.43", "160")])
        self.assertIn("BTC 5 分钟自己的档位：两边各 47.0¢×60 份、43.0¢×160 份", ctl.status())
        await self.finish(ctl)
        # a change of its first level alone: BTC 5-minute's first-level pair is withdrawn at the takeover — its deeper one and BTC
        # 15-minute's two rest on
        with tempfile.TemporaryDirectory() as folder:
            db = {"STATE_DB": str(Path(folder) / "state.sqlite3")}
            ctl, api, clock = controller(rows, **env, **db)
            for mk in rows:
                c = p.candidate(mk, NOW, ctl.owner.config, ctl.settings)
                for level, (price, shares) in enumerate(ctl.settings.levels):
                    await ctl.place_pair({**c, "slug": c["slug"] if level == 0 else f"{c['slug']}#{level}", "window": c["slug"], "level": level,
                                          "price": str(price), "shares": str(shares), "attempt": 0, "market": mk, "read_ms": NOW})
            ctl.keep_on_stop = True
            await ctl.on_stop()
            ctl.ledger.close()
            again, _, _ = controller(rows, **env, **db, PREOPEN_BTC5_SHARES="60")
            again.api = again.cancel_api = api; api.ctl = again
            again.adopt()
            states = sorted((x["key"], int(x.get("level") or 0), x["phase"]) for x in again.ledger.rows.values())
            self.assertEqual(states, [("BTC-15M", 0, "active"), ("BTC-15M", 1, "active"), ("BTC-5M", 0, "closing"), ("BTC-5M", 1, "active")])
            await self.finish(again)

    def assert_bad(self, base, **env):
        with self.assertRaises(ValueError) as caught:
            m.Config.from_env({**base, **env})
        return caught.exception

    async def test_eth_five_and_fifteen_minute_have_their_own_settings(self):
        """1.154.75 (the user: "给ETH 5分钟 15分钟 预挂 加个单独的选项"): ETH 5- and 15-minute's own first level, deeper levels,
        cancel time, signed expiry, entry cut-off and horizon (PREOPEN_ETH5_*, PREOPEN_ETH15_*, each 0 / empty = every card's), as
        BTC 5-minute's: their windows are signed, withdrawn and quoted by them, every other card's by the general ones; checked
        together as BTC 5-minute's are; the status says them; the control page has each card's seven in a group of its own."""
        D = m.D
        env = {"PREOPEN_MARKETS": "eth5,eth,btc5,btc", "PREOPEN_AHEAD_MINUTES": "120", "PREOPEN_PRICE_CENTS": "47", "PREOPEN_SHARES": "30",
               "PREOPEN_LEVELS": "43x40", "PREOPEN_ETH5_CANCEL_SECONDS": "120", "PREOPEN_ETH5_ENTRY_SECONDS": "240", "PREOPEN_ETH5_DEEP_LEVELS": "none",
               "PREOPEN_ETH15_CANCEL_SECONDS": "60", "PREOPEN_ETH15_SHARES": "3"}
        ctl, api, clock = controller([market(stem="eth", at=NOW + 10 * 60_000, mid=7, minutes=5)], **env)
        s = ctl.settings
        e5, e15 = s.timing("ETH-5M"), s.timing("ETH-15M")
        self.assertEqual((e5.cancel_seconds, e5.expiry_seconds, e5.entry_seconds, e5.ahead_minutes), (120, 10, 240, 120))
        self.assertEqual((e15.cancel_seconds, e15.entry_seconds), (60, 180))
        self.assertTrue(s.timing("BTC-5M") is s and s.timing("BTC-15M") is s, "BTC keeps the general timing")
        general = ((D("0.47"), D("30")), (D("0.43"), D("40")))
        self.assertEqual((s.levels_for("ETH-5M"), s.levels_for("ETH-15M"), s.levels_for("BTC-5M"), s.levels_for("BTC-15M")),
                         (general[:1], ((D("0.47"), D("3")), general[1]), general, general))
        c5 = p.candidate(market(stem="eth", at=NOW + 10 * 60_000, mid=7, minutes=5), NOW, ctl.owner.config, s)
        c15 = p.candidate(market(stem="eth", at=NOW + 15 * 60_000, mid=8), NOW, ctl.owner.config, s)
        b5 = p.candidate(market(at=NOW + 10 * 60_000, mid=9, minutes=5), NOW, ctl.owner.config, s)
        self.assertEqual([c["start_ms"] - c["cancel_ms"] for c in (c5, c15, b5)], [120_000, 60_000, 20_000])
        self.assertEqual(p.candidate_problem(market(stem="eth", at=NOW + 5 * 60_000, mid=10, minutes=5), NOW + 90_000, ctl.owner.config, s), "已开始或不足 240 秒",
                         "210 s out — ETH 5-minute: its own 240 s")
        self.assertEqual(p.candidate_problem(market(at=NOW + 5 * 60_000, mid=11, minutes=5), NOW + 90_000, ctl.owner.config, s), "", "BTC 5-minute: the general 180 s")
        status = ctl.status()
        self.assertIn("ETH 5 分钟自己的时间：开场前 120 秒撤单、10 秒签名到期，不足 240 秒不再新挂，只挂 120 分钟内开始的场次", status)
        self.assertIn("ETH 5 分钟自己的档位：两边各 47.0¢×30 份", status)
        self.assertIn("ETH 15 分钟自己的时间：开场前 60 秒撤单", status)
        self.assertIn("ETH 15 分钟自己的档位：两边各 47.0¢×3 份、43.0¢×40 份", status)
        self.assertNotIn("BTC 5 分钟自己的", status)
        # placed at its own ladder: ETH 5-minute's first level alone
        await ctl.place_pair(c5)
        self.assertEqual([(o["state"], pair["shares"]) for pair in ctl.ledger.rows.values() for o in pair["orders"].values()], [("open", "30"), ("open", "30")])
        await self.finish(ctl)
        # checked together as BTC 5-minute's (its cancel and the general entry cut-off; the ladder in its own words)
        base = {"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64}
        self.assertIn("ETH 5 分钟自己的预挂时间（PREOPEN_ETH5_*", str(self.assert_bad(base, PREOPEN_ETH5_CANCEL_SECONDS="150")))
        self.assertIn("新挂截止 180 秒", str(self.assert_bad(base, PREOPEN_ETH15_CANCEL_SECONDS="150")))
        self.assertEqual(m.Config.from_env({**base, "PREOPEN_ETH5_CANCEL_SECONDS": "150", "PREOPEN_ETH5_ENTRY_SECONDS": "210"}).preopen.timing("ETH-5M").cancel_seconds, 150)
        self.assertIn("ETH 15 分钟的价格档位合起来是 ", str(self.assert_bad(base, PREOPEN_ETH15_PRICE_CENTS="1", PREOPEN_ETH15_SHARES="10")))
        self.assertEqual(m.Config.from_env(base).preopen.own, (), "nothing set: nothing of its own")
        # the control page: each card's seven in a group of its own, as BTC 5-minute's; what is in force goes back to it as typed
        for stem, group in (("ETH5", "预挂E5"), ("ETH15", "预挂E15")):
            keys = [k for k, _, _ in m.CONTROL_KEYS if m.CONTROL_FORMS.get(k, {}).get("group") == group]
            self.assertEqual(keys, [f"PREOPEN_{stem}_{part}" for part in ("PRICE_CENTS", "SHARES", "DEEP_LEVELS", "CANCEL_SECONDS", "EXPIRY_SECONDS",
                                                                           "ENTRY_SECONDS", "AHEAD_MINUTES")])
            self.assertTrue(set(keys) <= m.CONTROL_KEY_SET)
        bot = m.Bot(m.Config.from_env({**base, **env}), m.Store(":memory:"), types.SimpleNamespace(now_ms=lambda: NOW, config=None), None)
        self.assertEqual([bot.control_value(k) for k in ("PREOPEN_ETH5_CANCEL_SECONDS", "PREOPEN_ETH5_DEEP_LEVELS", "PREOPEN_ETH15_SHARES", "PREOPEN_ETH15_DEEP_LEVELS",
                                                         "PREOPEN_ETH15_PRICE_CENTS")], ["120", "none", "3", "", "0"])

    async def test_five_minute_cards_are_quoted_on_their_own_rhythm(self):
        """1.137.0: btc5 / eth5 / bnb5 name the 5-minute cards (btc-updown-5m-<start>): a window every five minutes, the
        listing expected five minutes after the last; the 15-minute card keeps the stem btc. The model trader leaves them
        alone by default (DEFAULT_SIM_SKIP)."""
        ctl, api, clock = controller(PREOPEN_MARKETS="btc5,btc", PREOPEN_AHEAD_MINUTES="30")
        self.assertEqual(ctl.settings.markets, frozenset({"BTC-5M", "BTC-15M"}))
        five = next(r for r in m.WINDOW_UPDOWN_MARKETS if r.key == "BTC-5M")
        self.assertEqual((five.short, five.prefix, five.window_ms, five.model, five.venue), ("btc5", "btc-updown-5m-", 300_000, False, "spot"))
        starts = [(r.key, (s - NOW) // 60_000) for r, s in p.window_starts(NOW, ctl.owner.config, ctl.settings)]
        self.assertEqual([mn for key, mn in starts if key == "BTC-5M"], [5, 10, 15, 20, 25, 30])
        self.assertEqual([mn for key, mn in starts if key == "BTC-15M"], [15, 30])
        self.assertEqual(p.window_slug(five, NOW + 5 * 60_000), f"btc-updown-5m-{(NOW + 5 * 60_000) // 1000}")
        c = p.candidate(market(at=NOW + 10 * 60_000, mid=7, minutes=5), NOW, ctl.owner.config, ctl.settings)
        self.assertEqual((c["key"], c["slug"], c["name"]), ("BTC-5M", f"btc-updown-5m-{(NOW + 10 * 60_000) // 1000}", "BTC 5分涨跌 09:10–09:15"))
        self.assertEqual(p.candidate_problem(market(at=NOW + 7 * 60_000, mid=8, minutes=5), NOW, ctl.owner.config, ctl.settings), "链接里的时间不在场次边界上")
        self.assertEqual(p.candidate_problem({"categorySlug": "btc-updown-3m-1793986800"}, NOW, ctl.owner.config, ctl.settings), "不是涨跌卡（15 分钟、5 分钟、小时、日）的链接")
        only15, _, _ = controller(PREOPEN_MARKETS="btc")
        self.assertEqual(p.candidate_problem(market(at=NOW + 10 * 60_000, minutes=5), NOW, only15.owner.config, only15.settings), "不在 PREOPEN_MARKETS 里")
        await self.finish(only15)
        self.assertEqual((p.cycle_ms("BTC-5M"), p.cycle_ms("BTC-15M"), p.cycle_ms("nope")), (300_000, 900_000, p.LISTING_CYCLE_MS))
        ctl.listed_at["BTC-5M"] = NOW - 5 * 60_000 + 20_000  # the next 5-minute listing expected 20 s on: the fast poll
        self.assertEqual(ctl.poll_wait(), p.FAST_POLL_S)
        self.assertIn("BTC-5M 上次", ctl.status())
        ctl.listed_at.clear()
        self.assertIn("BTC-5M", m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_MARKETS": "all"}).preopen.markets)
        await self.finish(ctl)

    async def test_hourly_and_daily_cards_are_quoted_before_their_start(self):
        """1.142.0: btc1h / eth1h / bnb1h name the hourly markets ("<coin> Up or Down - <date>, <hour> ET": the hour from <hour> ET)
        and btc1d / eth1d / bnb1d the daily ones ("<coin> Up or Down on <date>": from 12:00 ET the day before) — quoted like the
        windows, before the start, and settled after the end. NOW is 11-06 09:00 Beijing, 11-05 20:00 ET (EST)."""
        ctl, api, clock = controller(PREOPEN_MARKETS="btc1h,eth1d", PREOPEN_AHEAD_MINUTES="1440")
        self.assertEqual(ctl.settings.markets, frozenset({"BTC-1H", "ETH-1D"}))
        hour = next(r for r in m.HOURLY_UPDOWN_MARKETS if r.key == "BTC-1H")
        day = next(r for r in m.DAY_WINDOW_MARKETS if r.key == "ETH-1D")
        two_am, noon, next_noon = m.et_ms(2026, 11, 6, 2, 0, -5), m.et_ms(2026, 11, 6, 12, 0, -5), m.et_ms(2026, 11, 7, 12, 0, -5)
        self.assertEqual((hour.slug_of(two_am), hour.start_of("bitcoin-up-or-down-november-6-2026-2am-et")), ("bitcoin-up-or-down-november-6-2026-2am-et", two_am))
        self.assertEqual([hour.slug_of(m.et_ms(2026, 11, 7, h, 0, -5)).rsplit("-", 2)[1] for h in (0, 11, 12, 23)], ["12am", "11am", "12pm", "11pm"])
        self.assertIsNone(hour.start_of("bitcoin-up-or-down-on-november-7-2026"))
        self.assertIsNone(day.start_of("ethereum-up-or-down-november-7-2026-2am-et"))
        self.assertEqual((day.start_of("ethereum-up-or-down-on-november-7-2026"), day.slug_of(noon), day.end_of(noon)), (noon, "ethereum-up-or-down-on-november-7-2026", next_noon))
        starts = p.window_starts(NOW, ctl.owner.config, ctl.settings)
        hours = [s for r, s in starts if r.key == "BTC-1H"]
        self.assertEqual((len(hours), hours[0], hours[1] - hours[0]), (24, NOW + 3600_000, 3600_000))  # 10:00 Beijing (21:00 ET) the first past the entry seconds
        self.assertEqual([s for r, s in starts if r.key == "ETH-1D"], [noon])  # the window of the market "on November 7"
        self.assertEqual((p.cycle_ms("BTC-1H"), p.cycle_ms("ETH-1D")), (3600_000, 86_400_000))
        # an hour listed: quoted, withdrawn before 2AM ET, settled after 3AM ET; a question naming another hour or date: not quoted
        hourly = {**market(mid=31), "categorySlug": "bitcoin-up-or-down-november-6-2026-2am-et", "question": "Bitcoin Up or Down - November 6, 2AM ET"}
        c = p.candidate(hourly, NOW, ctl.owner.config, ctl.settings)
        self.assertEqual((c["key"], c["start_ms"], c["end_ms"], c["cancel_ms"], c["name"]), ("BTC-1H", two_am, two_am + 3600_000, two_am - 20_000, "BTC 小时涨跌 15:00–16:00"))
        for question, why in (("Bitcoin Up or Down - November 6, 3AM ET", "问题里的时间段和链接对不上"), ("Bitcoin Up or Down - November 7, 2AM ET", "问题里的时间段和链接对不上"),
                              ("Bitcoin Up or Down - November 6, 2-3AM ET", ""), ("Bitcoin Up or Down - November 6, 2:00AM-3:00AM ET", ""), ("比特币 涨或跌", "")):
            self.assertEqual(p.candidate_problem({**hourly, "question": question}, NOW, ctl.owner.config, ctl.settings), why, question)
        daily = {**market(mid=32), "categorySlug": "ethereum-up-or-down-on-november-7-2026", "question": "Ethereum Up or Down on November 7?"}
        c2 = p.candidate(daily, NOW, ctl.owner.config, ctl.settings)
        self.assertEqual((c2["key"], c2["start_ms"], c2["end_ms"], c2["cancel_ms"], c2["name"]), ("ETH-1D", noon, next_noon, noon - 20_000, "ETH 日涨跌 11-07"))
        self.assertEqual(p.candidate_problem({**daily, "question": "Ethereum Up or Down on November 8?"}, NOW, ctl.owner.config, ctl.settings), "问题里的时间段和链接对不上")
        self.assertEqual(p.candidate_problem({**daily, "categorySlug": "bnb-up-or-down-on-november-7-2026"}, NOW, ctl.owner.config, ctl.settings), "不在 PREOPEN_MARKETS 里")
        self.assertEqual(p.candidate_problem({**daily, "categorySlug": "ethereum-up-or-down-on-november-9-2026"}, NOW, ctl.owner.config, ctl.settings), "开始还在 1440 分钟之外（之后再扫）")
        # placed: the pair carries its end, and its result is read only after it (not a quarter hour after the start)
        api.markets["31"] = hourly
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        self.assertEqual((pair["end_ms"], pair["phase"], p.settle_from(pair)), (two_am + 3600_000, "active", two_am + 3600_000))
        self.assertEqual(p.settle_from({"start_ms": two_am}), two_am + 15 * 60_000, "a record from before kept no end: a quarter hour on, as then")
        pair.update(phase="closed")
        clock.ms = two_am + 20 * 60_000  # 20 minutes into the hour: a quarter-hour window's result would be read by now
        reads = []
        real = api.market

        async def market_read(mid):
            reads.append(mid)
            return await real(mid)
        api.market = market_read
        await ctl.settle_pair(pair)
        self.assertEqual(reads, [], "the hour is not over: no result asked for")
        ctl.listed_at["BTC-1H"] = NOW
        self.assertIn("小时卡每小时、日卡每天", ctl.status())
        cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_MARKETS": "all"})
        self.assertTrue({"BTC-1H", "ETH-1H", "BNB-1H", "BTC-1D", "ETH-1D", "BNB-1D"} <= cfg.preopen.markets)
        await self.finish(ctl)

    async def test_all_cards_off_and_weekends_off(self):
        """1.136.0: PREOPEN_MARKETS=none (the page's 一键全关) quotes no card — the mode stays on, a resting pair is withdrawn
        at the takeover as a settings change (free); PREOPEN_WEEKEND=off leaves out the windows starting on a Saturday or
        Sunday (Beijing time) and withdraws resting ones the same way. NOW is Friday 09:00 Beijing."""
        ctl, api, clock = controller(PREOPEN_MARKETS="none")
        self.assertEqual(ctl.settings.markets, frozenset())
        self.assertEqual(p.window_starts(NOW, ctl.owner.config, ctl.settings), [])
        self.assertIn("没选卡：不挂新场次", ctl.status())
        await ctl.discover()
        self.assertEqual(ctl.queue.qsize(), 0)
        await self.finish(ctl)
        self.assertTrue(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline"}).preopen.weekend, "the default: weekends quoted")
        with self.assertRaises(ValueError):
            m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64, "PREOPEN_MARKETS": "nope"})
        sat = NOW + 15 * 3600_000  # Saturday 00:00 Beijing
        self.assertEqual([p.on_weekend(t) for t in (NOW, sat - 1, sat, sat + 47 * 3600_000, sat + 48 * 3600_000, sat + 56 * 3600_000 - 1, sat + 56 * 3600_000)],
                         [False, False, True, True, True, True, False], "1.139.2: to Monday 08:00 Beijing")
        ctl, api, clock = controller(PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="1440", PREOPEN_WEEKEND="off")
        self.assertFalse(ctl.settings.weekend)
        starts = [s for _, s in p.window_starts(NOW, ctl.owner.config, ctl.settings)]
        self.assertEqual((max(starts), sat in starts), (sat - 15 * 60_000, False), "Friday's last window is the last")
        self.assertIn("周末不挂（北京时间周六 00:00 到周一 08:00 开始的场次）", ctl.status())
        clock.ms = sat + 3600_000
        self.assertEqual(p.window_starts(clock.ms, ctl.owner.config, ctl.settings), [])
        self.assertIn("周末不挂：现在是周末", ctl.status())
        await self.finish(ctl)
        for env, note in (({"PREOPEN_MARKETS": "none"}, "一键全关"), ({"PREOPEN_WEEKEND": "off"}, "the weekend switched off")):
            with tempfile.TemporaryDirectory() as folder:  # quoted before the switch: the next start withdraws it, a free withdrawal
                db = {"STATE_DB": str(Path(folder) / "state.sqlite3")}
                ctl, api, clock = controller([market(at=sat)], PREOPEN_MARKETS="btc", PREOPEN_AHEAD_MINUTES="1440", **db)
                c = p.candidate(market(at=sat), NOW, ctl.owner.config, ctl.settings)
                await ctl.place_pair(c)
                self.assertEqual(ctl.ledger.rows[c["slug"]]["phase"], "active", note)
                ctl.keep_on_stop = True
                await ctl.on_stop()
                ctl.ledger.close()
                again, _, _ = controller([market(at=sat)], PREOPEN_AHEAD_MINUTES="1440", **{"PREOPEN_MARKETS": "btc", **env}, **db)
                again.api = again.cancel_api = api; api.ctl = again
                again.adopt()
                pair = again.ledger.rows[c["slug"]]
                self.assertEqual((pair["phase"], pair["reason"], pair.get("free"), again.resumed), ("closing", "设置不同，撤掉后重新报价", True, 0), note)
                await again.cancel_pair(pair, pair["reason"])
                await again.discover()
                self.assertEqual(again.queue.qsize(), 0, note)
                await self.finish(again)

    async def test_status_says_why_nothing_is_placed(self):
        """The holds that used to be silent — the snapshot not read or stale, open orders nobody here placed — and the
        budget are on the status line; the pause names its reason."""
        ctl, api, clock = controller()
        self.assertEqual((ctl.hold(), ctl.budget_problem()), ("", ""))
        self.assertNotIn("暂不新挂", ctl.status())
        api.rows["other"] = {"id": "manual", "order": {"hash": "0xother", "side": 0, "makerAmount": str(L.to_wei(1))},
                             "amount": str(L.to_wei(2)), "amountFilled": "0", "status": "OPEN"}
        await ctl.account()
        self.assertEqual((ctl.unmanaged, ctl.hold(), ctl.open_reserved), (1, "", 1.0))  # 1.126.0: shown, no hold; its money in the balance check
        await ctl.place_pair(p.candidate(market(), NOW, ctl.owner.config, ctl.settings))
        self.assertEqual(len(api.sent), 2)
        self.assertIn("\n账户另有 1 笔本程序没记录的挂单", ctl.status())
        self.assertNotIn("暂不新挂", ctl.status())
        del api.rows["other"]
        await ctl.account()
        self.assertEqual((ctl.unmanaged, ctl.hold()), (0, ""))
        self.assertNotIn("账户另有", ctl.status())
        clock.ms += 31_000
        self.assertEqual(ctl.hold(), "账户快照过期（31 秒前读到，每 5 秒读一次）")
        ctl.account_at = 0
        self.assertIn("还没有读到", ctl.hold())
        ctl.account_at = clock.ms
        self.assertEqual((ctl.settings.max_open_usd, ctl.budget_problem(9_000.0)), (0.0, ""), "1.132.3: no cap by default — Predict locks nothing for a resting order")
        self.assertIn("不够一场双边", ctl.budget_problem(49_000_000.0), "the balance must still cover one pair")
        self.assertIn("预留 49.00U，不设上限（本模式自己的账", ctl.status())
        ctl.settings = dataclasses.replace(ctl.settings, max_open_usd=48)
        self.assertEqual(ctl.hold(), "")
        self.assertIn("\n暂不新挂：预挂资金上限不足：已预留 49.00U，再挂一场双边要 49U，限额 48U（PREOPEN_MAX_OPEN_USD）", ctl.status())
        self.assertIn("预留 49.00U / 限额 48U（本模式自己的账", ctl.status())
        self.assertIn("各卡：BTC-15M 还没见到已发布的场次，挂到 10:00:00；", ctl.status())
        ctl.manual_pause = True
        self.assertTrue(ctl.status().startswith("开盘前自动预挂：暂停，本模式已手动暂停（/preopen resume 恢复）\n"), ctl.status())
        self.assertNotIn("暂不新挂", ctl.status())
        await self.finish(ctl)

    async def test_known_orders_are_excluded_from_unknown_warning(self):
        ctl, api, _ = controller()
        await ctl.place_pair(p.candidate(market(), NOW, ctl.owner.config, ctl.settings))
        self.assertEqual(ctl.unmanaged_rows(await api.orders()), [])
        self.assertEqual(len(ctl.unmanaged_rows([{"id": "manual", "order": {"hash": "other"}}])), 1)
        await self.finish(ctl)

    async def test_deferred_postonly_rejection_cancels_surviving_side(self):
        ctl, api, clock = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        api.rows[pair["orders"]["up"]["hash"]]["status"] = "CANCELLED"
        clock.ms += 15_000
        await ctl.monitor_pair(pair)
        self.assertIn("2", api.removed)
        self.assertEqual(pair["phase"], "closed")
        self.assertEqual(len(api.sent), 2)
        await self.finish(ctl)

    async def test_unknown_status_does_not_finalize(self):
        self.assertFalse(p.final_order({"amount": "50", "amountFilled": "0", "status": "UNKNOWN"}))
        self.assertFalse(p.final_order({"amountFilled": "0", "status": "CANCELLED"}))

    async def test_operator_cancel_and_resume_include_preopen(self):
        ctl, api, _ = controller()
        await ctl.place_pair(p.candidate(market(), NOW, ctl.owner.config, ctl.settings))
        bot = L.LiveBot.__new__(L.LiveBot)
        bot.preopen = ctl
        async def normal_cancel(*args): return "normal orders: none"
        bot._live_cancel_command = normal_cancel
        message = await bot.live_cancel_command(["all"], NOW)
        self.assertTrue(ctl.manual_pause)
        self.assertTrue(ctl.ledger.pause())
        self.assertEqual(set(api.removed), {"1", "2"})
        self.assertIn("预挂模式已暂停", message)
        req = m.Request("/preopen", ["resume"], 1, 0, 1)
        await bot.cmd_preopen(req)
        self.assertFalse(ctl.manual_pause)
        self.assertFalse(ctl.ledger.pause())
        self.assertIn("开盘前预挂体检", await bot.cmd_preopen(m.Request("/preopen", ["check"], 1, 0, 1)))
        await self.finish(ctl)

    async def test_check_reports_anomalies_and_the_alerts_go_out(self):
        """1.130.1: /preopen check says what is wrong (the cooldown and the 429s, the snapshot, the holds, the parked
        candidates, the stuck pairs, the orders in doubt, the last problems), and a cooldown, a stuck pair and a new problem
        are announced to the administrator's chats; a record that says FILLED without an amount is the whole order."""
        ctl, api, clock = controller()
        notes = []
        ctl.owner.live_notify = lambda text, key="": notes.append((key, text))
        self.assertIn("：正常", ctl.check())
        ctl.error = "HTTP 500: boom"
        self.assertEqual((ctl.errors[-1][1], notes[-1][0]), ("HTTP 500: boom", "preopen:error:HTTP #: boom"))  # (1.154.60: the key without the figures)
        ctl.error = "Predict 接口限流冷却中（20 秒后重试）"
        ctl.error = "Predict 接口限流冷却中（19 秒后重试）"
        self.assertEqual([key for key, _ in notes[-2:]], ["preopen:error:Predict 接口限流冷却中（# 秒后重试）"] * 2, "a countdown: one key, so one notice in ten minutes (live_notify)")
        self.assertEqual(notes[-1][1], "⚠️ 开盘前预挂：Predict 接口限流冷却中（19 秒后重试）")
        ctl.errors.pop(); ctl.errors.pop()
        ctl._error = "HTTP 500: boom"
        ctl.error = "HTTP 500: boom"
        self.assertEqual(len(ctl.errors), 1, "the same problem again: kept once")
        api.blocked_until = time.monotonic() + 30
        ctl.note_cooldown(); ctl.note_cooldown()
        self.assertEqual((len(ctl.limited_at), notes[-1][0], ctl.in_cooldown), (1, "preopen:429", True))
        text = ctl.check()
        self.assertTrue("：有问题" in text and "限流冷却中" in text and "24 小时内触发 1 次" in text and "HTTP 500: boom" in text, text)
        api.blocked_until = 0.0
        ctl.note_cooldown()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        # a record that says FILLED without the amount: the whole order — the other side stays
        up = pair["orders"]["up"]
        api.rows[up["hash"]].update(status="FILLED", amountFilled="0")
        clock.ms += 10_000; ctl.account_at = clock.ms; ctl.listed_ids = set()
        await ctl.monitor_pair(pair)
        self.assertEqual((up["state"], up["filled"], api.removed, pair["phase"]), ("final", 50.0, [], "active"))
        self.assertIn("按全部成交记", up.get("note", "")) if up.get("note") else None
        # a stuck pair: withdrawn long ago, never cleared
        stuck = {**pair, "slug": "btc-updown-15m-stuck", "name": "BTC 15分涨跌 08:00–08:15", "phase": "closing", "accounted": False, "cancel_ms": clock.ms - 20 * 60_000,
                 "reason": "到时撤单", "orders": {"up": {"state": "unknown", "hash": "0xs", "filled": 0.0}, "down": {"state": "removed", "hash": "0xt", "id": "9", "filled": 0.0}}}
        ctl.ledger.save(stuck)
        filled = {**stuck, "slug": "btc-updown-15m-filled", "start_ms": clock.ms - 20 * 60_000, "end_ms": clock.ms - 5 * 60_000, "cancel_ms": clock.ms - 20 * 60_000 - 20_000,
                  "orders": {"up": {"state": "final", "hash": "0xf", "id": "8", "filled": 10.0}, "down": {"state": "final", "hash": "0xg", "id": "7", "filled": 0.0}}}
        ctl.ledger.save(filled)
        self.assertEqual([p["slug"] for p in ctl.stuck_pairs(clock.ms)], ["btc-updown-15m-stuck"], "a fill waits for the result: not stuck 20 minutes past the start")
        self.assertEqual(len(ctl.stuck_pairs(clock.ms + 30 * 60_000)), 2)
        ctl.ledger.delete(filled["slug"])
        ctl.check_stuck(clock.ms)
        self.assertEqual(notes[-1][0], "preopen:stuck:btc-updown-15m-stuck")
        text = ctl.check()
        self.assertTrue("未结清：1 场" in text and "状态未知的订单：1 张" in text and "up unknown" in notes[-1][1], text)
        await self.finish(ctl)

    async def test_a_fill_on_one_side_never_withdraws_the_other(self):
        """11-17 (the operator's question: 47¢ 的涨成交了，47¢ 的跌会不会被撤？): no. A fill on one side — the whole order, or part of it
        before its end — leaves the other side resting until it fills too or the window's cancel time; only a side that ends with
        nothing filled (Predict refused, cancelled or expired it) has the other side of its level withdrawn (一边未成交即结束)."""
        for case in ("filled", "part then cancelled", "nothing filled"):
            ctl, api, clock = controller()
            c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
            await ctl.place_pair(c)
            pair = ctl.ledger.rows[c["slug"]]
            up, down = pair["orders"]["up"], pair["orders"]["down"]
            row = api.rows[up["hash"]]
            amount = int(row["amount"])
            row.update({"filled": dict(status="FILLED", amountFilled=str(amount)),
                        "part then cancelled": dict(status="CANCELLED", amountFilled=str(amount // 5)),
                        "nothing filled": dict(status="CANCELLED", amountFilled="0")}[case])
            clock.ms += 10_000; ctl.account_at = clock.ms; ctl.listed_ids = set()
            await ctl.monitor_pair(pair)
            if case == "nothing filled":
                self.assertEqual((api.removed, pair.get("reason")), ([down["id"]], "一边未成交即结束，撤掉另一边"), case)
                self.assertIn(pair["phase"], {"closing", "closed"}, case)
            else:
                self.assertTrue(up["filled"] > 0, case)
                self.assertEqual((api.removed, pair["phase"], down["state"]), ([], "active", "open"), case)
            await self.finish(ctl)

    async def test_default_is_off_and_normal_strategy_held_only_while_the_mode_runs_for_real(self):
        cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline"})
        self.assertFalse(p.Settings.from_config(cfg).enabled)
        bot = L.LiveBot.__new__(L.LiveBot)
        bot.preopen = types.SimpleNamespace(alive=True, risk=lambda: 40., daily_pnl=lambda: -10., filled_by_outcome=lambda days=3: {("123", 2): 50.})
        bot.config = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64, "PREOPEN_EXCLUSIVE": "on"})
        self.assertTrue(bot.preopen_active() and bot.preopen_exclusive())
        self.assertIn("不开新仓", bot.live_room({}, None, "up", .49, 50))
        self.assertIn("不发送", bot.live_send_block({"live": {}}, {}, NOW))
        # 1.125.0: off by default — the two run side by side; the mode's money, day and fills count in the model trader's checks
        bot.config = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64, "LIVE_MAX_OPEN_USD": "100"})
        self.assertTrue(bot.preopen_active() and not bot.preopen_exclusive() and not bot.config.preopen.exclusive)
        self.assertEqual((bot.preopen_risk(), bot.preopen_pnl(), bot.preopen_filled()), (40., -10., {("123", 2): 50.}))
        bot.live = types.SimpleNamespace(usd=lambda: None)
        self.assertEqual(bot.live_fit_shares({}, 0.5), 200.)  # min(100 order cap, 100 − 0) / 0.5: the mode's 40U reservation is its own cap's (1.132.1; was 120)
        bot.config = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64, "LIVE_MAX_OPEN_USD": "40.5"})
        self.assertEqual((bot.live_fit_why({}, 0.5), bot.live_fit_shares({}, 0.5)), ("", 81.), "the model trader keeps its whole LIVE_MAX_OPEN_USD")
        bot.preopen.alive = False
        self.assertFalse(bot.preopen_active())  # crashed: the model trader is free again — the mode's fills still its own (1.131.2), never positions to adopt
        # Audit PRE-PNL-DROP: ...and its day's realised loss still in the loss stop (this pinned 0: a crash gave the model
        # trader its whole LIVE_MAX_DAILY_LOSS_USD back); the reservation and the exclusive hold end with the mode
        self.assertEqual((bot.preopen_risk(), bot.preopen_pnl(), bot.preopen_filled()), (0., -10., {("123", 2): 50.}))
        bot.preopen.alive = True
        bot.config = cfg  # LIVE=off: paper trading goes on
        self.assertFalse(bot.preopen_active())
        bot.preopen = None
        self.assertFalse(bot.preopen_active())
        self.assertIn("未启用", await bot.cmd_preopen(m.Request("/preopen", [], 1, 0, 1)))

    async def test_control_page_starts_stops_and_restarts_the_mode(self):
        """PREOPEN_15M and the PREOPEN_* settings saved on the page: the live bot starts the mode when on, stops it when off, and
        restarts it (its orders withdrawn by its own run) when a setting changed."""
        class FakeCtl:
            made = []
            def __init__(self, owner):
                self.owner, self.settings, self.alive, self.stop, self.error = owner, owner.config.preopen, True, asyncio.Event(), ""
                self.ledger = types.SimpleNamespace(close=lambda: None)
                self.ran, self.ended, self.winding = asyncio.Event(), False, ""
                FakeCtl.made.append(self)
            def wind_down(self, reason):  # 1.141.0: switched off — its orders withdrawn until none is left; this one has none, so it ends at once
                self.winding = reason
                self.stop.set()
            async def run(self):
                self.ran.set()
                try:
                    await self.stop.wait()
                finally:
                    self.ended = True
                    self.alive = False
            def status(self): return "fake"
        env = {"TELEGRAM_BOT_TOKEN": "offline", "STATE_DB": ":memory:", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64}
        bot = L.LiveBot.__new__(L.LiveBot)
        bot.config, bot.stopping = m.Config.from_env({**env, "PREOPEN_15M": "on"}), asyncio.Event()
        bot.preopen, bot.preopen_task, bot.preopen_factory, bot.preopen_lock = None, None, FakeCtl, asyncio.Lock()
        await bot.preopen_apply()
        first = bot.preopen
        self.assertIsInstance(first, FakeCtl) and self.assertTrue(bot.preopen_active())
        await asyncio.wait_for(first.ran.wait(), 1)
        await bot.preopen_apply()
        self.assertIs(bot.preopen, first, "the same settings: not restarted")
        bot.config = m.Config.from_env({**env, "PREOPEN_15M": "on", "PREOPEN_SHARES": "20"})
        await bot.preopen_apply()
        self.assertTrue(first.ended and bot.preopen is not first and bot.preopen.settings.shares == 20, "a setting changed: stopped, started anew")
        self.assertTrue(first.keep_on_stop, "a settings change leaves the orders on the book for the next controller")
        second = bot.preopen
        bot.config = m.Config.from_env({**env, "PREOPEN_15M": "off"})
        bot.market = types.SimpleNamespace(now_ms=lambda: NOW)
        await bot.preopen_apply()
        self.assertEqual(second.winding, "预挂模式已关闭")  # winding down: its orders withdrawn, then it stops itself
        for _ in range(100):
            if bot.preopen is None:
                break
            await asyncio.sleep(0.01)
        self.assertTrue(second.ended and bot.preopen is None and bot.preopen_task is None and not bot.preopen_active(), "off: wound down, cleared")
        self.assertFalse(second.keep_on_stop, "off: withdrawn")
        await bot.preopen_apply()
        self.assertIsNone(bot.preopen)
        bot.config = m.Config.from_env({**env, "PREOPEN_15M": "on"})
        await bot.preopen_apply()
        third = bot.preopen
        third.alive = False  # crashed: the next apply starts it afresh
        await bot.preopen_apply()
        self.assertTrue(bot.preopen is not third and bot.preopen.alive)
        await bot.preopen_stop("test")
        self.assertEqual(len(FakeCtl.made), 4)

    async def test_fills_by_outcome_for_the_positions_reconciliation(self):
        ctl, api, clock = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        self.assertEqual((pair["orders"]["up"]["index_set"], pair["orders"]["down"]["index_set"]), (2, 1))  # Up is the second outcome here
        self.assertEqual(ctl.filled_by_outcome(), {})
        api.rows[pair["orders"]["up"]["hash"]]["amountFilled"] = str(L.to_wei(20))
        await ctl.account()
        self.assertEqual(ctl.filled_by_outcome(), {("123", 2): 20.})
        clock.ms = c["start_ms"] + 4 * 86_400_000
        self.assertEqual(ctl.filled_by_outcome(), {}, "windows older than three days: redeemed or settled by then")
        self.assertTrue(ctl.settings.exclusive is False and p.Settings.from_config(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_EXCLUSIVE": "on"})).exclusive)
        await self.finish(ctl)

    async def test_probe_reports_each_coming_window(self):
        ctl, api, _ = controller([market(), market(at=NOW + 15 * 60_000, mid=5, status="CLOSED"), market("sol", mid=6)])
        lines = await p.probe(api, ctl.owner.config, ctl.settings, NOW)
        text = "\n".join(lines)
        self.assertIn("btc-updown-15m-1793930400（10:00）：会挂；状态 REGISTERED / 交易状态 MATCHING_NOT_ENABLED；缺字段：question", text)  # the fixture states no question
        self.assertIn("btc-updown-15m-1793927700（09:15）：不挂：交易状态 CLOSED", text)
        self.assertGreaterEqual(text.count("：还没发布"), 6)  # the other BTC quarters, 海力士's and 宇树's
        self.assertNotIn("sol", text)
        self.assertIn("只看 2 小时内、每张卡最近 8 场", text)  # 1.130.6: bounded — a day of five cards was hundreds of reads
        self.assertLessEqual(text.count("btc-updown-15m-"), 8)
        async def broken(method, path, **kw): raise m.RemoteError("HTTP 403: 访问被拒绝")
        api.call = broken
        self.assertIn("读取失败：HTTP 403", "\n".join(await p.probe(api, ctl.owner.config, ctl.settings, NOW)))
        empty = p.Settings.from_config(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on", "STATE_DB": ":memory:", "PREOPEN_MARKETS": "unitree"}))
        self.assertIn("没有要看的场次", "\n".join(await p.probe(api, ctl.owner.config, empty, NOW + 24 * 3600_000)))  # Saturday 09:00: no 宇树 session for a day
        text = "\n".join(await p.probe(api, ctl.owner.config, empty, NOW))
        self.assertTrue(2 <= text.count("）：读取失败：") <= 4 and text.count("没问（UNITREE-15M 连续读取失败）") >= 3, text)  # failures end a card's reads
        await self.finish(ctl)

    async def test_rate_limit_cooldown_is_shared_with_the_bots_client(self):
        shared = types.SimpleNamespace(blocked_until=0.0)
        api = p.safe_api("https://offline.invalid/v1", "key", None, None, shared)
        api.blocked_until = 5.0
        self.assertEqual(shared.blocked_until, 5.0)
        shared.blocked_until = 9.0
        self.assertEqual(api.blocked_until, 9.0)
        alone = p.safe_api("https://offline.invalid/v1", "key", None)
        alone.blocked_until = 3.0
        self.assertEqual(alone.blocked_until, 3.0)

    async def test_account_refresh_commits_snapshot_atomically(self):
        ctl, api, _ = controller()
        started, release = asyncio.Event(), asyncio.Event()
        original = api.orders
        async def slow_orders(*args):
            started.set(); await release.wait(); return await original(*args)
        api.orders = slow_orders
        task = asyncio.create_task(ctl.account())
        await started.wait()
        self.assertEqual(ctl.account_at, NOW)
        release.set(); await task
        self.assertEqual(ctl.account_at, NOW)
        async def fail(*args): raise m.RemoteError("failed list")
        api.orders = fail
        with self.assertRaises(m.RemoteError): await ctl.account()
        self.assertEqual(ctl.account_at, 0)
        await self.finish(ctl)

    async def test_restart_takes_over_matching_quotes_and_requotes_withdrawn_windows(self):
        """1.127.0: at a start the last run's active pair that is what the settings say keeps resting (its queue position
        with it); one that is not is withdrawn and the window quoted again under a new salt; a window withdrawn by a stop
        is quoted again too, at most MAX_ATTEMPTS times, never after Predict's own refusal; the process stopping or a
        settings change leaves the orders on the book, switching off withdraws them."""
        with tempfile.TemporaryDirectory() as folder:
            db = {"STATE_DB": str(Path(folder) / "state.sqlite3")}
            ctl, api, clock = controller(**db)
            c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
            await ctl.place_pair(c)
            ctl.keep_on_stop = True
            await ctl.on_stop()
            self.assertEqual(api.removed, [], "a settings change: the orders stay")
            ctl.ledger.close()
            again, _, _ = controller(**db)
            again.api = again.cancel_api = api; api.ctl = again  # the same account: the orders placed by the last run
            again.adopt()
            pair = again.ledger.rows[c["slug"]]
            self.assertEqual((pair["phase"], again.resumed, pair["cancel_ms"]), ("active", 1, NOW + 3600_000 - 20_000))
            await again.discover()
            self.assertEqual(again.queue.qsize(), 0, "taken over: not quoted again")
            again.owner.stopping.set()
            await again.on_stop()
            self.assertEqual(api.removed, [], "the process stopping: the orders stay")
            again.ledger.close()
            bigger, _, bclock = controller(PREOPEN_SHARES="20", **db)
            bigger.api = bigger.cancel_api = api; api.ctl = bigger
            bigger.adopt()
            pair = bigger.ledger.rows[c["slug"]]
            self.assertEqual((pair["phase"], pair["reason"], bigger.resumed), ("closing", "设置不同，撤掉后重新报价", 0))
            await bigger.cancel_pair(pair, pair["reason"])
            # Audit PRE-LATE-FILL: withdrawn, but not accounted — nor quoted again — on a read right after the removal (this pinned
            # both): the records are read LATE_FILL_MS on, and the window waits for them
            self.assertEqual((set(api.removed), pair.get("accounted"), bigger.supersedable(pair)), ({"1", "2"}, None, False))
            await bigger.discover()
            self.assertEqual((bigger.queue.qsize(), bigger.scan_state["pending"]), (0, 1), "the withdrawn level waits for its final reads")
            await self.read_finals(bigger, bclock, pair)
            self.assertEqual((pair["accounted"], bigger.supersedable(pair)), (True, True))
            bigger.account_at = bclock.ms
            await bigger.discover()
            self.assertEqual((bigger.queue.qsize(), bigger.requoted, c["slug"] in bigger.ledger.rows), (1, 1, False))
            old_key = c["slug"] + "@0"  # the window and the attempt that stepped aside
            self.assertIs(bigger.ledger.rows[old_key], pair)
            queued = bigger.queue.get_nowait()[2]
            self.assertEqual(queued["attempt"], 1)
            await bigger.place_pair(queued)
            fresh = bigger.ledger.rows[c["slug"]]
            self.assertEqual((fresh["attempt"], len(api.sent)), (1, 4))
            self.assertNotEqual({o["hash"] for o in fresh["orders"].values()}, {o["hash"] for o in pair["orders"].values()}, "a new salt")
            self.assertEqual(api.sent[2]["data"]["order"]["takerAmount"], str(L.to_wei(20)))
            self.assertEqual(bigger.risk(), 2 * 0.49 * 20)  # the withdrawn pair released, the new one reserved
            self.assertIn("已记录 1 场、2 条（1 条挂着，1 条已结清）", bigger.status())
            self.assertIn("重新报价 1 场", bigger.status())
            # the cap (the window's own failures: the settings change above was none of them — 1.131.6) and Predict's own refusal
            self.assertTrue(pair.get("free") and not p.counts_against(pair))
            fresh["attempt"] = p.MAX_ATTEMPTS - 1
            await bigger.cancel_pair(fresh, "test")
            await self.read_finals(bigger, bclock, fresh)  # (its records read LATE_FILL_MS after the removal: Audit PRE-LATE-FILL)
            self.assertTrue(fresh["accounted"] and bigger.supersedable(fresh), "one failure: quoted again, whatever the salt's attempt")
            self.assertFalse(bigger.supersedable(fresh, strikes=p.MAX_ATTEMPTS))
            fresh["attempt"] = 0
            self.assertTrue(bigger.supersedable(fresh))
            fresh["orders"]["up"]["state"] = "rejected"
            self.assertFalse(bigger.supersedable(fresh))
            fresh["orders"]["up"]["limited"] = True
            self.assertTrue(bigger.supersedable(fresh))
            bclock.ms = NOW + 3600_000 - 170_000  # bigger's own clock
            self.assertFalse(bigger.supersedable(fresh), "too close to the start")
            bigger.keep_on_stop = False
            bigger.owner.stopping.clear()
            await self.finish(bigger)
            bigger.ledger = p.Ledger(db["STATE_DB"] + ".x")  # on_stop on a closed ledger would fail: a fresh one for the off case
            bigger.ledger.save({**ctl.ledger.rows.get(c["slug"], fresh), "slug": "w", "phase": "active", "orders": {"up": {"id": "9", "state": "open", "hash": "x", "filled": 0.0}}, "start_ms": NOW + 3600_000, "cancel_ms": NOW + 3580_000, "expires": NOW // 1000 + 3590})
            await bigger.on_stop()
            self.assertIn("9", api.removed, "switched off: withdrawn")
            bigger.ledger.close()

    async def test_review_tables_for_the_journal_page(self):
        """1.128.0: the review page's 开盘前预挂 section from the ledger — the totals, by day / card / start hour / side, the
        windows that filled; a pair keeps its own price and shares, and its result once settled."""
        with tempfile.TemporaryDirectory() as folder:
            ctl, api, clock = controller(STATE_DB=str(Path(folder) / "s.sqlite3"))
            c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
            await ctl.place_pair(c)  # active, nothing filled: 50 a side at 49¢ (the tests' size)
            placed = ctl.ledger.rows[c["slug"]]
            self.assertEqual((placed["price"], placed["shares"], p.pair_quote(placed)), ("0.49", "50", (0.49, 50.0)))
            self.assertEqual(p.pair_quote({**placed, "price": "", "shares": ""}), (0.49, 50.0), "an older record: read off the signed order")
            base = {"key": "BTC-15M", "price": "0.49", "shares": "10", "created_ms": NOW - 20 * 3600_000, "attempt": 0}
            won = {**base, "slug": "btc-updown-15m-1", "name": "BTC 15分涨跌 10:00–10:15", "start_ms": NOW + 3600_000, "phase": "closed", "accounted": True,
                   "pnl": 5.1, "result": "up", "cancel_sent_ms": NOW, "cancel_done_ms": NOW + 400,
                   "orders": {"up": {"state": "final", "filled": 10.0}, "down": {"state": "final", "filled": 0.0}}}
            withdrawn = {**base, "slug": "btc-updown-15m-2", "name": "BTC 15分涨跌 10:15–10:30", "start_ms": NOW + 4500_000, "phase": "closed", "accounted": True,
                         "pnl": 0.0, "orders": {"up": {"state": "final", "filled": 0.0}, "down": {"state": "final", "filled": 0.0}}}
            pending = {**base, "key": "UNITREE-15M", "slug": "unitree-updown-15m-3", "name": "宇树 15分涨跌 13:00–13:15", "start_ms": NOW + 4 * 3600_000, "phase": "closing",
                       "attempt": 1, "orders": {"up": {"state": "removed", "filled": 2.0}, "down": {"state": "removed", "filled": 3.0}}}
            never = {**base, "slug": "btc-updown-15m-4", "name": "BTC 15分涨跌 11:00–11:15", "start_ms": NOW + 7200_000, "phase": "closed", "accounted": True,
                     "orders": {"up": {"state": "not_sent", "filled": 0.0}, "down": {"state": "rejected", "filled": 0.0}}}
            tomorrow = {**base, "slug": "btc-updown-15m-5", "name": "BTC 15分涨跌 05:00–05:15", "start_ms": NOW + 20 * 3600_000, "phase": "active",
                        "orders": {"up": {"state": "open", "filled": 0.0}, "down": {"state": "open", "filled": 0.0}}}
            for row in (won, withdrawn, pending, never, tomorrow):
                ctl.ledger.save(row)
            r = p.review(list(ctl.ledger.rows.values()), NOW)
            self.assertEqual(([d["name"] for d in r["days"]], sorted(r["daily"]["days"])), (["11-07", "11-06"], ["2026-11-06"]), "tomorrow's resting pairs: a table row, no summary day")
            ctl.ledger.delete(tomorrow["slug"])
            r = p.review(list(ctl.ledger.rows.values()), NOW)
            t = r["total"]
            self.assertEqual((t["records"], t["windows"], t["resting"], t["filled_windows"], t["filled"], t["volume"], t["pending"]), (4, 4, 1, 2, 15.0, 7.35, 1))
            self.assertEqual((t["settled"], t["wins"], t["losses"], t["ties"], t["pnl"], t["withdrawn"]), (1, 1, 0, 0, 5.1, 1))
            self.assertEqual(r["resting_usd"], 49.0)
            self.assertEqual([(d["name"], d["records"]) for d in r["days"]], [("11-06", 4)])
            self.assertEqual([(d["name"], d["records"], d["filled_windows"]) for d in r["cards"]], [("BTC 15分涨跌", 3, 1), ("宇树 15分涨跌", 1, 1)])
            self.assertEqual([d["name"] for d in r["hours"]], ["BTC 15分涨跌 10 时开始", "宇树 15分涨跌 13 时开始"])
            up, down = r["sides"]
            self.assertEqual((up["name"], up["records"], up["filled_windows"], up["filled"], up["settled"], up["wins"], up["pnl"]), ("涨", 4, 2, 12.0, 1, 1, 5.1))
            weekend, weekday = r["weekly"]  # 1.139.2: 11-06 is a Friday — every window a weekday's
            self.assertEqual((weekend["name"], weekend["records"], weekday["name"], weekday["records"], weekday["pnl"]), ("周末（周六 00:00 到周一 08:00）", 0, "工作日", 4, 5.1))
            bets = p.ev_bets(list(ctl.ledger.rows.values()))  # 1.140.0: the settled fills as coin-flip bets for the EV timeline — the won pair's up side only
            self.assertEqual([(b["strategy"], b["card"], b["day"], b["shares"], round(b["ev"], 6), round(b["pnl"], 6), b["var"], b["fair"]) for b in bets],
                             [("预挂", "BTC 15分涨跌", "2026-11-06", 10.0, 0.1, 5.1, 25.0, 0.5)])
            ev = m.sim_ev_timeline(bets, NOW)
            self.assertEqual((ev["total"]["bets"], ev["strategies"][0]["name"], ev["total"]["verdict"], round(ev["days"][0]["cum_pnl"], 6)), (1, "预挂", "样本不足", 5.1))
            self.assertEqual((down["records"], down["filled"], down["volume"], down["settled"]), (4, 3.0, 1.47, 0))
            self.assertEqual([(x["name"], x["status"], x["pnl"], x["attempt"]) for x in r["recent"]],
                             [("宇树 15分涨跌 13:00–13:15", "待结算", None, 1), ("BTC 15分涨跌 10:00–10:15", "已结算", 5.1, 0)])
            self.assertEqual((r["recent"][1]["cancel_s"], r["recent"][1]["lead_min"], r["recent"][1]["result"]), (0.4, 1260.0, "up"))
            # the live bot: the running controller's ledger, or the file while the mode is off; nothing recorded → None
            self.assertEqual((r["daily"]["days"]["2026-11-06"]["records"], r["daily"]["days"]["2026-11-06"]["pnl"], r["daily"]["total"]["open"], r["daily"]["total"]["open_cost"]), (4, 5.1, 1, 2.45))
            bot = L.LiveBot.__new__(L.LiveBot)
            bot.preopen, bot.config, bot.market = ctl, ctl.owner.config, ctl.owner.market
            self.assertEqual(bot.preopen_review()["total"]["records"], 4)
            lv = bot.preopen_levels("2026-11-06", "2026-11-07")  # 1.154.35: 预挂按价格档位 on 按市场's days, read as preopen_cards reads them
            self.assertEqual(([x["name"] for x in lv["levels"]], lv["pnl"], lv["settled"]), (["49¢×50", "49¢×10"], 5.1, 1))
            self.assertIsNone(m.Bot.preopen_levels(bot, "2026-11-06", "2026-11-07"), "a bot without the mode: no block")
            # 1.154.26: within one build of the review page (review_scope) the review is made once — the daily summary and the
            # analysis both ask — and each record's figures once (quote_of: four walks of every record a build); the same
            # figures as made afresh, and nothing kept beyond the build
            rows_now = list(ctl.ledger.rows.values())
            fresh_review, fresh_bets, fresh_cards = p.review(rows_now, NOW), p.ev_bets(rows_now), p.cards_between(rows_now, "2026-11-06", "2026-11-07")
            self.assertIsNot(p.quote_of(rows_now[0]), p.quote_of(rows_now[0]))
            made, real_made = [], p.quote_made
            p.quote_made = lambda pair: made.append(pair.get("slug")) or real_made(pair)
            try:
                with bot.review_scope():
                    first = bot.preopen_review()
                    self.assertIs(bot.preopen_review(), first)
                    self.assertIs(p.quote_of(rows_now[0]), p.quote_of(rows_now[0]))
                    self.assertEqual((p.review(rows_now, NOW), p.ev_bets(rows_now), p.cards_between(rows_now, "2026-11-06", "2026-11-07")), (fresh_review, fresh_bets, fresh_cards))
                    with bot.review_scope():  # (one within another: the outer one's)
                        self.assertIs(bot.preopen_review(), first)
                    self.assertIs(bot.preopen_review(), first)
                    # ...of the controller's own records alone. One read off a file for this build (the archive, the working file
                    # while the mode is off: new objects at every read) is made and let go, never kept until the build's end —
                    # tens of thousands of them together were 600 MB and twice the time at every start's first build
                    off_file, own_makes = [json.loads(json.dumps(r)) for r in rows_now], list(made)
                    self.assertIsNot(p.quote_of(off_file[0]), p.quote_of(off_file[0]))
                    self.assertEqual(p.review(off_file, NOW), fresh_review)
                    self.assertTrue(all(held[0] is ctl.ledger.rows.get(held[0]["slug"]) for held in p.QUOTES_KEPT[1].values()) and len(p.QUOTES_KEPT[1]) <= len(rows_now))
                    self.assertGreater(len(made), len(own_makes), "made afresh each time it is asked")
                    made[:] = own_makes  # (the count below is of the controller's own records)
                self.assertEqual((len(made), len(set(made)), set(made) <= {r["slug"] for r in rows_now}), (4, 4, True),
                                 "each record's figures once for the whole build (the one whose orders were never sent is in no table)")
            finally:
                p.quote_made = real_made
            self.assertTrue(p.QUOTES_KEPT is None and bot.review_kept is None and bot.preopen_review() is not first and bot.preopen_review() == first)
            self.assertIsInstance(m.Bot.review_scope(bot), __import__("contextlib").nullcontext)
            running, bot.preopen = bot.preopen, None  # no controller (the mode off): every record is read off the file — none kept at all
            with bot.review_scope():
                self.assertIsNot(p.quote_of(rows_now[0]), p.quote_of(rows_now[0]))
                self.assertEqual(p.QUOTES_KEPT[1], {})
            bot.preopen = running
            # the daily summary: the day's line counts the quotes in and has them as a part; 累计 too; not under a filter
            rows = bot.summary_rows([], {})
            self.assertEqual([(x["label"], x["records"], x["pnl"], [q["name"] for q in x["parts"]]) for x in rows],
                             [("11-06 周五", 4, 5.1, ["开盘前预挂"]), ("累计", 4, 5.1, ["开盘前预挂"])])
            self.assertEqual((rows[0]["open"], rows[0]["open_cost"], rows[0]["resting"], rows[0]["markets"]), (1, 2.45, 1, 4))
            self.assertEqual(bot.summary_rows([], {}, {"card": "x"}), [])
            day = {"day": "2026-11-06", "label": "11-06 周五", **m.summary_of([]), "records": 2, "pnl": -1.0, "parts": [{"name": "挂单", **m.summary_of([])}]}
            merged = m.merge_preopen_summary([day, {"day": "", "label": "累计", **m.summary_of([]), "records": 2, "pnl": -1.0, "parts": []}], r["daily"])
            self.assertEqual([(x["records"], round(x["pnl"], 4), len(x["parts"])) for x in merged], [(6, 4.1, 2), (6, 4.1, 1)])
            bot.preopen = None
            self.assertEqual(bot.preopen_review()["total"]["records"], 4, "read off the file")
            won["market_id"], won["orders"]["up"]["index_set"] = "777", 2
            ctl.ledger.save(won)
            self.assertEqual((bot.preopen_filled(), p.fills_by_outcome([won], NOW)), ({("777", 2): 10.0}, {("777", 2): 10.0}), "1.131.2: the fills count with the mode off too")
            bot.config = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "STATE_DB": ":memory:"})
            self.assertIsNone(bot.preopen_review())
            self.assertEqual(p.review([], NOW)["total"]["records"], 0)
            await self.finish(ctl)

    def test_the_ledger_names_its_markets_and_tokens(self):
        """1.154.39: the ledger keeps, as its records are saved, which markets and outcome tokens they are of (market_names) —
        what LiveBot.live_known_markets walked every record's orders for each time the orphan sweep had an unrecorded order to
        judge: once a minute for as long as a manual order rested on the account, 0.3 s of the event loop each (11-24). The
        same answer as that walk through saves, a record stepping aside, deletions, the archive and records put in by hand."""
        def row(slug, mid, name, tokens=("", ""), **more):
            return {"slug": slug, "key": "BTC-15M", "name": name, "market_id": mid, "start_ms": NOW + 3600_000, "phase": "active", "price": "0.49", "shares": "10",
                    "orders": {side: {"state": "open", "filled": 0.0, "id": f"{slug}-{side}", "hash": f"0x{slug}{side}", "index_set": i + 1,
                                      **({"body": {"data": {"order": {"tokenId": token, "makerAmount": "1"}}}} if token else {})}
                               for i, (side, token) in enumerate(zip(("up", "down"), tokens))}, **more}

        def walked(led):  # (the walk as LiveBot.live_known_markets made it before)
            ids, tokens = {}, {}
            for pair in led.rows.values():
                mid = str(pair.get("market_id") or "")
                name = str(pair.get("name") or pair.get("slug") or mid)
                if mid:
                    ids[mid] = name
                for o in (pair.get("orders") or {}).values():
                    token = str((((o.get("body") or {}).get("data") or {}).get("order") or {}).get("tokenId") or "")
                    if token:
                        tokens[token] = name
            return ids, tokens
        w1, w2 = "btc-updown-15m-100", "btc-updown-15m-200"
        led = p.Ledger(":memory:")
        led.save(row(w1, "11", "BTC 10:00", ("t1", "t2")))
        led.save_many([row(w1 + "#1", "11", "BTC 10:00", ("t1", "t2")), row(w2, 12, "BTC 10:15", ("t3", "t4")),
                       row("btc-updown-15m-300", "", "no market", ("", "")), row("btc-updown-15m-400", "14", "", ("t7", ""))])
        self.assertEqual(led.market_names(), walked(led))
        self.assertEqual(led.market_names(), ({"11": "BTC 10:00", "12": "BTC 10:15", "14": "btc-updown-15m-400"},
                                              {"t1": "BTC 10:00", "t2": "BTC 10:00", "t3": "BTC 10:15", "t4": "BTC 10:15", "t7": "btc-updown-15m-400"}))
        self.assertEqual((led.markets, led.tokens["t1"]), ({"11": 2, "12": 1, "14": 1}, 2))
        led.save(led.rows[w1])  # saved again as it is, then with its id changed: counted once all the same
        led.rows[w1]["orders"]["up"]["id"] = "other"
        led.save(led.rows[w1])
        self.assertEqual((led.markets["11"], led.tokens["t1"], led.market_names()), (2, 2, walked(led)))
        led.rename(w1, w1 + "@0")  # the record steps aside (its window quoted again): still that market's
        self.assertEqual((led.markets["11"], led.market_names()), (2, walked(led)))
        led.delete(w1 + "#1")
        self.assertEqual((led.markets["11"], led.tokens["t2"], led.market_names()), (1, 1, walked(led)))
        led.delete(w1 + "@0")  # its last record: the market and its tokens are none of the ledger's any more
        self.assertEqual(led.market_names(), walked(led))
        self.assertEqual((sorted(led.market_names()[0]), sorted(led.market_names()[1])), (["12", "14"], ["t3", "t4", "t7"]))
        led.rows[w2]["name"] = "BTC 10:15 改名"  # a name changed and saved: the one it goes by now
        led.save(led.rows[w2])
        self.assertEqual((led.market_names()[0]["12"], led.market_names()[1]["t3"], led.markets["12"]), ("BTC 10:15 改名", "BTC 10:15 改名", 1))
        led.rows["by-hand"] = row("by-hand", "15", "put in by hand", ("t8", "t9"))  # (a test's doing: the indexes are made again)
        self.assertEqual(led.market_names(), walked(led))
        self.assertIn("15", led.market_names()[0])
        led.close()
        # the archive: a finished quote that filled nothing leaves the working records two days on — and its market with it (unless
        # another record of that market stays: the card's latest window always does)
        led = p.Ledger(":memory:")
        old = NOW - 5 * 86_400_000
        done = lambda slug, mid, start, tokens: {**row(slug, mid, slug, tokens), "start_ms": start, "phase": "closed", "accounted": True, "pnl": 0.0,
                                                  "orders": {s: {**o, "state": "final"} for s, o in row(slug, mid, slug, tokens)["orders"].items()}}
        led.save_many([done("btc-updown-15m-1", "21", old, ("a1", "a2")), done("btc-updown-15m-2", "22", old + 900_000, ("b1", "b2")),
                       done("btc-updown-15m-3", "23", NOW - 60_000, ("c1", "c2"))])
        self.assertEqual(led.archive_old(NOW), 2)
        self.assertEqual(led.market_names(), walked(led))
        self.assertEqual(led.market_names(), ({"23": "btc-updown-15m-3"}, {"c1": "btc-updown-15m-3", "c2": "btc-updown-15m-3"}))
        led.close()

    def test_each_level_against_a_coin_toss(self):
        """1.154.34 (the user: "复盘可以看预挂，哪个档位赚的多吗……比如 47 抽 3 个点，期望值盈利跟实际盈利对比"): the review's levels
        and the mode's whole, and a card's levels over a range (card_diag), with what their settled fills were worth at the
        order — 0.5 − the price a share, 47¢: 3¢ — beside what they made, the gap in the σ of the unhedged shares (a window
        one toss: its levels and re-quotes netted), and where it came from: the hedged windows, the one-sided ones' wins."""
        BJ = m.BEIJING
        at = lambda h: int(dt.datetime(2026, 11, 5, h, tzinfo=BJ).timestamp() * 1000)

        def rec(start, up, down, result, level=0, price=0.47, shares=30, accounted=True, attempt=0):
            payout = {"up": 1.0, "down": 0.0}[result] if result else None
            pnl = (payout - price) * up + ((1 - payout) - price) * down if payout is not None and accounted else 0.0
            return {"slug": f"btc-updown-15m-{start // 1000}" + (f"#{level}" if level else "") + (f"@{attempt}" if attempt else ""), "key": "BTC-15M",
                    "name": "BTC 15分涨跌 x", "start_ms": start, "phase": "closed" if accounted else "closing", "accounted": accounted,
                    "result": result if accounted else "", "pnl": pnl, "price": str(price), "shares": str(shares), "level": level, "attempt": attempt,
                    "orders": {side: {"state": "final", "filled": float(f)} for side, f in (("up", up), ("down", down))}}
        rows = [rec(at(10), 30, 0, "up"), rec(at(10), 0, 80, "up", level=1, price=0.43, shares=80),  # A: Up won at 47¢, Down lost at 43¢
                rec(at(11), 0, 30, "up"),            # B: one side, lost
                rec(at(12), 30, 30, "down"),         # C: both sides — hedged, 1 − 2 × 47¢ a pair
                rec(at(13), 10, 0, "", accounted=False),  # D: filled, no result yet — worth nothing to the comparison
                rec(at(14), 0, 0, "", attempt=1)]    # E: withdrawn unfilled
        r = p.review(rows, NOW)
        top, deep = r["levels"]
        self.assertEqual((top["name"], top["price"], top["size"], deep["name"]), ("47¢×30", 0.47, 30.0, "43¢×80"))
        self.assertEqual((top["settled"], top["settled_shares"], top["ev"], top["pnl"]), (3, 120.0, 3.6, 3.6))  # 3¢ a share of 120
        self.assertEqual((top["sigma"], top["z"]), (round(450 ** 0.5, 4), 0.0))  # A and B 30 unhedged shares each; C none
        self.assertEqual((top["both"], top["one"]), ({"windows": 1, "pnl": 1.8}, {"windows": 2, "won": 1, "pnl": 1.8}))
        self.assertEqual((deep["settled_shares"], deep["ev"], deep["pnl"], deep["sigma"], deep["z"]), (80.0, 5.6, -34.4, 40.0, -1.0))
        self.assertEqual((deep["both"]["windows"], deep["one"]), (0, {"windows": 1, "won": 0, "pnl": -34.4}))
        # the whole: A is one toss (Up 30 at one level, Down 80 at the other: 50 unhedged) — and a window filled on both sides
        e = r["ev"]
        self.assertEqual((e["settled_shares"], e["ev"], e["sigma"], e["z"]), (200.0, 9.2, round(850 ** 0.5, 4), round((-30.8 - 9.2) / 850 ** 0.5, 2)))
        self.assertEqual((e["both"], e["one"]), ({"windows": 2, "pnl": -16.7}, {"windows": 1, "won": 0, "pnl": -14.1}))
        self.assertAlmostEqual(r["total"]["pnl"], -30.8, places=6)
        # a card's levels over a range: by price, the same figures
        d = p.cards_between(rows, "2026-11-05", "2026-11-05")[0]["diag"]
        self.assertEqual([(x["name"], x["settled_shares"], x["ev"], x["pnl"], x["z"]) for x in d["levels"]], [("47¢", 120.0, 3.6, 3.6, 0.0), ("43¢", 80.0, 5.6, -34.4, -1.0)])
        self.assertEqual(p.ev_line([]), {"settled_shares": 0.0, "ev": 0.0, "sigma": 0.0, "z": None, "both": {"windows": 0, "pnl": 0.0}, "one": {"windows": 0, "won": 0, "pnl": 0.0}})
        # 1.154.35 ("这个也可以用时间选择"): over a range of days (levels_between, under 按市场 and on its days) — only the windows
        # starting on them; the archived records (finished quotes that filled nothing) counted in by their kept counts, a level's
        # records and windows on each date, never a fill
        at4 = lambda h: int(dt.datetime(2026, 11, 4, h, tzinfo=BJ).timestamp() * 1000)
        older = [rec(at4(h), 0, 0, "") for h in range(3)] + [rec(at4(5), 30, 0, "down")]  # 11-04: three 47¢×30 quotes that filled nothing, one that lost
        day5 = p.levels_between(rows + older, "2026-11-05", "2026-11-05")
        self.assertEqual((day5["levels"], day5["ev"], day5["pnl"], day5["settled"]), (r["levels"], r["ev"], r["total"]["pnl"], r["total"]["settled"]))
        wide = p.levels_between(rows + older, "2026-11-04", "2026-11-05")
        self.assertEqual([(x["name"], x["records"], x["windows"], x["withdrawn"], x["settled_shares"], x["ev"], x["pnl"]) for x in wide["levels"]],
                         [("47¢×30", 9, 9, 4, 150.0, 4.5, -10.5), ("43¢×80", 1, 1, 0, 80.0, 5.6, -34.4)])
        self.assertEqual(p.levels_between(rows, "2026-11-06", "2026-11-30"), {"levels": [], "ev": p.ev_line([]), "pnl": 0, "settled": 0})
        with tempfile.TemporaryDirectory() as folder:
            led = p.Ledger(str(Path(folder) / "x.preopen.sqlite3"))
            for row in rows + older:
                led.save(row)
            later = int(dt.datetime(2026, 11, 9, tzinfo=BJ).timestamp() * 1000)
            self.assertEqual(led.archive_old(later), 3)  # the three of 11-04 (E, the card's latest window, stays whatever its age)
            working = list(led.rows.values())
            share = p.archive_share(led, working)
            self.assertTrue(share.fits(later))
            self.assertEqual(p.levels_between(working, "2026-11-04", "2026-11-05", share), wide)
            self.assertEqual(p.levels_between(working, "2026-11-04", "2026-11-04", share)["levels"][0]["records"], 4)
            led.close()

    async def test_review_findings_only_an_operators_stop_withdraws_and_attempts_are_not_reused(self):
        """1.128.3: the account not verified yet at a start (or the stop itself) withdraws nothing — an operator's stop does;
        the attempt after a superseded record is not reused when the re-quote left no record; Predict's deferred refusal
        is not quoted again; a record still open past the give-up is final as known; the review derives an old settled
        record's result from its one filled side."""
        ctl, api, clock = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        ctl.owner.live.ready = False
        self.assertEqual((ctl.hold(), ctl.withdrawing(), ctl.due(NOW)), ("账户未就绪", "", []))
        ctl.owner.live.ready = True
        ctl.stop.set()
        self.assertEqual((ctl.blocked(), ctl.withdrawing(), ctl.due(NOW)), ("进程或预挂模式停止中", "", []))
        ctl.stop.clear()
        ctl.owner.live_paused = lambda: "已暂停"
        self.assertEqual((ctl.withdrawing(), [x["slug"] for x in ctl.due(NOW)]), ("已暂停", [c["slug"]]))
        ctl.owner.live_paused = lambda: ""
        # the attempt after the records that stepped aside, the latest having left no record (its first request rate-limited)
        gone = {**pair, "slug": c["slug"] + "@0", "attempt": 0, "phase": "closed", "accounted": True,
                "orders": {s: {**o, "state": "final", "filled": 0.0} for s, o in pair["orders"].items()}}
        ctl.ledger.delete(c["slug"]); ctl.ledger.save(gone)
        await ctl.discover()
        queued = ctl.queue.get_nowait()[2]
        self.assertEqual(queued["attempt"], 1, "not attempt 0 again: that salt is the withdrawn orders'")
        ctl.queued.discard(c["slug"])
        ctl.ledger.save({**gone, "slug": c["slug"] + "@2", "attempt": 2})
        ctl.full_scan_ms = 0  # a full scan each time: the fixture lists this window alone (the windows before it unlisted would stop a quick scan)
        await ctl.discover()
        queued = ctl.queue.get_nowait()[2]  # 1.131.6: two failures — the attempt that left no record sent nothing, no failure of the window
        self.assertEqual(queued["attempt"], 3, "quoted again, under a salt of its own")
        ctl.queued.discard(c["slug"])
        ctl.ledger.save({**gone, "slug": c["slug"] + "@3", "attempt": 3})
        ctl.full_scan_ms = 0
        await ctl.discover()
        self.assertEqual(ctl.queue.qsize(), 0, "three failures: not quoted again")
        self.assertIn("不再报价：1 档失败满 3 次", ctl.status())
        for key in (c["slug"] + "@0", c["slug"] + "@2", c["slug"] + "@3"):
            ctl.ledger.delete(key)
        # 1.131.6: withdrawals that were not the window's doing — a settings change, the mode stopped, LIVE paused, the stock
        # guard — do not use its quotes up: five of them, and it is quoted again
        for n, reason in enumerate(("设置不同，撤掉后重新报价", "预挂模式停止，撤销预挂单", "到时撤单或真实交易暂停", "SK 海力士 现价 129,000.00 距涨停 130,000.00 不足 2%")):
            ctl.ledger.save({**gone, "slug": f"{c['slug']}@{n}", "attempt": n, "reason": reason})
        ctl.ledger.save({**gone, "slug": f"{c['slug']}@4", "attempt": 4, "reason": "test", "free": True})
        self.assertEqual(p.window_strikes(ctl.ledger.rows.values()), {})
        ctl.full_scan_ms = 0
        await ctl.discover()
        queued = ctl.queue.get_nowait()[2]
        self.assertEqual(queued["attempt"], 5)
        ctl.queued.discard(c["slug"])
        self.assertNotIn("不再报价", ctl.status())
        for n in range(5):
            ctl.ledger.delete(f"{c['slug']}@{n}")
        # Predict's deferred refusal (the record ends REJECTED with nothing filled): withdrawn, and not quoted again
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        up = pair["orders"]["up"]
        api.rows[up["hash"]].update(status="REJECTED")
        clock.ms += 10_000; ctl.account_at = clock.ms; ctl.listed_ids = set()
        await ctl.monitor_pair(pair)
        await self.settle_tasks(ctl)
        # Audit PRE-LATE-FILL: the refusal is noted and the other side withdrawn there and then; the two records end their orders
        # on a read LATE_FILL_MS on, not on this one (this pinned the pair accounted here) — never quoted again either way
        self.assertEqual((up["state"], up.get("refused"), pair.get("accounted"), ctl.supersedable(pair), pair["phase"]), ("removed", True, None, False, "closed"))
        await self.read_finals(ctl, clock, pair)
        self.assertEqual((up["state"], up.get("refused"), pair["accounted"], ctl.supersedable(pair)), ("final", True, True, False))
        # 1.154.65: why, as its record said it; and quoted again REFUSED_RETRY_MS after the refusal while the window is far enough
        # ahead (a refused post-only bid fills nothing: the book may have moved). It was never quoted again
        self.assertEqual((up.get("refused_why"), pair["refused_ms"]), ("REJECTED", NOW + 10_000))
        self.assertEqual(p.refusal_words(pair), "挂上后被 Predict 作废（REJECTED，多半是 post-only 会吃单）")
        self.assertIn("\n被拒后等重挂：1 档——按原因：挂上后被 Predict 作废（REJECTED，多半是 post-only 会吃单） 1 档；被拒 10 分钟后再挂一次（每档最多 3 次）", ctl.status())
        self.assertNotIn("不再报价", ctl.status())
        unplaced_with = lambda refused: (up.__setitem__("refused", refused), ctl.unplaced()[1])[1]
        self.assertEqual(unplaced_with(True), unplaced_with(False), "to be quoted again: among the levels still to place, as any other")
        up["refused"] = True
        refused_at = clock.ms
        # ...not within REFUSED_RETRY_AHEAD_MS of the start: refused for good, said so — and not counted among the levels to place
        clock.ms = pair["start_ms"] - p.REFUSED_RETRY_AHEAD_MS + 1
        self.assertFalse(ctl.supersedable(pair))
        self.assertIn("\n不再报价：1 档被 Predict 拒绝——按原因：挂上后被 Predict 作废（REJECTED，多半是 post-only 会吃单） 1 档；如 ", ctl.status())
        self.assertIn("（开场前 30 分钟内的、或这一档已经试满 3 次的不再重挂）", ctl.status())
        self.assertEqual(unplaced_with(True), unplaced_with(False) - 1, "refused for good: not among the levels still to place")
        up["refused"] = True
        # the window far enough ahead and the wait over: quoted again
        clock.ms = pair["refused_ms"] + p.REFUSED_RETRY_MS
        self.assertTrue(ctl.supersedable(pair))
        ctl.full_scan_ms = 0
        await ctl.discover()
        queued = ctl.queue.get_nowait()[2]
        self.assertEqual((queued["slug"], queued["attempt"]), (c["slug"], 1), "quoted again under a salt of its own")
        ctl.queued.discard(c["slug"])
        clock.ms = refused_at
        # an outright refusal: Predict's answer, in words where it is a known kind, as it came where it is not
        self.assertEqual(p.refusal_kind("下单被拒绝：HTTP 400: post-only order would cross"), "post-only 会吃单（挂价碰到了对面已有的卖价）")
        for text, words in (("HTTP 400: insufficient balance", "余额不足"), ("HTTP 400: allowance too low", "授权额度不足"),
                            ("HTTP 400: expiration must be at least 120s", "到期时间不合规"), ("HTTP 400: market is closed", "市场现在不接受下单"),
                            ("HTTP 400: invalid tick size for price", "价格不合规"), ("HTTP 400: bad signature", "订单签名或字段被拒"),
                            ("HTTP 400: something new", "HTTP 400: something new"), ("", "没说原因")):
            self.assertEqual(p.refusal_kind(text), words, text)
        rejected = {**pair, "orders": {"up": {"state": "rejected", "error": "下单被拒绝：HTTP 400: post-only order would cross"},
                                       "down": {"state": "rejected", "error": "HTTP 429", "limited": True}}}
        self.assertEqual((p.refused_pair(rejected), p.refusal_words(rejected)), (True, "post-only 会吃单（挂价碰到了对面已有的卖价）"))
        self.assertFalse(p.refused_pair({**pair, "orders": {"up": {"state": "rejected", "limited": True}}}), "the rate limiter's: no refusal of Predict's")
        # a record Predict still shows open past the give-up: final as known
        stuck = {**pair, "slug": "btc-updown-15m-stuck", "phase": "closing", "accounted": False,
                 "orders": {"up": {"hash": "0xstuck", "id": "77", "state": "removed", "filled": 0.0}}}
        api.rows["0xstuck"] = {"id": "77", "order": {"hash": "0xstuck"}, "amount": str(L.to_wei(50)), "amountFilled": "0", "status": "OPEN"}
        await ctl.read_final(stuck["orders"]["up"], stuck)
        self.assertEqual(stuck["orders"]["up"]["state"], "removed", "before the give-up: still waited for")
        clock.ms = stuck["start_ms"] + p.GIVE_UP_MS
        await ctl.read_final(stuck["orders"]["up"], stuck)
        self.assertEqual(stuck["orders"]["up"]["state"], "final")
        self.assertIn("签名早已过期", stuck["orders"]["up"]["note"])
        # the review: an old settled record without its result, from its one filled side
        old = {"slug": "btc-updown-15m-old", "key": "BTC-15M", "name": "BTC 15分涨跌 10:00–10:15", "start_ms": NOW + 3600_000, "created_ms": NOW,
               "price": "0.49", "shares": "10", "phase": "closed", "accounted": True, "pnl": -4.9,
               "orders": {"up": {"state": "final", "filled": 0.0}, "down": {"state": "final", "filled": 10.0}}}
        self.assertEqual(p.quote_of(old)["result"], "up")  # the down side lost: up won
        self.assertEqual(p.quote_of({**old, "pnl": 5.1})["result"], "down")
        self.assertEqual(p.quote_of({**old, "pnl": 0.1})["result"], "split")
        self.assertEqual(p.quote_of({**old, "orders": {"up": {"state": "final", "filled": 4.0}, "down": {"state": "final", "filled": 10.0}}})["result"], "", "both sides: ambiguous")
        self.assertEqual(p.review([old], NOW)["sides"][1]["pnl"], -4.9)
        await self.finish(ctl)

    async def test_price_levels_quote_each_level_as_its_own_record(self):
        """1.129.0: PREOPEN_LEVELS adds deeper bids under the first level; each level is a record (slug#cents) with its own
        price and size, placed only within PREOPEN_DEEP_AHEAD_MINUTES of the start (the first level keeps the wide horizon);
        one read a window; the takeover goes by each record's own level; the review counts by level."""
        on = lambda **e: p.Settings.from_config(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on", "STATE_DB": ":memory:", **e}))
        s = on(PREOPEN_LEVELS="48x20, 47×40", PREOPEN_DEEP_AHEAD_MINUTES="120")
        self.assertEqual((s.levels, s.deep_ahead_minutes, s.price, s.shares), (((m.D("0.49"), m.D("10")), (m.D("0.48"), m.D("20")), (m.D("0.47"), m.D("40"))), 120, m.D("0.49"), m.D("10")))
        self.assertEqual((on().levels, on().deep_ahead_minutes, on(PREOPEN_LEVELS="48x20").deep_ahead_minutes), (((m.D("0.49"), m.D("10")),), 1440, 1440))
        self.assertEqual(len(on(PREOPEN_LEVELS="48x10,47x20,46x40,43x40,40x40").levels), 6, "1.139.1: up to eight levels")
        for bad in ("49x10", "48x20,48x10", "abc", "48x20,47x20,46x20,45x20,44x20,43x20,42x20,41x20", "48x1", "60x5"):
            with self.assertRaises(ValueError, msg=bad):
                on(PREOPEN_LEVELS=bad)
        with self.assertRaises(ValueError):
            on(PREOPEN_LEVELS="48x20", PREOPEN_DEEP_AHEAD_MINUTES="2000")
        self.assertTrue({"PREOPEN_LEVELS", "PREOPEN_DEEP_AHEAD_MINUTES"} <= m.CONTROL_KEY_SET and m.CONTROL_FORMS["PREOPEN_LEVELS"]["kind"] == "text")
        self.assertIn("48.0¢×20 份、47.0¢×40 份（第 2 档起只挂 120 分钟内开始的场次）", p.levels_text(s))
        with tempfile.TemporaryDirectory() as folder:
            db = {"STATE_DB": str(Path(folder) / "s.sqlite3")}
            rows = [market(at=NOW + 15 * 60_000, mid=1), market(at=NOW + 45 * 60_000, mid=2)]
            ctl, api, clock = controller(rows, PREOPEN_LEVELS="48x20,47x40", PREOPEN_DEEP_AHEAD_MINUTES="30", PREOPEN_MARKETS="btc", PREOPEN_SHARES="10", **db)
            await ctl.discover()
            keys = [ctl.queue.get_nowait()[2] for _ in range(ctl.queue.qsize())]
            near, far = rows[0]["categorySlug"], rows[1]["categorySlug"]
            self.assertEqual([(c["slug"], c["level"], c["price"], c["shares"]) for c in keys],
                             [(near, 0, "0.49", "10"), (near + "#1", 1, "0.48", "20"), (near + "#2", 2, "0.47", "40"), (far, 0, "0.49", "10")])
            self.assertEqual(api.calls.count("/categories/" + near), 1, "one read a window, whatever the levels")
            for c in keys:
                self.assertEqual(await ctl.place_pair(c), "")
            self.assertEqual(len(api.sent), 8)
            deep = ctl.ledger.rows[near + "#1"]
            self.assertEqual((deep["window"], deep["level"], deep["price"], deep["shares"], p.window_of(deep), p.record_key(deep)), (near, 1, "0.48", "20", near, near + "#1"))
            self.assertEqual(api.sent[2]["data"]["order"]["makerAmount"], str(L.to_wei("9.6")))  # 48¢ × 20
            self.assertEqual(len({o["hash"] for r in ctl.ledger.rows.values() for o in r["orders"].values()}), 8, "a salt a level")
            self.assertAlmostEqual(ctl.risk(), 2 * (4.9 + 9.6 + 18.8) + 9.8)
            self.assertIn("已记录 2 场、4 条（4 条挂着", ctl.status())
            self.assertIn("两边各 49.0¢×10 份、48.0¢×20 份、47.0¢×40 份（第 2 档起只挂 30 分钟内开始的场次）", ctl.status())
            api.markets["1"]["tradingStatus"] = "OPEN"
            await ctl.refresh_active()
            self.assertTrue(all(ctl.ledger.rows[k].get("open_seen_ms") for k in (near, near + "#1", near + "#2")), "matching on: noted on every level")
            clock.ms += 20 * 60_000  # the far window comes within the deeper levels' horizon
            ctl.account_at = clock.ms
            await ctl.discover()
            self.assertEqual(sorted(ctl.queue.get_nowait()[2]["slug"] for _ in range(ctl.queue.qsize())), [far + "#1", far + "#2"])
            r = p.review(list(ctl.ledger.rows.values()), clock.ms)
            self.assertEqual([(x["name"], x["records"]) for x in r["levels"]], [("49¢×10", 2), ("48¢×20", 1), ("47¢×40", 1)])
            ctl.keep_on_stop = True
            await ctl.on_stop()
            ctl.ledger.close()
            again, _, _ = controller(rows, PREOPEN_LEVELS="48x20", PREOPEN_MARKETS="btc", PREOPEN_SHARES="10", **db)
            again.api = again.cancel_api = api; api.ctl = again
            again.adopt()
            self.assertEqual((again.resumed, again.ledger.rows[near + "#2"]["phase"], again.ledger.rows[near + "#1"]["phase"]), (3, "closing", "active"), "the level dropped is withdrawn")
            moved, _, _ = controller(rows, PREOPEN_PRICE_CENTS="50", PREOPEN_LEVELS="49x10", PREOPEN_MARKETS="btc", PREOPEN_SHARES="10", **db)
            self.assertFalse(moved.resumable(again.ledger.rows[near]), "1.131.2: the first level moved to 50x10 — the 49x10 record is not it, whatever level now says 49x10")
            moved.ledger.close()
            await self.finish(again)

    async def test_a_stock_card_near_its_limit_or_halted_is_not_quoted(self):
        """1.130.0: within PREOPEN_LIMIT_GAP_PCT of the day's limit (the previous close ± the exchange's limit) or halted (the
        quote's own time still HALT_MS in session), a stock card's resting pairs are withdrawn and its windows passed by;
        quoted again once the hold lifts. Judged on the card's lead quote; none, or a quiet feed: nothing judged."""
        sk = next(r for r in m.WINDOW_UPDOWN_MARKETS if r.key == "SKHYNIX-15M")
        ctl, api, clock = controller([market("skhynix"), market("skhynix", at=NOW + 30 * 60_000, mid=9)])
        quote = lambda last, prev="100000", quoted=None, fetched=None: types.SimpleNamespace(last=m.D(last), prev_close=m.D(prev) if prev else None,
                                                                                            quoted_ms=clock.ms if quoted is None else quoted, fetched_ms=clock.ms if fetched is None else fetched)
        ctl.owner.updowns = {"SKHYNIX-15M": types.SimpleNamespace(lead=None)}
        self.assertEqual(ctl.card_hold(sk), "", "no exchange quote: nothing judged")
        await ctl.discover()
        far_c, c = sorted((ctl.queue.get_nowait()[2] for _ in range(ctl.queue.qsize())), key=lambda x: x["start_ms"])  # 09:30 and 10:00
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        ctl.owner.updowns["SKHYNIX-15M"].lead = quote("129000")  # KRX ±30%: the limit up is 130,000, 2% under it is 127,400
        self.assertEqual(ctl.card_hold(sk), "SK 海力士 现价 129,000.00 距涨停 130,000.00 不足 2%")
        await ctl.check_cards(clock.ms)
        self.assertEqual((set(api.removed), pair["phase"], ctl.card_holds["SKHYNIX-15M"].startswith("SK 海力士 现价")), ({"1", "2"}, "closed", True))
        self.assertIn("股票卡暂停：SK 海力士 现价", ctl.status())
        self.assertEqual((await ctl.place_pair(far_c), len(api.sent)), ("held", 2))
        api.calls.clear()
        await ctl.discover()
        self.assertFalse([x for x in api.calls if "skhynix" in x], "held: its windows are not even asked for")
        self.assertEqual(ctl.card_hold(sk) and ctl.card_hold(sk)[:4], "SK 海")
        ctl.owner.updowns["SKHYNIX-15M"].lead = quote("71000")  # the limit down is 70,000: 2% over it is 71,400
        self.assertIn("距跌停 70,000.00", ctl.card_hold(sk))
        ctl.owner.updowns["SKHYNIX-15M"].lead = quote("110000", quoted=clock.ms - 4 * 60_000)  # in session (10:00 KST), the quote's time 4 minutes still
        self.assertIn("成交时间 4 分钟没动", ctl.card_hold(sk))
        was = clock.ms
        clock.ms = NOW + 5 * 3600_000 + 23 * 60_000  # 15:23 KST: the closing auction prints nothing — no halt (1.131.2)
        ctl.owner.updowns["SKHYNIX-15M"].lead = quote("110000", quoted=clock.ms - 4 * 60_000)
        self.assertEqual(ctl.card_hold(sk), "")
        clock.ms = NOW - 56 * 60_000  # 09:04 KST (NOW is 10:00 KST): the first minutes after the open
        ctl.owner.updowns["SKHYNIX-15M"].lead = quote("110000", quoted=clock.ms - 4 * 60_000)
        self.assertEqual(ctl.card_hold(sk), "")
        clock.ms = was
        ctl.owner.updowns["SKHYNIX-15M"].lead = quote("110000", quoted=clock.ms - 4 * 60_000, fetched=clock.ms - 120_000)
        self.assertEqual(ctl.card_hold(sk), "", "the feed itself quiet: nothing judged")
        unitree = next(r for r in m.WINDOW_UPDOWN_MARKETS if r.key == "UNITREE-15M")
        ctl.owner.updowns["UNITREE-15M"] = types.SimpleNamespace(lead=quote("90", "100", quoted=clock.ms - 4 * 60_000))
        self.assertEqual(ctl.card_hold(unitree), "", "09:00 Beijing: 宇树 not in session, a still quote is no halt")
        clock.ms = NOW + 4 * 3600_000 + 30_000  # 13:00:30: the lunch break does not count, nor the first minutes after it
        ctl.owner.updowns["UNITREE-15M"].lead = quote("90", "100", quoted=NOW + 2 * 3600_000 + 29 * 60_000)
        self.assertEqual(ctl.card_hold(unitree), "")
        clock.ms = NOW + 4 * 3600_000 + 10 * 60_000  # 13:10, the last print 11:29: 11 minutes of trading without one (the break not counted) — a halt
        ctl.owner.updowns["UNITREE-15M"].lead = quote("90", "100", quoted=NOW + 2 * 3600_000 + 29 * 60_000)
        self.assertIn("成交时间 11 分钟没动", ctl.card_hold(unitree))
        clock.ms = NOW
        ctl.owner.updowns["UNITREE-15M"].lead = quote("119", "100")
        self.assertIn("距涨停 120.00", ctl.card_hold(unitree))
        ctl.settings = dataclasses.replace(ctl.settings, limit_gap_pct=0.0)
        self.assertEqual(ctl.card_hold(unitree), "", "PREOPEN_LIMIT_GAP_PCT=0: off")
        ctl.settings = dataclasses.replace(ctl.settings, limit_gap_pct=2.0)
        # the hold lifts: the withdrawn pair, accounted, is quoted again — accounted by the read of its records LATE_FILL_MS after
        # the removal (Audit PRE-LATE-FILL), not by one right after it (this pinned it accounted by then)
        await self.settle_tasks(ctl)
        self.assertEqual((pair.get("accounted"), ctl.supersedable(pair)), (None, False))
        await self.read_finals(ctl, clock, pair)
        ctl.owner.updowns["SKHYNIX-15M"].lead = quote("110000")
        ctl.owner.updowns["UNITREE-15M"].lead = quote("110", "100")
        clock.ms += p.CARD_CHECK_MS
        await ctl.check_cards(clock.ms)
        self.assertEqual((ctl.card_holds, pair["accounted"], ctl.supersedable(pair)), ({}, True, True))
        ctl.queued.clear()
        clock.ms += p.FULL_SCAN_MS  # a full scan: the window sits behind unlisted quarters
        await ctl.discover()
        self.assertIn(c["slug"], [ctl.queue.get_nowait()[2]["slug"] for _ in range(ctl.queue.qsize())])
        self.assertTrue({"PREOPEN_LIMIT_GAP_PCT"} <= m.CONTROL_KEY_SET and sk.limit_pct == 0.30 and unitree.limit_pct == 0.20)
        await self.finish(ctl)

    async def test_restart_preserves_hash_and_pause(self):
        with tempfile.TemporaryDirectory() as folder:
            path = str(Path(folder) / "state.sqlite")
            ledger = p.Ledger(path)
            row = {"slug": "s", "phase": "closing", "orders": {"up": {"hash": "h", "state": "unknown"}}}
            ledger.save(row); ledger.pause(True); ledger.close()
            again = p.Ledger(path)
            self.assertEqual(again.rows["s"]["orders"]["up"]["hash"], "h")
            self.assertTrue(again.pause())
            again.close()

    async def test_final_resolution_rereads_late_fills_and_actual_cost(self):
        ctl, api, clock = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        for row in api.rows.values():
            row["amountFilled"] = row["amount"]  # both filled before the cancel
        await ctl.cancel_many([pair], "cancel")
        await self.settle_tasks(ctl)
        self.assertEqual((pair["phase"], pair.get("accounted"), ctl.risk()), ("closed", None, 49.))  # filled: the result decides, reserved until then
        clock.ms = c["start_ms"] + 20 * 60_000
        api.markets["123"]["outcomes"][0]["status"] = "WON"  # Down
        api.markets["123"]["status"] = "RESOLVED"  # (1.154.77: a result is booked on a market Predict calls settled)
        soon = []
        ctl.owner.live_redeem_soon = lambda market_id="": soon.append(str(market_id))
        await ctl.settle_pair(pair)
        self.assertTrue(pair["accounted"])
        self.assertAlmostEqual(pair["pnl"], 1.)
        # 1.131.3: a fill on the winning side (LIVE_AUTO_REDEEM on by default) — redeemed soon; 1.150.0 (RDM-SOON-NO-TARGET): the
        # market named, so the round it brings reads it first
        self.assertEqual(soon, ["123"])
        self.assertEqual(ctl.risk(), 0.)
        await self.finish(ctl)

    async def test_the_entry_cut_off_is_judged_again_as_the_orders_leave(self):
        """1.154.26 (the audit of 11-18, 17): PREOPEN_ENTRY_SECONDS was judged when the window was taken as a candidate, not after
        the waits on its orders' way (the allowance's read, the sign-in, the account lock, a token of the key's pace): a
        window looked at 181 s before its start went out at 178 — or later still, up to the cancel time's own gate."""
        start = NOW + 3600_000  # (market()'s window)
        for lead_s, sent in ((181, 0), (190, 2)):
            ctl, api, clock = controller()
            self.assertEqual(ctl.settings.entry_seconds, 180)
            clock.ms = ctl.account_at = start - lead_s * 1000
            c = p.candidate(market(), clock.ms, ctl.owner.config, ctl.settings)
            self.assertIsNotNone(c, p.candidate_problem(market(), clock.ms, ctl.owner.config, ctl.settings))

            async def waits(at=clock.ms):  # what stands between the candidate's judgement and its bytes: three seconds of it
                clock.ms = at + 3_000
            api.before = waits
            await ctl.place_pair(c)
            self.assertEqual(len(api.sent), sent, lead_s)
            if sent:
                self.assertEqual({o["state"] for o in ctl.ledger.rows[c["slug"]]["orders"].values()}, {"open"})
            else:
                self.assertEqual((c["slug"] in ctl.ledger.rows, ctl.error), (False, "已过新挂的截止时间（等授权、登录或排队时过了），不再发送"))
            await self.finish(ctl)

    async def test_safe_api_validates_error_envelope_and_checks_after_gate(self):
        api = p.safe_api("https://offline.invalid/v1", "key", None)
        api.jwt = "offline"
        sent = []
        async def transport(*args):
            sent.append(args)
            return b'{"success":false,"error":"temporary"}'
        with patch.object(m, "predict_blocking", transport):
            with self.assertRaises(m.RemoteError) as caught: await api.orders()
        self.assertIn("（GET /orders）", str(caught.exception))  # the request named: /preopen's 最近问题 says which call it was
        self.assertEqual(len(sent), 1)
        self.assertTrue(api.orders_complete)  # no caller receives a successful empty list
        answers = [b'{"success":true,"removed":["1"],"noop":[]}', b'{"removed":[],"noop":["1"]}']  # the cancel's answer: no "data" key (1.127.1)
        async def cancel_transport(*args): return answers.pop(0)
        with patch.object(m, "predict_blocking", cancel_transport):
            self.assertEqual(await api.remove_orders(["1"]), {"removed": ["1"], "noop": []})
            self.assertEqual(await api.remove_orders(["1"]), {"removed": [], "noop": ["1"]})
        block = {"yes": False}
        api = p.safe_api("https://offline.invalid/v1", "key", None, lambda body: "paused" if block["yes"] else "")
        api.jwt = "offline"; api.gate = asyncio.Semaphore(0)
        sent.clear()
        with patch.object(m, "predict_blocking", transport):
            task = asyncio.create_task(api.create_order({"data": {}}))
            await asyncio.sleep(0)
            block["yes"] = True
            api.gate.release()
            with self.assertRaises(p.NotSent): await task
        self.assertEqual(sent, [])

    # --- 1.141.0: withdrawing a book of thousands -----------------------------------------------------------------------------
    async def book_of(self, ctl, api, n, phase="active"):
        """``n`` pairs like a placed one, each with two open orders of its own on the fake book (closing: as a settings change leaves them)."""
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        proto = copy.deepcopy(ctl.ledger.rows[c["slug"]])
        ctl.ledger.delete(c["slug"])
        api.rows.clear()
        pairs = []
        for k in range(n):
            pair = copy.deepcopy(proto)
            pair.update(slug=f"{proto['slug']}#{k}", phase=phase, **({"retry_ms": 0, "reason": "设置不同，撤掉后重新报价", "free": True} if phase == "closing" else {}))
            for side, order in pair["orders"].items():
                oid, digest = f"{k}{side[0]}", f"0x{k:04d}{side}"
                order.update(id=oid, hash=digest, state="open", filled=0.0)
                order.pop("api_hash", None)
                api.rows[digest] = {"id": oid, "order": {"hash": digest}, "amount": "50", "amountFilled": "0", "status": "OPEN"}
            ctl.ledger.save(pair)
            pairs.append(pair)
        api.removed.clear()
        return pairs

    def count_removals(self, api):
        sizes = []
        real = api.remove_orders

        async def counted(ids):
            sizes.append(len(ids))
            return await real(ids)
        api.remove_orders = counted
        return sizes

    async def test_bulk_removal_grows_its_batch_and_measures_predicts_ceiling(self):
        ids = [str(i) for i in range(1000)]
        api, limit = BulkApi(), L.RemoveLimit()
        r = await L.remove_in_batches(api, ids, limit, workers=4)
        self.assertEqual((sorted(r["removed"], key=int), r["failed"], r["left"], r["limited"]), (ids, [], [], 0))
        self.assertLessEqual(len(api.calls), 16, f"doubling from 20 while answered whole (four at a time: a round a size), not fifty requests of twenty: {[len(c) for c in api.calls]}")
        self.assertTrue(limit.ok >= 160 and limit.cap == 0 and api.most <= 4 and limit.changed, (limit.as_dict(), api.most))
        # Predict refuses more than 50 ids a request: the 80 that failed went through as two 40s — the size, measured
        api, limit = BulkApi(cap=50), L.RemoveLimit()
        r = await L.remove_in_batches(api, ids[:600], limit, workers=4)
        self.assertEqual((len(r["removed"]), r["failed"], (limit.ok, limit.cap, limit.soft, limit.size())), (600, [], (40, 0, 80, 40)))
        limit.soft_at = 0  # an hour on, 80 is tried again — and a second withdrawal finding the same makes it the ceiling (1.141.1)
        r = await L.remove_in_batches(BulkApi(cap=50), ids[:600], limit, workers=4)
        self.assertEqual((len(r["removed"]), limit.cap, limit.size()), (600, 80, 40))
        self.assertIn("一次 80 张太多", limit.text())
        # Predict answers only the first 30 of a request: the tail left out goes again, and its going through measures the 30
        api, limit = BulkApi(take=30), L.RemoveLimit()
        r = await L.remove_in_batches(api, ids[:300], limit, workers=1)
        self.assertEqual((len(r["removed"]), r["unanswered"], (limit.ok, limit.cap, limit.soft, limit.size())), (300, [], (30, 0, 40, 30)))
        self.assertEqual([len(c) for c in api.calls[:4]], [20, 40, 10, 30])
        # an id Predict will not take is found alone and given up — the content, not the size: no ceiling from it
        api, limit = BulkApi(bad={"7"}), L.RemoveLimit()
        r = await L.remove_in_batches(api, ids[:40], limit, workers=1)
        self.assertEqual((r["failed"], len(r["removed"]), limit.cap, limit.soft, r["halted"]), (["7"], 39, 0, 0, ""))
        self.assertIn("HTTP 400", r["errors"][-1])
        # a big batch that times out is split; both halves through: that size is the ceiling
        api, limit = BulkApi(timeout_over=50), L.RemoveLimit(ok=40)
        r = await L.remove_in_batches(api, ids[:80], limit, workers=1)
        self.assertEqual((len(r["removed"]), [len(c) for c in api.calls], limit.cap, limit.soft), (80, [80, 40, 40], 0, 80))
        # answered "noop": counted apart, nothing sent again
        api = BulkApi(gone={"1", "2"})
        r = await L.remove_in_batches(api, ids[:10], L.RemoveLimit(), workers=1)
        self.assertEqual((sorted(r["noop"]), len(r["removed"]), len(api.calls)), (["1", "2"], 8, 1))

    async def test_bulk_removal_waits_out_a_rate_limit_and_stops_when_told(self):
        ids = [str(i) for i in range(100)]
        api = BulkApi(limit_on={2})  # the second request answered with a 429: waited out, sent again, the rest at half the pace
        progress = {}
        r = await L.remove_in_batches(api, ids, L.RemoveLimit(), workers=4, progress=progress)
        self.assertEqual((len(r["removed"]), r["left"], r["failed"], r["limited"]), (100, [], [], 1))
        self.assertEqual((progress["removed"], progress["limited"], progress["workers"]), (100, 1, 2))
        # one try only (a window's own orders): the rate limit ends it at once, the rest left to the caller's retry
        api = BulkApi(limit_on={1})
        r = await L.remove_in_batches(api, ids[:30], L.RemoveLimit(), workers=1, patient=False)
        self.assertEqual((r["removed"], r["left"], r["limited"], len(api.calls)), ([], ids[:30], 1, 1))
        self.assertIn("429", r["errors"][-1])
        api = BulkApi()
        api.blocked_until = time.monotonic() + 30  # a cooldown another client began: one try sends nothing into it
        r = await L.remove_in_batches(api, ids[:30], L.RemoveLimit(), workers=1, patient=False)
        self.assertEqual((r["left"], api.calls), (ids[:30], []))
        self.assertIn("限流冷却中", r["errors"][-1])
        # the deadline and the stop: what was not sent is left, never lost
        api = BulkApi(slow=0.05)
        r = await L.remove_in_batches(api, ids, L.RemoveLimit(), workers=1, deadline=time.monotonic() + 0.08)
        self.assertTrue(r["removed"] and r["left"] and len(r["removed"]) + len(r["left"]) == 100, (len(r["removed"]), len(r["left"])))
        r = await L.remove_in_batches(BulkApi(), ids, L.RemoveLimit(), stop=lambda: True)
        self.assertEqual((r["removed"], r["left"]), ([], ids))
        # the batch measured is the bot's, kept in its store across restarts
        bot = L.LiveBot.__new__(L.LiveBot)
        bot.store = m.Store(":memory:")
        lim = bot.live_remove_limit()
        lim.note_ok(80, NOW)
        bot.live_remove_saved(lim)
        again = L.LiveBot.__new__(L.LiveBot)
        again.store = bot.store
        self.assertEqual((again.live_remove_limit().as_dict(), again.live_remove_limit().size()), ({"ok": 80, "cap": 0, "at": NOW}, 160))
        self.assertIn("每批 160 张（实测一次 80 张全部应答", again.live_remove_limit().text())
        self.assertEqual(L.RemoveLimit(ok=90, cap=80).as_dict()["ok"], 79, "a stored record that contradicts itself: under the cap")

    async def test_bulk_removal_stops_on_a_refusal_of_the_request_itself(self):
        """1.141.1 (the review): an account-wide refusal used to be split down to single ids — 1,170 requests for 600 ids."""
        ids = [str(i) for i in range(600)]
        api = BulkApi(refuse_all="HTTP 403: forbidden")
        r = await L.remove_in_batches(api, ids, L.RemoveLimit(), workers=4)
        self.assertEqual((r["halted"], len(r["left"]), r["failed"], r["removed"]), ("systemic", 600, [], []))
        self.assertLessEqual(len(api.calls), 4, [len(c) for c in api.calls])
        api = BulkApi(refuse_all="HTTP 400: bad request")  # not a status that says so: both halves of a split refused is the proof
        r = await L.remove_in_batches(api, ids, L.RemoveLimit(), workers=4)
        self.assertEqual((r["halted"], len(r["left"]) + len(r["failed"]), r["removed"]), ("refused", 600, []))
        self.assertLessEqual(len(api.calls), 12, [len(c) for c in api.calls])
        api = BulkApi(refuse_all="HTTP 403: forbidden")
        r = await L.remove_in_batches(api, ids[:10], L.RemoveLimit(), workers=1, patient=False)
        self.assertEqual((r["halted"], r["left"], len(api.calls)), ("systemic", ids[:10], 1))
        # one refusal of a batch of 20, fine after: not the size — no ceiling, and the batch goes on growing
        api, limit = BulkApi(refuse_first=1), L.RemoveLimit()
        r = await L.remove_in_batches(api, ids[:200], limit, workers=1)
        self.assertEqual((len(r["removed"]), limit.cap, limit.soft), (200, 0, 0))
        self.assertGreaterEqual(limit.size(), 40)
        # a ceiling a day old lapses: Predict may take more by now
        old = L.RemoveLimit(ok=40, cap=80, at=NOW - 2 * 86_400_000)
        old.refresh(NOW)
        self.assertEqual((old.cap, old.size(), old.changed), (0, 80, True))

    async def test_a_window_in_its_last_seconds_is_retried_from_the_loop_not_behind_queued_reads(self):
        """1.141.1 (the review): a failed cancel at the cancel time was retried only through the reconcile queue, behind every
        final read a big withdrawal had left there — its second try went out after the order's signed expiry."""
        ctl, api, clock = controller()
        pair = (await self.book_of(ctl, api, 1))[0]
        pair.update(phase="closing", reason="到时撤单或真实交易暂停", free=True)  # its first try failed
        clock.ms = pair["cancel_ms"] + 1_000  # the last stretch: past the cancel time, before the signed expiry
        ctl.ledger.save(pair)
        await ctl.reconcile_gate.acquire()
        await ctl.reconcile_gate.acquire()  # the final reads of a big withdrawal, ahead in the queue
        ctl.launch_reconcile(pair)  # its own reconcile queued behind them
        task = asyncio.create_task(ctl.cancellation_loop())
        try:
            await self.until(lambda: {"0u", "0d"} <= set(api.removed), timeout=2)
        finally:
            ctl.stop.set()
            await asyncio.gather(task, return_exceptions=True)
            ctl.reconcile_gate.release()
            ctl.reconcile_gate.release()
        await self.finish(ctl)

    async def test_a_crashed_controllers_withdrawal_has_a_deadline_and_the_account_cancel_leaves_the_modes_orders(self):
        ctl, api, clock = controller()
        await self.book_of(ctl, api, 100)  # 200 orders, a controller that crashed: no loop to leave them to
        ctl.alive = False
        quick = api.remove_orders

        async def slow(ids):
            await asyncio.sleep(0.5)
            return await quick(ids)
        api.remove_orders = slow
        with patch.object(p, "DEAD_WITHDRAW_S", 0.2):
            started = time.monotonic()
            line = await ctl.withdraw_all("紧急停止")
        self.assertLess(time.monotonic() - started, 2, "the command waits the deadline, not the whole book")
        self.assertTrue(line.startswith("预挂模式已停止：0.2 秒内撤了 80 张，还有 120 张没撤成"), line)
        await self.finish(ctl)
        # /live cancel account: the mode's own orders are its loop's (in batches) — the account's other ones alone go from here
        ctl, api, _ = controller()
        await self.book_of(ctl, api, 2)

        class LiveApi:
            def __init__(self):
                self.removed, self.blocked_until = [], 0.0
            async def orders(self, status="OPEN"):
                return [{"id": "0u", "order": {"hash": "0x0000up"}}, {"id": "x1", "order": {"hash": "0x0001down"}},  # the mode's, by id and by hash
                        {"id": "999", "order": {"hash": "0xforeign"}}]
            async def remove_orders(self, ids):
                self.removed.extend(ids)
                return {"removed": list(ids), "noop": []}
        bot = L.LiveBot.__new__(L.LiveBot)
        bot.preopen, bot.sim_trades, bot.stopping, bot.store = ctl, (lambda: {}), asyncio.Event(), m.Store(":memory:")
        bot.live, bot.live_unknown_open = types.SimpleNamespace(ready=True, api=LiveApi()), []
        text = await bot._live_cancel_command(["account"], NOW)
        self.assertEqual(bot.live.api.removed, ["999"])
        self.assertIn("账户上无记录的挂单 1 张：Predict 确认撤掉 1 张", text)
        # 1.154.26 (the audit of 11-18, 2): the read is cut at ORDERS_PAGES_MAX pages — always, beside this mode's thousands — and
        # nobody looked at its own word (orders_complete): an order listed past the cut was not withdrawn, the reply silent. The
        # mode's last whole snapshot of the account fills the list in...
        bot.live.api.removed, bot.live.api.orders_complete = [], False
        ctl.account_rows = [{"id": "0u", "order": {"hash": "0x0000up"}}, {"id": "999", "order": {"hash": "0xforeign"}},
                            {"id": "777", "order": {"hash": "0xpastthecut"}}]
        ctl.account_at = NOW - 60_000
        text = await bot._live_cancel_command(["account"], NOW)
        self.assertEqual((bot.live.api.removed, text.startswith("✅")), (["999", "777"], True), text)
        self.assertIn("账户上无记录的挂单 2 张（其中 1 张是按预挂 60 秒前的账户快照补上的）：Predict 确认撤掉 2 张", text)
        # (an id the snapshot alone named, left out of Predict's answer: most likely gone since — said, and no ❌ for it)
        plain_remove = bot.live.api.remove_orders

        async def without_777(ids):
            bot.live.api.removed.extend(ids)
            return {"removed": [i for i in ids if i != "777"], "noop": []}
        bot.live.api.remove_orders, bot.live.api.removed = without_777, []
        with patch.object(L, "REMOVE_RETRY_S", 0.0):
            text = await bot._live_cancel_command(["account"], NOW)
        self.assertTrue(text.startswith("✅") and "，1 张快照里有、Predict 这次没提（多半已不在）" in text and "没撤成" not in text, text)
        bot.live.api.remove_orders = plain_remove
        # ...and with none young enough the reply says the list was cut, and is no success — with or without anything to withdraw
        bot.live.api.removed, ctl.account_at = [], NOW - bot.LIVE_SNAPSHOT_MAX_AGE_MS - 1
        text = await bot._live_cancel_command(["account"], NOW)
        self.assertEqual((bot.live.api.removed, text.startswith("❌")), (["999"], True), text)
        self.assertIn("账户挂单太多，这次只读到前 3 张：排在后面的挂单（手动单）没有撤到", text)
        only_own = bot.live.api.orders

        async def own_alone(status="OPEN"):
            return (await only_own(status))[:2]
        bot.live.api.orders, bot.live.api.removed = own_alone, []
        text = await bot._live_cancel_command(["account"], NOW)
        self.assertEqual((bot.live.api.removed, text.startswith("❌ 读到的挂单里没有要撤的，但账户挂单太多，这次只读到前 2 张")), ([], True), text)
        # an id Predict leaves out of every answer is not one that "needed no removal": not withdrawn, the reply a ❌
        bot.live.api.orders, bot.live.api.orders_complete, bot.live.api.removed = only_own, True, []

        async def silent(ids):
            bot.live.api.removed.extend(ids)
            return {"removed": [], "noop": []}
        bot.live.api.remove_orders = silent
        with patch.object(L, "REMOVE_RETRY_S", 0.0):
            text = await bot._live_cancel_command(["account"], NOW)
        self.assertTrue(text.startswith("❌") and "称无需撤" not in text, text)
        self.assertIn("账户上无记录的挂单 1 张：Predict 确认撤掉 0 张，1 张没撤成（Predict 的应答里没有它们），再发一次 /live cancel account", text)

        async def gone(ids):
            return {"removed": [], "noop": list(ids)}
        bot.live.api.remove_orders = gone
        text = await bot._live_cancel_command(["account"], NOW)
        self.assertTrue(text.startswith("✅") and "账户上无记录的挂单 1 张：Predict 确认撤掉 0 张，1 张称无需撤" in text, text)
        await self.finish(ctl)

    async def test_a_settings_change_withdraws_a_big_book_in_batches(self):
        ctl, api, clock = controller()
        pairs = await self.book_of(ctl, api, 40, phase="closing")  # 80 orders the new settings no longer describe (adopt marks them so)
        sizes = self.count_removals(api)
        task = asyncio.create_task(ctl.cancellation_loop())
        try:
            # Audit PRE-LATE-FILL: removed in batches — and each order's record read LATE_FILL_MS after it left the book, not right
            # after the removal (this waited for "final" with the clock standing still)
            await self.until(lambda: all(pr["phase"] == "closed" for pr in pairs))
            self.assertEqual(({o["state"] for pr in pairs for o in pr["orders"].values()}, any(pr.get("accounted") for pr in pairs)), ({"removed"}, False))
            clock.ms += p.LATE_FILL_MS
            await self.until(lambda: all(o["state"] == "final" for pr in pairs for o in pr["orders"].values()))
        finally:
            ctl.stop.set()
            await asyncio.gather(task, return_exceptions=True)
            if ctl.bulk_task is not None:
                await asyncio.gather(ctl.bulk_task, return_exceptions=True)
        self.assertEqual(len(api.removed), 80)
        self.assertLessEqual(len(sizes), 4, f"in batches, not one request an order: {sizes}")
        self.assertEqual((ctl.bulk_last["orders"], ctl.bulk_last["removed"], ctl.bulk_last["missed"]), (80, 80, 0))
        self.assertTrue(all(pr["accounted"] for pr in pairs), "removed, the final fills read: released")
        self.assertIn("上次分批撤 80 张", ctl.status())
        await self.finish(ctl)

    async def test_switching_off_winds_down_until_nothing_rests(self):
        ctl, api, clock = controller()
        pairs = await self.book_of(ctl, api, 30)  # 60 orders resting
        sizes = self.count_removals(api)
        ctl.wind_down("预挂模式已关闭")
        self.assertEqual((ctl.withdrawing(), ctl.blocked()), ("预挂模式已关闭", "预挂模式已关闭"))
        self.assertTrue(ctl.status().startswith("开盘前自动预挂：收尾中（预挂模式已关闭）：不再新挂"), ctl.status())
        self.assertIn("还可能挂着 60 张", ctl.status())
        task = asyncio.create_task(ctl.cancellation_loop())
        # Audit PRE-LATE-FILL: nothing rests any more — but the mode stops only once the withdrawn orders' records are read, and
        # that is LATE_FILL_MS after they left the book, not right after the removal (this waited with the clock standing still)
        await self.until(lambda: all(pr["phase"] == "closed" for pr in pairs))
        await asyncio.sleep(0.6)  # (a pass of the loop with nothing left on the book)
        self.assertEqual((ctl.stop.is_set(), any(pr.get("accounted") for pr in pairs), len(api.removed)), (False, False, 60))
        clock.ms += p.LATE_FILL_MS
        await asyncio.wait_for(ctl.stop.wait(), 5)  # wound up: it stops itself
        await asyncio.gather(task, return_exceptions=True)
        self.assertTrue(all(pr["accounted"] for pr in pairs) and len(api.removed) == 60 and len(sizes) <= 4, sizes)
        self.assertEqual(ctl.bulk_last["removed"], 60)
        self.assertEqual(await ctl.withdraw_all("x"), "")  # nothing left
        await self.finish(ctl)

    async def test_switched_off_the_filled_windows_are_still_settled(self):
        """1.154.26 (the audit of 11-18, 8): a pair with a fill is accounted by settle_pair alone, on Predict's result. Switched
        off, the mode wound down until nothing could rest and the fills were read — a minute or two after the cancels, the
        windows not yet run — and stopped; nothing settled the filled ones after that: their loss in no day's P&L, outside
        LIVE_MAX_DAILY_LOSS_USD, until the mode was switched on again (and then booked on that day). Now the operator is told
        the orders are off as before, the controller goes on until those windows have their result, and a start with the
        mode off takes them up too."""
        ctl, api, clock = controller()
        pairs = await self.book_of(ctl, api, 3)  # 6 orders resting
        filled = pairs[0]
        api.rows[filled["orders"]["up"]["hash"]]["amountFilled"] = "20"  # 20 of its 50 Up taken before the switch
        notes = []
        ctl.notify = lambda text, key="": notes.append(text)
        ctl.wind_down("预挂模式已关闭")
        task = asyncio.create_task(ctl.cancellation_loop())
        try:
            await self.until(lambda: all(pr["phase"] == "closed" for pr in pairs))
            clock.ms += p.LATE_FILL_MS
            await self.until(lambda: len(notes) == 1)  # nothing rests, the fills read: said now
            self.assertEqual(notes[0], "✅ 开盘前预挂已关闭：本模式的挂单已全部撤完；还有 1 场有成交、等结果：出结果后照常结算入账，都结算完本模式才停")
            await asyncio.sleep(1.2)  # (two passes of the loop)
            self.assertEqual((ctl.stop.is_set(), filled.get("accounted"), filled["orders"]["up"]["filled"], [bool(pr.get("accounted")) for pr in pairs[1:]]),
                             (False, None, 20.0, [True, True]), "not stopped: the filled window waits for its result")
            self.assertEqual((ctl.rest_cleared(clock.ms), ctl.results_awaited(clock.ms), ctl.wound_up(clock.ms), len(notes)), (True, 1, False, 1))
            self.assertIn("已成交的场次都出结果结算完就自动停止", ctl.status())
            self.assertEqual(ctl.blocked(), "预挂模式已关闭", "nothing new goes out meanwhile")
            # the window over, Predict's result in: settled as ever (Up lost: 20 shares at the pair's price), and only then the stop
            clock.ms = p.settle_from(filled) + 61_000
            api.markets["123"]["outcomes"][0]["status"] = "WON"  # Down
            api.markets["123"]["status"] = "RESOLVED"  # (1.154.77: a result is booked on a market Predict calls settled)
            await asyncio.wait_for(ctl.stop.wait(), 5)
            price = p.pair_quote(filled)[0]
            self.assertTrue(filled["accounted"] and filled["result"] == "down")
            self.assertAlmostEqual(filled["pnl"], -price * 20)
            self.assertEqual((filled["accounted_day"], len(notes)), (dt.datetime.fromtimestamp(clock.ms / 1000, m.BEIJING).date().isoformat(), 1))
        finally:
            ctl.stop.set()
            await asyncio.gather(task, return_exceptions=True)
        # a market that never gives its result is not waited for beyond three days from its start
        ctl, api, clock = controller()
        pairs = await self.book_of(ctl, api, 1)
        for order in pairs[0]["orders"].values():
            order.update(state="final", filled=10.0)
        self.assertTrue(p.awaiting_result(pairs[0], clock.ms) and p.awaiting_result(pairs[0], pairs[0]["start_ms"] + 3 * m.DAY_MS))
        self.assertFalse(p.awaiting_result(pairs[0], pairs[0]["start_ms"] + 3 * m.DAY_MS + 1))
        self.assertFalse(p.awaiting_result({**pairs[0], "accounted": True}, clock.ms) or p.awaiting_result(
            {**pairs[0], "orders": {s: {**o, "filled": 0.0} for s, o in pairs[0]["orders"].items()}}, clock.ms))
        await self.finish(ctl)

    async def test_a_rate_limit_mid_withdrawal_is_waited_out_not_left_to_one_order_at_a_time(self):
        ctl, api, clock = controller()
        pairs = await self.book_of(ctl, api, 40)
        calls = {"n": 0}
        real = api.remove_orders

        async def limited(ids):
            calls["n"] += 1
            if calls["n"] == 2:
                api.blocked_until = time.monotonic() + 0.1
                raise m.RemoteError("HTTP 429: too many requests", 1)
            return await real(ids)
        api.remove_orders = limited
        await ctl.cancel_all("test", free=True)
        self.assertTrue(all(o["state"] in {"removed", "final"} for pr in pairs for o in pr["orders"].values()))
        self.assertEqual((ctl.bulk_last["limited"], ctl.bulk_last["missed"], ctl.bulk_last["removed"]), (1, 0, 80))
        self.assertLessEqual(calls["n"], 8)
        await self.finish(ctl)

    async def test_an_operators_withdrawal_of_thousands_answers_at_once(self):
        ctl, api, clock = controller()
        await self.book_of(ctl, api, 30)
        line = await ctl.withdraw_all("管理员暂停开盘前模式")
        self.assertTrue(line.startswith("正在分批撤 60 张预挂单（每批 20 张") and api.removed == [], line)  # the loop takes them, in batches
        few, fapi, _ = controller()
        await self.book_of(few, fapi, 1)
        self.assertEqual((await few.withdraw_all("x"), sorted(fapi.removed)), ("", ["0d", "0u"]))  # a few: here and now
        self.assertIn("批量撤单：每批 20 张（还没有实测", few.check())
        await self.finish(ctl)
        await self.finish(few)

    async def test_a_mode_off_with_orders_left_starts_a_wind_down(self):
        """Switched off with orders out: the controller winds down (it does not stop at once with a 12-second withdrawal),
        and is cleared once it stopped itself; the mode off at a start with orders still resting in its records: one is
        started just to wind them down."""
        class FakeCtl:
            def __init__(self, owner):
                self.owner, self.settings, self.alive, self.stop, self.error = owner, owner.config.preopen, True, asyncio.Event(), ""
                self.ledger, self.winding, self.ended = types.SimpleNamespace(close=lambda: None), "", False
            def wind_down(self, reason):
                self.winding = reason
            async def run(self):
                try:
                    await self.stop.wait()
                finally:
                    self.ended, self.alive = True, False
            def status(self): return "fake"
        with tempfile.TemporaryDirectory() as folder:
            env = {"TELEGRAM_BOT_TOKEN": "offline", "STATE_DB": str(Path(folder) / "s.sqlite3"), "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64}
            bot = L.LiveBot.__new__(L.LiveBot)
            bot.config, bot.stopping = m.Config.from_env({**env, "PREOPEN_15M": "on"}), asyncio.Event()
            bot.market = types.SimpleNamespace(now_ms=lambda: NOW)
            bot.preopen, bot.preopen_task, bot.preopen_factory, bot.preopen_lock = None, None, FakeCtl, asyncio.Lock()
            await bot.preopen_apply()
            first = bot.preopen
            bot.config = m.Config.from_env({**env, "PREOPEN_15M": "off"})
            await bot.preopen_apply()
            self.assertTrue(bot.preopen is first and first.winding == "预挂模式已关闭" and not first.ended, "off: winding down, not cut short")
            await bot.preopen_apply()
            self.assertIs(bot.preopen, first, "asked again while winding down: left to finish")
            first.stop.set()  # wound up: it stops itself, and is cleared
            for _ in range(100):
                if bot.preopen is None:
                    break
                await asyncio.sleep(0.01)
            self.assertTrue(bot.preopen is None and first.ended and not bot.preopen_active())
            # the mode off at a start, its records holding an order that may rest: a controller just to wind it down
            ledger = p.Ledger(bot.config.preopen.db)
            ledger.save({"slug": "w1", "key": "BTC-15M", "phase": "closing", "start_ms": NOW + 3600_000, "expires": (NOW + 3590_000) // 1000,
                         "orders": {"up": {"id": "1", "hash": "0xh", "state": "open", "filled": 0.0}}})
            ledger.save({"slug": "w0", "key": "BTC-15M", "phase": "closing", "start_ms": NOW - 3600_000, "expires": (NOW - 3610_000) // 1000,
                         "orders": {"up": {"id": "2", "hash": "0xg", "state": "open", "filled": 0.0}}})  # expired: rests nowhere...
            ledger.close()
            # ...but what it filled before its expiry is still unread: counted too (1.154.33, an outside review, R09 — it was 1, and
            # with no order of the mode left resting nothing read it: test_a_mode_off_start_reads_the_orders_that_expired_while_it_was_down)
            self.assertEqual((bot.preopen_left(), bot.preopen_leftovers()), ((2, 0), 2))
            await bot.preopen_apply()
            self.assertEqual(bot.preopen.winding, "预挂模式关着，但上次还有挂单没撤完")
            bot.config = m.Config.from_env({**env, "PREOPEN_15M": "on"})  # switched back on while winding down: a fresh controller takes the records over
            winding = bot.preopen
            await bot.preopen_apply()
            self.assertTrue(winding.ended and bot.preopen is not winding and not bot.preopen.winding and winding.keep_on_stop)
            await bot.preopen_stop("test")
            # 1.154.26 (the audit of 11-18, 8): the mode off at a start with nothing that may rest — but a window with fills whose
            # result nobody booked (the wind-down ended before its result, a restart in the middle): a controller to settle it
            bot.config = m.Config.from_env({**env, "PREOPEN_15M": "off"})
            for _ in range(3):  # (the apply the wound-up controller's end asked for may still be on its way: let it pass, then none runs)
                await asyncio.sleep(0.02)
                await bot.preopen_stop("test")
            ledger = p.Ledger(bot.config.preopen.db)
            for slug in ("w0", "w1"):
                ledger.delete(slug)
            ledger.save({"slug": "f1", "key": "BTC-15M", "phase": "closed", "start_ms": NOW - 3600_000, "expires": (NOW - 3610_000) // 1000,
                         "orders": {"up": {"id": "3", "hash": "0xf", "state": "final", "filled": 20.0}, "down": {"id": "4", "hash": "0xe", "state": "final", "filled": 0.0}}})
            ledger.save({"slug": "f0", "key": "BTC-15M", "phase": "closed", "start_ms": NOW - 7200_000, "expires": (NOW - 7210_000) // 1000, "accounted": True,
                         "accounted_day": "2026-11-17", "pnl": -9.8, "orders": {"up": {"id": "5", "hash": "0xd", "state": "final", "filled": 20.0}}})  # settled: nothing to do
            ledger.close()
            self.assertEqual((bot.preopen_left(), bot.preopen_leftovers()), ((0, 1), 0))
            await bot.preopen_apply()
            self.assertEqual(bot.preopen.winding, "预挂模式关着，但还有已成交的场次没结算")
            await bot.preopen_stop("test")
            ledger = p.Ledger(bot.config.preopen.db)
            ledger.save({**ledger.rows["f1"], "accounted": True, "accounted_day": "2026-11-18", "pnl": -9.8})  # settled by that controller
            ledger.close()
            self.assertEqual(bot.preopen_left(), (0, 0))
            await bot.preopen_apply()
            self.assertIsNone(bot.preopen, "nothing left: none started")

    # --- 1.154.33: an outside review of the pre-open mode ---------------------------------------------------------------------
    async def test_a_fill_read_after_its_pair_was_accounted_takes_it_back_to_settle(self):
        """1.154.33 (an outside review, CR-02) — the account snapshot reads every page of the open orders first and applies them
        after (thousands of orders: minutes), so its rows are often from before a window's cancel. When the order's own record
        still lagged a minute after the removal (Audit PRE-LATE-FILL's case not covered: LATE_FILL_MS), the final reads
        accounted the pair with nothing filled, and the snapshot's fill landed on it after that: the shares written in, the
        pair left accounted at 0 — settle_pair and the loop passed it by, its loss never in the day's P&L nor the loss stop.
        Now such a fill takes the pair back among those to settle, and Predict's result prices it."""
        ctl, api, clock = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        up = pair["orders"]["up"]
        clock.ms = ctl.account_at = c["cancel_ms"] - 30_000
        row = api.rows[up["hash"]]
        row["amountFilled"] = str(int(row["amount"]) * 20 // 50)  # 20 of Up's 50 shares matched: no snapshot has seen them yet
        release, listed = asyncio.Event(), api.orders

        async def pages(status="OPEN"):
            rows = await listed(status)  # the page with Up read now: open, 20 filled...
            await release.wait()  # ...the rest of the pages take their time
            return rows
        api.orders = pages
        snapshot = asyncio.create_task(ctl.account())
        await asyncio.sleep(0.05)
        record = api.order

        async def lagging(oid, digest=""):  # Up's own record: cancelled with nothing filled, a minute after the removal still
            found = await record(oid, digest)
            if found is not None and digest == up["hash"]:
                found["amountFilled"] = "0"
            return found
        api.order = lagging
        clock.ms = c["cancel_ms"]
        await ctl.cancel_many([pair], "到时撤单或真实交易暂停", free=True)
        await self.settle_tasks(ctl)
        await self.read_finals(ctl, clock, pair)
        self.assertEqual((pair.get("accounted"), pair.get("pnl"), up["state"], up["filled"]), (True, 0.0, "final", 0.0))
        release.set()
        await snapshot  # applied now: the fill read before the cancel
        self.assertEqual((up["filled"], pair["accounted"], "pnl" in pair, "accounted_day" in pair), (20.0, False, False, False))
        self.assertIn(pair, ctl.ledger.pending(), "back among the working records: the loop settles it on the result")
        saved = json.loads(ctl.ledger.conn.execute("SELECT body FROM pairs WHERE slug=?", (pair["slug"],)).fetchone()[0])
        self.assertEqual((saved["accounted"], saved["orders"]["up"]["filled"]), (False, 20.0), "and so on file")
        api.markets["123"]["outcomes"][0]["status"] = "WON"  # Down won: Up's 20 shares at 49¢ are lost
        api.markets["123"]["status"] = "RESOLVED"  # (1.154.77: a result is booked on a market Predict calls settled)
        clock.ms = p.settle_from(pair) + 61_000
        await ctl.settle_pair(pair)
        self.assertEqual((pair["accounted"], pair["result"]), (True, "down"))
        self.assertAlmostEqual(pair["pnl"], -9.8)
        self.assertAlmostEqual(ctl.daily_pnl(), -9.8, msg="in the day's figure the loss stop reads")
        await self.finish(ctl)

    async def test_a_mode_off_start_reads_the_orders_that_expired_while_it_was_down(self):
        """1.154.33 (an outside review, R09) — the process's stop leaves the orders on the book (the next controller takes them
        over), and a start with the mode off counted what may still rest (before the signed expiry), the withdrawn orders'
        unread final fills and the windows with a fill on record: an order that expired while no process ran, matched
        meanwhile, was none of them. With nothing else of the mode resting nothing ever read it — its loss out of the day's P&L
        and the loss stop, its outcomes kept from the model trader's positions check for three days. Now it starts the
        wind-down, which reads it and settles the window."""
        with tempfile.TemporaryDirectory() as folder:
            sized = dict(PREOPEN_DB=str(Path(folder) / "down.preopen.sqlite3"), PREOPEN_MARKETS="btc", PREOPEN_SHARES="100")
            ctl, api, clock = controller(**sized)
            c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
            await ctl.place_pair(c)
            up, down = (ctl.ledger.rows[c["slug"]]["orders"][side] for side in ("up", "down"))
            await self.finish(ctl)  # the process stops: the orders left on the book
            # while no process runs: 40 of Up's 100 shares matched, then the signed expiry passes (a removal now: noop)
            api.rows[up["hash"]].update(amountFilled=str(int(api.rows[up["hash"]]["amount"]) * 40 // 100), status="EXPIRED")
            api.rows[down["hash"]]["status"] = "EXPIRED"
            api.stuck = {up["id"], down["id"]}
            clock.ms = c["start_ms"] + 5 * 60_000  # back five minutes into the window, PREOPEN_15M off
            bot = L.LiveBot.__new__(L.LiveBot)
            bot.config = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "STATE_DB": ":memory:", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64,
                                            "PREOPEN_15M": "off", **sized})
            bot.store, bot.stopping, bot.market = m.Store(":memory:"), asyncio.Event(), types.SimpleNamespace(now_ms=lambda: clock.ms)
            bot.live_notify, bot.live_note_error = (lambda text, key="": None), (lambda text: None)
            bot.preopen, bot.preopen_task, bot.preopen_lock = None, None, asyncio.Lock()
            bot.preopen_tidied = 1e18  # (no archive pass in this check)
            made = []

            def factory(owner):  # the real controller on that ledger file, on the bot's clock, Predict as the fake answers
                again, _, _ = controller(**sized)
                again.owner.market, again.api, again.cancel_api, api.ctl = bot.market, api, api, again
                made.append(again)
                return again
            bot.preopen_factory = factory
            self.assertEqual(bot.preopen_left(), (2, 0), "both orders' fills unread (it was (0, 0): nothing started, nothing read them)")
            await bot.preopen_apply()
            self.assertEqual((len(made), bot.preopen is made[0], bot.preopen.winding), (1, True, "预挂模式关着，但上次还有挂单没撤完"))
            pair = made[0].ledger.rows[c["slug"]]
            orders = pair["orders"]
            await self.until(lambda: orders["up"]["state"] == "removed" and orders["down"]["state"] == "removed")
            self.assertEqual((orders["up"]["filled"], orders["down"]["filled"]), (40.0, 0.0), "the fill of the minutes the process was down: read")
            clock.ms += p.LATE_FILL_MS  # the records' last word (PRE-LATE-FILL)
            await self.until(lambda: orders["up"]["state"] == "final" and orders["down"]["state"] == "final")
            api.markets["123"]["outcomes"][0]["status"] = "WON"  # Down won: Up's 40 shares at 49¢ are lost
            api.markets["123"]["status"] = "RESOLVED"  # (1.154.77: a result is booked on a market Predict calls settled)
            clock.ms = p.settle_from(pair) + 61_000
            await self.until(lambda: bot.preopen is None, 5.0)  # settled, wound up and cleared — and none started again
            self.assertEqual((pair["accounted"], pair["result"]), (True, "down"))
            self.assertAlmostEqual(pair["pnl"], -19.6)
            self.assertAlmostEqual(bot.preopen_pnl(), -19.6, msg="in the day's loss stop (the file's, the mode off)")
            self.assertEqual((bot.preopen_left(), bot.preopen_unverified()), ((0, 0), set()))

    async def test_the_signature_leaves_the_record_once_its_request_is_done(self):
        """1.154.33 (an outside review, R24) — the ledger kept every order's whole signed request, the signature that makes the
        order good until its expiry, in the working records and the archive for good, though nothing reads it after the
        request (an order whose answer went missing is looked for by its hash, never sent again). Dropped once the request is
        done with — not before: a refusal on the rule taker's borrowed key sends the same body again on the mode's own (through)
        — and from an order that will never be sent; what resumable, pair_quote and the model trader's market check read stays."""
        with tempfile.TemporaryDirectory() as folder:
            db = str(Path(folder) / "signed.preopen.sqlite3")
            ctl, api, clock = controller(PREOPEN_DB=db)
            seen = []  # (key, the body still signed) at each order request

            class Lent:  # the rule taker's idle key: it refuses (its limit), and through() sends the request on the mode's own
                api_key = "lent"

                async def market(self, mid):
                    raise m.RemoteError("HTTP 429: Too Many Requests", 20)

                async def create_order(self, body):
                    seen.append(("lent", "signature" in body["data"]["order"]))
                    raise m.RemoteError("HTTP 429: Too Many Requests", 20)
            lent = Lent()
            ctl.lent_api, ctl.lane = lent, (lambda home, trade=False: lent)  # (1.154.60: the lane is told when the request is an order)
            plain = api.create_order

            async def create(body):
                seen.append(("own", "signature" in body["data"]["order"]))
                return await plain(body)
            api.create_order = create
            c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
            await ctl.place_pair(c)
            pair = ctl.ledger.rows[c["slug"]]
            self.assertEqual({side: o["state"] for side, o in pair["orders"].items()}, {"up": "open", "down": "open"})
            self.assertEqual(sorted(seen), [("lent", True), ("lent", True), ("own", True), ("own", True)], "sent signed, the second time too")
            self.assertTrue(all(sent["data"]["order"]["signature"] == "0xsignature" for sent in api.sent))
            on_file = next(x for x in p.read_rows(db) if x["slug"] == c["slug"])
            for side in ("up", "down"):
                for body in (on_file["orders"][side]["body"], pair["orders"][side]["body"]):
                    signed = body["data"]["order"]
                    self.assertNotIn("signature", signed)
                    self.assertTrue(signed["makerAmount"] and signed["takerAmount"] and signed["tokenId"] and body["data"]["pricePerShare"], body)
            self.assertEqual(p.pair_quote({"orders": on_file["orders"]}), (0.49, 50.0), "an older record's price and size: still off the request")
            await self.finish(ctl)
            # a restart: the resting pair is taken over as before (resumable reads the amounts off the request)
            again, _, _ = controller(PREOPEN_DB=db)
            again.adopt()
            pair = again.ledger.rows[c["slug"]]
            self.assertEqual((again.resumed, pair["phase"]), (1, "active"))
            # never to be sent: a "prepared" order (a process that ended between the record and its request) loses it as it
            # is withdrawn — by the batch withdrawal (cancel_many) and by the pair's own (cancel_pair)
            pair["orders"]["up"].update(state="prepared", body=copy.deepcopy(api.sent[0]))
            await again.cancel_many([pair], "设置不同，撤掉后重新报价", free=True)
            pair["orders"]["down"].update(state="prepared", body=copy.deepcopy(api.sent[1]))
            await again.cancel_pair(pair, "撤单状态核对")
            on_file = next(x for x in p.read_rows(db) if x["slug"] == c["slug"])
            for side in ("up", "down"):
                self.assertEqual((pair["orders"][side]["state"], on_file["orders"][side]["state"]), ("not_sent", "not_sent"))
                self.assertNotIn("signature", on_file["orders"][side]["body"]["data"]["order"])
            await self.finish(again)


class BulkApi:
    """Predict's removal endpoint for the batch tests (1.141.0): ``cap`` — more ids than this refused whole (HTTP 400);
    ``take`` — only the first so many of a request answered; ``bad`` — a request with one of these refused whole;
    ``limit_on`` — these requests (by count) answered with a 429; ``gone`` — answered "noop"; ``timeout_over`` — a bigger
    request times out; ``slow`` — seconds an answer takes."""
    def __init__(self, cap=0, take=0, bad=(), limit_on=(), gone=(), slow=0.0, timeout_over=0, refuse_all="", refuse_first=0):
        self.cap, self.take, self.bad, self.limit_on, self.gone, self.slow, self.timeout_over = cap, take, set(bad), set(limit_on), set(gone), slow, timeout_over
        self.refuse_all, self.refuse_first = refuse_all, refuse_first  # every request refused with this text; the first n refused (HTTP 400)
        self.calls, self.blocked_until, self.inflight, self.most = [], 0.0, 0, 0

    async def remove_orders(self, ids):
        self.calls.append(list(ids))
        nth = len(self.calls)  # this request's place, before the others' arrive
        self.inflight += 1
        self.most = max(self.most, self.inflight)
        try:
            await asyncio.sleep(self.slow)
            if self.refuse_all:
                raise m.RemoteError(self.refuse_all)
            if nth <= self.refuse_first:
                raise m.RemoteError("HTTP 400: transient")
            if nth in self.limit_on:
                self.blocked_until = time.monotonic() + 0.05
                raise m.RemoteError("HTTP 429: too many requests", 1)
            if self.cap and len(ids) > self.cap:
                raise m.RemoteError("HTTP 400: too many ids")
            if self.timeout_over and len(ids) > self.timeout_over:
                raise m.RemoteError("网络错误 (TimeoutError)")
            if any(i in self.bad for i in ids):
                raise m.RemoteError("HTTP 400: order not cancellable")
            answered = ids[:self.take] if self.take else ids
            return {"removed": [i for i in answered if i not in self.gone], "noop": [i for i in answered if i in self.gone]}
        finally:
            self.inflight -= 1


if __name__ == "__main__":
    result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(PreopenTests))
    if not result.wasSuccessful(): raise SystemExit(1)
    print("PREOPEN_OK")
