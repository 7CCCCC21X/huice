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


def market(stem="btc", at=NOW + 3600_000, mid=123, status="MATCHING_NOT_ENABLED"):
    return {"id": mid, "categorySlug": f"{stem}-updown-15m-{at//1000}", "conditionId": "0xcondition",
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
                             "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64, "LIVE_MAX_OPEN_USD": "7000",
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
        self.assertEqual(p.candidate_problem(market(at=NOW + 120000), NOW, ctl.owner.config, ctl.settings), "链接里的时间不是整刻钟")
        self.assertEqual(p.candidate_problem(market(at=NOW + 2 * 3600_000), NOW, ctl.owner.config, ctl.settings), "开始还在 60 分钟之外（之后再扫）")
        self.assertEqual(p.candidate_problem(market("sol"), NOW, ctl.owner.config, ctl.settings), "不是 15 分钟卡的链接")
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
        self.assertEqual(on(PREOPEN_MARKETS="all").markets, frozenset({"BTC-15M", "ETH-15M", "BNB-15M", "XAU-15M", "CL-15M", "SKHYNIX-15M", "UNITREE-15M", "ZHIPU-15M"}))
        self.assertEqual(on(PREOPEN_MARKETS="xau, zhipu").markets, frozenset({"XAU-15M", "ZHIPU-15M"}))  # 1.134.0
        zcfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on", "PREOPEN_MARKETS": "zhipu", "SYMBOLS": "BTCUSDT"})
        self.assertEqual((zcfg.tickers["ZHIPUUSDT"].market, zcfg.tickers["ZHIPUUSDT"].code), ("hk", "02513"), "the quoted stock card's exchange ticker, SYMBOLS or not")
        self.assertNotIn("ZHIPUUSDT", m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "SYMBOLS": "BTCUSDT"}).tickers)
        self.assertEqual(on(PREOPEN_MARKETS="btc,eth,bnb").markets, frozenset({"BTC-15M", "ETH-15M", "BNB-15M"}))
        self.assertEqual(on(PREOPEN_MARKETS="btc, skhynix").markets, frozenset({"BTC-15M", "SKHYNIX-15M"}))
        self.assertEqual(on(PREOPEN_MARKETS="").markets, frozenset({"BTC-15M"}))

    async def test_failure_cancels_other_side_and_never_resubmits_unknown(self):
        ctl, api, _ = controller()
        api.fail_side = 2
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        self.assertEqual(api.removed, ["1"])
        pair = ctl.ledger.rows[c["slug"]]
        self.assertEqual((pair["orders"]["down"]["state"], pair["orders"]["up"]["state"]), ("unknown", "final"))
        self.assertEqual(ctl.risk(), 49.)  # the unknown side may exist; the removed side's record read: nothing filled, still reserved until the pair is accounted
        await ctl.place_pair(c)
        self.assertEqual(len(api.sent), 2)
        await self.finish(ctl)

    async def test_refusal_is_final_and_releases(self):
        """A definite refusal (post-only would cross) ends the order: nothing reserved for good, the other side withdrawn,
        the pair accounted at once (the first revision kept it "unknown" with its money, and asked every half second)."""
        ctl, api, _ = controller()
        api.reject_side = 2
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        self.assertEqual((pair["orders"]["down"]["state"], pair["orders"]["up"]["state"], pair["phase"]), ("rejected", "final", "closed"))
        self.assertIn("would cross", pair["orders"]["down"]["error"])
        self.assertTrue(pair["accounted"]) and self.assertEqual(pair["pnl"], 0.)
        self.assertEqual(ctl.risk(), 0.)
        self.assertEqual(ctl.due(NOW + 10 ** 9), [])  # nothing left to try
        ctl.stop.set()  # the stop itself is not a withdrawal: on_stop decides (1.128.2)
        self.assertEqual(ctl.due(NOW), [])
        ctl.stop.clear()
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
        self.assertEqual(p.candidate_problem(market(at=NOW + 15 * 60_000), NOW + 15 * 60_000 - 119_000, ctl.owner.config, ctl.settings), "已开始或不足 3 分钟")
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
        self.assertEqual((pair["orders"]["up"]["state"], pair["orders"]["down"]["state"]), ("final", "open"))
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
        await self.settle_tasks(ctl)  # the reconcile the loop runs: both records final, nothing filled — released now, not at the result
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
        self.assertEqual((pair["orders"]["up"]["state"], pair["orders"]["down"]["state"], pair["phase"]), ("final", "open", "closing"))
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
        api.stuck.clear()
        clock.ms = pair["retry_ms"]
        ctl.bookkeep(pair); ctl.launch_reconcile(pair)
        await self.settle_tasks(ctl)
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

    async def test_discovery_takes_the_soonest_first_and_only_within_the_horizon(self):
        rows = [market(at=NOW + 45 * 60_000, mid=1), market(at=NOW + 15 * 60_000, mid=2), market(at=NOW + 3 * 3600_000, mid=3), market("skhynix", at=NOW + 30 * 60_000, mid=4)]
        ctl, api, _ = controller(rows)
        starts = p.window_starts(NOW, ctl.owner.config, ctl.settings)
        self.assertEqual([(r.key, (s - NOW) // 60_000) for r, s in starts][:4], [("BTC-15M", 15), ("SKHYNIX-15M", 15), ("BTC-15M", 30), ("SKHYNIX-15M", 30)])
        self.assertTrue(all(s - NOW >= 180_000 and s - NOW <= 3_600_000 for _, s in starts) and ("UNITREE-15M", 30) in [(r.key, (s - NOW) // 60_000) for r, s in starts])
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
        self.assertEqual((pair["orders"]["up"]["state"], pair["orders"]["down"]["state"], pair["phase"]), ("final", "not_sent", "closed"))
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
        self.assertEqual(ctl.hold(), "账户快照过期（31 秒前读到）")
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
        self.assertEqual((ctl.errors[-1][1], notes[-1][0]), ("HTTP 500: boom", "preopen:error:HTTP 500: boom"))
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
        filled = {**stuck, "slug": "btc-updown-15m-filled", "start_ms": clock.ms - 20 * 60_000, "cancel_ms": clock.ms - 20 * 60_000 - 20_000,
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
        self.assertEqual((bot.preopen_risk(), bot.preopen_pnl(), bot.preopen_filled()), (0., 0., {("123", 2): 50.}))
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
                self.ran, self.ended = asyncio.Event(), False
                FakeCtl.made.append(self)
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
        await bot.preopen_apply()
        self.assertTrue(second.ended and bot.preopen is None and bot.preopen_task is None and not bot.preopen_active(), "off: stopped")
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
        self.assertIn("只看 2 小时内、每张卡最近 8 个刻钟", text)  # 1.130.6: bounded — a day of five cards was hundreds of reads
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
            self.assertEqual((set(api.removed), pair["accounted"], bigger.supersedable(pair)), ({"1", "2"}, True, True))
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
            self.assertEqual((down["records"], down["filled"], down["volume"], down["settled"]), (4, 3.0, 1.47, 0))
            self.assertEqual([(x["name"], x["status"], x["pnl"], x["attempt"]) for x in r["recent"]],
                             [("宇树 15分涨跌 13:00–13:15", "待结算", None, 1), ("BTC 15分涨跌 10:00–10:15", "已结算", 5.1, 0)])
            self.assertEqual((r["recent"][1]["cancel_s"], r["recent"][1]["lead_min"], r["recent"][1]["result"]), (0.4, 1260.0, "up"))
            # the live bot: the running controller's ledger, or the file while the mode is off; nothing recorded → None
            self.assertEqual((r["daily"]["days"]["2026-11-06"]["records"], r["daily"]["days"]["2026-11-06"]["pnl"], r["daily"]["total"]["open"], r["daily"]["total"]["open_cost"]), (4, 5.1, 1, 2.45))
            bot = L.LiveBot.__new__(L.LiveBot)
            bot.preopen, bot.config, bot.market = ctl, ctl.owner.config, ctl.owner.market
            self.assertEqual(bot.preopen_review()["total"]["records"], 4)
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
        self.assertEqual((up["state"], up.get("refused"), pair["accounted"], ctl.supersedable(pair)), ("final", True, True, False))
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
        for bad in ("49x10", "48x20,48x10", "abc", "48x20,47x20,46x20,45x20,44x20", "48x1", "60x5"):
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
        # the hold lifts: the withdrawn pair, accounted, is quoted again
        ctl.owner.updowns["SKHYNIX-15M"].lead = quote("110000")
        ctl.owner.updowns["UNITREE-15M"].lead = quote("110", "100")
        await self.settle_tasks(ctl)
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
        soon = []
        ctl.owner.live_redeem_soon = lambda: soon.append(1)
        await ctl.settle_pair(pair)
        self.assertTrue(pair["accounted"])
        self.assertAlmostEqual(pair["pnl"], 1.)
        self.assertEqual(soon, [1], "1.131.3: a fill on the winning side (LIVE_AUTO_REDEEM on by default) — redeemed soon")
        self.assertEqual(ctl.risk(), 0.)
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


if __name__ == "__main__":
    result = unittest.TextTestRunner().run(unittest.defaultTestLoader.loadTestsFromTestCase(PreopenTests))
    if not result.wasSuccessful(): raise SystemExit(1)
    print("PREOPEN_OK")
