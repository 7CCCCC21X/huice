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
                             "PREOPEN_MARKETS": "all", "PREOPEN_SHARES": "50", "PREOPEN_AHEAD_MINUTES": "60", **env})  # the tests' figures: the three cards, 50 a side, an hour ahead
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

    async def settle_tasks(self, ctl):
        """The reconcile / verify tasks the loop would run: awaited here."""
        await asyncio.gather(*list(ctl.settling.values()), return_exceptions=True)

    async def test_identity_clock_and_all_three(self):
        ctl, api, clock = controller()
        for stem in ("btc", "unitree", "skhynix"):
            self.assertIsNotNone(p.candidate(market(stem), NOW, ctl.owner.config, ctl.settings))
        for row in (market("eth"), market(at=NOW + 120000), market(at=NOW - 900000), market(at=NOW + 86400000),
                    market(status="CANCEL_ONLY"), market(status="CLOSED")):
            self.assertIsNone(p.candidate(row, NOW, ctl.owner.config, ctl.settings))
        row = market(); row["question"] = "10:00AM-10:15AM ET"
        self.assertIsNone(p.candidate(row, NOW, ctl.owner.config, ctl.settings))
        self.assertEqual(p.candidate_problem(market(at=NOW), NOW, ctl.owner.config, ctl.settings), "已开始或不足 3 分钟")
        self.assertEqual(p.candidate_problem(market(at=NOW + 120000), NOW, ctl.owner.config, ctl.settings), "链接里的时间不是整刻钟")
        self.assertEqual(p.candidate_problem(market(at=NOW + 2 * 3600_000), NOW, ctl.owner.config, ctl.settings), "开始还在 60 分钟之外（之后再扫）")
        self.assertEqual(p.candidate_problem(market("eth"), NOW, ctl.owner.config, ctl.settings), "不是三张 15 分钟卡的链接")
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
        for bad in ({"PREOPEN_SHARES": "1"}, {"PREOPEN_PRICE_CENTS": "51"}, {"PREOPEN_MARKETS": "eth"}, {"PREOPEN_AHEAD_MINUTES": "2"}):
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
        self.assertEqual(on(PREOPEN_MARKETS="all").markets, frozenset({"BTC-15M", "SKHYNIX-15M", "UNITREE-15M"}))
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

    async def test_global_budget_and_existing_risk_are_not_overridden(self):
        ctl, api, _ = controller(LIVE_MAX_OPEN_USD="48")
        await ctl.place_pair(p.candidate(market(), NOW, ctl.owner.config, ctl.settings))
        self.assertEqual(api.sent, [])
        ctl.owner.config = dataclasses.replace(ctl.owner.config, live_max_open_usd=100)
        ctl.owner.live_exposure = lambda trades: 60.
        await ctl.place_pair(p.candidate(market(), NOW, ctl.owner.config, ctl.settings))
        self.assertEqual(api.sent, [])
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
        ctl.owner.config = dataclasses.replace(ctl.owner.config, live_max_open_usd=48)
        self.assertEqual(ctl.hold(), "")
        self.assertIn("\n暂不新挂：预挂资金上限不足：需要再预留 49U，限额 48U", ctl.status())
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
        await self.finish(ctl)

    async def test_default_is_off_and_normal_strategy_held_only_while_the_mode_runs_for_real(self):
        cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline"})
        self.assertFalse(p.Settings.from_config(cfg).enabled)
        bot = L.LiveBot.__new__(L.LiveBot)
        bot.preopen = types.SimpleNamespace(alive=True, risk=lambda: 40., daily_pnl=lambda: -10., filled_by_outcome=lambda: {("123", 2): 50.})
        bot.config = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64, "PREOPEN_EXCLUSIVE": "on"})
        self.assertTrue(bot.preopen_active() and bot.preopen_exclusive())
        self.assertIn("不开新仓", bot.live_room({}, None, "up", .49, 50))
        self.assertIn("不发送", bot.live_send_block({"live": {}}, {}, NOW))
        # 1.125.0: off by default — the two run side by side; the mode's money, day and fills count in the model trader's checks
        bot.config = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64, "LIVE_MAX_OPEN_USD": "100"})
        self.assertTrue(bot.preopen_active() and not bot.preopen_exclusive() and not bot.config.preopen.exclusive)
        self.assertEqual((bot.preopen_risk(), bot.preopen_pnl(), bot.preopen_filled()), (40., -10., {("123", 2): 50.}))
        bot.live = types.SimpleNamespace(usd=lambda: None)
        self.assertEqual(bot.live_fit_shares({}, 0.5), 120.)  # min(100 order cap, 100 − 40) / 0.5
        bot.config = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64, "LIVE_MAX_OPEN_USD": "40.5"})
        self.assertIn("持仓+挂单 $40.00 已占满", bot.live_fit_why({}, 0.5))
        bot.preopen.alive = False
        self.assertFalse(bot.preopen_active())  # crashed: the model trader is free again
        self.assertEqual((bot.preopen_risk(), bot.preopen_pnl(), bot.preopen_filled()), (0., 0., {}))
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
        ctl, api, _ = controller([market(), market(at=NOW + 15 * 60_000, mid=5, status="CLOSED"), market("eth", mid=6)])
        lines = await p.probe(api, ctl.owner.config, ctl.settings, NOW)
        text = "\n".join(lines)
        self.assertIn("btc-updown-15m-1793930400（10:00）：会挂；状态 REGISTERED / 交易状态 MATCHING_NOT_ENABLED；缺字段：question", text)  # the fixture states no question
        self.assertIn("btc-updown-15m-1793927700（09:15）：不挂：交易状态 CLOSED", text)
        self.assertGreaterEqual(text.count("：还没发布"), 6)  # the other BTC quarters, 海力士's and 宇树's
        self.assertNotIn("eth", text)
        self.assertIn("UNITREE-15M 连续 4 个刻钟没发布或读不到，更晚的不再问", text)  # the whole day is scanned, each card's list ending at four unlisted in a row
        async def broken(method, path, **kw): raise m.RemoteError("HTTP 403: 访问被拒绝")
        api.call = broken
        self.assertIn("读取失败：HTTP 403", "\n".join(await p.probe(api, ctl.owner.config, ctl.settings, NOW)))
        empty = p.Settings.from_config(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on", "STATE_DB": ":memory:", "PREOPEN_MARKETS": "unitree"}))
        self.assertIn("没有要看的场次", "\n".join(await p.probe(api, ctl.owner.config, empty, NOW + 24 * 3600_000)))  # Saturday 09:00: no 宇树 session for a day
        self.assertEqual("\n".join(await p.probe(api, ctl.owner.config, empty, NOW)).count("读取失败"), 4)  # failures end a card's list too
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
            old_key = c["slug"] + "@" + str(pair["created_ms"])
            self.assertIs(bigger.ledger.rows[old_key], pair)
            queued = bigger.queue.get_nowait()[2]
            self.assertEqual(queued["attempt"], 1)
            await bigger.place_pair(queued)
            fresh = bigger.ledger.rows[c["slug"]]
            self.assertEqual((fresh["attempt"], len(api.sent)), (1, 4))
            self.assertNotEqual({o["hash"] for o in fresh["orders"].values()}, {o["hash"] for o in pair["orders"].values()}, "a new salt")
            self.assertEqual(api.sent[2]["data"]["order"]["takerAmount"], str(L.to_wei(20)))
            self.assertEqual(bigger.risk(), 2 * 0.49 * 20)  # the withdrawn pair released, the new one reserved
            self.assertIn("已记录 1 场（1 场挂着，1 场已结清）", bigger.status())
            self.assertIn("重新报价 1 场", bigger.status())
            # the cap and Predict's own refusal
            fresh["attempt"] = p.MAX_ATTEMPTS - 1
            await bigger.cancel_pair(fresh, "test")
            self.assertTrue(fresh["accounted"] and not bigger.supersedable(fresh))
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
        await ctl.settle_pair(pair)
        self.assertTrue(pair["accounted"])
        self.assertAlmostEqual(pair["pnl"], 1.)
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
