"""New listing -> durable fixed pair -> independent early cancellation. Entirely offline."""
import asyncio
import copy
import dataclasses
import datetime as dt
import hashlib
import json
import sys
import tempfile
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
        self.pages = {}
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
        params = m.urllib.parse.parse_qs(m.urllib.parse.urlsplit(path).query)
        variant = params["marketVariant"][0]
        after = params.get("after", [""])[0]
        return self.pages.get((variant, after), {"success": True, "data": list(self.markets.values()), "cursor": None})


def controller(markets=None, **env):
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "PREOPEN_15M": "on", "STATE_DB": ":memory:",
                             "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64, "LIVE_MAX_OPEN_USD": "7000", **env})
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
        self.assertEqual((p.Settings.from_config(cfg).shares, p.Settings.from_config(cfg, force=True).shares), (50, 20))  # the probe reads them while off

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
        await ctl.discover()  # the listing shows it OPEN: the moment kept
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

    async def test_new_listing_later_page_and_duplicate_variant(self):
        ctl, api, _ = controller()
        api.pages[("UP_DOWN", "")] = {"success": True, "data": [], "cursor": None}
        api.pages[("CRYPTO_UP_DOWN", "")] = {"success": True, "data": [{"id": 999}], "cursor": "next"}
        api.pages[("CRYPTO_UP_DOWN", "next")] = {"success": True, "data": [market(), market()], "cursor": None}
        await ctl.discover()
        self.assertEqual(ctl.queue.qsize(), 1)
        await ctl.discover()
        self.assertEqual(ctl.queue.qsize(), 1)
        await self.finish(ctl)

    async def test_discovery_takes_the_soonest_first_and_only_within_the_horizon(self):
        rows = [market(at=NOW + 45 * 60_000, mid=1), market(at=NOW + 15 * 60_000, mid=2), market(at=NOW + 3 * 3600_000, mid=3), market("skhynix", at=NOW + 30 * 60_000, mid=4)]
        ctl, api, _ = controller(rows)
        await ctl.discover()
        self.assertEqual(ctl.queue.qsize(), 3)  # the 3-hour one waits for a later scan
        order = [ctl.queue.get_nowait()[2]["market_id"] for _ in range(3)]
        self.assertEqual(order, ["2", "4", "1"])
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
        bot.preopen = types.SimpleNamespace(alive=True)
        bot.config = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline", "LIVE": "on", "PREDICT_PRIVATE_KEY": "a" * 64})
        self.assertTrue(bot.preopen_active())
        self.assertIn("不开新仓", bot.live_room({}, None, "up", .49, 50))
        self.assertIn("不发送", bot.live_send_block({"live": {}}, {}, NOW))
        bot.preopen.alive = False
        self.assertFalse(bot.preopen_active())  # crashed: the model trader is free again
        bot.preopen.alive = True
        bot.config = cfg  # LIVE=off: paper trading goes on
        self.assertFalse(bot.preopen_active())
        bot.preopen = None
        self.assertFalse(bot.preopen_active())
        self.assertIn("未启用", await bot.cmd_preopen(m.Request("/preopen", [], 1, 0, 1)))

    async def test_probe_reports_fields_and_candidates(self):
        ctl, api, _ = controller([market(), market(at=NOW, mid=5), market("eth", mid=6)])
        lines = await p.probe(api, ctl.owner.config, ctl.settings, NOW)
        text = "\n".join(lines)
        self.assertIn("UP_DOWN：3 行", text)
        self.assertIn("字段：categorySlug、conditionId、feeRateBps、id、isVisible、outcomes、status、tradingStatus", text)
        self.assertIn("发现逻辑要用的字段缺失：makerFeeBps、question", text)
        self.assertIn("现在会挂的：1 场；不挂的原因：", text)
        self.assertIn("已开始或不足 3 分钟×1", text)
        self.assertIn("不是三张 15 分钟卡的链接×1", text)
        api.pages[("CRYPTO_UP_DOWN", "")] = {"success": True, "data": [], "cursor": None}
        self.assertIn("CRYPTO_UP_DOWN：0 行", "\n".join(await p.probe(api, ctl.owner.config, ctl.settings, NOW)))
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
            with self.assertRaises(m.RemoteError): await api.orders()
        self.assertEqual(len(sent), 1)
        self.assertTrue(api.orders_complete)  # no caller receives a successful empty list
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
