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
        self.fail_side = 0
        self.before = None
        self.auth = None

    async def market(self, mid): return copy.deepcopy(self.markets[mid])
    async def ensure_auth(self):
        if self.auth: await self.auth()

    async def create_order(self, body):
        if self.before: await self.before()
        if why := self.ctl.before_send(body): raise p.NotSent(why)
        self.sent.append(copy.deepcopy(body))
        if len(self.sent) == self.fail_side: raise m.RemoteError("timeout: delivery unknown")
        oid = str(len(self.sent))
        signed = body["data"]["order"]
        self.rows[signed["hash"]] = {"id": oid, "order": signed, "amount": signed["takerAmount"], "amountFilled": "0", "status": "OPEN"}
        return {"order_id": oid, "hash": signed["hash"]}

    async def remove_orders(self, ids):
        self.removed.extend(ids)
        for row in self.rows.values():
            if row["id"] in ids: row["status"] = "CANCELLED"
        return {"removed": list(ids), "noop": []}

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
                                  live=types.SimpleNamespace(wallet=wallet, chain=Chain(), ready=True),
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

    async def test_identity_clock_and_all_three(self):
        ctl, api, clock = controller()
        for stem in ("btc", "unitree", "skhynix"):
            self.assertIsNotNone(p.candidate(market(stem), NOW, ctl.owner.config, ctl.settings))
        for row in (market("eth"), market(at=NOW + 120000), market(at=NOW - 900000), market(at=NOW + 86400000),
                    market(status="CANCEL_ONLY"), market(status="CLOSED")):
            self.assertIsNone(p.candidate(row, NOW, ctl.owner.config, ctl.settings))
        row = market(); row["question"] = "10:00AM-10:15AM ET"
        self.assertIsNone(p.candidate(row, NOW, ctl.owner.config, ctl.settings))
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
            self.assertEqual(int(order["expiration"]), c["start_ms"] // 1000 - 30)
            tokens.append(order["tokenId"])
        self.assertEqual(tokens, ["247", "246"])  # reversed metadata maps by name
        self.assertEqual(ctl.risk(), 49.)
        await ctl.place_pair(c)
        self.assertEqual(len(api.sent), 2)
        await self.finish(ctl)

    async def test_failure_cancels_other_side_and_never_resubmits_unknown(self):
        ctl, api, _ = controller()
        api.fail_side = 2
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        self.assertEqual(api.removed, ["1"])
        self.assertEqual(ctl.ledger.rows[c["slug"]]["orders"]["down"]["state"], "unknown")
        self.assertEqual(ctl.risk(), 49.)
        await ctl.place_pair(c)
        self.assertEqual(len(api.sent), 2)
        await self.finish(ctl)

    async def test_pause_during_send_queue_blocks_http(self):
        ctl, api, _ = controller()
        async def pause(): ctl.owner.live_paused = lambda: "paused"
        api.before = pause
        await ctl.place_pair(p.candidate(market(), NOW, ctl.owner.config, ctl.settings))
        self.assertEqual(api.sent, [])
        self.assertEqual(ctl.risk(), 0.)
        await self.finish(ctl)

    async def test_expiry_rechecked_after_auth_delay(self):
        ctl, api, clock = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        async def slow_auth(): clock.ms = c["start_ms"] - 70_000
        api.auth = slow_auth
        await ctl.place_pair(c)
        self.assertEqual(api.sent, [])
        await self.finish(ctl)

    async def test_bulk_cancels_do_not_release_unread_fill_reservation(self):
        ctl, api, clock = controller()
        c = p.candidate(market(), NOW, ctl.owner.config, ctl.settings)
        await ctl.place_pair(c)
        pair = ctl.ledger.rows[c["slug"]]
        clock.ms = c["start_ms"] - 60_000
        await ctl.cancel_many([pair], "deadline")
        self.assertEqual(set(api.removed), {"1", "2"})
        self.assertEqual(pair["phase"], "closed")
        self.assertEqual(ctl.risk(), 49.)
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

    async def test_default_is_off_and_normal_strategy_cannot_open_in_preopen(self):
        cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "offline"})
        self.assertFalse(p.Settings.from_config(cfg).enabled)
        bot = L.LiveBot.__new__(L.LiveBot)
        bot.preopen = object(); bot.config = cfg
        self.assertIn("不开新仓", bot.live_room({}, None, "up", .49, 50))
        self.assertIn("不发送", bot.live_send_block({"live": {}}, {}, NOW))

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
        await ctl.cancel_many([pair], "cancel")
        clock.ms = c["start_ms"] + 20 * 60_000
        api.markets["123"]["outcomes"][0]["status"] = "WON"  # Down
        for row in api.rows.values():
            row["amountFilled"] = row["amount"]  # final records caught up only now
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
