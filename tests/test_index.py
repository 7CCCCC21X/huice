"""1.154.22 — the kept records' index (main.TradeIndex). On 11-18 /diag's ⏱️ 主循环 line said the event loop was held 16% of
the time, a pushed book waiting 8.7 ms at the median and up to 1.9 s — by the model trader's own records: every view of
them (one a pushed book) built its indexes by walking all 12,800, the order gates, the sync and the step walked them again.
Here: the index kept beside the records gives the answers those walks gave — a market's records, a driver's, the open
ones, the orders' — through any sequence of saves, moves and removals, for a view that changes and for one grown stale;
the bot keeps it in step (sim_save, sim_rekey, sim_prune, a cache read afresh) and checks it against the records.

Every other suite checks the same at every call (tests/offline.py: SIM_INDEX_AUDIT).

Run: python tests/test_index.py
"""
import asyncio
import copy
import datetime as dt
import random
import sys
import time
import types

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live as L
import preopen as P

D = m.D
WEI = 10 ** 18
KEY = "0x" + "a" * 64
BJ = lambda mo, d, h, mi=0: int(dt.datetime(2026, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
NOW = BJ(11, 18, 10, 0)
CLOSE = BJ(11, 18, 16, 10)
MARKETS = [f"m{i}" for i in range(14)] + ["slug#7"]
DRIVERS = [f"D{i}" for i in range(5)]
world = {"markets": []}


# --- records of every kind ------------------------------------------------------------------------------------------------------
def record(i: int, rnd: random.Random) -> dict:
    market = rnd.choice(MARKETS)
    status = rnd.choice(["filled", "resting", "cancelled", "cancelled", "expired", "settled", "settled", "settled"])
    t = {"market": market, "slug": market, "kind": "close", "key": "K" + market, "driver": rnd.choice(DRIVERS), "side": rnd.choice(["up", "down"]),
         "status": status, "shares": rnd.choice([0.0, 0.0, 5.0, 100.0]), "order": 100.0, "price": 0.5, "opened": 1000 + i}
    if status == "settled" and rnd.random() < 0.8:
        t["final"] = {"up": 1.0}
    if rnd.random() < 0.75:
        t["live"] = {"state": rnd.choice(["done", "done", "done", "done", "open", "pending", "cancelling", "placing", "unknown"]),
                     "order_id": str(5000 + rnd.randrange(400)) if rnd.random() < 0.8 else "", "hash": f"0x{rnd.randrange(400):064X}" if rnd.random() < 0.9 else "", "events": []}
        if rnd.random() < 0.1:
            t["live"]["fills_pending"] = 123
    return t


def change(t: dict, rnd: random.Random) -> None:
    """A record changed in place, as the bot changes them: filled, withdrawn, settled, its order's state or id. One that can
    never count for its driver again (driver_may_count: settled, lapsed, a paper attempt withdrawn) is not brought back — no
    path of the bot's does that, and the drivers' lists go by it since 1.150.0."""
    what = rnd.randrange(6) if m.driver_may_count(t) else 4
    if what == 0:
        t.update(status="filled", shares=float(rnd.choice([5, 100])))
    elif what == 1:
        t.update(status="cancelled", shares=rnd.choice([0.0, 0.0, 5.0]))
    elif what == 2:
        t.update(status="settled", final={"up": 0.0})
    elif what == 3 and isinstance(t.get("live"), dict):
        t["live"]["state"] = rnd.choice(["done", "open", "cancelling", "unknown"])
    elif what == 4 and isinstance(t.get("live"), dict):
        t["live"].update(order_id=str(5000 + rnd.randrange(400)), hash=f"0x{rnd.randrange(400):064x}")
    elif what == 5:
        t.update(status="resting", shares=0.0)
        t.pop("final", None)


def same(got: list, want: list) -> bool:
    return len(got) == len(want) and all(a is b for a, b in zip(got, want))


def check(view: m.TradeMap, why: str) -> bool:
    """The view's answers against the walks of its own records; whether it knew its open records (open_items)."""
    for market in MARKETS + ["nowhere"]:
        assert same(view.of_market(market), [t for t in view.values() if str(t.get("market") or "") == market]), (why, market)
        assert view.ids_of_market(market) == [k for k, t in view.items() if str(t.get("market") or "") == market], (why, market)
        assert m.trades_of_market(view, market) is view.of_market(market) or same(m.trades_of_market(view, market), view.of_market(market))
    for driver in DRIVERS + ["nobody"]:
        got = view.of_driver(driver)
        if view._driver is None:  # its own index, built on first use: a record that stopped counting since stays listed until the map changes (as ever)
            got = [t for t in got if m.driver_may_count(t)]
        assert same(got, [t for t in view.values() if m.driver_may_count(t) and m.trade_driver(t)[0] == driver]), (why, driver)
    rows = view.open_items()
    if rows is not None:
        mine, walked = [kv for kv in rows if m.trade_open(kv[1])], [kv for kv in view.items() if m.trade_open(kv[1])]
        assert [k for k, _ in mine] == [k for k, _ in walked] and same([t for _, t in mine], [t for _, t in walked]), (why, [k for k, _ in mine][-5:], [k for k, _ in walked][-5:])
    assert [k for k, _ in m.trades_open(view)] == [k for k, _ in rows] if rows is not None else list(m.trades_open(view)) == list(view.items())
    return rows is not None


def in_step(index: m.TradeIndex, why: str) -> None:
    """The index against one built afresh from the same records: every list, every set."""
    fresh = m.TradeIndex(dict(index.records))
    assert index.market == fresh.market and index.driver == fresh.driver, why
    assert index.open == fresh.open and index.open_order() == fresh.open_order(), why
    held = lambda by: {k: sorted(v) for k, v in by.items()}  # (which records hold an order; among several on one id — never in the bot's own records — in no set order)
    assert held(index.by_order) == held(fresh.by_order) and held(index.by_hash) == held(fresh.by_hash) and index.orders == fresh.orders, why
    assert {k: sorted(v) for k, v in index.numbers.items()} == {k: sorted(v) for k, v in fresh.numbers.items()}, why
    assert index.where == fresh.where and index.counting == fresh.counting and list(index.seq) == list(index.records), why
    assert index.drift() == [], why


def the_open_test_covers_every_look() -> None:
    """trade_open is a superset of each look's own test: a record any of them acts on is an open one."""
    rnd = random.Random(7)
    bot = types.SimpleNamespace(touches={}, SIM_SETTLE_MS=0)
    for i in range(6000):
        t = record(i, rnd)
        for _ in range(rnd.randrange(3)):
            change(t, rnd)
        st = t.get("live") if isinstance(t.get("live"), dict) else None
        looked = [
            not (t.get("final") or (t["status"] in {"expired", "cancelled"} and float(t.get("shares") or 0) <= 1e-9)),  # the step's walk (sim_decide)
            m.Bot.sim_due(bot, {**t, "settle": {"close_ms": 1}}, 10 ** 15),                                              # the settlement's confirmation
            st is not None and (t["status"] in ("filled", "resting") or (t["status"] == "cancelled" and bool(t.get("shares")))
                                or st.get("state") in L.PLACING or bool(st.get("fills_pending"))),                       # what is reserved (live_exposure)
            st is not None and st.get("state") == "pending" and t["status"] == "resting",                                # the orders to send
            st is not None and st.get("state") == "cancelling",                                                          # ...to cancel
            st is not None and st.get("state") in L.PLACING,                                                             # ...to follow (live_sync)
            m.sim_in_flight(t) or t["status"] == "filled",                                                               # the feed's hot markets
            m.trade_counts(t),                                                                                           # the caps on money at risk (1.154.40: sim_group_room, sim_window_open_usd)
        ]
        assert not any(looked) or m.trade_open(t), (t, looked)
    assert not m.trade_open({"status": "settled", "final": {"up": 1.0}, "shares": 100.0, "live": {"state": "done"}})
    assert not m.trade_open({"status": "cancelled", "shares": 0.0, "live": {"state": "done"}}) and not m.trade_open({"status": "expired", "shares": 0})
    assert m.trade_open({"status": "cancelled", "shares": 0.0, "live": {"state": "done", "fills_pending": 5}})
    assert m.trade_open({"status": "settled", "shares": 5.0}), "settled on the bot's own data, Predict's word still to come"


def the_index_follows_every_change() -> None:
    rnd = random.Random(20261118)
    records: dict[str, dict] = {}
    for i in range(500):
        t = record(i, rnd)
        key = f"{t['market']}|{t['side']}|{rnd.choice('挂吃')}"
        records[key if key not in records else f"{key}#{i}"] = t
    index = m.TradeIndex(records)
    in_step(index, "built")
    kept: list[tuple[m.TradeMap, int]] = []  # views taken earlier, with the step they were taken at
    knew = fell = 0
    for step in range(900):
        op = rnd.randrange(12)
        keys = list(records)
        if op <= 1:  # a new record saved
            t = record(1000 + step, rnd)
            index.put(f"{t['market']}|{t['side']}|new{step}", t)
        elif op <= 4 and keys:  # a record changed in place, and saved
            k = rnd.choice(keys)
            change(records[k], rnd)
            index.put(k, records[k])
        elif op == 5 and keys:  # moved to tid#n (sim_rekey)
            k = rnd.choice([k for k in keys if "#" not in k.rsplit("|", 1)[-1]] or keys)
            if "#" not in k.rsplit("|", 1)[-1]:
                assert index.top_number(k) == max((int(x.rpartition("#")[2]) for x in records if x.startswith(k + "#") and x.rpartition("#")[2].isdigit()), default=0)
                index.move(k, f"{k}#{index.top_number(k) + 1}", records[k])
        elif op == 6 and keys:  # removed (sim_prune)
            index.drop(rnd.choice(keys))
        elif op == 7 and keys:  # another record under a key kept (never the bot's own doing)
            index.put(rnd.choice(keys), record(5000 + step, rnd))
        elif op == 8:  # a view, changed as a look changes it
            view = m.TradeMap.of(index)
            assert check(view, f"fresh view at {step}") and view.kept() is index
            for _ in range(rnd.randrange(1, 6)):
                vkeys, what = list(view), rnd.randrange(8)
                if what == 0:
                    t = record(9000 + step, rnd)
                    view[f"{t['market']}|{t['side']}|v{step}{rnd.randrange(99)}"] = t
                elif what == 1 and vkeys:  # moved in the view after the cache (sim_quote: sim_rekey, then trades[new] = trades.pop(old))
                    k = rnd.choice(vkeys)
                    if "#" not in k.rsplit("|", 1)[-1] and k in records and view.get(k) is records[k]:
                        new = f"{k}#{index.top_number(k) + 1}"
                        index.move(k, new, records[k])
                        view[new] = view.pop(k)
                        view[k] = record(9500 + step, rnd)  # ...and the slot's fresh order, in the view alone until the look is saved
                elif what == 2 and vkeys:
                    del view[rnd.choice(vkeys)]
                elif what == 3 and vkeys:
                    k = rnd.choice(vkeys)
                    view[k] = view[k]  # the same record again: nothing moves
                elif what == 4 and vkeys:
                    k = rnd.choice(vkeys)
                    change(view[k], rnd)
                    if k in records and records[k] is view[k]:
                        index.put(k, records[k])  # (changed and saved, as the bot does)
                elif what == 5 and vkeys and rnd.random() < 0.3:
                    view[rnd.choice(vkeys)] = record(9900 + step, rnd)  # another record under a key it has: its own indexes from here on
                    assert view.kept() is None
                elif what == 6 and rnd.random() < 0.3:
                    view.update({f"u{step}": record(9950 + step, rnd)})
                    assert view.setdefault(f"u{step}", {}) is view[f"u{step}"] and view.pop("never", None) is None
                elif what == 7 and vkeys and rnd.random() < 0.2:
                    view.popitem()
                check(view, f"view changed at {step}")
            assert view.taken is None or not view._added
            kept.append((view, step))
        elif op == 9:  # a view kept across other work (a task that waited): its answers are still its own records'
            kept.append((m.TradeMap.of(index), step))
        if step % 3 == 0:
            in_step(index, f"step {step} op {op}")
        for view, taken in kept[-6:]:
            if check(view, f"view of {taken} at {step}"):
                knew += 1
            else:
                fell += 1
    assert knew > 300 and fell > 20, (knew, fell)
    # a view of records read afresh (the cache replaced): it goes by its own walk
    stale = m.TradeMap.of(index)
    index.dead = True
    assert stale.open_items() is None and stale.kept() is None and check(stale, "dead") is False
    # a hand-made map has no index kept for it: built on first use, dropped at every change — as every view was before
    plain = m.TradeMap({k: t for k, t in list(records.items())[:50]})
    assert plain.open_items() is None and plain.kept() is None and check(plain, "by hand") is False
    plain["x|up|吃"] = record(1, rnd)
    plain.pop(next(iter(plain)))
    check(plain, "by hand, changed")
    assert same(m.TradeMap(plain).copy().of_market("m1"), plain.of_market("m1"))
    # a record moved by another's hand since the view was taken: the view still has it under its slot, and says it cannot tell
    index2 = m.TradeIndex({"m1|up|挂": {"market": "m1", "status": "filled", "shares": 5.0, "driver": "D1"}, "m2|up|挂": {"market": "m2", "status": "resting", "driver": "D1"}})
    old, fresh = m.TradeMap.of(index2), None
    index2.move("m1|up|挂", "m1|up|挂#1", index2.records["m1|up|挂"])
    fresh = m.TradeMap.of(index2)
    assert old.open_items() is None and [k for k, _ in fresh.open_items()] == ["m2|up|挂", "m1|up|挂#1"]
    assert [k for k, _ in m.trades_open(old)] == ["m1|up|挂", "m2|up|挂"], "its own walk: every record it holds"
    assert old.ids_of_market("m1") == ["m1|up|挂"] and fresh.ids_of_market("m1") == ["m1|up|挂#1"], "each view its own records' lists"
    # the orders' index: which records hold one of the listed orders, in the cache's order
    rows_by_id = {str(5000 + i): {"id": str(5000 + i)} for i in range(0, 400, 7)}
    rows_by_hash = {f"0x{i:064x}": {"id": "h"} for i in range(0, 400, 11)}
    walked = [k for k, t in records.items() if isinstance(t.get("live"), dict)
              and (rows_by_id.get(str(t["live"].get("order_id") or "-")) or rows_by_hash.get(str(t["live"].get("hash") or "-").lower())) is not None]
    assert index.holding(rows_by_id, rows_by_hash) == walked and walked, len(walked)
    assert index.holding({}, {}) == [] and index.in_order(reversed(walked)) == walked


# --- the bot keeps it in step ---------------------------------------------------------------------------------------------------
class FakeApi:
    def __init__(self):
        self.jwt, self.jwt_exp, self.auth_error, self.lookup = "t", 0, "", ""
        self.markets, self.open, self.positions_rows = {}, {}, []
        self.next_id, self.orders_complete = 100, True

    async def ensure_auth(self): pass
    async def market(self, mid): return self.markets[str(mid)]

    async def create_order(self, body):
        self.next_id += 1
        oid, data = str(self.next_id), body["data"]
        self.open[oid] = {"id": oid, "status": "OPEN", "amount": data["order"]["takerAmount"], "amountFilled": "0", "strategy": data["strategy"], "order": {**data["order"]}}
        return {"order_id": oid, "hash": data["order"]["hash"], "code": None}

    async def remove_orders(self, ids):
        removed = [i for i in ids if self.open.pop(i, None) is not None]
        return {"removed": removed, "noop": [i for i in ids if i not in removed]}

    async def orders(self, status="OPEN", **params):
        return [dict(r) for r in self.open.values()] if status == "OPEN" else []

    async def order(self, oid, hash_=""):
        row = self.open.get(oid)
        return dict(row) if row is not None else None

    async def positions(self): return self.positions_rows


class FakeChain:
    def __init__(self): self.owner = "0x8fd379246834eac74B8419FfdA202CF8051F7A03"
    async def account_owner(self, account): return self.owner
    async def usdt_balance(self, owner=None): return 100000 * WEI
    async def bnb_balance(self, owner=None): return 10 ** 17
    async def allowance(self, key, owner=None): return 10 ** 30


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


def mk(slug, fair, bids, asks, at, mid):
    settle = {"key": "K" + mid, "target": "2026-11-18", "line": 24600.0, "close_ms": CLOSE}
    side = lambda rows: tuple((D(str(p)), D(str(q))) for p, q in rows)
    book = m.PredictBook("K" + mid, slug, mid, "t", side(bids), side(asks), at, None)
    return m.SimMarket(slug, "恒生指数", "close", "K" + mid, fair, book, 0.03, "", ("涨", "跌"), settle,
                       {"basis": {"fair_up": fair, "ref": 24600.0}, "sources": [{"what": "恒指期货", "source": "etnet"}], "proxy": None})


def make_bot(**env):
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
                             "SIM_WAYS": "taker", "SIM_MARKETS": "all", "LIVE": "on", "PREDICT_PRIVATE_KEY": KEY, "PREDICT_API_KEY": "k",
                             "SIM_TAKER_MAX_SHARES": "0", "SIM_TAKER_SWEEP_SHARES": "0", "SIM_TAKER_HOLD_SECONDS": "0", "SIM_OPEN_QUIET_MINUTES": "0",
                             "SIM_GROUP_USD": "0", "LIVE_TAKER_WAIT_SECONDS": "600", "LIVE_MAX_ORDER_USD": "100", "LIVE_MAX_OPEN_USD": "100000",
                             "LIVE_MAX_DAILY_LOSS_USD": "500", **env})
    bot = L.LiveBot(cfg, m.Store(":memory:"), FM(NOW), None)
    bot.live.api, bot.live.chain = FakeApi(), FakeChain()
    bot.sim_markets = lambda now: world["markets"]
    bot.sim_markets_for = lambda key, now: world["markets"]
    return bot


async def taker(bot, at, mid):
    slug = f"hsi-{mid}"
    bot.live.api.markets[mid] = {"id": int(mid), "status": "REGISTERED", "isNegRisk": False, "isYieldBearing": False, "feeRateBps": 200, "conditionId": "0x" + "ab" * 32,
                                 "outcomes": [{"name": "Up", "indexSet": 1, "onChainId": f"{mid}1"}, {"name": "Down", "indexSet": 2, "onChainId": f"{mid}2"}]}
    world["markets"] = [mk(slug, 0.70, [], [("0.58", "400")], at, mid)]
    bot.market.now, bot.sim_ran = at, -1e9
    await bot.live_prefetch(at)
    await bot.sim_step(at)
    return f"{slug}|up|吃"


async def the_bot_keeps_it_in_step() -> None:
    bot = make_bot()
    await bot.live_prepare()
    tids = [await taker(bot, NOW + i * 5000, str(101 + i)) for i in range(3)]
    index = bot.sim_index
    assert index is not None and index.records is bot.sim_cache and set(tids) <= index.open, index.open
    in_step(index, "three orders placed")
    bot.reference_state["模拟交易"] = {"ok_at": time.time() - 2, "error": "", "error_at": 0, "ms": 900, "runs": 50}
    diag = lambda: "\n".join(bot.diag_state(NOW))
    assert bot.sim_step_info["open"] == len(index.open) == 3 and "3 条记录（其中 3 条还没结束，每步只看这些）" in diag() and "｜订单签名：" in diag(), diag()
    # the history: thousands of finished records beside them — none of them open, each market and driver still found
    seed, old = bot.sim_trades()[tids[0]], {}
    for i in range(3000):
        t = copy.deepcopy(seed)
        t["market"] = t["slug"] = f"old-{i // 4}"
        t["live"].update(state="done", order_id=str(900000 + i), hash="0x" + f"{i:064x}")
        if i % 10 < 6:
            t.update(status="cancelled", shares=0.0)
        else:
            t.update(status="settled", shares=100.0, payout=1.0, settled=NOW - 3600_000, confirm="confirmed", final={"up": 1.0, "name": "Up", "status": "RESOLVED"})
        old[f"old-{i // 4}|up|吃#{i % 4 + 1}"] = t
    bot.sim_save(list(old.items()))
    assert bot.sim_index is index and len(index.open) == 3 and len(bot.sim_trades()) == 3003
    in_step(index, "history saved")
    view = bot.sim_trades()
    assert [k for k, _ in view.open_items()] == tids and view.kept() is index and len(view.of_market("old-5")) == 4
    assert bot.live_exposure(view) == sum(bot.live_reserved(t) for t in view.values()) > 0
    # looking at the records is no walk of them: an express look, the order's gates, the lists of what to send and to cancel
    seen = []
    values, items = m.TradeMap.values, m.TradeMap.items
    m.TradeMap.values = lambda self: seen.append("values") or values(self)
    m.TradeMap.items = lambda self: seen.append("items") or items(self)
    audit, m.TRADE_AUDIT = m.TRADE_AUDIT, False  # (the audit itself walks: off for this count)
    try:
        world["markets"] = [mk("hsi-101", 0.70, [], [("0.90", "400")], NOW + 20_000, "101")]
        bot.market.now = NOW + 20_000
        await bot.sim_express_look("K101", NOW + 20_000)
        bot.live_exposure(bot.sim_trades())
        bot.live_send_block(seed, bot.sim_trades(), NOW + 20_000)
        bot.live_cancels_unsent()
        await bot.live_place_pending(NOW + 20_000)
        assert seen == [], seen
    finally:
        m.TradeMap.values, m.TradeMap.items, m.TRADE_AUDIT = values, items, audit
    # sim_rekey: the slot's record to #n — past the highest number its attempts hold — the index with it
    moved = bot.sim_rekey(tids[0], bot.sim_cache[tids[0]])
    assert moved == tids[0] + "#1" and tids[0] not in bot.sim_cache and index.where[moved] == "hsi-101" and moved in index.open
    assert bot.sim_rekey(moved, bot.sim_cache[moved]) == moved, "a record already numbered keeps its key"
    assert index.top_number("old-5|up|吃") == 4 and bot.sim_rekey("old-5|up|吃", copy.deepcopy(old["old-5|up|吃#1"])) == "old-5|up|吃#5"
    in_step(index, "moved")
    assert view.open_items() is None, "the view taken before the move: its own walk"
    assert [k for k, _ in bot.sim_trades().open_items()] == [tids[1], tids[2], moved]
    # sim_prune: the attempts beyond SIM_ATTEMPTS_KEEP go, from the index too
    hold, type(bot).SIM_ATTEMPTS_KEEP = type(bot).SIM_ATTEMPTS_KEEP, 2
    try:
        pruned = bot.sim_prune()
    finally:
        type(bot).SIM_ATTEMPTS_KEEP = hold
    assert pruned > 100 and len(bot.sim_cache) == 3004 - pruned
    in_step(index, "pruned")
    # a record changed and not saved: the minute's check finds it, says so and puts the index right
    cold = next(k for k, t in bot.sim_cache.items() if t["status"] == "settled")
    bot.sim_cache[cold].update(status="filled")
    bot.sim_cache[cold].pop("final")
    assert cold not in index.open and bot.sim_index_check() == [], "not before its minute"
    notes = []
    bot.log_limited = lambda key, text, *a, **k: notes.append(text)
    bot.sim_index_checked = time.monotonic() - bot.SIM_INDEX_CHECK_SECONDS - 1
    assert bot.sim_index_check() == [cold] and cold in index.open and "记录索引自检：1 条记录的状态改了但索引没跟上" in notes[0], notes
    assert bot.sim_index_drift["n"] == 1 and bot.sim_index_check(force=True) == [] and "；记录索引自检 1 条没跟上（" in diag() and "），已更正" in diag(), diag()
    in_step(index, "checked")
    # read afresh — a sim: record written past sim_save, or the cache dropped: a new index, the old one over
    before = bot.sim_trades()
    bot.store.put("sim:written|up|吃", {**copy.deepcopy(seed), "market": "written", "status": "resting"})
    after = bot.sim_trades()
    assert bot.sim_index is not index and index.dead and "written|up|吃" in bot.sim_index.open and before.open_items() is None and after.kept() is bot.sim_index
    in_step(bot.sim_index, "read afresh")
    gone = bot.sim_index
    bot.sim_cache = None
    assert gone.dead and bot.sim_index is None and after.open_items() is None and len(bot.sim_trades()) == len(after)
    # a cache put in by hand has its index made at the next look; one saved to before any look has none, and none is needed
    bot.sim_cache = {"m1|up|吃": {"market": "m1", "status": "resting", "driver": "D1"}}
    bot.sim_gen = bot.store.touched.get("sim", 0)
    bot.sim_save([("m2|up|吃", {"market": "m2", "status": "filled", "shares": 5.0, "driver": "D1"})])
    assert bot.sim_index is None and [k for k, _ in bot.sim_trades().open_items()] == ["m1|up|吃", "m2|up|吃"]


async def the_orders_are_found_by_the_index() -> None:
    """live_readopt and live_note_unknown go by the orders' index: an order on the list is found on its record without a walk,
    a listed order no record holds is still unrecorded, and the pre-open snapshot's answer is kept until an order's id or hash
    is written to a record."""
    bot = make_bot(LIVE_ORPHAN_CANCEL="off")
    await bot.live_prepare()
    tid = await taker(bot, NOW, "101")
    rec = bot.sim_trades()[tid]
    oid, digest = rec["live"]["order_id"], rec["live"]["hash"]
    index = bot.sim_index
    assert index.by_order[oid] == [tid] and index.by_hash[digest.lower()] == [tid] and index.orders[tid] == (oid, digest.lower())
    # ended here, still on Predict's list: taken back
    rec["live"].update(state="done", final="cancelled")
    rec.update(status="cancelled", shares=0.0, withdrawn={"at": NOW, "why": "测试"})
    bot.sim_save([(tid, rec)])
    assert tid not in index.open
    row = {"id": oid, "status": "OPEN", "amount": str(100 * WEI), "amountFilled": "0", "order": {"hash": digest}}
    notes = []
    bot.live_notify = lambda text, key="": notes.append(key)
    bot.live_readopt([row], {oid: row}, {digest.lower(): row}, NOW + 60_000)
    assert rec["live"]["state"] == "cancelling" and tid in index.open and notes == [f"readopt:{tid}"], (rec["live"]["state"], notes)
    # an order no record holds is unrecorded; the record's own is not
    stray = {"id": "777", "status": "OPEN", "amount": str(5 * WEI), "amountFilled": "0", "order": {"hash": "0x" + "9" * 64}}
    bot.live_note_unknown([row, stray], NOW + 61_000)
    assert [u["id"] for u in bot.live_unknown_open] == ["777"]
    # the snapshot's answer: worked out once, again only when an order's id or hash is written
    rows = [row, stray]
    held = bot.live_snapshot_view(rows, NOW + 62_000)
    calls = []
    holding = m.TradeIndex.holding
    m.TradeIndex.holding = lambda self, a, b: (calls.append(1) if a is held["by_id"] else None) or holding(self, a, b)
    try:
        for _ in range(3):
            bot.live_readopt(rows, held["by_id"], held["by_hash"], NOW + 63_000, NOW + 62_000)
        assert len(calls) == 1 and held["mine"][2] == [tid]
        other = await taker(bot, NOW + 70_000, "102")  # (an order more: its hash and id written to its record)
        for _ in range(2):
            bot.live_readopt(rows, held["by_id"], held["by_hash"], NOW + 71_000, NOW + 62_000)
        assert len(calls) == 2 and other in bot.sim_index.open, calls
    finally:
        m.TradeIndex.holding = holding
    # records from elsewhere (a stand-in's sim_trades): walked, as before
    saved: list = []
    plain = types.SimpleNamespace(sim_trades=lambda: {"a": {"live": {"state": "done", "order_id": "5", "hash": "0xab", "events": []}, "status": "settled",
                                                            "item": "恒指", "label": "挂涨"}},
                                  live_snapshot_held=None, __dict__={}, sim_save=saved.extend, live_notify=lambda *a, **k: None)
    L.LiveBot.live_readopt(plain, [], {"5": {"id": "5"}}, {}, NOW)
    # (settled: not tracked again — but its order still on the open list is withdrawn, 1.154.76: an outside audit, P0-1)
    assert [(k, t["live"]["state"], t["status"]) for k, t in saved] == [("a", "cancelling", "settled")], saved
    # the record's last change, kept between the minute's checks (live_updated)
    assert bot.live_updated(tid, rec) == m.trade_updated(rec)
    calls = []
    updated = m.trade_updated
    m.trade_updated = L.core.trade_updated = lambda t: calls.append(1) or updated(t)
    try:
        assert bot.live_updated(tid, rec) == updated(rec) and calls == [], "as kept"
        rec["live"]["events"].append({"at": NOW + 999_000, "what": "测试"})
        assert bot.live_updated(tid, rec) == NOW + 999_000 and calls == [1], "an event more: worked out again"
        assert bot.live_updated(tid, copy.deepcopy(rec)) == NOW + 999_000 and calls == [1, 1], "another record under the key"
    finally:
        m.trade_updated = L.core.trade_updated = updated


def the_ledgers_fills_and_unverified() -> None:
    """The pre-open ledger's own two lists (1.154.22): the records that filled anything and those with an order whose fills
    are not known for certain — what the model trader's positions check asks once a minute, as the walks of every record gave it."""
    rnd = random.Random(5)
    ledger = P.Ledger(":memory:")
    states = ["prepared", "open", "final", "final", "final", "cancelled", "not_sent", "unknown", "placing", "rejected"]

    def pair(i: int) -> dict:
        start = NOW + (rnd.randrange(-40, 3)) * m.DAY_MS + i
        return {"slug": f"w{i}", "key": "BTC-15M", "market_id": str(100 + i % 40) if i % 23 else "", "start_ms": start, "end_ms": start + 900_000, "phase": "active",
                "price": "0.47", "shares": "30", "level": i % 7, "attempt": 0, "window": f"win{i // 7}", "accounted": rnd.random() < 0.5,
                "orders": {s: {"hash": f"0x{i:060x}{s[:2].encode().hex()}", "id": str(i * 2 + n), "state": rnd.choice(states), "index_set": rnd.choice([n + 1, n + 1, 0]),
                               "filled": rnd.choice([0.0, 0.0, 0.0, 12.5, 30.0, 0.37038])} for n, s in enumerate(("up", "down"))}}
    for i in range(1500):
        row = pair(i)
        ledger.save(row)
    rows = lambda: list(ledger.rows.values())
    agree = lambda why: (P.fills_by_outcome(ledger.filled_rows(), NOW, 30) == P.fills_by_outcome(rows(), NOW, 30)
                         and list(P.fills_by_outcome(ledger.filled_rows(), NOW, 30).items()) == list(P.fills_by_outcome(rows(), NOW, 30).items())
                         and P.unverified_outcomes(ledger.unsure_rows(), NOW) == P.unverified_outcomes(rows(), NOW)) or why
    assert agree("loaded") is True and 0 < len(ledger.filled) < 1500 and 0 < len(ledger.unsure) < 1500
    for i in rnd.sample(range(1500), 400):  # read, filled, verified, removed — and saved, as the mode does
        row = ledger.rows.get(f"w{i}")
        if row is None:
            continue
        what = rnd.randrange(4)
        if what == 0:
            for o in row["orders"].values():
                o.update(state="final", filled=rnd.choice([0.0, 30.0]))
        elif what == 1:
            row["orders"]["up"].update(state="open", filled=0.0)
        elif what == 2:
            ledger.delete(row["slug"])
            continue
        ledger.save(row)
    assert agree("changed") is True
    ledger.rows["by-hand"] = pair(99999)  # put in by hand (a test's): the lists are made again before they answer
    assert agree("by hand") is True
    ledger.close()


def the_diag_lines() -> None:
    """/diag: ten of the places that held the loop and what the rest come to; what the feed's REST reads are of; the signer."""
    watch = m.LoopWatch()
    now = time.monotonic()
    watch.started = now - 600
    for i in range(14):
        for _ in range(i + 1):
            watch.stalls.append((now - 5, 150.0 + i * 10, f"main.f{i} ← main.g"))
            watch.lags.append((now - 5, 150.0 + i * 10))
    line = watch.line()
    assert line.count(" 次共 ") == 10 and "；另有 4 处共 10 次 1.7 秒（" in line and "main.f13 ← main.g 14 次" in line and "main.f3 ← main.g" not in line, line
    for url, kind in (("https://api.predict.fun/v1/markets/123/orderbook", "盘口"), ("https://api.predict.fun/v1/markets/123", "市场详情"),
                      ("https://api.predict.fun/v1/categories/btc-updown-15m-1", "分类"), ("https://api.predict.fun/v1/markets?first=50", "市场列表"),
                      ("https://api.predict.fun/v1/markets/123/statistics", "其他")):
        assert m.predict_rest_kind(url) == kind, (url, m.predict_rest_kind(url))
    feed = m.PredictFeed.__new__(m.PredictFeed)
    feed.rest_kinds = [(time.monotonic() - 90, "盘口")] + [(time.monotonic(), k) for k in ["市场详情"] * 3 + ["盘口"] * 5 + ["分类"]]
    assert list(feed.rest_kinds_last().items()) == [("盘口", 5), ("市场详情", 3), ("分类", 1)]
    bot = types.SimpleNamespace(live_rate_samples=[], predict=types.SimpleNamespace(rate=lambda: 1.8, rest_kinds_last=feed.rest_kinds_last), preopen=None,
                                book_own_key=lambda: "")
    assert L.LiveBot.live_request_rates(bot) == ["盘口 1.8（最近一分钟 9 次：盘口 5、市场详情 3、分类 1）"]
    note = L.LiveBot.live_signer_note(types.SimpleNamespace(config=types.SimpleNamespace(live_mode="on")))
    assert note.startswith("订单签名：") and ("原生库" in note or "纯 Python" in note), note
    assert L.LiveBot.live_signer_note(types.SimpleNamespace(config=types.SimpleNamespace(live_mode="off"))) == ""


def old_window_open_usd(bot, trades, key):
    """Bot.sim_window_open_usd as it was written before 1.154.40: a walk of every record."""
    total = 0.0
    for t in trades.values():
        if t.get("key") != key or not (t.get("settle") or {}).get("window"):
            continue
        unfilled = m.sim_open_order(t)
        if t.get("status") not in {"filled", "resting"} and not unfilled:
            continue
        price = float(t.get("price") or 0)
        total += price * float(t.get("shares") or 0)
        if unfilled:
            total += bot.sim_resting_cost(t) * max(float(t.get("order") or 0) - float(t.get("shares") or 0), 0.0)
    return total


def old_record_of_order(trades, order_id):
    """LiveBot.live_cancel_fills_read's search for its record, as it was written before 1.154.40."""
    return next(((k, t) for k, t in trades.items() if isinstance(t.get("live"), dict) and str(t["live"].get("order_id") or "") == order_id), None)


async def the_caps_go_by_the_open_records() -> None:
    """1.154.40: the group cap (sim_group_room), a card's windows' money (sim_window_open_usd) and the read of a cancelled
    order's fills (live_cancel_fills_read → trade_of_order) look at the open records and the orders' index — where the first
    walked its driver's whole list (every real attempt ever withdrawn stays on it), the second every record, the third every
    record again for the one that holds an order. The answers are the walks', to the digit and to the object."""
    rnd = random.Random(40)
    bot = make_bot(SIM_GROUP_USD="300")
    await bot.live_prepare()
    tids = [await taker(bot, NOW + i * 5000, str(101 + i)) for i in range(3)]
    seed, old = bot.sim_trades()[tids[0]], {}
    for i in range(3000):  # the history: on the first order's driver, three in five a real attempt withdrawn (kept on the driver's list for good)
        t = copy.deepcopy(seed)
        t["market"] = t["slug"] = f"old-{i // 4}"
        t["live"].update(state="done", order_id=str(900000 + i), hash="0x" + f"{i:064x}")
        if i % 10 < 6:
            t.update(status="cancelled", shares=0.0)
        else:
            t.update(status="settled", shares=100.0, payout=1.0, settled=NOW - 3600_000, confirm="confirmed", final={"up": 1.0, "name": "Up", "status": "RESOLVED"})
        old[f"old-{i // 4}|up|吃#{i % 4 + 1}"] = t
    # a card's 15-minute windows: positions, orders resting, in flight, being cancelled, their fills still to be read — and the ended ones
    windows = {}
    for i in range(400):
        t = copy.deepcopy(seed)
        t["market"] = t["slug"] = f"btc-15m-{i // 2}"
        t.update(key="BTC-15M", driver="BTCUSDT", driver_name="BTC", settle={"key": "BTC-15M", "window": True, "pair": "BTCUSDT", "close_ms": CLOSE},
                 price=rnd.choice([0.47, 0.5, 0.613, 0.07]), order=rnd.choice([10.0, 33.3, 100.0]))
        kind = i % 40
        if kind == 0:
            t.update(status="resting", shares=0.0); t["live"].update(state="open", order_id=str(700000 + i))
        elif kind == 1:
            t.update(status="filled", shares=round(t["order"] * rnd.random(), 3)); t["live"].update(state="done", order_id=str(700000 + i))
        elif kind == 2:
            t.update(status="cancelled", shares=3.0); t["live"].update(state="cancelling", order_id=str(700000 + i))
        elif kind == 3:
            t.update(status="cancelled", shares=0.0); t["live"].update(state="done", order_id=str(700000 + i), fills_pending=time.time() * 1000)
        elif kind == 4:
            t.update(status="filled", shares=t["order"]); t["live"].update(state="done", order_id=str(700000 + i))
        elif kind < 30:
            t.update(status="cancelled", shares=0.0); t["live"].update(state="done", order_id=str(700000 + i))
        else:
            t.update(status="settled", shares=t["order"], payout=1.0, settled=NOW - 60_000, confirm="confirmed", final={"up": 1.0, "name": "Up", "status": "RESOLVED"})
            t["live"].update(state="done", order_id=str(700000 + i))
        t["live"]["hash"] = "0x" + f"{10 ** 6 + i:064x}"
        windows[f"btc-15m-{i // 2}|{'up' if i % 2 else 'down'}|吃"] = t
    bot.sim_save(list(old.items()) + list(windows.items()))
    view = bot.sim_trades()
    driver = m.trade_driver(seed)[0]
    assert view.open_items() is not None and len(m.trades_of_driver(view, driver)) > 1500, "the driver's list keeps every real attempt withdrawn"
    counts = lambda rows: [t for t in rows if m.trade_counts(t)]
    for name in (driver, "BTCUSDT", "nobody"):
        mine, whole = m.trades_open_of_driver(view, name), m.trades_of_driver(view, name)
        assert len(counts(mine)) == len(counts(whole)) and all(a is b for a, b in zip(counts(mine), counts(whole))), name
        assert len(mine) <= len(view.open_items()) and all(m.driver_may_count(t) and m.trade_driver(t)[0] == name for t in mine)
    assert counts(m.trades_open_of_driver(view, driver)) == [seed] and len(counts(m.trades_open_of_driver(view, "BTCUSDT"))) == 50
    # the cap's own answer, for a candidate that fits and one that does not: the walk's words
    def the_walks_answer(*args):
        new, m.trades_open_of_driver = m.trades_open_of_driver, m.trades_of_driver
        try:
            return bot.sim_group_room(view, *args)
        finally:
            m.trades_open_of_driver = new
    btc = m.SimMarket("btc-15m-9999", "BTC 15 分钟", "updown", "BTC-15M", 0.5, None, 0.03, "", ("涨", "跌"), {"key": "BTC-15M", "window": True, "pair": "BTCUSDT", "close_ms": CLOSE}, {})
    assert m.market_driver(btc.kind, btc.key, btc.settle, btc.item)[0] == "BTCUSDT"
    hsi = mk("hsi-101", 0.70, [], [("0.58", "400")], NOW, "101")
    answers = []
    for market in (hsi, btc):
        for shares in (1.0, 100.0, 5000.0, 10 ** 6):
            for side in ("up", "down"):
                got = bot.sim_group_room(view, market, side, 0.58, shares)
                assert got == the_walks_answer(market, side, 0.58, shares), (market.market, side, shares, got)
                answers.append(bool(got))
    assert True in answers and False in answers, "(candidates that fit and candidates the cap refuses)"
    # ...and it looks at the open records alone: no record of the history is asked whether its order may still fill
    asked, plain_open = [], m.sim_open_order
    m.sim_open_order = lambda t: asked.append(t) or plain_open(t)
    audit, m.TRADE_AUDIT = m.TRADE_AUDIT, False  # (the audit itself walks the driver's list: off for these counts)
    walks, values, items = [], m.TradeMap.values, m.TradeMap.items
    m.TradeMap.values = lambda self: walks.append("values") or values(self)
    m.TradeMap.items = lambda self: walks.append("items") or items(self)
    try:
        bot.sim_group_room(view, hsi, "up", 0.58, 100.0)
        assert len(asked) == 1 and asked[0] is seed, len(asked)
        asked.clear()
        bot.sim_group_room(view, btc, "up", 0.58, 100.0)
        assert 0 < len(asked) <= len(view.open_items()) < 200, (len(asked), len(view.open_items()))
        # a card's windows' money: the open records, and the sum the walk of every record gave
        assert walks == []
        total = bot.sim_window_open_usd(view, "BTC-15M")
        assert walks == [] and total > 0 and repr(total) == repr(old_window_open_usd(bot, view, "BTC-15M")), (total, walks)
        assert bot.sim_window_open_usd(view, "ETH-15M") == 0.0 == old_window_open_usd(bot, view, "ETH-15M")
        walks.clear()
        # the record of an order: by the orders' index, the same record the walk finds first
        found = m.trade_of_order(view, "700002")
        assert walks == [] and found is not None and found[1]["live"]["order_id"] == "700002" and found[1]["live"]["state"] == "cancelling"
        for oid in ("900000", "902999", "700399", seed["live"]["order_id"]):
            got = m.trade_of_order(view, oid)
            assert walks == [] and got is not None and got[1] is view[got[0]], oid
            want = old_record_of_order(view, oid)
            assert got[0] == want[0] and got[1] is want[1], oid
            walks.clear()
        assert m.trade_of_order(view, "no-such-order") is None and m.trade_of_order(view, "") is None, "(none: the walk says so)"
        walks.clear()
        # an order id two records hold (an order adopted back onto an older record): the first in the map's order, as the walk
        twin = copy.deepcopy(windows["btc-15m-0|down|吃"])
        twin["live"]["order_id"] = "900007"
        bot.sim_save([("btc-15m-0|down|吃", twin)])
        fresh = bot.sim_trades()
        walks.clear()
        got = m.trade_of_order(fresh, "900007")
        assert walks == [] and got[0] == old_record_of_order(fresh, "900007")[0] == "old-1|up|吃#4", got[0]
        # an id written onto a record and not saved yet is not in the index: found by the walk, as before
        fresh["btc-15m-1|down|吃"]["live"]["order_id"] = "fresh-id"
        walks.clear()
        got = m.trade_of_order(fresh, "fresh-id")
        assert got is not None and got[0] == "btc-15m-1|down|吃" and walks == ["items"]
        # a map that is not the kept records as they stand (its holder put a record in), or no view at all: the walk
        fresh["scratch|up|吃"] = {"market": "scratch", "status": "resting", "live": {"state": "open", "order_id": "scratch-1"}}
        assert fresh.kept() is None and m.trade_of_order(fresh, "scratch-1")[0] == "scratch|up|吃" and m.trade_of_order(fresh, "900000")[0] == old_record_of_order(fresh, "900000")[0]
        plain = dict(view)
        assert m.trade_of_order(plain, "900000")[1] is old_record_of_order(plain, "900000")[1] and m.trades_open_of_driver(plain, driver) == m.trades_of_driver(plain, driver)
        assert repr(bot.sim_window_open_usd(plain, "BTC-15M")) == repr(old_window_open_usd(bot, plain, "BTC-15M"))
    finally:
        m.sim_open_order, m.TRADE_AUDIT, m.TradeMap.values, m.TradeMap.items = plain_open, audit, values, items
    # with the audit on (every suite), each of the three is checked against its walk at every call; none differed
    for name in (driver, "BTCUSDT"):
        m.trades_open_of_driver(bot.sim_trades(), name)
    assert m.trade_of_order(bot.sim_trades(), "700004") is not None and not m.TRADE_AUDIT_FAILS, m.TRADE_AUDIT_FAILS[:3]
    # the read of a cancelled order's fills finds its record without a walk of them all
    order_id = seed["live"]["order_id"]
    bot.live.api.open[order_id].update(status="CANCELLED", amountFilled="0")
    audit, m.TRADE_AUDIT = m.TRADE_AUDIT, False
    m.TradeMap.items = lambda self: walks.append("items") or items(self)
    walks.clear()
    try:
        await bot.live_cancel_fills_read(order_id, seed["live"]["hash"], None)
        assert "items" not in walks, walks
    finally:
        m.TradeMap.items, m.TRADE_AUDIT = items, audit


async def main():
    the_open_test_covers_every_look()
    the_index_follows_every_change()
    await the_bot_keeps_it_in_step()
    await the_caps_go_by_the_open_records()
    await the_orders_are_found_by_the_index()
    the_ledgers_fills_and_unverified()
    the_diag_lines()
    assert not m.TRADE_AUDIT_FAILS, m.TRADE_AUDIT_FAILS[:3]


asyncio.run(main())
print("INDEX_OK")
