"""Paper trading as a review system. Whenever a card suggests a trade whose net edge reaches 10¢, 100 shares are bought on
paper (the best maker and the best taker apart, once per market and side), and every trade keeps its evidence:
- a taker is judged on the very fill it gets for those 100 shares (signal, cost and net edge are one figure; the card's
  own $100 view is kept beside it), and a thin book only fills what it holds;
- a resting order fills only as far as sellers show at or through its price (presumed, 推定成交: the most ever seen,
  never a sum of looks), partially if that is all; the expectation is kept at the order and at each fill;
- each trade records the model's inputs, the price sources (real source, code, quote and read times), proxy and anchor,
  the book, the code version and settings, at the order and at each fill;
- results are pre-settled on the bot's own data (预结算), then confirmed by Predict (已确认) or corrected (结果不一致),
  every change kept as a revision; touch markets need the whole window read, ladders a complete history."""
import asyncio, csv, io, os, sys, json, time, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
D = m.D
BJ = lambda mo, d, h, mi=0: int(dt.datetime(2026, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
NOW = BJ(10, 5, 10, 0)
HSI_SLUG = "hang-seng-index-up-or-down-on-october-5-2026"
KOSPI_SLUG = "kospi-composite-index-up-or-down-on-october-5-2026"
CLOSE = BJ(10, 5, 16, 10)
LEGACY = {"SIM_MAKER_MIN_BID": "0", "SIM_MAKER_EXIT_CENTS": "0", "SIM_MAKER_SESSION": "off", "SIM_MAKER_POINTS": "off",
          "SIM_MAKER_SPREAD_CENTS": "10", "SIM_TAKER_MAX_SHARES": "0", "SIM_TAKER_SWEEP_SHARES": "0"}  # the resting-order rules off and the old 10¢ spread rule on: orders rest as placed
FEE = 0.02  # Predict: 2% × min(p, 1 − p) when the market states no rate

# --- settings ----------------------------------------------------------------------------------------------------------
c = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"})
assert c.sim and c.sim_edge == 0.10 and c.sim_shares == 100
assert c.sim_ways == "taker" and c.sim_markets == frozenset({"close"}) and m.sim_scope(c) == ("只吃单", "指数/个股日涨跌")  # the defaults
c = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM": "off", "SIM_EDGE_CENTS": "15", "SIM_SHARES": "250", "SIM_WAYS": "Both",
                       "SIM_MARKETS": "range, Ladder"})
assert not c.sim and abs(c.sim_edge - 0.15) < 1e-12 and c.sim_shares == 250 and c.sim_ways == "both" and c.sim_markets == {"range", "ladder"}
assert m.sim_scope(c) == ("挂单和吃单", "价格阶梯、市值阶梯")
c = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_MARKETS": "all", "SIM_WAYS": ""})
assert c.sim_markets == frozenset(m.SIM_KINDS) and c.sim_ways == "taker" and m.sim_scope(c) == ("只吃单", "全部市场")
assert c.sim_taker_after_hours == 0 and c.sim_after_hours_max_price == 0.60  # after-hours taker mode off; 60¢ the most paid after hours
c = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_TAKER_AFTER_HOURS_CENTS": "15", "SIM_AFTER_HOURS_MAX_PRICE_CENTS": "0"})
assert abs(c.sim_taker_after_hours - 0.15) < 1e-12 and c.sim_after_hours_max_price == 0
for bad in ({"SIM_TAKER_AFTER_HOURS_CENTS": "51"}, {"SIM_AFTER_HOURS_MAX_PRICE_CENTS": "100"}):
    try: m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", **bad}); assert False, bad
    except ValueError: pass
assert c.sim_skip == frozenset()  # the detail chips: SIM_SKIP names markets left out of the chosen kinds, upper-cased, blanks dropped
assert m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_SKIP": " hsi, bnb ,HSI,"}).sim_skip == frozenset({"HSI", "BNB"})
c = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_MAKER_DEEP_MARKETS": " hsi, kospi ", "SIM_MAKER_AFTER_HOURS_MARKETS": "sse", "SIM_TAKER_AFTER_HOURS_MARKETS": " hk0625usdt ,HSI"})
assert c.sim_maker_deep_markets == {"HSI", "KOSPI"} and c.sim_maker_after_hours_markets == {"SSE"} and m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}).sim_maker_deep_markets == frozenset()
assert c.sim_taker_after_hours_markets == {"HK0625USDT", "HSI"} and m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}).sim_taker_after_hours_markets == frozenset()  # 1.76.0
for bad in ({"SIM_EDGE_CENTS": "0"}, {"SIM_EDGE_CENTS": "60"}, {"SIM_SHARES": "0"}, {"SIM_WAYS": "none"}, {"SIM_MARKETS": "index"},
            {"SIM_SKIP": "x" * 41}, {"SIM_SKIP": ",".join(f"k{i}" for i in range(201))}, {"SIM_MAKER_DEEP_MARKETS": "x" * 41}):
    try: m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", **bad}); assert False, bad
    except ValueError: pass


# --- the arithmetic -----------------------------------------------------------------------------------------------------
def book(bids, asks, at=NOW, slug=HSI_SLUG, key="HSI", fee=None, mid="101"):
    return m.PredictBook(key, slug, mid, "t", tuple((D(p), D(q)) for p, q in bids), tuple((D(p), D(q)) for p, q in asks), at, fee)


b = book([("0.55", "300"), ("0.50", "50")], [("0.58", "400")])
assert m.side_levels(b, "up") == [(0.58, 400.0)]
assert [(round(p, 9), q) for p, q in m.side_levels(b, "down")] == [(0.45, 300.0), (0.5, 50.0)]  # No = 1 − the Yes bids
assert m.own_levels(b, "up") == [(0.55, 300.0), (0.5, 50.0)] and [(round(p, 9), q) for p, q in m.own_levels(b, "down")] == [(0.42, 400.0)]
assert m.fill_shares([(0.5, 30.0), (0.6, 100.0)], 100) == ((30 * 0.5 + 70 * 0.6) / 100, 100.0)
assert m.fill_shares([(0.5, 30.0)], 100) == (0.5, 30.0) and m.fill_shares([], 100) == (0.0, 0.0)  # a thin book fills what it has
assert m.sim_payout("up", 1.0) == 1.0 and m.sim_payout("down", 1.0) == 0.0 and m.sim_payout("down", 0.5) == 0.5
# a taker quote for a number of shares: the levels walked, the fee on the average, short when the book runs out
q = m.taker_quote(book([], [("0.58", "30"), ("0.66", "500")]), "up", 100, 200)
avg = (30 * 0.58 + 70 * 0.66) / 100
assert abs(q["avg"] - avg) < 1e-12 and q["got"] == 100 and abs(q["fee"] - FEE * (1 - avg)) < 1e-12 and not q["short"]
assert q["levels"] == [[0.58, 30.0], [0.66, 70.0]] and q["best"] == 0.58 and abs(q["cost"] - (avg + q["fee"])) < 1e-12
assert m.taker_quote(book([], [("0.58", "40")]), "up", 100, 200)["short"] and m.taker_quote(book([], []), "up", 100, 200) is None
# the cap: the dearest price that keeps the required edge after the fee (rate × min(p, 1 − p)), on Predict's three-digit tick
assert m.taker_cap(0.70, 0.10, 200) == 0.59 and m.taker_cap(0.30, 0.10, 200) == 0.19 and m.taker_cap(0.10, 0.10, 200) == 0 and m.taker_cap(0.05, 0.10, 200) == 0
assert m.taker_cap(0.995, 0.001, 0) == 0.99 and m.floor_price(0.59184) == 0.59 and m.floor_price(0.059184) == 0.05 and m.floor_price(1.2) == 0.99 and m.floor_price(0.0001) == 0
assert m.floor_price(1 - 0.87) == 0.13 and m.floor_price(0.1299) == 0.12  # a float a hair under the cent is that cent; a real 0.1299 is not
q = m.taker_quote(book([], [("0.58", "30"), ("0.66", "500")]), "up", 100, 200, cap=0.59)  # with a cap only the levels under it are walked
assert q["got"] == 30 and q["levels"] == [[0.58, 30.0]] and q["short"] and q["cap"] == 0.59 and m.taker_quote(book([], [("0.60", "30")]), "up", 100, 200, cap=0.59) is None
# a resting buy: sellers at or through its price are what fills it; the most seen, never a sum of looks
order = {"price": 0.55, "side": "up", "order": 100.0, "shares": 0.0}
got, seen = m.maker_fill(order, book([("0.54", "10")], [("0.55", "30"), ("0.56", "500")]))
assert got == 30 and seen == {"through": [[0.55, 30.0]], "visible": 30.0, "queue_now": 0.0}, seen
assert m.maker_fill({**order, "shares": 30.0}, book([], [("0.55", "20")]))[0] == 30  # fewer seen later: nothing more
assert m.maker_fill({**order, "shares": 30.0}, book([], [("0.54", "50"), ("0.55", "20")]))[0] == 70
assert m.maker_fill(order, book([("0.55", "120")], [("0.57", "400")])) == (0.0, {"through": [], "visible": 0.0, "queue_now": 120.0})
assert m.maker_fill(order, book([], [("0.40", "999")]))[0] == 100  # never more than the order
# Predict's resolution, read from the market object (an outcome WON, payouts, or a resolution named on a resolved market)
outcomes = lambda *rows: [{"name": n, "indexSet": i + 1, **extra} for i, (n, extra) in enumerate(rows)]
assert m.predict_resolution({"status": "REGISTERED", "outcomes": outcomes(("Up", {}), ("Down", {}))}) is None
won = m.predict_resolution({"status": "RESOLVED", "outcomes": outcomes(("Up", {"status": "LOST"}), ("Down", {"status": "WON"}))})
assert won == {"index": 1, "name": "Down", "split": False, "how": "outcomes.status"}, won
assert m.predict_resolution({"outcomes": outcomes(("Yes", {"payout": "1"}), ("No", {"payout": "0"}))})["name"] == "Yes"
assert m.predict_resolution({"outcomes": outcomes(("Up", {"payout": 0.5}), ("Down", {"payout": 0.5}))})["split"]
assert m.predict_resolution({"status": "RESOLVED", "resolution": {"name": "No"}, "outcomes": outcomes(("Yes", {}), ("No", {}))})["index"] == 1
assert m.predict_resolution({"status": "REGISTERED", "resolution": {"name": "No"}, "outcomes": outcomes(("Yes", {}), ("No", {}))}) is None
# an old record (before evidence was kept) in today's shape: nothing invented
legacy = {"market": "old", "slug": "old", "item": "恒生指数", "kind": "close", "key": "HSI", "side": "up", "label": "挂涨",
          "maker": True, "fair": 0.70, "signal": 0.15, "opened": NOW - 86400_000, "settle": {}, "price": 0.55, "shares": 100,
          "status": "settled", "filled": NOW - 80000_000, "fill_fair": 0.62, "edge": 0.15, "payout": 1.0,
          "settled": NOW - 3600_000, "note": "10-04 收盘 24,650，对 24,600"}
up = m.sim_upgrade(legacy)
assert up["v"] == 2 and up["legacy"] and up["order"] == 100 and up["confirm"] == "local" and up["local"]["up"] == 1.0
assert up["fills"] == [{"at": NOW - 80000_000, "shares": 100, "fair": 0.62, "how": "旧规则：触价即算全部成交"}] and "v" not in legacy
assert m.sim_upgrade({**legacy, "status": "resting", "payout": None, "filled": None})["shares"] == 0.0  # had filled nothing


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


async def step(bot, at, *markets):
    bot.market.now, bot.sim_ran = at, -1e9  # the trader's own pacing runs on the monotonic clock
    if markets:
        world["markets"] = list(markets)
    return await bot.sim_step(at)


world = {"markets": []}
SETTLE_HSI = {"key": "HSI", "target": "2026-10-05", "line": 24600.0, "close_ms": CLOSE}


def hsi(fair, bids, asks, at, **kw):
    return m.SimMarket(HSI_SLUG, "恒生指数", "close", "HSI", fair, book(bids, asks, at), 0.03, kw.get("hold", ""), ("涨", "跌"),
                       SETTLE_HSI, {"basis": {"fair_up": fair, "ref": 24600.0}, "sources": [{"what": "恒指期货", "source": "etnet"}],
                                    "proxy": {"proxy": "恒指期货", "anchor": 24500.0}})


async def run():
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                             "WEB_PORT": "8080", "SIM_WAYS": "both", "SIM_MARKETS": "all", **LEGACY})  # every kind and both ways, as below
    store = m.Store(":memory:")
    bot = m.Bot(cfg, store, FM(NOW), None)
    bot.sim_markets = lambda now: world["markets"]
    assert "模拟交易" in [name for name, _ in bot.reference_jobs()] and bot.cmd_sim(None).text.count("还没有触发过") == 1
    asked = []
    answers = {}

    async def details(mid):
        asked.append(mid)
        answer = answers.get(mid, {"outcomes": ["Up", "Down"], "status": "REGISTERED", "resolved": None})
        if isinstance(answer, Exception):
            raise answer
        return answer
    bot.predict.market_details = details

    # --- HSI: fair 70¢, Yes bid 55¢×300 (挂涨 +15¢) and ask 58¢×400 (100 shares: 58¢ + 0.84¢ fee → +11.16¢) --------------
    await step(bot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    trades = bot.sim_trades()
    assert sorted(trades) == [f"{HSI_SLUG}|up|吃", f"{HSI_SLUG}|up|挂"], sorted(trades)
    maker, taker = trades[f"{HSI_SLUG}|up|挂"], trades[f"{HSI_SLUG}|up|吃"]
    assert maker["status"] == "resting" and maker["price"] == 0.55 and maker["order"] == 100 and maker["shares"] == 0
    assert maker["queue_ahead"] == 300 and maker["filled"] is None and maker["label"] == "挂涨" and abs(maker["edge"] - 0.15) < 1e-9
    fee = FEE * 0.42
    assert taker["status"] == "filled" and taker["shares"] == 100 and abs(taker["price"] - (0.58 + fee)) < 1e-12 and taker["avg"] == 0.58
    assert abs(taker["edge"] - (0.70 - 0.58 - fee)) < 1e-12 and taker["signal"] == taker["edge"] and taker["slip"] == 0
    assert taker["fills"] == [{"at": NOW, "shares": 100.0, "fair": 0.70, "how": "吃单立即成交", "levels": [[0.58, 100.0]], "short": False}]
    # what the order was decided on: the model, its sources and proxy, the book, the card's four directions, the version
    entry = maker["entry"]
    assert entry["at"] == NOW and entry["fair"] == 0.70 and entry["need"] == 0.03 and entry["basis"] == {"fair_up": 0.70, "ref": 24600.0}
    assert entry["sources"] == [{"what": "恒指期货", "source": "etnet"}] and entry["proxy"]["anchor"] == 24500.0
    assert entry["book"] == {"bids": [[0.55, 300.0]], "asks": [[0.58, 400.0]], "fetched_ms": NOW, "market_id": "101", "fee_bps": None}
    assert [(e["label"], e["best"]) for e in entry["card"]] == [("挂涨", True), ("挂跌", False), ("吃涨", False), ("吃跌", False)]
    assert maker["version"] == {"code": m.VERSION, "sim_edge": 0.10, "sim_shares": 100, "sim_ways": "挂单和吃单", "sim_markets": "全部市场", "sim_group_usd": 300, "min_edge": 0.02, "fee_bps": 200,
                                "trade_usd": 100, "a50_beta": 0.8, "kospi_beta": 1.0, "sigma_error": m.MODEL_SIGMA_ERROR,
                                "beta_error": m.MODEL_BETA_ERROR, "big_move": 1.5, "sim_taker_max_shares": 0.0, "sim_taker_sweep": 0.0, "sim_max_fair": 0.95, "sim_after_hours_max_shares": 300.0, "sim_open_quiet_minutes": 3, "sim_maker_spread": 0.10, "sim_after_hours_max_price": 0.60, "sim_shares_net": True, "sim_no_odds_withdraw": 30} and maker["market_id"] == "101"  # LEGACY keeps the 10¢ rule
    # the edge lasting for hours buys nothing more
    await step(bot, NOW + 60_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 60_000))
    assert len(bot.sim_trades()) == 2

    # --- one trade size: a taker is judged on its own 100 shares (the card's $100 view is kept beside it) --------------------
    same = lambda asks, at, name: m.SimMarket(name, name, "close", "X", 0.70, book([], asks, at, name, mid=name), 0.03, "", ("涨", "跌"), {})
    # 100 shares at 58¢ clear 10¢ (+11.16¢); $100 would walk into 70¢ (+6.75¢ on the card)
    await step(bot, NOW + 70_000, same([("0.58", "100"), ("0.70", "1000")], NOW + 70_000, "deep"))
    t = bot.sim_trades()["deep|up|吃"]
    assert t["shares"] == 100 and abs(t["edge"] - (0.70 - 0.58 - fee)) < 1e-12, t
    card = next(e for e in t["entry"]["card"] if e["label"] == "吃涨")
    assert abs(card["edge"] - (0.70 - 0.625 - FEE * 0.375)) < 1e-12 and card["size"] == 160, card
    # only the levels under the cap (59¢: the dearest price keeping 10¢ after the fee) are bought: 10 at 58¢, none at 69¢
    await step(bot, NOW + 80_000, same([("0.58", "10"), ("0.69", "1000")], NOW + 80_000, "shallow"))
    t = bot.sim_trades()["shallow|up|吃"]
    assert t["shares"] == 10 and t["order"] == 100 and t["cap"] == 0.59 and t["fills"][0]["short"] and t["fills"][0]["levels"] == [[0.58, 10.0]], t
    # a thin book fills what it holds (whatever is there, up to the order), and says so
    await step(bot, NOW + 90_000, same([("0.58", "60")], NOW + 90_000, "thin"))
    t = bot.sim_trades()["thin|up|吃"]
    assert t["shares"] == 60 and t["order"] == 100 and t["fills"][0]["short"], t
    # anything from one share up is bought; under one share is dust, not a trade: the slot stays free for the real one
    await step(bot, NOW + 95_000, same([("0.58", "5")], NOW + 95_000, "dust"))
    assert bot.sim_trades()["dust|up|吃"]["shares"] == 5
    await step(bot, NOW + 96_000, same([("0.58", "0.5")], NOW + 96_000, "dust2"))
    assert "dust2|up|吃" not in bot.sim_trades()
    # a crossed snapshot (bid at or above ask) is not a book anyone could trade: neither way opens on it
    crossed = m.SimMarket("x", "x", "close", "X", 0.70, book([("0.60", "100")], [("0.58", "100")], NOW + 97_000, "x", mid="x"), 0.03, "", ("涨", "跌"), {})
    await step(bot, NOW + 97_000, crossed)
    assert not [k for k in bot.sim_trades() if k.startswith("x|")] and m.book_crossed(crossed.book)
    # a stale book or a card that holds back: no new trade and no fill read from it
    await step(bot, NOW + 100_000, m.dataclasses.replace(same([("0.40", "100")], NOW - 200_000, "stale"), market="stale"),
               m.dataclasses.replace(hsi(0.80, [("0.50", "100")], [("0.52", "100")], NOW + 100_000, hold="期货锚点是近似值"), market="held"))
    assert not [k for k in bot.sim_trades() if k.startswith(("stale|", "held|"))]
    # a bar above the edge (model error 12¢): an 11¢ taker edge is not a suggestion, so nothing is bought
    await step(bot, NOW + 110_000, m.dataclasses.replace(same([("0.58", "400")], NOW + 110_000, "wide"), need=0.12))
    assert not [k for k in bot.sim_trades() if k.startswith("wide|")]

    # --- a resting order fills only as far as sellers show at or through its price ---------------------------------------
    hsi_maker = f"{HSI_SLUG}|up|挂"
    await step(bot, NOW + 120_000, hsi(0.70, [("0.55", "120")], [("0.57", "400")], NOW + 120_000))
    t = bot.sim_trades()[hsi_maker]
    assert t["status"] == "resting" and t["shares"] == 0 and t["queue_min"] == 120  # the queue ahead shrank, nothing traded with us
    wait = {x["id"]: x["wait"] for x in bot.journal_payload()["trades"]}  # the review page says what each open trade waits for
    assert wait[hsi_maker] == "挂 55.0¢，最低卖价 57.0¢（高出 2.0¢）：要有人卖到挂价或更低才算成交；现在公平价 70.0¢", wait[hsi_maker]
    assert wait["deep|up|吃"] == "等市场出结果" and wait[f"{HSI_SLUG}|up|吃"].startswith("等 10-05 收盘（10-05 16:10）后 1 小时（10-05 17:10），按官方收盘预结算")
    await step(bot, NOW + 180_000, hsi(0.70, [("0.54", "200")], [("0.55", "30"), ("0.60", "100")], NOW + 180_000))
    await step(bot, NOW + 240_000, hsi(0.70, [("0.54", "200")], [("0.55", "30"), ("0.60", "100")], NOW + 240_000))  # same seller
    t = bot.sim_trades()[hsi_maker]
    assert t["shares"] == 30 and len(t["fills"]) == 1 and t["filled"] == NOW + 180_000 and t["status"] == "resting", t
    f = t["fills"][0]
    assert f["how"] == "推定成交" and f["seen"] == {"through": [[0.55, 30.0]], "visible": 30.0, "queue_now": 0.0} and f["fair"] == 0.70
    assert f["book"]["asks"] == [[0.55, 30.0], [0.6, 100.0]] and f["sources"] == entry["sources"] and f["basis"]["fair_up"] == 0.70
    assert m.sim_status(t) == "挂单中（推定成交 30/100 份）"
    # the market turns against the order as it fills: the fill-time expectation falls below the order-time one
    await step(bot, NOW + 300_000, hsi(0.62, [("0.53", "200")], [("0.54", "50"), ("0.55", "20")], NOW + 300_000))
    await step(bot, NOW + 360_000, hsi(0.60, [("0.52", "200")], [("0.53", "500")], NOW + 360_000))
    t = bot.sim_trades()[hsi_maker]
    assert t["status"] == "filled" and t["shares"] == 100 and [x["shares"] for x in t["fills"]] == [30, 40, 30], t["fills"]
    assert abs(m.sim_fill_expectation(t) - (30 * 0.15 + 40 * 0.07 + 30 * 0.05)) < 1e-9 and abs(t["edge"] * t["shares"] - 15) < 1e-9

    # --- KOSPI (跌 side): a maker that fills in part, a taker over two levels ----------------------------------------------
    kospi = lambda bids, asks, at: m.SimMarket(KOSPI_SLUG, "KOSPI", "close", "KOSPI", 0.20, book(bids, asks, at, KOSPI_SLUG, "KOSPI", mid="102"),
                                               0.02, "", ("涨", "跌"), {"key": "KOSPI", "target": "2026-10-05", "line": 3300.0,
                                                                     "close_ms": BJ(10, 5, 14, 30)})
    await step(bot, NOW + 400_000, kospi([("0.35", "30"), ("0.30", "500")], [("0.38", "100")], NOW + 400_000))
    t = bot.sim_trades()
    down_maker, down_taker = t[f"{KOSPI_SLUG}|down|挂"], t[f"{KOSPI_SLUG}|down|吃"]
    assert down_maker["label"] == "挂跌" and abs(down_maker["price"] - 0.62) < 1e-9 and down_maker["queue_ahead"] == 100
    # the 跌 cap (fair 80¢, 10¢ required after the fee) is 69¢: the 65¢ level (30 shares) is bought, the 70¢ level is not
    k_cost = 0.65 + FEE * min(0.65, 1 - 0.65)
    assert down_taker["shares"] == 30 and down_taker["order"] == 100 and abs(down_taker["price"] - k_cost) < 1e-12 and down_taker["slip"] == 0
    assert down_taker["cap"] == 0.69 and down_taker["fills"][0]["short"] and down_taker["fills"][0]["levels"] == [[0.65, 30.0]], down_taker["fills"]
    await step(bot, NOW + 460_000, kospi([("0.39", "25")], [("0.41", "100")], NOW + 460_000))  # a 跌 seller at 61¢ ≤ 62¢
    assert bot.sim_trades()[f"{KOSPI_SLUG}|down|挂"]["shares"] == 25
    wait = {x["id"]: x["wait"] for x in bot.journal_payload()["trades"]}
    assert wait[f"{KOSPI_SLUG}|down|挂"] == "盘口有卖到挂价的卖单，已按看到的数量推定成交；现在公平价 80.0¢", wait
    assert wait[f"{KOSPI_SLUG}|down|吃"] == "等 10-05 收盘（10-05 14:30）后 1 小时（10-05 15:30），按官方收盘预结算，再等 Predict 确认"
    assert wait[hsi_maker] == "等 10-05 收盘（10-05 16:10）后 1 小时（10-05 17:10），按官方收盘预结算，再等 Predict 确认"  # filled by now
    assert bot.sim_wait({"status": "resting"}, None, NOW) == "这个市场现在没有报价（卡片未定价或盘口没读到），挂单原地等着"
    assert bot.sim_wait({"status": "resting", "side": "up", "price": 0.55}, hsi(0.70, [], [], NOW - 600_000), NOW) == "盘口已过期，等新盘口"
    assert bot.sim_wait({"status": "resting", "side": "up", "price": 0.55}, hsi(0.70, [("0.50", "9")], [], NOW), NOW) == (
        "挂 55.0¢，盘口这一边没有卖单：要有人卖到挂价或更低才算成交；现在公平价 70.0¢")
    past = {"status": "filled", "kind": "close", "settle": SETTLE_HSI, "final_check": {"status": "REGISTERED"}}
    assert bot.sim_wait(past, None, CLOSE + 2 * 3_600_000) == "收盘（10-05 16:10）已过 1 小时，官方收盘还没读到，等 Predict 结算（Predict 市场状态：REGISTERED）"
    assert bot.sim_wait({**past, "kind": "range", "settle": {"end": CLOSE}, "final_check": None, "final_error": "HTTP 500"}, None, CLOSE + 2 * 3_600_000) == (
        "窗口已结束（10-05 16:10），本地数据还定不了结果，等 Predict 结算（读取失败：HTTP 500）")
    assert bot.sim_wait({"status": "settled"}, None, NOW) == "" and bot.sim_wait({"status": "cancelled"}, None, NOW) == ""

    # --- local pre-settlement on the official close (an hour after it), with where the close came from ----------------------
    bot.note_outcome("HSI", "2026-10-05", 24650.0, "tencent 日K")
    assert store.get("outsrc:HSI:2026-10-05")["source"] == "tencent 日K" and store.get("outcome:HSI:2026-10-05") == 24650.0
    world["markets"] = []
    await step(bot, CLOSE + 30 * 60_000)
    assert bot.sim_trades()[hsi_maker]["status"] == "filled"  # the official close is not final yet
    # Predict is asked only once a market can have a result: KOSPI (closed 14:30), not yet HSI (16:10)
    assert asked == ["102"], asked
    asked.clear()
    await step(bot, CLOSE + 61 * 60_000)
    t = bot.sim_trades()
    assert t[hsi_maker]["status"] == "settled" and t[hsi_maker]["payout"] == 1.0 and t[hsi_maker]["confirm"] == "local"
    assert t[hsi_maker]["note"] == "10-05 收盘 24,650，对 24,600" and m.sim_status(t[hsi_maker]) == "赢 +$45.00"
    assert m.sim_state(t[hsi_maker]) == "预结算" and t[hsi_maker]["local"]["evidence"] == {
        "rule": "收盘高于参考线为涨，低于为跌，相同各半", "close": 24650.0, "line": 24600.0, "source": "tencent 日K", "day": "2026-10-05"}
    assert asked == ["101"] and t[hsi_maker]["final_check"]["status"] == "REGISTERED"  # Predict not resolved yet: stays 预结算
    # --- Predict's final word: asked at most every 10 minutes per market; it confirms -------------------------------------
    answers["101"] = {"outcomes": ["Up", "Down"], "status": "RESOLVED", "resolved": {"index": 0, "name": "Up", "split": False,
                                                                                      "how": "outcomes.status"}}
    await step(bot, CLOSE + 62 * 60_000)
    assert asked == ["101"]  # not again within 10 minutes
    bot.sim_checked.clear()  # ten minutes on
    await step(bot, CLOSE + 72 * 60_000)
    t = bot.sim_trades()
    assert asked.count("101") == 2 and t[hsi_maker]["confirm"] == "confirmed" and m.sim_state(t[hsi_maker]) == "已确认", asked
    assert t[hsi_maker]["final"]["name"] == "Up" and t[hsi_maker]["final"]["up"] == 1.0 and t[hsi_maker]["revisions"] == []
    assert t[f"{HSI_SLUG}|up|吃"]["confirm"] == "confirmed" and t[f"{HSI_SLUG}|up|吃"]["payout"] == 1.0
    bot.sim_checked.clear()
    await step(bot, CLOSE + 90 * 60_000)
    assert asked.count("101") == 2  # confirmed: never asked again

    # --- a tie locally, Down on Predict: re-settled on Predict's result, the change kept --------------------------------------
    bot.note_outcome("KOSPI", "2026-10-05", 3300.0, "Yahoo ^KS11 日K")
    await step(bot, CLOSE + 91 * 60_000)
    t = bot.sim_trades()
    k_maker, k_taker = t[f"{KOSPI_SLUG}|down|挂"], t[f"{KOSPI_SLUG}|down|吃"]
    assert k_taker["payout"] == 0.5 and k_taker["confirm"] == "local" and k_maker["status"] == "settled"
    assert k_maker["shares"] == 25 and k_maker["unfilled"] == 75 and k_maker["payout"] == 0.5  # only what filled is paid
    answers["102"] = {"outcomes": ["Up", "Down"], "status": "RESOLVED", "resolved": {"index": 1, "name": "Down", "split": False, "how": "x"}}
    bot.sim_checked.clear()
    await step(bot, CLOSE + 92 * 60_000)
    k_taker = bot.sim_trades()[f"{KOSPI_SLUG}|down|吃"]
    assert k_taker["confirm"] == "mismatch" and k_taker["payout"] == 1.0 and m.sim_state(k_taker) == "结果不一致"
    assert k_taker["revisions"] == [{"at": CLOSE + 92 * 60_000, "by": "Predict 最终结果", "from": 0.5, "to": 1.0,
                                     "note": "Predict 结算：Down（本地预结算为 10-05 收盘 3,300，对 3,300）"}], k_taker["revisions"]
    assert k_taker["local"]["up"] == 0.5 and k_taker["final"]["up"] == 0.0  # both kept

    # --- the local data corrected before Predict answers: re-settled, kept as a revision ------------------------------------
    sse_slug = "sse-composite-index-up-or-down-on-october-5-2026"
    sse = m.SimMarket(sse_slug, "上证指数", "close", "SSE", 0.70, book([], [("0.55", "500")], CLOSE + 93 * 60_000, sse_slug, "SSE", mid="103"),
                      0.02, "", ("涨", "跌"), {"key": "SSE", "target": "2026-10-05", "line": 3850.0, "close_ms": BJ(10, 5, 15, 0)})
    # (opened by hand: since 1.54.3 nothing is bought on a market past its close, and this one closed at 15:00)
    q = m.taker_quote(sse.book, "up", 100, bot.config.predict_fee_bps, 0.60)
    bot.sim_save([(f"{sse_slug}|up|吃", bot.sim_open(sse, "up", CLOSE + 93 * 60_000, taker=q))]); bot.sim_cache = None
    assert bot.sim_trades()[f"{sse_slug}|up|吃"]["status"] == "filled"
    bot.note_outcome("SSE", "2026-10-05", 3851.0, "腾讯实时（日K未出）")
    world["markets"] = []
    answers["103"] = m.RemoteError("HTTP 500")
    bot.sim_checked.clear()
    await step(bot, CLOSE + 94 * 60_000)
    t = bot.sim_trades()[f"{sse_slug}|up|吃"]
    assert t["payout"] == 1.0 and t["confirm"] == "local" and t["final_error"] == "HTTP 500"
    bot.note_outcome("SSE", "2026-10-05", 3849.5, "腾讯日K")
    await step(bot, CLOSE + 95 * 60_000)
    t = bot.sim_trades()[f"{sse_slug}|up|吃"]
    assert t["payout"] == 0.0 and t["local"]["evidence"]["source"] == "腾讯日K" and t["confirm"] == "local"
    assert [(r["by"], r["from"], r["to"]) for r in t["revisions"]] == [("本地数据更正", 1.0, 0.0)], t["revisions"]

    # --- markets the bot's data cannot decide wait for Predict ---------------------------------------------------------------
    touch = bot.touches["BNB"]; touch.start_ms = BJ(9, 1, 0)
    trade = lambda kind, key, side, **settle: {"kind": kind, "key": key, "side": side, "market": f"x#{settle.pop('mid', '')}", "settle": settle}
    assert bot.sim_result(trade("touch", "BNB", "up", deadline=touch.spec.deadline_ms), NOW) is None
    store.put(f"touch:{touch.spec.slug}", {"kind": "high", "time": NOW - 3_600_000, "hi": 901, "lo": 880, "start": touch.start_ms})
    up_res = bot.sim_result(trade("touch", "BNB", "up", deadline=touch.spec.deadline_ms), NOW)
    assert up_res[0] == 1.0 and up_res[2]["history"]["kind"] == "high" and "币安现货" in up_res[2]["source"]
    store.put(f"touch:{touch.spec.slug}", {"kind": "low", "time": NOW - 3_600_000, "hi": 720, "lo": 699, "start": touch.start_ms})
    assert bot.sim_result(trade("touch", "BNB", "up", deadline=touch.spec.deadline_ms), NOW)[0] == 0.0
    deadline = touch.spec.deadline_ms
    # read to the last full hour only: the final partial hour is unread, so "neither" is not settled on it
    store.put(f"touch:{touch.spec.slug}", {"kind": "clear", "through": deadline - 59 * 60_000, "start": touch.start_ms})
    assert bot.sim_result(trade("touch", "BNB", "up", deadline=deadline), deadline + 61 * 60_000) is None
    store.put(f"touch:{touch.spec.slug}", {"kind": "clear", "through": deadline + 60_000, "start": touch.start_ms})
    assert bot.sim_result(trade("touch", "BNB", "up", deadline=deadline), deadline + 30 * 60_000) is None  # settles an hour on
    assert bot.sim_result(trade("touch", "BNB", "up", deadline=deadline), deadline + 61 * 60_000)[:2] == (0.5, "整个窗口都没碰到两条线，按 50/50")
    oct_spec = next(s for s in m.UPDOWN_MARKETS if s.key == "BTC-2026-10")  # BTC 10月涨跌
    assert bot.sim_result(trade("updown", oct_spec.key, "up"), NOW) is None
    store.put(f"updown:{oct_spec.slug}:start", {"open": oct_spec.start_ms, "close": "117234.56"})
    store.put(f"updown:{oct_spec.slug}:end", {"open": oct_spec.end_ms, "close": "110000"})
    res = bot.sim_result(trade("updown", oct_spec.key, "up"), oct_spec.end_ms + 120_000)
    assert res[0] == 0.0 and res[2]["start"] == 117234.56 and res[2]["end"] == 110000.0
    cap = bot.caps["NIULAI"]
    cap.price, cap.supply, cap.priced_ms = D("0.1"), D("1000000000"), NOW  # $100M now
    ladder = lambda target, mid: trade("ladder", "NIULAI", "up", target=target, end=cap.spec.end_ms, mid=mid)
    assert bot.sim_result(ladder("200000000", "11"), NOW) is None
    store.put(f"cap:{cap.spec.slug}", {"high": 0.21, "at": NOW // 1000, "start": cap.spec.start_ms})  # $210M seen
    res = bot.sim_result(ladder("200000000", "11"), NOW)
    assert res[0] == 1.0 and res[2]["window_high"] == 210_000_000.0
    assert bot.sim_result(ladder("300000000", "12"), NOW) is None
    late = cap.spec.end_ms + 61 * 60_000
    # the window's history is not read to its end: "not reached" is not settled on it
    assert cap.coverage() == "窗口尚未核验到截止" and bot.sim_result(ladder("300000000", "12"), late) is None
    store.put(f"cap:{cap.spec.slug}", {"high": 0.21, "at": NOW // 1000, "start": cap.spec.start_ms, "through": cap.window_end_s,
                                       "first": "done"})
    assert cap.coverage() == "" and bot.sim_result(ladder("300000000", "12"), late)[0] == 0.0
    # a price read after the window never counts towards its high
    cap.price, cap.priced_ms = D("0.5"), cap.spec.end_ms + 120_000
    assert bot.sim_result(ladder("300000000", "12"), late)[0] == 0.0 and cap.window_high()[0] == D("0.21") * D("1000000000")
    # Predict settles what the bot's data could not: a ladder level with an incomplete history
    pons = m.SimMarket("pons#77", "$PONS FDV $10M", "ladder", "PONS", 0.30,
                       book([("0.10", "100")], [("0.12", "100")], late, "pons", "PONS", mid="77"), 0.02, "", ("Yes", "No"),
                       {"target": "10000000", "end": late - 61 * 60_000})
    await step(bot, late - 62 * 60_000, m.dataclasses.replace(pons, book=m.dataclasses.replace(pons.book, fetched_ms=late - 62 * 60_000)))
    assert bot.sim_trades()["pons#77|up|吃"]["status"] == "filled"
    answers["77"] = {"outcomes": ["Yes", "No"], "status": "RESOLVED", "resolved": {"index": 1, "name": "No", "split": False, "how": "x"}}
    world["markets"] = []
    bot.sim_checked.clear()
    await step(bot, late)
    t = bot.sim_trades()["pons#77|up|吃"]
    assert t["status"] == "settled" and t["payout"] == 0.0 and t["confirm"] == "confirmed" and "local" not in t, t
    # a contract's daily market: its exchange close is kept per day, with its source
    sym = "UNITREEUSDT"
    close_ms = int(dt.datetime(2026, 10, 5, 15, 0, tzinfo=m.BEIJING).timestamp() * 1000)
    bot.stocks.closes[sym] = m.Baseline(D("75.5"), "k", "label", 0, close_ms, "", "上交所688836·东方财富")
    bot.sim_note_closes()
    assert store.get(f"outcome:{sym}:2026-10-05") == 75.5 and store.get(f"outsrc:{sym}:2026-10-05")["source"] == "上交所688836·东方财富"

    # --- an old record is upgraded, confirmed through its slug ----------------------------------------------------------------
    store.put("sim:old|up|挂", legacy)
    async def resolve(slug):
        return {"id": "900"} if slug == "old" else None
    bot.predict.resolve = resolve
    answers["900"] = {"outcomes": ["Up", "Down"], "status": "RESOLVED", "resolved": {"index": 0, "name": "Up", "split": False, "how": "x"}}
    bot.sim_checked.clear()
    await step(bot, late + 60_000)
    old = bot.sim_trades()["old|up|挂"]
    assert old["legacy"] and old["confirm"] == "confirmed" and old["payout"] == 1.0 and store.get("sim:old|up|挂")["v"] == 2

    # --- the record: totals, the expectation at the order and at the fills, how results were confirmed --------------------
    r = bot.sim_report()
    tot = r["total"]
    settled = [t for t in bot.sim_trades().values() if t["status"] == "settled"]
    assert tot["settled"] == len(settled) and tot["confirmed"] == sum(t["confirm"] == "confirmed" for t in settled)
    # both KOSPI trades (the 25 filled shares of the maker too) were re-settled on Predict's result; SSE is 预结算
    assert tot["mismatch"] == 2 and tot["local"] == 1 and tot["confirmed"] == 4, tot
    assert abs(tot["pnl"] - sum((t["payout"] - t["price"]) * t["shares"] for t in settled)) < 1e-9
    assert abs(tot["expected"] - sum(t["edge"] * t["shares"] for t in settled)) < 1e-9
    assert abs(tot["expected_fill"] - sum(m.sim_fill_expectation(t) for t in settled)) < 1e-9 and tot["expected_fill"] < tot["expected"]
    assert tot["partial"] == 1 and tot["expired"] == 0 and tot["cancelled"] == 1 and tot["open"] == 4, tot  # PONS's maker never filled: withdrawn at the end (1.54.3); deep/thin/shallow/dust still held
    row = next(x for x in r["rows"] if x["id"] == f"{KOSPI_SLUG}|down|吃")
    assert row["state"] == "结果不一致" and row["order"] == 100 and row["url"].startswith(m.PREDICT_SITE)
    json.dumps(r)
    text = bot.cmd_sim(None).text
    assert "已确认 4｜预结算 1｜结果不一致 2" in text and "模型预期：下单时" in text and "成交时" in text, text
    assert "部分成交 1 笔" in text and f"{bot.web_url()}/journal" in text and "·结果不一致" in text, text
    assert "已关闭" in m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM": "off"}), m.Store(":memory:"), FM(NOW), None).cmd_sim(None).text
    off = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM": "off"}), m.Store(":memory:"), FM(NOW), None)
    assert "模拟交易" not in [name for name, _ in off.reference_jobs()] and not any(i["name"] == "模拟交易" for i in off.odds_payload()["items"])

    # --- the journal: light rows for the page, every record whole on request, and a CSV for spreadsheets ---------------------
    j = bot.journal_payload()
    assert j["trades"][0]["opened"] >= j["trades"][-1]["opened"] and len(j["trades"]) == len(bot.sim_trades())
    jt = next(x for x in j["trades"] if x["id"] == hsi_maker)
    assert jt["state"] == "已确认" and "entry" not in jt and "fills" not in jt and jt["expected_fill"] < 15  # a list row (1.59.1)
    jt = bot.journal_trade(hsi_maker)
    assert jt["state"] == "已确认" and jt["entry"]["proxy"]["anchor"] == 24500.0 and len(jt["fills"]) == 3 and jt["expected_fill"] < 15
    assert bot.journal_trade("nope") is None and next(x for x in bot.journal_payload(full=True)["trades"] if x["id"] == hsi_maker)["fills"] == jt["fills"]
    json.dumps(j, default=str)
    raw = bot.journal_csv()
    assert raw.startswith("﻿")
    rows = list(csv.reader(io.StringIO(raw[1:])))
    assert rows[0] == list(m.Bot.JOURNAL_COLUMNS) and all(len(x) == len(rows[0]) for x in rows) and len(rows) == len(j["trades"]) + 1
    line = next(x for x in rows if x[0] == f"{KOSPI_SLUG}|down|吃")
    col = dict(zip(rows[0], line))
    assert col["确认"] == "结果不一致" and col["结果不一致"] == "是" and col["本地结果"] == "50/50" and col["Predict 结果"].startswith("跌/No")
    assert "Predict 最终结果：0.5→1.0" in col["修订"] and col["市场链接"].startswith(m.PREDICT_SITE), col
    line = next(x for x in rows if x[0] == "old|up|挂")
    assert dict(zip(rows[0], line))["行情来源"] == "旧记录：无快照"

    # --- the real cards feed it, evidence included: the BTC 10月涨跌 card frames 挂涨 at 40¢ against a fair ~54¢ ----------
    real = m.Bot(cfg, m.Store(":memory:"), FM(BJ(10, 1, 16, 15)), None)
    up_mkt = real.updowns[oct_spec.key]
    real.store.put(f"updown:{oct_spec.slug}:start", {"open": oct_spec.start_ms, "close": "117234.56"})
    at = real.market.now
    up_mkt.price, up_mkt.priced_ms, up_mkt.sigma, up_mkt.sigma_ms = D("118500"), at, 0.28, at
    real.predict.books[oct_spec.key] = m.PredictBook(oct_spec.key, oct_spec.slug, "77", "t", ((D("0.40"), D("500")),), ((D("0.45"), D("500")),), at)
    real.predict.info[oct_spec.slug] = {"outcomes": ["Up", "Down"], "created_ms": 0}
    card = next(i for i in real.odds_payload()["items"] if i["name"] == "BTC 10月涨跌")
    page_best = next(e for e in card["predict"]["edges"] if e["best"])
    assert page_best["label"] == "挂涨" and page_best["edge"] >= 0.10, card["predict"]["edges"]
    mk = next(x for x in real.sim_markets(at) if x.kind == "updown")
    assert mk.fair_up == card["fair_up"] and mk.need == card["predict"]["need"] and not mk.hold
    real.sim_ran = -1e9
    await real.sim_step(at)
    (t,) = real.sim_trades().values()
    assert (t["label"], t["price"], t["status"]) == ("挂涨", 0.40, "resting") and t["settle"] == {"end": oct_spec.end_ms}
    assert t["entry"]["basis"]["line"] == 117234.56 and t["entry"]["basis"]["price"] == 118500.0 and t["entry"]["basis"]["sigma"] == 0.28
    assert t["entry"]["sources"][0] == {"what": "现货", "source": "币安现货", "symbol": "BTCUSDT", "type": "最新价", "price": 118500.0,
                                        "quoted_ms": at, "fetched_ms": at}
    sim_card = next(i for i in real.odds_payload()["items"] if i["name"] == "模拟交易")
    assert sim_card["group"] == "sim" and sim_card["sim"]["total"]["resting"] == 1 and sim_card["sim"]["rows"][0]["id"] == f"{oct_spec.slug}|up|挂"

    # a daily card: the live index, its dated reference close (real sources, codes, quote and read times)
    sbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"}),
                 m.Store(":memory:"), FM(BJ(9, 28, 10, 5)), None)
    sat = sbot.market.now
    sbot.cn.quote = m.IndexQuote("上证指数", D("3860"), D("3850"), None, None, None, sat - 3000, "腾讯", fetched_ms=sat - 1000)
    sbot.cn.close = m.DailyClose(dt.date(2026, 9, 25), D("3850"), D("3840"), "腾讯日K", sat - 600_000)
    slug = "sse-composite-index-up-or-down-on-september-28-2026"
    sbot.predict.slugs["SSE"] = slug
    sbot.predict.books["SSE"] = m.PredictBook("SSE", slug, "901", "t", ((D("0.40"), D("100")),), ((D("0.45"), D("100")),), sat)
    smk = next(x for x in sbot.sim_markets(sat) if x.kind == "close" and x.key == "SSE")
    ev = smk.evidence
    assert ev["basis"]["direct"] and ev["basis"]["ref"] == 3850.0 and ev["basis"]["close_ms"] == BJ(9, 28, 15, 0) and ev["proxy"] is None
    assert ev["sources"] == [{"what": "上证现货", "source": "腾讯", "symbol": "sh000001", "type": "最新价", "price": 3860.0,
                              "quoted_ms": sat - 3000, "fetched_ms": sat - 1000},
                             {"what": "参考收盘", "source": "腾讯日K", "symbol": "sh000001", "type": "日K收盘", "price": 3850.0,
                              "quoted_ms": m.DailyClose(dt.date(2026, 9, 25), D("3850"), None, "", 0).close_ms,
                              "fetched_ms": sat - 600_000, "day": "2026-09-25"}], ev["sources"]
    # a contract card after hours: the Binance price is the proxy, its price at the exchange close the anchor
    kbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "SKHYNIXUSDT"}), m.Store(":memory:"), FM(sat), None)
    base_ms = BJ(9, 25, 14, 30)
    kbot.stocks.closes["SKHYNIXUSDT"] = m.Baseline(D("1768000"), "k", "l", 0, base_ms, "KRW", "韩交所000660·Naver KRX")
    kbot.snapshots["SKHYNIXUSDT"] = {"quote": m.Quote(D("1300"), sat - 2000, "mark")}
    kbot.anchors["SKHYNIXUSDT"] = (base_ms, D("1290"))
    odds = m.close_odds("SK 海力士", D("1768000"), D("1781705"), 0.03, 1.0, dt.date(2026, 9, 29), D("1000"), "09-25 14:30·Naver KRX",
                        "币安 1,300 / 收盘时刻 1,290 → +0.775%", "σ")
    ev = kbot.close_evidence("SK 海力士｜SKHYNIXUSDT", odds, sat)
    assert ev["proxy"] == {"proxy": "币安 SKHYNIXUSDT 合约", "price": 1300.0, "quoted_ms": sat - 2000, "anchor": 1290.0, "anchor_ms": base_ms,
                           "anchor_note": "收盘时刻的币安价格", "approx": False}, ev["proxy"]
    assert ev["sources"][0]["source"] == "韩交所000660·Naver KRX" and ev["sources"][1]["type"] == "标记价" and not ev["basis"]["direct"]
    # an evidence failure never stops a trade: the failure is the record
    assert sbot.evidence(lambda: 1 / 0) == {"error": "division by zero"}

    # the default scope: only the index / contract daily cards, only takers (SIM_MARKETS and SIM_WAYS widen it)
    dbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"}),
                 m.Store(":memory:"), FM(NOW), None)
    dbot.sim_markets = lambda now: world["markets"]
    rep = dbot.sim_report()
    assert rep["ways"] == "只吃单" and rep["scope"] == "指数/个股日涨跌" and "范围：指数/个股日涨跌；只吃单。吃单：算出仍留足净优势的最高价位（封顶价），盘口在它之下有多少买多少，最多 100 份" in dbot.cmd_sim(None).text
    assert "挂单" not in dbot.cmd_sim(None).text.split("\n")[1] and dbot.sim_version()["sim_ways"] == "只吃单"
    far = m.SimMarket("will-bnb-hit-700-or-900", "BNB 先触 700/900", "touch", "BNB", 0.70, book([(0.55, 100)], [(0.58, 100)], NOW), 0.03, "",
                      ("$900", "$700"), {"low": 700, "high": 900, "deadline": CLOSE}, {})
    await step(dbot, NOW, hsi(0.70, [(0.55, 100)], [(0.58, 100)], NOW), far)  # 挂涨 +15¢ and 吃涨 +11¢ on both
    assert sorted(dbot.sim_trades()) == [f"{HSI_SLUG}|up|吃"], sorted(dbot.sim_trades())
    mbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                                    "SIM_WAYS": "maker", "SIM_MARKETS": "touch", **LEGACY}), m.Store(":memory:"), FM(NOW), None)
    mbot.sim_markets = lambda now: world["markets"]
    await step(mbot, NOW, hsi(0.70, [(0.55, 100)], [(0.58, 100)], NOW), far)
    assert sorted(mbot.sim_trades()) == ["will-bnb-hit-700-or-900|up|挂"], sorted(mbot.sim_trades())
    assert "范围：先触价；只挂单。挂单只挂在双边都有报价、价差不超过 10¢ 的盘口，排在已有挂单之后" in mbot.cmd_sim(None).text
    # the detail chips (SIM_SKIP): a market left out by key is neither taken nor quoted on, whatever its kind; keys compare upper-cased
    kbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                                    "SIM_WAYS": "both", "SIM_MARKETS": "all", "SIM_SKIP": "hsi", **LEGACY}), m.Store(":memory:"), FM(NOW), None)
    kbot.sim_markets = lambda now: world["markets"]
    skipped = hsi(0.70, [(0.55, 100)], [(0.58, 100)], NOW)
    assert not kbot.sim_in_scope(skipped) and kbot.sim_in_scope(far) and kbot.sim_maker_reason(skipped, {}, NOW) == (None, "详细选项里已排除这个市场")
    await step(kbot, NOW, skipped, far)
    assert all(k.startswith("will-bnb-hit-700-or-900|") for k in kbot.sim_trades()) and kbot.sim_trades(), sorted(kbot.sim_trades())
    rows = {r["market"]: r for r in kbot.sim_checks(NOW)}
    assert rows[HSI_SLUG]["maker_why"] == rows[HSI_SLUG]["taker_why"] == "详细选项里已排除这个市场" and rows["will-bnb-hit-700-or-900"]["taker_why"] != "详细选项里已排除这个市场"
    assert "详细选项排除了 1 个市场（HSI）" in kbot.cmd_sim(None).text and kbot.sim_version()["sim_skip"] == ["HSI"] and "sim_skip" not in mbot.sim_version()
    cat = kbot.sim_catalog(NOW)  # every market the bot prices, for the page's chips: the contract's daily card here, flagged when skipped
    assert {(r["kind"], r["key"], r["name"], r["skipped"]) for r in cat} >= {("close", "UNITREEUSDT", "宇树 UNITREE", False)}, cat
    assert len({(r["kind"], r["key"]) for r in cat}) == len(cat) and all(r["skipped"] == (r["key"].upper() in {"HSI"}) for r in cat), cat

    # --- a resting order is placed only on a book it could fill in: two-sided, no wider than SIM_MAKER_SPREAD ---------------
    assert m.SIM_MAKER_SPREAD == 0.10 and m.sim_maker_block(book([("0.55", "100")], [("0.58", "100")])) == ""
    assert m.sim_maker_block(book([("0.55", "100")], [])) == "盘口只有一边" and m.sim_maker_block(book([], [("0.58", "100")])) == "盘口只有一边"
    assert m.sim_maker_block(book([("0.003", "50")], [("0.80", "100")])) == "买卖价差 79.7¢ 超过 10.0¢"
    assert m.sim_maker_block(book([("0.60", "50")], [("0.58", "100")])).startswith("盘口交叉") and not m.book_crossed(book([("0.55", "1")], [("0.58", "1")]))
    # SIM_MAKER_SPREAD_CENTS=0 (the default since 1.51.0): any gap and a one-sided book are quoted on, a crossed book never
    assert m.sim_maker_block(book([("0.003", "50")], [("0.80", "100")]), 0) == "" and m.sim_maker_block(book([("0.55", "100")], []), 0) == ""
    assert m.sim_maker_block(book([("0.60", "50")], [("0.58", "100")]), 0).startswith("盘口交叉") and m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}).sim_maker_spread == 0
    gbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                                    "SIM_WAYS": "both", "SIM_MARKETS": "all", **LEGACY}), m.Store(":memory:"), FM(NOW), None)
    gbot.sim_markets = lambda now: world["markets"]
    lone = lambda name, bids, asks, fair: m.SimMarket(name, name, "close", "X", fair, book(bids, asks, NOW, name, mid=name), 0.03, "", ("涨", "跌"), {})
    # the 10-03 price-ladder case: a lone 0.3¢ bid under an 80¢ ask shows 挂涨 +58.7¢, but nobody sells into it: no order;
    # a one-sided book neither; a 10¢ spread is still a book
    await step(gbot, NOW, lone("lone", [("0.003", "50")], [("0.80", "100")], 0.59), lone("oneside", [("0.55", "100")], [], 0.70),
               lone("tight", [("0.55", "100")], [("0.65", "100")], 0.70))
    assert sorted(gbot.sim_trades()) == ["tight|up|挂"], sorted(gbot.sim_trades())

    # --- a resting order the trader no longer places is withdrawn, not left to the result ----------------------------------
    # (10-03: 54 price-ladder makers at 0.1–0.3¢ from before the ladders went taker-only sat as 挂单中 0/100 until month end)
    resting = lambda market, kind, shares=0.0: {
        "v": 2, "market": market, "slug": market.partition("#")[0], "market_id": market.partition("#")[2], "item": market, "kind": kind,
        "key": "X", "side": "down", "label": "挂No", "maker": True, "fair": 0.59, "opened": NOW - 3_600_000, "settle": {"end": BJ(11, 1, 11, 59)},
        "order": 100.0, "fills": [{"at": NOW - 1_800_000, "shares": shares, "fair": 0.5, "how": "推定成交"}] if shares else [], "revisions": [],
        "entry": {}, "version": {}, "price": 0.003, "signal": 0.587, "shares": shares, "status": "resting",
        "filled": NOW - 1_800_000 if shares else None, "queue_ahead": 50.0, "queue_min": 50.0, "edge": 0.587}
    for b in (dbot, gbot):
        b.store.put("sim:sol#11|down|挂", resting("sol#11", "range"))
        b.store.put("sim:sol#12|down|挂", resting("sol#12", "range", 30.0))
    gbot.store.put("sim:hsi-x|up|挂", resting("hsi-x", "close"))  # a kind and a way gbot still trades
    mbot.store.put("sim:hsi-y|up|挂", resting("hsi-y", "close"))  # mbot trades touch markets only
    kbot.store.put("sim:hsi-z|up|挂", {**resting("hsi-z", "close"), "key": "hsi"})  # kbot leaves HSI out by the detail chips
    kbot.store.put("sim:hsi-w|up|挂", {**resting("hsi-w", "close"), "key": "KOSPI"})
    world["markets"] = []
    for b in (dbot, gbot, mbot, kbot):
        await step(b, NOW + 60_000)
    k = kbot.sim_trades()
    assert k["hsi-z|up|挂#1"]["withdrawn"]["why"] == "详细选项已排除hsi-z" and "hsi-z|up|挂" not in k  # withdrawn with nothing filled: the slot is free
    assert k["hsi-w|up|挂"]["status"] == "resting" and "withdrawn" not in k["hsi-w|up|挂"]
    t = dbot.sim_trades()["sol#11|down|挂#1"]
    assert t["status"] == "cancelled" and t["note"] == "撤单：模拟交易已改为只吃单，一份都没成交", t
    assert t["withdrawn"] == {"at": NOW + 60_000, "why": "模拟交易已改为只吃单", "unfilled": 100.0} and t["unfilled"] == 100
    assert m.sim_status(t) == "已撤单" and m.sim_state(t) == "" and not dbot.sim_due(t, BJ(12, 1, 0, 0))  # nothing to confirm, ever
    t = dbot.sim_trades()["sol#12|down|挂"]  # what had filled stays a position, the rest lapses now
    assert t["status"] == "filled" and t["shares"] == 30 and t["unfilled"] == 70 and m.sim_status(t) == "持仓"
    assert t["note"] == "撤单：模拟交易已改为只吃单；已推定成交的 30 份继续持有，其余 70 份作废", t["note"]
    g = gbot.sim_trades()
    assert g["sol#11|down|挂#1"]["withdrawn"]["why"] == "价格阶梯只做吃单" and g["sol#12|down|挂"]["status"] == "filled"
    assert g["hsi-x|up|挂"]["status"] == "resting" and "withdrawn" not in g["hsi-x|up|挂"]  # still traded: it stands
    assert mbot.sim_trades()["hsi-y|up|挂#1"]["withdrawn"]["why"] == "模拟交易范围已不含指数/个股日涨跌"
    await step(dbot, NOW + 120_000)  # withdrawn once: the record does not change again
    assert dbot.sim_trades()["sol#11|down|挂#1"]["withdrawn"]["at"] == NOW + 60_000
    tot = dbot.sim_report()["total"]
    assert tot["cancelled"] == 1 and tot["resting"] == 0 and tot["open"] == 2 and tot["partial"] == 1 and tot["expired"] == 0, tot
    fresh = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), m.Store(":memory:"), FM(NOW), None)
    assert "｜撤单 1 笔" in dbot.cmd_sim(None).text and "撤单" not in fresh.cmd_sim(None).text  # counted only when there are any
    j = {x["id"]: x for x in dbot.journal_payload()["trades"]}
    assert j["sol#11|down|挂#1"]["wait"] == "" and j["sol#11|down|挂#1"]["text"] == "已撤单"
    assert j["sol#12|down|挂"]["wait"] == "碰到档位即 Yes；否则等窗口结束 11-01 11:59 后按 No（11-01 12:59 起）"
    assert "已撤单" in dbot.journal_csv() and json.dumps(dbot.journal_payload())

    await browser_check(bot)
    print("SIM_OK")


async def browser_check(bot):
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        async_playwright = None
    chrome = next((p for p in [os.environ.get("CHROMIUM_PATH", ""), "/opt/pw-browsers/chromium-1194/chrome-linux/chrome"]
                   if p and os.path.exists(p)), "")
    if async_playwright is None or not (chrome or os.environ.get("PLAYWRIGHT_BROWSERS_PATH")):
        print("browser check skipped (no Playwright/Chromium)")
        return
    web = m.WebServer(bot, 0, "t" * 20); web.CACHE_SECONDS = {}; port = await web.start()  # the tests change the payload and reload at once
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**({"executable_path": chrome} if chrome else {}))
        page = await browser.new_page(viewport={"width": 390, "height": 900})
        await page.goto(f"http://127.0.0.1:{port}/p/{'t' * 20}")
        await page.wait_for_selector(".meta span")
        # the 模拟交易 section starts hidden; 自定义 shows it again (and links the review page)
        assert not await page.is_visible("#g-sim .card") and not await page.is_visible("#h-sim")
        await page.click("#edit")
        assert await page.get_attribute("#journal", "href") == f"/p/{'t' * 20}/journal"
        await page.check("#secs input[data-sec=sim]")
        await page.click("#done")
        await page.wait_for_selector("#g-sim .card")
        text = await page.inner_text("#g-sim .card")
        tot = bot.sim_report()["total"]
        assert "已结算盈亏" in text and f"已结算 {tot['settled']} 笔" in text and "结果不一致 2 笔" in text and "成交时" in text, text
        assert await page.get_attribute("#g-sim a.simj", "href") == f"/p/{'t' * 20}/journal"
        await page.click("#g-sim details summary")
        assert await page.locator("#g-sim a.simrow").count() == len(bot.sim_report()["rows"])
        assert "· 结果不一致" in await page.inner_text("#g-sim .simrows")
        assert await page.evaluate("document.documentElement.scrollWidth <= window.innerWidth"), "no sideways scroll"
        if os.environ.get("WEB_SCREENSHOT"):
            await page.locator("#g-sim .card").screenshot(path=os.environ["WEB_SCREENSHOT"])
        await browser.close()
    await web.stop()


async def maker_rules():
    """The resting-order rules on by default: the valid 买1, following it, the exit line, the session, the freed slot."""
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                             "WEB_PORT": "8080", "SIM_WAYS": "maker", "SIM_MARKETS": "all", "SIM_MAKER_SPREAD_CENTS": "10"})  # the 10¢ spread rule on
    assert cfg.sim_maker_min_bid == 100 and cfg.sim_maker_exit == 0.05 and cfg.sim_maker_session and cfg.sim_maker_spread == 0.10
    for bad in ({"SIM_MAKER_MIN_BID": "-1"}, {"SIM_MAKER_EXIT_CENTS": "51"}, {"SIM_MAKER_EXIT_CENTS": "x"}):
        try: m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", **bad}); assert False, bad
        except ValueError: pass
    bot = m.Bot(cfg, m.Store(":memory:"), FM(NOW), None)
    bot.sim_markets = lambda now: world["markets"]
    active = {"rewards": {"current": {"hourlyRate": "10", "startsAt": "2026-10-01T00:00:00Z", "endsAt": "2026-12-31T00:00:00Z"}}, "trading_status": "OPEN"}
    bot.predict.market_meta["101"] = (active, time.monotonic())  # the HSI market pays points: its resting orders are allowed
    assert cfg.sim_maker_points and bot.control_value("SIM_MAKER_POINTS") == "on" and bot.sim_version()["sim_maker_points"] is True
    assert bot.control_value("SIM_MAKER_MIN_BID") == "100" and bot.control_value("SIM_MAKER_EXIT_CENTS") == "5" and bot.control_value("SIM_MAKER_SESSION") == "on"
    assert bot.sim_version()["sim_maker_min_bid"] == 100 and bot.sim_version()["sim_maker_exit"] == 0.05 and bot.sim_version()["sim_maker_session"] is True
    # maker_level: the first level deep enough, our own order not counted
    b = book([("0.56", "5"), ("0.55", "300"), ("0.53", "500")], [("0.58", "400")])
    assert m.maker_level(b, "up", 100) == (0.55, 300.0) and m.maker_level(b, "up", 0) == (0.56, 5.0) and m.maker_level(b, "up", 400) == (0.53, 500.0)
    assert m.maker_level(b, "up", 1000) is None and m.maker_level(b, "up", 100, own=250.0, own_price=0.55) == (0.53, 500.0)
    assert m.maker_level(b, "down", 100) == (0.42, 400.0)  # 跌 bids are 1 − the asks, as a clean price
    # a 5-share 买1 is not followed: the order joins the 300 at 55¢ behind them
    await step(bot, NOW, hsi(0.70, [("0.56", "5"), ("0.55", "300")], [("0.58", "400")], NOW))
    tid = f"{HSI_SLUG}|up|挂"
    t = bot.sim_trades()[tid]
    assert list(bot.sim_trades()) == [tid] and t["price"] == 0.55 and t["queue_ahead"] == 300 and abs(t["edge"] - 0.15) < 1e-9
    # only thin bids: nothing to follow, the resting order is withdrawn and its slot freed (the attempt kept as #1)
    await step(bot, NOW + 10_000, hsi(0.70, [("0.56", "5"), ("0.55", "30")], [("0.58", "400")], NOW + 10_000))
    trades = bot.sim_trades()
    assert sorted(trades) == [f"{tid}#1"] and trades[f"{tid}#1"]["status"] == "cancelled" and trades[f"{tid}#1"]["note"] == "撤单：买1 不足 100 份，不跟，一份都没成交"
    await step(bot, NOW + 20_000, hsi(0.70, [("0.56", "5"), ("0.55", "30")], [("0.58", "400")], NOW + 20_000))
    assert sorted(bot.sim_trades()) == [f"{tid}#1"]  # still nothing to join
    # the 买1 is back, deep enough: a fresh order; then it moves up two cents → withdrawn, re-placed at the new level next step
    await step(bot, NOW + 30_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 30_000))
    assert sorted(bot.sim_trades()) == [tid, f"{tid}#1"] and bot.sim_trades()[tid]["price"] == 0.55
    await step(bot, NOW + 40_000, hsi(0.70, [("0.57", "150"), ("0.55", "300")], [("0.58", "400")], NOW + 40_000))
    trades = bot.sim_trades()
    assert sorted(trades) == [f"{tid}#1", f"{tid}#2"] and trades[f"{tid}#2"]["note"] == "撤单：买1 移到 57.0¢，改跟，一份都没成交" and trades[f"{tid}#2"]["price"] == 0.55
    await step(bot, NOW + 50_000, hsi(0.70, [("0.57", "150"), ("0.55", "300")], [("0.58", "400")], NOW + 50_000))
    trades = bot.sim_trades()
    assert trades[tid]["price"] == 0.57 and trades[tid]["queue_ahead"] == 150 and abs(trades[tid]["edge"] - 0.13) < 1e-9  # re-placed at the new 买1
    # a half-cent move is not followed
    await step(bot, NOW + 60_000, hsi(0.70, [("0.575", "150"), ("0.55", "300")], [("0.58", "400")], NOW + 60_000))
    assert bot.sim_trades()[tid]["price"] == 0.57 and bot.sim_trades()[tid]["status"] == "resting"
    # the edge falls under the exit line (5¢): withdrawn; back above the bar: placed again
    await step(bot, NOW + 70_000, hsi(0.61, [("0.57", "150"), ("0.55", "300")], [("0.58", "400")], NOW + 70_000))
    trades = bot.sim_trades()
    assert tid not in trades and trades[f"{tid}#3"]["note"] == "撤单：净优势降到 4.0¢，低于撤单线 5.0¢，一份都没成交"
    await step(bot, NOW + 80_000, hsi(0.66, [("0.57", "150"), ("0.55", "300")], [("0.58", "400")], NOW + 80_000))
    assert tid not in bot.sim_trades()  # 9¢: under the 10¢ bar, not placed
    await step(bot, NOW + 90_000, hsi(0.70, [("0.57", "150"), ("0.55", "300")], [("0.58", "400")], NOW + 90_000))
    assert bot.sim_trades()[tid]["price"] == 0.57
    # SIM_MAKER_SESSION: the underlying not trading → withdrawn, nothing placed; trading again → placed
    closed = m.dataclasses.replace(hsi(0.70, [("0.57", "150"), ("0.55", "300")], [("0.58", "400")], NOW + 100_000), in_session=False)
    await step(bot, NOW + 100_000, closed)
    trades = bot.sim_trades()
    assert tid not in trades and trades[f"{tid}#4"]["note"] == "撤单：标的未开盘，不挂单，一份都没成交"
    await step(bot, NOW + 110_000, closed)
    assert tid not in bot.sim_trades()
    await step(bot, NOW + 120_000, m.dataclasses.replace(closed, in_session=True))
    assert bot.sim_trades()[tid]["price"] == 0.57
    # --- the after-hours maker mode (SIM_MAKER_AFTER_HOURS_CENTS): after the close a daily card is still quoted on, but only
    # with that much edge (never under the ordinary bar); a cent of slack keeps an order from being withdrawn and re-placed on
    # a few tenths; the close re-judges every resting order, the open brings back the ordinary bar and exit line
    assert cfg.sim_maker_after_hours == 0 and bot.control_value("SIM_MAKER_AFTER_HOURS_CENTS") == "0" and "sim_maker_after_hours" not in bot.sim_version()
    for bad in ({"SIM_MAKER_AFTER_HOURS_CENTS": "51"}, {"SIM_MAKER_AFTER_HOURS_CENTS": "-1"}, {"SIM_MAKER_AFTER_HOURS_CENTS": "x"}):
        try: m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", **bad}); assert False, bad
        except ValueError: pass
    abot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
                                    "SIM_WAYS": "maker", "SIM_MARKETS": "all", "SIM_EDGE_CENTS": "5", "SIM_MAKER_EXIT_CENTS": "2",
                                    "SIM_MAKER_AFTER_HOURS_CENTS": "10"}), m.Store(":memory:"), FM(NOW), None)
    abot.sim_markets = lambda now: world["markets"]
    abot.predict.market_meta["101"] = (active, time.monotonic())
    assert abot.config.sim_maker_after_hours == 0.10 and abot.control_value("SIM_MAKER_AFTER_HOURS_CENTS") == "10" and abot.sim_version()["sim_maker_after_hours"] == 0.10
    assert "盘后挂单模式：指数/个股日涨跌开盘前、收盘后也挂，但只挂净优势 ≥ 10.0¢ 的，降到 9.0¢ 以下撤掉" in abot.sim_text() and "开盘前、收盘后撤掉" not in abot.sim_text()
    after = lambda fair, at: m.dataclasses.replace(hsi(fair, [("0.57", "150"), ("0.55", "300")], [("0.58", "400")], at), in_session=False)
    open_ = lambda fair, at: m.dataclasses.replace(after(fair, at), in_session=True)
    crypto = m.SimMarket("will-bnb-hit-700-or-900", "BNB 先触 700/900", "touch", "BNB", 0.70, book([("0.55", "100")], [("0.58", "100")], NOW), 0.03, "",
                         ("$900", "$700"), {"deadline": NOW + 3_600_000}, {})  # in_session None: no session, never "after hours"
    assert abot.sim_after_hours(after(0.70, NOW)) and not abot.sim_after_hours(open_(0.70, NOW)) and not abot.sim_after_hours(crypto)
    assert abot.sim_maker_bar(after(0.70, NOW)) == 0.10 and abot.sim_maker_bar(open_(0.70, NOW)) == 0.05 and abot.sim_maker_bar(crypto) == 0.05
    assert abot.sim_maker_reason(after(0.64, NOW), {}, NOW) == (None, "盘后只挂净优势 ≥ 10.0¢ 的，现 7.0¢")  # 7¢ clears the 5¢ bar in session only
    assert abot.sim_maker_reason(open_(0.64, NOW), {}, NOW)[0] is not None and bot.sim_maker_reason(after(0.70, NOW), {}, NOW) == (None, "标的未开盘，不挂单")
    await step(abot, NOW, after(0.64, NOW))
    assert tid not in abot.sim_trades() and [r["maker_why"] for r in abot.sim_checks(NOW)] == ["盘后只挂净优势 ≥ 10.0¢ 的，现 7.0¢"]
    await step(abot, NOW + 10_000, after(0.70, NOW + 10_000))  # 13¢: placed after hours
    t = abot.sim_trades()[tid]
    assert t["price"] == 0.57 and abs(t["edge"] - 0.13) < 1e-9 and t["version"]["sim_maker_after_hours"] == 0.10, t
    await step(abot, NOW + 20_000, after(0.665, NOW + 20_000))  # 9.5¢: within the cent of slack under the bar, it stands
    assert abot.sim_trades()[tid]["status"] == "resting"
    await step(abot, NOW + 30_000, after(0.655, NOW + 30_000))  # 8.5¢: under the after-hours line
    trades = abot.sim_trades()
    assert tid not in trades and trades[f"{tid}#1"]["note"] == "撤单：盘后只挂净优势 ≥ 10.0¢ 的，现 8.5¢ 低于盘后撤单线 9.0¢，一份都没成交", trades[f"{tid}#1"]["note"]
    # in session the ordinary bar: a 7¢ order is placed; the close re-judges it by the after-hours line and withdraws it
    await step(abot, NOW + 40_000, open_(0.64, NOW + 40_000))
    assert abot.sim_trades()[tid]["status"] == "resting"
    await step(abot, NOW + 50_000, after(0.64, NOW + 50_000))
    trades = abot.sim_trades()
    assert tid not in trades and trades[f"{tid}#2"]["note"] == "撤单：盘后只挂净优势 ≥ 10.0¢ 的，现 7.0¢ 低于盘后撤单线 9.0¢，一份都没成交"
    # a 12¢ order survives the close; the open brings back the ordinary exit line (2¢ here): a 3¢ edge stands in session
    await step(abot, NOW + 60_000, open_(0.69, NOW + 60_000))
    await step(abot, NOW + 70_000, after(0.69, NOW + 70_000))
    assert abot.sim_trades()[tid]["status"] == "resting" and "withdrawn" not in abot.sim_trades()[tid]
    await step(abot, NOW + 80_000, open_(0.60, NOW + 80_000))
    assert abot.sim_trades()[tid]["status"] == "resting" and len([k for k in abot.sim_trades() if k.startswith(tid)]) == 3
    # the mode applies whatever SIM_MAKER_SESSION says (盘前盘后也挂 still needs the after-hours edge after hours), and an
    # after-hours bar under the ordinary one never loosens it
    obot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
                                    "SIM_WAYS": "maker", "SIM_MARKETS": "all", "SIM_EDGE_CENTS": "5", "SIM_MAKER_SESSION": "off",
                                    "SIM_MAKER_AFTER_HOURS_CENTS": "10"}), m.Store(":memory:"), FM(NOW), None)
    obot.predict.market_meta["101"] = (active, time.monotonic())
    assert obot.sim_maker_reason(after(0.64, NOW), {}, NOW) == (None, "盘后只挂净优势 ≥ 10.0¢ 的，现 7.0¢") and obot.sim_maker_reason(after(0.70, NOW), {}, NOW)[0] is not None
    lbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_WAYS": "maker", "SIM_EDGE_CENTS": "10", "SIM_MAKER_AFTER_HOURS_CENTS": "5"}),
                 m.Store(":memory:"), FM(NOW), None)
    lbot.predict.market_meta["101"] = (active, time.monotonic())
    assert lbot.sim_maker_bar(after(0.70, NOW)) == 0.10 and abs(lbot.sim_maker_line(after(0.70, NOW)) - 0.09) < 1e-12
    assert lbot.sim_maker_reason(after(0.66, NOW), {}, NOW) == (None, "盘后只挂净优势 ≥ 10.0¢ 的，现 9.0¢")
    # --- the deep-bid mode (SIM_MAKER_DEEP_CENTS): a wide or one-sided book, where no ordinary resting order goes, is quoted
    # on at the valid 买1 when the edge is that large — the 10-08 book: fair 38¢, bids 5¢×300 / 1¢×1000, asks 38¢×1 / 40¢×100 …
    assert cfg.sim_maker_deep == 0 and bot.control_value("SIM_MAKER_DEEP_CENTS") == "0" and "sim_maker_deep" not in bot.sim_version()
    for bad in ({"SIM_MAKER_DEEP_CENTS": "91"}, {"SIM_MAKER_DEEP_CENTS": "-1"}):
        try: m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", **bad}); assert False, bad
        except ValueError: pass
    wide = lambda fair, at, bids=(("0.05", "300"), ("0.01", "1000")), asks=(("0.38", "1"), ("0.40", "100"), ("0.98", "500"), ("0.99", "1200")): \
        m.dataclasses.replace(hsi(fair, list(bids), list(asks), at), in_session=True)
    tight = lambda fair, at, asks=(("0.58", "400"),): hsi(fair, [("0.57", "150"), ("0.55", "300")], list(asks), at)
    assert bot.sim_maker_reason(wide(0.38, NOW), {}, NOW) == (None, "买卖价差 33.0¢ 超过 10.0¢")  # the ordinary rule: nobody sells into it
    dbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
                                    "SIM_WAYS": "maker", "SIM_MARKETS": "all", "SIM_MAKER_DEEP_CENTS": "25", "SIM_MAKER_SPREAD_CENTS": "10"}),
                 m.Store(":memory:"), FM(NOW), None)
    dbot.sim_markets = lambda now: world["markets"]
    dbot.predict.market_meta["101"] = (active, time.monotonic())
    assert dbot.config.sim_maker_deep == 0.25 and dbot.control_value("SIM_MAKER_DEEP_CENTS") == "25" and dbot.sim_version()["sim_maker_deep"] == 0.25
    assert "挂单平时只挂在双边都有报价、价差不超过 10¢ 的盘口，排在已有挂单之后" in dbot.sim_text() and "挂单平时" not in bot.sim_text()
    assert "低价挂单模式：按挂价算净优势 ≥ 25.0¢ 的单挂在有效买1 之上 1¢ 优先成交，价差超过上限的盘口也挂、只挂这种单，这种单净优势降到 24.0¢ 以下撤掉" in dbot.sim_text()
    assert dbot.sim_deep_book(wide(0.38, NOW)) and not dbot.sim_deep_book(tight(0.70, NOW)) and not bot.sim_deep_book(wide(0.38, NOW))
    assert dbot.sim_maker_mode(wide(0.38, NOW)) == (0.25, "价差 33.0¢ 的盘口", "低价挂单撤单线") and dbot.sim_maker_mode(tight(0.70, NOW)) == (0.10, "", "撤单线")
    assert abs(dbot.sim_maker_line(wide(0.38, NOW)) - 0.24) < 1e-12 and dbot.sim_maker_line(tight(0.70, NOW)) == 0.05
    r = dbot.sim_maker_reason(wide(0.38, NOW), {}, NOW)[0]
    assert r is not None and (r.side, r.price, r.size) == ("涨", 0.06, 0.0) and abs(r.edge - 0.32) < 1e-9, r  # a cent above the 300 at 5¢, at the front: 32¢
    assert m.Bot.sim_maker_price(wide(0.38, NOW).book, "up", (0.05, 300.0), False) == (0.05, 300.0)  # the ordinary order joins the level
    thin = wide(0.38, NOW, bids=(("0.06", "20"), ("0.05", "300"), ("0.01", "1000")))  # 20 thin shares at 6¢ over the valid 买1: a cent over them
    assert m.maker_level(thin.book, "up", 100) == (0.05, 300.0) and m.Bot.front_reference(thin.book, "up") == 0.06
    assert dbot.sim_maker_reason(thin, {}, NOW)[0].size == 0.0 and dbot.sim_maker_reason(thin, {}, NOW)[0].price == 0.07
    assert m.Bot.front_reference(wide(0.38, NOW, bids=(("0.09", "0.5"), ("0.05", "300"))).book, "up") == 0.05  # sub-share dust is not a bid
    assert m.Bot.front_reference(thin.book, "up", own=20.0, own_price=0.06) == 0.05  # our own real order at 6¢ is not someone else
    assert m.Bot.sim_maker_price(book([("0.05", "300")], [("0.06", "50")]), "up", (0.05, 300.0), True) == (0.05, 300.0)  # never at the ask: joins
    assert dbot.sim_maker_reason(wide(0.30, NOW), {}, NOW) == (None, "价差 33.0¢ 的盘口只挂净优势 ≥ 25.0¢ 的，现 24.0¢")  # judged at 6¢
    assert dbot.sim_maker_reason(wide(0.38, NOW, asks=()), {}, NOW)[0] is not None and dbot.sim_maker_mode(wide(0.38, NOW, asks=()))[1] == "单边盘口"
    assert dbot.sim_maker_reason(wide(0.38, NOW, bids=(("0.40", "300"),)), {}, NOW)[1].startswith("盘口交叉")  # a crossed book: never
    world["markets"] = [wide(0.38, NOW)]
    assert [r["maker"] for r in dbot.sim_checks(NOW)] == ["挂涨 6.0¢×100 份（净优势 32.0¢，买1 之上 1¢，排在最前）"]
    world["markets"] = [thin]
    assert [r["maker"] for r in dbot.sim_checks(NOW)] == ["挂涨 7.0¢×100 份（净优势 31.0¢，买1 之上 1¢，排在最前）"]
    await step(dbot, NOW, wide(0.38, NOW))
    t = dbot.sim_trades()[tid]
    assert t["price"] == 0.06 and t["queue_ahead"] == 0 and t["deep"] is True and abs(t["edge"] - 0.32) < 1e-9 and t["version"]["sim_maker_deep"] == 0.25, t
    # sub-share dust over the order is ignored; a 20-share bid at its price (a paper order is not in the book, so all of
    # it is someone else's) has pushed it off the front: re-quoted a cent over, on the next step
    await step(dbot, NOW + 5_000, wide(0.38, NOW + 5_000, bids=(("0.09", "0.5"), ("0.05", "300"), ("0.01", "1000"))))
    assert dbot.sim_trades()[tid]["status"] == "resting" and dbot.sim_trades()[tid]["price"] == 0.06
    await step(dbot, NOW + 6_000, wide(0.38, NOW + 6_000, bids=(("0.06", "20"), ("0.05", "300"), ("0.01", "1000"))))
    trades = dbot.sim_trades()
    assert tid not in trades and trades[f"{tid}#1"]["note"] == "撤单：买1 被顶到 6.0¢，改挂 7.0¢，一份都没成交", trades[f"{tid}#1"]["note"]
    await step(dbot, NOW + 7_000, wide(0.38, NOW + 7_000, bids=(("0.06", "20"), ("0.05", "300"), ("0.01", "1000"))))
    assert dbot.sim_trades()[tid]["price"] == 0.07 and dbot.sim_trades()[tid]["queue_ahead"] == 0 and dbot.sim_trades()[tid]["deep"] is True
    await step(dbot, NOW + 10_000, wide(0.315, NOW + 10_000, bids=(("0.06", "20"), ("0.05", "300"), ("0.01", "1000"))))  # 24.5¢ at 7¢: within the slack
    assert dbot.sim_trades()[tid]["status"] == "resting"
    # pushed again, but a cent over the new bid would leave under 25¢: it stays where it is rather than chase
    await step(dbot, NOW + 12_000, wide(0.38, NOW + 12_000, bids=(("0.13", "40"), ("0.06", "20"), ("0.05", "300"))))
    assert dbot.sim_trades()[tid]["status"] == "resting" and dbot.sim_trades()[tid]["price"] == 0.07
    # 300 shares land at its price: pushed, re-placed a cent over them on the next step
    await step(dbot, NOW + 15_000, wide(0.38, NOW + 15_000, bids=(("0.07", "300"), ("0.01", "1000"))))
    trades = dbot.sim_trades()
    assert tid not in trades and trades[f"{tid}#2"]["note"] == "撤单：买1 被顶到 7.0¢，改挂 8.0¢，一份都没成交", trades[f"{tid}#2"]["note"]
    await step(dbot, NOW + 16_000, wide(0.38, NOW + 16_000, bids=(("0.07", "300"), ("0.01", "1000"))))
    trades = dbot.sim_trades()
    assert trades[tid]["price"] == 0.08 and trades[tid]["queue_ahead"] == 0 and abs(trades[tid]["edge"] - 0.30) < 1e-9 and trades[tid]["deep"] is True
    # the bids under it fall away: down to a cent over what is left
    await step(dbot, NOW + 17_000, wide(0.38, NOW + 17_000, bids=(("0.05", "300"), ("0.01", "1000"))))
    trades = dbot.sim_trades()
    assert tid not in trades and trades[f"{tid}#3"]["note"] == "撤单：买1 移到 5.0¢，改挂 6.0¢，一份都没成交", trades[f"{tid}#3"]["note"]
    await step(dbot, NOW + 18_000, wide(0.38, NOW + 18_000))
    assert dbot.sim_trades()[tid]["price"] == 0.06
    await step(dbot, NOW + 20_000, wide(0.29, NOW + 20_000))  # 23¢ at 6¢: under the deep-bid line
    trades = dbot.sim_trades()
    assert tid not in trades and trades[f"{tid}#4"]["note"] == "撤单：价差 33.0¢ 的盘口只挂净优势 ≥ 25.0¢ 的，现 23.0¢ 低于低价挂单撤单线 24.0¢，一份都没成交", trades[f"{tid}#4"]["note"]
    # a tight book keeps the ordinary bar and exit line, and an order placed there stands when the book thins out later
    await step(dbot, NOW + 30_000, tight(0.70, NOW + 30_000))
    t = dbot.sim_trades()[tid]
    assert t["price"] == 0.57 and "deep" not in t and abs(t["edge"] - 0.13) < 1e-9
    await step(dbot, NOW + 40_000, tight(0.70, NOW + 40_000, asks=(("0.90", "400"),)))  # the asks pulled: a 33¢ spread now
    assert dbot.sim_trades()[tid]["status"] == "resting" and "withdrawn" not in dbot.sim_trades()[tid]
    await step(dbot, NOW + 50_000, tight(0.61, NOW + 50_000, asks=(("0.90", "400"),)))  # 4¢: the ordinary exit line
    assert tid not in dbot.sim_trades() and dbot.sim_trades()[f"{tid}#5"]["note"].startswith("撤单：净优势降到 4.0¢，低于撤单线 5.0¢")
    # after hours with both modes the higher bar wins (the after-hours 30¢ here) and the order check names both
    bbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_WAYS": "maker", "SIM_MAKER_DEEP_CENTS": "25", "SIM_MAKER_AFTER_HOURS_CENTS": "30",
                                    "SIM_MAKER_SPREAD_CENTS": "10"}), m.Store(":memory:"), FM(NOW), None)
    bbot.predict.market_meta["101"] = (active, time.monotonic())
    closed_wide = m.dataclasses.replace(wide(0.33, NOW), in_session=False)
    assert bbot.sim_maker_mode(closed_wide) == (0.30, "盘后、价差 33.0¢ 的盘口", "盘后低价挂单撤单线") and abs(bbot.sim_maker_line(closed_wide) - 0.29) < 1e-12
    why = bbot.sim_maker_reason(closed_wide, {}, NOW)  # at 6¢; the 跌 side (a cent over its 62¢ 买1) is over the 60¢ after-hours limit
    assert why[0] is None and why[1].startswith("盘后、价差 33.0¢ 的盘口只挂净优势 ≥ 30.0¢ 的，现 27.0¢；") and why[1].endswith("的价位高于盘后上限 60.0¢"), why
    assert bbot.sim_maker_reason(m.dataclasses.replace(closed_wide, fair_up=0.36), {}, NOW)[0] is not None  # 30¢ clears both
    # each mode can be limited to chosen markets (the detail chips under its card): elsewhere it is as if the mode were off
    nbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_WAYS": "maker", "SIM_MAKER_DEEP_CENTS": "25", "SIM_MAKER_DEEP_MARKETS": "kospi",
                                    "SIM_MAKER_AFTER_HOURS_CENTS": "10", "SIM_MAKER_AFTER_HOURS_MARKETS": "KOSPI,SSE", "SIM_MAKER_SPREAD_CENTS": "10"}),
                 m.Store(":memory:"), FM(NOW), None)
    nbot.predict.market_meta["101"] = (active, time.monotonic())
    assert nbot.control_value("SIM_MAKER_DEEP_MARKETS") == "KOSPI" and nbot.control_value("SIM_MAKER_AFTER_HOURS_MARKETS") == "KOSPI,SSE"
    assert not nbot.sim_deep_book(wide(0.38, NOW)) and nbot.sim_maker_reason(wide(0.38, NOW), {}, NOW) == (None, "买卖价差 33.0¢ 超过 10.0¢（低价挂单模式未选这个市场）")
    assert not nbot.sim_after_hours(after(0.70, NOW)) and nbot.sim_maker_reason(after(0.70, NOW), {}, NOW) == (None, "标的未开盘，不挂单（盘后挂单模式未选这个市场）")
    assert nbot.sim_version()["sim_maker_deep_markets"] == ["KOSPI"] and nbot.sim_version()["sim_maker_after_hours_markets"] == ["KOSPI", "SSE"]
    assert "以下撤掉（SIM_MAKER_DEEP_CENTS），只做 KOSPI" in nbot.sim_text() and "以下撤掉（SIM_MAKER_AFTER_HOURS_CENTS），只做 KOSPI、SSE" in nbot.sim_text()
    kospi_wide, kospi_after = m.dataclasses.replace(wide(0.38, NOW), key="KOSPI"), m.dataclasses.replace(after(0.70, NOW), key="kospi")
    assert nbot.sim_deep_book(kospi_wide) and nbot.sim_maker_reason(kospi_wide, {}, NOW)[0] is not None  # chosen (keys compare upper-cased)
    assert nbot.sim_after_hours(kospi_after) and nbot.sim_maker_reason(kospi_after, {}, NOW)[0] is not None
    assert "sim_maker_deep_markets" not in dbot.sim_version() and dbot.sim_maker_reason(wide(0.38, NOW), {}, NOW)[0] is not None  # no list: every market
    # a maker mode set while SIM_WAYS=taker (the 10-08 case: 低价挂单 25¢ on, 挂单 off) places nothing: the order check and /sim say so
    tbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_MAKER_DEEP_CENTS": "25"}), m.Store(":memory:"), FM(NOW), None)
    assert tbot.sim_maker_reason(wide(0.38, NOW), {}, NOW) == (None, "SIM_WAYS=taker：只吃单（盘后挂单 / 低价挂单模式要先打开挂单）")
    assert "低价挂单模式已设置，但现在只吃单，不会挂单" in tbot.sim_text()
    plain = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}), m.Store(":memory:"), FM(NOW), None)
    assert plain.sim_maker_reason(wide(0.38, NOW), {}, NOW) == (None, "SIM_WAYS=taker：只吃单") and "不会挂单" not in plain.sim_text()
    # --- no spread limit by default (SIM_MAKER_SPREAD_CENTS=0): the 10-08 ask "价差大也挂，比如两边都是 5，公平价 40" — bids 5¢ on
    # both sides (涨 5¢×300; 跌 5¢ = a 95¢ ask ×200), fair 40¢: the 跌 bid is joined at 5¢ with 55¢ of edge; the deep-bid mode
    # then puts it a cent in front, on this and any book, when the edge there clears its bar
    both5 = lambda **env: m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_WAYS": "maker", **env}), m.Store(":memory:"), FM(NOW), None)
    ubot = both5()
    ubot.predict.market_meta["101"] = (active, time.monotonic())
    five = m.dataclasses.replace(hsi(0.40, [("0.05", "300")], [("0.95", "200")], NOW), in_session=True)
    assert ubot.config.sim_maker_spread == 0 and m.sim_maker_block(five.book, 0) == "" and not ubot.sim_deep_book(five)
    r = ubot.sim_maker_reason(five, {}, NOW)[0]
    assert r is not None and (r.side, r.price, r.size, r.front) == ("跌", 0.05, 200.0, False) and abs(r.edge - 0.55) < 1e-9, r
    assert ubot.sim_maker_reason(wide(0.38, NOW, asks=()), {}, NOW)[0].price == 0.05  # one-sided: joined too
    assert "挂单不限价差，单边盘口也挂，跟有效买1（SIM_MAKER_SPREAD_CENTS=0）" in ubot.sim_text() and "sim_maker_spread" not in ubot.sim_version()
    fbot = both5(SIM_MAKER_DEEP_CENTS="25")
    fbot.predict.market_meta["101"] = (active, time.monotonic())
    r = fbot.sim_maker_reason(five, {}, NOW)[0]
    assert (r.side, r.price, r.size, r.front) == ("跌", 0.06, 0.0, True) and abs(r.edge - 0.54) < 1e-9, r  # a cent in front: 54¢ still clears 25¢
    r = fbot.sim_maker_reason(tight(0.70, NOW, asks=(("0.60", "400"),)), {}, NOW)[0]  # 买1 57¢×150, 13¢ of edge: too little for the cent, joins
    assert (r.price, r.size, r.front) == (0.57, 150.0, False), r
    r = fbot.sim_maker_reason(tight(0.90, NOW, asks=(("0.60", "400"),)), {}, NOW)[0]  # 33¢ of edge at 57¢: a cent in front at 58¢
    assert (r.price, r.size, r.front) == (0.58, 0.0, True) and abs(r.edge - 0.32) < 1e-9, r
    assert fbot.sim_maker_reason(tight(0.90, NOW), {}, NOW)[0].price == 0.57  # the ask sits at 58¢: never at the ask, joins
    fbot.sim_markets = lambda now: world["markets"]
    await step(fbot, NOW, five)
    t = fbot.sim_trades()[f"{HSI_SLUG}|down|挂"]
    assert t["price"] == 0.06 and t["deep"] is True and t["queue_ahead"] == 0 and abs(t["edge"] - 0.54) < 1e-9, t
    assert "低价挂单模式：按挂价算净优势 ≥ 25.0¢ 的单挂在有效买1 之上 1¢ 优先成交，这种单净优势降到 24.0¢ 以下撤掉" in fbot.sim_text()
    # SIM_MAKER_DEEP_ONLY: only the deep-bid orders — a joining order is not placed, a resting one is withdrawn, and a
    # deep-bid order is held to the mode's line on any book
    obot2 = both5(SIM_MAKER_DEEP_CENTS="25", SIM_MAKER_DEEP_ONLY="on")
    obot2.predict.market_meta["101"] = (active, time.monotonic())
    assert obot2.config.sim_maker_deep_only and obot2.control_value("SIM_MAKER_DEEP_ONLY") == "on" and obot2.sim_version()["sim_maker_deep_only"] is True
    assert "sim_maker_deep_only" not in fbot.sim_version() and "sim_maker_deep_only" not in both5(SIM_MAKER_DEEP_ONLY="on").sim_version()  # needs the mode
    r = obot2.sim_maker_reason(tight(0.70, NOW, asks=(("0.60", "400"),)), {}, NOW)[0]  # the front (58¢) has 12¢: a sweep order at 1¢ instead (1.55.0)
    assert r is not None and (r.price, r.sweep, r.front) == (0.01, True, True) and abs(r.edge - 0.69) < 1e-9, r
    assert obot2.sim_maker_reason(five, {}, NOW)[0].front and obot2.sim_maker_reason(tight(0.90, NOW, asks=(("0.60", "400"),)), {}, NOW)[0].price == 0.58
    assert "；只挂低价挂单：买1 之上 1¢ 够门槛就挂在那里，不够就挂击穿单" in obot2.sim_text() and "只挂低价挂单" not in fbot.sim_text()
    obot2.sim_markets = lambda now: world["markets"]
    obot2.config = m.dataclasses.replace(obot2.config, sim_maker_deep_only=False)
    await step(obot2, NOW, tight(0.70, NOW, asks=(("0.60", "400"),)))  # an ordinary joining order first…
    assert obot2.sim_trades()[tid]["price"] == 0.57 and "deep" not in obot2.sim_trades()[tid]
    obot2.config = m.dataclasses.replace(obot2.config, sim_maker_deep_only=True)
    await step(obot2, NOW + 10_000, tight(0.70, NOW + 10_000, asks=(("0.60", "400"),)))  # …withdrawn once only the deep-bid ones are wanted
    trades = obot2.sim_trades()
    assert tid not in trades and trades[f"{tid}#1"]["note"] == "撤单：已改为只挂低价挂单，平时的挂单撤掉，一份都没成交", trades[f"{tid}#1"]["note"]
    await step(obot2, NOW + 20_000, tight(0.90, NOW + 20_000, asks=(("0.60", "400"),)))  # 33¢ at 57¢: a cent in front at 58¢
    t = obot2.sim_trades()[tid]
    assert t["price"] == 0.58 and t["deep"] is True and abs(t["edge"] - 0.32) < 1e-9, t
    await step(obot2, NOW + 30_000, tight(0.81, NOW + 30_000, asks=(("0.60", "400"),)))  # 23¢: under the line, on a book the ordinary rule accepts
    trades = obot2.sim_trades()
    assert tid not in trades and trades[f"{tid}#2"]["note"] == "撤单：低价挂单只挂净优势 ≥ 25.0¢ 的，现 23.0¢ 低于低价挂单撤单线 24.0¢，一份都没成交", trades[f"{tid}#2"]["note"]
    # --- the after-hours taker mode and the after-hours price limit (10-08): outside the session a daily card is taken only
    # with SIM_TAKER_AFTER_HOURS_CENTS of edge, and nothing, taker or maker, is bought over SIM_AFTER_HOURS_MAX_PRICE_CENTS
    hbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
                                    "SIM_WAYS": "both", "SIM_MARKETS": "all", "SIM_TAKER_SESSION": "on", "SIM_TAKER_AFTER_HOURS_CENTS": "15",
                                    "SIM_MAKER_SESSION": "off"}), m.Store(":memory:"), FM(NOW), None)
    hbot.sim_markets = lambda now: world["markets"]
    hbot.predict.market_meta["101"] = (active, time.monotonic())
    assert hbot.control_value("SIM_TAKER_AFTER_HOURS_CENTS") == "15" and hbot.control_value("SIM_AFTER_HOURS_MAX_PRICE_CENTS") == "60"
    assert hbot.sim_version()["sim_taker_after_hours"] == 0.15 and hbot.sim_version()["sim_after_hours_max_price"] == 0.60
    assert "盘后吃单模式：指数/个股日涨跌开盘前、收盘后也吃，但只吃净优势 ≥ 15.0¢ 的" in hbot.sim_text() and "只在标的开盘时段吃单" not in hbot.sim_text()
    assert "开盘前、收盘后只买 60.0¢ 以下的：吃单只买这个价以下的卖单，挂单只挂这个价以下的" in hbot.sim_text()
    closed = lambda fair, bids, asks, at: m.dataclasses.replace(hsi(fair, list(bids), list(asks), at), in_session=False)
    opened = lambda fair, bids, asks, at: m.dataclasses.replace(hsi(fair, list(bids), list(asks), at), in_session=True)
    bps = hbot.config.predict_fee_bps
    assert hbot.sim_taker_bar(closed(0.80, [], [], NOW)) == 0.15 and hbot.sim_taker_bar(opened(0.80, [], [], NOW)) == 0.10 and hbot.sim_after_hours_price(opened(0.80, [], [], NOW)) is None
    assert hbot.sim_after_hours_price(closed(0.80, [], [], NOW)) == 0.60 and hbot.sim_taker_cap(closed(0.95, [], [], NOW), "up", bps) == 0.60  # capped by the limit
    assert abs(hbot.sim_taker_cap(opened(0.95, [], [], NOW), "up", bps) - m.taker_cap(0.95, 0.10, bps)) < 1e-12  # in session: the ordinary cap
    tk = f"{HSI_SLUG}|up|吃"
    await step(hbot, NOW, closed(0.68, [("0.50", "300")], [("0.55", "100")], NOW))  # ~11¢ after the fee: enough in session, not after hours
    assert tk not in hbot.sim_trades() and [r["taker"] for r in hbot.sim_checks(NOW)][0].startswith("盘后：涨：封顶 5")
    await step(hbot, NOW + 10_000, closed(0.80, [("0.50", "300")], [("0.55", "100")], NOW + 10_000))  # ~23¢: taken after hours
    t = hbot.sim_trades()[tk]
    assert t["status"] == "filled" and t["shares"] == 100 and t["version"]["sim_taker_after_hours"] == 0.15, t
    # a 65¢ ask with 30¢ of edge: not after hours (over the 60¢ limit), bought in session
    pricey = lambda at, in_session: m.dataclasses.replace(hsi(0.95, [("0.60", "300")], [("0.65", "100")], at, key="X2"), market="x2", in_session=in_session)
    await step(hbot, NOW + 20_000, pricey(NOW + 20_000, False))
    assert "x2|up|吃" not in hbot.sim_trades() and [r["taker"] for r in hbot.sim_checks(NOW + 20_000)] == ["盘后：涨：封顶 60.0¢，之下没有卖单；跌：无封顶价（公平价 5.0¢ 留不出 15.0¢）"]
    await step(hbot, NOW + 30_000, pricey(NOW + 30_000, True))
    assert hbot.sim_trades()["x2|up|吃"]["status"] == "filled" and abs(hbot.sim_trades()["x2|up|吃"]["best"] - 0.65) < 1e-9
    # a resting order over the limit: not placed after hours (the cheap side has no edge), placed in session, withdrawn at the close
    dear = lambda at, in_session: m.dataclasses.replace(hsi(0.10, [("0.05", "300")], [("0.30", "200")], at, key="X3"), market="x3", in_session=in_session)
    assert hbot.sim_maker_reason(dear(NOW, False), {}, NOW) == (None, "净优势 5.0¢ 低于触发门槛 10.0¢；70.0¢ 的价位高于盘后上限 60.0¢")  # the cheap side has no edge
    assert hbot.sim_maker_reason(m.dataclasses.replace(dear(NOW, False), book=book([], [("0.30", "200")], NOW, mid="101")), {}, NOW) == (None, "盘后只挂 60.0¢ 以下的，可挂的价位 70.0¢ 高于它")
    assert hbot.sim_maker_reason(dear(NOW, True), {}, NOW)[0].price == 0.70
    await step(hbot, NOW + 40_000, dear(NOW + 40_000, True))
    assert hbot.sim_trades()["x3|down|挂"]["price"] == 0.70
    await step(hbot, NOW + 50_000, dear(NOW + 50_000, False))
    trades = hbot.sim_trades()
    assert "x3|down|挂" not in trades and trades["x3|down|挂#1"]["note"] == "撤单：盘后只挂 60.0¢ 以下的，70.0¢ 的单撤掉，一份都没成交", trades["x3|down|挂#1"]["note"]
    # the sweep order (10-08): with only the deep-bid orders wanted and the front of the book too close to the fair price, the
    # order goes deep under it — a cent over the next bid under fair − 25¢ — and waits for the book to be swept
    hynix = lambda fair, at, bids=(("0.48", "3430"), ("0.01", "1000")), asks=(("0.53", "1720"), ("0.99", "1000")): \
        m.dataclasses.replace(hsi(fair, list(bids), list(asks), at), in_session=False)  # before the open: the after-hours rules too
    assert m.Bot.sweep_price(hynix(0.50, NOW).book, "up", 0.50, 0.25) == (0.02, 0.0, 3430.0)
    assert m.Bot.sweep_price(hynix(0.50, NOW, bids=(("0.48", "3430"),)).book, "up", 0.50, 0.25) == (0.01, 0.0, 3430.0)  # nothing under: 1¢
    assert m.Bot.sweep_price(hynix(0.50, NOW, bids=(("0.48", "3430"), ("0.10", "200"), ("0.01", "1000"))).book, "up", 0.50, 0.25) == (0.11, 0.0, 3430.0)
    assert m.Bot.sweep_price(hynix(0.50, NOW, bids=(("0.48", "3430"), ("0.25", "50"), ("0.02", "20"))).book, "up", 0.50, 0.25) == (0.03, 0.0, 3480.0)  # 25¢ sits at the threshold: under it
    assert m.Bot.sweep_price(hynix(0.50, NOW).book, "up", 0.50, 0.25, own=100.0, own_price=0.02) == (0.02, 0.0, 3430.0)  # our own order at 2¢ is not someone else
    assert m.Bot.sweep_price(hynix(0.25, NOW).book, "up", 0.25, 0.25) is None and m.Bot.sweep_price(hynix(0.26, NOW).book, "up", 0.26, 0.25) == (0.01, 1000.0, 3430.0)  # 1¢ is all that is left: behind the 1,000 there
    wbot = both5(SIM_MAKER_DEEP_CENTS="25", SIM_MAKER_DEEP_ONLY="on", SIM_MAKER_AFTER_HOURS_CENTS="10")
    wbot.predict.market_meta["101"] = (active, time.monotonic())
    r = wbot.sim_maker_reason(hynix(0.50, NOW), {}, NOW)[0]
    assert r is not None and (r.side, r.price, r.sweep, r.front, r.size) == ("涨", 0.02, True, True, 0.0) and abs(r.edge - 0.48) < 1e-9, r
    assert "击穿单" in wbot.sim_text() and "只挂低价挂单" in wbot.sim_text()
    wbot.sim_markets = lambda now: world["markets"]
    world["markets"] = [hynix(0.50, NOW)]
    assert [x["maker"] for x in wbot.sim_checks(NOW)] == ["挂涨 2.0¢×100 份（净优势 48.0¢，击穿单，前面 3430 份）"]
    await step(wbot, NOW, hynix(0.50, NOW))
    t = wbot.sim_trades()[tid]
    assert t["price"] == 0.02 and t["sweep"] is True and t["deep"] is True and t["queue_ahead"] == 0 and abs(t["edge"] - 0.48) < 1e-9, t
    await step(wbot, NOW + 10_000, hynix(0.50, NOW + 10_000, bids=(("0.48", "3430"),)))  # the 1¢ bid gone: down to 1¢
    trades = wbot.sim_trades()
    assert tid not in trades and trades[f"{tid}#1"]["note"] == "撤单：下面的买单变了，击穿单改挂 1.0¢，一份都没成交", trades[f"{tid}#1"]["note"]
    await step(wbot, NOW + 11_000, hynix(0.50, NOW + 11_000, bids=(("0.48", "3430"),)))
    assert wbot.sim_trades()[tid]["price"] == 0.01 and wbot.sim_trades()[tid]["sweep"] is True
    await step(wbot, NOW + 20_000, hynix(0.50, NOW + 20_000, bids=(("0.48", "3430"), ("0.10", "200"))))  # a 10¢ bid: a cent over it
    assert wbot.sim_trades()[f"{tid}#2"]["note"] == "撤单：下面的买单变了，击穿单改挂 11.0¢，一份都没成交"
    await step(wbot, NOW + 21_000, hynix(0.50, NOW + 21_000, bids=(("0.48", "3430"), ("0.10", "200"))))
    assert wbot.sim_trades()[tid]["price"] == 0.11
    await step(wbot, NOW + 30_000, hynix(0.50, NOW + 30_000, bids=(("0.20", "3430"), ("0.10", "200"))))  # the front at 21¢ clears 25¢: to the front
    assert wbot.sim_trades()[f"{tid}#3"]["note"] == "撤单：盘口离公平价远了，击穿单改挂到买1 之上 1¢（21.0¢），一份都没成交"
    await step(wbot, NOW + 31_000, hynix(0.50, NOW + 31_000, bids=(("0.20", "3430"), ("0.10", "200"))))
    t = wbot.sim_trades()[tid]
    assert t["price"] == 0.21 and t["deep"] is True and "sweep" not in t
    await step(wbot, NOW + 40_000, hynix(0.50, NOW + 40_000))  # the wall is back at 48¢ over it: the front order stays, 29¢ of edge behind the wall
    assert wbot.sim_trades()[tid]["price"] == 0.21 and wbot.sim_trades()[tid]["status"] == "resting"
    await step(wbot, NOW + 50_000, hynix(0.25, NOW + 50_000))  # the fair price down to 25¢: under the deep-bid line
    trades = wbot.sim_trades()
    assert tid not in trades and trades[f"{tid}#4"]["note"] == "撤单：净优势降到 4.0¢，低于撤单线 5.0¢，一份都没成交", trades[f"{tid}#4"]["note"]  # the ordinary exit line comes first
    await step(wbot, NOW + 60_000, hynix(0.50, NOW + 60_000))  # a sweep order again at 2¢; the fair price collapsing withdraws it
    assert wbot.sim_trades()[tid]["price"] == 0.02 and wbot.sim_trades()[tid]["sweep"] is True
    await step(wbot, NOW + 70_000, hynix(0.25, NOW + 70_000))
    trades = wbot.sim_trades()
    assert tid not in trades and trades[f"{tid}#5"]["note"] == "撤单：盘后、低价挂单只挂净优势 ≥ 25.0¢ 的，现 23.0¢ 低于盘后低价挂单撤单线 24.0¢，一份都没成交", trades[f"{tid}#5"]["note"]  # the line fires first
    r = wbot.sim_maker_reason(hynix(0.25, NOW + 70_000), {}, NOW + 70_000)[0]  # 涨 has no price with 25¢ left; 跌 (fair 75¢) at the front, 48¢
    assert r is not None and (r.side, r.price, r.sweep, r.front) == ("跌", 0.48, False, True) and abs(r.edge - 0.27) < 1e-9, r
    assert wbot.sim_maker_reason(hynix(0.25, NOW + 70_000), {f"{HSI_SLUG}|down|挂": {"status": "resting", "price": 0.48, "order": 100.0, "maker": True}}, NOW + 70_000) \
        == (None, "已有挂单中：48.0¢×100 份")  # 跌 held, 涨 nothing: the held one is the reason
    # both sides cheap enough is a pair: with 涨 resting, a later step rested 跌 too (1.55.0: the other side when the best is held)
    assert any(k.startswith(f"{HSI_SLUG}|down|挂") for k in wbot.sim_trades())
    sbot2 = both5(SIM_MAKER_SPREAD_CENTS="10")
    sbot2.predict.market_meta["101"] = (active, time.monotonic())
    assert sbot2.sim_maker_reason(five, {}, NOW) == (None, "买卖价差 90.0¢ 超过 10.0¢") and sbot2.sim_version()["sim_maker_spread"] == 0.10
    assert "挂单只挂在双边都有报价、价差不超过 10¢ 的盘口，排在已有挂单之后" in sbot2.sim_text()
    # a part fill, then the 买1 moves: the filled shares stay a position under the slot, the rest lapses, no fresh order
    await step(bot, NOW + 130_000, hsi(0.70, [("0.565", "150")], [("0.57", "40"), ("0.58", "400")], NOW + 130_000))  # a seller at 57¢: 40 presumed
    assert bot.sim_trades()[tid]["shares"] == 40 and bot.sim_trades()[tid]["status"] == "resting"
    await step(bot, NOW + 140_000, hsi(0.70, [("0.59", "200"), ("0.565", "150")], [("0.60", "400")], NOW + 140_000))
    t = bot.sim_trades()[tid]
    assert t["status"] == "filled" and t["shares"] == 40 and t["unfilled"] == 60 and "买1 移到 59.0¢，改跟" in t["note"] and f"{tid}#5" not in bot.sim_trades()
    await step(bot, NOW + 150_000, hsi(0.70, [("0.59", "200"), ("0.565", "150")], [("0.60", "400")], NOW + 150_000))
    trades = bot.sim_trades()  # the position no longer holds the slot (10-08): it moves to #5, a fresh order joins the new 买1
    assert trades[f"{tid}#5"]["shares"] == 40 and trades[f"{tid}#5"]["status"] == "filled" and m.sim_position(trades[f"{tid}#5"])
    assert trades[tid]["status"] == "resting" and trades[tid]["price"] == 0.59 and trades[tid]["queue_ahead"] == 200 and len([k for k in trades if k.startswith(tid)]) == 6
    world["markets"] = [hsi(0.70, [("0.59", "200"), ("0.565", "150")], [("0.60", "400")], NOW + 150_000)]
    assert bot.sim_maker_reason(world["markets"][0], trades, NOW + 150_000) == (None, "已有挂单中：59.0¢×100 份")  # the resting one does hold it
    # a full fill: sellers through 59¢ for the whole order; the next step rests another order behind the 买1, the position at #6
    await step(bot, NOW + 160_000, hsi(0.70, [("0.585", "200")], [("0.59", "120"), ("0.60", "400")], NOW + 160_000))  # 买1 within a cent: not re-quoted
    assert bot.sim_trades()[tid]["status"] == "filled" and bot.sim_trades()[tid]["shares"] == 100
    world["markets"] = [hsi(0.70, [("0.59", "200"), ("0.565", "150")], [("0.60", "400")], NOW + 170_000)]
    assert [r["maker"] for r in bot.sim_checks(NOW + 170_000)] == ["挂涨 59.0¢×100 份（净优势 11.0¢，排在 200 份之后）；已有持仓 100 份，再挂一笔"]
    await step(bot, NOW + 170_000)
    trades = bot.sim_trades()
    assert trades[f"{tid}#6"]["shares"] == 100 and trades[f"{tid}#6"]["status"] == "filled" and trades[tid]["status"] == "resting" and trades[tid]["price"] == 0.59
    assert len([k for k in trades if k.startswith(tid)]) == 7 and not m.sim_position(trades[tid]) and not m.sim_position(trades[f"{tid}#4"])
    # the exit line never sits above the bar: SIM_EDGE_CENTS=3 with the 5¢ default → 2.5¢
    low = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_EDGE_CENTS": "3"}), m.Store(":memory:"), FM(NOW), None)
    why = low.sim_requote_reason({"maker": True, "side": "up", "price": 0.69, "order": 100, "shares": 0}, hsi(0.70, [("0.69", "300")], [("0.72", "400")], NOW), NOW)
    assert abs(low.sim_maker_exit() - 0.025) < 1e-12 and why == "净优势降到 1.0¢，低于撤单线 2.5¢", why
    assert "只跟不少于 100 份的买1" in bot.sim_text() and "降到 5.0¢ 以下撤掉" in bot.sim_text() and "开盘前、收盘后撤掉" in bot.sim_text() and "只挂积分已激活的市场" in bot.sim_text()
    # SIM_MAKER_POINTS: nothing while the points status is unknown or inactive; placed when they pay; withdrawn when they stop
    pts = lambda at: m.SimMarket("pts", "pts", "close", "X", 0.70, book([("0.55", "300")], [("0.58", "400")], at, "pts", mid="202"), 0.03, "", ("涨", "跌"),
                                 {"key": "X", "close_ms": CLOSE})
    await step(bot, NOW + 160_000, pts(NOW + 160_000))
    checks = {r["item"]: r for r in bot.sim_checks(NOW + 160_000)}
    assert "pts|up|挂" not in bot.sim_trades() and checks["pts"]["maker_why"] == "积分状态暂缺，不挂" and checks["pts"]["points"] == "积分状态暂缺" and checks["pts"]["maker"] == ""
    bot.predict.market_meta["202"] = ({"rewards": {"current": None}, "trading_status": "OPEN"}, time.monotonic())
    await step(bot, NOW + 170_000, pts(NOW + 170_000))
    assert "pts|up|挂" not in bot.sim_trades() and {r["item"]: r for r in bot.sim_checks(NOW + 170_000)}["pts"]["maker_why"] == "积分未激活，不挂"
    bot.predict.market_meta["202"] = (active, time.monotonic())
    await step(bot, NOW + 180_000, pts(NOW + 180_000))
    assert bot.sim_trades()["pts|up|挂"]["price"] == 0.55
    row = {r["item"]: r for r in bot.sim_checks(NOW + 180_000)}["pts"]
    assert row["points"] == "积分已激活" and row["points_active"] is True and row["maker_why"].startswith("已有挂单中") and row["taker_why"] == "SIM_WAYS=maker：只挂单"
    assert row["session"] is None and row["bid"] == [0.55, 300.0] and row["ask"] == [0.58, 400.0] and row["url"].startswith(m.PREDICT_SITE) and row["kind"] == "指数/个股日涨跌"
    bot.predict.market_meta["202"] = ({"rewards": {"current": None}, "trading_status": "OPEN"}, time.monotonic())
    await step(bot, NOW + 190_000, pts(NOW + 190_000))
    assert "pts|up|挂" not in bot.sim_trades() and bot.sim_trades()["pts|up|挂#1"]["note"] == "撤单：积分已停止发放，不挂，一份都没成交"
    # a changed setting re-judges the resting orders: SIM_SHARES → re-placed with the new size; a higher bar → withdrawn when under it
    bot.predict.market_meta["202"] = (active, time.monotonic())
    await step(bot, NOW + 200_000, pts(NOW + 200_000))
    assert bot.sim_trades()["pts|up|挂"]["order"] == 100
    bot.apply_config(m.dataclasses.replace(bot.config, sim_shares=50.0))
    await step(bot, NOW + 210_000, pts(NOW + 210_000))
    assert bot.sim_trades()["pts|up|挂#2"]["note"] == "撤单：每次份数已改为 50 份，按新份数重挂，一份都没成交" and "pts|up|挂" not in bot.sim_trades()
    await step(bot, NOW + 220_000, pts(NOW + 220_000))
    assert bot.sim_trades()["pts|up|挂"]["order"] == 50
    bot.apply_config(m.dataclasses.replace(bot.config, sim_edge=0.20))
    await step(bot, NOW + 230_000, pts(NOW + 230_000))
    assert bot.sim_trades()["pts|up|挂#3"]["note"] == "撤单：触发门槛已改为 20.0¢，现净优势 15.0¢ 不达标，一份都没成交" and "pts|up|挂" not in bot.sim_trades()
    assert {r["item"]: r for r in bot.sim_checks(NOW + 230_000)}["pts"]["maker_why"] == "净优势 15.0¢ 低于触发门槛 20.0¢"
    # a book silent for over five minutes: the resting order is withdrawn (no pricing basis left), placed again on a fresh one
    bot.apply_config(m.dataclasses.replace(bot.config, sim_edge=0.10))
    await step(bot, NOW + 240_000, pts(NOW + 240_000))
    assert bot.sim_trades()["pts|up|挂"]["order"] == 50
    await step(bot, NOW + 250_000, pts(NOW + 250_000 - 6 * 60_000))  # the book is six minutes old
    assert bot.sim_trades()["pts|up|挂#4"]["note"] == "撤单：盘口断流超过 5 分钟，撤单，一份都没成交" and "pts|up|挂" not in bot.sim_trades()
    await step(bot, NOW + 260_000, pts(NOW + 260_000 - 2 * 60_000))  # stale, but not for that long: nothing new, nothing withdrawn
    assert "pts|up|挂" not in bot.sim_trades()
    await step(bot, NOW + 270_000, pts(NOW + 270_000))
    assert bot.sim_trades()["pts|up|挂"]["price"] == 0.55
    print("MAKER_RULES_OK")


async def follow_guard():
    """An ordinary resting order follows the valid 买1 only to a level a fresh order would be placed at: when the 买1
    moves up to where the edge no longer clears the bar, the order stays (it still has its edge) instead of being
    withdrawn for nothing."""
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                             "WEB_PORT": "8080", "SIM_WAYS": "maker", "SIM_MARKETS": "all", "SIM_MAKER_POINTS": "off"})
    bot = m.Bot(cfg, m.Store(":memory:"), FM(NOW), None)
    bot.sim_markets = lambda now: world["markets"]
    tid = f"{HSI_SLUG}|up|挂"
    await step(bot, NOW, hsi(0.66, [("0.55", "300")], [("0.60", "400")], NOW))  # 买1 55¢: 11¢ ≥ the 10¢ bar
    assert bot.sim_trades()[tid]["price"] == 0.55
    # the 买1 moves up to 57¢ (9¢ there, under the bar): the 55¢ order stays
    mk = hsi(0.66, [("0.57", "200"), ("0.55", "300")], [("0.60", "400")], NOW + 5_000)
    assert bot.sim_requote_reason(bot.sim_trades()[tid], mk, NOW + 5_000) == ""
    await step(bot, NOW + 5_000, mk)
    assert list(bot.sim_trades()) == [tid] and bot.sim_trades()[tid]["status"] == "resting" and bot.sim_trades()[tid]["price"] == 0.55
    # ...and to 56¢ (10¢ there, at the bar): followed, re-placed there next step
    await step(bot, NOW + 10_000, hsi(0.66, [("0.56", "200"), ("0.55", "300")], [("0.60", "400")], NOW + 10_000))
    assert bot.sim_trades()[f"{tid}#1"]["note"] == "撤单：买1 移到 56.0¢，改跟，一份都没成交"
    await step(bot, NOW + 15_000, hsi(0.66, [("0.56", "200"), ("0.55", "300")], [("0.60", "400")], NOW + 15_000))
    assert bot.sim_trades()[tid]["price"] == 0.56
    # the 买1 moving down is always followed (more edge at the new level)
    await step(bot, NOW + 20_000, hsi(0.66, [("0.54", "200")], [("0.60", "400")], NOW + 20_000))
    assert bot.sim_trades()[f"{tid}#2"]["note"] == "撤单：买1 移到 54.0¢，改跟，一份都没成交"
    # after hours with the price limit: a 买1 above the limit is not followed either
    abot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
                                    "SIM_WAYS": "maker", "SIM_MARKETS": "all", "SIM_MAKER_POINTS": "off", "SIM_MAKER_AFTER_HOURS_CENTS": "10",
                                    "SIM_AFTER_HOURS_MAX_PRICE_CENTS": "60"}), m.Store(":memory:"), FM(NOW), None)
    abot.sim_markets = lambda now: world["markets"]
    closed = lambda fair, bids, at: m.dataclasses.replace(hsi(fair, bids, [("0.70", "400")], at), in_session=False)
    await step(abot, NOW, closed(0.80, [("0.59", "300")], NOW))  # 59¢ ≤ 60¢, 21¢ of edge
    assert abot.sim_trades()[tid]["price"] == 0.59
    await step(abot, NOW + 5_000, closed(0.80, [("0.61", "300"), ("0.59", "300")], NOW + 5_000))  # 61¢ is over the limit: stays at 59¢
    assert list(abot.sim_trades()) == [tid] and abot.sim_trades()[tid]["price"] == 0.59
    print("FOLLOW_GUARD_OK")


async def review_fixes():
    """The 10-08 review: a sweep through a resting order is credited before the withdrawal is judged; a withdrawal for a
    setting frees the slot; the deep-bid rules under SIM_MAKER_MIN_BID=0, a bar above SIM_MAKER_DEEP_CENTS, a wide book
    under deep-only, and a front the ask forbids."""
    env = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
           "SIM_WAYS": "maker", "SIM_MARKETS": "all", "SIM_MAKER_POINTS": "off"}
    mk_bot = lambda **extra: m.Bot(m.Config.from_env({**env, **extra}), m.Store(":memory:"), FM(NOW), None)
    tid = f"{HSI_SLUG}|up|挂"
    # (1) a dump through the order: 900 shares offered at 53¢ fill the 55¢ order (300 queued ahead of it) — not "买1 移到 50¢，改跟"
    bot = mk_bot(); bot.sim_markets = lambda now: world["markets"]
    await step(bot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    assert bot.sim_trades()[tid]["price"] == 0.55 and bot.sim_trades()[tid]["queue_ahead"] == 300
    await step(bot, NOW + 5_000, hsi(0.70, [("0.50", "300")], [("0.53", "900")], NOW + 5_000))
    t = bot.sim_trades()[tid]
    assert t["status"] == "filled" and t["shares"] == 100 and f"{tid}#1" not in bot.sim_trades(), (t["status"], t["shares"], t.get("note"))
    # (2) withdrawn for a setting (SIM_MARKETS narrowed): the record moves to #n, the slot is free once the setting is back
    bot2 = mk_bot(); bot2.sim_markets = lambda now: world["markets"]
    await step(bot2, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    bot2.apply_config(m.Config.from_env({**env, "SIM_MARKETS": "touch"}))
    await step(bot2, NOW + 5_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 5_000))
    assert sorted(bot2.sim_trades()) == [f"{tid}#1"] and bot2.sim_trades()[f"{tid}#1"]["note"] == "撤单：模拟交易范围已不含指数/个股日涨跌，一份都没成交", bot2.sim_trades()
    bot2.apply_config(m.Config.from_env(env))
    await step(bot2, NOW + 10_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 10_000))
    assert bot2.sim_trades()[tid]["status"] == "resting" and bot2.sim_trades()[tid]["price"] == 0.55  # placed again, not "已有已撤单"
    # (3) SIM_MAKER_MIN_BID=0: a deep-bid order pushed off the front still goes back over the pusher
    dbot = mk_bot(SIM_MAKER_MIN_BID="0", SIM_MAKER_DEEP_CENTS="25", SIM_MAKER_SPREAD_CENTS="10"); dbot.sim_markets = lambda now: world["markets"]
    wide = lambda bids, at: hsi(0.38, bids, [("0.40", "100"), ("0.98", "500")], at)
    await step(dbot, NOW, wide([("0.05", "300"), ("0.01", "1000")], NOW))
    assert dbot.sim_trades()[tid]["price"] == 0.06 and dbot.sim_trades()[tid]["deep"] is True
    assert dbot.sim_requote_reason(dbot.sim_trades()[tid], wide([("0.06", "20"), ("0.05", "300"), ("0.01", "1000")], NOW + 5_000), NOW + 5_000) == "买1 被顶到 6.0¢，改挂 7.0¢"
    # (4) deep-only with the after-hours bar above SIM_MAKER_DEEP_CENTS: the sweep order is priced off the bar in force
    abot = mk_bot(SIM_MAKER_DEEP_CENTS="25", SIM_MAKER_DEEP_ONLY="on", SIM_MAKER_AFTER_HOURS_CENTS="30"); abot.sim_markets = lambda now: world["markets"]
    closed = lambda bids, at: m.dataclasses.replace(hsi(0.50, bids, [("0.53", "1720"), ("0.99", "1000")], at), in_session=False)
    await step(abot, NOW, closed([("0.48", "3430"), ("0.22", "50")], NOW))
    t = abot.sim_trades()[tid]
    assert t["price"] == 0.01 and t["sweep"] is True and abs(t["edge"] - 0.49) < 1e-9, t  # fair − 30¢ = 20¢: the 22¢ bid is above it, nothing under → 1¢ (not 23¢, refused by the 30¢ bar)
    assert abot.sim_requote_reason(t, closed([("0.48", "3430"), ("0.22", "50")], NOW + 5_000), NOW + 5_000) == ""
    # (5) deep-only on a wide book (SIM_MAKER_SPREAD_CENTS > 0): a sweep order when the front has no edge, not nothing
    sbot = mk_bot(SIM_MAKER_DEEP_CENTS="25", SIM_MAKER_DEEP_ONLY="on", SIM_MAKER_SPREAD_CENTS="10"); sbot.sim_markets = lambda now: world["markets"]
    await step(sbot, NOW, hsi(0.50, [("0.30", "300")], [("0.60", "200")], NOW))
    t = sbot.sim_trades()[tid]
    assert t["price"] == 0.01 and t["sweep"] is True, t  # the front (31¢) has 19¢ < 25¢; the sweep goes under fair − 25¢
    # (6) a front the ask forbids joins the level as an ordinary order: no deep flag, the ordinary bar and rules
    fbot = mk_bot(SIM_MAKER_DEEP_CENTS="25"); fbot.sim_markets = lambda now: world["markets"]
    await step(fbot, NOW, hsi(0.90, [("0.57", "150")], [("0.58", "400")], NOW))
    t = fbot.sim_trades()[tid]
    assert t["price"] == 0.57 and "deep" not in t and t["queue_ahead"] == 150, t
    # (7) the follow guard on a book the deep-bid mode would quote on: the replacement would need the deep-bid bar
    gbot = mk_bot(SIM_MAKER_DEEP_CENTS="25", SIM_MAKER_SPREAD_CENTS="10"); gbot.sim_markets = lambda now: world["markets"]
    await step(gbot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    assert gbot.sim_trades()[tid]["price"] == 0.55 and "deep" not in gbot.sim_trades()[tid]
    pulled = hsi(0.70, [("0.57", "200"), ("0.55", "300")], [("0.90", "400")], NOW + 5_000)  # the ask pulled to 90¢: a wide book, where a fresh order needs 25¢
    assert gbot.sim_requote_reason(gbot.sim_trades()[tid], pulled, NOW + 5_000) == ""
    await step(gbot, NOW + 5_000, pulled)
    assert gbot.sim_trades()[tid]["status"] == "resting" and gbot.sim_trades()[tid]["price"] == 0.55
    # (8) deep-only on the chosen markets only (SIM_MAKER_DEEP_ONLY_MARKETS): elsewhere both kinds of order are placed
    obot = mk_bot(SIM_MAKER_DEEP_CENTS="25", SIM_MAKER_DEEP_ONLY="on", SIM_MAKER_DEEP_ONLY_MARKETS="kospi"); obot.sim_markets = lambda now: world["markets"]
    assert obot.config.sim_maker_deep_only_markets == {"KOSPI"} and obot.sim_version()["sim_maker_deep_only_markets"] == ["KOSPI"]
    await step(obot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    t = obot.sim_trades()[tid]
    assert t["price"] == 0.55 and "deep" not in t and "sweep" not in t, t  # HSI is not on the list: the ordinary order joins the 买1
    assert obot.sim_requote_reason(t, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 5_000), NOW + 5_000) == ""
    print("REVIEW_FIXES_OK")


async def taker_shares():
    """Takers get their own share count: SIM_TAKER_SHARES in session, SIM_TAKER_AFTER_HOURS_SHARES after hours; makers keep SIM_SHARES."""
    env = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
           "SIM_WAYS": "both", "SIM_MARKETS": "all", "SIM_MAKER_POINTS": "off", "SIM_MAKER_SESSION": "off", "SIM_TAKER_SWEEP_SHARES": "0",
           "SIM_TAKER_SHARES": "20", "SIM_TAKER_AFTER_HOURS_SHARES": "10", "SIM_TAKER_AFTER_HOURS_CENTS": "10"}
    bot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(NOW), None)
    bot.sim_markets = lambda now: world["markets"]
    assert bot.sim_version()["sim_taker_shares"] == 20 and bot.sim_version()["sim_taker_after_hours_shares"] == 10 and bot.sim_version()["sim_shares"] == 100
    live = m.dataclasses.replace(hsi(0.75, [("0.55", "300")], [("0.58", "400")], NOW), in_session=True)
    assert bot.sim_taker_shares(live) == 20
    await step(bot, NOW, live)
    t = bot.sim_trades()
    assert t[f"{HSI_SLUG}|up|吃"]["order"] == 20 and t[f"{HSI_SLUG}|up|吃"]["shares"] == 20 and t[f"{HSI_SLUG}|up|挂"]["order"] == 100, t  # the maker keeps SIM_SHARES
    bot2 = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(NOW), None)
    bot2.sim_markets = lambda now: world["markets"]
    closed = m.dataclasses.replace(hsi(0.75, [("0.55", "300")], [("0.58", "400")], NOW), in_session=False)
    assert bot2.sim_taker_shares(closed) == 10
    await step(bot2, NOW, closed)
    assert bot2.sim_trades()[f"{HSI_SLUG}|up|吃"]["order"] == 10 and bot2.sim_trades()[f"{HSI_SLUG}|up|吃"]["shares"] == 10
    bot3 = m.Bot(m.Config.from_env({**env, "SIM_TAKER_SHARES": "0", "SIM_TAKER_AFTER_HOURS_SHARES": "0"}), m.Store(":memory:"), FM(NOW), None)
    assert bot3.sim_taker_shares(live) == 100 and bot3.sim_taker_shares(closed) == 100 and "sim_taker_shares" not in bot3.sim_version()
    # a side is taken again while the edge lasts, until SIM_TAKER_MAX_SHARES (the last buy takes the remainder); then no more
    cap_env = {**env, "SIM_TAKER_SHARES": "0", "SIM_TAKER_AFTER_HOURS_SHARES": "0", "SIM_TAKER_MAX_SHARES": "250", "SIM_WAYS": "taker", "SIM_GROUP_USD": "0"}
    cbot = m.Bot(m.Config.from_env(cap_env), m.Store(":memory:"), FM(NOW), None)
    cbot.sim_markets = lambda now: world["markets"]
    deep = lambda at: m.dataclasses.replace(hsi(0.75, [("0.55", "300")], [("0.58", "2000")], at), in_session=True)
    tid = f"{HSI_SLUG}|up|吃"
    await step(cbot, NOW, deep(NOW))
    assert cbot.sim_trades()[tid]["shares"] == 100 and cbot.sim_taker_room(cbot.sim_trades(), deep(NOW), "up") == (100.0, 100.0)
    await step(cbot, NOW + 5_000, deep(NOW + 5_000))
    t = cbot.sim_trades()
    assert t[tid]["shares"] == 100 and t[f"{tid}#1"]["shares"] == 100 and t[f"{tid}#1"]["status"] == "filled"  # the first position moved aside, a second buy
    await step(cbot, NOW + 10_000, deep(NOW + 10_000))
    t = cbot.sim_trades()
    assert t[tid]["shares"] == 50 and t[tid]["order"] == 50 and sorted(k for k in t if k.startswith(tid)) == [tid, f"{tid}#1", f"{tid}#2"]  # the remainder only
    assert cbot.sim_taker_room(t, deep(NOW + 10_000), "up") == (0.0, 250.0)
    await step(cbot, NOW + 15_000, deep(NOW + 15_000))
    assert len([k for k in cbot.sim_trades() if k.startswith(tid)]) == 3  # at the cap: nothing more
    row = next(r for r in cbot.sim_checks(NOW + 15_000) if r["market"] == HSI_SLUG)
    assert "这一边持仓 250 份，达到上限 250 份" in row["taker"], row["taker"]  # 1.91.0: SIM_SHARES_NET (default on) counts the position
    # 1.91.0: SIM_SHARES_NET — the cap is on the side's net position: every fill here (makers' too) less the other side's fills
    # (10-25: CXMT held 400 涨 against 400 跌, flat; "每边 500" would have stopped each side at 100 more)
    nbot = m.Bot(m.Config.from_env({**cap_env, "SIM_TAKER_MAX_SHARES": "500"}), m.Store(":memory:"), FM(NOW), None)
    pbot = m.Bot(m.Config.from_env({**cap_env, "SIM_TAKER_MAX_SHARES": "500", "SIM_SHARES_NET": "off"}), m.Store(":memory:"), FM(NOW), None)
    assert nbot.config.sim_shares_net and not pbot.config.sim_shares_net and nbot.sim_version().get("sim_shares_net") is True and "sim_shares_net" not in pbot.sim_version()
    mkt = deep(NOW).market
    rec = lambda side, shares, maker=False, status="filled": {"market": mkt, "side": side, "status": status, "shares": shares, "order": shares, "maker": maker}
    both = {f"{mkt}|up|吃": rec("up", 450), f"{mkt}|down|挂": rec("down", 400, maker=True)}
    assert nbot.sim_taker_room(both, deep(NOW), "up") == (100.0, 450.0) and pbot.sim_taker_room(both, deep(NOW), "up") == (50.0, 450.0), "net 50 of 500 / 450 of 500"
    assert nbot.sim_taker_room(both, deep(NOW), "down") == (100.0, 0.0) and pbot.sim_taker_room(both, deep(NOW), "down") == (100.0, 0.0)
    flat = {f"{mkt}|up|吃": rec("up", 900), f"{mkt}|down|吃": rec("down", 400)}
    assert nbot.sim_taker_room(flat, deep(NOW), "up") == (0.0, 900.0) and nbot.sim_taker_room(flat, deep(NOW), "down") == (100.0, 400.0), "net 500: up at the cap, down 500 under it"
    assert nbot.sim_taker_full_text(flat, deep(NOW), "up", 900.0) == "这一边持仓 900 份减另一边 400 份、净 500 份，达到上限 500 份"
    assert pbot.sim_taker_full_text(flat, deep(NOW), "up", 900.0) == "这一边吃单已累计 900 份，达到上限 500 份"
    assert nbot.sim_taker_room({f"{mkt}|up|吃": rec("up", 480, maker=True)}, deep(NOW), "up") == (20.0, 0.0), "makers' fills count under the net cap"
    assert pbot.sim_taker_room({f"{mkt}|up|吃": rec("up", 480, maker=True)}, deep(NOW), "up") == (100.0, 0.0), "...not under the per-side taker count"
    obot = m.Bot(m.Config.from_env({**cap_env, "SIM_TAKER_MAX_SHARES": "0"}), m.Store(":memory:"), FM(NOW), None)
    obot.sim_markets = lambda now: world["markets"]
    await step(obot, NOW, deep(NOW)); await step(obot, NOW + 5_000, deep(NOW + 5_000))
    assert [k for k in obot.sim_trades() if k.startswith(tid)] == [tid]  # 0 = once only
    row = next(r for r in obot.sim_checks(NOW + 5_000) if r["market"] == HSI_SLUG)
    assert "每边只吃一次" in row["taker"], row["taker"]
    # 1.68.0: SIM_TAKER_SWEEP_SHARES — the asks under the cap are taken in one order, however many levels (a fair price
    # of 50¢: the 20¢ and the 30¢ asks both clear the bar, both go), up to the ceiling and never under the per-buy count
    two = lambda at, **kw: m.dataclasses.replace(hsi(0.50, [("0.10", "100")], [("0.20", "100"), ("0.30", "100"), ("0.45", "500")], at, **kw), in_session=True)
    sweep_env = {**cap_env, "SIM_TAKER_MAX_SHARES": "0", "SIM_TAKER_SWEEP_SHARES": "300"}
    sbot = m.Bot(m.Config.from_env(sweep_env), m.Store(":memory:"), FM(NOW), None)
    sbot.sim_markets = lambda now: world["markets"]
    mk = two(NOW)
    assert sbot.sim_taker_cap(mk, "up", 200) == 0.39 and sbot.sim_taker_want(mk, "up", 0.39) == 200 and sbot.sim_taker_want(mk, "down", 0.39) == 100
    assert sbot.sim_taker_want(two(NOW, hold="x"), "up", 0) == 100 and sbot.sim_version()["sim_taker_sweep"] == 300
    gap = sbot.sim_taker_gap(mk, sbot.sim_trades(), NOW)
    assert gap["state"] == "ready" and "可买 200 份" in gap["text"], gap
    await step(sbot, NOW, mk)
    t = sbot.sim_trades()[tid]
    assert t["order"] == 200 and t["shares"] == 200 and t["fills"][0]["levels"] == [[0.2, 100.0], [0.3, 100.0]] and not t["fills"][0]["short"], t
    assert abs(t["avg"] - 0.25) < 1e-9 and t["cap"] == 0.39 and t["version"]["sim_taker_sweep"] == 300
    wbot = m.Bot(m.Config.from_env({**sweep_env, "SIM_TAKER_SWEEP_SHARES": "150"}), m.Store(":memory:"), FM(NOW), None)  # the ceiling
    wbot.sim_markets = lambda now: world["markets"]
    await step(wbot, NOW, two(NOW))
    assert wbot.sim_trades()[tid]["fills"][0]["levels"] == [[0.2, 100.0], [0.3, 50.0]]
    mbot = m.Bot(m.Config.from_env({**sweep_env, "SIM_TAKER_MAX_SHARES": "120"}), m.Store(":memory:"), FM(NOW), None)  # under the per-side total
    mbot.sim_markets = lambda now: world["markets"]
    await step(mbot, NOW, two(NOW))
    assert mbot.sim_trades()[tid]["fills"][0]["levels"] == [[0.2, 100.0], [0.3, 20.0]]
    thin = lambda at: m.dataclasses.replace(hsi(0.50, [("0.10", "100")], [("0.20", "50"), ("0.45", "500")], at), in_session=True)
    tbot = m.Bot(m.Config.from_env(sweep_env), m.Store(":memory:"), FM(NOW), None)  # a thin book: the per-buy count asked, as before
    tbot.sim_markets = lambda now: world["markets"]
    assert tbot.sim_taker_want(thin(NOW), "up", 0.39) == 100
    await step(tbot, NOW, thin(NOW))
    t = tbot.sim_trades()[tid]
    assert t["order"] == 100 and t["shares"] == 50 and t["fills"][0]["short"]
    # the group cap does not take the sweep ($50 at risk): the most it does goes — 131 shares (100 at 20¢ and 31 at 30¢:
    # $29.9 at risk under $30), neither nothing nor just the per-buy count (1.69.0: a cap bounds the order)
    gbot = m.Bot(m.Config.from_env({**sweep_env, "SIM_GROUP_USD": "30"}), m.Store(":memory:"), FM(NOW), None)
    gbot.sim_markets = lambda now: world["markets"]
    gap = gbot.sim_taker_gap(two(NOW), gbot.sim_trades(), NOW)
    assert gap["state"] == "ready" and "可买 131 份" in gap["text"], gap
    world["markets"] = [two(NOW)]
    row = next(r for r in gbot.sim_checks(NOW) if r["market"] == HSI_SLUG)
    assert "涨：封顶 39.0¢，可买 131 份" in row["taker"], row["taker"]
    await step(gbot, NOW, two(NOW))
    t = gbot.sim_trades()[tid]
    assert t["order"] == 131 and t["shares"] == 131 and t["fills"][0]["levels"] == [[0.2, 100.0], [0.3, 31.0]], t
    assert not gbot.sim_blocks(), "a smaller buy, not a refusal"
    assert gbot.sim_group_room(gbot.sim_trades(), two(NOW), "up", 0.204, 1).startswith("恒生指数"), "one more share would not fit"
    sbot2 = m.Bot(m.Config.from_env({**sweep_env, "SIM_GROUP_USD": "10"}), m.Store(":memory:"), FM(NOW), None)  # $10: 49 shares at 20¢ (+ fee)
    sbot2.sim_markets = lambda now: world["markets"]
    await step(sbot2, NOW, two(NOW))
    assert sbot2.sim_trades()[tid]["order"] == 49 and sbot2.sim_trades()[tid]["fills"][0]["levels"] == [[0.2, 49.0]]
    nbot = m.Bot(m.Config.from_env({**sweep_env, "SIM_GROUP_USD": "0.1"}), m.Store(":memory:"), FM(NOW), None)  # not even a share fits: refused, noted
    nbot.sim_markets = lambda now: world["markets"]
    gap = nbot.sim_taker_gap(two(NOW), nbot.sim_trades(), NOW)
    assert gap["state"] == "blocked" and "超过组上限 $0.1" in gap["text"], gap
    await step(nbot, NOW, two(NOW))
    assert tid not in nbot.sim_trades() and nbot.sim_blocks() and "超过组上限 $0.1" in nbot.sim_blocks()[0]["why"]
    assert m.Bot.sim_size_down(0, lambda n: True) is None and m.Bot.sim_size_down(7, lambda n: True) == 7 and m.Bot.sim_size_down(300, lambda n: n <= 129) == 129
    assert m.Bot.sim_size_down(300, lambda n: n <= 1) == 1 and m.Bot.sim_size_down(300, lambda n: False) is None and m.Bot.sim_size_down(10, lambda n: n <= 8) == 8
    # a resting order the group cap does not take at SIM_SHARES is placed for the most it does: 100 at 48¢ is $48, 62 are $29.8
    kbot = m.Bot(m.Config.from_env({**sweep_env, "SIM_WAYS": "maker", "SIM_GROUP_USD": "30"}), m.Store(":memory:"), FM(NOW), None)
    kbot.sim_markets = lambda now: world["markets"]
    wide = lambda at: m.dataclasses.replace(hsi(0.70, [("0.48", "300")], [("0.90", "100")], at), in_session=True)
    world["markets"] = [wide(NOW)]
    row = next(r for r in kbot.sim_checks(NOW) if r["market"] == HSI_SLUG)
    assert row["maker"].startswith("挂涨 48.0¢×62 份") and not row["maker_why"], row
    await step(kbot, NOW, wide(NOW))
    t = kbot.sim_trades()[f"{HSI_SLUG}|up|挂"]
    assert t["status"] == "resting" and t["order"] == 62 and t["price"] == 0.48 and not kbot.sim_blocks(), t
    obot2 = m.Bot(m.Config.from_env({**sweep_env, "SIM_TAKER_SWEEP_SHARES": "0"}), m.Store(":memory:"), FM(NOW), None)  # off: the 20¢ ones only
    obot2.sim_markets = lambda now: world["markets"]
    await step(obot2, NOW, two(NOW))
    assert obot2.sim_trades()[tid]["fills"][0]["levels"] == [[0.2, 100.0]]
    print("TAKER_SHARES_OK")


async def taker_gap():
    """距吃单 (1.60.0): how far each market is from a taker buy, by the trader's own bar, cap, size and fee — the nearer
    side's ask against its cap, buyable now, held for the moment, held by an order or the per-side total, waiting on
    the card, or not taken at all; on the page's cards, the paper-trading card's nearest list and /sim."""
    gbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                                    "SIM_WAYS": "both", "SIM_MARKETS": "all", **LEGACY}), m.Store(":memory:"), FM(NOW), None)
    gbot.sim_markets = lambda now: world["markets"]
    bps, cap_up, cap_down = 200, m.taker_cap(0.70, 0.10, 200), m.taker_cap(0.30, 0.10, 200)
    assert cap_up == 0.59 and cap_down == 0.19
    world["markets"] = [hsi(0.70, [(0.55, 100)], [(0.62, 100)], NOW)]
    g = gbot.sim_taker_gap(world["markets"][0], {}, NOW)
    assert g["state"] == "near" and abs(g["gap"] - 0.03) < 1e-9 and g["label"] == "吃涨" and g["bar"] == 0.10 and not g["after"] and g["limit"] is None, g
    assert g["text"] == "距吃单 3.0¢：吃涨 卖1 62.0¢，封顶 59.0¢" and g["short"] == "吃涨 差 3.0¢", g
    assert g["detail"] == "吃涨：卖1 62.0¢，封顶 59.0¢，差 3.0¢；吃跌：卖1 45.0¢，封顶 19.0¢，差 26.0¢", g["detail"]
    up, down = g["sides"]
    assert (up["ask"], up["cap"], up["block"], up["got"]) == (0.62, 0.59, "", 0.0) and (down["ask"], down["cap"], down["block"]) == (0.45, 0.19, "")
    gaps = gbot.sim_taker_gaps(NOW)
    assert list(gaps) == ["101"] and gaps["101"]["market"] == HSI_SLUG and gaps["101"]["item"] == "恒生指数"
    assert [(n["item"], n["state"], n["short"]) for n in gbot.sim_near(gaps)] == [("恒生指数", "near", "吃涨 差 3.0¢")]
    card = next(i for i in gbot.odds_payload()["items"] if i["name"] == "模拟交易")
    assert card["near"][0]["short"] == "吃涨 差 3.0¢" and card["near"][0]["text"] == g["text"]
    assert "📏 距吃单最近：恒生指数 吃涨 差 3.0¢" in gbot.cmd_sim(None).text
    # the ask falls to the cap: buyable now; bought (every side once under LEGACY), the other side is the nearer one
    world["markets"] = [hsi(0.70, [(0.55, 100)], [(0.58, 100)], NOW)]
    g = gbot.sim_taker_gap(world["markets"][0], {}, NOW)
    assert g["state"] == "ready" and g["text"] == "可吃单：吃涨 封顶 59.0¢，可买 100 份" and g["short"] == "吃涨 可买 100 份" and abs(g["gap"] + 0.01) < 1e-9, g
    assert gbot.sim_near(gbot.sim_taker_gaps(NOW))[0]["state"] == "ready"
    await step(gbot, NOW)
    assert f"{HSI_SLUG}|up|吃" in gbot.sim_trades()
    g = gbot.sim_taker_gap(world["markets"][0], gbot.sim_trades(), NOW + 5_000)
    assert g["state"] == "near" and g["label"] == "吃跌" and g["sides"][0]["block"] == "已有持仓（每边只吃一次）" and g["text"] == "距吃单 26.0¢：吃跌 卖1 45.0¢，封顶 19.0¢", g
    # a rule holds the buy for the moment: the market's end; the card's hold; no asks on one side, on both; a fair price too low for a cap
    g = gbot.sim_taker_gap(hsi(0.70, [(0.55, 100)], [(0.58, 100)], CLOSE), {}, CLOSE)
    assert g["state"] == "blocked" and g["text"] == "吃涨可买 100 份，但已到结束时间，等结果出来：不开新单，挂单撤掉" and g["short"] == "吃涨 可买，但已到结束时间，等结果出来：不开新单，挂单撤掉", g
    g = gbot.sim_taker_gap(hsi(0.70, [(0.55, 100)], [(0.58, 100)], NOW, hold="行情过期"), {}, NOW)
    assert g["state"] == "wait" and g["text"] == "不吃单：卡片暂不建议（行情过期）" and g["sides"][0]["got"] == 100  # the figures are still there
    g = gbot.sim_taker_gap(hsi(0.70, [(0.55, 100)], [], NOW), {}, NOW)
    assert g["state"] == "near" and g["label"] == "吃跌" and g["sides"][0]["block"] == "没有卖单" and g["sides"][0]["gap"] is None
    g = gbot.sim_taker_gap(hsi(0.70, [], [], NOW), {}, NOW)
    assert g["state"] == "held" and g["text"] == "吃涨：没有卖单；吃跌：没有卖单" and g["short"] == "没有卖单"
    g = gbot.sim_taker_gap(hsi(0.08, [(0.55, 100)], [(0.58, 100)], NOW), {}, NOW)
    assert g["state"] == "ready" and g["label"] == "吃跌" and g["sides"][0]["block"] == "公平价 8.0¢ 留不出 10.0¢" and g["sides"][1]["cap"] == m.taker_cap(0.92, 0.10, 200), g
    assert gbot.sim_near({"a": g, "b": gbot.sim_taker_gap(hsi(0.70, [(0.55, 100)], [(0.62, 100)], NOW), {}, NOW)})[0]["state"] == "ready"  # buyable first
    # not taken at all: SIM_WAYS=maker, a kind outside SIM_MARKETS, a skipped key
    mbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                                    "SIM_WAYS": "maker", "SIM_MARKETS": "touch", "SIM_SKIP": "hsi", **LEGACY}), m.Store(":memory:"), FM(NOW), None)
    g = mbot.sim_taker_gap(hsi(0.70, [(0.55, 100)], [(0.58, 100)], NOW), {}, NOW)
    assert g["state"] == "off" and g["text"] == "不在模拟交易范围，不吃单" and g["sides"] == [] and mbot.sim_near({"x": g}) == []
    mbot.config = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "SIM_WAYS": "maker", "SIM_MARKETS": "all", **LEGACY})
    assert mbot.sim_taker_gap(hsi(0.70, [(0.55, 100)], [(0.58, 100)], NOW), {}, NOW)["text"] == "SIM_WAYS=maker：只挂单，不吃单"
    # after hours: the after-hours bar and price limit set the cap; the line says so
    hbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                                    "SIM_WAYS": "both", "SIM_MARKETS": "all", "SIM_TAKER_AFTER_HOURS_CENTS": "15", "SIM_AFTER_HOURS_MAX_PRICE_CENTS": "60", **LEGACY}),
                 m.Store(":memory:"), FM(NOW), None)
    g = hbot.sim_taker_gap(m.dataclasses.replace(hsi(0.80, [(0.55, 100)], [(0.62, 100)], NOW), in_session=False), {}, NOW)
    assert g["after"] and g["bar"] == 0.15 and g["limit"] == 0.60 and g["state"] == "near" and g["text"] == "距吃单 2.0¢：吃涨 卖1 62.0¢，封顶 60.0¢", g
    print("TAKER_GAP_OK")


async def faster():
    """1.61.0: a round of books wakes the trader at once (sim_wake, never two looks within a second); the orderbook feed
    refreshes the hot keys every round and the ladders only when something is near them (predict_hot_keys)."""
    fbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                                    "SIM_WAYS": "both", "SIM_MARKETS": "all", **LEGACY}), m.Store(":memory:"), FM(NOW), None)
    fbot.sim_markets = lambda now: world["markets"]
    fbot.sim_ran = m.time.monotonic()
    assert not fbot.sim_step_due()                      # the timer has not run out
    fbot.sim_wake = True
    assert not fbot.sim_step_due()                      # books just came, but the last look was under a second ago
    fbot.sim_ran -= 1.5
    assert fbot.sim_step_due()                          # ...now it looks
    fbot.sim_wake = False
    assert not fbot.sim_step_due() and fbot.sim_step_due() is False
    fbot.sim_ran -= 10
    assert fbot.sim_step_due()                          # the timer alone
    world["markets"] = [hsi(0.70, [(0.55, 100)], [(0.62, 100)], NOW)]
    assert await fbot.sim_step(NOW) is not False and fbot.sim_wake is False and m.time.monotonic() - fbot.sim_ran < 1
    # the feed's round: hot keys in, the trader woken
    seen = {}
    async def fake_refresh(targets, force=False, hot=None, hot_rows=None):
        seen["targets"], seen["hot"], seen["rows"] = targets, hot, hot_rows
        return m.Refreshed("ok")
    fbot.predict.refresh = fake_refresh
    fbot.predict.slugs = {"HSI": HSI_SLUG, "PONS": "pons", "NIULAI": "niulai"}
    fbot.predict.ladder_keys = {"PONS", "NIULAI"}
    pons = lambda ask: m.SimMarket("pons#77", "$PONS FDV $10M", "ladder", "PONS", 0.30, book([(0.20, 100)], [(ask, 100)], NOW, slug="pons", key="PONS", mid="77"),
                                   0.03, "", ("Yes", "No"), {"target": "10000000", "end": NOW + 86_400_000}, {})
    niu = m.SimMarket("niulai#5", "$牛来 市值 $1B", "ladder", "NIULAI", 0.10, book([(0.02, 100)], [(0.60, 100)], NOW, slug="niulai", key="NIULAI", mid="5"),
                      0.03, "", ("Yes", "No"), {"target": "1000000000", "end": NOW + 86_400_000}, {})
    world["markets"] = [hsi(0.70, [(0.55, 100)], [(0.62, 100)], NOW), pons(0.50), niu]  # PONS: cap 19¢, ask 50¢: far; 牛来: far
    assert fbot.predict_hot_keys(NOW) == {"HSI"}
    world["markets"][1] = pons(0.23)                    # within PREDICT_HOT_GAP of the cap: refreshed every round
    assert fbot.predict_hot_keys(NOW) == {"HSI", "PONS"}
    world["markets"][1] = pons(0.50)
    fbot.store.put("sim:niulai#5|up|吃", {"v": 2, "market": "niulai#5", "slug": "niulai", "item": "$牛来 市值 $1B", "kind": "ladder", "key": "NIULAI",
                                          "side": "up", "label": "吃Yes", "maker": False, "fair": 0.10, "opened": NOW, "settle": {"target": "1000000000", "end": NOW + 86_400_000},
                                          "price": 0.05, "shares": 100.0, "order": 100.0, "status": "filled", "filled": NOW, "fill_fair": 0.10, "edge": 0.05,
                                          "fills": [], "revisions": []})  # a fresh position: its level stays hot (the markouts)
    assert fbot.predict_hot_keys(NOW) == {"HSI", "NIULAI"} and fbot.predict_hot(NOW) == ({"HSI", "NIULAI"}, {"5"})
    fbot.sim_wake = False
    assert await fbot.refresh_books(NOW) == m.Refreshed("ok") and seen["hot"] == {"HSI", "NIULAI"} and fbot.sim_wake is True
    assert "HSI" in seen["targets"] and seen["rows"] == {"5"}, "1.67.0: the hot levels go with the hot keys"
    # per level (1.67.0): a position past its markout window no longer keeps its level hot; a resting order does; the
    # other levels of a hot ladder are not hot (they follow the cold cadence, a few per round)
    old = fbot.store.get("sim:niulai#5|up|吃")
    fbot.store.put("sim:niulai#5|up|吃", {**old, "filled": NOW - 2 * 3600_000})
    assert fbot.predict_hot(NOW) == ({"HSI"}, set())
    fbot.store.put("sim:niulai#5|up|挂", {**old, "maker": True, "status": "resting", "shares": 0.0, "filled": None, "label": "挂Yes"})
    world["markets"].append(m.SimMarket("niulai#6", "$牛来 市值 $2B", "ladder", "NIULAI", 0.05, book([(0.01, 100)], [(0.60, 100)], NOW, slug="niulai", key="NIULAI", mid="6"),
                                        0.03, "", ("Yes", "No"), {"target": "2000000000", "end": NOW + 86_400_000}, {}))
    assert fbot.predict_hot(NOW) == ({"HSI", "NIULAI"}, {"5"})
    world["markets"][1] = pons(0.23)
    assert fbot.predict_hot(NOW) == ({"HSI", "NIULAI", "PONS"}, {"5", "77"})
    world["markets"].pop(); world["markets"][1] = pons(0.50)
    fbot.store.delete_keys(["sim:niulai#5|up|挂"]); fbot.sim_cache = None
    # the records come as a TradeMap (1.67.0): one market's or driver's records from an index that any change to the
    # map drops, the same answers as a scan of every record
    trades = fbot.sim_trades()
    assert isinstance(trades, m.TradeMap) and trades.of_market("niulai#5") == [trades["niulai#5|up|吃"]] and trades.of_market("x") == []
    assert m.trades_of_market(dict(trades), "niulai#5") == trades.of_market("niulai#5") and trades.of_driver("NIULAI") == [trades["niulai#5|up|吃"]]
    assert m.trades_of_driver(dict(trades), "NIULAI") == trades.of_driver("NIULAI") and trades.of_driver("PONS") == []
    trades["pons#77|up|吃"] = {"market": "pons#77", "kind": "ladder", "key": "PONS", "side": "up", "status": "filled", "shares": 50.0, "price": 0.2, "settle": {}}
    assert trades.of_driver("PONS") == [trades["pons#77|up|吃"]] and trades.of_market("pons#77") == [trades["pons#77|up|吃"]]
    moved = trades.pop("pons#77|up|吃")
    trades["pons#77|up|吃#1"] = moved
    assert trades.of_market("pons#77") == [moved] and list(trades.copy().of_market("pons#77")) == [moved]
    del trades["pons#77|up|吃#1"]
    assert trades.of_market("pons#77") == [] and trades.of_driver("PONS") == []
    for side in ("up", "down"):
        assert fbot.sim_taker_room(trades, niu, side) == fbot.sim_taker_room(dict(trades), niu, side)
        assert fbot.sim_group_room(trades, niu, side, 0.5, 100) == fbot.sim_group_room(dict(trades), niu, side, 0.5, 100)
        assert m.Bot.sim_own_resting(trades, "niulai#5", side) == m.Bot.sim_own_resting(dict(trades), "niulai#5", side)
    # a feed refresh that moved a fair price wakes the trader too (1.63.0); an unchanged one, or a feed the odds do not hang on, does not
    fbot.sim_wake, fbot.fair_sig = False, None
    fresh = lambda: setattr(fbot, "fair_sig_at", -1e9)  # (one look a second: the throttle is reset between these calls)
    fresh(); fbot.note_job("恒指期货", NOW)
    assert fbot.sim_wake and fbot.fair_sig and ("pons#77", 0.3) in fbot.fair_sig  # the first signature counts as a change
    fbot.sim_wake = False
    fresh(); fbot.note_job("恒指期货", NOW)
    assert not fbot.sim_wake, "nothing moved"
    world["markets"][0] = hsi(0.71, [(0.55, 100)], [(0.62, 100)], NOW)
    fresh(); fbot.note_job("汇率", NOW)
    assert not fbot.sim_wake, "not a feed the fair prices hang on"
    fresh(); fbot.note_job("上证/A50", NOW)
    assert fbot.sim_wake and ("hang-seng-index-up-or-down-on-october-5-2026", 0.71) in fbot.fair_sig
    fbot.sim_wake = False; fbot.note_job("上证/A50", NOW)
    assert not fbot.sim_wake, "within the second: not looked at"
    # the express lane (1.66.0; 1.88.0 the makers too): a market's book just came in -> its taker and its maker are decided now, without the step
    xbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                                    "SIM_WAYS": "both", "SIM_MARKETS": "all", **LEGACY}), m.Store(":memory:"), FM(NOW), None)
    xbot.sim_markets = lambda now: world["markets"]
    world["markets"] = [hsi(0.70, [(0.55, 100)], [(0.58, 100)], NOW)]
    assert xbot.predict.on_update == xbot.sim_express
    await xbot.sim_express("KOSPI")
    assert not xbot.sim_trades() and xbot.sim_express_count == 0, "another key: nothing here"
    await xbot.sim_express("HSI")
    both = [f"{HSI_SLUG}|up|吃", f"{HSI_SLUG}|up|挂"]
    assert sorted(xbot.sim_trades()) == both and xbot.sim_express_count == 1, sorted(xbot.sim_trades())
    await xbot.sim_express("HSI")
    assert xbot.sim_express_count == 1 and len(xbot.sim_trades()) == 2, "the slots are taken: nothing twice"
    xbot.predict.updated("HSI"); await asyncio.sleep(0.05)  # the feed's hook schedules the same
    assert xbot.sim_express_count == 1 and not xbot.predict.express_tasks
    await step(xbot, NOW + 5_000)  # the step finds both slots taken and leaves them alone
    assert sorted(xbot.sim_trades()) == both and set(xbot.sim_step_ms) == {"盘口", "判断", "结算", "保存"}
    # 1.88.0: the lane withdraws a resting order whose edge is gone (the exit line), frees its slot and quotes it again only
    # after its cooldown; a 买1 that moved (the follow) is the step's business
    tid = f"{HSI_SLUG}|up|挂"
    ebot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                                    "SIM_WAYS": "maker", "SIM_MARKETS": "all", **LEGACY, "SIM_MAKER_EXIT_CENTS": "5"}), m.Store(":memory:"), FM(NOW), None)
    ebot.sim_markets = lambda now: world["markets"]
    world["markets"] = [hsi(0.70, [(0.55, 100)], [(0.58, 100)], NOW)]
    await ebot.sim_express("HSI")
    assert sorted(ebot.sim_trades()) == [tid] and abs(ebot.sim_trades()[tid]["edge"] - 0.15) < 1e-9
    world["markets"] = [hsi(0.58, [(0.55, 100)], [(0.60, 100)], NOW)]  # 3¢ left under the 5¢ line
    await ebot.sim_express("HSI")
    t = ebot.sim_trades()
    assert sorted(t) == [f"{tid}#1"] and t[f"{tid}#1"]["status"] == "cancelled" and t[f"{tid}#1"]["withdrawn"]["why"] == "净优势降到 3.0¢，低于撤单线 5.0¢" \
        and tid in ebot.sim_express_withdrawn and ebot.sim_express_count == 2, t
    world["markets"] = [hsi(0.70, [(0.55, 100)], [(0.58, 100)], NOW)]  # the edge is back within the cooldown: the lane leaves the slot alone
    await ebot.sim_express("HSI")
    assert sorted(ebot.sim_trades()) == [f"{tid}#1"] and ebot.sim_express_count == 2, "within the cooldown"
    ebot.sim_express_withdrawn[tid] = -1e9
    await ebot.sim_express("HSI")
    assert sorted(ebot.sim_trades()) == [tid, f"{tid}#1"] and ebot.sim_trades()[tid]["status"] == "resting"
    assert m.Bot.requote_urgent("净优势降到 3.0¢，低于撤单线 5.0¢") and m.Bot.requote_urgent("盘后只挂净优势 ≥ 15.0¢ 的，现 9.0¢ 低于撤单线 14.0¢") \
        and m.Bot.requote_urgent("触发门槛已改为 12.0¢，现净优势 11.0¢ 不达标") and not m.Bot.requote_urgent("买1 移到 57.0¢，改跟") \
        and not m.Bot.requote_urgent("每次份数已改为 50 份，按新份数重挂") and not m.Bot.requote_urgent("积分已停止发放，不挂")
    fbot2 = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                                     "SIM_WAYS": "maker", "SIM_MARKETS": "all", **LEGACY, "SIM_MAKER_MIN_BID": "100"}), m.Store(":memory:"), FM(NOW), None)
    fbot2.sim_markets = lambda now: world["markets"]
    world["markets"] = [hsi(0.70, [(0.55, 100)], [(0.58, 100)], NOW)]
    await fbot2.sim_express("HSI")
    assert sorted(fbot2.sim_trades()) == [tid] and fbot2.sim_trades()[tid]["price"] == 0.55
    world["markets"] = [hsi(0.70, [(0.57, 100)], [(0.60, 100)], NOW)]
    await fbot2.sim_express("HSI")
    assert fbot2.sim_trades()[tid]["status"] == "resting" and fbot2.sim_trades()[tid]["price"] == 0.55, "the 买1 moved: the step's business"
    await step(fbot2, NOW + 5_000)
    assert sorted(fbot2.sim_trades()) == [f"{tid}#1"] and fbot2.sim_trades()[f"{tid}#1"]["withdrawn"]["why"] == "买1 移到 57.0¢，改跟"
    await step(fbot2, NOW + 10_000)
    assert sorted(fbot2.sim_trades()) == [tid, f"{tid}#1"] and fbot2.sim_trades()[tid]["price"] == 0.57
    print("FASTER_OK")


asyncio.run(run())
asyncio.run(maker_rules())
asyncio.run(follow_guard())
asyncio.run(review_fixes())
asyncio.run(taker_shares())
asyncio.run(taker_gap())
asyncio.run(faster())


async def prune():
    """1.71.0: the attempts that filled nothing are kept only SIM_ATTEMPTS_KEEP per slot (a maker re-quoted every few
    seconds for days left tens of thousands); a moved record is numbered past the highest ever used; the view follows
    a move without a rebuild; an old record's market id is looked up once, side by side with the others, and kept."""
    pbot = m.Bot(m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off",
                                    "SIM_WAYS": "both", "SIM_MARKETS": "all", **LEGACY}), m.Store(":memory:"), FM(NOW), None)
    base = {"v": 2, "market": HSI_SLUG, "slug": HSI_SLUG, "market_id": "101", "item": "恒生指数", "kind": "close", "key": "HSI", "side": "up",
            "label": "挂涨", "maker": True, "fair": 0.7, "settle": SETTLE_HSI, "order": 100.0, "fills": [], "revisions": [], "price": 0.55, "edge": 0.15}
    rows = [(f"{HSI_SLUG}|up|挂#{n}", {**base, "opened": NOW - n * 1000, "status": "cancelled", "shares": 0.0}) for n in range(1, 31)]
    rows += [(f"{HSI_SLUG}|up|挂#31", {**base, "opened": NOW - 31_000, "status": "filled", "shares": 40.0, "filled": NOW - 31_000}),  # a position: kept
             (f"{HSI_SLUG}|up|挂#32", {**base, "opened": NOW - 32_000, "status": "cancelled", "shares": 0.0, "live": {"state": "cancelling", "want": {}}}),  # a real order not final: kept
             (f"{HSI_SLUG}|up|挂", {**base, "opened": NOW, "status": "resting", "shares": 0.0}),
             (f"{HSI_SLUG}|down|吃#1", {**base, "side": "down", "label": "吃跌", "maker": False, "opened": NOW - 5000, "status": "cancelled", "shares": 0.0})]
    pbot.sim_save(rows)
    assert len(pbot.sim_trades()) == 34
    assert pbot.sim_prune() == 10 and pbot.sim_prune() == 0
    kept = pbot.sim_trades()
    assert len(kept) == 24 and all(f"{HSI_SLUG}|up|挂#{n}" in kept for n in range(1, 21)) and not any(f"{HSI_SLUG}|up|挂#{n}" in kept for n in range(21, 31)), sorted(kept)
    assert f"{HSI_SLUG}|up|挂#31" in kept and f"{HSI_SLUG}|up|挂#32" in kept and f"{HSI_SLUG}|up|挂" in kept and f"{HSI_SLUG}|down|吃#1" in kept
    assert pbot.store.count("sim:") == 24, "gone from the store too"
    gen = pbot.store.touched.get("sim", 0)
    new_tid = pbot.sim_rekey(f"{HSI_SLUG}|up|挂", kept[f"{HSI_SLUG}|up|挂"])
    assert new_tid == f"{HSI_SLUG}|up|挂#33", new_tid  # past the highest ever used, never a pruned number
    assert pbot.sim_cache is not None and new_tid in pbot.sim_cache and f"{HSI_SLUG}|up|挂" not in pbot.sim_cache and pbot.sim_gen == pbot.store.touched.get("sim", 0) > gen
    assert pbot.sim_trades()[new_tid]["status"] == "resting" and pbot.store.get(f"sim:{new_tid}") is not None and pbot.store.get(f"sim:{HSI_SLUG}|up|挂") is None
    # the step prunes once an hour (and at its first look)
    pbot.sim_save([(f"{HSI_SLUG}|up|挂#{n}", {**base, "opened": NOW - n * 1000, "status": "cancelled", "shares": 0.0}) for n in range(40, 70)])
    pbot.sim_markets = lambda now: []
    pbot.sim_ran = -1e9
    await pbot.sim_step(NOW)
    assert pbot.store.count("sim:") == 24 and pbot.sim_pruned_at > 0, pbot.store.count("sim:")  # the 30 new attempts: the newest 20 of the slot stay (#1–#20), the rest go
    # old records without a market id: looked up by slug a few at a time, remembered on the record and in the bot
    looked = []
    async def resolve(slug):
        looked.append(slug)
        return {"id": "909"} if slug == "old-slug" else None
    asked = []
    async def details(mid):
        asked.append(mid)
        return {"outcomes": ["Up", "Down"], "status": "REGISTERED", "resolved": None}
    pbot.predict.resolve, pbot.predict.market_details = resolve, details
    legacy = {**base, "market": "old-slug", "slug": "old-slug", "opened": NOW - 86_400_000, "status": "filled", "shares": 100.0,
              "local": {"up": 1.0, "note": "x", "at": NOW - 1000, "evidence": {}}, "confirm": "local"}
    legacy.pop("market_id")
    pbot.sim_save([("old-slug|up|挂", legacy), ("gone-slug|up|挂", {**legacy, "market": "gone-slug", "slug": "gone-slug"})])
    await pbot.sim_confirm(pbot.sim_trades(), NOW)
    assert looked == ["old-slug", "gone-slug"] and asked == ["909"] and pbot.sim_mids == {"old-slug": "909"}, (looked, asked)
    assert pbot.sim_trades()["old-slug|up|挂"]["market_id"] == "909" and "market_id" not in pbot.sim_trades()["gone-slug|up|挂"]
    await pbot.sim_confirm(pbot.sim_trades(), NOW)
    assert looked == ["old-slug", "gone-slug"], "found: on the record; not found: not asked again within SIM_CONFIRM_SECONDS"
    print("PRUNE_OK")


asyncio.run(prune())


async def fair_limit():
    """1.72.0 / 1.85.0: SIM_MAX_FAIR_CENTS — a side the model calls near certain is judged at the limit, no higher: the
    cents of edge the model claims past it do not pay for the tail a normal model cannot see, so such a side is bought or
    quoted only when the price leaves the bar under the limit (before 1.85.0 it was refused outright, whatever the price)."""
    env = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
           "SIM_WAYS": "both", "SIM_MARKETS": "all", **LEGACY}
    assert m.Config.from_env(env).sim_max_fair == 0.95 and m.Config.from_env({**env, "SIM_MAX_FAIR_CENTS": "100"}).sim_max_fair == 1.0
    for bad in ("49", "101", "x"):
        try: m.Config.from_env({**env, "SIM_MAX_FAIR_CENTS": bad}); assert False, bad
        except ValueError: pass
    near = lambda pair, want: all(abs(a - b) < 1e-9 for a, b in zip(pair, want))
    sure = lambda at, fair=0.97, bids=(("0.80", "100"),), asks=(("0.85", "100"),): m.dataclasses.replace(
        hsi(fair, list(bids), list(asks), at), in_session=True)  # 涨 97¢: by the card a taker at 85¢ has +11.7¢ and a maker at 80¢ +17¢
    fbot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(NOW), None)
    fbot.sim_markets = lambda now: world["markets"]
    world["markets"] = [sure(NOW)]
    assert near(fbot.sim_fair(sure(NOW), "up"), (0.95, 0.02)) and near(fbot.sim_fair(sure(NOW), "down"), (0.03, 0.0))
    assert near(fbot.sim_fair(sure(NOW, 0.95), "up"), (0.95, 0.0)) and near(fbot.sim_fair(sure(NOW, 0.03), "down"), (0.95, 0.02))
    # judged at 95¢: the taker's cap is 95 − 10 after the fee = 84¢ (m.taker_cap), the 85¢ ask a cent over it; the maker at the
    # 80¢ 买1 has 15¢ (not the card's 17) and goes
    cap = m.taker_cap(0.95, 0.10, 200)
    assert cap == 0.84 and fbot.sim_taker_cap(sure(NOW), "up", 200) == cap
    gap = fbot.sim_taker_gap(sure(NOW), fbot.sim_trades(), NOW)
    assert gap["state"] == "near" and gap["label"] == "吃涨" and abs(gap["gap"] - 0.01) < 1e-9, gap
    assert gap["text"] == "距吃单 1.0¢：吃涨 卖1 85.0¢，封顶 84.0¢（公平价 97.0¢ 按上限 95.0¢ 计）" and gap["sides"][0]["fair"] == 0.95 \
        and gap["sides"][0]["model"] == 0.97 and gap["sides"][1]["capped"] == "" and "吃跌：公平价 3.0¢ 留不出 10.0¢" in gap["detail"], gap
    cand, why = fbot.sim_maker_reason(sure(NOW), {}, NOW)
    assert why == "" and abs(cand.price - 0.80) < 1e-9 and abs(cand.edge - 0.15) < 1e-9, (cand, why)
    row = next(r for r in fbot.sim_checks(NOW) if r["market"] == HSI_SLUG)
    assert "涨：封顶 84.0¢（公平价 97.0¢ 按上限 95.0¢ 计），之下没有卖单" in row["taker"] and row["maker_why"] == "", row
    await step(fbot, NOW, sure(NOW))
    assert sorted(fbot.sim_trades()) == [f"{HSI_SLUG}|up|挂"] and not fbot.sim_blocks(), "the maker only, and not a refusal to record"
    t = fbot.sim_trades()[f"{HSI_SLUG}|up|挂"]
    assert t["fair"] == 0.97 and abs(t["edge"] - 0.15) < 1e-9 and abs(t["signal"] - 0.15) < 1e-9 and t["entry"]["fair_cap"] == 0.95 \
        and t["entry"]["fair"] == 0.97, t
    assert fbot.sim_version()["sim_max_fair"] == 0.95 and fbot.control_value("SIM_MAX_FAIR_CENTS") == "95"
    # an ask under the cap is taken: 84¢ + the 0.32¢ fee leaves 10.68¢ under the 95¢ it is judged by (the card says 12.68)
    tbot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(NOW), None)
    tbot.sim_markets = lambda now: world["markets"]
    await step(tbot, NOW, sure(NOW, asks=(("0.84", "100"),)))
    t = tbot.sim_trades()[f"{HSI_SLUG}|up|吃"]
    assert t["shares"] == 100 and abs(t["price"] - 0.8432) < 1e-9 and abs(t["edge"] - 0.1068) < 1e-9 and t["fair"] == 0.97 \
        and t["fill_fair"] == 0.97 and t["fills"][0]["fair"] == 0.97 and t["entry"]["fair_cap"] == 0.95, t
    # a 买1 too near the limit is not joined, and the reason says what the fair price was judged at
    assert fbot.sim_maker_reason(sure(NOW, bids=(("0.90", "100"),), asks=(("0.95", "100"),)), {}, NOW) == \
        (None, "净优势 5.0¢ 低于触发门槛 10.0¢；涨的公平价 97.0¢ 按上限 95.0¢ 计")
    # a resting order's withdrawal line is judged the same way: at 92¢ under a 97¢ card the model sees 5¢, the trader 3¢
    resting = {"maker": True, "side": "up", "price": 0.92, "status": "resting", "order": 100.0, "shares": 0.0, "market": HSI_SLUG, "kind": "close", "key": "HSI"}
    tight = lambda at, fair=0.97: sure(at, fair, bids=(("0.92", "100"),), asks=(("0.96", "100"),))
    xbot = m.Bot(m.Config.from_env({**env, "SIM_MAKER_EXIT_CENTS": "5"}), m.Store(":memory:"), FM(NOW), None)
    assert xbot.sim_requote_reason(resting, tight(NOW), NOW) == "净优势降到 3.0¢，低于撤单线 5.0¢"
    ybot = m.Bot(m.Config.from_env({**env, "SIM_MAKER_EXIT_CENTS": "5", "SIM_MAX_FAIR_CENTS": "100"}), m.Store(":memory:"), FM(NOW), None)
    assert ybot.sim_requote_reason(resting, tight(NOW), NOW) == "", ybot.sim_requote_reason(resting, tight(NOW), NOW)
    for limit in ("100", "98"):  # no limit, or one the 97¢ side is under: judged as the card says, both orders go
        obot = m.Bot(m.Config.from_env({**env, "SIM_MAX_FAIR_CENTS": limit}), m.Store(":memory:"), FM(NOW), None)
        obot.sim_markets = lambda now: world["markets"]
        await step(obot, NOW, sure(NOW))
        assert sorted(obot.sim_trades()) == [f"{HSI_SLUG}|up|吃", f"{HSI_SLUG}|up|挂"], (limit, sorted(obot.sim_trades()))
        assert "fair_cap" not in obot.sim_trades()[f"{HSI_SLUG}|up|吃"]["entry"] and abs(obot.sim_trades()[f"{HSI_SLUG}|up|挂"]["edge"] - 0.17) < 1e-9
    print("FAIR_LIMIT_OK")


asyncio.run(fair_limit())


async def after_hours_cap():
    """1.73.0: SIM_AFTER_HOURS_MAX_SHARES — after hours a market's side holds at most this many shares, positions and
    orders together: an order is cut to the room left, none when there is none; in session the cap does not apply."""
    env = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
           "SIM_WAYS": "both", "SIM_MARKETS": "all", **LEGACY, "SIM_TAKER_MAX_SHARES": "1000", "SIM_TAKER_SWEEP_SHARES": "300", "SIM_GROUP_USD": "0"}
    assert m.Config.from_env(env).sim_after_hours_max_shares == 300 and m.Config.from_env({**env, "SIM_AFTER_HOURS_MAX_SHARES": "0"}).sim_after_hours_max_shares == 0
    after = lambda at, session=False: m.dataclasses.replace(hsi(0.70, [("0.48", "1000")], [("0.50", "1000")], at), in_session=session)
    abot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(NOW), None)
    abot.sim_markets = lambda now: world["markets"]
    assert abot.sim_side_room({}, after(NOW), "up") == (300.0, "") and abot.sim_side_room({}, after(NOW, True), "up") == (m.math.inf, "")
    await step(abot, NOW, after(NOW))  # the maker first (100 resting), then the taker: 300 wanted, 200 left under the cap
    t = abot.sim_trades()
    maker, taker = t[f"{HSI_SLUG}|up|挂"], t[f"{HSI_SLUG}|up|吃"]
    assert maker["status"] == "resting" and maker["order"] == 100 and taker["status"] == "filled" and taker["shares"] == 200 and taker["order"] == 200, (maker["order"], taker["order"])
    room, full = abot.sim_side_room(t, after(NOW), "up")
    assert room == 0.0 and full == "盘后这一边持仓 200 份 + 挂单 1 笔未成交 100 份 = 300 份，达到上限 300 份（SIM_AFTER_HOURS_MAX_SHARES）", (room, full)
    # 1.81.1: the refusal itemises what fills the cap (10-20: "已有 499.195 份（持仓加挂单）" on the SSE card against a 195.6-share
    # position on Predict — the rest sat unfilled in two resting orders, which its positions tab does not show); a partial
    # fill counts as position, the rest of its order as resting; the other side, withdrawn, expired and settled records
    # hold nothing; under a share short of the cap the line says so rather than "达到上限"
    mkt = after(NOW).market
    rec = lambda status, shares, order, side="down", maker=True: {"market": mkt, "side": side, "status": status, "shares": shares, "order": order, "maker": maker}
    t5 = {"a": rec("filled", 195.195, 200, maker=False), "b": rec("resting", 0.0, 150), "c": rec("resting", 0.405, 154),
          "d": rec("resting", 0.0, 100, side="up"), "e": rec("cancelled", 0.0, 100), "f": rec("settled", 100.0, 100), "g": rec("expired", 0.0, 100)}
    held = m.side_held(t5, mkt, "down")
    assert abs(held["positions"] - 195.6) < 1e-9 and abs(held["resting"] - 303.595) < 1e-9 and held["orders"] == 2 and abs(held["total"] - 499.195) < 1e-9, held
    assert m.side_held_text(held) == "持仓 195.6 份 + 挂单 2 笔未成交 303.595 份 = 499.195 份"
    cbot = m.Bot(m.Config.from_env({**env, "SIM_AFTER_HOURS_MAX_SHARES": "500"}), m.Store(":memory:"), FM(NOW), None)
    assert cbot.sim_side_room(t5, after(NOW), "down") == (0.0, "盘后这一边持仓 195.6 份 + 挂单 2 笔未成交 303.595 份 = 499.195 份，离上限 500 份只差 0.805 份、不足 1 份（SIM_AFTER_HOURS_MAX_SHARES）"), cbot.sim_side_room(t5, after(NOW), "down")
    room, _ = cbot.sim_side_room(t5, after(NOW), "up")
    assert abs(room - 595.6) < 1e-9, (room, "1.91.0: net — the other side's 195.6 filled offset this side's 100 resting (its resting orders do not)")
    pbot = m.Bot(m.Config.from_env({**env, "SIM_AFTER_HOURS_MAX_SHARES": "500", "SIM_SHARES_NET": "off"}), m.Store(":memory:"), FM(NOW), None)
    assert pbot.sim_side_room(t5, after(NOW), "up") == (400.0, ""), "per side: the other side counts its own records only"
    # the net cap, itemised: 400 涨 held against 400 跌 is flat (room 500 under a 500 cap); 900 against 400 is at it
    nenv = {**env, "SIM_AFTER_HOURS_MAX_SHARES": "500"}
    nbot = m.Bot(m.Config.from_env(nenv), m.Store(":memory:"), FM(NOW), None)
    hedged = {"u": rec("filled", 400, 400, side="up", maker=False), "d": rec("filled", 400, 400, maker=False)}
    assert nbot.sim_side_room(hedged, after(NOW), "up") == (500.0, "") and nbot.sim_side_room(hedged, after(NOW), "down") == (500.0, "")
    assert pbot.sim_side_room(hedged, after(NOW), "up") == (100.0, "")
    heavy = {"u": rec("filled", 900, 900, side="up", maker=False), "d": rec("filled", 400, 400, maker=False), "r": rec("resting", 0.0, 100, side="up")}
    assert nbot.sim_side_room(heavy, after(NOW), "up") == (0.0, "盘后这一边持仓 900 份 + 挂单 1 笔未成交 100 份 = 1000 份，另一边持仓 400 份抵掉、净 600 份，达到上限 500 份（SIM_AFTER_HOURS_MAX_SHARES）"), nbot.sim_side_room(heavy, after(NOW), "up")
    assert nbot.sim_side_room(heavy, after(NOW), "down") == (1000.0, ""), "down: 400 less the 900 filled on up — the 100 resting on up offsets nothing"
    assert abot.sim_side_room({"a": rec("filled", 300, 300, maker=False)}, after(NOW), "down") == (0.0, "盘后这一边持仓 300 份，达到上限 300 份（SIM_AFTER_HOURS_MAX_SHARES）")
    assert abot.sim_side_room({"b": rec("resting", 0.0, 300)}, after(NOW), "down") == (0.0, "盘后这一边挂单 1 笔未成交 300 份，达到上限 300 份（SIM_AFTER_HOURS_MAX_SHARES）")
    assert abot.sim_side_room({"b": rec("resting", 0.0, 299.5)}, after(NOW), "down") == (0.0, "盘后这一边挂单 1 笔未成交 299.5 份，离上限 300 份只差 0.5 份、不足 1 份（SIM_AFTER_HOURS_MAX_SHARES）")
    assert abot.sim_side_room({"a": rec("filled", 100, 100, maker=False), "c": rec("resting", 20.0, 120)}, after(NOW), "down") == (80.0, ""), "the room left: 300 − (100 + 20 filled) − 100 unfilled"
    # 1.82.0: a deep-bid order (SIM_MAKER_DEEP_CENTS; a sweep order too) is outside the cap — placed whatever the side holds,
    # at its whole count — and its unfilled shares are not counted against it; what it fills is a position, counted
    at_cap = {"a": rec("filled", 300, 300, side="up", maker=False)}
    deep_cand = m.BookEdge("涨", True, 0.06, 0.32, 0.0, 0.32, front=True)
    sweep_cand = m.BookEdge("涨", True, 0.02, 0.36, 0.0, 0.36, front=True, sweep=True)
    join_cand = m.BookEdge("涨", True, 0.05, 0.33, 300.0, 0.33)
    assert abot.sim_maker_pick(after(NOW), at_cap, "up", deep_cand, NOW) == (100.0, "") and abot.sim_maker_pick(after(NOW), at_cap, "up", sweep_cand, NOW) == (100.0, "")
    assert abot.sim_maker_pick(after(NOW), at_cap, "up", join_cand, NOW) == (100.0, "盘后这一边持仓 300 份，达到上限 300 份（SIM_AFTER_HOURS_MAX_SHARES）")
    part = {"a": rec("filled", 250, 250, side="up", maker=False)}
    assert abot.sim_maker_pick(after(NOW), part, "up", join_cand, NOW) == (50.0, "") and abot.sim_maker_pick(after(NOW), part, "up", deep_cand, NOW) == (100.0, "")
    t6 = {**at_cap, "b": {**rec("resting", 0.405, 304, side="up"), "deep": True}}
    held = m.side_held(t6, mkt, "up")
    assert abs(held["positions"] - 300.405) < 1e-9 and abs(held["deep"] - 303.595) < 1e-9 and held["orders"] == 0 and held["resting"] == 0 and abs(held["total"] - 300.405) < 1e-9, held
    assert abot.sim_side_room(t6, after(NOW), "up") == (0.0, "盘后这一边持仓 300.405 份，另有低价挂单 303.595 份不占额度，达到上限 300 份（SIM_AFTER_HOURS_MAX_SHARES）")
    room6, full6 = abot.sim_side_room({"b": t6["b"]}, after(NOW), "up")
    assert abs(room6 - 299.595) < 1e-9 and full6 == "", "a resting deep-bid order alone: only the 0.405 it filled counts"
    # in a step: the deep-bid mode on a wide book after hours places its whole SIM_SHARES over a 50-share cap; an ordinary
    # order on the same cap is cut to 50
    wbot = m.Bot(m.Config.from_env({**env, "SIM_WAYS": "maker", "SIM_MAKER_DEEP_CENTS": "25", "SIM_AFTER_HOURS_MAX_SHARES": "50"}), m.Store(":memory:"), FM(NOW), None)
    wbot.sim_markets = lambda now: world["markets"]
    wide_after = m.dataclasses.replace(hsi(0.38, [("0.05", "300"), ("0.01", "1000")], [("0.38", "1"), ("0.40", "100")], NOW), in_session=False)
    assert wbot.sim_maker_reason(wide_after, {}, NOW)[0].front and wbot.sim_maker_pick(wide_after, {}, "up", wbot.sim_maker_reason(wide_after, {}, NOW)[0], NOW) == (100.0, "")
    await step(wbot, NOW, wide_after)
    tw = wbot.sim_trades()[f"{HSI_SLUG}|up|挂"]
    assert tw["status"] == "resting" and tw["deep"] is True and tw["order"] == 100 and tw["price"] == 0.06, tw
    assert wbot.sim_side_room(wbot.sim_trades(), wide_after, "up") == (50.0, "")
    obot5 = m.Bot(m.Config.from_env({**env, "SIM_WAYS": "maker", "SIM_AFTER_HOURS_MAX_SHARES": "50"}), m.Store(":memory:"), FM(NOW), None)
    obot5.sim_markets = lambda now: world["markets"]
    await step(obot5, NOW, after(NOW))
    to5 = obot5.sim_trades()[f"{HSI_SLUG}|up|挂"]
    assert to5["status"] == "resting" and "deep" not in to5 and to5["order"] == 50, to5
    assert obot5.sim_side_room(obot5.sim_trades(), after(NOW), "up") == (0.0, "盘后这一边挂单 1 笔未成交 50 份，达到上限 50 份（SIM_AFTER_HOURS_MAX_SHARES）")
    gap = abot.sim_taker_gap(after(NOW + 5_000), t, NOW + 5_000)
    assert gap["state"] == "blocked" and "达到上限 300 份" in gap["text"], gap
    await step(abot, NOW + 5_000, after(NOW + 5_000))  # full: nothing more after hours, the refusal noted
    assert len([k for k in abot.sim_trades() if k.startswith(f"{HSI_SLUG}|up|吃")]) == 1 and any("达到上限 300 份" in b["why"] for b in abot.sim_blocks())
    assert abot.sim_version()["sim_after_hours_max_shares"] == 300 and abot.control_value("SIM_AFTER_HOURS_MAX_SHARES") == "300"
    await step(abot, NOW + 10_000, after(NOW + 10_000, session=True))  # in session: no cap, the taker buys its 300
    t = abot.sim_trades()
    assert t[f"{HSI_SLUG}|up|吃"]["shares"] == 300 and t[f"{HSI_SLUG}|up|吃#1"]["shares"] == 200, sorted(t)
    obot = m.Bot(m.Config.from_env({**env, "SIM_AFTER_HOURS_MAX_SHARES": "0"}), m.Store(":memory:"), FM(NOW), None)  # off: as before
    obot.sim_markets = lambda now: world["markets"]
    await step(obot, NOW, after(NOW))
    assert obot.sim_trades()[f"{HSI_SLUG}|up|吃"]["shares"] == 300 and obot.sim_trades()[f"{HSI_SLUG}|up|挂"]["order"] == 100
    print("AFTER_HOURS_CAP_OK")


asyncio.run(after_hours_cap())


async def no_odds_withdraw():
    """1.92.0: SIM_NO_ODDS_WITHDRAW_MINUTES — a resting order on a market the bot cannot price now (its card's odds
    missing: the SSE card over the weekend, A50 closed; or its suggestion held) is withdrawn once that has lasted this
    long, placed afresh when the odds return; 0 = it stands, as before. (10-25: a 300-share order stood on the SSE market
    all weekend under 概率暂缺 — no new order goes on such a market, but the old one was never withdrawn.)"""
    env = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
           "SIM_WAYS": "both", "SIM_MARKETS": "all", **LEGACY, "SIM_GROUP_USD": "0"}
    assert m.Config.from_env(env).sim_no_odds_withdraw == 30 and m.Config.from_env({**env, "SIM_NO_ODDS_WITHDRAW_MINUTES": "0"}).sim_no_odds_withdraw == 0
    assert m.Config.from_env(env).sim_no_odds_withdraw == 30 and "sim_no_odds_withdraw" not in m.Bot(m.Config.from_env({**env, "SIM_NO_ODDS_WITHDRAW_MINUTES": "0"}), m.Store(":memory:"), FM(NOW), None).sim_version()
    bot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(NOW), None)
    bot.sim_markets = lambda now: world["markets"]
    priced = lambda at, **kw: m.dataclasses.replace(hsi(0.70, [("0.55", "300")], [("0.58", "400")], at, **kw), in_session=True)
    tid = f"{HSI_SLUG}|up|挂"
    await step(bot, NOW, priced(NOW))
    assert bot.sim_trades()[tid]["status"] == "resting" and bot.sim_trades()[tid]["price"] == 0.55 and bot.sim_unpriced_since == {}
    bot.odds_items = lambda now_ms, only=None: [("恒生指数｜HSI", "A50 休市，10-26 09:00 开盘后恢复概率")]  # the card's odds gone: a string says why
    world["markets"] = []  # nothing priced: the step meets the record with no market for it
    await step(bot, NOW + 60_000)
    assert bot.sim_trades()[tid]["status"] == "resting" and bot.sim_unpriced_since == {HSI_SLUG: NOW + 60_000}, "the clock starts"
    await step(bot, NOW + 60_000 + 29 * 60_000)
    assert bot.sim_trades()[tid]["status"] == "resting" and bot.sim_unpriced_since == {HSI_SLUG: NOW + 60_000}
    await step(bot, NOW + 60_000 + 30 * 60_000)
    t = bot.sim_trades()
    assert tid not in t and t[f"{tid}#1"]["status"] == "cancelled" and float(t[f"{tid}#1"]["shares"]) == 0, t.get(f"{tid}#1")
    assert "概率暂缺已超过 30 分钟（A50 休市，10-26 09:00 开盘后恢复概率），撤单" in t[f"{tid}#1"]["note"], t[f"{tid}#1"]["note"]
    assert bot.sim_unpriced_since == {}, "nothing resting there any more: the clock is dropped"
    # the odds back: the slot is free, the order placed afresh on the new view
    at = NOW + 60_000 + 31 * 60_000
    await step(bot, at, priced(at))
    assert bot.sim_trades()[tid]["status"] == "resting" and bot.sim_unpriced_since == {}
    # a held card (odds there, the suggestion held) counts the same; a priced step in between restarts the clock
    await step(bot, at + 60_000, priced(at + 60_000, hold="数据有疑问"))
    assert bot.sim_unpriced_since == {HSI_SLUG: at + 60_000}
    await step(bot, at + 10 * 60_000, priced(at + 10 * 60_000))
    assert bot.sim_unpriced_since == {} and bot.sim_trades()[tid]["status"] == "resting"
    held_at = at + 20 * 60_000
    await step(bot, held_at, priced(held_at, hold="数据有疑问"))
    await step(bot, held_at + 29 * 60_000, priced(held_at + 29 * 60_000, hold="数据有疑问"))
    assert bot.sim_trades()[tid]["status"] == "resting" and bot.sim_unpriced_since == {HSI_SLUG: held_at}
    await step(bot, held_at + 30 * 60_000, priced(held_at + 30 * 60_000, hold="数据有疑问"))
    t = bot.sim_trades()
    assert tid not in t and "卡片暂不建议已超过 30 分钟（数据有疑问），撤单" in t[f"{tid}#2"]["note"], t.get(f"{tid}#2")
    # 0 = never: the order stands through hours without odds
    zbot = m.Bot(m.Config.from_env({**env, "SIM_NO_ODDS_WITHDRAW_MINUTES": "0"}), m.Store(":memory:"), FM(NOW), None)
    zbot.sim_markets = lambda now: world["markets"]
    await step(zbot, NOW, priced(NOW))
    world["markets"] = []
    await step(zbot, NOW + 60_000); await step(zbot, NOW + 5 * 3600_000)
    assert zbot.sim_trades()[tid]["status"] == "resting" and zbot.sim_unpriced_since == {}
    print("NO_ODDS_WITHDRAW_OK")


asyncio.run(no_odds_withdraw())


async def open_quiet():
    """1.74.0: SIM_OPEN_QUIET_MINUTES — no new order on a daily card in the first minutes after its underlying opens
    (the opening auction just done, the feeds lagging the most): takers and makers alike; crypto markets never."""
    env = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
           "SIM_WAYS": "both", "SIM_MARKETS": "all", **LEGACY}
    qbot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(NOW), None)
    qbot.sim_markets = lambda now: world["markets"]
    assert qbot.config.sim_open_quiet_minutes == 3 and dt.date(2026, 10, 5) not in qbot.config.holidays["hk"]
    live = lambda at: m.dataclasses.replace(hsi(0.70, [("0.55", "300")], [("0.58", "400")], at), in_session=True)  # HSI: HK opens 09:30
    at = BJ(10, 5, 9, 31)
    assert qbot.sim_open_quiet(live(at), at) == "开盘后 3 分钟内不开新单，挂单撤掉（SIM_OPEN_QUIET_MINUTES）"
    assert qbot.sim_open_quiet(live(at), BJ(10, 5, 9, 26)) == "" and qbot.sim_open_quiet(live(at), BJ(10, 5, 9, 33)) == ""
    # 1.75.0: the minutes before an open too, and the afternoon open (HK 13:00) as much as the day's first
    assert qbot.sim_open_quiet(live(at), BJ(10, 5, 9, 27)) == "开盘前 3 分钟内不开新单，挂单撤掉（SIM_OPEN_QUIET_MINUTES）"
    assert qbot.sim_open_quiet(live(at), BJ(10, 5, 12, 58)) == "午后开盘前 3 分钟内不开新单，挂单撤掉（SIM_OPEN_QUIET_MINUTES）"
    assert qbot.sim_open_quiet(live(at), BJ(10, 5, 13, 2)) == "午后开盘后 3 分钟内不开新单，挂单撤掉（SIM_OPEN_QUIET_MINUTES）"
    assert qbot.sim_open_quiet(live(at), BJ(10, 5, 12, 56)) == "" and qbot.sim_open_quiet(live(at), BJ(10, 5, 13, 3)) == ""
    assert qbot.sim_open_quiet(m.dataclasses.replace(live(at), kind="updown"), at) == "", "crypto markets have no open"
    assert qbot.sim_open_quiet(live(at), BJ(10, 3, 9, 31)) == "", "a Saturday"
    gap = qbot.sim_taker_gap(live(at), qbot.sim_trades(), at)
    assert gap["state"] == "blocked" and "开盘后 3 分钟内不开新单" in gap["text"], gap
    maker = qbot.sim_maker_reason(live(at), {}, at)[0]
    assert maker is not None and qbot.sim_maker_pick(live(at), qbot.sim_trades(), "up", maker, at)[1].startswith("开盘后 3 分钟内不开新单")
    await step(qbot, at, live(at))
    assert not qbot.sim_trades(), "nothing in the quiet minutes"
    await step(qbot, BJ(10, 5, 9, 34), live(BJ(10, 5, 9, 34)))
    assert sorted(qbot.sim_trades()) == [f"{HSI_SLUG}|up|吃", f"{HSI_SLUG}|up|挂"], sorted(qbot.sim_trades())
    assert qbot.sim_version()["sim_open_quiet_minutes"] == 3 and qbot.control_value("SIM_OPEN_QUIET_MINUTES") == "3"
    obot = m.Bot(m.Config.from_env({**env, "SIM_OPEN_QUIET_MINUTES": "0"}), m.Store(":memory:"), FM(NOW), None)
    obot.sim_markets = lambda now: world["markets"]
    await step(obot, at, live(at))
    assert len(obot.sim_trades()) == 2
    # 1.75.0: a resting order is withdrawn for the window around an open and placed afresh after it — the one placed
    # overnight on the proxy's fair price above all (LEGACY: SIM_MAKER_SESSION off, so it rests before the open)
    wbot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(NOW), None)
    wbot.sim_markets = lambda now: world["markets"]
    night = lambda at: m.dataclasses.replace(hsi(0.70, [("0.55", "300")], [("0.58", "400")], at), in_session=False)
    await step(wbot, BJ(10, 5, 9, 20), night(BJ(10, 5, 9, 20)))
    assert wbot.sim_trades()[f"{HSI_SLUG}|up|挂"]["status"] == "resting"
    await step(wbot, BJ(10, 5, 9, 27), night(BJ(10, 5, 9, 27)))
    t = wbot.sim_trades()
    assert f"{HSI_SLUG}|up|挂" not in t and t[f"{HSI_SLUG}|up|挂#1"]["status"] == "cancelled", sorted(t)
    assert t[f"{HSI_SLUG}|up|挂#1"]["withdrawn"]["why"] == "开盘前 3 分钟内不开新单，挂单撤掉（SIM_OPEN_QUIET_MINUTES）", t[f"{HSI_SLUG}|up|挂#1"]["withdrawn"]
    await step(wbot, BJ(10, 5, 9, 31), live(BJ(10, 5, 9, 31)))
    assert not any(k.startswith(f"{HSI_SLUG}|up|挂") and v["status"] == "resting" for k, v in wbot.sim_trades().items()), "nothing placed in the window"
    await step(wbot, BJ(10, 5, 9, 34), live(BJ(10, 5, 9, 34)))
    assert wbot.sim_trades()[f"{HSI_SLUG}|up|挂"]["status"] == "resting", sorted(wbot.sim_trades())
    # 1.75.0: the lunch break puts a daily card under the after-hours rules though it is still priced from the spot
    assert qbot.sim_exchange("HSI") == "hk" and qbot.sim_exchange("UNITREEUSDT") == "sh" and qbot.sim_exchange("BNB") == ""
    assert qbot.sim_lunch("hk", BJ(10, 5, 12, 30)) and not qbot.sim_lunch("hk", BJ(10, 5, 11, 0)) and not qbot.sim_lunch("", BJ(10, 5, 12, 30)) and not qbot.sim_lunch("kr", BJ(10, 5, 12, 30))
    lbot = m.Bot(m.Config.from_env({**env, "SIM_MAKER_SESSION": "on", "SIM_TAKER_SESSION": "on"}), m.Store(":memory:"), FM(NOW), None)
    odds = m.close_odds("恒生指数", D("24600"), D("24650"), 0.013, 0.5, dt.date(2026, 10, 5), D("0.01"), "10-02 收盘", "恒指现货 24,650（盘中直接用现货）", "σ")
    lbot.odds_items = lambda ms: [("恒生指数", odds)]
    lbot.predict.books["HSI"] = book([("0.55", "300")], [("0.58", "400")], BJ(10, 5, 11, 0))
    mk = lbot.sim_markets(BJ(10, 5, 11, 0))[0]
    assert odds.direct and mk.in_session is True and not mk.lunch, (mk.in_session, mk.lunch)
    lbot.predict.books["HSI"] = book([("0.55", "300")], [("0.58", "400")], BJ(10, 5, 12, 30))
    mk = lbot.sim_markets(BJ(10, 5, 12, 30))[0]
    assert mk.lunch and mk.in_session is False, (mk.in_session, mk.lunch)
    assert lbot.sim_maker_reason(mk, {}, BJ(10, 5, 12, 30))[1] == "标的午休，不挂单", lbot.sim_maker_reason(mk, {}, BJ(10, 5, 12, 30))
    assert "标的午休，不吃单" in lbot.sim_taker_gap(mk, {}, BJ(10, 5, 12, 30))["text"], lbot.sim_taker_gap(mk, {}, BJ(10, 5, 12, 30))
    resting = {"status": "resting", "price": 0.55, "order": 100.0, "shares": 0.0, "maker": True, "side": "up", "kind": "close", "market": HSI_SLUG, "version": {"sim_shares": 100}}
    assert lbot.sim_requote_reason(resting, mk, BJ(10, 5, 12, 30)) == "标的午休，不挂单"
    # 1.79.0: the express lane builds one key's markets alone (sim_markets(only=key)); a stubbed odds_items is filtered
    assert [x.key for x in lbot.sim_markets(BJ(10, 5, 11, 0), "HSI")] == ["HSI"] and lbot.sim_markets(BJ(10, 5, 11, 0), "KOSPI") == []
    assert [x.key for x in lbot.sim_markets_for("HSI", BJ(10, 5, 11, 0))] == ["HSI"] and qbot.sim_markets_for("HSI", at) == [mk for mk in world["markets"] if mk.key == "HSI"]
    assert m.Config.from_env(env).hl_refresh == 10 and m.Config.from_env({**env, "HL_REFRESH_SECONDS": "5"}).hl_refresh == 5 and m.Hyperliquid({}).refresh_seconds == 10
    try: m.Config.from_env({**env, "HL_REFRESH_SECONDS": "1"}); assert False
    except ValueError: pass
    assert m.WEB_PAGE.count('fetchedAt=okAt=Date.now();failMsg=""') == 1 and "pushReload=setTimeout(load,1000)" in m.WEB_PAGE, "a stream patch is not a full refresh"
    print("OPEN_QUIET_OK")


asyncio.run(open_quiet())


async def taker_hold():
    """1.93.0: SIM_TAKER_HOLD_SECONDS — a taker buys only once its side has been buyable at every look for that long.
    Binance's trades are pushed as they print and Predict's book lags them: a wick lifts the fair price for a second,
    the book looks cheap against it, and without this the bot took it at once (the decision of the very second is sent
    as made). A run of looks is broken by a look that finds the side not buyable, or cannot judge the market."""
    env = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
           "SIM_WAYS": "both", "SIM_MARKETS": "all", "SIM_TAKER_HOLD_SECONDS": "3", **LEGACY}
    hbot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(NOW), None)
    hbot.sim_markets = lambda now: world["markets"]
    assert hbot.config.sim_taker_hold == 3 and m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"}).sim_taker_hold == 0  # off by default
    for bad in ({"SIM_TAKER_HOLD_SECONDS": "-1"}, {"SIM_TAKER_HOLD_SECONDS": "601"}):
        try: m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", **bad}); assert False, bad
        except ValueError: pass
    taker, maker = f"{HSI_SLUG}|up|吃", f"{HSI_SLUG}|up|挂"
    good = lambda at: hsi(0.70, [("0.55", "300")], [("0.58", "400")], at)  # 58¢ ask against 70¢ fair: buyable
    # the first look: buyable, but seen so for 0 s — the maker is placed as ever, the taker waits
    await step(hbot, NOW, good(NOW))
    assert sorted(hbot.sim_trades()) == [maker] and hbot.sim_taker_seen == {taker: NOW}, sorted(hbot.sim_trades())
    gap = hbot.sim_taker_gap(good(NOW), hbot.sim_trades(), NOW)
    assert gap["state"] == "blocked" and gap["text"] == "吃涨可买 100 份，但净优势要连续保持 3 秒才吃，还差 3 秒（SIM_TAKER_HOLD_SECONDS）", gap
    blocks = hbot.sim_blocks()
    assert len(blocks) == 1 and blocks[0]["id"] == taker and blocks[0]["why"] == "净优势要连续保持 3 秒才吃（SIM_TAKER_HOLD_SECONDS）", blocks
    # 2 s on, still buyable: not yet (the run started at NOW); the one block is not noted again
    await step(hbot, NOW + 2_000, good(NOW + 2_000))
    assert taker not in hbot.sim_trades() and hbot.sim_taker_seen == {taker: NOW} and len(hbot.sim_blocks()) == 1
    assert "还差 1 秒" in hbot.sim_taker_gap(good(NOW + 2_000), hbot.sim_trades(), NOW + 2_000)["text"]
    # the wick: at 2.5 s the fair price is back at 60¢ (58¢ + fee leaves under 10¢): the run ends and starts over
    await step(hbot, NOW + 2_500, hsi(0.60, [("0.55", "300")], [("0.58", "400")], NOW + 2_500))
    assert taker not in hbot.sim_trades() and hbot.sim_taker_seen == {}
    await step(hbot, NOW + 4_000, good(NOW + 4_000))
    assert taker not in hbot.sim_trades() and hbot.sim_taker_seen == {taker: NOW + 4_000}, "a new run"
    # a look that cannot judge the market (the card holds) ends the run too
    await step(hbot, NOW + 5_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 5_000, hold="行情过期"))
    assert hbot.sim_taker_seen == {}
    # the express lane counts as a look: a run of 3 s across the lane and the step buys
    await step(hbot, NOW + 6_000, good(NOW + 6_000))
    hbot.market.now = NOW + 7_500
    await hbot.sim_express("HSI")
    assert taker not in hbot.sim_trades() and hbot.sim_taker_seen == {taker: NOW + 6_000}
    hbot.market.now = NOW + 9_000
    await hbot.sim_express("HSI")
    t = hbot.sim_trades()[taker]
    assert t["status"] == "filled" and t["shares"] == 100 and t["opened"] == NOW + 9_000 and t["version"]["sim_taker_hold"] == 3, t
    assert hbot.sim_taker_gap(good(NOW + 9_000), hbot.sim_trades(), NOW + 9_000)["sides"][0]["block"] == "已有持仓（每边只吃一次）", "one buy per side under LEGACY"
    # the setting off: the first look buys, as before 1.93.0
    obot = m.Bot(m.Config.from_env({**env, "SIM_TAKER_HOLD_SECONDS": "0"}), m.Store(":memory:"), FM(NOW), None)
    obot.sim_markets = lambda now: world["markets"]
    await step(obot, NOW, good(NOW))
    assert sorted(obot.sim_trades()) == [taker, maker] and obot.sim_taker_hold_left(good(NOW), "up", NOW) == 0.0
    assert "sim_taker_hold" not in obot.sim_trades()[taker]["version"]
    print("TAKER_HOLD_OK")


asyncio.run(taker_hold())


async def after_hours_markets():
    """1.76.0: SIM_TAKER_AFTER_HOURS_MARKETS — after hours only the chosen daily cards are taken, whatever SIM_TAKER_SESSION
    says (10-16: 希音 out of the after-hours trading); the after-hours taker mode and its bar apply on them; in session every
    market as before; empty = all, as before."""
    env = {"TELEGRAM_BOT_TOKEN": "1:x", "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
           "SIM_WAYS": "both", "SIM_MARKETS": "all", **LEGACY, "SIM_TAKER_AFTER_HOURS_MARKETS": "hsi, kospi"}
    shein_slug = "shein-up-or-down-on-october-5-2026"
    shein = lambda at, session=False: m.dataclasses.replace(hsi(0.80, [("0.50", "300")], [("0.55", "100")], at), market=shein_slug, item="希音", key="HK0625USDT",
                                                              book=book([("0.50", "300")], [("0.55", "100")], at, slug=shein_slug, key="HK0625USDT", mid="202"), in_session=session)
    index = lambda at, session=False: m.dataclasses.replace(hsi(0.80, [("0.50", "300")], [("0.55", "100")], at), in_session=session)
    tbot = m.Bot(m.Config.from_env(env), m.Store(":memory:"), FM(NOW), None)  # SIM_TAKER_SESSION off: takers all day, after hours on the chosen markets only
    tbot.sim_markets = lambda now: world["markets"]
    assert tbot.config.sim_taker_after_hours_markets == {"HSI", "KOSPI"} and tbot.control_value("SIM_TAKER_AFTER_HOURS_MARKETS") == "HSI,KOSPI"
    assert tbot.sim_version()["sim_taker_after_hours_markets"] == ["HSI", "KOSPI"]
    closed_text = "标的未开盘，盘后吃单未选这个市场，不吃单"
    assert tbot.sim_taker_closed(shein(NOW)) == closed_text and tbot.sim_taker_closed(shein(NOW, True)) == "" and tbot.sim_taker_closed(index(NOW)) == ""
    gap = tbot.sim_taker_gap(shein(NOW), {}, NOW)
    assert gap["state"] == "wait" and gap["short"] == closed_text and gap["text"] == f"不吃单：{closed_text}", gap
    await step(tbot, NOW, shein(NOW), index(NOW))
    t = tbot.sim_trades()
    assert f"{HSI_SLUG}|up|吃" in t and f"{shein_slug}|up|吃" not in t and f"{shein_slug}|up|挂" in t, sorted(t)  # the index is taken, 希音 only quoted on
    await step(tbot, NOW + 5_000, shein(NOW + 5_000, True))
    assert f"{shein_slug}|up|吃" in tbot.sim_trades(), sorted(tbot.sim_trades())  # in session as before
    assert "开盘前、收盘后只吃 HSI、KOSPI 的单，其余指数/个股日涨跌盘后不吃（SIM_TAKER_AFTER_HOURS_MARKETS）" in tbot.sim_text(), tbot.sim_text()
    # with SIM_TAKER_SESSION on and the after-hours taker mode, the mode (and its bar) applies on the chosen markets only
    mbot = m.Bot(m.Config.from_env({**env, "SIM_TAKER_SESSION": "on", "SIM_TAKER_AFTER_HOURS_CENTS": "15"}), m.Store(":memory:"), FM(NOW), None)
    assert mbot.sim_taker_after_hours(index(NOW)) and not mbot.sim_taker_after_hours(shein(NOW))
    assert mbot.sim_taker_bar(index(NOW)) == 0.15 and mbot.sim_taker_bar(shein(NOW)) == 0.10 and mbot.sim_taker_bar(shein(NOW, True)) == 0.10
    assert mbot.sim_taker_closed(shein(NOW)) == closed_text and mbot.sim_taker_closed(index(NOW)) == ""
    assert "只吃净优势 ≥ 15.0¢ 的（SIM_TAKER_AFTER_HOURS_CENTS），只做 HSI、KOSPI" in mbot.sim_text(), mbot.sim_text()
    # SIM_TAKER_SESSION on without the mode: nothing is taken after hours, chosen or not
    sbot = m.Bot(m.Config.from_env({**env, "SIM_TAKER_SESSION": "on"}), m.Store(":memory:"), FM(NOW), None)
    assert sbot.sim_taker_closed(index(NOW)) == "标的未开盘，不吃单" and sbot.sim_taker_closed(shein(NOW)) == closed_text
    assert sbot.sim_taker_closed(m.dataclasses.replace(shein(NOW), lunch=True)) == "标的午休，盘后吃单未选这个市场，不吃单"
    # empty = every market, as before
    abot = m.Bot(m.Config.from_env({**env, "SIM_TAKER_AFTER_HOURS_MARKETS": ""}), m.Store(":memory:"), FM(NOW), None)
    assert abot.sim_taker_closed(shein(NOW)) == "" and abot.control_value("SIM_TAKER_AFTER_HOURS_MARKETS") == "" and "sim_taker_after_hours_markets" not in abot.sim_version()
    print("AFTER_HOURS_MARKETS_OK")


asyncio.run(after_hours_markets())


# 1.77.0: a record moved to another key (sim_rekey) is deleted and written in one transaction: a failure in between leaves
# the old record in place, never nothing
class Flaky:
    def __init__(self, conn): self.conn = conn
    def __enter__(self): return self.conn.__enter__()
    def __exit__(self, *a): return self.conn.__exit__(*a)
    def executemany(self, sql, rows):
        if "INSERT" in sql: raise RuntimeError("disk full")
        return self.conn.executemany(sql, rows)
    def __getattr__(self, name): return getattr(self.conn, name)


fstore = m.Store(":memory:")
fstore.put("sim:a", {"x": 1})
fstore.conn = Flaky(fstore.conn)
try:
    fstore.replace(["sim:a"], [("sim:a#1", {"x": 1})]); assert False, "the write failed"
except RuntimeError:
    pass
assert fstore.get("sim:a") == {"x": 1} and fstore.get("sim:a#1") is None, "rolled back as one"
fstore.conn = fstore.conn.conn
fstore.replace(["sim:a"], [("sim:a#1", {"x": 2})])
assert fstore.get("sim:a") is None and fstore.get("sim:a#1") == {"x": 2} and fstore.touched["sim"] >= 3
print("REPLACE_OK")
