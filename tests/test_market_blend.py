"""1.154.79 — the owner, on 上证 (两条都改):

- SSE_SIGMA_EXTRA back to 1 (1.154.69's 1.5 withdrawn): replayed on the real fills it could only show what it would have stopped
  buying, never what it would buy instead — on 11-27 the index was up 1.9% and the model still gave 跌 11%: 1¢ floor orders
  filled 24 times in 8 minutes. A value saved on the control page while 1.5 was the default is dropped once at the start.
- SIM_MARKET_BLEND (default SSE:0.5): the trader judges a card's sides by its fair price taken that far toward the market's own
  (market_view: the book's middle; one-sided, that side's own bid, else 0 — never a reason to buy more). Over 33 days of 上证 the
  market had been the better judge (Brier 0.200 against the model's 0.215), the model bearish on 32 of them (−$491).

Here: the setting and its words; market_view; sim_fair with the blend and the cap; a step: the 11-27 floor order and a taker the
blend refuses, one it still takes and what its record keeps; the review's EV at the judged fair; the start's clean-up; /sim.

Run: python tests/test_market_blend.py
"""
import asyncio
import datetime as dt
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

D = m.D
BJ = lambda mo, d, h, mi=0: int(dt.datetime(2026, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
NOW = BJ(11, 27, 10, 40)  # a Friday, the morning session
SLUG = "sse-composite-index-up-or-down-on-november-27-2026"


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


def book(bids, asks, at=NOW):
    return m.PredictBook("SSE", SLUG, "4827036", "t", tuple((D(p), D(q)) for p, q in bids), tuple((D(p), D(q)) for p, q in asks), at, None)


def sse(fair_up, bids, asks, at=NOW):
    return m.SimMarket(SLUG, "上证指数", "close", "SSE", fair_up, book(bids, asks, at), 0.03, "", ("涨", "跌"),
                       {"key": "SSE", "target": "2026-11-27", "line": 3476.88, "close_ms": BJ(11, 27, 15, 0)}, in_session=True)


def make(**env):
    cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "SIM_MARKETS": "close",
                             "SIM_EDGE_CENTS": "10", "SIM_MAKER_FOLLOW_SETTLE_SECONDS": "0", "SIM_GROUP_USD": "0", **env})
    bot = m.Bot(cfg, m.Store(":memory:"), FM(NOW), None)
    world = {"markets": []}
    bot.sim_markets = lambda now: world["markets"]

    async def details(mid):
        return {"outcomes": ["Up", "Down"], "status": "REGISTERED", "resolved": None}
    bot.predict.market_details = details

    async def step(at, *markets):
        bot.market.now, bot.sim_ran = at, -1e9
        world["markets"] = list(markets)
        bot.sim_checked.clear()
        await bot.sim_step(at)
    return bot, step


# --- the setting --------------------------------------------------------------------------------------------------------------------
cfg = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"})
assert cfg.sim_market_blend == (("SSE", 0.5),) and cfg.sse_sigma_extra == 1.0, "the owner's choice: 上证 halfway, no extra σ"
parse = m.parse_market_blend
assert parse("SSE:0.5") == (("SSE", 0.5),) and parse(" sse：0.5 ，hk0625usdt:0.3 ") == (("HK0625USDT", 0.3), ("SSE", 0.5))
assert parse("") == parse("none") == parse("OFF") == () and parse("SSE:0,HSI:1") == (("HSI", 1.0),), "a weight of 0 is no blend"
for bad in ("SSE", "SSE:2", "SSE:-0.1", "SSE:x", ":0.5", "SSE:nan", "上证:0.5"):
    try:
        parse(bad)
    except ValueError as error:
        assert "SIM_MARKET_BLEND" in str(error)
    else:
        raise AssertionError(f"SIM_MARKET_BLEND={bad} taken")
assert m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x", "SIM_MARKET_BLEND": "none"}).sim_market_blend == ()
assert m.market_blend_text((("HK0625USDT", 0.3), ("SSE", 0.5))) == "HK0625USDT:0.3,SSE:0.5"
assert "SIM_MARKET_BLEND" in m.CONTROL_KEY_SET and m.CONTROL_FORMS["SIM_MARKET_BLEND"]["kind"] == "text"
assert m.version_text("sim_market_blend", ()) == "都不靠（只看模型）" and m.version_text("sim_market_blend", ("SSE:0.5",)) == "SSE:0.5"

# --- market_view: the middle; one-sided, the side's own bid; nobody bidding, 0 -------------------------------------------------------
mv = m.market_view
assert abs(mv(book([("0.60", "10")], [("0.66", "10")]), "up") - 0.63) < 1e-12 and abs(mv(book([("0.60", "10")], [("0.66", "10")]), "down") - 0.37) < 1e-12
only_bid = book([("0.98", "2678")], [])  # 11-27 13:05: 涨 bid 98¢, nothing offered
assert mv(only_bid, "up") == 0.98 and mv(only_bid, "down") == 0.0, "nobody bids for 跌: worth nothing to buy, not 2¢"
only_ask = book([], [("0.55", "500")])
assert mv(only_ask, "up") == 0.0 and abs(mv(only_ask, "down") - 0.45) < 1e-12 and mv(book([], []), "up") == 0.0

# --- sim_fair: taken toward the market, then capped ---------------------------------------------------------------------------------
bot, _ = make()
mk = sse(0.30, [("0.60", "10")], [("0.66", "10")])
assert bot.sim_blend(mk) == 0.5
up, cut = bot.sim_fair(mk, "up")
assert abs(up - 0.465) < 1e-12 and cut == 0.0, up  # 30 + (63 − 30) / 2
down, _ = bot.sim_fair(mk, "down")
assert abs(down - 0.535) < 1e-12, down
sure = sse(0.01, [("0.02", "10")], [("0.04", "10")])  # 跌 99% by the model, 97% by the market: 98% → capped at 95
d, cut = bot.sim_fair(sure, "down")
assert d == 0.95 and abs(cut - 0.03) < 1e-12, (d, cut)
hsi = m.SimMarket("hsi", "恒生指数", "close", "HSI", 0.30, book([("0.60", "10")], [("0.66", "10")]), 0.03, "", ("涨", "跌"), {"key": "HSI"})
assert bot.sim_blend(hsi) == 0.0 and bot.sim_fair(hsi, "up") == (0.30, 0.0), "a card not listed: the model alone"
plain_bot, _ = make(SIM_MARKET_BLEND="none")
assert plain_bot.sim_fair(mk, "up") == (0.30, 0.0)
assert bot.sim_blend_note(mk, "up") == "涨按模型 30.0¢、市场 63.0¢ 往市场靠 50% 计 46.5¢（SIM_MARKET_BLEND）", bot.sim_blend_note(mk, "up")
assert bot.sim_blend_note(sure, "down") == "跌按模型 99.0¢、市场 97.0¢ 往市场靠 50% 计 98.0¢、按上限 95.0¢（SIM_MARKET_BLEND）", bot.sim_blend_note(sure, "down")
assert bot.sim_blend_note(hsi, "up") == "" and bot.sim_judged(hsi, "up") == {} and abs(bot.sim_judged(mk, "up")["judged"] - 0.465) < 1e-12


async def steps() -> None:
    # --- 11-27 13:05: the index up 1.9%, 涨 bid 98¢ and nothing offered; the model (1.5 gone or not) gives 跌 11.5%. A 1¢ floor
    # order is 10.5¢ under it — over the bar for the model alone; taken halfway toward a market that bids nothing for 跌: 5.75%
    at = BJ(11, 27, 13, 30)
    floor = dict(SIM_WAYS="both", SIM_MAKER_FLOOR_SHARES="100", SIM_MAKER_FLOOR_CENTS="1", SIM_MAKER_POINTS="off")
    alone, step = make(SIM_MARKET_BLEND="none", **floor)
    await step(at, sse(0.885, [("0.98", "2678")], [], at))
    placed = [t for t in alone.sim_trades().values() if t.get("floor")]
    assert len(placed) == 1 and placed[0]["side"] == "down" and placed[0]["price"] == 0.01, alone.sim_trades()
    blended, step = make(**floor)
    await step(at, sse(0.885, [("0.98", "2678")], [], at))
    assert not blended.sim_trades(), blended.sim_trades()
    # --- a taker the model alone takes and the blend refuses: 跌 60% by the model, 40% by the market's middle, offered at 42¢
    alone, step = make(SIM_MARKET_BLEND="none", SIM_WAYS="taker")
    await step(NOW, sse(0.40, [("0.58", "500")], [("0.62", "500")]))
    assert [t["side"] for t in alone.sim_trades().values()] == ["down"], alone.sim_trades()
    blended, step = make(SIM_WAYS="taker")
    await step(NOW, sse(0.40, [("0.58", "500")], [("0.62", "500")]))
    assert not blended.sim_trades(), "60% halfway to 40% is 50%: 8¢ under the bar"
    # --- one it still takes: 跌 80% by the model, 50% by the market, offered at 52¢ — judged at 65%, and the record says so
    took, step = make(SIM_WAYS="taker")
    await step(NOW, sse(0.20, [("0.48", "500")], [("0.52", "500")]))
    (t,) = took.sim_trades().values()
    entry = t["entry"]
    assert t["side"] == "down" and abs(t["fair"] - 0.80) < 1e-12, "the record's fair: the model's own, as ever"
    assert entry["blend"] == 0.5 and abs(entry["mkt"] - 0.50) < 1e-12 and abs(entry["fair_blend"] - 0.65) < 1e-12, entry
    assert abs(t["edge"] - (0.65 - t["price"])) < 1e-12 and 0.10 <= t["edge"] < 0.15, t["edge"]  # the edge as judged: 65% − the cost
    assert abs(m.judged_fair(t) - 0.65) < 1e-12 and abs(t["fills"][0]["judged"] - 0.65) < 1e-12
    assert abs(m.sim_fill_expectation(t) - t["edge"] * t["shares"]) < 1e-9, "at the fill as at the order: the judged fair"
    assert t["version"]["sim_market_blend"] == ["SSE:0.5"] and t["version"]["sse_sigma_extra"] == 1.0
    # the review's EV bet takes the same figure
    settled = {**t, "status": "settled", "payout": 1.0}
    (bet,) = m.sim_ev_bets([settled])
    assert abs(bet["ev"] - t["edge"] * t["shares"]) < 1e-9, bet
    # /sim says the trader leans on the market there
    assert "估值往市场价靠：上证指数 50%" in took.sim_text(), took.sim_text()
asyncio.run(steps())

# --- a record before 1.154.79 (no fair_blend): judged as before ---------------------------------------------------------------------
old = {"fair": 0.80, "entry": {"fair": 0.80}, "version": {"sim_max_fair": 0.95}}
assert m.judged_fair(old) == 0.80 and m.judged_fair({**old, "fair": 0.99}) == 0.95

# --- the start: SSE_SIGMA_EXTRA saved on the page (while 1.5 was the default) goes once; one saved after that stays -------------------
store = m.Store(":memory:")
store.put("control:env", {"SSE_SIGMA_EXTRA": "1.5", "SIM_EDGE_CENTS": "12"})
base = m.Config.from_env({"TELEGRAM_BOT_TOKEN": "1:x"})
config, dropped = m.Bot.control_config(base, store)
assert config.sse_sigma_extra == 1.0 and config.sim_edge == 0.12 and dropped == [], (config.sse_sigma_extra, dropped)
assert store.get("control:env") == {"SIM_EDGE_CENTS": "12"} and store.get("mig:sse_sigma_extra")
store.put("control:env", {"SSE_SIGMA_EXTRA": "1.25"})  # chosen again on the page, after the upgrade
config, _ = m.Bot.control_config(base, store)
assert config.sse_sigma_extra == 1.25 and store.get("control:env") == {"SSE_SIGMA_EXTRA": "1.25"}
fresh = m.Store(":memory:")
assert m.Bot.control_config(base, fresh) == (base, []) and fresh.get("mig:sse_sigma_extra"), "nothing saved: nothing to drop"

# --- the control page: saved, in force at once; a bad value refused ------------------------------------------------------------------
cbot, _ = make()
assert cbot.control_value("SIM_MARKET_BLEND") == "SSE:0.5" and cbot.control_value("SSE_SIGMA_EXTRA") == "1"
answer = cbot.control_set({"SIM_MARKET_BLEND": "SSE:0.7,HK0625USDT:0.4"})
assert answer["ok"] and cbot.config.sim_market_blend == (("HK0625USDT", 0.4), ("SSE", 0.7)), answer
assert cbot.control_value("SIM_MARKET_BLEND") == "HK0625USDT:0.4,SSE:0.7"
assert not cbot.control_set({"SIM_MARKET_BLEND": "SSE:3"})["ok"] and cbot.config.sim_market_blend == (("HK0625USDT", 0.4), ("SSE", 0.7))
assert cbot.control_set({"SIM_MARKET_BLEND": "none"})["ok"] and cbot.config.sim_market_blend == () and cbot.control_value("SIM_MARKET_BLEND") == "none"
assert cbot.control_set({"SIM_MARKET_BLEND": ""})["ok"] and cbot.config.sim_market_blend == (("SSE", 0.5),), "cleared: the default again"
print("MARKET_BLEND_OK")
