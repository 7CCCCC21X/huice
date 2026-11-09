"""1.145.3: a round-the-clock card's σ (黄金, 原油) around its market's shut. σ is the last four hours of the index's 1-minute
candles; while gold and oil are shut (Friday's close to Sunday evening, and an hour every day) Binance's index stands still,
and those zero returns made the hours after the reopen look calm — the first windows after the weekend priced on a fraction
of the market's σ. Now a still stretch (ten minutes or more at one price) is left out, and when the day back holds too few
live minutes (a weekend) the market's last live hours before the shut are read once and kept.

Run: python tests/test_sigma_flat.py
"""
import asyncio
import datetime as dt
import math
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

BJ = lambda d, h, mi: int(dt.datetime(2026, 11, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)  # 11-09 is a Monday
X = next(s for s in m.WINDOW_UPDOWN_MARKETS if s.key == "XAU-15M")
LIVE = math.log(1.0005) * math.sqrt(365 * 24 * 60)  # the market's σ: 0.05% a minute, up and down


def shut_from(o: int) -> int | None:
    """The first minute of the shut ``o`` falls in (Beijing): Saturday 06:00 to Monday 07:00, and 06:00–07:00 Tuesday to
    Friday (17:00–18:00 New York); None while the market trades."""
    t = dt.datetime.fromtimestamp(o / 1000, m.BEIJING)
    six = t.replace(hour=6, minute=0, second=0, microsecond=0)
    if (t.weekday() == 5 and t.hour >= 6) or t.weekday() == 6 or (t.weekday() == 0 and t.hour < 7):
        return int((six - dt.timedelta(days=(t.weekday() - 5) % 7)).timestamp() * 1000)
    if t.weekday() in (1, 2, 3, 4) and t.hour == 6:
        return int(six.timestamp() * 1000)
    return None


def close_at(o: int) -> float:
    """The index: up and down 0.05% a minute while the market trades, standing at its last price while it is shut."""
    start = shut_from(o)
    live = o if start is None else start - 60_000
    return 2000.0 * (1 + 0.0005 * (live // 60_000 % 2))


class Futures:
    def __init__(self, now): self.now, self.calls = now, []

    async def __call__(self, path, **params):
        assert path == "indexPriceKlines" and params["pair"] == "XAUUSDT", (path, params)
        self.calls.append(dict(params))
        if params["interval"] == "15m":
            return []
        end = (params["endTime"] if "endTime" in params else self.now) // 60_000 * 60_000
        return [[o, "0", "0", "0", str(close_at(o)), "0", o + 59_999, "0", 0, "0", "0", "0"]
                for o in range(end - (params["limit"] - 1) * 60_000, end + 1, 60_000)]

    def minutes(self):
        return [c["limit"] if "endTime" not in c else ("back", c["limit"]) for c in self.calls if c["interval"] == "1m"]


def card_at(now):
    fut = Futures(now)
    return m.IndexUpDownMarket(m.Store(":memory:"), X.current(now), fut, "", None), fut


async def old_sigma(now):
    """What 1.145.2 priced on: the last four hours, every minute counted."""
    rows = await Futures(now)("indexPriceKlines", pair="XAUUSDT", interval="1m", limit=242)
    return m.minute_vol(rows, now, 240)


async def run():
    # the still stretches: ten minutes or more at one price; a quiet minute or two in trading is no shut
    rows = lambda closes, start=BJ(11, 12, 0): [[start + i * 60_000, "0", "0", "0", str(c), "0", start + i * 60_000 + 59_999] for i, c in enumerate(closes)]
    assert m.flat_minutes(rows([1, 2, 2, 2, 3])) == set() and m.flat_minutes(rows([1, 2] + [2] * 10 + [3])) == {BJ(11, 12, 0) + i * 60_000 for i in range(2, 12)}
    assert m.flat_minutes(rows([5] * 12)) == {BJ(11, 12, 0) + i * 60_000 for i in range(1, 12)} and m.FLAT_RUN == 10
    gap = rows([1] * 6, BJ(11, 12, 0)) + rows([1] * 6, BJ(11, 12, 10))  # a missing minute breaks a stretch: the gap is left out by minute_returns anyway
    assert m.flat_minutes(gap) == set()

    # Wednesday noon, trading for hours: nothing still — the four hours as before, one read
    noon = BJ(11, 12, 0)
    card, fut = card_at(noon)
    await card.read_vol(noon)
    assert fut.minutes() == [242] and abs(card.sigma - LIVE) < 1e-9 and abs(card.sigma - await old_sigma(noon)) < 1e-12

    # Wednesday 07:20, twenty minutes after the daily hour's shut: the still hour left out (a day back read: the hour well behind it)
    morning = BJ(11, 7, 20)
    card, fut = card_at(morning)
    await card.read_vol(morning)
    before = await old_sigma(morning)
    assert fut.minutes() == [242, 1500] and abs(card.sigma - LIVE) < 1e-9 and before < 0.9 * LIVE, (fut.minutes(), card.sigma, before)

    # Monday 07:10, ten minutes after the weekend: the day back is all shut — Saturday morning's last live hours are read back, once
    monday = BJ(16, 7, 10)
    card, fut = card_at(monday)
    await card.read_vol(monday)
    before = await old_sigma(monday)
    assert abs(card.sigma - LIVE) < 1e-9 and before < 0.4 * LIVE, (card.sigma, before)  # 1.145.2 priced a third of it
    assert fut.minutes() == [242, 1500, ("back", 1500), ("back", 1500)], fut.minutes()  # Sunday, then Saturday back to Friday
    kept_at, kept = card.flat_back
    assert kept_at == BJ(15, 6, 11) and (int(kept[0][0]), int(kept[-1][0])) == (BJ(14, 1, 59), BJ(14, 5, 59)) and len(kept) == 241, (kept_at, kept[0][0], kept[-1][0])
    # a minute on, and the next window's card: kept, nothing read back again
    await card.read_vol(monday + 60_000)
    nxt = card.at(X.window_spec(BJ(16, 7, 15)))
    assert nxt.flat_back is card.flat_back
    await nxt.read_vol(monday + 120_000)
    assert fut.minutes()[4:] == [242, 1500, 242, 1500] and abs(nxt.sigma - LIVE) < 1e-9, fut.minutes()
    # forty minutes in: the open's own minutes are the floor, as after any session's start (the same 0.05% here)
    later = BJ(16, 7, 40)
    card40, fut40 = card_at(later)
    await card40.read_vol(later)
    assert abs(card40.sigma - LIVE) < 1e-9 and ("back", 1500) in fut40.minutes()

    # the next Monday: last week's hours are not taken for this week's — read back afresh
    week = BJ(23, 7, 10)
    card.market_now = week
    card.futures = fut_w = Futures(week)
    await card.read_vol(week)
    assert ("back", 1500) in fut_w.minutes() and card.flat_back[0] == BJ(22, 6, 11) and abs(card.sigma - LIVE) < 1e-9, fut_w.minutes()

    # a session card is untouched: its own minutes (the session), not the still ones
    k = next(s for s in m.WINDOW_UPDOWN_MARKETS if s.key == "SKHYNIX-15M")
    assert m.IndexUpDownMarket(m.Store(":memory:"), k.current(noon), Futures(noon), "kr", m.session_minute_fn("kr")).flat_back is None
    print("SIGMA_FLAT_OK")


asyncio.run(run())
