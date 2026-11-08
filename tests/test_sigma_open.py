"""1.145.1: a session card's σ at a Monday open (and the day after a holiday). σ is read from the session's own minutes of the
last 1,500 one-minute index candles — a day back, yesterday's session in it; on a Monday that day back is the weekend, so
until 1.145.1 there was no σ until 30 minutes of Monday's session were in (指数 1 分钟 K 线不足): each card's first two windows
of the week unpriced. Now the sessions before are read once and kept, and the open is priced as any other day's.

Run: python tests/test_sigma_open.py
"""
import asyncio
import datetime as dt
import math
import sys

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

BJ = lambda d, h, mi: int(dt.datetime(2026, 11, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
K = next(s for s in m.WINDOW_UPDOWN_MARKETS if s.key == "SKHYNIX-15M")   # KRX: 09:00–15:30 KST = 08:00–14:30 Beijing
KR = m.session_minute_fn("kr")
MONDAY = dt.date(2026, 11, 9)
PER_YEAR = math.sqrt(365 * 24 * 60)


def close_at(open_ms: int, trading=KR) -> float:
    """The index: still outside the KRX session; inside it up and down every minute — 0.1% on Friday, 0.3% on Monday."""
    if not trading(open_ms):
        return 100.0
    amp = 0.003 if dt.datetime.fromtimestamp(open_ms / 1000, m.BEIJING).date() == MONDAY else 0.001
    return 100.0 * (1 + amp * (open_ms // 60_000 % 2))


class Futures:
    """Binance's indexPriceKlines: ``limit`` candles ending at endTime, or at now (the current one still open); ``short``: an
    index listed lately — a page shorter than asked."""
    def __init__(self, now, trading=KR, short=0):
        self.now, self.trading, self.short, self.calls = now, trading, short, []

    async def __call__(self, path, **params):
        assert path == "indexPriceKlines" and params["pair"] == "SKHYNIXUSDT", (path, params)
        self.calls.append(dict(params))
        if params["interval"] == "15m":
            return []  # no shape of the day: the plain σ
        end = (params["endTime"] if "endTime" in params else self.now) // 60_000 * 60_000
        limit = min(params["limit"], self.short or params["limit"])
        return [[o, "0", "0", "0", str(close_at(o, self.trading)), "0", o + 59_999, "0", 0, "0", "0", "0"]
                for o in range(end - (limit - 1) * 60_000, end + 1, 60_000)]

    def back(self):
        return [c for c in self.calls if c["interval"] == "1m" and "endTime" in c]


def card_at(now, trading=KR, short=0):
    fut = Futures(now, trading, short)
    return m.IndexUpDownMarket(m.Store(":memory:"), K.current(now), fut, "kr", trading), fut


async def run():
    friday, monday = math.log(1.001) * PER_YEAR, math.log(1.003) * PER_YEAR
    # Monday 09:05 KST (08:05 Beijing): five minutes of the session in — the 1,500 candles back reach Sunday morning, the weekend alone
    now = BJ(9, 8, 5)
    card, fut = card_at(now)
    recent = await fut("indexPriceKlines", pair="SKHYNIXUSDT", interval="1m", limit=1500)
    fut.calls.clear()
    try:  # what 1.145.0 did: the day back alone
        m.minute_vol(recent, now, 240, KR)
        assert False, "the weekend holds no session minute"
    except ValueError as error:
        assert str(error) == "指数 1 分钟 K 线不足"
    await card.read_vol(now)
    assert card.sigma is not None and friday < card.sigma < monday, (card.sigma, friday, monday)  # Friday's last four hours, Monday's minutes weighing most
    back = fut.back()
    assert len(back) == 1 and back[0]["endTime"] == BJ(6, 14, 29) + 59_999 and back[0]["limit"] == 241, back  # Friday's last 241 session minutes, once
    assert card.sigma_back[0] == (BJ(6, 10, 29), BJ(6, 14, 29)) and len(card.sigma_back[1]) == 241
    # a minute on, and the next window's card: the earlier sessions kept, not read again
    await card.read_vol(now + 60_000)
    nxt = card.at(K.window_spec(BJ(9, 8, 15)))
    assert nxt.sigma_back is card.sigma_back
    await nxt.read_vol(now + 120_000)
    assert len(fut.back()) == 1 and nxt.sigma is not None
    # 25 minutes in: the open's own minutes are the floor (as on any day's open from its tenth minute) — Monday's 0.3% a minute
    later = BJ(9, 8, 25)
    card25, fut25 = card_at(later)
    await card25.read_vol(later)
    assert abs(card25.sigma - monday) < 1e-9 and len(fut25.back()) == 1, (card25.sigma, monday)
    # Tuesday: the day back holds Monday's whole session — nothing more is read
    tue = BJ(10, 8, 5)
    card_t, fut_t = card_at(tue)
    await card_t.read_vol(tue)
    assert not fut_t.back() and card_t.sigma is not None and friday < card_t.sigma < monday * 1.0001
    # Monday a KRX holiday: Tuesday's open reads back over it to Friday
    hol = m.session_minute_fn("kr", frozenset({MONDAY}))
    card_h, fut_h = card_at(tue, hol)
    await card_h.read_vol(tue)
    assert len(fut_h.back()) == 1 and fut_h.back()[0]["endTime"] == BJ(6, 14, 29) + 59_999 and card_h.sigma is not None, fut_h.back()
    # an index listed lately (a page shorter than asked): nothing older to read — as before, no σ until 30 minutes are in
    card_s, fut_s = card_at(now, short=600)
    try:
        await card_s.read_vol(now)
        assert False
    except ValueError as error:
        assert str(error) == "指数 1 分钟 K 线不足" and not fut_s.back()
    # round-the-clock cards (黄金, 原油) are untouched: the last four hours, no session, nothing read back
    xau_spec = next(s for s in m.WINDOW_UPDOWN_MARKETS if s.key == "XAU-15M")
    seen = []

    async def xau_futures(path, **params):
        seen.append(dict(params))
        if params["interval"] == "15m":
            return []
        end = now // 60_000 * 60_000
        return [[o, "0", "0", "0", str(2000 * (1 + 0.0005 * (o // 60_000 % 2))), "0", o + 59_999, "0", 0, "0", "0", "0"]
                for o in range(end - (params["limit"] - 1) * 60_000, end + 1, 60_000)]
    xau = m.IndexUpDownMarket(m.Store(":memory:"), xau_spec.current(now), xau_futures, "", None)
    await xau.read_vol(now)
    assert [c["limit"] for c in seen if c["interval"] == "1m"] == [242] and not any("endTime" in c for c in seen) and xau.sigma > 0
    print("SIGMA_OPEN_OK")


asyncio.run(run())
