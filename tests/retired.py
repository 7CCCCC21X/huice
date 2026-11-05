"""Markets main.py no longer lists because their windows are over (taken out in 1.95.0; STRC 触及 $100 in 1.112.3) or
they were decided early (BNB 先触 700 / 900, 1.101.1), kept as they were listed.

They are the suites' fixtures: the market-cap ladders, the flip and the monthly up/down have no market open now, and the
suites test those kinds of cards (and a 先触 window the rules date, and the October price ladders) through these lines.
``install()`` puts main's tables back as they stood in 1.94.1; ``LIVE`` keeps the tables main itself lists, for the
checks on what the bot runs with. Each line is also an example of its kind for the next market to add. The two touch
markets are main's own lines in TOUCH_SETTLED (kept there to read Predict's results of the trades bought on them)."""
import contextlib

import main as m

D, et_ms = m.D, m.et_ms

LIVE = {name: getattr(m, name) for name in ("TOUCH_MARKETS", "UPDOWN_MARKETS", "DAILY_UPDOWN_MARKETS", "WINDOW_UPDOWN_MARKETS",
                                            "FLIP_MARKETS", "CAP_MARKETS", "RANGE_MARKETS", "STOCK_HIT_MARKETS")}

# "between August 25th, 2026 at 10:00 AM ET and October 25th, 2026 at 11:59 PM ET" (both EDT); and BNB, which fell to $700
# first (11-04), its 12-31 deadline still ahead
BTC_TOUCH, BNB_TOUCH = m.touch_spec("BTC"), m.touch_spec("BNB")
assert BTC_TOUCH in m.TOUCH_SETTLED and BNB_TOUCH in m.TOUCH_SETTLED and BTC_TOUCH.fixed_start

UPDOWN_MARKETS = (
    # "the Binance 1 minute candle for BTC/USDT Sep 30 '26 11:59 PM in the ET timezone" against the one for
    # "Oct 31 '26 11:59 PM ET" (both EDT: daylight time ends on Nov 1)
    m.UpDownSpec("BTC-2026-10", "btc-up-down-october-2026", "BTCUSDT", "BTC 10月涨跌",
                 et_ms(2026, 9, 30, 23, 59, -4), et_ms(2026, 10, 31, 23, 59, -4)),
    # the same two minutes on ETH/USDT ("ETH closed September at $2,696.07 on Binance": the starting candle's close)
    m.UpDownSpec("ETH-2026-10", "eth-up-down-october-2026", "ETHUSDT", "ETH 10月涨跌",
                 et_ms(2026, 9, 30, 23, 59, -4), et_ms(2026, 10, 31, 23, 59, -4)),
)

FLIP_MARKETS = (
    # "at any point between October 2, 2026, 04:00 AM ET and October 31, 2026, 11:59 PM ET" (both EDT): the HYPE/USDC
    # and SOL/USDC 1-minute closes with the same timestamp on Hyperliquid
    m.FlipSpec("HYPE-SOL", "will-hype-flip-sol-by-nov-26", "HYPE 反超 SOL", "HYPE", "SOL",
               et_ms(2026, 10, 2, 4, 0, -4), et_ms(2026, 10, 31, 23, 59, -4)),
)

CAP_MARKETS = (
    # "from 11:30 PM ET on August 16, 2026 to 11:59 PM ET on October 31, 2026" (both EDT); settles on Flap.sh
    m.CapSpec("NIULAI", "what-marketcap-will-niu-lai-hit-before-nov-2026", "$牛来 市值", "0xbeea1d618e533a387d941f58a7d4c9b7bd377777",
              et_ms(2026, 8, 16, 23, 30, -4), et_ms(2026, 10, 31, 23, 59, -4),
              (D("2e8"), D("3e8"), D("5e8"), D("1e9")), "Predict 交易至北京 11-01 07:59"),
    # "between market creation on August 17, 13:00 PM ET to 11:59 PM ET on October 31, 2026"; FDV = price × total supply
    m.CapSpec("ANSEM", "what-fdv-will-ansem-hit-before-nov-2026", "$ANSEM FDV", "9cRCn9rGT8V2imeM2BaKs13yhMEais3ruM3rPvTGpump",
              et_ms(2026, 8, 17, 13, 0, -4), et_ms(2026, 10, 31, 23, 59, -4),
              tuple(D(f"{n}e8") for n in (5, 6, 7, 8, 9)) + (D("1e9"),),
              chain="solana", supply="fdv", gecko="solana", metric="FDV", settle="pump.fun"),
    # "between market creation on August 31, 2026 at 06:00 AM ET to October 31, 2026 at 11:59 PM ET"; the rules' own
    # source is this DexScreener pair; FDV = (total supply − burned) × price
    m.CapSpec("PONS", "what-fdv-will-pons-hit-before-nov-2026", "$PONS FDV", "0x39dBED3a2bd333467115dE45665cC57F813C4571",
              et_ms(2026, 8, 31, 6, 0, -4), et_ms(2026, 10, 31, 23, 59, -4),
              (D("7e8"), D("8e8"), D("9e8"), D("1e9")),
              chain="robinhood", pair="0x10cc6bd38112cac182db90b6a71d8bb5939526ba", supply="fdv", gecko="",
              metric="FDV", settle="DexScreener"),
    # three more Robinhood-chain FDV ladders settled on the rules' own DexScreener pairs (FDV = (total − burned) × price);
    # their levels are not listed here: the card shows the ones Predict's market titles carry
    # "between market creation on September 4 at 6:00 AM ET, 2026 to October 31, 2026 at 11:59 PM ET" (MEME/USDG)
    m.CapSpec("MEME", "what-fdv-will-meme-hit-before-november-2026", "$MEME FDV", "0x385F4f8ae47651ce5F58F5265395a669f8281e18",
              et_ms(2026, 9, 4, 6, 0, -4), et_ms(2026, 10, 31, 23, 59, -4), (),
              chain="robinhood", pair="0xc6e298e137f2905398db87e6eae49ede64d231fee37330fa433fec917f4618b6", supply="fdv", gecko="",
              metric="FDV", settle="DexScreener"),
    # "between market creation on September 2, 2026 at 01:00 AM ET to October 31, 2026 at 11:59 PM ET" (CASHCAT/WETH)
    m.CapSpec("CASHCAT", "what-fdv-will-cashcat-hit-before-november-2026", "$CASHCAT FDV", "0x020bfC650A365f8BB26819deAAbF3E21291018b4",
              et_ms(2026, 9, 2, 1, 0, -4), et_ms(2026, 10, 31, 23, 59, -4), (),
              chain="robinhood", pair="0xa70fc67c9f69da90b63a0e4c05d229954574e313", supply="fdv", gecko="", metric="FDV", settle="DexScreener"),
    # "between market creation on September 1, 2026 at 5:00 AM ET to October 31, 2026 at 11:59 PM ET" (the AI pair)
    m.CapSpec("AI", "what-fdv-will-ai-hit-before-nov-2026", "$AI FDV", "0x2E8c31162b855A2ffa90F6F8634643Ad6F111e18",
              et_ms(2026, 9, 1, 5, 0, -4), et_ms(2026, 10, 31, 23, 59, -4), (),
              chain="robinhood", pair="0xcbdfea90430a30ee4469c9902e120a77e7c7e4711d5643671c1d1957f2f1ce27", supply="fdv", gecko="",
              metric="FDV", settle="DexScreener"),
    # "between market creation on September 6 at 04:00 AM ET, 2026 to October 31, 2026 at 11:59 PM ET" (STONK/SOL on Solana);
    # the rules' own DexScreener pair settles it, FDV = (total − burned) × price; the pair is spelt as the rules' link spells it
    # (DexScreener reads it either way), which GeckoTerminal would not accept as a pool path; the hourly bars (σ, the window's
    # high since 09-06) come from the mint's pool whose address matches it without regard to case, else its most liquid pool
    m.CapSpec("STONK", "what-fdv-will-stonk-hit-before-november-2026", "$STONK FDV", "6GmAFSYs4gk3FDao5FzzySQpPZaWsa4rUJHacpMpUNgx",
              et_ms(2026, 9, 6, 4, 0, -4), et_ms(2026, 10, 31, 23, 59, -4), (),
              chain="solana", pair="afrddtgywcveqb1gxcahr8i48o6qtxyqksdvkeludehg", supply="fdv", gecko="solana", metric="FDV", settle="DexScreener"),
    # "between market creation on September 1, 2026 at 3:45 AM ET to October 31, 2026 at 11:59 PM ET" (STONKBROKER/WETH)
    m.CapSpec("STONKBROKER", "what-fdv-will-stonkbroker-hit-by-november-2026", "$STONKBROKER FDV", "0xe934e36A439C94017B64a3FecE66AF12099aBF50",
              et_ms(2026, 9, 1, 3, 45, -4), et_ms(2026, 10, 31, 23, 59, -4), (),
              chain="robinhood", pair="0xd33c8fd38b06e989cdbd4dffdefab71c4bdd415b24964c8d69e38ff35b068f92", supply="fdv", gecko="",
              metric="FDV", settle="DexScreener"),
)

RANGE_OCTOBER = (
    # "from 00:00 AM ET on the first day to 11:59 PM ET on the last" (October: EDT throughout); Binance 1m candles
    m.RangeSpec("BTC-HIT-10", "what-price-will-bitcoin-hit-in-october-2026", "BTCUSDT", "spot", "BTC 10月价格",
                et_ms(2026, 10, 1, 0, 0, -4), et_ms(2026, 10, 31, 23, 59, -4), "down"),
    m.RangeSpec("ETH-HIT-10", "what-price-will-ethereum-hit-in-october-2026", "ETHUSDT", "spot", "ETH 10月价格",
                et_ms(2026, 10, 1, 0, 0, -4), et_ms(2026, 10, 31, 23, 59, -4)),
    m.RangeSpec("SOL-HIT-10", "what-price-will-solana-hit-in-october-2026", "SOLUSDT", "spot", "SOL 10月价格",
                et_ms(2026, 10, 1, 0, 0, -4), et_ms(2026, 10, 31, 23, 59, -4)),
    # HYPEUSDT on Binance futures (the rules link binance.com/en/futures/HYPEUSDT)
    m.RangeSpec("HYPE-HIT-10", "what-price-will-hyperliquid-hit-in-october-2026", "HYPEUSDT", "futures", "HYPE 10月价格",
                et_ms(2026, 10, 1, 0, 0, -4), et_ms(2026, 10, 31, 23, 59, -4)),
)

STOCK_HIT_MARKETS = (
    # "any TradingView 1 minute candle for STRC between market creation and the listed date, 11:59 PM ET, has a final
    # High of at least $100" (NASDAQ:STRC); over, taken out of main in 1.112.3. The window is read from Predict: the
    # market's createdAt and the date its title names (start_ms / end_ms = 0 here); LADDER_DEADLINES=STRC-100=2026-12-31
    # pins the deadline by hand.
    m.RangeSpec("STRC-100", "strc-hits-100-by-20260618001620693", "STRC", "nasdaq", "STRC 触及 $100", 0, 0),
)


def install() -> None:
    """main's tables as they stood in 1.94.1, in the same order: the markets still listed plus the retired ones (the
    suites unpack them by position). The November ladders are left out, as the October ones stand in their place, and
    so are the daily up/down cards (1.97.0; tests/test_daily.py tests them) and the 15-minute ones (1.102.0;
    tests/test_window.py). The STRC stock card (over since 1.112.3) is put back: tests/test_stockhit.py, test_tuning
    and test_risk read it."""
    touch = {s.key: s for s in LIVE["TOUCH_MARKETS"]}
    ranges = {s.key: s for s in LIVE["RANGE_MARKETS"]}
    m.TOUCH_MARKETS = (BNB_TOUCH, touch["SOL"], BTC_TOUCH, touch["ETH"])
    m.UPDOWN_MARKETS = UPDOWN_MARKETS
    m.DAILY_UPDOWN_MARKETS = ()
    m.WINDOW_UPDOWN_MARKETS = ()
    m.FLIP_MARKETS = FLIP_MARKETS
    m.CAP_MARKETS = CAP_MARKETS
    m.RANGE_MARKETS = (*RANGE_OCTOBER, ranges["BTC-HIT-2027"], ranges["ZEC-HIT-2027"])
    m.STOCK_HIT_MARKETS = STOCK_HIT_MARKETS


@contextlib.contextmanager
def live():
    """main's own tables for the length of a with block, the suite's fixtures back after it: a bot built and asked inside
    it runs as the deployed one does (the tables are read whenever a bot is built or lists its markets)."""
    saved = {name: getattr(m, name) for name in LIVE}
    for name, table in LIVE.items():
        setattr(m, name, table)
    try:
        yield
    finally:
        for name, table in saved.items():
            setattr(m, name, table)
