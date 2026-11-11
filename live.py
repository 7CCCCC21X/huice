#!/usr/bin/env python3
"""Real trading on Predict.fun for the paper trader's decisions (LIVE=on).

The paper trader in main.py (/sim) decides *what* to buy: whenever a card's suggestion clears SIM_EDGE_CENTS it buys
SIM_SHARES of that side, takers across the book, makers resting at the suggested price. This module keeps every one of
those decisions and sends it to Predict for real:

- a taker becomes a LIMIT order for SIM_SHARES capped at the dearest acceptable price (main.taker_cap: the price that
  still leaves the required edge after the fee): what the book holds at or under it fills at once, the rest waits
  LIVE_TAKER_WAIT_SECONDS and is then cancelled; no MARKET orders;
- a maker becomes a LIMIT order resting at the suggested price; it is cancelled when the paper trader withdraws it
  (SIM_WAYS / SIM_MARKETS narrowed) and when the market's result is in;
- fills are read back from Predict (GET /v1/orders), never presumed from the book; settlement and the journal are the
  paper trader's (local pre-settlement, then Predict's result), on the shares really filled;
- before any order: the market's outcome token, fee rate and exchange (standard / multi-outcome, yield-bearing) are read
  from GET /v1/markets/{id}, the order is signed (EIP-712, "predict.fun CTF Exchange" v1 on BNB chain), and the hash is
  saved before the request goes out, so a crash can never place the same order twice (a record left in "placing" is
  reconciled by its hash on restart);
- hard limits sit above the strategy: LIVE_MAX_ORDER_USD per order, LIVE_MAX_OPEN_USD open exposure (filled positions
  plus resting orders), LIVE_MAX_DAILY_LOSS_USD realised loss per Beijing day (trips a kill switch until the next day),
  the USDT balance, the exchange allowance, and a manual pause (/live pause). A decision the limits refuse is recorded
  the way the paper trader records a group-cap refusal (the journal's "拦下的买入"), never traded on paper instead.

Wallets: a plain wallet (PREDICT_PRIVATE_KEY) or a Predict account (the smart wallet behind the website: the Privy
wallet's exported key as PREDICT_PRIVATE_KEY and the deposit address as PREDICT_ACCOUNT; orders are signed through the
Kernel wrapper the official SDKs use). On-chain actions (USDT allowance, outcome-token approvals, redeeming resolved
positions) are sent as BNB-chain transactions from the signing wallet (it needs a little BNB for gas), through
Kernel.execute for a Predict account.

Requires the ``eth-account`` package (pip install eth-account); nothing else beyond the standard library.
"""
from __future__ import annotations

import asyncio
import base64
import collections
import contextlib
import contextvars
import dataclasses
import datetime as dt
import decimal
import json
import math
import random
import re
import time
from decimal import Decimal
from typing import Any

import main as core

LOG = core.LOG
D = Decimal
WEI = 10 ** 18
MIN_ORDER_USD = 1.0  # Predict refuses an order "with a value of" under 0.9 USD; a round dollar keeps clear of its rounding
MAX_SALT = 2_147_483_648
ZERO_ADDRESS = "0x" + "0" * 40
ZERO_HASH = "0x" + "0" * 64
PROTOCOL_NAME, PROTOCOL_VERSION = "predict.fun CTF Exchange", "1"
KERNEL_NAME, KERNEL_VERSION = "Kernel", "0.3.1"
FAR_FUTURE = 4102444800  # 2100-01-01: a LIMIT order without an expiry, as the SDKs build it
TAKER_ORDER_SECONDS = 300  # a taker's LIMIT order expires five minutes after it is built (its rest is cancelled sooner)...
ORDER_END_GRACE_SECONDS = 60  # ...and no later than this long after its market's end, as a maker's does (order_expiry, 1.150.0)...
ORDER_MIN_SECONDS = 120       # ...or this long after the order is decided / built, should that be later: none is signed for less
API_TIMEOUT = 12
RPC_TIMEOUT = 15
MAX_UINT256 = 2 ** 256 - 1
UP_WORDS = {"yes", "up", "涨", "上涨", "higher", "above"}
DOWN_WORDS = {"no", "down", "跌", "下跌", "lower", "below"}

# Contract addresses per chain, as published in the official SDKs (@predictdotfun/sdk, predict-sdk).
ADDRESSES: dict[int, dict[str, str]] = {
    56: {"CTF_EXCHANGE": "0x8BC070BEdAB741406F4B1Eb65A72bee27894B689",
         "NEG_RISK_CTF_EXCHANGE": "0x365fb81bd4A24D6303cd2F19c349dE6894D8d58A",
         "NEG_RISK_ADAPTER": "0xc3Cf7c252f65E0d8D88537dF96569AE94a7F1A6E",
         "CONDITIONAL_TOKENS": "0x22DA1810B194ca018378464a58f6Ac2B10C9d244",
         "NEG_RISK_CONDITIONAL_TOKENS": "0x22DA1810B194ca018378464a58f6Ac2B10C9d244",  # (the SDKs' own key: the same contract on this track)
         "YIELD_BEARING_CTF_EXCHANGE": "0x6bEb5a40C032AFc305961162d8204CDA16DECFa5",
         "YIELD_BEARING_NEG_RISK_CTF_EXCHANGE": "0x8A289d458f5a134bA40015085A8F50Ffb681B41d",
         "YIELD_BEARING_NEG_RISK_ADAPTER": "0x41dCe1A4B8FB5e6327701750aF6231B7CD0B2A40",
         "YIELD_BEARING_CONDITIONAL_TOKENS": "0x9400F8Ad57e9e0F352345935d6D3175975eb1d9F",
         "YIELD_BEARING_NEG_RISK_CONDITIONAL_TOKENS": "0xF64b0b318AAf83BD9071110af24D24445719A07F",
         "USDT": "0x55d398326f99059fF775485246999027B3197955",
         "ECDSA_VALIDATOR": "0x845ADb2C711129d4f3966735eD98a9F09fC4cE57"},
    97: {"CTF_EXCHANGE": "0x2A6413639BD3d73a20ed8C95F634Ce198ABbd2d7",
         "NEG_RISK_CTF_EXCHANGE": "0xd690b2bd441bE36431F6F6639D7Ad351e7B29680",
         "NEG_RISK_ADAPTER": "0x285c1B939380B130D7EBd09467b93faD4BA623Ed",
         "CONDITIONAL_TOKENS": "0x2827AAef52D71910E8FBad2FfeBC1B6C2DA37743",
         "NEG_RISK_CONDITIONAL_TOKENS": "0x2827AAef52D71910E8FBad2FfeBC1B6C2DA37743",
         "YIELD_BEARING_CTF_EXCHANGE": "0x8a6B4Fa700A1e310b106E7a48bAFa29111f66e89",
         "YIELD_BEARING_NEG_RISK_CTF_EXCHANGE": "0x95D5113bc50eD201e319101bbca3e0E250662fCC",
         "YIELD_BEARING_NEG_RISK_ADAPTER": "0xb74aea04bdeBE912Aa425bC9173F9668e6f11F99",
         "YIELD_BEARING_CONDITIONAL_TOKENS": "0x38BF1cbD66d174bb5F3037d7068E708861D68D7f",
         "YIELD_BEARING_NEG_RISK_CONDITIONAL_TOKENS": "0x26e865CbaAe99b62fbF9D18B55c25B5E079A93D5",
         "USDT": "0xB32171ecD878607FFc4F8FC0bCcE6852BB3149E0",
         "ECDSA_VALIDATOR": "0x845ADb2C711129d4f3966735eD98a9F09fC4cE57"},
}
RPC_URLS = {56: "https://bsc-dataseed.bnbchain.org/", 97: "https://bsc-testnet-dataseed.bnbchain.org/"}
API_BASES = {56: core.PREDICT_REST, 97: "https://api-testnet.predict.fun/v1"}  # the testnet API needs no API key
CHAIN_NAMES = {56: "BNB 主网", 97: "BNB 测试网"}
ORDER_TYPES = [("salt", "uint256"), ("maker", "address"), ("signer", "address"), ("taker", "address"),
               ("tokenId", "uint256"), ("makerAmount", "uint256"), ("takerAmount", "uint256"), ("expiration", "uint256"),
               ("nonce", "uint256"), ("feeRateBps", "uint256"), ("side", "uint8"), ("signatureType", "uint8")]
DOMAIN_TYPES = [("name", "string"), ("version", "string"), ("chainId", "uint256"), ("verifyingContract", "address")]


def exchange_key(neg_risk: bool, yield_bearing: bool) -> str:
    """The exchange that settles a market: standard or multi-outcome (neg-risk), each with a yield-bearing twin."""
    return ("YIELD_BEARING_" if yield_bearing else "") + ("NEG_RISK_CTF_EXCHANGE" if neg_risk else "CTF_EXCHANGE")


def tokens_key(neg_risk: bool, yield_bearing: bool) -> str:
    return ("YIELD_BEARING_" if yield_bearing else "") + ("NEG_RISK_CONDITIONAL_TOKENS" if neg_risk else "CONDITIONAL_TOKENS")


# --- amounts (a port of the official SDK's helpers: the same truncation, the same integer arithmetic) --------------------
def to_wei(value: Any) -> int:
    """0.46 -> 460000000000000000 exactly (through Decimal, rounded down), as the SDK's floatToWei."""
    return int((D(str(value)) * WEI).quantize(D(1), rounding=decimal.ROUND_DOWN))


def from_wei(value: Any) -> float:
    return int(value) / WEI


def bnb_text(wei: Any) -> str:
    """BNB from wei with the decimals it needs, seven at most — a redeem's gas is a few hundred-thousandths (GAS-STATS)."""
    return f"{from_wei(wei):.7f}".rstrip("0").rstrip(".")


PRICE_TICK_WEI = 10 ** 16  # Predict prices carry at most two decimals (whole cents): the API refuses a third


def tick_price(price_wei: int) -> int:
    """A price on Predict's tick: rounded down to the cent, except a value a hair under a cent boundary (a float's
    1 − 0.87 = 0.12999…), which is that cent."""
    ticks, rem = divmod(max(price_wei, 0), PRICE_TICK_WEI)
    if rem and PRICE_TICK_WEI - rem <= 10 ** 9:
        ticks += 1
    return ticks * PRICE_TICK_WEI


def retain_sig(num: int, digits: int) -> int:
    """Keep ``digits`` significant digits of an integer (the rest become zeros): prices keep 3, quantities 5."""
    if num == 0:
        return 0
    sign, num = (-1, -num) if num < 0 else (1, num)
    excess = len(str(num)) - digits
    if excess <= 0:
        return sign * num
    scale = 10 ** excess
    return sign * (num // scale) * scale


def limit_amounts(buy: bool, price_wei: int, qty_wei: int) -> dict:
    """A LIMIT order's amounts: BUY offers price × shares USDT for the shares, SELL the other way round."""
    if price_wei <= 0:
        raise ValueError("价格必须大于 0")
    price, qty = tick_price(price_wei), retain_sig(qty_wei, 5)  # the price on the cent tick, the quantity to 5 significant digits (the SDK's rule)
    if price <= 0:
        raise ValueError("价格低于 1¢")
    if qty < 10 ** 16:
        raise ValueError("数量不足 0.01 份")
    notional = price * qty // WEI
    return {"price_per_share": price, "maker": notional if buy else qty, "taker": qty if buy else notional,
            "amount": qty, "last": price, "slippage_bps": 0, "min_out": False}


def _eth() -> Any:
    try:
        import eth_account  # noqa: F401
        import eth_abi
        import eth_utils
        from eth_account import Account
        from eth_account.messages import _hash_eip191_message, encode_defunct, encode_typed_data
    except ImportError as error:  # pragma: no cover - depends on the environment
        raise RuntimeError("真实交易需要 eth-account 包：pip install eth-account（requirements.txt 已列出）") from error
    return Account, encode_defunct, encode_typed_data, _hash_eip191_message, eth_abi, eth_utils


def checksum(address: str) -> str:
    _, _, _, _, _, eth_utils = _eth()
    return eth_utils.to_checksum_address(address)


class Wallet:
    """The signing key, and the account the orders are made for: the key's own address, or a Predict account (smart
    wallet) it controls, whose signatures are wrapped the way the Kernel contract verifies them."""

    def __init__(self, private_key: str, chain_id: int, predict_account: str = ""):
        Account, _, _, _, _, eth_utils = _eth()
        if chain_id not in ADDRESSES:
            raise ValueError(f"不支持的链 {chain_id}（56 = BNB 主网，97 = BNB 测试网）")
        self.chain_id, self.addresses = chain_id, ADDRESSES[chain_id]
        self.account = Account.from_key(private_key)
        self.signer: str = self.account.address
        self.predict_account = eth_utils.to_checksum_address(predict_account) if predict_account else ""
        self.maker: str = self.predict_account or self.signer  # the orders' maker: whoever holds the USDT and the shares

    @property
    def kind(self) -> str:
        return "Predict 账户（智能钱包）" if self.predict_account else "普通钱包"

    def typed_data(self, order: dict, neg_risk: bool, yield_bearing: bool) -> dict:
        return {"types": {"EIP712Domain": [{"name": n, "type": t} for n, t in DOMAIN_TYPES],
                          "Order": [{"name": n, "type": t} for n, t in ORDER_TYPES]},
                "primaryType": "Order",
                "domain": {"name": PROTOCOL_NAME, "version": PROTOCOL_VERSION, "chainId": self.chain_id,
                           "verifyingContract": self.addresses[exchange_key(neg_risk, yield_bearing)]},
                "message": {name: order[name] for name, _ in ORDER_TYPES}}

    def order_hash(self, order: dict, neg_risk: bool, yield_bearing: bool) -> str:
        _, _, encode_typed_data, hash_eip191, _, _ = _eth()
        return "0x" + hash_eip191(encode_typed_data(full_message=self.typed_data(order, neg_risk, yield_bearing))).hex()

    def sign_order(self, order: dict, neg_risk: bool, yield_bearing: bool) -> tuple[str, str]:
        """(order hash, signature) for ``order`` on the exchange that settles its market."""
        _, encode_defunct, encode_typed_data, _, _, _ = _eth()
        digest = self.order_hash(order, neg_risk, yield_bearing)
        if self.predict_account:
            return digest, self.kernel_sign(digest)
        signed = self.account.sign_message(encode_typed_data(full_message=self.typed_data(order, neg_risk, yield_bearing)))
        return digest, "0x" + signed.signature.hex()

    def sign_text(self, message: str) -> str:
        """The sign-in message (GET /v1/auth/message), EIP-191 signed; wrapped for a Predict account."""
        _, encode_defunct, _, hash_eip191, _, _ = _eth()
        if self.predict_account:
            return self.kernel_sign("0x" + hash_eip191(encode_defunct(text=message)).hex())
        return "0x" + self.account.sign_message(encode_defunct(text=message)).signature.hex()

    def kernel_sign(self, message_hash: str) -> str:
        """A Predict account's signature of ``message_hash``: the hash wrapped in the Kernel domain of the account,
        signed as a personal message, prefixed with 0x01 and the ECDSA validator (the official SDKs' format)."""
        _, encode_defunct, _, _, eth_abi, eth_utils = _eth()
        keccak = eth_utils.keccak
        domain = keccak(eth_abi.encode(
            ["bytes32", "bytes32", "bytes32", "uint256", "address"],
            [keccak(text="EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"),
             keccak(text=KERNEL_NAME), keccak(text=KERNEL_VERSION), self.chain_id, self.predict_account]))
        wrapped = keccak(eth_abi.encode(["bytes32", "bytes32"], [keccak(text="Kernel(bytes32 hash)"),
                                                                 bytes.fromhex(message_hash.removeprefix("0x"))]))
        digest = keccak(b"\x19\x01" + domain + wrapped)
        signed = self.account.sign_message(encode_defunct(primitive=digest))
        return "0x01" + self.addresses["ECDSA_VALIDATOR"][2:] + signed.signature.hex()

    def sign_transaction(self, tx: dict) -> bytes:
        signed = self.account.sign_transaction(tx)
        return bytes(getattr(signed, "raw_transaction", None) or getattr(signed, "rawTransaction"))


def build_order(wallet: Wallet, token_id: str, maker_amount: int, taker_amount: int, fee_bps: int, expiration: int,
                salt: int | None = None) -> dict:
    """An unsigned BUY order as the exchange reads it (every uint as a decimal string, as the SDKs send them)."""
    return {"salt": str(random.SystemRandom().randint(0, MAX_SALT) if salt is None else salt),
            "maker": wallet.maker, "signer": wallet.maker, "taker": ZERO_ADDRESS, "tokenId": str(token_id),
            "makerAmount": str(maker_amount), "takerAmount": str(taker_amount), "expiration": str(int(expiration)),
            "nonce": "0", "feeRateBps": str(int(fee_bps)), "side": 0, "signatureType": 0}


def order_expiry(end_ms: int | None, now_ms: int) -> int:
    """The latest expiry (epoch seconds) a model order on a market that ends at ``end_ms`` is signed with:
    ORDER_END_GRACE_SECONDS after that end, never under ORDER_MIN_SECONDS from ``now_ms``; FAR_FUTURE (no expiry) only
    when the market's end is not known.

    1.150.0: the signed expiry is the one bound on an order that needs nothing from this bot or from Predict's API —
    past it the exchange contract no longer matches the order — so it is what is left when a cancel cannot get through:
    the cancel endpoint erroring, the sign-in broken, a long 429 cooldown, the process or its host down across the end
    (stopping cancels nothing: drain_orders). The strategy wants no order on a market past its end: sim_quiet withdraws
    every one there whatever the settings ("a bid left on a finished market is a free option for whoever holds the
    losing side") and live_send_block sends none from then on. Yet a maker was signed for two hours past the end, and a
    taker for its five minutes wherever the end fell: with the removal failing, the bid stayed matchable all that time
    after the result was public, for the losing side's holders to sell into — a loss of up to LIVE_MAX_ORDER_USD an
    order. The pre-open mode signs its orders to expire before their start for the same reason (PREOPEN.md: 撤单失败时的
    兜底). The grace keeps an order good for its market's whole life whatever the clocks (this bot's, the chain's): the
    withdrawal at the end comes first, the expiry is what is left when that fails. The floor replaces the old fallback
    to FAR_FUTURE (an expiry under two minutes away meant no expiry at all — with the grace above, every order decided
    in a market's last minute): an order decided that late is valid two minutes. Nothing is sent at or after the end
    (live_send_block), so an order has over a minute left when it goes out — what the pre-open mode asks of its own
    (SEND_MIN_LEFT_MS)."""
    if not end_ms:
        return FAR_FUTURE
    return max(int(end_ms) // 1000 + ORDER_END_GRACE_SECONDS, int(now_ms) // 1000 + ORDER_MIN_SECONDS)


# --- the REST API (api.predict.fun/v1): sign-in, orders, positions -----------------------------------------------------
def answer_shape(answer: Any) -> str:
    """An API answer's field names (one level into "data"), for an error that must not quote its values."""
    if not isinstance(answer, dict):
        return type(answer).__name__
    inner = answer.get("data")
    names = sorted(str(k) for k in answer)
    if isinstance(inner, dict):
        names.append("data{" + ",".join(sorted(str(k) for k in inner)) + "}")
    return ",".join(names)[:160]


def http_status(error: BaseException) -> int:
    match = re.match(r"HTTP (\d{3})", str(error))
    return int(match.group(1)) if match else 0


def jwt_expiry(token: str) -> int:
    """The token's exp claim in seconds, 0 when unreadable."""
    try:
        payload = token.split(".")[1]
        data = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
        return int(data.get("exp") or 0)
    except (IndexError, ValueError, TypeError, AttributeError):
        return 0


class OrderRejected(core.RemoteError):
    """Predict refused the order outright (success=false, HTTP 4xx): nothing was placed, the attempt is over. Any
    other failure after the request went out (a 5xx, a timeout, an answer without an order id) leaves it unknown."""


# --- withdrawing many orders in one go (1.141.0) -------------------------------------------------------------------------------
REMOVE_START = 20      # ids in one POST /orders/remove at first: what the pre-open mode always sent (1.124.0 to 1.140.0)
REMOVE_MAX = 500       # ...doubled while Predict answers every id of a batch, up to this many (Predict's own ceiling is measured: RemoveLimit)
REMOVE_TRIES = 3       # a batch failing this often (a timeout, a server error, ids its answer leaves out; a rate limit never counts): given up
REMOVE_RETRY_S = 1.0   # ...sent again this long after such a failure, doubling
REMOVE_CAP_DAYS = 1    # a measured ceiling lapses after this long: Predict may take more by then (1.141.1)
REMOVE_SOFT_MS = 3_600_000  # a size that failed in one withdrawal is avoided this long in the process; a second withdrawal finding the same makes it the ceiling
SYSTEMIC_STATUSES = {401, 403, 404, 405}  # a removal refused with these: the sign-in, the access or the endpoint — every batch would be; the withdrawal stops at once
REMOVE_RUNS = [0]      # withdrawals so far in this process (remove_in_batches): a ceiling needs two of them to agree


class RemoveLimit:
    """How many order ids Predict takes in one removal request, as measured on this account (1.141.0): ``ok``, the most a
    request was answered for whole; ``cap``, the ceiling — a size two withdrawals each found too many (a request of it
    refused or timed out on whole, its halves then through; or only its head answered, the tail then through). 1.141.1: one
    withdrawal's finding only keeps the size out of this process for REMOVE_SOFT_MS (``soft``); a refusal never lowers the
    batch to REMOVE_START or under (Predict took 20 for months: a one-off refusal said nothing about the size); the ceiling
    lapses after REMOVE_CAP_DAYS. The next batch: the best size seen under the ceiling; with none, twice the best size seen,
    from REMOVE_START up to REMOVE_MAX. One for every client on the key, kept in the bot's store (LiveBot.live_remove_limit)."""

    def __init__(self, ok: int = 0, cap: int = 0, at: int = 0):
        self.ok, self.cap, self.at = max(0, int(ok or 0)), max(0, int(cap or 0)), int(at or 0)
        if self.cap and self.ok >= self.cap:
            self.ok = self.cap - 1
        self.soft, self.soft_at = 0, 0  # a size one withdrawal found too many: avoided in this process for REMOVE_SOFT_MS (not kept)
        self.strikes: dict[int, set] = {}  # size -> the withdrawals that found it too many: two make it the ceiling
        self.changed = False

    @classmethod
    def from_dict(cls, data: Any) -> "RemoveLimit":
        data = data if isinstance(data, dict) else {}
        try:
            return cls(data.get("ok", 0), data.get("cap", 0), data.get("at", 0))
        except (TypeError, ValueError):
            return cls()

    def as_dict(self) -> dict:
        return {"ok": self.ok, "cap": self.cap, "at": self.at}

    def refresh(self, now_ms: int) -> None:
        """At a withdrawal's start: a ceiling REMOVE_CAP_DAYS old lapses (Predict may take more by now), and a size avoided
        after one withdrawal found it too many is tried again REMOVE_SOFT_MS on."""
        if self.cap and now_ms - self.at >= REMOVE_CAP_DAYS * 86_400_000:
            self.cap, self.at, self.changed = 0, int(now_ms), True
            self.strikes.clear()
        if self.soft and now_ms - self.soft_at >= REMOVE_SOFT_MS:
            self.soft = 0

    def size(self) -> int:
        ceiling = min([x for x in (self.cap, self.soft) if x], default=0)
        if ceiling:
            return max(1, min(self.ok if 0 < self.ok < ceiling else ceiling // 2, ceiling - 1))
        return min(REMOVE_MAX, self.ok * 2 if self.ok >= REMOVE_START else REMOVE_START)

    def note_ok(self, n: int, now_ms: int) -> None:
        """A request of ``n`` ids answered for whole (at or over the ceiling: a passing exception, not counted; at or over a
        size avoided after one withdrawal: that one was the exception)."""
        if self.soft and n >= self.soft:
            self.soft = 0
        if n > self.ok and (not self.cap or n < self.cap):
            self.ok, self.at, self.changed = int(n), int(now_ms), True

    def note_cap(self, n: int, now_ms: int, run: int, proven: bool = False) -> None:
        """``n`` ids in one request looked like too many in withdrawal ``run`` — a request of them refused or timed out on
        whole, its halves then through; ``proven``: only its head answered, the tail then through (a pattern no blip makes).
        A refusal at REMOVE_START or under is not believed. Avoided at once in this process (soft); the ceiling once a second
        withdrawal finds the same."""
        if n <= 1 or (n <= REMOVE_START and not proven):
            return
        self.soft, self.soft_at = min(self.soft or n, n), int(now_ms)
        runs = self.strikes.setdefault(int(n), set())
        runs.add(run)
        if len(runs) >= 2 and (not self.cap or n < self.cap):
            self.cap, self.at, self.changed = int(n), int(now_ms), True
            self.ok = min(self.ok, self.cap - 1)

    def text(self) -> str:
        """The status's words: the batch now, and what it rests on."""
        seen = (([f"实测一次 {self.ok} 张全部应答"] if self.ok >= REMOVE_START or self.cap or self.soft else []) + ([f"一次 {self.cap} 张太多"] if self.cap else [])
                + ([f"{self.soft} 张一批刚失败过，暂时不用"] if self.soft and self.soft != self.cap else []))
        return f"每批 {self.size()} 张（{'，'.join(seen) or '还没有实测'}；从 {REMOVE_START} 张起，全部应答就翻倍，最多 {REMOVE_MAX} 张）"


def remove_failure(error: BaseException) -> str:
    """What a failed removal request means: "limited" — a 429, or a cooldown the client is in (begun by any client on the
    key): waited out; "systemic" — the sign-in, the access or the endpoint (SYSTEMIC_STATUSES): every batch would be
    refused, the withdrawal stops (1.141.1); "refused" — Predict answered with a refusal (too many ids, or one it will not
    take); "transient" — a timeout, a network or server error."""
    status = http_status(error)
    if status == 429 or getattr(error, "retry_after", 0):
        return "limited"
    if status in SYSTEMIC_STATUSES:
        return "systemic"
    if 400 <= status < 500 or "不是已确认成功" in str(error) or "撤单被拒绝" in str(error):
        return "refused"
    return "transient"


async def remove_in_batches(api: Any, ids: list, limit: RemoveLimit, *, workers: int = 4, patient: bool = True,
                            deadline: float | None = None, stop: Any = None, on_answer: Any = None,
                            progress: dict | None = None, now_ms: Any = None) -> dict:
    """``ids`` withdrawn (POST /orders/remove) in batches of ``limit.size()``, in the order given (the soonest first), at most
    ``workers`` requests at a time — the client's other slots stay free (an order, a window at its cancel time); each
    answer handed to ``on_answer(removed, noop)`` as it comes (1.141.0).

    ``patient`` (a book of hundreds or thousands: a pause, a settings change, the mode switched off): a rate limit — a 429,
    or a cooldown another client on the key began — is waited out and the batch sent again, the rest going with half as
    many requests at a time; a batch Predict refuses whole, or times out on while bigger than REMOVE_START, is split in two
    and each half sent (both going through: the size was the problem — ``limit.cap``; an id Predict will not take ends up
    alone and is given up); the ids an answer leaves out of both its lists are sent again (only a tail left out, then going
    through: Predict took no more than the head — measured the same way); any other failure is tried REMOVE_TRIES times.
    Impatient (a window's own few orders, the orphan sweep): one try each, whatever happens, and nothing sent into a
    cooldown — the caller's own retry takes the rest on, as before 1.141.0. The batch grows while answered whole
    (RemoveLimit). Nothing more is sent past ``deadline`` (time.monotonic) or once ``stop()`` says so.

    1.141.1: a refusal of the request itself — SYSTEMIC_STATUSES, or both halves of a split refused whole (neither one id
    nor the size: the request) — stops the withdrawal at once, the rest left for the caller's next try; it used to split
    down to single ids, two requests an id every retry.

    Returns the ids — removed; noop (Predict: nothing to remove); unanswered (left out of every answer); failed (their
    request failed every try, or Predict refused them alone); left (never sent: the deadline, the stop, a rate limit when
    impatient, a refusal of the request itself) — with why it stopped early (``halted``: "limited", "systemic",
    "refused" or ""), the requests made, the rate limits met, the seconds taken, the biggest batch answered whole and the
    last errors."""
    clock = now_ms or (lambda: int(time.time() * 1000))
    limit.refresh(clock())
    REMOVE_RUNS[0] += 1
    run = REMOVE_RUNS[0]
    pending = collections.deque(dict.fromkeys(str(i) for i in ids))
    front: collections.deque = collections.deque()  # batches that go before new ones: a retry, a split's halves, a tail left out
    out: dict[str, Any] = {"removed": [], "noop": [], "unanswered": [], "failed": [], "left": [], "requests": 0, "limited": 0,
                           "seconds": 0.0, "biggest": 0, "errors": [], "halted": ""}
    state = {"allowed": max(1, int(workers)), "cooled": 0.0, "halted": False, "busy": 0}
    started = time.monotonic()
    if progress is not None:
        progress.clear()
        progress.update(total=len(pending), removed=0, noop=0, failed=0, requests=0, limited=0, size=limit.size(),
                        workers=state["allowed"], started=started)

    def note() -> None:
        if progress is not None:
            progress.update(removed=len(out["removed"]), noop=len(out["noop"]) + len(out["unanswered"]), failed=len(out["failed"]),
                            requests=out["requests"], limited=out["limited"], size=limit.size(), workers=state["allowed"])

    def family_done(fam: dict | None, whole: bool) -> None:
        """A split's half (or a tail left out) answered: once all of them are, the size they came from was too big if every one went through whole."""
        if fam is None:
            return
        fam["left"] -= 1
        fam["whole"] = fam["whole"] and whole
        if fam["left"] <= 0 and fam["whole"]:
            limit.note_cap(fam["n"], clock(), run, proven=bool(fam.get("k")))
            if fam.get("k"):
                limit.note_ok(fam["k"], clock())

    def halted() -> bool:
        return state["halted"] or (deadline is not None and time.monotonic() >= deadline) or bool(stop is not None and stop())

    def take() -> dict | None:
        if front:
            return front.popleft()
        if not pending:
            return None
        return {"ids": [pending.popleft() for _ in range(min(limit.size(), len(pending)))], "tries": 0, "fam": None}

    def give_up(batch: dict, key: str, why: str) -> None:
        out[key].extend(batch["ids"])
        family_done(batch["fam"], False)
        if why:
            out["errors"] = [*out["errors"][-4:], why]

    def cooldown(error: BaseException | None) -> None:
        """A rate limit met: counted, and the requests at a time halved — once a cooldown."""
        until = float(getattr(api, "blocked_until", 0.0) or 0.0)
        if until > state["cooled"] + 0.5 or (not until and error is not None):
            state["cooled"] = until or time.monotonic()
            state["allowed"] = max(1, state["allowed"] // 2)
            out["limited"] += 1

    async def wait(seconds: float) -> None:
        """A pause that counts as work in hand: the other workers wait for what it may send back, they do not end."""
        state["busy"] += 1
        try:
            await asyncio.sleep(max(0.0, seconds if deadline is None else min(seconds, deadline - time.monotonic())))
        finally:
            state["busy"] -= 1

    def answered(batch: dict, answer: Any) -> None:
        data = answer if isinstance(answer, dict) else {}
        said_removed = {str(x) for x in data.get("removed") or []}
        said_noop = {str(x) for x in data.get("noop") or []} - said_removed
        removed = [i for i in batch["ids"] if i in said_removed]
        noop = [i for i in batch["ids"] if i in said_noop]
        out["removed"] += removed
        out["noop"] += noop
        if on_answer is not None and (removed or noop):
            on_answer(removed, noop)
        missing = [i for i in batch["ids"] if i not in said_removed and i not in said_noop]
        if not missing:
            limit.note_ok(len(batch["ids"]), clock())
            out["biggest"] = max(out["biggest"], len(batch["ids"]))
            family_done(batch["fam"], True)
            return
        family_done(batch["fam"], False)
        head = len(batch["ids"]) - len(missing)
        if not patient:
            out["unanswered"] += missing
        elif head and missing == batch["ids"][head:] and len(missing) >= max(2, len(batch["ids"]) // 4):
            # only the head answered: Predict may take no more than that many at a time — the tail goes again, and its going
            # through whole is the proof (an id simply not on the book anywhere in the list would not make a clean tail)
            front.appendleft({"ids": missing, "tries": batch["tries"], "fam": {"n": len(batch["ids"]), "left": 1, "whole": True, "k": head}})
        elif batch["tries"] + 1 >= REMOVE_TRIES:
            out["unanswered"] += missing
        else:
            front.append({"ids": missing, "tries": batch["tries"] + 1, "fam": None})

    def stop_here(batch: dict, why: str, cause: str) -> None:
        """The withdrawal stops: this batch and every one not sent yet are left for the caller's next try."""
        front.appendleft(batch)
        state["halted"], out["halted"] = True, cause
        out["errors"] = [*out["errors"][-4:], why]

    async def failed(batch: dict, error: BaseException) -> None:
        kind, why = remove_failure(error), core.clean_error(error) or type(error).__name__
        fam = batch["fam"]
        if kind == "limited":
            cooldown(error)
            if not patient:
                stop_here(batch, why, "limited")
                return
            front.appendleft(batch)  # sent again once the cooldown is over: not a try
            await wait(max(float(getattr(api, "blocked_until", 0.0) or 0.0) - time.monotonic(), 0.05) + 0.05)
            return
        if kind == "systemic":  # the sign-in, the access, the endpoint: every batch would be refused
            stop_here(batch, why, "systemic")
            return
        if kind == "refused" and fam is not None:
            fam["refused"] = fam.get("refused", 0) + 1
            if fam["refused"] >= 2:  # both halves refused whole: neither one id nor the size — the request itself
                stop_here(batch, why, "refused")
                return
        if not patient:
            give_up(batch, "failed", why)
        elif len(batch["ids"]) > 1 and (kind == "refused" or len(batch["ids"]) > REMOVE_START):
            family_done(batch["fam"], False)  # the halves are a family of their own: this batch is not whole
            half, fam = len(batch["ids"]) // 2, {"n": len(batch["ids"]), "left": 2, "whole": True}
            # queued behind the halves already waiting (breadth first, 1.141.1): a split's two halves go out side by side, so a
            # refusal of every request shows as both refused within a round — not after a worker has drilled down to single ids
            front.append({"ids": batch["ids"][:half], "tries": 0, "fam": fam})
            front.append({"ids": batch["ids"][half:], "tries": 0, "fam": fam})
        elif kind == "refused" or batch["tries"] + 1 >= REMOVE_TRIES:
            give_up(batch, "failed", why)
        else:
            await wait(REMOVE_RETRY_S * 2 ** batch["tries"])
            front.append({**batch, "tries": batch["tries"] + 1})

    async def worker(index: int) -> None:
        while not halted() and index < state["allowed"]:
            cool = float(getattr(api, "blocked_until", 0.0) or 0.0) - time.monotonic()
            if cool > 0:  # a cooldown on the key (any client's 429): nothing is sent into it
                if not patient:
                    state["halted"] = True
                    out["errors"] = [*out["errors"][-4:], f"Predict 接口限流冷却中（{int(cool) + 1} 秒后重试）"]
                    return
                cooldown(None)
                await wait(cool + 0.05)
                continue
            batch = take()
            if batch is None:
                if state["busy"] or front:  # an answer still to come may send a batch back (a retry, a split's halves)
                    await asyncio.sleep(0.02)
                    continue
                return
            state["busy"] += 1
            out["requests"] += 1
            try:
                answer, error = await api.remove_orders(batch["ids"]), None
            except asyncio.CancelledError:
                front.appendleft(batch)
                raise
            except Exception as exc:  # judged below: a rate limit, a refusal, a timeout
                answer, error = None, exc
            finally:
                state["busy"] -= 1
            if error is None:
                answered(batch, answer)
            else:
                await failed(batch, error)
            note()

    if pending:
        tasks = [asyncio.create_task(worker(i), name=f"remove-{i}") for i in range(state["allowed"])]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    out["left"] = [i for batch in front for i in batch["ids"]] + list(pending)
    out["seconds"] = time.monotonic() - started
    note()
    return out


# 1.150.0 (audit LIVE-SEND-AFTER-PAUSE): the model trader's last check (live_send_gate) ran before create_order's own waits — a
# slot of PredictApi.gate (busy with reads and cancels) — so an order held there went out after /live pause, the emergency stop or
# LIVE=off had come meanwhile: a new position opened after the operator paused, or a taker filled before its withdrawal's cancel.
# live_place_order sets this (task-local: it follows authed -> call -> gate -> send in the same task) to a function that judges
# the order again; PredictApi.send runs it inside the slot, right before the request is handed to the transport, as the pre-open
# mode's safe_api does with its before_send. Unset (every other caller) nothing changes.
ORDER_SEND_CHECK: contextvars.ContextVar[Any] = contextvars.ContextVar("order_send_check", default=None)


class SendRefused(core.RemoteError):
    """A local gate refused the order inside the request slot, before its bytes were dispatched: nothing went out."""


class PredictApi:
    """Signed-in requests to Predict's REST API. The API key goes in x-api-key on every request; the JWT from the
    sign-in flow (GET /v1/auth/message → sign → POST /v1/auth) in Authorization, renewed before it expires or after a
    401. A 429 pauses every request for its retry-after."""
    PARALLEL = 5           # requests in flight at most (1.112.1: 4, was 3 — a cancel and its re-quote go side by side; 1.138.2: 5)
    READ_PARALLEL = 3      # ...of which the reads may hold this many: two slots always stay free for an order or a cancel (1.138.2: 3 — the
    #                        sync's per-order reads, six a step, two at a time, were half its second)
    BACKOFF_SECONDS = 20

    def __init__(self, base: str, api_key: str, wallet: Wallet | None):
        self.base, self.api_key, self.wallet = base.rstrip("/"), api_key, wallet
        self.jwt, self.jwt_exp, self.jwt_at = "", 0, 0.0
        self.auth_error = ""
        self._auth_lock: tuple[Any, asyncio.Lock] | None = None  # (loop, lock): the sign-in runs once at a time (see auth_lock)
        self.gate = asyncio.Semaphore(self.PARALLEL)
        self.read_gate = asyncio.Semaphore(max(1, min(self.READ_PARALLEL, self.PARALLEL - 1)))  # the reads leave slots: an order or a cancel never queues behind them
        self.writes_inflight = 0  # orders and cancels on their way (call: priority): the shutdown drain waits for their answers
        self.blocked_until = 0.0
        self.calls = 0
        self.limited: collections.deque = collections.deque(maxlen=500)  # monotonic times of the 429s answered to this client (/diag, 1.138.0)
        self.orders_complete = self.positions_complete = True  # the last list read came whole (every page)

    def headers(self) -> dict[str, str]:
        out = {}
        if self.api_key:
            out["x-api-key"] = self.api_key
        if self.jwt:
            out["Authorization"] = f"Bearer {self.jwt}"
        return out

    async def call(self, method: str, path: str, payload: dict | None = None, timeout: int = API_TIMEOUT, priority: bool = False) -> Any:
        """One request under the gate (PARALLEL in flight at most). A read takes a slot of the reads' own gate first
        (READ_PARALLEL), so slots are always free for a ``priority`` request — an order, a cancel — which never waits
        behind the reads."""
        left = self.blocked_until - time.monotonic()
        if left > 0:
            raise core.RemoteError(f"Predict 接口限流冷却中（{int(left) + 1} 秒后重试）", int(left) + 1)
        if method == "POST" and payload is None:
            payload = {}
        self.calls += 1
        if priority:
            self.writes_inflight += 1
            try:
                async with self.gate:
                    raw = await self.send(method, path, payload, timeout)
            finally:
                self.writes_inflight -= 1
        else:
            async with self.read_gate, self.gate:
                raw = await self.send(method, path, payload, timeout)
        try:
            return json.loads(raw) if raw else {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise core.RemoteError("Predict 接口未返回有效 JSON") from None

    async def send(self, method: str, path: str, payload: dict | None, timeout: int) -> Any:
        # 1.138.0: a cooldown that began while this request waited for its slot (another client's 429 on the key) is honoured
        # here too: nothing goes out into it — refused by the client (status 0, retry_after), so an order is deferred, not failed
        if (left := self.blocked_until - time.monotonic()) > 0:
            raise core.RemoteError(f"Predict 接口限流冷却中（{int(left) + 1} 秒后重试）", int(left) + 1)
        if method == "POST" and path == "/orders" and (check := ORDER_SEND_CHECK.get()) is not None and (why := check()):
            raise SendRefused(why)  # 1.150.0: judged again inside the slot (see ORDER_SEND_CHECK) — never sent
        try:
            return await core.predict_blocking(core._http_get, self.base + path, payload, timeout, self.headers())
        except core.RemoteError as error:
            if error.retry_after or http_status(error) == 429:
                self.blocked_until = max(self.blocked_until, time.monotonic() + max(error.retry_after, self.BACKOFF_SECONDS))
                if http_status(error) == 429:
                    self.limited.append(time.monotonic())
            raise

    async def authed(self, method: str, path: str, payload: dict | None = None, timeout: int = API_TIMEOUT, priority: bool = False) -> Any:
        """A request that needs the sign-in: signs in first when the token is missing or about to expire, once more
        after a 401."""
        await self.ensure_auth()
        used = self.jwt  # the token this request goes out with (a renewal while it waits for its slot is fine: it then carries the newer one)
        try:
            return await self.call(method, path, payload, timeout, priority)
        except core.RemoteError as error:
            if http_status(error) != 401 or not self.wallet:
                raise
            # 1.150.0 (audit LIVE-AUTH-SINGLEFLIGHT): only the token that was refused is dropped. Several requests in flight share
            # one token: on its 401 each of them used to blank self.jwt — wiping the fresh token another one had just signed in for
            # — and sign in again on its own (N × GET /auth/message + POST /auth on the key's shared rate limit, and, should a new
            # sign-in revoke the older tokens, their retries refusing each other). Now the first clears it, the sign-in runs once
            # under auth_lock, and the others retry with its token.
            if self.jwt == used:
                self.jwt = ""
            await self.ensure_auth()
            return await self.call(method, path, payload, timeout, priority)

    def fresh(self) -> bool:
        """A token that is there and not about to expire (five minutes' margin)."""
        return bool(self.jwt) and (not self.jwt_exp or time.time() < self.jwt_exp - 300)

    @property
    def auth_lock(self) -> asyncio.Lock:
        """The lock that makes the sign-in single-flight, one per event loop (a client reused across asyncio.run calls — the
        tests — never meets a lock bound to a closed loop)."""
        loop = asyncio.get_running_loop()
        if self._auth_lock is None or self._auth_lock[0] is not loop:
            self._auth_lock = (loop, asyncio.Lock())
        return self._auth_lock[1]

    async def ensure_auth(self) -> None:
        if self.fresh():
            return
        async with self.auth_lock:
            if self.fresh():  # 1.150.0: signed in by the caller ahead in the queue — its token serves this one too
                return
            await self._sign_in()

    async def authenticate(self) -> str:
        """A sign-in now (the background renewal, /control): single-flight with ensure_auth's — it waits for one already
        running, then signs in itself (the caller asked for a new token, not just a fresh one)."""
        async with self.auth_lock:
            return await self._sign_in()

    async def _sign_in(self) -> str:
        """The sign-in flow: a message to sign, its signature, a JWT. Raises (and records why) when it fails. Callers hold
        auth_lock (ensure_auth, authenticate)."""
        if self.wallet is None:
            raise core.RemoteError("没有签名钱包，无法登录 Predict")
        try:
            data = await self.call("GET", "/auth/message")
            message = str(((data or {}).get("data") or {}).get("message") or "")
            if not message:
                raise core.RemoteError("登录消息格式异常（应答字段：" + answer_shape(data) + "）")
            answer = await self.call("POST", "/auth", {"signer": self.wallet.maker, "signature": self.wallet.sign_text(message),
                                                      "message": message})
            token = str(((answer or {}).get("data") or {}).get("token") or "")
            if not token:
                raise core.RemoteError("登录未返回 token（应答字段：" + answer_shape(answer) + "）")  # never the answer itself: a token under another name would be logged
        except core.RemoteError as error:
            self.auth_error = core.clean_error(error)
            raise
        self.jwt, self.jwt_exp, self.jwt_at, self.auth_error = token, jwt_expiry(token), time.time(), ""
        return token

    @staticmethod
    def rows(data: Any) -> list[dict]:
        """The records of a list answer ({success, data: [...]}, {data: {items: [...]}} or a bare list); success=false is an
        error (1.122.0: it used to read as an empty list — every open order looked gone)."""
        if isinstance(data, dict):
            if data.get("success") is False:
                raise core.RemoteError("Predict 接口返回 success=false：" + (core.http_error_text(data) or core.brief_error(json.dumps(data, ensure_ascii=False), 120)))
            data = data.get("data", data)
        if isinstance(data, dict):
            for key in ("items", "orders", "positions", "results", "rows"):
                if isinstance(data.get(key), list):
                    data = data[key]
                    break
        return [row for row in data if isinstance(row, dict)] if isinstance(data, list) else []

    async def market(self, market_id: str) -> dict:
        data = await self.call("GET", f"/markets/{core.urllib.parse.quote(str(market_id))}")
        if isinstance(data, dict) and data.get("success") is False:
            raise core.RemoteError("Predict 接口返回 success=false：" + (core.http_error_text(data) or core.brief_error(json.dumps(data, ensure_ascii=False), 120)))
        market = data.get("data", data) if isinstance(data, dict) else {}
        if not isinstance(market, dict) or not market:
            raise core.RemoteError("Predict 市场详情格式异常")
        return market

    async def create_order(self, body: dict) -> dict:
        """POST /v1/orders -> {"order_id", "hash", "code"}; success=false is an error with the API's own reason."""
        answer = await self.authed("POST", "/orders", body, timeout=20, priority=True)
        data = answer.get("data") if isinstance(answer, dict) else None
        if not isinstance(answer, dict) or answer.get("success") is False or not isinstance(data, dict):
            raise OrderRejected("下单被拒绝：" + (core.http_error_text(answer) if isinstance(answer, dict) else "")
                                   or core.brief_error(json.dumps(answer, ensure_ascii=False), 120))
        order_id = str(data.get("orderId") or data.get("id") or data.get("order_id") or "")
        if not order_id:
            raise core.RemoteError("下单应答没有订单号：" + core.brief_error(json.dumps(answer, ensure_ascii=False), 120))
        return {"order_id": order_id, "hash": str(data.get("orderHash") or data.get("hash") or ""), "code": data.get("code")}

    async def remove_orders(self, ids: list[str]) -> dict:
        """POST /v1/orders/remove -> {"removed": [...], "noop": [...]}. A batch of hundreds (remove_in_batches, 1.141.0) gets
        more time than an order: a second per hundred ids beyond the first."""
        timeout = API_TIMEOUT + max(0, len(ids) - REMOVE_START) // 100
        answer = await self.authed("POST", "/orders/remove", {"data": {"ids": [str(i) for i in ids]}}, timeout=timeout, priority=True)
        data = answer.get("data") if isinstance(answer, dict) and isinstance(answer.get("data"), dict) else answer
        if not isinstance(answer, dict) or answer.get("success") is False:
            raise core.RemoteError("撤单被拒绝：" + (core.http_error_text(answer) if isinstance(answer, dict) else "")
                                   or core.brief_error(json.dumps(answer, ensure_ascii=False), 120))
        return {"removed": [str(i) for i in (data.get("removed") or [])], "noop": [str(i) for i in (data.get("noop") or [])]}

    ORDERS_PAGE = 50      # orders per page (the API pages with first / after and answers a cursor)
    ORDERS_PAGES_MAX = 20  # ...and this many pages at most in one read (1000 orders)

    async def paged(self, path: str, query: dict, stop_ids: set | None = None, max_pages: int | None = None,
                    cap: int | None = None) -> tuple[list[dict], bool]:
        """Every page of a list (first / after, the cursor the answer gives): (rows, complete). Not complete when the
        page cap was reached with a cursor still on offer — the caller must not take the list for the whole truth — or
        when the read stopped on purpose (1.137.1): ``stop_ids`` all seen, or ``max_pages`` read. ``cap`` (1.150.0) is
        this one read's own page cap in place of ORDERS_PAGES_MAX (the redeem's read of the positions: positions_read)."""
        rows, after, seen = [], "", set()
        limit = self.ORDERS_PAGES_MAX if not cap else max(1, int(cap))
        pages = limit if not max_pages else max(1, min(limit, int(max_pages)))
        for _ in range(pages):
            q = {**query, "first": self.ORDERS_PAGE}
            if after:
                q["after"] = after
            data = await self.authed("GET", f"{path}?{core.urllib.parse.urlencode(q)}")
            page = self.rows(data)
            rows.extend(page)
            cursor = data.get("cursor") if isinstance(data, dict) else None
            if isinstance(cursor, dict):
                cursor = cursor.get("after") or cursor.get("next") or cursor.get("endCursor")
            if not cursor or not page or str(cursor) == after:
                return rows, True
            if stop_ids:
                seen.update(str(r.get("id")) for r in page)
                if stop_ids <= seen:
                    return rows, False  # every order asked for is on these pages: the rest is not read
            after = str(cursor)
        return rows, False

    async def orders(self, status: str = "OPEN", stop_ids: set | None = None, max_pages: int | None = None, **params: Any) -> list[dict]:
        """Every order of a status: the pages are followed by their cursor, so a long list is never cut off (an
        open order missing from a cut list would look cancelled); ``orders_complete`` says whether it was. With
        ``stop_ids`` / ``max_pages`` the read stops early (paged) and the list is not whole."""
        rows, self.orders_complete = await self.paged("/orders", {"status": status, **{k: v for k, v in params.items() if v not in (None, "")}},
                                                      stop_ids=stop_ids, max_pages=max_pages)
        return rows

    async def order(self, order_id: str, hash_: str = "") -> dict | None:
        """One order, open or closed, by its hash: GET /v1/orders/{hash} is the API's only single-order read (there is
        no read by id, and its list filter takes no status but OPEN that we know of). None when Predict knows nothing of
        it (404). Without a hash only the open list can be searched, by id."""
        if hash_:
            try:
                data = await self.authed("GET", f"/orders/{core.urllib.parse.quote(str(hash_))}")
            except core.RemoteError as error:
                if http_status(error) == 404:
                    return None
                raise
            if isinstance(data, dict) and data.get("success") is False:  # 1.122.0: not "Predict knows nothing of it"
                raise core.RemoteError("Predict 接口返回 success=false：" + (core.http_error_text(data) or core.brief_error(json.dumps(data, ensure_ascii=False), 120)))
            row = data.get("data", data) if isinstance(data, dict) else None
            if isinstance(row, list):
                row = next((r for r in row if isinstance(r, dict) and (order_hash_of(r) == hash_.lower() or (order_id and str(r.get("id")) == str(order_id)))), None)
            return row if isinstance(row, dict) and (row.get("id") is not None or row.get("order") or row.get("status")) else None
        if order_id:
            for row in await self.orders("OPEN"):
                if str(row.get("id")) == str(order_id):
                    return row
        return None

    async def positions(self) -> list[dict]:
        rows, self.positions_complete = await self.paged("/positions", {})  # every page: a long list cut short would hide positions (and redemptions)
        return rows

    async def positions_read(self, pages: int) -> tuple[list[dict], bool]:
        """The positions read ``pages`` pages deep at most, with this read's own answer: (rows, whole). The redeem's read
        (1.150.0, audit RDM-POSITIONS-CUT): positions() stops at ORDERS_PAGES_MAX (20 pages, 1000 rows at 50 a page) and
        the bot never burns a losing token, so on a busy account the list outgrows that — and a winner listed past the
        cut was never seen by any redeem round, while the check concluded 赢的都领完了. ORDERS_PAGES_MAX stays as it is
        for the open-order reads and the once-a-minute positions readers (twenty pages already cost seconds), and
        positions_complete stays theirs: this read neither raises their cap nor writes their flag."""
        return await self.paged("/positions", {}, cap=pages)


def order_hash_of(row: dict) -> str:
    inner = row.get("order") if isinstance(row.get("order"), dict) else {}
    return str(inner.get("hash") or row.get("hash") or row.get("orderHash") or "").lower()


def order_fill(row: dict, ordered_shares: float) -> tuple[float, str]:
    """(shares filled, status) of an order record: amountFilled / amount of the shares ordered (the record's unit is
    not assumed), the status upper-cased."""
    status = str(row.get("status") or "").upper()
    try:
        amount, filled = int(str(row.get("amount") or "0")), int(str(row.get("amountFilled") or row.get("filled") or "0"))
    except ValueError:
        try:
            amount, filled = int(float(row.get("amount") or 0)), int(float(row.get("amountFilled") or 0))
        except (TypeError, ValueError):
            return 0.0, status
    if amount <= 0:
        return (ordered_shares if status in FILLED_STATUSES else 0.0), status
    if filled <= 0 and status in FILLED_STATUSES:
        filled = amount  # 1.130.2: a record that says filled without the amount: the whole order, by its own word (not "一份都没成交")
    return min(ordered_shares, ordered_shares * filled / amount), status


FILLED_STATUSES = {"FILLED", "MATCHED", "COMPLETED", "DONE"}  # a record ending so is every share filled, whatever amount it states


FINAL_STATUSES = {"FILLED", "CANCELLED", "CANCELED", "EXPIRED", "INVALIDATED", "REJECTED", "FAILED", "MATCHED", "CLOSED",
                  "COMPLETED", "DONE", "SETTLED", "RESOLVED", "UNKNOWN"}
OPEN_STATUSES = {"OPEN", "PENDING", "ACTIVE", "LIVE", "NEW", "PARTIAL", "PARTIALLY_FILLED", "PARTIALLY FILLED", "RESTING"}


def order_final(row: dict, ordered_shares: float) -> bool:
    """Whether a record is Predict's last word on the order: a final status, or every share filled. An open status,
    or a word this code does not know with shares still unfilled, keeps the order tracked (never ended by guess)."""
    status = str(row.get("status") or "").upper()
    if status in OPEN_STATUSES:
        return False
    if status in FINAL_STATUSES:
        return True
    filled, _ = order_fill(row, ordered_shares)
    return ordered_shares > 0 and filled >= ordered_shares - 1e-9


def order_price(row: dict) -> float | None:
    """The average price the record states for its fills (several APIs' spellings), None when it states none."""
    for key in ("averagePrice", "avgPrice", "priceFilled", "filledPrice", "executedPrice", "averageFillPrice"):
        value = row.get(key)
        if value in (None, "", 0, "0"):
            continue
        with contextlib.suppress(TypeError, ValueError, decimal.InvalidOperation):
            price = D(str(value))
            if price > 1:
                price = price / WEI
            if 0 < price <= 1:
                return float(price)
    return None


# --- BNB chain (JSON-RPC): balances, allowances, approvals, redeeming -----------------------------------------------------
# 1.150.0 (audit RDM-REVERT-REASON-DROPPED): the custom errors both SDKs' ABIs declare (ConditionalTokens, the yield-bearing
# ConditionalTokens, the NegRiskAdapter, Kernel), by their 4-byte selector — what a redeem that reverts in simulation names.
REVERT_ERRORS = {
    "0xe237d922": "ERC1155MissingApprovalForAll：结果代币没授权给调用的合约",
    "0x03dee4c5": "ERC1155InsufficientBalance：链上份额少于要领的数量",
    "0xcd7769ff": "NotApprovedForAll：多结果适配器没有结果代币的操作权",
    "0x1c1f9e2e": "NotAllowedToTransfer：合约不允许这次转移",
    "0xfc220038": "Insolvent：收益金库资不抵债",
    "0x855bef1a": "VTokenCallFailed：收益金库（Venus）取款失败",
    "0xe2517d3f": "AccessControlUnauthorizedAccount：合约拒绝了这个调用方",
    "0x48f5c3ed": "InvalidCaller：Predict 账户不接受这个签名钱包的调用",
}


def rpc_error_text(error: Any) -> str:
    """A JSON-RPC error object in words (after 「RPC 错误：」): the node's own message, cut at 120 characters, and — for a
    revert carrying a custom error — its 4-byte selector with the error's name when both SDKs' ABIs declare it.

    1.150.0 (audit RDM-REVERT-REASON-DROPPED): only ``message`` was kept and ``data`` dropped, so a redeem reverting in
    simulation with a custom error read the bare 「RPC 错误：execution reverted」 whatever the cause — a missing adapter
    approval, an amount over the balance, the yield vault — on a node that keeps the revert bytes in ``data`` alone (a
    BSC_RPC_URL of that kind); BNB Chain's own client (1.5.16+) repeats them after the message, where they showed as raw hex
    cut at 120 characters. Such a refusal never leaves the bot, so there is no transaction to look up on bscscan: this text
    is all the owner has. The selector goes after the cut, so the cut never takes it; the node's message, a reason string
    (Error(string): 「…: result for condition not received yet」), a Panic and every non-revert text (nonce too low,
    insufficient funds…) read as before. Never raises (only isinstance tests): an exception that is not a RemoteError here
    would turn a five-minute retry into an hour's."""
    if not isinstance(error, dict):
        return core.brief_error(str(error), 120)
    text = str(error.get("message") or (f"code {error['code']}" if error.get("code") is not None else "?"))
    data = error.get("data")
    if isinstance(data, dict):  # (some clients nest it: {"data": "0x…"})
        data = data.get("data")
    blob = data if isinstance(data, str) and re.fullmatch(r"0x[0-9a-fA-F]*", data) else ""
    if blob:
        text = text.removesuffix(": " + blob)  # the BNB Chain client repeats the raw data after the message
    elif found := re.fullmatch(r"(execution reverted):\s*(0x[0-9a-fA-F]{8,})", text):
        text, blob = found.group(1), found.group(2)  # the data in the message alone
    selector = blob[:10].lower()
    if len(selector) < 10 or selector in {"0x08c379a0", "0x4e487b71"}:  # none, Error(string), Panic: the message has the words
        return core.brief_error(text, 120)
    return core.brief_error(text, 120) + f"（合约错误 {selector}" + (f" {REVERT_ERRORS[selector]}" if selector in REVERT_ERRORS else "") + "）"


class RpcRefused(core.RemoteError):
    """The node was reached and ANSWERED with a JSON-RPC error object — unlike a request that failed on the way (a timeout,
    an HTTP status, a body that is no JSON), after which nobody knows what the node did. Its text is the bot's usual
    「RPC 错误：…」 (so every ``except core.RemoteError`` and every message reads as before); ``said`` is the node's own
    message, uncut (1.150.0, audit RDM-UNSENT-AS-SENT: Chain.send tells a raw transaction the node refused from one whose
    answer was lost)."""

    def __init__(self, message: str, said: str = ""):
        super().__init__(message)
        self.said = said


# The node's answers to eth_sendRawTransaction that mean it did NOT take the transaction (go-ethereum / BNB Chain's pool
# errors): nothing was broadcast, nothing spent (1.150.0, audit RDM-UNSENT-AS-SENT). "already known" and "nonce too low"
# are not here: the first is this very transaction already in the node, the second may be (Chain.send looks it up).
TX_REFUSED = ("insufficient funds", "underpriced", "intrinsic gas too low", "exceeds block gas limit", "txpool is full")


class TxNotSent(core.RemoteError):
    """A transaction that never left the bot: its nonce or gas price could not be read, or its gas not estimated (a
    revert in simulation — a market Predict shows resolved before the chain does), or the node answered the raw
    transaction with a refusal (TX_REFUSED: no funds for the gas, underpriced…). Nothing spent: tried again soon."""


class TxPending(core.RemoteError):
    """A transaction that was sent but whose receipt did not come within the wait (Chain.wait): it may still be mined —
    never a reason to send it again soon (1.150.0, audit RDM-ROUND-LOCK-STALL: a redeem round stops sending after one).
    Its text is the wait's own; ``tx_hash`` the transaction's."""

    def __init__(self, message: str, tx_hash: str = ""):
        super().__init__(message)
        self.tx_hash = tx_hash


class TxReverted(core.RemoteError):
    """A transaction that was mined and reverted (receipt status 0): its gas is spent and nothing more will come of it — a
    new attempt is a new transaction (1.150.0, audit RDM-RESTART-MID-SEND: the redeem's sent note is dropped). Its text
    (交易失败（已回滚）：<hash>) and its class (a RemoteError) read as before for every caller. ``fee``: what it cost the
    signer in wei by its receipt (Chain.receipt_fee; None when the receipt does not say — GAS-STATS)."""

    def __init__(self, message: str, tx_hash: str = "", fee: int | None = None):
        super().__init__(message)
        self.tx_hash = tx_hash
        self.fee = fee


class Sent(str):
    """A transaction Chain.send signed: its hash (a str, as before), with ``gas_price`` — the price per gas it was signed with,
    what its fee is reckoned at when the receipt states no effectiveGasPrice (1.150.0, GAS-STATS)."""
    gas_price: int | None = None

    def __new__(cls, tx_hash: str, gas_price: int | None = None):
        made = super().__new__(cls, tx_hash)
        made.gas_price = gas_price
        return made


# PayoutRedemption, as both SDKs' ABIs declare it: on the conditional tokens contracts (redeemer, collateral, parent indexed;
# data conditionId, indexSets, payout) and on the multi-outcome adapters (redeemer, conditionId indexed; data amounts, payout)
PAYOUT_TOKENS_TOPIC = "0x2682012a4a4f1973119f1c9b90745d1bd91fa2bab387344f044cb3586864d18d"
PAYOUT_ADAPTER_TOPIC = "0x9140a6a270ef945260c03894b3c6b3b2695e9d5101feef0ff24fec960cfd3224"


class Redeemed(str):
    """A mined redeem: its transaction hash (a str, as before), with ``payout`` — the USDT (wei) it paid the account by its
    receipt's PayoutRedemption, None when the receipt does not say (1.150.0, audit RDM-PAYOUT-UNCHECKED) — and ``fee``, the
    BNB (wei) its gas cost the signer by the same receipt, None when it does not say (1.150.0, GAS-STATS)."""
    payout: int | None = None
    fee: int | None = None

    def __new__(cls, tx_hash: str, payout: int | None = None, fee: int | None = None):
        made = super().__new__(cls, tx_hash)
        made.payout, made.fee = payout, fee
        return made


class RedeemUnread(core.RemoteError):
    """A redeem round could not read the account's positions (the key's rate-limit cooldown, a 429, a 5xx, a timeout):
    no position was judged and nothing was sent — the one failure of a round after which looking again soon is safe
    (1.150.0, audit RDM-ROUND-VOID). Its text and retry_after are the read's own."""


class Chain:
    """The few on-chain calls real trading needs, over plain JSON-RPC: USDT / BNB balances, the exchange allowances
    and operator approvals, and the transactions that set them or redeem resolved positions (from the signing wallet;
    through Kernel.execute for a Predict account)."""

    def __init__(self, rpc_url: str, wallet: Wallet):
        self.rpc_url, self.wallet = rpc_url, wallet
        self.addresses = wallet.addresses
        self.error = ""
        self.send_lock = asyncio.Lock()  # one transaction at a time: two senders reading the same pending nonce would collide

    async def rpc(self, method: str, params: list) -> Any:
        try:
            raw = await core._blocking(core._http_get, self.rpc_url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                                       RPC_TIMEOUT, {})
            data = json.loads(raw)
        except core.RemoteError as error:
            self.error = core.clean_error(error)
            raise
        except (json.JSONDecodeError, UnicodeDecodeError):
            self.error = "RPC 未返回有效 JSON"
            raise core.RemoteError(self.error) from None
        if isinstance(data, dict) and data.get("error"):
            self.error = f"RPC 错误：{rpc_error_text(data['error'])}"  # (RDM-REVERT-REASON-DROPPED) a custom error's selector kept
            said = data["error"].get("message") if isinstance(data["error"], dict) else data["error"]
            raise RpcRefused(self.error, str(said or ""))  # (RDM-UNSENT-AS-SENT) the node answered: still a RemoteError
        self.error = ""
        return data.get("result") if isinstance(data, dict) else None

    @staticmethod
    def encode(signature: str, types: list[str], args: list) -> bytes:
        _, _, _, _, eth_abi, eth_utils = _eth()
        return eth_utils.keccak(text=signature)[:4] + eth_abi.encode(types, args)

    async def call(self, to: str, data: bytes) -> bytes:
        result = await self.rpc("eth_call", [{"to": to, "data": "0x" + data.hex()}, "latest"])
        return bytes.fromhex(str(result or "0x").removeprefix("0x"))

    async def call_uint(self, to: str, signature: str, types: list[str], args: list) -> int:
        out = await self.call(to, self.encode(signature, types, args))
        return int.from_bytes(out[:32], "big") if out else 0

    async def usdt_balance(self, owner: str | None = None) -> int:
        return await self.call_uint(self.addresses["USDT"], "balanceOf(address)", ["address"], [owner or self.wallet.maker])

    async def bnb_balance(self, owner: str | None = None) -> int:
        return int(str(await self.rpc("eth_getBalance", [owner or self.wallet.signer, "latest"]) or "0x0"), 16)

    async def allowance(self, spender_key: str, owner: str | None = None) -> int:
        return await self.call_uint(self.addresses["USDT"], "allowance(address,address)", ["address", "address"],
                                    [owner or self.wallet.maker, self.addresses[spender_key]])

    async def account_owner(self, account: str) -> str:
        """The signer the ECDSA validator holds for a Predict account (0x000… when the account is unknown to it)."""
        out = await self.call(self.addresses["ECDSA_VALIDATOR"], self.encode("ecdsaValidatorStorage(address)", ["address"], [account]))
        return "0x" + out[12:32].hex() if len(out) >= 32 else ZERO_ADDRESS

    async def approved_for_all(self, token_key: str, operator_key: str, owner: str | None = None) -> bool:
        return bool(await self.call_uint(self.addresses[token_key], "isApprovedForAll(address,address)", ["address", "address"],
                                         [owner or self.wallet.maker, self.addresses[operator_key]]))

    async def send(self, to: str, data: bytes, value: int = 0, sent: Any = None) -> str:
        """A transaction from the signing wallet (to the Kernel account, which executes it, for a Predict account);
        the hash once the node accepted it. ``sent`` (a coroutine function): told the transaction's hash once it is signed,
        before it is broadcast — no broadcast if it raises. The hash given and returned is a Sent: a str carrying the gas
        price signed with (1.150.0, GAS-STATS)."""
        if self.wallet.predict_account:
            execution = bytes.fromhex(to[2:]) + value.to_bytes(32, "big") + data  # target ‖ value ‖ calldata
            data = self.encode("execute(bytes32,bytes)", ["bytes32", "bytes"], [bytes(32), execution])
            to, value = self.wallet.predict_account, 0
        sender = self.wallet.signer
        async with self.send_lock:
            try:
                nonce = int(str(await self.rpc("eth_getTransactionCount", [sender, "pending"])), 16)
                gas_price = int(str(await self.rpc("eth_gasPrice", [])), 16)
                estimate = await self.rpc("eth_estimateGas", [{"from": sender, "to": to, "data": "0x" + data.hex(), "value": hex(value)}])
                gas = int(str(estimate), 16) * 125 // 100
            except core.RemoteError as error:
                raise TxNotSent(str(error), error.retry_after) from error  # nothing sent, nothing spent
            except (TypeError, ValueError) as error:  # (RDM-UNSENT-AS-SENT) an answer that is no number (result: null): nothing sent either
                raise TxNotSent(f"RPC 回答的不是数字（{core.brief_error(str(error), 80)}）") from error
            tx = {"chainId": self.wallet.chain_id, "nonce": nonce, "gasPrice": gas_price, "gas": gas, "to": to,
                  "value": value, "data": "0x" + data.hex()}
            raw = self.wallet.sign_transaction(tx)
            tx_hash = Sent("0x" + _eth()[5].keccak(raw).hex(), gas_price)  # a transaction's hash is the keccak of its signed bytes
            if sent is not None:
                # 1.150.0 (audit RDM-RESTART-MID-SEND, RDM-RECEIPT-POLL): the caller's note of this transaction, on disk before
                # it is broadcast — a stop from here on (a deploy, a crash), a receipt that did not come in time or an answer
                # lost on the way leaves its hash behind, and the next round asks for its receipt before anything is sent
                # for that position again. No note, no broadcast.
                try:
                    await sent(tx_hash)
                except TxNotSent:
                    raise
                except Exception as error:
                    raise TxNotSent(f"交易记录没写下来，交易没发出（{core.clean_error(error) or type(error).__name__}）") from error
            # 1.150.0 (audit RDM-UNSENT-AS-SENT): the node's ANSWER to the raw transaction is told from a lost one. A refusal
            # (TX_REFUSED: no BNB for the gas, underpriced, a full pool) means it was never broadcast and cost nothing, yet it
            # counted as a sent failure — an hour's wait (README promised 5 分钟后再试 for 交易没发出去的) also when the wallet
            # was topped up minutes later. "already known": this very transaction is in the node — sent; its receipt decides.
            # "nonce too low": its nonce is used — by this transaction (a request delivered twice) or by another (a stale
            # pending-nonce read): one look-up by its hash says which. Anything else the node says, and a request that failed
            # on the way (timeout, HTTP 429 / 5xx, no JSON: it may have been taken), stays a sent failure as before. Never a
            # second eth_sendRawTransaction here: one transaction per call, the next round decides.
            try:
                answer = await self.rpc("eth_sendRawTransaction", ["0x" + raw.hex()])
            except RpcRefused as error:
                said = error.said.lower()
                if "already known" in said or "known transaction" in said:
                    return tx_hash
                if "nonce too low" in said:
                    if await self.rpc("eth_getTransactionByHash", [tx_hash]):  # (a failing read raises: unknown, an hour as before)
                        return tx_hash
                    raise TxNotSent(str(error), error.retry_after) from error
                if any(word in said for word in TX_REFUSED):
                    raise TxNotSent(str(error), error.retry_after) from error  # refused: nothing broadcast, nothing spent
                raise
            return Sent(str(answer), gas_price) if answer else tx_hash  # (a node answering null: the hash of what it was given)

    RECEIPT_POLL_SECONDS = 3.0  # the receipt is asked for this often while a sent transaction is awaited (Chain.wait)

    async def wait(self, tx_hash: str, timeout: float = 120.0) -> dict:
        """The receipt once mined; RemoteError when it reverted, TxPending when it did not come in time."""
        # 1.150.0 (audit RDM-RECEIPT-POLL): one poll that failed — a timeout, a dropped connection, HTTP 429 / 5xx, a body that is
        # no JSON, a JSON-RPC error answer: every JSON-RPC call is a POST, which the HTTP layer never repeats — ended the wait,
        # and the redeem of a transaction that had been sent (and was mined a block later) was booked as failed: no mark, no
        # 💵 line, a 没领成 alert and an hour's back-off (then, while Predict still listed the row, a second redeem that paid
        # nothing). A failed poll says nothing about the transaction: polled again, to the same deadline (at most 40 reads, no
        # transaction); after a 429 not sooner than the node asked. A revert still ends the wait at once; the deadline is
        # still TxPending (never "not sent": the transaction may still be mined), now saying why the receipt was not read.
        deadline = time.monotonic() + timeout
        trouble = ""
        while time.monotonic() < deadline:
            pause = self.RECEIPT_POLL_SECONDS
            try:
                receipt, trouble = await self.rpc("eth_getTransactionReceipt", [tx_hash]), ""
            except core.RemoteError as error:  # (only a RemoteError: a cancel at shutdown still ends the wait)
                receipt, trouble = None, core.clean_error(error) or type(error).__name__
                pause = max(pause, float(error.retry_after or 0))
            if isinstance(receipt, dict) and receipt.get("blockNumber"):
                if int(str(receipt.get("status") or "0x0"), 16) != 1:
                    # (a RemoteError, the same words: RDM-RESTART-MID-SEND) with what its gas cost all the same (GAS-STATS)
                    raise TxReverted(f"交易失败（已回滚）：{tx_hash}", tx_hash, self.receipt_fee(receipt, getattr(tx_hash, "gas_price", None)))
                return receipt
            await asyncio.sleep(max(0.0, min(pause, deadline - time.monotonic())))
        raise TxPending(f"交易 {tx_hash} 在 {int(timeout)} 秒内未上链" + (f"（回执读取失败：{core.brief_error(trouble, 80)}）" if trouble else ""),
                        tx_hash)  # (a RemoteError: every caller's except as before)

    @staticmethod
    def receipt_fee(receipt: Any, signed_price: Any = None) -> int | None:
        """What a mined transaction cost the signer, in wei, read off its receipt alone (no request): gasUsed ×
        effectiveGasPrice, or × ``signed_price`` (the price it was signed with: Sent.gas_price) where the receipt states no
        price. None when it cannot be told — no receipt, no gasUsed, a field that is no quantity: never taken for 0
        (1.150.0, GAS-STATS: the owner asked what the redeems cost a day)."""
        def quantity(value: Any) -> int | None:  # a JSON-RPC quantity ("0x…"), or our own whole number
            if isinstance(value, int) and not isinstance(value, bool):
                return value if value >= 0 else None
            text = str(value or "").strip().lower()
            return int(text, 16) if re.fullmatch(r"0x[0-9a-f]{1,64}", text) else None
        if not isinstance(receipt, dict):
            return None
        used = quantity(receipt.get("gasUsed"))
        price = quantity(receipt.get("effectiveGasPrice"))
        price = quantity(signed_price) if price is None else price
        return used * price if used is not None and price is not None else None

    async def receipt(self, tx_hash: str) -> dict | None:
        """One look at a sent transaction: its receipt once mined, None while it is not (no waiting); a failed read raises."""
        found = await self.rpc("eth_getTransactionReceipt", [tx_hash])
        return found if isinstance(found, dict) and found.get("blockNumber") else None

    async def known(self, tx_hash: str) -> bool:
        """Whether the node knows a transaction at all — pending in its pool or mined (eth_getTransactionByHash)."""
        return bool(await self.rpc("eth_getTransactionByHash", [tx_hash]))

    def approval_steps(self, yield_bearing: bool) -> list[tuple[str, str, str]]:
        """(label, kind, spender) for one market track, as the SDK's setApprovals sets them."""
        prefix = "YIELD_BEARING_" if yield_bearing else ""
        ct, ex, nex, adapter = (prefix + k for k in ("CONDITIONAL_TOKENS", "CTF_EXCHANGE", "NEG_RISK_CTF_EXCHANGE", "NEG_RISK_ADAPTER"))
        # 1.150.0 (audit RDM-YB-NEGRISK-APPROVAL): the multi-outcome exchange and the adapter are operators on the track's
        # MULTI-OUTCOME outcome tokens (both SDKs: _ctf_key(is_neg_risk=True, …) / getCtfIdentifier(true, …)). All three steps
        # named the binary tokens contract — the same address on the standard track, but on the yield-bearing one
        # YIELD_BEARING_CONDITIONAL_TOKENS (0x9400…) instead of YIELD_BEARING_NEG_RISK_CONDITIONAL_TOKENS (0xF64b…): --approve
        # signed two grants neither SDK sends and never the one a yield-bearing multi-outcome redeem needs, and --live-check
        # read OK for it (or MISSING on an account the website had approved correctly). The binary step stays on ``ct``.
        nct = tokens_key(True, yield_bearing)
        word = "收益型" if yield_bearing else ""
        return [(f"{word}结果代币 → 交易所", f"1155:{ct}", ex), (f"{word}结果代币 → 多结果交易所", f"1155:{nct}", nex),
                (f"{word}结果代币 → 多结果适配器", f"1155:{nct}", adapter),
                (f"{word}USDT 额度 → 交易所", "20", ex), (f"{word}USDT 额度 → 多结果交易所", "20", nex)]

    async def check_approvals(self, yield_bearing: bool = False) -> list[tuple[str, bool]]:
        out = []
        for label, kind, spender in self.approval_steps(yield_bearing):
            if kind == "20":
                ok = await self.allowance(spender) >= 10 ** 24  # a million USDT: effectively unlimited
            else:
                ok = await self.approved_for_all(kind.partition(":")[2], spender)
            out.append((label, ok))
        return out

    async def set_approvals(self, yield_bearing: bool = False, log: Any = None) -> list[tuple[str, str]]:
        """Every missing approval of one track, one transaction each; (label, outcome) per step."""
        out = []
        for label, kind, spender in self.approval_steps(yield_bearing):
            try:
                if kind == "20":
                    if await self.allowance(spender) >= 10 ** 24:
                        out.append((label, "已授权"))
                        continue
                    tx = await self.send(self.addresses["USDT"], self.encode("approve(address,uint256)", ["address", "uint256"],
                                                                            [self.addresses[spender], MAX_UINT256]))
                else:
                    token = kind.partition(":")[2]
                    if await self.approved_for_all(token, spender):
                        out.append((label, "已授权"))
                        continue
                    tx = await self.send(self.addresses[token], self.encode("setApprovalForAll(address,bool)", ["address", "bool"],
                                                                           [self.addresses[spender], True]))
                if log:
                    log(f"{label}：已发送 {tx}，等待上链…")
                await self.wait(tx)
                out.append((label, f"已完成 {tx}"))
            except Exception as error:
                out.append((label, f"失败：{core.clean_error(error) or type(error).__name__}"))
                break  # the next ones would fail the same way (no gas, wrong key); say so once
        return out

    def redeem_payout(self, receipt: Any, contract: str, neg_risk: bool) -> int | None:
        """The USDT (wei) a mined redeem paid the account: its receipt's PayoutRedemption emitted by ``contract`` (the one the
        redeem called) for this account as the redeemer — the event's last field, never the data's last word (that is an
        index set or an amount). None when there is no such log or it cannot be read: never taken for 0. A multi-outcome
        receipt also carries the tokens contract's event with the adapter as the redeemer: the filter leaves it out."""
        try:
            topic = PAYOUT_ADAPTER_TOPIC if neg_risk else PAYOUT_TOKENS_TOPIC
            redeemer = "0x" + "0" * 24 + self.wallet.maker[2:].lower()
            for log in (receipt.get("logs") or []) if isinstance(receipt, dict) else []:
                topics = [str(t).lower() for t in (log.get("topics") or [])] if isinstance(log, dict) else []
                if len(topics) < 2 or topics[0] != topic or topics[1] != redeemer or str(log.get("address") or "").lower() != contract.lower():
                    continue
                types = ["uint256[]", "uint256"] if neg_risk else ["bytes32", "uint256[]", "uint256"]
                return int(_eth()[4].decode(types, bytes.fromhex(str(log.get("data") or "0x").removeprefix("0x")))[-1])
        except Exception:  # an odd log: says nothing (None), never stops the redeem's bookkeeping
            return None
        return None

    async def outcome_balance(self, token: Any, neg_risk: bool, yield_bearing: bool, owner: str | None = None) -> int | None:
        """The account's on-chain balance of one outcome token: ERC-1155 balanceOf on the track's conditional tokens contract
        for that kind of market (the SDKs' pairing, tokens_key). None when the answer is shorter than a word; a failed read
        raises."""
        text = str(token).strip()
        token_id = int(text, 16) if text.lower().startswith("0x") else int(text)
        out = await self.call(self.addresses[tokens_key(neg_risk, yield_bearing)],
                              self.encode("balanceOf(address,uint256)", ["address", "uint256"], [owner or self.wallet.maker, token_id]))
        return int.from_bytes(out[:32], "big") if len(out) >= 32 else None

    ADAPTER_MISSING = ("多结果适配器没有这个账户结果代币的操作权（授权缺失），领取交易发不出去：运行 python main.py --approve"
                       "（补上这项授权，签名钱包付一笔 gas），或在 Predict 网站上领取一次")

    async def adapter_unapproved(self, yield_bearing: bool) -> bool:
        """True only when the chain says so: the track's multi-outcome adapter is no operator of this account's multi-outcome
        outcome tokens (isApprovedForAll(maker, adapter) on tokens_key(True, …) reads 0 — the one approval both SDKs' REDEEM
        scope names for a neg-risk market). A failed or empty read says nothing (False)."""
        try:
            out = await self.call(self.addresses[tokens_key(True, yield_bearing)],
                                  self.encode("isApprovedForAll(address,address)", ["address", "address"],
                                              [self.wallet.maker, self.addresses[("YIELD_BEARING_" if yield_bearing else "") + "NEG_RISK_ADAPTER"]]))
        except Exception:
            return False
        return len(out) >= 32 and int.from_bytes(out[:32], "big") == 0

    def redeem_target(self, neg_risk: bool, yield_bearing: bool) -> str:
        """The contract a redeem calls: the track's multi-outcome adapter for a neg-risk market, its conditional tokens else."""
        return self.addresses[("YIELD_BEARING_" if yield_bearing else "") + ("NEG_RISK_ADAPTER" if neg_risk else "CONDITIONAL_TOKENS")]

    async def redeem(self, condition_id: str, index_set: int, amount: int, neg_risk: bool, yield_bearing: bool, sent: Any = None) -> Redeemed:
        """Redeem a resolved position: the conditional tokens contract for a standard market (index set 1 or 2), the
        multi-outcome adapter (an amount per outcome) for a neg-risk one. The transaction hash once mined, with what it
        paid (Redeemed.payout) and what its gas cost (Redeemed.fee), both from the receipt: no extra request. ``sent``:
        Chain.send's, told the hash before broadcast."""
        condition = bytes.fromhex(condition_id.removeprefix("0x"))
        target = self.redeem_target(neg_risk, yield_bearing)
        if neg_risk:
            amounts = [amount, 0] if index_set == 1 else [0, amount]
            try:
                tx = await self.send(target, self.encode("redeemPositions(bytes32,uint256[])", ["bytes32", "uint256[]"], [condition, amounts]), sent=sent)
            except TxNotSent as error:
                # 1.150.0 (audit RDM-NEGRISK-ADAPTER-UNCHECKED): a multi-outcome redeem goes through the adapter, which pulls the
                # winning tokens from the account — it needs the account's ERC-1155 approval (both SDKs' REDEEM scope), which
                # nothing in the running bot read or set: missing, the estimate reverted every round for ever as the bare
                # 「RPC 错误：execution reverted」. Read once, only AFTER such a failure (a redeem that goes through reads nothing;
                # a gate before the send would rest on an assumption about Predict's contract), and named when the chain says
                # it is missing. Never sent from here — it is a permission grant and a transaction the owner chooses
                # (python main.py --approve sets it). Still TxNotSent: the five-minute retry, no mark.
                if not any(word in str(error).lower() for word in TX_REFUSED) and await self.adapter_unapproved(yield_bearing):
                    raise TxNotSent(f"{self.ADAPTER_MISSING}（{error}）", error.retry_after) from error
                raise
        else:
            tx = await self.send(target, self.encode("redeemPositions(address,bytes32,bytes32,uint256[])",
                                                     ["address", "bytes32", "bytes32", "uint256[]"],
                                                     [self.addresses["USDT"], bytes(32), condition, [index_set]]), sent=sent)
        receipt = await self.wait(tx)
        return Redeemed(tx, self.redeem_payout(receipt, target, neg_risk), self.receipt_fee(receipt, getattr(tx, "gas_price", None)))


# --- the trader: wallet, API and chain together, with their readiness ---------------------------------------------------
@dataclasses.dataclass
class MarketInfo:
    """What an order needs to know about its market, from GET /v1/markets/{id}."""
    market_id: str
    outcomes: list[dict]  # [{"name", "index_set", "token"}] in index order
    fee_bps: int | None
    neg_risk: bool
    yield_bearing: bool
    condition_id: str
    status: str
    at: float  # monotonic


def parse_market(market: dict, default_fee: int) -> MarketInfo:
    rows = sorted((o for o in market.get("outcomes") or [] if isinstance(o, dict)), key=lambda o: int(o.get("indexSet") or 0))
    outcomes = [{"name": str(o.get("name") or ""), "index_set": int(o.get("indexSet") or i + 1),
                 "token": str(o.get("onChainId") or o.get("tokenId") or o.get("token_id") or "")} for i, o in enumerate(rows)]
    fee = None
    with contextlib.suppress(TypeError, ValueError):
        stated = int(market.get("feeRateBps"))
        fee = stated if 0 <= stated <= 10_000 else None
    return MarketInfo(str(market.get("id") or ""), outcomes, fee if fee is not None else default_fee,
                      bool(market.get("isNegRisk")), bool(market.get("isYieldBearing")), str(market.get("conditionId") or ""),
                      str(market.get("status") or ""), time.monotonic())


def outcome_for_side(kind: str, side: str, outcomes: list[dict], touch_spec: Any = None) -> tuple[dict | None, str]:
    """The outcome (name, index set, token) a paper trade's side buys, and a note when it was assumed from the order
    of the outcomes rather than read from their names — or, for a daily index / stock card (kind "close"), when the
    names put the side at the other place than the book the card was priced from. The card prices the 涨 / Yes /
    high-barrier side first; its 跌 is the other outcome, bought as its own token (never a sale of the first)."""
    if len(outcomes) != 2 or not all(o.get("token") for o in outcomes):
        return None, "市场不是两个结果，或缺少结果代币编号"
    names = [str(o.get("name") or "").strip().lower() for o in outcomes]
    if kind == "touch" and touch_spec is not None:
        want = "high" if side == "up" else "low"
        hits = [o for o, n in zip(outcomes, names) if core.touch_outcome(n, touch_spec) == want]
        return (hits[0], "") if len(hits) == 1 else (None, f"结果名称无法对应触价线：{'、'.join(names)}")
    ups = [o for o, n in zip(outcomes, names) if n in UP_WORDS]
    downs = [o for o, n in zip(outcomes, names) if n in DOWN_WORDS]
    hit = None  # the side's outcome by the names: the one named for it, else the other one of the pair
    if side == "up":
        if len(ups) == 1:
            hit = ups[0]
        elif len(downs) == 1:
            hit = next(o for o in outcomes if o is not downs[0])
    else:
        if len(downs) == 1:
            hit = downs[0]
        elif len(ups) == 1:
            hit = next(o for o in outcomes if o is not ups[0])
    if hit is not None:
        if kind == "close" and hit is not outcomes[0 if side == "up" else 1]:
            # 1.150.0 (audit FEED-CLOSE-BOOK-ORIENT): a daily index / stock card reads Predict's book — which prices the
            # market's FIRST outcome — as 涨 just as it comes (Bot.sim_markets; every other kind turns its book by the
            # names: updown_book, touch_book, flip_yes_book, yes_book). On a market listed Down / No first the price, the
            # limit, the cap and the count would all be worked out on the other outcome's book while the token bought is
            # the one the name says, and with no note nothing refused the order: model and market agreeing at 68¢ for 涨
            # read as a 38¢ edge, a real BUY of Up at 30¢ (the audit's replay; no market has been listed that way so
            # far). The names say which token is which, and that the book is not this side's: noted, so never a real
            # order (live_room). The card itself holds such a book once the feed has read the names (Bot.close_book);
            # this is the trader's own read of them, for when the feed has not
            return hit, f"卡片把第一个结果的盘口当作涨，但按名称第一个结果不是涨（结果名称：{'、'.join(names)}）"
        return hit, ""
    if kind in {"close", "updown"}:  # a daily book prices its first outcome as 涨 (see resolution_up)
        return outcomes[0 if side == "up" else 1], f"按结果顺序推定（结果名称：{'、'.join(names)}）"
    return None, f"结果名称无法对应方向：{'、'.join(names)}"


class LiveTrader:
    """The pieces real trading needs, and whether they are ready: the wallet, the API sign-in, the chain reads."""

    def __init__(self, config: core.Config):
        self.config = config
        self.wallet = Wallet(config.live_key, config.live_chain, config.live_account)
        self.api = PredictApi(API_BASES[config.live_chain], config.predict_api_key, self.wallet)
        self.api.ORDERS_PAGE = config.live_orders_page  # LIVE_ORDERS_PAGE: fewer pages (a round trip each) when the account rests many orders
        self.chain = Chain(config.live_rpc or RPC_URLS[config.live_chain], self.wallet)
        self.ready_error = "尚未登录 Predict"
        self.ready_at = 0.0
        self.account_error = ""  # a Predict account the key does not control (checked on-chain): nothing is sent
        self.account_checked = False  # the on-chain check has succeeded (a plain wallet needs none): "no error" alone is not readiness
        self.account_reread = ""  # why the last RE-read of an account verified in this run failed (display only: verify_account, 1.150.0)
        self.balance: tuple[int, int, float] | None = None  # (USDT wei, BNB wei, when)
        self.balance_error = ""
        self.approvals: dict[str, bool] = {}  # exchange key -> allowance in place (read from the chain)
        self.approvals_at = 0.0  # wall clock, for display
        self.approvals_mono = -1e9  # monotonic, for the refresh interval
        self.approvals_error = ""
        self.markets: dict[str, MarketInfo] = {}

    @property
    def ready(self) -> bool:
        return not self.ready_error and not self.account_error and self.account_checked

    @property
    def signed_in(self) -> bool:
        """Predict's sign-in succeeded: enough to follow the orders already out — read their fills, send their cancels —
        which go through Predict's API alone (the sign-in names the order account, wallet.maker, as its signer: a session
        Predict accepted is that account's). New orders need ``ready`` (live_room, live_send_block). 1.150.0: the order
        management waited for ``ready`` too, whose on-chain account check is one BNB RPC read, made at a restart, /live
        check, /live mode and the not-ready retries — a node that timed out once there kept every withdrawal (the edge
        gone, the market's end, the loss stop, LIVE=off, a taker's rest) unsent and every fill unread until a retry a
        minute later found the node answering again, for the whole outage when it did not; a withdrawn order then rested
        where /live cancel all and the emergency stop no longer looked (they skip records already being cancelled)."""
        return not self.ready_error

    @property
    def not_ready_why(self) -> str:
        return self.ready_error or self.account_error or ("账户校验尚未完成" if not self.account_checked else "")

    async def verify_account(self) -> None:
        """PREDICT_ACCOUNT must be a Predict account whose signer is PREDICT_PRIVATE_KEY (the validator says who): a
        mismatch would have every order refused, so it blocks trading until fixed. A plain wallet needs no check."""
        if not self.wallet.predict_account:
            self.account_error, self.account_checked = "", True
            return
        try:
            owner = await self.chain.account_owner(self.wallet.predict_account)
        except Exception as error:
            why = core.clean_error(error) or type(error).__name__
            text = f"无法在链上核对 PREDICT_ACCOUNT 的控制钥匙：{why}"
            if self.account_checked:
                # 1.150.0 (audit RDM-CHECK-UNREADIES): a failed RE-read keeps the verification this run already made. The key that
                # controls the account does not change while the process runs, and nothing re-reads it on a timer — only the start,
                # /live check (the page's 自检), /live mode pause|on and the not-ready retries do. One timeout or a public node's
                # "limit exceeded" at the owner's own self-check un-readied a bot that traded a minute before: no new order or
                # pre-open quote, the redeem round starting in that minute voided (真实交易未就绪), a 🔴 未就绪 and a ✅ 登录成功
                # push a minute later when the step's retry found the node again (with SIM=off: the next redeem round's retry,
                # minutes on). The failure is still raised (live_prepare's 账户校验失败 note; live_ensure_ready withholds its
                # ✅) and said beside the ✓ (account_reread). A first verification that fails stays not ready, retried once a
                # minute; a read that ANSWERS — another owner, or none — un-readies as before, verified or not.
                self.account_reread = why
            else:
                self.account_error = text  # never verified in this run: not ready, whatever the sign-in said
            raise core.RemoteError(text) from None
        self.account_reread = ""
        if int(owner, 16) == 0:
            self.account_error = f"链上没有 Predict 账户 {short_addr(self.wallet.predict_account)} 的签名记录：地址填错，或账户还没在链上激活（先在网站交易一次）"
        elif owner.lower() != self.wallet.signer.lower():
            self.account_error = (f"PREDICT_PRIVATE_KEY（{short_addr(self.wallet.signer)}）不是 Predict 账户 {short_addr(self.wallet.predict_account)} 的控制钥匙"
                                  f"（链上记录的是 {short_addr(checksum(owner))}）：请导出该账户的 Privy 钱包私钥")
        else:
            self.account_error = ""
        self.account_checked = not self.account_error
        if self.account_error:
            raise core.RemoteError(self.account_error)

    async def sign_in(self) -> None:
        try:
            await self.api.ensure_auth()
            self.ready_error, self.ready_at = "", time.time()
        except Exception as error:
            self.ready_error = core.clean_error(error) or type(error).__name__
            if not self.config.predict_api_key and http_status(error) in {401, 403}:
                self.ready_error += "（可能需要 PREDICT_API_KEY）"
            raise

    async def refresh_balance(self) -> None:
        try:
            usdt, bnb = await asyncio.gather(self.chain.usdt_balance(), self.chain.bnb_balance())
            self.balance, self.balance_error = (usdt, bnb, time.time()), ""
        except Exception as error:
            self.balance_error = core.clean_error(error) or type(error).__name__
            raise

    async def refresh_approvals(self) -> None:
        try:
            # 1.150.0 (audit LIVE-YIELD-ALLOWANCE): the yield-bearing twins too. Orders on a yield-bearing market are signed
            # for YIELD_BEARING_*_EXCHANGE (exchange_key), and live_room's gate only refuses a key it has read: with just the
            # two standard keys an account lacking the yield-bearing allowance sent orders Predict / the chain then rejected,
            # over and over, while the page said approvals were fine.
            keys = [exchange_key(neg, yb) for yb in (False, True) for neg in (False, True)]
            found = await asyncio.gather(*(self.chain.allowance(k) for k in keys))
            self.approvals = {k: v >= 10 ** 20 for k, v in zip(keys, found)}  # ≥ 100 USDT of allowance counts as set
            self.approvals_at, self.approvals_mono, self.approvals_error = time.time(), time.monotonic(), ""
        except Exception as error:
            self.approvals_error = core.clean_error(error) or type(error).__name__
            raise

    async def market(self, market_id: str, max_age: float = 3600.0) -> MarketInfo:
        found = self.markets.get(str(market_id))
        if found and time.monotonic() - found.at < max_age:
            return found
        info = parse_market(await self.api.market(str(market_id)), self.config.predict_fee_bps)
        info.market_id = info.market_id or str(market_id)
        self.markets[str(market_id)] = info
        return info

    def usd(self) -> float | None:
        return from_wei(self.balance[0]) if self.balance else None

    def approvals_ok(self) -> tuple[bool, bool]:
        """(standard track, yield-bearing track) USDT allowances in place. 1.150.0: shown apart, so an account that never
        trades yield-bearing markets is not painted red for them (live_room refuses those markets on its own), while a
        missing yield-bearing allowance is no longer hidden behind a green ✓."""
        a = self.approvals
        return (bool(a.get("CTF_EXCHANGE")) and bool(a.get("NEG_RISK_CTF_EXCHANGE")),
                bool(a.get("YIELD_BEARING_CTF_EXCHANGE")) and bool(a.get("YIELD_BEARING_NEG_RISK_CTF_EXCHANGE")))

    def account_reread_note(self) -> str:
        """Beside the account check's ✓: the last re-read failed and this run's earlier verification stands (1.150.0, audit
        RDM-CHECK-UNREADIES) — the ✓ no longer sits silently above /live check's 账户校验失败 note."""
        return f"（最近一次重读没读到：{self.account_reread}，沿用本次运行早先的核对）" if self.account_reread and self.account_checked else ""

    def summary_lines(self) -> list[str]:
        c = self.config
        lines = [f"钱包：{self.wallet.kind}｜下单账户 {short_addr(self.wallet.maker)}"
                 + (f"｜签名钱包 {short_addr(self.wallet.signer)}" if self.wallet.predict_account else "")
                 + f"｜{CHAIN_NAMES.get(c.live_chain, c.live_chain)}",
                 ("API：已登录" + (f"（token 至 {core.stamp(self.api.jwt_exp * 1000, seconds=False)}）" if self.api.jwt_exp else "")
                  if not self.ready_error else f"API：未就绪：{self.ready_error}")
                 + ("" if c.predict_api_key else "｜未设置 PREDICT_API_KEY")]
        if self.wallet.predict_account:
            lines.append("账户校验：" + (f"✗ {self.account_error}" if self.account_error else "✓ 私钥是这个 Predict 账户的控制钥匙" + self.account_reread_note()))
        if self.balance:
            lines.append(f"余额：USDT {from_wei(self.balance[0]):,.2f}｜BNB {from_wei(self.balance[1]):.4f}（gas）"
                         f"｜读取于 {core.stamp(self.balance[2] * 1000)}")
        else:
            lines.append(f"余额：未读到（{self.balance_error or '尚未读取'}）")
        if self.approvals:
            std_ok, yb_ok = self.approvals_ok()
            lines.append("授权：交易所 USDT 额度 " + ("✓" if self.approvals.get("CTF_EXCHANGE") else "✗")
                         + "｜多结果交易所 " + ("✓" if self.approvals.get("NEG_RISK_CTF_EXCHANGE") else "✗")
                         + "｜收益型 " + ("✓" if yb_ok else "✗")
                         + ("" if std_ok else "（python main.py --approve，或在网站交易一次）")
                         + ("" if yb_ok or not std_ok else "（收益型市场不会真实下单，直到运行 python main.py --approve）"))
        else:
            lines.append(f"授权：未读到（{self.approvals_error or '尚未读取'}）")
        lines.append(f"风控：单笔 ≤${c.live_max_order_usd:g}｜持仓+挂单 ≤${c.live_max_open_usd:g}｜日亏损 ≤${c.live_max_daily_loss_usd:g}"
                     f"｜吃单 封顶限价单，{c.live_taker_wait} 秒未成交撤单"
                     f"｜自动领取 {'开' if c.live_auto_redeem else '关'}")
        if not c.live_allowed:
            lines.append("硬开关：部署变量 LIVE_TRADING_ALLOWED=off，不会真实下单（最多 pause）；数据库里保存的模式改不了它")
        return lines


    def summary_info(self) -> list[dict]:
        """The facts of summary_lines as label / value / tone rows (tone "", ok, warn or bad) for the page's grid."""
        c = self.config
        rows = [{"k": "钱包", "v": f"{self.wallet.kind}，下单账户 {short_addr(self.wallet.maker)}"
                 + (f"，签名钱包 {short_addr(self.wallet.signer)}" if self.wallet.predict_account else "")
                 + f"，{CHAIN_NAMES.get(c.live_chain, c.live_chain)}", "tone": ""}]
        if self.ready_error:
            rows.append({"k": "API", "v": f"未就绪：{self.ready_error}", "tone": "bad"})
        else:
            rows.append({"k": "API", "v": "已登录" + (f"，token 至 {core.stamp(self.api.jwt_exp * 1000, seconds=False)}" if self.api.jwt_exp else "")
                         + ("" if c.predict_api_key else "，未设置 PREDICT_API_KEY"), "tone": "ok" if c.predict_api_key else "warn"})
        if self.wallet.predict_account:
            rows.append({"k": "账户校验", "v": f"✗ {self.account_error}" if self.account_error else "✓ 私钥是这个 Predict 账户的控制钥匙" + self.account_reread_note(),
                         "tone": "bad" if self.account_error else "ok"})
        if self.balance:
            rows.append({"k": "余额", "v": f"USDT {from_wei(self.balance[0]):,.2f}，BNB {from_wei(self.balance[1]):.4f}（gas），"
                                          f"读取于 {core.stamp(self.balance[2] * 1000)}", "tone": ""})
        else:
            rows.append({"k": "余额", "v": f"未读到（{self.balance_error or '尚未读取'}）", "tone": "warn"})
        if self.approvals:
            ok, yb_ok = self.approvals_ok()
            rows.append({"k": "授权", "v": "交易所 USDT 额度 " + ("✓" if self.approvals.get("CTF_EXCHANGE") else "✗")
                         + "，多结果交易所 " + ("✓" if self.approvals.get("NEG_RISK_CTF_EXCHANGE") else "✗")
                         + "，收益型交易所 " + ("✓" if yb_ok else "✗")
                         + ("" if ok else "（python main.py --approve，或在网站交易一次）")
                         + ("" if yb_ok or not ok else "（收益型市场不会真实下单，直到运行 python main.py --approve）"),
                         "tone": "bad" if not ok else "ok" if yb_ok else "warn"})
        else:
            rows.append({"k": "授权", "v": f"未读到（{self.approvals_error or '尚未读取'}）", "tone": "warn"})
        rows.append({"k": "风控", "v": f"单笔 ≤${c.live_max_order_usd:g}，持仓+挂单 ≤${c.live_max_open_usd:g}，日亏损 "
                     + (f"≤${c.live_max_daily_loss_usd:g}" if c.live_max_daily_loss_usd else "不限")
                     + f"，吃单封顶限价单 {c.live_taker_wait} 秒未成交撤单，自动领取{'开' if c.live_auto_redeem else '关'}", "tone": ""})
        if not c.live_allowed:
            rows.append({"k": "硬开关", "v": "部署变量 LIVE_TRADING_ALLOWED=off，不会真实下单（最多 pause）；数据库里保存的模式改不了它", "tone": "warn"})
        return rows


def short_addr(address: str) -> str:
    return f"{address[:6]}…{address[-4:]}" if len(address) > 12 else address


def money(x: float) -> str:
    return f"{'+' if x >= 0 else '−'}${abs(x):,.2f}"


# --- the bot: the paper trader's decisions, sent to Predict ------------------------------------------------------------
PLACING = {"pending", "placing", "open", "cancelling", "unknown"}  # a trade whose order is in flight, resting, or possibly so (unknown: being reconciled)


_sim_status = core.sim_status


def sim_status(trade: dict) -> str:
    """The paper trader's status words, with the real order's state for a live trade."""
    st = trade.get("live") if isinstance(trade.get("live"), dict) else None
    shares, order = float(trade.get("shares") or 0), float(trade.get("order") or 0)
    tag = f"真实订单 #{st.get('order_id')}" if st and st.get("order_id") else "真实订单"
    if st and st.get("state") == "unknown":
        return f"状态未知，对账中（{tag} 已成交 {shares:g}/{order:g} 份；额度照旧占用）"
    if not st or trade.get("status") != "resting":
        return _sim_status(trade)
    state = st.get("state")
    if state == "pending":
        return "真实下单准备中"
    if state == "placing":
        return "真实下单发送中（待确认）"
    way = "挂单中" if trade.get("maker") else "吃单等待成交"
    if state == "cancelling":
        way = "撤单中"
    return f"{way}（{tag} 已成交 {shares:g}/{order:g} 份）"


core.sim_status = sim_status  # the base class looks the function up by name at call time


class LiveBot(core.Bot):
    """The paper trader, with every decision sent to Predict for real (see the module docstring)."""
    # open live orders are read back from Predict on every paper step (SIM_SECONDS)
    LIVE_BALANCE_SECONDS = 60   # the USDT / BNB balances are re-read this often while trading
    LIVE_APPROVALS_SECONDS = 600
    LIVE_REDEEM_SECONDS = 600   # resolved positions are looked for this often...
    LIVE_REDEEM_SOON_SECONDS = 20  # ...and this soon after a real position settled in the bot's favour (live_redeem_soon: the USDT back for the next quotes)
    LIVE_REDEEM_READS = 40      # markets read a redeem round at most, the longest unread first (1.150.0; the rest the next rounds): never a flood on the rate limit the orders share
    LIVE_REDEEM_PAGES = 200     # the redeem's own read of the positions, this many pages at most (10,000 rows at 50 a page: positions_read, 1.150.0);
    #                             a list longer still is said (the round's summary, /live redeem check, one notice an hour), never taken for the whole
    LIVE_REDEEM_RETRY_SECONDS = 3600  # a position whose redeem failed (gas, a revert) is tried again after this long, not every look...
    LIVE_REDEEM_UNSENT_RETRY_SECONDS = 300  # ...one whose transaction never left (TxNotSent: not resolved on-chain yet, an RPC blip) after this long...
    LIVE_REDEEM_UNSENT_FIRST_SECONDS = 60   # ...a position's first such try after this long (1.150.0: the chain is mostly a moment behind) — the wait only:
    #                                         the retry is the next round that comes anyway, none is brought for it (live_redeem_pass)
    LIVE_REDEEM_UNSENT_NAMED = 3  # ...and for its first this many such failures in a row its market stays named (live_redeem_first): the retry reads it first (1.150.0)
    LIVE_REDEEM_TICK_SECONDS = 5     # the automatic redeem's own timer (live_redeem_loop): rounds whatever the paper trader does
    LIVE_REDEEM_AGAIN_SECONDS = 30   # a round that could not look (the key cooling, the list unread, not signed in) or that a rate limit cut short:
    #                                  the next one this long after the key's cooldown ends, not LIVE_REDEEM_SECONDS on (live_redeem_again, 1.150.0)...
    LIVE_REDEEM_AGAIN_MAX = 3        # ...this many times in a row, the wait doubling (30, 60, 120 s); then the usual cadence again
    LIVE_REDEEM_STUCK_SECONDS = 1800  # no round has looked at the positions for this long: told on Telegram (key redeem:read), not only in /live
    LIVE_REDEEM_UNCLEAR_SECONDS = 1800  # a position whose market Predict calls settled with no winner readable, this long so: told (redeem:unclear);
    #                                     a named result without the settled word (it may be a proposal still) twelve times this long (1.150.0)
    LIVE_REDEEM_NOTE_SECONDS = 3600  # a failed redeem reaches Telegram at most this often
    REDEEM_REPORT_LINES = 6          # /live redeem check: the positions listed per kind (the rest counted)
    LIVE_REDEEM_KEPT_GRACE_MS = 30 * 60_000  # a row Predict still lists this long after its mined redeem is no list lag: the check says to look (1.150.0)
    REDEEM_CHECK_WAIT_SECONDS = 60   # /live redeem check waits this long at most for a round that holds the redeem lock, then answers without its dry run (1.150.0)
    LIVE_REDEEM_MIN_BNB_WEI = 5 * 10 ** 14  # under 0.0005 BNB no redeem goes out (one costs about 0.0002): one note, not a failure per position
    LIVE_NOTE_SECONDS = 600     # the same failure is announced at most this often
    LIVE_PLACING_GRACE_MS = 120_000  # a record left in "placing" is reconciled by its hash after this long
    LIVE_IDLE_LIST_MS = 60_000      # the open-order list is re-read this often even with nothing in flight (orders this bot did not place)
    LIVE_SNAPSHOT_MAX_AGE_MS = 600_000  # the pre-open mode's account snapshot stands in for a cut read of the open list while younger than this (1.136.2);
    #                                     with no whole view of the account for this long, the whole read is tried again even after a cut one (1.150.0)
    LIVE_STEP_PAGES = 1             # a step's own read of the open list: the pages until every active record's order is seen, this many at most (1.137.1:
    #                                 3; 1.138.2: 1 — under the pre-open mode's thousands of orders the model's older ones are never on the first pages, the
    #                                 two more pages cost 0.6 s a step for nothing: they are read by their hash in rotation)
    LIVE_TRACK_READS = 6            # resting maker orders the bounded list did not show, read by their hash a step at most, the longest unread first (1.137.2)
    LIVE_MARKET_READS = 10          # the step's open-order read by market (1.139.0): at most this many markets a step, else the bounded read
    LIVE_MARKET_PAGES = 4           # ...and at most this many pages a market once Predict is known to filter (a market holds a few of our orders)
    LIVE_TRACK_READ_MS = 15_000     # ...each at most this often: its fills are seen within this, not every step at two reads at a time
    LIVE_ORPHAN_GRACE_MS = 120_000  # an unrecorded open order on one of this bot's markets is withdrawn once seen so for this long (LIVE_ORPHAN_CANCEL)
    LIVE_ORPHAN_EVERY_MS = 60_000   # ...looked for this often
    LIVE_MISSES = 3             # an open order Predict stops listing without a final record counts as cancelled after this many looks...
    LIVE_MISS_WINDOW_MS = 60_000    # ...and never within a minute of its placement (Predict's reads may lag a fresh order)
    LIVE_CANCEL_RETRY_MS = 30_000   # a cancel Predict did not confirm, the order still listed (or open by its own record): asked again after this long
    LIVE_MARKET_RETRY_SECONDS = 60  # a market whose details could not be read is asked for again after this long, not every step
    LIVE_UNKNOWN_MARGIN_MS = 10 * 60_000  # an order of unknown state is given up (nothing can fill) this long after its expiry
    LIVE_UNKNOWN_LOOKS = 3                # ...or sooner (1.104.9): after this many complete open-order lists without it (the hash read 404ing too)...
    LIVE_UNKNOWN_SETTLE_MS = 3 * 60_000   # ...and this long unknown, once the account's positions show nothing it could have filled
    LIVE_UNEXPLAINED_EVERY_MS = 60_000    # the positions are read for an unknown record this often at most
    LIVE_SYNC_STALE_MS = 150_000    # no new order while the account's open orders were not read successfully for this long
    LIVE_DRAIN_SECONDS = 15         # at shutdown, how long the orders and cancels on their way may take to be answered (drain_orders)

    def __init__(self, config: core.Config, store: core.Store, market: Any, telegram: Any):
        super().__init__(config, store, market, telegram)
        self.live = LiveTrader(self.config)  # self.config: with the control page's saved changes applied
        self.handlers["/live"] = self.cmd_live
        self.live_backoff: dict[tuple[str, str], tuple[float, str]] = {}  # (market, side) -> (monotonic until, why)
        self.live_noted: dict[str, float] = {}
        self.live_misses: dict[str, int] = {}
        self.live_seq = 0
        self.live_note_buffer: dict[str, list[str]] = {}  # sub id -> unkeyed live notices waiting for that chat's one batch message (live_notify)
        self.live_note_dropped: dict[str, int] = {}       # sub id -> notices dropped from a full buffer since its last batch
        self.live_last_sync = 0.0
        self.live_last_redeem = -1e9
        self.live_last_balance = -1e9
        self.live_redeem_task: asyncio.Task | None = None   # the auto-redeem round, beside the step (chain calls never hold a decision)
        self.live_account_task: asyncio.Task | None = None  # the balances / approvals refresh, likewise
        self.live_redeem_failed: dict[str, float] = {}      # position key -> monotonic time its redeem failed
        self.live_redeem_lock = asyncio.Lock()               # one redeem round at a time (the command beside the automatic round)
        self.live_redeem_last: dict = {}                     # the last round (automatic or /live redeem): when, what it found, or why it could not look
        self.live_redeem_errors: dict[str, str] = {}         # position key -> why its last redeem failed (/live redeem check)
        self.live_redeem_unsent: dict[str, int] = {}         # position key -> its redeems in a row that never left (TxNotSent): LIVE_REDEEM_UNSENT_NAMED (1.150.0)
        self.live_redeem_read_round: dict[str, int] = {}     # market id -> the number of the redeem round that last read it (this process's rounds): the longest unread are read first (1.150.0)
        self.live_redeem_first: dict[str, float] = {}        # market id -> when a win there asked for a round (live_redeem_soon): read ahead of the rotation, once (1.150.0)
        self.live_redeem_small: dict[str, float] = {}        # position key -> the per-share payout of a resolved winner left under LIVE_REDEEM_MIN_USD: judged again with no market read (1.150.0)
        self.live_redeem_noted = -1e9                        # the last never-sent failed redeem told on Telegram (LIVE_REDEEM_NOTE_SECONDS)
        self.live_redeem_untold: dict[str, dict] = {}        # position key -> a sent redeem that failed, not told yet (the next ⚠️; 1.150.0)
        self.live_redeem_notes: dict[str, str] = {}          # position key, or gas / cut / unclear / round / loop -> its problem line (the check, 1.150.0)
        self.live_redeem_fields_told: set = set()            # (market, index set) of the 缺字段 positions told on Telegram (1.150.0)...
        self.live_redeem_fields_noted = -1e9                 # ...and when such a line last went to /live's 最近错误 (hourly)
        self.live_redeem_again_n = 0                         # early rounds in a row asked for by live_redeem_again (0 again once a round looked, 1.150.0)
        self.live_redeem_whole_at = time.monotonic()         # when a round last looked at the positions with no rate limit cutting it (the start counts)
        self.live_redeem_stuck_noted = -1e9                  # ...and when no such look for LIVE_REDEEM_STUCK_SECONDS was last told (hourly at most)
        self.live_redeem_loop_task: asyncio.Task | None = None
        self.live_gas_noted = -1e9
        self.live_redeem_since = 0        # when the redeem pass holding the lock began (bot clock, ms; 0: none) — /live and the check (1.150.0)
        self.live_redeem_auto_at = 0      # when the last automatic round began (bot clock, ms; 0: none yet): /live beside a manual 上一轮 (1.150.0)
        self.live_redeem_cut = 0          # the last redeem pass's own positions read: 0 = whole, else the rows it read of a longer list (1.150.0)
        self.live_cut_noted = -1e9        # ...and when such a cut list was last told on Telegram (an hour apart, as the gas)
        self.live_redeem_unclear: dict[str, float] = {}  # "<market>:<index set>" -> when a round first met its settlement unreadable (1.150.0)
        self.live_unclear_noted = -1e9    # ...and when such positions were last told on Telegram (an hour apart)
        self.live_step_ms: dict[str, int] = {}  # the last step's phases (预取 / 决策 / 下单 / 同步), for /diag
        self.live_fill_tasks: set = set()   # the fills reads of orders confirmed removed, beside the lane (live_fills_later)
        self.live_placing: set[str] = set()     # records whose order is being sent right now: the step and the express lane never both
        self.live_cancels: set[str] = set()     # order ids one path is removing right now, the request to Predict's final word (live_cancel): never two at once
        self.live_last_signin = -1e9
        self.live_positions: tuple[list[dict], float] = ([], 0.0)
        self.live_positions_cut = False  # the last positions read for a reconciliation came cut at the page cap
        self.live_positions_busy = ""    # ...or left a surplus that cannot be told from the pre-open mode's shares: why (1.150.0)
        self.live_late_checked = -1e9    # when the ended orders were last set against the account's positions (live_late_fills)
        self.live_trim_seen: dict[tuple[str, int], tuple[float, int]] = {}  # outcome -> (the holding's shortfall seen, when): trimmed once it stands a minute
        self.live_errors: list[str] = []  # the latest failures, newest last, for /live
        self.live_wanted: set[str] = set()  # market ids a decision waited for (details read next step, whatever the edge)
        self.live_market_retry: dict[str, float] = {}  # market id -> monotonic time its details are asked for again after a failure
        self.live_unknown_open: list[dict] = []  # open orders on the account this bot has no record of (manual, or lost)
        self.live_unknown_first: dict[str, int] = {}  # unrecorded order id -> when it was first seen so (live_orphan_sweep withdraws it after the grace, 1.136.2)
        self.live_unrecorded_rows: dict[str, dict] = {}  # ...and its row as the list showed it (its market, for the sweep's judgement and its line)
        self.live_orphans: list[dict] = []  # the unrecorded orders this bot withdrew ({"at", "id", "brief"}, newest last; /live)
        self.live_orphan_swept_ms = -10 ** 15
        self.live_open_ids: set[str] = set()  # the last complete open list's order ids, and when it was read: a record whose order is on it is not pruned (prunable)
        self.live_open_ids_at = 0
        self.live_full_at_ms = -10 ** 15  # when this bot last read the whole open list itself (a step reads a few pages otherwise: 1.137.1)
        self.live_full_cut = False  # that read came cut at the page cap: while the pre-open mode runs, its snapshot stands for the account instead (1.137.2;
        #                             1.150.0: tried again once nothing has shown the whole account for LIVE_SNAPSHOT_MAX_AGE_MS — live_sync_orders)
        self.live_market_filter: bool | None = None  # whether Predict's open-order list honours marketId (1.139.0): None until a read shows it either way
        self.live_whole: tuple[set, set, int] = (set(), set(), 0)  # the last whole view of the account's open orders: ids, hashes, when read (live_counts_miss)
        self.live_requote_looked: set[str] = set()  # order ids of confirmed cancels whose slot live_requote_freed has looked at (live_cancel_fills, 1.138.0)
        self.live_track_reads: dict[str, int] = {}  # record id -> when its order was last read by its hash in the sync (the rotation, 1.137.2)
        self.live_sync_detail: dict = {}  # the last sync's parts, for /diag: the list's pages and seconds, the hash reads, the ones left for later
        self.live_rate_samples: collections.deque = collections.deque(maxlen=130)  # (monotonic, the model trader's requests so far), each step
        self.live_synced_at = 0  # when the open-order list was last read in full
        self.live_sync_failed_ms = 0  # when a read of it last failed (a failure after the last success holds new orders)
        self.live_list_at_ms = -10 ** 15  # ...on the bot's clock, for the idle re-read
        self.live_auth_tried = -1e9  # monotonic time of the last sign-in renewal tried ahead of the token's expiry
        self.live_place_ms = 0       # the last step's placement time (sim_after_decide), for the phases
        self.preopen = None  # the pre-open quoting mode's controller while it runs (preopen_apply: PREOPEN_15M, on the page or in the environment)
        self.preopen_task: asyncio.Task | None = None
        self.preopen_day_read: tuple | None = None  # (ledger file, Beijing day, its accounted P&L, valid until): read while no controller holds it (preopen_file_day)
        self.preopen_tidied = -1e9  # monotonic time of the last archive pass over the ledger file while the mode is off (preopen_tidy)
        self.preopen_share: Any = None  # the archive's share of the review's figures, kept from call to call (preopen.ArchiveShare; preopen_review)
        self.preopen_factory: Any = None  # the tests' stand-in controller class
        self.preopen_lock = asyncio.Lock()
        self.handlers["/preopen"] = self.cmd_preopen  # the status, on / off, pause / resume, and the read-only probe of the listing

    # --- readiness -------------------------------------------------------------------------------------------------------
    async def live_prepare(self) -> list[str]:
        """Sign in, read the balances and the allowances; the report lines. Nothing here stops the bot: a failure is
        shown in /live and retried by the sync step."""
        for name, job in (("登录", self.live.sign_in), ("账户校验", self.live.verify_account), ("余额", self.live.refresh_balance),
                          ("授权", self.live.refresh_approvals)):
            try:
                await job()
            except Exception as error:
                self.live_note_error(f"{name}失败：{core.clean_error(error) or type(error).__name__}")
        self.live_last_signin = self.live_last_balance = time.monotonic()
        if self.live.ready:  # the account's open orders before any decision: no first order on an account never looked at
            try:
                await self.live_sync_orders({}, self.market.now_ms())
            except Exception as error:
                self.live_note_error(f"订单同步失败：{core.clean_error(error) or type(error).__name__}")
        return self.live.summary_lines()

    def live_note_error(self, text: str) -> None:
        LOG.warning("真实交易：%s", text)
        self.live_errors = [*self.live_errors[-9:], f"{core.stamp(self.market.now_ms(), seconds=False)} {text}"]

    def live_paused(self) -> str:
        record = self.store.get("live:paused")
        if isinstance(record, dict):
            return str(record.get("why") or "手动暂停")
        return ""

    def live_kill_record(self) -> dict:
        """Today's kill-switch record: tripped (why) or resumed by the administrator; {} on any other day."""
        record = self.store.get("live:killed")
        if isinstance(record, dict) and record.get("day") == core.beijing_day(self.market.now_ms() / 1000):
            return record
        return {}

    def live_killed(self) -> str:
        record = self.live_kill_record()
        return "" if not record or record.get("resumed") else str(record.get("why") or "触发日亏损上限")

    def live_loss_floor(self, cap: float) -> float:
        """The day's settled P&L (the model trader's and the pre-open mode's) at or under which LIVE_MAX_DAILY_LOSS_USD stops
        new orders: −cap. After /live resume lifted today's stop (1.150.0): a cap's worth under the loss the resume found —
        the loss the operator went on with is not counted again, the next cap's worth is. The resume used to take the stop
        away until midnight (Beijing) whatever was lost after it — −$294 in a day at a $50 cap — and a cap lowered after
        it changed nothing: the line follows the cap as it is set now. A day in profit at the resume keeps the plain line
        (never tighter than a day that had no stop). −inf for a record resumed by a version that kept no figure, until
        live_check_kill dates it from its next look."""
        record = self.live_kill_record()
        if not record.get("resumed"):
            return -cap
        found = record.get("resumed_pnl")
        return -cap + min(0.0, float(found)) if found is not None else -math.inf

    def live_resumed_floor(self) -> float | None:
        """For the status texts: the line the stop stands at once /live resume has lifted it today (live_loss_floor) — the
        day's P&L may be past the cap there with the trader running. None on a day without a resume, with no limit set, or
        while a record of an older version has no figure yet."""
        cap = self.config.live_max_daily_loss_usd
        if not cap or not self.live_kill_record().get("resumed"):
            return None
        floor = self.live_loss_floor(cap)
        return floor if math.isfinite(floor) else None

    # --- the gate: why a decision is not sent (recorded like a group-cap refusal) ------------------------------------
    def sim_group_room(self, trades: dict[str, dict], mk: core.SimMarket, side: str, price: float, shares: float,
                       taker: dict | None = None) -> str:
        why = super().sim_group_room(trades, mk, side, price, shares, taker)
        return why or self.live_room(trades, mk, side, price, shares, taker)

    def sim_group_order(self, mk: core.SimMarket, price: float, shares: float, taker: dict | None = None) -> tuple[float, float]:
        """A real taker's whole LIMIT order: every share asked for, at its limit plus the fee — what the book shows under
        the cap fills now, the rest waits at the limit and may fill too (1.83.0: one 50¢ share in the book let a
        100-share order past a $30 group cap)."""
        if self.config.live_mode == "off" or not taker or not taker.get("cap"):
            return super().sim_group_order(mk, price, shares, taker)
        limit = self.sim_taker_limit(taker)
        bps = mk.book.fee_bps if mk.book.fee_bps is not None else self.config.predict_fee_bps
        want = float(taker.get("want") or self.sim_taker_shares(mk))
        return max(float(price), limit + core.taker_fee(limit, bps)), max(float(shares), want)

    def sim_resting_cost(self, trade: dict) -> float:
        """A real taker's unfilled shares may fill up to its limit (plus the fee), not at the quote's average."""
        st = trade.get("live")
        if not isinstance(st, dict) or trade.get("maker"):
            return super().sim_resting_cost(trade)
        limit = self.live_unit_cap(trade)
        return max(float(trade.get("price") or 0), limit + core.taker_fee(limit, int((st.get("want") or {}).get("fee_bps") or 0)))

    def live_room(self, trades: dict[str, dict], mk: core.SimMarket, side: str, price: float, shares: float,
                  taker: dict | None = None) -> str:
        c = self.config
        if self.preopen_exclusive():
            return "开盘前固定双边预挂模式：普通模型策略不开新仓"
        if c.live_mode == "off":
            return ""  # paper trading, as the base class does it
        if not self.live.ready:
            return f"真实交易未就绪：{self.live.not_ready_why}"
        if gate := self.live_balance_gate():
            return gate
        if c.live_mode == "pause":
            return "LIVE=pause：只做登录、自检、挂单测试和撤单，不开新仓（改成 LIVE=on 才会下单）"
        if not c.live_allowed:
            return "部署变量 LIVE_TRADING_ALLOWED=off：不下真实单"
        if paused := self.live_paused():
            return f"真实交易已暂停（{paused}；/live resume 恢复）"
        if killed := self.live_killed():
            return f"今日停止开新仓：{killed}"
        # 1.138.0: by way — a maker's failure no longer shuts out the side's taker, nor a taker's its maker; (market, side) alone
        # (an operator's release) holds both
        held = max((h for h in (self.live_backoff.get((mk.market, side)), self.live_backoff.get((mk.market, side, "挂" if taker is None else "吃"))) if h),
                   key=lambda h: h[0], default=None)
        if held and time.monotonic() < held[0]:
            return f"{held[1]}，{int(held[0] - time.monotonic()) + 1} 秒后再试"
        busy = next((t for t in core.trades_of_market(trades, mk.market) if t.get("side") == side and bool(t.get("maker")) == (taker is None)
                     and isinstance(t.get("live"), dict) and t["live"].get("state") in {"pending", "placing", "cancelling", "unknown"}), None)
        if busy is not None:  # a re-placed resting order waits for the old one's cancel; named, so the page says which (1.101.2)
            st = busy["live"]
            word = {"pending": "等待发送", "placing": "发送中", "cancelling": "撤单待 Predict 确认", "unknown": "状态未知、对账中"}[st["state"]]
            since = int((st.get("events") or [{}])[-1].get("at") or 0)
            return ("这个市场这一边还有同类订单在发送中、撤单中或对账中，等它结束（"
                    + (f"#{st['order_id']} " if st.get("order_id") else "") + f"{core.cents(float(busy.get('price') or 0))}×{float(busy.get('order') or 0):g} 份，"
                    + word + (f"，自 {core.stamp(since, seconds=False)}" if since else "")
                    + (f"；撤单原因：{st['cancel_why']}" if st.get("state") == "cancelling" and st.get("cancel_why") else "") + "）")
        if hold := self.live_send_hold(self.market.now_ms()):
            return hold  # no new risk on an account whose orders were not read lately
        if taker and taker.get("cap"):
            price, shares = self.sim_taker_limit(taker), float(taker.get("want") or self.sim_taker_shares(mk))  # the LIMIT order asks for the count at its limit: the most it can cost
        notional = float(price) * float(shares)
        if notional < MIN_ORDER_USD - 1e-9:
            beta = self.sim_beta(mk.settle)  # the count that binds: a market in beta's own, else SIM_SHARES
            more = f"{beta.env}_SHARES" if beta is not None and float(shares) >= self.sim_order_cap(mk) - 1e-9 else "SIM_SHARES"
            return f"本单 ${notional:,.2f} 低于 Predict 最低订单金额 ${MIN_ORDER_USD:g}（提高 {more}）"
        if notional > c.live_max_order_usd + 1e-9:
            return f"单笔 ${notional:,.2f} 超过 LIVE_MAX_ORDER_USD ${c.live_max_order_usd:g}"
        exposure = self.live_exposure(trades)
        if exposure + notional > c.live_max_open_usd + 1e-9:
            return f"持仓+挂单 ${exposure:,.2f} 加本单 ${notional:,.2f} 超过 LIVE_MAX_OPEN_USD ${c.live_max_open_usd:g}"
        usd = self.live.usd()
        if usd is not None and usd < notional:
            return f"USDT 余额 ${usd:,.2f} 不足本单 ${notional:,.2f}"
        info = self.live.markets.get(str(mk.book.market_id))
        if info is None:
            self.live_wanted.add(str(mk.book.market_id))
            return "市场信息（结果代币、费率）尚未读到，稍后再试"
        if core.PredictFeed.market_closed({"status": info.status}):
            return f"Predict 市场状态 {info.status}，不再交易"
        outcome, assumed = outcome_for_side(mk.kind, side, info.outcomes, self.touches[mk.key].spec if mk.kind == "touch" and mk.key in self.touches else None)
        if outcome is None:
            return "无法确定要买的结果代币：" + assumed
        if assumed:
            return f"结果名称无法确认方向，不下真实单（{assumed}）"  # a guessed side is a paper matter, never a real order
        key = exchange_key(info.neg_risk, info.yield_bearing)
        if self.live.approvals and key in self.live.approvals and not self.live.approvals[key]:
            return f"未授权{'收益型' if info.yield_bearing else ''}{'多结果' if info.neg_risk else ''}交易所使用 USDT（python main.py --approve）"
        return ""

    def sim_taker_limit(self, q: dict) -> float:
        """A taker's LIMIT price: the dearest level its quote walks (LIVE_TAKER_PRICE=book: what was seen is the most
        paid; an ask that shows up dearer in the next second is left to the next look), or the cap (cap: the dearest
        acceptable price, so such asks are taken too)."""
        if self.config.live_taker_price == "book" and q.get("levels"):
            return float(q["levels"][-1][0])
        return float(q["cap"])

    @staticmethod
    def live_unit_cap(trade: dict) -> float:
        """The most a share of this order can cost: a maker's price, a taker's limit (the price the order was signed at)."""
        st = trade.get("live") if isinstance(trade.get("live"), dict) else {}
        if trade.get("maker"):
            return float(trade.get("price") or 0)
        want = st.get("want") or {}
        return float(want.get("limit") or want.get("cap") or trade.get("cap") or st.get("price_per_share") or trade.get("price") or 0)

    def live_reserved(self, trade: dict) -> float:
        """USDT this record ties up: the shares filled, at their price, plus — while the real order may still fill
        (pending, placing, open, cancelling: until Predict's final word), or its fills are not read back yet after a
        confirmed cancel (fills_pending, 1.122.0) — every unfilled share at the order's cap. A withdrawal releases nothing
        by itself; the confirmed cancel does, once what it filled is known."""
        st = trade.get("live")
        if not isinstance(st, dict):
            return 0.0
        shares, order = float(trade.get("shares") or 0), float(trade.get("order") or 0)
        held = shares * float(trade.get("price") or 0) if trade.get("status") in {"filled", "resting", "cancelled"} else 0.0
        if st.get("state") in PLACING or trade.get("status") == "resting" or core.fills_pending(st):  # on the book, or possibly so: it may still fill —
            held += max(0.0, order - shares) * self.live_unit_cap(trade)  # or cancelled with its fills not read yet (1.122.0)
        return held

    def live_exposure(self, trades: dict[str, dict]) -> float:
        """USDT the real records tie up (live_reserved summed): positions at their price, every order still able to
        fill at its cap."""
        return sum(self.live_reserved(t) for t in trades.values())

    def live_fit_shares(self, trades: dict[str, dict], price: float) -> float:
        """The most shares the real-trading limits take at ``price`` per share (a maker's price, a taker's cap): the
        order limit, what LIVE_MAX_OPEN_USD leaves beside the open records, the USDT balance; 0 when what fits is
        under Predict's minimum order. No limit on paper (LIVE=off)."""
        c = self.config
        if c.live_mode == "off" or price <= 0:
            return math.inf
        room = min(c.live_max_order_usd, c.live_max_open_usd - self.live_exposure(trades))  # 1.132.1: the pre-open's reservation is its own cap's
        usd = self.live.usd()
        if usd is not None:
            room = min(room, usd)
        return room / price if room >= MIN_ORDER_USD - 1e-9 else 0.0

    def live_fit_why(self, trades: dict[str, dict], price: float) -> str:
        """Why not even a share fits the real-trading limits at ``price``: the exposure limit (LIVE_MAX_OPEN_USD less the
        open records) or the USDT balance leaves less than Predict's minimum order; "" when something fits (1.92.1)."""
        c = self.config
        if c.live_mode == "off" or price <= 0:
            return ""
        exposure = self.live_exposure(trades)
        left = c.live_max_open_usd - exposure
        if left < MIN_ORDER_USD - 1e-9:
            return f"持仓+挂单 ${exposure:,.2f} 已占满 LIVE_MAX_OPEN_USD ${c.live_max_open_usd:g}（剩 ${max(left, 0.0):,.2f}，不够一份）"
        usd = self.live.usd()
        if usd is not None and usd < MIN_ORDER_USD - 1e-9:
            return f"USDT 余额 ${usd:,.2f} 不够一份"
        if c.live_max_order_usd < MIN_ORDER_USD - 1e-9:
            return f"LIVE_MAX_ORDER_USD ${c.live_max_order_usd:g} 不够一份"
        return ""

    def live_daily_pnl(self, trades: dict[str, dict], now_ms: int) -> float:
        """Today's settled P&L of the real records (the Beijing day; the loss stop's figure): every record settled today, at
        its result as it stands. 1.150.0: plus what a result corrected TODAY took from a record settled on an earlier day —
        a position pre-settled on the bot's own data before midnight (a win, counted that day) that Predict resolves the
        other way after it keeps its first settlement's time (sim_settle: the journal's), so the correction's loss fell on a
        day already over and no day's stop counted it. Only what today's corrections took away (their net, a record's
        revisions of today together), never what they gave back: a correction in the bot's favour of yesterday's record
        gives today's stop no room."""
        today = dt.datetime.fromtimestamp(now_ms / 1000, core.BEIJING).replace(hour=0, minute=0, second=0, microsecond=0)
        since = int(today.timestamp() * 1000)
        total = 0.0
        for t in trades.values():
            if not isinstance(t.get("live"), dict) or t.get("status") != "settled":
                continue
            shares = float(t["shares"])
            if int(t.get("settled") or 0) >= since:
                total += (float(t["payout"]) - float(t["price"])) * shares
            else:  # settled before today: the payout's change today (its revisions carry their own time), when it is a loss
                moved = sum(float(r.get("to") or 0) - float(r.get("from") or 0) for r in t.get("revisions") or ()
                            if isinstance(r, dict) and int(r.get("at") or 0) >= since)
                total += min(0.0, moved) * shares
        return total

    def live_check_kill(self, trades: dict[str, dict], now_ms: int) -> None:
        """The daily loss stop: tripped when the day's settled P&L is at −LIVE_MAX_DAILY_LOSS_USD or under — new orders
        stop until midnight (Beijing), the resting ones are withdrawn. 1.150.0: lifted by hand (/live resume), it is armed
        again a cap's worth further down (live_loss_floor): the losses the resume found do not trip it again (the operator
        went on with them), the next cap's worth does — as the first did, and another resume lifts that one in turn. A day
        with a record used to be skipped whole, tripped or resumed: one resume, and no loss of any size stopped the trader
        until midnight."""
        cap = self.config.live_max_daily_loss_usd
        record = self.live_kill_record()
        if not cap or (record and not record.get("resumed")):
            return  # no limit set; or tripped today and standing
        pnl = self.live_daily_pnl(trades, now_ms) + self.preopen_pnl()  # the pre-open mode's day counted too (1.125.0)
        if record and record.get("resumed_pnl") is None:  # resumed by a version that kept no figure: counted from this look on
            self.store.put("live:killed", {**record, "resumed_pnl": pnl})
            return
        if pnl <= self.live_loss_floor(cap):
            why = f"今日已结算亏损 {money(pnl)} 达到 LIVE_MAX_DAILY_LOSS_USD ${cap:g}"
            if record:  # the second stop of a day says what the first one's resume found, and what went since
                found = float(record["resumed_pnl"])
                why += f"，手动恢复时 {money(found)}、之后又亏 ${found - pnl:,.2f}"
            self.store.put("live:killed", {"day": core.beijing_day(now_ms / 1000), "why": why, "at": now_ms})
            for tid, t in trades.items():
                if isinstance(t.get("live"), dict) and t["status"] == "resting" and t["live"].get("state") in {"open", "placing", "unknown"}:
                    self.sim_withdraw(t, "触发日亏损上限", now_ms)
                    self.sim_save([(tid, t)])
            self.live_notify(f"🛑 {why}；今天不再开新仓，已请求撤掉挂单（以 Predict 的确认为准）。/live resume 可手动恢复。", key="kill")

    # --- opening: the paper record becomes an order to place ----------------------------------------------------------
    def sim_paper(self) -> bool:
        """Paper buys only with LIVE=off (sim_open below). Otherwise the fills are Predict's and the book is the real one:
        what earlier paper fills took of it (Bot.sim_paper_book, 1.150.0) is never taken out of a real decision."""
        return self.config.live_mode == "off"

    def sim_open(self, mk: core.SimMarket, side: str, now_ms: int, maker: Any = None, taker: dict | None = None,
                 shares: float | None = None) -> dict:
        trade = super().sim_open(mk, side, now_ms, maker, taker, shares)
        if self.config.live_mode == "off":
            return trade  # a paper record, filled and settled as the base class does
        info = self.live.markets[str(mk.book.market_id)]
        spec = self.touches[mk.key].spec if mk.kind == "touch" and mk.key in self.touches else None
        outcome, assumed = outcome_for_side(mk.kind, side, info.outcomes, spec)
        settle = mk.settle or {}
        end = int(settle.get("close_ms") or settle.get("deadline") or settle.get("end") or 0)
        want = {"maker": maker is not None, "side": side, "shares": float(trade["order"]),
                "price": float(maker.price) if maker is not None else float(taker["avg"]),
                "cap": float(taker["cap"]) if taker and taker.get("cap") else None,
                "limit": self.sim_taker_limit(taker) if taker and taker.get("cap") else None,  # the LIMIT price (LIVE_TAKER_PRICE)
                "cost": float(trade["price"]), "token": outcome["token"], "outcome": outcome["name"], "book_ms": mk.book.fetched_ms,
                "index_set": outcome["index_set"], "assumed": assumed, "fee_bps": info.fee_bps,
                "neg_risk": info.neg_risk, "yield_bearing": info.yield_bearing, "condition_id": info.condition_id,
                "asks": [[p, q] for p, q in core.side_levels(mk.book, side)][:core.PREDICT_DEPTH],
                # a maker's signed expiry — 1.150.0: a minute past the market's end, where it was two hours past it (order_expiry:
                # the backstop when its cancel at the end cannot get through; the taker's is set as it is built, live_place_order)
                "expires": order_expiry(end, now_ms)}
        trade.update(status="resting", shares=0.0, filled=None, fills=[], expected_price=float(trade["price"]),
                     cost_confirmed=maker is not None,  # a maker fills at its price; a taker's cost waits for Predict's average
                     live={"state": "pending", "want": want, "attempt": 1, "order_id": "", "hash": "", "placed_at": None,
                           # 1.138.0: stamped when it is made — the pass's now_ms is read before the prefetch and the lock, so a step's
                           # decisions were over a second old at once and every one was re-judged (and some voided) at the send
                           "events": [{"at": max(now_ms, self.market.now_ms()), "what": "决定下单"}]})
        if taker:
            trade["queue_ahead"] = trade["queue_min"] = 0.0
        return trade

    def sim_fill(self, trade: dict, mk: core.SimMarket, now_ms: int) -> None:
        if isinstance(trade.get("live"), dict):
            return  # a real order's fills come from Predict (live_sync), never presumed from the book
        super().sim_fill(trade, mk, now_ms)

    def sim_withdraw_reason(self, trade: dict, mk: core.SimMarket | None, now_ms: int | None = None) -> str:
        if isinstance(trade.get("live"), dict) and not trade.get("maker"):
            # a taker order in flight rests only until LIVE_TAKER_WAIT_SECONDS: SIM_WAYS / SIM_MARKETS do not withdraw it —
            # a market at its end does (a bid left on a finished market is a free option for whoever holds the losing side)
            return self.sim_quiet(trade.get("settle") or (mk.settle if mk is not None else None), self.market.now_ms() if now_ms is None else now_ms)
        return super().sim_withdraw_reason(trade, mk, now_ms)

    def sim_withdraw(self, trade: dict, why: str, now_ms: int) -> None:
        super().sim_withdraw(trade, why, now_ms)
        st = trade.get("live")
        if isinstance(st, dict) and st.get("state") in {"pending", "placing", "open", "unknown"}:
            st["state"], st["cancel_why"] = "cancelling", why
            st["events"].append({"at": now_ms, "what": f"撤单：{why}"})

    def sim_settle(self, trade: dict, up: float, note: str, now_ms: int, by: str) -> None:
        super().sim_settle(trade, up, note, now_ms, by)
        st = trade.get("live")
        if (isinstance(st, dict) and by == "Predict 最终结果" and trade.get("status") == "settled" and float(trade.get("shares") or 0) > 1e-9
                and float(trade.get("payout") or 0) > 0 and self.config.live_auto_redeem):
            # a real position settled in its favour: redeemed within seconds, not at the next ten-minute round — its market read
            # first by that round (1.150.0, RDM-SOON-NO-TARGET)
            self.live_redeem_soon(str((st.get("want") or {}).get("market_id") or trade.get("market_id") or ""))
        if isinstance(st, dict) and st.get("state") in {"pending", "placing", "open", "unknown"} and trade["status"] in {"expired", "settled"}:
            st["state"], st["cancel_why"] = "cancelling", "市场已出结果"
            st["events"].append({"at": now_ms, "what": "撤单：市场已出结果"})

    def live_settle_late(self, trade: dict, now_ms: int) -> None:
        """A real order's fill read only after its market's result (1.150.0). The step decides before it reads the orders
        back (sim_decide, then live_sync), so a result the bot's own data gives in the very step — a first-touch line
        crossed, a flip, a range or cap level reached — finds a just-filled order's record still at 0 shares: sim_settle
        lapses it ("expired": 市场已出结果，挂单一直没成交) and the cancel that follows, or the positions check minutes
        later (live_late_fills), then reads the fill. Every path left such a record alone (live_finish, live_cancel_fills_read
        and live_adopted_status skip settled and expired records alike; sim_decide re-settles only on a changed result): it
        held real shares as "未成交" with no payout — out of the journal's P&L and out of the day's settled loss, so the
        loss stop (LIVE_MAX_DAILY_LOSS_USD) did not see the money until Predict's own result, hours later, and then on the
        day of the lapse (after Beijing midnight: on no day's stop at all). The record is settled here instead, the moment
        the shares are on it, on the result it already has — Predict's, else the bot's own — and stamped now (sim_settle):
        the loss counts on the day it becomes known, a win as any pre-settled win does; Predict's word still confirms or
        corrects it later (sim_confirm). No order goes out past a loss settled here (live_send_block recomputes the day's
        figure before every send); the switch itself trips at the step's next check (live_check_kill)."""
        shares, order = float(trade.get("shares") or 0), float(trade.get("order") or 0)
        if trade.get("status") != "expired" or shares <= 1e-9:
            return
        final, local = trade.get("final"), trade.get("local")
        result = final if isinstance(final, dict) and final.get("up") is not None else local
        if not isinstance(result, dict) or result.get("up") is None:
            return  # no result on the record to settle it on: left as it is
        by = "Predict 最终结果" if result is final else "本地预结算"
        what = f"Predict 结算：{final.get('name', '')}" if result is final else str(local.get("note") or "")
        trade["live"]["events"].append({"at": now_ms, "what": f"市场出结果后才读到成交 {shares:g}/{order:g} 份：按已有的结果（{by}）结算，计入今日已结算盈亏"})
        self.sim_settle(trade, float(result["up"]), f"{what}（市场出结果后才读到这笔真实成交 {shares:g}/{order:g} 份，按已有的结果结算）", now_ms, by)

    def sim_wait(self, trade: dict, mk: core.SimMarket | None, now_ms: int) -> str:
        st = trade.get("live")
        if not isinstance(st, dict) or trade["status"] != "resting":
            return super().sim_wait(trade, mk, now_ms)
        shares, order = float(trade.get("shares") or 0), float(trade.get("order") or 0)
        tag = f"真实订单 #{st['order_id']}" if st.get("order_id") else "真实订单"
        if st.get("state") in {"pending", "placing"}:
            return f"{tag}正在发送给 Predict，等待订单号"
        if st.get("state") == "cancelling":
            return f"{tag} 撤单中（{st.get('cancel_why', '')}），已成交 {shares:g}/{order:g} 份"
        if not trade.get("maker"):
            cap = st.get("want", {}).get("cap")
            return (f"{tag} 限价吃单等待 Predict 成交（封顶 {core.cents(float(cap))}，" if cap else f"{tag} 吃单等待 Predict 成交（") \
                + f"{self.config.live_taker_wait} 秒未成交的部分撤掉），已成交 {shares:g}/{order:g} 份"
        price = float(trade["price"])
        best = ""
        if mk is not None and not mk.book.stale(now_ms):
            levels = core.side_levels(mk.book, trade["side"])
            if levels:
                best = f"，最低卖价 {core.cents(levels[0][0])}（{'高出 ' + core.cents(levels[0][0] - price) if levels[0][0] > price + 1e-9 else '已到价'}）"
        return f"{tag} 挂 {core.cents(price)}，已成交 {shares:g}/{order:g} 份{best}；成交以 Predict 回报为准"

    def sim_version(self) -> dict:
        c = self.config
        if c.live_mode == "off":
            return super().sim_version()
        return {**super().sim_version(), "live": c.live_mode, "live_account": short_addr(self.live.wallet.maker),
                "live_max_order_usd": c.live_max_order_usd, "live_max_open_usd": c.live_max_open_usd}

    # --- the step: prefetch, decide (the paper trader), place, read back --------------------------------------------
    async def sim_step(self, now_ms: int) -> Any:
        if not self.sim_step_due():
            return False
        marks, t0 = {}, time.monotonic()

        def mark(phase: str) -> None:
            nonlocal t0
            now = time.monotonic()
            marks[phase], t0 = int((now - t0) * 1000), now
        try:
            if self.config.live_mode != "off":
                await self.live_prefetch(now_ms)
        except Exception as error:
            self.live_note_error(f"读取市场信息失败：{core.clean_error(error) or type(error).__name__}")
        mark("预取")
        self.live_place_ms = 0
        result = await super().sim_step(now_ms)  # ...which sends the pending orders (sim_after_decide) before its settlement lookups
        mark("决策")
        marks["决策"] -= self.live_place_ms
        marks["下单"] = self.live_place_ms
        try:
            # 1.150.0 (audit SIM-STALE-CLOCK): on the clock the step decided on (sim_looked_ms: Bot.sim_step reads it under the lock),
            # as the decisions and their sync always shared one — not on the job's, read before the prefetch: a taker's unfilled rest
            # was cancelled that much after LIVE_TAKER_WAIT_SECONDS (live_taker_due weighs it against placed_at, read off the clock).
            # Nor on a clock read here: this sync's open-order list is stamped with it and weighed against the records' own stamps
            # (live_readopt, prunable), and the express lane stamps a record when its pass begins, not when Predict answers — a
            # list stamped after a pass that ran beside this step's placements, yet read before that pass's cancel was confirmed,
            # would take the cancelled order back
            await self.live_sync(max(now_ms, self.sim_looked_ms))
            mark("同步")
        except Exception as error:
            self.live_note_error(f"同步真实订单失败：{core.clean_error(error) or type(error).__name__}")
        self.live_step_ms = marks
        calls = getattr(self.live.api, "calls", 0)
        self.live_rate_samples.append((time.monotonic(), calls if isinstance(calls, int) else len(calls or ())))  # a counter (a stand-in's call log: its length)
        while len(self.live_rate_samples) > 2 and self.live_rate_samples[-1][0] - self.live_rate_samples[1][0] >= 60:
            self.live_rate_samples.popleft()  # the last minute's
        return result

    async def sim_after_decide(self, now_ms: int) -> None:
        """The step's decisions made: the loss switch is checked on this step's settlements and the pending orders go
        out now — before the settlement lookups of old positions (sim_confirm) are waited for, which used to hold a new
        order behind a slow Predict read."""
        started = time.monotonic()
        try:
            self.live_check_kill(self.sim_trades(), now_ms)
            # 1.138.1: a withdrawn record that never went out (no order id, no hash) is closed now and its slot quoted again — it needs no
            # list; the step's other cancels stay in its sync, after the open list's read, whose fills count before a confirmed removal
            # (1.83.0: a single-order read may lag the list, and a blind cancel would lose what filled)
            unsent = {tid: t for tid, t in self.live_cancels_unsent().items() if not t["live"].get("order_id")}
            if unsent and self.live.signed_in:  # 1.150.0: signed in, not ready — closed all the same (its re-quote waits for ready)
                await self.live_cancel_blind(unsent, now_ms)
                await self.live_requote_freed(list(unsent), now_ms)
            await self.live_place_pending(now_ms)
        except Exception as error:
            self.live_note_error(f"下单步骤失败：{core.clean_error(error) or type(error).__name__}")
        self.live_place_ms = int((time.monotonic() - started) * 1000)

    async def live_prefetch(self, now_ms: int) -> None:
        """Sign in again when needed; market details for the markets whose suggestion is near the bar (a decision
        needs them in the same step); the balances now and then."""
        await self.live_ensure_ready()
        if not self.live.ready:
            return
        if time.monotonic() - self.live_last_balance >= self.LIVE_BALANCE_SECONDS:
            self.live_last_balance = time.monotonic()
            if self.live.balance is None or not self.live.approvals:  # never read: the step needs them now (the balance gate, the approvals)
                await self.live_account_round()
            elif self.live_account_task is None or self.live_account_task.done():
                self.live_account_task = asyncio.create_task(self.live_account_round(), name="live-account")  # beside the step: chain reads never hold a decision
        c, trades = self.config, self.sim_trades()
        wanted, self.live_wanted = list(self.live_wanted), set()
        for mk in self.sim_markets(now_ms):
            if mk.hold or mk.book.stale(now_ms) or not self.sim_in_scope(mk) or core.book_crossed(mk.book):
                continue
            info = self.live.markets.get(str(mk.book.market_id))
            if info and time.monotonic() - info.at < 3600:
                continue
            near = False  # judged as the paper trader judges: the valid 买1's edge, a taker under its cap
            if mk.makers and c.sim_ways != "taker":
                near = self.sim_maker_reason(mk, trades, now_ms)[0] is not None
            bps = mk.book.fee_bps if mk.book.fee_bps is not None else c.predict_fee_bps
            for side in ("up", "down") if c.sim_ways != "maker" else ():
                fair = mk.fair_up if side == "up" else 1 - mk.fair_up
                cap = self.sim_taker_cap(mk, side, bps, slack=0.03)  # a little under the paper trader's cap: read the market early
                q = core.taker_quote(mk.book, side, self.sim_taker_shares(mk), bps, cap) if cap > 0 else None
                near = near or (q is not None and q["got"] >= core.SIM_MIN_SHARES - 1e-9)
            if near:
                wanted.append(str(mk.book.market_id))
        due = [mid for mid in dict.fromkeys(wanted) if time.monotonic() >= self.live_market_retry.get(mid, 0.0)]

        async def read(mid: str) -> None:
            try:
                await self.live.market(mid)
                self.live_market_retry.pop(mid, None)
            except Exception as error:
                limited = getattr(error, "retry_after", 0) and http_status(error) in (0, 429)  # 1.138.0: a cooldown's refusal: again when it ends, not in a minute
                self.live_market_retry[mid] = time.monotonic() + (max(2.0, float(error.retry_after)) if limited else self.LIVE_MARKET_RETRY_SECONDS)
                self.live_note_error(f"市场 {mid} 详情读取失败：{core.clean_error(error) or type(error).__name__}")
        await asyncio.gather(*(read(mid) for mid in due[:5]))  # side by side (the API gate bounds them), not one after another

    async def live_account_round(self) -> None:
        """The balances, and now and then the approvals (chain reads of a second or two each)."""
        with contextlib.suppress(Exception):
            await self.live.refresh_balance()
        if time.monotonic() - self.live.approvals_mono >= self.LIVE_APPROVALS_SECONDS or not self.live.approvals:
            with contextlib.suppress(Exception):
                await self.live.refresh_approvals()

    async def live_ensure_ready(self) -> None:
        """Sign in (and check the account) when not ready, at most once a minute."""
        if self.live.ready or time.monotonic() - self.live_last_signin < 60:
            return
        self.live_last_signin = time.monotonic()
        with contextlib.suppress(Exception):
            await self.live.sign_in()
            await self.live.verify_account()
            self.live_notify("✅ Predict 登录成功，真实交易就绪。", key="signin")

    async def live_place_pending(self, now_ms: int) -> None:
        if self.stopping.is_set():
            return  # stopping: nothing new goes out (a request cut off by the exit would leave a record Predict may or may not know)
        trades = self.sim_trades()
        sending = []
        for tid, trade in trades.items():
            st = trade.get("live")
            if isinstance(st, dict) and st.get("state") == "pending" and trade["status"] == "resting" and tid not in self.live_placing:
                if hold := self.live_send_hold(now_ms):
                    self.live_defer(tid, trade, hold, now_ms)  # waits (pending) rather than failing: the condition passes by itself
                    continue
                if self.live_send_gate(tid, trade, trades, now_ms, now_ms):  # judged with every pending record counted (live_reserved)
                    sending.append((tid, trade))
        # side by side (the API gate bounds them): two orders after a close took 4 s one after the other
        await asyncio.gather(*(self.live_place(tid, trade, now_ms) for tid, trade in sending))

    LIVE_BALANCE_STALE_S = 600  # after a failed refresh, a balance read longer ago than this is no ground for a new order

    def live_balance_gate(self) -> str:
        """Why the balance cannot vouch for a new order: never read, or the last refresh failed and the last good read
        is old. A stale figure is not "enough USDT"."""
        b = self.live.balance
        if b is None:
            return "余额尚未读到，暂不开新仓"
        if self.live.balance_error and time.time() - b[2] > self.LIVE_BALANCE_STALE_S:
            return f"余额刷新失败（{self.live.balance_error}），上次成功读取已超过 {self.LIVE_BALANCE_STALE_S // 60} 分钟，暂不开新仓"
        return ""

    def live_send_hold(self, now_ms: int) -> str:
        """Why no new order goes out right now, although nothing is wrong with the order itself: the account's open
        orders could not be read lately (a list that cannot be read is no ground for new risk; cancels still go out),
        or the API is in a cooldown. "" to proceed."""
        failing = self.live_sync_failed_ms and self.live_sync_failed_ms >= self.live_synced_at  # the latest read failed
        if failing and not self.live_synced_at:
            return "账户订单尚未同步成功，暂不开新仓"
        if failing and now_ms - self.live_synced_at > self.LIVE_SYNC_STALE_MS:
            return f"账户订单已 {(now_ms - self.live_synced_at) // 1000} 秒没有同步成功（最近一次读取失败），暂不开新仓"
        if (left := getattr(self.live.api, "blocked_until", 0.0) - time.monotonic()) > 0:
            return f"Predict 接口限流冷却中（{int(left) + 1} 秒后重试）"
        return ""

    LIVE_DECISION_MAX_MS = 90_000  # a decision older than this (a restart in between) is not sent: its book is gone

    def live_send_block(self, trade: dict, trades: dict[str, dict], now_ms: int) -> str:
        """Why a pending order must not go out, checked right before it is signed: the mode, the hard switch, the
        pauses, the age of the decision, the caps and the balance as they stand now — the decision was checked a step
        ago, or before a restart, and anything may have changed since. "" to send."""
        c, st = self.config, trade["live"]
        if self.preopen_exclusive():
            return "开盘前固定双边预挂模式：不发送普通模型策略的旧决定"
        if c.live_mode != "on":
            return f"LIVE={c.live_mode}，不下单"
        if not c.live_allowed:
            return "部署变量 LIVE_TRADING_ALLOWED=off，不下单"
        if paused := self.live_paused():
            return f"真实交易已暂停（{paused}）"
        if killed := self.live_killed():
            return f"今日停止开新仓：{killed}"
        # recomputed now, not a flag saved a step ago — after /live resume too (1.150.0: against the line the resume left,
        # live_loss_floor; the check used to be skipped for the rest of a day whose stop was lifted by hand)
        if (cap := c.live_max_daily_loss_usd) and (pnl := self.live_daily_pnl(trades, now_ms) + self.preopen_pnl()) <= self.live_loss_floor(cap):
            return f"今日已结算亏损 {money(pnl)} 已达 LIVE_MAX_DAILY_LOSS_USD ${cap:g}"
        if not self.live.ready:
            return f"真实交易未就绪：{self.live.not_ready_why}"
        decided = self.live_decided(trade, now_ms)
        if now_ms - decided > self.LIVE_DECISION_MAX_MS:
            return f"决定已过时（{(now_ms - decided) // 1000} 秒前的盘口），不下单"
        if quiet := self.sim_quiet(trade.get("settle"), now_ms):
            return quiet  # decided before the market's last minutes, sent inside them: not sent (1.102.1: 15 分钟涨跌最后 90 秒)
        notional = float((st.get("want") or {}).get("shares") or trade.get("order") or 0) * self.live_unit_cap(trade)
        if notional > c.live_max_order_usd + 1e-9:
            return f"单笔最多 ${notional:,.2f} 超过 LIVE_MAX_ORDER_USD ${c.live_max_order_usd:g}"
        others = self.live_exposure(trades) - self.live_reserved(trade)
        if others + notional > c.live_max_open_usd + 1e-9:
            return f"持仓+挂单 ${others:,.2f} 加本单 ${notional:,.2f} 超过 LIVE_MAX_OPEN_USD ${c.live_max_open_usd:g}"
        usd = self.live.usd()
        if usd is not None and usd < notional:
            return f"USDT 余额 ${usd:,.2f} 不足本单 ${notional:,.2f}"
        return ""

    @staticmethod
    def live_decided(trade: dict, default: int) -> int:
        """When the record's order was decided (its first event), on the bot's clock."""
        return int(((trade.get("live") or {}).get("events") or [{}])[0].get("at") or trade.get("opened") or default)

    def live_send_gate(self, tid: str, trade: dict, trades: dict[str, dict], now_ms: int, at_ms: int) -> bool:
        """Whether a pending order goes out now, judged right before it is signed (at ``at_ms``): blocked (live_send_block)
        it fails; a taker the book now voids fails too; one that cannot be judged now waits, pending, until it can or
        its decision is too old to send (live_taker_recheck). True to send."""
        if why := self.live_send_block(trade, trades, at_ms):
            # 1.138.0: the decision's age and the market's last minutes are the bot's own reasons, not Predict's: the side is free at
            # once (they took LIVE_RETRY_SECONDS, five minutes, after a cooldown had held the order past 90 s)
            own = why.startswith("决定已过时") or why == self.sim_quiet(trade.get("settle"), at_ms)
            self.live_fail(tid, trade, f"未下单：{why}", now_ms, retry=0 if own else None)
            return False
        why, wait = self.live_taker_recheck(trade, at_ms)
        if why and wait:
            self.live_defer(tid, trade, why, now_ms)
        elif why:  # the bot's own re-judgement: no failure — a maker re-decides at the new price in seconds, a taker after its short wait
            self.live_fail(tid, trade, f"未下单：{why}", now_ms, retry=self.LIVE_MAKER_VOID_SECONDS if trade.get("maker") else self.config.live_taker_retry)
        return not why

    LIVE_TAKER_RECHECK_MS = 1_000  # a taker decided longer ago than this (held by a cooldown, a sign-in) is priced again before it goes out
    LIVE_MAKER_VOID_SECONDS = 3    # a resting order voided by that re-check: its side free again this soon (1.138.0: 30 s; a withdrawn resting one is re-quoted at once)
    LIVE_TAKER_REST_SLACK = 0.02   # a taker's unfilled rest stays while its limit keeps the bar less this (1.90.0: a fair price wobbling a few
    #                                tenths across a cent boundary cancelled the rest, and its side then sat out LIVE_RETRY_SECONDS)

    def live_taker_recheck(self, trade: dict, now_ms: int) -> tuple[str, bool]:
        """(why, wait) for a taker decided a while ago — held by a cooldown or a sign-in, up to LIVE_DECISION_MAX_MS —
        priced again before it is sent (1.83.0: such an order went out at a limit set on a book up to 90 s old). Its
        decision is void when its limit is over the cap the fair price allows now or the card holds its suggestion;
        it waits (``wait``) when it cannot be judged now — no fresh book, the market not priced, its pricing failing:
        1.83.0 failed it then, and its side sat out LIVE_RETRY_SECONDS for a book a second past PREDICT_STALE_MS.
        ("", False) to send; a decision of this very second (the step's own, the express lane's) is sent as made."""
        age = now_ms - self.live_decided(trade, now_ms)
        if age <= self.LIVE_TAKER_RECHECK_MS:
            return "", False
        word = "挂单" if trade.get("maker") else "吃单"
        try:
            mk = next((x for x in self.sim_markets_for(str(trade.get("key") or ""), now_ms) if x.market == trade.get("market")), None)
        except Exception as error:  # the order waits; a pricing failure is no ground to send it, nor to fail it
            return f"复核{word}决定时定价出错（{core.clean_error(error) or type(error).__name__}），等下一轮", True  # one note per cause (live_defer)
        if mk is None or mk.book.stale(now_ms) or core.book_crossed(mk.book):
            return f"{word}决定之后还没有新鲜盘口可复核，等新盘口", True
        if not self.sim_in_scope(mk) or self.config.sim_ways == ("taker" if trade.get("maker") else "maker"):
            # 1.122.0: SIM_MARKETS / SIM_SKIP / SIM_WAYS changed since the decision: it is not this strategy's order any more
            return f"策略范围已变（SIM_MARKETS / SIM_SKIP / SIM_WAYS），{age // 1000} 秒前的{word}决定作废", False
        if trade.get("maker"):
            # 1.110.0: a resting order decided a while ago (held by a cooldown, a sign-in) is judged on the book now, as a
            # taker is — the rules that would withdraw or re-quote it resting apply before it goes out
            why = self.sim_withdraw_reason(trade, mk, now_ms) or self.sim_requote_reason(trade, mk, now_ms)
            return (f"盘口变了：{why}，{age // 1000} 秒前的挂单决定作废", False) if why else ("", False)
        if mk.hold:
            return f"卡片暂不建议（{mk.hold}），{age // 1000} 秒前的吃单决定作废", False
        bps = mk.book.fee_bps if mk.book.fee_bps is not None else self.config.predict_fee_bps
        # 1.138.0: with the resting rest's slack — a chance right at the bar was voided by a fair price a tenth of a cent lower
        cap, limit = self.sim_taker_cap(mk, str(trade.get("side") or "up"), bps, slack=self.LIVE_TAKER_REST_SLACK), self.live_unit_cap(trade)
        if cap <= 0 or limit > cap + 1e-9:
            return f"公平价变了：限价 {core.cents(limit)} 已高于现在的封顶价 {core.cents(cap)}，{age // 1000} 秒前的吃单决定作废", False
        return "", False

    async def live_place(self, tid: str, trade: dict, now_ms: int) -> None:
        """Build, sign and send the order of a pending record — once: the express lane and the step may both find it
        pending within the same second."""
        if tid in self.live_placing:
            return
        self.live_placing.add(tid)
        try:
            await self.live_place_order(tid, trade, now_ms)
        finally:
            self.live_placing.discard(tid)

    async def drain_orders(self) -> None:
        """At shutdown (a redeploy): the placements and cancels in flight are given LIVE_DRAIN_SECONDS to be answered
        before their tasks are cancelled, and nothing new goes out meanwhile (live_place_pending). 1.104.10: the 16:46
        redeploy on 11-04 cut five placements off mid-request; each record was left "placing" with a hash Predict never
        answered to, unknown after the grace, its side held until the evidence or an operator freed it."""
        api = getattr(self.live, "api", None)
        inflight = lambda: len(self.live_placing) + int(getattr(api, "writes_inflight", 0) or 0)
        started = time.monotonic()
        while inflight() and time.monotonic() - started < self.LIVE_DRAIN_SECONDS:
            await asyncio.sleep(0.05)
        if inflight():
            LOG.warning("停止时仍有 %d 个下单/撤单请求等了 %d 秒没有应答：重启后按哈希找回", inflight(), self.LIVE_DRAIN_SECONDS)
        elif time.monotonic() - started >= 0.05:
            LOG.info("停止前等在途的下单/撤单请求应答：%.1f 秒", time.monotonic() - started)

    def live_cancels_unsent(self, skip: Any = ()) -> dict[str, dict]:
        """The records whose cancel is decided but not sent yet (state cancelling, an order id known), by tid. Not one whose
        removal another path has on its way right now (live_cancels, 1.150.0): its cancel_sent is only set once Predict answers."""
        return {tid: t for tid, t in self.sim_trades().items() if tid not in skip and isinstance(t.get("live"), dict)
                and t["live"].get("state") == "cancelling" and not t["live"].get("cancel_sent") and tid not in self.live_placing
                and str(t["live"].get("order_id") or "") not in self.live_cancels
                and (t["live"].get("order_id") or not t["live"].get("hash"))}  # 1.138.1: one never sent (no id, no hash) is closed at once too

    async def sim_express_after(self, now_ms: int) -> None:
        """An express decision in real trading: its orders go out now and the cancels of the orders it withdrew, side by
        side (1.104.8: a cancel used to wait behind the pass's placements — the step's own placements and its sync skip
        what is done); a slot whose cancel Predict confirms is quoted again at once and that order sent too
        (live_requote_freed)."""
        cancelling = self.live_cancels_unsent()

        async def place() -> None:
            if self.config.live_mode != "off":
                try:
                    await self.live_place_pending(now_ms)
                except Exception as error:
                    self.live_note_error(f"快车道下单失败：{core.clean_error(error) or type(error).__name__}")

        async def cancel(records: dict[str, dict]) -> None:
            if records and self.live.signed_in:  # 1.150.0: a cancel needs the sign-in alone (LiveTrader.signed_in), not the account check
                try:
                    await self.live_cancel_blind(records, now_ms)
                except Exception as error:
                    self.live_note_error(f"快车道撤单失败：{core.clean_error(error) or type(error).__name__}")

        await asyncio.gather(cancel(cancelling), place())
        late = self.live_cancels_unsent(cancelling)  # withdrawn while its placement was in flight (the id just came back): cancelled now too
        await cancel(late)
        if (cancelling or late) and self.live.ready:
            await self.live_requote_freed([*cancelling, *late], now_ms)

    def sim_requote_freed(self, markets: dict, freed: dict[str, dict], trades: dict[str, dict], now_ms: int, changed: list) -> None:
        if self.config.live_mode != "off":
            return  # the real order's cancel goes out after the step's decisions, and its slot is busy until Predict confirms it (live_room): quoted again then (live_requote_freed)
        super().sim_requote_freed(markets, freed, trades, now_ms, changed)

    async def live_requote_freed(self, tids: list[str], now_ms: int) -> None:
        """The slots whose real orders Predict has just confirmed cancelled (``tids``: the records whose cancels went
        out; what filled stays a position) are quoted again now — one express look at each of their markets, the orders sent —
        rather than at the next push or step (1.104.6: the cancel went out at once, but the order that took its place
        waited for the next look, one to five seconds, while the market maker on the same book re-posts within half a
        second). Only the strategy's own withdrawals (withdrawn.rule: the edge gone, a 买1 moved, a setting changed); an
        operator's cancel, the emergency stop, LIVE=off and the loss stop are not followed by a fresh order here. The
        fresh order faces every gate a first one does; at the withdrawn price within the lane's cooldown it is not
        placed (sim_express_cooling). It names the record it follows (requote_of) and that one's withdrawal
        (withdrawn_at): the 撤单→重挂 span of live_latency. Not while the account is not ready (1.150.0): the cancels of a
        bot signed in but not ready go out (LiveTrader.signed_in), a fresh order would only be refused (live_room) — the
        step quotes the slot again once ready."""
        if self.config.live_mode == "off" or self.config.sim_ways == "taker" or not tids or not self.live.ready:
            return
        trades = self.sim_trades()
        freed: dict[tuple[str, str], tuple[str, dict]] = {}
        for tid in tids:
            t = trades.get(tid)
            st = t.get("live") if isinstance(t, dict) else None
            if isinstance(st, dict) and st.get("state") == "done" and (t.get("withdrawn") or {}).get("rule") and t.get("maker") and t.get("key"):
                freed[(str(t["market"]), str(t.get("side") or ""))] = (tid, t)
        if not freed:
            return
        for _, t in freed.values():
            if (t.get("live") or {}).get("fills_pending") and len(self.live_requote_looked) < 1000:
                self.live_requote_looked.add(str(t["live"].get("order_id") or ""))
        at_ms = self.market.now_ms()
        try:
            for key in sorted({str(t["key"]) for _, t in freed.values()}):
                changed = await self.sim_express_look(key, at_ms)
                fresh = []
                for tid, t in changed:
                    st = t.get("live")
                    old = freed.get((str(t.get("market")), str(t.get("side") or "")))
                    if (old is not None and isinstance(st, dict) and st.get("state") == "pending" and t.get("maker")
                            and not st.get("requote_of")):
                        st["requote_of"], st["withdrawn_at"] = old[0], int((old[1].get("withdrawn") or {}).get("at") or 0)
                        st["requote_cancel_sent_at"] = int(old[1]["live"].get("cancel_sent_at") or 0)  # the chain's points (live_latency)
                        st["requote_cancel_done_at"] = int(old[1]["live"].get("cancel_done_at") or 0)
                        st["events"].append({"at": at_ms, "what": f"上一张单（{core.cents(float(old[1].get('price') or 0))}）的撤单已获确认，随即重挂"})
                        fresh.append((tid, t))
                if fresh:
                    self.sim_requote_count += len(fresh)
                    self.sim_requote_live += len(fresh)
                    self.sim_save(fresh)
                if changed:
                    await self.live_place_pending(at_ms)
        except Exception as error:
            self.live_note_error(f"撤单后重挂失败：{core.clean_error(error) or type(error).__name__}")

    async def live_place_order(self, tid: str, trade: dict, now_ms: int) -> None:
        """Build, sign and send the order of a pending record. The hash is saved (state "placing") before the request
        goes out: a crash in between is reconciled by the hash, never by sending again."""
        st, want = trade["live"], trade["live"]["want"]
        c, wallet = self.config, self.live.wallet
        if (left := getattr(self.live.api, "blocked_until", 0.0) - time.monotonic()) > 0:
            return self.live_defer(tid, trade, f"Predict 接口限流冷却中（{int(left) + 1} 秒后重试）", now_ms)
        try:
            await self.live.api.ensure_auth()  # a sign-in failure is "not sent", never "result unknown"
        except Exception as error:
            if self.live_unsent_withdrawn(tid, trade, now_ms):
                return  # withdrawn while the sign-in ran: finished where it lives, not deferred (and saved back) at the slot
            return self.live_defer(tid, trade, f"登录失败：{core.clean_error(error) or type(error).__name__}", now_ms)
        if self.live_unsent_withdrawn(tid, trade, now_ms):
            return  # withdrawn by the strategy while the sign-in ran (1.138.0: overwritten as "placing" and sent)
        # the sign-in may have taken seconds: what was checked before it (the mode, the pauses, the stops, the caps, a
        # taker's price) is checked again on what holds now — a LIVE=off / pause or an emergency stop set meanwhile sends
        # nothing (1.83.0: the order went out, and with LIVE=off it then rested untouched)
        if not self.live_send_gate(tid, trade, self.sim_trades(), now_ms, self.market.now_ms()):
            return
        try:
            shares_wei = to_wei(want["shares"])
            if want["maker"]:
                amounts = limit_amounts(True, to_wei(want["price"]), shares_wei)
                strategy, expiration = "LIMIT", int(want.get("expires") or FAR_FUTURE)
            else:
                # the limit (the dearest level seen, or the cap: LIVE_TAKER_PRICE): the book at or under it fills now, the
                # rest waits LIVE_TAKER_WAIT_SECONDS, or less once the fair price moves away (live_taker_due)
                amounts = limit_amounts(True, to_wei(want.get("limit") or want.get("cap") or want["price"]), shares_wei)
                # 1.150.0: its five minutes end with the market, as a maker's expiry does (order_expiry): built in a market's last
                # minutes, its rest — withdrawn at the end like any order — stayed matchable up to five minutes past it when that
                # cancel failed. Never longer than before; live_order_deadline keeps the five minutes (nothing is released sooner)
                strategy, expiration = "LIMIT", min(int(now_ms // 1000) + TAKER_ORDER_SECONDS,
                                                    order_expiry(core.sim_end_ms(trade.get("settle")), now_ms))
            order = build_order(wallet, want["token"], amounts["maker"], amounts["taker"], want["fee_bps"], expiration)
            digest, signature = wallet.sign_order(order, want["neg_risk"], want["yield_bearing"])
            body: dict = {"data": {"order": {**order, "signature": signature, "hash": digest},
                                   "pricePerShare": str(amounts["price_per_share"]), "strategy": strategy}}
            st.update(state="placing", hash=digest.lower(), strategy=strategy, order=order, placing_at=now_ms,
                      price_per_share=from_wei(amounts["price_per_share"]), usd_cap=from_wei(amounts["maker"]),
                      shares_requested=from_wei(amounts["amount"]))
            self.sim_save([(tid, trade)])
            flushing = time.monotonic()
            flushed = await asyncio.to_thread(self.store.flush)  # the hash on disk before the request goes out: a crash in between is reconciled by it
            st["flush_ms"] = int((time.monotonic() - flushing) * 1000)  # the wait for the writer (live_latency, /diag: 落盘等待)
            # 1.122.0: the flush may have waited seconds — LIVE=off, a pause, the loss stop, a withdrawal by the express lane (the
            # record is "cancelling" then) may have come meanwhile: judged once more, as a pending order is, right before the request
            if self.live_unsent_withdrawn(tid, trade, now_ms):
                return  # 1.138.0: finished where the withdrawal moved it, no backoff (1.138.1: before the flush's own failure is judged —
            #                 that reset it to pending and saved it back at the slot, its side's way blocked for good)
            if not flushed:
                return self.live_unsend(tid, trade, f"数据库写入未完成（{self.store.write_error or '超时'}），暂不发单", now_ms)
            st["state"] = "pending"
            if not self.live_send_gate(tid, trade, self.sim_trades(), now_ms, self.market.now_ms()):
                st["hash"] = ""
                self.sim_save([(st.get("rekeyed") or tid, trade)])  # 1.138.1: where live_fail moved it, not back at the slot
                return
            st["state"] = "placing"
            # the order path's timing points (live_latency, /diag): the book's read (want.book_ms), the decision (opened), the
            # request sent, its answer — read off the clock as they happen (1.83.0: the step's clock left out the time the
            # decisions, the sign-in, the signing and the save took before the request went out)
            st["sent_at"] = self.market.now_ms()
            sent = time.monotonic()

            def still_sendable() -> str:
                # 1.150.0 (audit LIVE-SEND-AFTER-PAUSE): run by PredictApi.send inside the request slot — the wait for it came after
                # the check above, and a pause, the emergency stop, LIVE=off or a withdrawal set meanwhile let the order out
                if trade.get("withdrawn") or st.get("state") != "placing":
                    return "发出前已撤单"
                return self.live_send_block(trade, self.sim_trades(), self.market.now_ms())

            token = ORDER_SEND_CHECK.set(still_sendable)
            try:
                result = await self.live.api.create_order(body)
            finally:
                ORDER_SEND_CHECK.reset(token)
            took = time.monotonic() - sent
            st["answered_at"] = st["sent_at"] + int(took * 1000)
            # 1.150.0: Predict's answer on the books' clock too (the wall clock a book's fetched_ms is stamped with), as
            # cancel_done_ms is: answered_at is on the Binance-offset clock (market.now_ms = wall clock + offset_ms) for
            # live_latency and is not to be compared with a book's read — own_on_book did, so within the clock offset of the
            # answer our order was taken out of a level it had joined (a false "买1 gone") or counted as someone else's 买1 on
            # a book that already showed it. The offset is taken back out rather than the wall clock read anew, so a market
            # clock without an offset (the tests' fakes, whose books are stamped on that clock) keeps the two equal.
            st["answered_ms"] = st["answered_at"] - int(getattr(self.market, "offset_ms", 0) or 0)
        except Exception as error:
            tid = st.get("rekeyed") or tid  # 1.138.0: withdrawn and moved (sim_rekey) while it waited: it lives there now — never saved back at the slot
            text = core.clean_error(error) or type(error).__name__
            if isinstance(error, SendRefused):
                # 1.150.0 (audit LIVE-SEND-AFTER-PAUSE): refused inside the request slot — nothing went out (it would otherwise read as
                # "result unknown", status 0). Withdrawn meanwhile: finished unsent, nothing to cancel; else failed as the gate above fails it
                for k in ("sent_at", "answered_at"):
                    st.pop(k, None)
                st["hash"] = ""
                if trade.get("withdrawn") or st.get("state") == "cancelling":
                    self.live_end_unsent(tid, trade, now_ms)
                    return
                st["state"] = "pending"
                at_ms = self.market.now_ms()
                own = text.startswith("决定已过时") or text == self.sim_quiet(trade.get("settle"), at_ms)
                self.live_fail(tid, trade, f"未下单：{text}", now_ms, retry=0 if own else None)
                self.sim_save([(st.get("rekeyed") or tid, trade)])
                return
            definite = isinstance(error, OrderRejected) or 400 <= http_status(error) < 500
            # 1.122.0: an answer that the order already exists (a resend taken twice, a hash Predict holds) is Predict having it,
            # not a refusal: the record is "result unknown" and the sync adopts the order by its hash, nothing is freed
            # 1.150.0: a bare 409 Conflict on an order POST is the same answer without the wording (Predict, or a gateway in front
            # of it, holding that hash): failed and freed, a taker it took and filled at once was never looked at again (no order
            # id) — real shares outside exposure, P&L and the loss stop. Kept "result unknown": the sync adopts it by its hash,
            # or the positions evidence books it, or the grace period gives it up. Other 4xx answers stay definite refusals
            duplicate = http_status(error) == 409 or any(w in text.lower() for w in ("already exist", "already placed", "duplicate", "exists"))
            limited = isinstance(error, core.RemoteError) and (http_status(error) == 429 or (error.retry_after and http_status(error) == 0))
            if st.get("order_id") or st.get("state") not in {"pending", "placing", "cancelling"}:
                # 1.150.0: the step's sync ran while this request was in flight (the express lane sends outside sim_lock) and found
                # the order by its hash: live_reconcile made the record "open", or "done" for a taker that filled at once; live_cancel
                # gave one withdrawn meanwhile its id, and maybe its end. Predict has the order, so this answer (the 20 s timeout, a
                # dropped connection, a 5xx, even a 4xx or a 429) says nothing about it: the record stays as the sync left it, where it
                # lives, and Predict's own record decides its fills and its end. It used to be failed (live_fail): a filled taker booked
                # "cancelled" with every share unfilled (outside the share caps, SIM_GROUP_USD and the local pre-settlement), a resting
                # maker untracked and unreserved while its side was quoted again beside it after the backoff, and a "下单失败" alert; a
                # 429 here would have cleared its hash (live_unsend) and sent it again. Checked before all of that: the request itself
                # and a withdrawal only set pending/placing/cancelling and never an id — an id, or any other state, is a record another
                # path has taken on (found on Predict, or already finished), and failing it again would undo what that path recorded
                found = f"同步已按哈希找到订单 #{st['order_id']}" if st.get("order_id") else f"这条记录已另行处理（{st.get('state')}）"
                st["events"].append({"at": now_ms, "what": f"下单应答出错（{text}），但{found}：以 Predict 的记录为准，不按下单失败处理"})
                self.sim_save([(tid, trade)])
                return
            if limited and not trade.get("withdrawn") and st.get("state") != "cancelling":
                # refused by the client itself (a cooldown that began between the check above and the request), or by Predict's rate
                # limiter (429: turned away before it was processed — 1.138.0, it counted as a refusal and shut the side out for five
                # minutes): nothing went out, it waits pending for the cooldown
                return self.live_unsend(tid, trade, text, now_ms)
            if limited:  # withdrawn meanwhile: never sent, nothing to cancel
                self.live_end_unsent(tid, trade, now_ms)
                return
            if st.get("state") in {"placing", "cancelling"} and (not definite or duplicate):
                # the request may have reached Predict (a timeout, a dropped connection, a 5xx from a proxy, an answer
                # without an order id): never send it again; the sync step finds the order by its hash, or gives the
                # attempt up after the grace period ("cancelling": withdrawn while the request was in flight — the
                # emergency stop — which is no reason to call it "never sent")
                st["last_error"] = text
                st["events"].append({"at": now_ms, "what": f"下单请求结果未知：{text}"})
                self.sim_save([(tid, trade)])
                return
            self.live_fail(tid, trade, f"下单失败：{text}", now_ms)
            self.sim_save([(st.get("rekeyed") or tid, trade)])  # 1.138.1: a record already at tid#n is not moved by live_fail, so not written by it either
            return
        tid = st.get("rekeyed") or tid  # 1.138.0: withdrawn and moved while the request was in flight: saved where it lives (it was saved back at the slot too — one order counted twice)
        if trade.get("withdrawn") or st.get("state") == "cancelling":
            # withdrawn while the request was in flight (the emergency stop, a rule): the order now rests on Predict under
            # a record that wants it gone — it is cancelled by the id just returned, at the next sync, and never tracked
            # as open (the answer used to overwrite the withdrawal and the order stayed on the book)
            st.update(state="cancelling", order_id=result["order_id"], placed_at=self.market.now_ms(), code=result.get("code"), cancel_sent=0)
            st["events"].append({"at": now_ms, "what": f"已下单 #{result['order_id']}（Predict 应答用时 {took:.1f} 秒），但应答到达前已被撤单"
                                                       f"（{st.get('cancel_why') or '已撤'}）：随即撤单"})
        else:
            st.update(state="open", order_id=result["order_id"], placed_at=self.market.now_ms(), code=result.get("code"))
            st["events"].append({"at": now_ms, "what": f"已下单 #{result['order_id']}（Predict 应答用时 {took:.1f} 秒）"})
        api_hash = str(result.get("hash") or "").lower()
        if api_hash and api_hash != st["hash"]:  # Predict's own hash is what its reads answer to: tracked by that one
            st["events"].append({"at": now_ms, "what": f"Predict 返回的订单哈希与本地算的不同（{api_hash[:12]}… / {st['hash'][:12]}…），按它的跟踪"})
            st["local_hash"], st["hash"] = st["hash"], api_hash
        self.sim_save([(tid, trade)])
        self.live_notify(f"📤 已向 Predict 下单：{trade['item']} {trade['label']} {core.cents(want['price'])}×{want['shares']:g} 份"
                         f"（{'限价挂单' if want['maker'] else '限价吃单'}，订单 #{result['order_id']}，"
                         f"最多花 ${from_wei(amounts['maker']):,.2f}）\n{self.sim_url(trade)}")

    def live_unsend(self, tid: str, trade: dict, why: str, now_ms: int) -> None:
        """A placement that went no further than its saved hash (the flush failed, the cooldown or Predict's 429 turned it away):
        pending again, its hash and send times void, saved at once — the store kept "placing" with the hash, and a restart made it
        an order of unknown state that never existed (1.138.1) — then waiting as a deferred order does."""
        st = trade["live"]
        st.update(state="pending", hash="")
        for k in ("sent_at", "answered_at", "answered_ms"):
            st.pop(k, None)
        self.sim_save([(tid, trade)])
        self.live_defer(tid, trade, why, now_ms)

    def live_unsent_withdrawn(self, tid: str, trade: dict, now_ms: int) -> bool:
        """A pending record the strategy withdrew while its placement waited (the sign-in, the store flush): never sent — finished
        as cancelled where it lives now (the withdrawal moved it to tid#n), with no backoff, and its market looked at again (the
        withdrawal's own re-quote found the side busy with it). 1.138.0: it was overwritten as "placing" and sent, or failed with
        LIVE_RETRY_SECONDS and saved back at the slot while its copy at tid#n stayed "cancelling" in the store."""
        st = trade["live"]
        if not (trade.get("withdrawn") or st.get("cancel_why")):
            return False
        self.live_end_unsent(tid, trade, now_ms)
        return True

    def live_end_unsent(self, tid: str, trade: dict, now_ms: int) -> str:
        """A withdrawn record whose order never went out: done, nothing filled, nothing reserved, no backoff — kept as an attempt
        (moved to tid#n where the withdrawal did not move it already) so the slot is free; the market is looked at again. The key
        it lives at."""
        st = trade["live"]
        why = f"未下单：发出前已撤单（{st.get('cancel_why') or '撤单'}）"
        st.update(state="done", final="cancelled", hash="")
        st["events"].append({"at": now_ms, "what": why + "，这张单没有发出"})
        trade.update(status="cancelled", unfilled=float(trade.get("order") or 0), note=why)
        self.live_misses.pop(tid, None)
        key = st.get("rekeyed") or tid
        if key == tid and "#" not in tid.rsplit("|", 1)[-1]:
            key = st["rekeyed"] = self.live_rekey(tid, trade)
        self.sim_save([(key, trade)])
        if trade.get("key") and (trade.get("withdrawn") or {}).get("rule"):  # the strategy's own withdrawal (as live_requote_freed): looked at again
            with contextlib.suppress(Exception):
                self.predict.updated(str(trade["key"]))
        return key

    def live_defer(self, tid: str, trade: dict, why: str, now_ms: int) -> None:
        """A pending order that cannot go out right now (the API in a cooldown, the sign-in failing): it stays pending and
        is tried again next step, until the decision is too old to send (live_send_block). One event a cause, whatever its
        countdown (1.138.0: "N 秒后重试" made every pass a new event)."""
        st = trade["live"]
        same = re.sub(r"\d+ 秒", "N 秒", st.get("last_error") or "") == re.sub(r"\d+ 秒", "N 秒", why)
        if same:
            st["last_error"] = why  # the latest countdown for the card (1.138.1), no new event
        else:
            st["last_error"] = why
            st["events"].append({"at": now_ms, "what": f"暂未发送：{why}"})
            self.sim_save([(tid, trade)])

    def live_fail(self, tid: str, trade: dict, why: str, now_ms: int, retry: int | None = None) -> None:
        """An order that never went out (or filled nothing): the record is kept under a numbered key so the journal
        shows the attempt, the slot is free for the next decision after ``retry`` seconds — LIVE_RETRY_SECONDS unless
        given (LIVE_TAKER_RETRY_SECONDS for a taker that filled nothing or was dropped by the bot's own re-check: the
        market, not Predict, said no, and the chance may be back in seconds)."""
        st = trade["live"]
        st.update(state="done", final="failed", error=why)
        st["events"].append({"at": now_ms, "what": why})
        trade.update(status="cancelled", unfilled=float(trade.get("order") or 0), note=why)
        # 1.150.0: a record failed by a path that still holds its slot key while it lives at tid#n already — failed before by
        # another path (two paths ended one order: a cancel's final read beside another's, a read answered after the record was
        # finished), or withdrawn (and moved) while this path waited on Predict — is not moved again. It was: the attempt stored
        # twice (slot#1 and slot#2, one object under both keys), and when a fresh order had taken the slot meanwhile
        # (LIVE_TAKER_RETRY_SECONDS may be as short as 1 s) sim_rekey's replace of sim:<slot> deleted THAT live order's record —
        # resting on Predict with no record, its fills booked nowhere, outside LIVE_MAX_OPEN_USD. Saved where it lives instead
        # (the callers write nothing more under the key they hold). Only this very object at its numbered key counts as moved:
        # a copy saved back at the slot with an old "rekeyed" is moved as before
        moved = st.get("rekeyed")
        if moved and moved != tid and self.sim_trades().get(moved) is trade:
            self.sim_save([(moved, trade)])
        else:
            st["rekeyed"] = self.live_rekey(tid, trade)
        wait = self.config.live_retry if retry is None else retry
        self.live_backoff[(trade["market"], trade["side"], "挂" if trade.get("maker") else "吃")] = (time.monotonic() + wait, why)
        self.live_note_error(f"{trade['item']} {trade['label']}：{why}")
        if wait:
            self.live_notify(f"⚠️ {trade['item']} {trade['label']} {why}；{wait} 秒后再看。", key=f"fail:{trade['market']}:{trade['side']}:{'挂' if trade.get('maker') else '吃'}")

    def live_rekey(self, tid: str, trade: dict) -> str:
        """Move a record to tid#n, freeing the paper trader's one slot per market, side and way."""
        return self.sim_rekey(tid, trade)

    def live_save(self, tid: str, trade: dict) -> None:
        """A record a pass over the orders changed (live_sync_orders, live_cancel_blind), saved where it lives: under the key
        the pass holds — or, moved to tid#n since the pass took that key, there; never again under the old key (the slot may
        hold a fresh order by now)."""
        moved = trade["live"].get("rekeyed")
        if not moved or moved == tid:
            return self.sim_save([(tid, trade)])
        # 1.150.0: moved by live_fail (a taker that filled nothing: saved there as it moved) — or withdrawn with nothing filled
        # (sim_rekey: the express lane's look) while the pass, begun before, still held the slot key. Such a pass saved nothing,
        # and with one path at a time on a cancel (live_cancels) it may be the only one that acts: the step's sync has its open
        # list on its way, the lane withdraws the order, the sync gets to the cancel first and ends the record on Predict's
        # confirmation — the lane leaves the order to it, and the fills read saves only a record Predict still returns
        # (live_cancel_fills_read). The store kept the row "cancelling", its cancel unsent, beside a record done in memory: after
        # a restart the removal went out again ("noop"), and a single-order read that no longer knew the order made the
        # confirmed cancel "unknown" — its $ reserved, the side blocked for minutes, a false ❓ alert. Only this very object at
        # its numbered key is written: a record no longer there (pruned, the view rebuilt from the store) is not put back
        if self.sim_trades().get(moved) is trade:
            self.sim_save([(moved, trade)])

    # --- reading back: fills, cancels, the final state of each order ---------------------------------------------------
    async def live_sync(self, now_ms: int) -> None:
        trades = self.sim_trades()
        self.live_check_kill(trades, now_ms)  # first, and never behind the open-order read: a list that cannot be read must not keep the switch from tripping
        active = {tid: t for tid, t in trades.items() if isinstance(t.get("live"), dict) and t["live"].get("state") in PLACING
                  and t["live"].get("state") != "pending"}
        if active and not self.live.ready:
            await self.live_ensure_ready()  # real orders are followed to their end whatever the mode (LIVE=off included)
        if not self.live.signed_in:
            return  # 1.150.0: the sign-in is enough (LiveTrader.signed_in): the on-chain account check holds new orders, never these reads and cancels
        await self.live_renew_auth()
        if active or now_ms - self.live_list_at_ms >= self.LIVE_IDLE_LIST_MS:
            cancelling = [tid for tid, t in active.items() if t["live"].get("state") == "cancelling"]
            await self.live_sync_orders(active, now_ms)  # with nothing in flight the list is still read now and then: orders this bot did not place
            await self.live_requote_freed(cancelling, now_ms)  # the slots the confirmed cancels freed: quoted again now (once ready), not next step
        try:
            await self.live_late_fills(now_ms)  # 1.119.0: an order that ended with shares unfilled, against the account's positions
        except Exception as error:
            self.live_note_error(f"结束订单核对持仓失败：{core.clean_error(error) or type(error).__name__}")
        self.live_redeem_tick(now_ms)  # also on its own timer (live_redeem_loop): the step is not the only way a round starts

    LIVE_AUTH_RENEW_S = 900  # the sign-in token is renewed this long before it expires, in the background

    async def live_renew_auth(self) -> None:
        """A token about to expire is renewed here, between the steps, rather than by the order that finds it expired
        (that order then waited for two sign-in requests first). Tried once a minute at most; a failure is the next
        request's to report."""
        api = self.live.api
        exp = float(getattr(api, "jwt_exp", 0) or 0)
        if not getattr(api, "jwt", "") or not exp or not 0 < exp - time.time() < self.LIVE_AUTH_RENEW_S:
            return
        if time.monotonic() - self.live_auth_tried < 60:
            return
        self.live_auth_tried = time.monotonic()
        with contextlib.suppress(Exception):
            await api.authenticate()

    def live_latency(self, last: int = 30) -> dict | None:
        """The order path of the last ``last`` real orders sent, in seconds (median and longest): 盘口→决策 (the book's
        read to the decision), 决策→发出 (to the request), 发出→订单号 (Predict's answer); 撤单→重挂发出 for the orders
        that took a cancelled one's place (live_requote_freed: from that one's withdrawal to this one's request, the
        cancel's round trip included), with its parts' medians (``parts``: the withdrawal decision to the cancel request,
        the request to Predict's confirmation, the confirmation to the new order's request). None before any was sent."""
        rows = []
        for t in self.sim_trades().values():
            st = t.get("live")
            if not isinstance(st, dict) or not st.get("sent_at") or not st.get("answered_at"):
                continue
            want = st.get("want") or {}
            rows.append((int(t.get("opened") or 0), int(want.get("book_ms") or 0), int(st["sent_at"]), int(st["answered_at"]),
                         int(st.get("withdrawn_at") or 0), int(st.get("requote_cancel_sent_at") or 0), int(st.get("requote_cancel_done_at") or 0),
                         st.get("flush_ms")))
        rows = sorted(rows, key=lambda r: r[2])[-last:]
        if not rows:
            return None

        def span(values: list) -> dict | None:
            values = sorted(v for v in values if v is not None and v >= 0)
            return {"median": core.median(values) / 1000, "max": values[-1] / 1000} if values else None
        chain = [(w, cs, cd, s) for o, b, s, a, w, cs, cd, _ in rows if w and cs and cd and w <= cs <= cd <= s]
        parts = None
        if chain:
            mid = lambda values: core.median(sorted(values)) / 1000
            parts = (mid([cs - w for w, cs, cd, s in chain]), mid([cd - cs for w, cs, cd, s in chain]), mid([s - cd for w, cs, cd, s in chain]))
        return {"n": len(rows), "book": span([r[0] - r[1] for r in rows if r[1] and r[0]]), "send": span([r[2] - r[0] for r in rows if r[0]]),
                "answer": span([r[3] - r[2] for r in rows]), "requote": span([r[2] - r[4] for r in rows if r[4]]),
                "requotes": sum(1 for r in rows if r[4]), "parts": parts,
                "flush": span([int(r[7]) for r in rows if r[7] is not None])}  # the wait for the writer before the request (1.111.1)

    def live_redeem_tick(self, now_ms: int | None = None) -> None:
        """An automatic redeem round started when one is due (LIVE_REDEEM_SECONDS since the last, sooner after a win:
        live_redeem_soon) and none runs — beside the caller: a round of chain calls (a dozen positions failing on gas took
        25 s) never holds a decision. Called by the trading step and by the redeem's own loop."""
        if not self.config.live_auto_redeem or self.live_redeem_shut():
            return
        if time.monotonic() - self.live_last_redeem < self.LIVE_REDEEM_SECONDS or (self.live_redeem_task is not None and not self.live_redeem_task.done()):
            return
        self.live_last_redeem = time.monotonic()
        self.live_redeem_task = asyncio.create_task(self.live_redeem_round(self.market.now_ms() if now_ms is None else now_ms), name="live-redeem")

    def live_redeem_shut(self) -> str:
        """Why no automatic redeem round can start now, "" when one can (LIVE_AUTO_REDEEM aside): live_redeem_tick's gate, and
        the words live_redeem_status_line says it in — one test for both, so the line follows the gate (1.150.0). LIVE=off
        and not ready: an off process does not sign in to Predict by itself (off 只记账): started in off it stays so until a
        command signs in (/live redeem, /live redeem check, /live check) or /live mode pause|on; switched to off while
        running it keeps the sign-in it had, and its rounds go on."""
        if self.config.live_mode == "off" and not self.live.ready:
            return f"LIVE=off 且未就绪（{self.live.not_ready_why}）"
        return ""

    async def live_redeem_loop(self) -> None:
        """The automatic redeem's own timer (1.131.4): it rode on the paper trader's step only, so with SIM=off (the
        pre-open mode alone) or the step held up, nothing was ever redeemed."""
        while not self.stopping.is_set():
            try:
                self.live_redeem_tick()
            except Exception as error:
                self.live_redeem_note("loop", f"自动领取计时出错：{core.clean_error(error) or type(error).__name__}")
            await self.wait(self.LIVE_REDEEM_TICK_SECONDS)

    async def live_redeem_round(self, now_ms: int) -> None:
        # 1.150.0 (audit RDM-ROUND-VOID): the tick stamps live_last_redeem before the round runs, and a round that could not look —
        # the key's 20 s cooldown (any client's 429: the pre-open mode's orders and cancels share it), one failed positions read,
        # the sign-in not there yet — or whose market reads a rate limit cut short left every waiting winner ten minutes more,
        # the round live_redeem_soon had brought included. Such a round now asks for the next one soon (live_redeem_again: after
        # the cooldown, at most LIVE_REDEEM_AGAIN_MAX times in a row). A failure in the middle of the pass keeps the usual cadence
        # under its own label — what it sent is marked, the rest is not hurried — and 读取持仓失败 names the positions read alone.
        self.live_redeem_auto_at = now_ms  # (RDM-STATUS-LINE) when the last automatic round began, whatever a manual one wrote since
        try:
            if not self.live.ready:
                await self.live_ensure_ready()  # signed in by the step otherwise: with the step off, here
                if not self.live.ready:
                    self.live_redeem_last = {"at": now_ms, "error": f"真实交易未就绪：{self.live.not_ready_why}"}
                    # the sign-in is tried once a minute (live_ensure_ready): the next round past that minute, not ten on
                    self.live_redeem_again(65 - (time.monotonic() - getattr(self, "live_last_signin", -1e9)))
                    return
            await self.live_redeem(now_ms)
        except RedeemUnread as error:  # the list itself: nothing judged, nothing sent
            text = core.clean_error(error) or type(error).__name__
            self.live_redeem_last = {"at": now_ms, "error": f"读取持仓失败：{text}"}
            self.live_redeem_note("round", f"领取结算失败：{text}")
            self.live_redeem_again()
        except Exception as error:  # in the middle of the pass: the usual cadence
            text = core.clean_error(error) or type(error).__name__
            self.live_redeem_last = {"at": now_ms, "error": f"这一轮中途出错：{text}"}
            self.live_redeem_note("round", f"领取结算失败：{text}")
        else:
            if (self.live_redeem_last or {}).get("limited"):
                self.live_redeem_again()  # the rate limit ended the market reads: the ones left once the cooldown is over, not ten minutes on
            else:
                self.live_redeem_again_n = 0  # a round that looked: the usual cadence, and the next trouble has its early rounds again

    def live_redeem_pull(self, seconds: float) -> None:
        """The next automatic redeem round within ``seconds`` (1.150.0): never later than one already due (the min, as
        live_redeem_soon — neither undoes the other), and never inside the API key's 429 cooldown — the end of it plus
        LIVE_REDEEM_AGAIN_SECONDS at the soonest, a round would only be refused there or start the next 429. Only the
        clock moves: nothing is read or sent here, and that round redeems only what has no mark and no back-off. Its
        caller bounds how often (live_redeem_again). Not for the retry of a redeem that never left: every such round is a
        positions read and market reads more on the key the orders share (live_redeem_pass, RDM-UNSENT-RETRY-GRID)."""
        cooling = max(0.0, float(getattr(self.live.api, "blocked_until", 0.0) or 0.0) - time.monotonic())
        wait = max(0.0, float(seconds), (cooling + self.LIVE_REDEEM_AGAIN_SECONDS) if cooling else 0.0)
        self.live_last_redeem = min(self.live_last_redeem, time.monotonic() - self.LIVE_REDEEM_SECONDS + min(float(self.LIVE_REDEEM_SECONDS), wait))

    def live_redeem_again(self, floor: float = 0.0) -> None:
        """The round that just ended judged nothing, or not everything, for a reason that passes by itself — the key's
        rate-limit cooldown, the positions list not read, the sign-in not there yet: the next automatic round
        LIVE_REDEEM_AGAIN_SECONDS after the cooldown ends (``floor`` at the soonest), the wait doubling each time, at most
        LIVE_REDEEM_AGAIN_MAX times in a row; then the usual cadence. When no round has looked at the positions for
        LIVE_REDEEM_STUCK_SECONDS (a cause that does not pass: /positions down for an hour, the key limited round after
        round) it is said on Telegram, hourly at most — a void round itself was never pushed."""
        n = self.live_redeem_again_n = getattr(self, "live_redeem_again_n", 0) + 1
        stuck = time.monotonic() - getattr(self, "live_redeem_whole_at", time.monotonic())
        if stuck >= self.LIVE_REDEEM_STUCK_SECONDS and time.monotonic() - getattr(self, "live_redeem_stuck_noted", -1e9) >= self.LIVE_REDEEM_NOTE_SECONDS:
            self.live_redeem_stuck_noted = time.monotonic()
            self.live_notify(f"⚠️ 自动领取：已 {int(stuck // 60)} 分钟没能看完持仓（{self.live_redeem_summary(self.live_redeem_last)}），"
                             "赢的暂时领不了；恢复后自动继续。/live redeem check 查看原因", key="redeem:read")
        if n > self.LIVE_REDEEM_AGAIN_MAX:
            return
        cooling = max(0.0, float(getattr(self.live.api, "blocked_until", 0.0) or 0.0) - time.monotonic())
        self.live_redeem_pull(max(float(floor), cooling + self.LIVE_REDEEM_AGAIN_SECONDS * 2 ** (n - 1)))

    def live_redeem_soon(self, market_id: Any = "") -> None:
        """A real position just settled in the bot's favour (the pre-open mode's window, a record on Predict's final
        result): the next automatic redeem round within LIVE_REDEEM_SOON_SECONDS, not up to LIVE_REDEEM_SECONDS later —
        the USDT back on the account for the next quotes (1.131.3). 1.150.0 (audit RDM-SOON-NO-TARGET): ``market_id``, the
        market that settled, is read first by the rounds to come (live_redeem_first) — it only moved the clock, and with
        more than LIVE_REDEEM_READS markets unjudged the round it brought spent its reads on the longest unread ones and
        the win waited for a later round. Once read it goes back into the rotation (kept through its first
        LIVE_REDEEM_UNSENT_NAMED redeems in a row that never left: live_redeem_pass), so names never take every round's reads."""
        if market_id not in (None, ""):
            first = self.__dict__.setdefault("live_redeem_first", {})
            first[str(market_id)] = time.monotonic()
            while len(first) > 10 * self.LIVE_REDEEM_READS:  # (bounded: the oldest name goes, its market keeps its turn in the rotation)
                del first[min(first, key=first.get)]
        self.live_last_redeem = min(self.live_last_redeem, time.monotonic() - self.LIVE_REDEEM_SECONDS + self.LIVE_REDEEM_SOON_SECONDS)

    def live_redeem_retry_words(self, wait: float) -> str:
        """When a redeem that never left is tried again, counted from its failure (1.150.0, audit RDM-UNSENT-RETRY-GRID): by
        the next automatic round after its ``wait`` — at the soonest the wait's end, at the latest a round's interval after
        it: a round that comes inside the wait (another win's) finds it waiting and starts LIVE_REDEEM_SECONDS afresh. 「5 分钟
        后再试」 named the wait, not the retry (five to fifteen minutes on), and 「最迟约 10 分钟」 left the wait out."""
        return f"{max(1, round(wait / 60))}～{round((wait + self.LIVE_REDEEM_SECONDS) / 60)} 分钟后的下一轮"

    async def live_sync_orders(self, active: dict[str, dict], now_ms: int) -> None:
        # 1.137.1: the step reads the pages until every active record's order is seen (LIVE_STEP_PAGES at most) whenever the pre-open
        # snapshot stands for the whole account, or within a minute of this bot's own whole read; the rest of the records are read by
        # their hash (live_track). The 11-07 /diag: 同步 10.5 s of an 11.3 s step, twenty pages against the pre-open mode's thousands
        wanted = {str(t["live"].get("order_id")) for t in active.values() if t["live"].get("order_id")}
        latched = self.live_full_cut and getattr(self, "preopen", None) is not None  # 1.137.2: a cut full read tells nothing the snapshot will not
        # 1.150.0: ...which holds while a snapshot comes. live_full_cut is re-read by a whole read alone, and that clause allowed none
        # for as long as a controller was attached — but one stays attached with no snapshot to give: crashed (alive False, kept
        # until the next preopen_apply: a control-page save, /preopen on|off, a restart), paused (/preopen pause), held by an
        # operator's stop or the day's loss cap (its account loop reads nothing while blocked), or its reads failing. The sync then
        # stayed bounded for good and the account was never seen whole again, the model trader trading on: an order of its own that
        # vanished never counted as missing, nor one of unknown state released by the evidence (live_counts_miss), an unrecorded
        # order never seen or swept (live_note_unknown, live_orphan_sweep) — with that mode's orders withdrawn long since, the list
        # would have read whole. So with no whole view — this bot's own or the snapshot's — and no whole read of its own for
        # LIVE_SNAPSHOT_MAX_AGE_MS (the age at which a snapshot stops standing for the account), the clause lapses for one whole
        # read: whole, the flag clears and the read is every minute's again, as without that mode; cut once more, the next is that
        # long away — one in ten minutes, and only while nothing else shows the account
        blind = (now_ms - int(self.live_whole[2] or 0) > self.LIVE_SNAPSHOT_MAX_AGE_MS
                 and now_ms - self.live_full_at_ms >= self.LIVE_SNAPSHOT_MAX_AGE_MS)
        bounded = (self.live_snapshot_fresh(now_ms) or now_ms - self.live_full_at_ms < self.LIVE_IDLE_LIST_MS
                   or (latched and not blind))
        if latched and not bounded:
            # ...whether that read succeeds or not: against that mode's thousands it is twenty pages, any of which may fail — asked
            # for again at every step, a failing one would fail every step's sync with it (the records unread meanwhile)
            self.live_full_at_ms = now_ms
        listing = time.monotonic()
        # 1.139.0: only the markets the model trader has orders on (the index and contract cards), when Predict filters the list by market —
        # every one of those orders seen every step; the pre-open mode's thousands on other markets never read here
        order_markets = sorted({str(t.get("market_id") or (t["live"].get("want") or {}).get("market_id") or "") for t in active.values()} - {""})
        by_market = bounded and self.live_market_filter is not False and 0 < len(order_markets) <= self.LIVE_MARKET_READS
        try:
            open_rows = await self.live_orders_by_market(order_markets) if by_market else None
            if open_rows is None:
                by_market = False
                open_rows = await (self.live.api.orders("OPEN", stop_ids=wanted, max_pages=self.LIVE_STEP_PAGES) if bounded else self.live.api.orders("OPEN"))
            else:
                self.live.api.orders_complete = False  # the chosen markets' lists, whole each — never the whole account (live_full_list: the snapshot)
        except Exception:
            self.live_sync_failed_ms = now_ms  # a failure after the last success: no new order until a read succeeds (live_send_hold)
            await self.live_cancel_blind(active, now_ms)  # the cancels need no list: a list that cannot be read must not keep an order resting
            with contextlib.suppress(Exception):  # 1.138.1: the slots those confirmed cancels freed are quoted again (the gates judge the hold)
                await self.live_requote_freed([tid for tid, t in active.items() if t["live"].get("state") == "done"], now_ms)
            raise
        list_s = time.monotonic() - listing
        if not bounded:
            self.live_full_cut = not getattr(self.live.api, "orders_complete", True)
        if not bounded or getattr(self.live.api, "orders_complete", True):
            self.live_full_at_ms = now_ms
        by_id = {str(r.get("id")): r for r in open_rows}
        by_hash = {order_hash_of(r): r for r in open_rows if order_hash_of(r)}
        self.live_readopt(open_rows, by_id, by_hash, now_ms, now_ms)
        full_rows, full_at = self.live_full_list(open_rows, now_ms)  # the whole account (1.136.2): this read when whole, else the pre-open mode's snapshot
        if full_rows is not None:
            self.live_whole = ({str(r.get("id")) for r in full_rows}, {order_hash_of(r) for r in full_rows if order_hash_of(r)}, full_at)
            self.live_open_ids, self.live_open_ids_at = {str(r.get("id")) for r in full_rows}, full_at
            if full_rows is not open_rows:
                self.live_readopt(full_rows, {str(r.get("id")): r for r in full_rows}, {order_hash_of(r): r for r in full_rows if order_hash_of(r)}, now_ms, full_at)
        if full_rows is not None:  # 1.138.1: only a whole view says which orders are unrecorded — a cut list emptied it, and an order
            self.live_note_unknown(full_rows, now_ms)  # of unknown state could be released over the stray order that may be it
        self.live_synced_at = self.live_list_at_ms = now_ms
        markets: dict[str, core.SimMarket] = {}
        with contextlib.suppress(Exception):  # the records' own cards alone (1.112.1: every card used to be priced for this, each step)
            for key in sorted({str(t.get("key") or "") for t in active.values() if t.get("key")}):
                markets.update({mk.market: mk for mk in self.sim_markets_for(key, now_ms)})

        async def one(tid: str, trade: dict) -> None:
            st = trade["live"]
            before = json.dumps(trade, sort_keys=True, default=str)
            try:
                if st["state"] == "placing":
                    await self.live_reconcile(tid, trade, by_hash, now_ms)
                elif st["state"] == "open":
                    await self.live_track(tid, trade, by_id, markets.get(trade["market"]), now_ms)
                elif st["state"] == "cancelling":
                    await self.live_cancel(tid, trade, by_id, markets.get(trade["market"]), now_ms)
                elif st["state"] == "unknown":
                    await self.live_recheck(tid, trade, by_id, by_hash, markets.get(trade["market"]), now_ms)
            except Exception as error:
                st["last_error"] = core.clean_error(error) or type(error).__name__
                self.live_note_error(f"{trade['item']} {trade['label']} 订单同步失败：{st['last_error']}")
            if json.dumps(trade, sort_keys=True, default=str) != before:
                self.live_save(tid, trade)  # where it lives: moved to an attempt record (tid#n) meanwhile, there — never again under the old key
        # 1.137.2: a resting maker order the list did not show is read by its hash LIVE_TRACK_READS a step at most, each at most every
        # LIVE_TRACK_READ_MS, the longest unread first — seventeen of them read every step, two at a time, were most of the sync's seconds
        unseen = sorted((tid for tid, t in active.items() if t["live"].get("state") == "open" and t.get("maker")
                         and str(t["live"].get("order_id") or "") not in by_id), key=lambda tid: self.live_track_reads.get(tid, 0))
        later: set[str] = set()
        reads = 0
        for tid in unseen:
            if reads < self.LIVE_TRACK_READS and now_ms - self.live_track_reads.get(tid, -10 ** 15) >= self.LIVE_TRACK_READ_MS:
                reads += 1
                self.live_track_reads[tid] = now_ms
            else:
                later.add(tid)
        self.live_track_reads = {tid: at for tid, at in self.live_track_reads.items() if tid in active}
        # side by side (1.112.1: one after another, each record's reads and cancels added up to the step's 同步 seconds); the
        # API gate bounds them, and a record's own failure is noted on it alone
        working = time.monotonic()
        await asyncio.gather(*(one(tid, trade) for tid, trade in list(active.items()) if tid not in later))
        page = int(getattr(self.live.api, "ORDERS_PAGE", 50) or 50)
        self.live_sync_detail = {"pages": -(-len(open_rows) // page) if open_rows else 1, "bounded": bounded, "list_s": list_s,
                                 "markets": len(order_markets) if by_market else 0, "filter": self.live_market_filter,
                                 "reads": reads, "later": len(later), "work_s": time.monotonic() - working, "records": len(active)}
        await self.live_orphan_sweep(now_ms)

    def live_request_rates(self) -> list[str]:
        """The requests a second over the last minute of each Predict client on this API key (the rate limit is the key's):
        the orderbook feed, this trader, the pre-open mode — for /diag."""
        out = []
        samples = self.live_rate_samples
        if len(samples) >= 2 and samples[-1][0] > samples[0][0]:
            out.append(f"原策略 {(samples[-1][1] - samples[0][1]) / (samples[-1][0] - samples[0][0]):.1f}")
        feed = getattr(getattr(self, "predict", None), "rate", None)
        if callable(feed) and (rate := feed()) is not None:
            out.append(f"盘口 {rate:.1f}")
        preopen = getattr(self, "preopen", None)
        if preopen is not None and (rate := preopen.request_rate()) is not None:
            out.append(f"预挂 {rate:.1f}")
        return out

    def live_limited_counts(self) -> str:
        """The 429s answered on this API key in the last 24 hours, by client — /diag (1.138.0): whether the key is over Predict's limit."""
        day = time.monotonic() - 86400
        parts = [("原策略", sum(1 for t in getattr(self.live.api, "limited", ()) if t >= day))]
        feed = getattr(getattr(self, "predict", None), "limited", None)
        if feed is not None:
            parts.append(("盘口", sum(1 for t in feed if t >= day)))
        preopen = getattr(self, "preopen", None)
        if preopen is not None:
            parts.append(("预挂", sum(1 for t in getattr(preopen, "limited_at", ()) if preopen.now() - t <= core.DAY_MS)))
        return "、".join(f"{name} {n} 次" for name, n in parts)

    @staticmethod
    def row_market_id(row: dict) -> str:
        """The market an open-order row is on, as the row states it ("" when it does not)."""
        market = row.get("market") if isinstance(row.get("market"), dict) else {}
        return str(market.get("id") or row.get("marketId") or row.get("market_id") or "")

    async def live_orders_by_market(self, markets: list[str]) -> list[dict] | None:
        """The open orders of ``markets`` only — GET /orders?marketId=… for each, side by side — or None when Predict does not filter by
        market (1.139.0). Whether it does is learnt from the answers: every row on the market asked for, and at least one row seen, says
        it does; a row on another market (the parameter ignored: the account's whole list), or one that does not say its market, says
        it does not — then the bounded read is used for good. Until it is known, each market is read one page deep (an ignored parameter
        must not page through the pre-open mode's thousands)."""
        api = self.live.api
        pages = self.LIVE_MARKET_PAGES if self.live_market_filter else 1

        async def one(mid: str) -> list[dict]:
            query = {"status": "OPEN", "marketId": mid}
            if hasattr(api, "paged"):
                rows, _ = await api.paged("/orders", query, max_pages=pages)  # (paged: no shared completeness flag between the parallel reads)
                return rows
            return await api.orders("OPEN", marketId=mid)
        answers = await asyncio.gather(*(one(mid) for mid in markets))
        seen = False
        for mid, rows in zip(markets, answers):
            for row in rows:
                if self.row_market_id(row) != mid:
                    if self.live_market_filter is not False:  # not an error: /diag says it (同步内：Predict 按市场过滤：不支持)
                        LOG.info("Predict 挂单列表不按市场过滤（或不注明市场），同步改回按页读")
                    self.live_market_filter = False
                    return None
                seen = True
        if seen and not self.live_market_filter:
            self.live_market_filter = True
        return [row for rows in answers for row in rows]

    def live_full_list(self, open_rows: list[dict], now_ms: int) -> tuple[list[dict] | None, int]:
        """Every open order of the account and when that list was read: this step's read when it came whole, else the
        pre-open mode's last complete snapshot (it pages through everything every minute or two). 1.136.2: with that
        mode's thousands of resting pairs this bot's own read — ORDERS_PAGES_MAX pages — never was whole, so an order
        beyond them was never taken back by live_readopt when its record wrongly ended, never reported as unrecorded, and
        sim_prune's guard never cleared. (None, 0) when neither is to be had (the snapshot older than LIVE_SNAPSHOT_MAX_AGE_MS)."""
        if getattr(self.live.api, "orders_complete", True):
            return open_rows, now_ms
        if self.live_snapshot_fresh(now_ms):
            return list(self.preopen.account_rows), int(self.preopen.account_at)
        return None, 0

    def live_snapshot_fresh(self, now_ms: int) -> bool:
        """Whether the pre-open mode's last complete account snapshot is young enough to stand for the whole account."""
        preopen = getattr(self, "preopen", None)
        at = int(getattr(preopen, "account_at", 0) or 0)
        return preopen is not None and getattr(preopen, "account_rows", None) is not None and bool(at) and 0 <= now_ms - at <= self.LIVE_SNAPSHOT_MAX_AGE_MS

    def live_known_markets(self) -> tuple[dict[str, str], dict[str, str]]:
        """(market id -> name, outcome token -> name) of every market this bot trades or traded for real: its records,
        the pre-open mode's windows.

        1.150.x: not the market-details cache (self.live.markets) any more: live_prefetch reads it for any card whose
        suggestion is merely near the bar, and /live test for its test market, so a market the bot only priced counted as
        its own for the rest of the run and the operator's manual order there was withdrawn by the orphan sweep, against
        the documented promise that only the markets the bot 做过 are swept. Every real order has a record before it is
        sent (the "pending" record carries market_id and want.token) and sim_prune keeps any record whose order may still
        rest, so the records and the ledger still cover the lost-tracking case the sweep exists for."""
        ids: dict[str, str] = {}
        tokens: dict[str, str] = {}
        for t in self.sim_trades().values():
            st = t.get("live")
            if not isinstance(st, dict):
                continue
            want = st.get("want") or {}
            name = str(t.get("item") or t.get("market") or "")
            mid = str(want.get("market_id") or t.get("market_id") or "")
            if mid:
                ids[mid] = name or mid
            if want.get("token"):
                tokens[str(want["token"])] = name or mid
        ledger = getattr(getattr(self, "preopen", None), "ledger", None)
        for pair in (getattr(ledger, "rows", None) or {}).values():
            mid = str(pair.get("market_id") or "")
            name = str(pair.get("name") or pair.get("slug") or mid)
            if mid:
                ids[mid] = name
            for o in (pair.get("orders") or {}).values():
                token = str((((o.get("body") or {}).get("data") or {}).get("order") or {}).get("tokenId") or "")
                if token:
                    tokens[token] = name
        return ids, tokens

    def live_market_of(self, row: dict, known: tuple[dict, dict] | None = None) -> str:
        """Which of this bot's markets an open order is on ("" when none): by the market id the row carries, or by the
        outcome token the order names. An order on a market neither strategy ever touched is somebody's manual order."""
        market = row.get("market") if isinstance(row.get("market"), dict) else {}
        order = row.get("order") if isinstance(row.get("order"), dict) else {}
        mid, token = str(market.get("id") or row.get("marketId") or ""), str(order.get("tokenId") or "")
        ids, tokens = known or self.live_known_markets()
        return ids.get(mid, "") if mid and mid in ids else tokens.get(token, "") if token else ""

    def live_remove_limit(self) -> RemoveLimit:
        """Predict's measured removal batch (RemoveLimit, 1.141.0): one for every client on the key, kept in the store."""
        limit = getattr(self, "_remove_limit", None)
        if limit is None:
            store = getattr(self, "store", None)
            limit = self._remove_limit = RemoveLimit.from_dict(store.get("predict_remove_limit") if store is not None else None)
        return limit

    def live_remove_saved(self, limit: RemoveLimit) -> None:
        """The measured batch written back once it changed."""
        store = getattr(self, "store", None)
        if limit.changed and store is not None:
            limit.changed = False
            with contextlib.suppress(Exception):
                store.put("predict_remove_limit", limit.as_dict())

    async def live_orphan_sweep(self, now_ms: int) -> None:
        """The account's unrecorded open orders on this bot's own markets, seen so for LIVE_ORPHAN_GRACE_MS, are
        withdrawn (LIVE_ORPHAN_CANCEL, 1.136.2): a record lost or wrongly ended leaves its order resting as one nobody
        manages — it fills when the market turns against it (the 1¢ SHEIN fill, the two 恒生 bids of 11-07) and the fill
        is booked nowhere. One on a market neither strategy touched is left alone (a manual order), only reported."""
        if not getattr(self.config, "live_orphan_cancel", True) or not self.live_unknown_first or now_ms - self.live_orphan_swept_ms < self.LIVE_ORPHAN_EVERY_MS:
            return
        self.live_orphan_swept_ms = now_ms
        waited = [i for i, first in self.live_unknown_first.items() if now_ms - first >= self.LIVE_ORPHAN_GRACE_MS]
        known = self.live_known_markets() if waited else ({}, {})  # the records are walked once a sweep, not once an order
        due = {i: self.live_market_of(self.live_unrecorded_rows.get(i) or {}, known) for i in waited}
        due = {i: name for i, name in due.items() if name}
        if not due:
            return
        from preopen import order_brief
        ids = list(due)[:50]
        limit = self.live_remove_limit()
        result = await remove_in_batches(self.live.api, ids, limit, workers=2, patient=False, stop=self.stopping.is_set)  # 1.141.0: at the measured batch size
        self.live_remove_saved(limit)
        unsent = set(result["failed"]) | set(result["left"])  # the request failed or never went: asked again at the next sweep
        if unsent and not (result["removed"] or result["noop"] or result["unanswered"]):
            self.live_note_error("撤无记录挂单失败：" + (result["errors"][-1] if result["errors"] else "接口限流冷却中，下一轮再撤"))
            return
        ids = [i for i in ids if i not in unsent]
        removed = [i for i in ids if i in set(result["removed"])]
        noop = [i for i in ids if i not in removed]
        for i in ids:  # answered either way: not asked for again now (a noop is gone, or seen again on the next list and timed afresh)
            self.live_unknown_first.pop(i, None)
        self.live_unknown_open = [u for u in self.live_unknown_open if u["id"] not in set(ids)]
        briefs = {i: order_brief(self.live_unrecorded_rows.get(i) or {"id": i}) + (f" {due[i]}" if due[i] not in order_brief(self.live_unrecorded_rows.get(i) or {"id": i}) else "")
                  for i in removed}
        self.live_orphans = [*self.live_orphans[-49:], *({"at": now_ms, "id": i, "brief": briefs[i]} for i in removed)]
        if removed:
            self.live_notify(f"🧹 撤掉 {len(removed)} 张本机器人没有记录、却挂在本机器人市场上的挂单（丢失跟踪的单；LIVE_ORPHAN_CANCEL=off 可关）："
                             + "；".join(briefs[i] for i in removed[:5]) + (f"；Predict 称 {len(noop)} 张无需撤" if noop else ""), key="orphans:" + ",".join(removed[:5]))
        elif noop:
            self.live_note_error(f"撤无记录挂单：Predict 称 {len(noop)} 张无需撤（#{'、#'.join(noop[:5])}）")

    async def live_cancel_blind(self, active: dict[str, dict], now_ms: int) -> None:
        """The open list could not be read (or the express lane cannot wait for one): the withdrawn records whose order id
        is known get their cancels sent now, side by side (remove_orders needs no list; the API gate bounds them). Each
        is saved as it changes; a failure is noted on the record, never raised."""
        markets: dict[str, core.SimMarket] = {}
        with contextlib.suppress(Exception):  # the cancels' own cards alone (1.111.0: every card used to be priced for this)
            for key in sorted({str(t.get("key") or "") for t in active.values() if t.get("key")}):
                markets.update({mk.market: mk for mk in self.sim_markets_for(key, now_ms)})

        async def one(tid: str, trade: dict) -> None:
            st = trade["live"]
            if st.get("state") != "cancelling" or (not st.get("order_id") and st.get("hash")) or tid in self.live_placing:
                return  # (no id and no hash: never sent — live_cancel closes it without a request, 1.138.1)
            before = json.dumps(trade, sort_keys=True, default=str)
            try:
                await self.live_cancel(tid, trade, {}, markets.get(trade["market"]), now_ms)
            except Exception as error:
                st["last_error"] = core.clean_error(error) or type(error).__name__
            if json.dumps(trade, sort_keys=True, default=str) != before:
                self.live_save(tid, trade)  # moved to an attempt record meanwhile: there, never again under the old key (1.138.1, as live_sync_orders)
        await asyncio.gather(*(one(tid, trade) for tid, trade in active.items()))

    def live_fills_later(self, tid: str, trade: dict, mk: Any) -> None:
        """The fills of an order Predict just confirmed removed, read in a task beside the lane (live_cancel_fills)."""
        task = asyncio.create_task(self.live_cancel_fills(tid, trade, mk), name=f"cancel-fills:{tid}")
        self.live_fill_tasks.add(task)
        task.add_done_callback(self.live_fill_tasks.discard)

    async def live_cancel_fills(self, tid: str, trade: dict, mk: Any) -> None:
        """What a removed order filled before its cancel (GET by its hash), onto its record — found by the order id, under
        whatever tid it has by now (the slot's fresh order may have moved it to tid#n); a position's status follows."""
        st = trade["live"]
        order_id, hash_ = str(st.get("order_id") or ""), str(st.get("hash") or "")
        try:
            await self.live_cancel_fills_read(order_id, hash_, mk)
        finally:
            self.live_requote_looked.discard(order_id)  # 1.138.1: on every end of the read (a failure kept it: the set filled up)

    async def live_cancel_fills_read(self, order_id: str, hash_: str, mk: Any) -> None:
        for attempt in range(3):  # 1.138.0: tried again — one failed read kept the reservation (and the slot's re-quote cut short) for minutes
            try:
                final = await self.live.api.order(order_id, hash_)
                break
            except Exception as error:
                if attempt == 2:
                    self.live_note_error(f"撤单后读成交失败（#{order_id}）：{core.clean_error(error) or type(error).__name__}")
                    return
                await asyncio.sleep(max(1.0 + attempt, float(getattr(error, "retry_after", 0) or 0)))
        if not isinstance(final, dict):
            return
        trades = self.sim_trades()
        cur = next(((k, t) for k, t in trades.items() if isinstance(t.get("live"), dict) and str(t["live"].get("order_id") or "") == order_id), None)
        if cur is None:
            return
        tid, trade = cur
        st, now_ms = trade["live"], self.market.now_ms()
        before = float(trade.get("shares") or 0)
        self.live_apply(tid, trade, final, mk, now_ms)
        st.pop("fills_pending", None)  # read: the reservation is what filled
        after, order = float(trade.get("shares") or 0), float(trade.get("order") or 0)
        if after > before + 1e-9 and trade["status"] not in {"settled", "expired"}:
            trade.update(status="filled", unfilled=max(order - after, 0.0),
                         note=f"真实成交 {after:g}/{order:g} 份（撤单确认后读到），其余 {max(order - after, 0.0):g} 份{st.get('cancel_why') or '已撤'}")
            st["events"].append({"at": now_ms, "what": f"撤单确认后读到成交 {after - before:g} 份（累计 {after:g}/{order:g}）"})
        self.sim_save([(tid, trade)])
        # 1.138.0: the reservation released — a re-quote the caps refused on it (live_requote_freed ran before this read) is looked
        # at again now, not at the next trigger; only while the slot is still empty (a re-quote that went out needs nothing)
        slot = tid.rpartition("#")[0] if "#" in tid.rsplit("|", 1)[-1] else tid
        held = self.sim_trades().get(slot)
        looked = order_id in self.live_requote_looked  # the re-quote ran before this read (it reads the released room itself otherwise)
        self.live_requote_looked.discard(order_id)
        if looked and trade.get("key") and (trade.get("withdrawn") or {}).get("rule") and (held is None or held is trade or core.sim_spent(held)):
            with contextlib.suppress(Exception):
                self.predict.updated(str(trade["key"]))

    LIVE_LATE_FILLS_MS = 60_000               # live_late_fills runs at most this often
    LIVE_LATE_FILLS_WINDOW_MS = 2 * core.DAY_MS  # ...over the orders that ended within this long (1.119.2: two days, as /live positions looks back;
    #                                             two hours missed a 300-share SK Hynix fill whose orders had ended the evening before the deploy)
    LIVE_DUST_SHARES = 0.1                     # a holding, or an excess of one, under this many shares is dust: neither adopted nor flagged
    LIVE_ADOPT_READS = 8                       # an outcome's ended orders whose own records are read for the fills, newest first (1.120.3)
    LIVE_ADOPT_WAIT_MS = 10 * 60_000           # ...and how long after the newest ended those records get before the fill is guessed onto the newest
    LIVE_ADOPT_RECHECK_MS = 2 * 3_600_000      # ...and how long a guess is checked against the other orders' records
    LIVE_LATE_FILLS_GRACE_MS = 15_000          # ...and not before this long after their end: the positions list follows the matching a little late

    LIVE_TRIM_MAX = 0.05                       # a holding short of the records by more than this share is not a fee: left as it is (noted on the page)

    REPORT_GROUPS, REPORT_RECORDS = 8, 2  # /live fills: the outcomes with a gap shown, newest records each (1.131.1: a day of every record was dozens of messages)

    async def live_fills_report(self, now_ms: int) -> str:
        """/live fills: the account's positions against the records, the gaps first — what the account holds beyond the
        records (a fill nobody recorded), or short of its open records — at most REPORT_GROUPS outcomes and REPORT_RECORDS
        records each; the outcomes that agree are named in one line. A settled record explains the shares the account
        still holds of it (until redeemed); a resolved, redeemed position is no gap."""
        try:
            rows = await self.live.api.positions()
        except Exception as error:
            return f"持仓读取失败：{core.clean_error(error) or type(error).__name__}"
        complete = getattr(self.live.api, "positions_complete", True)
        key_of = lambda t: (str((t["live"].get("want") or {}).get("market_id") or t.get("market_id") or ""), int((t["live"].get("want") or {}).get("index_set") or 0))
        held: dict[tuple[str, int], tuple[float, float | None, str]] = {}
        for row in rows:
            market = row.get("market") if isinstance(row.get("market"), dict) else {}
            outcome = row.get("outcome") if isinstance(row.get("outcome"), dict) else {}
            k = (str(market.get("id") or row.get("marketId") or ""), int(outcome.get("indexSet") or 0))
            shares, price = self.position_shares(row), self.position_price(row)
            name = f"{market.get('title') or market.get('question') or market.get('slug') or k[0]} / {outcome.get('name') or outcome.get('title') or k[1]}"
            was = held.get(k, (0.0, None, name))
            held[k] = (was[0] + shares, price if price is not None else was[1], name)
        recs: dict[tuple[str, int], list[tuple[str, dict]]] = {}
        for tid, t in self.sim_trades().items():
            if isinstance(t.get("live"), dict) and t.get("status") in {"filled", "resting", "settled"} and float(t.get("shares") or 0) > 1e-9:
                recs.setdefault(key_of(t), []).append((tid, t))
        preopen = self.preopen_filled()
        busy = self.preopen_unverified()  # 1.131.5: a pre-open order's fills not read yet there — not compared
        gaps, same = [], []
        for k in set(held) | set(recs):
            if k in busy:
                continue
            shares, price, name = held.get(k, (0.0, None, ""))
            r = sorted(recs.get(k, []), key=lambda kv: core.trade_updated(kv[1]), reverse=True)
            name = name or (f"{r[0][1].get('item', k[0])} / {r[0][1].get('label', k[1])}" if r else f"{k[0]} / {k[1]}")
            recorded, po = sum(float(t.get("shares") or 0) for _, t in r), float(preopen.get(k, 0.0))
            gap = shares - recorded - po
            open_recs = any(t.get("status") in {"filled", "resting"} for _, t in r)
            if gap > self.LIVE_DUST_SHARES or (gap < -self.LIVE_DUST_SHARES and open_recs):
                gaps.append((abs(gap), name, shares, price, recorded, po, gap, r))
            elif shares > self.LIVE_DUST_SHARES or open_recs:
                same.append((name, shares))
        gaps.sort(key=lambda g: -g[0])
        lines = [f"持仓核对 {core.stamp(now_ms, seconds=False)}：账户 {len(held)} 个持仓" + ("" if complete else "（列表被截断，可能不全）")
                 + f"；对不上的 {len(gaps)} 个方向" + (f"（列出前 {self.REPORT_GROUPS} 个）" if len(gaps) > self.REPORT_GROUPS else "") + f"，一致的 {len(same)} 个"
                 + (f"；{len(busy & (set(held) | set(recs)))} 个方向有预挂订单的成交还没读完，稍后再比" if busy & (set(held) | set(recs)) else "")
                 + (f"；上次自动核对 {(now_ms - self.live_late_checked) // 1000} 秒前" if self.live_late_checked > 0 else "")]
        for _, name, shares, price, recorded, po, gap, r in gaps[:self.REPORT_GROUPS]:
            lines.append(f"{name}：账户 {shares:g} 份" + (f"（均价 {core.cents(price)}）" if price is not None else "") + f"，记录 {recorded:g} 份"
                         + (f"，预挂 {po:g} 份" if po else "") + f"，差 {gap:+g} 份")
            for tid, t in r[:self.REPORT_RECORDS]:
                st = t["live"]
                lines.append(f"  · {t.get('label', '')} {core.cents(float(t['price']))}×{float(t.get('order') or 0):g}：{t['status']}，成交 {float(t.get('shares') or 0):g} 份，"
                             f"订单 #{st.get('order_id') or '-'}，Predict 状态 {st.get('api_status') or '?'}")
        if same:
            lines.append("一致：" + "；".join(f"{name} {shares:g} 份" for name, shares in sorted(same, key=lambda x: -x[1])[:8]) + ("；…" if len(same) > 8 else ""))
        if not gaps and not same:
            lines.append("没有持仓，也没有在持的记录")
        return "\n".join(lines)

    async def live_late_fills(self, now_ms: int) -> None:
        """The account's positions against the records, once a minute (1.119.0): one read of the positions, then
        (a) fills that landed as an order was ending and were not read then (a 300-share KOSPI order withdrawn with 0
        filled as far as the cancel's read said, while the account held 299.99 shares of it): the records whose real
        order ended within LIVE_LATE_FILLS_WINDOW_MS with shares unfilled — shares the account holds on their outcome
        beyond what its records explain are read from the order's own record first, and what that still does not show
        is taken as filled on the newest such order, at most its unfilled count, costed as a fill of that order (a
        maker's price; a taker's limit, 待对账), noted on the record and announced; not within LIVE_LATE_FILLS_GRACE_MS
        of the end (the positions list follows the matching a little late), nor while an order still working on the
        outcome may hold that fill unread (live_late_unread, 1.150.0);
        (b) 1.119.1: an outcome the account holds a little less of than its open records say (Predict takes the taker
        fee in shares: 97.74 bought, 96.30 held) — the records are trimmed to the holding, in proportion, when no order
        with fills is still working there, the shortfall is within LIVE_TRIM_MAX and the same shortfall stands a minute
        later (the positions list follows the matching a little late); a position's payout is then exactly the shares
        held, and its cost the money spent, over the fewer shares: a maker's price × shares, kept (1.122.0); a taker's
        average price × the shares it bought — the fee its price carries was paid in the shares cut, not in USDT
        (1.150.0: left in the cost as well, it was counted twice); a taker whose shares were booked from the holding
        (live_from_holding), net of the fee already: its price × shares, kept like a maker's. A list that cannot be
        read, or came cut at the page cap, changes nothing."""
        if now_ms - self.live_late_checked < self.LIVE_LATE_FILLS_MS:
            return
        self.live_late_checked = now_ms
        trades = self.sim_trades()
        key_of = lambda t: (str((t["live"].get("want") or {}).get("market_id") or t.get("market_id") or ""), int((t["live"].get("want") or {}).get("index_set") or 0))
        live = [(tid, t) for tid, t in trades.items() if isinstance(t.get("live"), dict)]
        ended = [(tid, t) for tid, t in live  # 1.122.0: a settled record too — a fill read after the result still cost money (the day's loss stop counts it)
                 # (1.150.0: one that lapsed at the result with nothing filled is settled as the fill lands on it — live_apply: live_settle_late)
                 if t["live"].get("state") == "done" and t["live"].get("order_id") and t.get("status") in {"cancelled", "filled", "settled", "expired"}
                 and float(t.get("order") or 0) - float(t.get("shares") or 0) > 1e-9
                 and self.LIVE_LATE_FILLS_GRACE_MS <= now_ms - core.trade_updated(t) <= self.LIVE_LATE_FILLS_WINDOW_MS]
        open_recs = [(tid, t) for tid, t in live if t.get("status") in {"filled", "resting"} and float(t.get("shares") or 0) > 1e-9]
        if not ended and not open_recs:
            return
        try:
            rows = await self.live.api.positions()
        except Exception:
            return
        self.live_positions = (rows, time.time())
        if not getattr(self.live.api, "positions_complete", True):
            return  # cut at the page cap: a position missing from it says nothing
        held: dict[tuple[str, int], float] = {}
        priced: dict[tuple[str, int], tuple[float, float]] = {}  # outcome -> (Σ price × shares, Σ shares) of the rows stating a price
        pos_time: dict[tuple[str, int], int] = {}
        for row in rows:
            market = row.get("market") if isinstance(row.get("market"), dict) else {}
            outcome = row.get("outcome") if isinstance(row.get("outcome"), dict) else {}
            k = (str(market.get("id") or row.get("marketId") or ""), int(outcome.get("indexSet") or 0))
            shares = self.position_shares(row)
            held[k] = held.get(k, 0.0) + shares
            price = self.position_price(row)
            if price is not None and shares > 0:
                priced[k] = (priced.get(k, (0.0, 0.0))[0] + price * shares, priced.get(k, (0.0, 0.0))[1] + shares)
            pos_time[k] = max(pos_time.get(k, 0), self.position_time(row))
        pos_price = {k: round(v / n, 6) for k, (v, n) in priced.items() if n > 0}
        recorded: dict[tuple[str, int], float] = {}
        for tid, t in live:
            k = key_of(t)
            recorded[k] = recorded.get(k, 0.0) + float(t.get("shares") or 0)
        po = self.preopen_filled()
        for k, shares in po.items():  # the pre-open mode's fills: held for a reason, not fills of these orders (1.125.0)
            recorded[k] = recorded.get(k, 0.0) + shares
        busy = self.preopen_unverified()  # 1.131.5: a pre-open order there whose fills are not read yet — the excess may be its own
        # (a) the shares held beyond the records, onto the order they came from: the orders' own records first — whichever shows fills
        # beyond its record (1.120.3: a KOSPI fill at 21¢ and an SK Hynix one at 10¢ had each been put on a 1¢ sweep order placed
        # after them, the newest to end) — else the order the position's price or time names, else, once the records have had
        # LIVE_ADOPT_WAIT_MS to catch up, the newest as a guess, checked against the other orders' records for LIVE_ADOPT_RECHECK_MS
        ended.sort(key=lambda kv: core.trade_updated(kv[1]), reverse=True)
        by_out: dict[tuple[str, int], list[tuple[str, dict]]] = {}
        for tid, t in ended:
            k = key_of(t)
            if k[0] and k[1]:
                by_out.setdefault(k, []).append((tid, t))
        seen: set[tuple[str, int]] = set(busy)  # (the busy outcomes: no adoption, no cost fix, no trim until the pre-open mode has read them)
        for k, cands in by_out.items():
            if k in busy:
                continue
            cands = [c for c in cands if not c[1]["live"].get("adopt_undone")]  # a record once found to hold another's fill takes none again
            if not cands:
                continue
            excess = held.get(k, 0.0) - recorded.get(k, 0.0)
            if excess <= self.LIVE_DUST_SHARES:
                continue
            seen.add(k)  # the outcome is not trimmed in the same pass
            left = excess
            for tid, t in cands[:self.LIVE_ADOPT_READS]:
                if left <= self.LIVE_DUST_SHARES:
                    break
                got = await self.live_reread_fills(tid, t, now_ms)
                if got > 1e-9:
                    left -= got
                    recorded[k] = recorded.get(k, 0.0) + got
                    self.live_adopted_status(tid, t, now_ms, f"结束后再读订单记录，读到成交 {got:g} 份（累计 {float(t['shares']):g}/{float(t.get('order') or 0):g}）")
            if left <= self.LIVE_DUST_SHARES:
                continue
            # 1.150.0: the excess may be a WORKING order's own fill that its record here does not show yet — a resting maker the
            # step's bounded read left for later (read by its hash every LIVE_TRACK_READ_MS), a cancel confirmed whose fills are
            # read beside the step (fills_pending), a placement in flight. Before, it went at once onto the ended order whose price
            # or time matched (a maker re-quoted at the same price is the normal re-quote; an ended taker matches any average at or
            # under its limit) — not a guess, so never checked again — and when the working order's own read landed, the same
            # shares sat on two records for good (50 held, 100 recorded): exposure, the journal, the P&L and the daily loss stop
            # counting them twice. The orders resting there are read now (live_late_unread); while a record there may still hold
            # fills not read, the outcome waits for the next minute's look (not trimmed either: it is in ``seen``). An order read
            # now does not hold it — a re-quote resting on the side would otherwise keep a genuine late fill unbooked for hours.
            if await self.live_late_unread(k, key_of, now_ms):
                continue
            # ...and the records as they stand now: a working order's read may have landed during the awaited reads above (the
            # express lane and the cancels' fills reads run beside the step), after ``recorded`` was summed
            left = min(left, held.get(k, 0.0) - po.get(k, 0.0) - sum(float(o.get("shares") or 0) for o in self.sim_trades().values()
                                                                 if isinstance(o.get("live"), dict) and key_of(o) == k))
            if left <= self.LIVE_DUST_SHARES:
                continue
            price, when = pos_price.get(k), pos_time.get(k, 0)
            room = [c for c in cands if float(c[1].get("order") or 0) - float(c[1].get("shares") or 0) > 1e-9]
            tid, t, by = self.live_pick_adoptee(room or cands, price, when, self.LIVE_LATE_FILLS_GRACE_MS)
            guess = by.startswith("按最近结束")
            if guess and now_ms - max(core.trade_updated(c[1]) for c in cands) < self.LIVE_ADOPT_WAIT_MS:
                continue  # neither the records nor the position say which order filled: the records get a while to catch up
            st = t["live"]
            before, order = float(t.get("shares") or 0), float(t.get("order") or 0)
            adopt = min(left, order - before)
            if adopt <= self.LIVE_DUST_SHARES:
                continue
            self.live_apply(tid, t, {"status": str(st.get("final") or "closed").upper(), "amount": str(to_wei(order)),
                                     "amountFilled": str(to_wei(before + adopt))}, None, now_ms)
            after = float(t.get("shares") or 0)
            how = f"账户在这一边的持仓比记录多 {excess:g} 份、订单记录没显示：按成交 {adopt:g} 份补记（累计 {after:g}/{order:g}，{by}）"
            if when and t.get("fills"):
                t["fills"][-1]["at"] = when  # the position's own time: the fill's, not this pass's
                t["filled"] = when
            st["adopted"] = {"at": now_ms, "by": by, "pos_price": price, "shares": adopt, "guess": guess}
            if price is not None and abs(price - float(t["price"])) > 0.0051:
                own, label = price, "Predict 持仓均价"
                others = [(o, float(o.get("shares") or 0)) for _, o in open_recs if o is not t and key_of(o) == k and float(o.get("shares") or 0) > 1e-9]
                if others:
                    # 1.122.0: the position's average covers every record on the outcome — this fill's own price is what the average
                    # leaves once the other records' known costs are taken out (orders at 20¢ and 60¢ average 40¢: the second is not 40)
                    rest_shares = held.get(k, 0.0) - sum(sh for _, sh in others)
                    rest_cost = price * held.get(k, 0.0) - sum(float(o.get("avg") or o["price"]) * sh for o, sh in others)
                    if rest_shares > 1e-9 and 0 < rest_cost / rest_shares <= 1:
                        own, label = round(rest_cost / rest_shares, 6), f"Predict 持仓均价 {core.cents(price)} 扣掉其他记录的成本后"
                cost = own if t.get("maker") else own + core.taker_fee(own, int((st.get("want") or {}).get("fee_bps") or 0))
                how += f"；成本按{label} {core.cents(own)} 记（订单 #{st['order_id']} 的价 {core.cents(float(t['price']))} 不是这笔成交的）"
                t["price"] = cost
            if guess:
                how += "；之后两小时内继续核对这一边其他订单的记录，哪张显示了成交就改到哪张"
            recorded[k] = recorded.get(k, 0.0) + after - before
            self.live_adopted_status(tid, t, now_ms, how)
        # a guessed adoption against the other orders' records (1.120.3): the one whose record shows the fills takes the shares back
        for tid, t in open_recs:
            st, k = t["live"], key_of(t)
            ad = st.get("adopted") or {}
            if not ad.get("guess") or ad.get("moved") or now_ms - int(ad.get("at") or 0) > self.LIVE_ADOPT_RECHECK_MS or k in seen:
                continue
            for tid2, t2 in [c for c in by_out.get(k, []) if c[0] != tid][:self.LIVE_ADOPT_READS]:
                got = await self.live_reread_fills(tid2, t2, now_ms)
                if got <= 1e-9:
                    continue
                oid2 = str(t2["live"].get("order_id") or "")
                moved = round(min(got, float(ad.get("shares") or 0) or float(t.get("shares") or 0), float(t.get("shares") or 0)), 6)
                order = float(t.get("order") or 0)
                t["shares"] = round(float(t["shares"]) - moved, 6)
                if t.get("fills"):  # the adopted fill, taken back
                    t["fills"][-1]["shares"] = round(float(t["fills"][-1].get("shares") or 0) - moved, 6)
                    if t["fills"][-1]["shares"] <= 1e-9:
                        t["fills"].pop()
                ad["moved"] = {"to": tid2, "order_id": oid2, "shares": moved, "at": now_ms}
                if t["shares"] <= 1e-9:
                    t.update(status="cancelled", shares=0.0, unfilled=order, filled=None,
                             note=f"撤单：{st.get('cancel_why') or '已结束'}，一份都没成交（补记的 {moved:g} 份其实是订单 #{oid2} 的，已改到那笔）")
                else:
                    t.update(unfilled=max(order - float(t["shares"]), 0.0),
                             note=f"真实成交 {float(t['shares']):g}/{order:g} 份（订单结束后补记），其余 {max(order - float(t['shares']), 0.0):g} 份{st.get('cancel_why') or '已结束'}")
                st["events"].append({"at": now_ms, "what": f"补记改正：按最近结束的订单补记的 {moved:g} 份其实是订单 #{oid2} 的（它的记录显示了成交），已改到那笔"})
                self.sim_save([(tid, t)])
                self.live_adopted_status(tid2, t2, now_ms, f"结束后再读订单记录，读到成交 {got:g} 份（累计 {float(t2['shares']):g}/{float(t2.get('order') or 0):g}）；"
                                                            f"之前按最近结束的订单补记在 #{st.get('order_id')} 上的 {moved:g} 份已改到这里")
                break
        await self.live_undo_preopen_adoptions(live, key_of, busy, po, now_ms)
        # an earlier backfill (1.119.0–1.119.2 took the newest ended order, whatever its price) costed at that order's price: the
        # position's average price, once the row states one and this is the only record holding the outcome, is the cost
        for tid, t in open_recs:
            st, k = t["live"], key_of(t)
            backfilled = st.get("adopted") or any("补记" in str(e.get("what") or "") for e in st.get("events") or ())
            price = pos_price.get(k)
            if not backfilled or st.get("adopted_price_fixed") or price is None or k in seen or po.get(k):
                continue  # (1.131.5: the pre-open mode holds the outcome too — the average is not this record's alone)
            if any(o is not t and key_of(o) == k and float(o.get("shares") or 0) > 1e-9 for _, o in open_recs):
                continue  # several records hold the outcome: the average price is not this one's alone
            st["adopted_price_fixed"] = True
            if abs(price - float(t["price"])) > 0.0051:
                was = float(t["price"])
                t["price"] = price if t.get("maker") else price + core.taker_fee(price, int((st.get("want") or {}).get("fee_bps") or 0))
                st["events"].append({"at": now_ms, "what": f"补记的成本按 Predict 持仓均价改为 {core.cents(price)}（原按订单 #{st.get('order_id')} 的价 {core.cents(was)} 记）"})
                self.live_notify(f"📌 成本校正：{t['item']} {t['label']}：补记那笔的成本按 Predict 持仓均价改为 {core.cents(price)}（原 {core.cents(was)}）", key=f"adoptprice:{tid}")
            self.sim_save([(tid, t)])
        # (b) a holding a little short of the open records: the fee taken in shares — the records trimmed to it, in proportion
        groups: dict[tuple[str, int], list[tuple[str, dict]]] = {}
        for tid, t in open_recs:
            groups.setdefault(key_of(t), []).append((tid, t))
        for k, recs in groups.items():
            if not k[0] or not k[1] or k in seen or any(t["live"].get("state") != "done" and float(t.get("shares") or 0) > 1e-9 for _, t in recs):
                continue  # a working order with fills there: its fills and the holding are not settled against each other yet
            pk = po.get(k, 0.0)  # 1.131.5: the pre-open mode's fills there are post-only makers, no fee taken: the shortfall is the records' alone
            total, got = recorded.get(k, 0.0) - pk, (held[k] - pk if k in held else None)
            short = total - got if got is not None and total > 1e-9 else 0.0
            if short <= 0.01 or short > self.LIVE_TRIM_MAX * total:
                self.live_trim_seen.pop(k, None)
                continue
            earlier = self.live_trim_seen.get(k)
            if earlier is None or abs(earlier[0] - short) > 0.01 or now_ms - earlier[1] < self.LIVE_LATE_FILLS_MS:
                self.live_trim_seen[k] = (short, now_ms)  # the same shortfall has to stand a minute later
                continue
            self.live_trim_seen.pop(k, None)
            scale = got / total
            fee_off = 0.0  # what the takers' costs go down by: the fee they carried in USDT beside the shares cut (the notice says it)
            for tid, t in recs:
                before = float(t["shares"])
                after = round(before * scale, 6)
                cut = float(t.get("fee_shares") or 0)  # the shares earlier trims took off this record
                t["shares"] = after
                t["fee_shares"] = round(cut + before - after, 6)
                what = f"按 Predict 持仓校正份数：{before:g} → {after:g}（账户在这一边共持有 {got:g} 份，比记录的 {total:g} 份少 {short:g} 份：手续费以份额扣除）"
                if after > 0:
                    price = float(t["price"])
                    keep = price * before / after  # 1.122.0: the money spent is the same, over fewer shares
                    if not t.get("maker") and not self.live_from_holding(t):
                        # 1.150.0: ...but a taker's price × shares is not the money it spent. live_apply costs it at the average
                        # plus the fee per share, and Predict takes that fee out of the shares bought — the very shares cut here —
                        # not in USDT: a fill costs average × shares, never more than the order signed for (100 shares at 58¢:
                        # $58.00 left the account, which holds 98.55 of them). Kept whole over the fewer shares, the fee was counted
                        # twice — the shares gone AND their worth still in the cost ($58.84): every trimmed taker's P&L, a win's
                        # and a loss's alike, came out low by the fee (−$0.84 per 100 shares at 58¢, up to 2% of the money in: the
                        # journal, the EV comparison, the exposure and the day's loss figure). The money a taker spent is its
                        # average price × the shares it bought (those held now and every share cut off it): the cost goes there —
                        # never above the cost the record had, and never by lowering its per-share price (the 1.119.1 rule): a
                        # record cut by less than its own fee (the shortfall is spread in proportion over the outcome's records)
                        # keeps the rest of the fee in its cost, dear rather than cheap. (The premise — no USDT fee beside the
                        # shares — is what the signed order allows and what the shortfall 1.119.1 saw amounts to: 97.74 bought,
                        # 96.30 held is the whole 2% fee at about 58¢. It has not been confirmed with Predict.)
                        # Only a taker whose shares were read from its order's own record, though: the shares bought, the fee
                        # still among them. One whose shares were booked from the account's holding (live_from_holding: a late
                        # fill adopted from the positions, an order of unknown state settled by them) holds them net of the fee
                        # already, at a cost that is the money spent as it stands (within a cent), its "average" its limit and
                        # the shares it bought unknown: it carries no fee twice, and what is cut off it is not its fee (another
                        # record's, spread onto it; shares that left the account since) — its cost is kept whole, like a
                        # maker's, as before 1.150.0. (Under the taker's rule only the per-share floor held it, and its cost
                        # fell by price × the shares cut: 98.55 shares adopted at 58¢ beside a later taker of 100 at 30¢ —
                        # $88.00 spent — were booked $87.70 together after the trim: under the money spent, the one way this
                        # must not err.)
                        keep = min(keep, max(price, float(t.get("avg") or price) * (before + cut) / after))
                        if price * before - keep * after >= 0.005:
                            fee_off += price * before - keep * after
                            what += f"；成本 ${price * before:,.2f} → ${keep * after:,.2f}（吃单的手续费就是少掉的这些份额，不再另算进成本）"
                    t["price"] = round(keep, 6)
                t["live"]["events"].append({"at": now_ms, "what": what})
                self.sim_save([(tid, t)])
            costs = "（成本不变）" if fee_off < 0.005 else (f"（吃单的手续费就是少掉的这些份额，不再另算进成本：成本共减 ${fee_off:,.2f}"
                                                           + ("；挂单的成本不变" if any(t.get("maker") for _, t in recs) else "")
                                                           + ("；按账户持仓记入的吃单本来就是扣过手续费的份数，成本不变"
                                                              if any(not t.get("maker") and self.live_from_holding(t) for _, t in recs) else "") + "）")
            self.live_notify(f"📐 份数校正：{recs[0][1]['item']} {recs[0][1]['label']}：账户持有 {got:g} 份，记录 {total:g} 份，少的 {short:g} 份是手续费以份额扣除；"
                             f"{len(recs)} 笔记录按比例改为持有的份数{costs}", key=f"trim:{k[0]}:{k[1]}")

    @staticmethod
    def live_from_holding(t: dict) -> bool:
        """Whether shares of this record were booked from the account's holding rather than read from its order's own
        record (1.150.0): a late fill adopted from the positions (live_late_fills (a): ``adopted``), or an order of
        unknown state settled by them (live_recheck, which leaves no mark but its event). Both events say the holding
        exceeded the records — 持仓比记录多, the words matched here: they stay in those two events. Shares booked so are
        what the account holds, net of the fee Predict takes in shares; read from the order's record they are the
        shares bought, the fee still among them (live_late_fills (b))."""
        st = t["live"]
        return bool(st.get("adopted")) or any("持仓比记录多" in str(e.get("what") or "") for e in st.get("events") or ())

    async def live_undo_preopen_adoptions(self, live: list[tuple[str, dict]], key_of: Any, busy: set, po: dict, now_ms: int) -> None:
        """A backfill that was the pre-open mode's fill, undone (1.131.5): the order's own record, read again once the
        adoption is LIVE_ADOPT_WAIT_MS old, shows nothing filled, every share of the record came from the backfill, and the
        pre-open fills read on the outcome cover it — before 1.131.5 a pre-open fill the ledger had not read yet (or any,
        while the mode was off, before 1.131.2) could be booked onto the model trader's ended order on the same side. Each
        backfill is looked at once (LIVE_ADOPT_READS a pass); the record is never adopted onto again."""
        reads = 0
        for tid, t in live:
            st = t["live"]
            ad = st.get("adopted") or {}
            age = now_ms - int(ad.get("at") or 0)
            if (not ad or ad.get("moved") or ad.get("undone") or ad.get("po_checked") or not st.get("order_id")
                    or not self.LIVE_ADOPT_WAIT_MS <= age <= self.LIVE_LATE_FILLS_WINDOW_MS):
                continue
            k, shares, adopted = key_of(t), float(t.get("shares") or 0), float(ad.get("shares") or 0)
            if k in busy or shares - adopted > self.LIVE_DUST_SHARES or po.get(k, 0.0) < shares - self.LIVE_DUST_SHARES or shares <= self.LIVE_DUST_SHARES:
                continue  # fills of its own beyond the backfill, or no pre-open fill there that could be it
            if reads >= self.LIVE_ADOPT_READS:
                break
            reads += 1
            row = None
            with contextlib.suppress(Exception):
                row = await self.live.api.order(str(st["order_id"]), str(st.get("hash") or ""))
            if not isinstance(row, dict):
                continue  # unread: asked again next pass
            ad["po_checked"] = now_ms
            own, _ = order_fill(row, float(t.get("order") or 0))
            if own > self.LIVE_DUST_SHARES:
                self.sim_save([(tid, t)])
                continue  # the order's own record shows fills: the backfill stands
            order = float(t.get("order") or 0)
            ad["undone"] = {"at": now_ms, "shares": shares}
            st["adopt_undone"] = True
            ended = "expired" if t.get("status") in {"settled", "expired"} else "cancelled"
            t.update(shares=0.0, fills=[], filled=None, unfilled=order, status=ended,
                     note=(f"撤单：{st.get('cancel_why') or '已结束'}，一份都没成交（之前补记的 {shares:g} 份是开盘前预挂在同一边的成交，已撤销）" if ended == "cancelled"
                           else f"市场已出结果，挂单一直没成交（之前补记的 {shares:g} 份是开盘前预挂在同一边的成交，已撤销）"))
            st["events"].append({"at": now_ms, "what": f"补记撤销：订单 #{st['order_id']} 自己的记录一份都没成交，之前补记的 {shares:g} 份是开盘前预挂在同一边的成交（预挂已读到），已撤销"})
            self.sim_save([(tid, t)])
            self.live_notify(f"↩️ 补记撤销：{t['item']} {t['label']}：订单 #{st['order_id']} 在 Predict 的记录一份都没成交，之前补记的 {shares:g} 份"
                             f"其实是开盘前预挂在同一边的成交，已从这笔记录撤销（预挂的记录里照常有它）", key=f"unadopt:{tid}")

    async def live_reread_fills(self, tid: str, t: dict, now_ms: int) -> float:
        """An ended order's own record read again, onto the trade (live_apply): the shares it shows filled beyond the
        record; 0 when none, or the record cannot be read."""
        st = t["live"]
        before = float(t.get("shares") or 0)
        row = None
        with contextlib.suppress(Exception):
            row = await self.live.api.order(str(st["order_id"]), str(st.get("hash") or ""))
        if not isinstance(row, dict):
            return 0.0
        self.live_apply(tid, t, row, None, now_ms)
        st.pop("fills_pending", None)
        return max(0.0, float(t.get("shares") or 0) - before)

    async def live_late_unread(self, k: tuple[str, int], key_of: Any, now_ms: int) -> str:
        """Why the account's excess on outcome ``k`` may still be a working order's own fill that its record here does not
        show — "" when none (live_late_fills (a), 1.150.0). The orders resting there are read first, by their hash, as
        live_track reads them: a record Predict still shows open gets its fills. What still holds the outcome is a record
        whose fills cannot be known now: a cancel confirmed whose fills are read beside the step (fills_pending), a
        placement or a cancel in flight, an order of unknown state, a resting order whose read failed, found it ended
        (its end is the sync's to book) or that was withdrawn meanwhile. A record never sent, or ended and read, holds
        nothing; nor does a resting order read here."""
        read: set[str] = set()
        for tid, t in list(self.sim_trades().items()):
            st = t.get("live")
            if not isinstance(st, dict) or key_of(t) != k or st.get("state") != "open" or not st.get("hash"):
                continue  # (by its hash alone: a read by id searches the account's whole open list)
            row = None
            with contextlib.suppress(Exception):
                row = await self.live.api.order(str(st.get("order_id") or ""), str(st["hash"]))
            if st.get("state") != "open" or not isinstance(row, dict) or order_final(row, float(t.get("order") or 0)):
                continue  # withdrawn meanwhile (its cancel reads it), unreadable, or ended: not read here — it holds the outcome
            before = float(t.get("shares") or 0)
            self.live_apply(tid, t, row, None, now_ms)
            read.add(str(st.get("order_id") or st["hash"]))
            if float(t.get("shares") or 0) > before + 1e-9:
                self.sim_save([(st.get("rekeyed") or tid, t)])
        for t in list(self.sim_trades().values()):
            st = t.get("live")
            if not isinstance(st, dict) or key_of(t) != k:
                continue
            state, name = st.get("state"), str(st.get("order_id") or st.get("hash") or "")
            if core.fills_pending(st):
                return f"订单 #{name} 撤单已确认，撤单前的成交还没读回"
            if state in {"placing", "cancelling", "unknown"} and name:  # (never sent: no id, no hash — nothing to fill)
                return f"订单 #{name} {'发送中' if state == 'placing' else '撤单中' if state == 'cancelling' else '状态未知'}，成交未定"
            if state == "open" and name not in read:
                return f"挂着的订单 #{name} 这次没读到"
        return ""

    def live_adopted_status(self, tid: str, t: dict, now_ms: int, how: str) -> None:
        """A record that gained fills after its order ended: a position now (unless settled), the event, the notice, saved."""
        st = t["live"]
        after, order = float(t.get("shares") or 0), float(t.get("order") or 0)
        if t["status"] not in {"settled", "expired"} and after > 1e-9:
            t.update(status="filled", unfilled=max(order - after, 0.0),
                     note=f"真实成交 {after:g}/{order:g} 份（订单结束后补记），其余 {max(order - after, 0.0):g} 份{st.get('cancel_why') or '已结束'}")
        st["events"].append({"at": now_ms, "what": "结束后核对持仓：" + how})
        self.live_notify(f"📌 补记成交：{t['item']} {t['label']} {core.cents(float(t['price']))}：{how}（订单 #{st.get('order_id')}，请在 Predict 核对）",
                         key=f"latefill:{st.get('order_id')}")
        self.sim_save([(tid, t)])

    async def live_fills_wait(self) -> None:
        """Every fills read in flight (live_fills_later), waited for: the step's sync and the tests."""
        if self.live_fill_tasks:
            await asyncio.gather(*list(self.live_fill_tasks), return_exceptions=True)

    def live_note_unknown(self, open_rows: list[dict], now_ms: int) -> None:
        """Open orders on the account that no record here placed (a manual order, a lost record, another bot): kept
        for /live and the page, announced once each. They are not this strategy's to manage; /live cancel account
        withdraws them with everything else."""
        if getattr(self, "preopen", None) is not None:
            open_rows = self.preopen.unmanaged_rows(open_rows)
        trades = self.sim_trades()
        known_ids = {str(t["live"].get("order_id")) for t in trades.values() if isinstance(t.get("live"), dict) and t["live"].get("order_id")}
        known_hashes = {str(t["live"].get("hash") or "").lower() for t in trades.values() if isinstance(t.get("live"), dict)}
        unknown = [r for r in open_rows if str(r.get("id")) not in known_ids and (order_hash_of(r) or "-") not in known_hashes]
        seen = {u["id"] for u in self.live_unknown_open}
        self.live_unknown_open = [{"id": str(r.get("id")), "hash": order_hash_of(r), "status": str(r.get("status") or ""),
                                   "amount": str(r.get("amount") or "")} for r in unknown]
        self.live_unknown_first = {u["id"]: self.live_unknown_first.get(u["id"], now_ms) for u in self.live_unknown_open}  # live_orphan_sweep (1.136.2)
        self.live_unrecorded_rows = {str(r.get("id")): r for r in unknown}
        fresh = [u["id"] for u in self.live_unknown_open if u["id"] not in seen]
        if fresh:
            self.live_notify(f"⚠️ Predict 账户上有 {len(self.live_unknown_open)} 张本机器人没有记录的挂单（#{'、#'.join(fresh[:5])}）：可能是手动单、"
                             f"挂单测试残留或丢失跟踪的单。它们不计入这里的风控额度；"
                             + (f"挂在本机器人市场上的 {self.LIVE_ORPHAN_GRACE_MS // 60_000} 分钟后自动撤掉（LIVE_ORPHAN_CANCEL），其余 /live cancel account 可连同策略挂单一起撤掉。"
                                if getattr(self.config, "live_orphan_cancel", True) else "/live cancel account 可连同策略挂单一起撤掉。"), key="unknown:" + ",".join(fresh[:5]))

    async def live_reconcile(self, tid: str, trade: dict, by_hash: dict, now_ms: int) -> None:
        """A record whose request may or may not have reached Predict (the process died in between, the answer was
        lost): adopt the order when Predict knows it — in the open list, or by its hash (a taker that filled at once
        is never in the open list) — give up (never re-send) only after the grace period."""
        st = trade["live"]
        row = by_hash.get(st.get("hash", ""))
        if row is None and st.get("hash"):
            row = await self.live.api.order("", st["hash"])
        if row is not None:
            # withdrawn meanwhile: found, it is cancelled next, never tracked as open
            st.update(state="cancelling" if trade.get("withdrawn") else "open", order_id=str(row.get("id") or ""),
                      placed_at=st.get("placed_at") or now_ms, cancel_sent=0)
            st["events"].append({"at": now_ms, "what": f"按哈希找回订单 #{st['order_id']}" + ("，已被撤单：随即撤单" if trade.get("withdrawn") else "")})
            self.live_apply(tid, trade, row, None, now_ms)
            return
        if now_ms - int(st.get("placing_at") or 0) > self.LIVE_PLACING_GRACE_MS:
            self.live_unknown(tid, trade, "下单后无法确认 Predict 是否收到（重启或超时）", now_ms)  # never re-sent; reserved until it cannot fill

    async def live_track(self, tid: str, trade: dict, by_id: dict, mk: Any, now_ms: int) -> None:
        st = trade["live"]
        row = by_id.get(st["order_id"])
        if row is not None:
            self.live_misses.pop(tid, None)
            self.live_apply(tid, trade, row, mk, now_ms)
            await self.live_taker_due(tid, trade, by_id, mk, now_ms)
            return
        final = await self.live.api.order(st["order_id"], st.get("hash", ""))
        if final is not None:
            self.live_misses.pop(tid, None)
            if order_final(final, float(trade.get("order") or 0)):
                self.live_finish(tid, trade, final, mk, now_ms)
                return
            self.live_apply(tid, trade, final, mk, now_ms)  # Predict's own record says it is still open (the list missed it): its fills, and on with the tracking
            if not st.get("list_missed") and getattr(self.live.api, "orders_complete", True):  # a bounded read says nothing (1.137.1)
                st["list_missed"] = now_ms
                st["events"].append({"at": now_ms, "what": f"开放订单列表没有它，但按哈希查到仍为 {str(final.get('status') or '?')}：继续跟踪"})
            await self.live_taker_due(tid, trade, by_id, mk, now_ms)
            return
        if not self.live_counts_miss(st, max(int(st.get("placed_at") or 0), int(st.get("answered_at") or 0))):
            return  # no whole list without it since the last look: its silence says nothing; the hash read above is all there is to go on
        self.live_misses[tid] = self.live_misses.get(tid, 0) + 1
        if self.live_misses[tid] < self.LIVE_MISSES or now_ms - int(st.get("placed_at") or now_ms) < self.LIVE_MISS_WINDOW_MS:
            return  # a fresh order may take Predict a moment to show on its reads: the looks only count after a minute
        # Predict has answered "no such order" this many looks in a row, a minute or more after the placement: a removal is
        # still asked for (if it exists after all, it must not fill — and the answer says whether it did), then the record ends
        oid = str(st["order_id"])
        if oid in self.live_cancels:
            return  # 1.150.0: another path is removing this very order right now (live_cancel: withdrawn meanwhile): its answer ends the record
        self.live_cancels.add(oid)
        try:
            result = await self.live.api.remove_orders([st["order_id"]])
        except Exception as error:
            st["last_error"] = core.clean_error(error) or type(error).__name__
            return  # the removal could not be asked for: again next look, the order stays reserved meanwhile
        finally:
            self.live_cancels.discard(oid)
        if st["order_id"] in (result.get("removed") or []):
            st["events"].append({"at": now_ms, "what": f"连续 {self.LIVE_MISSES} 次查不到这张单，补发的撤单请求撤掉了它（它其实还在）"})
            self.live_finish(tid, trade, {"status": "CANCELLED"}, mk, now_ms)
            return
        self.live_unknown(tid, trade, f"连续 {self.LIVE_MISSES} 次查不到这张单，补发的撤单请求也未确认", now_ms)

    async def live_taker_due(self, tid: str, trade: dict, by_id: dict, mk: Any, now_ms: int) -> None:
        """A taker's LIMIT order still open: the rest is cancelled after LIVE_TAKER_WAIT_SECONDS, and sooner when the
        fair price has moved so that its limit no longer leaves the bar (the cap recomputed every step: a bid left at
        the old price is a free option for whoever knows why the price moved) or the card suggests nothing now."""
        st = trade["live"]
        if trade.get("maker"):
            return
        why = ""
        if now_ms - int(st.get("placed_at") or now_ms) >= self.config.live_taker_wait * 1000:
            why = f"吃单 {self.config.live_taker_wait} 秒未全部成交"
        elif mk is not None and float(trade.get("shares") or 0) < float(trade.get("order") or 0) - 1e-9:
            why = self.live_taker_stale(trade, mk, now_ms)
        if why:
            st.update(state="cancelling", cancel_why=why)
            st["events"].append({"at": now_ms, "what": why})
            await self.live_cancel(tid, trade, by_id, mk, now_ms)

    def live_taker_stale(self, trade: dict, mk: Any, now_ms: int) -> str:
        """Why a taker's unfilled rest should not go on waiting: its limit is over the cap the fair price allows now
        with LIVE_TAKER_REST_SLACK off the bar (the rest is not re-placed, so it keeps a little room: a wobble of a few
        tenths of a cent is not a move), or the card holds its suggestion. "" while it may rest (a stale book decides
        nothing: the timer does)."""
        if mk.book.stale(now_ms):
            return ""
        if mk.hold:
            return f"卡片暂不建议（{mk.hold}），剩余吃单撤掉"
        bps = mk.book.fee_bps if mk.book.fee_bps is not None else self.config.predict_fee_bps
        cap = self.sim_taker_cap(mk, str(trade.get("side") or "up"), bps, slack=self.LIVE_TAKER_REST_SLACK)
        limit = self.live_unit_cap(trade)
        if cap <= 0 or limit > cap + 1e-9:
            return (f"公平价变了：限价 {core.cents(limit)} 已高于现在的封顶价 {core.cents(cap)}"
                    f"（按门槛低 {core.cents(self.LIVE_TAKER_REST_SLACK)} 的撤单线算），剩余吃单撤掉")
        return ""

    async def live_cancel(self, tid: str, trade: dict, by_id: dict, mk: Any, now_ms: int) -> None:
        """A withdrawn record's real order is removed from Predict and the record ends only on Predict's final word
        (or its confirmed removal); until then every unfilled share stays reserved (live_reserved)."""
        st = trade["live"]
        if not st.get("order_id"):
            if not st.get("hash"):
                return self.live_finish(tid, trade, {"status": "CANCELLED"}, mk, now_ms)  # never built, never sent
            # the request may have reached Predict although no order id came back: ask by the hash first
            row = await self.live.api.order("", st["hash"])
            if row is not None and row.get("id") is not None:
                st["order_id"] = str(row.get("id"))
                st["events"].append({"at": now_ms, "what": f"撤单前按哈希找到订单 #{st['order_id']}"})
            elif now_ms - int(st.get("placing_at") or st.get("placed_at") or now_ms) <= self.LIVE_PLACING_GRACE_MS:
                return  # not known to Predict (yet): asked again next step until the grace period passes
            else:
                return self.live_unknown(tid, trade, "撤单时按哈希也查不到这张单，下单是否送达未知", now_ms)  # not "cancelled": unknown
        listed = by_id.get(st["order_id"])
        if listed is not None:
            # the open list's record of it: its fills count, whatever the removal answers — a removal confirmed at once
            # used to skip it for the single-order read, which may lag the list (or not know a removed order), and the
            # fills the list showed were lost with their reservation (1.83.0)
            self.live_apply(tid, trade, listed, mk, now_ms)
        # 1.150.0: one path at a time removes an order and reads Predict's final word on it. The step's sync, every express pass that
        # changed anything (live_cancels_unsent is global: any key's lane sends every unsent cancel), /live cancel and the emergency
        # stop beside the step all reach the same withdrawn record, outside sim_lock, and cancel_sent is only set once Predict
        # answers — so two paths sent two removals. The second answer ("noop": the first had taken the order off) overwrote the
        # first's confirmation, and that path's single-order read, which may not know a just-removed order, put the finished record
        # back to "unknown": its $ reserved again, the side blocked for minutes, a false ❓ alert. A taker's attempt was failed twice
        # and stored at slot#1 and slot#2; a second final read answering after a fresh taker had taken the slot deleted that live
        # order's record (live_fail). The other path leaves the order to this one: what the list showed is applied above, the
        # answers here end the record — or leave it "cancelling", its cancel unsent, for the next pass when the request fails
        oid = str(st["order_id"])
        if oid in self.live_cancels:
            return
        self.live_cancels.add(oid)
        try:
            await self.live_cancel_order(tid, trade, listed, mk, now_ms)
        finally:
            self.live_cancels.discard(oid)

    async def live_cancel_order(self, tid: str, trade: dict, listed: dict | None, mk: Any, now_ms: int) -> None:
        """live_cancel's removal request and Predict's final word on the order, the order claimed by this path (live_cancels).
        A record another path ended while an answer was on its way (the fills of a list completing it, an order's own record
        read beside this) stays as that path left it: the answer changes nothing."""
        st = trade["live"]
        if not st.get("cancel_sent"):
            st.setdefault("cancel_sent_at", self.market.now_ms())  # the first request's time, off the clock (live_latency: 决定→撤单发出)
            result = await self.live.api.remove_orders([st["order_id"]])
            if st.get("state") != "cancelling":
                return  # 1.150.0: ended meanwhile
            gone_now = st["order_id"] in result["removed"]
            st["cancel_sent"], st["cancel_confirmed"] = now_ms, bool(gone_now)
            if gone_now:
                st["cancel_done_ms"] = int(time.time() * 1000)  # off the book since (the books' clock): one read before may still show it (sim_own_resting)
                st["cancel_done_at"] = self.market.now_ms()
            st["events"].append({"at": now_ms, "what": f"撤单请求已发送（{'Predict 确认已撤' if gone_now else 'Predict 称无需撤/已不在盘口'}）"})
            if gone_now and trade.get("maker"):
                # 1.111.0: Predict confirmed the removal, so the order is off the book and its slot is free now: the record ends
                # on what the open list showed (nothing, in the express lane) and the fills it took before the cancel are
                # read right after (live_cancel_fills), not before the slot is quoted again — one round trip less in the
                # 撤单→重挂 chain (the "撤单确认 0.3 s" of the 1.7 measured on 11-05)
                st["fills_pending"] = int(time.time() * 1000)  # 1.122.0: until that read, the unfilled part stays reserved (live_reserved, sim_open_order)
                self.live_finish(tid, trade, {**(listed or {}), "status": "CANCELLED"}, mk, now_ms)
                self.live_fills_later(tid, trade, mk)
                return
        if listed is not None and not st.get("cancel_confirmed"):
            self.live_cancel_again(st, now_ms, "撤单请求发出后仍在盘口，再发一次")  # still listed: its fills count while it rests
            return
        final = await self.live.api.order(st["order_id"], st.get("hash", ""))
        if st.get("state") != "cancelling":
            return  # 1.150.0: ended meanwhile
        order = float(trade.get("order") or 0)
        if final is not None and not order_final(final, order) and not st.get("cancel_confirmed"):
            self.live_apply(tid, trade, final, mk, now_ms)  # Predict still shows it open and did not confirm the removal: wait —
            # and ask again: missing from the open list but open by its own record, it may well rest, whatever the "noop"
            # said (1.83.0: one removal went out, then never another)
            self.live_cancel_again(st, now_ms, f"撤单未获确认，按哈希查到订单仍为 {str(final.get('status') or '?')}，再发一次")
            return
        if final is None and not st.get("cancel_confirmed"):
            return self.live_unknown(tid, trade, "撤单请求未获 Predict 确认，订单也查不到", now_ms)  # neither gone for sure nor found
        if final is not None and not order_final(final, order):
            final = {**final, "status": "CANCELLED"}  # removed on Predict's word; its record still lags
        st.setdefault("cancel_done_ms", int(time.time() * 1000))
        st.setdefault("cancel_done_at", self.market.now_ms())
        self.live_finish(tid, trade, final or {"status": "CANCELLED"}, mk, now_ms)

    def live_cancel_again(self, st: dict, now_ms: int, what: str) -> None:
        """A removal Predict did not confirm, the order still open LIVE_CANCEL_RETRY_MS after it: asked again next look."""
        if now_ms - int(st.get("cancel_sent") or now_ms) >= self.LIVE_CANCEL_RETRY_MS:
            st["cancel_sent"] = 0
            st["events"].append({"at": now_ms, "what": what})

    # --- an order of unknown state: reserved and blocking until Predict's word, its expiry, or a human says so ----------------
    def live_unknown(self, tid: str, trade: dict, why: str, now_ms: int) -> None:
        """Predict neither shows the order nor confirms its removal: the record is not finished (a "no such order"
        answer is not a cancellation — the order may still be live and fill). It keeps its reservation and its slot,
        is asked for again every step (live_recheck), and ends on Predict's final word, when the evidence has settled
        that it is gone (LIVE_UNKNOWN_LOOKS complete open lists without it, LIVE_UNKNOWN_SETTLE_MS unknown, the account's
        positions showing nothing it filled — 1.104.9), when its expiry has passed (nothing can fill), or when an
        operator releases it (/live release)."""
        st = trade["live"]
        deadline = self.live_order_deadline(trade)
        st.update(state="unknown", unknown_since=st.get("unknown_since") or now_ms, unknown_why=why, unknown_misses=0)
        st["events"].append({"at": now_ms, "what": f"状态未知：{why}。额度照旧占用、这一边不开新单，直到 Predict 给出终态、连续 {self.LIVE_UNKNOWN_LOOKS} 次完整的"
                                                   f"开放订单列表都没有它且 {self.LIVE_UNKNOWN_SETTLE_MS // 60_000} 分钟后账户持仓也没有多出的份数、订单有效期过去"
                                                   + (f"（{core.stamp(deadline, seconds=False)} 之后）" if deadline else "（无到期时间：需人工确认）") + "或 /live release 人工释放"})
        self.live_misses.pop(tid, None)
        self.live_note_error(f"{trade['item']} {trade['label']} 订单状态未知：{why}")
        self.live_notify(f"❓ {trade['item']} {trade['label']}（{'订单 #' + str(st.get('order_id')) if st.get('order_id') else '未拿到订单号'}）状态未知：{why}。"
                         f"额度照旧占用，请在 Predict 网站核对；确认不存在后 /live release {st.get('order_id') or tid} 释放。", key=f"unknown:{tid}")

    def live_order_deadline(self, trade: dict) -> int:
        """When this order can no longer fill, on the bot's clock: a taker's LIMIT expires TAKER_ORDER_SECONDS after
        it was built; a maker's two hours after the market's end, or at the expiry it was signed with should that be
        later — 0 when nothing bounds it (then only Predict's word or an operator ends an unknown record). 1.150.0: an
        order is signed to expire sooner than this now (order_expiry: a minute or two past its market's end), but what
        waits on this — an unknown record's reservation and its side (live_recheck) — is released no sooner than it was:
        a maker signed before that is valid the two hours, and the evidence (the open lists, the positions) ends an
        unknown record within minutes anyway; this is only what is left when it cannot."""
        st, want = trade["live"], trade["live"].get("want") or {}
        if not trade.get("maker"):
            start = int(st.get("placed_at") or st.get("placing_at") or trade.get("opened") or 0)
            return start + TAKER_ORDER_SECONDS * 1000 if start else 0
        expires = int(want.get("expires") or 0)
        end = core.sim_end_ms(trade.get("settle"))
        bounds = [b for b in ((expires * 1000 if 0 < expires < FAR_FUTURE else 0), (end + 2 * 3600_000 if end else 0)) if b]
        return max(bounds) if bounds else 0  # the later of the two (they were one and the same until 1.150.0)

    def live_unknown_suspects(self, trade: dict) -> list[dict]:
        """The unrecorded open orders (live_unknown_open) that could be this unknown record's own order under a hash it
        did not expect. 1.150.0: before, every one of them held the record's release — a manual order on a market this
        bot never touched is never swept (live_orphan_sweep withdraws only on its markets), so it blocked every unknown
        record until its expiry plus the margin (a daily maker: the close plus two hours), the 11-04 "blocked for a day"
        case the 1.104.9 release was made for. The record's signed order names its market and outcome token, so an order
        resting on another market and another token cannot be it. Kept as suspects: an order on the same market or the
        same token, one that cannot be compared (its row, or the record, names neither the market nor the token on the
        other's side: nothing says where it rests)."""
        want = (trade.get("live") or {}).get("want") or {}
        my_mid, my_token = str(want.get("market_id") or trade.get("market_id") or ""), str(want.get("token") or "")
        if not my_mid and not my_token:
            return list(self.live_unknown_open)

        def could_be_this(u: dict) -> bool:
            row = self.live_unrecorded_rows.get(u["id"]) or {}
            market = row.get("market") if isinstance(row.get("market"), dict) else {}
            order = row.get("order") if isinstance(row.get("order"), dict) else {}
            mid = str(market.get("id") or row.get("marketId") or row.get("market_id") or "")
            token = str(order.get("tokenId") or "")
            same_market, same_token = (mid == my_mid if mid and my_mid else None), (token == my_token if token and my_token else None)
            if same_market is None and same_token is None:
                return True  # nothing to compare it by: it may be this one
            return bool(same_market) or bool(same_token)

        return [u for u in self.live_unknown_open if could_be_this(u)]

    async def live_recheck(self, tid: str, trade: dict, by_id: dict, by_hash: dict, mk: Any, now_ms: int) -> None:
        """An unknown record, every step: found on the list or by its hash, it is tracked again (or finished on a final
        word). Missing from LIVE_UNKNOWN_LOOKS complete open lists, unknown to the hash read and unknown for
        LIVE_UNKNOWN_SETTLE_MS, the account's positions decide (1.104.9): nothing beyond what the other records explain
        and it ends unfilled, its slot and reservation freed; a surplus is what it filled. Before, only its expiry plus a
        margin ended it — a daily market's maker order expired two hours after the close then (live_order_deadline still
        waits that long), so the 11-04 长鑫 跌 side sat blocked from 16:46 for a day ("状态未知、对账中"). The expiry path
        stays for a record the evidence did not settle by then: past its expiry the positions alone decide. While they
        cannot (unreadable, the list cut, a surplus that cannot be told from the pre-open mode's shares —
        live_unexplained) the record stays unknown and reserved on either path, the positions read for it once a minute
        at most, until they can, Predict's word, or an operator's /live release."""
        st = trade["live"]
        order = float(trade.get("order") or 0)
        row = by_id.get(str(st.get("order_id") or "-")) or by_hash.get(str(st.get("hash") or "-").lower())
        if row is None and (st.get("hash") or st.get("order_id")):
            try:
                row = await self.live.api.order(str(st.get("order_id") or ""), str(st.get("hash") or ""))
            except Exception as error:
                st["last_error"] = core.clean_error(error) or type(error).__name__
                # 1.15x.x (LIVE-RECHECK-ERROR): a hash read that keeps failing (success=false, a 4xx other than 404, a 5xx)
                # must not hold the record past its signed expiry — it used to return here forever, so an order nothing can
                # fill kept its reservation and its slot until /live release. Once the expiry plus the margin has passed the
                # expiry backstop below decides, by the account's positions (still holding when they cannot be read, booking
                # a surplus as filled). Before that an error is no evidence either way: wait, nothing is released on it.
                deadline = self.live_order_deadline(trade)
                if not deadline or now_ms < deadline + self.LIVE_UNKNOWN_MARGIN_MS:
                    return
        if row is not None:
            st["order_id"] = str(row.get("id") or st.get("order_id") or "")
            st["events"].append({"at": now_ms, "what": f"找回了订单 #{st['order_id']}（{str(row.get('status') or '?')}）"})
            if order_final(row, order):
                self.live_finish(tid, trade, row, mk, now_ms)
                return
            st["state"] = "cancelling" if trade.get("withdrawn") else "open"  # withdrawn meanwhile: the cancel goes out again
            st["cancel_sent"] = 0
            self.live_apply(tid, trade, row, mk, now_ms)
            return
        if self.live_counts_miss(st, int(st.get("unknown_since") or 0)):
            st["unknown_misses"] = int(st.get("unknown_misses") or 0) + 1  # one more complete open list without it (a cut list says nothing)
        deadline = self.live_order_deadline(trade)
        expired = bool(deadline) and now_ms >= deadline + self.LIVE_UNKNOWN_MARGIN_MS
        unknown_ms = now_ms - int(st.get("unknown_since") or now_ms)
        settled = (not expired and int(st.get("unknown_misses") or 0) >= self.LIVE_UNKNOWN_LOOKS and unknown_ms >= self.LIVE_UNKNOWN_SETTLE_MS)
        if not expired and not settled:
            return
        suspects = self.live_unknown_suspects(trade)
        if not expired and suspects:
            # an order on the account this bot has no record of may be this very one under a hash it did not expect: an
            # operator looks first (/live release, or /live cancel account), nothing is released on top of it
            st["last_error"] = (f"连续 {st['unknown_misses']} 次完整列表没有它，但账户上还有 {len(suspects)} 张本机器人没有记录的挂单"
                                f"（#{'、#'.join(u['id'] for u in suspects[:3])}）：可能就是这张，先核对；核对后 /live release 或 /live cancel account")
            return
        if now_ms - int(st.get("unexplained_at") or 0) < self.LIVE_UNEXPLAINED_EVERY_MS:
            # the positions are read for it once a minute at most — past its expiry too (1.150.0). An expired record used to look
            # every step until a look settled it; but a look can settle nothing for days — the list cut at the page cap, and now
            # a surplus that cannot be told from the pre-open mode's shares (live_positions_busy: until an operator's /live
            # release) — and each look reads every page of /positions, inside the step's sync and on the rate limit the orders
            # and the pre-open mode share. The record stays reserved between the looks, as before its expiry.
            return
        st["unexplained_at"] = now_ms
        # nothing can fill it any more (expired), or nothing says it exists (settled) — but it may have filled while it could
        # not be found (Predict's hash of it differing from the one computed here, its answer lost): the account's position on
        # that outcome beyond what the other records explain is what it filled; the absence of a record never is
        extra = await self.live_unexplained(trade)
        head = (f"订单的有效期已过（{core.stamp(deadline, seconds=False)}）" if expired
                else f"连续 {st['unknown_misses']} 次完整的开放订单列表里没有它、按哈希也查不到，状态未知已 {unknown_ms // 60_000} 分钟")
        if extra is None:
            st["last_error"] = head + (f"，但{self.live_positions_busy}（额度照旧占用）" if self.live_positions_busy
                                       else "，但持仓列表没读全（超过分页上限）：下次再核对（额度照旧占用）" if self.live_positions_cut
                                       else "，但持仓读取失败：下次再核对（额度照旧占用）")
            return
        if extra > 1e-9:
            filled = min(extra, order)
            st["events"].append({"at": now_ms, "what": f"{head}，查不到订单，但账户在这一边的持仓比记录多 {extra:g} 份：按成交 {filled:g} 份记（成本按限价，待对账）"})
            status = "EXPIRED" if expired else ("FILLED" if filled >= order - 1e-9 else "CANCELLED")
            self.live_finish(tid, trade, {"status": status, "amount": str(to_wei(order)), "amountFilled": str(to_wei(filled))}, mk, now_ms)
            self.live_notify(f"❓→✅ {trade['item']} {trade['label']}：查不到的订单{'有效期已过' if expired else '已不在 Predict'}，但账户持仓多出 {extra:g} 份，"
                             f"按成交 {filled:g} 份记入（成本按限价 {core.cents(float(trade['price']))} 记，请在 Predict 核对）。", key=f"unknown-filled:{tid}")
            return
        st["events"].append({"at": now_ms, "what": f"{head}，{'之后不可能再成交，' if expired else ''}账户持仓也没有多出的份数：按未成交结束"
                                                   + ("，释放额度" if expired else "，这一边可以再挂")})
        self.live_finish(tid, trade, {"status": "EXPIRED" if expired else "CANCELLED"}, mk, now_ms)
        if not expired:
            self.live_notify(f"❓→✅ {trade['item']} {trade['label']}（订单 #{st.get('order_id') or '?'}）：{head}，账户持仓也没有多出的份数，按未成交结束、额度已释放，"
                             f"这一边恢复挂单。若 Predict 网站上它还挂着，请手动撤掉（同步时也会当作账户无记录的挂单报出来）。", key=f"unknown-released:{tid}")

    async def live_unexplained(self, trade: dict) -> float | None:
        """The shares the account holds on this record's outcome beyond what the other records explain (their filled
        shares on the same market and outcome) and the pre-open mode's fills there: what an order that cannot be found may
        have filled. None when the positions cannot be read now, or not whole (live_positions_cut), or when a surplus cannot
        be told from the pre-open mode's shares (live_positions_busy says why)."""
        self.live_positions_cut, self.live_positions_busy = False, ""
        want = trade["live"].get("want") or {}
        mid, index_set = str(want.get("market_id") or trade.get("market_id") or ""), int(want.get("index_set") or 0)
        if not mid or not index_set:
            return None
        try:
            rows = await self.live.api.positions()
        except Exception:
            rows = None
        # again after the read: the step rechecks its unknown records side by side, and another one's look may have set these
        # meanwhile — live_recheck reads them for this record right after this returns (no await in between)
        self.live_positions_cut, self.live_positions_busy = False, ""
        if rows is None:
            return None
        self.live_positions = (rows, time.time())
        if not getattr(self.live.api, "positions_complete", True):
            # the list was cut at the page cap: a position missing from it says nothing — never "it filled nothing" (1.83.0)
            self.live_positions_cut = True
            return None
        held = 0.0
        for row in rows:
            market = row.get("market") if isinstance(row.get("market"), dict) else {}
            outcome = row.get("outcome") if isinstance(row.get("outcome"), dict) else {}
            if str(market.get("id") or row.get("marketId") or "") == mid and int(outcome.get("indexSet") or 0) == index_set:
                with contextlib.suppress(ValueError, TypeError):
                    held += from_wei(int(str(row.get("amount") or "0")))
        explained, across = 0.0, 0.0  # the other records' shares on this outcome, and on the market's other outcomes
        for other in self.sim_trades().values():
            if other is trade or not isinstance(other.get("live"), dict):
                continue
            o_want = other["live"].get("want") or {}
            if str(o_want.get("market_id") or other.get("market_id") or "") == mid:
                if int(o_want.get("index_set") or 0) == index_set:
                    explained += float(other.get("shares") or 0)
                else:
                    across += float(other.get("shares") or 0)
        extra = max(0.0, held - explained)
        if extra <= 1e-9:
            # nothing beyond the records: this order filled nothing the account still holds, whatever the pre-open mode holds or
            # has yet to read there (as before; a fill since merged into USDT with shares on the market's other side would not show)
            return extra
        # 1.150.0: the pre-open mode's shares are not this order's fill (PREOPEN.md: its fills are never booked onto the model
        # trader's orders; live_recorded_by_outcome, live_late_fills and /live fills count them as recorded). Before, they stayed
        # in this surplus: an order lost on an outcome where that mode held 10 shares was booked "filled 10" at its limit, the
        # same shares then in the model's records and in the mode's ledger — exposure, the P&L and the loss stop counting them
        # twice. A surplus is now attributed only once the pre-open mode's part of it is known for certain:
        # - an order of that mode on this outcome whose fills are not read yet (preopen_unverified): the surplus may be its fill;
        # - its read fills here are taken off only when nothing on the market's other side could have merged with them: Predict
        #   merges the two sides of a market into USDT (1.104.11), so a pair of it filled both ways — or against a record's shares
        #   on the other side — is no longer on the account, and taking its fills off would hide this order's own fill.
        # Either way the record waits, reserved: for the ledger to read the order, else for Predict's word or an operator
        # (/live release) — never a guess in either direction. Without the mode's fills or orders here nothing changes.
        busy, po = self.preopen_unverified(), self.preopen_filled()
        if (mid, index_set) in busy:
            self.live_positions_busy = "这一边有开盘前预挂订单的成交还没读完，多出的份数可能是它的：读完再核对"
            return None
        theirs = float(po.get((mid, index_set), 0.0))
        if theirs <= 1e-9:
            return extra
        other_side = (across > 1e-9 or any(m_id == mid and j != index_set and shares > 1e-9 for (m_id, j), shares in po.items())
                      or any(m_id == mid and j != index_set for m_id, j in busy))  # anything its fills here may have merged with
        if other_side:
            self.live_positions_busy = (f"开盘前预挂在这一边成交了 {theirs:g} 份，这个市场另一边也有份数，Predict 会把两边的份额合并成 USDT，"
                                        "分不清多出的份数是谁的：请在 Predict 核对，确认这张单不存在后 /live release")
            return None
        return max(0.0, extra - theirs)

    def prunable(self, trade: dict) -> bool:
        """A live record that filled nothing is pruned only once a complete open list read after it ended did not show
        its order (1.130.3): a record that wrongly ended — a removal answered but not done, a list that missed the order —
        stays for live_readopt to take the order back; pruned, the order rested on as one nobody here knew, its fills
        unrecorded (the "没有记录的挂单" of 11-06)."""
        st = trade.get("live")
        if not isinstance(st, dict) or not st.get("order_id"):
            return True
        return bool(self.live_open_ids_at) and self.live_open_ids_at > core.trade_updated(trade) and str(st["order_id"]) not in self.live_open_ids

    def live_recorded_by_outcome(self) -> dict[tuple[str, int], float]:
        """The shares the records (and the pre-open mode's fills) explain, by (market id, index set)."""
        out: dict[tuple[str, int], float] = {}
        for t in self.sim_trades().values():
            st = t.get("live")
            if isinstance(st, dict):
                k = (str((st.get("want") or {}).get("market_id") or t.get("market_id") or ""), int((st.get("want") or {}).get("index_set") or 0))
                out[k] = out.get(k, 0.0) + float(t.get("shares") or 0)
        for k, shares in self.preopen_filled().items():
            out[k] = out.get(k, 0.0) + shares
        return out

    def live_unrecorded_positions(self) -> list[dict]:
        """The positions the account holds beyond the records, from the last positions read (live_positions): the review
        page's 账户里有、记录里没有的持仓 (1.130.3). Dust left out."""
        rows, _ = self.live_positions
        recorded = self.live_recorded_by_outcome()
        held: dict[tuple[str, int], dict] = {}
        for row in rows:
            market = row.get("market") if isinstance(row.get("market"), dict) else {}
            outcome = row.get("outcome") if isinstance(row.get("outcome"), dict) else {}
            k = (str(market.get("id") or row.get("marketId") or ""), int(outcome.get("indexSet") or 0))
            h = held.setdefault(k, {"name": f"{market.get('title') or market.get('question') or market.get('slug') or k[0]} / {outcome.get('name') or outcome.get('title') or k[1]}",
                                    "shares": 0.0, "price": None, "at": 0})
            h["shares"] += self.position_shares(row)
            price = self.position_price(row)
            if price is not None:
                h["price"] = price
            h["at"] = max(h["at"], self.position_time(row))
        out = []
        busy = self.preopen_unverified()
        for k, h in held.items():
            if (self.store.get(f"live:redeemed:{k[0]}:{k[1]}") or {}).get("lost"):
                continue  # resolved against it (live_redeem): worth nothing, no holding to explain
            if k in busy:
                continue  # a pre-open order there whose fills are not read yet: likely its own (1.131.5)
            excess = h["shares"] - recorded.get(k, 0.0)
            if excess > self.LIVE_DUST_SHARES:
                out.append({**h, "shares": round(excess, 4), "market_id": k[0], "index_set": k[1]})
        return sorted(out, key=lambda r: -r["shares"])

    def live_counts_miss(self, st: dict, since: int) -> bool:
        """Whether this look counts as one more whole list of the account's open orders without the record's order: this step's
        own read when it came whole, else the latest complete snapshot (the pre-open mode's) read after ``since`` and not counted
        for this record before — one look a snapshot. 1.138.0: with the pre-open mode's thousands of orders no read of this bot's
        was ever whole, so an order of unknown state was never released by the evidence (1.104.9) and held its side and its
        reservation until two hours after its market's end."""
        ids, hashes, at = self.live_whole  # this step's own read when it came whole (live_sync_orders set it just now), else the snapshot
        if not at or at <= since or at == int(st.get("miss_view_at") or 0):
            return False
        if str(st.get("order_id") or "-") in ids or str(st.get("hash") or "-").lower() in hashes:
            return False
        st["miss_view_at"] = at
        return True

    def live_readopt(self, open_rows: list[dict], by_id: dict, by_hash: dict, now_ms: int, at_ms: int | None = None) -> None:
        """An order on the open list whose record here was already finished (judged gone, lapsed, released): it is
        live after all — tracked again with its reservation, never left as "known, so not unknown". A list read before
        the record ended (``at_ms``: the pre-open mode's snapshot) says nothing about that record (1.136.2)."""
        for tid, t in self.sim_trades().items():
            st = t.get("live")
            if not isinstance(st, dict) or st.get("state") != "done" or t.get("status") in {"settled", "expired"}:
                continue
            row = by_id.get(str(st.get("order_id") or "-")) or by_hash.get(str(st.get("hash") or "-").lower())
            # 1.150.0: ...nor does a list of a look that began before Predict confirmed the record's removal (cancel_done_at, off the
            # clock as the answer came). An express pass stamps its events with the time the pass began, before its cancel's round
            # trip: a step that began a moment after it, its list read by Predict before that removal, found the record "ended
            # earlier" and still listed — the cancelled order was taken back (a false ⚠️ alert, its $ reserved and the side held
            # again) and removed a second time
            if row is None or (at_ms is not None and at_ms < max(core.trade_updated(t), int(st.get("cancel_done_at") or 0))):
                continue
            st.pop("final", None)
            st.pop("error", None)
            # 1.138.0: an attempt record (tid#n) is outside its slot — the strategy has moved on, and a resting order there stood beside
            # the slot's own, or let a second one in (the card showed 可挂): taken back to be cancelled, its fills kept
            outside = "#" in tid.rsplit("|", 1)[-1]
            st.update(state="cancelling" if t.get("withdrawn") or outside else "open", order_id=str(row.get("id") or st.get("order_id") or ""), cancel_sent=0)
            if outside and not t.get("withdrawn"):
                st["cancel_why"] = "已结束的尝试记录的订单仍在 Predict 挂着：撤掉，这一边按当前盘口重新判断"
            elif not t.get("withdrawn"):
                t.update(status="resting", note="")
            st["events"].append({"at": now_ms, "what": f"订单 #{st['order_id']} 重新出现在开放订单列表：恢复跟踪，额度重新占用" + ("，随即撤单" if outside else "")})
            self.live_apply(tid, t, row, None, now_ms)
            self.sim_save([(tid, t)])
            self.live_notify(f"⚠️ {t['item']} {t['label']} 订单 #{st['order_id']} 本已按结束处理，现在又出现在 Predict 的开放订单里：已恢复跟踪，额度重新占用。", key=f"readopt:{tid}")

    def live_release_command(self, words: list[str], now_ms: int) -> str:
        """/live release <订单号或记录号>: an operator has checked on the site that an order of unknown state does not
        exist; its record is finished as unfilled and its reservation released."""
        if not words:
            return "用法：/live release <订单号或记录号>（在 Predict 网站确认这张状态未知的订单不存在后，释放它占用的额度）"
        hits = {tid: t for tid, t in self.sim_trades().items() if isinstance(t.get("live"), dict) and t["live"].get("state") == "unknown"
                and (str(t["live"].get("order_id")) == words[0] or tid.lower() == words[0])}
        if not hits:
            return f"没有状态未知、订单号或记录号为 {words[0]} 的订单。"
        for tid, t in hits.items():
            t["live"]["events"].append({"at": now_ms, "what": "管理员确认订单不存在：按未成交结束，释放额度"})
            self.live_finish(tid, t, {"status": "RELEASED"}, None, now_ms)
            t["note"] = "管理员确认订单不存在，已释放额度"
            self.sim_save([(t["live"].get("rekeyed") or tid, t)])  # a taker's record moved to #n (live_fail): saved there
            # its slot is free now (sim_spent), but no new order on that side for LIVE_RETRY_SECONDS: should the order show up
            # on the account's list after all (the next idle read, within a minute), it is tracked again in its own slot —
            # rather than resting beside a fresh one, two orders where one was meant (1.101.2)
            self.live_backoff[(t["market"], t["side"])] = (time.monotonic() + self.config.live_retry, "刚手动释放了这一边一张状态未知的订单")
        return f"已释放 {len(hits)} 张状态未知的订单的额度；它若再出现在 Predict 的开放订单里会自动恢复跟踪。"

    def live_unknown_rows(self, live: list[tuple[str, dict]], now_ms: int) -> list[dict]:
        rows = []
        for tid, t in live:
            st = t["live"]
            if st.get("state") != "unknown":
                continue
            deadline = self.live_order_deadline(t)
            rows.append({"id": tid, "order_id": str(st.get("order_id") or ""), "item": t["item"], "label": t["label"], "price": float(t["price"]),
                         "order": float(t.get("order") or 0), "shares": float(t.get("shares") or 0), "why": str(st.get("unknown_why") or ""),
                         "since": core.stamp(int(st.get("unknown_since") or now_ms), seconds=False), "age_s": (now_ms - int(st.get("unknown_since") or now_ms)) // 1000,
                         "deadline": core.stamp(deadline + self.LIVE_UNKNOWN_MARGIN_MS, seconds=False) if deadline else "", "reserved": self.live_reserved(t)})
        return rows

    def live_apply(self, tid: str, trade: dict, row: dict, mk: Any, now_ms: int) -> None:
        """The fills a record states, onto the trade: new shares become a fill (at the model's fair price now), the
        average price it states replaces the expected cost. Shares landing on a record that lapsed at its market's result
        settle it on that result (live_settle_late, 1.150.0)."""
        st = trade["live"]
        order = float(trade.get("order") or 0)
        filled, status = order_fill(row, order)
        st["api_status"], st["checked_at"] = status, now_ms
        price = order_price(row)
        if price is not None and not trade.get("maker"):
            fee = core.taker_fee(price, int(st["want"].get("fee_bps") or 0))
            trade.update(avg=price, fee=fee, price=price + fee, slip=price - float(trade.get("best", price)), cost_confirmed=True)
        elif not trade.get("maker") and filled > 1e-9 and not trade.get("cost_confirmed") and st["want"].get("cap"):
            # Predict's record states no average price: the fill is costed at the order's limit — the most it could
            # have paid — until a price is read back, never at the estimate made before the order (which may be lower)
            cap = float(st["want"].get("limit") or st["want"]["cap"])
            fee = core.taker_fee(cap, int(st["want"].get("fee_bps") or 0))
            trade.update(avg=cap, fee=fee, price=cap + fee, slip=cap - float(trade.get("best", cap)), cost_confirmed=False)
        prev = float(trade.get("shares") or 0)
        if filled > prev + 1e-9:
            fair = (mk.fair_up if trade["side"] == "up" else 1 - mk.fair_up) if mk is not None else float(trade["fair"])
            trade["fills"].append({"at": now_ms, "shares": filled - prev, "fair": fair,
                                   "how": "真实成交（" + ("挂单" if trade.get("maker") else "吃单") + "）", "order_id": st.get("order_id"),
                                   **({"book": core.book_snapshot(mk.book)} if mk is not None else {})})
            trade["shares"] = filled
            if trade.get("filled") is None:
                trade["filled"], trade["fill_fair"] = now_ms, fair
            self.live_notify(f"✅ 成交：{trade['item']} {trade['label']} {core.cents(float(trade['price']))}×{filled - prev:g} 份"
                             f"（累计 {filled:g}/{order:g}，订单 #{st.get('order_id')}）")
            # 1.150.0: read after the market's result (the record lapsed at 0 shares): settled on that result now. Only as shares
            # land — every caller saves a record whose shares grew; a read that shows nothing new changes nothing here
            self.live_settle_late(trade, now_ms)
        if filled >= order - 1e-9 and trade["status"] == "resting":
            trade["status"] = "filled"
            st["state"], st["final"] = "done", "filled"

    def live_finish(self, tid: str, trade: dict, row: dict, mk: Any, now_ms: int) -> None:
        """The order is no longer on the book: what filled is the position, the rest lapsed."""
        st = trade["live"]
        self.live_apply(tid, trade, row, mk, now_ms)
        status = str(row.get("status") or "").upper()
        shares, order = float(trade.get("shares") or 0), float(trade.get("order") or 0)
        st.update(state="done", final=status.lower() or "closed")
        st["events"].append({"at": now_ms, "what": f"订单结束：{status or '不在盘口'}，成交 {shares:g}/{order:g} 份"})
        self.live_misses.pop(tid, None)
        if trade["status"] in {"settled", "expired"}:
            return  # settled meanwhile (the result came in): nothing to re-decide
        if shares >= order - 1e-9:
            trade["status"] = "filled"
        elif shares > 1e-9:
            trade.update(status="filled", unfilled=order - shares,
                         note=f"真实成交 {shares:g}/{order:g} 份，其余 {order - shares:g} 份{st.get('cancel_why') or status or '未成交'}")
        elif trade.get("maker") and trade["status"] == "cancelled":
            trade["note"] = f"撤单：{st.get('cancel_why') or status}，一份都没成交"
        elif trade.get("maker"):
            trade.update(status="cancelled", unfilled=order, note=f"挂单结束（{status or '不在盘口'}），一份都没成交")
            self.live_notify(f"ℹ️ {trade['item']} {trade['label']} 挂单结束（{status or '不在盘口'}），一份都没成交。")
        else:  # the market said no (nothing at the limit in time, or the bot withdrew the rest): the short wait, not a failure's
            self.live_fail(tid, trade, f"吃单未成交（{st.get('cancel_why') or status or '不在盘口'}）", now_ms, retry=self.config.live_taker_retry)

    # --- redeeming resolved positions -------------------------------------------------------------------------------------
    REDEEM_KINDS = {"redeemed": "领了", "claimable": "可领取", "small": "小额暂不领", "failed": "领取失败", "waiting": "失败等重试",
                    "sent": "已发出待确认", "fields": "缺字段", "unclear": "结算读不出", "kept": "已领仍列着", "open": "未结算", "lost": "输掉的",
                    "unread": "没读到市场"}
    REDEEM_SENT = "live:redeem-sent:"  # + <market>:<index set>: a redeem transaction signed (and broadcast, or about to be) but not yet booked
    REDEEM_GAS = "live:redeem-gas"     # what the redeem transactions mined in the last 24 hours cost: {"fees": [[at ms, fee wei, 1 if it reverted], …]}
    REDEEM_GAS_KEPT = 1000             # ...this many at most (a few hundred a day): one small record, rewritten with each (1.150.0, GAS-STATS)

    async def live_redeem_forget(self, words: list[str]) -> str:
        """/live redeem forget <市场号>…: the LOST marks of those markets lifted, so the next round reads the market again and
        judges afresh — a loser is marked again (no transaction), a winner redeemed once.

        1.150.0 (audit RDM-MARKS-PERMANENT): nothing could lift a mark. A lost mark rests on one market read, so a winner Predict
        named wrong in that read stayed 输掉的 for good. A kept mark (a mined redeem) is NOT lifted: a second redeem of the same
        position would pay nothing on a binary market (the first burnt the whole balance) and only cost gas — the website
        claims what the chain still holds. Under the redeem lock, so it never falls between a round's transaction and its mark.
        The key's trailing colon keeps market 50 from matching 501."""
        mids = [w for w in words if w.isdigit()]
        if not mids:
            return "用法：/live redeem forget <市场号>（市场号见 /live redeem check）：清除这个市场“输掉的”记录，下一轮重新读市场、重新判断输赢"
        lines = []
        async with self.redeem_lock():
            for mid in mids:
                marks = [(k, v) for k, v in self.store.items(f"live:redeemed:{mid}:") if isinstance(v, dict)]
                lost = [k for k, v in marks if v.get("lost")]
                kept = [k for k, v in marks if not v.get("lost")]
                if lost:
                    self.store.delete_keys(lost)
                    lines.append(f"已清除市场 {mid} 的 {len(lost)} 条“输掉的”记录：下一轮自动领取（或 /live redeem）会重新读这个市场、"
                                 "重新判断——输的再记下，赢的领取一次")
                if kept:
                    lines.append(f"市场 {mid} 有 {len(kept)} 条已发过领取交易的记录，没有清除：同一个持仓再发一次领取多半一分钱不付、"
                                 "白付 gas；链上若还有这份持仓，到 Predict 网站上领取")
                if not marks:
                    lines.append(f"市场 {mid} 没有领取记录")
        return "\n".join(lines)

    def redeem_lock(self) -> asyncio.Lock:
        lock = getattr(self, "live_redeem_lock", None)
        if lock is None:
            lock = self.live_redeem_lock = asyncio.Lock()
        return lock

    REDEEM_NOTES_KEPT = 20  # the redeem's own problem lines kept (live_redeem_note); /live redeem check shows the last three

    def live_redeem_note(self, key: str, text: str) -> None:
        """A redeem problem: its line in /live's 最近错误 (live_errors) and in the redeem's own short list (live_redeem_notes: one
        line per position — or per cause: gas, cut, unclear, round, loop — the newest last), which /live redeem check shows as
        最近的领取问题. 1.150.0 (audit RDM-NOTE-THROTTLE): the check took its lines from live_errors, ten lines shared with every
        order failure — ten unfilled taker orders pushed a redeem failure out — and a failure stayed listed after its position
        was redeemed. A line here goes once its cause has (live_redeem_pass)."""
        self.live_note_error(text)
        notes = self.__dict__.setdefault("live_redeem_notes", {})
        notes.pop(key, None)
        notes[key] = f"{core.stamp(self.market.now_ms(), seconds=False)} {text}"
        while len(notes) > self.REDEEM_NOTES_KEPT:
            del notes[next(iter(notes))]

    async def live_redeem(self, now_ms: int, manual: bool = False) -> list[str]:
        """Positions on markets Predict has resolved: the winning outcome's shares are redeemed for USDT on-chain
        (once per market and outcome); the lines say what was done. A position whose redeem failed waits
        LIVE_REDEEM_RETRY_SECONDS — ``manual`` (the owner's /live redeem, the page's 领取已结算) tries it once now: 1.150.0,
        audit RDM-MANUAL-BACKOFF; with no BNB for gas nothing is tried and the administrator hears once an hour. A
        position resolved against the bot is marked settled (``lost``) and never asked about again (1.131.3: each one
        cost a market read every round, and the pre-open mode leaves one or more a filled window — hundreds a day
        against the rate limit its orders and cancels share); a market is read once a round whatever outcomes are held,
        at most LIVE_REDEEM_READS markets a round, the longest unread first (the rest the next rounds; 1.150.0: by the
        newest positions alone the same markets took every read for as long as they stayed unresolved, and a resolved
        winner behind them was never reached), and a rate limit ends the round. One round at a time: the command and
        the automatic round never redeem one position twice."""
        async with self.redeem_lock():
            self.live_redeem_since = self.market.now_ms()  # 1.150.0 (RDM-ROUND-LOCK-STALL): /live and the check say since when a pass holds the lock
            try:
                done, _ = await self.live_redeem_pass(now_ms, force=manual)
            finally:
                self.live_redeem_since = 0
            if manual:  # (RDM-STATUS-LINE) the round /live shows as 上一轮 was the owner's, not the automatic one: said so there
                self.live_redeem_last = {**self.live_redeem_last, "manual": True}
            return done

    def live_redeem_fees(self, now_ms: int) -> list[list[int]]:
        """The redeem transactions mined in the last 24 hours, oldest first: [at ms, what its gas cost in wei, 1 if it reverted]
        (REDEEM_GAS; an entry that is no such list is passed over)."""
        record = self.store.get(self.REDEEM_GAS)
        rows = record.get("fees") if isinstance(record, dict) else None
        if not isinstance(rows, list):
            return []
        return [[r[0], r[1], 1 if len(r) > 2 and r[2] else 0] for r in rows
                if isinstance(r, list) and len(r) >= 2 and all(isinstance(x, int) and not isinstance(x, bool) for x in r[:2])
                and now_ms - r[0] < core.DAY_MS]

    def live_redeem_fee_pair(self, fee: int, now_ms: int, reverted: bool = False) -> tuple[str, dict]:
        """REDEEM_GAS with one more transaction's fee, as a (key, record) pair: written by the caller in the same store
        transaction as what it books (the mark, the sent note's going), so a fee is never counted twice or lost between them.
        The entries over a day old go; REDEEM_GAS_KEPT at most."""
        fees = [*self.live_redeem_fees(now_ms), [now_ms, int(fee), 1 if reverted else 0]]
        return self.REDEEM_GAS, {"fees": fees[-self.REDEEM_GAS_KEPT:]}

    def live_redeem_gas_day(self, now_ms: int) -> tuple[int, int, int]:
        """(redeem transactions mined in the last 24 hours, what their gas cost in wei, how many of them reverted)."""
        fees = self.live_redeem_fees(now_ms)
        return len(fees), sum(f[1] for f in fees), sum(f[2] for f in fees)

    def live_redeem_booked(self, key: str, tx: str, paid: int | None, amount: int, title: str, outcome: str, now_ms: int,
                           done: list[str], sent_key: str = "", fee: int | None = None) -> dict:
        """A mined redeem (status 1) booked: the mark (and the sent note gone, in the same transaction), the line said and
        pushed — 💵 with what it paid, or the words for one that paid nothing (RDM-PAYOUT-UNCHECKED). Its seen fields.

        1.150.0 (GAS-STATS): the owner asked what a day of redeems costs in gas and how long the signer's BNB lasts — nothing
        kept that. ``fee`` (its receipt's gasUsed × effectiveGasPrice: Chain.receipt_fee, no request) goes on the mark
        (fee_wei) and into REDEEM_GAS, in the same store transaction; None (a receipt that does not say) writes neither."""
        # 1.150.0 (audit RDM-PAYOUT-UNCHECKED): 💵 已领取 was said, and the position closed for good, on a mined receipt alone —
        # the payout never looked at. A redeem of a balance already gone (claimed on the website a moment before, or by an
        # earlier transaction the bot did not book) is mined with status 1 and pays nothing, yet read as paid. The receipt
        # in hand says what was paid (its PayoutRedemption: no extra request). The mark is written whatever the payout — a
        # zero left unmarked would be redeemed again, for nothing, every round Predict still lists the row — but a zero is
        # said as such, not as 💵; a payout the receipt does not state (None) reads as before.
        mark = {"at": now_ms, "tx": str(tx), "amount": str(amount), "title": title, **({"paid": str(paid)} if paid is not None else {}),
                **({"fee_wei": str(fee)} if fee is not None else {})}
        self.store.replace([sent_key] if sent_key else [], [(key, mark), *([self.live_redeem_fee_pair(fee, now_ms)] if fee is not None else [])])
        shares = f"{title} {outcome} {from_wei(amount):g} 份"
        if paid == 0:
            line = (f"⚠️ 领取交易上链了，但没领到 USDT：{shares}（交易 {tx}）。链上已经没有这份持仓：多半已在网站上领过，"
                    "或之前的一笔领取已经到账；网站上若还显示可领取，把这条发我")
            self.live_redeem_note(key, line)
            fields = {"kind": "kept", "tx": str(tx), "paid": "0"}
        else:
            line = f"💵 已领取 {shares}" + (f"，到账 ${from_wei(paid):,.2f}" if paid else "") + f"：{tx}"
            fields = {"kind": "redeemed", "tx": str(tx)}
        done.append(line)
        self.live_notify(line)
        return fields

    async def live_redeem_sent(self, now_ms: int, done: list[str], errors: dict, unsent_runs: dict) -> tuple[dict[str, dict], dict[str, dict]]:
        """Every redeem sent but not booked (its note, REDEEM_SENT), looked up by its receipt before anything is sent for that
        position again: (booked or failed now — position key -> its seen fields; still out — position key -> its note).

        1.150.0 (audit RDM-RECEIPT-POLL, RDM-RESTART-MID-SEND): nothing was recorded while a redeem transaction was out. A
        receipt that did not come within the wait (or an answer to the raw transaction lost on the way) was booked as a
        failure, the hash forgotten: no mark, no 💵 for a redeem that had paid, and an hour later — or at once after a restart
        (every deploy: the back-offs live in memory), with Predict still listing the row — a second transaction that paid
        nothing was the one announced 💵 (a neg-risk one: 没领成 for a position already paid). The hash is now noted on disk
        before the broadcast (Chain.send's ``sent``), and each round asks for its receipt first — once per note, a BNB read,
        never Predict's: status 1 is booked with THAT hash (whether Predict still lists the row or not), status 0 is the
        failure it is (an hour), none leaves the position alone. Only a note an hour old whose transaction the node does not
        know at all (never mined, not in its pool: dropped, or never broadcast) is let go — then the position is redeemed
        again, never sooner."""
        booked: dict[str, dict] = {}
        out: dict[str, dict] = {}
        chain = self.live.chain
        for sent_key, note in self.store.items(self.REDEEM_SENT):
            key = "live:redeemed:" + sent_key.removeprefix(self.REDEEM_SENT)
            if not isinstance(note, dict) or not note.get("tx"):
                self.store.delete_keys([sent_key])  # (unreadable: says nothing)
                continue
            tx, title, age_ms = str(note["tx"]), str(note.get("title") or "?"), max(0, now_ms - int(note.get("at") or now_ms))
            signed = note.get("gas_price")  # (GAS-STATS) the price it was signed with: its fee where the receipt states none
            try:
                receipt = await chain.receipt(tx)
            except Exception as error:  # unknown: nothing is sent for it this round
                out[key] = {**note, "age_ms": age_ms, "error": f"回执没读到：{core.clean_error(error) or type(error).__name__}"}
                continue
            if receipt is not None:
                try:
                    status = int(str(receipt.get("status") or "0x0"), 16)
                except ValueError:
                    status = -1
                if status == 1:
                    neg, yb = bool(note.get("neg_risk")), bool(note.get("yield_bearing"))
                    paid = chain.redeem_payout(receipt, chain.redeem_target(neg, yb), neg)
                    errors.pop(key, None)
                    unsent_runs.pop(key, None)
                    self.live_redeem_failed.pop(key, None)
                    self.__dict__.setdefault("live_redeem_untold", {}).pop(key, None)  # (RDM-NOTE-THROTTLE) booked: nothing left to tell or list
                    self.__dict__.setdefault("live_redeem_notes", {}).pop(key, None)
                    booked[key] = self.live_redeem_booked(key, tx, paid, int(str(note.get("amount") or "0")), title,
                                                          str(note.get("outcome") or ""), now_ms, done, sent_key, Chain.receipt_fee(receipt, signed))
                    continue
                if status == 0:  # mined and reverted: its gas spent, nothing more to come of it — the failure's hour
                    fee = Chain.receipt_fee(receipt, signed)  # (GAS-STATS) counted with the note's going, in one store transaction
                    self.store.replace([sent_key], [self.live_redeem_fee_pair(fee, now_ms, reverted=True)] if fee is not None else [])
                    text = f"交易失败（已回滚）：{tx}"
                    self.live_redeem_failed.setdefault(key, time.monotonic())
                    errors[key] = text
                    unsent_runs.pop(key, None)
                    self.live_redeem_note(key, f"领取 {title} 失败：{text}（一小时后再试）")
                    booked[key] = {"kind": "failed", "error": text, "unsent": False}
                    self.__dict__.setdefault("live_redeem_untold", {})[key] = {"title": title, **booked[key]}  # (RDM-NOTE-THROTTLE) its gas spent: told
                    continue
            if age_ms < self.LIVE_REDEEM_RETRY_SECONDS * 1000:
                out[key] = {**note, "age_ms": age_ms}
                continue
            try:
                known = await chain.known(tx)
            except Exception as error:
                out[key] = {**note, "age_ms": age_ms, "error": f"节点查不到它的状态：{core.clean_error(error) or type(error).__name__}"}
                continue
            if known:  # still in the node's pool: a second redeem could be mined beside it
                out[key] = {**note, "age_ms": age_ms, "error": "节点里还在排队，没上链"}
                continue
            self.store.delete_keys([sent_key])  # an hour on, never mined and unknown to the node: let go — redeemed again below
            self.live_redeem_note(key, f"领取 {title} 的交易 {tx} 一小时没上链、节点里也查不到：重新领取")
        return booked, out

    async def live_negrisk_amount(self, outcome: dict, info: MarketInfo, index_set: int, listed: int) -> int:
        """What a multi-outcome redeem names: the account's balance of the winning token on the chain, when one read says it.

        1.150.0 (audit RDM-NEGRISK-AMOUNT): the adapter pulls exactly the amounts the redeem names, and the bot named Predict's
        listed amount (as the SDK recipe does): one wei more than held reverted the estimate every round, less left the rest
        on the chain behind the mark for good. One eth_call (balanceOf on the track's multi-outcome tokens: no transaction,
        not Predict's limit), only for a neg-risk redeem about to be sent. Any doubt keeps the listed amount as before — no
        token id, two different ids (the row's and the market's), a failed or empty read, a zero — never a skip or a mark."""
        ids = {str(outcome.get("onChainId") or outcome.get("tokenId") or ""),
               next((str(o.get("token") or "") for o in info.outcomes if o.get("index_set") == index_set), "")} - {""}
        read = getattr(self.live.chain, "outcome_balance", None)
        if len(ids) != 1 or not callable(read):
            return listed
        try:
            held = await read(ids.pop(), True, info.yield_bearing)
        except Exception:
            return listed
        return held if isinstance(held, int) and held > 0 else listed

    async def live_redeem_pass(self, now_ms: int, dry_run: bool = False, force: bool = False) -> tuple[list[str], list[dict]]:
        """One look at the account's positions, the markets read the longest ago first (never yet: first of all; the
        newest positions first among equals): (the redeemed lines, each position judged). Kinds:
        redeemed (now), claimable (resolved in its favour, not redeemed now: ``dry_run``, or no gas), small (resolved in
        its favour, worth less than LIVE_REDEEM_MIN_USD: left as it is), failed (this round), waiting (a failed redeem's
        back-off), lost, open (not resolved), unread (its market not read: the round's cap, a failed read, a rate limit),
        fields (no index set on the position, no condition on the market), kept (redeemed before, still listed).
        ``dry_run`` (/live redeem check): nothing sent, written or announced — the markets the next round would read,
        whose turn it leaves as it was."""
        # 1.150.0 (audit RDM-POSITIONS-CUT): the redeem's own read, LIVE_REDEEM_PAGES deep, and its own answer whether the list
        # came whole. It read api.positions() — ORDERS_PAGES_MAX pages, 1000 rows at 50 a page — and never looked at whether
        # that was the whole list: the bot never burns a losing token, so for as long as Predict lists resolved losers the list
        # grows with every filled window (how fast is not measured: the owner's check of 11-11 listed 48 rows, no lost one among
        # them), and a winner listed past the cut was never seen by any round (the ten-minute one, the one a win brings, /live
        # redeem) while the round said 持仓 1000 个：输掉的 1000 and the check 赢的都领完了. The answer is
        # taken from this read itself, never from api.positions_complete: that flag is shared with the once-a-minute readers
        # (live_late_fills, /live fills, live_unexplained), which keep their twenty pages and their flag. A list longer even
        # than this read is said — live_redeem_cut, the round's summary, the check, one notice an hour — never taken whole.
        read = getattr(self.live.api, "positions_read", None)
        try:
            if callable(read):
                rows, whole = await read(self.LIVE_REDEEM_PAGES)
            else:  # a client without it (a stand-in): its own read and flag
                rows = await self.live.api.positions()
                whole = bool(getattr(self.live.api, "positions_complete", True))
        except Exception as error:  # (RDM-ROUND-VOID) the same words for /live redeem and the check; the automatic round looks again soon
            raise RedeemUnread(core.clean_error(error) or type(error).__name__, int(getattr(error, "retry_after", 0) or 0)) from error
        cut = 0 if whole else len(rows)
        self.live_redeem_cut = cut  # (the dry run's for live_redeem_report, read under the same lock right after it)
        self.live_positions = (rows, time.time())
        done: list[str] = []
        seen: list[dict] = []
        balance = self.live.balance
        bnb = int(balance[1]) if balance is not None and len(balance) >= 2 else None
        gas_unread = ""
        if not dry_run and (balance is None or len(balance) < 3 or time.time() - float(balance[2]) > self.LIVE_BALANCE_SECONDS):
            # 1.150.0 (audit RDM-GAS-STALE): the gas was judged on whatever reading was left when the re-read failed — USDT and BNB
            # are read together (refresh_balance), so one failing half stored neither, and an old LOW reading kept every round
            # at BNB 不足 and the ⛽ note asking for BNB already sent, while /live redeem check (its own read) said 够付 gas. The
            # BNB alone is read now when the joint read fails (never written into the stored pair: its stamp vouches for the
            # USDT the order gate relies on). With no fresh figure at all, a reading up to LIVE_BALANCE_STALE_S old still
            # judges (as for orders); an older one is not taken for the present — neither 不足 nor a ⛽ note, and nothing sent
            # blind: the round says the BNB could not be read and leaves the winners 可领取. Never read: as before (not low).
            try:  # the paper trader's step keeps it fresh; with that step off, here (1.131.4)
                await self.live.refresh_balance()
                bnb = int(self.live.balance[1])
            except Exception as error:
                try:
                    bnb = int(await self.live.chain.bnb_balance())
                except Exception as alone:
                    if balance is not None and len(balance) >= 3 and time.time() - float(balance[2]) > self.LIVE_BALANCE_STALE_S:
                        gas_unread = (f"签名钱包的 BNB 读不到（{core.clean_error(alone) or core.clean_error(error) or type(alone).__name__}），"
                                      f"上次读到是 {int((time.time() - float(balance[2])) // 60)} 分钟前")
                        bnb = None
        low_gas = bnb is not None and bnb < self.LIVE_REDEEM_MIN_BNB_WEI
        markets: dict[str, dict | None] = {}  # this round's reads, one per market (None: the read failed)
        read_errors: dict[str, str] = {}
        limited = False
        errors = self.__dict__.setdefault("live_redeem_errors", {})
        untold = self.__dict__.setdefault("live_redeem_untold", {})  # (RDM-NOTE-THROTTLE) position key -> a sent redeem that failed, not yet told
        notes = self.__dict__.setdefault("live_redeem_notes", {})    # (RDM-NOTE-THROTTLE) the redeem's own problem lines (live_redeem_note)
        # 1.150.0: the markets go by how long ago this process read them here — never yet first, then by the round of their last read,
        # the newest positions first among equals. By the newest positions alone (1.131.3) a capped round spent its LIVE_REDEEM_READS
        # on the same newest markets every round for as long as they stayed unresolved (or their read kept failing), so a resolved
        # winner behind forty of them was never read — by the automatic round or by /live redeem — and its USDT stayed locked: the
        # pre-open mode's fills on the windows ahead are newer than the window that just resolved. A round that a rate limit cut
        # short began at the same markets again, too. Now a round reads first what the last one did not reach and fills up with the
        # newest: every market's turn comes within about (markets / LIVE_REDEEM_READS) rounds, and while one round reads them all
        # each is read every round, as before.
        read_round = self.__dict__.setdefault("live_redeem_read_round", {})
        this_round = max(read_round.values(), default=0) + 1
        unsettled: set[str] = set()  # the markets of the positions with no redeemed / lost mark: only their turn is remembered
        # 1.150.0 (audit RDM-UNSENT-RETRY-GRID): a redeem that never left (TxNotSent: the chain a moment behind Predict's result, an
        # RPC blip) waited LIVE_REDEEM_UNSENT_RETRY_SECONDS and was promised 5 分钟后 — but nothing brings a round when a wait ends:
        # the retry is the next round that comes (a win's, the ten-minute one), and a round that comes inside the wait finds it
        # waiting and starts the ten minutes afresh: five to fifteen minutes. No round is brought for it: tried (the next round
        # pulled to the wait's end), it cost 20-60 % more Predict requests a day on the owner's flow with the chain 30-90 s behind
        # Predict's result — every win's first try fails so and each brought a round of its own (a positions read and market reads
        # on the key the orders share) — and at 45 s it made the typical wait longer (0.6 -> 4.4 min): the pulled round met the
        # next windows before the chain had them. Instead a position's FIRST such try (no failed redeem before it in this process)
        # waits LIVE_REDEEM_UNSENT_FIRST_SECONDS, so the rounds that come anyway take it — measured at a 90 s lag: the typical
        # wait 9 -> 5 min, the longest 15 -> 10, with the same rounds, requests and transactions; the later ones keep five minutes
        # (a chain still behind, an RPC still failing). The texts say what happens: the next round after the wait, at the latest
        # a round's interval after it (live_redeem_retry_words). A sent failure (its gas paid; its transaction may yet be mined)
        # keeps its hour. The counter of such failures in a row only bounds the naming below.
        unsent_runs = self.__dict__.setdefault("live_redeem_unsent", {})
        # 1.150.0 (audit RDM-SOON-NO-TARGET): a market a win just named (live_redeem_soon) is read first, inside the cap, ahead of the
        # rotation; a real round that reads it drops the name (one read ahead per win, never a standing priority). Kept through its
        # first LIVE_REDEEM_UNSENT_NAMED redeems in a row that never left: the retry after the wait reads it first too — then the
        # rotation, so winners the chain keeps refusing never take every round's reads.
        first = self.__dict__.setdefault("live_redeem_first", {})
        # 1.150.0 (GAS-MIN-VALUE): the owner's floor on what a redeem transaction is worth sending for. A few hundred redeems a day,
        # each paid in BNB by the signer, and many of them pay the pre-open mode's ten-share wins: with LIVE_REDEEM_MIN_USD > 0 a
        # resolved winner worth less (shares × the payout per share, as below) is neither redeemed — by the automatic round or by
        # /live redeem — nor counted as claimable (the ⛽ note, the gas verdicts), nor marked: it stays the owner's, shown as
        # 小额暂不领, and the first round after the setting is lowered or cleared (or the position grows past it) redeems it.
        # Such a winner is remembered with its payout per share (a resolved market's result does not change), so the rounds after
        # judge it with no market read: left unread it would come back to the rotation for good, a read every round taken from
        # the markets still to be judged. 0 (the default): none of this — every winner as before.
        floor = float(self.config.live_redeem_min_usd or 0)
        small = self.__dict__.setdefault("live_redeem_small", {})
        # 1.150.0 (audit RDM-ROUND-LOCK-STALL): the winners are redeemed one after another, each waiting up to 120 s for its receipt
        # under the redeem lock — with a node that takes the transactions and mines none (a stuck nonce, an underpriced gas price)
        # a round of N winners held the lock N × two minutes and sent N transactions into the same stuck queue. After one receipt
        # that did not come in time (TxPending) this round sends nothing more: the rest stay 可领取 — no back-off, no mark — for
        # the next round; the pending one keeps its hour (it may yet be mined: a second redeem would pay nothing).
        held = ""
        gas_refused = ""  # (RDM-UNSENT-AS-SENT) the node refused a redeem for the gas this round: the rest are 可领取 under the ⛽ note
        # (RDM-RECEIPT-POLL, RDM-RESTART-MID-SEND) the redeems sent but not booked: each looked up by its receipt first (a real round;
        # the dry run reads no chain and writes nothing: it shows them as they stand)
        if dry_run:
            booked = {}
            out = {"live:redeemed:" + k.removeprefix(self.REDEEM_SENT): {**v, "age_ms": max(0, now_ms - int(v.get("at") or now_ms))}
                   for k, v in self.store.items(self.REDEEM_SENT) if isinstance(v, dict) and v.get("tx")}
        else:
            booked, out = await self.live_redeem_sent(now_ms, done, errors, unsent_runs)
        market_of = lambda r: str((r.get("market") if isinstance(r.get("market"), dict) else {}).get("id") or r.get("marketId") or "")
        for row in sorted(rows, key=lambda r: (market_of(r) not in first, read_round.get(market_of(r), 0), -self.position_time(r))):
            market = row.get("market") if isinstance(row.get("market"), dict) else {}
            outcome = row.get("outcome") if isinstance(row.get("outcome"), dict) else {}
            mid = market_of(row)
            # 1.150.0 (audit RDM-ROW-ABORT): one row the pass cannot read is that row's problem, never the round's. An index set int()
            # cannot read ('YES', '1.0', a list) raised out of the pass — every round (and /live redeem check) ended at that row, and
            # under the rotation its market, never read, sorted first every round: nothing at all redeemed. A row whose amount is no
            # whole number of wei, or that names no market, was dropped without a word (a false all-clear if every row came so).
            # Each is now 缺字段 with its reason: shown and counted, no market read, no transaction, no mark — never a guess at a side
            # or an amount (a guessed side can redeem the wrong one or mark a winner lost). An emptied row (amount 0) stays uncounted.
            try:
                amount = int(str(row.get("amount") or "0"))
            except ValueError:
                amount = None  # stated, but no whole number of wei: unreadable, which is not "nothing held"
            if amount is None or (not mid and amount > 0):
                seen.append({"title": str(market.get("title") or market.get("question") or mid or "?"), "outcome": str(outcome.get("name") or "?"),
                             "shares": 0.0 if amount is None else from_wei(amount), "market_id": mid, "index_set": 0, "kind": "fields",
                             "error": f"持仓的份数（amount）读不懂（{core.brief_error(repr(row.get('amount')), 40)}）" if amount is None else "持仓没写市场编号"})
                continue
            if amount <= 0:
                continue  # nothing held (an emptied position still listed): nothing to redeem, in no count — as before
            unreadable = ""
            try:
                index_set = int(outcome.get("indexSet") or 0)
            except (TypeError, ValueError, OverflowError):
                index_set, unreadable = 0, f"持仓的 indexSet 读不懂（{core.brief_error(repr(outcome.get('indexSet')), 40)}），分不出是哪一边"
            key = f"live:redeemed:{mid}:{index_set}"
            entry = {"title": str(market.get("title") or market.get("question") or mid), "outcome": str(outcome.get("name") or index_set or "?"),
                     "shares": from_wei(amount), "market_id": mid, "index_set": index_set}
            if key in booked:  # (RDM-RECEIPT-POLL) its sent transaction looked up this round: booked (or failed) above
                if booked[key]["kind"] == "failed":
                    unsettled.add(mid)
                seen.append({**entry, **booked[key]})
                continue
            mark = self.store.get(key)
            # 1.150.0 (audit RDM-MARKS-PERMANENT): a mark is for good — what stops a second redeem of a row Predict still lists and
            # a re-read of every listed loser (1.131.3) — but it is said in full (the whole hash, its age, the market id: the
            # check cut the hash to 14 characters and said 过 10 分钟还在 for ever), and a lost one can be lifted by the owner
            # (/live redeem forget <市场号>: live_redeem_forget). Never lifted automatically, never into a second redeem.
            if mark:
                seen.append({**entry, "kind": "lost" if mark.get("lost") else "kept", "tx": str(mark.get("tx") or ""),
                             "paid": mark.get("paid"),  # (RDM-PAYOUT-UNCHECKED) "0": mined, but it paid nothing
                             "marked_at": int(mark.get("at") or 0)})
                continue
            unsettled.add(mid)
            if key in out:  # (RDM-RECEIPT-POLL, RDM-RESTART-MID-SEND) a transaction out for it, no receipt yet: nothing sent beside it
                s = out[key]
                seen.append({**entry, "kind": "sent", "tx": str(s.get("tx") or ""), "age_ms": int(s.get("age_ms") or 0), "error": str(s.get("error") or "")})
                continue
            failed_at = self.live_redeem_failed.get(key)
            # 1.150.0 (audit RDM-MANUAL-BACKOFF): the back-off held /live redeem (and the page's 领取已结算) as well — after the
            # cause was gone (BNB topped up, the RPC back) the owner's command sent nothing for up to an hour and answered 没有可领取
            # 的已结算持仓; only a restart cleared it. ``force`` (that command) tries such a position once in this pass: one attempt
            # per position per command; the automatic rounds keep the wait. Nothing beside a transaction still out (the sent note
            # above comes first), the gas floor and the marks as ever, and Chain.send's estimate refuses a redeem that would revert
            # before anything is spent. A failure here starts its wait afresh.
            if not force and failed_at is not None and time.monotonic() - failed_at < self.LIVE_REDEEM_RETRY_SECONDS:
                seen.append({**entry, "kind": "waiting", "left": self.LIVE_REDEEM_RETRY_SECONDS - (time.monotonic() - failed_at), "error": errors.get(key, "")})
                continue
            if not index_set:
                seen.append({**entry, "kind": "fields", "error": unreadable or "持仓没写 indexSet，分不出是哪一边"})
                continue
            per_share = small.get(key) if floor > 0 else None
            if per_share is not None and entry["shares"] * per_share < floor:  # (GAS-MIN-VALUE) a small winner judged before: no read
                seen.append({**entry, "value": entry["shares"] * per_share, "kind": "small"})
                continue
            if mid not in markets:
                if limited or len(markets) >= self.LIVE_REDEEM_READS:
                    seen.append({**entry, "kind": "unread", "error": "限流，这一轮不再读" if limited else "这一轮读满了，下一轮再看"})
                    continue
                try:
                    markets[mid] = await self.live.api.market(mid)
                except Exception as error:
                    markets[mid] = None
                    read_errors[mid] = core.clean_error(error) or type(error).__name__
                    limited = bool(getattr(error, "retry_after", 0) or http_status(error) == 429)  # rate-limited: nothing more asked of Predict this round
                if not dry_run and not limited:
                    # read — or not readable (a delisted market's 404): the other markets' turn before it is read again. One the rate
                    # limit stopped keeps its place: it is the next round's first
                    read_round[mid] = this_round
                    first.pop(mid, None)  # (a name is one read ahead: read now, back in the rotation)
            raw = markets[mid]
            if raw is None:
                seen.append({**entry, "kind": "unread", "error": read_errors.get(mid, "")})
                continue
            try:  # (RDM-ROW-ABORT) the market's own JSON in a shape these cannot read: this position alone — not redeemed, not marked lost
                info = parse_market(raw, self.config.predict_fee_bps)
                resolved = core.predict_resolution(raw)
            except Exception as error:
                seen.append({**entry, "kind": "fields", "error": f"市场详情读不懂（{core.clean_error(error) or type(error).__name__}），判断不了输赢"})
                continue
            settled = core.PredictFeed.market_settled({"status": info.status})
            if not settled or resolved is None:
                # 1.150.0 (audit RDM-OPEN-SHAPES): both signals must say "over" before anything is sent or written — the settled
                # status word and a readable winner — and that stays: a named result without the settled word may still be a
                # proposal (a redeem then could be mined for nothing if it flips; never a lost mark on less than settled). But half
                # an answer read 未结算 for ever, with 结算后自动领取 in the check and nothing said: now its own kind with the
                # missing half named, and told on Telegram once it has stood so (live_redeem_unclear, below). Neither half: 未结算.
                if settled or resolved is not None:
                    why = ("Predict 写了已结算，但读不出哪一边赢" if settled
                           else f"Predict 写了结果（{resolved.get('name') or '?'}），但市场状态不是已结算")
                    seen.append({**entry, "kind": "unclear", "status": info.status or "?", "error": why, "settled": settled})
                else:
                    seen.append({**entry, "kind": "open", "status": info.status or "?"})
                continue
            won = resolved.get("split") or (resolved.get("index") is not None and index_set == int(resolved["index"]) + 1)
            if not won:
                if resolved.get("index") is not None and not dry_run:  # resolved against it: worth nothing, never read again
                    self.store.put(key, {"at": now_ms, "lost": True, "amount": str(amount), "title": entry["title"],
                                         "status": info.status, "winner": str(resolved.get("name") or ""),
                                         "how": str(resolved.get("how") or "")})  # (RDM-MARKS-PERMANENT) the read it rests on
                seen.append({**entry, "kind": "lost"})
                continue
            entry["value"] = entry["shares"] * (0.5 if resolved.get("split") else 1.0)
            if not info.condition_id:
                seen.append({**entry, "kind": "fields", "error": "市场没写 conditionId，领取交易填不出来"})
                continue
            if floor > 0 and entry["value"] < floor:  # (GAS-MIN-VALUE) under the owner's floor: left, unmarked, never claimable
                if not dry_run:
                    small[key] = 0.5 if resolved.get("split") else 1.0
                seen.append({**entry, "kind": "small"})
                continue
            if dry_run or low_gas or held or gas_unread:  # (RDM-GAS-STALE) no BNB figure to send on: none sent blind
                # 1.150.0 (audit RDM-CHECK-FORGETS-FAILURE): with its back-off over, a position whose last redeem failed was 可领取 with
                # nothing more — the remembered reason was read in the back-off alone — and /live redeem check concluded 下一轮自动领取会领
                # for one the chain refuses round after round (half of every ten minutes for a redeem that never leaves). The reason goes
                # with the row; the kind stays (the counts, /live redeem, the ⛽ note count it as before).
                seen.append({**entry, "kind": "claimable", **({"error": errors[key]} if key in errors else {})})
                continue
            if info.neg_risk:  # (RDM-NEGRISK-AMOUNT) the chain's balance, not Predict's figure: what the adapter will pull
                amount = await self.live_negrisk_amount(outcome, info, index_set, amount)
            sent_key = self.REDEEM_SENT + key.removeprefix("live:redeemed:")

            async def note_sent(tx_hash: str, sent_key: str = sent_key, title: str = entry["title"], name: str = str(outcome.get("name") or ""),
                                neg: bool = info.neg_risk, yb: bool = info.yield_bearing) -> None:
                """(RDM-RESTART-MID-SEND) the hash on disk before the broadcast, as an order's is (live_place): no note, no transaction.
                (GAS-STATS) with the gas price it was signed with (Sent.gas_price): its fee once a later round reads its receipt."""
                signed = getattr(tx_hash, "gas_price", None)
                self.store.put(sent_key, {"at": now_ms, "tx": str(tx_hash), "amount": str(amount), "title": title, "outcome": name,
                                          "neg_risk": neg, "yield_bearing": yb, "maker": self.live.wallet.maker,
                                          **({"gas_price": int(signed)} if isinstance(signed, int) else {})})
                if not await asyncio.to_thread(self.store.flush):
                    raise TxNotSent(f"领取记录没写进数据库（{self.store.write_error or '超时'}），交易没发出")
            try:
                # (a stand-in chain without the note: its redeem as before)
                more = {"sent": note_sent} if isinstance(self.live.chain, Chain) else {}
                tx = await self.live.chain.redeem(info.condition_id, index_set, amount, info.neg_risk, info.yield_bearing, **more)
            except Exception as error:
                # (GAS-STATS) a redeem mined and reverted cost its gas all the same: counted, with its note's going (one transaction)
                gas_paid = [self.live_redeem_fee_pair(error.fee, now_ms, reverted=True)] if isinstance(error, TxReverted) and error.fee is not None else []
                if isinstance(error, (TxNotSent, TxReverted)) and (self.store.get(sent_key) or gas_paid):
                    # nothing left the bot, or it was mined and failed: no transaction to look up
                    self.store.replace([sent_key] if self.store.get(sent_key) else [], gas_paid)
                if isinstance(error, TxNotSent) and "insufficient funds" in str(error).lower():
                    # 1.150.0 (audit RDM-UNSENT-AS-SENT): the node refused the transaction for its gas — the wallet's shortage,
                    # not this position's failure (the 0.0005 floor is judged once a round, on a fixed figure: a round that
                    # runs the wallet down, or a higher gas price, ends here). It and the rest of the round are 可领取 under the
                    # ⛽ note with the address to fund — no back-off, no 没领成, no refused send per winner; the next round
                    # tries again (after a top-up: at once). Nothing was broadcast, nothing spent.
                    low_gas, gas_refused = True, core.clean_error(error)
                    seen.append({**entry, "kind": "claimable"})
                    continue
                unsent = isinstance(error, TxNotSent)  # never left (1.131.3): soon again — the chain may be a moment behind Predict's result
                # (RDM-UNSENT-RETRY-GRID) its first such try — no failed redeem before it in this process — waits a minute: the rounds
                # that come anyway take it; a second one in a row (the chain still behind, the RPC still failing) five
                fresh = unsent and key not in errors
                wait = (self.LIVE_REDEEM_UNSENT_FIRST_SECONDS if fresh else self.LIVE_REDEEM_UNSENT_RETRY_SECONDS) if unsent else self.LIVE_REDEEM_RETRY_SECONDS
                text = core.clean_error(error) or type(error).__name__
                self.live_redeem_failed[key] = time.monotonic() - self.LIVE_REDEEM_RETRY_SECONDS + wait
                errors[key] = text
                if isinstance(error, TxPending):
                    held = text  # (RDM-ROUND-LOCK-STALL) sent, no receipt in time: nothing more sent this round
                retry = self.live_redeem_retry_words(wait) if unsent else ""  # (RDM-UNSENT-RETRY-GRID) 1～11 / 5～15 分钟后的下一轮
                if unsent:
                    unsent_runs[key] = runs = unsent_runs.get(key, 0) + 1
                    if runs <= self.LIVE_REDEEM_UNSENT_NAMED:
                        first[mid] = time.monotonic()  # (RDM-SOON-NO-TARGET) still named: the retry after the wait reads it first
                else:
                    unsent_runs.pop(key, None)
                self.live_redeem_note(key, f"领取 {entry['title']} 失败：{text}" + (f"（交易没发出，{retry}再试）" if unsent else "（一小时后再试）"))
                seen.append({**entry, "kind": "failed", "error": text, "unsent": unsent, "retry": retry})
                if not unsent:  # (RDM-NOTE-THROTTLE) sent and failed — its gas spent, its hour's wait: told at the next notice
                    untold[key] = {"title": entry["title"], "error": text, "unsent": False}
                continue
            errors.pop(key, None)
            unsent_runs.pop(key, None)
            untold.pop(key, None)  # (RDM-NOTE-THROTTLE) redeemed: nothing left to tell or list for it
            notes.pop(key, None)
            seen.append({**entry, **self.live_redeem_booked(key, str(tx), getattr(tx, "payout", None), amount, entry["title"],
                                                            str(outcome.get("name") or ""), now_ms, done, sent_key, getattr(tx, "fee", None))})
        if dry_run:
            return done, seen
        for gone in set(read_round) - unsettled:
            del read_round[gone]  # every position on it settled here (redeemed, lost) before this round, or no longer held: forgotten
        for gone in [k for k in unsent_runs if k.split(":")[2] not in unsettled]:  # (key: live:redeemed:<market>:<index set>)
            del unsent_runs[gone]
        for gone in set(first) - unsettled:
            del first[gone]  # a name with nothing left to judge there (redeemed, lost, or not listed): forgotten
        left_small = {f"live:redeemed:{s['market_id']}:{s['index_set']}" for s in seen if s["kind"] == "small"}
        for gone in set(small) - left_small:
            del small[gone]  # (GAS-MIN-VALUE) over the floor now, redeemed, or no longer listed: judged afresh if it comes back
        # (RDM-NOTE-THROTTLE) a problem line goes once its cause has: a position no longer listed (its own line), the gas, the
        # cut list, the unreadable settlements, a failed round (this one looked); a failure not yet told, once no longer listed
        listed = {f"live:redeemed:{s['market_id']}:{s['index_set']}" for s in seen}
        found = {s["kind"] for s in seen}
        over = ({"round", "loop"} | ({"gas"} if not low_gas else set()) | ({"cut"} if not cut else set())
                | ({"unclear"} if "unclear" not in found else set()) | ({"fields"} if "fields" not in found else set()))
        for gone in [k for k in notes if k in over or (k.startswith("live:redeemed:") and k not in listed)]:
            del notes[gone]
        for gone in [k for k in untold if k not in listed]:
            del untold[gone]
        claimable = [s for s in seen if s["kind"] == "claimable"]
        if low_gas and claimable:
            short = f"签名钱包的 BNB 不够付领取交易的 gas（节点拒收：{gas_refused}）" if gas_refused else "签名钱包的 BNB 不足 0.0005"  # (RDM-UNSENT-AS-SENT)
            mined, spent, _ = self.live_redeem_gas_day(now_ms)  # (GAS-STATS) what one redeem costs, when the last day's receipts say
            text = (f"⛽ {short}，{len(claimable)} 个已结算的持仓（约 ${sum(s['value'] for s in claimable):,.2f}）等着领取：暂不自动领取"
                    + (f"（近 24 小时每笔领取平均 gas {bnb_text(spent // mined)} BNB）" if mined else "") + "。"
                    f"往 {self.live.wallet.signer} 转一点 BNB（BNB 链）后自动恢复")
            # 1.150.0 (audit RDM-NOTIFY-GATES): the hour was started before the notice was asked for — with LIVE_NOTIFY off, no chat
            # subscribed, the chat /pause'd or its one sendMessage refused, nothing reached the owner and the next ⛽ came an hour
            # on. The line in /live (live_errors) keeps this hourly clock; the notice is asked for every such round and keeps its
            # own hour (LIVE_NOTE_EVERY), started only once a chat was given it, given back when no chat took it.
            if time.monotonic() - self.live_gas_noted >= self.LIVE_REDEEM_RETRY_SECONDS:
                self.live_gas_noted = time.monotonic()
                self.live_redeem_note("gas", text)
            self.live_notify(text, key="redeem:gas")  # 1.131.4: told on Telegram, not only in the log
        # 1.150.0 (audit RDM-NOTE-THROTTLE): one hourly clock for every failed redeem, and the notice named the first alone (its
        # title, error and retry time): a harmless never-sent failure took the hour, and a redeem SENT and failed minutes later
        # — its gas spent, an hour's wait — was never pushed; two failures in one round were told with the first one's retry.
        # A sent-and-failed redeem is now told at the next notice (redeem:fail: one every LIVE_NOTE_SECONDS at most); the
        # never-sent ones, routine for windows redeemed seconds after their result, keep their hour; each notice names up to
        # three, each with its own retry.
        soft = [s for s in seen if s["kind"] == "failed" and s.get("unsent")]
        if untold or (soft and time.monotonic() - getattr(self, "live_redeem_noted", -1e9) >= self.LIVE_REDEEM_NOTE_SECONDS):
            items, auto = [*untold.values(), *soft], self.config.live_auto_redeem
            when = lambda s: (s.get("retry") or "一小时后") + "自动再试"  # (RDM-UNSENT-RETRY-GRID) never left: 1～11 / 5～15 分钟后的下一轮
            manual = "自动领取关着，不会自己再试：发 /live redeem 再领"  # (RDM-CHECK-VERDICT)
            if len(items) == 1:
                text = f"⚠️ 自动领取：1 个已结算的持仓没领成（{items[0]['title']}：{items[0]['error']}），{when(items[0]) if auto else manual}"
            else:
                text = (f"⚠️ 自动领取：{len(items)} 个已结算的持仓没领成（"
                        + "；".join(f"{s['title']}：{s['error']}" + (f"，{when(s)}" if auto else "") for s in items[:3])
                        + (f"；另有 {len(items) - 3} 个" if len(items) > 3 else "") + "）" + ("" if auto else f"，{manual}"))
            if self.live_notify(text + "。/live redeem check 查看原因", key="redeem:fail") is not False:
                # (RDM-NOTIFY-GATES) told once a chat was given it — and all given back when none took it (False: no chat given
                # it; a stand-in that answers nothing counts as told, as before)
                carried, before = dict(untold), getattr(self, "live_redeem_noted", -1e9)
                untold.clear()
                if soft:
                    self.live_redeem_noted = time.monotonic()

                def undo(carried: dict = carried, before: float = before, soft: bool = bool(soft)) -> None:
                    for k, v in carried.items():
                        untold.setdefault(k, v)
                    if soft:
                        self.live_redeem_noted = before
                self.live_note_given_back("redeem:fail", undo)
        # 1.150.0 (audit RDM-FIELDS-SILENT): a position the bot can never redeem — no indexSet on the row, no conditionId on a
        # resolved winner's market, a row or a market it cannot read (缺字段) — was never pushed and never written to /live's
        # 最近错误: it was looked at every round for good in silence, the money left to the owner's luck. Each such position is
        # told once (this process), a newcomer at the next hourly gate; the line in /live once an hour while one is new. Nothing
        # else changes for it: no mark (a field may come later), no back-off, no transaction.
        fields = [s for s in seen if s["kind"] == "fields"]
        told = self.__dict__.setdefault("live_redeem_fields_told", set())
        told &= {(s["market_id"], s["index_set"]) for s in fields}  # (one no longer 缺字段: forgotten)
        fresh = [s for s in fields if (s["market_id"], s["index_set"]) not in told]
        if fresh:
            text = (f"⚠️ 自动领取：{len(fields)} 个持仓缺字段，机器人领不了（{fresh[0]['title']}：{fresh[0]['error']}）："
                    "到 Predict 网站看一下，结算赢了的手动领取；/live redeem check 查看，并把它的回答发我")
            if time.monotonic() - getattr(self, "live_redeem_fields_noted", -1e9) >= self.LIVE_REDEEM_NOTE_SECONDS:
                self.live_redeem_fields_noted = time.monotonic()
                self.live_redeem_note("fields", text)
            if self.live_notify(text, key="redeem:fields") is not False:  # (hourly: LIVE_NOTE_EVERY; False: no chat given it)
                new = {(s["market_id"], s["index_set"]) for s in fresh}
                told |= new
                self.live_note_given_back("redeem:fields", lambda new=new: told.difference_update(new))
        if cut and time.monotonic() - getattr(self, "live_cut_noted", -1e9) >= self.LIVE_REDEEM_NOTE_SECONDS:
            # 1.150.0 (RDM-POSITIONS-CUT): a list longer than even the redeem's own read — told once an hour on its own timer (a bare
            # key would come back every LIVE_NOTE_SECONDS, a round or two), not only in the summary nobody asks for
            self.live_cut_noted = time.monotonic()
            text = (f"⚠️ 自动领取：Predict 的持仓列表超过 {cut} 条，只读到前 {cut} 条，后面的持仓没检查（可能有赢的没领）："
                    "先到 Predict 网站核对、手动领取。/live redeem check 查看")
            self.live_redeem_note("cut", text)
            self.live_notify(text, key="redeem:cut")
        # (RDM-OPEN-SHAPES) a settlement read half: remembered from the round that first met it while it stays so (or unread), told
        # once it has stood LIVE_REDEEM_UNCLEAR_SECONDS — twelve times that for a named result without the settled word, which a
        # proposal still in its dispute window looks like — an hour apart on its own timer. Nothing is sent or written for it.
        unclear_since = self.__dict__.setdefault("live_redeem_unclear", {})
        kinds = {f"{s['market_id']}:{s['index_set']}": s["kind"] for s in seen}
        for gone in [k for k in unclear_since if kinds.get(k) not in ("unclear", "unread")]:
            del unclear_since[gone]
        stood = [s for s in seen if s["kind"] == "unclear"
                 and time.monotonic() - unclear_since.setdefault(f"{s['market_id']}:{s['index_set']}", time.monotonic())
                 >= self.LIVE_REDEEM_UNCLEAR_SECONDS * (1 if s.get("settled") else 12)]
        if stood and time.monotonic() - getattr(self, "live_unclear_noted", -1e9) >= self.LIVE_REDEEM_NOTE_SECONDS:
            self.live_unclear_noted = time.monotonic()
            s = stood[0]
            text = (f"⚠️ 自动领取：{len(stood)} 个持仓 Predict 的结算读不出（{s['title']}：{s['error']}，市场状态 {s['status']}），"
                    "机器人不会去领、也不记成输：先到 Predict 网站手动领取，并把 /live redeem check 的回答发我")
            self.live_redeem_note("unclear", text)
            self.live_notify(text, key="redeem:unclear")
        counts: dict[str, int] = {}
        for s in seen:
            counts[s["kind"]] = counts.get(s["kind"], 0) + 1
        self.live_redeem_last = {"at": now_ms, "counts": counts, "low_gas": low_gas, "error": "", "cut": cut, "limited": limited, "held": held,
                                 "gas_refused": gas_refused, "gas_unread": gas_unread}
        if not limited:
            self.live_redeem_whole_at = time.monotonic()  # (RDM-ROUND-VOID) a look no rate limit cut short: live_redeem_again's notice waits again
        return done, seen

    @classmethod
    def live_redeem_summary(cls, last: dict) -> str:
        """A round's findings in a line: the kinds of position it met, counted (or why it could not look)."""
        if not last:
            return "还没跑过"
        if last.get("error"):
            return str(last["error"])
        counts = last.get("counts") or {}
        text = "，".join(f"{label} {counts[k]}" for k, label in cls.REDEEM_KINDS.items() if counts.get(k))
        cut = int(last.get("cut") or 0)  # 1.150.0 (RDM-POSITIONS-CUT): the counts are of the part read — said, so 输掉的 N reads for what it is
        return (f"持仓 {sum(counts.values())} 个" + (f"：{text}" if text else "")
                + (("；签名钱包的 BNB 不够付领取交易的 gas（节点拒收），领取交易发不出去" if last.get("gas_refused")  # (RDM-UNSENT-AS-SENT)
                    else "；签名钱包 BNB 不足，领取交易发不出去") if last.get("low_gas") and counts.get("claimable") else "")
                + ("；一笔领取交易没在时限内上链，这一轮其余的先不发" if last.get("held") and counts.get("claimable") else "")  # (RDM-ROUND-LOCK-STALL)
                + (f"；{last['gas_unread']}，这一轮先不发" if last.get("gas_unread") and counts.get("claimable") else "")  # (RDM-GAS-STALE)
                + (f"；⚠️ 持仓列表超过 {cut} 条，只读到前 {cut} 条，其余没检查" if cut else ""))

    def live_redeem_status_line(self) -> str:
        """/live: the automatic redeem in a line — on or off, the last round, whether one is running."""
        if not self.config.live_auto_redeem:
            return "自动领取：关（/live redeem 手动领取）"
        last = getattr(self, "live_redeem_last", {}) or {}
        task = getattr(self, "live_redeem_task", None)
        minutes = lambda at: max(0, int((self.market.now_ms() - int(at)) / 60_000))
        ago = f"上一轮 {minutes(last['at'])} 分钟前：{self.live_redeem_summary(last)}" if last.get("at") else ""
        if ago and last.get("manual"):
            # 1.150.0 (audit RDM-STATUS-LINE): every non-dry pass writes live_redeem_last, the owner's /live redeem (and the page's
            # 领取已结算) as well, and the line said 上一轮 0 分钟前 of it as of an automatic round — with the automatic rounds
            # stopped, each manual one made them look alive. Now said as the owner's, beside the last automatic round's time
            auto_at = int(getattr(self, "live_redeem_auto_at", 0) or 0)
            ago = (f"上一轮（手动）{minutes(last['at'])} 分钟前：{self.live_redeem_summary(last)}"
                   + (f"｜自动的上一轮 {minutes(auto_at)} 分钟前" if auto_at else "｜自动的本次启动后还没跑过"))
        if shut := self.live_redeem_shut():
            # 1.150.0 (audit RDM-OFF-NO-SIGNIN): a process started in LIVE=off does not sign in to Predict by itself (as designed:
            # off 只记账), so live_redeem_tick's gate stays shut and no automatic round runs for the positions still held — a
            # restart after /live mode off (every deploy) leaves the winners unclaimed — while this line said 自动领取：开｜本次启动后
            # 还没跑过 for as long as the process lived. It says now that nothing runs, and why (the gate's own words); nothing
            # in what the bot does changes (a sign-in in off would read the account every minute and sweep its unrecorded
            # orders). /live redeem claims them (it signs in: the rounds then run, until the next restart)
            return f"自动领取：开，但 {shut}，不会自动领取（/live redeem 手动领取）" + (f"｜{ago}" if ago else "")
        when = ago or "本次启动后还没跑过"
        since = int(getattr(self, "live_redeem_since", 0) or 0)  # 1.150.0 (RDM-ROUND-LOCK-STALL): a long round is told from a fresh one
        running = (f"｜正在领取（{core.stamp(since, seconds=False)} 起）" if since
                   else "｜正在领取" if task is not None and not task.done() else "")
        return f"自动领取：开｜{when}" + running + "｜/live redeem check 查原因"

    async def live_redeem_report(self, now_ms: int) -> str:
        """/live redeem check, read-only (nothing sent, written or announced): the switches, the signing wallet's gas,
        the last round, the account's positions by what the redeem makes of each (a dry run of live_redeem_pass), a
        conclusion, the recent redeem problems (1.131.4)."""
        c, wallet = self.config, self.live.wallet
        shut = self.live_redeem_shut()  # (before the sign-in below: the state the check found)
        if not self.live.ready:
            await self.live_ensure_ready()
        ready = self.live.ready
        lines = [f"🔎 自动领取自检 {core.stamp(now_ms, seconds=False)}",
                 f"开关：自动领取 {'开' if c.live_auto_redeem else '关'}｜LIVE={c.live_mode}｜" + ("已就绪" if ready else f"未就绪：{self.live.not_ready_why}"),
                 self.live_redeem_status_line()]
        if c.live_auto_redeem and shut and not self.live_redeem_shut():
            # 1.150.0 (audit RDM-OFF-NO-SIGNIN): the check signs in when not ready, and in LIVE=off that is what opens the automatic
            # rounds' gate — it said 已就绪 … 下一轮自动领取会领 of the state after its own sign-in, never that until then nothing
            # had run (a start in off does not sign in): said, and that it lasts until the next restart
            lines.append(f"{shut}：这次自检之前自动领取没在跑；自检已登录 Predict，从现在到下次重启自动领取照常（LIVE=off 启动时不登录）")
        try:
            bnb = await self.live.chain.bnb_balance()
        except Exception as error:
            bnb = None
            lines.append(f"签名钱包 {wallet.signer}（付 gas）：BNB 读取失败：{core.clean_error(error) or type(error).__name__}")
        else:
            lines.append(f"签名钱包 {wallet.signer}（付 gas）：BNB {from_wei(bnb):.5f}"
                         + ("｜⛽ 不足 0.0005，领取交易发不出去" if bnb < self.LIVE_REDEEM_MIN_BNB_WEI else "｜够付 gas"))
        mined, spent, reverted = self.live_redeem_gas_day(now_ms)
        if mined:
            # 1.150.0 (GAS-STATS): what a day of redeems costs and how far the signer's BNB goes — the receipts' own figures
            # (REDEEM_GAS: no request here); no line before the first redeem with a fee on record
            each = spent // mined
            lines.append(f"近 24 小时领取 {mined} 笔" + (f"（其中 {reverted} 笔回滚，gas 照付）" if reverted else "")
                         + f"，gas 共 {bnb_text(spent)} BNB（每笔平均 {bnb_text(each)} BNB）"
                         + (f"；签名钱包 BNB 余额约够 {bnb // each} 笔" if bnb is not None and each > 0 else ""))
        if wallet.predict_account:
            lines.append(f"持仓账户 {wallet.maker}（Predict 账户：领取由签名钱包经它的 Kernel 合约执行，领到的 USDT 进这个账户）")
        if not ready:
            lines.append("结论：真实交易未就绪，读不了持仓，也领不了：" + self.live.not_ready_why)
            return "\n".join(lines)
        # 1.150.0 (audit RDM-ROUND-LOCK-STALL): the dry run waited for the redeem lock with no bound, so while a round redeemed —
        # each winner up to 120 s for its receipt — the check answered only when that round ended: half an hour and more with a
        # node that mines nothing, exactly when the owner needs it. It waits REDEEM_CHECK_WAIT_SECONDS at most (a healthy round
        # of a dozen winners ends within a minute or so), then answers with what it has, without the dry run. Never beside the
        # round: a second burst of reads on the key the orders share, and positions that round is redeeming called 可领取.
        # What it says the round does covers both of its phases: its own read of a long positions list (up to LIVE_REDEEM_PAGES
        # pages, RDM-POSITIONS-CUT) and its market reads hold the lock too, before anything is sent — 「它把领取交易逐个发出」
        # alone was said of a round still reading.
        lock = self.redeem_lock()
        deadline = time.monotonic() + self.REDEEM_CHECK_WAIT_SECONDS
        while lock.locked() and time.monotonic() < deadline:
            await asyncio.sleep(1)
        if lock.locked():  # (no await between this test and the acquire below)
            since = int(getattr(self, "live_redeem_since", 0) or 0)
            started = f"（{core.stamp(since, seconds=False)} 起，已 {max(0, int((self.market.now_ms() - since) / 60_000))} 分钟）" if since else ""
            lines.append(f"结论：有一轮领取正在进行{started}：它要读完持仓（持仓多时要读很多页）和市场，再把领取交易逐个发出、"
                         "每个等上链（最多 120 秒）后才结束；这次没有逐个查看持仓，等 /live 不再显示“正在领取”再发 /live redeem check")
            recent = list(getattr(self, "live_redeem_notes", {}).values())[-3:]  # (RDM-NOTE-THROTTLE) the redeem's own list
            if recent:
                lines.append("最近的领取问题：\n" + "\n".join("  " + e for e in recent))
            return "\n".join(lines)
        recent = list(getattr(self, "live_redeem_notes", {}).values())[-3:]  # (RDM-NOTE-THROTTLE) the redeem's own list, not live_errors
        try:
            async with lock:
                _, seen = await self.live_redeem_pass(now_ms, dry_run=True)
                cut = int(getattr(self, "live_redeem_cut", 0) or 0)  # this dry run's own read (1.150.0): set by the pass, under the same lock
        except Exception as error:
            # 1.150.0 (audit RDM-CHECK-ABORT): a dry run that raised — the positions list not readable (the key's 20 s rate-limit
            # cooldown, which the pre-open mode's orders and cancels share; a 5xx; a timeout), or any failure inside the pass — went
            # out of this function, and the check answered one line, 「/live redeem 失败：…」: the switches, the last round and the
            # signer's BNB gathered above (⛽ 不足 is read on the chain, nothing asked of Predict) were thrown away, and the line read
            # like a failed redeem, though the check sends nothing. It answers now with what it has and why it could not look.
            # Read-only as ever: nothing written, nothing announced; the lock is released before this (the try is outside it).
            text = core.clean_error(error) or type(error).__name__
            limited = bool(getattr(error, "retry_after", 0) or http_status(error) == 429)
            why = (f"这次读不了持仓：{text}。" if isinstance(error, RedeemUnread) else f"这次查看持仓时出错：{text}。") + (
                "限流冷却过了再发一次 /live redeem check" if limited else "稍后再发一次 /live redeem check；一直这样就把这条发我")
            if bnb is not None and bnb < self.LIVE_REDEEM_MIN_BNB_WEI:
                why += f"。另外签名钱包的 BNB 不够付 gas，有可领取的也发不出领取交易：往 {wallet.signer} 转一点 BNB（BNB 链，比如 0.005）"
            lines.append("结论：" + why)
            if recent:
                lines.append("最近的领取问题：\n" + "\n".join("  " + e for e in recent))
            return "\n".join(lines)
        n = lambda kind: sum(1 for s in seen if s["kind"] == kind)
        value = sum(float(s.get("value") or 0) for s in seen if s["kind"] == "claimable")
        lines.append(f"持仓 {len(seen)} 个：" + ("，".join(f"{label} {n(k)}" for k, label in self.REDEEM_KINDS.items() if n(k)) or "没有")
                     + (f"（⚠️ 持仓列表超过 {cut} 条，只读到前 {cut} 条，其余没检查）" if cut else ""))
        small = [s for s in seen if s["kind"] == "small"]  # (GAS-MIN-VALUE) under the owner's LIVE_REDEEM_MIN_USD: its count and total
        small_value = sum(float(s.get("value") or 0) for s in small)
        floor = float(c.live_redeem_min_usd or 0)
        listed = {"claimable": ("可领取", self.REDEEM_REPORT_LINES),
                  "small": (f"小额暂不领（低于 ${floor:g}，LIVE_REDEEM_MIN_USD；{len(small)} 个共约 ${small_value:,.2f}）", self.REDEEM_REPORT_LINES),
                  "waiting": ("领取失败、等重试", self.REDEEM_REPORT_LINES),
                  "sent": ("领取交易已发出、还没确认上链（下一轮先查回执，不会再发）", self.REDEEM_REPORT_LINES),
                  "fields": ("缺字段", self.REDEEM_REPORT_LINES), "unclear": ("Predict 的结算读不出（机器人不会去领）", self.REDEEM_REPORT_LINES),
                  "kept": ("已发过领取交易、Predict 还列着", 3), "open": ("未结算", 3), "unread": ("这次没读到市场", 2),
                  "lost": ("输掉的（不再读市场；判断错了可发 /live redeem forget <市场号> 重新判断）", 3)}  # (RDM-MARKS-PERMANENT)
        # (RDM-MARKS-PERMANENT) a mark's age, the market id and the whole hash — the 14 characters the check printed could not be
        # looked up, and a row still listed hours after its redeem read like one listed a minute after it
        marked = lambda s: f"市场 {s.get('market_id')}，{core.age_text((now_ms - int(s.get('marked_at') or now_ms)) / 1000)}"
        for kind, (label, cap) in listed.items():
            items = [s for s in seen if s["kind"] == kind]
            if not items:
                continue
            lines.append(f"{label}：")
            for s in items[:cap]:
                extra = {"claimable": f"，约 ${float(s.get('value') or 0):,.2f}"  # (RDM-CHECK-FORGETS-FAILURE) a failure remembered past its back-off
                                      + (f"｜上次领取失败：{s['error']}（{'下一轮自动再试' if c.live_auto_redeem else '自动领取关着，发 /live redeem 再试'}）"
                                         if s.get("error") else ""),
                         "small": f"，约 ${float(s.get('value') or 0):,.2f}",
                         "waiting": f"：{s.get('error') or '?'}（" + (f"{max(1, int(float(s.get('left') or 0) / 60))} 分钟后再试）" if c.live_auto_redeem
                                                                       else "自动领取关着：发 /live redeem 再试）"),  # (RDM-CHECK-VERDICT)
                         "sent": f"（交易 {s.get('tx')}，{int(s.get('age_ms') or 0) // 60_000} 分钟前发出" + (f"；{s['error']}" if s.get("error") else "") + "）",
                         "fields": f"：{s.get('error')}", "unclear": f"：{s.get('error')}（市场状态 {s.get('status')}）",
                         "kept": f"（{marked(s)}领取，交易 {s.get('tx') or '?'}" + ("，没领到 USDT" if s.get("paid") == "0" else "") + "）",  # (RDM-PAYOUT-UNCHECKED)
                         "open": f"（市场状态 {s.get('status')}）", "unread": f"：{s.get('error')}", "lost": f"（{marked(s)}判定）"}[kind]
                lines.append(f"  · {s['title']} / {s['outcome']} {s['shares']:g} 份{extra}")
            if len(items) > cap:
                lines.append(f"  …另有 {len(items) - cap} 个")
        auto = c.live_auto_redeem
        low = bnb is not None and bnb < self.LIVE_REDEEM_MIN_BNB_WEI
        waiting = [s for s in seen if s["kind"] == "waiting"]
        retrying = [s for s in seen if s["kind"] == "claimable" and s.get("error")]  # (RDM-CHECK-FORGETS-FAILURE) failed last time, back-off over
        fresh = [s for s in seen if s["kind"] == "claimable" and not s.get("error")]
        first = []  # the gas first: without it neither the automatic round nor /live redeem can send anything
        refused = str((getattr(self, "live_redeem_last", {}) or {}).get("gas_refused") or "")
        then = "到账后下一轮自动领取" if auto else "到账后发 /live redeem 领取（自动领取关着）"
        if (n("claimable") or waiting) and low:
            first.append(f"签名钱包的 BNB 不够付 gas，领取交易发不出去：往 {wallet.signer} 转一点 BNB（BNB 链，比如 0.005），{then}")
        elif (n("claimable") or waiting) and refused:  # (RDM-UNSENT-AS-SENT) over the floor, yet the node refused the last round's redeem for its gas
            first.append(f"上一轮节点以 gas 不够拒收了领取交易（{refused}）：签名钱包的 BNB 不够付领取交易的 gas，"
                         f"往 {wallet.signer} 转一点 BNB（BNB 链，比如 0.005），{then}")
        if n("claimable") and not auto:
            first.append(f"自动领取关着，{n('claimable')} 个可领取（约 ${value:,.2f}）不会自己领：控制台“高级”里打开“自动领取已结算持仓”，或发 /live redeem 手动领")
        # 1.150.0 (audit RDM-CHECK-VERDICT): the conclusion was ONE reason picked by a fixed order — one row Predict still lists
        # after its redeem, or one five-minute retry, took it from 缺字段 (money the bot can never claim), from the claimable and from
        # the markets not read; and its 自动 promises (到时自动再试, 结算后自动领取…) were made with the switch off. Every kind found
        # now has its clause — the first is the 结论, the others follow as 另外 lines, the most pressing first — each in the words
        # of the switch as it stands. A single kind reads word for word as before.
        reasons = ["；".join(first)] if first else []
        if waiting or retrying:
            reasons.append(f"链上领取失败：{(waiting or retrying)[0].get('error') or '见上'}；"
                           + ("到时自动再试" if auto else "自动领取关着，不会自己再试：发 /live redeem 再试一次") + "，一直失败就把这条发我")
        if n("fields"):  # (RDM-FIELDS-SILENT) a row without indexSet is judged before its market is read: not 已结算 for sure
            reasons.append("有持仓缺字段，机器人领不了（见上，有的还分不出是否结算、输赢）：到 Predict 网站看一下，结算赢了的手动领取，并把这条发我")
        if n("unclear"):  # 1.150.0 (RDM-OPEN-SHAPES): never 结算后自动领取 for half an answer
            reasons.append("有持仓的市场 Predict 的结算读不出（见上），机器人不会去领、也不记成输：可先到 Predict 网站手动领取，并把这条发我")
        if fresh and auto and not first:  # (the gas, the switch: said above, these included)
            reasons.append(f"有 {len(fresh)} 个可领取（约 ${sum(float(s.get('value') or 0) for s in fresh):,.2f}），下一轮自动领取会领；急用就发 /live redeem")
        if n("unread"):
            reasons.append("有市场这次没读到（限流或一轮读满），" + ("下一轮继续看" if auto else "发 /live redeem 接着看（自动领取关着）"))
        if n("sent"):  # (RDM-RECEIPT-POLL, RDM-RESTART-MID-SEND)
            reasons.append("有领取交易已经发出、还没确认上链（见上）：下一轮先查它的回执，上链了就记为已领取，不会再发一次；"
                           "一小时后还没上链、节点里也查不到它，才重新领取")
        if n("kept"):
            # (RDM-MARKS-PERMANENT) by the marks' age: "过 10 分钟还在" was said for ever, of a mark a minute or a week old
            kept = [s for s in seen if s["kind"] == "kept"]
            empty = [s for s in kept if s.get("paid") == "0"]
            old = [int(s.get("marked_at") or now_ms) for s in kept if now_ms - int(s.get("marked_at") or now_ms) >= self.LIVE_REDEEM_KEPT_GRACE_MS]
            if empty:
                reasons.append(f"有 {len(empty)} 个持仓的领取交易上链了，但没领到 USDT（见上）：链上多半已经没有这份持仓（之前领过）；"
                               "Predict 网站上若还显示可领取，到网站上领取，并把这条发我")
            elif old:
                reasons.append(f"有 {len(old)} 个持仓 {core.age_text((now_ms - min(old)) / 1000)}就发过领取交易并上链，Predict 一直还列着："
                               "在 bscscan.com 打开上面的交易哈希，看持仓账户有没有收到 USDT；没收到就到 Predict 网站上领取，并把这条发我")
            else:
                reasons.append("有持仓已经发过领取交易并上链，Predict 还列着：可能只是列表没更新，过半小时再查")
        settled = not reasons  # nothing to say but that the rest is open or lost (or left small by the owner's floor)
        if settled:
            rest = (("剩下的还没结算，结算后" + ("自动领取" if auto else "发 /live redeem 手动领（自动领取关着）")) if n("open")
                    else "其余是输掉的" if n("lost") or not small else "")
            reasons = ["现在没有可领取的：" + ("赢的除小额暂不领的（见下）以外都领完了" if small else "赢的都领完了") + (f"，{rest}" if rest else "")]
        if cut:
            # 1.150.0 (RDM-POSITIONS-CUT): the part read says nothing of the rest — the cut first, and never 赢的都领完了 on it
            head = f"持仓列表超过 {cut} 条，只读到前 {cut} 条，其余没检查：后面可能还有赢的没领，先到 Predict 网站核对、手动领取"
            reasons = [head + "；读到的这部分里没有可领取的"] if settled else [head + "；" + reasons[0], *reasons[1:]]
        if small:
            # 1.150.0 (GAS-MIN-VALUE): left by the owner's own setting — said last, after whatever needs doing, and never under 赢的都领完了
            reasons.append(f"有 {len(small)} 个赢了的持仓价值低于 ${floor:g}（共约 ${small_value:,.2f}），按 LIVE_REDEEM_MIN_USD 暂不领取、不花 gas；"
                           "没有记成已领或输掉：这项调低或清零后" + ("下一轮自动领取就领" if auto else "发 /live redeem 就领（自动领取关着）"))
        lines.append("结论：" + reasons[0])
        lines.extend(f"另外：{r}" for r in reasons[1:])
        if recent:
            lines.append("最近的领取问题：\n" + "\n".join("  " + e for e in recent))
        return "\n".join(lines)

    # --- telling the administrator ------------------------------------------------------------------------------------------
    LIVE_NOTE_BUFFER = 50   # unkeyed notices held per chat for its next batch message; beyond it the oldest are dropped (and counted)
    # 1.150.0 (audit RDM-NOTIFY-GATES): the notices only the owner can act on — send BNB to the signer, a redeem keeps failing —
    # are said whatever LIVE_NOTIFY says (it silences the orders' and fills' notices), and to the administrator's private chat
    # when no chat is subscribed (never /subscribe'd, /pause'd, suspended); each sendMessage tried up to LIVE_NOTE_TRIES times
    LIVE_NOTE_MUST = frozenset({"redeem:gas", "redeem:fail", "redeem:fields"})  # (redeem:fields, RDM-FIELDS-SILENT: a winner the bot can never claim)
    LIVE_NOTE_EVERY = {"redeem:gas": LIVE_REDEEM_NOTE_SECONDS, "redeem:fields": LIVE_REDEEM_NOTE_SECONDS}  # a key's own pace (the others: LIVE_NOTE_SECONDS)
    LIVE_NOTE_TRIES = 3

    def live_notify(self, text: str, key: str = "") -> bool:
        """A line to every active subscription (the administrator's chats); a keyed notice repeats at most every
        LIVE_NOTE_SECONDS (LIVE_NOTE_EVERY: its own pace). True when it was given to at least one chat.

        1.150.0 (audit LIVE-NOTIFY-PRIORITY): the unkeyed notices (📤 each placement, ✅ each fill, 💵, ℹ️ …) are coalesced
        per chat: each one joins that chat's buffer, and one batch delivery per chat sends everything buffered as ONE message, then whatever
        arrived while that message waited in Telegram's per-chat queue (about one message per 1.1 s), until the buffer
        is empty. They used to be one message each: a maker re-quoting every few seconds over a few slots, plus fills,
        outran that rate, the queue grew without bound, and a keyed critical alert (🛑 kill switch, ❓ unknown order,
        ⚠️ …) waited behind every notice already queued — minutes late. Now a critical alert waits behind at most one
        live batch message. Keyed notices keep their own delivery each, as before.

        1.150.0 (audit RDM-NOTIFY-GATES): the ⛽ no-gas and ⚠️ failed-redeem notices vanished with LIVE_NOTIFY=off, after /pause,
        without /subscribe, or on one refused sendMessage — and their hour was started anyway, before the notice was even asked
        for: the owner, who alone can fund the signer, heard nothing, and an hour late once the gate opened. A LIVE_NOTE_MUST key
        now passes LIVE_NOTIFY and falls back to the administrator's chat; a keyed notice's clock starts only when a chat was
        given it (False: none — no clock), and a must-notice no chat took after LIVE_NOTE_TRIES sends each gives its clock back
        (and its caller's: live_note_given_back), so the next round says it again. Every other notice as before."""
        must = key in self.LIVE_NOTE_MUST
        if self.telegram is None or not (self.config.live_notify or must):
            return False
        targets = {sub_id: sub for sub_id, sub in self.subscriptions().items() if sub.get("active")}
        if not targets and must and self.config.admin_id:  # (as suspend_target's notice: the administrator's private chat)
            targets = {core.subscription_key(self.config.admin_id, 0): {"chat": self.config.admin_id, "thread": 0}}
        if not targets or (key and self.stopping.is_set()):
            return False  # nobody given it: a keyed notice's clock is not started
        stamp = 0.0
        if key:
            if time.monotonic() - self.live_noted.get(key, -1e9) < self.LIVE_NOTE_EVERY.get(key, self.LIVE_NOTE_SECONDS):
                return False
            self.live_noted[key] = stamp = time.monotonic()
        told: list[bool] = []  # a must-notice: what each chat's sends came to
        queued = False
        for sub_id, sub in targets.items():
            if not key:
                buffer = self.live_note_buffer.setdefault(sub_id, [])
                buffer.append(text)
                if len(buffer) > self.LIVE_NOTE_BUFFER:
                    over = len(buffer) - self.LIVE_NOTE_BUFFER
                    del buffer[:over]
                    self.live_note_dropped[sub_id] = self.live_note_dropped.get(sub_id, 0) + over
                # one batch per chat at a time: while it is in flight this does nothing, and the batch takes this line too
                async def batch(sub_id: str = sub_id, sub: dict = sub) -> None:
                    await self.live_note_batch(sub_id, sub)
                self.deliver(f"live:batch:{sub_id}", batch)
                queued = True
                continue
            self.live_seq += 1

            async def send(sub: dict = sub, text: str = text) -> None:
                ok = await self.tell(sub["chat"], sub["thread"], text)
                for _ in range(self.LIVE_NOTE_TRIES - 1 if must else 0):  # (the sender waits out a 429's cool-down for that chat itself)
                    if ok or self.stopping.is_set():
                        break
                    ok = await self.tell(sub["chat"], sub["thread"], text)
                if must:
                    told.append(ok)
                    if len(told) == len(targets):  # every chat's sends have ended
                        undo = self.__dict__.setdefault("live_note_undo", {}).pop((key, stamp), None)
                        if not any(told):  # no chat has it: its clock given back — the next round says it, not an hour on
                            if self.live_noted.get(key) == stamp:
                                self.live_noted.pop(key, None)
                            if undo is not None:
                                undo()
            queued = self.deliver(f"live:{self.live_seq}:{sub_id}", send) or queued
        if key and not queued and self.live_noted.get(key) == stamp:
            self.live_noted.pop(key, None)
        return queued

    def live_note_given_back(self, key: str, undo: Any) -> None:
        """For the must-notice (LIVE_NOTE_MUST) just given to the chats under ``key`` (live_notify said True): ``undo`` runs when no
        chat took it — whatever its caller wrote down for it (its own clock) given back with the notice's (1.150.0, RDM-NOTIFY-GATES).
        Run once at most; dropped once a chat took it."""
        stamp = self.live_noted.get(key)
        if stamp is not None:
            self.__dict__.setdefault("live_note_undo", {})[(key, stamp)] = undo

    async def live_note_batch(self, sub_id: str, sub: dict) -> None:
        """live_notify's batch delivery for one chat: send the buffered notices as one message, and again for those
        that arrived while it waited in the send queue, until none are left (no await between the last check and the
        end, so a notice is never left behind in the buffer without a batch on its way)."""
        while lines := self.live_note_buffer.pop(sub_id, []):
            dropped = self.live_note_dropped.pop(sub_id, 0)
            if dropped:
                lines = [f"（另有 {dropped} 条较早的真实交易通知未单独发送）", *lines]
            await self.tell(sub["chat"], sub["thread"], "\n\n".join(lines))

    # --- commands and texts -------------------------------------------------------------------------------------------------
    async def cmd_live(self, req: core.Request) -> Any:
        words = [w.lower() for w in req.args]
        action = words[0] if words else ""
        if action in {"check", "redeem", "fills"} and self.reference_tasks:  # 1.131.1: reads or chain calls for seconds: answered in the background, the commands behind it not held
            async def later() -> None:
                try:
                    answer = await self.live_action(action, req.args[1:], self.market.now_ms())
                except Exception as error:
                    answer = f"/live {action} 失败：{core.clean_error(error) or type(error).__name__}"
                text = answer if isinstance(answer, str) else getattr(answer, "text", str(answer))

                async def send() -> None:
                    # 1.150.0 (audit RDM-ANSWER-LOST): the answer promised by 「…结果随后发来」 went out once — a 429 or a network blip on
                    # that one message, and the owner, in the middle of finding why nothing is redeemed, got the placeholder alone
                    # (the trace only in /status). Sent once more; the sender waits out the chat's cool-down first. Plain text as before
                    if not await self.tell(req.chat, req.thread, text) and not self.stopping.is_set():
                        await self.tell(req.chat, req.thread, text)
                await self.background("reply", send)
            asyncio.create_task(later(), name=f"live-{action}")
            if action == "redeem" and words[1:2] == ["check"]:
                return "领取自检中，结果随后发来"
            if action == "redeem" and words[1:2] == ["forget"]:  # (RDM-MARKS-PERMANENT) it redeems nothing: not 领取中
                return "清除记录中，结果随后发来"
            return {"check": "自检中，结果随后发来", "redeem": "领取中，结果随后发来", "fills": "持仓核对中，结果随后发来"}[action]
        return await self.live_action(action, req.args[1:], self.market.now_ms())

    async def live_action(self, action: str, args: list[str], now_ms: int) -> Any:
        """/live and the control page share these: "" = the status card (a Reply), else the action's answer text."""
        words = [a.lower() for a in args]
        if action == "pause":
            why = " ".join(a for a in args if a).strip() or "手动暂停"
            self.store.put("live:paused", {"at": now_ms, "why": why})
            return f"⏸ 真实交易已暂停（{why}）：不再开新仓；已有挂单保留，/live cancel all 可撤。/live resume 恢复。"
        if action == "resume":
            self.store.delete_keys(["live:paused"])
            if getattr(self, "preopen", None) is not None:
                self.preopen.manual_pause = self.preopen.ledger.pause(False)
            lifted = ""
            record = self.live_kill_record()
            if record and not record.get("resumed"):
                # 1.150.0: today's loss stop lifted. The day's settled P&L as the resume finds it is kept with the record: the stop
                # is armed again a cap's worth under it (live_loss_floor), where it used to be gone until midnight whatever was lost
                # next. Only a standing stop is lifted — a resume that merely ends a pause later in the day moves nothing, or every
                # pause and resume would hand out another cap. And the answer says what was lifted: the same command (the page's
                # 恢复 button) ends a pause, and an operator ending one had no word that the day's loss stop went with it.
                pnl = self.live_daily_pnl(self.sim_trades(), now_ms) + self.preopen_pnl()
                self.store.put("live:killed", {**record, "resumed": True, "resumed_at": now_ms, "resumed_pnl": pnl})
                self.live_noted.pop("kill", None)  # the next stop is announced however soon it comes (a keyed notice waits LIVE_NOTE_SECONDS)
                cap = self.config.live_max_daily_loss_usd
                lifted = f"；同时解除了今日亏损停机：今日已结算 {money(pnl)}，" + (
                    f"再亏到 {money(self.live_loss_floor(cap))} 会再次停机" if cap else "现在不设亏损上限（LIVE_MAX_DAILY_LOSS_USD=0）")
                held = getattr(self.preopen, "withdrawing", None) if self.preopen_active() else None
                if held is not None and (why := held()):  # its own loss check is not lifted by this command (PREOPEN.md): said, not changed
                    lifted += f"。开盘前预挂模式仍不挂单：{why}"
            return "▶️ 真实交易已恢复：满足条件的建议会重新下单" + lifted + "。"
        if action == "cancel":
            return await self.live_cancel_command(words, now_ms)
        if action == "release":
            return self.live_release_command(words, now_ms)
        if action == "test":
            return await self.live_test(args, now_ms)
        if action == "mode":
            return await self.live_set_mode(args[0] if args else "")
        if action == "redeem":
            if words[:1] == ["check"]:  # 1.131.4: why the resolved positions are (not) redeemed, read-only
                return await self.live_redeem_report(now_ms)
            if words[:1] == ["forget"]:  # 1.150.0 (RDM-MARKS-PERMANENT): a lost mark lifted by the owner; nothing sent
                return await self.live_redeem_forget(words[1:])
            if not self.live.ready:
                await self.live_ensure_ready()
                if not self.live.ready:
                    return f"未就绪：{self.live.not_ready_why}"
            lines = await self.live_redeem(now_ms, manual=True)  # (RDM-MANUAL-BACKOFF) a position in a failed redeem's back-off: tried once now
            last = self.live_redeem_last
            counts = last.get("counts") or {}
            # 1.150.0 (audit RDM-MANUAL-BACKOFF): the head said 没有可领取的已结算持仓 unless the gas held winners back — also for a
            # failed redeem, a 缺字段 or 结算读不出 winner (none of them claimed) and a market not read; and a reply with a 💵 line
            # was those lines alone, the failures beside them unsaid. Now: 这次没有领取 when something claimable was not claimed,
            # 读到的持仓里没有可领取的 when part was not looked at, and the rest of the round after the 💵 lines
            missed = {k: counts[k] for k in ("claimable", "failed", "waiting", "fields", "unclear") if counts.get(k)}
            if lines:
                if missed or counts.get("unread") or last.get("cut"):
                    rest = sum(missed.values()) + int(counts.get("unread") or 0)
                    lines.append((f"另有 {rest} 个这次没领：" if rest else "") + f"{self.live_redeem_summary(last)}。/live redeem check 看原因")
                return "\n".join(lines)
            head = ("这次没有领取" if missed
                    else "读到的持仓里没有可领取的" if counts.get("unread") or last.get("cut") else "没有可领取的已结算持仓")  # 1.150.0: a cut list is no all-clear
            return f"{head}：{self.live_redeem_summary(last)}。/live redeem check 看原因"
        if action == "check":
            lines = await self.live_prepare()
            return "🔎 真实交易自检\n" + "\n".join(lines) + ("\n最近错误：\n" + "\n".join(self.live_errors[-5:]) if self.live_errors else "")
        if action == "fills":  # 1.130.2: the account's positions against the records, outcome by outcome
            return await self.live_fills_report(now_ms)
        if action == "orders":
            if not self.live.ready:
                return f"未就绪：{self.live.not_ready_why}"
            rows = await self.live.api.orders("OPEN")
            return "Predict 上的开放订单：\n" + ("\n".join(core.brief_error(json.dumps(r, ensure_ascii=False), 300) for r in rows[:20]) or "无")
        if action == "positions":
            if not self.live.ready:
                return f"未就绪：{self.live.not_ready_why}"
            rows = await self.live.api.positions()
            self.live_positions = (rows, time.time())
            if words and words[0] == "raw":  # 1.119.3: the rows as Predict sends them (which fields carry a price and a time)
                return "Predict 持仓原始记录（前 10 条）：\n" + ("\n".join(core.brief_error(json.dumps(r, ensure_ascii=False), 600) for r in rows[:10]) or "无")
            return self.positions_text(rows, now_ms)
        return core.Reply(self.live_text(now_ms), html=True)

    # --- the control page --------------------------------------------------------------------------------------------------
    def apply_config(self, config: core.Config) -> None:
        before = self.config.live_mode
        super().apply_config(config)
        self.live.config = config
        if before != config.live_mode:
            self.live_mode_changed(before, config.live_mode)
        with contextlib.suppress(RuntimeError):  # no loop yet (the start-up apply): run() applies them
            # the pre-open mode follows PREOPEN_* as saved on the control page: started, stopped, or restarted on new settings
            asyncio.get_running_loop().create_task(self.preopen_apply(), name="preopen-apply")

    def live_mode_changed(self, before: str, after: str) -> None:
        """off → pause / on: sign in at the next step; → off: the resting real orders are withdrawn (the positions
        stay, followed to settlement), pending ones are not sent."""
        now_ms = self.market.now_ms()
        if after == "off":
            trades = self.sim_trades()
            for tid, t in trades.items():
                st = t.get("live")
                if isinstance(st, dict) and t["status"] == "resting" and st.get("state") in {"open", "placing", "unknown"}:
                    self.sim_withdraw(t, "切换到 LIVE=off", now_ms)
                    self.sim_save([(tid, t)])
            # 1.150.0 (audit RDM-OFF-NO-SIGNIN): ...and what a restart in off then does — no sign-in, so no automatic redeem for the
            # positions still held (they used to be "followed to settlement" here with no word of it)
            self.live_notify(f"⚪ 真实交易模式 {before} → off：回到只记账，已请求撤掉未成交的真实挂单（以 Predict 的确认为准），已有持仓继续跟踪到结算；"
                             "但重启后（每次部署都会重启）LIVE=off 不登录 Predict、不自动领取，赢的持仓发 /live redeem 领。")
        else:
            self.live_last_signin = -1e9
            if before == "off":  # paper orders resting from LIVE=off are not real: withdrawn, placed again as real ones
                for tid, t in list(self.sim_trades().items()):
                    if t.get("status") == "resting" and not isinstance(t.get("live"), dict):
                        self.sim_withdraw(t, "切到真实交易，按真实订单重挂", now_ms)
                        if float(t.get("shares") or 0) <= 1e-9:
                            self.sim_rekey(tid, t)
                        else:
                            self.sim_save([(tid, t)])
            self.live_notify(("🧪 真实交易模式 {b} → pause：登录 Predict，可自检和挂单测试，不开新仓。" if after == "pause"
                              else "💰 真实交易模式 {b} → on：模拟交易的每个决定都会真实下单。").format(b=before))

    async def live_set_mode(self, value: str) -> str:
        """/live mode off|pause|on (also the control page): saved like a control setting, in force at once."""
        value = value.strip().lower()
        if value not in {"off", "pause", "on"}:
            return "用法：/live mode off｜pause｜on（off 只记账；pause 登录 Predict、可自检和挂单测试、不开新仓；on 真实下单）"
        before = self.config.live_mode
        if before == value:
            return f"真实交易模式已经是 {value}。"
        if value == "on" and not self.config.live_allowed:
            return "❌ 部署变量 LIVE_TRADING_ALLOWED=off 禁止真实下单（硬开关，数据库里的设置改不了它）：在 Railway 把它改成 on 并重新部署后才能切到 on。"
        result = self.control_set({"LIVE": value})
        if not result["ok"]:
            return "❌ " + result["message"]
        lines = [f"真实交易模式：{before} → {value}（已保存，重启后仍有效；环境变量 LIVE 只是初始值）"]
        if value != "off":
            lines += await self.live_prepare()
            if not self.live.ready:
                lines.append(f"⚠️ 未就绪：{self.live.not_ready_why}；就绪前不会下单，每分钟自动重试登录。")
            if value == "on":
                lines.append("⚠️ 从现在起模拟交易的每个决定都会真实下单。/live mode pause 或 /live pause 可停。")
            else:
                lines.append("现在可以发 /live check 自检、/live test 挂单测试；都正常后 /live mode on。")
        return "\n".join(lines)

    def control_payload(self) -> dict:
        data = super().control_payload()
        data["mode"], data["live"] = "live", self.live_status(self.market.now_ms())
        if getattr(self, "preopen", None) is not None:
            data["preopen"] = {"status": self.preopen.status(), "reserved_usd": self.preopen.risk()}
        return data

    def live_status(self, now_ms: int) -> dict:
        """The real-trading block of the control page: readiness, the switches' state, limits and their use, the
        recent orders (with what the page may cancel), the positions last read, the latest failures."""
        c = self.config
        trades = self.sim_trades()
        live = [(tid, t) for tid, t in trades.items() if isinstance(t.get("live"), dict)]
        today = dt.datetime.fromtimestamp(now_ms / 1000, core.BEIJING).replace(hour=0, minute=0, second=0, microsecond=0)
        since = int(today.timestamp() * 1000)
        rows = []
        active = [(tid, t) for tid, t in live if t["live"].get("state") in PLACING]
        finished = [(tid, t) for tid, t in live if t["live"].get("state") not in PLACING]
        shown = (sorted(active, key=lambda kv: kv[1].get("opened", 0), reverse=True)
                 + sorted(finished, key=lambda kv: kv[1].get("opened", 0), reverse=True)[:max(10, 30 - len(active))])
        for tid, t in shown:
            st = t["live"]
            rows.append({"id": tid, "opened": core.stamp(t["opened"], seconds=False), "item": t["item"], "label": t["label"],
                         "price": float(t["price"]), "order": float(t.get("order") or 0), "shares": float(t.get("shares") or 0),
                         "status": sim_status(t), "state": core.sim_state(t), "order_id": str(st.get("order_id") or ""),
                         "live_state": str(st.get("state") or ""), "maker": bool(t.get("maker")),
                         "why": str((t.get("withdrawn") or {}).get("why") or st.get("error") or ""),  # why it was withdrawn / failed
                         "cancellable": t["status"] == "resting" and st.get("state") in {"open", "placing"} and bool(st.get("order_id")),
                         "url": self.sim_url(t)})
        positions, at = self.live_positions
        return {"ready": self.live.ready, "ready_error": self.live.not_ready_why, "mode": c.live_mode, "paused": self.live_paused(), "killed": self.live_killed(),
                "lines": self.live.summary_lines(), "info": self.live.summary_info(), "account": short_addr(self.live.wallet.maker),
                "exposure": self.live_exposure(trades), "daily_pnl": (own_pnl := self.live_daily_pnl(trades, now_ms)),
                # the stop (live_check_kill / live_send_block) compares the model trader's day PLUS the pre-open mode's against
                # LIVE_MAX_DAILY_LOSS_USD (1.125.0): the page shows that same sum (day_pnl), daily_pnl stays the model trader's own
                "preopen_pnl": (pre_pnl := self.preopen_pnl()), "day_pnl": own_pnl + pre_pnl,
                "caps": {"order": c.live_max_order_usd, "open": c.live_max_open_usd, "daily_loss": c.live_max_daily_loss_usd},
                "loss_floor": self.live_resumed_floor(),  # 1.150.0: where the loss stop stands after today's /live resume (None: at the cap)
                "resting": sum(t["status"] == "resting" for _, t in live),
                "unconfirmed": sum(t["live"].get("state") in {"pending", "placing", "cancelling", "unknown"} for _, t in live),
                "synced_at": core.stamp(self.live_synced_at) if self.live_synced_at else "",
                "sync_age_s": (now_ms - self.live_synced_at) // 1000 if self.live_synced_at else None, "hold": self.live_send_hold(now_ms),
                "unknown": self.live_unknown_rows(live, now_ms),
                "oldest_cancel_s": max([(now_ms - int((t.get("withdrawn") or {}).get("at") or now_ms)) // 1000 for _, t in live if t["live"].get("state") == "cancelling"], default=None),
                "unknown_open": [dict(u) for u in self.live_unknown_open], "orphans": [dict(o) for o in self.live_orphans[-20:]], "allowed": c.live_allowed,
                "orders_complete": bool(getattr(self.live.api, "orders_complete", True)), "positions_complete": bool(getattr(self.live.api, "positions_complete", True)),
                "gate": (f"未就绪：{self.live.not_ready_why}" if not self.live.ready else self.live_balance_gate() or self.live_send_hold(now_ms)),
                "balance_age_s": int(time.time() - self.live.balance[2]) if self.live.balance else None, "balance_error": self.live.balance_error,
                "placed_today": sum(int(t["opened"]) >= since for _, t in live),
                "failed_today": sum(int(t["opened"]) >= since and t["live"].get("final") == "failed" for _, t in live),
                "orders": rows, "positions": self.position_lines(positions[:30], trades, now_ms),
                "positions_at": core.stamp(at * 1000) if at else "", "errors": self.live_errors[-5:],
                "last_test": str((self.store.get("live:test") or {}).get("text") or "")}

    # --- the order test: a tiny resting order, listed, cancelled ------------------------------------------------------------
    LIVE_TEST_SHARES = 5.0  # the floor; the exchange's minimum order value usually asks for more (50 shares at 2¢)
    LIVE_TEST_PRICE = 0.02  # never above this, and always under the best bid: it rests, it does not trade
    LIVE_TEST_HARD_USD = 2.0  # LIVE_TRADING_ALLOWED=off: the most a test is worth, whatever is saved (a minimum order at any price is under it)

    async def live_test(self, args: list[str], now_ms: int) -> str:
        """The real order path checked end to end with a dollar or so locked for minutes at most: a LIMIT buy far
        under the market (it rests; it is withdrawn at once; it expires in ten minutes by itself), then the open-order
        list, the cancel and the final state, each step reported with what Predict answered. Arguments: a market name
        to pick it, a share count, a price in cents (a price at or above the best bid may trade). The arguments are
        held to the limits like any real order (1.150.0, live_test_hold): never at or above the best ask, worth
        LIVE_MAX_ORDER_USD at the most (LIVE_TEST_HARD_USD under the hard switch), and at a price that may trade only
        while a model order could open."""
        if not self.live.ready:
            return f"未就绪：{self.live.not_ready_why}"
        words = [a.strip() for a in args if a and a.strip()]
        numbers = [w for w in words if re.fullmatch(r"\d+(\.\d+)?", w)]
        want = next((w.lower() for w in words if w not in numbers), "")
        shares_given = float(numbers[0]) if numbers else None
        cents_given = float(numbers[1]) if len(numbers) > 1 else None
        if shares_given is not None and (shares_given < 0.01 or shares_given > 1000):
            return "测试份数应在 0.01～1000 之间"
        markets = [mk for mk in self.sim_markets(now_ms) if mk.book.bid and mk.book.ask and not mk.book.stale(now_ms) and not core.book_crossed(mk.book)]
        if want:
            markets = [mk for mk in markets if want in mk.item.lower() or want in mk.key.lower() or want in mk.market.lower()]
        markets.sort(key=lambda mk: (mk.kind != "close", mk.item))
        if not markets:
            return "没有可用的市场：需要一个双边有报价、盘口新鲜的市场" + (f"，且名字含“{want}”" if want else "") + "。"
        mk = markets[0]
        try:
            info = await self.live.market(mk.book.market_id)
        except Exception as error:
            return f"读取市场 {mk.item} 的详情失败：{core.clean_error(error) or type(error).__name__}"
        spec = self.touches[mk.key].spec if mk.kind == "touch" and mk.key in self.touches else None
        outcome, note = outcome_for_side(mk.kind, "up", info.outcomes, spec)
        if outcome is None:
            return f"{mk.item}：无法确定结果代币（{note}）"
        best_bid = float(mk.book.bid[0])
        price = core.floor_price(cents_given / 100) if cents_given is not None else min(self.LIVE_TEST_PRICE, core.floor_price(best_bid - 0.01))
        if price < 0.01:
            return f"{mk.item} 的最高买价只有 {core.cents(best_bid)}，放不下更低的测试挂单；换个市场：/live test <市场名>"
        caution = "（价格不低于盘口最高买价，可能成交）" if price >= best_bid - 1e-9 else ""
        least = math.ceil(MIN_ORDER_USD / price - 1e-9)  # the exchange's minimum order value, in shares at this price
        shares = max(shares_given if shares_given is not None else self.LIVE_TEST_SHARES, least)
        sized = (f"（{shares_given:g} 份不足 Predict 最低订单金额 ${MIN_ORDER_USD:g}，改为 {shares:g} 份）"
                 if shares_given is not None and shares > shares_given else "")
        # 1.150.0: the share count and the price are the operator's (the control endpoint passes its request's "args" on as
        # well), and readiness was all that stood before the signature: `/live test 恒生 1000 99` sent a $990 buy across the
        # book with LIVE_MAX_ORDER_USD at $5 — under LIVE=pause, /live pause, today's loss stop and LIVE_TRADING_ALLOWED=off
        # alike. The test is a resting order, and stays one whatever the arguments: (1) never at or above the best ask — that
        # trades at once: a purchase no record holds, not a test (`恒生 100 1` typed as `恒生 1 100` was one); (2) worth
        # LIVE_MAX_ORDER_USD at the most, like every real order — and LIVE_TEST_HARD_USD while LIVE_TRADING_ALLOWED=off: the
        # cap is a saved setting (the page raises it), and under the hard switch no saved setting decides what leaves; (3) at
        # a price that may trade (at or above the best bid) only while a model order could open a position (live_test_hold).
        # The test without arguments — 2¢ at the most, under the best bid, $1 — passes all three in every mode: LIVE=pause is
        # where it is meant to be run
        c = self.config
        best_ask, notional = float(mk.book.ask[0]), price * shares
        if price >= best_ask - 1e-9:
            return (f"❌ {mk.item}：测试价 {core.cents(price)} 不低于盘口最低卖价 {core.cents(best_ask)}，会立刻成交。挂单测试只挂单、不吃单："
                    "给一个低于最低卖价的价格，或不给价格（自动挂在最高买价之下）")
        most = c.live_max_order_usd if c.live_allowed else min(c.live_max_order_usd, self.LIVE_TEST_HARD_USD)
        if notional > most + 1e-9:
            limit = f"LIVE_MAX_ORDER_USD ${most:g}" if most == c.live_max_order_usd else f"${most:g}（部署变量 LIVE_TRADING_ALLOWED=off 时挂单测试的上限）"
            return f"❌ 测试单 {core.cents(price)}×{shares:g} 份 = ${notional:,.2f}，超过 {limit}：减少份数或降低价格（不给份数和价格时约 $1）"
        if caution and (why := self.live_test_hold(notional)):
            return (f"❌ {why}：测试价 {core.cents(price)} 不低于盘口最高买价 {core.cents(best_bid)}，可能成交。"
                    "这时挂单测试只能挂在最高买价之下（不给价格即可）")
        try:
            amounts = limit_amounts(True, to_wei(price), to_wei(shares))
            order = build_order(self.live.wallet, outcome["token"], amounts["maker"], amounts["taker"], info.fee_bps, int(now_ms // 1000) + 600)
            digest, signature = self.live.wallet.sign_order(order, info.neg_risk, info.yield_bearing)
        except Exception as error:
            return f"构造测试订单失败：{core.clean_error(error) or type(error).__name__}"
        body = {"data": {"order": {**order, "signature": signature, "hash": digest}, "pricePerShare": str(amounts["price_per_share"]), "strategy": "LIMIT"}}
        lines = [f"🧪 挂单测试 {core.stamp(now_ms)}：{mk.item}（市场 {info.market_id}）买 {outcome['name']} {core.cents(price)}×{shares:g} 份，"
                 f"最多花 ${from_wei(amounts['maker']):.2f}，10 分钟后自动过期{caution}{sized}"]
        record = {"at": now_ms, "market": mk.item, "market_id": info.market_id, "price": price, "shares": shares, "hash": digest, "order_id": ""}
        try:
            result = await self.live.api.create_order(body)
        except Exception as error:
            lines.append(f"1/4 下单失败：{core.clean_error(error) or type(error).__name__}")
            shown = {**body["data"], "order": {k: v for k, v in body["data"]["order"].items() if k != "signature"}}
            lines.append("请求体（不含签名）：" + json.dumps(shown, ensure_ascii=False))
            return self.live_test_done(record, lines)
        oid = record["order_id"] = result["order_id"]
        lines.append(f"1/4 下单成功：订单 #{oid}" + (f"（code {result['code']}）" if result.get("code") else ""))
        try:
            rows = await self.live.api.orders("OPEN")
            listed = next((r for r in rows if str(r.get("id")) == oid or order_hash_of(r) == digest.lower()), None)
            lines.append("2/4 开放订单列表：" + (f"已列出（status {listed.get('status')}，amount {listed.get('amount')}，amountFilled {listed.get('amountFilled')}）"
                                            if listed else f"未列出（列表共 {len(rows)} 条）"))
        except Exception as error:
            lines.append(f"2/4 读取开放订单失败：{core.clean_error(error) or type(error).__name__}")
        try:
            removed = await self.live.api.remove_orders([oid])
            lines.append("3/4 撤单：" + ("已撤" if oid in removed["removed"] else f"接口未确认（removed {removed['removed']}，noop {removed['noop']}）"))
        except Exception as error:
            lines.append(f"3/4 撤单失败：{core.clean_error(error) or type(error).__name__}；这张单 10 分钟后自动过期，也可在网站撤")
        try:
            final = await self.live.api.order(oid, digest.lower())
            if final is None:
                lines.append("4/4 最终状态：Predict 已不再列出这张单")
            else:
                filled, status = order_fill(final, shares)
                lines.append(f"4/4 最终状态：{status or '未知'}，成交 {filled:g}/{shares:g} 份" + ("；成交的份额留在账户里" if filled > 0 else ""))
        except Exception as error:
            lines.append(f"4/4 查询最终状态失败：{core.clean_error(error) or type(error).__name__}")
        return self.live_test_done(record, lines)

    def live_test_hold(self, notional: float) -> str:
        """Why an order test at a price that may trade is not sent now: the operator's stops and caps a model order of
        its value would answer to before it opened a position — the hard switch, the mode, /live pause, today's loss stop
        (the same flags live_room reads), LIVE_MAX_OPEN_USD. "" when none holds. A test under the best bid is not asked:
        it rests, and is what LIVE=pause is there for."""
        c = self.config
        if not c.live_allowed:
            return "部署变量 LIVE_TRADING_ALLOWED=off（硬开关），不开新仓"
        if c.live_mode != "on":
            return f"LIVE={c.live_mode}，" + ("只记账" if c.live_mode == "off" else "不开新仓")
        if paused := self.live_paused():
            return f"真实交易已暂停（{paused}；/live resume 恢复）"
        if killed := self.live_killed():
            return f"今日停止开新仓（{killed}）"
        exposure = self.live_exposure(self.sim_trades())
        if exposure + notional > c.live_max_open_usd + 1e-9:
            return f"持仓+挂单 ${exposure:,.2f} 加本单 ${notional:,.2f} 超过 LIVE_MAX_OPEN_USD ${c.live_max_open_usd:g}"
        return ""

    def live_test_done(self, record: dict, lines: list[str]) -> str:
        text = "\n".join(lines)
        self.store.put("live:test", {**record, "text": text})
        self.live_notify(text)
        return text

    async def live_control(self, action: str, data: dict) -> dict:
        now_ms = self.market.now_ms()
        if action == "pause":
            text = await self.live_action("pause", [str(data.get("why") or "")], now_ms)
        elif action == "cancel":
            text = await self.live_action("cancel", [str(data.get("id") or "")], now_ms)
        elif action == "test":
            text = await self.live_action("test", [str(a) for a in (data.get("args") or []) if str(a).strip()], now_ms)
        elif action == "mode":
            text = await self.live_set_mode(str(data.get("value") or ""))
            if text.startswith("❌") or text.startswith("用法"):
                return {"ok": False, "message": text.removeprefix("❌ ")}
        elif action == "release":
            text = await self.live_action("release", [str(data.get("id") or "")], now_ms)
        elif action == "stop":  # the emergency stop: no new orders, every resting order of the strategy asked to be withdrawn
            # Audit LIVE-STOP-OK: the stop's verdict is its cancels', not the pause's. The pause reply used to be the
            # head of the message, and the generic check below only reads the head ("⏸ …", never a failure), so a stop whose
            # cancels all failed (❌ HTTP 500, unsent requests, pre-open orders left behind) still came back ok=True and the page
            # showed a green toast — during an emergency, the one moment it matters. The pause is still recorded the same way;
            # its text ("已有挂单保留", which the stop contradicts) is replaced by the stop's own head line.
            await self.live_action("pause", ["紧急停止"], now_ms)
            first = "⛔ 紧急停止：已暂停开新仓，正在撤掉本策略的全部挂单（以 Predict 的确认为准）。/live resume 恢复。"
            second = await self.live_cancel_command(["all"], now_ms)
            line = ""
            if getattr(self, "preopen", None) is not None:
                line = await self.preopen.withdraw_all("紧急停止")
                second += (f"\n{line}" if line else "") + "\n" + self.preopen.status()
            text = f"{first}\n{second}"
            # "⏳" (sent, awaiting Predict's confirmation) and "没有真实挂单可撤。" are not failures; "❌"/"未就绪" and pre-open
            # orders the stop could not withdraw ("没撤成") are
            stop_failed = second.startswith(("❌", "未就绪")) or "没撤成" in line
            return {"ok": not stop_failed, "message": text}
        elif action in {"resume", "redeem", "check", "orders", "positions"}:
            text = await self.live_action(action, [], now_ms)
        else:
            return {"ok": False, "message": "未知操作"}
        message = text if isinstance(text, str) else text.text
        # "nothing to do" is not a failure — but 这次没有领取 is: something claimable was not claimed (no BNB for gas, a redeem that
        # failed, 缺字段 …). 1.150.0 (audit RDM-PAGE-REDEEM): the page's 领取已结算 showed that as a success (the green toast)
        failed = message.startswith(("❌", "未就绪", "用法", "这次没有领取")) or "失败" in message.split("\n")[0]
        return {"ok": not failed, "message": message}

    async def live_cancel_command(self, words: list[str], now_ms: int) -> str:
        preopen = getattr(self, "preopen", None)
        if preopen is not None and words:
            matches = [p for p in preopen.ledger.rows.values() if any(str(o.get("id") or "") == words[0] for o in p["orders"].values())]
            if words[0] in {"all", "account"} or matches:
                preopen.manual_pause = preopen.ledger.pause(True)
                if matches and words[0] not in {"all", "account"}:
                    await preopen.cancel_many(matches, "管理员撤单")
                    return "已请求撤销该场双边预挂单，预挂模式已暂停。\n" + preopen.status()
                result = await self._live_cancel_command(words, now_ms)  # 1.141.1: the model trader's own few first
                line = await preopen.withdraw_all("管理员撤单")  # 1.141.0: thousands go in batches from the mode's own loop
                return result + "\n已请求撤销预挂单，预挂模式已暂停。" + (f"\n{line}" if line else "") + "\n" + preopen.status()
        return await self._live_cancel_command(words, now_ms)

    async def _live_cancel_command(self, words: list[str], now_ms: int) -> str:
        trades = self.sim_trades()
        targets = {tid: t for tid, t in trades.items() if isinstance(t.get("live"), dict) and t["status"] == "resting"
                   and t["live"].get("state") in {"open", "placing", "pending", "unknown"}}
        if not words:
            return "用法：/live cancel all（撤掉本策略的全部真实挂单）、/live cancel account（撤掉账户上的全部挂单，含手动单）或 /live cancel <订单号>；/live release <订单号> 把状态未知的订单按不存在处理"
        extra: list[str] = []  # the account's orders this bot has no record of
        if words[0] == "account":
            if not self.live.ready:
                return f"❌ 未就绪：{self.live.not_ready_why}"
            known = {str(t["live"].get("order_id")) for t in trades.values() if isinstance(t.get("live"), dict)}
            preopen = getattr(self, "preopen", None)  # 1.141.1: the pre-open mode's own orders are its loop's to withdraw (in batches), not sent twice
            pre_ids, pre_hashes = preopen.known() if preopen is not None else (set(), set())
            extra = [str(r.get("id")) for r in await self.live.api.orders("OPEN")
                     if str(r.get("id")) not in known and str(r.get("id")) not in pre_ids and (order_hash_of(r) or "-") not in pre_hashes]
        elif words[0] != "all":
            targets = {tid: t for tid, t in targets.items() if str(t["live"].get("order_id")) == words[0]}
            if not targets:
                return f"没有订单号为 {words[0]} 的真实挂单（/live 查看）"
        for tid, t in targets.items():
            self.sim_withdraw(t, "管理员撤单", now_ms)
            self.sim_save([(tid, t)])
        if not targets and not extra:
            return "没有真实挂单可撤。"
        problems = []
        if extra:
            limit = self.live_remove_limit()
            result = await remove_in_batches(self.live.api, extra, limit, workers=3, patient=True, deadline=time.monotonic() + 60,
                                             stop=self.stopping.is_set)  # 1.141.0: was one request for them all
            self.live_remove_saved(limit)
            self.live_unknown_open = [u for u in self.live_unknown_open if u["id"] not in set(result["removed"])]
            quiet = len(result["noop"]) + len(result["unanswered"])
            missed = len(result["failed"]) + len(result["left"])
            extra_text = (f"账户上无记录的挂单 {len(extra)} 张：Predict 确认撤掉 {len(result['removed'])} 张" + (f"，{quiet} 张称无需撤" if quiet else "")
                          + (f"，{missed} 张没撤成（{result['errors'][-1] if result['errors'] else '一分钟内没撤完'}），再发一次 /live cancel account" if missed else ""))
            if missed:
                problems.append(extra_text)
        list_error = ""
        try:
            if targets:
                await self.live_sync_orders(targets, now_ms)
        except Exception as error:
            list_error = core.clean_error(error) or type(error).__name__
        after = self.sim_trades()
        states = [after.get(tid, t)["live"] for tid, t in targets.items()]
        confirmed = sum(s.get("state") == "done" for s in states)
        # 1.150.0: a removal another path has on its way (the step's sync, an express pass: live_cancels) is sent, its answer awaited —
        # this command's own pass left that order to it (live_cancel), and its cancel_sent is only set once Predict answers
        flying = lambda s: str(s.get("order_id") or "") in self.live_cancels
        sent = sum(s.get("state") == "cancelling" and (bool(s.get("cancel_sent")) or flying(s)) for s in states)
        unsent = sum(s.get("state") == "cancelling" and not s.get("cancel_sent") and not flying(s) for s in states)
        if list_error:
            # the open list could not be read: the cancels with an order id went out by id all the same (live_cancel_blind)
            note = f"账户挂单列表读取失败（{list_error}）：撤单改按订单号直接发送" + ("，未发出的下一轮重试" if unsent else "")
            if unsent:
                problems.append(note)
            else:
                extra_note = note
        failed = [s.get("last_error") for s in states if s.get("last_error") and s.get("state") == "cancelling"]
        parts = []
        if targets:
            parts.append(f"策略挂单 {len(targets)} 笔：Predict 确认撤掉 {confirmed} 笔"
                         + (f"，{sent} 笔撤单请求已发送、待确认（每 10 秒核对，确认前额度仍占用）" if sent else "")
                         + (f"，{unsent} 笔撤单请求尚未发出（下一轮重试，额度仍占用）" if unsent else "")
                         + (f"，{len(failed)} 笔出错（{failed[0]}）" if failed else ""))
        if extra:
            parts.append(extra_text)
        if list_error and not unsent:
            parts.append(extra_note)
        parts.extend(p for p in problems if p != (extra_text if extra else None))
        pending = sent + unsent
        head = "❌ " if problems or failed else ("⏳ " if pending else "✅ ")
        return head + "；".join(parts) + ("" if not targets else "\n" + "、".join(f"{t['item']} {t['label']}" for t in targets.values()))

    @staticmethod
    def position_line(row: dict) -> str:
        market = row.get("market") if isinstance(row.get("market"), dict) else {}
        outcome = row.get("outcome") if isinstance(row.get("outcome"), dict) else {}
        try:
            shares = from_wei(int(str(row.get("amount") or "0")))
        except ValueError:
            shares = 0.0
        value = row.get("valueUsd") or row.get("value")
        return (f"{market.get('title') or row.get('marketId') or '?'}｜{outcome.get('name') or '?'} {shares:g} 份"
                + (f"｜≈${float(value):,.2f}" if value not in (None, "") else "") + (f"｜{outcome.get('status')}" if outcome.get("status") else ""))

    @staticmethod
    def position_price(row: dict) -> float | None:
        """The average price a position was bought at, as its row states it (several spellings; a fraction, cents or
        wei): None when it states none (1.119.3)."""
        for key in ("avgPrice", "averagePrice", "avgCost", "averageCost", "costBasis", "entryPrice", "avgEntryPrice", "price"):
            value = row.get(key)
            if value in (None, "", 0, "0"):
                continue
            with contextlib.suppress(TypeError, ValueError, decimal.InvalidOperation):
                price = D(str(value))
                if price > 100:
                    price = price / WEI
                elif price > 1:
                    price = price / 100
                if 0 < price <= 1:
                    return float(price)
        return None

    @staticmethod
    def position_time(row: dict) -> int:
        """When a position last changed, as its row states it (ISO text, or an epoch in seconds or ms), in ms; 0 when
        it states none (1.119.3)."""
        for key in ("lastTradeAt", "lastFilledAt", "updatedAt", "lastUpdatedAt", "timestamp", "createdAt"):
            value = row.get(key)
            if value in (None, "", 0, "0"):
                continue
            with contextlib.suppress(TypeError, ValueError, OverflowError):
                if isinstance(value, (int, float)) or str(value).strip().isdigit():
                    n = int(float(value))
                    return n if n > 10 ** 11 else n * 1000
                return int(dt.datetime.fromisoformat(str(value).strip().replace("Z", "+00:00")).timestamp() * 1000)
        return 0

    @staticmethod
    def live_pick_adoptee(cands: list[tuple[str, dict]], price: float | None, when: int, grace_ms: int) -> tuple[str, dict, str]:
        """Which of an outcome's ended orders (newest first) a late fill goes on, and why (1.119.3): the newest whose
        price is the position's average (a maker fills at its price; a taker at or under its limit), else the newest
        that was open when the position last changed, else the newest of all (1.119.0–1.119.2 always took that one: a
        KOSPI fill at 21¢ landed on a 1¢ sweep order placed after it)."""
        if price is not None:
            for tid, t in cands:
                want = t["live"].get("want") or {}
                limit = float(want.get("limit") or want.get("cap") or 0) or float(t["price"])
                if (t.get("maker") and abs(float(t["price"]) - price) <= 0.0051) or (not t.get("maker") and price <= limit + 0.0051):
                    return tid, t, f"按 Predict 持仓均价 {core.cents(price)} 对上这张单"
        if when:
            for tid, t in cands:
                if int(t.get("opened") or 0) - 60_000 <= when <= core.trade_updated(t) + grace_ms:
                    return tid, t, f"按持仓变动时间 {core.stamp(when, seconds=False)} 对上当时挂着的这张单"
        tid, t = cands[0]
        return tid, t, "按最近结束的订单对应（Predict 持仓没给均价和时间）"

    @staticmethod
    def position_shares(row: dict) -> float:
        try:
            return from_wei(int(str(row.get("amount") or "0")))
        except ValueError:
            return 0.0

    def position_token(self, row: dict) -> str:
        """The outcome token a Predict position is in: the row's own, else the market's outcome at that index set as
        the bot read the market (live.markets). "" when neither says."""
        outcome = row.get("outcome") if isinstance(row.get("outcome"), dict) else {}
        token = str(outcome.get("onChainId") or outcome.get("tokenId") or "")
        if token:
            return token
        market = row.get("market") if isinstance(row.get("market"), dict) else {}
        info = self.live.markets.get(str(market.get("id") or row.get("marketId") or ""))
        if info is None:
            return ""
        with contextlib.suppress(TypeError, ValueError):
            index = int(outcome.get("indexSet"))
            return next((str(o.get("token") or "") for o in info.outcomes if int(o.get("index_set") or 0) == index), "")
        return ""

    def position_lines(self, rows: list[dict], trades: dict, now_ms: int) -> list[str]:
        """Predict's positions, each against the bot's records on its outcome token (1.118.2): the records holding it
        and their shares against Predict's, or ⚠️ 无记录 — then the orders on that outcome that ended without a fill in
        the last two days, the way a fill that landed as the cancel did can go unread."""
        out = []
        for r in rows:
            token, shares, line = self.position_token(r), self.position_shares(r), self.position_line(r)
            if shares < self.LIVE_DUST_SHARES:
                out.append(line + "｜零头（不到 0.1 份），不计")
                continue
            if not token:
                out.append(line + "｜记录？（读不出结果代币，无法对照）")
                continue
            mine = [(tid, t) for tid, t in trades.items()
                    if isinstance(t.get("live"), dict) and str((t["live"].get("want") or {}).get("token") or "") == token]
            held = [(tid, t) for tid, t in mine if float(t.get("shares") or 0) > 1e-9 and t.get("status") in {"filled", "resting", "settled"}]
            tag = lambda tid, t: f"#{t['live']['order_id']}" if t["live"].get("order_id") else tid
            if held:
                total = sum(float(t["shares"]) for _, t in held)
                diff = shares - total
                line += (f"｜记录 {len(held)} 笔 {total:g} 份（{'、'.join(tag(tid, t) for tid, t in held[:5])}）"
                         + (f"，比记录多 {diff:g} 份" if diff > 0.01 else f"，比记录少 {-diff:g} 份" if diff < -0.01 else "，一致"))
            else:
                ended = [(tid, t) for tid, t in mine if int(t.get("opened") or 0) >= now_ms - 2 * core.DAY_MS and float(t.get("shares") or 0) <= 1e-9]
                line += "｜⚠️ 无记录" + (f"（这一边有 {len(ended)} 张没成交就结束的订单：{'、'.join(tag(tid, t) for tid, t in ended[:5])}，可能撤单前成交了没读到，每分钟自动核对补记）"
                                     if ended else "（本机器人近两天没有这一边的订单：手动买的或旧持仓，到期照常自动领取）")
            out.append(line)
        return out

    def positions_text(self, rows: list[dict], now_ms: int) -> str:
        """/live positions: Predict's positions against the records (position_lines), a count of the matches first."""
        lines = self.position_lines(rows[:30], self.sim_trades(), now_ms)
        if not lines:
            return "Predict 上的持仓：无"
        missing, differ, dust = sum("⚠️ 无记录" in l for l in lines), sum("比记录" in l for l in lines), sum("｜零头（" in l for l in lines)
        # 1.150.0 (audit RDM-CHECK-VERDICT): the head counted the lines listed (30 at most), so an account of 48 read 持仓 30 个
        whole = "" if getattr(self.live.api, "positions_complete", True) else "以上（列表没读全）"
        head = (f"Predict 上的持仓共 {len(rows)} 个{whole}，只列出并核对前 {len(lines)} 个" if len(rows) > len(lines)
                else f"Predict 上的持仓 {len(lines)} 个")
        return (f"{head}：{len(lines) - missing - differ - dust} 个与记录一致，{differ} 个份数不符，{missing} 个无记录"
                + (f"，{dust} 个零头" if dust else "") + "\n" + "\n".join(lines))

    def live_text(self, now_ms: int) -> str:
        trades = self.sim_trades()
        live = [t for t in trades.values() if isinstance(t.get("live"), dict)]
        # 1.150.0: a pause and today's loss stop are shown side by side — the pause alone was shown, and /live resume, sent to end
        # it, lifts the stop as well
        stops = "｜".join(x for x in ("⏸ 已暂停（" + self.live_paused() + "）" if self.live_paused() else "",
                                    "🛑 今日停开新仓（" + self.live_killed() + "）" if self.live_killed() else "") if x)
        state = ("⚪ LIVE=off：只记账（/live mode pause 登录测试，/live mode on 真实下单）" if self.config.live_mode == "off"
                 else "🧪 LIVE=pause：只测试，不开新仓" + ("" if self.live.ready else "｜未就绪：" + self.live.not_ready_why) if self.config.live_mode == "pause"
                 else stops or ("🟢 运行中" if self.live.ready else "🔴 未就绪：" + self.live.not_ready_why))
        ways, kinds = core.sim_scope(self.config)
        lines = [f"💰 {core.bold('真实交易')} v{core.VERSION}｜{state}", *self.live.summary_lines(),
                 f"策略：同模拟交易：净优势 ≥{self.config.sim_edge * 100:g}¢ 买 {self.config.sim_shares:g} 份；{ways}；{kinds}"]
        exposure = self.live_exposure(trades)
        resting = sum(t["status"] == "resting" for t in live)
        unconfirmed = sum(t["live"].get("state") in {"pending", "placing", "cancelling"} for t in live)
        if unconfirmed:
            lines.append(f"⏳ 未确认订单 {unconfirmed} 张（发送中 / 撤单中：Predict 确认前其额度仍占用）")
        if self.live_unknown_open:
            lines.append(f"⚠️ 账户上有 {len(self.live_unknown_open)} 张本机器人没有记录的挂单（#{'、#'.join(u['id'] for u in self.live_unknown_open[:5])}）：不计入这里的额度；/live cancel account 可全部撤掉")
        recent = [o for o in self.live_orphans if now_ms - int(o.get("at") or 0) <= 24 * 3600_000]
        if recent:
            lines.append(f"🧹 24 小时内自动撤掉 {len(recent)} 张无记录挂单（丢失跟踪的单；最近：{recent[-1]['brief']}）")
        today = dt.datetime.fromtimestamp(now_ms / 1000, core.BEIJING).replace(hour=0, minute=0, second=0, microsecond=0)
        since = int(today.timestamp() * 1000)
        placed_today = sum(int(t["opened"]) >= since for t in live)
        failed_today = sum(int(t["opened"]) >= since and (t["live"].get("final") == "failed") for t in live)
        # the stop (live_check_kill / live_send_block) compares the model trader's day plus the pre-open mode's against
        # LIVE_MAX_DAILY_LOSS_USD (1.125.0), so the line shows the same number (and the split when the pre-open part is non-zero):
        # showing only the model trader's part read e.g. −$15 of a −$50 cap while the stop had tripped at a combined −$55
        own, pre = self.live_daily_pnl(trades, now_ms), self.preopen_pnl()
        floor = self.live_resumed_floor()  # 1.150.0: after /live resume the stop stands a cap's worth further down: said, beside the cap
        lines.append(f"持仓+挂单 ${exposure:,.2f} / ${self.config.live_max_open_usd:g}｜今日已结算盈亏 {money(own + pre)}"
                     + (f"（原策略 {money(own)} + 预挂 {money(pre)}）" if pre else "")
                     + f"（上限 −${self.config.live_max_daily_loss_usd:g}" + (f"；今天手动恢复过，停机线 {money(floor)}" if floor is not None else "")
                     + f"）｜挂单中 {resting}｜今日下单 {placed_today}｜失败 {failed_today}")
        lines.append(self.live_redeem_status_line())
        stats = core.sim_stats(live)
        if stats["settled"]:
            lines.append(f"已结算 {stats['settled']} 笔：赢 {stats['wins']}｜输 {stats['losses']}｜平 {stats['ties']}"
                         f"｜成本 ${stats['cost']:,.2f} → 回款 ${stats['payout']:,.2f}｜盈亏 {core.bold(money(stats['pnl']))}")
        recent = sorted(live, key=lambda t: t.get("opened", 0), reverse=True)[:10]
        if recent:
            lines.append("\n最近的真实订单：")
            for t in recent:
                st = t["live"]
                tag = f"#{st['order_id']}" if st.get("order_id") else st.get("state", "")
                lines.append(f"{core.stamp(t['opened'], seconds=False)} {t['item']} {t['label']} {core.cents(float(t['price']))}×{float(t['order']):g}"
                             f" → {sim_status(t)}" + (f"·{core.sim_state(t)}" if core.sim_state(t) else "") + f"（{tag}）")
        if self.live_errors:
            lines.append("\n最近错误：\n" + "\n".join(self.live_errors[-3:]))
        if last := (self.store.get("live:test") or {}).get("text"):
            lines.append("\n最近挂单测试：" + str(last).split("\n")[0].removeprefix("🧪 挂单测试 ") + "…（/live test 重跑）")
        lines.append("\n/live mode off|pause|on｜pause｜resume｜cancel all｜redeem｜redeem check｜redeem forget <市场号>｜check｜orders｜positions｜test [市场] [份数] [价格¢]")
        if getattr(self, "preopen", None) is not None:
            # no html.escape here: the /live reply is sent with Bot.tell's to_html, which escapes the whole text once; escaping the
            # status too showed "&amp;lt;" etc. for any &, < or > in it (e.g. a remote error in "最近问题：")
            lines.append("\n" + self.preopen.status() + "\n/preopen 查看预挂；紧急停止会撤销本模式订单")
        return "\n".join(lines)

    def sim_text(self) -> str:
        text = super().sim_text()
        if self.config.live_mode == "off":
            return text + "\n\n真实交易模块已加载但处于 LIVE=off：/live mode pause 登录测试，/live mode on 真实下单。"
        word = "LIVE=pause：已登录但不开新仓" if self.config.live_mode == "pause" else "真实下单：LIVE=on"
        return text.replace("只记账不下单", word).replace("🧪", "💰", 1) + "\n\n真实订单与风控：/live"

    def cmd_help(self, req: core.Request) -> str:
        if self.config.live_mode == "off":
            return super().cmd_help(req) + "\n真实交易模块已加载（LIVE=off，只记账）：/live mode pause|on 切换。"
        return super().cmd_help(req).replace("不会自动下单、撤单。", f"⚠️ LIVE={self.config.live_mode}：模拟交易的决定会在 Predict 真实下单、撤单"
                                                             "（pause 模式不开新仓；/live 查看与暂停，/live mode off 回到只记账）。")

    def status(self, sub_id: str) -> str:
        state = ("LIVE=off，只记账（/live mode 切换）" if self.config.live_mode == "off" else "LIVE=pause，不开新仓" if self.config.live_mode == "pause"
                 else "、".join(x for x in ("已暂停" if self.live_paused() else "", "今日停开新仓" if self.live_killed() else "") if x)  # both, when both (1.150.0)
                 or ("运行中" if self.live.ready else f"未就绪：{self.live.not_ready_why}"))
        return super().status(sub_id).replace("仅价格提醒；不会自动撤单/交易。", f"💰 真实交易 {state}｜/live 查看订单、持仓与风控")

    def journal_payload(self, full: bool = False, history: int | None = None) -> dict:
        data = super().journal_payload(full, history)
        data["mode"] = "live"
        data["live"] = {"enabled": True, "account": short_addr(self.live.wallet.maker), "ready": self.live.ready,
                        "paused": self.live_paused(), "killed": self.live_killed(),
                        "unrecorded": self.live_unrecorded_positions(), "unknown_orders": len(self.live_unknown_open),  # 1.130.3
                        "positions_at": int(self.live_positions[1] * 1000) if self.live_positions[1] else 0}
        return data

    def preopen_review(self) -> dict | None:
        """The review page's 开盘前预挂 section (preopen.review): the running controller's ledger, or the file read while the
        mode is off; None when nothing is recorded. Audit PRE-LEDGER-GROWTH: with the ledger's archive — the old finished
        quotes that filled nothing left the working records (Ledger.archive_old), and the totals and tables count them as
        they always did. By their counts, kept from call to call (preopen.ArchiveShare: the archive is read once, after
        that only the rows moved in since): reading and parsing the whole archive at every call — two a build of
        journal.json — took twice as long as the review had with every record in memory, and longer with every day of
        history. Record by record off the file, as before that, only when the counts cannot stand for the records (one in
        the archive that is no plain withdrawn quote) or could not be had; never kept in memory either way."""
        from preopen import archive_share, read_archive, read_rows, review
        try:
            if self.preopen is not None:
                rows, old = list(self.preopen.ledger.rows.values()), self.preopen.ledger.archived()
            else:
                rows, old = read_rows(self.config.preopen.db), read_archive(self.config.preopen.db)
        except Exception as error:
            LOG.warning("预挂复盘读取失败：%s", core.clean_error(error) or type(error).__name__)
            return None
        now_ms = self.market.now_ms()
        try:
            share = self.preopen_share = archive_share(self.preopen.ledger if self.preopen is not None else self.config.preopen.db, rows, getattr(self, "preopen_share", None))
            if share.fits(now_ms):
                return review(rows, now_ms, share) if rows or share.count else None
        except Exception as error:  # the counts could not be had: dropped (they may be half brought up to date), and the archive read record by record below
            self.preopen_share = None
            LOG.warning("预挂复盘的归档统计没用上（这次逐条读归档）：%s", core.clean_error(error) or type(error).__name__)
        keys = {p.get("slug") for p in rows}  # a record in both (its move undone by hand) counts once: the working one
        more: list[int] = [0]
        failed: list[Exception] = []

        def every() -> Any:
            yield from rows
            try:
                for p in old:
                    if p.get("slug") not in keys:
                        more[0] += 1
                        yield p
            except Exception as error:  # the archive cannot be read (it is read as the review walks it): no figures, not short ones
                failed.append(error)
        out = review(every(), now_ms)
        if failed:
            LOG.warning("预挂复盘读取失败（归档）：%s", core.clean_error(failed[0]) or type(failed[0]).__name__)
            return None
        return out if rows or more[0] else None

    async def preopen_apply(self) -> None:
        """The pre-open mode as PREOPEN_15M and the PREOPEN_* settings say now (the environment, the control page's saved
        values over it): started when on and not running; restarted when its settings changed — the orders stay on the
        book, the new controller takes over those the settings still describe and withdraws the rest (in batches) — or
        when it crashed. Switched off, it winds down (1.141.0): nothing new, every order withdrawn in batches until none is
        left and the final fills read, then it stops itself and is cleared here (the stop used to withdraw for 12 seconds
        and leave the rest of a book of thousands resting). Off, but with orders its records say may rest (a restart in the
        middle of that, an older version's cut): one is started just to wind them down."""
        async with self.preopen_lock:
            want = self.config.preopen
            cur = self.preopen
            if cur is not None and not cur.alive:  # crashed, or wound up: cleared (started afresh below when on)
                await self.preopen_stop("已停止")
                cur = None
            if cur is not None:
                if getattr(cur, "winding", ""):
                    if want.enabled:  # switched back on while winding down: the new controller takes the records over
                        await self.preopen_stop("重新开启", keep=True)
                elif not want.enabled:  # 1.141.0: switched off — every order withdrawn in batches until none is left, then it stops itself
                    cur.wind_down("预挂模式已关闭")
                elif cur.settings != want:
                    await self.preopen_stop("设置改变", keep=want.enabled)
            if self.preopen is None and not self.stopping.is_set():
                if want.enabled:
                    self.preopen_start()
                else:
                    self.preopen_tidy()  # off: no controller will ever move the old records out of the file the check below and the positions check read whole
                    if self.preopen_leftovers():  # off, yet its records hold orders that may rest: withdrawn all the same
                        self.preopen_start(wind_down="预挂模式关着，但上次还有挂单没撤完")

    def preopen_tidy(self) -> None:
        """The mode off and no controller on its ledger file: the old finished quotes that filled nothing still leave the
        file's working records (preopen.archive_file — a running controller does it itself, Controller.archive). Audit
        PRE-LEDGER-GROWTH: while the mode is off that file is read whole twice a minute by the positions check
        (preopen_filled, preopen_unverified) and several times by every build of the review page — at 50,000 records two
        seconds of the event loop each time. At a start and at a settings apply, an hour apart at least; a failure is
        logged and nothing moves."""
        from preopen import ARCHIVE_EVERY_MS, archive_file
        path = self.config.preopen.db
        if path == ":memory:" or time.monotonic() - getattr(self, "preopen_tidied", -1e9) < ARCHIVE_EVERY_MS / 1000:
            return
        self.preopen_tidied = time.monotonic()
        try:
            moved = archive_file(path, self.market.now_ms())
        except Exception as error:
            LOG.warning("预挂旧记录归档失败（记录原样保留）：%s", core.clean_error(error) or type(error).__name__)
            return
        if moved:
            LOG.info("开盘前预挂（未开启）：%d 条早已结束、已撤且没成交的记录移入归档", moved)

    def preopen_leftovers(self) -> int:
        """Orders the pre-open mode's records say may still rest while the mode is off (1.141.0) — a stop in the middle of a
        withdrawal, a crash, an older version's 12-second cut: a controller is started to wind them down."""
        from preopen import read_rows, resting_orders
        try:
            rows = read_rows(self.config.preopen.db)
        except Exception as error:
            LOG.warning("预挂记录读取失败：%s", core.clean_error(error) or type(error).__name__)
            return 0
        if not rows:
            return 0
        now = self.market.now_ms()
        return sum(resting_orders(p, now) for p in rows)

    def preopen_start(self, wind_down: str = "") -> None:
        from preopen import Controller
        self.preopen_day_read = None  # a controller holds the ledger from here: its own records count (preopen_pnl), not a figure read off the file
        preopen = self.preopen = (self.preopen_factory or Controller)(self)
        if wind_down:  # nothing new goes out: the orders still resting are withdrawn, then it stops itself
            preopen.wind_down(wind_down)
        task = self.preopen_task = asyncio.create_task(preopen.run(), name="preopen")

        def finished(t: asyncio.Task) -> None:
            if not t.cancelled() and t.exception() is not None:
                text = core.clean_error(t.exception()) or type(t.exception()).__name__
                LOG.error("开盘前策略异常结束：%s", text)
                # the bot goes on (its run() withdrew what it could of this mode's orders on the way out: counted below); the
                # model trader is free again — within the day's loss stop, which still counts this mode's realised loss
                # (preopen_pnl, Audit PRE-PNL-DROP)
                preopen.error, preopen.alive = f"预挂模式已停止：{text}", False
                self.live_note_error(f"开盘前预挂模式异常停止：{text}")
                # Audit PRE-CRASH-WITHDRAW: what is left is counted, not assumed. "本模式的挂单已撤" went out whatever the
                # withdrawal did — nothing at all on a ledger it could not write (fixed in on_stop), cut short by
                # STOP_CANCEL_S, or stopped by a refusal of every request (401/403). The records in memory carry each removal.
                try:
                    from preopen import resting_orders
                    left = sum(resting_orders(p, preopen.now()) for p in preopen.ledger.rows.values())
                    gone = ("本模式的挂单已撤" if not left else
                            f"还有 {left} 张预挂单可能还挂着，开场前 {preopen.settings.expiry_seconds} 秒签名到期之前仍可能成交，请到 Predict 核对")
                except Exception:
                    gone = "撤单情况不明，请到 Predict 核对"
                self.live_notify(f"⚠️ 开盘前预挂模式异常停止：{text}（{gone}，重启后恢复）", key="preopen:crash")
            elif not t.cancelled() and getattr(preopen, "winding", "") and self.preopen is preopen and not self.stopping.is_set():
                asyncio.get_running_loop().create_task(self.preopen_apply(), name="preopen-apply")  # wound up: cleared (or started afresh)
        task.add_done_callback(finished)
        LOG.info("开盘前预挂模式启动：%s", "、".join(sorted(preopen.settings.markets)))

    async def preopen_stop(self, why: str, keep: bool = False) -> None:
        """The mode stopped: its own stop set, its task waited for. Its run() withdraws its orders on the way out unless
        ``keep`` (a settings change, the process stopping): they stay on the book for the next controller to take over."""
        preopen, task = self.preopen, self.preopen_task
        if preopen is None:
            return
        preopen.keep_on_stop = keep
        preopen.stop.set()
        if task is not None:
            await asyncio.wait([task], timeout=20)
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        with contextlib.suppress(Exception):
            preopen.ledger.close()
        self.preopen, self.preopen_task = None, None
        self.preopen_day_read = None  # its last records are on file now: read afresh at the next loss check (preopen_file_day)
        LOG.info("开盘前预挂模式停止：%s", why)

    def preopen_active(self) -> bool:
        """Whether the pre-open mode is running for real: its orders go out only with LIVE=on (paper trading goes on with
        LIVE=off; a mode that crashed, alive False, is not running)."""
        preopen = getattr(self, "preopen", None)
        return preopen is not None and bool(getattr(preopen, "alive", True)) and self.config.live_mode == "on"

    def preopen_exclusive(self) -> bool:
        """Whether the running mode holds the model trader back (PREOPEN_EXCLUSIVE=on, 1.125.0: off by default — the two run
        side by side, the mode's reservation in the model's exposure cap, its day's P&L in the loss stop, its fills known
        to the positions reconciliation)."""
        return self.preopen_active() and bool(self.config.preopen.exclusive) and not getattr(self.preopen, "winding", "")

    def preopen_risk(self) -> float:
        """The money the running pre-open mode may have tied up (Controller.risk). Shown; since 1.132.1 not counted against
        LIVE_MAX_OPEN_USD (each mode keeps to its own cap: Predict locks no USDT for a resting order)."""
        return float(self.preopen.risk()) if self.preopen_active() else 0.0

    def preopen_pnl(self) -> float:
        """The pre-open mode's accounted P&L of the day, counted with the model trader's against LIVE_MAX_DAILY_LOSS_USD: while
        it runs for real, its day as it stands (a gain offsets the model trader's losses); while it does not — crashed (alive
        False, kept until the next apply), LIVE≠on, switched off and cleared, off since a restart — its day's realised loss
        alone, a gain counting 0."""
        if self.preopen_active():
            return float(self.preopen.daily_pnl())
        # Audit PRE-PNL-DROP: this was 0.0 whenever the mode was not running (1.125.0 gated the day on preopen_active together
        # with the reservation), so a crash, /preopen off (wound down, then cleared) or a restart with the mode off took the
        # day's realised pre-open loss out of the stop: the model trader got its whole LIVE_MAX_DAILY_LOSS_USD back, and the
        # account's real loss of the Beijing day could reach the cap plus that loss. A booked loss stays booked; what ends
        # with the mode is its reservation (preopen_risk) and its exclusive hold (preopen_exclusive). Floored at 0: a stopped
        # mode's gain never loosens the stop (it counted 0 before), so no state counts less loss than it used to. Records
        # that cannot be read count 0, as before (logged).
        preopen = getattr(self, "preopen", None)
        try:
            day = float(preopen.daily_pnl()) if preopen is not None else self.preopen_file_day()
        except Exception as error:
            LOG.warning("预挂当日盈亏读取失败（日亏损停机线这次只算原策略）：%s", core.clean_error(error) or type(error).__name__)
            return 0.0
        return min(0.0, day)

    def preopen_file_day(self) -> float:
        """Today's (Beijing) accounted P&L in the pre-open mode's ledger file while no controller holds it (Audit PRE-PNL-DROP):
        read once and kept for the day — the loss checks run every step, every sync and every send, the file is never pruned
        (about a second's read at 20,000 records), and nothing writes it while no controller runs; preopen_start and
        preopen_stop drop the figure. A file that cannot be read counts 0 (the stop as it was) and is tried again a minute
        on, not at every check."""
        from preopen import read_rows
        path, day, now = self.config.preopen.db, core.beijing_day(self.market.now_ms() / 1000), time.monotonic()
        kept = getattr(self, "preopen_day_read", None)
        if kept is not None and kept[:2] == (path, day) and now < kept[3]:
            return kept[2]
        try:
            value, until = sum(float(p.get("pnl") or 0) for p in read_rows(path) if p.get("accounted_day") == day), math.inf
        except Exception as error:
            LOG.warning("预挂记录读取失败（日亏损停机线暂只算原策略，一分钟后再读）：%s", core.clean_error(error) or type(error).__name__)
            value, until = 0.0, now + 60
        self.preopen_day_read = (path, day, value, until)
        return value

    def preopen_rows(self) -> list[dict]:
        """The pre-open mode's ledger records: the controller's (running or stopped), else the file; [] when neither reads."""
        from preopen import read_rows
        try:
            return list(self.preopen.ledger.rows.values()) if self.preopen is not None else read_rows(self.config.preopen.db)
        except Exception:
            return []

    def preopen_unverified(self) -> set[tuple[str, int]]:
        """The outcomes where a pre-open order's fills are not read for certain yet (preopen.unverified_outcomes): a position
        there may be its fill — the positions check leaves them alone until the mode has read them (1.131.5)."""
        from preopen import unverified_outcomes
        return unverified_outcomes(self.preopen_rows(), self.market.now_ms())

    def sim_analysis(self, trades: list[dict], marks: Any, flt: Any = None) -> dict:
        out = super().sim_analysis(trades, marks, flt)
        if not flt:  # the filter bar picks the model trader's records; the pre-open mode's rows stand beside them unfiltered
            from preopen import cards_today
            rows = self.preopen_rows()
            if rows:
                out["cards_today"] = [*out.get("cards_today", []), *cards_today(rows, self.market.now_ms())]  # 1.131.5: the 49¢ pairs in 今日按市场
        return out

    def preopen_filled(self, days: int = 30) -> dict[tuple[str, int], float]:
        """The pre-open mode's fills by outcome (preopen.fills_by_outcome): recorded shares to the positions reconciliation —
        the controller's ledger, or the file while the mode is off or stopped (1.131.2: whatever the mode's state, its fill
        is never a position for the model trader to book onto one of its own orders). The windows of the last ``days``
        days (1.131.3: 30, was 3 — an older fill still held, unredeemed or lost, read as a position nobody recorded)."""
        from preopen import fills_by_outcome, read_rows
        if self.preopen is not None:
            return dict(self.preopen.filled_by_outcome(days))  # its ledger, running or stopped
        try:
            return fills_by_outcome(read_rows(self.config.preopen.db), self.market.now_ms(), days)
        except Exception as error:
            LOG.warning("预挂成交读取失败：%s", core.clean_error(error) or type(error).__name__)
            return {}

    async def cmd_preopen(self, req: core.Request) -> str:
        action = req.args[0].lower() if req.args else ""
        if action == "weekend" and len(req.args) > 1 and req.args[1].lower() in {"on", "off"}:  # 1.138.2: PREOPEN_WEEKEND from Telegram
            result = self.control_set({"PREOPEN_WEEKEND": req.args[1].lower()})
            if not result["ok"]:
                return "❌ " + result["message"]
            await self.preopen_apply()
            return (result["message"] + "\n" + ("周末不挂：北京时间周六 00:00 到周一 08:00 开始的场次不挂，已挂的撤掉，周一 08:00 起恢复" if req.args[1].lower() == "off"
                                                 else "周末照挂") + ("\n" + self.preopen.status() if self.preopen is not None else ""))
        if action in {"on", "off"}:  # saved on the control page's record and applied at once, as the page would
            result = self.control_set({"PREOPEN_15M": action})
            if not result["ok"]:
                return "❌ " + result["message"]
            await self.preopen_apply()
            return result["message"] + "\n" + (self.preopen.status() if self.preopen is not None else "开盘前预挂模式已关闭")
        if action == "probe":  # the listing's fields and the windows it would place now, read-only: run before PREOPEN_15M=on
            from preopen import Settings, probe, safe_api
            try:
                settings = self.preopen.settings if self.preopen is not None else Settings.from_config(self.config, force=True)
            except ValueError as error:
                return f"设置有误：{core.clean_error(error)}"
            api = (self.preopen.cancel_api if self.preopen is not None
                   else safe_api(API_BASES[self.config.live_chain], self.config.predict_api_key, self.live.wallet, None, self.live.api))
            from preopen import PROBE_BUDGET_S, PROBE_HOURS, PROBE_PER_CARD
            try:
                hours = min(24.0, max(0.5, float(req.args[1]))) if len(req.args) > 1 else PROBE_HOURS
            except ValueError:
                hours = PROBE_HOURS

            async def later() -> None:  # 1.130.6: in the background — the commands are answered one after another, and this one reads for seconds
                try:
                    text = "\n".join(await probe(api, self.config, settings, self.market.now_ms(), hours))
                except Exception as error:
                    text = f"探测失败：{core.clean_error(error) or type(error).__name__}"
                await self.reply(req, text)
            asyncio.create_task(later(), name="preopen-probe")
            return (f"探测中：{len(settings.markets)} 张卡，各问最近 {PROBE_PER_CARD} 个刻钟（{hours:g} 小时内），最多 {PROBE_BUDGET_S:g} 秒，结果随后发来"
                    "（/preopen probe 6 = 看 6 小时内）")
        if self.preopen is None:
            return "开盘前预挂模式未启用（PREOPEN_15M=off）；/preopen probe 只读探测市场列表的字段和现在会挂的场次，/preopen on 开启（也可在控制台改）"
        if action == "check":  # what is wrong now, in one message (1.130.1)
            return self.preopen.check()
        if action in {"pause", "resume"}:
            self.preopen.manual_pause = self.preopen.ledger.pause(action == "pause")
            if action == "pause" and (line := await self.preopen.withdraw_all("管理员暂停开盘前模式")):
                return line + "\n" + self.preopen.status()
        elif action:
            return "用法：/preopen｜/preopen check｜/preopen on｜/preopen off｜/preopen pause｜/preopen resume｜/preopen probe"
        return self.preopen.status()

    async def run(self) -> int:
        await self.preopen_apply()  # PREOPEN_15M as the environment and the control page's saved values say at the start
        try:
            return await self._run_live()
        finally:
            self.stopping.set()
            await self.preopen_stop("进程停止", keep=True)  # the orders stay on the book: the signed expiry is the backstop, the next process takes them over

    def live_redeem_loop_start(self) -> None:
        """The redeem's own timer (live_redeem_loop), one per process, started by _run_live whatever LIVE says at the start."""
        task = getattr(self, "live_redeem_loop_task", None)
        if task is None or task.done():
            self.live_redeem_loop_task = asyncio.create_task(self.live_redeem_loop(), name="live-redeem-loop")

    async def _run_live(self) -> int:
        if self.config.live_mode == "off":
            LOG.info("真实交易模块已加载（LIVE=off，只记账）：/live mode pause|on 切换")
            # 1.150.0 (audit RDM-LOOP-NOT-STARTED): the redeem's own timer in this mode too. It was started only below, after a
            # start in pause / on had signed in, so a process started in off — the documented first start: LIVE=off in the
            # environment, no mode saved yet — and switched to pause / on while running (/live mode, the control page) had none
            # until its next restart: its rounds came from the trading step alone, whose call (the end of live_sync) a failing
            # open-order read skips, and the winners waited for as long as that read failed. Here it idles: live_redeem_tick's
            # gate keeps an off process that is not signed in from signing in or reading anything (no request, no round); the
            # rounds run once a switch to pause / on (or a command that signs in) has made it ready. Never started above the
            # off test: at a start in pause / on its first round would race live_prepare's sign-in (a second account check and
            # a stray ✅ 登录成功 push ahead of 💰 真实交易已启动) — that start keeps its order below.
            self.live_redeem_loop_start()
            return await super().run()
        lines = await self.live_prepare()
        if self.config.live_mode == "pause":
            LOG.warning("LIVE=pause：已登录 Predict，可自检、挂单测试、撤单；不开新仓。%s", "｜".join(lines))
            self.live_notify("🧪 真实交易处于 LIVE=pause：不开新仓，可自检和挂单测试；确认无误后把 LIVE 改成 on。\n" + "\n".join(lines), key="start")
        else:
            LOG.warning("LIVE=on：模拟交易的决定会真实下单。%s", "｜".join(lines))
            self.live_notify("💰 真实交易已启动\n" + "\n".join(lines), key="start")
        self.live_redeem_loop_start()  # 1.131.4: whatever the paper trader does
        return await super().run()


# --- one-off runs (python main.py --live-check / --approve) ------------------------------------------------------------------
async def live_check(config: core.Config) -> int:
    """Sign in, read the balances, the allowances, the open orders and positions; print the report. No Telegram."""
    trader = LiveTrader(config)
    print(f"签名钱包 {trader.wallet.signer}；下单账户 {trader.wallet.maker}（{trader.wallet.kind}）；{CHAIN_NAMES.get(config.live_chain)}")
    ok = True
    for name, job in (("登录", trader.sign_in), ("账户校验", trader.verify_account), ("余额", trader.refresh_balance), ("授权", trader.refresh_approvals)):
        try:
            await job()
            print(f"OK {name}")
        except Exception as error:
            ok = False
            print(f"FAIL {name}: {core.clean_error(error) or type(error).__name__}")
    for line in trader.summary_lines():
        print(line)
    if trader.ready:
        for name, job in (("开放订单", lambda: trader.api.orders("OPEN")), ("持仓", trader.api.positions)):
            try:
                rows = await job()
                print(f"{name} {len(rows)} 条")
                for row in rows[:10]:
                    print("  " + (LiveBot.position_line(row) if name == "持仓" else core.brief_error(json.dumps(row, ensure_ascii=False), 200)))
            except Exception as error:
                ok = False
                print(f"FAIL {name}: {core.clean_error(error) or type(error).__name__}")
    try:
        for yb in (False, True):  # 1.150.0: both tracks; yield-bearing markets settle on their own exchanges
            for label, done in await trader.chain.check_approvals(yb):
                print(f"{'OK' if done else 'MISSING'} 授权 {label}")
    except Exception as error:
        ok = False
        print(f"FAIL 授权读取: {core.clean_error(error) or type(error).__name__}")
    return 0 if ok else 1


async def live_approve(config: core.Config, yield_bearing: bool | None = None) -> int:
    """Send the approvals the exchange needs (USDT allowance, outcome-token operator), one transaction each, from the
    signing wallet (gas in BNB). Idempotent: what is in place is skipped. 1.150.0: both tracks by default (None) — the
    standard and the yield-bearing exchanges — since the bot trades yield-bearing markets too and `--approve` had no
    way to set those; True / False still does just one track."""
    trader = LiveTrader(config)
    print(f"下单账户 {trader.wallet.maker}（{trader.wallet.kind}）；gas 由 {trader.wallet.signer} 支付；{CHAIN_NAMES.get(config.live_chain)}")
    try:
        bnb = await trader.chain.bnb_balance()
        print(f"BNB 余额 {from_wei(bnb):.5f}")
        if bnb == 0:
            print("签名钱包没有 BNB，无法支付 gas：先转入约 0.005 BNB")
            return 1
    except Exception as error:
        print(f"FAIL 读取 BNB 余额: {core.clean_error(error) or type(error).__name__}")
        return 1
    results = []
    for yb in ((False, True) if yield_bearing is None else (yield_bearing,)):
        results += await trader.chain.set_approvals(yb, log=print)
    for label, outcome in results:
        print(f"{label}：{outcome}")
    return 0 if all(not outcome.startswith("失败") for _, outcome in results) else 1

