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
TAKER_ORDER_SECONDS = 300  # a taker's LIMIT order expires five minutes after it is built (its rest is cancelled sooner)
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
        try:
            return await self.call(method, path, payload, timeout, priority)
        except core.RemoteError as error:
            if http_status(error) != 401 or not self.wallet:
                raise
            self.jwt = ""
            await self.ensure_auth()
            return await self.call(method, path, payload, timeout, priority)

    async def ensure_auth(self) -> None:
        if self.jwt and (not self.jwt_exp or time.time() < self.jwt_exp - 300):
            return
        await self.authenticate()

    async def authenticate(self) -> str:
        """The sign-in flow: a message to sign, its signature, a JWT. Raises (and records why) when it fails."""
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
        """POST /v1/orders/remove -> {"removed": [...], "noop": [...]}."""
        answer = await self.authed("POST", "/orders/remove", {"data": {"ids": [str(i) for i in ids]}}, priority=True)
        data = answer.get("data") if isinstance(answer, dict) and isinstance(answer.get("data"), dict) else answer
        if not isinstance(answer, dict) or answer.get("success") is False:
            raise core.RemoteError("撤单被拒绝：" + (core.http_error_text(answer) if isinstance(answer, dict) else "")
                                   or core.brief_error(json.dumps(answer, ensure_ascii=False), 120))
        return {"removed": [str(i) for i in (data.get("removed") or [])], "noop": [str(i) for i in (data.get("noop") or [])]}

    ORDERS_PAGE = 50      # orders per page (the API pages with first / after and answers a cursor)
    ORDERS_PAGES_MAX = 20  # ...and this many pages at most in one read (1000 orders)

    async def paged(self, path: str, query: dict, stop_ids: set | None = None, max_pages: int | None = None) -> tuple[list[dict], bool]:
        """Every page of a list (first / after, the cursor the answer gives): (rows, complete). Not complete when the
        page cap was reached with a cursor still on offer — the caller must not take the list for the whole truth — or
        when the read stopped on purpose (1.137.1): ``stop_ids`` all seen, or ``max_pages`` read."""
        rows, after, seen = [], "", set()
        pages = self.ORDERS_PAGES_MAX if not max_pages else max(1, min(self.ORDERS_PAGES_MAX, int(max_pages)))
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
class TxNotSent(core.RemoteError):
    """A transaction that never left the bot: its nonce or gas price could not be read, or its gas not estimated (a
    revert in simulation — a market Predict shows resolved before the chain does). Nothing spent: tried again soon."""


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
            text = data["error"].get("message") if isinstance(data["error"], dict) else str(data["error"])
            self.error = f"RPC 错误：{core.brief_error(str(text), 120)}"
            raise core.RemoteError(self.error)
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

    async def send(self, to: str, data: bytes, value: int = 0) -> str:
        """A transaction from the signing wallet (to the Kernel account, which executes it, for a Predict account);
        the hash once the node accepted it."""
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
            except core.RemoteError as error:
                raise TxNotSent(str(error), error.retry_after) from error  # nothing sent, nothing spent
            gas = int(str(estimate), 16) * 125 // 100
            tx = {"chainId": self.wallet.chain_id, "nonce": nonce, "gasPrice": gas_price, "gas": gas, "to": to,
                  "value": value, "data": "0x" + data.hex()}
            raw = self.wallet.sign_transaction(tx)
            return str(await self.rpc("eth_sendRawTransaction", ["0x" + raw.hex()]))

    async def wait(self, tx_hash: str, timeout: float = 120.0) -> dict:
        """The receipt once mined; RemoteError when it reverted or did not come in time."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            receipt = await self.rpc("eth_getTransactionReceipt", [tx_hash])
            if isinstance(receipt, dict) and receipt.get("blockNumber"):
                if int(str(receipt.get("status") or "0x0"), 16) != 1:
                    raise core.RemoteError(f"交易失败（已回滚）：{tx_hash}")
                return receipt
            await asyncio.sleep(3)
        raise core.RemoteError(f"交易 {tx_hash} 在 {int(timeout)} 秒内未上链")

    def approval_steps(self, yield_bearing: bool) -> list[tuple[str, str, str]]:
        """(label, kind, spender) for one market track, as the SDK's setApprovals sets them."""
        prefix = "YIELD_BEARING_" if yield_bearing else ""
        ct, ex, nex, adapter = (prefix + k for k in ("CONDITIONAL_TOKENS", "CTF_EXCHANGE", "NEG_RISK_CTF_EXCHANGE", "NEG_RISK_ADAPTER"))
        word = "收益型" if yield_bearing else ""
        return [(f"{word}结果代币 → 交易所", f"1155:{ct}", ex), (f"{word}结果代币 → 多结果交易所", f"1155:{ct}", nex),
                (f"{word}结果代币 → 多结果适配器", f"1155:{ct}", adapter),
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

    async def redeem(self, condition_id: str, index_set: int, amount: int, neg_risk: bool, yield_bearing: bool) -> str:
        """Redeem a resolved position: the conditional tokens contract for a standard market (index set 1 or 2), the
        multi-outcome adapter (an amount per outcome) for a neg-risk one. The transaction hash once mined."""
        condition = bytes.fromhex(condition_id.removeprefix("0x"))
        if neg_risk:
            adapter = self.addresses[("YIELD_BEARING_" if yield_bearing else "") + "NEG_RISK_ADAPTER"]
            amounts = [amount, 0] if index_set == 1 else [0, amount]
            tx = await self.send(adapter, self.encode("redeemPositions(bytes32,uint256[])", ["bytes32", "uint256[]"], [condition, amounts]))
        else:
            tokens = self.addresses[("YIELD_BEARING_" if yield_bearing else "") + "CONDITIONAL_TOKENS"]
            tx = await self.send(tokens, self.encode("redeemPositions(address,bytes32,bytes32,uint256[])",
                                                     ["address", "bytes32", "bytes32", "uint256[]"],
                                                     [self.addresses["USDT"], bytes(32), condition, [index_set]]))
        await self.wait(tx)
        return tx


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
    of the outcomes rather than read from their names. The card prices the 涨 / Yes / high-barrier side first; its 跌
    is the other outcome, bought as its own token (never a sale of the first)."""
    if len(outcomes) != 2 or not all(o.get("token") for o in outcomes):
        return None, "市场不是两个结果，或缺少结果代币编号"
    names = [str(o.get("name") or "").strip().lower() for o in outcomes]
    if kind == "touch" and touch_spec is not None:
        want = "high" if side == "up" else "low"
        hits = [o for o, n in zip(outcomes, names) if core.touch_outcome(n, touch_spec) == want]
        return (hits[0], "") if len(hits) == 1 else (None, f"结果名称无法对应触价线：{'、'.join(names)}")
    ups = [o for o, n in zip(outcomes, names) if n in UP_WORDS]
    downs = [o for o, n in zip(outcomes, names) if n in DOWN_WORDS]
    if side == "up":
        if len(ups) == 1:
            return ups[0], ""
        if len(downs) == 1:
            return next(o for o in outcomes if o is not downs[0]), ""
    else:
        if len(downs) == 1:
            return downs[0], ""
        if len(ups) == 1:
            return next(o for o in outcomes if o is not ups[0]), ""
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
            self.account_error = f"无法在链上核对 PREDICT_ACCOUNT 的控制钥匙：{core.clean_error(error) or type(error).__name__}"
            self.account_checked = False  # not verified: not ready, whatever the sign-in said
            raise core.RemoteError(self.account_error) from None
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
            keys = ["CTF_EXCHANGE", "NEG_RISK_CTF_EXCHANGE"]
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

    def summary_lines(self) -> list[str]:
        c = self.config
        lines = [f"钱包：{self.wallet.kind}｜下单账户 {short_addr(self.wallet.maker)}"
                 + (f"｜签名钱包 {short_addr(self.wallet.signer)}" if self.wallet.predict_account else "")
                 + f"｜{CHAIN_NAMES.get(c.live_chain, c.live_chain)}",
                 ("API：已登录" + (f"（token 至 {core.stamp(self.api.jwt_exp * 1000, seconds=False)}）" if self.api.jwt_exp else "")
                  if not self.ready_error else f"API：未就绪：{self.ready_error}")
                 + ("" if c.predict_api_key else "｜未设置 PREDICT_API_KEY")]
        if self.wallet.predict_account:
            lines.append("账户校验：" + (f"✗ {self.account_error}" if self.account_error else "✓ 私钥是这个 Predict 账户的控制钥匙"))
        if self.balance:
            lines.append(f"余额：USDT {from_wei(self.balance[0]):,.2f}｜BNB {from_wei(self.balance[1]):.4f}（gas）"
                         f"｜读取于 {core.stamp(self.balance[2] * 1000)}")
        else:
            lines.append(f"余额：未读到（{self.balance_error or '尚未读取'}）")
        if self.approvals:
            lines.append("授权：交易所 USDT 额度 " + ("✓" if self.approvals.get("CTF_EXCHANGE") else "✗")
                         + "｜多结果交易所 " + ("✓" if self.approvals.get("NEG_RISK_CTF_EXCHANGE") else "✗")
                         + ("" if all(self.approvals.values()) else "（python main.py --approve，或在网站交易一次）"))
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
            rows.append({"k": "账户校验", "v": f"✗ {self.account_error}" if self.account_error else "✓ 私钥是这个 Predict 账户的控制钥匙",
                         "tone": "bad" if self.account_error else "ok"})
        if self.balance:
            rows.append({"k": "余额", "v": f"USDT {from_wei(self.balance[0]):,.2f}，BNB {from_wei(self.balance[1]):.4f}（gas），"
                                          f"读取于 {core.stamp(self.balance[2] * 1000)}", "tone": ""})
        else:
            rows.append({"k": "余额", "v": f"未读到（{self.balance_error or '尚未读取'}）", "tone": "warn"})
        if self.approvals:
            ok = all(self.approvals.values())
            rows.append({"k": "授权", "v": "交易所 USDT 额度 " + ("✓" if self.approvals.get("CTF_EXCHANGE") else "✗")
                         + "，多结果交易所 " + ("✓" if self.approvals.get("NEG_RISK_CTF_EXCHANGE") else "✗")
                         + ("" if ok else "（python main.py --approve，或在网站交易一次）"), "tone": "ok" if ok else "bad"})
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
    LIVE_REDEEM_READS = 40      # markets read a redeem round at most, the newest positions first (the rest the next round): never a flood on the rate limit the orders share
    LIVE_REDEEM_RETRY_SECONDS = 3600  # a position whose redeem failed (gas, a revert) is tried again after this long, not every look...
    LIVE_REDEEM_UNSENT_RETRY_SECONDS = 300  # ...one whose transaction never left (TxNotSent: not resolved on-chain yet, an RPC blip) after this long
    LIVE_REDEEM_TICK_SECONDS = 5     # the automatic redeem's own timer (live_redeem_loop): rounds whatever the paper trader does
    LIVE_REDEEM_NOTE_SECONDS = 3600  # a failed redeem reaches Telegram at most this often
    REDEEM_REPORT_LINES = 6          # /live redeem check: the positions listed per kind (the rest counted)
    LIVE_REDEEM_MIN_BNB_WEI = 5 * 10 ** 14  # under 0.0005 BNB no redeem goes out (one costs about 0.0002): one note, not a failure per position
    LIVE_NOTE_SECONDS = 600     # the same failure is announced at most this often
    LIVE_PLACING_GRACE_MS = 120_000  # a record left in "placing" is reconciled by its hash after this long
    LIVE_IDLE_LIST_MS = 60_000      # the open-order list is re-read this often even with nothing in flight (orders this bot did not place)
    LIVE_SNAPSHOT_MAX_AGE_MS = 600_000  # the pre-open mode's account snapshot stands in for a cut read of the open list while younger than this (1.136.2)
    LIVE_STEP_PAGES = 1             # a step's own read of the open list: the pages until every active record's order is seen, this many at most (1.137.1:
    #                                 3; 1.138.2: 1 — under the pre-open mode's thousands of orders the model's older ones are never on the first pages, the
    #                                 two more pages cost 0.6 s a step for nothing: they are read by their hash in rotation)
    LIVE_TRACK_READS = 6            # resting maker orders the bounded list did not show, read by their hash a step at most, the longest unread first (1.137.2)
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
        self.live_last_sync = 0.0
        self.live_last_redeem = -1e9
        self.live_last_balance = -1e9
        self.live_redeem_task: asyncio.Task | None = None   # the auto-redeem round, beside the step (chain calls never hold a decision)
        self.live_account_task: asyncio.Task | None = None  # the balances / approvals refresh, likewise
        self.live_redeem_failed: dict[str, float] = {}      # position key -> monotonic time its redeem failed
        self.live_redeem_lock = asyncio.Lock()               # one redeem round at a time (the command beside the automatic round)
        self.live_redeem_last: dict = {}                     # the last round (automatic or /live redeem): when, what it found, or why it could not look
        self.live_redeem_errors: dict[str, str] = {}         # position key -> why its last redeem failed (/live redeem check)
        self.live_redeem_noted = -1e9                        # the last failed redeem told on Telegram (LIVE_REDEEM_NOTE_SECONDS)
        self.live_redeem_loop_task: asyncio.Task | None = None
        self.live_gas_noted = -1e9
        self.live_step_ms: dict[str, int] = {}  # the last step's phases (预取 / 决策 / 下单 / 同步), for /diag
        self.live_fill_tasks: set = set()   # the fills reads of orders confirmed removed, beside the lane (live_fills_later)
        self.live_placing: set[str] = set()     # records whose order is being sent right now: the step and the express lane never both
        self.live_last_signin = -1e9
        self.live_positions: tuple[list[dict], float] = ([], 0.0)
        self.live_positions_cut = False  # the last positions read for a reconciliation came cut at the page cap
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
        self.live_full_cut = False  # that read came cut at the page cap: while the pre-open mode runs, its snapshot stands for the account instead (1.137.2)
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
            return f"未授权{'多结果' if info.neg_risk else ''}交易所使用 USDT（python main.py --approve）"
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
        today = dt.datetime.fromtimestamp(now_ms / 1000, core.BEIJING).replace(hour=0, minute=0, second=0, microsecond=0)
        since = int(today.timestamp() * 1000)
        return sum((float(t["payout"]) - float(t["price"])) * float(t["shares"]) for t in trades.values()
                   if isinstance(t.get("live"), dict) and t.get("status") == "settled" and int(t.get("settled") or 0) >= since)

    def live_check_kill(self, trades: dict[str, dict], now_ms: int) -> None:
        cap = self.config.live_max_daily_loss_usd
        if not cap or self.live_kill_record():
            return  # once a day: tripped already, or resumed by hand (the losses are still there)
        pnl = self.live_daily_pnl(trades, now_ms) + self.preopen_pnl()  # the pre-open mode's day counted too (1.125.0)
        if pnl <= -cap:
            why = f"今日已结算亏损 {money(pnl)} 达到 LIVE_MAX_DAILY_LOSS_USD ${cap:g}"
            self.store.put("live:killed", {"day": core.beijing_day(now_ms / 1000), "why": why, "at": now_ms})
            for tid, t in trades.items():
                if isinstance(t.get("live"), dict) and t["status"] == "resting" and t["live"].get("state") in {"open", "placing", "unknown"}:
                    self.sim_withdraw(t, "触发日亏损上限", now_ms)
                    self.sim_save([(tid, t)])
            self.live_notify(f"🛑 {why}；今天不再开新仓，已请求撤掉挂单（以 Predict 的确认为准）。/live resume 可手动恢复。", key="kill")

    # --- opening: the paper record becomes an order to place ----------------------------------------------------------
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
                "expires": (end // 1000 + 7200) if end and end // 1000 + 7200 > now_ms // 1000 + 120 else FAR_FUTURE}
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
            self.live_redeem_soon()  # a real position settled in its favour: redeemed within seconds, not at the next ten-minute round
        if isinstance(st, dict) and st.get("state") in {"pending", "placing", "open", "unknown"} and trade["status"] in {"expired", "settled"}:
            st["state"], st["cancel_why"] = "cancelling", "市场已出结果"
            st["events"].append({"at": now_ms, "what": "撤单：市场已出结果"})

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
            await self.live_sync(now_ms)
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
            if unsent and self.live.ready:
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
        resumed = bool(self.live_kill_record().get("resumed"))  # /live resume after today's stop: the operator's call stands
        if (cap := c.live_max_daily_loss_usd) and not resumed and (pnl := self.live_daily_pnl(trades, now_ms) + self.preopen_pnl()) <= -cap:
            return f"今日已结算亏损 {money(pnl)} 已达 LIVE_MAX_DAILY_LOSS_USD ${cap:g}"  # recomputed now, not a flag saved a step ago
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
        """The records whose cancel is decided but not sent yet (state cancelling, an order id known), by tid."""
        return {tid: t for tid, t in self.sim_trades().items() if tid not in skip and isinstance(t.get("live"), dict)
                and t["live"].get("state") == "cancelling" and not t["live"].get("cancel_sent") and tid not in self.live_placing
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
            if records and self.live.ready:
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
        (withdrawn_at): the 撤单→重挂 span of live_latency."""
        if self.config.live_mode == "off" or self.config.sim_ways == "taker" or not tids:
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
                strategy, expiration = "LIMIT", int(now_ms // 1000) + TAKER_ORDER_SECONDS
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
            result = await self.live.api.create_order(body)
            took = time.monotonic() - sent
            st["answered_at"] = st["sent_at"] + int(took * 1000)
        except Exception as error:
            tid = st.get("rekeyed") or tid  # 1.138.0: withdrawn and moved (sim_rekey) while it waited: it lives there now — never saved back at the slot
            text = core.clean_error(error) or type(error).__name__
            definite = isinstance(error, OrderRejected) or 400 <= http_status(error) < 500
            # 1.122.0: an answer that the order already exists (a resend taken twice, a hash Predict holds) is Predict having it,
            # not a refusal: the record is "result unknown" and the sync adopts the order by its hash, nothing is freed
            duplicate = any(w in text.lower() for w in ("already exist", "already placed", "duplicate", "exists"))
            limited = isinstance(error, core.RemoteError) and (http_status(error) == 429 or (error.retry_after and http_status(error) == 0))
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
        for k in ("sent_at", "answered_at"):
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
        st["rekeyed"] = self.live_rekey(tid, trade)
        wait = self.config.live_retry if retry is None else retry
        self.live_backoff[(trade["market"], trade["side"], "挂" if trade.get("maker") else "吃")] = (time.monotonic() + wait, why)
        self.live_note_error(f"{trade['item']} {trade['label']}：{why}")
        if wait:
            self.live_notify(f"⚠️ {trade['item']} {trade['label']} {why}；{wait} 秒后再看。", key=f"fail:{trade['market']}:{trade['side']}:{'挂' if trade.get('maker') else '吃'}")

    def live_rekey(self, tid: str, trade: dict) -> str:
        """Move a record to tid#n, freeing the paper trader's one slot per market, side and way."""
        return self.sim_rekey(tid, trade)

    # --- reading back: fills, cancels, the final state of each order ---------------------------------------------------
    async def live_sync(self, now_ms: int) -> None:
        trades = self.sim_trades()
        self.live_check_kill(trades, now_ms)  # first, and never behind the open-order read: a list that cannot be read must not keep the switch from tripping
        active = {tid: t for tid, t in trades.items() if isinstance(t.get("live"), dict) and t["live"].get("state") in PLACING
                  and t["live"].get("state") != "pending"}
        if active and not self.live.ready:
            await self.live_ensure_ready()  # real orders are followed to their end whatever the mode (LIVE=off included)
        if not self.live.ready:
            return
        await self.live_renew_auth()
        if active or now_ms - self.live_list_at_ms >= self.LIVE_IDLE_LIST_MS:
            cancelling = [tid for tid, t in active.items() if t["live"].get("state") == "cancelling"]
            await self.live_sync_orders(active, now_ms)  # with nothing in flight the list is still read now and then: orders this bot did not place
            await self.live_requote_freed(cancelling, now_ms)  # the slots the confirmed cancels freed: quoted again now, not next step
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
        if not self.config.live_auto_redeem or (self.config.live_mode == "off" and not self.live.ready):
            return
        if time.monotonic() - self.live_last_redeem < self.LIVE_REDEEM_SECONDS or (self.live_redeem_task is not None and not self.live_redeem_task.done()):
            return
        self.live_last_redeem = time.monotonic()
        self.live_redeem_task = asyncio.create_task(self.live_redeem_round(self.market.now_ms() if now_ms is None else now_ms), name="live-redeem")

    async def live_redeem_loop(self) -> None:
        """The automatic redeem's own timer (1.131.4): it rode on the paper trader's step only, so with SIM=off (the
        pre-open mode alone) or the step held up, nothing was ever redeemed."""
        while not self.stopping.is_set():
            try:
                self.live_redeem_tick()
            except Exception as error:
                self.live_note_error(f"自动领取计时出错：{core.clean_error(error) or type(error).__name__}")
            await self.wait(self.LIVE_REDEEM_TICK_SECONDS)

    async def live_redeem_round(self, now_ms: int) -> None:
        try:
            if not self.live.ready:
                await self.live_ensure_ready()  # signed in by the step otherwise: with the step off, here
                if not self.live.ready:
                    self.live_redeem_last = {"at": now_ms, "error": f"真实交易未就绪：{self.live.not_ready_why}"}
                    return
            await self.live_redeem(now_ms)
        except Exception as error:
            text = core.clean_error(error) or type(error).__name__
            self.live_redeem_last = {"at": now_ms, "error": f"读取持仓失败：{text}"}
            self.live_note_error(f"领取结算失败：{text}")

    def live_redeem_soon(self) -> None:
        """A real position just settled in the bot's favour (the pre-open mode's window, a record on Predict's final
        result): the next automatic redeem round within LIVE_REDEEM_SOON_SECONDS, not up to LIVE_REDEEM_SECONDS later —
        the USDT back on the account for the next quotes (1.131.3)."""
        self.live_last_redeem = min(self.live_last_redeem, time.monotonic() - self.LIVE_REDEEM_SECONDS + self.LIVE_REDEEM_SOON_SECONDS)

    async def live_sync_orders(self, active: dict[str, dict], now_ms: int) -> None:
        # 1.137.1: the step reads the pages until every active record's order is seen (LIVE_STEP_PAGES at most) whenever the pre-open
        # snapshot stands for the whole account, or within a minute of this bot's own whole read; the rest of the records are read by
        # their hash (live_track). The 11-07 /diag: 同步 10.5 s of an 11.3 s step, twenty pages against the pre-open mode's thousands
        wanted = {str(t["live"].get("order_id")) for t in active.values() if t["live"].get("order_id")}
        bounded = (self.live_snapshot_fresh(now_ms) or now_ms - self.live_full_at_ms < self.LIVE_IDLE_LIST_MS
                   or (self.live_full_cut and getattr(self, "preopen", None) is not None))  # 1.137.2: a cut full read tells nothing the snapshot will not
        listing = time.monotonic()
        try:
            open_rows = await (self.live.api.orders("OPEN", stop_ids=wanted, max_pages=self.LIVE_STEP_PAGES) if bounded else self.live.api.orders("OPEN"))
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
            if st.get("rekeyed") and st["rekeyed"] != tid:
                return  # moved to an attempt record (live_fail): saved there, never again under the old key
            if json.dumps(trade, sort_keys=True, default=str) != before:
                self.sim_save([(tid, trade)])
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
        """(market id -> name, outcome token -> name) of every market this bot trades or traded for real: the markets
        priced for its orders, its records, the pre-open mode's windows."""
        ids: dict[str, str] = {}
        tokens: dict[str, str] = {}
        for mid, info in getattr(self.live, "markets", {}).items():
            ids[str(mid)] = str(mid)
            for o in getattr(info, "outcomes", None) or []:
                if o.get("token"):
                    tokens[str(o["token"])] = str(mid)
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
        try:
            result = await self.live.api.remove_orders(ids)
        except Exception as error:
            self.live_note_error(f"撤无记录挂单失败：{core.clean_error(error) or type(error).__name__}")
            return
        removed = [i for i in ids if i in set(result.get("removed") or [])]
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
            if st.get("rekeyed") and st["rekeyed"] != tid:
                return  # moved to an attempt record (a taker's live_fail): saved there, never again under the old key (1.138.1, as live_sync_orders)
            if json.dumps(trade, sort_keys=True, default=str) != before:
                self.sim_save([(tid, trade)])
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
        of the end (the positions list follows the matching a little late);
        (b) 1.119.1: an outcome the account holds a little less of than its open records say (Predict takes the taker
        fee in shares: 97.74 bought, 96.30 held) — the records are trimmed to the holding, in proportion, when no order
        with fills is still working there, the shortfall is within LIVE_TRIM_MAX and the same shortfall stands a minute
        later (the positions list follows the matching a little late); a position's payout is then exactly the shares
        held (the cost, price with the fee times the shares bought, stays right within a cent). A list that cannot be
        read, or came cut at the page cap, changes nothing."""
        if now_ms - self.live_late_checked < self.LIVE_LATE_FILLS_MS:
            return
        self.live_late_checked = now_ms
        trades = self.sim_trades()
        key_of = lambda t: (str((t["live"].get("want") or {}).get("market_id") or t.get("market_id") or ""), int((t["live"].get("want") or {}).get("index_set") or 0))
        live = [(tid, t) for tid, t in trades.items() if isinstance(t.get("live"), dict)]
        ended = [(tid, t) for tid, t in live  # 1.122.0: a settled record too — a fill read after the result still cost money (the day's loss stop counts it)
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
            for tid, t in recs:
                before = float(t["shares"])
                after = round(before * scale, 6)
                t["shares"] = after
                t["fee_shares"] = round(float(t.get("fee_shares") or 0) + before - after, 6)
                if after > 0:
                    t["price"] = round(float(t["price"]) * before / after, 6)  # 1.122.0: the money spent is the same, over fewer shares
                t["live"]["events"].append({"at": now_ms, "what": f"按 Predict 持仓校正份数：{before:g} → {after:g}（账户在这一边共持有 {got:g} 份，比记录的 {total:g} 份少 {short:g} 份：手续费以份额扣除）"})
                self.sim_save([(tid, t)])
            self.live_notify(f"📐 份数校正：{recs[0][1]['item']} {recs[0][1]['label']}：账户持有 {got:g} 份，记录 {total:g} 份，少的 {short:g} 份是手续费以份额扣除；"
                             f"{len(recs)} 笔记录按比例改为持有的份数（成本不变）", key=f"trim:{k[0]}:{k[1]}")

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
        try:
            result = await self.live.api.remove_orders([st["order_id"]])
        except Exception as error:
            st["last_error"] = core.clean_error(error) or type(error).__name__
            return  # the removal could not be asked for: again next look, the order stays reserved meanwhile
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
        if not st.get("cancel_sent"):
            st.setdefault("cancel_sent_at", self.market.now_ms())  # the first request's time, off the clock (live_latency: 决定→撤单发出)
            result = await self.live.api.remove_orders([st["order_id"]])
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
        it was built; a maker's at the expiry it was signed with, or two hours after the market's end — 0 when
        nothing bounds it (then only Predict's word or an operator ends an unknown record)."""
        st, want = trade["live"], trade["live"].get("want") or {}
        if not trade.get("maker"):
            start = int(st.get("placed_at") or st.get("placing_at") or trade.get("opened") or 0)
            return start + TAKER_ORDER_SECONDS * 1000 if start else 0
        expires = int(want.get("expires") or 0)
        end = core.sim_end_ms(trade.get("settle"))
        bounds = [b for b in ((expires * 1000 if 0 < expires < FAR_FUTURE else 0), (end + 2 * 3600_000 if end else 0)) if b]
        return min(bounds) if bounds else 0

    async def live_recheck(self, tid: str, trade: dict, by_id: dict, by_hash: dict, mk: Any, now_ms: int) -> None:
        """An unknown record, every step: found on the list or by its hash, it is tracked again (or finished on a final
        word). Missing from LIVE_UNKNOWN_LOOKS complete open lists, unknown to the hash read and unknown for
        LIVE_UNKNOWN_SETTLE_MS, the account's positions decide (1.104.9): nothing beyond what the other records explain
        and it ends unfilled, its slot and reservation freed; a surplus is what it filled. Before, only its expiry plus a
        margin ended it — a daily market's maker order expires two hours after the close, so the 11-04 长鑫 跌 side sat
        blocked from 16:46 for a day ("状态未知、对账中"). The expiry path stays for a record the positions cannot
        settle (unreadable, or the list cut)."""
        st = trade["live"]
        order = float(trade.get("order") or 0)
        row = by_id.get(str(st.get("order_id") or "-")) or by_hash.get(str(st.get("hash") or "-").lower())
        if row is None and (st.get("hash") or st.get("order_id")):
            try:
                row = await self.live.api.order(str(st.get("order_id") or ""), str(st.get("hash") or ""))
            except Exception as error:
                st["last_error"] = core.clean_error(error) or type(error).__name__
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
        if not expired and self.live_unknown_open:
            # an order on the account this bot has no record of may be this very one under a hash it did not expect: an
            # operator looks first (/live release, or /live cancel account), nothing is released on top of it
            st["last_error"] = (f"连续 {st['unknown_misses']} 次完整列表没有它，但账户上还有 {len(self.live_unknown_open)} 张本机器人没有记录的挂单"
                                f"（#{'、#'.join(u['id'] for u in self.live_unknown_open[:3])}）：可能就是这张，先核对；核对后 /live release 或 /live cancel account")
            return
        if not expired and now_ms - int(st.get("unexplained_at") or 0) < self.LIVE_UNEXPLAINED_EVERY_MS:
            return  # the positions are read for it once a minute at most
        st["unexplained_at"] = now_ms
        # nothing can fill it any more (expired), or nothing says it exists (settled) — but it may have filled while it could
        # not be found (Predict's hash of it differing from the one computed here, its answer lost): the account's position on
        # that outcome beyond what the other records explain is what it filled; the absence of a record never is
        extra = await self.live_unexplained(trade)
        head = (f"订单的有效期已过（{core.stamp(deadline, seconds=False)}）" if expired
                else f"连续 {st['unknown_misses']} 次完整的开放订单列表里没有它、按哈希也查不到，状态未知已 {unknown_ms // 60_000} 分钟")
        if extra is None:
            st["last_error"] = head + ("，但持仓列表没读全（超过分页上限）：下次再核对（额度照旧占用）" if self.live_positions_cut
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
        shares on the same market and outcome): what an order that cannot be found may have filled. None when the
        positions cannot be read now, or not whole (live_positions_cut)."""
        self.live_positions_cut = False
        want = trade["live"].get("want") or {}
        mid, index_set = str(want.get("market_id") or trade.get("market_id") or ""), int(want.get("index_set") or 0)
        if not mid or not index_set:
            return None
        try:
            rows = await self.live.api.positions()
        except Exception:
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
        explained = 0.0
        for other in self.sim_trades().values():
            if other is trade or not isinstance(other.get("live"), dict):
                continue
            o_want = other["live"].get("want") or {}
            if str(o_want.get("market_id") or other.get("market_id") or "") == mid and int(o_want.get("index_set") or 0) == index_set:
                explained += float(other.get("shares") or 0)
        return max(0.0, held - explained)

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
            if row is None or (at_ms is not None and at_ms < core.trade_updated(t)):
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
        average price it states replaces the expected cost."""
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
    REDEEM_KINDS = {"redeemed": "领了", "claimable": "可领取", "failed": "领取失败", "waiting": "失败等重试", "fields": "缺字段",
                    "kept": "已领仍列着", "open": "未结算", "lost": "输掉的", "unread": "没读到市场"}

    def redeem_lock(self) -> asyncio.Lock:
        lock = getattr(self, "live_redeem_lock", None)
        if lock is None:
            lock = self.live_redeem_lock = asyncio.Lock()
        return lock

    async def live_redeem(self, now_ms: int) -> list[str]:
        """Positions on markets Predict has resolved: the winning outcome's shares are redeemed for USDT on-chain
        (once per market and outcome); the lines say what was done. A position whose redeem failed waits
        LIVE_REDEEM_RETRY_SECONDS; with no BNB for gas nothing is tried and the administrator hears once an hour. A
        position resolved against the bot is marked settled (``lost``) and never asked about again (1.131.3: each one
        cost a market read every round, and the pre-open mode leaves one or more a filled window — hundreds a day
        against the rate limit its orders and cancels share); a market is read once a round whatever outcomes are held,
        at most LIVE_REDEEM_READS markets a round, the newest positions first (the rest the next round), and a rate limit
        ends the round. One round at a time: the command and the automatic round never redeem one position twice."""
        async with self.redeem_lock():
            done, _ = await self.live_redeem_pass(now_ms)
            return done

    async def live_redeem_pass(self, now_ms: int, dry_run: bool = False) -> tuple[list[str], list[dict]]:
        """One look at the account's positions, the newest first: (the redeemed lines, each position judged). Kinds:
        redeemed (now), claimable (resolved in its favour, not redeemed now: ``dry_run``, or no gas), failed (this
        round), waiting (a failed redeem's back-off), lost, open (not resolved), unread (its market not read: the round's
        cap, a failed read, a rate limit), fields (no index set on the position, no condition on the market), kept
        (redeemed before, still listed). ``dry_run`` (/live redeem check): nothing sent, written or announced."""
        rows = await self.live.api.positions()
        self.live_positions = (rows, time.time())
        done: list[str] = []
        seen: list[dict] = []
        balance = self.live.balance
        if not dry_run and (balance is None or len(balance) < 3 or time.time() - float(balance[2]) > self.LIVE_BALANCE_SECONDS):
            with contextlib.suppress(Exception):  # the paper trader's step keeps it fresh; with that step off, here (1.131.4)
                await self.live.refresh_balance()
            balance = self.live.balance
        low_gas = balance is not None and int(balance[1]) < self.LIVE_REDEEM_MIN_BNB_WEI
        markets: dict[str, dict | None] = {}  # this round's reads, one per market (None: the read failed)
        read_errors: dict[str, str] = {}
        limited = False
        errors = self.__dict__.setdefault("live_redeem_errors", {})
        for row in sorted(rows, key=lambda r: -self.position_time(r)):  # the newest first: a capped round reaches the fresh wins
            market = row.get("market") if isinstance(row.get("market"), dict) else {}
            outcome = row.get("outcome") if isinstance(row.get("outcome"), dict) else {}
            mid = str(market.get("id") or row.get("marketId") or "")
            try:
                amount = int(str(row.get("amount") or "0"))
            except ValueError:
                amount = 0
            if not mid or amount <= 0:
                continue
            index_set = int(outcome.get("indexSet") or 0)
            key = f"live:redeemed:{mid}:{index_set}"
            entry = {"title": str(market.get("title") or market.get("question") or mid), "outcome": str(outcome.get("name") or index_set or "?"),
                     "shares": from_wei(amount), "market_id": mid, "index_set": index_set}
            mark = self.store.get(key)
            if mark:
                seen.append({**entry, "kind": "lost" if mark.get("lost") else "kept", "tx": str(mark.get("tx") or "")})
                continue
            failed_at = self.live_redeem_failed.get(key)
            if failed_at is not None and time.monotonic() - failed_at < self.LIVE_REDEEM_RETRY_SECONDS:
                seen.append({**entry, "kind": "waiting", "left": self.LIVE_REDEEM_RETRY_SECONDS - (time.monotonic() - failed_at), "error": errors.get(key, "")})
                continue
            if not index_set:
                seen.append({**entry, "kind": "fields", "error": "持仓没写 indexSet，分不出是哪一边"})
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
            raw = markets[mid]
            if raw is None:
                seen.append({**entry, "kind": "unread", "error": read_errors.get(mid, "")})
                continue
            info = parse_market(raw, self.config.predict_fee_bps)
            resolved = core.predict_resolution(raw)
            if not core.PredictFeed.market_settled({"status": info.status}) or resolved is None:
                seen.append({**entry, "kind": "open", "status": info.status or "?"})
                continue
            won = resolved.get("split") or (resolved.get("index") is not None and index_set == int(resolved["index"]) + 1)
            if not won:
                if resolved.get("index") is not None and not dry_run:  # resolved against it: worth nothing, never read again
                    self.store.put(key, {"at": now_ms, "lost": True, "amount": str(amount), "title": entry["title"]})
                seen.append({**entry, "kind": "lost"})
                continue
            entry["value"] = entry["shares"] * (0.5 if resolved.get("split") else 1.0)
            if not info.condition_id:
                seen.append({**entry, "kind": "fields", "error": "市场没写 conditionId，领取交易填不出来"})
                continue
            if dry_run or low_gas:
                seen.append({**entry, "kind": "claimable"})
                continue
            try:
                tx = await self.live.chain.redeem(info.condition_id, index_set, amount, info.neg_risk, info.yield_bearing)
            except Exception as error:
                unsent = isinstance(error, TxNotSent)  # never left (1.131.3): soon again — the chain may be a moment behind Predict's result
                wait = self.LIVE_REDEEM_UNSENT_RETRY_SECONDS if unsent else self.LIVE_REDEEM_RETRY_SECONDS
                text = core.clean_error(error) or type(error).__name__
                self.live_redeem_failed[key] = time.monotonic() - self.LIVE_REDEEM_RETRY_SECONDS + wait
                errors[key] = text
                self.live_note_error(f"领取 {entry['title']} 失败：{text}" + ("（交易没发出，5 分钟后再试）" if unsent else "（一小时后再试）"))
                seen.append({**entry, "kind": "failed", "error": text, "unsent": unsent})
                continue
            self.store.put(key, {"at": now_ms, "tx": tx, "amount": str(amount), "title": entry["title"]})
            errors.pop(key, None)
            line = f"💵 已领取 {entry['title']} {outcome.get('name') or ''} {from_wei(amount):g} 份：{tx}"
            done.append(line)
            seen.append({**entry, "kind": "redeemed", "tx": tx})
            self.live_notify(line)
        if dry_run:
            return done, seen
        claimable = [s for s in seen if s["kind"] == "claimable"]
        if low_gas and claimable and time.monotonic() - self.live_gas_noted >= self.LIVE_REDEEM_RETRY_SECONDS:
            self.live_gas_noted = time.monotonic()
            text = (f"⛽ 签名钱包的 BNB 不足 0.0005，{len(claimable)} 个已结算的持仓（约 ${sum(s['value'] for s in claimable):,.2f}）等着领取：暂不自动领取。"
                    f"往 {self.live.wallet.signer} 转一点 BNB（BNB 链）后自动恢复")
            self.live_note_error(text)
            self.live_notify(text, key="redeem:gas")  # 1.131.4: told on Telegram, not only in the log
        failed = [s for s in seen if s["kind"] == "failed"]
        if failed and time.monotonic() - getattr(self, "live_redeem_noted", -1e9) >= self.LIVE_REDEEM_NOTE_SECONDS:
            self.live_redeem_noted = time.monotonic()
            self.live_notify(f"⚠️ 自动领取：{len(failed)} 个已结算的持仓没领成（{failed[0]['title']}：{failed[0]['error']}），"
                             f"{'5 分钟' if failed[0].get('unsent') else '一小时'}后自动再试。/live redeem check 查看原因", key="redeem:fail")
        counts: dict[str, int] = {}
        for s in seen:
            counts[s["kind"]] = counts.get(s["kind"], 0) + 1
        self.live_redeem_last = {"at": now_ms, "counts": counts, "low_gas": low_gas, "error": ""}
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
        return (f"持仓 {sum(counts.values())} 个" + (f"：{text}" if text else "")
                + ("；签名钱包 BNB 不足，领取交易发不出去" if last.get("low_gas") and counts.get("claimable") else ""))

    def live_redeem_status_line(self) -> str:
        """/live: the automatic redeem in a line — on or off, the last round, whether one is running."""
        if not self.config.live_auto_redeem:
            return "自动领取：关（/live redeem 手动领取）"
        last = getattr(self, "live_redeem_last", {}) or {}
        task = getattr(self, "live_redeem_task", None)
        when = f"上一轮 {max(0, int((self.market.now_ms() - int(last['at'])) / 60_000))} 分钟前：{self.live_redeem_summary(last)}" if last.get("at") else "本次启动后还没跑过"
        return f"自动领取：开｜{when}" + ("｜正在领取" if task is not None and not task.done() else "") + "｜/live redeem check 查原因"

    async def live_redeem_report(self, now_ms: int) -> str:
        """/live redeem check, read-only (nothing sent, written or announced): the switches, the signing wallet's gas,
        the last round, the account's positions by what the redeem makes of each (a dry run of live_redeem_pass), a
        conclusion, the recent redeem problems (1.131.4)."""
        c, wallet = self.config, self.live.wallet
        if not self.live.ready:
            await self.live_ensure_ready()
        ready = self.live.ready
        lines = [f"🔎 自动领取自检 {core.stamp(now_ms, seconds=False)}",
                 f"开关：自动领取 {'开' if c.live_auto_redeem else '关'}｜LIVE={c.live_mode}｜" + ("已就绪" if ready else f"未就绪：{self.live.not_ready_why}"),
                 self.live_redeem_status_line()]
        try:
            bnb = await self.live.chain.bnb_balance()
        except Exception as error:
            bnb = None
            lines.append(f"签名钱包 {wallet.signer}（付 gas）：BNB 读取失败：{core.clean_error(error) or type(error).__name__}")
        else:
            lines.append(f"签名钱包 {wallet.signer}（付 gas）：BNB {from_wei(bnb):.5f}"
                         + ("｜⛽ 不足 0.0005，领取交易发不出去" if bnb < self.LIVE_REDEEM_MIN_BNB_WEI else "｜够付 gas"))
        if wallet.predict_account:
            lines.append(f"持仓账户 {wallet.maker}（Predict 账户：领取由签名钱包经它的 Kernel 合约执行，领到的 USDT 进这个账户）")
        if not ready:
            lines.append("结论：真实交易未就绪，读不了持仓，也领不了：" + self.live.not_ready_why)
            return "\n".join(lines)
        async with self.redeem_lock():
            _, seen = await self.live_redeem_pass(now_ms, dry_run=True)
        n = lambda kind: sum(1 for s in seen if s["kind"] == kind)
        value = sum(float(s.get("value") or 0) for s in seen if s["kind"] == "claimable")
        lines.append(f"持仓 {len(seen)} 个：" + ("，".join(f"{label} {n(k)}" for k, label in self.REDEEM_KINDS.items() if n(k)) or "没有"))
        listed = {"claimable": ("可领取", self.REDEEM_REPORT_LINES), "waiting": ("领取失败、等重试", self.REDEEM_REPORT_LINES),
                  "fields": ("缺字段", self.REDEEM_REPORT_LINES), "kept": ("已发过领取交易、Predict 还列着", 3), "open": ("未结算", 3), "unread": ("这次没读到市场", 2)}
        for kind, (label, cap) in listed.items():
            items = [s for s in seen if s["kind"] == kind]
            if not items:
                continue
            lines.append(f"{label}：")
            for s in items[:cap]:
                extra = {"claimable": f"，约 ${float(s.get('value') or 0):,.2f}",
                         "waiting": f"：{s.get('error') or '?'}（{max(1, int(float(s.get('left') or 0) / 60))} 分钟后再试）",
                         "fields": f"：{s.get('error')}", "kept": f"（交易 {s.get('tx', '')[:14]}…）",
                         "open": f"（市场状态 {s.get('status')}）", "unread": f"：{s.get('error')}"}[kind]
                lines.append(f"  · {s['title']} / {s['outcome']} {s['shares']:g} 份{extra}")
            if len(items) > cap:
                lines.append(f"  …另有 {len(items) - cap} 个")
        low = bnb is not None and bnb < self.LIVE_REDEEM_MIN_BNB_WEI
        waiting = [s for s in seen if s["kind"] == "waiting"]
        reasons = []  # the gas first: without it neither the automatic round nor /live redeem can send anything
        if (n("claimable") or waiting) and low:
            reasons.append(f"签名钱包的 BNB 不够付 gas，领取交易发不出去：往 {wallet.signer} 转一点 BNB（BNB 链，比如 0.005），到账后下一轮自动领取")
        if n("claimable") and not c.live_auto_redeem:
            reasons.append(f"自动领取关着，{n('claimable')} 个可领取（约 ${value:,.2f}）不会自己领：控制台“高级”里打开“自动领取已结算持仓”，或发 /live redeem 手动领")
        if reasons:
            why = "；".join(reasons)
        elif waiting:
            why = f"链上领取失败：{waiting[0].get('error') or '见下方'}；到时自动再试，一直失败就把这条发我"
        elif n("kept"):
            why = "有持仓已经发过领取交易并上链，Predict 还列着：可能只是列表没更新；过 10 分钟还在，就把这条发我"
        elif n("fields"):
            why = "有已结算的持仓缺字段，没法领取：把这条发我"
        elif n("claimable"):
            why = f"有 {n('claimable')} 个可领取（约 ${value:,.2f}），下一轮自动领取会领；急用就发 /live redeem"
        elif n("unread"):
            why = "有市场这次没读到（限流或一轮读满），下一轮继续看"
        elif n("open"):
            why = "现在没有可领取的：赢的都领完了，剩下的还没结算，结算后自动领取"
        else:
            why = "现在没有可领取的：赢的都领完了，其余是输掉的"
        lines.append("结论：" + why)
        recent = [e for e in self.live_errors if "领取" in e or "⛽" in e][-3:]
        if recent:
            lines.append("最近的领取问题：\n" + "\n".join("  " + e for e in recent))
        return "\n".join(lines)

    # --- telling the administrator ------------------------------------------------------------------------------------------
    def live_notify(self, text: str, key: str = "") -> None:
        """A line to every active subscription (the administrator's chats); a keyed notice repeats at most every
        LIVE_NOTE_SECONDS."""
        if not self.config.live_notify or self.telegram is None:
            return
        if key:
            if time.monotonic() - self.live_noted.get(key, -1e9) < self.LIVE_NOTE_SECONDS:
                return
            self.live_noted[key] = time.monotonic()
        for sub_id, sub in self.subscriptions().items():
            if not sub.get("active"):
                continue
            self.live_seq += 1

            async def send(sub: dict = sub, text: str = text) -> None:
                await self.tell(sub["chat"], sub["thread"], text)
            self.deliver(f"live:{self.live_seq}:{sub_id}", send)

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
                await self.reply(req, answer if isinstance(answer, str) else getattr(answer, "text", str(answer)))
            asyncio.create_task(later(), name=f"live-{action}")
            if action == "redeem" and words[1:2] == ["check"]:
                return "领取自检中，结果随后发来"
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
            if record := self.live_kill_record():
                self.store.put("live:killed", {**record, "resumed": True, "resumed_at": now_ms})
            return "▶️ 真实交易已恢复：满足条件的建议会重新下单。"
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
            if not self.live.ready:
                await self.live_ensure_ready()
                if not self.live.ready:
                    return f"未就绪：{self.live.not_ready_why}"
            lines = await self.live_redeem(now_ms)
            if lines:
                return "\n".join(lines)
            last = self.live_redeem_last
            head = "这次没有领取" if (last.get("counts") or {}).get("claimable") else "没有可领取的已结算持仓"
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
            self.live_notify(f"⚪ 真实交易模式 {before} → off：回到只记账，已请求撤掉未成交的真实挂单（以 Predict 的确认为准），已有持仓继续跟踪到结算。")
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
                "exposure": self.live_exposure(trades), "daily_pnl": self.live_daily_pnl(trades, now_ms),
                "caps": {"order": c.live_max_order_usd, "open": c.live_max_open_usd, "daily_loss": c.live_max_daily_loss_usd},
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

    async def live_test(self, args: list[str], now_ms: int) -> str:
        """The real order path checked end to end with a dollar or so locked for minutes at most: a LIMIT buy far
        under the market (it rests; it is withdrawn at once; it expires in ten minutes by itself), then the open-order
        list, the cancel and the final state, each step reported with what Predict answered. Arguments: a market name
        to pick it, a share count, a price in cents (a price at or above the best bid may trade)."""
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
            first = await self.live_action("pause", ["紧急停止"], now_ms)
            second = await self.live_cancel_command(["all"], now_ms)
            if getattr(self, "preopen", None) is not None:
                await self.preopen.cancel_all("紧急停止")
                second += "\n" + self.preopen.status()
            text = f"{first}\n{second}"
        elif action in {"resume", "redeem", "check", "orders", "positions"}:
            text = await self.live_action(action, [], now_ms)
        else:
            return {"ok": False, "message": "未知操作"}
        message = text if isinstance(text, str) else text.text
        failed = message.startswith(("❌", "未就绪", "用法")) or "失败" in message.split("\n")[0]  # "nothing to do" is not a failure
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
                await preopen.cancel_all("管理员撤单")
                result = await self._live_cancel_command(words, now_ms)
                return result + "\n已请求撤销预挂单，预挂模式已暂停。\n" + preopen.status()
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
            extra = [str(r.get("id")) for r in await self.live.api.orders("OPEN") if str(r.get("id")) not in known]
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
            try:
                result = await self.live.api.remove_orders(extra)
                self.live_unknown_open = [u for u in self.live_unknown_open if u["id"] not in set(result["removed"])]
                extra_text = f"账户上无记录的挂单 {len(extra)} 张：Predict 确认撤掉 {len(result['removed'])} 张" + (
                    f"，{len(result['noop'])} 张称无需撤" if result["noop"] else "")
            except Exception as error:
                extra_text = f"账户上无记录的挂单 {len(extra)} 张：撤单请求失败（{core.clean_error(error)}）"
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
        sent = sum(s.get("state") == "cancelling" and bool(s.get("cancel_sent")) for s in states)
        unsent = sum(s.get("state") == "cancelling" and not s.get("cancel_sent") for s in states)
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
        return (f"Predict 上的持仓 {len(lines)} 个：{len(lines) - missing - differ - dust} 个与记录一致，{differ} 个份数不符，{missing} 个无记录"
                + (f"，{dust} 个零头" if dust else "") + "\n" + "\n".join(lines))

    def live_text(self, now_ms: int) -> str:
        trades = self.sim_trades()
        live = [t for t in trades.values() if isinstance(t.get("live"), dict)]
        state = ("⚪ LIVE=off：只记账（/live mode pause 登录测试，/live mode on 真实下单）" if self.config.live_mode == "off"
                 else "🧪 LIVE=pause：只测试，不开新仓" + ("" if self.live.ready else "｜未就绪：" + self.live.not_ready_why) if self.config.live_mode == "pause"
                 else "⏸ 已暂停（" + self.live_paused() + "）" if self.live_paused() else "🛑 今日停开新仓（" + self.live_killed() + "）"
                 if self.live_killed() else ("🟢 运行中" if self.live.ready else "🔴 未就绪：" + self.live.not_ready_why))
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
        lines.append(f"持仓+挂单 ${exposure:,.2f} / ${self.config.live_max_open_usd:g}｜今日已结算盈亏 {money(self.live_daily_pnl(trades, now_ms))}"
                     f"（上限 −${self.config.live_max_daily_loss_usd:g}）｜挂单中 {resting}｜今日下单 {placed_today}｜失败 {failed_today}")
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
        lines.append("\n/live mode off|pause|on｜pause｜resume｜cancel all｜redeem｜redeem check｜check｜orders｜positions｜test [市场] [份数] [价格¢]")
        if getattr(self, "preopen", None) is not None:
            lines.append("\n" + core.html.escape(self.preopen.status()) + "\n/preopen 查看预挂；紧急停止会撤销本模式订单")
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
                 else "已暂停" if self.live_paused() else "今日停开新仓" if self.live_killed()
                 else ("运行中" if self.live.ready else f"未就绪：{self.live.not_ready_why}"))
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
        mode is off; None when nothing is recorded."""
        from preopen import read_rows, review
        try:
            rows = list(self.preopen.ledger.rows.values()) if self.preopen is not None else read_rows(self.config.preopen.db)
        except Exception as error:
            LOG.warning("预挂复盘读取失败：%s", core.clean_error(error) or type(error).__name__)
            return None
        return review(rows, self.market.now_ms()) if rows else None

    async def preopen_apply(self) -> None:
        """The pre-open mode as PREOPEN_15M and the PREOPEN_* settings say now (the environment, the control page's saved
        values over it): started when on and not running, stopped when off (its orders withdrawn), restarted when its
        settings changed — the orders stay on the book, the new controller takes over those the settings still describe
        and withdraws the rest — or when it crashed."""
        async with self.preopen_lock:
            want = self.config.preopen
            if self.preopen is not None and (not want.enabled or self.preopen.settings != want or not self.preopen.alive):
                await self.preopen_stop("设置改变" if want.enabled else "PREOPEN_15M=off", keep=want.enabled)
            if want.enabled and self.preopen is None and not self.stopping.is_set():
                self.preopen_start()

    def preopen_start(self) -> None:
        from preopen import Controller
        preopen = self.preopen = (self.preopen_factory or Controller)(self)
        task = self.preopen_task = asyncio.create_task(preopen.run(), name="preopen")

        def finished(t: asyncio.Task) -> None:
            if not t.cancelled() and t.exception() is not None:
                text = core.clean_error(t.exception()) or type(t.exception()).__name__
                LOG.error("开盘前策略异常结束：%s", text)
                # the bot goes on (its run() withdrew this mode's orders on the way out); the model trader is free again
                preopen.error, preopen.alive = f"预挂模式已停止：{text}", False
                self.live_note_error(f"开盘前预挂模式异常停止：{text}")
                self.live_notify(f"⚠️ 开盘前预挂模式异常停止：{text}（本模式的挂单已撤，重启后恢复）", key="preopen:crash")
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
        return self.preopen_active() and bool(self.config.preopen.exclusive)

    def preopen_risk(self) -> float:
        """The money the running pre-open mode may have tied up (Controller.risk). Shown; since 1.132.1 not counted against
        LIVE_MAX_OPEN_USD (each mode keeps to its own cap: Predict locks no USDT for a resting order)."""
        return float(self.preopen.risk()) if self.preopen_active() else 0.0

    def preopen_pnl(self) -> float:
        """The running pre-open mode's accounted P&L of the day, counted with the model trader's against LIVE_MAX_DAILY_LOSS_USD."""
        return float(self.preopen.daily_pnl()) if self.preopen_active() else 0.0

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
            return (result["message"] + "\n" + ("周末不挂：北京时间周六、周日开始的场次不挂，已挂的撤掉，周一 00:00 起恢复" if req.args[1].lower() == "off"
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
            if action == "pause":
                await self.preopen.cancel_all("管理员暂停开盘前模式")
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

    async def _run_live(self) -> int:
        if self.config.live_mode == "off":
            LOG.info("真实交易模块已加载（LIVE=off，只记账）：/live mode pause|on 切换")
            return await super().run()
        lines = await self.live_prepare()
        if self.config.live_mode == "pause":
            LOG.warning("LIVE=pause：已登录 Predict，可自检、挂单测试、撤单；不开新仓。%s", "｜".join(lines))
            self.live_notify("🧪 真实交易处于 LIVE=pause：不开新仓，可自检和挂单测试；确认无误后把 LIVE 改成 on。\n" + "\n".join(lines), key="start")
        else:
            LOG.warning("LIVE=on：模拟交易的决定会真实下单。%s", "｜".join(lines))
            self.live_notify("💰 真实交易已启动\n" + "\n".join(lines), key="start")
        self.live_redeem_loop_task = asyncio.create_task(self.live_redeem_loop(), name="live-redeem-loop")  # 1.131.4: whatever the paper trader does
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
        for label, done in await trader.chain.check_approvals(False):
            print(f"{'OK' if done else 'MISSING'} 授权 {label}")
    except Exception as error:
        ok = False
        print(f"FAIL 授权读取: {core.clean_error(error) or type(error).__name__}")
    return 0 if ok else 1


async def live_approve(config: core.Config, yield_bearing: bool = False) -> int:
    """Send the approvals the exchange needs (USDT allowance, outcome-token operator), one transaction each, from the
    signing wallet (gas in BNB). Idempotent: what is in place is skipped."""
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
    results = await trader.chain.set_approvals(yield_bearing, log=print)
    for label, outcome in results:
        print(f"{label}：{outcome}")
    return 0 if all(not outcome.startswith("失败") for _, outcome in results) else 1

