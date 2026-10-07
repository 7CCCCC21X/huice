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
    price, qty = retain_sig(price_wei, 3), retain_sig(qty_wei, 5)
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


class PredictApi:
    """Signed-in requests to Predict's REST API. The API key goes in x-api-key on every request; the JWT from the
    sign-in flow (GET /v1/auth/message → sign → POST /v1/auth) in Authorization, renewed before it expires or after a
    401. A 429 pauses every request for its retry-after."""
    PARALLEL = 3
    BACKOFF_SECONDS = 20

    def __init__(self, base: str, api_key: str, wallet: Wallet | None):
        self.base, self.api_key, self.wallet = base.rstrip("/"), api_key, wallet
        self.jwt, self.jwt_exp, self.jwt_at = "", 0, 0.0
        self.auth_error = ""
        self.gate = asyncio.Semaphore(self.PARALLEL)
        self.blocked_until = 0.0
        self.calls = 0

    def headers(self) -> dict[str, str]:
        out = {}
        if self.api_key:
            out["x-api-key"] = self.api_key
        if self.jwt:
            out["Authorization"] = f"Bearer {self.jwt}"
        return out

    async def call(self, method: str, path: str, payload: dict | None = None, timeout: int = API_TIMEOUT) -> Any:
        left = self.blocked_until - time.monotonic()
        if left > 0:
            raise core.RemoteError(f"Predict 接口限流冷却中（{int(left) + 1} 秒后重试）", int(left) + 1)
        if method == "POST" and payload is None:
            payload = {}
        self.calls += 1
        async with self.gate:
            try:
                raw = await core._blocking(core._http_get, self.base + path, payload, timeout, self.headers())
            except core.RemoteError as error:
                if error.retry_after or http_status(error) == 429:
                    self.blocked_until = max(self.blocked_until, time.monotonic() + max(error.retry_after, self.BACKOFF_SECONDS))
                raise
        try:
            return json.loads(raw) if raw else {}
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise core.RemoteError("Predict 接口未返回有效 JSON") from None

    async def authed(self, method: str, path: str, payload: dict | None = None, timeout: int = API_TIMEOUT) -> Any:
        """A request that needs the sign-in: signs in first when the token is missing or about to expire, once more
        after a 401."""
        await self.ensure_auth()
        try:
            return await self.call(method, path, payload, timeout)
        except core.RemoteError as error:
            if http_status(error) != 401 or not self.wallet:
                raise
            self.jwt = ""
            await self.ensure_auth()
            return await self.call(method, path, payload, timeout)

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
                raise core.RemoteError("登录消息格式异常：" + core.brief_error(json.dumps(data, ensure_ascii=False), 80))
            answer = await self.call("POST", "/auth", {"signer": self.wallet.maker, "signature": self.wallet.sign_text(message),
                                                      "message": message})
            token = str(((answer or {}).get("data") or {}).get("token") or "")
            if not token:
                raise core.RemoteError("登录未返回 token：" + core.brief_error(json.dumps(answer, ensure_ascii=False), 80))
        except core.RemoteError as error:
            self.auth_error = core.clean_error(error)
            raise
        self.jwt, self.jwt_exp, self.jwt_at, self.auth_error = token, jwt_expiry(token), time.time(), ""
        return token

    @staticmethod
    def rows(data: Any) -> list[dict]:
        """The records of a list answer ({success, data: [...]}, {data: {items: [...]}} or a bare list)."""
        if isinstance(data, dict):
            data = data.get("data", data)
        if isinstance(data, dict):
            for key in ("items", "orders", "positions", "results", "rows"):
                if isinstance(data.get(key), list):
                    data = data[key]
                    break
        return [row for row in data if isinstance(row, dict)] if isinstance(data, list) else []

    async def market(self, market_id: str) -> dict:
        data = await self.call("GET", f"/markets/{core.urllib.parse.quote(str(market_id))}")
        market = data.get("data", data) if isinstance(data, dict) else {}
        if not isinstance(market, dict) or not market:
            raise core.RemoteError("Predict 市场详情格式异常")
        return market

    async def create_order(self, body: dict) -> dict:
        """POST /v1/orders -> {"order_id", "hash", "code"}; success=false is an error with the API's own reason."""
        answer = await self.authed("POST", "/orders", body, timeout=20)
        data = answer.get("data") if isinstance(answer, dict) else None
        if not isinstance(answer, dict) or answer.get("success") is False or not isinstance(data, dict):
            raise core.RemoteError("下单被拒绝：" + (core.http_error_text(answer) if isinstance(answer, dict) else "")
                                   or core.brief_error(json.dumps(answer, ensure_ascii=False), 120))
        order_id = str(data.get("orderId") or data.get("id") or data.get("order_id") or "")
        if not order_id:
            raise core.RemoteError("下单应答没有订单号：" + core.brief_error(json.dumps(answer, ensure_ascii=False), 120))
        return {"order_id": order_id, "hash": str(data.get("orderHash") or data.get("hash") or ""), "code": data.get("code")}

    async def remove_orders(self, ids: list[str]) -> dict:
        """POST /v1/orders/remove -> {"removed": [...], "noop": [...]}."""
        answer = await self.authed("POST", "/orders/remove", {"data": {"ids": [str(i) for i in ids]}})
        data = answer.get("data") if isinstance(answer, dict) and isinstance(answer.get("data"), dict) else answer
        if not isinstance(answer, dict) or answer.get("success") is False:
            raise core.RemoteError("撤单被拒绝：" + (core.http_error_text(answer) if isinstance(answer, dict) else "")
                                   or core.brief_error(json.dumps(answer, ensure_ascii=False), 120))
        return {"removed": [str(i) for i in (data.get("removed") or [])], "noop": [str(i) for i in (data.get("noop") or [])]}

    async def orders(self, status: str = "OPEN", **params: Any) -> list[dict]:
        query = core.urllib.parse.urlencode({"status": status, **{k: v for k, v in params.items() if v not in (None, "")}})
        return self.rows(await self.authed("GET", f"/orders?{query}"))

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
        return self.rows(await self.authed("GET", "/positions"))


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
        return (ordered_shares if status == "FILLED" else 0.0), status
    return min(ordered_shares, ordered_shares * filled / amount), status


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
class Chain:
    """The few on-chain calls real trading needs, over plain JSON-RPC: USDT / BNB balances, the exchange allowances
    and operator approvals, and the transactions that set them or redeem resolved positions (from the signing wallet;
    through Kernel.execute for a Predict account)."""

    def __init__(self, rpc_url: str, wallet: Wallet):
        self.rpc_url, self.wallet = rpc_url, wallet
        self.addresses = wallet.addresses
        self.error = ""

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
        nonce = int(str(await self.rpc("eth_getTransactionCount", [sender, "pending"])), 16)
        gas_price = int(str(await self.rpc("eth_gasPrice", [])), 16)
        estimate = await self.rpc("eth_estimateGas", [{"from": sender, "to": to, "data": "0x" + data.hex(), "value": hex(value)}])
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
        self.chain = Chain(config.live_rpc or RPC_URLS[config.live_chain], self.wallet)
        self.ready_error = "尚未登录 Predict"
        self.ready_at = 0.0
        self.account_error = ""  # a Predict account the key does not control (checked on-chain): nothing is sent
        self.balance: tuple[int, int, float] | None = None  # (USDT wei, BNB wei, when)
        self.balance_error = ""
        self.approvals: dict[str, bool] = {}  # exchange key -> allowance in place (read from the chain)
        self.approvals_at = 0.0
        self.approvals_error = ""
        self.markets: dict[str, MarketInfo] = {}

    @property
    def ready(self) -> bool:
        return not self.ready_error and not self.account_error

    @property
    def not_ready_why(self) -> str:
        return self.ready_error or self.account_error

    async def verify_account(self) -> None:
        """PREDICT_ACCOUNT must be a Predict account whose signer is PREDICT_PRIVATE_KEY (the validator says who): a
        mismatch would have every order refused, so it blocks trading until fixed. A plain wallet needs no check."""
        if not self.wallet.predict_account:
            self.account_error = ""
            return
        try:
            owner = await self.chain.account_owner(self.wallet.predict_account)
        except Exception as error:
            raise core.RemoteError(f"无法在链上核对 PREDICT_ACCOUNT 的控制钥匙：{core.clean_error(error) or type(error).__name__}") from None
        if int(owner, 16) == 0:
            self.account_error = f"链上没有 Predict 账户 {short_addr(self.wallet.predict_account)} 的签名记录：地址填错，或账户还没在链上激活（先在网站交易一次）"
        elif owner.lower() != self.wallet.signer.lower():
            self.account_error = (f"PREDICT_PRIVATE_KEY（{short_addr(self.wallet.signer)}）不是 Predict 账户 {short_addr(self.wallet.predict_account)} 的控制钥匙"
                                  f"（链上记录的是 {short_addr(checksum(owner))}）：请导出该账户的 Privy 钱包私钥")
        else:
            self.account_error = ""
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
            self.approvals_at, self.approvals_error = time.time(), ""
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
        return lines


def short_addr(address: str) -> str:
    return f"{address[:6]}…{address[-4:]}" if len(address) > 12 else address


def money(x: float) -> str:
    return f"{'+' if x >= 0 else '−'}${abs(x):,.2f}"


# --- the bot: the paper trader's decisions, sent to Predict ------------------------------------------------------------
PLACING = {"pending", "placing", "open", "cancelling"}  # a trade whose order is in flight or resting


_sim_status = core.sim_status


def sim_status(trade: dict) -> str:
    """The paper trader's status words, with the real order's state for a live trade."""
    st = trade.get("live") if isinstance(trade.get("live"), dict) else None
    if not st or trade.get("status") != "resting":
        return _sim_status(trade)
    shares, order = float(trade.get("shares") or 0), float(trade.get("order") or 0)
    state = st.get("state")
    if state == "pending":
        return "真实下单准备中"
    if state == "placing":
        return "真实下单发送中（待确认）"
    way = "挂单中" if trade.get("maker") else "吃单等待成交"
    if state == "cancelling":
        way = "撤单中"
    tag = f"真实订单 #{st.get('order_id')}" if st.get("order_id") else "真实订单"
    return f"{way}（{tag} 已成交 {shares:g}/{order:g} 份）"


core.sim_status = sim_status  # the base class looks the function up by name at call time


class LiveBot(core.Bot):
    """The paper trader, with every decision sent to Predict for real (see the module docstring)."""
    LIVE_SYNC_SECONDS = 10      # open live orders are read back from Predict this often (the paper step's cadence)
    LIVE_BALANCE_SECONDS = 60   # the USDT / BNB balances are re-read this often while trading
    LIVE_APPROVALS_SECONDS = 600
    LIVE_REDEEM_SECONDS = 600   # resolved positions are looked for this often
    LIVE_NOTE_SECONDS = 600     # the same failure is announced at most this often
    LIVE_PLACING_GRACE_MS = 120_000  # a record left in "placing" is reconciled by its hash after this long
    LIVE_MISSES = 3             # an open order Predict stops listing without a final record counts as cancelled after this many looks

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
        self.live_last_signin = -1e9
        self.live_positions: tuple[list[dict], float] = ([], 0.0)
        self.live_errors: list[str] = []  # the latest failures, newest last, for /live
        self.live_wanted: set[str] = set()  # market ids a decision waited for (details read next step, whatever the edge)

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

    def live_room(self, trades: dict[str, dict], mk: core.SimMarket, side: str, price: float, shares: float,
                  taker: dict | None = None) -> str:
        c = self.config
        if c.live_mode == "off":
            return ""  # paper trading, as the base class does it
        if not self.live.ready:
            return f"真实交易未就绪：{self.live.not_ready_why}"
        if c.live_mode == "pause":
            return "LIVE=pause：只做登录、自检、挂单测试和撤单，不开新仓（改成 LIVE=on 才会下单）"
        if paused := self.live_paused():
            return f"真实交易已暂停（{paused}；/live resume 恢复）"
        if killed := self.live_killed():
            return f"今日停止开新仓：{killed}"
        held = self.live_backoff.get((mk.market, side))
        if held and time.monotonic() < held[0]:
            return f"{held[1]}，{int(held[0] - time.monotonic()) + 1} 秒后再试"
        if taker and taker.get("cap"):
            price, shares = float(taker["cap"]), float(c.sim_shares)  # the LIMIT order asks for SIM_SHARES at the cap: the most it can cost
        notional = float(price) * float(shares)
        if notional < MIN_ORDER_USD - 1e-9:
            return f"本单 ${notional:,.2f} 低于 Predict 最低订单金额 ${MIN_ORDER_USD:g}（提高 SIM_SHARES）"
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
        outcome, _ = outcome_for_side(mk.kind, side, info.outcomes, self.touches[mk.key].spec if mk.kind == "touch" and mk.key in self.touches else None)
        if outcome is None:
            return "无法确定要买的结果代币：" + outcome_for_side(mk.kind, side, info.outcomes)[1]
        key = exchange_key(info.neg_risk, info.yield_bearing)
        if self.live.approvals and key in self.live.approvals and not self.live.approvals[key]:
            return f"未授权{'多结果' if info.neg_risk else ''}交易所使用 USDT（python main.py --approve）"
        return ""

    def live_exposure(self, trades: dict[str, dict]) -> float:
        """USDT in the real positions and resting orders: filled shares at their price, plus a resting order's
        unfilled shares at its price."""
        total = 0.0
        for t in trades.values():
            if not isinstance(t.get("live"), dict) or t.get("status") not in {"filled", "resting"}:
                continue
            shares, order, price = float(t.get("shares") or 0), float(t.get("order") or 0), float(t.get("price") or 0)
            total += shares * price
            if t["status"] == "resting":
                total += max(0.0, order - shares) * price
        return total

    def live_daily_pnl(self, trades: dict[str, dict], now_ms: int) -> float:
        today = dt.datetime.fromtimestamp(now_ms / 1000, core.BEIJING).replace(hour=0, minute=0, second=0, microsecond=0)
        since = int(today.timestamp() * 1000)
        return sum((float(t["payout"]) - float(t["price"])) * float(t["shares"]) for t in trades.values()
                   if isinstance(t.get("live"), dict) and t.get("status") == "settled" and int(t.get("settled") or 0) >= since)

    def live_check_kill(self, trades: dict[str, dict], now_ms: int) -> None:
        cap = self.config.live_max_daily_loss_usd
        if not cap or self.live_kill_record():
            return  # once a day: tripped already, or resumed by hand (the losses are still there)
        pnl = self.live_daily_pnl(trades, now_ms)
        if pnl <= -cap:
            why = f"今日已结算亏损 {money(pnl)} 达到 LIVE_MAX_DAILY_LOSS_USD ${cap:g}"
            self.store.put("live:killed", {"day": core.beijing_day(now_ms / 1000), "why": why, "at": now_ms})
            for tid, t in trades.items():
                if isinstance(t.get("live"), dict) and t["status"] == "resting" and t["live"].get("state") in {"open", "placing"}:
                    self.sim_withdraw(t, "触发日亏损上限", now_ms)
                    self.sim_save([(tid, t)])
            self.live_notify(f"🛑 {why}；今天不再开新仓，挂单已撤。/live resume 可手动恢复。", key="kill")

    # --- opening: the paper record becomes an order to place ----------------------------------------------------------
    def sim_open(self, mk: core.SimMarket, side: str, now_ms: int, maker: Any = None, taker: dict | None = None) -> dict:
        trade = super().sim_open(mk, side, now_ms, maker, taker)
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
                "cost": float(trade["price"]), "token": outcome["token"], "outcome": outcome["name"],
                "index_set": outcome["index_set"], "assumed": assumed, "fee_bps": info.fee_bps,
                "neg_risk": info.neg_risk, "yield_bearing": info.yield_bearing, "condition_id": info.condition_id,
                "asks": [[p, q] for p, q in core.side_levels(mk.book, side)][:core.PREDICT_DEPTH],
                "expires": (end // 1000 + 7200) if end and end // 1000 + 7200 > now_ms // 1000 + 120 else FAR_FUTURE}
        trade.update(status="resting", shares=0.0, filled=None, fills=[], expected_price=float(trade["price"]),
                     live={"state": "pending", "want": want, "attempt": 1, "order_id": "", "hash": "", "placed_at": None,
                           "events": [{"at": now_ms, "what": "决定下单"}]})
        if taker:
            trade["queue_ahead"] = trade["queue_min"] = 0.0
        return trade

    def sim_fill(self, trade: dict, mk: core.SimMarket, now_ms: int) -> None:
        if isinstance(trade.get("live"), dict):
            return  # a real order's fills come from Predict (live_sync), never presumed from the book
        super().sim_fill(trade, mk, now_ms)

    def sim_withdraw_reason(self, trade: dict, mk: core.SimMarket | None, now_ms: int | None = None) -> str:
        if isinstance(trade.get("live"), dict) and not trade.get("maker"):
            return ""  # a taker order in flight rests only until LIVE_TAKER_WAIT_SECONDS: SIM_WAYS / SIM_MARKETS do not withdraw it
        return super().sim_withdraw_reason(trade, mk, now_ms)

    def sim_withdraw(self, trade: dict, why: str, now_ms: int) -> None:
        super().sim_withdraw(trade, why, now_ms)
        st = trade.get("live")
        if isinstance(st, dict) and st.get("state") in {"pending", "placing", "open"}:
            st["state"], st["cancel_why"] = "cancelling", why
            st["events"].append({"at": now_ms, "what": f"撤单：{why}"})

    def sim_settle(self, trade: dict, up: float, note: str, now_ms: int, by: str) -> None:
        super().sim_settle(trade, up, note, now_ms, by)
        st = trade.get("live")
        if isinstance(st, dict) and st.get("state") in {"pending", "placing", "open"} and trade["status"] in {"expired", "settled"}:
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
        if time.monotonic() - self.sim_ran < self.SIM_SECONDS:
            return False
        try:
            if self.config.live_mode != "off":
                await self.live_prefetch(now_ms)
        except Exception as error:
            self.live_note_error(f"读取市场信息失败：{core.clean_error(error) or type(error).__name__}")
        result = await super().sim_step(now_ms)
        try:
            await self.live_place_pending(now_ms)
            await self.live_sync(now_ms)
        except Exception as error:
            self.live_note_error(f"同步真实订单失败：{core.clean_error(error) or type(error).__name__}")
        return result

    async def live_prefetch(self, now_ms: int) -> None:
        """Sign in again when needed; market details for the markets whose suggestion is near the bar (a decision
        needs them in the same step); the balances now and then."""
        await self.live_ensure_ready()
        if not self.live.ready:
            return
        if time.monotonic() - self.live_last_balance >= self.LIVE_BALANCE_SECONDS:
            self.live_last_balance = time.monotonic()
            with contextlib.suppress(Exception):
                await self.live.refresh_balance()
            if time.monotonic() - self.live.approvals_at >= self.LIVE_APPROVALS_SECONDS or not self.live.approvals:
                with contextlib.suppress(Exception):
                    await self.live.refresh_approvals()
        costs, bar, c = self.edge_costs(), self.config.sim_edge - 0.03, self.config
        wanted, self.live_wanted = list(self.live_wanted), set()
        for mk in self.sim_markets(now_ms):
            if mk.hold or mk.book.stale(now_ms) or mk.kind not in c.sim_markets or core.book_crossed(mk.book):
                continue
            info = self.live.markets.get(str(mk.book.market_id))
            if info and time.monotonic() - info.at < 3600:
                continue
            near = False  # judged as the paper trader judges: the best maker, a taker on SIM_SHARES
            if mk.makers and c.sim_ways != "taker":
                maker = core.best_edge([e for e in core.book_edges(mk.fair_up, mk.book, costs) if e.maker], mk.need)
                near = maker is not None and maker.edge >= bar
            bps = mk.book.fee_bps if mk.book.fee_bps is not None else c.predict_fee_bps
            for side in ("up", "down") if c.sim_ways != "maker" else ():
                fair = mk.fair_up if side == "up" else 1 - mk.fair_up
                cap = core.taker_cap(fair, max(mk.need, bar), bps)  # a little under the paper trader's cap: read the market early
                q = core.taker_quote(mk.book, side, c.sim_shares, bps, cap) if cap > 0 else None
                near = near or (q is not None and q["got"] >= core.SIM_MIN_SHARES - 1e-9)
            if near:
                wanted.append(str(mk.book.market_id))
        for mid in list(dict.fromkeys(wanted))[:5]:
            try:
                await self.live.market(mid)
            except Exception as error:
                self.live_note_error(f"市场 {mid} 详情读取失败：{core.clean_error(error) or type(error).__name__}")

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
        trades = self.sim_trades()
        for tid, trade in trades.items():
            st = trade.get("live")
            if isinstance(st, dict) and st.get("state") == "pending" and trade["status"] == "resting":
                if self.config.live_mode == "off":
                    self.live_fail(tid, trade, "已切到 LIVE=off，未下单", now_ms)
                    continue
                await self.live_place(tid, trade, now_ms)

    async def live_place(self, tid: str, trade: dict, now_ms: int) -> None:
        """Build, sign and send the order of a pending record. The hash is saved (state "placing") before the request
        goes out: a crash in between is reconciled by the hash, never by sending again."""
        st, want = trade["live"], trade["live"]["want"]
        c, wallet = self.config, self.live.wallet
        try:
            shares_wei = to_wei(want["shares"])
            if want["maker"]:
                amounts = limit_amounts(True, to_wei(want["price"]), shares_wei)
                strategy, expiration = "LIMIT", int(want.get("expires") or FAR_FUTURE)
            else:
                # the dearest acceptable price: the book at or under it fills now, the rest waits LIVE_TAKER_WAIT_SECONDS
                amounts = limit_amounts(True, to_wei(want.get("cap") or want["price"]), shares_wei)
                strategy, expiration = "LIMIT", int(now_ms // 1000) + TAKER_ORDER_SECONDS
            order = build_order(wallet, want["token"], amounts["maker"], amounts["taker"], want["fee_bps"], expiration)
            digest, signature = wallet.sign_order(order, want["neg_risk"], want["yield_bearing"])
            body: dict = {"data": {"order": {**order, "signature": signature, "hash": digest},
                                   "pricePerShare": str(amounts["price_per_share"]), "strategy": strategy}}
            st.update(state="placing", hash=digest.lower(), strategy=strategy, order=order, placing_at=now_ms,
                      price_per_share=from_wei(amounts["price_per_share"]), usd_cap=from_wei(amounts["maker"]),
                      shares_requested=from_wei(amounts["amount"]))
            self.sim_save([(tid, trade)])
            result = await self.live.api.create_order(body)
        except Exception as error:
            text = core.clean_error(error) or type(error).__name__
            if st.get("state") == "placing" and isinstance(error, core.RemoteError) and not http_status(error) and "网络错误" in str(error):
                # the request may have reached Predict (a timeout, a dropped connection): never send it again; the sync
                # step finds the order by its hash, or gives the attempt up after the grace period
                st["last_error"] = text
                st["events"].append({"at": now_ms, "what": f"下单请求结果未知：{text}"})
                self.sim_save([(tid, trade)])
                return
            self.live_fail(tid, trade, f"下单失败：{text}", now_ms)
            return
        st.update(state="open", order_id=result["order_id"], placed_at=now_ms, code=result.get("code"))
        st["events"].append({"at": now_ms, "what": f"已下单 #{result['order_id']}"})
        self.sim_save([(tid, trade)])
        self.live_notify(f"📤 已向 Predict 下单：{trade['item']} {trade['label']} {core.cents(want['price'])}×{want['shares']:g} 份"
                         f"（{'限价挂单' if want['maker'] else '限价吃单'}，订单 #{result['order_id']}，"
                         f"最多花 ${from_wei(amounts['maker']):,.2f}）\n{self.sim_url(trade)}")

    def live_fail(self, tid: str, trade: dict, why: str, now_ms: int) -> None:
        """An order that never went out (or filled nothing): the record is kept under a numbered key so the journal
        shows the attempt, the slot is free for the next decision after LIVE_RETRY_SECONDS."""
        st = trade["live"]
        st.update(state="done", final="failed", error=why)
        st["events"].append({"at": now_ms, "what": why})
        trade.update(status="cancelled", unfilled=float(trade.get("order") or 0), note=why)
        st["rekeyed"] = self.live_rekey(tid, trade)
        self.live_backoff[(trade["market"], trade["side"])] = (time.monotonic() + self.config.live_retry, why)
        self.live_note_error(f"{trade['item']} {trade['label']}：{why}")
        self.live_notify(f"⚠️ {trade['item']} {trade['label']} {why}；{self.config.live_retry} 秒后再看。", key=f"fail:{trade['market']}:{trade['side']}")

    def live_rekey(self, tid: str, trade: dict) -> str:
        """Move a record to tid#n, freeing the paper trader's one slot per market, side and way."""
        if "#" in tid.rsplit("|", 1)[-1]:
            return tid  # already an attempt record
        existing = [k for k in self.sim_trades() if k.startswith(tid + "#")]
        new_tid = f"{tid}#{len(existing) + 1}"
        self.store.delete_keys([f"sim:{tid}"])
        self.sim_cache = None
        self.sim_save([(new_tid, trade)])
        return new_tid

    # --- reading back: fills, cancels, the final state of each order ---------------------------------------------------
    async def live_sync(self, now_ms: int) -> None:
        trades = self.sim_trades()
        active = {tid: t for tid, t in trades.items() if isinstance(t.get("live"), dict) and t["live"].get("state") in PLACING
                  and t["live"].get("state") != "pending"}
        if active and not self.live.ready:
            await self.live_ensure_ready()  # real orders are followed to their end whatever the mode (LIVE=off included)
        if not self.live.ready:
            return
        if active:
            await self.live_sync_orders(active, now_ms)
        self.live_check_kill(trades, now_ms)
        if self.config.live_auto_redeem and time.monotonic() - self.live_last_redeem >= self.LIVE_REDEEM_SECONDS:
            self.live_last_redeem = time.monotonic()
            try:
                await self.live_redeem(now_ms)
            except Exception as error:
                self.live_note_error(f"领取结算失败：{core.clean_error(error) or type(error).__name__}")

    async def live_sync_orders(self, active: dict[str, dict], now_ms: int) -> None:
        open_rows = await self.live.api.orders("OPEN")
        by_id = {str(r.get("id")): r for r in open_rows}
        by_hash = {order_hash_of(r): r for r in open_rows if order_hash_of(r)}
        markets = {}
        with contextlib.suppress(Exception):
            markets = {mk.market: mk for mk in self.sim_markets(now_ms)}
        for tid, trade in active.items():
            st = trade["live"]
            before = json.dumps(trade, sort_keys=True, default=str)
            try:
                if st["state"] == "placing":
                    await self.live_reconcile(tid, trade, by_hash, now_ms)
                elif st["state"] == "open":
                    await self.live_track(tid, trade, by_id, markets.get(trade["market"]), now_ms)
                elif st["state"] == "cancelling":
                    await self.live_cancel(tid, trade, by_id, markets.get(trade["market"]), now_ms)
            except Exception as error:
                st["last_error"] = core.clean_error(error) or type(error).__name__
                self.live_note_error(f"{trade['item']} {trade['label']} 订单同步失败：{st['last_error']}")
            if st.get("rekeyed") and st["rekeyed"] != tid:
                continue  # moved to an attempt record (live_fail): saved there, never again under the old key
            if json.dumps(trade, sort_keys=True, default=str) != before:
                self.sim_save([(tid, trade)])

    async def live_reconcile(self, tid: str, trade: dict, by_hash: dict, now_ms: int) -> None:
        """A record whose request may or may not have reached Predict (the process died in between): adopt the order
        when it is there under the saved hash, give up (never re-send) after the grace period."""
        st = trade["live"]
        row = by_hash.get(st.get("hash", ""))
        if row is None and now_ms - int(st.get("placing_at") or 0) > self.LIVE_PLACING_GRACE_MS:
            row = await self.live.api.order("", st.get("hash", "")) if st.get("hash") else None
        if row is not None:
            st.update(state="open", order_id=str(row.get("id") or ""), placed_at=st.get("placed_at") or now_ms)
            st["events"].append({"at": now_ms, "what": f"按哈希找回订单 #{st['order_id']}"})
            self.live_apply(tid, trade, row, None, now_ms)
            return
        if now_ms - int(st.get("placing_at") or 0) > self.LIVE_PLACING_GRACE_MS:
            self.live_fail(tid, trade, "下单后无法确认 Predict 是否收到（重启或超时），已放弃；请在网站核对订单", now_ms)

    async def live_track(self, tid: str, trade: dict, by_id: dict, mk: Any, now_ms: int) -> None:
        st = trade["live"]
        row = by_id.get(st["order_id"])
        if row is not None:
            self.live_misses.pop(tid, None)
            self.live_apply(tid, trade, row, mk, now_ms)
            if not trade.get("maker") and now_ms - int(st.get("placed_at") or now_ms) >= self.config.live_taker_wait * 1000:
                st.update(state="cancelling", cancel_why=f"吃单 {self.config.live_taker_wait} 秒未全部成交")
                st["events"].append({"at": now_ms, "what": st["cancel_why"]})
                await self.live_cancel(tid, trade, by_id, mk, now_ms)
            return
        final = await self.live.api.order(st["order_id"], st.get("hash", ""))
        if final is not None:
            self.live_finish(tid, trade, final, mk, now_ms)
            return
        self.live_misses[tid] = self.live_misses.get(tid, 0) + 1
        if self.live_misses[tid] >= self.LIVE_MISSES:
            self.live_finish(tid, trade, {"status": "UNKNOWN"}, mk, now_ms)

    async def live_cancel(self, tid: str, trade: dict, by_id: dict, mk: Any, now_ms: int) -> None:
        st = trade["live"]
        if not st.get("order_id"):
            return self.live_finish(tid, trade, {"status": "CANCELLED"}, mk, now_ms)  # never reached Predict
        gone_now = False
        if not st.get("cancel_sent"):
            result = await self.live.api.remove_orders([st["order_id"]])
            st["cancel_sent"], gone_now = now_ms, st["order_id"] in result["removed"]
            st["events"].append({"at": now_ms, "what": f"撤单请求已发送（{'已撤' if gone_now else '无需撤/已不在盘口'}）"})
        if st["order_id"] in by_id and not gone_now:
            return  # still listed: the fills so far, the next look will see it gone
        final = await self.live.api.order(st["order_id"], st.get("hash", ""))
        self.live_finish(tid, trade, final or {"status": "CANCELLED"}, mk, now_ms)

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
            trade.update(avg=price, fee=fee, price=price + fee, slip=price - float(trade.get("best", price)))
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
        else:
            self.live_fail(tid, trade, f"吃单未成交（{st.get('cancel_why') or status or '不在盘口'}）", now_ms)

    # --- redeeming resolved positions -------------------------------------------------------------------------------------
    async def live_redeem(self, now_ms: int) -> list[str]:
        """Positions on markets Predict has resolved: the winning outcome's shares are redeemed for USDT on-chain
        (once per market and outcome); the lines say what was done."""
        rows = await self.live.api.positions()
        self.live_positions = (rows, time.time())
        done = []
        for row in rows:
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
            if self.store.get(key):
                continue
            try:
                raw = await self.live.api.market(mid)
            except Exception:
                continue
            info = parse_market(raw, self.config.predict_fee_bps)
            resolved = core.predict_resolution(raw)
            if not core.PredictFeed.market_settled({"status": info.status}) or resolved is None:
                continue
            won = resolved.get("split") or (resolved.get("index") is not None and index_set == int(resolved["index"]) + 1)
            if not won or not info.condition_id:
                continue
            try:
                tx = await self.live.chain.redeem(info.condition_id, index_set, amount, info.neg_risk, info.yield_bearing)
            except Exception as error:
                self.live_note_error(f"领取 {market.get('title') or mid} 失败：{core.clean_error(error) or type(error).__name__}")
                continue
            self.store.put(key, {"at": now_ms, "tx": tx, "amount": str(amount), "title": str(market.get("title") or "")})
            line = f"💵 已领取 {market.get('title') or mid} {outcome.get('name') or ''} {from_wei(amount):g} 份：{tx}"
            done.append(line)
            self.live_notify(line)
        return done

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
        return await self.live_action(words[0] if words else "", req.args[1:], self.market.now_ms())

    async def live_action(self, action: str, args: list[str], now_ms: int) -> Any:
        """/live and the control page share these: "" = the status card (a Reply), else the action's answer text."""
        words = [a.lower() for a in args]
        if action == "pause":
            why = " ".join(a for a in args if a).strip() or "手动暂停"
            self.store.put("live:paused", {"at": now_ms, "why": why})
            return f"⏸ 真实交易已暂停（{why}）：不再开新仓；已有挂单保留，/live cancel all 可撤。/live resume 恢复。"
        if action == "resume":
            self.store.delete_keys(["live:paused"])
            if record := self.live_kill_record():
                self.store.put("live:killed", {**record, "resumed": True, "resumed_at": now_ms})
            return "▶️ 真实交易已恢复：满足条件的建议会重新下单。"
        if action == "cancel":
            return await self.live_cancel_command(words, now_ms)
        if action == "test":
            return await self.live_test(args, now_ms)
        if action == "mode":
            return await self.live_set_mode(args[0] if args else "")
        if action == "redeem":
            if not self.live.ready:
                return f"未就绪：{self.live.not_ready_why}"
            lines = await self.live_redeem(now_ms)
            return "\n".join(lines) if lines else "没有可领取的已结算持仓（或已全部领取）。"
        if action == "check":
            lines = await self.live_prepare()
            return "🔎 真实交易自检\n" + "\n".join(lines) + ("\n最近错误：\n" + "\n".join(self.live_errors[-5:]) if self.live_errors else "")
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
            return "Predict 上的持仓：\n" + ("\n".join(self.position_line(r) for r in rows[:30]) or "无")
        return core.Reply(self.live_text(now_ms), html=True)

    # --- the control page --------------------------------------------------------------------------------------------------
    def apply_config(self, config: core.Config) -> None:
        before = self.config.live_mode
        super().apply_config(config)
        self.live.config = config
        if before != config.live_mode:
            self.live_mode_changed(before, config.live_mode)

    def live_mode_changed(self, before: str, after: str) -> None:
        """off → pause / on: sign in at the next step; → off: the resting real orders are withdrawn (the positions
        stay, followed to settlement), pending ones are not sent."""
        now_ms = self.market.now_ms()
        if after == "off":
            trades = self.sim_trades()
            for tid, t in trades.items():
                st = t.get("live")
                if isinstance(st, dict) and t["status"] == "resting" and st.get("state") in {"open", "placing"}:
                    self.sim_withdraw(t, "切换到 LIVE=off", now_ms)
                    self.sim_save([(tid, t)])
            self.live_notify(f"⚪ 真实交易模式 {before} → off：回到只记账，未成交的真实挂单已撤，已有持仓继续跟踪到结算。")
        else:
            self.live_last_signin = -1e9
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
        for tid, t in sorted(live, key=lambda kv: kv[1].get("opened", 0), reverse=True)[:30]:
            st = t["live"]
            rows.append({"id": tid, "opened": core.stamp(t["opened"], seconds=False), "item": t["item"], "label": t["label"],
                         "price": float(t["price"]), "order": float(t.get("order") or 0), "shares": float(t.get("shares") or 0),
                         "status": sim_status(t), "state": core.sim_state(t), "order_id": str(st.get("order_id") or ""),
                         "live_state": str(st.get("state") or ""), "maker": bool(t.get("maker")),
                         "cancellable": t["status"] == "resting" and st.get("state") in {"open", "placing"} and bool(st.get("order_id")),
                         "url": self.sim_url(t)})
        positions, at = self.live_positions
        return {"ready": self.live.ready, "ready_error": self.live.not_ready_why, "mode": c.live_mode, "paused": self.live_paused(), "killed": self.live_killed(),
                "lines": self.live.summary_lines(), "account": short_addr(self.live.wallet.maker),
                "exposure": self.live_exposure(trades), "daily_pnl": self.live_daily_pnl(trades, now_ms),
                "caps": {"order": c.live_max_order_usd, "open": c.live_max_open_usd, "daily_loss": c.live_max_daily_loss_usd},
                "resting": sum(t["status"] == "resting" for _, t in live),
                "placed_today": sum(int(t["opened"]) >= since for _, t in live),
                "failed_today": sum(int(t["opened"]) >= since and t["live"].get("final") == "failed" for _, t in live),
                "orders": rows, "positions": [self.position_line(r) for r in positions[:30]],
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
        price = round(cents_given / 100, 3) if cents_given is not None else min(self.LIVE_TEST_PRICE, round(best_bid - 0.01, 3))
        if price < 0.001:
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
        elif action in {"resume", "redeem", "check", "orders", "positions"}:
            text = await self.live_action(action, [], now_ms)
        else:
            return {"ok": False, "message": "未知操作"}
        return {"ok": True, "message": text if isinstance(text, str) else text.text}

    async def live_cancel_command(self, words: list[str], now_ms: int) -> str:
        trades = self.sim_trades()
        targets = {tid: t for tid, t in trades.items() if isinstance(t.get("live"), dict) and t["status"] == "resting"
                   and t["live"].get("state") in {"open", "placing"}}
        if not words:
            return "用法：/live cancel all（撤掉全部真实挂单）或 /live cancel <订单号>"
        if words[0] != "all":
            targets = {tid: t for tid, t in targets.items() if str(t["live"].get("order_id")) == words[0]}
            if not targets:
                return f"没有订单号为 {words[0]} 的真实挂单（/live 查看）"
        for tid, t in targets.items():
            self.sim_withdraw(t, "管理员撤单", now_ms)
            self.sim_save([(tid, t)])
        if not targets:
            return "没有真实挂单可撤。"
        try:
            await self.live_sync_orders(targets, now_ms)
        except Exception as error:
            return f"已标记撤单 {len(targets)} 笔，但发送撤单请求失败：{core.clean_error(error)}；下一轮会重试。"
        return f"已撤单 {len(targets)} 笔：" + "、".join(f"{t['item']} {t['label']}" for t in targets.values())

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
        today = dt.datetime.fromtimestamp(now_ms / 1000, core.BEIJING).replace(hour=0, minute=0, second=0, microsecond=0)
        since = int(today.timestamp() * 1000)
        placed_today = sum(int(t["opened"]) >= since for t in live)
        failed_today = sum(int(t["opened"]) >= since and (t["live"].get("final") == "failed") for t in live)
        lines.append(f"持仓+挂单 ${exposure:,.2f} / ${self.config.live_max_open_usd:g}｜今日已结算盈亏 {money(self.live_daily_pnl(trades, now_ms))}"
                     f"（上限 −${self.config.live_max_daily_loss_usd:g}）｜挂单中 {resting}｜今日下单 {placed_today}｜失败 {failed_today}")
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
        lines.append("\n/live mode off|pause|on｜pause｜resume｜cancel all｜redeem｜check｜orders｜positions｜test [市场] [份数] [价格¢]")
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

    def journal_payload(self) -> dict:
        data = super().journal_payload()
        data["live"] = {"enabled": True, "account": short_addr(self.live.wallet.maker), "ready": self.live.ready,
                        "paused": self.live_paused(), "killed": self.live_killed()}
        return data

    async def run(self) -> int:
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
