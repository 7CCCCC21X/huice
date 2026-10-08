"""Real trading (LIVE=on): the paper trader's decisions become Predict orders. Offline: the API, the chain and the
cards are stand-ins; the signing vectors come from the official predict-sdk (same key, same order, same output)."""
import asyncio, base64, dataclasses, json, sys, time, datetime as dt
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m
import live as L
D = m.D
WEI = 10 ** 18
KEY = "0x" + "a" * 64  # the SDK test suite's key (never a real one): 0x8fd379246834eac74B8419FfdA202CF8051F7A03
ADDR = "0x8fd379246834eac74B8419FfdA202CF8051F7A03"
PA = "0x1111111111111111111111111111111111111111"
BJ = lambda mo, d, h, mi=0: int(dt.datetime(2026, mo, d, h, mi, tzinfo=m.BEIJING).timestamp() * 1000)
NOW = BJ(10, 5, 10, 0)
HSI_SLUG = "hang-seng-index-up-or-down-on-october-5-2026"
CLOSE = BJ(10, 5, 16, 10)

# --- settings ----------------------------------------------------------------------------------------------------------
base = {"TELEGRAM_BOT_TOKEN": "1:x"}
c = m.Config.from_env(base)
assert not c.live and c.live_key == "" and c.live_chain == 56 and c.live_max_order_usd == 100
assert "live_key" not in repr(c)  # the key is never in a repr / log line
c = m.Config.from_env({**base, "LIVE": "on", "PREDICT_PRIVATE_KEY": KEY[2:], "PREDICT_ACCOUNT": PA,
                       "LIVE_MAX_ORDER_USD": "50", "LIVE_MAX_OPEN_USD": "300", "LIVE_MAX_DAILY_LOSS_USD": "0",
                       "LIVE_AUTO_REDEEM": "off", "PREDICT_CHAIN_ID": "97", "BSC_RPC_URL": "https://rpc.example/"})
assert c.live and c.live_key == KEY[2:] and c.live_account == PA and c.live_max_order_usd == 50
assert c.live_max_open_usd == 300 and c.live_max_daily_loss_usd == 0 and not c.live_auto_redeem
assert c.live_chain == 97 and c.live_rpc == "https://rpc.example"
for bad in ({"LIVE": "on"}, {"LIVE": "on", "PREDICT_PRIVATE_KEY": "abc"}, {"PREDICT_PRIVATE_KEY": KEY, "PREDICT_ACCOUNT": "0x12"},
            {"LIVE": "on", "PREDICT_PRIVATE_KEY": KEY, "PREDICT_CHAIN_ID": "1"}, {"BSC_RPC_URL": "ftp://x"},
            {"LIVE": "on", "PREDICT_PRIVATE_KEY": KEY, "SIM": "off"}, {"LIVE": "on", "PREDICT_PRIVATE_KEY": KEY, "PREDICT": "off"}):
    try: m.Config.from_env({**base, **bad}); assert False, bad
    except ValueError: pass
assert not m.Config.from_env({**base, "PREDICT_PRIVATE_KEY": KEY}).live  # a key alone does not switch live trading on
pc = m.Config.from_env({**base, "LIVE": "pause", "PREDICT_PRIVATE_KEY": KEY})
assert pc.live and pc.live_mode == "pause" and m.Config.from_env({**base, "LIVE": "on", "PREDICT_PRIVATE_KEY": KEY}).live_mode == "on"
assert m.Config.from_env(base).live_mode == "off" and m.Config.from_env({**base, "LIVE": "test", "PREDICT_PRIVATE_KEY": KEY}).live_mode == "pause"
try: m.Config.from_env({**base, "LIVE": "pause"}); assert False  # a key is needed in pause mode too
except ValueError: pass
assert not m.Config.from_env(base).sim_taker_session and m.Config.from_env({**base, "SIM_TAKER_SESSION": "on"}).sim_taker_session
assert "SIM_TAKER_SESSION" in m.CONTROL_KEY_SET

# --- amounts: the official SDK's arithmetic (vectors computed with predict-sdk 0.0.22) --------------------------------------
assert L.to_wei(0.46) == 460000000000000000 and L.to_wei("0.421031") == 421031000000000000 and L.to_wei(D("100")) == 100 * WEI
assert L.retain_sig(627500000000000000, 3) == 627000000000000000 and L.retain_sig(123456, 5) == 123450 and L.retain_sig(0, 3) == 0
assert L.retain_sig(-123456, 2) == -120000 and L.retain_sig(99, 5) == 99
assert L.limit_amounts(True, 627500000000000000, 100 * WEI) == {  # the price on the cent tick (0.6275 → 62¢): the API takes no third decimal
    "price_per_share": 620000000000000000, "maker": 62000000000000000000, "taker": 100 * WEI, "amount": 100 * WEI,
    "last": 620000000000000000, "slippage_bps": 0, "min_out": False}
assert L.limit_amounts(False, 627000000000000000, 10 * WEI)["maker"] == 10 * WEI and L.limit_amounts(False, 627000000000000000, 10 * WEI)["taker"] == 6200000000000000000
assert L.tick_price(129999999999999900) == 130000000000000000 and L.tick_price(591000000000000000) == 590000000000000000 and L.tick_price(10 ** 16) == 10 ** 16 and L.tick_price(10 ** 15) == 0
for bad in (lambda: L.limit_amounts(True, 0, 100 * WEI), lambda: L.limit_amounts(True, WEI, 10 ** 15)):
    try: bad(); assert False
    except ValueError: pass
assert L.exchange_key(False, False) == "CTF_EXCHANGE" and L.exchange_key(True, True) == "YIELD_BEARING_NEG_RISK_CTF_EXCHANGE"
assert L.tokens_key(True, False) == "NEG_RISK_CONDITIONAL_TOKENS"

# --- signing: the same hash and signature as the official SDK -----------------------------------------------------------------
w = L.Wallet(KEY, 56)
assert w.signer == ADDR and w.maker == ADDR and not w.predict_account and w.kind == "普通钱包"
ZERO = "0x" + "0" * 40
order = {"salt": "123456789", "maker": "0x1234567890123456789012345678901234567890", "signer": "0x1234567890123456789012345678901234567890",
         "taker": ZERO, "tokenId": "12345", "makerAmount": "1000000000000000000", "takerAmount": "2000000000000000000",
         "expiration": "4102444800", "nonce": "0", "feeRateBps": "100", "side": 0, "signatureType": 0}
assert w.order_hash(order, False, False) == "0x814000c89efa61ae42a2bcc4c98e06e90c11480b95a12edea00e3411ec76821d"  # the SDK's own vector
assert w.order_hash(order, True, False) == "0x8933540fa69d874eb627f27b47fc913902ac470d3cefe61ca16f05425c50c4a1"  # another exchange, another hash
o2 = L.build_order(w, "98765", 55 * WEI, 100 * WEI, 200, 4102444800, salt=123456789)
assert o2 == {"salt": "123456789", "maker": ADDR, "signer": ADDR, "taker": ZERO, "tokenId": "98765", "makerAmount": "55000000000000000000",
              "takerAmount": "100000000000000000000", "expiration": "4102444800", "nonce": "0", "feeRateBps": "200", "side": 0, "signatureType": 0}
h2, sig2 = w.sign_order(o2, False, False)
assert h2 == "0xc053e9707ffed7a3c30af95d33f20d7c39ac3e322a2f42b5065a3ffe87d0b672"
assert sig2 == "0xf11cc96337b432b3686ee32d4686507d4705ae3a85d1d87e69a8be3556213512005234d1590b46bc2362c671995eb0849c4fcb091a9661b61877111b206f0a471b"
from eth_account import Account
from eth_account.messages import encode_typed_data
assert Account.recover_message(encode_typed_data(full_message=w.typed_data(o2, False, False)), signature=sig2) == ADDR
assert w.sign_text("Sign in to Predict\nNonce: abc123") == ("0xa845ed1d522b341e854cc0ae95e33d2d29a1c49469596f939ac55b4de376460d"
                                                            "57f293cf49aba6834d021ecf9ffd305efa966dafda2786f6d5d29a964e14c81c1c")
assert int(L.build_order(w, "1", 1, 1, 0, 1)["salt"]) <= L.MAX_SALT and L.build_order(w, "1", 1, 1, 0, 1)["salt"] != L.build_order(w, "1", 1, 1, 0, 1)["salt"]
# a Predict account: the key signs for the smart wallet, Kernel-wrapped, validator-prefixed (the SDK's format and bytes)
k = L.Wallet(KEY, 56, PA.lower())
assert k.maker == PA and k.signer == ADDR and k.predict_account == PA and k.kind.startswith("Predict 账户")
o3 = L.build_order(k, "98765", 55 * WEI, 100 * WEI, 200, 4102444800, salt=123456789)
assert o3["maker"] == PA and o3["signer"] == PA
h3, sig3 = k.sign_order(o3, False, False)
assert h3 == "0xb09712f122846a41d60af7541d20829d14d47e8510b8f01ff670027286222d21"
assert sig3 == ("0x01845ADb2C711129d4f3966735eD98a9F09fC4cE5709392012521d9efa76de3a3f617e4856130005a888d1ecd493b485a6e57667ce73c0a3e8dd4f8fe5"
                "008b0e0a1ee1b9b1404323e478cbe66f98e400851087906b1c")
assert k.sign_text("Sign in to Predict\nNonce: abc123") == (
    "0x01845ADb2C711129d4f3966735eD98a9F09fC4cE5792259d89fe6dccaf6458c4a36eebf28b0b1136ddde2a2f8388af343425e9f6294f33121a9ae7dd2692a32095a8970d9ee7148fd538840dc53d739cc0134417e51c")
try: L.Wallet(KEY, 1); assert False
except ValueError: pass

# --- small parsers ----------------------------------------------------------------------------------------------------------
tok = "x." + base64.urlsafe_b64encode(json.dumps({"exp": 1800000000}).encode()).decode().rstrip("=") + ".y"
assert L.jwt_expiry(tok) == 1800000000 and L.jwt_expiry("bad") == 0 and L.jwt_expiry("") == 0
assert L.http_status(m.RemoteError("HTTP 401: nope")) == 401 and L.http_status(m.RemoteError("网络错误")) == 0
assert L.order_fill({"amount": str(100 * WEI), "amountFilled": str(30 * WEI), "status": "open"}, 100) == (30.0, "OPEN")
assert L.order_fill({"amount": "0", "status": "FILLED"}, 60) == (60.0, "FILLED") and L.order_fill({"status": "CANCELLED"}, 60) == (0.0, "CANCELLED")
assert L.order_fill({"amount": "100", "amountFilled": "250"}, 100) == (100.0, "")  # never more than ordered
assert L.order_price({"averagePrice": "0.58"}) == 0.58 and L.order_price({"avgPrice": str(58 * 10 ** 16)}) == 0.58 and L.order_price({}) is None
assert L.order_price({"averagePrice": "0"}) is None and L.order_hash_of({"order": {"hash": "0xAB"}}) == "0xab" and L.order_hash_of({"hash": "0xCD"}) == "0xcd"
assert L.PredictApi.rows({"success": True, "data": [{"id": 1}, 3]}) == [{"id": 1}] and L.PredictApi.rows({"data": {"items": [{"a": 1}]}}) == [{"a": 1}]
assert L.PredictApi.rows([{"b": 2}]) == [{"b": 2}] and L.PredictApi.rows({"data": {}}) == []
outs = [{"name": "Up", "index_set": 1, "token": "1"}, {"name": "Down", "index_set": 2, "token": "2"}]
assert L.outcome_for_side("close", "up", outs) == (outs[0], "") and L.outcome_for_side("close", "down", outs) == (outs[1], "")
flipped = [{"name": "No", "index_set": 1, "token": "9"}, {"name": "Yes", "index_set": 2, "token": "8"}]
assert L.outcome_for_side("ladder", "up", flipped)[0]["token"] == "8" and L.outcome_for_side("range", "down", flipped)[0]["token"] == "9"
assert L.outcome_for_side("flip", "down", [{"name": "Yes", "index_set": 1, "token": "5"}, {"name": "Maybe", "index_set": 2, "token": "6"}])[0]["token"] == "6"
odd = [{"name": "A", "index_set": 1, "token": "3"}, {"name": "B", "index_set": 2, "token": "4"}]
assert L.outcome_for_side("close", "down", odd) == (odd[1], "按结果顺序推定（结果名称：a、b）") and L.outcome_for_side("ladder", "up", odd)[0] is None
assert L.outcome_for_side("close", "up", outs[:1])[0] is None and L.outcome_for_side("close", "up", [{**outs[0], "token": ""}, outs[1]])[0] is None
bnb = next(s for s in m.TOUCH_MARKETS if s.key == "BNB")
touch_outs = [{"name": "$700", "index_set": 1, "token": "70"}, {"name": "$900", "index_set": 2, "token": "90"}]
assert L.outcome_for_side("touch", "up", touch_outs, bnb)[0]["token"] == "90" and L.outcome_for_side("touch", "down", touch_outs, bnb)[0]["token"] == "70"
info = L.parse_market({"id": 101, "status": "REGISTERED", "isNegRisk": True, "isYieldBearing": False, "feeRateBps": "150", "conditionId": "0xab",
                       "outcomes": [{"name": "Down", "indexSet": 2, "onChainId": "222"}, {"name": "Up", "indexSet": 1, "onChainId": "111"}]}, 200)
assert info.market_id == "101" and info.neg_risk and not info.yield_bearing and info.fee_bps == 150 and info.condition_id == "0xab"
assert [o["token"] for o in info.outcomes] == ["111", "222"]  # index order, whatever the API's order
assert L.parse_market({"id": 5, "outcomes": [], "feeRateBps": None}, 200).fee_bps == 200
assert L.short_addr(ADDR) == "0x8fd3…7A03" and L.money(-3.5) == "−$3.50" and L.money(0) == "+$0.00"
assert L.Chain.encode("approve(address,uint256)", ["address", "uint256"], [L.ADDRESSES[56]["USDT"], 1]).hex().startswith("095ea7b3")

# --- the REST client against a canned server -------------------------------------------------------------------------------
server = {"calls": [], "answers": {}, "fail": {}}


def fake_http(url, payload=None, timeout=15, headers=None):
    path = url.replace(m.PREDICT_REST, "")
    server["calls"].append((path, payload, dict(headers or {})))
    if path in server["fail"]:
        error = server["fail"][path]
        if isinstance(error, list):
            error = error.pop(0) if error else None
        if error:
            raise m.RemoteError(error, 7 if "429" in error else 0)
    answer = server["answers"].get(path, {"success": True, "data": {}})
    return json.dumps(answer(payload) if callable(answer) else answer).encode()


m._http_get = fake_http


async def api_checks():
    api = L.PredictApi(m.PREDICT_REST, "key-1", w)
    server["answers"]["/auth/message"] = {"success": True, "data": {"message": "Sign in to Predict\nNonce: abc123"}}
    server["answers"]["/auth"] = lambda body: {"success": True, "data": {"token": tok}} if body == {
        "signer": ADDR, "signature": w.sign_text("Sign in to Predict\nNonce: abc123"), "message": "Sign in to Predict\nNonce: abc123"} else {"success": False}
    assert await api.authenticate() == tok and api.jwt_exp == 1800000000 and api.auth_error == ""
    assert server["calls"][0][2] == {"x-api-key": "key-1"} and api.headers() == {"x-api-key": "key-1", "Authorization": f"Bearer {tok}"}
    # a 401 signs in again, once; a 429 pauses every request
    server["answers"]["/orders?status=OPEN&first=50"] = {"success": True, "cursor": None, "data": [{"id": "7", "status": "OPEN"}]}
    server["fail"]["/orders?status=OPEN&first=50"] = ["HTTP 401: expired"]
    server["calls"].clear()
    assert await api.orders("OPEN") == [{"id": "7", "status": "OPEN"}]
    assert [p for p, _, _ in server["calls"]] == ["/orders?status=OPEN&first=50", "/auth/message", "/auth", "/orders?status=OPEN&first=50"]
    server["answers"]["/orders?status=OPEN&first=50"] = {"success": True, "cursor": "c1", "data": [{"id": "7", "status": "OPEN"}]}
    server["answers"]["/orders?status=OPEN&first=50&after=c1"] = {"success": True, "cursor": None, "data": [{"id": "8", "status": "OPEN"}]}
    assert [r["id"] for r in await api.orders("OPEN")] == ["7", "8"]  # the second page is read: a long list is never cut off
    server["answers"]["/orders?status=OPEN&first=50"] = {"success": True, "cursor": None, "data": [{"id": "7", "status": "OPEN"}]}
    server["fail"]["/orders?status=OPEN&first=50"] = ["HTTP 429: slow down"]
    try: await api.orders("OPEN"); assert False
    except m.RemoteError as e: assert "429" in str(e)
    try: await api.orders("OPEN"); assert False
    except m.RemoteError as e: assert "限流冷却" in str(e) and e.retry_after >= 1
    api.blocked_until = 0.0
    # orders: the body the API documents; its refusals carry its reason
    server["answers"]["/orders"] = lambda body: {"success": True, "data": {"code": "OK", "orderId": "123", "orderHash": body["data"]["order"]["hash"]}}
    got = await api.create_order({"data": {"order": {"hash": "0xh"}, "strategy": "LIMIT"}})
    assert got == {"order_id": "123", "hash": "0xh", "code": "OK"} and server["calls"][-1][0] == "/orders"
    server["answers"]["/orders"] = {"success": False, "message": "Insufficient balance"}
    try: await api.create_order({"data": {}}); assert False
    except m.RemoteError as e: assert isinstance(e, L.OrderRejected); assert "Insufficient balance" in str(e)
    server["answers"]["/orders"] = {"success": True, "data": {"nothing": 1}}
    try: await api.create_order({"data": {}}); assert False
    except m.RemoteError as e: assert "订单号" in str(e)
    server["answers"]["/orders/remove"] = lambda body: {"success": True, "removed": body["data"]["ids"][:1], "noop": body["data"]["ids"][1:]}
    assert await api.remove_orders(["1", "2"]) == {"removed": ["1"], "noop": ["2"]}
    server["answers"]["/orders/remove"] = {"success": True, "data": {"removed": [], "noop": ["9"]}}
    assert await api.remove_orders(["9"]) == {"removed": [], "noop": ["9"]}
    # one order: by its hash (GET /orders/{hash}, the API's single-order read); 404 = unknown; other failures surface;
    # without a hash only the open list is searched, by id — never a made-up status filter (the API rejects those)
    server["answers"]["/orders/0xaa"] = {"success": True, "data": {"id": "55", "status": "FILLED", "order": {"hash": "0xaa"}}}
    assert (await api.order("55", "0xaa"))["status"] == "FILLED" and server["calls"][-1][0] == "/orders/0xaa"
    server["fail"]["/orders/0xbb"] = ["HTTP 404: not found"]
    assert await api.order("66", "0xbb") is None
    server["fail"]["/orders/0xdd"] = ["HTTP 500: boom"]
    try: await api.order("66", "0xdd"); assert False
    except m.RemoteError as e: assert "500" in str(e)
    server["answers"]["/orders/0xee"] = {"success": True, "data": {}}
    assert await api.order("", "0xee") is None  # an empty answer is no order
    server["answers"]["/orders?status=OPEN&first=50"] = {"data": [{"id": "66", "status": "OPEN", "order": {"hash": "0xcc"}}]}
    server["calls"].clear()
    assert (await api.order("66"))["id"] == "66" and [p for p, _, _ in server["calls"]] == ["/orders?status=OPEN&first=50"]
    assert await api.order("77") is None and all(p in {"/orders?status=OPEN&first=50"} for p, _, _ in server["calls"])
    server["answers"]["/positions"] = {"success": True, "data": [{"id": "p1"}]}
    assert await api.positions() == [{"id": "p1"}]
    server["answers"]["/markets/101"] = {"success": True, "data": {"id": 101, "outcomes": []}}
    assert (await api.market("101"))["id"] == 101
    # no wallet: no sign-in, a plain reason
    plain_api = L.PredictApi(m.PREDICT_REST, "", None)
    try: await plain_api.authenticate(); assert False
    except m.RemoteError as e: assert "签名钱包" in str(e)
    # the chain: calls encoded and decoded, transactions built, signed and awaited; Kernel-wrapped for a Predict account
    rpc_calls = []
    values = {"eth_call": "0x" + hex(5 * WEI)[2:].rjust(64, "0"), "eth_getBalance": hex(10 ** 17), "eth_getTransactionCount": "0x5",
              "eth_gasPrice": "0x3b9aca00", "eth_estimateGas": "0x5208", "eth_sendRawTransaction": "0xtxhash",
              "eth_getTransactionReceipt": {"blockNumber": "0x10", "status": "0x1"}}

    class FakeChain(L.Chain):
        async def rpc(self, method, params):
            rpc_calls.append((method, params))
            return values[method]
    chain = FakeChain("https://rpc.example", w)
    assert await chain.usdt_balance() == 5 * WEI and await chain.bnb_balance() == 10 ** 17 and await chain.allowance("CTF_EXCHANGE") == 5 * WEI
    assert rpc_calls[0][1][0]["to"] == L.ADDRESSES[56]["USDT"] and rpc_calls[0][1][0]["data"].startswith("0x70a08231")  # balanceOf(address)
    assert await chain.approved_for_all("CONDITIONAL_TOKENS", "CTF_EXCHANGE") is True
    signed = []
    w.sign_transaction = lambda tx: signed.append(tx) or b"\x01\x02"
    assert await chain.send(L.ADDRESSES[56]["USDT"], b"\xaa\xbb") == "0xtxhash"
    assert signed[-1] == {"chainId": 56, "nonce": 5, "gasPrice": 10 ** 9, "gas": 26250, "to": L.ADDRESSES[56]["USDT"], "value": 0, "data": "0xaabb"}
    assert rpc_calls[-1] == ("eth_sendRawTransaction", ["0x0102"])
    assert (await chain.wait("0xtxhash"))["status"] == "0x1"
    values["eth_getTransactionReceipt"] = {"blockNumber": "0x10", "status": "0x0"}
    try: await chain.wait("0xtxhash"); assert False
    except m.RemoteError as e: assert "回滚" in str(e)
    values["eth_getTransactionReceipt"] = {"blockNumber": "0x10", "status": "0x1"}
    kchain = FakeChain("https://rpc.example", k)
    k.sign_transaction = lambda tx: signed.append(tx) or b"\x03"
    await kchain.send(L.ADDRESSES[56]["USDT"], b"\xaa\xbb", 0)
    tx = signed[-1]
    assert tx["to"] == PA and tx["data"].startswith("0xe9ae5c53")  # Kernel.execute(bytes32,bytes), to the account, from the key
    assert tx["data"].endswith(L.ADDRESSES[56]["USDT"][2:].lower() + "0" * 64 + "aabb" + "0" * 20)  # target ‖ value ‖ calldata, padded
    # approvals: five steps per track; in place = skipped, missing = sent
    assert [ok for _, ok in await chain.check_approvals(False)] == [True, True, True, False, False]  # 5 USDT allowance is not "unlimited"
    values["eth_call"] = "0x" + "0" * 64
    steps = await chain.set_approvals(False)
    assert len(steps) == 5 and all(outcome.startswith("已完成 0xtxhash") for _, outcome in steps), steps
    values["eth_call"] = "0x" + hex(10 ** 24)[2:].rjust(64, "0")
    assert [outcome for _, outcome in await chain.set_approvals(True)] == ["已授权"] * 5
    # redeeming: the conditional tokens for a standard market, the adapter for a multi-outcome one
    await chain.redeem("0x" + "ab" * 32, 1, 7 * WEI, False, False)
    assert signed[-1]["to"] == L.ADDRESSES[56]["CONDITIONAL_TOKENS"] and signed[-1]["data"].startswith("0x01b7037c")
    await chain.redeem("0x" + "ab" * 32, 2, 7 * WEI, True, False)
    neg_selector = "0x" + L.Chain.encode("redeemPositions(bytes32,uint256[])", ["bytes32", "uint256[]"], [bytes(32), [0, 0]]).hex()[:8]
    assert signed[-1]["to"] == L.ADDRESSES[56]["NEG_RISK_ADAPTER"] and signed[-1]["data"].startswith(neg_selector)
    assert signed[-1]["data"].endswith("0" * 64 + hex(7 * WEI)[2:].rjust(64, "0"))  # amounts [0, 7 shares] for outcome 2
    values["eth_call"] = "0x"
    assert await chain.usdt_balance() == 0


asyncio.run(api_checks())


# --- the bot: every decision an order, every fill read back -------------------------------------------------------------------
class FakeApi:
    """Predict as the bot sees it: markets, an order book of our own orders, positions."""
    def __init__(self):
        self.jwt, self.jwt_exp, self.auth_error, self.lookup = "t", 0, "", ""
        self.markets, self.open, self.closed, self.calls, self.positions_rows = {}, {}, {}, [], []
        self.next_id, self.fail_create, self.fail_open = 100, None, None

    async def ensure_auth(self): pass

    async def market(self, mid): return self.markets[str(mid)]

    async def create_order(self, body):
        self.calls.append(("create", body))
        if self.fail_create:
            raise m.RemoteError(self.fail_create)
        self.next_id += 1
        oid, data = str(self.next_id), body["data"]
        amount = data["order"]["takerAmount"]
        self.open[oid] = {"id": oid, "status": "OPEN", "amount": amount, "amountFilled": "0", "strategy": data["strategy"],
                          "order": {**data["order"]}}
        return {"order_id": oid, "hash": data["order"]["hash"], "code": None}

    async def remove_orders(self, ids):
        self.calls.append(("remove", list(ids)))
        removed = []
        for i in ids:
            row = self.open.pop(i, None)
            if row:
                row["status"] = "CANCELLED"
                self.closed[i] = row
                removed.append(i)
        return {"removed": removed, "noop": [i for i in ids if i not in removed]}

    async def orders(self, status="OPEN", **params):
        if self.fail_open:
            raise m.RemoteError(self.fail_open)
        return list(self.open.values()) if status == "OPEN" else [r for r in self.closed.values() if r["status"] == status]

    async def order(self, oid, hash_=""):
        rows = [*self.open.values(), *self.closed.values()]
        return self.open.get(oid) or self.closed.get(oid) or next((r for r in rows if hash_ and r["order"]["hash"] == hash_), None)

    async def positions(self): return self.positions_rows

    def fill(self, oid, shares, done=False, price=None):
        row = self.open[oid]
        row["amountFilled"] = str(L.to_wei(shares))
        if price is not None:
            row["averagePrice"] = str(price)
        if done:
            row["status"] = "FILLED"
            self.closed[oid] = self.open.pop(oid)


class FakeChain:
    def __init__(self): self.usdt, self.bnb, self.allow, self.redeemed, self.fail, self.owner = 1000 * WEI, 10 ** 17, 10 ** 30, [], "", ADDR
    async def account_owner(self, account): return self.owner
    async def usdt_balance(self, owner=None): return self.usdt
    async def bnb_balance(self, owner=None): return self.bnb
    async def allowance(self, key, owner=None): return self.allow
    async def redeem(self, cid, index_set, amount, neg, yb):
        if self.fail:
            raise m.RemoteError(self.fail)
        self.redeemed.append((cid, index_set, amount, neg, yb))
        return "0xredeem"


class FM:
    def __init__(self, now): self.now, self.config = now, None
    def now_ms(self): return self.now


def book(bids, asks, at=NOW, slug=HSI_SLUG, key="HSI", fee=None, mid="101"):
    return m.PredictBook(key, slug, mid, "t", tuple((D(str(p)), D(str(q))) for p, q in bids), tuple((D(str(p)), D(str(q))) for p, q in asks), at, fee)


SETTLE_HSI = {"key": "HSI", "target": "2026-10-05", "line": 24600.0, "close_ms": CLOSE}


def hsi(fair, bids, asks, at, **kw):
    return m.SimMarket(HSI_SLUG, "恒生指数", "close", "HSI", fair, book(bids, asks, at), 0.03, kw.get("hold", ""), ("涨", "跌"), SETTLE_HSI,
                       {"basis": {"fair_up": fair, "ref": 24600.0}, "sources": [{"what": "恒指期货", "source": "etnet"}], "proxy": None})


def market_json(mid, names=("Up", "Down"), status="REGISTERED", neg=False, fee=200):
    return {"id": int(mid), "status": status, "isNegRisk": neg, "isYieldBearing": False, "feeRateBps": fee, "conditionId": "0x" + "ab" * 32,
            "outcomes": [{"name": names[0], "indexSet": 1, "onChainId": f"{mid}1"}, {"name": names[1], "indexSet": 2, "onChainId": f"{mid}2"}]}


world = {"markets": []}
FEE = 0.02


async def step(bot, at, *markets):
    bot.market.now, bot.sim_ran = at, -1e9
    if markets:
        world["markets"] = list(markets)
    return await bot.sim_step(at)


def make_bot(**env):
    cfg = m.Config.from_env({**base, "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "WEB_PORT": "8080",
                             "SIM_WAYS": "both", "SIM_MARKETS": "all", "LIVE": "on", "PREDICT_PRIVATE_KEY": KEY, "PREDICT_API_KEY": "k",
                             "SIM_MAKER_MIN_BID": "0", "SIM_MAKER_EXIT_CENTS": "0", "SIM_MAKER_SESSION": "off", "SIM_MAKER_POINTS": "off", "SIM_MAKER_SPREAD_CENTS": "10",  # resting orders rest as placed, unless a test says otherwise
                             "LIVE_MAX_ORDER_USD": "100", "LIVE_MAX_OPEN_USD": "250", "LIVE_MAX_DAILY_LOSS_USD": "50", **env})
    bot = L.LiveBot(cfg, m.Store(":memory:"), FM(NOW), None)
    bot.live.api, bot.live.chain = FakeApi(), FakeChain()
    bot.live.api.markets["101"] = market_json("101")
    bot.sim_markets = lambda now: world["markets"]
    return bot


async def run():
    bot = make_bot()
    fake = bot.live.api
    assert "/live" in bot.handlers and not bot.live.ready and bot.live.ready_error
    assert L.LiveTrader(bot.config).api.base == m.PREDICT_REST and L.API_BASES[97].startswith("https://api-testnet.predict.fun")
    assert L.LiveTrader(dataclasses.replace(bot.config, live_chain=97)).api.base == "https://api-testnet.predict.fun/v1"
    assert "真实交易未就绪" in bot.live_room({}, hsi(0.70, [], [], NOW), "up", 0.5, 100)
    lines = await bot.live_prepare()
    assert bot.live.ready and bot.live.usd() == 1000.0 and bot.live.approvals == {"CTF_EXCHANGE": True, "NEG_RISK_CTF_EXCHANGE": True}
    assert any("普通钱包" in line and "0x8fd3…7A03" in line for line in lines) and any("USDT 1,000.00" in line for line in lines), lines
    # a market whose details are not read yet is not traded, and is read when its suggestion nears the bar
    assert "尚未读到" in bot.live_room({}, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW), "up", 0.58, 100)
    world["markets"] = [hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW)]
    await bot.live_prefetch(NOW)
    assert "101" in bot.live.markets and bot.live.markets["101"].fee_bps == 200
    assert bot.live_room({}, world["markets"][0], "up", 0.58, 100) == ""

    # --- HSI: fair 70¢, bid 55¢ (挂涨 +15¢) and ask 58¢×400 (吃涨 +11.16¢): two LIMIT orders go out, the taker's capped at 59.0¢
    # (the dearest price that keeps 10¢ of edge after the fee) for the full 100 shares -------------------------------------------
    await step(bot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    trades = bot.sim_trades()
    maker_tid, taker_tid = f"{HSI_SLUG}|up|挂", f"{HSI_SLUG}|up|吃"
    assert sorted(trades) == [taker_tid, maker_tid], sorted(trades)
    maker, taker = trades[maker_tid], trades[taker_tid]
    assert maker["status"] == "resting" and maker["shares"] == 0 and maker["live"]["state"] == "open" and maker["live"]["order_id"] == "101"
    assert taker["status"] == "resting" and taker["shares"] == 0 and taker["live"]["state"] == "open" and taker["live"]["order_id"] == "102"
    assert taker["fills"] == [] and taker["filled"] is None and abs(taker["expected_price"] - (0.58 + FEE * 0.42)) < 1e-12
    creates = [body for kind, body in fake.calls if kind == "create"]
    lim, mkt = creates[0]["data"], creates[1]["data"]
    assert lim["strategy"] == "LIMIT" and lim["pricePerShare"] == str(55 * 10 ** 16) and "slippageBps" not in lim
    o = lim["order"]
    assert o["tokenId"] == "1011" and o["makerAmount"] == str(55 * WEI) and o["takerAmount"] == str(100 * WEI) and o["side"] == 0
    assert o["maker"] == ADDR and o["feeRateBps"] == "200" and o["expiration"] == str(CLOSE // 1000 + 7200) and o["signatureType"] == 0
    assert o["hash"] == bot.live.wallet.order_hash(o, False, False) and o["signature"].startswith("0x") and len(o["signature"]) == 132
    assert Account.recover_message(encode_typed_data(full_message=bot.live.wallet.typed_data(o, False, False)), signature=o["signature"]) == ADDR
    assert mkt["strategy"] == "LIMIT" and "slippageBps" not in mkt and "isMinAmountOut" not in mkt and "amount" not in mkt
    assert mkt["pricePerShare"] == str(59 * 10 ** 16) and mkt["order"]["makerAmount"] == str(59 * WEI) and mkt["order"]["takerAmount"] == str(100 * WEI)
    assert taker["cap"] == 0.59 and taker["live"]["want"]["cap"] == 0.59 and taker["live"]["usd_cap"] == 59.0 and taker["live"]["shares_requested"] == 100
    assert mkt["order"]["expiration"] == str(NOW // 1000 + 300) and mkt["order"]["tokenId"] == "1011"
    assert maker["live"]["hash"] == o["hash"].lower() and maker["live"]["want"]["outcome"] == "Up" and maker["live"]["usd_cap"] == 55.0
    assert maker["version"]["live"] and maker["version"]["live_account"] == "0x8fd3…7A03"
    assert abs(bot.live_exposure(trades) - (55 + 100 * 0.59)) < 1e-9  # resting orders are reserved at their cap: the maker's price, the taker's limit
    assert L.sim_status(taker) == "吃单等待成交（真实订单 #102 已成交 0/100 份）" and L.sim_status(maker) == "挂单中（真实订单 #101 已成交 0/100 份）"
    assert "限价吃单等待 Predict 成交（封顶 59.0¢" in bot.sim_wait(taker, world["markets"][0], NOW) and "挂 55.0¢" in bot.sim_wait(maker, world["markets"][0], NOW)
    # the edge lasting buys nothing more; the open orders are read back each step
    await step(bot, NOW + 10_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 10_000))
    assert len(bot.sim_trades()) == 2 and len([1 for kind, _ in fake.calls if kind == "create"]) == 2

    # --- the taker's order fills in full: a position, with the fill's own record -------------------------------------------------
    fake.fill("102", 100, done=True, price=0.585)
    await step(bot, NOW + 20_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 20_000))
    t = bot.sim_trades()[taker_tid]
    assert t["status"] == "filled" and t["shares"] == 100 and t["filled"] == NOW + 20_000 and t["live"]["state"] == "done" and t["live"]["final"] == "filled"
    assert t["fills"] == [{"at": NOW + 20_000, "shares": 100.0, "fair": 0.70, "how": "真实成交（吃单）", "order_id": "102",
                           "book": m.book_snapshot(world["markets"][0].book)}], t["fills"]
    assert t["avg"] == 0.585 and abs(t["price"] - (0.585 + FEE * 0.415)) < 1e-12 and t["expected_price"] < t["price"]  # the stated average replaces the estimate
    assert L.sim_status(t) == "持仓" and bot.sim_wait(t, None, NOW) == "等 10-05 收盘（10-05 16:10）后 1 小时（10-05 17:10），按官方收盘预结算，再等 Predict 确认"

    # --- the LIMIT order fills in part, then the paper trader withdraws it: the rest is cancelled on Predict ------------------
    fake.fill("101", 30)
    await step(bot, NOW + 30_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 30_000))
    t = bot.sim_trades()[maker_tid]
    assert t["status"] == "resting" and t["shares"] == 30 and t["fills"][0]["how"] == "真实成交（挂单）" and t["fills"][0]["shares"] == 30
    assert L.sim_status(t) == "挂单中（真实订单 #101 已成交 30/100 份）" and "已成交 30/100" in bot.sim_wait(t, world["markets"][0], NOW + 30_000)
    await step(bot, NOW + 40_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 40_000))  # the same fill seen again: nothing new
    assert len(bot.sim_trades()[maker_tid]["fills"]) == 1
    bot.config = dataclasses.replace(bot.config, sim_ways="taker")
    await step(bot, NOW + 50_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 50_000))
    t = bot.sim_trades()[maker_tid]
    assert ("remove", ["101"]) in fake.calls and "101" in fake.closed and t["live"]["state"] == "done" and t["live"]["final"] == "cancelled"
    assert t["status"] == "filled" and t["shares"] == 30 and t["unfilled"] == 70 and "其余 70 份模拟交易已改为只吃单" in t["note"], t["note"]
    assert t["live"]["cancel_why"] == "模拟交易已改为只吃单" and any("撤单请求已发送" in e["what"] for e in t["live"]["events"])

    # --- a taker order Predict leaves open fills nothing: cancelled after LIVE_TAKER_WAIT_SECONDS, the slot freed later ---------
    deep = lambda at, mid="103": m.SimMarket("deep", "deep", "close", "X", 0.70, book([], [("0.58", "100"), ("0.70", "1000")], at, "deep", mid=mid), 0.03, "", ("涨", "跌"),
                                             {"key": "X", "target": "2026-10-05", "line": 1.0, "close_ms": CLOSE})
    fake.markets["103"] = market_json("103")
    await step(bot, NOW + 60_000, deep(NOW + 60_000))
    t = bot.sim_trades()["deep|up|吃"]
    assert t["live"]["order_id"] == "103" and t["live"]["placed_at"] == NOW + 60_000 and "103" in fake.open
    await step(bot, NOW + 100_000, deep(NOW + 100_000))  # 40 s: still waiting
    assert bot.sim_trades()["deep|up|吃"]["status"] == "resting" and "103" in fake.open
    await step(bot, NOW + 121_000, deep(NOW + 121_000))  # 61 s: cancelled, no fill: the attempt is kept, the slot is free
    trades = bot.sim_trades()
    assert "deep|up|吃" not in trades and trades["deep|up|吃#1"]["status"] == "cancelled" and "103" in fake.closed
    assert trades["deep|up|吃#1"]["live"]["final"] == "failed" and "吃单未成交" in trades["deep|up|吃#1"]["note"]
    assert ("deep", "up") in bot.live_backoff and "再试" in bot.live_room(trades, deep(NOW + 121_000), "up", 0.58, 100)
    await step(bot, NOW + 131_000, deep(NOW + 131_000))  # within LIVE_RETRY_SECONDS: refused, and the refusal recorded
    assert "deep|up|吃" not in bot.sim_trades() and any(b["id"] == "deep|up|吃" and "再试" in b["why"] for b in bot.sim_blocks())
    bot.live_backoff[("deep", "up")] = (time.monotonic() - 1, "x")
    await step(bot, NOW + 141_000, deep(NOW + 141_000))
    t = bot.sim_trades()["deep|up|吃"]
    assert t["live"]["order_id"] == "104" and bot.sim_trades()["deep|up|吃#1"]["status"] == "cancelled"  # the second attempt, the first kept
    fake.fill("104", 60, done=False)
    await step(bot, NOW + 203_000, deep(NOW + 203_000))  # 62 s: the rest cancelled, the 60 shares are the position
    t = bot.sim_trades()["deep|up|吃"]
    assert t["status"] == "filled" and t["shares"] == 60 and t["unfilled"] == 40 and "真实成交 60/100 份" in t["note"], t

    # --- a refused request: no record under the slot, the attempt journaled, the market backed off ------------------------------
    fake.fail_create = "HTTP 400: insufficient balance"
    fake.markets["105"] = market_json("105")
    thin = lambda at: m.SimMarket("thin", "thin", "close", "X2", 0.70, book([], [("0.58", "200")], at, "thin", mid="105"), 0.03, "", ("涨", "跌"), {})
    await step(bot, NOW + 210_000, thin(NOW + 210_000))
    trades = bot.sim_trades()
    assert "thin|up|吃" not in trades and trades["thin|up|吃#1"]["status"] == "cancelled" and "下单失败：HTTP 400" in trades["thin|up|吃#1"]["note"]
    assert trades["thin|up|吃#1"]["live"]["state"] == "done" and ("thin", "up") in bot.live_backoff and bot.live_errors[-1].endswith("insufficient balance")
    # a request whose outcome is unknown (the network dropped) stays "placing": adopted by its hash when Predict lists it,
    # given up (never re-sent) when it does not within the grace period
    fake.fail_create = "网络错误 (TimeoutError)"
    fake.markets["106"] = market_json("106")
    lost = lambda at: m.SimMarket("lost", "lost", "close", "X3", 0.70, book([], [("0.58", "200")], at, "lost", mid="106"), 0.03, "", ("涨", "跌"), {})
    bot.live_backoff.clear()
    await step(bot, NOW + 220_000, lost(NOW + 220_000))
    t = bot.sim_trades()["lost|up|吃"]
    assert t["live"]["state"] == "placing" and t["live"]["hash"] and t["live"]["placing_at"] == NOW + 220_000 and not t["live"]["order_id"]
    fake.fail_create = None
    fake.open["900"] = {"id": "900", "status": "OPEN", "amount": str(100 * WEI), "amountFilled": str(100 * WEI), "order": {"hash": t["live"]["hash"]}}
    await step(bot, NOW + 230_000, lost(NOW + 230_000))
    t = bot.sim_trades()["lost|up|吃"]
    assert t["live"]["order_id"] == "900" and t["shares"] == 100 and t["status"] == "filled" and any("找回" in e["what"] for e in t["live"]["events"])
    fake.markets["107"] = market_json("107")
    gone = lambda at: m.SimMarket("gone", "gone", "close", "X4", 0.70, book([], [("0.58", "200")], at, "gone", mid="107"), 0.03, "", ("涨", "跌"), {})
    fake.fail_create = "网络错误 (TimeoutError)"
    await step(bot, NOW + 240_000, gone(NOW + 240_000))
    fake.fail_create = None
    await step(bot, NOW + 250_000, gone(NOW + 250_000))
    assert bot.sim_trades()["gone|up|吃"]["live"]["state"] == "placing"  # within the grace period: still waiting
    await step(bot, NOW + 370_000, gone(NOW + 370_000))
    gt = bot.sim_trades()["gone|up|吃"]  # past the grace: not given up — unknown, reserved, blocking its side, never re-sent
    assert gt["live"]["state"] == "unknown" and "无法确认" in gt["live"]["unknown_why"] and len([1 for kind, _ in fake.calls if kind == "create"]) == 7
    assert bot.live_reserved(gt) > 0 and "对账中" in bot.live_room(bot.sim_trades(), gone(NOW + 370_000), "up", 0.58, 100, taker={"cap": 0.59, "got": 100})  # a taker's side is held by the unknown taker
    result = await bot.live_control("release", {"id": "gone|up|吃"})  # an operator checked the site: it is not there
    trades = bot.sim_trades()
    assert result["ok"] and "gone|up|吃" not in trades and trades["gone|up|吃#1"]["note"] == "管理员确认订单不存在，已释放额度", (result, trades.keys())
    assert trades["gone|up|吃#1"]["live"]["state"] == "done" and bot.live_reserved(trades["gone|up|吃#1"]) == 0
    # an open order Predict stops listing without a final record: cancelled after a few looks, nothing invented
    fake.markets["108"] = market_json("108")
    ghost = lambda at: m.SimMarket("ghost", "ghost", "close", "X5", 0.70, book([], [("0.58", "200")], at, "ghost", mid="108"), 0.03, "", ("涨", "跌"), {})
    await step(bot, NOW + 380_000, ghost(NOW + 380_000))
    oid = bot.sim_trades()["ghost|up|吃"]["live"]["order_id"]
    fake.open.pop(oid)
    for i in range(3):  # three "no such order" answers within a minute of the placement: not yet (Predict's reads may lag a fresh order)
        await step(bot, NOW + 390_000 + i * 10_000, ghost(NOW + 390_000 + i * 10_000))
    assert bot.sim_trades()["ghost|up|吃"]["live"]["state"] == "open" and bot.live_misses["ghost|up|吃"] == 3
    await step(bot, NOW + 445_000, ghost(NOW + 445_000))  # a minute on, still nowhere, the removal unconfirmed: unknown, reserved, blocking
    ghost_t = bot.sim_trades()["ghost|up|吃"]
    assert ghost_t["live"]["state"] == "unknown" and any("状态未知" in e["what"] for e in ghost_t["live"]["events"]) and bot.live_reserved(ghost_t) > 0
    assert bot.live_status(NOW + 445_000)["unknown"][0]["order_id"] == oid and bot.live_status(NOW + 445_000)["unconfirmed"] == 1
    fake.open[oid] = {"id": oid, "status": "OPEN", "amount": str(100 * WEI), "amountFilled": "0", "order": {"hash": ghost_t["live"]["hash"]}}
    await step(bot, NOW + 455_000, ghost(NOW + 455_000))  # back on the list: found and tracked again
    await step(bot, NOW + 465_000, ghost(NOW + 465_000))  # ...and, a taker past its wait, cancelled for real
    ghost_t = bot.sim_trades()["ghost|up|吃#1"]
    assert any("找回了订单" in e["what"] for e in ghost_t["live"]["events"]) and ghost_t["live"]["final"] == "failed" and ("remove", [oid]) in fake.calls

    # --- the gate: every limit in words -----------------------------------------------------------------------------------------
    mk = hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW)
    trades = bot.sim_trades()
    assert "单笔 $120.00 超过 LIVE_MAX_ORDER_USD $100" in bot.live_room(trades, mk, "up", 0.6, 200)
    held = {"a": {"live": {}, "status": "filled", "shares": 100, "order": 100, "price": 0.9},
            "b": {"live": {}, "status": "resting", "shares": 20, "order": 100, "price": 0.5},
            "c": {"live": {}, "status": "filled", "shares": 20, "order": 100, "price": 1.0},
            "paper": {"status": "filled", "shares": 100, "order": 100, "price": 0.9}, "d": {"live": {}, "status": "settled", "shares": 100, "price": 0.9, "payout": 1}}
    assert bot.live_exposure(held) == 160.0  # filled shares and the unfilled rest of a resting order, live records only
    assert "持仓+挂单 $160.00 加本单 $95.00 超过 LIVE_MAX_OPEN_USD $250" in bot.live_room(held, mk, "up", 0.95, 100)
    assert bot.live_room(held, mk, "up", 0.9, 100) == ""
    bot.live.chain.usdt = 10 * WEI
    await bot.live.refresh_balance()
    assert "USDT 余额 $10.00 不足本单 $58.00" in bot.live_room({}, mk, "up", 0.58, 100)
    bot.live.chain.usdt = 1000 * WEI
    await bot.live.refresh_balance()
    bot.live.approvals["CTF_EXCHANGE"] = False
    assert "未授权交易所使用 USDT" in bot.live_room({}, mk, "up", 0.58, 100)
    bot.live.approvals["CTF_EXCHANGE"] = True
    bot.live.markets["101"] = L.parse_market(market_json("101", status="RESOLVED"), 200)
    assert "RESOLVED" in bot.live_room({}, mk, "up", 0.58, 100)
    bot.live.markets["101"] = L.parse_market(market_json("101", names=("A", "B")), 200)
    assert "结果名称无法确认方向，不下真实单（按结果顺序推定" in bot.live_room({}, mk, "down", 0.42, 100)  # a guessed side is never a real order
    bot.live.markets["101"] = L.parse_market(market_json("101", names=("Maybe", "Later")), 200)
    assert "无法确定要买的结果代币" in bot.live_room({}, dataclasses.replace(mk, kind="ladder"), "up", 0.58, 100)
    bot.live.markets["101"] = L.parse_market(market_json("101"), 200)
    bot.store.put("live:paused", {"at": NOW, "why": "试一下"})
    assert "已暂停（试一下" in bot.live_room({}, mk, "up", 0.58, 100)
    bot.store.delete_keys(["live:paused"])
    bot.live.ready_error = "HTTP 401"
    assert "未就绪：HTTP 401" in bot.live_room({}, mk, "up", 0.58, 100)
    bot.live.ready_error = ""
    # 跌: the other outcome's token at the 跌 price, never a sale of 涨
    fake.markets["109"] = market_json("109", names=("Yes", "No"), neg=True)
    down = lambda at: m.SimMarket("down", "down", "flip", "F", 0.20, book([("0.35", "30"), ("0.30", "500")], [("0.38", "100")], at, "down", mid="109"),
                                  0.02, "", ("Yes", "No"), {})
    await step(bot, NOW + 500_000, down(NOW + 500_000))
    t = bot.sim_trades()["down|down|吃"]
    body = [b for kind, b in fake.calls if kind == "create"][-1]["data"]
    assert t["label"] == "吃No" and body["order"]["tokenId"] == "1092" and body["order"]["side"] == 0 and t["live"]["want"]["neg_risk"]
    assert body["pricePerShare"] == str(69 * 10 ** 16) and t["cap"] == 0.69 and t["order"] == 100  # 跌 fair 80¢: the cap keeps 10¢ after the fee
    assert body["order"]["takerAmount"] == str(100 * WEI) and body["order"]["makerAmount"] == str(69 * WEI)  # only the 65¢ level is under it now; the rest waits
    assert body["order"]["hash"] == bot.live.wallet.order_hash(body["order"], True, False)  # signed for the multi-outcome exchange

    # --- settlement: the paper trader's, on the shares really filled; a loss trips the daily limit ------------------------------
    bot.note_outcome("HSI", "2026-10-05", 24500.0, "tencent 日K")  # below 24,600: 涨 loses
    world["markets"] = []
    await step(bot, CLOSE + 61 * 60_000)
    trades = bot.sim_trades()
    t = trades[taker_tid]
    assert t["status"] == "settled" and t["payout"] == 0.0 and t["confirm"] == "local" and abs(t["payout"] - t["price"]) * 100 > 50
    assert trades[maker_tid]["status"] == "settled" and trades[maker_tid]["shares"] == 30 and trades[maker_tid]["payout"] == 0.0
    assert bot.live_daily_pnl(trades, CLOSE + 61 * 60_000) < -50 and bot.live_killed().startswith("今日已结算亏损")
    assert "今日停止开新仓" in bot.live_room(trades, mk, "up", 0.58, 100) and bot.store.get("live:killed")["day"] == "2026-10-05"
    down_t = trades["down|down|吃#1"]  # its taker order had waited too long: cancelled, nothing filled, the attempt kept
    assert down_t["live"]["final"] == "failed" and ("remove", [down_t["live"]["order_id"]]) in fake.calls and "down|down|吃" not in trades
    assert bot.live_killed() and not m.beijing_day((CLOSE + 25 * 3_600_000) / 1000) == "2026-10-05"
    bot.market.now = CLOSE + 25 * 3_600_000
    assert bot.live_killed() == ""  # the next day trades again
    bot.market.now = CLOSE + 61 * 60_000
    # the administrator's commands
    req = lambda *args: m.Request("/live", list(args), 1, 0, 1)
    assert "▶️" in await bot.cmd_live(req("resume")) and bot.live_killed() == "" and bot.store.get("live:killed")["resumed"]
    bot.live_check_kill(bot.sim_trades(), CLOSE + 61 * 60_000)
    assert bot.live_killed() == ""  # resumed by hand: the same day's losses do not trip it again
    assert "⏸" in await bot.cmd_live(req("pause", "休息", "一下")) and bot.live_paused() == "休息 一下"
    reply = await bot.cmd_live(req())
    assert isinstance(reply, m.Reply) and "真实交易" in reply.text and "已暂停（休息 一下）" in reply.text and "已结算 2 笔" in reply.text, reply.text
    assert "最近的真实订单" in reply.text and "#102" in reply.text and "持仓+挂单" in reply.text and "日亏损 ≤$50" in reply.text
    await bot.cmd_live(req("resume"))
    assert "用法" in await bot.cmd_live(req("cancel")) and "没有订单号为 77" in await bot.cmd_live(req("cancel", "77"))
    fake.markets["110"] = market_json("110")
    rest = lambda at: m.SimMarket("rest", "rest", "close", "R", 0.70, book([("0.55", "100")], [("0.60", "100")], at, "rest", mid="110"), 0.03, "", ("涨", "跌"), {})
    bot.config = dataclasses.replace(bot.config, sim_ways="both")
    await step(bot, CLOSE + 62 * 60_000, rest(CLOSE + 62 * 60_000))
    assert "rest|up|挂" in bot.sim_trades(), (bot.sim_blocks()[:3], bot.live_errors[-3:], bot.live_status(bot.market.now_ms())["hold"])
    oid = bot.sim_trades()["rest|up|挂"]["live"]["order_id"]
    assert oid in fake.open and "策略挂单 1 笔：Predict 确认撤掉 1 笔" in await bot.cmd_live(req("cancel", "all"))
    t = bot.sim_trades()["rest|up|挂"]
    assert oid in fake.closed and t["status"] == "cancelled" and "管理员撤单" in t["note"] and t["live"]["final"] == "cancelled"
    assert "没有真实挂单可撤" in await bot.cmd_live(req("cancel", "all"))
    fake.positions_rows = [{"id": "p1", "market": {"id": 101, "title": "恒生指数 10-05"}, "outcome": {"name": "Down", "indexSet": 2, "onChainId": "1012"},
                            "amount": str(100 * WEI), "valueUsd": "100"}]
    assert "恒生指数 10-05｜Down 100 份｜≈$100.00" in await bot.cmd_live(req("positions"))
    assert "开放订单" in await bot.cmd_live(req("orders")) and "自检" in await bot.cmd_live(req("check"))
    assert "真实下单：LIVE=on" in bot.sim_text() and "/live" in bot.sim_text() and "💰" in bot.sim_text()
    assert "LIVE=on" in bot.cmd_help(None) and "不会自动下单" not in bot.cmd_help(None)
    assert "💰 真实交易" in bot.status("1:0") and "不会自动撤单" not in bot.status("1:0")
    assert bot.journal_payload()["live"] == {"enabled": True, "account": "0x8fd3…7A03", "ready": True, "paused": "", "killed": ""}
    assert bot.journal_csv().count("\n") >= 8 and json.dumps(bot.journal_payload(), default=str) and json.dumps(bot.sim_report())

    # --- redeeming: the winning outcome of a resolved market, once ----------------------------------------------------------------
    fake.markets["101"] = {**market_json("101", status="RESOLVED"), "outcomes": [{"name": "Up", "indexSet": 1, "onChainId": "1011", "status": "LOST"},
                                                                                {"name": "Down", "indexSet": 2, "onChainId": "1012", "status": "WON"}]}
    fake.positions_rows.append({"id": "p2", "market": {"id": 101, "title": "恒生指数 10-05"}, "outcome": {"name": "Up", "indexSet": 1, "onChainId": "1011"},
                                "amount": str(40 * WEI)})
    fake.positions_rows.append({"id": "p3", "market": {"id": 110, "title": "rest"}, "outcome": {"name": "Up", "indexSet": 1}, "amount": str(5 * WEI)})
    lines = await bot.live_redeem(CLOSE + 70 * 60_000)
    assert len(lines) == 1 and "已领取 恒生指数 10-05 Down 100 份：0xredeem" in lines[0], lines
    assert bot.live.chain.redeemed == [("0x" + "ab" * 32, 2, 100 * WEI, False, False)] and bot.store.get("live:redeemed:101:2")["tx"] == "0xredeem"
    assert await bot.live_redeem(CLOSE + 71 * 60_000) == [] and len(bot.live.chain.redeemed) == 1  # not twice; the loser and the open market untouched
    assert "没有可领取" in await bot.cmd_live(req("redeem"))
    bot.live.chain.fail = "交易失败（已回滚）"
    fake.positions_rows.append({"id": "p4", "market": {"id": 101, "title": "恒生指数 10-05"}, "outcome": {"name": "Down", "indexSet": 2, "onChainId": "1012"},
                                "amount": str(1 * WEI)})
    bot.store.delete_keys(["live:redeemed:101:2"])
    assert await bot.live_redeem(CLOSE + 72 * 60_000) == [] and "领取" in bot.live_errors[-1] and bot.store.get("live:redeemed:101:2") is None
    # the sync step never stops the paper trader: an API failure is a line in /live
    fake.fail_open = "HTTP 500: down"
    fake.markets["111"] = market_json("111")
    late = lambda at: m.SimMarket("late", "late", "close", "Z", 0.70, book([("0.55", "100")], [("0.60", "100")], at, "late", mid="111"), 0.03, "", ("涨", "跌"), {})
    await step(bot, CLOSE + 73 * 60_000, late(CLOSE + 73 * 60_000))
    assert bot.sim_trades()["late|up|挂"]["live"]["state"] == "open" and "同步真实订单失败" in bot.live_errors[-1]
    # while the list cannot be read, no new order (the cancel path still works); a successful read opens the gate again
    fake.markets["112"] = market_json("112")
    late2 = lambda at: m.SimMarket("late2", "late2", "close", "Z2", 0.70, book([("0.55", "100")], [("0.60", "100")], at, "late2", mid="112"), 0.03, "", ("涨", "跌"), {})
    await step(bot, CLOSE + 76 * 60_000, late(CLOSE + 76 * 60_000), late2(CLOSE + 76 * 60_000))
    assert "late2|up|挂" not in bot.sim_trades() and any(b["id"] == "late2|up|挂" and "没有同步成功" in b["why"] for b in bot.sim_blocks()), bot.sim_blocks()
    assert bot.live_status(CLOSE + 76 * 60_000)["hold"].startswith("账户订单已")
    fake.fail_open = None
    await step(bot, CLOSE + 77 * 60_000, late(CLOSE + 77 * 60_000), late2(CLOSE + 77 * 60_000))  # read again at the end of this step
    await step(bot, CLOSE + 78 * 60_000, late(CLOSE + 78 * 60_000), late2(CLOSE + 78 * 60_000))
    assert bot.sim_trades()["late2|up|挂"]["live"]["state"] == "open" and bot.live_status(CLOSE + 78 * 60_000)["hold"] == ""

    # --- the taker's LIMIT order: SIM_SHARES at the cap, whatever the book holds under it ------------------------------------------
    lbot = make_bot(SIM_WAYS="taker")
    await lbot.live_prepare()
    lbot.live.api.markets["103"] = market_json("103")
    world["markets"] = [deep(NOW)]
    await lbot.live_prefetch(NOW)
    await step(lbot, NOW, deep(NOW))
    body = lbot.live.api.calls[-1][1]["data"]
    assert body["strategy"] == "LIMIT" and body["pricePerShare"] == str(59 * 10 ** 16) and body["order"]["makerAmount"] == str(59 * WEI)
    assert body["order"]["expiration"] == str(NOW // 1000 + 300) and "slippageBps" not in body
    # fair 80¢: the cap is 69¢, both levels (58¢×40, 62¢×100) are under it, the order still asks for the full 100 at the cap
    world["markets"] = [m.SimMarket("two", "two", "close", "X", 0.80, book([], [("0.58", "40"), ("0.62", "100")], NOW + 10_000, "two", mid="103"), 0.03, "", ("涨", "跌"), {})]
    await step(lbot, NOW + 10_000)
    body = lbot.live.api.calls[-1][1]["data"]
    assert body["pricePerShare"] == str(69 * 10 ** 16) and body["order"]["takerAmount"] == str(100 * WEI) and lbot.sim_trades()["two|up|吃"]["cap"] == 0.69

    # a Predict account signs and trades for the smart wallet
    kbot = make_bot(PREDICT_ACCOUNT=PA, SIM_WAYS="maker")
    await kbot.live_prepare()
    world["markets"] = [hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW)]
    await kbot.live_prefetch(NOW)
    await step(kbot, NOW)
    body = kbot.live.api.calls[-1][1]["data"]
    assert body["order"]["maker"] == PA and body["order"]["signer"] == PA and body["order"]["signature"].startswith("0x01845adb2c".lower()[:4])
    assert body["order"]["signature"].lower().startswith("0x01" + L.ADDRESSES[56]["ECDSA_VALIDATOR"][2:].lower()) and len(body["order"]["signature"]) == 2 + 2 + 40 + 130
    assert any("Predict 账户" in line and "下单账户 0x1111…1111" in line and "签名钱包 0x8fd3…7A03" in line for line in kbot.live.summary_lines())
    assert kbot.live.ready and any(line.startswith("账户校验：✓") for line in kbot.live.summary_lines())
    # the key must control the Predict account (the validator says who); a plain wallet needs no such check
    kbot.live.chain.owner = "0x2222222222222222222222222222222222222222"
    try: await kbot.live.verify_account(); assert False
    except m.RemoteError as e: assert "不是 Predict 账户" in str(e) and "0x2222…2222" in str(e)
    assert not kbot.live.ready and "不是 Predict 账户" in kbot.live_room({}, hsi(0.70, [], [], NOW), "up", 0.5, 100) and kbot.live.ready_error == ""
    assert any(line.startswith("账户校验：✗") for line in kbot.live.summary_lines()) and "未就绪：PREDICT_PRIVATE_KEY" in kbot.live_text(NOW)
    kbot.live.chain.owner = "0x" + "0" * 40
    try: await kbot.live.verify_account(); assert False
    except m.RemoteError as e: assert "没有 Predict 账户" in str(e)
    kbot.live.chain.owner = ADDR.lower()
    await kbot.live.verify_account()
    assert kbot.live.ready and kbot.live.account_error == ""
    await bot.live.verify_account()  # a plain wallet: nothing to check
    assert bot.live.account_error == "" and (await bot.live_prepare()) and bot.live.ready
    # the on-chain read, decoded from the validator's answer
    class OwnerChain(L.Chain):
        async def rpc(self, method, params):
            assert method == "eth_call" and params[0]["to"] == L.ADDRESSES[56]["ECDSA_VALIDATOR"] and params[0]["data"].startswith("0x" + L.Chain.encode("ecdsaValidatorStorage(address)", ["address"], [PA]).hex()[:8])
            return "0x" + "0" * 24 + ADDR[2:].lower()
    assert (await OwnerChain("https://rpc.example", k).account_owner(PA)).lower() == ADDR.lower()

    # --- SIM_TAKER_SESSION: a daily card after hours (priced from a proxy) is not taken, only quoted; crypto cards unaffected ------
    sbot = make_bot(SIM_TAKER_SESSION="on", SIM_WAYS="both")
    await sbot.live_prepare()
    assert sbot.control_value("SIM_TAKER_SESSION") == "on" and sbot.sim_version()["sim_taker_session"] is True and "sim_taker_session" not in bot.sim_version()
    after_hours = dataclasses.replace(hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW), in_session=False)
    world["markets"] = [after_hours]
    await sbot.live_prefetch(NOW)
    await step(sbot, NOW, after_hours)
    assert sorted(sbot.sim_trades()) == [f"{HSI_SLUG}|up|挂"], sorted(sbot.sim_trades())  # the maker rests, the taker waits for the session
    sbot.live.api.markets["103"] = market_json("103")
    crypto = m.SimMarket("deep2", "deep2", "updown", "X", 0.70, book([], [("0.58", "100"), ("0.70", "1000")], NOW + 10_000, "deep2", mid="103"), 0.03, "",
                         ("涨", "跌"), {"key": "X", "end": CLOSE})  # in_session None: not a session market
    world["markets"] = [dataclasses.replace(after_hours, in_session=True), crypto]
    await sbot.live_prefetch(NOW + 10_000)
    await step(sbot, NOW + 10_000)
    assert sorted(sbot.sim_trades()) == ["deep2|up|吃", f"{HSI_SLUG}|up|吃", f"{HSI_SLUG}|up|挂"], sorted(sbot.sim_trades())
    assert "只在标的开盘时段吃单" in sbot.sim_text() and "只在标的开盘时段吃单" not in bot.sim_text()
    # the daily cards carry whether their underlying trades now (the odds come from the spot)
    cbot = m.Bot(m.Config.from_env({**base, "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"}), m.Store(":memory:"), FM(BJ(9, 28, 10, 5)), None)
    sat = cbot.market.now
    cbot.cn.quote = m.IndexQuote("上证指数", D("3860"), D("3850"), None, None, None, sat - 3000, "腾讯", fetched_ms=sat - 1000)
    cbot.cn.close = m.DailyClose(dt.date(2026, 9, 25), D("3850"), D("3840"), "腾讯日K", sat - 600_000)
    sse_slug = "sse-composite-index-up-or-down-on-september-28-2026"
    cbot.predict.slugs["SSE"] = sse_slug
    cbot.predict.books["SSE"] = m.PredictBook("SSE", sse_slug, "901", "t", ((D("0.40"), D("100")),), ((D("0.45"), D("100")),), sat)
    smk = next(x for x in cbot.sim_markets(sat) if x.kind == "close" and x.key == "SSE")
    assert smk.in_session is True and smk.evidence["basis"]["direct"]
    # the day moved on but the Predict refresh has not: yesterday's book (and the refresher's slug list) are still the old
    # market — not the one the card prices now, so it is not traded (10-08: 1¢ asks of a finished day bought at tomorrow's fair)
    old_slug = "sse-composite-index-up-or-down-on-september-25-2026"
    cbot.predict.slugs["SSE"] = old_slug
    cbot.predict.books["SSE"] = m.PredictBook("SSE", old_slug, "900", "t", ((D("0.01"), D("1000")),), ((D("0.01"), D("100")),), sat)
    assert not [x for x in cbot.sim_markets(sat) if x.kind == "close" and x.key == "SSE"]
    cbot.predict.books["SSE"] = m.PredictBook("SSE", sse_slug, "901", "t", ((D("0.40"), D("100")),), ((D("0.45"), D("100")),), sat)
    assert next(x for x in cbot.sim_markets(sat) if x.kind == "close" and x.key == "SSE").market == sse_slug  # the right day's book: traded

    # --- SIM_QUIET_MINUTES: from N minutes before a market's end no new order (refusals recorded), a resting one is withdrawn ----
    assert m.Config.from_env({**base, "SIM_QUIET_MINUTES": "15"}).sim_quiet_minutes == 15 and m.Config.from_env(base).sim_quiet_minutes == 0
    for bad in ("-1", "1441", "x"):
        try: m.Config.from_env({**base, "SIM_QUIET_MINUTES": bad}); assert False, bad
        except ValueError: pass
    assert m.sim_end_ms({"close_ms": CLOSE}) == CLOSE and m.sim_end_ms({"deadline": 5}) == 5 and m.sim_end_ms({"end": "7"}) == 7
    assert m.sim_end_ms({}) is None and m.sim_end_ms(None) is None and m.sim_end_ms({"end": "soon"}) is None
    qbot = make_bot(SIM_QUIET_MINUTES="15", SIM_WAYS="both")
    await qbot.live_prepare()
    assert qbot.control_value("SIM_QUIET_MINUTES") == "15" and qbot.sim_version()["sim_quiet_minutes"] == 15 and "sim_quiet_minutes" not in bot.sim_version()
    assert qbot.sim_quiet(SETTLE_HSI, CLOSE - 15 * 60_000 - 1) == "" and qbot.sim_quiet(SETTLE_HSI, CLOSE - 15 * 60_000) == "结束前 15 分钟不交易"
    assert qbot.sim_quiet(SETTLE_HSI, CLOSE + 1) != "" and qbot.sim_quiet({}, CLOSE) == ""  # past the end: still quiet; no end known: not
    # the end itself stops every bot, option or not (10-08 review): a bid left on a finished market is the other side's free option
    assert bot.sim_quiet(SETTLE_HSI, CLOSE - 1) == "" and bot.sim_quiet(SETTLE_HSI, CLOSE) == "已到结束时间，等结果出来：不开新单，挂单撤掉" and bot.sim_quiet({}, CLOSE + 1) == ""
    early = CLOSE - 60 * 60_000
    await qbot.live_prefetch(early)
    await step(qbot, early, hsi(0.70, [("0.55", "300")], [("0.58", "400")], early))
    assert sorted(qbot.sim_trades()) == [f"{HSI_SLUG}|up|吃", f"{HSI_SLUG}|up|挂"]  # an hour before the close: trades as usual
    late = CLOSE - 14 * 60_000
    qbot.live.api.markets["103"] = market_json("103")
    other = m.SimMarket("other", "other", "close", "X", 0.70, book([("0.55", "300")], [("0.58", "400")], late, "other", mid="103"), 0.03, "", ("涨", "跌"),
                        {"key": "X", "close_ms": CLOSE})
    await qbot.live_prefetch(late)
    await step(qbot, late, hsi(0.70, [("0.55", "300")], [("0.58", "400")], late), other)
    qt = qbot.sim_trades()
    assert sorted(qt) == [f"{HSI_SLUG}|up|吃#1", f"{HSI_SLUG}|up|挂#1"], sorted(qt)  # nothing new on either market (the unfilled market order lapsed after LIVE_TAKER_WAIT_SECONDS, as always; the maker withdrawn with nothing filled moved to #1)
    maker_rec = qt[f"{HSI_SLUG}|up|挂#1"]
    assert maker_rec["status"] == "cancelled" and maker_rec["note"] == "撤单：结束前 15 分钟不交易，一份都没成交" and maker_rec["withdrawn"]["why"] == "结束前 15 分钟不交易"
    assert maker_rec["live"]["state"] == "done" and maker_rec["live"]["cancel_why"] == "结束前 15 分钟不交易"  # the real order: cancel sent and confirmed in the same step
    assert ("remove", [maker_rec["live"]["order_id"]]) in qbot.live.api.calls
    assert any(b["id"] == "other|up|吃" and b["why"] == "结束前 15 分钟不交易" for b in qbot.sim_blocks()), qbot.sim_blocks()
    assert "结束前 15 分钟起不开新单" in qbot.sim_text() and "结束前" not in bot.sim_text()

    # --- the resting-order rules on a real order: our own shares make no 买1 valid; the cancel is confirmed before the re-placement
    mbot = make_bot(SIM_MAKER_MIN_BID="100", SIM_MAKER_EXIT_CENTS="5", SIM_MAKER_SESSION="on", SIM_WAYS="maker")
    await mbot.live_prepare()
    mfake = mbot.live.api
    await mbot.live_prefetch(NOW)
    await step(mbot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    mtid = f"{HSI_SLUG}|up|挂"
    mt = mbot.sim_trades()[mtid]
    assert mt["live"]["state"] == "open" and mt["price"] == 0.55 and mt["live"]["order_id"] == "101"
    # the real book now shows our 100 among the 55¢ bids: 400 there, 300 of them others → still the valid 买1
    await step(mbot, NOW + 10_000, hsi(0.70, [("0.55", "400")], [("0.58", "400")], NOW + 10_000))
    assert mbot.sim_trades()[mtid]["status"] == "resting"
    # the others leave: 55¢ holds only our 100, the valid 买1 is the 200 at 53¢ → cancelled on Predict, re-placed there next step
    await step(mbot, NOW + 20_000, hsi(0.70, [("0.55", "100"), ("0.53", "200")], [("0.58", "400")], NOW + 20_000))
    trades = mbot.sim_trades()
    old = trades[f"{mtid}#1"]
    assert mtid not in trades and old["status"] == "cancelled" and old["live"]["final"] == "cancelled" and ("remove", ["101"]) in mfake.calls
    assert old["live"]["cancel_why"] == "买1 移到 53.0¢，改跟" and old["live"]["rekeyed"] == f"{mtid}#1"
    await step(mbot, NOW + 30_000, hsi(0.70, [("0.53", "200")], [("0.58", "400")], NOW + 30_000))  # our cancelled 100 are gone from the book
    new = mbot.sim_trades()[mtid]
    assert new["price"] == 0.53 and new["live"]["order_id"] == "102" and new["live"]["want"]["price"] == 0.53
    # while a cancel is unconfirmed (the order list cannot be read), no fresh order goes out for that slot
    mfake.fail_open = "HTTP 500: boom"
    await step(mbot, NOW + 40_000, hsi(0.70, [("0.57", "300"), ("0.53", "200")], [("0.58", "400")], NOW + 40_000))
    trades = mbot.sim_trades()
    assert mtid not in trades and trades[f"{mtid}#2"]["live"]["state"] == "cancelling"
    await step(mbot, NOW + 50_000, hsi(0.70, [("0.57", "300"), ("0.53", "200")], [("0.58", "400")], NOW + 50_000))
    assert mtid not in mbot.sim_trades() and any(b["id"] == mtid and "撤单中" in b["why"] for b in mbot.sim_blocks()), mbot.sim_blocks()
    mfake.fail_open = None
    await step(mbot, NOW + 60_000, hsi(0.70, [("0.57", "300"), ("0.53", "200")], [("0.58", "400")], NOW + 60_000))
    assert mbot.sim_trades()[f"{mtid}#2"]["live"]["state"] == "done" and ("remove", ["102"]) in mfake.calls
    await step(mbot, NOW + 70_000, hsi(0.70, [("0.57", "300"), ("0.53", "200")], [("0.58", "400")], NOW + 70_000))
    assert mbot.sim_trades()[mtid]["price"] == 0.57 and mbot.sim_trades()[mtid]["live"]["order_id"] == "103"

    # --- execution safety: reserved until Predict's final word, cancels confirmed, unknown results reconciled, the pre-send check
    sbot = make_bot(SIM_WAYS="both")
    await sbot.live_prepare()
    sfake = sbot.live.api
    assert 0 <= time.monotonic() - sbot.live.approvals_mono < 60 and sbot.live.approvals_at > 1e9  # one clock for the interval, one for display
    await sbot.live_prefetch(NOW)
    await step(sbot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    s_trades = sbot.sim_trades()
    smaker, staker = s_trades[f"{HSI_SLUG}|up|挂"], s_trades[f"{HSI_SLUG}|up|吃"]
    assert abs(sbot.live_reserved(smaker) - 55.0) < 1e-9 and abs(sbot.live_reserved(staker) - 59.0) < 1e-9  # every unfilled share at the order's cap
    status0 = sbot.live_status(NOW)
    assert status0["unconfirmed"] == 0 and status0["synced_at"] and status0["unknown_open"] == [] and status0["allowed"] is True
    # a withdrawal releases nothing while the cancel is unconfirmed (the open list cannot be read): the reservation stays
    sfake.fail_open = "HTTP 500: boom"
    result = await sbot.live_control("cancel", {"id": "all"})
    text = result["message"]
    assert result["ok"] is False and text.startswith("❌") and "发送撤单请求失败" in text and "尚未发出" in text, text  # the page is told it did not succeed
    s_trades = sbot.sim_trades()
    assert s_trades[f"{HSI_SLUG}|up|挂"]["status"] == "cancelled" and s_trades[f"{HSI_SLUG}|up|挂"]["live"]["state"] == "cancelling"
    assert abs(sbot.live_exposure(s_trades) - 114.0) < 1e-9 and sbot.live_status(NOW)["unconfirmed"] == 2
    assert sbot.live_status(NOW)["orders"][0]["live_state"] == "cancelling" and "why" in sbot.live_status(NOW)["orders"][0]  # the active orders come first
    sfake.fail_open = None
    await step(sbot, NOW + 10_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 10_000))
    s_trades = sbot.sim_trades()
    assert all(t["live"]["state"] == "done" for t in s_trades.values()) and sbot.live_exposure(s_trades) == 0.0  # confirmed: released
    assert any(("Predict 确认已撤" in e["what"]) for e in s_trades[f"{HSI_SLUG}|up|挂"]["live"]["events"])
    # an order whose id never came back (the answer was lost): the cancel asks Predict by the hash, adopts the id, removes it
    sfake.markets["103"] = market_json("103")
    await sbot.live_prefetch(NOW + 20_000)
    await step(sbot, NOW + 20_000, deep(NOW + 20_000))
    d_rec = sbot.sim_trades()["deep|up|吃"]
    lost_id = d_rec["live"]["order_id"]
    d_rec["live"].update(order_id="", state="cancelling", cancel_why="测试", placing_at=NOW + 20_000)
    d_rec["status"] = "cancelled"
    sbot.sim_save([("deep|up|吃", d_rec)])
    await step(sbot, NOW + 30_000, deep(NOW + 30_000))
    d2 = sbot.sim_trades()["deep|up|吃#1"]
    assert d2["live"]["order_id"] == lost_id and ("remove", [lost_id]) in sfake.calls and d2["live"]["state"] == "done"
    assert any("撤单前按哈希找到" in e["what"] for e in d2["live"]["events"]), d2["live"]["events"]
    # a hash Predict does not know: the record waits through the grace period, then is unknown (reserved) until released; no hash at all ends at once
    sfake.markets["110"] = market_json("110")
    rest_mk = lambda at: m.SimMarket("rest2", "rest2", "close", "R2", 0.70, book([("0.55", "300")], [("0.60", "100")], at, "rest2", mid="110"), 0.03, "", ("涨", "跌"), {"key": "R2", "close_ms": CLOSE})
    await sbot.live_prefetch(NOW + 40_000)
    await step(sbot, NOW + 40_000, rest_mk(NOW + 40_000))
    r_rec = sbot.sim_trades()["rest2|up|挂"]
    real_id = r_rec["live"]["order_id"]
    sfake.open.pop(real_id)  # the real one is gone from the fake book: this record now stands for a request Predict never saw
    r_rec["live"].update(order_id="", hash="0x" + "77" * 32, state="cancelling", cancel_why="测试", placing_at=NOW + 40_000)
    r_rec["status"] = "cancelled"
    sbot.sim_save([("rest2|up|挂", r_rec)])
    await step(sbot, NOW + 50_000, rest_mk(NOW + 50_000))
    assert sbot.sim_trades()["rest2|up|挂"]["live"]["state"] == "cancelling"  # within the grace period: still asked for
    assert abs(sbot.live_reserved(sbot.sim_trades()["rest2|up|挂"]) - 55.0) < 1e-9  # and still reserved
    await step(sbot, NOW + 50_000 + sbot.LIVE_PLACING_GRACE_MS + 10_000, rest_mk(NOW + 50_000 + sbot.LIVE_PLACING_GRACE_MS + 10_000))
    r2 = sbot.sim_trades()["rest2|up|挂"]
    assert r2["live"]["state"] == "unknown" and abs(sbot.live_reserved(r2) - 55.0) < 1e-9, r2["live"]  # past the grace: unknown and still reserved, never "cancelled" on silence
    assert (await sbot.live_control("release", {"id": "rest2|up|挂"}))["ok"]  # an operator checked the site
    r2 = sbot.sim_trades()["rest2|up|挂"]
    assert r2["live"]["state"] == "done" and sbot.live_reserved(r2) == 0.0 and "管理员确认" in r2["note"]
    sfake.markets["111"] = market_json("111")
    rest3 = lambda at: m.SimMarket("rest3", "rest3", "close", "R3", 0.70, book([("0.55", "300")], [("0.60", "100")], at, "rest3", mid="111"), 0.03, "", ("涨", "跌"), {"key": "R3", "close_ms": CLOSE})
    await sbot.live_prefetch(NOW + 300_000)
    await step(sbot, NOW + 300_000, rest3(NOW + 300_000))
    r3 = sbot.sim_trades()["rest3|up|挂"]
    sfake.open.pop(r3["live"]["order_id"])
    r3["live"].update(order_id="", hash="", state="cancelling", cancel_why="测试")
    r3["status"] = "cancelled"
    sbot.sim_save([("rest3|up|挂", r3)])
    calls_before = len(sfake.calls)
    await step(sbot, NOW + 310_000, rest3(NOW + 310_000))
    assert sbot.sim_trades()["rest3|up|挂"]["live"]["state"] == "done" and len([c for c in sfake.calls[calls_before:] if c[0] == "remove"]) == 0  # never built: nothing to ask Predict
    # the open list misses an order whose own record still says OPEN: it is not ended; when the record says FILLED it is
    sfake.markets["112"] = market_json("112")
    gone = lambda at: m.SimMarket("gone", "gone", "close", "G", 0.70, book([], [("0.58", "100"), ("0.70", "1000")], at, "gone", mid="112"), 0.03, "", ("涨", "跌"), {"key": "G", "close_ms": CLOSE})
    await sbot.live_prefetch(NOW + 320_000)
    await step(sbot, NOW + 320_000, gone(NOW + 320_000))
    g_rec = sbot.sim_trades()["gone|up|吃"]
    g_row = sfake.open.pop(g_rec["live"]["order_id"])
    sfake.closed[g_rec["live"]["order_id"]] = g_row  # findable by id / hash, but no longer in the open list; still OPEN
    await step(sbot, NOW + 330_000, gone(NOW + 330_000))
    g2 = sbot.sim_trades()["gone|up|吃"]
    assert g2["live"]["state"] == "open" and g2["status"] == "resting" and any("继续跟踪" in e["what"] for e in g2["live"]["events"])
    assert abs(sbot.live_reserved(g2) - 59.0) < 1e-9
    g_row["status"], g_row["amountFilled"] = "FILLED", g_row["amount"]
    await step(sbot, NOW + 340_000, gone(NOW + 340_000))
    g3 = sbot.sim_trades()["gone|up|吃"]
    assert g3["live"]["state"] == "done" and g3["live"]["final"] == "filled" and g3["shares"] == 100 and g3["status"] == "filled"
    # a 5xx on sending leaves the result unknown: the record stays "placing" (nothing is re-sent) and is reconciled by its hash —
    # here Predict had filled it at once, so the position is recognised rather than the attempt failed
    sfake.markets["113"] = market_json("113")
    fast = lambda at: m.SimMarket("fast", "fast", "close", "F", 0.70, book([], [("0.58", "100"), ("0.70", "1000")], at, "fast", mid="113"), 0.03, "", ("涨", "跌"), {"key": "F", "close_ms": CLOSE})
    sfake.fail_create = "HTTP 502: bad gateway"
    await sbot.live_prefetch(NOW + 400_000)
    await step(sbot, NOW + 400_000, fast(NOW + 400_000))
    f_rec = sbot.sim_trades()["fast|up|吃"]
    assert f_rec["live"]["state"] == "placing" and any("结果未知" in e["what"] for e in f_rec["live"]["events"]) and abs(sbot.live_reserved(f_rec) - 59.0) < 1e-9
    sfake.fail_create = None
    sfake.closed["900"] = {"id": "900", "status": "FILLED", "amount": str(100 * WEI), "amountFilled": str(100 * WEI), "order": {"hash": f_rec["live"]["hash"]}}
    await step(sbot, NOW + 410_000, fast(NOW + 410_000))
    f2 = sbot.sim_trades()["fast|up|吃"]
    assert f2["live"]["order_id"] == "900" and f2["shares"] == 100 and f2["status"] == "filled" and any("按哈希找回" in e["what"] for e in f2["live"]["events"])
    await step(sbot, NOW + 420_000, fast(NOW + 420_000))
    assert sbot.sim_trades()["fast|up|吃"]["live"]["state"] == "done" and len([1 for k, _ in sfake.calls if k == "create"]) == len([1 for k, _ in sfake.calls if k == "create"])
    # an answer without an order id is unknown too; a 4xx is a definite refusal (failed at once, backed off)
    sfake.markets["114"] = market_json("114")
    noid = lambda at: m.SimMarket("noid", "noid", "close", "N", 0.70, book([], [("0.58", "100"), ("0.70", "1000")], at, "noid", mid="114"), 0.03, "", ("涨", "跌"), {"key": "N", "close_ms": CLOSE})
    sfake.fail_create = "下单应答没有订单号：{}"
    await sbot.live_prefetch(NOW + 500_000)
    await step(sbot, NOW + 500_000, noid(NOW + 500_000))
    assert sbot.sim_trades()["noid|up|吃"]["live"]["state"] == "placing"
    sfake.fail_create = None
    await step(sbot, NOW + 500_000 + sbot.LIVE_PLACING_GRACE_MS + 10_000, noid(NOW + 500_000 + sbot.LIVE_PLACING_GRACE_MS + 10_000))
    nt = sbot.sim_trades()["noid|up|吃"]  # never found by its hash: unknown (reserved, never re-sent) until Predict's word, its expiry or a release
    assert nt["live"]["state"] == "unknown" and sbot.live_reserved(nt) > 0 and len([1 for k, _ in sfake.calls if k == "create"]) == len([1 for k, _ in sfake.calls if k == "create"])
    assert (await sbot.live_control("release", {"id": "noid|up|吃"}))["ok"] and "noid|up|吃" not in sbot.sim_trades() and sbot.sim_trades()["noid|up|吃#1"]["live"]["state"] == "done"
    # the pre-send check: a pending record is not sent in pause, nor when its decision is stale
    sfake.markets["115"] = market_json("115")
    late_mk = lambda at: m.SimMarket("late2", "late2", "close", "L2", 0.70, book([], [("0.58", "100"), ("0.70", "1000")], at, "late2", mid="115"), 0.03, "", ("涨", "跌"), {"key": "L2", "close_ms": CLOSE})
    await sbot.live_prefetch(NOW + 600_000)
    await step(sbot, NOW + 600_000, late_mk(NOW + 600_000))
    l_rec = sbot.sim_trades()["late2|up|吃"]
    sfake.open.pop(l_rec["live"]["order_id"])
    l_rec["live"].update(state="pending", order_id="", hash="")  # as if restored before it was ever sent
    sbot.sim_save([("late2|up|吃", l_rec)])
    sbot.apply_config(dataclasses.replace(sbot.config, live_mode="pause"))
    creates = len([1 for k, _ in sfake.calls if k == "create"])
    await step(sbot, NOW + 610_000, late_mk(NOW + 610_000))
    assert len([1 for k, _ in sfake.calls if k == "create"]) == creates and sbot.sim_trades()["late2|up|吃#1"]["note"] == "未下单：LIVE=pause，不下单"
    sbot.apply_config(dataclasses.replace(sbot.config, live_mode="on"))
    sbot.live_backoff.clear()
    stale_rec = dict(sbot.sim_trades()["late2|up|吃#1"])
    stale_rec["live"] = {**stale_rec["live"], "state": "pending", "order_id": "", "hash": "", "events": [{"at": NOW + 400_000, "what": "决定下单"}]}
    stale_rec["status"] = "resting"
    sbot.sim_save([("late2|up|吃", stale_rec)])
    await step(sbot, NOW + 620_000, late_mk(NOW + 620_000))
    assert len([1 for k, _ in sfake.calls if k == "create"]) == creates and "决定已过时" in sbot.sim_trades()["late2|up|吃#2"]["note"]
    # orders on the account this bot never placed: listed, announced, not counted; /live cancel account removes them too
    sfake.open["777"] = {"id": "777", "status": "OPEN", "amount": str(10 * WEI), "amountFilled": "0", "order": {"hash": "0xforeign"}}
    await step(sbot, NOW + 700_000, late_mk(NOW + 700_000))  # nothing in flight: the list is still re-read every minute
    assert [u["id"] for u in sbot.live_unknown_open] == ["777"] and "没有记录的挂单" in sbot.live_text(NOW + 700_000) and sbot.live_status(NOW + 700_000)["unknown_open"][0]["id"] == "777"
    text = await sbot.cmd_live(m.Request("/live", ["cancel", "account"], 1, 0, 1))
    assert ("remove", ["777"]) in sfake.calls and "账户上无记录的挂单 1 张：Predict 确认撤掉 1 张" in text and sbot.live_unknown_open == [], text
    # the hard switch: LIVE_TRADING_ALLOWED=off caps the mode at pause, refuses the switch to on and the sending alike
    hc = m.Config.from_env({**base, "SYMBOLS": "UNITREEUSDT", "LIVE": "on", "PREDICT_PRIVATE_KEY": KEY, "LIVE_TRADING_ALLOWED": "off"})
    assert hc.live_mode == "pause" and hc.live_allowed is False and hc.live
    hbot = make_bot(LIVE_TRADING_ALLOWED="off")
    await hbot.live_prepare()
    assert hbot.config.live_mode == "pause" and "硬开关" in "\n".join(hbot.live.summary_lines()) and hbot.live_status(NOW)["allowed"] is False
    assert "LIVE_TRADING_ALLOWED=off" in await hbot.cmd_live(m.Request("/live", ["mode", "on"], 1, 0, 1)) and hbot.config.live_mode == "pause"
    hbot.apply_config(dataclasses.replace(hbot.config, live_mode="on"))  # even forced on in memory, nothing goes out
    assert "LIVE_TRADING_ALLOWED=off" in hbot.live_room({}, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW), "up", 0.58, 100)
    assert "LIVE_TRADING_ALLOWED=off" in hbot.live_send_block({"live": {"want": {"shares": 100}, "events": [{"at": NOW}]}, "order": 100, "price": 0.5, "opened": NOW}, {}, NOW)
    # a failing page action is not reported as a success
    sbot.live.ready_error = "HTTP 401"
    assert (await sbot.live_control("test", {}))["ok"] is False
    sbot.live.ready_error = ""

    # --- /live test: a tiny resting order far under the market, listed, withdrawn, its final state read ---------------------
    tbot = make_bot(SIM_WAYS="both")
    tfake = tbot.live.api
    await tbot.live_prepare()
    world["markets"] = []
    assert "没有可用的市场" in await tbot.live_action("test", [], NOW)
    world["markets"] = [hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW)]
    text = await tbot.live_action("test", [], NOW)
    body = [b for k, b in tfake.calls if k == "create"][-1]["data"]
    assert body["strategy"] == "LIMIT" and body["order"]["tokenId"] == "1011" and body["pricePerShare"] == str(2 * 10 ** 16)  # 2¢: under the 55¢ bid
    assert body["order"]["takerAmount"] == str(50 * WEI) and body["order"]["makerAmount"] == str(WEI) and body["order"]["expiration"] == str(NOW // 1000 + 600)  # 50 × 2¢ = the $1 minimum
    assert text.startswith("🧪 挂单测试") and "恒生指数" in text and "2.0¢×50 份" in text and "最多花 $1.00" in text and "改为" not in text, text
    assert "1/4 下单成功：订单 #" in text and "2/4 开放订单列表：已列出" in text and "3/4 撤单：已撤" in text and "4/4 最终状态：CANCELLED，成交 0/50 份" in text, text
    assert "低于 Predict 最低订单金额 $1" in tbot.live_room({}, world["markets"][0], "up", 0.58, 1)  # a real decision under the minimum is refused, not sent
    assert "最低订单金额" not in tbot.live_room({}, world["markets"][0], "up", 0.58, 2)
    assert ("remove", [tbot.store.get("live:test")["order_id"]]) in tfake.calls and tbot.store.get("live:test")["text"] == text
    assert tbot.live_status(NOW)["last_test"] == text and "最近挂单测试：" in tbot.live_text(NOW) and "deep2|up|吃" not in tbot.sim_trades()
    assert len([1 for kind, _ in tfake.calls if kind == "create"]) == 1 and not tbot.sim_trades()  # a test is never a paper record
    # arguments: the market by name, the shares, the price in cents (a price at the bid may trade, and says so)
    world["markets"] = [hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW),
                        m.SimMarket("other", "BTC 先触", "updown", "X", 0.7, book([("0.30", "10")], [("0.35", "10")], NOW, "other", mid="103"), 0.03, "", ("涨", "跌"), {})]
    tfake.markets["103"] = market_json("103")
    text = await tbot.live_action("test", ["btc", "3", "30"], NOW)
    body = [b for k, b in tfake.calls if k == "create"][-1]["data"]
    assert "BTC 先触" in text and body["order"]["takerAmount"] == str(4 * WEI) and body["pricePerShare"] == str(30 * 10 ** 16) and "可能成交" in text
    assert "3 份不足 Predict 最低订单金额 $1，改为 4 份" in text and "30.0¢×4 份" in text and "最多花 $1.20" in text, text  # raised to the minimum, and said so
    text = await tbot.live_action("test", ["btc", "4", "30"], NOW)
    assert "改为" not in text and "30.0¢×4 份" in text, text
    assert "没有可用的市场" in await tbot.live_action("test", ["nothing-like-this"], NOW) and "0.01～1000" in await tbot.live_action("test", ["5000"], NOW)
    # a refusal is reported with the request body (without the signature) so the API's complaint can be read against it
    tfake.fail_create = "HTTP 400: order value below minimum"
    text = await tbot.live_action("test", [], NOW)
    assert "1/4 下单失败：HTTP 400: order value below minimum" in text and "请求体（不含签名）" in text and "\"signature\":" not in text and "signatureType" in text and "2/4" not in text
    tfake.fail_create = None
    # a market whose best bid leaves no room under it
    world["markets"] = [m.SimMarket("low", "low", "close", "L", 0.1, book([("0.005", "10")], [("0.02", "10")], NOW, "low", mid="103"), 0.03, "", ("涨", "跌"), {})]
    assert "放不下更低的测试挂单" in await tbot.live_action("test", [], NOW)
    tbot.live.ready_error = "HTTP 401"
    assert "未就绪" in await tbot.live_action("test", [], NOW)
    tbot.live.ready_error = ""

    # --- LIVE=pause: signed in, checks and the order test work, nothing opens ------------------------------------------------
    pbot2 = make_bot(LIVE="pause", SIM_WAYS="both")
    await pbot2.live_prepare()
    assert pbot2.live.ready and pbot2.config.live_mode == "pause"
    world["markets"] = [hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW)]
    await pbot2.live_prefetch(NOW)
    why = pbot2.live_room({}, world["markets"][0], "up", 0.58, 100)
    assert why.startswith("LIVE=pause") and "不开新仓" in why
    await step(pbot2, NOW, world["markets"][0])
    assert not pbot2.sim_trades() and not [1 for kind, _ in pbot2.live.api.calls if kind == "create"]  # refused, not even on paper
    assert any(b["id"].startswith(HSI_SLUG) and "LIVE=pause" in b["why"] for b in pbot2.sim_blocks())
    text = await pbot2.live_action("test", [], NOW)
    assert "1/4 下单成功" in text and "3/4 撤单：已撤" in text  # the order test is allowed: it is how the mode is meant to be used
    assert "LIVE=pause" in pbot2.live_text(NOW) and "不开新仓" in pbot2.live_status(NOW)["ready_error"] + pbot2.live_text(NOW) and pbot2.live_status(NOW)["mode"] == "pause"
    assert "LIVE=pause：已登录但不开新仓" in pbot2.sim_text() and "LIVE=pause，不开新仓" in pbot2.status("1:0")
    assert bot.live_status(NOW)["mode"] == "on"

    # --- LIVE=off with a key: the module is loaded, the trader trades on paper; /live mode switches it while running -----------
    obot = make_bot(LIVE="off", SIM_WAYS="both")
    assert obot.config.live_mode == "off" and not obot.config.live and obot.live_room({}, hsi(0.70, [], [], NOW), "up", 0.5, 100) == ""
    assert "LIVE=off" in obot.live_text(NOW) and obot.sim_version() == m.Bot.sim_version(obot) and "LIVE=off" in obot.status("1:0")
    assert "LIVE=off，只记账" in obot.status("1:0") and "/live mode pause" in obot.sim_text() and "LIVE=off，只记账" in obot.cmd_help(None)
    world["markets"] = [hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW)]
    await step(obot, NOW, world["markets"][0])
    ot = obot.sim_trades()
    assert sorted(ot) == [f"{HSI_SLUG}|up|吃", f"{HSI_SLUG}|up|挂"] and "live" not in ot[f"{HSI_SLUG}|up|吃"] and ot[f"{HSI_SLUG}|up|吃"]["status"] == "filled"
    assert ot[f"{HSI_SLUG}|up|吃"]["fills"][0]["how"] == "吃单立即成交" and not obot.live.api.calls and not obot.live.ready  # paper, no sign-in
    await step(obot, NOW + 10_000, hsi(0.70, [("0.54", "200")], [("0.55", "30"), ("0.60", "100")], NOW + 10_000))
    assert obot.sim_trades()[f"{HSI_SLUG}|up|挂"]["shares"] == 30  # presumed fills, as on paper
    assert "用法" in await obot.cmd_live(m.Request("/live", ["mode"], 1, 0, 1)) and "用法" in await obot.cmd_live(m.Request("/live", ["mode", "maybe"], 1, 0, 1))
    assert "已经是 off" in await obot.cmd_live(m.Request("/live", ["mode", "off"], 1, 0, 1))
    text = await obot.cmd_live(m.Request("/live", ["mode", "pause"], 1, 0, 1))
    assert "off → pause" in text and "/live test" in text and obot.config.live_mode == "pause" and obot.live.config.live_mode == "pause"
    assert obot.store.get("control:env") == {"LIVE": "pause"} and obot.live.ready and obot.control_value("LIVE") == "pause"
    assert "LIVE=pause" in obot.live_room({}, world["markets"][0], "up", 0.58, 100)
    fake2 = obot.live.api
    fake2.markets["103"] = market_json("103")
    text = await obot.cmd_live(m.Request("/live", ["mode", "ON"], 1, 0, 1))
    assert "pause → on" in text and "⚠️" in text and obot.config.live_mode == "on" and obot.sim_version()["live"] == "on"
    world["markets"] = [deep(NOW + 20_000)]
    await obot.live_prefetch(NOW + 20_000)
    await step(obot, NOW + 20_000)
    assert obot.sim_trades()["deep|up|吃"]["live"]["state"] == "open" and [k for k, _ in fake2.calls if k == "create"]  # real now
    fake2.markets["110"] = market_json("110")
    rest2 = m.SimMarket("rest2", "rest2", "close", "R2", 0.70, book([("0.55", "100")], [("0.60", "100")], NOW + 30_000, "rest2", mid="110"), 0.03, "", ("涨", "跌"), {})
    await obot.live_prefetch(NOW + 30_000)
    await step(obot, NOW + 30_000, rest2)
    assert obot.sim_trades()["rest2|up|挂"]["live"]["state"] == "open"
    text = await obot.cmd_live(m.Request("/live", ["mode", "off"], 1, 0, 1))
    assert "on → off" in text and obot.config.live_mode == "off" and obot.store.get("control:env") == {"LIVE": "off"}
    t2 = obot.sim_trades()["rest2|up|挂"]
    assert t2["status"] == "cancelled" and t2["live"]["state"] == "cancelling" and "切换到 LIVE=off" in t2["note"]  # withdrawn at once
    await step(obot, NOW + 40_000)  # the next step sends the cancel and follows the taker to its end, LIVE=off or not
    assert ("remove", [t2["live"]["order_id"]]) in fake2.calls and obot.sim_trades()["rest2|up|挂"]["live"]["state"] == "done"
    # the saved mode outlives a restart (the variable is only the initial value)
    obot2 = L.LiveBot(m.Config.from_env({**base, "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off", "LIVE": "on",
                                         "PREDICT_PRIVATE_KEY": KEY}), obot.store, FM(NOW), None)
    assert obot2.config.live_mode == "off" and obot2.live.config.live_mode == "off"
    # without a key the base bot explains what to configure
    assert "PREDICT_PRIVATE_KEY" in m.Bot(m.Config.from_env({**base, "SYMBOLS": "UNITREEUSDT"}), m.Store(":memory:"), FM(NOW), None).cmd_live(None)
    # a key cannot be added from the page or the command
    r = obot.control_set({"LIVE": "on"})
    assert r["ok"] and obot.config.live_mode == "on"
    assert not m.Bot(m.Config.from_env({**base, "SYMBOLS": "UNITREEUSDT"}), m.Store(":memory:"), FM(NOW), None).control_set({"LIVE": "pause"})["ok"]

    # --- LIVE off → pause: the paper orders resting from paper mode are withdrawn (their slots freed) and placed again as real orders
    wbot = make_bot(LIVE="off", SIM_WAYS="maker")
    world["markets"] = [hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW)]
    await step(wbot, NOW, world["markets"][0])
    wtid = f"{HSI_SLUG}|up|挂"
    assert wbot.sim_trades()[wtid]["status"] == "resting" and "live" not in wbot.sim_trades()[wtid]  # a paper order
    text = await wbot.cmd_live(m.Request("/live", ["mode", "pause"], 1, 0, 1))
    assert "off → pause" in text
    wt = wbot.sim_trades()
    assert wtid not in wt and wt[f"{wtid}#1"]["status"] == "cancelled" and wt[f"{wtid}#1"]["note"] == "撤单：切到真实交易，按真实订单重挂，一份都没成交"
    await wbot.cmd_live(m.Request("/live", ["mode", "on"], 1, 0, 1))
    await wbot.live_prefetch(NOW + 10_000)
    await step(wbot, NOW + 10_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 10_000))
    assert wbot.sim_trades()[wtid]["live"]["state"] == "open" and wbot.sim_trades()[wtid]["price"] == 0.55  # the real one

    # paper mode: the base bot answers /live with a plain refusal, and trades on paper as before
    pbot = m.Bot(m.Config.from_env({**base, "SYMBOLS": "UNITREEUSDT", "HSI_FUTURES": "off", "KOSPI_INDEX": "off"}), m.Store(":memory:"), FM(NOW), None)
    assert "PREDICT_PRIVATE_KEY" in pbot.cmd_live(None) and "/live mode" in pbot.cmd_live(None) and not any(name == "真实交易" for name, _ in pbot.reference_jobs())
    assert L.sim_status({"status": "filled", "payout": 1, "price": 0.5, "shares": 10}) == "持仓"  # the base words, untouched for paper records
    print("LIVE_OK")


async def review_fixes():
    """The 10-08 review of the order lifecycle: the kill switch before anything goes out and whatever the list read; a
    placement the client refuses stays pending; a cancel Predict does not confirm is re-asked and the fills meanwhile
    count; a vanished order that the removal finds after all; a taker order withdrawn at the market's end; the hard
    switch on a raw LIVE=on; a failed market read not repeated every step."""
    # kill switch: tripped by this step's settlements before the pending order goes out, with the open-order read failing
    kbot = make_bot(SIM_WAYS="taker")
    await kbot.live_prepare(); kfake = kbot.live.api
    await kbot.live_prefetch(NOW)
    kbot.live_daily_pnl = lambda trades, now_ms: -60.0  # today's settled losses, past the $50 cap
    kfake.fail_open = "HTTP 500: down"
    await step(kbot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    assert kbot.live_killed() and not [c for c in kfake.calls if c[0] == "create"], kfake.calls
    kt = kbot.sim_trades()[f"{HSI_SLUG}|up|吃#1"]
    assert kt["live"]["final"] == "failed" and "今日停止开新仓" in kt["note"], kt["note"]
    kfake.fail_open = None
    # a cooldown: the order is not signed and sent into it; it stays pending and goes out once the cooldown is over
    cbot = make_bot(SIM_WAYS="taker")
    await cbot.live_prepare(); cfake = cbot.live.api
    await cbot.live_prefetch(NOW)
    cfake.blocked_until = time.monotonic() + 30
    await step(cbot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    assert f"{HSI_SLUG}|up|吃" not in cbot.sim_trades() and not [c for c in cfake.calls if c[0] == "create"]  # nothing even decided into the cooldown
    assert any(b["id"] == f"{HSI_SLUG}|up|吃" and "限流冷却中" in b["why"] for b in cbot.sim_blocks()), cbot.sim_blocks()
    pend = {"live": {"want": {"shares": 100.0, "price": 0.58, "cap": 0.59, "maker": False}, "events": [{"at": NOW}], "state": "pending", "hash": ""}, "order": 100.0, "price": 0.58, "opened": NOW, "maker": False,
            "market": "x", "side": "up", "item": "x", "label": "吃涨", "status": "resting", "shares": 0.0}
    await cbot.live_place("x|up|吃", pend, NOW)  # a record pending from before the cooldown: not signed, not sent, stays pending
    assert pend["live"]["state"] == "pending" and not cfake.calls and any(e.get("what", "").startswith("暂未发送：Predict 接口限流冷却中") for e in pend["live"]["events"]), pend["live"]
    pend["live"]["state"], pend["status"] = "done", "cancelled"
    cbot.sim_save([("x|up|吃", pend)])  # out of the way
    cfake.blocked_until = 0.0
    await step(cbot, NOW + 10_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 10_000))
    assert cbot.sim_trades()[f"{HSI_SLUG}|up|吃"]["live"]["state"] == "open" and len([c for c in cfake.calls if c[0] == "create"]) == 1
    # a cancel answered "noop" while the order stays listed: its fills count, the removal is asked again after a while
    nbot = make_bot(SIM_WAYS="maker")
    await nbot.live_prepare(); nfake = nbot.live.api
    await nbot.live_prefetch(NOW)
    await step(nbot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    ntid = f"{HSI_SLUG}|up|挂"
    oid = nbot.sim_trades()[ntid]["live"]["order_id"]
    async def noop(ids):
        nfake.calls.append(("remove", list(ids))); return {"removed": [], "noop": list(ids)}
    nfake.remove_orders = noop
    nbot.control_set({"SIM_MARKETS": "touch"})  # the paper trader withdraws it (and opens nothing else here)
    await step(nbot, NOW + 5_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 5_000))
    nt = nbot.sim_trades()[f"{ntid}#1"]
    assert nt["live"]["state"] == "cancelling" and [c for c in nfake.calls if c[0] == "remove"] == [("remove", [oid])], (nt["live"], nfake.calls)
    nfake.fill(oid, 40)
    await step(nbot, NOW + 10_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 10_000))
    nt = nbot.sim_trades()[f"{ntid}#1"]
    assert nt["shares"] == 40 and nt["live"]["state"] == "cancelling" and len([c for c in nfake.calls if c[0] == "remove"]) == 1, nt["live"]
    await step(nbot, NOW + 40_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 40_000))  # 35 s after the first ask: again
    await step(nbot, NOW + 45_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 45_000))
    assert len([c for c in nfake.calls if c[0] == "remove"]) == 2 and any("再发一次" in e["what"] for e in nbot.sim_trades()[f"{ntid}#1"]["live"]["events"])
    assert abs(nbot.live_exposure(nbot.sim_trades()) - 55.0) < 1e-9  # 40 filled at 55¢ plus 60 still reserved
    # a vanished order the removal then finds: cancelled (it existed), never "unknown"
    vbot = make_bot(SIM_WAYS="taker")
    await vbot.live_prepare(); vfake = vbot.live.api
    await vbot.live_prefetch(NOW)
    await step(vbot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    vtid = f"{HSI_SLUG}|up|吃"
    async def no_list(status="OPEN", **params): return []
    async def no_order(oid, hash_=""): return None
    vfake.orders, vfake.order = no_list, no_order  # hidden from every read, yet the removal reaches it
    for at in (NOW + 10_000, NOW + 20_000, NOW + 30_000):
        await step(vbot, at, hsi(0.70, [("0.55", "300")], [("0.58", "400")], at))
    assert vbot.sim_trades()[vtid]["live"]["state"] == "open"
    await step(vbot, NOW + 65_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 65_000))
    vt = vbot.sim_trades()[f"{vtid}#1"]
    assert vt["live"]["final"] == "failed" and any("它其实还在" in e["what"] for e in vt["live"]["events"]) and not any("UNKNOWN" in e["what"] for e in vt["live"]["events"]), vt["live"]["events"]
    # a taker order still resting when the market ends is withdrawn (a bid left on a finished market is a free option)
    tbot = make_bot(SIM_WAYS="taker", LIVE_TAKER_WAIT_SECONDS="3600")
    await tbot.live_prepare(); tfake = tbot.live.api
    await tbot.live_prefetch(CLOSE - 30_000)
    await step(tbot, CLOSE - 30_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], CLOSE - 30_000))
    assert tbot.sim_trades()[f"{HSI_SLUG}|up|吃"]["live"]["state"] == "open"
    await step(tbot, CLOSE + 5_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], CLOSE + 5_000))
    tt = tbot.sim_trades()[f"{HSI_SLUG}|up|吃#1"]
    assert tt["live"]["cancel_why"] == "已到结束时间，等结果出来：不开新单，挂单撤掉" and ("remove", [tt["live"]["order_id"]]) in tfake.calls, tt["live"]
    # the hard switch: a raw LIVE=on is not even saved
    hbot = make_bot(LIVE_TRADING_ALLOWED="off", LIVE="pause")
    result = hbot.control_set({"LIVE": "on"})
    assert not result["ok"] and "LIVE_TRADING_ALLOWED" in result["message"] and hbot.store.get("control:env") is None
    # a market whose details cannot be read is asked for again a minute later, not every step
    rbot = make_bot(SIM_WAYS="taker")
    await rbot.live_prepare(); rfake = rbot.live.api
    rfake.markets.pop("101")
    world["markets"] = [hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW)]
    await rbot.live_prefetch(NOW)
    await rbot.live_prefetch(NOW)
    assert "市场 101 详情读取失败" in rbot.live_errors[-1] and len([e for e in rbot.live_errors if "101" in e]) == 1 and "101" in rbot.live_market_retry, rbot.live_errors
    rbot.live_market_retry["101"] = 0.0
    rfake.markets["101"] = market_json("101")
    await rbot.live_prefetch(NOW)
    assert "101" in rbot.live.markets and "101" not in rbot.live_market_retry
    assert L.answer_shape({"success": True, "data": {"token": "secret", "x": 1}}) == "data,success,data{token,x}" and "secret" not in L.answer_shape({"data": {"token": "secret"}})
    print("REVIEW_FIXES_OK")


async def audit_fixes():
    """The 10-08 audit: an order Predict neither shows nor confirms removed is "unknown" — reserved, blocking its side,
    re-asked every step — until Predict's final word, its expiry, or an operator's release; a finished record whose
    order reappears is tracked again; the daily loss is recomputed right before a send."""
    hide = lambda api: (setattr(api, "orders", _none_list), setattr(api, "order", _none_order), setattr(api, "remove_orders", _noop_remove(api)))
    # a maker order that vanishes: unknown after a minute; its deadline; released by hand; back on the list → tracked again
    ubot = make_bot(SIM_WAYS="maker")
    await ubot.live_prepare(); ufake = ubot.live.api
    await ubot.live_prefetch(NOW)
    await step(ubot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    utid = f"{HSI_SLUG}|up|挂"
    oid = ubot.sim_trades()[utid]["live"]["order_id"]
    saved = (ufake.orders, ufake.order, ufake.remove_orders)
    hide(ufake)
    for at in (NOW + 10_000, NOW + 20_000, NOW + 30_000, NOW + 65_000):
        await step(ubot, at, hsi(0.70, [("0.55", "300")], [("0.58", "400")], at))
    ut = ubot.sim_trades()[utid]
    assert ut["live"]["state"] == "unknown" and ut["status"] == "resting" and abs(ubot.live_reserved(ut) - 55.0) < 1e-9, ut["live"]
    assert ubot.live_order_deadline(ut) == CLOSE + 2 * 3600_000 and ubot.live_status(NOW + 65_000)["unknown"][0]["deadline"]
    assert "对账中" in ubot.live_room(ubot.sim_trades(), hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 65_000), "up", 0.55, 100)
    result = await ubot.live_control("release", {"id": oid})
    ut = ubot.sim_trades()[utid]
    assert result["ok"] and ut["live"]["state"] == "done" and ut["status"] == "cancelled" and ubot.live_reserved(ut) == 0 and "管理员确认" in ut["note"], (result, ut)
    ufake.orders, ufake.order, ufake.remove_orders = saved  # it is on the list after all: the next idle re-read (a minute on) finds it
    await step(ubot, NOW + 130_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 130_000))
    ut = ubot.sim_trades()[utid]
    assert ut["live"]["state"] == "open" and ut["status"] == "resting" and abs(ubot.live_reserved(ut) - 55.0) < 1e-9 and any("重新出现" in e["what"] for e in ut["live"]["events"]), ut["live"]
    # a cancel Predict neither confirms nor can the order be found: unknown; found again later → the cancel goes out again
    cbot = make_bot(SIM_WAYS="maker")
    await cbot.live_prepare(); cfake = cbot.live.api
    await cbot.live_prefetch(NOW)
    await step(cbot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    ctid = f"{HSI_SLUG}|up|挂"
    coid = cbot.sim_trades()[ctid]["live"]["order_id"]
    saved = (cfake.orders, cfake.order, cfake.remove_orders)
    hide(cfake)
    cbot.control_set({"SIM_MARKETS": "touch"})
    await step(cbot, NOW + 5_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 5_000))
    ct = cbot.sim_trades()[f"{ctid}#1"]
    assert ct["live"]["state"] == "unknown" and ct["status"] == "cancelled" and "未获 Predict 确认" in ct["live"]["unknown_why"] and cbot.live_reserved(ct) > 0, ct["live"]
    cfake.orders, cfake.order, cfake.remove_orders = saved
    await step(cbot, NOW + 10_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 10_000))  # found: the cancel is asked again
    await step(cbot, NOW + 15_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 15_000))
    ct = cbot.sim_trades()[f"{ctid}#1"]
    assert ct["live"]["state"] == "done" and ct["live"]["final"] == "cancelled" and cbot.live_reserved(ct) == 0 and ("remove", [coid]) in cfake.calls[-3:], ct["live"]
    # a taker's unknown order ends by itself once its LIMIT has expired (plus a margin): nothing can fill it then
    ebot = make_bot(SIM_WAYS="taker")
    await ebot.live_prepare(); efake = ebot.live.api
    await ebot.live_prefetch(NOW)
    await step(ebot, NOW, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW))
    etid = f"{HSI_SLUG}|up|吃"
    hide(efake)
    for at in (NOW + 10_000, NOW + 20_000, NOW + 30_000, NOW + 65_000):
        await step(ebot, at, hsi(0.70, [("0.55", "300")], [("0.58", "400")], at))
    assert ebot.sim_trades()[etid]["live"]["state"] == "unknown" and ebot.live_order_deadline(ebot.sim_trades()[etid]) == NOW + 300_000
    await step(ebot, NOW + 300_000 + 600_000 - 5_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 895_000))
    assert ebot.sim_trades()[etid]["live"]["state"] == "unknown"  # not yet: the margin
    await step(ebot, NOW + 300_000 + 600_000 + 5_000, hsi(0.70, [("0.55", "300")], [("0.58", "400")], NOW + 905_000))
    et = ebot.sim_trades()[f"{etid}#1"]
    assert et["live"]["state"] == "done" and any("有效期已过" in e["what"] for e in et["live"]["events"]) and ebot.live_reserved(et) == 0, et["live"]
    # the daily loss is recomputed at the moment of sending, not read from a flag saved a step ago
    pbot = make_bot(SIM_WAYS="taker")
    await pbot.live_prepare()
    pbot.live_daily_pnl = lambda trades, now_ms: -60.0
    pending = {"live": {"want": {"shares": 100.0, "price": 0.5}, "events": [{"at": NOW}], "state": "pending"}, "order": 100.0, "price": 0.5, "opened": NOW, "maker": False}
    assert "已达 LIVE_MAX_DAILY_LOSS_USD" in pbot.live_send_block(pending, {}, NOW)
    print("AUDIT_FIXES_OK")


async def _none_list(status="OPEN", **params): return []
async def _none_order(oid, hash_=""): return None
def _noop_remove(api):
    async def noop(ids):
        api.calls.append(("remove", list(ids))); return {"removed": [], "noop": list(ids)}
    return noop


asyncio.run(run())
asyncio.run(review_fixes())
asyncio.run(audit_fixes())
