"""Kept-alive HTTP (1.62.0): one connection per host reused across requests; a pooled connection the server had closed
is retried once on a fresh one for a GET, for a POST only before anything was sent; HTTP errors mapped as before
(status, the service's words, Retry-After, hints); redirects followed; gzip inflated; the pool bounded; a proxy or
plain http takes the urllib way. All with a fake connection class: nothing touches the network."""
import gzip, json, sys, time, urllib.request
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m


class Answer:
    def __init__(self, status=200, body=b"", headers=None, will_close=False):
        self.status, self.body, self.headers, self.will_close, self.pos = status, body, dict(headers or {}), will_close, 0

    def read(self, n=-1):
        chunk = self.body[self.pos:self.pos + n] if n is not None and n >= 0 else self.body[self.pos:]
        self.pos += len(chunk)
        return chunk

    def getheader(self, name, default=None):
        return self.headers.get(name, default)


SCRIPT = {}  # host -> the answers in order: an Answer, "send-fail" (the request could not be written), "recv-fail" (no answer came)
LOG = []     # (connection id, method, path, body)


class FakeConn:
    made = 0

    def __init__(self, host, port, timeout=None, context=None):
        FakeConn.made += 1
        self.id, self.host, self.port, self.timeout, self.sock, self.closed, self.pending = FakeConn.made, host, port, timeout, None, False, None

    def request(self, method, path, body=None, headers=None):
        assert not self.closed and headers["Accept-Encoding"] == "gzip" and headers["User-Agent"].startswith("CloseAlert/")
        LOG.append((self.id, method, path, body))
        step = SCRIPT[self.host].pop(0)
        if step == "send-fail":
            raise ConnectionResetError("Connection reset by peer")
        self.sock = self.sock or _Sock()
        self.pending = step

    def getresponse(self):
        if self.pending == "recv-fail":
            raise m.http.client.RemoteDisconnected("Remote end closed connection without response")
        return self.pending

    def close(self):
        self.closed, self.sock = True, None


class _Sock:
    def settimeout(self, t): self.timeout = t


def run():
    m.HTTPS_CONNECTION = FakeConn
    real_proxies = urllib.request.getproxies
    urllib.request.getproxies = lambda: {}  # the kept-alive path, whatever this machine's proxy settings
    m._POOL.clear(); m.HTTP_STATS.update(new=0, reused=0, retried=0)
    try:
        host = "api.example.test"
        ok = lambda body=b'{"ok": true}', **kw: Answer(200, body, **kw)
        # 1. two GETs, one connection
        SCRIPT[host] = [ok(b"a"), ok(b"b")]
        assert m._http_get(f"https://{host}/x?q=1") == b"a" and m._http_get(f"https://{host}/y") == b"b"
        assert [(c, meth, p) for c, meth, p, _ in LOG] == [(1, "GET", "/x?q=1"), (1, "GET", "/y")] and m.HTTP_STATS == {"new": 1, "reused": 1, "retried": 0}
        # 2. the pooled connection was closed by the server: the request could not be written -> once more, on a fresh one
        LOG.clear(); SCRIPT[host] = ["send-fail", ok(b"c")]
        assert m._http_get(f"https://{host}/z") == b"c" and [c for c, *_ in LOG] == [1, 2] and m.HTTP_STATS["retried"] == 1
        # 3. ...or no answer came to a GET: again on a fresh one
        LOG.clear(); SCRIPT[host] = ["recv-fail", ok(b"d")]
        assert m._http_get(f"https://{host}/z") == b"d" and [c for c, *_ in LOG] == [2, 3] and m.HTTP_STATS["retried"] == 2
        # 4. a POST on a fresh pooled connection with no answer: never sent twice (the caller treats it as "result unknown")
        LOG.clear(); SCRIPT[host] = ["recv-fail"]
        try:
            m._http_get(f"https://{host}/orders", {"a": 1}); raise AssertionError("no error")
        except m.RemoteError as error:
            assert str(error).startswith("网络错误 (RemoteDisconnected") and len(LOG) == 1 and LOG[0][1] == "POST" and json.loads(LOG[0][3]) == {"a": 1}, error
        # 5. a POST reuses only a connection used within KEEPALIVE_POST_IDLE_SECONDS; a GET within KEEPALIVE_IDLE_SECONDS
        LOG.clear(); SCRIPT[host] = [ok(b"e"), ok(b"f"), ok(b"g")]
        assert m._http_get(f"https://{host}/a") == b"e"; fresh = LOG[-1][0]
        m._POOL[(host, 443)][-1] = (m._POOL[(host, 443)][-1][0], time.monotonic() - 10)  # idle for 10 s
        assert m._http_get(f"https://{host}/orders", {"b": 2}) == b"f" and LOG[-1][0] != fresh, "a stale-ish connection is not trusted with a body"
        m._POOL[(host, 443)][-1] = (m._POOL[(host, 443)][-1][0], time.monotonic() - 10)
        assert m._http_get(f"https://{host}/a") == b"g" and LOG[-1][0] == LOG[-2][0], "a GET reuses it"
        m._POOL[(host, 443)][-1] = (m._POOL[(host, 443)][-1][0], time.monotonic() - 60)
        SCRIPT[host] = [ok(b"h")]
        assert m._http_get(f"https://{host}/a") == b"h" and LOG[-1][0] != LOG[-2][0], "a connection idle a minute is dropped"
        # 6. HTTP errors: the status, the service's words, Retry-After (and Binance's 418/429 floor), Predict's 403 hint
        SCRIPT[host] = [Answer(429, b'{"message": "slow down"}', {"Retry-After": "7"})]
        try:
            m._http_get(f"https://{host}/a"); raise AssertionError("no error")
        except m.RemoteError as error:
            assert str(error) == "HTTP 429: 接口限流，等待后重试" and error.retry_after == 7, (str(error), error.retry_after)  # the hint for a generic host, the service's words for Predict
        SCRIPT["api.predict.fun"] = [Answer(403, b"{}")]
        try:
            m._http_get("https://api.predict.fun/v1/orders", {"x": 1}); raise AssertionError("no error")
        except m.RemoteError as error:
            assert str(error).startswith("HTTP 403: ") and "受限地区" in str(error), error
        SCRIPT["fapi.binance.com"] = [Answer(418, b"")]
        try:
            m._http_get("https://fapi.binance.com/fapi/v1/time"); raise AssertionError("no error")
        except m.RemoteError as error:
            assert str(error).startswith("HTTP 418") and error.retry_after == 120, (str(error), error.retry_after)
        assert not m._POOL.get(("api.predict.fun", 443)) and not m._POOL.get(("fapi.binance.com", 443)), "an error answer's connection is not pooled"
        # 7. a redirect is followed (a GET), the body of the final answer returned; gzip is inflated; Connection: close is honoured
        LOG.clear(); SCRIPT[host] = [Answer(302, b"", {"Location": f"https://{host}/moved"}), ok(gzip.compress(b"zipped"), headers={"Content-Encoding": "gzip"}, will_close=True), ok(b"i")]
        assert m._http_get(f"https://{host}/old") == b"zipped" and [p for _, _, p, _ in LOG] == ["/old", "/moved"]
        assert m._http_get(f"https://{host}/next") == b"i" and LOG[-1][0] != LOG[-2][0], "the closed connection was not reused"
        # 8. the pool is bounded per host
        for i in range(m.KEEPALIVE_PER_HOST + 3):
            m._pool_put(host, 443, FakeConn(host, 443))
        assert len(m._POOL[(host, 443)]) == m.KEEPALIVE_PER_HOST
        # 9. a proxy, or plain http: the urllib way (offline here: the guard answers)
        urllib.request.getproxies = lambda: {"https": "http://proxy:3128"}
        n = FakeConn.made
        try:
            m._http_get(f"https://{host}/a"); raise AssertionError("no error")
        except m.RemoteError as error:
            assert str(error) == "网络错误 (OSError)" and FakeConn.made == n, error  # urllib's guard, as before; no pooled connection touched
        urllib.request.getproxies = lambda: {}
        try:
            m._http_get("http://plain.example.test/a"); raise AssertionError("no error")
        except m.RemoteError as error:
            assert str(error) == "网络错误 (OSError)" and FakeConn.made == n, error
        # 10. the real connection class is guarded by offline.py too: connecting raises, mapped to 网络错误
        m.HTTPS_CONNECTION = m.http.client.HTTPSConnection
        try:
            m._http_get("https://nowhere.example.test/a"); raise AssertionError("no error")
        except m.RemoteError as error:
            assert str(error).startswith("网络错误 (OSError: network disabled in tests"), error
    finally:
        m.HTTPS_CONNECTION = m.http.client.HTTPSConnection
        urllib.request.getproxies = real_proxies
        m._POOL.clear()
    print("HTTP_OK")


run()
