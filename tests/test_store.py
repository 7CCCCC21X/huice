"""1.88.0: Store — the records written by a writer thread, read back at once through the pending overlay."""
import json, os, sqlite3, sys, tempfile, time
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
import offline  # noqa: F401  (blocks real HTTP)
import main as m

path = os.path.join(tempfile.mkdtemp(), "bot.sqlite3")
s = m.Store(path)
assert s.writer is not None and s.writer.is_alive() and s.line() == "后台写入，无积压" and s.flush()
s.put("sim:a", {"x": 1})
assert s.get("sim:a") == {"x": 1} and s.touched["sim"] == 1, "read back at once, through the overlay"
s.put_many([("sim:b", {"y": 2}), ("pred:1", [1, 2])])
assert s.keys("sim:") == ["sim:a", "sim:b"] and s.items("sim:") == [("sim:a", {"x": 1}), ("sim:b", {"y": 2})] and s.count("sim:") == 2
assert s.get("sim:a") is not s.get("sim:a"), "every read is its own copy"
assert s.flush() and not s.pending and s.written == s.version == 2 and s.line() == "后台写入，无积压"
other = sqlite3.connect(path)
assert dict(other.execute("SELECT k, v FROM records")) == {"sim:a": '{"x": 1}', "sim:b": '{"y": 2}', "pred:1": "[1, 2]"}
# a later put of the same key wins over an earlier one still queued; a get in between sees the latest
s.put("sim:a", {"x": 2}); s.put("sim:a", {"x": 3})
assert s.get("sim:a") == {"x": 3} and s.flush() and json.loads(other.execute("SELECT v FROM records WHERE k='sim:a'").fetchone()[0]) == {"x": 3}
# the structural writes wait for the queue: a delete after a queued put removes the record, a replace moves it, a prefix delete clears it
s.put("sim:c", {"z": 1}); s.delete_keys(["sim:c"])
assert s.get("sim:c") is None and other.execute("SELECT count(*) FROM records WHERE k='sim:c'").fetchone()[0] == 0
s.put("sim:d", {"z": 2}); s.replace(["sim:d"], [("sim:d#1", {"z": 2})])
assert s.get("sim:d") is None and s.get("sim:d#1") == {"z": 2} and s.keys("sim:d") == ["sim:d#1"] and s.touched["sim"] >= 7
s.put("alert:1:x", 1); s.put("alert:2:y", 2); s.delete_prefix("alert:1:")
assert s.keys("alert:") == ["alert:2:y"] and s.items("alert:") == [("alert:2:y", 2)]
# the writer failing: the records stay readable, flush says so, the writer goes on once the disk is back
real = s.writer_conn


class Broken:
    def __init__(self): self.fails = 2
    def __enter__(self): return real.__enter__()
    def __exit__(self, *a): return real.__exit__(*a)
    def executemany(self, sql, rows):
        if self.fails:
            self.fails -= 1
            raise RuntimeError("disk full")
        return real.executemany(sql, rows)
    def __getattr__(self, name): return getattr(real, name)


s.writer_conn = Broken()
s.put("sim:e", {"e": 1})
assert s.get("sim:e") == {"e": 1} and not s.flush(0.3) and s.write_error == "RuntimeError: disk full" and "最近错误：RuntimeError: disk full" in s.line() \
    and "排队 1 批" in s.line(), s.line()
assert s.flush(5.0) and s.write_error == "" and json.loads(other.execute("SELECT v FROM records WHERE k='sim:e'").fetchone()[0]) == {"e": 1}
s.writer_conn = real
# a big batch is one transaction; a record under a queued write is in the keys and the items
s.put_many((f"sim:big{i}", {"i": i}) for i in range(2000))
assert s.count("sim:big") == 2000 and len(s.items("sim:big1")) == 1111 and s.flush() \
    and other.execute("SELECT count(*) FROM records WHERE k LIKE 'sim:big%'").fetchone()[0] == 2000
# the close waits for the queue; a store opened on the file after it reads everything
s.put("sim:last", 1)
s.close()
assert not s.writer.is_alive() and sqlite3.connect(path).execute("SELECT v FROM records WHERE k='sim:last'").fetchone()[0] == "1"
again = m.Store(path)
assert again.get("sim:last") == 1 and again.count("sim:big") == 2000
again.close()
# the memory store writes synchronously (the tests' stores)
mem = m.Store(":memory:")
assert mem.writer is None and mem.line() == "同步写入（内存库）"
mem.put("a", 1)
assert mem.get("a") == 1 and mem.flush() and not mem.pending and mem.keys("") == ["a"]
mem.close()
print("STORE_OK")
