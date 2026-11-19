#!/usr/bin/env python3
"""
Reproduce cross-instance SQLite write-lock contention and inspect it through
the lock-diagnostics API.

Prereq: the two demo processes are running and share one database:

    bash scripts/local_up.sh                 # local, no Docker
    # or: docker compose up -d --build       # containers

Then:

    python3 scripts/reproduce_contention.py

What it does:
  1. app1 takes the main-DB write lock and holds it (POST /demo/hold-lock).
  2. While it holds, several writes are fired at BOTH instances; they queue
     on the single-writer lock.
  3. While the lock is STILL held, the script queries GET /locks/current on
     the peer instance - proving diagnostics stay readable while the main
     database is write-locked - and prints the live holder + waiter queue.
  4. After release it prints every participant's diagnostic record
     (GET /locks/events/{diag_id}): operation, instance, wait/hold ms,
     completion reason.

Then open http://localhost:8001/docs#/lock-diagnostics (or :8002) and poke
GET /locks/current and GET /locks/events/{diag_id} yourself in Swagger UI.

Timeout variant: start the apps with a short write-lock budget so waiters
give up quickly -

    bash scripts/local_down.sh
    BUSY_TIMEOUT_MS=3000 bash scripts/local_up.sh
    python3 scripts/reproduce_contention.py --timeout-demo

One waiter then fails with HTTP 503 and its record shows state='timeout'
with the reason (busy_timeout after ~3000 ms).
"""

import argparse
import json
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid


def req(method, port, path, body=None, timeout=90):
    url = f"http://127.0.0.1:{port}{path}"
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, (json.loads(raw) if raw else {})
        except json.JSONDecodeError:
            return e.code, {"raw": raw[:200].decode(errors="replace")}


def fmt_event(ev):
    wait = f"{ev['wait_ms']:.0f} ms" if ev.get("wait_ms") is not None else "-"
    hold = f"{ev['hold_ms']:.0f} ms" if ev.get("hold_ms") is not None else "-"
    return (f"  diag #{ev['id']:<3} {ev['instance']:<5} {ev['op']:<18} "
            f"state={ev['state']:<10} wait={wait:<10} hold={hold:<10} "
            f"outcome={ev.get('outcome')}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--app1", type=int, default=8001)
    ap.add_argument("--app2", type=int, default=8002)
    ap.add_argument("--hold", type=float, default=6.0,
                    help="seconds the holder keeps the write lock")
    ap.add_argument("--timeout-demo", action="store_true",
                    help="also reproduce a busy-timeout (needs apps started "
                         "with BUSY_TIMEOUT_MS<=10000)")
    args = ap.parse_args()

    for port in (args.app1, args.app2):
        st, js = req("GET", port, "/health")
        if st != 200:
            print(f"app on :{port} not healthy ({st}); start the apps first "
                  f"(bash scripts/local_up.sh or docker compose up -d --build)")
            sys.exit(1)
    print(f"apps healthy: app1=:{args.app1} app2=:{args.app2}\n")

    uniq = uuid.uuid4().hex[:8]
    results = {}

    def fire(key, method, port, path, body=None):
        try:
            results[key] = req(method, port, path, body)
        except Exception as exc:
            results[key] = (-1, {"error": repr(exc)})

    # 1) app1 grabs the write lock and holds it.
    print(f"1) app1 holds the main-DB write lock for {args.hold:.0f}s "
          f"(POST /demo/hold-lock?seconds={args.hold})")
    th_hold = threading.Thread(
        target=fire, args=("hold", "POST", args.app1, f"/demo/hold-lock?seconds={args.hold}"))
    th_hold.start()
    time.sleep(0.6)  # let the hold land

    # 2) Fire a mix of writes at BOTH instances; they must queue.
    specs = [
        ("svc-a", "POST", args.app2, "/services", {"name": f"contention-a-{uniq}"}),
        ("svc-b", "POST", args.app1, "/services", {"name": f"contention-b-{uniq}"}),
        ("step", "POST", args.app2, "/validation/step?batch_size=5", None),
        ("svc-c", "POST", args.app2, "/services", {"name": f"contention-c-{uniq}"}),
    ]
    print(f"2) firing {len(specs)} writes at both instances -> they queue on the lock")
    threads = []
    for key, method, port, path, body in specs:
        t = threading.Thread(target=fire, args=(key, method, port, path, body))
        t.start()
        threads.append(t)
        time.sleep(0.15)

    # 3) While the lock is still held, read diagnostics from the PEER instance.
    time.sleep(0.6)
    print("3) lock still held -> GET /locks/current on the PEER instance (app2):")
    st, cur = req("GET", args.app2, "/locks/current")
    if st != 200:
        print(f"   FAILED: diagnostics not readable while main DB is locked ({st})")
        sys.exit(1)
    for h in cur["holders"]:
        print(f"   HOLDER  diag #{h['id']} {h['instance']} {h['op']} "
              f"holding_for={h.get('holding_for_ms')} ms live={h.get('instance_live')}")
    for w in cur["waiters"]:
        print(f"   waiter  diag #{w['id']} {w['instance']} {w['op']} "
              f"waiting_for={w.get('waiting_for_ms')} ms")
    if not cur["holders"]:
        print("   (no holder visible - unexpected during the hold)")

    for t in threads:
        t.join()
    th_hold.join()

    print("\n4) all requests finished; their diagnostic records "
          "(GET /locks/events/{diag_id}, read via app2):")
    st, hold_js = results["hold"]
    print(f"   holder HTTP {st}, diag_id={hold_js.get('diag_id')}")
    ids = [("hold", hold_js.get("diag_id"))]
    for key, *_ in specs:
        st, js = results[key]
        ids.append((key, js.get("diag_id")))
        print(f"   waiter {key}: HTTP {st}, diag_id={js.get('diag_id')}")
    print()
    for key, diag_id in ids:
        if diag_id is None:
            continue
        st, ev = req("GET", args.app2, f"/locks/events/{diag_id}")
        if st == 200:
            print(fmt_event(ev))

    # 5) Optional: reproduce a busy timeout.
    if args.timeout_demo:
        print("\n5) timeout demo:")
        st, health = req("GET", args.app2, "/health")
        busy_ms = health.get("busy_timeout_ms", 30000)
        if busy_ms > 10000:
            print(f"   apps run busy_timeout={busy_ms} ms - restart them with "
                  f"'BUSY_TIMEOUT_MS=3000 bash scripts/local_up.sh' to see a quick timeout")
        else:
            hold_s = busy_ms / 1000 + 3
            print(f"   busy_timeout={busy_ms} ms; app1 holds {hold_s:.0f}s, one waiter will give up")
            th = threading.Thread(
                target=fire, args=("slowhold", "POST", args.app1,
                                   f"/demo/hold-lock?seconds={hold_s}"))
            th.start()
            time.sleep(0.6)
            t0 = time.time()
            st, js = req("POST", args.app2, "/services", {"name": f"timeout-{uniq}"})
            waited = time.time() - t0
            print(f"   waiter -> HTTP {st} after {waited:.1f}s, diag_id={js.get('diag_id')}")
            if js.get("diag_id") is not None:
                st2, ev = req("GET", args.app2, f"/locks/events/{js['diag_id']}")
                if st2 == 200:
                    print(fmt_event(ev))
            th.join()

    print("\nDone. Browse the diagnostics in Swagger UI:")
    print(f"  http://localhost:{args.app1}/docs#/lock-diagnostics")
    print(f"  http://localhost:{args.app2}/docs#/lock-diagnostics")
    print("  GET /locks/current           - live holder + waiter queue")
    print("  GET /locks/events            - recent events (filter by state/instance/op)")
    print("  GET /locks/events/{diag_id}  - one event by diagnostic number")


if __name__ == "__main__":
    main()
