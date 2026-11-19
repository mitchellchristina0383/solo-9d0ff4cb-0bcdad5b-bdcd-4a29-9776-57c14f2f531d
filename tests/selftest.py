#!/usr/bin/env python3
"""
End-to-end self test for the online FK enablement demo.

Runs against two service processes (default ports 8001/8002) sharing one
SQLite database. Covers:

  Phase A  legacy writes -> start -> cross-process enforcement -> batched scan
           with interleaved corrections -> finalize fail -> fix -> finalize ok
           -> post-enable enforcement
  Phase B  restart recovery: restart mid-validation, restart after failed
           finalize, restart after enabled; invariant: never "enabled" while
           orphans exist
  Phase C  concurrency storm: parallel scanner / fixers / deleters / bad
           writers / invariant monitor across both processes
  Phase D  lock-contention diagnostics: diag_id on writes, live holder/waiter
           view while the main DB is locked, failure honesty, hold-order vs.
           real lock ownership, busy timeout (--timeout-test), SIGKILL ->
           interrupted records (needs --restart-hook)

Usage:
  python3 tests/selftest.py [--app1 8001] [--app2 8002] \
      [--restart-hook "shell command to restart the app1 process"] \
      [--timeout-test]

For Docker Compose:  --restart-hook "docker compose restart app1"
--timeout-test needs the apps started with a short busy timeout, e.g.
  BUSY_TIMEOUT_MS=3000 bash scripts/local_up.sh
"""

import argparse
import json
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor

CHECKS = 0
FAILURES = []


def check(cond, msg):
    global CHECKS
    CHECKS += 1
    if cond:
        print(f"    ok   {msg}")
    else:
        FAILURES.append(msg)
        print(f"    FAIL {msg}")


def req(method, port, path, body=None):
    url = f"http://127.0.0.1:{port}{path}"
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, (json.loads(raw) if raw else {})
        except json.JSONDecodeError:
            return e.code, {"raw": raw[:200].decode(errors="replace")}


def expect(method, port, path, body, want, msg):
    try:
        st, js = req(method, port, path, body)
    except Exception as exc:  # connection refused etc.
        check(False, f"{msg} [{method} :{port}{path} raised {exc!r}]")
        return {}
    check(st == want, f"{msg} [{method} :{port}{path} -> {st}, want {want}]"
                      + ("" if st == want else f" body={js}"))
    return js


def wait_ready(port, timeout=60):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            st, _ = req("GET", port, "/health")
            if st == 200:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def drain_scan(port):
    """Run /validation/step until nothing remains; return last response."""
    last = {}
    for _ in range(1000):
        st, js = req("POST", port, "/validation/step?batch_size=50")
        if st != 200:
            return None
        last = js
        if js["remaining"] == 0:
            return last
    return last


def fix_all_orphans(port, service_id):
    st, js = req("GET", port, "/orphans")
    assert st == 200, js
    for o in js["orphans"]:
        st2, js2 = req("PATCH", port, f"/deployments/{o['id']}", {"service_id": service_id})
        assert st2 == 200, (st2, js2)
    return len(js["orphans"])


# --------------------------------------------------------------------------- #
# Phase A: basic lifecycle
# --------------------------------------------------------------------------- #

def phase_a(app1, app2):
    print("== Phase A: legacy -> validating -> enabled lifecycle ==")

    js = expect("POST", app1, "/demo/reset", None, 200, "A0 reset")
    st, js = req("GET", app2, "/validation/status")
    check(js.get("state") == "off", "A0 state is off on both processes")
    check(js.get("enforcement_triggers_present") == [], "A0 no triggers while off")

    # Seed services via both processes.
    s_web = expect("POST", app1, "/services", {"name": "web"}, 201, "A1 create svc web (app1)")["id"]
    s_api = expect("POST", app1, "/services", {"name": "api"}, 201, "A1 create svc api (app1)")["id"]
    expect("POST", app2, "/services", {"name": "worker"}, 201, "A1 create svc worker (app2)")

    # Legacy mode: orphans can be created freely.
    d1 = expect("POST", app1, "/deployments", {"service_id": s_web, "note": "d1"}, 201, "A2 valid dep (app1)")["id"]
    d2 = expect("POST", app2, "/deployments", {"service_id": 9999, "note": "d2-orphan"}, 201, "A2 orphan dep allowed while off (app2)")["id"]
    d3 = expect("POST", app1, "/deployments", {"service_id": s_api, "note": "d3"}, 201, "A2 valid dep (app1)")["id"]
    d4 = expect("POST", app2, "/deployments", {"service_id": 9998, "note": "d4-orphan"}, 201, "A2 orphan dep allowed while off (app2)")["id"]
    expect("POST", app1, "/deployments", {"service_id": s_web, "note": "d5"}, 201, "A2 valid dep (app1)")

    st, js = req("GET", app2, "/orphans")
    check(st == 200 and {o["id"] for o in js["orphans"]} == {d2, d4}, "A3 /orphans lists the 2 seeded orphans")

    # Legacy mode: deleting a referenced service creates an orphan (the problem).
    s_tmp = expect("POST", app1, "/services", {"name": "tmp"}, 201, "A4 create svc tmp")["id"]
    d6 = expect("POST", app2, "/deployments", {"service_id": s_tmp, "note": "d6"}, 201, "A4 dep -> tmp")["id"]
    expect("DELETE", app2, f"/services/{s_tmp}", None, 200, "A4 legacy delete of referenced svc allowed (off)")
    st, js = req("GET", app1, "/orphans")
    check({o["id"] for o in js["orphans"]} == {d2, d4, d6}, "A4 delete created a new orphan (legacy defect)")

    # Start validation: triggers installed atomically with the state flip.
    js = expect("POST", app1, "/validation/start", None, 200, "A5 start validation (app1)")
    st, js = req("GET", app2, "/validation/status")
    check(js.get("state") == "validating", "A5 state=validating visible from app2")
    check(len(js.get("enforcement_triggers_present", [])) == 4, "A5 all 4 triggers present")
    expect("POST", app2, "/validation/start", None, 409, "A5 second start rejected")

    # From validation start: no new orphans, from EITHER process.
    expect("POST", app2, "/deployments", {"service_id": 7777}, 409, "A6 insert orphan rejected (app2)")
    expect("PATCH", app1, f"/deployments/{d1}", {"service_id": 7777}, 409, "A6 re-point to missing svc rejected (app1)")
    expect("DELETE", app2, f"/services/{s_web}", None, 409, "A6 delete referenced svc rejected (app2)")
    s_solo = expect("POST", app1, "/services", {"name": "solo"}, 201, "A6 create unreferenced svc")["id"]
    expect("DELETE", app2, f"/services/{s_solo}", None, 200, "A6 delete unreferenced svc still allowed")
    expect("POST", app2, "/deployments", {"service_id": s_api, "note": "valid"}, 201, "A6 valid insert still accepted")

    # Old orphans can still be corrected.
    expect("PATCH", app2, f"/deployments/{d2}", {"service_id": s_web}, 200, "A7 fix old orphan (app2)")
    st, js = req("GET", app1, "/orphans")
    check({o["id"] for o in js["orphans"]} == {d4, d6}, "A7 orphan set shrank")

    # Batched scan across both processes.
    scanned_ids = []
    total_scanned = 0
    for port in (app1, app2, app1, app2, app1, app2):
        st, js = req("POST", port, "/validation/step?batch_size=2")
        check(st == 200, f"A8 step on :{port} ok")
        if st != 200:
            break
        total_scanned = js["scanned_total"]
        if js["remaining"] == 0:
            break
    st, js = req("GET", app1, "/validation/status")
    check(js["scan"]["scanned"] >= 6, "A8 all deployments scanned")
    st, js = req("GET", app2, "/validation/findings")
    found_ids = {f["deployment_id"] for f in js["findings"]}
    check(found_ids == {d4, d6}, f"A8 findings == remaining orphans at scan time (got {found_ids})")

    # Rows already scanned cannot be turned invalid afterwards.
    expect("PATCH", app1, f"/deployments/{d3}", {"service_id": 8888}, 409, "A9 re-break scanned row rejected")
    expect("DELETE", app2, f"/services/{s_api}", None, 409, "A9 delete svc of scanned row rejected")

    # Finalize gate: orphans remain -> 409, state stays validating.
    js = expect("POST", app1, "/validation/finalize", None, 409, "A10 finalize with orphans rejected")
    st, js = req("GET", app2, "/validation/status")
    check(js.get("state") == "validating", "A10 state still validating after failed finalize")

    # Correct the rest, then finalize succeeds.
    n = fix_all_orphans(app2, s_api)
    check(n == 2, "A11 fixed the 2 remaining orphans")
    st, js = req("GET", app1, "/orphans")
    check(js.get("count") == 0, "A11 no orphans left")
    js = expect("POST", app2, "/validation/finalize", None, 200, "A12 finalize succeeds (app2)")
    check(js.get("state") == "enabled", "A12 state=enabled")
    st, js = req("GET", app1, "/validation/status")
    check(js.get("state") == "enabled" and js.get("live_orphans") == 0, "A12 enabled + zero orphans (app1 view)")

    # Constraint keeps holding after enablement, on both processes.
    expect("POST", app1, "/deployments", {"service_id": 4242}, 409, "A13 bad insert rejected after enable (app1)")
    expect("PATCH", app2, f"/deployments/{d1}", {"service_id": 4242}, 409, "A13 bad re-point rejected after enable (app2)")
    expect("DELETE", app1, f"/services/{s_web}", None, 409, "A13 delete referenced svc rejected after enable")
    expect("POST", app2, "/deployments", {"service_id": s_web, "note": "post-enable"}, 201, "A13 valid insert accepted after enable")
    expect("POST", app1, "/validation/start", None, 409, "A13 re-start rejected once enabled")


# --------------------------------------------------------------------------- #
# Phase B: restart / crash recovery
# --------------------------------------------------------------------------- #

def phase_b(app1, app2, restart_hook):
    print("== Phase B: restart recovery ==")
    if not restart_hook:
        print("    (skipped: no --restart-hook given)")
        return

    expect("POST", app1, "/demo/reset", None, 200, "B0 reset")
    s1 = expect("POST", app1, "/services", {"name": "s1"}, 201, "B1 svc s1")["id"]
    s2 = expect("POST", app2, "/services", {"name": "s2"}, 201, "B1 svc s2")["id"]
    e1 = expect("POST", app1, "/deployments", {"service_id": s1}, 201, "B1 dep e1")["id"]
    e2 = expect("POST", app2, "/deployments", {"service_id": 5555}, 201, "B1 orphan e2")["id"]
    e3 = expect("POST", app1, "/deployments", {"service_id": 5556}, 201, "B1 orphan e3")["id"]
    expect("POST", app2, "/deployments", {"service_id": s2}, 201, "B1 dep e4")

    expect("POST", app2, "/validation/start", None, 200, "B2 start validation (app2)")
    st, js = req("POST", app1, "/validation/step?batch_size=2")
    check(st == 200 and js["cursor"] == e2 and js["scanned_total"] == 2, "B2 first batch scanned (cursor at e2)")

    # Crash app1 mid-validation.
    subprocess.run(restart_hook, shell=True, check=True)
    check(wait_ready(app1), "B3 app1 back after restart")
    st, js = req("GET", app1, "/validation/status")
    check(js.get("state") == "validating", "B3 state still validating after restart")
    check(js["scan"]["cursor"] == e2 and js["scan"]["scanned"] == 2, "B3 scan cursor survived restart")
    check(len(js.get("enforcement_triggers_present", [])) == 4, "B3 triggers survived restart")
    expect("POST", app1, "/deployments", {"service_id": 6666}, 409, "B3 enforcement works on restarted process")

    # Failed finalize, then another restart: must NOT become enabled.
    expect("POST", app1, "/validation/finalize", None, 409, "B4 finalize fails while orphans exist")
    subprocess.run(restart_hook, shell=True, check=True)
    check(wait_ready(app1), "B4 app1 back after 2nd restart")
    st, js = req("GET", app1, "/validation/status")
    check(js.get("state") == "validating", "B4 failure+restart did not produce enabled state")
    check(js.get("live_orphans", 0) == 2, "B4 orphans still tracked")

    # Resume after restart: fix orphans, drain, finalize.
    expect("PATCH", app1, f"/deployments/{e2}", {"service_id": s1}, 200, "B5 fix e2 after restart")
    expect("PATCH", app2, f"/deployments/{e3}", {"service_id": s2}, 200, "B5 fix e3 (app2)")
    last = drain_scan(app2)
    check(last is not None and last["remaining"] == 0, "B5 scan drained after restart")
    js = expect("POST", app1, "/validation/finalize", None, 200, "B5 finalize after restart")
    check(js.get("state") == "enabled", "B5 enabled")

    # Enabled state survives restart, enforcement intact, no orphans.
    subprocess.run(restart_hook, shell=True, check=True)
    check(wait_ready(app1), "B6 app1 back after 3rd restart")
    st, js = req("GET", app1, "/validation/status")
    check(js.get("state") == "enabled" and js.get("live_orphans") == 0,
          "B6 enabled + zero orphans after restart (no enabled-with-orphans)")
    expect("POST", app1, "/deployments", {"service_id": 6666}, 409, "B6 enforcement intact after restart")
    expect("POST", app2, "/deployments", {"service_id": s1}, 201, "B6 valid insert still works")


# --------------------------------------------------------------------------- #
# Phase C: concurrency storm
# --------------------------------------------------------------------------- #

def phase_c(app1, app2):
    print("== Phase C: concurrency storm (scan/fix/delete/bad-writes interleaved) ==")
    random.seed(20261118)

    expect("POST", app1, "/demo/reset", None, 200, "C0 reset")
    svc_ids = [
        expect("POST", app1, "/services", {"name": n}, 201, f"C0 svc {n}")["id"]
        for n in ("alpha", "beta", "gamma")
    ]
    # 3 guaranteed references (one per service) + 27 random valid + 30 orphans.
    for sid in svc_ids:
        req("POST", app2, "/deployments", {"service_id": sid, "note": "anchor"})
    for i in range(27):
        port = app1 if i % 2 == 0 else app2
        req("POST", port, "/deployments", {"service_id": random.choice(svc_ids), "note": f"v{i}"})
    for i in range(30):
        port = app1 if i % 2 == 0 else app2
        req("POST", port, "/deployments", {"service_id": 9000 + i, "note": f"orphan{i}"})
    st, js = req("GET", app1, "/orphans")
    check(js.get("count") == 30, "C0 seeded 30 orphans")

    expect("POST", app1, "/validation/start", None, 200, "C0 start validation")

    stop = threading.Event()
    errors = []
    lock = threading.Lock()

    def record(msg):
        with lock:
            errors.append(msg)

    def port_of(i):
        return app1 if i % 2 == 0 else app2

    def scanner():
        i = 0
        while not stop.is_set():
            try:
                st, js = req("POST", port_of(i), "/validation/step?batch_size=7")
                if st != 200:
                    record(f"scanner: step -> {st} {js}")
            except Exception as exc:
                record(f"scanner: {exc!r}")
            i += 1
            time.sleep(0.01)

    def fixer():
        while not stop.is_set():
            try:
                st, js = req("GET", port_of(random.randrange(2)), "/orphans")
                if st != 200:
                    record(f"fixer: orphans -> {st}")
                    continue
                for o in js["orphans"][:5]:
                    st2, js2 = req("PATCH", port_of(random.randrange(2)),
                                   f"/deployments/{o['id']}",
                                   {"service_id": random.choice(svc_ids)})
                    if st2 not in (200, 404):
                        record(f"fixer: patch {o['id']} -> {st2} {js2}")
            except Exception as exc:
                record(f"fixer: {exc!r}")
            time.sleep(0.01)

    def deleter():
        while not stop.is_set():
            try:
                st, js = req("GET", port_of(random.randrange(2)), "/orphans")
                if st != 200:
                    record(f"deleter: orphans -> {st}")
                    continue
                for o in js["orphans"]:
                    if o["id"] % 7 == 0:  # delete some orphans instead of fixing
                        st2, js2 = req("DELETE", port_of(random.randrange(2)),
                                       f"/deployments/{o['id']}", None)
                        if st2 not in (200, 404):
                            record(f"deleter: delete {o['id']} -> {st2} {js2}")
            except Exception as exc:
                record(f"deleter: {exc!r}")
            time.sleep(0.02)

    def bad_writer():
        while not stop.is_set():
            try:
                port = port_of(random.randrange(2))
                st, js = req("POST", port, "/deployments", {"service_id": 8888, "note": "bad"})
                if st != 409:
                    record(f"bad_writer: orphan insert -> {st} {js}")
                st, js = req("GET", port, "/deployments")
                if st == 200 and js["deployments"]:
                    d = random.choice(js["deployments"])
                    st2, js2 = req("PATCH", port, f"/deployments/{d['id']}", {"service_id": 8888})
                    if st2 not in (409, 404):
                        record(f"bad_writer: re-point -> {st2} {js2}")
                sid = random.choice(svc_ids)
                st3, js3 = req("DELETE", port, f"/services/{sid}", None)
                if st3 != 409:
                    record(f"bad_writer: delete referenced svc -> {st3} {js3}")
                st4, js4 = req("POST", port, "/deployments",
                               {"service_id": random.choice(svc_ids), "note": "ok"})
                if st4 != 201:
                    record(f"bad_writer: valid insert -> {st4} {js4}")
                name = f"tmp-{uuid.uuid4().hex[:12]}"
                st5, js5 = req("POST", port, "/services", {"name": name})
                if st5 != 201:
                    record(f"bad_writer: create tmp svc -> {st5} {js5}")
                else:
                    st6, js6 = req("DELETE", port, f"/services/{js5['id']}", None)
                    if st6 != 200:
                        record(f"bad_writer: delete unreferenced svc -> {st6} {js6}")
            except Exception as exc:
                record(f"bad_writer: {exc!r}")
            time.sleep(0.01)

    def monitor():
        while not stop.is_set():
            try:
                for port in (app1, app2):
                    st, js = req("GET", port, "/validation/status")
                    if st != 200:
                        record(f"monitor: status -> {st}")
                    elif js["state"] != "validating":
                        record(f"monitor: unexpected state {js['state']} on :{port}")
                    elif len(js["enforcement_triggers_present"]) != 4:
                        record(f"monitor: triggers missing on :{port}")
            except Exception as exc:
                record(f"monitor: {exc!r}")
            time.sleep(0.1)

    with ThreadPoolExecutor(max_workers=9) as pool:
        futures = [pool.submit(scanner),
                   pool.submit(fixer), pool.submit(fixer), pool.submit(fixer),
                   pool.submit(deleter),
                   pool.submit(bad_writer), pool.submit(bad_writer),
                   pool.submit(monitor)]
        time.sleep(8)
        stop.set()
        for f in futures:
            f.result(timeout=60)

    check(not errors, f"C1 storm produced no unexpected responses ({len(errors)} errors)"
                      + ("" if not errors else f": {errors[:5]}"))

    # Drain and reconcile.
    last = drain_scan(app1)
    check(last is not None and last["remaining"] == 0, "C2 scan drained after storm")
    n = fix_all_orphans(app2, svc_ids[0])
    print(f"    (fixed {n} orphans remaining after storm)")
    st, js = req("GET", app1, "/orphans")
    check(js.get("count") == 0, "C2 zero orphans after final fixes")

    js = expect("POST", app2, "/validation/finalize", None, 200, "C3 finalize after storm")
    check(js.get("state") == "enabled", "C3 enabled")
    for port in (app1, app2):
        st, js = req("GET", port, "/validation/status")
        check(js.get("state") == "enabled" and js.get("live_orphans") == 0,
              f"C3 enabled + zero orphans on :{port}")
    st, js = req("GET", app1, "/validation/findings")
    still = [f for f in js["findings"] if f["current_status"] == "still_orphan"]
    check(len(js["findings"]) > 0, "C4 scan recorded findings during storm")
    check(not still, "C4 every finding reconciled (fixed or deleted), none still orphan")

    expect("POST", app1, "/deployments", {"service_id": 8888}, 409, "C5 bad insert rejected after enable")
    expect("DELETE", app2, f"/services/{svc_ids[0]}", None, 409, "C5 delete referenced svc rejected after enable")


# --------------------------------------------------------------------------- #
# Phase D: cross-instance lock-contention diagnostics
# --------------------------------------------------------------------------- #

def phase_d(app1, app2, restart_hook, timeout_test):
    print("== Phase D: cross-instance lock contention diagnostics ==")
    uniq = uuid.uuid4().hex[:8]
    results = {}

    def fire(key, method, port, path, body=None):
        try:
            results[key] = req(method, port, path, body)
        except Exception as exc:  # e.g. connection reset when the process is killed
            results[key] = (-1, {"error": repr(exc)})

    def record_of(key):
        """(http status, lock event dict or None) for a fired request."""
        st, js = results.get(key, (-1, {}))
        diag_id = js.get("diag_id") if isinstance(js, dict) else None
        if diag_id is None:
            return st, None
        st2, ev = req("GET", app2, f"/locks/events/{diag_id}")
        return st, (ev if st2 == 200 else None)

    # D1: every write returns a diagnostic id; the record is queryable from
    # BOTH instances and shows op, instance, wait/hold durations, outcome.
    st, js = req("POST", app1, "/services", {"name": f"diag-{uniq}"})
    check(st == 201 and isinstance(js.get("diag_id"), int), "D1 write returns a diagnostic id")
    st, ev = req("GET", app2, f"/locks/events/{js.get('diag_id')}")
    check(st == 200 and ev.get("op") == "create_service" and ev.get("instance") == "app1",
          "D1 record queryable cross-instance, shows op + instance")
    check(ev.get("state") == "completed" and ev.get("outcome") == "committed",
          "D1 successful write recorded as committed")
    check(ev.get("wait_ms") is not None and ev.get("hold_ms") is not None
          and ev["wait_ms"] >= 0 and ev["hold_ms"] >= 0,
          "D1 wait and hold durations recorded")

    # D1b: a FAILED write must be recorded as failed - never as a success.
    st, js = req("POST", app1, "/services", {"name": f"diag-{uniq}"})  # duplicate -> 409
    check(st == 409 and isinstance(js.get("diag_id"), int),
          "D1b failed write still returns its diagnostic id")
    st, ev = req("GET", app2, f"/locks/events/{js['diag_id']}")
    check(st == 200 and ev.get("state") == "failed" and "committed" not in (ev.get("outcome") or ""),
          "D1b failed write recorded as failed (not as success)")

    # D2: contention - one holder, and a mix of waiters (deployment correction,
    # validation step, reset, plain writes) queuing behind it, observed live.
    # The hold must be comfortably SHORTER than the servers' busy timeout so
    # the waiters survive the queue (D4 exercises the give-up path separately).
    sx = expect("POST", app1, "/services", {"name": f"dx-{uniq}"}, 201, "D2 seed svc x")["id"]
    sy = expect("POST", app1, "/services", {"name": f"dy-{uniq}"}, 201, "D2 seed svc y")["id"]
    dep = expect("POST", app2, "/deployments", {"service_id": sx, "note": "to-correct"}, 201,
                 "D2 seed deployment")["id"]

    st, health1 = req("GET", app1, "/health")
    busy_ms = health1.get("busy_timeout_ms", 30000)
    HOLD_S = max(2.0, min(5.0, busy_ms / 1000 * 0.6))
    th_hold = threading.Thread(target=fire,
                               args=("hold", "POST", app1, f"/demo/hold-lock?seconds={HOLD_S}"))
    th_hold.start()

    # The holder must become visible from app2 WHILE the main DB is locked.
    snap = None
    t0 = time.time()
    while time.time() - t0 < 5:
        st, cur = req("GET", app2, "/locks/current")
        if st == 200 and cur["holders"]:
            snap = cur
            break
        time.sleep(0.1)
    check(snap is not None, "D2 diagnostics readable while main DB write-locked; holder visible")
    if snap:
        check(len(snap["holders"]) == 1, "D2 exactly one holder")
        h = snap["holders"][0]
        check(h["op"] == "demo_hold_lock" and h["instance"] == "app1" and h.get("instance_live"),
              "D2 holder is app1/demo_hold_lock and live")

    waiter_specs = [
        ("patch", "PATCH", app2, f"/deployments/{dep}", {"service_id": sy}),
        ("step", "POST", app1, "/validation/step?batch_size=5", None),
        ("reset", "POST", app2, "/demo/reset", None),
        ("mk1", "POST", app1, "/services", {"name": f"w1-{uniq}"}),
        ("mk2", "POST", app2, "/services", {"name": f"w2-{uniq}"}),
    ]
    threads = []
    for key, m, p, path, body in waiter_specs:
        t = threading.Thread(target=fire, args=(key, m, p, path, body))
        t.start()
        threads.append(t)
        time.sleep(0.15)

    # While the lock is still held, the waiter queue is visible from BOTH instances.
    time.sleep(min(0.6, HOLD_S * 0.2))
    st, cur2 = req("GET", app2, "/locks/current")
    st1, cur1 = req("GET", app1, "/locks/current")
    check(st == 200 and st1 == 200, "D2 /locks/current served by both instances during the hold")
    waiter_ops = {w["op"] for w in cur2["waiters"]}
    check({"patch_deployment", "validation_step", "demo_reset", "create_service"} <= waiter_ops,
          f"D2 waiter queue shows the interleaved mix (got {sorted(waiter_ops)})")
    check(len(cur2["waiters"]) >= 5, "D2 all five waiters queued")
    q = [w["queued_at"] for w in cur2["waiters"]]
    check(q == sorted(q), "D2 waiters listed in enqueue order")
    check({w["id"] for w in cur1["waiters"]} == {w["id"] for w in cur2["waiters"]},
          "D2 both instances report the same waiter set")

    for t in threads:
        t.join()
    th_hold.join()

    # Every participant's record must match what actually happened (a failure
    # is never recorded as success).
    expected_status = {"hold": (200,), "patch": (200, 404), "step": (409,),
                       "reset": (200,), "mk1": (201,), "mk2": (201,)}
    all_events = []
    for key, want in expected_status.items():
        http_st, ev = record_of(key)
        check(http_st in want, f"D2 {key}: HTTP {http_st}, want one of {want}")
        if ev is None:
            check(False, f"D2 {key}: diagnostic record missing")
            continue
        all_events.append((key, ev))
        if http_st < 300:
            check(ev["state"] == "completed" and ev["outcome"] == "committed",
                  f"D2 {key}: 2xx recorded as committed")
        else:
            check(ev["state"] == "failed" and "committed" not in (ev["outcome"] or ""),
                  f"D2 {key}: error recorded as failed, not as success")
        check(ev.get("acquired_at") is not None and ev.get("released_at") is not None,
              f"D2 {key}: lock interval recorded")

    # D3: the displayed hold order must correspond to actual lock ownership:
    # sorted by acquisition time, hold intervals must not overlap, and the
    # demo holder (which grabbed the lock first) comes first.
    if (len(all_events) == len(expected_status)
            and all(ev.get("acquired_at") is not None for _, ev in all_events)):
        ordered = sorted(all_events, key=lambda kv: kv[1]["acquired_at"])
        check(ordered[0][0] == "hold", "D3 the demo holder acquired the lock first")
        overlap_ok = all(
            ordered[i + 1][1]["acquired_at"] >= ordered[i][1]["released_at"] - 0.1
            for i in range(len(ordered) - 1)
        )
        check(overlap_ok, "D3 hold intervals do not overlap (order matches real lock ownership)")
        check(all(ev["wait_ms"] is not None and ev["wait_ms"] >= 0 for _, ev in all_events)
              and all(ev["hold_ms"] is not None and ev["hold_ms"] >= 0 for _, ev in all_events),
              "D3 wait/hold durations recorded for every participant")
        hold_ev = dict(all_events)["hold"]
        check(hold_ev["hold_ms"] >= HOLD_S * 800, "D3 holder held the lock for ~the requested time")
        waits = [ev["wait_ms"] for k, ev in all_events if k != "hold"]
        check(all(w >= 500 for w in waits), "D3 every waiter genuinely queued behind the holder")

    # D4: busy timeout (only with --timeout-test and a short BUSY_TIMEOUT_MS).
    if timeout_test:
        st, health2 = req("GET", app2, "/health")
        busy_ms = health2.get("busy_timeout_ms", 30000)
        if busy_ms > 10000:
            print(f"    (D4 skipped: app2 busy_timeout={busy_ms} ms; restart the apps with "
                  f"BUSY_TIMEOUT_MS=3000 to run the timeout test)")
        else:
            hold_s = busy_ms / 1000 + 3
            th = threading.Thread(target=fire, args=("slowhold", "POST", app1,
                                                     f"/demo/hold-lock?seconds={hold_s}"))
            th.start()
            time.sleep(0.6)
            t0 = time.time()
            st, js = req("POST", app2, "/services", {"name": f"slow-{uniq}"})
            waited = time.time() - t0
            check(st == 503 and isinstance(js.get("diag_id"), int),
                  "D4 waiter gives up with 503 (+diag_id) after busy timeout")
            check(waited >= busy_ms / 1000 * 0.8,
                  f"D4 waiter actually waited ~busy_timeout ({waited:.1f}s)")
            st, ev = req("GET", app1, f"/locks/events/{js.get('diag_id')}")
            check(st == 200 and ev.get("state") == "timeout"
                  and "busy_timeout" in (ev.get("outcome") or ""),
                  "D4 timeout recorded with reason")
            check(ev.get("acquired_at") is None and ev.get("hold_ms") is None
                  and (ev.get("wait_ms") or 0) >= busy_ms * 0.8,
                  "D4 timeout record: never acquired, waited the full budget")
            th.join(timeout=hold_s + 15)
            st, js = results.get("slowhold", (-1, {}))
            if isinstance(js, dict) and js.get("diag_id") is not None:
                st2, ev2 = req("GET", app1, f"/locks/events/{js['diag_id']}")
                check(st2 == 200 and ev2.get("state") == "completed",
                      "D4 holder itself completed normally")

    # D5: SIGKILL the holder mid-hold. The unfinished record must be marked
    # 'interrupted' at restart and must not keep posing as the lock holder.
    if restart_hook:
        th = threading.Thread(target=fire, args=("doomed", "POST", app1,
                                                 "/demo/hold-lock?seconds=30"))
        th.start()
        doomed_id = None
        t0 = time.time()
        while time.time() - t0 < 5:
            st, cur = req("GET", app2, "/locks/current")
            if st == 200 and cur["holders"]:
                doomed_id = cur["holders"][0]["id"]
                break
            time.sleep(0.1)
        check(doomed_id is not None, "D5 holder visible before the crash")
        subprocess.run(restart_hook, shell=True, check=True)
        check(wait_ready(app1), "D5 app1 back after SIGKILL")
        th.join(timeout=15)  # the HTTP request died with the process
        if doomed_id is not None:
            st, ev = req("GET", app2, f"/locks/events/{doomed_id}")
            check(st == 200 and ev.get("state") == "interrupted"
                  and "observation_interrupted" in (ev.get("outcome") or ""),
                  "D5 crashed holder's record marked observation-interrupted")
            st, cur = req("GET", app2, "/locks/current")
            check(st == 200 and not cur["holders"]
                  and all(h["id"] != doomed_id for h in cur["holders"]),
                  "D5 restarted instance does not impersonate the dead holder")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--app1", type=int, default=8001)
    parser.add_argument("--app2", type=int, default=8002)
    parser.add_argument("--restart-hook", default=None,
                        help="shell command that restarts the app1 process")
    parser.add_argument("--timeout-test", action="store_true",
                        help="also run the busy-timeout test (D4); apps must run with "
                             "BUSY_TIMEOUT_MS<=10000, e.g. BUSY_TIMEOUT_MS=3000 bash scripts/local_up.sh")
    args = parser.parse_args()

    print(f"targets: app1=:{args.app1} app2=:{args.app2}")
    check(wait_ready(args.app1), "app1 /health")
    check(wait_ready(args.app2), "app2 /health")
    if FAILURES:
        print("processes not reachable; aborting")
        sys.exit(1)

    phase_a(args.app1, args.app2)
    phase_b(args.app1, args.app2, args.restart_hook)
    phase_c(args.app1, args.app2)
    phase_d(args.app1, args.app2, args.restart_hook, args.timeout_test)

    print(f"\n{CHECKS} checks, {len(FAILURES)} failures")
    if FAILURES:
        for f in FAILURES:
            print(f"  FAILED: {f}")
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    main()
