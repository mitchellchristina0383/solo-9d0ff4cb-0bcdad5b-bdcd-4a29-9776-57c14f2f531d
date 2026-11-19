# Online FK enablement on a legacy SQLite database

A legacy database allows `deployments.service_id` to reference services that do
not exist. This demo enables the referential constraint **online** — while two
service processes keep reading and writing the same database — using FastAPI +
SQLite, modeled on PostgreSQL's `NOT VALID` → `VALIDATE CONSTRAINT` flow.

```
state:  off  --POST /validation/start-->  validating  --POST /validation/finalize-->  enabled
```

## How it works

| Concern | Mechanism |
| --- | --- |
| "From validation start, no new orphans" | 4 enforcement **triggers** (deployments INSERT / UPDATE service_id, services DELETE / UPDATE id) are created **in the same transaction** as the `off → validating` flip. They live in the SQLite schema, so they bind *every* process sharing the file and survive restarts. |
| "Writes continue during validation" | Valid inserts/updates/deletes are untouched; only orphan-creating statements are refused (HTTP 409). Old orphans can still be fixed (`PATCH /deployments/{id}` to an existing service). |
| "Batched validation" | `POST /validation/step?batch_size=N` scans `deployments` in id order; the cursor is committed per batch (`validation_progress`), so a crash resumes where it stopped. Found orphans are logged to `validation_findings`. |
| "Interleaved corrections can't slip through" | Triggers make valid→invalid transitions impossible from the start; `finalize` re-counts **live** orphans inside the same write transaction that flips the state, so post-scan changes are rechecked atomically. |
| "Enabled only when all rows valid" | `POST /validation/finalize` drains the unscanned tail and requires `live_orphans == 0` in one `BEGIN IMMEDIATE` transaction — no writer on either process can interleave with the decision. Otherwise 409 + current orphan ids, state stays `validating`. |
| "No enabled-with-orphans after failure/restart" | `enabled` is only ever written together with a zero-orphan check; triggers persist in the schema; on startup the app re-ensures triggers exist whenever state ∈ {validating, enabled}. |

## Run with Docker Compose

```bash
docker compose up --build
# app1 → http://localhost:8001/docs
# app2 → http://localhost:8002/docs   (same database via shared volume)
```

Both instances share `app.db` (business data) and `lockdiag.db` (lock
diagnostics, see below) on the same volume.

## Cross-instance lock-contention diagnostics

Two instances on one SQLite file means every write serializes on the single
writer lock — and when a write times out, you can't tell who was holding it.
Every mutating endpoint therefore records a **lock event** in a *separate*
diagnostics database (`lockdiag.db`, shared by both instances), and every
write response carries a `diag_id` (诊断号) — including error responses, so a
failed or timed-out request can be looked up afterwards.

| Endpoint | What it shows |
| --- | --- |
| `GET /locks/current` | Live snapshot: current lock **holder(s)** and the **waiter queue**, per-instance heartbeat/liveness. Works while the main DB is write-locked (reads only the diagnostics DB). |
| `GET /locks/events` | Recent lock events, newest first; filter by `state` / `instance` / `op`. |
| `GET /locks/events/{diag_id}` | One event by diagnostic number: operation type, instance, wait/hold durations, completion or timeout reason. |
| `POST /demo/hold-lock?seconds=N` | Demo/test helper: holds the write lock for N seconds so you can watch contention live in `/docs`. |

Lock event lifecycle: `waiting → holding → completed | failed | timeout`,
plus `interrupted` for records left unfinished by a process exit.

Design guarantees:

* **Diagnostics stay readable while the main DB is write-locked** — they live
  in a separate SQLite file; `GET /locks/*` never touches the main database.
* **Recording diagnostics never extends the main-DB lock hold.** The `waiting`
  row is written before the lock attempt; the `holding` mark is written by a
  fire-and-forget helper thread (the request thread does no diagnostics I/O
  while holding the lock); the completion record is written after
  COMMIT/ROLLBACK. Diagnostics also never turn a failed write into a success:
  the HTTP status is unchanged and the record says `failed`/`timeout` with
  the reason.
* **Displayed order matches real lock ownership.** `acquired_at` is stamped
  when `BEGIN IMMEDIATE` actually returns, so holders ordered by
  `acquired_at` reflect the true lock handoff; waiters are ordered by
  `queued_at`. This holds when validation, corrections and resets interleave.
* **A crashed holder cannot impersonate a live one.** Each instance
  heartbeats into the diagnostics DB; unfinished records of a dead instance
  are flagged `suspected_interrupted` immediately, and marked
  `interrupted` ("observation interrupted") when that instance restarts.

### Reproduce lock contention locally

```bash
bash scripts/local_up.sh                      # two processes, one SQLite file
python3 scripts/reproduce_contention.py       # hold the lock, queue writes, print diagnostics
# then browse http://localhost:8001/docs#/lock-diagnostics
```

The script holds the write lock on app1, fires writes at both instances
(they queue), reads `GET /locks/current` from the *peer* instance while the
lock is still held, then prints every participant's record.

To see a **busy timeout** (HTTP 503 + a `timeout` record with the reason),
start the apps with a short lock-wait budget:

```bash
bash scripts/local_down.sh
BUSY_TIMEOUT_MS=3000 bash scripts/local_up.sh
python3 scripts/reproduce_contention.py --timeout-demo
```

The same works under Docker Compose (`docker compose up`), browsing
`http://localhost:8001/docs` or `:8002/docs`.

## Demo walkthrough (via /docs)

All steps are clickable in Swagger UI (`/docs` on either port):

1. `POST /demo/reset` — clean slate (state `off`, no triggers).
2. `POST /services` `{"name": "web"}` → note the id (e.g. 1).
3. `POST /deployments` `{"service_id": 1}` (valid) and `{"service_id": 9999}` — **accepted while `off`**: the legacy defect.
4. `GET /orphans` — see the orphan.
5. `DELETE /services/1` — also allowed while `off` (creates more orphans). This is the problem being fixed.
6. `POST /validation/start` — triggers installed, state `validating`. From now on, on **both** processes: bad inserts / re-points / referenced-service deletes → 409; valid writes still work.
7. `POST /validation/step?batch_size=2` repeatedly — watch `cursor`/`remaining`; `GET /validation/findings` lists orphans found by the scan.
8. `PATCH /deployments/{id}` `{"service_id": <existing>}` — correct old orphans (still allowed).
9. `POST /validation/finalize` — 409 while any orphan remains; once all are fixed → 200, state `enabled`.
10. `GET /validation/status` — state, scan progress, live orphan count, triggers.

## Tests

```bash
# with Docker:
docker compose up -d --build
python3 tests/selftest.py --restart-hook "docker compose restart app1"

# without Docker (two local processes sharing one SQLite file):
bash scripts/local_up.sh
python3 tests/selftest.py --restart-hook "bash scripts/local_restart_app1.sh"
bash scripts/local_down.sh

# busy-timeout test (D4) needs a short lock-wait budget:
BUSY_TIMEOUT_MS=3000 bash scripts/local_up.sh
BUSY_TIMEOUT_MS=3000 python3 tests/selftest.py \
    --restart-hook "bash scripts/local_restart_app1.sh" --timeout-test
```

`tests/selftest.py` (stdlib only) covers: legacy behavior, cross-process
enforcement from validation start, batched scan with interleaved corrections,
failed-then-successful finalize, SIGKILL restart mid-validation / after failed
finalize / after enable, an 8 s concurrency storm (scanner + 3 fixers +
deleter + 2 bad writers + invariant monitor across both processes), and the
lock-contention diagnostics (diag_id on writes, live holder/waiter view while
the main DB is locked, failure honesty, hold-order vs. real lock ownership,
busy timeout, SIGKILL → `interrupted` records).
Last run here: **138 checks, 0 failures** (3 consecutive runs); timeout
variant with `BUSY_TIMEOUT_MS=3000 --timeout-test`: **143 checks, 0 failures**.

## Layout

```
app/main.py            FastAPI app: schema, triggers, endpoints, validation state machine,
                       lock-contention diagnostics (run_write + /locks/* + diagnostics DB)
tests/selftest.py      end-to-end self test (phases A/B/C/D)
scripts/reproduce_contention.py   lock-contention repro: hold the lock, watch /locks/*
scripts/local_*.sh     run/restart/stop two processes without Docker
Dockerfile, docker-compose.yml, requirements.txt
```

## Known limitations (honest list)

* **Logical, not native, FK.** SQLite cannot add a foreign key to an existing
  table; enforcement is via triggers and the `enabled` flag is a logical state.
  Migrating to a native `REFERENCES` clause would require a table rebuild
  (shadow table + swap) — not implemented here.
* **Triggers can be dropped by direct DB access.** Enforcement covers all
  writers going through either service process (and any client that doesn't
  touch the schema); it does not defend against someone with shell access to
  the database file dropping triggers — same trust model as any non-native
  constraint.
* **Single-writer SQLite.** `finalize` and each scan batch take the write lock
  briefly; throughput is bounded by SQLite's single writer. Fine for a demo;
  not a horizontally scalable design.
* **Diagnostics observability gaps.** The `holding` mark is written by a
  helper thread, so it appears a few milliseconds after acquisition; a
  sub-millisecond hold may never display as `holding` live (its completed
  record still carries exact `acquired_at`/`hold_ms`). Waiter *grant* order is
  decided by SQLite, not by the queue display.
* **Crash detection is heartbeat-based.** An unfinished record of a dead
  instance is flagged `suspected_interrupted` once its heartbeat goes stale
  (~3 s) and is marked `interrupted` only when *that* instance restarts; if it
  never restarts, the row stays unfinished (flagged, not impersonated).
  Instances never rewrite each other's rows, so a live holder is never
  falsely marked interrupted.
* **Diagnostics DB grows unbounded** — no retention/cleanup is implemented.
* **Diagnostics are best-effort.** If the diagnostics DB is unavailable,
  writes still proceed (`diag_id: null`) — diagnostics never block business
  writes. Both instances must share one host clock for cross-instance
  timestamp ordering (true for one Compose host / one machine).
* **One process per instance name.** The startup sweep and the heartbeat are
  keyed by `SERVER_NAME`; do not run multiple worker processes sharing one
  `SERVER_NAME` (e.g. `uvicorn --workers 4`) — give each process its own name.
* **Scan cursor relies on monotonic ids** (`AUTOINCREMENT`). Even if ids were
  reused, the atomic live-orphan recount in `finalize` remains authoritative.
* **Shared volume must be a local filesystem** (docker named volume on one
  host). SQLite locking does not work reliably over NFS.
* The concurrency storm exercises interleaved HTTP requests; it does not
  formally prove race-freedom — the safety argument rests on the transactional
  design (triggers + atomic finalize), which the tests probe but cannot
  exhaustively verify.
