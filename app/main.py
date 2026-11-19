"""
Online enablement of a referential constraint on a legacy SQLite database.

Legacy situation
----------------
``deployments.service_id`` references ``services.id`` but the column was created
WITHOUT a foreign key, so orphan deployments (pointing at deleted / never
existing services) exist. SQLite cannot add an FK to an existing table without
rebuilding it, so this demo enables the constraint *logically* and *online*,
modeled on PostgreSQL's ``NOT VALID`` -> ``VALIDATE CONSTRAINT`` flow:

  state: off  --/validation/start-->  validating  --/validation/finalize-->  enabled

Mechanism
---------
1. /validation/start installs four enforcement triggers and flips the state to
   ``validating`` inside ONE immediate transaction. From that commit on, no
   writer in ANY process can create a new orphan (insert bad deployment,
   re-point a deployment to a missing service, delete/renumber a referenced
   service). Writes that keep the constraint are still accepted, and existing
   orphans can still be fixed. The triggers live in the SQLite schema, so they
   are shared by both service processes and survive restarts.
2. /validation/step scans ``deployments`` in id-ordered batches (cursor kept in
   ``validation_progress``, committed per batch -> crash resumable) and logs
   orphans into ``validation_findings``.
3. /validation/finalize, inside ONE immediate (write-locked) transaction:
   drains the unscanned tail, counts *live* orphans, and only if zero flips the
   state to ``enabled``. Because the write lock is held, no writer on either
   process can interleave between the check and the state flip, and rows
   scanned earlier cannot have become invalid (the triggers forbid it). If
   orphans remain, the state stays ``validating`` and the caller gets the
   current orphan ids; it can be retried after corrections.

Invariants
----------
* From validation start: no new orphans, from any process, ever.
* Corrections interleaved with the scan can only shrink the orphan set; the
  atomic finalize re-checks every row, so nothing slips through.
* ``enabled`` is only ever written in the same transaction as a zero-orphan
  check, and the triggers persist -> after a failure or a restart the database
  can never be in an "enabled but orphans exist" state.

Cross-instance lock-contention diagnostics
------------------------------------------
Both instances share ONE SQLite file, so every write serializes on the single
writer lock. To make that contention observable, every mutating endpoint runs
through ``run_write()``, which records a lock event in a SEPARATE diagnostics
database (``lockdiag.db`` next to the main database, shared by both instances):

  waiting -> holding -> completed | failed | timeout        (normal lifecycle)
                     \-> interrupted                        (process died; see below)

* Write responses carry a ``diag_id`` (the diagnostic number); error responses
  (409/503) carry it too, so a timed-out request can be looked up afterwards.
* ``GET /locks/current`` shows the current lock holder(s) and the waiter queue
  (ordered by actual lock acquisition / enqueue time) from EITHER instance,
  and works while the main database is write-locked, because it only reads the
  diagnostics database.
* ``GET /locks/events/{diag_id}`` shows one event: operation type, instance,
  wait/hold durations, and the completion or timeout reason.
* Recording diagnostics never extends the main-DB lock hold: the "waiting" row
  is written before the lock attempt, the "holding" mark is done by a
  fire-and-forget thread, and the completion record is written after
  COMMIT/ROLLBACK. Failed writes are recorded as failed and keep their HTTP
  error status - diagnostics never turn a failure into a success.
* Each instance heartbeats into the diagnostics DB. On startup an instance
  marks its own unfinished records ``interrupted`` (observation interrupted by
  process exit), so a crashed holder never impersonates a live one after
  restart; while the crashed instance is down, its stale heartbeat flags its
  unfinished records as ``suspected_interrupted`` in /locks/current.
"""

import os
import sqlite3
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

DB_PATH = os.environ.get("DB_PATH", "data/app.db")
SERVER_NAME = os.environ.get("SERVER_NAME", "app")
# Diagnostics live in a SEPARATE SQLite file (shared by both instances via the
# same volume) so they stay readable/writable while the MAIN database is
# write-locked, and so that recording diagnostics never adds work to the main
# database's lock.
DIAG_DB_PATH = os.environ.get("DIAG_DB_PATH") or os.path.join(
    os.path.dirname(os.path.abspath(DB_PATH)), "lockdiag.db"
)
# How long a writer waits for the main-DB write lock before giving up (HTTP
# 503 + a 'timeout' lock event). Configurable so lock timeouts can be
# reproduced quickly in demos/tests (e.g. BUSY_TIMEOUT_MS=3000).
BUSY_TIMEOUT_MS = int(os.environ.get("BUSY_TIMEOUT_MS", "30000"))

# Identity of this process run, stamped on every lock event so records can be
# attributed to a specific instance AND a specific boot of it.
RUN_ID = uuid.uuid4().hex[:12]
PID = os.getpid()

HEARTBEAT_INTERVAL_S = 1.0
HEARTBEAT_STALE_S = 3.0  # older than this -> instance considered not live

SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS services (
    id   INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE
);
-- NOTE: no FOREIGN KEY on service_id. That is the legacy defect being fixed.
CREATE TABLE IF NOT EXISTS deployments (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    service_id INTEGER NOT NULL,
    note       TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS constraint_state (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    state      TEXT NOT NULL CHECK (state IN ('off', 'validating', 'enabled')),
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS validation_progress (
    id          INTEGER PRIMARY KEY CHECK (id = 1),
    cursor      INTEGER NOT NULL DEFAULT 0,
    scanned     INTEGER NOT NULL DEFAULT 0,
    started_at  TEXT,
    finished_at TEXT
);
CREATE TABLE IF NOT EXISTS validation_findings (
    deployment_id INTEGER PRIMARY KEY,
    service_id    INTEGER NOT NULL,
    found_at      TEXT NOT NULL
);
"""

DIAG_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS lock_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    instance    TEXT NOT NULL,           -- SERVER_NAME of the writer
    run_id      TEXT NOT NULL,           -- uuid of that process boot
    pid         INTEGER NOT NULL,
    op          TEXT NOT NULL,           -- operation type (create_service, validation_step, ...)
    detail      TEXT NOT NULL DEFAULT '',-- short human context (ids, names)
    state       TEXT NOT NULL CHECK (state IN
                    ('waiting', 'holding', 'completed', 'failed', 'timeout', 'interrupted')),
    queued_at   REAL NOT NULL,           -- unix ts: started waiting for the write lock
    acquired_at REAL,                    -- unix ts: write lock actually acquired (lock ownership!)
    released_at REAL,                    -- unix ts: write lock released (commit/rollback done)
    closed_at   REAL,                    -- unix ts: this record reached its final state
    wait_ms     REAL,                    -- acquired_at - queued_at (or full wait on timeout)
    hold_ms     REAL,                    -- released_at - acquired_at
    outcome     TEXT                     -- committed | rolled_back:... | busy_timeout:... | observation_interrupted:...
);
CREATE INDEX IF NOT EXISTS idx_lock_events_state ON lock_events(state);
CREATE INDEX IF NOT EXISTS idx_lock_events_acquired ON lock_events(acquired_at);
CREATE TABLE IF NOT EXISTS heartbeats (
    instance TEXT PRIMARY KEY,
    run_id   TEXT NOT NULL,
    pid      INTEGER NOT NULL,
    ts       REAL NOT NULL
);
"""

TRIGGER_DDL = [
    """
    CREATE TRIGGER IF NOT EXISTS trg_deployments_ins_fk
    BEFORE INSERT ON deployments FOR EACH ROW
    WHEN NOT EXISTS (SELECT 1 FROM services WHERE id = NEW.service_id)
    BEGIN
        SELECT RAISE(ABORT, 'FK violation: deployments.service_id has no matching services.id');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_deployments_upd_fk
    BEFORE UPDATE OF service_id ON deployments FOR EACH ROW
    WHEN NOT EXISTS (SELECT 1 FROM services WHERE id = NEW.service_id)
    BEGIN
        SELECT RAISE(ABORT, 'FK violation: deployments.service_id has no matching services.id');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_services_del_fk
    BEFORE DELETE ON services FOR EACH ROW
    WHEN EXISTS (SELECT 1 FROM deployments WHERE service_id = OLD.id)
    BEGIN
        SELECT RAISE(ABORT, 'FK violation: services.id is still referenced by deployments');
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_services_upd_fk
    BEFORE UPDATE OF id ON services FOR EACH ROW
    WHEN EXISTS (SELECT 1 FROM deployments WHERE service_id = OLD.id)
    BEGIN
        SELECT RAISE(ABORT, 'FK violation: services.id is still referenced by deployments');
    END
    """,
]

TRIGGER_NAMES = [
    "trg_deployments_ins_fk",
    "trg_deployments_upd_fk",
    "trg_services_del_fk",
    "trg_services_upd_fk",
]


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def connect() -> sqlite3.Connection:
    # isolation_level=None -> autocommit; the write path uses explicit
    # BEGIN IMMEDIATE (see run_write) so check-and-act sequences are atomic
    # across both processes sharing the file.
    conn = sqlite3.connect(DB_PATH, timeout=BUSY_TIMEOUT_MS / 1000, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    return conn


def init_db() -> None:
    directory = os.path.dirname(os.path.abspath(DB_PATH))
    os.makedirs(directory, exist_ok=True)
    conn = connect()
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.executescript(SCHEMA_DDL)
        conn.execute(
            "INSERT OR IGNORE INTO constraint_state (id, state, updated_at) VALUES (1, 'off', ?)",
            (utcnow(),),
        )
        conn.execute("INSERT OR IGNORE INTO validation_progress (id) VALUES (1)")
        # Restart/crash recovery: if the constraint is (being) enforced, the
        # enforcement triggers must exist. Idempotent.
        state = conn.execute("SELECT state FROM constraint_state WHERE id = 1").fetchone()["state"]
        if state in ("validating", "enabled"):
            for ddl in TRIGGER_DDL:
                conn.execute(ddl)
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# lock-contention diagnostics (separate database, shared by both instances)
# --------------------------------------------------------------------------- #

def diag_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DIAG_DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_diag_db() -> None:
    os.makedirs(os.path.dirname(os.path.abspath(DIAG_DB_PATH)), exist_ok=True)
    conn = diag_connect()
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        conn.executescript(DIAG_SCHEMA_DDL)
    finally:
        conn.close()


def diag_begin(op: str, detail: str, queued_at: float):
    """Insert the 'waiting' row BEFORE attempting the main-DB write lock and
    return the diagnostic id. Never raises: diagnostics must not break writes.
    """
    try:
        conn = diag_connect()
        try:
            cur = conn.execute(
                "INSERT INTO lock_events (instance, run_id, pid, op, detail, state, queued_at)"
                " VALUES (?, ?, ?, ?, ?, 'waiting', ?)",
                (SERVER_NAME, RUN_ID, PID, op, str(detail)[:200], queued_at),
            )
            return cur.lastrowid
        finally:
            conn.close()
    except Exception:
        return None


def diag_mark_holding_async(diag_id, acquired_at: float, wait_ms: float) -> None:
    """Mark the event as holding the main write lock. Runs on a daemon thread
    so the diagnostics write NEVER extends the main-DB lock hold. The UPDATE
    is conditional (state='waiting'), so if this thread is scheduled late it
    can never flip an already-completed record back to 'holding'."""
    if diag_id is None:
        return

    def _mark():
        try:
            conn = diag_connect()
            try:
                conn.execute(
                    "UPDATE lock_events SET state='holding', acquired_at=?, wait_ms=?"
                    " WHERE id = ? AND state = 'waiting'",
                    (acquired_at, wait_ms, diag_id),
                )
            finally:
                conn.close()
        except Exception:
            pass

    threading.Thread(target=_mark, daemon=True).start()


def diag_finish(diag_id, *, state: str, outcome: str, acquired_at=None,
                released_at=None, wait_ms=None, hold_ms=None) -> None:
    """Write the final record for a lock event. Always called AFTER the
    main-DB lock has been released (or was never acquired), so recording it
    cannot extend the hold. Never raises."""
    if diag_id is None:
        return
    try:
        conn = diag_connect()
        try:
            conn.execute(
                """UPDATE lock_events SET state = ?, outcome = ?,
                       acquired_at = COALESCE(acquired_at, ?), released_at = ?,
                       wait_ms = COALESCE(wait_ms, ?), hold_ms = ?, closed_at = ?
                   WHERE id = ?""",
                (state, outcome, acquired_at, released_at, wait_ms, hold_ms,
                 time.time(), diag_id),
            )
        finally:
            conn.close()
    except Exception:
        pass


def diag_startup_sweep() -> int:
    """Mark THIS instance's unfinished records from previous runs as
    'interrupted'. Runs at startup before we serve requests, so any unfinished
    row of ours is necessarily dead. Other instances' rows are never touched
    here (we cannot know whether they are alive); their staleness is reported
    via heartbeats in /locks/current instead."""
    conn = diag_connect()
    try:
        cur = conn.execute(
            """UPDATE lock_events SET state = 'interrupted',
                   outcome = 'observation_interrupted: process exited before this record completed; lock state at exit is unknown',
                   closed_at = ?
               WHERE instance = ? AND state IN ('waiting', 'holding')""",
            (time.time(), SERVER_NAME),
        )
        return cur.rowcount
    finally:
        conn.close()


_heartbeat_stop = threading.Event()


def _heartbeat_once() -> None:
    conn = diag_connect()
    try:
        conn.execute(
            """INSERT INTO heartbeats (instance, run_id, pid, ts) VALUES (?, ?, ?, ?)
               ON CONFLICT(instance) DO UPDATE SET run_id = excluded.run_id,
                   pid = excluded.pid, ts = excluded.ts""",
            (SERVER_NAME, RUN_ID, PID, time.time()),
        )
    finally:
        conn.close()


def _heartbeat_loop() -> None:
    while not _heartbeat_stop.wait(HEARTBEAT_INTERVAL_S):
        try:
            _heartbeat_once()
        except Exception:
            pass


def _with_startup_retry(fn, attempts=20, delay=0.25):
    """Retry startup DB initialization when the peer instance holds the file
    lock. Two instances booting simultaneously race here: PRAGMA journal_mode
    can return 'database is locked' immediately instead of honoring the busy
    timeout, so a short retry loop is the robust option."""
    for attempt in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc) or attempt == attempts - 1:
                raise
            time.sleep(delay)


@asynccontextmanager
async def lifespan(_: FastAPI):
    _with_startup_retry(init_db)
    _with_startup_retry(init_diag_db)
    swept = _with_startup_retry(diag_startup_sweep)
    if swept:
        print(f"[lockdiag] startup: marked {swept} unfinished lock event(s) of "
              f"instance '{SERVER_NAME}' as interrupted (observation ended with the process)")
    try:
        _heartbeat_once()
    except Exception:
        pass
    hb = threading.Thread(target=_heartbeat_loop, daemon=True)
    hb.start()
    yield
    _heartbeat_stop.set()
    hb.join(timeout=2)


app = FastAPI(
    title="Online FK enablement demo",
    version="1.1.0",
    lifespan=lifespan,
    description=(
        "Legacy `deployments.service_id` has no enforced foreign key. "
        "Flow: seed data (orphans allowed while state=off) -> POST /validation/start "
        "-> POST /validation/step (batched scan) -> fix orphans via PATCH /deployments/{id} "
        "-> POST /validation/finalize (atomic zero-orphan gate) -> state=enabled. "
        "Every write is recorded in the cross-instance lock-contention diagnostics: "
        "watch GET /locks/current for the live holder/waiters and look up any "
        "diag_id under GET /locks/events/{diag_id}. "
        "POST /demo/hold-lock reproduces lock contention on demand."
    ),
)


@app.exception_handler(sqlite3.OperationalError)
async def operational_error_handler(_, exc: sqlite3.OperationalError):
    return JSONResponse(status_code=503, content={"detail": f"database temporarily unavailable: {exc}"})


@app.exception_handler(HTTPException)
async def http_exception_handler(_, exc: HTTPException):
    # Same shape as FastAPI's default, plus the lock diagnostic id when the
    # error came out of the instrumented write path (e.g. 409 FK refusal,
    # 503 busy timeout) so the failed request can be looked up afterwards.
    content = {"detail": exc.detail}
    diag_id = getattr(exc, "diag_id", None)
    if diag_id is not None:
        content["diag_id"] = diag_id
    return JSONResponse(status_code=exc.status_code, content=content,
                        headers=getattr(exc, "headers", None))


# --------------------------------------------------------------------------- #
# instrumented write path
# --------------------------------------------------------------------------- #

def _http(status_code: int, detail, diag_id=None) -> HTTPException:
    exc = HTTPException(status_code=status_code, detail=detail)
    exc.diag_id = diag_id
    return exc


def _rollback_quietly(conn: sqlite3.Connection) -> None:
    try:
        conn.execute("ROLLBACK")
    except sqlite3.OperationalError:
        pass


def run_write(op: str, detail: str, work):
    """Run ``work(conn)`` inside one BEGIN IMMEDIATE transaction on the main
    DB, recording a lock-contention event in the SEPARATE diagnostics DB.

    Timeline (main-DB lock is held only between BEGIN IMMEDIATE and
    COMMIT/ROLLBACK; diagnostics never extend that window and never turn a
    failed write into a success):

      1. insert 'waiting' row              (lock NOT held)
      2. BEGIN IMMEDIATE                   (blocks up to BUSY_TIMEOUT_MS;
                                            on timeout -> 'timeout' + HTTP 503)
      3. mark 'holding' from a helper thread (fire-and-forget; the request
         thread does no diagnostics I/O while holding the lock)
      4. work(conn) + COMMIT/ROLLBACK      (the only main-lock window)
      5. record completed/failed           (lock already released)
    """
    queued_at = time.time()
    queued_mono = time.monotonic()
    diag_id = diag_begin(op, detail, queued_at)
    conn = connect()
    try:
        try:
            conn.execute("BEGIN IMMEDIATE")
        except sqlite3.OperationalError as exc:
            wait_ms = (time.monotonic() - queued_mono) * 1000
            diag_finish(
                diag_id, state="timeout",
                outcome=f"busy_timeout: gave up after {wait_ms:.0f} ms waiting for the write lock ({exc})",
                wait_ms=wait_ms,
            )
            raise _http(
                503,
                f"database write lock busy for {wait_ms:.0f} ms "
                f"(busy_timeout={BUSY_TIMEOUT_MS} ms): {exc}",
                diag_id,
            )

        acquired_at = time.time()
        acquired_mono = time.monotonic()
        wait_ms = (acquired_mono - queued_mono) * 1000
        diag_mark_holding_async(diag_id, acquired_at, wait_ms)

        def finish_failed(outcome: str):
            _rollback_quietly(conn)  # lock released here, BEFORE the diag write
            diag_finish(
                diag_id, state="failed", outcome=outcome,
                acquired_at=acquired_at, released_at=time.time(),
                wait_ms=wait_ms, hold_ms=(time.monotonic() - acquired_mono) * 1000,
            )

        try:
            result = work(conn)
        except HTTPException as exc:
            exc.diag_id = diag_id
            finish_failed(f"rolled_back: HTTP {exc.status_code}: {str(exc.detail)[:160]}")
            raise
        except Exception as exc:
            finish_failed(f"rolled_back: {type(exc).__name__}: {str(exc)[:160]}")
            raise
        try:
            conn.execute("COMMIT")
        except Exception as exc:
            finish_failed(f"commit_failed: {type(exc).__name__}: {str(exc)[:160]}")
            raise

        diag_finish(
            diag_id, state="completed", outcome="committed",
            acquired_at=acquired_at, released_at=time.time(),
            wait_ms=wait_ms, hold_ms=(time.monotonic() - acquired_mono) * 1000,
        )
        if isinstance(result, dict):
            result["diag_id"] = diag_id
        return result
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #

def get_state(conn: sqlite3.Connection) -> str:
    return conn.execute("SELECT state FROM constraint_state WHERE id = 1").fetchone()["state"]


def live_orphan_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT d.id, d.service_id, d.note
        FROM deployments d
        WHERE NOT EXISTS (SELECT 1 FROM services s WHERE s.id = d.service_id)
        ORDER BY d.id
        """
    ).fetchall()


def scan_batch(conn: sqlite3.Connection, batch_size: int) -> dict:
    """Scan up to ``batch_size`` unscanned deployments. Caller holds the txn."""
    prog = conn.execute("SELECT cursor, scanned FROM validation_progress WHERE id = 1").fetchone()
    cursor, scanned = prog["cursor"], prog["scanned"]
    rows = conn.execute(
        "SELECT id, service_id FROM deployments WHERE id > ? ORDER BY id LIMIT ?",
        (cursor, batch_size),
    ).fetchall()
    found = []
    for row in rows:
        exists = conn.execute("SELECT 1 FROM services WHERE id = ?", (row["service_id"],)).fetchone()
        if not exists:
            conn.execute(
                "INSERT OR IGNORE INTO validation_findings (deployment_id, service_id, found_at)"
                " VALUES (?, ?, ?)",
                (row["id"], row["service_id"], utcnow()),
            )
            found.append(row["id"])
    if rows:
        cursor = rows[-1]["id"]
    scanned += len(rows)
    remaining = conn.execute(
        "SELECT COUNT(*) AS c FROM deployments WHERE id > ?", (cursor,)
    ).fetchone()["c"]
    conn.execute(
        "UPDATE validation_progress SET cursor = ?, scanned = ? WHERE id = 1",
        (cursor, scanned),
    )
    return {
        "batch": len(rows),
        "orphans_in_batch": found,
        "cursor": cursor,
        "scanned_total": scanned,
        "remaining": remaining,
    }


# --------------------------------------------------------------------------- #
# services
# --------------------------------------------------------------------------- #

class ServiceIn(BaseModel):
    name: str = Field(min_length=1, max_length=200)


@app.post("/services", status_code=201)
def create_service(body: ServiceIn):
    def work(conn):
        try:
            cur = conn.execute("INSERT INTO services (name) VALUES (?)", (body.name,))
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail=str(exc))
        return {"served_by": SERVER_NAME, "id": cur.lastrowid, "name": body.name}

    return run_write("create_service", f"name={body.name!r}", work)


@app.get("/services")
def list_services():
    conn = connect()
    try:
        rows = conn.execute(
            """
            SELECT s.id, s.name,
                   (SELECT COUNT(*) FROM deployments d WHERE d.service_id = s.id) AS deployment_count
            FROM services s ORDER BY s.id
            """
        ).fetchall()
        return {"served_by": SERVER_NAME, "services": [dict(r) for r in rows]}
    finally:
        conn.close()


@app.delete("/services/{service_id}")
def delete_service(service_id: int):
    def work(conn):
        try:
            cur = conn.execute("DELETE FROM services WHERE id = ?", (service_id,))
        except sqlite3.IntegrityError as exc:
            # Enforcement trigger (state validating/enabled) refused the delete.
            raise HTTPException(status_code=409, detail=f"refused: {exc}")
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="service not found")
        return {"served_by": SERVER_NAME, "deleted_service_id": service_id}

    return run_write("delete_service", f"service_id={service_id}", work)


# --------------------------------------------------------------------------- #
# deployments
# --------------------------------------------------------------------------- #

class DeploymentIn(BaseModel):
    service_id: int
    note: str = ""


class DeploymentPatch(BaseModel):
    service_id: int


@app.post("/deployments", status_code=201)
def create_deployment(body: DeploymentIn):
    def work(conn):
        try:
            cur = conn.execute(
                "INSERT INTO deployments (service_id, note) VALUES (?, ?)",
                (body.service_id, body.note),
            )
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail=f"refused: {exc}")
        return {"served_by": SERVER_NAME, "id": cur.lastrowid, "service_id": body.service_id}

    return run_write("create_deployment",
                     f"service_id={body.service_id} note={body.note[:60]!r}", work)


@app.get("/deployments")
def list_deployments():
    conn = connect()
    try:
        rows = conn.execute(
            """
            SELECT d.id, d.service_id, d.note,
                   EXISTS (SELECT 1 FROM services s WHERE s.id = d.service_id) AS valid
            FROM deployments d ORDER BY d.id
            """
        ).fetchall()
        deployments = [
            {"id": r["id"], "service_id": r["service_id"], "note": r["note"], "valid": bool(r["valid"])}
            for r in rows
        ]
        return {"served_by": SERVER_NAME, "deployments": deployments}
    finally:
        conn.close()


@app.patch("/deployments/{deployment_id}")
def patch_deployment(deployment_id: int, body: DeploymentPatch):
    """Re-point a deployment. This is how legacy orphans are corrected; the
    triggers only forbid pointing at a *missing* service."""

    def work(conn):
        try:
            cur = conn.execute(
                "UPDATE deployments SET service_id = ? WHERE id = ?",
                (body.service_id, deployment_id),
            )
        except sqlite3.IntegrityError as exc:
            raise HTTPException(status_code=409, detail=f"refused: {exc}")
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="deployment not found")
        return {"served_by": SERVER_NAME, "id": deployment_id, "service_id": body.service_id}

    return run_write("patch_deployment",
                     f"deployment_id={deployment_id} service_id={body.service_id}", work)


@app.delete("/deployments/{deployment_id}")
def delete_deployment(deployment_id: int):
    def work(conn):
        cur = conn.execute("DELETE FROM deployments WHERE id = ?", (deployment_id,))
        if cur.rowcount == 0:
            raise HTTPException(status_code=404, detail="deployment not found")
        return {"served_by": SERVER_NAME, "deleted_deployment_id": deployment_id}

    return run_write("delete_deployment", f"deployment_id={deployment_id}", work)


# --------------------------------------------------------------------------- #
# orphans
# --------------------------------------------------------------------------- #

@app.get("/orphans")
def get_orphans():
    """Live view of deployments whose service_id currently has no service."""
    conn = connect()
    try:
        rows = live_orphan_rows(conn)
        return {
            "served_by": SERVER_NAME,
            "count": len(rows),
            "orphans": [dict(r) for r in rows],
        }
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# validation lifecycle
# --------------------------------------------------------------------------- #

@app.post("/validation/start")
def validation_start():
    """Begin online validation: install enforcement triggers and switch
    state off -> validating, atomically. From this commit on, no process can
    create new orphans."""

    def work(conn):
        state = get_state(conn)
        if state == "validating":
            raise HTTPException(status_code=409, detail="validation already running")
        if state == "enabled":
            raise HTTPException(status_code=409, detail="constraint already enabled")
        for ddl in TRIGGER_DDL:
            conn.execute(ddl)
        conn.execute("DELETE FROM validation_findings")
        conn.execute(
            "UPDATE validation_progress SET cursor = 0, scanned = 0,"
            " started_at = ?, finished_at = NULL WHERE id = 1",
            (utcnow(),),
        )
        conn.execute(
            "UPDATE constraint_state SET state = 'validating', updated_at = ? WHERE id = 1",
            (utcnow(),),
        )
        return {
            "served_by": SERVER_NAME,
            "state": "validating",
            "detail": "enforcement triggers installed; batched scan ready (POST /validation/step)",
        }

    return run_write("validation_start", "", work)


@app.post("/validation/step")
def validation_step(batch_size: int = Query(100, ge=1, le=10000)):
    """Scan one batch of deployments (id-ordered, resumable cursor)."""

    def work(conn):
        state = get_state(conn)
        if state != "validating":
            raise HTTPException(
                status_code=409,
                detail=f"validation not running (state={state}); POST /validation/start first",
            )
        result = scan_batch(conn, batch_size)
        return {"served_by": SERVER_NAME, **result}

    return run_write("validation_step", f"batch_size={batch_size}", work)


@app.post("/validation/finalize")
def validation_finalize():
    """Atomically: drain the unscanned tail, require zero live orphans, and
    only then mark the constraint enabled. Runs inside one write transaction,
    so no writer on either process can interleave with the decision."""

    def work(conn):
        state = get_state(conn)
        if state == "enabled":
            raise HTTPException(status_code=409, detail="constraint already enabled")
        if state != "validating":
            raise HTTPException(
                status_code=409,
                detail=f"validation not running (state={state}); POST /validation/start first",
            )
        tail = scan_batch(conn, 1_000_000)
        orphans = live_orphan_rows(conn)
        if orphans:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "cannot enable constraint: orphan deployments remain",
                    "orphan_deployment_ids": [r["id"] for r in orphans],
                },
            )
        conn.execute(
            "UPDATE constraint_state SET state = 'enabled', updated_at = ? WHERE id = 1",
            (utcnow(),),
        )
        conn.execute(
            "UPDATE validation_progress SET finished_at = ? WHERE id = 1",
            (utcnow(),),
        )
        return {
            "served_by": SERVER_NAME,
            "state": "enabled",
            "tail_scanned": tail["batch"],
            "detail": "all existing deployments valid; constraint now enforced for all writes",
        }

    return run_write("validation_finalize", "", work)


@app.get("/validation/status")
def validation_status():
    conn = connect()
    try:
        st = conn.execute("SELECT state, updated_at FROM constraint_state WHERE id = 1").fetchone()
        prog = conn.execute("SELECT * FROM validation_progress WHERE id = 1").fetchone()
        orphans = live_orphan_rows(conn)
        findings = conn.execute("SELECT COUNT(*) AS c FROM validation_findings").fetchone()["c"]
        present = {
            r["name"]
            for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'")
        }
        return {
            "served_by": SERVER_NAME,
            "state": st["state"],
            "state_updated_at": st["updated_at"],
            "scan": {
                "cursor": prog["cursor"],
                "scanned": prog["scanned"],
                "started_at": prog["started_at"],
                "finished_at": prog["finished_at"],
            },
            "live_orphans": len(orphans),
            "findings_recorded": findings,
            "enforcement_triggers_present": sorted(set(TRIGGER_NAMES) & present),
        }
    finally:
        conn.close()


@app.get("/validation/findings")
def validation_findings():
    """History of orphans found by the scan, reconciled against current data."""
    conn = connect()
    try:
        rows = conn.execute(
            """
            SELECT f.deployment_id, f.service_id, f.found_at,
                   CASE
                       WHEN d.id IS NULL THEN 'deleted'
                       WHEN EXISTS (SELECT 1 FROM services s WHERE s.id = d.service_id) THEN 'fixed'
                       ELSE 'still_orphan'
                   END AS current_status
            FROM validation_findings f
            LEFT JOIN deployments d ON d.id = f.deployment_id
            ORDER BY f.deployment_id
            """
        ).fetchall()
        return {"served_by": SERVER_NAME, "findings": [dict(r) for r in rows]}
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# lock-contention diagnostics (read side; served from the SEPARATE diagnostics
# database, so they answer even while the MAIN database is write-locked)
# --------------------------------------------------------------------------- #

def _heartbeats(conn) -> dict:
    return {r["instance"]: dict(r) for r in conn.execute("SELECT * FROM heartbeats")}


def _enrich_event(row: sqlite3.Row, heartbeats: dict, now: float) -> dict:
    ev = dict(row)
    hb = heartbeats.get(ev["instance"])
    age = None if hb is None else max(0.0, now - hb["ts"])
    ev["heartbeat_age_s"] = None if age is None else round(age, 2)
    ev["instance_live"] = None if hb is None else age <= HEARTBEAT_STALE_S
    if ev["state"] == "holding" and ev["acquired_at"] is not None:
        ev["holding_for_ms"] = round((now - ev["acquired_at"]) * 1000, 1)
    if ev["state"] == "waiting":
        ev["waiting_for_ms"] = round((now - ev["queued_at"]) * 1000, 1)
    if ev["state"] in ("waiting", "holding"):
        # Unfinished record whose owner is not heartbeating: its process is
        # gone; it will be marked 'interrupted' when that instance restarts.
        ev["suspected_interrupted"] = hb is None or age > HEARTBEAT_STALE_S
    return ev


@app.get("/locks/current", tags=["lock-diagnostics"])
def locks_current():
    """Live snapshot of the cross-instance write-lock contention: current
    holder(s) and the waiter queue. Reads only the diagnostics database, so it
    answers even while the main database is write-locked. Holders are ordered
    by actual lock acquisition time (``acquired_at``), waiters by enqueue time
    (``queued_at``); at most one LIVE holder can exist (SQLite single writer).
    """
    conn = diag_connect()
    try:
        now = time.time()
        hbs = _heartbeats(conn)
        holders = [
            _enrich_event(r, hbs, now)
            for r in conn.execute(
                "SELECT * FROM lock_events WHERE state = 'holding' ORDER BY acquired_at"
            )
        ]
        waiters = [
            _enrich_event(r, hbs, now)
            for r in conn.execute(
                "SELECT * FROM lock_events WHERE state = 'waiting' ORDER BY queued_at"
            )
        ]
        return {
            "served_by": SERVER_NAME,
            "now": now,
            "holders": holders,
            "waiters": waiters,
            "instances": {
                name: {
                    "run_id": hb["run_id"],
                    "pid": hb["pid"],
                    "last_heartbeat": hb["ts"],
                    "heartbeat_age_s": round(now - hb["ts"], 2),
                    "live": now - hb["ts"] <= HEARTBEAT_STALE_S,
                }
                for name, hb in hbs.items()
            },
        }
    finally:
        conn.close()


@app.get("/locks/events", tags=["lock-diagnostics"])
def locks_events(
    state: str = Query(None, description="filter: waiting|holding|completed|failed|timeout|interrupted"),
    instance: str = Query(None, description="filter by instance name (app1/app2)"),
    op: str = Query(None, description="filter by operation type"),
    limit: int = Query(50, ge=1, le=500),
):
    """Recent lock events, newest first. Reads only the diagnostics database."""
    clauses, params = [], []
    if state:
        clauses.append("state = ?")
        params.append(state)
    if instance:
        clauses.append("instance = ?")
        params.append(instance)
    if op:
        clauses.append("op = ?")
        params.append(op)
    sql = "SELECT * FROM lock_events"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    conn = diag_connect()
    try:
        now = time.time()
        hbs = _heartbeats(conn)
        events = [_enrich_event(r, hbs, now) for r in conn.execute(sql, params)]
        return {"served_by": SERVER_NAME, "count": len(events), "events": events}
    finally:
        conn.close()


@app.get("/locks/events/{diag_id}", tags=["lock-diagnostics"])
def locks_event(diag_id: int):
    """One lock event by its diagnostic number (returned by every write
    request as ``diag_id``): operation type, instance, wait/hold durations,
    and the completion or timeout reason."""
    conn = diag_connect()
    try:
        row = conn.execute("SELECT * FROM lock_events WHERE id = ?", (diag_id,)).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail=f"no lock event with diag_id {diag_id}")
        return _enrich_event(row, _heartbeats(conn), time.time())
    finally:
        conn.close()


# --------------------------------------------------------------------------- #
# demo / ops
# --------------------------------------------------------------------------- #

@app.post("/demo/hold-lock", tags=["demo"])
def demo_hold_lock(seconds: float = Query(5.0, gt=0, le=60)):
    """DEMO/TEST ONLY: take the main-DB write lock and hold it for ``seconds``
    (a real, content-preserving write transaction). While it runs, watch
    GET /locks/current (on either instance) to see this request as the holder
    and other writes queue up as waiters; afterwards inspect every
    participant via GET /locks/events/{diag_id}."""

    def work(conn):
        conn.execute("UPDATE validation_progress SET scanned = scanned WHERE id = 1")
        time.sleep(seconds)
        return {
            "served_by": SERVER_NAME,
            "held_seconds": seconds,
            "detail": "write lock was held; re-run and watch GET /locks/current while it runs",
        }

    return run_write("demo_hold_lock", f"seconds={seconds}", work)


@app.post("/demo/reset")
def demo_reset():
    """Demo helper: wipe all BUSINESS data, drop enforcement triggers, state
    -> off. Lock-contention diagnostics (a separate database) are kept."""

    def work(conn):
        for name in TRIGGER_NAMES:
            conn.execute(f"DROP TRIGGER IF EXISTS {name}")
        conn.execute("DELETE FROM deployments")
        conn.execute("DELETE FROM services")
        conn.execute("DELETE FROM validation_findings")
        try:
            conn.execute("DELETE FROM sqlite_sequence")
        except sqlite3.OperationalError:
            pass
        conn.execute(
            "UPDATE validation_progress SET cursor = 0, scanned = 0,"
            " started_at = NULL, finished_at = NULL WHERE id = 1"
        )
        conn.execute(
            "UPDATE constraint_state SET state = 'off', updated_at = ? WHERE id = 1",
            (utcnow(),),
        )
        return {"served_by": SERVER_NAME, "state": "off"}

    return run_write("demo_reset", "", work)


@app.get("/health")
def health():
    return {"ok": True, "served_by": SERVER_NAME, "busy_timeout_ms": BUSY_TIMEOUT_MS}


@app.get("/")
def index():
    return {
        "service": "online FK enablement demo",
        "served_by": SERVER_NAME,
        "docs": "/docs",
        "flow": [
            "POST /services, POST /deployments (orphans allowed while state=off)",
            "POST /validation/start",
            "POST /validation/step?batch_size=N (repeat)",
            "GET /orphans, PATCH /deployments/{id} to correct",
            "POST /validation/finalize",
            "GET /validation/status",
        ],
        "lock_diagnostics": [
            "every write response carries diag_id",
            "GET /locks/current - live holder + waiter queue (works while main DB is locked)",
            "GET /locks/events/{diag_id} - op, instance, wait/hold ms, completion/timeout reason",
            "POST /demo/hold-lock?seconds=N - reproduce lock contention on demand",
        ],
    }
