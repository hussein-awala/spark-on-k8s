from __future__ import annotations

import base64
import json
import math
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

STATUSES = {"Pending", "Running", "Succeeded", "Failed", "Unknown"}


class LostLease(RuntimeError):
    """The collecting worker no longer owns the current fenced lease."""


@dataclass(frozen=True)
class Lease:
    owner: str
    token: int


def timestamp(value):
    return (
        datetime.fromtimestamp(value, UTC).isoformat().replace("+00:00", "Z") if value is not None else None
    )


class RunStore:
    """Small SQLite metadata store. A fresh bounded connection is used for every operation."""

    def __init__(self, path: Path, *, retention_days=30, max_rows=10000, clock=time.time):
        self.path = path
        self.retention_seconds = retention_days * 86400
        self.max_rows = max_rows
        self.clock = clock

    @contextmanager
    def connection(self, *, write=False):
        connection = sqlite3.connect(str(self.path), timeout=2, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA busy_timeout=2000")
            if write:
                connection.execute("BEGIN IMMEDIATE")
            yield connection
            if write:
                connection.commit()
        except BaseException:
            if write:
                connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self):
        # The directory is provisioned by the operator, avoiding silently ephemeral storage.
        # SQLite may report BUSY immediately while another worker changes journal mode.
        # Retry only that bounded initialization step; never conceal schema or filesystem errors.
        for attempt in range(20):
            try:
                with self.connection() as db:
                    db.execute("PRAGMA busy_timeout=100")
                    db.execute("PRAGMA journal_mode=WAL")
                break
            except sqlite3.OperationalError as error:
                if (
                    error.sqlite_errorcode & 255 not in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}
                    or attempt == 19
                ):
                    raise
                time.sleep(0.01)
        with self.connection(write=True) as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version not in {0, 1}:
                raise ValueError("Unsupported run-history database schema version")
            db.execute("""CREATE TABLE IF NOT EXISTS runs (
                uid TEXT PRIMARY KEY, namespace TEXT NOT NULL, app_id TEXT, name TEXT NOT NULL,
                driver_pod TEXT NOT NULL, image TEXT, spark_version TEXT, status TEXT NOT NULL,
                created_at TEXT, first_seen REAL NOT NULL, last_seen REAL NOT NULL, disappeared_at REAL
            )""")
            db.execute("CREATE INDEX IF NOT EXISTS runs_listing ON runs(first_seen DESC, uid DESC)")
            db.execute("CREATE INDEX IF NOT EXISTS runs_namespace ON runs(namespace)")
            db.execute("""CREATE TABLE IF NOT EXISTS collection (
                namespace TEXT PRIMARY KEY, last_attempt REAL, last_success REAL,
                successes INTEGER NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS collector_lease (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), owner TEXT NOT NULL,
                token INTEGER NOT NULL, expires REAL NOT NULL
            )""")
            db.execute("INSERT OR IGNORE INTO collector_lease VALUES(1, '', 0, 0)")
            db.execute("PRAGMA user_version=1")

    def acquire(self, owner: str, seconds: float) -> Lease | None:
        with self.connection(write=True) as db:
            row = db.execute("SELECT * FROM collector_lease WHERE singleton=1").fetchone()
            now = self.clock()
            if row["owner"] != owner and row["expires"] > now:
                return None
            token = row["token"] + 1
            db.execute(
                "UPDATE collector_lease SET owner=?,token=?,expires=? WHERE singleton=1",
                (owner, token, now + seconds),
            )
            return Lease(owner, token)

    def release(self, lease: Lease):
        with self.connection(write=True) as db:
            db.execute(
                "UPDATE collector_lease SET owner='',expires=0 WHERE singleton=1 AND owner=? AND token=?",
                (lease.owner, lease.token),
            )

    def _fence(self, db, lease):
        row = db.execute("SELECT owner,token,expires FROM collector_lease WHERE singleton=1").fetchone()
        if row["owner"] != lease.owner or row["token"] != lease.token or row["expires"] <= self.clock():
            raise LostLease("The run-history collector lease expired or changed")

    def snapshot(self, namespace: str, records: list[dict], lease: Lease):
        """Commit a complete namespaced snapshot, or roll it back in its entirety."""
        with self.connection(write=True) as db:
            self._fence(db, lease)
            now = self.clock()
            db.execute("CREATE TEMP TABLE seen(uid TEXT PRIMARY KEY)")
            for record in records:
                if record["namespace"] != namespace or record["status"] not in STATUSES:
                    raise ValueError("Invalid run metadata")
                db.execute("INSERT INTO seen VALUES(?)", (record["uid"],))
                db.execute(
                    """INSERT INTO runs VALUES(?,?,?,?,?,?,?,?,?,?,?,NULL)
                    ON CONFLICT(uid) DO UPDATE SET
                    app_id=excluded.app_id,name=excluded.name,driver_pod=excluded.driver_pod,
                    image=excluded.image,spark_version=excluded.spark_version,
                    status=CASE WHEN runs.status IN ('Succeeded','Failed')
                    THEN runs.status ELSE excluded.status END,
                    last_seen=excluded.last_seen,disappeared_at=NULL""",
                    tuple(
                        record[key]
                        for key in (
                            "uid",
                            "namespace",
                            "app_id",
                            "name",
                            "driver_pod",
                            "image",
                            "spark_version",
                            "status",
                            "created_at",
                        )
                    )
                    + (now, now),
                )
            db.execute(
                """UPDATE runs SET disappeared_at=? WHERE namespace=? AND disappeared_at IS NULL
                        AND uid NOT IN (SELECT uid FROM seen)""",
                (now, namespace),
            )
            db.execute(
                """INSERT INTO collection(namespace,last_attempt,last_success,successes)
                        VALUES(?,?,?,1) ON CONFLICT(namespace)
                        DO UPDATE SET last_attempt=excluded.last_attempt,
                        last_success=excluded.last_success,successes=collection.successes+1""",
                (namespace, now, now),
            )
            self._prune(db, now)
            # The transaction lock prevents takeover during the write; this second check also fences expiry.
            self._fence(db, lease)

    def _prune(self, db, now):
        db.execute("DELETE FROM runs WHERE last_seen < ?", (now - self.retention_seconds,))
        db.execute(
            """DELETE FROM runs WHERE uid IN (
            SELECT uid FROM runs ORDER BY last_seen DESC,first_seen DESC,uid DESC LIMIT -1 OFFSET ?
        )""",
            (self.max_rows,),
        )

    def failure(self, namespace: str, lease: Lease):
        with self.connection(write=True) as db:
            self._fence(db, lease)
            db.execute(
                """INSERT INTO collection(namespace,last_attempt,failures) VALUES(?,?,1)
                ON CONFLICT(namespace) DO UPDATE SET last_attempt=excluded.last_attempt,
                failures=collection.failures+1""",
                (namespace, self.clock()),
            )
            self._prune(db, self.clock())
            self._fence(db, lease)

    def list_runs(
        self, *, namespace=None, status=None, presence="all", limit=50, cursor=None, allowed_namespaces=None
    ):
        if (
            status is not None
            and status not in STATUSES
            or presence not in {"all", "present", "missing"}
            or not 1 <= limit <= 200
        ):
            raise ValueError("Invalid run-list filter")
        filters = [namespace, status, presence, list(allowed_namespaces) if allowed_namespaces else None]
        conditions, params = [], []
        if allowed_namespaces:
            conditions.append("namespace IN (" + ",".join("?" for _ in allowed_namespaces) + ")")
            params.extend(allowed_namespaces)
        if namespace is not None:
            conditions.append("namespace=?")
            params.append(namespace)
        if status is not None:
            conditions.append("status=?")
            params.append(status)
        if presence != "all":
            conditions.append("disappeared_at IS " + ("NULL" if presence == "present" else "NOT NULL"))
        if cursor:
            try:
                data = json.loads(base64.b64decode(cursor, altchars=b"-_", validate=True))
                first, uid = data["first"], data["uid"]
                if (
                    data["filters"] != filters
                    or type(first) not in {int, float}
                    or not math.isfinite(first)
                    or not isinstance(uid, str)
                ):
                    raise ValueError
                # first_seen is REAL; also bounds integers that SQLite cannot bind.
                first = float(first)
            except (ValueError, KeyError, TypeError, OverflowError) as error:
                raise ValueError("Invalid run-list cursor") from error
            conditions.append("(first_seen < ? OR (first_seen = ? AND uid < ?))")
            params.extend((first, first, uid))
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        with self.connection() as db:
            rows = db.execute(
                "SELECT * FROM runs" + where + " ORDER BY first_seen DESC,uid DESC LIMIT ?",
                (*params, limit + 1),
            ).fetchall()
        next_cursor = None
        if len(rows) > limit:
            last = rows[limit - 1]
            next_cursor = base64.urlsafe_b64encode(
                json.dumps(
                    {"first": last["first_seen"], "uid": last["uid"], "filters": filters},
                    separators=(",", ":"),
                ).encode()
            ).decode()
        items = []
        for row in rows[:limit]:
            item = dict(row)
            for field in ("first_seen", "last_seen", "disappeared_at"):
                item[field] = timestamp(item[field])
            items.append(item)
        return {"schema_version": 1, "items": items, "next_cursor": next_cursor}

    def health(self):
        with self.connection() as db:
            return {row["namespace"]: dict(row) for row in db.execute("SELECT * FROM collection")}

    def counts(self):
        with self.connection() as db:
            return [
                dict(row)
                for row in db.execute(
                    "SELECT namespace,status,COUNT(*) AS count FROM runs "
                    "GROUP BY namespace,status ORDER BY namespace,status"
                )
            ]
