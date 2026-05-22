"""SQLite store for the cloud_loader plugin.

Two tables:
  remote_index — what's known to exist in Drive (filename → drive_file_id, mtime, size)
  local_state  — what's currently sitting in cloud-dlc/ and its upload status

We key on filename (not drive_file_id) because slopsmith's meta_db keys
on filename — keeping them aligned keeps the play-intercept simple.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Optional


class CloudDB:
    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._migrate()

    def _migrate(self):
        with self._lock:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS remote_index (
                    filename TEXT PRIMARY KEY,
                    drive_file_id TEXT NOT NULL,
                    remote_mtime TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    indexed_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS local_state (
                    filename TEXT PRIMARY KEY,
                    local_mtime REAL NOT NULL,
                    local_size INTEGER NOT NULL,
                    drive_file_id TEXT,
                    uploaded_at REAL,
                    last_used_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_local_last_used
                    ON local_state(last_used_at);
            """)
            # meta_json (added later): cached extract_meta() result, so we can
            # repopulate slopsmith's meta_db without re-downloading the
            # archive — important when the server's background_scan wipes
            # meta_db after finding the DLC dir empty between restarts.
            try:
                self.conn.execute("ALTER TABLE remote_index ADD COLUMN meta_json TEXT")
            except sqlite3.OperationalError:
                pass
            self.conn.commit()

    # ---- remote_index ----

    def put_remote(self, filename: str, drive_file_id: str, remote_mtime: str,
                   size: int, meta_json: Optional[str] = None):
        with self._lock:
            self.conn.execute(
                "INSERT INTO remote_index (filename, drive_file_id, remote_mtime, size, indexed_at, meta_json) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(filename) DO UPDATE SET "
                "drive_file_id=excluded.drive_file_id, remote_mtime=excluded.remote_mtime, "
                "size=excluded.size, indexed_at=excluded.indexed_at, "
                # Don't clobber existing meta_json with NULL on idempotent puts
                # (e.g. the watcher re-pinning after upload) — only overwrite
                # when the caller provided a fresh value.
                "meta_json=COALESCE(excluded.meta_json, remote_index.meta_json)",
                (filename, drive_file_id, remote_mtime, size, time.time(), meta_json),
            )
            self.conn.commit()

    def get_remote(self, filename: str) -> Optional[dict]:
        with self._lock:
            row = self.conn.execute(
                "SELECT filename, drive_file_id, remote_mtime, size, meta_json "
                "FROM remote_index WHERE filename = ?",
                (filename,),
            ).fetchone()
        if not row:
            return None
        return {"filename": row[0], "drive_file_id": row[1], "remote_mtime": row[2],
                "size": row[3], "meta_json": row[4]}

    def all_remote(self) -> list[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT filename, drive_file_id, remote_mtime, size, meta_json FROM remote_index"
            ).fetchall()
        return [
            {"filename": r[0], "drive_file_id": r[1], "remote_mtime": r[2],
             "size": r[3], "meta_json": r[4]}
            for r in rows
        ]

    def delete_remote(self, filename: str):
        with self._lock:
            self.conn.execute("DELETE FROM remote_index WHERE filename = ?", (filename,))
            self.conn.commit()

    def remote_count(self) -> int:
        with self._lock:
            return self.conn.execute("SELECT COUNT(*) FROM remote_index").fetchone()[0]

    # ---- local_state ----

    def put_local(self, filename: str, local_mtime: float, local_size: int,
                  drive_file_id: Optional[str], uploaded_at: Optional[float]):
        with self._lock:
            self.conn.execute(
                "INSERT OR REPLACE INTO local_state VALUES (?, ?, ?, ?, ?, ?)",
                (filename, local_mtime, local_size, drive_file_id, uploaded_at, time.time()),
            )
            self.conn.commit()

    def get_local(self, filename: str) -> Optional[dict]:
        with self._lock:
            row = self.conn.execute(
                "SELECT filename, local_mtime, local_size, drive_file_id, uploaded_at, last_used_at "
                "FROM local_state WHERE filename = ?",
                (filename,),
            ).fetchone()
        if not row:
            return None
        return {
            "filename": row[0], "local_mtime": row[1], "local_size": row[2],
            "drive_file_id": row[3], "uploaded_at": row[4], "last_used_at": row[5],
        }

    def all_local(self) -> list[dict]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT filename, local_mtime, local_size, drive_file_id, uploaded_at, last_used_at "
                "FROM local_state"
            ).fetchall()
        return [
            {"filename": r[0], "local_mtime": r[1], "local_size": r[2],
             "drive_file_id": r[3], "uploaded_at": r[4], "last_used_at": r[5]}
            for r in rows
        ]

    def delete_local(self, filename: str):
        with self._lock:
            self.conn.execute("DELETE FROM local_state WHERE filename = ?", (filename,))
            self.conn.commit()

    def touch_local(self, filename: str):
        with self._lock:
            self.conn.execute(
                "UPDATE local_state SET last_used_at = ? WHERE filename = ?",
                (time.time(), filename),
            )
            self.conn.commit()
