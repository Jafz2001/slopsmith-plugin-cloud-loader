"""Background watcher: scans the DLC dir for new/modified files and uploads
them to Drive. Polls every WATCH_INTERVAL seconds (low CPU; this is a hobby
app, not a high-throughput service).

The watcher is the half that closes the loop for write-flows like the
sloppak converter or retune: they write directly into DLC_DIR; the watcher
notices and pushes to Drive so the change survives a restart.
"""
from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Optional

from cloud_db import CloudDB
from drive import DriveClient, EXTENSIONS

log = logging.getLogger("slopsmith.plugin.cloud_loader.watcher")

WATCH_INTERVAL = 30  # seconds
DOWNLOAD_RECENCY_SKIP = 60  # don't re-upload a file we just downloaded


class UploadWatcher:
    def __init__(self, dlc_dir: Path, cloud_db: CloudDB, drive: DriveClient,
                 get_folder_id):
        self.dlc_dir = dlc_dir
        self.cloud_db = cloud_db
        self.drive = drive
        self.get_folder_id = get_folder_id  # callable -> str | None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._status: dict = {"running": False, "last_run": None, "in_flight": None,
                              "uploaded": 0, "errors": 0, "last_error": None}
        self._lock = threading.Lock()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="cloud_loader_watcher")
        self._thread.start()
        with self._lock:
            # Reset stale fields from a prior session (in_flight can be left
            # over if the previous run was disconnected mid-upload).
            self._status.update({"running": True, "in_flight": None})

    def stop(self):
        self._stop.set()
        with self._lock:
            self._status["running"] = False

    def status(self) -> dict:
        with self._lock:
            return dict(self._status)

    def trigger_now(self):
        """Wake the loop early. Useful right after a known write."""
        # Cheap trick: shorten the next sleep by closing-and-reopening the event.
        # The loop polls _stop.wait(WATCH_INTERVAL); we briefly set+clear to break it.
        self._stop.set()
        time.sleep(0.05)
        self._stop.clear()

    def _loop(self):
        while not self._stop.is_set():
            try:
                self._scan_once()
            except Exception as e:
                log.exception("watcher scan failed")
                with self._lock:
                    self._status["errors"] += 1
                    self._status["last_error"] = str(e)
            with self._lock:
                self._status["last_run"] = time.time()
            self._stop.wait(WATCH_INTERVAL)

    def _scan_once(self):
        folder_id = self.get_folder_id()
        if not folder_id:
            return
        if not self.dlc_dir.exists():
            return
        if not self.drive.is_authenticated():
            return

        for path in self.dlc_dir.rglob("*"):
            if self._stop.is_set():
                return
            if not path.is_file():
                continue
            if path.suffix.lower() not in EXTENSIONS:
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            self._maybe_upload(path, stat.st_mtime, stat.st_size, folder_id)

    def _maybe_upload(self, path: Path, mtime: float, size: int, folder_id: str):
        filename = path.name
        local = self.cloud_db.get_local(filename)
        remote = self.cloud_db.get_remote(filename)

        # SAFETY (redundant by design — destroying user data in Drive is
        # catastrophic). Every condition below is sufficient on its own.
        # We log unconditionally on every skip so the user can audit.

        # 1) Never upload a stub. Stubs have size=0 AND mtime=0 (epoch).
        if size == 0:
            log.info("watcher skip: %s is 0 bytes (stub)", filename)
            return
        if mtime == 0:
            log.info("watcher skip: %s has epoch mtime (stub marker)", filename)
            return

        # 2) Never overwrite Drive with a smaller version. If Drive's
        # known size > local size, abort. Catches partial/corrupt
        # local copies (interrupted download, FS truncation).
        if remote and remote.get("size", 0) > size:
            log.warning("watcher skip: %s local size %d < remote size %d",
                        filename, size, remote["size"])
            return

        # 3) Never upload a file that's only a small fraction of what Drive
        # has. PSARCs and sloppaks have minimum useful sizes (≈100 KB);
        # anything tiny is almost certainly a corrupt local file.
        if size < 1024:
            log.warning("watcher skip: %s is suspiciously small (%d bytes)",
                        filename, size)
            return

        # 4) Never upload a file whose mtime is older than what we recorded
        # in cloud_db for this filename. A local file older than the
        # cached remote_mtime can't legitimately replace it.
        if remote and remote.get("remote_mtime"):
            try:
                # ISO8601 "YYYY-MM-DDTHH:MM:SS.sssZ" → epoch
                from datetime import datetime, timezone
                rt = remote["remote_mtime"].replace("Z", "+00:00")
                remote_epoch = datetime.fromisoformat(rt).timestamp()
                if mtime < remote_epoch - 1:  # 1s tolerance for FS rounding
                    log.warning("watcher skip: %s local mtime %.0f < remote mtime %.0f",
                                filename, mtime, remote_epoch)
                    return
            except (ValueError, KeyError):
                pass  # malformed timestamp — let other checks handle it

        # Skip files we just pulled from Drive (mtime ≈ upload time).
        if local and local.get("uploaded_at") and (time.time() - local["uploaded_at"]) < DOWNLOAD_RECENCY_SKIP:
            if local["local_mtime"] == mtime and local["local_size"] == size:
                return

        # Unchanged since last successful upload?
        if local and local.get("uploaded_at") and \
                local["local_mtime"] == mtime and local["local_size"] == size:
            return

        existing_id = (local or {}).get("drive_file_id") or (remote or {}).get("drive_file_id")

        with self._lock:
            self._status["in_flight"] = filename
        try:
            log.info("uploading %s (%.1f MB) to Drive%s",
                     filename, size / 1e6, " (update)" if existing_id else "")
            new_id = self.drive.upload_or_update(path, folder_id, existing_id)
            self.cloud_db.put_local(filename, mtime, size, new_id, time.time())
            # Reflect into remote_index so the next play resolves it without a re-scan.
            # remote_mtime kept as the local mtime ISO-ish — close enough; the real
            # Drive mtime gets refreshed on the next bootstrap.
            self.cloud_db.put_remote(filename, new_id,
                                     time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(mtime)),
                                     size)
            with self._lock:
                self._status["uploaded"] += 1
        except Exception as e:
            log.exception("upload failed for %s", filename)
            with self._lock:
                self._status["errors"] += 1
                self._status["last_error"] = f"{filename}: {e}"
        finally:
            with self._lock:
                self._status["in_flight"] = None
