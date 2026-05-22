"""Plugin-registered FastAPI routes for cloud_loader.

The plugin populates slopsmith's meta_db at bootstrap time by downloading
each remote song once, extracting metadata, and discarding the binary —
so the library browser shows the full catalog without anything sitting on
disk. At play time, the frontend script (screen.js) wraps window.playSong:
it asks /api/cloud_loader/needs_prefetch first, calls /prefetch if so,
polls /prefetch/status until the file is on disk, then lets the original
playSong continue. This means the server's WebSocket play handler runs
against a real local file and needs zero patching.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import os
import threading
import time
from pathlib import Path
from typing import Optional

# How many archives to download/extract in parallel during bootstrap. 4 is a
# sweet spot for Drive: bandwidth is shared, but the per-request TTFB (~200ms)
# amortizes well across requests, and 4 worker threads × ~200 MB peak ~= 800 MB
# of memory pressure during extract — fine on a 16 GB Mac. Drive's quota
# (1000 req/100s/user) easily covers 4 × ~10 chunk reqs/file = 40/sec, well
# under the limit.
PARALLEL_DOWNLOADS = 4

from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

PLUGIN_ID = "cloud_loader"
log = logging.getLogger(f"slopsmith.plugin.{PLUGIN_ID}")


def setup(app: FastAPI, context: dict) -> None:
    config_dir = Path(context["config_dir"])
    plugin_state_dir = config_dir / PLUGIN_ID
    plugin_state_dir.mkdir(parents=True, exist_ok=True)

    # Always-on file logger so post-mortem debugging works even in packaged
    # builds (which don't set LOG_FILE). One file, rotated only by user.
    plugin_log_file = plugin_state_dir / "plugin.log"
    if not any(isinstance(h, logging.FileHandler)
               and getattr(h, "baseFilename", "") == str(plugin_log_file)
               for h in log.handlers):
        fh = logging.FileHandler(str(plugin_log_file), encoding="utf-8")
        fh.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] %(message)s"
        ))
        log.addHandler(fh)
        log.setLevel(logging.INFO)

    # Lazy-import plugin siblings so requirements.txt has been installed
    # by the time we touch google_auth_*. The plugin loader has already
    # done the pip install before importing this module, but the sibling
    # files live in the same dir so just-by-name imports work.
    load_sibling = context["load_sibling"]
    cloud_db_mod = load_sibling("cloud_db")
    drive_mod = load_sibling("drive")
    watcher_mod = load_sibling("watcher")

    db_path = plugin_state_dir / "index.db"
    config_path = plugin_state_dir / "config.json"
    client_secret_path = plugin_state_dir / "client_secret.json"
    token_path = plugin_state_dir / "token.json"

    cdb = cloud_db_mod.CloudDB(db_path)
    drive = drive_mod.DriveClient(client_secret_path, token_path)

    # Capture meta_db + extract_meta at setup time so nested helpers
    # (_fetch_meta_dict_for, _gc_stale_locals, _bootstrap_worker) close over
    # them. They used to be bound inside _bootstrap_worker_impl only — which
    # meant calling _gc_stale_locals from the prefetch worker raised
    # NameError("meta_db is not defined") on every first Play.
    meta_db = context["meta_db"]
    extract_meta = context["extract_meta"]

    # Album art cache. Matches the server's ART_CACHE_DIR (server.py:235).
    # The server's /api/song/{X}/art endpoint checks this cache first for
    # PSARCs — if a PNG exists here with the right name, it's served without
    # any disk read of the archive. We pre-populate during bootstrap so the
    # library browser shows thumbnails even though the files on disk are 0-byte
    # stubs. (sloppaks aren't cached here — the server reads cover.jpg from
    # their unpacked source dir directly, which requires a real zip.)
    art_cache_dir = config_dir / "art_cache"
    art_cache_dir.mkdir(parents=True, exist_ok=True)

    def _load_config() -> dict:
        if not config_path.exists():
            return {}
        try:
            return json.loads(config_path.read_text())
        except Exception:
            return {}

    def _save_config(cfg: dict):
        config_path.write_text(json.dumps(cfg, indent=2))

    def _get_folder_id() -> Optional[str]:
        return _load_config().get("root_folder_id")

    def _get_cloud_dlc_dir() -> Optional[Path]:
        cfg = _load_config()
        d = cfg.get("dlc_dir")
        if not d:
            d = context["get_dlc_dir"]()
            return Path(d).expanduser() if d else None
        # expanduser so a config value like "~/Music/Slopsmith" doesn't get
        # written as a literal "~" subdir under CWD (bundle Resources at
        # runtime).
        return Path(d).expanduser()

    def _fetch_meta_dict_for(filename: str) -> Optional[dict]:
        """Read a meta_db row by filename, ignoring the mtime/size guard.

        Public meta_db.get(filename, mtime, size) returns None unless both
        keys match — we need the raw dict so we can re-pin a row to the
        sentinel (0,0) keys after the server's scan accidentally wrote
        real mtime/size in. Reads conn directly under the meta_db lock.
        """
        with meta_db._lock:
            row = meta_db.conn.execute(
                "SELECT title, artist, album, year, duration, tuning, arrangements, "
                "has_lyrics, format, stem_count, stem_ids, tuning_name, tuning_sort_key "
                "FROM songs WHERE filename = ?", (filename,)
            ).fetchone()
        if not row or not row[0]:
            return None
        return {
            "title": row[0], "artist": row[1], "album": row[2],
            "year": row[3], "duration": row[4], "tuning": row[5],
            "arrangements": json.loads(row[6]) if row[6] else [],
            "has_lyrics": bool(row[7]),
            "format": row[8] or "psarc",
            "stem_count": int(row[9] or 0),
            "stem_ids": json.loads(row[10]) if row[10] else [],
            "tuning_name": row[11] or "",
            "tuning_sort_key": int(row[12] or 0),
        }

    def _cache_psarc_art(filename: str, psarc_path: Path):
        """Extract album art from a PSARC into the server's art_cache_dir.

        Mirrors the extraction the server does on-demand in /api/song/<X>/art —
        unpack PSARC → find largest .dds → PIL convert to PNG → save with the
        same `safe_name` key the server uses. After this runs, art is served
        from cache even though the PSARC on disk is a 0-byte stub.
        """
        if not filename.lower().endswith(".psarc"):
            return  # sloppak/loose handled by the server reading source dir directly
        safe_name = filename.replace("/", "_").replace(" ", "_")
        cached = art_cache_dir / f"{safe_name}.png"
        if cached.exists():
            return
        import tempfile
        import shutil
        from psarc import unpack_psarc
        from PIL import Image
        tmp = tempfile.mkdtemp(prefix="cl_art_")
        try:
            unpack_psarc(str(psarc_path), tmp)
            dds_files = sorted(Path(tmp).rglob("*.dds"),
                               key=lambda p: p.stat().st_size, reverse=True)
            if dds_files:
                Image.open(dds_files[0]).convert("RGB").save(str(cached), "PNG")
        except Exception as e:
            log.warning("art cache failed for %s: %s", filename, e)
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def _stub_file(path: Path):
        """Truncate to 0 bytes and stamp mtime=0 (epoch).

        slopsmith's background_scan runs every 5 minutes (server.py:1617).
        If DLC_DIR were empty, it would call meta_db.delete_missing({}) and
        wipe everything the plugin indexed. So we keep a 0-byte stub per
        cloud song: the scanner lists it, meta_db.get(filename, 0, 0)
        returns a cache hit (we put the metadata with the same sentinel
        keys), so neither delete nor re-scan fires. ~4 KB of FS metadata
        per stub — for 1350 songs, ~5 MB total. Negligible.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("wb"):
            pass
        try:
            os.utime(path, (0, 0))
        except OSError:
            pass

    # Bootstrap state (one global; only one scan at a time).
    bootstrap_state = {
        "running": False, "total": 0, "done": 0, "skipped": 0,
        "current": None, "error": None, "started_at": None, "finished_at": None,
    }
    bootstrap_lock = threading.Lock()

    # Per-filename prefetch state.
    prefetch_state: dict[str, dict] = {}
    prefetch_lock = threading.Lock()

    # Watcher (started on demand once auth + folder + dlc_dir are configured).
    watcher = watcher_mod.UploadWatcher(
        dlc_dir=Path("/dev/null"),  # replaced below before start
        cloud_db=cdb,
        drive=drive,
        get_folder_id=_get_folder_id,
    )

    def _maybe_start_watcher():
        # The watcher uploads local DLC changes to Drive. It's OFF by default
        # because a misbehaving watcher with a Drive-write scope can destroy
        # data — we want the user to opt in explicitly after they understand
        # the trade-off (auto-upload of converter output vs. risk of
        # overwriting Drive when something goes wrong). Enable via the UI:
        # Settings → Cloud Library → "Enable auto-upload watcher".
        cfg = _load_config()
        if not cfg.get("watcher_enabled", False):
            return
        dlc = _get_cloud_dlc_dir()
        if not dlc:
            return
        watcher.dlc_dir = dlc
        if drive.is_authenticated() and _get_folder_id():
            watcher.start()

    # ---- bootstrap ----

    def _bootstrap_worker():
        try:
            _bootstrap_worker_impl()
        except Exception as e:
            log.exception("BOOTSTRAP CRASHED — uncaught exception")
            with bootstrap_lock:
                bootstrap_state.update({
                    "running": False,
                    "error": f"crashed: {e!r}",
                    "finished_at": time.time(),
                })

    def _bootstrap_worker_impl():
        folder_id = _get_folder_id()
        dlc = _get_cloud_dlc_dir()
        if not folder_id or not dlc:
            with bootstrap_lock:
                bootstrap_state.update({
                    "running": False,
                    "error": "folder_id and dlc_dir must both be set",
                    "finished_at": time.time(),
                })
            return
        dlc.mkdir(parents=True, exist_ok=True)

        scratch = plugin_state_dir / "scratch"
        scratch.mkdir(parents=True, exist_ok=True)

        cfg = _load_config()
        drive_id = cfg.get("drive_id")
        log.info("bootstrap: listing Drive folder %s (driveId=%s) ...",
                 folder_id, drive_id)
        try:
            files = list(drive.iter_song_files(folder_id, recursive=True,
                                                drive_id=drive_id))
        except Exception as e:
            log.exception("listing Drive folder failed")
            with bootstrap_lock:
                bootstrap_state.update({"running": False, "error": str(e),
                                        "finished_at": time.time()})
            return
        log.info("bootstrap: %d files to process", len(files))

        # Prune entries that vanished from Drive since the last scan: drop
        # from the plugin's remote_index AND the on-disk stub (so the next
        # server background_scan's delete_missing() naturally cleans the
        # meta_db row — no need to reach into meta_db's internals).
        current_names = {f["name"] for f in files}
        for stale in cdb.all_remote():
            if stale["filename"] not in current_names:
                cdb.delete_remote(stale["filename"])
                stub = dlc / stale["filename"]
                if stub.exists():
                    try:
                        stub.unlink()
                    except OSError:
                        pass
                log.info("pruned %s (no longer in Drive)", stale["filename"])

        with bootstrap_lock:
            bootstrap_state.update({"total": len(files), "done": 0, "skipped": 0})

        def _worker(entry):
            filename = entry["name"]
            with bootstrap_lock:
                bootstrap_state["current"] = filename  # last-started wins; informational only
            was_skip = False
            try:
                was_skip = _process_one(entry, dlc, scratch, extract_meta, meta_db)
            except Exception:
                log.exception("processing %s failed", filename)
            with bootstrap_lock:
                bootstrap_state["done"] += 1
                if was_skip:
                    bootstrap_state["skipped"] += 1
            return None

        # The `with` block waits for ALL submitted tasks to complete before
        # leaving — no need to manually as_completed; we don't care about
        # individual results, only the cumulative counter.
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=PARALLEL_DOWNLOADS,
                thread_name_prefix="cl_dl") as pool:
            for entry in files:
                pool.submit(_worker, entry)

        with bootstrap_lock:
            bootstrap_state.update({"running": False, "finished_at": time.time(),
                                    "current": None})
        log.info("bootstrap: complete (done=%d skipped=%d)",
                 bootstrap_state["done"], bootstrap_state["skipped"])
        _maybe_start_watcher()

    def _process_one(entry, dlc, scratch, extract_meta, meta_db) -> bool:
        """Process one Drive entry. Returns True if skipped (no download)."""
        filename = entry["name"]
        existing_remote = cdb.get_remote(filename)
        stub_path = dlc / filename
        remote_unchanged = (existing_remote
                            and existing_remote["remote_mtime"] == entry["modifiedTime"]
                            and existing_remote["size"] == entry["size"])

        if remote_unchanged:
            sentinel_meta = meta_db.get(filename, 0.0, 0)
            # For PSARCs, the album art lives in art_cache_dir alongside meta.
            # If it's missing, the library browser shows the thumbnail blank —
            # fall through to the download path so this scan refreshes it.
            safe_name = filename.replace("/", "_").replace(" ", "_")
            art_present = (
                not filename.lower().endswith(".psarc")
                or (art_cache_dir / f"{safe_name}.png").exists()
            )
            if sentinel_meta is not None and stub_path.exists() and art_present:
                return True  # nothing to do

            meta_json_raw = existing_remote.get("meta_json")
            cached_meta = None
            if meta_json_raw:
                try:
                    cached_meta = json.loads(meta_json_raw)
                except json.JSONDecodeError:
                    log.warning("meta_json corrupted for %s — will re-extract", filename)
            meta_dict = sentinel_meta or _fetch_meta_dict_for(filename) or cached_meta
            if meta_dict is not None:
                meta_db.put(filename, 0.0, 0, meta_dict)
                if not stub_path.exists():
                    _stub_file(stub_path)
                # Backfill meta_json on cloud_db entries that predate it.
                if not meta_json_raw:
                    cdb.put_remote(filename, entry["id"], entry["modifiedTime"],
                                   entry["size"], json.dumps(meta_dict))
                return True
            # No metadata anywhere — fall through to full download below.

        log.info("downloading %s (%.1f MB) ...",
                 filename, (entry.get("size") or 0) / 1e6)
        scratch_file = scratch / filename
        try:
            drive.download_to(entry["id"], scratch_file)
            meta = extract_meta(scratch_file)
            meta_db.put(filename, 0.0, 0, meta)
            cdb.put_remote(filename, entry["id"], entry["modifiedTime"],
                           entry["size"], json.dumps(meta))
            # Cache album art BEFORE truncating to a stub — the server reads
            # the cached PNG from disk forever after, even though the PSARC
            # itself is empty.
            _cache_psarc_art(filename, scratch_file)
            _stub_file(stub_path)
            log.info("indexed %s", filename)
        finally:
            for f in (scratch_file,
                      scratch_file.with_suffix(scratch_file.suffix + ".part")):
                if f.exists():
                    try:
                        f.unlink()
                    except OSError:
                        pass
        return False

    def _start_bootstrap():
        with bootstrap_lock:
            if bootstrap_state["running"]:
                return False
            bootstrap_state.update({
                "running": True, "error": None,
                "started_at": time.time(), "finished_at": None,
                "total": 0, "done": 0, "skipped": 0, "current": None,
            })
        threading.Thread(target=_bootstrap_worker, daemon=True,
                         name="cloud_loader_bootstrap").start()
        return True

    # ---- prefetch (lazy download at play time) ----

    def _prefetch_worker(filename: str):
        dlc = _get_cloud_dlc_dir()
        if not dlc:
            _set_prefetch(filename, state="error", error="dlc_dir not configured")
            return
        remote = cdb.get_remote(filename)
        if not remote:
            _set_prefetch(filename, state="error", error="not in cloud index")
            return
        dest = dlc / filename
        try:
            def cb(frac):
                _set_prefetch(filename, progress=frac)
            drive.download_to(remote["drive_file_id"], dest, progress_cb=cb)
            stat = dest.stat()
            cdb.put_local(filename, stat.st_mtime, stat.st_size,
                          remote["drive_file_id"], time.time())
            _set_prefetch(filename, state="ready", progress=1.0)
            # GC: drop previously-downloaded files that aren't this one.
            _gc_stale_locals(keep=filename)
        except Exception as e:
            log.exception("prefetch failed for %s", filename)
            _set_prefetch(filename, state="error", error=str(e))

    def _set_prefetch(filename: str, **kwargs):
        with prefetch_lock:
            cur = prefetch_state.setdefault(filename, {
                "state": "running", "progress": 0.0, "error": None, "started_at": time.time(),
            })
            cur.update(kwargs)

    def _gc_stale_locals(keep: Optional[str] = None):
        """Reduce other local copies back to 0-byte stubs.

        Honors the user's 'borrar al terminar' policy without actually
        deleting the FS entry — the stub keeps the meta_db row alive
        across the server's periodic background_scan. We never stub a
        file that the watcher hasn't yet uploaded (uploaded_at is None) —
        that would lose work.
        """
        dlc = _get_cloud_dlc_dir()
        if not dlc:
            return
        for entry in cdb.all_local():
            fn = entry["filename"]
            if fn == keep:
                continue
            if entry.get("uploaded_at") is None:
                continue  # not yet on Drive; don't risk it
            p = dlc / fn
            if p.exists() and p.stat().st_size > 0:
                try:
                    _stub_file(p)
                    log.info("GC: stubbed local copy of %s", fn)
                except OSError:
                    pass
            cdb.delete_local(fn)
            # Re-pin the meta_db row to (0,0) so the next background_scan
            # cache-hits cleanly instead of warning on a failed extract.
            existing = meta_db.get(fn, 0.0, 0)
            if existing is None:
                # Cached entry has different (mtime,size) — the recent play
                # caused the scanner to overwrite the sentinel keys with
                # real ones. Re-extract from the meta we already have isn't
                # possible cheaply; let the scanner log one failed extract
                # next cycle. Not fatal — meta_db row stays, browser keeps
                # showing the song.
                pass

    def _needs_prefetch(filename: str) -> bool:
        dlc = _get_cloud_dlc_dir()
        if not dlc:
            return False
        p = dlc / filename
        # Real file present (not the 0-byte stub) → already on disk.
        if p.exists() and p.stat().st_size > 0:
            return False
        return cdb.get_remote(filename) is not None

    # ---- endpoints ----

    @app.get(f"/api/{PLUGIN_ID}/status")
    def status():
        cfg = _load_config()
        with bootstrap_lock:
            boot = dict(bootstrap_state)
        return {
            "authenticated": drive.is_authenticated(),
            "auth_status": drive.auth_status(),
            "has_client_secret": client_secret_path.exists(),
            "root_folder_id": cfg.get("root_folder_id"),
            "root_folder_name": cfg.get("root_folder_name"),
            "dlc_dir": str(_get_cloud_dlc_dir()) if _get_cloud_dlc_dir() else None,
            "remote_count": cdb.remote_count(),
            "bootstrap": boot,
            "watcher": watcher.status(),
        }

    @app.post(f"/api/{PLUGIN_ID}/client-secret")
    async def upload_client_secret(file: UploadFile = File(...)):
        body = await file.read()
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            raise HTTPException(400, "invalid JSON")
        if "installed" not in parsed and "web" not in parsed:
            raise HTTPException(400, "not a Google OAuth client_secret JSON")
        client_secret_path.write_text(json.dumps(parsed))
        return {"ok": True}

    @app.post(f"/api/{PLUGIN_ID}/auth/start")
    def auth_start():
        drive.start_auth_flow()
        return drive.auth_status()

    @app.get(f"/api/{PLUGIN_ID}/auth/poll")
    def auth_poll():
        st = drive.auth_status()
        if st["state"] == "ok":
            _maybe_start_watcher()
        return st

    @app.post(f"/api/{PLUGIN_ID}/disconnect")
    def disconnect():
        watcher.stop()
        drive.disconnect()
        return {"ok": True}

    @app.get(f"/api/{PLUGIN_ID}/folders")
    def list_folders(parent: str = "root"):
        try:
            return {"folders": drive.list_folders(parent)}
        except Exception as e:
            raise HTTPException(500, str(e))

    @app.get(f"/api/{PLUGIN_ID}/shared-drives")
    def list_shared_drives():
        try:
            return {"drives": drive.list_shared_drives()}
        except Exception as e:
            raise HTTPException(500, str(e))

    @app.get(f"/api/{PLUGIN_ID}/shared-with-me")
    def list_shared_with_me():
        try:
            return {"folders": drive.list_shared_with_me()}
        except Exception as e:
            raise HTTPException(500, str(e))

    @app.post(f"/api/{PLUGIN_ID}/config")
    async def save_config(req: Request):
        body = await req.json()
        cfg = _load_config()
        for k in ("root_folder_id", "root_folder_name", "dlc_dir"):
            if k in body:
                cfg[k] = body[k]
        # When the user picks a new root, auto-detect whether it's inside a
        # Shared Drive. The drive_id we cache here gets passed to every
        # iter_song_files / list_folders call — without it, queries against
        # Shared Drives silently return nothing.
        if "root_folder_id" in body and drive.is_authenticated():
            try:
                cfg["drive_id"] = drive.get_drive_id_for(body["root_folder_id"])
            except Exception as e:
                log.warning("failed to resolve drive_id for %s: %s",
                            body["root_folder_id"], e)
                cfg["drive_id"] = None
        _save_config(cfg)
        _maybe_start_watcher()
        return {"ok": True, "config": cfg}

    @app.post(f"/api/{PLUGIN_ID}/scan")
    def trigger_scan():
        started = _start_bootstrap()
        return {"started": started, "state": bootstrap_state}

    @app.get(f"/api/{PLUGIN_ID}/scan/status")
    def scan_status():
        with bootstrap_lock:
            return dict(bootstrap_state)

    @app.get(f"/api/{PLUGIN_ID}/needs_prefetch")
    def needs_prefetch(filename: str):
        return {"needs_prefetch": _needs_prefetch(filename)}

    @app.post(f"/api/{PLUGIN_ID}/prefetch")
    def trigger_prefetch(filename: str):
        with prefetch_lock:
            existing = prefetch_state.get(filename)
            if existing and existing["state"] == "running":
                return {"started": False, "state": existing}
            prefetch_state[filename] = {
                "state": "running", "progress": 0.0, "error": None,
                "started_at": time.time(),
            }
        threading.Thread(
            target=_prefetch_worker, args=(filename,), daemon=True,
            name=f"cloud_loader_prefetch_{filename[:32]}",
        ).start()
        return {"started": True, "state": prefetch_state[filename]}

    @app.get(f"/api/{PLUGIN_ID}/prefetch/status")
    def prefetch_status(filename: str):
        with prefetch_lock:
            return prefetch_state.get(filename, {"state": "unknown"})

    @app.post(f"/api/{PLUGIN_ID}/materialize")
    def materialize(filename: str):
        """Download the real file into DLC and leave it there (no stub).

        Used when an external plugin (RS1 Extractor, Base Game Extractor) needs
        the actual PSARC on disk to read its contents. After the extractor runs,
        the user is expected to either:
          - delete the original from Drive (since it's now decomposed into
            individual song PSARCs that the watcher will upload), OR
          - call DELETE /local?filename=X to drop the local copy and let the
            next Play re-download it.
        """
        dlc = _get_cloud_dlc_dir()
        if not dlc:
            raise HTTPException(400, "dlc_dir not configured")
        remote = cdb.get_remote(filename)
        if not remote:
            raise HTTPException(404, "not in cloud index")
        dest = dlc / filename
        try:
            drive.download_to(remote["drive_file_id"], dest)
        except Exception as e:
            raise HTTPException(500, f"download failed: {e}")
        stat = dest.stat()
        # NOTE: we deliberately do NOT update cloud_db.local_state with
        # uploaded_at — that would tell the watcher this file is "already
        # synced". This file is materialized for external use; the watcher's
        # safety checks (size > 1024, mtime != 0, etc.) keep it safe.
        return {
            "ok": True,
            "path": str(dest),
            "size_mb": round(stat.st_size / 1e6, 1),
        }

    @app.delete(f"/api/{PLUGIN_ID}/local")
    def drop_local(filename: str):
        """Trade a real local file for a 0-byte stub. Reverses materialize()."""
        dlc = _get_cloud_dlc_dir()
        if not dlc:
            raise HTTPException(400, "dlc_dir not configured")
        target = dlc / filename
        if not target.exists():
            raise HTTPException(404, "file not present locally")
        _stub_file(target)
        return {"ok": True, "stubbed": str(target)}

    @app.post(f"/api/{PLUGIN_ID}/watcher/start")
    def watcher_start():
        """Enable + start the auto-upload watcher (persists across restarts)."""
        cfg = _load_config()
        cfg["watcher_enabled"] = True
        _save_config(cfg)
        _maybe_start_watcher()
        return {"running": watcher.status().get("running", False)}

    @app.post(f"/api/{PLUGIN_ID}/watcher/stop")
    def watcher_stop():
        """Disable + stop the auto-upload watcher (persists across restarts)."""
        cfg = _load_config()
        cfg["watcher_enabled"] = False
        _save_config(cfg)
        watcher.stop()
        return {"running": False}

    # Sweep orphaned scratch files from a previous crashed scan. They're
    # always empty (.part) or replaceable (.psarc that wasn't cleaned up
    # in a finally block) — bootstrap will re-fetch what it needs.
    scratch_dir = plugin_state_dir / "scratch"
    if scratch_dir.exists():
        removed = 0
        for f in scratch_dir.iterdir():
            try:
                f.unlink()
                removed += 1
            except OSError:
                pass
        if removed:
            log.info("swept %d orphan scratch files", removed)

    # Kick off the watcher on startup if everything's already configured.
    _maybe_start_watcher()
    log.info("cloud_loader plugin ready")
