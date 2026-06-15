"""Google Drive client wrapper for the cloud_loader plugin.

OAuth: the user supplies their own OAuth 2.0 client credentials JSON
(downloaded from Google Cloud Console for an "OAuth client ID" of type
"Desktop app"). The plugin stores it in config_dir/cloud_loader/client.json
and the resulting refresh token in config_dir/cloud_loader/token.json.

We use the `drive` scope (full access) rather than `drive.file` because
the user's library most likely already exists in Drive — drive.file only
sees files the app itself created, which would force re-uploading
everything. This is documented in the settings UI.
"""
from __future__ import annotations

import io
import json
import logging
import socket
import threading
from pathlib import Path
from typing import Callable, Iterator, Optional

# Default socket timeout — the googleapiclient transport inherits this. Without
# it a stalled connection blocks forever (no progress, no exception, no retry
# triggers either since num_retries only kicks in on raised exceptions). This
# is module-level so the very first connection picks it up.
socket.setdefaulttimeout(120)

from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

log = logging.getLogger("slopsmith.plugin.cloud_loader.drive")

SCOPES = ["https://www.googleapis.com/auth/drive"]
# Sloppak-only: the cloud library indexes/streams Slopsmith's own .sloppak
# song format (the app's converted, non-encrypted container).
EXTENSIONS = (".sloppak",)


class DriveClient:
    def __init__(self, client_secret_path: Path, token_path: Path):
        self.client_secret_path = client_secret_path
        self.token_path = token_path
        self._creds: Optional[Credentials] = None
        # The googleapiclient `service` object (and its underlying httplib2.Http)
        # is NOT thread-safe — calling .execute() concurrently from multiple
        # threads races on the shared connection pool. Each worker thread gets
        # its own service instance via thread-local storage; build() pays one
        # discovery-doc fetch per thread, but the thread pool reuses workers
        # so the overhead is ~N (workers), not ~N (downloads).
        self._thread_local = threading.local()
        self._lock = threading.Lock()
        self._auth_thread: Optional[threading.Thread] = None
        self._auth_status: dict = {"state": "idle", "error": None}

    # ---- auth ----

    def is_authenticated(self) -> bool:
        return self._load_creds() is not None

    def _load_creds(self) -> Optional[Credentials]:
        if self._creds and self._creds.valid:
            return self._creds
        if not self.token_path.exists():
            log.info("_load_creds: token file does not exist at %s", self.token_path)
            return None
        try:
            creds = Credentials.from_authorized_user_file(str(self.token_path), SCOPES)
        except Exception as e:
            log.warning("_load_creds: failed to parse token (%s); deleting it", e)
            # Token file is corrupted/incompatible — nuke it so the user can
            # re-auth cleanly instead of an endless "stuck" state.
            try:
                self.token_path.unlink()
            except OSError:
                pass
            return None

        log.info("_load_creds: loaded token expired=%s has_refresh_token=%s valid=%s",
                 creds.expired, bool(creds.refresh_token), creds.valid)
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(GoogleAuthRequest())
                self.token_path.write_text(creds.to_json())
                log.info("_load_creds: refreshed token successfully")
            except Exception as e:
                log.warning("_load_creds: token refresh failed: %s", e)
                return None
        if not creds or not creds.valid:
            log.warning("_load_creds: creds still not valid after refresh attempt")
            return None
        self._creds = creds
        return creds

    def _service_for(self, creds: Credentials):
        svc = getattr(self._thread_local, "service", None)
        if svc is None:
            svc = build("drive", "v3", credentials=creds, cache_discovery=False)
            self._thread_local.service = svc
        return svc

    def auth_status(self) -> dict:
        return dict(self._auth_status)

    def start_auth_flow(self) -> None:
        """Start OAuth flow in background thread (blocks on local_server).

        The user must have already saved their client_secret JSON. We pop a
        local HTTP server on a random port, open the browser to Google's
        consent screen, and wait for the redirect callback. Success = token
        persisted to disk.
        """
        with self._lock:
            if self._auth_thread and self._auth_thread.is_alive():
                return
            if not self.client_secret_path.exists():
                self._auth_status = {"state": "error", "error": "client_secret.json not configured"}
                return
            self._auth_status = {"state": "running", "error": None}
            self._auth_thread = threading.Thread(target=self._run_auth_flow, daemon=True)
            self._auth_thread.start()

    def _run_auth_flow(self):
        try:
            flow = InstalledAppFlow.from_client_secrets_file(
                str(self.client_secret_path), SCOPES
            )
            creds = flow.run_local_server(port=0, open_browser=True)
            self.token_path.write_text(creds.to_json())
            self._creds = creds
            self._thread_local = threading.local()  # force re-build per thread
            self._auth_status = {"state": "ok", "error": None}
            log.info("OAuth flow completed; token persisted")
        except Exception as e:
            log.exception("OAuth flow failed")
            self._auth_status = {"state": "error", "error": str(e)}

    def disconnect(self):
        with self._lock:
            self._creds = None
            self._thread_local = threading.local()
            if self.token_path.exists():
                self.token_path.unlink()
            self._auth_status = {"state": "idle", "error": None}

    # ---- Drive operations ----

    def _svc(self):
        creds = self._load_creds()
        if not creds:
            raise RuntimeError("not authenticated")
        return self._service_for(creds)

    def get_drive_id_for(self, folder_id: str) -> Optional[str]:
        """Return the Shared Drive id this folder belongs to, or None for
        My Drive. Required by list/iter so we can pass corpora='drive' +
        driveId — without those, Google's API quietly truncates results
        for items inside Shared Drives.
        """
        if folder_id in ("root", None):
            return None
        svc = self._svc()
        try:
            info = svc.files().get(
                fileId=folder_id, fields="driveId",
                supportsAllDrives=True,
            ).execute()
            return info.get("driveId")
        except HttpError as e:
            # If the id IS a Shared Drive id itself (not a folder under it),
            # files.get fails with 404 — fall back to drives.get to confirm.
            if e.resp.status == 404:
                try:
                    svc.drives().get(driveId=folder_id, fields="id").execute()
                    return folder_id
                except HttpError:
                    return None
            return None

    def list_folders(self, parent_id: str = "root",
                     drive_id: Optional[str] = None) -> list[dict]:
        svc = self._svc()
        # Auto-detect drive context if caller didn't pass one. Without
        # corpora='drive'+driveId, files.list against a Shared Drive parent
        # returns 0 items even with includeItemsFromAllDrives=true.
        if drive_id is None and parent_id not in ("root", None):
            drive_id = self.get_drive_id_for(parent_id)

        extra = {}
        if drive_id:
            extra = {"corpora": "drive", "driveId": drive_id}

        q = (
            f"'{parent_id}' in parents "
            "and mimeType = 'application/vnd.google-apps.folder' "
            "and trashed = false"
        )
        out = []
        page_token = None
        while True:
            resp = svc.files().list(
                q=q, fields="nextPageToken, files(id, name)",
                pageToken=page_token, pageSize=200,
                supportsAllDrives=True, includeItemsFromAllDrives=True,
                **extra,
            ).execute()
            out.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return out

    def list_shared_drives(self) -> list[dict]:
        """List Shared Drives (Team Drives) the user has access to.

        These are top-level drives separate from "My Drive" — typical in
        Google Workspace organizations. Each has its own drive ID that
        can be used as a parent for list_folders() and iter_song_files().
        """
        svc = self._svc()
        out = []
        page_token = None
        while True:
            resp = svc.drives().list(
                pageSize=100, pageToken=page_token,
                fields="nextPageToken, drives(id, name)",
            ).execute()
            out.extend(resp.get("drives", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return out

    def list_shared_with_me(self) -> list[dict]:
        """List top-level folders shared with the user but living in someone
        else's My Drive (the "Shared with me" tab in Drive's UI).

        These have no fixed parent — the only way to find them is the
        sharedWithMe=true query. Once we know their id, listing their
        contents works through the normal list_folders().
        """
        svc = self._svc()
        q = ("sharedWithMe = true "
             "and mimeType = 'application/vnd.google-apps.folder' "
             "and trashed = false")
        out = []
        page_token = None
        while True:
            resp = svc.files().list(
                q=q, fields="nextPageToken, files(id, name)",
                pageToken=page_token, pageSize=200,
                supportsAllDrives=True, includeItemsFromAllDrives=True,
            ).execute()
            out.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
        return out

    def iter_song_files(self, folder_id: str, recursive: bool = True,
                        drive_id: Optional[str] = None) -> Iterator[dict]:
        """Yield {id, name, modifiedTime, size, parents} for every .sloppak.

        Recursion is BFS over folders to avoid blowing the call stack on deep trees.
        drive_id is auto-detected if not provided — required for Shared Drives.
        """
        svc = self._svc()
        if drive_id is None:
            drive_id = self.get_drive_id_for(folder_id)
        extra = {}
        if drive_id:
            extra = {"corpora": "drive", "driveId": drive_id}

        queue: list[str] = [folder_id]
        seen: set[str] = set()
        while queue:
            current = queue.pop(0)
            if current in seen:
                continue
            seen.add(current)
            page_token = None
            while True:
                resp = svc.files().list(
                    q=f"'{current}' in parents and trashed = false",
                    fields="nextPageToken, files(id, name, mimeType, modifiedTime, size, parents)",
                    pageToken=page_token, pageSize=200,
                    supportsAllDrives=True, includeItemsFromAllDrives=True,
                    **extra,
                ).execute()
                for f in resp.get("files", []):
                    if f.get("mimeType") == "application/vnd.google-apps.folder":
                        if recursive:
                            queue.append(f["id"])
                        continue
                    name = f.get("name", "")
                    if name.lower().endswith(EXTENSIONS):
                        yield {
                            "id": f["id"],
                            "name": name,
                            "modifiedTime": f.get("modifiedTime", ""),
                            "size": int(f.get("size", 0) or 0),
                        }
                page_token = resp.get("nextPageToken")
                if not page_token:
                    break

    def download_to(self, drive_file_id: str, dest: Path,
                    progress_cb: Optional[Callable[[float], None]] = None) -> None:
        svc = self._svc()
        dest.parent.mkdir(parents=True, exist_ok=True)
        request = svc.files().get_media(fileId=drive_file_id, supportsAllDrives=True)
        tmp = dest.with_suffix(dest.suffix + ".part")
        with tmp.open("wb") as fh:
            downloader = MediaIoBaseDownload(fh, request, chunksize=8 * 1024 * 1024)
            done = False
            while not done:
                status, done = downloader.next_chunk(num_retries=3)
                if progress_cb and status:
                    try:
                        progress_cb(status.progress())
                    except Exception:
                        pass
        tmp.replace(dest)

    def upload_or_update(self, local_path: Path, parent_folder_id: str,
                         existing_file_id: Optional[str] = None) -> str:
        """Upload (create) or update an existing Drive file. Returns the file id."""
        svc = self._svc()
        media = MediaFileUpload(
            str(local_path), resumable=True,
            chunksize=8 * 1024 * 1024,
        )
        if existing_file_id:
            try:
                svc.files().update(
                    fileId=existing_file_id, media_body=media,
                    supportsAllDrives=True,
                ).execute()
                return existing_file_id
            except HttpError as e:
                if e.resp.status != 404:
                    raise
                # File was deleted from Drive — fall through to create.
        body = {"name": local_path.name, "parents": [parent_folder_id]}
        resp = svc.files().create(
            body=body, media_body=media, fields="id",
            supportsAllDrives=True,
        ).execute()
        return resp["id"]
