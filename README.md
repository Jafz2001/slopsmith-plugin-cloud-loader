# cloud_loader — Google Drive backend for the Slopsmith library

Stream your `.psarc` / `.sloppak` library from Google Drive instead of
keeping everything on local disk. Metadata is indexed once; song files
download on Play and are removed after the next Play. New files created
by other plugins (sloppak converter, retune) are auto-uploaded.

## How it works

```
Bootstrap (one-time, per song):
  Drive → download to scratch dir → extract_meta() → meta_db.put() → delete scratch
  Result: library browser shows all songs without any binaries on disk.

Play:
  Frontend wrapper around window.playSong asks /needs_prefetch first.
  If yes → /prefetch starts a background download with progress polling.
           An overlay shows "Downloading from Google Drive · X%".
  When ready → original playSong runs against a real local file.

After play:
  Next prefetch GC-deletes the previous song's local copy.

Auto-upload:
  Background watcher polls cloud-dlc/ every 30 s for new/modified files.
  Uploads to Drive, updates the local index, keeps a stable file_id so
  later edits update the same Drive object.
```

The server's WebSocket play handler runs **unmodified** against a real
on-disk file. No monkey-patching, no fragile core hooks.

## Installation

```
cd /path/to/slopsmith/plugins
git clone https://github.com/Jafz2001/slopsmith-plugin-cloud-loader.git cloud_loader
pip install -r cloud_loader/requirements.txt
# restart Slopsmith
```

A new **Cloud Library** panel appears under Settings once the plugin is
loaded. Continue with *Setup* below to connect your Google account.

## Setup (one-time, ~5 min)

1. **Create OAuth credentials** in
   [Google Cloud Console](https://console.cloud.google.com/apis/credentials):
   - Create a project (or reuse one)
   - Enable the **Google Drive API** for the project
   - Credentials → Create credentials → OAuth client ID → **Desktop app**
   - Download the JSON
2. In Slopsmith: **Settings → Cloud Library**
   - Step 1: Upload the downloaded `client_secret.json`
   - Step 2: Click *Connect Google Drive* — completes OAuth in your browser
   - Step 3: *Browse...* and pick the folder containing your library
   - Step 4: Leave Local cache directory empty to reuse your DLC folder
   - Step 5: *Start scan* — metadata extraction (~5–10 min for 500 songs)

## Auth scope

This plugin requests the full `drive` scope (read + write), not the more
restrictive `drive.file`. Reason: `drive.file` only sees files the app
itself created — it could not read your existing library, forcing a
re-upload of everything. If that trade-off matters to you, use a
dedicated Google account for the plugin.

## What's stored where

| Path | Contents |
|---|---|
| `$CONFIG_DIR/cloud_loader/client_secret.json` | Your OAuth client credentials |
| `$CONFIG_DIR/cloud_loader/token.json` | OAuth refresh token (sensitive) |
| `$CONFIG_DIR/cloud_loader/config.json` | Root folder id, dlc dir |
| `$CONFIG_DIR/cloud_loader/index.db` | SQLite index of remote files + local state |
| `<dlc_dir>/<song>.psarc` | Transient — present only between Play and the next Play |

Token and client_secret are **excluded** from diagnostics bundles.

## Limitations / known issues

- **First Play is slow** — downloads the whole archive before the
  WebSocket play handler runs. A 200 MB PSARC on 50 Mbps ≈ 30 s.
- **No range-request streaming** — the PSARC reader doesn't support
  partial reads, so we fetch whole archives. Block-level streaming
  would require core changes to `lib/psarc.py`.
- **Conflict resolution** — multi-device editing is last-write-wins.
  Editing the same file from two devices without restarting can clobber
  one side's changes when the watcher runs.
- **Rate limits** — Drive API allows ~1000 requests / 100 s / user. A
  scan of 500 songs stays well under this.
