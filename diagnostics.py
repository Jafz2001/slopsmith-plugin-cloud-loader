"""Diagnostics callable for the cloud_loader plugin.

Returns a snapshot of plugin state for the bundle. Excludes the token and
client_secret — those would be a credential leak in shared bundles.
"""
from __future__ import annotations

import json
from pathlib import Path


def collect(ctx: dict) -> dict:
    config_dir = Path(ctx["config_dir"])
    state_dir = config_dir / ctx["plugin_id"]
    out = {
        "schema": "cloud_loader.diag.v1",
        "has_client_secret": (state_dir / "client_secret.json").exists(),
        "has_token": (state_dir / "token.json").exists(),
        "has_db": (state_dir / "index.db").exists(),
        "config": None,
        "remote_count": None,
        "local_count": None,
    }
    cfg_path = state_dir / "config.json"
    if cfg_path.exists():
        try:
            cfg = json.loads(cfg_path.read_text())
            out["config"] = {
                "root_folder_name": cfg.get("root_folder_name"),
                "root_folder_id_present": bool(cfg.get("root_folder_id")),
                "dlc_dir_set": bool(cfg.get("dlc_dir")),
            }
        except Exception as e:
            out["config_error"] = str(e)

    try:
        import sqlite3
        db = state_dir / "index.db"
        if db.exists():
            conn = sqlite3.connect(str(db))
            out["remote_count"] = conn.execute("SELECT COUNT(*) FROM remote_index").fetchone()[0]
            out["local_count"] = conn.execute("SELECT COUNT(*) FROM local_state").fetchone()[0]
            conn.close()
    except Exception as e:
        out["db_error"] = str(e)

    return out
