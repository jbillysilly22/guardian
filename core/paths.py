from __future__ import annotations

import os
import sys
from pathlib import Path

def project_root() -> Path:
    # core/paths.py lives at <repo_root>/core/paths.py
    return Path(__file__).resolve().parents[1]

def app_data_dir(app_name: str = "guardian") -> Path:
    override = os.environ.get("GUARDIAN_DATA_DIR")
    if override:
        return Path(override).expanduser().resolve()

    if sys.platform.startswith("win"):
        base = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA")
        if base:
            return Path(base) / app_name
        return Path.home() / app_name

    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / app_name

    base = os.environ.get("XDG_DATA_HOME")
    if base:
        return Path(base) / app_name

    return Path.home() / ".local" / "share" / app_name
