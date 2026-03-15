"""Shared storage helpers.

This module provides a sensible default for where the app writes small local
state files (users, cache, connection store) while still allowing the location
to be overridden via environment variables.

Azure App Service (Linux containers) provides a persistent writable folder at
"/home"; when deploying elsewhere you can set APP_DATA_DIR or DATA_DIR.
"""

from __future__ import annotations

import os
from pathlib import Path


def get_data_dir() -> Path:
    """Return a directory path suitable for persistent writable state."""
    env_dir = os.getenv("APP_DATA_DIR") or os.getenv("DATA_DIR")
    if env_dir:
        data_dir = Path(env_dir)
    else:
        # Standard writable location on Azure App Service for Linux containers
        data_dir = Path("/home")
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        # Best-effort: fall back to current working directory if /home isn't writable.
        data_dir = Path(".")
    return data_dir
