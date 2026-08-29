"""Estado em JSON atomico + kill-switch control.json (padrao dos bots XRP)."""
from __future__ import annotations
import json
import os
import tempfile
from typing import Any


def atomic_write(path: str, obj: Any) -> None:
    d = os.path.dirname(os.path.abspath(path)) or "."
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(obj, f, separators=(",", ":"))
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


def read_json(path: str, default: Any = None) -> Any:
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def kill_active(control_path: str) -> bool:
    return bool(read_json(control_path, {"kill": False}).get("kill", False))
