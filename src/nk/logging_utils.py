from __future__ import annotations

from copy import copy, deepcopy
from typing import Any
from urllib.parse import unquote

from uvicorn.config import LOGGING_CONFIG
from uvicorn.logging import AccessFormatter as UvicornAccessFormatter


def _decode_path(value: str) -> str:
    try:
        return unquote(value, encoding="utf-8", errors="replace")
    except Exception:
        return value


class Utf8AccessFormatter(UvicornAccessFormatter):
    """Uvicorn access log formatter that prints decoded UTF-8 paths."""

    def formatMessage(self, record):  # type: ignore[override]
        try:
            client_addr, method, full_path, http_version, status_code = record.args
        except Exception:
            return super().formatMessage(record)
        decoded_path = _decode_path(full_path) if isinstance(full_path, str) else full_path
        new_record = copy(record)
        new_record.args = (client_addr, method, decoded_path, http_version, status_code)
        return super().formatMessage(new_record)


def build_uvicorn_log_config() -> dict[str, Any]:
    """Return a uvicorn logging config that uses Utf8AccessFormatter."""
    config = deepcopy(LOGGING_CONFIG)
    formatter = config.get("formatters", {}).get("access")
    if isinstance(formatter, dict):
        formatter["()"] = "nk.logging_utils.Utf8AccessFormatter"
    return config
