"""User-Agent parser using device_detector (Matomo).

Enterprise-grade UA parsing: 10K+ device models, 16K+ bots, 1M parses/sec.
LRU cache avoids re-parsing the same UA string (common in production).
"""

from functools import lru_cache
from device_detector import DeviceDetector


@lru_cache(maxsize=4096)
def parse_ua(ua: str) -> dict:
    """Parse User-Agent string. Returns dict with all device/os/browser info.

    Cached: identical UA strings return the same result instantly.
    """
    if not ua:
        return _empty_result()

    try:
        d = DeviceDetector(ua).parse()
    except Exception:
        return _empty_result()

    device_type = d.device_type() or ""
    # Normalize device_type to our categories
    if device_type in ("smartphone", "phablet", "feature phone"):
        normalized_type = "mobile"
    elif device_type == "tablet":
        normalized_type = "tablet"
    elif device_type in ("desktop", "tv", "console", "car browser",
                         "smart display", "camera", "portable media player",
                         "smart speaker"):
        normalized_type = "desktop"
    else:
        normalized_type = "desktop"  # unknown → desktop (safe default)

    return {
        "device_type": normalized_type,
        "device_type_raw": device_type,
        "os": (d.os_name() or "").lower() or "other",
        "os_version": d.os_version() or "",
        "browser": (d.client_name() or "").lower() or "other",
        "browser_version": d.client_version() or "",
        "device_brand": d.device_brand() or "",
        "device_model": d.device_model() or "",
        "is_bot": d.is_bot(),
    }


def _empty_result() -> dict:
    return {
        "device_type": "desktop",
        "device_type_raw": "",
        "os": "other",
        "os_version": "",
        "browser": "other",
        "browser_version": "",
        "device_brand": "",
        "device_model": "",
        "is_bot": False,
    }
