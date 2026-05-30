"""User-Agent parser using device_detector (Matomo).

Enterprise-grade UA parsing: 10K+ device models, 16K+ bots, 1M parses/sec.
LRU cache avoids re-parsing the same UA string (common in production).

F.41 (2026-05-30) — cold-start latency fix
------------------------------------------
``device_detector`` lazy-loads its YAML/regex rule bundles on first use of
each detector family (OS, client/browser, per-device-type, bot, brand). The
**first** parse after a process start therefore pays the full load and can
take 300–780ms (measured: median ~720ms on the staging node). That exceeds
the CF Worker's 2000ms race deadline in the worst case
(``services/worker/src/index.js`` ``AbortSignal.timeout(2000)``), so the
first click after every node start/deploy fell back to the worker
``fallback_url`` instead of the offer (finding F1, Sentry
``GEO-TDS-WORKERS-2 "All backends failed"``).

Fix (zero output change): ``warmup()`` pre-loads every lazy module at
service startup (called from ``app.main`` lifespan), and the LRU is enlarged
so hot UAs survive longer. Neither touches the parsing logic — every parsed
value is byte-identical to before (verified by the F.41 golden diff).
"""

import os
from functools import lru_cache
from device_detector import DeviceDetector

# LRU size is env-overridable (default 32768 ≈ 8× the previous 4096). A larger
# cache keeps more distinct hot UAs resident under real traffic, where the
# working set of unique UAs comfortably exceeds 4096. Each entry is a small
# dict (~0.5KB) ⇒ 32768 ≈ ~16MB per worker process — sized against droplet
# RAM before raising further. Override via TDS_UA_CACHE_SIZE for tuning.
_UA_CACHE_SIZE = int(os.getenv("TDS_UA_CACHE_SIZE", "32768"))


@lru_cache(maxsize=_UA_CACHE_SIZE)
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
        # F.17 (2026-05-03): emit device_detector's canonical Title
        # Case client name verbatim ("Chrome", "Mobile Safari",
        # "Microsoft Edge", "Samsung Browser", "Yandex Browser").
        # admin-api's `browser` criterion enum is the SAME taxonomy
        # (loaded from device_detector's bundled YAML via
        # `app.common.browser_taxonomy.KNOWN_BROWSERS`); operators
        # save criteria with these exact strings, so the storage and
        # runtime vocabularies must agree. The previous `.lower()` —
        # which leaked "microsoft edge" / "chrome mobile" — silently
        # broke the would-be match between OT criteria and live
        # clicks. URL macro `{browser}` substitution likewise now
        # yields Title Case in target URLs (downstream analytics
        # that case-folded the value see no behaviour change; rare
        # case-sensitive consumers should update their parsers).
        "browser": d.client_name() or "other",
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


# Representative UAs chosen to touch EVERY device_detector lazy bundle so a
# single warm-up pass pre-loads them all: each OS family, the major
# client/browser regex groups (incl. Yandex/Samsung/Opera/Huawei/Xiaomi which
# matter for CIS + Android traffic), each device-type detector (smartphone,
# tablet, tv, console, desktop), the bot detector, and several brand regex
# files. Not exhaustive of real traffic — exhaustive of the *code paths* that
# would otherwise load on the first production request. Order is irrelevant.
_WARMUP_UAS: tuple[str, ...] = (
    # iOS — smartphone + tablet (Apple brand, Mobile Safari client)
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 17_2 like Mac OS X) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    # Android — phone (Chrome Mobile + Samsung brand/device) + tablet
    "Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-X710) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/119.0.0.0 Safari/537.36",
    # Android in-app WebView + Facebook app client (separate client regexes)
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Version/4.0 Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; SM-A536B) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/118.0.0.0 Mobile Safari/537.36 [FB_IAB/FB4A;FBAV/440.0.0.0;]",
    # Samsung Browser, Yandex Browser (CIS-critical), Opera, Firefox-Android
    "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) "
    "SamsungBrowser/22.0 Chrome/115.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 12; RMX3085) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/108.0.0.0 YaBrowser/23.1.1.100 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; VOG-L29) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/110.0.0.0 Mobile Safari/537.36 OPR/74.0.0.0",
    "Mozilla/5.0 (Android 13; Mobile; rv:121.0) Gecko/121.0 Firefox/121.0",
    # Huawei + Xiaomi/MIUI browsers (brand-specific device + client regexes)
    "Mozilla/5.0 (Linux; Android 10; HRY-LX1T) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/107.0.0.0 Mobile Safari/537.36 HuaweiBrowser/13.0.0.300",
    "Mozilla/5.0 (Linux; Android 12; 2201123G) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/107.0.0.0 Mobile Safari/537.36 XiaoMi/MiuiBrowser/13.24.0",
    # Desktop — Windows Chrome + Edge, macOS Safari, Linux Firefox, ChromeOS
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; CrOS x86_64 14541.0.0) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36",
    # TV (Tizen Smart-TV) + consoles (PlayStation, Xbox) — tv/console detectors
    "Mozilla/5.0 (SMART-TV; LINUX; Tizen 7.0) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Version/7.0 TV Safari/537.36",
    "Mozilla/5.0 (PlayStation; PlayStation 5/8.00) AppleWebKit/605.1.15 (KHTML, like Gecko) "
    "Version/14.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; Xbox; Xbox One) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edge/120.0",
    # Bot detector
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
)


def warmup() -> int:
    """Pre-load every device_detector lazy bundle (F.41).

    Iterates ``_WARMUP_UAS`` through ``parse_ua`` once. The first parse of
    each OS/client/device-type/bot/brand family triggers that family's
    one-time YAML/regex load, so after this returns the parser is fully warm
    and the first *production* click parses in single-digit milliseconds
    instead of paying the ~720ms cold load (see module docstring, F1).

    Called from ``app.main`` lifespan BEFORE the node accepts traffic. Each
    UA is parsed inside its own try/except so one malformed/edge entry can
    never abort the warm-up (and thus never blocks node boot). Returns the
    number of UAs parsed without error (diagnostics only).
    """
    parsed = 0
    for ua in _WARMUP_UAS:
        try:
            parse_ua(ua)
            parsed += 1
        except Exception:  # noqa: BLE001 — warm-up must never raise
            # A single bad warm-up UA is non-fatal: skip it, keep warming the
            # rest. parse_ua already swallows parse errors internally; this is
            # a defense-in-depth backstop for anything unexpected.
            continue
    return parsed
