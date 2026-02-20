# src/pycronado/util.py
import logging
import threading
from typing import Optional
from urllib.parse import urlparse

_logger_lock = threading.Lock()


def getLogger(name, level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    with _logger_lock:
        # If this logger OR any parent/root already has handlers, don't add another.
        # (This avoids the "root handler + local handler" double-print.)
        if logger.hasHandlers():
            return logger

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # If we *do* attach a handler here, don't also bubble to root.
        logger.propagate = False

    return logger


def normalize_origin(raw: str | None) -> Optional[str]:
    """
    Normalize an Origin-ish string to canonical 'scheme://host[:port]'.

    - only http/https
    - strips default ports (80/443)
    - lowercases scheme + host
    - rejects userinfo, paths, query, fragment
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() == "null":
        return None

    p = urlparse(s)

    scheme = (p.scheme or "").lower()
    if scheme not in ("http", "https"):
        return None
    if not p.netloc:
        return None
    if "@" in p.netloc:
        return None
    if p.path not in ("", "/") or p.query or p.fragment:
        return None

    host = p.hostname
    if not host:
        return None
    host = host.lower()

    # IPv6 needs brackets in origins
    if ":" in host and not host.startswith("[") and host.count(":") >= 2:
        host = f"[{host}]"

    port = p.port
    if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        port = None

    return f"{scheme}://{host}" if port is None else f"{scheme}://{host}:{int(port)}"
