"""
rate_limiter.py — Rate limiter thread-safe para chamadas OCR.
"""

import threading
import time


class RateLimiter:
    """Controla a taxa de chamadas API (requests por minuto)."""

    def __init__(self, rpm: int = 30):
        self._interval = 60.0 / rpm
        self._last_call = 0.0
        self._lock = threading.Lock()

    def wait(self):
        """Bloqueia até a próxima chamada ser permitida."""
        with self._lock:
            elapsed = time.time() - self._last_call
            if elapsed < self._interval:
                time.sleep(self._interval - elapsed)
            self._last_call = time.time()
