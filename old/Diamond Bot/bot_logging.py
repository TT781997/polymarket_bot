"""
bot_logging.py — Structured logging utilities for prediction bots.

Each bot writes to its own distinct log file:
    - arbitrage_bot.log
    - bayesian_bot.log
    - mm_bot.log

Uses Python's built-in ``logging`` module with a consistent format
and microsecond timestamps for low-latency diagnostics.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Optional


LOG_DIR: str = os.environ.get("BOT_LOG_DIR", ".")

_FORMAT = (
    "%(asctime)s.%(msecs)03d | %(levelname)-8s | "
    "%(name)s | %(message)s"
)
_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def setup_bot_logger(
    name: str,
    log_file: str,
    *,
    level: int = logging.DEBUG,
    console: bool = False,
) -> logging.Logger:
    """Create and configure a logger for a specific bot.

    Args:
        name: Logger name (e.g. 'arbitrage_bot').
        log_file: Filename for the log (e.g. 'arbitrage_bot.log').
        level: Minimum logging level.
        console: Whether to also log to stderr.

    Returns:
        Configured logging.Logger instance.

    Example:
        >>> log = setup_bot_logger('arb', 'arbitrage_bot.log')
        >>> log.info('Bot started')
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    # Avoid duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    fmt = logging.Formatter(fmt=_FORMAT, datefmt=_DATE_FMT)

    fpath = os.path.join(LOG_DIR, log_file)
    fh = logging.FileHandler(fpath, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    if console:
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(fmt)
        logger.addHandler(ch)

    return logger


def ts_ms() -> str:
    """Return current timestamp with millisecond precision.

    Returns:
        String like '2026-03-06 14:23:05.123'.
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
