"""
risk_management.py — Universal Risk Management for Prediction Market Bots.

Provides base classes and mixins implementing the mandatory risk
constraints shared by all three trading bots (Arbitrage, Bayesian,
Market Maker):

    1. The 3-5-7 Rule (per-trade, portfolio, and minimum-EV gates).
    2. Fractional Kelly Criterion (NEVER full Kelly on 5-min markets).
    3. Tail Market Filter (liquidity / volume guard).
    4. Diversification Guard (one active trade per event).

Design decisions:
    - Abstract base class ``RiskManagedBot`` encapsulates shared state
      and validation; concrete bots inherit and call ``validate_trade``
      before every entry.
    - All monetary values denominated in the same unit (USDC).
    - Pure functions where possible; mutable state limited to bankroll
      and active-event tracking.
    - No external dependencies beyond the standard library.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set


# ─────────────────────────────────────────────────────────────────────
# Risk-gate constants (hard-coded as specified)
# ─────────────────────────────────────────────────────────────────────

MAX_SINGLE_TRADE_PCT: float = 0.03      # 3 % per trade
MAX_TOTAL_EXPOSURE_PCT: float = 0.05    # 5 % across all active
MIN_EXPECTED_PROFIT_PCT: float = 0.07   # 7 % min EV to enter

KELLY_MAX_FRACTION: float = 0.25        # hard cap on Kelly
KELLY_NEVER_FULL: bool = True           # enforced always

MIN_BOOK_DEPTH_USD: float = 50.0        # tail-market filter
MIN_BOOK_VOLUME_24H: float = 500.0      # 24 h volume floor


# ─────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────

@dataclass
class TradeProposal:
    """Describes a potential trade before risk validation.

    Attributes:
        event_id: Unique identifier of the underlying event.
        market_id: Market or contract identifier.
        side: 'YES' or 'NO' (or 'BUY' / 'SELL').
        price: Entry price (0.0 – 1.0 for prediction markets).
        size_usd: Proposed dollar size.
        estimated_prob: Bot's estimate of true probability.
        market_price: Current market-implied probability.
        book_depth_usd: Current top-of-book depth in USD.
        volume_24h_usd: Rolling 24-h volume in USD.
        bot_name: Originating bot identifier.
    """

    event_id: str
    market_id: str
    side: str
    price: float
    size_usd: float
    estimated_prob: float
    market_price: float
    book_depth_usd: float = 1000.0
    volume_24h_usd: float = 10000.0
    bot_name: str = ""


@dataclass
class RiskVerdict:
    """Result of risk validation on a TradeProposal.

    Attributes:
        approved: Whether the trade passes all risk gates.
        adjusted_size_usd: Position size after Kelly / caps.
        reject_reason: Human-readable explanation if rejected.
        kelly_raw: Raw Kelly fraction before capping.
        kelly_used: Fractional Kelly actually applied.
        expected_value: EV of the proposed trade.
    """

    approved: bool
    adjusted_size_usd: float = 0.0
    reject_reason: str = ""
    kelly_raw: float = 0.0
    kelly_used: float = 0.0
    expected_value: float = 0.0


# ─────────────────────────────────────────────────────────────────────
# Pure-function helpers
# ─────────────────────────────────────────────────────────────────────

def calc_expected_value(
    est_prob: float,
    market_price: float,
) -> float:
    """Compute EV = p_hat - p  (simplified binary EV).

    Args:
        est_prob: Bot's estimated true probability.
        market_price: Current market-implied price.

    Returns:
        Expected value as a float.

    Example:
        >>> calc_expected_value(0.65, 0.55)
        0.1
    """
    return est_prob - market_price


def calc_kelly_fraction(
    est_prob: float,
    market_price: float,
    cap: float = KELLY_MAX_FRACTION,
) -> tuple[float, float]:
    """Compute fractional Kelly bet sizing for a binary market.

    Kelly for a binary bet at price *p* with estimated win-prob
    *p_hat*:

        f* = (p_hat * (1 - p) - (1 - p_hat) * p) / (1 - p)
           = (p_hat - p) / (1 - p)

    We then apply a hard cap (default 0.25) to enforce the
    "NEVER full Kelly on 5-min markets" constraint.

    Args:
        est_prob: Estimated true probability of the outcome.
        market_price: Current market price / implied probability.
        cap: Maximum allowed Kelly fraction.

    Returns:
        Tuple of (raw_kelly, capped_kelly).

    Raises:
        ValueError: If probabilities are outside (0, 1).

    Example:
        >>> calc_kelly_fraction(0.70, 0.55)
        (0.333..., 0.25)
    """
    if not (0.0 < est_prob < 1.0):
        raise ValueError(
            f"est_prob must be in (0,1), got {est_prob}"
        )
    if not (0.0 < market_price < 1.0):
        raise ValueError(
            f"market_price must be in (0,1), got {market_price}"
        )

    edge = est_prob - market_price
    if edge <= 0.0:
        return 0.0, 0.0

    raw = edge / (1.0 - market_price)
    capped = min(raw, cap)
    return raw, capped


# ─────────────────────────────────────────────────────────────────────
# Abstract base class for risk-managed bots
# ─────────────────────────────────────────────────────────────────────

class RiskManagedBot(ABC):
    """Abstract base providing universal risk management.

    Concrete bots must implement ``run_cycle`` (the main async
    trading loop) and ``_bot_name`` (string identifier).

    Shared state:
        bankroll: Current capital in USD.
        active_exposure_usd: Sum of all open position sizes.
        active_events: Set of event IDs with an open trade.

    Risk pipeline (called via ``validate_trade``):
        1. Diversification guard (one trade per event).
        2. Tail-market filter (book depth + 24 h volume).
        3. EV gate (>= 7 %).
        4. Kelly sizing with hard cap.
        5. 3 % per-trade cap.
        6. 5 % total-exposure cap.
    """

    def __init__(
        self,
        bankroll: float,
        logger: logging.Logger,
        *,
        max_single_pct: float = MAX_SINGLE_TRADE_PCT,
        max_total_pct: float = MAX_TOTAL_EXPOSURE_PCT,
        min_ev_pct: float = MIN_EXPECTED_PROFIT_PCT,
        kelly_cap: float = KELLY_MAX_FRACTION,
        min_depth: float = MIN_BOOK_DEPTH_USD,
        min_vol_24h: float = MIN_BOOK_VOLUME_24H,
    ) -> None:
        self.bankroll = bankroll
        self.log = logger

        self._max_single_pct = max_single_pct
        self._max_total_pct = max_total_pct
        self._min_ev_pct = min_ev_pct
        self._kelly_cap = kelly_cap
        self._min_depth = min_depth
        self._min_vol_24h = min_vol_24h

        self.active_exposure_usd: float = 0.0
        self.active_events: Set[str] = set()
        self._lock = asyncio.Lock()

    # ── Public risk-gate ─────────────────────────────────────────

    async def validate_trade(
        self,
        proposal: TradeProposal,
    ) -> RiskVerdict:
        """Run the full risk pipeline on a trade proposal.

        Args:
            proposal: Candidate trade to validate.

        Returns:
            RiskVerdict with approval status and adjusted size.
        """
        async with self._lock:
            return self._validate_unlocked(proposal)

    def _validate_unlocked(
        self,
        p: TradeProposal,
    ) -> RiskVerdict:
        # 1. Diversification guard
        if p.event_id in self.active_events:
            return RiskVerdict(
                approved=False,
                reject_reason=(
                    f"Diversification: already exposed to "
                    f"event {p.event_id}"
                ),
            )

        # 2. Tail-market filter
        if p.book_depth_usd < self._min_depth:
            return RiskVerdict(
                approved=False,
                reject_reason=(
                    f"Liquidity: book depth ${p.book_depth_usd:.2f}"
                    f" < min ${self._min_depth:.2f}"
                ),
            )
        if p.volume_24h_usd < self._min_vol_24h:
            return RiskVerdict(
                approved=False,
                reject_reason=(
                    f"Liquidity: 24h vol ${p.volume_24h_usd:.2f}"
                    f" < min ${self._min_vol_24h:.2f}"
                ),
            )

        # 3. EV gate (7 % minimum)
        ev = calc_expected_value(p.estimated_prob, p.market_price)
        if ev < self._min_ev_pct:
            return RiskVerdict(
                approved=False,
                reject_reason=(
                    f"EV too low: {ev:.4f} < "
                    f"min {self._min_ev_pct:.4f}"
                ),
                expected_value=ev,
            )

        # 4. Kelly sizing
        try:
            kelly_raw, kelly_used = calc_kelly_fraction(
                p.estimated_prob, p.market_price,
                cap=self._kelly_cap,
            )
        except ValueError as exc:
            return RiskVerdict(
                approved=False,
                reject_reason=f"Kelly error: {exc}",
            )

        if kelly_used <= 0.0:
            return RiskVerdict(
                approved=False,
                reject_reason="Kelly fraction <= 0 (no edge)",
                kelly_raw=kelly_raw,
                kelly_used=kelly_used,
                expected_value=ev,
            )

        kelly_size = self.bankroll * kelly_used

        # 5. Per-trade cap (3 %)
        max_single = self.bankroll * self._max_single_pct
        size = min(kelly_size, max_single, p.size_usd)

        # 6. Total-exposure cap (5 %)
        max_total = self.bankroll * self._max_total_pct
        headroom = max_total - self.active_exposure_usd
        if headroom <= 0.0:
            return RiskVerdict(
                approved=False,
                reject_reason=(
                    f"Portfolio cap: exposure "
                    f"${self.active_exposure_usd:.2f} >= "
                    f"max ${max_total:.2f}"
                ),
                kelly_raw=kelly_raw,
                kelly_used=kelly_used,
                expected_value=ev,
            )
        size = min(size, headroom)

        if size < 0.01:
            return RiskVerdict(
                approved=False,
                reject_reason="Adjusted size < $0.01",
                kelly_raw=kelly_raw,
                kelly_used=kelly_used,
                expected_value=ev,
            )

        return RiskVerdict(
            approved=True,
            adjusted_size_usd=size,
            kelly_raw=kelly_raw,
            kelly_used=kelly_used,
            expected_value=ev,
        )

    # ── Exposure bookkeeping ─────────────────────────────────────

    async def register_trade(
        self,
        event_id: str,
        size_usd: float,
    ) -> None:
        """Record a new open position."""
        async with self._lock:
            self.active_events.add(event_id)
            self.active_exposure_usd += size_usd

    async def close_trade(
        self,
        event_id: str,
        size_usd: float,
        pnl: float,
    ) -> None:
        """Record a closed position and update bankroll."""
        async with self._lock:
            self.active_events.discard(event_id)
            self.active_exposure_usd = max(
                0.0, self.active_exposure_usd - size_usd,
            )
            self.bankroll += pnl

    # ── Lifecycle ────────────────────────────────────────────────

    @property
    @abstractmethod
    def _bot_name(self) -> str:
        """Short identifier for logging."""

    @abstractmethod
    async def run_cycle(self) -> None:
        """Execute one full trading cycle."""

    async def start(self) -> None:
        """Entry point — runs cycles indefinitely."""
        self.log.info(
            f"[{self._bot_name}] Starting | "
            f"bankroll=${self.bankroll:.2f}"
        )
        while True:
            try:
                await self.run_cycle()
            except asyncio.CancelledError:
                self.log.info(
                    f"[{self._bot_name}] Cancelled — "
                    f"shutting down"
                )
                break
            except Exception as exc:
                self.log.exception(
                    f"[{self._bot_name}] Cycle error: {exc}"
                )
                await asyncio.sleep(1.0)
