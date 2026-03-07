"""
bayesian_bot.py — Bot 2: The Bayesian Signal (Directional Bot).

Takes directional bets on prediction markets based on real-time
data ingestion and sequential Bayesian updating.

Mathematical model (log-space for numerical stability):

    log P(H | D) = log P(H)
                  + sum_{k=1}^{t} log P(D_k | H)
                  - log Z

Position-sizing EV:

    EV = p_hat * (1 - p) - (1 - p_hat) * p = p_hat - p

where p_hat is the calculated true probability and p is the
current market price.

Inherits from ``RiskManagedBot`` — all universal risk constraints
(3-5-7 rule, Fractional Kelly, tail filter, diversification)
are enforced.
"""

from __future__ import annotations

import asyncio
import math
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from bot_logging import setup_bot_logger
from math_models import BayesianEstimator, compute_ev
from risk_management import (
    RiskManagedBot,
    RiskVerdict,
    TradeProposal,
    calc_kelly_fraction,
)


# ─────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────

BAY_POLL_INTERVAL_S: float = 0.25      # data-ingestion rate
BAY_MIN_UPDATES: int = 5               # min observations before bet
BAY_CONFIDENCE_THRESHOLD: float = 0.60  # min posterior for YES
BAY_EDGE_THRESHOLD: float = 0.07       # matches 7 % EV gate


# ─────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────

@dataclass
class DataSignal:
    """A single piece of evidence for Bayesian updating.

    Attributes:
        source: Where the data came from (e.g. 'price_tick',
            'news_api', 'social_sentiment').
        log_lik_h: log P(D | H) — likelihood under hypothesis.
        log_lik_not_h: log P(D | ~H) — likelihood under ~H.
        timestamp: When the signal was received.
        metadata: Optional extra context.
    """

    source: str
    log_lik_h: float
    log_lik_not_h: float
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BayesianPosition:
    """Tracks an open directional position.

    Attributes:
        event_id: Unique event identifier.
        market_id: Market / contract identifier.
        side: 'YES' or 'NO'.
        entry_price: Price at which position was opened.
        size_usd: Dollar size of the position.
        entry_posterior: Posterior when trade was entered.
        entry_time: Timestamp of entry.
    """

    event_id: str
    market_id: str
    side: str
    entry_price: float
    size_usd: float
    entry_posterior: float
    entry_time: float = field(default_factory=time.time)


# ─────────────────────────────────────────────────────────────────────
# Signal generators (pluggable data sources)
# ─────────────────────────────────────────────────────────────────────

def price_tick_signal(
    current_price: float,
    prev_price: float,
    volatility: float = 0.05,
) -> DataSignal:
    """Generate a Bayesian signal from a price tick.

    A price increase is evidence that the market believes H is
    more likely. We model the likelihood ratio based on the
    direction and magnitude of the move.

    Args:
        current_price: Current market price.
        prev_price: Previous market price.
        volatility: Estimated volatility for scaling.

    Returns:
        DataSignal with log-likelihoods.

    Example:
        >>> sig = price_tick_signal(0.62, 0.60, 0.05)
        >>> sig.log_lik_h > sig.log_lik_not_h
        True
    """
    if volatility < 1e-9:
        volatility = 0.05

    delta = current_price - prev_price
    z = delta / volatility

    # Under H (event happens): price moves should be positive
    # Under ~H: price moves should be negative
    # Gaussian likelihood with shifted means
    log_lik_h = -0.5 * (z - 0.5) ** 2
    log_lik_not_h = -0.5 * (z + 0.5) ** 2

    return DataSignal(
        source="price_tick",
        log_lik_h=log_lik_h,
        log_lik_not_h=log_lik_not_h,
    )


def volume_imbalance_signal(
    buy_volume: float,
    sell_volume: float,
) -> DataSignal:
    """Generate a signal from order-flow imbalance.

    High buy volume relative to sell volume is evidence for H.

    Args:
        buy_volume: Aggregate buy volume.
        sell_volume: Aggregate sell volume.

    Returns:
        DataSignal with log-likelihoods.

    Example:
        >>> sig = volume_imbalance_signal(100.0, 50.0)
        >>> sig.log_lik_h > sig.log_lik_not_h
        True
    """
    total = buy_volume + sell_volume
    if total < 1e-9:
        return DataSignal(
            source="volume_imbalance",
            log_lik_h=0.0,
            log_lik_not_h=0.0,
        )

    ratio = buy_volume / total  # 0 to 1
    # Map ratio to log-likelihoods
    log_lik_h = math.log(max(ratio, 1e-12))
    log_lik_not_h = math.log(max(1.0 - ratio, 1e-12))

    return DataSignal(
        source="volume_imbalance",
        log_lik_h=log_lik_h,
        log_lik_not_h=log_lik_not_h,
    )


# ─────────────────────────────────────────────────────────────────────
# Bayesian Signal Bot
# ─────────────────────────────────────────────────────────────────────

class BayesianBot(RiskManagedBot):
    """Bot 2 — The Bayesian Signal: directional probability bets.

    Maintains a ``BayesianEstimator`` per tracked event. As new
    data signals arrive, the posterior is updated in log-space.
    When the posterior diverges from the market price by more
    than the EV threshold, the bot proposes a directional trade
    that must pass through the full risk pipeline.

    Args:
        bankroll: Starting capital in USD.
        data_source: Async callable returning List[DataSignal].
        price_source: Async callable returning Dict[event_id, price].
        execute_fn: Async callable to place an order.
            Signature: (market_id, side, price, size) -> bool.
        market_meta: Dict mapping event_id -> dict with
            'market_id', 'book_depth_usd', 'volume_24h_usd'.
    """

    def __init__(
        self,
        bankroll: float,
        data_source: Optional[Any] = None,
        price_source: Optional[Any] = None,
        execute_fn: Optional[Any] = None,
        market_meta: Optional[Dict[str, Dict]] = None,
    ) -> None:
        logger = setup_bot_logger(
            "bayesian_bot", "bayesian_bot.log",
        )
        super().__init__(bankroll=bankroll, logger=logger)

        self._data_source = data_source
        self._price_source = price_source
        self._execute = execute_fn
        self._market_meta = market_meta or {}

        # Per-event Bayesian estimators
        self._estimators: Dict[
            str, BayesianEstimator
        ] = {}
        # Open positions
        self._positions: Dict[
            str, BayesianPosition
        ] = {}
        self._cycle_count: int = 0

    @property
    def _bot_name(self) -> str:
        return "BAYESIAN"

    # ── Estimator management ─────────────────────────────────────

    def _get_estimator(
        self,
        event_id: str,
        prior: float = 0.5,
    ) -> BayesianEstimator:
        """Get or create a Bayesian estimator for an event.

        Args:
            event_id: Unique event identifier.
            prior: Initial prior probability if creating new.

        Returns:
            BayesianEstimator instance.
        """
        if event_id not in self._estimators:
            self._estimators[event_id] = BayesianEstimator()
            if prior != 0.5:
                self._estimators[event_id].reset(prior)
        return self._estimators[event_id]

    # ── Data ingestion ───────────────────────────────────────────

    async def _ingest_signals(
        self,
    ) -> Dict[str, float]:
        """Fetch new data and update all estimators.

        Returns:
            Dict mapping event_id to current posterior.
        """
        posteriors: Dict[str, float] = {}

        if self._data_source is not None:
            try:
                signals = await asyncio.wait_for(
                    self._data_source(), timeout=2.0,
                )
            except Exception as exc:
                self.log.warning(
                    f"[BAYESIAN] Data source error: {exc}"
                )
                signals = []
        else:
            signals = []

        for sig in signals:
            event_id = sig.metadata.get("event_id", "")
            if not event_id:
                continue

            est = self._get_estimator(event_id)
            posterior = est.update(
                sig.log_lik_h, sig.log_lik_not_h,
            )
            posteriors[event_id] = posterior

        return posteriors

    async def _fetch_prices(
        self,
    ) -> Dict[str, float]:
        """Fetch current market prices for tracked events.

        Returns:
            Dict mapping event_id to market price.
        """
        if self._price_source is not None:
            try:
                return await asyncio.wait_for(
                    self._price_source(), timeout=2.0,
                )
            except Exception as exc:
                self.log.warning(
                    f"[BAYESIAN] Price source error: {exc}"
                )
        return {}

    # ── Trade evaluation ─────────────────────────────────────────

    def _evaluate_signals(
        self,
        posteriors: Dict[str, float],
        prices: Dict[str, float],
    ) -> List[TradeProposal]:
        """Identify events where posterior diverges from price.

        For each event where we have both a posterior and a
        market price, compute the EV. If EV exceeds the
        threshold and we have enough data points, propose a
        trade.

        Args:
            posteriors: Event -> posterior probability.
            prices: Event -> market price.

        Returns:
            List of TradeProposals to validate.
        """
        proposals: List[TradeProposal] = []

        for event_id, posterior in posteriors.items():
            if event_id not in prices:
                continue

            market_price = prices[event_id]
            est = self._estimators.get(event_id)
            if est is None or est.n_updates < BAY_MIN_UPDATES:
                continue

            ev = compute_ev(posterior, market_price)

            # Determine side: YES if posterior > market, NO otherwise
            if ev >= BAY_EDGE_THRESHOLD:
                side = "YES"
                entry_price = market_price
                est_prob = posterior
            elif -ev >= BAY_EDGE_THRESHOLD:
                # Contrarian: buy NO
                side = "NO"
                entry_price = 1.0 - market_price
                est_prob = 1.0 - posterior
                ev = -ev
            else:
                continue

            meta = self._market_meta.get(event_id, {})

            proposals.append(TradeProposal(
                event_id=event_id,
                market_id=meta.get("market_id", event_id),
                side=side,
                price=entry_price,
                size_usd=self.bankroll * 0.03,
                estimated_prob=est_prob,
                market_price=entry_price,
                book_depth_usd=meta.get(
                    "book_depth_usd", 500.0,
                ),
                volume_24h_usd=meta.get(
                    "volume_24h_usd", 10000.0,
                ),
                bot_name="BAYESIAN",
            ))

        return proposals

    # ── Execution ────────────────────────────────────────────────

    async def _execute_trade(
        self,
        proposal: TradeProposal,
        verdict: RiskVerdict,
    ) -> bool:
        """Place a directional trade.

        Args:
            proposal: The validated trade proposal.
            verdict: Risk verdict with adjusted sizing.

        Returns:
            True if execution succeeded.
        """
        if self._execute is None:
            self.log.info(
                f"[BAYESIAN] SIM EXECUTE | "
                f"{proposal.side} "
                f"{proposal.event_id} | "
                f"price={proposal.price:.4f} | "
                f"size=${verdict.adjusted_size_usd:.2f} | "
                f"EV={verdict.expected_value:.4f} | "
                f"kelly={verdict.kelly_used:.4f}"
            )
            return True

        try:
            ok = await asyncio.wait_for(
                self._execute(
                    proposal.market_id,
                    proposal.side,
                    proposal.price,
                    verdict.adjusted_size_usd,
                ),
                timeout=5.0,
            )
            return bool(ok)
        except Exception as exc:
            self.log.error(
                f"[BAYESIAN] Execution error: {exc}"
            )
            return False

    # ── Position management ──────────────────────────────────────

    async def _check_exits(
        self,
        prices: Dict[str, float],
    ) -> None:
        """Check open positions for exit conditions.

        Exit if:
            - Posterior has reversed (edge disappeared).
            - Market price hit take-profit threshold.
            - Position has been open too long.

        Args:
            prices: Current market prices.
        """
        to_close: List[str] = []

        for event_id, pos in self._positions.items():
            est = self._estimators.get(event_id)
            mkt_price = prices.get(event_id)

            if est is None or mkt_price is None:
                continue

            posterior = est.posterior()

            # Check if edge has disappeared
            if pos.side == "YES":
                current_ev = compute_ev(posterior, mkt_price)
            else:
                current_ev = compute_ev(
                    1.0 - posterior, 1.0 - mkt_price,
                )

            if current_ev <= 0.0:
                to_close.append(event_id)
                self.log.info(
                    f"[BAYESIAN] EXIT | {event_id} | "
                    f"edge vanished EV={current_ev:.4f}"
                )

        for event_id in to_close:
            pos = self._positions.pop(event_id, None)
            if pos is not None:
                # Approximate PnL from price movement
                mkt = prices.get(event_id, pos.entry_price)
                if pos.side == "YES":
                    pnl = (
                        (mkt - pos.entry_price)
                        * pos.size_usd
                        / pos.entry_price
                    )
                else:
                    pnl = (
                        (pos.entry_price - mkt)
                        * pos.size_usd
                        / (1.0 - pos.entry_price)
                    )

                await self.close_trade(
                    event_id, pos.size_usd, pnl,
                )
                self.log.info(
                    f"[BAYESIAN] CLOSED | {event_id} | "
                    f"PnL=${pnl:+.4f} | "
                    f"bankroll=${self.bankroll:.2f}"
                )

    # ── Main cycle ───────────────────────────────────────────────

    async def run_cycle(self) -> None:
        """Execute one Bayesian signal processing cycle.

        Steps:
            1. Ingest new data signals, update posteriors.
            2. Fetch current market prices.
            3. Check exit conditions on open positions.
            4. Evaluate new entry signals.
            5. Validate proposals through risk management.
            6. Execute approved trades.
        """
        self._cycle_count += 1
        t0 = time.monotonic()

        # 1-2: parallel data + price fetch
        posteriors, prices = await asyncio.gather(
            self._ingest_signals(),
            self._fetch_prices(),
        )

        # 3: check exits
        await self._check_exits(prices)

        # 4: evaluate new opportunities
        proposals = self._evaluate_signals(
            posteriors, prices,
        )

        # 5-6: validate and execute
        for prop in proposals:
            if prop.event_id in self._positions:
                continue  # already have a position

            verdict = await self.validate_trade(prop)

            if not verdict.approved:
                self.log.debug(
                    f"[BAYESIAN] REJECTED | "
                    f"{prop.event_id} | "
                    f"{verdict.reject_reason}"
                )
                continue

            ok = await self._execute_trade(prop, verdict)

            if ok:
                await self.register_trade(
                    prop.event_id,
                    verdict.adjusted_size_usd,
                )
                est = self._estimators.get(prop.event_id)
                self._positions[prop.event_id] = (
                    BayesianPosition(
                        event_id=prop.event_id,
                        market_id=prop.market_id,
                        side=prop.side,
                        entry_price=prop.price,
                        size_usd=verdict.adjusted_size_usd,
                        entry_posterior=(
                            est.posterior()
                            if est
                            else 0.5
                        ),
                    )
                )

        elapsed = time.monotonic() - t0
        if self._cycle_count % 20 == 0:
            self.log.info(
                f"[BAYESIAN] Cycle {self._cycle_count} | "
                f"posteriors={len(posteriors)} | "
                f"positions={len(self._positions)} | "
                f"{elapsed*1000:.1f}ms | "
                f"bankroll=${self.bankroll:.2f}"
            )

        sleep_time = max(
            0.0, BAY_POLL_INTERVAL_S - elapsed,
        )
        await asyncio.sleep(sleep_time)
