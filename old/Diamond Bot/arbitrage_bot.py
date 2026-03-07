"""
arbitrage_bot.py — Bot 1: The PEG (Arbitrage Bot).

Scans for and exploits price discrepancies between prediction
markets (e.g. Polymarket vs. Kalshi) or correlated contracts on
the same exchange.

Strategy:
    Buy a contract at a lower price on Market A and simultaneously
    sell at a higher price on Market B to lock in a risk-free profit
    minus fees.

The bot inherits from ``RiskManagedBot`` so the 3-5-7 rule,
Fractional Kelly, tail-market filter, and diversification guard
are enforced on every opportunity.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from bot_logging import setup_bot_logger
from math_models import compute_ev
from risk_management import (
    RiskManagedBot,
    RiskVerdict,
    TradeProposal,
)


# ─────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────

ARB_POLL_INTERVAL_S: float = 0.5
ARB_MIN_SPREAD_PCT: float = 0.02   # 2 % minimum cross-spread
ARB_FEE_PER_LEG: float = 0.005     # 0.5 % estimated fee per leg
ARB_CYCLE_TIMEOUT_S: float = 300.0  # one 5-min cycle


# ─────────────────────────────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────────────────────────────

@dataclass
class MarketQuote:
    """Snapshot of a single market's best prices.

    Attributes:
        exchange: Exchange name ('polymarket', 'kalshi').
        event_id: Unique event identifier.
        market_id: Market / contract identifier.
        best_bid: Best bid price.
        best_ask: Best ask price.
        bid_size_usd: Depth at best bid.
        ask_size_usd: Depth at best ask.
        volume_24h_usd: 24 h volume.
        timestamp: When quote was captured.
    """

    exchange: str
    event_id: str
    market_id: str
    best_bid: float
    best_ask: float
    bid_size_usd: float = 500.0
    ask_size_usd: float = 500.0
    volume_24h_usd: float = 10000.0
    timestamp: float = field(default_factory=time.time)


@dataclass
class ArbOpportunity:
    """An identified arbitrage opportunity.

    Attributes:
        buy_quote: Market where we buy (lower ask).
        sell_quote: Market where we sell (higher bid).
        gross_spread: bid_sell - ask_buy.
        net_spread: gross_spread - estimated fees.
        ev: Expected value as fraction of notional.
    """

    buy_quote: MarketQuote
    sell_quote: MarketQuote
    gross_spread: float
    net_spread: float
    ev: float


# ─────────────────────────────────────────────────────────────────────
# Arbitrage Bot
# ─────────────────────────────────────────────────────────────────────

class ArbitrageBot(RiskManagedBot):
    """Bot 1 — The PEG: cross-market arbitrage scanner.

    Continuously polls price feeds from multiple exchanges,
    identifies mispricings, and executes simultaneous buy/sell
    orders to capture risk-free spread.

    The bot uses the shared risk management from RiskManagedBot:
        - 3 % single-trade cap
        - 5 % total-exposure cap
        - 7 % minimum EV gate
        - Fractional Kelly (capped at 0.25)
        - Tail-market liquidity filter
        - One trade per event (diversification)

    Args:
        bankroll: Starting capital in USD.
        market_feeds: Dict mapping exchange name to an async
            callable that returns List[MarketQuote].
        execute_fn: Async callable to place orders.
            Signature: (exchange, market_id, side, price, size)
            -> bool.
    """

    def __init__(
        self,
        bankroll: float,
        market_feeds: Optional[
            Dict[str, Any]
        ] = None,
        execute_fn: Optional[Any] = None,
    ) -> None:
        logger = setup_bot_logger(
            "arbitrage_bot", "arbitrage_bot.log"
        )
        super().__init__(bankroll=bankroll, logger=logger)
        self._feeds = market_feeds or {}
        self._execute = execute_fn
        self._cycle_count: int = 0

    @property
    def _bot_name(self) -> str:
        return "ARB"

    # ── Price scanning ───────────────────────────────────────────

    async def _fetch_quotes(self) -> Dict[str, List[MarketQuote]]:
        """Fetch latest quotes from all configured feeds.

        Returns:
            Dict mapping exchange -> list of MarketQuotes.
        """
        results: Dict[str, List[MarketQuote]] = {}
        tasks = {}
        for name, feed_fn in self._feeds.items():
            tasks[name] = asyncio.create_task(feed_fn())

        for name, task in tasks.items():
            try:
                results[name] = await asyncio.wait_for(
                    task, timeout=5.0,
                )
            except Exception as exc:
                self.log.warning(
                    f"[ARB] Feed {name} error: {exc}"
                )
                results[name] = []

        return results

    def _find_opportunities(
        self,
        quotes: Dict[str, List[MarketQuote]],
    ) -> List[ArbOpportunity]:
        """Identify cross-market arbitrage opportunities.

        Pairs up quotes across exchanges for the same event_id.
        An opportunity exists when one exchange's best bid exceeds
        another's best ask by more than total fees.

        Args:
            quotes: Exchange -> quote list mapping.

        Returns:
            List of viable ArbOpportunity objects.
        """
        # Index quotes by event_id
        by_event: Dict[
            str, List[MarketQuote]
        ] = {}
        for exchange_quotes in quotes.values():
            for q in exchange_quotes:
                by_event.setdefault(q.event_id, []).append(q)

        opps: List[ArbOpportunity] = []
        for event_id, q_list in by_event.items():
            if len(q_list) < 2:
                continue
            # Check all pairs
            for i, qa in enumerate(q_list):
                for qb in q_list[i + 1:]:
                    # Direction 1: buy on A, sell on B
                    opp = self._check_pair(qa, qb)
                    if opp is not None:
                        opps.append(opp)
                    # Direction 2: buy on B, sell on A
                    opp = self._check_pair(qb, qa)
                    if opp is not None:
                        opps.append(opp)

        # Sort by net spread descending
        opps.sort(key=lambda o: o.net_spread, reverse=True)
        return opps

    def _check_pair(
        self,
        buy_q: MarketQuote,
        sell_q: MarketQuote,
    ) -> Optional[ArbOpportunity]:
        """Check if buying on buy_q and selling on sell_q is profitable.

        Args:
            buy_q: Quote from the exchange where we buy.
            sell_q: Quote from the exchange where we sell.

        Returns:
            ArbOpportunity if net profitable, else None.
        """
        if buy_q.exchange == sell_q.exchange:
            # Same-exchange peg (complementary contracts)
            # buy YES @ ask + buy NO @ ask < 1.0
            gross = 1.0 - (buy_q.best_ask + sell_q.best_ask)
        else:
            # Cross-exchange: sell bid - buy ask
            gross = sell_q.best_bid - buy_q.best_ask

        if gross <= 0.0:
            return None

        total_fee = ARB_FEE_PER_LEG * 2.0
        net = gross - total_fee

        if net < ARB_MIN_SPREAD_PCT:
            return None

        # EV for arb is approximately the net spread
        # (since it's "risk-free" minus execution risk)
        return ArbOpportunity(
            buy_quote=buy_q,
            sell_quote=sell_q,
            gross_spread=gross,
            net_spread=net,
            ev=net,
        )

    # ── Execution ────────────────────────────────────────────────

    async def _execute_arb(
        self,
        opp: ArbOpportunity,
        size_usd: float,
    ) -> bool:
        """Execute both legs of an arbitrage trade.

        Attempts simultaneous buy and sell. If one leg fails,
        logs the error (production would need unwind logic).

        Args:
            opp: The arbitrage opportunity to execute.
            size_usd: Dollar size for each leg.

        Returns:
            True if both legs succeeded.
        """
        if self._execute is None:
            # Simulation mode
            self.log.info(
                f"[ARB] SIM EXECUTE | "
                f"BUY {opp.buy_quote.exchange} "
                f"@ {opp.buy_quote.best_ask:.4f} | "
                f"SELL {opp.sell_quote.exchange} "
                f"@ {opp.sell_quote.best_bid:.4f} | "
                f"size=${size_usd:.2f} | "
                f"net_spread={opp.net_spread:.4f}"
            )
            return True

        buy_task = asyncio.create_task(
            self._execute(
                opp.buy_quote.exchange,
                opp.buy_quote.market_id,
                "BUY",
                opp.buy_quote.best_ask,
                size_usd,
            )
        )
        sell_task = asyncio.create_task(
            self._execute(
                opp.sell_quote.exchange,
                opp.sell_quote.market_id,
                "SELL",
                opp.sell_quote.best_bid,
                size_usd,
            )
        )

        buy_ok, sell_ok = await asyncio.gather(
            buy_task, sell_task, return_exceptions=True,
        )

        success = (
            isinstance(buy_ok, bool)
            and buy_ok
            and isinstance(sell_ok, bool)
            and sell_ok
        )

        if success:
            self.log.info(
                f"[ARB] EXECUTED | "
                f"{opp.buy_quote.exchange}->"
                f"{opp.sell_quote.exchange} | "
                f"net=${opp.net_spread * size_usd:.4f}"
            )
        else:
            self.log.warning(
                f"[ARB] PARTIAL FILL | "
                f"buy={buy_ok} sell={sell_ok}"
            )

        return success

    # ── Main cycle ───────────────────────────────────────────────

    async def run_cycle(self) -> None:
        """Execute one full arbitrage scanning cycle.

        Steps:
            1. Fetch quotes from all exchanges.
            2. Identify cross-market mispricings.
            3. Validate against risk management.
            4. Execute approved opportunities.
            5. Sleep until next poll.
        """
        self._cycle_count += 1
        t0 = time.monotonic()

        quotes = await self._fetch_quotes()

        total_quotes = sum(len(v) for v in quotes.values())
        if total_quotes == 0:
            self.log.debug("[ARB] No quotes available")
            await asyncio.sleep(ARB_POLL_INTERVAL_S)
            return

        opportunities = self._find_opportunities(quotes)

        for opp in opportunities:
            # Build proposal for risk management
            proposal = TradeProposal(
                event_id=opp.buy_quote.event_id,
                market_id=opp.buy_quote.market_id,
                side="ARB",
                price=opp.buy_quote.best_ask,
                size_usd=self.bankroll * 0.03,
                estimated_prob=0.5 + opp.ev,
                market_price=0.5,
                book_depth_usd=min(
                    opp.buy_quote.ask_size_usd,
                    opp.sell_quote.bid_size_usd,
                ),
                volume_24h_usd=min(
                    opp.buy_quote.volume_24h_usd,
                    opp.sell_quote.volume_24h_usd,
                ),
                bot_name="ARB",
            )

            verdict = await self.validate_trade(proposal)

            if not verdict.approved:
                self.log.debug(
                    f"[ARB] REJECTED | "
                    f"{verdict.reject_reason} | "
                    f"spread={opp.net_spread:.4f}"
                )
                continue

            ok = await self._execute_arb(
                opp, verdict.adjusted_size_usd,
            )
            if ok:
                pnl = (
                    opp.net_spread
                    * verdict.adjusted_size_usd
                )
                await self.register_trade(
                    opp.buy_quote.event_id,
                    verdict.adjusted_size_usd,
                )
                # Arb is closed immediately; book PnL
                await self.close_trade(
                    opp.buy_quote.event_id,
                    verdict.adjusted_size_usd,
                    pnl,
                )
                self.log.info(
                    f"[ARB] PnL=${pnl:+.4f} | "
                    f"bankroll=${self.bankroll:.2f}"
                )

        elapsed = time.monotonic() - t0
        self.log.debug(
            f"[ARB] Cycle {self._cycle_count} | "
            f"{len(opportunities)} opps | "
            f"{elapsed*1000:.1f}ms"
        )

        sleep_time = max(
            0.0, ARB_POLL_INTERVAL_S - elapsed,
        )
        await asyncio.sleep(sleep_time)
