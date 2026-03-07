"""
main.py — Orchestrator for the three prediction-market trading bots.

Runs all three bots concurrently via asyncio:

    Bot 1: ArbitrageBot    ("The PEG")
    Bot 2: BayesianBot     ("The Bayesian Signal")
    Bot 3: MarketMakerBot  ("The Liquidity Provider")

Each bot writes to its own log file and shares the universal
risk-management constraints via the RiskManagedBot base class.

Usage:
    python main.py                     # paper-trading, $100 bankroll
    python main.py --bankroll 1000     # custom bankroll
    python main.py --live              # live mode (requires adapters)

Example:
    $ python main.py
    [INFO] Starting prediction-market bot fleet
    [INFO] ARB      → arbitrage_bot.log
    [INFO] BAYESIAN → bayesian_bot.log
    [INFO] MM       → mm_bot.log
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import math
import random
import signal
import sys
import time
from typing import Any, Dict, List, Optional

from arbitrage_bot import ArbitrageBot, MarketQuote
from bayesian_bot import (
    BayesianBot,
    DataSignal,
    price_tick_signal,
    volume_imbalance_signal,
)
from bot_logging import setup_bot_logger
from market_maker_bot import MarketMakerBot
from math_models import BayesianEstimator, LMSR, compute_ev
from risk_management import RiskManagedBot


# ─────────────────────────────────────────────────────────────────────
# Simulated data sources (for paper-trading / demo mode)
# ─────────────────────────────────────────────────────────────────────

class SimulatedMarket:
    """Generates realistic simulated market data for all bots.

    Simulates a binary prediction market with mean-reverting
    mid-price, occasional regime shifts, and stochastic volume.

    Attributes:
        mid: Current mid-price (0-1).
        event_id: Simulated event identifier.
    """

    def __init__(
        self,
        initial_mid: float = 0.50,
        event_id: str = "SIM_EVENT_001",
    ) -> None:
        self.mid = initial_mid
        self.event_id = event_id
        self._regime = "NORMAL"
        self._regime_ticks = 0
        self._prev_mid = initial_mid
        self._start = time.time()

    def tick(self) -> dict:
        """Advance one tick and return market data.

        Returns:
            Dict with mid, volume, is_buy, book_depth,
            volume_24h, t_remaining.
        """
        # Regime shifts
        if self._regime_ticks <= 0:
            self._regime = random.choice(
                ["NORMAL"] * 85 + ["INFORMED"] * 15
            )
            self._regime_ticks = random.randint(10, 60)
        self._regime_ticks -= 1

        # Price evolution
        drift = -0.001 * (self.mid - 0.5)  # mean-reversion
        noise = random.gauss(0, 0.002)
        if self._regime == "INFORMED":
            noise += random.choice([-0.003, 0.003])

        self._prev_mid = self.mid
        self.mid = max(0.05, min(0.95, self.mid + drift + noise))

        volume = max(0.5, random.expovariate(1.0) + 0.5)
        is_buy = (
            True
            if self._regime == "INFORMED"
            else random.random() > 0.5
        )

        elapsed = time.time() - self._start
        t_remaining = max(0.0, 300.0 - (elapsed % 300.0))

        return {
            "mid": self.mid,
            "volume": volume,
            "is_buy": is_buy,
            "book_depth": random.uniform(200, 2000),
            "volume_24h": random.uniform(5000, 50000),
            "t_remaining": t_remaining,
        }


_sim = SimulatedMarket()


async def sim_arb_feed_a() -> List[MarketQuote]:
    """Simulated price feed for exchange A."""
    data = _sim.tick()
    spread = random.uniform(0.01, 0.03)
    return [
        MarketQuote(
            exchange="exchange_a",
            event_id=_sim.event_id,
            market_id="MKT_A_001",
            best_bid=data["mid"] - spread / 2,
            best_ask=data["mid"] + spread / 2,
            bid_size_usd=data["book_depth"],
            ask_size_usd=data["book_depth"],
            volume_24h_usd=data["volume_24h"],
        ),
    ]


async def sim_arb_feed_b() -> List[MarketQuote]:
    """Simulated price feed for exchange B with slight offset."""
    data = _sim.tick()
    offset = random.gauss(0, 0.01)
    spread = random.uniform(0.01, 0.03)
    mid = data["mid"] + offset
    return [
        MarketQuote(
            exchange="exchange_b",
            event_id=_sim.event_id,
            market_id="MKT_B_001",
            best_bid=mid - spread / 2,
            best_ask=mid + spread / 2,
            bid_size_usd=data["book_depth"] * 0.8,
            ask_size_usd=data["book_depth"] * 0.8,
            volume_24h_usd=data["volume_24h"] * 0.7,
        ),
    ]


async def sim_bayesian_data() -> List[DataSignal]:
    """Simulated data signals for the Bayesian bot."""
    data = _sim.tick()
    signals: List[DataSignal] = []

    # Price-tick signal
    sig = price_tick_signal(
        data["mid"],
        _sim._prev_mid,
        volatility=0.02,
    )
    sig.metadata["event_id"] = _sim.event_id
    signals.append(sig)

    # Volume imbalance signal
    buy_v = data["volume"] if data["is_buy"] else 0.0
    sell_v = 0.0 if data["is_buy"] else data["volume"]
    sig2 = volume_imbalance_signal(
        buy_v + random.uniform(0, 1),
        sell_v + random.uniform(0, 1),
    )
    sig2.metadata["event_id"] = _sim.event_id
    signals.append(sig2)

    return signals


async def sim_bayesian_prices() -> Dict[str, float]:
    """Simulated price source for the Bayesian bot."""
    return {_sim.event_id: _sim.mid}


async def sim_mm_market() -> dict:
    """Simulated market data for the Market Maker."""
    return _sim.tick()


# ─────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────

async def run_fleet(bankroll: float) -> None:
    """Run all three bots concurrently.

    Args:
        bankroll: Starting capital split across bots.
    """
    main_log = setup_bot_logger(
        "fleet", "fleet.log", console=True,
    )

    # Split bankroll: 20% arb, 30% bayesian, 50% MM
    arb_bk = bankroll * 0.20
    bay_bk = bankroll * 0.30
    mm_bk = bankroll * 0.50

    main_log.info("=" * 64)
    main_log.info(
        "PREDICTION MARKET BOT FLEET v1.0"
    )
    main_log.info("=" * 64)
    main_log.info(
        f"Total bankroll: ${bankroll:.2f}"
    )
    main_log.info(
        f"  ARB (The PEG):            ${arb_bk:.2f}"
    )
    main_log.info(
        f"  BAYESIAN (The Signal):    ${bay_bk:.2f}"
    )
    main_log.info(
        f"  MM (Liquidity Provider):  ${mm_bk:.2f}"
    )
    main_log.info("-" * 64)
    main_log.info("Risk Management:")
    main_log.info("  3-5-7 Rule: 3% per trade / 5% total / 7% min EV")
    main_log.info("  Fractional Kelly: capped at 0.25 (NEVER full Kelly)")
    main_log.info("  Tail filter: min depth $50 / min vol $500")
    main_log.info("  Diversification: 1 trade per event")
    main_log.info("-" * 64)
    main_log.info("Log files:")
    main_log.info("  arbitrage_bot.log")
    main_log.info("  bayesian_bot.log")
    main_log.info("  mm_bot.log")
    main_log.info("=" * 64)

    # Instantiate bots
    arb_bot = ArbitrageBot(
        bankroll=arb_bk,
        market_feeds={
            "exchange_a": sim_arb_feed_a,
            "exchange_b": sim_arb_feed_b,
        },
    )

    bay_bot = BayesianBot(
        bankroll=bay_bk,
        data_source=sim_bayesian_data,
        price_source=sim_bayesian_prices,
        market_meta={
            _sim.event_id: {
                "market_id": "MKT_BAY_001",
                "book_depth_usd": 1000.0,
                "volume_24h_usd": 20000.0,
            },
        },
    )

    mm_bot = MarketMakerBot(
        bankroll=mm_bk,
        market_source=sim_mm_market,
        token_id="SIM_TOKEN_YES",
    )

    # Run concurrently
    tasks = [
        asyncio.create_task(
            arb_bot.start(), name="arb",
        ),
        asyncio.create_task(
            bay_bot.start(), name="bayesian",
        ),
        asyncio.create_task(
            mm_bot.start(), name="mm",
        ),
    ]

    # Graceful shutdown handler
    shutdown_event = asyncio.Event()

    def _handle_shutdown() -> None:
        main_log.info("Shutdown signal received")
        shutdown_event.set()
        for t in tasks:
            t.cancel()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_shutdown)
        except NotImplementedError:
            pass

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        pass

    main_log.info("=" * 64)
    main_log.info(
        f"FLEET SHUTDOWN | "
        f"ARB=${arb_bot.bankroll:.2f} | "
        f"BAY=${bay_bot.bankroll:.2f} | "
        f"MM=${mm_bot.bankroll:.2f} | "
        f"TOTAL=${arb_bot.bankroll + bay_bot.bankroll + mm_bot.bankroll:.2f}"
    )
    main_log.info("=" * 64)


def main() -> None:
    """Parse arguments and launch the bot fleet."""
    parser = argparse.ArgumentParser(
        description="Prediction Market Bot Fleet",
    )
    parser.add_argument(
        "--bankroll",
        type=float,
        default=100.0,
        help="Starting bankroll in USD (default: 100)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Run in live mode (requires adapters)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(run_fleet(args.bankroll))
    except KeyboardInterrupt:
        print("\nBot fleet stopped.")


if __name__ == "__main__":
    main()
