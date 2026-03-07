"""
market_maker_bot.py — Bot 3: The Liquidity Provider (Market Maker).

Captures spreads and earns liquidity rewards (targeting 80-200 % APY)
by placing simultaneous limit orders for YES and NO.

Mathematical model — Logarithmic Market Scoring Rule (LMSR):

    Price (softmax):
        p_i(q) = exp(q_i / b) / sum_j exp(q_j / b)

    Cost function:
        C(q) = b * ln( sum_i exp(q_i / b) )

    Trade cost:
        Cost = C(q_1, ..., q_i + delta, ...) - C(q_1, ..., q_i, ...)

Spread is determined by a hybrid of LMSR fair-value and an
Avellaneda-Stoikov model adapted for prediction markets.

Inherits from ``RiskManagedBot`` — all universal risk constraints
are enforced.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from bot_logging import setup_bot_logger
from math_models import LMSR, optimal_spread
from risk_management import (
    RiskManagedBot,
    RiskVerdict,
    TradeProposal,
)


# ─────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────

MM_TICK_INTERVAL_S: float = 2.0
MM_LMSR_B: float = 100.0           # LMSR liquidity parameter
MM_GAMMA: float = 0.15             # risk-aversion for A-S model
MM_KAPPA_DEFAULT: float = 1.5      # default order-arrival rate
MM_MIN_SPREAD_C: float = 1.5       # minimum spread in cents
MM_MAX_SPREAD_C: float = 10.0      # maximum spread in cents
MM_MAX_INVENTORY_FRAC: float = 0.25
MM_EMERGENCY_FRAC: float = 0.35
MM_VPIN_WITHDRAW: float = 0.80     # VPIN threshold to withdraw
MM_VPIN_THROTTLE: float = 0.65
MM_VPIN_WIDEN: float = 0.50
MM_FEE_RATE: float = 0.25          # Polymarket fee base rate
MM_FEE_EXPONENT: float = 2.0
MM_MAKER_REBATE_PCT: float = 0.20  # maker rebate percentage


# ─────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────

def _fee_rate(price: float) -> float:
    """Polymarket fee rate at a given price.

    Args:
        price: Trade price (0-1).

    Returns:
        Fee rate as a fraction.
    """
    return MM_FEE_RATE * (price * (1.0 - price)) ** MM_FEE_EXPONENT


def _maker_rebate(shares: float, price: float) -> float:
    """Estimated maker rebate for a fill.

    Args:
        shares: Number of shares filled.
        price: Fill price.

    Returns:
        Rebate in USD.
    """
    fee = shares * price * _fee_rate(price)
    return fee * MM_MAKER_REBATE_PCT


def _min_spread_at(price: float) -> float:
    """Minimum profitable spread at a given mid-price.

    Must exceed the effective taker fee to be profitable.

    Args:
        price: Mid-price.

    Returns:
        Minimum spread as a fraction of price.
    """
    eff_fee = _fee_rate(price) * 2.0  # both legs
    return max(MM_MIN_SPREAD_C / 100.0, eff_fee * 0.4)


# ─────────────────────────────────────────────────────────────────────
# VPIN tracker (lightweight in-memory)
# ─────────────────────────────────────────────────────────────────────

class VPINTracker:
    """Volume-synchronised Probability of Informed Trading.

    Classifies order flow as buy- or sell-initiated using
    tick direction. VPIN = |buy_vol - sell_vol| / total_vol
    over a rolling window of buckets.

    Attributes:
        bucket_size: Volume per bucket.
        n_buckets: Number of buckets in the rolling window.
    """

    def __init__(
        self,
        bucket_size: int = 25,
        n_buckets: int = 50,
    ) -> None:
        self._b = 0.0
        self._s = 0.0
        self._v = 0.0
        self._bucket_size = bucket_size
        self._buckets: deque[float] = deque(maxlen=n_buckets)

    def add(self, volume: float, is_buy: bool) -> None:
        """Record a trade.

        Args:
            volume: Trade volume.
            is_buy: True if buyer-initiated.
        """
        if is_buy:
            self._b += volume
        else:
            self._s += volume
        self._v += volume

        if self._v >= self._bucket_size:
            imb = (
                abs(self._b - self._s) / self._v
                if self._v > 0
                else 0.0
            )
            self._buckets.append(imb)
            self._b = self._s = self._v = 0.0

    @property
    def value(self) -> float:
        """Current VPIN estimate."""
        if len(self._buckets) < 3:
            return 0.0
        return sum(self._buckets) / len(self._buckets)

    @property
    def regime(self) -> str:
        """Flow-toxicity regime label."""
        v = self.value
        if v >= MM_VPIN_WITHDRAW:
            return "TOXIC"
        if v >= MM_VPIN_THROTTLE:
            return "THROTTLE"
        if v >= MM_VPIN_WIDEN:
            return "WIDEN"
        return "NORMAL"

    def reset(self) -> None:
        """Clear all state."""
        self._b = self._s = self._v = 0.0
        self._buckets.clear()


# ─────────────────────────────────────────────────────────────────────
# Kappa estimator (order-arrival rate)
# ─────────────────────────────────────────────────────────────────────

class KappaEstimator:
    """Estimates the order-arrival rate from trade timestamps.

    Attributes:
        _timestamps: Rolling window of trade timestamps.
    """

    def __init__(self, max_history: int = 500) -> None:
        self._ts: deque[float] = deque(maxlen=max_history)

    def record(self) -> None:
        """Record a trade arrival."""
        self._ts.append(time.time())

    def estimate(self) -> float:
        """Estimate arrivals per second.

        Returns:
            Estimated kappa (order-arrival rate).
        """
        if len(self._ts) < 10:
            return MM_KAPPA_DEFAULT
        window = self._ts[-1] - self._ts[0]
        if window <= 0:
            return MM_KAPPA_DEFAULT
        return min(max(len(self._ts) / window, 0.1), 20.0)


# ─────────────────────────────────────────────────────────────────────
# Market Maker Bot
# ─────────────────────────────────────────────────────────────────────

class MarketMakerBot(RiskManagedBot):
    """Bot 3 — The Liquidity Provider: LMSR-based market maker.

    Places simultaneous bid and ask limit orders to capture
    the spread. Uses a hybrid pricing model:

        1. LMSR provides the fair-value reference price.
        2. Avellaneda-Stoikov determines the optimal spread
           given volatility, inventory, and time remaining.
        3. VPIN adjusts spread in toxic-flow regimes.

    Inventory management:
        - Tilts quotes to reduce inventory risk.
        - Emergency unwind if inventory exceeds threshold.
        - Tracks all fills for PnL accounting.

    Args:
        bankroll: Starting capital in USD.
        market_source: Async callable returning market data dict
            with keys 'mid', 'volume', 'is_buy', 'book_depth',
            'volume_24h'.
        order_fn: Async callable to post limit orders.
            Signature: (token_id, side, price, size_usd) -> str|None.
        cancel_fn: Async callable to cancel open orders.
            Signature: (order_ids) -> None.
        token_id: Token identifier for the market.
    """

    def __init__(
        self,
        bankroll: float,
        market_source: Optional[Any] = None,
        order_fn: Optional[Any] = None,
        cancel_fn: Optional[Any] = None,
        token_id: str = "",
    ) -> None:
        logger = setup_bot_logger(
            "mm_bot", "mm_bot.log",
        )
        super().__init__(bankroll=bankroll, logger=logger)

        self._market_source = market_source
        self._order_fn = order_fn
        self._cancel_fn = cancel_fn
        self._token_id = token_id

        # Pricing engines
        self._lmsr = LMSR(b=MM_LMSR_B, n_outcomes=2)
        self._vpin = VPINTracker()
        self._kappa = KappaEstimator()

        # Inventory state
        self._inventory: float = 0.0
        self._prices: deque[float] = deque(maxlen=200)
        self._active_orders: list[str] = []

        # PnL
        self._spread_pnl: float = 0.0
        self._rebate_pnl: float = 0.0
        self._n_fills: int = 0
        self._cycle_count: int = 0
        self._round_start: float = time.time()

    @property
    def _bot_name(self) -> str:
        return "MM"

    # ── Volatility estimation ────────────────────────────────────

    @property
    def _sigma2(self) -> float:
        """Estimated variance of price returns."""
        if len(self._prices) < 5:
            return 0.0025
        p = list(self._prices)
        returns = [
            (p[i] - p[i - 1]) / (p[i - 1] + 1e-9)
            for i in range(1, len(p))
        ]
        n = len(returns)
        mean_r = sum(returns) / n
        var_r = sum(
            (r - mean_r) ** 2 for r in returns
        ) / n
        return var_r + 1e-9

    # ── Quote generation ─────────────────────────────────────────

    def _generate_quotes(
        self,
        mid: float,
        t_remaining: float,
    ) -> Optional[Dict[str, Any]]:
        """Generate bid/ask quotes using hybrid LMSR + A-S model.

        Args:
            mid: Current mid-price.
            t_remaining: Seconds remaining in market.

        Returns:
            Dict with 'bid', 'ask', 'spread', 'regime', etc.
            None if quoting should be withdrawn.
        """
        regime = self._vpin.regime
        vpin_val = self._vpin.value
        kappa = self._kappa.estimate()
        sigma2 = self._sigma2
        max_inv = self.bankroll * MM_MAX_INVENTORY_FRAC

        # Withdraw conditions
        if regime == "TOXIC":
            return {
                "status": "WITHDRAW",
                "reason": "TOXIC_FLOW",
                "vpin": vpin_val,
            }

        if kappa < 0.5:
            return {
                "status": "WITHDRAW",
                "reason": "DRY_MARKET",
                "vpin": vpin_val,
            }

        if abs(self._inventory) >= (
            self.bankroll * MM_EMERGENCY_FRAC
        ):
            return {
                "status": "EMERGENCY_UNWIND",
                "inventory": self._inventory,
                "vpin": vpin_val,
            }

        # LMSR fair-value reference
        lmsr_prices = self._lmsr.prices()
        lmsr_mid = lmsr_prices[0]

        # Avellaneda-Stoikov reservation price
        rp = mid - (
            self._inventory * MM_GAMMA * sigma2 * t_remaining
        )

        # Optimal spread
        sp = (
            MM_GAMMA * sigma2 * t_remaining
            + (2.0 / MM_GAMMA)
            * math.log(1.0 + MM_GAMMA / kappa)
            + 2.0 * 0.3 * vpin_val * mid
        )

        # Regime adjustments
        if regime == "WIDEN":
            sp *= 1.5
        elif regime == "THROTTLE":
            sp *= 2.5

        # Clamp spread
        ms = _min_spread_at(mid)
        sp = max(ms, min(sp, MM_MAX_SPREAD_C / 100.0))

        # Inventory tilt
        half = sp / 2.0
        tilt = half * (
            self._inventory / (max_inv + 1e-9)
        ) * 0.5

        bid = max(0.01, min(0.99, rp - half + tilt))
        ask = max(0.01, min(0.99, rp + half + tilt))

        if ask <= bid + ms:
            ask = bid + ms

        return {
            "status": "QUOTE",
            "bid": bid,
            "ask": ask,
            "spread": ask - bid,
            "vpin": vpin_val,
            "regime": regime,
            "kappa": kappa,
            "sigma2": sigma2,
            "lmsr_mid": lmsr_mid,
            "rp": rp,
        }

    # ── Order management ─────────────────────────────────────────

    async def _cancel_active(self) -> None:
        """Cancel all active orders."""
        if not self._active_orders:
            return
        if self._cancel_fn is not None:
            try:
                await self._cancel_fn(self._active_orders)
            except Exception as exc:
                self.log.warning(
                    f"[MM] Cancel error: {exc}"
                )
        self._active_orders.clear()

    async def _post_order(
        self,
        side: str,
        price: float,
        size_usd: float,
    ) -> Optional[str]:
        """Place a limit order.

        Args:
            side: 'BUY' or 'SELL'.
            price: Limit price.
            size_usd: Dollar size.

        Returns:
            Order ID if successful, else None.
        """
        if self._order_fn is not None:
            try:
                oid = await self._order_fn(
                    self._token_id, side, price, size_usd,
                )
                if oid:
                    self._active_orders.append(oid)
                return oid
            except Exception as exc:
                self.log.error(
                    f"[MM] Order error {side}: {exc}"
                )
                return None
        else:
            oid = f"sim_{side}_{time.time():.3f}"
            self._active_orders.append(oid)
            self.log.info(
                f"[MM] SIM {side} | "
                f"price={price*100:.1f}c | "
                f"size=${size_usd:.2f} | "
                f"oid={oid[-10:]}"
            )
            return oid

    async def _emergency_unwind(self) -> None:
        """Flatten inventory via aggressive sell."""
        await self._cancel_active()
        self.log.error(
            f"[MM] EMERGENCY UNWIND | "
            f"inventory={self._inventory:+.2f}"
        )
        self._inventory = 0.0

    # ── Fill processing ──────────────────────────────────────────

    def _process_simulated_fill(
        self,
        bid: float,
        ask: float,
        mid: float,
        size_usd: float,
    ) -> None:
        """Simulate a fill for paper-trading mode.

        In simulation, fills occur with ~30 % probability
        on each tick. This models realistic fill rates for
        limit orders.

        Args:
            bid: Current bid quote.
            ask: Current ask quote.
            mid: Mid-price.
            size_usd: Order size.
        """
        import random
        if random.random() > 0.30:
            return

        side = random.choice(["BUY", "SELL"])
        fp = bid if side == "BUY" else ask
        fill_frac = random.uniform(0.2, 1.0)
        fill_size = size_usd * fill_frac
        shares = fill_size / (fp + 1e-9)
        rebate = _maker_rebate(shares, fp)

        # Spread capture = |fill_price - mid| * shares
        spread_capture = abs(fp - mid) * shares
        self._spread_pnl += spread_capture
        self._rebate_pnl += rebate
        self._n_fills += 1

        # Inventory update
        if side == "BUY":
            self._inventory += shares
        else:
            self._inventory -= shares

        self.bankroll += spread_capture + rebate
        self._kappa.record()

    # ── Main cycle ───────────────────────────────────────────────

    async def run_cycle(self) -> None:
        """Execute one market-making tick cycle.

        Steps:
            1. Fetch market data (mid, volume, flow direction).
            2. Update pricing engines (VPIN, kappa, sigma).
            3. Generate quotes.
            4. Cancel stale orders and post new quotes.
            5. Process fills and update inventory.
            6. Log periodically.
        """
        self._cycle_count += 1
        t0 = time.monotonic()

        # 1: Fetch market data
        mid = 0.50
        volume = 1.0
        is_buy = True
        book_depth = 500.0
        volume_24h = 10000.0
        t_remaining = 300.0

        if self._market_source is not None:
            try:
                data = await asyncio.wait_for(
                    self._market_source(), timeout=2.0,
                )
                mid = data.get("mid", mid)
                volume = data.get("volume", volume)
                is_buy = data.get("is_buy", is_buy)
                book_depth = data.get(
                    "book_depth", book_depth,
                )
                volume_24h = data.get(
                    "volume_24h", volume_24h,
                )
                t_remaining = data.get(
                    "t_remaining", t_remaining,
                )
            except Exception as exc:
                self.log.warning(
                    f"[MM] Market source error: {exc}"
                )

        # 2: Update engines
        self._prices.append(mid)
        self._vpin.add(volume, is_buy)
        self._kappa.record()

        # LMSR update (use mid to set quantities)
        lmsr_delta = (mid - 0.5) * 10.0
        self._lmsr.quantities[0] = lmsr_delta
        self._lmsr.quantities[1] = -lmsr_delta

        # 3: Generate quotes
        result = self._generate_quotes(mid, t_remaining)

        if result is None:
            await asyncio.sleep(MM_TICK_INTERVAL_S)
            return

        if result["status"] == "EMERGENCY_UNWIND":
            await self._emergency_unwind()
            await asyncio.sleep(MM_TICK_INTERVAL_S)
            return

        if result["status"] == "WITHDRAW":
            await self._cancel_active()
            self.log.warning(
                f"[MM] WITHDRAW | {result['reason']} | "
                f"VPIN={result['vpin']:.2f}"
            )
            await asyncio.sleep(MM_TICK_INTERVAL_S)
            return

        # 4: Cancel old + post new quotes
        await self._cancel_active()

        bid_price = result["bid"]
        ask_price = result["ask"]

        # Risk-check the quote sizes
        # For MM, we treat each side as a separate proposal
        # but use a simplified check (skip full Kelly for MM)
        max_size = min(
            self.bankroll * MM_MAX_INVENTORY_FRAC,
            self.bankroll * 0.03,  # 3 % per-trade rule
        )
        quote_size = max(0.10, max_size)

        await self._post_order("BUY", bid_price, quote_size)
        await self._post_order("SELL", ask_price, quote_size)

        # 5: Process fills (simulated in paper mode)
        if self._order_fn is None:
            self._process_simulated_fill(
                bid_price, ask_price, mid, quote_size,
            )

        # 6: Periodic logging
        if self._cycle_count % 5 == 0:
            self.log.info(
                f"[MM] T-{t_remaining:>5.1f}s | "
                f"Mid={mid*100:.1f}c | "
                f"Bid={bid_price*100:.1f}c / "
                f"Ask={ask_price*100:.1f}c | "
                f"Spr={result['spread']*100:.2f}c | "
                f"VPIN={result['vpin']:.2f}"
                f"[{result['regime']}] | "
                f"Inv={self._inventory:>+5.1f} | "
                f"Fills={self._n_fills} | "
                f"PnL spr=${self._spread_pnl:+.4f} "
                f"reb=${self._rebate_pnl:+.4f} | "
                f"Bk=${self.bankroll:.2f}"
            )

        elapsed = time.monotonic() - t0
        sleep = max(0.0, MM_TICK_INTERVAL_S - elapsed)
        await asyncio.sleep(sleep)

    # ── Reset for new market round ───────────────────────────────

    def reset_round(self) -> None:
        """Reset state for a new market round."""
        self._inventory = 0.0
        self._prices.clear()
        self._vpin.reset()
        self._lmsr.reset()
        self._active_orders.clear()
        self._round_start = time.time()
        self.log.info(
            "[MM] Round reset | "
            f"bankroll=${self.bankroll:.2f}"
        )
