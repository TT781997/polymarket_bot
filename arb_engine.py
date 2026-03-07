"""
arb_engine.py — Risk-Free Arbitrage Engine for Binary Prediction Markets
=========================================================================

Production-grade arbitrage execution logic for Polymarket binary markets.

Data Models:
    OrderBookLevel  — Single price/size level in the order book.
    OrderBookSide   — One side (asks or bids) of the order book.
    ArbOpportunity  — Validated arbitrage opportunity ready for execution.
    ArbRejection    — Reason why an opportunity was rejected.

Core Logic:
    evaluate_arb()  — Main entry point. Evaluates if a risk-free arb exists.
    calc_vwap()     — Volume-Weighted Average Price for deeper liquidity.
    check_liquidity() — Validates available volume at target price levels.

QA:
    All functions include assert-based validation.
    Comprehensive unit tests at bottom of module.

Rules:
    1. ASK-based prices ONLY (no eff_price, no mid, no last traded).
    2. Peg = Lowest_Ask_Up + Lowest_Ask_Down.
    3. Peg < 0.98 strictly enforced (2c margin for gas/slippage).
    4. Liquidity must support target order size at stated prices.
    5. VWAP fallback when top-of-book volume is insufficient.
    6. Equal shares on both sides (one resolves to $1.00/share).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# =============================================================================
# CONSTANTS
# =============================================================================

PEG_TRIGGER = 0.98       # Max Peg for valid arbitrage
RESOLUTION_PAYOUT = 1.00 # Binary market payout per winning share
MIN_SHARES = 0.001       # Minimum shares to bother executing
FEE_RATE_BASE = 0.25     # Polymarket fee rate base
FEE_EXPONENT = 2         # Polymarket fee exponent


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass(frozen=True, slots=True)
class OrderBookLevel:
    """Single price/size level in the order book.

    Attributes:
        price: Price in decimal (0.01 to 0.99). This is the RAW
               ASK price — the actual price you pay when buying.
        size:  Number of shares available at this price level.

    Invariants:
        - price is in (0.0, 1.0) exclusive
        - size > 0
    """
    price: float
    size: float

    def __post_init__(self):
        if not (0.0 < self.price < 1.0):
            raise ValueError(
                f"Price must be in (0, 1), got {self.price}"
            )
        if self.size <= 0:
            raise ValueError(
                f"Size must be > 0, got {self.size}"
            )


@dataclass(slots=True)
class OrderBookSide:
    """One side (asks) of the order book, sorted by price.

    For asks: sorted ascending (lowest/best ask first).
    Levels with size <= 0 are filtered out.

    Attributes:
        levels: List of OrderBookLevel sorted by price ascending.
    """
    levels: list[OrderBookLevel] = field(default_factory=list)

    @property
    def best_price(self) -> Optional[float]:
        """Lowest ask price (best for buyer)."""
        return self.levels[0].price if self.levels else None

    @property
    def best_size(self) -> Optional[float]:
        """Volume available at best ask price."""
        return self.levels[0].size if self.levels else None

    @property
    def is_empty(self) -> bool:
        return len(self.levels) == 0

    def total_volume(self) -> float:
        """Total volume across all levels."""
        return sum(lv.size for lv in self.levels)

    @classmethod
    def from_raw(cls, entries: list[dict]) -> OrderBookSide:
        """Build from raw WS data [{"price": "0.45", "size": "100"}, ...].

        Filters out zero-size entries, sorts ascending by price.
        """
        levels = []
        for e in entries:
            sz = float(e.get("size", 0))
            if sz <= 0:
                continue
            pr = float(e["price"])
            if 0.0 < pr < 1.0:
                levels.append(OrderBookLevel(price=pr, size=sz))
        levels.sort(key=lambda lv: lv.price)
        return cls(levels=levels)


class ArbStatus(Enum):
    """Result status of arbitrage evaluation."""
    OPPORTUNITY = "OPPORTUNITY"
    REJECT_PEG_TOO_HIGH = "REJECT_PEG_TOO_HIGH"
    REJECT_NEGATIVE_PROFIT = "REJECT_NEGATIVE_PROFIT"
    REJECT_NO_LIQUIDITY_UP = "REJECT_NO_LIQUIDITY_UP"
    REJECT_NO_LIQUIDITY_DOWN = "REJECT_NO_LIQUIDITY_DOWN"
    REJECT_VWAP_BREAKS_PEG = "REJECT_VWAP_BREAKS_PEG"
    REJECT_EMPTY_BOOK = "REJECT_EMPTY_BOOK"
    REJECT_BUDGET_TOO_LOW = "REJECT_BUDGET_TOO_LOW"


@dataclass(frozen=True, slots=True)
class ArbResult:
    """Result of arbitrage evaluation.

    If status == OPPORTUNITY: all fields are populated and
    the trade is ready for execution.

    Attributes:
        status:         Pass/fail status.
        lowest_ask_up:  Best ask price for UP side (raw ASK).
        lowest_ask_down: Best ask price for DOWN side (raw ASK).
        peg:            Sum of both lowest asks.
        gross_margin:   1.00 - peg (before fees).
        shares:         Equal shares to buy on BOTH sides.
        cost_up:        Total cost for UP side (shares × ask + fee).
        cost_down:      Total cost for DOWN side.
        total_cost:     cost_up + cost_down.
        payout:         shares × $1.00 (winner side).
        net_profit:     payout - total_cost.
        profit_pct:     net_profit / total_cost × 100.
        used_vwap:      True if VWAP was needed (top-of-book insufficient).
        vwap_up:        VWAP price for UP if used, else None.
        vwap_down:      VWAP price for DOWN if used, else None.
        volume_at_ask_up:  Available volume at best ask UP.
        volume_at_ask_down: Available volume at best ask DOWN.
        reason:         Human-readable rejection reason if not OPPORTUNITY.
    """
    status: ArbStatus
    lowest_ask_up: float = 0.0
    lowest_ask_down: float = 0.0
    peg: float = 0.0
    gross_margin: float = 0.0
    shares: float = 0.0
    cost_up: float = 0.0
    cost_down: float = 0.0
    total_cost: float = 0.0
    payout: float = 0.0
    net_profit: float = 0.0
    profit_pct: float = 0.0
    used_vwap: bool = False
    vwap_up: Optional[float] = None
    vwap_down: Optional[float] = None
    volume_at_ask_up: float = 0.0
    volume_at_ask_down: float = 0.0
    reason: str = ""


# =============================================================================
# FEE CALCULATION
# =============================================================================

def fee_rate(p: float) -> float:
    """Polymarket fee rate at price p.

    Formula: fee_rate(p) = 0.25 × (p × (1-p))²
    Max at p=0.50: 0.25 × 0.0625 = 0.015625 (1.5625%)
    Zero at p=0 and p=1 (resolution is free).
    """
    return FEE_RATE_BASE * (p * (1.0 - p)) ** FEE_EXPONENT


def cost_with_fee(shares: float, ask: float) -> float:
    """Total cost to buy `shares` at `ask` including fees.

    cost = shares × ask × (1 + fee_rate(ask))
    """
    return shares * ask * (1.0 + fee_rate(ask))


# =============================================================================
# VWAP — Volume-Weighted Average Price
# =============================================================================

def calc_vwap(
    book_side: OrderBookSide,
    target_size: float,
) -> tuple[Optional[float], float]:
    """Calculate VWAP across order book levels for target_size shares.

    Walks through the order book from best price upward,
    accumulating volume until target_size is filled.

    Args:
        book_side: Ask side of the order book (sorted ascending).
        target_size: Number of shares we want to buy.

    Returns:
        (vwap_price, filled_size):
            vwap_price: Volume-weighted average price, or None if
                        book is empty.
            filled_size: Total shares that can actually be filled.
                         May be < target_size if book is thin.

    Example:
        Book: [(0.45, 50), (0.46, 100), (0.47, 200)]
        target_size = 120
        Fills: 50 @ 0.45 + 70 @ 0.46 = 54.70
        VWAP = 54.70 / 120 = 0.4558
    """
    if book_side.is_empty:
        return None, 0.0

    total_cost = 0.0
    filled = 0.0

    for level in book_side.levels:
        remaining = target_size - filled
        if remaining <= 0:
            break
        fill_at_level = min(level.size, remaining)
        total_cost += fill_at_level * level.price
        filled += fill_at_level

    if filled < MIN_SHARES:
        return None, 0.0

    vwap = total_cost / filled
    return vwap, filled


# =============================================================================
# LIQUIDITY CHECK
# =============================================================================

def check_liquidity(
    book_side: OrderBookSide,
    target_size: float,
) -> tuple[bool, float, float]:
    """Check if order book has sufficient liquidity.

    Args:
        book_side: Ask side of the order book.
        target_size: Shares we want to buy.

    Returns:
        (sufficient, available_at_best, total_available):
            sufficient: True if top-of-book volume >= target_size.
            available_at_best: Volume at the best ask price only.
            total_available: Total volume across all levels.
    """
    if book_side.is_empty:
        return False, 0.0, 0.0

    available_at_best = book_side.best_size or 0.0
    total_available = book_side.total_volume()
    sufficient = available_at_best >= target_size

    return sufficient, available_at_best, total_available


# =============================================================================
# CORE ARBITRAGE EVALUATION
# =============================================================================

def evaluate_arb(
    asks_up: OrderBookSide,
    asks_down: OrderBookSide,
    budget: float,
    peg_trigger: float = PEG_TRIGGER,
) -> ArbResult:
    """Evaluate if a risk-free arbitrage opportunity exists.

    This is the MAIN entry point for the arbitrage engine.

    Steps:
        1. Check both order books are non-empty.
        2. Read Lowest_Ask_Up and Lowest_Ask_Down (raw ASK prices).
        3. Calculate Peg = Lowest_Ask_Up + Lowest_Ask_Down.
        4. Check Peg < peg_trigger (0.98).
        5. Calculate equal shares based on budget.
        6. Check liquidity: is there enough volume at best ask?
        7. If not: calculate VWAP and re-check Peg with VWAP prices.
        8. Calculate fees, costs, payout, net profit.
        9. Final profitability gate: net_profit > 0.

    Args:
        asks_up:     Ask side of UP order book.
        asks_down:   Ask side of DOWN order book.
        budget:      Total USDC available for this trade.
        peg_trigger: Maximum Peg value to trigger (default 0.98).

    Returns:
        ArbResult with status and all trade parameters.
    """
    # ── Check 0: Non-empty order books ──────────────────
    if asks_up.is_empty:
        return ArbResult(
            status=ArbStatus.REJECT_EMPTY_BOOK,
            reason="UP order book is empty",
        )
    if asks_down.is_empty:
        return ArbResult(
            status=ArbStatus.REJECT_EMPTY_BOOK,
            reason="DOWN order book is empty",
        )

    # ── Check 1: Price Peg Calculation ──────────────────
    # Use ONLY raw ASK prices (not eff_price, not mid, not last)
    lowest_ask_up = asks_up.best_price
    lowest_ask_down = asks_down.best_price
    vol_at_ask_up = asks_up.best_size
    vol_at_ask_down = asks_down.best_size
    peg = lowest_ask_up + lowest_ask_down
    gross_margin = RESOLUTION_PAYOUT - peg

    # ── Check 2: Profitability Threshold ────────────────
    if peg >= peg_trigger:
        return ArbResult(
            status=ArbStatus.REJECT_PEG_TOO_HIGH,
            lowest_ask_up=lowest_ask_up,
            lowest_ask_down=lowest_ask_down,
            peg=peg,
            gross_margin=gross_margin,
            volume_at_ask_up=vol_at_ask_up,
            volume_at_ask_down=vol_at_ask_down,
            reason=(
                f"Peg={peg:.4f} >= trigger={peg_trigger} "
                f"(need {peg_trigger - peg:.4f} more margin)"
            ),
        )

    # ── Calculate equal shares ──────────────────────────
    # cost_per_share = ask_up + ask_down + fees_per_share
    fee_up = fee_rate(lowest_ask_up) * lowest_ask_up
    fee_down = fee_rate(lowest_ask_down) * lowest_ask_down
    cost_per_share = lowest_ask_up + lowest_ask_down + fee_up + fee_down

    if cost_per_share <= 0 or budget <= 0:
        return ArbResult(
            status=ArbStatus.REJECT_BUDGET_TOO_LOW,
            lowest_ask_up=lowest_ask_up,
            lowest_ask_down=lowest_ask_down,
            peg=peg,
            reason="Budget or cost_per_share is zero",
        )

    shares = budget / cost_per_share
    if shares < MIN_SHARES:
        return ArbResult(
            status=ArbStatus.REJECT_BUDGET_TOO_LOW,
            lowest_ask_up=lowest_ask_up,
            lowest_ask_down=lowest_ask_down,
            peg=peg,
            shares=shares,
            reason=f"Shares={shares:.6f} < min={MIN_SHARES}",
        )

    # ── Check 3: Liquidity & Slippage Prevention ────────
    liq_ok_up, avail_up, total_up = check_liquidity(asks_up, shares)
    liq_ok_down, avail_down, total_down = check_liquidity(asks_down, shares)

    used_vwap = False
    vwap_up_price = None
    vwap_down_price = None
    eff_ask_up = lowest_ask_up
    eff_ask_down = lowest_ask_down

    if not liq_ok_up or not liq_ok_down:
        # Top-of-book insufficient — try VWAP across deeper levels
        vwap_up_price, filled_up = calc_vwap(asks_up, shares)
        vwap_down_price, filled_down = calc_vwap(asks_down, shares)

        # Can we even fill the full size?
        if vwap_up_price is None or filled_up < shares * 0.95:
            return ArbResult(
                status=ArbStatus.REJECT_NO_LIQUIDITY_UP,
                lowest_ask_up=lowest_ask_up,
                lowest_ask_down=lowest_ask_down,
                peg=peg,
                gross_margin=gross_margin,
                shares=shares,
                volume_at_ask_up=avail_up,
                volume_at_ask_down=avail_down,
                reason=(
                    f"UP liquidity insufficient: "
                    f"need={shares:.2f} avail_best={avail_up:.2f} "
                    f"total={total_up:.2f} filled={filled_up:.2f}"
                ),
            )

        if vwap_down_price is None or filled_down < shares * 0.95:
            return ArbResult(
                status=ArbStatus.REJECT_NO_LIQUIDITY_DOWN,
                lowest_ask_up=lowest_ask_up,
                lowest_ask_down=lowest_ask_down,
                peg=peg,
                gross_margin=gross_margin,
                shares=shares,
                volume_at_ask_up=avail_up,
                volume_at_ask_down=avail_down,
                reason=(
                    f"DOWN liquidity insufficient: "
                    f"need={shares:.2f} avail_best={avail_down:.2f} "
                    f"total={total_down:.2f} filled={filled_down:.2f}"
                ),
            )

        # Re-check Peg with VWAP prices
        vwap_peg = vwap_up_price + vwap_down_price
        if vwap_peg >= peg_trigger:
            return ArbResult(
                status=ArbStatus.REJECT_VWAP_BREAKS_PEG,
                lowest_ask_up=lowest_ask_up,
                lowest_ask_down=lowest_ask_down,
                peg=peg,
                gross_margin=RESOLUTION_PAYOUT - vwap_peg,
                shares=shares,
                used_vwap=True,
                vwap_up=vwap_up_price,
                vwap_down=vwap_down_price,
                volume_at_ask_up=avail_up,
                volume_at_ask_down=avail_down,
                reason=(
                    f"VWAP Peg={vwap_peg:.4f} >= trigger={peg_trigger} "
                    f"(slippage breaks profitability)"
                ),
            )

        used_vwap = True
        eff_ask_up = vwap_up_price
        eff_ask_down = vwap_down_price
        # Recalculate shares with VWAP prices
        fee_up = fee_rate(eff_ask_up) * eff_ask_up
        fee_down = fee_rate(eff_ask_down) * eff_ask_down
        cost_per_share = eff_ask_up + eff_ask_down + fee_up + fee_down
        shares = budget / cost_per_share

    # ── Final cost calculation ──────────────────────────
    cost_up = cost_with_fee(shares, eff_ask_up)
    cost_down = cost_with_fee(shares, eff_ask_down)
    total_cost = cost_up + cost_down
    payout = shares * RESOLUTION_PAYOUT
    net_profit = payout - total_cost
    profit_pct = (net_profit / total_cost * 100.0
                  ) if total_cost > 0 else 0.0

    # ── Final profitability gate ────────────────────────
    if net_profit <= 0:
        return ArbResult(
            status=ArbStatus.REJECT_NEGATIVE_PROFIT,
            lowest_ask_up=lowest_ask_up,
            lowest_ask_down=lowest_ask_down,
            peg=peg,
            gross_margin=gross_margin,
            shares=shares,
            cost_up=cost_up,
            cost_down=cost_down,
            total_cost=total_cost,
            payout=payout,
            net_profit=net_profit,
            profit_pct=profit_pct,
            used_vwap=used_vwap,
            vwap_up=vwap_up_price,
            vwap_down=vwap_down_price,
            volume_at_ask_up=vol_at_ask_up,
            volume_at_ask_down=vol_at_ask_down,
            reason=f"Net profit=${net_profit:.6f} <= 0 after fees",
        )

    # ── OPPORTUNITY DETECTED ────────────────────────────
    return ArbResult(
        status=ArbStatus.OPPORTUNITY,
        lowest_ask_up=lowest_ask_up,
        lowest_ask_down=lowest_ask_down,
        peg=peg,
        gross_margin=gross_margin,
        shares=shares,
        cost_up=cost_up,
        cost_down=cost_down,
        total_cost=total_cost,
        payout=payout,
        net_profit=net_profit,
        profit_pct=profit_pct,
        used_vwap=used_vwap,
        vwap_up=vwap_up_price,
        vwap_down=vwap_down_price,
        volume_at_ask_up=vol_at_ask_up,
        volume_at_ask_down=vol_at_ask_down,
        reason="OPORTUNIDADE DETETADA — risk-free arb",
    )


# =============================================================================
# QA — COMPREHENSIVE UNIT TESTS
# =============================================================================

def run_tests():
    """Run all QA tests. Raises AssertionError on failure."""

    print("=" * 70)
    print("ARB ENGINE — QA TEST SUITE")
    print("=" * 70)

    # ─── Test 1: ASK-based prices used exclusively ──────
    print("\n[TEST 1] ASK-based prices used exclusively")
    asks_up = OrderBookSide(levels=[
        OrderBookLevel(price=0.45, size=100),
    ])
    asks_down = OrderBookSide(levels=[
        OrderBookLevel(price=0.50, size=100),
    ])
    result = evaluate_arb(asks_up, asks_down, budget=10.0)
    # Verify prices come from ASK (not mid, not eff)
    assert result.lowest_ask_up == 0.45, (
        f"Expected ASK 0.45, got {result.lowest_ask_up}"
    )
    assert result.lowest_ask_down == 0.50, (
        f"Expected ASK 0.50, got {result.lowest_ask_down}"
    )
    assert result.peg == 0.95, (
        f"Peg should be sum of ASKs: 0.95, got {result.peg}"
    )
    print(f"  ✅ lowest_ask_up={result.lowest_ask_up} (raw ASK)")
    print(f"  ✅ lowest_ask_down={result.lowest_ask_down} (raw ASK)")
    print(f"  ✅ peg={result.peg} (sum of raw ASKs)")

    # ─── Test 2: Peg < 0.98 strictly enforced ──────────
    print("\n[TEST 2] Peg < 0.98 strictly enforced")

    # 2a: Peg = 0.95 → PASS
    r = evaluate_arb(
        OrderBookSide([OrderBookLevel(0.45, 100)]),
        OrderBookSide([OrderBookLevel(0.50, 100)]),
        budget=10.0,
    )
    assert r.status == ArbStatus.OPPORTUNITY, (
        f"Peg=0.95 should be OPPORTUNITY, got {r.status}"
    )
    assert r.peg < 0.98
    print(f"  ✅ Peg=0.95 → {r.status.value} (profit=${r.net_profit:.4f})")

    # 2b: Peg = 0.98 → REJECT (>= trigger)
    r = evaluate_arb(
        OrderBookSide([OrderBookLevel(0.48, 100)]),
        OrderBookSide([OrderBookLevel(0.50, 100)]),
        budget=10.0,
    )
    assert r.status == ArbStatus.REJECT_PEG_TOO_HIGH, (
        f"Peg=0.98 should be REJECT, got {r.status}"
    )
    print(f"  ✅ Peg=0.98 → {r.status.value}")

    # 2c: Peg = 1.01 → REJECT
    r = evaluate_arb(
        OrderBookSide([OrderBookLevel(0.46, 100)]),
        OrderBookSide([OrderBookLevel(0.55, 100)]),
        budget=10.0,
    )
    assert r.status == ArbStatus.REJECT_PEG_TOO_HIGH
    print(f"  ✅ Peg=1.01 → {r.status.value}")

    # 2d: Peg = 0.979 → PASS (just under)
    r = evaluate_arb(
        OrderBookSide([OrderBookLevel(0.479, 100)]),
        OrderBookSide([OrderBookLevel(0.50, 100)]),
        budget=10.0,
    )
    assert r.status == ArbStatus.OPPORTUNITY
    assert r.peg < 0.98
    print(f"  ✅ Peg={r.peg:.3f} → {r.status.value}")

    # ─── Test 3: Liquidity volume checks ────────────────
    print("\n[TEST 3] Liquidity volume checks")

    # 3a: Sufficient volume → PASS
    r = evaluate_arb(
        OrderBookSide([OrderBookLevel(0.45, 500)]),
        OrderBookSide([OrderBookLevel(0.50, 500)]),
        budget=10.0,
    )
    assert r.status == ArbStatus.OPPORTUNITY
    assert r.shares <= 500  # should fit in available volume
    print(f"  ✅ Volume=500 each, shares={r.shares:.2f} → {r.status.value}")

    # 3b: Tiny volume at best ask → VWAP fallback
    r = evaluate_arb(
        OrderBookSide([
            OrderBookLevel(0.45, 2),    # only 2 shares at best
            OrderBookLevel(0.46, 100),  # deeper liquidity
        ]),
        OrderBookSide([OrderBookLevel(0.50, 500)]),
        budget=10.0,
    )
    if r.shares > 2:
        # Needed VWAP because top-of-book (2 shares) was insufficient
        assert r.used_vwap or r.status != ArbStatus.OPPORTUNITY
    print(f"  ✅ Thin UP book → used_vwap={r.used_vwap}, status={r.status.value}")

    # 3c: Zero volume → REJECT
    r = evaluate_arb(
        OrderBookSide(levels=[]),  # empty
        OrderBookSide([OrderBookLevel(0.50, 100)]),
        budget=10.0,
    )
    assert r.status == ArbStatus.REJECT_EMPTY_BOOK
    print(f"  ✅ Empty UP book → {r.status.value}")

    # ─── Test 4: VWAP slippage breaks Peg ───────────────
    print("\n[TEST 4] VWAP slippage breaks Peg")
    # Best ask at 0.45 but only 1 share.
    # Next level at 0.53 → VWAP will push Peg above 0.98.
    r = evaluate_arb(
        OrderBookSide([
            OrderBookLevel(0.45, 1),
            OrderBookLevel(0.53, 100),
        ]),
        OrderBookSide([OrderBookLevel(0.50, 100)]),
        budget=10.0,
    )
    # VWAP for UP ≈ 0.529 (mostly at 0.53)
    # VWAP Peg ≈ 0.529 + 0.50 = 1.029 > 0.98
    assert r.status in (
        ArbStatus.REJECT_VWAP_BREAKS_PEG,
        ArbStatus.REJECT_PEG_TOO_HIGH,
        ArbStatus.REJECT_NEGATIVE_PROFIT,
    )
    print(f"  ✅ Slippage breaks Peg → {r.status.value}")

    # ─── Test 5: Equal shares on both sides ─────────────
    print("\n[TEST 5] Equal shares on both sides")
    r = evaluate_arb(
        OrderBookSide([OrderBookLevel(0.40, 1000)]),
        OrderBookSide([OrderBookLevel(0.55, 1000)]),
        budget=20.0,
    )
    if r.status == ArbStatus.OPPORTUNITY:
        # shares field is used for BOTH sides equally
        cost_check = r.cost_up + r.cost_down
        assert abs(cost_check - r.total_cost) < 0.0001
        assert r.payout > r.total_cost  # profitable
        print(f"  ✅ shares={r.shares:.4f} (equal both sides)")
        print(f"  ✅ cost_up=${r.cost_up:.4f} cost_down=${r.cost_down:.4f}")
        print(f"  ✅ total_cost=${r.total_cost:.4f} payout=${r.payout:.4f}")
        print(f"  ✅ net_profit=${r.net_profit:.4f} ({r.profit_pct:+.2f}%)")

    # ─── Test 6: Fee calculation correctness ────────────
    print("\n[TEST 6] Fee calculation")
    # At p=0.50: fee_rate = 0.25 × (0.50 × 0.50)² = 0.25 × 0.0625 = 0.015625
    fr = fee_rate(0.50)
    assert abs(fr - 0.015625) < 1e-9, f"fee_rate(0.50)={fr}"
    print(f"  ✅ fee_rate(0.50)={fr:.6f} (1.5625%)")

    # At p=1.0: fee_rate = 0 (resolution is free)
    # We can't test p=1.0 directly (OrderBookLevel rejects it)
    # but the formula gives 0
    assert fee_rate(0.99) < 0.001  # near-zero at extremes
    assert fee_rate(0.01) < 0.001
    print(f"  ✅ fee_rate(0.99)={fee_rate(0.99):.8f} (near zero)")
    print(f"  ✅ fee_rate(0.01)={fee_rate(0.01):.8f} (near zero)")

    # ─── Test 7: OrderBookLevel validation ──────────────
    print("\n[TEST 7] Data model validation")
    try:
        OrderBookLevel(price=0.0, size=10)
        assert False, "Should reject price=0.0"
    except ValueError:
        print("  ✅ Rejects price=0.0")

    try:
        OrderBookLevel(price=1.0, size=10)
        assert False, "Should reject price=1.0"
    except ValueError:
        print("  ✅ Rejects price=1.0")

    try:
        OrderBookLevel(price=0.50, size=0)
        assert False, "Should reject size=0"
    except ValueError:
        print("  ✅ Rejects size=0")

    try:
        OrderBookLevel(price=0.50, size=-5)
        assert False, "Should reject size<0"
    except ValueError:
        print("  ✅ Rejects size<0")

    # Valid level
    lv = OrderBookLevel(price=0.45, size=100)
    assert lv.price == 0.45 and lv.size == 100
    print("  ✅ Valid level: price=0.45, size=100")

    # ─── Test 8: VWAP calculation ───────────────────────
    print("\n[TEST 8] VWAP calculation")
    book = OrderBookSide([
        OrderBookLevel(0.45, 50),
        OrderBookLevel(0.46, 100),
        OrderBookLevel(0.47, 200),
    ])
    # Buy 120 shares: 50@0.45 + 70@0.46
    vwap, filled = calc_vwap(book, 120)
    expected_cost = 50 * 0.45 + 70 * 0.46
    expected_vwap = expected_cost / 120
    assert abs(vwap - expected_vwap) < 1e-9
    assert filled == 120
    print(f"  ✅ VWAP(120 shares)={vwap:.6f} (expected {expected_vwap:.6f})")

    # Buy more than available
    vwap2, filled2 = calc_vwap(book, 500)
    assert filled2 == 350  # 50 + 100 + 200
    print(f"  ✅ VWAP(500 shares, book=350) filled={filled2}")

    # ─── Test 9: Budget edge cases ──────────────────────
    print("\n[TEST 9] Edge cases")
    r = evaluate_arb(
        OrderBookSide([OrderBookLevel(0.45, 100)]),
        OrderBookSide([OrderBookLevel(0.50, 100)]),
        budget=0.0,
    )
    assert r.status == ArbStatus.REJECT_BUDGET_TOO_LOW
    print(f"  ✅ budget=0 → {r.status.value}")

    r = evaluate_arb(
        OrderBookSide([OrderBookLevel(0.45, 100)]),
        OrderBookSide([OrderBookLevel(0.50, 100)]),
        budget=0.0001,
    )
    assert r.status == ArbStatus.REJECT_BUDGET_TOO_LOW
    print(f"  ✅ budget=0.0001 → {r.status.value}")

    # ─── Test 10: Integration — realistic scenario ──────
    print("\n[TEST 10] Realistic scenario")
    up_book = OrderBookSide.from_raw([
        {"price": "0.46", "size": "200"},
        {"price": "0.47", "size": "500"},
        {"price": "0.48", "size": "1000"},
    ])
    down_book = OrderBookSide.from_raw([
        {"price": "0.50", "size": "300"},
        {"price": "0.51", "size": "600"},
        {"price": "0.52", "size": "1000"},
    ])
    r = evaluate_arb(up_book, down_book, budget=50.0)
    print(f"  Peg = {r.peg:.4f}")
    print(f"  Status = {r.status.value}")
    if r.status == ArbStatus.OPPORTUNITY:
        print(f"  Shares = {r.shares:.4f} (equal both sides)")
        print(f"  Cost = ${r.total_cost:.4f}")
        print(f"  Payout = ${r.payout:.4f}")
        print(f"  Net Profit = ${r.net_profit:.4f} ({r.profit_pct:+.2f}%)")
        print(f"  VWAP used = {r.used_vwap}")
        assert r.net_profit > 0
        assert r.peg < 0.98
        assert r.shares > 0
    print(f"  ✅ Realistic scenario: {r.status.value}")

    print("\n" + "=" * 70)
    print("ALL 10 TESTS PASSED ✅")
    print("=" * 70)


if __name__ == "__main__":
    run_tests()
