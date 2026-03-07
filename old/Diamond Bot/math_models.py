"""
math_models.py — Mathematical models for prediction-market bots.

Contains three core model families:

1. **Bayesian Signal** — Sequential Bayesian updating in log-space
   for numerical stability, plus EV calculation.

2. **LMSR (Logarithmic Market Scoring Rule)** — Cost function,
   softmax price function, and trade-cost calculation used by the
   Market Maker bot.

3. **EV / Position-sizing helpers** shared across bots.

All functions are pure (no side-effects) and fully typed.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional, Sequence


# ─────────────────────────────────────────────────────────────────────
# 1. Sequential Bayesian Updating (log-space)
# ─────────────────────────────────────────────────────────────────────

@dataclass
class BayesianEstimator:
    """Maintains a running log-posterior for a binary hypothesis.

    Implements the update rule in log-space:

        log P(H | D) = log P(H)
                      + sum_k log P(D_k | H)
                      - log Z

    where Z is the normalising constant computed from both
    hypotheses.

    Attributes:
        log_prior_h: Log-prior for hypothesis H (event happens).
        log_prior_not_h: Log-prior for ~H.
        log_likelihood_sum_h: Accumulated log-likelihoods under H.
        log_likelihood_sum_not_h: Accumulated under ~H.
        n_updates: Number of data points ingested so far.
    """

    log_prior_h: float = field(
        default_factory=lambda: math.log(0.5)
    )
    log_prior_not_h: float = field(
        default_factory=lambda: math.log(0.5)
    )
    log_likelihood_sum_h: float = 0.0
    log_likelihood_sum_not_h: float = 0.0
    n_updates: int = 0

    # ── Core update ──────────────────────────────────────────────

    def update(
        self,
        log_lik_h: float,
        log_lik_not_h: float,
    ) -> float:
        """Ingest one data point and return updated P(H | D).

        Args:
            log_lik_h: log P(D_k | H) for this data point.
            log_lik_not_h: log P(D_k | ~H) for this data point.

        Returns:
            Updated posterior probability P(H | D_1..D_k).

        Example:
            >>> be = BayesianEstimator()
            >>> p = be.update(math.log(0.9), math.log(0.3))
            >>> 0.0 < p < 1.0
            True
        """
        self.log_likelihood_sum_h += log_lik_h
        self.log_likelihood_sum_not_h += log_lik_not_h
        self.n_updates += 1
        return self.posterior()

    def posterior(self) -> float:
        """Compute P(H | all data) from accumulated log-values.

        Uses the log-sum-exp trick for numerical stability:
            log Z = logsumexp(log_joint_h, log_joint_not_h)

        Returns:
            Posterior probability in [0, 1].
        """
        lj_h = (
            self.log_prior_h + self.log_likelihood_sum_h
        )
        lj_nh = (
            self.log_prior_not_h
            + self.log_likelihood_sum_not_h
        )
        log_z = _logsumexp(lj_h, lj_nh)
        return math.exp(lj_h - log_z)

    def reset(self, prior: float = 0.5) -> None:
        """Reset estimator with a new prior.

        Args:
            prior: Prior probability for H (0, 1).
        """
        prior = max(1e-12, min(1.0 - 1e-12, prior))
        self.log_prior_h = math.log(prior)
        self.log_prior_not_h = math.log(1.0 - prior)
        self.log_likelihood_sum_h = 0.0
        self.log_likelihood_sum_not_h = 0.0
        self.n_updates = 0


def _logsumexp(a: float, b: float) -> float:
    """Numerically stable log(exp(a) + exp(b)).

    Args:
        a: First log-value.
        b: Second log-value.

    Returns:
        log(exp(a) + exp(b)).
    """
    mx = max(a, b)
    return mx + math.log(math.exp(a - mx) + math.exp(b - mx))


def compute_ev(
    est_prob: float,
    market_price: float,
) -> float:
    """Expected value for a binary prediction-market bet.

    EV = p_hat * (1 - p) - (1 - p_hat) * p = p_hat - p

    where p_hat is the estimated true probability and p is the
    current market price.

    Args:
        est_prob: Estimated true probability.
        market_price: Current market-implied probability / price.

    Returns:
        Expected value as a float.

    Example:
        >>> compute_ev(0.70, 0.55)
        0.15
    """
    return est_prob - market_price


# ─────────────────────────────────────────────────────────────────────
# 2. LMSR — Logarithmic Market Scoring Rule
# ─────────────────────────────────────────────────────────────────────

class LMSR:
    """Logarithmic Market Scoring Rule pricing engine.

    Implements the Hanson LMSR for *n* mutually exclusive outcomes
    with liquidity parameter *b*.

    Cost function:
        C(q) = b * ln( sum_i exp(q_i / b) )

    Price (softmax):
        p_i(q) = exp(q_i / b) / sum_j exp(q_j / b)

    Trade cost to move outcome *i* from q_i to q_i + delta:
        cost = C(q_1, ..., q_i + delta, ...) - C(q_1, ..., q_i, ...)

    Maximum market-maker loss:
        L_max = b * ln(n)

    Attributes:
        b: Liquidity parameter (larger -> tighter spreads,
           higher max loss).
        quantities: Current outstanding quantity vector.
    """

    def __init__(
        self,
        b: float = 100.0,
        n_outcomes: int = 2,
    ) -> None:
        if b <= 0.0:
            raise ValueError(f"b must be positive, got {b}")
        if n_outcomes < 2:
            raise ValueError(
                f"Need >= 2 outcomes, got {n_outcomes}"
            )
        self.b = b
        self.quantities: list[float] = [0.0] * n_outcomes

    # ── Cost function ────────────────────────────────────────────

    def cost(
        self,
        q: Optional[Sequence[float]] = None,
    ) -> float:
        """Evaluate the LMSR cost function C(q).

        Args:
            q: Quantity vector. Uses self.quantities if None.

        Returns:
            Cost as a float.

        Example:
            >>> lmsr = LMSR(b=100.0, n_outcomes=2)
            >>> round(lmsr.cost([0, 0]), 2)
            69.31
        """
        q = q if q is not None else self.quantities
        return self.b * _log_sum_exp_vec(q, self.b)

    # ── Price function (softmax) ─────────────────────────────────

    def prices(
        self,
        q: Optional[Sequence[float]] = None,
    ) -> list[float]:
        """Compute LMSR prices for all outcomes (softmax).

        Critical properties: sum(prices) == 1.0, each in (0, 1).

        Args:
            q: Quantity vector. Uses self.quantities if None.

        Returns:
            List of prices, one per outcome.

        Example:
            >>> lmsr = LMSR(b=100.0, n_outcomes=2)
            >>> lmsr.prices([0, 0])
            [0.5, 0.5]
        """
        q = q if q is not None else self.quantities
        scaled = [qi / self.b for qi in q]
        mx = max(scaled)
        exps = [math.exp(s - mx) for s in scaled]
        total = sum(exps)
        return [e / total for e in exps]

    # ── Trade cost ───────────────────────────────────────────────

    def trade_cost(
        self,
        outcome_idx: int,
        delta: float,
    ) -> float:
        """Cost to buy *delta* shares of outcome *outcome_idx*.

        cost = C(q_1, ..., q_i + delta, ...) - C(q_1, ..., q_i, ...)

        Args:
            outcome_idx: Index of the outcome to trade.
            delta: Number of shares (positive = buy, negative = sell).

        Returns:
            Dollar cost of the trade (can be negative for sells).

        Raises:
            IndexError: If outcome_idx is out of range.

        Example:
            >>> lmsr = LMSR(b=100.0, n_outcomes=2)
            >>> cost = lmsr.trade_cost(0, 10.0)
            >>> cost > 0
            True
        """
        if not 0 <= outcome_idx < len(self.quantities):
            raise IndexError(
                f"outcome_idx {outcome_idx} out of range "
                f"[0, {len(self.quantities)})"
            )
        c_before = self.cost()
        q_after = list(self.quantities)
        q_after[outcome_idx] += delta
        c_after = self.cost(q_after)
        return c_after - c_before

    def execute_trade(
        self,
        outcome_idx: int,
        delta: float,
    ) -> float:
        """Execute a trade: compute cost and update quantities.

        Args:
            outcome_idx: Index of the outcome to trade.
            delta: Shares to buy (pos) or sell (neg).

        Returns:
            Dollar cost of the trade.
        """
        cost = self.trade_cost(outcome_idx, delta)
        self.quantities[outcome_idx] += delta
        return cost

    # ── Utility ──────────────────────────────────────────────────

    @property
    def max_loss(self) -> float:
        """Maximum possible market-maker loss: b * ln(n)."""
        return self.b * math.log(len(self.quantities))

    def inefficiency(self) -> float:
        """Detect pricing inefficiency for binary markets.

        For a binary market, if |p_yes - 0.5| is large, the market
        has high conviction. The *inefficiency signal* is the
        absolute deviation from fair value, useful for the
        arbitrage bot.

        Returns:
            Inefficiency metric (0 = perfectly balanced).
        """
        p = self.prices()
        if len(p) != 2:
            return 0.0
        return abs(p[0] - 0.5)

    def reset(self) -> None:
        """Reset all quantities to zero."""
        self.quantities = [0.0] * len(self.quantities)


def _log_sum_exp_vec(
    q: Sequence[float],
    b: float,
) -> float:
    """Compute ln(sum exp(q_i / b)) with numerical stability.

    Args:
        q: Quantity vector.
        b: Liquidity parameter.

    Returns:
        Log-sum-exp value.
    """
    scaled = [qi / b for qi in q]
    mx = max(scaled)
    return mx + math.log(
        sum(math.exp(s - mx) for s in scaled)
    )


# ─────────────────────────────────────────────────────────────────────
# 3. Shared position-sizing helpers
# ─────────────────────────────────────────────────────────────────────

def optimal_spread(
    sigma: float,
    gamma: float,
    t_remaining: float,
    kappa: float,
) -> float:
    """Avellaneda-Stoikov optimal half-spread.

    spread = gamma * sigma^2 * T + (2/gamma) * ln(1 + gamma/kappa)

    Args:
        sigma: Estimated volatility (standard deviation).
        gamma: Risk-aversion parameter.
        t_remaining: Time remaining in seconds.
        kappa: Order-arrival rate.

    Returns:
        Optimal full spread.

    Example:
        >>> s = optimal_spread(0.05, 0.1, 300.0, 1.5)
        >>> s > 0
        True
    """
    if kappa <= 0 or gamma <= 0:
        return 0.0
    return (
        gamma * sigma * sigma * t_remaining
        + (2.0 / gamma) * math.log(1.0 + gamma / kappa)
    )
