"""Selecao de mercado por liquido esperado no delta* de cada um.

A referencia filtra so por pool > limiar. Insuficiente: um fill adverso pode
apagar varios dias de reward. Ranqueia por E[liquido diario], rejeita net<=0,
rho acima do limiar, ou mercados infeasiveis ao bankroll.
"""
from __future__ import annotations
from dataclasses import dataclass

from optimizer import optimal_delta, reward_rate, cost_rate, mid_suboptimal, placement_regime

SECS_DAY = 86400.0


def _drift_sd(mid_c: float, requote_s: float = 60.0, vol_c_per_s: float = 0.03) -> float:
    """Desvio do drift do mid no intervalo de requote (c). sqrt(t)*vol."""
    return vol_c_per_s * (requote_s ** 0.5)


@dataclass
class MarketEval:
    market_id: str
    mid_c: float
    max_spread_c: float
    min_size: float
    daily_pool: float
    size: float
    delta_c: float
    regime: str
    reward_daily: float
    cost_daily: float
    net_daily: float
    rho: float
    feasible: bool
    reason: str


def evaluate_market(market_id, mid_c, max_spread_c, min_size, daily_pool,
                    q_others, c_loss_per_share, a_fill, k_fill, bankroll, rho_max) -> MarketEval:
    """Procura o (size, delta*) que maximiza o net diario. A receita esta limitada
    pelo pool e o risco cresce com o size, por isso o size otimo dimensiona-se ao
    POOL, nao a carteira (o erro do video ao por 1000 shares em todo o lado).

    c_loss_per_share = perda adversa esperada por share cheia ($/share); o custo
    por evento e c_loss_per_share * (fatia media) e escala com o size.
    """
    cheap_price = max(mid_c, 0.5) / 100.0
    max_feasible = bankroll / (2.0 * cheap_price)          # teto da carteira
    pool_ps = daily_pool / SECS_DAY
    sd_c = _drift_sd(mid_c)

    if max_feasible < min_size:
        return MarketEval(market_id, mid_c, max_spread_c, min_size, daily_pool,
                          max_feasible, 0.0, "n/a", 0.0, 0.0, 0.0, float("inf"),
                          False, f"infeasivel: min_size {min_size:.0f} > "
                          f"{max_feasible:.0f} sh a ${bankroll:.0f}")

    best = None
    steps = 16
    for j in range(steps + 1):
        size = min_size * (max_feasible / min_size) ** (j / steps)   # log-spaced
        c_loss = c_loss_per_share * size                              # $/evento ~ size*fatia
        d, _ = optimal_delta(size, max_spread_c, pool_ps, q_others, c_loss,
                             a_fill, k_fill, sd_c=sd_c)
        rwd = reward_rate(d, size, max_spread_c, pool_ps, q_others, sd_c) * SECS_DAY
        cst = cost_rate(d, c_loss, a_fill, k_fill) * SECS_DAY
        net = rwd - cst
        if best is None or net > best[0]:
            best = (net, size, d, rwd, cst)

    net_daily, size, delta_c, reward_daily, cost_daily = best
    regime = placement_regime(delta_c, max_spread_c)
    rho = min(cost_daily / reward_daily, 9999.0) if reward_daily > 0.0 else 9999.0

    if net_daily <= 0.0:
        reason = "net<=0"
    elif rho > rho_max:
        reason = f"rho {rho:.2f} > {rho_max:.2f}"
    else:
        reason = "ok"

    return MarketEval(market_id, mid_c, max_spread_c, min_size, daily_pool, size,
                      delta_c, regime, reward_daily, cost_daily, net_daily,
                      rho, True, reason)


def rank_markets(evals: list[MarketEval], rho_max: float) -> list[MarketEval]:
    ok = [e for e in evals
          if e.feasible and e.net_daily > 0.0 and e.rho <= rho_max]
    return sorted(ok, key=lambda e: e.net_daily, reverse=True)
