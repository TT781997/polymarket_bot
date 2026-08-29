"""Scoring do programa de Liquidity Rewards do Polymarket.

Score por ordem quadratico na distancia ao midpoint ajustado, Q_min das duas
pernas com as duas bandas de midpoint, e share normalizada contra a concorrencia
observada no book. Funcoes puras. Distancias e niveis em centimos; midpoint
convertido para preco so no teste de banda.
"""
from __future__ import annotations


def order_score(spread_c: float, size: float, max_spread_c: float) -> float:
    """Score de uma ordem a `spread_c` do mid ajustado. Zero fora da band."""
    if size <= 0.0 or spread_c < 0.0 or spread_c > max_spread_c:
        return 0.0
    return ((max_spread_c - spread_c) / max_spread_c) ** 2 * size


def side_q(orders: list[tuple[float, float]], max_spread_c: float) -> float:
    """Q de um lado = soma dos scores. `orders`: [(spread_c, size), ...]."""
    return sum(order_score(s, sz, max_spread_c) for s, sz in orders)


def adjusted_mid_c(bids: list[tuple[float, float]],
                   asks: list[tuple[float, float]],
                   min_size: float) -> float | None:
    """Midpoint ajustado: melhor bid/ask depois de remover poeira < min_size.

    `bids`/`asks`: [(level_c, size), ...]. None se um dos lados fica vazio.
    """
    b = [lvl for lvl, sz in bids if sz >= min_size]
    a = [lvl for lvl, sz in asks if sz >= min_size]
    if not b or not a:
        return None
    return (max(b) + min(a)) / 2.0


def q_min(q_one: float, q_two: float, mid_c: float, c_penalty: float = 3.0) -> float:
    """Combina as duas pernas.

    Banda [10c, 90c]: unilateral pontua a taxa reduzida (/c_penalty).
    Fora dela (regime alvo, um lado < 5c): bilateral obrigatorio.
    """
    if 10.0 <= mid_c <= 90.0:
        return max(min(q_one, q_two), max(q_one / c_penalty, q_two / c_penalty))
    return min(q_one, q_two)


def pool_share(q_you: float, q_others: float) -> float:
    """Share da pool = Q_you / (Q_you + Q_others)."""
    tot = q_you + q_others
    return q_you / tot if tot > 0.0 else 0.0
