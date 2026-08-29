"""delta* otimo, condicao de solucao interior, calibracao A/k e skew de inventario.

Objetivo por perna, com share EXPLICITA e fator de permanencia na band. Tres
tensoes que juntas dao um maximo interior genuino:

  s(d)      = size * ((D - d)/D)^2                      score quadratico
  g(d)      = P(fica na band apesar do drift do mid)    2*Phi((D-d)/sd) - 1
  reward(d) = pool_ps * s(d)/(s(d)+q_others) * g(d)     [$/s]
  cost(d)   = c_loss * A * exp(-k d)                     [$/s]
  U(d)      = reward(d) - cost(d)

Sem g(d), o otimo degenera para a borda em mercados finos (a perda de reward
achata mas o custo ainda cai). g(d) penaliza a borda -- e onde o drift te deita
fora da band e passas a marcar zero, exatamente o que o post descreve -- e
devolve um interior. d e D em centimos; pool_ps em $/s; c_loss em $/fill; A em
fills/s; sd = desvio do drift do mid no intervalo de requote (c).
"""
from __future__ import annotations
import math


def _phi(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def stay_factor(delta_c: float, max_spread_c: float, sd_c: float) -> float:
    """Fracao da amostra em que a perna fica dentro da band dado o drift do mid."""
    if sd_c <= 0.0:
        return 1.0 if delta_c < max_spread_c else 0.0
    g = 2.0 * _phi((max_spread_c - delta_c) / sd_c) - 1.0
    return max(0.0, min(1.0, g))


def score_size(size: float, delta_c: float, max_spread_c: float) -> float:
    if delta_c >= max_spread_c or delta_c < 0.0:
        return 0.0
    return size * ((max_spread_c - delta_c) / max_spread_c) ** 2


def reward_rate(delta_c, size, max_spread_c, pool_ps, q_others, sd_c=0.0) -> float:
    s = score_size(size, delta_c, max_spread_c)
    tot = s + q_others
    if tot <= 0.0:
        return 0.0
    return pool_ps * s / tot * stay_factor(delta_c, max_spread_c, sd_c)


def cost_rate(delta_c, c_loss, a_fill, k_fill) -> float:
    return c_loss * a_fill * math.exp(-k_fill * delta_c)


def utility(delta_c, size, max_spread_c, pool_ps, q_others, c_loss, a_fill, k_fill, sd_c=0.0) -> float:
    return (reward_rate(delta_c, size, max_spread_c, pool_ps, q_others, sd_c)
            - cost_rate(delta_c, c_loss, a_fill, k_fill))


def mid_suboptimal(size, max_spread_c, pool_ps, q_others, c_loss, a_fill, k_fill, sd_c=0.0) -> bool:
    """U sobe ao sair do mid => o mid nao e o otimo (recuar ajuda). Nao garante
    que o otimo seja interior: pode ser a borda. Ver placement_regime."""
    args = (size, max_spread_c, pool_ps, q_others, c_loss, a_fill, k_fill, sd_c)
    eps = 1e-4 * max_spread_c
    return utility(eps, *args) > utility(0.0, *args)


def placement_regime(delta_c: float, max_spread_c: float) -> str:
    """Onde caiu o delta*: MID (fica no mid), BORDA (recua/nao farmar) ou
    INTERIOR (recuo intermedio que evita toxicidade). Regime real, nao boolean."""
    if delta_c < 0.05 * max_spread_c:
        return "MID"
    if delta_c > 0.95 * max_spread_c:
        return "BORDA"
    return "INTERIOR"


def optimal_delta(size, max_spread_c, pool_ps, q_others, c_loss, a_fill, k_fill,
                  sd_c=0.0, grid: int = 240) -> tuple[float, float]:
    """Maximiza U em [0, D]: grid grosso + refinamento por seccao aurea.

    Robusto a forma. Retorna a borda em mercados demasiado finos (sinal de que o
    mercado nao vale a pena farmar -- o guardrail rho rejeita-o).
    """
    args = (size, max_spread_c, pool_ps, q_others, c_loss, a_fill, k_fill, sd_c)
    d = max_spread_c
    best_x, best_u = 0.0, utility(0.0, *args)
    for i in range(1, grid + 1):
        x = d * i / grid
        u = utility(x, *args)
        if u > best_u:
            best_u, best_x = u, x
    step = d / grid
    lo, hi = max(0.0, best_x - step), min(d, best_x + step)
    r = 0.6180339887
    while hi - lo > 1e-6 * d:
        m1 = hi - r * (hi - lo)
        m2 = lo + r * (hi - lo)
        if utility(m1, *args) < utility(m2, *args):
            lo = m1
        else:
            hi = m2
    x = 0.5 * (lo + hi)
    return x, utility(x, *args)


def calibrate_ak(buckets: list[tuple[float, int, float]]) -> tuple[float, float] | None:
    """Regressao ln(lambda) vs delta. `buckets`: [(delta_c, fills, seconds), ...]."""
    xs, ys = [], []
    for dc, f, s in buckets:
        if s > 0.0 and f > 0:
            xs.append(dc)
            ys.append(math.log(f / s))
    n = len(xs)
    if n < 2:
        return None
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0.0:
        return None
    sxy = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    slope = sxy / sxx
    return math.exp(my - slope * mx), -slope


def reservation_mid_c(mid_c, inv, inv_cap, max_skew_c) -> float:
    """Skew A-S normalizado e limitado. Inventario positivo (long do lado barato)
    => reserva abaixo do mid => bid mais longe (compra menos), ask mais perto
    (vende mais) => escoa a posicao. Shift maximo = max_skew_c quando inv=inv_cap.
    """
    q = max(-1.0, min(1.0, inv / inv_cap)) if inv_cap > 0 else 0.0
    return mid_c - q * max_skew_c
