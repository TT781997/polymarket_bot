"""Orcamento de ordens e cancelamentos por signer.

A spec (seccoes 4 e 7) exige dimensionar o loop de requote ao tier do signer, em
vez de descobrir o teto numa sessao volatil. Um loop de requote e cancel-heavy por
natureza: cada reposicionamento das pernas custa 2 cancelamentos + 2 ordens.

AVISO -- os limites reais por tier do Polymarket (escalonados por volume maker de
30 dias) NAO estao aqui, porque nao foi possivel confirma-los contra a doc oficial
(rede bloqueada). O default e deliberadamente conservador e serve para a ferramenta
NUNCA ser a primeira a descobrir o teto. Quem souber o seu tier passa os numeros:

    Budget(orders_per_min=..., cancels_per_min=...)

Janela deslizante real (deque de timestamps), nao um contador que reinicia: um
contador por minuto de relogio deixa passar o dobro do limite na fronteira.
"""
from __future__ import annotations
from collections import deque

# conservador de proposito -- ver aviso acima
DEFAULT_ORDERS_PER_MIN = 30
DEFAULT_CANCELS_PER_MIN = 30


class Budget:
    def __init__(self, orders_per_min: int = DEFAULT_ORDERS_PER_MIN,
                 cancels_per_min: int = DEFAULT_CANCELS_PER_MIN,
                 window_s: float = 60.0):
        self.orders_per_min = orders_per_min
        self.cancels_per_min = cancels_per_min
        self.window_s = window_s
        self._orders: deque[float] = deque()
        self._cancels: deque[float] = deque()
        self.denied = 0                 # quantas vezes o orcamento travou um requote

    def _prune(self, dq: deque, now: float) -> None:
        limite = now - self.window_s
        while dq and dq[0] <= limite:
            dq.popleft()

    def can_afford(self, now: float, orders: int = 2, cancels: int = 2) -> bool:
        """Ha espaco para um requote completo (cancelar as duas pernas e repor)?"""
        self._prune(self._orders, now)
        self._prune(self._cancels, now)
        return (len(self._orders) + orders <= self.orders_per_min
                and len(self._cancels) + cancels <= self.cancels_per_min)

    def spend(self, now: float, orders: int = 2, cancels: int = 2) -> None:
        for _ in range(orders):
            self._orders.append(now)
        for _ in range(cancels):
            self._cancels.append(now)

    def deny(self) -> None:
        self.denied += 1

    def usage(self, now: float) -> tuple[int, int]:
        """(ordens, cancelamentos) na janela corrente."""
        self._prune(self._orders, now)
        self._prune(self._cancels, now)
        return len(self._orders), len(self._cancels)
