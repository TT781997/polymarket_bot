"""Detetor de burst de fluxo (Hawkes univariado). Regime, nao gerador de spread.

lambda(t) = mu + sum_{t_i < t} alpha * exp(-beta*(t - t_i)), forma recursiva.
Clustering de fills/trades => informed flow => alargar ou retirar as pernas.
Estacionariedade: alpha < beta.
"""
from __future__ import annotations
import math


class HawkesBurst:
    def __init__(self, mu: float, alpha: float, beta: float, mult: float = 3.0):
        if not alpha < beta:
            raise ValueError("alpha/beta deve ser < 1 (estacionariedade)")
        self.mu, self.alpha, self.beta, self.mult = mu, alpha, beta, mult
        self._lambda = mu
        self._last: float | None = None

    def event(self, t: float) -> None:
        if self._last is None:
            self._lambda = self.mu + self.alpha
        else:
            decay = math.exp(-self.beta * (t - self._last))
            self._lambda = self.mu + (self._lambda - self.mu) * decay + self.alpha
        self._last = t

    def intensity(self, t: float) -> float:
        if self._last is None:
            return self.mu
        return self.mu + (self._lambda - self.mu) * math.exp(-self.beta * (t - self._last))

    def is_burst(self, t: float) -> bool:
        return self.intensity(t) > self.mult * self.mu
