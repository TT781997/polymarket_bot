"""Book e fills sinteticos para modos paper/replay.

Os fills correlacionam com movimento adverso do mid (compraste => mid cai), que
e o mecanismo central da selecao adversa e a origem do -17 do video.
"""
from __future__ import annotations
import math
import random


class SyntheticMarket:
    def __init__(self, market_id, mid_c, max_spread_c, min_size, daily_pool,
                 a_fill, k_fill, toxicity_c=1.2, comp=1500.0, drift=0.0,
                 vol_c=0.22, seed=None):
        self.market_id = market_id
        self.mid_c = mid_c
        self.max_spread_c = max_spread_c
        self.min_size = min_size
        self.daily_pool = daily_pool
        self.comp = comp              # concorrencia (Q_others alvo)
        self.a_fill = a_fill          # fills/s no mid (verdade do gerador)
        self.k_fill = k_fill
        self.toxicity_c = toxicity_c  # movimento adverso medio por fill (c)
        self.drift = drift
        self.vol_c = vol_c
        self.rng = random.Random(seed)

    def step_mid(self, dt: float) -> float:
        self.mid_c += self.drift * dt + self.rng.gauss(0.0, self.vol_c) * math.sqrt(dt)
        if self.rng.random() < 0.02 * dt:                       # salto raro
            self.mid_c += self.rng.choice([-1.0, 1.0]) * self.rng.uniform(1.0, 4.0)
        self.mid_c = min(9.5, max(1.0, self.mid_c))             # mantem regime extremo
        return self.mid_c

    def book(self, levels=7, tick=0.4):
        base = self.comp / levels                    # nivel medio ~ comp/levels
        bids, asks = [], []
        for i in range(1, levels + 1):
            bids.append((self.mid_c - i * tick, base * self.rng.uniform(0.5, 1.5)))
            asks.append((self.mid_c + i * tick, base * self.rng.uniform(0.5, 1.5)))
        return bids, asks

    def try_fill(self, side: str, delta_c: float, size: float, dt: float,
                 fill_frac: float = 0.15):
        """Fill no passo dt a distancia delta_c. Enche uma FATIA da perna (fills
        reais sao parciais), nao a perna toda. Devolve (shares, adverse_c)."""
        lam = self.a_fill * math.exp(-self.k_fill * delta_c)
        p = 1.0 - math.exp(-lam * dt)
        if self.rng.random() < p:
            filled = min(size, size * self.rng.uniform(0.5 * fill_frac, 1.5 * fill_frac))
            adverse = abs(self.rng.gauss(self.toxicity_c, self.toxicity_c * 0.5))
            self.mid_c += (-adverse if side == "bid" else adverse)
            self.mid_c = min(9.5, max(1.0, self.mid_c))
            return filled, adverse
        return 0.0, 0.0
