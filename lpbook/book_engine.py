"""Motor por mercado.

Mantem as duas pernas em delta*, requote no drift, skew de inventario, resposta
a burst de fluxo, calibracao A/k continua, e credito de reward por amostra.
Agnostico a execucao (paper/live via executor).
"""
from __future__ import annotations
from dataclasses import dataclass

from scoring import order_score, side_q, q_min, pool_share
from optimizer import optimal_delta, mid_suboptimal, placement_regime, reservation_mid_c, calibrate_ak
from flow import HawkesBurst

SECS_DAY = 86400.0


@dataclass
class Leg:
    side: str
    level_c: float
    size: float


@dataclass
class MarketState:
    market_id: str
    max_spread_c: float
    min_size: float
    daily_pool: float
    size: float
    inv: float = 0.0        # inventario do lado barato (shares)
    avg_c: float = 0.0      # preco medio de entrada (c)
    realized: float = 0.0   # rewards acumulados ($)
    fills: int = 0
    delta_c: float = 0.0
    regime: str = "MID"
    bid: Leg | None = None
    ask: Leg | None = None


class BookEngine:
    def __init__(self, st: MarketState, max_skew_c, inv_cap, hawkes: HawkesBurst,
                 c_loss, a_fill, k_fill, dither_c: float = 0.0):
        self.st = st
        self.max_skew_c = max_skew_c
        self.inv_cap = inv_cap
        self.hawkes = hawkes
        self.c_loss = c_loss
        self.a_fill = a_fill
        self.k_fill = k_fill
        self.dither_c = dither_c                # amplitude do dithering de delta (c)
        self._dither_phase = 0
        self._buckets: dict[float, list] = {}   # distancia ao mid -> [fills, segundos]
        self.withdrawn = False

    @staticmethod
    def _bucket_key(level_c: float, mid_c: float) -> float:
        return round(abs(level_c - mid_c), 1)

    def observe_time(self, dt: float, mid_c: float) -> None:
        """Acumula exposicao no bucket de CADA perna, pela distancia real dessa
        perna ao mid -- nao pelo delta simetrico.

        Com o skew ativo as pernas ficam em r +/- d a volta do mid enviesado, ou
        seja a d +/- skew do mid verdadeiro: duas distancias distintas, que e o
        que da declive a calibracao. Indexar as duas pelo mesmo d deita fora essa
        informacao e a regressao nunca identifica o k.
        """
        for leg in (self.st.bid, self.st.ask):
            if leg is not None:
                self._buckets.setdefault(self._bucket_key(leg.level_c, mid_c),
                                         [0, 0.0])[1] += dt

    def recalibrate(self) -> None:
        got = calibrate_ak([(dc, f, s) for dc, (f, s) in self._buckets.items()])
        if got is not None:
            self.a_fill, self.k_fill = got

    def requote(self, mid_c, q_others, sd_c, max_skew_c, now) -> None:
        pool_ps = self.st.daily_pool / SECS_DAY
        d, _ = optimal_delta(self.st.size, self.st.max_spread_c, pool_ps, q_others,
                             self.c_loss, self.a_fill, self.k_fill, sd_c=sd_c)
        self.st.regime = placement_regime(d, self.st.max_spread_c)
        # DITHERING: cotar sempre no delta* nao identifica o k -- sem variacao
        # imposta em delta nao ha declive para estimar, e o k e quem decide o
        # regime (MID/INTERIOR/BORDA). Ciclo simetrico a volta do delta*: paga-se
        # algum reward por estar fora do otimo parte do tempo, e compra-se a
        # informacao que diz onde o otimo esta. O regime acima e classificado no
        # delta* verdadeiro, nao no dithered.
        if self.dither_c > 0.0:
            self._dither_phase = (self._dither_phase + 1) % 4
            offset = self.dither_c * (-1.5, -0.5, 0.5, 1.5)[self._dither_phase]
            d = min(max(d + offset, 0.0), self.st.max_spread_c)
        # retirada em burst de fluxo: recua para a borda da band
        self.withdrawn = self.hawkes.is_burst(now)
        if self.withdrawn:
            d = self.st.max_spread_c
        r = reservation_mid_c(mid_c, self.st.inv, self.inv_cap, max_skew_c)
        self.st.delta_c = d
        # O skew desloca as pernas para r +/- d, logo a d +/- skew do mid VERDADEIRO:
        # sem limite, a perna do lado contrario ao inventario sai da band. Fora da
        # band a perna marca zero e, na banda extrema (mid < 10c), Q_min = min(dois
        # lados) = 0 -- o par inteiro deixa de pontuar por causa de uma perna. Manter
        # ambas dentro da band; o skew gasta-se do orcamento da band, nao alem dele.
        band = self.st.max_spread_c
        self.st.bid = Leg("bid", max(r - d, mid_c - band), self.st.size)
        self.st.ask = Leg("ask", min(r + d, mid_c + band), self.st.size)

    def on_fill(self, side, shares, price_c, adverse_c, now, delta_c) -> None:
        """`delta_c` e a distancia a que a perna estava do mid quando encheu, dada
        pelo executor -- nao se recalcula aqui porque o mid ja se moveu com o fill."""
        self.st.fills += 1
        self.hawkes.event(now)
        self._buckets.setdefault(round(delta_c, 1), [0, 0.0])[0] += 1
        if side == "bid":
            tot = self.st.inv + shares
            self.st.avg_c = ((self.st.avg_c * self.st.inv + price_c * shares) / tot
                             if tot > 0 else price_c)
            self.st.inv = tot
        else:
            self.st.inv = max(0.0, self.st.inv - shares)

    def credit(self, mid_c, bids, asks, dt):
        """Reward da amostra: Q_min share * pool_ps * dt. Retorna (amt, q_you, share)."""
        d = self.st.max_spread_c
        q_bid = order_score(abs(mid_c - self.st.bid.level_c), self.st.bid.size, d) if self.st.bid else 0.0
        q_ask = order_score(abs(self.st.ask.level_c - mid_c), self.st.ask.size, d) if self.st.ask else 0.0
        q_you = q_min(q_bid, q_ask, mid_c)
        q_others = (side_q([(abs(mid_c - lvl), sz) for lvl, sz in bids], d)
                    + side_q([(abs(lvl - mid_c), sz) for lvl, sz in asks], d))
        share = pool_share(q_you, q_others)
        amt = share * (self.st.daily_pool / SECS_DAY) * dt
        self.st.realized += amt
        return amt, q_you, share

    def mark(self, mid_c) -> float:
        """PnL nao realizado do inventario ($)."""
        return self.st.inv * (mid_c - self.st.avg_c) / 100.0

    def inv_breach(self) -> bool:
        return self.st.inv > self.inv_cap
