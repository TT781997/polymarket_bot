"""Shadow fill contra livro L2 (adaptado do ShadowFillEngine do repo XRP).

Modo paper realista: as pernas post-only enchem quando o livro as cruza, com
latencia, slippage, fills parciais e rebate de maker. Online corre contra o livro
real do CLOB; offline contra o livro do fillmodel. Fonte de livro agnostica.
"""
from __future__ import annotations
from dataclasses import dataclass
import random

from fees import maker_rebate, MAKER_REBATE_BPS


@dataclass
class ShadowFill:
    side: str          # "bid" | "ask"
    order_price_c: float
    fill_price_c: float
    shares: float
    rebate: float
    slippage_pct: float


class ShadowFillEngine:
    def __init__(self, latency_ms=100.0, fill_prob=0.6, max_slippage_pct=0.02,
                 partial_frac=0.3, bps=MAKER_REBATE_BPS, seed=None):
        self.latency_s = latency_ms / 1000.0
        self.fill_prob = fill_prob
        self.max_slip = max_slippage_pct
        self.partial = partial_frac
        self.bps = bps
        self.rng = random.Random(seed)

    def try_match(self, bid, ask, book_bids, book_asks, now, placed_ts):
        """`book_bids`/`book_asks`: [(level_c, size), ...] do livro. Uma perna de
        compra enche se o melhor ask do livro <= preco do bid; venda se o melhor
        bid do livro >= preco do ask. Fatia da liquidez disponivel."""
        fills = []
        if now - placed_ts < self.latency_s:
            return fills
        best_ask = min((l for l, _ in book_asks), default=None)
        best_bid = max((l for l, _ in book_bids), default=None)

        if bid is not None and best_ask is not None and best_ask <= bid.level_c:
            liq = sum(s for l, s in book_asks if l <= bid.level_c)
            if liq > 0 and self.rng.random() <= self.fill_prob:
                fills.append(self._fill("bid", bid, liq))
        if ask is not None and best_bid is not None and best_bid >= ask.level_c:
            liq = sum(s for l, s in book_bids if l >= ask.level_c)
            if liq > 0 and self.rng.random() <= self.fill_prob:
                fills.append(self._fill("ask", ask, liq))
        return fills

    def _fill(self, side, leg, liq):
        shares = min(leg.size, max(0.01, liq * self.partial))
        slip = self.rng.uniform(0.0, self.max_slip)
        if side == "bid":
            fp = leg.level_c * (1.0 + slip)
        else:
            fp = leg.level_c * (1.0 - slip)
        fp = max(0.5, min(99.5, fp))
        reb = maker_rebate(shares, fp / 100.0, self.bps)
        return ShadowFill(side, leg.level_c, fp, shares, reb, abs(fp - leg.level_c) / (leg.level_c + 1e-9))
