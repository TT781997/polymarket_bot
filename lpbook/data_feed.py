"""Feed real: Gamma (metadados de reward) + CLOB REST (book).

Gamma: https://gamma-api.polymarket.com/markets  campos rewardsMaxSpread,
rewardsMinSize, rewardsDailyRate, clobTokenIds, bestBid/bestAsk, volume24hr.
CLOB:  https://clob.polymarket.com/book?token_id=...  (precos como strings; a
ordenacao dos arrays importa). Gamma bloqueia UAs vazios: usar UA real.

Precisa de rede aberta para os hosts do Polymarket. Nos modos paper/replay nao e
usado; ai o feed vem do fillmodel.
"""
from __future__ import annotations
from dataclasses import dataclass

import httpx

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
UA = "lp-book-pro/1.0 (+https://polymarket.com)"


@dataclass
class RewardMarket:
    market_id: str
    slug: str
    question: str
    token_bid: str          # clobTokenIds[0]
    token_ask: str          # clobTokenIds[1]
    max_spread_c: float
    min_size: float
    daily_pool: float
    best_bid_c: float
    best_ask_c: float
    volume_24h: float

    @property
    def mid_c(self) -> float:
        return (self.best_bid_c + self.best_ask_c) / 2.0


def _f(v, default=0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def fetch_reward_markets(pool_threshold: float, limit: int = 500) -> list[RewardMarket]:
    """Mercados com reward ativo e pool >= pool_threshold ($/dia)."""
    out: list[RewardMarket] = []
    offset = 0
    with httpx.Client(headers={"User-Agent": UA}, timeout=20.0) as cli:
        while True:
            r = cli.get(f"{GAMMA}/markets", params={
                "active": "true", "closed": "false", "archived": "false",
                "limit": 100, "offset": offset,
            })
            r.raise_for_status()
            rows = r.json()
            if not rows:
                break
            for m in rows:
                daily = _f(m.get("rewardsDailyRate"))
                if daily < pool_threshold:
                    continue
                toks = m.get("clobTokenIds") or []
                if isinstance(toks, str):
                    import json
                    toks = json.loads(toks)
                if len(toks) < 2:
                    continue
                out.append(RewardMarket(
                    market_id=str(m.get("id")),
                    slug=m.get("slug", ""),
                    question=m.get("question", ""),
                    token_bid=str(toks[0]),
                    token_ask=str(toks[1]),
                    max_spread_c=_f(m.get("rewardsMaxSpread"), 3.0),
                    min_size=_f(m.get("rewardsMinSize"), 0.0),
                    daily_pool=daily,
                    best_bid_c=_f(m.get("bestBid")) * 100.0,
                    best_ask_c=_f(m.get("bestAsk")) * 100.0,
                    volume_24h=_f(m.get("volume24hr")),
                ))
            offset += 100
            if offset >= limit:
                break
    return out


def fetch_book(token_id: str):
    """Book de um token. Retorna (bids, asks) em [(level_c, size), ...]."""
    with httpx.Client(headers={"User-Agent": UA}, timeout=20.0) as cli:
        r = cli.get(f"{CLOB}/book", params={"token_id": token_id})
        r.raise_for_status()
        b = r.json()
    bids = [(_f(x["price"]) * 100.0, _f(x["size"])) for x in b.get("bids", [])]
    asks = [(_f(x["price"]) * 100.0, _f(x["size"])) for x in b.get("asks", [])]
    return bids, asks
