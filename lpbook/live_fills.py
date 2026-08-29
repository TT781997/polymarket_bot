"""Fills reais do WS de user -> contrato do BookEngine.

Em `paper` o gerador devolve o movimento adverso NO MESMO instante do fill. Em
live isso e impossivel: no momento em que enches so sabes o preco a que encheste.
O movimento adverso e o que o mid faz A SEGUIR -- e a definicao de selecao adversa.

Ligar o WS de user "ingenuamente" (adverse_c = 0 no momento do fill) faria a
ferramenta concluir que os fills sao gratis, que e exatamente o erro que ela
existe para evitar. Por isso os fills entram numa fila e so sao liquidados depois
de um horizonte, com o deslocamento do mid medido. So ai alimentam o custo e a
calibracao.

O envelope segue o que os bots deste repo ja fazem no WS de user
(`xrp_bot_v9_4_1.py:2920`): dict ou lista de dicts, discriminados por
`event_type`, com `asset_id`; auth L2 no `connect`; PING/PONG a cada 4s;
reconexao com backoff. Os campos da mensagem de trade NAO foram confirmados
contra a doc (rede bloqueada), por isso o parser exige explicitamente o que
precisa e conta o que nao percebeu em vez de inventar um fill.
"""
from __future__ import annotations
from dataclasses import dataclass


@dataclass
class PendingFill:
    side: str           # "bid" (compraste) | "ask" (vendeste)
    shares: float
    level_c: float      # preco a que a perna estava
    delta_c: float      # distancia ao mid no momento do fill (para a calibracao)
    mid_at_fill_c: float
    ts: float


def _f(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


class FillRouter:
    """Traduz mensagens do WS de user em fills do motor, com o adverso medido.

    `horizon_s`: quanto tempo esperar antes de medir o movimento adverso. Curto
    demais mede ruido; longo demais atribui ao fill movimento que nao e dele.
    O intervalo de requote e um default defensavel.
    """

    def __init__(self, horizon_s: float = 60.0):
        self.horizon_s = horizon_s
        self._legs: dict[str, tuple[str, float]] = {}   # order_id -> (side, level_c)
        self._pendentes: list[PendingFill] = []
        self.ignoradas = 0        # mensagens que nao sao trades nossos
        self.nao_parseadas = 0    # trades nossos que o parser nao percebeu

    def register_leg(self, order_id: str, side: str, level_c: float) -> None:
        self._legs[order_id] = (side, level_c)

    def forget_leg(self, order_id: str) -> None:
        self._legs.pop(order_id, None)

    def on_message(self, msg, mid_c: float, now: float) -> int:
        """Consome uma mensagem (dict ou lista) e enfileira os fills. Devolve
        quantos enfileirou. Nunca inventa um fill: o que nao percebe, conta."""
        itens = msg if isinstance(msg, list) else [msg]
        n = 0
        for item in itens:
            if not isinstance(item, dict):
                self.ignoradas += 1
                continue
            if item.get("event_type") != "trade":
                self.ignoradas += 1
                continue
            oid = item.get("order_id") or item.get("maker_order_id")
            if oid not in self._legs:
                self.ignoradas += 1       # trade de outro participante
                continue
            shares = _f(item.get("size"))
            preco = _f(item.get("price"))
            if shares is None or preco is None or shares <= 0:
                self.nao_parseadas += 1   # e nosso mas nao da para ler: NAO inventar
                continue
            side, level_c = self._legs[oid]
            preco_c = preco * 100.0
            self._pendentes.append(PendingFill(
                side=side, shares=shares, level_c=preco_c,
                delta_c=abs(preco_c - mid_c), mid_at_fill_c=mid_c, ts=now))
            n += 1
        return n

    def settle(self, mid_c: float, now: float) -> list[tuple]:
        """Liquida os fills cujo horizonte passou. Devolve o mesmo contrato do
        PaperExecutor: (side, shares, level_c, adverse_c, delta_c).

        O adverso e SINALIZADO: um fill que correu bem entra com valor negativo e
        baixa o custo medido. Clampar a zero enviesava o custo para cima e fazia a
        ferramenta rejeitar mercados bons.
        """
        prontos, restantes = [], []
        for p in self._pendentes:
            if now - p.ts < self.horizon_s:
                restantes.append(p)
                continue
            # compraste (bid): adverso e o mid CAIR. vendeste (ask): e o mid SUBIR.
            desloc = (p.mid_at_fill_c - mid_c) if p.side == "bid" else (mid_c - p.mid_at_fill_c)
            prontos.append((p.side, p.shares, p.level_c, desloc, p.delta_c))
        self._pendentes = restantes
        return prontos

    @property
    def pendentes(self) -> int:
        return len(self._pendentes)


class AdverseEstimator:
    """Media corrente do movimento adverso por share, para alimentar o `c_loss`.

    Mantem o valor sinalizado para relatorio, mas entrega ao optimizer um custo
    nunca negativo: um `c_loss` < 0 inverteria o sinal do termo de custo e o
    optimizer passaria a QUERER fills.
    """

    def __init__(self, janela: int = 200):
        self.janela = janela
        self._amostras: list[float] = []

    def add(self, adverse_c: float) -> None:
        self._amostras.append(adverse_c)
        if len(self._amostras) > self.janela:
            self._amostras.pop(0)

    @property
    def media_sinalizada_c(self) -> float:
        return sum(self._amostras) / len(self._amostras) if self._amostras else 0.0

    def c_loss(self, size: float, fill_frac: float) -> float:
        """Custo esperado por evento de fill ($), nunca negativo."""
        return max(0.0, self.media_sinalizada_c) * fill_frac * size / 100.0

    @property
    def n(self) -> int:
        return len(self._amostras)
