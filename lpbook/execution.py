"""Camada de execucao separavel.

paper: fills vindos do modelo sintetico. live: ordens CLOB reais (post-only),
armadas so com --live-confirm e com control.json kill=false. Sem confirmacao
explicita, nunca coloca ordens reais.
"""
from __future__ import annotations

from state import kill_active


class PaperExecutor:
    """Em paper as pernas vivem no engine; os fills vem do SyntheticMarket."""

    def __init__(self, market):
        self.market = market

    def place(self, bid, ask) -> None:
        pass

    def poll_fills(self, st, dt):
        """Devolve [(side, shares, level_c, adverse_c, delta_c), ...].

        `delta_c` e a distancia da perna ao mid ANTES do fill -- e a distancia que
        gerou o evento, e e a que a calibracao tem de registar (depois do fill o
        mid ja saltou com o movimento adverso).
        """
        out = []
        if st.bid:
            d = abs(self.market.mid_c - st.bid.level_c)
            sh, adv = self.market.try_fill("bid", d, st.bid.size, dt)
            if sh > 0:
                out.append(("bid", sh, st.bid.level_c, adv, d))
        if st.ask:
            d = abs(st.ask.level_c - self.market.mid_c)
            sh, adv = self.market.try_fill("ask", d, st.ask.size, dt)
            if sh > 0:
                out.append(("ask", sh, st.ask.level_c, adv, d))
        return out


class LiveExecutor:
    """Coloca ordens reais no CLOB. Reutiliza um cliente py-clob-client ja
    autenticado (mesma infra dos bots XRP). Requer arm() explicito."""

    def __init__(self, clob_client, token_id_bid, token_id_ask, control_path):
        self.clob = clob_client
        self.token_bid = token_id_bid    # token do lado onde poe o bid
        self.token_ask = token_id_ask    # complementar (ask = bid no outro token)
        self.control_path = control_path
        self.armed = False
        self._live_ids: dict[str, str] = {}

    def arm(self, confirm_flag: bool) -> None:
        if not confirm_flag:
            raise RuntimeError("live requer --live-confirm explicito")
        if kill_active(self.control_path):
            raise RuntimeError("control.json kill=true: execucao bloqueada")
        self.armed = True

    def _post(self, token_id, side, price_c, size):
        # post-only limit order via py-clob-client
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY, SELL
        args = OrderArgs(
            token_id=token_id,
            price=round(price_c / 100.0, 3),
            size=size,
            side=BUY if side == "bid" else SELL,
        )
        signed = self.clob.create_order(args)
        resp = self.clob.post_order(signed, OrderType.GTC)
        return resp.get("orderID") if isinstance(resp, dict) else None

    def place(self, bid, ask) -> None:
        if not self.armed:
            raise RuntimeError("executor live nao armado")
        if kill_active(self.control_path):
            self.cancel_all()
            raise RuntimeError("control.json kill=true: pernas retiradas")
        self.cancel_all()
        if bid:
            self._live_ids["bid"] = self._post(self.token_bid, "bid", bid.level_c, bid.size)
        if ask:
            self._live_ids["ask"] = self._post(self.token_ask, "bid", 100.0 - ask.level_c, ask.size)

    def cancel_all(self) -> None:
        for oid in filter(None, self._live_ids.values()):
            try:
                self.clob.cancel(oid)
            except Exception:
                pass
        self._live_ids.clear()

    def poll_fills(self, st, dt):
        # em live, os fills chegam pelo WS de user; ligar ao mesmo listener dos
        # bots XRP e encaminhar para BookEngine.on_fill. Sem WS, nao inventa fills.
        # Mesmo contrato do PaperExecutor: (side, shares, level_c, adverse_c,
        # delta_c), com delta_c medido contra o mid do momento do fill.
        return []
