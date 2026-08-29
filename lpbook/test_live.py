"""Testes do caminho de fills reais (WS de user). Correr: python test_live.py"""
from __future__ import annotations

from live_fills import FillRouter, AdverseEstimator


def _trade(oid, price, size):
    return {"event_type": "trade", "order_id": oid, "price": price, "size": size}


def test_router_so_aceita_trades_nossos():
    r = FillRouter(horizon_s=60.0)
    r.register_leg("A", "bid", 4.5)
    assert r.on_message(_trade("A", 0.045, 100), mid_c=5.0, now=0.0) == 1
    assert r.on_message(_trade("OUTRO", 0.045, 100), mid_c=5.0, now=0.0) == 0
    assert r.on_message({"event_type": "market_resolved"}, 5.0, 0.0) == 0
    assert r.on_message("PONG", 5.0, 0.0) == 0
    assert r.pendentes == 1 and r.ignoradas == 3
    print("ok  router aceita so os nossos trades e ignora o resto")


def test_router_nunca_inventa_fill():
    # mensagem nossa mas ilegivel: conta como nao parseada, NAO vira um fill
    r = FillRouter()
    r.register_leg("A", "bid", 4.5)
    assert r.on_message({"event_type": "trade", "order_id": "A", "size": "?"}, 5.0, 0.0) == 0
    assert r.on_message({"event_type": "trade", "order_id": "A", "price": 0.045}, 5.0, 0.0) == 0
    assert r.nao_parseadas == 2 and r.pendentes == 0
    print("ok  router conta o que nao percebe em vez de inventar um fill")


def test_adverso_so_e_conhecido_depois_do_horizonte():
    # e a diferenca essencial face ao paper: no instante do fill o adverso nao existe
    r = FillRouter(horizon_s=60.0)
    r.register_leg("A", "bid", 4.5)
    r.on_message(_trade("A", 0.045, 100), mid_c=5.0, now=0.0)
    assert r.settle(mid_c=4.0, now=30.0) == []      # ainda dentro do horizonte
    prontos = r.settle(mid_c=4.0, now=61.0)
    assert len(prontos) == 1
    side, shares, level_c, adverse_c, delta_c = prontos[0]
    assert side == "bid" and shares == 100
    assert abs(adverse_c - 1.0) < 1e-9              # compraste e o mid caiu 1c
    assert abs(delta_c - 0.5) < 1e-9               # 4.5c contra mid 5.0c
    assert r.pendentes == 0
    print("ok  adverso medido so depois do horizonte (1c de queda apos compra)")


def test_adverso_sinalizado_nos_dois_lados():
    r = FillRouter(horizon_s=0.0)
    r.register_leg("B", "bid", 4.5)
    r.register_leg("S", "ask", 5.5)
    r.on_message([_trade("B", 0.045, 10), _trade("S", 0.055, 10)], mid_c=5.0, now=0.0)
    por_lado = {p[0]: p[3] for p in r.settle(mid_c=6.0, now=1.0)}
    assert por_lado["bid"] < 0, por_lado          # comprou e subiu: correu BEM
    assert por_lado["ask"] > 0, por_lado          # vendeu e subiu: toxico
    print(f"ok  adverso sinalizado (bid {por_lado['bid']:+.1f}c, ask {por_lado['ask']:+.1f}c)")


def test_estimador_nunca_devolve_custo_negativo():
    # um c_loss negativo inverteria o termo de custo e o optimizer passaria a
    # QUERER fills -- o oposto de tudo o que a ferramenta faz.
    e = AdverseEstimator()
    for v in (-2.0, -1.0, -3.0):
        e.add(v)
    assert e.media_sinalizada_c < 0                 # reporta a verdade
    assert e.c_loss(size=200, fill_frac=0.15) == 0.0
    e2 = AdverseEstimator()
    for v in (1.0, 2.0):
        e2.add(v)
    assert e2.c_loss(size=200, fill_frac=0.15) > 0
    print("ok  estimador reporta adverso negativo mas nunca entrega custo negativo")


if __name__ == "__main__":
    test_router_so_aceita_trades_nossos()
    test_router_nunca_inventa_fill()
    test_adverso_so_e_conhecido_depois_do_horizonte()
    test_adverso_sinalizado_nos_dois_lados()
    test_estimador_nunca_devolve_custo_negativo()
    print("\nTODOS OS TESTES DO CAMINHO LIVE PASSARAM")
