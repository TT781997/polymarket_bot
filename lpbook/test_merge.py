"""Testes das pecas trazidas dos bots XRP do repo. Correr: python test_merge.py"""
from __future__ import annotations

from scoring import order_score, q_min
from fees import (taker_fee, maker_rebate, rebate_per_share, flatten_cost,
                  net_fill_cost_per_share, MAKER_REBATE_BPS)
from toxicity import ToxicitySignal, BayesianSignal
from shadow import ShadowFillEngine
from book_engine import Leg


def test_taker_fee_quase_zero_nos_extremos():
    # a fee e quadratica no centro e anula-se nos extremos: e por isso que no
    # regime alvo (<5c) escoar a mercado custa pouco -- mas nao zero.
    assert taker_fee(1000, 0.50) > taker_fee(1000, 0.046) * 50
    assert 0.02 < taker_fee(1000, 0.046) < 0.03
    assert taker_fee(1000, 0.0) == 0.0 and taker_fee(1000, 1.0) == 0.0
    assert flatten_cost(1000, 4.6) == taker_fee(1000, 0.046)
    print(f"ok  taker fee ~0 nos extremos (1000sh @4.6c = ${taker_fee(1000, 0.046):.4f})")


def test_rebate_desligado_por_omissao():
    # O rebate de maker vem do default de um bot do repo, nao da doc do Polymarket.
    # Ligado por omissao faria o fill parecer receita -- o que a spec proibe.
    assert MAKER_REBATE_BPS == 0.0
    assert maker_rebate(1000, 0.046) == 0.0
    assert net_fill_cost_per_share(4.6, 1.2) == 1.2 / 100.0      # adverso puro
    # e mesmo ligado a 20 bps a magnitude e irrelevante face ao adverso
    frac = rebate_per_share(4.6, 20.0) / (1.2 / 100.0)
    assert frac < 0.02, frac
    print(f"ok  rebate desligado por omissao (a 20bps valeria {frac*100:.2f}% do adverso)")


def test_delta_assimetrico_destroi_qmin():
    """O motivo pelo qual o skew assimetrico do sinal foi REJEITADO.

    Na banda extrema Q_min = min(Q_bid, Q_ask): a perna pior fixa a pontuacao.
    Apertar uma e alargar a outra troca quase todo o reward por esquivar fills
    de um lado. Correto para um market maker direcional, errado para farming.
    """
    D, size, mid = 2.0, 200.0, 4.6
    def qmin(d_bid, d_ask):
        return q_min(order_score(d_bid, size, D), order_score(d_ask, size, D), mid)

    base, gain_c = 0.0, 3.5                       # gain do port original
    retido = {}
    for lean in (0.10, 0.30, 0.50):
        shift = lean * gain_c
        d_b = max(0.0, min(D, base - shift))
        d_a = max(0.0, min(D, base + shift))
        retido[lean] = qmin(d_b, d_a) / qmin(base, base)
    assert retido[0.10] < 0.75, retido           # ja perde 1/3 com vies pequeno
    assert retido[0.50] < 0.05, retido           # no vies maximo sobra ~1.6%
    assert retido[0.10] > retido[0.30] > retido[0.50]
    print(f"ok  delta assimetrico destroi Q_min (retem {retido[0.10]*100:.0f}% / "
          f"{retido[0.30]*100:.0f}% / {retido[0.50]*100:.1f}% do reward)")


def test_sinal_entra_como_custo_simetrico():
    # A toxicidade prevista multiplica o custo por fill; o delta* re-resolve
    # simetrico. Desligado por omissao (gain=0).
    assert ToxicitySignal().cost_multiplier(0.046) == 1.0
    sig = ToxicitySignal(gain=1.0)
    for _ in range(40):                           # deriva persistente para cima
        sig.on_underlying(sig._last_px * 1.01 if sig._last_px else 0.046)
    assert sig.lean(0.046) > 0.03
    m = sig.cost_multiplier(0.046)
    assert 1.0 < m <= 2.0, m
    # simetrico por construcao: o sinal nao devolve deltas por perna
    assert not hasattr(sig, "per_leg_deltas")
    print(f"ok  sinal entra como custo simetrico (multiplicador {m:.2f}x, off por omissao)")


def test_bayesiano_nao_satura_e_centra_em_lateral():
    # o port do repo saturava: sem esquecimento nem winsorizacao, ret/var explode
    # fora dos retornos minusculos do XRP a 5 min.
    # sob deriva persistente a crenca satura (isso e correto), mas o esquecimento
    # tem de a trazer de volta quando a deriva para -- sem `decay` ficava presa
    # para sempre, que era o defeito do port direto do bot de 5 min com reset.
    b = BayesianSignal()
    for _ in range(200):
        b.update(+0.05)
    assert b.p_up > 0.99, b.p_up
    for _ in range(60):
        b.update(0.0)
    assert abs(b.p_up - 0.5) < 0.02, b.p_up      # a crenca liberta-se
    lateral = BayesianSignal()
    px, sinal = 0.046, []
    import random
    rng = random.Random(5)
    for _ in range(300):
        novo = px * (1 + rng.gauss(0, 0.01))
        lateral.update((novo - px) / px)
        px = novo
        sinal.append(lateral.p_up - 0.5)
    media = sum(sinal) / len(sinal)
    assert abs(media) < 0.10, media               # ~zero em lateral
    print(f"ok  bayesiano nao satura e centra em lateral (media {media:+.3f})")


def test_shadow_enche_quando_o_livro_cruza():
    eng = ShadowFillEngine(latency_ms=0.0, fill_prob=1.0, partial_frac=0.3, seed=1)
    bid = Leg("bid", 4.0, 500)
    ask = Leg("ask", 6.0, 500)
    # livro que cruza o bid (melhor ask 3.9 <= 4.0) e nao cruza o ask
    fills = eng.try_match(bid, ask, [(3.5, 400)], [(3.9, 400)], now=1.0, placed_ts=0.0)
    assert len(fills) == 1 and fills[0].side == "bid"
    assert 0 < fills[0].shares <= bid.size
    assert fills[0].rebate == 0.0                 # rebate desligado por omissao
    # livro que nao cruza nenhuma perna
    assert eng.try_match(bid, ask, [(3.5, 400)], [(5.0, 400)], now=1.0, placed_ts=0.0) == []
    print("ok  shadow enche so quando o livro cruza a perna")


if __name__ == "__main__":
    test_taker_fee_quase_zero_nos_extremos()
    test_rebate_desligado_por_omissao()
    test_delta_assimetrico_destroi_qmin()
    test_sinal_entra_como_custo_simetrico()
    test_bayesiano_nao_satura_e_centra_em_lateral()
    test_shadow_enche_quando_o_livro_cruza()
    print("\nTODOS OS TESTES DE FUSAO PASSARAM")
