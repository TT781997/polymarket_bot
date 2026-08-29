"""Testes das partes puras. Correr: python test_core.py"""
from __future__ import annotations
import math
import random

from scoring import order_score, side_q, q_min, adjusted_mid_c
from optimizer import (optimal_delta, mid_suboptimal, placement_regime, utility,
                       calibrate_ak, reservation_mid_c, stay_factor)


def approx(a, b, tol=1e-9):
    return abs(a - b) <= tol


def test_order_score_quadratic():
    assert approx(order_score(0.0, 1000, 2.0), 1000.0)
    assert approx(order_score(1.0, 1000, 2.0), 250.0)      # metade da distancia -> 1/4
    assert approx(order_score(0.4, 1000, 2.0), 640.0)
    assert order_score(2.5, 1000, 2.0) == 0.0
    assert order_score(0.5, 0, 2.0) == 0.0
    print("ok  order_score quadratico (1/2 distancia -> 1/4 score)")


def test_qmin_bands():
    assert approx(q_min(800, 200, 4.6), 200.0)             # extrema: bilateral
    assert approx(q_min(800, 0, 4.6), 0.0)                 # uma perna -> zero
    assert approx(q_min(900, 0, 50.0, c_penalty=3.0), 300.0)   # central: /c
    assert approx(q_min(600, 600, 50.0), 600.0)
    print("ok  q_min: extrema exige duas pernas, central admite /c")


def test_adjusted_mid_filters_dust():
    bids = [(4.0, 500), (4.4, 5)]
    asks = [(5.2, 500), (4.8, 8)]
    m = adjusted_mid_c(bids, asks, min_size=50)
    assert approx(m, (4.0 + 5.2) / 2.0)
    print("ok  adjusted_mid ignora poeira < min_size")


def test_stay_factor_shape():
    assert stay_factor(0.0, 2.0, 0.6) > 0.99      # no mid, fica na band
    assert stay_factor(2.0, 2.0, 0.6) < 0.30      # na borda, o drift deita fora
    assert stay_factor(1.0, 2.0, 0.6) > stay_factor(1.6, 2.0, 0.6)
    print("ok  stay_factor decai da centralidade para a borda")


def test_three_regimes():
    # A tese corrigida: o otimo e MID, BORDA ou INTERIOR conforme o k medido.
    D, size, q, A, sd = 2.0, 200.0, 3000.0, 0.02, 0.6
    # (a) k baixo + pool rico -> fica no MID (maximiza share, aceita fills)
    d_rich, _ = optimal_delta(size, D, 5000/86400, q, 0.05, A, 0.9, sd_c=sd)
    assert placement_regime(d_rich, D) == "MID", d_rich
    # (b) k baixo + fino/toxico -> BORDA (nao vale a pena; rho rejeita)
    d_thin, _ = optimal_delta(size, D, 21.6/86400, q, 0.05, A, 0.9, sd_c=sd)
    assert placement_regime(d_thin, D) == "BORDA", d_thin
    # (c) k alto (fills concentrados no mid) -> INTERIOR genuino
    d_int, u_int = optimal_delta(size, D, 480/86400, q, 0.9*200/100, A, 6.0, sd_c=sd)
    assert placement_regime(d_int, D) == "INTERIOR", d_int
    assert u_int > 0
    print(f"ok  tres regimes: MID={d_rich:.3f} BORDA={d_thin:.3f} INTERIOR={d_int:.3f} (k=6)")


def test_mid_suboptimal_flags_backoff():
    # mid subotimo NAO garante interior: pode mandar recuar ate a borda
    D, size, q = 2.0, 200.0, 3000.0
    assert mid_suboptimal(size, D, 21.6/86400, q, 0.05, 0.02, 0.9, sd_c=0.6)
    d, _ = optimal_delta(size, D, 21.6/86400, q, 0.05, 0.02, 0.9, sd_c=0.6)
    assert d > 0.0
    print(f"ok  mid_suboptimal sinaliza recuo (delta*={d:.3f}c, aqui ate a borda)")


def test_calibration_recovers_ak():
    A_true, k_true = 0.03, 1.1
    rng = random.Random(3)
    buckets = []
    for dc in (0.2, 0.6, 1.0, 1.4, 1.8):
        lam = A_true * math.exp(-k_true * dc)
        secs = 20000.0
        fills = sum(1 for _ in range(int(secs)) if rng.random() < 1 - math.exp(-lam))
        buckets.append((dc, fills, secs))
    A_hat, k_hat = calibrate_ak(buckets)
    assert abs(k_hat - k_true) < 0.2, f"k {k_hat} vs {k_true}"
    assert abs(A_hat - A_true) / A_true < 0.4, f"A {A_hat} vs {A_true}"
    print(f"ok  calibracao recupera A={A_hat:.4f} (v {A_true}) k={k_hat:.3f} (v {k_true})")


def test_calibration_usa_buckets_vazios():
    # Um bucket com exposicao e ZERO fills e informacao sobre o k. A regressao
    # ln(lambda) tinha de o descartar (ln 0); o MLE de Poisson usa-o.
    povoados = [(0.2, 40, 5000.0), (0.8, 6, 5000.0)]
    _, k_sem = calibrate_ak(povoados)
    _, k_com = calibrate_ak(povoados + [(1.6, 0, 5000.0)])
    assert k_com >= k_sem, f"o bucket vazio devia apertar o k: {k_com} < {k_sem}"
    print(f"ok  bucket vazio entra na estimativa (k {k_sem:.3f} -> {k_com:.3f})")


def test_calibration_recusa_nao_identificavel():
    # (a) uma so distancia: sem declive, por muitos fills que haja
    assert calibrate_ak([(0.4, 500, 9000.0)]) is None
    # (b) fills todos na menor distancia exposta: o MLE foge para k_max, o que e
    #     ausencia de informacao. Melhor devolver None e ficar nos priores.
    assert calibrate_ak([(0.2, 40, 5000.0), (1.4, 0, 5000.0)]) is None
    # (c) poucos fills: nao se vira um regime com 2 observacoes
    assert calibrate_ak([(0.2, 2, 500.0), (0.9, 1, 500.0)], min_fills=5) is None
    print("ok  calibracao recusa historicos nao identificaveis (1 delta / fronteira / poucos fills)")


def test_buckets_por_perna_dao_declive():
    # O bug: indexar as duas pernas pelo delta simetrico colapsa tudo num bucket
    # e a calibracao nunca identifica o k. Com o skew as pernas estao a distancias
    # diferentes do mid verdadeiro -- tem de gerar dois buckets.
    from book_engine import BookEngine, MarketState
    from flow import HawkesBurst
    st = MarketState("T", max_spread_c=2.0, min_size=40, daily_pool=480.0, size=200)
    eng = BookEngine(st, max_skew_c=0.8, inv_cap=600, hawkes=HawkesBurst(0.01, 0.006, 1.2, 4.0),
                     c_loss=1.8, a_fill=0.02, k_fill=6.0)      # regime INTERIOR
    st.inv = 300                                   # metade do cap -> skew de 0.4c
    eng.requote(mid_c=5.0, q_others=3000, sd_c=0.6, max_skew_c=0.8, now=0.0)
    eng.observe_time(60.0, mid_c=5.0)
    chaves = sorted(eng._buckets)
    assert len(chaves) == 2, f"esperava duas distancias distintas, veio {chaves}"
    assert abs((chaves[1] - chaves[0]) - 0.8) < 0.11   # 2 * skew de 0.4c
    print(f"ok  buckets por perna dao dois deltas distintos {chaves} (era um so)")


def test_skew_nao_empurra_perna_para_fora_da_band():
    # Com delta* na borda e inventario no cap, o skew cru poe uma perna fora da
    # band -- e na banda extrema Q_min = min(...) = 0, logo o par deixa de pontuar.
    from book_engine import BookEngine, MarketState
    from flow import HawkesBurst
    st = MarketState("T", max_spread_c=2.0, min_size=40, daily_pool=12.0, size=200)
    eng = BookEngine(st, max_skew_c=0.8, inv_cap=600, hawkes=HawkesBurst(0.01, 0.006, 1.2, 4.0),
                     c_loss=0.05, a_fill=0.02, k_fill=1.0)      # regime BORDA
    st.inv = 600                                                # inventario no cap
    eng.requote(mid_c=5.0, q_others=3000, sd_c=0.6, max_skew_c=0.8, now=0.0)
    for leg in (st.bid, st.ask):
        assert abs(leg.level_c - 5.0) <= st.max_spread_c + 1e-9, \
            f"perna {leg.side} a {abs(leg.level_c - 5.0):.2f}c, fora da band de {st.max_spread_c}c"
    print("ok  skew nunca empurra uma perna para fora da band")


def test_skew_normalized_and_clamped():
    # inventario no cap -> shift = max_skew_c; nunca explode
    r_cap = reservation_mid_c(5.0, inv=600, inv_cap=600, max_skew_c=0.8)
    assert approx(r_cap, 5.0 - 0.8)
    r_half = reservation_mid_c(5.0, inv=300, inv_cap=600, max_skew_c=0.8)
    assert approx(r_half, 5.0 - 0.4)
    r_over = reservation_mid_c(5.0, inv=5000, inv_cap=600, max_skew_c=0.8)  # clamp
    assert approx(r_over, 5.0 - 0.8)
    r_zero = reservation_mid_c(5.0, inv=0, inv_cap=600, max_skew_c=0.8)
    assert approx(r_zero, 5.0)
    print("ok  skew normalizado ao cap e limitado a max_skew_c (sem explosao)")


if __name__ == "__main__":
    test_order_score_quadratic()
    test_qmin_bands()
    test_adjusted_mid_filters_dust()
    test_stay_factor_shape()
    test_three_regimes()
    test_mid_suboptimal_flags_backoff()
    test_calibration_recovers_ak()
    test_calibration_usa_buckets_vazios()
    test_calibration_recusa_nao_identificavel()
    test_buckets_por_perna_dao_declive()
    test_skew_nao_empurra_perna_para_fora_da_band()
    test_skew_normalized_and_clamped()
    print("\nTODOS OS TESTES PASSARAM")
