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
    test_skew_normalized_and_clamped()
    print("\nTODOS OS TESTES PASSARAM")
