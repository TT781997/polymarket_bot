"""LP BOOK PRO -- versao corrigida e superior do lp_book.py do video.

Corrige o erro central da referencia (cotar no mid) colocando em delta* com share
explicita, calibrado a fills reais, e seleciona mercados por liquido esperado com
guardrail rho. Modos: scan, paper, replay, live.

Uso:
  python lp_book_pro.py --mode paper  --bankroll 20
  python lp_book_pro.py --mode scan   --bankroll 20            (precisa de rede)
  python lp_book_pro.py --mode live   --bankroll 20 --live-confirm   (rede+CLOB)
"""
from __future__ import annotations
import argparse
import math
import time
from collections import deque

from flow import HawkesBurst
from fillmodel import SyntheticMarket
from toxicity import ToxicitySignal
from book_engine import MarketState, BookEngine, SECS_DAY
from execution import PaperExecutor
from selector import evaluate_market, rank_markets
from state import atomic_write, kill_active
import tui

# priors de calibracao antes de haver fills medidos
A0, K0 = 0.0004, 0.9        # fills/s no mid (evento), decaimento por centimo
FILL_FRAC = 0.15            # fatia media da perna por evento de fill
C_LOSS = None                # derivado por mercado da toxicidade
INV_CAP_MULT = 3.0           # cap de inventario = mult * size da perna
MAX_SKEW_C = 0.8             # shift maximo do skew A-S (c) com inv no cap
VOL_C_PER_S = 0.03           # vol do mid (c/sqrt(s)) para o drift/sd
REQUOTE_S = 60.0             # intervalo de requote (s)
DITHER_FRAC = 0.30           # amplitude do dithering de delta, em fracao da band
GAMMA = 0.0                  # placeholder removido abaixo


def c_loss_from(toxicity_c, size):
    """Perda esperada por EVENTO de fill = movimento adverso * fatia media, em $."""
    return toxicity_c * FILL_FRAC * size / 100.0


def run_paper(args):
    console_sleep = args.speed
    # universo sintetico (substitui o scan real de ~3255 mercados)
    universe = [
        SyntheticMarket("MLB-SD-COL-F5", 4.6, 2.0, 100, 21.60, A0, K0, toxicity_c=1.2, comp=1800, seed=7+args.seed),
        SyntheticMarket("NHL-VGK-EDM-P2", 6.0, 2.0, 100, 0.60, A0, K0, toxicity_c=1.0, comp=900, seed=8+args.seed),
        SyntheticMarket("NBA-DEN-MIN-U8", 3.2, 2.0, 100, 0.85, A0, K0, toxicity_c=1.4, comp=1400, seed=9+args.seed),
        SyntheticMarket("EPL-BHA-EVE-BTS", 5.0, 3.0, 50, 6.40, A0, K0, toxicity_c=2.6, comp=2200, seed=10+args.seed),
        SyntheticMarket("TEN-QUAL-R1-S1", 5.0, 2.0, 40, 12.00, A0, K0, toxicity_c=0.3, comp=200, seed=11+args.seed),
    ]

    # seleccao: avalia cada mercado no seu delta*, aplica rho e feasibilidade
    evals = []
    for m in universe:
        evals.append(evaluate_market(
            m.market_id, m.mid_c, m.max_spread_c, m.min_size, m.daily_pool,
            q_others=m.comp,
            c_loss_per_share=m.toxicity_c * FILL_FRAC / 100.0,
            a_fill=A0, k_fill=K0, bankroll=args.bankroll, rho_max=args.rho_max))

    print("\n== SCAN (paper) ==")
    for e in sorted(evals, key=lambda x: x.net_daily, reverse=True):
        flag = "ARM " if e.reason == "ok" else "skip"
        print(f"{flag} {e.market_id:16s} pool ${e.daily_pool:6.2f}/d  "
              f"delta* {e.delta_c:.2f}c  net/d ${e.net_daily:+7.2f}  "
              f"rho {e.rho:5.2f}  {e.reason}")

    ranked = rank_markets(evals, args.rho_max)
    observe = not ranked
    if observe:
        print("\nNenhum mercado passa o filtro ao bankroll/rho dados -- o correto "
              "a $%.0f. Corro o melhor em OBSERVACAO so para mostrar a mecanica "
              "(nao ha lucro a farmar).\n" % args.bankroll)
        top = max(evals, key=lambda e: e.net_daily)
    else:
        top = ranked[0]
    market = next(m for m in universe if m.market_id == top.market_id)

    st = MarketState(top.market_id, top.max_spread_c, top.min_size,
                     top.daily_pool, size=max(top.size, top.min_size))
    hawkes = HawkesBurst(mu=A0, alpha=0.6 * K0, beta=1.2, mult=4.0)
    sinal = ToxicitySignal(gain=args.signal_gain) if args.signal_gain > 0 else None
    eng = BookEngine(st, MAX_SKEW_C, INV_CAP_MULT * st.size, hawkes,
                     c_loss_from(market.toxicity_c, st.size), A0, K0,
                     dither_c=args.dither_frac * st.max_spread_c, signal=sinal)
    execu = PaperExecutor(market)

    log: deque = deque(maxlen=40)
    log.appendleft(f"[green]arm[/]  {st.market_id}  pool ${st.daily_pool:.2f}/d  size {st.size:.0f}/perna")

    dt = 20.0                     # passo de amostra (s)
    total_secs = args.hours * 3600
    t = 0.0
    pnl_realized = 0.0
    share = 0.0
    from rich.live import Live
    from rich.console import Console
    con = Console()
    with Live(console=con, refresh_per_second=8, screen=False) as live:
        while t < total_secs:
            if kill_active(args.control):
                log.appendleft("[red]control.json kill=true -> stop[/]")
                break
            market.step_mid(dt)
            if sinal is not None:
                # PROXY: alimentar o sinal com o proprio mid. Em paper isto NAO pode
                # ter poder preditivo -- o mid do gerador e um martingale (drift=0),
                # portanto o sinal so mede momentum de ruido. Em producao, ligar aqui
                # o feed do subjacente (Binance WS para cripto, oraculo para desporto).
                sinal.on_underlying(market.mid_c / 100.0)
            sd_c = VOL_C_PER_S * math.sqrt(REQUOTE_S)
            eng.requote(market.mid_c, q_others=market.comp, sd_c=sd_c, max_skew_c=MAX_SKEW_C, now=t)
            st.withdrawn_flag = eng.withdrawn
            # exposicao contada DEPOIS do requote: e nestas pernas, a estas
            # distancias, que os fills deste passo vao ser gerados. Contar antes
            # atribuia o tempo as pernas antigas e os fills as novas.
            eng.observe_time(dt, market.mid_c)

            for side, sh, px, adv, d_fill in execu.poll_fills(st, dt):
                eng.on_fill(side, sh, px, adv, t, d_fill)
                log.appendleft(f"[red]fill[/] {side} {sh:.0f}@{px:.1f}c  adverse {adv:.1f}c  inv {st.inv:.0f}")
                if eng.inv_breach():
                    inv_antes = st.inv
                    pnl_realized += eng.flatten(market.mid_c)   # cruza o spread: paga taker
                    log.appendleft(f"[bold red]flatten[/] inv {inv_antes:.0f} > cap {eng.inv_cap:.0f}")

            bids, asks = market.book()
            amt, q_you, share = eng.credit(market.mid_c, bids, asks, dt)
            pnl_realized += amt
            if int(t) % 200 == 0:
                eng.recalibrate()
            mark = eng.mark(market.mid_c)
            pnl_net = pnl_realized + mark

            # taxa diaria projetada e perda/fill para a linha de selecao adversa
            proj_reward_d = share * st.daily_pool  # share * pool_ps * SECS_DAY
            per_fill = c_loss_from(market.toxicity_c, st.size)
            fills_d = eng.a_fill * math.exp(-eng.k_fill * st.delta_c) * SECS_DAY
            proj_cost_d = per_fill * fills_d
            rho = proj_cost_d / proj_reward_d if proj_reward_d > 0 else float("inf")

            clock = f"{int(t)//3600:02d}:{(int(t)%3600)//60:02d}"
            if int(t) % 60 == 0:
                log.appendleft(f"[grey54]credit[/] +{amt*3:.3f}  q {q_you:.0f}  bal {pnl_realized:.2f}")

            atomic_write(args.state, {
                "market": st.market_id, "pnl_net": round(pnl_net, 4),
                "realized": round(pnl_realized, 4), "inv": st.inv,
                "delta_c": round(st.delta_c, 3), "share": round(share, 4),
                "rho": round(rho, 3), "fills": st.fills, "t": t,
            })

            live.update(tui.render(
                st, market.mid_c, bids, asks, pnl_net, args.bankroll, share, clock,
                proj_reward_d, proj_cost_d, per_fill, rho, args.rho_max, mark, log))
            time.sleep(console_sleep)
            t += dt

    print(f"\n== fim paper ==  realizado ${pnl_realized:.2f}  "
          f"mark ${eng.mark(market.mid_c):+.2f}  "
          f"liquido ${pnl_realized + eng.mark(market.mid_c):+.2f}  fills {st.fills}")
    print(f"A/k calibrados: A={eng.a_fill:.4f} k={eng.k_fill:.3f} "
          f"(priors A={A0} k={K0})")


def run_scan(args):
    from data_feed import fetch_reward_markets, fetch_book
    from scoring import side_q
    mkts = fetch_reward_markets(args.pool_threshold, limit=args.limit)
    evals = []
    for m in mkts:
        try:
            bids, asks = fetch_book(m.token_bid)
        except Exception:
            bids, asks = [], []
        q_others = (side_q([(abs(m.mid_c - l), s) for l, s in bids], m.max_spread_c)
                    + side_q([(abs(l - m.mid_c), s) for l, s in asks], m.max_spread_c))
        evals.append(evaluate_market(
            m.slug or m.market_id, m.mid_c, m.max_spread_c, m.min_size, m.daily_pool,
            q_others=q_others, c_loss_per_share=1.0 * FILL_FRAC / 100.0,  # toxicidade default
            a_fill=A0, k_fill=K0, bankroll=args.bankroll, rho_max=args.rho_max))
    for e in rank_markets(evals, args.rho_max):
        print(f"{e.market_id:40s} pool ${e.daily_pool:8.2f}/d  delta* {e.delta_c:.2f}c  "
              f"net/d ${e.net_daily:+8.2f}  rho {e.rho:.2f}")


def main():
    p = argparse.ArgumentParser(description="LP BOOK PRO")
    p.add_argument("--mode", choices=["scan", "paper", "replay", "live"], default="paper")
    p.add_argument("--bankroll", type=float, default=20.0)
    p.add_argument("--rho-max", dest="rho_max", type=float, default=0.6)
    p.add_argument("--pool-threshold", type=float, default=1.0)
    p.add_argument("--limit", type=int, default=500)
    p.add_argument("--hours", type=float, default=24.0)
    p.add_argument("--speed", type=float, default=0.04, help="sleep por passo na TUI (s)")
    p.add_argument("--state", default="lp_state.json")
    p.add_argument("--control", default="control.json")
    p.add_argument("--seed", type=int, default=0,
                   help="desloca as seeds do universo sintetico (para repetir corridas)")
    p.add_argument("--signal-gain", dest="signal_gain", type=float, default=0.0,
                   help="sinal de toxicidade (LMSR+Bayesiano) como multiplicador do "
                        "custo por fill; 0 = desligado. NAO VALIDADO: no harness "
                        "paper o mid e um martingale, logo nao pode ter edge")
    p.add_argument("--dither-frac", dest="dither_frac", type=float, default=DITHER_FRAC,
                   help="amplitude do dithering de delta em fracao da band; 0 desliga "
                        "(e entao o k nao se identifica)")
    p.add_argument("--live-confirm", dest="live_confirm", action="store_true")
    args = p.parse_args()

    if args.mode == "paper" or args.mode == "replay":
        run_paper(args)
    elif args.mode == "scan":
        run_scan(args)
    elif args.mode == "live":
        raise SystemExit("live: ligar data_feed + py-clob-client autenticado e o "
                         "WS de user, depois usar LiveExecutor.arm(--live-confirm). "
                         "Bloqueado ate a infra CLOB estar ligada.")


if __name__ == "__main__":
    main()
