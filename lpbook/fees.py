"""Fees do Polymarket (portado dos bots XRP do repo).

    taker: fee = shares * price * 0.25 * (price*(1-price))^2

Quadratica no centro, ~0 nos extremos: 1000 shares a 4.6c custam $0.022 em taker
fee. Interessa-nos no caminho de FLATTEN -- escoar inventario a mercado cruza o
spread e paga taker. As pernas de farming sao post-only e nunca pagam taker.

    maker: rebate = shares * price * bps/10000

AVISO -- o rebate de maker esta DESLIGADO por omissao (bps = 0), de proposito:

  1. Proveniencia. Os 20 bps vem do default `maker_rebate_bps` do
     `xrp_true_market_maker_v5_3_1.py` deste repo. Isso e a configuracao de um bot,
     nao documentacao do Polymarket. Nao foi possivel confirmar contra a doc
     oficial (rede bloqueada), e um rebate por fill e distinto do programa de
     Liquidity Rewards -- que e a receita que esta ferramenta ja modela.
  2. Direcao do erro. Ligar o rebate faz o fill parecer parcialmente receita, que
     e exatamente o que a spec proibe ("nao tratar fills como receita em lado
     nenhum da contabilidade") e o modo de falha que levou o video a -17.
  3. Magnitude. Mesmo a 20 bps o rebate vale 0.5%-1.3% do movimento adverso tipico
     (a 4.6c: $0.000092/share contra $0.012/share de adverso). Nao muda decisao
     nenhuma -- so torna as projecoes ligeiramente mais otimistas.

Confirmado o rebate na doc oficial, passar `bps=20` explicitamente.
"""
from __future__ import annotations

MAKER_REBATE_BPS = 0.0      # ver aviso acima; so ligar com confirmacao na doc


def taker_fee(shares: float, price: float) -> float:
    """Fee de taker em $. `price` em unidades de preco (0..1)."""
    if price <= 0.0 or price >= 1.0:
        return 0.0
    return shares * price * 0.25 * (price * (1.0 - price)) ** 2


def maker_rebate(shares: float, price: float, bps: float = MAKER_REBATE_BPS) -> float:
    """Rebate de maker em $. Zero por omissao -- ver o aviso no topo."""
    return shares * price * bps / 10000.0


def rebate_per_share(price_c: float, bps: float = MAKER_REBATE_BPS) -> float:
    """Rebate por share ($/share). `price_c` em centimos."""
    return (price_c / 100.0) * bps / 10000.0


def net_fill_cost_per_share(price_c: float, adverse_c: float,
                            bps: float = MAKER_REBATE_BPS) -> float:
    """Custo liquido de um fill de maker por share ($/share): movimento adverso
    menos o rebate. Com bps=0 e o adverso puro, que e o default deliberado."""
    return adverse_c / 100.0 - rebate_per_share(price_c, bps)


def flatten_cost(shares: float, price_c: float) -> float:
    """Custo de escoar `shares` a mercado (cruza o spread, paga taker). E o unico
    sitio do farming onde ha taker fee."""
    return taker_fee(shares, price_c / 100.0)
