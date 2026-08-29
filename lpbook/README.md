# LP BOOK PRO

Versao corrigida e superior do `lp_book.py` do video. Farming do programa de
Liquidity Rewards do Polymarket em mercados extremos (um lado < 5c), com colocacao
em delta* preçado ao custo de fill (nao no mid as cegas), calibracao A/k, e seleçao
de mercado por liquido esperado com guardrail rho.

## Correr

    python lp_book_pro.py --mode paper --bankroll 20        # sintetico, roda offline
    python lp_book_pro.py --mode scan  --bankroll 20        # precisa de rede Polymarket
    python lp_book_pro.py --mode live  --bankroll 20 --live-confirm   # rede + CLOB
    python test_core.py                                     # testes das partes puras

Flags: `--rho-max 0.6`, `--pool-threshold 1.0`, `--hours 24`, `--speed 0.04`
(sleep por passo na TUI; 0 = tao rapido quanto possivel), `--dither-frac 0.30`
(amplitude do dithering de delta em fracao da band; 0 desliga e o k deixa de se
identificar), `--seed 0` (desloca as seeds do universo sintetico), `--state`,
`--control`.

## Modulos

- `scoring.py`     score quadratico, Q_min nas duas bandas, midpoint ajustado, share
- `fees.py`        taker fee real (paga no flatten); rebate de maker a 0 bps por omissao
- `toxicity.py`    LMSR + Bayesiano -> multiplicador do custo por fill (NAO validado, off)
- `shadow.py`      fill contra livro L2 com latencia, slippage e fills parciais
- `optimizer.py`   delta* (share + fator de permanencia na band), regimes MID/INTERIOR/BORDA, calibracao A/k, skew A-S normalizado
- `flow.py`        detetor de burst (Hawkes) -> alargar/retirar
- `selector.py`    otimizacao conjunta (size, delta), E[liquido diario], rho, ranking
- `fillmodel.py`   book e fills sinteticos (fills parciais, movimento adverso) para paper/replay
- `book_engine.py` motor por mercado: requote em delta*, skew, resposta a burst, credito por amostra
- `execution.py`   paper (fills do modelo) e live (CLOB post-only, atras de flag e control.json)
- `data_feed.py`   Gamma (metadados de reward) + CLOB (book) reais
- `state.py`       JSON atomico + kill-switch control.json
- `tui.py`         painel Rich (reward band, delta*, linha de selecao adversa sempre visivel, log)
- `lp_book_pro.py` main/CLI, modos scan/paper/replay/live

## Nota de honestidade

A $20 a maioria dos mercados nao farma (share pequena demais para o custo de fill).
So mercados de baixa competicao passam o filtro -- e a ferramenta rejeita o resto
em vez de fingir. Os modos scan/live precisam da rede do Polymarket (Gamma/CLOB).

## Limitacoes conhecidas (ler antes de por em live)

Detalhe e diagnostico em `../docs/PROMPT_LP_BOOK_PRO.md`, seccao 0.6.

1. ~~A calibracao A/k nao arranca sozinha.~~ **Corrigido** (bucket por perna + MLE de
   Poisson que usa os buckets vazios + dithering de delta). Erro absoluto medio do `k`
   em 5 seeds x 72h: 0.433 -> 0.207, ao custo de ~8.7% do liquido -- o preco da
   informacao. Detalhe em `../docs/PROMPT_LP_BOOK_PRO.md`, seccao 0.7. Fica em aberto:
   a amplitude do dithering (`--dither-frac`, default 0.30) **nao esta otimizada** --
   0.15 e 0.30 sao indistinguiveis com n=5 e querem um varrimento a serio.
2. **Sem WebSocket e sem asyncio.** `data_feed.py` e REST sincrono. Em `live`,
   `LiveExecutor.poll_fills()` devolve `[]`: sem o WS de user ligado, inventario,
   skew e calibracao nao recebem fills nenhuns.
3. **Sem contabilidade de order/cancel budget.** `LiveExecutor.place()` cancela e
   repoe a cada requote, sem contar contra o tier do signer.
4. **As formulas de scoring nao estao verificadas** contra a doc do Polymarket (rede
   bloqueada no ambiente de construcao). Os testes provam consistencia interna, nao
   conformidade. Reconfirmar antes de confiar em qualquer projecao.
5. **O sinal de toxicidade (`toxicity.py`) nao esta validado** e vem desligado
   (`--signal-gain 0`). No harness paper o mid e um martingale e o sinal e alimentado
   pelo proprio mid: nao pode ter edge por construcao. A/B em 3 seeds x 72h da pior
   com ele ligado. Precisa de feed do subjacente + backtest antes de valer algo.
6. **O rebate de maker esta a 0 bps** de proposito -- ver o aviso no topo de
   `fees.py`. Nao foi confirmado que o Polymarket pague rebate por fill.

## Porque NAO ha delta assimetrico por perna

Um sinal direcional sugere apertar a perna benigna e alargar a toxica. Isso e certo
para um market maker direcional, e errado aqui: na banda extrema o score e
`Q_min = min(Q_bid, Q_ask)`, logo a perna PIOR fixa a pontuacao e a apertada nao
conta. Com o gain do port original, um vies maximo retinha **1.6%** do reward (e
zero assim que o ask sai da band). A toxicidade prevista entra por isso como
multiplicador do custo por fill, e o `optimizer` re-resolve um delta* simetrico.
Registado em `test_merge.py::test_delta_assimetrico_destroi_qmin`.
