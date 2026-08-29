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
(sleep por passo na TUI; 0 = tao rapido quanto possivel), `--state`, `--control`.

## Modulos

- `scoring.py`     score quadratico, Q_min nas duas bandas, midpoint ajustado, share
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

1. **A calibracao A/k nao arranca sozinha.** O bucket e indexado pelo delta corrente
   (`book_engine.py:56,81`); como a politica converge para um delta, todos os fills
   caem num bucket, a regressao fica sem variacao em x e devolve `None` -- a
   ferramenta corre nos priores `A0/K0` para sempre. Correcoes: indexar pela
   distancia real de cada perna ao mid (as duas pernas ja divergem com o skew) e
   fazer dithering deliberado do delta. Como e o `k` que decide o regime, correr com
   `k` assumido e o mesmo erro do video, so que documentado.
2. **Sem WebSocket e sem asyncio.** `data_feed.py` e REST sincrono. Em `live`,
   `LiveExecutor.poll_fills()` devolve `[]`: sem o WS de user ligado, inventario,
   skew e calibracao nao recebem fills nenhuns.
3. **Sem contabilidade de order/cancel budget.** `LiveExecutor.place()` cancela e
   repoe a cada requote, sem contar contra o tier do signer.
4. **As formulas de scoring nao estao verificadas** contra a doc do Polymarket (rede
   bloqueada no ambiente de construcao). Os testes provam consistencia interna, nao
   conformidade. Reconfirmar antes de confiar em qualquer projecao.
