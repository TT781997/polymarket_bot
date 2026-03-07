<<<<<<< HEAD
# BOT XRP POLYMARKET v4.2.0

## Pré-requisitos de Instalação

```bash
sudo yum update -y
sudo dnf install python3.11 python3.11-pip -y
python3.11 -m pip install py-clob-client python-dotenv requests
python3.11 -m pip install websockets requests
python3.11 -m pip install py-clob-client
```

Criar `secrets.txt` na mesma pasta:
```
POLYMARKET_PRIVATE_KEY=a_tua_chave_privada_aqui
```

Correr: `python3.11 bot_xrp_v4_1.py`
Logs: `bot_xrp.log` (terminal silencioso).

---

## 1. Visão Geral

O bot opera em mercados binários XRP UP/DOWN de 5 minutos na Polymarket. A cada 5 minutos nasce um novo mercado com dois tokens: UP e DOWN. No final, um resolve a $1.00/share e o outro a $0.00.

A arquitectura segue um ciclo contínuo de 3 componentes:

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  1. MARKET       │     │  2. TRADING      │     │  3. UPDATE       │
│  PRICING (LMSR)  │────►│  DECISION (EV)   │◄────│  BELIEFS         │
│                  │     │                  │     │  (BAYESIAN)      │
│  pi = softmax    │     │  EV = p̂ - p      │     │  log P(H|D) =    │
│  → market prob p │     │  if p̂ > p → BUY  │     │  log P(H) + ...  │
└──────────────────┘     └──────────────────┘     └──────────────────┘
         ▲                                                │
         └────────────────────────────────────────────────┘
                    REAL EDGE: EXECUTION SPEED (~1ms cycle)
```

O ciclo de referência nos documentos de pesquisa é ~828ms. O nosso é ~1ms porque: zero REST no hot path, tudo via WebSocket, Bayesian e LMSR calculados localmente em microsegundos.

---

## 2. Fluxo de Dados

```
Polymarket WebSocket → ws_handler() → best_bids/asks/sizes
                                          │
                                   price_change.set()
                                          │
                                          ▼
                                    logic_loop() ~1ms
                                          │
                    ┌─────────────────────┼──────────────────────┐
                    ▼                     ▼                      ▼
              Kalman Filter        BayesianTracker          LMSRPricer
              (mid smooth)         (P(UP|data))            (fair prices)
                    │                     │                      │
                    ▼                     ▼                      ▼
              HFT Window          p̂ = posterior           inefficiency
              Z-Score, σ          EV = p̂ - ask           = p̂ - market
              VPIN, OBI                                        │
                    │                     │                      │
                    └─────────────────────┼──────────────────────┘
                                          ▼
                                    10 Gate Checks
                                          │
                                    Kelly Sizing (1/8)
                                    × Martingale
                                          │
                                     BUY ao ASK
```

---

## 3. Kalman Filter — Suavização do Preço

Filtra ruído do mid_price (média de BID e ASK) para obter o preço real estimado.

```
Predição:     x_pred = x_{k-1}
              P_pred = P_{k-1} + Q
Actualização: K = P_pred / (P_pred + R)
              x_k = x_pred + K × (z_k - x_pred)
```

Q=8e-6 (process noise), R=4e-3 (measurement noise). Se o mid salta de 75c→80c num tick (wick), o Kalman ajusta apenas para ~76c.

---

## 4. HFT Window — Z-Score e Regime

Janela de 10 segundos sobre preços Kalman.

**Z-Score:** `Z = (preço - média) / desvio_padrão`
- Z > 1.3 → pico anormal → NÃO comprar
- Z < -5.0 → crash → STOP LOSS

**Regime σ:** `σ = sqrt(Σ(p-µ)²/n)` sobre 10s
- σ <= 0.03 → estável → OK
- σ > 0.03 → volátil → ESPERAR

---

## 5. VPIN — Toxicidade do Fluxo

```
VPIN = |buy_vol - sell_vol| / (buy_vol + sell_vol)
```
- <= 0.55 → fluxo saudável → OK
- \> 0.55 → desequilíbrio → BLOQUEAR
- \> 0.97 → dump institucional → STOP LOSS

---

## 6. OBI — Orderbook Imbalance

```
OBI = BID_size / (BID_size + ASK_size)
```
- \>= 0.20 → suporte mínimo aceitável
- <= 0.02 → compradores abandonaram → STOP LOSS

---

## 7. Bayesian Sequential Updating

Mantém log-posterior actualizado a cada tick WS com 3 sinais de likelihood:

**Signal 1 — Direcção Kalman:** se UP sobe mais que DOWN → evidência para UP.
**Signal 2 — OBI Dominância:** compradores dominam UP → evidência para UP.
**Signal 3 — VPIN Saúde:** fluxo limpo em UP → evidência para UP.

```
log P(H|D) = log P(H) + Σ log P(Dk|H) - log Z
```

Normalizado via log-sum-exp. Decay de 0.5%/tick previne overfit. Resultado: `p̂_up`, `p̂_down`.

---

## 8. LMSR — Preço Justo via Softmax

```
pi(q) = e^(qi/b) / Σ e^(qj/b)
```

Preços somam a 1.0 exactamente. Inefficiency = `p̂ - market_ask`. Positivo = mercado subvaloriza = COMPRAR.

---

## 9. EV — Expected Value

```
EV = p̂ - p
```
- `p̂` = posterior Bayesiano
- `p` = preço ASK (custo real de entrada)
- EV > 0 → edge positivo → considerar trade

---

## 10. Kelly (1/8) + Martingale

```
kelly = p̂ - (1-p̂)/odds
risk = kelly × 1/8 × martingale_multiplier
```

Martingale: Loss→x2 (x1→x2→x4→x8→reset). Win→x1. Novo dia→x1.

---

## 11. Pipeline de Entrada — 10 Gates

| # | Gate | Condição | Valor |
|---|------|----------|-------|
| 0 | Gambling Window | rem <= 300s | 300s |
| 1 | Cooldown | time >= 15s since last | 15s |
| 2 | Spread | spread <= 2.20c | 2.20c |
| 3 | ASK Range | 75c <= ask×100 <= 95c | raw ASK |
| 4 | BID/ASK Ratio | bid/ask >= 0.94 | 0.94 |
| 5 | Regime σ | σ <= 0.03 | 0.03 |
| 6 | Z-Score | Z <= 1.3 (endgame: 99) | 1.3 |
| 7 | OBI | OBI >= 0.20 | 0.20 |
| 8 | VPIN | VPIN <= 0.55 (endgame: 0.70) | 0.55 |
| 9 | Bayesian Edge | p̂-ask >= 0.04 | 4c |
| 10 | LMSR Ineff | ineff >= 0.02 | 2c |

Todos PASS → Kelly sizing → BUY ao ASK.

---

## 12. Exemplo: Trade que ENTRA

```
rem=60s  UP ASK=84c BID=82c  σ=0.01 Z=+0.5 OBI=0.45 VPIN=0.20
         BAYES P(UP)=0.92  LMSR ineff=+0.08

Todos os gates: PASS
Kelly: (0.92 - 0.08/0.19) × 1/8 = ~5%
→ BUY UP @ 84c, invest = banca × 5%
```

## 13. Exemplo: Trade que NÃO ENTRA

```
rem=120s  UP ASK=84c  σ=0.05 Z=+2.0 OBI=0.15 VPIN=0.60
          BAYES P(UP)=0.55

Gate 5: σ=0.05 > 0.03         → BLOCKED (volátil)
Gate 6: Z=+2.0 > 1.3          → BLOCKED (pico)
Gate 8: VPIN=0.60 > 0.55      → BLOCKED (tóxico)
Gate 9: edge=0.55-0.84=-0.29  → BLOCKED (EV negativo)
→ NÃO comprar
```

---

## 14. Compra nos Dois Lados

O bot pode comprar UP e DOWN no mesmo ciclo. Se o mercado vira, comprar o lado correcto perto do final reduz perdas:

```
t=4:00  Compra UP @ 76c (Bayesian P(UP)=0.88)
t=1:30  Mercado vira, DOWN sobe
t=1:20  Compra DOWN @ 85c (Bayesian P(DOWN)=0.82)
t=0:00  DOWN ganha:
  UP:   -$investido (perda total)
  DOWN: shares×$1 - investido (ganho)
  Net:  perda parcial em vez de total
```

---

## 15. Peg Arbit — Arbitragem Risk-Free via Order Book

A arbitragem Peg Arbit é a operação mais segura do bot. Compra SHARES IGUAIS dos dois lados (UP e DOWN) quando a soma dos ASKs está abaixo de $1.00. Como um dos lados resolve sempre a $1.00/share, o lucro é garantido.

**Lógica de cálculo (Order Book Execution):**

```
1. Identifica Lowest Ask UP  (preço mais baixo que vendedores aceitam para UP)
2. Identifica Lowest Ask DOWN (preço mais baixo que vendedores aceitam para DOWN)
3. Calcula o Peg: Peg = Lowest_Ask_UP + Lowest_Ask_DOWN
```

**Condição de entrada (Trigger):**
```
Peg < 0.98  (PA_TRIGGER_SUM)
```

A margem de 0.02 (1.00 - 0.98) absorve fees nos dois lados, slippage, e variações de liquidez no topo do order book.

**Shares iguais nos dois lados:**
```python
cost_per_share = ask_up + ask_down + fee(ask_up) + fee(ask_down)
shares = budget / cost_per_share    # IGUAL para ambos os lados
```

**Profitability gate:** antes de entrar, verifica que o lucro líquido (após fees) é positivo. Se as fees comem a margem, rejeita com `REJECT_FEES`.

**Exemplo real:**
```
Ask UP = 46c, Ask DOWN = 50c
Peg = 0.96 (< 0.98 → TRIGGER!)

Compra 2.56 shares de UP  @ 46c = $1.18 + fee $0.018
Compra 2.56 shares de DOWN @ 50c = $1.28 + fee $0.020

Custo total = $2.50
Se UP ganha: payout = 2.56 × $1.00 = $2.56
Lucro = $2.56 - $2.50 = $0.064 (+2.57%)

Se DOWN ganha: payout = 2.56 × $1.00 = $2.56
Lucro = $2.56 - $2.50 = $0.064 (+2.57%)

→ LUCRO GARANTIDO independentemente do resultado!
```

**Quando NÃO entra:**
```
Ask UP = 46c, Ask DOWN = 55c
Peg = 1.01 (> 0.98 → BLOCKED)
Custo > payout → perda garantida → NÃO comprar
```

---

## 16. Settlement Local

No final (rem=0): `ASK_UP > ASK_DOWN → UP ganha`. Tokens vencedores = $1.00 (fee=0). Perdedores = $0.00.

---

## 17. Net PnL (Pos/Neg)

Vasos comunicantes: ganhos recuperam Neg antes de aumentar Pos. Perdas reduzem Pos antes de aumentar Neg. `Pos + Neg` = net P&L real.

---

## 18. Order Book

| Lado | Descrição |
|------|-----------|
| Bids | Preço mais alto que traders querem pagar (BUY orders) |
| Asks | Preço mais baixo que traders querem aceitar (SELL orders) |

O bot compra SEMPRE ao ASK (lowest ask). Filtro de preço usa `ask × 100` cents raw, não eff_price com fees.
=======
# BOT XRP POLYMARKET v4.2.0

## Pré-requisitos de Instalação

```bash
sudo yum update -y
sudo dnf install python3.11 python3.11-pip -y
python3.11 -m pip install py-clob-client python-dotenv requests
python3.11 -m pip install websockets requests
python3.11 -m pip install py-clob-client
```

Criar `secrets.txt` na mesma pasta:
```
POLYMARKET_PRIVATE_KEY=a_tua_chave_privada_aqui
```

Correr: `python3.11 bot_xrp_v4_1.py`
Logs: `bot_xrp.log` (terminal silencioso).

---

## 1. Visão Geral

O bot opera em mercados binários XRP UP/DOWN de 5 minutos na Polymarket. A cada 5 minutos nasce um novo mercado com dois tokens: UP e DOWN. No final, um resolve a $1.00/share e o outro a $0.00.

A arquitectura segue um ciclo contínuo de 3 componentes:

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│  1. MARKET       │     │  2. TRADING      │     │  3. UPDATE       │
│  PRICING (LMSR)  │────►│  DECISION (EV)   │◄────│  BELIEFS         │
│                  │     │                  │     │  (BAYESIAN)      │
│  pi = softmax    │     │  EV = p̂ - p      │     │  log P(H|D) =    │
│  → market prob p │     │  if p̂ > p → BUY  │     │  log P(H) + ...  │
└──────────────────┘     └──────────────────┘     └──────────────────┘
         ▲                                                │
         └────────────────────────────────────────────────┘
                    REAL EDGE: EXECUTION SPEED (~1ms cycle)
```

O ciclo de referência nos documentos de pesquisa é ~828ms. O nosso é ~1ms porque: zero REST no hot path, tudo via WebSocket, Bayesian e LMSR calculados localmente em microsegundos.

---

## 2. Fluxo de Dados

```
Polymarket WebSocket → ws_handler() → best_bids/asks/sizes
                                          │
                                   price_change.set()
                                          │
                                          ▼
                                    logic_loop() ~1ms
                                          │
                    ┌─────────────────────┼──────────────────────┐
                    ▼                     ▼                      ▼
              Kalman Filter        BayesianTracker          LMSRPricer
              (mid smooth)         (P(UP|data))            (fair prices)
                    │                     │                      │
                    ▼                     ▼                      ▼
              HFT Window          p̂ = posterior           inefficiency
              Z-Score, σ          EV = p̂ - ask           = p̂ - market
              VPIN, OBI                                        │
                    │                     │                      │
                    └─────────────────────┼──────────────────────┘
                                          ▼
                                    10 Gate Checks
                                          │
                                    Kelly Sizing (1/8)
                                    × Martingale
                                          │
                                     BUY ao ASK
```

---

## 3. Kalman Filter — Suavização do Preço

Filtra ruído do mid_price (média de BID e ASK) para obter o preço real estimado.

```
Predição:     x_pred = x_{k-1}
              P_pred = P_{k-1} + Q
Actualização: K = P_pred / (P_pred + R)
              x_k = x_pred + K × (z_k - x_pred)
```

Q=8e-6 (process noise), R=4e-3 (measurement noise). Se o mid salta de 75c→80c num tick (wick), o Kalman ajusta apenas para ~76c.

---

## 4. HFT Window — Z-Score e Regime

Janela de 10 segundos sobre preços Kalman.

**Z-Score:** `Z = (preço - média) / desvio_padrão`
- Z > 1.3 → pico anormal → NÃO comprar
- Z < -5.0 → crash → STOP LOSS

**Regime σ:** `σ = sqrt(Σ(p-µ)²/n)` sobre 10s
- σ <= 0.03 → estável → OK
- σ > 0.03 → volátil → ESPERAR

---

## 5. VPIN — Toxicidade do Fluxo

```
VPIN = |buy_vol - sell_vol| / (buy_vol + sell_vol)
```
- <= 0.55 → fluxo saudável → OK
- \> 0.55 → desequilíbrio → BLOQUEAR
- \> 0.97 → dump institucional → STOP LOSS

---

## 6. OBI — Orderbook Imbalance

```
OBI = BID_size / (BID_size + ASK_size)
```
- \>= 0.20 → suporte mínimo aceitável
- <= 0.02 → compradores abandonaram → STOP LOSS

---

## 7. Bayesian Sequential Updating

Mantém log-posterior actualizado a cada tick WS com 3 sinais de likelihood:

**Signal 1 — Direcção Kalman:** se UP sobe mais que DOWN → evidência para UP.
**Signal 2 — OBI Dominância:** compradores dominam UP → evidência para UP.
**Signal 3 — VPIN Saúde:** fluxo limpo em UP → evidência para UP.

```
log P(H|D) = log P(H) + Σ log P(Dk|H) - log Z
```

Normalizado via log-sum-exp. Decay de 0.5%/tick previne overfit. Resultado: `p̂_up`, `p̂_down`.

---

## 8. LMSR — Preço Justo via Softmax

```
pi(q) = e^(qi/b) / Σ e^(qj/b)
```

Preços somam a 1.0 exactamente. Inefficiency = `p̂ - market_ask`. Positivo = mercado subvaloriza = COMPRAR.

---

## 9. EV — Expected Value

```
EV = p̂ - p
```
- `p̂` = posterior Bayesiano
- `p` = preço ASK (custo real de entrada)
- EV > 0 → edge positivo → considerar trade

---

## 10. Kelly (1/8) + Martingale

```
kelly = p̂ - (1-p̂)/odds
risk = kelly × 1/8 × martingale_multiplier
```

Martingale: Loss→x2 (x1→x2→x4→x8→reset). Win→x1. Novo dia→x1.

---

## 11. Pipeline de Entrada — 10 Gates

| # | Gate | Condição | Valor |
|---|------|----------|-------|
| 0 | Gambling Window | rem <= 300s | 300s |
| 1 | Cooldown | time >= 15s since last | 15s |
| 2 | Spread | spread <= 2.20c | 2.20c |
| 3 | ASK Range | 75c <= ask×100 <= 95c | raw ASK |
| 4 | BID/ASK Ratio | bid/ask >= 0.94 | 0.94 |
| 5 | Regime σ | σ <= 0.03 | 0.03 |
| 6 | Z-Score | Z <= 1.3 (endgame: 99) | 1.3 |
| 7 | OBI | OBI >= 0.20 | 0.20 |
| 8 | VPIN | VPIN <= 0.55 (endgame: 0.70) | 0.55 |
| 9 | Bayesian Edge | p̂-ask >= 0.04 | 4c |
| 10 | LMSR Ineff | ineff >= 0.02 | 2c |

Todos PASS → Kelly sizing → BUY ao ASK.

---

## 12. Exemplo: Trade que ENTRA

```
rem=60s  UP ASK=84c BID=82c  σ=0.01 Z=+0.5 OBI=0.45 VPIN=0.20
         BAYES P(UP)=0.92  LMSR ineff=+0.08

Todos os gates: PASS
Kelly: (0.92 - 0.08/0.19) × 1/8 = ~5%
→ BUY UP @ 84c, invest = banca × 5%
```

## 13. Exemplo: Trade que NÃO ENTRA

```
rem=120s  UP ASK=84c  σ=0.05 Z=+2.0 OBI=0.15 VPIN=0.60
          BAYES P(UP)=0.55

Gate 5: σ=0.05 > 0.03         → BLOCKED (volátil)
Gate 6: Z=+2.0 > 1.3          → BLOCKED (pico)
Gate 8: VPIN=0.60 > 0.55      → BLOCKED (tóxico)
Gate 9: edge=0.55-0.84=-0.29  → BLOCKED (EV negativo)
→ NÃO comprar
```

---

## 14. Compra nos Dois Lados

O bot pode comprar UP e DOWN no mesmo ciclo. Se o mercado vira, comprar o lado correcto perto do final reduz perdas:

```
t=4:00  Compra UP @ 76c (Bayesian P(UP)=0.88)
t=1:30  Mercado vira, DOWN sobe
t=1:20  Compra DOWN @ 85c (Bayesian P(DOWN)=0.82)
t=0:00  DOWN ganha:
  UP:   -$investido (perda total)
  DOWN: shares×$1 - investido (ganho)
  Net:  perda parcial em vez de total
```

---

## 15. Peg Arbit — Arbitragem Risk-Free via Order Book

A arbitragem Peg Arbit é a operação mais segura do bot. Compra SHARES IGUAIS dos dois lados (UP e DOWN) quando a soma dos ASKs está abaixo de $1.00. Como um dos lados resolve sempre a $1.00/share, o lucro é garantido.

**Lógica de cálculo (Order Book Execution):**

```
1. Identifica Lowest Ask UP  (preço mais baixo que vendedores aceitam para UP)
2. Identifica Lowest Ask DOWN (preço mais baixo que vendedores aceitam para DOWN)
3. Calcula o Peg: Peg = Lowest_Ask_UP + Lowest_Ask_DOWN
```

**Condição de entrada (Trigger):**
```
Peg < 0.98  (PA_TRIGGER_SUM)
```

A margem de 0.02 (1.00 - 0.98) absorve fees nos dois lados, slippage, e variações de liquidez no topo do order book.

**Shares iguais nos dois lados:**
```python
cost_per_share = ask_up + ask_down + fee(ask_up) + fee(ask_down)
shares = budget / cost_per_share    # IGUAL para ambos os lados
```

**Profitability gate:** antes de entrar, verifica que o lucro líquido (após fees) é positivo. Se as fees comem a margem, rejeita com `REJECT_FEES`.

**Exemplo real:**
```
Ask UP = 46c, Ask DOWN = 50c
Peg = 0.96 (< 0.98 → TRIGGER!)

Compra 2.56 shares de UP  @ 46c = $1.18 + fee $0.018
Compra 2.56 shares de DOWN @ 50c = $1.28 + fee $0.020

Custo total = $2.50
Se UP ganha: payout = 2.56 × $1.00 = $2.56
Lucro = $2.56 - $2.50 = $0.064 (+2.57%)

Se DOWN ganha: payout = 2.56 × $1.00 = $2.56
Lucro = $2.56 - $2.50 = $0.064 (+2.57%)

→ LUCRO GARANTIDO independentemente do resultado!
```

**Quando NÃO entra:**
```
Ask UP = 46c, Ask DOWN = 55c
Peg = 1.01 (> 0.98 → BLOCKED)
Custo > payout → perda garantida → NÃO comprar
```

---

## 16. Settlement Local

No final (rem=0): `ASK_UP > ASK_DOWN → UP ganha`. Tokens vencedores = $1.00 (fee=0). Perdedores = $0.00.

---

## 17. Net PnL (Pos/Neg)

Vasos comunicantes: ganhos recuperam Neg antes de aumentar Pos. Perdas reduzem Pos antes de aumentar Neg. `Pos + Neg` = net P&L real.

---

## 18. Order Book

| Lado | Descrição |
|------|-----------|
| Bids | Preço mais alto que traders querem pagar (BUY orders) |
| Asks | Preço mais baixo que traders querem aceitar (SELL orders) |

O bot compra SEMPRE ao ASK (lowest ask). Filtro de preço usa `ask × 100` cents raw, não eff_price com fees.
>>>>>>> a96139d (first commit)
