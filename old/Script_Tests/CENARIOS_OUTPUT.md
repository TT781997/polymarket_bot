# BOT XRP v0.36.1 — ANÁLISE COMPLETA DE OUTPUTS POR CENÁRIO

## 📋 CONFIGURAÇÃO ATUAL
```
LIVE_TRADING       = False (Simulação)
BANKROLL_INIT      = $10.00
EIGHTY_ACTIVE      = True
PEG_ARBIT_ACTIVE   = True
CICLO_30S_ACTIVE   = False
CICLO_20S_ACTIVE   = False
KELLY_ACTIVE       = False
AS_VPIN_ACTIVE     = False
```

---

## 🟢 CENÁRIO 1: STARTUP DO BOT

### OUTPUT ESPERADO:

```
================================================================================
[dd/mm/yy | HH:MM:SS.mmm] | BOT XRP POLYMARKET v0.36.1 INICIADO
================================================================================
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | LIVE_TRADING     : False
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | BANKROLL_INIT    : $10.00

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | RISCO BASE:
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    RISK_PER_TRADE   : 5%
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    EIGHTY_RISK      : 15%
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    PEG_ARBIT_RISK   : 25%

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | MARTINGALE:
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    MAX_RISK_PERCENT : 50% (CAP INVIOLAVEL)
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    MAX_MULTIPLIER   : x16
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    Formula          : min(base x mult + (acc_loss / rounds / bank), MAX_RISK)

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | MODULOS:
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    EIGHTY           : ON
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    PEG_ARBIT        : ON
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    CICLO_30S        : OFF
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    CICLO_20S        : OFF
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    KELLY            : OFF
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    AS+VPIN          : OFF
================================================================================
```

---

## 🟡 CENÁRIO 2: NOVO DIA (Reset Diário)

### OUTPUT ESPERADO:

```
================================================================================
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | NOVO DIA 2026-03-03
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    Banca            : $10.0000
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    Martingale reset : x1 | acc_loss=$0.00 | rounds=1
================================================================================
```

---

## 🔵 CENÁRIO 3: WEBSOCKET CONECTADO + ORDEM BOOK RECEBIDO

### OUTPUT ESPERADO:

```
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | WS conectado ao order book Polymarket (ASK/BID Tracking)
[dd/mm/yy | HH:MM:SS.mmm] | rem=04:59:999 | UP=52.3c Eff=52.9c | DOWN=47.7c Eff=47.1c | PEG_Eff=1.000 underpeg=0.00c
[dd/mm/yy | HH:MM:SS.mmm] | rem=04:59:995 | UP=52.1c Eff=52.7c | DOWN=47.9c Eff=48.3c | PEG_Eff=1.010 underpeg=-1.00c
```

---

## 🟢 CENÁRIO 4: PEG ARBITRAGE — ENTRADA BEM-SUCEDIDA

### CONDIÇÕES NECESSÁRIAS:
- `PEG_ARBIT_ACTIVE = True`
- `PEG_Eff < 1.0` (underpeg ≥ 0.8c)
- Ambos UP/DOWN no range [45.0c - 55.0c]
- `rem > 5.0s`

### OUTPUT ESPERADO:

```
================================================================================
[INFO] [PEG ARBIT] [ENTRADA] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:30:500 | 
PEG_Eff=0.9950 (-0.50c) | Shares=0.1234 | Total=$12.45 | 
Lucro est.=$0.0625 (0.50%) | arb #1

[INFO] [PEG ARBIT] [BUY] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:30:500 | 
UP @ nom=50.5c ask=50.51c eff=51.0c | PEG_Eff: 99.5c (0.995) | 
inv=$6.23 (50.0% banca) | shares=0.1234 | fee=0.125%

[INFO] [PEG ARBIT] [BUY] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:30:500 | 
DOWN @ nom=49.5c ask=49.51c eff=50.0c | PEG_Eff: 99.5c (0.995) | 
inv=$6.22 (49.9% banca) | shares=0.1234 | fee=0.125%
================================================================================
```

---

## 🔴 CENÁRIO 5: PEG ARBITRAGE — ENTRADA BLOQUEADA

### RAZÕES POSSÍVEIS:
1. **Fora do range:**
```
[INFO] [PEG ARBIT] [SKIP] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:30:000 | 
PEG_Eff OK (0.9950) mas UP_Eff 42.1c fora [45.0-55.0] | DOWN_Eff 55.3c fora [45.0-55.0]
```

2. **PEG não underpeg o suficiente:**
```
[dd/mm/yy | HH:MM:SS.mmm] | rem=04:30:000 | UP=50.0c Eff=50.5c | 
DOWN=50.0c Eff=50.5c (sem peg_disp pois underpeg < 0.8c)
```

---

## 🟦 CENÁRIO 6: EIGHTY — START E MONITORIZAÇÃO

### CONDIÇÕES:
- `EIGHTY_ACTIVE = True`
- `rem > EIGHTY_CUTOFF_S (5s)` E `rem <= EIGHTY_START_REM_S (300s)`

### OUTPUT ESPERADO:

```
[INFO] [EIGHTY] [START] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:59:000 | 
EIGHTY activo [300s->5s] | risco=15.0%

[INFO] [EIGHTY] [WATCH] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:58:500 | 
UP Eff=82.5c | VOL OK (2.1c/5.2s) | D1.0s:+0.5c D2.0s:+1.2c D3.0s:+1.8c (UP) | 
PEG_Eff=1.000 | ticks=3/5

[INFO] [EIGHTY] [WATCH] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:58:490 | 
DOWN Eff=17.5c | VOL OK (1.9c/5.1s) | D aguarda (2.3s) (WAIT) | ticks=2/5
```

---

## 🟩 CENÁRIO 7: EIGHTY — ENTRADA (BUY)

### CONDIÇÕES ALL TRUE:
- ✅ Passou VOL check `(var <= 4.5c em 5s)`
- ✅ Sem PUMP rápido `(delta 1.5s < 2.0c)`
- ✅ `ticks >= 5` (mínimo níveis únicos)
- ✅ `PEG_Eff >= 97.0c`
- ✅ Sem exaustão `(delta 3s < 3.5c)`
- ✅ Passou `EIGHTY_MIN_EFF_C <= eff <= EIGHTY_MAX_EFF_C`

### OUTPUT ESPERADO:

```
[INFO] [EIGHTY] [BUY] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:30:200 | 
UP @ nom=82.5c ask=82.51c eff=83.1c | PEG_Eff: 97.5c (0.975) | 
inv=$1.50 (15.0% banca) | shares=0.0180 | fee=0.145% | 
ticks=5 | D1.0s:+0.2c D2.0s:+0.8c D3.0s:+1.1c

[INFO] [EIGHTY] [COOLDOWN] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:30:195 | 
UP - cooldown 4.0s
```

---

## 🟨 CENÁRIO 8: EIGHTY — BLOQUEIOS

### 8A — VOL MUITO ALTA:
```
[INFO] [EIGHTY] [VOL_COOLDOWN] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:55:000 | 
UP - bloqueado 5.0s

[INFO] [EIGHTY] [WATCH] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:55:000 | 
UP Eff=82.5c | VOL NOK (7.2c/2.3s) | D aguarda (0.5s) (WAIT) | ticks=1/5
```

### 8B — PUMP RÁPIDO DETECTADO:
```
[INFO] [EIGHTY] [VOL_COOLDOWN] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:52:000 | 
UP - bloqueado 5.0s

[INFO] [EIGHTY] [WATCH] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:52:000 | 
UP Eff=85.2c | VOL SKIP | D1.5s:+3.2c (pump rapido) (DOWN) | ticks=3/5
```

### 8C — EXAUSTÃO TENDENCIAL:
```
[INFO] [EIGHTY] [RESET] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:40:000 | 
UP - DELTA NOK - D3s=+4.1c (exaustao tendencial)
```

### 8D — PEG NÃO EQUILIBRADO:
```
[INFO] [EIGHTY] [RESET] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:35:000 | 
UP - PEG_Eff 94.2c < min 97.0c
```

### 8E — CUTOFF ATINGIDO (Parada Final):
```
[INFO] [EIGHTY] [CUTOFF] [dd/mm/yy | HH:MM:SS.mmm] | rem=00:04:500 | 
EIGHTY parado - rem <= 5s
```

---

## 🟧 CENÁRIO 9: TARGET ATINGIDO (EXIT COM GANHO)

### CENÁRIO:
- Trade comprado em UP @ 82.5c
- `EIGHTY_TARGET_C = 0.0` (natural market close OU bid >= 99.0c)

### OUTPUT ESPERADO:

```
[INFO] [EIGHTY] [SELL] [dd/mm/yy | HH:MM:SS.mmm] | rem=00:15:000 | 
UP @ 99.0c | PnL: $+0.0250 (+1.67%) (+) | Reason: TARGET
```

---

## 🟤 CENÁRIO 10: STOP-LOSS FLASH-CRASH

### CONDIÇÕES:
- BID efetivo cai abaixo de 27.0c
- Deteta 5+ níveis estruturais de descida em steps de 1.0c

### OUTPUT ESPERADO:

```
[INFO] [STOPLOSS] [MONITOR] [dd/mm/yy | HH:MM:SS.mmm] | rem=00:10:000 | 
UP iniciado @ 25.3c < 27.0c

[INFO] [STOPLOSS] [MONITOR] [dd/mm/yy | HH:MM:SS.mmm] | rem=00:09:995 | 
UP desceu para 24.2c - nível 24.0c novo

[INFO] [STOPLOSS] [MONITOR] [dd/mm/yy | HH:MM:SS.mmm] | rem=00:09:990 | 
UP desceu para 23.1c - nível 23.0c novo

[INFO] [STOPLOSS] [SELL] [dd/mm/yy | HH:MM:SS.mmm] | rem=00:09:985 | 
UP @ 20.5c | PnL: $-0.0340 (-2.27%) (-) | Reason: STOP-LOSS FLASH-CRASH
```

---

## ⚫ CENÁRIO 11: FIM DE MERCADO (SETTLEMENT)

### TIMELINE:
- Market encerra no tempo `m_end` (start_ts + 300s)
- Sistema avalia BID final de ambos
- Winner = side com BID superior

### OUTPUT ESPERADO:

```
================================================================================
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | FIM DE MERCADO | UP final=52.3c | DOWN final=47.7c

[INFO] [EIGHTY] [SELL] [dd/mm/yy | HH:MM:SS.mmm] | rem=00:00:000 | 
UP @ 100.0c | PnL: $+0.1500 (+10.00%) (+) | Reason: RESOLUCAO GANHA ($1/share)

[INFO] [EIGHTY] [SELL] [dd/mm/yy | HH:MM:SS.mmm] | rem=00:00:000 | 
DOWN @ 0.0c | PnL: $-0.0500 (-100.00%) (-) | Reason: RESOLUCAO PERDIDA (Total)
```

---

## 🟦 CENÁRIO 12: RONDA COM PROFIT (RECOVERY PARCIAL)

### PANO DE FUNDO:
- Martingale active (accumulated_loss > 0)
- Esta ronda: **+$0.15 de profit**
- Antes: accumulated_loss = $0.40, rounds = 11

### OUTPUT ESPERADO:

```
================================================================================
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | MARTINGALE | RECOVERY parcial 
(recuperados $0.15 | restam $0.25) | Rounds restantes: 10

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | ROUND | PnL: $+0.1500 (+1.50%)

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | TOTAL | PnL: $+0.8500 (+8.50%) | 
Banca: $10.8500 | Accumulated loss: $0.2500 | Uptime: 0y:00m:01d:02h:15m:30s
================================================================================
```

---

## 🔴 CENÁRIO 13: RONDA COM LOSS (MARTINGALE ATIVADO)

### PANO DE FUNDO:
- Martingale NOT active antes
- Esta ronda: **-$0.30 de loss**

### OUTPUT ESPERADO:

```
================================================================================
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | MARTINGALE UPDATE | ROUND PnL < 0 | 
Proximo Mult: x2 | Acc_loss: $0.1500 | Recovery Rounds: 11 | 
Proximo Risco EIGHTY=30.0% PEG=50.0% [CAP]

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | ROUND | PnL: $-0.3000 (-3.00%)

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | TOTAL | PnL: $-0.3000 (-3.00%) | 
Banca: $9.7000 | Accumulated loss: $0.1500 | Uptime: 0y:00m:01d:02h:15m:30s
================================================================================
```

---

## 🟠 CENÁRIO 14: MARTINGALE ESCALADO (x4 RISCO)

### PANO DE FUNDO:
- Múltiplas rodas com loss aumentando o multiplicador
- Atual: multiplier = 4x, accumulated_loss = $0.80

### OUTPUT ESPERADO (NO INÍCIO DA RONDA):

```
================================================================================
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | NOVO CICLO | Market: xrp-updown-5m-1709521200 | LIVE: False

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | Banca: $9.2000 | Profit acum.: $-0.8000 [MARTINGALE x4]

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | Modulos: EIGHTY(300s->5s) | PEG_ARBIT(range 45-55c)

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | MARTINGALE | x4 | accum_loss=$0.8000 | rec_rounds=21 | 
recovery_bonus=$0.8000/21/$9.2000=0.4% | eff_risk: EIGHTY=60.0% [CAP] PEG=50.0% [CAP] (cap=50%)

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | Risco efetivo: EIGHTY=50.0% [CAP] | 
PEG=50.0% [CAP] | CICLOS=5.0% | CAP=50%
```

---

## ⚪ CENÁRIO 15: RONDA SEM TRADES (0 PnL)

### SITUAÇÃO:
- Nenhum módulo acionou entrada
- Ou entradas bloqueadas por condições

### OUTPUT ESPERADO:

```
================================================================================
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | NOVO CICLO | Market: xrp-updown-5m-1709521200 | LIVE: False

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | Banca: $10.0000 | Profit acum.: $0.0000

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | Modulos: EIGHTY(300s->5s) | PEG_ARBIT(range 45-55c)

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | Risco efetivo: EIGHTY=15.0% | 
PEG=25.0% | CICLOS=5.0% | CAP=50%

================================================================================
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | ESCUTA ACTIVA
================================================================================

[dd/mm/yy | HH:MM:SS.mmm] | rem=04:59:999 | UP=50.0c Eff=50.5c | 
DOWN=50.0c Eff=50.5c

[INFO] [PEG ARBIT] [SKIP] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:30:000 | 
PEG_Eff OK (1.0100) mas underpeg=-1.00c < 0.8c

[INFO] [EIGHTY] [WATCH] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:55:000 | 
UP Eff=50.5c | VOL OK (0.1c/5.2s) | D aguarda (0.3s) (WAIT) | ticks=0/5

[INFO] [EIGHTY] [WATCH] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:55:000 | 
DOWN Eff=49.5c | VOL OK (0.1c/5.1s) | D aguarda (0.2s) (WAIT) | ticks=0/5

[dd/mm/yy | HH:MM:SS.mmm] | rem=00:00:000 | UP=50.1c Eff=50.6c | 
DOWN=49.9c Eff=50.4c (sem peg_disp)

================================================================================
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | MARTINGALE | x1 mantido | 
sem trades activadas - estado intacto

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | ROUND | PnL: $0.0000 (0.00%)

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | TOTAL | PnL: $0.0000 (0.00%) | 
Banca: $10.0000 | Accumulated loss: $0.0000 | Uptime: 0y:00m:01d:02h:15m:30s
================================================================================
```

---

## 🔵 CENÁRIO 16: KELLY ATIVO (com Historico)

### SETUP:
- `KELLY_ACTIVE = True`
- `KELLY_MIN_HISTORY = 10` trades passados

### OUTPUT ESPERADO NO TRADE:

```
[INFO] [EIGHTY] [BUY] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:30:200 | 
UP @ nom=82.5c ask=82.51c eff=83.1c | PEG_Eff: 97.5c (0.975) | 
inv=$0.75 (7.5% banca) | shares=0.0090 | fee=0.145% | ticks=5 | 
D1.0s:+0.2c | Kelly f=0.075 | f_kelly=0.125 | CV=0.42 | u=0.023 s=0.010 | 
MC_worst=0.950 | n=18
```

---

## 🟪 CENÁRIO 17: AS+VPIN BLOQUEIO (Fluxos Tóxicos)

### CONDIÇÕES:
- `AS_VPIN_ACTIVE = True`
- `VPIN >= AS_VPIN_WITHDRAW (0.90)`

### OUTPUT ESPERADO:

```
[INFO] [AS VPIN] [WITHDRAW] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:30:000 | 
VPIN=0.92 >= 0.90 | r=50.5c half=0.45c var=0.00123 - BLOQUEADO

[INFO] [EIGHTY] [WATCH] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:29:990 | 
UP Eff=82.5c | VOL OK (2.1c/5.2s) | D1.0s:+0.5c D2.0s:+1.2c D3.0s:+1.8c (UP) | 
PEG_Eff=1.000 | ticks=5/5

[INFO] [EIGHTY] [RESET] [dd/mm/yy | HH:MM:SS.mmm] | rem=04:29:985 | 
UP - AS EDGE NOK - edge 17.5c < min 0.45c
```

---

## 📊 RESUMO DE TAGS E ÍCONES

| Símbolo | Significado |
|---------|-------------|
| `(+)` | Trade com ganho |
| `(-)` | Trade com perda |
| `[CAP]` | Risco limitado pelo CAP (50%) |
| `[MARTINGALE x*]` | Multiplicador martingale ativo |
| `UP` | Delta positivo / saudável |
| `DOWN` | Delta negativo / problema |
| `WAIT` | Aguardando dados |
| `VOL OK` | Volatilidade aceitável |
| `VOL NOK` | Volatilidade excessiva |

---

## 🔧 COMO ATIVIZAR/DESATIVAR MÓDULOS

```python
# NO FICHEIRO bot_xrp_v0.36.1.py (linhas ~73-79)

CICLO_30S_ACTIVE = False  # → True para ativar CICLO_30S
CICLO_20S_ACTIVE = False  # → True para ativar CICLO_20S
EIGHTY_ACTIVE = True      # → False para desativar EIGHTY
PEG_ARBIT_ACTIVE = True   # → False para desativar PEG_ARBIT
KELLY_ACTIVE = False      # → True para ativar KELLY
AS_VPIN_ACTIVE = False    # → True para ativar AS+VPIN
```

---

## ⚙️ FLOW SIMPLIFICADO DAS DECISÕES

```
┌─ NOVO CICLO
├─ Conecta WebSocket → Order Book (ASK/BID)
├─ Loop: A cada novo preço:
│  ├─ 1. PEG ARBIT: underpeg >= 0.8c?
│  │  ├─ SIM + range OK → BUY UP + DOWN
│  │  └─ NÃO → LOG SKIP
│  ├─ 2. EIGHTY: VOL + ticks + delta OK?
│  │  ├─ SIM → BUY side
│  │  └─ NÃO → LOG WATCH
│  ├─ 3. TARGET CHECK: bid >= target?
│  │  └─ SIM → SELL com PnL
│  ├─ 4. STOP-LOSS: 5+ flash levels?
│  │  └─ SIM → SELL com PnL negativo
│  ├─ 5. SETTLEMENT: rem <= 0?
│  │  └─ SIM → Resolve todos trades @ 1.00 ou 0.00
│  └─ [Repeat até settlement]
└─ Calcula PnL da ronda
   ├─ PnL > 0 → Recovery (mult reset)
   ├─ PnL < 0 → Martingale escalado (mult x2)
   └─ PnL = 0 → Estado mantido
```

