# SISTEMA MARTINGALE CONDICIONAL + RECUPERAÇÃO SUAVE

## 📋 ALTERAÇÕES IMPLEMENTADAS

### 1. **Parâmetros Novo Sistema**
```python
MAX_RISK_MULTIPLIER = 32         # Range: [2 ... 32] | Limite máximo (x2, x4, x8, x16, x32)
RECOVERY_ROUNDS_BASE = 10        # Range: [5 ... 20] | Rondas iniciais por cada loss
MAX_RISK_PERCENT = 0.15          # Range: [0.10 ... 0.20] | CAP RÍGIDO 15% da banca
```

### 2. **Variáveis Globais (Demo Persistente)**
```python
bankroll                    = BANKROLL_INIT  # Nunca reseta em Demo
martingale_multiplier       = 1.0            # x1, x2, x4, x8, x16, x32
accumulated_loss            = 0.0            # Soma de perdas para recuperação
recovery_rounds_remaining   = 1              # Rondas para recuperar
```

### 3. **Fórmula de Risco com CAP 15%**
```
Risco Efetivo = min(base * mult + accumulated_loss / rounds / bankroll, 0.15)
```

---

## 🎬 OUTPUTS DO NOVO SISTEMA

### CENÁRIO A: STARTUP DO BOT

```
================================================================================
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | BOT XRP POLYMARKET v0.36.1 INICIADO
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | LIVE_TRADING     : False
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | BANKROLL_INIT    : $10.00

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | RISCO BASE:
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    RISK_PER_TRADE   : 5%
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    EIGHTY_RISK      : 15%
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    PEG_ARBIT_RISK   : 25%

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | MARTINGALE CONDICIONAL + RECUPERAÇÃO:
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    MAX_MULTIPLIER   : x32
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    RECOVERY_ROUNDS  : 10 base
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    MAX_RISK CAP     : 15% (RÍGIDO)
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    Regras:
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    - PnL < 0: mult x2 | +10 rounds | acc_loss += loss
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    - PnL = 0: mult mantém | estado intacto
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    - PnL > 0: mult = x1 | acc_loss -= profit | -1 round
```

---

### CENÁRIO B: NOVO DIA (Reset Diário com Leitura de Saldo)

#### B1 — LIVE TRADING
```
================================================================================
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | NOVO DIA 2026-03-03
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | Saldo Polymarket LIDO: $12.3456
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    Banca (init/live) : $12.3456
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    Modo              : LIVE TRADING
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    Martingale        : x1 | acc_loss=$0.0000 | rounds=1
================================================================================
```

#### B2 — DEMO (Persistente)
```
================================================================================
[INFO] [dd/mm/yy | HH:MM:SS.mmm] | NOVO DIA 2026-03-03
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    Banca (init/live) : $10.0000
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    Modo              : DEMO (banca persistente)
[INFO] [dd/mm/yy | HH:MM:SS.mmm] |    Martingale        : x1 | acc_loss=$0.0000 | rounds=1
================================================================================
```

---

### CENÁRIO C: PRIMEIRA LOSS (Ativa Martingale)

**Contexto:** Ronda 1 com -$0.50 de loss

```
================================================================================
[INFO] [MARTINGALE CONDICIONAL] | PnL < 0 (Loss) | Mult escalado: x2 | 
Acc_loss: $0.2500 | Recovery rounds: 11 | 
Proximo Risco EIGHTY=30.0% PEG=50.0% [CAP 15%]

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | ROUND | PnL: $-0.5000 (-5.00%)

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | TOTAL | PnL: $-0.5000 (-5.00%) | 
Banca: $9.5000 | Accumul.Loss: $0.2500 | Mult: x2 | Uptime: 0y:00m:00d:00h:05m:12s
================================================================================
```

**O que acontece:**
- Loss total de $0.50 → 50% ($0.25) é acumulado
- Multiplicador escala de x1 → **x2**
- Recovery rounds aumentam de 1 → **11** (1 + 10 RECOVERY_ROUNDS_BASE)
- Próximo risco EIGHTY: min(15% * 2 + 0.25/11/9.5, 15%) = **15% [CAP]**
- Próximo risco PEG: min(25% * 2 + 0.25/11/9.5, 15%) = **15% [CAP]**

---

### CENÁRIO D: SEGUNDA LOSS CONSECUTIVA (Dobra Multiplicador Novamente)

**Contexto:** Ronda 2 com mult=x2, acc_loss=$0.25, rounds=11 → Nova loss de -$0.30

```
================================================================================
[INFO] [MARTINGALE CONDICIONAL] | PnL < 0 (Loss) | Mult escalado: x4 | 
Acc_loss: $0.4000 | Recovery rounds: 21 | 
Proximo Risco EIGHTY=60.0% [CAP 15%] PEG=75.0% [CAP 15%]

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | ROUND | PnL: $-0.3000 (-3.16%)

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | TOTAL | PnL: $-0.8000 (-8.00%) | 
Banca: $9.2000 | Accumul.Loss: $0.4000 | Mult: x4 | Uptime: 0y:00m:00d:00h:10m:24s
================================================================================
```

**O que acontece:**
- Nova loss de $0.30 → 50% ($0.15) acumulado
- accumulated_loss: $0.25 + $0.15 = **$0.40**
- Multiplicador escala novamente: **x2 × 2 = x4**
- Recovery rounds: 11 + 10 = **21**
- Ambos os riscos maxam no **CAP RÍGIDO de 15%**

---

### CENÁRIO E: GREEN (Profit > 0) - Recuperação Parcial

**Contexto:** Ronda 3 com mult=x4, acc_loss=$0.40, rounds=21 → Profit de +$0.25

```
================================================================================
[INFO] [MARTINGALE CONDICIONAL] | PnL > 0 (Green) | Mult reset x1 | 
RECUPERAÇÃO PARCIAL (recuperados $0.2500 | restam $0.1500) | 
Rounds restantes: 20

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | ROUND | PnL: $+0.2500 (+2.72%)

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | TOTAL | PnL: $-0.5500 (-5.50%) | 
Banca: $9.4500 | Accumul.Loss: $0.1500 | Mult: x1 | Uptime: 0y:00m:00d:00h:15m:36s
================================================================================
```

**O que acontece:**
- Profit de $0.25
- accumulated_loss: max($0.40 - $0.25, 0) = **$0.15**
- Multiplicador reset imediatamente: **x1**
- Recovery rounds caem: 21 - 1 = **20**
- Recuperação parcial: $0.25 de $0.40 ainda faltam $0.15

---

### CENÁRIO F: GREEN COMPLETO - Recuperação Total

**Contexto:** Ronda 4 com mult=x1, acc_loss=$0.15, rounds=20 → Profit de +$0.20

```
================================================================================
[INFO] [MARTINGALE CONDICIONAL] | PnL > 0 (Green) | Mult reset x1 | 
RECUPERAÇÃO COMPLETA ($0.1500 recuperados em total) | 
Sistema de recovery finalizado

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | ROUND | PnL: $+0.2000 (+2.11%)

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | TOTAL | PnL: $-0.3500 (-3.50%) | 
Banca: $9.6500 | Accumul.Loss: $0.0000 | Mult: x1 | Uptime: 0y:00m:00d:00h:20m:48s
================================================================================
```

**O que acontece:**
- Profit de $0.20 (maior que acc_loss de $0.15)
- accumulated_loss: max($0.15 - $0.20, 0) = **$0.00** ✅
- Multiplicador mantém: **x1** (já estava)
- Recovery rounds reset para **1**
- **Sistema de recovery finalizado**
- Martingale volta ao estado base

---

### CENÁRIO G: ZERO PnL (Mantém Estado)

**Contexto:** Ronda 5 com mult=x2, acc_loss=$0.30, rounds=15 → Trade bloqueado (0 PnL)

```
================================================================================
[INFO] [MARTINGALE CONDICIONAL] | PnL = 0 (Neutro) | Mult x2 mantido | 
sem trades ou sem impacto - estado intacto

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | ROUND | PnL: $0.0000 (0.00%)

[INFO] [dd/mm/yy | HH:MM:SS.mmm] | TOTAL | PnL: $-0.1500 (-1.50%) | 
Banca: $9.8500 | Accumul.Loss: $0.3000 | Mult: x2 | Uptime: 0y:00m:00d:00h:25m:30s
================================================================================
```

**O que acontece:**
- Nenhuma mudança no estado
- Multiplicador mantém: **x2**
- accumulated_loss: **$0.30** (sem alteração)
- recovery_rounds: **15** (sem alteração)
- Sistema aguarda próxima decisão

---

## 📊 TABELA DE ESTADOS (Exemplo Sequencial)

| Ronda | Profit | Mult Antes | Mult Depois | Acc_Loss Antes | Acc_Loss Depois | Rounds Antes | Rounds Depois | Status |
|-------|--------|-----------|-----------|---|---|---|---|---------|
| 1     | -$0.50 | x1        | **x2**    | $0.00   | **$0.25**   | 1   | **11**  | Loss → Escala |
| 2     | -$0.30 | x2        | **x4**    | $0.25   | **$0.40**   | 11  | **21**  | Loss → Escala |
| 3     | +$0.25 | x4        | **x1**    | $0.40   | **$0.15**   | 21  | **20**  | Green → Reset + Recover |
| 4     | +$0.20 | x1        | x1        | $0.15   | **$0.00**   | 20  | **1**   | Green → Completo ✅ |
| 5     | $0.00  | x1        | x1        | $0.00   | $0.00       | 1   | 1       | Neutro → Maintains |

---

## 🔒 SEGURANÇA: CAP RÍGIDO 15%

### Fórmula Aplicada:
```
Risco_Efetivo = min(base_risk * martingale_mult + accum_loss/rounds/bank, 0.15)
```

### Exemplos de Cálculo:

**Exemplo 1 - Loss Simples:**
```
Base EIGHTY Risk = 15%
Mult = x2
Acc_Loss = $0.25
Rounds = 11
Bank = $9.50

Recovery Bonus = 0.25 / 11 / 9.50 = 0.239%
Raw Risk = 15% × 2 + 0.239% = 30.239%
Efetivo = min(30.239%, 15%) = 15% [CAP APLICADO]
```

**Exemplo 2 - Recuperação Avançada:**
```
Base PEG Risk = 25%
Mult = x4
Acc_Loss = $0.40
Rounds = 21
Bank = $9.20

Recovery Bonus = 0.40 / 21 / 9.20 = 2.065%
Raw Risk = 25% × 4 + 2.065% = 102.065%
Efetivo = min(102.065%, 15%) = 15% [CAP APLICADO]
```

---

## 🎯 RESUMO DE COMPORTAMENTOS

| Evento | Mult | Acc_Loss | Rounds | Risco | Descrição |
|--------|------|----------|---------|-------|-----------|
| Startup | x1 | $0 | 1 | Base | Estado inicial |
| Loss #1 | **x2** | **+50% loss** | **+10** | **CAP** | Martingale ativado |
| Loss #2 | **x4** | **+50% loss** | **+10** | **CAP** | Escala continuada |
| Loss #3 | **x8** | **+50% loss** | **+10** | **CAP** | Escala máxima? |
| Loss #4 | **x16** | **+50% loss** | **+10** | **CAP** | Próximo: x32 |
| Loss #5 | **x32** | **+50% loss** | **+10** | **CAP** | Limite atingido |
| Green > Loss | **x1** | **-profit%** | **-1** | Base | Reset completo |

---

## 🔧 COMO AJUSTAR

```python
# Aumentar agressividade de recuperação
RECOVERY_ROUNDS_BASE = 15  # Range [5...20] (estava 10)

# Desacelerar multiplicador
MAX_RISK_MULTIPLIER = 16   # Range [2...32] (estava 32)

# Aumentar CAP se necessário (recomenda-se manter em 15%)
MAX_RISK_PERCENT = 0.20    # Range [0.10...0.20] (estava 0.15)
```

---

## 📍 LOCALIZAÇÃO NO CÓDIGO

- **Parâmetros:** Linhas 48-58
- **Variáveis Globais:** Linhas 113-126
- **Cálculo de Risco:** Linhas 258-274
- **Main Loop:** Linhas 1227-1300 (Reset Diário)
- **Martingale Lógica:** Linhas 1325-1385 (PnL Processing)

