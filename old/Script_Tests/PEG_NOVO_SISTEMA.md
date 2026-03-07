# PEG ARBITRAGE — NOVO SISTEMA IMPLEMENTADO

## 📋 MUDANÇAS IMPLEMENTADAS

### 1. **Novos Parâmetros**
```python
PEG_ARBIT_EFF_THRESHOLD = 0.0985  # PEG_Eff máximo para ativar
PEG_ARBIT_RANGE_1 = (0.0, 45.0)   # Range 1: fechado [0-45c]
PEG_ARBIT_RANGE_2 = (55.0, 99.9)  # Range 2: fechado [55-99.9c]
PEG_ARBIT_BANCA_PCT = 0.25        # 25% de banca fixa
PEG_ARBIT_MIN_REM = 0.05          # 0.05s (50 milisegundos)
```

**Removidos:**
- `PEG_ARBIT_RANGE` (antigo sistema de range único)
- `PEG_ARBIT_UNDERPEG_C` (cálculo de underpeg)

---

## 🎯 NOVA LÓGICA DE ATIVAÇÃO

### Condições para Entrada:

```
✅ PEG_ARBIT_ACTIVE = True
✅ peg_eff <= 0.0985             (PEG efetivo muito baixo)
✅ rem > 0.05s                   (mais de 50ms de tempo restante)
✅ AS/VPIN não bloqueou
✅ peg_arbit_count < MAX_ENTRIES
✅ Cooldown aguarda >= 0.05s

E DEPOIS:
✅ Ambos (UP e DOWN) no range:
   - [0.0c - 45.0c]  OU  [55.0c - 99.9c]
```

---

## 💰 CÁLCULO DO INVESTIMENTO (NOVO)

### Antes (Sistema Antigo):
```
budget = bankroll * eff_peg_risk  (variável com martingale)
budget pode variar entre 5% e 50%+
```

### Agora (Sistema Novo):
```
budget = bankroll * 0.25           (FIXO 25%)
shares_to_buy = budget / max(eff_up, eff_down)
invest_up = shares_to_buy * eff_up
invest_down = shares_to_buy * eff_down
total_invest = invest_up + invest_down
```

---

## 📊 EXEMPLOS DE OUTPUT

### Cenário 1: Entrada Bem-Sucedida

**Condições:**
```
PEG_Eff = 0.0980 (≤ 0.0985 ✓)
UP = 35.2c (range [0-45] ✓)
DOWN = 64.1c (range [55-99.9] ✓)
rem = 125.5s (> 0.05s ✓)
bankroll = $10.00
```

**Output:**
```
================================================================================
[INFO] [PEG ARBIT] [ENTRADA] [dd/mm/yy | HH:MM:SS.mmm] | rem=02:05:500 | 
PEG_Eff=0.0980 (margin 92.00c) | UP=35.2c DOWN=64.1c | Shares=0.0714 | 
Total=$2.5000 (25% banca) | arb #1

[INFO] [PEG ARBIT] [BUY] [dd/mm/yy | HH:MM:SS.mmm] | rem=02:05:500 | 
UP @ nom=35.0c ask=35.01c eff=35.5c | PEG_Eff: 9.8c (0.0980) | 
inv=$1.2500 (12.5% banca) | shares=0.0352 | fee=0.145%

[INFO] [PEG ARBIT] [BUY] [dd/mm/yy | HH:MM:SS.mmm] | rem=02:05:500 | 
DOWN @ nom=64.0c ask=64.01c eff=64.8c | PEG_Eff: 9.8c (0.0980) | 
inv=$1.2500 (12.5% banca) | shares=0.0193 | fee=0.285%
================================================================================
```

---

### Cenário 2: Bloqueio por Range

**Condições:**
```
PEG_Eff = 0.0950 (≤ 0.0985 ✓)
UP = 47.2c (fora dos ranges ✗)
DOWN = 83.3c (range [55-99.9] ✓)
rem = 120.0s (> 0.05s ✓)
```

**Output:**
```
[INFO] [PEG ARBIT] [SKIP] [dd/mm/yy | HH:MM:SS.mmm] | rem=02:00:000 | 
PEG_Eff OK (0.0950) mas UP_Eff 47.2c fora [0-45] e [55-99.9]
```

---

### Cenário 3: PEG_Eff Acima do Threshold

**Condições:**
```
PEG_Eff = 0.1050 (> 0.0985 ✗)
UP = 28.5c (range [0-45] ✓)
DOWN = 75.8c (range [55-99.9] ✓)
rem = 60.0s (> 0.05s ✓)
```

**Output:**
```
[dd/mm/yy | HH:MM:SS.mmm] | rem=01:00:000 | UP=28.3c Eff=28.8c | 
DOWN=75.6c Eff=76.8c
(Sem display PEG pois PEG_Eff > threshold)
```

---

### Cenário 4: Tempo Insuficiente

**Condições:**
```
PEG_Eff = 0.0975 (≤ 0.0985 ✓)
rem = 0.03s (< 0.05s ✗)
UP = 22.1c (range [0-45] ✓)
DOWN = 77.9c (range [55-99.9] ✓)
```

**Output:**
```
[dd/mm/yy | HH:MM:SS.mmm] | rem=00:00:030 | UP=22.0c Eff=22.5c | 
DOWN=77.8c Eff=79.0c | PEG_Eff=0.0975
(Não entra pois rem < 0.05s)
```

---

## 📈 COMPARAÇÃO: ANTES vs DEPOIS

| Aspecto | ANTES | DEPOIS |
|---------|-------|--------|
| PEG Eff Threshold | < 1.0 | ≤ 0.0985 |
| Underpeg Mínimo | ≥ 0.8c | N/A |
| Tempo Mínimo | 5.0s | 0.05s (50ms) |
| Budget | 25% (via eff_peg_risk) | 25% (fixo) |
| Ranges Aceitos | [45-55]c | [0-45] / [55-99.9]c |
| Condição | underpeg | PEG_Eff <= 9.85% |

---

## 🔍 ANÁLISE TÉCNICA

### Por que PEG_Eff = 0.0985?

PEG_Eff = effective_entry(UP) + effective_entry(DOWN)

Se PEG_Eff = 0.0985 (~9.85%), significa:
- UP @ 5c + DOWN @ 4.85c (muito baixo)
- OU qualquer combinação que some ≤ 9.85c

Isso indica um **market extremamente ineficiente**.

### Ranges [0-45] e [55-99.9]:

Evita a zona [45-55] que é considerada neutra/normal no mercado.
- Zona verde: [0-45]c e [55-99.9]c
- Zona vermelha/neutra: [45-55]c (não entra)

---

## 🎬 FLUXO VISUAL

```
┌─ PEG ARBIT LOOP (A cada novo preço)
├─ 1. Calcula PEG_Eff = eff_up + eff_down
├─ 2. Verifica PEG_Eff <= 0.0985? 
│  ├─ NÃO → Passa adiante
│  └─ SIM → Passo 3
├─ 3. Verifica rem > 0.05s?
│  ├─ NÃO → Espera
│  └─ SIM → Passo 4
├─ 4. Verifica ranges:
│  ├─ UP ∈ [0-45] ∨ [55-99.9]? AND DOWN ∈ [0-45] ∨ [55-99.9]?
│  ├─ NÃO → Log SKIP
│  └─ SIM → Passo 5
├─ 5. Budget = bankroll * 0.25
├─ 6. Calcula shares = budget / max(eff_up, eff_down)
├─ 7. BUY UP @ eff_up   (shares)
├─ 8. BUY DOWN @ eff_down (shares)
├─ 9. Aguarda settlement ou EXIT
└─ FIM
```

---

## 📍 LOCALIZAÇÃO NO CÓDIGO

- **Parâmetros:** Linhas 123-131
- **Cálculo PEG_Eff Display:** Linhas 850-857
- **Lógica de Entrada:** Linhas 875-935
- **Display em Módulos:** Linha 661

---

## ⚠️ CUIDADOS

1. **50ms é muito rápido** — Sistema pode ser sensível a lag
2. **25% é agressivo** — Tenha margem na banca
3. **Ranges [0-45] & [55-99.9]** — Evita zona [-1 para entrar

---

## 🔧 AJUSTES POSSÍVEIS

```python
# Aumentar threshold (mais permissivo)
PEG_ARBIT_EFF_THRESHOLD = 0.15  # Range [0.01...0.30]

# Aumentar tempo mínimo (mais seguro)
PEG_ARBIT_MIN_REM = 0.50       # Range [0.01...5.0]

# Reduzir budget (mais conservador)
PEG_ARBIT_BANCA_PCT = 0.15     # Range [0.05...0.50]

# Expandir ranges (mais flexível)
PEG_ARBIT_RANGE_1 = (0.0, 50.0)    # [0-50]
PEG_ARBIT_RANGE_2 = (50.0, 99.9)   # [50-99.9] (quase tudo)
```

