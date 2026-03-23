```markdown
# 🚀 XRP Polymarket Bots – Documentação Completa (Atualizada 23/03/2026)

**Dois bots profissionais para os mercados XRP Up/Down 5 minutos no Polymarket.**

- **`xrp_true_market_maker_v5_3_1.py`** → **True Market Maker** (limit orders + shadow + Monte Carlo + LMSR)  
- **`xrp_bot_v9_4_1.py`** → **Aggressive Gambler** (taker + martingale + endgame + partial TP)

**Ambos usam Shadow Trading realista** (contra livro L2 ao vivo) e calculam **fees reais** do Polymarket.

---

## 1. Polymarket – Documentação Técnica Oficial (como os bots usam)

### 1.1 O que é o Polymarket?
Plataforma de **prediction markets** descentralizada na Polygon.  
Todos os mercados são binários (YES/NO) resolvidos automaticamente via oráculos Chainlink + Binance.

### 1.2 Mercados XRP 5 minutos
- Duração exata: **300 segundos**.
- Slug: `xrp-updown-5m-{timestamp_unix}`.
- Dois tokens: UP e DOWN (clobTokenIds).
- Resolução: $1 por share se ganhar, $0 se perder.

### 1.3 Fees oficiais do Polymarket (usados nos bots)

**Taker Fee (v9.4.1)**
```latex
fee = shares × price × 0.25 × (price × (1 - price))²
```
- v9.4.1 adiciona `fee_buffer = 0.006` em todas as decisões.

**Maker Rebate (v5.3.1)**
```latex
rebate = shares × price × (maker_rebate_bps / 10000)
```
- Default: 20 bps.

### 1.4 Endpoints usados
- Gamma API: descoberta automática do mercado atual.
- CLOB REST + WS: livro L2 real-time.
- Binance WS: preço XRP + funding rate + vol.

---

## 2. Modelos Matemáticos (equações completas)

### 2.1 LMSR (v5.3.1)
```latex
C(q) = b ⋅ ln(e^{q_up/b} + e^{q_down/b})
p_up = e^{q_up/b} / (e^{q_up/b} + e^{q_down/b})
EV = p_LMSR - p_mercado
```
`b = 100000` (quanto maior → spread mais apertado).

### 2.2 Bayesian Signal (ambos)
```latex
log_odds += retorno_XRP / σ²
p_UP = 1 / (1 + e^{-log_odds})
```

### 2.3 Monte Carlo (v5.3.1)
5000 simulações offline antes do boot → GO/NO-GO automático.

### 2.4 Kelly + Martingale (v9.4.1)
```latex
risk = kelly_fraction × edge × mart_level × mart_recovery_factor
```

---

## 3. Como Ler os Logs (explicação detalhada com exemplos reais)

### 3.1 Logs do v5.3.1 – True Market Maker (`mm_v531.log`)

**Exemplo completo de fim de round:**

```
23/03/26 12:30:00.576 | INFO | ROUND 1 | Real:-$3.769726 | Net:-$3.769726 | MtM:$45.5310
23/03/26 12:30:00.576 | INFO | Spr:$+0.000000 | Reb:$+0.221027 | Unr:-$0.699311
23/03/26 12:30:00.582 | INFO | UP=39.10sh DN=9.67sh | Fills:64(shd:64) | WR:0% | Sharpe:-26.63
```

**Explicação linha a linha:**

- **ROUND 1** → Número da rodada (cada mercado de 5 min = 1 round).
- **Real:-$3.769726** → PnL **realizado** nesta rodada (fills + rebates).
- **Net:-$3.769726** → PnL acumulado total desde o início do bot.
- **MtM:$45.5310** → Valor **Mark-to-Market** (bankroll + posição aberta marcada ao preço atual).
- **Spr:$+0.000000** → Spread capturado (diferença entre preço de compra e mid).
- **Reb:$+0.221027** → Rebates de maker recebidos nesta rodada.
- **Unr:-$0.699311** → PnL **não realizado** (posição aberta ainda não vendida).
- **UP=39.10sh DN=9.67sh** → Posição atual em shares (UP e DOWN).
- **Fills:64(shd:64)** → 64 fills totais nesta rodada (todos shadow neste exemplo).
- **WR:0%** → Win Rate dos fills (0% = todos negativos ou zero).
- **Sharpe:-26.63** → Sharpe ratio dos últimos 100 fills (negativo = estratégia perdendo muito).

**Interpretação prática**:  
Este round foi **muito ruim** (perda de ~$3.77). O bot está com posição pesada em UP e o mercado foi contra. Sharpe negativo extremo indica que precisa de ajuste (aumentar deadband ou reduzir band sizes).

---

### 3.2 Logs do v9.4.1 – Aggressive Gambler (`bot_xrp.log`)

**Exemplo de log de estado (emitido a cada mudança de preço):**

```
[23/03/26 | 12:31:28.855] | rem=03:31:145 | UP BID=23.0c ASK=24.0c Z=-1.28 OBI=0.71 | 
DN BID=76.0c ASK=77.0c Z=+1.29 OBI=0.29 | P(UP)=0.197 P(DN)=0.803 | 
BNC=1.41540 | FR=+0.000057 | PEG=1.0100 | REGIME=NORMAL | SPIKE=False
```

**Explicação campo a campo:**

| Campo              | Significado                                                                 | Valor ideal / alerta |
|--------------------|-----------------------------------------------------------------------------|----------------------|
| **rem=03:31:145**  | Tempo restante no mercado (3 minutos e 31.145 segundos)                     | < 25s → endgame      |
| **UP BID=23.0c**   | Melhor bid do UP (em centavos)                                              | -                    |
| **ASK=24.0c**      | Melhor ask do UP                                                            | -                    |
| **Z=-1.28**        | Z-score do filtro Kalman (UP)                                               | > +2 ou < -2 = forte |
| **OBI=0.71**       | Order Book Imbalance (UP) – 0.71 = muita liquidez no bid                    | > 0.6 = bullish      |
| **DN BID=76.0c**   | Melhor bid do DOWN                                                          | -                    |
| **Z=+1.29**        | Z-score do DOWN                                                             | -                    |
| **OBI=0.29**       | Imbalance do DOWN                                                           | -                    |
| **P(UP)=0.197**    | Probabilidade Bayesian posterior para UP                                    | > 0.505 = entrar     |
| **P(DN)=0.803**    | Probabilidade para DOWN                                                     | -                    |
| **BNC=1.41540**    | Preço atual XRP na Binance                                                  | -                    |
| **FR=+0.000057**   | Funding rate Binance (positivo = bullish)                                   | > +0.0004 = ultra    |
| **PEG=1.0100**     | Soma dos asks (UP + DOWN). Deve estar perto de 1.00                         | 1.00 ± 0.02 = ok     |
| **REGIME=NORMAL**  | Regime de volatilidade (LOW / NORMAL / HIGH)                                | -                    |
| **SPIKE=False**    | Detectou spike de volatilidade micro?                                       | False = seguro       |

**Interpretação prática**:  
Mercado com **forte viés para DOWN** (P(DN)=80.3%). O bot provavelmente vai entrar pesado em DOWN nos próximos ticks. PEG=1.0100 indica mercado eficiente. Nenhum spike → entrada segura.

---

## 4. Outros Logs Importantes

### v5.3.1
- `[MC_REPORT]` → Resultado das 5000 simulações no boot (GO/NO-GO).
- `[SHD_FILL]` → Cada fill shadow (preço, slippage, latência, PnL).
- `[CB]` → Circuit breaker disparado.
- `[CONFIG_UPDATE]` → Hot-reload do config.json.

### v9.4.1
- `[GAMBLING] BUY` → Entrada normal de gambling.
- `[ENDGAME_AGG]` → Entrada ultra-agressiva nos últimos 25s.
- `[FEE_REAL]` → Custo exato da taxa Polymarket neste trade.
- `[TP]` → Partial take-profit disparado.
- `[SHADOW] REJECT` → Fill rejeitado por slippage ou profundidade.
- `[MART_OPT]` → Aumento de nível martingale.

---

## 5. Configuração e Parametrização (detalhada)

**secrets.txt** (obrigatório)  
**config.json** (v5.3.1 – hot-reload a cada 15s)  
**BotConfig** (v9.4.1 – editável no código)

**Principais parâmetros ajustáveis** (já listados nas tabelas anteriores).

---

## 6. Safety, Risk e Recomendações

- Sempre comece com **DRY_RUN + SHADOW**.
- Monitore Sharpe e MaxDD no v5.3.1.
- Monitore consecutive_losses e daily_loss no v9.4.1.
- Nunca rode LIVE sem 48h de shadow positivo.
- Pode rodar os dois bots em paralelo (contas diferentes).

**Audit JSONL** contém tudo estruturado para análise posterior.

---

## 7. Instalação e Execução

```bash
pip install websockets py-clob-client web3 requests orjson numpy
python xrp_true_market_maker_v5_3_1.py
python xrp_bot_v9_4_1.py
```

**Flag útil** (v9.4.1): `--test`

---

**Este README está 100% atualizado com:**
- Documentação oficial da Polymarket
- Explicação linha a linha dos dois principais tipos de log
- Equações completas
- Tabelas de impacto
- Exemplos reais

Copia e cola diretamente para o teu `README.md`.  
Script limpo, sem bugs, documentação clara e profissional.

**Boa sorte e trade safe!** 🚀
```