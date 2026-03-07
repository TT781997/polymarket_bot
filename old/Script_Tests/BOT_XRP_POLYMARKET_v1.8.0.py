# =============================================================================
# BOT XRP POLYMARKET — v1.8.0
# =============================================================================
# CHANGELOG v1.8.0 [WebSocket Full-Stream + Overlapping Subs + Metadata Cache]:
#
# ─────────────────────────────────────────────────────────────────────────────
# ARQUITECTURA NOVA (6 áreas críticas de refatoração):
# ─────────────────────────────────────────────────────────────────────────────
#
# [1] WEBSOCKET FULL-STREAM (Área 1):
#     ANTES: fetch_spread_sdk (HTTP REST) chamado em cada evento "book" e
#            "price_change" do WS — latência de dezenas de ms por tick.
#     AGORA: Spread calculado inline a partir dos dados do próprio evento "book"
#            → spread_c = (ask_p - bid_p) * 100 — zero latência REST.
#            Para eventos "best_bid_ask": spread inline de item.get("spread").
#            Para eventos "price_change":  spread inline de bid/ask extraídos.
#            Resultado: única fonte de dados é o stream WS; REST elimidado do
#            caminho crítico de tick.
#     NOVO parâmetro: WS_HEARTBEAT_INTERVAL (linhas: parâmetros WS).
#
# [2] OVERLAPPING SUBSCRIPTIONS (Área 2):
#     ANTES: ws_handler era criado/destruído por ciclo de 5 min. "Ponto cego"
#            nos primeiros segundos de cada novo mercado (WS a reconectar).
#     AGORA: ws_handler é uma task PERSISTENTE que dura toda a sessão do bot.
#            Aceita sub_queue (asyncio.Queue) para subscrições dinâmicas.
#            Em logic_loop, quando rem <= WS_OVERLAP_PRE_S (20s), subscreve
#            tokens do Mercado B via _ws_sub_queue ANTES de A expirar.
#            Ambas as subscrições activas no mesmo túnel WS — sem pontos cegos.
#     NOVO parâmetro: WS_OVERLAP_PRE_S = 20.
#     MODIFICADO: ws_handler(sub_queue) — assinatura mudou.
#     MODIFICADO: logic_loop — overlap trigger interno.
#     MODIFICADO: main() — ws_task persistente entre ciclos.
#
# [3] STARTUP METADATA CACHE (Área 3):
#     ANTES: fetch_metadata(slug) chamado no início de CADA ciclo de 5 min —
#            GET REST por ciclo, bloqueante para o 1º trade.
#     AGORA: _prefetch_metadata_cache() no arranque faz batch GETs para os
#            próximos METADATA_PREFETCH_COUNT (12) mercados → cache em
#            _metadata_cache dict.
#            main() consulta _metadata_cache primeiro; fetch_metadata só como
#            fallback se slug ausente (ex: mercado fora da janela de 1h).
#            Reconexão WS longa (>= 8s backoff) dispara reload do cache via
#            _ws_needs_cache_reload flag.
#     NOVA função: _prefetch_metadata_cache() → async.
#     NOVO global: _metadata_cache: dict.
#     NOVO global: _ws_needs_cache_reload: bool.
#     NOVO parâmetro: METADATA_PREFETCH_COUNT = 12.
#
# [4] REAL-TIME SETTLEMENT / FIM DO LUCRO FALSO (Área 4):
#     ANTES: Timeout RESOLVE_TIMEOUT_S (35s) → fallback: estima vencedor pelo
#            BID mais alto. SELL logado com payout estimado, não real.
#     AGORA: ELIMINADO o bloco asyncio.TimeoutError com estimativa por BID.
#            close_trade_resolution SÓ é chamado quando market_resolved WS
#            chega com winning_asset_id real.
#            Se WS demorar > RESOLVE_TIMEOUT_S: log WARN mas NÃO loga PnL
#            falso — espera indefinidamente pela mensagem real.
#            Mecanismo: _per_market_resolved[slug] → {event, winner} por ciclo.
#            _token_to_slug dict: token_id → slug para routing no ws_handler.
#     NOVO global: _per_market_resolved: dict.
#     NOVO global: _token_to_slug: dict.
#     MODIFICADO: lógica de resolução em logic_loop (fim de mercado).
#     MODIFICADO: ws_handler — market_resolved popula _per_market_resolved.
#
# [5] ARQUITECTURA NÃO-BLOQUEANTE (Área 5):
#     ANTES: main() fazia await logic_loop(...) — espera pela resolução do
#            Mercado A bloqueava o arranque do Mercado B.
#     AGORA: main() usa asyncio.create_task(logic_loop(...)) — não-bloqueante.
#            Novo ciclo (1ª aposta em B) arranca imediatamente enquanto A espera
#            market_resolved. _active_logic_tasks lista gere o ciclo de vida.
#            PnL accounting movido para o interior de logic_loop (era em main).
#     NOVO global: _active_logic_tasks: list.
#     MODIFICADO: logic_loop — PnL contabilização interna; aceita ws_sub_queue.
#     MODIFICADO: main() — create_task; sem await logic_loop.
#
# [6] RESILIÊNCIA DE LIGAÇÃO (Área 6):
#     ANTES: backoff exponencial em reconexão mas sem heartbeat activo.
#            Conexões "zombie" (socket aberto, sem tráfego) não detectadas.
#     AGORA: ws_handler lança _heartbeat_task interno (asyncio.create_task).
#            Envia ws.ping() cada WS_HEARTBEAT_INTERVAL (15s).
#            Aguarda pong com timeout de 10s.
#            Pong timeout → ws.close() forçado → reconexão imediata.
#            Em reconexão: re-subscreve todos os tokens conhecidos via
#            _known_subscriptions buffer interno do ws_handler.
#            Backoff alto (>= 8s) → seta _ws_needs_cache_reload → main()
#            recarrega _metadata_cache no próximo ciclo.
#     NOVO parâmetro: WS_HEARTBEAT_INTERVAL = 15.
#     MODIFICADO: ws_handler — heartbeat task + re-subscrição após reconexão.
#
# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÕES/GLOBALS AFECTADOS v1.8.0:
#   - Parâmetros globais: +3 novos (WS_OVERLAP_PRE_S, WS_HEARTBEAT_INTERVAL,
#                                    METADATA_PREFETCH_COUNT).
#   - Globais de estado: +5 novos (_metadata_cache, _ws_sub_queue,
#                                   _per_market_resolved, _active_logic_tasks,
#                                   _token_to_slug, _ws_needs_cache_reload).
#   - _prefetch_metadata_cache() [NOVA] — batch GET 12 mercados no arranque.
#   - ws_handler(sub_queue) [REFATORADO] — persistente, sub dinâmica, heartbeat.
#   - logic_loop(m_start, m_end, meta, ws_sub_queue) [MODIFICADO]:
#       + aceita ws_sub_queue; overlap sub; PnL interno; sem BID fallback.
#   - main() [REFATORADO] — cache prefetch; ws_task persistente; create_task.
#
# FUNÇÕES PRESERVADAS (lógica core intacta):
#   - generate_polymarket_url / get_current_slug / get_market_and_token_ids
#   - fetch_metadata / fetch_spread_sdk (mantidas para fallback e compatibilidade)
#   - get_token_price (mantida estruturalmente; eliminada do caminho crítico WS)
#   - KalmanFilter1D, HFTWindow, VPINTracker — sem alterações.
#   - open_trade / close_trade / close_trade_resolution — sem alterações.
#   - RateLimiter, CircuitBreaker, retry_with_backoff — sem alterações.
#   - Toda a lógica PEG ARBIT, GAMBLING, SL, TP — sem alterações.
# =============================================================================
# CHANGELOG v1.7.2 [Filtro Spread Oficial SDK + Parametrização 92-95% WR]:
#
# [1] SPREAD OFICIAL SDK:
#     Removido completamente BID_ASK_MIN_RATIO + cálculos manuais.
#     Novo: MAX_SPREAD_CENTS = 1.10 (usa diretamente best_spreads_c do client.get_spread).
#     Bloqueia entradas quando o livro está largo (pre-crash).
#
# [2] PARAMETRIZAÇÃO ULTRA-CONSERVADORA (92-95% win rate target):
#     GAMB_START_REM_S     = 165s (só últimos 2m45s)
#     GAMB_MAX_VOL_DEV     = 0.018 (squeeze extremo)
#     GAMB_MAX_ZSCORE      = 0.55  (anti-pico de ferro)
#     GAMB_MIN_OBI         = 0.83
#     VPIN_SAFE_LIMIT      = 0.33
#     MAX_SPREAD_CENTS     = 1.10
#     SL_BASE_TRIGGER      = 0.34 (corta mais cedo)
#     TP_SPIKE_ZSCORE      = 5.0
#     GAMB_BUY_COOLDOWN    = 18s (evita acumular)
#
# [3] STOP_LOSS e TAKE_PROFIT ativados por defeito.
# [4] Mantidos todos os módulos Production-Ready (RateLimiter, CircuitBreaker, etc.).
# =============================================================================
# CHANGELOG v1.7.1  [Remoção Total do Módulo TREND]:
#   Removido: parâmetros TREND_UPDATE_S, TREND_FIDELITY, TREND_THRESHOLD,
#             TREND_INTERVAL, TREND_LOG_PTS, MICRO_TREND_*, GAMB_NEUTRAL_BOTH.
#   Removido: globais xrp_1h_trend, xrp_1h_token_up.
#   Removido: funções _fetch_trend_sync_sdk, _fetch_trend_sync_rest,
#             fetch_trend_from_clob, trend_update_task.
#   Removido: class MicroTrendTracker.
#   Removido: filtro de lado por trend no loop Gambling (opera sempre ambos).
#   Removido: NEUTRAL_BLOCK logic e gamb_neutral_block_last.
#   Removido: micro_trend_trackers state e log periódico de micro-trend.
#   Removido: trend_task em main() (create + cancel).
#   Gambling opera agora sempre UP e DOWN sem restrições de trend.
# =============================================================================
# =============================================================================
#
# CHANGELOG v1.7.0  [Production-Ready + Win Rate 95% — 4 áreas críticas]:
#
# ─────────────────────────────────────────────────────────────────────────────
# ROOT CAUSE ANALYSIS (logs 05/03/26):
#   Bug A — TP vende com PREJUÍZO:
#     BUY @ ASK=65c, TP dispara a BID=62c (Z=+3.03) → loss -7.14%.
#     BUY @ ASK=66c, TP dispara a BID=67c (Z=+3.38) → loss -0.97% (fees).
#     Break-even BID: sempre ~2c ACIMA do ASK de entrada (fees embutem isso).
#     Causa: TP_SPIKE_ZSCORE=3.0 demasiado baixo; nenhuma verificação de lucro.
#   Bug B — Entradas GAMB_MAX_ZSCORE=1.1 bloqueiam Z negativos mas não filtram
#     suficientemente o ruído de spread, permitindo entradas a 65-66c que nunca
#     atingem o BID de break-even antes de crash.
# ─────────────────────────────────────────────────────────────────────────────
#
# [1] TAKE-PROFIT — Portão de Lucratividade (fix crítico do Bug A):
#     NOVO: TP só dispara se sell_payout_net(shares, bid) > total_out * (1 + TP_MIN_PROFIT_PCT).
#     NOVO: TP só dispara se bid >= trade["ask"] + TP_MIN_BID_OVER_ASK (2c de margem mínima).
#     TP_SPIKE_ZSCORE : 3.0 -> 4.5 (menos falsos positivos; Z=3 ocorre com frequência em 10s)
#     TP_MIN_PROFIT_PCT : 0.005 (novo — 0.5% lucro líquido mínimo após fees)
#     TP_MIN_BID_OVER_ASK : 0.02 (novo — BID precisa de +2c sobre ASK de entrada para cobrir fees)
#     Log diferenciado: SKIP_UNPROFITABLE quando portão bloqueia; WICK quando dispara.
#
# [2] GAMBLING — Filtros de Qualidade de Entrada (evitar entrar em crashes):
#     GAMB_MIN_EFF_C : 65.0 -> 67.0 (evita entradas em preços onde break-even BID é impossível)
#     GAMB_MAX_EFF_C : 78.0         (mantido — bloqueia preços altos demais)
#     GAMB_MAX_ZSCORE : 1.1 -> 1.3  (ligeiramente mais permissivo — menos WAIT sem entradas)
#     GAMB_MAX_VOL_DEV : 0.04 -> 0.03 (mais apertado — regime mais comprimido exigido)
#     GAMB_BUY_COOLDOWN : 8.0 -> 12.0 (mais tempo entre entradas — evita compras em cascata)
#     GAMB_MIN_OBI : 0.70 -> 0.75 (compradores precisam de dominar 75%+ do livro)
#     Novo filtro BID >= ASK * BID_ASK_MIN_RATIO antes de entrar (liquidez saudável):
#     BID_ASK_MIN_RATIO = 0.94 (BID não pode ser <94% do ASK — spread demasiado largo = perigo)
#
# [3] PRODUCTION READY — Infraestrutura de Resiliência (novo):
#
#     [3a] RateLimiter (class RateLimiter — token bucket):
#          Previne ban por excesso de pedidos à API Polymarket.
#          RATE_LIMIT_CALLS = 8   (pedidos REST por segundo — abaixo do limite Polymarket)
#          RATE_LIMIT_BURST = 15  (rajada máxima permitida)
#          Uso: await rate_limiter.acquire() antes de cada chamada REST.
#          Partilhado globalmente por fetch_metadata, fetch_spread_sdk, place_live_order, etc.
#
#     [3b] retry_with_backoff (função utilitária async):
#          Executa qualquer callable com retry exponencial em falha de rede.
#          MAX_API_RETRIES = 3    (tentativas máximas antes de desistir)
#          BASE_BACKOFF_S  = 1.0  (espera inicial em segundos)
#          MAX_BACKOFF_S   = 32.0 (tecto exponencial)
#          BACKOFF_JITTER  = True (adiciona ruído aleatório para evitar thundering herd)
#          Retorna None em falha permanente — callers fazem fallback gracioso.
#
#     [3c] CircuitBreaker (class CircuitBreaker):
#          Para de chamar endpoints que estão em falha sistemática.
#          CB_FAIL_THRESHOLD  = 5  (falhas consecutivas para abrir o circuito)
#          CB_RECOVERY_S      = 60 (segundos em OPEN antes de tentar HALF-OPEN)
#          Estados: CLOSED (normal) → OPEN (parar calls) → HALF-OPEN (testar) → CLOSED.
#          Instância global: api_circuit_breaker (partilhada por todos os REST calls).
#          Log de transições de estado para ficheiro.
#
#     [3d] WebSocket Reconnection com Backoff Exponencial:
#          v1.6.0 usava sleep(1) fixo em qualquer erro WS.
#          v1.7.0: backoff 1s → 2s → 4s → 8s → 16s (máx) com reset em conexão bem-sucedida.
#          WS_RECONNECT_BASE_S = 1.0, WS_RECONNECT_MAX_S = 16.0
#
#     [3e] LIVE_TRADING = True (produção activa):
#          Requer secrets.txt com POLYMARKET_PRIVATE_KEY.
#          Reverter para DEMO: LIVE_TRADING = False (sem outras alterações necessárias).
#          place_live_order: confirmação de orderID no log; falha não crasheia o bot.
#          fetch_fee_rate_bps: protegido por rate_limiter + retry_with_backoff.
#
#     [3f] Graceful Shutdown (SIGTERM + KeyboardInterrupt):
#          Handler SIGTERM registado em main() para shutdown limpo.
#          Cancela ws_task antes de sair.
#          Log de estado final antes de terminar.
#
# [4] STOP-LOSS — Ajuste Anti-Pânico adicional:
#     SL_CRASH_ZSCORE : -4.0 -> -5.0 (mais tolerância a quedas abruptas — menos panic sell)
#     SL_BASE_TRIGGER : 0.25          (mantido — linha de perigo a 25c)
#     SL_PANIC_OBI    : 0.02          (mantido — colapso real a 2% compradores)
#     SL_TOXIC_VPIN   : 0.95 -> 0.97  (mais tolerância antes de aceitar dump institucional)
#
# FUNÇÕES/CLASSES AFECTADAS v1.7.0:
#   - Parâmetros globais: 11 valores alterados, 10 novos parâmetros adicionados.
#   - class RateLimiter (nova)
#   - class CircuitBreaker (nova)
#   - retry_with_backoff() (nova função async)
#   - fetch_metadata(): protegido por rate_limiter + retry + circuit_breaker
#   - fetch_fee_rate_bps(): protegido por rate_limiter + retry
#   - fetch_spread_sdk(): protegido por rate_limiter + retry
#   - fetch_live_bankroll(): protegido por retry
#   - ws_handler(): backoff exponencial em reconnect
#   - logic_loop():
#       - Novo filtro BID_ASK_MIN_RATIO antes de entrar em Gambling
#       - TP: portão de lucratividade (2 novas condições) + log SKIP_UNPROFITABLE
#       - SL: SL_CRASH_ZSCORE e SL_TOXIC_VPIN actualizados
#   - main(): handler SIGTERM; log de arranque actualizado
#
# =============================================================================
# CHANGELOG v1.6.0  [6 Refactorings Cirúrgicos — Win Rate 95%]:
# [1] GAMB_MAX_VOL_DEV 0.10->0.04; GAMB_MAX_ZSCORE 1.8->1.1; VPIN_SAFE_LIMIT 0.55->0.40
# [3] Endgame Override: rem<=30.999s -> Z_lim=99 / VPIN_lim=0.60
# [4] GAMB_MAX_EFF_C 95->78; TP_SPIKE_ZSCORE mantido 3.0 (corrigido em v1.7.0)
# [5] SL_BASE_TRIGGER 0.35->0.25; SL_PANIC_OBI 0.05->0.02
# [6] KELLY_FRACTION 0.20->0.08; KELLY_MAX_RISK_PCT 0.08->0.05
# =============================================================================
# CHANGELOG v1.5.0  [Cérebro Quantitativo Bidirecional — 5 módulos]:
# [1] KalmanFilter1D; [2] VPINTracker; [3] Gambling 4 condições;
# [4] TP Dinâmico Z-Score; [5] SL Triple-Trigger OR; [6] Kelly Criterion
# =============================================================================
# CHANGELOG v1.4.0  [Motor HFT Z-Score + Orderbook Imbalance]
# CHANGELOG v1.3.0  [3 alterações cirúrgicas]
# CHANGELOG v1.2.1  [Stop-Loss Cirúrgico]
# CHANGELOG v1.2.0  [5 alterações: PEG ARBIT; Tick Log; Gambling NEUTRAL; Preços SDK]
# CHANGELOG v1.1.0: GAMBLING_RISK=0.03; PEG_ARBIT_RISK=0.05; Logging
# CHANGELOG v1.0.0: BUY ao ASK; SELL ao BID; WS market_resolved; Martingale 20%/x8
# =============================================================================

import asyncio
import math
import random
import signal
import websockets
import json
import time
import logging
import requests
import os
from datetime import datetime
from collections import deque

# =============================================================================
# PARAMETROS CONFIGURÁVEIS
# =============================================================================

# --- MODO DE OPERACAO ---
LIVE_TRADING    = False    # True=ordens reais (requer secrets.txt); False=simulacao DEMO [Range: False | True]

# --- BANCA ---
BANKROLL_DEMO   = 10.0    # Banca DEMO em USDC; persistente, nunca reseta entre dias [Range: 1.0 | 100000.0]

# --- RISCO BASE ---
# v1.5.0: GAMBLING_RISK removido — Kelly Criterion calcula dinamicamente por trade.
# [DEPRECATED v1.5.0] GAMBLING_RISK = 0.03  # Substituido por calc_kelly_risk(ask)
# [DEPRECATED v1.5.0] MAX_RISK_MULT = 8     # Substituido por Kelly (sem Martingale)
# [DEPRECATED v1.5.0] RECOVERY_ROUNDS_STEP = 10  # Removido com Martingale
PEG_ARBIT_RISK    = 0.25  # Risco fixo PEG ARBIT (arb, nao direccional — Kelly nao se aplica) [Range: 0.01 | 0.20]
MAX_RISK_PERCENT  = 0.35  # Cap PEG ARBIT: investimento PA nunca excede 35% da banca [Range: 0.10 | 0.50]

# --- TOGGLES ---
PEG_ARBIT_ACTIVE    = True   # Peg Arbit activo [Range: False | True]
GAMBLING_ACTIVE     = True   # Gambling activo [Range: False | True]
STOP_LOSS_ACTIVE    = False   # Stop-Loss activo [Range: False | True]
TAKE_PROFIT_ACTIVE  = False   # Take-Profit dinamico activo (wick capture) [Range: False | True]

# --- PEG ARBIT (ex SPREAD CATCH) ---
PA_TRIGGER_SUM    = 0.985        # Gatilho: entra se ask_up+ask_down <= valor (underpeg no custo real) [Range: 0.940 | 0.999]
PA_COOLDOWN       = 0.05         # Intervalo minimo entre entradas PA consecutivas (seg) [Range: 0.01 | 5.0]
PA_MIN_REM        = 1.0          # Remaining minimo para entrar no PA (seg) [Range: 1.0 | 30.0]
PA_TARGET_BID_C   = 0.0          # Target de venda antecipada ao BID (cents; 0=hold ate resolucao) [Range: 0.0 | 99.0]
MAX_PA_ENTRIES    = 10_000_000   # Entradas maximas PA por ciclo [Range: 1 | 10000000]

# --- GAMBLING ---
GAMB_START_REM_S  = 300      # Activa Gambling quando remaining <= X seg [Range: 60 | 300]
GAMB_CUTOFF_S     = 0        # Para Gambling quando remaining <= X seg [Range: 0 | 30]
GAMB_MIN_EFF_C    = 75.0     # eff_c minimo para entrada (cents); v1.7.0: 65->67 (break-even BID >= 69c) [Range: 50.0 | 95.0]
GAMB_MAX_EFF_C    = 95.0     # eff_c maximo para entrada (cents); v1.6.0: 95->78 [Range: 65.0 | 99.9]
GAMB_BUY_COOLDOWN = 8.0     # Cooldown entre compras do mesmo lado (seg); v1.7.0: 8->12 (menos entradas em cascata) [Range: 0.5 | 60.0]
GAMB_PEG_MIN      = 0.980    # Soma minima ask_up + ask_down para entrar (liquidez minima) [Range: 0.90 | 0.999]
GAMB_TARGET_BID_C = 0.0      # Take-Profit ESTATICO ao BID (cents; 0=desactivado; TP dinamico via TP_SPIKE_ZSCORE) [Range: 0.0 | 99.0]
# [DEPRECATED v1.5.0] GAMB_MIN_IMBALANCE = 0.60  # Renomeado para GAMB_MIN_OBI
# [DEPRECATED v1.4.0] GAMB_MIN_TICKS, GAMB_VOL_MAX_C, GAMB_D*_THRESH_C — substituidos por HFT

# --- FILTRO SPREAD OFICIAL SDK (novo v1.7.2) ---
MAX_SPREAD_CENTS  = 2.20     # Spread máximo permitido (em cents) — oficial do client.get_spread

# --- FILTRO DE LIQUIDEZ BID/ASK (novo v1.7.0) ---
#
#   Antes de entrar num lado Gambling, verifica que o spread BID/ASK e saudavel.
#   BID < ASK * BID_ASK_MIN_RATIO => spread demasiado largo => perigo de crash iminente.
#   Ex: ASK=0.65 e BID_ASK_MIN_RATIO=0.94 => BID >= 0.611 obrigatorio para entrar.
#   Evita entrar quando market makers se afastaram (sinal pre-crash).
#
BID_ASK_MIN_RATIO = 0.94    # BID >= ASK * ratio: spread saudavel minimo para entrada [Range: 0.85 | 0.99]

# --- MOTOR QUANTITATIVO HFT (KALMAN, Z-SCORE, VPIN, OBI) ---
#
#   Arquitectura por lado (UP / DOWN):
#     KalmanFilter1D  -> suaviza mid_price (separa ruido do preco real)
#     HFTWindow       -> janela 10s de preco Kalman para Z-Score e StdDev
#     VPINTracker     -> janela 10s de fluxo de ordens para toxicidade
#
#   A cada tick WS:
#     kal = kalman.update(mid)        -> preco real estimado
#     hft.add(kal, now)               -> actualiza janela temporal
#     z   = hft.zscore(kal)           -> Z-Score vs. trajectoria Kalman
#     std = hft.std()                 -> volatilidade da janela
#     vpin_tracker.add(kal, vol, now) -> actualiza tracker de fluxo
#     vpin = vpin_tracker.vpin()      -> toxicidade 0-1
#     obi  = calc_imbalance(bs, as)   -> orderbook imbalance do top
#
HFT_WINDOW_SECONDS   = 10      # Janela termica de memoria do mercado (seg) [Range: 10 | 120]
KALMAN_PROCESS_NOISE = 8e-6    # Ruido do modelo de transicao Q (menor=mais suavizado) [Range: 1e-7 | 1e-2]
KALMAN_MEASURE_NOISE = 4e-3    # Ruido de observacao do mercado R (maior=mais suavizado) [Range: 1e-4 | 1.0]

# --- MICRO-REGIMES & GAMBLING ENTRY (4 condicoes) ---
#
#   Cond 1 — Regime de compressao (Squeeze):
GAMB_MAX_VOL_DEV   = 0.03   # StdDev(Kalman,10s) <= 3c: regime estavel/comprimido; v1.7.0: 0.04->0.03 [Range: 0.01 | 0.15]
#   Cond 2 — Anti-pico (Z-Score):
GAMB_MAX_ZSCORE    = 1.3    # Z <= 1.3: preco nao esta num pico vs. trajectoria Kalman; v1.7.0: 1.1->1.3 [Range: 0.5 | 3.0]
#   Cond 3 — Suporte real (OBI = Orderbook Imbalance):
GAMB_MIN_OBI       = 0.65   # OBI >= 65%: compradores dominam o Top of Book; v1.7.0: 0.70->0.65 [Range: 0.50 | 0.90]
#   Cond 4 — Fluxo saudavel (VPIN = Order Flow Toxicity):
VPIN_SAFE_LIMIT    = 0.40   # VPIN <= 0.40: fluxo normal; sem dump institucional detetado [Range: 0.30 | 0.95]

# --- ENDGAME OVERRIDE — Modo Agressivo Final (v1.6.0) ---
#
#   Quando remaining_seconds <= ENDGAME_TRIGGER_S:
#     Cond 2 (Z-Score): limite temporário -> ENDGAME_ZSCORE_LIMIT (99.0 = desativado)
#     Cond 4 (VPIN):    limite temporário -> ENDGAME_VPIN_LIMIT  (0.60 = relaxado)
#   Globals GAMB_MAX_ZSCORE e VPIN_SAFE_LIMIT NUNCA são mutados (thread-safe).
#
ENDGAME_TRIGGER_S    = 30.999  # Activa modo agressivo quando remaining <= X seg [Range: 10.0 | 60.0]
ENDGAME_ZSCORE_LIMIT = 99.0    # Limite Z-Score em modo ENDGAME (99.0 = desativado) [Range: 2.0 | 99.0]
ENDGAME_VPIN_LIMIT   = 0.70    # Limite VPIN em modo ENDGAME (relaxado vs. VPIN_SAFE_LIMIT) [Range: 0.40 | 0.95]

# --- TAKE-PROFIT DINAMICO (Wick Capture — v1.5.0 / v1.7.0) ---
#
#   v1.7.0 — FIX CRÍTICO: TP agora tem PORTÃO DE LUCRATIVIDADE duplo antes de vender:
#
#   Portão 1 — BID mínimo vs. ASK de entrada:
#     bid >= trade["ask"] + TP_MIN_BID_OVER_ASK
#     Razão: break-even BID é sempre ~2c ACIMA do ASK de entrada (fees de compra + venda).
#     Ex: ASK=65c => break-even BID >= 67c; TP a BID=62c era uma perda garantida.
#
#   Portão 2 — Lucratividade líquida real:
#     sell_payout_net(shares, bid) > total_out * (1 + TP_MIN_PROFIT_PCT)
#     Garante que o payout líquido real supera o custo total all-in + margem mínima.
#
#   Só depois dos 2 portões é verificado Z >= TP_SPIKE_ZSCORE.
#   Log SKIP_UNPROFITABLE quando portões bloqueiam (Z >= threshold mas lucro insuficiente).
#   TP_SPIKE_ZSCORE: 3.0 -> 4.5 (menos falsos positivos — Z=3 ocorre frequentemente em 10s).
#
TP_SPIKE_ZSCORE      = 4.5    # Vende GAMBLING se Z(Kalman) >= 4.5 (wick muito absurdo); v1.7.0: 3.0->4.5 [Range: 2.0 | 6.0]
TP_MIN_BID_OVER_ASK  = 0.02   # BID deve ser >= ASK_entrada + 2c para cobrir fees de compra+venda [Range: 0.01 | 0.10]
TP_MIN_PROFIT_PCT    = 0.005  # Lucro líquido mínimo após fees para TP disparar (0.5%) [Range: 0.0 | 0.05]

# --- STOP-LOSS (lógica OR: BID <= threshold E qualquer trigger) ---
#
#   Trigger base: BID <= SL_BASE_TRIGGER (linha de perigo)
#   Trigger A — VPIN toxicidade extrema:
SL_TOXIC_VPIN      = 0.97   # VPIN >= 97%: dump institucional confirmado; v1.7.0: 0.95->0.97 [Range: 0.60 | 1.0]
#   Trigger B — Z-Score crash Kalman:
SL_CRASH_ZSCORE    = -5.0   # Z <= -5.0: queda violenta vs. trajectoria Kalman; v1.7.0: -4.0->-5.0 [Range: -6.0 | -0.5]
#   Trigger C — OBI panico:
SL_PANIC_OBI       = 0.02   # OBI <= 2%: colapso real confirmado — compradores abandonaram [Range: 0.01 | 0.50]
SL_BASE_TRIGGER    = 0.25   # BID <= 25c activa a verificacao dos triggers A/B/C [Range: 0.10 | 0.60]
# [DEPRECATED v1.5.0] SL_TRIGGER_PRICE = 0.35  # Renomeado para SL_BASE_TRIGGER
# [DEPRECATED v1.5.0] SL_PANIC_IMBALANCE = 0.25  # Renomeado para SL_PANIC_OBI
# [DEPRECATED v1.4.0] SL_TICKS, SL_CHECK_S, SL_MID_MAX, SL_THRESHOLD — removidos

# --- KELLY CRITERION (Money Management dinamico) ---
#
#   Formula: p_est = ask + KELLY_ASSUMED_EDGE
#            odds  = (1-ask) / ask
#            kelly = p_est - (1-p_est) / odds
#            frac  = kelly * KELLY_FRACTION
#            risk  = min(frac, KELLY_MAX_RISK_PCT)
#
KELLY_ASSUMED_EDGE = 0.10   # Vantagem probabilistica assumida quando HFT da luz verde (+5%) [Range: 0.01 | 0.20]
KELLY_FRACTION     = 0.60   # Fractional Kelly: ~1/12 de Kelly para seguranca maxima [Range: 0.05 | 1.0]
KELLY_MAX_RISK_PCT = 0.25   # Cap absoluto de risco por trade Kelly (5% da banca) [Range: 0.01 | 0.25]

# --- PRODUCTION READY — Rate Limiting e Resiliência (novo v1.7.0) ---
#
#   Rate Limiter (token bucket — previne ban da API Polymarket):
RATE_LIMIT_CALLS    = 8     # Pedidos REST sustentados por segundo (abaixo do limite Polymarket) [Range: 1 | 20]
RATE_LIMIT_BURST    = 15    # Rajada máxima instantânea de pedidos [Range: 5 | 30]
#
#   Retry com backoff exponencial (resiliência a falhas de rede):
MAX_API_RETRIES     = 3     # Tentativas máximas antes de desistir [Range: 1 | 10]
BASE_BACKOFF_S      = 1.0   # Espera inicial entre retries (seg) [Range: 0.1 | 5.0]
MAX_BACKOFF_S       = 32.0  # Tecto do backoff exponencial (seg) [Range: 5.0 | 120.0]
BACKOFF_JITTER      = True  # Adiciona ruído aleatório para evitar thundering herd [Range: False | True]
#
#   Circuit Breaker (para de chamar endpoints em falha sistemática):
CB_FAIL_THRESHOLD   = 5     # Falhas consecutivas para abrir o circuito [Range: 2 | 20]
CB_RECOVERY_S       = 60.0  # Segundos em estado OPEN antes de tentar HALF-OPEN [Range: 10.0 | 300.0]
#
#   WebSocket Reconnect com backoff exponencial:
WS_RECONNECT_BASE_S = 1.0   # Espera inicial em reconexão WS (seg) [Range: 0.5 | 5.0]
WS_RECONNECT_MAX_S  = 16.0  # Tecto do backoff WS (seg) [Range: 4.0 | 60.0]

# --- WEBSOCKET FULL-STREAM — Overlapping Subs + Heartbeat + Cache (novo v1.8.0) ---
#
#   Overlapping Subscriptions (evitar pontos cegos na transição de mercados):
WS_OVERLAP_PRE_S    = 20    # Subscreve próximo mercado X seg antes de A expirar [Range: 10 | 45]
#
#   Heartbeat (detecta conexões zombie — socket aberto sem tráfego):
WS_HEARTBEAT_INTERVAL = 15  # Intervalo entre pings WS (seg); pong timeout = 10s [Range: 5 | 60]
#
#   Threshold de backoff para forçar reload do metadata cache após reconexão longa:
WS_CACHE_RELOAD_BACKOFF = 8.0  # Se backoff >= X seg, recarrega cache no próximo ciclo [Range: 4.0 | 32.0]

# --- STARTUP METADATA CACHE (novo v1.8.0) ---
#
#   Pré-carrega metadados dos próximos N mercados no arranque.
#   Elimina chamadas GET fetch_metadata durante o ciclo de trading.
#   N = 12 → 1 hora de operações sem REST para metadata.
#   Reload automático se WS teve reconexão longa (WS_CACHE_RELOAD_BACKOFF).
#
METADATA_PREFETCH_COUNT = 12   # Mercados a pré-carregar no arranque (12 = 60min) [Range: 1 | 48]

# --- FEES (Polymarket 5-min crypto — docs trading/fees) ---
#
#   Formula oficial: fee = C * p * feeRate * (p * (1-p))^exponent
#   Simplificada como taxa sobre valor transaccionado:
#       fee_rate(p) = feeRate * (p*(1-p))^exponent = 0.25 * (p*(1-p))^2
#
#   BUY ao ASK: fee_paid = invested_pure * fee_rate(ask)  [cobrada em shares]
#   SELL ao BID: fee_paid = payout_bruto * fee_rate(bid)  [cobrada em USDC]
#   RESOLUCAO (p=1.0 ou 0.0): fee_rate(1.0) = 0 => sem fee no resgate
#
FEE_RATE = 0.25  # Taxa base Polymarket 5-min crypto; NAO ALTERAR [Range: 0.25 | 0.25]
FEE_EXP  = 2     # Expoente da curva de fee; NAO ALTERAR [Range: 2 | 2]

# --- LOOP ---
LOOP_SLEEP        = 0.001   # Timeout entre iteracoes do loop principal (seg) [Range: 0.0001 | 0.1]
RESOLVE_TIMEOUT_S = 35.0    # Tempo de aviso (warn) enquanto aguarda market_resolved WS (seg) [Range: 10.0 | 120.0]
# v1.8.0: RESOLVE_TIMEOUT_S é agora um threshold de AVISO, não de fallback.
#         Se ultrapassado, loga WARN mas CONTINUA a aguardar a mensagem real.
#         Elimina estimativa por BID (Área 4 da refatoração).

# =============================================================================
# ENDPOINTS
# =============================================================================

CLOB_REST_URL = "https://clob.polymarket.com"
GAMMA_API_URL = "https://gamma-api.polymarket.com"
WS_URI        = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# =============================================================================
# GLOBAIS DE ESTADO
# =============================================================================

bankroll         = BANKROLL_DEMO
daily_profit     = 0.0
last_day         = None

# v1.5.0: Martingale globals REMOVIDOS.
# [DEPRECATED v1.5.0] risk_multiplier  = 1.0
# [DEPRECATED v1.5.0] accumulated_loss = 0.0
# [DEPRECATED v1.5.0] recovery_rounds  = 0

# Microestrutura em tempo real (actualizados por tick WS)
best_bids      = {"up": None, "down": None}  # Melhor BID por lado (preco de VENDA) [Range: 0.0 | 1.0]
best_asks      = {"up": None, "down": None}  # Melhor ASK por lado (preco de COMPRA) [Range: 0.0 | 1.0]
best_spreads_c = {"up": None, "down": None}  # Spread em cents absolutos (inline WS — v1.8.0) [Range: 0.0 | 100.0]

# Sizes do Top of Book para OBI (actualizados pelo WS evento "book")
best_bid_sizes = {"up": None, "down": None}  # Volume do melhor BID [Range: 0.0 | inf]
best_ask_sizes = {"up": None, "down": None}  # Volume do melhor ASK [Range: 0.0 | inf]

price_change   = asyncio.Event()
bot_start_time = time.time()
_shutdown_flag = False   # v1.7.0: sinaliza shutdown gracioso via SIGTERM

# Resolucao do mercado actual (actualizados pelo WS) — mantido para compatibilidade
resolved_event        = asyncio.Event()   # Set quando WS envia market_resolved
resolved_winner_asset = None              # winning_asset_id do evento WS [Range: None | str]

# PnL global
total_pnl_pos = 0.0
total_pnl_neg = 0.0

# --- v1.8.0: Novos globais de arquitectura ---

# Metadata cache: slug -> meta_dict; pré-carregado por _prefetch_metadata_cache() no arranque.
# Elimina chamadas REST fetch_metadata durante o ciclo de trading.
_metadata_cache: dict       = {}

# Queue de subscrições dinâmicas para o ws_handler persistente.
# Callers fazem: await _ws_sub_queue.put({"assets_ids": [...], "slug": "..."})
# ws_handler lê entre mensagens WS e envia o payload de subscrição.
_ws_sub_queue: asyncio.Queue = None   # Inicializado em main() após asyncio.run()

# Per-market resolution state (Área 4 — Real-Time Settlement).
# Cada slug tem um asyncio.Event e o winner_asset associado.
# Estrutura: {slug: {"event": asyncio.Event(), "winner": None | str}}
# ws_handler popula quando market_resolved chega; logic_loop espera o seu slug.
_per_market_resolved: dict  = {}

# Mapa token_id -> slug para o ws_handler conseguir fazer routing de market_resolved.
# Populado por logic_loop antes de iniciar cada ciclo.
_token_to_slug: dict        = {}

# Lista de tasks logic_loop activas (Área 5 — Non-Blocking).
# main() cria com asyncio.create_task; gere ciclo de vida.
_active_logic_tasks: list   = []

# Flag de reload de cache após reconexão WS longa (Área 6 — Resiliência).
# ws_handler seta True quando backoff >= WS_CACHE_RELOAD_BACKOFF.
# main() verifica e recarrega _metadata_cache.
_ws_needs_cache_reload: bool = False

# =============================================================================
# LOGGING — exclusivo em ficheiro; bot corre silencioso no terminal
# =============================================================================

_fmt  = logging.Formatter("%(message)s")
_fh   = logging.FileHandler("bot_xrp.log", encoding="utf-8")
_fh.setFormatter(_fmt)
logger = logging.getLogger("bot_xrp")
logger.setLevel(logging.DEBUG)
logger.addHandler(_fh)
logger.propagate = False

# =============================================================================
# FORMATACAO — sem print(); apenas logger.info / logger.warning
# =============================================================================

def get_ts() -> str:
    return datetime.now().strftime("%d/%m/%y | %H:%M:%S.%f")[:-3]

def get_remaining_str(rem: float) -> str:
    rem = max(0.0, rem)
    return f"{int(rem // 60):02d}:{int(rem % 60):02d}:{int((rem * 1000) % 1000):03d}"

def get_uptime_str() -> str:
    e    = int(time.time() - bot_start_time)
    h, e = divmod(e, 3600)
    m, s = divmod(e, 60)
    return f"{h:02d}h:{m:02d}m:{s:02d}s"

def fc(p: float) -> str:
    """Formata preco 0-1 em cents: 0.87 -> '87.0c'"""
    return f"{p * 100:.1f}c"

def log_m(module: str, action: str, msg: str):
    logger.info(f"[INFO] [{module}] [{action}] [{get_ts()}] | {msg}")

def log_info(msg: str):
    logger.info(f"[INFO] [{get_ts()}] | {msg}")

def log_warn(msg: str):
    logger.warning(f"[WARN] [{get_ts()}] | {msg}")

def log_raw(msg: str):
    logger.info(f"[{get_ts()}] | {msg}")

def log_sep():
    logger.info("-" * 80)

def log_sep2():
    logger.info("=" * 80)

# =============================================================================
# PRODUCTION READY — Rate Limiter (Token Bucket)
# =============================================================================

class RateLimiter:
    """
    Token bucket rate limiter para pedidos REST à API Polymarket.

    Previne ban por excesso de pedidos (rate limiting) em modo LIVE.
    Partilhado globalmente por todas as funções que fazem chamadas REST.

    Parâmetros:
      calls_per_second : taxa sustentada (RATE_LIMIT_CALLS = 8 req/s)
      burst            : rajada máxima instantânea (RATE_LIMIT_BURST = 15)

    Algoritmo Token Bucket:
      tokens += (now - last_check) * calls_per_second    (recarga contínua)
      tokens = min(tokens, burst)                         (tecto = burst)
      acquire(): se tokens >= 1: consome 1 token e retorna imediatamente.
                 senão: aguarda (1 - tokens) / calls_per_second segundos.

    Uso:
      await rate_limiter.acquire()   # antes de qualquer chamada REST
    """
    __slots__ = ("calls_per_second", "burst", "tokens", "last_check", "_lock")

    def __init__(self, calls_per_second: float = 8.0, burst: float = 15.0):
        self.calls_per_second: float  = calls_per_second
        self.burst:            float  = burst
        self.tokens:           float  = burst  # começa cheio
        self.last_check:       float  = time.monotonic()
        self._lock                    = asyncio.Lock()

    async def acquire(self):
        """Aguarda até ter token disponível. Sempre liberta depois de chamar."""
        async with self._lock:
            now    = time.monotonic()
            delta  = now - self.last_check
            self.last_check = now
            self.tokens = min(self.burst, self.tokens + delta * self.calls_per_second)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
            else:
                wait_s = (1.0 - self.tokens) / self.calls_per_second
                await asyncio.sleep(wait_s)
                self.tokens = 0.0

# Instância global partilhada por todos os REST calls
rate_limiter = RateLimiter(calls_per_second=RATE_LIMIT_CALLS, burst=RATE_LIMIT_BURST)

# =============================================================================
# PRODUCTION READY — Circuit Breaker
# =============================================================================

class CircuitBreaker:
    """
    Circuit Breaker para endpoints REST externos.

    Previne cascata de erros quando a API Polymarket está em falha sistemática.
    Três estados:
      CLOSED   -> normal — todas as chamadas passam.
      OPEN     -> em falha — rejeita imediatamente sem tentar; espera CB_RECOVERY_S.
      HALF-OPEN -> a testar — permite uma chamada; se OK volta a CLOSED, senão OPEN.

    Parâmetros:
      fail_threshold : CB_FAIL_THRESHOLD = 5 (falhas consecutivas para OPEN)
      recovery_s     : CB_RECOVERY_S = 60 (segundos em OPEN)

    Uso:
      if api_circuit_breaker.is_open():
          log_warn("Circuit OPEN — a saltar chamada")
          return None
      # ... faz a chamada ...
      api_circuit_breaker.record_success()   # ou
      api_circuit_breaker.record_failure()
    """
    __slots__ = ("fail_threshold", "recovery_s", "_failures", "_state", "_opened_at")

    STATE_CLOSED    = "CLOSED"
    STATE_OPEN      = "OPEN"
    STATE_HALF_OPEN = "HALF-OPEN"

    def __init__(self, fail_threshold: int = 5, recovery_s: float = 60.0):
        self.fail_threshold: int   = fail_threshold
        self.recovery_s:    float  = recovery_s
        self._failures:     int    = 0
        self._state:        str    = self.STATE_CLOSED
        self._opened_at:    float  = 0.0

    def is_open(self) -> bool:
        """Retorna True se o circuito está OPEN (não deve fazer chamadas)."""
        if self._state == self.STATE_CLOSED:
            return False
        if self._state == self.STATE_OPEN:
            if time.monotonic() - self._opened_at >= self.recovery_s:
                self._state = self.STATE_HALF_OPEN
                log_info(f"CircuitBreaker | OPEN -> HALF-OPEN (a testar depois de {self.recovery_s:.0f}s)")
                return False  # permite uma tentativa em HALF-OPEN
            return True
        # HALF-OPEN: permite a tentativa
        return False

    def record_success(self):
        """Chamada bem-sucedida — fecha o circuito se estava HALF-OPEN."""
        if self._state != self.STATE_CLOSED:
            log_info(f"CircuitBreaker | {self._state} -> CLOSED (chamada OK)")
        self._state    = self.STATE_CLOSED
        self._failures = 0

    def record_failure(self):
        """Regista falha. Abre o circuito se ultrapassar o threshold."""
        self._failures += 1
        if self._state == self.STATE_HALF_OPEN:
            # Falhou em HALF-OPEN — volta a OPEN
            self._state     = self.STATE_OPEN
            self._opened_at = time.monotonic()
            log_warn(f"CircuitBreaker | HALF-OPEN -> OPEN (falhou em teste)")
        elif self._failures >= self.fail_threshold and self._state == self.STATE_CLOSED:
            self._state     = self.STATE_OPEN
            self._opened_at = time.monotonic()
            log_warn(
                f"CircuitBreaker | CLOSED -> OPEN "
                f"({self._failures} falhas consecutivas >= threshold {self.fail_threshold})"
            )

# Instância global partilhada
api_circuit_breaker = CircuitBreaker(
    fail_threshold=CB_FAIL_THRESHOLD,
    recovery_s=CB_RECOVERY_S
)

# =============================================================================
# PRODUCTION READY — Retry com Backoff Exponencial
# =============================================================================

async def retry_with_backoff(fn, *args, label: str = "call", **kwargs):
    """
    Executa fn(*args, **kwargs) com retry exponencial em caso de excepção.

    Parâmetros globais:
      MAX_API_RETRIES : número máximo de tentativas (default 3)
      BASE_BACKOFF_S  : espera inicial (default 1.0s)
      MAX_BACKOFF_S   : tecto do backoff (default 32.0s)
      BACKOFF_JITTER  : adiciona ruído aleatório ±0.3×backoff (default True)

    Retorna o resultado de fn() em sucesso.
    Retorna None após MAX_API_RETRIES falhas (caller deve fazer fallback gracioso).

    Nota: fn deve ser uma função síncrona (chamada em executor se bloqueante)
          ou uma coroutine. retry_with_backoff detecta automaticamente.

    Integrado em:
      fetch_metadata, fetch_fee_rate_bps, fetch_spread_sdk,
      fetch_live_bankroll.
    """
    last_exc = None
    for attempt in range(1, MAX_API_RETRIES + 1):
        try:
            if asyncio.iscoroutinefunction(fn):
                return await fn(*args, **kwargs)
            else:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, lambda: fn(*args, **kwargs))
        except Exception as exc:
            last_exc = exc
            if attempt < MAX_API_RETRIES:
                backoff = min(BASE_BACKOFF_S * (2 ** (attempt - 1)), MAX_BACKOFF_S)
                if BACKOFF_JITTER:
                    backoff *= (0.7 + random.random() * 0.6)  # ±30% jitter
                log_warn(
                    f"retry_with_backoff [{label}] | attempt {attempt}/{MAX_API_RETRIES} "
                    f"falhou: {exc} — aguardando {backoff:.1f}s"
                )
                await asyncio.sleep(backoff)
    log_warn(
        f"retry_with_backoff [{label}] | DESISTIDO após {MAX_API_RETRIES} tentativas: {last_exc}"
    )
    return None

# =============================================================================
# SECRETS + SDK
# =============================================================================

def load_secrets(filepath: str = "secrets.txt") -> dict:
    if not os.path.exists(filepath):
        return {}
    out = {}
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    return out

_creds                 = load_secrets()
POLYMARKET_PRIVATE_KEY = _creds.get("POLYMARKET_PRIVATE_KEY", "")

clob_client    = None   # Cliente autenticado (LIVE_TRADING=True)
clob_ro_client = None   # Cliente somente-leitura (spread + trend; sempre disponivel)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs
    from py_clob_client.order_builder.constants import BUY as SDK_BUY
    clob_ro_client = ClobClient(host=CLOB_REST_URL, chain_id=137)
    if LIVE_TRADING:
        if not POLYMARKET_PRIVATE_KEY:
            logger.error(f"[ERROR] [{get_ts()}] | FATAL: LIVE_TRADING=True mas chave ausente em secrets.txt!")
            raise SystemExit(1)
        clob_client = ClobClient(host=CLOB_REST_URL, key=POLYMARKET_PRIVATE_KEY, chain_id=137)
        log_info("SDK Polymarket carregado — LIVE TRADING ACTIVO")
    else:
        log_info("SDK Polymarket carregado (read-only) — DEMO MODE")
except ImportError:
    if LIVE_TRADING:
        logger.error(f"[ERROR] [{get_ts()}] | py-clob-client nao instalado! Instala com: pip install py-clob-client")
        raise SystemExit(1)
    log_warn("py-clob-client nao instalado — precos via WS; trend via REST fallback")
except Exception as _sdk_err:
    log_warn(f"SDK init parcial: {_sdk_err}")

# =============================================================================
# MATEMATICA CORE
# =============================================================================

_FEE_RATE = FEE_RATE
_FEE_EXP  = FEE_EXP

def fee_rate(p: float) -> float:
    """
    Taxa de fee Polymarket 5-min crypto como fraccao do valor transaccionado.
    Formula (docs trading/fees): fee_rate(p) = feeRate * (p*(1-p))^exponent
    feeRate=0.25, exponent=2. Max=1.56% em p=0.50. Zero em p=0 e p=1.

    BUY (ao ASK):  fee = invested_pure * fee_rate(ask)    [cobrada em shares]
    SELL (ao BID): fee = payout_bruto * fee_rate(bid)     [cobrada em USDC]
    RESOLUCAO p=1: fee_rate(1.0) = 0 => resgate sem fee   [100% do valor]
    """
    return _FEE_RATE * (p * (1.0 - p)) ** _FEE_EXP

def eff_price_c(ask: float) -> float:
    """
    Custo efectivo all-in por share ao COMPRAR ao ASK (em cents).
    eff_c = ask * (1 + fee_rate(ask)) * 100
    Usado para filtragem de ranges no Gambling.
    """
    return ask * (1.0 + fee_rate(ask)) * 100.0

def sell_payout_net(shares: float, bid: float) -> float:
    """
    Payout liquido ao VENDER 'shares' ao BID (preco real de venda).
    docs: 'you will receive the bid when selling'
    payout_bruto = shares * bid
    fee_sell     = payout_bruto * fee_rate(bid)  [cobrada em USDC]
    payout_net   = payout_bruto * (1 - fee_rate(bid))
    """
    return shares * bid * (1.0 - fee_rate(bid))

def resolution_payout(shares: float, winner: bool) -> float:
    """
    Payout na resolucao do mercado (docs concepts/resolution).
    Vencedor: redeem a $1.00/share; fee_rate(1.0)=0 => payout=shares
    Perdedor: tokens worthless => payout=0.0
    """
    return shares if winner else 0.0

def calc_kelly_risk(ask: float) -> float:
    """
    Kelly Criterion para dimensionamento dinamico de posicoes em mercados binarios.

    Contexto:
      Compramos shares a 'ask' por share. Se ganharmos: recebemos $1/share.
      Probabilidade implicita do mercado: ask (ex: 0.87 = 87% chance de ganhar).
      Vantagem estimada (edge HFT): KELLY_ASSUMED_EDGE = 5% acima do implicito.

    Formula:
      p_est = min(ask + KELLY_ASSUMED_EDGE, 0.995)   # prob. estimada com edge
      odds  = (1 - ask) / ask                         # payout liquido se ganhar
      kelly = p_est - (1 - p_est) / odds              # kelly % da banca
      frac  = kelly * KELLY_FRACTION                  # ~1/12 Kelly para seguranca
      risk  = min(frac, KELLY_MAX_RISK_PCT)           # cap absoluto 5%

    Kelly negativo ou zero: nao entra (sem edge suficiente neste preco).

    Parametros configuráveis:
      KELLY_ASSUMED_EDGE = 0.05  — edge assumido quando HFT da luz verde
      KELLY_FRACTION     = 0.08  — fraccao de Kelly (seguranca maxima)
      KELLY_MAX_RISK_PCT = 0.05  — cap de 5% da banca por trade
    """
    if ask <= 0.0 or ask >= 1.0:
        return 0.0
    p_est = min(ask + KELLY_ASSUMED_EDGE, 0.995)
    odds  = (1.0 - ask) / ask
    kelly = p_est - (1.0 - p_est) / odds
    if kelly <= 0.0:
        return 0.0
    frac = kelly * KELLY_FRACTION
    return min(frac, KELLY_MAX_RISK_PCT)

def calc_imbalance(bid_size, ask_size):
    """
    Calcula Orderbook Imbalance (OBI) do Top of Book.

    OBI = BID_Size / (BID_Size + ASK_Size)

    Range: 0.0 (100% vendedores) a 1.0 (100% compradores).
    Retorna None se sizes indisponiveis (WS ainda nao enviou evento book).

    Interpretacao:
      >= GAMB_MIN_OBI (0.65): compradores dominam — suporte real.
      <= SL_PANIC_OBI (0.02): compradores abandonaram — colapso real.

    Fonte: exclusivamente eventos "book" do WS (best_bid_sizes, best_ask_sizes).
    Eventos "best_bid_ask" e "price_change" nao fornecem sizes.
    """
    if bid_size is None or ask_size is None:
        return None
    total = bid_size + ask_size
    if total <= 1e-9:
        return None
    return bid_size / total

# =============================================================================
# SDK HELPERS — spread (com rate limiter + retry)
# Mantidas para fallback e compatibilidade; v1.8.0 não as usa no caminho crítico WS.
# =============================================================================

def _fetch_spread_sdk_sync(token_id: str):
    """
    Versão síncrona interna de fetch_spread_sdk.
    Chamada via run_in_executor pelo WS handler ou via retry_with_backoff.
    v1.8.0: não chamada do WS handler (spread inline); disponível como fallback.
    """
    if clob_ro_client is None:
        return None
    result = clob_ro_client.get_spread(token_id)
    raw    = result.get("spread")
    if raw is None:
        return None
    return float(raw) * 100.0

async def fetch_spread_sdk(token_id: str):
    """
    Obtem spread nativo via SDK Polymarket (uso interno para best_spreads_c).
    v1.7.0: protegido por rate_limiter + retry_with_backoff + circuit_breaker.
    v1.8.0: mantida para fallback manual; removida do caminho crítico WS.
            No ws_handler o spread é calculado inline sem REST.
    client.get_spread(token_id)["spread"] * 100 = cents absolutos.
    Fallback: retorna None (caller usa ultimo spread conhecido).
    """
    if api_circuit_breaker.is_open():
        return None
    await rate_limiter.acquire()
    result = await retry_with_backoff(
        _fetch_spread_sdk_sync, token_id, label=f"spread_sdk({token_id[:8]})"
    )
    if result is not None:
        api_circuit_breaker.record_success()
    else:
        api_circuit_breaker.record_failure()
        log_warn(f"fetch_spread_sdk ({token_id[:12]}...) falhou — usando ultimo spread conhecido")
    return result

# =============================================================================
# API HELPERS (com rate limiter + retry + circuit breaker)
# =============================================================================

def _fetch_metadata_sync(slug: str):
    """Versão síncrona interna de fetch_metadata."""
    data = requests.get(f"{GAMMA_API_URL}/events?slug={slug}", timeout=5).json()[0]["markets"][0]
    ids  = json.loads(data["clobTokenIds"])
    return {"id": data["conditionId"], "up": ids[0], "down": ids[1], "slug": slug}

async def fetch_metadata(slug: str):
    """
    v1.7.0: protegido por rate_limiter + retry_with_backoff + circuit_breaker.
    v1.8.0: usado principalmente na pre-carga (_prefetch_metadata_cache) e fallback.
            Durante o ciclo de trading, main() consulta _metadata_cache primeiro.
    Retorna None em falha permanente (main() faz retry no próximo ciclo).
    """
    if api_circuit_breaker.is_open():
        log_warn(f"fetch_metadata | CircuitBreaker OPEN — a saltar ({slug})")
        return None
    await rate_limiter.acquire()
    result = await retry_with_backoff(_fetch_metadata_sync, slug, label=f"metadata({slug})")
    if result is not None:
        api_circuit_breaker.record_success()
    else:
        api_circuit_breaker.record_failure()
        log_warn(f"fetch_metadata falhou ({slug}) — circuit_breaker.failures={api_circuit_breaker._failures}")
    return result

def _fetch_fee_rate_bps_sync(token_id: str) -> int:
    """Versão síncrona interna de fetch_fee_rate_bps."""
    r = requests.get(f"{CLOB_REST_URL}/fee-rate", params={"token_id": token_id}, timeout=4)
    return int(r.json().get("fee_rate_bps", 0))

async def fetch_fee_rate_bps(token_id: str) -> int:
    """
    Fetch dinamico de fee_rate_bps antes de cada ordem LIVE.
    docs: 'Always fetch fee_rate_bps dynamically — do not hardcode.'
    v1.7.0: protegido por rate_limiter + retry_with_backoff.
    Retorna 0 em falha (ordem continua sem fee_bps — menos preciso mas seguro).
    """
    await rate_limiter.acquire()
    result = await retry_with_backoff(
        _fetch_fee_rate_bps_sync, token_id, label=f"fee_bps({token_id[:8]})"
    )
    return result if result is not None else 0

def get_current_slug():
    now      = time.time()
    start_ts = now - (now % 300)
    if (start_ts + 300 - now) < 5:
        start_ts += 300
    return f"xrp-updown-5m-{int(start_ts)}", start_ts

def _fetch_live_bankroll_sync():
    """Versão síncrona interna."""
    if not clob_client:
        return None
    return float(clob_client.get_balance())

async def fetch_live_bankroll():
    """
    v1.7.0: protegido por rate_limiter + retry_with_backoff.
    Retorna None em falha (main usa BANKROLL_DEMO como fallback).
    """
    if not clob_client:
        return None
    await rate_limiter.acquire()
    result = await retry_with_backoff(_fetch_live_bankroll_sync, label="live_bankroll")
    if result is None:
        log_warn("fetch_live_bankroll falhou — usando banca actual")
    return result

def redeem_live_position(shares: float, token_id: str):
    """
    Resgata tokens vencedores apos resolucao (LIVE mode).
    Chama redeemPositions via SDK — converte winning tokens em USDC.
    docs concepts/resolution: 'call redeemPositions on the CTF contract'
    """
    if not clob_client:
        return
    try:
        result = clob_client.redeem_positions(token_id=token_id, amount=shares)
        log_info(f"REDEEM | {shares:.4f} shares resgatadas | token={token_id[:16]}... | {result}")
    except Exception as e:
        log_warn(f"REDEEM falhou: {e}")

# =============================================================================
# STARTUP METADATA CACHE (novo v1.8.0 — Área 3)
# =============================================================================

async def _prefetch_metadata_cache() -> dict:
    """
    Pré-carrega metadados dos próximos METADATA_PREFETCH_COUNT mercados XRP-5M.

    Realiza um único batch de GETs no arranque do bot para os próximos N mercados
    (N=12 = 1 hora de operações), armazenando em cache. Elimina chamadas REST
    fetch_metadata durante o ciclo de trading.

    Algoritmo:
      1. Detecta o slot de 5 minutos actual (ou o seguinte se <5s de vida).
      2. Calcula os slugs dos próximos METADATA_PREFETCH_COUNT slots (×300s).
      3. Chama fetch_metadata para cada slug com pausa de 0.15s entre calls
         (respeita rate limiter — 8 req/s sustentados).
      4. Armazena resultados em dict {slug: meta_dict}.
      5. Retorna o dict (main() guarda em _metadata_cache global).

    Fallback:
      Se um slug falhar (network, slug inexistente), é omitido do cache.
      main() faz fallback para fetch_metadata em runtime se slug ausente.

    Reconexão WS longa:
      ws_handler seta _ws_needs_cache_reload = True quando backoff >= WS_CACHE_RELOAD_BACKOFF.
      main() chama _prefetch_metadata_cache() novamente para garantir cache fresco.
    """
    cache    = {}
    now      = time.time()
    start_ts = now - (now % 300)
    if (start_ts + 300 - now) < 5:
        start_ts += 300  # Salta ciclo quase expirado

    log_sep()
    log_info(f"METADATA PREFETCH | {METADATA_PREFETCH_COUNT} mercados ({METADATA_PREFETCH_COUNT * 5} min)")

    for i in range(METADATA_PREFETCH_COUNT):
        ts   = start_ts + (i * 300)
        slug = f"xrp-updown-5m-{int(ts)}"
        meta = await fetch_metadata(slug)
        if meta:
            cache[slug] = meta
            log_info(
                f"  CACHE [{i+1:02d}/{METADATA_PREFETCH_COUNT}] OK | {slug} "
                f"| up={meta['up'][:12]}... | down={meta['down'][:12]}..."
            )
        else:
            log_warn(f"  CACHE [{i+1:02d}/{METADATA_PREFETCH_COUNT}] FALHOU | {slug}")
        # Pausa mínima para não sobrecarregar o rate limiter
        await asyncio.sleep(0.15)

    log_info(f"METADATA PREFETCH | {len(cache)}/{METADATA_PREFETCH_COUNT} mercados em cache")
    log_sep()
    return cache

# =============================================================================
# WEBSOCKET HANDLER PERSISTENTE (v1.8.0 — Áreas 1, 2, 4, 6)
# =============================================================================

async def ws_handler(sub_queue: asyncio.Queue):
    """
    WebSocket handler persistente com subscrições dinâmicas e heartbeat.

    v1.8.0 — Refatoração completa (6 melhorias):

    [1] Full-Stream (Área 1):
        Spread calculado INLINE a partir dos dados WS (sem REST fetch_spread_sdk):
          - Evento "book":         spread_c = (ask_p - bid_p) * 100
          - Evento "best_bid_ask": spread_c = item.get("spread") * 100
          - Evento "price_change": spread_c = (ask_p - bid_p) * 100 se ambos presentes
        Zero latência REST no caminho crítico de tick.

    [2] Overlapping Subscriptions (Área 2):
        Aceita sub_queue (asyncio.Queue) para subscrições dinâmicas.
        _tid_map é mutável: updated between messages via sub_queue.get_nowait().
        Suporte a múltiplas subscrições simultâneas no mesmo túnel WS.
        Cada mensagem de subscrição: {"assets_ids": [...], "slug": "..."}

    [4] Real-Time Settlement (Área 4):
        market_resolved popula _per_market_resolved[slug] via _token_to_slug routing.
        ws_handler também mantém resolved_event/resolved_winner_asset globais
        para compatibilidade retroactiva.

    [6] Resiliência + Heartbeat (Área 6):
        ping_interval=None — gerimos pings manualmente via _heartbeat_task.
        _heartbeat_task: asyncio.create_task interno.
          - Envia ws.ping() cada WS_HEARTBEAT_INTERVAL (15s).
          - Aguarda pong com timeout=10s.
          - Pong timeout → ws.close() forçado → reconexão imediata.
        Em reconexão: re-subscreve todos os tokens em _known_subscriptions.
        Backoff >= WS_CACHE_RELOAD_BACKOFF → seta _ws_needs_cache_reload = True.

    Legado (mantido de v1.7.0):
        Backoff exponencial: 1s→2s→4s→8s→16s (reset em conexão bem-sucedida).
        Eventos tratados: book / best_bid_ask / price_change / market_resolved.
        best_bid_sizes e best_ask_sizes para OBI (evento "book").

    Parâmetros:
      sub_queue : asyncio.Queue com payloads {"assets_ids": [t_up, t_down], "slug": slug}
                  Criada em main(); populada por main() e logic_loop().
    """
    global resolved_winner_asset, _ws_needs_cache_reload

    _bids   = best_bids
    _asks   = best_asks
    _sprc   = best_spreads_c
    _bsizes = best_bid_sizes
    _asizes = best_ask_sizes
    _set    = price_change.set

    # _tid_map mutável: token_id -> side_key ("up" | "down")
    # Actualizado entre mensagens quando sub_queue tem novos payloads.
    _tid_map: dict = {}

    # Buffer de subscrições conhecidas para re-subscrição após reconexão.
    # Lista de payloads {"assets_ids": [...], "slug": ...}
    _known_subscriptions: list = []

    _ws_backoff = WS_RECONNECT_BASE_S   # backoff actual — reset em conexão bem-sucedida

    while not _shutdown_flag:
        try:
            # ping_interval=None: gerimos heartbeat manualmente para logging e controlo.
            async with websockets.connect(
                WS_URI,
                ping_interval=None,
                ping_timeout=None
            ) as ws:

                # ── Reset backoff em conexão bem-sucedida ────────────────────
                log_info(
                    f"WS conectado (backoff_reset={WS_RECONNECT_BASE_S}s) "
                    f"| subs_conhecidas={len(_known_subscriptions)}"
                )
                _ws_backoff = WS_RECONNECT_BASE_S

                # ── Re-subscreve tokens conhecidos após reconexão ────────────
                # Garante que não perdemos mercados activos (Área 6).
                for _sub_payload in _known_subscriptions:
                    await ws.send(json.dumps({
                        "assets_ids":             _sub_payload["assets_ids"],
                        "type":                   "market",
                        "custom_feature_enabled": True
                    }))
                    log_info(
                        f"WS RE-SUB | slug={_sub_payload.get('slug','?')} "
                        f"| {[t[:12] for t in _sub_payload['assets_ids']]}"
                    )

                # ── Drena sub_queue pendente (novas subs enquanto reconnectava) ─
                while not sub_queue.empty():
                    try:
                        _pending = sub_queue.get_nowait()
                        # Verifica se não é duplicado
                        _aids = _pending["assets_ids"]
                        if not any(s["assets_ids"] == _aids for s in _known_subscriptions):
                            _known_subscriptions.append(_pending)
                            _tid_map[_aids[0]] = "up"
                            _tid_map[_aids[1]] = "down"
                            # Actualiza routing de market_resolved
                            _slug = _pending.get("slug", "")
                            if _slug:
                                _token_to_slug[_aids[0]] = _slug
                                _token_to_slug[_aids[1]] = _slug
                        await ws.send(json.dumps({
                            "assets_ids":             _aids,
                            "type":                   "market",
                            "custom_feature_enabled": True
                        }))
                        log_info(
                            f"WS NOVA SUB (queue pendente) | slug={_pending.get('slug','?')} "
                            f"| {[t[:12] for t in _aids]}"
                        )
                    except asyncio.QueueEmpty:
                        break

                # ── Heartbeat Task (Área 6) ──────────────────────────────────
                async def _heartbeat():
                    """
                    Envia ws.ping() cada WS_HEARTBEAT_INTERVAL segundos.
                    Aguarda pong com timeout de 10s.
                    Pong timeout → fecha ws → força reconexão imediata.
                    """
                    try:
                        while True:
                            await asyncio.sleep(WS_HEARTBEAT_INTERVAL)
                            try:
                                pong_waiter = await ws.ping()
                                await asyncio.wait_for(pong_waiter, timeout=10.0)
                            except asyncio.TimeoutError:
                                log_warn(
                                    f"WS HEARTBEAT | pong timeout (>10s) — "
                                    f"fechando conexão para forçar reconexão"
                                )
                                await ws.close()
                                return
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass

                _hb_task = asyncio.create_task(_heartbeat())

                try:
                    # ── Loop principal de mensagens ──────────────────────────
                    async for raw in ws:

                        # ── Processa sub_queue entre mensagens (Área 2) ──────
                        # get_nowait() é non-blocking — zero latência no tick.
                        while not sub_queue.empty():
                            try:
                                _new_sub = sub_queue.get_nowait()
                                _aids    = _new_sub["assets_ids"]
                                _slug    = _new_sub.get("slug", "")
                                # Actualiza _tid_map (Mercado B -> "up"/"down")
                                _tid_map[_aids[0]] = "up"
                                _tid_map[_aids[1]] = "down"
                                # Routing market_resolved (Área 4)
                                if _slug:
                                    _token_to_slug[_aids[0]] = _slug
                                    _token_to_slug[_aids[1]] = _slug
                                # Buffer para re-subscrição após reconexão
                                if not any(s["assets_ids"] == _aids for s in _known_subscriptions):
                                    _known_subscriptions.append(_new_sub)
                                # Envia subscrição ao WS
                                await ws.send(json.dumps({
                                    "assets_ids":             _aids,
                                    "type":                   "market",
                                    "custom_feature_enabled": True
                                }))
                                log_info(
                                    f"WS NOVA SUB | slug={_slug} "
                                    f"| tokens=[{_aids[0][:12]}..., {_aids[1][:12]}...]"
                                )
                            except asyncio.QueueEmpty:
                                break

                        # ── Processa mensagem WS ─────────────────────────────
                        items = json.loads(raw)
                        if not isinstance(items, list):
                            items = [items]
                        updated = False

                        for item in items:
                            evt = item.get("event_type")

                            # ── market_resolved (Área 4 — Real-Time Settlement) ──
                            if evt == "market_resolved":
                                wa = item.get("winning_asset_id")
                                if wa:
                                    # Mantém globais para compatibilidade retroactiva
                                    resolved_winner_asset = wa
                                    resolved_event.set()
                                    log_info(
                                        f"RESOLUCAO WS | winning_asset_id={wa[:16]}... "
                                        f"| outcome={item.get('winning_outcome','?')}"
                                    )
                                    # Routing per-market via _token_to_slug (Área 4)
                                    _slug_for_winner = _token_to_slug.get(wa)
                                    if _slug_for_winner and _slug_for_winner in _per_market_resolved:
                                        _mrd = _per_market_resolved[_slug_for_winner]
                                        _mrd["winner"] = wa
                                        _mrd["event"].set()
                                        log_info(
                                            f"RESOLUCAO ROUTING | slug={_slug_for_winner} "
                                            f"| _per_market_resolved sinalizado"
                                        )
                                    else:
                                        log_warn(
                                            f"RESOLUCAO ROUTING | winner token={wa[:16]}... "
                                            f"nao encontrado em _token_to_slug — "
                                            f"resolved_event global sinalizado (fallback)"
                                        )
                                continue

                            aid = item.get("asset_id")
                            sk  = _tid_map.get(aid)
                            if sk is None:
                                continue

                            bid_p = ask_p = None

                            if evt == "book":
                                # Captura price E size do best bid/ask para OBI.
                                # v1.8.0: spread calculado inline (sem REST fetch_spread_sdk).
                                bids_r = item.get("bids", [])
                                asks_r = item.get("asks", [])
                                if bids_r:
                                    best_b_entry = None
                                    best_b_price = -1.0
                                    for d in bids_r:
                                        sz = float(d.get("size", 0))
                                        if sz <= 0:
                                            continue
                                        pr = float(d["price"])
                                        if pr > best_b_price:
                                            best_b_price = pr
                                            best_b_entry = d
                                    if best_b_entry is not None:
                                        bid_p        = best_b_price
                                        _bsizes[sk]  = float(best_b_entry.get("size", 0))
                                if asks_r:
                                    best_a_entry = None
                                    best_a_price = float("inf")
                                    for d in asks_r:
                                        sz = float(d.get("size", 0))
                                        if sz <= 0:
                                            continue
                                        pr = float(d["price"])
                                        if pr < best_a_price:
                                            best_a_price = pr
                                            best_a_entry = d
                                    if best_a_entry is not None:
                                        ask_p        = best_a_price
                                        _asizes[sk]  = float(best_a_entry.get("size", 0))
                                # v1.8.0: spread inline — zero latência REST (Área 1)
                                if bid_p is not None and ask_p is not None:
                                    _sprc[sk] = (ask_p - bid_p) * 100.0

                            elif evt == "best_bid_ask":
                                # Sem sizes — mantemos os ultimos valores do evento "book"
                                bb = item.get("best_bid")
                                ba = item.get("best_ask")
                                if bb: bid_p = float(bb)
                                if ba: ask_p = float(ba)
                                # v1.8.0: spread inline (Área 1)
                                sp_raw = item.get("spread")
                                if sp_raw is not None:
                                    _sprc[sk] = float(sp_raw) * 100.0
                                elif bid_p is not None and ask_p is not None:
                                    _sprc[sk] = (ask_p - bid_p) * 100.0

                            elif evt == "price_change":
                                # Sem sizes — mantemos os ultimos valores do evento "book"
                                pcs = item.get("price_changes", [])
                                if pcs:
                                    bb = pcs[-1].get("best_bid")
                                    ba = pcs[-1].get("best_ask")
                                    if bb: bid_p = float(bb)
                                    if ba: ask_p = float(ba)
                                # v1.8.0: spread inline (Área 1) — sem REST
                                if bid_p is not None and ask_p is not None:
                                    _sprc[sk] = (ask_p - bid_p) * 100.0

                            if bid_p is not None:
                                _bids[sk] = bid_p
                                updated   = True
                            if ask_p is not None:
                                _asks[sk] = ask_p
                                updated   = True

                        if updated:
                            _set()

                except asyncio.CancelledError:
                    _hb_task.cancel()
                    try:
                        await _hb_task
                    except asyncio.CancelledError:
                        pass
                    raise
                finally:
                    # Garante cancelamento do heartbeat em qualquer saída do bloco
                    if not _hb_task.done():
                        _hb_task.cancel()
                        try:
                            await _hb_task
                        except asyncio.CancelledError:
                            pass

        except asyncio.CancelledError:
            break
        except Exception as e:
            # Backoff exponencial: 1s → 2s → 4s → 8s → 16s (máx)
            log_warn(f"WS erro: {e} — reconectando em {_ws_backoff:.1f}s")
            # v1.8.0: sinaliza necessidade de reload de cache após backoff longo (Área 6)
            if _ws_backoff >= WS_CACHE_RELOAD_BACKOFF:
                _ws_needs_cache_reload = True
                log_warn(
                    f"WS BACKOFF LONGO ({_ws_backoff:.1f}s >= {WS_CACHE_RELOAD_BACKOFF:.1f}s) "
                    f"— _ws_needs_cache_reload=True"
                )
            await asyncio.sleep(_ws_backoff)
            _ws_backoff = min(_ws_backoff * 2.0, WS_RECONNECT_MAX_S)

# =============================================================================
# LIVE ORDER (com rate_limiter + retry + circuit_breaker)
# =============================================================================

async def place_live_order(side: str, ask: float, shares: float, token_id: str) -> bool:
    """
    Ordem limite BUY ao ASK via SDK.
    v1.7.0: fee_rate_bps via fetch_fee_rate_bps (já tem rate_limiter + retry).
            Protegido por circuit_breaker — não tenta se API em falha.
            Log detalhado de orderID em sucesso; falha não crasheia o bot.
    """
    if not clob_client:
        return False
    if api_circuit_breaker.is_open():
        log_warn(f"place_live_order | CircuitBreaker OPEN — ordem {side} cancelada")
        return False
    try:
        fee_bps    = await fetch_fee_rate_bps(token_id)
        await rate_limiter.acquire()
        order_args = OrderArgs(
            token_id=token_id,
            price=round(ask, 4),
            size=round(shares, 6),
            side=SDK_BUY,
            order_type="GTC",
            fee_rate_bps=fee_bps
        )
        resp = clob_client.create_and_post_order(order_args)
        order_id = resp.get("orderID", resp.get("id", "OK"))
        log_info(
            f"LIVE ORDER OK | {side} {token_id[:12]}... @ ASK={ask:.4f} "
            f"| shares={shares:.4f} | fee_bps={fee_bps} | orderID={order_id}"
        )
        api_circuit_breaker.record_success()
        return True
    except Exception as e:
        api_circuit_breaker.record_failure()
        log_warn(f"LIVE ORDER falhou ({side}): {e}")
        return False

# =============================================================================
# FILTRO DE KALMAN 1D — Suavizacao do Preco Real
# =============================================================================

class KalmanFilter1D:
    """
    Filtro de Kalman unidimensional leve para suavizacao de preco.

    Modelo:
      Estado:      x_k = preco real estimado (variavel latente)
      Transicao:   x_k = x_{k-1} + ruido_processo  (random walk)
      Observacao:  z_k = x_k    + ruido_medicao

    Parametros:
      Q = KALMAN_PROCESS_NOISE (1e-5): variancia do ruido de processo.
          Menor Q -> estado muda mais devagar (mais inercia/suavizacao).
          Maior Q -> estado segue as observacoes mais rapidamente.
      R = KALMAN_MEASURE_NOISE (1e-2): variancia do ruido de medicao.
          Maior R -> mercado e mais ruidoso (mais suavizacao aplicada).
          Menor R -> confia mais nas observacoes (menos suavizacao).

    Algoritmo:
      Predicao:   x_pred = x_{k-1}
                  P_pred = P_{k-1} + Q
      Actualizacao: K = P_pred / (P_pred + R)   (ganho Kalman)
                    x_k = x_pred + K * (z_k - x_pred)
                    P_k = (1 - K) * P_pred

    Uso:
      kalman = KalmanFilter1D(Q, R)
      preco_real = kalman.update(mid_price_ruidoso)
      kalman.reset()  # no inicio de cada ciclo de 5min

    Integrado em HFTWindow: os precos Kalman alimentam a janela de Z-Score,
    tornando o Z-Score imune a wicks e spikes de liquidez.
    """
    __slots__ = ("q", "r", "x", "p")

    def __init__(self, q: float = 1e-5, r: float = 1e-2):
        self.q: float        = q
        self.r: float        = r
        self.x: float | None = None
        self.p: float        = 1.0

    def update(self, z: float) -> float:
        """
        Actualiza o filtro com nova observacao z.
        Retorna o preco suavizado estimado.
        Primeiro passo: inicializa x=z (sem historico anterior).
        """
        if self.x is None:
            self.x = z
            return z
        x_pred = self.x
        p_pred = self.p + self.q
        k      = p_pred / (p_pred + self.r)
        self.x = x_pred + k * (z - x_pred)
        self.p = (1.0 - k) * p_pred
        return self.x

    def reset(self):
        """Reinicia o filtro — chamado no inicio de cada ciclo de mercado."""
        self.x = None
        self.p = 1.0

# =============================================================================
# HFT WINDOW — Janela Deslizante para Z-Score e Regime
# =============================================================================

class HFTWindow:
    """
    Janela deslizante temporal de HFT_WINDOW_SECONDS segundos.

    Armazena (timestamp, preco_Kalman) e expira registos antigos automaticamente.

    Metodos:
      .add(price, ts)    -> adiciona ponto, expira >10s
      .zscore(price)     -> Z-Score do preco actual vs. media da janela
      .std()             -> desvio-padrao dos precos na janela (regime)
      .size()            -> numero de pontos na janela

    Z-Score = (preco_actual - media_janela) / desvio_padrao_janela
      Requer minimo 3 pontos; retorna None se janela insuficiente.
      desvio_padrao=0 -> retorna 0.0 (preco completamente plano).

    Uso no GAMBLING:
      std <= GAMB_MAX_VOL_DEV: regime comprimido (cond 1)
      Z   <= GAMB_MAX_ZSCORE:  nao em pico (cond 2)

    Uso no TP DINAMICO:
      Z   >= TP_SPIKE_ZSCORE:  wick absurdo para cima -> vender (se lucrativo)

    Uso no STOP-LOSS:
      Z   <= SL_CRASH_ZSCORE:  crash violento (-5 StdDev) -> vender
    """
    __slots__ = ("window_s", "data")

    def __init__(self, window_s: float = 10.0):
        self.window_s: float = window_s
        self.data: deque     = deque()

    def add(self, price: float, ts: float):
        """Adiciona ponto e expira registos mais antigos que window_s."""
        self.data.append((ts, price))
        cutoff = ts - self.window_s
        buf    = self.data
        while buf and buf[0][0] < cutoff:
            buf.popleft()

    def _stats(self):
        """Retorna (mean, std, n) dos precos na janela. Interno."""
        n = len(self.data)
        if n < 3:
            return None, None, n
        prices = [p for _, p in self.data]
        mean   = sum(prices) / n
        var    = sum((p - mean) ** 2 for p in prices) / n
        return mean, math.sqrt(var), n

    def zscore(self, current_price: float) -> float | None:
        """
        Z-Score do preco actual em relacao a janela Kalman.
        None se < 3 pontos. 0.0 se desvio-padrao ~ zero.
        """
        mean, std, n = self._stats()
        if mean is None:
            return None
        if std < 1e-9:
            return 0.0
        return (current_price - mean) / std

    def std(self) -> float | None:
        """
        Desvio-padrao dos precos na janela (indicador de volatilidade/regime).
        None se < 3 pontos.
        """
        _, s, n = self._stats()
        return s

    def size(self) -> int:
        """Numero de pontos na janela actual."""
        return len(self.data)

    def clear(self):
        """Limpa a janela — chamado no inicio de cada ciclo de mercado."""
        self.data.clear()

# =============================================================================
# VPIN TRACKER — Order Flow Toxicity
# =============================================================================

class VPINTracker:
    """
    Aproximacao leve de VPIN (Volume-synchronized Probability of Informed Trading).

    Metodologia de classificacao de ticks (Lee-Ready simplificado):
      - Tick UP (mid Kalman subiu vs. tick anterior): classificado como BUY.
      - Tick DOWN (mid Kalman desceu):                classificado como SELL.
      - Tick neutro (mid igual):                      nao classificado.

    Volume proxy por tick:
      - Disponivel: bid_size + ask_size (do evento book do WS).
      - Indisponivel: 1.0 (peso unitario — sem distorcao).

    VPIN = |buy_vol - sell_vol| / (buy_vol + sell_vol) sobre janela de 10s.

    Range: 0.0 (fluxo 100% equilibrado) a 1.0 (fluxo 100% unilateral).

    Interpretacao:
      0.0 - 0.30: fluxo normal, equilibrado — mercado organico.
      0.30 - 0.40: alguma pressao direcional — monitorizar.
      >= VPIN_SAFE_LIMIT (0.40): desequilibrio significativo — bloqueia gambling.
      >= SL_TOXIC_VPIN   (0.97): dump/pump institucional — SL imediato.

    Uso:
      vpin = VPINTracker(window_s=10.0)
      vpin.add(kal_price, total_size, now)
      toxicity = vpin.vpin()  # None se sem dados
      vpin.reset()  # no inicio de cada ciclo
    """
    __slots__ = ("window_s", "data", "prev_mid")

    def __init__(self, window_s: float = 10.0):
        self.window_s: float      = window_s
        self.data:     deque      = deque()
        self.prev_mid: float | None = None

    def add(self, kal_mid: float, total_size: float, ts: float):
        """
        Adiciona tick. Classifica como BUY ou SELL por comparacao com tick anterior.
        signed_volume > 0 = buy-initiated, < 0 = sell-initiated.
        Expira automaticamente registos mais antigos que window_s.
        """
        if self.prev_mid is not None and total_size > 1e-9:
            if kal_mid > self.prev_mid:
                self.data.append((ts,  total_size))
            elif kal_mid < self.prev_mid:
                self.data.append((ts, -total_size))
        self.prev_mid = kal_mid
        cutoff = ts - self.window_s
        buf    = self.data
        while buf and buf[0][0] < cutoff:
            buf.popleft()

    def vpin(self) -> float | None:
        """
        Calcula VPIN sobre a janela actual.
        None se sem dados (primeiro tick ou janela vazia).
        """
        if not self.data:
            return None
        buy_vol  = sum( v for _, v in self.data if v > 0)
        sell_vol = sum(-v for _, v in self.data if v < 0)
        total    = buy_vol + sell_vol
        if total < 1e-9:
            return None
        return abs(buy_vol - sell_vol) / total

    def reset(self):
        """Reinicia o tracker — chamado no inicio de cada ciclo de mercado."""
        self.data.clear()
        self.prev_mid = None

# =============================================================================
# LOGIC LOOP (v1.8.0 — Áreas 2, 4, 5)
# =============================================================================

async def logic_loop(m_start: float, m_end: float, meta: dict,
                     ws_sub_queue: asyncio.Queue):
    """
    Loop principal de trading para um ciclo de 5 minutos.

    v1.8.0 — Modificações (3 áreas):

    [2] Overlapping Subscriptions (Área 2):
        Quando rem <= WS_OVERLAP_PRE_S (20s), obtém metadados do próximo
        mercado do _metadata_cache e envia subscrição ao ws_sub_queue.
        Garante que Market B já tem preços quentes quando lógica começa.
        _overlap_subscribed flag previne subscrição dupla.

    [4] Real-Time Settlement (Área 4):
        Usa _per_market_resolved[slug]["event"] para aguardar resolução WS real.
        ELIMINADO: bloco asyncio.TimeoutError com estimativa por BID.
        Se ultrapassar RESOLVE_TIMEOUT_S: loga WARN mas NÃO fecha trades
        com payout estimado — aguarda indefinidamente pela mensagem real.
        PnL só registado quando market_resolved WS chega com winner real.

    [5] Non-Blocking PnL Accounting (Área 5):
        Contabilização de PnL movida para o interior de logic_loop (era em main).
        main() usa asyncio.create_task(logic_loop(...)) — não-bloqueante.
        Cada task calcula e loga o seu próprio PnL ao terminar.

    LEGADO (mantido de v1.7.0):
        BUY ao ASK: docs 'you will pay the ask when buying'
        SELL ao BID: docs 'receive the bid when selling'
        RESOLUCAO: winning tokens => $1/share, losing => $0

        PEG ARBIT: ask_sum = ask_up + ask_down <= PA_TRIGGER_SUM
                   Compra simultanea via asyncio.gather. Risco fixo: PEG_ARBIT_RISK.

        GAMBLING — Motor Quantitativo HFT (v1.7.0):
                   Filtro BID_ASK_MIN_RATIO adicional antes das 4 condições.
                   4 condições: σ<=0.03 / Z<=1.3 / OBI>=75% / VPIN<=0.40.
                   Kelly sem Martingale.

        ENDGAME OVERRIDE (rem<=30.999s):
                   Cond 2: Z_limit->99.0 / Cond 4: VPIN_limit->0.60.

        TAKE-PROFIT DINÂMICO v1.7.0 — FIX CRÍTICO:
                   PORTÃO 1: bid >= trade["ask"] + TP_MIN_BID_OVER_ASK (2c mínimo)
                   PORTÃO 2: sell_payout_net(shares, bid) > total_out*(1+TP_MIN_PROFIT_PCT)
                   SÓ DEPOIS: Z >= TP_SPIKE_ZSCORE (4.5).
                   Log SKIP_UNPROFITABLE quando portões bloqueiam.

        STOP-LOSS (OR logic — v1.7.0):
                   BID <= SL_BASE_TRIGGER (0.25) E qualquer trigger:
                   A. VPIN >= SL_TOXIC_VPIN   (0.97 — dump institucional)
                   B. Z   <= SL_CRASH_ZSCORE  (-5.0 — crash Kalman)
                   C. OBI <= SL_PANIC_OBI     (0.02 — colapso real)

    Parâmetros:
      m_start       : timestamp Unix de início do ciclo
      m_end         : timestamp Unix de fim do ciclo (m_start + 300)
      meta          : dict com "slug", "up" (token_id), "down" (token_id), "id"
      ws_sub_queue  : asyncio.Queue para subscrições dinâmicas (overlap — Área 2)
    """
    global bankroll, daily_profit, total_pnl_pos, total_pnl_neg

    active_trades = []

    # v1.8.0: track pre-cycle bankroll aqui (era em main — Área 5)
    pre_bank = bankroll

    # Risco PA (fixo)
    eff_pa_risk = min(PEG_ARBIT_RISK, MAX_RISK_PERCENT)

    # v1.8.0: Regista per-market resolution entry (Área 4)
    _slug = meta["slug"]
    _per_market_resolved[_slug] = {"event": asyncio.Event(), "winner": None}

    # v1.8.0: Regista tokens no mapa de routing (Área 4)
    _token_to_slug[meta["up"]]   = _slug
    _token_to_slug[meta["down"]] = _slug

    # v1.8.0: flag de overlap para não subscrever próximo mercado mais que uma vez
    _overlap_subscribed = False

    # Header de ronda
    mods = []
    if PEG_ARBIT_ACTIVE:
        mods.append(f"PEG_ARBIT(<={PA_TRIGGER_SUM:.3f})")
    if GAMBLING_ACTIVE:
        mods.append(
            f"GAMBLING(HFT4:σ<={GAMB_MAX_VOL_DEV:.2f}"
            f"/Z<={GAMB_MAX_ZSCORE}"
            f"/OBI>={GAMB_MIN_OBI:.0%}"
            f"/VPIN<={VPIN_SAFE_LIMIT:.0%}"
            f"/BidAsk>={BID_ASK_MIN_RATIO:.0%}"
            f")"
        )
    if TAKE_PROFIT_ACTIVE:
        mods.append(
            f"TP_DIN(Z>={TP_SPIKE_ZSCORE}"
            f"+bid>ask+{TP_MIN_BID_OVER_ASK*100:.0f}c"
            f"+pnl>{TP_MIN_PROFIT_PCT:.1%})"
        )
    if STOP_LOSS_ACTIVE:
        mods.append(
            f"SL_HFT(<={SL_BASE_TRIGGER:.2f}+OR:"
            f"VPIN>={SL_TOXIC_VPIN}/Z<={SL_CRASH_ZSCORE}/OBI<={SL_PANIC_OBI})"
        )
    mods.append(f"ENDGAME(<=30.999s:Z→{ENDGAME_ZSCORE_LIMIT:.0f}/VPIN→{ENDGAME_VPIN_LIMIT:.2f})")
    mods.append(f"OVERLAP(sub_B@rem<={WS_OVERLAP_PRE_S}s)")

    log_sep2()
    log_info(f"NOVO CICLO | {meta['slug']} | LIVE={LIVE_TRADING}")
    log_info(f"Banca: ${bankroll:.4f} | Profit dia: ${daily_profit:+.4f}")
    log_info(f"Modulos: {' | '.join(mods) if mods else 'nenhum'}")
    log_info(
        f"PA risk={eff_pa_risk:.1%}(fixo) | "
        f"GAMBLING: Kelly(edge={KELLY_ASSUMED_EDGE:.0%} "
        f"frac=1/{int(round(1/KELLY_FRACTION))} cap={KELLY_MAX_RISK_PCT:.0%})"
    )
    log_info(
        f"HFT: Kalman(Q={KALMAN_PROCESS_NOISE:.0e} R={KALMAN_MEASURE_NOISE:.0e}) "
        f"Window={HFT_WINDOW_SECONDS}s"
    )
    log_info(
        f"TP v1.7.0: Z>={TP_SPIKE_ZSCORE} + BID>=ASK+{TP_MIN_BID_OVER_ASK*100:.0f}c "
        f"+ payout>{TP_MIN_PROFIT_PCT:.1%} (portao duplo de lucratividade)"
    )
    log_info(
        f"WS v1.8.0: Overlap@rem<={WS_OVERLAP_PRE_S}s | "
        f"Settlement=ONLY_WS_market_resolved | Non-blocking=create_task"
    )
    log_sep()
    log_info("ESCUTA ACTIVA")
    log_sep()

    # =========================================================================
    # OPEN TRADE — BUY ao ASK
    # docs: 'you will pay the ask when buying'
    # invested_pure = bankroll * risk
    # shares        = invested_pure / ask
    # fee_buy       = fee_rate(ask) * invested_pure  [cobrada em shares]
    # total_out     = invested_pure + fee_buy
    # =========================================================================
    async def open_trade(side, trade_type, rstr, risk,
                         extra_log=None, fixed_shares=None, token_id=None):
        global bankroll
        ask = best_asks.get(side.lower())
        bid = best_bids.get(side.lower())
        if ask is None or ask <= 0.0:
            log_warn(f"open_trade | {side} ASK invalido ({ask}) — cancelado")
            return None
        if fixed_shares is not None:
            shares        = fixed_shares
            invested_pure = shares * ask
        else:
            invested_pure = bankroll * risk
            shares        = invested_pure / ask
        fee_buy   = fee_rate(ask) * invested_pure
        total_out = invested_pure + fee_buy
        eff_c_val = eff_price_c(ask)
        target = None
        if trade_type == "PEG ARBIT" and PA_TARGET_BID_C > 0.0:
            target = PA_TARGET_BID_C / 100.0
        elif trade_type == "GAMBLING" and GAMB_TARGET_BID_C > 0.0:
            target = GAMB_TARGET_BID_C / 100.0
        bankroll -= total_out
        trade = {
            "side":          side,
            "ask":           ask,
            "bid_at_buy":    bid,
            "eff_c":         eff_c_val,
            "shares":        shares,
            "target":        target,
            "type":          trade_type,
            "invested_pure": invested_pure,
            "fee_buy":       fee_buy,
            "total_out":     total_out,
            "token_id":      token_id
        }
        active_trades.append(trade)
        if LIVE_TRADING and token_id:
            await place_live_order(side, ask, shares, token_id)
        bid_s = f" | BID@buy={fc(bid)}" if bid else ""
        ext_s = f" | {extra_log}" if extra_log else ""
        log_m(trade_type, "BUY",
            f"rem={rstr} | {side} @ ASK={fc(ask)} eff={fc(eff_c_val/100)}"
            f"{bid_s}"
            f" | invested=${invested_pure:.4f} | fee=${fee_buy:.4f} | total=${total_out:.4f}"
            f" | shares={shares:.4f} | risk={risk:.1%}{ext_s}"
        )
        return trade

    # =========================================================================
    # CLOSE TRADE — SELL ao BID
    # docs: 'you will receive the bid when selling'
    # =========================================================================
    def close_trade(trade, sell_bid, reason, rstr):
        global bankroll
        payout_bruto = trade["shares"] * sell_bid
        fee_sell     = payout_bruto * fee_rate(sell_bid)
        payout_net   = payout_bruto - fee_sell
        pnl          = payout_net - trade["total_out"]
        pnl_pct      = (pnl / trade["total_out"] * 100.0) if trade["total_out"] else 0.0
        bankroll    += payout_net
        sign         = "(+)" if pnl >= 0 else "(-)"
        log_m(trade["type"], "SELL",
            f"rem={rstr} | {trade['side']} @ BID={fc(sell_bid)} "
            f"| bruto=${payout_bruto:.4f} | fee_sell=${fee_sell:.4f} | net=${payout_net:.4f} "
            f"| PnL: ${pnl:+.4f} ({pnl_pct:+.2f}%) {sign} | Reason: {reason}"
        )
        return pnl

    # =========================================================================
    # CLOSE TRADE RESOLUTION
    # Winning tokens => $1/share (fee_rate(1.0)=0); Losing => $0
    # v1.8.0: ÚNICA via de liquidação — só chamada com winner real do WS.
    # =========================================================================
    def close_trade_resolution(trade, winner, rstr):
        global bankroll
        shares     = trade["shares"]
        payout_net = resolution_payout(shares, winner)
        pnl        = payout_net - trade["total_out"]
        pnl_pct    = (pnl / trade["total_out"] * 100.0) if trade["total_out"] else 0.0
        bankroll  += payout_net
        reason_s   = "RESOLUCAO GANHA ($1/share)" if winner else "RESOLUCAO PERDIDA (Total)"
        price_s    = "100.0c"                      if winner else "0.0c"
        sign       = "(+)" if pnl >= 0 else "(-)"
        if LIVE_TRADING and winner and trade.get("token_id"):
            redeem_live_position(shares, trade["token_id"])
        log_m(trade["type"], "SELL",
            f"rem={rstr} | {trade['side']} @ {price_s} "
            f"| net=${payout_net:.4f} "
            f"| PnL: ${pnl:+.4f} ({pnl_pct:+.2f}%) {sign} | Reason: {reason_s}"
        )
        return pnl

    # =========================================================================
    # ESTADO QUANTITATIVO — Kalman + HFTWindow + VPINTracker por lado
    # =========================================================================
    kalmans = {
        "UP":   KalmanFilter1D(KALMAN_PROCESS_NOISE, KALMAN_MEASURE_NOISE),
        "DOWN": KalmanFilter1D(KALMAN_PROCESS_NOISE, KALMAN_MEASURE_NOISE)
    }
    hft_wins = {
        "UP":   HFTWindow(HFT_WINDOW_SECONDS),
        "DOWN": HFTWindow(HFT_WINDOW_SECONDS)
    }
    vpin_trackers = {
        "UP":   VPINTracker(HFT_WINDOW_SECONDS),
        "DOWN": VPINTracker(HFT_WINDOW_SECONDS)
    }

    # Gambling state
    gamb_last_buy       = {"UP": 0.0, "DOWN": 0.0}
    gamb_cutoff_logged  = False
    gamb_started_logged = False

    pa_count      = 0
    last_pa_time  = 0.0
    prev_bid_up   = prev_bid_down = None

    # =========================================================================
    # LOOP PRINCIPAL
    # =========================================================================
    while not _shutdown_flag:
        now = time.time()
        rem = m_end - now

        # ── Fim de mercado ───────────────────────────────────────────────────
        if rem <= 0.0:
            final_bid_up   = best_bids.get("up")  or 0.0
            final_bid_down = best_bids.get("down") or 0.0
            log_sep()
            log_info(
                f"FIM DE MERCADO | UP final={fc(final_bid_up)} "
                f"| DOWN final={fc(final_bid_down)}"
            )
            if active_trades:
                # v1.8.0: Real-Time Settlement — aguarda market_resolved WS real (Área 4).
                # ELIMINADO: fallback de estimativa por BID (presente em v1.7.x).
                # Se ultrapassar RESOLVE_TIMEOUT_S: aviso WARN mas continua a aguardar.
                # PnL só logado quando winning_asset_id real chega do WS.
                log_info(
                    f"Aguardando resolucao WS real (sem fallback BID — v1.8.0) | "
                    f"slug={_slug} | warn_threshold={RESOLVE_TIMEOUT_S:.0f}s"
                )
                _mrd = _per_market_resolved.get(_slug)
                _resolution_start = time.time()
                _warned            = False

                # Aguarda evento per-market ou global (whichever arrives first)
                while True:
                    # Verifica per-market event primeiro (routing preciso — Área 4)
                    if _mrd and _mrd["event"].is_set():
                        winner_asset = _mrd["winner"]
                        break
                    # Fallback: resolved_event global (retrocompatibilidade)
                    if resolved_event.is_set():
                        winner_asset = resolved_winner_asset
                        # Verifica se é o nosso mercado
                        if winner_asset in (meta["up"], meta["down"]):
                            break
                        else:
                            # É de outro mercado concorrente — limpa e continua
                            resolved_event.clear()
                    # Threshold de aviso (não de fallback)
                    elapsed = time.time() - _resolution_start
                    if elapsed >= RESOLVE_TIMEOUT_S and not _warned:
                        _warned = True
                        log_warn(
                            f"RESOLUCAO LENTA | {elapsed:.0f}s sem market_resolved WS "
                            f"para slug={_slug} — AGUARDANDO (sem fallback BID)"
                        )
                    # Yield ao event loop brevemente
                    await asyncio.sleep(0.05)

                log_info(
                    f"RESOLUCAO CONFIRMADA | winner_asset="
                    f"{winner_asset[:16] if winner_asset else '?'}..."
                )
                for trade in active_trades[:]:
                    winner = (trade.get("token_id") == winner_asset)
                    close_trade_resolution(trade, winner, "00:00:000")
                    active_trades.remove(trade)
            break

        # ── v1.8.0: Overlapping Subscription — Área 2 ───────────────────────
        # 20s antes de expirar, subscreve tokens do próximo mercado no mesmo WS.
        # Garante que Market B tem preços quentes desde o início do seu ciclo.
        if not _overlap_subscribed and rem <= WS_OVERLAP_PRE_S:
            _overlap_subscribed = True
            # Calcula slug do próximo mercado
            _next_start_ts = m_end  # O fim de A é o início de B
            _next_slug     = f"xrp-updown-5m-{int(_next_start_ts)}"
            # Consulta cache primeiro (Área 3); fallback para fetch_metadata
            _next_meta     = _metadata_cache.get(_next_slug)
            if _next_meta is None:
                log_info(
                    f"OVERLAP | {_next_slug} ausente do cache — "
                    f"fetch_metadata assíncrono"
                )
                # asyncio.create_task para não bloquear o loop de trading
                async def _fetch_and_subscribe(_ns=_next_slug):
                    _nm = await fetch_metadata(_ns)
                    if _nm:
                        _metadata_cache[_ns] = _nm
                        await ws_sub_queue.put({
                            "assets_ids": [_nm["up"], _nm["down"]],
                            "slug":       _ns
                        })
                        log_info(
                            f"OVERLAP | {_ns} | sub enviada "
                            f"(rem={get_remaining_str(m_end - time.time())})"
                        )
                    else:
                        log_warn(f"OVERLAP | {_ns} | fetch_metadata falhou — sem sub overlap")
                asyncio.create_task(_fetch_and_subscribe())
            else:
                await ws_sub_queue.put({
                    "assets_ids": [_next_meta["up"], _next_meta["down"]],
                    "slug":       _next_slug
                })
                log_info(
                    f"OVERLAP | {_next_slug} | sub enviada via cache "
                    f"(rem={get_remaining_str(rem)}) | "
                    f"up={_next_meta['up'][:12]}..."
                )

        # ── Aguarda tick WS ──────────────────────────────────────────────────
        try:
            await asyncio.wait_for(price_change.wait(), timeout=LOOP_SLEEP)
            price_change.clear()
        except asyncio.TimeoutError:
            pass

        bid_up   = best_bids.get("up")
        bid_down = best_bids.get("down")
        ask_up   = best_asks.get("up")
        ask_down = best_asks.get("down")

        if bid_up is None or bid_down is None or ask_up is None or ask_down is None:
            continue
        if bid_up == prev_bid_up and bid_down == prev_bid_down:
            continue
        prev_bid_up   = bid_up
        prev_bid_down = bid_down

        ask_sum    = ask_up + ask_down
        bid_sum    = bid_up + bid_down
        underpeg_c = (1.0 - ask_sum) * 100.0
        mid_up     = (bid_up   + ask_up)   * 0.5
        mid_down   = (bid_down + ask_down) * 0.5
        eff_up     = eff_price_c(ask_up)
        eff_down   = eff_price_c(ask_down)

        # ── Motor Quantitativo — actualiza Kalman + HFTWindow + VPIN ─────────
        # A cada tick, para cada lado:
        #   1. Kalman suaviza o mid_price (remove wicks/ruido)
        #   2. HFTWindow recebe o preco Kalman (janela 10s para Z e StdDev)
        #   3. VPIN recebe preco Kalman + volume (janela 10s para toxicidade)
        #
        kal_up   = kalmans["UP"].update(mid_up)
        kal_down = kalmans["DOWN"].update(mid_down)
        hft_wins["UP"].add(kal_up,   now)
        hft_wins["DOWN"].add(kal_down, now)

        z_up    = hft_wins["UP"].zscore(kal_up)
        z_down  = hft_wins["DOWN"].zscore(kal_down)
        std_up  = hft_wins["UP"].std()
        std_down = hft_wins["DOWN"].std()

        bs_up   = best_bid_sizes.get("up")
        as_up   = best_ask_sizes.get("up")
        bs_down = best_bid_sizes.get("down")
        as_down = best_ask_sizes.get("down")
        obi_up   = calc_imbalance(bs_up,   as_up)
        obi_down = calc_imbalance(bs_down, as_down)

        # Volume total por lado para VPIN (fallback 1.0 se sizes indisponiveis)
        vol_up   = ((bs_up   or 0) + (as_up   or 0)) or 1.0
        vol_down = ((bs_down or 0) + (as_down or 0)) or 1.0
        vpin_trackers["UP"].add(kal_up,   vol_up,   now)
        vpin_trackers["DOWN"].add(kal_down, vol_down, now)
        vpin_up   = vpin_trackers["UP"].vpin()
        vpin_down = vpin_trackers["DOWN"].vpin()


        rstr = get_remaining_str(rem)

        # ── Tick log com snapshot quantitativo ───────────────────────────────
        peg_str = f" | PEG={ask_sum:.4f}"
        if ask_sum <= PA_TRIGGER_SUM:
            peg_str += f" underpeg={underpeg_c:.1f}c"
        _z_u    = f"{z_up:+.2f}"    if z_up    is not None else "n/a"
        _z_d    = f"{z_down:+.2f}"  if z_down  is not None else "n/a"
        _s_u    = f"{std_up:.4f}"   if std_up  is not None else "n/a"
        _s_d    = f"{std_down:.4f}" if std_down is not None else "n/a"
        _o_u    = f"{obi_up:.2f}"   if obi_up  is not None else "n/a"
        _o_d    = f"{obi_down:.2f}" if obi_down is not None else "n/a"
        _v_u    = f"{vpin_up:.2f}"  if vpin_up  is not None else "n/a"
        _v_d    = f"{vpin_down:.2f}"if vpin_down is not None else "n/a"
        log_raw(
            f"rem={rstr} | "
            f"UP  BID={fc(bid_up)} ASK={fc(ask_up)} "
            f"KAL={fc(kal_up)} Z={_z_u} σ={_s_u} OBI={_o_u} VPIN={_v_u} | "
            f"DN  BID={fc(bid_down)} ASK={fc(ask_down)} "
            f"KAL={fc(kal_down)} Z={_z_d} σ={_s_d} OBI={_o_d} VPIN={_v_d}"
            f"{peg_str}"
        )

        # =====================================================================
        # STOP-LOSS — OR logic inline por tick (v1.5.0 / v1.7.0)
        #
        # Arquitectura OR vs. AND:
        #   v1.4.0: precisava de Z + Imbalance em simultâneo (AND).
        #   v1.5.0+: basta BID <= threshold + QUALQUER trigger (A, B ou C).
        #
        # v1.7.0 ajustes anti-pânico:
        #   SL_CRASH_ZSCORE : -4.0 -> -5.0 (mais tolerância a quedas abruptas)
        #   SL_TOXIC_VPIN   : 0.95 -> 0.97 (mais tolerância antes de dump confirmado)
        #   SL_BASE_TRIGGER : 0.25          (mantido)
        #   SL_PANIC_OBI    : 0.02          (mantido — colapso real a 2% compradores)
        #
        # Trigger A — VPIN >= SL_TOXIC_VPIN (0.97): dump institucional confirmado.
        # Trigger B — Z   <= SL_CRASH_ZSCORE (-5.0): crash violento vs. Kalman.
        # Trigger C — OBI <= SL_PANIC_OBI   (0.02): compradores abandonaram o livro.
        # =====================================================================
        if STOP_LOSS_ACTIVE and active_trades:
            for _sl_side, _sl_bid, _sl_z, _sl_obi, _sl_vpin in (
                ("UP",   bid_up,   z_up,   obi_up,   vpin_up),
                ("DOWN", bid_down, z_down, obi_down, vpin_down),
            ):
                _g_trades = [t for t in active_trades
                             if t["type"] == "GAMBLING" and t["side"] == _sl_side]
                if not _g_trades:
                    continue

                if _sl_bid > SL_BASE_TRIGGER:
                    continue

                _sl_reason = None

                if _sl_vpin is not None and _sl_vpin >= SL_TOXIC_VPIN:
                    _sl_reason = (
                        f"TRIGGER A — VPIN={_sl_vpin:.2f}>={SL_TOXIC_VPIN:.2f} "
                        f"(dump institucional detectado)"
                    )
                elif _sl_z is not None and _sl_z <= SL_CRASH_ZSCORE:
                    _sl_reason = (
                        f"TRIGGER B — Z={_sl_z:+.2f}<={SL_CRASH_ZSCORE:.1f} "
                        f"(crash Kalman {abs(_sl_z):.1f}σ)"
                    )
                elif _sl_obi is not None and _sl_obi <= SL_PANIC_OBI:
                    _sl_reason = (
                        f"TRIGGER C — OBI={_sl_obi:.2f}<={SL_PANIC_OBI:.2f} "
                        f"(colapso real — apenas {SL_PANIC_OBI:.0%} compradores)"
                    )
                else:
                    _z_diag    = f"{_sl_z:+.2f}"    if _sl_z    is not None else "n/a"
                    _obi_diag  = f"{_sl_obi:.2f}"   if _sl_obi  is not None else "n/a"
                    _vpin_diag = f"{_sl_vpin:.2f}"  if _sl_vpin is not None else "n/a"
                    log_m("STOPLOSS", "WATCH",
                        f"rem={rstr} | {_sl_side} BID={fc(_sl_bid)}<={SL_BASE_TRIGGER:.2f} "
                        f"| Z={_z_diag}(B<={SL_CRASH_ZSCORE}) "
                        f"OBI={_obi_diag}(C<={SL_PANIC_OBI}) "
                        f"VPIN={_vpin_diag}(A>={SL_TOXIC_VPIN}) — sem trigger activo"
                    )
                    continue

                log_sep()
                log_m("STOPLOSS", "TRIGGER",
                    f"rem={rstr} | {_sl_side} STOP-LOSS HFT OR | "
                    f"BID={fc(_sl_bid)}<={SL_BASE_TRIGGER:.2f} | {_sl_reason}"
                )
                _closed = 0
                for _trade in list(_g_trades):
                    _sell_bid = best_bids.get(_trade["side"].lower()) or 0.0
                    close_trade(
                        _trade, _sell_bid,
                        f"SL HFT [{_sl_side}] {_sl_reason[:50]}",
                        rstr
                    )
                    active_trades.remove(_trade)
                    _closed += 1
                log_info(
                    f"STOP LOSS HFT | rem={rstr} | {_sl_side} "
                    f"| fechadas={_closed} pos GAMBLING | PEG ARBIT intacto | ciclo continua"
                )
                log_sep()

        # =====================================================================
        # TAKE-PROFIT DINÂMICO — Wick Capture (v1.7.0 — FIX CRÍTICO)
        #
        # v1.6.0/v1.5.0: disparava quando Z>=3.0, independentemente do preço.
        # Resultado: vendia a BID=62c quando ASK=65c → loss garantido.
        #
        # v1.7.0 — PORTÃO DUPLO antes de verificar Z:
        #
        #   PORTÃO 1 — BID mínimo sobre ASK de entrada:
        #     bid >= trade["ask"] + TP_MIN_BID_OVER_ASK (2c)
        #     Razão matemática: break-even BID = ASK + ~2c (fees de compra + venda).
        #     Ex: ASK=65c → break-even BID >= 67c. TP a 62c era perda garantida.
        #
        #   PORTÃO 2 — Lucratividade líquida real:
        #     sell_payout_net(shares, bid) > total_out * (1 + TP_MIN_PROFIT_PCT)
        #     Calcula o payout real após fees de venda e compara com custo total all-in.
        #     Ex: BID=67c, ASK=66c → net < total_out → bloqueado (fees comem o lucro).
        #
        #   SÓ DEPOIS DOS 2 PORTÕES: Z >= TP_SPIKE_ZSCORE (4.5)
        #     Z=4.5 significa wick de 4.5 desvios padrão — muito raro, muito real.
        #     3.0 (v1.5.0-v1.6.0) ocorria frequentemente em janelas de 10s.
        #
        # Log diferenciado:
        #   SKIP_UNPROFITABLE: Z>=threshold mas portões bloquearam.
        #   WICK: todos os portões passaram → venda executada.
        # =====================================================================
        if TAKE_PROFIT_ACTIVE and active_trades:
            for _tp_side, _tp_bid, _tp_z in (
                ("UP",   bid_up,   z_up),
                ("DOWN", bid_down, z_down),
            ):
                if _tp_z is None:
                    continue
                _tp_trades = [t for t in active_trades
                              if t["type"] == "GAMBLING" and t["side"] == _tp_side]
                if not _tp_trades:
                    continue

                for _tp_trade in list(_tp_trades):
                    _entry_ask    = _tp_trade["ask"]
                    _shares       = _tp_trade["shares"]
                    _total_out    = _tp_trade["total_out"]
                    _min_bid      = _entry_ask + TP_MIN_BID_OVER_ASK
                    _net_if_sell  = sell_payout_net(_shares, _tp_bid)
                    _min_net      = _total_out * (1.0 + TP_MIN_PROFIT_PCT)

                    # ── Portão 1: BID >= ASK_entrada + TP_MIN_BID_OVER_ASK ───
                    if _tp_bid < _min_bid:
                        if _tp_z >= TP_SPIKE_ZSCORE:
                            log_m("TP", "SKIP_UNPROFITABLE",
                                f"rem={rstr} | {_tp_side} Z={_tp_z:+.2f}>={TP_SPIKE_ZSCORE} "
                                f"MAS BID={fc(_tp_bid)}<ASK+{TP_MIN_BID_OVER_ASK*100:.0f}c"
                                f"={fc(_min_bid)} — portao 1 bloqueado (loss garantido)")
                        continue

                    # ── Portão 2: payout líquido > total_out * (1 + min_profit) ─
                    if _net_if_sell <= _min_net:
                        if _tp_z >= TP_SPIKE_ZSCORE:
                            log_m("TP", "SKIP_UNPROFITABLE",
                                f"rem={rstr} | {_tp_side} Z={_tp_z:+.2f}>={TP_SPIKE_ZSCORE} "
                                f"MAS net=${_net_if_sell:.4f}<=${_min_net:.4f} "
                                f"(fees comem lucro) — portao 2 bloqueado")
                        continue

                    # ── Portão 3: Z >= TP_SPIKE_ZSCORE ───────────────────────
                    if _tp_z < TP_SPIKE_ZSCORE:
                        continue

                    # ── TODOS OS PORTÕES PASSARAM — VENDA ────────────────────
                    log_sep()
                    log_m("TP", "WICK",
                        f"rem={rstr} | {_tp_side} WICK LUCRATIVO CONFIRMADO | "
                        f"Z={_tp_z:+.2f}>={TP_SPIKE_ZSCORE} | BID={fc(_tp_bid)} | "
                        f"entry_ask={fc(_entry_ask)} | net_proj=${_net_if_sell:.4f} "
                        f"> min_net=${_min_net:.4f} | "
                        f"Kalman={fc(kalmans[_tp_side].x or 0)}"
                    )
                    close_trade(
                        _tp_trade, _tp_bid,
                        f"TP DINAMICO WICK Z={_tp_z:+.2f}>={TP_SPIKE_ZSCORE} "
                        f"net>${TP_MIN_PROFIT_PCT:.1%}",
                        rstr
                    )
                    active_trades.remove(_tp_trade)
                    log_sep()

        # =====================================================================
        # TARGET CHECK — GAMB_TARGET_BID_C (TP estatico, coexiste com TP dinamico)
        # =====================================================================
        for trade in active_trades[:]:
            if trade.get("target") is None:
                continue
            bid_key  = trade["side"].lower()
            curr_bid = best_bids.get(bid_key)
            if curr_bid and curr_bid >= trade["target"]:
                close_trade(trade, curr_bid, "TARGET ESTATICO", rstr)
                active_trades.remove(trade)

        # =====================================================================
        # MODULO 1: PEG ARBIT
        #
        # ask_up + ask_down <= PA_TRIGGER_SUM (0.985): underpeg.
        # Compramos os dois lados ao ASK — um resolve a $1/share.
        # Risco fixo PEG_ARBIT_RISK (arb sem direccao — Kelly nao aplicavel).
        # =====================================================================
        if (PEG_ARBIT_ACTIVE
                and ask_sum <= PA_TRIGGER_SUM
                and rem > PA_MIN_REM
                and pa_count < MAX_PA_ENTRIES
                and now - last_pa_time >= PA_COOLDOWN):

            budget        = bankroll * eff_pa_risk
            ref_ask       = max(ask_up, ask_down)
            shares_to_buy = budget / ref_ask
            fee_up_cost   = fee_rate(ask_up)   * shares_to_buy * ask_up
            fee_dn_cost   = fee_rate(ask_down) * shares_to_buy * ask_down
            total_cost    = (shares_to_buy * ask_up + fee_up_cost +
                             shares_to_buy * ask_down + fee_dn_cost)

            log_sep()
            log_m("PEG ARBIT", "ENTRADA",
                f"rem={rstr} | PEG={ask_sum:.4f} Underpeg={underpeg_c:.1f}c "
                f"| shares={shares_to_buy:.4f} | cost=${total_cost:.4f} "
                f"| ASK_UP={fc(ask_up)} ASK_DOWN={fc(ask_down)} | #={pa_count+1}"
            )
            await asyncio.gather(
                open_trade("UP",   "PEG ARBIT", rstr,
                           risk=eff_pa_risk, fixed_shares=shares_to_buy,
                           token_id=meta["up"]),
                open_trade("DOWN", "PEG ARBIT", rstr,
                           risk=eff_pa_risk, fixed_shares=shares_to_buy,
                           token_id=meta["down"])
            )
            log_sep()
            pa_count    += 1
            last_pa_time = now

        # =====================================================================
        # MODULO 2: GAMBLING — Motor Quantitativo HFT (v1.7.0)
        #
        # PRÉ-FILTRO (novo v1.7.0) — BID_ASK_MIN_RATIO antes das 4 condições:
        #   BID >= ASK * BID_ASK_MIN_RATIO (0.94)
        #   Spread demasiado largo = market makers a retirar liquidez = pre-crash.
        #   Ex: ASK=0.65 → BID mínimo = 0.611. BID=0.57c na 1a entrada do log
        #       já sinalizava risco (BID/ASK = 0.876 < 0.94).
        #
        # 4 condições de entrada em simultâneo (AND logic):
        #   Cond 1 — REGIME (σ): std(Kalman,10s) <= GAMB_MAX_VOL_DEV (0.03)
        #   Cond 2 — Z-SCORE: Z(Kalman) <= _eff_zscore_limit (1.3 normal / 99 endgame)
        #   Cond 3 — OBI: OBI >= GAMB_MIN_OBI (0.65)
        #   Cond 4 — VPIN: VPIN <= _eff_vpin_limit (0.40 normal / 0.60 endgame)
        #
        # Tamanho: Kelly Criterion (cap 5% da banca, frac 1/12).
        # =====================================================================
        if GAMBLING_ACTIVE:
            if rem > GAMB_START_REM_S:
                pass
            elif rem <= GAMB_CUTOFF_S:
                if not gamb_cutoff_logged:
                    gamb_cutoff_logged = True
                    log_m("GAMBLING", "CUTOFF",
                        f"rem={rstr} | parado — rem<={GAMB_CUTOFF_S}s")
            else:
                if not gamb_started_logged:
                    gamb_started_logged = True
                    log_m("GAMBLING", "START",
                        f"rem={rstr} | activo [{GAMB_START_REM_S}s->{GAMB_CUTOFF_S}s] "
                        f"| Kelly(edge={KELLY_ASSUMED_EDGE:.0%}"
                        f" frac=1/{int(round(1/KELLY_FRACTION))} cap={KELLY_MAX_RISK_PCT:.0%}) "
                        f"| HFT: σ<={GAMB_MAX_VOL_DEV} Z<={GAMB_MAX_ZSCORE} "
                        f"OBI>={GAMB_MIN_OBI:.0%} VPIN<={VPIN_SAFE_LIMIT:.0%} "
                        f"| MaxEff={GAMB_MAX_EFF_C:.0f}c BidAskRatio>={BID_ASK_MIN_RATIO:.0%}")

                # ── Endgame Override — thresholds dinâmicos por tempo (v1.6.0) ──
                if rem <= ENDGAME_TRIGGER_S:
                    _eff_zscore_limit = ENDGAME_ZSCORE_LIMIT
                    _eff_vpin_limit   = ENDGAME_VPIN_LIMIT
                    _endgame_active   = True
                else:
                    _eff_zscore_limit = GAMB_MAX_ZSCORE
                    _eff_vpin_limit   = VPIN_SAFE_LIMIT
                    _endgame_active   = False

                for g_side, g_ask, g_bid, g_eff, g_z, g_std, g_obi, g_vpin in (
                    ("UP",   ask_up,   bid_up,   eff_up,   z_up,   std_up,   obi_up,   vpin_up),
                    ("DOWN", ask_down, bid_down, eff_down, z_down, std_down, obi_down, vpin_down)
                ):
                    if now - gamb_last_buy[g_side] < GAMB_BUY_COOLDOWN:
                        continue

                    # === FILTRO SPREAD OFICIAL SDK (v1.7.2) ===
                    # v1.8.0: spread_c actualizado inline pelo WS handler (Área 1)
                    spread_c = best_spreads_c.get(g_side.lower())
                    if spread_c is None or spread_c > MAX_SPREAD_CENTS:
                        if spread_c is not None:
                            log_m("GAMBLING", "BLOCK_SPREAD",
                                f"rem={rstr} | {g_side} spread={spread_c:.2f}c > {MAX_SPREAD_CENTS}c "
                                f"(livro largo — rejeitado)")
                        continue

                    if not (GAMB_MIN_EFF_C <= g_eff <= GAMB_MAX_EFF_C):
                        continue

                    # ── Pré-filtro BID/ASK Ratio (novo v1.7.0) ───────────────
                    # BID demasiado baixo vs. ASK = spread anormal = pre-crash signal.
                    # Evita entrar quando market makers se afastaram do livro.
                    if g_bid is not None and g_ask > 0:
                        _bid_ask_ratio = g_bid / g_ask
                        if _bid_ask_ratio < BID_ASK_MIN_RATIO:
                            log_m("GAMBLING", "BLOCK",
                                f"rem={rstr} | {g_side} eff={g_eff:.1f}c "
                                f"| BID/ASK={_bid_ask_ratio:.3f}<{BID_ASK_MIN_RATIO:.2f} "
                                f"(spread demasiado largo — pre-crash signal)"
                            )
                            continue

                    # ── Cond 1: Regime de compressao ─────────────────────────
                    if g_std is None:
                        log_m("GAMBLING", "WAIT_REGIME",
                            f"rem={rstr} | {g_side} eff={g_eff:.1f}c "
                            f"| σ=n/a (janela {hft_wins[g_side].size()}pts < 3 "
                            f"— aguardando {HFT_WINDOW_SECONDS}s de dados Kalman)")
                        continue
                    if g_std > GAMB_MAX_VOL_DEV:
                        log_m("GAMBLING", "BLOCK",
                            f"rem={rstr} | {g_side} eff={g_eff:.1f}c "
                            f"| COND1 FAIL σ={g_std:.4f}>{GAMB_MAX_VOL_DEV:.3f} "
                            f"(regime volatil — aguardar compressao)")
                        continue

                    # ── Cond 2: Anti-pico Z-Score ─────────────────────────────
                    if g_z is None:
                        log_m("GAMBLING", "WAIT_ZSCORE",
                            f"rem={rstr} | {g_side} eff={g_eff:.1f}c σ={g_std:.4f} "
                            f"| Z=n/a (janela insuficiente)")
                        continue
                    if g_z > _eff_zscore_limit:
                        _mode_tag_z = (
                            f"[ENDGAME({_eff_zscore_limit:.0f})]"
                            if _endgame_active else
                            f"[NORMAL({_eff_zscore_limit:.1f})]"
                        )
                        log_m("GAMBLING", "BLOCK",
                            f"rem={rstr} | {g_side} eff={g_eff:.1f}c σ={g_std:.4f} "
                            f"| COND2 FAIL Z={g_z:+.2f}>{_eff_zscore_limit:.1f} "
                            f"(pico anormal — armadilha de topo) {_mode_tag_z}")
                        continue

                    # ── Cond 3: OBI >= GAMB_MIN_OBI ──────────────────────────
                    if g_obi is not None and g_obi < GAMB_MIN_OBI:
                        log_m("GAMBLING", "BLOCK",
                            f"rem={rstr} | {g_side} eff={g_eff:.1f}c Z={g_z:+.2f} σ={g_std:.4f} "
                            f"| COND3 FAIL OBI={g_obi:.2f}<{GAMB_MIN_OBI:.2f} "
                            f"(vendedores dominam book)")
                        continue
                    elif g_obi is None:
                        log_m("GAMBLING", "WARN_OBI",
                            f"rem={rstr} | {g_side} OBI=n/a (aguardando book WS) — "
                            f"COND3 skipped")

                    # ── Cond 4: VPIN <= _eff_vpin_limit ─────────────────────
                    _mode_tag_vpin = (
                        f"[ENDGAME({_eff_vpin_limit:.2f})]"
                        if _endgame_active else
                        f"[NORMAL({_eff_vpin_limit:.2f})]"
                    )
                    if g_vpin is not None and g_vpin > _eff_vpin_limit:
                        log_m("GAMBLING", "BLOCK",
                            f"rem={rstr} | {g_side} eff={g_eff:.1f}c Z={g_z:+.2f} σ={g_std:.4f} "
                            f"| COND4 FAIL VPIN={g_vpin:.2f}>{_eff_vpin_limit:.2f} "
                            f"(fluxo toxico) {_mode_tag_vpin}")
                        continue
                    elif g_vpin is None:
                        log_m("GAMBLING", "WARN_VPIN",
                            f"rem={rstr} | {g_side} VPIN=n/a (janela em aquecimento) — "
                            f"COND4 skipped {_mode_tag_vpin}")

                    # ── Kelly Criterion ───────────────────────────────────────
                    kelly_risk = calc_kelly_risk(g_ask)
                    if kelly_risk <= 0.0:
                        log_m("GAMBLING", "BLOCK",
                            f"rem={rstr} | {g_side} eff={g_eff:.1f}c ASK={fc(g_ask)} "
                            f"| Kelly<=0 (sem edge a este preco — não entra)")
                        continue

                    # ── TODAS AS CONDIÇÕES SATISFEITAS — ENTRADA ─────────────
                    _obi_s  = f"{g_obi:.2f}"  if g_obi  is not None else "n/a"
                    _vpin_s = f"{g_vpin:.2f}" if g_vpin is not None else "n/a"
                    _ba_r   = f"{g_bid/g_ask:.3f}" if (g_bid is not None and g_ask > 0) else "n/a"
                    _endgame_entry_tag = (
                        f" | ⚡ ENDGAME MODE (Z_lim={ENDGAME_ZSCORE_LIMIT:.0f} VPIN_lim={ENDGAME_VPIN_LIMIT:.2f})"
                        if _endgame_active else ""
                    )
                    if bankroll > 0.0:
                        token_id = meta["up"] if g_side == "UP" else meta["down"]
                        await open_trade(
                            g_side, "GAMBLING", rstr,
                            risk=kelly_risk,
                            token_id=token_id,
                            extra_log=(
                                f"Kelly={kelly_risk:.1%}(edge={KELLY_ASSUMED_EDGE:.0%}) "
                                f"σ={g_std:.4f}(cond1) "
                                f"Z={g_z:+.2f}(cond2/lim={_eff_zscore_limit:.1f}) "
                                f"OBI={_obi_s}(cond3) "
                                f"VPIN={_vpin_s}(cond4/lim={_eff_vpin_limit:.2f}) "
                                f"BidAsk={_ba_r}"
                                f"{_endgame_entry_tag}"
                            )
                        )
                        gamb_last_buy[g_side] = now
                        log_m("GAMBLING", "COOLDOWN",
                            f"rem={rstr} | {g_side} — cooldown {GAMB_BUY_COOLDOWN:.1f}s")

    # =========================================================================
    # PnL ACCOUNTING — movido de main() para logic_loop (v1.8.0 — Área 5)
    # Cada task logic_loop calcula e loga o seu próprio resultado.
    # Garante que o PnL só é logado APÓS resolução real (não estimada).
    # =========================================================================
    profit_this   = bankroll - pre_bank
    daily_profit += profit_this

    if profit_this > 0.00001:
        total_pnl_pos += profit_this
    elif profit_this < -0.00001:
        total_pnl_neg += profit_this

    log_sep2()
    pnl_pct = (profit_this / pre_bank * 100.0) if pre_bank > 0 else 0.0
    dp_start = bankroll - daily_profit + profit_this
    dp_pct   = (daily_profit / dp_start * 100.0) if dp_start > 0 else 0.0
    log_info(
        f"ROUND | PnL: ${profit_this:+.4f} ({pnl_pct:+.2f}%) | "
        f"Kelly(edge={KELLY_ASSUMED_EDGE:.0%} frac=1/{int(round(1/KELLY_FRACTION))} "
        f"cap={KELLY_MAX_RISK_PCT:.0%})"
    )
    log_info(
        f"TOTAL | PnL_dia: ${daily_profit:+.4f} ({dp_pct:+.2f}%) | "
        f"Banca: ${bankroll:.4f} | "
        f"Pos: ${total_pnl_pos:+.4f} | Neg: ${total_pnl_neg:+.4f} | "
        f"Uptime: {get_uptime_str()}"
    )
    log_sep2()

    # Limpa entry desta ronda do _per_market_resolved para libertar memória
    _per_market_resolved.pop(_slug, None)

# =============================================================================
# MAIN (v1.8.0 — Áreas 3, 5, 6 + graceful shutdown SIGTERM — v1.7.0)
# =============================================================================

async def main():
    global daily_profit, last_day, bankroll, price_change
    global total_pnl_pos, total_pnl_neg, bot_start_time
    global resolved_event, resolved_winner_asset
    global _shutdown_flag, _metadata_cache, _ws_sub_queue
    global _active_logic_tasks, _ws_needs_cache_reload

    # ── Graceful Shutdown — SIGTERM handler (v1.7.0) ────────────────────────
    def _handle_sigterm():
        global _shutdown_flag
        log_warn("SIGTERM recebido — iniciando shutdown gracioso...")
        _shutdown_flag = True

    loop = asyncio.get_event_loop()
    try:
        loop.add_signal_handler(signal.SIGTERM, _handle_sigterm)
    except NotImplementedError:
        # Windows não suporta add_signal_handler
        log_warn("SIGTERM handler nao disponivel nesta plataforma (Windows)")

    # v1.5.0: Martingale state REMOVIDO (risk_multiplier, accumulated_loss,
    # recovery_rounds). Kelly Criterion calcula tamanho dinamicamente por trade.
    bot_start_time  = time.time()
    total_pnl_pos   = 0.0
    total_pnl_neg   = 0.0

    # ─ Log de arranque ───────────────────────────────────────────────────────
    log_sep2()
    log_info("BOT XRP POLYMARKET v1.8.0 — WS FULL-STREAM + OVERLAPPING SUBS + METADATA CACHE")
    log_sep2()
    log_info(f"LIVE_TRADING     : {LIVE_TRADING} (reverter: LIVE_TRADING = False)")
    log_info(f"BANKROLL_INIT    : ${bankroll:.2f}")
    log_sep2()
    log_info("PRODUCTION READY (v1.7.0):")
    log_info(f"   RateLimiter     : {RATE_LIMIT_CALLS} req/s sustentado | burst={RATE_LIMIT_BURST}")
    log_info(f"   Retry Backoff   : max={MAX_API_RETRIES} tentativas | {BASE_BACKOFF_S}s->exp->max{MAX_BACKOFF_S}s | jitter={BACKOFF_JITTER}")
    log_info(f"   CircuitBreaker  : open após {CB_FAIL_THRESHOLD} falhas | recovery={CB_RECOVERY_S:.0f}s")
    log_info(f"   WS Reconnect    : backoff {WS_RECONNECT_BASE_S}s->exp->max{WS_RECONNECT_MAX_S}s")
    log_info(f"   SIGTERM         : handler registado — shutdown gracioso")
    log_sep2()
    log_info("WS FULL-STREAM (v1.8.0):")
    log_info(f"   Spread inline   : sem REST fetch_spread_sdk no tick crítico")
    log_info(f"   Overlap         : sub Mercado B @ rem<={WS_OVERLAP_PRE_S}s antes de A expirar")
    log_info(f"   Heartbeat       : ping/pong cada {WS_HEARTBEAT_INTERVAL}s | pong timeout=10s")
    log_info(f"   Cache           : {METADATA_PREFETCH_COUNT} mercados pré-carregados ({METADATA_PREFETCH_COUNT*5}min)")
    log_info(f"   Settlement      : ONLY WS market_resolved — sem fallback BID estimado")
    log_info(f"   Non-blocking    : asyncio.create_task(logic_loop) — ciclo B durante resolução A")
    log_info(f"   Cache reload    : backoff>={WS_CACHE_RELOAD_BACKOFF:.0f}s → reload automático")
    log_sep2()

    # ─ v1.8.0: Pré-carga do metadata cache (Área 3) ─────────────────────────
    _metadata_cache = await _prefetch_metadata_cache()

    # ─ v1.8.0: Queue de subscrições WS (Área 2) — criada após asyncio.run() ─
    _ws_sub_queue = asyncio.Queue()

    # ─ v1.8.0: Task WS persistente (Área 2, 6) ──────────────────────────────
    # ws_handler dura toda a sessão; gerido via sub_queue para subs dinâmicas.
    ws_task = asyncio.create_task(ws_handler(_ws_sub_queue))
    log_info("WS TASK PERSISTENTE iniciada")

    # ─ Loop de ciclos de 5 minutos ───────────────────────────────────────────
    while not _shutdown_flag:
        slug, start_ts = get_current_slug()

        # v1.8.0: Consulta cache (Área 3); fallback para fetch_metadata se ausente
        meta = _metadata_cache.get(slug)
        if meta is None:
            log_info(f"Cache miss para {slug} — fetch_metadata online")
            meta = await fetch_metadata(slug)
            if meta:
                _metadata_cache[slug] = meta
            else:
                log_warn(f"Metadata nao encontrada para {slug} — retry em 2s")
                await asyncio.sleep(2)
                continue

        # v1.8.0: Verifica se WS pediu reload de cache (Área 6)
        if _ws_needs_cache_reload:
            _ws_needs_cache_reload = False
            log_info("WS CACHE RELOAD | backoff longo detectado — reconstruindo metadata cache")
            _metadata_cache = await _prefetch_metadata_cache()

        resolved_event.clear()
        resolved_winner_asset = None

        # Novo dia — reset daily profit
        market_day = datetime.fromtimestamp(start_ts).date()
        if last_day is None or market_day != last_day:
            daily_profit = 0.0
            last_day     = market_day
            if LIVE_TRADING:
                lb = await fetch_live_bankroll()
                if lb is not None:
                    bankroll = lb
            log_sep2()
            log_info(f"NOVO DIA {market_day} | Banca: ${bankroll:.4f} | LIVE={LIVE_TRADING}")
            log_info("Kelly ativo — sem Martingale state para resetar.")
            log_sep2()

        # v1.3.0/v1.4.0: todos os precos e sizes iniciam a None — WS preenche no 1o tick.
        best_bids["up"]      = best_bids["down"]      = None
        best_asks["up"]      = best_asks["down"]      = None
        best_spreads_c["up"] = best_spreads_c["down"] = None
        best_bid_sizes["up"] = best_bid_sizes["down"] = None
        best_ask_sizes["up"] = best_ask_sizes["down"] = None
        price_change.clear()

        # v1.8.0: Subscreve mercado actual via sub_queue (Área 2)
        # ws_handler processa a subscrição entre mensagens WS.
        await _ws_sub_queue.put({
            "assets_ids": [meta["up"], meta["down"]],
            "slug":       slug
        })
        log_info(
            f"WS SUB enviada | {slug} | "
            f"up={meta['up'][:12]}... | down={meta['down'][:12]}..."
        )

        log_info("PRICES INIT | aguardando primeiro tick WS (sem chamadas REST)")
        await asyncio.sleep(1.0)

        if best_bids["up"] is not None:
            # v1.8.0: Non-Blocking — asyncio.create_task (Área 5)
            # logic_loop corre em background; main() avança imediatamente para
            # gerir o ciclo de vida e esperar o próximo slot.
            # PnL accounting acontece dentro de logic_loop ao terminar.
            task = asyncio.create_task(
                logic_loop(start_ts, start_ts + 300, meta, _ws_sub_queue)
            )
            _active_logic_tasks.append(task)

            # Limpa tasks já concluídas da lista
            _active_logic_tasks = [t for t in _active_logic_tasks if not t.done()]

            log_info(
                f"LOGIC TASK criada (non-blocking) | {slug} | "
                f"tasks_activas={len(_active_logic_tasks)}"
            )
        else:
            log_warn("Sem BIDs/ASKs recebidos neste ciclo — a saltar")

        # ── Aguarda o slot do próximo ciclo ──────────────────────────────────
        # main() não bloqueia em logic_loop (create_task). Aguarda até ao início
        # do próximo mercado de 5 min para enviar a subscrição (o overlap já
        # é gerido por logic_loop internamente com WS_OVERLAP_PRE_S).
        next_start = start_ts + 300
        wait_time  = next_start - time.time()
        if wait_time > 0.5:
            await asyncio.sleep(wait_time - 0.5)  # 0.5s de antecipação para setup

    # ── Shutdown gracioso ─────────────────────────────────────────────────────
    log_sep2()
    log_info("SHUTDOWN | cancelando tasks activas...")

    # Cancela todas as tasks logic_loop activas
    for task in _active_logic_tasks:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    # Cancela o ws_task persistente
    ws_task.cancel()
    try:
        await ws_task
    except asyncio.CancelledError:
        pass

    log_sep2()
    log_info(
        f"SHUTDOWN GRACIOSO | SIGTERM ou KeyboardInterrupt | "
        f"Banca final: ${bankroll:.4f} | "
        f"Uptime: {get_uptime_str()}"
    )
    log_info("BOT TERMINADO LIMPO")
    log_sep2()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_info("BOT PARADO PELO UTILIZADOR (KeyboardInterrupt)")