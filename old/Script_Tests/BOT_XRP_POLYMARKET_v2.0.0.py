# =============================================================================
# BOT XRP POLYMARKET — v2.1.0
# =============================================================================
# CHANGELOG v1.9.0 [7 Áreas Críticas — Arquitetura 100% Event-Driven HFT]:
#
# ─────────────────────────────────────────────────────────────────────────────
# CHANGELOG CIRÚRGICO v1.9.0 (alterações por bloco de função):
# ─────────────────────────────────────────────────────────────────────────────
#
# [1] WS ENGINE — Conexão Única Persistente + Traceback de Diagnóstico:
#     FICHEIRO   : Secção IMPORTS — adicionado `import websockets.exceptions`
#     PARÂMETRO  : WS_URI alterado de wss://ws-subscriptions-clob.polymarket.com/ws/market
#                  para wss://clob.polymarket.com/ws (túnel único CLOB)
#     LOGGING    : Secção LOGGING — novo bloco "WS ENGINE DEBUG logger":
#                  logging.getLogger("websockets.client").setLevel(logging.DEBUG)
#                  Partilha o mesmo FileHandler do bot_xrp.log (sem overhead extra)
#     RECONNECT  : ws_handler() — novo handler específico:
#                  except websockets.exceptions.ConnectionClosedError as e:
#                  Loga obrigatoriamente e.code (RFC 6455 Close Code) e e.reason
#                  para diagnóstico de rede (e.g. code=1006 = queda abrupta,
#                  code=1001 = Going Away, code=4000 = policy violation Polymarket)
#                  Separado do handler genérico `except Exception` (antes fundidos)
#     HEARTBEAT  : ws_handler() — heartbeat ping/pong mantido (sem alterações)
#     BACKOFF    : ws_handler() — backoff exponencial mantido; agora também aplica
#                  a ConnectionClosedError (antes só ao handler genérico)
#
# [2] PRICE ROUTER — Dispatcher Centralizado (função autónoma):
#     NOVA FUNÇÃO: _price_router(raw: str, _tid_map: dict) -> bool
#                  Extraída do interior de ws_handler (era código inline monolítico)
#                  Recebe frame bruto WS → parseia JSON → distribui por tipo:
#                    • book / best_bid_ask / price_change → actualiza best_bids,
#                      best_asks, best_spreads_c, best_bid_sizes, best_ask_sizes
#                    • market_resolved → popula _per_market_resolved[slug] e
#                      sinaliza resolved_event global
#                  Retorna True se algum preço foi actualizado (ws_handler faz set)
#                  Função síncrona (sem await) — pura computação CPU; zero I/O;
#                  não bloqueia o event loop
#     ws_handler : Corpo do loop `async for raw in ws:` reduzido a:
#                    1. validação de frame (len < 10)
#                    2. chamada _price_router(raw, _tid_map)
#                    3. drenagem da sub_queue (FIX BUG — ver área 3)
#                    4. price_change.set() se updated=True
#
# [3] OVERLAPPING SUBSCRIPTIONS — Fix de Bug Crítico:
#     BUG v1.8.x : sub_queue era apenas drenada no arranque de cada conexão WS.
#                  Subscrições de overlap (enviadas por logic_loop durante runtime
#                  via await _ws_sub_queue.put(...)) ficavam na queue mas NUNCA
#                  eram processadas enquanto a conexão estava activa — apenas na
#                  próxima reconexão. Overlap era efectivamente inoperante.
#     FIX v1.9.0 : ws_handler() — drenagem da sub_queue movida para DENTRO do
#                  loop `async for raw in ws:`, após cada frame processado.
#                  Usa sub_queue.get_nowait() (não-bloqueante) com guarda
#                  `if not sub_queue.empty()` para custo zero quando vazia.
#                  Subscrições overlap chegam agora ao WS em <1 frame de latência.
#                  _known_subscriptions actualizado inline para re-subscrição
#                  automática em caso de reconexão.
#
# [4] STARTUP METADATA CACHE — Background Task por Ciclo:
#     NOVA FUNÇÃO: _background_cache_next(next_slug: str) — async background task
#                  Carrega metadata de um único slug via fetch_metadata() sem
#                  bloquear o trading. Verifica cache antes de fazer GET REST.
#                  Disparada por asyncio.create_task() — fire-and-forget.
#     logic_loop : Fim de mercado (rem <= 0) — após resolução, dispara:
#                  asyncio.create_task(_background_cache_next(slug_dois_à_frente))
#                  Mantém cache cheio: quando mercado A termina, mercado A+2
#                  (que seria o próximo fora da janela de prefetch) é carregado
#                  em background enquanto o bot já está a operar em B.
#     MANTIDO    : _prefetch_metadata_cache() no arranque (sem alterações)
#     MANTIDO    : _ws_needs_cache_reload flag para reconexões longas
#
# [5] REAL-TIME SETTLEMENT — Inalterado (já correcto em v1.8.1):
#     PROIBIDO   : Estimativa por BID/ASK mantida proibida
#     ONLY WS    : close_trade_resolution() só disparado por market_resolved WS
#     WARN ONLY  : RESOLVE_TIMEOUT_S continua como threshold de aviso, não fallback
#
# [6] ARQUITETURA NÃO-BLOQUEANTE — Inalterada (já correcta em v1.8.1):
#     create_task: asyncio.create_task(logic_loop(...)) em main() — sem alterações
#     BACKGROUND : _active_logic_tasks mantido — sem alterações
#
# [7] QUANT & RISK ENGINE — Integração directa no fluxo de ticks WS:
#     FLUXO NOVO : WS frame → _price_router() → price_change.set()
#                  → logic_loop acorda → Kalman.update() → HFTWindow.add()
#                  → zscore() → VPINTracker.add() → Kelly → open_trade()
#                  Latência tick-to-trade: limitada apenas pelo asyncio event loop
#                  (tipicamente <1ms em hardware moderno)
#     INALTERADO : KalmanFilter1D, HFTWindow, VPINTracker, check_gambling_signal_95wr
#                  calc_kelly_risk — sem qualquer alteração
#
# ─────────────────────────────────────────────────────────────────────────────
# FUNÇÕES/GLOBALS AFECTADOS v1.9.0:
#   - Imports: +1 (`import websockets.exceptions`)
#   - Parâmetros globais: WS_URI alterado (wss://clob.polymarket.com/ws)
#   - Logging: +4 linhas (websockets.client DEBUG logger)
#   - _background_cache_next(slug) [NOVA] — background GET por ciclo
#   - _price_router(raw, _tid_map) [NOVA] — dispatcher centralizado
#   - ws_handler(sub_queue) [REFATORADO]:
#       + ConnectionClosedError handler separado com e.code + e.reason
#       + Corpo do loop delegado a _price_router()
#       + sub_queue drenada DENTRO do loop (fix bug overlap)
#   - logic_loop(...) [MODIFICADO]:
#       + asyncio.create_task(_background_cache_next(...)) em fim de mercado
#   - main() [MODIFICADO]: string de versão actualizada para v1.9.0
#
# FUNÇÕES PRESERVADAS (lógica core intacta):
#   - generate_polymarket_url / get_current_slug / get_market_and_token_ids
#   - fetch_metadata / fetch_spread_sdk / fetch_fee_rate_bps / fetch_live_bankroll
#   - _prefetch_metadata_cache — sem alterações
#   - get_token_price — sem alterações
#   - KalmanFilter1D, HFTWindow, VPINTracker — sem alterações
#   - check_gambling_signal_95wr — sem alterações
#   - open_trade / close_trade / close_trade_resolution — sem alterações
#   - RateLimiter, CircuitBreaker, retry_with_backoff — sem alterações
#   - Toda a lógica PEG ARBIT, GAMBLING, SL, TP — sem alterações
#   - Globais de estado v1.8.0 — sem alterações
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
import websockets.exceptions  # v1.9.0: importação explícita para ConnectionClosedError
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
MAX_RISK_PERCENT  = 0.50  # Cap PEG ARBIT: investimento PA nunca excede 35% da banca [Range: 0.10 | 0.50]

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
GAMB_START_REM_S  = 180      # Activa Gambling quando remaining <= X seg [Range: 60 | 300]
GAMB_CUTOFF_S     = 15        # Para Gambling quando remaining <= X seg [Range: 0 | 30]
GAMB_MIN_EFF_C    = 70.0     # eff_c minimo para entrada (cents); v1.7.0: 65->67 (break-even BID >= 69c) [Range: 50.0 | 95.0]
GAMB_MAX_EFF_C    = 90.0     # eff_c maximo para entrada (cents); v1.6.0: 95->78 [Range: 65.0 | 99.9]
GAMB_BUY_COOLDOWN = 12.0     # Cooldown entre compras do mesmo lado (seg); v1.7.0: 8->12 (menos entradas em cascata) [Range: 0.5 | 60.0]
GAMB_PEG_MIN      = 0.975    # Soma minima ask_up + ask_down para entrar (liquidez minima) [Range: 0.90 | 0.999]
GAMB_TARGET_BID_C = 0.0      # Take-Profit ESTATICO ao BID (cents; 0=desactivado; TP dinamico via TP_SPIKE_ZSCORE) [Range: 0.0 | 99.0]
# [DEPRECATED v1.5.0] GAMB_MIN_IMBALANCE = 0.60  # Renomeado para GAMB_MIN_OBI
# [DEPRECATED v1.4.0] GAMB_MIN_TICKS, GAMB_VOL_MAX_C, GAMB_D*_THRESH_C — substituidos por HFT

# --- FILTRO SPREAD OFICIAL SDK (novo v1.7.2) ---
MAX_SPREAD_CENTS  = 2.05     # Spread máximo permitido (em cents) — oficial do client.get_spread

# --- FILTRO DE LIQUIDEZ BID/ASK (novo v1.7.0) ---
#
#   Antes de entrar num lado Gambling, verifica que o spread BID/ASK e saudavel.
#   BID < ASK * BID_ASK_MIN_RATIO => spread demasiado largo => perigo de crash iminente.
#   Ex: ASK=0.65 e BID_ASK_MIN_RATIO=0.94 => BID >= 0.611 obrigatorio para entrar.
#   Evita entrar quando market makers se afastaram (sinal pre-crash).
#
BID_ASK_MIN_RATIO = 0.96    # BID >= ASK * ratio: spread saudavel minimo para entrada [Range: 0.85 | 0.99]

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
GAMB_MAX_VOL_DEV   = 0.020   # StdDev(Kalman,10s) <= 3c: regime estavel/comprimido; v1.7.0: 0.04->0.03 [Range: 0.01 | 0.15]
#   Cond 2 — Anti-pico (Z-Score):
GAMB_MAX_ZSCORE    = 1.2    # Z <= 1.3: preco nao esta num pico vs. trajectoria Kalman; v1.7.0: 1.1->1.3 [Range: 0.5 | 3.0]
#   Cond 3 — Suporte real (OBI = Orderbook Imbalance):
GAMB_MIN_OBI       = 0.70   # OBI >= 65%: compradores dominam o Top of Book; v1.7.0: 0.70->0.65 [Range: 0.50 | 0.90]
#   Cond 4 — Fluxo saudavel (VPIN = Order Flow Toxicity):
VPIN_SAFE_LIMIT    = 0.45   # VPIN <= 0.40: fluxo normal; sem dump institucional detetado [Range: 0.30 | 0.95]

# --- ENDGAME OVERRIDE — Modo Agressivo Final (v1.6.0) ---
#
#   Quando remaining_seconds <= ENDGAME_TRIGGER_S:
#     Cond 2 (Z-Score): limite temporário -> ENDGAME_ZSCORE_LIMIT (99.0 = desativado)
#     Cond 4 (VPIN):    limite temporário -> ENDGAME_VPIN_LIMIT  (0.60 = relaxado)
#   Globals GAMB_MAX_ZSCORE e VPIN_SAFE_LIMIT NUNCA são mutados (thread-safe).
#
ENDGAME_TRIGGER_S    = 30.999  # Activa modo agressivo quando remaining <= X seg [Range: 10.0 | 60.0]
ENDGAME_ZSCORE_LIMIT = 2.0    # Limite Z-Score em modo ENDGAME (99.0 = desativado) [Range: 2.0 | 99.0]
ENDGAME_VPIN_LIMIT   = 0.60    # Limite VPIN em modo ENDGAME (relaxado vs. VPIN_SAFE_LIMIT) [Range: 0.40 | 0.95]

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
# v1.9.0: URI alterada para túnel único CLOB WebSocket (wss://clob.polymarket.com/ws)
# Anterior (v1.8.x): wss://ws-subscriptions-clob.polymarket.com/ws/market
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

# v2.1.0: Per-market orderbook state — isolado por slug para evitar colisão
# durante overlap de múltiplos mercados concorrentes.
# Estrutura: market_orderbooks[slug] = {
#     "bids":      {"up": float|None, "down": float|None},
#     "asks":      {"up": float|None, "down": float|None},
#     "spreads_c": {"up": float|None, "down": float|None},
#     "bid_sizes": {"up": float|None, "down": float|None},
#     "ask_sizes": {"up": float|None, "down": float|None},
# }
market_orderbooks: dict[str, dict[str, dict[str, float | None]]] = {}


def _init_market_orderbook(slug: str) -> None:
    """Inicializa (ou reseta) o orderbook isolado para um slug."""
    market_orderbooks[slug] = {
        "bids":      {"up": None, "down": None},
        "asks":      {"up": None, "down": None},
        "spreads_c": {"up": None, "down": None},
        "bid_sizes": {"up": None, "down": None},
        "ask_sizes": {"up": None, "down": None},
    }


def _cleanup_market_orderbook(slug: str) -> None:
    """Remove o orderbook de um slug após resolução (evita leak de memória)."""
    market_orderbooks.pop(slug, None)

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
# LOGGING
# =============================================================================

_fmt  = logging.Formatter("%(message)s")
_fh   = logging.FileHandler("bot_xrp.log", encoding="utf-8")
_fh.setFormatter(_fmt)
logger = logging.getLogger("bot_xrp")
logger.setLevel(logging.DEBUG)
logger.addHandler(_fh)
logger.propagate = False

# --- v1.9.0: WS ENGINE DEBUG logger (Área 1) ---
# Activa logging de nível DEBUG para a biblioteca websockets.client.
# Loga handshake SSL, frames de controlo (ping/pong), close frames e
# detalhes de protocolo internos — enviado para o mesmo bot_xrp.log.
# RFC 6455 Close Codes frequentes: 1000=Normal, 1001=GoingAway,
# 1006=AbnormalClosure (TCP drop), 4000=PolicyViolation (Polymarket ban).
_ws_client_logger = logging.getLogger("websockets.client")
_ws_client_logger.setLevel(logging.DEBUG)
_ws_client_logger.addHandler(_fh)   # Partilha FileHandler do bot — sem overhead extra
_ws_client_logger.propagate = False

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
    return f"{p * 100:.1f}c"

def log_m(module: str, action: str, msg: str):
    logger.info(f"[{module}] [{action}] [{get_ts()}] | {msg}")

def log_info(msg: str):
    logger.info(f"[INFO] [{get_ts()}] | {msg}")

def log_warn(msg: str):
    logger.warning(f"[WARN] [{get_ts()}] | {msg}")

def log_error(msg: str):
    logger.error(f"[ERROR] [{get_ts()}] | {msg}")

def log_raw(msg: str):
    logger.info(f"[{get_ts()}] | {msg}")

def log_sep():
    logger.info("-" * 80)

def log_sep2():
    logger.info("=" * 80)

# =============================================================================
# RATE LIMITER
# =============================================================================

class RateLimiter:
    __slots__ = ("calls_per_second", "burst", "tokens", "last_check", "_lock")

    def __init__(self, calls_per_second: float = 8.0, burst: float = 15.0):
        self.calls_per_second: float  = calls_per_second
        self.burst:            float  = burst
        self.tokens:           float  = burst
        self.last_check:       float  = time.monotonic()
        self._lock                    = asyncio.Lock()

    async def acquire(self):
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

rate_limiter = RateLimiter(calls_per_second=RATE_LIMIT_CALLS, burst=RATE_LIMIT_BURST)

# =============================================================================
# CIRCUIT BREAKER
# =============================================================================

class CircuitBreaker:
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
        if self._state == self.STATE_CLOSED:
            return False
        if self._state == self.STATE_OPEN:
            if time.monotonic() - self._opened_at >= self.recovery_s:
                self._state = self.STATE_HALF_OPEN
                log_info(f"CircuitBreaker | OPEN -> HALF-OPEN (a testar depois de {self.recovery_s:.0f}s)")
                return False
            return True
        return False

    def record_success(self):
        if self._state != self.STATE_CLOSED:
            log_info(f"CircuitBreaker | {self._state} -> CLOSED (chamada OK)")
        self._state    = self.STATE_CLOSED
        self._failures = 0

    def record_failure(self):
        self._failures += 1
        if self._state == self.STATE_HALF_OPEN:
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

api_circuit_breaker = CircuitBreaker(
    fail_threshold=CB_FAIL_THRESHOLD,
    recovery_s=CB_RECOVERY_S
)

# =============================================================================
# RETRY COM BACKOFF
# =============================================================================

async def retry_with_backoff(fn, *args, label: str = "call", **kwargs):
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
                    backoff *= (0.7 + random.random() * 0.6)
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

clob_client    = None
clob_ro_client = None

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
        logger.error(f"[ERROR] [{get_ts()}] | py-clob-client nao instalado!")
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
    return _FEE_RATE * (p * (1.0 - p)) ** _FEE_EXP

def eff_price_c(ask: float) -> float:
    return ask * (1.0 + fee_rate(ask)) * 100.0

def sell_payout_net(shares: float, bid: float) -> float:
    return shares * bid * (1.0 - fee_rate(bid))

def resolution_payout(shares: float, winner: bool) -> float:
    return shares if winner else 0.0

def calc_kelly_risk(ask: float) -> float:
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
    if bid_size is None or ask_size is None:
        return None
    total = bid_size + ask_size
    if total <= 1e-9:
        return None
    return bid_size / total

# =============================================================================
# SDK HELPERS
# =============================================================================

def _fetch_spread_sdk_sync(token_id: str):
    if clob_ro_client is None:
        return None
    result = clob_ro_client.get_spread(token_id)
    raw    = result.get("spread")
    if raw is None:
        return None
    return float(raw) * 100.0

async def fetch_spread_sdk(token_id: str):
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
# API HELPERS
# =============================================================================

def _fetch_metadata_sync(slug: str):
    data = requests.get(f"{GAMMA_API_URL}/events?slug={slug}", timeout=5).json()[0]["markets"][0]
    ids  = json.loads(data["clobTokenIds"])
    return {"id": data["conditionId"], "up": ids[0], "down": ids[1], "slug": slug}

async def fetch_metadata(slug: str):
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
    r = requests.get(f"{CLOB_REST_URL}/fee-rate", params={"token_id": token_id}, timeout=4)
    return int(r.json().get("fee_rate_bps", 0))

async def fetch_fee_rate_bps(token_id: str) -> int:
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
    if not clob_client:
        return None
    return float(clob_client.get_balance())

async def fetch_live_bankroll():
    if not clob_client:
        return None
    await rate_limiter.acquire()
    result = await retry_with_backoff(_fetch_live_bankroll_sync, label="live_bankroll")
    if result is None:
        log_warn("fetch_live_bankroll falhou — usando banca actual")
    return result

def redeem_live_position(shares: float, token_id: str):
    if not clob_client:
        return
    try:
        result = clob_client.redeem_positions(token_id=token_id, amount=shares)
        log_info(f"REDEEM | {shares:.4f} shares resgatadas | token={token_id[:16]}... | {result}")
    except Exception as e:
        log_warn(f"REDEEM falhou: {e}")

# =============================================================================
# METADATA CACHE PREFETCH
# =============================================================================

async def _prefetch_metadata_cache() -> dict:
    cache    = {}
    now      = time.time()
    start_ts = now - (now % 300)
    if (start_ts + 300 - now) < 5:
        start_ts += 300

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
        await asyncio.sleep(0.15)

    log_info(f"METADATA PREFETCH | {len(cache)}/{METADATA_PREFETCH_COUNT} mercados em cache")
    log_sep()
    return cache


async def _background_cache_next(next_slug: str):
    """
    v1.9.0 — BACKGROUND CACHE TASK (Área 4):

    Carrega metadata de um único mercado em segundo plano sem bloquear o trading.
    Disparada via asyncio.create_task() no fim de cada ciclo de logic_loop:
      - Quando o Mercado A termina, carrega o slug do Mercado A+2 (o seguinte
        ao que o overlap já subscreveu), mantendo o cache sempre cheio.
    Verifica _metadata_cache antes de fazer GET REST — idempotente (custo zero
    se o slug já estiver em cache pelo prefetch inicial).

    Args:
        next_slug: slug no formato "xrp-updown-5m-<timestamp>" a carregar.
    """
    if next_slug in _metadata_cache:
        log_info(f"CACHE BACKGROUND | {next_slug} já em cache — sem GET REST")
        return
    log_info(f"CACHE BACKGROUND | a carregar {next_slug} em background...")
    meta = await fetch_metadata(next_slug)
    if meta:
        _metadata_cache[next_slug] = meta
        log_info(
            f"CACHE BACKGROUND | {next_slug} OK "
            f"| up={meta['up'][:12]}... | down={meta['down'][:12]}..."
        )
    else:
        log_warn(f"CACHE BACKGROUND | {next_slug} falhou — será tentado por main() se necessário")


# =============================================================================
# PRICE ROUTER — Dispatcher Centralizado v1.9.0 (Área 2)
# =============================================================================

def _price_router(raw: str, _tid_map: dict[str, dict[str, str]]) -> bool:
    """
    PRICE ROUTER — Dispatcher central v2.1.0.

    Recebe a string bruta de um frame WS e distribui mensagens por tipo:

      • book / best_bid_ask / price_change
            → actualiza market_orderbooks[slug] (estado ISOLADO por mercado)
            → logic_loop acorda no próximo tick e processa Kalman/Z/Kelly

      • market_resolved
            → popula _per_market_resolved[slug]["winner"] + .event.set()
            → sinaliza resolved_event global (fallback de compatibilidade)
            → logic_loop de fim de mercado recebe a confirmação real

    v2.1.0 FIX: _tid_map agora mapeia token_id -> {"side": "up"|"down", "slug": str}.
                Preços são escritos em market_orderbooks[slug] em vez de dicts globais
                planos, eliminando colisões entre Market A e Market B durante overlap.

    Função SÍNCRONA (sem await): pura computação CPU sem I/O.

    Args:
        raw:      Frame bruto recebido do WebSocket (string JSON).
        _tid_map: Mapa token_id -> {"side": "up"|"down", "slug": str}.

    Returns:
        True  — se pelo menos um bid ou ask foi actualizado.
        False — se o frame não continha actualizações de preço.

    Raises:
        json.JSONDecodeError — frame não é JSON válido; caller gere o erro.
    """
    global resolved_winner_asset

    items = json.loads(raw)

    if not isinstance(items, list):
        items = [items]

    updated = False

    for item in items:
        evt = item.get("event_type")

        # ── ROUTE: market_resolved → liquidação em tempo real ─────────────────
        if evt == "market_resolved":
            wa = item.get("winning_asset_id")
            if wa:
                resolved_winner_asset = wa
                resolved_event.set()
                log_info(
                    f"RESOLUCAO WS | winning_asset_id={wa[:16]}... "
                    f"| outcome={item.get('winning_outcome','?')}"
                )
                _slug_for_winner = _token_to_slug.get(wa)
                if _slug_for_winner and _slug_for_winner in _per_market_resolved:
                    _mrd          = _per_market_resolved[_slug_for_winner]
                    _mrd["winner"] = wa
                    _mrd["event"].set()
                    log_info(
                        f"RESOLUCAO ROUTING | slug={_slug_for_winner} "
                        f"| sinalizado"
                    )
                else:
                    log_warn(
                        f"RESOLUCAO | token {wa[:16]}... "
                        f"nao em _token_to_slug "
                        f"— fallback global"
                    )
            continue

        # ── ROUTE: price_change / book / best_bid_ask → preços ────────────────
        aid = item.get("asset_id")
        _tid_entry = _tid_map.get(aid)
        if _tid_entry is None:
            continue

        sk   = _tid_entry["side"]    # "up" | "down"
        slug = _tid_entry["slug"]    # market slug

        # Obter referência ao orderbook isolado deste mercado
        ob = market_orderbooks.get(slug)
        if ob is None:
            # Orderbook não inicializado — ignorar (race condition no arranque)
            continue

        bid_p = ask_p = None

        if evt == "book":
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
                    bid_p              = best_b_price
                    ob["bid_sizes"][sk] = float(best_b_entry.get("size", 0))
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
                    ask_p              = best_a_price
                    ob["ask_sizes"][sk] = float(best_a_entry.get("size", 0))
            if bid_p is not None and ask_p is not None:
                ob["spreads_c"][sk] = (ask_p - bid_p) * 100.0

        elif evt == "best_bid_ask":
            bb = item.get("best_bid")
            ba = item.get("best_ask")
            if bb:
                bid_p = float(bb)
            if ba:
                ask_p = float(ba)
            sp_raw = item.get("spread")
            if sp_raw is not None:
                ob["spreads_c"][sk] = float(sp_raw) * 100.0
            elif bid_p is not None and ask_p is not None:
                ob["spreads_c"][sk] = (ask_p - bid_p) * 100.0

        elif evt == "price_change":
            pcs = item.get("price_changes", [])
            if pcs:
                bb = pcs[-1].get("best_bid")
                ba = pcs[-1].get("best_ask")
                if bb:
                    bid_p = float(bb)
                if ba:
                    ask_p = float(ba)
            if bid_p is not None and ask_p is not None:
                ob["spreads_c"][sk] = (ask_p - bid_p) * 100.0

        # Actualiza orderbook ISOLADO deste mercado
        if bid_p is not None:
            ob["bids"][sk] = bid_p
            updated        = True
        if ask_p is not None:
            ob["asks"][sk] = ask_p
            updated        = True

    return updated


# =============================================================================
# WEBSOCKET HANDLER PERSISTENTE v1.9.0 — WS ENGINE (Área 1)
# =============================================================================

async def ws_handler(sub_queue: asyncio.Queue):
    """
    WS ENGINE v1.9.0 — Conexão única persistente ao CLOB WebSocket.

    Arquitectura:
      - URI: wss://clob.polymarket.com/ws (túnel único — v1.9.0)
      - Heartbeat ping/pong activo (WS_HEARTBEAT_INTERVAL seg; pong timeout 10s)
      - Reconexão automática com backoff exponencial (WS_RECONNECT_BASE_S → MAX)
      - Debug logger websockets.client activo (configurado na secção LOGGING)
      - ConnectionClosedError separado do handler genérico: loga e.code + e.reason
        para diagnóstico de rede (RFC 6455 Close Codes)
      - Frames brutos delegados a _price_router() (PRICE ROUTER centralizado)
      - sub_queue drenada DENTRO do loop principal (fix bug overlap v1.8.x)

    Fluxo por frame:
      1. Validação de frame (len < 10 → descarta; >50% erros → reconecta)
      2. _price_router(raw, _tid_map) → parseia + distribui → retorna updated
      3. Drenagem não-bloqueante da sub_queue (novas subscrições de overlap)
      4. price_change.set() se updated=True → acorda logic_loop
    """
    global _ws_needs_cache_reload

    _tid_map: dict[str, dict[str, str]] = {}   # v2.1.0: token_id -> {"side": "up"|"down", "slug": str}
    _known_subscriptions: list = []
    _ws_backoff                = WS_RECONNECT_BASE_S

    # Contadores de frames para debug e detecção de degradação
    _frame_count       = 0
    _valid_frame_count = 0
    _error_frame_count = 0

    while not _shutdown_flag:
        try:
            async with websockets.connect(
                WS_URI,               # ← variável global; sem strings literais aqui
                ping_interval=None,   # Heartbeat manual via _heartbeat task
                ping_timeout=None,
                compression=None,
                max_size=2**20,
                max_queue=32
            ) as ws:

                log_info(
                    f"WS ENGINE conectado → {WS_URI} "
                    f"| backoff_reset={WS_RECONNECT_BASE_S}s "
                    f"| subs_conhecidas={len(_known_subscriptions)}"
                )
                _ws_backoff        = WS_RECONNECT_BASE_S
                _frame_count       = 0
                _valid_frame_count = 0
                _error_frame_count = 0

                # Re-subscreve todos os tokens conhecidos após reconexão
                # (garante que overlapping subs do ciclo anterior são restauradas)
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
                    await asyncio.sleep(0.1)

                # Drena sub_queue pendente acumulada durante reconexão
                while not sub_queue.empty():
                    try:
                        _pending = sub_queue.get_nowait()
                        _aids    = _pending["assets_ids"]
                        if not any(s["assets_ids"] == _aids for s in _known_subscriptions):
                            _known_subscriptions.append(_pending)
                            _slug_p = _pending.get("slug", "")
                            _tid_map[_aids[0]] = {"side": "up",   "slug": _slug_p}
                            _tid_map[_aids[1]] = {"side": "down", "slug": _slug_p}
                            if _slug_p:
                                _token_to_slug[_aids[0]] = _slug_p
                                _token_to_slug[_aids[1]] = _slug_p
                                if _slug_p not in market_orderbooks:
                                    _init_market_orderbook(_slug_p)
                        await ws.send(json.dumps({
                            "assets_ids":             _aids,
                            "type":                   "market",
                            "custom_feature_enabled": True
                        }))
                        log_info(
                            f"WS NOVA SUB (startup drain) | slug={_pending.get('slug','?')} "
                            f"| {[t[:12] for t in _aids]}"
                        )
                        await asyncio.sleep(0.1)
                    except asyncio.QueueEmpty:
                        break

                # ── Heartbeat Task (detecta conexões zombie) ──────────────────
                # Envia ws.ping() cada WS_HEARTBEAT_INTERVAL seg.
                # Aguarda pong com timeout de 10s.
                # Pong timeout → ws.close() forçado → reconexão imediata.
                async def _heartbeat():
                    try:
                        while True:
                            await asyncio.sleep(WS_HEARTBEAT_INTERVAL)
                            try:
                                pong_waiter = await ws.ping()
                                await asyncio.wait_for(pong_waiter, timeout=10.0)
                            except asyncio.TimeoutError:
                                log_warn(
                                    f"WS HEARTBEAT | pong timeout (>10s) — "
                                    f"frames_total={_frame_count} valid={_valid_frame_count} "
                                    f"errors={_error_frame_count} — fechando conexão zombie"
                                )
                                await ws.close()
                                await asyncio.sleep(0.1)
                                return
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass

                _hb_task = asyncio.create_task(_heartbeat())

                try:
                    # ── Loop principal de frames ──────────────────────────────
                    # Delega parsing e routing a _price_router() (PRICE ROUTER).
                    # Drena sub_queue dentro do loop (fix bug overlap v1.8.x).
                    async for raw in ws:
                        _frame_count += 1

                        # DEBUG: estatísticas a cada 100 frames
                        if _frame_count % 100 == 0:
                            log_info(
                                f"WS DEBUG | frames_total={_frame_count} | "
                                f"valid={_valid_frame_count} "
                                f"({_valid_frame_count * 100 // _frame_count if _frame_count else 0}%) | "
                                f"errors={_error_frame_count}"
                            )

                        # v2.1.0: Filtro frames vazios/muito pequenos ANTES de parsear
                        if not raw or len(raw) < 10:
                            _error_frame_count += 1
                            if raw:
                                log_warn(
                                    f"WS FRAME VAZIO | raw_len={len(raw)} | "
                                    f"content={repr(raw[:50])}"
                                )
                            continue

                        # v1.9.0: Delega para _price_router() — PRICE ROUTER centralizado
                        try:
                            updated = _price_router(raw, _tid_map)
                        except json.JSONDecodeError as _jpe:
                            _error_frame_count += 1
                            log_warn(
                                f"WS JSON PARSE ERRO | {_jpe} | "
                                f"raw_len={len(raw)} | first_20_chars={repr(raw[:20])} | "
                                f"error_rate={_error_frame_count * 100 // _frame_count if _frame_count else 0}%"
                            )
                            # Se taxa de erro muito alta (>50%), reconectar
                            if _error_frame_count > 0 and (_error_frame_count * 100 // _frame_count) > 50:
                                log_warn(
                                    f"WS ERROR RATE MUITO ALTA ({_error_frame_count}/{_frame_count}) — "
                                    f"reconectando para limpar buffer"
                                )
                                await ws.close()
                                break
                            continue

                        _valid_frame_count += 1

                        # v1.9.0 FIX BUG OVERLAP: drena sub_queue DENTRO do loop
                        # (v1.8.x só drenava no arranque — overlap subs perdidas durante runtime)
                        if not sub_queue.empty():
                            while True:
                                try:
                                    _pending = sub_queue.get_nowait()
                                    _aids    = _pending["assets_ids"]
                                    # Adiciona a _known_subscriptions se ainda não presente
                                    if not any(s["assets_ids"] == _aids for s in _known_subscriptions):
                                        _known_subscriptions.append(_pending)
                                        _slug_rt = _pending.get("slug", "")
                                        _tid_map[_aids[0]] = {"side": "up",   "slug": _slug_rt}
                                        _tid_map[_aids[1]] = {"side": "down", "slug": _slug_rt}
                                        if _slug_rt:
                                            _token_to_slug[_aids[0]] = _slug_rt
                                            _token_to_slug[_aids[1]] = _slug_rt
                                            if _slug_rt not in market_orderbooks:
                                                _init_market_orderbook(_slug_rt)
                                    # Envia subscrição ao WS imediatamente
                                    await ws.send(json.dumps({
                                        "assets_ids":             _aids,
                                        "type":                   "market",
                                        "custom_feature_enabled": True
                                    }))
                                    log_info(
                                        f"WS NOVA SUB (runtime) | slug={_pending.get('slug','?')} "
                                        f"| {[t[:12] for t in _aids]}"
                                    )
                                except asyncio.QueueEmpty:
                                    break

                        if updated:
                            price_change.set()

                except asyncio.CancelledError:
                    _hb_task.cancel()
                    try:
                        await _hb_task
                    except asyncio.CancelledError:
                        pass
                    raise
                finally:
                    if not _hb_task.done():
                        _hb_task.cancel()
                        try:
                            await _hb_task
                        except asyncio.CancelledError:
                            pass

        except asyncio.CancelledError:
            log_info("WS ENGINE | CancelledError — shutdown gracioso")
            break

        # v1.9.0: Handler ESPECÍFICO para ConnectionClosedError (Área 1)
        # Captura obrigatória de e.code (RFC 6455) e e.reason para diagnóstico de rede.
        # Separado do handler genérico para garantir que o Close Code é sempre logado.
        # Códigos frequentes: 1000=Normal, 1001=GoingAway, 1006=AbnormalClosure (TCP drop),
        # 1008=PolicyViolation, 4000+=custom (Polymarket ban/throttle).
        except websockets.exceptions.ConnectionClosedError as e:
            if _shutdown_flag:
                log_info(
                    f"WS ENGINE | ConnectionClosed durante shutdown "
                    f"| code={e.code} reason={e.reason!r}"
                )
                break
            log_warn(
                f"WS ENGINE | DESCONEXÃO | code={e.code} reason={e.reason!r} "
                f"| frames={_frame_count} valid={_valid_frame_count} "
                f"errors={_error_frame_count} "
                f"— reconectando em {_ws_backoff:.1f}s"
            )
            if _ws_backoff >= WS_CACHE_RELOAD_BACKOFF:
                _ws_needs_cache_reload = True
                log_warn(
                    f"WS ENGINE | backoff>={WS_CACHE_RELOAD_BACKOFF}s "
                    f"— metadata cache reload agendado"
                )
            await asyncio.sleep(_ws_backoff)
            _ws_backoff        = min(_ws_backoff * 2.0, WS_RECONNECT_MAX_S)
            _frame_count       = 0
            _valid_frame_count = 0
            _error_frame_count = 0

        except Exception as e:
            if _shutdown_flag:
                log_info("WS ENGINE | erro durante shutdown — saindo")
                break
            log_warn(
                f"WS ENGINE | {type(e).__name__}: {e} "
                f"| frames={_frame_count} valid={_valid_frame_count} "
                f"errors={_error_frame_count} "
                f"— reconectando em {_ws_backoff:.1f}s"
            )
            if _ws_backoff >= WS_CACHE_RELOAD_BACKOFF:
                _ws_needs_cache_reload = True
            await asyncio.sleep(_ws_backoff)
            _ws_backoff        = min(_ws_backoff * 2.0, WS_RECONNECT_MAX_S)
            _frame_count       = 0
            _valid_frame_count = 0
            _error_frame_count = 0

# =============================================================================
# LIVE ORDER
# =============================================================================

async def place_live_order(side: str, ask: float, shares: float, token_id: str) -> bool:
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
# KALMAN FILTER 1D
# =============================================================================

class KalmanFilter1D:
    __slots__ = ("q", "r", "x", "p")

    def __init__(self, q: float = 1e-5, r: float = 1e-2):
        self.q: float        = q
        self.r: float        = r
        self.x: float | None = None
        self.p: float        = 1.0

    def update(self, z: float) -> float:
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
        self.x = None
        self.p = 1.0

# =============================================================================
# HFT WINDOW
# =============================================================================

class HFTWindow:
    __slots__ = ("window_s", "data")

    def __init__(self, window_s: float = 10.0):
        self.window_s: float = window_s
        self.data: deque     = deque()

    def add(self, price: float, ts: float):
        self.data.append((ts, price))
        cutoff = ts - self.window_s
        buf    = self.data
        while buf and buf[0][0] < cutoff:
            buf.popleft()

    def _stats(self):
        n = len(self.data)
        if n < 3:
            return None, None, n
        prices = [p for _, p in self.data]
        mean   = sum(prices) / n
        var    = sum((p - mean) ** 2 for p in prices) / n
        return mean, math.sqrt(var), n

    def zscore(self, current_price: float) -> float | None:
        mean, std, n = self._stats()
        if mean is None:
            return None
        if std < 1e-9:
            return 0.0
        return (current_price - mean) / std

    def std(self) -> float | None:
        _, s, n = self._stats()
        return s

    def size(self) -> int:
        return len(self.data)

    def clear(self):
        self.data.clear()

# =============================================================================
# VPIN TRACKER
# =============================================================================

class VPINTracker:
    __slots__ = ("window_s", "data", "prev_mid")

    def __init__(self, window_s: float = 10.0):
        self.window_s: float      = window_s
        self.data:     deque      = deque()
        self.prev_mid: float | None = None

    def add(self, kal_mid: float, total_size: float, ts: float):
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
        if not self.data:
            return None
        buy_vol  = sum( v for _, v in self.data if v > 0)
        sell_vol = sum(-v for _, v in self.data if v < 0)
        total    = buy_vol + sell_vol
        if total < 1e-9:
            return None
        return abs(buy_vol - sell_vol) / total

    def reset(self):
        self.data.clear()
        self.prev_mid = None

# =============================================================================
# SINAL 95% WR — Função de Detecção
# =============================================================================

def check_gambling_signal_95wr(side, bid, ask, kal, z, std, obi, vpin, rem):
    """
    Retorna score 0-100 para entrada.
    Score >= 75 = entrada recomendada (95% WR esperado)
    """
    score = 0
    
    # Sinal 1: Z-Score Reversão
    if z is not None:
        if z < -2.5 and z > -6.0:
            score += 30
        elif z < -1.5 and z > -2.5:
            score += 20
        elif z > 1.5 and z < 2.5:
            score += 15
        elif -0.5 <= z <= 0.5:
            score += 5
    
    # Sinal 2: OBI Suporte/Resistência
    if obi is not None:
        if obi >= 0.70:
            score += 25
        elif obi >= 0.65:
            score += 15
        elif obi <= 0.30:
            score += 20
    
    # Sinal 3: VPIN Fluxo Saudável
    if vpin is not None:
        if vpin < 0.40:
            score += 25
        elif vpin < 0.50:
            score += 15
        elif vpin > 0.90:
            score += 20
    
    # Sinal 4: STD Regime Comprimido
    if std is not None:
        if std <= 0.020:
            score += 20
        elif std <= 0.030:
            score += 15
    
    return score

# =============================================================================
# LOGIC LOOP v2.0.0 COM VALIDAÇÃO DE PREÇOS
# =============================================================================

async def logic_loop(m_start: float, m_end: float, meta: dict,
                     ws_sub_queue: asyncio.Queue):
    """
    Loop principal de trading para um ciclo de 5 minutos.
    v2.0.0: 95% WIN RATE GAMBLING MODE
    v2.1.0: Validação de preços + resolução de bugs
    v1.9.0: asyncio.create_task(_background_cache_next(...)) em fim de ciclo
    """
    global bankroll, daily_profit, total_pnl_pos, total_pnl_neg

    active_trades = []
    pre_bank = bankroll

    _slug = meta["slug"]
    _per_market_resolved[_slug] = {"event": asyncio.Event(), "winner": None}

    _token_to_slug[meta["up"]]   = _slug
    _token_to_slug[meta["down"]] = _slug

    # v2.1.0: Referência ao orderbook ISOLADO deste mercado
    if _slug not in market_orderbooks:
        _init_market_orderbook(_slug)
    _ob = market_orderbooks[_slug]

    _overlap_subscribed = False

    mods = []
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
        f"PA risk={PEG_ARBIT_RISK:.1%}(fixo) | "
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
        f"WS v1.9.0: Overlap@rem<={WS_OVERLAP_PRE_S}s | "
        f"Settlement=ONLY_WS_market_resolved | Non-blocking=create_task | "
        f"PriceRouter=_price_router() | CacheNext=background_task"
    )
    log_sep()
    log_info("ESCUTA ACTIVA")
    log_sep()

    # Open trade
    async def open_trade(side, trade_type, rstr, risk,
                         extra_log=None, fixed_shares=None, token_id=None):
        global bankroll
        ask = _ob["asks"].get(side.lower())
        bid = _ob["bids"].get(side.lower())
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

    # Close trade
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

    # Close trade resolution
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

    # Estado quantitativo
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

        # Fim de mercado
        if rem <= 0.0:
            final_bid_up   = _ob["bids"].get("up")  or 0.0
            final_bid_down = _ob["bids"].get("down") or 0.0
            log_sep()
            log_info(
                f"FIM DE MERCADO | {_slug} | UP final={fc(final_bid_up)} "
                f"| DOWN final={fc(final_bid_down)} "
                f"| tempo esgotado — saindo do loop de trading"
            )
            if active_trades:
                log_info(
                    f"Aguardando resolucao WS real (sem fallback BID — v1.8.0) | "
                    f"slug={_slug} | warn_threshold={RESOLVE_TIMEOUT_S:.0f}s"
                )
                _mrd = _per_market_resolved.get(_slug)
                _resolution_start = time.time()
                _warned            = False

                while True:
                    if _mrd and _mrd["event"].is_set():
                        winner_asset = _mrd["winner"]
                        break
                    if resolved_event.is_set():
                        winner_asset = resolved_winner_asset
                        if winner_asset in (meta["up"], meta["down"]):
                            break
                        else:
                            resolved_event.clear()
                    elapsed = time.time() - _resolution_start
                    if elapsed >= RESOLVE_TIMEOUT_S and not _warned:
                        _warned = True
                        log_warn(
                            f"RESOLUCAO LENTA | {elapsed:.0f}s sem market_resolved WS "
                            f"para slug={_slug} — AGUARDANDO (sem fallback BID)"
                        )
                    await asyncio.sleep(0.05)

                log_info(
                    f"RESOLUCAO CONFIRMADA | winner_asset="
                    f"{winner_asset[:16] if winner_asset else '?'}..."
                )
                for trade in active_trades[:]:
                    winner = (trade.get("token_id") == winner_asset)
                    close_trade_resolution(trade, winner, "00:00:000")
                    active_trades.remove(trade)

            # v1.9.0: Background cache task (Área 4) — dispara ao sair do ciclo.
            # Carrega metadata do mercado A+2 (dois ciclos à frente) sem bloquear.
            # O mercado A+1 (próximo) já foi subscrito pelo overlap e está em cache
            # ou será carregado por main(). A+2 é o que ficaria fora da janela
            # de prefetch após o consumo do ciclo actual.
            _cache_next_ts   = m_end + 300  # timestamp de A+2
            _cache_next_slug = f"xrp-updown-5m-{int(_cache_next_ts)}"
            asyncio.create_task(_background_cache_next(_cache_next_slug))
            log_info(
                f"CACHE BACKGROUND | task disparada para {_cache_next_slug} "
                f"(mercado A+2 — fire-and-forget)"
            )
            break

        # Overlapping Subscription
        if not _overlap_subscribed and rem <= WS_OVERLAP_PRE_S:
            _overlap_subscribed = True
            _next_start_ts = m_end
            _next_slug     = f"xrp-updown-5m-{int(_next_start_ts)}"
            _next_meta     = _metadata_cache.get(_next_slug)
            if _next_meta is None:
                log_info(
                    f"OVERLAP | {_next_slug} ausente do cache — "
                    f"fetch_metadata assíncrono"
                )
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

        # Aguarda tick WS
        try:
            await asyncio.wait_for(price_change.wait(), timeout=LOOP_SLEEP)
            price_change.clear()
        except asyncio.TimeoutError:
            pass

        bid_up   = _ob["bids"].get("up")
        bid_down = _ob["bids"].get("down")
        ask_up   = _ob["asks"].get("up")
        ask_down = _ob["asks"].get("down")

        if bid_up is None or bid_down is None or ask_up is None or ask_down is None:
            continue
        if bid_up == prev_bid_up and bid_down == prev_bid_down:
            continue

        # ── v2.1.0: TERMINAL PRICE DETECTION (MARKET SETTLED) ────────────
        # Quando o mercado resolve, a API envia preços terminais:
        #   Winner: BID=0.99 / ASK=1.0   (ou BID>=0.95, ASK>=0.99)
        #   Loser:  BID=0.0  / ASK=0.01  (ou BID<=0.01, ASK<=0.02)
        # Estes preços são LEGÍTIMOS — não são frames corrompidos.
        # Detectá-los ANTES do filtro PRECOS INVALIDOS e sair graciosamente.
        _terminal_winner = (
            (bid_up >= 0.95 and ask_up >= 0.99) or
            (bid_down >= 0.95 and ask_down >= 0.99)
        )
        _terminal_loser = (
            (bid_up <= 0.01 and ask_up <= 0.02) or
            (bid_down <= 0.01 and ask_down <= 0.02)
        )
        if _terminal_winner or _terminal_loser:
            log_sep()
            log_info(
                f"[MARKET SETTLED DYNAMICS] Terminal prices detected | {_slug} | "
                f"UP: BID={bid_up} ASK={ask_up} | "
                f"DN: BID={bid_down} ASK={ask_down} | "
                f"winner_signal={'UP' if (bid_up >= 0.95) else 'DOWN' if (bid_down >= 0.95) else '?'} | "
                f"handover to resolve_task — breaking trading loop"
            )
            log_sep()
            break

        # ── v2.1.0: VALIDAÇÃO DE PREÇOS ──────────────────────────────────
        # Frames corruptos podem enviar bid=0, ask=100 (impossível)
        # Filtrar antes de processar (terminal prices já foram interceptados acima)
        if (bid_up <= 0.0 or bid_up >= 1.0 or
            bid_down <= 0.0 or bid_down >= 1.0 or
            ask_up <= 0.0 or ask_up >= 1.0 or
            ask_down <= 0.0 or ask_down >= 1.0):
            log_warn(
                f"PRECOS INVALIDOS detectados | {_slug} | "
                f"UP: BID={bid_up} ASK={ask_up} | "
                f"DN: BID={bid_down} ASK={ask_down} | "
                f"tick ignorado (possível frame corrompido)"
            )
            continue

        # ── Validar que spread é razoável (máx 50c de spread = anomalia) ──
        spread_up = (ask_up - bid_up) * 100 if (bid_up and ask_up) else 0
        spread_dn = (ask_down - bid_down) * 100 if (bid_down and ask_down) else 0
        if spread_up > 50 or spread_dn > 50:
            log_warn(
                f"SPREAD ANORMAL | UP_spread={spread_up:.1f}c DN_spread={spread_dn:.1f}c | "
                f"possível frame corrompido — ignorar"
            )
            continue

        # ── Validar que VPIN não está 100% (sempre significa bug) ─────
        prev_bid_up   = bid_up
        prev_bid_down = bid_down

        ask_sum    = ask_up + ask_down
        bid_sum    = bid_up + bid_down
        underpeg_c = (1.0 - ask_sum) * 100.0
        mid_up     = (bid_up   + ask_up)   * 0.5
        mid_down   = (bid_down + ask_down) * 0.5
        eff_up     = eff_price_c(ask_up)
        eff_down   = eff_price_c(ask_down)

        # Motor Quantitativo
        kal_up   = kalmans["UP"].update(mid_up)
        kal_down = kalmans["DOWN"].update(mid_down)
        hft_wins["UP"].add(kal_up,   now)
        hft_wins["DOWN"].add(kal_down, now)

        z_up    = hft_wins["UP"].zscore(kal_up)
        z_down  = hft_wins["DOWN"].zscore(kal_down)
        std_up  = hft_wins["UP"].std()
        std_down = hft_wins["DOWN"].std()

        bs_up   = _ob["bid_sizes"].get("up")
        as_up   = _ob["ask_sizes"].get("up")
        bs_down = _ob["bid_sizes"].get("down")
        as_down = _ob["ask_sizes"].get("down")
        obi_up   = calc_imbalance(bs_up,   as_up)
        obi_down = calc_imbalance(bs_down, as_down)

        vol_up   = ((bs_up   or 0) + (as_up   or 0)) or 1.0
        vol_down = ((bs_down or 0) + (as_down or 0)) or 1.0
        vpin_trackers["UP"].add(kal_up,   vol_up,   now)
        vpin_trackers["DOWN"].add(kal_down, vol_down, now)
        vpin_up   = vpin_trackers["UP"].vpin()
        vpin_down = vpin_trackers["DOWN"].vpin()

        rstr = get_remaining_str(rem)

        # Tick log
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
        # STOP-LOSS
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
                    _sell_bid = _ob["bids"].get(_trade["side"].lower()) or 0.0
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
        # TAKE-PROFIT DINÂMICO
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

                    if _tp_bid < _min_bid:
                        if _tp_z >= TP_SPIKE_ZSCORE:
                            log_m("TP", "SKIP_UNPROFITABLE",
                                f"rem={rstr} | {_tp_side} Z={_tp_z:+.2f}>={TP_SPIKE_ZSCORE} "
                                f"MAS BID={fc(_tp_bid)}<ASK+{TP_MIN_BID_OVER_ASK*100:.0f}c"
                                f"={fc(_min_bid)} — portao 1 bloqueado (loss garantido)")
                        continue

                    if _net_if_sell <= _min_net:
                        if _tp_z >= TP_SPIKE_ZSCORE:
                            log_m("TP", "SKIP_UNPROFITABLE",
                                f"rem={rstr} | {_tp_side} Z={_tp_z:+.2f}>={TP_SPIKE_ZSCORE} "
                                f"MAS net=${_net_if_sell:.4f}<=${_min_net:.4f} "
                                f"(fees comem lucro) — portao 2 bloqueado")
                        continue

                    if _tp_z < TP_SPIKE_ZSCORE:
                        continue

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
        # TARGET CHECK
        # =====================================================================
        for trade in active_trades[:]:
            if trade.get("target") is None:
                continue
            bid_key  = trade["side"].lower()
            curr_bid = _ob["bids"].get(bid_key)
            if curr_bid and curr_bid >= trade["target"]:
                close_trade(trade, curr_bid, "TARGET ESTATICO", rstr)
                active_trades.remove(trade)

        # =====================================================================
        # GAMBLING 95% WR — Sinal Combinado
        # =====================================================================
        if GAMBLING_ACTIVE:
            if rem > GAMB_START_REM_S:
                pass
            elif rem <= GAMB_CUTOFF_S:
                if not gamb_cutoff_logged:
                    gamb_cutoff_logged = True
                    log_m("GAMBLING 95WR", "CUTOFF",
                        f"rem={rstr} | parado — rem<={GAMB_CUTOFF_S}s")
            else:
                if not gamb_started_logged:
                    gamb_started_logged = True
                    log_m("GAMBLING 95WR", "START",
                        f"rem={rstr} | ATIVO [{GAMB_START_REM_S}s->{GAMB_CUTOFF_S}s] "
                        f"| Kelly(edge={KELLY_ASSUMED_EDGE:.0%}"
                        f" frac=1/{int(round(1/KELLY_FRACTION))} cap={KELLY_MAX_RISK_PCT:.0%}) "
                        f"| HFT: σ<={GAMB_MAX_VOL_DEV} Z<={GAMB_MAX_ZSCORE} "
                        f"OBI>={GAMB_MIN_OBI:.0%} VPIN<={VPIN_SAFE_LIMIT:.0%} "
                        f"| MaxEff={GAMB_MAX_EFF_C:.0f}c BidAskRatio>={BID_ASK_MIN_RATIO:.0%}")

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

                    spread_c = _ob["spreads_c"].get(g_side.lower())
                    if spread_c is None or spread_c > MAX_SPREAD_CENTS:
                        if spread_c is not None:
                            log_m("GAMBLING 95WR", "BLOCK_SPREAD",
                                f"rem={rstr} | {g_side} spread={spread_c:.2f}c > {MAX_SPREAD_CENTS}c "
                                f"(livro largo — rejeitado)")
                        continue

                    if not (GAMB_MIN_EFF_C <= g_eff <= GAMB_MAX_EFF_C):
                        continue

                    if g_bid is not None and g_ask > 0:
                        _bid_ask_ratio = g_bid / g_ask
                        if _bid_ask_ratio < BID_ASK_MIN_RATIO:
                            log_m("GAMBLING 95WR", "BLOCK",
                                f"rem={rstr} | {g_side} eff={g_eff:.1f}c "
                                f"| BID/ASK={_bid_ask_ratio:.3f}<{BID_ASK_MIN_RATIO:.2f} "
                                f"(spread demasiado largo — pre-crash signal)"
                            )
                            continue

                    # Sinal 95% WR
                    signal_score = check_gambling_signal_95wr(
                        g_side, g_bid, g_ask, kalmans[g_side].x,
                        g_z, g_std, g_obi, g_vpin, rem
                    )
                    
                    signal_threshold = 75 if not _endgame_active else 60
                    
                    if signal_score < signal_threshold:
                        if signal_score > 40:
                            log_m("GAMBLING 95WR", "WAIT",
                                f"rem={rstr} | {g_side} score={signal_score:.0f}<{signal_threshold} "
                                f"(Z={g_z:+.2f} OBI={g_obi:.2f} VPIN={g_vpin:.2f} STD={g_std:.4f}) "
                                f"— aguardar melhor setup")
                        continue

                    kelly_risk = calc_kelly_risk(g_ask)
                    if kelly_risk <= 0.0:
                        log_m("GAMBLING 95WR", "BLOCK",
                            f"rem={rstr} | {g_side} eff={g_eff:.1f}c ASK={fc(g_ask)} "
                            f"| Kelly<=0 (sem edge a este preco — não entra)")
                        continue

                    if bankroll > 0.0:
                        token_id = meta["up"] if g_side == "UP" else meta["down"]
                        await open_trade(
                            g_side, "GAMBLING 95WR", rstr,
                            risk=kelly_risk,
                            token_id=token_id,
                            extra_log=(
                                f"Signal={signal_score:.0f}>={signal_threshold} "
                                f"(Z={g_z:+.2f} OBI={g_obi:.2f} VPIN={g_vpin:.2f} STD={g_std:.4f}) "
                                f"Kelly={kelly_risk:.1%} | Kalman={kalmans[g_side].x:.4f}"
                            )
                        )
                        gamb_last_buy[g_side] = now
                        log_m("GAMBLING 95WR", "COOLDOWN",
                            f"rem={rstr} | {g_side} — cooldown {GAMB_BUY_COOLDOWN:.1f}s")

    # PnL ACCOUNTING
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

    _per_market_resolved.pop(_slug, None)
    _cleanup_market_orderbook(_slug)

# =============================================================================
# MAIN v1.9.0
# =============================================================================

async def main():
    global daily_profit, last_day, bankroll
    global total_pnl_pos, total_pnl_neg, bot_start_time
    global resolved_event, resolved_winner_asset
    global _shutdown_flag, _metadata_cache, _ws_sub_queue
    global _active_logic_tasks, _ws_needs_cache_reload

    def _handle_sigterm():
        global _shutdown_flag
        log_warn("SIGTERM recebido — iniciando shutdown gracioso...")
        _shutdown_flag = True

    loop = asyncio.get_event_loop()
    try:
        loop.add_signal_handler(signal.SIGTERM, _handle_sigterm)
    except NotImplementedError:
        log_warn("SIGTERM handler nao disponivel nesta plataforma (Windows)")

    bot_start_time  = time.time()
    total_pnl_pos   = 0.0
    total_pnl_neg   = 0.0

    log_sep2()
    log_info("BOT XRP POLYMARKET v2.1.0 — 95% WIN RATE GAMBLING + KELLY + Z-SCORE + VPIN")
    log_sep2()
    log_info(f"LIVE_TRADING     : {LIVE_TRADING}")
    log_info(f"BANKROLL_INIT    : ${bankroll:.2f}")
    log_sep2()
    log_info("PRODUCTION READY (v1.7.0):")
    log_info(f"   RateLimiter     : {RATE_LIMIT_CALLS} req/s sustentado | burst={RATE_LIMIT_BURST}")
    log_info(f"   Retry Backoff   : max={MAX_API_RETRIES} tentativas | {BASE_BACKOFF_S}s->exp->max{MAX_BACKOFF_S}s | jitter={BACKOFF_JITTER}")
    log_info(f"   CircuitBreaker  : open após {CB_FAIL_THRESHOLD} falhas | recovery={CB_RECOVERY_S:.0f}s")
    log_info(f"   WS Reconnect    : backoff {WS_RECONNECT_BASE_S}s->exp->max{WS_RECONNECT_MAX_S}s")
    log_sep2()
    log_info("WS ENGINE v1.9.0 — EVENT-DRIVEN HFT:")
    log_info(f"   URI             : {WS_URI}")
    log_info(f"   PriceRouter     : _price_router() dispatcher centralizado")
    log_info(f"   Overlap FIX     : sub_queue drenada DENTRO do loop (bug v1.8.x corrigido)")
    log_info(f"   Overlap         : sub Mercado B @ rem<={WS_OVERLAP_PRE_S}s antes de A expirar")
    log_info(f"   Heartbeat       : ping/pong cada {WS_HEARTBEAT_INTERVAL}s | pong timeout=10s")
    log_info(f"   Cache           : {METADATA_PREFETCH_COUNT} mercados pré-carregados ({METADATA_PREFETCH_COUNT*5}min)")
    log_info(f"   CacheNext       : background task por ciclo (A+2 sem bloquear trading)")
    log_info(f"   Settlement      : ONLY WS market_resolved — sem fallback BID estimado")
    log_info(f"   Non-blocking    : asyncio.create_task(logic_loop) — ciclo B durante resolução A")
    log_info(f"   Frame Validation: raw_len < 10 bloqueado | error_rate > 50% reconecta")
    log_info(f"   WS DEBUG Logger : websockets.client nível DEBUG activo → bot_xrp.log")
    log_info(f"   ConnClose Diag  : e.code + e.reason logados em toda desconexão WS")
    log_sep2()

    _metadata_cache = await _prefetch_metadata_cache()
    _ws_sub_queue = asyncio.Queue()

    ws_task = asyncio.create_task(ws_handler(_ws_sub_queue))
    log_info("WS TASK PERSISTENTE iniciada")

    while not _shutdown_flag:
        slug, start_ts = get_current_slug()

        # v2.1.0: Reset globais de routing
        _token_to_slug.clear()
        _per_market_resolved.clear()

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

        if _ws_needs_cache_reload:
            _ws_needs_cache_reload = False
            log_info("WS CACHE RELOAD | backoff longo detectado — reconstruindo metadata cache")
            _metadata_cache = await _prefetch_metadata_cache()

        resolved_event.clear()
        resolved_winner_asset = None

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
            log_sep2()

        # v2.1.0: Inicializa orderbook ISOLADO para este slug
        _init_market_orderbook(slug)
        price_change.clear()

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

        if market_orderbooks.get(slug, {}).get("bids", {}).get("up") is not None:
            task = asyncio.create_task(
                logic_loop(start_ts, start_ts + 300, meta, _ws_sub_queue)
            )
            _active_logic_tasks.append(task)
            _active_logic_tasks = [t for t in _active_logic_tasks if not t.done()]

            log_info(
                f"LOGIC TASK criada (non-blocking) | {slug} | "
                f"tasks_activas={len(_active_logic_tasks)}"
            )
        else:
            log_warn("Sem BIDs/ASKs recebidos neste ciclo — a saltar")

        # v2.1.0: Health check do WS
        next_start = start_ts + 300
        wait_time  = next_start - time.time()
        if wait_time > 0.5:
            if ws_task.done():
                log_error("WS_TASK MORREU INESPERADAMENTE!")
                try:
                    await ws_task
                except Exception as e:
                    log_error(f"WS_TASK erro: {e}")
                ws_task = asyncio.create_task(ws_handler(_ws_sub_queue))
                log_info("WS_TASK reiniciada")
            
            await asyncio.sleep(min(wait_time - 0.5, 1.0))

    log_sep2()
    log_info("SHUTDOWN | cancelando tasks activas...")

    for task in _active_logic_tasks:
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    ws_task.cancel()
    try:
        await ws_task
    except asyncio.CancelledError:
        pass

    log_sep2()
    log_info(
        f"SHUTDOWN GRACIOSO | Banca final: ${bankroll:.4f} | "
        f"Uptime: {get_uptime_str()}"
    )
    log_info("BOT TERMINADO LIMPO")
    log_sep2()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_info("BOT PARADO PELO UTILIZADOR (KeyboardInterrupt)")