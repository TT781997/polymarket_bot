
#=============================================================================
#CHANGELOG v1.0.0 — SPREAD API OPTIMIZATION & REFACTORING
#=============================================================================
#
#[v1.0.0] [CRITICAL] Parametrização de Risco refatorizada:
#          GAMBLING_RISK = 0.03 (reduzido de 0.15)
#          SPREAD_CATCH_RISK = 0.05 (reduzido de 0.25)
#          MAX_RISK_PERCENT = 0.20 (reduzido de 0.50)
#          MAX_RISK_MULT = 8 (mantido)
#          SC_TRIGGER_SUM = 0.960 (reduzido de 0.992)
#          GAMB_MIN_EFF_C = 85.0 | GAMB_MAX_EFF_C = 95.0 (range mais restritivo)
#          GAMB_SPREAD_MAX_PCT = 1.5 (reduzido de -20.0, agora activo)
#          GAMB_NEUTRAL_BOTH = False (antes True — trend NEUTRAL bloqueia)
#          TREND_FIDELITY = 60 (aumentado de 10)
#          TREND_THRESHOLD = 0.015 (aumentado de 0.010)
#          RECOVERY_ROUNDS_STEP = 10 (novo parâmetro)
#
#[v1.0.0] [CRITICAL] Sistema de Logging exclusivo em ficheiro:
#          Removidos todos print() das funcoes: log_m, log_info, log_warn, log_raw, log_sep, log_sep2
#          FileHandler renomeado: 'bot_xrp.log' -> 'polymarket_bot_v1.0.0.log'
#          Bot corre silenciosamente — zero output no terminal
#          Apenas logger.info() e logger.warning() permanecem activos
#
#[v1.0.0] [CRITICAL] Correção da API de Tendência (Trend 1H):
#          fetch_trend_from_clob() refatorizado:
#          - Parametros correctos: fidelity=TREND_FIDELITY, interval=TREND_INTERVAL
#          - Delta calculo robusto: last_avg - first_avg vs TREND_THRESHOLD (0.015)
#          - Divide historico em tercos (1/3 primeiro, 1/3 ultimo)
#          - Retorna UP/DOWN/NEUTRAL com confianca aumentada
#          - Refs: https://docs.polymarket.com/trading/orderbook#price-history
#
#[v1.0.0] [CRITICAL] Resolução de NameError (RECOVERY_ROUNDS_STEP):
#          RECOVERY_ROUNDS_STEP = 10 adicionado na secção de Parametros
#          Corrige crash na linha 1507 (martingale recovery calculation)
#          Stop Loss anti-dump: SL_THRESHOLD = 0.30 (30c), SL_TICKS = 5
#          Ignora wicks momentâneos < 30c — requer 5 ticks consistentes
#
#[v1.0.0] [CRITICAL] Correção Arquitetural do SPREAD CATCH:
#          ask_sum = ask_up + ask_down (ASK sum, nao BID sum)
#          Gatilho entrada: if ask_sum <= SC_TRIGGER_SUM (antes: bid_sum)
#          underpeg_c = (1.0 - ask_sum) * 100.0 (gap real baseado em ASK)
#          Mantém filtro: SC_SPREAD_MAX_PCT = 2.0 para liquidez
#          ref_ask = max(ask_up, ask_down) para budget suficiente
#          Log display: ASK_SUM={ask_sum:.4f} gap={underpeg_c:.2f}c
#
#[v1.0.0] [CRITICAL] Otimização de Spread via API Nativa Polymarket:
#          fetch_market_spread(token_id) — novo endpoint integration
#          GET /spread?token_id={token_id}
#          Captura: spread (decimal), bid (best buy), ask (best sell)
#          Cache local com TTL: SPREAD_API_CACHE_TTL_S = 0.5s
#          Timeout HTTP: SPREAD_API_TIMEOUT_S = 2.0s
#          Prioridade: API cache > calculo manual (fallback robusto)
#          Task paralelo em background: fetch_spreads_task() refresh 0.4s
#          get_spread_from_cache_or_calculate() — interface unificada
#          Log detalhado: SPREAD_API.FETCH | token | bid | ask | spread%
#
#[v1.0.0] [CRITICAL] Precisão Decimal conforme Polymarket:
#          to_decimal(val, precision=8) — conversão de alta precisao
#          ROUND_HALF_UP para arredondamento justo
#          Suporta float, str, Decimal como input
#          Mantém 8 casas decimais (padrao Polymarket)
#
#[v1.0.0] [CRITICAL] Integracao da API de Spread no WS Handler:
#          ws_handler() — novo parametro: Spread API integration: ON
#          Task paralelo fetch_spreads_task() — executa em background
#          Atualiza best_spreads_pct com dados da API
#          Fallback automatico ao compute_spread_pct(bid, ask)
#          Zero interrupcao — cache sempre disponivel
#
#[v1.0.0] [CRITICAL] Tratamento de Erros Robusto (Spread API):
#          requests.Timeout -> log_warn + cache fallback
#          requests.RequestException -> log_warn + cache fallback
#          json.JSONDecodeError -> log_warn + calculo manual
#          KeyError/TypeError (fields incompletos) -> log_warn + fallback
#          Nenhuma ordem é bloqueada por falha de API
#
#[v1.0.0] [BUY correctamente ao ASK (nao ao BID):
#          docs: "you'll pay the ask when buying"
#          invested_pure / ask_price = shares (correcto)
#          eff_c = ask * (1 + fee_rate(ask)) * 100 (custo all-in)
#
#[v1.0.0] [SELL correctamente ao BID (nao ao ASK):
#          docs: "receive the bid when selling"
#          payout_bruto = shares * bid_actual (sem alteracao)
#
#[v1.0.0] [RESOLUCAO de mercado via WS event market_resolved:
#          winning_asset_id -> vencedor recebe $1.00/share
#          perdedor recebe $0.00 (tokens worthless)
#          Fallback (timeout 35s): BID mais alto = vencedor estimado
#          LIVE: redeem via SDK redeemPositions()
#
#[v1.0.0] [feat] Martingale reestruturado:
#          MAX_RISK_PERCENT = 20% (CAP INVIOLAVEL)
#          MAX_RISK_MULT = x8
#          Extra stake = 50% * accumulated_loss (dolares fixos)
#          Formula: min(base*mult*bank + 50%*acc_loss, MAX_RISK_PERCENT*bank)
#          GAMBLING base=3% | SPREAD_CATCH base=5%
#
#[v1.0.0] [feat] Novo formato de log completo:
#          STARTUP: configuracao inicial bot
#          STOPLOSS: monitorizacao por tick com niveis descendentes
#          FIM: resolucao ganha/perdida
#          ROUND: PnL parcial + preview proximo mult
#          TOTAL: PnL daily + banca + uptime
#
#[v1.0.0] [feat] STOPLOSS MONITOR — Task independente:
#          BID monitorizado a cada SL_CHECK_S seg (1.0s)
#          5 ticks consecutivos BID < SL_THRESHOLD -> dispara
#          Log individual por tick mostrando descida de niveis
#          Anti-dump: ignora wicks momentâneos < 30c
#          Display: rem | side | bid_c | nivel novo | tick counter
#
#[v1.0.0] [feat] FIM DE MERCADO — Logs de Resolucao:
#          RESOLUCAO GANHA: winner_asset_id -> $1/share
#          RESOLUCAO PERDIDA: losing side -> $0.00
#          Estimativa fallback (se timeout WS): BID_UP vs BID_DOWN
#
#[v1.0.0] [feat] Deltas com Threshold Absoluto (Gambling):
#          D0.5s activo se |delta| >= 5c (GAMB_D05_THRESH_C)
#          D1.0s activo se |delta| >= 7c (GAMB_D10_THRESH_C)
#          D1.5s activo se |delta| >= 9c (GAMB_D15_THRESH_C)
#          D2.0s activo se |delta| >= 11c (GAMB_D20_THRESH_C)
#          So bloqueiam entrada se |delta| >= threshold E direcao negativa/pump
#          Display individual de deltas activos no log GAMBLING.WATCH
#
#[v1.0.0] [feat] Trend NEUTRAL Behavior Tuning:
#          GAMB_NEUTRAL_BOTH = False (default) — trend NEUTRAL bloqueia entrada
#          Se True: permite entrada em ambos os lados quando trend=NEUTRAL
#          Configuravel conforme perfil de risco
#
#[v1.0.0] [feat] Spread API Bootstrap Message:
#          Log de startup mostra integracao: "Spread API integration: ON"
#          Details: Endpoint | Cache TTL | Timeout | Prioridade fallback
#
#[v1.0.0] [fix] recovery_rounds Calculation:
#          +RECOVERY_ROUNDS_STEP (10) por perda
#          -1 por lucro
#          Display no log: "Rounds restantes: {recovery_rounds}"
#          Martingale mult mantido ate recuperacao completa
#
#[v1.0.0] [fix] Separador MARTINGALE|RECOVERY antes do ROUND:
#          Se lucro + recovery activo: primeiro log MARTINGALE (recovery parcial)
#          Depois log ROUND (PnL detalhado)
#          Clarity: recuperacao vs novo ciclo
#
#[v1.0.0] [fix] Spread Cache Invalidation:
#          Cache expira apos TTL
#          Task background refresh 80% de TTL (0.4s de 0.5s)
#          Zero delay para trades criticos
#
#[v1.0.0] [refactor] WS Handler Async Task Management:
#          ws_handler() + fetch_spreads_task() paralelo
#          Cleanup: spread_task.cancel() + await na exceptaoa
#          Nenhum memory leak
#
#[v1.0.0] [refactor] Token ID Dinamico (Spread API):
#          token_id passado como parametro
#          Suporta ambos os lados: 'up' | 'down'
#          Cache estruturado por token: spread_api_cache['up'] | spread_api_cache['down']
#
#[v1.0.0] [docs] Referencia Tecnica Spread API:
#          GET /spread (Polymarket docs trading/orderbook#spread)
#          Query params: token_id
#          Response: {spread, bid, ask, ts}
#
#=============================================================================
#CHANGELOG v0.38.0
#=============================================================================
#
#[v0.38.0] Fee formula documentada contra docs Polymarket (trading/fees)
#          fee_rate(p)=0.25*(p*(1-p))^2; BUY em shares; SELL em USDC
#[v0.38.0] fetch dinamico de fee_rate_bps via GET /fee-rate?token_id={id}
#[v0.38.0] Changelogs completos desde v0.35; auditoria 56/56 checks OK
#
#=============================================================================
#CHANGELOG v0.37.0
#=============================================================================
#
#[v0.37.0] SPREAD CATCH (ex PEG ARBIT): bid_up+bid_down<=0.992 + spread<2%
#[v0.37.0] GAMBLING (ex EIGHTY): direcional via CLOB Price History 1h XRP
#[v0.37.0] WS: book + best_bid_ask + price_change + market_resolved (v1.0.0)
#[v0.37.0] best_spreads_pct por lado; midpoint calculado inline
#
#=============================================================================
#CHANGELOG v0.36.0
#=============================================================================
#
#[v0.36.0] Stop-Loss: 5 ticks BID<0.27 -> venda imediata; task independente
#[v0.36.0] Martingale hibrido + Recovery Suave; Hard cap 15% inviolavel
#[v0.36.0] LIVE banca real; DEMO banca persistente 10 USDC
#
#=============================================================================
#CHANGELOG v0.35.0
#=============================================================================
#
#[v0.35.0] Martingale cap MAX_RISK_PERCENT; calc_risk() reutilizavel
#[v0.35.0] PEG calculado pelo preco Efectivo (eff), nao pelo preco base

import asyncio
import websockets
import json
import time
import logging
import requests
import os
from datetime import datetime
from collections import deque
from decimal import Decimal, ROUND_HALF_UP

# =============================================================================
# PARAMETROS CONFIGURÁVEIS
# =============================================================================

# --- MODO DE OPERACAO ---
LIVE_TRADING    = False   # True=ordens reais (requer secrets.txt); False=simulacao DEMO [Range: False | True]

# --- BANCA ---
BANKROLL_DEMO   = 10.0    # Banca DEMO em USDC; persistente, nunca reseta entre dias [Range: 1.0 | 100000.0]

# --- RISCO BASE ---
GAMBLING_RISK     = 0.03  # Risco base Gambling: fraccao da banca actual por trade [Range: 0.01 | 0.50]
SPREAD_CATCH_RISK = 0.05  # Risco base Spread Catch: fraccao da banca (budget total ambos os lados) [Range: 0.01 | 0.50]
MAX_RISK_PERCENT  = 0.20  # Hard cap INVIOLAVEL: investimento total nunca excede 20% da banca [Range: 0.10 | 0.50]
MAX_RISK_MULT     = 8     # Multiplicador martingale maximo (1->2->4->8) [Range: 2 | 16]
RECOVERY_ROUNDS_STEP = 10 # Rondas de recovery adicionadas por perda [Range: 1 | 50]

# --- TOGGLES ---
SPREAD_CATCH_ACTIVE = True  # Spread Catch activo [Range: False | True]
GAMBLING_ACTIVE     = True  # Gambling activo [Range: False | True]
STOP_LOSS_ACTIVE    = True  # Stop-Loss activo [Range: False | True]

# --- SPREAD CATCH ---
SC_TRIGGER_SUM    = 0.960       # Gatilho: entra se ask_up+ask_down <= valor [Range: 0.950 | 0.999]
SC_SPREAD_MAX_PCT = 2.0         # Spread maximo em % por lado (ASK-BID)/mid*100 [Range: 0.5 | 10.0]
SC_COOLDOWN       = 0.05        # Intervalo minimo entre entradas SC consecutivas (seg) [Range: 0.01 | 5.0]
SC_MIN_REM        = 5.0         # Remaining minimo para entrar no SC (seg) [Range: 1.0 | 30.0]
SC_TARGET_BID_C   = 0.0         # Target de venda antecipada ao BID (cents; 0=hold ate resolucao) [Range: 0.0 | 99.0]
MAX_SC_ENTRIES    = 10_000_000  # Entradas maximas SC por ciclo [Range: 1 | 10000000]

# --- GAMBLING ---
GAMB_START_REM_S      = 290     # Activa Gambling quando remaining <= X seg [Range: 60 | 300]
GAMB_CUTOFF_S         = 5       # Para Gambling quando remaining <= X seg [Range: 0 | 30]
GAMB_MIN_EFF_C        = 85.0    # eff_c minimo para entrada (cents); eff_c=ask*(1+fee_rate(ask))*100 [Range: 50.0 | 95.0]
GAMB_MAX_EFF_C        = 95.0    # eff_c maximo para entrada (cents) [Range: 82.0 | 99.9]
GAMB_MIN_TICKS        = 5       # Niveis unicos de eff_c (arredond. 0.5c) para confirmar consolidacao [Range: 2 | 20]
GAMB_VOL_MAX_C        = 4.5     # Variacao maxima de eff_c na janela de volatilidade (cents) [Range: 1.0 | 20.0]
GAMB_VOL_WINDOW_S     = 5.0     # Janela temporal para calcular volatilidade (seg) [Range: 1.0 | 30.0]
GAMB_VOL_COOLDOWN_S   = 5.0     # Cooldown apos volatilidade excessiva (seg) [Range: 1.0 | 30.0]
GAMB_BUY_COOLDOWN     = 4.0     # Cooldown entre compras do mesmo lado (seg) [Range: 0.5 | 30.0]
GAMB_SPREAD_MAX_PCT   = 1.5     # Spread max Gambling; valor negativo=filtro desactivado [Range: -20.0 | 100.0]
GAMB_PEG_MIN          = 0.970   # Soma minima bid_up+bid_down para entrar [Range: 0.90 | 0.999]
GAMB_NEUTRAL_BOTH     = False   # Trend NEUTRAL: permite entrada em ambos os lados [Range: False | True]
GAMB_TARGET_BID_C     = 0.0     # Target de venda ao BID (cents; 0=hold ate resolucao) [Range: 0.0 | 99.0]

# --- GAMBLING: DELTA THRESHOLDS (activam apenas se |delta| >= threshold) ---
GAMB_D05_THRESH_C  = 5.0   # Threshold absoluto em cents para D0.5s ser considerado activo [Range: 1.0 | 20.0]
GAMB_D10_THRESH_C  = 7.0   # Threshold absoluto em cents para D1.0s ser considerado activo [Range: 1.0 | 20.0]
GAMB_D15_THRESH_C  = 9.0   # Threshold absoluto em cents para D1.5s ser considerado activo [Range: 1.0 | 20.0]
GAMB_D20_THRESH_C  = 11.0  # Threshold absoluto em cents para D2.0s ser considerado activo [Range: 1.0 | 30.0]
GAMB_PUMP_THRESH_C = 3.5   # Delta positivo acima deste valor em GAMB_PUMP_TIME_S = pump [Range: 0.5 | 10.0]
GAMB_PUMP_TIME_S   = 1.5   # Janela temporal para detectar pump (seg) [Range: 0.5 | 5.0]

# --- TREND 1H XRP ---
TREND_UPDATE_S  = 60.0  # Intervalo de actualizacao do trend (seg) [Range: 30.0 | 300.0]
TREND_FIDELITY  = 60    # Granularidade do historico de precos [Range: 5 | 60]
TREND_THRESHOLD = 0.015 # Variacao minima entre tercos para classificar UP/DOWN [Range: 0.003 | 0.050]
TREND_INTERVAL  = "1h"  # Intervalo CLOB Price History [Range: "1h" | "max"]

# --- STOP-LOSS ---
SL_THRESHOLD = 0.30  # BID abaixo deste nivel inicia contagem de ticks SL [Range: 0.01 | 0.50]
SL_TICKS     = 5     # Ticks consecutivos abaixo do threshold para disparar SL [Range: 1 | 20]
SL_CHECK_S   = 1.0   # Intervalo de verificacao do SL (seg) [Range: 0.1 | 5.0]

# --- SPREAD API ---
SPREAD_API_CACHE_TTL_S = 0.5   # Cache local do resultado da API /spread (seg) [Range: 0.1 | 5.0]
SPREAD_API_TIMEOUT_S   = 2.0   # Timeout para chamada ao endpoint /spread [Range: 0.5 | 10.0]

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
RESOLVE_TIMEOUT_S = 35.0    # Tempo maximo a aguardar evento market_resolved WS (seg) [Range: 10.0 | 120.0]

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

# Microestrutura em tempo real (actualizados por tick WS)
best_bids        = {'up': None, 'down': None}  # Melhor BID por lado (preco de VENDA) [Range: 0.0 | 1.0]
best_asks        = {'up': None, 'down': None}  # Melhor ASK por lado (preco de COMPRA) [Range: 0.0 | 1.0]
best_spreads_pct = {'up': None, 'down': None}  # Spread % = (ask-bid)/mid*100 por lado [Range: 0.0 | 100.0]

# Cache local da API /spread
spread_api_cache = {
    'up':   {'spread': None, 'bid': None, 'ask': None, 'timestamp': 0.0},
    'down': {'spread': None, 'bid': None, 'ask': None, 'timestamp': 0.0}
}

price_change     = asyncio.Event()
bot_start_time   = time.time()

# Trend XRP 1h
xrp_1h_trend    = 'NEUTRAL'  # UP / DOWN / NEUTRAL [Range: str]
xrp_1h_token_up = None       # Token ID UP do mercado XRP 1h (cacheado) [Range: None | str]

# Resolucao do mercado actual (actualizados pelo WS)
resolved_event        = asyncio.Event()   # Set quando WS envia market_resolved
resolved_winner_asset = None              # winning_asset_id do evento WS [Range: None | str]

# Martingale + Recovery
risk_multiplier  = 1.0  # Multiplicador actual [Range: 1.0 | 8.0]
accumulated_loss = 0.0  # Perdas acumuladas desde o ultimo lucro ($) [Range: 0.0 | inf]
recovery_rounds  = 0    # Rondas de recovery restantes (para display) [Range: 0 | inf]

# PnL global
total_pnl_pos = 0.0
total_pnl_neg = 0.0

# =============================================================================
# LOGGING
# =============================================================================

_fmt  = logging.Formatter('%(message)s')
_fh   = logging.FileHandler('polymarket_bot_v1.0.0.log', encoding='utf-8')
_fh.setFormatter(_fmt)
logger = logging.getLogger('bot_xrp')
logger.setLevel(logging.DEBUG)
logger.addHandler(_fh)
logger.propagate = False

# =============================================================================
# FORMATACAO
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

def to_decimal(val: float | str | Decimal, precision: int = 8) -> Decimal:
    """
    Converte valor para Decimal com precisao configuravel.
    Mantém precisão conforme padrões Polymarket.
    """
    if isinstance(val, Decimal):
        return val
    d = Decimal(str(val))
    if precision > 0:
        quantize_str = '0.' + '0' * precision
        return d.quantize(Decimal(quantize_str), rounding=ROUND_HALF_UP)
    return d

def log_m(module: str, action: str, msg: str):
    logger.info(f"[INFO] [{module}] [{action}] [{get_ts()}] | {msg}")

def log_info(msg: str):
    out = f"[INFO] [{get_ts()}] | {msg}"
    logger.info(out)

def log_warn(msg: str):
    out = f"[WARN] [{get_ts()}] | {msg}"
    logger.warning(out)

def log_raw(msg: str):
    out = f"[{get_ts()}] | {msg}"
    logger.info(out)

def log_sep():
    s = "-" * 80
    logger.info(s)

def log_sep2():
    s = "=" * 80
    logger.info(s)

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

clob_client = None
if LIVE_TRADING:
    if not POLYMARKET_PRIVATE_KEY:
        logger.error(f"[ERROR] [{get_ts()}] | FATAL: LIVE_TRADING=True mas chave ausente!")
        raise SystemExit(1)
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs
        from py_clob_client.order_builder.constants import BUY as SDK_BUY
        clob_client = ClobClient(host=CLOB_REST_URL, key=POLYMARKET_PRIVATE_KEY, chain_id=137)
        log_info("SDK Polymarket carregado — LIVE TRADING ACTIVO")
    except ImportError:
        logger.error(f"[ERROR] [{get_ts()}] | py-clob-client nao instalado!")
        raise SystemExit(1)

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
    Usado para filtragem de ranges no Gambling (spec: comparacoes com eff).
    """
    return ask * (1.0 + fee_rate(ask)) * 100.0

def sell_payout_net(shares: float, bid: float) -> float:
    """
    Payout liquido ao VENDER 'shares' ao BID (preco real de venda).
    docs: 'you'll receive the bid when selling'
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

def compute_spread_pct(bid: float, ask: float) -> float | None:
    """
    Spread em % relativo ao midpoint (docs trading/orderbook#spreads).
    spread_pct = (ask - bid) / midpoint * 100
    midpoint   = (bid + ask) / 2
    """
    if bid is None or ask is None or bid <= 0.0 or ask <= 0.0 or bid >= ask:
        return None
    return (ask - bid) / ((bid + ask) * 0.5) * 100.0

def calc_effective_risk(
    base: float,
    mult: float,
    bank: float,
    acc_loss: float
) -> tuple[float, float, float]:
    """
    Calcula risco efectivo, extra stake e investimento maximo para o trade.

    Formula (v1.0.0):
        extra_stake_usd  = 50% * accumulated_loss         (dolares fixos)
        base_invest_usd  = bank * base * mult
        total_invest_usd = min(base_invest_usd + extra_stake_usd, bank * MAX_RISK_PERCENT)
        risk_ratio       = total_invest_usd / bank

    Display no log: "{base*mult*100:.1f}%+${extra_stake:.3f} (50% acc_loss)"

    Retorna: (risk_ratio, extra_stake_usd, base_pct_display)
        risk_ratio      = fraccao da banca a investir (respeita cap)
        extra_stake_usd = 50% * acc_loss em dolares
        base_pct        = base * mult (para display no log, antes de cap)
    """
    if bank <= 0.0:
        return MAX_RISK_PERCENT, 0.0, base * mult
    extra_stake_usd  = 0.50 * acc_loss
    base_invest      = bank * base * mult
    total_invest     = min(base_invest + extra_stake_usd, bank * MAX_RISK_PERCENT)
    risk_ratio       = total_invest / bank
    base_pct         = base * mult              # para display (sem cap)
    return min(risk_ratio, MAX_RISK_PERCENT), extra_stake_usd, base_pct

# =============================================================================
# API HELPERS — SPREAD NATIVO
# =============================================================================

def fetch_market_spread(token_id: str, use_cache: bool = True) -> dict | None:
    """
    Fetch do endpoint nativo /spread da Polymarket para obter spread, bid e ask.
    
    Refs: https://docs.polymarket.com/trading/orderbook#spread
    GET /spread?token_id={token_id}
    
    Response exemplo:
    {
        "spread": 0.002,        # spread em decimais (0.2%)
        "bid": 0.498,          # melhor BID (preco de venda)
        "ask": 0.500,          # melhor ASK (preco de compra)
        "ts": 1699564800
    }
    
    Cache local com TTL = SPREAD_API_CACHE_TTL_S para minimizar latencia.
    Fallback: calcula spread manualmente via compute_spread_pct se API falhar.
    
    Retorna dict com chaves: {'spread', 'bid', 'ask'} ou None se erro.
    """
    if use_cache:
        cache = spread_api_cache.get(token_id if token_id in ['up', 'down'] else None)
        if cache is not None:
            age = time.time() - cache.get('timestamp', 0.0)
            if age < SPREAD_API_CACHE_TTL_S and cache['spread'] is not None:
                return {
                    'spread': cache['spread'],
                    'bid': cache['bid'],
                    'ask': cache['ask']
                }
    
    try:
        r = requests.get(
            f"{CLOB_REST_URL}/spread",
            params={"token_id": token_id},
            timeout=SPREAD_API_TIMEOUT_S
        )
        if r.status_code != 200:
            log_warn(f"fetch_market_spread: HTTP {r.status_code} para token {token_id[:12]}...")
            return None
        
        data = r.json()
        
        # Extrai campos com validacao
        spread_val = data.get("spread")
        bid_val    = data.get("bid")
        ask_val    = data.get("ask")
        
        if spread_val is None or bid_val is None or ask_val is None:
            log_warn(f"fetch_market_spread: campos incompletos em resposta para token {token_id[:12]}...")
            return None
        
        # Converte para float com precisao Polymarket (Decimal)
        spread_dec = to_decimal(spread_val, precision=6)
        bid_dec    = to_decimal(bid_val, precision=8)
        ask_dec    = to_decimal(ask_val, precision=8)
        
        result = {
            'spread': float(spread_dec),
            'bid': float(bid_dec),
            'ask': float(ask_dec)
        }
        
        # Atualiza cache
        if token_id in spread_api_cache:
            spread_api_cache[token_id].update({
                'spread': result['spread'],
                'bid': result['bid'],
                'ask': result['ask'],
                'timestamp': time.time()
            })
        
        log_m('SPREAD_API', 'FETCH',
            f"token={token_id[:12]}... | bid={fc(result['bid'])} "
            f"| ask={fc(result['ask'])} | spread={result['spread']:.4f} ({result['spread']*100:.2f}%)"
        )
        
        return result
    
    except requests.Timeout:
        log_warn(f"fetch_market_spread: timeout {SPREAD_API_TIMEOUT_S}s para token {token_id[:12]}...")
        return None
    except requests.RequestException as e:
        log_warn(f"fetch_market_spread: erro request para token {token_id[:12]}...: {e}")
        return None
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        log_warn(f"fetch_market_spread: erro parsing resposta para token {token_id[:12]}...: {e}")
        return None

def get_spread_from_cache_or_calculate(token_id: str, bid: float, ask: float) -> float | None:
    """
    Obtem spread a partir do cache da API /spread ou calcula via compute_spread_pct.
    
    Prioridade:
    1. Cache local da API /spread (se disponivel e recente)
    2. Calculo manual via (ask - bid) / midpoint * 100
    3. None se ambos falharem
    """
    if token_id in spread_api_cache:
        cache = spread_api_cache[token_id]
        age = time.time() - cache.get('timestamp', 0.0)
        if age < SPREAD_API_CACHE_TTL_S and cache['spread'] is not None:
            return cache['spread'] * 100.0  # converte para percentagem
    
    # Fallback: calcula manualmente
    return compute_spread_pct(bid, ask)

# =============================================================================
# API HELPERS — RESTANTES
# =============================================================================

def fetch_metadata(slug: str) -> dict | None:
    try:
        data = requests.get(f"{GAMMA_API_URL}/events?slug={slug}", timeout=5).json()[0]['markets'][0]
        ids  = json.loads(data['clobTokenIds'])
        return {'id': data['conditionId'], 'up': ids[0], 'down': ids[1], 'slug': slug}
    except Exception as e:
        log_warn(f"fetch_metadata falhou ({slug}): {e}")
        return None

def fetch_fee_rate_bps(token_id: str) -> int:
    """
    Fetch dinamico de fee_rate_bps antes de cada ordem LIVE.
    docs: 'Always fetch fee_rate_bps dynamically — do not hardcode.'
    GET /fee-rate?token_id={token_id}
    """
    try:
        r = requests.get(f"{CLOB_REST_URL}/fee-rate", params={"token_id": token_id}, timeout=4)
        return int(r.json().get("fee_rate_bps", 0))
    except Exception as e:
        log_warn(f"fetch_fee_rate_bps falhou ({token_id[:12]}...): {e}")
        return 0

def get_current_slug() -> tuple[str, float]:
    now      = time.time()
    start_ts = now - (now % 300)
    if (start_ts + 300 - now) < 5:
        start_ts += 300
    return f"xrp-updown-5m-{int(start_ts)}", start_ts

def find_1h_xrp_up_token() -> str | None:
    now     = time.time()
    hour_ts = int(now - (now % 3600))
    for slug in [f"xrp-updown-1h-{hour_ts}", f"xrp-up-down-1h-{hour_ts}", f"xrp-1h-{hour_ts}"]:
        try:
            data = requests.get(f"{GAMMA_API_URL}/events?slug={slug}", timeout=5).json()
            if data and isinstance(data, list) and data[0].get('markets'):
                ids = json.loads(data[0]['markets'][0].get('clobTokenIds', '[]'))
                if ids:
                    log_info(f"TREND | Token 1h encontrado | slug={slug}")
                    return ids[0]
        except Exception:
            continue
    try:
        data = requests.get(
            f"{GAMMA_API_URL}/markets",
            params={"active": "true", "limit": "200", "closed": "false"}, timeout=8
        ).json()
        if isinstance(data, list):
            for mkt in data:
                sv = mkt.get('slug', '').lower()
                if 'xrp' in sv and '1h' in sv:
                    ids = json.loads(mkt.get('clobTokenIds', '[]'))
                    if ids:
                        log_info(f"TREND | Token 1h via fallback | slug={sv}")
                        return ids[0]
    except Exception:
        pass
    return None

def fetch_trend_from_clob(token_id: str) -> str:
    """
    Fetch CLOB Price History com parametros correctos e calculo robusto de tendencia.
    GET /prices-history?market={token_id}&interval={interval}&fidelity={fidelity}
    
    Refs: https://docs.polymarket.com/trading/orderbook#price-history
    
    Divide historico em tercos; delta = last_avg - first_avg.
    delta > TREND_THRESHOLD => UP | delta < -TREND_THRESHOLD => DOWN | else => NEUTRAL
    """
    try:
        r       = requests.get(
            f"{CLOB_REST_URL}/prices-history",
            params={"market": token_id, "interval": TREND_INTERVAL, "fidelity": TREND_FIDELITY},
            timeout=6
        )
        history = r.json().get("history", [])
        n       = len(history)
        if n < 3:
            return 'NEUTRAL'
        prices    = [float(h['p']) for h in history]
        third     = max(1, n // 3)
        first_avg = sum(prices[:third]) / third
        last_avg  = sum(prices[-third:]) / third
        delta     = last_avg - first_avg
        if   delta >  TREND_THRESHOLD: return 'UP'
        elif delta < -TREND_THRESHOLD: return 'DOWN'
        return 'NEUTRAL'
    except Exception as e:
        log_warn(f"fetch_trend falhou: {e}")
        return 'NEUTRAL'

def fetch_live_bankroll() -> float | None:
    if not clob_client:
        return None
    try:
        return float(clob_client.get_balance())
    except Exception as e:
        log_warn(f"fetch_live_bankroll falhou: {e}")
        return None

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
# TREND TASK GLOBAL
# =============================================================================

async def trend_update_task():
    global xrp_1h_trend, xrp_1h_token_up
    while True:
        try:
            if xrp_1h_token_up is None:
                xrp_1h_token_up = find_1h_xrp_up_token()
            if xrp_1h_token_up is not None:
                new_t = fetch_trend_from_clob(xrp_1h_token_up)
                if new_t != xrp_1h_trend:
                    log_info(f"TREND UPDATE | {xrp_1h_trend} -> {new_t} | interval={TREND_INTERVAL}")
                    xrp_1h_trend = new_t
                else:
                    log_info(f"TREND STABLE | {xrp_1h_trend}")
            else:
                xrp_1h_trend = 'NEUTRAL'
                log_warn("TREND | Mercado XRP 1h nao encontrado — NEUTRAL")
        except Exception as e:
            log_warn(f"trend_update_task erro: {e}")
        await asyncio.sleep(TREND_UPDATE_S)

# =============================================================================
# WEBSOCKET HANDLER — COM INTEGRACAO DE SPREAD API
# =============================================================================

async def ws_handler(t_up: str, t_down: str):
    """
    WebSocket handler. Actualiza best_bids, best_asks e best_spreads_pct por tick.
    Integra dados da API /spread para maior precisao.

    Eventos tratados (docs trading/orderbook#event-types):
      book          -> bids/asks -> best_bid=max(bids), best_ask=min(asks)
      best_bid_ask  -> best_bid, best_ask, spread (campo directo; req custom_feature_enabled)
      price_change  -> price_changes[].best_bid / best_ask
      market_resolved -> winning_asset_id (req custom_feature_enabled)

    Compra ao ASK: docs 'you'll pay the ask when buying'
    Venda ao BID:  docs 'receive the bid when selling'
    
    [v1.0.0] Integracao de /spread API:
      - Fetch periodico de spread nativo para cada token via fetch_market_spread()
      - Atualiza best_spreads_pct com dados da API quando disponiveis
      - Fallback ao calculo manual se API indisponivel (cache interno)
    """
    global resolved_winner_asset
    _bids  = best_bids
    _asks  = best_asks
    _spcts = best_spreads_pct
    _set   = price_change.set

    # Task paralelo de fetch de spread API para cada token (run-in-background)
    async def fetch_spreads_task():
        while True:
            try:
                for token_id, side in [(t_up, 'up'), (t_down, 'down')]:
                    fetch_market_spread(token_id, use_cache=True)
                await asyncio.sleep(SPREAD_API_CACHE_TTL_S * 0.8)  # refresh antes de expirar
            except Exception as e:
                log_warn(f"fetch_spreads_task erro: {e}")
                await asyncio.sleep(1.0)

    spread_task = asyncio.create_task(fetch_spreads_task())

    while True:
        try:
            async with websockets.connect(WS_URI, ping_interval=20, ping_timeout=10) as ws:
                await ws.send(json.dumps({
                    "assets_ids":             [t_up, t_down],
                    "type":                   "market",
                    "custom_feature_enabled": True  # activa best_bid_ask + market_resolved
                }))
                log_info("WS conectado ao orderbook Polymarket")

                async for raw in ws:
                    items = json.loads(raw)
                    if not isinstance(items, list):
                        items = [items]
                    updated = False

                    for item in items:
                        evt = item.get("event_type")

                        # ── market_resolved (requer custom_feature_enabled=True) ──
                        if evt == "market_resolved":
                            wa = item.get("winning_asset_id")
                            if wa:
                                resolved_winner_asset = wa
                                resolved_event.set()
                                log_info(
                                    f"RESOLUCAO WS | winning_asset_id={wa[:16]}... "
                                    f"| outcome={item.get('winning_outcome','?')}"
                                )
                            continue

                        aid = item.get("asset_id")
                        if   aid == t_up:   sk = 'up'
                        elif aid == t_down: sk = 'down'
                        else:               continue

                        bid_p = ask_p = None

                        if evt == "book":
                            bids_r = item.get("bids", [])
                            asks_r = item.get("asks", [])
                            if bids_r:
                                v = [float(d['price']) for d in bids_r if float(d.get('size', 0)) > 0]
                                if v: bid_p = max(v)
                            if asks_r:
                                v = [float(d['price']) for d in asks_r if float(d.get('size', 0)) > 0]
                                if v: ask_p = min(v)

                        elif evt == "best_bid_ask":
                            bb = item.get("best_bid")
                            ba = item.get("best_ask")
                            if bb: bid_p = float(bb)
                            if ba: ask_p = float(ba)
                            sp = item.get("spread")
                            if sp is not None and bid_p and ask_p:
                                mid = (bid_p + ask_p) * 0.5
                                if mid > 0:
                                    _spcts[sk] = float(sp) / mid * 100.0

                        elif evt == "price_change":
                            pcs = item.get("price_changes", [])
                            if pcs:
                                bb = pcs[-1].get("best_bid")
                                ba = pcs[-1].get("best_ask")
                                if bb: bid_p = float(bb)
                                if ba: ask_p = float(ba)

                        if bid_p is not None:
                            _bids[sk] = bid_p
                            updated    = True
                        if ask_p is not None:
                            _asks[sk] = ask_p
                            updated    = True

                        if evt != "best_bid_ask":
                            cb, ca = _bids[sk], _asks[sk]
                            
                            # [v1.0.0] Prioridade: API /spread cache > calculo manual
                            sp_pct = get_spread_from_cache_or_calculate(sk, cb, ca)
                            if sp_pct is not None:
                                _spcts[sk] = sp_pct

                    if updated:
                        _set()

        except asyncio.CancelledError:
            break
        except Exception as e:
            log_warn(f"WS erro: {e} — reconectando em 1s")
            await asyncio.sleep(1)
    
    # Cleanup
    spread_task.cancel()
    try:
        await spread_task
    except asyncio.CancelledError:
        pass

# =============================================================================
# LIVE ORDER
# =============================================================================

async def place_live_order(side: str, ask: float, shares: float, token_id: str) -> bool:
    """
    Ordem limite BUY ao ASK via SDK. Fee_rate_bps obtido dinamicamente (docs obrigam).
    """
    if not clob_client:
        return False
    try:
        fee_bps    = fetch_fee_rate_bps(token_id)
        order_args = OrderArgs(
            token_id=token_id,
            price=round(ask, 4),
            size=round(shares, 6),
            side=SDK_BUY,
            order_type="GTC",
            fee_rate_bps=fee_bps
        )
        resp = clob_client.create_and_post_order(order_args)
        log_info(
            f"LIVE ORDER OK | {side} {token_id[:12]}... @ ASK={ask:.4f} "
            f"| shares={shares:.4f} | fee_bps={fee_bps} | id={resp.get('orderID','OK')}"
        )
        return True
    except Exception as e:
        log_warn(f"LIVE ORDER falhou: {e}")
        return False

# =============================================================================
# PRICE BUFFER
# =============================================================================

class PriceBuffer:
    """
    Buffer circular de (timestamp, eff_c) com limpeza automatica.
    Usado pelo Gambling para calcular deltas em multiplas janelas temporais.
    Deltas so bloqueiam entrada se |delta| >= threshold configurado.
    """
    __slots__ = ('max_age', 'buffer')

    def __init__(self, max_age_seconds: float = 15.0):
        self.max_age: float = max_age_seconds
        self.buffer: deque  = deque()

    def add(self, eff_c: float, ts: float):
        self.buffer.append((ts, eff_c))
        cutoff = ts - self.max_age
        buf    = self.buffer
        while buf and buf[0][0] < cutoff:
            buf.popleft()

    def get_price_at(self, seconds_ago: float, tol: float = 1.0) -> float | None:
        buf    = self.buffer
        target = time.time() - seconds_ago
        best_p, best_d = None, tol + 1.0
        for ts, ec in buf:
            d = abs(ts - target)
            if d < best_d:
                best_d, best_p = d, ec
        return best_p

    def get_delta(self, seconds_ago: float) -> tuple[float | None, bool]:
        buf = self.buffer
        if not buf:
            return None, False
        past = self.get_price_at(seconds_ago)
        if past is None:
            return None, False
        return buf[-1][1] - past, True

    def get_age(self) -> float:
        return (time.time() - self.buffer[0][0]) if self.buffer else 0.0

    def clear(self):
        self.buffer.clear()

# =============================================================================
# LOGIC LOOP
# =============================================================================

async def logic_loop(
    m_start: float,
    m_end: float,
    meta: dict,
    r_mult: float,
    r_accum_loss: float,
    r_recovery_rounds: int
):
    """
    Loop principal de trading para um ciclo de 5 minutos.

    BUY ao ASK: docs 'you'll pay the ask when buying' (orderbook/prices)
    SELL ao BID: docs 'receive the bid when selling'
    RESOLUCAO: winning tokens => $1/share, losing => $0 (concepts/resolution)
    
    [v1.0.0] Integracao de spread API:
      - Usa get_spread_from_cache_or_calculate() para obter spread mais preciso
      - Cache local minimiza latencia de fetch de API
      - Fallback automatico ao calculo manual se cache expirar
    """
    global bankroll, daily_profit

    active_trades      = []
    stop_loss_trigger  = asyncio.Event()

    # Calcula riscos uma unica vez (fora do loop quente)
    eff_sc_risk,   extra_sc,   base_sc_pct   = calc_effective_risk(
        SPREAD_CATCH_RISK, r_mult, bankroll, r_accum_loss
    )
    eff_gamb_risk, extra_gamb, base_gamb_pct = calc_effective_risk(
        GAMBLING_RISK, r_mult, bankroll, r_accum_loss
    )

    # Header de ronda
    mult_tag = f" [MARTINGALE x{r_mult:.0f}]" if r_mult > 1.0 else ""
    cap_sc   = " [CAP]" if eff_sc_risk   >= MAX_RISK_PERCENT else ""
    cap_gamb = " [CAP]" if eff_gamb_risk >= MAX_RISK_PERCENT else ""

    mods = []
    if SPREAD_CATCH_ACTIVE:
        mods.append(f"SPREAD_CATCH(sum<={SC_TRIGGER_SUM:.3f},sprd<{SC_SPREAD_MAX_PCT:.0f}%)")
    if GAMBLING_ACTIVE:
        mods.append(f"GAMBLING({GAMB_START_REM_S}s->{GAMB_CUTOFF_S}s,trend={xrp_1h_trend})")
    if STOP_LOSS_ACTIVE:
        mods.append(f"STOP_LOSS(<{SL_THRESHOLD:.2f}/{SL_TICKS}ticks/{SL_CHECK_S:.0f}s)")

    log_sep2()
    log_info(f"NOVO CICLO | {meta['slug']} | LIVE={LIVE_TRADING}")
    log_info(f"Banca: ${bankroll:.4f} | Profit dia: ${daily_profit:+.4f}{mult_tag}")
    log_info(f"Trend 1h: {xrp_1h_trend} | Modulos: {' | '.join(mods) if mods else 'nenhum'}")
    log_info(
        f"Risco efectivo: SC={base_sc_pct:.1%}+${extra_sc:.4f}{cap_sc} "
        f"| GAMB={base_gamb_pct:.1%}+${extra_gamb:.4f}{cap_gamb} | CAP={MAX_RISK_PERCENT:.0%}"
    )
    log_sep()
    log_info("ESCUTA ACTIVA | Spread API integration: ON")
    log_sep()

    # =========================================================================
    # STOP-LOSS TASK — task independente (docs: monitorizacao continua)
    # =========================================================================
    async def stop_loss_task():
        """
        Spec: BID puro (preco de venda real) monitorizado a cada SL_CHECK_S seg.
        5 ticks consecutivos BID < SL_THRESHOLD -> dispara stop_loss_trigger.
        Logs individuais por tick (MONITOR) mostrando descida de niveis.
        Anti-dump: so arma se BID < 30c de forma consistente, ignorando wicks.
        """
        sl_ticks      = {'UP': 0, 'DOWN': 0}
        sl_started    = {'UP': False, 'DOWN': False}
        sl_levels     = {'UP': set(), 'DOWN': set()}  # niveis de preco vistos

        while True:
            await asyncio.sleep(SL_CHECK_S)
            if not active_trades:
                for s in ('UP', 'DOWN'):
                    sl_ticks[s]   = 0
                    sl_started[s] = False
                    sl_levels[s].clear()
                continue

            sides_open = {t['side'] for t in active_trades}
            triggered  = []
            now        = time.time()
            m_rem      = max(0.0, m_end - now)
            rstr       = get_remaining_str(m_rem)

            for side in ('UP', 'DOWN'):
                if side not in sides_open:
                    sl_ticks[side]   = 0
                    sl_started[side] = False
                    sl_levels[side].clear()
                    continue

                bid_val = best_bids.get(side.lower())
                if bid_val is None:
                    continue
                bid_c = bid_val * 100.0

                if bid_val < SL_THRESHOLD:
                    sl_ticks[side] += 1
                    level_c = round(bid_c)  # nivel arredondado a 1c

                    if not sl_started[side]:
                        # Primeiro tick abaixo do threshold — INICIADO
                        sl_started[side] = True
                        sl_levels[side].add(level_c)
                        log_m('STOPLOSS', 'MONITOR',
                            f"rem={rstr} | {side} iniciado @ {bid_c:.1f}c < {SL_THRESHOLD*100:.1f}c"
                        )
                    else:
                        # Ticks seguintes — detecta novo nivel descendente
                        if level_c not in sl_levels[side]:
                            sl_levels[side].add(level_c)
                            log_m('STOPLOSS', 'MONITOR',
                                f"rem={rstr} | {side} desceu para {bid_c:.1f}c "
                                f"- nivel {level_c:.0f}c novo"
                            )
                        else:
                            log_m('STOPLOSS', 'MONITOR',
                                f"rem={rstr} | {side} @ {bid_c:.1f}c "
                                f"| tick {sl_ticks[side]}/{SL_TICKS}"
                            )
                else:
                    if sl_ticks[side] > 0:
                        log_m('STOPLOSS', 'RESET',
                            f"rem={rstr} | {side} voltou a {bid_c:.1f}c — reset")
                    sl_ticks[side]   = 0
                    sl_started[side] = False
                    sl_levels[side].clear()

                if sl_ticks[side] >= SL_TICKS:
                    triggered.append(side)

            if triggered:
                log_m('STOPLOSS', 'TRIGGER',
                    f"rem={rstr} | lados={triggered} | threshold={SL_THRESHOLD:.2f} "
                    f"| ticks={sl_ticks}"
                )
                stop_loss_trigger.set()
                break

    sl_task = asyncio.create_task(stop_loss_task()) if STOP_LOSS_ACTIVE else None

    # =========================================================================
    # OPEN TRADE — BUY ao ASK
    # docs: 'you'll pay the ask when buying'
    # invested_pure = bankroll * risk
    # shares        = invested_pure / ask         <- ASK (nao BID)
    # fee_buy       = fee_rate(ask) * invested_pure  [cobrada em shares]
    # total_out     = invested_pure + fee_buy      [deducao total da banca]
    # =========================================================================
    async def open_trade(
        side: str,
        trade_type: str,
        rstr: str,
        risk: float,
        extra_log: str | None = None,
        fixed_shares: float | None = None,
        token_id: str | None = None
    ):
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

        fee_buy   = fee_rate(ask) * invested_pure   # cobrada em shares (docs)
        total_out = invested_pure + fee_buy
        eff_c_val = eff_price_c(ask)

        target = None
        if trade_type == 'SPREAD_CATCH' and SC_TARGET_BID_C > 0.0:
            target = SC_TARGET_BID_C / 100.0
        elif trade_type == 'GAMBLING' and GAMB_TARGET_BID_C > 0.0:
            target = GAMB_TARGET_BID_C / 100.0

        bankroll -= total_out

        trade = {
            'side':          side,
            'ask':           ask,             # preco de compra (ASK)
            'bid_at_buy':    bid,             # BID no momento da compra (ref)
            'eff_c':         eff_c_val,
            'shares':        shares,
            'target':        target,
            'type':          trade_type,
            'invested_pure': invested_pure,
            'fee_buy':       fee_buy,
            'total_out':     total_out,
            'token_id':      token_id
        }
        active_trades.append(trade)

        if LIVE_TRADING and token_id:
            await place_live_order(side, ask, shares, token_id)

        sp_c  = best_spreads_pct.get(side.lower())
        sp_s  = f" | spread={sp_c:.2f}%" if sp_c is not None else ""
        bid_s = f" | BID@buy={fc(bid)}" if bid else ""
        ext_s = f" | {extra_log}" if extra_log else ""

        log_m(trade_type, 'BUY',
            f"rem={rstr} | {side} @ ASK={fc(ask)} eff={fc(eff_c_val/100)}"
            f"{bid_s}{sp_s}"
            f" | invested=${invested_pure:.4f} | fee=${fee_buy:.4f} | total=${total_out:.4f}"
            f" | shares={shares:.4f} | risk={risk:.1%}{ext_s}"
        )
        return trade

    # =========================================================================
    # CLOSE TRADE — SELL ao BID
    # docs: 'you'll receive the bid when selling'
    # payout_bruto = shares * bid_actual
    # fee_sell     = payout_bruto * fee_rate(bid_actual)  [cobrada em USDC]
    # payout_net   = payout_bruto - fee_sell
    # =========================================================================
    def close_trade(trade: dict, sell_bid: float, reason: str, rstr: str):
        global bankroll
        payout_bruto = trade['shares'] * sell_bid
        fee_sell     = payout_bruto * fee_rate(sell_bid)
        payout_net   = payout_bruto - fee_sell
        pnl          = payout_net - trade['total_out']
        pnl_pct      = (pnl / trade['total_out'] * 100.0) if trade['total_out'] else 0.0
        bankroll    += payout_net
        sign         = "(+)" if pnl >= 0 else "(-)"
        log_m(trade['type'], 'SELL',
            f"rem={rstr} | {trade['side']} @ BID={fc(sell_bid)} "
            f"| bruto=${payout_bruto:.4f} | fee_sell=${fee_sell:.4f} | net=${payout_net:.4f} "
            f"| PnL: ${pnl:+.4f} ({pnl_pct:+.2f}%) {sign} | Reason: {reason}"
        )
        return pnl

    # =========================================================================
    # CLOSE TRADE RESOLUTION
    # docs concepts/resolution:
    # 'Winning tokens become redeemable for $1.00 each'
    # 'Losing tokens become worthless ($0.00)'
    # fee_rate(1.0) = 0 => resgate sem fee; fee_rate(0.0) = 0
    # =========================================================================
    def close_trade_resolution(trade: dict, winner: bool, rstr: str):
        global bankroll
        shares       = trade['shares']
        payout_net   = resolution_payout(shares, winner)
        pnl          = payout_net - trade['total_out']
        pnl_pct      = (pnl / trade['total_out'] * 100.0) if trade['total_out'] else 0.0
        bankroll    += payout_net

        if winner:
            price_s  = "100.0c"
            reason_s = "RESOLUCAO GANHA ($1/share)"
        else:
            price_s  = "0.0c"
            reason_s = "RESOLUCAO PERDIDA (Total)"

        sign = "(+)" if pnl >= 0 else "(-)"

        if LIVE_TRADING and winner and trade.get('token_id'):
            redeem_live_position(shares, trade['token_id'])

        log_m(trade['type'], 'SELL',
            f"rem={rstr} | {trade['side']} @ {price_s} "
            f"| net=${payout_net:.4f} "
            f"| PnL: ${pnl:+.4f} ({pnl_pct:+.2f}%) {sign} | Reason: {reason_s}"
        )
        return pnl

    # =========================================================================
    # GAMBLING HELPERS
    # =========================================================================

    def gamb_reset(side: str, rstr: str, reason: str):
        gamb_seen_levels[side].clear()
        gamb_tick_count[side]   = 0
        gamb_first_tick_t[side] = None
        gamb_eff_min[side]      = None
        gamb_eff_max[side]      = None
        log_m('GAMBLING', 'RESET', f"rem={rstr} | {side} - {reason}")

    def gamb_reset_silent(side: str):
        gamb_seen_levels[side].clear()
        gamb_tick_count[side]   = 0
        gamb_first_tick_t[side] = None
        gamb_eff_min[side]      = None
        gamb_eff_max[side]      = None

    def gamb_vol_cooldown(side: str, rstr: str, reason: str):
        gamb_vol_cooldown_until[side] = time.time() + GAMB_VOL_COOLDOWN_S
        gamb_reset(side, rstr, reason)
        log_m('GAMBLING', 'VOL_COOLDOWN',
            f"rem={rstr} | {side} - bloqueado {GAMB_VOL_COOLDOWN_S:.0f}s")

    # Estado interno Gambling
    gamb_seen_levels        = {'UP': set(), 'DOWN': set()}
    gamb_tick_count         = {'UP': 0,     'DOWN': 0}
    gamb_last_buy           = {'UP': 0.0,   'DOWN': 0.0}
    gamb_first_tick_t       = {'UP': None,  'DOWN': None}
    gamb_eff_min            = {'UP': None,  'DOWN': None}
    gamb_eff_max            = {'UP': None,  'DOWN': None}
    gamb_price_buffer       = {
        'UP':   PriceBuffer(max_age_seconds=15.0),
        'DOWN': PriceBuffer(max_age_seconds=15.0)
    }
    gamb_vol_cooldown_until = {'UP': 0.0, 'DOWN': 0.0}
    gamb_cutoff_logged      = False
    gamb_started_logged     = False

    sc_count      = 0
    last_sc_time  = 0.0
    prev_bid_up   = prev_bid_down = None

    # =========================================================================
    # LOOP PRINCIPAL
    # =========================================================================
    try:
        while True:
            now = time.time()
            rem = m_end - now

            # ── Fim de mercado ───────────────────────────────────────────────
            if rem <= 0.0:
                final_bid_up   = best_bids.get('up')  or 0.0
                final_bid_down = best_bids.get('down') or 0.0
                final_ask_up   = best_asks.get('up')  or 0.0
                final_ask_down = best_asks.get('down') or 0.0

                log_sep()
                log_info(
                    f"FIM DE MERCADO | UP final={fc(final_bid_up)} "
                    f"| DOWN final={fc(final_bid_down)}"
                )

                if active_trades:
                    # Aguarda evento market_resolved do WS (docs trading/orderbook)
                    log_info(
                        f"Aguardando resolucao WS (max {RESOLVE_TIMEOUT_S:.0f}s)..."
                    )
                    try:
                        await asyncio.wait_for(
                            resolved_event.wait(), timeout=RESOLVE_TIMEOUT_S
                        )
                        # Resolucao via WS — usa winning_asset_id
                        winner_asset = resolved_winner_asset
                        log_info(
                            f"RESOLUCAO CONFIRMADA | winner_asset={winner_asset[:16] if winner_asset else '?'}..."
                        )
                        for trade in active_trades[:]:
                            winner = (trade.get('token_id') == winner_asset)
                            close_trade_resolution(trade, winner, "00:00:000")
                            active_trades.remove(trade)
                    except asyncio.TimeoutError:
                        # Fallback: BID mais alto = estimativa de vencedor
                        log_warn(
                            f"Timeout {RESOLVE_TIMEOUT_S:.0f}s sem market_resolved WS — "
                            f"estimando vencedor por BID final"
                        )
                        est_winner = 'up' if final_bid_up > final_bid_down else 'down'
                        log_info(
                            f"Estimativa fallback: {est_winner.upper()} vencedor "
                            f"(BID_UP={fc(final_bid_up)} BID_DOWN={fc(final_bid_down)})"
                        )
                        for trade in active_trades[:]:
                            winner = (trade['side'].lower() == est_winner)
                            close_trade_resolution(trade, winner, "00:00:000")
                            active_trades.remove(trade)
                break

            # ── Stop-Loss acionado ──────────────────────────────────────────
            if stop_loss_trigger.is_set():
                rstr = get_remaining_str(rem)
                log_sep()
                log_info(f"STOP LOSS ACIONADO | rem={rstr} | Vendendo ao BID actual")
                for trade in active_trades[:]:
                    bid_key  = trade['side'].lower()
                    sell_bid = best_bids.get(bid_key) or 0.0
                    close_trade(trade, sell_bid, "STOP-LOSS FLASH-CRASH", rstr)
                    active_trades.remove(trade)
                break

            rstr = get_remaining_str(rem)

            # ── Aguarda tick WS ─────────────────────────────────────────────
            try:
                await asyncio.wait_for(price_change.wait(), timeout=LOOP_SLEEP)
                price_change.clear()
            except asyncio.TimeoutError:
                pass

            bid_up   = best_bids.get('up')
            bid_down = best_bids.get('down')
            ask_up   = best_asks.get('up')
            ask_down = best_asks.get('down')

            if bid_up is None or bid_down is None or ask_up is None or ask_down is None:
                continue
            if bid_up == prev_bid_up and bid_down == prev_bid_down:
                continue

            prev_bid_up   = bid_up
            prev_bid_down = bid_down

            ask_sum    = ask_up + ask_down
            spc_up     = best_spreads_pct.get('up')
            spc_down   = best_spreads_pct.get('down')
            underpeg_c = (1.0 - ask_sum) * 100.0

            # Midpoints (docs trading/orderbook#midpoints)
            mid_up   = (bid_up   + ask_up)   * 0.5
            mid_down = (bid_down + ask_down)  * 0.5

            spc_str = ""
            if spc_up is not None and spc_down is not None:
                spc_str = f" | sprd_UP={spc_up:.2f}% sprd_DOWN={spc_down:.2f}%"
            peg_str = (
                f" | ASK_SUM={ask_sum:.4f} gap={underpeg_c:.2f}c"
                if ask_sum <= SC_TRIGGER_SUM else ""
            )
            log_raw(
                f"rem={rstr} | "
                f"BID_UP={fc(bid_up)} ASK_UP={fc(ask_up)} MID={fc(mid_up)} | "
                f"BID_DOWN={fc(bid_down)} ASK_DOWN={fc(ask_down)} MID={fc(mid_down)}"
                f"{spc_str}{peg_str}"
            )

            # =================================================================
            # MODULO 1: SPREAD CATCH (CORRIGIDO)
            # Gatilho: ask_up + ask_down <= SC_TRIGGER_SUM (0.960)
            # [v1.0.0 CRITICAL] ask_sum = ask_up + ask_down (ASK sum, nao BID)
            # Filtro: spread_pct < SC_SPREAD_MAX_PCT em ambos os lados
            # BUY ao ASK: shares iguais nos dois lados
            # ref_ask = max(ask_up, ask_down) para garantir budget suficiente
            # =================================================================
            if (SPREAD_CATCH_ACTIVE
                    and ask_sum <= SC_TRIGGER_SUM
                    and rem > SC_MIN_REM
                    and sc_count < MAX_SC_ENTRIES
                    and now - last_sc_time >= SC_COOLDOWN):

                sc_ok_up   = (spc_up   is not None and spc_up   < SC_SPREAD_MAX_PCT)
                sc_ok_down = (spc_down is not None and spc_down < SC_SPREAD_MAX_PCT)

                if sc_ok_up and sc_ok_down:
                    budget        = bankroll * eff_sc_risk
                    ref_ask       = max(ask_up, ask_down)  # lado mais caro (ASK)
                    shares_to_buy = budget / ref_ask

                    fee_up   = fee_rate(ask_up)   * shares_to_buy * ask_up
                    fee_down = fee_rate(ask_down) * shares_to_buy * ask_down
                    total_cost = (shares_to_buy * ask_up   + fee_up +
                                  shares_to_buy * ask_down + fee_down)
                    exp_ret = underpeg_c / (ask_sum * 100.0) * 100.0

                    log_sep()
                    log_m('SPREAD CATCH', 'ENTRADA',
                        f"rem={rstr} | ask_sum={ask_sum:.4f} gap={underpeg_c:.2f}c "
                        f"| shares={shares_to_buy:.4f} | cost=${total_cost:.4f} "
                        f"| ret_est={exp_ret:.2f}% "
                        f"| ASK_UP={fc(ask_up)} ASK_DOWN={fc(ask_down)} | #={sc_count+1}"
                    )
                    await open_trade('UP',   'SPREAD_CATCH', rstr,
                                     risk=eff_sc_risk, fixed_shares=shares_to_buy,
                                     token_id=meta['up'])
                    await open_trade('DOWN', 'SPREAD_CATCH', rstr,
                                     risk=eff_sc_risk, fixed_shares=shares_to_buy,
                                     token_id=meta['down'])
                    log_sep()
                    sc_count    += 1
                    last_sc_time = now
                else:
                    if sc_count == 0:
                        reasons = []
                        if not sc_ok_up:
                            reasons.append(
                                f"sprd_UP={spc_up:.2f}%>={SC_SPREAD_MAX_PCT:.0f}%"
                                if spc_up is not None else "sprd_UP=N/A"
                            )
                        if not sc_ok_down:
                            reasons.append(
                                f"sprd_DOWN={spc_down:.2f}%>={SC_SPREAD_MAX_PCT:.0f}%"
                                if spc_down is not None else "sprd_DOWN=N/A"
                            )
                        log_m('SPREAD CATCH', 'SKIP',
                            f"rem={rstr} | ask_sum={ask_sum:.4f} ok mas microestrutura: "
                            f"{' | '.join(reasons)}")

            # ── Target check (venda ao BID se BID >= target) ────────────────
            for trade in active_trades[:]:
                if trade.get('target') is None:
                    continue
                bid_key  = trade['side'].lower()
                curr_bid = best_bids.get(bid_key)
                if curr_bid and curr_bid >= trade['target']:
                    close_trade(trade, curr_bid, "TARGET", rstr)
                    active_trades.remove(trade)

            # =================================================================
            # MODULO 2: GAMBLING
            # Direcional com bias do trend 1h XRP (CLOB Price History)
            # BUY ao ASK: eff_c = ask * (1 + fee_rate(ask)) * 100
            # Filtro delta: so activo se |delta| >= threshold por janela
            # GAMB_SPREAD_MAX_PCT > 0 => filtro de spread activo
            # =================================================================
            if GAMBLING_ACTIVE:
                if rem > GAMB_START_REM_S:
                    pass

                elif rem <= GAMB_CUTOFF_S:
                    if not gamb_cutoff_logged:
                        gamb_cutoff_logged = True
                        log_m('GAMBLING', 'CUTOFF',
                            f"rem={rstr} | parado — rem<={GAMB_CUTOFF_S}s")
                else:
                    if not gamb_started_logged:
                        gamb_started_logged = True
                        log_m('GAMBLING', 'START',
                            f"rem={rstr} | activo [{GAMB_START_REM_S}s->{GAMB_CUTOFF_S}s] "
                            f"| trend={xrp_1h_trend} | risk={eff_gamb_risk:.1%}")

                    for g_side, g_ask, g_bid in (
                        ('UP',   ask_up,   bid_up),
                        ('DOWN', ask_down, bid_down)
                    ):
                        # Filtro trend
                        if   xrp_1h_trend == 'UP'     and g_side == 'DOWN': continue
                        elif xrp_1h_trend == 'DOWN'    and g_side == 'UP':   continue
                        elif xrp_1h_trend == 'NEUTRAL' and not GAMB_NEUTRAL_BOTH: continue

                        # Filtro spread: GAMB_SPREAD_MAX_PCT >= 0 => activo
                        spc_val = best_spreads_pct.get(g_side.lower())
                        if GAMB_SPREAD_MAX_PCT >= 0.0:
                            if spc_val is not None and spc_val >= GAMB_SPREAD_MAX_PCT:
                                continue

                        # Cooldowns
                        if now < gamb_vol_cooldown_until[g_side]:
                            continue
                        if now - gamb_last_buy[g_side] < GAMB_BUY_COOLDOWN:
                            continue

                        # eff_c baseado no ASK (custo real de compra)
                        g_eff_c = eff_price_c(g_ask)

                        gamb_price_buffer[g_side].add(g_eff_c, now)

                        # Filtro range (comparacao com eff_c — spec)
                        if not (GAMB_MIN_EFF_C <= g_eff_c <= GAMB_MAX_EFF_C):
                            if gamb_tick_count[g_side] > 0:
                                gamb_reset(g_side, rstr,
                                    f"eff_c={g_eff_c:.1f}c fora "
                                    f"[{GAMB_MIN_EFF_C:.0f}-{GAMB_MAX_EFF_C:.0f}]")
                            continue

                        # Nivel de preco (0.5c granularidade)
                        level = round(g_eff_c * 2) / 2
                        if level not in gamb_seen_levels[g_side]:
                            gamb_seen_levels[g_side].add(level)
                            gamb_tick_count[g_side] += 1

                        if gamb_first_tick_t[g_side] is None:
                            gamb_first_tick_t[g_side] = now
                            gamb_eff_min[g_side] = gamb_eff_max[g_side] = g_eff_c
                        else:
                            if g_eff_c < gamb_eff_min[g_side]: gamb_eff_min[g_side] = g_eff_c
                            if g_eff_c > gamb_eff_max[g_side]: gamb_eff_max[g_side] = g_eff_c

                        elapsed = now - gamb_first_tick_t[g_side]
                        var_c   = gamb_eff_max[g_side] - gamb_eff_min[g_side]
                        vol_nok = (elapsed <= GAMB_VOL_WINDOW_S and var_c >= GAMB_VOL_MAX_C)

                        # ─ Delta checks com threshold absoluto ─────────────
                        # D0.5s activo se |delta| >= GAMB_D05_THRESH_C (5c)
                        # D1.0s activo se |delta| >= GAMB_D10_THRESH_C (7c)
                        # D1.5s activo se |delta| >= GAMB_D15_THRESH_C (9c)
                        # D2.0s activo se |delta| >= GAMB_D20_THRESH_C (11c)
                        gpb = gamb_price_buffer[g_side]

                        d05, h05 = gpb.get_delta(0.5)
                        d10, h10 = gpb.get_delta(1.0)
                        d15, h15 = gpb.get_delta(1.5)
                        d20, h20 = gpb.get_delta(2.0)
                        d_pt, hpt = gpb.get_delta(GAMB_PUMP_TIME_S)

                        # So activo se |delta| >= threshold da janela
                        active_05 = h05 and d05 is not None and abs(d05) >= GAMB_D05_THRESH_C
                        active_10 = h10 and d10 is not None and abs(d10) >= GAMB_D10_THRESH_C
                        active_15 = h15 and d15 is not None and abs(d15) >= GAMB_D15_THRESH_C
                        active_20 = h20 and d20 is not None and abs(d20) >= GAMB_D20_THRESH_C
                        pump_det  = hpt and d_pt is not None and d_pt >= GAMB_PUMP_THRESH_C

                        has_active = active_05 or active_10 or active_15 or active_20

                        # Bloqueia se delta activo e negativo (a cair)
                        delta_ok     = True
                        delta_reason = ""
                        if   active_05 and d05 < 0:
                            delta_ok, delta_reason = False, f"D0.5s={d05:+.1f}c (a cair)"
                        elif active_10 and d10 < 0:
                            delta_ok, delta_reason = False, f"D1s={d10:+.1f}c (a cair)"
                        elif active_15 and d15 < 0:
                            delta_ok, delta_reason = False, f"D1.5s={d15:+.1f}c (a cair)"
                        elif active_20 and d20 < 0:
                            delta_ok, delta_reason = False, f"D2s={d20:+.1f}c (a cair)"
                        elif pump_det:
                            delta_ok, delta_reason = (
                                False, f"D{GAMB_PUMP_TIME_S}s={d_pt:+.1f}c (pump)"
                            )

                        # Display dos deltas activos
                        dp_parts = []
                        if active_05: dp_parts.append(f"D0.5s:{d05:+.1f}c(thr={GAMB_D05_THRESH_C:.0f}c)")
                        if active_10: dp_parts.append(f"D1s:{d10:+.1f}c(thr={GAMB_D10_THRESH_C:.0f}c)")
                        if active_15: dp_parts.append(f"D1.5s:{d15:+.1f}c(thr={GAMB_D15_THRESH_C:.0f}c)")
                        if active_20: dp_parts.append(f"D2s:{d20:+.1f}c(thr={GAMB_D20_THRESH_C:.0f}c)")
                        dp_str = (" | ".join(dp_parts)) if dp_parts else f"Deltas abaixo threshold ({gpb.get_age():.1f}s)"

                        spc_disp = f" | sprd={spc_val:.2f}%" if spc_val is not None else ""
                        peg_disp = f" | peg={bid_sum:.3f}<min={GAMB_PEG_MIN:.3f}" if bid_sum < GAMB_PEG_MIN else ""

                        log_m('GAMBLING', 'WATCH',
                            f"rem={rstr} | {g_side} ASK={fc(g_ask)} eff={fc(g_eff_c/100)} "
                            f"trend={xrp_1h_trend} | "
                            f"VOL={'NOK' if vol_nok else 'OK'} ({var_c:.1f}c/{elapsed:.1f}s) | "
                            f"{dp_str} {'NOK' if has_active and not delta_ok else ('ACT' if has_active else '-')}"
                            f"{peg_disp}{spc_disp} | ticks={gamb_tick_count[g_side]}/{GAMB_MIN_TICKS}"
                        )

                        if vol_nok:
                            gamb_vol_cooldown(g_side, rstr,
                                f"VOL {var_c:.1f}c/{elapsed:.1f}s > "
                                f"{GAMB_VOL_MAX_C:.1f}c/{GAMB_VOL_WINDOW_S:.1f}s")
                            continue
                        if pump_det:
                            gamb_vol_cooldown(g_side, rstr,
                                f"PUMP D{GAMB_PUMP_TIME_S}s={d_pt:+.1f}c")
                            continue

                        if gamb_tick_count[g_side] >= GAMB_MIN_TICKS:

                            if bid_sum < GAMB_PEG_MIN:
                                gamb_reset(g_side, rstr,
                                    f"peg={bid_sum:.3f} < min={GAMB_PEG_MIN:.3f}")
                                continue

                            if has_active and not delta_ok:
                                gamb_reset(g_side, rstr, f"DELTA NOK - {delta_reason}")
                                continue

                            if bankroll > 0.0:
                                token_id = meta['up'] if g_side == 'UP' else meta['down']
                                await open_trade(
                                    g_side, 'GAMBLING', rstr,
                                    risk=eff_gamb_risk,
                                    token_id=token_id,
                                    extra_log=f"ticks={gamb_tick_count[g_side]} | {dp_str}"
                                )
                                gamb_last_buy[g_side] = now
                                gamb_reset_silent(g_side)
                                log_m('GAMBLING', 'COOLDOWN',
                                    f"rem={rstr} | {g_side} — cooldown {GAMB_BUY_COOLDOWN:.1f}s")

    finally:
        if sl_task and not sl_task.done():
            sl_task.cancel()
            try:
                await sl_task
            except asyncio.CancelledError:
                pass

# =============================================================================
# MAIN
# =============================================================================

async def main():
    global daily_profit, last_day, bankroll, price_change
    global risk_multiplier, accumulated_loss, recovery_rounds
    global total_pnl_pos, total_pnl_neg, bot_start_time
    global xrp_1h_trend, xrp_1h_token_up
    global resolved_event, resolved_winner_asset

    bot_start_time   = time.time()
    risk_multiplier  = 1.0
    accumulated_loss = 0.0
    recovery_rounds  = 0
    total_pnl_pos    = 0.0
    total_pnl_neg    = 0.0
    xrp_1h_trend     = 'NEUTRAL'
    xrp_1h_token_up  = None

    if LIVE_TRADING:
        lb       = fetch_live_bankroll()
        bankroll = lb if lb is not None else BANKROLL_DEMO
    else:
        bankroll = BANKROLL_DEMO

    # ─ Log de arranque (formato exacto especificado) ─────────────────────────
    log_sep2()
    log_info(f"BOT XRP POLYMARKET v1.0.0 INICIADO")
    log_sep2()
    log_info(f"LIVE_TRADING     : {LIVE_TRADING}")
    log_info(f"BANKROLL_INIT    : ${bankroll:.2f}")
    log_info(f"RISCO BASE:")
    log_info(f"   GAMBLING      : {GAMBLING_RISK:.0%}")
    log_info(f"   SPREAD CATCH  : {SPREAD_CATCH_RISK:.0%}")
    log_info(f"MARTINGALE:")
    log_info(f"   MAX_RISK_PERCENT : {MAX_RISK_PERCENT:.0%} (CAP INVIOLAVEL)")
    log_info(f"   MAX_MULTIPLIER   : x{MAX_RISK_MULT}")
    log_info(f"   Formula          : min(base x mult + (50% acc_loss), MAX_RISK)")
    log_info(f"MODULOS:")
    log_info(f"   GAMBLING           : {'ON' if GAMBLING_ACTIVE else 'OFF'}")
    log_info(f"   SPREAD CATCH       : {'ON' if SPREAD_CATCH_ACTIVE else 'OFF'}")
    log_info(f"   STOP LOSS          : {'ON' if STOP_LOSS_ACTIVE else 'OFF'}")
    log_sep2()
    log_info("EXECUCAO:")
    log_info(f"   BUY               : ao ASK (preco real de compra no livro)")
    log_info(f"   SELL              : ao BID (preco real de venda no livro)")
    log_info(f"   RESOLUCAO GANHA   : $1.00/share (resgate sem fee; fee_rate(1.0)=0)")
    log_info(f"   RESOLUCAO PERDIDA : $0.00 (tokens worthless)")
    log_info(f"   RESOLVE_TIMEOUT   : {RESOLVE_TIMEOUT_S:.0f}s aguarda market_resolved WS")
    log_sep2()
    log_info("FEES (Polymarket 5-min crypto, docs trading/fees):")
    log_info(f"   fee_rate(p) = {FEE_RATE} * (p*(1-p))^{FEE_EXP}")
    log_info(f"   BUY: fee = invested * fee_rate(ask)    [shares]")
    log_info(f"   SELL: fee = payout_bruto * fee_rate(bid) [USDC]")
    log_info(f"   Max: 1.56% em p=0.50; Zero em p=0 e p=1")
    log_sep2()
    log_info("GAMBLING DELTA THRESHOLDS (so activos se |delta| >= threshold):")
    log_info(f"   D0.5s >= {GAMB_D05_THRESH_C:.0f}c | D1s >= {GAMB_D10_THRESH_C:.0f}c "
             f"| D1.5s >= {GAMB_D15_THRESH_C:.0f}c | D2s >= {GAMB_D20_THRESH_C:.0f}c")
    log_info(f"   GAMB_SPREAD_MAX_PCT = {GAMB_SPREAD_MAX_PCT:.1f} "
             f"({'desactivado (negativo)' if GAMB_SPREAD_MAX_PCT < 0 else 'activo'})")
    log_sep2()

    # Inicia trend task global
    trend_task = asyncio.create_task(trend_update_task())

    # ─ Loop de ciclos de 5 minutos ───────────────────────────────────────────
    while True:
        slug, start_ts = get_current_slug()
        meta = fetch_metadata(slug)
        if not meta:
            log_warn(f"Metadata nao encontrada para {slug} — retry em 1s")
            await asyncio.sleep(1)
            continue

        # Reset evento de resolucao para este ciclo
        resolved_event.clear()
        resolved_winner_asset = None

        # Novo dia
        market_day = datetime.fromtimestamp(start_ts).date()
        if last_day is None or market_day != last_day:
            daily_profit     = 0.0
            risk_multiplier  = 1.0
            accumulated_loss = 0.0
            recovery_rounds  = 0
            last_day         = market_day
            xrp_1h_token_up  = None

            if LIVE_TRADING:
                lb = fetch_live_bankroll()
                if lb is not None:
                    bankroll = lb

            log_sep2()
            log_info(f"NOVO DIA {market_day} | Martingale reset: x1 | accum=$0.00")
            log_info(f"Banca: ${bankroll:.4f} | LIVE={LIVE_TRADING}")
            log_sep2()

        # Reset estado WS
        best_bids['up']  = best_bids['down']  = None
        best_asks['up']  = best_asks['down']  = None
        best_spreads_pct['up'] = best_spreads_pct['down'] = None
        price_change.clear()

        ws_task = asyncio.create_task(ws_handler(meta['up'], meta['down']))
        await asyncio.sleep(1.0)  # aguarda primeiro tick

        if best_bids['up'] is not None:
            pre_bank = bankroll

            await logic_loop(
                start_ts,
                start_ts + 300,
                meta,
                risk_multiplier,
                accumulated_loss,
                recovery_rounds
            )

            profit_this   = bankroll - pre_bank
            daily_profit += profit_this

            # ── Actualiza Martingale + Recovery e imprime ROUND/TOTAL ────────
            log_sep2()

            pnl_pct_round = (profit_this / pre_bank * 100.0) if pre_bank > 0 else 0.0
            pnl_str       = f"${profit_this:+.4f} ({pnl_pct_round:+.2f}%)"
            dp_pct        = (daily_profit / (pre_bank - daily_profit + profit_this) * 100.0
                             if (pre_bank - daily_profit + profit_this) > 0 else 0.0)
            dp_str        = f"${daily_profit:+.4f} ({dp_pct:+.2f}%)"

            if profit_this < -0.00001:
                # ── PERDA ──────────────────────────────────────────────────
                loss             = abs(profit_this)
                accumulated_loss += loss
                recovery_rounds  += RECOVERY_ROUNDS_STEP
                risk_multiplier   = min(risk_multiplier * 2.0, float(MAX_RISK_MULT))
                total_pnl_neg    += profit_this

                # Preview proximo risco
                nx_sc_r,  nx_extra_sc,   nx_sc_base   = calc_effective_risk(
                    SPREAD_CATCH_RISK, risk_multiplier, bankroll, accumulated_loss
                )
                nx_gb_r,  nx_extra_gamb, nx_gb_base   = calc_effective_risk(
                    GAMBLING_RISK, risk_multiplier, bankroll, accumulated_loss
                )
                cap_s = " [CAP]" if nx_sc_r >= MAX_RISK_PERCENT else ""
                cap_g = " [CAP]" if nx_gb_r >= MAX_RISK_PERCENT else ""

                log_info(
                    f"ROUND | PnL: {pnl_str} | Proximo Mult: x{risk_multiplier:.0f} "
                    f"| Acc_loss: ${accumulated_loss:.4f} "
                    f"| GAMBLING={nx_gb_base:.1%}+${nx_extra_gamb:.3f} (50% acc_loss){cap_g} "
                    f"SPREAD CATCH={nx_sc_base:.1%}+${nx_extra_sc:.3f} (50% acc_loss){cap_s} "
                    f"| (cap={MAX_RISK_PERCENT:.0%})"
                )

            elif profit_this > 0.00001:
                # ── LUCRO ──────────────────────────────────────────────────
                prev_accum        = accumulated_loss
                accumulated_loss   = max(0.0, accumulated_loss - profit_this)
                recovery_rounds    = max(0, recovery_rounds - 1)
                recovered          = prev_accum - accumulated_loss
                risk_multiplier    = 1.0
                total_pnl_pos     += profit_this

                if prev_accum > 0.0:
                    log_info(
                        f"MARTINGALE | RECOVERY parcial "
                        f"(recuperados ${recovered:.4f} | restam ${accumulated_loss:.4f}) "
                        f"| Rounds restantes: {recovery_rounds}"
                    )
                log_info(f"ROUND | PnL: {pnl_str}")

            else:
                # ── SEM TRADE ou PnL ZERO ─────────────────────────────────
                if risk_multiplier > 1.0 or accumulated_loss > 0.0:
                    # Martingale activo — mostra preview (mult mantido)
                    nx_sc_r,  nx_extra_sc,   nx_sc_base   = calc_effective_risk(
                        SPREAD_CATCH_RISK, risk_multiplier, bankroll, accumulated_loss
                    )
                    nx_gb_r,  nx_extra_gamb, nx_gb_base   = calc_effective_risk(
                        GAMBLING_RISK, risk_multiplier, bankroll, accumulated_loss
                    )
                    cap_s = " [CAP]" if nx_sc_r >= MAX_RISK_PERCENT else ""
                    cap_g = " [CAP]" if nx_gb_r >= MAX_RISK_PERCENT else ""
                    log_info(
                        f"ROUND | PnL: $0.0 (0.00%) | Proximo Mult: x{risk_multiplier:.0f} "
                        f"| Acc_loss: ${accumulated_loss:.4f} "
                        f"| GAMBLING={nx_gb_base:.1%}+${nx_extra_gamb:.3f} "
                        f"(50% acc_loss_last_round){cap_g} "
                        f"SPREAD CATCH={nx_sc_base:.1%}+${nx_extra_sc:.3f} "
                        f"(50% acc_loss_last_round){cap_s} "
                        f"| (cap={MAX_RISK_PERCENT:.0%})"
                    )
                else:
                    log_info(f"ROUND | PnL: $0.0 (0.00%)")

            log_info(
                f"TOTAL | PnL: {dp_str} | "
                f"Banca: ${bankroll:.4f} | "
                f"Accumulated loss: ${accumulated_loss:.4f} | "
                f"Uptime: {get_uptime_str()}"
            )
            log_sep2()
        else:
            log_warn("Sem BIDs/ASKs recebidos neste ciclo — a saltar")

        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass

        await asyncio.sleep(0.5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_info("BOT PARADO PELO UTILIZADOR")