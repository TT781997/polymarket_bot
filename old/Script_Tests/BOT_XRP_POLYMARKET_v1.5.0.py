# =============================================================================
# BOT XRP POLYMARKET — v1.5.0
# =============================================================================
#
# CHANGELOG v1.5.0  [Cérebro Quantitativo Bidirecional — 5 módulos]:
#
# [1] FILTRO DE KALMAN (novo — class KalmanFilter1D):
#     Filtro de Kalman unidimensional leve para cada lado (UP/DOWN).
#     Separa o ruído de mercado (wicks falsos) do preço real estimado.
#     Parâmetros: KALMAN_PROCESS_NOISE (Q=1e-5) e KALMAN_MEASURE_NOISE (R=1e-2).
#     Alimenta o HFTWindow com preços suavizados em vez de raw mid_price.
#     Z-Score agora mede desvio em relação ao trajectória Kalman, não ao ruído.
#     Reset por ciclo (.reset() em main() a cada novo mercado).
#
# [2] VPIN — Order Flow Toxicity (novo — class VPINTracker):
#     Aproximação leve de VPIN por classificação de ticks.
#     Tick classificado como buy (mid Kalman subiu) ou sell (mid Kalman desceu).
#     Volume proxy: bid_size + ask_size se disponível, 1.0 caso contrário.
#     VPIN = |buy_vol - sell_vol| / (buy_vol + sell_vol) sobre janela de 30s.
#     Range: 0.0 (fluxo 100% equilibrado) a 1.0 (fluxo 100% tóxico).
#     Interpretação:
#       >= SL_TOXIC_VPIN (0.85): dump institucional confirmado — SL imediato.
#       >= VPIN_SAFE_LIMIT (0.70): fluxo desequilibrado — bloqueia entrada GAMBLING.
#       < VPIN_SAFE_LIMIT: fluxo normal — entrada permitida (se outros filtros OK).
#     Reset por ciclo (.reset() em main() a cada novo mercado).
#
# [3] GAMBLING — Motor Quantitativo de 4 Condições (substitui HFT 2-condições):
#     REMOVIDAS de v1.4.0:
#       Condição Z-Score <= GAMB_MAX_ZSCORE (1.5) e Imbalance >= GAMB_MIN_IMBALANCE
#     NOVAS 4 condições (todas obrigatórias em simultâneo):
#       Cond 1 — Regime de compressão: std(Kalman, 30s) <= GAMB_MAX_VOL_DEV (0.05)
#                Preço comprimido = mercado a acumular antes de break direcional.
#       Cond 2 — Anti-pico: Z-Score(Kalman) <= GAMB_MAX_ZSCORE (1.0)
#                Preço não está num pico estatístico anormal vs. trajectory Kalman.
#       Cond 3 — Suporte real: OBI >= GAMB_MIN_OBI (0.60)
#                Compradores dominam 60%+ do Top of Book.
#       Cond 4 — Fluxo saudável: VPIN <= VPIN_SAFE_LIMIT (0.70)
#                Sem atividade institucional tóxica detetada.
#     Cond 1 None (janela insuficiente): bloqueia entrada (aguarda 30s).
#     Cond 3/4 None (sizes/VPIN indisponíveis): passa com WARN (graceful degradation).
#     Tamanho da posição: calc_kelly_risk(ask) em vez de GAMBLING_RISK fixo.
#
# [4] TAKE-PROFIT DINÂMICO (novo módulo inline — TP_SPIKE_ZSCORE):
#     Verificação tick-a-tick nas posições GAMBLING abertas.
#     Se Z-Score(Kalman) do lado >= TP_SPIKE_ZSCORE (2.5): venda imediata ao BID.
#     Captura o wick antes que o preço reverta à média Kalman.
#     Coexiste com GAMB_TARGET_BID_C (TP estático): o que disparar primeiro vence.
#     Toggle: TAKE_PROFIT_ACTIVE (default True).
#     PEG ARBIT não é afetado — only GAMBLING positions.
#
# [5] STOP-LOSS — Triple-Trigger com lógica OR (v1.4.0 era AND):
#     ALTERAÇÃO CRÍTICA: de v1.4.0 (3 condições AND simultâneas) para v1.5.0
#     (BID <= threshold AND qualquer uma das 3 condições abaixo):
#       Trigger A — VPIN: vpin >= SL_TOXIC_VPIN (0.85) → dump institucional
#       Trigger B — Z-Score: Z <= SL_CRASH_ZSCORE (-2.0) → queda Kalman violenta
#       Trigger C — OBI: obi <= SL_PANIC_OBI (0.25) → compradores abandonaram
#     Reação no próprio tick (sem task separada, sem sleep, sem contagem).
#     Fecha APENAS posições GAMBLING do lado em crash — PEG ARBIT intacto.
#     Logs diferenciados: qual trigger disparou o SL.
#
# [6] KELLY CRITERION — Fim do Martingale (substitui GAMBLING_RISK fixo):
#     REMOVIDOS: GAMBLING_RISK, MAX_RISK_MULT, RECOVERY_ROUNDS_STEP.
#     REMOVIDOS globals: risk_multiplier, accumulated_loss, recovery_rounds.
#     Nova função calc_kelly_risk(ask):
#       p_implícita = ask (probabilidade de mercado)
#       p_estimada  = min(ask + KELLY_ASSUMED_EDGE, 0.995)
#       odds_retorno = (1 - ask) / ask (payout líquido se ganhar)
#       Kelly %      = p_est - (1 - p_est) / odds_retorno
#       Fractional   = kelly * KELLY_FRACTION (0.25 = 1/4 Kelly)
#       Cap          = min(fractional, KELLY_MAX_RISK_PCT) (8%)
#     PEG ARBIT mantém PEG_ARBIT_RISK fixo (é arb, não direcional).
#     main() simplificado: sem Martingale state, sem ROUND/RECOVERY logs complexos.
#
# RENOMEAÇÕES de v1.4.0 → v1.5.0:
#   GAMB_MIN_IMBALANCE → GAMB_MIN_OBI   (mesmo conceito, nome mais preciso)
#   SL_TRIGGER_PRICE   → SL_BASE_TRIGGER
#   SL_PANIC_IMBALANCE → SL_PANIC_OBI
#
# PARÂMETROS DEPRECADOS v1.5.0 (mantidos como comentários para referência):
#   GAMBLING_RISK, MAX_RISK_MULT, RECOVERY_ROUNDS_STEP — Kelly substitui
#   GAMB_MIN_IMBALANCE, SL_TRIGGER_PRICE, SL_PANIC_IMBALANCE — renomeados
#   HFT_MIN_SAMPLES — HFTWindow usa 3 pontos como mínimo interno
#
# =============================================================================
# CHANGELOG v1.4.0  [Motor HFT Z-Score + Orderbook Imbalance]:
# [1] Gambling: Z-Score + Imbalance (2 condições) substituiu ticks/deltas.
# [2] Stop-Loss: triple-trigger inline substituiu stop_loss_task() com sleep.
# [3] PriceBuffer substituída por HFTWindow (janela temporal, não por contagem).
# [DEPRECADO em v1.5.0: Gambling agora 4 condições + Kelly; SL agora OR lógica]
# =============================================================================
# CHANGELOG v1.3.0  [3 alterações cirúrgicas]:
# [1] SL_THRESHOLD=0.35 SL_TICKS=10 SL_CHECK_S=0.5 [DEPRECADO v1.4.0]
# [2] TREND — removida pesquisa get_markets(); usa meta["up"] directamente.
# [3] PRICES INIT — WS exclusivo; fetch_initial_prices_sdk() eliminada.
# =============================================================================
# CHANGELOG v1.2.1  [Stop-Loss Cirúrgico]:
# [1] stop_loss_trigger Event → stop_loss_triggered_sides set
# [2] stop_loss_task() — disparo cirúrgico por lado sem break
# [3] Handler loop — fecha apenas GAMBLING do lado exato
# =============================================================================
# CHANGELOG v1.2.0  [5 alterações]:
# PEG ARBIT; Tick Log PEG; Gambling NEUTRAL visível; Preços SDK; Trend história
# =============================================================================
# CHANGELOG v1.1.0:
# GAMBLING_RISK=0.03; PEG_ARBIT_RISK=0.05; Logging ficheiro; SDK Trend
# =============================================================================
# CHANGELOG v1.0.0:
# BUY ao ASK; SELL ao BID; WS market_resolved; Martingale 20%/x8
# =============================================================================

import asyncio
import math
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
LIVE_TRADING    = False   # True=ordens reais (requer secrets.txt); False=simulacao DEMO [Range: False | True]

# --- BANCA ---
BANKROLL_DEMO   = 10.0    # Banca DEMO em USDC; persistente, nunca reseta entre dias [Range: 1.0 | 100000.0]

# --- RISCO BASE ---
# v1.5.0: GAMBLING_RISK removido — Kelly Criterion calcula dinamicamente por trade.
# [DEPRECATED v1.5.0] GAMBLING_RISK = 0.03  # Substituido por calc_kelly_risk(ask)
# [DEPRECATED v1.5.0] MAX_RISK_MULT = 8     # Substituido por Kelly (sem Martingale)
# [DEPRECATED v1.5.0] RECOVERY_ROUNDS_STEP = 10  # Removido com Martingale
PEG_ARBIT_RISK    = 0.25  # Risco fixo PEG ARBIT (arb, nao direccional — Kelly nao se aplica) [Range: 0.01 | 0.20]
MAX_RISK_PERCENT  = 0.35  # Cap PEG ARBIT: investimento PA nunca excede 25% da banca [Range: 0.10 | 0.50]

# --- TOGGLES ---
PEG_ARBIT_ACTIVE    = True   # Peg Arbit activo [Range: False | True]
GAMBLING_ACTIVE     = True   # Gambling activo [Range: False | True]
STOP_LOSS_ACTIVE    = True   # Stop-Loss activo [Range: False | True]
TAKE_PROFIT_ACTIVE  = True   # Take-Profit dinamico activo (wick capture) [Range: False | True]

# --- PEG ARBIT (ex SPREAD CATCH) ---
PA_TRIGGER_SUM    = 0.985        # Gatilho: entra se ask_up+ask_down <= valor (underpeg no custo real) [Range: 0.940 | 0.999]
PA_COOLDOWN       = 0.05        # Intervalo minimo entre entradas PA consecutivas (seg) [Range: 0.01 | 5.0]
PA_MIN_REM        = 1.0         # Remaining minimo para entrar no PA (seg) [Range: 1.0 | 30.0]
PA_TARGET_BID_C   = 0.0         # Target de venda antecipada ao BID (cents; 0=hold ate resolucao) [Range: 0.0 | 99.0]
MAX_PA_ENTRIES    = 10_000_000  # Entradas maximas PA por ciclo [Range: 1 | 10000000]

# --- GAMBLING ---
GAMB_START_REM_S  = 300     # Activa Gambling quando remaining <= X seg [Range: 60 | 300]
GAMB_CUTOFF_S     = 5       # Para Gambling quando remaining <= X seg [Range: 0 | 30]
GAMB_MIN_EFF_C    = 65.0    # eff_c minimo para entrada (cents); eff_c=ask*(1+fee_rate(ask))*100 [Range: 50.0 | 95.0]
GAMB_MAX_EFF_C    = 95.0    # eff_c maximo para entrada (cents) [Range: 82.0 | 99.9]
GAMB_BUY_COOLDOWN = 8.0     # Cooldown entre compras do mesmo lado (seg) [Range: 0.5 | 30.0]
GAMB_PEG_MIN      = 0.970   # Soma minima ask_up + ask_down para entrar (liquidez minima) [Range: 0.90 | 0.999]
GAMB_NEUTRAL_BOTH = True    # Trend NEUTRAL: False=nao opera, True=opera ambos os lados [Range: False | True]
GAMB_TARGET_BID_C = 0.0     # Take-Profit ESTATICO ao BID (cents; 0=desactivado; TP dinamico via TP_SPIKE_ZSCORE) [Range: 0.0 | 99.0]
# [DEPRECATED v1.5.0] GAMB_MIN_IMBALANCE = 0.60  # Renomeado para GAMB_MIN_OBI
# [DEPRECATED v1.4.0] GAMB_MIN_TICKS, GAMB_VOL_MAX_C, GAMB_D*_THRESH_C — substituidos por HFT

# --- MOTOR QUANTITATIVO HFT (KALMAN, Z-SCORE, VPIN, OBI) ---
#
#   Arquitectura por lado (UP / DOWN):
#     KalmanFilter1D  -> suaviza mid_price (separa ruido do preco real)
#     HFTWindow       -> janela 30s de preco Kalman para Z-Score e StdDev
#     VPINTracker     -> janela 30s de fluxo de ordens para toxicidade
#
#   A cada tick WS:
#     kal = kalman.update(mid)       -> preco real estimado
#     hft.add(kal, now)              -> actualiza janela temporal
#     z   = hft.zscore(kal)          -> Z-Score vs. trajectoria Kalman
#     std = hft.std()                -> volatilidade da janela
#     vpin_tracker.add(kal, vol, now) -> actualiza tracker de fluxo
#     vpin = vpin_tracker.vpin()     -> toxicidade 0-1
#     obi  = calc_imbalance(bs, as)  -> orderbook imbalance do top
#
HFT_WINDOW_SECONDS   = 10      # Janela termica de memoria do mercado (seg) [Range: 10 | 120]
KALMAN_PROCESS_NOISE = 1e-5    # Ruido do modelo de transicao Q (menor=mais suavizado) [Range: 1e-7 | 1e-2]
KALMAN_MEASURE_NOISE = 1e-2    # Ruido de observacao do mercado R (maior=mais suavizado) [Range: 1e-4 | 1.0]

# --- MICRO-REGIMES & GAMBLING ENTRY (4 condicoes) ---
#
#   Cond 1 — Regime de compressao (Squeeze):
GAMB_MAX_VOL_DEV   = 0.10   # StdDev(Kalman,30s) <= 5c: regime estavel/comprimido [Range: 0.01 | 0.15]
#   Cond 2 — Anti-pico (Z-Score):
GAMB_MAX_ZSCORE    = 1.8    # Z <= 1.0: preco nao esta num pico vs. trajectoria Kalman [Range: 0.5 | 3.0]
#   Cond 3 — Suporte real (OBI = Orderbook Imbalance):
GAMB_MIN_OBI       = 0.70   # OBI >= 60%: compradores dominam o Top of Book [Range: 0.50 | 0.90]
#   Cond 4 — Fluxo saudavel (VPIN = Order Flow Toxicity):
VPIN_SAFE_LIMIT    = 0.55   # VPIN <= 0.70: fluxo normal; sem dump institucional detetado [Range: 0.30 | 0.95]

# --- TAKE-PROFIT DINAMICO (Wick Capture) ---
TP_SPIKE_ZSCORE    = 3.0    # Vende GAMBLING imediatamente se Z(Kalman) >= 2.5 (wick absurdo) [Range: 1.5 | 4.0]

# --- STOP-LOSS (lógica OR: BID <= threshold E qualquer trigger) ---
#
#   Trigger base: BID <= SL_BASE_TRIGGER (linha de perigo)
#   Trigger A — VPIN toxicidade extrema:
SL_TOXIC_VPIN      = 0.95   # VPIN >= 85%: dump institucional confirmado [Range: 0.60 | 1.0]
#   Trigger B — Z-Score crash Kalman:
SL_CRASH_ZSCORE    = -4.0   # Z <= -3.5: queda violenta vs. trajectoria Kalman (-3.5 StdDev) [Range: -5.0 | -0.5]
#   Trigger C — OBI panico:
SL_PANIC_OBI       = 0.05   # OBI <= 25%: compradores abandonaram o livro completamente [Range: 0.05 | 0.50]
SL_BASE_TRIGGER    = 0.35   # BID <= 35c activa a verificacao dos triggers A/B/C [Range: 0.10 | 0.60]
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
KELLY_ASSUMED_EDGE = 0.05   # Vantagem probabilistica assumida quando HFT da luz verde (+5% acima do preco implicito) [Range: 0.01 | 0.20]
KELLY_FRACTION     = 0.20   # Fractional Kelly: usar 1/4 de Kelly para seguranca [Range: 0.10 | 1.0]
KELLY_MAX_RISK_PCT = 0.08   # Cap absoluto de risco por trade Kelly (8% da banca) [Range: 0.01 | 0.25]

# --- TREND 1H XRP ---
# v1.3.0: TREND_INTERVAL="1h"; token alimentado por meta["up"] — sem pesquisa de mercado.
TREND_UPDATE_S  = 60.0   # Intervalo de actualizacao do trend (seg) [Range: 30.0 | 300.0]
TREND_FIDELITY  = 60     # Granularidade do historico de precos (data points/intervalo) [Range: 5 | 60]
TREND_THRESHOLD = 0.015  # Variacao minima entre tercos para classificar UP/DOWN [Range: 0.003 | 0.050]
TREND_INTERVAL  = "1h"   # Intervalo CLOB Price History — "1h" cobre a ultima hora [Range: "1h" | "max"]
TREND_LOG_PTS   = 5      # Numero de pontos (inicio e fim) a logar no ficheiro [Range: 1 | 30]

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

# v1.5.0: Martingale globals REMOVIDOS.
# [DEPRECATED v1.5.0] risk_multiplier  = 1.0
# [DEPRECATED v1.5.0] accumulated_loss = 0.0
# [DEPRECATED v1.5.0] recovery_rounds  = 0

# Microestrutura em tempo real (actualizados por tick WS)
best_bids      = {"up": None, "down": None}  # Melhor BID por lado (preco de VENDA) [Range: 0.0 | 1.0]
best_asks      = {"up": None, "down": None}  # Melhor ASK por lado (preco de COMPRA) [Range: 0.0 | 1.0]
best_spreads_c = {"up": None, "down": None}  # Spread em cents absolutos (SDK nativo) [Range: 0.0 | 100.0]

# Sizes do Top of Book para OBI (actualizados pelo WS evento "book")
best_bid_sizes = {"up": None, "down": None}  # Volume do melhor BID [Range: 0.0 | inf]
best_ask_sizes = {"up": None, "down": None}  # Volume do melhor ASK [Range: 0.0 | inf]

price_change   = asyncio.Event()
bot_start_time = time.time()

# Trend XRP 1h — v1.3.0: token alimentado por meta["up"] sem pesquisa.
xrp_1h_trend    = "NEUTRAL"  # UP / DOWN / NEUTRAL [Range: str]
xrp_1h_token_up = None       # Token ID UP do ciclo actual [Range: None | str]

# Resolucao do mercado actual (actualizados pelo WS)
resolved_event        = asyncio.Event()   # Set quando WS envia market_resolved
resolved_winner_asset = None              # winning_asset_id do evento WS [Range: None | str]

# PnL global
total_pnl_pos = 0.0
total_pnl_neg = 0.0

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
            logger.error(f"[ERROR] [{get_ts()}] | FATAL: LIVE_TRADING=True mas chave ausente!")
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
      frac  = kelly * KELLY_FRACTION                  # 1/4 Kelly para seguranca
      risk  = min(frac, KELLY_MAX_RISK_PCT)           # cap absoluto 8%

    Kelly negativo ou zero: nao entra (sem edge suficiente neste preco).
    Interpretacao: a p=0.87 ask, com edge=5%, kelly ~ 4-5% → frac ~ 1-1.25%.

    Parametros configuráveis:
      KELLY_ASSUMED_EDGE = 0.05  — edge assumido quando HFT da luz verde
      KELLY_FRACTION     = 0.25  — fraccao de Kelly (seguranca)
      KELLY_MAX_RISK_PCT = 0.08  — cap de 8% da banca por trade
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
      >= GAMB_MIN_OBI (0.60): compradores dominam — suporte real.
      <= SL_PANIC_OBI (0.25): compradores abandonaram — panico.

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
# SDK HELPERS — spread
# =============================================================================

def fetch_spread_sdk(token_id: str):
    """
    Obtem spread nativo via SDK Polymarket (uso interno para best_spreads_c).
    client.get_spread(token_id)["spread"] * 100 = cents absolutos.
    Fallback: retorna None (caller usa ultimo spread conhecido).
    """
    if clob_ro_client is None:
        return None
    try:
        result = clob_ro_client.get_spread(token_id)
        raw    = result.get("spread")
        if raw is None:
            return None
        return float(raw) * 100.0
    except Exception as e:
        log_warn(f"fetch_spread_sdk ({token_id[:12]}...): {e} — usando ultimo spread conhecido")
        return None

# =============================================================================
# API HELPERS
# =============================================================================

def fetch_metadata(slug: str):
    try:
        data = requests.get(f"{GAMMA_API_URL}/events?slug={slug}", timeout=5).json()[0]["markets"][0]
        ids  = json.loads(data["clobTokenIds"])
        return {"id": data["conditionId"], "up": ids[0], "down": ids[1], "slug": slug}
    except Exception as e:
        log_warn(f"fetch_metadata falhou ({slug}): {e}")
        return None

def fetch_fee_rate_bps(token_id: str) -> int:
    """
    Fetch dinamico de fee_rate_bps antes de cada ordem LIVE.
    docs: 'Always fetch fee_rate_bps dynamically — do not hardcode.'
    """
    try:
        r = requests.get(f"{CLOB_REST_URL}/fee-rate", params={"token_id": token_id}, timeout=4)
        return int(r.json().get("fee_rate_bps", 0))
    except Exception as e:
        log_warn(f"fetch_fee_rate_bps falhou ({token_id[:12]}...): {e}")
        return 0

def get_current_slug():
    now      = time.time()
    start_ts = now - (now % 300)
    if (start_ts + 300 - now) < 5:
        start_ts += 300
    return f"xrp-updown-5m-{int(start_ts)}", start_ts

def fetch_trend_from_clob(token_id: str) -> str:
    """
    Fetch do historico de preco via SDK nativo e calculo de tendencia.

    v1.3.0: token_id e agora o token UP do ciclo actual de 5min,
    alimentado por main() via xrp_1h_token_up = meta["up"].
    TREND_INTERVAL="1h" para cobrir a ultima hora de price action.
    Nao e feita nenhuma pesquisa de mercado — o token e passado directamente.

    SDK nativo (docs trading/orderbook#price-history):
        history = clob_ro_client.get_prices_history(
            market=token_id,   # param chama-se "market" mas recebe token_id
            interval="1h",     # historico da ultima hora (TREND_INTERVAL)
            fidelity=60        # 60 data points (TREND_FIDELITY)
        )

    Iteracao dos pontos (exactamente como os docs mostram):
        for point in history:
            t = point["t"]   # unix timestamp
            p = point["p"]   # price float

    Primeiros TREND_LOG_PTS e ultimos TREND_LOG_PTS sao logados para ficheiro.
    Filtra pontos da ultima hora (t >= now-3600) para calculo de tendencia 1h.
    Delta = last_avg - first_avg; threshold = TREND_THRESHOLD.
    Fallback para REST manual se SDK falhar.
    """
    now_ts   = time.time()
    hour_ago = now_ts - 3600.0

    def _calc_and_log(history_list, source):
        n_total = len(history_list)
        if n_total == 0:
            log_warn(f"TREND {source} | historico vazio")
            return "NEUTRAL"
        log_info(f"TREND {source} | {n_total} pontos totais | token={token_id[:16]}...")
        for i, pt in enumerate(history_list[:TREND_LOG_PTS]):
            log_info(f"TREND DATA [{i:02d}] | t={pt['t']} p={pt['p']:.4f}")
        if n_total > TREND_LOG_PTS * 2:
            log_info(f"TREND DATA ... ({n_total - TREND_LOG_PTS*2} pontos omitidos)")
        for i, pt in enumerate(history_list[-TREND_LOG_PTS:]):
            log_info(f"TREND DATA [{n_total - TREND_LOG_PTS + i:02d}] | t={pt['t']} p={pt['p']:.4f}")
        pts_1h = [float(pt["p"]) for pt in history_list if float(pt.get("t", 0)) >= hour_ago]
        log_info(f"TREND | Pontos na ultima hora: {len(pts_1h)}/{n_total}")
        if len(pts_1h) < 3:
            pts_1h = [float(pt["p"]) for pt in history_list]
            log_info(f"TREND | Poucos pontos 1h — usando todos ({len(pts_1h)})")
        n = len(pts_1h)
        if n < 3:
            log_warn(f"TREND | Insuficientes ({n} pts) — NEUTRAL")
            return "NEUTRAL"
        third     = max(1, n // 3)
        first_avg = sum(pts_1h[:third]) / third
        last_avg  = sum(pts_1h[-third:]) / third
        delta     = last_avg - first_avg
        log_info(
            f"TREND CALC | first_avg={first_avg:.4f} last_avg={last_avg:.4f} "
            f"delta={delta:+.4f} threshold={TREND_THRESHOLD:.3f}"
        )
        if   delta >  TREND_THRESHOLD: result = "UP"
        elif delta < -TREND_THRESHOLD: result = "DOWN"
        else:                          result = "NEUTRAL"
        log_info(f"TREND RESULT | {result}")
        return result

    if clob_ro_client is not None:
        try:
            log_info(
                f"TREND SDK | get_prices_history(market={token_id[:16]}..., "
                f"interval={TREND_INTERVAL!r}, fidelity={TREND_FIDELITY})"
            )
            history_raw = clob_ro_client.get_prices_history(
                market=token_id, interval=TREND_INTERVAL, fidelity=TREND_FIDELITY
            )
            if isinstance(history_raw, dict):
                history_raw = history_raw.get("history", [])
            history_list = list(history_raw) if history_raw else []
            return _calc_and_log(history_list, "SDK")
        except Exception as e:
            log_warn(f"TREND SDK get_prices_history falhou: {e} — fallback REST")

    try:
        log_info(
            f"TREND REST | GET /prices-history market={token_id[:16]}... "
            f"interval={TREND_INTERVAL} fidelity={TREND_FIDELITY}"
        )
        r = requests.get(
            f"{CLOB_REST_URL}/prices-history",
            params={"market": token_id, "interval": TREND_INTERVAL, "fidelity": TREND_FIDELITY},
            timeout=6
        )
        history_list = r.json().get("history", [])
        return _calc_and_log(history_list, "REST")
    except Exception as e:
        log_warn(f"TREND REST falhou: {e}")
        return "NEUTRAL"

def fetch_live_bankroll():
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
    """
    v1.3.0: Removida pesquisa de mercado (find_1h_xrp_up_token eliminada).
    xrp_1h_token_up e agora alimentado por main() com meta["up"] a cada ciclo.
    A task apenas verifica se o token esta disponivel e chama fetch_trend_from_clob.
    Nenhum get_markets() e chamado — sem WARNs de mercado nao encontrado.
    """
    global xrp_1h_trend
    while True:
        try:
            if xrp_1h_token_up is not None:
                new_t = fetch_trend_from_clob(xrp_1h_token_up)
                if new_t != xrp_1h_trend:
                    log_info(f"TREND UPDATE | {xrp_1h_trend} -> {new_t} | interval={TREND_INTERVAL}")
                    xrp_1h_trend = new_t
                else:
                    log_info(f"TREND STABLE | {xrp_1h_trend}")
            else:
                log_info("TREND | Token ainda nao disponivel — aguardando ciclo de mercado")
        except Exception as e:
            log_warn(f"trend_update_task erro: {e}")
        await asyncio.sleep(TREND_UPDATE_S)

# =============================================================================
# WEBSOCKET HANDLER
# =============================================================================

async def ws_handler(t_up: str, t_down: str):
    """
    WebSocket handler. Actualiza best_bids, best_asks, best_spreads_c,
    best_bid_sizes e best_ask_sizes por tick.

    v1.4.0: Sizes do Top of Book capturados no evento "book" para OBI.
            best_bid_ask e price_change nao fornecem sizes — mantidos do ultimo book.
    v1.3.0: WS e a UNICA fonte de precos iniciais (sem chamadas REST).

    Eventos tratados (docs trading/orderbook#event-types):
      book          -> bids/asks + SIZES -> best_bid, best_ask, sizes
      best_bid_ask  -> best_bid, best_ask (sem sizes)
      price_change  -> price_changes[].best_bid / best_ask (sem sizes)
      market_resolved -> winning_asset_id (req custom_feature_enabled)

    Precos (docs trading/orderbook#prices):
      ASK = preco ao comprar (lowest ask) -> BUY side
      BID = preco ao vender (highest bid) -> SELL side
    """
    global resolved_winner_asset
    _bids   = best_bids
    _asks   = best_asks
    _sprc   = best_spreads_c
    _bsizes = best_bid_sizes
    _asizes = best_ask_sizes
    _set    = price_change.set
    _loop   = asyncio.get_event_loop()

    _tid_map = {t_up: "up", t_down: "down"}

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
                        sk  = _tid_map.get(aid)
                        if sk is None:
                            continue

                        bid_p = ask_p = None

                        if evt == "book":
                            # Captura price E size do best bid/ask para OBI
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
                            try:
                                sp_c = await _loop.run_in_executor(None, fetch_spread_sdk, aid)
                                if sp_c is not None:
                                    _sprc[sk] = sp_c
                            except Exception:
                                pass

                        elif evt == "best_bid_ask":
                            # Sem sizes — mantemos os ultimos valores do evento "book"
                            bb = item.get("best_bid")
                            ba = item.get("best_ask")
                            if bb: bid_p = float(bb)
                            if ba: ask_p = float(ba)
                            sp_raw = item.get("spread")
                            if sp_raw is not None:
                                _sprc[sk] = float(sp_raw) * 100.0

                        elif evt == "price_change":
                            # Sem sizes — mantemos os ultimos valores do evento "book"
                            pcs = item.get("price_changes", [])
                            if pcs:
                                bb = pcs[-1].get("best_bid")
                                ba = pcs[-1].get("best_ask")
                                if bb: bid_p = float(bb)
                                if ba: ask_p = float(ba)
                            try:
                                sp_c = await _loop.run_in_executor(None, fetch_spread_sdk, aid)
                                if sp_c is not None:
                                    _sprc[sk] = sp_c
                            except Exception:
                                pass

                        if bid_p is not None:
                            _bids[sk] = bid_p
                            updated   = True
                        if ask_p is not None:
                            _asks[sk] = ask_p
                            updated   = True

                    if updated:
                        _set()

        except asyncio.CancelledError:
            break
        except Exception as e:
            log_warn(f"WS erro: {e} — reconectando em 1s")
            await asyncio.sleep(1)

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
        self.q: float       = q    # ruido do processo
        self.r: float       = r    # ruido de medicao
        self.x: float | None = None  # estado estimado (None = nao inicializado)
        self.p: float       = 1.0  # covariancia do erro (incerteza inicial)

    def update(self, z: float) -> float:
        """
        Actualiza o filtro com nova observacao z.
        Retorna o preco suavizado estimado.
        Primeiro passo: inicializa x=z (sem historico anterior).
        """
        if self.x is None:
            self.x = z
            return z
        # Predicao
        x_pred = self.x
        p_pred = self.p + self.q
        # Actualizacao
        k      = p_pred / (p_pred + self.r)   # ganho de Kalman
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
      .add(price, ts)    -> adiciona ponto, expira >30s
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
      Z   >= TP_SPIKE_ZSCORE:  wick absurdo para cima -> vender

    Uso no STOP-LOSS:
      Z   <= SL_CRASH_ZSCORE:  crash violento (-2 StdDev) -> vender
    """
    __slots__ = ("window_s", "data")

    def __init__(self, window_s: float = 30.0):
        self.window_s: float = window_s
        self.data: deque     = deque()  # deque de (timestamp, preco_Kalman)

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
        Usado para filtro de regime: std <= GAMB_MAX_VOL_DEV = regime estavel.
        """
        _, s, n = self._stats()
        return s  # None se n < 3, float caso contrario

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

    VPIN = |buy_vol - sell_vol| / (buy_vol + sell_vol) sobre janela de 30s.

    Range: 0.0 (fluxo 100% equilibrado) a 1.0 (fluxo 100% unilateral).

    Interpretacao:
      0.0 - 0.45: fluxo normal, equilibrado — mercado organico.
      0.45 - 0.70: alguma pressao direcional — monitorizar.
      >= VPIN_SAFE_LIMIT (0.70): desequilibrio significativo — bloqueia gambling.
      >= SL_TOXIC_VPIN   (0.85): dump/pump institucional — SL imediato.

    Uso:
      vpin = VPINTracker(window_s=30.0)
      vpin.add(kal_price, total_size, now)
      toxicity = vpin.vpin()  # None se sem dados
      vpin.reset()  # no inicio de cada ciclo
    """
    __slots__ = ("window_s", "data", "prev_mid")

    def __init__(self, window_s: float = 30.0):
        self.window_s: float      = window_s
        self.data:     deque      = deque()  # (timestamp, signed_volume)
        self.prev_mid: float | None = None

    def add(self, kal_mid: float, total_size: float, ts: float):
        """
        Adiciona tick. Classifica como BUY ou SELL por comparacao com tick anterior.
        signed_volume > 0 = buy-initiated, < 0 = sell-initiated.
        Expira automaticamente registos mais antigos que window_s.
        """
        if self.prev_mid is not None and total_size > 1e-9:
            if kal_mid > self.prev_mid:
                self.data.append((ts,  total_size))   # buy
            elif kal_mid < self.prev_mid:
                self.data.append((ts, -total_size))   # sell
            # tick neutro: nao classificado
        self.prev_mid = kal_mid
        # Expirar registos antigos
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
# LOGIC LOOP
# =============================================================================

async def logic_loop(m_start: float, m_end: float, meta: dict):
    """
    Loop principal de trading para um ciclo de 5 minutos.

    BUY ao ASK: docs 'you will pay the ask when buying' (orderbook/prices)
    SELL ao BID: docs 'receive the bid when selling'
    RESOLUCAO: winning tokens => $1/share, losing => $0 (concepts/resolution)

    PEG ARBIT: ask_sum = ask_up + ask_down <= PA_TRIGGER_SUM
               Compra simultanea via asyncio.gather. Risco fixo: PEG_ARBIT_RISK.

    GAMBLING — Motor Quantitativo HFT (v1.5.0):
               4 condicoes simultaneas obrigatorias:
               1. std(Kalman,30s) <= GAMB_MAX_VOL_DEV  (regime comprimido)
               2. Z(Kalman)       <= GAMB_MAX_ZSCORE    (nao em pico)
               3. OBI             >= GAMB_MIN_OBI        (compradores dominam)
               4. VPIN            <= VPIN_SAFE_LIMIT     (fluxo saudavel)
               Tamanho: calc_kelly_risk(ask) — sem Martingale.

    TAKE-PROFIT DINAMICO (v1.5.0):
               Se Z(Kalman) >= TP_SPIKE_ZSCORE: vende GAMBLING ao BID imediatamente.
               Captura wick antes de reversao a media Kalman.

    STOP-LOSS (v1.5.0 — OR logic):
               BID <= SL_BASE_TRIGGER E qualquer trigger:
               A. VPIN >= SL_TOXIC_VPIN   (dump institucional)
               B. Z   <= SL_CRASH_ZSCORE  (crash Kalman -2sigma)
               C. OBI <= SL_PANIC_OBI     (compradores abandonaram)
               Fecha APENAS GAMBLING do lado em crash. PEG ARBIT intacto.
    """
    global bankroll, daily_profit

    active_trades = []

    # Risco PA (fixo)
    eff_pa_risk = min(PEG_ARBIT_RISK, MAX_RISK_PERCENT)

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
            f",trend={xrp_1h_trend})"
        )
    if TAKE_PROFIT_ACTIVE:
        mods.append(f"TP_DIN(Z>={TP_SPIKE_ZSCORE})")
    if STOP_LOSS_ACTIVE:
        mods.append(
            f"SL_HFT(<={SL_BASE_TRIGGER:.2f}+OR:"
            f"VPIN>={SL_TOXIC_VPIN}/Z<={SL_CRASH_ZSCORE}/OBI<={SL_PANIC_OBI})"
        )

    log_sep2()
    log_info(f"NOVO CICLO | {meta['slug']} | LIVE={LIVE_TRADING}")
    log_info(f"Banca: ${bankroll:.4f} | Profit dia: ${daily_profit:+.4f}")
    log_info(f"Trend 1h: {xrp_1h_trend} | Modulos: {' | '.join(mods) if mods else 'nenhum'}")
    log_info(
        f"PA risk={eff_pa_risk:.1%}(fixo) | "
        f"GAMBLING: Kelly(edge={KELLY_ASSUMED_EDGE:.0%} "
        f"frac=1/{int(1/KELLY_FRACTION)} cap={KELLY_MAX_RISK_PCT:.0%})"
    )
    log_info(
        f"HFT: Kalman(Q={KALMAN_PROCESS_NOISE:.0e} R={KALMAN_MEASURE_NOISE:.0e}) "
        f"Window={HFT_WINDOW_SECONDS}s"
    )
    if GAMBLING_ACTIVE and xrp_1h_trend == "NEUTRAL" and not GAMB_NEUTRAL_BOTH:
        log_warn(
            "GAMBLING | NEUTRAL_BLOCK: trend=NEUTRAL e GAMB_NEUTRAL_BOTH=False "
            "-> Gambling nao vai disparar neste ciclo. "
            "Verifica o log TREND para diagnosticar o fetch do historico."
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
    gamb_last_buy           = {"UP": 0.0, "DOWN": 0.0}
    gamb_cutoff_logged      = False
    gamb_started_logged     = False
    gamb_neutral_block_last = 0.0

    pa_count      = 0
    last_pa_time  = 0.0
    prev_bid_up   = prev_bid_down = None

    # =========================================================================
    # LOOP PRINCIPAL
    # =========================================================================
    while True:
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
                log_info(f"Aguardando resolucao WS (max {RESOLVE_TIMEOUT_S:.0f}s)...")
                try:
                    await asyncio.wait_for(resolved_event.wait(), timeout=RESOLVE_TIMEOUT_S)
                    winner_asset = resolved_winner_asset
                    log_info(
                        f"RESOLUCAO CONFIRMADA | winner_asset="
                        f"{winner_asset[:16] if winner_asset else '?'}..."
                    )
                    for trade in active_trades[:]:
                        winner = (trade.get("token_id") == winner_asset)
                        close_trade_resolution(trade, winner, "00:00:000")
                        active_trades.remove(trade)
                except asyncio.TimeoutError:
                    log_warn(
                        f"Timeout {RESOLVE_TIMEOUT_S:.0f}s sem market_resolved WS — "
                        f"estimando vencedor por BID final"
                    )
                    est_winner = "up" if final_bid_up > final_bid_down else "down"
                    log_info(
                        f"Estimativa fallback: {est_winner.upper()} vencedor "
                        f"(BID_UP={fc(final_bid_up)} BID_DOWN={fc(final_bid_down)})"
                    )
                    for trade in active_trades[:]:
                        winner = (trade["side"].lower() == est_winner)
                        close_trade_resolution(trade, winner, "00:00:000")
                        active_trades.remove(trade)
            break

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
        #   2. HFTWindow recebe o preco Kalman (janela 30s para Z e StdDev)
        #   3. VPIN recebe preco Kalman + volume (janela 30s para toxicidade)
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
        _z_u   = f"{z_up:+.2f}"    if z_up   is not None else "n/a"
        _z_d   = f"{z_down:+.2f}"  if z_down  is not None else "n/a"
        _s_u   = f"{std_up:.4f}"   if std_up   is not None else "n/a"
        _s_d   = f"{std_down:.4f}" if std_down  is not None else "n/a"
        _o_u   = f"{obi_up:.2f}"   if obi_up   is not None else "n/a"
        _o_d   = f"{obi_down:.2f}" if obi_down  is not None else "n/a"
        _v_u   = f"{vpin_up:.2f}"  if vpin_up   is not None else "n/a"
        _v_d   = f"{vpin_down:.2f}" if vpin_down is not None else "n/a"
        log_raw(
            f"rem={rstr} | "
            f"UP  BID={fc(bid_up)} ASK={fc(ask_up)} "
            f"KAL={fc(kal_up)} Z={_z_u} σ={_s_u} OBI={_o_u} VPIN={_v_u} | "
            f"DN  BID={fc(bid_down)} ASK={fc(ask_down)} "
            f"KAL={fc(kal_down)} Z={_z_d} σ={_s_d} OBI={_o_d} VPIN={_v_d}"
            f"{peg_str}"
        )

        # =====================================================================
        # STOP-LOSS — OR logic inline por tick (v1.5.0)
        #
        # Arquitectura OR (v1.5.0) vs. AND (v1.4.0):
        #   v1.4.0: precisava de Z + Imbalance em simultâneo (AND).
        #   v1.5.0: basta BID <= threshold + QUALQUER trigger (A, B ou C).
        #
        # Rationale: um dump real raramente satisfaz todas as condições ao mesmo
        # tempo (liquidez pode não ter caído ainda quando VPIN já explodiu).
        # OR é mais responsivo e elimina falhas de detecção tardias.
        #
        # Trigger A — VPIN >= SL_TOXIC_VPIN: fluxo institucional de dump.
        # Trigger B — Z   <= SL_CRASH_ZSCORE: crash violento vs. Kalman.
        # Trigger C — OBI <= SL_PANIC_OBI: compradores abandonaram o livro.
        #
        # SL_TRIGGER e Z/OBI/VPIN None -> SL nao dispara (proteccao inicio ciclo).
        # =====================================================================
        if STOP_LOSS_ACTIVE and active_trades:
            for _sl_side, _sl_bid, _sl_z, _sl_obi, _sl_vpin in (
                ("UP",   bid_up,   z_up,   obi_up,   vpin_up),
                ("DOWN", bid_down, z_down, obi_down, vpin_down),
            ):
                # Verificar se ha posicoes GAMBLING neste lado
                _g_trades = [t for t in active_trades
                             if t["type"] == "GAMBLING" and t["side"] == _sl_side]
                if not _g_trades:
                    continue

                # Condicao base: BID passou a linha de perigo
                if _sl_bid > SL_BASE_TRIGGER:
                    continue

                # OR: qualquer trigger basta
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
                        f"(compradores abandonaram o livro)"
                    )
                else:
                    # Nenhum trigger activo — apenas loga WATCH
                    _z_diag   = f"{_sl_z:+.2f}"    if _sl_z   is not None else "n/a"
                    _obi_diag = f"{_sl_obi:.2f}"   if _sl_obi  is not None else "n/a"
                    _vpin_diag = f"{_sl_vpin:.2f}" if _sl_vpin is not None else "n/a"
                    log_m("STOPLOSS", "WATCH",
                        f"rem={rstr} | {_sl_side} BID={fc(_sl_bid)}<={SL_BASE_TRIGGER:.2f} "
                        f"| Z={_z_diag}(B<={SL_CRASH_ZSCORE}) "
                        f"OBI={_obi_diag}(C<={SL_PANIC_OBI}) "
                        f"VPIN={_vpin_diag}(A>={SL_TOXIC_VPIN}) — sem trigger activo"
                    )
                    continue

                # ── TRIGGER ACTIVO — PANIC SELL IMEDIATO ────────────────────
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
                # sem break — ciclo continua para gerir posicoes remanescentes

        # =====================================================================
        # TAKE-PROFIT DINAMICO — Wick Capture (v1.5.0)
        #
        # Se Z-Score(Kalman) >= TP_SPIKE_ZSCORE (2.5): o preco disparou de forma
        # estatisticamente absurda vs. a sua trajectoria Kalman dos ultimos 30s.
        # Vendemos imediatamente ao BID para capturar o topo do wick antes que
        # o preco reverta a media Kalman (Mean Reversion tipica em HFT).
        #
        # Coexiste com GAMB_TARGET_BID_C (TP estatico): o que disparar primeiro.
        # Apenas afecta posicoes GAMBLING — PEG ARBIT nao e afectado (resolve a $1).
        # =====================================================================
        if TAKE_PROFIT_ACTIVE and active_trades:
            for _tp_side, _tp_bid, _tp_z in (
                ("UP",   bid_up,   z_up),
                ("DOWN", bid_down, z_down),
            ):
                if _tp_z is None or _tp_z < TP_SPIKE_ZSCORE:
                    continue
                _tp_trades = [t for t in active_trades
                              if t["type"] == "GAMBLING" and t["side"] == _tp_side]
                if not _tp_trades:
                    continue
                log_sep()
                log_m("TP", "WICK",
                    f"rem={rstr} | {_tp_side} WICK DETECTADO | "
                    f"Z={_tp_z:+.2f}>={TP_SPIKE_ZSCORE} | BID={fc(_tp_bid)} | "
                    f"Kalman={fc(kalmans[_tp_side].x or 0)} | "
                    f"vendendo {len(_tp_trades)} pos GAMBLING"
                )
                for _tp_trade in list(_tp_trades):
                    close_trade(
                        _tp_trade, _tp_bid,
                        f"TP DINAMICO WICK Z={_tp_z:+.2f}>={TP_SPIKE_ZSCORE}",
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
        # ask_up + ask_down <= PA_TRIGGER_SUM (0.98): underpeg.
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
        # MODULO 2: GAMBLING — Motor Quantitativo HFT (v1.5.0)
        #
        # 4 condições de entrada em simultâneo (AND logic):
        #
        #   Cond 1 — REGIME (σ): std(Kalman,30s) <= GAMB_MAX_VOL_DEV
        #     Mercado comprimido = prestes a break direcional.
        #     Comprar durante volatilidade alta = topo de wick aleatorio.
        #     None (janela insuf.): bloqueia entrada (aguarda 30s de dados).
        #
        #   Cond 2 — Z-SCORE: Z(Kalman) <= GAMB_MAX_ZSCORE (1.0)
        #     Preco nao esta num pico estatistico anormal vs. Kalman.
        #     Z > 1.0 = preco ja subiu >1 desvio acima da media — armadilha de topo.
        #     None (janela insuf.): bloqueia entrada.
        #
        #   Cond 3 — OBI (suporte real): OBI >= GAMB_MIN_OBI (0.60)
        #     Compradores dominam 60%+ do livro = suporte confirmado.
        #     None (sizes indisponiveis): PASSA (graceful degradation).
        #
        #   Cond 4 — VPIN (fluxo saudavel): VPIN <= VPIN_SAFE_LIMIT (0.70)
        #     Sem atividade institucional toxica detectada.
        #     None (sem dados suficientes): PASSA (graceful degradation).
        #
        # Tamanho da posicao: Kelly Criterion dinamico.
        #   Se Kelly <= 0.0 (sem edge a este preco): nao entra.
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
                        f"| trend={xrp_1h_trend} | Kelly(edge={KELLY_ASSUMED_EDGE:.0%}"
                        f" frac=1/{int(1/KELLY_FRACTION)} cap={KELLY_MAX_RISK_PCT:.0%}) "
                        f"| HFT: σ<={GAMB_MAX_VOL_DEV} Z<={GAMB_MAX_ZSCORE} "
                        f"OBI>={GAMB_MIN_OBI:.0%} VPIN<={VPIN_SAFE_LIMIT:.0%}")

                if xrp_1h_trend == "NEUTRAL" and not GAMB_NEUTRAL_BOTH:
                    if now - gamb_neutral_block_last > 30.0:
                        gamb_neutral_block_last = now
                        log_m("GAMBLING", "NEUTRAL_BLOCK",
                            f"rem={rstr} | trend=NEUTRAL e GAMB_NEUTRAL_BOTH=False "
                            f"=> nenhuma entrada possivel. "
                            f"Verifica log TREND CALC para diagnosticar.")
                    continue

                for g_side, g_ask, g_bid, g_eff, g_z, g_std, g_obi, g_vpin in (
                    ("UP",   ask_up,   bid_up,   eff_up,   z_up,   std_up,   obi_up,   vpin_up),
                    ("DOWN", ask_down, bid_down, eff_down, z_down, std_down, obi_down, vpin_down)
                ):
                    if   xrp_1h_trend == "UP"   and g_side == "DOWN": continue
                    elif xrp_1h_trend == "DOWN"  and g_side == "UP":   continue

                    if now - gamb_last_buy[g_side] < GAMB_BUY_COOLDOWN:
                        continue

                    if not (GAMB_MIN_EFF_C <= g_eff <= GAMB_MAX_EFF_C):
                        continue

                    if ask_sum < GAMB_PEG_MIN:
                        continue

                    # ── Cond 1: Regime de compressao ─────────────────────────
                    # std None = janela < 3pts = bloqueia
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
                    # Z None = janela < 3pts = bloqueia
                    if g_z is None:
                        log_m("GAMBLING", "WAIT_ZSCORE",
                            f"rem={rstr} | {g_side} eff={g_eff:.1f}c σ={g_std:.4f} "
                            f"| Z=n/a (janela insuficiente)")
                        continue
                    if g_z > GAMB_MAX_ZSCORE:
                        log_m("GAMBLING", "BLOCK",
                            f"rem={rstr} | {g_side} eff={g_eff:.1f}c σ={g_std:.4f} "
                            f"| COND2 FAIL Z={g_z:+.2f}>{GAMB_MAX_ZSCORE} "
                            f"(pico anormal — armadilha de topo)")
                        continue

                    # ── Cond 3: OBI >= GAMB_MIN_OBI ──────────────────────────
                    # None = sizes indisponiveis = PASSA com WARN
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

                    # ── Cond 4: VPIN <= VPIN_SAFE_LIMIT ─────────────────────
                    # None = sem dados = PASSA com WARN
                    if g_vpin is not None and g_vpin > VPIN_SAFE_LIMIT:
                        log_m("GAMBLING", "BLOCK",
                            f"rem={rstr} | {g_side} eff={g_eff:.1f}c Z={g_z:+.2f} σ={g_std:.4f} "
                            f"| COND4 FAIL VPIN={g_vpin:.2f}>{VPIN_SAFE_LIMIT:.2f} "
                            f"(fluxo toxico — sem condicoes de entrada)")
                        continue
                    elif g_vpin is None:
                        log_m("GAMBLING", "WARN_VPIN",
                            f"rem={rstr} | {g_side} VPIN=n/a (janela em aquecimento) — "
                            f"COND4 skipped")

                    # ── Kelly Criterion — tamanho de posicao dinamico ────────
                    kelly_risk = calc_kelly_risk(g_ask)
                    if kelly_risk <= 0.0:
                        log_m("GAMBLING", "BLOCK",
                            f"rem={rstr} | {g_side} eff={g_eff:.1f}c ASK={fc(g_ask)} "
                            f"| Kelly<=0 (sem edge a este preco — não entra)")
                        continue

                    # ── TODAS AS CONDIÇÕES SATISFEITAS — ENTRADA ─────────────
                    _obi_s  = f"{g_obi:.2f}"  if g_obi  is not None else "n/a"
                    _vpin_s = f"{g_vpin:.2f}" if g_vpin is not None else "n/a"
                    if bankroll > 0.0:
                        token_id = meta["up"] if g_side == "UP" else meta["down"]
                        await open_trade(
                            g_side, "GAMBLING", rstr,
                            risk=kelly_risk,
                            token_id=token_id,
                            extra_log=(
                                f"Kelly={kelly_risk:.1%}(edge={KELLY_ASSUMED_EDGE:.0%}) "
                                f"σ={g_std:.4f}(cond1) "
                                f"Z={g_z:+.2f}(cond2) "
                                f"OBI={_obi_s}(cond3) "
                                f"VPIN={_vpin_s}(cond4)"
                            )
                        )
                        gamb_last_buy[g_side] = now
                        log_m("GAMBLING", "COOLDOWN",
                            f"rem={rstr} | {g_side} — cooldown {GAMB_BUY_COOLDOWN:.1f}s")

# =============================================================================
# MAIN
# =============================================================================

async def main():
    global daily_profit, last_day, bankroll, price_change
    global total_pnl_pos, total_pnl_neg, bot_start_time
    global xrp_1h_trend, xrp_1h_token_up
    global resolved_event, resolved_winner_asset

    # v1.5.0: Martingale state REMOVIDO (risk_multiplier, accumulated_loss,
    # recovery_rounds). Kelly Criterion calcula tamanho dinamicamente por trade.
    bot_start_time = time.time()
    total_pnl_pos  = 0.0
    total_pnl_neg  = 0.0
    xrp_1h_trend   = "NEUTRAL"
    xrp_1h_token_up = None

    if LIVE_TRADING:
        lb       = fetch_live_bankroll()
        bankroll = lb if lb is not None else BANKROLL_DEMO
    else:
        bankroll = BANKROLL_DEMO

    # ─ Log de arranque ───────────────────────────────────────────────────────
    log_sep2()
    log_info("BOT XRP POLYMARKET v1.5.0 INICIADO — CEREBRO QUANTITATIVO BIDIRECIONAL")
    log_sep2()
    log_info(f"LIVE_TRADING     : {LIVE_TRADING}")
    log_info(f"BANKROLL_INIT    : ${bankroll:.2f}")
    log_sep2()
    log_info("MOTOR QUANTITATIVO HFT:")
    log_info(f"   Kalman Q        : {KALMAN_PROCESS_NOISE:.0e} (ruido processo)")
    log_info(f"   Kalman R        : {KALMAN_MEASURE_NOISE:.0e} (ruido medicao)")
    log_info(f"   HFT Window      : {HFT_WINDOW_SECONDS}s (memoria do mercado)")
    log_info(f"   VPIN Window     : {HFT_WINDOW_SECONDS}s (classificacao fluxo)")
    log_sep2()
    log_info("PEG ARBIT:")
    log_info(f"   Activo          : {'ON' if PEG_ARBIT_ACTIVE else 'OFF'}")
    log_info(f"   Gatilho         : ask_sum <= {PA_TRIGGER_SUM:.3f} (underpeg)")
    log_info(f"   Risco           : {PEG_ARBIT_RISK:.0%} fixo (cap {MAX_RISK_PERCENT:.0%})")
    log_info(f"   Execucao        : asyncio.gather (ambos os lados)")
    log_sep2()
    log_info("GAMBLING (4 condições HFT simultâneas):")
    log_info(f"   Activo          : {'ON' if GAMBLING_ACTIVE else 'OFF'}")
    log_info(f"   Cond 1 Regime   : σ(Kalman,30s) <= {GAMB_MAX_VOL_DEV:.3f} (squeeze)")
    log_info(f"   Cond 2 Z-Score  : Z(Kalman)     <= {GAMB_MAX_ZSCORE:.1f} (anti-topo)")
    log_info(f"   Cond 3 OBI      : OBI           >= {GAMB_MIN_OBI:.0%} (suporte real)")
    log_info(f"   Cond 4 VPIN     : VPIN          <= {VPIN_SAFE_LIMIT:.0%} (fluxo OK)")
    log_info(f"   Gestao risco    : Kelly (edge={KELLY_ASSUMED_EDGE:.0%} frac=1/{int(1/KELLY_FRACTION)} cap={KELLY_MAX_RISK_PCT:.0%})")
    log_info(f"   Martingale      : REMOVIDO — Kelly substitui")
    log_sep2()
    log_info("TAKE-PROFIT DINAMICO (Wick Capture):")
    log_info(f"   Activo          : {'ON' if TAKE_PROFIT_ACTIVE else 'OFF'}")
    log_info(f"   TP_SPIKE_ZSCORE : {TP_SPIKE_ZSCORE:.1f} -> vende GAMBLING no wick de subida")
    log_info(f"   Logica          : Z(Kalman) >= {TP_SPIKE_ZSCORE} => sell imediato ao BID")
    log_sep2()
    log_info("STOP-LOSS (OR logic — qualquer trigger basta):")
    log_info(f"   Activo          : {'ON' if STOP_LOSS_ACTIVE else 'OFF'}")
    log_info(f"   Base trigger    : BID <= {SL_BASE_TRIGGER:.2f} ({SL_BASE_TRIGGER*100:.0f}c)")
    log_info(f"   Trigger A VPIN  : VPIN >= {SL_TOXIC_VPIN:.2f} (dump institucional)")
    log_info(f"   Trigger B Z     : Z   <= {SL_CRASH_ZSCORE:.1f} (crash Kalman)")
    log_info(f"   Trigger C OBI   : OBI <= {SL_PANIC_OBI:.2f} (livro abandonado)")
    log_info(f"   Logica          : OR (v1.4.0 era AND — mais responsivo)")
    log_info(f"   Fecha apenas    : GAMBLING do lado em crash (PEG ARBIT intacto)")
    log_sep2()
    log_info("TREND (v1.3.0 — sem pesquisa de mercado):")
    log_info(f"   Token alimentado por meta['up'] a cada ciclo")
    log_info(f"   TREND_INTERVAL={TREND_INTERVAL!r} fidelity={TREND_FIDELITY}")
    log_sep2()
    log_info("PRECOS (v1.3.0 — WS exclusivo, sem REST inicial):")
    log_info(f"   best_bids/asks/sizes iniciam a None")
    log_info(f"   WS preenche no primeiro tick do orderbook")
    log_sep2()

    trend_task = asyncio.create_task(trend_update_task())

    # ─ Loop de ciclos de 5 minutos ───────────────────────────────────────────
    while True:
        slug, start_ts = get_current_slug()
        meta = fetch_metadata(slug)
        if not meta:
            log_warn(f"Metadata nao encontrada para {slug} — retry em 1s")
            await asyncio.sleep(1)
            continue

        resolved_event.clear()
        resolved_winner_asset = None

        # Novo dia — reset daily profit
        market_day = datetime.fromtimestamp(start_ts).date()
        if last_day is None or market_day != last_day:
            daily_profit = 0.0
            last_day     = market_day
            if LIVE_TRADING:
                lb = fetch_live_bankroll()
                if lb is not None:
                    bankroll = lb
            log_sep2()
            log_info(f"NOVO DIA {market_day} | Banca: ${bankroll:.4f} | LIVE={LIVE_TRADING}")
            log_info("Kelly ativo — sem Martingale state para resetar.")
            log_sep2()

        # v1.3.0: alimenta token UP do ciclo actual para trend sem pesquisa de mercado.
        xrp_1h_token_up = meta["up"]
        log_info(f"TREND TOKEN | ciclo={slug} | token_up={meta['up'][:16]}... (sem pesquisa de mercado)")

        # v1.3.0/v1.4.0: todos os precos e sizes iniciam a None — WS preenche no 1o tick.
        best_bids["up"]      = best_bids["down"]      = None
        best_asks["up"]      = best_asks["down"]      = None
        best_spreads_c["up"] = best_spreads_c["down"] = None
        best_bid_sizes["up"] = best_bid_sizes["down"] = None
        best_ask_sizes["up"] = best_ask_sizes["down"] = None
        price_change.clear()

        ws_task = asyncio.create_task(ws_handler(meta["up"], meta["down"]))

        log_info("PRICES INIT | aguardando primeiro tick WS (sem chamadas REST)")
        await asyncio.sleep(1.0)

        if best_bids["up"] is not None:
            pre_bank = bankroll

            # v1.5.0: logic_loop sem parametros Martingale
            await logic_loop(start_ts, start_ts + 300, meta)

            profit_this   = bankroll - pre_bank
            daily_profit += profit_this

            if profit_this > 0.00001:
                total_pnl_pos += profit_this
            elif profit_this < -0.00001:
                total_pnl_neg += profit_this

            log_sep2()
            pnl_pct = (profit_this / pre_bank * 100.0) if pre_bank > 0 else 0.0
            dp_pct  = (daily_profit / (bankroll - daily_profit + profit_this) * 100.0
                       if (bankroll - daily_profit + profit_this) > 0 else 0.0)
            log_info(
                f"ROUND | PnL: ${profit_this:+.4f} ({pnl_pct:+.2f}%) | "
                f"Kelly(edge={KELLY_ASSUMED_EDGE:.0%} frac=1/{int(1/KELLY_FRACTION)} "
                f"cap={KELLY_MAX_RISK_PCT:.0%})"
            )
            log_info(
                f"TOTAL | PnL_dia: ${daily_profit:+.4f} ({dp_pct:+.2f}%) | "
                f"Banca: ${bankroll:.4f} | "
                f"Pos: ${total_pnl_pos:+.4f} | Neg: ${total_pnl_neg:+.4f} | "
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