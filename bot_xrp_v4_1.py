# =============================================================================
# BOT XRP POLYMARKET — v4.1.0
# =============================================================================
# CHANGELOG v4.1.0 [Bayesian Signal Processing + LMSR Inefficiency Detection]:
#
# ─────────────────────────────────────────────────────────────────────────────
# SOURCE: QR-PM-2026-0041 (Quantitative Research Division)
#   Doc 1: "Real-Time Bayesian Signal Processing Agent Decision Architecture"
#   Doc 2: "LMSR Pricing Mechanism & Inefficiency Detection"
# ─────────────────────────────────────────────────────────────────────────────
#
# [1] BAYESIAN SEQUENTIAL UPDATING (Doc 1, Eq. 2-3):
#     NEW class BayesianTracker — replaces static KELLY_ASSUMED_EDGE.
#     Maintains log-posterior P(UP wins | D1,...,Dt) updated per WS tick.
#     log P(H|D) = log P(H) + Σ log P(Dk|H) - log Z        (Eq. 3)
#     Prior: P(UP) = 0.50 (uninformative). Updated every tick via:
#       - Price movement likelihood (Kalman-filtered mid direction)
#       - Orderbook imbalance likelihood (OBI as signal strength)
#       - VPIN flow direction likelihood
#     Output: p_hat_up, p_hat_down = dynamic true probabilities.
#     REPLACES: KELLY_ASSUMED_EDGE (static 8%) with (p_hat - ask).
#
# [2] LMSR INEFFICIENCY DETECTION (Doc 2, Eq. 1-4):
#     NEW class LMSRPricer — computes theoretical fair prices.
#     Cost function: C(q) = b * ln(Σ e^(qi/b))              (Eq. 1)
#     Price (softmax): pi(q) = e^(qi/b) / Σ e^(qj/b)       (Eq. 3)
#     Key insight: LMSR prices sum to 1.0 EXACTLY by construction.
#       If market ask_up + ask_down != 1.0, there's an inefficiency.
#     NEW entry gate: LMSR_INEFFICIENCY — only enter when Bayesian
#       posterior diverges from market price by >= threshold.
#     Detects: mispricing = p_hat - market_ask (positive = underpriced).
#
# [3] DYNAMIC EV (Doc 1, Eq. 4):
#     EV = p_hat * (1-p) - (1-p_hat) * p = p_hat - p        (Eq. 4)
#     Where p_hat = Bayesian posterior, p = market ASK price.
#     REPLACES: static calc_ev() with dynamic Bayesian EV.
#     Entry gate: EV must be > 0 AND > LMSR inefficiency threshold.
#
# [4] FRACTIONAL KELLY (Doc 1 note: "NEVER full Kelly on 5min markets!"):
#     KELLY_FRACTION: 0.15 → 0.125 (1/8 Kelly — per handwritten note)
#     Dynamic edge: edge = p_hat - ask (Bayesian, not static assumed).
#     kelly = edge - (1-p_hat)/odds, scaled by 1/8.
#
# [5] PARAMETERS v4.1.0:
#     BAYESIAN_PRIOR          = 0.50  (uninformative start)
#     BAYESIAN_LIKELIHOOD_STD = 0.02  (likelihood spread)
#     BAYESIAN_MIN_EDGE       = 0.04  (min p_hat-ask to enter)
#     LMSR_B                  = 100000 (liquidity parameter)
#     LMSR_INEFF_THRESHOLD    = 0.02  (min inefficiency to enter)
#     KELLY_FRACTION           = 0.125 (1/8 Kelly per doc note)
#     GAMB_START_REM_S         = 60   (kept from v3)
#     GAMB_MIN_EFF_C           = 80   (slightly relaxed; Bayesian
#                                       filters handle quality now)
#
# ─────────────────────────────────────────────────────────────────────────────
# ARCHITECTURE v4.0.0 — SIGNAL FLOW:
#
#   WS tick → Kalman(mid) → BayesianTracker.update(kal, obi, vpin)
#                         → p_hat_up, p_hat_down (dynamic posteriors)
#                         → LMSRPricer.fair_price(quantities)
#                         → inefficiency = p_hat - market_ask
#                         → EV = p_hat - ask (Doc 1 Eq. 4)
#                         → Kelly sizing with dynamic edge
#                         → Entry if EV > 0 AND inefficiency > threshold
#                             AND all HFT filters pass
# ─────────────────────────────────────────────────────────────────────────────

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
from typing import Optional, List, Tuple

# =============================================================================
# PARAMETROS CONFIGURÁVEIS
# =============================================================================

LIVE_TRADING    = False      # Ativa a execução real de ordens. Falso = Paper Trading. Unidade: Booleano. Range: True / False.
BANKROLL_DEMO   = 10.0       # Saldo inicial para simulação (Paper Trading). Unidade: USD/USDC. Range: 0.0 - Infinito.
SLIPPAGE_TOLERANCE = 0.02    # Derrapagem máxima permitida no preço de execução. Unidade: USD (0.02 = 2 cêntimos). Range: 0.00 - 1.00.

# --- AGGRESSIVE ENDGAME ---
AGGRESSIVE_ENDGAME_ACTIVE = True  # Ativa o modo de trading agressivo nos últimos segundos do mercado. Unidade: Booleano. Range: True / False.
AGGRESSIVE_ENDGAME_RISK   = 0.03  # Percentagem da banca a arriscar neste modo. Unidade: Decimal (0.03 = 3%). Range: 0.0 - 1.0.
AGGRESSIVE_ENDGAME_S      = 15.0   # Tempo restante para ativar o Endgame. Unidade: Segundos. Range: 0.0 - 60.0.
AGGRESSIVE_ENDGAME_MIN_C  = 0.75  # Preço mínimo para entrar em posições no Endgame. Unidade: USD (75 cêntimos). Range: 0.0 - 1.0.
AGGRESSIVE_ENDGAME_MAX_C  = 0.95  # Preço máximo para entrar em posições no Endgame. Unidade: USD (95 cêntimos). Range: 0.0 - 1.0.

# --- RISK RULES ---
MAX_MARKET_EXPOSURE = 0.15   # Exposição máxima da banca permitida num único mercado. Unidade: Decimal (15%). Range: 0.0 - 1.0.
MIN_EXPECTED_PROFIT = 0.05   # Margem de lucro mínimo esperado para validar uma trade. Unidade: USD (5 cêntimos). Range: 0.0 - 1.0.

# --- MARTINGALE ---
MART_MAX_MULT = 8            # Multiplicador máximo de redobramento da aposta (recuperação de perdas). Unidade: Inteiro. Range: 1 - 20 (Alto risco).

# --- BASE RISK ---
PEG_ARBIT_RISK   = 0.25      # Fração da banca alocada especificamente a arbitragem. Unidade: Decimal (25%). Range: 0.0 - 1.0.
MAX_RISK_PERCENT = 5         # Risco global máximo por trade (geralmente % inteira). Unidade: Inteiro (1 = 1%). Range: 1 - 100.

# --- TOGGLES ---
PEG_ARBIT_ACTIVE   = True    # Ativa arbitragem Risk-Free (comprar ambos os lados). Unidade: Booleano.
GAMBLING_ACTIVE    = True    # Ativa trading direcional (apostar apenas num lado). Unidade: Booleano.
STOP_LOSS_ACTIVE   = False   # Ativa fecho automático de posições em prejuízo. Unidade: Booleano.
TAKE_PROFIT_ACTIVE = False   # Ativa fecho automático de posições em lucro. Unidade: Booleano.

# --- PEG ARBIT ---
PA_TRIGGER_SUM  = 0.990      # Soma máx dos Asks (Up+Down) para acionar arbitragem (1 cêntimo de margem). Unidade: USD. Range: 0.0 - 1.0.
PA_COOLDOWN     = 0.05       # Tempo de espera entre ordens de arbitragem. Unidade: Segundos. Range: 0.0 - 60.0.
PA_MIN_REM      = 1.0        # Tempo mínimo restante de mercado para permitir arbitragem. Unidade: Segundos. Range: 0.0 - Infinito.
PA_TARGET_BID_C = 0.0        # Preço Bid alvo para Limit Orders (0.0 = Market Taker). Unidade: USD. Range: 0.0 - 1.0.
MAX_PA_ENTRIES  = 10_000_000 # Limite máximo de trades de arbitragem na sessão. Unidade: Contagem (Inteiro). Range: 1 - Infinito.

# --- GAMBLING ---
GAMB_START_REM_S  = 300       # Só inicia trading direcional quando faltar este tempo. Unidade: Segundos. Range: 0 - Infinito.
GAMB_CUTOFF_S     = 0        # Para o trading direcional abaixo deste tempo. Unidade: Segundos. Range: 0 - GAMB_START_REM_S.
GAMB_MIN_ASK_C    = 75.0     # Preço mínimo ASK bruto para entrar (Notar: está em CÊNTIMOS reais). Unidade: Cêntimos. Range: 0.0 - 100.0.
GAMB_MAX_ASK_C    = 95.0     # Preço máximo ASK bruto para entrar. Unidade: Cêntimos. Range: 0.0 - 100.0.
GAMB_BUY_COOLDOWN = 15.0     # Pausa obrigatória entre compras direcionais. Unidade: Segundos. Range: 0.0 - Infinito.
GAMB_PEG_MIN      = 0.980    # Soma mínima aceitável do Peg de mercado para considerar o mercado líquido. Unidade: USD. Range: 0.0 - 1.0.
GAMB_TARGET_BID_C = 0.0      # Preço Bid alvo (0.0 = Market Order). Unidade: USD. Range: 0.0 - 1.0.

# --- FILTERS ---
MAX_SPREAD_CENTS  = 2.20     # Distância máxima permitida entre o melhor Bid e Ask. Unidade: Cêntimos. Range: 0.0 - 100.0.
BID_ASK_MIN_RATIO = 0.96     # Rácio mínimo de volume Bid/Ask para garantir liquidez bilateral. Unidade: Rácio. Range: 0.0 - 1.0+.

# --- HFT ENGINE ---
HFT_WINDOW_SECONDS   = 10    # Janela temporal para cálculo de métricas (VPIN, OBI). Unidade: Segundos. Range: 1 - 3600.
KALMAN_PROCESS_NOISE = 8e-6  # Q: Rapidez com que o modelo reage à mudança de preço (volatilidade real). Unidade: Variância. Range: 1e-8 - 1e-2.
KALMAN_MEASURE_NOISE = 4e-3  # R: Quantidade de "ruído" ignorada no mercado (micro-oscilações). Unidade: Variância. Range: 1e-6 - 1.0.

# --- GAMBLING ENTRY CONDITIONS ---
GAMB_MAX_VOL_DEV = 0.03      # Desvio máximo da volatilidade permitida para entrar. Unidade: Decimal (3%). Range: 0.0 - 1.0.
GAMB_MAX_ZSCORE  = 1.3       # Limite Z-Score para evitar comprar no "topo" de anomalias de preço. Unidade: Desvios Padrão. Range: 0.0 - 5.0.
GAMB_MIN_OBI     = 0.20      # Order Book Imbalance mínimo (Força compradora > 0). Unidade: Rácio (-1 a 1). Range: 0.0 - 1.0.
VPIN_SAFE_LIMIT  = 0.55      # Limite do Volume-Synchronized Probability of Informed Trading (Toxicidade alta = mau). Unidade: Probabilidade. Range: 0.0 - 1.0.

# --- ENDGAME OVERRIDE ---
ENDGAME_TRIGGER_S    = 30.999 # Tempo para ignorar filtros de segurança padrão e forçar Endgame. Unidade: Segundos. Range: 0.0 - 60.0.
ENDGAME_ZSCORE_LIMIT = 99.0   # Z-Score relaxado durante o Endgame (basicamente sem limite). Unidade: Desvios Padrão. Range: 0.0 - 100.0.
ENDGAME_VPIN_LIMIT   = 0.70   # Tolerância maior a fluxo tóxico (VPIN) no Endgame. Unidade: Probabilidade. Range: 0.0 - 1.0.

# --- TAKE-PROFIT ---
TP_SPIKE_ZSCORE     = 4.5    # Vender se o preço der um pico irrealista para cima. Unidade: Desvios Padrão. Range: 1.0 - 10.0.
TP_MIN_BID_OVER_ASK = 0.05   # Vender se o Bid atual ultrapassar em 5c o nosso Ask de entrada. Unidade: USD. Range: 0.0 - 1.0.
TP_MIN_PROFIT_PCT   = 0.05   # Percentagem de lucro bruto mínimo para fechar a trade. Unidade: Decimal (5%). Range: 0.0 - 1.0.

# --- STOP-LOSS ---
SL_TOXIC_VPIN   = 0.97       # Vender em pânico se o VPIN indicar despejo massivo e informado contra nós. Unidade: Probabilidade. Range: 0.0 - 1.0.
SL_CRASH_ZSCORE = -5.0       # Vender se o preço cair drasticamente além do normal (Flash Crash). Unidade: Desvios Padrão. Range: -10.0 a 0.0.
SL_PANIC_OBI    = 0.02       # Vender se os compradores (Bids) desaparecerem do livro de ordens. Unidade: Rácio (-1 a 1). Range: -1.0 a 1.0.
SL_BASE_TRIGGER = 0.25       # Perda percentual base tolerada antes de acionar o Stop-Loss genérico. Unidade: Decimal (25%). Range: 0.0 - 1.0.

# --- KELLY (v4.0.0) ---
KELLY_ASSUMED_EDGE = 0.08    # Vantagem probabilística assumida se o modelo Bayesiano falhar (Edge). Unidade: Probabilidade/USD. Range: 0.0 - 1.0.
KELLY_FRACTION     = 0.125   # Fração de Kelly (1/8) para evitar risco de ruína. Unidade: Fração/Decimal. Range: 0.0 - 1.0.
KELLY_MAX_RISK_PCT = 0.05    # Hard-cap percentual da banca arriscada por aposta Kelly. Unidade: Decimal (5%). Range: 0.0 - 1.0.

# ─────────────────────────────────────────────────────────────────────────────
# BAYESIAN SIGNAL PROCESSING
# ─────────────────────────────────────────────────────────────────────────────
BAYESIAN_PRIOR           = 0.50   # Probabilidade base inicial antes de avaliar o mercado. Unidade: Probabilidade. Range: 0.0 - 1.0.
BAYESIAN_LIKELIHOOD_STD  = 0.015  # Desvio padrão do modelo (Menor = reage mais rápido). Unidade: Desvio Padrão. Range: 0.0 - 1.0.
BAYESIAN_MIN_EDGE        = 0.04   # Diferença mínima entre Probabilidade Bayesiana e Ask de mercado para entrar. Unidade: Probabilidade. Range: 0.0 - 1.0.
BAYESIAN_DECAY           = 0.995  # Fator de esquecimento das previsões antigas (Evita overfit). Unidade: Fator (Decay). Range: 0.0 - 1.0.
BAYESIAN_MIN_TICKS       = 10     # Ticks de WS mínimos necessários antes de confiar na previsão. Unidade: Contagem. Range: 1 - 1000.

# ─────────────────────────────────────────────────────────────────────────────
# LMSR PRICING
# ─────────────────────────────────────────────────────────────────────────────
LMSR_B                = 100000.0  # Parâmetro de profundidade de liquidez teórica. Unidade: Fichas/Shares. Range: >0.
LMSR_INEFF_THRESHOLD  = 0.02      # Divergência mínima entre Preço Teórico e Ask para considerar Ineficiência. Unidade: USD (2 cêntimos). Range: 0.0 - 1.0.

# --- PRODUCTION ---
RATE_LIMIT_CALLS      = 8         # Número máximo de chamadas API por janela de tempo. Unidade: Contagem. Range: 1 - Limite da API.
RATE_LIMIT_BURST      = 15        # Limite máximo absoluto de chamadas API concorrentes. Unidade: Contagem. Range: > CALLS.
MAX_API_RETRIES       = 3         # Tentativas máximas por falha de API. Unidade: Contagem. Range: 1 - 10.
BASE_BACKOFF_S        = 1.0       # Tempo base de espera após erro de rede (Exponential Backoff). Unidade: Segundos. Range: 0.1 - 10.0.
MAX_BACKOFF_S         = 32.0      # Tempo máximo de espera após repetidos erros de rede. Unidade: Segundos. Range: > BASE_BACKOFF_S.
BACKOFF_JITTER        = True      # Adiciona aleatoriedade ao tempo de backoff para não saturar a rede. Unidade: Booleano. Range: True / False.
CB_FAIL_THRESHOLD     = 5         # Erros necessários para acionar o Circuit Breaker (pausa de segurança). Unidade: Contagem. Range: 1 - 20.
CB_RECOVERY_S         = 60.0      # Tempo de pausa do Circuit Breaker antes de tentar ligar de novo. Unidade: Segundos. Range: 10.0 - 3600.0.
WS_RECONNECT_BASE_S   = 1.0       # Tempo inicial para tentar reconexão ao WebSockets. Unidade: Segundos. Range: 0.1 - 10.0.
WS_RECONNECT_MAX_S    = 16.0      # Tempo máximo de backoff para reconexões do WebSockets. Unidade: Segundos. Range: > WS_RECONNECT_BASE.
WS_HEARTBEAT_INTERVAL = 20        # Frequência de pings para manter o WebSocket vivo. Unidade: Segundos. Range: 10 - 60.
WS_HEARTBEAT_TIMEOUT  = 10        # Tempo até considerar a ligação WS "morta" se não houver resposta. Unidade: Segundos. Range: 1 - 30.

# --- FEES ---
FEE_RATE = 0.25      # Taxa de transação teórica cobrada (se aplicável). Unidade: Percentagem (0.25%). Range: 0.0 - 5.0.
FEE_EXP  = 2         # Expoente usado no cálculo cumulativo de taxas. Unidade: Multiplicador/Expoente. Range: 1 - 3.

# --- LOOP ---
LOOP_SLEEP = 0.001   # Tempo de descanso do ciclo principal de CPU. Unidade: Segundos (1 milissegundo). Range: 0.0001 - 1.0.

# =============================================================================
# ENDPOINTS
# =============================================================================

CLOB_REST_URL = "https://clob.polymarket.com"
GAMMA_API_URL = "https://gamma-api.polymarket.com"
WS_URI        = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# =============================================================================
# GLOBAL STATE
# =============================================================================

bankroll              = BANKROLL_DEMO
daily_profit          = 0.0
last_day              = None
mart_multiplier       = 1
mart_accumulated_loss = 0.0

best_bids      = {"up": None, "down": None}
best_asks      = {"up": None, "down": None}
best_spreads_c = {"up": None, "down": None}
best_bid_sizes = {"up": None, "down": None}
best_ask_sizes = {"up": None, "down": None}

price_change        = asyncio.Event()
bot_start_time      = time.time()
_shutdown_flag      = False
resolved_event      = asyncio.Event()
resolved_winner_asset = None
total_pnl_pos       = 0.0
total_pnl_neg       = 0.0

# =============================================================================
# LOGGING
# =============================================================================

_fmt = logging.Formatter("%(message)s")
_fh  = logging.FileHandler("bot_xrp.log", encoding="utf-8")
_fh.setFormatter(_fmt)
logger = logging.getLogger("bot_xrp")
logger.setLevel(logging.DEBUG)
logger.addHandler(_fh)
logger.propagate = False
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("websockets.client").setLevel(logging.WARNING)

# =============================================================================
# FORMATTING
# =============================================================================

def get_ts() -> str:
    return datetime.now().strftime("%d/%m/%y | %H:%M:%S.%f")[:-3]

def get_remaining_str(rem: float) -> str:
    rem = max(0.0, rem)
    return (f"{int(rem // 60):02d}:{int(rem % 60):02d}:"
            f"{int((rem * 1000) % 1000):03d}")

def get_uptime_str() -> str:
    e = int(time.time() - bot_start_time)
    h, e = divmod(e, 3600)
    m, s = divmod(e, 60)
    return f"{h:02d}h:{m:02d}m:{s:02d}s"

def fc(p: float) -> str:
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

def log_ws_event(action: str, msg: str):
    logger.info(f"[WS] [{action}] [{get_ts()}] | {msg}")

def fmt_dollar(v: float) -> str:
    if v < 0:
        return f"$-{abs(v):.4f}"
    elif v > 0:
        return f"$+{v:.4f}"
    return f"${v:.4f}"

def fmt_pct(v: float) -> str:
    if v < 0:
        return f"-{abs(v):.2f}%"
    elif v > 0:
        return f"+{v:.2f}%"
    return f"{v:.2f}%"

def fmt_fee(fee: float, base: float) -> str:
    pct = (fee / base * 100.0) if base > 1e-9 else 0.0
    return f"{fmt_dollar(fee)} ({pct:.2f}%)"

# =============================================================================
# BAYESIAN SEQUENTIAL TRACKER (NEW v4.0.0)
# =============================================================================
# Doc 1, Equations 1-3:
#   P(H|D) = P(D|H) * P(H) / P(D)                       (Eq. 1)
#   P(H|D1,...,Dt) ∝ P(H) * Π P(Dk|H)                   (Eq. 2)
#   log P(H|D) = log P(H) + Σ log P(Dk|H) - log Z       (Eq. 3)
#
# Implementation: maintain log_posterior_up and log_posterior_down.
# Each tick: compute log-likelihood for both hypotheses, add to
# log-posterior, then normalize via log-sum-exp.
#
# Three likelihood signals per tick:
#   1. Price direction: Kalman mid moved up → evidence for UP
#   2. OBI signal: buyer dominance → evidence for dominant side
#   3. VPIN signal: low toxicity → healthy flow → supports trend
# =============================================================================

class BayesianTracker:
    """Sequential Bayesian probability estimator.

    Maintains log-posterior for P(UP wins) and P(DOWN wins)
    updated at every WS tick using three signal likelihoods.

    The posterior gives us p_hat — our best estimate of the
    true probability that each side wins. This replaces the
    static KELLY_ASSUMED_EDGE with a dynamic, data-driven edge.

    Doc 1, Eq. 3 (log-space for numerical stability):
      log P(H|D) = log P(H) + Σ log P(Dk|H) - log Z

    Attributes:
        log_post_up:   Log-posterior for UP winning.
        log_post_down: Log-posterior for DOWN winning.
        prev_kal_up:   Previous Kalman mid for UP (direction).
        prev_kal_down: Previous Kalman mid for DOWN (direction).
        tick_count:    Number of ticks processed.
        std:           Likelihood standard deviation.
        decay:         Exponential decay toward prior.
    """
    __slots__ = (
        "log_post_up", "log_post_down",
        "prev_kal_up", "prev_kal_down",
        "tick_count", "std", "decay",
    )

    def __init__(
        self,
        prior: float = 0.50,
        std: float = 0.015,
        decay: float = 0.995,
    ):
        # Eq. 3: start with log P(H) = log(prior)
        self.log_post_up: float   = math.log(max(prior, 1e-15))
        self.log_post_down: float = math.log(max(1.0 - prior, 1e-15))
        self.prev_kal_up: float | None  = None
        self.prev_kal_down: float | None = None
        self.tick_count: int      = 0
        self.std: float           = std
        self.decay: float         = decay

    def update(
        self,
        kal_up: float,
        kal_down: float,
        obi_up: float | None,
        obi_down: float | None,
        vpin_up: float | None,
        vpin_down: float | None,
    ) -> tuple[float, float]:
        """Update posteriors with new tick data.

        Returns (p_hat_up, p_hat_down) — posterior probabilities.

        Signal 1 — Price direction:
          If Kalman mid UP rose more than DOWN → evidence for UP.
          Likelihood: Gaussian centered on delta, std=self.std.

        Signal 2 — OBI dominance:
          OBI > 0.5 means buyers dominate → supports that side.
          Strength = (obi - 0.5) as evidence magnitude.

        Signal 3 — VPIN flow:
          Low VPIN = healthy flow = supports trend.
          High VPIN = toxic flow = weakens confidence.
        """
        self.tick_count += 1

        # Exponential decay toward prior (prevents overfit to noise)
        # Eq. 3 with forgetting: log P *= decay
        center = (self.log_post_up + self.log_post_down) / 2.0
        self.log_post_up = (
            center + self.decay * (self.log_post_up - center)
        )
        self.log_post_down = (
            center + self.decay * (self.log_post_down - center)
        )

        # Signal 1: Price direction (requires previous tick)
        if self.prev_kal_up is not None:
            delta_up = kal_up - self.prev_kal_up
            delta_down = kal_down - self.prev_kal_down

            # Net signal: positive = UP momentum, negative = DOWN
            net_signal = delta_up - delta_down

            # Log-likelihood: Gaussian with mean=net_signal
            # P(data | UP wins) ~ exp(-(-net_signal)^2 / (2*std^2))
            # P(data | DOWN wins) ~ exp(-(net_signal)^2 / (2*std^2))
            inv_2s2 = 1.0 / (2.0 * self.std * self.std)
            # UP hypothesis: expects positive net_signal
            ll_up = -((net_signal - abs(net_signal)) ** 2) * inv_2s2
            # Simplified: if net_signal > 0, ll_up boosted
            # Using direct signal strength:
            self.log_post_up   += net_signal / self.std * 0.5
            self.log_post_down -= net_signal / self.std * 0.5

        self.prev_kal_up   = kal_up
        self.prev_kal_down = kal_down

        # Signal 2: OBI dominance
        if obi_up is not None and obi_down is not None:
            # OBI > 0.5 = buyer dominance; asymmetric evidence
            obi_signal_up = (obi_up - 0.5) * 2.0     # [-1, +1]
            obi_signal_dn = (obi_down - 0.5) * 2.0
            # Net: positive if UP has more buyer support
            obi_net = (obi_signal_up - obi_signal_dn) * 0.3
            self.log_post_up   += obi_net
            self.log_post_down -= obi_net

        # Signal 3: VPIN flow health
        if vpin_up is not None and vpin_down is not None:
            # Low VPIN = healthy = supports that side's trend
            # Invert: healthy = (1 - vpin), signal = difference
            health_up = 1.0 - vpin_up
            health_dn = 1.0 - vpin_down
            vpin_net = (health_up - health_dn) * 0.2
            self.log_post_up   += vpin_net
            self.log_post_down -= vpin_net

        # Normalize via log-sum-exp (Eq. 3: - log Z)
        log_z = self._log_sum_exp(
            self.log_post_up, self.log_post_down,
        )
        p_up = math.exp(self.log_post_up - log_z)
        p_dn = math.exp(self.log_post_down - log_z)

        # Clamp to avoid numerical extremes
        p_up = max(0.01, min(0.99, p_up))
        p_dn = 1.0 - p_up

        return p_up, p_dn

    def get_posteriors(self) -> tuple[float, float]:
        """Get current posteriors without updating."""
        log_z = self._log_sum_exp(
            self.log_post_up, self.log_post_down,
        )
        p_up = math.exp(self.log_post_up - log_z)
        p_up = max(0.01, min(0.99, p_up))
        return p_up, 1.0 - p_up

    @staticmethod
    def _log_sum_exp(a: float, b: float) -> float:
        """Numerically stable log(exp(a) + exp(b))."""
        mx = max(a, b)
        return mx + math.log(
            math.exp(a - mx) + math.exp(b - mx)
        )

    def reset(self, prior: float = 0.50):
        """Reset for new market cycle."""
        self.log_post_up   = math.log(max(prior, 1e-15))
        self.log_post_down = math.log(max(1.0 - prior, 1e-15))
        self.prev_kal_up   = None
        self.prev_kal_down = None
        self.tick_count    = 0

# =============================================================================
# LMSR PRICER (NEW v4.0.0)
# =============================================================================
# Doc 2, Equations 1-4:
#   C(q) = b * ln(Σ e^(qi/b))                             (Eq. 1)
#   L_max = b * ln(n)                                      (Eq. 2)
#   pi(q) = e^(qi/b) / Σ e^(qj/b)    [SOFTMAX]           (Eq. 3)
#   Cost = C(q1,...,qi+δ,...) - C(q1,...,qi,...)           (Eq. 4)
#
# Key property: Σ pi = 1 and pi ∈ (0,1) ∀i
#   "This is identical to the softmax function in neural network
#    classifiers. The market is a neural network that prices beliefs."
#
# Usage: Compare LMSR fair prices with market ASK prices.
#   If market_ask < lmsr_fair → market underprices → BUY signal.
#   If market_ask > lmsr_fair → market overprices → avoid.
# =============================================================================

class LMSRPricer:
    """LMSR-based theoretical fair price calculator.

    Uses the Logarithmic Market Scoring Rule (Hanson 2003) to
    compute fair prices from outstanding quantities, then compares
    with actual market prices to detect inefficiencies.

    The softmax price function (Eq. 3) is identical to neural
    network classifiers — the market literally prices beliefs.

    Attributes:
        b: Liquidity parameter. Larger b = tighter spreads,
           higher max market maker loss (L_max = b * ln(n)).
    """
    __slots__ = ("b",)

    def __init__(self, b: float = 100000.0):
        self.b = b

    def cost(self, quantities: list[float]) -> float:
        """LMSR cost function C(q) = b * ln(Σ e^(qi/b)).

        Doc 2, Eq. 1. Uses log-sum-exp for numerical stability.
        """
        b = self.b
        max_q = max(quantities)
        # log-sum-exp trick: b * (max_q/b + ln(Σ exp((qi-max_q)/b)))
        s = sum(math.exp((qi - max_q) / b) for qi in quantities)
        return max_q + b * math.log(s)

    def prices(self, quantities: list[float]) -> list[float]:
        """LMSR price function (softmax).

        Doc 2, Eq. 3: pi(q) = e^(qi/b) / Σ e^(qj/b)

        Returns list of fair prices that sum to 1.0 exactly.
        This is the KEY formula (per handwritten note).
        """
        b = self.b
        max_q = max(quantities)
        exps = [math.exp((qi - max_q) / b) for qi in quantities]
        total = sum(exps)
        return [e / total for e in exps]

    def trade_cost(
        self,
        quantities: list[float],
        outcome_idx: int,
        delta: float,
    ) -> float:
        """Cost to buy delta shares of outcome_idx.

        Doc 2, Eq. 4:
          Cost = C(q1,...,qi+δ,...,qn) - C(q1,...,qi,...,qn)
        """
        c_before = self.cost(quantities)
        new_q = list(quantities)
        new_q[outcome_idx] += delta
        c_after = self.cost(new_q)
        return c_after - c_before

    def inefficiency(
        self,
        fair_prices: list[float],
        market_asks: list[float],
    ) -> list[float]:
        """Compute inefficiency per outcome.

        inefficiency_i = fair_price_i - market_ask_i
        Positive = market underprices outcome i → BUY signal.
        Negative = market overprices → avoid.

        Doc 2, Section 4: "Inefficiency Signal — Entry Condition"
        """
        return [
            fp - ma for fp, ma in zip(fair_prices, market_asks)
        ]

    def max_loss(self, n_outcomes: int = 2) -> float:
        """Maximum market maker loss. Doc 2, Eq. 2."""
        return self.b * math.log(n_outcomes)


# Global instances
lmsr_pricer = LMSRPricer(b=LMSR_B)

# =============================================================================
# PRODUCTION READY — Rate Limiter
# =============================================================================

class RateLimiter:
    __slots__ = (
        "calls_per_second", "burst", "tokens",
        "last_check", "_lock",
    )
    def __init__(self, calls_per_second=8.0, burst=15.0):
        self.calls_per_second = calls_per_second
        self.burst = burst
        self.tokens = burst
        self.last_check = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            delta = now - self.last_check
            self.last_check = now
            self.tokens = min(
                self.burst,
                self.tokens + delta * self.calls_per_second,
            )
            if self.tokens >= 1.0:
                self.tokens -= 1.0
            else:
                wait_s = (1.0 - self.tokens) / self.calls_per_second
                await asyncio.sleep(wait_s)
                self.tokens = 0.0

rate_limiter = RateLimiter(RATE_LIMIT_CALLS, RATE_LIMIT_BURST)

# =============================================================================
# Circuit Breaker
# =============================================================================

class CircuitBreaker:
    __slots__ = (
        "fail_threshold", "recovery_s",
        "_failures", "_state", "_opened_at",
    )
    STATE_CLOSED = "CLOSED"
    STATE_OPEN = "OPEN"
    STATE_HALF_OPEN = "HALF-OPEN"

    def __init__(self, fail_threshold=5, recovery_s=60.0):
        self.fail_threshold = fail_threshold
        self.recovery_s = recovery_s
        self._failures = 0
        self._state = self.STATE_CLOSED
        self._opened_at = 0.0

    def is_open(self) -> bool:
        if self._state == self.STATE_CLOSED:
            return False
        if self._state == self.STATE_OPEN:
            if time.monotonic() - self._opened_at >= self.recovery_s:
                self._state = self.STATE_HALF_OPEN
                return False
            return True
        return False

    def record_success(self):
        if self._state != self.STATE_CLOSED:
            log_info(f"CB | {self._state} -> CLOSED")
        self._state = self.STATE_CLOSED
        self._failures = 0

    def record_failure(self):
        self._failures += 1
        if self._state == self.STATE_HALF_OPEN:
            self._state = self.STATE_OPEN
            self._opened_at = time.monotonic()
        elif self._failures >= self.fail_threshold and self._state == self.STATE_CLOSED:
            self._state = self.STATE_OPEN
            self._opened_at = time.monotonic()
            log_warn(f"CB | CLOSED -> OPEN ({self._failures} failures)")

api_circuit_breaker = CircuitBreaker(CB_FAIL_THRESHOLD, CB_RECOVERY_S)

# =============================================================================
# Retry with Backoff
# =============================================================================

async def retry_with_backoff(fn, *args, label="call", **kwargs):
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
                await asyncio.sleep(backoff)
    log_warn(f"retry [{label}] GAVE UP: {last_exc}")
    return None

# =============================================================================
# SECRETS + SDK
# =============================================================================

def load_secrets(filepath="secrets.txt") -> dict:
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

_creds = load_secrets()
POLYMARKET_PRIVATE_KEY = _creds.get("POLYMARKET_PRIVATE_KEY", "")
clob_client = None
clob_ro_client = None

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY as SDK_BUY
    from py_clob_client.order_builder.constants import SELL as SDK_SELL
    _HAS_SDK = True
    clob_ro_client = ClobClient(host=CLOB_REST_URL, chain_id=137)
    if LIVE_TRADING:
        if not POLYMARKET_PRIVATE_KEY:
            raise SystemExit(1)
        clob_client = ClobClient(host=CLOB_REST_URL, key=POLYMARKET_PRIVATE_KEY, chain_id=137)
        log_info("SDK — LIVE TRADING ACTIVE")
    else:
        log_info("SDK — DEMO MODE")
except ImportError:
    _HAS_SDK = False
    if LIVE_TRADING:
        raise SystemExit(1)
    log_warn("py-clob-client not installed")
except Exception as _sdk_err:
    _HAS_SDK = False
    log_warn(f"SDK init: {_sdk_err}")

# =============================================================================
# CORE MATH
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

def calc_imbalance(bid_size, ask_size):
    if bid_size is None or ask_size is None:
        return None
    total = bid_size + ask_size
    if total <= 1e-9:
        return None
    return bid_size / total


def calc_ev_bayesian(p_hat: float, ask: float) -> float:
    """Dynamic EV using Bayesian posterior (Doc 1, Eq. 4).

    EV = p_hat * (1-p) - (1-p_hat) * p = p_hat - p

    Where p_hat = Bayesian posterior probability of winning,
          p = market ASK price (cost to enter).

    This is the CORRECT EV formula from the research doc.
    Replaces static calc_ev() which used KELLY_ASSUMED_EDGE.
    """
    return p_hat - ask


def calc_kelly_bayesian(
    p_hat: float, ask: float,
) -> float:
    """Kelly Criterion with dynamic Bayesian edge (v4.0.0).

    Doc 1: "NEVER full Kelly on 5min markets!"
    Uses 1/8 Kelly (KELLY_FRACTION = 0.125).

    edge = p_hat - ask (Bayesian, not static)
    odds = (1 - ask) / ask
    kelly = p_hat - (1 - p_hat) / odds
    risk = kelly * KELLY_FRACTION * mart_multiplier

    Falls back to static edge if Bayesian unavailable.
    """
    if ask <= 0.0 or ask >= 1.0:
        return 0.0
    odds = (1.0 - ask) / ask
    kelly = p_hat - (1.0 - p_hat) / odds
    if kelly <= 0.0:
        return 0.0
    frac = kelly * KELLY_FRACTION
    base_risk = min(frac, KELLY_MAX_RISK_PCT)
    scaled = base_risk * mart_multiplier
    return min(scaled, KELLY_MAX_RISK_PCT * MART_MAX_MULT)


def calc_kelly_risk(ask: float) -> float:
    """Fallback static Kelly (when Bayesian has < min ticks)."""
    if ask <= 0.0 or ask >= 1.0:
        return 0.0
    p_est = min(ask + KELLY_ASSUMED_EDGE, 0.995)
    odds = (1.0 - ask) / ask
    kelly = p_est - (1.0 - p_est) / odds
    if kelly <= 0.0:
        return 0.0
    frac = kelly * KELLY_FRACTION
    base_risk = min(frac, KELLY_MAX_RISK_PCT)
    scaled = base_risk * mart_multiplier
    return min(scaled, KELLY_MAX_RISK_PCT * MART_MAX_MULT)


def calc_ev(ask: float) -> float:
    """Legacy static EV (fallback)."""
    if ask <= 0.0 or ask >= 1.0:
        return 0.0
    p_est = min(ask + KELLY_ASSUMED_EDGE, 0.995)
    return p_est * (1.0 - ask) - (1.0 - p_est) * ask

# =============================================================================
# SDK/API HELPERS (unchanged from v3)
# =============================================================================

def _fetch_metadata_sync(slug):
    data = requests.get(f"{GAMMA_API_URL}/events?slug={slug}", timeout=5).json()[0]["markets"][0]
    ids = json.loads(data["clobTokenIds"])
    return {"id": data["conditionId"], "up": ids[0], "down": ids[1], "slug": slug}

async def fetch_metadata(slug):
    if api_circuit_breaker.is_open():
        return None
    await rate_limiter.acquire()
    result = await retry_with_backoff(_fetch_metadata_sync, slug, label=f"meta({slug})")
    if result:
        api_circuit_breaker.record_success()
    else:
        api_circuit_breaker.record_failure()
    return result

def _fetch_fee_rate_bps_sync(token_id):
    r = requests.get(f"{CLOB_REST_URL}/fee-rate", params={"token_id": token_id}, timeout=4)
    return int(r.json().get("fee_rate_bps", 0))

async def fetch_fee_rate_bps(token_id):
    await rate_limiter.acquire()
    result = await retry_with_backoff(_fetch_fee_rate_bps_sync, token_id, label="fee_bps")
    return result if result is not None else 0

def get_current_slug():
    now = time.time()
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
    return await retry_with_backoff(_fetch_live_bankroll_sync, label="bankroll")

def redeem_live_position(shares, token_id):
    if not clob_client:
        return
    try:
        clob_client.redeem_positions(token_id=token_id, amount=shares)
    except Exception as e:
        log_warn(f"REDEEM failed: {e}")

# =============================================================================
# WEBSOCKET HANDLER (unchanged from v3)
# =============================================================================

async def ws_handler(t_up, t_down):
    global resolved_winner_asset
    _bids, _asks, _sprc = best_bids, best_asks, best_spreads_c
    _bsizes, _asizes = best_bid_sizes, best_ask_sizes
    _set = price_change.set
    _tid_map = {t_up: "up", t_down: "down"}
    _ws_backoff = WS_RECONNECT_BASE_S

    while not _shutdown_flag:
        try:
            async with websockets.connect(
                WS_URI,
                ping_interval=WS_HEARTBEAT_INTERVAL,
                ping_timeout=WS_HEARTBEAT_TIMEOUT,
            ) as ws:
                await ws.send(json.dumps({
                    "assets_ids": [t_up, t_down],
                    "type": "market",
                    "custom_feature_enabled": True,
                }))
                log_ws_event("OPEN", f"hb={WS_HEARTBEAT_INTERVAL}s/{WS_HEARTBEAT_TIMEOUT}s")
                _ws_backoff = WS_RECONNECT_BASE_S

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
                                log_ws_event("RESOLVED", f"winner={wa[:16]}...")
                            continue
                        aid = item.get("asset_id")
                        sk = _tid_map.get(aid)
                        if sk is None:
                            continue
                        bid_p = ask_p = None
                        if evt == "book":
                            bids_r = item.get("bids", [])
                            asks_r = item.get("asks", [])
                            if bids_r:
                                best_b_entry, best_b_price = None, -1.0
                                for d in bids_r:
                                    sz = float(d.get("size", 0))
                                    if sz <= 0: continue
                                    pr = float(d["price"])
                                    if pr > best_b_price:
                                        best_b_price, best_b_entry = pr, d
                                if best_b_entry:
                                    bid_p = best_b_price
                                    _bsizes[sk] = float(best_b_entry.get("size", 0))
                            if asks_r:
                                best_a_entry, best_a_price = None, float("inf")
                                for d in asks_r:
                                    sz = float(d.get("size", 0))
                                    if sz <= 0: continue
                                    pr = float(d["price"])
                                    if pr < best_a_price:
                                        best_a_price, best_a_entry = pr, d
                                if best_a_entry:
                                    ask_p = best_a_price
                                    _asizes[sk] = float(best_a_entry.get("size", 0))
                            if bid_p is not None and ask_p is not None:
                                _sprc[sk] = (ask_p - bid_p) * 100.0
                        elif evt == "best_bid_ask":
                            bb, ba = item.get("best_bid"), item.get("best_ask")
                            if bb: bid_p = float(bb)
                            if ba: ask_p = float(ba)
                            sp = item.get("spread")
                            if sp is not None:
                                _sprc[sk] = float(sp) * 100.0
                            elif bid_p and ask_p:
                                _sprc[sk] = (ask_p - bid_p) * 100.0
                        elif evt == "price_change":
                            pcs = item.get("price_changes", [])
                            if pcs:
                                bb, ba = pcs[-1].get("best_bid"), pcs[-1].get("best_ask")
                                if bb: bid_p = float(bb)
                                if ba: ask_p = float(ba)
                            if bid_p and ask_p:
                                _sprc[sk] = (ask_p - bid_p) * 100.0
                        if bid_p is not None:
                            _bids[sk] = bid_p; updated = True
                        if ask_p is not None:
                            _asks[sk] = ask_p; updated = True
                    if updated:
                        _set()
        except asyncio.CancelledError:
            break
        except Exception as e:
            log_ws_event("ERROR", f"{type(e).__name__}: {e} — {_ws_backoff:.1f}s")
            await asyncio.sleep(_ws_backoff)
            _ws_backoff = min(_ws_backoff * 2.0, WS_RECONNECT_MAX_S)

# =============================================================================
# LIVE ORDER
# =============================================================================

def _compute_worst_price(side, price, slippage):
    worst = price + slippage if side == "BUY" else price - slippage
    return round(max(0.01, min(0.99, worst)), 2)

async def place_live_order(side, ask, shares, token_id):
    if api_circuit_breaker.is_open():
        return False
    fee_bps = 0
    if LIVE_TRADING and clob_client:
        fee_bps = await fetch_fee_rate_bps(token_id)
    await rate_limiter.acquire()
    dollar_amount = shares * ask
    worst_price = _compute_worst_price("BUY", ask, SLIPPAGE_TOLERANCE)
    mode = "LIVE" if LIVE_TRADING else "DEMO"
    log_m("ORDER", "FOK_BUY", f"[{mode}] amount={dollar_amount:.4f} @ {fc(ask)} worst={fc(worst_price)}")
    if LIVE_TRADING and _HAS_SDK and clob_client:
        try:
            order = clob_client.create_market_order(
                token_id=token_id, side=SDK_BUY, amount=dollar_amount,
                price=worst_price, fee_rate_bps=fee_bps,
                options={"tick_size": "0.01", "neg_risk": False})
            resp = clob_client.post_order(order, OrderType.FOK)
            api_circuit_breaker.record_success()
            return True
        except Exception as exc:
            api_circuit_breaker.record_failure()
            log_warn(f"FOK FAILED: {exc}")
            return False
    else:
        log_m("ORDER", "FOK_OK", f"[DEMO] {int(time.time()*1000)}")
        return True

# =============================================================================
# KALMAN FILTER 1D
# =============================================================================

class KalmanFilter1D:
    __slots__ = ("q", "r", "x", "p")
    def __init__(self, q=1e-5, r=1e-2):
        self.q, self.r, self.x, self.p = q, r, None, 1.0

    def update(self, z):
        if self.x is None:
            self.x = z; return z
        x_pred = self.x
        p_pred = self.p + self.q
        k = p_pred / (p_pred + self.r)
        self.x = x_pred + k * (z - x_pred)
        self.p = (1.0 - k) * p_pred
        return self.x

    def reset(self):
        self.x, self.p = None, 1.0

# =============================================================================
# HFT WINDOW
# =============================================================================

class HFTWindow:
    __slots__ = ("window_s", "data")
    def __init__(self, window_s=10.0):
        self.window_s, self.data = window_s, deque()

    def add(self, price, ts):
        self.data.append((ts, price))
        cutoff = ts - self.window_s
        while self.data and self.data[0][0] < cutoff:
            self.data.popleft()

    def _stats(self):
        n = len(self.data)
        if n < 3: return None, None, n
        prices = [p for _, p in self.data]
        mean = sum(prices) / n
        var = sum((p - mean) ** 2 for p in prices) / n
        return mean, math.sqrt(var), n

    def zscore(self, price):
        mean, std, _ = self._stats()
        if mean is None: return None
        if std < 1e-9: return 0.0
        return (price - mean) / std

    def std(self):
        _, s, _ = self._stats()
        return s

    def size(self): return len(self.data)
    def clear(self): self.data.clear()

# =============================================================================
# VPIN TRACKER
# =============================================================================

class VPINTracker:
    __slots__ = ("window_s", "data", "prev_mid")
    def __init__(self, window_s=10.0):
        self.window_s, self.data, self.prev_mid = window_s, deque(), None

    def add(self, kal_mid, total_size, ts):
        if self.prev_mid is not None and total_size > 1e-9:
            if kal_mid > self.prev_mid:
                self.data.append((ts, total_size))
            elif kal_mid < self.prev_mid:
                self.data.append((ts, -total_size))
        self.prev_mid = kal_mid
        cutoff = ts - self.window_s
        while self.data and self.data[0][0] < cutoff:
            self.data.popleft()

    def vpin(self):
        if not self.data: return None
        buy_vol = sum(v for _, v in self.data if v > 0)
        sell_vol = sum(-v for _, v in self.data if v < 0)
        total = buy_vol + sell_vol
        if total < 1e-9: return None
        return abs(buy_vol - sell_vol) / total

    def reset(self):
        self.data.clear(); self.prev_mid = None

# =============================================================================
# LOGIC LOOP (v4.0.0 — Bayesian + LMSR + Dynamic Kelly)
# =============================================================================

async def logic_loop(m_start, m_end, meta):
    """Main trading loop with Bayesian signal processing and LMSR.

    Signal flow per tick:
      1. Kalman filter smooths mid prices
      2. BayesianTracker updates P(UP|data), P(DOWN|data)
      3. LMSRPricer computes fair prices via softmax
      4. Inefficiency = Bayesian p_hat vs market ask
      5. EV = p_hat - ask (Doc 1, Eq. 4)
      6. Kelly sizing with dynamic edge (1/8 Kelly)
      7. Entry only if EV > 0 AND inefficiency > threshold
           AND all HFT filters pass
    """
    global bankroll, daily_profit

    active_trades = []
    eff_pa_risk = min(PEG_ARBIT_RISK, MAX_RISK_PERCENT)
    gamb_committed_side = None

    # v4.0.0: Bayesian tracker for this cycle
    bayesian = BayesianTracker(
        prior=BAYESIAN_PRIOR,
        std=BAYESIAN_LIKELIHOOD_STD,
        decay=BAYESIAN_DECAY,
    )

    # Track LMSR quantities (cumulative buys per side)
    lmsr_qty = [0.0, 0.0]  # [UP quantity, DOWN quantity]

    # Header
    log_sep2()
    log_info(f"NOVO CICLO | {meta['slug']} | LIVE={LIVE_TRADING}")
    log_info(f"Banca: ${bankroll:.4f} | Profit dia: {fmt_dollar(daily_profit)} | Mart: x{mart_multiplier}")
    log_info(
        f"v4.0.0 BAYESIAN+LMSR | prior={BAYESIAN_PRIOR} | "
        f"lhood_std={BAYESIAN_LIKELIHOOD_STD} | decay={BAYESIAN_DECAY} | "
        f"min_edge={BAYESIAN_MIN_EDGE} | min_ticks={BAYESIAN_MIN_TICKS}"
    )
    log_info(
        f"LMSR: b={LMSR_B:.0f} | ineff_thresh={LMSR_INEFF_THRESHOLD} | "
        f"L_max=${lmsr_pricer.max_loss(2):.0f}"
    )
    log_info(
        f"Kelly: 1/{int(round(1/KELLY_FRACTION))} Kelly (doc: NEVER full on 5min) | "
        f"cap={KELLY_MAX_RISK_PCT:.0%} | ×Mart x{mart_multiplier}"
    )
    log_info(
        f"GAMB: start={GAMB_START_REM_S}s | ask=[{GAMB_MIN_ASK_C:.0f}c-{GAMB_MAX_ASK_C:.0f}c] | "
        f"both_sides=ON | PA_trigger<={PA_TRIGGER_SUM:.3f}"
    )
    log_sep()

    # =========================================================================
    # OPEN TRADE
    # =========================================================================
    async def open_trade(side, trade_type, rstr, risk,
                         extra_log=None, fixed_shares=None, token_id=None):
        global bankroll
        ask = best_asks.get(side.lower())
        bid = best_bids.get(side.lower())
        if ask is None or ask <= 0.0:
            return None

        current_exposure = sum(t["total_out"] for t in active_trades)
        max_exp = bankroll * MAX_MARKET_EXPOSURE
        if current_exposure >= max_exp:
            log_m(trade_type, "BLOCK_EXP", f"rem={rstr} | {side} | exp>={MAX_MARKET_EXPOSURE:.0%}")
            return None

        if fixed_shares is not None:
            shares = fixed_shares
            invested_pure = shares * ask
        else:
            invested_pure = bankroll * risk
            shares = invested_pure / ask

        max_per = bankroll * min(KELLY_MAX_RISK_PCT * mart_multiplier, KELLY_MAX_RISK_PCT * MART_MAX_MULT)
        if invested_pure > max_per and fixed_shares is None:
            invested_pure = max_per
            shares = invested_pure / ask

        fee_buy = fee_rate(ask) * invested_pure
        total_out = invested_pure + fee_buy

        if current_exposure + total_out > max_exp:
            room = max_exp - current_exposure
            if room <= 0.001:
                return None
            total_out = room
            fee_buy = total_out * fee_rate(ask) / (1.0 + fee_rate(ask))
            invested_pure = total_out - fee_buy
            shares = invested_pure / ask

        eff_c_val = eff_price_c(ask)
        target = None
        if trade_type == "PEG ARBIT" and PA_TARGET_BID_C > 0.0:
            target = PA_TARGET_BID_C / 100.0
        elif trade_type == "GAMBLING" and GAMB_TARGET_BID_C > 0.0:
            target = GAMB_TARGET_BID_C / 100.0

        bankroll -= total_out
        trade = {
            "side": side, "ask": ask, "bid_at_buy": bid,
            "eff_c": eff_c_val, "shares": shares, "target": target,
            "type": trade_type, "invested_pure": invested_pure,
            "fee_buy": fee_buy, "total_out": total_out, "token_id": token_id,
        }
        active_trades.append(trade)
        if LIVE_TRADING and token_id:
            await place_live_order(side, ask, shares, token_id)

        # Update LMSR quantities
        idx = 0 if side == "UP" else 1
        lmsr_qty[idx] += shares

        bid_s = f" | BID@buy={fc(bid)}" if bid else ""
        ext_s = f" | {extra_log}" if extra_log else ""
        p_up, p_dn = bayesian.get_posteriors()
        p_hat = p_up if side == "UP" else p_dn
        ev = calc_ev_bayesian(p_hat, ask) if trade_type == "GAMBLING" else 0.0
        fee_s = fmt_fee(fee_buy, invested_pure)
        log_m(trade_type, "BUY",
            f"rem={rstr} | {side} @ ASK={fc(ask)} eff={fc(eff_c_val/100)}{bid_s}"
            f" | invested={fmt_dollar(invested_pure)} | fee={fee_s}"
            f" | total={fmt_dollar(total_out)} | shares={shares:.4f}"
            f" | risk={risk:.1%}{ext_s} | EV={ev:+.4f} | p_hat={p_hat:.3f}")
        return trade

    def close_trade(trade, sell_bid, reason, rstr):
        global bankroll
        payout_bruto = trade["shares"] * sell_bid
        fee_sell = payout_bruto * fee_rate(sell_bid)
        payout_net = payout_bruto - fee_sell
        pnl = payout_net - trade["total_out"]
        pnl_pct = (pnl / trade["total_out"] * 100.0) if trade["total_out"] else 0.0
        bankroll += payout_net
        sign = "(+)" if pnl >= 0 else "(-)"
        fee_s = fmt_fee(fee_sell, payout_bruto if payout_bruto > 0 else 1.0)
        log_m(trade["type"], "SELL",
            f"rem={rstr} | {trade['side']} @ BID={fc(sell_bid)} "
            f"| bruto={fmt_dollar(payout_bruto)} | fee_sell={fee_s} "
            f"| net={fmt_dollar(payout_net)} "
            f"| PnL: {fmt_dollar(pnl)} ({fmt_pct(pnl_pct)}) {sign} | Reason: {reason}")
        return pnl

    def close_trade_resolution(trade, winner, rstr):
        global bankroll
        shares = trade["shares"]
        payout_net = resolution_payout(shares, winner)
        pnl = payout_net - trade["total_out"]
        pnl_pct = (pnl / trade["total_out"] * 100.0) if trade["total_out"] else 0.0
        bankroll += payout_net
        reason_s = "RESOLUCAO LOCAL GANHA ($1/share)" if winner else "RESOLUCAO LOCAL PERDIDA (Total)"
        price_s = "100.0c" if winner else "0.0c"
        sign = "(+)" if pnl >= 0 else "(-)"
        if LIVE_TRADING and winner and trade.get("token_id"):
            redeem_live_position(shares, trade["token_id"])
        log_m(trade["type"], "SELL",
            f"rem={rstr} | {trade['side']} @ {price_s} "
            f"| net={fmt_dollar(payout_net)} "
            f"| PnL: {fmt_dollar(pnl)} ({fmt_pct(pnl_pct)}) {sign} | Reason: {reason_s}")
        return pnl

    # Quant state
    kalmans = {"UP": KalmanFilter1D(KALMAN_PROCESS_NOISE, KALMAN_MEASURE_NOISE),
               "DOWN": KalmanFilter1D(KALMAN_PROCESS_NOISE, KALMAN_MEASURE_NOISE)}
    hft_wins = {"UP": HFTWindow(HFT_WINDOW_SECONDS), "DOWN": HFTWindow(HFT_WINDOW_SECONDS)}
    vpin_trackers = {"UP": VPINTracker(HFT_WINDOW_SECONDS), "DOWN": VPINTracker(HFT_WINDOW_SECONDS)}

    gamb_last_buy = {"UP": 0.0, "DOWN": 0.0}
    gamb_cutoff_logged = gamb_started_logged = False
    endgame_fired = False
    pa_count = 0
    last_pa_time = 0.0
    prev_bid_up = prev_bid_down = None

    while not _shutdown_flag:
        now = time.time()
        rem = m_end - now

        # ── End of market ───────────────────────────────
        if rem <= 0.0:
            final_ask_up = best_asks.get("up") or 0.0
            final_ask_down = best_asks.get("down") or 0.0
            log_sep()
            if final_ask_up >= final_ask_down:
                local_winner, winner_token = "UP", meta["up"]
            else:
                local_winner, winner_token = "DOWN", meta["down"]

            p_up, p_dn = bayesian.get_posteriors()
            log_info(
                f"FIM DE MERCADO | UP ASK={fc(final_ask_up)} DN ASK={fc(final_ask_down)} | "
                f"WINNER: {local_winner} | Bayesian final: P(UP)={p_up:.3f} P(DN)={p_dn:.3f} | "
                f"ticks={bayesian.tick_count}")

            if active_trades:
                for trade in list(active_trades):
                    close_trade_resolution(trade, trade.get("token_id") == winner_token, "00:00:000")
                active_trades.clear()
                log_info(f"SETTLEMENT DONE | Banca: ${bankroll:.4f}")
            log_sep()
            return

        try:
            await asyncio.wait_for(price_change.wait(), timeout=LOOP_SLEEP)
            price_change.clear()
        except asyncio.TimeoutError:
            pass

        bid_up, bid_down = best_bids.get("up"), best_bids.get("down")
        ask_up, ask_down = best_asks.get("up"), best_asks.get("down")
        if None in (bid_up, bid_down, ask_up, ask_down):
            continue
        if bid_up == prev_bid_up and bid_down == prev_bid_down:
            continue
        prev_bid_up, prev_bid_down = bid_up, bid_down

        ask_sum = ask_up + ask_down
        underpeg_c = (1.0 - ask_sum) * 100.0
        mid_up = (bid_up + ask_up) * 0.5
        mid_down = (bid_down + ask_down) * 0.5
        # v4.2: use raw ASK in cents (not eff_price_c with fees baked in)
        ask_up_c, ask_down_c = ask_up * 100.0, ask_down * 100.0

        # ── Kalman + HFT + VPIN ─────────────────────────
        kal_up = kalmans["UP"].update(mid_up)
        kal_down = kalmans["DOWN"].update(mid_down)
        hft_wins["UP"].add(kal_up, now)
        hft_wins["DOWN"].add(kal_down, now)
        z_up, z_down = hft_wins["UP"].zscore(kal_up), hft_wins["DOWN"].zscore(kal_down)
        std_up, std_down = hft_wins["UP"].std(), hft_wins["DOWN"].std()

        bs_up, as_up = best_bid_sizes.get("up"), best_ask_sizes.get("up")
        bs_down, as_down = best_bid_sizes.get("down"), best_ask_sizes.get("down")
        obi_up, obi_down = calc_imbalance(bs_up, as_up), calc_imbalance(bs_down, as_down)

        vol_up = ((bs_up or 0) + (as_up or 0)) or 1.0
        vol_down = ((bs_down or 0) + (as_down or 0)) or 1.0
        vpin_trackers["UP"].add(kal_up, vol_up, now)
        vpin_trackers["DOWN"].add(kal_down, vol_down, now)
        vpin_up, vpin_down = vpin_trackers["UP"].vpin(), vpin_trackers["DOWN"].vpin()

        # ── v4.0.0: BAYESIAN UPDATE (Doc 1, Eq. 2-3) ───
        p_hat_up, p_hat_down = bayesian.update(
            kal_up, kal_down, obi_up, obi_down, vpin_up, vpin_down,
        )

        # ── v4.0.0: LMSR FAIR PRICES (Doc 2, Eq. 3) ────
        lmsr_fair = lmsr_pricer.prices(lmsr_qty)
        lmsr_ineff = lmsr_pricer.inefficiency(
            [p_hat_up, p_hat_down],
            [ask_up, ask_down],
        )

        rstr = get_remaining_str(rem)

        # ── Tick log with Bayesian + LMSR ────────────────
        _z_u = f"{z_up:+.2f}" if z_up is not None else "n/a"
        _z_d = f"{z_down:+.2f}" if z_down is not None else "n/a"
        _s_u = f"{std_up:.4f}" if std_up is not None else "n/a"
        _s_d = f"{std_down:.4f}" if std_down is not None else "n/a"
        _o_u = f"{obi_up:.2f}" if obi_up is not None else "n/a"
        _o_d = f"{obi_down:.2f}" if obi_down is not None else "n/a"
        _v_u = f"{vpin_up:.2f}" if vpin_up is not None else "n/a"
        _v_d = f"{vpin_down:.2f}" if vpin_down is not None else "n/a"
        log_raw(
            f"rem={rstr} | "
            f"UP BID={fc(bid_up)} ASK={fc(ask_up)} Z={_z_u} σ={_s_u} OBI={_o_u} VPIN={_v_u} | "
            f"DN BID={fc(bid_down)} ASK={fc(ask_down)} Z={_z_d} σ={_s_d} OBI={_o_d} VPIN={_v_d} | "
            f"BAYES P(UP)={p_hat_up:.3f} P(DN)={p_hat_down:.3f} | "
            f"LMSR ineff=[{lmsr_ineff[0]:+.4f},{lmsr_ineff[1]:+.4f}] | "
            f"PEG={ask_sum:.4f}"
        )

        # ── AGGRESSIVE ENDGAME ──────────────────────────
        if (AGGRESSIVE_ENDGAME_ACTIVE and 0.0 < rem <= AGGRESSIVE_ENDGAME_S
                and not endgame_fired and bankroll > 0.0):
            _eg_candidates = []
            _ea_up, _ea_dn = best_asks.get("up"), best_asks.get("down")
            if _ea_up and AGGRESSIVE_ENDGAME_MIN_C <= _ea_up <= AGGRESSIVE_ENDGAME_MAX_C:
                _eg_candidates.append(("UP", meta["up"], _ea_up))
            if _ea_dn and AGGRESSIVE_ENDGAME_MIN_C <= _ea_dn <= AGGRESSIVE_ENDGAME_MAX_C:
                _eg_candidates.append(("DOWN", meta["down"], _ea_dn))
            if _eg_candidates:
                endgame_fired = True
                _eg_best = max(_eg_candidates, key=lambda x: x[2])
                _eg_side, _eg_tid, _eg_ask = _eg_best
                _eg_invest = min(bankroll * AGGRESSIVE_ENDGAME_RISK, bankroll * KELLY_MAX_RISK_PCT)
                _eg_shares = _eg_invest / _eg_ask
                _eg_fee = fee_rate(_eg_ask) * _eg_invest
                _eg_total = _eg_invest + _eg_fee
                _eg_exp = sum(t["total_out"] for t in active_trades)
                if _eg_exp + _eg_total <= bankroll * MAX_MARKET_EXPOSURE:
                    bankroll -= _eg_total
                    active_trades.append({
                        "side": _eg_side, "ask": _eg_ask, "bid_at_buy": best_bids.get(_eg_side.lower()),
                        "eff_c": eff_price_c(_eg_ask), "shares": _eg_shares, "target": None,
                        "type": "ENDGAME_AGG", "invested_pure": _eg_invest,
                        "fee_buy": _eg_fee, "total_out": _eg_total, "token_id": _eg_tid,
                    })
                    if LIVE_TRADING and _eg_tid:
                        await place_live_order(_eg_side, _eg_ask, _eg_shares, _eg_tid)
                    log_m("ENDGAME_AGG", "BUY",
                        f"rem={rstr} | {_eg_side} @ ASK={fc(_eg_ask)} | "
                        f"fee={fmt_fee(_eg_fee, _eg_invest)} | total={fmt_dollar(_eg_total)}")

        # ── STOP-LOSS ───────────────────────────────────
        if STOP_LOSS_ACTIVE and active_trades:
            for _sl_s, _sl_b, _sl_z, _sl_o, _sl_v in (
                ("UP", bid_up, z_up, obi_up, vpin_up),
                ("DOWN", bid_down, z_down, obi_down, vpin_down)):
                _gt = [t for t in active_trades if t["type"] == "GAMBLING" and t["side"] == _sl_s]
                if not _gt or _sl_b > SL_BASE_TRIGGER: continue
                reason = None
                if _sl_v is not None and _sl_v >= SL_TOXIC_VPIN: reason = f"VPIN={_sl_v:.2f}"
                elif _sl_z is not None and _sl_z <= SL_CRASH_ZSCORE: reason = f"Z={_sl_z:+.2f}"
                elif _sl_o is not None and _sl_o <= SL_PANIC_OBI: reason = f"OBI={_sl_o:.2f}"
                if reason:
                    for t in list(_gt):
                        close_trade(t, best_bids.get(t["side"].lower()) or 0.0, f"SL {reason}", rstr)
                        active_trades.remove(t)

        # ── TAKE-PROFIT ─────────────────────────────────
        if TAKE_PROFIT_ACTIVE and active_trades:
            for _tp_s, _tp_b, _tp_z in (("UP", bid_up, z_up), ("DOWN", bid_down, z_down)):
                if _tp_z is None: continue
                for t in [t for t in active_trades if t["type"] == "GAMBLING" and t["side"] == _tp_s]:
                    if _tp_b < t["ask"] + TP_MIN_BID_OVER_ASK: continue
                    if sell_payout_net(t["shares"], _tp_b) <= t["total_out"] * (1 + TP_MIN_PROFIT_PCT): continue
                    if _tp_z < TP_SPIKE_ZSCORE: continue
                    close_trade(t, _tp_b, f"TP Z={_tp_z:+.2f}", rstr)
                    active_trades.remove(t)

        # ── TARGET CHECK ────────────────────────────────
        for trade in active_trades[:]:
            if trade.get("target") and best_bids.get(trade["side"].lower(), 0) >= trade["target"]:
                close_trade(trade, best_bids[trade["side"].lower()], "TARGET", rstr)
                active_trades.remove(trade)

        # ── PEG ARBIT — Risk-Free Order Book Arbitrage (v4.3) ────
        #
        # Uses arb_engine.evaluate_arb() with full QA pipeline:
        #   Check 1: Peg = Lowest_Ask_UP + Lowest_Ask_DOWN (raw ASK)
        #   Check 2: Peg < 0.98 (2c margin for gas/slippage)
        #   Check 3: Liquidity check — volume at best ask >= shares
        #            VWAP fallback if top-of-book insufficient
        #   Check 4: Net profit > 0 after fees
        #   EQUAL shares on both sides.
        #
        if (PEG_ARBIT_ACTIVE and ask_sum <= PA_TRIGGER_SUM and rem > PA_MIN_REM
                and pa_count < MAX_PA_ENTRIES and now - last_pa_time >= PA_COOLDOWN):

            budget = bankroll * eff_pa_risk

            # Build OrderBookSide from WS state
            # Use best_asks + best_ask_sizes (top-of-book from WS)
            _as_up_sz = best_ask_sizes.get("up")
            _as_dn_sz = best_ask_sizes.get("down")

            # Construct minimal order book for arb evaluation
            # Top-of-book level from WS + approximate deeper levels
            from arb_engine import (
                OrderBookSide, OrderBookLevel, evaluate_arb as eval_arb,
                ArbStatus,
            )

            _ob_up_levels = []
            if ask_up and ask_up > 0 and _as_up_sz and _as_up_sz > 0:
                _ob_up_levels.append(
                    OrderBookLevel(price=ask_up, size=_as_up_sz)
                )
            _ob_dn_levels = []
            if ask_down and ask_down > 0 and _as_dn_sz and _as_dn_sz > 0:
                _ob_dn_levels.append(
                    OrderBookLevel(price=ask_down, size=_as_dn_sz)
                )

            _ob_up = OrderBookSide(levels=_ob_up_levels)
            _ob_dn = OrderBookSide(levels=_ob_dn_levels)

            arb = eval_arb(_ob_up, _ob_dn, budget=budget)

            if arb.status == ArbStatus.OPPORTUNITY:
                log_sep()
                log_m("PEG ARBIT", "OPORTUNIDADE DETETADA",
                    f"rem={rstr} | "
                    f"Lowest_Ask_UP={fc(arb.lowest_ask_up)} | "
                    f"Lowest_Ask_DOWN={fc(arb.lowest_ask_down)} | "
                    f"Peg={arb.peg:.4f} | "
                    f"Margem_Bruta={arb.gross_margin:.4f} "
                    f"({arb.gross_margin*100:.1f}c) | "
                    f"shares={arb.shares:.4f} (EQUAL) | "
                    f"cost=${arb.total_cost:.4f} | "
                    f"payout=${arb.payout:.4f} | "
                    f"Lucro_Esperado=${arb.net_profit:.4f} "
                    f"({arb.profit_pct:+.2f}%) | "
                    f"VWAP={arb.used_vwap} | "
                    f"vol_up={arb.volume_at_ask_up:.1f} "
                    f"vol_dn={arb.volume_at_ask_down:.1f} | "
                    f"#{pa_count+1}")
                await asyncio.gather(
                    open_trade(
                        "UP", "PEG ARBIT", rstr,
                        risk=eff_pa_risk,
                        fixed_shares=arb.shares,
                        token_id=meta["up"]),
                    open_trade(
                        "DOWN", "PEG ARBIT", rstr,
                        risk=eff_pa_risk,
                        fixed_shares=arb.shares,
                        token_id=meta["down"]))
                log_sep()
                pa_count += 1
                last_pa_time = now
            elif arb.status != ArbStatus.REJECT_PEG_TOO_HIGH:
                # Log non-obvious rejections (not just "peg too high")
                log_m("PEG ARBIT", arb.status.value,
                    f"rem={rstr} | {arb.reason}")

        # =================================================
        # GAMBLING (v4.0.0 — Bayesian + LMSR + Dynamic Kelly)
        # =================================================
        if GAMBLING_ACTIVE:
            if rem > GAMB_START_REM_S:
                pass
            elif rem <= GAMB_CUTOFF_S:
                if not gamb_cutoff_logged:
                    gamb_cutoff_logged = True
            else:
                if not gamb_started_logged:
                    gamb_started_logged = True
                    log_m("GAMBLING", "START",
                        f"rem={rstr} | Bayesian+LMSR active | Mart x{mart_multiplier} | both_sides=ON")

                _eff_zl = ENDGAME_ZSCORE_LIMIT if rem <= ENDGAME_TRIGGER_S else GAMB_MAX_ZSCORE
                _eff_vl = ENDGAME_VPIN_LIMIT if rem <= ENDGAME_TRIGGER_S else VPIN_SAFE_LIMIT
                _eg_on = rem <= ENDGAME_TRIGGER_S

                for (g_side, g_ask, g_bid, g_ask_c, g_z, g_std, g_obi, g_vpin,
                     g_p_hat, g_lmsr_ineff) in (
                    ("UP", ask_up, bid_up, ask_up_c, z_up, std_up, obi_up, vpin_up,
                     p_hat_up, lmsr_ineff[0]),
                    ("DOWN", ask_down, bid_down, ask_down_c, z_down, std_down, obi_down, vpin_down,
                     p_hat_down, lmsr_ineff[1]),
                ):
                    # v4.2: NO single-side lock — can buy both sides
                    # If market reverses, buying the other side reduces losses
                    if now - gamb_last_buy[g_side] < GAMB_BUY_COOLDOWN:
                        continue

                    # Spread filter
                    spread_c = best_spreads_c.get(g_side.lower())
                    if spread_c is None or spread_c > MAX_SPREAD_CENTS:
                        continue
                    # v4.2: filter on raw ASK cents (not eff_price with fees)
                    if not (GAMB_MIN_ASK_C <= g_ask_c <= GAMB_MAX_ASK_C):
                        continue

                    # BID/ASK ratio
                    if g_bid and g_ask > 0 and g_bid / g_ask < BID_ASK_MIN_RATIO:
                        continue

                    # Cond 1: Regime (σ)
                    if g_std is None or g_std > GAMB_MAX_VOL_DEV:
                        continue
                    # Cond 2: Z-Score
                    if g_z is None or g_z > _eff_zl:
                        continue
                    # Cond 3: OBI
                    if g_obi is not None and g_obi < GAMB_MIN_OBI:
                        continue
                    # Cond 4: VPIN
                    if g_vpin is not None and g_vpin > _eff_vl:
                        continue

                    # ── v4.0.0: BAYESIAN EDGE GATE (Doc 1, Eq. 4) ──
                    # EV = p_hat - ask. Must be positive.
                    bayes_ev = calc_ev_bayesian(g_p_hat, g_ask)
                    bayes_edge = g_p_hat - g_ask

                    if bayesian.tick_count >= BAYESIAN_MIN_TICKS:
                        # Use Bayesian posterior for edge
                        if bayes_edge < BAYESIAN_MIN_EDGE:
                            log_m("GAMBLING", "BLOCK_BAYES",
                                f"rem={rstr} | {g_side} | "
                                f"p_hat={g_p_hat:.3f} ask={fc(g_ask)} "
                                f"edge={bayes_edge:+.4f}<{BAYESIAN_MIN_EDGE} "
                                f"| EV={bayes_ev:+.4f} — insufficient Bayesian edge")
                            continue

                        # ── v4.0.0: LMSR INEFFICIENCY GATE (Doc 2, §4) ──
                        if g_lmsr_ineff < LMSR_INEFF_THRESHOLD:
                            log_m("GAMBLING", "BLOCK_LMSR",
                                f"rem={rstr} | {g_side} | "
                                f"ineff={g_lmsr_ineff:+.4f}<{LMSR_INEFF_THRESHOLD} "
                                f"| market fairly priced — no edge")
                            continue

                        # Dynamic Kelly with Bayesian p_hat
                        kelly_risk = calc_kelly_bayesian(g_p_hat, g_ask)
                    else:
                        # Fallback: static edge (not enough Bayesian data)
                        kelly_risk = calc_kelly_risk(g_ask)
                        bayes_ev = calc_ev(g_ask)

                    if kelly_risk <= 0.0:
                        continue

                    # ALL CONDITIONS MET — ENTER
                    _obi_s = f"{g_obi:.2f}" if g_obi is not None else "n/a"
                    _vpin_s = f"{g_vpin:.2f}" if g_vpin is not None else "n/a"
                    _ba_s = f"{g_bid/g_ask:.3f}" if g_bid and g_ask > 0 else "n/a"
                    _eg_tag = " | ENDGAME" if _eg_on else ""
                    _bayes_tag = "BAYESIAN" if bayesian.tick_count >= BAYESIAN_MIN_TICKS else "STATIC"

                    if bankroll > 0.0:
                        token_id = meta["up"] if g_side == "UP" else meta["down"]
                        await open_trade(
                            g_side, "GAMBLING", rstr, risk=kelly_risk, token_id=token_id,
                            extra_log=(
                                f"Kelly={kelly_risk:.1%}({_bayes_tag} "
                                f"p_hat={g_p_hat:.3f} edge={bayes_edge:+.3f} "
                                f"×Mart_x{mart_multiplier}) "
                                f"σ={g_std:.4f} Z={g_z:+.2f} "
                                f"OBI={_obi_s} VPIN={_vpin_s} BidAsk={_ba_s} "
                                f"LMSR_ineff={g_lmsr_ineff:+.4f}{_eg_tag}"))
                        # v4.2: both sides allowed — no side lock
                        gamb_last_buy[g_side] = now

# =============================================================================
# MAIN
# =============================================================================

async def main():
    global daily_profit, last_day, bankroll, price_change
    global total_pnl_pos, total_pnl_neg, bot_start_time
    global resolved_event, resolved_winner_asset, _shutdown_flag
    global mart_multiplier, mart_accumulated_loss

    def _handle_sigterm():
        global _shutdown_flag
        _shutdown_flag = True

    loop = asyncio.get_event_loop()
    try:
        loop.add_signal_handler(signal.SIGTERM, _handle_sigterm)
    except NotImplementedError:
        pass

    bot_start_time = time.time()
    total_pnl_pos = total_pnl_neg = 0.0

    log_sep2()
    log_info("BOT XRP POLYMARKET v4.0.0 — BAYESIAN + LMSR + DYNAMIC KELLY")
    log_sep2()
    log_info(f"LIVE={LIVE_TRADING} | BANKROLL=${bankroll:.2f}")
    log_info(f"Bayesian: prior={BAYESIAN_PRIOR} std={BAYESIAN_LIKELIHOOD_STD} "
             f"decay={BAYESIAN_DECAY} min_edge={BAYESIAN_MIN_EDGE} min_ticks={BAYESIAN_MIN_TICKS}")
    log_info(f"LMSR: b={LMSR_B:.0f} ineff={LMSR_INEFF_THRESHOLD} L_max=${lmsr_pricer.max_loss(2):.0f}")
    log_info(f"Kelly: 1/{int(round(1/KELLY_FRACTION))} (NEVER full on 5min) cap={KELLY_MAX_RISK_PCT:.0%} ×Mart")
    log_info(f"Martingale: x1→x2→x4→x8 (max={MART_MAX_MULT})")
    log_info(f"GAMB: start={GAMB_START_REM_S}s ask=[{GAMB_MIN_ASK_C:.0f}-{GAMB_MAX_ASK_C:.0f}c] both_sides=ON")
    log_info(f"PA: trigger<={PA_TRIGGER_SUM:.3f} (profit guaranteed after fees)")
    log_sep2()

    while not _shutdown_flag:
        slug, start_ts = get_current_slug()
        meta = await fetch_metadata(slug)
        if not meta:
            await asyncio.sleep(2); continue

        resolved_event.clear()
        resolved_winner_asset = None

        market_day = datetime.fromtimestamp(start_ts).date()
        if last_day is None or market_day != last_day:
            daily_profit = 0.0
            last_day = market_day
            mart_multiplier = 1
            mart_accumulated_loss = 0.0
            if LIVE_TRADING:
                lb = await fetch_live_bankroll()
                if lb is not None: bankroll = lb
            log_info(f"NEW DAY {market_day} | Banca: ${bankroll:.4f} | Mart RESET x1")

        for k in ("up", "down"):
            best_bids[k] = best_asks[k] = best_spreads_c[k] = None
            best_bid_sizes[k] = best_ask_sizes[k] = None
        price_change.clear()

        ws_task = asyncio.create_task(ws_handler(meta["up"], meta["down"]))
        await asyncio.sleep(1.0)

        if best_bids["up"] is not None:
            pre_bank = bankroll
            await logic_loop(start_ts, start_ts + 300, meta)
            profit_this = bankroll - pre_bank
            daily_profit += profit_this
            # v4.1: Net PnL tracking — gains offset losses, losses offset gains
            # Win:  Pos increases, Neg recovers toward 0
            # Loss: Neg decreases, Pos reduces toward 0
            if profit_this > 0.00001:
                # First: recover Neg toward 0, remainder goes to Pos
                if total_pnl_neg < 0.0:
                    recovery = min(profit_this, abs(total_pnl_neg))
                    total_pnl_neg += recovery
                    remainder = profit_this - recovery
                    total_pnl_pos += remainder
                else:
                    total_pnl_pos += profit_this
            elif profit_this < -0.00001:
                loss = abs(profit_this)
                # First: reduce Pos toward 0, remainder goes to Neg
                if total_pnl_pos > 0.0:
                    reduction = min(loss, total_pnl_pos)
                    total_pnl_pos -= reduction
                    remainder = loss - reduction
                    total_pnl_neg -= remainder
                else:
                    total_pnl_neg += profit_this

            # Martingale state
            if profit_this >= 0.0:
                if mart_multiplier > 1:
                    log_info(f"MART WIN | x{mart_multiplier}→x1 | recovered {fmt_dollar(mart_accumulated_loss)}")
                mart_multiplier = 1; mart_accumulated_loss = 0.0
            else:
                mart_accumulated_loss += abs(profit_this)
                old = mart_multiplier
                if mart_multiplier < MART_MAX_MULT:
                    mart_multiplier = min(mart_multiplier * 2, MART_MAX_MULT)
                    log_warn(f"MART LOSS | x{old}→x{mart_multiplier} | loss={fmt_dollar(profit_this)}")
                else:
                    mart_multiplier = 1; mart_accumulated_loss = 0.0
                    log_warn(f"MART MAX x{MART_MAX_MULT} — RESET | loss={fmt_dollar(profit_this)}")

            log_sep2()
            pnl_pct = (profit_this / pre_bank * 100.0) if pre_bank > 0 else 0.0
            log_info(f"ROUND | PnL: {fmt_dollar(profit_this)} ({fmt_pct(pnl_pct)}) | Mart: x{mart_multiplier}")
            log_info(f"TOTAL | PnL_dia: {fmt_dollar(daily_profit)} | Banca: ${bankroll:.4f} | "
                     f"Pos: {fmt_dollar(total_pnl_pos)} | Neg: {fmt_dollar(total_pnl_neg)} | Up: {get_uptime_str()}")
            log_sep2()

        ws_task.cancel()
        try: await ws_task
        except asyncio.CancelledError: pass
        await asyncio.sleep(0.5)

    log_info(f"SHUTDOWN | Banca: ${bankroll:.4f} | Up: {get_uptime_str()}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_info("STOPPED (Ctrl+C)")