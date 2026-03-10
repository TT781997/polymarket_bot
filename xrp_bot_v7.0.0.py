"""
last_xrp_bot_gambling_peg_unified.py
=====================================
VERSION: 7.0.0 — ATOMIC ARB + ZERO-SLIP + MART×8 + ENDGAME 15s

MELHORIAS v7.0.0 vs v6.0.0:
  ✅ NOVO CICLO log mostra strike K e direcção UP (XRP > K) / DOWN (XRP < K)
  ✅ PEG ARBIT trigger corrigido: ask_up + ask_down <= 0.982 (era < 0.978)
  ✅ evaluate_arb usa peg_trigger=PA_TRIGGER_SUM (0.982) — alinhado com pre-check
  ✅ place_live_order_arb: zero slippage para execução atómica (FOK ao ask exacto)
  ✅ place_market_close rollback: penalidade extra +5c (ROLLBACK_EXTRA_SLIP=0.05)
  ✅ Martingale max ×8 (era ×2): x1→x2→x4→x8→reset
  ✅ Endgame sniping activado aos 15s finais (era 11s)

MÓDULOS INTEGRADOS (ficheiro único, zero dependências externas ao projecto):
  ① BinanceOracle      — WS stream xrpusdt@ticker + Black-Scholes digital
  ② HFT Production     — execute_trade (DRY_RUN) + heartbeat_loop + user_ws_loop
  ③ ArbEngine          — evaluate_arb, VWAP fallback, fee LUT O(1)
  ④ TradeStateManager  — Persistência orjson não-bloqueante (atomic rename)
  ⑤ MarketTimer        — Janelas temporais centralizadas
  ⑥ AuditLogger        — Logs padronizados ROUND/TOTAL
  ⑦ BayesianTracker    — Signal Processing v4.0 em log-space
  ⑧ LMSRPricer         — LMSR Pricing v4.0
  ⑨ AtomicArbExecutor  — PEG ARBIT atómico com rollback em 500ms
  ⑩ Bot principal      — Logic loop + Main

MELHORIAS v6.0.0 vs v5.2.1:
  ✅ BinanceOracle integrado — rolling vol/drift + Black-Scholes digital
  ✅ calculate_true_prob() funde sinal Binance com Bayesian (blend 60/40)
  ✅ execute_trade() com DRY_RUN wrapper e fetch dinâmico de fee_rate_bps
  ✅ heartbeat_loop() com recovery de ID expirado (regex + circuit-breaker)
  ✅ user_ws_loop() com auth L2 HMAC-SHA256 + parse MATCHED/CONFIRMED/FAILED
  ✅ place_live_order() correctamente delegado para execute_trade()
  ✅ main() limpo — único bloco de orquestração, sem duplicação
  ✅ Tasks bnc_task e hb_task persistem entre ciclos (não recriam por ronda)
  ✅ set_cycle_strike() chamado no início de cada ciclo de 5 min
  ✅ DRY_RUN e LIVE_TRADING separados — DRY_RUN protege de ordens reais

LOG FORMAT (fim de ronda):
  [INFO] [dd/mm/yy | HH:MM:SS.mmm] | ROUND | PnL: $+X.XXXX (+X.XX%) | Mart: xN
  [INFO] [dd/mm/yy | HH:MM:SS.mmm] | TOTAL | PnL_dia: $+X.XXXX (+X.XX%) | Banca: $XX.XXXX | Up_Time: XXh:XXm:XXs

DEPENDÊNCIAS:
  pip install websockets py-clob-client orjson requests
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import math
import random
import re
import signal
import time
import logging
import os
import json
from datetime import datetime
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Callable, Awaitable
from enum import Enum

# ── orjson com fallback para stdlib json ──────────────────────────────────────
try:
    import orjson as _orjson
    def _state_dumps(obj: dict) -> bytes:
        return _orjson.dumps(obj, option=_orjson.OPT_INDENT_2)
    def _state_loads(raw: bytes | str) -> dict:
        return _orjson.loads(raw)
    _HAS_ORJSON = True
except ImportError:
    import json as _json_fallback  # type: ignore
    def _state_dumps(obj: dict) -> bytes:  # type: ignore[misc]
        return _json_fallback.dumps(obj, indent=2, ensure_ascii=False).encode("utf-8")
    def _state_loads(raw: bytes | str) -> dict:  # type: ignore[misc]
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return _json_fallback.loads(raw)
    _HAS_ORJSON = False


###############################################################################
#                                                                             #
#   PARÂMETROS — única zona de configuração do bot                           #
#   Mercado: XRP Up or Down — 5 minutos (Polymarket)                         #
#                                                                             #
###############################################################################

# ─── MODO DE EXECUÇÃO ────────────────────────────────────────────────────────

DRY_RUN: bool = True
# [Bool] | Ativa/desativa simulação. 
LIVE_TRADING: bool = False
# [Bool] | Ativa envio para blockchain. (Aviso: Cuidado ao mudar para True com esta config agressiva).

STATE_FILE: str = "trade_state.json"
BANKROLL_DEMO: float = 10.0
# [USD] | Banca inicial da simulação.

SLIPPAGE_TOLERANCE: float = 0.028
# [Percentual/Float] | Min: 0.01 | Max: 0.10 | Cima: Aceita preços piores (garante execução). Baixo: Falha entradas. 
# 2.8% — equilíbrio perfeito entre execução rápida e proteção.

# ─── ARB ENGINE (Arbitragem Pura) ────────────────────────────────────────────

ARB_PEG_TRIGGER: float = 0.978
# [USD] | Min: 0.95 | Max: 1.00 | Cima: Mais trades (menor lucro). Baixo: Menos trades (maior lucro). Entra se UP+DOWN < 0.978.
ARB_RESOLUTION: float  = 1.00
# [USD] | Pagamento final garantido.
ARB_MIN_SHARES: float  = 1.0
# [Shares] | Aumentado para 1.0 para não perder tempo com micro-ordens.
ARB_FEE_BASE: float    = 0.25
ARB_FEE_EXP: int       = 2

# ─── MARTINGALE (Recuperação de Perdas) ──────────────────────────────────────

MART_MAX_MULT: int = 8
# [Inteiro] | Min: 1 | Max: 32 | Cima: Risco brutal de quebra, mas recupera PnL rápido. Baixo: Mais seguro.
# ×8 — recuperação agressiva: x1→x2→x4→x8→reset

# ─── RISK RULES (Gestão de Banca) ────────────────────────────────────────────

MAX_MARKET_EXPOSURE: float = 0.038
# [Percentual] | Min: 0.01 | Max: 0.50 | Cima: Risco de ruína. Baixo: Crescimento lento. 
# 3.8% da banca por ciclo → permite dias de +100%+
KELLY_MAX_RISK_PCT: float  = 0.028
# [Percentual] | Max da banca num só trade. 2.8% antes do martingale → com ×2 = máximo 0.9%

# ─── STRATEGY TOGGLES ─────────────────────────────────────────────────────────

PEG_ARBIT_ACTIVE: bool          = True
GAMBLING_ACTIVE: bool           = True
AGGRESSIVE_ENDGAME_ACTIVE: bool = True
# [Bool] | Ativado. Obriga o bot a fazer "sniping" nos últimos segundos do mercado.

# ─── PEG ARBIT (Arbitragem Estatística) ───────────────────────────────────────

PEG_ARBIT_RISK: float  = 0.52
# [Percentual] | % do max exposure a usar em Arb.
PA_TRIGGER_SUM: float  = 0.982
# [USD] | Aceita somas de 98.2 cêntimos. Margem boa + giro rápido.
PA_COOLDOWN: float     = 0.03
# [Segundos] | Cooldown entre ordens. Reduzido para mais volume.
PA_MIN_REM: float      = 10.0
# [Segundos] | Aceita arbitrar até 10s do fim.
PA_BUFFER_S: float     = 6.0
MAX_PA_ENTRIES: int    = 10_000_000

# ─── GAMBLING (Apostas Direcionais) ───────────────────────────────────────────

GAMB_START_REM_S: float  = 300.0
# [Segundos] | 5 minutos exatos. Começa mal abre o mercado.
GAMB_CUTOFF_S: float     = 22.0
# [Segundos] | Corta apostas a 22s do fim.
GAMB_BUY_COOLDOWN: float = 8.0
# [Segundos] | Min: 1.0 | Max: 30.0 | Cima: Mais lento. Baixo: Mais entradas.
GAMB_MIN_ASK_C: float    = 84.0
# [Cêntimos] | Só entra em favoritos fortes (84c+).
GAMB_MAX_ASK_C: float    = 96.5
# [Cêntimos] | Aposta até 96.5c (evita os 98-99c perigosos).

# ─── FILTROS DE ENTRADA ───────────────────────────────────────────────────────

MAX_SPREAD_CENTS: float  = 2.20
# [Cêntimos] | Min: 1.0 | Max: 10.0 | Cima: Ignora spreads largos.
BID_ASK_MIN_RATIO: float = 0.975
GAMB_MAX_VOL_DEV: float  = 0.018
GAMB_MAX_ZSCORE: float   = 0.95
GAMB_MIN_OBI: float      = 0.48
# [Ratio] | Min: 0 | Max: 1 | Baixo: Entra com menos desequilíbrio de volume.
VPIN_SAFE_LIMIT: float   = 0.44

# ─── ENDGAME (Últimos segundos do mercado) ────────────────────────────────────

ENDGAME_TRIGGER_S: float      = 20.0
# [Segundos] | Ativa modo pânico de lucro aos 20s finais.
ENDGAME_ZSCORE_LIMIT: float   = 2.8
ENDGAME_VPIN_LIMIT: float     = 0.62
AGGRESSIVE_ENDGAME_S: float   = 15.0
AGGRESSIVE_ENDGAME_RISK: float = 0.012
# [Percentual] | 1.2% da banca no endgame (com ×2 = 2.4% mas filtros protegem)
AGGRESSIVE_ENDGAME_MIN_C: float = 0.86
AGGRESSIVE_ENDGAME_MAX_C: float = 0.97

# ─── KELLY CRITERION (Tamanho da posição) ─────────────────────────────────────

KELLY_ASSUMED_EDGE: float = 0.075
# [Float] | Min: 0.01 | Max: 0.15 | Assumimos margem de 7.5% para posições maiores.
KELLY_FRACTION: float     = 0.075
# [Float/Percent] | Fator Kelly. 7.5% Kelly → crescimento explosivo nos dias bons.

# ─── BAYESIAN (Motor de Probabilidades) ───────────────────────────────────────

BAYESIAN_PRIOR: float          = 0.50
BAYESIAN_LIKELIHOOD_STD: float = 0.011
# [Float] | Min: 0.005 | Max: 0.1 | Reage bem a ticks novos.
BAYESIAN_MIN_EDGE: float       = 0.095
# [Float] | Edge mínimo de 9.5% (mais trades que a versão conservadora).
BAYESIAN_DECAY: float          = 0.978
# [Float] | Decaimento agressivo mas controlado.
BAYESIAN_MIN_TICKS: int        = 7
# [Int] | Valida tendência com 7 ticks.

# ─── LMSR (Preço Justo Autommated Market Maker) ───────────────────────────────

LMSR_B: float               = 160_000.0
# [Float] | Profundidade simulada de mercado.
LMSR_INEFF_THRESHOLD: float = 0.009

# ─── HFT ENGINE (Métricas Temporais e Filtros) ────────────────────────────────

HFT_WINDOW_SECONDS: float   = 5.5
# [Segundos] | Olha para os últimos 5.5 segundos para decidir.
KALMAN_PROCESS_NOISE: float = 7e-6
# [Float] | Filtro segue o preço atual com boa velocidade.
KALMAN_MEASURE_NOISE: float = 3.2e-3

# ─── PRODUÇÃO (Timeouts e APIs) ───────────────────────────────────────────────
# (Mantido padrão da API Polymarket para não dar Ban)
RATE_LIMIT_CALLS: float  = 9.0
RATE_LIMIT_BURST: float  = 18.0
MAX_API_RETRIES: int     = 4
BASE_BACKOFF_S: float    = 1.1
MAX_BACKOFF_S: float     = 35.0
BACKOFF_JITTER: bool     = True
CB_FAIL_THRESHOLD: int   = 6
CB_RECOVERY_S: float     = 75.0
WS_RECONNECT_BASE_S: float = 1.2
WS_RECONNECT_MAX_S: float  = 20.0
WS_HEARTBEAT_INTERVAL: int = 18
WS_HEARTBEAT_TIMEOUT: int  = 9
LOOP_SLEEP: float          = 0.002
ROLLBACK_TIMEOUT_S: float  = 0.5
ROLLBACK_EXTRA_SLIP: float = 0.05
# [USD] | Penalidade extra de 5c abaixo do bid no rollback atómico (garante fill imediato).

# ─── ENDPOINTS ────────────────────────────────────────────────────────────────

CLOB_REST_URL: str = "https://clob.polymarket.com"
GAMMA_API_URL: str = "https://gamma-api.polymarket.com"
WS_URI: str        = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
USER_WS_URI: str   = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

# ─── BINANCE ORACLE (A Chave para os Mercados de 5 Minutos) ───────────────────

BINANCE_WS_URI: str          = "wss://stream.binance.com:9443/ws/xrpusdt@ticker"
BINANCE_RECONNECT_BASE_S: float = 1.2
BINANCE_RECONNECT_MAX_S: float  = 30.0
BINANCE_PING_INTERVAL_S: float  = 20.0
XRP_VOL_ANNUAL_DEFAULT: float   = 1.20
XRP_VOL_WINDOW_TICKS: int       = 20
# [Int] | Olha só para os últimos 20 ticks da Binance.
XRP_DRIFT_EMA_ALPHA: float      = 0.15
# [Float] | Dá peso alto ao drift instantâneo.
TIME_DECAY_FLOOR_S: float       = 1.0
SIGMOID_STEEPNESS: float        = 10.0
# [Float] | Converte a mais ligeira subida do XRP na Binance numa probabilidade de 90%+ no Polymarket.
SIGMA_FLOOR: float              = 0.05
PROB_MIN: float                 = 0.05
# [Float] | Cap inferior = 5% (evita "Overconfidence" bloqueado).
PROB_MAX: float                 = 0.95
# [Float] | Cap superior = 95%. Facilita a inversão se a Binance virar de repente.
_SECS_PER_YEAR: float           = 365.25 * 24.0 * 3600.0

BINANCE_BLEND_WEIGHT: float = 0.88
# [Float] | 88% de peso para a Binance, 12% para o Polymarket Orderbook. 
# Num mercado de 5m, o livro de ordens do Polymarket é lento/enganador. A Binance é onde o verdadeiro preço do XRP está.

# ─── HOT PATH CACHE (aliases pré-calculados — não editar) ────────────────────
_F_PA_TRIGGER_SUM          = PA_TRIGGER_SUM
_F_GAMB_MIN_ASK_C          = GAMB_MIN_ASK_C
_F_GAMB_MAX_ASK_C          = GAMB_MAX_ASK_C
_F_MAX_SPREAD_CENTS         = MAX_SPREAD_CENTS
_F_BID_ASK_MIN_RATIO        = BID_ASK_MIN_RATIO
_F_KELLY_MAX_RISK_PCT       = KELLY_MAX_RISK_PCT
_F_MAX_MARKET_EXPOSURE      = MAX_MARKET_EXPOSURE
_F_AGGRESSIVE_ENDGAME_MIN   = AGGRESSIVE_ENDGAME_MIN_C
_F_AGGRESSIVE_ENDGAME_MAX   = AGGRESSIVE_ENDGAME_MAX_C
_F_AGGRESSIVE_ENDGAME_RISK  = AGGRESSIVE_ENDGAME_RISK


###############################################################################
#                                                                             #
#   FEE LOOK-UP TABLE O(1)                                                   #
#                                                                             #
###############################################################################

_FEE_LUT: dict[int, float] = {
    i: ARB_FEE_BASE * ((i / 100.0) * (1.0 - i / 100.0)) ** ARB_FEE_EXP
    for i in range(1, 100)
}

def fee_rate_lut(p: float) -> float:
    """Fee rate O(1) via LUT. Fora de [0.01, 0.99] retorna 0.0."""
    return _FEE_LUT.get(round(p * 100), 0.0)

def _cost_with_fee_f(shares: float, ask: float) -> float:
    return shares * ask * (1.0 + fee_rate_lut(ask))


###############################################################################
#                                                                             #
#   ① BINANCE ORACLE — Estado partilhado + WS listener + Probabilidade      #
#                                                                             #
###############################################################################

@dataclass
class BinanceState:
    """Estado do Oráculo Binance — partilhado entre todos os módulos.

    Actualizado exclusivamente por binance_ticker_loop() no event loop
    (single-threaded asyncio → sem race conditions).

    Campos públicos:
        current_price      — Último preço XRP/USDT da Binance.
        cycle_open_price   — Preço de abertura do ciclo (strike K).
                             Fixado por set_cycle_strike() no início de cada ciclo.
        last_update_ts     — Timestamp UNIX do último tick.
        connected          — True se a conexão WS está activa.
        tick_count         — Ticks recebidos desde o arranque.
    """
    current_price:    Optional[float] = None
    cycle_open_price: Optional[float] = None
    last_update_ts:   float           = 0.0
    connected:        bool            = False
    tick_count:       int             = 0

    # Internos — rolling vol/drift
    _returns:    deque  = field(default_factory=lambda: deque(maxlen=XRP_VOL_WINDOW_TICKS))
    _vol_annual: float  = XRP_VOL_ANNUAL_DEFAULT
    _drift_ema:  float  = 0.0
    _prev_price: Optional[float] = None

    def update_price(self, price: float) -> None:
        """Actualiza preço e recalcula volatilidade/drift rolling. O(1) hot-path."""
        prev = self._prev_price
        self.current_price  = price
        self.last_update_ts = time.time()
        self.tick_count    += 1

        if prev is not None and prev > 1e-9:
            log_ret = math.log(price / prev)
            self._returns.append(log_ret)
            # EMA do drift
            self._drift_ema = (
                XRP_DRIFT_EMA_ALPHA * log_ret
                + (1.0 - XRP_DRIFT_EMA_ALPHA) * self._drift_ema
            )
            # Volatilidade rolling (desvio padrão dos log-retornos)
            n = len(self._returns)
            if n >= 5:
                mean_r = sum(self._returns) / n
                var_r  = sum((r - mean_r) ** 2 for r in self._returns) / n
                sigma_t = math.sqrt(var_r)
                # Anualizar: ~1 tick/s ≈ _SECS_PER_YEAR ticks/ano
                self._vol_annual = max(
                    sigma_t * math.sqrt(_SECS_PER_YEAR),
                    XRP_VOL_ANNUAL_DEFAULT * 0.10,
                )
        self._prev_price = price

    @property
    def vol_annual(self) -> float:
        """Volatilidade anualizada estimada. Fallback ao default se poucos ticks."""
        return self._vol_annual if len(self._returns) >= 5 else XRP_VOL_ANNUAL_DEFAULT

    @property
    def drift_annual(self) -> float:
        """Drift anualizado estimado via EMA de log-retornos."""
        return self._drift_ema * _SECS_PER_YEAR

    @property
    def staleness_s(self) -> float:
        return time.time() - self.last_update_ts if self.last_update_ts > 0 else float("inf")

    def is_stale(self, threshold_s: float = 10.0) -> bool:
        return self.staleness_s > threshold_s


# Instância global do estado Binance
binance_state: BinanceState = BinanceState()


async def binance_ticker_loop() -> None:
    """Stream xrpusdt@ticker da Binance → actualiza binance_state em cada tick.

    Reconecta automaticamente com backoff exponencial.
    Mantém conexão viva via ws.ping() nativo a cada BINANCE_PING_INTERVAL_S.
    Termina limpo via asyncio.CancelledError.
    """
    try:
        import websockets  # type: ignore
    except ImportError:
        logging.getLogger("bot_xrp").error(
            "[BINANCE] websockets não instalado — pip install websockets"
        )
        return

    _log = logging.getLogger("bot_xrp")
    _backoff = BINANCE_RECONNECT_BASE_S
    _log.info("[BINANCE] Loop iniciado | uri=%s", BINANCE_WS_URI)

    while True:
        try:
            async with websockets.connect(
                BINANCE_WS_URI,
                ping_interval=None,
                ping_timeout=None,
                open_timeout=15,
                max_size=2 ** 18,
            ) as ws:
                binance_state.connected = True
                _backoff = BINANCE_RECONNECT_BASE_S
                _log.info("[BINANCE] CONNECTED | tick_count=%d", binance_state.tick_count)

                async def _ping_task() -> None:
                    while True:
                        await asyncio.sleep(BINANCE_PING_INTERVAL_S)
                        try:
                            await ws.ping()
                        except Exception:
                            break

                ping_fut = asyncio.ensure_future(_ping_task())
                try:
                    async for raw in ws:
                        _process_binance_ticker(raw)
                finally:
                    binance_state.connected = False
                    ping_fut.cancel()
                    try:
                        await ping_fut
                    except (asyncio.CancelledError, Exception):
                        pass

        except asyncio.CancelledError:
            binance_state.connected = False
            logging.getLogger("bot_xrp").info("[BINANCE] Loop cancelado — shutdown limpo")
            return
        except Exception as exc:
            binance_state.connected = False
            logging.getLogger("bot_xrp").warning(
                "[BINANCE] %s: %s — reconectar em %.1f s",
                type(exc).__name__, str(exc)[:120], _backoff,
            )
            await asyncio.sleep(_backoff)
            _backoff = min(_backoff * 2.0, BINANCE_RECONNECT_MAX_S)


def _process_binance_ticker(raw: str | bytes) -> None:
    """Processa um frame do stream @ticker. Hot-path — O(1), sem bloqueio."""
    try:
        text: str = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        data: dict = json.loads(text)
        raw_price = data.get("c")
        if raw_price is None:
            return
        price = float(raw_price)
        if not (price > 0.0):
            return

        # Latência do tick (logging de aviso se > 2 s)
        event_ts_ms: Optional[int] = data.get("E")
        if event_ts_ms is not None:
            latency_ms = time.time() * 1000.0 - event_ts_ms
            if latency_ms > 2000.0:
                logging.getLogger("bot_xrp").warning(
                    "[BINANCE] Tick lag alto: %.0f ms | price=%.5f", latency_ms, price
                )

        binance_state.update_price(price)

        if binance_state.tick_count % 200 == 0:
            logging.getLogger("bot_xrp").debug(
                "[BINANCE] tick #%d | price=%.5f | vol_ann=%.2f%% | drift_ann=%+.2f%%",
                binance_state.tick_count,
                price,
                binance_state.vol_annual * 100.0,
                binance_state.drift_annual * 100.0,
            )
    except Exception:
        pass


def set_cycle_strike() -> Optional[float]:
    """Fixa o preço de abertura do ciclo como strike (K).

    Chamar no início de cada ciclo de 5 min, depois do primeiro tick Binance.
    Returns: strike fixado, ou None se ainda não há dados.
    """
    if binance_state.current_price is None:
        logging.getLogger("bot_xrp").warning(
            "[BINANCE] set_cycle_strike: sem preço ainda — strike=None"
        )
        return None
    binance_state.cycle_open_price = binance_state.current_price
    logging.getLogger("bot_xrp").info(
        "[BINANCE] STRIKE FIXADO | K=%.5f | tick_count=%d | vol_ann=%.2f%%",
        binance_state.cycle_open_price,
        binance_state.tick_count,
        binance_state.vol_annual * 100.0,
    )
    return binance_state.cycle_open_price


def _norm_cdf(x: float) -> float:
    """CDF normal padrão Φ(x) — Abramowitz & Stegun §26.2.17, |ε| < 7.5e-8."""
    sign = 1.0 if x >= 0.0 else -1.0
    x_abs = abs(x)
    p_  =  0.2316419
    b1  =  0.319381530; b2 = -0.356563782; b3 = 1.781477937
    b4  = -1.821255978; b5 =  1.330274429
    t   = 1.0 / (1.0 + p_ * x_abs)
    t2 = t*t; t3 = t2*t; t4 = t3*t; t5 = t4*t
    poly = b1*t + b2*t2 + b3*t3 + b4*t4 + b5*t5
    pdf_x = math.exp(-0.5 * x_abs * x_abs) / math.sqrt(2.0 * math.pi)
    cdf = 1.0 - pdf_x * poly
    return cdf if sign > 0 else 1.0 - cdf


def _sigmoid(x: float, k: float = SIGMOID_STEEPNESS) -> float:
    kx = k * x
    if kx > 50.0:  return 1.0
    if kx < -50.0: return 0.0
    return 1.0 / (1.0 + math.exp(-kx))


def calculate_true_prob(
    current_price:    Optional[float],
    strike_price:     Optional[float],
    seconds_to_close: float,
) -> Optional[float]:
    """Probabilidade real de UP resolver YES via Black-Scholes digital.

    Modelo:
        Regime normal   (t ≥ TIME_DECAY_FLOOR_S): P = Φ(d₂)
          d₂ = [ln(S/K) + (μ − σ²/2)·T] / (σ·√T)

        Regime time-decay (t < TIME_DECAY_FLOOR_S): P = sigmoid(Δ_norm)
          Δ_norm = (S − K) / (K · σ_floor)

    Returns [PROB_MIN, PROB_MAX] ou None se dados insuficientes.
    """
    if current_price is None or strike_price is None:
        return None
    if not (current_price > 0.0) or not (strike_price > 0.0):
        return None

    S: float = current_price
    K: float = strike_price
    t_s: float = max(seconds_to_close, 0.0)

    sigma: float = max(binance_state.vol_annual, 0.001)
    mu:    float = binance_state.drift_annual

    if t_s < TIME_DECAY_FLOOR_S:
        sigma_floor = max(sigma, SIGMA_FLOOR)
        delta_norm  = (S - K) / (K * sigma_floor)
        p = _sigmoid(delta_norm, k=SIGMOID_STEEPNESS)
        return max(PROB_MIN, min(PROB_MAX, p))

    T: float = t_s / _SECS_PER_YEAR
    try:
        ln_s_k: float = math.log(S / K)
    except (ValueError, ZeroDivisionError):
        return None

    denominator = sigma * math.sqrt(T)
    if denominator < 1e-12:
        return None

    d2 = (ln_s_k + (mu - 0.5 * sigma * sigma) * T) / denominator
    d2 = max(-8.0, min(8.0, d2))
    return max(PROB_MIN, min(PROB_MAX, _norm_cdf(d2)))


###############################################################################
#                                                                             #
#   ② HFT PRODUCTION FUNCTIONS                                               #
#                                                                             #
###############################################################################

# ── execute_trade ─────────────────────────────────────────────────────────────

async def execute_trade(
    clob_client,
    token_id:  str,
    side:      str,
    amount:    float,
    price:     float,
    is_dry_run: bool = True,
) -> bool:
    """Executa ou simula uma ordem FOK no CLOB da Polymarket.

    is_dry_run=True  → Log apenas (sem tocar na API).
    is_dry_run=False → create_market_order + post_order(FOK) real.

    Returns True em sucesso/simulação, False em falha.
    """
    _log = logging.getLogger("bot_xrp")
    shares: float    = round(amount / price, 6)       if price > 1e-9 else 0.0
    ev_raw: float    = round((1.0 - price) / price, 6) if price > 1e-9 else 0.0
    ev_exp: float    = round(shares * 1.0 - amount, 6)

    if is_dry_run:
        _log.info(
            "[DRY_RUN] FOK | side=%-4s | token=%s... | "
            "price=%.4f | amount=$%.4f | shares=%.6f | EV_raw=%.4f | EV_exp=$%+.4f",
            side, token_id[:16], price, amount, shares, ev_raw, ev_exp,
        )
        return True

    if clob_client is None:
        _log.error("[EXECUTE] clob_client is None — DRY_RUN=False requer cliente autenticado")
        return False

    try:
        from py_clob_client.clob_types import OrderType  # type: ignore

        fee_rate_bps: int = 0
        try:
            import requests as _req  # type: ignore
            _r = _req.get(
                f"{CLOB_REST_URL}/fee-rate",
                params={"token_id": token_id},
                timeout=3,
            )
            fee_rate_bps = int(_r.json().get("fee_rate_bps", 0))
        except Exception as _fee_err:
            _log.warning("[EXECUTE] fee_rate fetch falhou: %s — usando 0 bps", _fee_err)

        order = clob_client.create_market_order(
            token_id=token_id,
            side=side,
            amount=amount,
            price=price,
            fee_rate_bps=fee_rate_bps,
            options={"tick_size": "0.01", "neg_risk": False},
        )
        response = clob_client.post_order(order, OrderType.FOK)
        _log.info(
            "[EXECUTE] FOK SENT | side=%-4s | token=%s... | "
            "price=%.4f | amount=$%.4f | shares=%.6f | fee_bps=%d | resp=%s",
            side, token_id[:16], price, amount, shares, fee_rate_bps, response,
        )
        return True

    except Exception as exc:
        _log.error(
            "[EXECUTE] FOK FAILED | side=%s | token=%s... | price=%.4f | %s: %s",
            side, token_id[:16], price, type(exc).__name__, exc,
        )
        return False


# ── heartbeat_loop ────────────────────────────────────────────────────────────

_HEARTBEAT_INTERVAL_S: float    = 5.0
_HEARTBEAT_MAX_ERRORS: int      = 10
_HEARTBEAT_CB_PAUSE_FACTOR: int = 6  # pausa = intervalo × factor


async def heartbeat_loop(clob_client) -> None:
    """Mantém a sessão L2 do CLOB activa via heartbeats de 5 s.

    Recupera automaticamente de IDs expirados (HTTP 400) via regex.
    Circuit-breaker suave: pausa prolongada após N erros consecutivos.
    """
    _log = logging.getLogger("bot_xrp")
    heartbeat_id: Optional[str] = None
    consecutive_errors: int     = 0

    _log.info("[HEARTBEAT] Loop iniciado — intervalo=%.1f s", _HEARTBEAT_INTERVAL_S)

    while True:
        try:
            await asyncio.sleep(_HEARTBEAT_INTERVAL_S)

            if clob_client is None:
                continue

            loop = asyncio.get_running_loop()
            _snap = heartbeat_id
            response = await loop.run_in_executor(
                None, lambda: clob_client.post_heartbeat(_snap)
            )

            # Extrai novo ID da resposta
            _new_id: Optional[str] = None
            if isinstance(response, dict):
                _new_id = (
                    response.get("heartbeat_id")
                    or response.get("id")
                    or response.get("next_id")
                )
            elif hasattr(response, "heartbeat_id"):
                _new_id = response.heartbeat_id

            if _new_id and _new_id != heartbeat_id:
                _log.debug("[HEARTBEAT] ID: %s → %s", heartbeat_id, _new_id)
                heartbeat_id = _new_id

            consecutive_errors = 0
            _log.debug("[HEARTBEAT] OK | id=%s", heartbeat_id)

        except asyncio.CancelledError:
            _log.info("[HEARTBEAT] Cancelado — shutdown limpo")
            return

        except Exception as exc:
            exc_str = str(exc)
            is_bad_id = any(k in exc_str.lower() for k in ("400", "invalid", "expired", "heartbeat_id"))

            if is_bad_id:
                recovered = _extract_hb_id(exc_str)
                if recovered:
                    _log.warning("[HEARTBEAT] 400 — ID recuperado: %s", recovered)
                    heartbeat_id = recovered
                else:
                    _log.warning("[HEARTBEAT] 400 sem ID recuperável — reset heartbeat_id")
                    heartbeat_id = None
                consecutive_errors = 0
            else:
                consecutive_errors += 1
                _log.warning(
                    "[HEARTBEAT] %s: %s (erro %d/%d)",
                    type(exc).__name__, exc_str[:120],
                    consecutive_errors, _HEARTBEAT_MAX_ERRORS,
                )
                if consecutive_errors >= _HEARTBEAT_MAX_ERRORS:
                    pause = _HEARTBEAT_INTERVAL_S * _HEARTBEAT_CB_PAUSE_FACTOR
                    _log.error(
                        "[HEARTBEAT] %d erros consecutivos — pausa %.0f s (circuit-breaker)",
                        consecutive_errors, pause,
                    )
                    await asyncio.sleep(pause)
                    consecutive_errors = 0


def _extract_hb_id(error_str: str) -> Optional[str]:
    """Extrai heartbeat_id de uma mensagem de erro HTTP 400."""
    for pattern in (
        r'"heartbeat_id"\s*:\s*"([^"]+)"',
        r'"next_id"\s*:\s*"([^"]+)"',
        r'"id"\s*:\s*"([^"]+)"',
        r'heartbeat_id[=:]\s*([A-Za-z0-9_\-]+)',
    ):
        m = re.search(pattern, error_str)
        if m:
            return m.group(1)
    return None


# ── user_ws_loop ──────────────────────────────────────────────────────────────

_USER_WS_PING_S: float   = 10.0
_TRADE_TERMINAL: frozenset = frozenset({"MATCHED", "CONFIRMED", "FAILED"})


def _build_l2_auth(
    api_key: str,
    secret: str,
    passphrase: str,
    condition_id: str,
) -> dict:
    """Payload de autenticação L2 (HMAC-SHA256) para o canal user WS."""
    ts: str = str(int(time.time()))
    message: bytes = (ts + "GET" + "/ws-auth").encode("utf-8")
    try:
        key_bytes: bytes = base64.b64decode(secret)
    except Exception:
        key_bytes = secret.encode("utf-8")
    signature: str = base64.b64encode(
        hmac.new(key_bytes, message, hashlib.sha256).digest()
    ).decode("utf-8")
    return {
        "type": "auth",
        "channel": "user",
        "market": condition_id,
        "auth": {
            "apiKey":     api_key,
            "secret":     secret,
            "passphrase": passphrase,
            "timestamp":  ts,
            "signature":  signature,
        },
    }


async def user_ws_loop(
    api_key:      str,
    secret:       str,
    passphrase:   str,
    condition_id: str,
) -> None:
    """Canal WS autenticado (L2) para eventos de trade do utilizador.

    Subscreve o canal user da Polymarket e processa MATCHED/CONFIRMED/FAILED.
    Keepalive: envia "PING" literal a cada _USER_WS_PING_S segundos.
    """
    _log = logging.getLogger("bot_xrp")
    try:
        import websockets  # type: ignore
    except ImportError:
        _log.error("[USER_WS] websockets não instalado — pip install websockets")
        return

    _backoff = WS_RECONNECT_BASE_S
    _log.info(
        "[USER_WS] Loop iniciado | condition=%s... | ping=%.0f s",
        condition_id[:16], _USER_WS_PING_S,
    )

    while True:
        try:
            async with websockets.connect(
                USER_WS_URI,
                ping_interval=None,
                ping_timeout=None,
                open_timeout=15,
            ) as ws:
                auth_payload = _build_l2_auth(api_key, secret, passphrase, condition_id)
                await ws.send(json.dumps(auth_payload))
                _log.info("[USER_WS] OPEN | market=%s... | auth enviada", condition_id[:16])
                _backoff = WS_RECONNECT_BASE_S

                async def _ping_task() -> None:
                    while True:
                        await asyncio.sleep(_USER_WS_PING_S)
                        try:
                            await ws.send("PING")
                            _log.debug("[USER_WS] PING enviado")
                        except Exception:
                            break

                ping_fut = asyncio.ensure_future(_ping_task())
                try:
                    async for raw in ws:
                        _handle_user_ws_msg(raw, condition_id)
                finally:
                    ping_fut.cancel()
                    try:
                        await ping_fut
                    except (asyncio.CancelledError, Exception):
                        pass

        except asyncio.CancelledError:
            _log.info("[USER_WS] Cancelado — shutdown limpo")
            return
        except Exception as exc:
            _log.warning(
                "[USER_WS] %s: %s — reconectar em %.1f s",
                type(exc).__name__, str(exc)[:120], _backoff,
            )
            await asyncio.sleep(_backoff)
            _backoff = min(_backoff * 2.0, WS_RECONNECT_MAX_S)


def _handle_user_ws_msg(raw: str | bytes, condition_id: str) -> None:
    """Processa e loga uma mensagem do canal user WS."""
    _log = logging.getLogger("bot_xrp")
    try:
        text: str = raw.decode("utf-8") if isinstance(raw, bytes) else raw
        if text.strip() == "PONG":
            _log.debug("[USER_WS] PONG recebido")
            return

        data = json.loads(text)
        events = data if isinstance(data, list) else [data]

        for event in events:
            if not isinstance(event, dict):
                continue
            evt_type: str = event.get("event_type", event.get("type", "")).upper()

            if evt_type in ("AUTH", "AUTHENTICATED"):
                status = event.get("status", "").upper()
                if status in ("OK", "SUCCESS", "AUTHENTICATED", ""):
                    _log.info("[USER_WS] Auth OK | market=%s...", condition_id[:16])
                else:
                    _log.error("[USER_WS] Auth FALHOU | status=%s", status)
                continue

            if evt_type == "TRADE":
                trade_id = event.get("id", event.get("trade_id", "?"))
                status   = event.get("status", "UNKNOWN").upper()
                market   = event.get("market", event.get("condition_id", "?"))
                outcome  = event.get("outcome", event.get("side", "?"))
                price_s  = str(event.get("price", event.get("avg_price", "?")))
                size_s   = str(event.get("size",  event.get("matched",   "?")))
                order_id = event.get("order_id", "?")
                ts_s     = str(event.get("timestamp", event.get("created_at", "")))

                if status in _TRADE_TERMINAL:
                    _fn = _log.warning if status == "FAILED" else _log.info
                    _fn(
                        "[USER_WS] TRADE %-9s | trade=%s | order=%s | "
                        "market=%s... | outcome=%-4s | price=%s | size=%s | ts=%s",
                        status,
                        str(trade_id)[:16], str(order_id)[:16],
                        str(market)[:16], outcome, price_s, size_s, ts_s,
                    )
                else:
                    _log.debug("[USER_WS] TRADE %-9s | trade=%s", status, str(trade_id)[:16])
                continue

            _log.debug("[USER_WS] EVENT type=%s | %s", evt_type, str(event)[:160])

    except json.JSONDecodeError as jde:
        logging.getLogger("bot_xrp").warning(
            "[USER_WS] JSON inválido: %s | raw=%.80s", jde, str(raw)
        )
    except Exception as exc:
        logging.getLogger("bot_xrp").warning(
            "[USER_WS] Erro msg: %s: %s", type(exc).__name__, str(exc)[:120]
        )


###############################################################################
#                                                                             #
#   ③ ARB ENGINE                                                              #
#                                                                             #
###############################################################################

@dataclass(frozen=True)
class OrderBookLevel:
    price: float
    size:  float
    def __post_init__(self) -> None:
        if not (0.0 < self.price < 1.0):
            raise ValueError(f"Price must be in (0,1), got {self.price}")
        if self.size <= 0:
            raise ValueError(f"Size must be > 0, got {self.size}")


@dataclass(slots=True)
class OrderBookSide:
    levels: list[OrderBookLevel] = field(default_factory=list)

    @property
    def best_price(self) -> Optional[float]:
        return self.levels[0].price if self.levels else None

    @property
    def best_size(self) -> Optional[float]:
        return self.levels[0].size if self.levels else None

    @property
    def is_empty(self) -> bool:
        return len(self.levels) == 0

    def total_volume(self) -> float:
        return sum(lv.size for lv in self.levels)

    @classmethod
    def from_raw(cls, entries: list[dict]) -> "OrderBookSide":
        levels: list[OrderBookLevel] = []
        for e in entries:
            sz = float(e.get("size", 0))
            if sz <= 0: continue
            pr = float(e["price"])
            if 0.0 < pr < 1.0:
                levels.append(OrderBookLevel(price=pr, size=sz))
        levels.sort(key=lambda lv: lv.price)
        return cls(levels=levels)


class ArbStatus(Enum):
    OPPORTUNITY              = "OPPORTUNITY"
    REJECT_PEG_TOO_HIGH      = "REJECT_PEG_TOO_HIGH"
    REJECT_NEGATIVE_PROFIT   = "REJECT_NEGATIVE_PROFIT"
    REJECT_NO_LIQUIDITY_UP   = "REJECT_NO_LIQUIDITY_UP"
    REJECT_NO_LIQUIDITY_DOWN = "REJECT_NO_LIQUIDITY_DOWN"
    REJECT_VWAP_BREAKS_PEG   = "REJECT_VWAP_BREAKS_PEG"
    REJECT_EMPTY_BOOK        = "REJECT_EMPTY_BOOK"
    REJECT_BUDGET_TOO_LOW    = "REJECT_BUDGET_TOO_LOW"


@dataclass(frozen=True, slots=True)
class ArbResult:
    status:             ArbStatus
    lowest_ask_up:      float = 0.0
    lowest_ask_down:    float = 0.0
    peg:                float = 0.0
    gross_margin:       float = 0.0
    shares:             float = 0.0
    cost_up:            float = 0.0
    cost_down:          float = 0.0
    total_cost:         float = 0.0
    payout:             float = 0.0
    net_profit:         float = 0.0
    profit_pct:         float = 0.0
    used_vwap:          bool  = False
    vwap_up:    Optional[float] = None
    vwap_down:  Optional[float] = None
    volume_at_ask_up:   float = 0.0
    volume_at_ask_down: float = 0.0
    reason:             str   = ""


def _calc_vwap(book_side: OrderBookSide, target_size: float) -> tuple[Optional[float], float]:
    if book_side.is_empty: return None, 0.0
    total_cost = 0.0; filled = 0.0
    for level in book_side.levels:
        remaining = target_size - filled
        if remaining <= 0: break
        fill_at = min(level.size, remaining)
        total_cost += fill_at * level.price
        filled += fill_at
    if filled < ARB_MIN_SHARES: return None, 0.0
    return total_cost / filled, filled


def check_liquidity(
    book_side: OrderBookSide,
    target_size: float,
) -> tuple[bool, float, float]:
    if book_side.is_empty: return False, 0.0, 0.0
    avail_best  = book_side.best_size or 0.0
    avail_total = book_side.total_volume()
    return avail_best >= target_size, avail_best, avail_total


def evaluate_arb(
    asks_up:    OrderBookSide,
    asks_down:  OrderBookSide,
    budget:     float,
    peg_trigger: float = ARB_PEG_TRIGGER,
) -> ArbResult:
    """Avalia oportunidade de arbitragem risk-free com fallback VWAP."""
    if asks_up.is_empty:
        return ArbResult(status=ArbStatus.REJECT_EMPTY_BOOK, reason="UP order book vazio")
    if asks_down.is_empty:
        return ArbResult(status=ArbStatus.REJECT_EMPTY_BOOK, reason="DOWN order book vazio")

    la_up: float  = asks_up.best_price   # type: ignore[assignment]
    la_dn: float  = asks_down.best_price  # type: ignore[assignment]
    vol_up = asks_up.best_size or 0.0
    vol_dn = asks_down.best_size or 0.0
    peg    = la_up + la_dn
    gross_margin = ARB_RESOLUTION - peg

    if round(peg, 4) > round(peg_trigger, 4):
        return ArbResult(
            status=ArbStatus.REJECT_PEG_TOO_HIGH,
            lowest_ask_up=la_up, lowest_ask_down=la_dn,
            peg=peg, gross_margin=gross_margin,
            volume_at_ask_up=vol_up, volume_at_ask_down=vol_dn,
            reason=f"Peg={peg:.4f} > trigger={peg_trigger} (ask_up+ask_down > {peg_trigger})",
        )

    fee_u = fee_rate_lut(la_up) * la_up
    fee_d = fee_rate_lut(la_dn) * la_dn
    cost_per_share = la_up + la_dn + fee_u + fee_d

    if cost_per_share <= 0.0 or budget <= 0.0:
        return ArbResult(
            status=ArbStatus.REJECT_BUDGET_TOO_LOW,
            lowest_ask_up=la_up, lowest_ask_down=la_dn,
            peg=peg, reason="Budget ou cost_per_share = zero",
        )

    shares = budget / cost_per_share
    if shares < ARB_MIN_SHARES:
        return ArbResult(
            status=ArbStatus.REJECT_BUDGET_TOO_LOW,
            lowest_ask_up=la_up, lowest_ask_down=la_dn,
            peg=peg, shares=shares,
            reason=f"Shares={shares:.6f} < min={ARB_MIN_SHARES}",
        )

    liq_ok_up, avail_up, total_up = check_liquidity(asks_up,  shares)
    liq_ok_dn, avail_dn, total_dn = check_liquidity(asks_down, shares)

    used_vwap = False
    vwap_up: Optional[float] = None
    vwap_dn: Optional[float] = None
    eff_ask_up = la_up
    eff_ask_dn = la_dn

    if not liq_ok_up or not liq_ok_dn:
        vwap_up, filled_up = _calc_vwap(asks_up,  shares)
        vwap_dn, filled_dn = _calc_vwap(asks_down, shares)

        if vwap_up is None or filled_up < shares * 0.95:
            return ArbResult(
                status=ArbStatus.REJECT_NO_LIQUIDITY_UP,
                lowest_ask_up=la_up, lowest_ask_down=la_dn,
                peg=peg, gross_margin=gross_margin, shares=shares,
                volume_at_ask_up=avail_up, volume_at_ask_down=avail_dn,
                reason=f"UP insuficiente: need={shares:.2f} avail={avail_up:.2f}",
            )
        if vwap_dn is None or filled_dn < shares * 0.95:
            return ArbResult(
                status=ArbStatus.REJECT_NO_LIQUIDITY_DOWN,
                lowest_ask_up=la_up, lowest_ask_down=la_dn,
                peg=peg, gross_margin=gross_margin, shares=shares,
                volume_at_ask_up=avail_up, volume_at_ask_down=avail_dn,
                reason=f"DOWN insuficiente: need={shares:.2f} avail={avail_dn:.2f}",
            )

        vwap_peg = vwap_up + vwap_dn
        if round(vwap_peg, 4) > round(peg_trigger, 4):
            return ArbResult(
                status=ArbStatus.REJECT_VWAP_BREAKS_PEG,
                lowest_ask_up=la_up, lowest_ask_down=la_dn,
                peg=peg, gross_margin=ARB_RESOLUTION - vwap_peg, shares=shares,
                used_vwap=True, vwap_up=vwap_up, vwap_down=vwap_dn,
                volume_at_ask_up=avail_up, volume_at_ask_down=avail_dn,
                reason=f"VWAP Peg={vwap_peg:.4f} > trigger={peg_trigger}",
            )

        used_vwap = True
        eff_ask_up = vwap_up
        eff_ask_dn = vwap_dn
        fee_u = fee_rate_lut(eff_ask_up) * eff_ask_up
        fee_d = fee_rate_lut(eff_ask_dn) * eff_ask_dn
        cost_per_share = eff_ask_up + eff_ask_dn + fee_u + fee_d
        shares = budget / cost_per_share

    cost_up    = _cost_with_fee_f(shares, eff_ask_up)
    cost_dn    = _cost_with_fee_f(shares, eff_ask_dn)
    total_cost = cost_up + cost_dn
    payout     = shares * ARB_RESOLUTION
    net_profit = payout - total_cost
    profit_pct = (net_profit / total_cost * 100.0) if total_cost > 0 else 0.0

    if net_profit <= 0.0:
        return ArbResult(
            status=ArbStatus.REJECT_NEGATIVE_PROFIT,
            lowest_ask_up=la_up, lowest_ask_down=la_dn,
            peg=peg, gross_margin=gross_margin, shares=shares,
            cost_up=cost_up, cost_down=cost_dn, total_cost=total_cost,
            payout=payout, net_profit=net_profit, profit_pct=profit_pct,
            used_vwap=used_vwap, vwap_up=vwap_up, vwap_down=vwap_dn,
            volume_at_ask_up=vol_up, volume_at_ask_down=vol_dn,
            reason=f"Net profit=${net_profit:.6f} <= 0 após fees",
        )

    return ArbResult(
        status=ArbStatus.OPPORTUNITY,
        lowest_ask_up=la_up, lowest_ask_down=la_dn,
        peg=peg, gross_margin=gross_margin, shares=shares,
        cost_up=cost_up, cost_down=cost_dn, total_cost=total_cost,
        payout=payout, net_profit=net_profit, profit_pct=profit_pct,
        used_vwap=used_vwap, vwap_up=vwap_up, vwap_down=vwap_dn,
        volume_at_ask_up=vol_up, volume_at_ask_down=vol_dn,
        reason="OPORTUNIDADE DETETADA — risk-free arb",
    )


###############################################################################
#                                                                             #
#   ④ TRADE STATE MANAGER                                                    #
#                                                                             #
###############################################################################

@dataclass
class TradeState:
    current_martingale_level: int   = 1
    accumulated_loss_session: float = 0.0
    last_round_pnl: float           = 0.0
    daily_pnl: float                = 0.0
    bankroll: float                 = BANKROLL_DEMO
    initial_bankroll: float         = BANKROLL_DEMO
    last_market_day: Optional[str]  = None
    round_count: int                = 0

    @property
    def mart_level(self) -> int:
        return self.current_martingale_level


class TradeStateManager:
    """Persistência de estado com saves atómicos e não-bloqueantes."""

    def __init__(self, filepath: str = STATE_FILE) -> None:
        self.filepath: Path     = Path(filepath)
        self.state: TradeState  = TradeState()
        self._backup_path: Path = self.filepath.with_suffix(".json.bak")

    def _save_blocking(self) -> None:
        data: dict = {
            "current_martingale_level": self.state.current_martingale_level,
            "accumulated_loss_session": self.state.accumulated_loss_session,
            "last_round_pnl":           self.state.last_round_pnl,
            "daily_pnl":                self.state.daily_pnl,
            "bankroll":                 self.state.bankroll,
            "initial_bankroll":         self.state.initial_bankroll,
            "last_market_day":          self.state.last_market_day,
            "round_count":              self.state.round_count,
            "_version": "6.0.0",
            "_saved_at": time.time(),
        }
        raw: bytes   = _state_dumps(data)
        tmp: Path    = self.filepath.with_suffix(".json.tmp")
        try:
            tmp.write_bytes(raw)
            if self.filepath.exists():
                try:
                    self._backup_path.unlink(missing_ok=True)
                    self.filepath.rename(self._backup_path)
                except OSError:
                    pass
            tmp.rename(self.filepath)
        except Exception as exc:
            logging.getLogger("bot_xrp").warning("[STATE] Save FAILED: %s", exc)
            try: tmp.unlink(missing_ok=True)
            except OSError: pass
            raise

    def save(self) -> None:
        self._save_blocking()

    async def save_async(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._save_blocking)

    def load(self) -> bool:
        _log = logging.getLogger("bot_xrp")
        for path in (self.filepath, self._backup_path):
            if not path.exists(): continue
            try:
                raw  = path.read_bytes()
                data = _state_loads(raw)
                stored_br = float(data.get("bankroll", BANKROLL_DEMO))
                self.state = TradeState(
                    current_martingale_level=int(data.get("current_martingale_level", 1)),
                    accumulated_loss_session=float(data.get("accumulated_loss_session", 0.0)),
                    last_round_pnl=float(data.get("last_round_pnl", 0.0)),
                    daily_pnl=float(data.get("daily_pnl", 0.0)),
                    bankroll=stored_br,
                    initial_bankroll=float(data.get("initial_bankroll", stored_br)),
                    last_market_day=data.get("last_market_day"),
                    round_count=int(data.get("round_count", 0)),
                )
                _log.info(
                    "[STATE] Loaded from %s | Mart=x%d | AccLoss=%.4f | "
                    "DailyPnL=%.4f | Bankroll=$%.4f | Initial=$%.4f",
                    path.name, self.state.mart_level,
                    self.state.accumulated_loss_session,
                    self.state.daily_pnl, self.state.bankroll,
                    self.state.initial_bankroll,
                )
                return True
            except Exception as exc:
                _log.warning("[STATE] Corrupt %s: %s — trying backup", path.name, exc)
        _log.info("[STATE] Fresh start | Bankroll=$%.4f", BANKROLL_DEMO)
        self.state.bankroll         = BANKROLL_DEMO
        self.state.initial_bankroll = BANKROLL_DEMO
        return False

    def update_martingale(self, round_pnl: float, max_mult: int = MART_MAX_MULT) -> None:
        _log = logging.getLogger("bot_xrp")
        self.state.last_round_pnl = round_pnl
        _eps = 1e-9
        if round_pnl > _eps:
            old = self.state.current_martingale_level
            if old > 1:
                _log.info("[MART] WIN | x%d→x1 | recovered %.4f",
                          old, self.state.accumulated_loss_session)
            self.state.current_martingale_level = 1
            self.state.accumulated_loss_session = 0.0
        elif round_pnl < -_eps:
            old = self.state.current_martingale_level
            self.state.accumulated_loss_session += abs(round_pnl)
            if old < max_mult:
                new_level = min(old * 2, max_mult)
                self.state.current_martingale_level = new_level
                _log.warning("[MART] LOSS | x%d→x%d | loss=%.4f | acc=%.4f",
                             old, new_level, round_pnl,
                             self.state.accumulated_loss_session)
            else:
                self.state.current_martingale_level = 1
                self.state.accumulated_loss_session = 0.0
                _log.warning("[MART] MAX x%d REACHED — RESET | loss=%.4f",
                             max_mult, round_pnl)
        else:
            _log.info("[MART] NEUTRAL | maintaining x%d | acc=%.4f",
                      self.state.mart_level, self.state.accumulated_loss_session)

    def calc_next_stake(self, base_stake: float, ask_price: float) -> float:
        raw_stake = base_stake * float(self.state.mart_level)
        fee = fee_rate_lut(ask_price)
        acc = self.state.accumulated_loss_session
        if acc > 0.0 and 0.0 < ask_price < 1.0:
            margin = 1.0 - ask_price * (1.0 + fee)
            if margin > 1e-9:
                min_stake = (acc / margin) * ask_price * (1.0 + fee)
                if min_stake > raw_stake:
                    raw_stake = min_stake
        return round(raw_stake, 4)

    def update_daily_pnl(self, round_pnl: float) -> None:
        self.state.daily_pnl   += round_pnl
        self.state.round_count += 1

    def reset_daily(self, new_day: str) -> None:
        self.state.daily_pnl   = 0.0
        self.state.round_count = 0
        self.state.last_market_day = new_day
        logging.getLogger("bot_xrp").info(
            "[STATE] NEW DAY %s | Mart PERSISTED x%d | AccLoss=%.4f",
            new_day, self.state.mart_level, self.state.accumulated_loss_session,
        )

    def update_bankroll(self, new_bankroll: float) -> None:
        self.state.bankroll = new_bankroll

    def pnl_total_inicio_pct(self) -> float:
        ib = self.state.initial_bankroll
        if ib < 1e-9: return 0.0
        return (self.state.bankroll - ib) / ib * 100.0


###############################################################################
#                                                                             #
#   ⑤ MARKET TIMER                                                           #
#                                                                             #
###############################################################################

class MarketTimer:
    """Enforcer de janelas temporais por estratégia — toda a lógica centralizada."""

    def __init__(
        self,
        market_end_ts: float,
        gambling_window_s:  float = GAMB_START_REM_S,
        gambling_cutoff_s:  float = GAMB_CUTOFF_S,
        peg_arbit_buffer_s: float = PA_BUFFER_S,
        peg_arbit_min_rem_s: float = PA_MIN_REM,
        endgame_trigger_s:  float = AGGRESSIVE_ENDGAME_S,
    ) -> None:
        self.market_end_ts      = market_end_ts
        self.gambling_window_s  = gambling_window_s
        self.gambling_cutoff_s  = gambling_cutoff_s
        self.peg_arbit_buffer_s = peg_arbit_buffer_s
        self.peg_arbit_min_rem_s = peg_arbit_min_rem_s
        self.endgame_trigger_s  = endgame_trigger_s

    @property
    def remaining(self) -> float:
        return max(0.0, self.market_end_ts - time.time())

    @property
    def is_expired(self) -> bool:
        return self.remaining <= 0.0

    def can_gambling_enter(self) -> bool:
        rem = self.remaining
        return self.gambling_cutoff_s < rem <= self.gambling_window_s

    def can_peg_arbit(self) -> bool:
        return self.remaining > max(self.peg_arbit_buffer_s, self.peg_arbit_min_rem_s)

    def is_endgame(self) -> bool:
        rem = self.remaining
        return 0.0 < rem <= self.endgame_trigger_s

    def remaining_str(self) -> str:
        rem = max(0.0, self.remaining)
        return f"{int(rem // 60):02d}:{int(rem % 60):02d}:{int((rem * 1000) % 1000):03d}"


###############################################################################
#                                                                             #
#   ⑥ AUDIT LOGGER                                                           #
#                                                                             #
###############################################################################

class AuditLogger:
    def __init__(self, logger_instance: logging.Logger) -> None:
        self.logger = logger_instance

    @staticmethod
    def _ts() -> str:
        return datetime.now().strftime("%d/%m/%y | %H:%M:%S.%f")[:-3]

    def log_trade(
        self, strategy: str, action: str, symbol: str, price: float,
        mart_level: int, pnl_round: float, pnl_day: float, fee: float,
        extra: str = "",
    ) -> None:
        msg = (
            f"[{strategy}] [{action}] [{self._ts()}] | "
            f"Pair: {symbol} | Price: {price:.4f} | "
            f"Martingale: x{mart_level} | "
            f"PnL Round: {pnl_round:+.4f} | "
            f"PnL Day: {pnl_day:+.4f} | "
            f"Fees: {fee:.6f}"
        )
        if extra: msg += f" | {extra}"
        self.logger.info(msg)

    def log_event(self, strategy: str, action: str, message: str) -> None:
        self.logger.info(f"[{strategy}] [{action}] [{self._ts()}] | {message}")

    def log_error(self, strategy: str, action: str, message: str) -> None:
        self.logger.error(f"[{strategy}] [{action}] [ERROR] [{self._ts()}] | {message}")


###############################################################################
#                                                                             #
#   ⑨ ATOMIC ARB EXECUTOR                                                    #
#                                                                             #
###############################################################################

class AtomicExecStatus(Enum):
    SUCCESS                = "SUCCESS"
    PARTIAL_FILL_RECOVERED = "PARTIAL_FILL_RECOVERED"
    PARTIAL_FILL_FAILED    = "PARTIAL_FILL_FAILED"
    BOTH_FAILED            = "BOTH_FAILED"
    LIQUIDITY_CHECK_FAILED = "LIQUIDITY_CHECK_FAILED"
    SKIPPED                = "SKIPPED"


@dataclass
class AtomicExecResult:
    status:             AtomicExecStatus
    arb:                Optional[ArbResult] = None
    order_up_ok:        bool  = False
    order_down_ok:      bool  = False
    recovery_attempted: bool  = False
    recovery_ok:        bool  = False
    error_message:      str   = ""
    execution_time_ms:  float = 0.0


class AtomicArbExecutor:
    """Execução atómica de PEG ARBIT com rollback em caso de fill parcial."""

    def __init__(
        self,
        place_order_fn:       Callable[..., Awaitable[bool]],
        place_market_close_fn: Optional[Callable[..., Awaitable[bool]]] = None,
    ) -> None:
        self.place_order       = place_order_fn
        self.place_market_close = place_market_close_fn

    async def execute_atomic(
        self,
        arb:       ArbResult,
        meta:      dict,
        asks_up:   OrderBookSide,
        asks_down: OrderBookSide,
    ) -> AtomicExecResult:
        _log = logging.getLogger("bot_xrp")
        start_ms = time.monotonic() * 1000

        if arb.status != ArbStatus.OPPORTUNITY:
            return AtomicExecResult(
                status=AtomicExecStatus.SKIPPED, arb=arb,
                error_message=f"Não é oportunidade: {arb.status.value}",
            )

        liq_ok_up, avail_up, _ = check_liquidity(asks_up,  arb.shares)
        liq_ok_dn, avail_dn, _ = check_liquidity(asks_down, arb.shares)

        if not liq_ok_up and asks_up.total_volume() < arb.shares * 0.95:
            return AtomicExecResult(
                status=AtomicExecStatus.LIQUIDITY_CHECK_FAILED, arb=arb,
                error_message=f"Pre-flight FAIL: UP {avail_up:.2f} < need {arb.shares:.2f}",
            )
        if not liq_ok_dn and asks_down.total_volume() < arb.shares * 0.95:
            return AtomicExecResult(
                status=AtomicExecStatus.LIQUIDITY_CHECK_FAILED, arb=arb,
                error_message=f"Pre-flight FAIL: DOWN {avail_dn:.2f} < need {arb.shares:.2f}",
            )

        eff_up = arb.vwap_up   if arb.used_vwap and arb.vwap_up   else arb.lowest_ask_up
        eff_dn = arb.vwap_down if arb.used_vwap and arb.vwap_down  else arb.lowest_ask_down

        try:
            result_up, result_dn = await asyncio.gather(
                self.place_order("UP",   eff_up, arb.shares, meta["up"]),
                self.place_order("DOWN", eff_dn, arb.shares, meta["down"]),
                return_exceptions=True,
            )
        except Exception as exc:
            return AtomicExecResult(
                status=AtomicExecStatus.BOTH_FAILED, arb=arb,
                error_message=f"gather exception: {exc}",
                execution_time_ms=time.monotonic() * 1000 - start_ms,
            )

        ok_up = (result_up is True)  if not isinstance(result_up,  Exception) else False
        ok_dn = (result_dn is True)  if not isinstance(result_dn,  Exception) else False
        elapsed = time.monotonic() * 1000 - start_ms

        if ok_up and ok_dn:
            _log.info(
                "[PEG ARBIT] [ATOMIC_OK] | Both legs filled in %.0f ms | "
                "shares=%.4f | profit=$%.4f", elapsed, arb.shares, arb.net_profit,
            )
            return AtomicExecResult(
                status=AtomicExecStatus.SUCCESS, arb=arb,
                order_up_ok=True, order_down_ok=True, execution_time_ms=elapsed,
            )

        if not ok_up and not ok_dn:
            _log.warning("[PEG ARBIT] [ATOMIC_FAIL] | Both legs failed in %.0f ms", elapsed)
            return AtomicExecResult(
                status=AtomicExecStatus.BOTH_FAILED, arb=arb,
                error_message="Both legs failed", execution_time_ms=elapsed,
            )

        # Fill parcial — rollback imediato
        filled_side  = "UP"   if ok_up else "DOWN"
        failed_side  = "DOWN" if ok_up else "UP"
        filled_token = meta["up"] if ok_up else meta["down"]
        _log.error(
            "[PEG ARBIT] [PARTIAL_FILL] CRÍTICO | %s filled, %s FAILED — rollback",
            filled_side, failed_side,
        )

        recovery_ok = False
        if self.place_market_close is not None:
            try:
                recovery_ok = await asyncio.wait_for(
                    self.place_market_close(filled_side, arb.shares, filled_token),
                    timeout=ROLLBACK_TIMEOUT_S,
                )
            except asyncio.TimeoutError:
                _log.error("[PEG ARBIT] [ROLLBACK_TIMEOUT] %.0f ms", ROLLBACK_TIMEOUT_S * 1000)
            except Exception as exc:
                _log.error("[PEG ARBIT] [ROLLBACK_EXCEPTION] %s", exc)

        if recovery_ok:
            _log.warning("[PEG ARBIT] [ROLLBACK_OK] | Closed %s via Market Order", filled_side)
            return AtomicExecResult(
                status=AtomicExecStatus.PARTIAL_FILL_RECOVERED, arb=arb,
                order_up_ok=ok_up, order_down_ok=ok_dn,
                recovery_attempted=True, recovery_ok=True,
                execution_time_ms=time.monotonic() * 1000 - start_ms,
            )
        else:
            _log.critical("[PEG ARBIT] [ROLLBACK_FAILED] HALT | Cannot close %s", filled_side)
            return AtomicExecResult(
                status=AtomicExecStatus.PARTIAL_FILL_FAILED, arb=arb,
                order_up_ok=ok_up, order_down_ok=ok_dn,
                recovery_attempted=True, recovery_ok=False,
                error_message=f"CRITICAL: {filled_side} filled, {failed_side} failed — HALTING",
                execution_time_ms=time.monotonic() * 1000 - start_ms,
            )


###############################################################################
#                                                                             #
#   GLOBAL STATE                                                              #
#                                                                             #
###############################################################################

tsm: TradeStateManager = TradeStateManager(STATE_FILE)

best_bids:      dict[str, Optional[float]] = {"up": None, "down": None}
best_asks:      dict[str, Optional[float]] = {"up": None, "down": None}
best_spreads_c: dict[str, Optional[float]] = {"up": None, "down": None}
best_bid_sizes: dict[str, Optional[float]] = {"up": None, "down": None}
best_ask_sizes: dict[str, Optional[float]] = {"up": None, "down": None}

price_change:          asyncio.Event       = asyncio.Event()
bot_start_time:        float               = time.time()
_shutdown_flag:        bool                = False
resolved_event:        asyncio.Event       = asyncio.Event()
resolved_winner_asset: Optional[str]       = None


###############################################################################
#                                                                             #
#   LOGGING                                                                   #
#                                                                             #
###############################################################################

_fmt = logging.Formatter("%(message)s")
_fh  = logging.FileHandler("bot_xrp_v6_unified.log", encoding="utf-8")
_fh.setFormatter(_fmt)

logger: logging.Logger = logging.getLogger("bot_xrp")
logger.setLevel(logging.DEBUG)
logger.addHandler(_fh)
logger.propagate = False
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("websockets.client").setLevel(logging.WARNING)

audit: AuditLogger = AuditLogger(logger)


###############################################################################
#                                                                             #
#   FORMATTING HELPERS                                                        #
#                                                                             #
###############################################################################

def get_ts() -> str:
    return datetime.now().strftime("%d/%m/%y | %H:%M:%S.%f")[:-3]

def get_uptime_str() -> str:
    elapsed = int(time.time() - bot_start_time)
    h, rem = divmod(elapsed, 3600)
    m, s   = divmod(rem, 60)
    return f"{h:02d}h:{m:02d}m:{s:02d}s"

def fc(p: float) -> str:
    return f"{float(p) * 100:.1f}c"

def fmt_dollar(v: float) -> str:
    v = float(v)
    if v < 0.0:   return f"$-{abs(v):.4f}"
    elif v > 0.0: return f"$+{v:.4f}"
    return f"${v:.4f}"

def fmt_fee(fee: float, base: float) -> str:
    pct = (fee / base * 100.0) if base > 1e-9 else 0.0
    return f"{fmt_dollar(fee)} ({pct:.2f}%)"

def fmt_pct(v: float) -> str:
    if v < 0.0:   return f"-{abs(v):.2f}%"
    elif v > 0.0: return f"+{v:.2f}%"
    return f"{v:.2f}%"

def log_info(msg: str)  -> None: logger.info(f"[INFO] [{get_ts()}] | {msg}")
def log_warn(msg: str)  -> None: logger.warning(f"[WARN] [{get_ts()}] | {msg}")
def log_raw(msg: str)   -> None: logger.info(f"[{get_ts()}] | {msg}")
def log_sep()           -> None: logger.info("-" * 80)
def log_sep2()          -> None: logger.info("=" * 80)

def log_m(module: str, action: str, msg: str) -> None:
    logger.info(f"[INFO] [{module}] [{action}] [{get_ts()}] | {msg}")

def log_ws_event(action: str, msg: str) -> None:
    logger.info(f"[WS] [{action}] [{get_ts()}] | {msg}")


###############################################################################
#                                                                             #
#   ⑦ BAYESIAN TRACKER v4.0                                                  #
#                                                                             #
###############################################################################

class BayesianTracker:
    __slots__ = ("log_post_up", "log_post_down", "prev_kal_up", "prev_kal_down",
                 "tick_count", "std", "decay")

    def __init__(
        self,
        prior: float = BAYESIAN_PRIOR,
        std:   float = BAYESIAN_LIKELIHOOD_STD,
        decay: float = BAYESIAN_DECAY,
    ) -> None:
        self.log_post_up   = math.log(max(prior, 1e-15))
        self.log_post_down = math.log(max(1.0 - prior, 1e-15))
        self.prev_kal_up:   Optional[float] = None
        self.prev_kal_down: Optional[float] = None
        self.tick_count: int = 0
        self.std   = std
        self.decay = decay

    def update(
        self,
        kal_up: float, kal_down: float,
        obi_up: Optional[float], obi_down: Optional[float],
        vpin_up: Optional[float], vpin_down: Optional[float],
    ) -> tuple[float, float]:
        self.tick_count += 1
        center = (self.log_post_up + self.log_post_down) / 2.0
        self.log_post_up   = center + self.decay * (self.log_post_up   - center)
        self.log_post_down = center + self.decay * (self.log_post_down - center)

        if self.prev_kal_up is not None:
            net = (kal_up - self.prev_kal_up) - (kal_down - self.prev_kal_down)  # type: ignore[operator]
            self.log_post_up   += net / self.std * 0.5
            self.log_post_down -= net / self.std * 0.5

        self.prev_kal_up, self.prev_kal_down = kal_up, kal_down

        if obi_up is not None and obi_down is not None:
            obi_net = ((obi_up - 0.5) * 2.0 - (obi_down - 0.5) * 2.0) * 0.3
            self.log_post_up   += obi_net
            self.log_post_down -= obi_net

        if vpin_up is not None and vpin_down is not None:
            vpin_net = ((1.0 - vpin_up) - (1.0 - vpin_down)) * 0.2
            self.log_post_up   += vpin_net
            self.log_post_down -= vpin_net

        log_z = self._lse(self.log_post_up, self.log_post_down)
        p_up = max(0.01, min(0.99, math.exp(self.log_post_up - log_z)))
        return p_up, 1.0 - p_up

    def get_posteriors(self) -> tuple[float, float]:
        log_z = self._lse(self.log_post_up, self.log_post_down)
        p_up = max(0.01, min(0.99, math.exp(self.log_post_up - log_z)))
        return p_up, 1.0 - p_up

    @staticmethod
    def _lse(a: float, b: float) -> float:
        mx = max(a, b)
        return mx + math.log(math.exp(a - mx) + math.exp(b - mx))

    def reset(self, prior: float = BAYESIAN_PRIOR) -> None:
        self.log_post_up   = math.log(max(prior, 1e-15))
        self.log_post_down = math.log(max(1.0 - prior, 1e-15))
        self.prev_kal_up = self.prev_kal_down = None
        self.tick_count = 0


###############################################################################
#                                                                             #
#   ⑧ LMSR PRICER v4.0                                                       #
#                                                                             #
###############################################################################

class LMSRPricer:
    __slots__ = ("b",)

    def __init__(self, b: float = LMSR_B) -> None:
        self.b = b

    def prices(self, quantities: list[float]) -> list[float]:
        b = self.b; max_q = max(quantities)
        exps = [math.exp((qi - max_q) / b) for qi in quantities]
        total = sum(exps)
        return [e / total for e in exps]

    def inefficiency(self, fair_prices: list[float], market_asks: list[float]) -> list[float]:
        return [fp - ma for fp, ma in zip(fair_prices, market_asks)]

    def max_loss(self, n: int = 2) -> float:
        return self.b * math.log(n)


lmsr_pricer: LMSRPricer = LMSRPricer()


###############################################################################
#                                                                             #
#   PRODUÇÃO — Rate Limiter + Circuit Breaker                                 #
#                                                                             #
###############################################################################

class RateLimiter:
    __slots__ = ("cps", "burst", "tokens", "last_check", "_lock")

    def __init__(self, cps: float = RATE_LIMIT_CALLS, burst: float = RATE_LIMIT_BURST) -> None:
        self.cps = cps; self.burst = burst
        self.tokens = burst; self.last_check = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self.tokens = min(self.burst, self.tokens + (now - self.last_check) * self.cps)
            self.last_check = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
            else:
                await asyncio.sleep((1.0 - self.tokens) / self.cps)
                self.tokens = 0.0


rate_limiter: RateLimiter = RateLimiter()


class CircuitBreaker:
    __slots__ = ("ft", "rs", "_f", "_st", "_at")
    CLOSED = "CLOSED"; OPEN = "OPEN"; HALF = "HALF-OPEN"

    def __init__(self, ft: int = CB_FAIL_THRESHOLD, rs: float = CB_RECOVERY_S) -> None:
        self.ft = ft; self.rs = rs
        self._f = 0; self._st = self.CLOSED; self._at = 0.0

    def is_open(self) -> bool:
        if self._st == self.CLOSED: return False
        if self._st == self.OPEN:
            if time.monotonic() - self._at >= self.rs:
                self._st = self.HALF; return False
            return True
        return False

    def record_success(self) -> None:
        self._st = self.CLOSED; self._f = 0

    def record_failure(self) -> None:
        self._f += 1
        if self._st == self.HALF:
            self._st = self.OPEN; self._at = time.monotonic()
        elif self._f >= self.ft and self._st == self.CLOSED:
            self._st = self.OPEN; self._at = time.monotonic()
            log_warn(f"CB | CLOSED→OPEN ({self._f} failures)")


api_cb: CircuitBreaker = CircuitBreaker()


###############################################################################
#                                                                             #
#   RETRY + SECRETS + SDK                                                     #
#                                                                             #
###############################################################################

async def retry_with_backoff(fn: Callable, *args, label: str = "call", **kwargs) -> Optional[object]:
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_API_RETRIES + 1):
        try:
            if asyncio.iscoroutinefunction(fn):
                return await fn(*args, **kwargs)
            else:
                return await asyncio.get_running_loop().run_in_executor(
                    None, lambda: fn(*args, **kwargs)
                )
        except Exception as exc:
            last_exc = exc
            if attempt < MAX_API_RETRIES:
                bk = min(BASE_BACKOFF_S * (2 ** (attempt - 1)), MAX_BACKOFF_S)
                if BACKOFF_JITTER:
                    bk *= 0.7 + random.random() * 0.6
                await asyncio.sleep(bk)
    log_warn(f"retry [{label}] GAVE UP: {last_exc}")
    return None


def load_secrets(filepath: str = "secrets.txt") -> dict[str, str]:
    if not os.path.exists(filepath): return {}
    out: dict[str, str] = {}
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                out[k.strip()] = v.strip()
    return out


_creds: dict[str, str] = load_secrets()
POLYMARKET_PRIVATE_KEY: str = _creds.get("POLYMARKET_PRIVATE_KEY", "")
POLYMARKET_API_KEY:     str = _creds.get("POLYMARKET_API_KEY",     "")
POLYMARKET_SECRET:      str = _creds.get("POLYMARKET_SECRET",      "")
POLYMARKET_PASSPHRASE:  str = _creds.get("POLYMARKET_PASSPHRASE",  "")

clob_client    = None
clob_ro_client = None
_HAS_SDK: bool = False

try:
    from py_clob_client.client import ClobClient  # type: ignore
    from py_clob_client.clob_types import OrderArgs, OrderType  # type: ignore
    from py_clob_client.order_builder.constants import BUY  as SDK_BUY   # type: ignore
    from py_clob_client.order_builder.constants import SELL as SDK_SELL  # type: ignore
    _HAS_SDK = True
    clob_ro_client = ClobClient(host=CLOB_REST_URL, chain_id=137)
    if LIVE_TRADING:
        if not POLYMARKET_PRIVATE_KEY:
            raise SystemExit("POLYMARKET_PRIVATE_KEY não definida em secrets.txt")
        clob_client = ClobClient(host=CLOB_REST_URL, key=POLYMARKET_PRIVATE_KEY, chain_id=137)
        log_info("SDK — LIVE TRADING ACTIVE")
    else:
        log_info("SDK — DEMO MODE")
except ImportError:
    if LIVE_TRADING:
        raise SystemExit("py-clob-client não instalado — necessário para LIVE_TRADING=True")
    log_warn("py-clob-client não instalado — DEMO ONLY")
except SystemExit:
    raise
except Exception as _sdk_err:
    log_warn(f"SDK init: {_sdk_err}")


###############################################################################
#                                                                             #
#   CORE MATH                                                                 #
#                                                                             #
###############################################################################

def fee_rate_f(p: float) -> float:
    return fee_rate_lut(p)

def eff_price_c_f(ask: float) -> float:
    return ask * (1.0 + fee_rate_lut(ask)) * 100.0

def sell_payout_net(shares: float, bid: float) -> float:
    return float(shares) * float(bid) * (1.0 - fee_rate_lut(float(bid)))

def resolution_payout(shares: float, winner: bool) -> float:
    return float(shares) if winner else 0.0

def calc_imbalance(bid_size: Optional[float], ask_size: Optional[float]) -> Optional[float]:
    if bid_size is None or ask_size is None: return None
    total = bid_size + ask_size
    return bid_size / total if total > 1e-9 else None

def calc_ev_bayesian(p_hat: float, ask: float) -> float:
    return p_hat - ask

def calc_kelly_bayesian(p_hat: float, ask: float, mart_level: int) -> float:
    if ask <= 0.0 or ask >= 1.0: return 0.0
    kelly = p_hat - (1.0 - p_hat) / ((1.0 - ask) / ask)
    if kelly <= 0.0: return 0.0
    return min(kelly * KELLY_FRACTION * mart_level, _F_KELLY_MAX_RISK_PCT * MART_MAX_MULT)

def calc_kelly_risk(ask: float, mart_level: int) -> float:
    if ask <= 0.0 or ask >= 1.0: return 0.0
    p_est = min(ask + KELLY_ASSUMED_EDGE, 0.995)
    kelly = p_est - (1.0 - p_est) / ((1.0 - ask) / ask)
    if kelly <= 0.0: return 0.0
    return min(kelly * KELLY_FRACTION * mart_level, _F_KELLY_MAX_RISK_PCT * MART_MAX_MULT)

def calc_ev_static(ask: float) -> float:
    if ask <= 0.0 or ask >= 1.0: return 0.0
    p_est = min(ask + KELLY_ASSUMED_EDGE, 0.995)
    return p_est * (1.0 - ask) - (1.0 - p_est) * ask


###############################################################################
#                                                                             #
#   API HELPERS                                                               #
#                                                                             #
###############################################################################

try:
    import requests  # type: ignore
except ImportError:
    requests = None  # type: ignore


def _fetch_metadata_sync(slug: str) -> Optional[dict]:
    if requests is None: return None
    data = requests.get(
        f"{GAMMA_API_URL}/events?slug={slug}", timeout=5
    ).json()[0]["markets"][0]
    ids = json.loads(data["clobTokenIds"])
    return {"id": data["conditionId"], "up": ids[0], "down": ids[1], "slug": slug}


async def fetch_metadata(slug: str) -> Optional[dict]:
    if api_cb.is_open(): return None
    await rate_limiter.acquire()
    result = await retry_with_backoff(_fetch_metadata_sync, slug, label=f"meta({slug})")
    if result: api_cb.record_success()
    else:       api_cb.record_failure()
    return result  # type: ignore[return-value]


def get_current_slug() -> tuple[str, float]:
    now = time.time()
    start_ts = now - (now % 300)
    if (start_ts + 300 - now) < 5:
        start_ts += 300
    return f"xrp-updown-5m-{int(start_ts)}", start_ts


async def fetch_live_bankroll() -> Optional[float]:
    if not clob_client: return None
    await rate_limiter.acquire()
    result = await retry_with_backoff(
        lambda: float(clob_client.get_balance()), label="bankroll"
    )
    return float(result) if result is not None else None


###############################################################################
#                                                                             #
#   POLYMARKET WEBSOCKET HANDLER                                              #
#                                                                             #
###############################################################################

async def ws_handler(t_up: str, t_down: str) -> None:
    """Handler WS para preços em tempo real (Polymarket CLOB market channel)."""
    global resolved_winner_asset

    _bids, _asks, _sprc = best_bids, best_asks, best_spreads_c
    _bsizes, _asizes    = best_bid_sizes, best_ask_sizes
    _set                = price_change.set
    _tid_map: dict[str, str] = {t_up: "up", t_down: "down"}
    _backoff = WS_RECONNECT_BASE_S

    try:
        import websockets  # type: ignore
    except ImportError:
        log_warn("websockets não instalado — pip install websockets")
        return

    while not _shutdown_flag:
        try:
            async with websockets.connect(
                WS_URI,
                ping_interval=WS_HEARTBEAT_INTERVAL,
                ping_timeout=WS_HEARTBEAT_TIMEOUT,
            ) as ws:
                sub = {"assets_ids": [t_up, t_down], "type": "market",
                       "custom_feature_enabled": True}
                await ws.send(_state_dumps(sub) if _HAS_ORJSON else json.dumps(sub))
                log_ws_event("OPEN", f"hb={WS_HEARTBEAT_INTERVAL}s/{WS_HEARTBEAT_TIMEOUT}s")
                _backoff = WS_RECONNECT_BASE_S

                async for raw in ws:
                    items = _state_loads(raw) if isinstance(raw, bytes) else json.loads(raw)
                    if not isinstance(items, list): items = [items]
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

                        sk = _tid_map.get(item.get("asset_id"))
                        if sk is None: continue

                        bid_p = ask_p = None
                        if evt == "book":
                            for side_data, is_bid in [
                                (item.get("bids", []), True),
                                (item.get("asks", []), False),
                            ]:
                                if not side_data: continue
                                best_p = -1.0 if is_bid else float("inf")
                                best_e = None
                                for d in side_data:
                                    sz = float(d.get("size", 0))
                                    if sz <= 0: continue
                                    pr = float(d["price"])
                                    if (is_bid and pr > best_p) or (not is_bid and pr < best_p):
                                        best_p, best_e = pr, d
                                if best_e:
                                    if is_bid:
                                        bid_p = best_p; _bsizes[sk] = float(best_e.get("size", 0))
                                    else:
                                        ask_p = best_p; _asizes[sk] = float(best_e.get("size", 0))
                            if bid_p is not None and ask_p is not None:
                                _sprc[sk] = (ask_p - bid_p) * 100.0

                        elif evt in ("best_bid_ask", "price_change"):
                            src = item
                            if evt == "price_change":
                                pcs = item.get("price_changes", [])
                                if pcs: src = pcs[-1]
                            bb, ba = src.get("best_bid"), src.get("best_ask")
                            if bb: bid_p = float(bb)
                            if ba: ask_p = float(ba)
                            if bid_p and ask_p: _sprc[sk] = (ask_p - bid_p) * 100.0

                        if bid_p is not None: _bids[sk] = bid_p; updated = True
                        if ask_p is not None: _asks[sk] = ask_p; updated = True

                    if updated: _set()

        except asyncio.CancelledError:
            break
        except Exception as e:
            log_ws_event("ERROR", f"{type(e).__name__}: {e} — reconnect in {_backoff:.1f}s")
            await asyncio.sleep(_backoff)
            _backoff = min(_backoff * 2.0, WS_RECONNECT_MAX_S)


###############################################################################
#                                                                             #
#   ORDER EXECUTION                                                           #
#                                                                             #
###############################################################################

def _compute_worst_price(side: str, price: float, slippage: float) -> float:
    worst = price + slippage if side == "BUY" else price - slippage
    return round(max(0.01, min(0.99, worst)), 2)


async def place_live_order(side_label: str, ask: float, shares: float, token_id: str) -> bool:
    """Coloca ordem FOK — delega para execute_trade() com DRY_RUN global."""
    if api_cb.is_open(): return False
    await rate_limiter.acquire()
    worst    = _compute_worst_price("BUY", float(ask), SLIPPAGE_TOLERANCE)
    amount   = float(shares) * float(ask)

    success = await execute_trade(
        clob_client=clob_client,
        token_id=token_id,
        side="BUY",
        amount=amount,
        price=worst,
        is_dry_run=DRY_RUN,
    )
    if success:
        api_cb.record_success()
    else:
        api_cb.record_failure()
        audit.log_error("ORDER", "FOK_FAILED", f"token={token_id[:16]} ask={ask:.4f}")
    return success


async def place_live_order_arb(side_label: str, ask: float, shares: float, token_id: str) -> bool:
    """Ordem FOK atómica para PEG ARBIT — ZERO slippage, executa ao ask exacto.

    Requisito de arbitragem: o preço de execução tem de ser exactamente o ask
    do orderbook no momento da decisão. Qualquer slippage destrói a margem.
    Se a ordem não preencher ao preço exacto, é rejeitada (FOK).
    """
    if api_cb.is_open(): return False
    await rate_limiter.acquire()
    # Slippage = 0 — preço exacto, sem ajustamento
    exact_price = round(max(0.01, min(0.99, float(ask))), 4)
    amount      = float(shares) * float(ask)

    success = await execute_trade(
        clob_client=clob_client,
        token_id=token_id,
        side="BUY",
        amount=amount,
        price=exact_price,
        is_dry_run=DRY_RUN,
    )
    if success:
        api_cb.record_success()
    else:
        api_cb.record_failure()
        audit.log_error("ARB", "FOK_ARBIT_FAILED",
                        f"token={token_id[:16]} ask={ask:.4f} (zero-slip rejected)")
    return success


async def place_market_close(side_label: str, shares: float, token_id: str) -> bool:
    """Fecha posição via Market Order (rollback de fill parcial).

    Vende a perna preenchida com penalidade extra de ROLLBACK_EXTRA_SLIP (5c)
    abaixo do bid — garante fill imediato no rollback atómico de 500 ms.
    """
    if not LIVE_TRADING or not _HAS_SDK or not clob_client: return True
    try:
        await rate_limiter.acquire()
        bid = best_bids.get(side_label.lower())
        if bid is None or bid <= 0: return False
        # +5c de penalidade extra para garantir fill imediato no rollback
        worst  = _compute_worst_price("SELL", bid, SLIPPAGE_TOLERANCE + ROLLBACK_EXTRA_SLIP)
        amount = float(shares) * bid
        success = await execute_trade(
            clob_client=clob_client,
            token_id=token_id,
            side="SELL",
            amount=amount,
            price=worst,
            is_dry_run=DRY_RUN,
        )
        if success: api_cb.record_success()
        else:       api_cb.record_failure()
        return success
    except Exception as exc:
        api_cb.record_failure()
        audit.log_error("PEG ARBIT", "MARKET_CLOSE_FAILED", str(exc))
        return False


###############################################################################
#                                                                             #
#   KALMAN FILTER + HFT WINDOW + VPIN TRACKER                                 #
#                                                                             #
###############################################################################

class KalmanFilter1D:
    __slots__ = ("q", "r", "x", "p")
    def __init__(self, q: float = KALMAN_PROCESS_NOISE, r: float = KALMAN_MEASURE_NOISE) -> None:
        self.q = q; self.r = r; self.x: Optional[float] = None; self.p: float = 1.0
    def update(self, z: float) -> float:
        if self.x is None: self.x = z; return z
        p_pred = self.p + self.q; k = p_pred / (p_pred + self.r)
        self.x = self.x + k * (z - self.x); self.p = (1.0 - k) * p_pred; return self.x
    def reset(self) -> None: self.x = None; self.p = 1.0


class HFTWindow:
    __slots__ = ("window_s", "data", "_cached_mean", "_cached_std")

    def __init__(self, ws: float = HFT_WINDOW_SECONDS) -> None:
        self.window_s = ws; self.data: deque = deque()
        self._cached_mean: Optional[float] = None
        self._cached_std:  Optional[float] = None

    def add(self, price: float, ts: float) -> None:
        self.data.append((ts, price))
        cutoff = ts - self.window_s
        while self.data and self.data[0][0] < cutoff: self.data.popleft()
        self._cached_mean = self._cached_std = None

    def _compute_stats(self) -> tuple[Optional[float], Optional[float]]:
        if self._cached_mean is not None or self._cached_std is not None:
            return self._cached_mean, self._cached_std
        n = len(self.data)
        if n < 3: return None, None
        prices = [p for _, p in self.data]
        mean   = sum(prices) / n
        var    = sum((p - mean) ** 2 for p in prices) / n
        self._cached_mean = mean; self._cached_std = math.sqrt(var)
        return self._cached_mean, self._cached_std

    def zscore(self, price: float) -> Optional[float]:
        mean, std = self._compute_stats()
        if mean is None: return None
        return 0.0 if std < 1e-9 else (price - mean) / std  # type: ignore[operator]

    def std(self) -> Optional[float]:
        _, s = self._compute_stats(); return s

    def clear(self) -> None:
        self.data.clear(); self._cached_mean = self._cached_std = None


class VPINTracker:
    __slots__ = ("window_s", "data", "prev_mid")
    def __init__(self, ws: float = HFT_WINDOW_SECONDS) -> None:
        self.window_s = ws; self.data: deque = deque(); self.prev_mid: Optional[float] = None
    def add(self, kal_mid: float, total_size: float, ts: float) -> None:
        if self.prev_mid is not None and total_size > 1e-9:
            if   kal_mid > self.prev_mid: self.data.append((ts,  total_size))
            elif kal_mid < self.prev_mid: self.data.append((ts, -total_size))
        self.prev_mid = kal_mid
        cutoff = ts - self.window_s
        while self.data and self.data[0][0] < cutoff: self.data.popleft()
    def vpin(self) -> Optional[float]:
        if not self.data: return None
        buy  = sum(v for _, v in self.data if v > 0)
        sell = sum(-v for _, v in self.data if v < 0)
        total = buy + sell
        return abs(buy - sell) / total if total > 1e-9 else None
    def reset(self) -> None: self.data.clear(); self.prev_mid = None


###############################################################################
#                                                                             #
#   ⑩ LOGIC LOOP                                                              #
#                                                                             #
###############################################################################

async def logic_loop(m_start: float, m_end: float, meta: dict) -> float:
    """Loop principal de trading para um ciclo de mercado de 5 minutos.

    Integração Binance Oracle:
        Em cada tick, após o update Bayesiano, as probabilidades p_hat_up/down
        são blendadas com o sinal Black-Scholes da Binance:
            p_blend = BINANCE_BLEND_WEIGHT × p_binance + (1 - W) × p_bayes
        Fallback para Bayesian puro se Binance estiver stale (> 10 s).

    Returns: bankroll final após settlement.
    """
    bankroll: float       = tsm.state.bankroll
    active_trades: list[dict] = []
    eff_pa_risk = min(PEG_ARBIT_RISK, _F_MAX_MARKET_EXPOSURE)

    bayesian      = BayesianTracker()
    lmsr_qty: list[float] = [0.0, 0.0]
    timer         = MarketTimer(market_end_ts=m_end)
    atomic_executor = AtomicArbExecutor(
        place_order_fn=place_live_order_arb,        # zero slippage — arb exacto
        place_market_close_fn=place_market_close,   # +5c rollback
    )
    mart_level: int = tsm.state.mart_level

    log_sep2()
    _k_val = binance_state.cycle_open_price
    _k_str = f"{_k_val:.5f}" if _k_val is not None else "n/a"
    log_info(
        f"NOVO CICLO | {meta['slug']} | LIVE={LIVE_TRADING} | DRY_RUN={DRY_RUN} | "
        f"UP: XRP > K={_k_str} | DOWN: XRP < K={_k_str}"
    )
    log_info(
        f"Banca: ${bankroll:.4f} | PnL_dia: {fmt_dollar(tsm.state.daily_pnl)} | "
        f"Mart: x{mart_level} | Binance: {'OK' if not binance_state.is_stale() else 'STALE'} "
        f"K={_k_str}"
    )
    log_info(
        f"BAYESIAN+LMSR | prior={BAYESIAN_PRIOR} | std={BAYESIAN_LIKELIHOOD_STD} | "
        f"decay={BAYESIAN_DECAY} | min_edge={BAYESIAN_MIN_EDGE} | BLEND_W={BINANCE_BLEND_WEIGHT}"
    )
    log_sep()

    # ── Inner helpers ─────────────────────────────────────────────────────────

    async def open_trade(
        side: str, trade_type: str, rstr: str, risk: float,
        extra_log: Optional[str]  = None,
        fixed_shares: Optional[float] = None,
        token_id: Optional[str]  = None,
    ) -> Optional[dict]:
        nonlocal bankroll
        ask = best_asks.get(side.lower())
        bid = best_bids.get(side.lower())
        if ask is None or ask <= 0.0: return None
        current_exposure = sum(t["total_out"] for t in active_trades)
        max_exp = bankroll * _F_MAX_MARKET_EXPOSURE
        if round(current_exposure, 6) >= round(max_exp, 6): return None

        if fixed_shares is not None:
            shares = fixed_shares; invested_pure = shares * ask
        else:
            base_risk_amount = bankroll * risk
            recovery_stake   = tsm.calc_next_stake(base_risk_amount, ask)
            invested_pure    = min(recovery_stake, bankroll * risk * float(mart_level))
            shares           = invested_pure / ask

        max_per = bankroll * min(
            _F_KELLY_MAX_RISK_PCT * mart_level,
            _F_KELLY_MAX_RISK_PCT * MART_MAX_MULT,
        )
        if invested_pure > max_per and fixed_shares is None:
            invested_pure = max_per; shares = invested_pure / ask

        fee_buy   = fee_rate_lut(ask) * invested_pure
        total_out = invested_pure + fee_buy

        if round(current_exposure + total_out, 6) > round(max_exp, 6):
            room = max_exp - current_exposure
            if room <= 0.001: return None
            total_out     = room
            fee_buy       = total_out * fee_rate_lut(ask) / (1.0 + fee_rate_lut(ask))
            invested_pure = total_out - fee_buy
            shares        = invested_pure / ask

        bankroll -= total_out
        trade: dict = {
            "side": side, "ask": ask, "bid_at_buy": bid,
            "eff_c": eff_price_c_f(ask), "shares": shares,
            "target": None, "type": trade_type,
            "invested_pure": invested_pure,
            "fee_buy": fee_buy, "total_out": total_out, "token_id": token_id,
        }
        active_trades.append(trade)

        if LIVE_TRADING and token_id:
            await place_live_order(side, ask, shares, token_id)

        idx = 0 if side == "UP" else 1; lmsr_qty[idx] += shares
        p_up, p_dn = bayesian.get_posteriors()
        p_hat = p_up if side == "UP" else p_dn
        ev    = calc_ev_bayesian(p_hat, ask) if trade_type == "GAMBLING" else 0.0
        bid_s = f" | BID@buy={fc(bid)}" if bid else ""
        ext_s = f" | {extra_log}" if extra_log else ""

        log_m(
            trade_type, "BUY",
            f"rem={rstr} | {side} @ ASK={fc(ask)} eff={fc(eff_price_c_f(ask)/100.0)}{bid_s}"
            f" | invested={fmt_dollar(invested_pure)} | fee={fmt_fee(fee_buy, invested_pure)}"
            f" | total={fmt_dollar(total_out)} | shares={shares:.4f}"
            f" | risk={risk:.1%}{ext_s} | EV={ev:+.4f} | p_hat={p_hat:.3f}",
        )
        return trade

    def close_trade(trade: dict, sell_bid: float, reason: str, rstr: str) -> float:
        nonlocal bankroll
        payout_bruto = trade["shares"] * sell_bid
        fee_sell     = payout_bruto * fee_rate_lut(sell_bid)
        payout_net   = payout_bruto - fee_sell
        pnl          = payout_net - trade["total_out"]
        pnl_pct      = (pnl / trade["total_out"] * 100.0) if trade["total_out"] else 0.0
        bankroll     += payout_net
        sign = "(+)" if pnl >= 0 else "(-)"
        log_m(
            trade["type"], "SELL",
            f"rem={rstr} | {trade['side']} @ BID={fc(sell_bid)} "
            f"| bruto={fmt_dollar(payout_bruto)} | fee_sell={fmt_fee(fee_sell, payout_bruto or 1.0)} "
            f"| net={fmt_dollar(payout_net)} "
            f"| PnL: {fmt_dollar(pnl)} ({fmt_pct(pnl_pct)}) {sign} | Reason: {reason}",
        )
        return pnl

    def close_trade_resolution(trade: dict, winner: bool, rstr: str) -> float:
        nonlocal bankroll
        payout_net = resolution_payout(trade["shares"], winner)
        pnl        = payout_net - trade["total_out"]
        pnl_pct    = (pnl / trade["total_out"] * 100.0) if trade["total_out"] else 0.0
        bankroll  += payout_net
        reason_s = "RESOLUCAO GANHA ($1/share)" if winner else "RESOLUCAO PERDIDA (Total)"
        price_s  = "100.0c" if winner else "0.0c"
        sign     = "(+)" if pnl >= 0 else "(-)"
        log_m(
            trade["type"], "SELL",
            f"rem={rstr} | {trade['side']} @ {price_s} "
            f"| net={fmt_dollar(payout_net)} "
            f"| PnL: {fmt_dollar(pnl)} ({fmt_pct(pnl_pct)}) {sign} | Reason: {reason_s}",
        )
        return pnl

    # ── Inicializa filtros HFT ────────────────────────────────────────────────
    kalmans: dict[str, KalmanFilter1D] = {"UP": KalmanFilter1D(), "DOWN": KalmanFilter1D()}
    hft_wins: dict[str, HFTWindow]     = {"UP": HFTWindow(),      "DOWN": HFTWindow()}
    vpin_trackers: dict[str, VPINTracker] = {"UP": VPINTracker(), "DOWN": VPINTracker()}

    gamb_last_buy: dict[str, float]    = {"UP": 0.0, "DOWN": 0.0}
    gamb_started_logged: bool  = False
    endgame_fired: bool        = False
    pa_count: int              = 0
    last_pa_time: float        = 0.0
    prev_bid_up: Optional[float]   = None
    prev_bid_down: Optional[float] = None
    halt_peg_arbit: bool       = False

    # ── Main tick loop ────────────────────────────────────────────────────────
    while not _shutdown_flag:
        now = time.time()

        # Expiração → settlement
        if timer.is_expired:
            final_ask_up   = best_asks.get("up")   or 0.0
            final_ask_down = best_asks.get("down")  or 0.0
            local_winner   = "UP" if final_ask_up >= final_ask_down else "DOWN"
            winner_token   = meta["up"] if local_winner == "UP" else meta["down"]
            log_sep()
            p_up, p_dn = bayesian.get_posteriors()
            log_info(
                f"FIM DE MERCADO | UP ASK={fc(final_ask_up)} DN ASK={fc(final_ask_down)} | "
                f"WINNER: {local_winner} | Bayesian: P(UP)={p_up:.3f} P(DN)={p_dn:.3f} | "
                f"ticks={bayesian.tick_count} | Mart: x{mart_level}"
            )
            if active_trades:
                n_settled = len(active_trades)
                rstr = timer.remaining_str()
                for trade in list(active_trades):
                    close_trade_resolution(trade, trade.get("token_id") == winner_token, rstr)
                active_trades.clear()
                log_info(f"SETTLEMENT DONE | Banca: ${bankroll:.4f} | trades={n_settled}")
            log_sep()
            return bankroll

        # Aguardar evento de preço ou timeout
        try:
            await asyncio.wait_for(price_change.wait(), timeout=LOOP_SLEEP)
            price_change.clear()
        except asyncio.TimeoutError:
            pass

        bid_up   = best_bids.get("up");   bid_down  = best_bids.get("down")
        ask_up   = best_asks.get("up");   ask_down  = best_asks.get("down")
        if None in (bid_up, bid_down, ask_up, ask_down): continue
        if bid_up == prev_bid_up and bid_down == prev_bid_down: continue
        prev_bid_up, prev_bid_down = bid_up, bid_down

        bid_up_f:   float = bid_up    # type: ignore[assignment]
        bid_down_f: float = bid_down   # type: ignore[assignment]
        ask_up_f:   float = ask_up    # type: ignore[assignment]
        ask_down_f: float = ask_down   # type: ignore[assignment]

        ask_sum    = ask_up_f + ask_down_f
        mid_up     = (bid_up_f  + ask_up_f)   * 0.5
        mid_down   = (bid_down_f + ask_down_f) * 0.5
        ask_up_c   = ask_up_f   * 100.0
        ask_down_c = ask_down_f * 100.0
        rstr       = timer.remaining_str()

        # Kalman
        kal_up   = kalmans["UP"].update(mid_up)
        kal_down = kalmans["DOWN"].update(mid_down)
        hft_wins["UP"].add(kal_up, now);   hft_wins["DOWN"].add(kal_down, now)
        z_up    = hft_wins["UP"].zscore(kal_up)
        z_down  = hft_wins["DOWN"].zscore(kal_down)
        std_up  = hft_wins["UP"].std()
        std_down = hft_wins["DOWN"].std()

        # Order book metrics
        bs_up   = best_bid_sizes.get("up");   as_up   = best_ask_sizes.get("up")
        bs_down = best_bid_sizes.get("down");  as_down = best_ask_sizes.get("down")
        obi_up   = calc_imbalance(bs_up,   as_up)
        obi_down = calc_imbalance(bs_down, as_down)
        vol_up   = ((bs_up   or 0) + (as_up   or 0)) or 1.0
        vol_down = ((bs_down or 0) + (as_down or 0)) or 1.0
        vpin_trackers["UP"].add(kal_up,   vol_up,   now)
        vpin_trackers["DOWN"].add(kal_down, vol_down, now)
        vpin_up   = vpin_trackers["UP"].vpin()
        vpin_down = vpin_trackers["DOWN"].vpin()

        # Bayesian + LMSR
        p_hat_up, p_hat_down = bayesian.update(
            kal_up, kal_down, obi_up, obi_down, vpin_up, vpin_down
        )
        lmsr_ineff = lmsr_pricer.inefficiency(
            [p_hat_up, p_hat_down], [ask_up_f, ask_down_f]
        )

        # ── BINANCE ORACLE BLEND ─────────────────────────────────────────────
        # Funde sinal Binance (Black-Scholes digital) com Bayesian.
        # Só aplica se Binance não estiver stale e strike estiver definido.
        _bnc_active = (
            not binance_state.is_stale(10.0)
            and binance_state.current_price is not None
            and binance_state.cycle_open_price is not None
        )
        if _bnc_active:
            _bnc_p = calculate_true_prob(
                current_price=binance_state.current_price,
                strike_price=binance_state.cycle_open_price,
                seconds_to_close=timer.remaining,
            )
            if _bnc_p is not None:
                w = BINANCE_BLEND_WEIGHT
                p_hat_up_blend   = w * _bnc_p         + (1.0 - w) * p_hat_up
                p_hat_down_blend = w * (1.0 - _bnc_p) + (1.0 - w) * p_hat_down
                # Re-normalizar para somar 1
                _s = p_hat_up_blend + p_hat_down_blend
                if _s > 1e-9:
                    p_hat_up   = max(0.01, min(0.99, p_hat_up_blend   / _s))
                    p_hat_down = max(0.01, min(0.99, p_hat_down_blend  / _s))
                    # Re-calcular LMSR com p_hat blendados
                    lmsr_ineff = lmsr_pricer.inefficiency(
                        [p_hat_up, p_hat_down], [ask_up_f, ask_down_f]
                    )

        # Tick log
        _z_u  = f"{z_up:+.2f}"    if z_up    is not None else "n/a"
        _z_d  = f"{z_down:+.2f}"  if z_down   is not None else "n/a"
        _s_u  = f"{std_up:.4f}"   if std_up   is not None else "n/a"
        _s_d  = f"{std_down:.4f}" if std_down  is not None else "n/a"
        _o_u  = f"{obi_up:.2f}"   if obi_up   is not None else "n/a"
        _o_d  = f"{obi_down:.2f}" if obi_down  is not None else "n/a"
        _v_u  = f"{vpin_up:.2f}"  if vpin_up   is not None else "n/a"
        _v_d  = f"{vpin_down:.2f}" if vpin_down is not None else "n/a"
        _bnc_s = f"BNC={binance_state.current_price:.5f}" if binance_state.current_price else "BNC=n/a"
        log_raw(
            f"rem={rstr} | "
            f"UP BID={fc(bid_up_f)} ASK={fc(ask_up_f)} Z={_z_u} σ={_s_u} OBI={_o_u} VPIN={_v_u} | "
            f"DN BID={fc(bid_down_f)} ASK={fc(ask_down_f)} Z={_z_d} σ={_s_d} OBI={_o_d} VPIN={_v_d} | "
            f"BAYES P(UP)={p_hat_up:.3f} P(DN)={p_hat_down:.3f} | {_bnc_s} | "
            f"LMSR ineff=[{lmsr_ineff[0]:+.4f},{lmsr_ineff[1]:+.4f}] | PEG={ask_sum:.4f}"
        )

        # ── AGGRESSIVE ENDGAME ────────────────────────────────────────────────
        if AGGRESSIVE_ENDGAME_ACTIVE and timer.is_endgame() and not endgame_fired and bankroll > 0.0:
            _eg_candidates = []
            for _s, _t, _a in [("UP", meta["up"], ask_up_f), ("DOWN", meta["down"], ask_down_f)]:
                if _a and (
                    round(_a, 4) >= round(_F_AGGRESSIVE_ENDGAME_MIN, 4)
                    and round(_a, 4) <= round(_F_AGGRESSIVE_ENDGAME_MAX, 4)
                ):
                    _eg_candidates.append((_s, _t, _a))
            if _eg_candidates:
                endgame_fired = True
                _eg_side, _eg_tid, _ = max(_eg_candidates, key=lambda x: x[2])
                _eg_risk = min(_F_AGGRESSIVE_ENDGAME_RISK * mart_level,
                               _F_KELLY_MAX_RISK_PCT * mart_level)
                await open_trade(
                    _eg_side, "ENDGAME_AGG", rstr, risk=_eg_risk,
                    token_id=_eg_tid,
                    extra_log=(
                        f"ENDGAME x{mart_level} | "
                        f"acc_loss={tsm.state.accumulated_loss_session:.4f} | recovery_aware"
                    ),
                )

        # ── PEG ARBIT ─────────────────────────────────────────────────────────
        if (
            PEG_ARBIT_ACTIVE and not halt_peg_arbit and timer.can_peg_arbit()
            and round(ask_sum, 4) <= round(_F_PA_TRIGGER_SUM, 4)
            and pa_count < MAX_PA_ENTRIES
            and now - last_pa_time >= PA_COOLDOWN
        ):
            budget    = bankroll * eff_pa_risk
            _sz_up    = best_ask_sizes.get("up")
            _sz_dn    = best_ask_sizes.get("down")
            _ob_up = OrderBookSide(
                [OrderBookLevel(price=ask_up_f, size=_sz_up)]
                if ask_up_f > 0 and _sz_up and _sz_up > 0 else []
            )
            _ob_dn = OrderBookSide(
                [OrderBookLevel(price=ask_down_f, size=_sz_dn)]
                if ask_down_f > 0 and _sz_dn and _sz_dn > 0 else []
            )
            arb = evaluate_arb(_ob_up, _ob_dn, budget=budget, peg_trigger=PA_TRIGGER_SUM)

            if arb.status == ArbStatus.OPPORTUNITY:
                log_sep()
                log_m(
                    "PEG ARBIT", "OPORTUNIDADE DETETADA",
                    f"rem={rstr} | "
                    f"Ask_UP={fc(arb.lowest_ask_up)} Ask_DN={fc(arb.lowest_ask_down)} | "
                    f"Peg={arb.peg:.4f} | Margem={arb.gross_margin:.4f} ({arb.gross_margin*100:.1f}c) | "
                    f"shares={arb.shares:.4f} | cost=${arb.total_cost:.4f} | "
                    f"payout=${arb.payout:.4f} | profit=${arb.net_profit:.4f} ({arb.profit_pct:+.2f}%) | "
                    f"VWAP={arb.used_vwap} | vol_up={arb.volume_at_ask_up:.1f} "
                    f"vol_dn={arb.volume_at_ask_down:.1f} | #{pa_count+1}",
                )
                exec_result = await atomic_executor.execute_atomic(arb, meta, _ob_up, _ob_dn)

                if exec_result.status == AtomicExecStatus.SUCCESS:
                    log_m(
                        "PEG ARBIT", "ATOMIC_SUCCESS",
                        f"Both legs filled in {exec_result.execution_time_ms:.0f}ms | "
                        f"shares={arb.shares:.4f} | profit=${arb.net_profit:.4f}",
                    )
                    for sn, tid, av in [
                        ("UP",   meta["up"],   arb.lowest_ask_up),
                        ("DOWN", meta["down"], arb.lowest_ask_down),
                    ]:
                        fb  = fee_rate_lut(av) * arb.shares * av
                        tot = arb.shares * av + fb
                        bankroll -= tot
                        active_trades.append({
                            "side": sn, "ask": av,
                            "bid_at_buy": best_bids.get(sn.lower()),
                            "eff_c": eff_price_c_f(av), "shares": arb.shares,
                            "target": None, "type": "PEG ARBIT",
                            "invested_pure": arb.shares * av,
                            "fee_buy": fb, "total_out": tot, "token_id": tid,
                        })
                    log_sep(); pa_count += 1; last_pa_time = now

                elif exec_result.status == AtomicExecStatus.PARTIAL_FILL_FAILED:
                    log_m("PEG ARBIT", "HALT",
                          f"Partial fill unrecoverable — HALTING | {exec_result.error_message}")
                    halt_peg_arbit = True; log_sep()
                else:
                    log_m("PEG ARBIT", exec_result.status.value,
                          f"rem={rstr} | {exec_result.error_message}")
                    log_sep()

            elif arb.status != ArbStatus.REJECT_PEG_TOO_HIGH:
                log_m("PEG ARBIT", arb.status.value, f"rem={rstr} | {arb.reason}")

        # ── GAMBLING ──────────────────────────────────────────────────────────
        if GAMBLING_ACTIVE and timer.can_gambling_enter():
            if not gamb_started_logged:
                gamb_started_logged = True
                log_m("GAMBLING", "START",
                      f"rem={rstr} | Bayesian+LMSR+Binance | Mart x{mart_level} | both_sides=ON")

            _eff_zl = ENDGAME_ZSCORE_LIMIT if timer.is_endgame() else GAMB_MAX_ZSCORE
            _eff_vl = ENDGAME_VPIN_LIMIT   if timer.is_endgame() else VPIN_SAFE_LIMIT

            for (g_side, g_ask, g_bid, g_ask_c, g_z, g_std,
                 g_obi, g_vpin, g_p_hat, g_lmsr_ineff) in (
                ("UP",   ask_up_f,   bid_up_f,   ask_up_c,   z_up,   std_up,
                 obi_up,   vpin_up,   p_hat_up,   lmsr_ineff[0]),
                ("DOWN", ask_down_f, bid_down_f, ask_down_c, z_down, std_down,
                 obi_down, vpin_down, p_hat_down, lmsr_ineff[1]),
            ):
                if now - gamb_last_buy[g_side] < GAMB_BUY_COOLDOWN: continue
                spread_c = best_spreads_c.get(g_side.lower())
                if spread_c is None or spread_c > _F_MAX_SPREAD_CENTS: continue
                if not (round(g_ask_c, 4) >= round(_F_GAMB_MIN_ASK_C, 4)
                        and round(g_ask_c, 4) <= round(_F_GAMB_MAX_ASK_C, 4)): continue
                if g_bid and g_ask > 0 and round(g_bid/g_ask, 4) < round(_F_BID_ASK_MIN_RATIO, 4): continue
                if g_std is None or g_std > GAMB_MAX_VOL_DEV: continue
                if g_z   is None or g_z   > _eff_zl: continue
                if g_obi  is not None and g_obi  < GAMB_MIN_OBI: continue
                if g_vpin is not None and g_vpin > _eff_vl: continue

                bayes_edge = g_p_hat - g_ask
                bayes_ev   = calc_ev_bayesian(g_p_hat, g_ask)

                if bayesian.tick_count >= BAYESIAN_MIN_TICKS:
                    if bayes_edge < BAYESIAN_MIN_EDGE:
                        log_m("GAMBLING", "BLOCK_BAYES",
                              f"rem={rstr} | {g_side} | p_hat={g_p_hat:.3f} ask={fc(g_ask)} "
                              f"edge={bayes_edge:+.4f}<{BAYESIAN_MIN_EDGE} | EV={bayes_ev:+.4f}")
                        continue
                    if g_lmsr_ineff < LMSR_INEFF_THRESHOLD:
                        log_m("GAMBLING", "BLOCK_LMSR",
                              f"rem={rstr} | {g_side} | ineff={g_lmsr_ineff:+.4f}<{LMSR_INEFF_THRESHOLD}")
                        continue
                    kelly_risk = calc_kelly_bayesian(g_p_hat, g_ask, mart_level)
                else:
                    kelly_risk = calc_kelly_risk(g_ask, mart_level)
                    bayes_ev   = calc_ev_static(g_ask)

                if kelly_risk <= 0.0: continue
                if bankroll > 0.0:
                    _obi_s    = f"{g_obi:.2f}"       if g_obi   is not None else "n/a"
                    _vpin_s   = f"{g_vpin:.2f}"      if g_vpin  is not None else "n/a"
                    _ba_s     = f"{g_bid/g_ask:.3f}" if g_bid and g_ask > 0 else "n/a"
                    _eg_tag   = " | ENDGAME" if timer.is_endgame() else ""
                    _bt_tag   = "BAYESIAN+BNC" if _bnc_active else (
                                "BAYESIAN" if bayesian.tick_count >= BAYESIAN_MIN_TICKS else "STATIC")
                    token_id  = meta["up"] if g_side == "UP" else meta["down"]
                    await open_trade(
                        g_side, "GAMBLING", rstr, risk=kelly_risk, token_id=token_id,
                        extra_log=(
                            f"Kelly={kelly_risk:.1%}({_bt_tag} "
                            f"p_hat={g_p_hat:.3f} edge={bayes_edge:+.3f} ×Mart_x{mart_level}) "
                            f"σ={g_std:.4f} Z={g_z:+.2f} OBI={_obi_s} VPIN={_vpin_s} "
                            f"BidAsk={_ba_s} LMSR_ineff={g_lmsr_ineff:+.4f}{_eg_tag}"
                        ),
                    )
                    gamb_last_buy[g_side] = now

    return bankroll


###############################################################################
#                                                                             #
#   ⑩ MAIN — orquestração completa de todas as tarefas                       #
#                                                                             #
###############################################################################

async def main() -> None:
    """Orquestra todos os workers assíncronos e cicla mercados XRP de 5 min.

    Arquitectura de tasks:
        bnc_task      — Binance ticker WS (persiste entre ciclos)
        hb_task       — CLOB heartbeat    (persiste entre ciclos, só se LIVE)
        ws_task       — Polymarket market WS (recria em cada ciclo)
        user_ws_task  — Polymarket user WS   (recria em cada ciclo, só se LIVE)

    Formato de logs de fim de ronda:
        [INFO] [dd/mm/yy | HH:MM:SS.mmm] | ROUND | PnL: $+X.XXXX (+X.XX%) | Mart: xN
        [INFO] [dd/mm/yy | HH:MM:SS.mmm] | TOTAL | PnL_dia: $+X.XXXX (+X.XX%) | Banca: $XX.XXXX | Up_Time: XXh:XXm:XXs
    """
    global _shutdown_flag, resolved_event, resolved_winner_asset

    # ── Signal handlers ───────────────────────────────────────────────────────
    def _handle_signal() -> None:
        global _shutdown_flag
        _shutdown_flag = True
        try: tsm.save()
        except Exception: pass
        log_info("SIGNAL recebido — graceful shutdown iniciado")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except (NotImplementedError, OSError):
            pass  # Windows fallback via KeyboardInterrupt

    # ── Boot ─────────────────────────────────────────────────────────────────
    tsm.load()

    if not _HAS_ORJSON:
        log_warn("orjson não instalado — pip install orjson (stdlib json em fallback)")

    log_sep2()
    log_info("BOT XRP POLYMARKET v6.0.0 — UNIFIED PRODUCTION")
    log_info(
        f"LIVE={LIVE_TRADING} | DRY_RUN={DRY_RUN} | "
        f"Bankroll=${tsm.state.bankroll:.4f} | "
        f"Initial=${tsm.state.initial_bankroll:.4f} | "
        f"Mart=x{tsm.state.mart_level}"
    )
    log_info(
        f"Fee LUT: {len(_FEE_LUT)} entries O(1) | ARB_PEG_TRIGGER={ARB_PEG_TRIGGER} | "
        f"Binance blend={BINANCE_BLEND_WEIGHT:.0%}"
    )
    log_sep2()

    # ── Tasks persistentes (vivem durante toda a execução do bot) ─────────────
    bnc_task = asyncio.create_task(binance_ticker_loop(), name="binance_oracle")

    hb_task: Optional[asyncio.Task] = None
    if LIVE_TRADING and clob_client is not None:
        hb_task = asyncio.create_task(heartbeat_loop(clob_client), name="heartbeat")

    # ── Main cycle loop ───────────────────────────────────────────────────────
    while not _shutdown_flag:
        slug, start_ts = get_current_slug()
        meta = await fetch_metadata(slug)
        if not meta:
            await asyncio.sleep(2)
            continue

        resolved_event.clear()
        resolved_winner_asset = None
        market_day = datetime.fromtimestamp(start_ts).date().isoformat()

        if tsm.state.last_market_day != market_day:
            tsm.reset_daily(market_day)
            if LIVE_TRADING:
                lb = await fetch_live_bankroll()
                if lb is not None:
                    tsm.update_bankroll(lb)
            log_info(
                f"NEW DAY {market_day} | "
                f"Banca: ${tsm.state.bankroll:.4f} | "
                f"Mart: x{tsm.state.mart_level} (PERSISTED)"
            )
            await tsm.save_async()

        # Reset dados de mercado do ciclo anterior
        for k in ("up", "down"):
            best_bids[k] = best_asks[k] = best_spreads_c[k] = None
            best_bid_sizes[k] = best_ask_sizes[k] = None
        price_change.clear()

        # Verificar websockets disponível
        try:
            import websockets as _ws_check  # noqa: F401
        except ImportError:
            log_warn("websockets não instalado — pip install websockets")
            await asyncio.sleep(5)
            continue

        # Fixar strike Binance para este ciclo
        # Aguarda até 3 s por um tick Binance antes de prosseguir
        _strike_attempts = 0
        while binance_state.current_price is None and _strike_attempts < 30:
            await asyncio.sleep(0.1)
            _strike_attempts += 1
        set_cycle_strike()

        # Tasks por ciclo
        ws_task = asyncio.create_task(
            ws_handler(meta["up"], meta["down"]),
            name="polymarket_ws",
        )

        user_ws_task: Optional[asyncio.Task] = None
        if LIVE_TRADING and POLYMARKET_API_KEY:
            user_ws_task = asyncio.create_task(
                user_ws_loop(
                    api_key=POLYMARKET_API_KEY,
                    secret=POLYMARKET_SECRET,
                    passphrase=POLYMARKET_PASSPHRASE,
                    condition_id=meta["id"],
                ),
                name="user_ws",
            )

        # Aguarda primeiro tick de preço Polymarket (max 2 s)
        await asyncio.sleep(1.0)

        if best_bids["up"] is not None:
            pre_bank: float = tsm.state.bankroll

            final_bank: float = await logic_loop(start_ts, start_ts + 300, meta)
            if final_bank is None:  # type: ignore[comparison-overlap]
                final_bank = pre_bank

            round_pnl: float = final_bank - pre_bank

            tsm.update_bankroll(final_bank)
            tsm.update_martingale(round_pnl)
            tsm.update_daily_pnl(round_pnl)
            await tsm.save_async()

            pnl_pct: float       = (round_pnl / pre_bank * 100.0) if pre_bank > 1e-9 else 0.0
            pnl_total_pct: float = tsm.pnl_total_inicio_pct()

            log_sep2()
            log_info(
                f"ROUND | PnL: {fmt_dollar(round_pnl)} ({fmt_pct(pnl_pct)}) | "
                f"Mart: x{tsm.state.mart_level}"
            )
            log_info(
                f"TOTAL | PnL_dia: {fmt_dollar(tsm.state.daily_pnl)} ({fmt_pct(pnl_total_pct)}) | "
                f"Banca: ${tsm.state.bankroll:.4f} | "
                f"Up_Time: {get_uptime_str()}"
            )
            log_sep2()

        # Fechar tasks de ciclo
        ws_task.cancel()
        if user_ws_task is not None:
            user_ws_task.cancel()

        for t in filter(None, [ws_task, user_ws_task]):
            try:
                await t
            except asyncio.CancelledError:
                pass

        await asyncio.sleep(0.5)

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    bnc_task.cancel()
    if hb_task is not None:
        hb_task.cancel()
    for t in filter(None, [bnc_task, hb_task]):
        try:
            await t
        except asyncio.CancelledError:
            pass

    tsm.save()
    log_info(
        f"SHUTDOWN COMPLETO | "
        f"Banca: ${tsm.state.bankroll:.4f} | "
        f"PnL_Total: {fmt_pct(tsm.pnl_total_inicio_pct())} | "
        f"Up_Time: {get_uptime_str()}"
    )


###############################################################################
#                                                                             #
#   ENTRY POINT                                                               #
#                                                                             #
###############################################################################

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        tsm.save()
        log_info("STOPPED (Ctrl+C) — estado guardado")