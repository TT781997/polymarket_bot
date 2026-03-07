# =============================================================================
# BOT XRP POLYMARKET — v0.36.1 (Definitivo)
# =============================================================================
# CHANGELOG v0.36.1:
# - Fix: Settlement de Fim de Mercado agora avalia vencedor por BID e liquida
#   a exatos $1.00 (sem fee) ou $0.00 (perda total).
# - Fix: Martingale recovery_rounds com piso rígido de 1 para evitar crash de
#   divisão por zero (que forçava o risco a 50% incorretamente).
# - Refactor: Volatilidade do módulo Eighty sincronizada em Macro (5.0s spread)
#   e Micro (Pump 1.5s vs Exaustão 3.0s). Lookbacks limpos (1s, 2s, 3s).
# - Remoção global de emojis nos logs.
# - Correção crítica ASK vs BID mantida.
# =============================================================================

import asyncio
import websockets
import json
import time
import logging
import requests
import os
import math
import numpy as np
from datetime import datetime
from collections import deque

# =============================================================================
# PARÂMETROS CONFIGURÁVEIS (COM RANGES ABSOLUTOS E EXPLICAÇÕES)
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 0 — MODO DE OPERAÇÃO
# ─────────────────────────────────────────────────────────────────────────────
LIVE_TRADING = False  # Range: [True | False] | True = Executa ordens reais, False = Simulação completa

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 1 — BANCA
# ─────────────────────────────────────────────────────────────────────────────
BANKROLL_INIT = 10.0  # Range: [10.0 ... ∞] | Banca inicial em USDC. Em Demo, esta banca é persistente.

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 2 — RISCO BASE POR MÓDULO
# ─────────────────────────────────────────────────────────────────────────────
RISK_PER_TRADE = 0.05  # Range: [0.01 ... 0.20] | Fração de risco base para ciclos e ordens genéricas.
EIGHTY_RISK = 0.15     # Range: [0.01 ... 0.15] | Fração de risco base específica para o módulo EIGHTY.
PEG_ARBIT_RISK = 0.25  # Range: [0.05 ... 0.30] | Fração de risco base dedicada à Arbitragem PEG.

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 3 — MARTINGALE E RECOVERY
# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 3.0 — MARTINGALE CONDICIONAL E RECUPERAÇÃO SUAVE
# ─────────────────────────────────────────────────────────────────────────────
MAX_RISK_MULTIPLIER = 32         # Range: [2 ... 32] | Limite máximo do multiplicador (x2, x4, x8, x16, x32).
RECOVERY_ROUNDS_BASE = 10        # Range: [5 ... 20] | Rondas iniciais de recuperação por cada loss.
MAX_RISK_PERCENT = 0.15          # Range: [0.10 ... 0.20] | CAP RÍGIDO: Risco efetivo total máximo = 15% da banca.
# Fórmula: min(base * martingale_mult + (accumulated_loss / recovery_rounds / bankroll), 0.15)

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 3.1 — STOP-LOSS E BUCKETING
# ─────────────────────────────────────────────────────────────────────────────
STOPLOSS_PRICE_C = 27.0      # Range: [10.0 ... 80.0] | BID efetivo mínimo antes de ativar stop-loss check.
STOPLOSS_TICKS = 5           # Range: [1 ... 20] | Níveis estruturais de descida para confirmar flash-crash.
STOPLOSS_PRICE_STEP_C = 1.0  # Range: [0.1 ... 5.0] | Tamanho do tick (degrau) de stop-loss.

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 4 — TOGGLES DE MÓDULOS
# ─────────────────────────────────────────────────────────────────────────────
CICLO_30S_ACTIVE = False  # Range: [True | False] | Estratégia de snapshot com lookback de 30s.
CICLO_20S_ACTIVE = False  # Range: [True | False] | Estratégia de snapshot com lookback de 20s.
EIGHTY_ACTIVE = True      # Range: [True | False] | Compra direcional por consolidação de tick buffers.
PEG_ARBIT_ACTIVE = True   # Range: [True | False] | Arbitragem direcional inversa garantindo hedge.
KELLY_ACTIVE = False      # Range: [True | False] | Empirical Kelly com validação Monte Carlo.
AS_VPIN_ACTIVE = False    # Range: [True | False] | Bloqueador de trades baseado em toxidade do Order Book.

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 5 — CICLOS (30s e 20s)
# ─────────────────────────────────────────────────────────────────────────────
CYCLE_PRICE_MIN_C = 74.0   # Range: [50.0 ... 80.0] | Preço nominal mínimo para engatilhar um ciclo.
CYCLE_PRICE_MAX_C = 85.0   # Range: [80.0 ... 99.0] | Preço nominal máximo para evitar retornos diluídos.
CYCLE_PEG_MIN_C = 96.5     # Range: [90.0 ... 99.0] | Equilíbrio mínimo transversal do mercado.
CYCLE_VOL_MAX_C = 52.0     # Range: [10.0 ... 80.0] | Teto de oscilação permitida no lookback.
CYCLE_TARGET_C = 0.0       # Range: [0.0 ... 99.0] | Alvo de venda preemptiva (0.0 = desligado).

CYCLE_30S_SNAPSHOT_REM  = 35.0
CYCLE_30S_VOL_CHECK_REM = 30.0
CYCLE_30S_BUY_REM       = 29.8

CYCLE_20S_SNAPSHOT_REM  = 25.0  
CYCLE_20S_VOL_CHECK_REM = 20.0
CYCLE_20S_BUY_REM       = 19.8

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 6 — EIGHTY (Sincronizado)
# ─────────────────────────────────────────────────────────────────────────────
EIGHTY_START_REM_S = 300       # Range: [60.0 ... 300.0] | Janela inicial do EIGHTY.
EIGHTY_MIN_EFF_C = 80.0        # Range: [80.0 ... 90.0] | Entry mínimo (Preço efetivo).
EIGHTY_MAX_EFF_C = 98        # Range: [95.0 ... 99.9] | Teto de liquidez para entry EIGHTY.
EIGHTY_MIN_TICKS = 5           # Range: [3 ... 10] | Mínimo de níveis de preço distintos (passos discretos).
EIGHTY_CUTOFF_S = 5            # Range: [0 ... 20] | Congela entradas faltam X segundos.
EIGHTY_WHEN_CUTOFF_0_VOLT = 35.0 # Range: [0.0 ... 60.0] | Ignora vol nos últimos Xs se cutoff=0.
EIGHTY_PEG_MIN_C = 97.0        # Range: [90.0 ... 99.0] | Teto de estabilidade U/D específico.
EIGHTY_BUY_COOLDOWN = 4.0      # Range: [1.0 ... 10.0] | Cool-off obrigatório entre entradas do mesmo lado.
EIGHTY_PRICE_STEP_C = 0.5    # Range: [0.1 ... 2.0] | Tamanho do tick no EIGHTY (arredondamento subida).

# 1. MACRO VOLATILIDADE (Ruído Geral)
EIGHTY_VOL_WINDOW_S = 5.0      # Range: [3.0 ... 10.0] | Janela longa para spread de volatilidade.
EIGHTY_VOL_MAX_C = 4.5         # Range: [2.0 ... 10.0] | Spread máximo permitido (High - Low).
EIGHTY_VOL_COOLDOWN_S = 5.0    # Range: [3.0 ... 10.0] | Tempo de castigo após macro volatilidade.

# 2. MICRO VOLATILIDADE E DELTAS (Proteção Anti-Pump e Exaustão)
EIGHTY_DELTA_LOOKBACK_S = 5.0  # Range: [3.0 ... 10.0] | Buffer size.
EIGHTY_DELTA_INTERVALS = [1.0, 2.0, 3.0] # Range: List[float] | Lookbacks de delta graduais.
EIGHTY_DELTA_VOL_TIME_S = 1.5  # Range: [1.0 ... 3.0] | Janela curta para detecção de pump.
EIGHTY_DELTA_VOL_RISE_C = 2.0  # Range: [1.0 ... 3.0] | Pump falso (subida rápida instável).
EIGHTY_DELTA_MAX_RISE_C = 3.5  # Range: [2.0 ... 5.0] | Exaustão (subida longa demasiada esticada).

EIGHTY_TARGET_C = 0.0          # Range: [0.0 ... 99.0] | Exit percentual estático (0.0 = natural).

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 7 — PEG ARBITRAGE (NOVO SISTEMA)
# ─────────────────────────────────────────────────────────────────────────────
PEG_ARBIT_EFF_THRESHOLD = 0.985   # Range: [0.01 ... 0.99] | Soma de asks+fees: eff_up + eff_down <= 98.5c para ativar.
PEG_ARBIT_RANGE_1 = (0.0, 45.0)   # Range: [0-45c] | Primeiro range válido (fechado).
PEG_ARBIT_RANGE_2 = (55.0, 99.9)  # Range: [55-99.9c] | Segundo range válido (fechado).
PEG_ARBIT_BANCA_PCT = 0.25        # Range: [0.10 ... 0.50] | Percentagem fixa de banca por entrada (25%).
PEG_ARBIT_COOLDOWN = 0.05         # Range: [0.01 ... 1.0] | Ratelimit intra-ticks.
PEG_ARBIT_MIN_REM = 0.05          # Range: [0.01 ... 1.0] | Tempo remanescente mínimo (0.05s = 50ms).
MAX_PEG_ENTRIES = 10000000        # Range: [1 ... ∞] | Entradas aceitáveis num ciclo.
PEG_ARBIT_TARGET_C = 0.0          # Range: [0.0 ... 99.0] | Hold até fecho do order-book.
TARGET_MULTIPLIER = 1.25          # Range: [1.0 ... 2.0] | Modificador multiplicativo p/ ciclos.

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 8 — EMPIRICAL KELLY COM MONTE CARLO
# ─────────────────────────────────────────────────────────────────────────────
KELLY_MC_SIMULATIONS = 5000   # Range: [1000 ... 50000] | Amostras na ramificação estocástica.
KELLY_CONFIDENCE     = 0.90   # Range: [0.50 ... 0.99] | Tolerância p-value.
KELLY_MIN_HISTORY    = 10     # Range: [5 ... 100] | Amostragem estatística pré-kelly.
KELLY_MAX_FRACTION   = 0.25   # Range: [0.10 ... 0.50] | Teto de portfolio alocado autonomamente.
KELLY_MIN_FRACTION   = 0.02   # Range: [0.01 ... 0.10] | Piso marginal de segurança.
KELLY_RUIN_THRESHOLD = 0.50   # Range: [0.10 ... 0.90] | Corte punitivo por drawdown previsional.

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 9 — AVELLANEDA-STOIKOV + VPIN
# ─────────────────────────────────────────────────────────────────────────────
AS_GAMMA               = 0.05  # Range: [0.01 ... 0.5] | Coeficiente de Aversão ao Risco.
AS_KAPPA_DEFAULT       = 1.0   # Range: [0.1 ... 10.0] | Chegada de ordens inicial.
AS_VPIN_WINDOW         = 50    # Range: [10 ... 200] | Volume-bucket para VPIN.
AS_VPIN_WIDEN          = 0.70  # Range: [0.5 ... 0.99] | Gatilho para penalizar o spread.
AS_VPIN_WITHDRAW       = 0.90  # Range: [0.5 ... 0.99] | Killswitch fluxos tóxicos.
AS_SPREAD_WIDEN_FACTOR = 1.1   # Range: [1.0 ... 3.0] | Alavanca de spread widen.
AS_MIN_EDGE_C          = 0.1   # Range: [0.0 ... 2.0] | Delta mínimo centavos sobre bid.

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 10 — FEES E SPREAD
# ─────────────────────────────────────────────────────────────────────────────
FEE_RATE = 0.25      # Constante de estrutura de mercado Crypto Polymarket (NÃO ALTERAR)
FEE_EXP = 2          # Exponenciação de curva Crypto Polymarket (NÃO ALTERAR)
ASK_SPREAD = 0.01    # Range: [0.0 ... 1.0] | Simulação de price slippage na liquidez de entrada.
LOOP_SLEEP = 0.001   # Range: [0.0001 ... 0.1] | Wait time intra-evento (assíncrono não-bloqueante).

# ─────────────────────────────────────────────────────────────────────────────
# GLOBAIS DE ESTADO (NÃO ALTERAR)
# ─────────────────────────────────────────────────────────────────────────────
bankroll                        = BANKROLL_INIT  # Banca persistente (nunca reseta em Demo)
daily_profit                    = 0.0
last_day                        = None
best_asks                       = {'up': None, 'down': None}
best_bids                       = {'up': None, 'down': None}
price_change                    = asyncio.Event()
bot_start_time                  = time.time()
kelly                           = None
as_model                        = None

# Martingale Condicional e Recuperação
martingale_multiplier           = 1.0            # x1, x2, x4, x8, x16, x32
accumulated_loss                = 0.0            # Soma de perdas para recuperação
recovery_rounds_remaining       = 1              # Rondas para recuperar accumulated_loss

# =============================================================================
# LOGGING (FICHEIRO + CONSOLA)
# =============================================================================
_formatter    = logging.Formatter('%(message)s')
_file_handler = logging.FileHandler('bot_xrp.log', encoding='utf-8')
_file_handler.setFormatter(_formatter)

logger = logging.getLogger('bot_xrp')
logger.setLevel(logging.DEBUG)
logger.addHandler(_file_handler)
logger.propagate = False

def load_secrets(filepath: str = "secrets.txt") -> dict:
    if not os.path.exists(filepath):
        logger.warning("[WARN] secrets.txt nao encontrado - LIVE_TRADING nao disponivel")
        return {}
    secrets = {}
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                secrets[k.strip()] = v.strip()
    return secrets

credenciais = load_secrets()
POLYMARKET_PRIVATE_KEY = credenciais.get("POLYMARKET_PRIVATE_KEY", "")

if LIVE_TRADING and not POLYMARKET_PRIVATE_KEY:
    logger.error("[ERRO] FATAL: LIVE_TRADING=True mas POLYMARKET_PRIVATE_KEY nao encontrado!")
    raise SystemExit(1)

# =============================================================================
# SDK LIVE
# =============================================================================
clob_client = None
if LIVE_TRADING:
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs
        from py_clob_client.order_builder.constants import BUY, SELL
        clob_client = ClobClient(
            host="https://clob.polymarket.com",
            key=POLYMARKET_PRIVATE_KEY,
            chain_id=137
        )
        logger.info("[INFO] SDK Polymarket carregado - LIVE TRADING ACTIVO")
    except ImportError:
        logger.error("[ERRO] py-clob-client nao instalado! (pip install py-clob-client)")
        raise SystemExit(1)

# =============================================================================
# FUNÇÕES MATEMÁTICAS & HELPERS BÁSICOS (ESTRITAMENTE INALTERADOS)
# =============================================================================
_FEE_RATE = FEE_RATE
_FEE_EXP  = FEE_EXP

def fee_rate(p: float) -> float:
    return _FEE_RATE * (p * (1.0 - p)) ** _FEE_EXP

def buy_shares_net(invested: float, ask: float) -> float:
    return (invested / ask) * (1.0 - fee_rate(ask))

def effective_entry(ask: float) -> float:
    return ask / (1.0 - fee_rate(ask))

def sell_payout(shares: float, p: float) -> float:
    return shares * p * (1.0 - fee_rate(p))

def eff_sell_price(cp: float) -> float:
    return cp * (1.0 - fee_rate(cp))

def fc(p: float) -> str:
    return f"{p * 100:.1f}c"

def get_ts() -> str:
    return datetime.now().strftime("%d/%m/%y | %H:%M:%S.%f")[:-3]

def get_remaining_str(rem: float) -> str:
    rem = max(0.0, rem)
    m   = int(rem // 60)
    s   = int(rem % 60)
    ms  = int((rem * 1000) % 1000)
    return f"{m:02d}:{s:02d}:{ms:03d}"

def get_uptime_str() -> str:
    elapsed = int(time.time() - bot_start_time)
    years,  elapsed = divmod(elapsed, 365 * 24 * 3600)
    months, elapsed = divmod(elapsed, 30  * 24 * 3600)
    days,   elapsed = divmod(elapsed, 24  * 3600)
    hours,  elapsed = divmod(elapsed, 3600)
    mins,   secs    = divmod(elapsed, 60)
    return f"{years}y:{months:02d}m:{days:02d}d:{hours:02d}h:{mins:02d}m:{secs:02d}s"

# ─────────────────────────────────────────────────────────────────────────────
# RISCO E MARTINGALE HÍBRIDO (SMOOTH RECOVERY)
# ─────────────────────────────────────────────────────────────────────────────
def calc_risk(base: float, mult: float, accum_loss: float, rec_rounds: int, bank: float) -> float:
    """Calcula risco efetivo com Martingale + Recuperação, capped a 15%.
    Fórmula: min(base * mult + accum_loss / rec_rounds / bank, MAX_RISK_PERCENT=15%)
    """
    if bank <= 0 or rec_rounds <= 0:
        return MAX_RISK_PERCENT
    recovery_bonus = accum_loss / rec_rounds / bank
    raw = (base * mult) + recovery_bonus
    return min(raw, MAX_RISK_PERCENT)

def calc_risk_preview(base: float, mult: float, accum_loss: float, rec_rounds: int, bank: float) -> float:
    """Preview do risco efetivo para a próxima ronda com estado futuro."""
    if bank <= 0 or rec_rounds <= 0:
        return MAX_RISK_PERCENT
    recovery_bonus = accum_loss / rec_rounds / bank
    raw = (base * mult) + recovery_bonus
    return min(raw, MAX_RISK_PERCENT)

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS DE LOG (LIMPOS)
# ─────────────────────────────────────────────────────────────────────────────
def log_m(module: str, action: str, msg: str):
    logger.info(f"[INFO] [{module}] [{action}] [{get_ts()}] | {msg}")

def log_raw(msg: str):
    logger.info(f"[{get_ts()}] | {msg}")

def log_info(msg: str):
    logger.info(f"[INFO] [{get_ts()}] | {msg}")

def log_warn(msg: str):
    logger.warning(f"[WARN] [{get_ts()}] | {msg}")

def log_sep():
    logger.info("-" * 80)

def log_sep2():
    logger.info("=" * 80)

# =============================================================================
# API / WEBSOCKET E PARSER DUAL (ASK E BID)
# =============================================================================
def fetch_metadata(slug: str) -> dict | None:
    try:
        url  = f"https://gamma-api.polymarket.com/events?slug={slug}"
        data = requests.get(url, timeout=5).json()[0]['markets'][0]
        ids  = json.loads(data['clobTokenIds'])
        return {'id': data['conditionId'], 'up': ids[0], 'down': ids[1], 'slug': slug}
    except Exception as e:
        log_warn(f"fetch_metadata falhou: {e}")
        return None

def get_current_slug() -> tuple[str, float]:
    now      = time.time()
    start_ts = now - (now % 300)
    if (start_ts + 300 - now) < 5:
        start_ts += 300
    return f"xrp-updown-5m-{int(start_ts)}", start_ts

async def ws_handler(t_up: str, t_down: str):
    uri        = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    _best_asks = best_asks
    _best_bids = best_bids
    _set       = price_change.set
    
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                await ws.send(json.dumps({
                    "assets_ids":             [t_up, t_down],
                    "type":                   "market",
                    "custom_feature_enabled": True
                }))
                log_info("WS conectado ao order book Polymarket (ASK/BID Tracking)")
                
                async for raw in ws:
                    items = json.loads(raw)
                    if not isinstance(items, list):
                        items = [items]
                    
                    for item in items:
                        aid = item.get("asset_id")
                        ask_p = None
                        bid_p = None
                        evt = item.get("event_type")
                        
                        if evt == "book":
                            asks = item.get("asks")
                            if asks:
                                valid_a = [float(d['price']) for d in asks if float(d['size']) > 0]
                                if valid_a:
                                    ask_p = min(valid_a)
                            bids = item.get("bids")
                            if bids:
                                valid_b = [float(d['price']) for d in bids if float(d['size']) > 0]
                                if valid_b:
                                    bid_p = max(valid_b)
                        
                        elif evt == "best_bid_ask":
                            ba_ask = item.get("best_ask")
                            if ba_ask:
                                ask_p = float(ba_ask)
                            ba_bid = item.get("best_bid")
                            if ba_bid:
                                bid_p = float(ba_bid)
                        
                        if ask_p is not None:
                            if aid == t_up: _best_asks['up'] = ask_p
                            elif aid == t_down: _best_asks['down'] = ask_p
                            _set()
                        if bid_p is not None:
                            if aid == t_up: _best_bids['up'] = bid_p
                            elif aid == t_down: _best_bids['down'] = bid_p
                            _set()
                            
        except asyncio.CancelledError:
            break
        except Exception as e:
            log_warn(f"WS erro: {e} - reconectando em 1s")
            await asyncio.sleep(1)

# =============================================================================
# LIVE ORDER
# =============================================================================
async def place_live_order(side: str, price: float, shares: float, token_id: str) -> bool:
    if not clob_client:
        return False
    try:
        side_const = BUY if side.upper() in ('UP', 'BUY') else SELL
        order_args = OrderArgs(
            token_id=token_id,
            price=round(price, 4),
            size=round(shares, 6),
            side=side_const,
            order_type="GTC"
        )
        response = clob_client.create_and_post_order(order_args)
        log_info(
            f"LIVE ORDER OK -> {side} {token_id[:8]}... @ {price:.4f} "
            f"| Size: {shares:.4f} | OrderID: {response.get('orderID', 'OK')}"
        )
        return True
    except Exception as e:
        log_warn(f"LIVE ORDER falhou: {e}")
        return False

# =============================================================================
# PRICE BUFFER / EMPIRICAL KELLY / AVELLANEDA STOIKOV
# =============================================================================
class PriceBuffer:
    __slots__ = ('max_age', 'buffer')

    def __init__(self, max_age_seconds: float = 30.0):
        self.max_age: float = max_age_seconds
        self.buffer: deque  = deque()

    def add(self, eff_c: float, ts: float):
        self.buffer.append((ts, eff_c))
        self._cleanup(ts)

    def _cleanup(self, now: float):
        cutoff = now - self.max_age
        buf    = self.buffer
        while buf and buf[0][0] < cutoff:
            buf.popleft()

    def get_price_at(self, seconds_ago: float, tolerance: float = 1.0) -> float | None:
        buf = self.buffer
        if not buf:
            return None
        target_ts  = time.time() - seconds_ago
        best_price = None
        best_diff  = tolerance + 1.0
        for ts, eff_c in buf:
            diff = abs(ts - target_ts)
            if diff < best_diff:
                best_diff  = diff
                best_price = eff_c
        return best_price

    def get_age(self) -> float:
        return (time.time() - self.buffer[0][0]) if self.buffer else 0.0

    def get_delta(self, seconds_ago: float) -> tuple[float | None, bool]:
        buf = self.buffer
        if not buf:
            return None, False
        past = self.get_price_at(seconds_ago)
        if past is None:
            return None, False
        return buf[-1][1] - past, True

    def clear(self):
        self.buffer.clear()

class EmpiricalKelly:
    __slots__ = ('returns',)

    def __init__(self):
        self.returns: list[float] = []

    def add_result(self, invested: float, payout: float):
        if invested > 0:
            self.returns.append((payout - invested) / invested)

    def compute_fraction(self, fallback: float) -> tuple[float, str]:
        n = len(self.returns)
        if n < KELLY_MIN_HISTORY:
            return fallback, f"Kelly N/A ({n}/{KELLY_MIN_HISTORY}) -> fallback {fallback:.1%}"

        arr    = np.array(self.returns)
        mean_r = float(np.mean(arr))
        std_r  = float(np.std(arr))

        if mean_r <= 0:
            return KELLY_MIN_FRACTION, f"Kelly edge negativo (mean={mean_r:.3f}) -> min {KELLY_MIN_FRACTION:.1%}"

        cv_edge     = min(std_r / mean_r if mean_r > 0 else 1.0, 1.0)
        denom       = mean_r ** 2 + std_r ** 2
        f_kelly     = (mean_r / denom) if denom > 0 else fallback
        f_empirical = f_kelly * (1.0 - cv_edge)

        rng         = np.random.default_rng()
        sim_returns = rng.choice(arr, size=(KELLY_MC_SIMULATIONS, max(n, 20)), replace=True)
        growth      = np.prod(1.0 + f_empirical * sim_returns, axis=1)
        worst_case  = float(np.percentile(growth, (1.0 - KELLY_CONFIDENCE) * 100))

        ruin_note = ""
        if worst_case < (1.0 - KELLY_RUIN_THRESHOLD):
            f_empirical *= 0.5
            ruin_note    = " [MC ruin -> halved]"

        f_final = max(KELLY_MIN_FRACTION, min(KELLY_MAX_FRACTION, f_empirical))
        log_str = (
            f"Kelly f={f_final:.3f} | f_kelly={f_kelly:.3f} | "
            f"CV={cv_edge:.2f} | u={mean_r:.3f} s={std_r:.3f} | "
            f"MC_worst={worst_case:.3f}{ruin_note} | n={n}"
        )
        return f_final, log_str

class AvellanedaStoikov:
    __slots__ = ('tick_history', 'vol_history', 'inventory_up', 'inventory_down', '_kappa')

    def __init__(self):
        self.tick_history: deque = deque(maxlen=AS_VPIN_WINDOW * 2)
        self.vol_history:  deque = deque(maxlen=100)
        self.inventory_up   = 0.0
        self.inventory_down = 0.0
        self._kappa         = AS_KAPPA_DEFAULT

    def add_tick(self, price: float, prev_price: float | None):
        direction = 0
        if prev_price is not None:
            if   price > prev_price: direction =  1
            elif price < prev_price: direction = -1
        self.tick_history.append((time.time(), price, direction))
        self.vol_history.append(price)
        th = self.tick_history
        if len(th) >= 10:
            span = th[-1][0] - th[0][0]
            if span > 0:
                self._kappa = len(th) / span

    def update_inventory(self, side: str, shares: float, is_buy: bool):
        delta = shares if is_buy else -shares
        if side == 'UP': self.inventory_up   += delta
        else:            self.inventory_down += delta

    @property
    def sigma2(self) -> float:
        if len(self.vol_history) < 3:
            return 0.01
        prices  = np.array(list(self.vol_history))
        returns = np.diff(prices) / prices[:-1]
        return float(np.var(returns))

    @property
    def vpin(self) -> float:
        recent = list(self.tick_history)[-AS_VPIN_WINDOW:]
        if len(recent) < 5:
            return 0.0
        v_buy  = sum(1 for _, _, d in recent if d ==  1)
        v_sell = sum(1 for _, _, d in recent if d == -1)
        total  = v_buy + v_sell
        return abs(v_buy - v_sell) / total if total > 0 else 0.0

    def reservation_price(self, mid: float, q: float, t_remaining: float) -> float:
        return mid - q * AS_GAMMA * self.sigma2 * t_remaining

    def optimal_half_spread(self, t_remaining: float) -> float:
        inv_term = AS_GAMMA * self.sigma2 * t_remaining / 2.0
        liq_term = (1.0 / AS_GAMMA) * math.log(1.0 + AS_GAMMA / self._kappa) if AS_GAMMA > 0 else 0.0
        return inv_term + liq_term

    def get_min_edge_c(self, mid_c: float, q: float, t_remaining: float) -> tuple[float | None, str]:
        if not AS_VPIN_ACTIVE:
            return AS_MIN_EDGE_C, "AS/VPIN OFF"

        vpin_val = self.vpin
        sig2     = self.sigma2
        r        = self.reservation_price(mid_c / 100.0, q, t_remaining) * 100.0
        half_d   = self.optimal_half_spread(t_remaining) * 100.0

        if vpin_val >= AS_VPIN_WITHDRAW:
            return None, (
                f"VPIN={vpin_val:.2f} >= {AS_VPIN_WITHDRAW} | "
                f"r={r:.1f}c half={half_d:.2f}c var={sig2:.5f} - BLOQUEADO"
            )

        widen    = AS_SPREAD_WIDEN_FACTOR if vpin_val >= AS_VPIN_WIDEN else 1.0
        min_edge = max(AS_MIN_EDGE_C, half_d * widen)
        log_str  = (
            f"VPIN={vpin_val:.2f} | r={r:.1f}c | half={half_d:.2f}c | "
            f"min_edge={min_edge:.2f}c"
            + (f" [WIDEN x{AS_SPREAD_WIDEN_FACTOR}]" if widen > 1 else "")
        )
        return min_edge, log_str

# =============================================================================
# LOGIC LOOP
# =============================================================================
async def logic_loop(
    m_start: float,
    m_end: float,
    meta: dict,
    r_mult: float,
    accum_loss: float,
    rec_rounds: int
):
    global bankroll, daily_profit, kelly, as_model

    active_trades = []
    state         = {'c1': {}, 'c2': {}}
    flags         = {
        's35': False, 'v30': False, 'd29': False,
        's25': False, 'v20': False, 'd19': False
    }

    eff_risk_per_trade = calc_risk(RISK_PER_TRADE,  r_mult, accum_loss, rec_rounds, bankroll)
    eff_eighty_risk    = calc_risk(EIGHTY_RISK,     r_mult, accum_loss, rec_rounds, bankroll)
    eff_peg_risk       = calc_risk(PEG_ARBIT_RISK,  r_mult, accum_loss, rec_rounds, bankroll)

    if r_mult > 1.0 or accum_loss > 0:
        recovery_bonus_pct = (accum_loss / rec_rounds / bankroll) if bankroll > 0 and rec_rounds > 0 else 0.0
        cap_tag_e = " [CAP]" if eff_eighty_risk >= MAX_RISK_PERCENT else ""
        cap_tag_p = " [CAP]" if eff_peg_risk    >= MAX_RISK_PERCENT else ""
        log_info(
            f"MARTINGALE | x{r_mult:.0f} | accum_loss=${accum_loss:.4f} "
            f"| rec_rounds={rec_rounds} "
            f"| recovery_bonus=${accum_loss:.4f}/{rec_rounds}/${bankroll:.4f}={recovery_bonus_pct:.1%} "
            f"| eff_risk: EIGHTY={eff_eighty_risk:.1%}{cap_tag_e} "
            f"PEG={eff_peg_risk:.1%}{cap_tag_p} "
            f"(cap={MAX_RISK_PERCENT:.0%})"
        )

    # Estado EIGHTY
    eighty_seen_levels        = {'UP': set(), 'DOWN': set()}
    eighty_tick_count         = {'UP': 0,     'DOWN': 0}
    eighty_last_buy           = {'UP': 0.0,   'DOWN': 0.0}
    eighty_first_tick_t       = {'UP': None,  'DOWN': None}
    eighty_eff_min            = {'UP': None,  'DOWN': None}
    eighty_eff_max            = {'UP': None,  'DOWN': None}
    eighty_cutoff_logged      = False
    eighty_started_logged     = False
    eighty_price_buffer       = {
        'UP':   PriceBuffer(max_age_seconds=max(15.0, EIGHTY_DELTA_LOOKBACK_S * 2)),
        'DOWN': PriceBuffer(max_age_seconds=max(15.0, EIGHTY_DELTA_LOOKBACK_S * 2))
    }
    eighty_vol_cooldown_until = {'UP': 0.0, 'DOWN': 0.0}

    # Estado Stop-Loss
    stoploss_below_levels     = {'UP': set(), 'DOWN': set()}
    stoploss_consecutive      = {'UP': 0,     'DOWN': 0}
    stoploss_last_price_c     = {'UP': None,  'DOWN': None}
    stoploss_monitor_active   = {'UP': False, 'DOWN': False}
    
    peg_arbit_count = 0
    last_peg_time   = 0.0

    mult_tag = f" [MARTINGALE x{r_mult:.0f}]" if r_mult > 1.0 else ""
    mods = []
    if EIGHTY_ACTIVE:    mods.append(f"EIGHTY({EIGHTY_START_REM_S}s->{EIGHTY_CUTOFF_S}s)")
    if CICLO_30S_ACTIVE: mods.append("CICLO_30S")
    if CICLO_20S_ACTIVE: mods.append("CICLO_20S")
    if PEG_ARBIT_ACTIVE: mods.append(f"PEG_ARBIT(PEG≤{PEG_ARBIT_EFF_THRESHOLD:.2%} | range [0-45|55-99.9]c | 25% banca)")

    log_sep2()
    log_info(f"NOVO CICLO | Market: {meta['slug']} | LIVE: {LIVE_TRADING}")
    log_info(f"   Banca: ${bankroll:.4f} | Profit acum.: ${daily_profit:.4f}{mult_tag}")
    log_info(f"   Modulos: {' | '.join(mods) if mods else 'nenhum'}")
    log_info(
        f"   Risco efetivo: EIGHTY={eff_eighty_risk:.1%} | "
        f"PEG={eff_peg_risk:.1%} | "
        f"CICLOS={eff_risk_per_trade:.1%} | "
        f"CAP={MAX_RISK_PERCENT:.0%}"
    )
    log_sep()
    log_info("   ESCUTA ACTIVA")
    log_sep()

    def pct_banca(invested: float) -> str:
        base = bankroll + invested
        return f"{invested / base * 100:.1f}% banca" if base else "---"

    async def open_trade(
        side: str, nom: float, trade_type: str, rstr: str,
        risk: float = None, wait_close: bool = False,
        fixed_invest: float = None, peg_val: float = None,
        token_id: str = None, extra_log: str = None,
        fixed_shares: float = None
    ):
        global bankroll
        if risk is None:
            risk = eff_risk_per_trade

        kelly_log = ""
        if KELLY_ACTIVE and fixed_invest is None and fixed_shares is None:
            risk, kelly_log = kelly.compute_fraction(fallback=risk)

        ask  = nom + ASK_SPREAD
        _fee = fee_rate(ask)
        eff  = effective_entry(ask)

        if fixed_shares is not None:
            shares   = fixed_shares
            invested = shares * ask / (1.0 - _fee) 
        elif fixed_invest is not None:
            invested = fixed_invest
            shares   = buy_shares_net(invested, ask)
        else:
            invested = bankroll * risk
            shares   = buy_shares_net(invested, ask)

        if trade_type.startswith('CICLO'):
            target = CYCLE_TARGET_C / 100.0 if CYCLE_TARGET_C > 0 else None
        elif trade_type == 'EIGHTY':
            target = EIGHTY_TARGET_C / 100.0 if EIGHTY_TARGET_C > 0 else None
        elif trade_type == 'PEG_ARBIT':
            target = PEG_ARBIT_TARGET_C / 100.0 if PEG_ARBIT_TARGET_C > 0 else None
        else:
            target = min(0.99, eff * TARGET_MULTIPLIER)

        bankroll -= invested
        pct       = pct_banca(invested)
        buy_fee   = _fee * 100.0
        peg_str   = f" | PEG_Eff: {fc(peg_val)} ({peg_val:.3f})" if peg_val is not None else ""
        extra     = f" | {extra_log}" if extra_log else ""
        kelly_sfx = f" | {kelly_log}" if kelly_log else ""

        trade = {
            'side': side, 'nom': nom, 'entry': eff, 'shares': shares,
            'target': target, 'type': trade_type, 'invested': invested,
            'wait_close': wait_close, 'token_id': token_id
        }
        active_trades.append(trade)

        if LIVE_TRADING and token_id:
            await place_live_order(side, ask, shares, token_id)

        module = trade_type.replace('_', ' ')
        log_m(module, 'BUY',
            f"rem={rstr} | {side} @ nom={fc(nom)} ask={fc(ask)} eff={fc(eff)}"
            f"{peg_str} | inv=${invested:.4f} ({pct}) | shares={shares:.4f}"
            f" | fee={buy_fee:.3f}%{extra}{kelly_sfx}"
        )

    def close_trade(trade: dict, cp: float, reason: str, rstr: str, is_settlement: bool = False):
        """
        Fecha um trade. Se for 'settlement' (fim de mercado), cp e 1.0 (ganho) ou 0.0 (perda), sem fees.
        Caso contrario, vende ao preco 'cp' corrente liquido de fees.
        """
        global bankroll
        
        if is_settlement:
            payout = trade['shares'] * cp  
        else:
            payout = sell_payout(trade['shares'], cp)
            
        pnl      = payout - trade['invested']
        pnl_pct  = (pnl / trade['invested'] * 100.0) if trade['invested'] else 0.0
        bankroll += payout
        
        if KELLY_ACTIVE:
            kelly.add_result(trade['invested'], payout)
        if AS_VPIN_ACTIVE:
            as_model.update_inventory(trade['side'], trade['shares'], is_buy=False)
            
        icon   = "(+)" if pnl >= 0 else "(-)"
        module = trade['type'].replace('_', ' ')
        log_m(module, 'SELL',
            f"rem={rstr} | {trade['side']} @ {fc(cp)} "
            f"| PnL: ${pnl:+.4f} ({pnl_pct:+.1f}%) {icon} "
            f"| Reason: {reason}"
        )

    def eighty_reset(e_side: str, rstr: str, reason: str):
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side]   = 0
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side]      = None
        eighty_eff_max[e_side]      = None
        log_m('EIGHTY', 'RESET', f"rem={rstr} | {e_side} - {reason}")

    def eighty_reset_silent(e_side: str):
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side]   = 0
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side]      = None
        eighty_eff_max[e_side]      = None

    def eighty_activate_vol_cooldown(e_side: str, rstr: str, reason: str):
        eighty_vol_cooldown_until[e_side] = time.time() + EIGHTY_VOL_COOLDOWN_S
        eighty_reset(e_side, rstr, reason)
        log_m('EIGHTY', 'VOL_COOLDOWN',
            f"rem={rstr} | {e_side} - bloqueado {EIGHTY_VOL_COOLDOWN_S:.0f}s")

    prev_u_ask = prev_d_ask = None
    prev_u_bid = prev_d_bid = None
    _best_asks = best_asks
    _best_bids = best_bids
    _pc_wait   = price_change.wait
    _pc_clear  = price_change.clear

    while True:
        now = time.time()
        rem = m_end - now

        # ── Fim de mercado (Settlement Dual) ──────────────────────────────────
        if rem <= 0:
            u_ask = _best_asks.get('up')  or 0.0
            d_ask = _best_asks.get('down') or 0.0
            u_bid = _best_bids.get('up') or 0.0
            d_bid = _best_bids.get('down') or 0.0
            
            log_sep()
            log_info(f"FIM DE MERCADO | UP final={fc(u_ask)} | DOWN final={fc(d_ask)}")
            
            # Avalia quem ganhou com base no BID final superior
            winner_side = 'UP' if u_bid > d_bid else 'DOWN'
            
            for trade in active_trades[:]:
                res_price = 1.0 if trade['side'] == winner_side else 0.0
                res_str = "RESOLUCAO GANHA ($1/share)" if res_price == 1.0 else "RESOLUCAO PERDIDA (Total)"
                close_trade(trade, res_price, res_str, "00:00:000", is_settlement=True)
                active_trades.remove(trade)
            break

        rstr = get_remaining_str(rem)

        try:
            await asyncio.wait_for(_pc_wait(), timeout=LOOP_SLEEP)
            _pc_clear()
        except asyncio.TimeoutError:
            pass

        u_ask = _best_asks.get('up')
        d_ask = _best_asks.get('down')
        u_bid = _best_bids.get('up')
        d_bid = _best_bids.get('down')

        if u_ask is None or d_ask is None or u_bid is None or d_bid is None:
            continue

        if u_ask == prev_u_ask and d_ask == prev_d_ask and u_bid == prev_u_bid and d_bid == prev_d_bid:
            continue

        prev_u_ask = u_ask
        prev_d_ask = d_ask
        prev_u_bid = u_bid
        prev_d_bid = d_bid

        # ── Calcula PEG pelo preço EFETIVO de ASK ─────────────────────────────
        ask_up    = u_ask + ASK_SPREAD
        ask_down  = d_ask + ASK_SPREAD
        eff_up    = effective_entry(ask_up)
        eff_down  = effective_entry(ask_down)
        peg_eff   = eff_up + eff_down
        peg_base  = u_ask + d_ask

        underpeg_eff_c = (1.0 - peg_eff) * 100.0
        peg_disp = (
            f" | PEG_Eff={peg_eff:.4f}"
            if peg_eff <= PEG_ARBIT_EFF_THRESHOLD else ""
        )
        log_raw(
            f"rem={rstr} | UP={fc(u_ask)} Eff={fc(eff_up)} | "
            f"DOWN={fc(d_ask)} Eff={fc(eff_down)}{peg_disp}"
        )

        if AS_VPIN_ACTIVE:
            mid    = (u_ask + d_ask) * 0.5
            prev_mid = ((prev_u_ask or u_ask) + (prev_d_ask or d_ask)) * 0.5
            as_model.add_tick(mid, prev_mid)

        as_blocked = False
        min_edge   = AS_MIN_EDGE_C
        if AS_VPIN_ACTIVE:
            q_total  = as_model.inventory_up - as_model.inventory_down
            min_edge, as_log = as_model.get_min_edge_c(
                mid_c=(u_ask + d_ask) * 50.0,
                q=q_total,
                t_remaining=rem
            )
            if min_edge is None:
                as_blocked = True
                log_m('AS VPIN', 'WITHDRAW', f"rem={rstr} | {as_log}")
            else:
                log_m('AS VPIN', 'STATUS', f"rem={rstr} | {as_log}")

        # =====================================================================
        # 1. PEG ARBITRAGE (NOVO SISTEMA)
        # =====================================================================
        if (PEG_ARBIT_ACTIVE
                and not as_blocked
                and peg_eff <= PEG_ARBIT_EFF_THRESHOLD
                and rem > PEG_ARBIT_MIN_REM
                and peg_arbit_count < MAX_PEG_ENTRIES
                and now - last_peg_time >= PEG_ARBIT_COOLDOWN):

            eff_up_c   = eff_up   * 100.0
            eff_down_c = eff_down * 100.0
            
            # Verifica se ambos estão em um dos ranges válidos: [0-45c] OU [55-99.9c]
            up_in_range_1   = PEG_ARBIT_RANGE_1[0] <= eff_up_c <= PEG_ARBIT_RANGE_1[1]
            up_in_range_2   = PEG_ARBIT_RANGE_2[0] <= eff_up_c <= PEG_ARBIT_RANGE_2[1]
            down_in_range_1 = PEG_ARBIT_RANGE_1[0] <= eff_down_c <= PEG_ARBIT_RANGE_1[1]
            down_in_range_2 = PEG_ARBIT_RANGE_2[0] <= eff_down_c <= PEG_ARBIT_RANGE_2[1]
            
            up_in_range   = up_in_range_1 or up_in_range_2
            down_in_range = down_in_range_1 or down_in_range_2

            if up_in_range and down_in_range:
                # Budget fixo de 25% da banca
                budget        = bankroll * PEG_ARBIT_BANCA_PCT
                ref_eff       = max(eff_up, eff_down)
                shares_to_buy = budget / ref_eff
                
                # Calcula investimentos ao preço efetivo (ask + fee)
                invest_up     = shares_to_buy * eff_up
                invest_down   = shares_to_buy * eff_down
                total_invest  = invest_up + invest_down
                margin        = (1.0 - peg_eff) * 100.0

                log_sep()
                log_m('PEG ARBIT', 'ENTRADA',
                    f"rem={rstr} | PEG_Eff={peg_eff:.4f} (margin {margin:.2f}c) | "
                    f"UP={fc(eff_up_c/100)} DOWN={fc(eff_down_c/100)} | "
                    f"Shares={shares_to_buy:.4f} | "
                    f"Total=${total_invest:.4f} (25% banca) | "
                    f"arb #{peg_arbit_count + 1}"
                )
                await open_trade('UP',   u_ask, 'PEG_ARBIT', rstr,
                                 fixed_shares=shares_to_buy, wait_close=True,
                                 peg_val=peg_eff, token_id=meta['up'])
                await open_trade('DOWN', d_ask, 'PEG_ARBIT', rstr,
                                 fixed_shares=shares_to_buy, wait_close=True,
                                 peg_val=peg_eff, token_id=meta['down'])
                log_sep()
                peg_arbit_count += 1
                last_peg_time    = now
            else:
                if peg_arbit_count == 0:
                    reasons = []
                    if not up_in_range:
                        reasons.append(f"UP_Eff {eff_up_c:.1f}c fora [0-45] e [55-99.9]")
                    if not down_in_range:
                        reasons.append(f"DOWN_Eff {eff_down_c:.1f}c fora [0-45] e [55-99.9]")
                    log_m('PEG ARBIT', 'SKIP',
                        f"rem={rstr} | PEG_Eff OK ({peg_eff:.4f}) mas {' | '.join(reasons)}")

        # =====================================================================
        # 2. TARGET CHECK + STOP-LOSS INTRA-TRADE (VIA BID)
        # =====================================================================
        for trade in active_trades[:]:
            cp = u_bid if trade['side'] == 'UP' else d_bid
            if trade.get('target') is not None and cp and cp >= trade['target']:
                close_trade(trade, cp, "TARGET", rstr)
                active_trades.remove(trade)
                continue

            bid_eff_c = eff_sell_price(cp) * 100.0
            s_side = trade['side']

            if bid_eff_c < STOPLOSS_PRICE_C:
                if not stoploss_monitor_active[s_side]:
                    stoploss_monitor_active[s_side] = True
                    stoploss_last_price_c[s_side] = bid_eff_c
                    stoploss_below_levels[s_side].add(round(bid_eff_c / STOPLOSS_PRICE_STEP_C) * STOPLOSS_PRICE_STEP_C)
                    stoploss_consecutive[s_side] = 1
                    log_m('STOPLOSS', 'MONITOR', f"rem={rstr} | {s_side} iniciado @ {bid_eff_c:.1f}c < {STOPLOSS_PRICE_C:.1f}c")
                else:
                    if bid_eff_c < stoploss_last_price_c[s_side]:
                        level_key = round(bid_eff_c / STOPLOSS_PRICE_STEP_C) * STOPLOSS_PRICE_STEP_C
                        if level_key not in stoploss_below_levels[s_side]:
                            stoploss_below_levels[s_side].add(level_key)
                            stoploss_consecutive[s_side] += 1
                            stoploss_last_price_c[s_side] = bid_eff_c
                            if stoploss_consecutive[s_side] >= STOPLOSS_TICKS:
                                close_trade(trade, cp, "STOP-LOSS FLASH-CRASH", rstr)
                                active_trades.remove(trade)
                                stoploss_reset(s_side)
                    else:
                        stoploss_reset(s_side)
                        log_m('STOPLOSS', 'RESET', f"rem={rstr} | {s_side} - preco subiu acima do ultimo ({bid_eff_c:.1f}c)")
            else:
                if stoploss_monitor_active[s_side]:
                    stoploss_reset(s_side)
                    log_m('STOPLOSS', 'RESET', f"rem={rstr} | {s_side} - preco acima {STOPLOSS_PRICE_C:.1f}c ({bid_eff_c:.1f}c)")

        def stoploss_reset(s_side: str):
            stoploss_below_levels[s_side].clear()
            stoploss_consecutive[s_side] = 0
            stoploss_last_price_c[s_side] = None
            stoploss_monitor_active[s_side] = False

        # =====================================================================
        # 3. EIGHTY
        # =====================================================================
        if EIGHTY_ACTIVE:
            if rem > EIGHTY_START_REM_S:
                pass 
            elif rem <= EIGHTY_CUTOFF_S:
                if not eighty_cutoff_logged:
                    eighty_cutoff_logged = True
                    log_m('EIGHTY', 'CUTOFF',
                        f"rem={rstr} | EIGHTY parado - rem <= {EIGHTY_CUTOFF_S}s")
            else:
                if not eighty_started_logged:
                    eighty_started_logged = True
                    log_m('EIGHTY', 'START',
                        f"rem={rstr} | EIGHTY activo [{EIGHTY_START_REM_S}s->{EIGHTY_CUTOFF_S}s] "
                        f"| risco={eff_eighty_risk:.1%}")

                for e_side, nom in (('UP', u_ask), ('DOWN', d_ask)):
                    token_id = meta['up'] if e_side == 'UP' else meta['down']
                    
                    skip_vol = (
                        EIGHTY_CUTOFF_S == 0
                        and EIGHTY_WHEN_CUTOFF_0_VOLT > 0
                        and rem <= EIGHTY_WHEN_CUTOFF_0_VOLT
                    )

                    if not skip_vol and now < eighty_vol_cooldown_until[e_side]:
                        continue

                    if not skip_vol and now - eighty_last_buy[e_side] < EIGHTY_BUY_COOLDOWN:
                        continue

                    ask   = nom + ASK_SPREAD
                    _fee  = fee_rate(ask)
                    eff_c = effective_entry(ask) * 100.0  
                    eighty_price_buffer[e_side].add(eff_c, now)

                    if as_blocked:
                        continue

                    if not (EIGHTY_MIN_EFF_C <= eff_c <= EIGHTY_MAX_EFF_C):
                        if eighty_tick_count[e_side] > 0:
                            eighty_reset(e_side, rstr,
                                f"eff_c {eff_c:.1f}c fora [{EIGHTY_MIN_EFF_C:.0f}-{EIGHTY_MAX_EFF_C:.0f}]")
                        continue

                    level_key = math.ceil(eff_c / EIGHTY_PRICE_STEP_C) * EIGHTY_PRICE_STEP_C
                    if level_key not in eighty_seen_levels[e_side]:
                        eighty_seen_levels[e_side].add(level_key)
                        eighty_tick_count[e_side] += 1

                    if eighty_first_tick_t[e_side] is None:
                        eighty_first_tick_t[e_side] = now
                        eighty_eff_min[e_side]      = eff_c
                        eighty_eff_max[e_side]      = eff_c
                    else:
                        if eff_c < eighty_eff_min[e_side]: eighty_eff_min[e_side] = eff_c
                        if eff_c > eighty_eff_max[e_side]: eighty_eff_max[e_side] = eff_c

                    elapsed = now - eighty_first_tick_t[e_side]
                    var_c   = eighty_eff_max[e_side] - eighty_eff_min[e_side]
                    vol_nok = (elapsed <= EIGHTY_VOL_WINDOW_S and var_c >= EIGHTY_VOL_MAX_C)

                    epb = eighty_price_buffer[e_side]
                    delta_10, valid_10 = epb.get_delta(1.0)
                    delta_20, valid_20 = epb.get_delta(2.0)
                    delta_30, valid_30 = epb.get_delta(3.0)

                    delta_parts = []
                    if valid_10: delta_parts.append(f"D1.0s:{delta_10:+.1f}c")
                    if valid_20: delta_parts.append(f"D2.0s:{delta_20:+.1f}c")
                    if valid_30: delta_parts.append(f"D3.0s:{delta_30:+.1f}c")
                    delta_str = " | ".join(delta_parts) if delta_parts else f"D aguarda ({epb.get_age():.1f}s)"

                    delta_15, valid_15 = epb.get_delta(EIGHTY_DELTA_VOL_TIME_S)
                    rapid_rise = (
                        valid_15
                        and delta_15 is not None
                        and delta_15 >= EIGHTY_DELTA_VOL_RISE_C
                    )

                    delta_ok     = True
                    delta_reason = ""
                    has_delta    = valid_10 or valid_20 or valid_30

                    if valid_10 and delta_10 is not None and delta_10 < 0:
                        delta_ok, delta_reason = False, f"D1s={delta_10:+.1f}c (a cair)"
                    elif valid_20 and delta_20 is not None and delta_20 < 0:
                        delta_ok, delta_reason = False, f"D2s={delta_20:+.1f}c (a cair)"
                    elif valid_30 and delta_30 is not None and delta_30 < 0:
                        delta_ok, delta_reason = False, f"D3s={delta_30:+.1f}c (a cair)"
                    elif rapid_rise:
                        delta_ok, delta_reason = False, f"D{EIGHTY_DELTA_VOL_TIME_S}s={delta_15:+.1f}c (pump rapido)"
                    elif valid_30 and delta_30 is not None and delta_30 >= EIGHTY_DELTA_MAX_RISE_C:
                        delta_ok, delta_reason = False, f"D3s={delta_30:+.1f}c (exaustao tendencial)"

                    vol_str    = "VOL SKIP" if skip_vol else f"VOL {'NOK' if vol_nok else 'OK'} ({var_c:.1f}c/{elapsed:.1f}s)"
                    delta_icon = "UP" if (delta_ok and has_delta) else ("DOWN" if has_delta else "WAIT")
                    peg_str    = f" | PEG_Eff={peg_eff:.3f}" if peg_eff * 100.0 <= EIGHTY_PEG_MIN_C else ""

                    log_m('EIGHTY', 'WATCH',
                        f"rem={rstr} | {e_side} Eff={fc(eff_c/100)} | {vol_str} | "
                        f"{delta_str} ({delta_icon}){peg_str} | "
                        f"ticks={eighty_tick_count[e_side]}/{EIGHTY_MIN_TICKS}"
                    )

                    if not skip_vol:
                        if vol_nok:
                            eighty_activate_vol_cooldown(e_side, rstr,
                                f"VOL {var_c:.1f}c em {elapsed:.1f}s "
                                f"(max {EIGHTY_VOL_MAX_C:.1f}c/{EIGHTY_VOL_WINDOW_S:.1f}s)")
                            continue
                        if rapid_rise:
                            eighty_activate_vol_cooldown(e_side, rstr,
                                f"PUMP RAPIDO D{EIGHTY_DELTA_VOL_TIME_S}s={delta_15:+.1f}c")
                            continue

                    if eighty_tick_count[e_side] >= EIGHTY_MIN_TICKS:
                        if peg_eff * 100.0 < EIGHTY_PEG_MIN_C:
                            eighty_reset(e_side, rstr,
                                f"PEG_Eff {peg_eff*100:.1f}c < min {EIGHTY_PEG_MIN_C:.1f}c")
                            continue

                        if has_delta and not delta_ok:
                            eighty_reset(e_side, rstr, f"DELTA NOK - {delta_reason}")
                            continue

                        if AS_VPIN_ACTIVE and min_edge is not None:
                            edge_c = (99.0 - eff_c)
                            if edge_c < min_edge:
                                eighty_reset(e_side, rstr,
                                    f"AS EDGE NOK - edge {edge_c:.1f}c < min {min_edge:.2f}c")
                                continue

                        if bankroll > 0:
                            if AS_VPIN_ACTIVE:
                                shares_est = buy_shares_net(bankroll * eff_eighty_risk, nom + ASK_SPREAD)
                                as_model.update_inventory(e_side, shares_est, is_buy=True)

                            await open_trade(
                                e_side, nom, 'EIGHTY', rstr,
                                risk=eff_eighty_risk,
                                wait_close=True,
                                peg_val=peg_eff,
                                token_id=token_id,
                                extra_log=f"ticks={eighty_tick_count[e_side]} | {delta_str}"
                            )
                            eighty_last_buy[e_side] = now
                            eighty_reset_silent(e_side)
                            log_m('EIGHTY', 'COOLDOWN',
                                f"rem={rstr} | {e_side} - cooldown {EIGHTY_BUY_COOLDOWN:.1f}s")

        # =====================================================================
        # 4. CICLO 30s
        # =====================================================================
        if CICLO_30S_ACTIVE:
            if not flags['s35'] and rem <= CYCLE_30S_SNAPSHOT_REM:
                state['c1']['snap_u'] = u_ask
                state['c1']['snap_d'] = d_ask
                flags['s35'] = True
                log_m('CICLO 30s', 'SNAP',
                    f"rem={rstr} | UP={fc(u_ask)} DOWN={fc(d_ask)}")

            if flags['s35'] and not flags['v30'] and rem <= CYCLE_30S_VOL_CHECK_REM:
                vol_c = abs(u_ask - state['c1']['snap_u']) * 100.0
                flags['v30']          = True
                state['c1']['vol_ok'] = vol_c <= CYCLE_VOL_MAX_C
                log_m('CICLO 30s', 'VOLT',
                    f"rem={rstr} | vol={vol_c:.1f}c (max {CYCLE_VOL_MAX_C:.1f}c) "
                    f"| {'OK' if state['c1']['vol_ok'] else 'NOK'}")

            if (flags['v30'] and state['c1'].get('vol_ok')
                    and not flags['d29'] and rem <= CYCLE_30S_BUY_REM):
                flags['d29'] = True
                for e_side, nom, tid in (('UP', u_ask, meta['up']), ('DOWN', d_ask, meta['down'])):
                    price_c = effective_entry(nom + ASK_SPREAD) * 100.0
                    peg_c   = peg_base * 100.0
                    if CYCLE_PRICE_MIN_C <= price_c <= CYCLE_PRICE_MAX_C and peg_c >= CYCLE_PEG_MIN_C:
                        await open_trade(e_side, nom, 'CICLO_30s', rstr,
                                         risk=eff_risk_per_trade, wait_close=True,
                                         peg_val=peg_eff, token_id=tid)
                    else:
                        reasons = []
                        if price_c < CYCLE_PRICE_MIN_C:
                            reasons.append(f"preco {price_c:.1f}c < min {CYCLE_PRICE_MIN_C:.1f}c")
                        elif price_c > CYCLE_PRICE_MAX_C:
                            reasons.append(f"preco {price_c:.1f}c > max {CYCLE_PRICE_MAX_C:.1f}c")
                        if peg_c < CYCLE_PEG_MIN_C:
                            reasons.append(f"PEG {peg_c:.1f}c < {CYCLE_PEG_MIN_C:.1f}c")
                        log_m('CICLO 30s', 'SKIP',
                            f"rem={rstr} | {e_side} - {' | '.join(reasons)}")

        # =====================================================================
        # 5. CICLO 20s
        # =====================================================================
        if CICLO_20S_ACTIVE:
            if not flags['s25'] and rem <= CYCLE_20S_SNAPSHOT_REM:
                state['c2']['snap_u'] = u_ask
                state['c2']['snap_d'] = d_ask
                flags['s25'] = True
                log_m('CICLO 20s', 'SNAP',
                    f"rem={rstr} | UP={fc(u_ask)} DOWN={fc(d_ask)}")

            if flags['s25'] and not flags['v20'] and rem <= CYCLE_20S_VOL_CHECK_REM:
                vol_c = abs(u_ask - state['c2']['snap_u']) * 100.0
                flags['v20']          = True
                state['c2']['vol_ok'] = vol_c <= CYCLE_VOL_MAX_C
                log_m('CICLO 20s', 'VOLT',
                    f"rem={rstr} | vol={vol_c:.1f}c (max {CYCLE_VOL_MAX_C:.1f}c) "
                    f"| {'OK' if state['c2']['vol_ok'] else 'NOK'}")

            if (flags['v20'] and state['c2'].get('vol_ok')
                    and not flags['d19'] and rem <= CYCLE_20S_BUY_REM):
                flags['d19'] = True
                for e_side, nom, tid in (('UP', u_ask, meta['up']), ('DOWN', d_ask, meta['down'])):
                    price_c = effective_entry(nom + ASK_SPREAD) * 100.0
                    peg_c   = peg_base * 100.0
                    if CYCLE_PRICE_MIN_C <= price_c <= CYCLE_PRICE_MAX_C and peg_c >= CYCLE_PEG_MIN_C:
                        await open_trade(e_side, nom, 'CICLO_20s', rstr,
                                         risk=eff_risk_per_trade, wait_close=True,
                                         peg_val=peg_eff, token_id=tid)
                    else:
                        reasons = []
                        if price_c < CYCLE_PRICE_MIN_C:
                            reasons.append(f"preco {price_c:.1f}c < min {CYCLE_PRICE_MIN_C:.1f}c")
                        elif price_c > CYCLE_PRICE_MAX_C:
                            reasons.append(f"preco {price_c:.1f}c > max {CYCLE_PRICE_MAX_C:.1f}c")
                        if peg_c < CYCLE_PEG_MIN_C:
                            reasons.append(f"PEG {peg_c:.1f}c < {CYCLE_PEG_MIN_C:.1f}c")
                        log_m('CICLO 20s', 'SKIP',
                            f"rem={rstr} | {e_side} - {' | '.join(reasons)}")

# =============================================================================
# MAIN
# =============================================================================
async def main():
    global daily_profit, last_day, price_change, bankroll
    global martingale_multiplier, accumulated_loss, recovery_rounds_remaining
    global kelly, as_model

    kelly                   = EmpiricalKelly()
    as_model                = AvellanedaStoikov()
    martingale_multiplier   = 1.0
    accumulated_loss        = 0.0
    recovery_rounds_remaining = 1

    log_sep2()
    log_info("BOT XRP POLYMARKET v0.36.1 INICIADO")
    log_sep()
    log_info(f"   LIVE_TRADING     : {LIVE_TRADING}")
    log_info(f"   BANKROLL_INIT    : ${BANKROLL_INIT:.2f}")
    log_sep()
    log_info("   RISCO BASE:")
    log_info(f"   RISK_PER_TRADE   : {RISK_PER_TRADE:.0%}")
    log_info(f"   EIGHTY_RISK      : {EIGHTY_RISK:.0%}")
    log_info(f"   PEG_ARBIT_RISK   : {PEG_ARBIT_RISK:.0%}")
    log_sep()
    log_info("   MARTINGALE CONDICIONAL + RECUPERAÇÃO:")
    log_info(f"   MAX_MULTIPLIER   : x{MAX_RISK_MULTIPLIER}")
    log_info(f"   RECOVERY_ROUNDS  : {RECOVERY_ROUNDS_BASE} base")
    log_info(f"   MAX_RISK CAP     : {MAX_RISK_PERCENT:.0%} (RÍGIDO)")
    log_info("   Regras:")
    log_info("   - PnL < 0: mult x2 | +10 rounds | acc_loss += loss")
    log_info("   - PnL = 0: mult mantém | estado intacto")
    log_info("   - PnL > 0: mult = x1 | acc_loss -= profit | -1 round")
    log_sep()
    log_info("   MODULOS:")
    log_info(f"   EIGHTY           : {'ON' if EIGHTY_ACTIVE    else 'OFF'}")
    log_info(f"   PEG_ARBIT        : {'ON' if PEG_ARBIT_ACTIVE else 'OFF'}")
    log_info(f"   CICLO_30S        : {'ON' if CICLO_30S_ACTIVE else 'OFF'}")
    log_info(f"   CICLO_20S        : {'ON' if CICLO_20S_ACTIVE else 'OFF'}")
    log_info(f"   KELLY            : {'ON' if KELLY_ACTIVE     else 'OFF'}")
    log_info(f"   AS+VPIN          : {'ON' if AS_VPIN_ACTIVE   else 'OFF'}")
    log_sep2()

    while True:
        slug, start_ts = get_current_slug()
        meta = fetch_metadata(slug)
        if not meta:
            log_warn(f"Metadata nao encontrada para {slug} - retentando em 1s")
            await asyncio.sleep(1)
            continue

        market_day = datetime.fromtimestamp(start_ts).date()
        if last_day is None or market_day != last_day:
            daily_profit = 0.0
            
            # Lê saldo real se LIVE_TRADING=True
            if LIVE_TRADING and clob_client:
                try:
                    wallet_addr = clob_client.get_address()
                    resp_balance = clob_client.get_balance(wallet_addr)
                    if resp_balance:
                        new_balance = float(resp_balance)
                        log_info(f"Saldo Polymarket LIDO: ${new_balance:.4f}")
                        bankroll = new_balance  # Atualiza a banca com o valor real
                except Exception as e:
                    log_warn(f"Falha ao ler saldo (Fallback p/ $BANKROLL_INIT): {e}")
                    bankroll = BANKROLL_INIT
            else:
                # Demo: banca persistente, inicia em BANKROLL_INIT e nunca reseta
                if last_day is None:
                    bankroll = BANKROLL_INIT
            
            # Reset do Martingale no novo dia
            martingale_multiplier = 1.0
            accumulated_loss = 0.0
            recovery_rounds_remaining = 1
            last_day = market_day
            kelly = EmpiricalKelly()
            as_model = AvellanedaStoikov()
            
            log_sep2()
            log_info(f"NOVO DIA {market_day}")
            log_info(f"   Banca (init/live) : ${bankroll:.4f}")
            if LIVE_TRADING:
                log_info(f"   Modo              : LIVE TRADING")
            else:
                log_info(f"   Modo              : DEMO (banca persistente)")
            log_info(f"   Martingale        : x{martingale_multiplier:.0f} | acc_loss=$0.0000 | rounds={recovery_rounds_remaining}")
            log_sep2()

        best_asks['up'] = best_asks['down'] = None
        best_bids['up'] = best_bids['down'] = None
        price_change.clear()

        ws_task = asyncio.create_task(ws_handler(meta['up'], meta['down']))
        await asyncio.sleep(1.0) 

        if best_asks['up'] is not None:
            pre_bank = bankroll

            await logic_loop(
                start_ts,
                start_ts + 300,
                meta,
                martingale_multiplier,
                accumulated_loss,
                recovery_rounds_remaining
            )

            profit_this  = bankroll - pre_bank
            daily_profit += profit_this
            pnl_pct      = (profit_this / pre_bank * 100.0) if pre_bank > 0 else 0.0
            
            base_ref     = BANKROLL_INIT if not LIVE_TRADING else pre_bank
            daily_pct    = (daily_profit / base_ref * 100.0) if base_ref > 0 else 0.0

            if profit_this == 0.0:
                pnl_str = "PnL: $0.0000 (0.00%)"
            else:
                pnl_str = f"PnL: ${profit_this:+.4f} ({pnl_pct:+.2f}%)"

            log_sep2()

            # ═ MARTINGALE CONDICIONAL ═
            if profit_this < 0:
                # Loss: dobra multiplicador, adiciona 10 rounds, soma 50% da perda ao acc_loss
                loss = abs(profit_this)
                accumulated_loss += loss * 0.50
                martingale_multiplier = min(martingale_multiplier * 2.0, float(MAX_RISK_MULTIPLIER))
                recovery_rounds_remaining += RECOVERY_ROUNDS_BASE

                next_risk_eighty = calc_risk_preview(EIGHTY_RISK, martingale_multiplier, accumulated_loss, recovery_rounds_remaining, bankroll)
                next_risk_peg = calc_risk_preview(PEG_ARBIT_RISK, martingale_multiplier, accumulated_loss, recovery_rounds_remaining, bankroll)
                cap_e = " [CAP 15%]" if next_risk_eighty >= MAX_RISK_PERCENT else ""
                cap_p = " [CAP 15%]" if next_risk_peg >= MAX_RISK_PERCENT else ""

                log_info(
                    f"MARTINGALE CONDICIONAL | PnL < 0 (Loss) | Mult escalado: x{martingale_multiplier:.0f} "
                    f"| Acc_loss: ${accumulated_loss:.4f} "
                    f"| Recovery rounds: {recovery_rounds_remaining} "
                    f"| Proximo Risco EIGHTY={next_risk_eighty:.1%}{cap_e} PEG={next_risk_peg:.1%}{cap_p}"
                )
                log_info(f"ROUND | {pnl_str}")

            elif profit_this == 0.0:
                # Zero: mantém multiplicador e estado
                if martingale_multiplier > 1.0:
                    log_info(
                        f"MARTINGALE CONDICIONAL | PnL = 0 (Neutro) | Mult x{martingale_multiplier:.0f} mantido | "
                        f"sem trades ou sem impacto - estado intacto"
                    )
                    log_info(f"ROUND | {pnl_str}")
                else:
                    log_info(f"ROUND | {pnl_str}")

            elif profit_this > 0:
                # Green: reseta mult para x1 e recupera acc_loss
                prev_loss_before = accumulated_loss
                accumulated_loss = max(0.0, accumulated_loss - profit_this)
                recovered = prev_loss_before - accumulated_loss
                martingale_multiplier = 1.0
                recovery_rounds_remaining = max(1, recovery_rounds_remaining - 1)

                if recovered > 0 and accumulated_loss > 0:
                    log_info(
                        f"MARTINGALE CONDICIONAL | PnL > 0 (Green) | Mult reset x1 | "
                        f"RECUPERAÇÃO PARCIAL (recuperados ${recovered:.4f} | restam ${accumulated_loss:.4f}) | "
                        f"Rounds restantes: {recovery_rounds_remaining}"
                    )
                elif recovered > 0 and accumulated_loss == 0.0:
                    log_info(
                        f"MARTINGALE CONDICIONAL | PnL > 0 (Green) | Mult reset x1 | "
                        f"RECUPERAÇÃO COMPLETA (${prev_loss_before:.4f} recuperados em total) | "
                        f"Sistema de recovery finalizado"
                    )
                    recovery_rounds_remaining = 1
                log_info(f"ROUND | {pnl_str}")

            if daily_profit == 0.0:
                total_str = "$0.0000 (0.00%)"
            else:
                total_str = f"${daily_profit:+.4f} ({daily_pct:+.2f}%)"

            log_info(
                f"TOTAL | PnL: {total_str} | "
                f"Banca: ${bankroll:.4f} | "
                f"Accumul.Loss: ${accumulated_loss:.4f} | "
                f"Mult: x{martingale_multiplier:.0f} | "
                f"Uptime: {get_uptime_str()}"
            )
            log_sep2()

        else:
            log_warn("Sem precos recebidos neste ciclo - a saltar")

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