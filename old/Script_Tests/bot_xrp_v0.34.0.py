# =============================================================================
# BOT XRP POLYMARKET — v0.34.0
# =============================================================================
# CHANGELOG v0.34.0:
# [v0.34.0] [feat] MARTINGALE_RECOVERY — além de duplicar o risco base, adiciona
#           50% das perdas acumuladas (recovery) ao risco efectivo de todos os módulos
# -----------------------------------------------------------------------------
# CHANGELOG v0.33.0:
# [v0.33.0] [feat] PEG_ARBIT_RANGE — range de preço efetivo para entrada (ex: [35, 65] = 35c-65c)
# [v0.33.0] [fix]  PEG calculado com base no Eff (effective price) e não no preço base
#           - Só entra se (Eff_UP + Eff_DOWN) < 1.00 (100c)
# [v0.33.0] [fix]  PEG ARBIT agora compra por SHARES iguais em ambos os lados
#           - Calcula shares com 20% da banca no lado mais caro, depois compra igual no outro
# [v0.33.0] [fix]  EIGHTY_RISK fixo em 7% da banca
# [v0.33.0] [fix]  Martingale com limite de 50% da banca
# [v0.33.0] [fix]  PnL zero sem sinal (+): "PnL: $0.0000 (0.00%)" em vez de "$+0.0000 (+0.00%)"
# -----------------------------------------------------------------------------
# CHANGELOG v0.32.0:
# [v0.32.0] [feat] EIGHTY_START_REM_S — janela de início do EIGHTY
# [v0.32.0] [cfg]  CICLO_30S_ACTIVE = False, CICLO_20S_ACTIVE = False
# [v0.32.0] [perf] __slots__ em classes, aliases locais, fee_rate calculado UMA vez
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
# ======================== PARÂMETROS CONFIGURÁVEIS ===========================
# =============================================================================
#
# CONVENÇÃO DE UNIDADES:
#   _C   → cents  (0.0 ... 100.0 para preços individuais; 0.0 ... 200.0 para PEG = UP+DOWN)
#   _S   → segundos (float)
#   risk / fraction / confidence / threshold → rácio 0.0 ... 1.0
#   multiplier / factor → float ≥ 1.0
#   count / simulations / window → int
#   VPIN → rácio 0.0 ... 1.0
# =============================================================================

LIVE_TRADING = False
# LIVE_TRADING → True = executa ordens reais na Polymarket (usa chave privada)
#               False = modo simulação (apenas logs, sem gastar dinheiro)

# ── Banca e Risco ────────────────────────────────────────────────────────────
BANKROLL_INIT       = 25.0
# BANKROLL_INIT → Banca inicial em USDC que o bot usa no início de cada dia (é resetada automaticamente a cada novo mercado de 5 minutos)

RISK_PER_TRADE      = 0.05
# RISK_PER_TRADE → Risco base por trade (0.0 a 1.0). Ex: 0.05 = 5% da banca actual. Este valor é multiplicado pelo martingale e pelo recovery

MAX_RISK_MULTIPLIER = 16
# MAX_RISK_MULTIPLIER → Limite máximo do martingale (x2, x4, x8, x16). Evita que o risco exploda para valores absurdos

MAX_RISK_PERCENT    = 0.50
# MAX_RISK_PERCENT → Cap absoluto de risco por trade (50% da banca). Mesmo com martingale + recovery, nunca passa deste valor

MARTINGALE_RECOVERY = 0.50
# MARTINGALE_RECOVERY → [v0.34.0] Percentagem das perdas acumuladas que é adicionada ao risco. Ex: 0.50 = adiciona 50% do dinheiro perdido até agora

# ── Toggles (ligar/desligar módulos) ─────────────────────────────────────────
CICLO_30S_ACTIVE = False   # Ativa estratégia de ciclo de 30 segundos
CICLO_20S_ACTIVE = False   # Ativa estratégia de ciclo de 20 segundos
EIGHTY_ACTIVE    = True    # Estratégia "Eighty" (compra em tendência de 80+ segundos)
PEG_ARBIT_ACTIVE = True    # Arbitragem PEG (compra quando UP+DOWN < 100c)
KELLY_ACTIVE     = False   # Usa fórmula Kelly empírica para ajustar risco dinamicamente
AS_VPIN_ACTIVE   = False   # Usa modelo Avellaneda-Stoikov + VPIN (mais avançado)

# ── Ciclos (estratégias de snapshot + volume) ────────────────────────────────
CYCLE_PRICE_MIN_C       = 74.0   # Preço mínimo em cents para entrar no ciclo
CYCLE_PRICE_MAX_C       = 85.0   # Preço máximo em cents para entrar no ciclo
CYCLE_PEG_MIN_C         = 96.5   # PEG mínimo (em cents) para aceitar o ciclo
CYCLE_VOL_MAX_C         = 52.0   # Volatilidade máxima permitida no ciclo (em cents)

CYCLE_30S_SNAPSHOT_REM  = 35.0   # Tempo restante para tirar snapshot do ciclo 30s
CYCLE_30S_VOL_CHECK_REM = 30.0   # Tempo restante para verificar volume no ciclo 30s
CYCLE_30S_BUY_REM       = 29.8   # Tempo restante para comprar no ciclo 30s

CYCLE_20S_SNAPSHOT_REM  = 25.0   # Tempo restante para tirar snapshot do ciclo 20s
CYCLE_20S_VOL_CHECK_REM = 20.0   # Tempo restante para verificar volume no ciclo 20s
CYCLE_20S_BUY_REM       = 19.8   # Tempo restante para comprar no ciclo 20s

# ── Eighty (estratégia de tendência curta) ───────────────────────────────────
EIGHTY_START_REM_S        = 300    # Segundos restantes para começar o Eighty (5 minutos)
EIGHTY_MIN_EFF_C          = 82.0   # Preço efectivo mínimo em cents para comprar
EIGHTY_MAX_EFF_C          = 99.0   # Preço efectivo máximo em cents para comprar
EIGHTY_MIN_TICKS          = 7      # Número mínimo de ticks (movimentos) para confirmar tendência
EIGHTY_RISK               = 0.07   # Risco base para Eighty (7% da banca)
EIGHTY_CUTOFF_S           = 5      # Segundos restantes onde o Eighty é desligado
EIGHTY_WHEN_CUTOFF_0_VOLT = 35.0   # Quando cutoff=0, ignora volume abaixo deste tempo
EIGHTY_PEG_MIN_C          = 97.0   # PEG mínimo em cents para aceitar compra Eighty
EIGHTY_BUY_COOLDOWN       = 4.0    # Segundos de cooldown entre compras do mesmo lado
EIGHTY_VOL_WINDOW_S       = 5.0    # Janela de tempo para calcular volatilidade
EIGHTY_VOL_MAX_C          = 4.5    # Volatilidade máxima permitida na janela (em cents)
EIGHTY_VOL_COOLDOWN_S     = 5.0    # Cooldown após detectar volume excessivo

# Parâmetros avançados do Eighty (delta / volatilidade)
EIGHTY_DELTA_INTERVALS    = [0.5, 1.0, 2.0]   # Intervalos de tempo para calcular delta de preço
EIGHTY_DELTA_LOOKBACK_S   = 2.0               # Lookback para deltas rápidos
EIGHTY_DELTA_MAX_RISE_C   = 3.5               # Delta máximo permitido em subida rápida
EIGHTY_DELTA_VOL_RISE_C   = 3.5               # Delta de volume máximo em subida
EIGHTY_DELTA_VOL_TIME_S   = 1.5               # Tempo para detectar subida rápida de delta

EIGHTY_TARGET_C           = 0.0   # Target de venda automática (0.0 = desativado)

# ── PEG Arbitrage (arbitragem pura quando UP+DOWN < 100c) ────────────────────
PEG_ARBIT_RANGE         = (35.0, 65.0)   # Range de preço efectivo (cents) onde entra
# Exemplo: só compra se ambos os lados estiverem entre 35c e 65c efectivos

PEG_ARBIT_UNDERPEG_C    = 1.0            # Desvio mínimo do PEG (ex: 1.0c abaixo de 100c)
PEG_ARBIT_RISK          = 0.20           # Risco base para PEG (20% da banca)
PEG_ARBIT_COOLDOWN      = 0.05           # Tempo mínimo entre entradas PEG
PEG_ARBIT_MIN_REM       = 5.0            # Tempo restante mínimo para entrar
MAX_PEG_ENTRIES         = 10000000       # Limite de entradas PEG por mercado (praticamente ilimitado)
PEG_ARBIT_TARGET_C      = 0.0            # Target de venda automática (0.0 = desativado)
TARGET_MULTIPLIER       = 1.25           # Multiplicador de target quando não definido (1.25x entry)

# ── Empirical Kelly (gestão de risco avançada baseada em histórico) ──────────
KELLY_MC_SIMULATIONS = 5000    # Número de simulações Monte Carlo para Kelly
KELLY_CONFIDENCE     = 0.90    # Nível de confiança (90% = risco de ruína aceitável)
KELLY_MIN_HISTORY    = 10      # Mínimo de trades para calcular Kelly
KELLY_MAX_FRACTION   = 0.25    # Fração máxima de Kelly (25%)
KELLY_MIN_FRACTION   = 0.02    # Fração mínima de Kelly (2%)
KELLY_RUIN_THRESHOLD = 0.50    # Threshold de ruína (50% de perda na simulação)

# ── Avellaneda-Stoikov + VPIN (modelo de market-making avançado) ─────────────
AS_GAMMA               = 0.05   # Parâmetro de inventário (penaliza desbalanceamento)
AS_KAPPA_DEFAULT       = 1.0    # Intensidade de chegada de ordens
AS_VPIN_WINDOW         = 50     # Janela de ticks para calcular VPIN
AS_VPIN_WIDEN          = 0.70   # VPIN acima disto → widening de spread
AS_VPIN_WITHDRAW       = 0.90   # VPIN acima disto → retirar ordens
AS_SPREAD_WIDEN_FACTOR = 1.1    # Factor de widening quando VPIN alto
AS_MIN_EDGE_C          = 0.1    # Edge mínimo em cents (lucro mínimo por trade)

# ── Fee / Spread / Performance ───────────────────────────────────────────────
FEE_RATE          = 0.25   # Taxa base da Polymarket (0.25%)
FEE_EXP           = 2      # Expoente da curva de fee (não alterar)
ASK_SPREAD        = 0.01   # Spread adicionado ao ask price (0.01 = 1 cent)
LOOP_SLEEP        = 0.001  # Tempo de sleep entre loops (em segundos) — performance

# ── Globais de estado (variáveis internas controladas pelo bot) ───────────────
bankroll          = BANKROLL_INIT # bankroll → Banca ACTUAL em USDC. É atualizada em tempo real após cada compra/venda. Começa com BANKROLL_INIT e é resetada todo dia novo.
daily_profit      = 0.0 # daily_profit → Lucro/prejuízo acumulado no dia atual (reset a cada novo dia).
last_day          = None # last_day → Data do último mercado processado (usado para detectar "novo dia" e fazer reset).
best_asks         = {'up': None, 'down': None} # best_asks → Dicionário que guarda o melhor preço de venda (ask) atual dos dois lados. Atualizado em tempo real pelo WebSocket.
price_change      = asyncio.Event() # price_change → Evento asyncio usado para "acordar" o loop principal sempre que chega um novo preço do WebSocket (evita CPU 100% em loop infinito).
risk_multiplier   = 1.0 # risk_multiplier → Multiplicador atual do Martingale (1.0 = normal, 2.0, 4.0, 8.0...). Dobrado após perda, resetado após lucro.
bot_start_time    = time.time() # bot_start_time → Timestamp de quando o bot foi iniciado (usado para calcular uptime).
kelly             = None # kelly → Instância da classe EmpiricalKelly (calcula fração ótima de risco com base no histórico).
as_model          = None # as_model → Instância da classe AvellanedaStoikov (modelo avançado de inventário + VPIN).
accumulated_loss  = 0.0 # accumulated_loss → [v0.34.0] Total de perdas desde o último lucro. Usado no MARTINGALE_RECOVERY para adicionar "recovery" ao risco.

# =============================================================================
# ========================== LOGGER ===========================================
# =============================================================================

logging.basicConfig(filename='bot_xrp.log', level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

# =============================================================================
# ========================== SECRETS ==========================================
# =============================================================================

def load_secrets(filepath: str = "secrets.txt") -> dict:
    if not os.path.exists(filepath):
        print("❌ secrets.txt não encontrado!")
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
    print("❌ ERRO: LIVE_TRADING=True mas POLYMARKET_PRIVATE_KEY não está no secrets.txt!")
    raise SystemExit(1)

# =============================================================================
# ========================== SDK LIVE =========================================
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
        print("✅ SDK Polymarket carregado — LIVE TRADING ATIVADO")
    except ImportError:
        print("❌ py-clob-client não instalado!")
        raise SystemExit(1)

# =============================================================================
# ========================== FUNÇÕES AUXILIARES ===============================
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
    return datetime.now().strftime("%d/%m/%Y | %H:%M:%S.%f")[:-3]

def get_remaining_str(rem: float) -> str:
    rem = max(0.0, rem)
    m   = int(rem // 60)
    s   = int(rem % 60)
    ms  = int((rem * 1000) % 1000)
    return f"{m:02d}:{s:02d}:{ms:03d}"

_log_info_fn = logger.info

def log_m(module: str, action: str, msg: str):
    _log_info_fn(f"[INFO] [{module}] [{action}] [{get_ts()}] | {msg}")

def log_raw(msg: str):
    _log_info_fn(f"[{get_ts()}] | {msg}")

def log_info(msg: str):
    _log_info_fn(f"[INFO] [{get_ts()}] | {msg}")

def log_sep():
    _log_info_fn("=" * 73)

def get_uptime_str() -> str:
    elapsed = int(time.time() - bot_start_time)
    years,  elapsed = divmod(elapsed, 365 * 24 * 3600)
    months, elapsed = divmod(elapsed, 30  * 24 * 3600)
    days,   elapsed = divmod(elapsed, 24  * 3600)
    hours,  elapsed = divmod(elapsed, 3600)
    mins,   secs    = divmod(elapsed, 60)
    return f"{years}y:{months:02d}m:{days:02d}d:{hours:02d}h:{mins:02d}m:{secs:02d}s"

def format_pnl(value: float) -> str:
    if value == 0.0:
        return "$0.0000 (0.00%)"
    return f"${value:+.4f}"

def format_pnl_pct(value: float, pct: float) -> str:
    if value == 0.0:
        return "$0.0000 (0.00%)"
    return f"${value:+.4f} ({pct:+.2f}%)"

# =============================================================================
# ========================== API / WEBSOCKET ==================================
# =============================================================================

def fetch_metadata(slug: str) -> dict | None:
    try:
        url  = f"https://gamma-api.polymarket.com/events?slug={slug}"
        data = requests.get(url, timeout=5).json()[0]['markets'][0]
        ids  = json.loads(data['clobTokenIds'])
        return {'id': data['conditionId'], 'up': ids[0], 'down': ids[1], 'slug': slug}
    except Exception:
        return None

def get_current_slug() -> tuple[str, float]:
    now      = time.time()
    start_ts = now - (now % 300)
    if (start_ts + 300 - now) < 5:
        start_ts += 300
    return f"xrp-updown-5m-{int(start_ts)}", start_ts

async def ws_handler(t_up: str, t_down: str):
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    _best_asks = best_asks
    _set       = price_change.set
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                await ws.send(json.dumps({
                    "assets_ids":             [t_up, t_down],
                    "type":                   "market",
                    "custom_feature_enabled": True
                }))
                log_info("WS: Ligado ao order book Polymarket")
                async for raw in ws:
                    items = json.loads(raw)
                    if not isinstance(items, list):
                        items = [items]
                    for item in items:
                        aid = item.get("asset_id")
                        p   = None
                        evt = item.get("event_type")
                        if evt == "book":
                            asks = item.get("asks")
                            if asks:
                                valid = [float(d['price']) for d in asks if float(d['size']) > 0]
                                if valid:
                                    p = min(valid)
                        elif evt == "best_bid_ask":
                            ba = item.get("best_ask")
                            if ba:
                                p = float(ba)
                        if p is not None:
                            if   aid == t_up:   _best_asks['up']   = p
                            elif aid == t_down: _best_asks['down'] = p
                            _set()
        except asyncio.CancelledError:
            break
        except Exception as e:
            log_info(f"WS: Erro — {e} — reconectando em 1s")
            await asyncio.sleep(1)

# =============================================================================
# ========================== LIVE ORDER =======================================
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
            f"LIVE ORDER OK → {side} {token_id[:8]}… @ {price:.4f} "
            f"| Size: {shares:.4f} | OrderID: {response.get('orderID', 'OK')}"
        )
        return True
    except Exception as e:
        log_info(f"LIVE ORDER ERROR: {e}")
        return False

# =============================================================================
# ========================== PRICE BUFFER =====================================
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
        buf = self.buffer
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


# =============================================================================
# ========================== EMPIRICAL KELLY ==================================
# =============================================================================

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
            return fallback, f"Kelly N/A ({n}/{KELLY_MIN_HISTORY}) → {fallback:.1%}"

        arr    = np.array(self.returns)
        mean_r = float(np.mean(arr))
        std_r  = float(np.std(arr))

        if mean_r <= 0:
            return KELLY_MIN_FRACTION, f"Kelly edge negativo → {KELLY_MIN_FRACTION:.1%}"

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
            ruin_note = " [halved]"

        f_final = max(KELLY_MIN_FRACTION, min(KELLY_MAX_FRACTION, f_empirical))
        log_str = f"Kelly f={f_final:.3f}{ruin_note} | n={n}"
        return f_final, log_str


# =============================================================================
# ========================== AVELLANEDA-STOIKOV + VPIN ========================
# =============================================================================

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
            return None, f"AS WITHDRAW | VPIN={vpin_val:.2f}"

        widen    = AS_SPREAD_WIDEN_FACTOR if vpin_val >= AS_VPIN_WIDEN else 1.0
        min_edge = max(AS_MIN_EDGE_C, half_d * widen)
        log_str  = f"AS | VPIN={vpin_val:.2f} min_edge={min_edge:.2f}c"
        return min_edge, log_str


# =============================================================================
# ========================== LOGIC LOOP =======================================
# =============================================================================

async def logic_loop(m_start: float, m_end: float, meta: dict, r_mult: float):
    global bankroll, daily_profit, kelly, as_model, accumulated_loss

    active_trades = []
    state         = {'c1': {}, 'c2': {}}
    flags         = {
        's35': False, 'v30': False, 'd29': False,
        's25': False, 'v20': False, 'd19': False
    }

    # [v0.34.0] Calcular risco com martingale + recovery das perdas
    # Fórmula: (RISK_PER_TRADE × multiplier) + (MARTINGALE_RECOVERY × accumulated_loss / bankroll)
    base_risk      = RISK_PER_TRADE * r_mult
    recovery_risk  = (MARTINGALE_RECOVERY * accumulated_loss / bankroll) if bankroll > 0 else 0.0
    
    eff_risk_per_trade = min(base_risk + recovery_risk, MAX_RISK_PERCENT)
    eff_eighty_risk    = min(EIGHTY_RISK * r_mult + recovery_risk, MAX_RISK_PERCENT)
    eff_peg_risk       = min(PEG_ARBIT_RISK * r_mult + recovery_risk, MAX_RISK_PERCENT)
    
    # Log do martingale com recovery
    if r_mult > 1.0 or accumulated_loss > 0:
        log_info(f"MARTINGALE: x{r_mult:.0f} + Recovery {MARTINGALE_RECOVERY:.0%} de ${accumulated_loss:.2f} = {eff_risk_per_trade:.1%} risco efectivo")

    _EIGHTY_MIN_EFF_C     = EIGHTY_MIN_EFF_C
    _EIGHTY_MAX_EFF_C     = EIGHTY_MAX_EFF_C
    _EIGHTY_MIN_TICKS     = EIGHTY_MIN_TICKS
    _EIGHTY_PEG_MIN_C     = EIGHTY_PEG_MIN_C
    _EIGHTY_BUY_COOLDOWN  = EIGHTY_BUY_COOLDOWN
    _EIGHTY_VOL_WINDOW_S  = EIGHTY_VOL_WINDOW_S
    _EIGHTY_VOL_MAX_C     = EIGHTY_VOL_MAX_C
    _EIGHTY_VOL_COOLDOWN_S= EIGHTY_VOL_COOLDOWN_S
    _EIGHTY_CUTOFF_S      = EIGHTY_CUTOFF_S
    _EIGHTY_START_REM_S   = EIGHTY_START_REM_S
    _EIGHTY_WHEN_CV0      = EIGHTY_WHEN_CUTOFF_0_VOLT
    _EIGHTY_DELTA_VOL_T   = EIGHTY_DELTA_VOL_TIME_S
    _EIGHTY_DELTA_VOL_C   = EIGHTY_DELTA_VOL_RISE_C
    _EIGHTY_TARGET_C      = EIGHTY_TARGET_C
    _ASK_SPREAD           = ASK_SPREAD
    _PEG_RANGE_MIN        = PEG_ARBIT_RANGE[0]
    _PEG_RANGE_MAX        = PEG_ARBIT_RANGE[1]

    eighty_seen_levels        = {'UP': set(), 'DOWN': set()}
    eighty_tick_count         = {'UP': 0,     'DOWN': 0}
    eighty_last_buy           = {'UP': 0.0,   'DOWN': 0.0}
    eighty_first_tick_t       = {'UP': None,  'DOWN': None}
    eighty_eff_min            = {'UP': None,  'DOWN': None}
    eighty_eff_max            = {'UP': None,  'DOWN': None}
    eighty_cutoff_logged      = False
    eighty_started_logged     = False
    eighty_price_buffer       = {
        'UP':   PriceBuffer(max_age_seconds=15.0),
        'DOWN': PriceBuffer(max_age_seconds=15.0)
    }
    eighty_vol_cooldown_until = {'UP': 0.0, 'DOWN': 0.0}

    peg_arbit_count = 0
    last_peg_time   = 0.0

    mult_tag = f" [MARTINGALE x{r_mult:.0f}]" if r_mult > 1.0 else ""
    mods = []
    if EIGHTY_ACTIVE:    mods.append(f"EIGHTY({_EIGHTY_START_REM_S}s→{_EIGHTY_CUTOFF_S}s)")
    if CICLO_30S_ACTIVE: mods.append("CICLO_30S")
    if CICLO_20S_ACTIVE: mods.append("CICLO_20S")
    if PEG_ARBIT_ACTIVE: mods.append(f"PEG_ARBIT(range {_PEG_RANGE_MIN}-{_PEG_RANGE_MAX}c)")

    log_sep()
    log_info(f"Market: {meta['slug']} | LIVE: {LIVE_TRADING}")
    log_info(f"Bank: ${bankroll:.2f} | Profit: ${daily_profit:.2f}{mult_tag}")
    log_info(f"Módulos: {' | '.join(mods)}")
    log_info(f"Risk limits: EIGHTY={eff_eighty_risk:.1%} PEG={eff_peg_risk:.1%} (max {MAX_RISK_PERCENT:.0%})")
    log_sep()

    def pct_banca(invested: float) -> str:
        base = bankroll + invested
        return f"{invested / base * 100:.0f}% banca" if base else "—"

    # ── open_trade (para EIGHTY e CICLOS) ─────────────────────────────────────
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

        ask   = nom + _ASK_SPREAD
        _fee  = fee_rate(ask)
        eff   = ask / (1.0 - _fee)

        if fixed_shares is not None:
            shares   = fixed_shares
            invested = shares * ask / (1.0 - _fee)
        elif fixed_invest is not None:
            invested = fixed_invest
            shares   = (invested / ask) * (1.0 - _fee)
        else:
            invested = bankroll * risk
            shares   = (invested / ask) * (1.0 - _fee)

        if trade_type.startswith('CICLO'):
            target = CYCLE_TARGET_C / 100.0 if CYCLE_TARGET_C > 0 else None
        elif trade_type == 'EIGHTY':
            target = _EIGHTY_TARGET_C / 100.0 if _EIGHTY_TARGET_C > 0 else None
        elif trade_type == 'PEG_ARBIT':
            target = PEG_ARBIT_TARGET_C / 100.0 if PEG_ARBIT_TARGET_C > 0 else None
        else:
            target = min(0.99, eff * TARGET_MULTIPLIER)

        bankroll -= invested
        pct       = pct_banca(invested)
        buy_fee   = _fee * 100
        peg_str   = f" | *** PEG_Eff: {peg_val:.3f} ({peg_val*100:.1f}c) ***" if peg_val else ""
        extra     = f" | {extra_log}" if extra_log else ""
        kelly_str = f" | {kelly_log}" if kelly_log else ""

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
            f"Remaining: {rstr} | {side} @ {fc(nom)} | Ask: {fc(ask)} | Eff: {fc(eff)}"
            f"{peg_str} | Inv: ${invested:.2f} ({pct}) | Shares: {shares:.4f}"
            f" | Fee: {buy_fee:.2f}%{extra}{kelly_str}"
        )

    # ── close_trade ───────────────────────────────────────────────────────────
    def close_trade(trade: dict, cp: float, reason: str, rstr: str):
        global bankroll
        payout    = sell_payout(trade['shares'], cp)
        pnl       = payout - trade['invested']
        bankroll += payout
        if KELLY_ACTIVE:
            kelly.add_result(trade['invested'], payout)
        if AS_VPIN_ACTIVE:
            as_model.update_inventory(trade['side'], trade['shares'], is_buy=False)
        module = trade['type'].replace('_', ' ')
        log_m(module, 'SELL',
            f"Remaining: {rstr} | {trade['side']} @ {fc(cp)} "
            f"| PnL: ${pnl:+.4f} | Reason: {reason}"
        )

    def eighty_reset(e_side: str, rstr: str, reason: str):
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side]   = 0
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side]      = None
        eighty_eff_max[e_side]      = None
        log_m('EIGHTY', 'RESET', f"Remaining: {rstr} | {e_side} — {reason}")

    def eighty_reset_silent(e_side: str):
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side]   = 0
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side]      = None
        eighty_eff_max[e_side]      = None

    def eighty_activate_vol_cooldown(e_side: str, rstr: str, reason: str):
        eighty_vol_cooldown_until[e_side] = time.time() + _EIGHTY_VOL_COOLDOWN_S
        eighty_reset(e_side, rstr, reason)

    prev_u_p = prev_d_p = None
    _best_asks    = best_asks
    _pc_wait      = price_change.wait
    _pc_clear     = price_change.clear
    _loop_sleep   = LOOP_SLEEP
    _AS_VPIN      = AS_VPIN_ACTIVE
    _PEG_ACTIVE   = PEG_ARBIT_ACTIVE
    _PEG_UNDERPEG = PEG_ARBIT_UNDERPEG_C
    _PEG_MIN_REM  = PEG_ARBIT_MIN_REM
    _PEG_COOLDOWN = PEG_ARBIT_COOLDOWN
    _MAX_PEG      = MAX_PEG_ENTRIES
    _EIGHTY_ACT   = EIGHTY_ACTIVE
    _CICLO30_ACT  = CICLO_30S_ACTIVE
    _CICLO20_ACT  = CICLO_20S_ACTIVE

    while True:
        now = time.time()
        rem = m_end - now

        if rem <= 0:
            u_p = _best_asks.get('up')  or 0.0
            d_p = _best_asks.get('down') or 0.0
            for trade in active_trades[:]:
                cp = u_p if trade['side'] == 'UP' else d_p
                close_trade(trade, cp, "FIM MERCADO", "00:00:000")
                active_trades.remove(trade)
            log_info("Fim de Mercado")
            break

        rstr = get_remaining_str(rem)

        try:
            await asyncio.wait_for(_pc_wait(), timeout=_loop_sleep)
            _pc_clear()
        except asyncio.TimeoutError:
            pass

        u_p = _best_asks.get('up')
        d_p = _best_asks.get('down')
        if u_p is None or d_p is None:
            continue

        if u_p == prev_u_p and d_p == prev_d_p:
            continue

        prev_u_p = u_p
        prev_d_p = d_p

        # [v0.33.0] Calcular PEG baseado no Eff (effective price)
        ask_up   = u_p + _ASK_SPREAD
        ask_down = d_p + _ASK_SPREAD
        eff_up   = effective_entry(ask_up)
        eff_down = effective_entry(ask_down)
        peg_eff  = eff_up + eff_down
        peg_base = u_p + d_p

        underpeg_eff_c = (1.0 - peg_eff) * 100.0
        peg_disp = f" | PEG_Eff: {peg_eff:.3f} ({peg_eff*100:.1f}c)" if peg_eff < 1.0 else ""
        log_raw(f"Remaining: {rstr} | UP: {fc(u_p)} (Eff:{fc(eff_up)}) | DOWN: {fc(d_p)} (Eff:{fc(eff_down)}){peg_disp}")

        if _AS_VPIN:
            mid_p    = (u_p + d_p) * 0.5
            prev_mid = ((prev_u_p or u_p) + (prev_d_p or d_p)) * 0.5
            as_model.add_tick(mid_p, prev_mid)

        as_blocked = False
        min_edge   = AS_MIN_EDGE_C
        if _AS_VPIN:
            q_total  = as_model.inventory_up - as_model.inventory_down
            min_edge, _ = as_model.get_min_edge_c(
                mid_c=(u_p + d_p) * 50.0,
                q=q_total,
                t_remaining=rem
            )
            if min_edge is None:
                as_blocked = True

        # ── 1. PEG ARBITRAGE ─────────────────────────────────────────────────
        if (_PEG_ACTIVE
                and not as_blocked
                and peg_eff < 1.0
                and underpeg_eff_c >= _PEG_UNDERPEG
                and rem > _PEG_MIN_REM
                and peg_arbit_count < _MAX_PEG
                and now - last_peg_time >= _PEG_COOLDOWN):

            eff_up_c   = eff_up * 100.0
            eff_down_c = eff_down * 100.0

            up_in_range   = _PEG_RANGE_MIN <= eff_up_c <= _PEG_RANGE_MAX
            down_in_range = _PEG_RANGE_MIN <= eff_down_c <= _PEG_RANGE_MAX

            if up_in_range and down_in_range:
                budget = bankroll * eff_peg_risk

                if eff_up >= eff_down:
                    shares_to_buy = budget / eff_up
                else:
                    shares_to_buy = budget / eff_down

                invest_up   = shares_to_buy * eff_up
                invest_down = shares_to_buy * eff_down
                total_invest = invest_up + invest_down

                log_m('PEG ARBIT', 'ACTIVE',
                    f"Remaining: {rstr} | PEG_Eff: {peg_eff:.3f} ({peg_eff*100:.1f}c) | "
                    f"UP_Eff: {fc(eff_up)} | DOWN_Eff: {fc(eff_down)} | "
                    f"Shares: {shares_to_buy:.4f} | Total Inv: ${total_invest:.2f}"
                )

                await open_trade('UP', u_p, 'PEG_ARBIT', rstr,
                                 fixed_shares=shares_to_buy, wait_close=True,
                                 peg_val=peg_eff, token_id=meta['up'],
                                 extra_log=f"Shares: {shares_to_buy:.4f}")
                await open_trade('DOWN', d_p, 'PEG_ARBIT', rstr,
                                 fixed_shares=shares_to_buy, wait_close=True,
                                 peg_val=peg_eff, token_id=meta['down'],
                                 extra_log=f"Shares: {shares_to_buy:.4f}")

                peg_arbit_count += 1
                last_peg_time    = now
            else:
                if peg_arbit_count == 0:
                    reasons = []
                    if not up_in_range:
                        reasons.append(f"UP_Eff {eff_up_c:.1f}c fora [{_PEG_RANGE_MIN}-{_PEG_RANGE_MAX}]")
                    if not down_in_range:
                        reasons.append(f"DOWN_Eff {eff_down_c:.1f}c fora [{_PEG_RANGE_MIN}-{_PEG_RANGE_MAX}]")
                    log_m('PEG ARBIT', 'SKIP',
                        f"Remaining: {rstr} | PEG_Eff OK ({peg_eff:.3f}) mas {' | '.join(reasons)}")

        # ── 2. TARGET CHECK ──────────────────────────────────────────────────
        for trade in active_trades[:]:
            if trade.get('target') is None:
                continue
            cp = u_p if trade['side'] == 'UP' else d_p
            if cp and cp >= trade['target']:
                close_trade(trade, cp, "TARGET", rstr)
                active_trades.remove(trade)

        # ── 3. EIGHTY ────────────────────────────────────────────────────────
        if _EIGHTY_ACT:
            if rem > _EIGHTY_START_REM_S:
                pass
            elif rem <= _EIGHTY_CUTOFF_S:
                if not eighty_cutoff_logged:
                    eighty_cutoff_logged = True
                    log_m('EIGHTY', 'CUTOFF', f"Remaining: {rstr} | EIGHTY parado")
            else:
                if not eighty_started_logged:
                    eighty_started_logged = True
                    log_m('EIGHTY', 'START',
                        f"Remaining: {rstr} | EIGHTY activo [{_EIGHTY_START_REM_S}s→{_EIGHTY_CUTOFF_S}s]")

                for e_side, nom in (('UP', u_p), ('DOWN', d_p)):
                    token_id = meta['up'] if e_side == 'UP' else meta['down']
                    skip_vol = (_EIGHTY_CUTOFF_S == 0 and _EIGHTY_WHEN_CV0 > 0 and rem <= _EIGHTY_WHEN_CV0)

                    if not skip_vol and now < eighty_vol_cooldown_until[e_side]:
                        continue
                    if not skip_vol and now - eighty_last_buy[e_side] < _EIGHTY_BUY_COOLDOWN:
                        continue

                    ask   = nom + _ASK_SPREAD
                    _fee  = fee_rate(ask)
                    eff_c = (ask / (1.0 - _fee)) * 100.0

                    eighty_price_buffer[e_side].add(eff_c, now)

                    if as_blocked:
                        continue

                    if not (_EIGHTY_MIN_EFF_C <= eff_c <= _EIGHTY_MAX_EFF_C):
                        if eighty_tick_count[e_side] > 0:
                            eighty_reset(e_side, rstr,
                                f"eff_c {eff_c:.1f}c OUT [{_EIGHTY_MIN_EFF_C}-{_EIGHTY_MAX_EFF_C}]")
                        continue

                    level_key = round(eff_c * 2) / 2
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
                    vol_nok = (elapsed <= _EIGHTY_VOL_WINDOW_S and var_c >= _EIGHTY_VOL_MAX_C)

                    epb  = eighty_price_buffer[e_side]
                    delta_05, valid_05 = epb.get_delta(0.5)
                    delta_10, valid_10 = epb.get_delta(1.0)
                    delta_20, valid_20 = epb.get_delta(2.0)

                    delta_parts = []
                    if valid_05: delta_parts.append(f"Δ0.5s:{delta_05:+.1f}c")
                    if valid_10: delta_parts.append(f"Δ1s:{delta_10:+.1f}c")
                    if valid_20: delta_parts.append(f"Δ2s:{delta_20:+.1f}c")
                    delta_str = " | ".join(delta_parts) if delta_parts else "Δ wait"

                    delta_15, valid_15 = epb.get_delta(_EIGHTY_DELTA_VOL_T)
                    rapid_rise = valid_15 and delta_15 is not None and delta_15 >= _EIGHTY_DELTA_VOL_C

                    delta_ok      = True
                    delta_reason  = ""
                    has_any_delta = valid_05 or valid_10 or valid_20

                    if valid_05 and delta_05 is not None and delta_05 < 0:
                        delta_ok, delta_reason = False, f"Δ0.5s={delta_05:+.1f}c"
                    elif valid_10 and delta_10 is not None and delta_10 < 0:
                        delta_ok, delta_reason = False, f"Δ1s={delta_10:+.1f}c"
                    elif valid_20 and delta_20 is not None and delta_20 < 0:
                        delta_ok, delta_reason = False, f"Δ2s={delta_20:+.1f}c"
                    elif rapid_rise:
                        delta_ok, delta_reason = False, "rapid rise"

                    vol_str = "VOL SKIP" if skip_vol else f"VOL {'NOK' if vol_nok else 'OK'}"
                    delta_status = "↑" if (delta_ok and has_any_delta) else ("↓" if has_any_delta else "—")

                    peg_tick_str = f" | PEG_Eff: {peg_eff:.3f}" if peg_eff * 100.0 <= _EIGHTY_PEG_MIN_C else ""

                    log_m('EIGHTY', 'WATCH',
                        f"Remaining: {rstr} | {e_side} Eff: {fc(eff_c/100)} | {vol_str} | "
                        f"{delta_str} {delta_status}{peg_tick_str} | ticks: {eighty_tick_count[e_side]}/{_EIGHTY_MIN_TICKS}"
                    )

                    if not skip_vol:
                        if vol_nok:
                            eighty_activate_vol_cooldown(e_side, rstr, f"VOL {var_c:.1f}c/{elapsed:.1f}s")
                            continue
                        if rapid_rise:
                            eighty_activate_vol_cooldown(e_side, rstr, "RAPID RISE")
                            continue

                    if eighty_tick_count[e_side] >= _EIGHTY_MIN_TICKS:
                        if peg_eff * 100.0 < _EIGHTY_PEG_MIN_C:
                            eighty_reset(e_side, rstr, f"PEG_Eff {peg_eff*100:.1f}c < {_EIGHTY_PEG_MIN_C:.1f}c")
                            continue
                        if has_any_delta and not delta_ok:
                            eighty_reset(e_side, rstr, f"DELTA NOK {delta_reason}")
                            continue

                        if bankroll > 0:
                            if _AS_VPIN:
                                shares_est = buy_shares_net(bankroll * eff_eighty_risk, nom + _ASK_SPREAD)
                                as_model.update_inventory(e_side, shares_est, is_buy=True)
                            await open_trade(e_side, nom, 'EIGHTY', rstr,
                                             risk=eff_eighty_risk, wait_close=True,
                                             peg_val=peg_eff, token_id=token_id,
                                             extra_log=f"ticks:{eighty_tick_count[e_side]} | {delta_str}")
                            eighty_last_buy[e_side] = now
                            eighty_reset_silent(e_side)

        # ── 4. CICLO 30s ─────────────────────────────────────────────────────
        if _CICLO30_ACT:
            if not flags['s35'] and rem <= CYCLE_30S_SNAPSHOT_REM:
                state['c1']['snap_u'] = u_p
                state['c1']['snap_d'] = d_p
                flags['s35'] = True
                log_m('CICLO 30s', 'SNAP', f"Remaining: {rstr} | UP: {fc(u_p)} DOWN: {fc(d_p)}")

            if flags['s35'] and not flags['v30'] and rem <= CYCLE_30S_VOL_CHECK_REM:
                vol_c = abs(u_p - state['c1']['snap_u']) * 100.0
                flags['v30'] = True
                state['c1']['vol_ok'] = vol_c <= CYCLE_VOL_MAX_C
                log_m('CICLO 30s', 'VOLT', f"Remaining: {rstr} | vol={vol_c:.1f}c | {'OK' if state['c1']['vol_ok'] else 'NOK'}")

            if flags['v30'] and state['c1'].get('vol_ok') and not flags['d29'] and rem <= CYCLE_30S_BUY_REM:
                flags['d29'] = True
                for e_side, nom, tid in (('UP', u_p, meta['up']), ('DOWN', d_p, meta['down'])):
                    price_c = nom * 100.0
                    peg_c   = peg_base * 100.0
                    if CYCLE_PRICE_MIN_C <= price_c <= CYCLE_PRICE_MAX_C and peg_c >= CYCLE_PEG_MIN_C:
                        await open_trade(e_side, nom, 'CICLO_30s', rstr,
                                         risk=eff_risk_per_trade, wait_close=True, peg_val=peg_eff, token_id=tid)
                    else:
                        log_m('CICLO 30s', 'SKIP', f"Remaining: {rstr} | {e_side} fora do range")

        # ── 5. CICLO 20s ─────────────────────────────────────────────────────
        if _CICLO20_ACT:
            if not flags['s25'] and rem <= CYCLE_20S_SNAPSHOT_REM:
                state['c2']['snap_u'] = u_p
                state['c2']['snap_d'] = d_p
                flags['s25'] = True
                log_m('CICLO 20s', 'SNAP', f"Remaining: {rstr} | UP: {fc(u_p)} DOWN: {fc(d_p)}")

            if flags['s25'] and not flags['v20'] and rem <= CYCLE_20S_VOL_CHECK_REM:
                vol_c = abs(u_p - state['c2']['snap_u']) * 100.0
                flags['v20'] = True
                state['c2']['vol_ok'] = vol_c <= CYCLE_VOL_MAX_C
                log_m('CICLO 20s', 'VOLT', f"Remaining: {rstr} | vol={vol_c:.1f}c | {'OK' if state['c2']['vol_ok'] else 'NOK'}")

            if flags['v20'] and state['c2'].get('vol_ok') and not flags['d19'] and rem <= CYCLE_20S_BUY_REM:
                flags['d19'] = True
                for e_side, nom, tid in (('UP', u_p, meta['up']), ('DOWN', d_p, meta['down'])):
                    price_c = nom * 100.0
                    peg_c   = peg_base * 100.0
                    if CYCLE_PRICE_MIN_C <= price_c <= CYCLE_PRICE_MAX_C and peg_c >= CYCLE_PEG_MIN_C:
                        await open_trade(e_side, nom, 'CICLO_20s', rstr,
                                         risk=eff_risk_per_trade, wait_close=True, peg_val=peg_eff, token_id=tid)
                    else:
                        log_m('CICLO 20s', 'SKIP', f"Remaining: {rstr} | {e_side} fora do range")


# =============================================================================
# ============================= MAIN ==========================================
# =============================================================================

async def main():
    global daily_profit, last_day, price_change, bankroll, risk_multiplier, kelly, as_model, accumulated_loss

    kelly    = EmpiricalKelly()
    as_model = AvellanedaStoikov()
    accumulated_loss = 0.0  # [v0.34.0] Reset no início
    
    log_info("BOT INICIADO v0.34.0")
    log_info(f"LIVE: {LIVE_TRADING}")
    log_info(f"PEG_ARBIT: Range {PEG_ARBIT_RANGE[0]}-{PEG_ARBIT_RANGE[1]}c | PEG calculado por Eff")
    log_info(f"EIGHTY: {EIGHTY_RISK:.0%} da banca | Martingale max {MAX_RISK_PERCENT:.0%} + Recovery {MARTINGALE_RECOVERY:.0%}")

    while True:
        slug, start_ts = get_current_slug()
        meta = fetch_metadata(slug)
        if not meta:
            await asyncio.sleep(1)
            continue

        market_day = datetime.fromtimestamp(start_ts).date()
        if last_day is None or market_day != last_day:
            daily_profit    = 0.0
            bankroll        = BANKROLL_INIT
            risk_multiplier = 1.0
            last_day        = market_day
            kelly           = EmpiricalKelly()
            as_model        = AvellanedaStoikov()
            log_info(f"NOVO DIA {market_day} — Banca reset ${BANKROLL_INIT:.2f}")
            
            # Recriar modelos para limpar cache diário (opcional)
            kelly            = EmpiricalKelly()
            as_model         = AvellanedaStoikov()
            
            log_info(f"NOVO DIA {market_day} | Mantendo banca atual: ${bankroll:.2f}")

        best_asks['up'] = best_asks['down'] = None
        price_change.clear()

        ws_task = asyncio.create_task(ws_handler(meta['up'], meta['down']))
        await asyncio.sleep(1.0)

        if best_asks['up'] is not None:
            pre_bank = bankroll
            await logic_loop(start_ts, start_ts + 300, meta, risk_multiplier)

            profit_this  = bankroll - pre_bank
            daily_profit += profit_this
            pnl_pct      = (profit_this / pre_bank * 100) if pre_bank > 0 else 0
            daily_pct    = (daily_profit / BANKROLL_INIT * 100) if BANKROLL_INIT > 0 else 0

            log_sep()

            # [v0.33.0] Formatação do PnL sem sinal quando é zero
            if profit_this == 0.0:
                pnl_str = "PnL: $0.0000 (0.00%)"
            else:
                pnl_str = f"PnL: ${profit_this:+.4f} ({pnl_pct:+.2f}%)"

            if profit_this < 0:
                # Perda -> acumula perda e dobra o martingale
                accumulated_loss += abs(profit_this)
                risk_multiplier = min(risk_multiplier * 2.0, MAX_RISK_MULTIPLIER)
                recovery_amount = MARTINGALE_RECOVERY * accumulated_loss
                log_info(
                    f"ROUND | {pnl_str} | MARTINGALE → x{risk_multiplier:.0f} "
                    f"| Perdas acumuladas: ${accumulated_loss:.2f} (+${recovery_amount:.2f} recovery)"
                )
            elif profit_this == 0.0 and risk_multiplier > 1.0:
                # Round sem trades com martingale activo -> mantém
                recovery_amount = MARTINGALE_RECOVERY * accumulated_loss
                log_info(
                    f"ROUND | {pnl_str} | MARTINGALE → x{risk_multiplier:.0f} "
                    f"| Perdas acumuladas: ${accumulated_loss:.2f} (+${recovery_amount:.2f} recovery)"
                )
            else:
                # Lucro -> reset do martingale E das perdas acumuladas
                if accumulated_loss > 0:
                    log_info(f"ROUND | {pnl_str} | RECOVERY COMPLETE — recuperados ${accumulated_loss:.2f}")
                else:
                    log_info(f"ROUND | {pnl_str}")
                risk_multiplier = 1.0
                accumulated_loss = 0.0  # Reset das perdas acumuladas

            # Total também sem sinal se zero
            if daily_profit == 0.0:
                total_str = "PnL: $0.0000 (0.00%)"
            else:
                total_str = f"PnL: ${daily_profit:+.4f} ({daily_pct:+.2f}%)"

            log_info(f"TOTAL | {total_str} | Bank: ${bankroll:.2f} | Uptime: {get_uptime_str()}")
            log_sep()
        else:
            log_info("Sem preços — a saltar")

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
        log_info("BOT PARADO")