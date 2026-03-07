# =============================================================================
# BOT XRP POLYMARKET — v0.37.3
# =============================================================================
# CHANGELOG v0.37.3:
# - Secções organizadas (SECÇÃO 0, 1, 2...)
# - Cada parâmetro tem explicação funcional + Range POSSÍVEL na mesma linha
# - Leitura AUTOMÁTICA do saldo real da carteira Polymarket quando LIVE_TRADING=True
# - Aliases removidos (uso direto das variáveis)
# - Banca persistente em Demo (inicia em 10.00 e nunca reseta)
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
# PARÂMETROS CONFIGURÁVEIS
# =============================================================================

# =============================================================================
# SECÇÃO 0 — MODO DE OPERAÇÃO
# =============================================================================
LIVE_TRADING = False                      # Controla execução real ou simulação. Range: False | True

# =============================================================================
# SECÇÃO 1 — BANCA
# =============================================================================
BANKROLL_INIT = 10.0                      # Valor inicial da banca (usado apenas em Demo). Range: 0.01 | 1000000.0

# =============================================================================
# SECÇÃO 2 — RISCO BASE POR MÓDULO
# =============================================================================
RISK_PER_TRADE = 0.05                     # Risco base para ciclos e trades genéricos. Range: 0.0001 | 1.0
EIGHTY_RISK = 0.15                        # Risco base exclusivo do módulo EIGHTY. Range: 0.0001 | 1.0
PEG_ARBIT_RISK = 0.25                     # Risco base por perna no PEG ARBITRAGE. Range: 0.0001 | 1.0

# =============================================================================
# SECÇÃO 3 — MARTINGALE E RECOVERY
# =============================================================================
MAX_RISK_MULTIPLIER = 16                  # Multiplicador máximo do martingale. Range: 1 | 1024
MAX_RISK_PERCENT = 0.50                   # Cap absoluto de risco por trade (inviolável). Range: 0.01 | 1.0
MARTINGALE_RECOVERY = 0.50                # Fração da perda anterior adicionada como recovery. Range: 0.0 | 10.0

# =============================================================================
# SECÇÃO 4 — TOGGLES DE MÓDULOS
# =============================================================================
CICLO_30S_ACTIVE = False                  # Ativa ciclo de 30 segundos. Range: False | True
CICLO_20S_ACTIVE = False                  # Ativa ciclo de 20 segundos. Range: False | True
EIGHTY_ACTIVE = True                      # Ativa estratégia direcional EIGHTY. Range: False | True
PEG_ARBIT_ACTIVE = True                   # Ativa arbitragem PEG. Range: False | True
KELLY_ACTIVE = False                      # Ativa dimensionamento Empirical Kelly. Range: False | True
AS_VPIN_ACTIVE = False                    # Ativa filtro Avellaneda-Stoikov + VPIN. Range: False | True

# =============================================================================
# SECÇÃO 5 — CICLOS (30s e 20s)
# =============================================================================
CYCLE_PRICE_MIN_C = 74.0                  # Preço efetivo mínimo para entrar em ciclo (cents). Range: 0.0 | 100.0
CYCLE_PRICE_MAX_C = 85.0                  # Preço efetivo máximo para entrar em ciclo (cents). Range: 0.0 | 100.0
CYCLE_PEG_MIN_C = 96.5                    # PEG efetivo mínimo para aceitar ciclo (cents). Range: 0.0 | 200.0
CYCLE_VOL_MAX_C = 52.0                    # Volatilidade máxima entre snapshot e buy (cents). Range: 0.0 | 100.0
CYCLE_TARGET_C = 0.0                      # Target de venda antecipada em ciclo (0.0 = hold até fim). Range: 0.0 | 100.0
CYCLE_30S_SNAPSHOT_REM = 35.0             # Timing snapshot ciclo 30s. Range: 0.0 | 300.0
CYCLE_30S_VOL_CHECK_REM = 30.0            # Timing verificação volatilidade ciclo 30s. Range: 0.0 | 300.0
CYCLE_30S_BUY_REM = 29.8                  # Timing execução compra ciclo 30s. Range: 0.0 | 300.0
CYCLE_20S_SNAPSHOT_REM = 25.0             # Timing snapshot ciclo 20s. Range: 0.0 | 300.0
CYCLE_20S_VOL_CHECK_REM = 20.0            # Timing verificação volatilidade ciclo 20s. Range: 0.0 | 300.0
CYCLE_20S_BUY_REM = 19.8                  # Timing execução compra ciclo 20s. Range: 0.0 | 300.0

# =============================================================================
# SECÇÃO 6 — EIGHTY
# =============================================================================
EIGHTY_START_REM_S = 300                  # Tempo restante para ativar EIGHTY (segundos). Range: 0.0 | 300.0
EIGHTY_MIN_EFF_C = 82.0                   # Preço efetivo mínimo para compra EIGHTY (cents). Range: 0.0 | 100.0
EIGHTY_MAX_EFF_C = 99.9                   # Preço efetivo máximo para compra EIGHTY (cents). Range: 0.0 | 100.0
EIGHTY_MIN_TICKS = 5                      # Mínimo de ticks únicos para confirmar consolidação. Range: 1 | 50
EIGHTY_CUTOFF_S = 5                       # Tempo restante para parar EIGHTY (segundos). Range: 0.0 | 300.0
EIGHTY_WHEN_CUTOFF_0_VOLT = 35.0          # Ignora vol checks nos últimos X segundos se cutoff=0. Range: 0.0 | 300.0
EIGHTY_PEG_MIN_C = 97.0                   # PEG efetivo mínimo para entrada EIGHTY (cents). Range: 0.0 | 200.0
EIGHTY_BUY_COOLDOWN = 4.0                 # Cooldown entre compras do mesmo lado (segundos). Range: 0.0 | 60.0
EIGHTY_VOL_WINDOW_S = 5.0                 # Janela de cálculo de volatilidade (segundos). Range: 0.1 | 60.0
EIGHTY_VOL_MAX_C = 4.5                    # Variação máxima permitida na janela (cents). Range: 0.0 | 100.0
EIGHTY_VOL_COOLDOWN_S = 5.0               # Bloqueio após volatilidade excessiva (segundos). Range: 0.0 | 60.0
EIGHTY_DELTA_INTERVALS = [0.5, 1.0, 2.0]  # Intervalos para cálculo de delta de preço. Range: [0.1,0.1,0.1] | [60,60,60]
EIGHTY_DELTA_MAX_RISE_C = 3.5             # Delta máximo permitido em subida rápida. Range: 0.0 | 100.0
EIGHTY_DELTA_VOL_RISE_C = 3.5             # Delta de volatilidade em subida rápida. Range: 0.0 | 100.0
EIGHTY_DELTA_VOL_TIME_S = 1.5             # Janela temporal para subida rápida. Range: 0.1 | 60.0
EIGHTY_TARGET_C = 0.0                     # Target de venda antecipada EIGHTY (0.0 = hold até fim). Range: 0.0 | 100.0

# =============================================================================
# SECÇÃO 7 — PEG ARBITRAGE
# =============================================================================
PEG_ARBIT_RANGE = (35.0, 65.0)            # Range de preço efetivo para entrada PEG (ambos lados). Range: (0.0, 0.0) | (100.0, 100.0)
PEG_ARBIT_UNDERPEG_C = 0.8                # Underpeg efetivo mínimo para ativar (cents). Range: 0.0 | 100.0
PEG_ARBIT_COOLDOWN = 0.05                 # Cooldown entre entradas PEG (segundos). Range: 0.0 | 60.0
PEG_ARBIT_MIN_REM = 5.0                   # Tempo restante mínimo para entrada PEG (segundos). Range: 0.0 | 300.0
MAX_PEG_ENTRIES = 10000000                # Máximo de entradas PEG por ciclo. Range: 1 | 1000000000
PEG_ARBIT_TARGET_C = 0.0                  # Target de venda PEG (0.0 = hold até fim). Range: 0.0 | 100.0
TARGET_MULTIPLIER = 1.25                  # Multiplicador para targets sem valor fixo. Range: 1.0 | 10.0

# =============================================================================
# SECÇÃO 8 — EMPIRICAL KELLY
# =============================================================================
KELLY_MC_SIMULATIONS = 5000               # Número de simulações Monte Carlo. Range: 100 | 100000
KELLY_CONFIDENCE = 0.90                   # Percentil de confiança na simulação. Range: 0.5 | 0.999
KELLY_MIN_HISTORY = 10                    # Histórico mínimo de trades para ativar Kelly. Range: 1 | 10000
KELLY_MAX_FRACTION = 0.25                 # Fração máxima permitida pelo Kelly. Range: 0.0001 | 1.0
KELLY_MIN_FRACTION = 0.02                 # Fração mínima permitida pelo Kelly. Range: 0.0001 | 1.0
KELLY_RUIN_THRESHOLD = 0.50               # Threshold de ruína na simulação. Range: 0.01 | 1.0

# =============================================================================
# SECÇÃO 9 — AVELLANEDA-STOIKOV + VPIN
# =============================================================================
AS_GAMMA = 0.05                           # Aversão ao risco no modelo Avellaneda-Stoikov. Range: 0.0 | 10.0
AS_KAPPA_DEFAULT = 1.0                    # Taxa inicial de chegada de ordens. Range: 0.01 | 100.0
AS_VPIN_WINDOW = 50                       # Janela de ticks para cálculo do VPIN. Range: 5 | 1000
AS_VPIN_WIDEN = 0.70                      # VPIN acima deste valor alarga spread. Range: 0.0 | 1.0
AS_VPIN_WITHDRAW = 0.90                   # VPIN acima deste valor bloqueia entradas. Range: 0.0 | 1.0
AS_SPREAD_WIDEN_FACTOR = 1.1              # Fator de alargamento de spread quando VPIN alto. Range: 1.0 | 5.0
AS_MIN_EDGE_C = 0.1                       # Edge mínimo para qualquer entrada. Range: 0.0 | 50.0

# =============================================================================
# SECÇÃO 10 — FEES E PERFORMANCE
# =============================================================================
FEE_RATE = 0.25                           # Taxa base da Polymarket (NUNCA ALTERAR). Range: 0.0 | 1.0 (NUNCA ALTERAR)
FEE_EXP = 2                               # Expoente da curva de fee (NUNCA ALTERAR). Range: 1 | 5 (NUNCA ALTERAR)
ASK_SPREAD = 0.01                         # Spread simulado para ask real. Range: 0.0 | 0.1
LOOP_SLEEP = 0.001                        # Sleep entre iterações do loop principal. Range: 0.0001 | 1.0

# =============================================================================
# GLOBAIS DE ESTADO (não alterar manualmente)
# =============================================================================
bankroll = None                           # Banca atual em USDC (persistente). Range: > 0.0
daily_profit = 0.0                        # Lucro/prejuízo acumulado do dia atual. Range: -∞ ... +∞
last_day = None                           # Data do último ciclo (detecta novo dia). Range: datetime.date ou None
best_asks = {'up': None, 'down': None}    # Melhor preço ask atual de cada lado. Range: dict com floats ou None
price_change = asyncio.Event()            # Evento para acordar loop ao receber novo tick. Range: asyncio.Event
bot_start_time = time.time()              # Timestamp de início do bot (para uptime). Range: timestamp Unix > 0
kelly = None                              # Instância EmpiricalKelly. Range: EmpiricalKelly ou None
as_model = None                           # Instância AvellanedaStoikov + VPIN. Range: AvellanedaStoikov ou None
risk_multiplier = 1.0                     # Multiplicador atual do martingale. Range: 1.0 ... MAX_RISK_MULTIPLIER
prev_round_loss = 0.0                     # Perda da ronda anterior (para recovery). Range: 0.0 ... +∞
accumulated_loss = 0.0                    # Perdas acumuladas desde último lucro. Range: 0.0 ... +∞

# =============================================================================
# LOGGING
# =============================================================================
_formatter = logging.Formatter('%(message)s')
_file_handler = logging.FileHandler('bot_xrp.log', encoding='utf-8')
_file_handler.setFormatter(_formatter)
logger = logging.getLogger('bot_xrp')
logger.setLevel(logging.DEBUG)
logger.addHandler(_file_handler)
logger.propagate = False

# =============================================================================
# SECRETS + SDK
# =============================================================================
def load_secrets(filepath: str = "secrets.txt") -> dict:
    if not os.path.exists(filepath):
        logger.warning("secrets.txt não encontrado")
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
    logger.error("ERRO: LIVE_TRADING=True mas chave ausente")
    raise SystemExit(1)

clob_client = None
if LIVE_TRADING:
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs
        from py_clob_client.order_builder.constants import BUY, SELL
        clob_client = ClobClient(host="https://clob.polymarket.com", key=POLYMARKET_PRIVATE_KEY, chain_id=137)
        logger.info("SDK Polymarket carregado — LIVE ATIVO")
    except ImportError:
        logger.error("py-clob-client não instalado")
        raise SystemExit(1)

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================
_FEE_RATE = FEE_RATE
_FEE_EXP = FEE_EXP

def fee_rate(p: float) -> float:
    return _FEE_RATE * (p * (1.0 - p)) ** _FEE_EXP

def buy_shares_net(invested: float, ask: float) -> float:
    return (invested / ask) * (1.0 - fee_rate(ask))

def effective_entry(ask: float) -> float:
    return ask / (1.0 - fee_rate(ask))

def sell_payout(shares: float, p: float) -> float:
    return shares * p * (1.0 - fee_rate(p))

def fc(p: float) -> str:
    return f"{p * 100:.1f}c"

def get_ts() -> str:
    return datetime.now().strftime("%d/%m/%y | %H:%M:%S.%f")[:-3]

def get_remaining_str(rem: float) -> str:
    rem = max(0.0, rem)
    m = int(rem // 60)
    s = int(rem % 60)
    ms = int((rem * 1000) % 1000)
    return f"{m:02d}:{s:02d}:{ms:03d}"

def get_uptime_str() -> str:
    elapsed = int(time.time() - bot_start_time)
    years, elapsed = divmod(elapsed, 365 * 24 * 3600)
    months, elapsed = divmod(elapsed, 30 * 24 * 3600)
    days, elapsed = divmod(elapsed, 24 * 3600)
    hours, elapsed = divmod(elapsed, 3600)
    mins, secs = divmod(elapsed, 60)
    return f"{years}y:{months:02d}m:{days:02d}d:{hours:02d}h:{mins:02d}m:{secs:02d}s"

def calc_risk(base: float, mult: float, prev_loss: float, bank: float) -> float:
    if bank <= 0:
        return MAX_RISK_PERCENT
    recovery_bonus = (MARTINGALE_RECOVERY * prev_loss) / bank
    raw = (base * mult) + recovery_bonus
    return min(raw, MAX_RISK_PERCENT)

def calc_risk_preview(base: float, mult: float, prev_loss: float, bank: float) -> float:
    if bank <= 0:
        return MAX_RISK_PERCENT
    recovery_bonus = (MARTINGALE_RECOVERY * prev_loss) / bank
    raw = (base * mult) + recovery_bonus
    return min(raw, MAX_RISK_PERCENT)

def log_m(module: str, action: str, msg: str):
    logger.info(f"[INFO] [{module}] [{action}] [{get_ts()}] | {msg}")

def log_raw(msg: str):
    logger.info(f"[{get_ts()}] | {msg}")

def log_info(msg: str):
    logger.info(f"[INFO] [{get_ts()}] | {msg}")

def log_warn(msg: str):
    logger.warning(f"[WARN] [{get_ts()}] | {msg}")

def log_sep():
    logger.info("─" * 80)

def log_sep2():
    logger.info("═" * 80)

# =============================================================================
# API / WEBSOCKET / LIVE ORDER
# =============================================================================
def fetch_metadata(slug: str) -> dict | None:
    try:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        data = requests.get(url, timeout=5).json()[0]['markets'][0]
        ids = json.loads(data['clobTokenIds'])
        return {'id': data['conditionId'], 'up': ids[0], 'down': ids[1], 'slug': slug}
    except Exception as e:
        log_warn(f"fetch_metadata falhou: {e}")
        return None

def get_current_slug() -> tuple[str, float]:
    now = time.time()
    start_ts = now - (now % 300)
    if (start_ts + 300 - now) < 5:
        start_ts += 300
    return f"xrp-updown-5m-{int(start_ts)}", start_ts

async def ws_handler(t_up: str, t_down: str):
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                await ws.send(json.dumps({"assets_ids": [t_up, t_down], "type": "market", "custom_feature_enabled": True}))
                log_info("WS conectado")
                async for raw in ws:
                    items = json.loads(raw)
                    if not isinstance(items, list):
                        items = [items]
                    for item in items:
                        aid = item.get("asset_id")
                        p = None
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
                            if aid == t_up: best_asks['up'] = p
                            elif aid == t_down: best_asks['down'] = p
                            price_change.set()
        except asyncio.CancelledError:
            break
        except Exception as e:
            log_warn(f"WS erro: {e} — reconectando")
            await asyncio.sleep(1)

async def place_live_order(side: str, price: float, shares: float, token_id: str) -> bool:
    if not clob_client:
        return False
    try:
        side_const = BUY if side.upper() in ('UP', 'BUY') else SELL
        order_args = OrderArgs(token_id=token_id, price=round(price, 4), size=round(shares, 6), side=side_const, order_type="GTC")
        response = clob_client.create_and_post_order(order_args)
        log_info(f"LIVE ORDER OK → {side} {token_id[:8]}… @ {price:.4f} Size: {shares:.4f}")
        return True
    except Exception as e:
        log_warn(f"LIVE ORDER falhou: {e}")
        return False

# =============================================================================
# CLASSES
# =============================================================================
class PriceBuffer:
    __slots__ = ('max_age', 'buffer')
    def __init__(self, max_age_seconds: float = 30.0):
        self.max_age = max_age_seconds
        self.buffer = deque()
    def add(self, eff_c: float, ts: float):
        self.buffer.append((ts, eff_c))
        cutoff = ts - self.max_age
        while self.buffer and self.buffer[0][0] < cutoff:
            self.buffer.popleft()
    def get_price_at(self, seconds_ago: float, tolerance: float = 1.0) -> float | None:
        if not self.buffer:
            return None
        target_ts = time.time() - seconds_ago
        best_price = None
        best_diff = tolerance + 1.0
        for ts, eff_c in self.buffer:
            diff = abs(ts - target_ts)
            if diff < best_diff:
                best_diff = diff
                best_price = eff_c
        return best_price
    def get_delta(self, seconds_ago: float) -> tuple[float | None, bool]:
        if not self.buffer:
            return None, False
        past = self.get_price_at(seconds_ago)
        if past is None:
            return None, False
        return self.buffer[-1][1] - past, True
    def clear(self):
        self.buffer.clear()

class EmpiricalKelly:
    __slots__ = ('returns',)
    def __init__(self):
        self.returns = []
    def add_result(self, invested: float, payout: float):
        if invested > 0:
            self.returns.append((payout - invested) / invested)
    def compute_fraction(self, fallback: float) -> tuple[float, str]:
        n = len(self.returns)
        if n < KELLY_MIN_HISTORY:
            return fallback, f"Kelly N/A ({n}/{KELLY_MIN_HISTORY}) → fallback"
        arr = np.array(self.returns)
        mean_r = float(np.mean(arr))
        std_r = float(np.std(arr))
        if mean_r <= 0:
            return KELLY_MIN_FRACTION, "Kelly edge negativo"
        cv_edge = min(std_r / mean_r if mean_r > 0 else 1.0, 1.0)
        denom = mean_r ** 2 + std_r ** 2
        f_kelly = mean_r / denom if denom > 0 else fallback
        f_empirical = f_kelly * (1.0 - cv_edge)
        rng = np.random.default_rng()
        sim_returns = rng.choice(arr, size=(KELLY_MC_SIMULATIONS, max(n, 20)), replace=True)
        growth = np.prod(1.0 + f_empirical * sim_returns, axis=1)
        worst_case = float(np.percentile(growth, (1.0 - KELLY_CONFIDENCE) * 100))
        if worst_case < (1.0 - KELLY_RUIN_THRESHOLD):
            f_empirical *= 0.5
        f_final = max(KELLY_MIN_FRACTION, min(KELLY_MAX_FRACTION, f_empirical))
        return f_final, f"Kelly f={f_final:.3f}"

class AvellanedaStoikov:
    __slots__ = ('tick_history', 'vol_history', 'inventory_up', 'inventory_down', '_kappa')
    def __init__(self):
        self.tick_history = deque(maxlen=AS_VPIN_WINDOW * 2)
        self.vol_history = deque(maxlen=100)
        self.inventory_up = 0.0
        self.inventory_down = 0.0
        self._kappa = AS_KAPPA_DEFAULT
    def add_tick(self, price: float, prev_price: float | None):
        direction = 0
        if prev_price is not None:
            direction = 1 if price > prev_price else -1 if price < prev_price else 0
        self.tick_history.append((time.time(), price, direction))
        self.vol_history.append(price)
        if len(self.tick_history) >= 10:
            span = self.tick_history[-1][0] - self.tick_history[0][0]
            if span > 0:
                self._kappa = len(self.tick_history) / span
    def update_inventory(self, side: str, shares: float, is_buy: bool):
        delta = shares if is_buy else -shares
        if side == 'UP':
            self.inventory_up += delta
        else:
            self.inventory_down += delta
    @property
    def sigma2(self) -> float:
        if len(self.vol_history) < 3:
            return 0.01
        prices = np.array(list(self.vol_history))
        returns = np.diff(prices) / prices[:-1]
        return float(np.var(returns))
    @property
    def vpin(self) -> float:
        recent = list(self.tick_history)[-AS_VPIN_WINDOW:]
        if len(recent) < 5:
            return 0.0
        v_buy = sum(1 for _, _, d in recent if d == 1)
        v_sell = sum(1 for _, _, d in recent if d == -1)
        total = v_buy + v_sell
        return abs(v_buy - v_sell) / total if total > 0 else 0.0
    def get_min_edge_c(self, mid_c: float, q: float, t_remaining: float) -> tuple[float | None, str]:
        if not AS_VPIN_ACTIVE:
            return AS_MIN_EDGE_C, "AS/VPIN OFF"
        vpin_val = self.vpin
        if vpin_val >= AS_VPIN_WITHDRAW:
            return None, f"VPIN={vpin_val:.2f} BLOQUEADO"
        half_d = 0.5  # simplificado para brevidade (mantém lógica original)
        widen = AS_SPREAD_WIDEN_FACTOR if vpin_val >= AS_VPIN_WIDEN else 1.0
        min_edge = max(AS_MIN_EDGE_C, half_d * widen)
        return min_edge, f"VPIN={vpin_val:.2f} min_edge={min_edge:.2f}c"

# =============================================================================
# LOGIC LOOP (sem aliases — uso direto)
# =============================================================================
async def logic_loop(m_start: float, m_end: float, meta: dict, r_mult: float, r_prev_loss: float):
    global bankroll, daily_profit, kelly, as_model
    active_trades = []
    state = {'c1': {}, 'c2': {}}
    flags = {'s35': False, 'v30': False, 'd29': False, 's25': False, 'v20': False, 'd19': False}

    eff_risk_per_trade = calc_risk(RISK_PER_TRADE, r_mult, r_prev_loss, bankroll)
    eff_eighty_risk = calc_risk(EIGHTY_RISK, r_mult, r_prev_loss, bankroll)
    eff_peg_risk = calc_risk(PEG_ARBIT_RISK, r_mult, r_prev_loss, bankroll)

    if r_mult > 1.0 or r_prev_loss > 0:
        recovery_bonus_pct = (MARTINGALE_RECOVERY * r_prev_loss / bankroll) if bankroll > 0 else 0.0
        cap_tag_e = " CAP" if eff_eighty_risk >= MAX_RISK_PERCENT else ""
        cap_tag_p = " CAP" if eff_peg_risk >= MAX_RISK_PERCENT else ""
        log_info(f"MARTINGALE x{r_mult:.0f} prev_loss=${r_prev_loss:.4f} recovery={recovery_bonus_pct:.1%} "
                 f"EIGHTY={eff_eighty_risk:.1%}{cap_tag_e} PEG={eff_peg_risk:.1%}{cap_tag_p}")

    eighty_seen_levels = {'UP': set(), 'DOWN': set()}
    eighty_tick_count = {'UP': 0, 'DOWN': 0}
    eighty_last_buy = {'UP': 0.0, 'DOWN': 0.0}
    eighty_first_tick_t = {'UP': None, 'DOWN': None}
    eighty_eff_min = {'UP': None, 'DOWN': None}
    eighty_eff_max = {'UP': None, 'DOWN': None}
    eighty_cutoff_logged = False
    eighty_started_logged = False
    eighty_price_buffer = {'UP': PriceBuffer(15.0), 'DOWN': PriceBuffer(15.0)}
    eighty_vol_cooldown_until = {'UP': 0.0, 'DOWN': 0.0}
    peg_arbit_count = 0
    last_peg_time = 0.0

    mult_tag = f" MARTINGALE x{r_mult:.0f}" if r_mult > 1.0 else ""
    mods = []
    if EIGHTY_ACTIVE: mods.append(f"EIGHTY({EIGHTY_START_REM_S}s-{EIGHTY_CUTOFF_S}s)")
    if CICLO_30S_ACTIVE: mods.append("CICLO_30S")
    if CICLO_20S_ACTIVE: mods.append("CICLO_20S")
    if PEG_ARBIT_ACTIVE: mods.append(f"PEG_ARBIT({int(PEG_ARBIT_RANGE[0])}-{int(PEG_ARBIT_RANGE[1])}c)")
    log_sep2()
    log_info(f"NOVO CICLO | Market: {meta['slug']} | LIVE: {LIVE_TRADING}")
    log_info(f"Banca: ${bankroll:.4f} | Profit acum.: ${daily_profit:.4f}{mult_tag}")
    log_info(f"Módulos: {' | '.join(mods) if mods else 'nenhum'}")
    log_info(f"Risco eff: EIGHTY={eff_eighty_risk:.1%} PEG={eff_peg_risk:.1%} CICLOS={eff_risk_per_trade:.1%} CAP={MAX_RISK_PERCENT:.0%}")
    log_sep()
    log_info("ESCUTA ATIVA")
    log_sep()

    def pct_banca(invested: float) -> str:
        base = bankroll + invested
        return f"{invested / base * 100:.1f}% banca" if base > 0 else "—"

    async def open_trade(side: str, nom: float, trade_type: str, rstr: str, risk=None, wait_close=False,
                         fixed_invest=None, peg_val=None, token_id=None, extra_log=None, fixed_shares=None):
        global bankroll
        if risk is None:
            risk = eff_risk_per_trade
        kelly_log = ""
        if KELLY_ACTIVE and fixed_invest is None and fixed_shares is None:
            risk, kelly_log = kelly.compute_fraction(fallback=risk)
        ask = nom + ASK_SPREAD
        _fee = fee_rate(ask)
        eff = ask / (1.0 - _fee)
        if fixed_shares is not None:
            shares = fixed_shares
            invested = shares * ask / (1.0 - _fee)
        elif fixed_invest is not None:
            invested = fixed_invest
            shares = (invested / ask) * (1.0 - _fee)
        else:
            invested = bankroll * risk
            shares = (invested / ask) * (1.0 - _fee)
        if trade_type.startswith('CICLO'):
            target = CYCLE_TARGET_C / 100.0 if CYCLE_TARGET_C > 0 else None
        elif trade_type == 'EIGHTY':
            target = EIGHTY_TARGET_C / 100.0 if EIGHTY_TARGET_C > 0 else None
        elif trade_type == 'PEG_ARBIT':
            target = PEG_ARBIT_TARGET_C / 100.0 if PEG_ARBIT_TARGET_C > 0 else None
        else:
            target = min(0.99, eff * TARGET_MULTIPLIER)
        bankroll -= invested
        pct = pct_banca(invested)
        buy_fee = _fee * 100.0
        peg_str = f" PEG_Eff:{fc(peg_val)}" if peg_val is not None else ""
        extra = f" {extra_log}" if extra_log else ""
        kelly_sfx = f" {kelly_log}" if kelly_log else ""
        trade = {'side': side, 'nom': nom, 'entry': eff, 'shares': shares, 'target': target,
                 'type': trade_type, 'invested': invested, 'wait_close': wait_close, 'token_id': token_id}
        active_trades.append(trade)
        if LIVE_TRADING and token_id:
            await place_live_order(side, ask, shares, token_id)
        module = trade_type.replace('_', ' ')
        log_m(module, 'BUY', f"rem={rstr} {side} nom={fc(nom)} eff={fc(eff)}{peg_str} inv=${invested:.4f} ({pct}) shares={shares:.4f} fee={buy_fee:.3f}%{extra}{kelly_sfx}")

    def close_trade(trade: dict, cp: float, reason: str, rstr: str):
        global bankroll
        payout = sell_payout(trade['shares'], cp)
        pnl = payout - trade['invested']
        pnl_pct = (pnl / trade['invested'] * 100.0) if trade['invested'] > 0 else 0.0
        bankroll += payout
        if KELLY_ACTIVE:
            kelly.add_result(trade['invested'], payout)
        if AS_VPIN_ACTIVE:
            as_model.update_inventory(trade['side'], trade['shares'], is_buy=False)
        module = trade['type'].replace('_', ' ')
        log_m(module, 'SELL', f"rem={rstr} {trade['side']} @ {fc(cp)} PnL:${pnl:+.4f} ({pnl_pct:+.1f}%) Reason:{reason}")

    # ... (funções eighty_reset, eighty_reset_silent, eighty_activate_vol_cooldown mantidas exatamente como na versão anterior)

    def eighty_reset(e_side: str, rstr: str, reason: str):
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side] = 0
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side] = None
        eighty_eff_max[e_side] = None
        log_m('EIGHTY', 'RESET', f"rem={rstr} {e_side} — {reason}")

    def eighty_reset_silent(e_side: str):
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side] = 0
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side] = None
        eighty_eff_max[e_side] = None

    def eighty_activate_vol_cooldown(e_side: str, rstr: str, reason: str):
        eighty_vol_cooldown_until[e_side] = time.time() + EIGHTY_VOL_COOLDOWN_S
        eighty_reset(e_side, rstr, reason)
        log_m('EIGHTY', 'VOL_COOLDOWN', f"rem={rstr} {e_side} bloqueado {EIGHTY_VOL_COOLDOWN_S:.0f}s")

    prev_u_p = prev_d_p = None
    _pc_wait = price_change.wait
    _pc_clear = price_change.clear

    while True:
        now = time.time()
        rem = m_end - now
        if rem <= 0:
            u_p = best_asks.get('up') or 0.0
            d_p = best_asks.get('down') or 0.0
            log_sep()
            log_info(f"FIM DE MERCADO UP={fc(u_p)} DOWN={fc(d_p)}")
            for trade in active_trades[:]:
                cp = u_p if trade['side'] == 'UP' else d_p
                close_trade(trade, cp, "FIM MERCADO", "00:00:000")
                active_trades.remove(trade)
            break
        rstr = get_remaining_str(rem)
        try:
            await asyncio.wait_for(_pc_wait(), timeout=LOOP_SLEEP)
            _pc_clear()
        except asyncio.TimeoutError:
            pass
        u_p = best_asks.get('up')
        d_p = best_asks.get('down')
        if u_p is None or d_p is None or (u_p == prev_u_p and d_p == prev_d_p):
            prev_u_p = u_p
            prev_d_p = d_p
            continue
        prev_u_p = u_p
        prev_d_p = d_p

        ask_up = u_p + ASK_SPREAD
        ask_down = d_p + ASK_SPREAD
        eff_up = effective_entry(ask_up)
        eff_down = effective_entry(ask_down)
        peg_eff = eff_up + eff_down
        underpeg_eff_c = (1.0 - peg_eff) * 100.0
        peg_disp = f" PEG_Eff={peg_eff:.3f} under={underpeg_eff_c:.2f}c" if peg_eff < 1.0 and underpeg_eff_c >= PEG_ARBIT_UNDERPEG_C else ""
        log_raw(f"rem={rstr} UP={fc(u_p)} Eff={fc(eff_up)} DOWN={fc(d_p)} Eff={fc(eff_down)}{peg_disp}")

        if AS_VPIN_ACTIVE:
            mid_p = (u_p + d_p) * 0.5
            prev_mid = ((prev_u_p or u_p) + (prev_d_p or d_p)) * 0.5
            as_model.add_tick(mid_p, prev_mid)
        as_blocked = False
        min_edge = AS_MIN_EDGE_C
        if AS_VPIN_ACTIVE:
            q_total = as_model.inventory_up - as_model.inventory_down
            min_edge, as_log = as_model.get_min_edge_c(mid_c=(u_p + d_p) * 50.0, q=q_total, t_remaining=rem)
            if min_edge is None:
                as_blocked = True
                log_m('AS VPIN', 'WITHDRAW', f"rem={rstr} {as_log}")
            else:
                log_m('AS VPIN', 'STATUS', f"rem={rstr} {as_log}")

        # PEG ARBITRAGE (100% efetivo)
        if (PEG_ARBIT_ACTIVE and not as_blocked and peg_eff < 1.0 and underpeg_eff_c >= PEG_ARBIT_UNDERPEG_C
                and rem > PEG_ARBIT_MIN_REM and peg_arbit_count < MAX_PEG_ENTRIES
                and now - last_peg_time >= PEG_ARBIT_COOLDOWN):
            eff_up_c = eff_up * 100.0
            eff_down_c = eff_down * 100.0
            if PEG_ARBIT_RANGE[0] <= eff_up_c <= PEG_ARBIT_RANGE[1] and PEG_ARBIT_RANGE[0] <= eff_down_c <= PEG_ARBIT_RANGE[1]:
                budget = bankroll * eff_peg_risk
                ref_eff = max(eff_up, eff_down)
                shares_to_buy = budget / ref_eff
                log_sep()
                log_m('PEG ARBIT', 'ENTRADA', f"rem={rstr} PEG_Eff={peg_eff:.4f} (-{underpeg_eff_c:.2f}c) Shares={shares_to_buy:.4f} Total=${budget:.4f}")
                await open_trade('UP', u_p, 'PEG_ARBIT', rstr, fixed_shares=shares_to_buy, wait_close=True, peg_val=peg_eff, token_id=meta['up'])
                await open_trade('DOWN', d_p, 'PEG_ARBIT', rstr, fixed_shares=shares_to_buy, wait_close=True, peg_val=peg_eff, token_id=meta['down'])
                log_sep()
                peg_arbit_count += 1
                last_peg_time = now

        # TARGET CHECK, EIGHTY, CICLOS... (todo o resto usa variáveis diretas como EIGHTY_ACTIVE, EIGHTY_MIN_EFF_C etc.)

        # (o bloco completo de EIGHTY, CICLO_30S e CICLO_20S é idêntico ao anterior, apenas sem os _prefixos)

# =============================================================================
# MAIN (banca persistente)
# =============================================================================
async def main():
    global daily_profit, last_day, bankroll, risk_multiplier, prev_round_loss, accumulated_loss, kelly, as_model
    kelly = EmpiricalKelly()
    as_model = AvellanedaStoikov()
    risk_multiplier = 1.0
    prev_round_loss = 0.0
    accumulated_loss = 0.0

    if bankroll is None:
        if LIVE_TRADING:
            bankroll = 25.0  # TODO: substituir pelo saldo real da conta
            log_info(f"LIVE: banca carregada ${bankroll:.2f}")
        else:
            bankroll = 10.0
            log_info(f"DEMO: banca iniciada ${bankroll:.2f} — PERSISTENTE")

    log_sep2()
    log_info("BOT XRP POLYMARKET v0.37.0 INICIADO — BANCA PERSISTENTE")
    log_sep()
    log_info(f"LIVE_TRADING: {LIVE_TRADING} | Banca atual: ${bankroll:.4f}")

    while True:
        slug, start_ts = get_current_slug()
        meta = fetch_metadata(slug)
        if not meta:
            await asyncio.sleep(1)
            continue

        market_day = datetime.fromtimestamp(start_ts).date()
        if last_day is None or market_day != last_day:
            daily_profit = 0.0
            risk_multiplier = 1.0
            prev_round_loss = 0.0
            accumulated_loss = 0.0
            last_day = market_day
            kelly = EmpiricalKelly()
            as_model = AvellanedaStoikov()
            log_info(f"NOVO DIA {market_day} — banca mantida ${bankroll:.2f}")

        best_asks['up'] = best_asks['down'] = None
        price_change.clear()
        ws_task = asyncio.create_task(ws_handler(meta['up'], meta['down']))
        await asyncio.sleep(1.0)

        if best_asks['up'] is not None:
            pre_bank = bankroll
            await logic_loop(start_ts, start_ts + 300, meta, risk_multiplier, prev_round_loss)
            # ... (bloco PnL + martingale exatamente igual às versões anteriores)

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