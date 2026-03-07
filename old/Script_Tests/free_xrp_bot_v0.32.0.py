# =============================================================================
# BOT XRP POLYMARKET — v0.32.0
# =============================================================================
# CHANGELOG v0.32.0:
# [v0.32.0] [feat] EIGHTY_START_REM_S — janela de início do EIGHTY (60 = último minuto)
#           - Gate "rem > EIGHTY_START_REM_S" silencioso antes da janela activa
#           - Log único 'START' quando EIGHTY entra em acção
#           - eighty_started_logged como variável LOCAL (não atributo de função) — reset correcto entre rounds
# [v0.32.0] [cfg]  CICLO_30S_ACTIVE = False, CICLO_20S_ACTIVE = False (apenas EIGHTY + PEG ARBIT activos)
# [v0.32.0] [cfg]  EIGHTY_START_REM_S = 60.0 — EIGHTY activo apenas no último minuto
# [v0.32.0] [refac] Uniformização total dos parâmetros:
#           - Todos os valores _C (cents) e _S (segundos) são agora floats explícitos (ex: 82 → 82.0, 60 → 60.0)
#           - EIGHTY_PEG_MIN (rácio 0.0–2.0) renomeado para EIGHTY_PEG_MIN_C (cents 0.0–200.0)
#             para ser consistente com CYCLE_PEG_MIN_C — mesma unidade, mesmo conceito
#           - Comparações de PEG actualizadas: peg * 100.0 < EIGHTY_PEG_MIN_C
#           - Todos os parâmetros documentados com unidade e range (min ... max)
#           - Convenção de unidades documentada no topo da secção de parâmetros
# [v0.32.0] [perf] __slots__ em PriceBuffer, EmpiricalKelly e AvellanedaStoikov → menos memória e acesso mais rápido
# [v0.32.0] [perf] fee_rate(ask) calculado UMA vez por entrada e reutilizado em effective_entry + log
# [v0.32.0] [perf] Local variable aliasing para best_asks, constantes EIGHTY e flags no loop quente
# [v0.32.0] [perf] Pre-computação de ASK_SPREAD fora dos loops internos; evita atributo global repetido
# [v0.32.0] [perf] PriceBuffer.get_price_at: loop com early-exit quando já passou da tolerância (buf ordenado)
# [v0.32.0] [perf] asyncio.wait_for sem asyncio.shield desnecessário no loop principal (reduz overhead de tasks)
# [v0.32.0] [perf] get_ts() usa datetime.now() + strftime — sem alteração de lógica, mas movido para módulo-level
# [v0.32.0] [fix]  NameError potencial: min_edge não era atribuído quando AS_VPIN_ACTIVE=False mas era lido — agora sempre inicializado
# [v0.32.0] [fix]  buy_fee no log de open_trade calculado do fee já computado (0 chamadas extra a fee_rate)
# [v0.32.0] [fix]  eighty_cutoff_logged não era resetado se EIGHTY_START_REM_S > EIGHTY_CUTOFF_S — agora é variável local limpa por round
# -----------------------------------------------------------------------------
# CHANGELOG v0.31.0:
# [v0.31.0] [feat] Empirical Kelly com Monte Carlo — position sizing dinâmico
#           - f_empirical ≈ f_kelly × (1 - CV_edge)
#           - CV_edge = std / mean dos returns históricos
#           - Monte Carlo (KELLY_MC_SIMULATIONS) valida sobrevivência ao pior caso
#           - Substitui RISK_PER_TRADE e EIGHTY_RISK quando KELLY_ACTIVE=True
# [v0.31.0] [feat] Avellaneda-Stoikov Optimal Spread
# [v0.31.0] [feat] VPIN (Volume-synchronized Probability of Informed Trading)
# [v0.31.0] [feat] Toggle AS_VPIN_ACTIVE e KELLY_ACTIVE independentes
# -----------------------------------------------------------------------------
# [v0.30.5] [fix]  EIGHTY_DELTA_LOOKBACK_S não estava definido → NameError
# [v0.30.5] [fix]  TARGET CHECK bloqueado por wait_close=True → nunca vendia ao atingir target
# [v0.30.5] [feat] EIGHTY_WHEN_CUTOFF_0_VOLT: ignora volatilidade nos últimos N segundos
# -----------------------------------------------------------------------------
# [v0.30.4] EIGHTY: Delta de Preço Multi-Timeframe
# [v0.30.4] EIGHTY: Buffer circular de preços (EIGHTY_DELTA_LOOKBACK_S)
# [v0.30.4] EIGHTY: Cooldown de volatilidade
# -----------------------------------------------------------------------------
# [v0.30.0] [fix]  Múltiplos fixes críticos — ver versões anteriores
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

# ── Banca e Risco ────────────────────────────────────────────────────────────
BANKROLL_INIT       = 25.0  # USDC           (1.0 ... ∞)
RISK_PER_TRADE      = 0.05  # rácio 0.0–1.0  (0.01 ... 0.20)  — fracção da banca por trade nos Ciclos
MAX_RISK_MULTIPLIER = 2.2   # multiplicador  (1.0 ... 8.0)    — cap do martingale; limita drawdown máximo

# ── Toggles ──────────────────────────────────────────────────────────────────
CICLO_30S_ACTIVE = False    # [v0.32.0] Desactivado
CICLO_20S_ACTIVE = False    # [v0.32.0] Desactivado
EIGHTY_ACTIVE    = True
PEG_ARBIT_ACTIVE = True
KELLY_ACTIVE     = False    # Empirical Kelly com Monte Carlo — position sizing dinâmico (requer histórico mínimo KELLY_MIN_HISTORY)
AS_VPIN_ACTIVE   = False    # Avellaneda-Stoikov Optimal Spread + VPIN — bloqueia entradas em fluxo tóxico

# ── Ciclos ───────────────────────────────────────────────────────────────────
CYCLE_PRICE_MIN_C       = 74.0  # cents 0.0–100.0  (50.0 ... 95.0)   — preço mínimo para entrada nos Ciclos
CYCLE_PRICE_MAX_C       = 98.0  # cents 0.0–100.0  (85.0 ... 99.0)   — preço máximo para entrada nos Ciclos
CYCLE_PEG_MIN_C         = 98.0  # cents 0.0–200.0  (90.0 ... 102.0)  — PEG mínimo em cents (UP+DOWN)*100
CYCLE_VOL_MAX_C         = 25.0  # cents 0.0–100.0  (5.0  ... 50.0)   — variação máxima permitida entre snapshot e check
CYCLE_TARGET_C          =  0.0  # cents 0.0–100.0  (0.0 = hold-to-end, ou 90.0 ... 99.0 para saída antecipada)

CYCLE_30S_SNAPSHOT_REM  = 35.0  # segundos (0.0 ... 300.0)  — rem em que é tirado o snapshot do Ciclo 30s
CYCLE_30S_VOL_CHECK_REM = 30.0  # segundos (0.0 ... 300.0)  — rem em que é verificada a volatilidade (Ciclo 30s)
CYCLE_30S_BUY_REM       = 29.8  # segundos (0.0 ... 300.0)  — rem em que é executada a compra (Ciclo 30s)

CYCLE_20S_SNAPSHOT_REM  = 25.0  # segundos (0.0 ... 300.0)  — rem em que é tirado o snapshot do Ciclo 20s
CYCLE_20S_VOL_CHECK_REM = 20.0  # segundos (0.0 ... 300.0)  — rem em que é verificada a volatilidade (Ciclo 20s)
CYCLE_20S_BUY_REM       = 19.8  # segundos (0.0 ... 300.0)  — rem em que é executada a compra (Ciclo 20s)

# ── Eighty ───────────────────────────────────────────────────────────────────
# [v0.32.0] Janela activa: EIGHTY_START_REM_S >= rem > EIGHTY_CUTOFF_S
# Objectivo: win rate ≥80% por trade | Contribuição: ~1-2% PnL/round
# Lógica: janela mais curta + filtros mais apertados = só entra com alta convicção
# Retorno esperado por win a 87c efectivo: (100-87)/87 ≈ +14.9% sobre investido
# EV com 82% win rate: 0.82×14.9% − 0.18×100% = +12.2% − 18% → requer edge real
# (edge vem da convergência dos últimos 45s — mercado subaprecia a probabilidade terminal)
EIGHTY_START_REM_S        = 45.0  # segundos (0.0 ... 300.0)  — janela mais apertada que 60s = mais convicção (45s→8s = 37s activos)
EIGHTY_MIN_EFF_C          = 83.0  # cents 0.0–100.0  (70.0 ... 95.0)   — 83c → retorno de ~20% se ganhar; abaixo disso o risco/retorno piora
EIGHTY_MAX_EFF_C          = 94.0  # cents 0.0–100.0  (85.0 ... 99.5)   — cap a 94c; acima disso retorno <6.4% não justifica o risco de perder
EIGHTY_MIN_TICKS          = 3     # int    (1 ... 20)                   — 6 níveis confirmados (era 5) — mais confirmação de consolidação
EIGHTY_RISK               = 0.10  # rácio 0.0–1.0  (0.01 ... 0.15)     — 9% da banca por trade; equilibra retorno vs perda máxima aceitável
EIGHTY_CUTOFF_S           = 8.0   # segundos (0.0 ... 60.0)  — para a 8s do fim (era 5s); janela activa: 45s→8s
EIGHTY_WHEN_CUTOFF_0_VOLT = 35.0  # segundos (0.0 ... 60.0)  — só activo se CUTOFF_S=0: ignora vol nos últimos N segundos
EIGHTY_PEG_MIN_C          = 99.0  # cents 0.0–200.0  (90.0 ... 102.0)  — PEG mínimo 99c (era 98c); mercado muito equilibrado = maior convicção no lado vencedor
EIGHTY_BUY_COOLDOWN       =  4.0  # segundos (0.0 ... 30.0)  — 5s entre compras (era 3s); limita sobre-exposição no mesmo round
EIGHTY_VOL_WINDOW_S       =  2.0  # segundos (0.5 ... 10.0)  — janela maior (era 1.5s); detecta volatilidade num horizonte mais alargado
EIGHTY_VOL_MAX_C          =  3.5  # cents 0.0–100.0  (2.0 ... 15.0)    — mais restritivo (era 4.5c); rejeita entradas em preço a oscilar
EIGHTY_VOL_COOLDOWN_S     =  5.0  # segundos (0.0 ... 30.0)  — cooldown mais longo após vol (era 5s); protege contra entradas rápidas pós-spike

# [v0.30.4] Delta Multi-Timeframe
EIGHTY_DELTA_INTERVALS    = [0.5, 1.0, 2.0]  # segundos        — intervalos de comparação de delta de preço
EIGHTY_DELTA_LOOKBACK_S   =  2.0  # segundos (0.5 ... 10.0)  — lookback máximo do buffer de delta
EIGHTY_DELTA_MAX_RISE_C   =  4.0  # cents 0.0–100.0  (2.0 ... 15.0)   — era 5.0c; sobe mais rápido que isto = suspeita de pump falso
EIGHTY_DELTA_VOL_RISE_C   =  3.5  # cents 0.0–100.0  (2.0 ... 15.0)   — era 4.5c; mais sensível a subidas rápidas que sinalizam reversão
EIGHTY_DELTA_VOL_TIME_S   =  1.5  # segundos (0.5 ... 5.0)   — janela temporal para detecção de subida rápida

EIGHTY_TARGET_C           =  0.0  # cents 0.0–100.0  (0.0 = hold-to-end; hold-to-end maximiza win rate pois evita saídas prematuras)

# ── PEG Arbitrage ────────────────────────────────────────────────────────────
# Motor principal do PnL de 5%/round — lucro quase garantido quando PEG < 100c
# Lógica: UP + DOWN devem somar 100c na resolução; comprar ambos abaixo de 100c = arbitrage
# Exemplo: PEG = 0.97 → compras UP+DOWN a ~48.5c cada; ganhas sempre ~3c/97c = +3.1%
# Com PEG_ARBIT_RISK = 0.18 e bankroll $25 → $4.50/leg → $9.00 total → lucro +$0.28/arb
# 2 arbs/round a PEG=0.97 → +$0.56 = +2.2% bankroll; a PEG=0.94 → +$0.63 = +2.5%/arb
# 2-3 arbs/round → +3.8% a +5.7% bankroll ← principal driver do objectivo de 5%/round
PEG_ARBIT_UNDERPEG_C    =  3.0  # cents 0.0–200.0  (2.0 ... 20.0)   — era 5.0c; baixar para 3.0c activa com mais frequência (PEG < 0.97)
PEG_ARBIT_RISK          = 0.18  # rácio 0.0–1.0  (0.05 ... 0.30)    — era 0.15; subir para 0.18 aumenta lucro por arb (~+20% vs anterior)
PEG_ARBIT_COOLDOWN      = 0.05  # segundos (0.0 ... 5.0)   — intervalo mínimo entre entradas consecutivas
PEG_ARBIT_MIN_REM       =  8.0  # cents 0.0–60.0   (0.0 ... 60.0)   — era 5.0s; subir para 8.0s evita entrar quando PEG diverge irrecuperavelmente perto do fim
MAX_PEG_ENTRIES         =   15  # int    (1 ... 50)                   — era 10; subir para 15 captura mais oportunidades por round

# ── Empirical Kelly ──────────────────────────────────────────────────────────
KELLY_MC_SIMULATIONS =  5000   # int    (1000 ... 50000)              — resamplings Monte Carlo para validação
KELLY_CONFIDENCE     =  0.90   # rácio 0.0–1.0  (0.80 ... 0.99)      — percentil de sobrevivência exigido
KELLY_MIN_HISTORY    =    10   # int    (5 ... 100)                   — mínimo de trades antes de usar Kelly
KELLY_MAX_FRACTION   =  0.25   # rácio 0.0–1.0  (0.05 ... 0.50)      — cap máximo do Kelly (nunca arrisca mais disto)
KELLY_MIN_FRACTION   =  0.02   # rácio 0.0–1.0  (0.005 ... 0.10)     — floor mínimo do Kelly
KELLY_RUIN_THRESHOLD =  0.50   # rácio 0.0–1.0  (0.20 ... 0.80)      — se MC mostra drawdown > este valor, halve a fracção

# ── Avellaneda-Stoikov + VPIN ─────────────────────────────────────────────────
AS_GAMMA               = 0.05  # rácio 0.0–1.0  (0.01 ... 0.50)      — risk aversion γ (maior = mais conservador)
AS_KAPPA_DEFAULT       =  1.0  # float  (0.1 ... 10.0)                — taxa de chegada de ordens κ (ticks/s)
AS_VPIN_WINDOW         =   50  # int    (10 ... 200)                  — ticks para calcular o VPIN
AS_VPIN_WIDEN          = 0.70  # VPIN 0.0–1.0  (0.50 ... 0.90)       — acima disto alarga spread mínimo
AS_VPIN_WITHDRAW       = 0.90  # VPIN 0.0–1.0  (0.70 ... 1.00)       — acima disto bloqueia novas entradas
AS_SPREAD_WIDEN_FACTOR =  1.1  # multiplicador ≥ 1.0  (1.0 ... 3.0)  — multiplicador do spread quando VPIN > AS_VPIN_WIDEN
AS_MIN_EDGE_C          =  0.1  # cents 0.0–100.0  (0.05 ... 5.0)     — edge mínimo em cents para entrar

# ── Fee / Spread / Performance ───────────────────────────────────────────────
FEE_RATE          = 0.25   # rácio 0.0–1.0  (0.10 ... 0.50)          — taxa base de fee do Polymarket (fórmula quadrática)
FEE_EXP           =    2   # int    (1 ... 4)                         — expoente da fórmula de fee: fee = FEE_RATE*(p*(1-p))^FEE_EXP
ASK_SPREAD        = 0.01   # preço 0.0–1.0  (0.005 ... 0.03)         — spread adicionado ao nom para simular o ask real
TARGET_MULTIPLIER = 1.05   # multiplicador ≥ 1.0  (1.02 ... 1.20)    — multiplicador sobre eff para definir target do PEG_ARBIT
LOOP_SLEEP        = 0.001  # segundos (0.0001 ... 0.010)              — timeout máximo à espera de novo tick de preço

# ── Globais de estado ────────────────────────────────────────────────────────
bankroll      = BANKROLL_INIT
daily_profit  = 0.0
last_day      = None
best_asks     = {'up': None, 'down': None}
price_change  = asyncio.Event()
risk_multiplier = 1.0
bot_start_time  = time.time()
kelly    = None
as_model = None

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
        print("❌ py-clob-client não instalado! Rode: pip install py-clob-client")
        raise SystemExit(1)

# =============================================================================
# ========================== FUNÇÕES AUXILIARES ===============================
# =============================================================================

# [perf] Pre-computados para evitar lookup de atributo global no loop quente
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
    return datetime.now().strftime("%y/%d/%m | %H:%M:%S.%f")[:-3]

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
    _best_asks = best_asks  # local alias [perf]
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
    """
    Buffer circular para histórico de preços com timestamps.
    [v0.32.0] __slots__ para menor footprint; get_price_at com early-exit.
    """
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
        # Buffer is time-ordered → once diff starts growing past best, continue (could still find closer)
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
    """
    Empirical Kelly com Monte Carlo.
    [v0.32.0] __slots__ para menor footprint.
    """
    __slots__ = ('returns',)

    def __init__(self):
        self.returns: list[float] = []

    def add_result(self, invested: float, payout: float):
        if invested > 0:
            self.returns.append((payout - invested) / invested)

    def compute_fraction(self, fallback: float) -> tuple[float, str]:
        n = len(self.returns)
        if n < KELLY_MIN_HISTORY:
            return fallback, f"Kelly N/A (histórico {n}/{KELLY_MIN_HISTORY}) → fallback {fallback:.1%}"

        arr    = np.array(self.returns)
        mean_r = float(np.mean(arr))
        std_r  = float(np.std(arr))

        if mean_r <= 0:
            return KELLY_MIN_FRACTION, f"Kelly edge negativo ({mean_r:.3f}) → min {KELLY_MIN_FRACTION:.1%}"

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
            ruin_note = f" [MC ruin halved → {f_empirical:.3f}]"

        f_final = max(KELLY_MIN_FRACTION, min(KELLY_MAX_FRACTION, f_empirical))
        log_str = (
            f"Kelly f={f_final:.3f} | f_kelly={f_kelly:.3f} | "
            f"CV={cv_edge:.2f} | μ={mean_r:.3f} σ={std_r:.3f} | "
            f"MC_worst={worst_case:.3f}{ruin_note} | n={n}"
        )
        return f_final, log_str


# =============================================================================
# ========================== AVELLANEDA-STOIKOV + VPIN ========================
# =============================================================================

class AvellanedaStoikov:
    """
    Avellaneda-Stoikov Optimal Spread + VPIN.
    [v0.32.0] __slots__ para menor footprint.
    """
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
                f"AS WITHDRAW | VPIN={vpin_val:.2f} >= {AS_VPIN_WITHDRAW} | "
                f"r={r:.1f}c δ/2={half_d:.2f}c σ²={sig2:.5f}"
            )

        widen    = AS_SPREAD_WIDEN_FACTOR if vpin_val >= AS_VPIN_WIDEN else 1.0
        min_edge = max(AS_MIN_EDGE_C, half_d * widen)
        log_str  = (
            f"AS | VPIN={vpin_val:.2f} r={r:.1f}c δ/2={half_d:.2f}c "
            f"min_edge={min_edge:.2f}c{'  [WIDEN x'+str(AS_SPREAD_WIDEN_FACTOR)+']' if widen > 1 else ''}"
        )
        return min_edge, log_str

# =============================================================================
# ========================== LOGIC LOOP =======================================
# =============================================================================

async def logic_loop(m_start: float, m_end: float, meta: dict, r_mult: float):
    global bankroll, daily_profit, kelly, as_model

    active_trades = []
    state         = {'c1': {}, 'c2': {}}
    flags         = {
        's35': False, 'v30': False, 'd29': False,
        's25': False, 'v20': False, 'd19': False
    }

    eff_risk_per_trade = RISK_PER_TRADE * r_mult
    eff_eighty_risk    = EIGHTY_RISK    * r_mult
    eff_peg_risk       = PEG_ARBIT_RISK * r_mult

    # [perf] Aliases locais para constantes EIGHTY usadas no loop quente
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

    # ── Eighty tracking ──────────────────────────────────────────────────────
    eighty_seen_levels        = {'UP': set(), 'DOWN': set()}
    eighty_last_eff_c         = {'UP': None,  'DOWN': None}
    eighty_tick_count         = {'UP': 0,     'DOWN': 0}
    eighty_last_buy           = {'UP': 0.0,   'DOWN': 0.0}
    eighty_first_tick_t       = {'UP': None,  'DOWN': None}
    eighty_eff_min            = {'UP': None,  'DOWN': None}
    eighty_eff_max            = {'UP': None,  'DOWN': None}
    eighty_cutoff_logged      = False          # [v0.32.0] variável local, limpa por round
    eighty_started_logged     = False          # [v0.32.0] log único de START, local e correcto
    eighty_price_buffer       = {
        'UP':   PriceBuffer(max_age_seconds=15.0),
        'DOWN': PriceBuffer(max_age_seconds=15.0)
    }
    eighty_vol_cooldown_until = {'UP': 0.0, 'DOWN': 0.0}

    peg_arbit_count = 0
    last_peg_time   = 0.0

    # ── Header ───────────────────────────────────────────────────────────────
    mult_tag = f" [MARTINGALE x{r_mult:.0f}]" if r_mult > 1.0 else ""
    mods = []
    if EIGHTY_ACTIVE:    mods.append(f"EIGHTY(start {_EIGHTY_START_REM_S}s→cutoff {_EIGHTY_CUTOFF_S}s, delta {EIGHTY_DELTA_LOOKBACK_S}s)")
    if CICLO_30S_ACTIVE: mods.append("CICLO_30S")
    if CICLO_20S_ACTIVE: mods.append("CICLO_20S")
    if PEG_ARBIT_ACTIVE: mods.append("PEG_ARBIT")

    log_sep()
    log_info(f"Market: {meta['slug']} | LIVE_TRADING: {LIVE_TRADING}")
    log_info(f"UP: {meta['up']} | DW: {meta['down']}")
    log_info(f"Bank: ${bankroll:.2f} | Profit Acumulado: ${daily_profit:.2f}{mult_tag}")
    log_info(f"Módulos: {' | '.join(mods)}")
    log_info(f"EIGHTY: Janela [{_EIGHTY_START_REM_S}s→{_EIGHTY_CUTOFF_S}s] | Delta={EIGHTY_DELTA_LOOKBACK_S}s | Vol={_EIGHTY_VOL_MAX_C}c/{_EIGHTY_VOL_WINDOW_S}s | Cooldowns: buy={_EIGHTY_BUY_COOLDOWN}s vol={_EIGHTY_VOL_COOLDOWN_S}s")
    kelly_status = f"Kelly {'ON' if KELLY_ACTIVE else 'OFF'} | n={len(kelly.returns)}"
    vpin_status  = f"AS+VPIN {'ON' if AS_VPIN_ACTIVE else 'OFF'} | VPIN={as_model.vpin:.2f} | σ²={as_model.sigma2:.5f}"
    log_info(f"{kelly_status} | {vpin_status}")
    log_sep()
    log_info(">>> ESCUTA ATIVA")

    def pct_banca(invested: float) -> str:
        base = bankroll + invested
        return f"{invested / base * 100:.0f}% banca" if base else "—"

    # ── open_trade ────────────────────────────────────────────────────────────
    async def open_trade(
        side: str, nom: float, trade_type: str, rstr: str,
        risk: float = None, wait_close: bool = False,
        fixed_invest: float = None, peg_val: float = None,
        token_id: str = None, extra_log: str = None
    ):
        global bankroll
        if risk is None:
            risk = eff_risk_per_trade

        kelly_log = ""
        if KELLY_ACTIVE and fixed_invest is None:
            risk, kelly_log = kelly.compute_fraction(fallback=risk)

        ask      = nom + _ASK_SPREAD
        # [v0.32.0] [perf] fee_rate calculado UMA vez e reutilizado
        _fee     = fee_rate(ask)
        eff      = ask / (1.0 - _fee)
        invested = fixed_invest if fixed_invest is not None else (bankroll * risk)
        shares   = (invested / ask) * (1.0 - _fee)

        if trade_type.startswith('CICLO'):
            target = min(0.99, CYCLE_TARGET_C / 100.0) if CYCLE_TARGET_C > 0 else None
        elif trade_type == 'EIGHTY':
            target = min(0.99, _EIGHTY_TARGET_C / 100.0) if _EIGHTY_TARGET_C > 0 else None
        else:
            target = min(0.99, eff * TARGET_MULTIPLIER)

        bankroll -= invested
        pct      = pct_banca(invested)
        buy_fee  = _fee * 100   # [v0.32.0] reutiliza fee já calculado
        peg_str  = f" | *** PEG: {peg_val:.3f} ({peg_val*100:.1f}c) ***" if (peg_val is not None and peg_val * 100.0 <= _EIGHTY_PEG_MIN_C) else ""
        extra    = f" | {extra_log}" if extra_log else ""

        trade = {
            'side': side, 'nom': nom, 'entry': eff, 'shares': shares,
            'target': target, 'type': trade_type, 'invested': invested,
            'wait_close': wait_close, 'token_id': token_id
        }
        active_trades.append(trade)

        if LIVE_TRADING and token_id:
            await place_live_order(side, ask, shares, token_id)

        kelly_str = f" | {kelly_log}" if kelly_log else ""
        module = trade_type.replace('_', ' ')
        log_m(module, 'BUY',
            f"Remaining: {rstr} | {side} @ {fc(nom)} | Ask: {fc(ask)} | Eff: {fc(eff)}"
            f"{peg_str} | Inv: ${invested:.2f} ({pct}) | Shares: {shares:.4f} "
            f"| Fee: {buy_fee:.2f}%{extra}{kelly_str}"
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

    # ── eighty_reset ─────────────────────────────────────────────────────────
    def eighty_reset(e_side: str, rstr: str, reason: str):
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side]   = 0
        eighty_last_eff_c[e_side]   = None
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side]      = None
        eighty_eff_max[e_side]      = None
        log_m('EIGHTY', 'RESET',
            f"Remaining: {rstr} | {e_side} — {reason} | ticks reset")

    def eighty_reset_silent(e_side: str):
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side]   = 0
        eighty_last_eff_c[e_side]   = None
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side]      = None
        eighty_eff_max[e_side]      = None

    def eighty_activate_vol_cooldown(e_side: str, rstr: str, reason: str):
        eighty_vol_cooldown_until[e_side] = time.time() + _EIGHTY_VOL_COOLDOWN_S
        eighty_reset(e_side, rstr, reason)
        log_m('EIGHTY', 'VOL_COOLDOWN',
            f"Remaining: {rstr} | {e_side} — bloqueado por {_EIGHTY_VOL_COOLDOWN_S}s devido a volatilidade")

    prev_u_p = prev_d_p = None

    # [perf] Alias locais para evitar lookup global em cada iteração do loop
    _best_asks    = best_asks
    _pc_wait      = price_change.wait
    _pc_clear     = price_change.clear
    _loop_sleep   = LOOP_SLEEP
    _AS_VPIN      = AS_VPIN_ACTIVE
    _KELLY_ACT    = KELLY_ACTIVE
    _PEG_ACTIVE   = PEG_ARBIT_ACTIVE
    _PEG_UNDERPEG = PEG_ARBIT_UNDERPEG_C
    _PEG_MIN_REM  = PEG_ARBIT_MIN_REM
    _PEG_COOLDOWN = PEG_ARBIT_COOLDOWN
    _MAX_PEG      = MAX_PEG_ENTRIES
    _EIGHTY_ACT   = EIGHTY_ACTIVE
    _CICLO30_ACT  = CICLO_30S_ACTIVE
    _CICLO20_ACT  = CICLO_20S_ACTIVE

    # =========================================================================
    # ── LOOP PRINCIPAL ────────────────────────────────────────────────────────
    # =========================================================================
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
            log_info("Remaining: 00:00:000 | Fim de Mercado")
            break

        rstr = get_remaining_str(rem)

        try:
            # [v0.32.0] [perf] Sem asyncio.shield desnecessário — reduz overhead de wrapping de tasks
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
        peg        = u_p + d_p
        underpeg_c = (1.0 - peg) * 100.0
        peg_disp   = f" | PEG: {peg:.3f} -{underpeg_c:.1f}c" if peg * 100.0 <= _EIGHTY_PEG_MIN_C else ""
        log_raw(f"Remaining: {rstr} | UP: {fc(u_p)} | DOWN: {fc(d_p)}{peg_disp}")

        # ── AS+VPIN: tick ─────────────────────────────────────────────────────
        if _AS_VPIN:
            mid_p    = (u_p + d_p) * 0.5
            prev_mid = ((prev_u_p or u_p) + (prev_d_p or d_p)) * 0.5
            as_model.add_tick(mid_p, prev_mid)

        # ── AS+VPIN: gate global ──────────────────────────────────────────────
        as_blocked = False
        as_log_str = ""
        min_edge   = AS_MIN_EDGE_C  # [v0.32.0] sempre inicializado para evitar NameError
        if _AS_VPIN:
            q_total  = as_model.inventory_up - as_model.inventory_down
            min_edge, as_log_str = as_model.get_min_edge_c(
                mid_c=(u_p + d_p) * 50.0,
                q=q_total,
                t_remaining=rem
            )
            if min_edge is None:
                as_blocked = True
                log_info(f"[AS/VPIN] WITHDRAW | {as_log_str}")

        # ── 1. PEG ARBITRAGE ─────────────────────────────────────────────────
        if (_PEG_ACTIVE
                and not as_blocked
                and underpeg_c >= _PEG_UNDERPEG
                and rem > _PEG_MIN_REM
                and peg_arbit_count < _MAX_PEG
                and now - last_peg_time >= _PEG_COOLDOWN):
            invest_per_leg = bankroll * eff_peg_risk
            log_m('PEG ARBIT', 'ACTIVE',
                f"Remaining: {rstr} | PEG ARBIT ACTIVADO — PEG {peg:.3f}")
            await open_trade('UP',   u_p, 'PEG_ARBIT', rstr,
                             fixed_invest=invest_per_leg, wait_close=True,
                             peg_val=peg, token_id=meta['up'])
            await open_trade('DOWN', d_p, 'PEG_ARBIT', rstr,
                             fixed_invest=invest_per_leg, wait_close=True,
                             peg_val=peg, token_id=meta['down'])
            peg_arbit_count += 1
            last_peg_time    = now

        # ── 2. TARGET CHECK ──────────────────────────────────────────────────
        for trade in active_trades[:]:
            if trade.get('target') is None:
                continue
            cp = u_p if trade['side'] == 'UP' else d_p
            if cp and eff_sell_price(cp) >= trade['target']:
                close_trade(trade, cp, "TARGET", rstr)
                active_trades.remove(trade)

        # ── 3. EIGHTY ────────────────────────────────────────────────────────
        if _EIGHTY_ACT:
            # [v0.32.0] Gate 1: janela ainda não começou — silencioso
            if rem > _EIGHTY_START_REM_S:
                pass

            # Gate 2: abaixo do cutoff — para
            elif rem <= _EIGHTY_CUTOFF_S:
                if not eighty_cutoff_logged:
                    eighty_cutoff_logged = True
                    log_m('EIGHTY', 'CUTOFF',
                        f"Remaining: {rstr} | EIGHTY parado — rem <= {_EIGHTY_CUTOFF_S}s")

            # Janela activa: _EIGHTY_START_REM_S >= rem > _EIGHTY_CUTOFF_S
            else:
                # [v0.32.0] Log único de arranque da janela
                if not eighty_started_logged:
                    eighty_started_logged = True
                    log_m('EIGHTY', 'START',
                        f"Remaining: {rstr} | EIGHTY activado — janela [{_EIGHTY_START_REM_S}s → {_EIGHTY_CUTOFF_S}s]")

                for e_side, nom in (('UP', u_p), ('DOWN', d_p)):
                    token_id = meta['up'] if e_side == 'UP' else meta['down']

                    skip_vol = (_EIGHTY_CUTOFF_S == 0 and _EIGHTY_WHEN_CV0 > 0 and rem <= _EIGHTY_WHEN_CV0)

                    if not skip_vol and now < eighty_vol_cooldown_until[e_side]:
                        continue

                    if not skip_vol and now - eighty_last_buy[e_side] < _EIGHTY_BUY_COOLDOWN:
                        continue

                    ask   = nom + _ASK_SPREAD
                    # [v0.32.0] [perf] fee calculado uma vez por iteração de side
                    _fee  = fee_rate(ask)
                    eff_c = (ask / (1.0 - _fee)) * 100.0

                    eighty_price_buffer[e_side].add(eff_c, now)

                    if as_blocked:
                        continue

                    if not (_EIGHTY_MIN_EFF_C <= eff_c <= _EIGHTY_MAX_EFF_C):
                        if eighty_tick_count[e_side] > 0:
                            eighty_reset(e_side, rstr,
                                f"eff_c {eff_c:.1f}c OUT of RANGE [{_EIGHTY_MIN_EFF_C}c-{_EIGHTY_MAX_EFF_C}c]")
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
                    delta_str = " | ".join(delta_parts) if delta_parts else f"Δ aguardando ({epb.get_age():.1f}s acumulado)"

                    delta_15, valid_15 = epb.get_delta(_EIGHTY_DELTA_VOL_T)
                    rapid_rise = valid_15 and delta_15 is not None and delta_15 >= _EIGHTY_DELTA_VOL_C

                    delta_ok      = True
                    delta_reason  = ""
                    has_any_delta = valid_05 or valid_10 or valid_20

                    if valid_05 and delta_05 is not None and delta_05 < 0:
                        delta_ok = False; delta_reason = f"Δ0.5s={delta_05:+.1f}c (a cair)"
                    elif valid_10 and delta_10 is not None and delta_10 < 0:
                        delta_ok = False; delta_reason = f"Δ1s={delta_10:+.1f}c (a cair)"
                    elif valid_20 and delta_20 is not None and delta_20 < 0:
                        delta_ok = False; delta_reason = f"Δ2s={delta_20:+.1f}c (a cair)"
                    elif rapid_rise:
                        delta_ok = False; delta_reason = f"Δ1.5s={delta_15:+.1f}c >= {_EIGHTY_DELTA_VOL_C}c (subida rápida)"

                    if skip_vol:
                        vol_str = f"VOL SKIP (cutoff=0, rem<={_EIGHTY_WHEN_CV0}s) | {var_c:.1f}c em {elapsed:.1f}s"
                    else:
                        vol_str = f"VOL {'NOK' if vol_nok else 'OK'} ({var_c:.1f}c em {elapsed:.1f}s)"

                    delta_status = "↑OK" if (delta_ok and has_any_delta) else ("↓NOK" if has_any_delta else "—")
                    peg_tick_str = f" | PEG: {peg:.3f} ({peg*100:.1f}c)" if peg * 100.0 <= _EIGHTY_PEG_MIN_C else ""

                    log_m('EIGHTY', 'WATCH',
                        f"Remaining: {rstr} | {e_side} @ Ask {fc(ask)} "
                        f"| Eff: {fc(eff_c/100)} | {vol_str} | {delta_str} {delta_status}"
                        f"{peg_tick_str} | ticks: {eighty_tick_count[e_side]}/{_EIGHTY_MIN_TICKS}"
                    )

                    if not skip_vol:
                        if vol_nok:
                            eighty_activate_vol_cooldown(e_side, rstr,
                                f"VOLAT NOK — {var_c:.1f}c em {elapsed:.1f}s "
                                f"(limite: {_EIGHTY_VOL_MAX_C:.1f}c/{_EIGHTY_VOL_WINDOW_S:.1f}s)")
                            continue
                        if rapid_rise:
                            eighty_activate_vol_cooldown(e_side, rstr,
                                f"RAPID RISE — {delta_15:+.1f}c em {_EIGHTY_DELTA_VOL_T}s "
                                f"(limite: {_EIGHTY_DELTA_VOL_C:.1f}c)")
                            continue

                    if eighty_tick_count[e_side] >= _EIGHTY_MIN_TICKS:
                        if peg * 100.0 < _EIGHTY_PEG_MIN_C:
                            eighty_reset(e_side, rstr, f"NOK — PEG: {peg*100:.1f}c < {_EIGHTY_PEG_MIN_C:.1f}c")
                            continue

                        if has_any_delta and not delta_ok:
                            eighty_reset(e_side, rstr, f"DELTA NOK — {delta_reason}")
                            continue

                        if _AS_VPIN and not skip_vol and min_edge is not None:
                            edge_c = (_EIGHTY_TARGET_C if _EIGHTY_TARGET_C > 0 else 99.0) - eff_c
                            if edge_c < min_edge:
                                eighty_reset(e_side, rstr,
                                    f"AS EDGE NOK — edge {edge_c:.1f}c < min {min_edge:.2f}c | {as_log_str}")
                                continue

                        if bankroll > 0:
                            if _AS_VPIN:
                                shares_est = buy_shares_net(bankroll * eff_eighty_risk, nom + _ASK_SPREAD)
                                as_model.update_inventory(e_side, shares_est, is_buy=True)
                            delta_log = delta_str if has_any_delta else "Δ N/A"
                            await open_trade(e_side, nom, 'EIGHTY', rstr,
                                             risk=eff_eighty_risk, wait_close=True,
                                             peg_val=peg, token_id=token_id,
                                             extra_log=f"ticks: {eighty_tick_count[e_side]}/{_EIGHTY_MIN_TICKS} | {delta_log}")
                            eighty_last_buy[e_side] = now
                            eighty_reset_silent(e_side)
                            log_m('EIGHTY', 'COOLDOWN',
                                f"Remaining: {rstr} | {e_side} — cooldown {_EIGHTY_BUY_COOLDOWN:.1f}s (anti-stacking)")

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
                ok_str = "OK" if state['c1']['vol_ok'] else "NOK"
                log_m('CICLO 30s', 'VOLT',
                    f"Remaining: {rstr} | vol={vol_c:.1f}c (<= {CYCLE_VOL_MAX_C:.1f}c) "
                    f"| ok={state['c1']['vol_ok']} | {ok_str}")

            if (flags['v30'] and state['c1'].get('vol_ok')
                    and not flags['d29'] and rem <= CYCLE_30S_BUY_REM):
                flags['d29'] = True
                for e_side, nom, tid in (('UP', u_p, meta['up']), ('DOWN', d_p, meta['down'])):
                    price_c = nom * 100.0
                    peg_c   = peg * 100.0
                    if CYCLE_PRICE_MIN_C <= price_c <= CYCLE_PRICE_MAX_C and peg_c >= CYCLE_PEG_MIN_C:
                        if _AS_VPIN:
                            shares_tmp = buy_shares_net(bankroll * eff_risk_per_trade, nom + _ASK_SPREAD)
                            as_model.update_inventory(e_side, shares_tmp, is_buy=True)
                        await open_trade(e_side, nom, 'CICLO_30s', rstr,
                                         risk=eff_risk_per_trade, wait_close=True, peg_val=peg, token_id=tid)
                    else:
                        reasons = []
                        if price_c < CYCLE_PRICE_MIN_C: reasons.append(f"price {price_c:.1f}c < min {CYCLE_PRICE_MIN_C:.1f}c")
                        elif price_c > CYCLE_PRICE_MAX_C: reasons.append(f"price {price_c:.1f}c > max {CYCLE_PRICE_MAX_C:.1f}c")
                        if peg_c < CYCLE_PEG_MIN_C: reasons.append(f"PEG {peg_c:.1f}c < {CYCLE_PEG_MIN_C:.1f}c")
                        log_m('CICLO 30s', 'SKIP', f"Remaining: {rstr} | {e_side} sem compra — {' | '.join(reasons)}")

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
                ok_str = "OK" if state['c2']['vol_ok'] else "NOK"
                log_m('CICLO 20s', 'VOLT',
                    f"Remaining: {rstr} | vol={vol_c:.1f}c (<= {CYCLE_VOL_MAX_C:.1f}c) "
                    f"| ok={state['c2']['vol_ok']} | {ok_str}")

            if (flags['v20'] and state['c2'].get('vol_ok')
                    and not flags['d19'] and rem <= CYCLE_20S_BUY_REM):
                flags['d19'] = True
                for e_side, nom, tid in (('UP', u_p, meta['up']), ('DOWN', d_p, meta['down'])):
                    price_c = nom * 100.0
                    peg_c   = peg * 100.0
                    if CYCLE_PRICE_MIN_C <= price_c <= CYCLE_PRICE_MAX_C and peg_c >= CYCLE_PEG_MIN_C:
                        if _AS_VPIN:
                            shares_tmp = buy_shares_net(bankroll * eff_risk_per_trade, nom + _ASK_SPREAD)
                            as_model.update_inventory(e_side, shares_tmp, is_buy=True)
                        await open_trade(e_side, nom, 'CICLO_20s', rstr,
                                         risk=eff_risk_per_trade, wait_close=True, peg_val=peg, token_id=tid)
                    else:
                        reasons = []
                        if price_c < CYCLE_PRICE_MIN_C: reasons.append(f"price {price_c:.1f}c < min {CYCLE_PRICE_MIN_C:.1f}c")
                        elif price_c > CYCLE_PRICE_MAX_C: reasons.append(f"price {price_c:.1f}c > max {CYCLE_PRICE_MAX_C:.1f}c")
                        if peg_c < CYCLE_PEG_MIN_C: reasons.append(f"PEG {peg_c:.1f}c < {CYCLE_PEG_MIN_C:.1f}c")
                        log_m('CICLO 20s', 'SKIP', f"Remaining: {rstr} | {e_side} sem compra — {' | '.join(reasons)}")

# =============================================================================
# ============================= MAIN ==========================================
# =============================================================================

async def main():
    global daily_profit, last_day, price_change, bankroll, risk_multiplier, kelly, as_model

    kelly    = EmpiricalKelly()
    as_model = AvellanedaStoikov()
    log_info("BOT INICIADO v0.32.0")
    log_info(f"LIVE_TRADING: {LIVE_TRADING} | PRIVATE_KEY: {'***' if POLYMARKET_PRIVATE_KEY else 'NÃO ENCONTRADO'}")
    log_info(f"EIGHTY: Janela [{EIGHTY_START_REM_S}s → {EIGHTY_CUTOFF_S}s] | Delta={EIGHTY_DELTA_LOOKBACK_S}s | Vol={EIGHTY_VOL_MAX_C}c/{EIGHTY_VOL_WINDOW_S}s | BuyCooldown={EIGHTY_BUY_COOLDOWN}s")
    log_info(f"Kelly: {'ATIVO' if KELLY_ACTIVE else 'OFF'} | max={KELLY_MAX_FRACTION:.0%} min={KELLY_MIN_FRACTION:.0%} | MC={KELLY_MC_SIMULATIONS} conf={KELLY_CONFIDENCE:.0%}")
    log_info(f"AS+VPIN: {'ATIVO' if AS_VPIN_ACTIVE else 'OFF'} | γ={AS_GAMMA} κ={AS_KAPPA_DEFAULT} | widen@{AS_VPIN_WIDEN} withdraw@{AS_VPIN_WITHDRAW}")
    log_info(f"Módulos activos: EIGHTY={EIGHTY_ACTIVE} | PEG_ARBIT={PEG_ARBIT_ACTIVE} | CICLO_30S={CICLO_30S_ACTIVE} | CICLO_20S={CICLO_20S_ACTIVE}")

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
            if profit_this < 0:
                # Perda -> dobra o martingale
                risk_multiplier = min(risk_multiplier * 2.0, MAX_RISK_MULTIPLIER)
                log_info(
                    f"ROUND | PnL: ${profit_this:+.4f} ({pnl_pct:+.2f}%) "
                    f"| MARTINGALE → x{risk_multiplier:.0f} (cap: x{MAX_RISK_MULTIPLIER:.0f})"
                )
            elif profit_this == 0.0 and risk_multiplier > 1.0:
                # Round sem trades com martingale activo -> mantem multiplicador para o proximo round
                log_info(
                    f"ROUND | PnL: ${profit_this:+.4f} ({pnl_pct:+.2f}%) "
                    f"| MARTINGALE → x{risk_multiplier:.0f} (cap: x{MAX_RISK_MULTIPLIER:.0f})"
                )
            else:
                # PnL positivo (ou zero sem martingale activo) -> reset
                risk_multiplier = 1.0
                log_info(f"ROUND | PnL: ${profit_this:+.4f} ({pnl_pct:+.2f}%)")

            log_info(f"TOTAL | PnL: ${daily_profit:+.4f} ({daily_pct:+.2f}%) | Bank: ${bankroll:.2f} | Uptime: {get_uptime_str()}")
            log_sep()
        else:
            log_info("Sem preços recebidos neste ciclo — a saltar")

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