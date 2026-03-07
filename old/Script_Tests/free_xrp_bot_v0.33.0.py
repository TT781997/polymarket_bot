# =============================================================================
# BOT XRP POLYMARKET v0.38.0
# =============================================================================
# CHANGELOG v0.38.0 (relative ao v0.37.0):
# [fix]  EIGHTY: restaurados todos os filtros removidos na v0.37.0
#        (volatilidade, delta, PEG min, cooldown entre compras, cutoff/start log,
#         vol_cooldown, rapid-rise, AS edge check)
# [fix]  PEG ARBIT: restaurado completamente no logic_loop (estava ausente)
# [fix]  CICLO 30s/20s: restaurados completamente no logic_loop (estavam ausentes)
# [fix]  Bug: active_trades nao era limpo no FIM DE MERCADO (lista infinita)
# [fix]  PEG_ARBIT_RANGE=(50.0,50.0) estava quebrado -> restaurado (35.0,65.0)
# [fix]  ROUND log restaurado: PnL por ronda + preview Martingale em perda
# [fix]  SL_TICK / SL_RESET logs restaurados na seccao TARGET+SL
# [fix]  AS+VPIN gate restaurado no topo do loop (as_blocked)
# [fix]  log_raw por tick restaurado (tick de preco visivel no log)
# [fix]  open_trade log restaurado: fee%, peg_val, extra_log
# [fix]  close_trade log restaurado: PnL%
# [fix]  `now` calculado uma unica vez por iteracao do loop (consistencia)
# [keep] BotState dataclass (bom refactor da v0.37.0 — mantido)
# [keep] PRICE_STEP_C parametro configuravel para bucketing de niveis
# [keep] best_bids / best_asks separados (saidas vs entradas)
# =============================================================================

import asyncio
import json
import logging
import math
import os
import time
import requests
import numpy as np
import websockets
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Any, Dict, Optional

# =============================================================================
#
#  PARAMETROS CONFIGURÁVEIS
#
#  CONVENCAO DE UNIDADES:
#    _C   -> cents  (ex: 97.0 = 0.97 dolar = 97 centavos)
#    _S   -> segundos (float)
#    ratio/risk/fraction -> 0.0 a 1.0  (ex: 0.05 = 5%)
#    mult/factor         -> >= 1.0
#
# =============================================================================

# --- SECCAO 0 — MODO DE OPERACAO ---------------------------------------------

LIVE_TRADING = False
# True  = executa ordens reais na Polymarket (requer secrets.txt com chave privada)
#         e le o saldo real da carteira no inicio de cada ciclo.
# False = simulacao: toda a logica corre, logs completos, zero dinheiro gasto.
# Apenas mudar para True apos testar exaustivamente em False.

# --- SECCAO 1 — BANCA --------------------------------------------------------

BANKROLL_INIT = 10.0
# Banca inicial em USDC.
# Em modo Demo (LIVE=False): banca persiste entre dias — nunca e resetada.
# Em modo Live (LIVE=True): substituida pelo saldo real da carteira Polymarket.
# Range: [5.0 | 100000.0]

# --- SECCAO 2 — RISCO BASE POR MODULO ----------------------------------------

RISK_PER_TRADE = 0.05
# Fraccao da banca por trade nos Ciclos e trades genericos.
# Representa o risco BASE antes de qualquer multiplicador Martingale ou Recovery.
# Exemplo: 0.05 = 5% de banca actual por trade em condicoes normais.
# Range: [0.01 | 0.10]

EIGHTY_RISK = 0.07
# Fraccao da banca por trade do modulo EIGHTY.
# Separado do RISK_PER_TRADE porque o EIGHTY tem criterios de entrada proprios.
# Exemplo: 0.07 = 7% de $10.00 = $0.70 por trade em condicoes normais.
# Range: [0.01 | 0.10]

PEG_ARBIT_RISK = 0.14
# Fraccao da banca investida no PEG ARBIT por leg (UP e DOWN independentemente).
# O PEG e lucro quase garantido, mas o cap de MAX_RISK_PERCENT continua inviolavel.
# Exemplo: 0.10 = 10% de $10.00 = $1.00 por leg.
# Range: [0.05 | 0.12]

# --- SECCAO 3 — MARTINGALE E RECOVERY SUAVE ----------------------------------
#
# COMO FUNCIONA O SISTEMA HIBRIDO:
#
#   Apos ronda com PERDA:
#     1. risk_multiplier dobra: x1 -> x2 -> x4 -> x8 (cap: MAX_RISK_MULTIPLIER)
#     2. accumulated_loss += abs(pnl) * 0.5   (50% da perda entra no acumulado)
#     3. recovery_rounds  += RECOVERY_ROUNDS_PER_LOSS (default: +10)
#
#   Apos ronda com LUCRO:
#     1. risk_multiplier = 1
#     2. accumulated_loss = max(0, accumulated_loss - pnl)
#     3. recovery_rounds  = max(0, recovery_rounds - 1)
#
#   Apos ronda com PnL == 0.0 (sem trades):
#     Tudo mantem-se — multiplier, accumulated_loss e recovery_rounds inalterados.
#
#   Calculo do risco efectivo por trade:
#     recovery_fraction = (accumulated_loss / recovery_rounds) / bankroll
#     raw_risk          = (base x mult) + recovery_fraction
#     eff_risk          = min(raw_risk, MAX_RISK_PERCENT)   <- INVIOLAVEL

MAX_RISK_MULTIPLIER = 8
# Limite maximo do multiplicador Martingale.
# x8 = apos 3 perdas consecutivas (x1 -> x2 -> x4 -> x8).
# Range: [2 | 8]  (potencias de 2)

MAX_RISK_PERCENT = 0.50
# CAP ABSOLUTO E INVIOLAVEL de risco por trade (qualquer modulo).
# Independentemente do Martingale, Recovery ou qualquer outro factor,
# nenhum trade pode investir mais de MAX_RISK_PERCENT da banca actual.
# Nao reduzir abaixo de 0.20 (mata PEG_ARBIT); nao subir acima de 0.50 (ruina).
# Range: [0.20 | 0.50]

RECOVERY_ROUNDS_PER_LOSS = 10
# Numero de rondas adicionadas ao prazo de recovery por cada ronda com perda.
# Uma perda acrescenta 10 rondas; um lucro desconta 1 ronda do contador.
# Range: [5 | 20]

# --- SECCAO 4 — STOP-LOSS INTRA-TRADE DINAMICO ------------------------------
#
# Funciona DURANTE o mercado, tick a tick, sobre posicoes abertas.
# Usa o mesmo sistema de niveis unicos do EIGHTY, mas invertido (descendo).
# Avaliado contra o BID real — preco real de saida como taker.
#
# Anti-flash-crash: um spike de 1-4 niveis abaixo do threshold reseta
# ao subir e nunca chega ao gatilho. Precisas de STOPLOSS_TICKS niveis
# distintos para confirmar que a descida e estrutural.
#
# Nota: PEG_ARBIT nunca e fechado por stop-loss — resolve sempre a 100c.

STOPLOSS_PRICE_C = 25.0
# Preco do BID efectivo (cents) abaixo do qual o contador começa a incrementar.
# Range: [20.0 | 60.0]

STOPLOSS_TICKS = 8
# Numero de niveis de preco BID efectivo UNICOS (bucketed por PRICE_STEP_C)
# abaixo de STOPLOSS_PRICE_C necessarios para fechar a posicao.
# Um nivel acima do threshold reseta o set a zero (flash crash descartado).
# Range: [3 | 10]

PRICE_STEP_C = 0.5
# Tamanho do degrau de discretizacao de preco para contar niveis unicos.
# Usado no EIGHTY (subida) e no Stop-Loss (descida) para ignorar micro-ruido.
# Exemplo: 1.0 = agrupa em degraus de 1 centimo (39c, 38c, 37c...).
# Range: [0.1 | 2.0]

# --- SECCAO 5 — TOGGLES DE MODULOS ------------------------------------------

CICLO_30S_ACTIVE = False
# Estrategia de ciclo de 30 segundos (snapshot + verificacao de volatilidade + compra).

CICLO_20S_ACTIVE = False
# Estrategia de ciclo de 20 segundos. Identica ao 30s mas em janela mais curta.

EIGHTY_ACTIVE = True
# Estrategia EIGHTY: compra quando o preco efectivo esta no range definido e ha
# pelo menos EIGHTY_MIN_TICKS niveis distintos de consolidacao (usando PRICE_STEP_C).

PEG_ARBIT_ACTIVE = True
# Arbitragem PEG: compra UP e DOWN com shares iguais quando Eff_UP+Eff_DOWN < 100c.
# Lucro quase garantido — um dos lados resolve sempre a 100c.

KELLY_ACTIVE = False
# Empirical Kelly com Monte Carlo para sizing dinamico.
# Requer KELLY_MIN_HISTORY trades antes de activar (usa fallback ate la).

AS_VPIN_ACTIVE = False
# Avellaneda-Stoikov + VPIN: detecta fluxo toxico e bloqueia entradas de risco.

# --- SECCAO 6 — CICLOS (30s e 20s) ------------------------------------------

CYCLE_PRICE_MIN_C = 74.0
# Preco EFECTIVO minimo em cents para entrar num ciclo.
# Range: [50.0 | 85.0]

CYCLE_PRICE_MAX_C = 85.0
# Preco EFECTIVO maximo em cents para entrar num ciclo.
# Range: [75.0 | 92.0]

CYCLE_PEG_MIN_C = 96.5
# PEG_Eff minimo (soma Eff_UP + Eff_DOWN em cents) para aceitar o ciclo.
# Range: [94.0 | 99.5]

CYCLE_VOL_MAX_C = 5.2
# Variacao maxima do preco efectivo entre snapshot e verificacao (em cents).
# Range: [1.0 | 10.0]

CYCLE_TARGET_C = 0.0
# Target de venda antecipada em cents (0.0 = hold ate ao fim do mercado).

CYCLE_30S_SNAPSHOT_REM  = 35.0  # Remaining para snapshot do ciclo 30s  [25.0 | 60.0]
CYCLE_30S_VOL_CHECK_REM = 30.0  # Remaining para verificar volatilidade  [20.0 | 55.0]
CYCLE_30S_BUY_REM       = 29.8  # Remaining para executar a compra       [19.8 | 54.8]

CYCLE_20S_SNAPSHOT_REM  = 25.0  # Idem para o ciclo 20s  [15.0 | 50.0]
CYCLE_20S_VOL_CHECK_REM = 20.0  #                        [10.0 | 45.0]
CYCLE_20S_BUY_REM       = 19.8  #                        [ 9.8 | 44.8]

# --- SECCAO 7 — EIGHTY -------------------------------------------------------

EIGHTY_START_REM_S = 300
# Remaining (segundos) a partir do qual o EIGHTY fica activo.
# 300 = activo desde o inicio do mercado de 5 minutos.
# Range: [30 | 300]

EIGHTY_MIN_EFF_C = 80.0
# Preco EFECTIVO minimo para o EIGHTY comprar.
# Range: [70.0 | 90.0]

EIGHTY_MAX_EFF_C = 98.5
# Preco EFECTIVO maximo para o EIGHTY comprar.
# Range: [95.0 | 99.5]

EIGHTY_MIN_TICKS = 5
# Numero minimo de niveis de preco EFECTIVO distintos (bucketed por PRICE_STEP_C)
# para confirmar consolidacao. Mais ticks = mais selectivo, menos entradas.
# Range: [3 | 12]

EIGHTY_CUTOFF_S = 5
# Para o EIGHTY quando faltam X segundos para o fim do mercado.
# Range: [0 | 30]

EIGHTY_WHEN_CUTOFF_0_VOLT = 35.0
# Se EIGHTY_CUTOFF_S=0: ignora verificacoes de volatilidade nos ultimos X segundos.
# Range: [0.0 | 60.0]

EIGHTY_PEG_MIN_C = 97.0
# PEG_Eff minimo em cents para aceitar entrada EIGHTY.
# Range: [94.0 | 99.5]

EIGHTY_BUY_COOLDOWN = 4.0
# Segundos minimos entre compras consecutivas do mesmo lado.
# Range: [1.0 | 15.0]

EIGHTY_VOL_WINDOW_S = 5.0
# Janela temporal para calcular volatilidade interna (segundos).
# Range: [2.0 | 15.0]

EIGHTY_VOL_MAX_C = 4.5
# Variacao maxima do preco efectivo dentro de EIGHTY_VOL_WINDOW_S.
# Range: [1.0 | 8.0]

EIGHTY_VOL_COOLDOWN_S = 5.0
# Apos detectar volatilidade excessiva, bloqueia o lado por X segundos.
# Range: [2.0 | 15.0]

EIGHTY_DELTA_VOL_RISE_C = 3.5  # Delta de subida rapida que activa cooldown  [1.0 | 6.0]
EIGHTY_DELTA_VOL_TIME_S = 1.5  # Janela temporal para detectar subida rapida [0.5 | 3.0]

EIGHTY_TARGET_C = 0.0
# Target de venda antecipada (0.0 = hold ate ao fim — recomendado).

# --- SECCAO 8 — PEG ARBITRAGE ------------------------------------------------

PEG_ARBIT_RANGE = (49.0, 51.0)
# Range de preco EFECTIVO (cents) em que ambos os lados devem estar para activar o arb.
# Fora disto o mercado ja decidiu e o arb torna-se arriscado.
# Range: [(20.0, 80.0) ... maximo (30.0, 70.0) recomendado]

PEG_ARBIT_UNDERPEG_C = 1.5
# Desvio minimo do PEG efectivo para activar o arb (em cents).
# Range: [0.3 | 5.0]

PEG_ARBIT_COOLDOWN = 0.05
# Intervalo minimo entre duas entradas PEG consecutivas (segundos).
# Range: [0.01 | 1.0]

PEG_ARBIT_MIN_REM = 2.0
# Remaining minimo para entrar num PEG.
# Range: [2.0 | 30.0]

MAX_PEG_ENTRIES = 10_000_000
# Maximo de entradas PEG por ciclo. Range: [1 | ilimitado]

PEG_ARBIT_TARGET_C = 0.0
# Target de venda do PEG (0.0 = hold ate ao fim — SEMPRE recomendado).

TARGET_MULTIPLIER = 1.25
# Multiplicador do preco efectivo para trades sem target fixo.
# Range: [1.05 | 2.0]

# --- SECCAO 9 — EMPIRICAL KELLY COM MONTE CARLO ------------------------------

KELLY_MC_SIMULATIONS = 5000   # Simulacoes Monte Carlo  [1000 | 20000]
KELLY_CONFIDENCE     = 0.90   # Percentil de sobrevivencia exigido  [0.70 | 0.99]
KELLY_MIN_HISTORY    = 10     # Minimo de trades para usar Kelly  [5 | 50]
KELLY_MAX_FRACTION   = 0.12   # Cap maximo do Kelly  [0.05 | 0.15]
KELLY_MIN_FRACTION   = 0.02   # Floor minimo do Kelly  [0.01 | 0.05]
KELLY_RUIN_THRESHOLD = 0.50   # Se MC preve perder >50%, corta a fraccao a metade  [0.20 | 0.80]

# --- SECCAO 10 — AVELLANEDA-STOIKOV + VPIN ----------------------------------

AS_GAMMA               = 0.05   # Aversao ao risco  [0.01 | 0.20]
AS_KAPPA_DEFAULT       = 1.0    # Taxa de chegada de ordens (ticks/s)  [0.1 | 10.0]
AS_VPIN_WINDOW         = 50     # Ticks para calcular VPIN  [10 | 200]
AS_VPIN_WIDEN          = 0.70   # VPIN acima disto -> alarga spread  [0.50 | 0.85]
AS_VPIN_WITHDRAW       = 0.90   # VPIN acima disto -> bloqueia entradas  [0.70 | 0.99]
AS_SPREAD_WIDEN_FACTOR = 1.1    # Factor de alargamento  [1.0 | 2.0]
AS_MIN_EDGE_C          = 0.1    # Edge minimo em cents  [0.0 | 2.0]

# --- SECCAO 11 — FEES E PARAMETROS DE LOOP ----------------------------------

FEE_RATE = 0.25
# Taxa base da Polymarket. fee = FEE_RATE * (p*(1-p))^FEE_EXP. NAO ALTERAR.

FEE_EXP = 2
# Expoente da curva de fee. NAO ALTERAR.

ASK_SPREAD = 0.01
# Slippage buffer adicionado ao ask para simular pioria taker.
# Com bids reais disponiveis, actua apenas como margem de segurança.
# Range: [0.005 | 0.02]

LOOP_SLEEP = 0.0005
# Timeout maximo entre iteracoes do loop principal (segundos).
# Range: [0.0001 | 0.01]

# =============================================================================
# ESTADO GLOBAL DO BOT (DATACLASS)
# =============================================================================

@dataclass
class BotState:
    """Encapsula todo o estado mutavel do bot."""
    bankroll:         float = BANKROLL_INIT
    daily_profit:     float = 0.0
    last_day:         Optional[date] = None
    bot_start_time:   float = field(default_factory=time.time)

    # Order book em tempo real — asks para entradas, bids para saidas
    best_asks: Dict[str, Optional[float]] = field(
        default_factory=lambda: {'up': None, 'down': None}
    )
    best_bids: Dict[str, Optional[float]] = field(
        default_factory=lambda: {'up': None, 'down': None}
    )

    # Modelos
    kelly:    Any = None
    as_model: Any = None

    # Martingale + Recovery
    risk_multiplier:  float = 1.0
    accumulated_loss: float = 0.0
    recovery_rounds:  int   = 0


state = BotState()

# =============================================================================
# LOGGING
# =============================================================================

_formatter    = logging.Formatter('%(message)s')
_file_handler = logging.FileHandler('bot_xrp.log', encoding='utf-8')
_file_handler.setFormatter(_formatter)
logger = logging.getLogger('bot_xrp')
logger.setLevel(logging.DEBUG)
logger.addHandler(_file_handler)
logger.propagate = False

def get_ts()  -> str: return datetime.now().strftime("%d/%m/%y | %H:%M:%S.%f")[:-3]
def log_m(module: str, action: str, msg: str) -> None:
    logger.info(f"[INFO] [{module}] [{action}] [{get_ts()}] | {msg}")
def log_raw(msg: str)  -> None: logger.info(f"[{get_ts()}] | {msg}")
def log_info(msg: str) -> None: logger.info(f"[INFO] [{get_ts()}] | {msg}")
def log_warn(msg: str) -> None: logger.warning(f"[WARN] [{get_ts()}] | {msg}")
def log_sep()  -> None: logger.info("-" * 80)
def log_sep2() -> None: logger.info("=" * 80)

# =============================================================================
# SECRETS & SDK LIVE
# =============================================================================

def load_secrets(filepath: str = "secrets.txt") -> dict:
    if not os.path.exists(filepath):
        logger.warning("[WARN] secrets.txt nao encontrado — LIVE_TRADING nao disponivel")
        return {}
    secrets: dict = {}
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                secrets[k.strip()] = v.strip()
    return secrets

credenciais            = load_secrets()
POLYMARKET_PRIVATE_KEY = credenciais.get("POLYMARKET_PRIVATE_KEY", "")

if LIVE_TRADING and not POLYMARKET_PRIVATE_KEY:
    logger.error("[ERROR] LIVE_TRADING=True mas POLYMARKET_PRIVATE_KEY nao encontrado!")
    raise SystemExit(1)

clob_client = None
if LIVE_TRADING:
    try:
        from py_clob_client.client import ClobClient          # type: ignore
        from py_clob_client.clob_types import OrderArgs       # type: ignore
        from py_clob_client.order_builder.constants import BUY, SELL  # type: ignore
        clob_client = ClobClient(
            host="https://clob.polymarket.com",
            key=POLYMARKET_PRIVATE_KEY,
            chain_id=137
        )
        logger.info("[INFO] SDK Polymarket carregado — LIVE TRADING ACTIVO")
    except ImportError:
        logger.error("[ERROR] py-clob-client nao instalado! pip install py-clob-client")
        raise SystemExit(1)


def fetch_wallet_balance() -> Optional[float]:
    """Le o saldo USDC real da carteira Polymarket com 3 fallbacks."""
    if not clob_client:
        return None
    for attempt in range(3):
        try:
            if attempt == 0 and hasattr(clob_client, 'get_balance'):
                result = clob_client.get_balance()
                if isinstance(result, dict):
                    for key in ('USDC', 'usdc', 'balance', 'amount'):
                        if key in result:
                            val = float(result[key])
                            if val > 0:
                                return val
            elif attempt == 1:
                resp = requests.get(
                    "https://clob.polymarket.com/balance",
                    headers={"Authorization": f"Bearer {POLYMARKET_PRIVATE_KEY}"},
                    timeout=4
                )
                if resp.status_code == 200:
                    data = resp.json()
                    for key in ('USDC', 'usdc', 'balance', 'amount'):
                        if key in data:
                            return float(data[key])
            elif attempt == 2:
                resp = requests.get(
                    "https://gamma-api.polymarket.com/wallet/balance",
                    headers={"Authorization": f"Bearer {POLYMARKET_PRIVATE_KEY}"},
                    timeout=4
                )
                if resp.status_code == 200:
                    data = resp.json()
                    for key in ('USDC', 'usdc', 'balance'):
                        if key in data:
                            return float(data[key])
        except Exception:
            pass
    return None

# =============================================================================
# MATEMATICA E HELPERS
# =============================================================================

# Fee lookup table: pre-calculada para 0.001 a 0.999 em passos de 0.001
_FEE_LOOKUP: Dict[int, float] = {
    i: FEE_RATE * ((i / 1000.0) * (1.0 - i / 1000.0)) ** FEE_EXP
    for i in range(1, 1000)
}

def fee_rate(p: float) -> float:
    """Fee da Polymarket via lookup table (zero recalculo no hot loop)."""
    return _FEE_LOOKUP.get(int(round(p * 1000)),
                           FEE_RATE * (p * (1.0 - p)) ** FEE_EXP)

# Estas 4 funcoes NAO sao alteradas — matematicamente correctas.
def buy_shares_net(invested: float, ask: float) -> float:
    return (invested / ask) * (1.0 - fee_rate(ask))

def effective_entry(ask: float) -> float:
    f = fee_rate(ask)
    return ask / (1.0 - f) if f < 1.0 else ask

def sell_payout(shares: float, p: float) -> float:
    return shares * p * (1.0 - fee_rate(p))

def fc(p: float) -> str:
    return f"{p * 100:.1f}c"

def get_remaining_str(rem: float) -> str:
    rem = max(0.0, rem)
    return f"{int(rem // 60):02d}:{int(rem % 60):02d}:{int((rem * 1000) % 1000):03d}"

def get_uptime_str() -> str:
    elapsed = int(time.time() - state.bot_start_time)
    years,  elapsed = divmod(elapsed, 365 * 24 * 3600)
    months, elapsed = divmod(elapsed, 30  * 24 * 3600)
    days,   elapsed = divmod(elapsed, 24  * 3600)
    hours,  elapsed = divmod(elapsed, 3600)
    mins,   secs    = divmod(elapsed, 60)
    return f"{years}y:{months:02d}m:{days:02d}d:{hours:02d}h:{mins:02d}m:{secs:02d}s"


def calc_risk(base: float, mult: float, accum_loss: float,
              rec_rounds: int, bank: float) -> float:
    """
    Risco efectivo = min((base x mult) + (accum_loss/rec_rounds/bank), MAX_RISK_PERCENT).
    Cap de MAX_RISK_PERCENT e INVIOLAVEL em qualquer combinacao de parametros.
    """
    if bank <= 0:
        return MAX_RISK_PERCENT
    recovery_fraction = (accum_loss / rec_rounds) / bank if rec_rounds > 0 else 0.0
    return min((base * mult) + recovery_fraction, MAX_RISK_PERCENT)

# =============================================================================
# API / METADATA
# =============================================================================

def fetch_metadata(slug: str) -> Optional[dict]:
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

# =============================================================================
# WEBSOCKET
# =============================================================================

async def ws_handler(t_up: str, t_down: str, q: asyncio.Queue) -> None:
    """
    Conecta ao order book Polymarket e coloca ticks numa Queue.
    Captura best_ask e best_bid de ambos os eventos (book + best_bid_ask).
    COMPRA -> usa ask  |  VENDA/SL -> usa bid
    """
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    sub = json.dumps({
        "assets_ids":             [t_up, t_down],
        "type":                   "market",
        "custom_feature_enabled": True
    })
    while True:
        try:
            async with websockets.connect(
                uri,
                ping_interval=20,
                ping_timeout=10,
                max_size=2**20,
                open_timeout=10
            ) as ws:
                await ws.send(sub)
                log_info("WS conectado ao order book Polymarket")
                async for raw in ws:
                    items = json.loads(raw)
                    if not isinstance(items, list):
                        items = [items]
                    tick: Dict[str, float] = {}
                    for item in items:
                        aid = item.get("asset_id")
                        ask = bid = None
                        evt = item.get("event_type")
                        if evt == "book":
                            asks = item.get("asks")
                            if asks:
                                valid = [float(d['price']) for d in asks
                                         if float(d['size']) > 0]
                                if valid:
                                    ask = min(valid)
                            bids = item.get("bids")
                            if bids:
                                valid = [float(d['price']) for d in bids
                                         if float(d['size']) > 0]
                                if valid:
                                    bid = max(valid)
                        elif evt == "best_bid_ask":
                            ba = item.get("best_ask")
                            bb = item.get("best_bid")
                            if ba: ask = float(ba)
                            if bb: bid = float(bb)
                        if aid == t_up:
                            if ask is not None: tick['up_ask']   = ask
                            if bid is not None: tick['up_bid']   = bid
                        elif aid == t_down:
                            if ask is not None: tick['down_ask'] = ask
                            if bid is not None: tick['down_bid'] = bid
                    if tick:
                        if 'up_ask'   in tick: state.best_asks['up']   = tick['up_ask']
                        if 'down_ask' in tick: state.best_asks['down'] = tick['down_ask']
                        if 'up_bid'   in tick: state.best_bids['up']   = tick['up_bid']
                        if 'down_bid' in tick: state.best_bids['down'] = tick['down_bid']
                        try:
                            q.put_nowait(tick)
                        except asyncio.QueueFull:
                            pass
        except asyncio.CancelledError:
            break
        except Exception as e:
            log_warn(f"WS erro: {e} — reconectando em 1s")
            await asyncio.sleep(1)

# =============================================================================
# LIVE ORDER
# =============================================================================

async def place_live_order(side: str, price: float, shares: float,
                            token_id: str) -> bool:
    """Envia ordem real via SDK em executor para nao bloquear o event loop."""
    if not clob_client:
        return False
    def _place() -> bool:
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
                f"LIVE ORDER OK | {side} {token_id[:8]}... @ {price:.4f}"
                f" | Size: {shares:.6f} | OrderID: {response.get('orderID', 'OK')}"
            )
            return True
        except Exception as e:
            log_warn(f"LIVE ORDER falhou: {e}")
            return False
    return await asyncio.get_event_loop().run_in_executor(None, _place)

# =============================================================================
# PRICE BUFFER
# =============================================================================

class PriceBuffer:
    """Buffer circular de precos com timestamps para calculo de deltas."""
    __slots__ = ('max_age', 'buffer')

    def __init__(self, max_age_seconds: float = 30.0) -> None:
        self.max_age = max_age_seconds
        self.buffer: deque = deque()

    def add(self, eff_c: float, ts: float) -> None:
        self.buffer.append((ts, eff_c))
        cutoff = ts - self.max_age
        while self.buffer and self.buffer[0][0] < cutoff:
            self.buffer.popleft()

    def get_price_at(self, seconds_ago: float, tolerance: float = 1.0) -> Optional[float]:
        target_ts  = time.time() - seconds_ago
        best_price = None
        best_diff  = tolerance + 1.0
        for ts, eff_c in self.buffer:
            d = abs(ts - target_ts)
            if d < best_diff:
                best_diff  = d
                best_price = eff_c
        return best_price

    def get_age(self) -> float:
        return (time.time() - self.buffer[0][0]) if self.buffer else 0.0

    def get_delta(self, seconds_ago: float) -> tuple[Optional[float], bool]:
        if not self.buffer:
            return None, False
        past = self.get_price_at(seconds_ago)
        if past is None:
            return None, False
        return self.buffer[-1][1] - past, True

    def clear(self) -> None:
        self.buffer.clear()

# =============================================================================
# EMPIRICAL KELLY COM MONTE CARLO
# =============================================================================

class EmpiricalKelly:
    __slots__ = ('returns',)

    def __init__(self) -> None:
        self.returns: list[float] = []

    def add_result(self, invested: float, payout: float) -> None:
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
            return KELLY_MIN_FRACTION, f"Kelly edge negativo (mu={mean_r:.3f})"
        cv_edge     = min(std_r / mean_r, 1.0)
        denom       = mean_r ** 2 + std_r ** 2
        f_kelly     = (mean_r / denom) if denom > 0 else fallback
        f_empirical = f_kelly * (1.0 - cv_edge)
        rng         = np.random.default_rng()
        sim_returns = rng.choice(arr, size=(KELLY_MC_SIMULATIONS, max(n, 20)), replace=True)
        growth      = np.prod(1.0 + f_empirical * sim_returns, axis=1)
        worst_case  = float(np.percentile(growth, (1.0 - KELLY_CONFIDENCE) * 100))
        ruin_note   = ""
        if worst_case < (1.0 - KELLY_RUIN_THRESHOLD):
            f_empirical *= 0.5
            ruin_note    = " [MC ruin -> halved]"
        f_final = max(KELLY_MIN_FRACTION, min(KELLY_MAX_FRACTION, f_empirical))
        return f_final, (
            f"Kelly f={f_final:.3f} | f_kelly={f_kelly:.3f} | CV={cv_edge:.2f} | "
            f"mu={mean_r:.3f} sigma={std_r:.3f} | MC_worst={worst_case:.3f}{ruin_note} | n={n}"
        )

# =============================================================================
# AVELLANEDA-STOIKOV + VPIN
# =============================================================================

class AvellanedaStoikov:
    __slots__ = ('tick_history', 'vol_history', 'inventory_up', 'inventory_down', '_kappa')

    def __init__(self) -> None:
        self.tick_history: deque = deque(maxlen=AS_VPIN_WINDOW * 2)
        self.vol_history:  deque = deque(maxlen=100)
        self.inventory_up   = 0.0
        self.inventory_down = 0.0
        self._kappa         = AS_KAPPA_DEFAULT

    def add_tick(self, price: float, prev_price: Optional[float]) -> None:
        direction = (1 if price > prev_price else -1 if price < prev_price else 0) \
                    if prev_price is not None else 0
        self.tick_history.append((time.time(), price, direction))
        self.vol_history.append(price)
        th = self.tick_history
        if len(th) >= 10:
            span = th[-1][0] - th[0][0]
            if span > 0:
                self._kappa = len(th) / span

    def update_inventory(self, side: str, shares: float, is_buy: bool) -> None:
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

    def get_min_edge_c(self, mid_c: float, q: float,
                        t_remaining: float) -> tuple[Optional[float], str]:
        if not AS_VPIN_ACTIVE:
            return AS_MIN_EDGE_C, "AS/VPIN OFF"
        vpin_val = self.vpin
        sig2     = self.sigma2
        inv_term = AS_GAMMA * sig2 * t_remaining / 2.0
        liq_term = (1.0 / AS_GAMMA) * math.log(1.0 + AS_GAMMA / self._kappa) \
                   if AS_GAMMA > 0 else 0.0
        half_d   = (inv_term + liq_term) * 100.0
        r        = (mid_c / 100.0 - q * AS_GAMMA * sig2 * t_remaining) * 100.0
        if vpin_val >= AS_VPIN_WITHDRAW:
            return None, (
                f"VPIN={vpin_val:.2f}>={AS_VPIN_WITHDRAW} | "
                f"r={r:.1f}c delta/2={half_d:.2f}c sigma2={sig2:.5f} BLOQUEADO"
            )
        widen    = AS_SPREAD_WIDEN_FACTOR if vpin_val >= AS_VPIN_WIDEN else 1.0
        min_edge = max(AS_MIN_EDGE_C, half_d * widen)
        return min_edge, (
            f"VPIN={vpin_val:.2f} | r={r:.1f}c | delta/2={half_d:.2f}c | "
            f"min_edge={min_edge:.2f}c"
            + (f" [WIDEN x{AS_SPREAD_WIDEN_FACTOR}]" if widen > 1 else "")
        )

# =============================================================================
# LOGIC LOOP
# =============================================================================

async def logic_loop(m_start: float, m_end: float, meta: dict,
                     price_q: asyncio.Queue) -> None:
    """
    Loop principal de trading para um ciclo de 5 minutos.
    Entradas  -> usa ASK  (preco de compra como taker)
    Saidas/SL -> usa BID  (preco de venda como taker)
    """
    active_trades: list[dict] = []
    flags:   dict = {
        's35': False, 'v30': False, 'd29': False,
        's25': False, 'v20': False, 'd19': False
    }
    c_state: dict = {'c1': {}, 'c2': {}}

    # Riscos efectivos calculados UMA vez no inicio da ronda
    eff_risk_per_trade = calc_risk(RISK_PER_TRADE,  state.risk_multiplier,
                                   state.accumulated_loss, state.recovery_rounds,
                                   state.bankroll)
    eff_eighty_risk    = calc_risk(EIGHTY_RISK,     state.risk_multiplier,
                                   state.accumulated_loss, state.recovery_rounds,
                                   state.bankroll)
    eff_peg_risk       = calc_risk(PEG_ARBIT_RISK,  state.risk_multiplier,
                                   state.accumulated_loss, state.recovery_rounds,
                                   state.bankroll)

    # Log de estado do Martingale no inicio da ronda (apenas se activo)
    if state.risk_multiplier > 1.0 or state.accumulated_loss > 0:
        rec_frac = (
            (state.accumulated_loss / state.recovery_rounds) / state.bankroll
            if state.recovery_rounds > 0 and state.bankroll > 0 else 0.0
        )
        log_info(
            f"MARTINGALE | x{state.risk_multiplier:.0f} | "
            f"accum=${state.accumulated_loss:.4f} | "
            f"rec_rounds={state.recovery_rounds} | rec_frac={rec_frac:.2%} | "
            f"eff: EIGHTY={eff_eighty_risk:.1%}"
            + (" [CAP]" if eff_eighty_risk >= MAX_RISK_PERCENT else "") +
            f" PEG={eff_peg_risk:.1%}"
            + (" [CAP]" if eff_peg_risk >= MAX_RISK_PERCENT else "") +
            f" (cap={MAX_RISK_PERCENT:.0%})"
        )

    # Estado do EIGHTY
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
    prev_u_p = prev_d_p = None

    # Header de ronda
    mult_tag = f" [MARTINGALE x{state.risk_multiplier:.0f}]" if state.risk_multiplier > 1.0 else ""
    mods = []
    if EIGHTY_ACTIVE:    mods.append(f"EIGHTY({EIGHTY_START_REM_S}s->{EIGHTY_CUTOFF_S}s)")
    if CICLO_30S_ACTIVE: mods.append("CICLO_30S")
    if CICLO_20S_ACTIVE: mods.append("CICLO_20S")
    if PEG_ARBIT_ACTIVE: mods.append(
        f"PEG_ARBIT(range {PEG_ARBIT_RANGE[0]:.0f}-{PEG_ARBIT_RANGE[1]:.0f}c)"
    )
    log_sep2()
    log_info(f"NOVO CICLO | Market: {meta['slug']} | LIVE: {LIVE_TRADING}")
    log_info(
        f"   Banca: ${state.bankroll:.4f} | "
        f"Profit acum.: ${state.daily_profit:.4f}{mult_tag}"
    )
    log_info(f"   Modulos: {' | '.join(mods) if mods else 'nenhum'}")
    log_info(
        f"   Risco efectivo: EIGHTY={eff_eighty_risk:.1%} | "
        f"PEG={eff_peg_risk:.1%} | CICLOS={eff_risk_per_trade:.1%} | "
        f"CAP={MAX_RISK_PERCENT:.0%}"
    )
    log_sep()
    log_info("   ESCUTA ACTIVA")
    log_sep()

    # -------------------------------------------------------------------------
    def pct_banca(invested: float) -> str:
        base = state.bankroll + invested
        return f"{invested / base * 100:.1f}% banca" if base else "-"

    # -------------------------------------------------------------------------
    async def open_trade(
        side: str, nom: float, trade_type: str, rstr: str,
        risk: float = None, wait_close: bool = False,
        fixed_invest: float = None, peg_val: float = None,
        token_id: str = None, extra_log: str = None,
        fixed_shares: float = None
    ) -> None:
        nonlocal eff_risk_per_trade
        if risk is None:
            risk = eff_risk_per_trade

        kelly_log = ""
        if KELLY_ACTIVE and fixed_invest is None and fixed_shares is None:
            risk, kelly_log = state.kelly.compute_fraction(fallback=risk)

        ask = nom + ASK_SPREAD
        f   = fee_rate(ask)
        eff = ask / (1.0 - f) if f < 1.0 else ask

        if fixed_shares is not None:
            shares   = fixed_shares
            invested = shares * eff
        elif fixed_invest is not None:
            invested = fixed_invest
            shares   = (invested / ask) * (1.0 - f)
        else:
            invested = state.bankroll * risk
            shares   = (invested / ask) * (1.0 - f)

        if trade_type.startswith('CICLO'):
            target = CYCLE_TARGET_C / 100.0 if CYCLE_TARGET_C > 0 else None
        elif trade_type == 'EIGHTY':
            target = EIGHTY_TARGET_C / 100.0 if EIGHTY_TARGET_C > 0 else None
        elif trade_type == 'PEG_ARBIT':
            target = PEG_ARBIT_TARGET_C / 100.0 if PEG_ARBIT_TARGET_C > 0 else None
        else:
            target = min(0.99, eff * TARGET_MULTIPLIER)

        state.bankroll -= invested
        peg_str  = f" | PEG_Eff: {fc(peg_val)} ({peg_val:.3f})" if peg_val is not None else ""
        extra    = f" | {extra_log}" if extra_log else ""
        k_sfx    = f" | {kelly_log}" if kelly_log else ""

        trade = {
            'side': side, 'nom': nom, 'entry': eff, 'shares': shares,
            'target': target, 'type': trade_type, 'invested': invested,
            'wait_close': wait_close, 'token_id': token_id,
            'sl_seen_levels': set(), 'sl_tick_count': 0
        }
        active_trades.append(trade)

        if LIVE_TRADING and token_id:
            await place_live_order(side, ask, shares, token_id)

        log_m(trade_type.replace('_', ' '), 'BUY',
            f"rem={rstr} | {side} @ nom={fc(nom)} ask={fc(ask)} eff={fc(eff)}"
            f"{peg_str} | inv=${invested:.4f} ({pct_banca(invested)}) | "
            f"shares={shares:.4f} | fee={f*100:.3f}%{extra}{k_sfx}"
        )

    # -------------------------------------------------------------------------
    def close_trade(trade: dict, cp: float, reason: str, rstr: str) -> None:
        payout  = sell_payout(trade['shares'], cp)
        pnl     = payout - trade['invested']
        pnl_pct = (pnl / trade['invested'] * 100.0) if trade['invested'] else 0.0
        state.bankroll += payout
        if KELLY_ACTIVE:
            state.kelly.add_result(trade['invested'], payout)
        if AS_VPIN_ACTIVE:
            state.as_model.update_inventory(trade['side'], trade['shares'], is_buy=False)
        icon   = "OK" if pnl >= 0 else "LOSS"
        log_m(trade['type'].replace('_', ' '), 'SELL',
            f"rem={rstr} | {trade['side']} @ {fc(cp)} "
            f"| PnL: ${pnl:+.4f} ({pnl_pct:+.1f}%) "
            f"| Reason: {reason} [{icon}]"
        )

    # -------------------------------------------------------------------------
    def eighty_reset(e_side: str, rstr: str, reason: str) -> None:
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side]   = 0
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side]      = None
        eighty_eff_max[e_side]      = None
        log_m('EIGHTY', 'RESET', f"rem={rstr} | {e_side} — {reason}")

    def eighty_reset_silent(e_side: str) -> None:
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side]   = 0
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side]      = None
        eighty_eff_max[e_side]      = None

    def eighty_activate_vol_cooldown(e_side: str, rstr: str, reason: str) -> None:
        eighty_vol_cooldown_until[e_side] = time.time() + EIGHTY_VOL_COOLDOWN_S
        eighty_reset(e_side, rstr, reason)
        log_m('EIGHTY', 'VOL_COOLDOWN',
            f"rem={rstr} | {e_side} bloqueado {EIGHTY_VOL_COOLDOWN_S:.0f}s")

    # =========================================================================
    # LOOP PRINCIPAL
    # =========================================================================
    while True:
        now = time.time()
        rem = m_end - now

        # --- Fim de mercado --------------------------------------------------
        if rem <= 0:
            # Usa bid para saida; fallback para ask se bid indisponivel
            u_bid_f = state.best_bids.get('up')  or state.best_asks.get('up')  or 0.0
            d_bid_f = state.best_bids.get('down') or state.best_asks.get('down') or 0.0
            log_sep()
            log_info(
                f"FIM DE MERCADO | UP bid={fc(u_bid_f)} | DOWN bid={fc(d_bid_f)}"
            )
            for trade in active_trades[:]:
                cp = u_bid_f if trade['side'] == 'UP' else d_bid_f
                close_trade(trade, cp, "FIM MERCADO", "00:00:000")
                active_trades.remove(trade)   # limpa a lista (bug corrigido)
            break

        rstr = get_remaining_str(rem)

        # Aguarda tick de preco via Queue
        try:
            await asyncio.wait_for(price_q.get(), timeout=LOOP_SLEEP)
        except asyncio.TimeoutError:
            pass

        # Desempacota asks (entradas) e bids (saidas)
        u_p   = state.best_asks.get('up')
        d_p   = state.best_asks.get('down')
        u_bid = state.best_bids.get('up')
        d_bid = state.best_bids.get('down')

        if u_p is None or d_p is None:
            continue
        if u_p == prev_u_p and d_p == prev_d_p:
            continue
        prev_u_p = u_p
        prev_d_p = d_p

        # Pre-calcula eff prices UMA VEZ por tick
        ask_up   = u_p + ASK_SPREAD
        ask_down = d_p + ASK_SPREAD
        eff_up   = effective_entry(ask_up)
        eff_down = effective_entry(ask_down)
        peg_eff  = eff_up + eff_down
        underpeg_eff_c = (1.0 - peg_eff) * 100.0

        peg_disp = (
            f" | PEG_Eff={peg_eff:.3f} underpeg={underpeg_eff_c:.2f}c"
            if peg_eff < 1.0 and underpeg_eff_c >= PEG_ARBIT_UNDERPEG_C else ""
        )
        log_raw(
            f"rem={rstr} | UP={fc(u_p)} Eff={fc(eff_up)} | "
            f"DOWN={fc(d_p)} Eff={fc(eff_down)}{peg_disp}"
        )

        # --- AS+VPIN gate global ---------------------------------------------
        as_blocked = False
        min_edge   = AS_MIN_EDGE_C
        if AS_VPIN_ACTIVE:
            mid_p    = (u_p + d_p) * 0.5
            prev_mid = ((prev_u_p or u_p) + (prev_d_p or d_p)) * 0.5
            state.as_model.add_tick(mid_p, prev_mid)
            q_total   = state.as_model.inventory_up - state.as_model.inventory_down
            min_edge, as_log = state.as_model.get_min_edge_c(
                mid_c=(u_p + d_p) * 50.0, q=q_total, t_remaining=rem
            )
            if min_edge is None:
                as_blocked = True
                log_m('AS VPIN', 'WITHDRAW', f"rem={rstr} | {as_log}")

        # =====================================================================
        # 1. PEG ARBITRAGE
        # =====================================================================
        if (PEG_ARBIT_ACTIVE
                and not as_blocked
                and peg_eff < 1.0
                and underpeg_eff_c >= PEG_ARBIT_UNDERPEG_C
                and rem > PEG_ARBIT_MIN_REM
                and peg_arbit_count < MAX_PEG_ENTRIES
                and now - last_peg_time >= PEG_ARBIT_COOLDOWN):

            eff_up_c   = eff_up   * 100.0
            eff_down_c = eff_down * 100.0
            up_in_range   = PEG_ARBIT_RANGE[0] <= eff_up_c   <= PEG_ARBIT_RANGE[1]
            down_in_range = PEG_ARBIT_RANGE[0] <= eff_down_c <= PEG_ARBIT_RANGE[1]

            if up_in_range and down_in_range:
                budget        = state.bankroll * eff_peg_risk
                ref_eff       = max(eff_up, eff_down)
                shares_to_buy = budget / ref_eff
                total_invest  = shares_to_buy * (eff_up + eff_down)
                arb_profit    = (underpeg_eff_c / 100.0) * budget
                arb_return    = (underpeg_eff_c / (peg_eff * 100.0)) * 100.0

                log_sep()
                log_m('PEG ARBIT', 'ENTRADA',
                    f"rem={rstr} | PEG_Eff={peg_eff:.4f} (-{underpeg_eff_c:.2f}c) | "
                    f"Shares={shares_to_buy:.4f} | Total=${total_invest:.4f} | "
                    f"Lucro est.=${arb_profit:.4f} ({arb_return:.2f}%) | "
                    f"arb #{peg_arbit_count + 1}"
                )
                await open_trade('UP',   u_p, 'PEG_ARBIT', rstr,
                                 fixed_shares=shares_to_buy, wait_close=True,
                                 peg_val=peg_eff, token_id=meta['up'])
                await open_trade('DOWN', d_p, 'PEG_ARBIT', rstr,
                                 fixed_shares=shares_to_buy, wait_close=True,
                                 peg_val=peg_eff, token_id=meta['down'])
                log_sep()
                peg_arbit_count += 1
                last_peg_time    = now
            elif peg_arbit_count == 0:
                reasons = []
                if not up_in_range:
                    reasons.append(
                        f"UP_Eff {eff_up_c:.1f}c fora "
                        f"[{PEG_ARBIT_RANGE[0]:.0f}-{PEG_ARBIT_RANGE[1]:.0f}]"
                    )
                if not down_in_range:
                    reasons.append(
                        f"DOWN_Eff {eff_down_c:.1f}c fora "
                        f"[{PEG_ARBIT_RANGE[0]:.0f}-{PEG_ARBIT_RANGE[1]:.0f}]"
                    )
                log_m('PEG ARBIT', 'SKIP',
                    f"rem={rstr} | PEG_Eff OK ({peg_eff:.4f}) mas {' | '.join(reasons)}")

        # =====================================================================
        # 2. TARGET CHECK + STOP-LOSS INTRA-TRADE
        # Avaliado contra o BID real — preco real de saida como taker.
        # cp_eff = bid * (1 - fee(bid)) = payout efectivo liquido de fee ao vender.
        # =====================================================================
        for trade in active_trades[:]:
            raw_bid = u_bid if trade['side'] == 'UP' else d_bid
            cp      = raw_bid if raw_bid is not None else (
                u_p if trade['side'] == 'UP' else d_p
            )
            cp_eff  = (cp * (1.0 - fee_rate(cp)) * 100.0) if cp else 0.0

            # Target check
            if trade.get('target') is not None and cp and cp >= trade['target']:
                close_trade(trade, cp, "TARGET", rstr)
                active_trades.remove(trade)
                continue

            # Stop-Loss — PEG_ARBIT e sempre excluido
            if trade['type'] == 'PEG_ARBIT':
                continue

            if cp_eff > 0 and cp_eff < STOPLOSS_PRICE_C:
                # Nível unico via PRICE_STEP_C (mesmo mecanismo do EIGHTY, invertido)
                level_key = round(cp_eff / PRICE_STEP_C) * PRICE_STEP_C
                if level_key not in trade['sl_seen_levels']:
                    trade['sl_seen_levels'].add(level_key)
                    trade['sl_tick_count'] += 1
                    log_m(trade['type'].replace('_', ' '), 'SL_TICK',
                        f"rem={rstr} | {trade['side']} bid_eff={cp_eff:.1f}c "
                        f"< {STOPLOSS_PRICE_C:.0f}c | nivel {level_key:.1f}c | "
                        f"tick {trade['sl_tick_count']}/{STOPLOSS_TICKS}"
                    )
                if trade['sl_tick_count'] >= STOPLOSS_TICKS:
                    log_m(trade['type'].replace('_', ' '), 'STOP-LOSS',
                        f"rem={rstr} | {trade['side']} — {STOPLOSS_TICKS} niveis unicos "
                        f"abaixo de {STOPLOSS_PRICE_C:.0f}c | "
                        f"niveis: {sorted(trade['sl_seen_levels'])} | FECHAR POSICAO"
                    )
                    close_trade(trade, cp, "STOP-LOSS", rstr)
                    active_trades.remove(trade)
            else:
                # Bid subiu acima do threshold — flash crash descartado, reset
                if trade['sl_tick_count'] > 0:
                    log_m(trade['type'].replace('_', ' '), 'SL_RESET',
                        f"rem={rstr} | {trade['side']} bid_eff={cp_eff:.1f}c "
                        f">= {STOPLOSS_PRICE_C:.0f}c | set resetado "
                        f"({trade['sl_tick_count']} nivel(eis) descartados)"
                    )
                    trade['sl_seen_levels'].clear()
                    trade['sl_tick_count'] = 0

        # =====================================================================
        # 3. EIGHTY
        # =====================================================================
        if EIGHTY_ACTIVE:
            if rem > EIGHTY_START_REM_S:
                pass  # fora da janela — silencioso

            elif rem <= EIGHTY_CUTOFF_S:
                if not eighty_cutoff_logged:
                    eighty_cutoff_logged = True
                    log_m('EIGHTY', 'CUTOFF',
                        f"rem={rstr} | EIGHTY parado — rem <= {EIGHTY_CUTOFF_S}s")

            else:
                if not eighty_started_logged:
                    eighty_started_logged = True
                    log_m('EIGHTY', 'START',
                        f"rem={rstr} | EIGHTY activo "
                        f"[{EIGHTY_START_REM_S}s->{EIGHTY_CUTOFF_S}s] "
                        f"| risco={eff_eighty_risk:.1%}"
                    )

                for e_side, nom, eff_price in (
                    ('UP',   u_p, eff_up),
                    ('DOWN', d_p, eff_down)
                ):
                    token_id = meta['up'] if e_side == 'UP' else meta['down']

                    # Ignora verificacoes de volatilidade nos ultimos N segundos
                    # apenas quando cutoff=0 e activa a janela final
                    skip_vol = (
                        EIGHTY_CUTOFF_S == 0
                        and EIGHTY_WHEN_CUTOFF_0_VOLT > 0
                        and rem <= EIGHTY_WHEN_CUTOFF_0_VOLT
                    )

                    if not skip_vol and now < eighty_vol_cooldown_until[e_side]:
                        continue
                    if not skip_vol and now - eighty_last_buy[e_side] < EIGHTY_BUY_COOLDOWN:
                        continue

                    eff_c = eff_price * 100.0

                    # Adiciona ao buffer de precos para calculos de delta
                    eighty_price_buffer[e_side].add(eff_c, now)

                    if as_blocked:
                        continue

                    # Range check contra preco EFECTIVO
                    if not (EIGHTY_MIN_EFF_C <= eff_c <= EIGHTY_MAX_EFF_C):
                        if eighty_tick_count[e_side] > 0:
                            eighty_reset(e_side, rstr,
                                f"eff_c {eff_c:.1f}c fora "
                                f"[{EIGHTY_MIN_EFF_C:.0f}-{EIGHTY_MAX_EFF_C:.0f}]"
                            )
                        continue

                    # PEG check: so bloqueia se abaixo do minimo configurado
                    peg_c = peg_eff * 100.0
                    if peg_c < EIGHTY_PEG_MIN_C:
                        if eighty_tick_count[e_side] > 0:
                            eighty_reset(e_side, rstr,
                                f"PEG_Eff {peg_c:.1f}c < min {EIGHTY_PEG_MIN_C:.1f}c"
                            )
                        continue

                    # Bucketing de nivel por PRICE_STEP_C
                    level_key = round(eff_c / PRICE_STEP_C) * PRICE_STEP_C
                    if level_key not in eighty_seen_levels[e_side]:
                        eighty_seen_levels[e_side].add(level_key)
                        eighty_tick_count[e_side] += 1

                    # Rastreia min/max para calculo de volatilidade interna
                    if eighty_first_tick_t[e_side] is None:
                        eighty_first_tick_t[e_side] = now
                        eighty_eff_min[e_side]      = eff_c
                        eighty_eff_max[e_side]      = eff_c
                    else:
                        if eff_c < eighty_eff_min[e_side]: eighty_eff_min[e_side] = eff_c
                        if eff_c > eighty_eff_max[e_side]: eighty_eff_max[e_side] = eff_c

                    elapsed = now - eighty_first_tick_t[e_side]
                    var_c   = eighty_eff_max[e_side] - eighty_eff_min[e_side]
                    vol_nok = (elapsed <= EIGHTY_VOL_WINDOW_S
                               and var_c >= EIGHTY_VOL_MAX_C)

                    # Deltas de preco em multiplos intervalos
                    epb = eighty_price_buffer[e_side]
                    delta_05, valid_05 = epb.get_delta(0.5)
                    delta_10, valid_10 = epb.get_delta(1.0)
                    delta_20, valid_20 = epb.get_delta(2.0)

                    delta_parts = []
                    if valid_05: delta_parts.append(f"D0.5s:{delta_05:+.1f}c")
                    if valid_10: delta_parts.append(f"D1s:{delta_10:+.1f}c")
                    if valid_20: delta_parts.append(f"D2s:{delta_20:+.1f}c")
                    delta_str = " | ".join(delta_parts) if delta_parts else \
                                f"D aguarda ({epb.get_age():.1f}s)"

                    # Deteccao de subida rapida (pump falso)
                    delta_vt, valid_vt = epb.get_delta(EIGHTY_DELTA_VOL_TIME_S)
                    rapid_rise = (
                        valid_vt
                        and delta_vt is not None
                        and delta_vt >= EIGHTY_DELTA_VOL_RISE_C
                    )

                    # Avalia direcao dos deltas
                    delta_ok     = True
                    delta_reason = ""
                    has_delta    = valid_05 or valid_10 or valid_20

                    if valid_05 and delta_05 is not None and delta_05 < 0:
                        delta_ok, delta_reason = False, f"D0.5s={delta_05:+.1f}c (a cair)"
                    elif valid_10 and delta_10 is not None and delta_10 < 0:
                        delta_ok, delta_reason = False, f"D1s={delta_10:+.1f}c (a cair)"
                    elif valid_20 and delta_20 is not None and delta_20 < 0:
                        delta_ok, delta_reason = False, f"D2s={delta_20:+.1f}c (a cair)"
                    elif rapid_rise:
                        delta_ok, delta_reason = (
                            False,
                            f"D{EIGHTY_DELTA_VOL_TIME_S}s={delta_vt:+.1f}c (pump rapido)"
                        )

                    vol_str    = "VOL SKIP" if skip_vol else \
                                 f"VOL {'NOK' if vol_nok else 'OK'} ({var_c:.1f}c/{elapsed:.1f}s)"
                    delta_icon = "UP" if (delta_ok and has_delta) else \
                                 ("DOWN" if has_delta else "-")

                    log_m('EIGHTY', 'WATCH',
                        f"rem={rstr} | {e_side} Eff={fc(eff_price)} | {vol_str} | "
                        f"{delta_str} [{delta_icon}] | "
                        f"ticks={eighty_tick_count[e_side]}/{EIGHTY_MIN_TICKS}"
                    )

                    if not skip_vol:
                        if vol_nok:
                            eighty_activate_vol_cooldown(e_side, rstr,
                                f"VOL {var_c:.1f}c em {elapsed:.1f}s "
                                f"(max {EIGHTY_VOL_MAX_C:.1f}c/{EIGHTY_VOL_WINDOW_S:.1f}s)"
                            )
                            continue
                        if rapid_rise:
                            eighty_activate_vol_cooldown(e_side, rstr,
                                f"PUMP RAPIDO D{EIGHTY_DELTA_VOL_TIME_S}s={delta_vt:+.1f}c"
                            )
                            continue

                    if eighty_tick_count[e_side] >= EIGHTY_MIN_TICKS:
                        if has_delta and not delta_ok:
                            eighty_reset(e_side, rstr,
                                f"DELTA NOK — {delta_reason}")
                            continue

                        # AS edge check (quando AS+VPIN activo)
                        if AS_VPIN_ACTIVE and min_edge is not None:
                            edge_c = 99.0 - eff_c
                            if edge_c < min_edge:
                                eighty_reset(e_side, rstr,
                                    f"AS EDGE NOK — edge {edge_c:.1f}c < min {min_edge:.2f}c"
                                )
                                continue

                        if state.bankroll > 0:
                            if AS_VPIN_ACTIVE:
                                shares_est = buy_shares_net(
                                    state.bankroll * eff_eighty_risk,
                                    nom + ASK_SPREAD
                                )
                                state.as_model.update_inventory(e_side, shares_est, is_buy=True)

                            await open_trade(
                                e_side, nom, 'EIGHTY', rstr,
                                risk=eff_eighty_risk, wait_close=True,
                                peg_val=peg_eff, token_id=token_id,
                                extra_log=(
                                    f"ticks={eighty_tick_count[e_side]} | {delta_str}"
                                )
                            )
                            eighty_last_buy[e_side] = now
                            eighty_reset_silent(e_side)
                            log_m('EIGHTY', 'COOLDOWN',
                                f"rem={rstr} | {e_side} cooldown {EIGHTY_BUY_COOLDOWN:.1f}s")

        # =====================================================================
        # 4. CICLO 30s
        # =====================================================================
        if CICLO_30S_ACTIVE:
            if not flags['s35'] and rem <= CYCLE_30S_SNAPSHOT_REM:
                c_state['c1']['snap_eff_u'] = eff_up  * 100.0
                c_state['c1']['snap_eff_d'] = eff_down * 100.0
                flags['s35'] = True
                log_m('CICLO 30s', 'SNAP',
                    f"rem={rstr} | UP_Eff={fc(eff_up)} DOWN_Eff={fc(eff_down)}")

            if flags['s35'] and not flags['v30'] and rem <= CYCLE_30S_VOL_CHECK_REM:
                vol_c = abs(eff_up * 100.0 - c_state['c1']['snap_eff_u'])
                flags['v30']              = True
                c_state['c1']['vol_ok'] = vol_c <= CYCLE_VOL_MAX_C
                log_m('CICLO 30s', 'VOLT',
                    f"rem={rstr} | vol_eff={vol_c:.1f}c (max {CYCLE_VOL_MAX_C:.1f}c) "
                    f"| {'OK' if c_state['c1']['vol_ok'] else 'NOK'}"
                )

            if (flags['v30'] and c_state['c1'].get('vol_ok')
                    and not flags['d29'] and rem <= CYCLE_30S_BUY_REM):
                flags['d29'] = True
                for e_side, nom, eff_p, tid in (
                    ('UP',   u_p, eff_up,   meta['up']),
                    ('DOWN', d_p, eff_down, meta['down'])
                ):
                    eff_c_cycle = eff_p * 100.0
                    peg_c_cycle = peg_eff * 100.0
                    if (CYCLE_PRICE_MIN_C <= eff_c_cycle <= CYCLE_PRICE_MAX_C
                            and peg_c_cycle >= CYCLE_PEG_MIN_C):
                        await open_trade(e_side, nom, 'CICLO_30s', rstr,
                                         risk=eff_risk_per_trade, wait_close=True,
                                         peg_val=peg_eff, token_id=tid)
                    else:
                        reasons = []
                        if eff_c_cycle < CYCLE_PRICE_MIN_C:
                            reasons.append(
                                f"Eff {eff_c_cycle:.1f}c < min {CYCLE_PRICE_MIN_C:.1f}c"
                            )
                        elif eff_c_cycle > CYCLE_PRICE_MAX_C:
                            reasons.append(
                                f"Eff {eff_c_cycle:.1f}c > max {CYCLE_PRICE_MAX_C:.1f}c"
                            )
                        if peg_c_cycle < CYCLE_PEG_MIN_C:
                            reasons.append(
                                f"PEG_Eff {peg_c_cycle:.1f}c < {CYCLE_PEG_MIN_C:.1f}c"
                            )
                        log_m('CICLO 30s', 'SKIP',
                            f"rem={rstr} | {e_side} — {' | '.join(reasons)}")

        # =====================================================================
        # 5. CICLO 20s
        # =====================================================================
        if CICLO_20S_ACTIVE:
            if not flags['s25'] and rem <= CYCLE_20S_SNAPSHOT_REM:
                c_state['c2']['snap_eff_u'] = eff_up  * 100.0
                c_state['c2']['snap_eff_d'] = eff_down * 100.0
                flags['s25'] = True
                log_m('CICLO 20s', 'SNAP',
                    f"rem={rstr} | UP_Eff={fc(eff_up)} DOWN_Eff={fc(eff_down)}")

            if flags['s25'] and not flags['v20'] and rem <= CYCLE_20S_VOL_CHECK_REM:
                vol_c = abs(eff_up * 100.0 - c_state['c2']['snap_eff_u'])
                flags['v20']              = True
                c_state['c2']['vol_ok'] = vol_c <= CYCLE_VOL_MAX_C
                log_m('CICLO 20s', 'VOLT',
                    f"rem={rstr} | vol_eff={vol_c:.1f}c (max {CYCLE_VOL_MAX_C:.1f}c) "
                    f"| {'OK' if c_state['c2']['vol_ok'] else 'NOK'}"
                )

            if (flags['v20'] and c_state['c2'].get('vol_ok')
                    and not flags['d19'] and rem <= CYCLE_20S_BUY_REM):
                flags['d19'] = True
                for e_side, nom, eff_p, tid in (
                    ('UP',   u_p, eff_up,   meta['up']),
                    ('DOWN', d_p, eff_down, meta['down'])
                ):
                    eff_c_cycle = eff_p * 100.0
                    peg_c_cycle = peg_eff * 100.0
                    if (CYCLE_PRICE_MIN_C <= eff_c_cycle <= CYCLE_PRICE_MAX_C
                            and peg_c_cycle >= CYCLE_PEG_MIN_C):
                        await open_trade(e_side, nom, 'CICLO_20s', rstr,
                                         risk=eff_risk_per_trade, wait_close=True,
                                         peg_val=peg_eff, token_id=tid)
                    else:
                        reasons = []
                        if eff_c_cycle < CYCLE_PRICE_MIN_C:
                            reasons.append(
                                f"Eff {eff_c_cycle:.1f}c < min {CYCLE_PRICE_MIN_C:.1f}c"
                            )
                        elif eff_c_cycle > CYCLE_PRICE_MAX_C:
                            reasons.append(
                                f"Eff {eff_c_cycle:.1f}c > max {CYCLE_PRICE_MAX_C:.1f}c"
                            )
                        if peg_c_cycle < CYCLE_PEG_MIN_C:
                            reasons.append(
                                f"PEG_Eff {peg_c_cycle:.1f}c < {CYCLE_PEG_MIN_C:.1f}c"
                            )
                        log_m('CICLO 20s', 'SKIP',
                            f"rem={rstr} | {e_side} — {' | '.join(reasons)}")

# =============================================================================
# MAIN
# =============================================================================

async def main() -> None:
    state.kelly    = EmpiricalKelly()
    state.as_model = AvellanedaStoikov()

    log_sep2()
    log_info("BOT XRP POLYMARKET v0.38.0 INICIADO")
    log_sep()
    log_info(f"   LIVE_TRADING         : {LIVE_TRADING}")
    log_info(f"   BANKROLL_INIT        : ${BANKROLL_INIT:.2f}")
    log_info(f"   Banca Demo persiste  : {'Nao' if LIVE_TRADING else 'Sim (nunca reseta entre dias)'}")
    log_sep()
    log_info("   RISCO BASE:")
    log_info(f"   RISK_PER_TRADE       : {RISK_PER_TRADE:.0%}")
    log_info(f"   EIGHTY_RISK          : {EIGHTY_RISK:.0%}")
    log_info(f"   PEG_ARBIT_RISK       : {PEG_ARBIT_RISK:.0%}")
    log_sep()
    log_info("   MARTINGALE + RECOVERY:")
    log_info(f"   MAX_RISK_PERCENT     : {MAX_RISK_PERCENT:.0%}  (CAP ABSOLUTO INVIOLAVEL)")
    log_info(f"   MAX_MULTIPLIER       : x{MAX_RISK_MULTIPLIER}")
    log_info(f"   RECOVERY_ROUNDS/LOSS : +{RECOVERY_ROUNDS_PER_LOSS} rondas por perda")
    log_info(
        f"   Formula              : "
        f"min(base x mult + accum/rounds/bank, {MAX_RISK_PERCENT:.0%})"
    )
    log_sep()
    log_info("   STOP-LOSS INTRA-TRADE:")
    log_info(f"   Preco BID eff min    : {STOPLOSS_PRICE_C:.0f}c")
    log_info(f"   Niveis unicos        : {STOPLOSS_TICKS} (step={PRICE_STEP_C}c, anti-flash-crash)")
    log_sep()
    log_info("   MODULOS:")
    log_info(f"   EIGHTY               : {'ON' if EIGHTY_ACTIVE    else 'OFF'}")
    log_info(f"   PEG_ARBIT            : {'ON' if PEG_ARBIT_ACTIVE else 'OFF'}")
    log_info(f"   CICLO_30S            : {'ON' if CICLO_30S_ACTIVE else 'OFF'}")
    log_info(f"   CICLO_20S            : {'ON' if CICLO_20S_ACTIVE else 'OFF'}")
    log_info(f"   KELLY                : {'ON' if KELLY_ACTIVE     else 'OFF'}")
    log_info(f"   AS+VPIN              : {'ON' if AS_VPIN_ACTIVE   else 'OFF'}")
    log_sep2()

    while True:
        slug, start_ts = get_current_slug()
        meta = fetch_metadata(slug)
        if not meta:
            log_warn(f"Metadata nao encontrada para {slug} — retentando em 1s")
            await asyncio.sleep(1)
            continue

        # Novo dia: reset daily_profit e modelos; banca persiste em Demo
        market_day = datetime.fromtimestamp(start_ts).date()
        if state.last_day is None or market_day != state.last_day:
            state.daily_profit = 0.0
            state.last_day     = market_day
            state.kelly        = EmpiricalKelly()
            state.as_model     = AvellanedaStoikov()
            if LIVE_TRADING:
                live_bal = fetch_wallet_balance()
                if live_bal is not None and live_bal > 0:
                    state.bankroll = live_bal
                    log_sep2()
                    log_info(
                        f"NOVO DIA {market_day} | "
                        f"Saldo Live lido: ${state.bankroll:.4f}"
                    )
                else:
                    log_warn(
                        f"NOVO DIA {market_day} | "
                        f"Saldo Live nao disponivel — mantendo ${state.bankroll:.4f}"
                    )
            else:
                log_sep2()
                log_info(
                    f"NOVO DIA {market_day} | "
                    f"Banca Demo persistente: ${state.bankroll:.4f}"
                )
            log_info(
                f"   Martingale: x{state.risk_multiplier:.0f} | "
                f"accum=${state.accumulated_loss:.4f} | "
                f"rec_rounds={state.recovery_rounds}"
            )
            log_sep2()

        # Reset do order book para este ciclo
        state.best_asks['up'] = state.best_asks['down'] = None
        state.best_bids['up'] = state.best_bids['down'] = None

        price_queue = asyncio.Queue(maxsize=500)
        ws_task     = asyncio.create_task(
            ws_handler(meta['up'], meta['down'], price_queue)
        )
        await asyncio.sleep(0.8)

        if state.best_asks['up'] is not None:
            pre_bank = state.bankroll

            await logic_loop(start_ts, start_ts + 300, meta, price_queue)

            profit_this   = state.bankroll - pre_bank
            state.daily_profit += profit_this
            pnl_pct  = (profit_this / pre_bank * 100.0) if pre_bank > 0 else 0.0
            daily_pct = (
                (state.daily_profit / BANKROLL_INIT * 100.0) if BANKROLL_INIT > 0 else 0.0
            )
            pnl_str = (
                "PnL: $0.0000 (0.00%)" if profit_this == 0.0
                else f"PnL: ${profit_this:+.4f} ({pnl_pct:+.2f}%)"
            )

            log_sep2()

            # ------------------------------------------------------------------
            # Actualiza Martingale + Recovery
            # ------------------------------------------------------------------
            if profit_this < 0:
                loss              = math.fabs(profit_this)
                state.accumulated_loss += loss * 0.5   # 50% da perda entra no acumulado
                state.recovery_rounds  += RECOVERY_ROUNDS_PER_LOSS
                state.risk_multiplier   = min(
                    state.risk_multiplier * 2.0, float(MAX_RISK_MULTIPLIER)
                )
                # Preview do proximo risco na mesma linha do ROUND
                next_eighty = calc_risk(
                    EIGHTY_RISK,    state.risk_multiplier,
                    state.accumulated_loss, state.recovery_rounds, state.bankroll
                )
                next_peg    = calc_risk(
                    PEG_ARBIT_RISK, state.risk_multiplier,
                    state.accumulated_loss, state.recovery_rounds, state.bankroll
                )
                cap_e = " [CAP]" if next_eighty >= MAX_RISK_PERCENT else ""
                cap_p = " [CAP]" if next_peg    >= MAX_RISK_PERCENT else ""
                log_info(
                    f"ROUND | {pnl_str} | "
                    f"PROX. RONDA: MARTINGALE x{state.risk_multiplier:.0f} | "
                    f"EIGHTY={next_eighty:.1%}{cap_e} PEG={next_peg:.1%}{cap_p} | "
                    f"accum=${state.accumulated_loss:.4f} "
                    f"rec_rounds={state.recovery_rounds}"
                )

            elif profit_this == 0.0:
                # Sem trades: mantem todo o estado inalterado
                if state.risk_multiplier > 1.0:
                    log_info(
                        f"ROUND | {pnl_str} | "
                        f"MARTINGALE x{state.risk_multiplier:.0f} MANTIDO (sem trades)"
                    )
                else:
                    log_info(f"ROUND | {pnl_str}")

            else:
                # Lucro: reset multiplier, abate no acumulado, reduz 1 ronda
                prev_accum         = state.accumulated_loss
                state.accumulated_loss  = max(0.0, state.accumulated_loss - profit_this)
                state.recovery_rounds   = max(0, state.recovery_rounds - 1)
                recovered          = prev_accum - state.accumulated_loss
                state.risk_multiplier   = 1.0

                recovery_note = ""
                if recovered > 0 and state.accumulated_loss > 0:
                    recovery_note = (
                        f" | RECOVERY parcial: -${recovered:.4f} | "
                        f"restam ${state.accumulated_loss:.4f} "
                        f"em {state.recovery_rounds} rondas"
                    )
                elif recovered > 0 and state.accumulated_loss == 0.0:
                    recovery_note = (
                        f" | RECOVERY COMPLETO (${prev_accum:.4f} recuperados)"
                    )
                log_info(f"ROUND | {pnl_str}{recovery_note}")

            # Totais do dia
            total_str = (
                "$0.0000 (0.00%)" if state.daily_profit == 0.0
                else f"${state.daily_profit:+.4f} ({daily_pct:+.2f}%)"
            )
            log_info(
                f"TOTAL | PnL: {total_str} | "
                f"Banca: ${state.bankroll:.4f} | "
                f"accum_loss: ${state.accumulated_loss:.4f} | "
                f"Uptime: {get_uptime_str()}"
            )
            log_sep2()

        else:
            log_warn("Sem precos recebidos neste ciclo — a saltar")

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