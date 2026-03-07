# =============================================================================
# BOT XRP POLYMARKET v0.37.0 (Optimized)
# =============================================================================
# CHANGELOG v0.37.0:
# [refactor] Migracao de variaveis globais para dataclass BotState (thread-safe)
# [feat] Implementacao de leitura de best_bids para saidas (taker real spread)
# [feat] PRICE_STEP_C isolado como parametro configuravel (bucketing de niveis)
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
from datetime import datetime, date
from dataclasses import dataclass, field
from typing import Optional, Dict, Any

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
# O PEG e lucro quase garantido, mas o cap de 15% continua a ser inviolavel.
# Exemplo: 0.10 = 10% de $10.00 = $1.00 por leg.
# Range: [0.05 | 0.12]

# --- SECCAO 3 — MARTINGALE E RECOVERY SUAVE ----------------------------------

MAX_RISK_MULTIPLIER = 8
# Limite maximo do multiplicador Martingale.
# x8 = apos 3 perdas consecutivas (x1 -> x2 -> x4 -> x8).
# Acima disto o cap de 15% ja e atingido de qualquer forma.
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
# Exemplo: 3 perdas = 30 rondas de recovery; cada lucro reduz 1 ronda.
# Range: [5 | 20]

# --- SECCAO 4 — STOP-LOSS INTRA-TRADE DINAMICO ------------------------------

STOPLOSS_PRICE_C = 25.0
# Preco EFECTIVO (cents) abaixo do qual o contador de ticks de stop-loss
# começa a incrementar para uma posicao aberta.
# Exemplo: 40.0 = se o preco efectivo cair abaixo de 40c, começa a contar.
# Range: [20.0 | 60.0]

STOPLOSS_TICKS = 5
# Numero de NIVEIS DE PRECO EFECTIVO UNICOS distintos
# abaixo de STOPLOSS_PRICE_C necessarios para fechar a posicao.
# Range: [3 | 10]

PRICE_STEP_C = 1
# Nivel de discretizacao/arredondamento do preco para contar ticks unicos.
# Usado no EIGHTY (subida) e no STOP-LOSS (descida) para ignorar micro-ruido.
# Exemplo: 0.5 = agrupa precos em degraus de meio centimo (39.5c, 39.0c...).
# Range: [0.1 | 2.0]

# --- SECCAO 5 — TOGGLES DE MODULOS ------------------------------------------

CICLO_30S_ACTIVE = False
# Estrategia de ciclo de 30 segundos (snapshot + verificacao de volatilidade + compra).

CICLO_20S_ACTIVE = False
# Estrategia de ciclo de 20 segundos. Identica ao 30s mas em janela mais curta.

EIGHTY_ACTIVE = True
# Estrategia EIGHTY: compra quando o preco efectivo esta no range definido e ha
# pelo menos EIGHTY_MIN_TICKS niveis distintos de consolidacao.

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
# Abaixo disto o lado esta demasiado barato = muito incerto.
# Range: [50.0 | 85.0]

CYCLE_PRICE_MAX_C = 85.0
# Preco EFECTIVO maximo em cents para entrar num ciclo.
# Acima disto o retorno potencial e demasiado baixo para o risco.
# Range: [75.0 | 92.0]

CYCLE_PEG_MIN_C = 96.5
# PEG_Eff minimo (soma Eff_UP + Eff_DOWN em cents) para aceitar o ciclo.
# Garante equilibrio razoavel do mercado. Deve ser sempre < 100.0.
# Range: [94.0 | 99.5]

CYCLE_VOL_MAX_C = 5.2
# Variacao maxima do preco efectivo entre snapshot e verificacao (em cents).
# Se o preco efectivo oscilou mais que isto, a entrada e cancelada.
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
# Retorno a 82c: (100-82)/82 ~= +22%. Minimo aceitavel.
# Range: [70.0 | 90.0]

EIGHTY_MAX_EFF_C = 98.5
# Preco EFECTIVO maximo para o EIGHTY comprar.
# Acima de 99c o retorno e inferior a 1% — nao compensa a fee.
# Range: [95.0 | 99.5]

EIGHTY_MIN_TICKS = 5
# Numero minimo de niveis de preco distintos para confirmar consolidacao.
# Usa o PRICE_STEP_C para os agrupamentos.
# Range: [3 | 12]

EIGHTY_CUTOFF_S = 5
# Para o EIGHTY quando faltam X segundos para o fim do mercado.
# Evita entrar perto do limite onde nao ha tempo de reaccao.
# Range: [0 | 30]

EIGHTY_WHEN_CUTOFF_0_VOLT = 35.0
# Se EIGHTY_CUTOFF_S=0: ignora verificacoes de volatilidade nos ultimos X segundos.
# Permite capturar movimentos rapidos no final sem ser bloqueado pela volatilidade.
# Range: [0.0 | 60.0]

EIGHTY_PEG_MIN_C = 97.0
# PEG_Eff minimo em cents para aceitar entrada EIGHTY.
# PEG baixo = mercado desequilibrado = maior risco de inversao.
# Nota: o EIGHTY tambem bloqueia automaticamente quando PEG_Eff >= 100c.
# Range: [94.0 | 99.5]

EIGHTY_BUY_COOLDOWN = 4.0
# Segundos minimos entre compras consecutivas do mesmo lado.
# Evita acumular posicoes no mesmo nível de preco.
# Range: [1.0 | 15.0]

EIGHTY_VOL_WINDOW_S = 5
# Janela temporal para calcular volatilidade interna (segundos).
# Range: [2.0 | 15.0]

EIGHTY_VOL_MAX_C = 4.5
# Variacao maxima do preco efectivo dentro de EIGHTY_VOL_WINDOW_S.
# Se o preco variou mais de 4.5c nos ultimos 5s -> volatil -> nao entrar.
# Range: [1.0 | 8.0]

EIGHTY_VOL_COOLDOWN_S = 5.0
# Apos detectar volatilidade excessiva, bloqueia o lado por X segundos.
# Range: [2.0 | 15.0]

EIGHTY_DELTA_INTERVALS  = [0.5, 1.0, 2.0]  # Intervalos para calcular delta de preco
EIGHTY_DELTA_LOOKBACK_S = 2.0               # Lookback maximo do buffer de precos    [1.0 | 10.0]
EIGHTY_DELTA_MAX_RISE_C = 3.5               # Delta maximo permitido em subida        [1.0 | 6.0]
EIGHTY_DELTA_VOL_RISE_C = 3.5               # Delta de volatilidade em subida rapida  [1.0 | 6.0]
EIGHTY_DELTA_VOL_TIME_S = 1.5               # Janela temporal para subida rapida      [0.5 | 3.0]

EIGHTY_TARGET_C = 0.0
# Target de venda antecipada (0.0 = hold ate ao fim — recomendado).

# --- SECCAO 8 — PEG ARBITRAGE ------------------------------------------------

PEG_ARBIT_RANGE = (50.0, 50.0)
# Range de preco EFECTIVO (cents) em que ambos os lados devem estar para activar o arb.
# Fora disto o mercado ja decidiu e o arb torna-se arriscado.
# Range: [(20.0, 80.0) ... maximo (30.0, 70.0) recomendado]

PEG_ARBIT_UNDERPEG_C = 1.5
# Desvio minimo do PEG efectivo para activar o arb (em cents).
# PEG_Eff = 99.0c -> underpeg = 1.0c >= 0.8c -> activa.
# PEG_Eff = 99.3c -> underpeg = 0.7c < 0.8c  -> nao activa.
# Range: [0.3 | 5.0]

PEG_ARBIT_COOLDOWN = 0.05
# Intervalo minimo entre duas entradas PEG consecutivas (segundos).
# Evita comprar o mesmo tick duas vezes por latencia do WS.
# Range: [0.01 | 1.0]

PEG_ARBIT_MIN_REM = 5.0
# Remaining minimo para entrar num PEG. Abaixo disto nao ha tempo para settlement.
# Range: [2.0 | 30.0]

MAX_PEG_ENTRIES = 10_000_000
# Maximo de entradas PEG por ciclo de 5 minutos (praticamente ilimitado).
# Reduzir para limitar o numero de arbs por ronda. Range: [1 | ilimitado]

PEG_ARBIT_TARGET_C = 0.0
# Target de venda do PEG (0.0 = hold ate ao fim — SEMPRE recomendado).
# O PEG e lucro garantido apenas se aguardar o settlement final.

TARGET_MULTIPLIER = 1.25
# Multiplicador do preco efectivo para trades sem target fixo.
# Nao afecta PEG nem EIGHTY (ambos com target=0.0 por defeito).
# Range: [1.05 | 2.0]

# --- SECCAO 9 — EMPIRICAL KELLY COM MONTE CARLO ------------------------------

KELLY_MC_SIMULATIONS = 5000   # Simulacoes Monte Carlo  [1000 | 20000]
KELLY_CONFIDENCE     = 0.90   # Percentil de sobrevivencia exigido  [0.70 | 0.99]
KELLY_MIN_HISTORY    = 10     # Minimo de trades para usar Kelly  [5 | 50]
KELLY_MAX_FRACTION   = 0.12   # Cap maximo do Kelly  [0.05 | 0.15]
KELLY_MIN_FRACTION   = 0.02   # Floor minimo do Kelly  [0.01 | 0.05]
KELLY_RUIN_THRESHOLD = 0.50   # Se MC preve perder >50%, corta a fraccao a metade  [0.20 | 0.80]

# --- SECCAO 10 — AVELLANEDA-STOIKOV + VPIN ----------------------------------

AS_GAMMA               = 0.05   # Aversao ao risco: maior = mais conservador  [0.01 | 0.20]
AS_KAPPA_DEFAULT       = 1.0    # Taxa de chegada de ordens (ticks/segundo)  [0.1 | 10.0]
AS_VPIN_WINDOW         = 50     # Ticks para calcular VPIN  [10 | 200]
AS_VPIN_WIDEN          = 0.70   # VPIN acima disto -> alarga spread minimo  [0.50 | 0.85]
AS_VPIN_WITHDRAW       = 0.90   # VPIN acima disto -> bloqueia entradas  [0.70 | 0.99]
AS_SPREAD_WIDEN_FACTOR = 1.1    # Factor de alargamento quando VPIN > AS_VPIN_WIDEN  [1.0 | 2.0]
AS_MIN_EDGE_C          = 0.1    # Edge minimo em cents para qualquer entrada  [0.0 | 2.0]

# --- SECCAO 11 — FEES E PARAMETROS DE LOOP ----------------------------------

FEE_RATE = 0.25
# Taxa base da Polymarket. fee = FEE_RATE * (p*(1-p))^FEE_EXP
# Fee maxima em p=0.50 e zero em p=0 ou p=1. NAO ALTERAR.

FEE_EXP = 2
# Expoente da curva de fee. NAO ALTERAR.

ASK_SPREAD = 0.01
# NOTA: Com a implementacao de leitura de Asks e Bids em tempo real,
# isto deve actuar apenas como um "Slippage Buffer" (pioria simulada da fee maker/taker)
# e nao como o spread real do mercado.
# 0.01 = 1 cent. Range: [0.005 | 0.02]

LOOP_SLEEP = 0.0005
# Timeout maximo entre iteracoes do loop principal (segundos).
# 0.0005 = 0.5ms. Menor = mais reactividade. Range: [0.0001 | 0.01]

# =============================================================================
# ESTADO GLOBAL DO BOT (DATACLASS)
# =============================================================================

@dataclass
class BotState:
    """Encapsula todo o estado mutável do bot para garantir clean code e segurança."""
    bankroll: float = BANKROLL_INIT
    daily_profit: float = 0.0
    last_day: Optional[date] = None
    bot_start_time: float = field(default_factory=time.time)
    
    best_asks: Dict[str, Optional[float]] = field(default_factory=lambda: {'up': None, 'down': None})
    best_bids: Dict[str, Optional[float]] = field(default_factory=lambda: {'up': None, 'down': None})
    
    kelly: Any = None
    as_model: Any = None
    
    risk_multiplier: float = 1.0
    accumulated_loss: float = 0.0
    recovery_rounds: int = 0

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

def log_m(module: str, action: str, msg: str) -> None: logger.info(f"[INFO] [{module}] [{action}] [{get_ts()}] | {msg}")
def log_raw(msg: str) -> None: logger.info(f"[{get_ts()}] | {msg}")
def log_info(msg: str) -> None: logger.info(f"[INFO] [{get_ts()}] | {msg}")
def log_warn(msg: str) -> None: logger.warning(f"[WARN] [{get_ts()}] | {msg}")
def log_sep() -> None: logger.info("-" * 80)
def log_sep2() -> None: logger.info("=" * 80)

# =============================================================================
# SECRETS & API LIVE
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

credenciais = load_secrets()
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
        clob_client = ClobClient(host="https://clob.polymarket.com", key=POLYMARKET_PRIVATE_KEY, chain_id=137)
        logger.info("[INFO] SDK Polymarket carregado — LIVE TRADING ACTIVO")
    except ImportError:
        logger.error("[ERROR] py-clob-client nao instalado! pip install py-clob-client")
        raise SystemExit(1)

def fetch_wallet_balance() -> float | None:
    if not clob_client: return None
    try:
        if hasattr(clob_client, 'get_balance'):
            result = clob_client.get_balance()
            if isinstance(result, dict):
                for key in ('USDC', 'usdc', 'balance', 'amount'):
                    if key in result and float(result[key]) > 0: return float(result[key])
    except Exception: pass
    
    try:
        resp = requests.get("https://clob.polymarket.com/balance", headers={"Authorization": f"Bearer {POLYMARKET_PRIVATE_KEY}"}, timeout=4)
        if resp.status_code == 200:
            for key in ('USDC', 'usdc', 'balance', 'amount'):
                if key in resp.json(): return float(resp.json()[key])
    except Exception: pass
    return None

# =============================================================================
# MATEMÁTICA E HELPERS
# =============================================================================

_FEE_LOOKUP: dict[int, float] = {i: FEE_RATE * ((i / 1000.0) * (1.0 - i / 1000.0)) ** FEE_EXP for i in range(1, 1000)}

def fee_rate(p: float) -> float:
    key = int(round(p * 1000))
    return _FEE_LOOKUP.get(key, FEE_RATE * (p * (1.0 - p)) ** FEE_EXP)

def buy_shares_net(invested: float, ask: float) -> float: return (invested / ask) * (1.0 - fee_rate(ask))
def effective_entry(ask: float) -> float: f = fee_rate(ask); return ask / (1.0 - f) if f < 1.0 else ask
def sell_payout(shares: float, p: float) -> float: return shares * p * (1.0 - fee_rate(p))

def fc(p: float) -> str: return f"{p * 100:.1f}c"
def get_ts() -> str: return datetime.now().strftime("%d/%m/%y | %H:%M:%S.%f")[:-3]

def get_remaining_str(rem: float) -> str:
    rem = max(0.0, rem)
    return f"{int(rem // 60):02d}:{int(rem % 60):02d}:{int((rem * 1000) % 1000):03d}"

def get_uptime_str() -> str:
    elapsed = int(time.time() - state.bot_start_time)
    years, elapsed = divmod(elapsed, 365 * 24 * 3600)
    months, elapsed = divmod(elapsed, 30  * 24 * 3600)
    days, elapsed = divmod(elapsed, 24  * 3600)
    hours, elapsed = divmod(elapsed, 3600)
    mins, secs = divmod(elapsed, 60)
    return f"{years}y:{months:02d}m:{days:02d}d:{hours:02d}h:{mins:02d}m:{secs:02d}s"

def calc_risk(base: float, mult: float, accum_loss: float, rec_rounds: int, bank: float) -> float:
    if bank <= 0: return MAX_RISK_PERCENT
    recovery_fraction = (accum_loss / rec_rounds) / bank if rec_rounds > 0 else 0.0
    return min((base * mult) + recovery_fraction, MAX_RISK_PERCENT)

# =============================================================================
# WS E EXECUÇÃO
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
    now = time.time()
    start_ts = now - (now % 300)
    if (start_ts + 300 - now) < 5: start_ts += 300
    return f"xrp-updown-5m-{int(start_ts)}", start_ts

async def ws_handler(t_up: str, t_down: str, q: asyncio.Queue) -> None:
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    sub = json.dumps({"assets_ids": [t_up, t_down], "type": "market", "custom_feature_enabled": True})
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=10, max_size=2**20, open_timeout=10) as ws:
                await ws.send(sub)
                log_info("WS conectado ao order book Polymarket")
                async for raw in ws:
                    items = json.loads(raw)
                    if not isinstance(items, list): items = [items]
                    tick: dict[str, float | None] = {}
                    
                    for item in items:
                        aid  = item.get("asset_id")
                        ask, bid = None, None
                        evt  = item.get("event_type")
                        
                        if evt == "book":
                            asks = item.get("asks")
                            if asks:
                                valid = [float(d['price']) for d in asks if float(d['size']) > 0]
                                if valid: ask = min(valid)
                            bids = item.get("bids")
                            if bids:
                                valid = [float(d['price']) for d in bids if float(d['size']) > 0]
                                if valid: bid = max(valid)
                        elif evt == "best_bid_ask":
                            ba, bb = item.get("best_ask"), item.get("best_bid")
                            if ba: ask = float(ba)
                            if bb: bid = float(bb)
                            
                        if aid == t_up:
                            if ask is not None: tick['up_ask'] = ask
                            if bid is not None: tick['up_bid'] = bid
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

async def place_live_order(side: str, price: float, shares: float, token_id: str) -> bool:
    if not clob_client: return False
    def _place() -> bool:
        try:
            side_const = BUY if side.upper() in ('UP', 'BUY') else SELL
            order_args = OrderArgs(token_id=token_id, price=round(price, 4), size=round(shares, 6), side=side_const, order_type="GTC")
            response = clob_client.create_and_post_order(order_args)
            log_info(f"LIVE ORDER OK | {side} {token_id[:8]}... @ {price:.4f} | Size: {shares:.6f} | OrderID: {response.get('orderID', 'OK')}")
            return True
        except Exception as e:
            log_warn(f"LIVE ORDER falhou: {e}")
            return False
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _place)

# =============================================================================
# MODELOS (Kelly, AS, PriceBuffer)
# =============================================================================

class PriceBuffer:
    __slots__ = ('max_age', 'buffer')
    def __init__(self, max_age_seconds: float = 30.0):
        self.max_age = max_age_seconds
        self.buffer: deque = deque()
    def add(self, eff_c: float, ts: float) -> None:
        self.buffer.append((ts, eff_c))
        cutoff = ts - self.max_age
        while self.buffer and self.buffer[0][0] < cutoff: self.buffer.popleft()
    def get_price_at(self, seconds_ago: float, tolerance: float = 1.0) -> float | None:
        target_ts, best_price, best_diff = time.time() - seconds_ago, None, tolerance + 1.0
        for ts, eff_c in self.buffer:
            diff = abs(ts - target_ts)
            if diff < best_diff: best_diff, best_price = diff, eff_c
        return best_price
    def get_age(self) -> float: return (time.time() - self.buffer[0][0]) if self.buffer else 0.0
    def get_delta(self, seconds_ago: float) -> tuple[float | None, bool]:
        if not self.buffer: return None, False
        past = self.get_price_at(seconds_ago)
        return (self.buffer[-1][1] - past, True) if past is not None else (None, False)
    def clear(self) -> None: self.buffer.clear()

class EmpiricalKelly:
    __slots__ = ('returns',)
    def __init__(self) -> None: self.returns: list[float] = []
    def add_result(self, invested: float, payout: float) -> None:
        if invested > 0: self.returns.append((payout - invested) / invested)
    def compute_fraction(self, fallback: float) -> tuple[float, str]:
        n = len(self.returns)
        if n < KELLY_MIN_HISTORY: return fallback, f"Kelly N/A ({n}/{KELLY_MIN_HISTORY}) -> fallback {fallback:.1%}"
        arr = np.array(self.returns)
        mean_r, std_r = float(np.mean(arr)), float(np.std(arr))
        if mean_r <= 0: return KELLY_MIN_FRACTION, f"Kelly edge negativo (mu={mean_r:.3f})"
        cv_edge = min(std_r / mean_r, 1.0)
        denom = mean_r ** 2 + std_r ** 2
        f_kelly = (mean_r / denom) if denom > 0 else fallback
        f_empirical = f_kelly * (1.0 - cv_edge)
        rng = np.random.default_rng()
        sim_returns = rng.choice(arr, size=(KELLY_MC_SIMULATIONS, max(n, 20)), replace=True)
        growth = np.prod(1.0 + f_empirical * sim_returns, axis=1)
        worst_case = float(np.percentile(growth, (1.0 - KELLY_CONFIDENCE) * 100))
        ruin_note = ""
        if worst_case < (1.0 - KELLY_RUIN_THRESHOLD):
            f_empirical *= 0.5
            ruin_note = " [MC ruin -> halved]"
        f_final = max(KELLY_MIN_FRACTION, min(KELLY_MAX_FRACTION, f_empirical))
        return f_final, f"Kelly f={f_final:.3f} | f_kelly={f_kelly:.3f} | CV={cv_edge:.2f} | mu={mean_r:.3f} sigma={std_r:.3f} | MC_worst={worst_case:.3f}{ruin_note} | n={n}"

class AvellanedaStoikov:
    __slots__ = ('tick_history', 'vol_history', 'inventory_up', 'inventory_down', '_kappa')
    def __init__(self) -> None:
        self.tick_history: deque = deque(maxlen=AS_VPIN_WINDOW * 2)
        self.vol_history:  deque = deque(maxlen=100)
        self.inventory_up, self.inventory_down, self._kappa = 0.0, 0.0, AS_KAPPA_DEFAULT
    def add_tick(self, price: float, prev_price: float | None) -> None:
        direction = 1 if (prev_price and price > prev_price) else (-1 if (prev_price and price < prev_price) else 0)
        self.tick_history.append((time.time(), price, direction))
        self.vol_history.append(price)
        if len(self.tick_history) >= 10:
            span = self.tick_history[-1][0] - self.tick_history[0][0]
            if span > 0: self._kappa = len(self.tick_history) / span
    def update_inventory(self, side: str, shares: float, is_buy: bool) -> None:
        delta = shares if is_buy else -shares
        if side == 'UP': self.inventory_up += delta
        else: self.inventory_down += delta
    @property
    def sigma2(self) -> float:
        if len(self.vol_history) < 3: return 0.01
        prices = np.array(list(self.vol_history))
        return float(np.var(np.diff(prices) / prices[:-1]))
    @property
    def vpin(self) -> float:
        recent = list(self.tick_history)[-AS_VPIN_WINDOW:]
        if len(recent) < 5: return 0.0
        v_buy, v_sell = sum(1 for _, _, d in recent if d == 1), sum(1 for _, _, d in recent if d == -1)
        total = v_buy + v_sell
        return abs(v_buy - v_sell) / total if total > 0 else 0.0
    def get_min_edge_c(self, mid_c: float, q: float, t_remaining: float) -> tuple[float | None, str]:
        if not AS_VPIN_ACTIVE: return AS_MIN_EDGE_C, "AS/VPIN OFF"
        vpin_val, sig2 = self.vpin, self.sigma2
        inv_term = AS_GAMMA * sig2 * t_remaining / 2.0
        liq_term = (1.0 / AS_GAMMA) * math.log(1.0 + AS_GAMMA / self._kappa) if AS_GAMMA > 0 else 0.0
        half_d = (inv_term + liq_term) * 100.0
        r = (mid_c / 100.0 - q * AS_GAMMA * sig2 * t_remaining) * 100.0
        if vpin_val >= AS_VPIN_WITHDRAW: return None, f"VPIN={vpin_val:.2f}>={AS_VPIN_WITHDRAW} | r={r:.1f}c delta/2={half_d:.2f}c sigma2={sig2:.5f} BLOQUEADO"
        widen = AS_SPREAD_WIDEN_FACTOR if vpin_val >= AS_VPIN_WIDEN else 1.0
        min_edge = max(AS_MIN_EDGE_C, half_d * widen)
        return min_edge, f"VPIN={vpin_val:.2f} | r={r:.1f}c | delta/2={half_d:.2f}c | min_edge={min_edge:.2f}c" + (f" [WIDEN x{AS_SPREAD_WIDEN_FACTOR}]" if widen > 1 else "")

# =============================================================================
# LOGIC LOOP
# =============================================================================

async def logic_loop(m_start: float, m_end: float, meta: dict, price_q: asyncio.Queue) -> None:
    active_trades: list[dict] = []
    flags: dict = {'s35': False, 'v30': False, 'd29': False, 's25': False, 'v20': False, 'd19': False}
    c_state: dict = {'c1': {}, 'c2': {}}

    eff_risk_per_trade = calc_risk(RISK_PER_TRADE, state.risk_multiplier, state.accumulated_loss, state.recovery_rounds, state.bankroll)
    eff_eighty_risk    = calc_risk(EIGHTY_RISK,    state.risk_multiplier, state.accumulated_loss, state.recovery_rounds, state.bankroll)
    eff_peg_risk       = calc_risk(PEG_ARBIT_RISK, state.risk_multiplier, state.accumulated_loss, state.recovery_rounds, state.bankroll)

    if state.risk_multiplier > 1.0 or state.accumulated_loss > 0:
        rec_frac = (state.accumulated_loss / state.recovery_rounds) / state.bankroll if state.recovery_rounds > 0 and state.bankroll > 0 else 0.0
        log_info(f"MARTINGALE | x{state.risk_multiplier:.0f} | accum=${state.accumulated_loss:.4f} | rec_rounds={state.recovery_rounds} | rec_frac={rec_frac:.2%} | eff: EIGHTY={eff_eighty_risk:.1%} PEG={eff_peg_risk:.1%} (cap={MAX_RISK_PERCENT:.0%})")

    eighty_seen_levels        = {'UP': set(), 'DOWN': set()}
    eighty_tick_count         = {'UP': 0,     'DOWN': 0}
    eighty_last_buy           = {'UP': 0.0,   'DOWN': 0.0}
    eighty_first_tick_t       = {'UP': None,  'DOWN': None}
    eighty_eff_min            = {'UP': None,  'DOWN': None}
    eighty_eff_max            = {'UP': None,  'DOWN': None}
    eighty_cutoff_logged      = False
    eighty_started_logged     = False
    eighty_price_buffer       = {'UP': PriceBuffer(max_age_seconds=15.0), 'DOWN': PriceBuffer(max_age_seconds=15.0)}
    eighty_vol_cooldown_until = {'UP': 0.0, 'DOWN': 0.0}

    peg_arbit_count = 0
    last_peg_time   = 0.0
    prev_u_p = prev_d_p = None

    mult_tag = f" [MARTINGALE x{state.risk_multiplier:.0f}]" if state.risk_multiplier > 1.0 else ""
    log_sep2()
    log_info(f"NOVO CICLO | Market: {meta['slug']} | LIVE: {LIVE_TRADING}")
    log_info(f"   Banca: ${state.bankroll:.4f} | Profit acum.: ${state.daily_profit:.4f}{mult_tag}")
    log_sep()

    def pct_banca(invested: float) -> str: return f"{invested / (state.bankroll + invested) * 100:.1f}% banca" if (state.bankroll + invested) else "-"

    async def open_trade(side: str, nom: float, trade_type: str, rstr: str, risk: float = None, wait_close: bool = False, fixed_invest: float = None, peg_val: float = None, token_id: str = None, extra_log: str = None, fixed_shares: float = None) -> None:
        risk = risk or eff_risk_per_trade
        kelly_log = ""
        if KELLY_ACTIVE and fixed_invest is None and fixed_shares is None:
            risk, kelly_log = state.kelly.compute_fraction(fallback=risk)
            
        ask = nom + ASK_SPREAD
        f = fee_rate(ask)
        eff = ask / (1.0 - f) if f < 1.0 else ask

        if fixed_shares is not None:
            shares = fixed_shares
            invested = shares * eff 
        elif fixed_invest is not None:
            invested = fixed_invest
            shares = (invested / ask) * (1.0 - f)
        else:
            invested = state.bankroll * risk
            shares = (invested / ask) * (1.0 - f)

        target = None
        if trade_type.startswith('CICLO'): target = CYCLE_TARGET_C / 100.0 if CYCLE_TARGET_C > 0 else None
        elif trade_type == 'EIGHTY': target = EIGHTY_TARGET_C / 100.0 if EIGHTY_TARGET_C > 0 else None
        elif trade_type == 'PEG_ARBIT': target = PEG_ARBIT_TARGET_C / 100.0 if PEG_ARBIT_TARGET_C > 0 else None
        else: target = min(0.99, eff * TARGET_MULTIPLIER)

        state.bankroll -= invested
        trade = {'side': side, 'nom': nom, 'entry': eff, 'shares': shares, 'target': target, 'type': trade_type, 'invested': invested, 'wait_close': wait_close, 'token_id': token_id, 'sl_seen_levels': set(), 'sl_tick_count': 0}
        active_trades.append(trade)

        if LIVE_TRADING and token_id: await place_live_order(side, ask, shares, token_id)
        log_m(trade_type.replace('_', ' '), 'BUY', f"rem={rstr} | {side} @ nom={fc(nom)} ask={fc(ask)} eff={fc(eff)} | inv=${invested:.4f} ({pct_banca(invested)}) | shares={shares:.4f}")

    def close_trade(trade: dict, cp: float, reason: str, rstr: str) -> None:
        payout = sell_payout(trade['shares'], cp)
        pnl = payout - trade['invested']
        state.bankroll += payout
        if KELLY_ACTIVE: state.kelly.add_result(trade['invested'], payout)
        if AS_VPIN_ACTIVE: state.as_model.update_inventory(trade['side'], trade['shares'], is_buy=False)
        log_m(trade['type'].replace('_', ' '), 'SELL', f"rem={rstr} | {trade['side']} @ {fc(cp)} | PnL: ${pnl:+.4f} | Reason: {reason}")

    def eighty_reset(e_side: str, rstr: str, reason: str, silent=False) -> None:
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side] = 0
        eighty_first_tick_t[e_side] = eighty_eff_min[e_side] = eighty_eff_max[e_side] = None
        if not silent: log_m('EIGHTY', 'RESET', f"rem={rstr} | {e_side} — {reason}")

    while True:
        now, rem = time.time(), m_end - time.time()
        
        if rem <= 0:
            u_bid_final = state.best_bids.get('up') or state.best_asks.get('up') or 0.0
            d_bid_final = state.best_bids.get('down') or state.best_asks.get('down') or 0.0
            log_info(f"FIM DE MERCADO | UP bid={fc(u_bid_final)} | DOWN bid={fc(d_bid_final)}")
            for trade in active_trades[:]:
                close_trade(trade, u_bid_final if trade['side'] == 'UP' else d_bid_final, "FIM MERCADO", "00:00:000")
            break

        rstr = get_remaining_str(rem)
        try: await asyncio.wait_for(price_q.get(), timeout=LOOP_SLEEP)
        except asyncio.TimeoutError: pass

        u_p, d_p = state.best_asks.get('up'), state.best_asks.get('down')
        u_bid, d_bid = state.best_bids.get('up'), state.best_bids.get('down')
        if u_p is None or d_p is None or (u_p == prev_u_p and d_p == prev_d_p): continue
        prev_u_p, prev_d_p = u_p, d_p

        ask_up, ask_down = u_p + ASK_SPREAD, d_p + ASK_SPREAD
        eff_up, eff_down = effective_entry(ask_up), effective_entry(ask_down)
        peg_eff = eff_up + eff_down
        underpeg_eff_c = (1.0 - peg_eff) * 100.0

        # Target e Stop-Loss (Avaliado contra BID Real)
        for trade in active_trades[:]:
            raw_bid = u_bid if trade['side'] == 'UP' else d_bid
            cp = raw_bid if raw_bid is not None else (u_p if trade['side'] == 'UP' else d_p)
            cp_eff = (cp * (1.0 - fee_rate(cp)) * 100.0) if cp else 0.0

            if trade.get('target') is not None and cp and cp >= trade['target']:
                close_trade(trade, cp, "TARGET", rstr)
                active_trades.remove(trade)
                continue

            if trade['type'] == 'PEG_ARBIT': continue

            if cp_eff > 0 and cp_eff < STOPLOSS_PRICE_C:
                # Utiliza o PRICE_STEP_C para bucketing do Stop-Loss
                level_key = round(cp_eff / PRICE_STEP_C) * PRICE_STEP_C
                if level_key not in trade['sl_seen_levels']:
                    trade['sl_seen_levels'].add(level_key)
                    trade['sl_tick_count'] += 1
                if trade['sl_tick_count'] >= STOPLOSS_TICKS:
                    close_trade(trade, cp, "STOP-LOSS", rstr)
                    active_trades.remove(trade)
            else:
                if trade['sl_tick_count'] > 0:
                    trade['sl_seen_levels'].clear()
                    trade['sl_tick_count'] = 0

        # Logica EIGHTY
        if EIGHTY_ACTIVE and rem <= EIGHTY_START_REM_S and rem > EIGHTY_CUTOFF_S:
            for e_side, nom, eff_price in (('UP', u_p, eff_up), ('DOWN', d_p, eff_down)):
                eff_c = eff_price * 100.0
                if EIGHTY_MIN_EFF_C <= eff_c <= EIGHTY_MAX_EFF_C:
                    # Utiliza o PRICE_STEP_C para bucketing da entrada EIGHTY
                    level_key = round(eff_c / PRICE_STEP_C) * PRICE_STEP_C
                    if level_key not in eighty_seen_levels[e_side]:
                        eighty_seen_levels[e_side].add(level_key)
                        eighty_tick_count[e_side] += 1
                    
                    if eighty_tick_count[e_side] >= EIGHTY_MIN_TICKS and state.bankroll > 0:
                        await open_trade(e_side, nom, 'EIGHTY', rstr, risk=eff_eighty_risk, wait_close=True, token_id=meta['up'] if e_side == 'UP' else meta['down'])
                        eighty_reset(e_side, rstr, "ENTRADA FEITA", silent=True)
                        eighty_last_buy[e_side] = now

# =============================================================================
# MAIN — orquestracao principal
# =============================================================================

async def main() -> None:
    state.kelly = EmpiricalKelly()
    state.as_model = AvellanedaStoikov()

    log_sep2()
    log_info("BOT XRP POLYMARKET v0.37.0 INICIADO")
    log_info(f"   LIVE_TRADING : {LIVE_TRADING}")
    log_sep2()

    while True:
        slug, start_ts = get_current_slug()
        meta = fetch_metadata(slug)
        if not meta:
            await asyncio.sleep(1)
            continue

        market_day = datetime.fromtimestamp(start_ts).date()
        if state.last_day is None or market_day != state.last_day:
            state.daily_profit = 0.0
            state.last_day = market_day
            state.kelly = EmpiricalKelly()
            state.as_model = AvellanedaStoikov()
            
            if LIVE_TRADING:
                live_bal = fetch_wallet_balance()
                if live_bal and live_bal > 0: state.bankroll = live_bal
            log_info(f"NOVO DIA {market_day} | Banca Atual: ${state.bankroll:.4f}")

        price_queue = asyncio.Queue(maxsize=500)
        state.best_asks['up'] = state.best_asks['down'] = None
        state.best_bids['up'] = state.best_bids['down'] = None

        ws_task = asyncio.create_task(ws_handler(meta['up'], meta['down'], price_queue))
        await asyncio.sleep(0.8)

        if state.best_asks['up'] is not None:
            pre_bank = state.bankroll

            await logic_loop(start_ts, start_ts + 300, meta, price_queue)

            profit_this = state.bankroll - pre_bank
            state.daily_profit += profit_this

            # Gestao de Martingale Global
            if profit_this < 0:
                loss = math.fabs(profit_this)
                state.accumulated_loss += loss * 0.5
                state.recovery_rounds += RECOVERY_ROUNDS_PER_LOSS
                state.risk_multiplier = min(state.risk_multiplier * 2.0, float(MAX_RISK_MULTIPLIER))
            elif profit_this > 0:
                state.accumulated_loss = max(0.0, state.accumulated_loss - profit_this)
                state.recovery_rounds = max(0, state.recovery_rounds - 1)
                state.risk_multiplier = 1.0

            log_info(f"TOTAL | PnL Diário: ${state.daily_profit:+.4f} | Banca: ${state.bankroll:.4f} | Uptime: {get_uptime_str()}")
            log_sep2()

        ws_task.cancel()
        try: await ws_task
        except asyncio.CancelledError: pass
        await asyncio.sleep(0.5)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: log_info("BOT PARADO PELO UTILIZADOR")