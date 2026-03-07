# =============================================================================
# BOT XRP POLYMARKET — v1.2.1
# =============================================================================
#
# CHANGELOG v1.2.1  [3 alterações cirúrgicas — Stop-Loss Cirúrgico]:
#
# [1] stop_loss_trigger (Event global) -> stop_loss_triggered_sides (set):
#     Removido asyncio.Event() de pânico global.
#     Substituído por set() que regista especificamente qual(is) lado(s)
#     ('UP', 'DOWN') atingiram as condições de crash.
#     Apenas dentro de logic_loop — sem impacto em variáveis globais.
#
# [2] stop_loss_task() — disparo cirúrgico sem break:
#     Removido: stop_loss_trigger.set() + break (que matava a task).
#     Adicionado: para cada side em triggered, adiciona ao set
#     stop_loss_triggered_sides e faz reset imediato dos contadores
#     desse lado (sl_ticks, sl_started, sl_levels) para evitar
#     re-disparo imediato no ciclo seguinte.
#     A task CONTINUA A CORRER para monitorizar o lado oposto.
#
# [3] Handler no loop principal — venda cirúrgica, sem break:
#     Removido: if stop_loss_trigger.is_set() -> panic sell global
#               de active_trades + break (matava o ciclo de 5min).
#     Adicionado: if stop_loss_triggered_sides -> itera pelos lados
#               sinalizados e fecha APENAS posições com:
#                 * trade['type'] == 'GAMBLING'
#                 * trade['side'] == lado que ativou o gatilho
#     Posições 'PEG ARBIT' e o lado oposto ficam intactos.
#     Break ELIMINADO — logic_loop continua para gerir posições
#     remanescentes até resolução ou fim de mercado.
#
# =============================================================================
# CHANGELOG v1.2.0  [5 alterações cirúrgicas]:
#
# [1] PEG ARBIT (ex SPREAD CATCH) — renomear + trigger + async gather:
#     Todos os "SPREAD CATCH" / "SC_*" / "SPREAD_CATCH_*" renomeados para
#     "PEG ARBIT" / "PA_*" / "PEG_ARBIT_*" em params, logs e comentários.
#     PA_TRIGGER_SUM = 0.98  (era SC_TRIGGER_SUM = 0.960)
#     Gatilho: ask_sum = ask_up + ask_down <= PA_TRIGGER_SUM
#     Compra simultânea via asyncio.gather(open_trade_UP, open_trade_DOWN)
#     para reduzir latência entre as duas ordens.
#     Log: "Underpeg=Xc" mostra (1-ask_sum)*100 em cents.
#     Removido filtro de spread do PEG ARBIT (gatilho = ask_sum apenas).
#
# [2] TICK LOG — spread removido, PEG adicionado:
#     Removido "sprd_UP=Xc sprd_DOWN=Xc" do log de tick principal.
#     Substituído por "PEG=X.XXXX" (ask_sum) e "underpeg=Xc" quando activo.
#     best_spreads_c mantido internamente mas não aparece no tick log.
#
# [3] GAMBLING FIX — bloqueio NEUTRAL visível + GAMB_PEG_MIN usa ask_sum:
#     Raiz do problema: GAMB_NEUTRAL_BOTH=False + trend preso NEUTRAL =>
#     gambling nunca dispara. Adicionado log explícito por tick:
#       [GAMBLING] [NEUTRAL_BLOCK] trend=NEUTRAL, GAMB_NEUTRAL_BOTH=False
#     GAMB_PEG_MIN agora compara ask_sum (ask_up+ask_down), não bid_sum.
#     Comentário corrigido: "Soma minima ask_up + ask_down para entrar".
#     Gambling WATCH log substituiu spread por PEG info.
#     GAMB_SPREAD_MAX_CENTS removido (spread não é filtro do Gambling).
#
# [4] PREÇOS SDK — fetch_initial_prices_sdk():
#     Nova função async que semeia best_bids/best_asks antes do WS fluir:
#       ASK = client.get_price(token_id, "BUY")["price"]   (lowest ask)
#       BID = client.get_price(token_id, "SELL")["price"]  (highest bid)
#     Chamada em main() logo após ws_task iniciar, antes do sleep(1.0).
#     Logs para ficheiro: PRICES INIT | ASK_UP=Xc BID_UP=Xc ...
#
# [5] TREND HISTÓRIA — iteração correcta + log detalhado para ficheiro:
#     fetch_trend_from_clob() agora itera list(history) com point['t'],
#     point['p'] exactamente como os docs mostram.
#     Log de cada ponto para ficheiro: TREND DATA | t=... p=...
#     (máx 10 pontos logados para não saturar — primeiro 5, último 5).
#     Log resumo: n_pts, first_avg, last_avg, delta, resultado.
#     find_1h_xrp_up_token(): log detalhado de cada tentativa ao ficheiro.
#
# =============================================================================
# CHANGELOG v1.1.0:
# [1] GAMBLING_RISK=0.03; PEG_ARBIT_RISK=0.05; MAX_RISK_PERCENT=0.20
# [2] Logging exclusivo em ficheiro (bot_xrp.log)
# [3] Trend via SDK nativo (clob_ro_client.get_prices_history)
# [4] RECOVERY_ROUNDS_STEP=10; SL_THRESHOLD=0.30; Anti-Dump mid<SL_MID_MAX
# [5] SC gatilho em ask_sum (custo real)
# [6] Spread nativo SDK (get_spread); best_spreads_c em cents
# =============================================================================
# CHANGELOG v1.0.0:
# [v1.0.0] BUY ao ASK; SELL ao BID; resolucao WS market_resolved
# [v1.0.0] Martingale MAX_RISK=20%, MAX_MULT=x8, extra=50%*acc_loss
# [v1.0.0] Deltas absolutos: D0.5s>=5c D1s>=7c D1.5s>=9c D2s>=11c
# =============================================================================
# CHANGELOG v0.38.0:
# [v0.38.0] Fee formula documentada; fetch dinamico fee_rate_bps
# =============================================================================
# CHANGELOG v0.37.0:
# [v0.37.0] SPREAD CATCH + GAMBLING + WS eventos multiplos
# =============================================================================
# CHANGELOG v0.36.0:
# [v0.36.0] Stop-Loss independente; Martingale+Recovery; LIVE banca real
# =============================================================================
# CHANGELOG v0.35.0:
# [v0.35.0] Martingale cap; PEG com preco Efectivo
# =============================================================================

import asyncio
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
GAMBLING_RISK     = 0.03  # Risco base Gambling: fraccao da banca actual por trade [Range: 0.01 | 0.50]
PEG_ARBIT_RISK    = 0.05  # Risco base Peg Arbit: fraccao da banca (budget total ambos os lados) [Range: 0.01 | 0.50]
MAX_RISK_PERCENT  = 0.25  # Hard cap INVIOLAVEL: investimento total nunca excede 20% da banca [Range: 0.10 | 0.50]
MAX_RISK_MULT     = 8     # Multiplicador martingale maximo (1->2->4->8) [Range: 2 | 16]

# --- RECOVERY ---
RECOVERY_ROUNDS_STEP = 10  # Rondas de recovery adicionadas por cada perda registada [Range: 1 | 50]

# --- TOGGLES ---
PEG_ARBIT_ACTIVE    = True  # Peg Arbit activo [Range: False | True]
GAMBLING_ACTIVE     = True  # Gambling activo [Range: False | True]
STOP_LOSS_ACTIVE    = True  # Stop-Loss activo [Range: False | True]

# --- PEG ARBIT (ex SPREAD CATCH) ---
PA_TRIGGER_SUM    = 0.98        # Gatilho: entra se ask_up+ask_down <= valor (underpeg no custo real) [Range: 0.940 | 0.999]
PA_COOLDOWN       = 0.05        # Intervalo minimo entre entradas PA consecutivas (seg) [Range: 0.01 | 5.0]
PA_MIN_REM        = 1.0         # Remaining minimo para entrar no PA (seg) [Range: 1.0 | 30.0]
PA_TARGET_BID_C   = 0.0         # Target de venda antecipada ao BID (cents; 0=hold ate resolucao) [Range: 0.0 | 99.0]
MAX_PA_ENTRIES    = 10_000_000  # Entradas maximas PA por ciclo [Range: 1 | 10000000]

# --- GAMBLING ---
GAMB_START_REM_S      = 300     # Activa Gambling quando remaining <= X seg [Range: 60 | 300]
GAMB_CUTOFF_S         = 5       # Para Gambling quando remaining <= X seg [Range: 0 | 30]
GAMB_MIN_EFF_C        = 75.0    # eff_c minimo para entrada (cents); eff_c=ask*(1+fee_rate(ask))*100 [Range: 50.0 | 95.0]
GAMB_MAX_EFF_C        = 95.0    # eff_c maximo para entrada (cents) [Range: 82.0 | 99.9]
GAMB_MIN_TICKS        = 5       # Niveis unicos de eff_c (arredond. 0.5c) para confirmar consolidacao [Range: 2 | 20]
GAMB_VOL_MAX_C        = 4.5     # Variacao maxima de eff_c na janela de volatilidade (cents) [Range: 1.0 | 20.0]
GAMB_VOL_WINDOW_S     = 5.0     # Janela temporal para calcular volatilidade (seg) [Range: 1.0 | 30.0]
GAMB_VOL_COOLDOWN_S   = 5.0     # Cooldown apos volatilidade excessiva (seg) [Range: 1.0 | 30.0]
GAMB_BUY_COOLDOWN     = 4.0     # Cooldown entre compras do mesmo lado (seg) [Range: 0.5 | 30.0]
GAMB_PEG_MIN          = 0.970   # Soma minima ask_up + ask_down para entrar [Range: 0.90 | 0.999]
GAMB_NEUTRAL_BOTH     = True    # Trend NEUTRAL: False=nao opera, True=opera ambos os lados [Range: False | True]
GAMB_TARGET_BID_C     = 0.0     # Target de venda ao BID (cents; 0=hold ate resolucao) [Range: 0.0 | 99.0]

# --- GAMBLING: DELTA THRESHOLDS (activam apenas se |delta| >= threshold) ---
GAMB_D05_THRESH_C  = 5.0   # Threshold absoluto em cents para D0.5s ser considerado activo [Range: 1.0 | 20.0]
GAMB_D10_THRESH_C  = 7.0   # Threshold absoluto em cents para D1.0s ser considerado activo [Range: 1.0 | 20.0]
GAMB_D15_THRESH_C  = 9.0   # Threshold absoluto em cents para D1.5s ser considerado activo [Range: 1.0 | 20.0]
GAMB_D20_THRESH_C  = 11.0  # Threshold absoluto em cents para D2.0s ser considerado activo [Range: 1.0 | 30.0]
GAMB_PUMP_THRESH_C = 3.5   # Delta positivo acima deste valor em GAMB_PUMP_TIME_S = pump [Range: 0.5 | 10.0]
GAMB_PUMP_TIME_S   = 1.5   # Janela temporal para detectar pump (seg) [Range: 0.5 | 5.0]

# --- TREND 1H XRP ---
TREND_UPDATE_S  = 60.0   # Intervalo de actualizacao do trend (seg) [Range: 30.0 | 300.0]
TREND_FIDELITY  = 60     # Granularidade do historico de precos (data points/intervalo) [Range: 5 | 60]
TREND_THRESHOLD = 0.015  # Variacao minima entre tercos para classificar UP/DOWN [Range: 0.003 | 0.050]
TREND_INTERVAL  = "1d"   # Intervalo CLOB Price History para fetch do historico [Range: "1h" | "max"]
TREND_LOG_PTS   = 5      # Numero de pontos (inicio e fim) a logar no ficheiro [Range: 1 | 30]

# --- STOP-LOSS (Anti-Dump) ---
SL_THRESHOLD = 0.30  # BID abaixo deste nivel (30c) inicia contagem Anti-Dump [Range: 0.01 | 0.50]
SL_TICKS     = 6     # Ticks consecutivos abaixo do threshold para disparar SL [Range: 1 | 20]
SL_CHECK_S   = 2.0   # Intervalo de verificacao do SL (seg) [Range: 0.1 | 5.0]
SL_MID_MAX   = 0.80  # Midpoint maximo para validar tick SL (filtra wicks de liquidez) [Range: 0.10 | 0.80]

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

# Microestrutura em tempo real (actualizados por tick WS e SDK)
best_bids        = {'up': None, 'down': None}  # Melhor BID por lado (preco de VENDA) [Range: 0.0 | 1.0]
best_asks        = {'up': None, 'down': None}  # Melhor ASK por lado (preco de COMPRA) [Range: 0.0 | 1.0]
best_spreads_c   = {'up': None, 'down': None}  # Spread em cents absolutos (SDK nativo; interno) [Range: 0.0 | 100.0]

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
# LOGGING — exclusivo em ficheiro; bot corre silencioso no terminal
# =============================================================================

_fmt  = logging.Formatter('%(message)s')
_fh   = logging.FileHandler('bot_xrp.log', encoding='utf-8')
_fh.setFormatter(_fmt)
logger = logging.getLogger('bot_xrp')
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
clob_ro_client = None   # Cliente somente-leitura (spread + trend + prices; sempre disponivel)

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import OrderArgs
    from py_clob_client.order_builder.constants import BUY as SDK_BUY
    # Cliente read-only sem chave — para get_price, get_prices_history e get_spread públicos
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
# SDK HELPERS — precos, spread, trend
# =============================================================================

def fetch_spread_sdk(token_id: str) -> float | None:
    """
    Obtem spread nativo via SDK Polymarket (uso interno para best_spreads_c).
    Referencia: https://docs.polymarket.com/trading/orderbook#spreads
    client.get_spread(token_id)["spread"] * 100 = cents absolutos.
    Nao e feito nenhum calculo ou conversao manual.
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

def _sdk_get_price_sync(token_id: str, side: str) -> float | None:
    """
    Fetch síncrono do melhor preco via SDK.
    docs (trading/orderbook#prices):
      side="BUY"  -> retorna o lowest ask (preco que pagas ao comprar)
      side="SELL" -> retorna o highest bid (preco que recebes ao vender)
    Referencia:
      buy_price  = client.get_price(token_id, "BUY")
      sell_price = client.get_price(token_id, "SELL")
      ASK = buy_price["price"]   (lowest ask)
      BID = sell_price["price"]  (highest bid)
    """
    if clob_ro_client is None:
        return None
    try:
        result = clob_ro_client.get_price(token_id, side)
        return float(result["price"])
    except Exception as e:
        log_warn(f"get_price({token_id[:12]}..., {side}): {e}")
        return None

async def fetch_initial_prices_sdk(t_up: str, t_down: str):
    """
    Semeia best_bids e best_asks via SDK antes do WS fluir.
    Garante que o bot tem precos correctos desde o primeiro tick.

    SDK (docs trading/orderbook#prices):
      ASK_UP   = client.get_price(t_up,   "BUY")["price"]  (lowest ask UP)
      BID_UP   = client.get_price(t_up,   "SELL")["price"] (highest bid UP)
      ASK_DOWN = client.get_price(t_down, "BUY")["price"]  (lowest ask DOWN)
      BID_DOWN = client.get_price(t_down, "SELL")["price"] (highest bid DOWN)
    """
    if clob_ro_client is None:
        log_warn("fetch_initial_prices_sdk: clob_ro_client nao disponivel")
        return
    _loop = asyncio.get_event_loop()
    try:
        ask_up, bid_up, ask_down, bid_down = await asyncio.gather(
            _loop.run_in_executor(None, _sdk_get_price_sync, t_up,   "BUY"),
            _loop.run_in_executor(None, _sdk_get_price_sync, t_up,   "SELL"),
            _loop.run_in_executor(None, _sdk_get_price_sync, t_down, "BUY"),
            _loop.run_in_executor(None, _sdk_get_price_sync, t_down, "SELL"),
        )
        if ask_up   is not None: best_asks['up']   = ask_up
        if bid_up   is not None: best_bids['up']   = bid_up
        if ask_down is not None: best_asks['down'] = ask_down
        if bid_down is not None: best_bids['down'] = bid_down

        log_info(
            f"PRICES INIT SDK | "
            f"ASK_UP={fc(ask_up) if ask_up else 'N/A'} "
            f"BID_UP={fc(bid_up) if bid_up else 'N/A'} | "
            f"ASK_DOWN={fc(ask_down) if ask_down else 'N/A'} "
            f"BID_DOWN={fc(bid_down) if bid_down else 'N/A'}"
        )
        if ask_up and ask_down:
            ask_sum_init = ask_up + ask_down
            log_info(f"PRICES INIT SDK | PEG={ask_sum_init:.4f} (ask_sum inicial)")
    except Exception as e:
        log_warn(f"fetch_initial_prices_sdk falhou: {e}")

# =============================================================================
# API HELPERS
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
    """
    Encontra o token ID UP do mercado XRP 1h actual.
    Tenta primeiro via SDK (get_markets), fallback para REST Gamma.
    Todos os resultados (successo ou falha) vao para o ficheiro log.
    """
    log_info("TREND | Procurando token XRP 1h...")

    # Tentativa SDK
    if clob_ro_client is not None:
        try:
            markets_page = clob_ro_client.get_markets(next_cursor="")
            data_list    = markets_page.get("data") or []
            log_info(f"TREND | SDK get_markets: {len(data_list)} mercados recebidos")
            for mkt in data_list:
                slug_v = (mkt.get("market_slug") or mkt.get("slug") or "").lower()
                if "xrp" in slug_v and "1h" in slug_v and not mkt.get("closed", True):
                    token_ids = mkt.get("clob_token_ids") or mkt.get("tokens", [])
                    if isinstance(token_ids, list) and token_ids:
                        tid = token_ids[0] if isinstance(token_ids[0], str) else token_ids[0].get("token_id")
                        if tid:
                            log_info(f"TREND | Token 1h via SDK | slug={slug_v} | token={tid[:16]}...")
                            return tid
            log_warn("TREND | SDK: nenhum mercado XRP 1h encontrado na pagina")
        except Exception as e:
            log_warn(f"TREND | find_1h_xrp_up_token SDK falhou: {e} — fallback REST")

    # Fallback REST Gamma
    now_ts  = time.time()
    hour_ts = int(now_ts - (now_ts % 3600))
    for slug in [f"xrp-updown-1h-{hour_ts}", f"xrp-up-down-1h-{hour_ts}", f"xrp-1h-{hour_ts}"]:
        try:
            r    = requests.get(f"{GAMMA_API_URL}/events?slug={slug}", timeout=5)
            data = r.json()
            log_info(f"TREND | REST Gamma slug={slug}: status={r.status_code}")
            if data and isinstance(data, list) and data[0].get('markets'):
                ids = json.loads(data[0]['markets'][0].get('clobTokenIds', '[]'))
                if ids:
                    log_info(f"TREND | Token 1h via REST | slug={slug} | token={ids[0][:16]}...")
                    return ids[0]
        except Exception as e:
            log_warn(f"TREND | REST slug={slug} falhou: {e}")
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
                        log_info(f"TREND | Token 1h via REST fallback | slug={sv} | token={ids[0][:16]}...")
                        return ids[0]
        log_warn("TREND | REST fallback: nenhum mercado XRP 1h encontrado em 200 mercados")
    except Exception as e:
        log_warn(f"TREND | REST fallback geral falhou: {e}")

    return None

def fetch_trend_from_clob(token_id: str) -> str:
    """
    Fetch do historico de preco via SDK nativo e calculo de tendencia.

    SDK nativo (docs trading/orderbook#price-history):
        history = clob_ro_client.get_prices_history(
            market=token_id,   # param chama-se "market" mas recebe token_id
            interval="1d",     # historico do dia (TREND_INTERVAL)
            fidelity=60        # 60 data points (TREND_FIDELITY)
        )

    Iteracao dos pontos (exactamente como os docs mostram):
        for point in history:
            t = point['t']   # unix timestamp
            p = point['p']   # price float

    Primeiros TREND_LOG_PTS e ultimos TREND_LOG_PTS sao logados para ficheiro.
    Filtra pontos da ultima hora (t >= now-3600) para calculo de tendencia 1h.
    Delta = last_avg - first_avg; threshold = TREND_THRESHOLD.
    Fallback para REST manual se SDK falhar.
    """
    now_ts   = time.time()
    hour_ago = now_ts - 3600.0

    def _calc_and_log(history_list: list, source: str) -> str:
        """Calcula trend dado um historico e loga os pontos ao ficheiro."""
        n_total = len(history_list)
        if n_total == 0:
            log_warn(f"TREND {source} | historico vazio")
            return 'NEUTRAL'

        # Loga primeiros e ultimos TREND_LOG_PTS pontos
        log_info(f"TREND {source} | {n_total} pontos totais | token={token_id[:16]}...")
        for i, pt in enumerate(history_list[:TREND_LOG_PTS]):
            log_info(f"TREND DATA [{i:02d}] | t={pt['t']} p={pt['p']:.4f}")
        if n_total > TREND_LOG_PTS * 2:
            log_info(f"TREND DATA ... ({n_total - TREND_LOG_PTS*2} pontos omitidos)")
        for i, pt in enumerate(history_list[-TREND_LOG_PTS:]):
            log_info(f"TREND DATA [{n_total - TREND_LOG_PTS + i:02d}] | t={pt['t']} p={pt['p']:.4f}")

        # Filtra ultima hora
        pts_1h = [float(pt['p']) for pt in history_list if float(pt.get('t', 0)) >= hour_ago]
        log_info(f"TREND | Pontos na ultima hora: {len(pts_1h)}/{n_total}")
        if len(pts_1h) < 3:
            pts_1h = [float(pt['p']) for pt in history_list]
            log_info(f"TREND | Poucos pontos 1h — usando todos ({len(pts_1h)})")

        n = len(pts_1h)
        if n < 3:
            log_warn(f"TREND | Insuficientes ({n} pts) — NEUTRAL")
            return 'NEUTRAL'

        third     = max(1, n // 3)
        first_avg = sum(pts_1h[:third]) / third
        last_avg  = sum(pts_1h[-third:]) / third
        delta     = last_avg - first_avg

        log_info(
            f"TREND CALC | first_avg={first_avg:.4f} last_avg={last_avg:.4f} "
            f"delta={delta:+.4f} threshold={TREND_THRESHOLD:.3f}"
        )

        if   delta >  TREND_THRESHOLD:
            result = 'UP'
        elif delta < -TREND_THRESHOLD:
            result = 'DOWN'
        else:
            result = 'NEUTRAL'

        log_info(f"TREND RESULT | {result}")
        return result

    # Tentativa SDK nativa
    if clob_ro_client is not None:
        try:
            log_info(
                f"TREND SDK | get_prices_history(market={token_id[:16]}..., "
                f"interval={TREND_INTERVAL!r}, fidelity={TREND_FIDELITY})"
            )
            history_raw = clob_ro_client.get_prices_history(
                market=token_id,
                interval=TREND_INTERVAL,
                fidelity=TREND_FIDELITY
            )
            # SDK pode devolver lista directa ou dict com 'history'
            if isinstance(history_raw, dict):
                history_raw = history_raw.get("history", [])
            history_list = list(history_raw) if history_raw else []
            return _calc_and_log(history_list, "SDK")
        except Exception as e:
            log_warn(f"TREND SDK get_prices_history falhou: {e} — fallback REST")

    # Fallback REST manual
    try:
        log_info(
            f"TREND REST | GET /prices-history market={token_id[:16]}... "
            f"interval={TREND_INTERVAL} fidelity={TREND_FIDELITY}"
        )
        r    = requests.get(
            f"{CLOB_REST_URL}/prices-history",
            params={"market": token_id, "interval": TREND_INTERVAL, "fidelity": TREND_FIDELITY},
            timeout=6
        )
        history_list = r.json().get("history", [])
        return _calc_and_log(history_list, "REST")
    except Exception as e:
        log_warn(f"TREND REST falhou: {e}")
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
                if xrp_1h_token_up is None:
                    log_warn("TREND | Token XRP 1h nao encontrado — mantendo NEUTRAL")
            if xrp_1h_token_up is not None:
                new_t = fetch_trend_from_clob(xrp_1h_token_up)
                if new_t != xrp_1h_trend:
                    log_info(f"TREND UPDATE | {xrp_1h_trend} -> {new_t} | interval={TREND_INTERVAL}")
                    xrp_1h_trend = new_t
                else:
                    log_info(f"TREND STABLE | {xrp_1h_trend}")
            else:
                xrp_1h_trend = 'NEUTRAL'
        except Exception as e:
            log_warn(f"trend_update_task erro: {e}")
        await asyncio.sleep(TREND_UPDATE_S)

# =============================================================================
# WEBSOCKET HANDLER
# =============================================================================

async def ws_handler(t_up: str, t_down: str):
    """
    WebSocket handler. Actualiza best_bids, best_asks e best_spreads_c por tick.

    Eventos tratados (docs trading/orderbook#event-types):
      book          -> bids/asks -> best_bid=max(bids), best_ask=min(asks)
      best_bid_ask  -> best_bid, best_ask, spread nativo (cents) via custom_feature_enabled
      price_change  -> price_changes[].best_bid / best_ask
      market_resolved -> winning_asset_id (req custom_feature_enabled)

    Precos (docs trading/orderbook#prices):
      ASK = preço ao comprar (lowest ask) -> BUY side
      BID = preço ao vender (highest bid) -> SELL side
    """
    global resolved_winner_asset
    _bids   = best_bids
    _asks   = best_asks
    _sprc   = best_spreads_c
    _set    = price_change.set
    _loop   = asyncio.get_event_loop()

    _tid_map = {t_up: 'up', t_down: 'down'}

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

                        # ── market_resolved ──────────────────────────────────
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
                            bids_r = item.get("bids", [])
                            asks_r = item.get("asks", [])
                            if bids_r:
                                v = [float(d['price']) for d in bids_r if float(d.get('size', 0)) > 0]
                                if v: bid_p = max(v)
                            if asks_r:
                                v = [float(d['price']) for d in asks_r if float(d.get('size', 0)) > 0]
                                if v: ask_p = min(v)
                            # Spread interno via SDK (non-blocking)
                            try:
                                sp_c = await _loop.run_in_executor(
                                    None, fetch_spread_sdk, aid
                                )
                                if sp_c is not None:
                                    _sprc[sk] = sp_c
                            except Exception:
                                pass

                        elif evt == "best_bid_ask":
                            bb = item.get("best_bid")
                            ba = item.get("best_ask")
                            if bb: bid_p = float(bb)
                            if ba: ask_p = float(ba)
                            # Spread nativo da API — valor directo em cents
                            sp_raw = item.get("spread")
                            if sp_raw is not None:
                                _sprc[sk] = float(sp_raw) * 100.0

                        elif evt == "price_change":
                            pcs = item.get("price_changes", [])
                            if pcs:
                                bb = pcs[-1].get("best_bid")
                                ba = pcs[-1].get("best_ask")
                                if bb: bid_p = float(bb)
                                if ba: ask_p = float(ba)
                            try:
                                sp_c = await _loop.run_in_executor(
                                    None, fetch_spread_sdk, aid
                                )
                                if sp_c is not None:
                                    _sprc[sk] = sp_c
                            except Exception:
                                pass

                        if bid_p is not None:
                            _bids[sk] = bid_p
                            updated    = True
                        if ask_p is not None:
                            _asks[sk] = ask_p
                            updated    = True

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

    PEG ARBIT: ask_sum = ask_up + ask_down <= PA_TRIGGER_SUM
               Compra simultânea via asyncio.gather.
    GAMBLING: GAMB_PEG_MIN usa ask_sum (ask_up+ask_down).
              Quando NEUTRAL bloqueia, loga NEUTRAL_BLOCK explicitamente.
    STOP-LOSS CIRÚRGICO (v1.2.1):
              stop_loss_triggered_sides (set) regista qual(is) lado(s)
              atingiram as condições de crash.
              Apenas posições GAMBLING do lado exato são fechadas.
              Posições PEG ARBIT e o lado oposto ficam intactos.
              O ciclo de 5 minutos NÃO termina após o SL — continua
              a gerir posições remanescentes até resolução.
    """
    global bankroll, daily_profit

    active_trades = []

    # --- ALTERAÇÃO v1.2.1 [1/3] ---
    # Substituído asyncio.Event() global por set() de lados específicos.
    # stop_loss_triggered_sides regista qual(is) lado(s) ('UP', 'DOWN')
    # atingiram as condições de crash neste ciclo.
    stop_loss_triggered_sides: set[str] = set()

    # Calcula riscos uma unica vez (fora do loop quente)
    eff_pa_risk,   extra_pa,   base_pa_pct   = calc_effective_risk(
        PEG_ARBIT_RISK, r_mult, bankroll, r_accum_loss
    )
    eff_gamb_risk, extra_gamb, base_gamb_pct = calc_effective_risk(
        GAMBLING_RISK, r_mult, bankroll, r_accum_loss
    )

    # Header de ronda
    mult_tag = f" [MARTINGALE x{r_mult:.0f}]" if r_mult > 1.0 else ""
    cap_pa   = " [CAP]" if eff_pa_risk   >= MAX_RISK_PERCENT else ""
    cap_gamb = " [CAP]" if eff_gamb_risk >= MAX_RISK_PERCENT else ""

    mods = []
    if PEG_ARBIT_ACTIVE:
        mods.append(f"PEG_ARBIT(ask_sum<={PA_TRIGGER_SUM:.3f})")
    if GAMBLING_ACTIVE:
        mods.append(f"GAMBLING({GAMB_START_REM_S}s->{GAMB_CUTOFF_S}s,trend={xrp_1h_trend})")
    if STOP_LOSS_ACTIVE:
        mods.append(f"STOP_LOSS(<{SL_THRESHOLD:.2f}/{SL_TICKS}ticks/{SL_CHECK_S:.0f}s)")

    log_sep2()
    log_info(f"NOVO CICLO | {meta['slug']} | LIVE={LIVE_TRADING}")
    log_info(f"Banca: ${bankroll:.4f} | Profit dia: ${daily_profit:+.4f}{mult_tag}")
    log_info(f"Trend 1h: {xrp_1h_trend} | Modulos: {' | '.join(mods) if mods else 'nenhum'}")
    log_info(
        f"Risco efectivo: PA={base_pa_pct:.1%}+${extra_pa:.4f}{cap_pa} "
        f"| GAMB={base_gamb_pct:.1%}+${extra_gamb:.4f}{cap_gamb} | CAP={MAX_RISK_PERCENT:.0%}"
    )
    if GAMBLING_ACTIVE and xrp_1h_trend == 'NEUTRAL' and not GAMB_NEUTRAL_BOTH:
        log_warn(
            "GAMBLING | NEUTRAL_BLOCK: trend=NEUTRAL e GAMB_NEUTRAL_BOTH=False "
            "-> Gambling nao vai disparar neste ciclo. "
            "Verifica o log TREND para diagnosticar o fetch do historico."
        )
    log_sep()
    log_info("ESCUTA ACTIVA")
    log_sep()

    # =========================================================================
    # STOP-LOSS TASK — Anti-Dump cirúrgico (v1.2.1)
    # =========================================================================
    async def stop_loss_task():
        """
        BID monitorizado a cada SL_CHECK_S segundos.
        Anti-Dump: tick so conta se BID < SL_THRESHOLD E mid < SL_MID_MAX.
        Filtra wicks de liquidez onde BID colapsa mas ASK permanece normal.
        SL_TICKS ticks consecutivos validados -> regista o lado em
        stop_loss_triggered_sides e faz reset dos contadores desse lado.
        A task NAO usa break — continua a monitorizar o lado oposto.
        O loop principal fecha APENAS posicoes GAMBLING do lado exato.
        """
        sl_ticks      = {'UP': 0, 'DOWN': 0}
        sl_started    = {'UP': False, 'DOWN': False}
        sl_levels     = {'UP': set(), 'DOWN': set()}

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
                ask_val = best_asks.get(side.lower())
                if bid_val is None:
                    continue

                bid_c = bid_val * 100.0
                mid   = ((bid_val + ask_val) * 0.5) if ask_val else bid_val

                # Anti-Dump: valida tick apenas se mid tambem esta abaixo do limiar
                sl_valid = (bid_val < SL_THRESHOLD) and (mid < SL_MID_MAX)

                if sl_valid:
                    sl_ticks[side] += 1
                    level_c = round(bid_c)

                    if not sl_started[side]:
                        sl_started[side] = True
                        sl_levels[side].add(level_c)
                        log_m('STOPLOSS', 'MONITOR',
                            f"rem={rstr} | {side} iniciado @ {bid_c:.1f}c < {SL_THRESHOLD*100:.1f}c "
                            f"(mid={mid*100:.1f}c < {SL_MID_MAX*100:.0f}c)"
                        )
                    else:
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
                        reason = (
                            f"bid={bid_c:.1f}c voltou acima threshold"
                            if bid_val >= SL_THRESHOLD
                            else f"mid={mid*100:.1f}c acima {SL_MID_MAX*100:.0f}c (wick filtrado)"
                        )
                        log_m('STOPLOSS', 'RESET', f"rem={rstr} | {side} — {reason}")
                    sl_ticks[side]   = 0
                    sl_started[side] = False
                    sl_levels[side].clear()

                if sl_ticks[side] >= SL_TICKS:
                    triggered.append(side)

            # --- ALTERAÇÃO v1.2.1 [2/3] ---
            # Substituído: stop_loss_trigger.set() + break
            # Por: registo cirúrgico dos lados no set + reset de contadores.
            # A task NÃO faz break — continua a monitorizar o lado oposto.
            if triggered:
                log_m('STOPLOSS', 'TRIGGER',
                    f"rem={rstr} | lados={triggered} | threshold={SL_THRESHOLD:.2f} "
                    f"| ticks={sl_ticks}"
                )
                for _sl_side in triggered:
                    stop_loss_triggered_sides.add(_sl_side)
                    # Reset imediato dos contadores deste lado para evitar
                    # re-disparo imediato no próximo ciclo de SL_CHECK_S.
                    sl_ticks[_sl_side]   = 0
                    sl_started[_sl_side] = False
                    sl_levels[_sl_side].clear()
                # sem break — task continua a monitorizar o lado oposto

    sl_task = asyncio.create_task(stop_loss_task()) if STOP_LOSS_ACTIVE else None

    # =========================================================================
    # OPEN TRADE — BUY ao ASK
    # docs: 'you'll pay the ask when buying'
    # invested_pure = bankroll * risk   (ou shares fixas para PA)
    # shares        = invested_pure / ask
    # fee_buy       = fee_rate(ask) * invested_pure  [cobrada em shares]
    # total_out     = invested_pure + fee_buy
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

        fee_buy   = fee_rate(ask) * invested_pure
        total_out = invested_pure + fee_buy
        eff_c_val = eff_price_c(ask)

        target = None
        if trade_type == 'PEG ARBIT' and PA_TARGET_BID_C > 0.0:
            target = PA_TARGET_BID_C / 100.0
        elif trade_type == 'GAMBLING' and GAMB_TARGET_BID_C > 0.0:
            target = GAMB_TARGET_BID_C / 100.0

        bankroll -= total_out

        trade = {
            'side':          side,
            'ask':           ask,
            'bid_at_buy':    bid,
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

        bid_s = f" | BID@buy={fc(bid)}" if bid else ""
        ext_s = f" | {extra_log}" if extra_log else ""

        log_m(trade_type, 'BUY',
            f"rem={rstr} | {side} @ ASK={fc(ask)} eff={fc(eff_c_val/100)}"
            f"{bid_s}"
            f" | invested=${invested_pure:.4f} | fee=${fee_buy:.4f} | total=${total_out:.4f}"
            f" | shares={shares:.4f} | risk={risk:.1%}{ext_s}"
        )
        return trade

    # =========================================================================
    # CLOSE TRADE — SELL ao BID
    # docs: 'you'll receive the bid when selling'
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
    # Winning tokens => $1/share (fee_rate(1.0)=0); Losing => $0
    # =========================================================================
    def close_trade_resolution(trade: dict, winner: bool, rstr: str):
        global bankroll
        shares     = trade['shares']
        payout_net = resolution_payout(shares, winner)
        pnl        = payout_net - trade['total_out']
        pnl_pct    = (pnl / trade['total_out'] * 100.0) if trade['total_out'] else 0.0
        bankroll  += payout_net
        reason_s   = "RESOLUCAO GANHA ($1/share)" if winner else "RESOLUCAO PERDIDA (Total)"
        price_s    = "100.0c"                      if winner else "0.0c"
        sign       = "(+)" if pnl >= 0 else "(-)"

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
    gamb_neutral_block_last = 0.0  # ultimo timestamp do log NEUTRAL_BLOCK (throttle 30s)

    pa_count      = 0
    last_pa_time  = 0.0
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

                log_sep()
                log_info(
                    f"FIM DE MERCADO | UP final={fc(final_bid_up)} "
                    f"| DOWN final={fc(final_bid_down)}"
                )

                if active_trades:
                    log_info(f"Aguardando resolucao WS (max {RESOLVE_TIMEOUT_S:.0f}s)...")
                    try:
                        await asyncio.wait_for(
                            resolved_event.wait(), timeout=RESOLVE_TIMEOUT_S
                        )
                        winner_asset = resolved_winner_asset
                        log_info(
                            f"RESOLUCAO CONFIRMADA | winner_asset={winner_asset[:16] if winner_asset else '?'}..."
                        )
                        for trade in active_trades[:]:
                            winner = (trade.get('token_id') == winner_asset)
                            close_trade_resolution(trade, winner, "00:00:000")
                            active_trades.remove(trade)
                    except asyncio.TimeoutError:
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

            # --- ALTERAÇÃO v1.2.1 [3/3] ---
            # ── Stop-Loss cirúrgico — sem break, apenas GAMBLING do lado exato ──
            #
            # Substituído:
            #   if stop_loss_trigger.is_set(): panic sell global + break
            #
            # Por:
            #   Para cada lado sinalizado em stop_loss_triggered_sides:
            #     - Fecha APENAS trades com type=='GAMBLING' E side==_sl_side
            #     - Ignora trades 'PEG ARBIT' e o lado oposto
            #   Sem break — ciclo continua para gerir posicoes remanescentes.
            if stop_loss_triggered_sides:
                rstr = get_remaining_str(rem)
                for _sl_side in list(stop_loss_triggered_sides):
                    _closed = 0
                    for trade in active_trades[:]:
                        if trade['type'] == 'GAMBLING' and trade['side'] == _sl_side:
                            _bid_key  = trade['side'].lower()
                            _sell_bid = best_bids.get(_bid_key) or 0.0
                            close_trade(
                                trade, _sell_bid,
                                f"STOP-LOSS FLASH-CRASH [{_sl_side}]",
                                rstr
                            )
                            active_trades.remove(trade)
                            _closed += 1
                    log_sep()
                    log_info(
                        f"STOP LOSS CIRURGICO | rem={rstr} | lado={_sl_side} "
                        f"| fechadas={_closed} pos GAMBLING "
                        f"| PEG ARBIT e lado oposto intactos"
                    )
                    stop_loss_triggered_sides.discard(_sl_side)
                # sem break — ciclo continua para gerir posicoes remanescentes

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

            # PEG = ask_sum (custo real de execucao); underpeg em cents
            ask_sum    = ask_up + ask_down
            bid_sum    = bid_up + bid_down  # mantido para referencia
            underpeg_c = (1.0 - ask_sum) * 100.0

            # Midpoints (docs trading/orderbook#midpoints)
            mid_up   = (bid_up   + ask_up)   * 0.5
            mid_down = (bid_down + ask_down)  * 0.5

            # ── Tick log — PEG em vez de spread ─────────────────────────────
            # PEG = ask_sum (soma dos dois ASKs); underpeg quando < PA_TRIGGER_SUM
            peg_str = f" | PEG={ask_sum:.4f}"
            if ask_sum <= PA_TRIGGER_SUM:
                peg_str += f" underpeg={underpeg_c:.1f}c"

            log_raw(
                f"rem={rstr} | "
                f"BID_UP={fc(bid_up)} ASK_UP={fc(ask_up)} MID={fc(mid_up)} | "
                f"BID_DOWN={fc(bid_down)} ASK_DOWN={fc(ask_down)} MID={fc(mid_down)}"
                f"{peg_str}"
            )

            # =================================================================
            # MODULO 1: PEG ARBIT (ex SPREAD CATCH)
            #
            # Logica: se ask_up + ask_down <= 0.98 (underpeg), os dois lados
            # custam menos do que $1 total. Compramos ambos ao ASK e
            # um deles vai resolver a $1/share — lucro garantido se segurarmos
            # ate resolucao.
            #
            # Gatilho: ask_sum = ask_up + ask_down <= PA_TRIGGER_SUM (0.98)
            # Compra: asyncio.gather -> ambas as ordens quase simultaneas
            # Underpeg: (1 - ask_sum) * 100 cents = margem de lucro bruta
            # =================================================================
            if (PEG_ARBIT_ACTIVE
                    and ask_sum <= PA_TRIGGER_SUM
                    and rem > PA_MIN_REM
                    and pa_count < MAX_PA_ENTRIES
                    and now - last_pa_time >= PA_COOLDOWN):

                budget        = bankroll * eff_pa_risk
                ref_ask       = max(ask_up, ask_down)   # lado mais caro -> limita shares
                shares_to_buy = budget / ref_ask

                # Pre-calculo de custos (antes do bankroll ser alterado por gather)
                fee_up     = fee_rate(ask_up)   * shares_to_buy * ask_up
                fee_down   = fee_rate(ask_down) * shares_to_buy * ask_down
                total_cost = (shares_to_buy * ask_up   + fee_up +
                              shares_to_buy * ask_down + fee_down)

                log_sep()
                log_m('PEG ARBIT', 'ENTRADA',
                    f"rem={rstr} | PEG={ask_sum:.4f} Underpeg={underpeg_c:.1f}c "
                    f"| shares={shares_to_buy:.4f} | cost=${total_cost:.4f} "
                    f"| ASK_UP={fc(ask_up)} ASK_DOWN={fc(ask_down)} | #={pa_count+1}"
                )

                # Compra simultânea dos dois lados via asyncio.gather
                # (reduz latência entre as duas ordens)
                await asyncio.gather(
                    open_trade('UP',   'PEG ARBIT', rstr,
                               risk=eff_pa_risk, fixed_shares=shares_to_buy,
                               token_id=meta['up']),
                    open_trade('DOWN', 'PEG ARBIT', rstr,
                               risk=eff_pa_risk, fixed_shares=shares_to_buy,
                               token_id=meta['down'])
                )
                log_sep()
                pa_count    += 1
                last_pa_time = now

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
            #
            # GAMB_PEG_MIN usa ask_sum (ask_up+ask_down) — nao bid_sum.
            # Quando trend=NEUTRAL e GAMB_NEUTRAL_BOTH=False, log NEUTRAL_BLOCK
            # (throttled a 30s para nao saturar o ficheiro).
            # Tick log: "PEG=X.XXXX" em vez de spread.
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
                            f"| trend={xrp_1h_trend} | risk={eff_gamb_risk:.1%} "
                            f"| GAMB_NEUTRAL_BOTH={GAMB_NEUTRAL_BOTH}")

                    # NEUTRAL_BLOCK: log throttled a 30s para diagnóstico
                    if xrp_1h_trend == 'NEUTRAL' and not GAMB_NEUTRAL_BOTH:
                        if now - gamb_neutral_block_last > 30.0:
                            gamb_neutral_block_last = now
                            log_m('GAMBLING', 'NEUTRAL_BLOCK',
                                f"rem={rstr} | trend=NEUTRAL e GAMB_NEUTRAL_BOTH=False "
                                f"=> nenhuma entrada possivel. "
                                f"Verifica log TREND CALC para diagnosticar.")
                        continue  # salta todo o gambling neste tick

                    for g_side, g_ask, g_bid in (
                        ('UP',   ask_up,   bid_up),
                        ('DOWN', ask_down, bid_down)
                    ):
                        # Filtro trend (NEUTRAL ja tratado acima)
                        if   xrp_1h_trend == 'UP'   and g_side == 'DOWN': continue
                        elif xrp_1h_trend == 'DOWN'  and g_side == 'UP':   continue

                        # Cooldowns
                        if now < gamb_vol_cooldown_until[g_side]:
                            continue
                        if now - gamb_last_buy[g_side] < GAMB_BUY_COOLDOWN:
                            continue

                        # eff_c baseado no ASK (custo real de compra)
                        g_eff_c = eff_price_c(g_ask)

                        gamb_price_buffer[g_side].add(g_eff_c, now)

                        # Filtro range
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
                        gpb = gamb_price_buffer[g_side]

                        d05, h05 = gpb.get_delta(0.5)
                        d10, h10 = gpb.get_delta(1.0)
                        d15, h15 = gpb.get_delta(1.5)
                        d20, h20 = gpb.get_delta(2.0)
                        d_pt, hpt = gpb.get_delta(GAMB_PUMP_TIME_S)

                        active_05 = h05 and d05 is not None and abs(d05) >= GAMB_D05_THRESH_C
                        active_10 = h10 and d10 is not None and abs(d10) >= GAMB_D10_THRESH_C
                        active_15 = h15 and d15 is not None and abs(d15) >= GAMB_D15_THRESH_C
                        active_20 = h20 and d20 is not None and abs(d20) >= GAMB_D20_THRESH_C
                        pump_det  = hpt and d_pt is not None and d_pt >= GAMB_PUMP_THRESH_C

                        has_active = active_05 or active_10 or active_15 or active_20

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

                        dp_parts = []
                        if active_05: dp_parts.append(f"D0.5s:{d05:+.1f}c(thr={GAMB_D05_THRESH_C:.0f}c)")
                        if active_10: dp_parts.append(f"D1s:{d10:+.1f}c(thr={GAMB_D10_THRESH_C:.0f}c)")
                        if active_15: dp_parts.append(f"D1.5s:{d15:+.1f}c(thr={GAMB_D15_THRESH_C:.0f}c)")
                        if active_20: dp_parts.append(f"D2s:{d20:+.1f}c(thr={GAMB_D20_THRESH_C:.0f}c)")
                        dp_str = (" | ".join(dp_parts)) if dp_parts else f"Deltas<threshold ({gpb.get_age():.1f}s)"

                        # PEG info em vez de spread (ask_sum para este lado)
                        peg_d = f" | PEG={ask_sum:.4f}(min>={GAMB_PEG_MIN:.3f})"

                        log_m('GAMBLING', 'WATCH',
                            f"rem={rstr} | {g_side} ASK={fc(g_ask)} eff={fc(g_eff_c/100)} "
                            f"trend={xrp_1h_trend} | "
                            f"VOL={'NOK' if vol_nok else 'OK'} ({var_c:.1f}c/{elapsed:.1f}s) | "
                            f"{dp_str} {'NOK' if has_active and not delta_ok else ('ACT' if has_active else '-')}"
                            f"{peg_d} | ticks={gamb_tick_count[g_side]}/{GAMB_MIN_TICKS}"
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

                            # GAMB_PEG_MIN usa ask_sum (custo real), nao bid_sum
                            if ask_sum < GAMB_PEG_MIN:
                                gamb_reset(g_side, rstr,
                                    f"PEG={ask_sum:.4f} < min={GAMB_PEG_MIN:.3f}")
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

    # ─ Log de arranque ───────────────────────────────────────────────────────
    log_sep2()
    log_info(f"BOT XRP POLYMARKET v1.2.1 INICIADO")
    log_sep2()
    log_info(f"LIVE_TRADING     : {LIVE_TRADING}")
    log_info(f"BANKROLL_INIT    : ${bankroll:.2f}")
    log_info(f"RISCO BASE:")
    log_info(f"   GAMBLING      : {GAMBLING_RISK:.0%}")
    log_info(f"   PEG ARBIT     : {PEG_ARBIT_RISK:.0%}")
    log_info(f"MARTINGALE:")
    log_info(f"   MAX_RISK_PERCENT : {MAX_RISK_PERCENT:.0%} (CAP INVIOLAVEL)")
    log_info(f"   MAX_MULTIPLIER   : x{MAX_RISK_MULT}")
    log_info(f"   Formula          : min(base x mult + (50% acc_loss), MAX_RISK)")
    log_info(f"MODULOS:")
    log_info(f"   GAMBLING           : {'ON' if GAMBLING_ACTIVE else 'OFF'}")
    log_info(f"   PEG ARBIT          : {'ON' if PEG_ARBIT_ACTIVE else 'OFF'}")
    log_info(f"   STOP LOSS          : {'ON' if STOP_LOSS_ACTIVE else 'OFF'}")
    log_sep2()
    log_info("PEG ARBIT:")
    log_info(f"   Gatilho           : ask_sum = ask_up + ask_down <= {PA_TRIGGER_SUM:.3f}")
    log_info(f"   Execucao          : asyncio.gather (ambos os lados simultaneos)")
    log_info(f"   Log               : Underpeg=Xc = (1-ask_sum)*100")
    log_sep2()
    log_info("GAMBLING:")
    log_info(f"   GAMB_PEG_MIN      : {GAMB_PEG_MIN:.3f} (ask_up+ask_down, nao bid_sum)")
    log_info(f"   GAMB_NEUTRAL_BOTH : {GAMB_NEUTRAL_BOTH}")
    log_info(f"   Nota: se trend=NEUTRAL e NEUTRAL_BOTH=False, gambling nao opera")
    log_info(f"   Diagnostico       : ver linhas 'TREND CALC' e 'NEUTRAL_BLOCK' no log")
    log_sep2()
    log_info("PRECOS SDK (docs trading/orderbook#prices):")
    log_info(f"   ASK = client.get_price(token_id, 'BUY')['price']   (lowest ask)")
    log_info(f"   BID = client.get_price(token_id, 'SELL')['price']  (highest bid)")
    log_info(f"   fetch_initial_prices_sdk() semeia precos antes do WS fluir")
    log_sep2()
    log_info("TREND HISTORIA (docs trading/orderbook#price-history):")
    log_info(f"   get_prices_history(market=token_id, interval={TREND_INTERVAL!r}, fidelity={TREND_FIDELITY})")
    log_info(f"   Iteracao: for point in history: t=point['t'] p=point['p']")
    log_info(f"   Primeiros/Ultimos {TREND_LOG_PTS} pts logados para ficheiro")
    log_sep2()
    log_info("TICK LOG:")
    log_info(f"   PEG=ask_sum (ask_up+ask_down) em vez de spread")
    log_info(f"   underpeg=Xc aparece quando ask_sum <= {PA_TRIGGER_SUM:.3f}")
    log_sep2()
    log_info("STOP LOSS CIRURGICO (v1.2.1):")
    log_info(f"   stop_loss_triggered_sides: set() regista lado(s) em crash")
    log_info(f"   Fecha APENAS: type='GAMBLING' E side==lado_em_crash")
    log_info(f"   Ignora: PEG ARBIT e lado oposto (ficam abertos)")
    log_info(f"   Sem break: ciclo continua ate resolucao ou fim de mercado")
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
        best_spreads_c['up'] = best_spreads_c['down'] = None
        price_change.clear()

        ws_task = asyncio.create_task(ws_handler(meta['up'], meta['down']))

        # Semeia precos via SDK antes do WS fluir
        await fetch_initial_prices_sdk(meta['up'], meta['down'])

        await asyncio.sleep(1.0)  # aguarda primeiro tick WS

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

                nx_pa_r,  nx_extra_pa,   nx_pa_base   = calc_effective_risk(
                    PEG_ARBIT_RISK, risk_multiplier, bankroll, accumulated_loss
                )
                nx_gb_r,  nx_extra_gamb, nx_gb_base   = calc_effective_risk(
                    GAMBLING_RISK, risk_multiplier, bankroll, accumulated_loss
                )
                cap_p = " [CAP]" if nx_pa_r >= MAX_RISK_PERCENT else ""
                cap_g = " [CAP]" if nx_gb_r >= MAX_RISK_PERCENT else ""

                log_info(
                    f"ROUND | PnL: {pnl_str} | Proximo Mult: x{risk_multiplier:.0f} "
                    f"| Acc_loss: ${accumulated_loss:.4f} "
                    f"| GAMBLING={nx_gb_base:.1%}+${nx_extra_gamb:.3f} (50% acc_loss){cap_g} "
                    f"PEG ARBIT={nx_pa_base:.1%}+${nx_extra_pa:.3f} (50% acc_loss){cap_p} "
                    f"| (cap={MAX_RISK_PERCENT:.0%})"
                )

            elif profit_this > 0.00001:
                # ── LUCRO ──────────────────────────────────────────────────
                prev_accum       = accumulated_loss
                accumulated_loss = max(0.0, accumulated_loss - profit_this)
                recovery_rounds  = max(0, recovery_rounds - 1)
                recovered        = prev_accum - accumulated_loss
                risk_multiplier  = 1.0
                total_pnl_pos   += profit_this

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
                    nx_pa_r,  nx_extra_pa,   nx_pa_base   = calc_effective_risk(
                        PEG_ARBIT_RISK, risk_multiplier, bankroll, accumulated_loss
                    )
                    nx_gb_r,  nx_extra_gamb, nx_gb_base   = calc_effective_risk(
                        GAMBLING_RISK, risk_multiplier, bankroll, accumulated_loss
                    )
                    cap_p = " [CAP]" if nx_pa_r >= MAX_RISK_PERCENT else ""
                    cap_g = " [CAP]" if nx_gb_r >= MAX_RISK_PERCENT else ""
                    log_info(
                        f"ROUND | PnL: $0.0 (0.00%) | Proximo Mult: x{risk_multiplier:.0f} "
                        f"| Acc_loss: ${accumulated_loss:.4f} "
                        f"| GAMBLING={nx_gb_base:.1%}+${nx_extra_gamb:.3f} "
                        f"(50% acc_loss_last_round){cap_g} "
                        f"PEG ARBIT={nx_pa_base:.1%}+${nx_extra_pa:.3f} "
                        f"(50% acc_loss_last_round){cap_p} "
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