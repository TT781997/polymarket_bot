# =============================================================================
# BOT XRP POLYMARKET — v0.36.4
# =============================================================================
# CHANGELOG v0.36.4:
# - Fix: Delta validation corrigida para direção (UP/DOWN sinal contrário)
#   • UP: rejeita delta < 0 (queda indesejada)
#   • DOWN: rejeita delta > 0 (subida indesejada)
#   • Evita rejeição falsa de DOWN quando desce (momentum válido)
# - EIGHTY agora compra DOWN em queda confirmada (delta negativo = esperado)
# - Lógica de momentum alinhada com direção real do mercado
#
# CHANGELOG v0.36.3:
# - Fix: SyntaxError corrigido (parêntese extra no delta_str)
# - Stop-Loss: 100% baseado no BID puro (preço real onde vendes)
# - Proteção leve: só ativa monitor se ASK < 70c (evita wide-spread falso)
# - Sem MID, sem desativação nos últimos 60s
# - Martingale mantido com 100% loss → accum_loss + 50% → extra_stake
# =============================================================================
import asyncio
import websockets
import json
import time
import logging
import requests
import os
import math
from datetime import datetime
from collections import deque
# =============================================================================
# PARÂMETROS CONFIGURÁVEIS (COM RANGES ABSOLUTOS E EXPLICAÇÕES)
# =============================================================================
# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 0 — MODO DE OPERAÇÃO
# ─────────────────────────────────────────────────────────────────────────────
LIVE_TRADING = False # Range: [True | False] | True = Executa ordens reais, False = Simulação completa
# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 1 — BANCA
# ─────────────────────────────────────────────────────────────────────────────
BANKROLL_INIT = 10.0 # Range: [10.0 ... ∞] | Banca inicial em USDC. Em Demo, esta banca é persistente.
# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 2 — RISCO BASE POR MÓDULO
# ─────────────────────────────────────────────────────────────────────────────
RISK_PER_TRADE = 0.05 # Range: [0.01 ... 0.20] | Fração de risco base para ciclos e ordens genéricas.
EIGHTY_RISK = 0.15 # Range: [0.01 ... 0.15] | Fração de risco base específica para o módulo EIGHTY.
PEG_ARBIT_RISK = 0.25 # Range: [0.05 ... 0.30] | Fração de risco base dedicada à Arbitragem PEG.
# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 3 — MARTINGALE E RECOVERY
# ─────────────────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 3.0 — MARTINGALE CONDICIONAL E RECUPERAÇÃO SUAVE
# ─────────────────────────────────────────────────────────────────────────────
MAX_RISK_MULTIPLIER = 32 # Range: [2 ... 32] | Limite máximo do multiplicador (x2, x4, x8, x16, x32).
RECOVERY_ROUNDS_BASE = 10 # Range: [5 ... 20] | Rondas iniciais de recuperação por cada loss.
MAX_RISK_PERCENT = 0.15 # Range: [0.10 ... 0.20] | CAP RÍGIDO: Risco efetivo total máximo = 15% da banca.
# Fórmula: min(base * martingale_mult + (accumulated_loss / recovery_rounds / bankroll), 0.15)
# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 3.1 — STOP-LOSS E BUCKETING
# ─────────────────────────────────────────────────────────────────────────────
STOPLOSS_PRICE_C = 27.0 # Range: [10.0 ... 80.0] | BID efetivo mínimo antes de ativar stop-loss check.
STOPLOSS_TICKS = 5 # Range: [1 ... 20] | Níveis estruturais de descida para confirmar flash-crash.
STOPLOSS_PRICE_STEP_C = 1.0 # Range: [0.1 ... 5.0] | Tamanho do tick (degrau) de stop-loss.
STOPLOSS_MAX_ASK_C = 70.0        # Proteção: só ativa se ASK também < 70c (evita wide-spread falso)
# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 4 — TOGGLES DE MÓDULOS
# ─────────────────────────────────────────────────────────────────────────────
EIGHTY_ACTIVE = True # Range: [True | False] | Compra direcional por consolidação de tick buffers.
PEG_ARBIT_ACTIVE = True # Range: [True | False] | Arbitragem direcional inversa garantindo hedge.
# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 6 — EIGHTY (Sincronizado)
# ─────────────────────────────────────────────────────────────────────────────
EIGHTY_START_REM_S = 300 # Range: [60.0 ... 300.0] | Janela inicial do EIGHTY.
EIGHTY_MIN_EFF_C = 80.0 # Range: [80.0 ... 90.0] | Entry mínimo (Preço efetivo).
EIGHTY_MAX_EFF_C = 98 # Range: [95.0 ... 99.9] | Teto de liquidez para entry EIGHTY.
EIGHTY_MIN_TICKS = 5 # Range: [3 ... 10] | Mínimo de níveis de preço distintos (passos discretos).
EIGHTY_CUTOFF_S = 5 # Range: [0 ... 20] | Congela entradas faltam X segundos.
EIGHTY_WHEN_CUTOFF_0_VOLT = 35.0 # Range: [0.0 ... 60.0] | Ignora vol nos últimos Xs se cutoff=0.
EIGHTY_PEG_MIN_C = 97.0 # Range: [90.0 ... 99.0] | Teto de estabilidade U/D específico.
EIGHTY_BUY_COOLDOWN = 4.0 # Range: [1.0 ... 10.0] | Cool-off obrigatório entre entradas do mesmo lado.
EIGHTY_PRICE_STEP_C = 0.5 # Range: [0.1 ... 2.0] | Tamanho do tick no EIGHTY (arredondamento subida).
# 1. MACRO VOLATILIDADE (Ruído Geral)
EIGHTY_VOL_WINDOW_S = 5.0 # Range: [3.0 ... 10.0] | Janela longa para spread de volatilidade.
EIGHTY_VOL_MAX_C = 4.5 # Range: [2.0 ... 10.0] | Spread máximo permitido (High - Low).
EIGHTY_VOL_COOLDOWN_S = 5.0 # Range: [3.0 ... 10.0] | Tempo de castigo após macro volatilidade.
# 2. MICRO VOLATILIDADE E DELTAS (Proteção Anti-Pump e Exaustão)
EIGHTY_DELTA_LOOKBACK_S = 5.0 # Range: [3.0 ... 10.0] | Buffer size.
EIGHTY_DELTA_INTERVALS = [1.0, 2.0, 3.0] # Range: List[float] | Lookbacks de delta graduais.
EIGHTY_DELTA_VOL_TIME_S = 1.5 # Range: [1.0 ... 3.0] | Janela curta para detecção de pump.
EIGHTY_DELTA_VOL_RISE_C = 4.0 # Range: [1.0 ... 3.0] | Pump falso (subida rápida instável).
EIGHTY_DELTA_MAX_RISE_C = 6.5 # Range: [2.0 ... 5.0] | Exaustão (subida longa demasiada esticada).
EIGHTY_TARGET_C = 0.0 # Range: [0.0 ... 99.0] | Exit percentual estático (0.0 = natural).
# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 7 — PEG ARBITRAGE (NOVO SISTEMA)
# ─────────────────────────────────────────────────────────────────────────────
PEG_ARBIT_EFF_THRESHOLD = 0.985 # Range: [0.01 ... 0.99] | Soma de asks+fees: eff_up + eff_down <= 98.5c para ativar.
PEG_ARBIT_RANGE_1 = (0.0, 45.0) # Range: [0-45c] | Primeiro range válido (fechado).
PEG_ARBIT_RANGE_2 = (55.0, 99.9) # Range: [55-99.9c] | Segundo range válido (fechado).
PEG_ARBIT_BANCA_PCT = 0.25 # Range: [0.10 ... 0.50] | Percentagem fixa de banca por entrada (25%).
PEG_ARBIT_COOLDOWN = 0.05 # Range: [0.01 ... 1.0] | Ratelimit intra-ticks.
PEG_ARBIT_MIN_REM = 0.05 # Range: [0.01 ... 1.0] | Tempo remanescente mínimo (0.05s = 50ms).
MAX_PEG_ENTRIES = 10000000 # Range: [1 ... ∞] | Entradas aceitáveis num ciclo.
PEG_ARBIT_TARGET_C = 0.0 # Range: [0.0 ... 99.0] | Hold até fecho do order-book.
TARGET_MULTIPLIER = 1.25 # Range: [1.0 ... 2.0] | Modificador multiplicativo p/ ciclos.
# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 10 — FEES E SPREAD
# ─────────────────────────────────────────────────────────────────────────────
FEE_RATE = 0.25 # Constante de estrutura de mercado Crypto Polymarket (NÃO ALTERAR)
FEE_EXP = 2 # Exponenciação de curva Crypto Polymarket (NÃO ALTERAR)
ASK_SPREAD = 0.01 # Range: [0.0 ... 1.0] | Simulação de price slippage na liquidez de entrada.
LOOP_SLEEP = 0.001 # Range: [0.0001 ... 0.1] | Wait time intra-evento (assíncrono não-bloqueante).
# ─────────────────────────────────────────────────────────────────────────────
# GLOBAIS DE ESTADO (NÃO ALTERAR)
# ─────────────────────────────────────────────────────────────────────────────
bankroll = BANKROLL_INIT # Banca persistente (nunca reseta em Demo)
daily_profit = 0.0
last_day = None
best_asks = {'up': None, 'down': None}
best_bids = {'up': None, 'down': None}
price_change = asyncio.Event()
bot_start_time = time.time()
# Martingale Condicional e Recuperação
martingale_multiplier = 1.0 # x1, x2, x4, x8, x16, x32
accumulated_loss = 0.0 # Soma de perdas para recuperação
recovery_rounds_remaining = 1 # Rondas para recuperar accumulated_loss
extra_dollar_risk = 0.0 # Adicional em dólares para o risco da próxima ronda (50% da perda)
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

def load_secrets(filepath: str = "secrets.txt") -> dict:
    if not os.path.exists(filepath):
        logger.warning("[WARN] secrets.txt nao encontrado")
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
    logger.error("[ERRO] FATAL: LIVE_TRADING=True mas sem chave!")
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
        clob_client = ClobClient(host="https://clob.polymarket.com", key=POLYMARKET_PRIVATE_KEY, chain_id=137)
        logger.info("[INFO] SDK Polymarket carregado - LIVE TRADING ACTIVO")
    except ImportError:
        logger.error("[ERRO] py-clob-client nao instalado!")
        raise SystemExit(1)

# =============================================================================
# HELPERS
# =============================================================================
def fee_rate(p: float) -> float:
    return FEE_RATE * (p * (1.0 - p)) ** FEE_EXP

def buy_shares_net(invested: float, ask: float) -> float:
    return (invested / ask) * (1.0 - fee_rate(ask))

def effective_entry(ask: float) -> float:
    return ask

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
    m = int(rem // 60)
    s = int(rem % 60)
    ms = int((rem * 1000) % 1000)
    return f"{m:02d}:{s:02d}:{ms:03d}"

def get_uptime_str() -> str:
    elapsed = int(time.time() - bot_start_time)
    y, elapsed = divmod(elapsed, 365*24*3600)
    mo, elapsed = divmod(elapsed, 30*24*3600)
    d, elapsed = divmod(elapsed, 24*3600)
    h, elapsed = divmod(elapsed, 3600)
    mi, s = divmod(elapsed, 60)
    return f"{y}y:{mo:02d}m:{d:02d}d:{h:02d}h:{mi:02d}m:{s:02d}s"

def calc_risk(base, mult, accum, rec, bank, extra):
    if bank <= 0: return MAX_RISK_PERCENT
    rec_bonus = accum / rec / bank if rec > 0 else 0
    extra_bonus = extra / bank
    return min(base * mult + rec_bonus + extra_bonus, MAX_RISK_PERCENT)

def calc_risk_preview(base, mult, accum, rec, bank, extra):
    return calc_risk(base, mult, accum, rec, bank, extra)

def log_m(module, action, msg):
    logger.info(f"[INFO] [{module}] [{action}] [{get_ts()}] | {msg}")

def log_raw(msg):
    logger.info(f"[{get_ts()}] | {msg}")

def log_info(msg):
    logger.info(f"[INFO] [{get_ts()}] | {msg}")

def log_warn(msg):
    logger.warning(f"[WARN] [{get_ts()}] | {msg}")

def log_sep():
    logger.info("-" * 80)

def log_sep2():
    logger.info("=" * 80)

# =============================================================================
# API + WS
# =============================================================================
def fetch_metadata(slug: str):
    try:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        data = requests.get(url, timeout=5).json()[0]['markets'][0]
        ids = json.loads(data['clobTokenIds'])
        return {'id': data['conditionId'], 'up': ids[0], 'down': ids[1], 'slug': slug}
    except Exception as e:
        log_warn(f"fetch_metadata falhou: {e}")
        return None

def get_current_slug():
    now = time.time()
    start_ts = now - (now % 300)
    if (start_ts + 300 - now) < 5:
        start_ts += 300
    return f"xrp-updown-5m-{int(start_ts)}", start_ts

async def ws_handler(t_up, t_down):
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                await ws.send(json.dumps({"assets_ids": [t_up, t_down], "type": "market", "custom_feature_enabled": True}))
                log_info("WS conectado ao order book Polymarket")
                async for raw in ws:
                    items = json.loads(raw)
                    if not isinstance(items, list):
                        items = [items]
                    for item in items:
                        aid = item.get("asset_id")
                        ask_p = bid_p = None
                        evt = item.get("event_type")
                        if evt == "book":
                            if item.get("asks"):
                                valid = [float(d['price']) for d in item["asks"] if float(d['size']) > 0]
                                if valid: ask_p = min(valid)
                            if item.get("bids"):
                                valid = [float(d['price']) for d in item["bids"] if float(d['size']) > 0]
                                if valid: bid_p = max(valid)
                        elif evt == "best_bid_ask":
                            if item.get("best_ask"): ask_p = float(item["best_ask"])
                            if item.get("best_bid"): bid_p = float(item["best_bid"])

                        if ask_p is not None:
                            if aid == t_up: best_asks['up'] = ask_p
                            else: best_asks['down'] = ask_p
                            price_change.set()
                        if bid_p is not None:
                            if aid == t_up: best_bids['up'] = bid_p
                            else: best_bids['down'] = bid_p
                            price_change.set()
        except asyncio.CancelledError:
            break
        except Exception as e:
            log_warn(f"WS erro: {e} - reconectando em 1s")
            await asyncio.sleep(1)

# =============================================================================
# LIVE ORDER
# =============================================================================
async def place_live_order(side: str, price: float, shares: float, token_id: str) -> bool:
    if not clob_client: return False
    try:
        side_const = BUY if side.upper() in ('UP', 'BUY') else SELL
        order_args = OrderArgs(token_id=token_id, price=round(price,4), size=round(shares,6), side=side_const, order_type="GTC")
        response = clob_client.create_and_post_order(order_args)
        log_info(f"LIVE ORDER OK -> {side} @ {price:.4f} | Size: {shares:.4f}")
        return True
    except Exception as e:
        log_warn(f"LIVE ORDER falhou: {e}")
        return False

# =============================================================================
# PRICE BUFFER
# =============================================================================
class PriceBuffer:
    def __init__(self, max_age_seconds: float = 30.0):
        self.max_age = max_age_seconds
        self.buffer = deque()

    def add(self, eff_c: float, ts: float):
        self.buffer.append((ts, eff_c))
        while self.buffer and self.buffer[0][0] < time.time() - self.max_age:
            self.buffer.popleft()

    def get_delta(self, seconds_ago: float):
        if not self.buffer: return None, False
        past = None
        target = time.time() - seconds_ago
        for ts, price in self.buffer:
            if abs(ts - target) < 1.0:
                past = price
                break
        if past is None: return None, False
        return self.buffer[-1][1] - past, True

    def get_age(self):
        return (time.time() - self.buffer[0][0]) if self.buffer else 0.0

# =============================================================================
# LOGIC LOOP
# =============================================================================
async def logic_loop(m_start, m_end, meta, r_mult, accum_loss, rec_rounds, extra_dollar):
    global bankroll, daily_profit
    active_trades = []

    eff_eighty_risk = calc_risk(EIGHTY_RISK, r_mult, accum_loss, rec_rounds, bankroll, extra_dollar)
    eff_peg_risk = calc_risk(PEG_ARBIT_RISK, r_mult, accum_loss, rec_rounds, bankroll, extra_dollar)

    if r_mult > 1.0 or accum_loss > 0 or extra_dollar > 0:
        log_info(f"MARTINGALE | x{r_mult:.0f} | accum_loss=${accum_loss:.4f} | extra=${extra_dollar:.4f}")

    # Estado EIGHTY
    eighty_seen_levels = {'UP': set(), 'DOWN': set()}
    eighty_tick_count = {'UP': 0, 'DOWN': 0}
    eighty_last_buy = {'UP': 0.0, 'DOWN': 0.0}
    eighty_first_tick_t = {'UP': None, 'DOWN': None}
    eighty_eff_min = {'UP': None, 'DOWN': None}
    eighty_eff_max = {'UP': None, 'DOWN': None}
    eighty_cutoff_logged = False
    eighty_started_logged = False
    eighty_price_buffer = {'UP': PriceBuffer(), 'DOWN': PriceBuffer()}
    eighty_vol_cooldown_until = {'UP': 0.0, 'DOWN': 0.0}

    # Stop-Loss
    stoploss_below_levels = {'UP': set(), 'DOWN': set()}
    stoploss_consecutive = {'UP': 0, 'DOWN': 0}
    stoploss_last_price_c = {'UP': None, 'DOWN': None}
    stoploss_monitor_active = {'UP': False, 'DOWN': False}

    peg_arbit_count = 0
    last_peg_time = 0.0

    mult_tag = f" [MARTINGALE x{r_mult:.0f}]" if r_mult > 1.0 else ""
    mods = []
    if EIGHTY_ACTIVE: mods.append(f"EIGHTY({EIGHTY_START_REM_S}s->{EIGHTY_CUTOFF_S}s)")
    if PEG_ARBIT_ACTIVE: mods.append(f"PEG_ARBIT(PEG≤{PEG_ARBIT_EFF_THRESHOLD:.2%} | 25% banca)")
    log_sep2()
    log_info(f"NOVO CICLO | Market: {meta['slug']} | LIVE: {LIVE_TRADING}")
    log_info(f" Banca: ${bankroll:.4f} | Profit acum.: ${daily_profit:.4f}{mult_tag}")
    log_info(f" Modulos: {' | '.join(mods)}")
    log_info(f" Risco efetivo: EIGHTY={eff_eighty_risk:.1%} | PEG={eff_peg_risk:.1%} | CAP={MAX_RISK_PERCENT:.0%}")
    log_sep()
    log_info(" ESCUTA ACTIVA")
    log_sep()

    def pct_banca(invested: float) -> str:
        base = bankroll + invested
        return f"{invested / base * 100:.1f}% banca" if base else "---"

    async def open_trade(side, nom, trade_type, rstr, risk=None, fixed_shares=None, peg_val=None, token_id=None, extra_log=None):
        global bankroll
        if risk is None: risk = eff_eighty_risk
        ask = nom + ASK_SPREAD
        _fee = fee_rate(ask)
        eff = effective_entry(ask)
        if fixed_shares is not None:
            shares = fixed_shares
            invested = shares * ask / (1.0 - _fee)
        else:
            invested = bankroll * risk
            shares = buy_shares_net(invested, ask)
        target = None
        if trade_type == 'EIGHTY':
            target = EIGHTY_TARGET_C / 100.0 if EIGHTY_TARGET_C > 0 else None
        elif trade_type == 'PEG_ARBIT':
            target = PEG_ARBIT_TARGET_C / 100.0 if PEG_ARBIT_TARGET_C > 0 else None
        bankroll -= invested
        pct = pct_banca(invested)
        buy_fee = _fee * 100.0
        peg_str = f" | PEG: {fc(peg_val)} ({peg_val:.3f})" if peg_val is not None else ""
        extra = f" | {extra_log}" if extra_log else ""
        trade = {'side': side, 'nom': nom, 'entry': eff, 'shares': shares, 'target': target,
                 'type': trade_type, 'invested': invested, 'token_id': token_id}
        active_trades.append(trade)
        if LIVE_TRADING and token_id:
            await place_live_order(side, ask, shares, token_id)
        module = trade_type.replace('_', ' ')
        log_m(module, 'BUY', f"rem={rstr} | {side} @ nom={fc(nom)} ask={fc(ask)} eff={fc(eff)}{peg_str} | "
                            f"inv=${invested:.4f} ({pct}) | shares={shares:.4f} | fee={buy_fee:.3f}%{extra}")

    def close_trade(trade, cp, reason, rstr, is_settlement=False):
        global bankroll
        if is_settlement:
            payout = trade['shares'] * cp
        else:
            payout = sell_payout(trade['shares'], cp)
        pnl = payout - trade['invested']
        pnl_pct = (pnl / trade['invested'] * 100.0) if trade['invested'] else 0.0
        bankroll += payout
        icon = "(+)" if pnl >= 0 else "(-)"
        module = trade['type'].replace('_', ' ')
        log_m(module, 'SELL', f"rem={rstr} | {trade['side']} @ {fc(cp)} | PnL: ${pnl:+.4f} ({pnl_pct:+.1f}%) {icon} | Reason: {reason}")

    def eighty_reset(e_side, rstr, reason):
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side] = 0
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side] = None
        eighty_eff_max[e_side] = None
        log_m('EIGHTY', 'RESET', f"rem={rstr} | {e_side} - {reason}")

    def eighty_reset_silent(e_side):
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side] = 0
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side] = None
        eighty_eff_max[e_side] = None

    def eighty_activate_vol_cooldown(e_side, rstr, reason):
        eighty_vol_cooldown_until[e_side] = time.time() + EIGHTY_VOL_COOLDOWN_S
        eighty_reset(e_side, rstr, reason)
        log_m('EIGHTY', 'VOL_COOLDOWN', f"rem={rstr} | {e_side} - bloqueado {EIGHTY_VOL_COOLDOWN_S:.0f}s")

    prev_u_ask = prev_d_ask = prev_u_bid = prev_d_bid = None
    while True:
        now = time.time()
        rem = m_end - now
        if rem <= 0:
            u_ask = best_asks.get('up') or 0.0
            d_ask = best_asks.get('down') or 0.0
            u_bid = best_bids.get('up') or 0.0
            d_bid = best_bids.get('down') or 0.0
            log_sep()
            log_info(f"FIM DE MERCADO | UP final={fc(u_ask)} | DOWN final={fc(d_ask)}")
            winner_side = 'UP' if u_bid > d_bid else 'DOWN'
            for trade in active_trades[:]:
                res_price = 1.0 if trade['side'] == winner_side else 0.0
                res_str = "RESOLUCAO GANHA ($1/share)" if res_price == 1.0 else "RESOLUCAO PERDIDA (Total)"
                close_trade(trade, res_price, res_str, "00:00:000", is_settlement=True)
                active_trades.remove(trade)
            break

        rstr = get_remaining_str(rem)
        try:
            await asyncio.wait_for(price_change.wait(), timeout=LOOP_SLEEP)
            price_change.clear()
        except asyncio.TimeoutError:
            pass

        u_ask = best_asks.get('up')
        d_ask = best_asks.get('down')
        u_bid = best_bids.get('up')
        d_bid = best_bids.get('down')
        if None in (u_ask, d_ask, u_bid, d_bid): continue
        if (u_ask, d_ask, u_bid, d_bid) == (prev_u_ask, prev_d_ask, prev_u_bid, prev_d_bid): continue
        prev_u_ask = u_ask
        prev_d_ask = d_ask
        prev_u_bid = u_bid
        prev_d_bid = d_bid

        ask_up = u_ask + ASK_SPREAD
        ask_down = d_ask + ASK_SPREAD
        eff_up = effective_entry(ask_up)
        eff_down = effective_entry(ask_down)
        peg_eff = eff_up + eff_down
        log_raw(f"rem={rstr} | UP={fc(u_ask)} Eff={fc(eff_up)} | DOWN={fc(d_ask)} Eff={fc(eff_down)} | PEG={peg_eff:.4f}")

        # PEG ARBIT
        if (PEG_ARBIT_ACTIVE and peg_eff <= PEG_ARBIT_EFF_THRESHOLD and rem > PEG_ARBIT_MIN_REM and
                peg_arbit_count < MAX_PEG_ENTRIES and now - last_peg_time >= PEG_ARBIT_COOLDOWN):
            eff_up_c = eff_up * 100.0
            eff_down_c = eff_down * 100.0
            up_in_range = (PEG_ARBIT_RANGE_1[0] <= eff_up_c <= PEG_ARBIT_RANGE_1[1]) or (PEG_ARBIT_RANGE_2[0] <= eff_up_c <= PEG_ARBIT_RANGE_2[1])
            down_in_range = (PEG_ARBIT_RANGE_1[0] <= eff_down_c <= PEG_ARBIT_RANGE_1[1]) or (PEG_ARBIT_RANGE_2[0] <= eff_down_c <= PEG_ARBIT_RANGE_2[1])
            if up_in_range and down_in_range:
                budget = bankroll * PEG_ARBIT_BANCA_PCT
                ref_eff = max(eff_up, eff_down)
                shares_to_buy = budget / ref_eff
                invest_up = shares_to_buy * eff_up
                invest_down = shares_to_buy * eff_down
                total_invest = invest_up + invest_down
                margin = (1.0 - peg_eff) * 100.0
                log_sep()
                log_m('PEG ARBIT', 'ENTRADA', f"rem={rstr} | PEG={peg_eff:.4f} (margin {margin:.2f}c) | "
                    f"UP={fc(eff_up_c/100)} DOWN={fc(eff_down_c/100)} | Shares={shares_to_buy:.4f} | "
                    f"Total=${total_invest:.4f} (25% banca) | arb #{peg_arbit_count + 1}")
                await open_trade('UP', u_ask, 'PEG_ARBIT', rstr, fixed_shares=shares_to_buy, peg_val=peg_eff, token_id=meta['up'])
                await open_trade('DOWN', d_ask, 'PEG_ARBIT', rstr, fixed_shares=shares_to_buy, peg_val=peg_eff, token_id=meta['down'])
                log_sep()
                peg_arbit_count += 1
                last_peg_time = now
            else:
                if peg_arbit_count == 0:
                    reasons = []
                    if not up_in_range: reasons.append(f"UP {eff_up_c:.1f}c fora ranges")
                    if not down_in_range: reasons.append(f"DOWN {eff_down_c:.1f}c fora ranges")
                    log_m('PEG ARBIT', 'SKIP', f"rem={rstr} | PEG OK mas {' | '.join(reasons)}")

        # STOP-LOSS — BID PURO
        for trade in active_trades[:]:
            s_side = trade['side']
            ask_price = u_ask if s_side == 'UP' else d_ask
            bid_price = u_bid if s_side == 'UP' else d_bid
            cp = bid_price
            bid_eff_c = eff_sell_price(cp) * 100.0

            if trade.get('target') is not None and cp >= trade['target']:
                close_trade(trade, cp, "TARGET", rstr)
                active_trades.remove(trade)
                continue

            # Proteção contra spread enorme
            if ask_price * 100.0 > STOPLOSS_MAX_ASK_C:
                if stoploss_monitor_active[s_side]:
                    stoploss_reset(s_side)
                    log_m('STOPLOSS', 'RESET', f"rem={rstr} | {s_side} - ASK alto ({ask_price*100:.1f}c) → spread enorme, reset")
                continue

            if bid_eff_c < STOPLOSS_PRICE_C:
                if not stoploss_monitor_active[s_side]:
                    stoploss_monitor_active[s_side] = True
                    stoploss_last_price_c[s_side] = bid_eff_c
                    stoploss_below_levels[s_side].add(round(bid_eff_c / STOPLOSS_PRICE_STEP_C) * STOPLOSS_PRICE_STEP_C)
                    stoploss_consecutive[s_side] = 1
                    log_m('STOPLOSS', 'MONITOR',
                        f"rem={rstr} | {s_side} iniciado @ BID={bid_eff_c:.1f}c (ASK={ask_price*100:.1f}c) < {STOPLOSS_PRICE_C:.1f}c")
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
                        log_m('STOPLOSS', 'RESET', f"rem={rstr} | {s_side} - BID subiu ({bid_eff_c:.1f}c)")
            else:
                if stoploss_monitor_active[s_side]:
                    stoploss_reset(s_side)
                    log_m('STOPLOSS', 'RESET', f"rem={rstr} | {s_side} - BID acima {STOPLOSS_PRICE_C:.1f}c ({bid_eff_c:.1f}c)")

        def stoploss_reset(s_side: str):
            stoploss_below_levels[s_side].clear()
            stoploss_consecutive[s_side] = 0
            stoploss_last_price_c[s_side] = None
            stoploss_monitor_active[s_side] = False

        # EIGHTY
        if EIGHTY_ACTIVE:
            if rem > EIGHTY_START_REM_S:
                pass
            elif rem <= EIGHTY_CUTOFF_S:
                if not eighty_cutoff_logged:
                    eighty_cutoff_logged = True
                    log_m('EIGHTY', 'CUTOFF', f"rem={rstr} | EIGHTY parado - rem <= {EIGHTY_CUTOFF_S}s")
            else:
                if not eighty_started_logged:
                    eighty_started_logged = True
                    log_m('EIGHTY', 'START', f"rem={rstr} | EIGHTY activo | risco={eff_eighty_risk:.1%}")
                for e_side, nom in (('UP', u_ask), ('DOWN', d_ask)):
                    token_id = meta['up'] if e_side == 'UP' else meta['down']
                    skip_vol = (EIGHTY_CUTOFF_S == 0 and EIGHTY_WHEN_CUTOFF_0_VOLT > 0 and rem <= EIGHTY_WHEN_CUTOFF_0_VOLT)
                    if not skip_vol and now < eighty_vol_cooldown_until[e_side]: continue
                    if not skip_vol and now - eighty_last_buy[e_side] < EIGHTY_BUY_COOLDOWN: continue
                    ask = nom + ASK_SPREAD
                    _fee = fee_rate(ask)
                    eff_c = effective_entry(ask) * 100.0
                    eighty_price_buffer[e_side].add(eff_c, now)
                    if not (EIGHTY_MIN_EFF_C <= eff_c <= EIGHTY_MAX_EFF_C):
                        if eighty_tick_count[e_side] > 0:
                            eighty_reset(e_side, rstr, f"eff_c {eff_c:.1f}c fora range")
                        continue
                    level_key = math.ceil(eff_c / EIGHTY_PRICE_STEP_C) * EIGHTY_PRICE_STEP_C
                    if level_key not in eighty_seen_levels[e_side]:
                        eighty_seen_levels[e_side].add(level_key)
                        eighty_tick_count[e_side] += 1
                    if eighty_first_tick_t[e_side] is None:
                        eighty_first_tick_t[e_side] = now
                        eighty_eff_min[e_side] = eighty_eff_max[e_side] = eff_c
                    else:
                        if eff_c < eighty_eff_min[e_side]: eighty_eff_min[e_side] = eff_c
                        if eff_c > eighty_eff_max[e_side]: eighty_eff_max[e_side] = eff_c
                    elapsed = now - eighty_first_tick_t[e_side]
                    var_c = eighty_eff_max[e_side] - eighty_eff_min[e_side]
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
                    rapid_rise = valid_15 and delta_15 is not None and delta_15 >= EIGHTY_DELTA_VOL_RISE_C
                    
                    # ✓ NOVA LÓGICA: direção-aware
                    direcao = 1.0 if e_side == 'UP' else -1.0  # +1 para UP, -1 para DOWN
                    
                    delta_ok = True
                    delta_reason = ""
                    has_delta = valid_10 or valid_20 or valid_30
                    if valid_10 and delta_10 is not None and (delta_10 * direcao) < 0:
                        delta_ok, delta_reason = False, f"D1s={delta_10:+.1f}c (contra-trend)"
                    elif valid_20 and delta_20 is not None and (delta_20 * direcao) < 0:
                        delta_ok, delta_reason = False, f"D2s={delta_20:+.1f}c (contra-trend)"
                    elif valid_30 and delta_30 is not None and (delta_30 * direcao) < 0:
                        delta_ok, delta_reason = False, f"D3s={delta_30:+.1f}c (contra-trend)"
                    elif rapid_rise:
                        delta_ok, delta_reason = False, f"D{EIGHTY_DELTA_VOL_TIME_S}s={delta_15:+.1f}c (pump rapido)"
                    elif valid_30 and delta_30 is not None and (delta_30 * direcao) >= EIGHTY_DELTA_MAX_RISE_C:
                        delta_ok, delta_reason = False, f"D3s={delta_30:+.1f}c (exaustao)"
                    
                    vol_str = "VOL SKIP" if skip_vol else f"VOL {'NOK' if vol_nok else 'OK'} ({var_c:.1f}c/{elapsed:.1f}s)"
                    delta_icon = "UP" if (delta_ok and has_delta) else ("DOWN" if has_delta else "WAIT")
                    peg_str = f" | PEG={peg_eff:.3f}" if peg_eff * 100.0 <= EIGHTY_PEG_MIN_C else ""
                    log_m('EIGHTY', 'WATCH',
                        f"rem={rstr} | {e_side} Eff={fc(eff_c/100)} | {vol_str} | {delta_str} ({delta_icon}){peg_str} | "
                        f"ticks={eighty_tick_count[e_side]}/{EIGHTY_MIN_TICKS}")
                    if not skip_vol:
                        if vol_nok:
                            eighty_activate_vol_cooldown(e_side, rstr, f"VOL {var_c:.1f}c em {elapsed:.1f}s")
                            continue
                        if rapid_rise:
                            eighty_activate_vol_cooldown(e_side, rstr, f"PUMP RAPIDO")
                            continue
                    if eighty_tick_count[e_side] >= EIGHTY_MIN_TICKS:
                        if peg_eff * 100.0 < EIGHTY_PEG_MIN_C:
                            eighty_reset(e_side, rstr, f"PEG baixo")
                            continue
                        if has_delta and not delta_ok:
                            eighty_reset(e_side, rstr, f"DELTA NOK - {delta_reason}")
                            continue
                        if bankroll > 0:
                            await open_trade(e_side, nom, 'EIGHTY', rstr, risk=eff_eighty_risk,
                                            peg_val=peg_eff, token_id=token_id,
                                            extra_log=f"ticks={eighty_tick_count[e_side]} | {delta_str}")
                            eighty_last_buy[e_side] = now
                            eighty_reset_silent(e_side)
            log_m('EIGHTY', 'COOLDOWN', f"rem={rstr} | {e_side} - cooldown {EIGHTY_BUY_COOLDOWN:.1f}s")
# =============================================================================
# MAIN
# =============================================================================
async def main():
    global daily_profit, last_day, bankroll, martingale_multiplier, accumulated_loss, recovery_rounds_remaining, extra_dollar_risk
    martingale_multiplier = 1.0
    accumulated_loss = 0.0
    recovery_rounds_remaining = 1
    extra_dollar_risk = 0.0
    log_sep2()
    log_info("BOT XRP POLYMARKET v0.36.3 INICIADO")
    log_sep()
    log_info(f" LIVE_TRADING : {LIVE_TRADING}")
    log_info(f" BANKROLL_INIT : ${BANKROLL_INIT:.2f}")
    log_sep()
    log_info(" RISCO BASE:")
    log_info(f" EIGHTY_RISK : {EIGHTY_RISK:.0%}")
    log_info(f" PEG_ARBIT_RISK : {PEG_ARBIT_RISK:.0%}")
    log_sep()
    log_info(" MARTINGALE CONDICIONAL + RECUPERAÇÃO:")
    log_info(f" MAX_MULTIPLIER : x{MAX_RISK_MULTIPLIER}")
    log_info(f" RECOVERY_ROUNDS : {RECOVERY_ROUNDS_BASE} base")
    log_info(f" MAX_RISK CAP : {MAX_RISK_PERCENT:.0%} (RÍGIDO)")
    log_info(" Regras:")
    log_info(" - PnL < 0: mult x2 | +10 rounds | acc_loss += 100% loss | extra_stake += 50% loss")
    log_info(" - PnL = 0: mult mantém | estado intacto")
    log_info(" - PnL > 0: mult = x1 | acc_loss -= profit | extra_stake -= profit | -1 round")
    log_sep()
    log_info(" MODULOS:")
    log_info(f" EIGHTY : {'ON' if EIGHTY_ACTIVE else 'OFF'}")
    log_info(f" PEG_ARBIT : {'ON' if PEG_ARBIT_ACTIVE else 'OFF'}")
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
            if LIVE_TRADING and clob_client:
                try:
                    wallet_addr = clob_client.get_address()
                    resp_balance = clob_client.get_balance(wallet_addr)
                    if resp_balance:
                        new_balance = float(resp_balance)
                        log_info(f"Saldo Polymarket LIDO: ${new_balance:.4f}")
                        bankroll = new_balance
                except Exception as e:
                    log_warn(f"Falha ao ler saldo: {e}")
                    bankroll = BANKROLL_INIT
            else:
                if last_day is None:
                    bankroll = BANKROLL_INIT
            martingale_multiplier = 1.0
            accumulated_loss = 0.0
            recovery_rounds_remaining = 1
            extra_dollar_risk = 0.0
            last_day = market_day
            log_sep2()
            log_info(f"NOVO DIA {market_day}")
            log_info(f" Banca: ${bankroll:.4f}")
            log_info(f" Martingale : x1 | acc_loss=$0.0000 | extra_stake=$0.0000 | rounds=1")
            log_sep2()

        best_asks['up'] = best_asks['down'] = None
        best_bids['up'] = best_bids['down'] = None
        price_change.clear()
        ws_task = asyncio.create_task(ws_handler(meta['up'], meta['down']))
        await asyncio.sleep(1.0)
        if best_asks['up'] is not None:
            pre_bank = bankroll
            await logic_loop(start_ts, start_ts + 300, meta, martingale_multiplier,
                             accumulated_loss, recovery_rounds_remaining, extra_dollar_risk)
            profit_this = bankroll - pre_bank
            daily_profit += profit_this
            pnl_pct = (profit_this / pre_bank * 100.0) if pre_bank > 0 else 0.0
            if profit_this == 0.0:
                pnl_str = "PnL: $0.0000 (0.00%)"
            else:
                pnl_str = f"PnL: ${profit_this:+.4f} ({pnl_pct:+.2f}%)"
            log_sep2()
            if profit_this < 0:
                loss = abs(profit_this)
                accumulated_loss += loss
                extra_dollar_risk += loss * 0.5
                martingale_multiplier = min(martingale_multiplier * 2.0, float(MAX_RISK_MULTIPLIER))
                recovery_rounds_remaining += RECOVERY_ROUNDS_BASE
                next_eighty = calc_risk_preview(EIGHTY_RISK, martingale_multiplier, accumulated_loss, recovery_rounds_remaining, bankroll, extra_dollar_risk)
                next_peg = calc_risk_preview(PEG_ARBIT_RISK, martingale_multiplier, accumulated_loss, recovery_rounds_remaining, bankroll, extra_dollar_risk)
                log_info(f"MARTINGALE CONDICIONAL | PnL < 0 (Loss) | Mult x{martingale_multiplier:.0f} | "
                         f"Acc_loss +${loss:.4f} | Extra_stake +${loss*0.5:.4f} | Proximo risco EIGHTY={next_eighty:.1%} PEG={next_peg:.1%}")
                log_info(f"ROUND | {pnl_str}")
            elif profit_this == 0.0:
                log_info(f"MARTINGALE CONDICIONAL | PnL = 0 (Neutro) | Mult x{martingale_multiplier:.0f} mantido")
                log_info(f"ROUND | {pnl_str}")
            elif profit_this > 0:
                prev_accum = accumulated_loss
                accumulated_loss = max(0.0, accumulated_loss - profit_this)
                prev_extra = extra_dollar_risk
                extra_dollar_risk = max(0.0, extra_dollar_risk - profit_this)
                martingale_multiplier = 1.0
                recovery_rounds_remaining = max(1, recovery_rounds_remaining - 1)
                log_info(f"MARTINGALE CONDICIONAL | PnL > 0 (Green) | Mult reset x1 | "
                         f"RECUPERAÇÃO ACC ${prev_accum - accumulated_loss:.4f} | EXTRA ${prev_extra - extra_dollar_risk:.4f}")
                log_info(f"ROUND | {pnl_str}")
            log_info(f"TOTAL | PnL: ${daily_profit:+.4f} | Banca: ${bankroll:.4f} | Accumul.Loss: ${accumulated_loss:.4f} | "
                     f"Extra Stake: ${extra_dollar_risk:.4f} | Mult: x{martingale_multiplier:.0f} | Uptime: {get_uptime_str()}")
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