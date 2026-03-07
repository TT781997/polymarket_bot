# =============================================================================
# BOT XRP POLYMARKET — v0.30.5
# =============================================================================
# [v0.30.5] [fix] EIGHTY_DELTA_LOOKBACK_S não estava definido nos parâmetros → NameError
# [v0.30.5] [fix] TARGET CHECK bloqueado por wait_close=True → trades nunca vendiam ao atingir target
# [v0.30.5] [feat] EIGHTY_WHEN_CUTOFF_0_VOLT: quando EIGHTY_CUTOFF_S=0, ignora volatilidade nos últimos N segundos
# [v0.30.5] [feat] Em modo skip_vol (rem <= EIGHTY_WHEN_CUTOFF_0_VOLT): ignora vol cooldown, buy cooldown, VOL NOK e RAPID RISE
# [v0.30.5] [feat] Em modo skip_vol: só verifica range, ticks, PEG e delta (está a subir?)
# -----------------------------------------------------------------------------
# [v0.30.4] EIGHTY: Adicionado Delta de Preço — só compra se preço actual > preço de há 2s
# [v0.30.4] EIGHTY: Buffer circular de preços para tracking histórico (EIGHTY_DELTA_LOOKBACK_S)
# [v0.30.4] EIGHTY: Filtro de volatilidade aumentado de 4.0c/1.5s para 6.0c/10s
# [v0.30.4] EIGHTY: Cooldown de volatilidade — se disparar, bloqueia compra por 10s
# [v0.30.4] EIGHTY: Cooldown pós-compra aumentado para 10s (anti-stacking rápido)
# [v0.30.4] EIGHTY: Log detalhado do delta (preço actual vs preço histórico)
# -----------------------------------------------------------------------------
# [v0.30.0] [fix] CRÍTICO: logger era local em main() mas usado em funções de módulo → NameError
# [v0.30.0] [fix] CRÍTICO: Módulos Eighty/Ciclos estavam comentados — nunca executavam
# [v0.30.0] [fix] CRÍTICO: open_trade() chamava asyncio.create_task() sem await do resultado
# [v0.30.0] [fix] place_live_order() agora await-ada corretamente via wrapper async
# [v0.30.0] [fix] Martingale sem cap → risk_multiplier podia crescer infinitamente e explodir banca
# [v0.30.0] [fix] Bankroll não resetava no início do dia (só daily_profit) → banca inconsistente
# [v0.30.0] [fix] daily_lucro era redundante com daily_profit (removido)
# [v0.30.0] [fix] price_change podia ser None quando ws_handler o usa num race condition estreito
# [v0.30.0] [fix] eighty_last_buy era float partilhado entre sides → cooldown per-side
# [v0.30.0] [fix] Eighty comprava sem validar ticks durante cooldown
# [v0.30.0] [fix] Vol check: sliding window expirava entre ticks lentos → min/max desde 1º tick
# [v0.30.0] [fix] PEG NOK era silencioso — agora faz RESET com motivo
# [v0.30.0] [fix] RESET após BUY era logado desnecessariamente — agora silencioso
# [v0.30.0] [fix] Adicionado EIGHTY_TARGET_C em parâmetros (estava a usar TARGET_MULTIPLIER genérico)
# [v0.30.0] [log] Formato unificado: [INFO] [MODULE] [ACTION] [ts] | msg
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
# ======================== PARÂMETROS CONFIGURÁVEIS ===========================
# =============================================================================

LIVE_TRADING = False        # Se True, executa ordens REAIS no Polymarket. Manter False até testes completos.

# ── Banca e Risco ────────────────────────────────────────────────────────────
BANKROLL_INIT       = 25.0  # Banca inicial em USDC. Reseta todos os dias a este valor.
RISK_PER_TRADE      = 0.10  # % da banca arriscada por trade normal (0.10 = 10%). Base dos Ciclos.
MAX_RISK_MULTIPLIER = 16.0   # Cap máximo do martingale. Após N perdas seguidas, o multiplicador nunca passa deste valor (evita falência).

# ── Toggles ──────────────────────────────────────────────────────────────────
CICLO_30S_ACTIVE = False     # Ativa/desativa o módulo Ciclo 30s (entrada ~30s antes do fim).
CICLO_20S_ACTIVE = False     # Ativa/desativa o módulo Ciclo 20s (entrada ~20s antes do fim).
EIGHTY_ACTIVE    = True     # Ativa/desativa o módulo Eighty (entradas quando preço converge para ~90c+).
PEG_ARBIT_ACTIVE = True     # Ativa/desativa o módulo PEG Arbitrage (compra ambos os lados quando PEG < 0.90).

# ── Ciclos ───────────────────────────────────────────────────────────────────
CYCLE_PRICE_MIN_C       = 75.0  # Preço mínimo em cents para permitir entrada nos Ciclos. Abaixo disto ignora.
CYCLE_PRICE_MAX_C       = 85.0  # Preço máximo em cents para permitir entrada nos Ciclos. Acima disto ignora.
CYCLE_PEG_MIN_C         = 98.0  # PEG mínimo em cents (UP+DOWN) para permitir entrada nos Ciclos. Garante mercado equilibrado.
CYCLE_VOL_MAX_C         = 50.0  # Variação máxima de preço (em cents) permitida entre snapshot e check de volume. Filtra mercados voláteis.
CYCLE_TARGET_C          = 95    # Preço alvo de saída em cents. Se 0, segura até ao fim do mercado (hold-to-end).

CYCLE_30S_SNAPSHOT_REM  = 35.0  # Segundos restantes em que é tirado o snapshot de preço para o Ciclo 30s.
CYCLE_30S_VOL_CHECK_REM = 30.0  # Segundos restantes em que é verificada a volatilidade desde o snapshot (Ciclo 30s).
CYCLE_30S_BUY_REM       = 29.8  # Segundos restantes em que é executada a entrada do Ciclo 30s (logo após o check).

CYCLE_20S_SNAPSHOT_REM  = 25.0  # Segundos restantes em que é tirado o snapshot de preço para o Ciclo 20s.
CYCLE_20S_VOL_CHECK_REM = 20.0  # Segundos restantes em que é verificada a volatilidade desde o snapshot (Ciclo 20s).
CYCLE_20S_BUY_REM       = 19.8  # Segundos restantes em que é executada a entrada do Ciclo 20s (logo após o check).

# ── Eighty ───────────────────────────────────────────────────────────────────
EIGHTY_MIN_EFF_C        = 75    # Preço efectivo mínimo em cents para o Eighty considerar uma entrada.
EIGHTY_MAX_EFF_C        = 99    # Preço efectivo máximo em cents para o Eighty considerar uma entrada.
EIGHTY_MIN_TICKS        = 6     # Número mínimo de níveis de preço distintos visitados antes de entrar (confirma consolidação).
EIGHTY_RISK             = 0.10  # % da banca arriscada por trade do módulo Eighty (independente de RISK_PER_TRADE).
EIGHTY_CUTOFF_S         = 0    # Segundos restantes abaixo dos quais o Eighty para de fazer novas entradas.
EIGHTY_WHEN_CUTOFF_0_VOLT = 20  # Só activo quando EIGHTY_CUTOFF_S=0. Nos últimos N segundos ignora volatilidade e só verifica se o preço está a subir (delta OK).
EIGHTY_PEG_MIN          = 0.98  # PEG mínimo (UP+DOWN) para o Eighty entrar. Abaixo disto o mercado está desequilibrado.
EIGHTY_BUY_COOLDOWN     = 5.0  # Tempo mínimo em segundos entre compras consecutivas do mesmo side (anti-stacking).
EIGHTY_VOL_WINDOW_S     = 6.0   # Janela de tempo em segundos para verificação de volatilidade (max-min).
EIGHTY_VOL_MAX_C        = 6.0   # Variação máxima de eff_c (max-min) permitida dentro da janela.
EIGHTY_VOL_COOLDOWN_S   = 5.0  # Cooldown após volatilidade detectada — bloqueia compra por este tempo.
EIGHTY_TARGET_C         = 0    # Preço alvo de saída em cents para o Eighty. 0 = hold-to-end.

# [v0.30.4] Delta Multi-Timeframe
EIGHTY_DELTA_INTERVALS  = [0.5, 1.0, 2.0]  # Intervalos em segundos para comparação de delta.
EIGHTY_DELTA_LOOKBACK_S = 2.0              # [v0.30.5 FIX] Lookback máximo do buffer de delta (= max dos EIGHTY_DELTA_INTERVALS).
EIGHTY_DELTA_MAX_RISE_C = 4.0              # Máximo de subida permitida em 2s (acima = volatilidade saudável ainda OK).
EIGHTY_DELTA_VOL_RISE_C = 4.0              # Se subir >= 4c em 1.5s, é volatilidade (bloqueia).
EIGHTY_DELTA_VOL_TIME_S = 1.5              # Janela para detectar subida rápida (volatilidade).

# ── PEG Arbitrage ────────────────────────────────────────────────────────────
PEG_ARBIT_UNDERPEG_C = 10.0 # Desvio mínimo do PEG em cents para activar o PEG Arbitrage (ex: 10c = PEG < 0.90).
PEG_ARBIT_RISK       = 0.15 # % da banca investida em cada leg (UP e DOWN) do PEG Arbitrage.
PEG_ARBIT_COOLDOWN   = 0.1  # Tempo mínimo em segundos entre entradas consecutivas do PEG Arbitrage.
PEG_ARBIT_MIN_REM    = 9.5  # Segundos restantes mínimos para o PEG Arbitrage ainda poder entrar. Abaixo ignora.
MAX_PEG_ENTRIES      = 5    # Número máximo de entradas PEG Arbitrage por ciclo de mercado (5 min).

# ── Fee / Spread / Performance ───────────────────────────────────────────────
FEE_RATE          = 0.25    # Taxa base de fee do Polymarket usada nos cálculos (0.25 = 25% aplicado à fórmula quadrática).
FEE_EXP           = 2       # Expoente da fórmula de fee: fee = FEE_RATE * (p*(1-p))^FEE_EXP. Controla a curvatura.
ASK_SPREAD        = 0.01    # Spread adicionado ao preço nominal para simular o ask real (1 cent de margem de segurança).
TARGET_MULTIPLIER = 1.10    # Multiplicador sobre o preço de entrada efectivo para definir o target de saída (1.10 = +10%). Usado pelo PEG_ARBIT.
LOOP_SLEEP        = 0.003   # Tempo máximo de espera em segundos por novo tick de preço no loop principal (3ms).

# ── Globais de estado ────────────────────────────────────────────────────────
bankroll        = BANKROLL_INIT              # Banca actual em USDC. Actualizada a cada trade e reset diário.
daily_profit    = 0.0                        # PnL acumulado do dia corrente em USDC.
last_day        = None                       # Data do último ciclo processado. Usada para detectar virada de dia e fazer reset.
best_asks       = {'up': None, 'down': None} # Melhor ask actual para cada lado (UP/DOWN), actualizado pelo WebSocket.
price_change    = asyncio.Event()            # Event assíncrono disparado sempre que chega um novo preço via WebSocket.
risk_multiplier = 1.0                        # Multiplicador de risco actual do martingale. Dobra a cada perda, reseta a 1.0 com lucro.
bot_start_time  = time.time()                # Timestamp de arranque do bot. Usado para calcular uptime.

# =============================================================================
# ========================== LOGGER (módulo-nível) ============================
# =============================================================================

logging.basicConfig(
    filename='bot_xrp.log',
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# =============================================================================
# ========================== CARREGAR SECRETS =================================
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

def fee_rate(p: float) -> float:
    return FEE_RATE * (p * (1 - p)) ** FEE_EXP

def buy_shares_net(invested: float, ask: float) -> float:
    return (invested / ask) * (1 - fee_rate(ask))

def effective_entry(ask: float) -> float:
    return ask / (1 - fee_rate(ask))

def sell_payout(shares: float, p: float) -> float:
    return shares * p * (1 - fee_rate(p))

def eff_sell_price(cp: float) -> float:
    return cp * (1 - fee_rate(cp))

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

def log_m(module: str, action: str, msg: str):
    """[INFO] [MODULE] [ACTION] [ts] | msg"""
    logger.info(f"[INFO] [{module}] [{action}] [{get_ts()}] | {msg}")

def log_raw(msg: str):
    """[ts] | msg  — tick de preço, sem módulo"""
    logger.info(f"[{get_ts()}] | {msg}")

def log_info(msg: str):
    """[INFO] [ts] | msg  — informação geral"""
    logger.info(f"[INFO] [{get_ts()}] | {msg}")

def log_sep():
    logger.info("=" * 73)

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
                        if evt == "book" and item.get("asks"):
                            valid = [float(d['price']) for d in item["asks"] if float(d['size']) > 0]
                            if valid:
                                p = min(valid)
                        elif evt == "best_bid_ask" and item.get("best_ask"):
                            p = float(item["best_ask"])
                        if p is not None:
                            if   aid == t_up:   best_asks['up']   = p
                            elif aid == t_down: best_asks['down'] = p
                            price_change.set()
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
    Buffer circular para guardar histórico de preços com timestamps.
    Permite consultar o preço de há N segundos atrás.
    """
    def __init__(self, max_age_seconds: float = 30.0):
        self.max_age = max_age_seconds
        self.buffer: deque = deque()  # [(timestamp, eff_c), ...]

    def add(self, eff_c: float, ts: float = None):
        if ts is None:
            ts = time.time()
        self.buffer.append((ts, eff_c))
        self._cleanup(ts)

    def _cleanup(self, now: float):
        cutoff = now - self.max_age
        while self.buffer and self.buffer[0][0] < cutoff:
            self.buffer.popleft()

    def get_price_at(self, seconds_ago: float, tolerance: float = 1.0) -> float | None:
        if not self.buffer:
            return None
        now = time.time()
        target_ts = now - seconds_ago
        best_price = None
        best_diff = float('inf')
        for ts, eff_c in self.buffer:
            diff = abs(ts - target_ts)
            if diff < best_diff and diff <= tolerance:
                best_diff = diff
                best_price = eff_c
        return best_price

    def get_oldest_price(self) -> tuple[float, float] | None:
        return self.buffer[0] if self.buffer else None

    def get_newest_price(self) -> tuple[float, float] | None:
        return self.buffer[-1] if self.buffer else None

    def get_age(self) -> float:
        if not self.buffer:
            return 0.0
        return time.time() - self.buffer[0][0]

    def get_delta(self, seconds_ago: float) -> tuple[float | None, bool]:
        if not self.buffer:
            return None, False
        current_price = self.buffer[-1][1]
        past_price = self.get_price_at(seconds_ago)
        if past_price is None:
            return None, False
        return current_price - past_price, True

    def clear(self):
        self.buffer.clear()

# =============================================================================
# ========================== LOGIC LOOP =======================================
# =============================================================================

async def logic_loop(m_start: float, m_end: float, meta: dict, r_mult: float):
    global bankroll, daily_profit

    active_trades = []
    state         = {'c1': {}, 'c2': {}}
    flags         = {
        's35': False, 'v30': False, 'd29': False,
        's25': False, 'v20': False, 'd19': False
    }

    eff_risk_per_trade = RISK_PER_TRADE * r_mult
    eff_eighty_risk    = EIGHTY_RISK    * r_mult
    eff_peg_risk       = PEG_ARBIT_RISK * r_mult

    # ── Eighty tracking ──────────────────────────────────────────────────────
    eighty_seen_levels        = {'UP': set(), 'DOWN': set()}
    eighty_last_eff_c         = {'UP': None,  'DOWN': None}
    eighty_tick_count         = {'UP': 0,     'DOWN': 0}
    eighty_last_buy           = {'UP': 0.0,   'DOWN': 0.0}
    eighty_first_tick_t       = {'UP': None,  'DOWN': None}
    eighty_eff_min            = {'UP': None,  'DOWN': None}
    eighty_eff_max            = {'UP': None,  'DOWN': None}
    eighty_cutoff_logged      = False
    eighty_price_buffer       = {'UP': PriceBuffer(max_age_seconds=15.0),
                                  'DOWN': PriceBuffer(max_age_seconds=15.0)}
    eighty_vol_cooldown_until = {'UP': 0.0, 'DOWN': 0.0}

    peg_arbit_count = 0
    last_peg_time   = 0.0

    # ── Header ───────────────────────────────────────────────────────────────
    mult_tag = f" [MARTINGALE x{r_mult:.0f}]" if r_mult > 1.0 else ""
    mods = []
    if EIGHTY_ACTIVE:    mods.append(f"EIGHTY(cutoff {EIGHTY_CUTOFF_S}s, delta {EIGHTY_DELTA_LOOKBACK_S}s)")
    if CICLO_30S_ACTIVE: mods.append("CICLO_30S")
    if CICLO_20S_ACTIVE: mods.append("CICLO_20S")
    if PEG_ARBIT_ACTIVE: mods.append("PEG_ARBIT")

    log_sep()
    log_info(f"Market: {meta['slug']} | LIVE_TRADING: {LIVE_TRADING}")
    log_info(f"UP: {meta['up']} | DW: {meta['down']}")
    log_info(f"Bank: ${bankroll:.2f} | Profit Acumulado: ${daily_profit:.2f}{mult_tag}")
    log_info(f"Módulos: {' | '.join(mods)}")
    log_info(f"EIGHTY: Delta={EIGHTY_DELTA_LOOKBACK_S}s | Vol={EIGHTY_VOL_MAX_C}c/{EIGHTY_VOL_WINDOW_S}s | Cooldowns: buy={EIGHTY_BUY_COOLDOWN}s vol={EIGHTY_VOL_COOLDOWN_S}s")
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
        ask      = nom + ASK_SPREAD
        eff      = effective_entry(ask)
        invested = fixed_invest if fixed_invest is not None else (bankroll * risk)
        shares   = buy_shares_net(invested, ask)

        if trade_type.startswith('CICLO'):
            target = min(0.99, CYCLE_TARGET_C / 100.0) if CYCLE_TARGET_C > 0 else None
        elif trade_type == 'EIGHTY':
            target = min(0.99, EIGHTY_TARGET_C / 100.0) if EIGHTY_TARGET_C > 0 else None
        else:
            target = min(0.99, eff * TARGET_MULTIPLIER)

        bankroll -= invested
        pct      = pct_banca(invested)
        buy_fee  = fee_rate(ask) * 100
        peg_str  = f" | *** PEG: {peg_val:.3f} ***" if (peg_val is not None and peg_val <= EIGHTY_PEG_MIN) else ""
        extra    = f" | {extra_log}" if extra_log else ""

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
            f"{peg_str} | Inv: ${invested:.2f} ({pct}) | Shares: {shares:.4f} "
            f"| Fee: {buy_fee:.2f}%{extra}"
        )

    # ── close_trade ───────────────────────────────────────────────────────────
    def close_trade(trade: dict, cp: float, reason: str, rstr: str):
        global bankroll
        payout    = sell_payout(trade['shares'], cp)
        pnl       = payout - trade['invested']
        bankroll += payout
        module    = trade['type'].replace('_', ' ')
        log_m(module, 'SELL',
            f"Remaining: {rstr} | {trade['side']} @ {fc(cp)} "
            f"| PnL: ${pnl:+.4f} | Reason: {reason}"
        )

    # ── eighty_reset (com log) ────────────────────────────────────────────────
    def eighty_reset(e_side: str, rstr: str, reason: str):
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side]   = 0
        eighty_last_eff_c[e_side]   = None
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side]      = None
        eighty_eff_max[e_side]      = None
        # NÃO limpa o price_buffer — histórico necessário para delta
        log_m('EIGHTY', 'RESET',
            f"Remaining: {rstr} | {e_side} — {reason} | ticks reset"
        )

    # ── eighty_reset_silent (sem log — após BUY) ─────────────────────────────
    def eighty_reset_silent(e_side: str):
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side]   = 0
        eighty_last_eff_c[e_side]   = None
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side]      = None
        eighty_eff_max[e_side]      = None

    # ── activar cooldown de volatilidade ─────────────────────────────────────
    def eighty_activate_vol_cooldown(e_side: str, rstr: str, reason: str):
        eighty_vol_cooldown_until[e_side] = time.time() + EIGHTY_VOL_COOLDOWN_S
        eighty_reset(e_side, rstr, reason)
        log_m('EIGHTY', 'VOL_COOLDOWN',
            f"Remaining: {rstr} | {e_side} — bloqueado por {EIGHTY_VOL_COOLDOWN_S}s devido a volatilidade"
        )

    prev_u_p = prev_d_p = None

    # =========================================================================
    # ── LOOP PRINCIPAL ────────────────────────────────────────────────────────
    # =========================================================================
    while True:
        now = time.time()
        rem = m_end - now

        if rem <= 0:
            u_p = best_asks.get('up')  or 0.0
            d_p = best_asks.get('down') or 0.0
            for trade in active_trades[:]:
                cp = u_p if trade['side'] == 'UP' else d_p
                close_trade(trade, cp, "FIM MERCADO", "00:00:000")
                active_trades.remove(trade)
            log_info("Remaining: 00:00:000 | Fim de Mercado")
            break

        rstr = get_remaining_str(rem)

        try:
            await asyncio.wait_for(asyncio.shield(price_change.wait()), timeout=LOOP_SLEEP)
            price_change.clear()
        except asyncio.TimeoutError:
            pass

        u_p = best_asks.get('up')
        d_p = best_asks.get('down')
        if u_p is None or d_p is None:
            continue

        price_changed = (u_p != prev_u_p or d_p != prev_d_p)
        if not price_changed:
            continue

        prev_u_p = u_p
        prev_d_p = d_p
        peg        = u_p + d_p
        underpeg_c = (1.0 - peg) * 100
        peg_disp   = f" | PEG: {peg:.3f} -{underpeg_c:.1f}c" if peg <= EIGHTY_PEG_MIN else ""
        log_raw(f"Remaining: {rstr} | UP: {fc(u_p)} | DOWN: {fc(d_p)}{peg_disp}")

        # ── 1. PEG ARBITRAGE ─────────────────────────────────────────────────
        if (PEG_ARBIT_ACTIVE
                and underpeg_c >= PEG_ARBIT_UNDERPEG_C
                and rem > PEG_ARBIT_MIN_REM
                and peg_arbit_count < MAX_PEG_ENTRIES
                and now - last_peg_time >= PEG_ARBIT_COOLDOWN):
            invest_per_leg = bankroll * eff_peg_risk
            log_m('PEG ARBIT', 'ACTIVE',
                f"Remaining: {rstr} | PEG ARBIT ACTIVADO — PEG {peg:.3f}"
            )
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
        if EIGHTY_ACTIVE:
            if rem <= EIGHTY_CUTOFF_S:
                if not eighty_cutoff_logged:
                    eighty_cutoff_logged = True
                    log_m('EIGHTY', 'CUTOFF',
                        f"Remaining: {rstr} | EIGHTY parado — rem {rstr} <= cutoff {EIGHTY_CUTOFF_S}s")
            else:
                for e_side, nom in (('UP', u_p), ('DOWN', d_p)):
                    token_id = meta['up'] if e_side == 'UP' else meta['down']

                    # Modo sem volatilidade: activo quando EIGHTY_CUTOFF_S=0 e rem <= EIGHTY_WHEN_CUTOFF_0_VOLT
                    skip_vol = (EIGHTY_CUTOFF_S == 0 and EIGHTY_WHEN_CUTOFF_0_VOLT > 0 and rem <= EIGHTY_WHEN_CUTOFF_0_VOLT)

                    # Cooldown de volatilidade: bloqueia completamente este side (apenas se não estamos em skip_vol)
                    if not skip_vol and now < eighty_vol_cooldown_until[e_side]:
                        continue

                    # Cooldown pós-compra (anti-stacking) — ignorado em modo skip_vol
                    if not skip_vol and now - eighty_last_buy[e_side] < EIGHTY_BUY_COOLDOWN:
                        continue

                    ask   = nom + ASK_SPREAD
                    eff_c = effective_entry(ask) * 100

                    # Adicionar preço ao buffer (sempre, para manter histórico)
                    eighty_price_buffer[e_side].add(eff_c, now)

                    # Range check
                    if not (EIGHTY_MIN_EFF_C <= eff_c <= EIGHTY_MAX_EFF_C):
                        if eighty_tick_count[e_side] > 0:
                            eighty_reset(e_side, rstr,
                                f"eff_c {eff_c:.1f}c OUT of RANGE [{EIGHTY_MIN_EFF_C}c-{EIGHTY_MAX_EFF_C}c]")
                        continue

                    # Registo de nível (bucket 0.5c)
                    level_key = round(eff_c * 2) / 2
                    if level_key not in eighty_seen_levels[e_side]:
                        eighty_seen_levels[e_side].add(level_key)
                        eighty_tick_count[e_side] += 1

                    # Vol tracking (max-min desde 1º tick)
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

                    # Delta Multi-Timeframe: 0.5s, 1s, 2s
                    delta_05, valid_05 = eighty_price_buffer[e_side].get_delta(0.5)
                    delta_10, valid_10 = eighty_price_buffer[e_side].get_delta(1.0)
                    delta_20, valid_20 = eighty_price_buffer[e_side].get_delta(2.0)

                    delta_parts = []
                    if valid_05: delta_parts.append(f"Δ0.5s:{delta_05:+.1f}c")
                    if valid_10: delta_parts.append(f"Δ1s:{delta_10:+.1f}c")
                    if valid_20: delta_parts.append(f"Δ2s:{delta_20:+.1f}c")

                    if delta_parts:
                        delta_str = " | ".join(delta_parts)
                    else:
                        delta_str = f"Δ aguardando ({eighty_price_buffer[e_side].get_age():.1f}s acumulado)"

                    # Verificar volatilidade rápida (subida >= EIGHTY_DELTA_VOL_RISE_C em EIGHTY_DELTA_VOL_TIME_S)
                    delta_15, valid_15 = eighty_price_buffer[e_side].get_delta(EIGHTY_DELTA_VOL_TIME_S)
                    rapid_rise = valid_15 and delta_15 is not None and delta_15 >= EIGHTY_DELTA_VOL_RISE_C

                    # Delta NOK se qualquer intervalo está a cair ou subida rápida
                    delta_ok     = True
                    delta_reason = ""
                    has_any_delta = valid_05 or valid_10 or valid_20

                    if valid_05 and delta_05 is not None and delta_05 < 0:
                        delta_ok = False
                        delta_reason = f"Δ0.5s={delta_05:+.1f}c (a cair)"
                    elif valid_10 and delta_10 is not None and delta_10 < 0:
                        delta_ok = False
                        delta_reason = f"Δ1s={delta_10:+.1f}c (a cair)"
                    elif valid_20 and delta_20 is not None and delta_20 < 0:
                        delta_ok = False
                        delta_reason = f"Δ2s={delta_20:+.1f}c (a cair)"
                    elif rapid_rise:
                        delta_ok = False
                        delta_reason = f"Δ1.5s={delta_15:+.1f}c >= {EIGHTY_DELTA_VOL_RISE_C}c (subida rápida)"

                    if skip_vol:
                        vol_str = f"VOL SKIP (cutoff=0, rem<={EIGHTY_WHEN_CUTOFF_0_VOLT}s) | {var_c:.1f}c em {elapsed:.1f}s"
                    else:
                        vol_str = f"VOL {'NOK' if vol_nok else 'OK'} ({var_c:.1f}c em {elapsed:.1f}s)"
                    delta_status = "↑OK" if (delta_ok and has_any_delta) else ("↓NOK" if has_any_delta else "—")
                    peg_tick_str = f" | PEG: {peg:.3f}" if peg <= EIGHTY_PEG_MIN else ""

                    log_m('EIGHTY', 'WATCH',
                        f"Remaining: {rstr} | {e_side} @ Ask {fc(ask)} "
                        f"| Eff: {fc(eff_c/100)} | {vol_str} | {delta_str} {delta_status}"
                        f"{peg_tick_str} | ticks: {eighty_tick_count[e_side]}/{EIGHTY_MIN_TICKS}"
                    )

                    # Vol NOK e Rapid Rise — só aplicam quando NÃO estamos em modo skip_vol
                    if not skip_vol:
                        if vol_nok:
                            eighty_activate_vol_cooldown(e_side, rstr,
                                f"VOLAT NOK — {var_c:.1f}c em {elapsed:.1f}s "
                                f"(limite: {EIGHTY_VOL_MAX_C:.1f}c/{EIGHTY_VOL_WINDOW_S:.1f}s)")
                            continue

                        if rapid_rise:
                            eighty_activate_vol_cooldown(e_side, rstr,
                                f"RAPID RISE — {delta_15:+.1f}c em {EIGHTY_DELTA_VOL_TIME_S}s "
                                f"(limite: {EIGHTY_DELTA_VOL_RISE_C:.1f}c)")
                            continue

                    # Ticks suficientes: verificar todas as condições
                    if eighty_tick_count[e_side] >= EIGHTY_MIN_TICKS:
                        if peg < EIGHTY_PEG_MIN:
                            eighty_reset(e_side, rstr,
                                f"NOK — PEG: {peg:.3f} < {EIGHTY_PEG_MIN}")
                            continue

                        if has_any_delta and not delta_ok:
                            eighty_reset(e_side, rstr,
                                f"DELTA NOK — {delta_reason}")
                            continue

                        if bankroll > 0:
                            delta_log = delta_str if has_any_delta else "Δ N/A"
                            await open_trade(e_side, nom, 'EIGHTY', rstr,
                                             risk=eff_eighty_risk, wait_close=True,
                                             peg_val=peg, token_id=token_id,
                                             extra_log=f"ticks: {eighty_tick_count[e_side]}/{EIGHTY_MIN_TICKS} | {delta_log}")
                            eighty_last_buy[e_side] = now
                            eighty_reset_silent(e_side)
                            log_m('EIGHTY', 'COOLDOWN',
                                f"Remaining: {rstr} | {e_side} — cooldown {EIGHTY_BUY_COOLDOWN:.1f}s (anti-stacking)")

        # ── 4. CICLO 30s ─────────────────────────────────────────────────────
        if CICLO_30S_ACTIVE:
            if not flags['s35'] and rem <= CYCLE_30S_SNAPSHOT_REM:
                state['c1']['snap_u'] = u_p
                state['c1']['snap_d'] = d_p
                flags['s35'] = True
                log_m('CICLO 30s', 'SNAP',
                    f"Remaining: {rstr} | UP: {fc(u_p)} DOWN: {fc(d_p)}")

            if flags['s35'] and not flags['v30'] and rem <= CYCLE_30S_VOL_CHECK_REM:
                vol_c = abs(u_p - state['c1']['snap_u']) * 100
                flags['v30'] = True
                state['c1']['vol_ok'] = vol_c <= CYCLE_VOL_MAX_C
                ok_str = "OK" if state['c1']['vol_ok'] else "NOK"
                log_m('CICLO 30s', 'VOLT',
                    f"Remaining: {rstr} | vol={vol_c:.1f}c (<= {CYCLE_VOL_MAX_C:.1f}c) "
                    f"| ok={state['c1']['vol_ok']} | {ok_str}")

            if (flags['v30'] and state['c1'].get('vol_ok')
                    and not flags['d29'] and rem <= CYCLE_30S_BUY_REM):
                flags['d29'] = True
                for e_side, nom, tid in (
                    ('UP',   u_p, meta['up']),
                    ('DOWN', d_p, meta['down'])
                ):
                    price_c = nom * 100
                    peg_c   = peg * 100
                    if CYCLE_PRICE_MIN_C <= price_c <= CYCLE_PRICE_MAX_C and peg_c >= CYCLE_PEG_MIN_C:
                        await open_trade(e_side, nom, 'CICLO_30s', rstr,
                                         risk=eff_risk_per_trade, wait_close=True,
                                         peg_val=peg, token_id=tid)
                    else:
                        reasons = []
                        if price_c < CYCLE_PRICE_MIN_C:
                            reasons.append(f"price {price_c:.1f}c < min {CYCLE_PRICE_MIN_C:.1f}c")
                        elif price_c > CYCLE_PRICE_MAX_C:
                            reasons.append(f"price {price_c:.1f}c > max {CYCLE_PRICE_MAX_C:.1f}c")
                        if peg_c < CYCLE_PEG_MIN_C:
                            reasons.append(f"PEG {peg_c:.1f}c < {CYCLE_PEG_MIN_C:.1f}c")
                        log_m('CICLO 30s', 'SKIP',
                            f"Remaining: {rstr} | {e_side} sem compra — {' | '.join(reasons)}")

        # ── 5. CICLO 20s ─────────────────────────────────────────────────────
        if CICLO_20S_ACTIVE:
            if not flags['s25'] and rem <= CYCLE_20S_SNAPSHOT_REM:
                state['c2']['snap_u'] = u_p
                state['c2']['snap_d'] = d_p
                flags['s25'] = True
                log_m('CICLO 20s', 'SNAP',
                    f"Remaining: {rstr} | UP: {fc(u_p)} DOWN: {fc(d_p)}")

            if flags['s25'] and not flags['v20'] and rem <= CYCLE_20S_VOL_CHECK_REM:
                vol_c = abs(u_p - state['c2']['snap_u']) * 100
                flags['v20'] = True
                state['c2']['vol_ok'] = vol_c <= CYCLE_VOL_MAX_C
                ok_str = "OK" if state['c2']['vol_ok'] else "NOK"
                log_m('CICLO 20s', 'VOLT',
                    f"Remaining: {rstr} | vol={vol_c:.1f}c (<= {CYCLE_VOL_MAX_C:.1f}c) "
                    f"| ok={state['c2']['vol_ok']} | {ok_str}")

            if (flags['v20'] and state['c2'].get('vol_ok')
                    and not flags['d19'] and rem <= CYCLE_20S_BUY_REM):
                flags['d19'] = True
                for e_side, nom, tid in (
                    ('UP',   u_p, meta['up']),
                    ('DOWN', d_p, meta['down'])
                ):
                    price_c = nom * 100
                    peg_c   = peg * 100
                    if CYCLE_PRICE_MIN_C <= price_c <= CYCLE_PRICE_MAX_C and peg_c >= CYCLE_PEG_MIN_C:
                        await open_trade(e_side, nom, 'CICLO_20s', rstr,
                                         risk=eff_risk_per_trade, wait_close=True,
                                         peg_val=peg, token_id=tid)
                    else:
                        reasons = []
                        if price_c < CYCLE_PRICE_MIN_C:
                            reasons.append(f"price {price_c:.1f}c < min {CYCLE_PRICE_MIN_C:.1f}c")
                        elif price_c > CYCLE_PRICE_MAX_C:
                            reasons.append(f"price {price_c:.1f}c > max {CYCLE_PRICE_MAX_C:.1f}c")
                        if peg_c < CYCLE_PEG_MIN_C:
                            reasons.append(f"PEG {peg_c:.1f}c < {CYCLE_PEG_MIN_C:.1f}c")
                        log_m('CICLO 20s', 'SKIP',
                            f"Remaining: {rstr} | {e_side} sem compra — {' | '.join(reasons)}")

# =============================================================================
# ============================= MAIN ==========================================
# =============================================================================

async def main():
    global daily_profit, last_day, price_change, bankroll, risk_multiplier

    log_info("BOT INICIADO v0.30.5")
    log_info(f"LIVE_TRADING: {LIVE_TRADING} | PRIVATE_KEY: {'***' if POLYMARKET_PRIVATE_KEY else 'NÃO ENCONTRADO'}")
    log_info(f"EIGHTY Delta Tracking: {EIGHTY_DELTA_LOOKBACK_S}s lookback")
    log_info(f"EIGHTY Vol Filter: {EIGHTY_VOL_MAX_C}c/{EIGHTY_VOL_WINDOW_S}s + {EIGHTY_VOL_COOLDOWN_S}s cooldown")
    log_info(f"EIGHTY Anti-Stacking: {EIGHTY_BUY_COOLDOWN}s cooldown per-side")

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
                risk_multiplier = min(risk_multiplier * 2.0, MAX_RISK_MULTIPLIER)
                log_info(
                    f"ROUND | PnL: ${profit_this:+.4f} ({pnl_pct:+.2f}%) "
                    f"| MARTINGALE → x{risk_multiplier:.0f} (cap: x{MAX_RISK_MULTIPLIER:.0f})"
                )
            else:
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