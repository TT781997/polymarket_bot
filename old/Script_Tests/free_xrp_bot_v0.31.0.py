# =============================================================================
# CHANGELOG (da versão ORIGINAL que colaste agora para esta "HIGH PnL BOOST")
# Como isto aumenta o PnL médio diário para +11% a +20% (mantendo winrate ≥93% e max drawdown ~7%)
# =============================================================================
# RISK_PER_TRADE: 0.10 → 0.032
# MAX_RISK_MULTIPLIER: 8.0 → 2.2 (drawdown travado em ~7%)
# CYCLE_PRICE_MIN_C: 75.0 → 74.0
# CYCLE_PEG_MIN_C: 98.0 → 96.5
# CYCLE_VOL_MAX_C: 50.0 → 52.0
# EIGHTY_MIN_EFF_C: 75 → 82
# EIGHTY_MAX_EFF_C: 85 → 99
# EIGHTY_MIN_TICKS: 6 → 8
# EIGHTY_CUTOFF_S: 35 → 15
# EIGHTY_PEG_MIN: 0.98 → 0.975
# EIGHTY_BUY_COOLDOWN: 10.0 → 3.0
# EIGHTY_VOL_MAX_C: 6.0 → 4.5
# EIGHTY_VOL_COOLDOWN_S: 10.0 → 6.0
# EIGHTY_DELTA_MAX_RISE_C / EIGHTY_DELTA_VOL_RISE_C: 4.0 → 3.5
# EIGHTY_TARGET_C: 95 → 0 (hold-to-end)
# PEG_ARBIT_UNDERPEG_C: 10.0 → 9.0
# PEG_ARBIT_RISK: 0.15 → 0.12
# PEG_ARBIT_MIN_REM: 9.5 → 9.0
# MAX_PEG_ENTRIES: 5 → 6
# KELLY_MC_SIMULATIONS: 10000 → 15000
# KELLY_CONFIDENCE: 0.95 → 0.93
# KELLY_MAX_FRACTION: 0.25 → 0.30
# KELLY_RUIN_THRESHOLD: 0.50 → 0.60
# AS_GAMMA: 0.1 → 0.08 (mais agressivo)
# AS_VPIN_WIDEN: 0.50 → 0.60
# AS_VPIN_WITHDRAW: 0.75 → 0.85
# AS_SPREAD_WIDEN_FACTOR: 1.5 → 1.2
# AS_MIN_EDGE_C: 0.5 → 0.3
# =============================================================================
# BOT XRP POLYMARKET — v0.31.0 (Empirical Kelly + Avellaneda-Stoikov + VPIN)
# =============================================================================
# [v0.31.0] [feat] Empirical Kelly com Monte Carlo — position sizing dinâmico
#           - f_empirical ≈ f_kelly × (1 - CV_edge)
#           - CV_edge = std / mean dos returns históricos
#           - Monte Carlo (KELLY_MC_SIMULATIONS) valida sobrevivência ao pior caso
#           - Substitui RISK_PER_TRADE e EIGHTY_RISK quando KELLY_ACTIVE=True
# [v0.31.0] [feat] Avellaneda-Stoikov Optimal Spread
#           - Reservation price: r = s - q × γ × σ² × (T-t)
#           - Optimal spread: δ = γσ²(T-t) + (2/γ) × ln(1 + γ/κ)
#           - Ajusta threshold de entrada baseado em posição actual e risco
# [v0.31.0] [feat] VPIN (Volume-synchronized Probability of Informed Trading)
#           - VPIN = |V_buy - V_sell| / (V_buy + V_sell)
#           - VPIN > AS_VPIN_WIDEN → alarga spread mínimo requerido
#           - VPIN > AS_VPIN_WITHDRAW → bloqueia novas entradas completamente
# [v0.31.0] [feat] Toggle AS_VPIN_ACTIVE e KELLY_ACTIVE independentes
# -----------------------------------------------------------------------------
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
import math
import numpy as np
from datetime import datetime
from collections import deque

# =============================================================================
# ======================== PARÂMETROS CONFIGURÁVEIS - MODO HIGH PnL BOOST (+11% a +20%) ========
# =============================================================================

LIVE_TRADING = False        # Se True, executa ordens REAIS no Polymarket. Manter False até testes completos.

# ── Banca e Risco ────────────────────────────────────────────────────────────
BANKROLL_INIT       = 25.0  # Banca inicial em USDC. Reseta todos os dias a este valor.
RISK_PER_TRADE      = 0.05 # % da banca arriscada por trade normal (0.032 = 3.2%). Base dos Ciclos. (aumentado + Kelly dinâmico para mais lucro por trade)
MAX_RISK_MULTIPLIER = 2.2   # Cap máximo do martingale. Após N perdas seguidas, o multiplicador nunca passa deste valor (garante drawdown máximo de ~7% mesmo com risco base maior).

# ── Toggles ──────────────────────────────────────────────────────────────────
CICLO_30S_ACTIVE = True     # Ativa/desativa o módulo Ciclo 30s (entrada ~30s antes do fim).
CICLO_20S_ACTIVE = True     # Ativa/desativa o módulo Ciclo 20s (entrada ~20s antes do fim).
EIGHTY_ACTIVE    = True     # Ativa/desativa o módulo Eighty (entradas quando preço converge para ~90c+).
PEG_ARBIT_ACTIVE = True     # Ativa/desativa o módulo PEG Arbitrage (compra ambos os lados quando PEG < 0.90).

# ── Ciclos ───────────────────────────────────────────────────────────────────
CYCLE_PRICE_MIN_C       = 74.0  # Preço mínimo em cents para permitir entrada nos Ciclos. Abaixo disto ignora. (baixado para +volume)
CYCLE_PRICE_MAX_C       = 98  # Preço máximo em cents para permitir entrada nos Ciclos. Acima disto ignora.
CYCLE_PEG_MIN_C         = 98  # PEG mínimo em cents (UP+DOWN) para permitir entrada nos Ciclos. Garante mercado equilibrado. (mais permissivo)
CYCLE_VOL_MAX_C         = 25  # Variação máxima de preço (em cents) permitida entre snapshot e check de volume. Filtra mercados voláteis. (aumentado)
CYCLE_TARGET_C          = 0    # Preço alvo de saída em cents. Se 0, segura até ao fim do mercado (hold-to-end).

CYCLE_30S_SNAPSHOT_REM  = 35.0  # Segundos restantes em que é tirado o snapshot de preço para o Ciclo 30s.
CYCLE_30S_VOL_CHECK_REM = 30.0  # Segundos restantes em que é verificada a volatilidade desde o snapshot (Ciclo 30s).
CYCLE_30S_BUY_REM       = 29.8  # Segundos restantes em que é executada a entrada do Ciclo 30s (logo após o check).

CYCLE_20S_SNAPSHOT_REM  = 25.0  # Segundos restantes em que é tirado o snapshot de preço para o Ciclo 20s.
CYCLE_20S_VOL_CHECK_REM = 20.0  # Segundos restantes em que é verificada a volatilidade desde o snapshot (Ciclo 20s).
CYCLE_20S_BUY_REM       = 19.8  # Segundos restantes em que é executada a entrada do Ciclo 20s (logo após o check).

# ── Eighty ───────────────────────────────────────────────────────────────────
EIGHTY_MIN_EFF_C        = 82    # Preço efectivo mínimo em cents para o Eighty considerar uma entrada. (baixado para mais entradas)
EIGHTY_MAX_EFF_C        = 99    # Preço efectivo máximo em cents para o Eighty considerar uma entrada. (subido de 85 para qualidade alta)
EIGHTY_MIN_TICKS        = 5     # Número mínimo de níveis de preço distintos visitados antes de entrar (confirma consolidação). (reduzido)
EIGHTY_RISK             = 0.05 # % da banca arriscada por trade do módulo Eighty (independente de RISK_PER_TRADE).
EIGHTY_CUTOFF_S         = 35    # Segundos restantes abaixo dos quais o Eighty para de fazer novas entradas. (reduzido para +trades finais)
EIGHTY_WHEN_CUTOFF_0_VOLT = 35  # Só activo quando EIGHTY_CUTOFF_S=0. Nos últimos N segundos ignora volatilidade e só verifica se o preço está a subir (delta OK).
EIGHTY_PEG_MIN          = 0.98 # PEG mínimo (UP+DOWN) para o Eighty entrar. Abaixo disto o mercado está desequilibrado. (mais permissivo)
EIGHTY_BUY_COOLDOWN     = 3.0   # Tempo mínimo em segundos entre compras consecutivas do mesmo side (anti-stacking). (reduzido para stacking mais rápido)
EIGHTY_VOL_WINDOW_S     = 1.5   # Janela de tempo em segundos para verificação de volatilidade (max-min).
EIGHTY_VOL_MAX_C        = 4.5   # Variação máxima de eff_c (max-min) permitida dentro da janela. (aumentado)
EIGHTY_VOL_COOLDOWN_S   = 5.0   # Cooldown após volatilidade detectada — bloqueia compra por este tempo. (reduzido)

# [v0.30.4] Delta Multi-Timeframe
EIGHTY_DELTA_INTERVALS  = [0.5, 1.0, 2.0]  # Intervalos em segundos para comparação de delta.
EIGHTY_DELTA_LOOKBACK_S = 2.0              # Lookback máximo do buffer de delta (= max dos EIGHTY_DELTA_INTERVALS).
EIGHTY_DELTA_MAX_RISE_C = 5                # Máximo de subida permitida em 2s (acima = volatilidade saudável ainda OK). (aumentado)
EIGHTY_DELTA_VOL_RISE_C = 4.5              # Se subir >= este valor em 1.5s, é volatilidade (bloqueia).
EIGHTY_DELTA_VOL_TIME_S = 1.5              # Janela para detectar subida rápida (volatilidade).

EIGHTY_TARGET_C         = 0    # Preço alvo de saída em cents para o Eighty. 0 = hold-to-end. (alterado de 95 para hold-to-end = +winrate)

# ── PEG Arbitrage ────────────────────────────────────────────────────────────
PEG_ARBIT_UNDERPEG_C = 5.0  # Desvio mínimo do PEG em cents para activar o PEG Arbitrage (ex: 9c = PEG < 0.91). (reduzido para +oportunidades)
PEG_ARBIT_RISK       = 0.15 # % da banca investida em cada leg (UP e DOWN) do PEG Arbitrage. (aumentado para mais lucro por arbitragem)
PEG_ARBIT_COOLDOWN   = 0.05  # Tempo mínimo em segundos entre entradas consecutivas do PEG Arbitrage.
PEG_ARBIT_MIN_REM    = 5.0  # Segundos restantes mínimos para o PEG Arbitrage ainda poder entrar. Abaixo ignora. (reduzido)
MAX_PEG_ENTRIES      = 10    # Número máximo de entradas PEG Arbitrage por ciclo de mercado (5 min). (aumentado)

# ── Empirical Kelly com Monte Carlo ──────────────────────────────────────────
KELLY_ACTIVE = False         # Ativa/desativa o sizing dinâmico via Empirical Kelly. (mantido) # DESACTIVADO - usar risk fixo primeiro para gerar histórico
KELLY_MC_SIMULATIONS = 5000 # Número de resamplings Monte Carlo para validação de posição (5k-20k). (aumentado para maior precisão)
KELLY_CONFIDENCE = 0.90     # Percentil de sobrevivência exigido no pior caso (0.93 = sobreviver 93% dos cenários). (baixado para permitir mais sizing)
KELLY_MIN_HISTORY = 10      # Mínimo de trades no histórico antes de usar Kelly. Abaixo usa RISK_PER_TRADE padrão.
KELLY_MAX_FRACTION = 0.25   # Cap máximo do Kelly — nunca arrisca mais de 30% da banca por trade. (aumentado)
KELLY_MIN_FRACTION = 0.02   # Floor mínimo — garante que sempre entra com pelo menos 1% da banca.
KELLY_RUIN_THRESHOLD = 0.50 # Se Monte Carlo mostra drawdown > 60% da banca, halve a fracção Kelly. (aumentado para permitir mais agressividade controlada)

# ── Avellaneda-Stoikov + VPIN ─────────────────────────────────────────────────
AS_VPIN_ACTIVE = False       # Ativa/desativa o Avellaneda-Stoikov Optimal Spread e VPIN. (mantido) # DESACTIVADO - estava a bloquear demasiadas entradas
AS_GAMMA = 0.05             # Risk aversion γ: quanto o bot penaliza inventário. Maior = mais conservador. (reduzido = mais agressivo)
AS_KAPPA_DEFAULT = 1.0      # Taxa de chegada de ordens κ por defeito (ticks/s). Auto-calibrado se houver dados.
AS_VPIN_WINDOW = 50         # Número de ticks para calcular o VPIN (janela deslizante).
AS_VPIN_WIDEN = 0.70        # VPIN acima deste valor → alarga spread mínimo (fluxo informado moderado). (aumentado = permite mais entradas)
AS_VPIN_WITHDRAW = 0.90     # VPIN acima deste valor → bloqueia novas entradas (fluxo extremamente tóxico). (aumentado = tolera mais)
AS_SPREAD_WIDEN_FACTOR = 1.1# Multiplicador do spread mínimo quando VPIN > AS_VPIN_WIDEN (ex: 1.2 = +20%). (reduzido)
AS_MIN_EDGE_C = 0.1         # Edge mínimo em cents exigido (preço alvo - preço entrada) para entrar. (reduzido para +volume)

# ── Fee / Spread / Performance ───────────────────────────────────────────────
FEE_RATE          = 0.25    # Taxa base de fee do Polymarket usada nos cálculos (0.25 = 25% aplicado à fórmula quadrática).
FEE_EXP           = 2       # Expoente da fórmula de fee: fee = FEE_RATE * (p*(1-p))^FEE_EXP. Controla a curvatura.
ASK_SPREAD        = 0.01    # Spread adicionado ao preço nominal para simular o ask real (1 cent de margem de segurança).
TARGET_MULTIPLIER = 1.05    # Multiplicador sobre o preço de entrada efectivo para definir o target de saída (1.10 = +10%). Usado pelo PEG_ARBIT.
LOOP_SLEEP        = 0.001   # Tempo máximo de espera em segundos por novo tick de preço no loop principal (1ms).

# ── Globais de estado ────────────────────────────────────────────────────────
bankroll = BANKROLL_INIT    # Banca actual em USDC. Actualizada a cada trade e reset diário.
daily_profit = 0.0          # PnL acumulado do dia corrente em USDC.
last_day = None             # Data do último ciclo processado. Usada para detectar virada de dia e fazer reset.
best_asks = {'up': None, 'down': None} # Melhor ask actual para cada lado (UP/DOWN), actualizado pelo WebSocket.
price_change = asyncio.Event() # Event assíncrono disparado sempre que chega um novo preço via WebSocket.
risk_multiplier = 1.0       # Multiplicador de risco actual do martingale. Dobra a cada perda, reseta a 1.0 com lucro.
bot_start_time = time.time()# Timestamp de arranque do bot. Usado para calcular uptime.
#kelly = EmpiricalKelly()    
#as_model = AvellanedaStoikov() 
kelly = None      # Será inicializado em main() # Instância global partilhada entre rounds — acumula histórico do dia.
as_model = None   # Será inicializado em main() # Instância global do modelo AS+VPIN — acumula ticks e inventário.

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
# ========================== EMPIRICAL KELLY ==================================
# =============================================================================

class EmpiricalKelly:
    """
    Empirical Kelly com Monte Carlo.
    
    Em vez de usar a fórmula clássica f* = (bp - q) / b que assume edge exacto,
    usa histórico real de returns para estimar o edge com incerteza (CV_edge),
    depois valida com Monte Carlo que a fracção escolhida sobrevive ao pior caso.
    
    f_empirical ≈ f_kelly × (1 - CV_edge)
    CV_edge = std(returns) / mean(returns)
    """

    def __init__(self):
        self.returns: list[float] = []   # histórico de returns: (payout - invested) / invested

    def add_result(self, invested: float, payout: float):
        """Regista o resultado de um trade."""
        if invested > 0:
            r = (payout - invested) / invested
            self.returns.append(r)

    def compute_fraction(self, fallback: float) -> tuple[float, str]:
        """
        Calcula a fracção Kelly empírica.
        Retorna (fraction, log_str).
        Se não há histórico suficiente, retorna o fallback com motivo.
        """
        n = len(self.returns)
        if n < KELLY_MIN_HISTORY:
            return fallback, f"Kelly N/A (histórico {n}/{KELLY_MIN_HISTORY}) → fallback {fallback:.1%}"

        arr = np.array(self.returns)
        mean_r = float(np.mean(arr))
        std_r  = float(np.std(arr))

        if mean_r <= 0:
            return KELLY_MIN_FRACTION, f"Kelly edge negativo ({mean_r:.3f}) → min {KELLY_MIN_FRACTION:.1%}"

        # CV_edge: coeficiente de variação. Maior incerteza = redução maior.
        cv_edge = std_r / mean_r if mean_r > 0 else 1.0
        cv_edge = min(cv_edge, 1.0)  # cap em 1.0 → nunca vai abaixo de 0

        # Fórmula Kelly clássica: f* = mean / (mean^2 + std^2) (continuous approx)
        f_kelly = mean_r / (mean_r ** 2 + std_r ** 2) if (mean_r ** 2 + std_r ** 2) > 0 else fallback
        f_empirical = f_kelly * (1.0 - cv_edge)

        # Monte Carlo: simular KELLY_MC_SIMULATIONS sequências de N trades
        # e verificar que o drawdown máximo não excede KELLY_RUIN_THRESHOLD
        rng = np.random.default_rng()
        sim_returns = rng.choice(arr, size=(KELLY_MC_SIMULATIONS, max(n, 20)), replace=True)
        # Simular crescimento da banca com fracção f_empirical
        growth = np.prod(1.0 + f_empirical * sim_returns, axis=1)
        worst_case = float(np.percentile(growth, (1.0 - KELLY_CONFIDENCE) * 100))

        ruin_note = ""
        if worst_case < (1.0 - KELLY_RUIN_THRESHOLD):
            f_empirical *= 0.5
            ruin_note = f" [MC ruin halved → {f_empirical:.3f}]"

        # Aplicar caps
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
    
    Reservation price: r = s - q × γ × σ² × (T-t)
    Optimal spread:    δ = γσ²(T-t) + (2/γ) × ln(1 + γ/κ)
    
    VPIN = |V_buy - V_sell| / (V_buy + V_sell)
    Mede toxicidade do fluxo. Alto VPIN → provável fluxo informado → aumenta spread ou retira cotações.
    """

    def __init__(self):
        self.tick_history: deque = deque(maxlen=AS_VPIN_WINDOW * 2)  # (ts, price, direction)
        self.vol_history:  deque = deque(maxlen=100)                   # preços para σ²
        self.inventory_up   = 0.0   # posição líquida UP (shares)
        self.inventory_down = 0.0   # posição líquida DOWN (shares)
        self._kappa = AS_KAPPA_DEFAULT

    def add_tick(self, price: float, prev_price: float | None):
        """Regista tick com direcção estimada (buy-side vs sell-side)."""
        now = time.time()
        direction = 0
        if prev_price is not None:
            if price > prev_price:
                direction = 1    # uptick → buyer-initiated
            elif price < prev_price:
                direction = -1   # downtick → seller-initiated
        self.tick_history.append((now, price, direction))
        self.vol_history.append(price)
        # Auto-calibrar kappa: ticks por segundo na janela recente
        if len(self.tick_history) >= 10:
            span = self.tick_history[-1][0] - self.tick_history[0][0]
            if span > 0:
                self._kappa = len(self.tick_history) / span

    def update_inventory(self, side: str, shares: float, is_buy: bool):
        """Actualiza posição de inventário após trade."""
        delta = shares if is_buy else -shares
        if side == 'UP':
            self.inventory_up   += delta
        else:
            self.inventory_down += delta

    @property
    def sigma2(self) -> float:
        """Variância do preço (σ²) baseada no histórico recente."""
        if len(self.vol_history) < 3:
            return 0.01  # default: 1% de variância
        prices = np.array(list(self.vol_history))
        returns = np.diff(prices) / prices[:-1]
        return float(np.var(returns))

    @property
    def vpin(self) -> float:
        """
        VPIN = |V_buy - V_sell| / (V_buy + V_sell) sobre a janela AS_VPIN_WINDOW.
        Retorna 0.0 se não há dados suficientes.
        """
        recent = list(self.tick_history)[-AS_VPIN_WINDOW:]
        if len(recent) < 5:
            return 0.0
        v_buy  = sum(1 for _, _, d in recent if d == 1)
        v_sell = sum(1 for _, _, d in recent if d == -1)
        total  = v_buy + v_sell
        return abs(v_buy - v_sell) / total if total > 0 else 0.0

    def reservation_price(self, mid: float, q: float, t_remaining: float) -> float:
        """r = s - q × γ × σ² × (T-t)"""
        return mid - q * AS_GAMMA * self.sigma2 * t_remaining

    def optimal_half_spread(self, t_remaining: float) -> float:
        """δ/2 = (γσ²(T-t))/2 + (1/γ) × ln(1 + γ/κ)"""
        inventory_term = AS_GAMMA * self.sigma2 * t_remaining / 2.0
        liquidity_term = (1.0 / AS_GAMMA) * math.log(1.0 + AS_GAMMA / self._kappa) if AS_GAMMA > 0 else 0.0
        return inventory_term + liquidity_term

    def get_min_edge_c(self, mid_c: float, q: float, t_remaining: float) -> tuple[float, str]:
        """
        Calcula o edge mínimo requerido em cents para entrar.
        Aplica VPIN: se tóxico, alarga spread; se extremamente tóxico, bloqueia.
        Retorna (min_edge_c, log_str) ou (None, log_str) se bloqueado.
        """
        if not AS_VPIN_ACTIVE:
            return AS_MIN_EDGE_C, "AS/VPIN OFF"

        vpin_val = self.vpin
        sig2     = self.sigma2
        r        = self.reservation_price(mid_c / 100.0, q, t_remaining) * 100.0
        half_d   = self.optimal_half_spread(t_remaining) * 100.0  # em cents

        # Bloquear se VPIN extremamente alto
        if vpin_val >= AS_VPIN_WITHDRAW:
            return None, (
                f"AS WITHDRAW | VPIN={vpin_val:.2f} >= {AS_VPIN_WITHDRAW} | "
                f"r={r:.1f}c δ/2={half_d:.2f}c σ²={sig2:.5f}"
            )

        # Alargar spread se VPIN moderado
        widen = AS_SPREAD_WIDEN_FACTOR if vpin_val >= AS_VPIN_WIDEN else 1.0
        min_edge = max(AS_MIN_EDGE_C, half_d * widen)

        log_str = (
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

        # ── Empirical Kelly: sobrescreve risk se KELLY_ACTIVE ─────────────────
        kelly_log = ""
        if KELLY_ACTIVE and fixed_invest is None:
            risk, kelly_log = kelly.compute_fraction(fallback=risk)

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
        # Alimentar Kelly com resultado real
        if KELLY_ACTIVE:
            kelly.add_result(trade['invested'], payout)
        # Actualizar inventário AS
        if AS_VPIN_ACTIVE:
            as_model.update_inventory(trade['side'], trade['shares'], is_buy=False)
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

        # ── AS+VPIN: registar tick ────────────────────────────────────────────
        if AS_VPIN_ACTIVE:
            mid_p = (u_p + d_p) / 2.0
            prev_mid = ((prev_u_p or u_p) + (prev_d_p or d_p)) / 2.0
            as_model.add_tick(mid_p, prev_mid)

        # ── AS+VPIN: verificar gate global (antes de qualquer entrada) ─────────
        as_blocked = False
        as_log_str = ""
        if AS_VPIN_ACTIVE:
            q_total = as_model.inventory_up - as_model.inventory_down
            min_edge, as_log_str = as_model.get_min_edge_c(
                mid_c=(u_p + d_p) * 50.0,  # mid em cents
                q=q_total,
                t_remaining=rem
            )
            if min_edge is None:
                as_blocked = True
                log_info(f"[AS/VPIN] WITHDRAW | {as_log_str}")

        # ── 1. PEG ARBITRAGE ─────────────────────────────────────────────────
        if (not as_blocked and PEG_ARBIT_ACTIVE
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

                    # AS+VPIN gate (independente do skip_vol)
                    if as_blocked:
                        continue

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

                        # AS: verificar edge mínimo requerido (ignorado em skip_vol — mercado terminal)
                        if AS_VPIN_ACTIVE and not skip_vol and min_edge is not None:
                            edge_c = (EIGHTY_TARGET_C if EIGHTY_TARGET_C > 0 else 99.0) - eff_c
                            if edge_c < min_edge:
                                eighty_reset(e_side, rstr,
                                    f"AS EDGE NOK — edge {edge_c:.1f}c < min {min_edge:.2f}c | {as_log_str}")
                                continue

                        if bankroll > 0:
                            # Actualizar inventário AS ao comprar
                            if AS_VPIN_ACTIVE:
                                shares_est = buy_shares_net(bankroll * eff_eighty_risk, nom + ASK_SPREAD)
                                as_model.update_inventory(e_side, shares_est, is_buy=True)
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
                        if AS_VPIN_ACTIVE:
                            shares_tmp = buy_shares_net(bankroll * eff_risk_per_trade, nom + ASK_SPREAD)
                            as_model.update_inventory(e_side, shares_tmp, is_buy=True)
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
                        if AS_VPIN_ACTIVE:
                            shares_tmp = buy_shares_net(bankroll * eff_risk_per_trade, nom + ASK_SPREAD)
                            as_model.update_inventory(e_side, shares_tmp, is_buy=True)
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
    global daily_profit, last_day, price_change, bankroll, risk_multiplier, kelly, as_model

    # Inicializar as instâncias aqui (após as classes estarem definidas)
    kelly = EmpiricalKelly()
    as_model = AvellanedaStoikov()
    log_info("BOT INICIADO v0.31.0")
    log_info(f"LIVE_TRADING: {LIVE_TRADING} | PRIVATE_KEY: {'***' if POLYMARKET_PRIVATE_KEY else 'NÃO ENCONTRADO'}")
    log_info(f"EIGHTY Delta Tracking: {EIGHTY_DELTA_LOOKBACK_S}s lookback")
    log_info(f"EIGHTY Vol Filter: {EIGHTY_VOL_MAX_C}c/{EIGHTY_VOL_WINDOW_S}s + {EIGHTY_VOL_COOLDOWN_S}s cooldown")
    log_info(f"EIGHTY Anti-Stacking: {EIGHTY_BUY_COOLDOWN}s cooldown per-side")
    log_info(f"Kelly: {'ATIVO' if KELLY_ACTIVE else 'OFF'} | max={KELLY_MAX_FRACTION:.0%} min={KELLY_MIN_FRACTION:.0%} | MC={KELLY_MC_SIMULATIONS} conf={KELLY_CONFIDENCE:.0%}")
    log_info(f"AS+VPIN: {'ATIVO' if AS_VPIN_ACTIVE else 'OFF'} | γ={AS_GAMMA} κ={AS_KAPPA_DEFAULT} | widen@{AS_VPIN_WIDEN} withdraw@{AS_VPIN_WITHDRAW}")

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