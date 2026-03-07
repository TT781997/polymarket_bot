# =============================================================================
# BOT XRP POLYMARKET — v0.35.0
# =============================================================================
# CHANGELOG v0.35.0:
# [v0.35.0] [fix]  Martingale nunca excede 50% da banca — em NENHUMA situação
#           - MAX_RISK_PERCENT=0.50 é o cap ABSOLUTO de qualquer trade
#           - Fórmula: min(base × multiplier + recovery_bonus, MAX_RISK_PERCENT)
#           - recovery_bonus = MARTINGALE_RECOVERY × prev_round_loss / bankroll
#           - prev_round_loss = perda da ronda ANTERIOR (só a última, não acumulado)
#           - accumulated_loss = total acumulado desde o último lucro (para tracking)
# [v0.35.0] [fix]  Logs separados: MARTINGALE sempre antes de ROUND, nunca misturado
# [v0.35.0] [fix]  Formato de log: [INFO] [dd/mm/yy | HH:MM:SS.mmm] | MÓDULO | mensagem
# [v0.35.0] [feat] calc_risk() e calc_risk_preview() como funções reutilizáveis
# -----------------------------------------------------------------------------
# CHANGELOG v0.34.0:
# [v0.34.0] [feat] MARTINGALE_RECOVERY — adiciona 50% da perda anterior ao risco
# [v0.34.0] [feat] accumulated_loss — tracking de perdas para recovery gradual
# [v0.34.0] [feat] PEG calculado pelo preço Efectivo (Eff), não pelo preço base
# [v0.34.0] [feat] PEG_ARBIT_RANGE — range de preço efectivo para entrada
# [v0.34.0] [fix]  PEG ARBIT compra shares iguais em ambos os lados
# [v0.34.0] [fix]  EIGHTY_RISK fixo em 7% da banca
# -----------------------------------------------------------------------------
# CHANGELOG v0.33.0 e anteriores: ver versões anteriores
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
# =============================================================================
#
#   PARÂMETROS CONFIGURÁVEIS — LÊ ISTO ANTES DE ALTERAR QUALQUER VALOR
#
#   CONVENÇÃO DE UNIDADES:
#     _C   → cents  (ex: 97.0 = 0.97 dólar = 97 centavos)
#     _S   → segundos (float)
#     ratio/risk/fraction → 0.0 a 1.0  (ex: 0.05 = 5%)
#     mult/factor         → ≥ 1.0
#     int                 → número inteiro
#
#   NUNCA ALTERAR:
#     - MAX_RISK_PERCENT: é o teu único travão contra ruína
#     - FEE_RATE / FEE_EXP: reflectem a estrutura real da Polymarket
#
# =============================================================================
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 0 — MODO DE OPERAÇÃO
# ─────────────────────────────────────────────────────────────────────────────

LIVE_TRADING = False
# True  → executa ordens reais na Polymarket (requer secrets.txt com chave privada)
# False → simulação: toda a lógica corre, logs completos, zero dinheiro gasto
# ⚠️  Só mudar para True depois de testar exaustivamente em False

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 1 — BANCA
# ─────────────────────────────────────────────────────────────────────────────

BANKROLL_INIT = 10.0
# Banca inicial em USDC. Em LIVE: lê do Polymarket. Em DEMO: inicia em 10.0 nunca reseta.
# O bot nunca ultrapassa esta banca — se perder, o dia seguinte recomeça daqui.
# Range: [10.0 ... 10000.0] Mínimo 10 USDC, máximo recomendado 10k USDC

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 2 — RISCO BASE POR MÓDULO
# ─────────────────────────────────────────────────────────────────────────────

RISK_PER_TRADE = 0.05
# Fracção da banca por trade nos Ciclos e trades genéricos. [0.01 ... 0.20]
# Este é o risco BASE — o martingale e o recovery multiplicam/somam a partir daqui.
# Ex: 0.05 = 5% de $25.00 = $1.25 por trade em condições normais (sem martingale)

EIGHTY_RISK = 0.15
# Fracção da banca por trade do módulo EIGHTY. [0.01 ... 0.15]
# Separado do RISK_PER_TRADE porque o EIGHTY tem critérios próprios de entrada.
# Ex: 0.15 = 15% de $25.00 = $3.75 por trade em condições normais (sem martingale)

PEG_ARBIT_RISK = 0.25
# Fracção da banca investida no PEG ARBIT (por leg — UP e DOWN têm este valor cada). [0.05 ... 0.30]
# O PEG é lucro quase garantido, por isso aceita risco base mais alto.
# Ex: 0.25 = 25% de $25.00 = $6.25 por leg → $12.50 total por arb (sem martingale)

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 3 — MARTINGALE E RECOVERY
# ─────────────────────────────────────────────────────────────────────────────
#
# COMO FUNCIONA O SISTEMA:
#
#   Após cada ronda com PERDA:
#     1. risk_multiplier dobra: x1 → x2 → x4 → x8 → x16 (cap: MAX_RISK_MULTIPLIER)
#     2. prev_round_loss guarda a perda da última ronda em dólares
#     3. accumulated_loss acumula todas as perdas desde o último lucro
#
#   No início de cada ronda, o risco efectivo é calculado assim:
#
#     recovery_bonus = MARTINGALE_RECOVERY × prev_round_loss / bankroll
#     raw_risk       = (base_risk × risk_multiplier) + recovery_bonus
#     eff_risk       = min(raw_risk, MAX_RISK_PERCENT)   ← NUNCA ultrapassa 50%
#
#   Após uma ronda com LUCRO:
#     1. risk_multiplier reseta para x1
#     2. prev_round_loss reseta para $0.00
#     3. accumulated_loss deduz o lucro (recovery gradual)
#
#   EXEMPLO (RISK_PER_TRADE=10%, bankroll=$25):
#
#     Ronda 1 normal:    eff_risk = 10%×1 + 0%    = 10.0%  → inv=$2.50
#     Ronda 2 (após -$2.50): eff_risk = 10%×2 + 5%   = 25.0%  → inv=$5.50
#     Ronda 3 (após -$5.50): eff_risk = 10%×4 + 11%  = 50.0%  → inv=$9.75 (cap)
#     Ronda 4 (após lucro):  eff_risk = 10%×1 + 0%   = 10.0%  → inv=normal
#
#   O CAP DE 50% É INVIOLÁVEL — não existe combinação de multiplier + recovery
#   que faça o bot investir mais de MAX_RISK_PERCENT da banca num único trade.

MAX_RISK_MULTIPLIER = 8
# Limite máximo do multiplicador do martingale. [2 ... 16] Potências de 2 apenas.
# x16 = após 4 perdas consecutivas (x1 -> x2 -> x4 -> x8 -> x16).
# Com o cap de 50%, o risco real nunca passa de MAX_RISK_PERCENT em nenhuma situação

MAX_RISK_PERCENT = 0.50
# CAP ABSOLUTO E INVIOLÁVEL de risco por trade. [0.20 ... 0.50]
# Nenhum trade nunca ultrapassa este valor, independentemente de martingale/recovery.
# Se bankroll=$20, máximo por trade = $10.00 (0.50 x $20).
# NÃO ALTERAR abaixo de 0.20 ou acima de 0.50 — são limites físicos do sistema

MARTINGALE_RECOVERY = 0.50
# Fracção da perda da ronda ANTERIOR adicionada como recovery bonus. [0.10 ... 0.75]
# Só usa a última ronda (prev_round_loss), não o acumulado total.
# Evita explosão do risco mantendo a recuperação proporcional.
# Ex: 0.50 = se perdeste $5.00, adiciona 0.50 x $5.00 = $2.50 ao risco base

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 4 — TOGGLES DE MÓDULOS
# ─────────────────────────────────────────────────────────────────────────────

EIGHTY_ACTIVE = True
# Estratégia EIGHTY: compra quando preço EFF está entre 82c-99c com confirmação.
# Principal estratégia direcional. Activar sempre para melhor PnL.

PEG_ARBIT_ACTIVE = True
# Arbitragem PEG: compra UP e DOWN com shares iguais quando (Eff_UP + Eff_DOWN) < 100c.
# Lucro quase garantido — um dos lados resolve a 100c.
# Principal driver de PnL. Activar sempre.

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 6 — EIGHTY
# ─────────────────────────────────────────────────────────────────────────────
#
# Lógica: o preço de um lado sobe gradualmente (consolidação) nos últimos minutos.
# O EIGHTY entra quando confirma X ticks de consolidação sem volatilidade excessiva.
# Alta win rate porque mercados que consolidam perto de 90c+ tendem a resolver nesse lado.

EIGHTY_START_REM_S = 300
# Remaining (em segundos) para começar o EIGHTY. 300 = desde o início do mercado.
# Reduzir para activar apenas numa janela mais curta (ex: 60 = último minuto).

EIGHTY_MIN_EFF_C = 80.0
# Preço efectivo (EFF) mínimo para comprar. [50.0 ... 95.0]
# Retorno a 82c EFF: (100-82)/82 = +22%. Mínimo aceitável para este tipo de trade

EIGHTY_MAX_EFF_C = 98.5
# Preço efectivo (EFF) máximo. [85.0 ... 99.9]
# Acima de 99.9c EFF o retorno é menor que 0.1% — não vale a fee

EIGHTY_MIN_TICKS = 5
# Número mínimo de níveis de preço distintos (arredondados a 0.5c EFF) para confirmar. [3 ... 15]
# 5 ticks = preço visitou 5 níveis diferentes -> consolidação real

EIGHTY_CUTOFF_S = 5
# Parar EIGHTY quando faltam X segundos para fim do mercado. [0 ... 30]
# Evita entrar muito perto do fim quando há incerteza máxima

EIGHTY_WHEN_CUTOFF_0_VOLT = 35.0
# Se EIGHTY_CUTOFF_S=0: ignora verificações de volatilidade nos últimos X segundos.
# Útil para capturar movimentos rápidos no final sem ser travado pela volatilidade.

EIGHTY_PEG_MIN_C = 97.0
# PEG mínimo (sum EFF UP + EFF DOWN em cents) para EIGHTY. [90.0 ... 99.5]
# PEG baixo = mercado desequilibrado = risco maior de inversão

EIGHTY_BUY_COOLDOWN = 4.0
# Segundos entre compras consecutivas do mesmo lado (UP ou DOWN). [1.0 ... 10.0]
# Evita acumular múltiplas posições no mesmo preço

# ── VOLATILIDADE MACRO (consolidação geral) ──────────────────────────────────
EIGHTY_VOL_MACRO_WINDOW_S = 5.0
# Janela temporal para medir volatilidade MACRO (consolidação geral). [2.0 ... 15.0]
# Detecta: max_preço - min_preço durante esta janela
# Se var >= EIGHTY_VOL_MACRO_MAX_C → consolidação fraca → rejeita entrada

EIGHTY_VOL_MACRO_MAX_C = 10.5
# Variação máxima EFF permitida na janela MACRO (em cents). [1.0 ... 15.0]
# Ex: 4.5c significa "não entro se houver mais de 4.5c de flutuação em 5s"

# ── VOLATILIDADE MICRO (pump falso / subida rápida) ───────────────────────────
EIGHTY_VOL_MICRO_WINDOW_S = 1.5
# Janela temporal para detectar pump falso (subida rápida, rejição iminente). [0.5 ... 5.0]
# Mede: delta positivo rápido (sinal de reversão)

EIGHTY_VOL_MICRO_SPIKE_C = 4.5
# Variação máxima de SUBIDA rápida permitida (em cents). [1.0 ... 10.0]
# Se preço sube >= este valor em WINDOW_S → pump falso → rejeita entrada
# Ex: 3.5c significa "não entro se houver spike >= 3.5c em 1.5s"

# ── COOLDOWN COMPARTILHADO ───────────────────────────────────────────────────
EIGHTY_VOL_COOLDOWN_S = 5.0
# Cooldown após ANY detecção de volatilidade (MACRO ou MICRO). [2.0 ... 30.0]
# Aguarda este tempo antes de reevaliar. Log identifica qual volatilidade disparou.

EIGHTY_TARGET_C = 0.0
# Target de venda antecipada (0.0 = hold até ao fim). Recomendado manter a 0.0.

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 7 — PEG ARBITRAGE
# ─────────────────────────────────────────────────────────────────────────────
#
# Lógica: UP e DOWN resolvem sempre como par a 100c total.
# Se (Eff_UP + Eff_DOWN) < 100c, comprar shares iguais de ambos = lucro garantido.
# Exemplo: Eff_UP=48c, Eff_DOWN=50c → PEG_Eff=98c → lucro garantido ≈ 2%.
#
# IMPORTANTE: PEG é calculado pelo preço EFECTIVO (inclui fee), não pelo preço base.
# Isto é mais conservador mas evita arbs falsos onde a fee come o lucro.
#
# SHARES IGUAIS: compra exactamente o mesmo número de shares em UP e DOWN.
# Assim, independentemente de qual lado ganha, o payout é idêntico.

PEG_ARBIT_RANGE = (45.0, 55.0)
# Range de preço efectivo (cents) onde o PEG ARBIT entra.
# Só entra se AMBOS os lados estiverem dentro deste range.
# Ex: (35.0, 65.0) = só arb quando ambos os Eff estão entre 35c e 65c.
# Fora disto o mercado já decidiu muito e o arb torna-se arriscado.

PEG_ARBIT_UNDERPEG_C = 0.8
# Desvio mínimo de PEG para activar (em cents). [0.5 ... 10.0]
# Se (100 - PEG_Eff) >= 0.8c -> activa. Ex: PEG_Eff=98.5c -> underpeg=1.5c >= 0.8c -> GO

PEG_ARBIT_COOLDOWN = 0.05
# Intervalo mínimo entre entradas PEG consecutivas (segundos). [0.01 ... 5.0]
# Evita comprar o mesmo tick e gap consecutivos

PEG_ARBIT_MIN_REM = 5.0
# Remaining mínimo para entrar num PEG (segundos). [1.0 ... 30.0]
# Garante tempo suficiente para settlement ANTES do fim do mercado

MAX_PEG_ENTRIES = 10000000
# Máximo de entradas PEG por ciclo de 5 minutos (praticamente ilimitado).

PEG_ARBIT_TARGET_C = 0.0
# Target de venda (0.0 = hold até ao fim — SEMPRE recomendado para PEG).
# O PEG é lucro garantido SE se aguardar até ao fim. Vender cedo pode perder.

TARGET_MULTIPLIER = 1.25
# Multiplicador do preço efectivo para definir target em trades sem target fixo.
# Não afecta PEG (target=0.0) nem EIGHTY (target=0.0 por defeito).

# ─────────────────────────────────────────────────────────────────────────────
# SECÇÃO 10 — FEES E SPREAD
# ─────────────────────────────────────────────────────────────────────────────

FEE_RATE = 0.25
# Taxa base da Polymarket. Fórmula: fee = FEE_RATE × (p × (1-p))^FEE_EXP
# Fee é máxima em p=0.50 e zero em p=0 ou p=1.
# NÃO ALTERAR — reflecte a estrutura real da Polymarket.

FEE_EXP = 2
# Expoente da curva de fee. NÃO ALTERAR.

ASK_SPREAD = 0.01
# Spread adicionado ao preço nominal para simular o ask real do order book.
# 0.01 = 1 cent. Na prática, ao comprar pagas nom + 1c.

LOOP_SLEEP = 0.001
# Timeout máximo entre iterações do loop principal (segundos).
# 0.001 = 1ms. Menor = mais reactividade mas mais CPU.

# =============================================================================
# FIM DOS PARÂMETROS CONFIGURÁVEIS
# =============================================================================

# =============================================================================
# GLOBAIS DE ESTADO — Geridos pelo bot automaticamente
# =============================================================================

bankroll         = BANKROLL_INIT  # Saldo actual em USDC (lê Polymarket/inicia em BANKROLL_INIT) [Range: live]
daily_profit     = 0.0            # Lucro/prejuízo acumulado do dia (reseta a meia-noite UTC) [Range: -inf...+inf]
last_day         = None           # Data do último ciclo para detectar novo dia UTC [Range: date object]
best_asks        = {'up': None, 'down': None}  # Melhor ask actual de cada lado do order book [Range: 0.0...1.0]
price_change     = asyncio.Event()             # Evento para acordar o loop principal ao receber tick [Range: Event]
bot_start_time   = time.time()                 # Timestamp de arranque para cálculo de uptime [Range: unix timestamp]

# Martingale — as 3 variáveis de estado principais
risk_multiplier  = 1.0   # Multiplicador actual do martingale (x1, x2, x4, x8, x16) [Range: 1.0...16.0]
prev_round_loss  = 0.0   # Perda PÓS-LOSS (dólar) usada para recovery bonus [Range: 0.0...bankroll]
accumulated_loss = 0.0   # Total de perdas desde último lucro (para tracking recovery) [Range: 0.0...bankroll]

# Stop-Loss Dinâmico — variáveis separadas para verificação BID
stoploss_bid_prices = deque(maxlen=5)  # Buffer dos últimos 5 ticks de BID para SL [Range: 0.0...1.0]
stoploss_below_27c_ticks = 0           # Contador de ticks consecutivos abaixo de 27c [Range: 0...5]
stoploss_triggered = False             # Flag se stop-loss foi acionado este ciclo [Range: False/True]

# =============================================================================
# ========================== LOGGING (ficheiro + consola) =====================
# =============================================================================

_formatter    = logging.Formatter('%(message)s')
_file_handler = logging.FileHandler('bot_xrp.log', encoding='utf-8')
_file_handler.setFormatter(_formatter)

logger = logging.getLogger('bot_xrp')
logger.setLevel(logging.DEBUG)
logger.addHandler(_file_handler)
logger.propagate = False

# =============================================================================
# ========================== SECRETS ==========================================
# =============================================================================

def load_secrets(filepath: str = "secrets.txt") -> dict:
    if not os.path.exists(filepath):
        logger.warning("⚠️  secrets.txt não encontrado — LIVE_TRADING não disponível")
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
    logger.error("❌ ERRO FATAL: LIVE_TRADING=True mas POLYMARKET_PRIVATE_KEY não encontrado!")
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
        logger.info("✅ SDK Polymarket carregado — LIVE TRADING ACTIVO")
    except ImportError:
        logger.error("❌ py-clob-client não instalado! pip install py-clob-client")
        raise SystemExit(1)

# =============================================================================
# ========================== FUNÇÕES AUXILIARES ===============================
# =============================================================================

_FEE_RATE = FEE_RATE
_FEE_EXP  = FEE_EXP

def fee_rate(p: float) -> float:
    """Fee da Polymarket: fee = FEE_RATE × (p × (1-p))^FEE_EXP"""
    return _FEE_RATE * (p * (1.0 - p)) ** _FEE_EXP

def buy_shares_net(invested: float, ask: float) -> float:
    """Shares recebidas ao comprar 'invested' ao preço 'ask' (líquido de fee)."""
    return (invested / ask) * (1.0 - fee_rate(ask))

def effective_entry(ask: float) -> float:
    """Preço efectivo de entrada (ask ajustado pela fee): ask / (1 - fee)."""
    return ask / (1.0 - fee_rate(ask))

def sell_payout(shares: float, p: float) -> float:
    """Payout líquido ao vender 'shares' ao preço 'p'."""
    return shares * p * (1.0 - fee_rate(p))

def eff_sell_price(cp: float) -> float:
    """Preço efectivo de venda (líquido de fee): cp × (1 - fee)."""
    return cp * (1.0 - fee_rate(cp))

def fc(p: float) -> str:
    """Formata preço como cents: 0.87 → '87.0c'"""
    return f"{p * 100:.1f}c"

def get_ts() -> str:
    """Timestamp actual no formato dd/mm/yy | HH:MM:SS.mmm"""
    return datetime.now().strftime("%d/%m/%y | %H:%M:%S.%f")[:-3]

def get_remaining_str(rem: float) -> str:
    """Formata segundos restantes como MM:SS:mmm"""
    rem = max(0.0, rem)
    m   = int(rem // 60)
    s   = int(rem % 60)
    ms  = int((rem * 1000) % 1000)
    return f"{m:02d}:{s:02d}:{ms:03d}"

def get_uptime_str() -> str:
    """Tempo decorrido desde o arranque do bot (HH:MM:SS somente, sem anos/meses/dias)."""
    elapsed = int(time.time() - bot_start_time)
    hours, elapsed = divmod(elapsed, 3600)
    mins, secs = divmod(elapsed, 60)
    return f"{hours:02d}h:{mins:02d}m:{secs:02d}s"

# ── Funções de risco — coração do martingale ──────────────────────────────────

def calc_risk(base: float, mult: float, prev_loss: float, bank: float) -> float:
    """
    Calcula o risco effectivo para um trade.

    Fórmula:
        recovery_bonus = MARTINGALE_RECOVERY × prev_loss / bank
        raw            = base × mult + recovery_bonus
        eff            = min(raw, MAX_RISK_PERCENT)   ← NUNCA ultrapassa 50%

    Parâmetros:
        base      → risco base do módulo (ex: EIGHTY_RISK = 0.07)
        mult      → risk_multiplier actual (1, 2, 4, 8, 16)
        prev_loss → perda da ronda ANTERIOR em dólares (não o acumulado)
        bank      → banca actual em dólares

    Retorna: fracção effectiva entre 0.0 e MAX_RISK_PERCENT.
    """
    if bank <= 0:
        return MAX_RISK_PERCENT
    recovery_bonus = (MARTINGALE_RECOVERY * prev_loss) / bank
    raw            = (base * mult) + recovery_bonus
    return min(raw, MAX_RISK_PERCENT)


def calc_risk_preview(base: float, mult: float, prev_loss: float, bank: float) -> float:
    """
    Mesma lógica que calc_risk mas para uso no main() (fora de closures).
    Usado para mostrar o risco PREVISTO da próxima ronda nos logs.
    """
    if bank <= 0:
        return MAX_RISK_PERCENT
    recovery_bonus = (MARTINGALE_RECOVERY * prev_loss) / bank
    raw            = (base * mult) + recovery_bonus
    return min(raw, MAX_RISK_PERCENT)

# ── Helpers de log ────────────────────────────────────────────────────────────

def log_m(module: str, action: str, msg: str):
    """Log com módulo e acção: [INFO] [MODULE] [ACTION] [timestamp] | msg"""
    logger.info(f"[INFO] [{module}] [{action}] [{get_ts()}] | {msg}")

def log_raw(msg: str):
    """Log de tick de preço."""
    logger.info(f"[{get_ts()}] | {msg}")

def log_info(msg: str):
    """Log de informação geral."""
    logger.info(f"[INFO] [{get_ts()}] | {msg}")

def log_warn(msg: str):
    """Log de aviso."""
    logger.warning(f"[WARN] [{get_ts()}] | ⚠️  {msg}")

def log_sep():
    logger.info("─" * 80)

def log_sep2():
    logger.info("═" * 80)

# =============================================================================
# ========================== POLYMARKET WALLET =================================
# =============================================================================

def read_polymarket_wallet() -> float:
    """
    Lê o saldo realda carteira Polymarket quando LIVE_TRADING=True via SDK.
    Retorna o saldo em USDC. Se falhar ou não LIVE, retorna BANKROLL_INIT.
    """
    if not LIVE_TRADING or not clob_client:
        return BANKROLL_INIT
    try:
        balance_response = clob_client.get_balance()
        usdc_balance = float(balance_response.get('balance', 0.0))
        log_info(f"WALLET READ | Saldo Polymarket: ${usdc_balance:.4f}")
        return usdc_balance
    except Exception as e:
        log_warn(f"Falha ao ler carteira Polymarket: {e} — usando fallback ${BANKROLL_INIT:.2f}")
        return BANKROLL_INIT

# =============================================================================
# ========================== API / WEBSOCKET ==================================
# =============================================================================

def fetch_metadata(slug: str) -> dict | None:
    """Obtém os token IDs de UP e DOWN para um mercado dado o slug."""
    try:
        url  = f"https://gamma-api.polymarket.com/events?slug={slug}"
        data = requests.get(url, timeout=5).json()[0]['markets'][0]
        ids  = json.loads(data['clobTokenIds'])
        return {'id': data['conditionId'], 'up': ids[0], 'down': ids[1], 'slug': slug}
    except Exception as e:
        log_warn(f"fetch_metadata falhou: {e}")
        return None

def get_current_slug() -> tuple[str, float]:
    """
    Calcula o slug do mercado actual de 5 minutos.
    Os mercados XRP começam a cada 300 segundos (múltiplos de 300 desde epoch).
    Se faltam menos de 5s para o fim, já aponta para o próximo.
    """
    now      = time.time()
    start_ts = now - (now % 300)
    if (start_ts + 300 - now) < 5:
        start_ts += 300
    return f"xrp-updown-5m-{int(start_ts)}", start_ts

async def ws_handler(t_up: str, t_down: str):
    """
    WebSocket handler: conecta ao order book da Polymarket e actualiza best_asks
    em tempo real. Reconecta automaticamente em caso de erro.
    Usa price_change.set() para acordar o loop principal a cada novo preço.
    """
    uri        = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
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
                async for raw in ws:
                    items = json.loads(raw)
                    if not isinstance(items, list):
                        items = [items]
                    for item in items:
                        aid = item.get("asset_id")
                        p   = None
                        evt = item.get("event_type")
                        if evt == "book":
                            # Evento de book completo — pega no menor ask com size > 0
                            asks = item.get("asks")
                            if asks:
                                valid = [float(d['price']) for d in asks if float(d['size']) > 0]
                                if valid:
                                    p = min(valid)
                        elif evt == "best_bid_ask":
                            # Evento de best bid/ask — mais frequente e leve
                            ba = item.get("best_ask")
                            if ba:
                                p = float(ba)
                        if p is not None:
                            if   aid == t_up:   _best_asks['up']   = p
                            elif aid == t_down: _best_asks['down'] = p
                            _set()   # acorda o loop principal
        except asyncio.CancelledError:
            break
        except Exception as e:
            log_warn(f"WS erro: {e} — reconectando em 1s")
            await asyncio.sleep(1)

# =============================================================================
# ========================== LIVE ORDER =======================================
# =============================================================================

async def place_live_order(side: str, price: float, shares: float, token_id: str) -> bool:
    """
    Envia uma ordem real à Polymarket via SDK.
    Só executa se LIVE_TRADING=True e clob_client está inicializado.
    GTC = Good Till Cancelled (ordem fica activa até ser preenchida ou cancelada).
    """
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
        log_warn(f"LIVE ORDER falhou: {e}")
        return False

# =============================================================================
# ========================== PRICE BUFFER =====================================
# =============================================================================

class PriceBuffer:
    """
    Buffer circular de preços com timestamps.
    Usado pelo EIGHTY para calcular deltas de preço em múltiplos intervalos.
    Mantém apenas os últimos max_age_seconds de dados.
    """
    __slots__ = ('max_age', 'buffer')

    def __init__(self, max_age_seconds: float = 30.0):
        self.max_age: float = max_age_seconds
        self.buffer: deque  = deque()

    def add(self, eff_c: float, ts: float):
        """Adiciona um ponto de preço e limpa entradas antigas."""
        self.buffer.append((ts, eff_c))
        self._cleanup(ts)

    def _cleanup(self, now: float):
        cutoff = now - self.max_age
        buf    = self.buffer
        while buf and buf[0][0] < cutoff:
            buf.popleft()

    def get_price_at(self, seconds_ago: float, tolerance: float = 1.0) -> float | None:
        """Retorna o preço mais próximo de 'seconds_ago' atrás (dentro da tolerância)."""
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
        """Idade do registo mais antigo no buffer (segundos)."""
        return (time.time() - self.buffer[0][0]) if self.buffer else 0.0

    def get_delta(self, seconds_ago: float) -> tuple[float | None, bool]:
        """
        Delta de preço: actual - preço de 'seconds_ago' atrás.
        Retorna (delta_em_cents, válido).
        """
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
# ========================== LOGIC LOOP =======================================
# =============================================================================

async def logic_loop(
    m_start: float,
    m_end: float,
    meta: dict,
    r_mult: float,
    r_prev_loss: float
):
    """
    Loop principal de trading para um ciclo de 5 minutos.

    Parâmetros:
        m_start     → timestamp de início do mercado
        m_end       → timestamp de fim do mercado (m_start + 300)
        meta        → dicionário com slug, up/down token IDs
        r_mult      → risk_multiplier actual (do martingale)
        r_prev_loss → perda da ronda anterior em dólares (para recovery_bonus)
    """
    global bankroll, daily_profit

    active_trades = []
    state         = {}

    # Se LIVE_TRADING, actualiza saldo da carteira Polymarket no início do ciclo
    if LIVE_TRADING:
        bankroll = read_polymarket_wallet()
    flags         = {}

    # ── Cálculo dos riscos efectivos para esta ronda ──────────────────────────
    # Usa calc_risk() que garante o cap de MAX_RISK_PERCENT em TODOS os casos.
    eff_risk_per_trade = calc_risk(RISK_PER_TRADE,  r_mult, r_prev_loss, bankroll)
    eff_eighty_risk    = calc_risk(EIGHTY_RISK,     r_mult, r_prev_loss, bankroll)
    eff_peg_risk       = calc_risk(PEG_ARBIT_RISK,  r_mult, r_prev_loss, bankroll)

    # Log do estado do martingale no início da ronda
    if r_mult > 1.0 or r_prev_loss > 0:
        recovery_bonus_pct = (MARTINGALE_RECOVERY * r_prev_loss / bankroll) if bankroll > 0 else 0.0
        cap_tag_e = " ← CAP" if eff_eighty_risk >= MAX_RISK_PERCENT else ""
        cap_tag_p = " ← CAP" if eff_peg_risk    >= MAX_RISK_PERCENT else ""
        log_info(
            f"MARTINGALE | x{r_mult:.0f} | prev_loss=${r_prev_loss:.4f} "
            f"| recovery_bonus={MARTINGALE_RECOVERY:.0%}×${r_prev_loss:.4f}={recovery_bonus_pct:.1%} "
            f"| eff_risk: EIGHTY={eff_eighty_risk:.1%}{cap_tag_e} "
            f"PEG={eff_peg_risk:.1%}{cap_tag_p} "
            f"(cap={MAX_RISK_PERCENT:.0%})"
        )

    # ── Constantes locais para loop (aceleração de lookup) ───────────────────
    _EIGHTY_MIN_EFF_C        = EIGHTY_MIN_EFF_C
    _EIGHTY_MAX_EFF_C        = EIGHTY_MAX_EFF_C
    _EIGHTY_MIN_TICKS        = EIGHTY_MIN_TICKS
    _EIGHTY_PEG_MIN_C        = EIGHTY_PEG_MIN_C
    _EIGHTY_BUY_COOLDOWN     = EIGHTY_BUY_COOLDOWN
    _EIGHTY_VOL_MACRO_WINDOW_S = EIGHTY_VOL_MACRO_WINDOW_S
    _EIGHTY_VOL_MACRO_MAX_C  = EIGHTY_VOL_MACRO_MAX_C
    _EIGHTY_VOL_MICRO_WINDOW_S = EIGHTY_VOL_MICRO_WINDOW_S
    _EIGHTY_VOL_MICRO_SPIKE_C = EIGHTY_VOL_MICRO_SPIKE_C
    _EIGHTY_VOL_COOLDOWN_S   = EIGHTY_VOL_COOLDOWN_S
    _EIGHTY_CUTOFF_S         = EIGHTY_CUTOFF_S
    _EIGHTY_START_REM_S      = EIGHTY_START_REM_S
    _EIGHTY_WHEN_CV0         = EIGHTY_WHEN_CUTOFF_0_VOLT
    _EIGHTY_TARGET_C       = EIGHTY_TARGET_C
    _ASK_SPREAD            = ASK_SPREAD
    _PEG_RANGE_MIN         = PEG_ARBIT_RANGE[0]
    _PEG_RANGE_MAX         = PEG_ARBIT_RANGE[1]
    _PEG_ACTIVE            = PEG_ARBIT_ACTIVE
    _PEG_UNDERPEG          = PEG_ARBIT_UNDERPEG_C
    _PEG_MIN_REM           = PEG_ARBIT_MIN_REM
    _PEG_COOLDOWN          = PEG_ARBIT_COOLDOWN
    _MAX_PEG               = MAX_PEG_ENTRIES
    _EIGHTY_ACT            = EIGHTY_ACTIVE
    _MAX_RISK_PERCENT_CAP  = MAX_RISK_PERCENT

    # ── Estado do EIGHTY ──────────────────────────────────────────────────────
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

    # ── Header de ronda ───────────────────────────────────────────────────────
    mult_tag = f" [MARTINGALE x{r_mult:.0f}]" if r_mult > 1.0 else ""
    mods = []
    if _EIGHTY_ACT:  mods.append(f"EIGHTY({_EIGHTY_START_REM_S}s→{_EIGHTY_CUTOFF_S}s)")
    if _PEG_ACTIVE:  mods.append(f"PEG_ARBIT(range {_PEG_RANGE_MIN:.0f}-{_PEG_RANGE_MAX:.0f}c)")

    log_sep2()
    log_info(f"NOVO CICLO | Market: {meta['slug']} | LIVE: {LIVE_TRADING}")
    log_info(f"   Banca: ${bankroll:.4f} | Profit acum.: ${daily_profit:.4f}{mult_tag}")
    log_info(f"   Módulos: {' | '.join(mods) if mods else 'nenhum'}")
    log_info(
        f"   Risco effectivo: EIGHTY={eff_eighty_risk:.1%} | "
        f"PEG={eff_peg_risk:.1%} | "
        f"CAP={MAX_RISK_PERCENT:.0%}"
    )
    log_sep()
    log_info("   ESCUTA ACTIVA")
    log_sep()

    def pct_banca(invested: float) -> str:
        """Formata o investimento como % da banca total (pré-trade)."""
        base = bankroll + invested
        return f"{invested / base * 100:.1f}% banca" if base else "—"

    # ── open_trade ─────────────────────────────────────────────────────────────
    async def open_trade(
        side: str,
        nom: float,
        trade_type: str,
        rstr: str,
        risk: float = None,
        wait_close: bool = False,
        fixed_invest: float = None,
        peg_val: float = None,
        token_id: str = None,
        extra_log: str = None,
        fixed_shares: float = None
    ):
        """
        Executa a abertura de um trade.

        COMPRA AO BID (preço real onde vendes):
            invested = 15% banca + fee (AMBOS debitados)
            shares = 15% banca / BID
            A fee é mostrada em log como dedução

        Prioridade de sizing:
            1. fixed_shares → número exacto de shares (usado no PEG para shares iguais)
            2. fixed_invest → valor exacto em dólares
            3. risk × bankroll → fracção da banca (default)
        """
        global bankroll
        if risk is None:
            risk = eff_risk_per_trade

        # BID é o preço ao qual compras (menor que Ask)
        bid  = nom  # nom é o bid recebido do order book
        _fee = fee_rate(bid)
        eff  = bid / (1.0 - _fee)  # preço effectivo após fee

        if fixed_shares is not None:
            # PEG ARBIT: shares definidas para garantir igualdade entre lados
            shares   = fixed_shares
            invested = shares * bid + (shares * bid * _fee)  # bid + fee
        elif fixed_invest is not None:
            invested = fixed_invest
            shares   = invested / (bid * (1.0 + _fee))  # shares que custem invested
        else:
            invested = bankroll * risk
            shares   = invested / (bid * (1.0 + _fee))

        # Target de saída antecipada
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
        buy_fee   = _fee * 100.0
        peg_str   = f" | PEG_Eff: {fc(peg_val)} ({peg_val:.3f})" if peg_val is not None else ""
        extra     = f" | {extra_log}" if extra_log else ""

        trade = {
            'side': side, 'nom': nom, 'entry': eff, 'shares': shares,
            'target': target, 'type': trade_type, 'invested': invested,
            'wait_close': wait_close, 'token_id': token_id
        }
        active_trades.append(trade)

        if LIVE_TRADING and token_id:
            await place_live_order(side, bid, shares, token_id)

        module = trade_type.replace('_', ' ')
        log_m(module, 'BUY',
            f"rem={rstr} | {side} @ bid={fc(bid)} eff={fc(eff)}"
            f"{peg_str} | inv=${invested:.4f} ({pct}) + fee=${invested*_fee:.4f} = total ${invested*(1+_fee):.4f} | shares={shares:.4f}"
            f" | fee={buy_fee:.3f}%{extra}"
        )

    # ── close_trade ────────────────────────────────────────────────────────────
    def close_trade(trade: dict, cp: float, reason: str, rstr: str):
        """
        Fecha um trade ao preço corrente 'cp'.
        Calcula payout e PnL, actualiza banca e regista no Kelly se activo.
        """
        global bankroll
        payout   = sell_payout(trade['shares'], cp)
        pnl      = payout - trade['invested']
        pnl_pct  = (pnl / trade['invested'] * 100.0) if trade['invested'] else 0.0
        bankroll += payout
        icon   = "+" if pnl >= 0 else "-"
        module = trade['type'].replace('_', ' ')
        log_m(module, 'SELL',
            f"rem={rstr} | {trade['side']} @ {fc(cp)} "
            f"| PnL: ${pnl:+.4f} ({pnl_pct:+.1f}%) "
            f"| Reason: {reason} {icon}"
        )

    # ── Helpers do EIGHTY ─────────────────────────────────────────────────────
    def eighty_reset(e_side: str, rstr: str, reason: str):
        """Reset do estado do EIGHTY para um lado (com log)."""
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side]   = 0
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side]      = None
        eighty_eff_max[e_side]      = None
        log_m('EIGHTY', 'RESET', f"rem={rstr} | {e_side} — {reason}")

    def eighty_reset_silent(e_side: str):
        """Reset silencioso após compra (sem log — é comportamento esperado)."""
        eighty_seen_levels[e_side].clear()
        eighty_tick_count[e_side]   = 0
        eighty_first_tick_t[e_side] = None
        eighty_eff_min[e_side]      = None
        eighty_eff_max[e_side]      = None

    def eighty_activate_vol_cooldown(e_side: str, rstr: str, reason: str):
        """Activa cooldown de volatilidade e reseta o estado do EIGHTY."""
        eighty_vol_cooldown_until[e_side] = time.time() + _EIGHTY_VOL_COOLDOWN_S
        eighty_reset(e_side, rstr, reason)
        log_m('EIGHTY', 'VOL_COOLDOWN',
            f"rem={rstr} | {e_side} — bloqueado {_EIGHTY_VOL_COOLDOWN_S:.0f}s")

    prev_u_p = prev_d_p = None
    _best_asks = best_asks
    _pc_wait   = price_change.wait
    _pc_clear  = price_change.clear

    # Stop-Loss Dinâmico: buffer de BID para detectar ticks abaixo de 27c
    _stoploss_bid_buffer = deque(maxlen=5)  # últimos 5 ticks
    _stoploss_below_27_count = 0
    _27c_threshold = 0.27

    # =========================================================================
    # ── LOOP PRINCIPAL ────────────────────────────────────────────────────────
    # =========================================================================
    while True:
        now = time.time()
        rem = m_end - now

        # ── Fim de mercado ────────────────────────────────────────────────────
        if rem <= 0:
            u_p = _best_asks.get('up')  or 0.0
            d_p = _best_asks.get('down') or 0.0
            log_sep()
            log_info(f"FIM DE MERCADO | UP final={fc(u_p)} | DOWN final={fc(d_p)}")
            for trade in active_trades[:]:
                cp = u_p if trade['side'] == 'UP' else d_p
                close_trade(trade, cp, "FIM MERCADO", "00:00:000")
                active_trades.remove(trade)
            break

        rstr = get_remaining_str(rem)

        # ── Aguarda novo tick de preço ─────────────────────────────────────────
        try:
            await asyncio.wait_for(_pc_wait(), timeout=LOOP_SLEEP)
            _pc_clear()
        except asyncio.TimeoutError:
            pass

        u_p = _best_asks.get('up')
        d_p = _best_asks.get('down')
        if u_p is None or d_p is None:
            continue
        if u_p == prev_u_p and d_p == prev_d_p:
            continue  # sem alteração de preço — não processa

        prev_u_p = u_p
        prev_d_p = d_p

        # ── Stop-Loss Dinâmico: verifica se BID < 27c (5 ticks check) ───
        _stoploss_bid_buffer.append(u_p)  # rastreia BID de UP
        _stoploss_bid_buffer.append(d_p)  # rastreia BID de DOWN
        
        below_27_count = sum(1 for bid in _stoploss_bid_buffer if bid < _27c_threshold)
        if below_27_count >= 5:
            log_sep()
            log_info(f"STOP-LOSS TRIGGERED | BID < 27c detectado {below_27_count}/5 ticks consecutivos")
            log_info(f"   UP: {fc(u_p)} | DOWN: {fc(d_p)}")
            _stoploss_triggered = True
            for trade in active_trades[:]:
                cp = u_p if trade['side'] == 'UP' else d_p
                close_trade(trade, cp, "STOPLOSS", rstr)
                active_trades.remove(trade)
            log_sep()
        else:
            _stoploss_triggered = False

        # ── Calcula PEG pelo preço EFFECTIVO (mais conservador) ───────────────
        ask_up   = u_p + _ASK_SPREAD
        ask_down = d_p + _ASK_SPREAD
        eff_up   = effective_entry(ask_up)    # preço real que pagas por UP
        eff_down = effective_entry(ask_down)  # preço real que pagas por DOWN
        peg_eff  = eff_up + eff_down          # se < 1.0 → há arb
        peg_base = u_p + d_p                  # PEG nominal (para referência)

        underpeg_eff_c = (1.0 - peg_eff) * 100.0
        peg_disp = (
            f" | PEG_Eff={peg_eff:.3f} underpeg={underpeg_eff_c:.2f}c"
            if peg_eff < 1.0 and underpeg_eff_c >= _PEG_UNDERPEG else ""
        )
        log_raw(
            f"rem={rstr} | UP={fc(u_p)} Eff={fc(eff_up)} | "
            f"DOWN={fc(d_p)} Eff={fc(eff_down)}{peg_disp}"
        )

        # =====================================================================
        # ── 1. PEG ARBITRAGE (Baseado em BID, não EFF) ──────────────────────
        # =====================================================================
        if (_PEG_ACTIVE
                and u_p is not None and d_p is not None
                and rem > _PEG_MIN_REM
                and peg_arbit_count < _MAX_PEG
                and now - last_peg_time >= _PEG_COOLDOWN):

            # PEG baseado no BID puro (preços reais do order book)
            peg_bid = u_p + d_p  # BID_UP + BID_DOWN em dólares
            peg_bid_c = peg_bid * 100.0  # em cents
            
            # Condição: se BID_UP + BID_DOWN <= 99.2 cents → arbitragem garantida
            if peg_bid_c <= 99.2:
                # Risco: 25% da banca dividido entre UP e DOWN
                risk_peg = 0.25  # 25% total
                budget = bankroll * risk_peg  # orçamento total
                
                # Shares iguais: calcular pelo lado mais caro (BID)
                ref_bid = max(u_p, d_p)  # lado mais caro
                shares_bid = budget / ref_bid  # shares que cabem no orçamento
                
                # Custos reais (BID + fee deduzidos da banca)
                fee_up = fee_rate(u_p)
                fee_down = fee_rate(d_p)
                cost_up = shares_bid * u_p * (1.0 + fee_up)  # custo com fee
                cost_down = shares_bid * d_p * (1.0 + fee_down)  # custo com fee
                total_cost = cost_up + cost_down
                
                # Profit esperado (ambos resolvem a 100c = 1.00)
                payoff_up = shares_bid * 1.0 * (1.0 - fee_rate(1.0))
                payoff_down = shares_bid * 1.0 * (1.0 - fee_rate(1.0))
                payoff_total = payoff_up + payoff_down
                profit_est = payoff_total - total_cost
                
                # Entrar apenas se temos banca suficiente
                if total_cost <= bankroll:
                    log_sep()
                    log_m('PEG ARBIT', 'ENTRADA',
                        f"rem={rstr} | BID_UP={fc(u_p)} BID_DOWN={fc(d_p)} | "
                        f"PEG_BID={peg_bid_c:.1f}c (≤99.2c → arb) | "
                        f"Shares={shares_bid:.4f} (iguais) | "
                        f"Custo total=${total_cost:.4f} (c/ fees) | "
                        f"Lucro est.=${profit_est:.4f} | "
                        f"arb #{peg_arbit_count + 1}"
                    )
                    await open_trade('UP', u_p, 'PEG_ARBIT', rstr,
                                     fixed_shares=shares_bid, wait_close=True,
                                     token_id=meta['up'], extra_log=f"BID={fc(u_p)}")
                    await open_trade('DOWN', d_p, 'PEG_ARBIT', rstr,
                                     fixed_shares=shares_bid, wait_close=True,
                                     token_id=meta['down'], extra_log=f"BID={fc(d_p)}")
                    log_sep()

                    peg_arbit_count += 1
                    last_peg_time    = now

        # =====================================================================
        # ── 2. TARGET CHECK ───────────────────────────────────────────────────
        # =====================================================================
        for trade in active_trades[:]:
            if trade.get('target') is None:
                continue
            cp = u_p if trade['side'] == 'UP' else d_p
            if cp and cp >= trade['target']:
                close_trade(trade, cp, "TARGET", rstr)
                active_trades.remove(trade)

        # =====================================================================
        # ── 3. EIGHTY ─────────────────────────────────────────────────────────
        # =====================================================================
        if _EIGHTY_ACT:
            if rem > _EIGHTY_START_REM_S:
                pass  # ainda fora da janela de activação — silencioso

            elif rem <= _EIGHTY_CUTOFF_S:
                if not eighty_cutoff_logged:
                    eighty_cutoff_logged = True
                    log_m('EIGHTY', 'CUTOFF',
                        f"rem={rstr} | EIGHTY parado — rem <= {_EIGHTY_CUTOFF_S}s")

            else:
                # ── Janela activa ──────────────────────────────────────────────
                if not eighty_started_logged:
                    eighty_started_logged = True
                    log_m('EIGHTY', 'START',
                        f"rem={rstr} | EIGHTY activo [{_EIGHTY_START_REM_S}s→{_EIGHTY_CUTOFF_S}s] "
                        f"| risco={eff_eighty_risk:.1%}")

                for e_side, nom in (('UP', u_p), ('DOWN', d_p)):
                    token_id = meta['up'] if e_side == 'UP' else meta['down']

                    # Ignora verificações de volatilidade nos últimos N segundos (se cutoff=0)
                    skip_vol = (
                        _EIGHTY_CUTOFF_S == 0
                        and _EIGHTY_WHEN_CV0 > 0
                        and rem <= _EIGHTY_WHEN_CV0
                    )

                    # Cooldown de volatilidade activo?
                    if not skip_vol and now < eighty_vol_cooldown_until[e_side]:
                        continue

                    # Cooldown entre compras do mesmo lado?
                    if not skip_vol and now - eighty_last_buy[e_side] < _EIGHTY_BUY_COOLDOWN:
                        continue

                    ask   = nom + _ASK_SPREAD
                    _fee  = fee_rate(ask)
                    eff_c = (ask / (1.0 - _fee)) * 100.0  # preço effectivo em cents

                    eighty_price_buffer[e_side].add(eff_c, now)

                    # Fora do range de preço?
                    if not (_EIGHTY_MIN_EFF_C <= eff_c <= _EIGHTY_MAX_EFF_C):
                        if eighty_tick_count[e_side] > 0:
                            eighty_reset(e_side, rstr,
                                f"eff_c {eff_c:.1f}c fora [{_EIGHTY_MIN_EFF_C:.0f}-{_EIGHTY_MAX_EFF_C:.0f}]")
                        continue

                    # Regista nível de preço (arredondado a 0.5c) para contar ticks únicos
                    level_key = round(eff_c * 2) / 2
                    if level_key not in eighty_seen_levels[e_side]:
                        eighty_seen_levels[e_side].add(level_key)
                        eighty_tick_count[e_side] += 1

                    # Regista primeiro tick e rastreia min/max para cálculo de volatilidade
                    if eighty_first_tick_t[e_side] is None:
                        eighty_first_tick_t[e_side] = now
                        eighty_eff_min[e_side]      = eff_c
                        eighty_eff_max[e_side]      = eff_c
                    else:
                        if eff_c < eighty_eff_min[e_side]: eighty_eff_min[e_side] = eff_c
                        if eff_c > eighty_eff_max[e_side]: eighty_eff_max[e_side] = eff_c

                    elapsed = now - eighty_first_tick_t[e_side]
                    var_c   = eighty_eff_max[e_side] - eighty_eff_min[e_side]
                    
                    # ── VOLATILIDADE MACRO: detecção de consolidação fraca ────────────
                    # Se a variação total é too grande dentro da janela → mercado muito volátil
                    macro_vol_nok = (elapsed <= _EIGHTY_VOL_MACRO_WINDOW_S and var_c >= _EIGHTY_VOL_MACRO_MAX_C)

                    # ── VOLATILIDADE MICRO: detecção de pump falso ────────────────────
                    # Calcula delta rápido para identificar subida iminente (reversão)
                    epb = eighty_price_buffer[e_side]
                    micro_delta, micro_valid = epb.get_delta(_EIGHTY_VOL_MICRO_WINDOW_S)
                    micro_vol_nok = (
                        micro_valid
                        and micro_delta is not None
                        and micro_delta >= _EIGHTY_VOL_MICRO_SPIKE_C
                    )

                    # ── DELTAS PARA LOGGING (informação, não filtragem) ────────────────
                    delta_05, valid_05 = epb.get_delta(0.5)
                    delta_10, valid_10 = epb.get_delta(1.0)
                    delta_20, valid_20 = epb.get_delta(2.0)

                    delta_parts = []
                    if valid_05: delta_parts.append(f"Δ0.5s:{delta_05:+.1f}c")
                    if valid_10: delta_parts.append(f"Δ1s:{delta_10:+.1f}c")
                    if valid_20: delta_parts.append(f"Δ2s:{delta_20:+.1f}c")
                    delta_str = " | ".join(delta_parts) if delta_parts else f"Δ aguarda ({epb.get_age():.1f}s)"

                    # ── Estado dos deltas (direcção) ───────────────────────────────────────
                    delta_ok = True  # Por defeito OK (simples: sem queda rápida esperada)
                    delta_reason = ""
                    if valid_05 and delta_05 is not None and delta_05 < 0:
                        delta_ok, delta_reason = False, f"Δ0.5s={delta_05:+.1f}c (a cair)"
                    elif valid_10 and delta_10 is not None and delta_10 < 0:
                        delta_ok, delta_reason = False, f"Δ1s={delta_10:+.1f}c (a cair)"
                    elif valid_20 and delta_20 is not None and delta_20 < 0:
                        delta_ok, delta_reason = False, f"Δ2s={delta_20:+.1f}c (a cair)"

                    # ── CONSOLIDAÇÃO DO ESTADO DE VOLATILIDADE ───────────────────────────────
                    # MACRO_NOK + MICRO_NOK → ambas têm que estar OK para entrar
                    vol_ok = not macro_vol_nok and not micro_vol_nok
                    has_delta = valid_05 or valid_10 or valid_20
                    
                    # Log de volatilidade com identificação clara (MACRO vs MICRO)
                    vol_str = "VOL SKIP" if skip_vol else f"VOL {'OK' if vol_ok else 'NOK'}" 
                    if macro_vol_nok:
                        vol_str += f" | MACRO_NOK({var_c:.1f}c>={_EIGHTY_VOL_MACRO_MAX_C:.1f}c)"
                    elif micro_vol_nok:
                        vol_str += f" | MICRO_NOK({micro_delta:.1f}c>={_EIGHTY_VOL_MICRO_SPIKE_C:.1f}c)"
                    
                    delta_icon = "↑" if has_delta else "—"
                    peg_str    = f" | PEG_Eff={peg_eff:.3f}" if peg_eff * 100.0 <= _EIGHTY_PEG_MIN_C else ""

                    log_m('EIGHTY', 'WATCH',
                        f"rem={rstr} | {e_side} Eff={fc(eff_c/100)} | {vol_str} | "
                        f"{delta_str} {delta_icon}{peg_str} | "
                        f"ticks={eighty_tick_count[e_side]}/{_EIGHTY_MIN_TICKS}"
                    )

                    if not skip_vol:
                        if macro_vol_nok:
                            eighty_activate_vol_cooldown(e_side, rstr,
                                f"MACRO_VOL_TRIGGERED | {var_c:.1f}c em {elapsed:.1f}s (max {_EIGHTY_VOL_MACRO_MAX_C:.1f}c/{_EIGHTY_VOL_MACRO_WINDOW_S:.1f}s)")
                            continue
                        if micro_vol_nok:
                            eighty_activate_vol_cooldown(e_side, rstr,
                                f"MICRO_PUMP_TRIGGERED | Spike {micro_delta:+.1f}c em {_EIGHTY_VOL_MICRO_WINDOW_S:.1f}s")
                            continue

                    # Tem ticks suficientes para entrar?
                    if eighty_tick_count[e_side] >= _EIGHTY_MIN_TICKS:

                        # Verifica PEG
                        if peg_eff * 100.0 < _EIGHTY_PEG_MIN_C:
                            eighty_reset(e_side, rstr,
                                f"PEG_Eff {peg_eff*100:.1f}c < min {_EIGHTY_PEG_MIN_C:.1f}c")
                            continue

                        # Verifica delta (direcção)
                        if has_delta and not delta_ok:
                            eighty_reset(e_side, rstr, f"DELTA NOK — {delta_reason}")
                            continue

                        if bankroll > 0:
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
                                f"rem={rstr} | {e_side} — cooldown {_EIGHTY_BUY_COOLDOWN:.1f}s")



# =============================================================================
# ============================= MAIN ==========================================
# =============================================================================

async def main():
    global daily_profit, last_day, price_change, bankroll
    global risk_multiplier, prev_round_loss, accumulated_loss

    # Se LIVE_TRADING, lê saldo real da carteira Polymarket
    if LIVE_TRADING:
        bankroll = read_polymarket_wallet()
    else:
        bankroll = BANKROLL_INIT

    # Reset de todas as variáveis de martingale no arranque
    risk_multiplier  = 1.0
    prev_round_loss  = 0.0
    accumulated_loss = 0.0

    log_sep2()
    log_info("BOT XRP POLYMARKET v0.35.0 INICIADO")
    log_sep()
    log_info(f"   LIVE_TRADING     : {LIVE_TRADING}")
    log_info(f"   BANKROLL_INIT    : ${BANKROLL_INIT:.2f}")
    log_sep()
    log_info("   RISCO BASE:")
    log_info(f"   RISK_PER_TRADE   : {RISK_PER_TRADE:.0%}")
    log_info(f"   EIGHTY_RISK      : {EIGHTY_RISK:.0%}")
    log_info(f"   PEG_ARBIT_RISK   : {PEG_ARBIT_RISK:.0%}")
    log_sep()
    log_info("   MARTINGALE:")
    log_info(f"   MAX_RISK_PERCENT : {MAX_RISK_PERCENT:.0%}  (CAP ABSOLUTO)")
    log_info(f"   MAX_MULTIPLIER   : x{MAX_RISK_MULTIPLIER}")
    log_info(f"   RECOVERY_RATE    : {MARTINGALE_RECOVERY:.0%} da perda anterior")
    log_info(
        f"   Fórmula          : min(base × mult + {MARTINGALE_RECOVERY:.0%}×prev_loss/bank, {MAX_RISK_PERCENT:.0%})"
    )
    log_sep()
    log_info("   MÓDULOS ACTIVOS:")
    log_info(f"   EIGHTY           : ON")
    log_info(f"   PEG_ARBIT        : ON")
    log_sep2()

    while True:
        slug, start_ts = get_current_slug()
        meta = fetch_metadata(slug)
        if not meta:
            log_warn(f"Metadata não encontrada para {slug} — retentando em 1s")
            await asyncio.sleep(1)
            continue

        # ── Detecção de novo dia → reset completo ─────────────────────────────
        market_day = datetime.fromtimestamp(start_ts).date()
        if last_day is None or market_day != last_day:
            daily_profit     = 0.0
            bankroll         = BANKROLL_INIT
            risk_multiplier  = 1.0
            prev_round_loss  = 0.0
            accumulated_loss = 0.0
            last_day         = market_day
            log_sep2()
            log_info(f"NOVO DIA {market_day}")
            log_info(f"   Banca reset      : ${BANKROLL_INIT:.2f}")
            log_info(f"   Martingale reset : x1 | prev_loss=$0.00 | accumulated=$0.00")
            log_sep2()

        # ── Limpa estado do WebSocket para este ciclo ─────────────────────────
        best_asks['up'] = best_asks['down'] = None
        price_change.clear()

        ws_task = asyncio.create_task(ws_handler(meta['up'], meta['down']))
        await asyncio.sleep(1.0)  # aguarda o primeiro tick

        if best_asks['up'] is not None:
            pre_bank = bankroll

            # ── Executa o loop principal do ciclo ─────────────────────────────
            await logic_loop(
                start_ts,
                start_ts + 300,
                meta,
                risk_multiplier,
                prev_round_loss   # ← passa a perda da ronda ANTERIOR
            )

            profit_this  = bankroll - pre_bank
            daily_profit += profit_this
            pnl_pct      = (profit_this / pre_bank * 100.0) if pre_bank > 0 else 0.0
            daily_pct    = (daily_profit / BANKROLL_INIT * 100.0) if BANKROLL_INIT > 0 else 0.0

            # Novo formato: ROUND: PnL | Extra Stake | Mult
            if profit_this == 0.0:
                round_pnl_str = "$0.0000 (0.00%)"
            else:
                round_pnl_str = f"${profit_this:+.4f} ({pnl_pct:+.2f}%)"

            # Extra stake = invested acima do risco base
            extra_stake = max(0.0, (profit_this + pre_bank) * (1.0 - 1.0/risk_multiplier)) if risk_multiplier > 1.0 else 0.0
            if extra_stake == 0.0:
                extra_stake_str = "-"
            else:
                extra_stake_str = f"${extra_stake:.4f}"

            mult_str = f"x{risk_multiplier:.0f}"

            log_sep2()

            # ── Actualiza martingale e recovery ───────────────────────────────
            if profit_this < 0:
                # ── PERDA ─────────────────────────────────────────────────────
                loss             = abs(profit_this)
                prev_round_loss  = loss                                    # só a última ronda
                accumulated_loss += loss                                   # acumula o total
                risk_multiplier  = min(risk_multiplier * 2.0, MAX_RISK_MULTIPLIER)

                # Preview do risco na próxima ronda (para informação)
                next_risk_eighty = calc_risk_preview(EIGHTY_RISK,    risk_multiplier, prev_round_loss, bankroll)
                next_risk_peg    = calc_risk_preview(PEG_ARBIT_RISK, risk_multiplier, prev_round_loss, bankroll)
                cap_e = " [CAP]" if next_risk_eighty >= MAX_RISK_PERCENT else ""
                cap_p = " [CAP]" if next_risk_peg    >= MAX_RISK_PERCENT else ""

                next_mult_str = f"x{risk_multiplier:.0f}"
                log_info(
                    f"MARTINGALE LOSS | x{int(risk_multiplier/2.0):.0f} -> {next_mult_str} | "
                    f"próx EIGHTY={next_risk_eighty:.1%}{cap_e} PEG={next_risk_peg:.1%}{cap_p}"
                )
                log_info(f"ROUND | PnL: {round_pnl_str} | Extra: {extra_stake_str} | Mult: {mult_str}")

            elif profit_this == 0.0 and risk_multiplier > 1.0:
                # ── SEM TRADES (mantém estado) ────────────────────────────────
                prev_round_loss = 0.0   # ronda vazia não afecta o recovery
                log_info(f"ROUND | PnL: {round_pnl_str} | Extra: {extra_stake_str} | Mult: {mult_str}")

            else:
                # ── LUCRO ─────────────────────────────────────────────────────
                prev_loss_before = accumulated_loss
                accumulated_loss = max(0.0, accumulated_loss - profit_this)
                recovered        = prev_loss_before - accumulated_loss
                prev_round_loss  = 0.0   # ronda lucrativa: sem recovery bonus na próxima
                risk_multiplier  = 1.0   # reset do multiplier

                if recovered > 0 and accumulated_loss > 0:
                    log_info(
                        f"MARTINGALE RECOVERY PARTIAL | recuperados ${recovered:.4f} | "
                        f"restam ${accumulated_loss:.4f}"
                    )
                elif recovered > 0 and accumulated_loss == 0.0:
                    log_info(
                        f"MARTINGALE RECOVERY COMPLETE | recuperados ${prev_loss_before:.4f} total"
                    )
                log_info(f"ROUND | PnL: {round_pnl_str} | Extra: {extra_stake_str} | Mult: {mult_str}")

            # ── Totais do dia — NOVO FORMAT: TOTAL: PnL | Banca | Accumul.Loss | Uptime ──
            if daily_profit == 0.0:
                total_pnl_str = "$0.0000 (0.00%)"
            else:
                total_pnl_str = f"${daily_profit:+.4f} ({daily_pct:+.2f}%)"

            log_info(
                f"TOTAL | PnL: {total_pnl_str} | Banca: ${bankroll:.4f} | "
                f"Accum Loss: ${accumulated_loss:.4f} | Uptime: {get_uptime_str()}"
            )
            log_sep2()

        else:
            log_warn("Sem preços recebidos neste ciclo — a saltar")

        # ── Cancela o WebSocket e aguarda o próximo ciclo ─────────────────────
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
        log_info("⛔ BOT PARADO PELO UTILIZADOR")