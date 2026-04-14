"""
xrp_bot_v9_5_4.py -- Polymarket XRP Up/Down 5-Minute
VERSION: 9.5.4 -- HEDGE_RESTRICT + CIRCUIT_FREEZE + ENDGAME_RELAXED

Changes from v9.5.3:
1. [VOL_HEDGE]   Entry trigger raised from 1SD to 2SD. Fewer entries, higher
                 success rate. Global cut-off: NO hedge if rem <= 50s.
2. [HEDGE_FLIP]  Requires 4/5 confirmations (was 3). Stronger momentum check.
                 Also blocked if rem <= 50s.
3. [EDGE]        min_vwap_edge=0.004, fee_buffer=0.006 → total 1.0% edge floor.
                 bid_ask_min_ratio relaxed to 0.940.
4. [CIRCUIT]     Daily loss 50% → 30min FREEZE (was permanent halt).
                 Hourly loss 25% → 15min FREEZE (isolated from daily).
5. [ENDGAME]     Relaxed: min_c=0.42, max_c=0.998, risk=0.22, high_z_risk=0.28,
                 z_thresh=2.0, window=30s. More opportunities in final seconds.
6. [INHERITED]   Moonbag TP, stop-loss 40%, VOL_HEDGE dual-verify, Shadow Engine,
                 PEG ARB, Bayesian/Kalman, Martingale all unchanged.
"""
from __future__ import annotations
###############################################################################
# SECTION 1 -- PURE IMPORTS (zero side effects at module level)
###############################################################################
import asyncio
import base64
import hashlib
import hmac
import json
import logging
import logging.handlers
import math
import os
import queue
import random
import signal
import stat
import subprocess
import sys
import time
import traceback
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import functools

# v9.4.0: USER_WS OPEN log throttle -- prevent spam on rapid reconnects
USER_WS_OPEN_LOG_INTERVAL: float = 4.0

# === GAMMA API HEADERS (used by _fetch_metadata_sync) ===
_META_HEADERS: Dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://polymarket.com",
    "Referer": "https://polymarket.com/",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}

try:
    import numpy as _np
    _HAS_NUMPY: bool = True
except ImportError:
    _HAS_NUMPY = False

_HAS_ORJSON: bool = False
try:
    import orjson as _orjson
    _HAS_ORJSON = True
    def _json_dumps(obj: Any) -> bytes:
        return _orjson.dumps(obj, option=_orjson.OPT_INDENT_2)
    def _json_dumps_compact(obj: Any) -> bytes:
        return _orjson.dumps(obj)
    def _json_loads(raw: Union[bytes, str]) -> Any:
        return _orjson.loads(raw)
except ImportError:
    def _json_dumps(obj: Any) -> bytes:
        return json.dumps(obj, indent=2).encode()
    def _json_dumps_compact(obj: Any) -> bytes:
        return json.dumps(obj, separators=(",", ":")).encode()
    def _json_loads(raw: Union[bytes, str]) -> Any:
        return json.loads(raw)

###############################################################################
# SECTION 2 -- CONFIGURATION (pydantic-style with secrets.txt support)
###############################################################################
def _load_secrets_file(path: str = "secrets.txt") -> Dict[str, str]:
    secrets: Dict[str, str] = {}
    p = Path(path)
    if not p.exists():
        return secrets
    try:
        file_stat = os.stat(path)
        if file_stat.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            logging.getLogger("bot_xrp").warning(
                f"[SECURITY] {path} has insecure permissions "
                f"(mode={oct(file_stat.st_mode)[-3:]})! "
                f"Run: chmod 600 {path}"
            )
    except OSError:
        pass
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            k, _, v = line.partition("=")
            secrets[k.strip()] = v.strip()
    return secrets

###############################################################################
# PARAMETER GUIDE -- v9.4.3
# Cada parâmetro tem: descrição, unidade, [min..max], efeito ↑/↓
#
# ═══════════════════════════════════════════════════════════════════════════
# SIZING & RISCO (controlam tamanho das posições e perdas máximas)
# ═══════════════════════════════════════════════════════════════════════════
#
# kelly_max_risk_pct         | fração | [0.005 .. 0.15]
#   % máxima do bankroll numa única posição.
#   ↑ = posições maiores, mais profit mas mais risco por trade.
#   ↓ = posições menores, perda máxima por trade reduzida.
#   REGRA: se queres max 1% loss → usa 0.01.
#
# kelly_fraction             | fração | [0.10 .. 0.50]
#   Fração do Kelly completo usado no cálculo de sizing.
#   ↑ = closer to full Kelly, mais agressivo.
#   ↓ = "fractional Kelly", mais conservador.
#
# kelly_assumed_edge         | fração | [0.01 .. 0.10]
#   Edge assumido no cálculo de Kelly para boost em Martingale.
#   ↑ = boost de stake mais agressivo quando edge > assumed.
#   ↓ = boost mais fácil de trigger.
#
# kelly_mart_boost           | fração | [0.0 .. 0.50]
#   Multiplicador extra no stake quando edge > assumed_edge * 1.2.
#   ↑ = recovery mais rápida em Martingale mas mais risco.
#   ↓ = recovery mais lenta mas mais segura.
#
# max_market_exposure        | fração | [0.10 .. 0.50]
#   % máxima do bankroll em todas as posições abertas combinadas.
#   ↑ = mais capital em risco simultâneo.
#   ↓ = limita exposição total.
#
# max_bankroll_exposure      | fração | [0.20 .. 0.80]
#   Hard cap de exposição total do bankroll.
#   ↑ = permite quase todo o bankroll em posições.
#   ↓ = reserva mais cash.
#
# max_position_size_usd      | USD | [1.0 .. 10000.0]
#   Tamanho máximo absoluto em USD de uma posição.
#   ↑ = sem limitar banca crescente (usar valor alto).
#   ↓ = hard cap que bloqueia trades grandes (cuidado com Mart).
#
# max_active_trades          | count | [1 .. 20]
#   Nº máximo de trades abertos em simultâneo.
#   ↑ = mais posições paralelas.
#   ↓ = menos, mais focado.
#
# ═══════════════════════════════════════════════════════════════════════════
# MARTINGALE (recovery de perdas consecutivas)
# ═══════════════════════════════════════════════════════════════════════════
#
# mart_max_mult              | multiplicador | [1 .. 5]
#   Multiplicador máximo de Martingale (1=desligado).
#   ↑ = recovery mais agressiva mas risco de ruin muito maior.
#   ↓ = mais seguro, recovery mais lenta.
#
# mart_recovery_factor       | multiplicador | [1.00 .. 1.50]
#   Amplifica o stake em modo Martingale.
#   ↑ = stakes maiores em recovery → mais risco.
#   ↓ = stakes normais em recovery.
#
# max_consecutive_losses     | count | [2 .. 10]
#   Nº de perdas seguidas antes de pausar trading (safety halt).
#   ↑ = mais tolerante a streaks.
#   ↓ = para mais cedo (protege bankroll).
#
# ═══════════════════════════════════════════════════════════════════════════
# LIMITES DIÁRIOS / HORÁRIOS (circuit breakers)
# ═══════════════════════════════════════════════════════════════════════════
#
# max_daily_loss_pct         | % | [5.0 .. 100.0]
#   % máxima de perda diária antes de parar.
#   ↑ = mais tolerante (arriscado).
#   ↓ = para cedo, protege capital.
#
# max_hourly_loss_pct        | % | [5.0 .. 50.0]
#   % máxima de perda por hora antes de pausar.
#   ↑ = mais tolerante.
#   ↓ = pausa mais rápido em drawdowns.
#
# hourly_pause_duration_s    | segundos | [60 .. 3600]
#   Tempo de pausa depois de atingir limite horário.
#   ↑ = pausa mais longa.
#   ↓ = retoma mais rápido.
#
# ═══════════════════════════════════════════════════════════════════════════
# GAMBLING (estratégia principal de entrada)
# ═══════════════════════════════════════════════════════════════════════════
#
# gambling_active            | bool | True/False
#   Liga/desliga o módulo Gambling.
#
# gamb_start_rem_s           | segundos | [30 .. 300]
#   Tempo restante no ciclo para começar a entrar.
#   ↑ = entra mais cedo (mais tempo para entrar).
#   ↓ = só entra nos últimos segundos.
#
# gamb_cutoff_s              | segundos | [5 .. 60]
#   Tempo mínimo restante para permitir entrada.
#   ↑ = para de entrar mais cedo.
#   ↓ = entra até mais perto do fim.
#
# gamb_buy_cooldown          | segundos | [1.0 .. 30.0]
#   Cooldown mínimo entre compras do mesmo lado.
#   ↑ = menos entradas (espaçadas).
#   ↓ = mais entradas (mais rápido).
#
# gamb_min_ask_c             | cêntimos | [30.0 .. 60.0]
#   Ask mínimo (em cêntimos) para entrar. Ex: 45 = 0.45.
#   ↑ = só entra em asks altos (mais "certo", menos payout).
#   ↓ = entra em asks baixos (mais risco, mais payout).
#
# gamb_max_ask_c             | cêntimos | [80.0 .. 99.0]
#   Ask máximo (em cêntimos) para entrar.
#   ↑ = permite asks muito altos (caro).
#   ↓ = evita asks caros.
#
# ═══════════════════════════════════════════════════════════════════════════
# FILTROS DE ENTRADA (determinam WIN RATE)
# ═══════════════════════════════════════════════════════════════════════════
#
# min_prob_entry             | probabilidade | [0.50 .. 0.70]
#   Probabilidade mínima P(side) para entrar.
#   ↑ = menos entradas mas maior WR (mais selectivo).
#   ↓ = mais entradas mas menor WR.
#
# min_vwap_edge              | fração | [0.001 .. 0.05]
#   Edge mínimo (P_hat - VWAP_ask - fees) para entrar.
#   ↑ = só entra com edge grande → maior WR, menos trades.
#   ↓ = entra com edge pequeno → mais trades, menor WR.
#
# es_min_threshold           | z-score | [1.0 .. 4.0]
#   Edge Score mínimo do VolatilityEdgeTracker.
#   ↑ = filtro mais rigoroso → menos trades, maior WR.
#   ↓ = filtro mais solto → mais trades.
#
# max_spread_cents           | cêntimos | [0.5 .. 3.0]
#   Spread máximo (ask-bid) em cêntimos para entrar.
#   ↑ = permite spreads maiores (mais entradas).
#   ↓ = só entra em spreads apertados (melhor preço).
#
# bid_ask_min_ratio          | ratio | [0.90 .. 0.999]
#   Rácio mínimo bid/ask para entrar.
#   ↑ = exige preços mais apertados.
#   ↓ = permite mais discrepância.
#
# fee_buffer                 | fração | [0.002 .. 0.015]
#   Buffer de fee subtraído em todos os cálculos de edge.
#   ↑ = mais conservador (edge precisa ser maior).
#   ↓ = menos conservador.
#
# ═══════════════════════════════════════════════════════════════════════════
# ENDGAME (últimos segundos do ciclo de 5 min)
# ═══════════════════════════════════════════════════════════════════════════
#
# aggressive_endgame_active  | bool | True/False
#   Liga/desliga entrada agressiva no endgame.
#
# aggressive_endgame_s       | segundos | [10 .. 45]
#   Últimos N segundos que activam endgame.
#   ↑ = endgame começa mais cedo.
#   ↓ = só nos últimos segundos.
#
# aggressive_endgame_risk    | fração | [0.01 .. 0.15]
#   Risk % base para trades de endgame.
#   ↑ = posições endgame maiores.
#   ↓ = endgame mais conservador.
#
# endgame_high_z_risk        | fração | [0.01 .. 0.15]
#   Risk % quando Z-score é alto (alta convicção).
#   ↑ = mais agressivo em sinais fortes.
#   ↓ = mais conservador mesmo em sinais fortes.
#
# endgame_high_z_thresh      | z-score | [1.5 .. 4.0]
#   Limiar de Z-score para activar high_z_risk.
#   ↑ = precisa de sinal mais forte.
#   ↓ = activa com sinais mais fracos.
#
# ═══════════════════════════════════════════════════════════════════════════
# PEG ARB (arbitragem quando ask_UP + ask_DOWN < 1.00)
# ═══════════════════════════════════════════════════════════════════════════
#
# peg_arb_active             | bool | True/False
#   Liga/desliga o módulo PEG ARB.
#
# peg_trigger                | preço | [0.95 .. 0.995]
#   PEG máximo (ask_up + ask_down) para trigger de arb.
#   ↑ = mais oportunidades (PEG pode ser mais alto).
#   ↓ = mais selectivo, só PEGs muito baixos (mais lucro).
#
# peg_budget_pct             | fração | [0.01 .. 0.10]
#   % do bankroll alocado por operação de PEG ARB.
#   ↑ = posições arb maiores.
#   ↓ = posições arb menores.
#
# peg_cooldown_s             | segundos | [2.0 .. 30.0]
#   Cooldown mínimo entre operações PEG ARB.
#   ↑ = menos operações.
#   ↓ = mais operações.
#
# peg_min_profit_pct         | % | [0.1 .. 2.0]
#   Lucro mínimo % para executar PEG ARB (após fees).
#   ↑ = só arb muito lucrativo.
#   ↓ = aceita arb com lucro pequeno.
#
# arb_resolution             | preço | sempre 1.00
#   Payout de resolução (1 share = $1.00 se ganha). Fixo.
#
# arb_min_shares             | shares | [0.05 .. 1.0]
#   Nº mínimo de shares para executar arb.
#   ↑ = ignora arbs muito pequenas.
#   ↓ = aceita arbs pequenas.
#
# ═══════════════════════════════════════════════════════════════════════════
# BAYESIAN + KALMAN + BINANCE (modelo de probabilidade)
# ═══════════════════════════════════════════════════════════════════════════
#
# bayesian_prior             | probabilidade | [0.40 .. 0.60]
#   Prior do modelo Bayesiano. 0.50 = sem bias.
#
# bayesian_likelihood_std    | desvio padrão | [0.003 .. 0.02]
#   Largura da likelihood Gaussiana.
#   ↑ = modelo reage mais lentamente a dados novos.
#   ↓ = modelo reage mais rápido (mais volátil).
#
# bayesian_decay_rate        | fração/tick | [0.01 .. 0.10]
#   Taxa de decay do posterior em direcção ao prior.
#   ↑ = posterior reverte mais rápido ao prior (memória curta).
#   ↓ = posterior mantém-se mais tempo (memória longa).
#
# binance_blend_weight       | fração | [0.30 .. 0.90]
#   Peso do oráculo Binance no blend com posterior Bayesiano.
#   ↑ = confia mais no preço Binance.
#   ↓ = confia mais no modelo Bayesiano.
#
# kalman_process_noise       | variância | [1e-7 .. 1e-4]
#   Ruído de processo do filtro Kalman.
#   ↑ = Kalman segue dados mais de perto (mais reactivo).
#   ↓ = Kalman mais suave (filtra mais ruído).
#
# kalman_measure_noise       | variância | [1e-4 .. 1e-2]
#   Ruído de medida do filtro Kalman.
#   ↑ = Kalman desconfia mais das medições.
#   ↓ = Kalman confia mais nas medições.
#
# ═══════════════════════════════════════════════════════════════════════════
# VOLATILIDADE + JUMP DIFFUSION
# ═══════════════════════════════════════════════════════════════════════════
#
# xrp_vol_annual_default     | vol anualizada | [0.50 .. 2.50]
#   Volatilidade anualizada default do XRP.
#   ↑ = modelo assume mais incerteza (probs mais perto de 50/50).
#   ↓ = modelo assume mais certeza (probs mais extremas).
#
# jump_lambda                | rate/ano | [0.5 .. 3.0]
#   Intensidade de jumps no modelo Jump Diffusion.
#   ↑ = mais jumps esperados (tails mais pesadas).
#   ↓ = menos jumps (mais GBM puro).
#
# jump_sigma                 | desvio padrão | [0.01 .. 0.05]
#   Tamanho médio dos jumps.
#   ↑ = jumps maiores.
#   ↓ = jumps menores.
#
# ═══════════════════════════════════════════════════════════════════════════
# SHADOW TRADING (simulação de fills em dry_run)
# ═══════════════════════════════════════════════════════════════════════════
#
# shadow_latency_ms          | milissegundos | [20 .. 200]
#   Latência simulada de rede + matching engine.
#   ↑ = simulação mais pessimista (mais slippage).
#   ↓ = simulação mais optimista.
#
# shadow_max_slippage_pct    | fração | [0.005 .. 0.05]
#   Slippage máximo aceite em fills simulados.
#   ↑ = aceita mais slippage (mais fills).
#   ↓ = rejeita mais fills (mais realista).
#
# ═══════════════════════════════════════════════════════════════════════════
###############################################################################

@dataclass
class BotConfig:

    # ── CREDENTIALS & PATHS ──────────────────────────────────────────────
    dry_run: bool = True
    live_trading: bool = False
    polymarket_private_key: str = ""
    polymarket_api_key: str = ""
    polymarket_secret: str = ""
    polymarket_passphrase: str = ""
    state_file: str = "trade_state.json"
    audit_file: str = "trade_audit.jsonl"
    secrets_path: str = "secrets.txt"
    bankroll_demo: Decimal = Decimal("100.0")

    # ── FEES & SLIPPAGE ──────────────────────────────────────────────────
    slippage_tolerance: float = 0.001       # fração, max slippage aceite
    fee_buffer: float = 0.006               # fração, subtraído a todos os edges
    gas_cost_usdc: float = 0.0              # USD, custo de gas (0 em Polygon)

    # ── SHADOW TRADING ───────────────────────────────────────────────────
    shadow_latency_ms: float = 40.0         # ms, latência simulada (v9.5.1: reduced from 80)
    shadow_max_slippage_pct: float = 0.02   # fração, max 2% slippage

    # ── SIZING & RISCO ───────────────────────────────────────────────────
    kelly_max_risk_pct: float = 0.105        # fração, ~10.5% bankroll per trade (v9.5.2: was 0.095)
    kelly_fraction: float = 0.48            # fração, fractional Kelly
    kelly_assumed_edge: float = 0.040       # fração, edge para boost calc
    kelly_mart_boost: float = 0.30          # fração, boost em Mart quando edge alto
    max_market_exposure: float = 0.28       # fração, exposição máxima total
    max_bankroll_exposure: float = 0.58     # fração, hard cap exposição
    max_position_size_usd: float = 1000.0   # USD, hard cap por posição
    max_active_trades: int = 8              # count, trades simultâneos max

    # ── MARTINGALE ───────────────────────────────────────────────────────
    mart_max_mult: int = 4                  # multiplicador, max Mart level
    mart_recovery_factor: float = 1.22      # multiplicador, amplifica stake Mart
    max_consecutive_losses: int = 5         # count, halt após N losses seguidos

    # ── CIRCUIT BREAKERS ─────────────────────────────────────────────────
    # v9.5.4: Freeze instead of halt. Daily and hourly are isolated.
    max_daily_loss_pct: float = 50.0        # %, freeze 30min se perda diária > X%
    max_hourly_loss_pct: float = 25.0       # %, freeze 15min se perda horária > X%
    hourly_pause_duration_s: float = 900.0  # segundos, duração da pausa horária (15 min)
    daily_pause_duration_s: float = 1800.0  # segundos, duração da pausa diária (30 min)

    # ── PEG ARB ──────────────────────────────────────────────────────────
    peg_arb_active: bool = True             # liga/desliga PEG ARB
    peg_trigger: float = 0.985              # preço, PEG max para trigger arb
    peg_budget_pct: float = 0.03            # fração, % bankroll por arb
    peg_cooldown_s: float = 5.0             # segundos, entre arbs
    peg_min_profit_pct: float = 0.20        # %, lucro mínimo após fees
    arb_resolution: float = 1.00            # preço, payout por share (fixo)
    arb_min_shares: float = 0.10            # shares, mínimo para arb

    # ── GAMBLING (estratégia principal) ──────────────────────────────────
    gambling_active: bool = True
    gamb_start_rem_s: float = 300.0         # segundos, início do gambling
    gamb_cutoff_s: float = 25.0             # segundos, cutoff (para antes do fim)
    gamb_buy_cooldown: float = 12.0         # segundos, cooldown entre compras (v9.5.3: was 6.0)
    gamb_min_ask_c: float = 42.0            # cêntimos, ask mínimo (v9.5.2: was 45.0, more permissive)
    gamb_max_ask_c: float = 93.0            # cêntimos, ask máximo (0.93)

    # ── FILTROS DE ENTRADA (WR + frequência) ─────────────────────────────
    # v9.5.4: min_vwap_edge + fee_buffer = 0.004 + 0.006 = 0.010 (1.0% total edge floor)
    min_prob_entry: float = 0.52             # probabilidade, P(side) mínima
    min_vwap_edge: float = 0.004            # fração, edge mínimo após VWAP+fees (v9.5.4: was 0.020)
    max_spread_cents: float = 3.0           # cêntimos, spread max aceite
    bid_ask_min_ratio: float = 0.940        # ratio, bid/ask mínimo (v9.5.4: was 0.955, relaxed)

    # ── ENDGAME (v9.5.4: relaxed for more opportunities) ────────────────
    aggressive_endgame_active: bool = True
    aggressive_endgame_s: float = 30.0      # segundos, janela endgame (was 25)
    aggressive_endgame_risk: float = 0.22   # fração, risk % endgame base (was 0.18)
    aggressive_endgame_min_c: float = 0.42  # preço, ask mínimo endgame (was 0.48)
    aggressive_endgame_max_c: float = 0.998 # preço, ask máximo endgame (was 0.995)
    endgame_high_z_risk: float = 0.28       # fração, risk % em Z alto (was 0.24)
    endgame_high_z_thresh: float = 2.0      # z-score, limiar para high risk (was 2.5)

    # ── TAKE PROFIT (v9.5.3: Moonbag TP -- sell minimum to recover 100% capital)
    partial_tp_active: bool = True
    partial_tp_fraction: float = 0.80       # fração, CEILING: max % that can be sold (moonbag sells less)
    partial_tp_target_net_roi: float = 0.08 # fração, minimum bid gain to trigger TP check

    # ── HEDGE DINÂMICO (flip contra direção errada) ──────────────────────
    # v9.5.4: Requires 4/5 confirmations. Blocked if rem <= 50s.
    # NEVER fires on VOL_HEDGE or ENDGAME trades.
    adverse_stop_cents: float = 0.9          # cêntimos, 0.9c trigger
    hedge_max_risk_pct: float = 0.07         # fração, risk % do hedge buy oposto
    hedge_min_prob_opposite: float = 0.65    # probabilidade, P(oposto) mínima
    hedge_flip_speed_thresh: float = 0.003   # fração, BNC velocity (%/s) (v9.5.4: was 0.002, stricter)
    hedge_flip_imbalance_thresh: float = 0.22  # ratio, OBI < this (v9.5.4: was 0.25, stricter)
    hedge_flip_confirms_needed: int = 4      # count, min confirmations (v9.5.4: was 3)
    hedge_cutoff_s: float = 50.0             # segundos, NO hedge if rem <= this

    # ── STOP-LOSS POR TRADE (v9.5.2) ─────────────────────────────────────
    max_loss_per_trade_pct: float = 0.40     # fração, -40% max loss per GAMBLING trade (v9.5.3: was 0.15)

    # ── VOL HEDGE 2SD-3SD (v9.5.4: raised from 1SD to 2SD entry) ───────
    vol_hedge_active: bool = True            # liga/desliga vol hedge
    vol_hedge_sd_window: int = 30            # ticks Binance para cálculo do SD
    vol_hedge_1sd_trigger: float = 2.0       # multiplicador SD para trigger entrada (v9.5.4: was 1.0)
    vol_hedge_3sd_target: float = 3.0        # multiplicador SD para hedge fill target
    vol_hedge_no_limit_low: float = 0.10     # preço mínimo limite NO (10c)
    vol_hedge_no_limit_high: float = 0.15    # preço máximo limite NO (15c)
    vol_hedge_abandon_s: float = 60.0        # segundos antes do fecho para abandonar hedge
    vol_hedge_cooldown_s: float = 8.0        # cooldown entre entradas vol_hedge
    vol_hedge_max_risk_pct: float = 0.10     # risk % para posição vol_hedge
    vol_hedge_min_sd: float = 0.00005        # SD mínimo para evitar triggers em flat market
    vol_hedge_liquidity_min: float = 5.0     # volume mínimo no book para limit order NO
    vol_hedge_cutoff_s: float = 50.0         # NO vol_hedge if rem <= this (v9.5.4)

    # ── BAYESIAN + KALMAN ────────────────────────────────────────────────
    bayesian_prior: float = 0.50            # probabilidade, prior P(UP)
    bayesian_likelihood_std: float = 0.008  # desvio padrão, largura likelihood
    bayesian_decay_rate: float = 0.030      # fração/tick, decay ao prior

    # ── EDGE SCORING ─────────────────────────────────────────────────────
    vol_edge_window: int = 10               # ticks, janela do edge tracker
    vol_edge_sigma_floor: float = 0.004     # fração, sigma mínimo
    es_min_threshold: float = 1.60          # z-score, edge score mínimo (v9.5.2: was 1.80)
    vol_kelly_target: float = 0.025         # fração, kelly target vol scaling

    # ── HFT / KALMAN ────────────────────────────────────────────────────
    hft_window_seconds: float = 4.0         # segundos, janela HFT z-score
    kalman_process_noise: float = 5e-6      # variância, processo Kalman
    kalman_measure_noise: float = 2.5e-3    # variância, medida Kalman

    # ── API / RATE LIMITING / CIRCUIT BREAKER ────────────────────────────
    rate_limit_calls: float = 12.0          # calls/s
    rate_limit_burst: float = 25.0          # calls, burst max
    max_api_retries: int = 5                # count
    base_backoff_s: float = 0.8             # segundos
    max_backoff_s: float = 20.0             # segundos
    backoff_jitter: bool = True
    cb_fail_threshold: int = 5              # count, falhas para abrir CB
    cb_recovery_s: float = 45.0             # segundos, tempo de recovery CB
    redeem_cb_threshold: int = 5
    redeem_cb_recovery: float = 120.0       # segundos

    # ── WEBSOCKET ────────────────────────────────────────────────────────
    ws_reconnect_base_s: float = 0.8
    ws_reconnect_max_s: float = 15.0
    ws_heartbeat_interval: int = 4          # segundos
    ws_heartbeat_timeout: int = 8           # segundos

    # ── ENDPOINTS ────────────────────────────────────────────────────────
    clob_rest_url: str = "https://clob.polymarket.com"
    gamma_api_url: str = "https://gamma-api.polymarket.com"
    ws_uri: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    user_ws_uri: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    binance_ws_uri: str = "wss://stream.binance.com:9443/ws/xrpusdt@ticker"
    binance_rest_klines_url: str = "https://api.binance.com/api/v3/klines"
    binance_reconnect_base_s: float = 0.8
    binance_reconnect_max_s: float = 20.0
    binance_ping_interval_s: float = 15.0

    # ── BINANCE ORACLE / VOL ─────────────────────────────────────────────
    xrp_vol_annual_default: float = 1.10    # vol anualizada
    xrp_vol_window_ticks: int = 15          # ticks
    xrp_drift_ema_alpha: float = 0.18       # fração, EMA alpha
    stale_data_threshold_s: float = 2.5     # segundos
    time_decay_floor_s: float = 1.5         # segundos
    sigmoid_steepness: float = 15.0         # adimensional
    sigma_floor: float = 0.04               # fração
    prob_min: float = 0.02                  # probabilidade
    prob_max: float = 0.98                  # probabilidade

    # ── JUMP DIFFUSION ───────────────────────────────────────────────────
    jump_lambda: float = 1.2                # rate/ano, intensidade jumps
    jump_mu: float = 0.0                    # fração, média dos jumps
    jump_sigma: float = 0.025               # fração, desvio dos jumps
    jump_terms: int = 4                     # count, termos na série

    # ── FUNDING RATE ─────────────────────────────────────────────────────
    funding_rate_url: str = "https://fapi.binance.com/fapi/v1/fundingRate"
    funding_rate_symbol: str = "XRPUSDT"
    funding_rate_poll_s: float = 25.0       # segundos
    funding_rate_bull_thresh: float = 0.0004  # fração
    funding_rate_bear_thresh: float = -0.0004 # fração
    binance_blend_weight: float = 0.72      # fração, peso Binance no blend

    # ── ADAPTIVE EDGE ────────────────────────────────────────────────────
    default_taker_fee_bps: int = 50         # bps
    adaptive_edge_winrate_high: float = 0.65
    adaptive_edge_winrate_low: float = 0.50
    adaptive_edge_scale_win: float = 0.85
    adaptive_edge_scale_loss: float = 1.25
    adaptive_edge_min: float = 0.002
    adaptive_edge_max: float = 0.06

    # ── SETTLEMENT / RECONCILIATION ──────────────────────────────────────
    settlement_timeout_s: float = 180.0     # segundos
    settlement_backoff_base_s: float = 3.0  # segundos
    reconcile_interval_s: float = 20.0      # segundos
    slack_webhook_url: str = ""

    # ── VOL REGIME (logging only) ────────────────────────────────────────
    ewma_vol_alpha: float = 0.08
    ewma_vol_min_ticks: int = 4
    vol_regime_low_thresh: float = 0.40
    vol_regime_high_thresh: float = 1.20
    vol_short_weight: float = 0.65
    vol_hysteresis_s: float = 45.0

    def validate(self) -> None:
        if self.live_trading and not self.polymarket_private_key:
            raise ValueError("polymarket_private_key required for LIVE_TRADING")
        if self.mart_max_mult > 5:
            raise ValueError("mart_max_mult must be <= 5")
        if self.kelly_max_risk_pct > 0.15:
            raise ValueError("kelly_max_risk_pct must be <= 0.15 (15%)")
        if self.min_prob_entry < 0.50:
            raise ValueError("min_prob_entry must be >= 0.50 (50%)")
        if self.kelly_fraction > 0.50:
            raise ValueError("kelly_fraction must be <= 0.50 (50%)")
        if self.peg_trigger > 1.0:
            raise ValueError("peg_trigger must be <= 1.0")

    @classmethod
    def from_env_and_secrets(cls, secrets_path: str = "secrets.txt") -> "BotConfig":
        sfile = _load_secrets_file(secrets_path)
        def _get(key: str, default: str = "", *aliases: str) -> str:
            for k in (key, *aliases):
                if k in sfile:
                    return sfile[k]
            for k in (key, *aliases):
                v = os.environ.get(k, "")
                if v:
                    return v
            return default
        cfg = cls()
        cfg.secrets_path = secrets_path
        cfg.polymarket_private_key = _get("PRIVATE_KEY", "", "POLYMARKET_PRIVATE_KEY")
        cfg.polymarket_api_key = _get("API_KEY", "", "POLYMARKET_API_KEY")
        cfg.polymarket_secret = _get("API_SECRET", "", "POLYMARKET_SECRET")
        cfg.polymarket_passphrase = _get("API_PASSPHRASE", "", "POLYMARKET_PASSPHRASE")
        if _get("DRY_RUN", "1").lower() in ("0", "false", "no"):
            cfg.dry_run = False
        if _get("LIVE_TRADING", "0").lower() in ("1", "true", "yes"):
            cfg.live_trading = True
        if webhook := _get("SLACK_WEBHOOK_URL"):
            cfg.slack_webhook_url = webhook
        cfg.validate()
        return cfg

###############################################################################
# SECTION 3 -- DECIMAL HELPERS
###############################################################################
_QUANT6 = Decimal("0.000001")
_QUANT4 = Decimal("0.0001")
_ZERO = Decimal("0")
_ONE = Decimal("1")
_SECS_PER_YEAR: float = 365.25 * 24.0 * 3600.0
_FEE_BPS_CACHE: Dict[int, Decimal] = {}

@functools.lru_cache(maxsize=1024)
def _decimal_from_str(val_str: str) -> Decimal:
    try:
        return Decimal(val_str)
    except (InvalidOperation, ValueError):
        return _ZERO

def _d(x: Union[float, int, str, Decimal]) -> Decimal:
    if isinstance(x, Decimal):
        return x
    try:
        if isinstance(x, (int, float)):
            val_str = str(x)
        else:
            val_str = x
        return _decimal_from_str(val_str)
    except (InvalidOperation, ValueError, Exception):
        return _ZERO

def _d_cached(x: Union[float, int, str]) -> Decimal:
    try:
        val_str = str(x)
        return _decimal_from_str(val_str)
    except (InvalidOperation, ValueError, Exception):
        return _ZERO

def _dq(x: Union[Decimal, float, int], places: int = 6) -> Decimal:
    try:
        d = x if isinstance(x, Decimal) else _d(x)
        quant = Decimal(10) ** -places
        result = d.quantize(quant, rounding=ROUND_DOWN)
        if not result.is_finite():
            return _ZERO
        return result
    except (InvalidOperation, Exception):
        return _ZERO

def _dq_clamp(x: Union[Decimal, float], lo: float = 0.0, hi: float = 1.0) -> Decimal:
    v = _dq(x)
    lo_d, hi_d = _d(lo), _d(hi)
    if v < lo_d:
        return lo_d
    if v > hi_d:
        return hi_d
    return v

def _safe_float(x: Optional[Union[Decimal, float]], default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        f = float(x)
        return f if math.isfinite(f) else default
    except Exception:
        return default

def _safe_price(val: Any, min_v: float = 0.001, max_v: float = 0.999) -> Optional[float]:
    if val is None:
        return None
    try:
        f = float(val)
        if not math.isfinite(f):
            return None
        if f < min_v or f > max_v:
            return None
        return f
    except (ValueError, TypeError, InvalidOperation):
        return None

def _safe_size(val: Any, min_v: float = 0.0) -> float:
    if val is None:
        return 0.0
    try:
        f = float(val)
        return f if math.isfinite(f) and f >= min_v else 0.0
    except (ValueError, TypeError):
        return 0.0

def _fee_rate_decimal(bps: int) -> Decimal:
    if bps not in _FEE_BPS_CACHE:
        _FEE_BPS_CACHE[bps] = Decimal(bps) / Decimal(10000)
    return _FEE_BPS_CACHE[bps]

###############################################################################
# SECTION 4 -- LOGGING FACTORY (NO side effects at import)
###############################################################################
logger: Optional[logging.Logger] = None
_audit_fh: Optional[logging.FileHandler] = None
_log_listener: Optional[logging.handlers.QueueListener] = None

def init_logging(audit_file: str = "trade_audit.jsonl") -> logging.Logger:
    global logger, _audit_fh, _log_listener
    _q: queue.Queue[logging.LogRecord] = queue.Queue(-1)
    _ch = logging.StreamHandler(sys.stdout)
    _ch.setFormatter(logging.Formatter("%(message)s"))
    _ch.setLevel(logging.DEBUG)
    _fh = logging.FileHandler("bot_xrp.log", encoding="utf-8")
    _fh.setFormatter(logging.Formatter("%(message)s"))
    _fh.setLevel(logging.DEBUG)
    _audit_fh = logging.FileHandler(audit_file, encoding="utf-8")
    _audit_fh.setFormatter(logging.Formatter("%(message)s"))
    _audit_fh.setLevel(logging.INFO)
    _log_listener = logging.handlers.QueueListener(
        _q, _fh, _ch, respect_handler_level=True
    )
    _log_listener.start()
    _logger = logging.getLogger("bot_xrp")
    _logger.setLevel(logging.DEBUG)
    _logger.handlers.clear()
    _logger.addHandler(logging.handlers.QueueHandler(_q))
    _logger.propagate = False
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("websockets.client").setLevel(logging.WARNING)
    logger = _logger
    return _logger

def _get_logger() -> logging.Logger:
    return logger or logging.getLogger("bot_xrp")

def _ts() -> str:
    return datetime.now().strftime("%d/%m/%y | %H:%M:%S.%f")[:-3]

def _uptime(start: float) -> str:
    e = int(time.time() - start)
    h, r = divmod(e, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}h:{m:02d}m:{s:02d}s"

def fc(p: float) -> str:
    return f"{p * 100:.1f}c"

def fmt_dollar(v: Union[Decimal, float]) -> str:
    """Format a dollar amount with enough precision to show small PnL values.

    Uses 6 decimal places so $0.003300 never rounds to $0.0000.
    Trailing zeros are stripped for readability.
    """
    fv = _safe_float(v)
    if fv < 0:
        return f"$-{abs(fv):.6f}".rstrip("0").rstrip(".")
    elif fv > 0:
        return f"$+{fv:.6f}".rstrip("0").rstrip(".")
    else:
        return "$0.0"

def fmt_fee(fee: Union[Decimal, float], base: Union[Decimal, float]) -> str:
    fb, bb = _safe_float(fee), _safe_float(base)
    pct = (fb / bb * 100.0) if bb > 1e-9 else 0.0
    return f"{fmt_dollar(fee)} ({pct:.2f}%)"

def fmt_pct(v: float) -> str:
    if v < 0:
        return f"-{abs(v):.2f}%"
    elif v > 0:
        return f"+{v:.2f}%"
    else:
        return f"{v:.2f}%"

def fmt_pnl(pnl: Union[Decimal, float], pnl_pct: float) -> str:
    """v9.5.0: Exact PnL format: 'PnL: (+)$X.XXXXXX (+Y.YY%)' or 'PnL: (-)$X.XXXXXX (-Y.YY%)'."""
    fv = _safe_float(pnl)
    if fv >= 0:
        return f"PnL: (+)${fv:.6f} (+{abs(pnl_pct):.2f}%)"
    else:
        return f"PnL: (-)${abs(fv):.6f} (-{abs(pnl_pct):.2f}%)"

def log_info(msg: str) -> None:
    # Per spec: non-module-specific logs use [DEBUG] fallback format.
    # Module-specific logs use their own helpers (log_gambling, log_endgame, etc.)
    _get_logger().debug("[DEBUG] [%s] | %s", _ts(), msg)

def log_warn(msg: str) -> None:
    _get_logger().warning("[WARN] [%s] | %s", _ts(), msg)

def log_debug(msg: str) -> None:
    _get_logger().debug("[DEBUG] [%s] | %s", _ts(), msg)

def log_raw(msg: str) -> None:
    """Main market state log -- NO prefix at all per spec."""
    _get_logger().info("[%s] | %s", _ts(), msg)

def log_sep() -> None:
    _get_logger().info("-" * 80)

def log_sep2() -> None:
    _get_logger().info("=" * 80)

# ── Module-specific helpers (exact spec format) ───────────────────────────────
# Format: [INFO] [MODULE] [DD/MM/YY | HH:MM:SS.ms] | message
def log_endgame(msg: str) -> None:
    _get_logger().info("[INFO] [ENDGAME] [%s] | %s", _ts(), msg)

def log_gambling(msg: str) -> None:
    _get_logger().info("[INFO] [GAMBLING] [%s] | %s", _ts(), msg)

def log_peg(msg: str) -> None:
    _get_logger().info("[INFO] [PEG] [%s] | %s", _ts(), msg)

def log_binance(msg: str) -> None:
    _get_logger().info("[INFO] [BINANCE] [%s] | %s", _ts(), msg)

def log_vol_hedge(msg: str) -> None:
    _get_logger().info("[INFO] [VOL_HEDGE] [%s] | %s", _ts(), msg)

def log_m(module: str, action: str, msg: str) -> None:
    """Route to correct module helper; action is merged into message body.

    Output: [INFO] [MODULE] [timestamp] | ACTION | message
    No extra bracket for action per spec.
    """
    _prefix_map: Dict[str, str] = {
        "GAMBLING":        "[INFO] [GAMBLING]",
        "ENDGAME_AGG":     "[INFO] [ENDGAME_AGG]",
        "ENDGAME":         "[INFO] [ENDGAME]",
        "PEG_ARBIT":       "[INFO] [PEG]",
        "BINANCE":         "[INFO] [BINANCE]",
        "VOL_HEDGE_YES":   "[INFO] [VOL_HEDGE]",
        "VOL_HEDGE_NO":    "[INFO] [VOL_HEDGE]",
    }
    prefix = _prefix_map.get(module, f"[DEBUG]")
    # action folded into message body — no extra bracket
    _get_logger().info(
        "%s [%s] | %s | %s", prefix, _ts(), action, msg
    )

def log_ws_event(action: str, msg: str) -> None:
    _get_logger().debug("[DEBUG] [%s] | [WS] %s | %s", _ts(), action, msg)

###############################################################################
# SECTION 5 -- AUDIT LOGGER (structured JSON)
# v9.4.0: log_trade() writes ONLY to JSONL file -- zero console output
###############################################################################
class AuditLogger:
    def __init__(self, fh: Optional[logging.FileHandler] = None) -> None:
        self._fh = fh

    def _emit(self, record: Dict) -> None:
        record["ts"] = datetime.now(timezone.utc).isoformat()
        try:
            line = _json_dumps_compact(record).decode() + "\n"
            if self._fh:
                self._fh.stream.write(line)
                self._fh.stream.flush()
        except Exception:
            pass

    def log_trade(
        self, strategy: str, action: str, symbol: str, price: float,
        mart_level: int, pnl_round: Decimal, pnl_day: Decimal,
        fee: Decimal, extra: str = "",
    ) -> None:
        # v9.4.0: Writes ONLY to JSONL audit file -- NO console print to avoid duplication
        # Console output for fills is handled exclusively by the matching engine [FILLED] log
        self._emit({
            "event": "TRADE", "strategy": strategy, "action": action,
            "symbol": symbol, "price": price, "mart_level": mart_level,
            "pnl_round": float(pnl_round), "pnl_day": float(pnl_day),
            "fee": float(fee), "extra": extra,
        })

    def log_event(self, strategy: str, action: str, message: str) -> None:
        self._emit({"event": "BOT_EVENT", "strategy": strategy,
                    "action": action, "message": message})
        _get_logger().info("[%s] [%s] [%s] | %s", strategy, action, _ts(), message)

    def log_error(self, strategy: str, action: str, message: str) -> None:
        self._emit({"event": "ERROR", "strategy": strategy,
                    "action": action, "message": message})
        _get_logger().error("[%s] [%s] [ERROR] [%s] | %s", strategy, action, _ts(), message)

    def log_order_modify(
        self, order_uuid: str, old_price: float, new_price: float, reason: str
    ) -> None:
        self._emit({
            "event": "ORDER_MODIFY", "uuid": order_uuid,
            "old_price": old_price, "new_price": new_price, "reason": reason,
        })
        _get_logger().info(
            f"[AUDIT] ORDER_MODIFY | uuid={order_uuid[:12]}... | "
            f"old={old_price:.4f} -> new={new_price:.4f} | reason={reason}"
        )

    def log_order_cancel(
        self, order_uuid: str, price: float, reason: str
    ) -> None:
        self._emit({
            "event": "ORDER_CANCEL", "uuid": order_uuid,
            "price": price, "reason": reason,
        })
        _get_logger().info(
            f"[AUDIT] ORDER_CANCEL | uuid={order_uuid[:12]}... | "
            f"price={price:.4f} | reason={reason}"
        )

    def log_capital_event(
        self, event_type: str, bankroll: Decimal, trigger: str, detail: str
    ) -> None:
        self._emit({
            "event": "CAPITAL_SAFETY", "type": event_type,
            "bankroll": float(bankroll), "trigger": trigger, "detail": detail,
        })
        _get_logger().critical(
            "[CAPITAL_SAFETY] [%s] [%s] | bankroll=%s | %s", event_type,
            _ts(), bankroll, detail
        )

###############################################################################
# SECTION 6 -- ENUMS & DATA STRUCTURES
###############################################################################
class VolRegime(Enum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"

class ArbStatus(Enum):
    OPPORTUNITY = "OPPORTUNITY"
    REJECT_PEG_TOO_HIGH = "REJECT_PEG_TOO_HIGH"
    REJECT_NEGATIVE_PROFIT = "REJECT_NEGATIVE_PROFIT"
    REJECT_NO_LIQUIDITY_UP = "REJECT_NO_LIQUIDITY_UP"
    REJECT_NO_LIQUIDITY_DOWN = "REJECT_NO_LIQUIDITY_DOWN"
    REJECT_VWAP_BREAKS_PEG = "REJECT_VWAP_BREAKS_PEG"
    REJECT_EMPTY_BOOK = "REJECT_EMPTY_BOOK"
    REJECT_BUDGET_TOO_LOW = "REJECT_BUDGET_TOO_LOW"

@dataclass(frozen=True)
class OrderBookLevel:
    price: float
    size: float

@dataclass(slots=True)
class OrderBookSide:
    levels: List[OrderBookLevel] = field(default_factory=list)

    @property
    def best_price(self) -> Optional[float]:
        return self.levels[0].price if self.levels else None

    @property
    def best_size(self) -> Optional[float]:
        return self.levels[0].size if self.levels else None

    @property
    def is_empty(self) -> bool:
        return len(self.levels) == 0

    def total_volume(self) -> float:
        return sum(lv.size for lv in self.levels)

    @classmethod
    def from_raw(cls, entries: List[Dict], is_bid: bool = False) -> "OrderBookSide":
        levels: List[OrderBookLevel] = []
        for e in entries:
            try:
                sz = float(e.get("size", 0))
                pr = float(e["price"])
                if sz > 0 and 0.0 < pr < 1.0:
                    levels.append(OrderBookLevel(price=pr, size=sz))
            except (KeyError, ValueError):
                continue
        levels.sort(key=lambda lv: lv.price, reverse=is_bid)
        return cls(levels=levels)

@dataclass(frozen=True, slots=True)
class ArbResult:
    status: ArbStatus
    lowest_ask_up: float = 0.0
    lowest_ask_down: float = 0.0
    peg: float = 0.0
    gross_margin: float = 0.0
    shares: float = 0.0
    cost_up: float = 0.0
    cost_down: float = 0.0
    total_cost: float = 0.0
    payout: float = 0.0
    net_profit: float = 0.0
    profit_pct: float = 0.0
    used_vwap: bool = False
    vwap_up: Optional[float] = None
    vwap_down: Optional[float] = None
    volume_at_ask_up: float = 0.0
    volume_at_ask_down: float = 0.0
    reason: str = ""

@dataclass
class Trade:
    side: str
    ask: float
    bid_at_buy: Optional[float]
    eff_c: float
    shares: Decimal
    target: Optional[float]
    type: str
    invested_pure: Decimal
    fee_buy: Decimal
    total_out: Decimal
    token_id: Optional[str]
    partial_tp_done: bool = False
    filled: bool = True
    order_uuid: str = field(default_factory=lambda: str(uuid.uuid4()))

@dataclass
class PendingSettlement:
    trades: List[Trade] = field(default_factory=list)
    heuristic_winner: Optional[str] = None
    winner_token: Optional[str] = None
    meta: Dict[str, str] = field(default_factory=dict)
    created_ts: float = field(default_factory=time.time)
    theoretical_pnl: Decimal = field(default_factory=lambda: _ZERO)

    @property
    def locked_capital(self) -> Decimal:
        return sum(t.total_out for t in self.trades if t.filled)

    @property
    def is_stale(self) -> bool:
        return (time.time() - self.created_ts) > 600.0


###############################################################################
# SECTION 7 -- EVENT BUS
###############################################################################
class EventType(Enum):
    PRICE_UPDATE = "PRICE_UPDATE"
    BOOK_SNAPSHOT = "BOOK_SNAPSHOT"
    MARKET_RESOLVED = "MARKET_RESOLVED"
    BINANCE_TICK = "BINANCE_TICK"
    SHUTDOWN = "SHUTDOWN"
    CYCLE_TICK = "CYCLE_TICK"

@dataclass(slots=True)
class MarketEvent:
    type: EventType
    ts: float = field(default_factory=time.time)
    payload: Dict = field(default_factory=dict)

class EventBus:
    def __init__(self, maxsize: int = 16384) -> None:
        self._q: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=maxsize)
        # v9.4.0: throttle overflow log to once per 10s to avoid spam
        self._last_overflow_log: float = 0.0
        self._overflow_throttle_s: float = 10.0

    async def publish(self, event: MarketEvent) -> None:
        try:
            self._q.put_nowait(event)
        except asyncio.QueueFull:
            # v9.4.0: only log overflow once every 10 seconds
            _now = time.time()
            if _now - self._last_overflow_log >= self._overflow_throttle_s:
                log_debug("[EventBus] OVERFLOW -- dropping oldest event (throttled log)")
                self._last_overflow_log = _now
            try:
                self._q.get_nowait()
            except asyncio.QueueEmpty:
                pass
            await self._q.put(event)

    async def consume(self, timeout: float = 1.0) -> Optional[MarketEvent]:
        try:
            return await asyncio.wait_for(self._q.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    def qsize(self) -> int:
        return self._q.qsize()

###############################################################################
# SECTION 8 -- BOT CONTEXT
###############################################################################
@dataclass
class L2Snapshot:
    bids: List[Tuple[float, float]] = field(default_factory=list)
    asks: List[Tuple[float, float]] = field(default_factory=list)
    ts: float = 0.0

    def best_bid(self) -> Optional[float]:
        return self.bids[0][0] if self.bids else None

    def best_ask(self) -> Optional[float]:
        return self.asks[0][0] if self.asks else None

    def bid_depth(self, levels: int = 3) -> float:
        return sum(s for _, s in self.bids[:levels])

    def ask_depth(self, levels: int = 3) -> float:
        return sum(s for _, s in self.asks[:levels])

    def mid(self) -> Optional[float]:
        b, a = self.best_bid(), self.best_ask()
        if b is None or a is None:
            return None
        return (b + a) * 0.5

    def spread_cents(self) -> Optional[float]:
        b, a = self.best_bid(), self.best_ask()
        if b is None or a is None:
            return None
        return (a - b) * 100.0

    def is_stale(self, threshold_s: float = 3.0) -> bool:
        return (time.time() - self.ts) > threshold_s if self.ts > 0 else True

###############################################################################
# SECTION 8b -- SHADOW FILL ENGINE (v9.4.0)
# Realistic fill simulation: match shadow orders vs live L2 book + latency.
###############################################################################
@dataclass
class ShadowFillResult:
    filled: bool = False
    fill_price: float = 0.0
    shares_filled: float = 0.0
    slippage_pct: float = 0.0
    reject_reason: str = ""
    latency_ms: float = 0.0


class ShadowFillEngine:
    """Simulates realistic order fills against the LIVE L2 order book.

    For every shadow order:
      1. Snapshot best_ask from live WS book (pre-latency price).
      2. Sleep shadow_latency_ms (simulates network + matching engine delay).
      3. Re-read the live L2 book AFTER the latency window.
      4. Walk the post-latency asks to compute VWAP for requested shares.
      5. If VWAP > pre_ask * (1 + shadow_max_slippage_pct) -> REJECT.
      6. Otherwise FILL at VWAP price.

    This means fills can be rejected due to:
      - Book moved against us during latency window
      - Insufficient depth at acceptable prices
      - Slippage exceeds 2% threshold
    """

    def __init__(self, cfg: "BotConfig") -> None:
        self._latency_s = cfg.shadow_latency_ms / 1000.0
        self._max_slip = cfg.shadow_max_slippage_pct
        self._fill_count = 0
        self._reject_count = 0

    async def try_fill(
        self,
        side: str,
        shares: float,
        initial_ask: float,
        ctx: "BotContext",
    ) -> ShadowFillResult:
        """Attempt to fill a shadow BUY order against the live L2 book.

        Args:
            side: "UP" or "DOWN"
            shares: number of shares to buy
            initial_ask: best ask at the moment the order was created
            ctx: BotContext with live L2 data from WS
        Returns:
            ShadowFillResult with fill details or rejection reason.
        """
        side_key = side.lower()

        # ── Step 1: verify pre-latency book is alive ─────────────────────────
        l2_pre = ctx.l2_up if side_key == "up" else ctx.l2_down
        if l2_pre.is_stale(3.0) or not l2_pre.asks:
            self._reject_count += 1
            return ShadowFillResult(
                reject_reason=f"STALE_BOOK_{side} (pre-latency)"
            )

        # ── Step 2: simulate network + matching engine latency ───────────────
        _lat_start = time.time()
        jitter = random.uniform(0.6, 1.0)  # 60-100% of configured latency
        await asyncio.sleep(self._latency_s * jitter)
        actual_lat_ms = (time.time() - _lat_start) * 1000.0

        # ── Step 3: re-read live L2 AFTER latency ────────────────────────────
        l2_post = ctx.l2_up if side_key == "up" else ctx.l2_down
        if l2_post.is_stale(3.0) or not l2_post.asks:
            self._reject_count += 1
            return ShadowFillResult(
                latency_ms=actual_lat_ms,
                reject_reason=f"STALE_BOOK_{side} (post-latency)",
            )

        # ── Step 4: walk post-latency asks to compute VWAP ──────────────────
        total_cost = 0.0
        filled = 0.0
        for price, size in sorted(l2_post.asks, key=lambda x: x[0]):
            if filled >= shares:
                break
            take = min(size, shares - filled)
            total_cost += take * price
            filled += take

        if filled < shares * 0.95:
            self._reject_count += 1
            return ShadowFillResult(
                latency_ms=actual_lat_ms,
                reject_reason=(
                    f"INSUFFICIENT_DEPTH_{side} "
                    f"need={shares:.4f} have={filled:.4f}"
                ),
            )

        vwap = total_cost / filled if filled > 1e-9 else initial_ask

        # ── Step 5: slippage check (max 2% from initial ask) ─────────────────
        slippage = (vwap - initial_ask) / initial_ask if initial_ask > 1e-9 else 0.0
        if slippage > self._max_slip:
            self._reject_count += 1
            return ShadowFillResult(
                fill_price=vwap,
                shares_filled=filled,
                slippage_pct=slippage,
                latency_ms=actual_lat_ms,
                reject_reason=(
                    f"SLIPPAGE_{side} "
                    f"vwap={vwap:.4f} vs ask={initial_ask:.4f} "
                    f"slip={slippage:.4%} > max={self._max_slip:.1%}"
                ),
            )

        # ── Step 6: FILL at VWAP ─────────────────────────────────────────────
        self._fill_count += 1
        return ShadowFillResult(
            filled=True,
            fill_price=vwap,
            shares_filled=filled,
            slippage_pct=slippage,
            latency_ms=actual_lat_ms,
        )

    async def try_fill_sell(
        self,
        side: str,
        shares: float,
        initial_bid: float,
        ctx: "BotContext",
    ) -> ShadowFillResult:
        """v9.5.0: Simulate a SELL order against the live L2 BID book.

        Mirrors try_fill() logic exactly but walks bids (descending) instead of asks.
        """
        side_key = side.lower()

        # ── Step 1: verify pre-latency book is alive ─────────────────────────
        l2_pre = ctx.l2_up if side_key == "up" else ctx.l2_down
        if l2_pre.is_stale(3.0) or not l2_pre.bids:
            self._reject_count += 1
            return ShadowFillResult(
                reject_reason=f"STALE_BOOK_SELL_{side} (pre-latency)"
            )

        # ── Step 2: simulate network + matching engine latency ───────────────
        _lat_start = time.time()
        jitter = random.uniform(0.6, 1.0)
        await asyncio.sleep(self._latency_s * jitter)
        actual_lat_ms = (time.time() - _lat_start) * 1000.0

        # ── Step 3: re-read live L2 AFTER latency ────────────────────────────
        l2_post = ctx.l2_up if side_key == "up" else ctx.l2_down
        if l2_post.is_stale(3.0) or not l2_post.bids:
            self._reject_count += 1
            return ShadowFillResult(
                latency_ms=actual_lat_ms,
                reject_reason=f"STALE_BOOK_SELL_{side} (post-latency)",
            )

        # ── Step 4: walk post-latency bids (descending) to compute VWAP ─────
        total_proceeds = 0.0
        filled = 0.0
        for price, size in sorted(l2_post.bids, key=lambda x: -x[0]):
            if filled >= shares:
                break
            take = min(size, shares - filled)
            total_proceeds += take * price
            filled += take

        if filled < shares * 0.95:
            self._reject_count += 1
            return ShadowFillResult(
                latency_ms=actual_lat_ms,
                reject_reason=(
                    f"INSUFFICIENT_BID_DEPTH_{side} "
                    f"need={shares:.4f} have={filled:.4f}"
                ),
            )

        vwap = total_proceeds / filled if filled > 1e-9 else initial_bid

        # ── Step 5: slippage check (max 2% adverse from initial bid) ─────────
        slippage = (initial_bid - vwap) / initial_bid if initial_bid > 1e-9 else 0.0
        if slippage > self._max_slip:
            self._reject_count += 1
            return ShadowFillResult(
                fill_price=vwap,
                shares_filled=filled,
                slippage_pct=slippage,
                latency_ms=actual_lat_ms,
                reject_reason=(
                    f"SELL_SLIPPAGE_{side} "
                    f"vwap={vwap:.4f} vs bid={initial_bid:.4f} "
                    f"slip={slippage:.4%} > max={self._max_slip:.1%}"
                ),
            )

        # ── Step 6: FILL at VWAP ─────────────────────────────────────────────
        self._fill_count += 1
        return ShadowFillResult(
            filled=True,
            fill_price=vwap,
            shares_filled=filled,
            slippage_pct=slippage,
            latency_ms=actual_lat_ms,
        )

    async def try_fill_limit_no(
        self,
        side: str,
        shares: float,
        limit_price: float,
        ctx: "BotContext",
        max_total_cost_pct: float = 0.93,
        yes_total_out: float = 0.0,
    ) -> ShadowFillResult:
        """v9.5.0: Two-phase verification for NO hedge limit order fill.

        Phase 1: Check effective liquidity within ±2% of limit_price.
                 Compute total cost with slippage + fees.
        Wait 50ms.
        Phase 2: Repeat check. Only fill if BOTH pass AND
                 (yes_total_out + no_cost + fees) / shares <= max_total_cost_pct.

        Args:
            side: "UP" or "DOWN" (the NO side being bought)
            shares: exact number of shares (= YES trade shares)
            limit_price: dynamic max_hedge_price
            ctx: BotContext
            max_total_cost_pct: maximum total cost per share (YES+NO+fees) ≤ 0.93
            yes_total_out: total cost of the YES leg
        """
        side_key = side.lower()
        slip_pct = 0.02  # ±2% slippage tolerance
        max_price_with_slip = limit_price * (1.0 + slip_pct)

        async def _verify_phase(phase_label: str) -> Tuple[bool, float, float, float, str]:
            """Returns (ok, eff_vwap, eff_cost, eff_filled, reason)."""
            l2 = ctx.l2_up if side_key == "up" else ctx.l2_down
            if l2.is_stale(3.0) or not l2.asks:
                return False, 0.0, 0.0, 0.0, f"STALE_BOOK_{phase_label}"

            # Walk asks within ±2% of limit_price (up to 15+ levels)
            eff_cost = 0.0
            eff_filled = 0.0
            asks_sorted = sorted(l2.asks, key=lambda x: x[0])
            for price, size in asks_sorted[:max(15, len(asks_sorted))]:
                if price > max_price_with_slip:
                    break
                if eff_filled >= shares:
                    break
                take = min(size, shares - eff_filled)
                eff_cost += take * price
                eff_filled += take

            if eff_filled < shares:
                return (False, 0.0, eff_cost, eff_filled,
                        f"INSUFFICIENT_LIQ_{phase_label} "
                        f"need={shares:.4f} have={eff_filled:.4f}")

            eff_vwap = eff_cost / eff_filled if eff_filled > 1e-9 else limit_price

            # Add Polymarket fee on the NO leg
            fee_no = polymarket_fee(eff_filled, eff_vwap)
            total_no_cost = eff_cost + fee_no

            # Check total cost constraint: (YES_out + NO_cost) / shares <= 0.93
            if shares > 1e-9 and yes_total_out > 0.0:
                cost_per_share = (yes_total_out + total_no_cost) / shares
                if cost_per_share > max_total_cost_pct:
                    return (False, eff_vwap, total_no_cost, eff_filled,
                            f"TOTAL_COST_EXCEEDED_{phase_label} "
                            f"cost/share={cost_per_share:.4f} > "
                            f"max={max_total_cost_pct:.2f}")

            return True, eff_vwap, total_no_cost, eff_filled, ""

        # ── Phase 1 ──────────────────────────────────────────────────────────
        ok1, vwap1, cost1, filled1, reason1 = await _verify_phase("P1")
        if not ok1:
            self._reject_count += 1
            return ShadowFillResult(
                reject_reason=f"VOL_HEDGE_NO_REJECT_P1 | {reason1}"
            )

        # ── Wait 50ms ────────────────────────────────────────────────────────
        await asyncio.sleep(0.050)

        # ── Phase 2 ──────────────────────────────────────────────────────────
        ok2, vwap2, cost2, filled2, reason2 = await _verify_phase("P2")
        if not ok2:
            self._reject_count += 1
            return ShadowFillResult(
                fill_price=vwap1,
                shares_filled=filled1,
                latency_ms=50.0,
                reject_reason=f"VOL_HEDGE_NO_REJECT_P2 | {reason2}"
            )

        # ── Both phases passed → FILL at worst VWAP of the two ──────────────
        final_vwap = max(vwap1, vwap2)  # worst case
        slippage = (final_vwap - limit_price) / limit_price if limit_price > 1e-9 else 0.0
        self._fill_count += 1
        return ShadowFillResult(
            filled=True,
            fill_price=final_vwap,
            shares_filled=min(filled1, filled2),
            slippage_pct=slippage,
            latency_ms=50.0,
        )

    @property
    def stats(self) -> str:
        total = self._fill_count + self._reject_count
        rate = (self._fill_count / total * 100.0) if total > 0 else 0.0
        return (f"fills={self._fill_count} rejects={self._reject_count} "
                f"rate={rate:.1f}%")


###############################################################################
# SECTION 9 -- BOT CONTEXT
###############################################################################
@dataclass
class BotContext:
    cfg: BotConfig
    audit: AuditLogger
    event_bus: EventBus = field(default_factory=EventBus)
    l2_up: L2Snapshot = field(default_factory=L2Snapshot)
    l2_down: L2Snapshot = field(default_factory=L2Snapshot)
    best_bids: Dict[str, Optional[float]] = field(
        default_factory=lambda: {"up": None, "down": None}
    )
    best_asks: Dict[str, Optional[float]] = field(
        default_factory=lambda: {"up": None, "down": None}
    )
    best_spreads_c: Dict[str, Optional[float]] = field(
        default_factory=lambda: {"up": None, "down": None}
    )
    best_bid_sizes: Dict[str, Optional[float]] = field(
        default_factory=lambda: {"up": None, "down": None}
    )
    best_ask_sizes: Dict[str, Optional[float]] = field(
        default_factory=lambda: {"up": None, "down": None}
    )
    fee_cache: Dict[str, int] = field(default_factory=dict)
    shutdown_flag: bool = False
    resolved_event: asyncio.Event = field(default_factory=asyncio.Event)
    resolved_winner_asset: Optional[str] = None
    clob_client: Any = None
    clob_ro_client: Any = None
    has_sdk: bool = False
    bot_start_time: float = field(default_factory=time.time)
    trading_disabled: bool = False
    session_stop_reason: str = ""
    pending_orders: Dict[str, Dict] = field(default_factory=dict)
    last_reconciled_bankroll: Decimal = field(default_factory=lambda: _ZERO)
    pending_settlements: List = field(default_factory=list)
    _settlements_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    meta_cb: Any = field(default=None)
    redeem_cb: Any = field(default=None)
    hourly_start_ts: float = field(default_factory=time.time)
    hourly_start_bankroll: Decimal = field(default_factory=lambda: _ZERO)
    hourly_pause_until: float = 0.0
    daily_pause_until: float = 0.0          # v9.5.4: freeze instead of halt
    current_condition_id: Optional[str] = None
    user_ws_task: Optional[asyncio.Task] = None
    rate_limiter: Any = None
    api_cb: Any = None
    _first_book_logged: bool = False
    _final_log_done: bool = False
    # v9.4.0: Shadow Fill Engine for realistic dry_run simulation
    shadow_engine: Optional[ShadowFillEngine] = None
    # v9.5.0: Volatility Hedge Engine 1SD-3SD
    vol_hedge_engine: Optional["VolatilityHedgeEngine"] = None
    # v9.4.0: accumulated realized PnL for current cycle
    round_realized_pnl: Decimal = field(default_factory=lambda: _ZERO)

###############################################################################
# SECTION 9 -- TRADE STATE MANAGER
###############################################################################
@dataclass
class TradeState:
    current_martingale_level: int = 1
    accumulated_loss_session: Decimal = field(default_factory=lambda: _ZERO)
    last_round_pnl: Decimal = field(default_factory=lambda: _ZERO)
    daily_pnl: Decimal = field(default_factory=lambda: _ZERO)
    bankroll: Decimal = field(default_factory=lambda: Decimal("10.0"))
    initial_bankroll: Decimal = field(default_factory=lambda: Decimal("10.0"))
    daily_start_bankroll: Decimal = field(default_factory=lambda: Decimal("10.0"))
    last_market_day: Optional[str] = None
    round_count: int = 0
    consecutive_losses: int = 0
    session_start_bankroll: Decimal = field(default_factory=lambda: Decimal("10.0"))

    @property
    def mart_level(self) -> int:
        return self.current_martingale_level

def _trade_to_dict(t: Trade) -> Dict:
    return {
        "side": t.side, "ask": t.ask, "bid_at_buy": t.bid_at_buy,
        "eff_c": t.eff_c, "shares": str(t.shares), "target": t.target,
        "type": t.type, "invested_pure": str(t.invested_pure),
        "fee_buy": str(t.fee_buy), "total_out": str(t.total_out),
        "token_id": t.token_id, "partial_tp_done": t.partial_tp_done,
        "filled": t.filled, "order_uuid": t.order_uuid,
    }

def _dict_to_trade(d: Dict) -> Trade:
    return Trade(
        side=d["side"], ask=float(d["ask"]), bid_at_buy=d.get("bid_at_buy"),
        eff_c=float(d["eff_c"]), shares=_d(d["shares"]), target=d.get("target"),
        type=d["type"], invested_pure=_d(d["invested_pure"]),
        fee_buy=_d(d["fee_buy"]), total_out=_d(d["total_out"]),
        token_id=d.get("token_id"),
        partial_tp_done=bool(d.get("partial_tp_done", False)),
        filled=bool(d.get("filled", True)),
        order_uuid=d.get("order_uuid", str(uuid.uuid4())),
    )

class TradeStateManager:
    def __init__(self, filepath: str, bankroll_demo: Decimal) -> None:
        self.filepath: Path = Path(filepath)
        self._backup: Path = self.filepath.with_suffix(".json.bak")
        self._bankroll_demo: Decimal = bankroll_demo
        self.state: TradeState = TradeState(
            bankroll=bankroll_demo,
            initial_bankroll=bankroll_demo,
            daily_start_bankroll=bankroll_demo,
            session_start_bankroll=bankroll_demo,
        )
        self.active_trades: List[Trade] = []

    def _to_dict(self) -> Dict:
        return {
            "_version": "9.2.8",
            "_saved_at": time.time(),
            "current_martingale_level": self.state.current_martingale_level,
            "accumulated_loss_session": str(self.state.accumulated_loss_session),
            "last_round_pnl": str(self.state.last_round_pnl),
            "daily_pnl": str(self.state.daily_pnl),
            "bankroll": str(self.state.bankroll),
            "initial_bankroll": str(self.state.initial_bankroll),
            "daily_start_bankroll": str(self.state.daily_start_bankroll),
            "last_market_day": self.state.last_market_day,
            "round_count": self.state.round_count,
            "consecutive_losses": self.state.consecutive_losses,
            "session_start_bankroll": str(self.state.session_start_bankroll),
            "active_trades": [_trade_to_dict(t) for t in self.active_trades],
        }

    def _save_blocking(self) -> None:
        raw: bytes = _json_dumps(self._to_dict())
        tmp: Path = self.filepath.with_suffix(".json.tmp")
        try:
            tmp.write_bytes(raw)
            if self.filepath.exists():
                try:
                    self._backup.unlink(missing_ok=True)
                    self.filepath.rename(self._backup)
                except OSError:
                    pass
            tmp.rename(self.filepath)
        except OSError as exc:
            log_warn(f"[STATE] Save FAILED: {exc}")
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    def save(self) -> None:
        self._save_blocking()

    async def save_async(self) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._save_blocking)

    def load(self) -> bool:
        for path in (self.filepath, self._backup):
            if not path.exists():
                continue
            try:
                raw = path.read_bytes()
                data = _json_loads(raw)
                assert isinstance(data, dict)
                br = _d(data.get("bankroll", str(self._bankroll_demo)))
                self.state = TradeState(
                    current_martingale_level=int(
                        data.get("current_martingale_level", 1)
                    ),
                    accumulated_loss_session=_d(
                        data.get("accumulated_loss_session", "0")
                    ),
                    last_round_pnl=_d(data.get("last_round_pnl", "0")),
                    daily_pnl=_d(data.get("daily_pnl", "0")),
                    bankroll=br,
                    initial_bankroll=_d(
                        data.get("initial_bankroll", str(br))
                    ),
                    daily_start_bankroll=_d(
                        data.get("daily_start_bankroll", str(br))
                    ),
                    last_market_day=data.get("last_market_day"),
                    round_count=int(data.get("round_count", 0)),
                    consecutive_losses=int(data.get("consecutive_losses", 0)),
                    session_start_bankroll=_d(
                        data.get("session_start_bankroll", str(br))
                    ),
                )
                raw_trades = data.get("active_trades", [])
                self.active_trades = []
                for td in raw_trades:
                    try:
                        self.active_trades.append(_dict_to_trade(td))
                    except Exception:
                        pass
                log_info(
                    f"[STATE] Loaded {path.name} v{data.get('_version', '?')} | "
                    f"Mart=x{self.state.mart_level} | "
                    f"Bankroll={self.state.bankroll} | "
                    f"active_trades={len(self.active_trades)}"
                )
                return True
            except Exception as exc:
                log_warn(f"[STATE] Corrupt {path.name}: {exc}")
        log_info(f"[STATE] Fresh start | Bankroll={self._bankroll_demo}")
        return False

    def add_trade(self, t: Trade) -> None:
        self.active_trades.append(t)

    def remove_trade(self, t: Trade) -> None:
        try:
            self.active_trades.remove(t)
        except ValueError:
            pass

    def update_martingale(self, round_pnl: Decimal, cfg: BotConfig,
                          edge: float = 0.0) -> None:
        self.state.last_round_pnl = round_pnl
        eps = _d("1e-9")
        if round_pnl > eps:
            old = self.state.current_martingale_level
            if old > 1:
                log_info(
                    f"[MART] WIN x{old}->x1 | recovered "
                    f"{self.state.accumulated_loss_session}"
                )
            self.state.current_martingale_level = 1
            self.state.accumulated_loss_session = _ZERO
            self.state.consecutive_losses = 0
        elif round_pnl < -eps:
            old = self.state.current_martingale_level
            self.state.accumulated_loss_session += abs(round_pnl)
            self.state.consecutive_losses += 1
            effective_new = min(old + 1, cfg.mart_max_mult)
            if effective_new != old:
                self.state.current_martingale_level = effective_new
                log_warn(
                    f"[MART_OPT] level=x{effective_new} "
                    f"(recovery_factor={cfg.mart_recovery_factor:.2f}) | "
                    f"acc_loss=${self.state.accumulated_loss_session:.4f} | "
                    f"edge={edge:+.4f}"
                )
            else:
                log_warn(
                    f"[MART_OPT] MAX x{cfg.mart_max_mult} | pnl={round_pnl} | "
                    f"acc=${self.state.accumulated_loss_session:.4f} | "
                    f"edge={edge:+.4f}"
                )

    def calc_next_stake(
        self, base_stake: Decimal, ask: float, token_id: str,
        fee_fn: Callable[[str], float], bankroll: Decimal,
        edge: float = 0.0, kelly_assumed_edge: float = 0.040,
        mart_recovery_factor: float = 1.22, kelly_mart_boost: float = 0.30,
    ) -> Decimal:
        # v9.4.0: apply mart_recovery_factor to amplify recovery staking
        raw_stake = base_stake * _d(self.state.mart_level) * _d(mart_recovery_factor)
        fee = _d(fee_fn(token_id))
        acc = self.state.accumulated_loss_session
        if acc > _ZERO and 0.0 < ask < 1.0:
            ask_d = _d(ask)
            margin = _ONE - ask_d * (_ONE + fee)
            if margin > _d("1e-9"):
                min_stake = (acc / margin) * ask_d * (_ONE + fee)
                if min_stake > raw_stake:
                    raw_stake = min_stake
        # v9.4.0: kelly boost when edge significantly above expected
        if edge > kelly_assumed_edge * 1.2:
            raw_stake = _dq(raw_stake * _d(1.0 + kelly_mart_boost))
        cap = _dq(bankroll * _d("0.20"))
        raw_stake = min(_dq(raw_stake), cap, bankroll)
        return raw_stake

    def update_daily_pnl(self, round_pnl: Decimal) -> None:
        self.state.daily_pnl += round_pnl
        self.state.round_count += 1

    def reset_daily(self, new_day: str) -> None:
        self.state.daily_start_bankroll = self.state.bankroll
        self.state.daily_pnl = _ZERO
        self.state.round_count = 0
        self.state.last_market_day = new_day
        log_info(
            f"[STATE] NEW DAY {new_day} | daily_start={self.state.daily_start_bankroll} | "
            f"Mart x{self.state.mart_level}"
        )

    def update_bankroll(self, new_br: Decimal) -> None:
        self.state.bankroll = new_br

    def pnl_daily_pct(self) -> float:
        base = self.state.daily_start_bankroll
        if base < _d("1e-9"):
            return 0.0
        return _safe_float(self.state.daily_pnl / base * 100)

    def pnl_total_pct(self) -> float:
        ib = self.state.initial_bankroll
        if ib < _d("1e-9"):
            return 0.0
        return _safe_float((self.state.bankroll - ib) / ib * 100)

    def session_loss_pct(self) -> float:
        sb = self.state.session_start_bankroll
        if sb < _d("1e-9"):
            return 0.0
        loss = sb - self.state.bankroll
        return _safe_float(loss / sb * 100)

###############################################################################
# SECTION 10 -- CAPITAL SAFETY MONITOR
###############################################################################
class CapitalSafetyMonitor:
    def __init__(
        self, tsm: TradeStateManager, cfg: BotConfig, audit: AuditLogger
    ) -> None:
        self._tsm = tsm
        self._cfg = cfg
        self._audit = audit
        self._last_safety_check_ts: float = 0.0

    def check(self, ctx: "BotContext") -> bool:
        """v9.5.4: Daily loss → 30min FREEZE (not halt). Hourly → 15min FREEZE."""
        now = time.time()
        state = self._tsm.state
        cfg = self._cfg

        # Check daily freeze first (higher priority)
        if ctx.daily_pause_until > now:
            _rem = ctx.daily_pause_until - now
            if now - self._last_safety_check_ts > 30.0:
                self._last_safety_check_ts = now
                log_warn(f"[SAFETY] DAILY FREEZE active | resume in {_rem:.0f}s")
            return True

        # Check hourly freeze
        if ctx.hourly_pause_until > now:
            _rem = ctx.hourly_pause_until - now
            if now - self._last_safety_check_ts > 30.0:
                self._last_safety_check_ts = now
                log_warn(f"[SAFETY] HOURLY FREEZE active | resume in {_rem:.0f}s")
            return True

        # Consecutive losses → halt (permanent until restart)
        if state.consecutive_losses >= cfg.max_consecutive_losses:
            self._halt(ctx, "CONSECUTIVE_LOSSES",
                       f"consecutive_losses={state.consecutive_losses} >= "
                       f"{cfg.max_consecutive_losses}")
            return True

        if now - self._last_safety_check_ts < 30.0:
            return False
        self._last_safety_check_ts = now

        # Daily loss check → 30min FREEZE (not permanent halt)
        daily_base = state.daily_start_bankroll
        if daily_base > _d("1e-9"):
            daily_loss_pct = _safe_float(
                (daily_base - state.bankroll) / daily_base * 100
            )
            if daily_loss_pct >= cfg.max_daily_loss_pct:
                _pause_s = cfg.daily_pause_duration_s
                ctx.daily_pause_until = now + _pause_s
                log_warn(
                    f"[SAFETY] DAILY LOSS FREEZE | loss={daily_loss_pct:.2f}% "
                    f">= {cfg.max_daily_loss_pct}% | freezing {_pause_s:.0f}s (30min)"
                )
                self._audit.log_capital_event(
                    "DAILY_FREEZE", state.bankroll, "DAILY_LOSS",
                    f"loss={daily_loss_pct:.2f}% -- freeze {_pause_s:.0f}s"
                )
                asyncio.ensure_future(send_alert(
                    f"🚨 [XRP_BOT v9.5.4] DAILY LOSS FREEZE | "
                    f"loss={daily_loss_pct:.2f}% | bankroll={state.bankroll} | "
                    f"TRADING PAUSED {_pause_s:.0f}s",
                    cfg,
                ))
                return True

        # Hourly loss check → 15min FREEZE (isolated from daily)
        hourly_base = ctx.hourly_start_bankroll
        if hourly_base > _d("1e-9"):
            hourly_loss_pct = _safe_float(
                (hourly_base - state.bankroll) / hourly_base * 100
            )
            if hourly_loss_pct >= cfg.max_hourly_loss_pct:
                _pause_h = cfg.hourly_pause_duration_s
                ctx.hourly_pause_until = now + _pause_h
                log_warn(
                    f"[SAFETY] HOURLY LOSS FREEZE | loss={hourly_loss_pct:.2f}% "
                    f">= {cfg.max_hourly_loss_pct}% | freezing {_pause_h:.0f}s (15min)"
                )
                self._audit.log_capital_event(
                    "HOURLY_FREEZE", state.bankroll, "HOURLY_LOSS",
                    f"loss={hourly_loss_pct:.2f}% -- freeze {_pause_h:.0f}s"
                )
                ctx.hourly_start_ts = now + _pause_h
                ctx.hourly_start_bankroll = state.bankroll
                return True

        if now - ctx.hourly_start_ts >= 3600.0:
            ctx.hourly_start_ts = now
            ctx.hourly_start_bankroll = state.bankroll
        return False

    def reset_daily(self) -> None:
        pass

    def _halt(self, ctx: "BotContext", reason: str, detail: str) -> None:
        ctx.trading_disabled = True
        ctx.session_stop_reason = reason
        if reason == "DAILY_LOSS_LIMIT":
            log_info(f"DAILY LOSS HALT | {detail}")
        else:
            self._audit.log_capital_event("HALT", self._tsm.state.bankroll,
                                          reason, detail)

###############################################################################
# SECTION 11 -- BINANCE ORACLE
###############################################################################
@dataclass
class BinanceState:
    current_price: Optional[float] = None
    cycle_open_price: Optional[float] = None
    last_update_ts: float = 0.0
    connected: bool = False
    tick_count: int = 0
    _returns: deque = field(default_factory=lambda: deque(maxlen=30))
    _vol_annual: float = 1.20
    _ewma_var: Optional[float] = None
    _prev_price: Optional[float] = None
    _price_history_10s: deque = field(
        default_factory=lambda: deque(maxlen=30)
    )
    _last_10s_snap_ts: float = 0.0
    _short_returns: deque = field(default_factory=lambda: deque(maxlen=6))
    _long_returns: deque = field(default_factory=lambda: deque(maxlen=30))
    # v9.4.0: vol regime hysteresis state
    _last_regime: str = "NORMAL"
    _last_regime_ts: float = 0.0

    def update_price(self, price: float, ewma_alpha: float = 0.06) -> None:
        prev = self._prev_price
        self.current_price = price
        self.last_update_ts = time.time()
        self.tick_count += 1
        if prev is not None and prev > 1e-9:
            log_ret = math.log(price / prev)
            self._returns.append(log_ret)
            self._short_returns.append(log_ret)
            self._long_returns.append(log_ret)
            sq = log_ret ** 2
            if self._ewma_var is None:
                self._ewma_var = sq
            else:
                self._ewma_var = ewma_alpha * sq + (1.0 - ewma_alpha) * self._ewma_var
            if self._ewma_var is not None:
                self._vol_annual = max(
                    math.sqrt(self._ewma_var) * math.sqrt(_SECS_PER_YEAR),
                    1.20 * 0.10,
                )
        self._prev_price = price
        now = self.last_update_ts
        if now - self._last_10s_snap_ts >= 10.0:
            self._price_history_10s.append((now, price))
            self._last_10s_snap_ts = now

    @property
    def vol_annual(self) -> float:
        return self._vol_annual if len(self._returns) >= 5 else 1.20

    @property
    def vol_short(self) -> float:
        n = len(self._short_returns)
        if n < 3:
            return self.vol_annual
        vals = list(self._short_returns)
        mean = sum(vals) / n
        var = sum((r - mean) ** 2 for r in vals) / max(n - 1, 1)
        return max(math.sqrt(var) * math.sqrt(_SECS_PER_YEAR), 0.01)

    @property
    def vol_long(self) -> float:
        n = len(self._long_returns)
        if n < 5:
            return self.vol_annual
        vals = list(self._long_returns)
        mean = sum(vals) / n
        var = sum((r - mean) ** 2 for r in vals) / max(n - 1, 1)
        return max(math.sqrt(var) * math.sqrt(_SECS_PER_YEAR), 0.01)

    def recent_trend(self, lookback_s: float = 600.0) -> str:
        """Return 'FALLING', 'RISING' or 'FLAT' based on 10min price history."""
        if len(self._price_history_10s) < 2:
            return "FLAT"
        now = time.time()
        cutoff = now - lookback_s
        pts = [(ts, p) for ts, p in self._price_history_10s if ts >= cutoff]
        if len(pts) < 2:
            return "FLAT"
        first_p = pts[0][1]
        last_p = pts[-1][1]
        if first_p <= 0:
            return "FLAT"
        pct = (last_p - first_p) / first_p * 100.0
        if pct < -0.01:
            return "FALLING"
        elif pct > 0.01:
            return "RISING"
        return "FLAT"

    def get_vol_regime(self, cfg: BotConfig) -> "VolRegime":
        """Compute volatility regime from blended short/long vol.

        BUG FIXES (v9.4.0 corrected):
        - BUG1 FIXED: Default vol_annual=1.20 used to trigger HIGH immediately on
          startup because 1.20 > vol_regime_high_thresh=0.85. Now returns NORMAL
          when there is insufficient data (< 5 returns for annual, < 3 for short).
        - BUG2 FIXED: Probabilistic persistence gate (random.random() < 0.75) was
          randomly blocking legitimate regime switches, causing NORMAL to persist
          when the market genuinely moved to HIGH. Gate removed; only time-based
          hysteresis is used.
        - BUG3 FIXED: Inverted appearance — 'blended=0.184 NORMAL, blended=0.275 LOW'
          was caused by startup fallback inflating vol_short to vol_annual=1.20 then
          real ticks dropping it below 0.35. Now returns NORMAL when data is sparse.
        - THRESHOLD CALIBRATION: XRP intraday vol in annualised terms:
            Calm/stagnant: ~30-50%  -> blended < 0.40  -> LOW
            Normal market: 40-120%  -> 0.40-1.20       -> NORMAL
            Volatile spike: >120%   -> blended > 1.20   -> HIGH
          Old thresholds (LOW<0.35, HIGH>0.85) were triggering LOW on normal XRP
          activity and HIGH on borderline moves.
        """
        # Guard: return NORMAL when we don't have enough data.
        # This prevents the default vol_annual=1.20 from triggering HIGH on startup.
        has_short_data = len(self._short_returns) >= 3
        has_long_data  = len(self._long_returns)  >= 5
        if not has_short_data or not has_long_data:
            return VolRegime("NORMAL")

        # Blended vol: short-term (responsive) weighted more heavily
        blended = (
            self.vol_short * cfg.vol_short_weight
            + self.vol_long * (1.0 - cfg.vol_short_weight)
        )
        now = time.time()

        # Classify raw regime using calibrated thresholds
        # LOW  < 0.40  : stagnant/flat market  (XRP barely moving)
        # HIGH > 1.20  : genuine volatility spike (XRP moving >120% annual rate)
        # NORMAL       : everything in between
        if blended < cfg.vol_regime_low_thresh:
            raw_regime = "LOW"
        elif blended > cfg.vol_regime_high_thresh:
            raw_regime = "HIGH"
        else:
            raw_regime = "NORMAL"

        # Hysteresis gate: must hold new regime signal for vol_hysteresis_s seconds
        # before a switch is confirmed. This prevents thrashing on noisy ticks.
        # The probabilistic gate (random.random()) has been REMOVED -- it was
        # non-deterministic and caused legitimate switches to be silently dropped.
        if raw_regime != self._last_regime:
            if self._last_regime_ts == 0.0:
                # First classification -- accept immediately, no hysteresis needed
                self._last_regime = raw_regime
                self._last_regime_ts = now
            elif (now - self._last_regime_ts) < cfg.vol_hysteresis_s:
                # Hold previous regime until hysteresis window expires
                raw_regime = self._last_regime
            else:
                # Hysteresis elapsed -- confirm the switch
                log_info(
                    f"[VOL_REGIME] Switch {self._last_regime} -> {raw_regime} | "
                    f"blended={blended:.3f} short={self.vol_short:.3f} "
                    f"long={self.vol_long:.3f} | elapsed="
                    f"{now - self._last_regime_ts:.0f}s"
                )
                self._last_regime = raw_regime
                self._last_regime_ts = now
        else:
            # Still in same regime -- update timestamp on first entry
            if self._last_regime_ts == 0.0:
                self._last_regime_ts = now

        return VolRegime(raw_regime)

    @property
    def drift_5m(self) -> Optional[float]:
        if self.current_price is None or len(self._price_history_10s) < 2:
            return None
        _, oldest = self._price_history_10s[0]
        if oldest < 1e-9:
            return None
        return (self.current_price - oldest) / oldest

    def staleness_s(self) -> float:
        return time.time() - self.last_update_ts if self.last_update_ts > 0 \
            else float("inf")

    def is_stale(self, threshold_s: float = 10.0) -> bool:
        return self.staleness_s() > threshold_s

    @staticmethod
    def _fetch_cycle_open_price_rest_sync(
        cfg: BotConfig, cycle_start_ts: float
    ) -> Optional[float]:
        try:
            import urllib.request as _ur
            import urllib.parse as _up
            start_ms = int(cycle_start_ts * 1000)
            params = _up.urlencode({
                "symbol": "XRPUSDT", "interval": "1m",
                "startTime": start_ms - 60000, "endTime": start_ms + 60000,
                "limit": 3,
            })
            url = f"{cfg.binance_rest_klines_url}?{params}"
            with _ur.urlopen(url, timeout=5) as r:
                data = _json_loads(r.read())
                if data and isinstance(data, list):
                    best = None
                    for kline in data:
                        k_ts = int(kline[0]) / 1000.0
                        if abs(k_ts - cycle_start_ts) < abs(
                            (best[0] if best else float("inf")) - cycle_start_ts
                        ):
                            best = (k_ts, float(kline[1]))
                    if best:
                        log_binance(f"REST strike={best[1]:.5f} "
                                 f"at t={best[0]:.0f}")
                        return best[1]
        except Exception as exc:
            log_binance(f"REST fetch failed: {exc}")
        return None

    @staticmethod
    async def fetch_cycle_open_price_rest_async(
        cfg: BotConfig, cycle_start_ts: float
    ) -> Optional[float]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            None, BinanceState._fetch_cycle_open_price_rest_sync, cfg,
            cycle_start_ts
        )

    @staticmethod
    def fetch_cycle_open_price_rest(
        cfg: BotConfig, cycle_start_ts: float
    ) -> Optional[float]:
        return BinanceState._fetch_cycle_open_price_rest_sync(cfg, cycle_start_ts)

@dataclass
class FundingRateState:
    rate: Optional[float] = None
    last_update_ts: float = 0.0
    is_bullish: bool = False
    is_bearish: bool = False

    def update(self, rate: float, cfg: BotConfig) -> None:
        self.rate = rate
        self.last_update_ts = time.time()
        self.is_bullish = rate < cfg.funding_rate_bear_thresh
        self.is_bearish = rate > cfg.funding_rate_bull_thresh

    def is_stale(self, threshold_s: float = 120.0) -> bool:
        return (time.time() - self.last_update_ts) > threshold_s if \
            self.last_update_ts > 0 else True

    @property
    def signal_str(self) -> str:
        if self.rate is None:
            return "n/a"
        tag = " [BULLISH]" if self.is_bullish else (
            " [BEARISH]" if self.is_bearish else ""
        )
        return f"{self.rate:+.6f}{tag}"

###############################################################################
# SECTION 12 -- QUANT MODELS
###############################################################################
_SQRT_2PI: float = math.sqrt(2.0 * math.pi)

def _norm_cdf(x: float) -> float:
    sign = 1.0 if x >= 0.0 else -1.0
    xa = abs(x)
    if xa > 8.0:
        return 1.0 if sign > 0 else 0.0
    t = 1.0 / (1.0 + 0.2316419 * xa)
    t2 = t * t
    t3 = t2 * t
    t4 = t3 * t
    t5 = t4 * t
    poly = 0.319381530 * t + (-0.356563782) * t2 + 1.781477937 * t3 + \
         (-1.821255978) * t4 + 1.330274429 * t5
    pdf_val = math.exp(-0.5 * xa * xa) / _SQRT_2PI
    cdf = 1.0 - pdf_val * poly
    return cdf if sign > 0 else 1.0 - cdf

def _sigmoid(x: float, k: float = 12.0) -> float:
    kx = k * x
    if kx > 50.0:
        return 1.0
    if kx < -50.0:
        return 0.0
    return 1.0 / (1.0 + math.exp(-kx))

def compute_gbm_probability(
    price: float, strike: float, time_remaining_s: float,
    volatility_annual: float,
    prob_min: float = 0.03, prob_max: float = 0.97,
) -> Tuple[float, float]:
    if price <= 0.0 or strike <= 0.0:
        return 0.5, 0.5
    sigma = max(volatility_annual, 0.05)
    t_s = max(time_remaining_s, 0.0)
    if t_s == 0.0:
        p_up = prob_max if price > strike else (
            prob_min if price < strike else 0.5
        )
        return p_up, 1.0 - p_up
    if t_s < 2.0:
        delta_norm = (price - strike) / (strike * sigma)
        p_up = max(prob_min, min(prob_max, _sigmoid(delta_norm)))
        return p_up, max(prob_min, min(prob_max, 1.0 - p_up))
    T = t_s / _SECS_PER_YEAR
    try:
        ln_sk = math.log(price / strike)
    except (ValueError, ZeroDivisionError):
        return 0.5, 0.5
    denom = sigma * math.sqrt(T)
    if denom < 1e-14:
        p_up = prob_max if price > strike else (
            prob_min if price < strike else 0.5
        )
        return p_up, 1.0 - p_up
    d2 = max(-8.0, min(8.0, (ln_sk - 0.5 * sigma * sigma * T) / denom))
    p_up = max(prob_min, min(prob_max, _norm_cdf(d2)))
    return p_up, max(prob_min, min(prob_max, 1.0 - p_up))

def compute_jump_diffusion_probability(
    price: float, strike: float, time_remaining_s: float,
    volatility_annual: float, cfg: BotConfig,
    prob_min: float = 0.03, prob_max: float = 0.97,
) -> Tuple[float, float]:
    lam = cfg.jump_lambda
    mu_j = cfg.jump_mu
    sig_j = cfg.jump_sigma
    n_terms = cfg.jump_terms
    if price <= 0.0 or strike <= 0.0:
        return 0.5, 0.5
    t_s = max(time_remaining_s, 0.0)
    if t_s < 1.0:
        return compute_gbm_probability(price, strike, t_s, volatility_annual,
                                       prob_min, prob_max)
    T = t_s / _SECS_PER_YEAR
    lam_T = lam * T
    sigma_base = max(volatility_annual, 0.05)
    p_up_total = 0.0
    total_weight = 0.0
    try:
        for n in range(n_terms + 1):
            poisson_w = math.exp(-lam_T) * (lam_T ** n) / math.factorial(n)
            sigma_n = math.sqrt(
                sigma_base ** 2 + n * sig_j ** 2 / T
            ) if T > 1e-9 else sigma_base
            k_comp = math.exp(mu_j + 0.5 * sig_j ** 2) - 1.0
            adj_price = price * math.exp(
                -lam * k_comp * T + n * (mu_j + 0.5 * sig_j ** 2)
            )
            p_up_n, _ = compute_gbm_probability(
                adj_price, strike, t_s, sigma_n, prob_min, prob_max
            )
            p_up_total += poisson_w * p_up_n
            total_weight += poisson_w
    except Exception:
        return compute_gbm_probability(price, strike, t_s, volatility_annual,
                                       prob_min, prob_max)
    if total_weight < 1e-12:
        return compute_gbm_probability(price, strike, t_s, volatility_annual,
                                       prob_min, prob_max)
    p_up = max(prob_min, min(prob_max, p_up_total / total_weight))
    p_up = max(prob_min, min(prob_max, p_up))
    p_dn = max(prob_min, min(prob_max, 1.0 - p_up))
    return p_up, p_dn

def calculate_true_prob(
    current_price: Optional[float],
    strike_price: Optional[float],
    seconds_to_close: float,
    binance: BinanceState,
    cfg: BotConfig,
) -> Optional[float]:
    if current_price is None or strike_price is None:
        return None
    p_up, _ = compute_jump_diffusion_probability(
        current_price, strike_price, seconds_to_close, binance.vol_annual, cfg,
    )
    return p_up

###############################################################################
# SECTION 13 -- MARKET MICROSTRUCTURE SIGNALS
###############################################################################
@dataclass
class MicrostructureSignals:
    bid_vol: float = 0.0
    ask_vol: float = 0.0
    spread_cents: float = 0.0
    depth_imbalance: float = 0.5
    liquidity_signal: float = 0.5
    bid_pressure: float = 0.0
    is_volatile: bool = False
    price_change_30s: float = 0.0
    price_change_60s: float = 0.0

class MicrostructureAnalyzer:
    def __init__(self) -> None:
        self._price_history: deque = deque(maxlen=60)
        self._prev_bid_depth: Dict[str, float] = {}
        self._prev_ask_depth: Dict[str, float] = {}
        self._cached_signals: Dict[str, "MicrostructureSignals"] = {}
        self._cache_mid: Dict[str, float] = {}
        self._cache_ticks: Dict[str, int] = {}
    def analyze(
        self, l2_up: L2Snapshot, l2_down: L2Snapshot, side: str,
    ) -> MicrostructureSignals:
        l2 = l2_up if side == "up" else l2_down
        mid = l2.mid()
        if mid is not None:
            self._price_history.append((time.time(), mid))
        _prev_mid = self._cache_mid.get(side, 0.0)
        _ticks = self._cache_ticks.get(side, 0) + 1
        self._cache_ticks[side] = _ticks
        _price_moved = abs(mid - _prev_mid) / max(_prev_mid, 1e-9) > 0.001 if \
            mid and _prev_mid else True
        if not _price_moved and _ticks < 5 and side in self._cached_signals:
            return self._cached_signals[side]
        self._cache_ticks[side] = 0
        if mid:
            self._cache_mid[side] = mid
        bid_vol = l2.bid_depth(5)
        ask_vol = l2.ask_depth(5)
        total = bid_vol + ask_vol
        depth_imbalance = bid_vol / total if total > 1e-9 else 0.5
        bid3 = l2.bid_depth(3)
        ask3 = l2.ask_depth(3)
        total3 = bid3 + ask3
        liq_signal = bid3 / total3 if total3 > 1e-9 else 0.5
        spread = l2.spread_cents() or 0.0
        prev_bid = self._prev_bid_depth.get(side, bid_vol)
        prev_ask = self._prev_ask_depth.get(side, ask_vol)
        bid_pressure = (bid_vol - prev_bid) - (prev_ask - ask_vol)
        self._prev_bid_depth[side] = bid_vol
        self._prev_ask_depth[side] = ask_vol
        now = time.time()
        pc_30s = pc_60s = 0.0
        for snap_ts, snap_px in reversed(self._price_history):
            age = now - snap_ts
            if age >= 60.0 and pc_60s == 0.0 and mid is not None and snap_px > 0:
                pc_60s = (mid - snap_px) / snap_px
            if age >= 30.0 and pc_30s == 0.0 and mid is not None and snap_px > 0:
                pc_30s = (mid - snap_px) / snap_px
        is_volatile = abs(pc_30s) > 0.02 or abs(pc_60s) > 0.035
        _result = MicrostructureSignals(
            bid_vol=bid_vol, ask_vol=ask_vol, spread_cents=spread,
            depth_imbalance=depth_imbalance, liquidity_signal=liq_signal,
            bid_pressure=bid_pressure, is_volatile=is_volatile,
            price_change_30s=pc_30s, price_change_60s=pc_60s,
        )
        self._cached_signals[side] = _result
        return _result

###############################################################################
# SECTION 14 -- BAYESIAN TRACKER
###############################################################################
class BayesianTracker:
    __slots__ = ("log_post_up", "log_post_down", "prev_kal_up",
                 "prev_kal_down", "tick_count", "_std", "_decay_rate",
                 "_last_update_ts")

    def __init__(
        self, prior: float = 0.50, std: float = 0.011,
        decay_rate: float = 0.022
    ) -> None:
        self.log_post_up = math.log(max(prior, 1e-15))
        self.log_post_down = math.log(max(1.0 - prior, 1e-15))
        self.prev_kal_up: Optional[float] = None
        self.prev_kal_down: Optional[float] = None
        self.tick_count: int = 0
        self._std = std
        self._decay_rate = decay_rate
        self._last_update_ts: float = time.time()

    def update(
        self,
        kal_up: float, kal_down: float,
        obi_up: Optional[float], obi_down: Optional[float],
        vpin_up: Optional[float], vpin_down: Optional[float],
        micro_up: Optional[MicrostructureSignals] = None,
        micro_down: Optional[MicrostructureSignals] = None,
    ) -> Tuple[float, float]:
        self.tick_count += 1
        now = time.time()
        delta_t = max(now - self._last_update_ts, 0.01)
        self._last_update_ts = now
        decay = math.exp(-self._decay_rate * delta_t)
        center = (self.log_post_up + self.log_post_down) / 2.0
        self.log_post_up = center + decay * (self.log_post_up - center)
        self.log_post_down = center + decay * (self.log_post_down - center)
        if self.prev_kal_up is not None:
            net = (kal_up - self.prev_kal_up) - (kal_down - self.prev_kal_down)
            self.log_post_up += net / self._std * 0.5
            self.log_post_down -= net / self._std * 0.5
        self.prev_kal_up, self.prev_kal_down = kal_up, kal_down
        if obi_up is not None and obi_down is not None:
            obi_net = ((obi_up - 0.5) * 2.0 - (obi_down - 0.5) * 2.0) * 0.3
            self.log_post_up += obi_net
            self.log_post_down -= obi_net
        if vpin_up is not None and vpin_down is not None:
            vpin_net = ((1.0 - vpin_up) - (1.0 - vpin_down)) * 0.2
            self.log_post_up += vpin_net
            self.log_post_down -= vpin_net
        if micro_up is not None:
            imb_bias = (micro_up.depth_imbalance - 0.5) * 0.25
            self.log_post_up += imb_bias
            self.log_post_down -= imb_bias
        if micro_down is not None:
            imb_bias = (micro_down.depth_imbalance - 0.5) * 0.25
            self.log_post_up -= imb_bias
            self.log_post_down += imb_bias
        log_z = self._lse(self.log_post_up, self.log_post_down)
        p_up = max(0.01, min(0.99, math.exp(self.log_post_up - log_z)))
        return p_up, 1.0 - p_up

    def get_posteriors(self) -> Tuple[float, float]:
        log_z = self._lse(self.log_post_up, self.log_post_down)
        p_up = max(0.01, min(0.99, math.exp(self.log_post_up - log_z)))
        return p_up, 1.0 - p_up

    @staticmethod
    def _lse(a: float, b: float) -> float:
        mx = max(a, b)
        return mx + math.log(math.exp(a - mx) + math.exp(b - mx))

    def reset(self, prior: float = 0.50) -> None:
        self.log_post_up = math.log(max(prior, 1e-15))
        self.log_post_down = math.log(max(1.0 - prior, 1e-15))
        self.prev_kal_up = self.prev_kal_down = None
        self.tick_count = 0
        self._last_update_ts = time.time()

###############################################################################
# SECTION 15 -- VOLATILITY EDGE TRACKER
###############################################################################
class VolatilityEdgeTracker:
    __slots__ = ("_probs", "_window", "_sigma_floor", "_es_threshold",
                 "_kelly_target", "tick_count", "_win_history", "_pnl_history")

    def __init__(self, cfg: BotConfig) -> None:
        self._probs: deque = deque(maxlen=cfg.vol_edge_window)
        self._window: int = cfg.vol_edge_window
        self._sigma_floor: float = cfg.vol_edge_sigma_floor
        self._es_threshold: float = cfg.es_min_threshold
        self._kelly_target: float = cfg.vol_kelly_target
        self.tick_count: int = 0
        self._win_history: deque = deque(maxlen=20)
        self._pnl_history: deque = deque(maxlen=20)

    def update(self, market_mid_prob: float) -> None:
        self._probs.append(market_mid_prob)
        self.tick_count += 1

    def record_outcome(self, win: bool, pnl: float) -> None:
        self._win_history.append(win)
        self._pnl_history.append(pnl)

    @property
    def sigma_mkt(self) -> float:
        n = len(self._probs)
        if n < 3:
            return self._sigma_floor
        if _HAS_NUMPY:
            sigma = float(_np.std(list(self._probs), ddof=1))
        else:
            vals = list(self._probs)
            mean = sum(vals) / n
            var = sum((x - mean) ** 2 for x in vals) / max(n - 1, 1)
            sigma = math.sqrt(max(var, 0.0))
        return max(sigma, self._sigma_floor)

    def adaptive_es_threshold(self, cfg: BotConfig) -> float:
        base = self._es_threshold
        if len(self._win_history) >= 10:
            wr = sum(self._win_history) / len(self._win_history)
            if wr > cfg.adaptive_edge_winrate_high:
                base *= cfg.adaptive_edge_scale_win
            elif wr < cfg.adaptive_edge_winrate_low:
                base *= cfg.adaptive_edge_scale_loss
        return max(cfg.adaptive_edge_min * 100,
                   min(cfg.adaptive_edge_max * 100, base))

    def edge_score(self, p_model: float, p_mkt: float) -> float:
        return (p_model - p_mkt) / self.sigma_mkt

    def should_trade(
        self, p_model: float, p_mkt: float, cfg: BotConfig
    ) -> Tuple[bool, float]:
        es = self.edge_score(p_model, p_mkt)
        thr = self.adaptive_es_threshold(cfg)
        return es >= thr, es

    def vol_factor(self, cfg: BotConfig) -> float:
        return min(1.0, cfg.vol_kelly_target / self.sigma_mkt)

    def adaptive_kelly(self, base_kelly: float, cfg: BotConfig) -> float:
        return min(base_kelly * self.vol_factor(cfg), cfg.kelly_max_risk_pct)

    def status_str(self) -> str:
        return f"σ_mkt={self.sigma_mkt:.4f} ticks={self.tick_count}"

###############################################################################
# SECTION 15b -- VOLATILITY HEDGE ENGINE 1SD-3SD (v9.5.0)
###############################################################################
class VolHedgeState(Enum):
    """State machine for a single vol-hedge position."""
    IDLE = "IDLE"                       # waiting for 1SD cross
    YES_OPEN = "YES_OPEN"               # SIM bought, NO limit pending
    HEDGE_FILLED = "HEDGE_FILLED"       # NO limit filled → profit locked
    ABANDONED = "ABANDONED"             # < 60s to close, let resolve

@dataclass
class VolHedgePosition:
    """Tracks a single volatility hedge position (1SD entry + 3SD hedge)."""
    direction: str                     # "UP" or "DOWN"
    entry_price_bnc: float             # Binance price at 1SD trigger
    k_reference: float                 # cycle open price (k)
    sd_at_entry: float                 # SD at time of entry
    yes_trade: Optional[Trade] = None  # the YES trade (SIM side)
    no_limit_price: float = 0.0        # limit price for NO hedge (0.10-0.15)
    no_limit_placed: bool = False       # whether limit order was placed
    no_limit_filled: bool = False       # whether limit was filled (3SD reached)
    no_trade: Optional[Trade] = None   # the NO trade if filled
    state: VolHedgeState = VolHedgeState.IDLE
    created_ts: float = field(default_factory=time.time)
    abandoned: bool = False


class VolatilityHedgeEngine:
    """Implements the 1SD-3SD Volatility Hedge strategy for XRP 5-min candles.

    Strategy:
        k = cycle_open_price (baseline / moving average)
        SD = real-time standard deviation from Binance tick returns

        UP Rules:
          1. Price crosses k + 1*SD  → BUY YES(UP) at current ask
          2. Immediately place limit order to BUY NO(DOWN) at 0.10-0.15c
          3. Price reaches k + 3*SD  → NO limit fills → profit locked (YES+NO < $1)

        DOWN Rules (symmetric):
          1. Price crosses k - 1*SD  → BUY YES(DOWN)
          2. Place limit BUY NO(UP) at 0.10-0.15c
          3. Price reaches k - 3*SD  → NO fills → locked

        Risk Management:
          - If < 60s to candle close and 3SD not hit → abandon hedge,
            let YES position resolve at close ($1 or $0).
          - Verify liquidity on NO side before placing limit.
    """

    def __init__(self, cfg: BotConfig) -> None:
        self._cfg = cfg
        self._positions: List[VolHedgePosition] = []
        self._last_trigger_ts: float = 0.0
        self._price_buffer: deque = deque(maxlen=cfg.vol_hedge_sd_window)
        self._1sd_crossed_up: bool = False
        self._1sd_crossed_down: bool = False
        self._stats_entries: int = 0
        self._stats_hedged: int = 0
        self._stats_abandoned: int = 0
        self._stats_resolved: int = 0

    def feed_price(self, price: float) -> None:
        """Feed a new Binance tick price for SD calculation."""
        self._price_buffer.append(price)

    @property
    def current_sd(self) -> float:
        """Compute real-time standard deviation from buffered prices."""
        n = len(self._price_buffer)
        if n < 5:
            return 0.0
        prices = list(self._price_buffer)
        # Compute log returns for SD calculation
        returns = []
        for i in range(1, n):
            if prices[i - 1] > 1e-9:
                returns.append(math.log(prices[i] / prices[i - 1]))
        if len(returns) < 3:
            return 0.0
        mean_r = sum(returns) / len(returns)
        var = sum((r - mean_r) ** 2 for r in returns) / max(len(returns) - 1, 1)
        sd = math.sqrt(max(var, 0.0))
        return sd

    @property
    def current_sd_price(self) -> float:
        """SD expressed in price terms (SD * current_mean_price)."""
        n = len(self._price_buffer)
        if n < 5:
            return 0.0
        prices = list(self._price_buffer)
        mean_price = sum(prices) / n
        sd = self.current_sd
        return sd * mean_price

    def has_active_position(self, direction: str) -> bool:
        """Check if there's already an active vol-hedge position for this direction."""
        return any(
            p.direction == direction and p.state in (
                VolHedgeState.YES_OPEN, VolHedgeState.HEDGE_FILLED
            )
            for p in self._positions
        )

    def check_1sd_trigger(
        self, binance: "BinanceState", cfg: BotConfig,
    ) -> Optional[str]:
        """Check if Binance price has crossed k ± 1*SD.

        Returns:
            "UP" if price crossed k + 1*SD (bullish breakout)
            "DOWN" if price crossed k - 1*SD (bearish breakout)
            None if no trigger
        """
        if not cfg.vol_hedge_active:
            return None
        if binance.current_price is None or binance.cycle_open_price is None:
            return None

        k = binance.cycle_open_price
        price = binance.current_price
        sd = self.current_sd_price

        if sd < cfg.vol_hedge_min_sd:
            return None

        threshold_up = k + cfg.vol_hedge_1sd_trigger * sd
        threshold_down = k - cfg.vol_hedge_1sd_trigger * sd

        now = time.time()
        if now - self._last_trigger_ts < cfg.vol_hedge_cooldown_s:
            return None

        if price >= threshold_up and not self.has_active_position("UP"):
            self._1sd_crossed_up = True
            return "UP"
        elif price <= threshold_down and not self.has_active_position("DOWN"):
            self._1sd_crossed_down = True
            return "DOWN"

        return None

    def check_3sd_reached(
        self, binance: "BinanceState", cfg: BotConfig,
    ) -> List[VolHedgePosition]:
        """Check if any active position has reached the 3SD target.

        Returns list of positions where 3SD was reached (NO hedge should fill).
        """
        if binance.current_price is None or binance.cycle_open_price is None:
            return []

        k = binance.cycle_open_price
        price = binance.current_price
        sd = self.current_sd_price
        reached = []

        for pos in self._positions:
            if pos.state != VolHedgeState.YES_OPEN:
                continue
            # Use SD at entry time for consistency
            sd_entry = pos.sd_at_entry if pos.sd_at_entry > 1e-9 else sd
            target_3sd = cfg.vol_hedge_3sd_target * sd_entry

            if pos.direction == "UP":
                target_price = pos.k_reference + target_3sd
                if price >= target_price:
                    reached.append(pos)
            else:  # DOWN
                target_price = pos.k_reference - target_3sd
                if price <= target_price:
                    reached.append(pos)

        return reached

    def check_abandon(
        self, timer: "MarketTimer", cfg: BotConfig,
    ) -> List[VolHedgePosition]:
        """Check positions that should be abandoned (< abandon_s to close, 3SD not hit).

        Returns list of positions to abandon.
        """
        to_abandon = []
        rem = timer.remaining

        for pos in self._positions:
            if pos.state != VolHedgeState.YES_OPEN:
                continue
            if rem <= cfg.vol_hedge_abandon_s:
                to_abandon.append(pos)

        return to_abandon

    def compute_no_limit_price(
        self, cfg: BotConfig, yes_trade: Optional[Trade] = None,
    ) -> float:
        """v9.5.0: Dynamic max_hedge_price = 0.90 - (custo_total_YES / N_shares).

        Never uses fixed 0.12. Price rounded to 4 decimal places.
        Falls back to midpoint of [no_limit_low, no_limit_high] only if
        yes_trade is None (pre-entry estimate).
        """
        if yes_trade is not None and yes_trade.shares > _d("1e-9"):
            n_shares = float(yes_trade.shares)
            cost_per_share = float(yes_trade.total_out) / n_shares
            dynamic_price = round(0.90 - cost_per_share, 4)
            # Clamp to sane range: never below 0.01 or above 0.20
            dynamic_price = max(0.01, min(0.20, dynamic_price))
            return dynamic_price
        # Fallback for pre-entry estimate
        return round(
            (cfg.vol_hedge_no_limit_low + cfg.vol_hedge_no_limit_high) / 2.0,
            4,
        )

    def check_no_side_liquidity(
        self, direction: str, ctx: "BotContext", cfg: BotConfig,
        limit_price: Optional[float] = None,
    ) -> Tuple[bool, float]:
        """v9.5.0: Verify effective liquidity on NO side within ±2% slippage.

        Walks up to 15+ levels of the ask book on the opposite side.
        Only counts volume at prices within limit_price * (1 + 0.02).

        Returns (has_liquidity, available_volume).
        """
        # NO side is the OPPOSITE direction
        if direction == "UP":
            l2 = ctx.l2_down
        else:
            l2 = ctx.l2_up

        if l2.is_stale(3.0) or not l2.asks:
            return False, 0.0

        # Use provided limit_price or fallback estimate
        lp = limit_price if limit_price is not None else self.compute_no_limit_price(cfg)
        max_price = lp * 1.02  # +2% slippage tolerance

        available = 0.0
        asks_sorted = sorted(l2.asks, key=lambda x: x[0])
        for price, size in asks_sorted[:max(15, len(asks_sorted))]:
            if price <= max_price:
                available += size

        return available >= cfg.vol_hedge_liquidity_min, available

    def register_entry(
        self, direction: str, trade: Trade,
        k: float, sd: float, no_limit_price: float,
    ) -> VolHedgePosition:
        """v9.5.0: Register a new 1SD entry with dynamic NO limit price.

        The no_limit_price is computed dynamically as:
            max_hedge_price = 0.90 - (trade.total_out / trade.shares)
        This is computed by the caller via compute_no_limit_price(cfg, trade).
        """
        pos = VolHedgePosition(
            direction=direction,
            entry_price_bnc=trade.ask,
            k_reference=k,
            sd_at_entry=sd,
            yes_trade=trade,
            no_limit_price=no_limit_price,
            state=VolHedgeState.YES_OPEN,
        )
        self._positions.append(pos)
        self._last_trigger_ts = time.time()
        self._stats_entries += 1
        return pos

    def mark_hedge_filled(self, pos: VolHedgePosition, no_trade: Trade) -> None:
        """Mark a position as fully hedged (NO limit filled at 3SD)."""
        pos.no_trade = no_trade
        pos.no_limit_filled = True
        pos.state = VolHedgeState.HEDGE_FILLED
        self._stats_hedged += 1

    def mark_abandoned(self, pos: VolHedgePosition) -> None:
        """Mark a position as abandoned (time running out, no 3SD)."""
        pos.state = VolHedgeState.ABANDONED
        pos.abandoned = True
        self._stats_abandoned += 1

    def mark_resolved(self, pos: VolHedgePosition) -> None:
        """Mark position as resolved at candle close."""
        self._stats_resolved += 1

    def cleanup_cycle(self) -> None:
        """Clear all positions and reset for new cycle."""
        self._positions.clear()
        self._1sd_crossed_up = False
        self._1sd_crossed_down = False

    @property
    def active_positions(self) -> List[VolHedgePosition]:
        return [p for p in self._positions if p.state in (
            VolHedgeState.YES_OPEN, VolHedgeState.HEDGE_FILLED,
        )]

    @property
    def stats(self) -> str:
        return (
            f"entries={self._stats_entries} hedged={self._stats_hedged} "
            f"abandoned={self._stats_abandoned} resolved={self._stats_resolved} "
            f"active={len(self.active_positions)}"
        )

###############################################################################
# SECTION 16 -- RATE LIMITER + CIRCUIT BREAKER + RETRY
###############################################################################
class RateLimiter:
    __slots__ = ("cps", "burst", "tokens", "last_check", "_lock")

    def __init__(self, cps: float, burst: float) -> None:
        self.cps = cps
        self.burst = burst
        self.tokens = burst
        self.last_check = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self.tokens = min(
                self.burst, self.tokens + (now - self.last_check) * self.cps
            )
            self.last_check = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
            else:
                wait = (1.0 - self.tokens) / self.cps
                await asyncio.sleep(wait)
                self.tokens = 0.0

class CircuitBreaker:
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF = "HALF-OPEN"

    def __init__(self, ft: int, rs: float, label: str = "CB") -> None:
        self.ft = ft
        self.rs = rs
        self.label = label
        self._f = 0
        self._st = self.CLOSED
        self._at = 0.0

    @property
    def state(self) -> str:
        return self._st

    @property
    def failure_count(self) -> int:
        return self._f

    def is_open(self) -> bool:
        if self._st == self.CLOSED:
            return False
        if self._st == self.OPEN:
            if time.monotonic() - self._at >= self.rs:
                self._st = self.HALF
                log_debug(f"[{self.label}] OPEN->HALF-OPEN (recovery elapsed)")
                return False
            return True
        return False

    def record_success(self) -> None:
        if self._st == self.HALF:
            log_info(f"[{self.label}] HALF-OPEN->CLOSED (success)")
            self._st = self.CLOSED
            self._f = 0

    def record_failure(self) -> None:
        self._f += 1
        if self._st == self.HALF:
            self._st = self.OPEN
            self._at = time.monotonic()
            log_warn(f"[{self.label}] HALF-OPEN->OPEN (failure in probe)")
        elif self._f >= self.ft and self._st == self.CLOSED:
            self._st = self.OPEN
            self._at = time.monotonic()
            log_warn(f"[{self.label}] CLOSED->OPEN ({self._f} failures)")

async def retry_with_backoff(
    fn: Callable[..., Any],
    *args: Any,
    label: str = "call",
    cfg: BotConfig,
    **kwargs: Any,
) -> Any:
    last_exc: Optional[Exception] = None
    for attempt in range(1, cfg.max_api_retries + 1):
        try:
            if asyncio.iscoroutinefunction(fn):
                return await fn(*args, **kwargs)
            return await asyncio.get_running_loop().run_in_executor(
                None, lambda: fn(*args, **kwargs)
            )
        except Exception as exc:
            last_exc = exc
            _exc_str = str(exc).lower()
            _is_403 = (
                "403" in _exc_str
                or "forbidden" in _exc_str
                or (hasattr(exc, "code") and getattr(exc, "code", 0) == 403)
                or (hasattr(exc, "status_code") and
                    getattr(exc, "status_code", 0) == 403)
            )
            if _is_403:
                log_warn(f"[retry] [{label}] PERMANENT 403 -- aborting "
                         f"(no retry): {exc}")
                return None
            if attempt < cfg.max_api_retries:
                bk = min(cfg.base_backoff_s * (2 ** (attempt - 1)),
                         cfg.max_backoff_s)
                if cfg.backoff_jitter:
                    bk *= 0.7 + random.random() * 0.6
                await asyncio.sleep(bk)
    log_warn(f"[retry] [{label}] GAVE UP after {cfg.max_api_retries} attempts: "
             f"{last_exc}")
    return None

###############################################################################
# SECTION 17 -- MARKET TIMER
###############################################################################
class MarketTimer:
    def __init__(self, market_end_ts: float, cfg: BotConfig) -> None:
        self.market_end_ts = market_end_ts
        self._cfg = cfg

    @property
    def remaining(self) -> float:
        return max(0.0, self.market_end_ts - time.time())

    @property
    def is_expired(self) -> bool:
        return self.remaining <= 0.0

    def can_gambling_enter(self) -> bool:
        rem = self.remaining
        return rem > self._cfg.gamb_cutoff_s

    def is_endgame(self) -> bool:
        rem = self.remaining
        return 0.0 < rem <= self._cfg.aggressive_endgame_s

    def remaining_str(self) -> str:
        rem = max(0.0, self.remaining)
        return (
            f"{int(rem // 60):02d}:{int(rem % 60):02d}:"
            f"{int((rem * 1000) % 1000):03d}"
        )

###############################################################################
# SECTION 18 -- KALMAN FILTER + HFT WINDOW
###############################################################################
class KalmanFilter1D:
    __slots__ = ("q", "r", "x", "p")

    def __init__(self, q: float, r: float) -> None:
        self.q = q
        self.r = r
        self.x: Optional[float] = None
        self.p: float = 1.0

    def update(self, z: float) -> float:
        if self.x is None:
            self.x = z
            return z
        p_pred = self.p + self.q
        k = p_pred / (p_pred + self.r)
        self.x = self.x + k * (z - self.x)
        self.p = (1.0 - k) * p_pred
        return self.x

    def reset(self) -> None:
        self.x = None
        self.p = 1.0

class HFTWindow:
    __slots__ = ("window_s", "data", "_cached_mean", "_cached_std")

    def __init__(self, ws: float = 5.5) -> None:
        self.window_s = ws
        self.data = deque()
        self._cached_mean: Optional[float] = None
        self._cached_std: Optional[float] = None

    def add(self, price: float, ts: float) -> None:
        self.data.append((ts, price))
        cutoff = ts - self.window_s
        while self.data and self.data[0][0] < cutoff:
            self.data.popleft()
        self._cached_mean = self._cached_std = None

    def _compute_stats(self) -> Tuple[Optional[float], Optional[float]]:
        if self._cached_mean is not None:
            return self._cached_mean, self._cached_std
        n = len(self.data)
        if n < 3:
            return None, None
        prices = [p for _, p in self.data]
        mean = sum(prices) / n
        var = sum((p - mean) ** 2 for p in prices) / max(n - 1, 1)
        self._cached_mean = mean
        self._cached_std = math.sqrt(max(var, 0.0))
        return self._cached_mean, self._cached_std

    def zscore(self, price: float) -> Optional[float]:
        mean, std = self._compute_stats()
        if mean is None or std is None:
            return None
        return 0.0 if std < 1e-9 else (price - mean) / std

    def std(self) -> Optional[float]:
        _, s = self._compute_stats()
        return s

    def clear(self) -> None:
        self.data.clear()
        self._cached_mean = self._cached_std = None

###############################################################################
# SECTION 19 -- FEE SYSTEM
###############################################################################
def polymarket_fee(
    shares: float, price: float,
    fee_rate: float = 0.25, exponent: int = 2
) -> float:
    """Compute Polymarket taker fee using official formula.

    Formula (from docs.polymarket.com/trading/fees):
        fee = C x p x feeRate x (p x (1 - p))^exponent

    Where:
        C        = number of shares traded
        p        = share price (0.0 to 1.0)
        feeRate  = 0.25 for Crypto markets
        exponent = 2 for Crypto markets

    The effective rate peaks at 1.5625% at p=0.50 and decreases
    symmetrically toward both extremes (0% and 100%).

    Fee is paid in USDC on buys (deducted from USDC spent)
    and in USDC on sells (deducted from USDC received).

    Args:
        shares: Number of shares (C).
        price:  Share price in [0.001, 0.999].
        fee_rate: Market fee rate parameter (default 0.25 for Crypto).
        exponent: Market exponent parameter (default 2 for Crypto).

    Returns:
        Fee amount in USDC.
    """
    p = max(0.001, min(0.999, price))
    return shares * p * fee_rate * (p * (1.0 - p)) ** exponent


def polymarket_fee_decimal(
    shares: Decimal, price: float,
    fee_rate: float = 0.25, exponent: int = 2
) -> Decimal:
    """Decimal version of polymarket_fee for trade accounting."""
    return _dq(_d(polymarket_fee(float(shares), price, fee_rate, exponent)))


def fee_rate_lut(
    token_id: str, fee_cache: Dict[str, int], default_bps: int = 50
) -> float:
    """Legacy flat-rate helper used by arb/peg calculations (not for BUY/SELL).

    NOTE: For actual order fee computation use polymarket_fee() which
    implements the correct Polymarket non-linear formula. This function
    is retained for backwards-compatible arb edge calculations that
    need a simple approximation.
    """
    bps = fee_cache.get(token_id, default_bps)
    return bps / 10_000.0

def _cost_with_fee(shares: float, ask: float, fee_rate: float) -> float:
    return shares * ask * (1.0 + fee_rate)

def eff_price_c_f(ask: float, fee_rate: float) -> float:
    """Effective cost per share in cents, including Polymarket non-linear fee.

    fee_rate arg is kept for API compatibility but the actual fee is
    computed with the Polymarket formula (not flat bps).
    """
    # For 1 share at price ask: effective_price = ask + fee_per_share
    fee_per_share = polymarket_fee(1.0, ask)
    return (ask + fee_per_share) * 100.0

def sell_payout_net(
    shares: Decimal, bid: float, fee_rate: float
) -> Decimal:
    """Net payout on sell after Polymarket non-linear fee."""
    gross = _dq(shares * _d(bid))
    fee   = polymarket_fee_decimal(shares, bid)
    return _dq(gross - fee)

def resolution_payout(shares: Decimal, winner: bool) -> Decimal:
    return shares if winner else _ZERO

def calc_imbalance(
    bid_size: Optional[float], ask_size: Optional[float]
) -> Optional[float]:
    if bid_size is None or ask_size is None:
        return None
    total = bid_size + ask_size
    return bid_size / total if total > 1e-9 else None

def calc_kelly_bayesian(
    p_hat: float, ask: float, mart_level: int, cfg: BotConfig
) -> float:
    if ask <= 0.0 or ask >= 1.0:
        return 0.0
    kelly = p_hat - (1.0 - p_hat) / ((1.0 - ask) / ask)
    if kelly <= 0.0:
        return 0.0
    return min(kelly * cfg.kelly_fraction * mart_level,
               cfg.kelly_max_risk_pct * cfg.mart_max_mult)

def calculate_dynamic_tp(
    trade: Trade, fee_cache: Dict[str, int], target_net_roi: float = 0.02,
    z_score: float = 0.0,
) -> float:
    """v9.5.3: Returns the MINIMUM bid price at which a moonbag TP is possible.

    The moonbag TP fires when we can recover 100% of invested capital by
    selling at most 80% of shares. This function returns the threshold bid
    price below which TP cannot fire.

    Threshold = total_out / (shares * max_fraction)
    i.e. the bid at which selling 80% of shares exactly recovers total_out.

    Below this bid, it's impossible to recover 100% selling ≤ 80%.
    Above this bid, we sell fewer shares (the "moonbag" is bigger).
    """
    if trade.ask <= 0.0 or trade.ask >= 0.995:
        return 1.0
    shares_f = float(trade.shares)
    total_f = float(trade.total_out)
    if shares_f < 1e-9 or total_f < 1e-9:
        return 1.0
    # Minimum bid to make moonbag possible (sell 80% to break even)
    # Also account for sell fee at that bid level
    max_fraction = 0.80
    sellable_shares = shares_f * max_fraction
    # fee estimation at the break-even bid
    est_bid = total_f / sellable_shares if sellable_shares > 1e-9 else 1.0
    fee_at_bid = polymarket_fee(sellable_shares, est_bid)
    # Need: sellable_shares * bid - fee >= total_out
    # => bid >= (total_out + fee) / sellable_shares
    threshold = (total_f + fee_at_bid) / sellable_shares if sellable_shares > 1e-9 else 1.0
    if threshold >= 0.995:
        return 1.0
    return round(threshold, 6)


def calculate_moonbag_shares(
    trade: Trade, bid_now: float, max_fraction: float = 0.80,
) -> Tuple[Optional[float], float, float]:
    """v9.5.3: Calculate exact shares to sell for 100% capital recovery.

    Returns:
        (shares_to_sell, fraction_of_position, moonbag_shares)
        or (None, 0, 0) if moonbag TP is not possible at this bid.

    Logic:
        shares_to_sell = (total_out + sell_fee_estimate) / bid_now
        Only valid if shares_to_sell <= total_shares * max_fraction

    At high bids (0.90-0.98), fewer shares are needed → bigger moonbag.
    At lower bids, more shares needed → smaller moonbag (or None if > 80%).
    """
    total_out_f = float(trade.total_out)
    total_shares_f = float(trade.shares)

    if bid_now <= 0.0 or total_shares_f < 1e-9 or total_out_f < 1e-9:
        return None, 0.0, 0.0

    # Estimate sell fee: fee = C * p * 0.25 * (p*(1-p))^2
    # We need to solve: shares_to_sell * bid - fee(shares_to_sell, bid) >= total_out
    # Iterative approach (one Newton step is sufficient for convergence):
    shares_est = total_out_f / bid_now
    fee_est = polymarket_fee(shares_est, bid_now)
    shares_to_sell = (total_out_f + fee_est) / bid_now

    # Refine once more with updated fee
    fee_refined = polymarket_fee(shares_to_sell, bid_now)
    shares_to_sell = (total_out_f + fee_refined) / bid_now

    max_sellable = total_shares_f * max_fraction

    if shares_to_sell > max_sellable:
        # Cannot recover 100% within the 80% ceiling
        return None, 0.0, 0.0

    if shares_to_sell < 1e-6:
        return None, 0.0, 0.0

    fraction = shares_to_sell / total_shares_f
    moonbag = total_shares_f - shares_to_sell

    return shares_to_sell, fraction, moonbag

###############################################################################
# SECTION 20 -- ARB ENGINE
###############################################################################
def _calc_vwap(
    book_side: OrderBookSide, target_size: float
) -> Tuple[Optional[float], float]:
    if book_side.is_empty:
        return None, 0.0
    total_cost = 0.0
    filled = 0.0
    for level in book_side.levels:
        remaining = target_size - filled
        if remaining <= 0:
            break
        fill_at = min(level.size, remaining)
        total_cost += fill_at * level.price
        filled += fill_at
        if filled < 1.0:
            return None, 0.0
    return total_cost / filled if filled > 1e-9 else None, filled

def simulate_market_buy_l2(
    book_asks: List[Tuple[float, float]], target_shares: float
) -> Optional[float]:
    if not book_asks or target_shares <= 0.0:
        return None
    total_cost = 0.0
    filled = 0.0
    for price, size in sorted(book_asks, key=lambda x: x[0]):
        if filled >= target_shares:
            break
        take = min(size, target_shares - filled)
        total_cost += take * price
        filled += take
        if filled < target_shares * 0.95:
            return None
    return total_cost / filled if filled > 1e-9 else None

def check_liquidity(
    book_side: OrderBookSide, target_size: float
) -> Tuple[bool, float, float]:
    if book_side.is_empty:
        return False, 0.0, 0.0
    avail_best = book_side.best_size or 0.0
    avail_total = book_side.total_volume()
    return avail_best >= target_size, avail_best, avail_total


def evaluate_arb(
    asks_up: OrderBookSide,
    asks_down: OrderBookSide,
    budget: float,
    peg_trigger: float,
    token_id_up: str,
    token_id_dn: str,
    fee_cache: Dict[str, int],
    cfg: BotConfig,
) -> ArbResult:
    if asks_up.is_empty:
        return ArbResult(status=ArbStatus.REJECT_EMPTY_BOOK,
                         reason="UP book empty")
    if asks_down.is_empty:
        return ArbResult(status=ArbStatus.REJECT_EMPTY_BOOK,
                         reason="DOWN book empty")
    la_up = asks_up.best_price
    la_dn = asks_down.best_price
    if la_up is None:
        return ArbResult(status=ArbStatus.REJECT_EMPTY_BOOK,
                         reason="UP best_price is None")
    if la_dn is None:
        return ArbResult(status=ArbStatus.REJECT_EMPTY_BOOK,
                         reason="DOWN best_price is None")
    vol_up = asks_up.best_size or 0.0
    vol_dn = asks_down.best_size or 0.0
    peg = la_up + la_dn
    gross_margin = cfg.arb_resolution - peg
    if peg > peg_trigger + 1e-6:
        return ArbResult(
            status=ArbStatus.REJECT_PEG_TOO_HIGH,
            lowest_ask_up=la_up, lowest_ask_down=la_dn, peg=peg,
            gross_margin=gross_margin,
            volume_at_ask_up=vol_up, volume_at_ask_down=vol_dn,
            reason=f"Peg={peg:.4f} > trigger={peg_trigger}",
        )
    # v9.4.0: use real Polymarket non-linear fee instead of linear BPS approximation
    fee_u = polymarket_fee(1.0, la_up)
    fee_d = polymarket_fee(1.0, la_dn)
    cost_per_share = la_up + la_dn + fee_u + fee_d
    if cost_per_share <= 0.0 or budget <= 0.0:
        return ArbResult(
            status=ArbStatus.REJECT_BUDGET_TOO_LOW,
            lowest_ask_up=la_up, lowest_ask_down=la_dn, peg=peg,
            reason="Budget or cost_per_share = zero",
        )
    shares = budget / cost_per_share
    if shares < cfg.arb_min_shares:
        return ArbResult(
            status=ArbStatus.REJECT_BUDGET_TOO_LOW,
            lowest_ask_up=la_up, lowest_ask_down=la_dn, peg=peg, shares=shares,
            reason=f"Shares={shares:.6f} < min={cfg.arb_min_shares}",
        )
    liq_ok_up, avail_up, _ = check_liquidity(asks_up, shares)
    liq_ok_dn, avail_dn, _ = check_liquidity(asks_down, shares)
    used_vwap = False
    vwap_up: Optional[float] = None
    vwap_dn: Optional[float] = None
    eff_ask_up = la_up
    eff_ask_dn = la_dn
    if not liq_ok_up or not liq_ok_dn:
        vwap_up, filled_up = _calc_vwap(asks_up, shares)
        vwap_dn, filled_dn = _calc_vwap(asks_down, shares)
        if vwap_up is None or filled_up < shares * 0.95:
            return ArbResult(
                status=ArbStatus.REJECT_NO_LIQUIDITY_UP,
                lowest_ask_up=la_up, lowest_ask_down=la_dn, peg=peg,
                gross_margin=gross_margin,
                shares=shares, volume_at_ask_up=avail_up,
                volume_at_ask_down=avail_dn,
                reason=f"UP insufficient: need={shares:.2f} avail={avail_up:.2f}",
            )
        if vwap_dn is None or filled_dn < shares * 0.95:
            return ArbResult(
                status=ArbStatus.REJECT_NO_LIQUIDITY_DOWN,
                lowest_ask_up=la_up, lowest_ask_down=la_dn, peg=peg,
                gross_margin=gross_margin,
                shares=shares, volume_at_ask_up=avail_up,
                volume_at_ask_down=avail_dn,
                reason=f"DOWN insufficient: need={shares:.2f} avail={avail_dn:.2f}",
            )
        vwap_peg = vwap_up + vwap_dn
        if vwap_peg > peg_trigger + 1e-6:
            return ArbResult(
                status=ArbStatus.REJECT_VWAP_BREAKS_PEG,
                lowest_ask_up=la_up, lowest_ask_down=la_dn, peg=peg,
                gross_margin=cfg.arb_resolution - vwap_peg,
                shares=shares, used_vwap=True, vwap_up=vwap_up,
                vwap_down=vwap_dn,
                volume_at_ask_up=avail_up, volume_at_ask_down=avail_dn,
                reason=f"VWAP Peg={vwap_peg:.4f} > trigger={peg_trigger}",
            )
        used_vwap = True
        eff_ask_up = vwap_up
        eff_ask_dn = vwap_dn
    fee_u = polymarket_fee(1.0, eff_ask_up)
    fee_d = polymarket_fee(1.0, eff_ask_dn)
    cost_per_share = eff_ask_up + eff_ask_dn + fee_u + fee_d
    shares = budget / cost_per_share
    cost_up = shares * (eff_ask_up + polymarket_fee(1.0, eff_ask_up))
    cost_down = shares * (eff_ask_dn + polymarket_fee(1.0, eff_ask_dn))
    total_cost = cost_up + cost_down
    payout = shares * cfg.arb_resolution
    net_profit = payout - total_cost
    profit_pct = (net_profit / total_cost * 100.0) if total_cost > 0 else 0.0
    if net_profit <= 0.0:
        return ArbResult(
            status=ArbStatus.REJECT_NEGATIVE_PROFIT,
            lowest_ask_up=la_up, lowest_ask_down=la_dn, peg=peg,
            gross_margin=gross_margin, shares=shares,
            cost_up=cost_up, cost_down=cost_down, total_cost=total_cost,
            payout=payout, net_profit=net_profit, profit_pct=profit_pct,
            used_vwap=used_vwap, vwap_up=vwap_up, vwap_down=vwap_dn,
            volume_at_ask_up=vol_up, volume_at_ask_down=vol_dn,
            reason=f"Net profit=${net_profit:.6f} <= 0 after fees",
        )
    return ArbResult(
        status=ArbStatus.OPPORTUNITY,
        lowest_ask_up=la_up, lowest_ask_down=la_dn, peg=peg,
        gross_margin=gross_margin, shares=shares,
        cost_up=cost_up, cost_down=cost_down, total_cost=total_cost,
        payout=payout, net_profit=net_profit, profit_pct=profit_pct,
        used_vwap=used_vwap, vwap_up=vwap_up, vwap_down=vwap_dn,
        volume_at_ask_up=vol_up, volume_at_ask_down=vol_dn,
        reason="ARB OPPORTUNITY",
    )

###############################################################################
# SECTION 21 -- EXECUTION ENGINE
###############################################################################
def _compute_worst_price(side: str, price: float, slippage: float) -> float:
    worst = price + slippage if side == "BUY" else price - slippage
    return round(max(0.01, min(0.99, worst)), 2)

async def execute_trade(
    ctx: BotContext,
    token_id: str,
    side: str,
    amount: float,
    price: float,
    order_uuid: str,
    use_limit: bool = False,
) -> bool:
    if order_uuid in ctx.pending_orders:
        existing = ctx.pending_orders[order_uuid]
        if existing.get("status") in ("sent", "filled"):
            log_info(f"[EXEC] Duplicate order suppressed | "
                     f"uuid={order_uuid[:12]}...")
            return existing.get("ok", False)
    ctx.pending_orders[order_uuid] = {
        "status": "pending", "token": token_id, "ts": time.time()
    }
    for side_key, l2 in (("up", ctx.l2_up), ("down", ctx.l2_down)):
        if l2.is_stale(ctx.cfg.stale_data_threshold_s):
            log_warn(f"[EXEC] STALE L2 data ({side_key}) -- aborting order")
            ctx.pending_orders[order_uuid]["status"] = "aborted_stale"
            return False
    shares = round(amount / price, 6) if price > 1e-9 else 0.0
    _ot_label = "LIMIT" if use_limit else "FOK"
    if ctx.cfg.dry_run:
        log_info(
            f"[DRY_RUN] {_ot_label} | side={side:<4} | price={price:.4f} | "
            f"amount=${amount:.4f} | shares={shares:.4f} | "
            f"uuid={order_uuid[:12]}..."
        )
        ctx.pending_orders[order_uuid]["status"] = "filled"
        ctx.pending_orders[order_uuid]["ok"] = True
        return True
    if ctx.clob_client is None:
        log_warn("[EXEC] clob_client is None -- cannot execute live order")
        ctx.pending_orders[order_uuid]["status"] = "aborted_no_client"
        return False
    try:
        from py_clob_client.clob_types import OrderType
        fee_bps = ctx.fee_cache.get(token_id, ctx.cfg.default_taker_fee_bps)
        if use_limit:
            order = ctx.clob_client.create_limit_order(
                token_id=token_id, side=side, price=price,
                size=shares, fee_rate_bps=fee_bps,
                options={"tick_size": "0.01", "neg_risk": False},
            )
            ctx.clob_client.post_order(order, OrderType.GTC)
        else:
            order = ctx.clob_client.create_market_order(
                token_id=token_id, side=side, amount=amount, price=price,
                fee_rate_bps=fee_bps,
                options={"tick_size": "0.01", "neg_risk": False},
            )
            ctx.clob_client.post_order(order, OrderType.FOK)
        log_info(
            f"[EXEC] {_ot_label} SENT | fee_bps={fee_bps} | "
            f"shares={shares:.4f} | uuid={order_uuid[:12]}..."
        )
        ctx.pending_orders[order_uuid]["status"] = "filled"
        ctx.pending_orders[order_uuid]["ok"] = True
        return True
    except Exception as exc:
        log_warn(f"[EXEC] FAILED | {type(exc).__name__}: {exc} | "
                 f"uuid={order_uuid[:12]}...")
        ctx.pending_orders[order_uuid]["status"] = "failed"
        ctx.pending_orders[order_uuid]["ok"] = False
        return False


###############################################################################
# SECTION 22 -- API HELPERS
###############################################################################
def _fetch_metadata_sync(slug: str, cfg: BotConfig) -> Optional[Dict]:
    url = f"{cfg.gamma_api_url}/events?slug={slug}"
    try:
        import requests as _req
        sess = _req.Session()
        sess.headers.update(_META_HEADERS)
        r = sess.get(url, timeout=8)
        if r.status_code != 200:
            _preview = (r.text or " ")[:200].replace("\n", "  ").strip()
            log_warn(
                f"[META] Gamma API HTTP {r.status_code} (requests) | "
                f"url={url} | body_preview={_preview!r}"
            )
            return None
        try:
            data = r.json()
        except (ValueError, json.JSONDecodeError):
            _preview = (r.text or " ")[:200].replace("\n", "  ").strip()
            log_warn(
                f"[META] Gamma API non-JSON body (requests) | status=200 | "
                f"content_type={r.headers.get('content-type', 'n/a')} | "
                f"body_preview={_preview!r}"
            )
            return None
        if not isinstance(data, list) or not data:
            log_warn(
                f"[META] Gamma API empty or non-list response | "
                f"type={type(data).__name__}"
            )
            return None
        markets = data[0].get("markets")
        if not markets:
            log_warn(f"[META] Gamma API event has no markets | slug={slug}")
            return None
        m = markets[0]
        raw_ids = m.get("clobTokenIds", "[]")
        ids = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
        if not isinstance(ids, list) or len(ids) < 2:
            log_warn(f"[META] clobTokenIds malformed | raw={raw_ids!r}")
            return None
        return {
            "id": m["conditionId"], "up": ids[0], "down": ids[1], "slug": slug
        }
    except Exception as exc:
        log_warn(f"[META] requests path exception: {type(exc).__name__}: {exc}")
        return None

async def fetch_metadata(slug: str, ctx: BotContext) -> Optional[Dict]:
    if ctx.meta_cb.is_open():
        log_debug("[META] meta_cb OPEN -- backing off 5s before retry")
        await asyncio.sleep(5.0)
        return None
    await ctx.rate_limiter.acquire()
    result = await retry_with_backoff(
        _fetch_metadata_sync, slug, ctx.cfg, label=f"meta({slug})", cfg=ctx.cfg
    )
    if result:
        ctx.meta_cb.record_success()
    else:
        ctx.meta_cb.record_failure()
    return result

def get_current_slug() -> Tuple[str, float]:
    now = time.time()
    start_ts = now - (now % 300)
    if (start_ts + 300 - now) < 5:
        start_ts += 300
    return f"xrp-updown-5m-{int(start_ts)}", start_ts

async def fetch_live_bankroll(ctx: BotContext) -> Optional[Decimal]:
    if not ctx.cfg.live_trading or ctx.clob_client is None:
        return None
    try:
        loop = asyncio.get_running_loop()
        balance = await loop.run_in_executor(
            None, lambda: ctx.clob_client.get_balance()
        )
        if isinstance(balance, dict) and "balance" in balance:
            return _d(balance["balance"])
        if isinstance(balance, (int, float, str)):
            return _d(balance)
        return None
    except Exception as exc:
        log_warn(f"[BANKROLL] fetch_live_bankroll failed: "
                 f"{type(exc).__name__}: {exc}")
        return None

def _fetch_fee_via_sdk(clob_client: Any, token_id: str) -> Optional[int]:
    try:
        fn = getattr(clob_client, "get_fee_rate_bps", None)
        if fn is not None:
            result = fn(token_id)
            if isinstance(result, (int, float)) and result > 0:
                return int(result)
            if isinstance(result, dict):
                bps = int(result.get("base_fee", 0) or
                          result.get("fee_rate_bps", 0))
                if bps > 0:
                    return bps
    except Exception as exc:
        log_warn(f"[FEE] SDK get_fee_rate_bps failed: {type(exc).__name__}: "
                 f"{exc}")
    return None

def _fetch_fee_via_curl(clob_url: str, token_id: str) -> Optional[int]:
    url = f"{clob_url}/fee-rate?token_id={token_id}"
    cmd = [
        "curl", "-s", "-S",
        "--max-time", "10",
        "-H", ("User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
               "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 "
               "Safari/537.36"),
        "-H", "Accept: application/json, text/plain, */*",
        "-H", "Origin: https://polymarket.com",
        "-H", "Referer: https://polymarket.com/",
        url,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, timeout=15)
        raw_out = proc.stdout.decode("utf-8", errors="replace").strip()
        if proc.returncode != 0:
            return None
        if raw_out.startswith("<") or "<!DOCTYPE" in raw_out[:50]:
            return None
        data = json.loads(raw_out)
        bps = int(data.get("fee_rate_bps", 0))
        if bps > 0:
            return bps
        return None
    except Exception:
        return None

async def fetch_fee_for_token(token_id: str, ctx: BotContext) -> int:
    """Fetch Polymarket fee for a token from SDK or REST.

    Note: the bps value here is only used for legacy flat-rate arb edge
    calculations (fee_rate_lut). Actual order fees use polymarket_fee().
    """
    loop = asyncio.get_running_loop()
    if ctx.clob_client is not None:
        bps = await loop.run_in_executor(
            None, _fetch_fee_via_sdk, ctx.clob_client, token_id
        )
        if bps is not None:
            ctx.fee_cache[token_id] = bps
            log_info(f"[FEE] token={token_id[:16]}... -> {bps} bps (SDK auth)")
            return bps
    if ctx.clob_ro_client is not None and ctx.clob_ro_client != ctx.clob_client:
        bps = await loop.run_in_executor(
            None, _fetch_fee_via_sdk, ctx.clob_ro_client, token_id
        )
        if bps is not None:
            ctx.fee_cache[token_id] = bps
            log_info(f"[FEE] token={token_id[:16]}... -> {bps} bps (SDK ro)")
            return bps
    bps = await loop.run_in_executor(
        None, _fetch_fee_via_curl, ctx.cfg.clob_rest_url, token_id
    )
    if bps is not None:
        ctx.fee_cache[token_id] = bps
        log_info(f"[FEE] token={token_id[:16]}... -> {bps} bps (curl)")
        return bps
    fallback = ctx.fee_cache.get(token_id, ctx.cfg.default_taker_fee_bps)
    log_warn(f"[FEE] ALL TIERS FAILED for {token_id[:16]}... | "
             f"using cached/default={fallback} bps")
    return fallback

###############################################################################
# SECTION 23 -- BINANCE WS LOOP + FUNDING RATE
###############################################################################
async def binance_ticker_loop(
    binance: BinanceState, bus: EventBus, cfg: BotConfig,
    shutdown: Callable[[], bool]
) -> None:
    try:
        import websockets
    except ImportError:
        log_binance("[WARN] websockets not installed")
        return
    _backoff = cfg.binance_reconnect_base_s
    log_binance(f"WS loop started | uri={cfg.binance_ws_uri}")
    while not shutdown():
        try:
            async with websockets.connect(
                cfg.binance_ws_uri, ping_interval=None, ping_timeout=None,
                open_timeout=15, max_size=2 ** 18,
            ) as ws:
                binance.connected = True
                _backoff = cfg.binance_reconnect_base_s
                log_binance(f"CONNECTED | ticks={binance.tick_count}")
                async def _ping() -> None:
                    while True:
                        await asyncio.sleep(cfg.binance_ping_interval_s)
                        try:
                            await ws.ping()
                        except Exception:
                            break
                ping_t: asyncio.Task = asyncio.ensure_future(_ping())
                try:
                    async for raw in ws:
                        try:
                            data = _json_loads(raw)
                            rp = data.get("c")
                            if rp is None:
                                continue
                            price = float(rp)
                            if price > 0:
                                binance.update_price(price,
                                                     cfg.ewma_vol_alpha)
                                await bus.publish(MarketEvent(
                                    type=EventType.BINANCE_TICK,
                                    payload={"price": price,
                                             "tick_count": binance.tick_count},
                                ))
                        except Exception:
                            pass
                finally:
                    binance.connected = False
                    ping_t.cancel()
                    try:
                        await ping_t
                    except (asyncio.CancelledError, Exception):
                        pass
        except asyncio.CancelledError:
            binance.connected = False
            return
        except Exception as exc:
            binance.connected = False
            log_binance(f"WARN {type(exc).__name__}: {exc} -- retry "
                     f"{_backoff:.1f}s")
            await asyncio.sleep(_backoff)
            _backoff = min(_backoff * 2.0, cfg.binance_reconnect_max_s)

async def funding_rate_loop(
    funding: FundingRateState, cfg: BotConfig, shutdown: Callable[[], bool]
) -> None:
    log_info(f"[FUNDING] Loop started | symbol={cfg.funding_rate_symbol}")
    while not shutdown():
        try:
            await asyncio.sleep(cfg.funding_rate_poll_s)
            def _fetch() -> Optional[float]:
                try:
                    import urllib.request as _ur
                    import urllib.parse as _up
                    params = _up.urlencode({
                        "symbol": cfg.funding_rate_symbol, "limit": 1
                    })
                    url = f"{cfg.funding_rate_url}?{params}"
                    with _ur.urlopen(url, timeout=5) as r:
                        data = _json_loads(r.read())
                        if data and isinstance(data, list):
                            return float(data[0]["fundingRate"])
                except Exception:
                    pass
                return None
            rate = await asyncio.get_running_loop().run_in_executor(
                None, _fetch
            )
            if rate is not None:
                old = funding.rate
                funding.update(rate, cfg)
                if old is None or abs(rate - old) > 1e-6:
                    log_info(f"[FUNDING] rate={funding.signal_str}")
        except asyncio.CancelledError:
            return
        except Exception as exc:
            log_warn(f"[FUNDING] {type(exc).__name__}: {str(exc)[:80]}")

###############################################################################
# SECTION 24 -- POLYMARKET WS HANDLER
###############################################################################
def _update_l2_from_raw(
    l2: L2Snapshot, raw_bids: List[Dict], raw_asks: List[Dict], ts: float
) -> None:
    if raw_bids:
        bids = [
            (pr, sz) for d in raw_bids
            if (pr := _safe_price(d.get("price"))) is not None
            and (sz := _safe_size(d.get("size"))) > 0
        ]
        l2.bids = sorted(bids, key=lambda x: -x[0])
    if raw_asks:
        asks = [
            (pr, sz) for d in raw_asks
            if (pr := _safe_price(d.get("price"))) is not None
            and (sz := _safe_size(d.get("size"))) > 0
        ]
        l2.asks = sorted(asks, key=lambda x: x[0])
    l2.ts = ts

async def ws_handler(
    t_up: str, t_down: str,
    ctx: BotContext,
    binance: BinanceState,
) -> None:
    try:
        import websockets
    except ImportError:
        log_warn("[WS] websockets not installed")
        return
    _tid_map: Dict[str, str] = {t_up: "up", t_down: "down"}
    _backoff = ctx.cfg.ws_reconnect_base_s
    while not ctx.shutdown_flag:
        try:
            async with websockets.connect(
                ctx.cfg.ws_uri,
                ping_interval=ctx.cfg.ws_heartbeat_interval,
                ping_timeout=ctx.cfg.ws_heartbeat_timeout,
            ) as ws:
                sub = {"assets_ids": [t_up, t_down], "type": "market",
                       "custom_feature_enabled": True}
                await ws.send(_json_dumps(sub))
                log_ws_event("OPEN", f"hb={ctx.cfg.ws_heartbeat_interval}s")
                _backoff = ctx.cfg.ws_reconnect_base_s
                async for raw in ws:
                    now = time.time()
                    items = _json_loads(raw)
                    if not isinstance(items, list):
                        items = [items]
                    updated = False
                    for item in items:
                        evt = item.get("event_type")
                        if evt == "market_resolved":
                            wa = item.get("winning_asset_id")
                            if wa:
                                ctx.resolved_winner_asset = wa
                                ctx.resolved_event.set()
                                log_ws_event("RESOLVED", f"winner={wa[:16]}...")
                                await ctx.event_bus.publish(MarketEvent(
                                    type=EventType.MARKET_RESOLVED,
                                    payload={"winner_asset": wa},
                                ))
                            continue
                        sk = _tid_map.get(item.get("asset_id"))
                        if sk is None:
                            continue
                        l2 = ctx.l2_up if sk == "up" else ctx.l2_down
                        if evt == "book":
                            raw_bids = item.get("bids", [])
                            raw_asks = item.get("asks", [])
                            _update_l2_from_raw(l2, raw_bids, raw_asks, now)
                            updated = True
                            if not getattr(ctx, "_first_book_logged", False):
                                log_info(f"[WS] FIRST BOOK received for {sk} | bids={len(raw_bids)} asks={len(raw_asks)}")
                                ctx._first_book_logged = True
                            await ctx.event_bus.publish(MarketEvent(
                                type=EventType.BOOK_SNAPSHOT,
                                payload={"ts": now},
                            ))
                        elif evt in ("best_bid_ask", "price_change"):
                            src = item
                            if evt == "price_change":
                                pcs = item.get("price_changes", [])
                                if pcs:
                                    src = pcs[-1]
                            bb, ba = src.get("best_bid"), src.get("best_ask")
                            if bb and float(bb) > 0:
                                if not l2.bids:
                                    l2.bids = [(float(bb), 0.0)]
                                elif abs(l2.bids[0][0] - float(bb)) > 1e-6:
                                    l2.bids[0] = (float(bb), l2.bids[0][1])
                            if ba and float(ba) > 0:
                                if not l2.asks:
                                    l2.asks = [(float(ba), 0.0)]
                                elif abs(l2.asks[0][0] - float(ba)) > 1e-6:
                                    l2.asks[0] = (float(ba), l2.asks[0][1])
                            l2.ts = now
                            updated = True
                        if updated:
                            ctx.best_bids[sk] = l2.best_bid()
                            ctx.best_asks[sk] = l2.best_ask()
                            ctx.best_spreads_c[sk] = l2.spread_cents()
                            ctx.best_bid_sizes[sk] = l2.bids[0][1] if l2.bids else None
                            ctx.best_ask_sizes[sk] = l2.asks[0][1] if l2.asks else None
                        if updated:
                            await ctx.event_bus.publish(MarketEvent(
                                type=EventType.PRICE_UPDATE,
                                payload={
                                    "ts": now,
                                    "bid_up": ctx.best_bids.get("up"),
                                    "ask_up": ctx.best_asks.get("up"),
                                    "bid_down": ctx.best_bids.get("down"),
                                    "ask_down": ctx.best_asks.get("down"),
                                },
                            ))
        except asyncio.CancelledError:
            break
        except Exception as exc:
            log_ws_event("ERROR", f"{type(exc).__name__}: {exc} -- reconnect "
                         f"{_backoff:.1f}s")
            await asyncio.sleep(_backoff)
            _backoff = min(_backoff * 2.0, ctx.cfg.ws_reconnect_max_s)

###############################################################################
# SECTION 25 -- USER WS + HEARTBEAT
###############################################################################
def _build_l2_auth(
    api_key: str, secret: str, passphrase: str, condition_id: str
) -> Dict:
    ts: str = str(int(time.time()))
    msg: bytes = (ts + "GET" + "/ws-auth").encode("utf-8")
    try:
        key_bytes = base64.b64decode(secret)
    except Exception:
        key_bytes = secret.encode("utf-8")
    sig = base64.b64encode(
        hmac.new(key_bytes, msg, hashlib.sha256).digest()
    ).decode()
    return {
        "type": "auth", "channel": "user", "market": condition_id,
        "auth": {"apiKey": api_key, "secret": secret, "passphrase": passphrase,
                 "timestamp": ts, "signature": sig},
    }

async def user_ws_loop(
    api_key: str, secret: str, passphrase: str, condition_id: str,
    token_ids: List[str], ctx: BotContext,
) -> None:
    try:
        import websockets
    except ImportError:
        return
    _backoff = ctx.cfg.ws_reconnect_base_s
    last_open_log: float = 0.0  # v9.4.0: throttle [USER_WS] OPEN log
    while not ctx.shutdown_flag:
        try:
            async with websockets.connect(
                ctx.cfg.user_ws_uri, ping_interval=None, open_timeout=15,
            ) as ws:
                auth_payload = _build_l2_auth(api_key, secret, passphrase,
                                              condition_id)
                await ws.send(_json_dumps(auth_payload))
                # v9.4.0: throttle OPEN log to once per USER_WS_OPEN_LOG_INTERVAL
                if time.time() - last_open_log >= USER_WS_OPEN_LOG_INTERVAL:
                    log_raw(f"[USER_WS] OPEN | market={condition_id[:16]}...")
                    last_open_log = time.time()
                _backoff = ctx.cfg.ws_reconnect_base_s
                async def _ping() -> None:
                    while True:
                        await asyncio.sleep(4.0)
                        try:
                            await ws.send("PING")
                        except Exception:
                            break
                pt: asyncio.Task = asyncio.ensure_future(_ping())
                try:
                    async for raw in ws:
                        try:
                            text = raw.decode() if isinstance(raw, bytes) else raw
                            if text.strip() == "PONG":
                                continue
                            data = _json_loads(text)
                            if not isinstance(data, dict):
                                continue
                            if "taker_fee_rate_bps" in data:
                                bps = int(data["taker_fee_rate_bps"])
                                for tid in token_ids:
                                    ctx.fee_cache[tid] = bps
                            if data.get("event_type") == "market_resolved":
                                _wa = data.get("winning_asset_id")
                                _msg_cid = data.get("condition_id", "")
                                if _msg_cid and ctx.current_condition_id and \
                                        _msg_cid != ctx.current_condition_id:
                                    log_debug(
                                        f"[USER_WS] Ignoring stale resolution "
                                        f"for {_msg_cid[:16]}... "
                                        f"(current={ctx.current_condition_id[:16]}...)"
                                    )
                                    continue
                                if _wa:
                                    ctx.resolved_winner_asset = _wa
                                    ctx.resolved_event.set()
                                    log_info(
                                        f"[USER_WS] RESOLVED | "
                                        f"winner={_wa[:16]}..."
                                    )
                                    if ctx.pending_settlements:
                                        async with ctx._settlements_lock:
                                            _resolved = []
                                            for ps in ctx.pending_settlements:
                                                _ps_tokens = {
                                                    t.token_id for t in ps.trades
                                                }
                                                if _wa in _ps_tokens or (
                                                    ps.meta.get("up") in _ps_tokens or
                                                    ps.meta.get("down") in _ps_tokens
                                                ):
                                                    _resolved.append(ps)
                                            for ps in _resolved:
                                                ctx.pending_settlements.remove(ps)
                                            if _resolved:
                                                age_s = time.time() - ps.created_ts
                                                log_info(
                                                    f"[USER_WS] LATE RESOLUTION | "
                                                    f"winner={_wa[:16]}... | "
                                                    f"clearing {len(ps.trades)} pending trades | "
                                                    f"locked={ps.locked_capital} | "
                                                    f"age={age_s:.0f}s"
                                                )
                        except Exception as exc:
                            log_debug(f"[USER_WS] Parse error: {type(exc).__name__}")
                finally:
                    pt.cancel()
                    try:
                        await pt
                    except (asyncio.CancelledError, Exception):
                        pass
        except asyncio.CancelledError:
            return
        except Exception as exc:
            if ctx.cfg.live_trading:
                log_warn(f"[USER_WS] {type(exc).__name__}: {str(exc)[:80]} -- retry "
                         f"{_backoff:.1f}s")
            await asyncio.sleep(_backoff)
            _backoff = min(_backoff * 2.0, ctx.cfg.ws_reconnect_max_s)

async def heartbeat_loop(ctx: BotContext) -> None:
    _id: Optional[str] = None
    errors: int = 0
    log_info("[HEARTBEAT] Loop started")
    while not ctx.shutdown_flag:
        try:
            await asyncio.sleep(5.0)
            if ctx.clob_client is None:
                continue
            loop = asyncio.get_running_loop()
            snap = _id
            resp = await loop.run_in_executor(
                None, lambda: ctx.clob_client.post_heartbeat(snap)
            )
            if isinstance(resp, dict):
                _new = resp.get("heartbeat_id") or resp.get("id") or \
                       resp.get("next_id")
                if _new and _new != _id:
                    _id = _new
                    errors = 0
        except asyncio.CancelledError:
            return
        except Exception as exc:
            estr = str(exc)
            if "400" in estr or "invalid" in estr.lower():
                _id = None
            else:
                errors += 1
                if errors >= 10:
                    await asyncio.sleep(30.0)
                    errors = 0

###############################################################################
# SECTION 26 -- RECONCILIATION + POLY-WEB3 REDEEM + SETTLEMENT WAIT
###############################################################################
async def auto_redeem_positions(ctx: BotContext) -> bool:
    if not ctx.cfg.live_trading:
        return False
    if ctx.redeem_cb is not None and ctx.redeem_cb.is_open():
        log_debug("[REDEEM] Circuit breaker OPEN -- skipping redeem attempt")
        return False
    try:
        from poly_web3 import PolymarketClient
    except ImportError:
        log_warn("[REDEEM] poly-web3 not installed -- pip install poly-web3. "
                 "Auto-redeem disabled.")
        return False
    try:
        client = PolymarketClient(private_key=ctx.cfg.polymarket_private_key)
    except Exception as exc:
        log_warn(f"[REDEEM] Failed to init PolymarketClient: "
                 f"{type(exc).__name__}: {exc}")
        if ctx.redeem_cb is not None:
            ctx.redeem_cb.record_failure()
        return False
    async with ctx._settlements_lock:
        _to_process = list(ctx.pending_settlements)
        if not _to_process:
            return True
        _redeemed = []
        _failed = 0
        for ps in _to_process:
            cid = ps.meta.get("id")
            if not cid:
                continue
            try:
                await ctx.rate_limiter.acquire()
                success = await client.redeem_condition(cid)
                if success:
                    log_info(
                        f"[REDEEM] poly-web3 SUCCESS condition={cid[:16]}..."
                    )
                    _redeemed.append(ps)
                    if ctx.redeem_cb is not None:
                        ctx.redeem_cb.record_success()
                else:
                    _failed += 1
                    if ctx.redeem_cb is not None:
                        ctx.redeem_cb.record_failure()
            except Exception as exc:
                log_warn(f"[REDEEM] FAILED condition={cid[:16]}...: "
                         f"{type(exc).__name__}: {exc}")
                _failed += 1
                if ctx.redeem_cb is not None:
                    ctx.redeem_cb.record_failure()
        if _redeemed:
            async with ctx._settlements_lock:
                for ps in _redeemed:
                    if ps in ctx.pending_settlements:
                        ctx.pending_settlements.remove(ps)
            log_info(
                f"[REDEEM] Completed: {len(_redeemed)} redeemed, {_failed} "
                f"failed, {len(ctx.pending_settlements)} remaining"
            )
            return len(ctx.pending_settlements) == 0
    return False

async def wait_for_settlement(
    ctx: BotContext, tsm: TradeStateManager, timeout: float = 0.0
) -> bool:
    cfg = ctx.cfg
    _timeout = timeout if timeout > 0 else cfg.settlement_timeout_s
    start = time.time()
    _poll_delay = cfg.settlement_backoff_base_s
    while time.time() - start < _timeout:
        if ctx.resolved_event.is_set() and ctx.resolved_winner_asset:
            official_winner = ctx.resolved_winner_asset
            log_info(
                f"[RESOLUTION] Official winner={official_winner[:16]}... from "
                f"Polymarket WS"
            )
            async with ctx._settlements_lock:
                if not ctx.pending_settlements:
                    log_info("[RESOLUTION] No pending settlements to process")
                    return True
                total_pnl = _ZERO
                total_payout = _ZERO
                settled_count = 0
                for ps in list(ctx.pending_settlements):
                    for trade in ps.trades:
                        is_win = (trade.token_id == official_winner)
                        # Change 9: payout = sharesx$1.00 for winner, NO sell fee
                        payout = trade.shares if is_win else _ZERO
                        pnl = _dq(payout - trade.total_out)
                        total_pnl += pnl
                        total_payout += payout
                        settled_count += 1
                        result_str = "WIN ($1/share)" if is_win else "LOSS (total)"
                        log_info(
                            f"[RESOLUTION] Trade {trade.side} | {result_str} | "
                            f"shares={trade.shares} | pnl={fmt_dollar(pnl)}"
                        )
                ctx.pending_settlements.clear()
                if cfg.live_trading:
                    await auto_redeem_positions(ctx)
                    lb = await fetch_live_bankroll(ctx)
                    if lb is not None:
                        # Change 9: only update upward on winning settlements
                        if lb > tsm.state.bankroll or total_pnl < _ZERO:
                            tsm.update_bankroll(lb)
                            ctx.last_reconciled_bankroll = lb
                        else:
                            log_debug(
                                f"[RESOLUTION] Skipping downward bankroll override "
                                f"live={lb} internal={tsm.state.bankroll}"
                            )
                else:
                    # Change 9: add payout (not pnl) -- total_out already deducted at entry
                    new_bankroll = tsm.state.bankroll + total_payout
                    tsm.update_bankroll(new_bankroll)
                    log_info(
                        f"[RESOLUTION] DRY_RUN bankroll update: "
                        f"{tsm.state.bankroll - total_payout} + payout={fmt_dollar(total_payout)} "
                        f"= {tsm.state.bankroll}"
                    )
                tsm.update_martingale(total_pnl, cfg)
                tsm.update_daily_pnl(total_pnl)
                # Reset consecutive losses on positive PnL
                if total_pnl > _d("1e-9"):
                    tsm.state.consecutive_losses = 0
                await tsm.save_async()
                log_info(
                    f"[RESOLUTION] Official winner={official_winner[:16]}... | "
                    f"settled={settled_count} trades | PnL={fmt_dollar(total_pnl)} "
                    f"| payout={fmt_dollar(total_payout)} "
                    f"| Banca={tsm.state.bankroll} | "
                    f"mart_level={tsm.state.mart_level} | "
                    f"consec_losses={tsm.state.consecutive_losses}"
                )
                return True
        if not ctx.pending_settlements:
            log_info("[SETTLE] Pending settlements cleared (possibly by "
                     "user_ws_loop)")
            return True
        await asyncio.sleep(min(_poll_delay, 15.0))
        _poll_delay *= 1.5
    log_warn(f"[SETTLE] Timeout {_timeout:.0f}s -- pending settlements remain "
             f"unresolved")
    return False

async def reconciliation_loop(tsm: TradeStateManager, ctx: BotContext) -> None:
    log_info(f"[RECONCILE] Loop started | "
             f"interval={ctx.cfg.reconcile_interval_s:.0f}s")
    _tick = 0
    while not ctx.shutdown_flag:
        try:
            await asyncio.sleep(ctx.cfg.reconcile_interval_s)
            _tick += 1
            _now = time.time()
            _stale_order_keys = [
                k for k, v in ctx.pending_orders.items()
                if _now - v.get("ts", 0) > 300
            ]
            for k in _stale_order_keys:
                del ctx.pending_orders[k]
            if _stale_order_keys:
                log_debug(f"[RECONCILE] Cleaned {len(_stale_order_keys)} stale "
                          f"pending_orders")
            if not ctx.cfg.live_trading or ctx.clob_client is None:
                continue
            async with ctx._settlements_lock:
                if ctx.pending_settlements:
                    _redeem_ok = await auto_redeem_positions(ctx)
                    if _redeem_ok:
                        log_info(
                            f"[RECONCILE] Auto-redeem triggered | "
                            f"pending={len(ctx.pending_settlements)}"
                        )
                        await asyncio.sleep(2.0)
                    lb = await fetch_live_bankroll(ctx)
                    if lb is None:
                        continue
                    prev_recon = ctx.last_reconciled_bankroll
                    ctx.last_reconciled_bankroll = lb
                    _delta = lb - prev_recon if prev_recon > _ZERO else _ZERO
                    async with ctx._settlements_lock:
                        if ctx.pending_settlements and _delta > _d("0.001"):
                            _resolved = []
                            for ps in ctx.pending_settlements:
                                _resolved.append(ps)
                            for ps in _resolved:
                                ctx.pending_settlements.remove(ps)
                            if _resolved:
                                log_info(f"[RECONCILE] Cleared {len(_resolved)} "
                                         f"settlements on bankroll delta")
                    _stale = [ps for ps in ctx.pending_settlements if ps.is_stale]
                    for ps in _stale:
                        await auto_redeem_positions(ctx)
                        await asyncio.sleep(1.0)
                        _lb2 = await fetch_live_bankroll(ctx)
                        if _lb2 is not None and _lb2 > lb + _d("0.001"):
                            lb = _lb2
                            ctx.last_reconciled_bankroll = _lb2
                        else:
                            log_warn(
                                f"[RECONCILE] STALE PENDING_SETTLEMENT force-cleared | "
                                f"age={time.time() - ps.created_ts:.0f}s"
                            )
                            ctx.pending_settlements.remove(ps)
                    diff = abs(float(lb - tsm.state.bankroll))
                    if diff > 0.01:
                        # Change 9: only override internal bankroll upward to avoid
                        # wiping out a winning settlement that hasn't propagated yet
                        if lb > tsm.state.bankroll:
                            log_info(
                                f"[RECONCILE] Bankroll sync ↑ | old={tsm.state.bankroll} -> "
                                f"new={lb} | diff=${diff:.4f} | "
                                f"pending={len(ctx.pending_settlements)}"
                            )
                            tsm.update_bankroll(lb)
                            await tsm.save_async()
                        else:
                            log_debug(
                                f"[RECONCILE] Skipping downward sync | "
                                f"live={lb} internal={tsm.state.bankroll} | "
                                f"pending={len(ctx.pending_settlements)} -- "
                                f"may be winning settlement not yet redeemed"
                            )
        except asyncio.CancelledError:
            return
        except Exception as exc:
            log_warn(f"[RECONCILE] tick={_tick} | EXCEPTION: "
                     f"{type(exc).__name__}: {exc}")

def _is_ultra_bull(
    binance: BinanceState, funding: FundingRateState, cfg: BotConfig
) -> bool:
    fr = funding.rate
    d = binance.drift_5m
    if fr is None or d is None:
        return False
    return fr > cfg.funding_rate_bull_thresh and d > 0.005

async def process_pre_settlement(
    ctx: BotContext,
    tsm: TradeStateManager,
    active_trades: List[Trade],
    bankroll: Decimal,
    cfg: BotConfig,
    meta: Dict[str, str],
    audit: AuditLogger,
    cycle_start_bankroll: Optional[Decimal] = None,
    pre_realized_pnl: Optional[Decimal] = None,
) -> Tuple[Decimal, bool, Decimal]:
    """
    v9.4.0 -- PnL Refinement + Critical Bankroll Fix.

    pre_realized_pnl: PnL already realized this cycle via intra-cycle close_trade
    calls (GAMBLING TP, PEG ARBIT partial closes, etc.). This is the FIX for
    ROUND PnL showing $0.0000 even after successful GAMBLING sells -- those trades
    are removed from active_trades before cycle end, so without this parameter
    they were invisible to the settlement calculation.

    Total round_pnl_realized = pre_realized_pnl + pnl from still-open positions.
    """

    # ── Step 2: bail if no filled trades ─────────────────────────────────────
    filled_trades = [t for t in active_trades if t.filled]
    if not filled_trades and not active_trades:
        # v9.4.1 FIX: even with no open positions at settlement, intra-cycle
        # sells (GAMBLING TP, PEG closes) may have produced realized PnL.
        # Previously this returned _ZERO, silently dropping the PnL and
        # never calling update_martingale -- so wins never reset the mart.
        _pre = pre_realized_pnl if pre_realized_pnl is not None else _ZERO
        if _pre != _ZERO:
            tsm.update_martingale(_pre, cfg)
            # daily_pnl already updated by close_trade() -- do NOT add again
            log_info(
                f"[PNL_REFINED] No open positions at settlement | "
                f"pre_realized={fmt_dollar(_pre)} | mart updated"
            )
        return bankroll, False, _pre

    # ── Step 3: winner detection -- official WS first, then smart heuristic ──
    # Prefer ctx.resolved_winner_asset (set by user_ws_loop when
    # Polymarket fires market_resolved event with the official winning token).
    final_ask_up   = ctx.best_asks.get("up")   or 0.0
    final_ask_down = ctx.best_asks.get("down") or 0.0

    if ctx.resolved_winner_asset:
        # Official winner from Polymarket WS -- most reliable
        heur_token  = ctx.resolved_winner_asset
        heur_winner = "UP" if heur_token == meta["up"] else "DOWN"
        _get_logger().info(
            "[INFO] [WINNER] [%s] | %s (WS official) | "
            "ask_up=%s ask_dn=%s | token=%s...",
            _ts(), heur_winner, fc(final_ask_up), fc(final_ask_down),
            heur_token[:16]
        )
    else:
        # Heuristic fallback with Bayesian tiebreak (change 4)
        _abs_diff = abs(final_ask_up - final_ask_down)
        if _abs_diff < 0.005:
            # Market is ambiguous -- use Bayesian posterior
            p_up, _ = (0.5, 0.5)  # default; overridden if bayesian is in scope
            # best effort: read from ctx if stored
            _stored_p = getattr(ctx, '_last_p_hat_up', None)
            if _stored_p is not None:
                p_up = _stored_p
            heur_winner = "UP" if p_up > 0.5 else "DOWN"
            _get_logger().info(
                "[INFO] [WINNER] [%s] | %s (Bayesian tiebreak) | "
                "ask_diff=%.4f p_hat_up=%.3f | ask_up=%s ask_dn=%s",
                _ts(), heur_winner, _abs_diff, p_up,
                fc(final_ask_up), fc(final_ask_down)
            )
        else:
            heur_winner = "UP" if final_ask_up >= final_ask_down else "DOWN"
            _get_logger().info(
                "[INFO] [WINNER] [%s] | %s (heuristic) | "
                "ask_up=%s ask_dn=%s",
                _ts(), heur_winner, fc(final_ask_up), fc(final_ask_down)
            )
        heur_token = meta["up"] if heur_winner == "UP" else meta["down"]

    # ── v9.5.2: Settlement log -- WIN/LOSS shares breakdown ──────────────
    _win_shares = _ZERO
    _loss_shares = _ZERO
    for _st in filled_trades:
        if _st.token_id == heur_token:
            _win_shares += _st.shares
        else:
            _loss_shares += _st.shares
    _total_redeemed = _win_shares  # winners redeem at $1.00 each
    _get_logger().info(
        "[INFO] [SETTLEMENT] [%s] | WIN shares = %s @ $1.00 | "
        "LOSS shares = %s @ $0.00 | Total redeemed = $%s",
        _ts(),
        f"{float(_win_shares):.4f}",
        f"{float(_loss_shares):.4f}",
        f"{float(_total_redeemed):.4f}",
    )

    # ── Step 5: compute REALIZED PnL with CORRECT payout (change 9) ─────────
    # CRITICAL FIX: payout for winner = shares x $1.00 (full redemption).
    # NO sell fee on resolution -- Polymarket redemption is fee-free.
    # pnl = payout - total_out  (total_out already includes buy fee paid at entry)
    # bankroll gets += payout for winners (not += pnl, to avoid double-counting).
    def _calc_group_pnl_and_payout(
        trades: List[Trade],
    ) -> Tuple[Decimal, Decimal]:
        """Returns (pnl, total_payout_added_to_bankroll)."""
        pnl_sum    = _ZERO
        payout_sum = _ZERO
        for trade in trades:
            is_win = (trade.token_id == heur_token)
            # Change 9: payout = sharesx1.0 for winner, 0 for loser -- NO fee
            payout = trade.shares if is_win else _ZERO
            pnl    = _dq(payout - trade.total_out)
            pnl_sum    += pnl
            payout_sum += payout
        return pnl_sum, payout_sum

    trade_pnl, total_payout = _calc_group_pnl_and_payout(filled_trades)
    _pre = pre_realized_pnl if pre_realized_pnl is not None else _ZERO
    round_pnl_realized = trade_pnl + _pre

    # ── Step 6: unrealized PnL on still-open (unfilled) trades ───────────────
    unfilled_trades = [t for t in active_trades if not t.filled]
    unrealized_pnl  = _ZERO
    for trade in unfilled_trades:
        side_key = trade.side.lower()
        bid_now  = ctx.best_bids.get(side_key) or 0.0
        if bid_now > 1e-9 and trade.total_out > _ZERO:
            sell_fee_est = polymarket_fee(float(trade.shares), bid_now)
            net_bid = bid_now - (sell_fee_est / max(float(trade.shares), 1e-9))
            mark_value = _dq(trade.shares * _d(net_bid))
            unrealized_pnl += _dq(mark_value - trade.total_out)

    # ── Step 7: log refined PnL breakdown ────────────────────────────────────
    log_info(
        f"[PNL_REFINED] Realized={fmt_dollar(round_pnl_realized)} | "
        f"PreRealized={fmt_dollar(_pre)} | "
        f"Trades={fmt_dollar(trade_pnl)} | "
        f"Unrealized={fmt_dollar(unrealized_pnl)}"
    )

    # ── Step 8: build PendingSettlement ──────────────────────────────────────
    ps = PendingSettlement(
        trades=list(filled_trades),
        heuristic_winner=heur_winner,
        winner_token=heur_token,
        meta=dict(meta),
        theoretical_pnl=round_pnl_realized,
    )
    ctx.pending_settlements.append(ps)
    active_trades.clear()
    tsm.active_trades = []

    # ── Step 9: update bankroll CORRECTLY ─────────────────────────────────────
    # Only add payout from still-open resolved positions.
    # pre_realized_pnl trades already had their payout_net added to bankroll
    # inside close_trade() during the cycle -- do NOT double-count here.
    bankroll += total_payout
    tsm.update_bankroll(bankroll)

    # ── Step 10: martingale + daily PnL ───────────────────────────────────────
    # v9.4.0: daily_pnl was already partially updated by close_trade() for each
    # intra-cycle sell. Here we only add the open-position settlement PnL to avoid
    # double-counting the pre_realized portion.
    settlement_only_pnl = trade_pnl
    tsm.update_martingale(round_pnl_realized, cfg)
    # Only add the settlement portion here; pre_realized was added live in close_trade
    tsm.update_daily_pnl(settlement_only_pnl)
    # Positive realized PnL resets consecutive_losses
    if round_pnl_realized > _d("1e-9"):
        tsm.state.consecutive_losses = 0

    await tsm.save_async()

    _round_pnl_pct = _safe_float(round_pnl_realized / bankroll * 100) if \
        bankroll > _d("1e-9") else 0.0

    log_warn(
        f"END OF MARKET | PRE-SETTLEMENT UPDATED | "
        f"winner={heur_winner} | "
        f"final_ask_up={fc(final_ask_up)} final_ask_dn={fc(final_ask_down)} | "
        f"locked_capital={ps.locked_capital} | "
        f"{fmt_pnl(round_pnl_realized, _round_pnl_pct)} "
        f"[pre={fmt_dollar(_pre)} + trades={fmt_dollar(trade_pnl)}] | "
        f"payout_added={fmt_dollar(total_payout)} | "
        f"filled_trades={len(filled_trades)} | "
        f"banca={bankroll}"
    )
    return bankroll, True, round_pnl_realized

###############################################################################
# SECTION 29B -- BUG FIX FUNCTIONS (v9.4.2)
###############################################################################

# BUG FIX: EV negativo = entrada bloqueada (evita drawdown)
def should_enter_trade(edge_data: dict) -> bool:
    """Valida se o trade tem EV positivo antes de entrar.

    Args:
        edge_data: dict com chave 'EV' (float) = p_hat - ask.

    Returns:
        True se EV > 0 (trade permitido), False caso contrário.
    """
    if edge_data['EV'] <= 0.0:
        log_info(f"EV negativo ({edge_data['EV']:.4f}) - skip [BUG CORRIGIDO]")
        return False
    return True


# BUG FIX: Comparação constante BNC vs K para garantir direcionalidade correta + uso velas últimas 10min
def check_direction_bias(
    bnc_current: float, k_reference: float, p_up: float,
    *,
    neutral_thresh_pct: float = 0.003,
) -> str:
    """Compara preço BNC actual com K do ciclo para determinar viés direccional.

    Args:
        bnc_current:  Preço Binance actual (BNC_CURRENT).
        k_reference:  Strike/open do ciclo (K_REFERENCE).
        p_up:         Probabilidade P(UP) actual.
        neutral_thresh_pct: Limiar percentual para zona neutra (default 0.3%).

    Returns:
        "UP", "DOWN" ou "NEUTRAL".
    """
    # Verificando velas últimas 10min Binance para predição 5m
    if k_reference is None or k_reference <= 0.0:
        return "NEUTRAL"
    if bnc_current is None or bnc_current <= 0.0:
        return "NEUTRAL"

    diff: float = bnc_current - k_reference
    pct_diff: float = (diff / k_reference) * 100.0

    if abs(pct_diff) < neutral_thresh_pct:
        bias = "NEUTRAL"
    elif diff > 0.0:
        bias = "UP"
    else:
        bias = "DOWN"

    log_info(
        f"Direction check: BNC={bnc_current:.5f} | K={k_reference:.5f} | "
        f"diff={diff:.5f} ({pct_diff:+.3f}%) → viés={bias} | P(UP)={p_up:.3f}"
    )
    return bias


# BUG FIX + HEDGE ULTRA-SENSÍVEL: trigger a 1c com confirmação forte
def check_adverse_hedge(
    active_trades: List["Trade"],
    ctx: "BotContext",
    binance: "BinanceState",
    p_hat_up: float,
    p_hat_down: float,
    cfg: "BotConfig",
) -> Optional[Dict[str, Any]]:
    """v9.5.1: Ultra-fast HEDGE_FLIP for GAMBLING trades ONLY.

    NEVER fires on: VOL_HEDGE_YES, VOL_HEDGE_NO, ENDGAME_AGG, PEG_ARBIT.

    Uses flexible confirmation (need hedge_flip_confirms_needed of 5 signals):
      1. Loss >= adverse_stop_cents (0.3c)
      2. BNC vs K confirms opposite direction
      3. Binance trend confirms (FALLING/RISING)
      4. BNC price velocity exceeds speed threshold (fast adverse move)
      5. Orderbook imbalance on losing side < threshold (sellers dominate)
    Plus: P(opposite) >= hedge_min_prob_opposite always required.
    """
    if not active_trades:
        return None
    if binance.current_price is None or binance.cycle_open_price is None:
        return None

    bnc = binance.current_price
    k = binance.cycle_open_price
    trend = binance.recent_trend(lookback_s=300.0)

    # v9.5.1: pre-compute BNC velocity (price change per second)
    _bnc_velocity = 0.0
    if len(binance._price_history_10s) >= 2:
        _ts_old, _px_old = binance._price_history_10s[-2]
        _ts_new, _px_new = binance._price_history_10s[-1]
        _dt = max(_ts_new - _ts_old, 0.1)
        if _px_old > 1e-9:
            _bnc_velocity = abs((_px_new - _px_old) / _px_old) / _dt

    for trade in active_trades:
        # v9.5.1: ONLY fire on GAMBLING trades
        if trade.type != "GAMBLING":
            continue
        side = trade.side
        bid_now = ctx.best_bids.get(side.lower())
        if bid_now is None:
            continue

        loss_cents = (trade.ask - bid_now) * 100.0

        opposite = "DOWN" if side == "UP" else "UP"
        p_opposite = p_hat_down if side == "UP" else p_hat_up

        # P(opposite) always required
        if p_opposite < cfg.hedge_min_prob_opposite:
            continue

        # Count confirmation signals
        _confirms = 0

        # Signal 1: Loss exceeds threshold
        if loss_cents >= cfg.adverse_stop_cents:
            _confirms += 1

        # Signal 2: BNC vs K direction
        if side == "UP" and bnc < k:
            _confirms += 1
        elif side == "DOWN" and bnc > k:
            _confirms += 1

        # Signal 3: Binance trend
        if side == "UP" and trend == "FALLING":
            _confirms += 1
        elif side == "DOWN" and trend == "RISING":
            _confirms += 1

        # Signal 4: BNC velocity (fast adverse move)
        if _bnc_velocity >= cfg.hedge_flip_speed_thresh:
            _price_dir = bnc - k
            if (side == "UP" and _price_dir < 0) or \
               (side == "DOWN" and _price_dir > 0):
                _confirms += 1

        # Signal 5: Orderbook imbalance shift
        _micro_side = ctx.l2_up if side == "up" else ctx.l2_down
        if not _micro_side.is_stale(2.0):
            _bid_d = _micro_side.bid_depth(3)
            _ask_d = _micro_side.ask_depth(3)
            _total = _bid_d + _ask_d
            _obi = _bid_d / _total if _total > 1e-9 else 0.5
            if _obi < cfg.hedge_flip_imbalance_thresh:
                _confirms += 1

        if _confirms < cfg.hedge_flip_confirms_needed:
            continue

        opp_ask = ctx.best_asks.get(opposite.lower())
        if opp_ask is None or opp_ask <= 0.0:
            continue

        return {
            "trade": trade,
            "side_losing": side,
            "side_hedge": opposite,
            "loss_cents": loss_cents,
            "bid_sell": bid_now,
            "ask_hedge": opp_ask,
            "p_opposite": p_opposite,
            "trend": trend,
            "bnc": bnc,
            "k": k,
            "confirms": _confirms,
            "bnc_velocity": _bnc_velocity,
        }

    return None


###############################################################################
# SECTION 30 -- LOGIC LOOP (v9.4.0)
###############################################################################
async def logic_loop(
    m_start: float,
    m_end: float,
    meta: Dict[str, str],
    tsm: TradeStateManager,
    ctx: BotContext,
    binance: BinanceState,
    funding: FundingRateState,
    safety: CapitalSafetyMonitor,
    audit: AuditLogger,
) -> Tuple[Decimal, float, bool, Decimal]:
    cfg = ctx.cfg
    bankroll: Decimal = tsm.state.bankroll
    if bankroll <= _ZERO or not bankroll.is_finite():
        log_warn("[LOGIC] Invalid bankroll -- aborting cycle")
        return tsm.state.bankroll, 0.5, False, _ZERO
    active_trades: List[Trade] = []
    bayesian = BayesianTracker(
        prior=cfg.bayesian_prior,
        std=cfg.bayesian_likelihood_std,
        decay_rate=cfg.bayesian_decay_rate,
    )
    micro_analyzer = MicrostructureAnalyzer()
    timer = MarketTimer(market_end_ts=m_end, cfg=cfg)
    mart_level = tsm.state.mart_level


    # v9.4.0 PnL FIX: snapshot bankroll at the true start of this cycle,
    # BEFORE any matching-engine fills, so main() can compute a clean ROUND PnL.
    cycle_start_bankroll: Decimal = bankroll
    # v9.4.0 PnL FIX: accumulate realized PnL from intra-cycle GAMBLING/PEG
    # sells (close_trade) so ROUND PnL is correct even when market resolves
    # with zero open trades. Without this, ROUND PnL was always $0.0000.
    cycle_realized_pnl: Decimal = _ZERO

    log_sep2()
    k_val = binance.cycle_open_price
    k_str = f"{k_val:.5f}" if k_val is not None else "n/a"
    log_info(
        f"NOVO CICLO v9.5.4 | {meta['slug']} | LIVE={cfg.live_trading} | "
        f"DRY={cfg.dry_run} | K={k_str} | Banca={bankroll} | Mart x{mart_level}"
    )
    # v9.4.2 BUG FIX: direction bias check no início do ciclo
    if binance.current_price is not None and k_val is not None:
        check_direction_bias(binance.current_price, k_val, 0.5)
    log_sep()

    vol_trackers: Dict[str, VolatilityEdgeTracker] = {
        "UP": VolatilityEdgeTracker(cfg),
        "DOWN": VolatilityEdgeTracker(cfg),
    }

    # v9.5.0: Reset vol-hedge engine for this cycle
    _vh_engine: Optional[VolatilityHedgeEngine] = ctx.vol_hedge_engine
    if _vh_engine is not None:
        _vh_engine.cleanup_cycle()
        log_info(
            f"[VOL_HEDGE] Cycle reset | SD_window={cfg.vol_hedge_sd_window} | "
            f"1SD_mult={cfg.vol_hedge_1sd_trigger} 3SD_mult={cfg.vol_hedge_3sd_target} | "
            f"NO_limit=[{cfg.vol_hedge_no_limit_low:.2f}-{cfg.vol_hedge_no_limit_high:.2f}]"
        )
    _vh_last_feed_ts: float = 0.0  # throttle price feed to engine

    def close_trade(
        trade: Trade, sell_bid: float, reason: str, rstr: str
    ) -> Decimal:
        nonlocal bankroll, cycle_realized_pnl
        # Polymarket sell fee: fee = C x p x 0.25 x (p x (1-p))^2
        # Fee deducted from USDC received on sell
        payout_br = _dq(trade.shares * _d(sell_bid))
        fee_sell = polymarket_fee_decimal(trade.shares, sell_bid)
        payout_net = _dq(payout_br - fee_sell)
        pnl = _dq(payout_net - trade.total_out)
        pnl_pct = _safe_float(pnl / trade.total_out * 100) if trade.total_out \
            else 0.0
        bankroll += payout_net
        # v9.4.0 PnL FIX: accumulate every realized sell into cycle_realized_pnl
        # so ROUND PnL reflects GAMBLING/PEG sells even when no trades remain open.
        cycle_realized_pnl += pnl
        # Also update tsm daily_pnl + bankroll immediately so live logs are correct
        tsm.state.daily_pnl += pnl
        tsm.update_bankroll(bankroll)
        log_m(
            trade.type, "SELL",
            f"rem={rstr} | {trade.side} @ BID={fc(sell_bid)} | "
            f"bruto={fmt_dollar(payout_br)} | "
            f"fee_sell={fmt_fee(fee_sell, payout_br or _ONE)} | "
            f"net={fmt_dollar(payout_net)} | {fmt_pnl(pnl, pnl_pct)} | "
            f"Reason: {reason}",
        )
        # v9.4.0: audit.log_trade() -> JSONL only, no console duplication
        audit.log_trade(
            strategy=trade.type, action="SELL",
            symbol=f"{trade.side}@{trade.ask:.4f}",
            price=sell_bid, mart_level=mart_level,
            pnl_round=pnl, pnl_day=tsm.state.daily_pnl, fee=fee_sell,
        )
        vol_trackers[trade.side].record_outcome(pnl > _ZERO, _safe_float(pnl))
        return pnl

    def close_trade_partial(
        trade: Trade, sell_bid: float, fraction: float, reason: str, rstr: str
    ) -> Tuple[Decimal, Optional[Trade]]:
        nonlocal bankroll
        if fraction <= 0.0 or fraction > 1.0:
            return _ZERO, trade
        shares_sell = _dq(trade.shares * _d(fraction))
        shares_keep = _dq(trade.shares - shares_sell)
        if shares_sell < _d("1e-6"):
            return _ZERO, trade
        frac_d = _d(fraction)
        partial_trade = Trade(
            side=trade.side, ask=trade.ask, bid_at_buy=trade.bid_at_buy,
            eff_c=trade.eff_c, shares=shares_sell, target=trade.target,
            type=trade.type, invested_pure=_dq(trade.invested_pure * frac_d),
            fee_buy=_dq(trade.fee_buy * frac_d),
            total_out=_dq(trade.total_out * frac_d),
            token_id=trade.token_id, partial_tp_done=trade.partial_tp_done,
            filled=trade.filled, order_uuid=trade.order_uuid,
        )
        pnl = close_trade(partial_trade, sell_bid, reason, rstr)
        if shares_keep > _d("1e-6"):
            remain = Trade(
                side=trade.side, ask=trade.ask, bid_at_buy=trade.bid_at_buy,
                eff_c=trade.eff_c, shares=shares_keep, target=trade.target,
                type=trade.type,
                invested_pure=_dq(trade.invested_pure * (_ONE - frac_d)),
                fee_buy=_dq(trade.fee_buy * (_ONE - frac_d)),
                total_out=_dq(trade.total_out * (_ONE - frac_d)),
                token_id=trade.token_id, partial_tp_done=True,
                filled=trade.filled, order_uuid=trade.order_uuid,
            )
            return pnl, remain
        return pnl, None

    def close_trade_resolution(trade: Trade, winner: bool, rstr: str) -> Decimal:
        nonlocal bankroll
        payout_net = resolution_payout(trade.shares, winner)
        pnl = _dq(payout_net - trade.total_out)
        pnl_pct = _safe_float(pnl / trade.total_out * 100) if trade.total_out \
            else 0.0
        bankroll += payout_net
        reason_s = "WIN ($1/share)" if winner else "LOSS (total)"
        price_s = "100.0c" if winner else "0.0c"
        log_m(
            trade.type, "SELL",
            f"rem={rstr} | {trade.side} @ {price_s} | "
            f"net={fmt_dollar(payout_net)} | {fmt_pnl(pnl, pnl_pct)} | "
            f"Reason: {reason_s}",
        )
        return pnl

    async def open_trade(
        side: str, trade_type: str, rstr: str, risk: float,
        extra_log: Optional[str] = None,
        fixed_shares: Optional[float] = None,
        token_id: Optional[str] = None,
    ) -> Optional[Trade]:
        nonlocal bankroll
        if safety.check(ctx):
            log_warn(
                f"[open_trade] Trading DISABLED ({ctx.session_stop_reason}) -- "
                f"skip {side}"
            )
            return None
        if len(active_trades) >= cfg.max_active_trades:
            log_warn(f"[open_trade] MAX_ACTIVE_TRADES -- skip {side}")
            return None
        ask = ctx.best_asks.get(side.lower())
        bid = ctx.best_bids.get(side.lower())
        if ask is None or ask <= 0.0:
            return None
        l2 = ctx.l2_up if side.lower() == "up" else ctx.l2_down
        if l2.is_stale(cfg.stale_data_threshold_s):
            log_warn(f"[open_trade] STALE L2 data for {side} -- skip")
            return None
        ask_f: float = ask
        tid = token_id or ""
        fr = fee_rate_lut(tid, ctx.fee_cache, cfg.default_taker_fee_bps)
        _sizing_bankroll = bankroll
        if cfg.live_trading:
            if ctx.last_reconciled_bankroll > _ZERO:
                _sizing_bankroll = min(bankroll, ctx.last_reconciled_bankroll)
            _locked = sum(ps.locked_capital for ps in ctx.pending_settlements)
            if _locked > _ZERO:
                _sizing_bankroll = max(_ZERO, _sizing_bankroll - _locked)
        current_exposure = sum(t.total_out for t in active_trades)
        max_exp = _sizing_bankroll * _d(cfg.max_market_exposure)
        max_exp = min(max_exp, _sizing_bankroll * _d(cfg.max_bankroll_exposure))
        if current_exposure >= max_exp - _d("1e-9"):
            return None
        if fixed_shares is not None:
            shares_d = _dq(_d(fixed_shares))
            invested_pure = _dq(shares_d * _d(ask_f))
        else:
            # v9.5.2: Boost risk by 25% when edge is very strong (>0.25), cap at 12%
            _ot_p_hat = p_hat_up if side == "UP" else p_hat_down
            _ot_edge  = max(0.0, _ot_p_hat - ask_f)
            if _ot_edge > 0.25:
                risk = min(risk * 1.25, 0.12)
            base_risk_amount = _sizing_bankroll * _d(risk)
            recovery_stake = tsm.calc_next_stake(
                base_risk_amount, ask_f, tid,
                lambda t: fee_rate_lut(t, ctx.fee_cache,
                                       cfg.default_taker_fee_bps),
                _sizing_bankroll,
                edge=_ot_edge,
                kelly_assumed_edge=cfg.kelly_assumed_edge,
                mart_recovery_factor=cfg.mart_recovery_factor,
                kelly_mart_boost=cfg.kelly_mart_boost,
            )
            invested_pure = _dq(min(recovery_stake,
                                    _sizing_bankroll * _d(risk) *
                                    _d(mart_level)))
            shares_d = _dq(invested_pure / _d(ask_f)) if ask_f > 1e-9 else _ZERO
        max_per = _sizing_bankroll * _d(min(
            cfg.kelly_max_risk_pct * mart_level,
            cfg.kelly_max_risk_pct * cfg.mart_max_mult
        ))
        if invested_pure > max_per and fixed_shares is None:
            invested_pure = _dq(max_per)
            shares_d = _dq(invested_pure / _d(ask_f)) if ask_f > 1e-9 else _ZERO
        # Polymarket non-linear fee: fee = C x p x 0.25 x (p x (1-p))^2
        fee_buy = polymarket_fee_decimal(shares_d, ask_f)
        total_out = _dq(invested_pure + fee_buy)
        # v9.4.0 FEE_REAL: log real fee cost per entry
        _fee_real_pct = _safe_float(fee_buy / invested_pure) if invested_pure > _d("1e-9") else 0.0
        log_debug(
            f"[FEE_REAL] {side} | fee={fmt_dollar(fee_buy)} "
            f"total_cost={fmt_dollar(total_out)} | fee_pct={_fee_real_pct:.4f} "
            f"buffer={cfg.fee_buffer}"
        )
        if current_exposure + total_out > max_exp + _d("1e-6"):
            room = _dq(max_exp - current_exposure)
            if room <= _d("0.001"):
                return None
            total_out = room
            # Back-solve shares and fee from room budget
            # room = shares*ask + fee(shares,ask) => iterate once
            shares_est = _dq(room / _d(ask_f + polymarket_fee(1.0, ask_f)))
            fee_buy = polymarket_fee_decimal(shares_est, ask_f)
            invested_pure = _dq(shares_est * _d(ask_f))
            shares_d = shares_est
        if bankroll < total_out - _d("1e-9"):
            return None
        order_uuid = str(uuid.uuid4())
        trade = Trade(
            side=side, ask=ask_f, bid_at_buy=bid,
            eff_c=eff_price_c_f(ask_f, fr), shares=shares_d,
            target=None, type=trade_type,
            invested_pure=invested_pure, fee_buy=fee_buy, total_out=total_out,
            token_id=token_id, order_uuid=order_uuid,
        )
        if _safe_float(total_out) > cfg.max_position_size_usd:
            log_warn(
                f"[open_trade] Position ${_safe_float(total_out):.4f} > "
                f"max_position_size_usd=${cfg.max_position_size_usd:.2f} -- "
                f"skip {side}"
            )
            return None
        if cfg.live_trading and token_id:
            l2_now = ctx.l2_up if side.lower() == "up" else ctx.l2_down
            book_asks = l2_now.asks[:5]
            vwap = simulate_market_buy_l2(book_asks, float(shares_d)) if \
                book_asks else None
            if vwap is None or (vwap - ask_f) > cfg.slippage_tolerance:
                log_warn(
                    f"[EXEC_FAILED] slippage check | {side} ask={ask_f:.4f} "
                    f"vwap={'n/a' if vwap is None else f'{vwap:.4f}'} -- aborted"
                )
                return None
            try:
                ok = await execute_trade(ctx, token_id, "BUY",
                                         float(total_out), ask_f, order_uuid)
            except Exception as exc:
                log_warn(f"[EXEC_FAILED] {type(exc).__name__}: {exc} | {side}")
                raise
            if not ok:
                log_warn(f"[EXEC_FAILED] place order=False | {side}")
                return None
        elif cfg.dry_run and ctx.shadow_engine is not None:
            # ── Shadow Trading: validate fill against LIVE L2 book ────────
            _sf = await ctx.shadow_engine.try_fill(
                side, float(shares_d), ask_f, ctx
            )
            if not _sf.filled:
                log_info(
                    f"[SHADOW] REJECT {side} | {_sf.reject_reason} | "
                    f"lat={_sf.latency_ms:.0f}ms | ask={fc(ask_f)}"
                )
                return None
            # Fill confirmed -- use VWAP as actual execution price
            _fill_price = _sf.fill_price
            if abs(_fill_price - ask_f) > 1e-6:
                # Recalculate with real fill price
                invested_pure = _dq(shares_d * _d(_fill_price))
                fee_buy = polymarket_fee_decimal(shares_d, _fill_price)
                total_out = _dq(invested_pure + fee_buy)
                trade = Trade(
                    side=side, ask=_fill_price, bid_at_buy=bid,
                    eff_c=eff_price_c_f(_fill_price, fr), shares=shares_d,
                    target=None, type=trade_type,
                    invested_pure=invested_pure, fee_buy=fee_buy,
                    total_out=total_out, token_id=token_id,
                    order_uuid=order_uuid,
                )
                ask_f = _fill_price  # update for log below
            log_info(
                f"[SHADOW] FILL {side} @ {fc(_fill_price)} | "
                f"slip={_sf.slippage_pct:.3%} | lat={_sf.latency_ms:.0f}ms | "
                f"shares={float(shares_d):.4f} | "
                f"depth_ok={_sf.shares_filled:.4f}"
            )
        bankroll -= total_out
        if bankroll < _ZERO:
            log_warn(f"[SAFETY] Bankroll went negative ({bankroll}) -- clamping "
                     f"to 0")
            bankroll = _ZERO
        active_trades.append(trade)
        tsm.active_trades = list(active_trades)
        p_up, p_dn = bayesian.get_posteriors()
        p_hat = p_up if side == "UP" else p_dn
        ev = p_hat - ask_f
        bid_s = f" | BID@buy={fc(bid)}" if bid else ""
        ext_s = f" | {extra_log}" if extra_log else ""
        log_m(
            trade_type, "BUY",
            f"rem={rstr} | {side} @ ASK={fc(ask_f)} "
            f"eff={fc(eff_price_c_f(ask_f, fr) / 100)}{bid_s} | "
            f"invested=${_safe_float(invested_pure):.6f} | "
            f"fee=${_safe_float(fee_buy):.6f} ({_safe_float(fee_buy)/_safe_float(invested_pure)*100:.2f}%) | "
            f"total=${_safe_float(total_out):.6f} | shares={shares_d} | "
            f"risk={risk:.1%}{ext_s} | EV={ev:+.4f} | p_hat={p_hat:.3f} | "
            f"uuid={order_uuid[:8]}",
        )
        # v9.4.0: audit.log_trade() -> JSONL only, no console duplication
        audit.log_trade(
            strategy=trade_type, action="BUY", symbol=f"{side}@{ask_f:.4f}",
            price=ask_f, mart_level=mart_level,
            pnl_round=_ZERO, pnl_day=tsm.state.daily_pnl, fee=fee_buy,
            extra=extra_log or "",
        )
        return trade

    kalmans: Dict[str, KalmanFilter1D] = {
        "UP": KalmanFilter1D(q=cfg.kalman_process_noise,
                             r=cfg.kalman_measure_noise),
        "DOWN": KalmanFilter1D(q=cfg.kalman_process_noise,
                               r=cfg.kalman_measure_noise),
    }
    hft_wins: Dict[str, HFTWindow] = {
        "UP": HFTWindow(ws=cfg.hft_window_seconds),
        "DOWN": HFTWindow(ws=cfg.hft_window_seconds),
    }
    gamb_last_buy: Dict[str, float] = {"UP": 0.0, "DOWN": 0.0}
    gamb_started_logged: bool = False
    endgame_fired: bool = False
    _last_peg_arb_ts: float = 0.0
    prev_bid_up: Optional[float] = None
    prev_bid_down: Optional[float] = None
    # Change-only state log: snapshot of last emitted state values
    _prev_state_key: str = ""   # only emit log_raw when this key changes
    partial_tp_count: int = 0
    partial_tp_success: int = 0
    last_tp_check_ts: float = 0.0
    _loser_posterior: float = 0.5

    while not ctx.shutdown_flag:
        now = time.time()
        rem = timer.remaining
        if rem <= 0.005 and not ctx._final_log_done:
            bid_up_f   = ctx.best_bids.get("up")   or 0.0
            bid_down_f = ctx.best_bids.get("down") or 0.0
            ask_up_f   = ctx.best_asks.get("up")   or 0.0
            ask_down_f = ctx.best_asks.get("down") or 0.0
            log_info(f"rem=00:00:005 | UP BID={fc(bid_up_f)} ASK={fc(ask_up_f)} | DN BID={fc(bid_down_f)} ASK={fc(ask_down_f)}")
            ctx._final_log_done = True
        if rem <= 0.001:
            ask_up_f   = ctx.best_asks.get("up")   or 0.0
            ask_down_f = ctx.best_asks.get("down") or 0.0
            winner = "UP" if ask_up_f > ask_down_f else "DOWN"
            _get_logger().info(
                "[INFO] [WINNER] [%s] | %s | ask_up=%s ask_dn=%s",
                _ts(), winner, fc(ask_up_f), fc(ask_down_f)
            )
        if timer.is_expired:
            # v9.4.0 PnL FIX: pass cycle_realized_pnl (from intra-cycle sells)
            # into process_pre_settlement so ROUND PnL correctly includes
            # GAMBLING/PEG trades that were already closed before market end.
            bankroll, had_pending, round_pnl_this_cycle = await process_pre_settlement(
                ctx, tsm, active_trades, bankroll, cfg, meta, audit,
                cycle_start_bankroll=cycle_start_bankroll,
                pre_realized_pnl=cycle_realized_pnl,
            )
            _tp_pct = (partial_tp_success / partial_tp_count * 100.0
                       if partial_tp_count > 0 else 0.0)
            log_info(f"Partial TP fired {partial_tp_count}x "
                     f"({_tp_pct:.0f}% success)")
            # v9.5.0: Vol Hedge end-of-cycle stats
            if _vh_engine is not None:
                # Mark remaining YES_OPEN positions as resolved (let close)
                for _vh_p in list(_vh_engine.active_positions):
                    if _vh_p.state == VolHedgeState.YES_OPEN:
                        _vh_engine.mark_abandoned(_vh_p)
                    _vh_engine.mark_resolved(_vh_p)
                log_info(f"[VOL_HEDGE] Cycle end | {_vh_engine.stats}")
            log_sep()
            p_up, p_dn = bayesian.get_posteriors()
            final_ask_up   = ctx.best_asks.get("up")   or 0.0
            final_ask_down = ctx.best_asks.get("down") or 0.0
            _loser_posterior = p_dn if final_ask_up >= final_ask_down else p_up
            return bankroll, _loser_posterior, had_pending, round_pnl_this_cycle

        event = await ctx.event_bus.consume(timeout=0.2)
        if event is None:
            continue
        if event.type == EventType.MARKET_RESOLVED:
            continue
        if event.type == EventType.SHUTDOWN:
            break
        if event.type not in (EventType.PRICE_UPDATE, EventType.CYCLE_TICK,
                              EventType.BOOK_SNAPSHOT):
            continue

        # ── v9.5.2: HARD STOP-LOSS PER GAMBLING TRADE (-15% max) ────────
        for _sl_t in list(active_trades):
            if _sl_t.type != "GAMBLING":
                continue
            _sl_bid = ctx.best_bids.get(_sl_t.side.lower())
            if _sl_bid is None or _sl_bid <= 0:
                continue
            _sl_loss_pct = (_sl_bid - _sl_t.ask) / _sl_t.ask if _sl_t.ask > 1e-9 else 0.0
            if _sl_loss_pct <= -cfg.max_loss_per_trade_pct:
                log_warn(
                    f"[SAFETY] STOP-LOSS HIT | {_sl_t.side} "
                    f"loss={_sl_loss_pct * 100:.1f}% "
                    f"(limit=-{cfg.max_loss_per_trade_pct * 100:.0f}%) | "
                    f"ask_entry={fc(_sl_t.ask)} bid_now={fc(_sl_bid)} | "
                    f"shares={_sl_t.shares}"
                )
                active_trades.remove(_sl_t)
                tsm.active_trades = list(active_trades)
                close_trade(
                    _sl_t, float(_sl_bid),
                    reason=f"STOP_LOSS_{cfg.max_loss_per_trade_pct * 100:.0f}%",
                    rstr=timer.remaining_str(),
                )


        bid_up   = ctx.best_bids.get("up")
        bid_down = ctx.best_bids.get("down")
        ask_up   = ctx.best_asks.get("up")
        ask_down = ctx.best_asks.get("down")
        if None in (bid_up, bid_down, ask_up, ask_down):
            continue
        if bid_up == prev_bid_up and bid_down == prev_bid_down:
            continue
        prev_bid_up, prev_bid_down = bid_up, bid_down
        bid_up_f: float   = bid_up
        bid_down_f: float = bid_down
        ask_up_f: float   = ask_up
        ask_down_f: float = ask_down
        snap_bs_up: float   = ctx.best_bid_sizes.get("up")   or 0.0
        snap_bs_down: float = ctx.best_bid_sizes.get("down") or 0.0
        snap_as_up: float   = ctx.best_ask_sizes.get("up")   or 0.0
        snap_as_down: float = ctx.best_ask_sizes.get("down") or 0.0
        ask_sum   = ask_up_f + ask_down_f
        mid_up    = (bid_up_f + ask_up_f) * 0.5
        mid_down  = (bid_down_f + ask_down_f) * 0.5
        ask_up_c  = ask_up_f   * 100.0
        ask_down_c = ask_down_f * 100.0
        rstr = timer.remaining_str()
        regime = binance.get_vol_regime(cfg)
        eff_mart = mart_level

        kal_up   = kalmans["UP"].update(mid_up)
        kal_down = kalmans["DOWN"].update(mid_down)
        hft_wins["UP"].add(kal_up, now)
        hft_wins["DOWN"].add(kal_down, now)
        z_up   = hft_wins["UP"].zscore(kal_up)
        z_down = hft_wins["DOWN"].zscore(kal_down)
        micro_up   = micro_analyzer.analyze(ctx.l2_up, ctx.l2_down, "up")
        micro_down = micro_analyzer.analyze(ctx.l2_up, ctx.l2_down, "down")
        obi_up   = micro_up.depth_imbalance
        obi_down = micro_down.depth_imbalance
        p_hat_up, p_hat_down = bayesian.update(
            kal_up, kal_down, obi_up, obi_down, None, None, micro_up, micro_down
        )
        _bnc_active = (
            not binance.is_stale(10.0)
            and binance.current_price is not None
            and binance.cycle_open_price is not None
        )
        if _bnc_active:
            _bnc_p = calculate_true_prob(
                binance.current_price, binance.cycle_open_price,
                timer.remaining, binance, cfg,
            )
            if _bnc_p is not None:
                w = cfg.binance_blend_weight
                _up_b  = w * _bnc_p         + (1.0 - w) * p_hat_up
                _down_b = w * (1.0 - _bnc_p) + (1.0 - w) * p_hat_down
                _s = _up_b + _down_b
                if _s > 1e-9:
                    p_hat_up   = max(0.01, min(0.99, _up_b  / _s))
                    p_hat_down = max(0.01, min(0.99, _down_b / _s))
            # v9.4.2 BUG FIX: direction bias check a cada update Binance (throttled 5s)
            if now - getattr(check_direction_bias, '_last_ts', 0.0) >= 5.0:
                check_direction_bias(
                    binance.current_price, binance.cycle_open_price, p_hat_up,
                )
                check_direction_bias._last_ts = now

        _spike_detected = micro_up.is_volatile or micro_down.is_volatile
        _z_u  = f"{z_up:+.2f}"  if z_up   is not None else "n/a"
        _z_d  = f"{z_down:+.2f}" if z_down is not None else "n/a"
        _bnc_s = f"BNC={binance.current_price:.5f}" if binance.current_price \
            else "BNC=n/a"
        _fr_s  = f"FR={funding.signal_str}" if not funding.is_stale() else \
            "FR=stale"
        # Main state log: emit ONLY when a value actually changes (spec requirement)
        _state_key = (
            f"{bid_up_f:.3f}{ask_up_f:.3f}{bid_down_f:.3f}{ask_down_f:.3f}"
            f"{p_hat_up:.3f}{p_hat_down:.3f}{regime.value}{_spike_detected}"
            f"{round(ask_sum, 4)}"
        )
        if _state_key != _prev_state_key:
            _prev_state_key = _state_key
            _spr_up = ctx.best_spreads_c.get("up")
            _spr_dn = ctx.best_spreads_c.get("down")
            _spr_up_s = f"{_spr_up:.1f}c" if _spr_up is not None else "n/a"
            _spr_dn_s = f"{_spr_dn:.1f}c" if _spr_dn is not None else "n/a"
            log_raw(
                f"rem={rstr} | UP BID={fc(bid_up_f)} ASK={fc(ask_up_f)} Z={_z_u} "
                f"OBI={obi_up:.2f} | DN BID={fc(bid_down_f)} ASK={fc(ask_down_f)} "
                f"Z={_z_d} OBI={obi_down:.2f} | "
                f"UP_SPR={_spr_up_s} DN_SPR={_spr_dn_s} | "
                f"P(UP)={p_hat_up:.3f} "
                f"P(DN)={p_hat_down:.3f} | {_bnc_s} | {_fr_s} | PEG={ask_sum:.4f} | "
                f"REGIME={regime.value} | SPIKE={_spike_detected}"
            )

        
        _gamb_min_ask_c_temp = cfg.gamb_min_ask_c

        ctx._last_p_hat_up = p_hat_up

        # ══════════════════════════════════════════════════════════════════
        # ── VOL HEDGE 1SD-3SD (v9.5.0 -- dynamic pricing + dual-verify) ──
        # ══════════════════════════════════════════════════════════════════
        if _vh_engine is not None and _bnc_active and bankroll > _ZERO and \
                not safety.check(ctx):

            # ── Step 1: Feed Binance price to SD calculator ─────────────
            if now - _vh_last_feed_ts >= 0.5:  # feed every 500ms
                _vh_engine.feed_price(binance.current_price)
                _vh_last_feed_ts = now

            _vh_sd = _vh_engine.current_sd_price
            _vh_k = binance.cycle_open_price
            _vh_price = binance.current_price

            # ── Step 2: Check ABANDON (<60s to close, 3SD not hit) ──────
            _vh_abandon_list = _vh_engine.check_abandon(timer, cfg)
            for _vh_apos in _vh_abandon_list:
                _vh_engine.mark_abandoned(_vh_apos)
                log_warn(
                    f"[VOL_HEDGE] ABANDON | {_vh_apos.direction} | "
                    f"rem={rstr} < {cfg.vol_hedge_abandon_s:.0f}s | "
                    f"3SD not reached → let YES resolve at close | "
                    f"k={_vh_apos.k_reference:.5f} sd_entry={_vh_apos.sd_at_entry:.6f}"
                )

            # ── Step 3: Check 3SD REACHED → dual-verify NO fill ─────────
            _vh_3sd_list = _vh_engine.check_3sd_reached(binance, cfg)
            for _vh_3pos in _vh_3sd_list:
                _vh_no_side = "DOWN" if _vh_3pos.direction == "UP" else "UP"
                _vh_no_tid = meta["down"] if _vh_3pos.direction == "UP" else meta["up"]

                # v9.5.0: N = exact shares from YES trade
                _vh_yes_shares = float(_vh_3pos.yes_trade.shares) if \
                    _vh_3pos.yes_trade else 0.0
                _vh_yes_total_out = float(_vh_3pos.yes_trade.total_out) if \
                    _vh_3pos.yes_trade else 0.0

                if _vh_yes_shares < 0.01:
                    continue

                # v9.5.0: Dynamic limit price from position (already computed at entry)
                _vh_no_limit = _vh_3pos.no_limit_price

                # v9.5.0: Verify liquidity with ±2% slippage on NO side
                _vh_liq_ok, _vh_liq_vol = _vh_engine.check_no_side_liquidity(
                    _vh_3pos.direction, ctx, cfg, limit_price=_vh_no_limit,
                )

                if not _vh_liq_ok:
                    log_warn(
                        f"[VOL_HEDGE] 3SD NO FILL SKIPPED | "
                        f"insufficient liquidity ±2% | "
                        f"need={cfg.vol_hedge_liquidity_min:.1f} "
                        f"have={_vh_liq_vol:.1f} | limit={_vh_no_limit:.4f}"
                    )
                    continue

                # v9.5.0: DUAL-VERIFY via ShadowFillEngine (dry_run) or live
                if cfg.dry_run and ctx.shadow_engine is not None:
                    _vh_sf = await ctx.shadow_engine.try_fill_limit_no(
                        side=_vh_no_side,
                        shares=_vh_yes_shares,
                        limit_price=_vh_no_limit,
                        ctx=ctx,
                        max_total_cost_pct=0.93,
                        yes_total_out=_vh_yes_total_out,
                    )
                    if not _vh_sf.filled:
                        log_warn(
                            f"[VOL_HEDGE] 3SD NO DUAL-VERIFY REJECTED | "
                            f"{_vh_no_side} | {_vh_sf.reject_reason} | "
                            f"limit={_vh_no_limit:.4f} | "
                            f"shares={_vh_yes_shares:.4f}"
                        )
                        continue
                    _vh_fill_price = _vh_sf.fill_price
                    log_info(
                        f"[VOL_HEDGE] 3SD NO DUAL-VERIFY PASSED | "
                        f"{_vh_no_side} @ {fc(_vh_fill_price)} | "
                        f"slip={_vh_sf.slippage_pct:.3%} | "
                        f"shares={_vh_sf.shares_filled:.4f}"
                    )
                else:
                    _vh_fill_price = _vh_no_limit

                # Open the NO trade
                _vh_no_trade = await open_trade(
                    _vh_no_side, "VOL_HEDGE_NO", rstr,
                    risk=cfg.vol_hedge_max_risk_pct * 0.3,
                    fixed_shares=_vh_yes_shares,
                    token_id=_vh_no_tid,
                    extra_log=(
                        f"3SD REACHED | hedge NO {_vh_no_side} | "
                        f"BNC={_vh_price:.5f} | "
                        f"k={_vh_k:.5f} ± 3SD={_vh_sd * cfg.vol_hedge_3sd_target:.6f} | "
                        f"NO_limit_dynamic={_vh_no_limit:.4f} | "
                        f"YES_shares={_vh_yes_shares:.4f} | "
                        f"YES_cost=${_vh_yes_total_out:.6f} | "
                        f"liq_vol={_vh_liq_vol:.1f}"
                    ),
                )
                if _vh_no_trade is not None:
                    _vh_engine.mark_hedge_filled(_vh_3pos, _vh_no_trade)
                    _vh_no_cost = float(_vh_no_trade.total_out)
                    _vh_total_cost = _vh_yes_total_out + _vh_no_cost
                    _vh_payout = 1.0 * _vh_yes_shares
                    _vh_locked_profit = _vh_payout - _vh_total_cost
                    _vh_locked_pnl_pct = (_vh_locked_profit / _vh_total_cost * 100.0) \
                        if _vh_total_cost > 1e-9 else 0.0
                    log_info(
                        f"[VOL_HEDGE] PROFIT LOCKED | {_vh_3pos.direction} | "
                        f"YES_cost=${_vh_yes_total_out:.6f} + "
                        f"NO_cost=${_vh_no_cost:.6f} = "
                        f"${_vh_total_cost:.6f} | "
                        f"payout=${_vh_payout:.6f} | "
                        f"cost/share=${_vh_total_cost / _vh_yes_shares:.4f} | "
                        f"{fmt_pnl(_d(_vh_locked_profit), _vh_locked_pnl_pct)}"
                    )
                else:
                    log_warn(
                        f"[VOL_HEDGE] 3SD NO FILL FAILED | "
                        f"{_vh_no_side} | open_trade returned None | "
                        f"liq={_vh_liq_vol:.1f}"
                    )

            # ── Step 4: Check 2SD TRIGGER → open YES + compute dynamic NO price
            # v9.5.4: Blocked if rem <= vol_hedge_cutoff_s (50s)
            _vh_trigger = None
            if timer.remaining > cfg.vol_hedge_cutoff_s:
                _vh_trigger = _vh_engine.check_1sd_trigger(binance, cfg)
            if _vh_trigger is not None and _vh_sd >= cfg.vol_hedge_min_sd:
                _vh_trig_side = _vh_trigger
                _vh_trig_tid = meta["up"] if _vh_trig_side == "UP" else meta["down"]

                _vh_1sd_thresh = _vh_k + cfg.vol_hedge_1sd_trigger * _vh_sd if \
                    _vh_trig_side == "UP" else \
                    _vh_k - cfg.vol_hedge_1sd_trigger * _vh_sd
                _vh_3sd_thresh = _vh_k + cfg.vol_hedge_3sd_target * _vh_sd if \
                    _vh_trig_side == "UP" else \
                    _vh_k - cfg.vol_hedge_3sd_target * _vh_sd

                # v9.5.0: Pre-entry NO limit estimate (refined after YES fill)
                _vh_no_lim_est = _vh_engine.compute_no_limit_price(cfg)

                # Pre-check liquidity with estimate
                _vh_pre_liq_ok, _vh_pre_liq_vol = _vh_engine.check_no_side_liquidity(
                    _vh_trig_side, ctx, cfg, limit_price=_vh_no_lim_est,
                )

                _vh_ask = ctx.best_asks.get(_vh_trig_side.lower())
                _vh_p_hat = p_hat_up if _vh_trig_side == "UP" else p_hat_down
                _vh_ev = _vh_p_hat - (_vh_ask or 0.0)

                log_info(
                    f"[VOL_HEDGE] 1SD TRIGGER | {_vh_trig_side} | "
                    f"BNC={_vh_price:.5f} crossed "
                    f"{'k+1SD' if _vh_trig_side == 'UP' else 'k-1SD'}"
                    f"={_vh_1sd_thresh:.5f} | "
                    f"k={_vh_k:.5f} SD={_vh_sd:.6f} | "
                    f"3SD_target={_vh_3sd_thresh:.5f} | "
                    f"NO_limit_est={_vh_no_lim_est:.4f} | "
                    f"EV={_vh_ev:+.4f} | liq_ok={_vh_pre_liq_ok} "
                    f"vol={_vh_pre_liq_vol:.1f} | rem={rstr}"
                )

                if _vh_ask is not None and _vh_ev > 0.0 and _vh_pre_liq_ok:
                    # Open YES position
                    _vh_yes_trade = await open_trade(
                        _vh_trig_side, "VOL_HEDGE_YES", rstr,
                        risk=cfg.vol_hedge_max_risk_pct,
                        token_id=_vh_trig_tid,
                        extra_log=(
                            f"1SD ENTRY | BNC={_vh_price:.5f} | "
                            f"k={_vh_k:.5f} ± 1SD={_vh_sd:.6f} | "
                            f"3SD_target={_vh_3sd_thresh:.5f} | "
                            f"EV={_vh_ev:+.4f} | p_hat={_vh_p_hat:.3f}"
                        ),
                    )
                    if _vh_yes_trade is not None:
                        # v9.5.0: Compute DYNAMIC NO limit price from actual fill
                        _vh_no_lim_dynamic = _vh_engine.compute_no_limit_price(
                            cfg, yes_trade=_vh_yes_trade,
                        )
                        _vh_yes_n = float(_vh_yes_trade.shares)
                        _vh_yes_cost_ps = float(_vh_yes_trade.total_out) / _vh_yes_n \
                            if _vh_yes_n > 1e-9 else 0.0

                        log_info(
                            f"[VOL_HEDGE] NO LIMIT DYNAMIC | "
                            f"max_hedge_price = 0.90 - "
                            f"(${float(_vh_yes_trade.total_out):.6f} / "
                            f"{_vh_yes_n:.4f}) = 0.90 - {_vh_yes_cost_ps:.4f} = "
                            f"{_vh_no_lim_dynamic:.4f} | "
                            f"shares={_vh_yes_n:.4f}"
                        )

                        _vh_pos = _vh_engine.register_entry(
                            direction=_vh_trig_side,
                            trade=_vh_yes_trade,
                            k=_vh_k,
                            sd=_vh_sd,
                            no_limit_price=_vh_no_lim_dynamic,
                        )
                        _vh_pos.no_limit_placed = True

                        # Place the NO limit order (live) or simulate (dry_run)
                        _vh_no_side_name = "DOWN" if _vh_trig_side == "UP" else "UP"
                        _vh_no_tid_place = meta["down"] if _vh_trig_side == "UP" \
                            else meta["up"]

                        if cfg.live_trading and _vh_no_tid_place:
                            _vh_no_uuid = str(uuid.uuid4())
                            _vh_no_amount = _vh_yes_n * _vh_no_lim_dynamic
                            try:
                                await execute_trade(
                                    ctx, _vh_no_tid_place, "BUY",
                                    _vh_no_amount, _vh_no_lim_dynamic,
                                    _vh_no_uuid, use_limit=True,
                                )
                                log_info(
                                    f"[VOL_HEDGE] NO LIMIT PLACED | "
                                    f"{_vh_no_side_name} @ {_vh_no_lim_dynamic:.4f} | "
                                    f"shares={_vh_yes_n:.4f} | "
                                    f"amount=${_vh_no_amount:.6f} | "
                                    f"uuid={_vh_no_uuid[:8]}"
                                )
                            except Exception as _vh_exc:
                                log_warn(
                                    f"[VOL_HEDGE] NO LIMIT FAILED | "
                                    f"{type(_vh_exc).__name__}: {_vh_exc}"
                                )
                        else:
                            log_info(
                                f"[VOL_HEDGE] NO LIMIT SIMULATED | "
                                f"{_vh_no_side_name} @ {_vh_no_lim_dynamic:.4f} | "
                                f"shares={_vh_yes_n:.4f} | "
                                f"will dual-verify at 3SD ({_vh_3sd_thresh:.5f})"
                            )
                elif _vh_ev <= 0.0:
                    log_info(
                        f"[VOL_HEDGE] 1SD BLOCKED reason=ev_negative | "
                        f"EV={_vh_ev:.4f} | side={_vh_trig_side} | rem={rstr}"
                    )
                elif not _vh_pre_liq_ok:
                    log_info(
                        f"[VOL_HEDGE] 1SD BLOCKED reason=no_liquidity_NO_side | "
                        f"need={cfg.vol_hedge_liquidity_min:.1f} "
                        f"have={_vh_pre_liq_vol:.1f} | side={_vh_trig_side} | rem={rstr}"
                    )

        # ══════════════════════════════════════════════════════════════════

        # ── HEDGE DINÂMICO: flip contra direção errada (a cada tick) ─────
        # v9.5.4: Blocked if rem <= hedge_cutoff_s (50s)
        if active_trades and _bnc_active and \
                timer.remaining > cfg.hedge_cutoff_s:
            _hedge_action = check_adverse_hedge(
                active_trades, ctx, binance,
                p_hat_up, p_hat_down, cfg,
            )
            if _hedge_action is not None:
                _h_trade = _hedge_action["trade"]
                _h_side_lose = _hedge_action["side_losing"]
                _h_side_hedge = _hedge_action["side_hedge"]
                _h_loss_c = _hedge_action["loss_cents"]
                _h_bid = _hedge_action["bid_sell"]
                _h_ask_opp = _hedge_action["ask_hedge"]
                _h_p_opp = _hedge_action["p_opposite"]
                _h_trend = _hedge_action["trend"]
                _h_confirms = _hedge_action.get("confirms", 0)
                _h_velocity = _hedge_action.get("bnc_velocity", 0.0)

                log_warn(
                    f"HEDGE_FLIP | {_h_side_lose} loss={_h_loss_c:.1f}c | "
                    f"confirms={_h_confirms}/{cfg.hedge_flip_confirms_needed} | "
                    f"→ SELL {_h_side_lose} + BUY {_h_side_hedge} @{fc(_h_ask_opp)} | "
                    f"P({_h_side_hedge})={_h_p_opp:.3f} | "
                    f"trend={_h_trend} | velocity={_h_velocity:.5f}/s | "
                    f"BNC={_hedge_action['bnc']:.5f} K={_hedge_action['k']:.5f}"
                )

                # 1. Sell losing side
                active_trades.remove(_h_trade)
                tsm.active_trades = list(active_trades)
                close_trade(
                    _h_trade, _h_bid,
                    reason=f"HEDGE_FLIP | loss={_h_loss_c:.1f}c | "
                           f"P({_h_side_hedge})={_h_p_opp:.3f}",
                    rstr=rstr,
                )

                # 2. Buy opposite side
                _h_tid = meta["up"] if _h_side_hedge == "UP" else meta["down"]
                await open_trade(
                    _h_side_hedge, "GAMBLING", rstr,
                    risk=cfg.hedge_max_risk_pct,
                    token_id=_h_tid,
                    extra_log=(
                        f"HEDGE_BUY | flipped from {_h_side_lose} | "
                        f"P({_h_side_hedge})={_h_p_opp:.3f} | "
                        f"trend={_h_trend}"
                    ),
                )

        if cfg.aggressive_endgame_active and timer.is_endgame() and \
                not endgame_fired and bankroll > _ZERO and \
                not _spike_detected and not safety.check(ctx):
            _candidates: List[Tuple[str, str, float, Optional[float]]] = []
            for _s, _t, _a, _z in [
                ("UP",   meta["up"],   ask_up_f,   z_up),
                ("DOWN", meta["down"], ask_down_f, z_down),
            ]:
                if _a < cfg.min_prob_entry:
                    continue
                _p = p_hat_up if _s == "UP" else p_hat_down
                if _p - _a < cfg.min_vwap_edge:
                    continue
                if cfg.aggressive_endgame_min_c - 1e-6 <= _a <= \
                        cfg.aggressive_endgame_max_c + 1e-6:
                    _candidates.append((_s, _t, _a, _z))
            if not _candidates:
                log_endgame(f"SKIP reason=no_candidates | rem={rstr}")
            else:
                endgame_fired = True
                _ultra = _is_ultra_bull(binance, funding, cfg)
                if _ultra:
                    _eg_side, _eg_tid = "UP", meta["up"]
                    _eg_risk = min(cfg.endgame_high_z_risk * eff_mart,
                                   cfg.kelly_max_risk_pct * cfg.mart_max_mult)
                    _eg_label = "ULTRA_BULL"
                else:
                    _eg_side, _eg_tid, _, _eg_z = max(_candidates,
                                                      key=lambda x: x[2])
                    _z_abs = abs(_eg_z) if _eg_z is not None else 0.0
                    if _z_abs > cfg.endgame_high_z_thresh:
                        _eg_risk = min(cfg.endgame_high_z_risk * eff_mart,
                                       cfg.kelly_max_risk_pct *
                                       cfg.mart_max_mult)
                        _eg_label = f"HIGH_Z={_z_abs:.2f}"
                    else:
                        _eg_risk = min(cfg.aggressive_endgame_risk * eff_mart,
                                       cfg.kelly_max_risk_pct * eff_mart)
                        _eg_label = f"Z={_eg_z:+.2f}" if _eg_z is not None \
                            else "Z=n/a"
                # v9.4.0: cap at max risk
                _eg_risk = min(_eg_risk,
                               cfg.kelly_max_risk_pct * cfg.mart_max_mult)
                # v9.4.2 BUG FIX: EV negativo = entrada bloqueada (endgame)
                _eg_ask = ask_up_f if _eg_side == "UP" else ask_down_f
                _eg_p = p_hat_up if _eg_side == "UP" else p_hat_down
                _ev_eg = _eg_p - _eg_ask
                if not should_enter_trade({'EV': _ev_eg}):
                    log_endgame(
                        f"BLOCKED reason=ev_negative | EV={_ev_eg:.4f} | "
                        f"side={_eg_side} | rem={rstr}"
                    )
                else:
                    # v9.4.2 BUG FIX: direction bias check antes do BUY endgame
                    _eg_bias_ok = True
                    if _bnc_active:
                        _eg_bias = check_direction_bias(
                            binance.current_price, binance.cycle_open_price,
                            p_hat_up,
                        )
                        if _eg_bias != "NEUTRAL" and _eg_bias != _eg_side:
                            log_endgame(
                                f"BLOCKED reason=direction_mismatch | "
                                f"side={_eg_side} bias={_eg_bias} | rem={rstr}"
                            )
                            _eg_bias_ok = False
                    if _eg_bias_ok:
                        await open_trade(
                            _eg_side, "ENDGAME_AGG", rstr, risk=_eg_risk,
                            token_id=_eg_tid,
                            extra_log=(
                                f"ENDGAME x{eff_mart} | {_eg_label} | "
                                f"regime={regime.value} | "
                                f"acc_loss={tsm.state.accumulated_loss_session:.4f} | "
                            ),
                        )


        # ── v9.5.3: MOONBAG TAKE PROFIT ─────────────────────────────────
        # Sell the MINIMUM shares to recover 100% of invested capital.
        # Remaining shares ("moonbag") ride risk-free to market close.
        # Only fires if recovery is possible selling ≤ 80% of position.
        if cfg.partial_tp_active and active_trades and \
                (now - last_tp_check_ts >= 2.0):
            last_tp_check_ts = now
            _tp_candidates = [
                t for t in list(active_trades)
                if t.type == "GAMBLING" and not t.partial_tp_done
            ]
            for _tp_t in _tp_candidates:
                _tp_bid = ctx.best_bids.get(_tp_t.side.lower())
                if _tp_bid is None or _tp_bid <= 0:
                    continue

                # Step 1: Check if bid is high enough for moonbag to work
                _tp_threshold = calculate_dynamic_tp(
                    _tp_t, ctx.fee_cache, cfg.partial_tp_target_net_roi,
                )
                if _tp_threshold >= 0.995:
                    continue
                if _tp_bid < _tp_threshold:
                    continue

                # Step 2: Calculate exact shares to sell for 100% capital recovery
                _mb_shares, _mb_frac, _mb_moonbag = calculate_moonbag_shares(
                    _tp_t, _tp_bid, cfg.partial_tp_fraction,
                )
                if _mb_shares is None:
                    continue  # cannot recover 100% within 80% ceiling

                # Step 3: Execute the moonbag TP sell
                active_trades.remove(_tp_t)
                _gain_pct = ((_tp_bid - _tp_t.ask) / _tp_t.ask * 100.0
                             if _tp_t.ask > 1e-9 else 0.0)
                log_info(
                    f"[MOONBAG_TP] TRIGGERED | {_tp_t.side} | "
                    f"bid={fc(_tp_bid)} | gain={_gain_pct:+.1f}% | "
                    f"selling {_mb_frac * 100:.1f}% ({_mb_shares:.4f} shares) "
                    f"to recover ${float(_tp_t.total_out):.6f} | "
                    f"moonbag={_mb_moonbag:.4f} shares RISK-FREE"
                )
                pnl_partial, remain = close_trade_partial(
                    _tp_t, _tp_bid, _mb_frac,
                    reason=(
                        f"MOONBAG_TP {_mb_frac * 100:.1f}% @ "
                        f"+{_gain_pct:.1f}% | "
                        f"recovered=${float(_tp_t.total_out):.4f} | "
                        f"moonbag={_mb_moonbag:.4f}sh"
                    ),
                    rstr=rstr,
                )
                if remain is not None:
                    active_trades.append(remain)
                    tsm.active_trades = list(active_trades)
                    log_info(
                        f"[MOONBAG_TP] MOONBAG OPEN | {_tp_t.side} | "
                        f"{float(remain.shares):.4f} shares @ entry={fc(_tp_t.ask)} | "
                        f"invested=$0.00 (risk-free) | "
                        f"potential=${float(remain.shares):.4f} if wins"
                    )
                partial_tp_count += 1
                if pnl_partial > _ZERO:
                    partial_tp_success += 1

        # ── PEG ARB ─────────────────────────────────────────────────────
        if cfg.peg_arb_active and bankroll > _ZERO and \
                not safety.check(ctx) and \
                (now - _last_peg_arb_ts) >= cfg.peg_cooldown_s:
            _peg_val = ask_up_f + ask_down_f
            if _peg_val < cfg.peg_trigger - 1e-6:
                _peg_budget = _safe_float(bankroll * _d(cfg.peg_budget_pct))
                _obs_up = OrderBookSide(
                    levels=[OrderBookLevel(p, s) for p, s in ctx.l2_up.asks[:10]]
                )
                _obs_dn = OrderBookSide(
                    levels=[OrderBookLevel(p, s) for p, s in ctx.l2_down.asks[:10]]
                )
                _arb = evaluate_arb(
                    _obs_up, _obs_dn, _peg_budget, cfg.peg_trigger,
                    meta["up"], meta["down"], ctx.fee_cache, cfg,
                )
                if _arb.status == ArbStatus.OPPORTUNITY and \
                        _arb.profit_pct >= cfg.peg_min_profit_pct:
                    # Pre-validate: compute REAL fees (Polymarket formula)
                    _fee_real_up = polymarket_fee(1.0, _arb.lowest_ask_up)
                    _fee_real_dn = polymarket_fee(1.0, _arb.lowest_ask_down)
                    _real_cost = _arb.shares * (
                        _arb.lowest_ask_up + _arb.lowest_ask_down +
                        _fee_real_up + _fee_real_dn
                    )
                    _real_payout = _arb.shares * cfg.arb_resolution
                    _real_net = _real_payout - _real_cost
                    _real_pct = (_real_net / _real_cost * 100.0) if _real_cost > 0 else 0.0
                    if _real_net <= 0.0 or _real_pct < cfg.peg_min_profit_pct:
                        log_peg(
                            f"ARB REJECTED (fees) | PEG={_peg_val:.4f} | "
                            f"real_net=${_real_net:.6f} ({_real_pct:.2f}%) | "
                            f"fee_up={_fee_real_up:.6f} fee_dn={_fee_real_dn:.6f} | rem={rstr}"
                        )
                    else:
                        _last_peg_arb_ts = now
                        _peg_up_trade = await open_trade(
                            "UP", "PEG_ARBIT", rstr,
                            risk=cfg.peg_budget_pct * 0.5,
                            fixed_shares=_arb.shares,
                            token_id=meta["up"],
                            extra_log=(
                                f"PEG={_peg_val:.4f} | profit={_real_pct:.2f}% | "
                                f"shares={_arb.shares:.4f} | "
                                f"cost_up=${_arb.cost_up:.4f} cost_dn=${_arb.cost_down:.4f}"
                            ),
                        )
                        if _peg_up_trade is not None:
                            await open_trade(
                                "DOWN", "PEG_ARBIT", rstr,
                                risk=cfg.peg_budget_pct * 0.5,
                                fixed_shares=_arb.shares,
                                token_id=meta["down"],
                                extra_log=(
                                    f"PEG={_peg_val:.4f} | profit={_real_pct:.2f}% | "
                                    f"shares={_arb.shares:.4f} | PAIR of UP trade"
                                ),
                            )
                        log_peg(
                            f"ARB EXECUTED | PEG={_peg_val:.4f} | "
                            f"net=${_real_net:.6f} ({_real_pct:.2f}%) | "
                            f"shares={_arb.shares:.4f} | "
                            f"fee_up={_fee_real_up:.6f} fee_dn={_fee_real_dn:.6f} | rem={rstr}"
                        )

        if cfg.gambling_active and timer.can_gambling_enter() and \
                not safety.check(ctx):
            if not gamb_started_logged:
                gamb_started_logged = True
                log_m("GAMBLING", "START",
                      f"rem={rstr} | v9.4.0 IMMEDIATE START | "
                      f"REGIME={regime.value} | Mart x{eff_mart}")
            for (g_side, g_ask, g_bid, g_ask_c, g_bs, g_as) in (
                ("UP", ask_up_f, bid_up_f, ask_up_c, snap_bs_up, snap_as_up),
                ("DOWN", ask_down_f, bid_down_f, ask_down_c, snap_bs_down,
                 snap_as_down),
            ):
                if now - gamb_last_buy[g_side] < cfg.gamb_buy_cooldown:
                    continue
                if g_ask_c < _gamb_min_ask_c_temp or g_ask_c > cfg.gamb_max_ask_c:
                    log_gambling(
                        f"BLOCKED reason=ask_out_of_range | side={g_side} | "
                        f"ask={g_ask_c:.1f}c | range=[{_gamb_min_ask_c_temp:.0f}..{cfg.gamb_max_ask_c:.0f}]c | rem={rstr}"
                    )
                    continue
                spr = ctx.best_spreads_c.get(g_side.lower())
                if spr is None or spr > cfg.max_spread_cents:
                    _spr_u = ctx.best_spreads_c.get("up")
                    _spr_d = ctx.best_spreads_c.get("down")
                    log_gambling(
                        f"BLOCKED reason=spread_too_wide | side={g_side} | "
                        f"UP_SPR={_spr_u:.1f}c DN_SPR={_spr_d:.1f}c | "
                        f"max={cfg.max_spread_cents:.1f}c | rem={rstr}"
                    )
                    continue
                if g_bid and g_ask > 0 and g_bid / g_ask < \
                        cfg.bid_ask_min_ratio - 1e-6:
                    log_gambling(
                        f"BLOCKED reason=bid_ask_ratio | side={g_side} | "
                        f"ratio={g_bid/g_ask:.4f} < {cfg.bid_ask_min_ratio:.3f} | rem={rstr}"
                    )
                    continue
                if g_ask_c / 100.0 < cfg.min_prob_entry:
                    log_gambling(
                        f"BLOCKED reason=prob_too_low | side={g_side} | "
                        f"ask={g_ask_c:.1f}c < min={cfg.min_prob_entry*100:.1f}c | rem={rstr}"
                    )
                    continue
                micro_g = micro_up if g_side == "UP" else micro_down
                if micro_g.is_volatile:
                    log_gambling(f"BLOCKED reason=spike_detected | side={g_side} | rem={rstr}")
                    continue
                if (not binance.is_stale(10.0) and
                        binance.current_price is not None and
                        binance.cycle_open_price is not None):
                    _pu, _pd = compute_jump_diffusion_probability(
                        binance.current_price, binance.cycle_open_price,
                        timer.remaining, binance.vol_annual, cfg,
                    )
                    _gbm_prob = _pu if g_side == "UP" else _pd
                    _oracle = "JD+BNC"
                else:
                    _gbm_prob = p_hat_up if g_side == "UP" else p_hat_down
                    _oracle = "BAYES_FALLBACK"
                _p_mkt = g_ask_c / 100.0
                vol_trackers[g_side].update(_p_mkt)
                _should, _es = vol_trackers[g_side].should_trade(
                    _gbm_prob, _p_mkt, cfg
                )
                if not _should:
                    log_gambling(
                        f"BLOCKED reason=low_edge_score | side={g_side} | "
                        f"ES={_es:+.3f} < {cfg.es_min_threshold:.2f} | rem={rstr}"
                    )
                    continue
                _raw_edge = _gbm_prob - g_ask - cfg.fee_buffer
                if _raw_edge < cfg.min_vwap_edge:
                    log_gambling(
                        f"BLOCKED reason=raw_edge_too_low | side={g_side} | "
                        f"edge={_raw_edge:+.4f} < {cfg.min_vwap_edge:.3f} | rem={rstr}"
                    )
                    continue
                _kelly_pre = vol_trackers[g_side].adaptive_kelly(
                    calc_kelly_bayesian(_gbm_prob, g_ask, eff_mart, cfg), cfg
                )
                if _kelly_pre <= 0.0:
                    log_gambling(f"BLOCKED reason=kelly_zero | side={g_side} | rem={rstr}")
                    continue
                _size_usd = _safe_float(_dq(bankroll * _d(_kelly_pre)))
                _tgt_shares = _size_usd / g_ask if g_ask > 1e-9 else 0.0
                l2_g = ctx.l2_up if g_side == "UP" else ctx.l2_down
                _book_asks = l2_g.asks[:10]
                _vwap = simulate_market_buy_l2(_book_asks, _tgt_shares)
                _fee_t = fee_rate_lut(
                    meta["up"] if g_side == "UP" else meta["down"],
                    ctx.fee_cache, cfg.default_taker_fee_bps
                )
                if _vwap is None:
                    log_gambling(f"BLOCKED reason=vwap_depth_exhausted | side={g_side} | rem={rstr}")
                    continue
                _liq_bias = (micro_g.liquidity_signal - 0.5) * 0.1
                _blended_prob = max(0.01, min(0.99, _gbm_prob + _liq_bias))
                _vwap_edge = _blended_prob - _vwap - _fee_t
                if _vwap_edge < cfg.min_vwap_edge:
                    log_gambling(
                        f"BLOCKED reason=vwap_edge_too_low | side={g_side} | "
                        f"vwap_edge={_vwap_edge:+.4f} < {cfg.min_vwap_edge:.3f} | rem={rstr}"
                    )
                    continue
                _kelly_risk = _kelly_pre
                if _kelly_risk <= 0.0:
                    log_gambling(f"BLOCKED reason=kelly_risk_zero | side={g_side} | rem={rstr}")
                    continue
                if bankroll > _ZERO:
                    # v9.4.2 BUG FIX: EV negativo = entrada bloqueada
                    _ev_gamb = _blended_prob - g_ask
                    if not should_enter_trade({'EV': _ev_gamb}):
                        log_gambling(f"BLOCKED reason=ev_negative | EV={_ev_gamb:.4f} | rem={rstr}")
                        continue
                    # v9.4.2 BUG FIX: direction bias check antes do BUY
                    if _bnc_active:
                        _bias = check_direction_bias(
                            binance.current_price, binance.cycle_open_price,
                            p_hat_up,
                        )
                        if _bias != "NEUTRAL" and _bias != g_side:
                            log_gambling(
                                f"BLOCKED reason=direction_mismatch | "
                                f"side={g_side} bias={_bias} | rem={rstr}"
                            )
                            continue
                    tid_g = meta["up"] if g_side == "UP" else meta["down"]
                    await open_trade(
                        g_side, "GAMBLING", rstr, risk=_kelly_risk,
                        token_id=tid_g,
                        extra_log=(
                            f"ES={_es:+.3f} | p={_blended_prob:.3f}({_oracle}) "
                            f"mkt={_p_mkt:.3f} | raw_edge={_raw_edge:+.3f} | "
                            f"vwap={_vwap:.4f} vwap_edge={_vwap_edge:+.4f} | "
                            f"liq_signal={micro_g.liquidity_signal:.2f} | "
                            f"regime={regime.value} | kelly={_kelly_risk:.2%} "
                            f"x Mart_x{eff_mart}"
                        ),
                    )
                    gamb_last_buy[g_side] = now

    return bankroll, _loser_posterior, False, _ZERO

###############################################################################
# SECTION 30 -- METRICS
###############################################################################
def _init_prometheus(cfg: BotConfig) -> Dict:
    return {}

def _push_metrics(
    metrics: Dict, tsm: TradeStateManager, ctx: BotContext, n_active: int
) -> None:
    pass

###############################################################################
# SECTION 31 -- ALERTING
###############################################################################
async def send_alert(message: str, cfg: BotConfig) -> None:
    if cfg.slack_webhook_url:
        try:
            import urllib.request as _ur
            payload = _json_dumps_compact({"text": message})
            req = __import__("urllib.request").request.Request(
                cfg.slack_webhook_url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: _ur.urlopen(req,
                                                                 timeout=5))
        except Exception:
            pass

###############################################################################
# SECTION 32 -- MAIN (v9.4.0)
###############################################################################
async def main() -> None:
    cfg = BotConfig.from_env_and_secrets("secrets.txt")
    if cfg.live_trading and not _HAS_ORJSON:
        raise SystemExit(
            "[PERF-001] FATAL: orjson is required for live_trading=True. "
            "Install: pip install orjson"
        )
    log = init_logging(cfg.audit_file)
    audit = AuditLogger(_audit_fh)
    _ctx_ref: List[BotContext] = []
    _tsm_ref: List[TradeStateManager] = []

    def _handle_signal() -> None:
        if _ctx_ref:
            _ctx_ref[0].shutdown_flag = True
        if _tsm_ref:
            try:
                _tsm_ref[0].save()
            except Exception:
                pass
        log_info("[SIGNAL] Graceful shutdown requested")

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except (NotImplementedError, OSError):
            pass

    tsm = TradeStateManager(cfg.state_file, cfg.bankroll_demo)
    tsm.load()
    _tsm_ref.append(tsm)
    ctx = BotContext(cfg=cfg, audit=audit)
    _ctx_ref.append(ctx)
    # v9.4.0: Shadow Trading -- initialize fill engine for dry_run
    if cfg.dry_run:
        ctx.shadow_engine = ShadowFillEngine(cfg)
        log_info(
            f"[SHADOW] Engine initialized | latency={cfg.shadow_latency_ms:.0f}ms "
            f"max_slip={cfg.shadow_max_slippage_pct:.1%}"
        )
    ctx.last_reconciled_bankroll = tsm.state.bankroll
    # v9.5.0: Volatility Hedge Engine 1SD-3SD -- always active
    if cfg.vol_hedge_active:
        ctx.vol_hedge_engine = VolatilityHedgeEngine(cfg)
        log_info(
            f"[VOL_HEDGE] Engine initialized | SD_window={cfg.vol_hedge_sd_window} "
            f"1SD={cfg.vol_hedge_1sd_trigger} 3SD={cfg.vol_hedge_3sd_target} "
            f"NO_limit=[{cfg.vol_hedge_no_limit_low:.2f}-{cfg.vol_hedge_no_limit_high:.2f}] "
            f"abandon_s={cfg.vol_hedge_abandon_s:.0f}s"
        )
    ctx.rate_limiter = RateLimiter(cfg.rate_limit_calls, cfg.rate_limit_burst)
    ctx.api_cb = CircuitBreaker(cfg.cb_fail_threshold, cfg.cb_recovery_s,
                                label="API_CB")
    ctx.meta_cb = CircuitBreaker(ft=10, rs=20.0, label="META_CB")
    try:
        from py_clob_client.client import ClobClient
        ctx.has_sdk = True
        ctx.clob_ro_client = ClobClient(host=cfg.clob_rest_url, chain_id=137)
        if cfg.live_trading:
            if not cfg.polymarket_private_key:
                raise SystemExit(
                    "polymarket_private_key required for LIVE_TRADING"
                )
            ctx.clob_client = ClobClient(
                host=cfg.clob_rest_url, key=cfg.polymarket_private_key,
                chain_id=137
            )
            log_info("[BOOT] SDK -- LIVE TRADING ACTIVE")
        else:
            log_info("[BOOT] SDK -- DEMO MODE")
    except ImportError:
        if cfg.live_trading:
            raise SystemExit(
                "py-clob-client not installed -- required for LIVE_TRADING=True"
            )
        log_warn("[BOOT] py-clob-client not installed -- DEMO ONLY")
    except SystemExit:
        raise
    except Exception as e:
        log_warn(f"[BOOT] SDK init warning: {e}")

    binance = BinanceState()
    funding = FundingRateState()
    safety = CapitalSafetyMonitor(tsm, cfg, audit)
    ctx.redeem_cb = CircuitBreaker(cfg.redeem_cb_threshold,
                                   cfg.redeem_cb_recovery, label="REDEEM_CB")
    if cfg.live_trading:
        _startup_lb = await fetch_live_bankroll(ctx)
        if _startup_lb is not None:
            _delta = abs(float(_startup_lb - tsm.state.bankroll))
            if _delta > 0.01:
                log_info(
                    f"[BOOT] RECON | saved={tsm.state.bankroll} -> "
                    f"live={_startup_lb} | delta=${_delta:.4f}"
                )
                tsm.update_bankroll(_startup_lb)
                ctx.last_reconciled_bankroll = _startup_lb
        await auto_redeem_positions(ctx)
        log_info("[BOOT] Startup reconciliation complete")

    ctx.hourly_start_ts = time.time()
    ctx.hourly_start_bankroll = tsm.state.bankroll
    metrics = _init_prometheus(cfg)
    log_sep2()
    log_info(
        f"BOT XRP POLYMARKET v9.5.4 -- HEDGE_RESTRICT + CIRCUIT_FREEZE + ENDGAME_RELAXED | "
        f"LIVE={cfg.live_trading} | DRY={cfg.dry_run} | "
        f"Bankroll={tsm.state.bankroll} | Mart x{tsm.state.mart_level}"
    )
    log_info(
        f"RISK: MART_MAX={cfg.mart_max_mult} | "
        f"KELLY_MAX={cfg.kelly_max_risk_pct:.1%} | "
        f"DAILY_LOSS_MAX={cfg.max_daily_loss_pct:.0f}% | "
        f"HOURLY_LOSS_MAX={cfg.max_hourly_loss_pct:.0f}% | "
        f"MAX_CONSEC_LOSSES={cfg.max_consecutive_losses}"
    )
    log_info(
        f"STRATEGY: "
        f"GAMBLING={cfg.gambling_active} (IMMEDIATE) | "
        f"ENDGAME={cfg.aggressive_endgame_active} | "
        f"VOL_HEDGE_1SD3SD={cfg.vol_hedge_active} | "
        f"MIN_PROB={cfg.min_prob_entry:.0%}"
    )
    log_info(
        f"TIMING: GAMBLING immediate | ENDGAME last {cfg.aggressive_endgame_s:.0f}s | "
        f"VOL_HEDGE abandon={cfg.vol_hedge_abandon_s:.0f}s before close | "
        f"Settlement ASYNC"
    )
    log_info(
        f"SAFETY: STOP_LOSS={cfg.max_loss_per_trade_pct:.0%}/trade | "
        f"MOONBAG_TP=recover_100%_sell_max_{cfg.partial_tp_fraction:.0%} | "
        f"COOLDOWN={cfg.gamb_buy_cooldown:.0f}s | "
        f"HEDGE_FLIP confirms={cfg.hedge_flip_confirms_needed} "
        f"stop={cfg.adverse_stop_cents:.1f}c cutoff={cfg.hedge_cutoff_s:.0f}s"
    )
    log_info(
        f"CIRCUIT: DAILY_LOSS={cfg.max_daily_loss_pct:.0f}%→freeze_{cfg.daily_pause_duration_s:.0f}s | "
        f"HOURLY_LOSS={cfg.max_hourly_loss_pct:.0f}%→freeze_{cfg.hourly_pause_duration_s:.0f}s | "
        f"VOL_HEDGE={cfg.vol_hedge_1sd_trigger:.0f}SD_entry cutoff={cfg.vol_hedge_cutoff_s:.0f}s | "
        f"EDGE_FLOOR={cfg.min_vwap_edge + cfg.fee_buffer:.3f}"
    )
    log_sep2()
    bot_metrics: Dict[str, Any] = {
        "round_count": 0, "win_count": 0,
        "total_pnl": _ZERO, "pnl_sq_sum": _ZERO,
        "current_day": tsm.state.last_market_day or "",
    }

    def _shutdown() -> bool:
        return ctx.shutdown_flag

    bnc_task = asyncio.create_task(
        binance_ticker_loop(binance, ctx.event_bus, cfg, _shutdown),
        name="binance_oracle"
    )
    funding_task = asyncio.create_task(
        funding_rate_loop(funding, cfg, _shutdown), name="funding_rate"
    )
    hb_task: Optional[asyncio.Task] = None
    if cfg.live_trading and ctx.clob_client is not None:
        hb_task = asyncio.create_task(heartbeat_loop(ctx), name="heartbeat")
    recon_task = asyncio.create_task(
        reconciliation_loop(tsm, ctx), name="reconciliation"
    )

    while not ctx.shutdown_flag:
        slug, start_ts = get_current_slug()
        meta = await fetch_metadata(slug, ctx)
        if not meta:
            _wait = 5.0 if ctx.meta_cb.is_open() else 2.0
            _cb_state = ctx.meta_cb.state
            log_warn(
                f"[MAIN] metadata unavailable for {slug} -- "
                f"meta_cb={_cb_state}(fails={ctx.meta_cb.failure_count}) -- "
                f"retry in {_wait:.0f}s"
            )
            await asyncio.sleep(_wait)
            continue
        fee_up = await fetch_fee_for_token(meta["up"], ctx)
        fee_dn = await fetch_fee_for_token(meta["down"], ctx)
        ctx.fee_cache[meta["up"]] = fee_up
        ctx.fee_cache[meta["down"]] = fee_dn
        log_info(
            f"[CYCLE START] fees UP={fee_up}bps DOWN={fee_dn}bps | slug={slug}"
        )
        ctx.resolved_event.clear()
        ctx.resolved_winner_asset = None
        ctx.current_condition_id = meta["id"]
        ctx._final_log_done = False
        market_day = datetime.fromtimestamp(start_ts).date().isoformat()
        if tsm.state.last_market_day != market_day:
            safety.reset_daily()
            if bot_metrics["round_count"] > 0:
                _wr = int(bot_metrics["win_count"]) / int(
                    bot_metrics["round_count"]
                ) * 100.0
                _avg = _safe_float(bot_metrics["total_pnl"]) / int(
                    bot_metrics["round_count"]
                )
                _e2 = _safe_float(bot_metrics["pnl_sq_sum"]) / int(
                    bot_metrics["round_count"]
                )
                _std = math.sqrt(max(_e2 - _avg ** 2, 0.0))
                _shrp = (_avg / _std) if _std > 1e-9 else 0.0
                log_sep2()
                log_info(
                    f"METRICS | Day={bot_metrics['current_day']} | "
                    f"Rounds={bot_metrics['round_count']} | "
                    f"WinRate={_wr:.1f}% | AvgPnL={fmt_dollar(_d(_avg))} | "
                    f"Sharpe={_shrp:.2f} | DayPnL={fmt_dollar(tsm.state.daily_pnl)}"
                )
                log_sep2()
            tsm.reset_daily(market_day)
            bot_metrics.update({
                "round_count": 0, "win_count": 0,
                "total_pnl": _ZERO, "pnl_sq_sum": _ZERO,
                "current_day": market_day,
            })
        if cfg.live_trading:
            lb = await fetch_live_bankroll(ctx)
            if lb is not None:
                tsm.update_bankroll(lb)
                ctx.last_reconciled_bankroll = lb
                tsm.state.session_start_bankroll = tsm.state.bankroll
                await tsm.save_async()

        ctx.l2_up = L2Snapshot()
        ctx.l2_down = L2Snapshot()
        for k in ("up", "down"):
            ctx.best_bids[k] = ctx.best_asks[k] = ctx.best_spreads_c[k] = None
            ctx.best_bid_sizes[k] = ctx.best_ask_sizes[k] = None
        ctx.pending_orders.clear()
        try:
            import websockets
        except ImportError:
            log_warn("[MAIN] websockets not installed -- pip install websockets")
            await asyncio.sleep(5)
            continue

        _strike_attempts = 0
        while binance.current_price is None and _strike_attempts < 30:
            await asyncio.sleep(0.1)
            _strike_attempts += 1
        _rest_open = await BinanceState.fetch_cycle_open_price_rest_async(
            cfg, start_ts
        )
        if _rest_open is not None:
            binance.cycle_open_price = _rest_open
            log_binance(f"cycle_open_price={_rest_open:.5f} (REST)")
        elif binance.current_price is not None:
            binance.cycle_open_price = binance.current_price
            log_info(
                f"[Binance] cycle_open_price={binance.cycle_open_price:.5f} "
                f"(WS fallback)"
            )
        else:
            log_binance("[WARN] No price available for cycle strike -- skipping cycle")
            await asyncio.sleep(5)
            continue

        ws_task = asyncio.create_task(
            ws_handler(meta["up"], meta["down"], ctx, binance),
            name="polymarket_ws"
        )

        if ctx.user_ws_task is not None and not ctx.user_ws_task.done():
            ctx.user_ws_task.cancel()
            try:
                await asyncio.wait_for(ctx.user_ws_task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass
        ctx.user_ws_task = None

        if cfg.polymarket_api_key:
            user_ws_task = asyncio.create_task(
                user_ws_loop(
                    cfg.polymarket_api_key, cfg.polymarket_secret,
                    cfg.polymarket_passphrase, meta["id"],
                    token_ids=[meta["up"], meta["down"]], ctx=ctx,
                ),
                name="user_ws",
            )
            ctx.user_ws_task = user_ws_task
        else:
            log_debug("[MAIN] USER_WS skipped (no API key)")

        log_info("[MAIN] Waiting for L2 book data from Polymarket WS...")
        start_wait = time.time()
        while time.time() - start_wait < 5.0:
            if ctx.best_asks.get("up") is not None and ctx.best_asks.get("down") is not None:
                log_info("[MAIN] L2 book data received -- starting logic_loop")
                break
            await asyncio.sleep(0.2)
        else:
            log_warn("[MAIN] No book data after 5s -- skipping cycle")
            ws_task.cancel()
            try:
                await ws_task
            except asyncio.CancelledError:
                pass
            await asyncio.sleep(0.5)
            continue


        pre_bank: Decimal = tsm.state.bankroll
        _loop_result = await logic_loop(
            start_ts, start_ts + 300, meta,
            tsm=tsm, ctx=ctx, binance=binance,
            funding=funding, safety=safety, audit=audit,
        )
        # v9.4.0 PnL FIX: logic_loop now returns a 4-tuple.
        # round_pnl_this_cycle comes directly from process_pre_settlement,
        # which splits synthetic PEG vs other positions -- this is the CORRECT
        # ROUND PnL (fixes "o problema das contas" where (final_bank - pre_bank)
        # was mixing prior-cycle pending settlements into the current round).
        if isinstance(_loop_result, tuple) and len(_loop_result) == 4:
            final_bank, _loser_post, had_pending, round_pnl = _loop_result
        elif isinstance(_loop_result, tuple) and len(_loop_result) == 3:
            # fallback for any legacy call paths
            final_bank, _loser_post, had_pending = _loop_result
            round_pnl = final_bank - pre_bank
        else:
            final_bank, _loser_post, had_pending, round_pnl = _ZERO, 0.5, False, _ZERO

        if had_pending:
            log_info(
                f"[MAIN] {len(ctx.pending_settlements)} pending "
                f"settlement(s) -- async resolution via reconciliation_loop"
            )
        bot_metrics["round_count"] = int(bot_metrics["round_count"]) + 1
        if round_pnl > _ZERO:
            bot_metrics["win_count"] = int(bot_metrics["win_count"]) + 1
        bot_metrics["total_pnl"] = bot_metrics["total_pnl"] + round_pnl
        bot_metrics["pnl_sq_sum"] = bot_metrics["pnl_sq_sum"] + \
            round_pnl * round_pnl
        pnl_pct = _safe_float(round_pnl / pre_bank * 100) if pre_bank > \
            _d("1e-9") else 0.0
        _push_metrics(metrics, tsm, ctx, 0)
        _pending_tag = f" | PENDING={len(ctx.pending_settlements)}" if \
            ctx.pending_settlements else ""
        _shadow_tag = f" | SHADOW={ctx.shadow_engine.stats}" if \
            ctx.shadow_engine is not None else ""
        _vh_tag = f" | VOL_HEDGE={ctx.vol_hedge_engine.stats}" if \
            ctx.vol_hedge_engine is not None else ""
        log_sep2()
        log_info(
            f"ROUND | {fmt_pnl(round_pnl, pnl_pct)} | "
            f"Mart: x{tsm.state.mart_level} | "
            f"consec_losses={tsm.state.consecutive_losses}"
            f"{_pending_tag}{_shadow_tag}{_vh_tag}"
        )
        log_info(
            f"TOTAL | {fmt_pnl(tsm.state.daily_pnl, tsm.pnl_daily_pct())} | "
            f"Banca: ${tsm.state.bankroll:.6f} "
            f"| Up_Time: {_uptime(ctx.bot_start_time)}"
        )
        log_sep2()
        if pnl_pct < -5.0:
            await send_alert(
                f"[XRP_BOT v9.5.4] Round loss {pnl_pct:.2f}% | "
                f"bankroll={tsm.state.bankroll} | mart x{tsm.state.mart_level}",
                cfg,
            )

        # v9.4.1 FIX: In dry_run, pending_settlements serve no purpose
        # (reconciliation_loop skips dry_run entirely). Clear immediately
        # after the heuristic PnL/martingale have been applied so PENDING
        # count doesn't accumulate indefinitely and confuse the logs.
        if cfg.dry_run and ctx.pending_settlements:
            ctx.pending_settlements.clear()

        ws_task.cancel()
        if not ctx.pending_settlements and ctx.user_ws_task is not None:
            ctx.user_ws_task.cancel()
            try:
                await ctx.user_ws_task
            except asyncio.CancelledError:
                pass
        await asyncio.sleep(0.5)

    for t in filter(None, [bnc_task, funding_task, hb_task, recon_task]):
        t.cancel()
    for t in filter(None, [bnc_task, funding_task, hb_task, recon_task]):
        try:
            await t
        except asyncio.CancelledError:
            pass
    if ctx.user_ws_task is not None:
        ctx.user_ws_task.cancel()
        try:
            await ctx.user_ws_task
        except asyncio.CancelledError:
            pass

    tsm.save()
    log_info(
        f"[SHUTDOWN] Banca: {tsm.state.bankroll} | "
        f"PnL_Total: {fmt_pct(tsm.pnl_total_pct())} | "
        f"Up_Time: {_uptime(ctx.bot_start_time)}"
    )
    if _log_listener:
        _log_listener.stop()

###############################################################################
# SECTION 33 -- MINIMAL UNIT TESTS
###############################################################################
def _run_tests() -> None:
    """v9.5.4 minimal self-tests."""
    _pass = _fail = 0
    def _assert(cond: bool, label: str) -> None:
        nonlocal _pass, _fail
        if cond:
            _pass += 1
            print(f"  [PASS] {label}")
        else:
            _fail += 1
            print(f"  [FAIL] {label}")

    print("\n─── v9.5.4 Self-Tests ───\n")
    cfg = BotConfig()

    # ── v9.5.4 config values ────────────────────────────────────────────
    _assert(cfg.gamb_min_ask_c == 42.0, "gamb_min_ask_c=42.0")
    _assert(cfg.gamb_buy_cooldown == 12.0, "gamb_buy_cooldown=12.0")
    _assert(cfg.min_vwap_edge == 0.004, "min_vwap_edge=0.004")
    _assert(cfg.fee_buffer == 0.006, "fee_buffer=0.006")
    _assert(abs((cfg.min_vwap_edge + cfg.fee_buffer) - 0.010) < 1e-9,
            "edge_floor=min_vwap_edge+fee_buffer=0.010 (1.0%)")
    _assert(cfg.bid_ask_min_ratio == 0.940, "bid_ask_min_ratio=0.940")
    _assert(cfg.es_min_threshold == 1.60, "es_min_threshold=1.60")
    _assert(cfg.kelly_max_risk_pct == 0.105, "kelly_max_risk_pct=0.105")
    _assert(cfg.partial_tp_fraction == 0.80, "partial_tp_fraction=0.80")
    _assert(cfg.partial_tp_target_net_roi == 0.08, "partial_tp_target_net_roi=0.08")
    _assert(cfg.max_loss_per_trade_pct == 0.40, "max_loss_per_trade_pct=0.40")

    # v9.5.4: HEDGE_FLIP stricter
    _assert(cfg.adverse_stop_cents == 0.9, "adverse_stop_cents=0.9")
    _assert(cfg.hedge_max_risk_pct == 0.07, "hedge_max_risk_pct=0.07")
    _assert(cfg.hedge_flip_confirms_needed == 4, "hedge_flip_confirms_needed=4")
    _assert(cfg.hedge_flip_speed_thresh == 0.003, "hedge_flip_speed_thresh=0.003")
    _assert(cfg.hedge_flip_imbalance_thresh == 0.22, "hedge_flip_imbalance_thresh=0.22")
    _assert(cfg.hedge_cutoff_s == 50.0, "hedge_cutoff_s=50.0")

    # v9.5.4: VOL_HEDGE 2SD entry + cut-off
    _assert(cfg.vol_hedge_1sd_trigger == 2.0, "vol_hedge_trigger=2.0 (2SD entry)")
    _assert(cfg.vol_hedge_cutoff_s == 50.0, "vol_hedge_cutoff_s=50.0")
    _assert(cfg.vol_hedge_active == True, "vol_hedge_active=True")

    # v9.5.4: Circuit breakers — freeze durations
    _assert(cfg.daily_pause_duration_s == 1800.0, "daily_pause_duration_s=1800 (30min)")
    _assert(cfg.hourly_pause_duration_s == 900.0, "hourly_pause_duration_s=900 (15min)")
    _assert(cfg.max_daily_loss_pct == 50.0, "max_daily_loss_pct=50.0")
    _assert(cfg.max_hourly_loss_pct == 25.0, "max_hourly_loss_pct=25.0")

    # v9.5.4: Endgame relaxed
    _assert(cfg.aggressive_endgame_s == 30.0, "aggressive_endgame_s=30.0")
    _assert(cfg.aggressive_endgame_risk == 0.22, "aggressive_endgame_risk=0.22")
    _assert(cfg.aggressive_endgame_min_c == 0.42, "aggressive_endgame_min_c=0.42")
    _assert(cfg.aggressive_endgame_max_c == 0.998, "aggressive_endgame_max_c=0.998")
    _assert(cfg.endgame_high_z_risk == 0.28, "endgame_high_z_risk=0.28")
    _assert(cfg.endgame_high_z_thresh == 2.0, "endgame_high_z_thresh=2.0")

    # Fee system
    fee = polymarket_fee(1.0, 0.50)
    _assert(fee >= 0.0, f"polymarket_fee returns non-negative (got {fee})")

    # Martingale
    tsm_t = TradeStateManager("/tmp/_test_v954.json", Decimal("10.0"))
    tsm_t.state.consecutive_losses = 3
    tsm_t.update_martingale(_d("0.05"), cfg)
    _assert(tsm_t.state.consecutive_losses == 0, "WIN resets consecutive_losses")

    # ShadowFillEngine
    _sfe = ShadowFillEngine(cfg)
    _assert(hasattr(_sfe, 'try_fill_sell'), "ShadowFillEngine.try_fill_sell exists")
    _assert(hasattr(_sfe, 'try_fill_limit_no'), "ShadowFillEngine.try_fill_limit_no exists")

    # VolatilityHedgeEngine
    vhe = VolatilityHedgeEngine(cfg)
    for i in range(35):
        vhe.feed_price(2.5 + i * 0.001)
    _assert(vhe.current_sd > 0.0, f"VolHedge SD > 0 (got {vhe.current_sd:.8f})")
    vhe.cleanup_cycle()
    _assert(len(vhe.active_positions) == 0, "Cleanup resets positions")

    # fmt_pnl
    _pnl_pos = fmt_pnl(_d("0.123456"), 5.67)
    _assert("(+)$0.123456" in _pnl_pos, f"fmt_pnl positive (got {_pnl_pos})")
    _pnl_neg = fmt_pnl(_d("-0.054321"), -2.34)
    _assert("(-)$0.054321" in _pnl_neg, f"fmt_pnl negative (got {_pnl_neg})")

    # Moonbag TP
    _mb_trade1 = Trade(
        side="UP", ask=0.60, bid_at_buy=0.59,
        eff_c=60.5, shares=_d("10.0"), target=None,
        type="GAMBLING", invested_pure=_d("6.0"),
        fee_buy=_d("0.10"), total_out=_d("6.10"),
        token_id="t_mb1",
    )
    _mb_sh, _mb_fr, _mb_moon = calculate_moonbag_shares(_mb_trade1, 0.95, 0.80)
    _assert(_mb_sh is not None, "Moonbag possible at bid=0.95")
    _assert(_mb_fr < 0.80, f"Moonbag fraction < 80% (got {_mb_fr*100:.1f}%)")
    _mb_sh2, _, _ = calculate_moonbag_shares(_mb_trade1, 0.65, 0.80)
    _assert(_mb_sh2 is None, "Moonbag impossible at bid=0.65 (need >80%)")

    # Arb
    empty_book = OrderBookSide(levels=[])
    full_book = OrderBookSide(levels=[OrderBookLevel(price=0.48, size=100)])
    result = evaluate_arb(
        empty_book, full_book, budget=1.0, peg_trigger=0.98,
        token_id_up="t1", token_id_dn="t2", fee_cache={}, cfg=cfg,
    )
    _assert(result.status == ArbStatus.REJECT_EMPTY_BOOK, "empty book -> REJECT")

    print(f"\n{'═' * 40}")
    print(f"Tests: {_pass} passed, {_fail} failed")
    if _fail > 0:
        sys.exit(1)
    print("All tests passed! ✅\n")

###############################################################################
# SECTION 34 -- ENTRY POINT (v9.4.0)
###############################################################################
if __name__ == "__main__":
    def _handle_uncaught(
        exc_type: type, exc_value: BaseException, exc_tb: Any
    ) -> None:
        if issubclass(exc_type, KeyboardInterrupt):
            sys.excepthook(exc_type, exc_value, exc_tb)
            return
        tb_str = "".join(traceback.format_exception(exc_type, exc_value,
                                                    exc_tb))
        logging.getLogger("bot_xrp").critical(
            "[FATAL] UNCAUGHT | %s: %s\n%s", exc_type.__name__, exc_value,
            tb_str
        )
        sys.excepthook(exc_type, exc_value, exc_tb)

    sys.excepthook = _handle_uncaught

    if "--test" in sys.argv:
        _run_tests()
        sys.exit(0)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
