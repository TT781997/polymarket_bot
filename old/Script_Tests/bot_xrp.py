import asyncio
import websockets
import json
import time
import logging
import requests
import os
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING — ficheiro exclusivo, sem output na consola
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    filename='bot_xrp.log',
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger()

# ─────────────────────────────────────────────────────────────────────────────
# SECRETS
# ─────────────────────────────────────────────────────────────────────────────
def load_secrets(filepath="secrets.txt") -> dict:
    if not os.path.exists(filepath):
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

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURAÇÕES DE ESTRATÉGIA ← alterar aqui, nunca no meio do código
# ─────────────────────────────────────────────────────────────────────────────
BANKROLL_INIT = 10.0
MIN_OPEN_PRICE_C = 44.0  # Preço nominal mínimo para sniper (em cents)
MAX_OPEN_PRICE_C = 56.0  # Preço nominal máximo para sniper (em cents)
RISK_PER_TRADE = 0.10    # 10% da banca por entrada
CYCLE_PRICE_MIN_C = 76.0 # Preço mínimo para ciclos (cents)
CYCLE_PRICE_MAX_C = 94.0 # Preço máximo para ciclos (cents)
CYCLE_PEG_MIN_C = 98.0   # sum(UP + DOWN) mínimo em cents — mercado equilibrado
CYCLE_VOL_MAX_C = 50.0   # Volatilidade máxima em cents entre snapshot e decisão
SNIPER_WINDOW_S = 3.0    # Segundos desde m_start para o sniper entrar
LOOP_SLEEP = 0.005       # 5ms — ticker de alta precisão

# ── OPENING (Sniper) ────────────────────────────────────────────────────────
# STOP: dinâmico — quando preço toca 20c pela 1ª vez, stop = eff_entry × (1 − OPENING_STOP_PCT)
# CLOSING: rem <= 2s — vende a mercado perto do fim
OPENING_TOUCH_C   = 0.20  # preço que activa o cálculo do stop dinâmico
OPENING_STOP_PCT  = 0.10  # stop = eff_entry × (1 − 0.10)
OPENING_CLOSE_REM = 2.0   # Fecha todas as Opening quando rem <= 2s

# ── DIP BUY ────────────────────────────────────────────────────────────────
# Compra UP e DOWN (independentemente) quando o preço cai a 20c,
# nos primeiros 3 minutos de mercado. Target de venda: 40c.
# 1 entrada por lado por mercado. 10% da banca por trade.
DIP_TRIGGER_C  = 0.20  # preço de gatilho (compra quando preço <= 20c)
DIP_TARGET_C   = 0.40  # target de venda (vende quando preço >= 40c)
DIP_RISK       = 0.10  # 10% da banca por trade
DIP_WINDOW_REM = 120.0 # janela: apenas nos primeiros 3 minutos (rem > 120s)

# ── PEG ARBITRAGE (UNDERPEG) ───────────────────────────────────────────────
# Compra UP + DOWN quando UP+DOWN < 1.00 (underpeg >= threshold).
# O vencedor paga sempre 1.00 → garantido receber mais do que se pagou.
# Aguarda SEMPRE o fecho do mercado para resolver. Nunca aplica TARGET.
# Exemplo: UP 42c + DOWN 45c = 0.87 → paga $0.87 total → recebe $1.00 → lucro ~13c (menos fees)
PEG_ARBIT_UNDERPEG_C = 10.0  # underpeg mínimo em cents para activar (peg <= 0.90)
PEG_ARBIT_RISK      = 0.15  # 15% da banca por lado (UP e DOWN)
PEG_ARBIT_COOLDOWN  = 0.1   # Espera mínima em segundos entre entradas (100ms anti-duplo-tick)
MAX_PEG_ENTRIES     = 5     # Número máximo de arbitragens por mercado

# ─────────────────────────────────────────────────────────────────────────────
# ESTADO GLOBAL
# ─────────────────────────────────────────────────────────────────────────────
bankroll = BANKROLL_INIT
daily_profit = 0.0
last_day = None
best_asks = {'up': None, 'down': None}
price_change = None

# ─────────────────────────────────────────────────────────────────────────────
# FEES E PREÇOS
# ─────────────────────────────────────────────────────────────────────────────
# Polymarket — mercados crypto 5-min/15-min
# Fórmula oficial: fee = C × p × feeRate × (p × (1−p))^exponent
#   feeRate = 0.25 | exponent = 2
# COMPRA : fee cobrada em shares  → recebe menos shares
# VENDA  : fee cobrada em USDC   → recebe menos USDC
# Tabela de referência (100 shares):
#   p=0.10 → 0.20% | p=0.30 → 1.10% | p=0.50 → 1.56% | p=0.70 → 1.10% | p=0.90 → 0.20%

FEE_RATE = 0.25
FEE_EXP  = 2
ASK_SPREAD = 0.01  # 1c estimativa de spread ask vs mid-price

def fee_rate(p: float) -> float:
    """Taxa de fee para mercados crypto 5-min (igual em compra e venda).
    Retorna fracção: ex. 0.0156 para p=0.50 (1.56%)
    """
    return FEE_RATE * (p * (1 - p)) ** FEE_EXP

def buy_shares_net(invested: float, ask: float) -> float:
    """Shares LÍQUIDAS após compra.
    A fee é cobrada em shares: net = (invested/ask) × (1 − fee_rate(ask))
    """
    gross = invested / ask
    return gross * (1 - fee_rate(ask))

def effective_entry(ask: float) -> float:
    """Custo efectivo por share (inclui fee de compra).
    eff = ask / (1 − fee_rate(ask))
    """
    return ask / (1 - fee_rate(ask))

def sell_payout(shares: float, p: float) -> float:
    """Payout LÍQUIDO em USDC após venda (fee cobrada em USDC).
    payout = shares × p × (1 − fee_rate(p))
    """
    return shares * p * (1 - fee_rate(p))

# ─────────────────────────────────────────────────────────────────────────────
# UTILITÁRIOS DE LOG
# ─────────────────────────────────────────────────────────────────────────────
def c(p: float) -> float:
    return p * 100

def fc(p: float) -> str:
    return f"{p * 100:.1f}c"

def get_ts() -> str:
    return datetime.now().strftime("[%y/%d/%m | %H:%M:%S.%f")[:-3] + "]"

def get_remaining_str(rem: float) -> str:
    rem = max(0.0, rem)
    m = int(rem // 60)
    s = int(rem % 60)
    ms = int((rem * 1000) % 1000)
    return f"{m:02d}:{s:02d}:{ms:03d}"

def log_info(msg: str):
    """Linha com prefixo [INFO]."""
    logger.info(f"[INFO] {get_ts()} {msg}")

def log_raw(msg: str):
    """Linha sem prefixo [INFO] — para IDs, Cond, ticks de preço."""
    logger.info(f"{get_ts()} {msg}")

def log_sep():
    """Separador visual sem prefixo."""
    logger.info("=" * 73)

# ─────────────────────────────────────────────────────────────────────────────
# MERCADO
# ─────────────────────────────────────────────────────────────────────────────
def fetch_metadata(slug: str) -> dict | None:
    try:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        data = requests.get(url, timeout=5).json()[0]['markets'][0]
        ids = json.loads(data['clobTokenIds'])
        return {'id': data['conditionId'], 'up': ids[0], 'down': ids[1], 'slug': slug}
    except Exception:
        return None

def get_current_slug() -> tuple:
    now = time.time()
    start_ts = now - (now % 300)
    if (start_ts + 300 - now) < 5:
        start_ts += 300
    return f"xrp-updown-5m-{int(start_ts)}", start_ts

# ─────────────────────────────────────────────────────────────────────────────
# WEBSOCKET
# ─────────────────────────────────────────────────────────────────────────────
async def ws_handler(t_up: str, t_down: str):
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    while True:
        try:
            async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                await ws.send(json.dumps({
                    "assets_ids": [t_up, t_down],
                    "type": "market",
                    "custom_feature_enabled": True
                }))
                log_info("WS: Ligado ao order book Polymarket")
                async for raw in ws:
                    items = json.loads(raw)
                    if not isinstance(items, list):
                        items = [items]
                    for item in items:
                        aid = item.get("asset_id")
                        p = None
                        evt = item.get("event_type")
                        if evt == "book" and item.get("asks"):
                            valid = [float(d['price']) for d in item["asks"] if float(d['size']) > 0]
                            if valid:
                                p = min(valid)
                        elif evt == "best_bid_ask" and item.get("best_ask"):
                            p = float(item["best_ask"])
                        if p is not None:
                            if aid == t_up:
                                best_asks['up'] = p
                            elif aid == t_down:
                                best_asks['down'] = p
                            price_change.set()
        except asyncio.CancelledError:
            break
        except Exception as e:
            log_info(f"WS_ERR: {e} — reconectando em 1s")
            await asyncio.sleep(1)

# ─────────────────────────────────────────────────────────────────────────────
# LÓGICA PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────
async def logic_loop(m_start: float, m_end: float, meta: dict):
    global bankroll, daily_profit
    active_trades = []

    state = {
        'c1': {'snap_up': None, 'snap_dw': None},
        'c2': {'snap_up': None, 'snap_dw': None},
    }

    flags = {
        's35': False, 'v30': False, 'd29': False,
        's25': False, 'v20': False, 'd19': False,
        'open': False,
        'opening_closed': False,  # flag para fechar Opening uma só vez
        'dip_up':   False,        # DIP BUY UP já executado neste mercado
        'dip_down': False,        # DIP BUY DOWN já executado neste mercado
    }

    # Estado Opening — stop dinâmico activado quando preço toca 20c pela 1ª vez
    opening_stop: dict = {}  # {trade_id: stop_price} — keyed by id(trade)

    # ── PEG MOMENTUM (último minuto) ─────────────────────────────────
    # Regista (rem, peg) a cada tick nos últimos 60s.
    # Detecta pressão compradora consistente: PEG sobe enquanto rem desce.
    # Sinal FORTE quando:
    #   >= PEG_MOM_MIN_TICKS ticks consecutivos com PEG crescente
    #   variação total >= PEG_MOM_MIN_RISE_C cents
    PEG_MOM_WINDOW_REM = 60.0  # activo nos últimos 60s
    PEG_MOM_MIN_TICKS  = 3     # mínimo de ticks consecutivos crescentes
    PEG_MOM_MIN_RISE_C = 5.0   # variação mínima total de PEG em cents
    PEG_MOM_RISK       = 0.10  # 10% da banca por entrada
    peg_history: list  = []    # [(rem, peg), ...] — janela deslizante 60s
    flags['peg_mom']   = False # entrada PEG_MOM já executada neste mercado

    # ── REJECTION DETECTOR ───────────────────────────────────────────
    # Detecta quando o lado perdedor tenta recuperar mas é rejeitado.
    # Padrão: lado fraco sobe X cents em <= Y segundos, depois cai >= X*0.6c
    # Isso confirma que o lado dominante vai ganhar.
    # price_buf: {'up': [(rem, price), ...], 'down': [(rem, price), ...]}
    REJ_WINDOW_S    = 5.0   # janela de observação do spike (segundos)
    REJ_SPIKE_C     = 0.15  # spike mínimo do lado fraco para activar (15c)
    REJ_FALLBACK_C  = 0.10  # queda mínima após o spike para confirmar rejeição (10c)
    REJ_DOM_FLOOR_C = 0.70  # lado dominante deve estar acima deste piso (70c)
    REJ_MIN_REM     = 5.0   # não entra nos últimos 5s
    REJ_RISK        = 0.10  # 10% da banca
    price_buf: dict = {'up': [], 'down': []}  # histórico por lado
    flags['rejection'] = False  # entrada REJECTION já executada neste mercado

    # Controladores PEG ARBITRAGE
    peg_arbit_count = 0
    last_peg_time = 0.0

    sniper_deadline = m_start + SNIPER_WINDOW_S

    # ── Cabeçalho ── formato exacto pedido
    log_sep()
    log_info(f"Market: {meta['slug']} | URL: https://polymarket.com/event/{meta['slug']}")
    log_raw(f"UP: {meta['up']} | DW: {meta['down']}")
    log_raw(f"Cond: {meta['id']} | Bank: ${bankroll:.2f} | Profit Acumulado: ${daily_profit:.2f}")
    log_sep()
    log_info(">>> ESCUTA ATIVA")

    # ─────────────────────────────────────────────
    # HELPER — Abre posição
    # Opening:   target = entrada_real + 10c absolutos; fecha em rem<=0.5 se não atingido
    # Ciclos:    target = entrada_real + fee + 10% relativo
    # PEG_ARBIT: wait_close=True, sem target activo — underpeg garante lucro no vencedor
    # ─────────────────────────────────────────────
    def open_trade(side: str, nom: float, trade_type: str, rstr: str,
                   risk: float = RISK_PER_TRADE, wait_close: bool = False,
                   fixed_invest: float = None):
        global bankroll
        ask   = nom + ASK_SPREAD                          # preço real de ask (spread)
        eff   = effective_entry(ask)                      # custo efectivo por share (com fee compra)
        invested = fixed_invest if fixed_invest is not None else (bankroll * risk)
        shares   = buy_shares_net(invested, ask)          # shares líquidas após fee de compra

        if trade_type == 'Opening':
            # Saída por tempo (rem<=2s) ou stop-loss (20c) — sem target de preço
            target = None
        else:
            # TARGET: eff_entry × (1 + 10%) — já inclui fee de compra, target cobre fee de venda
            target = min(0.99, eff * 1.10)

        bankroll_antiga = bankroll
        bankroll -= invested
        diff_str = f"({bankroll - bankroll_antiga:+.2f})"

        buy_fee_pct  = fee_rate(ask) * 100   # fee de compra (em shares)
        sell_fee_pct = fee_rate(nom) * 100   # fee estimada de venda ao preço nominal

        trade = {
            'side':       side,
            'nom':        nom,
            'entry':      eff,                            # custo efectivo por share
            'shares':     shares,
            'target':     target,
            'type':       trade_type,
            'invested':   invested,
            'wait_close': wait_close,
        }
        active_trades.append(trade)

        if wait_close:
            log_info(
                f"Remaining: {rstr} | COMPRA {trade_type} {side} "
                f"@ {fc(nom)} | Ask: {fc(ask)} | Eff: {fc(eff)} | "
                f"Shares: {shares:.4f} | Inv: ${invested:.2f} | Banca: ${bankroll:.2f} {diff_str} "
                f"| Fee compra: {buy_fee_pct:.2f}% | AGUARDA FECHO DE MERCADO"
            )
        elif trade_type == 'Opening':
            log_info(
                f"Remaining: {rstr} | COMPRA {trade_type} {side} "
                f"@ {fc(nom)} | Ask: {fc(ask)} | Eff: {fc(eff)} | "
                f"Shares: {shares:.4f} | Inv: ${invested:.2f} | Banca: ${bankroll:.2f} {diff_str} "
                f"| STOP dyn (toca {fc(OPENING_TOUCH_C)} → eff×{1-OPENING_STOP_PCT:.0%}) | FECHA rem≤2s | Fee compra: {buy_fee_pct:.2f}%"
            )
        else:
            log_info(
                f"Remaining: {rstr} | COMPRA {trade_type} {side} "
                f"@ {fc(nom)} | Ask: {fc(ask)} | Eff: {fc(eff)} | "
                f"Shares: {shares:.4f} | Inv: ${invested:.2f} | Banca: ${bankroll:.2f} {diff_str} "
                f"| SELL TARGET {fc(target)} (+10%) | Fee compra: {buy_fee_pct:.2f}% | Fee venda est: {sell_fee_pct:.2f}%"
            )

    # ─────────────────────────────────────────────
    # HELPER — Fecha posição
    # Payout = valor líquido recebido (fee já deduzida).
    # PnL    = Payout − Investido.
    # ─────────────────────────────────────────────
    def close_trade(trade: dict, cp: float, reason: str, rstr: str):
        global bankroll
        payout = sell_payout(trade['shares'], cp)
        pnl = payout - trade['invested']
        fee_pct = fee_rate(cp) * 100

        bankroll_antiga = bankroll
        bankroll += payout
        diff_str = f"({bankroll - bankroll_antiga:+.2f})"

        log_info(
            f"Remaining: {rstr} | SELL [{reason}] "
            f"{trade['type']} {trade['side']} @ {fc(cp)} | "
            f"Fee: {fee_pct:.2f}% | "
            f"Payout (líq): ${payout:.4f} | PnL: ${pnl:+.4f} | Banca: ${bankroll:.2f} {diff_str}"
        )

    # ─────────────────────────────────────────────
    # LOOP PRINCIPAL — corre a 5ms
    # ─────────────────────────────────────────────
    prev_u_p = None
    prev_d_p = None

    while True:
        now = time.time()
        rem = m_end - now

        # ── FIM DE MERCADO ────────────────────────────────────────
        if rem <= 0:
            u_p = best_asks['up']
            d_p = best_asks['down']

            peg_inv_total = 0.0
            peg_payout_total = 0.0
            bankroll_antes_peg = bankroll

            for trade in active_trades:
                cp = (u_p if trade['side'] == 'UP' else d_p) or 0.0
                if trade['type'] == 'PEG_ARBIT':
                    payout = sell_payout(trade['shares'], cp)
                    peg_inv_total += trade['invested']
                    peg_payout_total += payout
                    bankroll += payout
                else:
                    close_trade(trade, cp, "FIM_MERCADO", "00:00:000")

            if peg_inv_total > 0:
                pnl_liquido = peg_payout_total - peg_inv_total
                diff_str = f"({bankroll - bankroll_antes_peg:+.2f})"
                log_info(
                    f"Remaining: 00:00:000 | SELL [FIM_MERCADO] PEG_ARBIT (UP+DOWN) | "
                    f"Total Inv: ${peg_inv_total:.2f} | Payout (líq): ${peg_payout_total:.4f} | "
                    f"PnL: ${pnl_liquido:+.4f} | Banca: ${bankroll:.2f} {diff_str}"
                )

            active_trades.clear()
            log_info(f"Remaining: 00:00:000 | Fim de Mercado | Bank: ${bankroll:.2f}")
            break

        rstr = get_remaining_str(rem)
        u_p = best_asks['up']
        d_p = best_asks['down']

        # Aguarda evento de preço com timeout de 5ms
        try:
            await asyncio.wait_for(price_change.wait(), timeout=LOOP_SLEEP)
            price_change.clear()
        except asyncio.TimeoutError:
            pass

        # Refresca preços após espera
        u_p = best_asks['up']
        d_p = best_asks['down']

        # Só loga e age quando pelo menos um preço mudou
        price_changed = (u_p != prev_u_p or d_p != prev_d_p)

        if price_changed and u_p is not None and d_p is not None:
            prev_u_p = u_p
            prev_d_p = d_p

            peg = u_p + d_p
            underpeg_c = (1.0 - peg) * 100
            indicator = f" -{underpeg_c:.0f}c" if peg < 0.92 else ""

            # Tick de preço — sem [INFO]
            log_raw(
                f"Remaining: {rstr} | UP: {fc(u_p)} | DOWN: {fc(d_p)} | PEG: {peg:.3f}{indicator}"
            )

            # ── 1. PEG ARBITRAGE ──────────────────────────────────────
            if underpeg_c >= PEG_ARBIT_UNDERPEG_C and rem > 9.5:
                if peg_arbit_count < MAX_PEG_ENTRIES:
                    if now - last_peg_time >= PEG_ARBIT_COOLDOWN:
                        invest_per_leg = bankroll * PEG_ARBIT_RISK
                        log_info(
                            f"Remaining: {rstr} | PEG ARBIT ACTIVADO ({peg_arbit_count+1}/{MAX_PEG_ENTRIES}) — "
                            f"PEG {peg:.3f} (-{underpeg_c:.1f}c) | "
                            f"Compra UP + DOWN @ ${invest_per_leg:.2f} cada"
                        )
                        open_trade('UP',   u_p, 'PEG_ARBIT', rstr, fixed_invest=invest_per_leg, wait_close=True)
                        open_trade('DOWN', d_p, 'PEG_ARBIT', rstr, fixed_invest=invest_per_leg, wait_close=True)
                        peg_arbit_count += 1
                        last_peg_time = now

            # ── 2. TARGET para trades normais (Ciclos + PEG_MOM + DIP) ──
            for trade in active_trades[:]:
                if trade['wait_close']:
                    continue
                if trade['type'] == 'Opening':
                    continue  # Opening fecha apenas por STOP (20c) ou rem<=2s
                cp = u_p if trade['side'] == 'UP' else d_p
                if cp is None:
                    continue
                if cp >= trade['target']:
                    close_trade(trade, cp, "TARGET", rstr)
                    active_trades.remove(trade)

            # ── 3. STOP dinâmico para Opening ─────────────────────────
            # Quando preço toca OPENING_TOUCH_C (20c) pela 1ª vez,
            # regista stop = eff_entry × (1 − OPENING_STOP_PCT).
            # Nas iterações seguintes, vende se preço <= stop registado.
            for trade in active_trades[:]:
                if trade['type'] != 'Opening':
                    continue
                tid = id(trade)
                cp = u_p if trade['side'] == 'UP' else d_p
                if cp is None:
                    continue
                # Activa stop dinâmico na 1ª vez que preço toca 20c
                if tid not in opening_stop and cp <= OPENING_TOUCH_C:
                    stop_price = round(trade['entry'] * (1 - OPENING_STOP_PCT), 4)
                    opening_stop[tid] = stop_price
                    log_info(
                        f"Remaining: {rstr} | OPENING STOP ACTIVADO {trade['side']} "
                        f"— preço tocou {fc(cp)} | stop fixado em {fc(stop_price)} "
                        f"(eff {fc(trade['entry'])} × {1-OPENING_STOP_PCT:.0%})"
                    )
                # Verifica stop
                if tid in opening_stop and cp <= opening_stop[tid]:
                    close_trade(trade, cp, "STOP", rstr)
                    active_trades.remove(trade)
                    opening_stop.pop(tid, None)

            # ── 4. TARGET para DIP BUY (a qualquer momento) ───────────
            for trade in active_trades[:]:
                if trade['type'] != 'DIP':
                    continue
                cp = u_p if trade['side'] == 'UP' else d_p
                if cp is None:
                    continue
                if cp >= trade['target']:
                    close_trade(trade, cp, "TARGET", rstr)
                    active_trades.remove(trade)

        # ── 5. FECHA OPENING nos últimos 2s (só se PnL negativo) ────────
        # PnL negativo → vende agora para limitar perda.
        # PnL positivo → deixa correr até FIM_MERCADO.
        if rem <= OPENING_CLOSE_REM and not flags['opening_closed']:
            u_p_now = best_asks['up']
            d_p_now = best_asks['down']
            for trade in active_trades[:]:
                if trade['type'] != 'Opening':
                    continue
                cp = (u_p_now if trade['side'] == 'UP' else d_p_now) or 0.0
                projected_payout = sell_payout(trade['shares'], cp)
                if projected_payout < trade['invested']:
                    close_trade(trade, cp, "REM_2S", rstr)
                    active_trades.remove(trade)
                    opening_stop.pop(id(trade), None)
                else:
                    log_info(
                        f"Remaining: {rstr} | REM_2S SKIP Opening {trade['side']} "
                        f"@ {fc(cp)} — PnL positivo (${projected_payout - trade['invested']:+.4f}), aguarda FIM_MERCADO"
                    )
            flags['opening_closed'] = True

        # ── 6. DIP BUY — primeiros 3 minutos (rem > 120s) ─────────────
        # Compra UP ou DOWN na 1ª vez que o preço cai a 20c.
        # Target de venda: 40c. 10% da banca. 1 entrada por lado.
        if rem > DIP_WINDOW_REM:
            if u_p is not None and not flags['dip_up'] and u_p <= DIP_TRIGGER_C:
                invest = bankroll * DIP_RISK
                ask_d  = u_p + ASK_SPREAD
                eff_d  = effective_entry(ask_d)
                shares_d = buy_shares_net(invest, ask_d)
                bankroll_ant = bankroll
                bankroll -= invest
                diff_str = f"({bankroll - bankroll_ant:+.2f})"
                dip_trade = {
                    'side':       'UP',
                    'nom':        u_p,
                    'entry':      eff_d,
                    'shares':     shares_d,
                    'target':     DIP_TARGET_C,
                    'type':       'DIP',
                    'invested':   invest,
                    'wait_close': False,
                }
                active_trades.append(dip_trade)
                flags['dip_up'] = True
                log_info(
                    f"Remaining: {rstr} | DIP BUY UP @ {fc(u_p)} | Ask: {fc(ask_d)} | Eff: {fc(eff_d)} | "
                    f"Shares: {shares_d:.4f} | Inv: ${invest:.2f} | Banca: ${bankroll:.2f} {diff_str} "
                    f"| TARGET {fc(DIP_TARGET_C)} | Fee compra: {fee_rate(ask_d)*100:.2f}%"
                )
            if d_p is not None and not flags['dip_down'] and d_p <= DIP_TRIGGER_C:
                invest = bankroll * DIP_RISK
                ask_d  = d_p + ASK_SPREAD
                eff_d  = effective_entry(ask_d)
                shares_d = buy_shares_net(invest, ask_d)
                bankroll_ant = bankroll
                bankroll -= invest
                diff_str = f"({bankroll - bankroll_ant:+.2f})"
                dip_trade = {
                    'side':       'DOWN',
                    'nom':        d_p,
                    'entry':      eff_d,
                    'shares':     shares_d,
                    'target':     DIP_TARGET_C,
                    'type':       'DIP',
                    'invested':   invest,
                    'wait_close': False,
                }
                active_trades.append(dip_trade)
                flags['dip_down'] = True
                log_info(
                    f"Remaining: {rstr} | DIP BUY DOWN @ {fc(d_p)} | Ask: {fc(ask_d)} | Eff: {fc(eff_d)} | "
                    f"Shares: {shares_d:.4f} | Inv: ${invest:.2f} | Banca: ${bankroll:.2f} {diff_str} "
                    f"| TARGET {fc(DIP_TARGET_C)} | Fee compra: {fee_rate(ask_d)*100:.2f}%"
                )

        # ── 7. PEG MOMENTUM — registo e decisão (último minuto) ────────
        # Regista cada tick de preço quando rem <= 60s.
        # Analisa se PEG está a subir consistentemente (pressão compradora).
        # Entra no lado dominante se sinal for forte.
        if price_changed and u_p is not None and d_p is not None:
            if rem <= PEG_MOM_WINDOW_REM:
                peg_now = u_p + d_p
                peg_history.append((rem, peg_now))
                # Mantém apenas os últimos 60s (remove entradas antigas)
                peg_history[:] = [(r, p) for r, p in peg_history if r >= rem - 0.5]

                if not flags['peg_mom'] and len(peg_history) >= PEG_MOM_MIN_TICKS + 1:
                    # Conta ticks consecutivos crescentes a partir do fim
                    consecutive = 0
                    for i in range(len(peg_history) - 1, 0, -1):
                        if peg_history[i][1] > peg_history[i-1][1]:
                            consecutive += 1
                        else:
                            break

                    peg_rise_c = (peg_history[-1][1] - peg_history[-consecutive][1]) * 100 if consecutive > 0 else 0.0
                    dominant_side = 'UP' if u_p > d_p else 'DOWN'
                    nom_mom = u_p if dominant_side == 'UP' else d_p

                    if consecutive >= PEG_MOM_MIN_TICKS and peg_rise_c >= PEG_MOM_MIN_RISE_C:
                        log_info(
                            f"Remaining: {rstr} | PEG_MOM SINAL — "
                            f"{consecutive} ticks crescentes | PEG subiu +{peg_rise_c:.1f}c "
                            f"({peg_history[-consecutive][1]:.3f} → {peg_history[-1][1]:.3f}) | "
                            f"Pressão em {dominant_side} @ {fc(nom_mom)}"
                        )
                        open_trade(dominant_side, nom_mom, 'PEG_MOM', rstr, risk=PEG_MOM_RISK)
                        flags['peg_mom'] = True
                    else:
                        # Log de monitorização sem sinal de entrada
                        logger.info(
                            f"{get_ts()} Remaining: {rstr} | "
                            f"PEG_MOM watch — ticks↑: {consecutive} | "
                            f"rise: +{peg_rise_c:.1f}c | PEG: {peg_now:.3f} | "
                            f"dom: {dominant_side} @ {fc(nom_mom)}"
                        )

        # ── 8. REJECTION DETECTOR ────────────────────────────────────
        # A cada tick, alimenta price_buf com (rem, price) para UP e DOWN.
        # Analisa o lado FRACO (preço mais baixo): se fez spike >= 15c
        # e depois caiu >= 10c dentro de 5s → rejeição confirmada.
        # O lado DOMINANTE deve estar >= 70c. Entra no dominante.
        if price_changed and u_p is not None and d_p is not None:
            # Alimenta buffer para ambos os lados (janela deslizante REJ_WINDOW_S)
            price_buf['up'].append((rem, u_p))
            price_buf['down'].append((rem, d_p))
            price_buf['up'][:]   = [(r, p) for r, p in price_buf['up']   if r >= rem - REJ_WINDOW_S]
            price_buf['down'][:] = [(r, p) for r, p in price_buf['down'] if r >= rem - REJ_WINDOW_S]

            if not flags['rejection'] and rem > REJ_MIN_REM:
                dominant_side = 'UP' if u_p > d_p else 'DOWN'
                weak_side     = 'DOWN' if dominant_side == 'UP' else 'UP'
                dom_price     = u_p if dominant_side == 'UP' else d_p
                weak_buf      = price_buf[weak_side.lower()]

                if dom_price >= REJ_DOM_FLOOR_C and len(weak_buf) >= 3:
                    weak_prices = [p for _, p in weak_buf]
                    peak        = max(weak_prices)
                    current_weak = weak_prices[-1]
                    spike_c     = peak - weak_prices[0]   # subida desde início da janela
                    fall_c      = peak - current_weak      # queda desde o pico

                    if spike_c >= REJ_SPIKE_C and fall_c >= REJ_FALLBACK_C:
                        nom_rej = u_p if dominant_side == 'UP' else d_p
                        log_info(
                            f"Remaining: {rstr} | REJECTION DETECTADO — "
                            f"{weak_side} tentou subir +{spike_c*100:.0f}c "
                            f"(pico {fc(peak)}) mas caiu -{fall_c*100:.0f}c → agora {fc(current_weak)} | "
                            f"{dominant_side} firme @ {fc(dom_price)} (>= {fc(REJ_DOM_FLOOR_C)}) | "
                            f"ENTRA {dominant_side}"
                        )
                        open_trade(dominant_side, nom_rej, 'REJECTION', rstr, risk=REJ_RISK)
                        flags['rejection'] = True
                    else:
                        # Monitorização — spike em progresso ou insuficiente
                        if spike_c >= REJ_SPIKE_C * 0.5:
                            logger.info(
                                f"{get_ts()} Remaining: {rstr} | "
                                f"REJ watch — {weak_side} spike: +{spike_c*100:.0f}c "
                                f"pico: {fc(peak)} agora: {fc(current_weak)} "
                                f"queda: -{fall_c*100:.0f}c/{REJ_FALLBACK_C*100:.0f}c | "
                                f"dom {dominant_side} @ {fc(dom_price)}"
                            )

        # ── 9. SNIPER DE ABERTURA ─────────────────
        if not flags['open'] and now <= sniper_deadline:
            if u_p is not None and d_p is not None:
                # Fixa o valor de investimento ANTES das duas compras
                # para que UP e DOWN recebam exactamente o mesmo montante
                opening_invest = bankroll * RISK_PER_TRADE
                for side, nom in (('UP', u_p), ('DOWN', d_p)):
                    nom_c = c(nom)
                    if MIN_OPEN_PRICE_C <= nom_c <= MAX_OPEN_PRICE_C:
                        open_trade(side, nom, 'Opening', rstr, fixed_invest=opening_invest)
                    else:
                        log_info(f"Remaining: {rstr} | SNIPER SKIP {side} — Nominal {nom_c:.1f}c fora do range")
                flags['open'] = True
        elif not flags['open'] and now > sniper_deadline:
            flags['open'] = True
            log_info(f"Remaining: {rstr} | SNIPER IGNORADO — fora da janela de {SNIPER_WINDOW_S}s")

        # ── 10. CICLO 30s ─────────────────────────
        if rem <= 35.0 and not flags['s35']:
            if u_p is not None and d_p is not None:
                state['c1']['snap_up'] = u_p
                state['c1']['snap_dw'] = d_p
                flags['s35'] = True
                log_info(f"Remaining: {rstr} | SNAPSHOT C1 (35s) — UP: {fc(u_p)} DOWN: {fc(d_p)}")

        if rem <= 30.0 and flags['s35'] and not flags['v30']:
            if u_p is not None and d_p is not None:
                vol_up_c = abs(state['c1']['snap_up'] - u_p) * 100
                vol_dw_c = abs(state['c1']['snap_dw'] - d_p) * 100
                vol_c = max(vol_up_c, vol_dw_c)
                peg_c = (u_p + d_p) * 100
                peg_ok = peg_c >= CYCLE_PEG_MIN_C
                flags['v30'] = True
                log_info(
                    f"Remaining: {rstr} | VOL C1 (30s) — UP_mov: {vol_up_c:.1f}c DOWN_mov: {vol_dw_c:.1f}c "
                    f"Max: {vol_c:.1f}c | PEG: {u_p + d_p:.3f} {'OK' if peg_ok else '*** ILÍQUIDO ***'}"
                )
                state['c1']['vol_c'] = vol_c
                state['c1']['peg_ok'] = peg_ok
                state['c1']['u_p'] = u_p
                state['c1']['d_p'] = d_p

        if rem <= 29.8 and flags['v30'] and not flags['d29']:
            if u_p is not None and d_p is not None:
                side = 'UP' if u_p > d_p else 'DOWN'
                nom = u_p if side == 'UP' else d_p
                vol_c = state['c1'].get('vol_c', 999)
                peg_ok = state['c1'].get('peg_ok', False)
                if vol_c >= CYCLE_VOL_MAX_C:
                    log_info(f"Remaining: {rstr} | ABORT C1 — VOLATILIDADE EXTREMA {vol_c:.1f}c")
                elif not peg_ok:
                    peg_dec = state['c1'].get('u_p', 0) + state['c1'].get('d_p', 0)
                    log_info(f"Remaining: {rstr} | ABORT C1 — MERCADO ILÍQUIDO PEG {peg_dec:.3f}")
                else:
                    open_trade(side, nom, 'Cycle_30s', rstr)
            flags['d29'] = True

        # ── 11. CICLO 20s ─────────────────────────
        if rem <= 25.0 and not flags['s25']:
            if u_p is not None and d_p is not None:
                state['c2']['snap_up'] = u_p
                state['c2']['snap_dw'] = d_p
                flags['s25'] = True
                log_info(f"Remaining: {rstr} | SNAPSHOT C2 (25s) — UP: {fc(u_p)} DOWN: {fc(d_p)}")

        if rem <= 20.0 and flags['s25'] and not flags['v20']:
            if u_p is not None and d_p is not None:
                vol_up_c = abs(state['c2']['snap_up'] - u_p) * 100
                vol_dw_c = abs(state['c2']['snap_dw'] - d_p) * 100
                vol_c = max(vol_up_c, vol_dw_c)
                peg_c = (u_p + d_p) * 100
                peg_ok = peg_c >= CYCLE_PEG_MIN_C
                flags['v20'] = True
                log_info(
                    f"Remaining: {rstr} | VOL C2 (20s) — UP_mov: {vol_up_c:.1f}c DOWN_mov: {vol_dw_c:.1f}c "
                    f"Max: {vol_c:.1f}c | PEG: {u_p + d_p:.3f} {'OK' if peg_ok else '*** ILÍQUIDO ***'}"
                )
                state['c2']['vol_c'] = vol_c
                state['c2']['peg_ok'] = peg_ok
                state['c2']['u_p'] = u_p
                state['c2']['d_p'] = d_p

        if rem <= 19.8 and flags['v20'] and not flags['d19']:
            if u_p is not None and d_p is not None:
                side = 'UP' if u_p > d_p else 'DOWN'
                nom = u_p if side == 'UP' else d_p
                vol_c = state['c2'].get('vol_c', 999)
                peg_ok = state['c2'].get('peg_ok', False)
                if vol_c >= CYCLE_VOL_MAX_C:
                    log_info(f"Remaining: {rstr} | ABORT C2 — VOLATILIDADE EXTREMA {vol_c:.1f}c")
                elif not peg_ok:
                    peg_dec = state['c2'].get('u_p', 0) + state['c2'].get('d_p', 0)
                    log_info(f"Remaining: {rstr} | ABORT C2 — MERCADO ILÍQUIDO PEG {peg_dec:.3f}")
                else:
                    open_trade(side, nom, 'Cycle_20s', rstr)
            flags['d19'] = True

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
async def main():
    global daily_profit, last_day, price_change, bankroll
    log_info("BOT INICIADO")
    while True:
        slug, start_ts = get_current_slug()
        meta = fetch_metadata(slug)
        if not meta:
            await asyncio.sleep(1)
            continue

        market_day = datetime.fromtimestamp(start_ts).date()
        if last_day is None or market_day != last_day:
            daily_profit = 0.0
            last_day = market_day
            log_info(f"Novo Dia Iniciado: {market_day}")

        best_asks['up'] = best_asks['down'] = None
        price_change = asyncio.Event()
        ws_task = asyncio.create_task(ws_handler(meta['up'], meta['down']))
        await asyncio.sleep(1.0)

        if best_asks['up'] is not None:
            pre_bank = bankroll
            await logic_loop(start_ts, start_ts + 300, meta)
            profit_this = bankroll - pre_bank
            daily_profit += profit_this
            log_info(
                f"ROUND PnL: ${profit_this:+.4f} | "
                f"Lucro Diário: ${daily_profit:+.4f} | "
                f"Banca: ${bankroll:.2f}"
            )
        else:
            log_info(f"SKIP: Sem dados de preço para {slug}")

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
