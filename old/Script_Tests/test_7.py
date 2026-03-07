import asyncio
import websockets
import json
import time
import logging
import requests
import os
from datetime import datetime

# Configuração de Log: Exclusivo no ficheiro bot_xrp.log
logging.basicConfig(
    filename='bot_xrp.log',
    level=logging.INFO,
    format='%(message)s'
)

def load_secrets(filepath="secrets.txt"):
    secrets = {}
    if not os.path.exists(filepath): return {}
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if "=" in line:
                k, v = line.split("=", 1)
                secrets[k.strip()] = v.strip()
    return secrets

# Configurações de Estratégia
credenciais = load_secrets()
bankroll = 10.0
MIN_OPEN_PRICE = 0.48
best_asks = {'up': None, 'down': None}
daily_profit = 0.0
last_day = None

def get_fee_rate(p):
    return 0.25 * (p * (1 - p)) ** 2

def get_ts():
    return datetime.now().strftime("[%y/%d/%m | %H:%M:%S.%f")[:-3] + "]"

def log_info(msg):
    logging.info(f"[INFO] {get_ts()} {msg}")

def get_remaining_str(rem):
    if rem < 0: rem = 0
    mins = int(rem // 60)
    secs = int(rem % 60)
    msecs = int((rem * 1000) % 1000)
    return f"{mins:02d}:{secs:02d}:{msecs:03d}"

def fetch_metadata(slug):
    try:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        resp = requests.get(url, timeout=5).json()
        data = resp[0]['markets'][0]
        clob_ids = json.loads(data['clobTokenIds'])
        return {'id': data['conditionId'], 'up': clob_ids[0], 'down': clob_ids[1], 'slug': slug}
    except: return None

async def ws_handler(t_up, t_down):
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    try:
        async with websockets.connect(uri) as ws:
            await ws.send(json.dumps({"assets_ids": [t_up, t_down], "type": "market", "custom_feature_enabled": True}))
            while True:
                data = json.loads(await ws.recv())
                items = data if isinstance(data, list) else [data]
                for item in items:
                    aid, p = item.get("asset_id"), None
                    if item.get("event_type") == "book" and item.get("asks"):
                        valid = [float(d['price']) for d in item["asks"] if float(d['size']) > 0]
                        if valid: p = min(valid)
                    elif item.get("event_type") == "best_bid_ask" and item.get("best_ask"):
                        p = float(item["best_ask"])
                    if p:
                        if aid == t_up: best_asks['up'] = p
                        elif aid == t_down: best_asks['down'] = p
    except: pass

async def logic_loop(m_start, m_end, meta):
    global bankroll
    active_trades = []
    # Estados de Snapshot e Volatilidade
    state = {
        'c1': {'snap_up': None, 'snap_dw': None, 'vol': 0.0}, # Ciclo 30s
        'c2': {'snap_up': None, 'snap_dw': None, 'vol': 0.0}  # Ciclo 20s
    }
    flags = {
        's35': False, 'v30': False, 'd29': False,  # Gatilhos Ciclo 1
        's25': False, 'v20': False, 'd19': False,  # Gatilhos Ciclo 2
        'open': False
    }
    last_ticker_time = 0
    logging.info("="*73)
    log_info(f"Market: {meta['slug']} | URL: https://polymarket.com/event/{meta['slug']}")
    logging.info(f"[INFO] {get_ts()} UP: {meta['up']} | DW: {meta['down']}")
    logging.info(f"[INFO] {get_ts()} Cond: {meta['id']} | Bank: ${bankroll:.2f} | Profit Acumulado: ${daily_profit:.2f}")
    logging.info("="*73)
    log_info(">>> ESCUTA ATIVA")
    while True:
        now = time.time()
        rem = m_end - now
        rem_str = get_remaining_str(rem)

        # 1. TICKER 100ms
        if now - last_ticker_time >= 0.1:
            u_p, d_p = best_asks['up'] or 0.0, best_asks['down'] or 0.0
            u_real = u_p * (1 + get_fee_rate(u_p))
            d_real = d_p * (1 + get_fee_rate(d_p))
            logging.info(f"{get_ts()} Remaining: {rem_str} | UP: {u_p:.2f} (Real: {u_real:.2f}) | DOWN: {d_p:.2f} (Real: {d_real:.2f})")
            last_ticker_time = now

        # 2. SNIPER ABERTURA (Filtro 48c)
        if not flags['open'] and (now <= m_start + 5.0):
            u_p, d_p = best_asks['up'], best_asks['down']
            if u_p and d_p:
                for side, price in [('UP', u_p), ('DOWN', d_p)]:
                    fee = get_fee_rate(price)
                    p_real = price * (1 + fee)
                    if p_real >= MIN_OPEN_PRICE:
                        invested = bankroll * 0.10
                        sh = invested / p_real
                        bankroll -= invested
                        target = round(min(0.99, p_real * 1.10), 2)
                        log_info(f"Remaining: {rem_str} | Compra Sniper: {side} a {price:.2f} | Numero Accoes: {sh:.2f} | Banca: ${bankroll:.2f}")
                        log_info(f"Remaining: {rem_str} | SET LIMIT ORDER SELL: {side} a {target:.2f} (+10%)")
                        active_trades.append({'side': side, 'entry': p_real, 'shares': sh, 'target': target, 'type': 'Opening'})
                    else:
                        log_info(f"SNIPER SKIP: {side} - Preço real {p_real:.2f} abaixo do mínimo de {MIN_OPEN_PRICE}")
                flags['open'] = True

        # --- BLOCO CICLO 30s ---
        if rem <= 35.0 and not flags['s35']:
            if best_asks['up'] and best_asks['down']:
                u_fee = get_fee_rate(best_asks['up'])
                d_fee = get_fee_rate(best_asks['down'])
                state['c1']['snap_up'] = best_asks['up'] * (1 + u_fee)
                state['c1']['snap_dw'] = best_asks['down'] * (1 + d_fee)
                flags['s35'] = True
                log_info(f"Remaining: {rem_str} | SNAPSHOT (35s) Capturado.")

        if rem <= 30.0 and flags['s35'] and not flags['v30']:
            u_p, d_p = best_asks['up'], best_asks['down']
            if u_p and d_p:
                u_real = u_p * (1 + get_fee_rate(u_p))
                d_real = d_p * (1 + get_fee_rate(d_p))
                state['c1']['vol'] = max(abs(state['c1']['snap_up'] - u_real), abs(state['c1']['snap_dw'] - d_real))
                flags['v30'] = True
                log_info(f"Remaining: {rem_str} | CALCULO VOL (30s): {state['c1']['vol']:.2f}")

        if rem <= 29.8 and flags['v30'] and not flags['d29']:
            u_p, d_p = best_asks['up'], best_asks['down']
            if u_p and d_p:
                side = 'UP' if u_p > d_p else 'DOWN'
                cur_p = u_p if side == 'UP' else d_p
                fee = get_fee_rate(cur_p)
                u_real = cur_p * (1 + fee)
                peg = u_p + d_p
                if (0.76 <= u_real <= 0.94) and peg >= 0.98 and state['c1']['vol'] < 0.50:
                    invested = bankroll * 0.10
                    sh = invested / u_real
                    bankroll -= invested
                    target = round(min(0.99, u_real * 1.04), 2)
                    log_info(f"Remaining: {rem_str} | DECISAO EXECUTE (30s): {side} a {cur_p:.2f} | Vol: {state['c1']['vol']:.2f}")
                    log_info(f"Remaining: {rem_str} | SET LIMIT ORDER SELL: {side} a {target:.2f} (+4%)")
                    active_trades.append({'side': side, 'entry': u_real, 'shares': sh, 'target': target, 'type': 'Cycle_30s'})
                else:
                    log_info(f"Remaining: {rem_str} | DECISAO SKIP (30s): P:{u_real:.2f} Peg:{peg:.2f} Vol:{state['c1']['vol']:.2f}")
            flags['d29'] = True

        # --- BLOCO CICLO 20s ---
        if rem <= 25.0 and not flags['s25']:
            if best_asks['up'] and best_asks['down']:
                u_fee = get_fee_rate(best_asks['up'])
                d_fee = get_fee_rate(best_asks['down'])
                state['c2']['snap_up'] = best_asks['up'] * (1 + u_fee)
                state['c2']['snap_dw'] = best_asks['down'] * (1 + d_fee)
                flags['s25'] = True
                log_info(f"Remaining: {rem_str} | SNAPSHOT (25s) Capturado.")

        if rem <= 20.0 and flags['s25'] and not flags['v20']:
            u_p, d_p = best_asks['up'], best_asks['down']
            if u_p and d_p:
                u_real = u_p * (1 + get_fee_rate(u_p))
                d_real = d_p * (1 + get_fee_rate(d_p))
                state['c2']['vol'] = max(abs(state['c2']['snap_up'] - u_real), abs(state['c2']['snap_dw'] - d_real))
                flags['v20'] = True
                log_info(f"Remaining: {rem_str} | CALCULO VOL (20s): {state['c2']['vol']:.2f}")

        if rem <= 19.8 and flags['v20'] and not flags['d19']:
            u_p, d_p = best_asks['up'], best_asks['down']
            if u_p and d_p:
                side = 'UP' if u_p > d_p else 'DOWN'
                cur_p = u_p if side == 'UP' else d_p
                fee = get_fee_rate(cur_p)
                u_real = cur_p * (1 + fee)
                peg = u_p + d_p
                if (0.76 <= u_real <= 0.94) and peg >= 0.98 and state['c2']['vol'] < 0.50:
                    invested = bankroll * 0.10
                    sh = invested / u_real
                    bankroll -= invested
                    target = round(min(0.99, u_real * 1.04), 2)
                    log_info(f"Remaining: {rem_str} | DECISAO EXECUTE (20s): {side} a {cur_p:.2f} | Vol: {state['c2']['vol']:.2f}")
                    log_info(f"Remaining: {rem_str} | SET LIMIT ORDER SELL: {side} a {target:.2f} (+4%)")
                    active_trades.append({'side': side, 'entry': u_real, 'shares': sh, 'target': target, 'type': 'Cycle_20s'})
                else:
                    log_info(f"Remaining: {rem_str} | DECISAO SKIP (20s): P:{u_real:.2f} Peg:{peg:.2f} Vol:{state['c2']['vol']:.2f}")
            flags['d19'] = True

        # 4. MONITORIZAÇÃO DE SAÍDAS
        for trade in active_trades[:]:
            cp = best_asks['up'] if trade['side'] == 'UP' else best_asks['down']
            if cp:
                trigger_emergency = 0.85 if 'Cycle' in trade['type'] else 0.95
                if cp >= trade['target'] or cp >= trigger_emergency:
                    fee = get_fee_rate(cp)
                    payout = (trade['shares'] * cp) * (1 - fee)
                    bankroll += payout
                    log_info(f"Remaining: {rem_str} | LIMIT ORDER FILLED: {trade['side']} a {cp:.2f} | Banca: ${bankroll:.2f}")
                    active_trades.remove(trade)

        if rem <= 0:
            for trade in active_trades:
                f_p = (best_asks['up'] if trade['side'] == 'UP' else best_asks['down']) or 0.0
                fee = get_fee_rate(f_p)
                bankroll += (trade['shares'] * f_p) * (1 - fee)
            log_info(f"Remaining: 00:00:000 | Fim de Mercado | Bank: ${bankroll:.2f}")
            break
        await asyncio.sleep(0)

async def main():
    global daily_profit, last_day
    log_info("BOT INICIADO")
    while True:
        now = time.time()
        start_ts = now - (now % 300)
        if (start_ts + 300 - now) < 5: start_ts += 300
        slug = f"xrp-updown-5m-{int(start_ts)}"
        meta = fetch_metadata(slug)
        if not meta:
            await asyncio.sleep(1)
            continue
        market_day = datetime.fromtimestamp(start_ts).date()
        if last_day is None or market_day != last_day:
            daily_profit = 0.0
            last_day = market_day
            log_info(f"Novo Dia Iniciado: {market_day}")
        ws_task = asyncio.create_task(ws_handler(meta['up'], meta['down']))
        await asyncio.sleep(1.0) # Estabilização 1s
        if best_asks['up'] is not None:
            pre_bank = bankroll
            await logic_loop(start_ts, start_ts + 300, meta)
            profit_this = bankroll - pre_bank
            daily_profit += profit_this
        ws_task.cancel()
        await asyncio.sleep(0.5)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: pass