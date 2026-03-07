import asyncio
import websockets
import json
import time
import logging
import requests
import os
from datetime import datetime
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType

# Logging formatado: [timestamp] Conteúdo (Espaço único após o timestamp)
logging.basicConfig(filename='bot_xrp.log', level=logging.INFO, format='%(message)s')

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
TAKER_FEE = 0.02
EXIT_CEILING = 0.95

OPEN_RISK, OPEN_TARGET, MIN_OPEN_PRICE = 0.10, 1.10, 0.35
INTERVAL_RISK, CYCLE_TARGET = 0.10, 1.04
MIN_INT_PRICE, MAX_INT_PRICE = 0.76, 0.94

best_asks = {'up': None, 'down': None}
creds = ApiCreds(credenciais.get("API_KEY"), credenciais.get("API_SECRET"), credenciais.get("API_PASSPHRASE"))
client = ClobClient("https://clob.polymarket.com", chain_id=137, key=credenciais.get("PRIVATE_KEY"), creds=creds)

def get_log_time():
    # [timestamp] (espaço)
    return datetime.now().strftime("[%d/%m/%y | %H:%M:%S.%f")[:-3] + "]"

def fetch_metadata(slug):
    try:
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        resp = requests.get(url, timeout=5).json()
        data = resp[0]['markets'][0]
        clob_ids = json.loads(data['clobTokenIds'])
        return {'id': data['conditionId'], 'up': clob_ids[0], 'down': clob_ids[1]}
    except: return None

async def ws_handler(t_up, t_down):
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    async with websockets.connect(uri) as ws:
        await ws.send(json.dumps({"assets_ids": [t_up, t_down], "type": "market", "custom_feature_enabled": True}))
        while True:
            try:
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
            except: break

async def logic_loop(m_start, m_end, t_up, t_down, slug):
    global bankroll
    active_trades = [] 
    ref_up = ref_dw = None
    tried_open = tried_30 = tried_20 = False
    snap_35 = snap_25 = False
    
    while True:
        now = time.time()
        rem = m_end - now
        
        # 1. ENTRADA DE ABERTURA (Buffer 500ms)
        # Só tenta se o mercado começou há menos de 5 segundos
        if not tried_open and (m_start + 0.500) <= now <= (m_start + 5.0):
            u_p, d_p = best_asks['up'], best_asks['down']
            if u_p and d_p:
                for side, price in [('UP', u_p), ('DOWN', d_p)]:
                    p_real = price * (1 + TAKER_FEE)
                    if p_real >= MIN_OPEN_PRICE:
                        invested = bankroll * OPEN_RISK
                        shares = invested / p_real
                        ls = min(0.99, p_real * OPEN_TARGET)
                        active_trades.append({'side': side, 'entry': p_real, 'shares': shares, 'cost': invested, 'type': 'Opening', 'limit': ls})
                        bankroll -= invested
                        logging.info(f"{get_log_time()} EXECUTE (Entry Open) {side} Shares: {shares:.2f} a {price:.2f} (Real: {p_real:.2f}) | Limit Sell Set: {ls:.2f}")
                tried_open = True
                logging.info(f"{get_log_time()} DECISION Sniper Abertura completo. Bank: ${bankroll:.2f}")
            elif now > m_start + 5.0:
                tried_open = True
        elif now > m_start + 5.0:
            tried_open = True

        # 2. MONITORIZAÇÃO DE SAÍDAS (HFT - 0ms delay)
        for trade in active_trades[:]:
            cp = best_asks['up'] if trade['side'] == 'UP' else best_asks['down']
            if cp:
                if cp >= trade['limit'] or cp >= EXIT_CEILING:
                    payout = (trade['shares'] * cp) * (1 - TAKER_FEE)
                    net_prof = payout - trade['cost']
                    bankroll += payout
                    logging.info(f"{get_log_time()} EXIT (Limit/95c) {trade['side']} ({trade['type']}) Shares: {trade['shares']:.2f} a {cp:.2f} Lucro Neto: ${net_prof:.2f} Bank: ${bankroll:.2f}")
                    active_trades.remove(trade)

        # 3. CICLOS DE VOLATILIDADE (Independente da Entry)
        # Snapshot 35s
        if 34.90 <= rem <= 35.10 and not snap_35 and best_asks['up']:
            ref_up, ref_dw = best_asks['up']*(1+TAKER_FEE), best_asks['down']*(1+TAKER_FEE)
            logging.info(f"{get_log_time()} SNAPSHOT (35s) UP: {ref_up:.2f}, DW: {ref_dw:.2f}")
            snap_35 = True

        # Decisão 30s
        if 29.90 <= rem <= 30.10 and not tried_30 and ref_up:
            u_p, d_p = best_asks['up'], best_asks['down']
            cur_p = u_p if (u_p or 0) > (d_p or 0) else d_p
            u_real = cur_p * (1 + TAKER_FEE)
            peg = (best_asks['up'] or 0) + (best_asks['down'] or 0)
            if abs(ref_up - u_real) < 0.50 and peg >= 0.98 and MIN_INT_PRICE <= u_real <= MAX_INT_PRICE:
                invested = bankroll * INTERVAL_RISK
                sh = invested / u_real
                ls = min(0.99, u_real * CYCLE_TARGET)
                side = 'UP' if (u_p or 0) > (d_p or 0) else 'DOWN'
                active_trades.append({'side': side, 'entry': u_real, 'shares': sh, 'cost': invested, 'type': 'Cycle_30s', 'limit': ls})
                bankroll -= invested
                logging.info(f"{get_log_time()} DECISION EXECUTE (30s) {side} Shares: {sh:.2f} a {cur_p:.2f} (Real: {u_real:.2f}) | Limit Set: {ls:.2f}")
            tried_30 = True

        # Snapshot 25s
        if 24.90 <= rem <= 25.10 and not snap_25 and best_asks['up']:
            ref_up, ref_dw = best_asks['up']*(1+TAKER_FEE), best_asks['down']*(1+TAKER_FEE)
            logging.info(f"{get_log_time()} SNAPSHOT (25s) UP: {ref_up:.2f}, DW: {ref_dw:.2f}")
            snap_25 = True

        # Decisão 20s
        if 19.90 <= rem <= 20.10 and not tried_20 and ref_up:
            u_p, d_p = best_asks['up'], best_asks['down']
            cur_p = u_p if (u_p or 0) > (d_p or 0) else d_p
            u_real = cur_p * (1 + TAKER_FEE)
            peg = (best_asks['up'] or 0) + (best_asks['down'] or 0)
            if abs(ref_up - u_real) < 0.50 and peg >= 0.98 and MIN_INT_PRICE <= u_real <= MAX_INT_PRICE:
                invested = bankroll * INTERVAL_RISK
                sh = invested / u_real
                ls = min(0.99, u_real * CYCLE_TARGET)
                side = 'UP' if (u_p or 0) > (d_p or 0) else 'DOWN'
                active_trades.append({'side': side, 'entry': u_real, 'shares': sh, 'cost': invested, 'type': 'Cycle_20s', 'limit': ls})
                bankroll -= invested
                logging.info(f"{get_log_time()} DECISION EXECUTE (20s) {side} Shares: {sh:.2f} a {cur_p:.2f} (Real: {u_real:.2f}) | Limit Set: {ls:.2f}")
            tried_20 = True

        if rem <= 0:
            for trade in active_trades:
                f_p = (best_asks['up'] if trade['side'] == 'UP' else best_asks['down']) or 0.0
                pay = (trade['shares'] * f_p) * (1 - TAKER_FEE)
                bankroll += pay
                logging.info(f"{get_log_time()} RESULT {trade['side']} ({trade['type']}) Shares: {trade['shares']:.2f} Final: {f_p:.2f} Bank: ${bankroll:.2f}")
            logging.info(f"{get_log_time()} MARKET FINISHED")
            break

        await asyncio.sleep(0) # Velocidade máxima

async def main():
    global bankroll
    while True:
        now = time.time()
        current_market_start = now - (now % 300)
        next_market_start = current_market_start + 300
        
        # Decide se entra no mercado atual ou espera pelo próximo
        if (next_market_start - now) > 10:
            start_ts = current_market_start
        else:
            start_ts = next_market_start
            
        slug = f"xrp-updown-5m-{int(start_ts)}"
        meta = None
        while not meta:
            meta = fetch_metadata(slug)
            if not meta: await asyncio.sleep(1)

        logging.info("=========================================================================")
        logging.info(f"Market: {slug} | URL: https://polymarket.com/event/{slug}")
        logging.info(f"UP: {meta['up']} | DW: {meta['down']} | Cond: {meta['id']} | Bank: ${bankroll:.2f}")
        logging.info("=========================================================================")

        best_asks['up'] = best_asks['down'] = None
        ws_task = asyncio.create_task(ws_handler(meta['up'], meta['down']))
        
        # O logic_loop agora é chamado e ele próprio gere o tempo
        await logic_loop(start_ts, start_ts + 300, meta['up'], meta['down'], slug)
        
        ws_task.cancel()
        await asyncio.sleep(0.5)

if __name__ == "__main__":
    asyncio.run(main())