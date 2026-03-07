import asyncio
import websockets
import json
import time
import logging
import requests
import os
import csv
from datetime import datetime
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType

# Logging Descritivo
logging.basicConfig(filename='bot_xrp.log', level=logging.INFO, format='%(message)s')

def load_secrets(filepath="secrets.txt"):
    secrets = {}
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"ERRO: secrets.txt nao encontrado!")
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line: continue
            key, value = line.split("=", 1)
            secrets[key.strip()] = value.strip()
    return secrets

def log_to_csv(slug, event, side, detail, up_p, dw_p, up_ref, dw_ref, amount, b_before, b_after, payout):
    file_exists = os.path.isfile('trading_report.csv')
    with open('trading_report.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow(['Timestamp', 'Market', 'Event', 'Side', 'Detail', 'UP_Real', 'DW_Real', 'UP_Ref', 'DW_Ref', 'Invested', 'Bank_Before', 'Bank_After', 'Payout'])
        writer.writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3], slug, event, side, detail, up_p, dw_p, up_ref, dw_ref, f"{amount:.2f}", f"{b_before:.2f}", f"{b_after:.2f}", f"{payout:.2f}"])

# Configurações
credenciais = load_secrets()
bankroll = 100.0
RISK_PERCENT = 0.10
TAKER_FEE = 0.02
MIN_BUY_PRICE_REAL = 0.76
MAX_BUY_PRICE_REAL = 0.94
EXIT_PRICE = 0.98

best_asks = {'up': None, 'down': None}
creds = ApiCreds(credenciais.get("API_KEY"), credenciais.get("API_SECRET"), credenciais.get("API_PASSPHRASE"))
client = ClobClient("https://clob.polymarket.com", chain_id=137, key=credenciais.get("PRIVATE_KEY"), creds=creds)

def get_log_time():
    return datetime.now().strftime("[%d/%m/%y | %H:%M:%S.%f")[:-3] + "]"

def fetch_metadata(slug):
    try:
        resp = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}", timeout=5)
        data = resp.json()[0]['markets'][0]
        clob_ids = json.loads(data['clobTokenIds'])
        return {'id': data['conditionId'], 'up': clob_ids[0], 'down': clob_ids[1]}
    except: return None

async def ws_handler(t_up, t_down):
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                await ws.send(json.dumps({"assets_ids": [t_up, t_down], "type": "market", "custom_feature_enabled": True}))
                while True:
                    data = json.loads(await ws.recv())
                    items = data if isinstance(data, list) else [data]
                    for item in items:
                        aid, p = item.get("asset_id"), None
                        if item.get("event_type") == "book" and item.get("asks"):
                            p = min(float(d['price']) for d in item["asks"] if float(d['size']) > 0)
                        elif item.get("event_type") == "best_bid_ask" and item.get("best_ask"):
                            p = float(item["best_ask"])
                        if p:
                            if aid == t_up: best_asks['up'] = p
                            elif aid == t_down: best_asks['down'] = p
        except asyncio.CancelledError: break
        except: await asyncio.sleep(1)

async def logic_loop(m_end, t_up, t_down, slug):
    global bankroll
    active_trade = None
    ref_up = ref_dw = None
    tried_30 = tried_20 = False
    last_log_time = 0

    while True:
        now = time.time()
        rem = m_end - now
        
        if rem <= 0:
            if active_trade:
                final_p = best_asks['up'] if active_trade['side'] == "UP" else best_asks['down']
                final_p = final_p if final_p is not None else 0.0
                bank_before = bankroll
                payout = active_trade['shares'] * final_p
                bankroll += payout
                status = "WIN" if final_p > 0.50 else "LOSS"
                logging.info(f"{get_log_time()} | RESULT | {status}: Preco Final {final_p:.2f} | Payout: ${payout:.2f} | Banca Final: ${bankroll:.2f}")
                log_to_csv(slug, "RESULT", active_trade['side'], status, final_p, 0, ref_up, ref_dw, 0, bank_before, bankroll, payout)
            logging.info(f"{get_log_time()} | MARKET FINISHED")
            break

        # TAKE PROFIT (98c)
        if active_trade:
            current_p = best_asks['up'] if active_trade['side'] == "UP" else best_asks['down']
            if current_p and current_p >= EXIT_PRICE:
                bank_before = bankroll
                payout = active_trade['shares'] * EXIT_PRICE
                bankroll += payout
                logging.info(f"{get_log_time()} | EXIT | Take Profit atingido ({int(EXIT_PRICE*100)}c) | Payout: ${payout:.2f} | Banca: ${bankroll:.2f}")
                log_to_csv(slug, "EXIT", active_trade['side'], "Take Profit", current_p, 0, ref_up, ref_dw, 0, bank_before, bankroll, payout)
                active_trade = None

        # LOGS DINÂMICOS
        log_interval = 10.0 if rem > 40 else 0.5
        if now - last_log_time >= log_interval:
            u_p, d_p = best_asks['up'], best_asks['down']
            u_r = f"{int(u_p*100)}c (Real: {int(u_p*(1+TAKER_FEE)*100)}c)" if u_p else "N/A"
            d_r = f"{int(d_p*100)}c (Real: {int(d_p*(1+TAKER_FEE)*100)}c)" if d_p else "N/A"
            logging.info(f"{get_log_time()} | Remaining: {int(rem)}s | UP: {u_r} | DOWN: {d_r} | Bank: ${bankroll:.2f}")
            last_log_time = now

        # SNAPSHOTS (35s e 25s)
        if not active_trade:
            # Snapshot 35s
            if 34.95 <= rem <= 35.05 and ref_up is None and not tried_30:
                if best_asks['up'] and best_asks['down']:
                    ref_up, ref_dw = best_asks['up']*(1+TAKER_FEE), best_asks['down']*(1+TAKER_FEE)
                    logging.info(f"{get_log_time()} | SNAPSHOT (35s) | Precos Ref: UP {ref_up:.2f}, DW {ref_dw:.2f}")
            
            # Novo Snapshot 25s (apenas se o ciclo de 30s falhou)
            if 24.95 <= rem <= 25.05 and tried_30 and not tried_20 and (ref_up is None or tried_30):
                if best_asks['up'] and best_asks['down']:
                    ref_up, ref_dw = best_asks['up']*(1+TAKER_FEE), best_asks['down']*(1+TAKER_FEE)
                    logging.info(f"{get_log_time()} | SNAPSHOT (25s) | Novos Precos Ref: UP {ref_up:.2f}, DW {ref_dw:.2f}")

        # DECISÕES (30s e 20s)
        for cp in [30, 20]:
            if (cp - 0.05) <= rem <= (cp + 0.05) and not active_trade:
                if (cp == 30 and tried_30) or (cp == 20 and tried_20): continue
                
                if cp == 30: tried_30 = True
                if cp == 20: tried_20 = True
                
                u_p, d_p = best_asks['up'], best_asks['down']
                if u_p and ref_up:
                    u_real, d_real = u_p*(1+TAKER_FEE), d_p*(1+TAKER_FEE)
                    u_mov, d_mov = abs(ref_up - u_real), abs(ref_dw - d_real)
                    peg, cur_max_real = u_p + d_p, max(u_real, d_real)
                    
                    # Verificação
                    vol_ok = u_mov < 0.50 and d_mov < 0.50
                    peg_ok = peg >= 0.98
                    price_ok = MIN_BUY_PRICE_REAL <= cur_max_real <= MAX_BUY_PRICE_REAL
                    
                    if not vol_ok: reason = f"Volatilidade alta (U:{u_mov:.2f} D:{d_mov:.2f} > 0.50)"
                    elif not peg_ok: reason = f"Liquidez/Peg baixo (Soma:{peg:.2f} < 0.98)"
                    elif cur_max_real < MIN_BUY_PRICE_REAL: reason = f"Preco baixo (Floor: {cur_max_real:.2f} < {MIN_BUY_PRICE_REAL})"
                    elif cur_max_real > MAX_BUY_PRICE_REAL: reason = f"Preco alto (Ceiling: {cur_max_real:.2f} > {MAX_BUY_PRICE_REAL})"
                    else: reason = None

                    if reason:
                        logging.info(f"{get_log_time()} | DECISION | Skip ({cp}s): {reason}")
                    else:
                        bank_before = bankroll
                        trade_amt = bankroll * RISK_PERCENT
                        side = "UP" if u_p > d_p else "DOWN"
                        shares = trade_amt / cur_max_real
                        bankroll -= trade_amt
                        active_trade = {'side': side, 'shares': shares}
                        
                        logging.info(f"{get_log_time()} | DECISION | EXECUTE ({cp}s) | {side} a {cur_max_real:.2f} | Peg {peg:.2f} OK | Vol {max(u_mov, d_mov):.2f} OK")
                        logging.info(f"{get_log_time()} | BANK | Investido: ${trade_amt:.2f} | Restante: ${bankroll:.2f}")
                        log_to_csv(slug, "EXECUTE", side, f"Entry {cp}s", u_real, d_real, ref_up, ref_dw, trade_amt, bank_before, bankroll, 0)
                        break

        await asyncio.sleep(0)

async def main():
    while True:
        now = time.time()
        start_ts = now - (now % 300)
        slug = f"xrp-updown-5m-{int(start_ts)}"
        meta = None
        while not meta:
            meta = fetch_metadata(slug)
            if not meta: await asyncio.sleep(0.5)
        
        logging.info("=========================================================================")
        logging.info(f"Market: {slug} | URL: https://polymarket.com/event/{slug}")
        logging.info(f"UP: {meta['up']} | DW: {meta['down']} | Cond: {meta['id']}")
        logging.info("=========================================================================")
        
        best_asks['up'] = best_asks['down'] = None
        ws_t = asyncio.create_task(ws_handler(meta['up'], meta['down']))
        logic_t = asyncio.create_task(logic_loop(start_ts + 300, meta['up'], meta['down'], slug))
        await logic_t
        ws_t.cancel()
        try: await asyncio.wait_for(ws_t, timeout=0.2)
        except: pass
        await asyncio.sleep(0.1)

if __name__ == "__main__":
    asyncio.run(main())