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

# Logging configurado exatamente como pediste
logging.basicConfig(filename='bot_xrp.log', level=logging.INFO, format='%(message)s')

def load_secrets(filepath="secrets.txt"):
    secrets = {}
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if "=" in line:
                k, v = line.split("=", 1)
                secrets[k.strip()] = v.strip()
    return secrets

# Setup Inicial
credenciais = load_secrets()
bankroll = 10.0
TRADE_RISK = 0.10 
TAKER_FEE = 0.02
PROFIT_TARGET = 1.10 # +10%
STOP_LOSS = 0.97 # -3% (Proteção contra quedas bruscas)

best_asks = {'up': None, 'down': None}
creds = ApiCreds(credenciais.get("API_KEY"), credenciais.get("API_SECRET"), credenciais.get("API_PASSPHRASE"))
client = ClobClient("https://clob.polymarket.com", chain_id=137, key=credenciais.get("PRIVATE_KEY"), creds=creds)

def get_log_time():
    return datetime.now().strftime("[%d/%m/%y | %H:%M:%S.%f")[:-3] + "]"

def format_rem_precision(seconds):
    m, s = divmod(max(0, seconds), 60)
    ms = int((seconds - int(seconds)) * 1000)
    return f"{int(m):02d}:{int(s):02d}:{abs(ms):03d}"

async def ws_handler(t_up, t_down):
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
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

async def logic_loop(m_start, m_end, t_up, t_down, slug):
    global bankroll
    trades = {'UP': None, 'DOWN': None}
    
    while True:
        now = time.time()
        rem = m_end - now
        if rem <= 0: break

        # 1. COMPRA IMEDIATA (DUAL ENTRY)
        if now >= m_start and not trades['UP'] and not trades['DOWN']:
            u_p, d_p = best_asks['up'], best_asks['down']
            if u_p and d_p:
                for side, price in [('UP', u_p), ('DOWN', d_p)]:
                    p_real = price * (1 + TAKER_FEE)
                    invested = bankroll * TRADE_RISK
                    trades[side] = {'entry': p_real, 'shares': invested / p_real, 'cost': invested}
                    logging.info(f"{get_log_time()} EXECUTE (Entry) {side} Price: {price:.2f} (Real: {p_real:.2f})")
                bankroll -= (bankroll * TRADE_RISK * 2)

        # 2. MONITORIZAÇÃO ULTRA-RÁPIDA (PROTEÇÃO E TAKE PROFIT)
        if trades['UP'] or trades['DOWN']:
            for side in ['UP', 'DOWN']:
                if trades[side]:
                    curr_p = best_asks['up'] if side == 'UP' else best_asks['down']
                    if curr_p:
                        # GATILHO 1: TAKE PROFIT (+10%)
                        if curr_p >= (trades[side]['entry'] * PROFIT_TARGET):
                            payout = trades[side]['shares'] * curr_p * (1 - TAKER_FEE)
                            bankroll += payout
                            logging.info(f"{get_log_time()} EXIT (Profit) {side} at {curr_p:.2f} | Payout: ${payout:.2f} | Bank: ${bankroll:.2f}")
                            trades[side] = None

                        # GATILHO 2: PANIC SELL (Se cair 10% do valor de compra para evitar o prejuízo que tiveste)
                        elif curr_p <= (trades[side]['entry'] * STOP_LOSS):
                            payout = trades[side]['shares'] * curr_p * (1 - TAKER_FEE)
                            bankroll += payout
                            logging.info(f"{get_log_time()} EXIT (Panic) {side} at {curr_p:.2f} | Salvaguarda: ${payout:.2f} | Bank: ${bankroll:.2f}")
                            trades[side] = None

        # Loop de microssegundos (yield control)
        await asyncio.sleep(0)

    # 3. RESULTADO NO FINAL DO MERCADO (Se sobrar algo)
    for side in ['UP', 'DOWN']:
        if trades[side]:
            final_p = best_asks['up'] if side == 'UP' else best_asks['down']
            final_p = final_p if final_p else 0.0
            payout = trades[side]['shares'] * final_p
            bankroll += payout
            status = "WIN" if final_p > 0.5 else "LOSS"
            logging.info(f"{get_log_time()} RESULT {status} {side} Final Price: {final_p:.2f} | Bank: ${bankroll:.2f}")
    
    logging.info(f"{get_log_time()} MARKET FINISHED")

async def main():
    global bankroll
    while True:
        now = time.time()
        start_ts = now - (now % 300)
        slug = f"xrp-updown-5m-{int(start_ts)}"
        
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        meta = None
        while not meta:
            try:
                data = requests.get(url).json()[0]['markets'][0]
                clob_ids = json.loads(data['clobTokenIds'])
                meta = {'id': data['conditionId'], 'up': clob_ids[0], 'down': clob_ids[1]}
            except: await asyncio.sleep(0.5)

        logging.info("=========================================================================")
        logging.info(f"Market: {slug} | URL: https://polymarket.com/event/{slug}")
        logging.info(f"UP: {meta['up']} | DW: {meta['down']} | Cond: {meta['id']} | Bank: ${bankroll:.2f}")
        logging.info("=========================================================================")

        ws_task = asyncio.create_task(ws_handler(meta['up'], meta['down']))
        await logic_loop(start_ts, start_ts + 300, meta['up'], meta['down'], slug)
        ws_task.cancel()
        await asyncio.sleep(0.1)

if __name__ == "__main__":
    asyncio.run(main())