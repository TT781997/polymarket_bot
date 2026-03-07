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

# Setup logging to file without timestamps, as we format manually
logging.basicConfig(filename='bot_xrp.log', level=logging.INFO, format='%(message)s')

# --- FUNÇÃO PARA LER OS SECRETS ---
def load_secrets(filepath="secrets.txt"):
    secrets = {}
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"❌ ERRO: O ficheiro '{filepath}' não foi encontrado na pasta do script!")
    
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            # Ignora linhas vazias ou comentários (que começam com #)
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                secrets[key.strip()] = value.strip()
    return secrets

# Carrega os segredos do ficheiro
credenciais = load_secrets()

# API Credentials - Lidas do secrets.txt
HOST = "https://clob.polymarket.com"
CHAIN_ID = 137
PRIVATE_KEY = credenciais.get("PRIVATE_KEY")
API_KEY = credenciais.get("API_KEY")
API_SECRET = credenciais.get("API_SECRET")
API_PASSPHRASE = credenciais.get("API_PASSPHRASE")

# Verifica se alguma chave não foi lida corretamente
if not all([PRIVATE_KEY, API_KEY, API_SECRET, API_PASSPHRASE]):
    raise ValueError("❌ ERRO: Faltam credenciais no ficheiro secrets.txt. Verifica se tens as 4 variáveis corretas.")

# Trading Configurations
FIXED_AMOUNT = 10.0  # Adjust as needed (in USDC)
TAKER_FEE_RATE = 0.02  # 2% Taker Fee da Polymarket para calcular o Real Price

# Global state for best asks
best_asks = {'up': None, 'down': None}

# ClobClient instance
creds = ApiCreds(API_KEY, API_SECRET, API_PASSPHRASE)
client = ClobClient(HOST, chain_id=CHAIN_ID, key=PRIVATE_KEY, creds=creds)

def get_current_market_slug():
    now = int(time.time())
    start = now - (now % 300)
    return f"xrp-updown-5m-{start}"

def fetch_metadata(slug):
    url = f"https://gamma-api.polymarket.com/events?slug={slug}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return None
        event = data[0]
        market = event['markets'][0]  # Assuming single market per event
        outcomes_str = market['outcomes']
        outcomes = json.loads(outcomes_str)
        clob_token_ids_str = market['clobTokenIds']
        clob_token_ids = json.loads(clob_token_ids_str)
        token_up = None
        token_down = None
        for i, name in enumerate(outcomes):
            upper_name = name.upper()
            if upper_name == 'UP':
                token_up = clob_token_ids[i]
            elif upper_name == 'DOWN':
                token_down = clob_token_ids[i]
        condition_id = market['conditionId']
        if token_up and token_down and condition_id:
            return {'up': token_up, 'down': token_down, 'condition_id': condition_id}
        else:
            return None
    except requests.exceptions.RequestException:
        return None

async def market_websocket_handler(token_up, token_down):
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                sub_msg = json.dumps({
                    "assets_ids": [token_up, token_down],
                    "type": "market",
                    "custom_feature_enabled": True
                })
                await ws.send(sub_msg)
                while True:
                    message = await ws.recv()
                    data = json.loads(message)
                    if isinstance(data, list):
                        for item in data:
                            process_item(item, token_up, token_down)
                    else:
                        process_item(data, token_up, token_down)
        except Exception as e:
            logging.error(f"Market WebSocket error: {e}")
            await asyncio.sleep(1)  # Retry after 1 second

def process_item(item, token_up, token_down):
    event_type = item.get("event_type")
    if event_type == "book":
        asset_id = item.get("asset_id")
        asks = item.get("asks", [])
        if asks:
            try:
                best_ask = min(float(d['price']) for d in asks if 'price' in d and isinstance(d['price'], str) and d['price'].replace('.', '', 1).isdigit() and float(d['size']) > 0)
                if asset_id == token_up:
                    best_asks['up'] = best_ask
                elif asset_id == token_down:
                    best_asks['down'] = best_ask
            except ValueError:
                pass  # Skip if no valid prices
    elif event_type == "best_bid_ask":
        asset_id = item.get("asset_id")
        best_ask_str = item.get("best_ask")
        if best_ask_str is not None:
            try:
                best_ask = float(best_ask_str)
                if asset_id == token_up:
                    best_asks['up'] = best_ask
                elif asset_id == token_down:
                    best_asks['down'] = best_ask
            except ValueError:
                pass  # Skip if conversion fails

async def user_websocket_handler(condition_id, token_up, token_down):
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/account"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                sub_msg = json.dumps({
                    "auth": {
                        "apiKey": API_KEY,
                        "secret": API_SECRET,
                        "passphrase": API_PASSPHRASE
                    },
                    "type": "user",
                    "markets": [condition_id]
                })
                await ws.send(sub_msg)
                while True:
                    message = await ws.recv()
                    data = json.loads(message)
                    if data.get("event_type") == "trade" and data.get("status") == "MATCHED" and data.get("side") == "BUY":
                        fill_size = float(data["size"])
                        fill_price = float(data["price"])
                        asset_id = data["asset_id"]
                        # Place proportional sell
                        await asyncio.to_thread(place_sell, asset_id, fill_price + 0.02, fill_size)
                        logging.info(f"Placed sell for {fill_size} shares at {fill_price + 0.02}")
        except Exception as e:
            logging.error(f"User WebSocket error: {e}")
            await asyncio.sleep(1)  # Retry after 1 second

def place_sell(token_id, price, size):
    args = OrderArgs(
        token_id=token_id,
        price=price,
        side="SELL",
        size=size
    )
    #order = client.create_order(args)
    #client.post_order(order, OrderType.GTC)

def place_buy(token_id, price, size):
    args = OrderArgs(
        token_id=token_id,
        price=price,
        side="BUY",
        size=size
    )
    #order = client.create_order(args)
    #client.post_order(order, OrderType.GTC)

async def logic_loop(market_start, market_end, token_up, token_down, condition_id):
    executed = False
    up_at_25 = down_at_25 = None
    up_at_20 = down_at_20 = None
    while True:
        now = time.time()
        if now >= market_end:
            break
        seconds_remaining = int(market_end - now)
        up_price = best_asks['up']
        down_price = best_asks['down']

        current_time_str = datetime.fromtimestamp(now).strftime("%H:%M:%S")

        # Lógica de cálculo do Real Price (Preço + Taker Fee) e formatação
        if up_price is not None:
            up_cent = int(up_price * 100)
            up_real = int(up_price * (1 + TAKER_FEE_RATE) * 100)
            up_display = f"{up_cent}c (Real: {up_real}c)"
        else:
            up_display = 'N/A'

        if down_price is not None:
            down_cent = int(down_price * 100)
            down_real = int(down_price * (1 + TAKER_FEE_RATE) * 100)
            down_display = f"{down_cent}c (Real: {down_real}c)"
        else:
            down_display = 'N/A'

        log_msg = f"{current_time_str} | Remaining: {seconds_remaining}s | UP: {up_display} | DOWN: {down_display}"
        logging.info(log_msg)

        if seconds_remaining == 25 and up_at_25 is None:
            up_at_25 = up_price
            down_at_25 = down_price

        if seconds_remaining == 20:
            up_at_20 = up_price
            down_at_20 = down_price
            if not executed:
                executed = True
                if up_price is None or down_price is None:
                    logging.info("Skip: Prices not available.")
                    continue
                abort = False
                reason = ""
                if up_at_25 is None or down_at_25 is None or up_at_20 is None or down_at_20 is None:
                    abort = True
                    reason = "Missing historical prices."
                else:
                    up_move = abs(up_at_25 - up_at_20)
                    down_move = abs(down_at_25 - down_at_20)
                    if up_move > 0.50 or down_move > 0.50:
                        abort = True
                        reason = "High volatility."
                if up_price + down_price < 0.98:
                    abort = True
                    reason = "Illiquid market."
                if up_price == 0 or down_price == 0:
                    abort = True
                    reason = "Dead side detected."
                leading_price = max(up_price, down_price)
                if leading_price > 0.97:
                    abort = True
                    reason = "Ceiling reached."
                if abort:
                    logging.info(f"Skip: {reason}")
                else:
                    # Identify lead
                    if up_price > down_price:
                        lead_side = 'up'
                        token = token_up
                        entry_price = up_price
                    else:
                        lead_side = 'down'
                        token = token_down
                        entry_price = down_price
                    # Place buy order
                    await asyncio.to_thread(place_buy, token, entry_price, FIXED_AMOUNT)
                    logging.info(f"Placed buy for {lead_side.upper()} at {entry_price} (Real Cost approx: {entry_price * (1 + TAKER_FEE_RATE):.3f})")

        if seconds_remaining > 30:
            await asyncio.sleep(10)
        else:
            await asyncio.sleep(0.25)

async def main():
    global best_asks, client
    while True:
        slug = get_current_market_slug()
        token_ids = None
        while not token_ids:
            metadata = fetch_metadata(slug)
            if metadata:
                token_ids = metadata
            else:
                await asyncio.sleep(5)  # Silent retry every 5 seconds

        best_asks = {'up': None, 'down': None}
        market_start = int(slug.split('-')[-1])
        market_end = market_start + 300
        polymarket_url = f"https://polymarket.com/event/{slug}"
        logging.info(f"Market: {slug} | URL: {polymarket_url} | UP Token: {token_ids['up']} | DOWN Token: {token_ids['down']} | Condition ID: {token_ids['condition_id']}")

        market_ws_task = asyncio.create_task(market_websocket_handler(token_ids['up'], token_ids['down']))
        #user_ws_task = asyncio.create_task(user_websocket_handler(token_ids['condition_id'], token_ids['up'], token_ids['down']))
        logic_task = asyncio.create_task(logic_loop(market_start, market_end, token_ids['up'], token_ids['down'], token_ids['condition_id']))

        await logic_task
        market_ws_task.cancel()
        #user_ws_task.cancel()
        try:
            await market_ws_task
            #await user_ws_task
        except asyncio.CancelledError:
            pass

if __name__ == "__main__":
    asyncio.run(main())