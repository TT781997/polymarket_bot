import asyncio
import websockets
import json
import time
import requests
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import ApiCreds
import config
import state

creds = ApiCreds(config.API_KEY, config.API_SECRET, config.API_PASSPHRASE)
client = ClobClient(config.HOST, chain_id=config.CHAIN_ID, key=config.PRIVATE_KEY, creds=creds)

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
        if not data: return None
        
        event = data[0]
        market = event['markets'][0]
        outcomes = json.loads(market['outcomes'])
        clob_token_ids = json.loads(market['clobTokenIds'])
        token_up = token_down = None
        
        for i, name in enumerate(outcomes):
            if name.upper() == 'UP': token_up = clob_token_ids[i]
            elif name.upper() == 'DOWN': token_down = clob_token_ids[i]
            
        condition_id = market['conditionId']
        if token_up and token_down and condition_id:
            return {'up': token_up, 'down': token_down, 'condition_id': condition_id}
    except: pass
    return None

def process_item(item, token_up, token_down):
    if item.get("event_type") == "book" and item.get("asks"):
        try:
            best_ask = min(float(d['price']) for d in item["asks"] if float(d['size']) > 0)
            if item.get("asset_id") == token_up: state.best_asks['up'] = best_ask
            elif item.get("asset_id") == token_down: state.best_asks['down'] = best_ask
        except: pass

async def market_websocket_handler(token_up, token_down):
    uri = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                sub_msg = json.dumps({"assets_ids": [token_up, token_down], "type": "market", "custom_feature_enabled": True})
                await ws.send(sub_msg)
                while True:
                    message = await ws.recv()
                    data = json.loads(message)
                    if isinstance(data, list):
                        for item in data: process_item(item, token_up, token_down)
                    else:
                        process_item(data, token_up, token_down)
        except Exception:
            await asyncio.sleep(1)

def place_buy(token_id, price, size):
    pass # Simulação

def place_sell(token_id, price, size):
    pass # Simulação
