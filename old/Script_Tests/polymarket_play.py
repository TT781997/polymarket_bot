import asyncio
import websockets
import json
import time
import logging
import requests
from datetime import datetime

# Setup logging to file without timestamps, as we format manually
logging.basicConfig(filename='bot_xrp.log', level=logging.INFO, format='%(message)s')

# Global state for best asks
best_asks = {'up': None, 'down': None}

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
        if token_up and token_down:
            return {'up': token_up, 'down': token_down}
        else:
            return None
    except requests.exceptions.RequestException:
        return None

async def websocket_handler(token_up, token_down):
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
            logging.error(f"WebSocket error: {e}")
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

async def logic_loop(market_start, market_end):
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
        up_cent = int(up_price * 100) if up_price is not None else None
        down_cent = int(down_price * 100) if down_price is not None else None
        current_time_str = datetime.fromtimestamp(now).strftime("%H:%M:%S")
        up_display = f"{up_cent}c" if up_cent is not None else 'N/A'
        down_display = f"{down_cent}c" if down_cent is not None else 'N/A'
        log_msg = f"{current_time_str} | Remaining: {seconds_remaining}s | UP: {up_display} | DOWN: {down_display}"
        logging.info(log_msg)

        if seconds_remaining == 25 and up_at_25 is None:
            up_at_25 = up_price
            down_at_25 = down_price

        if seconds_remaining == 20 and up_at_20 is None:
            up_at_20 = up_price
            down_at_20 = down_price
        
        if seconds_remaining == 19 and not executed:
            executed = True
            if up_price is None or down_price is None:
                logging.info("No entry: Prices not available.")
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
            leading = max(up_price, down_price)
            if leading > 0.97:
                abort = True
                reason = "Ceiling reached."
            if abort:
                logging.info(f"Aborted: {reason}")
            else:
                entered = False
                if up_price > 0.85:
                    logging.info(f"Simulation: Entry in UP at {up_cent}c")
                    entered = True
                if down_price > 0.85:
                    logging.info(f"Simulation: Entry in DOWN at {down_cent}c")
                    entered = True
                if not entered:
                    logging.info("No entry: Odds below threshold.")
        
        if seconds_remaining > 30:
            await asyncio.sleep(10)
        else:
            await asyncio.sleep(0.25)

async def main():
    global best_asks
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
        logging.info(f"Market: {slug} | URL: {polymarket_url} | UP Token: {token_ids['up']} | DOWN Token: {token_ids['down']}")
        
        ws_task = asyncio.create_task(websocket_handler(token_ids['up'], token_ids['down']))
        logic_task = asyncio.create_task(logic_loop(market_start, market_end))
        
        await logic_task
        ws_task.cancel()
        try:
            await ws_task
        except asyncio.CancelledError:
            pass

if __name__ == "__main__":
    asyncio.run(main())