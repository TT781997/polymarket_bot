import asyncio
import websockets
import json
import logging
import state

def calcular_ema(precos, periodos):
    if len(precos) < periodos: return None
    k = 2 / (periodos + 1)
    ema = sum(precos[:periodos]) / periodos
    for preco in precos[periodos:]:
        ema = (preco - ema) * k + ema
    return ema

def analisar_tendencia_xrp():
    precos = list(state.historico_xrp)
    if len(precos) < 100: return "SEM_DADOS"
    
    ema_curta = calcular_ema(precos, 20)
    ema_longa = calcular_ema(precos, 100)
    
    if ema_curta is None or ema_longa is None: return "NEUTRO"
    
    variacao_total = precos[-1] - precos[0]
    
    if ema_curta > ema_longa and variacao_total > 0: return "UP"
    elif ema_curta < ema_longa and variacao_total < 0: return "DOWN"
    return "NEUTRO"

async def binance_websocket_handler():
    uri = "wss://stream.binance.com:9443/ws/xrpusdt@aggTrade"
    ticks = 0
    while True:
        try:
            logging.info("🔗 A tentar ligar ao WebSocket público da Binance (XRP/USDT)...")
            async with websockets.connect(uri) as ws:
                logging.info("✅ SUCESSO! Conectado à Binance. A receber dados ao milissegundo!")
                while True:
                    message = await ws.recv()
                    data = json.loads(message)
                    if 'p' in data:
                        preco_xrp = float(data['p'])
                        state.historico_xrp.append(preco_xrp)
                        
                        ticks += 1
                        if ticks % 100 == 0:
                            logging.info(f"📡 [BINANCE] Preço Atual: ${preco_xrp:.4f} | Amostras: {len(state.historico_xrp)}/5000")
        except Exception as e:
            logging.error(f"❌ Erro na ligação à Binance: {e}. A reconectar em 2s...")
            await asyncio.sleep(2)
