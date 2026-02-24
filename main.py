import asyncio
import time
import logging
from datetime import datetime

import config
import state
from binance_ws import binance_websocket_handler, analisar_tendencia_xrp
from polymarket_api import (
    get_current_market_slug, fetch_metadata, market_websocket_handler, place_buy, place_sell
)

async def logic_loop(market_end, token_up, token_down):
    executed = False
    vendido = False
    posicao_atual = None
    
    while True:
        now = time.time()
        if now >= market_end: break
        
        seconds_remaining = market_end - now
        up_price = state.best_asks['up']
        down_price = state.best_asks['down']
        
        # --- 1. GATILHO DE ENTRADA (Aos 30 segundos) ---
        if seconds_remaining <= 30 and not executed:
            executed = True
            logging.info("="*70)
            logging.info("⏱️ 30s restantes. A processar decisão com base na Binance...")
            tendencia = analisar_tendencia_xrp()
            logging.info(f"🎯 DECISÃO DA BINANCE: {tendencia}")
            
            if tendencia in ["UP", "DOWN"]:
                lead_side = 'up' if tendencia == "UP" else 'down'
                token = token_up if tendencia == "UP" else token_down
                entry_price = up_price if tendencia == "UP" else down_price
                
                if entry_price and entry_price <= 0.85: 
                    real_entry_price = entry_price * (1 + config.TAKER_FEE_RATE)
                    
                    await asyncio.to_thread(place_buy, token, entry_price, config.FIXED_AMOUNT)
                    posicao_atual = {'side': lead_side, 'token': token, 'entry': entry_price, 'real_entry': real_entry_price}
                    
                    logging.info(f"⚡ ENTRADA EFETUADA: {lead_side.upper()} @ {entry_price*100:.1f}¢ | Custo Real c/ Taxa: {real_entry_price*100:.2f}¢")
                else:
                    motivo = f"Já está acima do limite ({entry_price*100:.1f}¢)" if entry_price else "Sem liquidez"
                    logging.info(f"⚠️ ENTRADA ABORTADA: {motivo}.")
            else:
                logging.info("⚖️ ENTRADA ABORTADA: Mercado sem direção clara.")
            logging.info("="*70)

        # --- 2. GATILHOS DE VIGILÂNCIA RÁPIDA (Entre os 30s e os 10s) ---
        if posicao_atual and not vendido and seconds_remaining > 10:
            current_price = up_price if posicao_atual['side'] == 'up' else down_price
            
            if current_price:
                # A) TAKE-PROFIT (+50%)
                if current_price >= (posicao_atual['entry'] * 1.50):
                    vendido = True
                    real_exit_price = current_price * (1 - config.TAKER_FEE_RATE)
                    lucro = real_exit_price - posicao_atual['real_entry']
                    await asyncio.to_thread(place_sell, posicao_atual['token'], current_price, config.FIXED_AMOUNT)
                    logging.info(f"🚀 TAKE-PROFIT (+50%): Vendido a {current_price*100:.1f}¢ | Receita Real: {real_exit_price*100:.2f}¢ | 🟢 LUCRO LÍQUIDO: {lucro*100:.2f}¢/ação")
                
                # B) STOP-LOSS (-2%)
                elif current_price <= (posicao_atual['entry'] * 0.98):
                    vendido = True
                    real_exit_price = current_price * (1 - config.TAKER_FEE_RATE)
                    lucro = real_exit_price - posicao_atual['real_entry'] 
                    await asyncio.to_thread(place_sell, posicao_atual['token'], current_price, config.FIXED_AMOUNT)
                    logging.info(f"🛡️ STOP-LOSS (-2%): Vendido a {current_price*100:.1f}¢ | Receita Real: {real_exit_price*100:.2f}¢ | 🔴 PREJUÍZO LÍQUIDO: {lucro*100:.2f}¢/ação")

        # --- 3. GATILHO FECHO DE SEGURANÇA (Aos 10 segundos) ---
        if seconds_remaining <= 10 and posicao_atual and not vendido:
            vendido = True
            preco_saida = up_price if posicao_atual['side'] == 'up' else down_price
            
            if preco_saida:
                real_exit_price = preco_saida * (1 - config.TAKER_FEE_RATE)
                lucro = real_exit_price - posicao_atual['real_entry']
                
                sinal = "🟢 LUCRO LÍQUIDO" if lucro > 0 else "🔴 PREJUÍZO LÍQUIDO"
                
                await asyncio.to_thread(place_sell, posicao_atual['token'], preco_saida, config.FIXED_AMOUNT)
                logging.info(f"🏁 FECHO DE SEGURANÇA (10s): Vendido a {preco_saida*100:.1f}¢ | Receita Real: {real_exit_price*100:.2f}¢ | {sinal}: {lucro*100:.2f}¢/ação")
            else:
                logging.info("🔴 ERRO NO FECHO: Faltou liquidez aos 10s. Posição bloqueada até à expiração.")

        await asyncio.sleep(0.05)

async def run_bot():
    logging.info("🚀 Bot HFT Binance/Polymarket iniciado!")
    
    binance_task = asyncio.create_task(binance_websocket_handler())
    
    while True:
        slug = get_current_market_slug()
        token_ids = None
        while not token_ids:
            metadata = fetch_metadata(slug)
            if metadata: token_ids = metadata
            else: await asyncio.sleep(5)
        
        state.best_asks['up'] = state.best_asks['down'] = None
        market_start = int(slug.split('-')[-1])
        market_end = market_start + 300
        polymarket_url = f"https://polymarket.com/event/{slug}"
        
        logging.info("")
        logging.info("="*70)
        logging.info(f"Market: {slug} | URL: {polymarket_url}")
        logging.info("="*70)
        
        market_ws_task = asyncio.create_task(market_websocket_handler(token_ids['up'], token_ids['down']))
        logic_task = asyncio.create_task(logic_loop(market_end, token_ids['up'], token_ids['down']))
        
        await logic_task
        market_ws_task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(run_bot())
    except KeyboardInterrupt:
        print("\n🛑 Bot parado.")
