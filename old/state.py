from collections import deque

# Guarda os melhores preços atuais do Polymarket
best_asks = {'up': None, 'down': None}

# Guarda as últimas 5000 transações de XRP da Binance ao milissegundo
historico_xrp = deque(maxlen=5000)
