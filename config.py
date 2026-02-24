import os
import sys
import logging

# Formato exato: [DD/MM/YY | HH:MM:SS.ms] Mensagem
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s.%(msecs)03d] %(message)s',
    datefmt='%d/%m/%y | %H:%M:%S',
    handlers=[
        logging.FileHandler("bot_xrp_hft.log"),
        logging.StreamHandler(sys.stdout)
    ]
)

def load_secrets(filepath="secrets.txt"):
    secrets = {}
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"❌ ERRO: O ficheiro '{filepath}' não existe!")
    with open(filepath, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"): continue
            if "=" in line:
                key, value = line.split("=", 1)
                secrets[key.strip()] = value.strip()
    return secrets

credenciais = load_secrets()

HOST = "https://clob.polymarket.com"
CHAIN_ID = 137
PRIVATE_KEY = credenciais.get("PRIVATE_KEY")
API_KEY = credenciais.get("API_KEY")
API_SECRET = credenciais.get("API_SECRET")
API_PASSPHRASE = credenciais.get("API_PASSPHRASE")

if not all([PRIVATE_KEY, API_KEY, API_SECRET, API_PASSPHRASE]):
    raise ValueError("❌ ERRO: Faltam credenciais no secrets.txt.")

FIXED_AMOUNT = 10.0  # (em USDC)
TAKER_FEE_RATE = 0.02  # 2% Taker Fee da Polymarket
